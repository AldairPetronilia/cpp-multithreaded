#include "trading/api.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace trading {
namespace {

class SlidingWindowRateLimiter {
public:
    explicit SlidingWindowRateLimiter(RateLimitConfig config) : config_(config) {
        if (config_.max_requests == 0U || config_.window <= std::chrono::milliseconds::zero()) {
            throw std::invalid_argument("rate limit allowance and window must be positive");
        }
    }

    bool acquire(std::stop_token cancellation, std::chrono::nanoseconds& waited) {
        const auto wait_started = MonotonicClock::now();
        std::unique_lock lock(mutex_);

        for (;;) {
            if (cancellation.stop_requested()) {
                waited += MonotonicClock::now() - wait_started;
                return false;
            }

            const auto now = MonotonicClock::now();
            while (!request_starts_.empty() &&
                   request_starts_.front() + config_.window <= now) {
                request_starts_.pop_front();
            }

            auto permitted_at = std::max(now, deferred_until_);
            if (request_starts_.size() >= config_.max_requests) {
                permitted_at = std::max(permitted_at,
                                        request_starts_.front() + config_.window);
            }

            if (permitted_at <= now) {
                request_starts_.push_back(now);
                waited += now - wait_started;
                return true;
            }

            wakeup_.wait_until(lock, cancellation, permitted_at, [] { return false; });
        }
    }

    void defer_for(std::chrono::milliseconds duration) {
        if (duration <= std::chrono::milliseconds::zero()) {
            return;
        }
        {
            std::lock_guard lock(mutex_);
            deferred_until_ = std::max(deferred_until_, MonotonicClock::now() + duration);
        }
        wakeup_.notify_all();
    }

private:
    RateLimitConfig config_;
    std::mutex mutex_;
    std::condition_variable_any wakeup_;
    std::deque<TimePoint> request_starts_;
    TimePoint deferred_until_{};
};

[[nodiscard]] bool is_retryable(const ApiResponse& response) noexcept {
    return response.error == ApiError::timeout ||
           response.error == ApiError::connection_failure ||
           response.error == ApiError::rate_limited || response.status_code == 429 ||
           response.status_code >= 500;
}

[[nodiscard]] std::chrono::milliseconds exponential_backoff(
    std::chrono::milliseconds initial,
    std::chrono::milliseconds maximum,
    std::size_t completed_attempts) {
    auto result = initial;
    for (std::size_t index = 1; index < completed_attempts && result < maximum; ++index) {
        result = std::min(maximum, result * 2);
    }
    return result;
}

}  // namespace

const char* to_string(ApiError error) noexcept {
    switch (error) {
        case ApiError::none:
            return "none";
        case ApiError::timeout:
            return "timeout";
        case ApiError::connection_failure:
            return "connection failure";
        case ApiError::rate_limited:
            return "rate limited";
        case ApiError::cancelled:
            return "cancelled";
        case ApiError::internal_error:
            return "internal error";
    }
    return "unknown";
}

RequestHandle::RequestHandle(
    std::uint64_t id,
    std::shared_ptr<detail::RequestCancellation> cancellation)
    : id_(id), cancellation_(std::move(cancellation)) {}

void RequestHandle::cancel() const {
    if (!cancellation_) {
        return;
    }
    cancellation_->source.request_stop();
    if (auto notifier = cancellation_->queue_notifier.lock()) {
        notifier->notify_all();
    }
}

bool RequestHandle::valid() const noexcept {
    return static_cast<bool>(cancellation_);
}

std::uint64_t RequestHandle::id() const noexcept {
    return id_;
}

class ApiRequestManager::Impl {
public:
    struct RequestState {
        std::uint64_t id{};
        std::uint64_t queue_sequence{};
        ApiRequest request;
        ApiCompletion completion;
        std::shared_ptr<detail::RequestCancellation> cancellation;
        TimePoint submitted_at{};
        TimePoint enqueued_at{};
        TimePoint not_before{};
        std::size_t attempt{1};
        std::chrono::nanoseconds queue_delay{};
        std::chrono::nanoseconds rate_limit_wait{};
        std::chrono::nanoseconds service_time{};
    };

    Impl(std::shared_ptr<IApiClient> client, ApiManagerConfig config)
        : client_(std::move(client)),
          config_(config),
          limiter_(config_.rate_limit),
          pending_wakeup_(std::make_shared<std::condition_variable>()) {
        if (!client_) {
            throw std::invalid_argument("API client must not be null");
        }
        if (config_.io_threads == 0U || config_.max_outstanding_requests == 0U) {
            throw std::invalid_argument("API thread and queue capacities must be positive");
        }
        if (config_.initial_retry_backoff < std::chrono::milliseconds::zero() ||
            config_.max_retry_backoff < config_.initial_retry_backoff) {
            throw std::invalid_argument("invalid API retry backoff configuration");
        }

        workers_.reserve(config_.io_threads);
        for (std::size_t index = 0; index < config_.io_threads; ++index) {
            workers_.emplace_back([this](std::stop_token token) { worker_loop(token); });
        }
    }

    ~Impl() {
        shutdown(ApiShutdownMode::cancel_pending);
    }

    SubmitResult submit(ApiRequest request, ApiCompletion completion) {
        if (!completion) {
            throw std::invalid_argument("API completion callback must not be empty");
        }
        if (request.timeout <= std::chrono::milliseconds::zero()) {
            throw std::invalid_argument("API request timeout must be positive");
        }

        std::unique_lock lock(mutex_);
        if (!accepting_) {
            ++rejected_shutdown_;
            return {SubmitStatus::shutting_down, {}};
        }
        if (requests_.size() >= config_.max_outstanding_requests) {
            ++rejected_full_;
            return {SubmitStatus::queue_full, {}};
        }

        const auto now = MonotonicClock::now();
        const auto id = next_request_id_++;
        auto cancellation = std::make_shared<detail::RequestCancellation>();
        cancellation->queue_notifier = pending_wakeup_;
        auto state = std::make_shared<RequestState>();
        state->id = id;
        state->queue_sequence = next_queue_sequence_++;
        state->request = std::move(request);
        state->completion = std::move(completion);
        state->cancellation = cancellation;
        state->submitted_at = now;
        state->enqueued_at = now;
        state->not_before = now;

        requests_.emplace(id, state);
        pending_.push_back(state);
        max_pending_ = std::max(max_pending_, pending_.size());
        ++accepted_;
        lock.unlock();
        pending_wakeup_->notify_one();
        return {SubmitStatus::accepted, RequestHandle(id, std::move(cancellation))};
    }

    void shutdown(ApiShutdownMode mode) {
        std::unique_lock lock(mutex_);
        if (joined_) {
            return;
        }
        accepting_ = false;
        if (mode == ApiShutdownMode::cancel_pending) {
            const auto now = MonotonicClock::now();
            for (auto& [id, state] : requests_) {
                static_cast<void>(id);
                state->cancellation->source.request_stop();
                state->not_before = now;
            }
        }
        lock.unlock();
        pending_wakeup_->notify_all();

        lock.lock();
        finished_wakeup_.wait(lock, [this] { return requests_.empty(); });
        stopping_ = true;
        lock.unlock();
        pending_wakeup_->notify_all();

        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers_.clear();

        lock.lock();
        joined_ = true;
    }

    [[nodiscard]] ApiManagerStats stats() const {
        std::lock_guard lock(mutex_);
        return ApiManagerStats{
            accepted_,
            rejected_full_,
            rejected_shutdown_,
            completed_,
            failed_,
            cancelled_,
            retries_,
            requests_.size(),
            pending_.size(),
            active_,
            max_pending_,
        };
    }

private:
    std::shared_ptr<RequestState> take_next(std::stop_token worker_stop) {
        std::unique_lock lock(mutex_);
        for (;;) {
            if (stopping_ || worker_stop.stop_requested()) {
                return {};
            }
            if (pending_.empty()) {
                pending_wakeup_->wait(lock, [this, worker_stop] {
                    return stopping_ || worker_stop.stop_requested() || !pending_.empty();
                });
                continue;
            }

            const auto now = MonotonicClock::now();
            auto selected = pending_.end();
            auto selected_time = TimePoint::max();
            std::uint64_t selected_sequence = std::numeric_limits<std::uint64_t>::max();
            for (auto iterator = pending_.begin(); iterator != pending_.end(); ++iterator) {
                const auto& candidate = *iterator;
                const auto candidate_time = candidate->cancellation->source.stop_requested()
                                                ? TimePoint::min()
                                                : candidate->not_before;
                if (candidate_time < selected_time ||
                    (candidate_time == selected_time &&
                     candidate->queue_sequence < selected_sequence)) {
                    selected = iterator;
                    selected_time = candidate_time;
                    selected_sequence = candidate->queue_sequence;
                }
            }

            if (selected_time > now) {
                pending_wakeup_->wait_until(lock, selected_time);
                continue;
            }

            auto state = std::move(*selected);
            pending_.erase(selected);
            state->queue_delay += now - state->enqueued_at;
            ++active_;
            return state;
        }
    }

    void worker_loop(std::stop_token worker_stop) {
        while (auto state = take_next(worker_stop)) {
            process(std::move(state));
        }
    }

    void process(std::shared_ptr<RequestState> state) {
        const auto cancellation = state->cancellation->source.get_token();
        if (cancellation.stop_requested()) {
            ApiResponse response;
            response.error = ApiError::cancelled;
            response.message = "request cancelled before dispatch";
            finish(std::move(state), std::move(response));
            return;
        }

        std::chrono::nanoseconds limiter_wait{};
        if (!limiter_.acquire(cancellation, limiter_wait)) {
            state->rate_limit_wait += limiter_wait;
            ApiResponse response;
            response.error = ApiError::cancelled;
            response.message = "request cancelled while waiting for rate-limit capacity";
            finish(std::move(state), std::move(response));
            return;
        }
        state->rate_limit_wait += limiter_wait;

        if (cancellation.stop_requested()) {
            ApiResponse response;
            response.error = ApiError::cancelled;
            response.message = "request cancelled before API call";
            finish(std::move(state), std::move(response));
            return;
        }

        const auto service_started = MonotonicClock::now();
        ApiResponse response;
        try {
            response = client_->perform(state->request, cancellation);
        } catch (const std::exception& exception) {
            response.error = ApiError::connection_failure;
            response.message = exception.what();
        } catch (...) {
            response.error = ApiError::internal_error;
            response.message = "API client threw an unknown exception";
        }
        state->service_time += MonotonicClock::now() - service_started;

        if (response.status_code == 429 || response.error == ApiError::rate_limited) {
            const auto retry_after = response.retry_after > std::chrono::milliseconds::zero()
                                         ? response.retry_after
                                         : config_.initial_retry_backoff;
            limiter_.defer_for(retry_after);
        }

        const bool retry_safe = state->request.idempotent &&
                                !state->request.idempotency_key.empty();
        if (!cancellation.stop_requested() && retry_safe && is_retryable(response) &&
            state->attempt <= config_.max_retries) {
            const auto delay = response.retry_after > std::chrono::milliseconds::zero()
                                   ? response.retry_after
                                   : exponential_backoff(config_.initial_retry_backoff,
                                                         config_.max_retry_backoff,
                                                         state->attempt);
            requeue_for_retry(std::move(state), delay);
            return;
        }

        if (cancellation.stop_requested() && response.error != ApiError::cancelled) {
            response.error = ApiError::cancelled;
            response.status_code = 0;
            response.message = "request cancelled";
        }
        finish(std::move(state), std::move(response));
    }

    void requeue_for_retry(std::shared_ptr<RequestState> state,
                           std::chrono::milliseconds delay) {
        const auto now = MonotonicClock::now();
        std::unique_lock lock(mutex_);
        --active_;
        ++state->attempt;
        state->queue_sequence = next_queue_sequence_++;
        state->enqueued_at = now;
        state->not_before = now + delay;
        pending_.push_back(std::move(state));
        max_pending_ = std::max(max_pending_, pending_.size());
        ++retries_;
        lock.unlock();
        pending_wakeup_->notify_one();
    }

    void finish(std::shared_ptr<RequestState> state, ApiResponse response) {
        response.attempts = state->attempt;
        response.timing.queue_delay = state->queue_delay;
        response.timing.rate_limit_wait = state->rate_limit_wait;
        response.timing.service_time = state->service_time;
        response.timing.total_time = MonotonicClock::now() - state->submitted_at;

        bool callback_failed = false;
        try {
            state->completion(response);
        } catch (const std::exception& exception) {
            callback_failed = true;
            std::cerr << "API completion callback failed for request " << state->id << ": "
                      << exception.what() << '\n';
        } catch (...) {
            callback_failed = true;
            std::cerr << "API completion callback failed for request " << state->id
                      << " with an unknown exception\n";
        }

        std::unique_lock lock(mutex_);
        --active_;
        if (response.error == ApiError::cancelled) {
            ++cancelled_;
        } else if (!response.ok() || callback_failed) {
            ++failed_;
        } else {
            ++completed_;
        }
        requests_.erase(state->id);
        const bool empty = requests_.empty();
        lock.unlock();
        if (empty) {
            finished_wakeup_.notify_all();
        }
    }

    std::shared_ptr<IApiClient> client_;
    ApiManagerConfig config_;
    SlidingWindowRateLimiter limiter_;
    std::shared_ptr<std::condition_variable> pending_wakeup_;
    mutable std::mutex mutex_;
    std::condition_variable finished_wakeup_;
    std::deque<std::shared_ptr<RequestState>> pending_;
    std::unordered_map<std::uint64_t, std::shared_ptr<RequestState>> requests_;
    std::vector<std::jthread> workers_;
    std::uint64_t next_request_id_{1};
    std::uint64_t next_queue_sequence_{1};
    bool accepting_{true};
    bool stopping_{false};
    bool joined_{false};
    std::uint64_t accepted_{};
    std::uint64_t rejected_full_{};
    std::uint64_t rejected_shutdown_{};
    std::uint64_t completed_{};
    std::uint64_t failed_{};
    std::uint64_t cancelled_{};
    std::uint64_t retries_{};
    std::size_t active_{};
    std::size_t max_pending_{};
};

ApiRequestManager::ApiRequestManager(std::shared_ptr<IApiClient> client,
                                     ApiManagerConfig config)
    : impl_(std::make_unique<Impl>(std::move(client), config)) {}

ApiRequestManager::~ApiRequestManager() = default;

SubmitResult ApiRequestManager::submit(ApiRequest request, ApiCompletion completion) {
    return impl_->submit(std::move(request), std::move(completion));
}

void ApiRequestManager::shutdown(ApiShutdownMode mode) {
    impl_->shutdown(mode);
}

ApiManagerStats ApiRequestManager::stats() const {
    return impl_->stats();
}

MockTradingApi::MockTradingApi(MockApiConfig config) : config_(config) {
    if (config_.latency < std::chrono::milliseconds::zero() ||
        config_.retry_after < std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("mock API durations must not be negative");
    }
}

ApiResponse MockTradingApi::perform(const ApiRequest& request,
                                    std::stop_token cancellation) {
    const auto invocation = invocation_count_.fetch_add(1, std::memory_order_relaxed) + 1;

    if (request.idempotent && !request.idempotency_key.empty()) {
        std::lock_guard lock(idempotency_mutex_);
        const auto found = completed_idempotent_requests_.find(request.idempotency_key);
        if (found != completed_idempotent_requests_.end()) {
            return found->second;
        }
    }

    const auto wait_duration = std::min(config_.latency, request.timeout);
    if (wait_duration > std::chrono::milliseconds::zero()) {
        std::mutex sleep_mutex;
        std::condition_variable_any sleeper;
        std::unique_lock lock(sleep_mutex);
        sleeper.wait_for(lock, cancellation, wait_duration, [] { return false; });
    }

    if (cancellation.stop_requested()) {
        ApiResponse response;
        response.error = ApiError::cancelled;
        response.message = "mock API call cancelled";
        return response;
    }
    if (config_.latency > request.timeout) {
        ApiResponse response;
        response.error = ApiError::timeout;
        response.message = "mock API timeout";
        return response;
    }
    if (config_.fail_connection_every > 0U &&
        invocation % config_.fail_connection_every == 0U) {
        ApiResponse response;
        response.error = ApiError::connection_failure;
        response.message = "simulated connection failure";
        return response;
    }
    if (config_.respond_429_every > 0U &&
        invocation % config_.respond_429_every == 0U) {
        ApiResponse response;
        response.status_code = 429;
        response.error = ApiError::rate_limited;
        response.message = "simulated HTTP 429";
        response.retry_after = config_.retry_after;
        return response;
    }

    ApiResponse response;
    response.status_code = 200;
    response.body = "filled:" + request.idempotency_key;
    response.message = "simulated order filled";

    if (request.idempotent && !request.idempotency_key.empty()) {
        std::lock_guard lock(idempotency_mutex_);
        const auto [iterator, inserted] =
            completed_idempotent_requests_.emplace(request.idempotency_key, response);
        if (!inserted) {
            return iterator->second;
        }
    }
    return response;
}

std::uint64_t MockTradingApi::invocation_count() const noexcept {
    return invocation_count_.load(std::memory_order_relaxed);
}

}  // namespace trading
