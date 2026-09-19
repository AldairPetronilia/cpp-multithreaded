#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/workers.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

#define CHECK(expression)                                                                  \
    do {                                                                                   \
        if (!(expression)) {                                                               \
            std::ostringstream failure_message;                                            \
            failure_message << __FILE__ << ':' << __LINE__ << ": check failed: "          \
                            << #expression;                                                 \
            throw TestFailure(failure_message.str());                                      \
        }                                                                                  \
    } while (false)

class CompletionGroup {
public:
    explicit CompletionGroup(std::size_t expected) : expected_(expected) {}

    void complete(trading::ApiResponse response) {
        {
            std::lock_guard lock(mutex_);
            responses_.push_back(std::move(response));
        }
        completed_.fetch_add(1, std::memory_order_release);
        wakeup_.notify_all();
    }

    bool wait_for(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return wakeup_.wait_for(lock, timeout, [this] {
            return completed_.load(std::memory_order_acquire) == expected_;
        });
    }

    [[nodiscard]] std::vector<trading::ApiResponse> responses() const {
        std::lock_guard lock(mutex_);
        return responses_;
    }

private:
    const std::size_t expected_;
    mutable std::mutex mutex_;
    std::condition_variable wakeup_;
    std::vector<trading::ApiResponse> responses_;
    std::atomic<std::size_t> completed_{0};
};

class RecordingClient final : public trading::IApiClient {
public:
    explicit RecordingClient(std::chrono::milliseconds latency = 0ms)
        : latency_(latency) {}

    trading::ApiResponse perform(const trading::ApiRequest&,
                                 std::stop_token cancellation) override {
        {
            std::lock_guard lock(mutex_);
            starts_.push_back(trading::MonotonicClock::now());
        }
        started_.fetch_add(1, std::memory_order_release);
        started_wakeup_.notify_all();

        if (latency_ > 0ms) {
            std::mutex sleep_mutex;
            std::condition_variable_any sleeper;
            std::unique_lock lock(sleep_mutex);
            sleeper.wait_for(lock, cancellation, latency_, [] { return false; });
        }
        if (cancellation.stop_requested()) {
            trading::ApiResponse response;
            response.error = trading::ApiError::cancelled;
            response.message = "cancelled";
            return response;
        }
        trading::ApiResponse response;
        response.status_code = 200;
        response.body = "ok";
        response.message = "ok";
        return response;
    }

    bool wait_for_starts(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock lock(started_mutex_);
        return started_wakeup_.wait_for(lock, timeout, [this, count] {
            return started_.load(std::memory_order_acquire) >= count;
        });
    }

    [[nodiscard]] std::vector<trading::TimePoint> starts() const {
        std::lock_guard lock(mutex_);
        return starts_;
    }

private:
    std::chrono::milliseconds latency_;
    mutable std::mutex mutex_;
    std::vector<trading::TimePoint> starts_;
    std::atomic<std::size_t> started_{0};
    std::mutex started_mutex_;
    std::condition_variable started_wakeup_;
};

class RetryClient final : public trading::IApiClient {
public:
    trading::ApiResponse perform(const trading::ApiRequest& request,
                                 std::stop_token) override {
        std::lock_guard lock(mutex_);
        auto& count = calls_[request.endpoint];
        ++count;
        if (count == 1U) {
            trading::ApiResponse response;
            response.error = trading::ApiError::connection_failure;
            response.message = "first call fails";
            return response;
        }
        trading::ApiResponse response;
        response.status_code = 200;
        response.body = "ok";
        response.message = "ok";
        return response;
    }

    [[nodiscard]] std::size_t calls(const std::string& endpoint) const {
        std::lock_guard lock(mutex_);
        const auto found = calls_.find(endpoint);
        return found == calls_.end() ? 0U : found->second;
    }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::size_t> calls_;
};

class RetryAfterClient final : public trading::IApiClient {
public:
    trading::ApiResponse perform(const trading::ApiRequest&,
                                 std::stop_token) override {
        std::lock_guard lock(mutex_);
        starts_.push_back(trading::MonotonicClock::now());
        if (starts_.size() == 1U) {
            trading::ApiResponse response;
            response.status_code = 429;
            response.error = trading::ApiError::rate_limited;
            response.message = "slow down";
            response.retry_after = 40ms;
            return response;
        }
        trading::ApiResponse response;
        response.status_code = 200;
        response.message = "ok";
        return response;
    }

    [[nodiscard]] std::vector<trading::TimePoint> starts() const {
        std::lock_guard lock(mutex_);
        return starts_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<trading::TimePoint> starts_;
};

void messaging_delivers_to_every_matching_subscriber() {
    trading::EventBus bus;
    auto first = std::make_shared<trading::BoundedMailbox>(8);
    auto second = std::make_shared<trading::BoundedMailbox>(8);
    auto signals_only = std::make_shared<trading::BoundedMailbox>(8);
    auto first_subscription = bus.subscribe<trading::MarketDataEvent>(first);
    auto second_subscription = bus.subscribe<trading::MarketDataEvent>(second);
    auto signal_subscription = bus.subscribe<trading::TradingSignalEvent>(signals_only);

    const auto result =
        bus.publish(trading::MarketDataEvent{"TEST", 101.5, 1});
    CHECK(result.subscribers == 2U);
    CHECK(result.delivered == 2U);
    CHECK(result.all_delivered());

    trading::EventEnvelope event;
    CHECK(first->try_pop(event));
    CHECK(event.is<trading::MarketDataEvent>());
    CHECK(event.as<trading::MarketDataEvent>()->price == 101.5);
    CHECK(second->try_pop(event));
    CHECK(!signals_only->try_pop(event));

    static_cast<void>(first_subscription);
    static_cast<void>(second_subscription);
    static_cast<void>(signal_subscription);
}

void messaging_is_safe_with_concurrent_publishers() {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t events_per_producer = 2'000;
    constexpr std::size_t total_events = producer_count * events_per_producer;

    trading::EventBus bus;
    auto mailbox = std::make_shared<trading::BoundedMailbox>(total_events);
    auto subscription = bus.subscribe<trading::BenchmarkEvent>(mailbox);
    std::atomic<std::size_t> publication_failures{0};
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&bus, &publication_failures, producer] {
            for (std::size_t sequence = 0; sequence < events_per_producer; ++sequence) {
                const auto result = bus.publish(trading::BenchmarkEvent{producer, sequence});
                if (result.delivered != 1U) {
                    publication_failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }

    CHECK(publication_failures.load(std::memory_order_relaxed) == 0U);
    CHECK(mailbox->size() == total_events);
    std::set<std::pair<std::uint64_t, std::uint64_t>> received;
    trading::EventEnvelope envelope;
    while (mailbox->try_pop(envelope)) {
        const auto event = envelope.as<trading::BenchmarkEvent>();
        CHECK(static_cast<bool>(event));
        received.emplace(event->producer_id, event->sequence);
    }
    CHECK(received.size() == total_events);
    CHECK(bus.stats().subscriber_deliveries == total_events);
    static_cast<void>(subscription);
}

void mailbox_backpressure_is_explicit() {
    trading::EventBus bus;
    auto mailbox = std::make_shared<trading::BoundedMailbox>(1);
    auto subscription = bus.subscribe<trading::BenchmarkEvent>(mailbox);
    CHECK(bus.publish(trading::BenchmarkEvent{1, 1}).delivered == 1U);
    const auto overloaded = bus.publish(trading::BenchmarkEvent{1, 2});
    CHECK(overloaded.delivered == 0U);
    CHECK(overloaded.rejected_full == 1U);
    CHECK(bus.stats().rejected_full == 1U);
    static_cast<void>(subscription);
}

void shared_rate_limit_applies_across_submitters() {
    auto client = std::make_shared<RecordingClient>();
    trading::ApiManagerConfig config;
    config.rate_limit = {2, 60ms};
    config.io_threads = 4;
    config.max_outstanding_requests = 16;
    config.max_retries = 0;
    trading::ApiRequestManager manager(client, config);
    CompletionGroup completions(6);
    std::atomic<std::size_t> submission_failures{0};

    std::vector<std::thread> submitters;
    for (std::size_t submitter = 0; submitter < 3; ++submitter) {
        submitters.emplace_back([&manager, &completions, &submission_failures, submitter] {
            for (std::size_t index = 0; index < 2; ++index) {
                trading::ApiRequest request;
                request.endpoint = "/rate-limit";
                request.idempotency_key = std::to_string(submitter) + '-' +
                                          std::to_string(index);
                request.idempotent = true;
                const auto result = manager.submit(
                    std::move(request),
                    [&completions](trading::ApiResponse response) {
                        completions.complete(std::move(response));
                    });
                if (!result.accepted()) {
                    submission_failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& submitter : submitters) {
        submitter.join();
    }

    CHECK(submission_failures.load(std::memory_order_relaxed) == 0U);
    manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(completions.wait_for(10ms));
    auto starts = client->starts();
    CHECK(starts.size() == 6U);
    std::sort(starts.begin(), starts.end());
    for (std::size_t index = 2; index < starts.size(); ++index) {
        CHECK(starts[index] - starts[index - 2] >= 50ms);
    }
}

void slow_api_does_not_block_unrelated_event_processing() {
    trading::EventBus bus;
    auto client = std::make_shared<RecordingClient>(250ms);
    trading::ApiManagerConfig api_config;
    api_config.rate_limit = {100, 1s};
    api_config.io_threads = 1;
    api_config.max_outstanding_requests = 8;
    api_config.max_retries = 0;
    trading::ApiRequestManager manager(client, api_config);
    trading::OrderExecutionWorker execution(
        bus, manager, trading::OrderExecutionConfig{1s, 64});

    std::mutex count_mutex;
    std::condition_variable count_wakeup;
    std::size_t market_events = 0;
    trading::EventWorker market_worker(
        "test-market-worker", bus, 64, [&](const trading::EventEnvelope& event) {
            if (event.is<trading::MarketDataEvent>()) {
                {
                    std::lock_guard lock(count_mutex);
                    ++market_events;
                }
                count_wakeup.notify_all();
            }
        });
    market_worker.subscribe<trading::MarketDataEvent>();

    market_worker.start();
    execution.start();
    CHECK(bus.publish(trading::TradingSignalEvent{
                          1, "TEST", trading::Side::buy, 1.0, 100.0,
                          trading::MonotonicClock::now()})
              .delivered == 1U);
    CHECK(client->wait_for_starts(1, 100ms));

    const auto processing_started = trading::MonotonicClock::now();
    for (std::size_t index = 0; index < 20; ++index) {
        CHECK(bus.publish(trading::MarketDataEvent{
                              "TEST", 100.0, static_cast<std::uint64_t>(index)})
                  .delivered == 1U);
    }
    {
        std::unique_lock lock(count_mutex);
        CHECK(count_wakeup.wait_for(lock, 100ms, [&] { return market_events == 20U; }));
    }
    CHECK(trading::MonotonicClock::now() - processing_started < 200ms);

    market_worker.stop();
    execution.stop();
    manager.shutdown(trading::ApiShutdownMode::drain);
}

void position_book_is_thread_safe() {
    trading::PositionBook book;
    constexpr std::size_t thread_count = 8;
    constexpr std::size_t fills_per_thread = 1'000;
    std::vector<std::thread> threads;
    for (std::size_t thread = 0; thread < thread_count; ++thread) {
        threads.emplace_back([&book, thread] {
            const auto side = thread % 2U == 0U ? trading::Side::buy : trading::Side::sell;
            for (std::size_t index = 0; index < fills_per_thread; ++index) {
                book.apply(trading::ExecutionReportEvent{
                    index,
                    "order",
                    "TEST",
                    side,
                    1.0,
                    100.0,
                    true,
                    "filled",
                    trading::MonotonicClock::now(),
                    trading::MonotonicClock::now(),
                });
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    CHECK(book.fills_applied() == thread_count * fills_per_thread);
    CHECK(book.position("TEST") == 0.0);
}

void retry_policy_requires_idempotency() {
    auto client = std::make_shared<RetryClient>();
    trading::ApiManagerConfig config;
    config.rate_limit = {100, 1ms};
    config.io_threads = 2;
    config.max_outstanding_requests = 8;
    config.max_retries = 2;
    config.initial_retry_backoff = 1ms;
    config.max_retry_backoff = 2ms;
    trading::ApiRequestManager manager(client, config);
    CompletionGroup completions(2);

    trading::ApiRequest safe;
    safe.endpoint = "/safe";
    safe.idempotent = true;
    safe.idempotency_key = "safe-key";
    CHECK(manager.submit(std::move(safe), [&](trading::ApiResponse response) {
                     completions.complete(std::move(response));
                 }).accepted());

    trading::ApiRequest unsafe;
    unsafe.endpoint = "/unsafe";
    unsafe.idempotent = false;
    CHECK(manager.submit(std::move(unsafe), [&](trading::ApiResponse response) {
                     completions.complete(std::move(response));
                 }).accepted());

    manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(completions.wait_for(10ms));
    CHECK(client->calls("/safe") == 2U);
    CHECK(client->calls("/unsafe") == 1U);
    const auto responses = completions.responses();
    CHECK(std::count_if(responses.begin(), responses.end(),
                        [](const auto& response) { return response.ok(); }) == 1);
}

void retry_after_and_timeouts_are_honoured() {
    auto retry_client = std::make_shared<RetryAfterClient>();
    trading::ApiManagerConfig retry_config;
    retry_config.rate_limit = {100, 1ms};
    retry_config.io_threads = 1;
    retry_config.max_outstanding_requests = 4;
    retry_config.max_retries = 1;
    trading::ApiRequestManager retry_manager(retry_client, retry_config);
    CompletionGroup retry_completion(1);

    trading::ApiRequest retry_request;
    retry_request.endpoint = "/retry-after";
    retry_request.idempotent = true;
    retry_request.idempotency_key = "retry-after-key";
    CHECK(retry_manager.submit(std::move(retry_request), [&](trading::ApiResponse response) {
                           retry_completion.complete(std::move(response));
                       }).accepted());
    retry_manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(retry_completion.wait_for(10ms));
    const auto retry_starts = retry_client->starts();
    CHECK(retry_starts.size() == 2U);
    CHECK(retry_starts[1] - retry_starts[0] >= 35ms);
    CHECK(retry_completion.responses().front().ok());
    CHECK(retry_completion.responses().front().attempts == 2U);

    auto timeout_client = std::make_shared<trading::MockTradingApi>(
        trading::MockApiConfig{50ms, 0, 0ms, 0});
    trading::ApiManagerConfig timeout_config;
    timeout_config.rate_limit = {100, 1ms};
    timeout_config.io_threads = 1;
    timeout_config.max_outstanding_requests = 2;
    timeout_config.max_retries = 0;
    trading::ApiRequestManager timeout_manager(timeout_client, timeout_config);
    CompletionGroup timeout_completion(1);
    trading::ApiRequest timeout_request;
    timeout_request.endpoint = "/timeout";
    timeout_request.timeout = 10ms;
    CHECK(timeout_manager.submit(std::move(timeout_request), [&](trading::ApiResponse response) {
                             timeout_completion.complete(std::move(response));
                         }).accepted());
    timeout_manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(timeout_completion.wait_for(10ms));
    CHECK(timeout_completion.responses().front().error == trading::ApiError::timeout);
}

void cancellation_and_shutdown_are_bounded() {
    auto client = std::make_shared<RecordingClient>(5s);
    trading::ApiManagerConfig config;
    config.rate_limit = {100, 1s};
    config.io_threads = 2;
    config.max_outstanding_requests = 16;
    config.max_retries = 0;
    trading::ApiRequestManager manager(client, config);
    CompletionGroup completions(10);

    for (std::size_t index = 0; index < 10; ++index) {
        trading::ApiRequest request;
        request.endpoint = "/slow";
        request.timeout = 10s;
        CHECK(manager.submit(std::move(request), [&](trading::ApiResponse response) {
                         completions.complete(std::move(response));
                     }).accepted());
    }
    CHECK(client->wait_for_starts(2, 100ms));

    const auto shutdown_started = trading::MonotonicClock::now();
    manager.shutdown(trading::ApiShutdownMode::cancel_pending);
    const auto shutdown_duration = trading::MonotonicClock::now() - shutdown_started;
    CHECK(shutdown_duration < 500ms);
    CHECK(completions.wait_for(10ms));
    CHECK(manager.stats().cancelled == 10U);

    trading::EventBus bus;
    trading::EventWorker idle("idle", bus, 4, [](const trading::EventEnvelope&) {});
    idle.subscribe<trading::BenchmarkEvent>();
    idle.start();
    const auto worker_stop_started = trading::MonotonicClock::now();
    idle.stop();
    CHECK(trading::MonotonicClock::now() - worker_stop_started < 200ms);
}

}  // namespace

int main() {
    const std::vector<std::pair<std::string, std::function<void()>>> tests{
        {"messaging fan-out and filtering", messaging_delivers_to_every_matching_subscriber},
        {"concurrent publication", messaging_is_safe_with_concurrent_publishers},
        {"bounded mailbox backpressure", mailbox_backpressure_is_explicit},
        {"shared rate limiting", shared_rate_limit_applies_across_submitters},
        {"asynchronous API isolation", slow_api_does_not_block_unrelated_event_processing},
        {"thread-safe position state", position_book_is_thread_safe},
        {"idempotency-gated retries", retry_policy_requires_idempotency},
        {"Retry-After and timeout handling", retry_after_and_timeouts_are_honoured},
        {"bounded cancellation and shutdown", cancellation_and_shutdown_are_bounded},
    };

    std::size_t failures = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& exception) {
            ++failures;
            std::cerr << "[FAIL] " << name << ": " << exception.what() << '\n';
        } catch (...) {
            ++failures;
            std::cerr << "[FAIL] " << name << ": unknown exception\n";
        }
    }

    std::cout << tests.size() - failures << '/' << tests.size() << " tests passed\n";
    return failures == 0U ? 0 : 1;
}
