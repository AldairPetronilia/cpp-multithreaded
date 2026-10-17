#include "trading/gateway_pool.hpp"

#include <algorithm>
#include <cmath>
#include <functional>
#include <stdexcept>
#include <stop_token>

namespace trading {

const char* to_string(GatewayState state) noexcept {
    switch (state) {
        case GatewayState::closed:
            return "closed";
        case GatewayState::open:
            return "open";
        case GatewayState::half_open:
            return "half-open";
    }
    return "closed";
}

FaultableApiClient::FaultableApiClient(std::shared_ptr<IApiClient> inner) : inner_(std::move(inner)) {
    if (!inner_) {
        throw std::invalid_argument("faultable client needs a client to wrap");
    }
}

ApiResponse FaultableApiClient::perform(const ApiRequest& request, std::stop_token cancellation) {
    if (down_.load(std::memory_order_relaxed)) {
        refused_.fetch_add(1, std::memory_order_relaxed);
        ApiResponse response;
        response.error = ApiError::connection_failure;
        response.message = "gateway unreachable (injected outage)";
        return response;
    }
    return inner_->perform(request, std::move(cancellation));
}

OrderLookupResult FaultableApiClient::lookup(const std::string& idempotency_key) {
    return inner_->lookup(idempotency_key);
}

struct GatewayPool::Operation {
    ApiRequest request;
    ApiCompletion completion;
    std::size_t reroutes{0};
    // The handle the caller holds is the first attempt's. Once the request moves to another
    // gateway, cancelling that handle has to cancel the attempt standing in for it.
    RequestHandle caller;
    RequestHandle current;
    std::unique_ptr<std::stop_callback<std::function<void()>>> forward_cancel;

    // Call with the pool's mutex held, once both handles are known.
    void link_cancellation() {
        if (caller.valid() && current.valid()) {
            forward_cancel = std::make_unique<std::stop_callback<std::function<void()>>>(
                caller.token(), [attempt = current] { attempt.cancel(); });
        }
    }
};

GatewayPool::GatewayPool(std::vector<Member> members, GatewayPoolConfig config, std::uint64_t jitter_seed)
    : members_(std::move(members)),
      config_(config),
      breakers_(members_.size()),
      random_(jitter_seed),
      budget_(config.retry_budget_cap / 2.0) {
    if (members_.empty() || members_.size() > 32U) {
        throw std::invalid_argument("a gateway pool needs between 1 and 32 gateways");
    }
    for (const auto& member : members_) {
        if (!member.manager) {
            throw std::invalid_argument("gateway " + member.name + " has no request manager");
        }
    }
    if (config_.failure_threshold == 0U || config_.probe_successes == 0U || config_.base_ejection.count() <= 0 ||
        config_.max_ejection < config_.base_ejection || config_.retry_budget_ratio < 0.0 ||
        config_.retry_budget_cap < 0.0 || config_.ejection_jitter < 0.0 || config_.ejection_jitter >= 1.0) {
        throw std::invalid_argument("invalid gateway pool configuration");
    }
}

GatewayPool::~GatewayPool() {
    shutdown(ApiShutdownMode::cancel_pending);
}

GatewayPool::Outcome GatewayPool::classify(const ApiResponse& response) noexcept {
    if (response.ok()) {
        return Outcome::success;
    }
    // Rate limiting is the gateway's own business, and a cancellation is ours.
    if (response.error == ApiError::cancelled || response.error == ApiError::rate_limited ||
        response.status_code == 429) {
        return Outcome::neutral;
    }
    if (response.error == ApiError::connection_failure || response.error == ApiError::timeout ||
        response.error == ApiError::internal_error || response.status_code >= 500) {
        return Outcome::hard_failure;
    }
    return Outcome::neutral;
}

void GatewayPool::promote_locked(std::chrono::steady_clock::time_point now) {
    for (auto& breaker : breakers_) {
        if (breaker.state == GatewayState::open && now >= breaker.reopen_at) {
            breaker.state = GatewayState::half_open;
            breaker.probe_successes = 0;
            breaker.probe_in_flight = false;
            breaker.next_probe = now;
        }
    }
}

std::optional<GatewayPool::Choice> GatewayPool::choose_locked(std::uint32_t excluded) {
    const auto now = std::chrono::steady_clock::now();
    promote_locked(now);
    const auto allowed = [&](std::size_t index) { return (excluded & (1U << index)) == 0U; };

    // A returning gateway gets a trickle of real traffic, one probe per interval.
    for (std::size_t index = 0; index < breakers_.size(); ++index) {
        auto& breaker = breakers_[index];
        if (allowed(index) && breaker.state == GatewayState::half_open && !breaker.probe_in_flight &&
            now >= breaker.next_probe) {
            breaker.probe_in_flight = true;
            breaker.next_probe = now + config_.probe_interval;
            return Choice{index, true};
        }
    }

    // The first closed gateway, or with spreading the next one round the closed ones.
    std::size_t closed = 0;
    for (std::size_t index = 0; index < breakers_.size(); ++index) {
        if (allowed(index) && breakers_[index].state == GatewayState::closed) {
            if (!config_.spread) {
                return Choice{index, false};
            }
            ++closed;
        }
    }
    if (closed > 0U) {
        auto skip = round_robin_++ % closed;
        for (std::size_t index = 0; index < breakers_.size(); ++index) {
            if (allowed(index) && breakers_[index].state == GatewayState::closed && skip-- == 0U) {
                return Choice{index, false};
            }
        }
    }

    // Nothing is closed. Fail static: use the gateway that will reopen soonest rather than
    // refusing every order.
    std::optional<std::size_t> best;
    for (std::size_t index = 0; index < breakers_.size(); ++index) {
        if (!allowed(index)) {
            continue;
        }
        if (!best || breakers_[index].reopen_at < breakers_[*best].reopen_at) {
            best = index;
        }
    }
    if (best) {
        return Choice{*best, false};
    }
    return std::nullopt;
}

std::optional<std::size_t> GatewayPool::preferred_locked() const {
    for (std::size_t index = 0; index < breakers_.size(); ++index) {
        if (breakers_[index].state == GatewayState::closed) {
            return index;
        }
    }
    return std::nullopt;
}

bool GatewayPool::other_available_locked(std::size_t member) const {
    for (std::size_t index = 0; index < breakers_.size(); ++index) {
        if (index != member && breakers_[index].state != GatewayState::open) {
            return true;
        }
    }
    return false;
}

void GatewayPool::eject_locked(std::size_t member) {
    auto& breaker = breakers_[member];
    ++breaker.ejection_level;
    ++breaker.ejections;
    auto duration = static_cast<double>(config_.base_ejection.count());
    for (std::size_t step = 1; step < breaker.ejection_level && duration < static_cast<double>(config_.max_ejection.count());
         ++step) {
        duration *= 2.0;
    }
    duration = std::min(duration, static_cast<double>(config_.max_ejection.count()));
    std::uniform_real_distribution<double> jitter(1.0 - config_.ejection_jitter, 1.0 + config_.ejection_jitter);
    duration *= jitter(random_);
    breaker.state = GatewayState::open;
    breaker.consecutive_failures = 0;
    breaker.probe_in_flight = false;
    breaker.reopen_at = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(static_cast<std::int64_t>(std::max(1.0, duration)));
}

void GatewayPool::record_locked(std::size_t member, Outcome outcome, bool probe) {
    auto& breaker = breakers_[member];
    if (probe) {
        breaker.probe_in_flight = false;
    }
    if (outcome == Outcome::neutral) {
        return;
    }
    if (outcome == Outcome::success) {
        ++breaker.succeeded;
        breaker.consecutive_failures = 0;
        if (breaker.state == GatewayState::half_open && probe &&
            ++breaker.probe_successes >= config_.probe_successes) {
            breaker.state = GatewayState::closed;
            if (breaker.ejection_level > 0U) {
                --breaker.ejection_level;
            }
        }
        return;
    }
    ++breaker.failed;
    ++breaker.consecutive_failures;
    if (breaker.state == GatewayState::half_open && probe) {
        eject_locked(member);
    } else if (breaker.state == GatewayState::closed && breaker.consecutive_failures >= config_.failure_threshold &&
               other_available_locked(member)) {
        eject_locked(member);
    }
}

SubmitResult GatewayPool::submit(ApiRequest request, ApiCompletion completion) {
    if (!completion) {
        throw std::invalid_argument("API completion callback must not be empty");
    }
    auto operation = std::make_shared<Operation>();
    operation->request = std::move(request);
    operation->completion = std::move(completion);
    {
        std::lock_guard lock(mutex_);
        budget_ = std::min(config_.retry_budget_cap, budget_ + config_.retry_budget_ratio);
    }
    auto result = dispatch(operation, 0U);
    if (result.accepted()) {
        // The first attempt may already have moved on by now; link whichever comes second.
        std::lock_guard lock(mutex_);
        operation->caller = result.handle;
        if (operation->reroutes > 0U) {
            operation->link_cancellation();
        }
    }
    return result;
}

SubmitResult GatewayPool::dispatch(const std::shared_ptr<Operation>& operation, std::uint32_t excluded) {
    SubmitResult last{SubmitStatus::shutting_down, {}};
    for (std::size_t attempts = 0; attempts < members_.size(); ++attempts) {
        std::optional<Choice> choice;
        {
            std::lock_guard lock(mutex_);
            choice = choose_locked(excluded);
            if (choice) {
                ++breakers_[choice->index].submitted;
            }
        }
        if (!choice) {
            break;
        }
        const auto member = choice->index;
        const auto probe = choice->probe;
        auto result = members_[member].manager->submit(
            operation->request, [this, operation, member, probe](ApiResponse response) {
                attempt_done(operation, member, probe, std::move(response));
            });
        if (result.accepted()) {
            return result;
        }
        // This gateway cannot take the request (queue full or shutting down): try another.
        {
            std::lock_guard lock(mutex_);
            if (probe) {
                breakers_[member].probe_in_flight = false;
            }
            --breakers_[member].submitted;
        }
        last = result;
        excluded |= 1U << member;
    }
    return last;
}

void GatewayPool::attempt_done(const std::shared_ptr<Operation>& operation, std::size_t member, bool probe,
                               ApiResponse response) {
    const auto outcome = classify(response);
    std::optional<std::uint32_t> reroute_excluding;
    // Only a request the broker can deduplicate may be sent again: a timeout or a dropped
    // connection does not say whether the first attempt executed.
    const bool safe_to_resend = operation->request.idempotent && !operation->request.idempotency_key.empty();
    {
        std::lock_guard lock(mutex_);
        record_locked(member, outcome, probe);
        const bool cancelled = operation->caller.valid() && operation->caller.token().stop_requested();
        if (outcome == Outcome::hard_failure && safe_to_resend && !cancelled &&
            operation->reroutes < config_.max_reroutes) {
            // A failed probe is the pool's own experiment and costs the order nothing. Any
            // other re-route is paid for from the pool-wide budget.
            if (probe || budget_ >= 1.0) {
                if (!probe) {
                    budget_ -= 1.0;
                }
                reroute_excluding = 1U << member;
            } else {
                ++reroutes_denied_;
            }
        }
    }
    if (reroute_excluding) {
        {
            std::lock_guard lock(mutex_);
            ++operation->reroutes;
        }
        const auto moved = dispatch(operation, *reroute_excluding);
        if (moved.accepted()) {
            std::lock_guard lock(mutex_);
            ++reroutes_;
            ++breakers_[member].rerouted_away;
            operation->current = moved.handle;
            operation->link_cancellation();
            return;
        }
    }
    operation->completion(std::move(response));
}

OrderLookupResult GatewayPool::lookup(const std::string& idempotency_key) {
    OrderLookupResult best;
    for (const auto& member : members_) {
        auto found = member.manager->lookup(idempotency_key);
        if (found.state == OrderLookup::in_flight) {
            return found;
        }
        if (found.state == OrderLookup::completed) {
            best = std::move(found);
        }
    }
    return best;
}

void GatewayPool::shutdown(ApiShutdownMode mode, std::optional<std::chrono::milliseconds> drain_timeout) {
    const auto deadline = drain_timeout ? std::optional(std::chrono::steady_clock::now() + *drain_timeout)
                                        : std::nullopt;
    for (auto& member : members_) {
        std::optional<std::chrono::milliseconds> remaining;
        if (deadline) {
            remaining = std::max(std::chrono::milliseconds(0),
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                     *deadline - std::chrono::steady_clock::now()));
        }
        member.manager->shutdown(mode, remaining);
    }
}

bool GatewayPool::set_down(std::size_t index, bool down) {
    if (index >= members_.size() || !members_[index].fault) {
        return false;
    }
    members_[index].fault->set_down(down);
    return true;
}

ApiManagerStats GatewayPool::stats() const {
    ApiManagerStats total;
    std::optional<std::size_t> preferred;
    std::uint64_t rerouted = 0;
    {
        std::lock_guard lock(mutex_);
        preferred = preferred_locked();
        rerouted = reroutes_;
    }
    for (std::size_t index = 0; index < members_.size(); ++index) {
        const auto stats = members_[index].manager->stats();
        total.accepted += stats.accepted;
        total.rejected_full += stats.rejected_full;
        total.rejected_shutdown += stats.rejected_shutdown;
        total.completed += stats.completed;
        total.failed += stats.failed;
        total.cancelled += stats.cancelled;
        total.retries += stats.retries;
        total.rate_limited_responses += stats.rate_limited_responses;
        total.timeouts += stats.timeouts;
        total.connection_failures += stats.connection_failures;
        total.outstanding += stats.outstanding;
        total.pending += stats.pending;
        total.active += stats.active;
        total.max_pending = std::max(total.max_pending, stats.max_pending);
        total.capacity += stats.capacity;
        total.io_threads += stats.io_threads;
        if (index == preferred.value_or(0U)) {
            total.limiter = stats.limiter;
        }
    }
    // A request that moved to another gateway was accepted twice and failed once on the way
    // to succeeding; count operations, not attempts.
    total.accepted -= std::min(total.accepted, rerouted);
    total.failed -= std::min(total.failed, rerouted);
    return total;
}

std::vector<GatewayStatus> GatewayPool::gateways() const {
    std::vector<GatewayStatus> result;
    std::lock_guard lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    const auto preferred = preferred_locked();
    for (std::size_t index = 0; index < members_.size(); ++index) {
        const auto& breaker = breakers_[index];
        GatewayStatus status;
        status.name = members_[index].name;
        status.state = breaker.state;
        // An open breaker whose time has passed is about to become half-open.
        if (breaker.state == GatewayState::open && now >= breaker.reopen_at) {
            status.state = GatewayState::half_open;
        }
        status.preferred = config_.spread ? breaker.state == GatewayState::closed : preferred == index;
        status.down_injected = members_[index].fault && members_[index].fault->down();
        status.submitted = breaker.submitted;
        status.succeeded = breaker.succeeded;
        status.failed = breaker.failed;
        status.rerouted_away = breaker.rerouted_away;
        status.ejections = breaker.ejections;
        status.consecutive_failures = breaker.consecutive_failures;
        if (breaker.state == GatewayState::open && breaker.reopen_at > now) {
            status.reopens_in = std::chrono::ceil<std::chrono::milliseconds>(breaker.reopen_at - now);
        }
        status.api = members_[index].manager->stats();
        result.push_back(std::move(status));
    }
    return result;
}

GatewayPoolStats GatewayPool::pool_stats() const {
    std::lock_guard lock(mutex_);
    GatewayPoolStats stats;
    stats.reroutes = reroutes_;
    stats.reroutes_denied = reroutes_denied_;
    stats.retry_budget = budget_;
    for (const auto& breaker : breakers_) {
        if (breaker.state != GatewayState::open) {
            ++stats.healthy;
        }
    }
    return stats;
}

}  // namespace trading
