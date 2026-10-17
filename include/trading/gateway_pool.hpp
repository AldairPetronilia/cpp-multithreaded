#pragma once

#include "trading/api.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace trading {

enum class GatewayState {
    closed,
    open,
    half_open,
};

[[nodiscard]] const char* to_string(GatewayState state) noexcept;

// Decorator that makes one gateway's connection fail on demand, for fault injection. The
// broker's own state is not affected, so a lookup still reaches it: an outage on one
// credential's path does not erase the orders the broker already holds.
class FaultableApiClient final : public IApiClient {
public:
    explicit FaultableApiClient(std::shared_ptr<IApiClient> inner);

    ApiResponse perform(const ApiRequest& request, std::stop_token cancellation) override;
    [[nodiscard]] OrderLookupResult lookup(const std::string& idempotency_key) override;

    void set_down(bool down) noexcept { down_.store(down, std::memory_order_relaxed); }
    [[nodiscard]] bool down() const noexcept { return down_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t refused() const noexcept { return refused_.load(std::memory_order_relaxed); }

private:
    std::shared_ptr<IApiClient> inner_;
    std::atomic<bool> down_{false};
    std::atomic<std::uint64_t> refused_{0};
};

struct GatewayPoolConfig {
    // false: every request goes to the first healthy gateway and the others stand by, so the
    // pool never offers more than one credential's rate limit. true: requests are spread
    // across all healthy gateways; use it only where the provider limits per credential.
    bool spread{false};
    // Consecutive failed requests (after the gateway's own retries) that eject a gateway.
    std::size_t failure_threshold{3};
    // Ejection time grows with repeated ejections: base * 2^(n-1), capped, with jitter.
    std::chrono::milliseconds base_ejection{500};
    std::chrono::milliseconds max_ejection{8'000};
    double ejection_jitter{0.2};
    // A returning gateway gets one probe request per interval and must succeed this many
    // times in a row before it takes full traffic again.
    std::size_t probe_successes{2};
    std::chrono::milliseconds probe_interval{100};
    // Moves one failed request may make to another gateway. The gateways already retry
    // inside themselves, so this is a re-route within one attempt budget, not another retry
    // layer.
    std::size_t max_reroutes{1};
    // Re-routes are paid for from a pool-wide budget that refills as a fraction of
    // submissions, so a failing fleet cannot multiply its own load.
    double retry_budget_ratio{0.2};
    double retry_budget_cap{20.0};
};

struct GatewayStatus {
    std::string name{};
    GatewayState state{GatewayState::closed};
    // Takes new requests now (the first closed gateway, or every closed one when spreading).
    bool preferred{false};
    bool down_injected{false};
    std::uint64_t submitted{};
    std::uint64_t succeeded{};
    std::uint64_t failed{};
    std::uint64_t rerouted_away{};
    std::uint64_t ejections{};
    std::size_t consecutive_failures{};
    std::chrono::milliseconds reopens_in{};
    ApiManagerStats api{};
};

struct GatewayPoolStats {
    std::uint64_t reroutes{};
    std::uint64_t reroutes_denied{};
    double retry_budget{};
    std::size_t healthy{};
};

// Several gateways, each with its own credential, limiter and I/O threads, behind the same
// IRequestManager interface the order path already uses.
//
// Failover is by request: a request that fails hard (connection failure, timeout, 5xx) on
// one gateway is sent once more, with the same idempotency key, to another. A gateway that
// keeps failing is ejected with exponential, jittered backoff; the last healthy gateway is
// never ejected, because a blip everywhere must not turn into an outage. HTTP 429 is not a
// failure: it means slow down that credential, which the gateway does by itself.
class GatewayPool final : public IRequestManager {
public:
    struct Member {
        std::string name;
        std::shared_ptr<ApiRequestManager> manager;
        std::shared_ptr<FaultableApiClient> fault;
    };

    GatewayPool(std::vector<Member> members, GatewayPoolConfig config, std::uint64_t jitter_seed = 1);
    ~GatewayPool() override;
    GatewayPool(const GatewayPool&) = delete;
    GatewayPool& operator=(const GatewayPool&) = delete;

    SubmitResult submit(ApiRequest request, ApiCompletion completion) override;
    [[nodiscard]] OrderLookupResult lookup(const std::string& idempotency_key) override;

    void shutdown(ApiShutdownMode mode = ApiShutdownMode::drain,
                  std::optional<std::chrono::milliseconds> drain_timeout = std::nullopt);

    // Sums over the gateways; the limiter shown is the preferred gateway's.
    [[nodiscard]] ApiManagerStats stats() const;
    [[nodiscard]] std::vector<GatewayStatus> gateways() const;
    [[nodiscard]] GatewayPoolStats pool_stats() const;
    // Fault injection; false if there is no such gateway.
    bool set_down(std::size_t index, bool down);
    [[nodiscard]] std::size_t size() const noexcept { return members_.size(); }

private:
    struct Operation;
    struct Breaker {
        GatewayState state{GatewayState::closed};
        std::size_t consecutive_failures{0};
        std::size_t probe_successes{0};
        std::size_t ejection_level{0};
        bool probe_in_flight{false};
        std::chrono::steady_clock::time_point reopen_at{};
        std::chrono::steady_clock::time_point next_probe{};
        std::uint64_t submitted{0};
        std::uint64_t succeeded{0};
        std::uint64_t failed{0};
        std::uint64_t rerouted_away{0};
        std::uint64_t ejections{0};
    };
    enum class Outcome { success, hard_failure, neutral };

    struct Choice {
        std::size_t index{};
        bool probe{};
    };

    SubmitResult dispatch(const std::shared_ptr<Operation>& operation, std::uint32_t excluded);
    void attempt_done(const std::shared_ptr<Operation>& operation, std::size_t member, bool probe,
                      ApiResponse response);
    [[nodiscard]] static Outcome classify(const ApiResponse& response) noexcept;
    std::optional<Choice> choose_locked(std::uint32_t excluded);
    void promote_locked(std::chrono::steady_clock::time_point now);
    void record_locked(std::size_t member, Outcome outcome, bool probe);
    void eject_locked(std::size_t member);
    [[nodiscard]] bool other_available_locked(std::size_t member) const;
    [[nodiscard]] std::optional<std::size_t> preferred_locked() const;

    std::vector<Member> members_;
    const GatewayPoolConfig config_;
    mutable std::mutex mutex_;
    std::vector<Breaker> breakers_;
    std::mt19937_64 random_;
    double budget_;
    std::size_t round_robin_{0};
    std::uint64_t reroutes_{0};
    std::uint64_t reroutes_denied_{0};
};

}  // namespace trading
