#pragma once

#include "trading/events.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <unordered_map>

namespace trading {

enum class ApiError {
    none,
    timeout,
    connection_failure,
    rate_limited,
    cancelled,
    internal_error,
};

[[nodiscard]] const char* to_string(ApiError error) noexcept;

struct ApiRequest {
    std::string method{"POST"};
    std::string endpoint{};
    std::string body{};
    std::string idempotency_key{};
    bool idempotent{false};
    std::chrono::milliseconds timeout{1'000};
};

struct ApiTiming {
    std::chrono::nanoseconds queue_delay{};
    std::chrono::nanoseconds rate_limit_wait{};
    std::chrono::nanoseconds service_time{};
    std::chrono::nanoseconds total_time{};
};

struct ApiResponse {
    int status_code{};
    ApiError error{ApiError::none};
    std::string body{};
    std::string message{};
    std::chrono::milliseconds retry_after{};
    std::size_t attempts{1};
    ApiTiming timing{};

    [[nodiscard]] bool ok() const noexcept {
        return error == ApiError::none && status_code >= 200 && status_code < 300;
    }
};

// What the broker side knows about an order identified by its idempotency key. Recovery
// asks before resubmitting, because a duplicate window is only as good as the venue's.
enum class OrderLookup {
    unknown,
    in_flight,
    completed,
};

struct OrderLookupResult {
    OrderLookup state{OrderLookup::unknown};
    // Valid when state is completed: the response the broker stored for the order.
    ApiResponse response{};
};

class IApiClient {
public:
    virtual ~IApiClient() = default;
    virtual ApiResponse perform(const ApiRequest& request, std::stop_token cancellation) = 0;
    // Clients without a status query report every order as unknown.
    [[nodiscard]] virtual OrderLookupResult lookup(const std::string& idempotency_key) {
        static_cast<void>(idempotency_key);
        return {};
    }
};

struct RateLimitConfig {
    std::size_t max_requests{10};
    std::chrono::milliseconds window{1'000};
};

struct ApiManagerConfig {
    RateLimitConfig rate_limit{};
    std::size_t io_threads{2};
    std::size_t max_outstanding_requests{1'024};
    std::size_t max_retries{2};
    std::chrono::milliseconds initial_retry_backoff{20};
    std::chrono::milliseconds max_retry_backoff{500};
    // A gateway in a pool turns this off: a connection that failed is better retried through
    // another gateway at once than through the same one after a backoff.
    bool retry_connection_failures{true};
};

enum class SubmitStatus {
    accepted,
    queue_full,
    shutting_down,
};

namespace detail {
struct RequestCancellation {
    std::stop_source source;
    std::weak_ptr<std::condition_variable> queue_notifier;
};
}  // namespace detail

class RequestHandle {
public:
    RequestHandle() = default;

    void cancel() const;
    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] std::uint64_t id() const noexcept;
    // Stops once the request is cancelled, so cancellation can be forwarded to another
    // request standing in for this one.
    [[nodiscard]] std::stop_token token() const noexcept;

private:
    friend class ApiRequestManager;
    RequestHandle(std::uint64_t id, std::shared_ptr<detail::RequestCancellation> cancellation);

    std::uint64_t id_{};
    std::shared_ptr<detail::RequestCancellation> cancellation_;
};

struct SubmitResult {
    SubmitStatus status{SubmitStatus::shutting_down};
    RequestHandle handle{};

    [[nodiscard]] bool accepted() const noexcept { return status == SubmitStatus::accepted; }
};

using ApiCompletion = std::function<void(ApiResponse)>;

class IRequestManager {
public:
    virtual ~IRequestManager() = default;
    virtual SubmitResult submit(ApiRequest request, ApiCompletion completion) = 0;
    // Reports whether a request with this idempotency key is queued or running here, or
    // already completed at the broker. Managers without that knowledge report unknown.
    [[nodiscard]] virtual OrderLookupResult lookup(const std::string& idempotency_key) {
        static_cast<void>(idempotency_key);
        return {};
    }
};

enum class ApiShutdownMode {
    drain,
    cancel_pending,
};

struct RateLimiterStats {
    std::size_t in_window{};
    std::size_t max_requests{};
    std::chrono::milliseconds window{};
    std::chrono::milliseconds paused_for{};
};

struct ApiManagerStats {
    std::uint64_t accepted{};
    std::uint64_t rejected_full{};
    std::uint64_t rejected_shutdown{};
    std::uint64_t completed{};
    std::uint64_t failed{};
    std::uint64_t cancelled{};
    std::uint64_t retries{};
    std::uint64_t rate_limited_responses{};
    std::uint64_t timeouts{};
    std::uint64_t connection_failures{};
    std::size_t outstanding{};
    std::size_t pending{};
    std::size_t active{};
    std::size_t max_pending{};
    std::size_t capacity{};
    std::size_t io_threads{};
    RateLimiterStats limiter{};
};

// The single gateway for every request made with the shared API token. submit() only
// enqueues; a small pool of I/O threads waits for rate-limit capacity, performs the call,
// retries when that is safe, and runs the completion on the I/O thread.
class ApiRequestManager final : public IRequestManager {
public:
    ApiRequestManager(std::shared_ptr<IApiClient> client, ApiManagerConfig config);
    ~ApiRequestManager() override;
    ApiRequestManager(const ApiRequestManager&) = delete;
    ApiRequestManager& operator=(const ApiRequestManager&) = delete;

    SubmitResult submit(ApiRequest request, ApiCompletion completion) override;
    [[nodiscard]] OrderLookupResult lookup(const std::string& idempotency_key) override;

    // Stops accepting requests and waits for accepted ones to finish. In drain mode,
    // requests still outstanding after drain_timeout (if given) are cancelled.
    void shutdown(ApiShutdownMode mode = ApiShutdownMode::drain,
                  std::optional<std::chrono::milliseconds> drain_timeout = std::nullopt);
    [[nodiscard]] ApiManagerStats stats() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

struct MockApiConfig {
    std::chrono::milliseconds latency{5};
    std::size_t respond_429_every{};
    std::chrono::milliseconds retry_after{25};
    std::size_t fail_connection_every{};
    // Remembered idempotency keys; the oldest are forgotten beyond this, as a real
    // broker forgets them after its deduplication window.
    std::size_t idempotency_capacity{16'384};
};

class MockTradingApi final : public IApiClient {
public:
    explicit MockTradingApi(MockApiConfig config = {});

    ApiResponse perform(const ApiRequest& request, std::stop_token cancellation) override;
    [[nodiscard]] OrderLookupResult lookup(const std::string& idempotency_key) override;
    [[nodiscard]] std::uint64_t invocation_count() const noexcept;

    // Runtime fault injection for the dashboard; takes effect on the next call.
    void set_latency(std::chrono::milliseconds latency);
    void set_respond_429_every(std::size_t every_nth);
    [[nodiscard]] std::chrono::milliseconds latency() const noexcept;
    [[nodiscard]] std::size_t respond_429_every() const noexcept;

private:
    void remember(const std::string& key, const ApiResponse& response);

    const MockApiConfig config_;
    std::atomic<std::int64_t> latency_ms_;
    std::atomic<std::size_t> respond_429_every_;
    std::atomic<std::uint64_t> invocation_count_{0};
    std::mutex idempotency_mutex_;
    std::unordered_map<std::string, ApiResponse> completed_idempotent_requests_;
    std::deque<std::string> idempotency_order_;
};

}  // namespace trading
