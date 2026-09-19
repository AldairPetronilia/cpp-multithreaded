#pragma once

#include "trading/events.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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
    std::string endpoint;
    std::string body;
    std::string idempotency_key;
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
    std::string body;
    std::string message;
    std::chrono::milliseconds retry_after{};
    std::size_t attempts{1};
    ApiTiming timing;

    [[nodiscard]] bool ok() const noexcept {
        return error == ApiError::none && status_code >= 200 && status_code < 300;
    }
};

class IApiClient {
public:
    virtual ~IApiClient() = default;
    virtual ApiResponse perform(const ApiRequest& request,
                                std::stop_token cancellation) = 0;
};

struct RateLimitConfig {
    std::size_t max_requests{10};
    std::chrono::milliseconds window{1'000};
};

struct ApiManagerConfig {
    RateLimitConfig rate_limit;
    std::size_t io_threads{2};
    std::size_t max_outstanding_requests{1'024};
    std::size_t max_retries{2};
    std::chrono::milliseconds initial_retry_backoff{20};
    std::chrono::milliseconds max_retry_backoff{500};
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

private:
    friend class ApiRequestManager;
    RequestHandle(std::uint64_t id,
                  std::shared_ptr<detail::RequestCancellation> cancellation);

    std::uint64_t id_{};
    std::shared_ptr<detail::RequestCancellation> cancellation_;
};

struct SubmitResult {
    SubmitStatus status{SubmitStatus::shutting_down};
    RequestHandle handle;

    [[nodiscard]] bool accepted() const noexcept {
        return status == SubmitStatus::accepted;
    }
};

using ApiCompletion = std::function<void(ApiResponse)>;

class IRequestManager {
public:
    virtual ~IRequestManager() = default;
    virtual SubmitResult submit(ApiRequest request, ApiCompletion completion) = 0;
};

enum class ApiShutdownMode {
    drain,
    cancel_pending,
};

struct ApiManagerStats {
    std::uint64_t accepted{};
    std::uint64_t rejected_full{};
    std::uint64_t rejected_shutdown{};
    std::uint64_t completed{};
    std::uint64_t failed{};
    std::uint64_t cancelled{};
    std::uint64_t retries{};
    std::size_t outstanding{};
    std::size_t pending{};
    std::size_t active{};
    std::size_t max_pending{};
};

class ApiRequestManager final : public IRequestManager {
public:
    ApiRequestManager(std::shared_ptr<IApiClient> client, ApiManagerConfig config);
    ~ApiRequestManager() override;
    ApiRequestManager(const ApiRequestManager&) = delete;
    ApiRequestManager& operator=(const ApiRequestManager&) = delete;

    SubmitResult submit(ApiRequest request, ApiCompletion completion) override;
    void shutdown(ApiShutdownMode mode = ApiShutdownMode::drain);
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
};

class MockTradingApi final : public IApiClient {
public:
    explicit MockTradingApi(MockApiConfig config = {});

    ApiResponse perform(const ApiRequest& request,
                        std::stop_token cancellation) override;
    [[nodiscard]] std::uint64_t invocation_count() const noexcept;

private:
    MockApiConfig config_;
    std::atomic<std::uint64_t> invocation_count_{0};
    std::mutex idempotency_mutex_;
    std::unordered_map<std::string, ApiResponse> completed_idempotent_requests_;
};

}  // namespace trading

