#pragma once

#include <chrono>
#include <concepts>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>

namespace trading {

using MonotonicClock = std::chrono::steady_clock;
using TimePoint = MonotonicClock::time_point;

// Events are plain aggregates published once and shared immutably by every subscriber, so
// they need no common base class or virtual destructor. The name is used for diagnostics.
template <typename T>
concept BusEvent = std::is_class_v<T> && requires {
    { T::kEventName } -> std::convertible_to<std::string_view>;
};

// A topic partitions one event type into independently subscribable streams, such as one
// market-data stream per instrument. Topics are interned integers so that routing never
// hashes or compares strings on the publish path. Topic{} is the untagged stream.
struct Topic {
    std::uint32_t id{0};

    friend constexpr bool operator==(Topic, Topic) noexcept = default;
};

enum class Side : std::uint8_t {
    buy,
    sell,
};

[[nodiscard]] constexpr const char* to_string(Side side) noexcept {
    return side == Side::buy ? "BUY" : "SELL";
}

struct MarketDataEvent {
    static constexpr std::string_view kEventName = "MarketData";

    std::string symbol{};
    double price{};
    std::uint64_t sequence{};
    TimePoint source_time{};
};

struct TradingSignalEvent {
    static constexpr std::string_view kEventName = "TradingSignal";

    std::string strategy{};
    std::uint64_t signal_id{};
    std::string symbol{};
    Side side{Side::buy};
    double quantity{};
    double reference_price{};
    TimePoint market_data_time{};
    TimePoint signal_time{};
};

struct ExecutionReportEvent {
    static constexpr std::string_view kEventName = "ExecutionReport";

    std::string strategy{};
    std::uint64_t signal_id{};
    std::string client_order_id{};
    std::string symbol{};
    Side side{Side::buy};
    double quantity{};
    double fill_price{};
    bool filled{};
    std::string message{};
    TimePoint market_data_time{};
    TimePoint execution_time{};
    std::chrono::nanoseconds api_queue_delay{};
    std::chrono::nanoseconds rate_limit_wait{};
    std::chrono::nanoseconds api_service_time{};
};

struct BenchmarkEvent {
    static constexpr std::string_view kEventName = "Benchmark";

    std::uint64_t producer_id{};
    std::uint64_t sequence{};
};

struct EventEnvelope {
    std::type_index type{typeid(void)};
    Topic topic{};
    TimePoint published_at{};
    std::shared_ptr<const void> payload{};

    template <BusEvent E>
    [[nodiscard]] bool is() const noexcept {
        return type == std::type_index(typeid(E));
    }

    // Borrowed pointer: valid while this envelope, or a copy of it, is alive. Handlers read
    // events without touching the shared reference count.
    template <BusEvent E>
    [[nodiscard]] const E* as() const noexcept {
        return is<E>() ? static_cast<const E*>(payload.get()) : nullptr;
    }
};

}  // namespace trading
