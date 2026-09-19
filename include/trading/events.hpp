#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <typeindex>
#include <utility>

namespace trading {

using MonotonicClock = std::chrono::steady_clock;
using TimePoint = MonotonicClock::time_point;

class Event {
public:
    virtual ~Event() = default;
};

enum class Side {
    buy,
    sell,
};

[[nodiscard]] inline const char* to_string(Side side) noexcept {
    return side == Side::buy ? "BUY" : "SELL";
}

struct MarketDataEvent final : Event {
    std::string symbol;
    double price{};
    std::uint64_t source_sequence{};
    TimePoint source_time{};

    MarketDataEvent(std::string symbol_value,
                    double price_value,
                    std::uint64_t sequence_value,
                    TimePoint source_time_value = MonotonicClock::now())
        : symbol(std::move(symbol_value)),
          price(price_value),
          source_sequence(sequence_value),
          source_time(source_time_value) {}
};

struct TradingSignalEvent final : Event {
    std::uint64_t signal_id{};
    std::string symbol;
    Side side{Side::buy};
    double quantity{};
    double reference_price{};
    TimePoint market_data_time{};
    TimePoint signal_time{};

    TradingSignalEvent(std::uint64_t id,
                       std::string symbol_value,
                       Side side_value,
                       double quantity_value,
                       double price_value,
                       TimePoint market_time,
                       TimePoint signal_time_value = MonotonicClock::now())
        : signal_id(id),
          symbol(std::move(symbol_value)),
          side(side_value),
          quantity(quantity_value),
          reference_price(price_value),
          market_data_time(market_time),
          signal_time(signal_time_value) {}
};

struct ExecutionReportEvent final : Event {
    std::uint64_t signal_id{};
    std::string client_order_id;
    std::string symbol;
    Side side{Side::buy};
    double quantity{};
    double fill_price{};
    bool filled{};
    std::string message;
    TimePoint market_data_time{};
    TimePoint execution_time{};
    std::chrono::nanoseconds api_queue_delay{};
    std::chrono::nanoseconds rate_limit_wait{};
    std::chrono::nanoseconds api_service_time{};

    ExecutionReportEvent(std::uint64_t signal_id_value,
                         std::string order_id,
                         std::string symbol_value,
                         Side side_value,
                         double quantity_value,
                         double fill_price_value,
                         bool filled_value,
                         std::string message_value,
                         TimePoint market_time,
                         TimePoint execution_time_value,
                         std::chrono::nanoseconds queue_delay = {},
                         std::chrono::nanoseconds limiter_wait = {},
                         std::chrono::nanoseconds service_time = {})
        : signal_id(signal_id_value),
          client_order_id(std::move(order_id)),
          symbol(std::move(symbol_value)),
          side(side_value),
          quantity(quantity_value),
          fill_price(fill_price_value),
          filled(filled_value),
          message(std::move(message_value)),
          market_data_time(market_time),
          execution_time(execution_time_value),
          api_queue_delay(queue_delay),
          rate_limit_wait(limiter_wait),
          api_service_time(service_time) {}
};

struct BenchmarkEvent final : Event {
    std::uint64_t producer_id{};
    std::uint64_t sequence{};

    BenchmarkEvent(std::uint64_t producer, std::uint64_t sequence_value)
        : producer_id(producer), sequence(sequence_value) {}
};

struct EventEnvelope {
    std::type_index type{typeid(void)};
    std::uint64_t bus_sequence{};
    TimePoint published_at{};
    std::shared_ptr<const Event> payload;

    template <typename EventType>
    [[nodiscard]] bool is() const noexcept {
        return type == std::type_index(typeid(EventType));
    }

    template <typename EventType>
    [[nodiscard]] std::shared_ptr<const EventType> as() const noexcept {
        if (!is<EventType>()) {
            return {};
        }
        return std::static_pointer_cast<const EventType>(payload);
    }
};

}  // namespace trading

