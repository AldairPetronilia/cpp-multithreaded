#pragma once

#include "trading/events.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace trading {

// Signal logic, free of threads and messaging. An instance is owned by exactly one
// SignalProcessor and only called on that processor's thread, so implementations keep
// per-symbol state without synchronisation.
class ISignalStrategy {
public:
    virtual ~ISignalStrategy() = default;
    [[nodiscard]] virtual std::optional<Side> on_market_data(const MarketDataEvent& quote) = 0;
    [[nodiscard]] virtual std::string_view kind() const noexcept = 0;
};

struct ThresholdStrategyConfig {
    double band_bps{12.0};
};

// Buys when a price breaks band_bps above its anchor (the first price seen for the symbol)
// and sells when it breaks the same distance below. Signals alternate, so noise inside the
// band cannot produce a storm of repeated orders.
class ThresholdStrategy final : public ISignalStrategy {
public:
    explicit ThresholdStrategy(ThresholdStrategyConfig config = {});

    [[nodiscard]] std::optional<Side> on_market_data(const MarketDataEvent& quote) override;
    [[nodiscard]] std::string_view kind() const noexcept override { return "threshold"; }

private:
    struct SymbolState {
        double anchor{};
        std::optional<Side> last_signal;
    };

    ThresholdStrategyConfig config_;
    std::unordered_map<std::string, SymbolState> symbols_;
};

struct EmaCrossStrategyConfig {
    std::size_t fast_period{150};
    std::size_t slow_period{600};
    // The EMAs must separate by this much before a cross counts, which filters whipsaws.
    double min_gap_bps{1.0};
};

// Moving-average crossover: buys when the fast EMA moves above the slow EMA and sells on
// the opposite cross. The first regime after warm-up is observed, not traded.
class EmaCrossStrategy final : public ISignalStrategy {
public:
    explicit EmaCrossStrategy(EmaCrossStrategyConfig config = {});

    [[nodiscard]] std::optional<Side> on_market_data(const MarketDataEvent& quote) override;
    [[nodiscard]] std::string_view kind() const noexcept override { return "ema-cross"; }

private:
    struct SymbolState {
        double fast{};
        double slow{};
        std::size_t observations{};
        int regime{};
    };

    EmaCrossStrategyConfig config_;
    double fast_alpha_;
    double slow_alpha_;
    std::unordered_map<std::string, SymbolState> symbols_;
};

// Demo-only decorator that adds processing time per event, so the dashboard can show one
// slow consumer falling behind while the rest of the pipeline is unaffected.
class SimulatedLoadStrategy final : public ISignalStrategy {
public:
    explicit SimulatedLoadStrategy(std::unique_ptr<ISignalStrategy> inner);

    [[nodiscard]] std::optional<Side> on_market_data(const MarketDataEvent& quote) override;
    [[nodiscard]] std::string_view kind() const noexcept override { return inner_->kind(); }

    void set_work_per_event(std::chrono::microseconds work);
    [[nodiscard]] std::chrono::microseconds work_per_event() const noexcept;

private:
    std::unique_ptr<ISignalStrategy> inner_;
    std::atomic<std::int64_t> work_us_{0};
};

[[nodiscard]] bool is_known_strategy_kind(std::string_view kind) noexcept;
// Creates a strategy with default parameters; throws std::invalid_argument for unknown kinds.
[[nodiscard]] std::unique_ptr<ISignalStrategy> make_strategy(std::string_view kind);

}  // namespace trading
