#include "trading/strategies.hpp"

#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace trading {

ThresholdStrategy::ThresholdStrategy(ThresholdStrategyConfig config) : config_(config) {
    if (config_.band_bps < 0.0) {
        throw std::invalid_argument("threshold band must not be negative");
    }
}

std::optional<Side> ThresholdStrategy::on_market_data(const MarketDataEvent& quote) {
    const auto [entry, inserted] = symbols_.try_emplace(quote.symbol, SymbolState{quote.price, {}});
    if (inserted) {
        return std::nullopt;
    }
    auto& state = entry->second;
    const auto band = state.anchor * config_.band_bps / 10'000.0;
    if (quote.price >= state.anchor + band && state.last_signal != Side::buy) {
        state.last_signal = Side::buy;
        return Side::buy;
    }
    if (quote.price <= state.anchor - band && state.last_signal != Side::sell) {
        state.last_signal = Side::sell;
        return Side::sell;
    }
    return std::nullopt;
}

EmaCrossStrategy::EmaCrossStrategy(EmaCrossStrategyConfig config)
    : config_(config),
      fast_alpha_(2.0 / (static_cast<double>(config.fast_period) + 1.0)),
      slow_alpha_(2.0 / (static_cast<double>(config.slow_period) + 1.0)) {
    if (config_.fast_period == 0U || config_.slow_period <= config_.fast_period ||
        config_.min_gap_bps < 0.0) {
        throw std::invalid_argument("EMA periods must satisfy 0 < fast < slow");
    }
}

std::optional<Side> EmaCrossStrategy::on_market_data(const MarketDataEvent& quote) {
    auto& state = symbols_[quote.symbol];
    if (state.observations++ == 0U) {
        state.fast = quote.price;
        state.slow = quote.price;
        return std::nullopt;
    }
    state.fast += fast_alpha_ * (quote.price - state.fast);
    state.slow += slow_alpha_ * (quote.price - state.slow);
    if (state.observations < config_.slow_period || state.slow == 0.0) {
        return std::nullopt;
    }

    const auto gap_bps = (state.fast - state.slow) / state.slow * 10'000.0;
    const int regime = gap_bps > config_.min_gap_bps ? 1 : (gap_bps < -config_.min_gap_bps ? -1 : 0);
    if (regime == 0 || regime == state.regime) {
        return std::nullopt;
    }
    const bool first_regime = state.regime == 0;
    state.regime = regime;
    if (first_regime) {
        return std::nullopt;
    }
    return regime > 0 ? Side::buy : Side::sell;
}

SimulatedLoadStrategy::SimulatedLoadStrategy(std::unique_ptr<ISignalStrategy> inner)
    : inner_(std::move(inner)) {
    if (!inner_) {
        throw std::invalid_argument("wrapped strategy must not be null");
    }
}

std::optional<Side> SimulatedLoadStrategy::on_market_data(const MarketDataEvent& quote) {
    const auto work = work_per_event();
    if (work > std::chrono::microseconds::zero()) {
        std::this_thread::sleep_for(work);
    }
    return inner_->on_market_data(quote);
}

void SimulatedLoadStrategy::set_work_per_event(std::chrono::microseconds work) {
    if (work < std::chrono::microseconds::zero()) {
        throw std::invalid_argument("simulated work must not be negative");
    }
    work_us_.store(work.count(), std::memory_order_relaxed);
}

std::chrono::microseconds SimulatedLoadStrategy::work_per_event() const noexcept {
    return std::chrono::microseconds(work_us_.load(std::memory_order_relaxed));
}

bool is_known_strategy_kind(std::string_view kind) noexcept {
    return kind == "threshold" || kind == "ema-cross";
}

std::unique_ptr<ISignalStrategy> make_strategy(std::string_view kind) {
    if (kind == "threshold") {
        return std::make_unique<ThresholdStrategy>();
    }
    if (kind == "ema-cross") {
        return std::make_unique<EmaCrossStrategy>();
    }
    throw std::invalid_argument("unknown strategy kind: " + std::string(kind) +
                                " (expected threshold or ema-cross)");
}

}  // namespace trading
