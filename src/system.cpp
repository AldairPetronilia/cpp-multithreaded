#include "trading/system.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

namespace trading {
namespace {

using namespace std::chrono_literals;

void validate(const SystemConfig& config) {
    if (config.feeds.empty()) {
        throw std::invalid_argument("at least one market-data feed is required");
    }
    std::set<std::string> feed_names;
    std::set<std::string> symbols;
    for (const auto& feed : config.feeds) {
        if (feed.name.empty() || !feed_names.insert(feed.name).second) {
            throw std::invalid_argument("feed names must be unique and non-empty");
        }
        for (const auto& instrument : feed.instruments) {
            // One publisher per topic keeps every instrument's quotes in sequence order.
            if (!symbols.insert(instrument.symbol).second) {
                throw std::invalid_argument("instrument " + instrument.symbol +
                                            " is published by more than one feed");
            }
        }
    }

    std::set<std::string> strategy_names;
    for (const auto& strategy : config.strategies) {
        if (strategy.name.empty() || !strategy_names.insert(strategy.name).second) {
            throw std::invalid_argument("strategy names must be unique and non-empty");
        }
        if (!is_known_strategy_kind(strategy.kind)) {
            throw std::invalid_argument("strategy " + strategy.name + " has unknown kind " +
                                        strategy.kind);
        }
        for (const auto& symbol : strategy.symbols) {
            if (!symbols.contains(symbol)) {
                throw std::invalid_argument("strategy " + strategy.name +
                                            " subscribes to unknown instrument " + symbol);
            }
        }
    }
}

template <typename Components, typename Action>
bool for_each_named(Components& components, std::string_view name, Action action) {
    bool matched = false;
    for (auto& component : components) {
        if (name == "*" || component->name() == name) {
            action(*component);
            matched = true;
        }
    }
    return matched;
}

}  // namespace

SystemConfig SystemConfig::demo() {
    SystemConfig config;
    config.feeds = {
        MarketDataProducerConfig{
            .name = "equities",
            .instruments = {{"AAPL", 190.0, 1.0}, {"MSFT", 420.0, 1.0}, {"NVDA", 120.0, 1.0}},
            .seed = 7,
        },
        MarketDataProducerConfig{
            .name = "fx",
            .instruments = {{"EURUSD", 1.085, 1.0}, {"GBPUSD", 1.27, 1.0}},
            .seed = 11,
        },
    };
    config.strategies = {
        StrategyConfig{"trend-tech", "ema-cross", {"AAPL", "MSFT", "NVDA"}, 10.0},
        StrategyConfig{"trend-fx", "ema-cross", {"EURUSD", "GBPUSD"}, 10'000.0},
        StrategyConfig{"levels", "threshold", {"AAPL", "EURUSD"}, 100.0},
    };
    return config;
}

TradingSystem::TradingSystem(SystemConfig config)
    : config_(std::move(config)), epoch_(MonotonicClock::now()) {
    validate(config_);

    mock_api_ = std::make_shared<MockTradingApi>(config_.mock_api);
    api_manager_ = std::make_unique<ApiRequestManager>(mock_api_, config_.api_manager);
    position_monitor_ = std::make_unique<PositionMonitor>(bus_, positions_, config_.position_monitor);
    execution_ = std::make_unique<OrderExecutionWorker>(bus_, *api_manager_, config_.execution);

    for (const auto& strategy : config_.strategies) {
        auto load = std::make_unique<SimulatedLoadStrategy>(make_strategy(strategy.kind));
        strategy_loads_.push_back(load.get());
        strategies_.push_back(std::make_unique<SignalProcessor>(
            bus_,
            SignalProcessorConfig{strategy.name, strategy.symbols, strategy.quantity,
                                  config_.strategy_worker},
            std::move(load)));
    }
    if (config_.record_activity) {
        activity_ = std::make_unique<ActivityRecorder>(bus_, config_.activity, epoch_);
    }
    for (const auto& feed : config_.feeds) {
        feeds_.push_back(std::make_unique<MarketDataProducer>(bus_, feed));
    }
}

TradingSystem::~TradingSystem() {
    stop();
}

void TradingSystem::start() {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_.load() != State::created) {
        throw std::logic_error("a trading system can only be started once");
    }
    if (activity_) {
        activity_->start();
    }
    position_monitor_->start();
    execution_->start();
    for (auto& strategy : strategies_) {
        strategy->start();
    }
    for (auto& feed : feeds_) {
        feed->start();
    }
    state_.store(State::running);
}

void TradingSystem::stop() {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_.exchange(State::stopped) == State::stopped) {
        return;
    }
    for (auto& feed : feeds_) {
        feed->stop();
    }
    for (auto& feed : feeds_) {
        feed->wait();
    }
    // Draining queued market data should not be slowed by a simulated slow consumer.
    for (auto* load : strategy_loads_) {
        load->set_work_per_event(0us);
    }
    for (auto& strategy : strategies_) {
        strategy->stop();
    }
    execution_->stop();
    api_manager_->shutdown(ApiShutdownMode::drain, config_.drain_timeout);
    position_monitor_->stop();
    if (activity_) {
        activity_->stop();
    }
}

bool TradingSystem::feeds_finished() const {
    return std::all_of(feeds_.begin(), feeds_.end(),
                       [](const auto& feed) { return feed->stats().finished; });
}

SystemSnapshot TradingSystem::snapshot() const {
    SystemSnapshot snapshot;
    snapshot.uptime_s = std::chrono::duration<double>(MonotonicClock::now() - epoch_).count();
    snapshot.running = state_.load() == State::running;
    for (const auto& feed : feeds_) {
        snapshot.feeds.push_back(feed->stats());
    }
    for (std::size_t index = 0; index < strategies_.size(); ++index) {
        snapshot.strategies.push_back(strategies_[index]->stats());
        snapshot.strategy_work.push_back(strategy_loads_[index]->work_per_event());
    }
    snapshot.execution = execution_->stats();
    snapshot.api = api_manager_->stats();
    snapshot.mock_api_calls = mock_api_->invocation_count();
    snapshot.mock_api_latency = mock_api_->latency();
    snapshot.mock_api_429_every = mock_api_->respond_429_every();
    snapshot.position_monitor = position_monitor_->stats();
    snapshot.positions = positions_.snapshot();
    snapshot.bus = bus_.stats();
    snapshot.routes = bus_.routes();
    if (activity_) {
        snapshot.activity = activity_->snapshot();
    }
    return snapshot;
}

bool TradingSystem::set_feed_interval(std::string_view feed, std::chrono::microseconds interval) {
    return for_each_named(feeds_, feed,
                          [interval](MarketDataProducer& producer) { producer.set_interval(interval); });
}

bool TradingSystem::set_feed_paused(std::string_view feed, bool paused) {
    return for_each_named(feeds_, feed,
                          [paused](MarketDataProducer& producer) { producer.set_paused(paused); });
}

bool TradingSystem::set_strategy_work(std::string_view strategy, std::chrono::microseconds work) {
    bool matched = false;
    for (std::size_t index = 0; index < strategies_.size(); ++index) {
        if (strategy == "*" || strategies_[index]->name() == strategy) {
            strategy_loads_[index]->set_work_per_event(work);
            matched = true;
        }
    }
    return matched;
}

void TradingSystem::set_api_latency(std::chrono::milliseconds latency) {
    mock_api_->set_latency(latency);
}

void TradingSystem::set_api_429_every(std::size_t every_nth) {
    mock_api_->set_respond_429_every(every_nth);
}

}  // namespace trading
