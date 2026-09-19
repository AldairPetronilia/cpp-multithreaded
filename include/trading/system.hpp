#pragma once

#include "trading/api.hpp"
#include "trading/messaging.hpp"
#include "trading/strategies.hpp"
#include "trading/workers.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace trading {

struct StrategyConfig {
    std::string name{};
    std::string kind{"ema-cross"};
    // Instruments whose market-data topics feed this strategy's mailbox; empty means all.
    std::vector<std::string> symbols{};
    double quantity{1.0};
};

struct SystemConfig {
    std::vector<MarketDataProducerConfig> feeds{};
    std::vector<StrategyConfig> strategies{};
    WorkerOptions strategy_worker{1'024, OverflowPolicy::drop_oldest, 64};
    OrderExecutionConfig execution{};
    PositionMonitorConfig position_monitor{};
    ApiManagerConfig api_manager{};
    MockApiConfig mock_api{};
    bool record_activity{false};
    ActivityRecorderConfig activity{};
    // Accepted API requests still unfinished this long into shutdown are cancelled.
    std::chrono::milliseconds drain_timeout{5'000};

    // Two feeds, five instruments and three strategies whose subscriptions overlap.
    [[nodiscard]] static SystemConfig demo();
};

struct SystemSnapshot {
    double uptime_s{};
    bool running{};
    std::vector<ProducerStats> feeds{};
    std::vector<SignalProcessorStats> strategies{};
    // Simulated per-event work of each strategy, in the same order as strategies.
    std::vector<std::chrono::microseconds> strategy_work{};
    ExecutionStats execution{};
    ApiManagerStats api{};
    std::uint64_t mock_api_calls{};
    std::chrono::milliseconds mock_api_latency{};
    std::size_t mock_api_429_every{};
    PositionMonitorStats position_monitor{};
    std::vector<PositionEntry> positions{};
    EventBusStats bus{};
    std::vector<RouteInfo> routes{};
    std::optional<ActivitySnapshot> activity{};
};

// Composition root. Builds every component once, injects the shared bus and request
// manager, and owns the lifecycle order: consumers start before producers, and shutdown
// runs upstream to downstream so each stage drains into a consumer that is still running.
class TradingSystem {
public:
    explicit TradingSystem(SystemConfig config);
    ~TradingSystem();
    TradingSystem(const TradingSystem&) = delete;
    TradingSystem& operator=(const TradingSystem&) = delete;

    void start();
    void stop();
    [[nodiscard]] bool feeds_finished() const;
    [[nodiscard]] SystemSnapshot snapshot() const;

    // Runtime controls used by the dashboard. A feed name of "*" addresses every feed.
    // They return false when no component has the given name.
    bool set_feed_interval(std::string_view feed, std::chrono::microseconds interval);
    bool set_feed_paused(std::string_view feed, bool paused);
    bool set_strategy_work(std::string_view strategy, std::chrono::microseconds work);
    void set_api_latency(std::chrono::milliseconds latency);
    void set_api_429_every(std::size_t every_nth);

private:
    enum class State { created, running, stopped };

    const SystemConfig config_;
    const TimePoint epoch_;
    // Declared first so it outlives every component that subscribes to or publishes on it.
    EventBus bus_;
    std::shared_ptr<MockTradingApi> mock_api_;
    std::unique_ptr<ApiRequestManager> api_manager_;
    PositionBook positions_;
    std::unique_ptr<PositionMonitor> position_monitor_;
    std::unique_ptr<OrderExecutionWorker> execution_;
    std::vector<std::unique_ptr<SignalProcessor>> strategies_;
    std::vector<SimulatedLoadStrategy*> strategy_loads_;  // owned by the processors above
    std::unique_ptr<ActivityRecorder> activity_;
    std::vector<std::unique_ptr<MarketDataProducer>> feeds_;
    std::mutex lifecycle_mutex_;
    std::atomic<State> state_{State::created};
};

}  // namespace trading
