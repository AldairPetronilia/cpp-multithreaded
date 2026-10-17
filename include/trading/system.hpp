#pragma once

#include "trading/api.hpp"
#include "trading/components.hpp"
#include "trading/gateway_pool.hpp"
#include "trading/journal.hpp"
#include "trading/messaging.hpp"
#include "trading/replay.hpp"
#include "trading/resilience.hpp"
#include "trading/strategies.hpp"
#include "trading/workers.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace trading {

struct JournalSettings {
    bool enabled{false};
    // Each run records into <root>/<session>/.
    std::filesystem::path root{"journal"};
    std::size_t segment_bytes{16U << 20U};
    std::size_t mailbox_capacity{65'536};
    std::chrono::milliseconds flush_interval{20};
    std::chrono::milliseconds fsync_interval{1'000};
    bool full_fsync{false};
};

struct SupervisionSettings {
    bool enabled{false};
    SupervisorConfig config{};
};

struct GatewaySettings {
    // Gateways, each with its own credential, limiter and I/O threads.
    std::size_t count{1};
    GatewayPoolConfig pool{};
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

    JournalSettings journal{};
    SupervisionSettings supervision{};
    GatewaySettings gateways{};
    // Names this run in the journal and prefixes its order ids; generated when empty.
    std::string session{};
    // Set to run a replay of a recorded session instead of live feeds.
    std::optional<ReplayOptions> replay{};

    // Two feeds, five instruments and three strategies whose subscriptions overlap.
    [[nodiscard]] static SystemConfig demo();

    // The configuration of a replay system: the same topology as the live one, a mock
    // broker of its own with no delay, no journal and no supervision.
    [[nodiscard]] static SystemConfig for_replay(const SystemConfig& live, ReplayOptions options);
    // Reconstructs the topology of a recorded session from its manifest.
    [[nodiscard]] static std::optional<SystemConfig> from_manifest(const std::filesystem::path& session_directory,
                                                                  const SystemConfig& base);
};

struct ReplaySnapshot {
    ReplayProgress progress{};
    VerificationReport verification{};
};

struct SystemSnapshot {
    double uptime_s{};
    // Where the charts' time axis stands: the uptime when live, and for a replay the position
    // in the recording, so a replay's charts show recorded time however fast it runs.
    double timeline_s{};
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

    std::string mode{"live"};
    std::string session{};
    bool supervised{false};
    std::vector<ComponentStatus> components{};
    std::vector<SupervisionLogEntry> supervision_log{};
    SupervisorStats supervisor{};
    std::optional<JournalStats> journal{};
    std::vector<GatewayStatus> gateways{};
    GatewayPoolStats gateway_pool{};
    std::optional<ReplaySnapshot> replay{};
};

enum class Fault { crash, hang, poison };

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
    // Waits until every strategy has handled the quotes it had been given, so the signals
    // those quotes trigger are published (and journaled). False on timeout.
    bool drain_strategies(std::chrono::milliseconds timeout);

    // Runtime controls used by the dashboard. A feed name of "*" addresses every feed.
    // They return false when no component has the given name.
    bool set_feed_interval(std::string_view feed, std::chrono::microseconds interval);
    bool set_feed_paused(std::string_view feed, bool paused);
    bool set_strategy_work(std::string_view strategy, std::chrono::microseconds work);
    void set_api_latency(std::chrono::milliseconds latency);
    void set_api_429_every(std::size_t every_nth);

    // Fault injection. component names a feed, a strategy, "order-execution" or
    // "position-monitor". A hang resumes by itself after hang_for, as a stuck thread that
    // wakes up later; feeds can only crash. Returns false for an unknown or unsupported target.
    bool inject_fault(std::string_view component, Fault fault,
                      std::chrono::milliseconds hang_for = std::chrono::milliseconds(8'000));
    bool set_gateway_down(std::size_t index, bool down);

    // Replay controls; false when this is not a replay system.
    bool replay_set_paused(bool paused);
    bool replay_set_speed(double speed);
    bool replay_set_pacing(ReplayPacing pacing);
    // Position in the recording as a fraction from 0 to 1.
    bool replay_seek(double fraction);

    [[nodiscard]] const std::string& session() const noexcept { return session_; }
    [[nodiscard]] bool is_replay() const noexcept { return config_.replay.has_value(); }
    [[nodiscard]] const SystemConfig& config() const noexcept { return config_; }
    // The journal being written, or null.
    [[nodiscard]] JournalWriter* journal() noexcept { return journal_.get(); }
    [[nodiscard]] std::filesystem::path journal_directory() const;
    [[nodiscard]] Supervisor* supervisor() noexcept { return supervisor_.get(); }
    // Called once if the supervisor gives up on the process.
    void set_escalation(std::function<void(const std::string&)> escalate);

private:
    enum class State { created, running, stopped };

    void wait_for_downstream();
    [[nodiscard]] std::vector<SupervisedComponent*> components() const;
    [[nodiscard]] SessionManifest manifest() const;

    const SystemConfig config_;
    const TimePoint epoch_;
    std::string session_;
    // Declared first so it outlives every component that subscribes to or publishes on it.
    EventBus bus_;
    // Rung by supervised workers as they die. Null when nothing supervises them.
    std::shared_ptr<Doorbell> doorbell_;
    std::unique_ptr<JournalWriter> journal_;
    std::shared_ptr<MockTradingApi> mock_api_;
    std::unique_ptr<GatewayPool> gateway_;
    PositionBook positions_;
    Graveyard graveyard_;
    std::unique_ptr<PositionComponent> position_monitor_;
    std::unique_ptr<ExecutionComponent> execution_;
    std::vector<std::unique_ptr<StrategyComponent>> strategies_;
    std::unique_ptr<ActivityRecorder> activity_;
    std::vector<std::unique_ptr<FeedComponent>> feeds_;
    std::unique_ptr<ReplayVerifier> verifier_;
    std::unique_ptr<ReplayFeed> replay_feed_;
    // Declared last so it stops probing before anything it probes is destroyed.
    std::unique_ptr<Supervisor> supervisor_;
    std::mutex lifecycle_mutex_;
    std::atomic<State> state_{State::created};
};

}  // namespace trading
