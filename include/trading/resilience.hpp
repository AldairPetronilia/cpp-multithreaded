#pragma once

#include "trading/events.hpp"
#include "trading/fault.hpp"
#include "trading/messaging.hpp"
#include "trading/strategies.hpp"
#include "trading/workers.hpp"

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
#include <string>
#include <thread>
#include <vector>

namespace trading {

struct SupervisorConfig {
    std::chrono::milliseconds probe_interval{20};
    // A worker with work waiting and nothing completing for this long is stalled. Idle is
    // not stalled: an empty mailbox never trips this.
    std::chrono::milliseconds stall_timeout{300};
    // A handler running longer than this marks its worker degraded. That is a warning,
    // not a restart: a merely slow handler still makes progress.
    std::chrono::milliseconds handler_deadline{1'000};
    // Consecutive handler exceptions that mark a worker as poisoned.
    std::uint64_t poison_threshold{5};
    // How long a replacement may take to rebuild its state and become ready.
    std::chrono::milliseconds recovery_timeout{30'000};
    // Restart budget: more than this many restarts of one component within the window
    // quarantines it instead of looping.
    std::size_t max_restarts{3};
    std::chrono::milliseconds restart_window{10'000};
    // Abandoned threads that are still running. Past this the process is leaking threads
    // and escalates: exiting non-zero for an external restarter (crash-only design).
    std::size_t max_zombies{8};
};

enum class ComponentState {
    healthy,
    // Alive but slow: a handler has run past its deadline.
    degraded,
    // Work is waiting and nothing completes.
    stalled,
    // The thread died.
    crashed,
    // Being replaced and rebuilding its state.
    restarting,
    // Out of restart budget: left down, loudly, while the rest of the system keeps running.
    quarantined,
};

[[nodiscard]] const char* to_string(ComponentState state) noexcept;

struct ComponentStatus {
    std::string name{};
    // feed | strategy | execution | position-monitor
    std::string kind{};
    ComponentState state{ComponentState::healthy};
    std::uint64_t generation{1};
    std::uint64_t restarts{};
    std::string last_cause{};
    std::string last_error{};
    // What the last recovery did, in words.
    std::string detail{};
    // How long the fault was visible before the supervisor acted, and how long the
    // recovery then took.
    double last_detect_ms{};
    double last_recovery_ms{};
    // How long the component has been unhealthy, while it is.
    double down_for_ms{};
    std::uint64_t recovered_records{};
    std::uint64_t dead_letters{};
    std::uint64_t fenced_writes{};
};

struct ProbeResult {
    enum class Verdict { ok, degraded, fault };

    Verdict verdict{Verdict::ok};
    ComponentState state{ComponentState::healthy};
    std::string cause{};
    std::string error{};
    // How long the problem had already been visible.
    double observed_ms{};
};

struct RecoveryResult {
    bool ok{true};
    std::string detail{};
    std::uint64_t recovered_records{};
    std::uint64_t dead_letters{};
};

// Distinguishes a stuck worker from an idle one. A worker is stalled if its mailbox holds
// work and its processed count has not moved for the timeout; an empty mailbox resets it.
class StallDetector {
public:
    // The time the worker has been stalled, once that reaches the timeout.
    [[nodiscard]] std::optional<std::chrono::milliseconds> observe(const WorkerHealth& health,
                                                                   std::chrono::milliseconds timeout);

private:
    bool initialized_{false};
    std::uint64_t last_processed_{0};
    std::chrono::steady_clock::time_point last_progress_{};
};

// Turns what a worker reports about itself into a verdict.
[[nodiscard]] ProbeResult evaluate_worker(const WorkerHealth& health, StallDetector& stall,
                                          const SupervisorConfig& config);

// Something the supervisor can watch and replace. probe() and recover() are called from the
// supervisor thread only; status() from any thread.
class SupervisedComponent {
public:
    SupervisedComponent(std::string name, std::string kind);
    virtual ~SupervisedComponent() = default;
    SupervisedComponent(const SupervisedComponent&) = delete;
    SupervisedComponent& operator=(const SupervisedComponent&) = delete;

    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] const std::string& kind() const noexcept { return kind_; }

    // Cheap and non-blocking.
    virtual ProbeResult probe() = 0;
    // Replaces the failed instance and rebuilds its state. Must not wait for a hung thread.
    virtual RecoveryResult recover(const std::string& cause) = 0;
    // Called once when the supervisor gives up on the component: take the failed instance
    // out of service so that it stops holding a subscription.
    virtual void quarantine() {}

    [[nodiscard]] ComponentStatus status() const;

protected:
    // Adds figures only the component knows (fenced writes, recovered records) to a status.
    virtual void fill_status(ComponentStatus& status) const { static_cast<void>(status); }

private:
    friend class Supervisor;

    const std::string name_;
    const std::string kind_;
    mutable std::mutex mutex_;
    ComponentStatus status_;
    std::deque<std::chrono::steady_clock::time_point> restart_times_;
    std::chrono::steady_clock::time_point unhealthy_since_{};
    bool unhealthy_{false};
};

// Threads that were abandoned because they would not stop. C++ cannot kill a thread, so
// the object a zombie runs on is kept alive here until the thread finishes by itself, and
// freed then. A zombie that never finishes is leaked at exit rather than waited for.
class Graveyard {
public:
    Graveyard() = default;
    ~Graveyard();
    Graveyard(const Graveyard&) = delete;
    Graveyard& operator=(const Graveyard&) = delete;

    void bury(std::string name, std::shared_ptr<void> owner, std::function<bool()> finished);
    // Frees entries whose thread has finished.
    void reap();
    // Waits for the zombies to finish, for shutdown and tests.
    bool wait_empty(std::chrono::milliseconds timeout);
    // Buried and still running.
    [[nodiscard]] std::size_t zombies() const;
    [[nodiscard]] std::uint64_t buried() const noexcept { return buried_.load(std::memory_order_relaxed); }

private:
    struct Entry {
        std::string name;
        std::shared_ptr<void> owner;
        std::function<bool()> finished;
    };

    mutable std::mutex mutex_;
    std::vector<Entry> entries_;
    std::atomic<std::uint64_t> buried_{0};
};

struct SupervisionLogEntry {
    double time_s{};
    std::string component{};
    std::string action{};
    std::string detail{};
    std::uint64_t generation{};
    double recovery_ms{};
};

struct SupervisorStats {
    std::uint64_t probes{};
    std::uint64_t faults{};
    std::uint64_t restarts{};
    std::uint64_t quarantines{};
    std::size_t zombies{};
    std::uint64_t buried{};
    bool escalated{};
    std::string escalation{};
};

// One thread that probes every component on an interval and replaces the ones that fail.
// It is deliberately small and does nothing else, because it cannot itself be supervised:
// the last line of defence is the process exiting for an external restarter.
class Supervisor {
public:
    Supervisor(EventBus& event_bus, Graveyard& graveyard, SupervisorConfig config, TimePoint epoch);
    ~Supervisor();
    Supervisor(const Supervisor&) = delete;
    Supervisor& operator=(const Supervisor&) = delete;

    // Components are not owned and must outlive the supervisor. Add before start().
    void add(SupervisedComponent& component);
    // Called once if the supervisor gives up on the process (too many zombies).
    void set_escalation(std::function<void(const std::string&)> escalate);

    void start();
    void stop();
    // One pass over every component; the thread calls this, tests may too.
    void tick();

    [[nodiscard]] std::vector<ComponentStatus> statuses() const;
    [[nodiscard]] std::vector<SupervisionLogEntry> log() const;
    [[nodiscard]] SupervisorStats stats() const;
    [[nodiscard]] const SupervisorConfig& config() const noexcept { return config_; }

private:
    void loop(std::stop_token stop);
    void handle_fault(SupervisedComponent& component, const ProbeResult& probe);
    void quarantine(SupervisedComponent& component, const ProbeResult& probe, const std::string& why);
    void record(SupervisedComponent& component, const std::string& action, const std::string& detail,
                double recovery_ms);

    EventBus& event_bus_;
    Graveyard& graveyard_;
    const SupervisorConfig config_;
    const TimePoint epoch_;
    std::vector<SupervisedComponent*> components_;
    std::function<void(const std::string&)> escalate_;
    mutable std::mutex mutex_;
    std::deque<SupervisionLogEntry> log_;
    SupervisorStats stats_{};
    std::jthread thread_;
};

// Per-strategy controls that outlive any one instance of the strategy: simulated work for
// the dashboard's "slow a strategy" control, and fault injection.
struct StrategyControls {
    std::atomic<std::int64_t> work_us{0};
    // True while a replacement replays history; the strategy then neither sleeps nor faults.
    std::atomic<bool> catching_up{false};
    FaultControl fault;
};

class ControlledStrategy final : public ISignalStrategy {
public:
    ControlledStrategy(std::unique_ptr<ISignalStrategy> inner, std::shared_ptr<StrategyControls> controls);

    [[nodiscard]] std::optional<Side> on_market_data(const MarketDataEvent& quote) override;
    [[nodiscard]] std::string_view kind() const noexcept override { return inner_->kind(); }

private:
    std::unique_ptr<ISignalStrategy> inner_;
    std::shared_ptr<StrategyControls> controls_;
};

}  // namespace trading
