#include "trading/resilience.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace trading {
namespace {

double milliseconds_between(std::chrono::steady_clock::time_point from, std::chrono::steady_clock::time_point to) {
    return std::chrono::duration<double, std::milli>(to - from).count();
}

}  // namespace

const char* to_string(ComponentState state) noexcept {
    switch (state) {
        case ComponentState::healthy:
            return "healthy";
        case ComponentState::degraded:
            return "degraded";
        case ComponentState::stalled:
            return "stalled";
        case ComponentState::crashed:
            return "crashed";
        case ComponentState::restarting:
            return "restarting";
        case ComponentState::quarantined:
            return "quarantined";
    }
    return "healthy";
}

// --- Health evaluation ---------------------------------------------------------------------

std::optional<std::chrono::milliseconds> StallDetector::observe(const WorkerHealth& health,
                                                                std::chrono::milliseconds timeout) {
    const auto now = std::chrono::steady_clock::now();
    // Progress, or nothing to do, both mean the worker is not stuck.
    if (!initialized_ || health.processed != last_processed_ || health.backlog == 0U) {
        initialized_ = true;
        last_processed_ = health.processed;
        last_progress_ = now;
        return std::nullopt;
    }
    const auto stalled_for = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_progress_);
    if (stalled_for >= timeout) {
        return stalled_for;
    }
    return std::nullopt;
}

ProbeResult evaluate_worker(const WorkerHealth& health, StallDetector& stall, const SupervisorConfig& config) {
    ProbeResult result;
    if (health.crashed) {
        result.verdict = ProbeResult::Verdict::fault;
        result.state = ComponentState::crashed;
        result.cause = "worker thread died";
        result.error = health.error;
        return result;
    }
    if (health.consecutive_errors >= config.poison_threshold) {
        result.verdict = ProbeResult::Verdict::fault;
        result.state = ComponentState::crashed;
        result.cause = std::to_string(health.consecutive_errors) + " consecutive handler failures (poison events)";
        return result;
    }
    // A worker that is still rebuilding its state is working, not stalled.
    if (health.ready) {
        if (const auto stalled = stall.observe(health, config.stall_timeout)) {
            result.verdict = ProbeResult::Verdict::fault;
            result.state = ComponentState::stalled;
            result.cause = "stalled: " + std::to_string(health.backlog) + " events waiting, none completed for " +
                           std::to_string(stalled->count()) + " ms";
            result.observed_ms = static_cast<double>(stalled->count());
            return result;
        }
    }
    if (health.handler_running_for > config.handler_deadline) {
        result.verdict = ProbeResult::Verdict::degraded;
        result.state = ComponentState::degraded;
        result.cause = "handler running for " +
                       std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(health.handler_running_for).count()) +
                       " ms";
    }
    return result;
}

// --- Component base ------------------------------------------------------------------------

SupervisedComponent::SupervisedComponent(std::string name, std::string kind)
    : name_(std::move(name)), kind_(std::move(kind)) {
    status_.name = name_;
    status_.kind = kind_;
}

ComponentStatus SupervisedComponent::status() const {
    ComponentStatus status;
    {
        std::lock_guard lock(mutex_);
        status = status_;
        if (unhealthy_) {
            status.down_for_ms = milliseconds_between(unhealthy_since_, std::chrono::steady_clock::now());
        }
    }
    fill_status(status);
    return status;
}

// --- Graveyard -----------------------------------------------------------------------------

Graveyard::~Graveyard() {
    reap();
    std::lock_guard lock(mutex_);
    for (auto& entry : entries_) {
        // The thread is still inside code that uses this object. Waiting could block the
        // process forever, and freeing it would be a use-after-free, so it is leaked.
        std::cerr << "graveyard: leaking " << entry.name << " (its thread never finished)\n";
        new std::shared_ptr<void>(std::move(entry.owner));
    }
}

void Graveyard::bury(std::string name, std::shared_ptr<void> owner, std::function<bool()> finished) {
    std::lock_guard lock(mutex_);
    entries_.push_back(Entry{std::move(name), std::move(owner), std::move(finished)});
    buried_.fetch_add(1, std::memory_order_relaxed);
}

void Graveyard::reap() {
    std::vector<Entry> done;
    {
        std::lock_guard lock(mutex_);
        for (auto it = entries_.begin(); it != entries_.end();) {
            if (it->finished()) {
                done.push_back(std::move(*it));
                it = entries_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Destroyed outside the lock: the destructor joins a thread that has already finished.
    done.clear();
}

bool Graveyard::wait_empty(std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        reap();
        if (zombies() == 0U) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

std::size_t Graveyard::zombies() const {
    std::lock_guard lock(mutex_);
    return static_cast<std::size_t>(std::count_if(entries_.begin(), entries_.end(),
                                                  [](const Entry& entry) { return !entry.finished(); }));
}

// --- Supervisor ----------------------------------------------------------------------------

Supervisor::Supervisor(EventBus& event_bus, Graveyard& graveyard, SupervisorConfig config, TimePoint epoch)
    : event_bus_(event_bus), graveyard_(graveyard), config_(config), epoch_(epoch) {
    if (config_.probe_interval <= std::chrono::milliseconds::zero() || config_.stall_timeout <= config_.probe_interval ||
        config_.max_restarts == 0U) {
        throw std::invalid_argument("invalid supervisor configuration");
    }
}

Supervisor::~Supervisor() {
    stop();
}

void Supervisor::add(SupervisedComponent& component) {
    components_.push_back(&component);
}

void Supervisor::set_escalation(std::function<void(const std::string&)> escalate) {
    escalate_ = std::move(escalate);
}

void Supervisor::start() {
    if (thread_.joinable()) {
        throw std::logic_error("supervisor already started");
    }
    thread_ = std::jthread([this](std::stop_token stop) { loop(stop); });
}

void Supervisor::stop() {
    if (thread_.joinable()) {
        thread_.request_stop();
        thread_.join();
    }
}

void Supervisor::loop(std::stop_token stop) {
    std::mutex wait_mutex;
    std::condition_variable_any wakeup;
    std::unique_lock wait_lock(wait_mutex);
    while (!stop.stop_requested()) {
        try {
            tick();
        } catch (const std::exception& exception) {
            std::cerr << "supervisor: " << exception.what() << '\n';
        } catch (...) {
            std::cerr << "supervisor: unknown exception\n";
        }
        wakeup.wait_for(wait_lock, stop, config_.probe_interval, [] { return false; });
    }
}

void Supervisor::tick() {
    graveyard_.reap();
    for (auto* component : components_) {
        {
            std::lock_guard lock(component->mutex_);
            if (component->status_.state == ComponentState::quarantined) {
                continue;
            }
        }
        ProbeResult probe;
        try {
            probe = component->probe();
        } catch (const std::exception& exception) {
            probe.verdict = ProbeResult::Verdict::fault;
            probe.state = ComponentState::crashed;
            probe.cause = "probe failed";
            probe.error = exception.what();
        }
        {
            std::lock_guard lock(mutex_);
            ++stats_.probes;
        }
        {
            std::lock_guard lock(component->mutex_);
            auto& status = component->status_;
            if (probe.verdict == ProbeResult::Verdict::fault) {
                status.state = probe.state;
                status.last_error = probe.error;
                if (!component->unhealthy_) {
                    component->unhealthy_ = true;
                    component->unhealthy_since_ = std::chrono::steady_clock::now();
                }
            } else {
                status.state = probe.state;
                component->unhealthy_ = false;
                status.down_for_ms = 0.0;
            }
        }
        if (probe.verdict == ProbeResult::Verdict::fault) {
            handle_fault(*component, probe);
        }
    }

    const auto zombies = graveyard_.zombies();
    bool escalate = false;
    std::string reason;
    {
        std::lock_guard lock(mutex_);
        if (zombies > config_.max_zombies && !stats_.escalated) {
            stats_.escalated = true;
            reason = std::to_string(zombies) + " abandoned threads are still running; the process must be restarted";
            stats_.escalation = reason;
            escalate = true;
        }
    }
    if (escalate) {
        std::cerr << "supervisor: escalating: " << reason << '\n';
        if (escalate_) {
            escalate_(reason);
        }
    }
}

void Supervisor::handle_fault(SupervisedComponent& component, const ProbeResult& probe) {
    const auto now = std::chrono::steady_clock::now();
    bool over_budget = false;
    {
        std::lock_guard lock(component.mutex_);
        while (!component.restart_times_.empty() &&
               now - component.restart_times_.front() > config_.restart_window) {
            component.restart_times_.pop_front();
        }
        over_budget = component.restart_times_.size() >= config_.max_restarts;
    }
    {
        std::lock_guard lock(mutex_);
        ++stats_.faults;
    }
    if (over_budget) {
        quarantine(component, probe,
                   std::to_string(config_.max_restarts) + " restarts within " +
                       std::to_string(std::chrono::duration_cast<std::chrono::seconds>(config_.restart_window).count()) +
                       " s; leaving it down");
        return;
    }

    record(component, "fault", probe.cause, 0.0);
    {
        std::lock_guard lock(component.mutex_);
        component.status_.state = ComponentState::restarting;
        component.status_.last_cause = probe.cause;
        component.status_.last_detect_ms = probe.observed_ms;
        component.restart_times_.push_back(now);
    }

    RecoveryResult result;
    try {
        result = component.recover(probe.cause);
    } catch (const std::exception& exception) {
        result.ok = false;
        result.detail = exception.what();
    }
    const auto elapsed_ms = milliseconds_between(now, std::chrono::steady_clock::now());

    std::uint64_t generation = 0;
    {
        std::lock_guard lock(component.mutex_);
        auto& status = component.status_;
        if (result.ok) {
            ++status.generation;
            ++status.restarts;
            status.state = ComponentState::healthy;
            status.last_recovery_ms = elapsed_ms;
            status.detail = result.detail;
            status.recovered_records = result.recovered_records;
            status.dead_letters += result.dead_letters;
            component.unhealthy_ = false;
            status.down_for_ms = 0.0;
        } else {
            status.state = probe.state;
            status.detail = "recovery failed: " + result.detail;
        }
        generation = status.generation;
    }
    {
        std::lock_guard lock(mutex_);
        if (result.ok) {
            ++stats_.restarts;
        }
    }
    auto entry_detail = result.detail;
    record(component, result.ok ? "restarted" : "recovery-failed", entry_detail, elapsed_ms);
    static_cast<void>(generation);
}

void Supervisor::quarantine(SupervisedComponent& component, const ProbeResult& probe, const std::string& why) {
    try {
        component.quarantine();
    } catch (const std::exception& exception) {
        std::cerr << "supervisor: quarantining " << component.name() << " failed: " << exception.what() << '\n';
    }
    {
        std::lock_guard lock(component.mutex_);
        component.status_.state = ComponentState::quarantined;
        component.status_.last_cause = probe.cause;
        component.status_.detail = why;
    }
    {
        std::lock_guard lock(mutex_);
        ++stats_.quarantines;
    }
    record(component, "quarantined", probe.cause + " — " + why, 0.0);
}

void Supervisor::record(SupervisedComponent& component, const std::string& action, const std::string& detail,
                        double recovery_ms) {
    std::uint64_t generation = 0;
    {
        std::lock_guard lock(component.mutex_);
        generation = component.status_.generation;
    }
    const auto now = MonotonicClock::now();
    {
        std::lock_guard lock(mutex_);
        log_.push_back(SupervisionLogEntry{std::chrono::duration<double>(now - epoch_).count(), component.name(),
                                           action, detail, generation, recovery_ms});
        while (log_.size() > 100U) {
            log_.pop_front();
        }
    }
    // Also on the bus, so the journal records it next to the data it affected.
    event_bus_.publish(SupervisionEvent{component.name(), action, detail, generation, recovery_ms, now});
}

std::vector<ComponentStatus> Supervisor::statuses() const {
    std::vector<ComponentStatus> statuses;
    statuses.reserve(components_.size());
    for (const auto* component : components_) {
        statuses.push_back(component->status());
    }
    return statuses;
}

std::vector<SupervisionLogEntry> Supervisor::log() const {
    std::lock_guard lock(mutex_);
    return {log_.begin(), log_.end()};
}

SupervisorStats Supervisor::stats() const {
    SupervisorStats stats;
    {
        std::lock_guard lock(mutex_);
        stats = stats_;
    }
    stats.zombies = graveyard_.zombies();
    stats.buried = graveyard_.buried();
    return stats;
}

// --- Controlled strategy -------------------------------------------------------------------

ControlledStrategy::ControlledStrategy(std::unique_ptr<ISignalStrategy> inner,
                                       std::shared_ptr<StrategyControls> controls)
    : inner_(std::move(inner)), controls_(std::move(controls)) {
    if (!inner_ || !controls_) {
        throw std::invalid_argument("controlled strategy needs a strategy and its controls");
    }
}

std::optional<Side> ControlledStrategy::on_market_data(const MarketDataEvent& quote) {
    if (!controls_->catching_up.load(std::memory_order_relaxed)) {
        const auto work = controls_->work_us.load(std::memory_order_relaxed);
        if (work > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(work));
        }
        controls_->fault.on_event();
    }
    return inner_->on_market_data(quote);
}

}  // namespace trading
