#include "trading/config.hpp"
#include "trading/session.hpp"
#include "trading/telemetry.hpp"

#if TRADING_HAS_DASHBOARD
#include "trading/dashboard_server.hpp"
#endif

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>

namespace {

using namespace std::chrono_literals;

std::atomic<bool> interrupted{false};
std::atomic<bool> escalated{false};
static_assert(std::atomic<bool>::is_always_lock_free, "signal handlers need a lock-free flag");

extern "C" void on_interrupt(int) {
    interrupted.store(true);
}

std::string format_latency(const trading::HistogramSnapshot& histogram) {
    const auto summary = histogram.summary();
    if (summary.count == 0U) {
        return "(no samples)";
    }
    std::ostringstream text;
    text << std::fixed << std::setprecision(1) << "p50=" << summary.p50_us << "us p99=" << summary.p99_us
         << "us max=" << summary.max_us << "us (n=" << summary.count << ')';
    return text.str();
}

std::string join(const std::vector<std::string>& items) {
    std::string joined;
    for (const auto& item : items) {
        joined += (joined.empty() ? "" : ",") + item;
    }
    return joined.empty() ? "*" : joined;
}

void print_summary(const trading::SystemSnapshot& snapshot) {
    const bool replay = snapshot.mode == "replay";
    std::cout << std::fixed << std::setprecision(2) << "Trading PoC " << (replay ? "replay" : "run") << " finished after "
              << snapshot.uptime_s << " s (session " << snapshot.session << ")\nFeeds:\n";
    for (const auto& feed : snapshot.feeds) {
        std::cout << "  " << std::left << std::setw(12) << feed.name << std::right << " quotes="
                  << feed.events_published << " symbols=" << join(feed.symbols)
                  << " rejected=" << feed.subscriber_rejections << " errors=" << feed.failures << '\n';
    }
    std::cout << "Strategies (one thread and one mailbox each):\n";
    for (const auto& strategy : snapshot.strategies) {
        const auto& worker = strategy.worker;
        std::cout << "  " << std::left << std::setw(12) << worker.name << std::setw(10) << strategy.kind
                  << std::right << " symbols=" << join(strategy.symbols) << " quotes=" << worker.processed
                  << " signals=" << strategy.signals_published << " shed=" << worker.mailbox.evicted_oldest
                  << " max_queue=" << worker.mailbox.max_depth << "\n    dispatch "
                  << format_latency(worker.dispatch_latency) << "\n    tick-to-signal "
                  << format_latency(strategy.tick_to_signal) << '\n';
    }
    const auto& execution = snapshot.execution;
    std::cout << "Execution: signals=" << execution.worker.processed
              << " submitted=" << execution.requests_submitted
              << " rejected=" << execution.requests_rejected << " dispatch "
              << format_latency(execution.worker.dispatch_latency) << '\n';
    const auto& api = snapshot.api;
    std::cout << "API gateways (" << snapshot.gateways.size() << ", " << api.limiter.max_requests << " requests / "
              << api.limiter.window.count() << " ms each): accepted=" << api.accepted
              << " completed=" << api.completed << " failed=" << api.failed
              << " cancelled=" << api.cancelled << " retries=" << api.retries
              << " http429=" << api.rate_limited_responses << " max_pending=" << api.max_pending << '\n';
    for (const auto& gateway : snapshot.gateways) {
        std::cout << "  " << std::left << std::setw(12) << gateway.name << std::right << " " << trading::to_string(gateway.state)
                  << " submitted=" << gateway.submitted << " failed=" << gateway.failed
                  << " ejections=" << gateway.ejections << " rerouted_away=" << gateway.rerouted_away << '\n';
    }
    const auto& monitor = snapshot.position_monitor;
    std::cout << "  queue delay     " << format_latency(monitor.api_queue) << "\n  rate-limit wait "
              << format_latency(monitor.rate_limit_wait) << "\n  mock service    "
              << format_latency(monitor.api_service) << '\n';
    std::cout << "Positions (fills=" << monitor.fills << " rejected=" << monitor.rejections
              << " duplicates ignored=" << monitor.duplicates << "):\n";
    for (const auto& position : snapshot.positions) {
        std::cout << "  " << std::left << std::setw(12) << position.strategy << std::setw(8) << position.symbol
                  << std::right << " qty=" << position.quantity << " fills=" << position.fills << '\n';
    }
    std::cout << "Market data to position update " << format_latency(monitor.end_to_end) << '\n';
    std::cout << "Event bus: published=" << snapshot.bus.events_published
              << " without_subscribers=" << snapshot.bus.events_without_subscribers << '\n';

    if (snapshot.journal) {
        const auto& journal = *snapshot.journal;
        std::cout << "Journal " << journal.directory << ": records=" << journal.records << " bytes=" << journal.bytes
                  << " segments=" << journal.segments << " gaps=" << journal.gaps << " (events lost=" << journal.gap_events
                  << ")\n";
    }
    if (snapshot.supervised) {
        const auto& supervisor = snapshot.supervisor;
        std::cout << "Supervision: faults=" << supervisor.faults << " restarts=" << supervisor.restarts
                  << " quarantined=" << supervisor.quarantines << " abandoned threads=" << supervisor.buried << '\n';
        for (const auto& component : snapshot.components) {
            if (component.restarts > 0U || component.state != trading::ComponentState::healthy) {
                std::cout << "  " << std::left << std::setw(18) << component.name << std::right << " "
                          << trading::to_string(component.state) << " restarts=" << component.restarts
                          << " generation=" << component.generation << " last recovery " << component.last_recovery_ms
                          << " ms: " << component.detail << '\n';
            }
        }
    }
    if (snapshot.replay) {
        const auto& progress = snapshot.replay->progress;
        const auto& check = snapshot.replay->verification;
        std::cout << "Replay (" << trading::to_string(progress.mode) << ", " << trading::to_string(progress.pacing)
                  << "): published=" << progress.published << " of records " << progress.first_sequence << ".."
                  << progress.last_sequence << " in " << progress.elapsed_s << " s";
        if (check.enabled) {
            std::cout << "\n  verification: " << check.verdict << " (recorded " << check.recorded << " signals, replay derived "
                      << check.rederived << ", matched " << check.matched << ", missing " << check.missing << ", extra "
                      << check.extra << ", explained by outages " << check.explained << ')';
            if (!check.note.empty()) {
                std::cout << "\n  " << check.note;
            }
        }
        std::cout << '\n';
    }
}

// Losses on the order path and any unrecovered error are failures. Shedding stale market
// data under overload is the designed behaviour, so it is reported above but not treated as
// one, and neither is a fault the supervisor recovered from.
bool completed_cleanly(const trading::SystemSnapshot& snapshot) {
    std::uint64_t problems = snapshot.execution.worker.handler_errors + snapshot.execution.requests_rejected +
                             snapshot.execution.report_publish_rejections +
                             snapshot.execution.worker.mailbox.rejected_full +
                             snapshot.position_monitor.worker.handler_errors +
                             snapshot.position_monitor.worker.mailbox.rejected_full + snapshot.api.failed +
                             snapshot.api.cancelled + snapshot.supervisor.quarantines;
    for (const auto& feed : snapshot.feeds) {
        problems += feed.failures;
    }
    for (const auto& strategy : snapshot.strategies) {
        problems += strategy.worker.handler_errors + strategy.publish_rejections;
    }
    for (const auto& component : snapshot.components) {
        if (component.state != trading::ComponentState::healthy && component.state != trading::ComponentState::degraded) {
            ++problems;
        }
    }
    if (snapshot.replay) {
        const auto& check = snapshot.replay->verification;
        if (snapshot.replay->progress.state == trading::ReplayState::failed || check.verdict == "diverged") {
            ++problems;
        }
    }
    return problems == 0U;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const auto config = trading::AppConfig::from_command_line(argc, argv);
        if (config.show_help) {
            std::cout << trading::AppConfig::usage(argv[0]);
            return 0;
        }
        std::signal(SIGINT, on_interrupt);
        std::signal(SIGTERM, on_interrupt);

        trading::LiveSession session(config.system);
        // A supervisor that has to give up on the process asks for it to exit, so that
        // whatever started it can start a clean one (crash-only design).
        session.live().set_escalation([](const std::string& reason) {
            std::cerr << "escalation: " << reason << '\n';
            escalated.store(true);
            interrupted.store(true);
        });

#if TRADING_HAS_DASHBOARD
        std::unique_ptr<trading::DashboardServer> dashboard;
        if (config.dashboard) {
            dashboard = std::make_unique<trading::DashboardServer>(
                trading::DashboardServerConfig{"127.0.0.1", config.dashboard_port, config.dashboard_interval, 32},
                std::string(trading::embedded_dashboard_page()), [&session] { return session.render(); },
                [&session](std::string_view name, std::string_view value) { return session.apply_control(name, value); });
            dashboard->start();
            std::cout << "Dashboard: http://127.0.0.1:" << dashboard->port() << "  (Ctrl+C to stop)\n"
                      << std::flush;
        }
#else
        if (config.dashboard) {
            throw std::runtime_error("this build has no dashboard support (POSIX sockets required)");
        }
#endif

        session.start();
        if (const auto* journal = session.live().journal()) {
            std::cout << "Journal: " << journal->directory().string() << '\n' << std::flush;
        }
        // Supervisory loop, far from any hot path. A signal handler can only set a flag,
        // so the flag, the deadline and feed completion are checked every 50 ms. A replay
        // shown on the dashboard stays up when it reaches the end, so it can be scrubbed back.
        const auto deadline = config.duration
                                  ? std::optional(trading::MonotonicClock::now() + *config.duration)
                                  : std::nullopt;
        const bool stop_when_done = !(config.dashboard && config.replay);
        const auto should_stop = [&] {
            return interrupted.load() || (deadline && trading::MonotonicClock::now() >= *deadline) ||
                   (stop_when_done && session.feeds_finished());
        };
        while (!should_stop()) {
            // The supervisor cannot report itself stuck (blocked on a lock a wedged worker
            // holds, say), so this thread watches it, and exits for an external restarter.
            if (const auto* supervisor = session.live().supervisor(); supervisor != nullptr && supervisor->stuck()) {
                std::cerr << "escalation: the supervisor has been stuck for over "
                          << supervisor->config().watchdog.count() << " ms\n";
                std::_Exit(3);
            }
            std::this_thread::sleep_for(50ms);
        }
        // Stopping drains every stage first, so the summary's counts reconcile.
        session.live().stop();

#if TRADING_HAS_DASHBOARD
        if (dashboard && !interrupted.load() && !deadline) {
            std::cout << "Run complete; the dashboard shows the final state. Ctrl+C to exit.\n" << std::flush;
            while (!interrupted.load()) {
                std::this_thread::sleep_for(50ms);
            }
        }
        if (dashboard) {
            dashboard->stop();
        }
#endif

        const auto snapshot = session.live().snapshot();
        session.stop();
        print_summary(snapshot);
        if (escalated.load()) {
            return 3;
        }
        return completed_cleanly(snapshot) ? 0 : 2;
    } catch (const std::exception& exception) {
        std::cerr << "fatal: " << exception.what() << '\n';
        std::cerr << trading::AppConfig::usage(argv[0]);
        return 1;
    }
}
