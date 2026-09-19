#include "trading/config.hpp"
#include "trading/system.hpp"
#include "trading/telemetry.hpp"

#if TRADING_HAS_DASHBOARD
#include "trading/dashboard_server.hpp"
#endif

#include <atomic>
#include <chrono>
#include <csignal>
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
    std::cout << std::fixed << std::setprecision(2) << "Trading PoC finished after " << snapshot.uptime_s
              << " s\nFeeds:\n";
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
    std::cout << "Shared API manager (" << api.limiter.max_requests << " requests / "
              << api.limiter.window.count() << " ms): accepted=" << api.accepted
              << " completed=" << api.completed << " failed=" << api.failed
              << " cancelled=" << api.cancelled << " retries=" << api.retries
              << " http429=" << api.rate_limited_responses << " max_pending=" << api.max_pending << '\n';
    const auto& monitor = snapshot.position_monitor;
    std::cout << "  queue delay     " << format_latency(monitor.api_queue) << "\n  rate-limit wait "
              << format_latency(monitor.rate_limit_wait) << "\n  mock service    "
              << format_latency(monitor.api_service) << '\n';
    std::cout << "Positions (fills=" << monitor.fills << " rejected=" << monitor.rejections << "):\n";
    for (const auto& position : snapshot.positions) {
        std::cout << "  " << std::left << std::setw(12) << position.strategy << std::setw(8) << position.symbol
                  << std::right << " qty=" << position.quantity << " fills=" << position.fills << '\n';
    }
    std::cout << "Market data to position update " << format_latency(monitor.end_to_end) << '\n';
    std::cout << "Event bus: published=" << snapshot.bus.events_published
              << " without_subscribers=" << snapshot.bus.events_without_subscribers << '\n';
}

// Losses on the order path and any error are failures. Shedding stale market data under
// overload is the designed behaviour, so it is reported above but not treated as one.
bool completed_cleanly(const trading::SystemSnapshot& snapshot) {
    std::uint64_t problems = snapshot.execution.worker.handler_errors + snapshot.execution.requests_rejected +
                             snapshot.execution.report_publish_rejections +
                             snapshot.execution.worker.mailbox.rejected_full +
                             snapshot.position_monitor.worker.handler_errors +
                             snapshot.position_monitor.worker.mailbox.rejected_full + snapshot.api.failed +
                             snapshot.api.cancelled;
    for (const auto& feed : snapshot.feeds) {
        problems += feed.failures;
    }
    for (const auto& strategy : snapshot.strategies) {
        problems += strategy.worker.handler_errors + strategy.publish_rejections;
    }
    return problems == 0U;
}

#if TRADING_HAS_DASHBOARD
trading::ControlResult apply_control(trading::TradingSystem& system, std::string_view name, std::string_view value) {
    // Controls are addressed as "<control>" or "<control>:<target>", e.g. "feed.paused:fx".
    const auto separator = name.find(':');
    const auto control = name.substr(0, separator);
    const std::string target(separator == std::string_view::npos ? "*" : name.substr(separator + 1));
    const auto number = [&] {
        std::size_t parsed_length = 0;
        const auto parsed = std::stoll(std::string(value), &parsed_length);
        if (parsed_length != value.size() || parsed < 0) {
            throw std::invalid_argument("control value must be a non-negative integer");
        }
        return parsed;
    };

    bool matched = true;
    if (control == "feed.interval_us") {
        matched = system.set_feed_interval(target, std::chrono::microseconds(number()));
    } else if (control == "feed.paused") {
        matched = system.set_feed_paused(target, number() != 0);
    } else if (control == "strategy.work_us") {
        matched = system.set_strategy_work(target, std::chrono::microseconds(number()));
    } else if (control == "api.latency_ms") {
        system.set_api_latency(std::chrono::milliseconds(number()));
    } else if (control == "api.inject_429_every") {
        system.set_api_429_every(static_cast<std::size_t>(number()));
    } else {
        return {404, R"({"ok":false,"error":"unknown control"})"};
    }
    return matched ? trading::ControlResult{} : trading::ControlResult{404, R"({"ok":false,"error":"unknown target"})"};
}
#endif

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

        trading::TradingSystem system(config.system);

#if TRADING_HAS_DASHBOARD
        std::unique_ptr<trading::DashboardServer> dashboard;
        if (config.dashboard) {
            dashboard = std::make_unique<trading::DashboardServer>(
                trading::DashboardServerConfig{"127.0.0.1", config.dashboard_port, config.dashboard_interval, 32},
                std::string(trading::embedded_dashboard_page()),
                [&system, renderer = std::make_shared<trading::TelemetryRenderer>()] {
                    return renderer->render(system.snapshot());
                },
                [&system](std::string_view name, std::string_view value) {
                    return apply_control(system, name, value);
                });
            dashboard->start();
            std::cout << "Dashboard: http://127.0.0.1:" << dashboard->port() << "  (Ctrl+C to stop)\n"
                      << std::flush;
        }
#else
        if (config.dashboard) {
            throw std::runtime_error("this build has no dashboard support (POSIX sockets required)");
        }
#endif

        system.start();
        // Supervisory loop, far from any hot path. A signal handler can only set a flag,
        // so the flag, the deadline and feed completion are checked every 50 ms.
        const auto deadline = config.duration
                                  ? std::optional(trading::MonotonicClock::now() + *config.duration)
                                  : std::nullopt;
        const auto should_stop = [&] {
            return interrupted.load() || (deadline && trading::MonotonicClock::now() >= *deadline) ||
                   system.feeds_finished();
        };
        while (!should_stop()) {
            std::this_thread::sleep_for(50ms);
        }
        system.stop();

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

        const auto snapshot = system.snapshot();
        print_summary(snapshot);
        return completed_cleanly(snapshot) ? 0 : 2;
    } catch (const std::exception& exception) {
        std::cerr << "fatal: " << exception.what() << '\n';
        std::cerr << trading::AppConfig::usage(argv[0]);
        return 1;
    }
}
