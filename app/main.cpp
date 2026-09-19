#include "trading/api.hpp"
#include "trading/config.hpp"
#include "trading/messaging.hpp"
#include "trading/workers.hpp"

#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string_view>

namespace {

void print_latency(std::string_view label, const trading::LatencySummary& summary) {
    std::cout << "  " << label << ": count=" << summary.count;
    if (summary.count > 0U) {
        std::cout << std::fixed << std::setprecision(2) << " p50=" << summary.p50_us
                  << "us p95=" << summary.p95_us << "us p99=" << summary.p99_us
                  << "us max=" << summary.maximum_us << "us";
    }
    std::cout << '\n';
}

void print_worker(std::string_view name, const trading::WorkerStats& stats) {
    std::cout << "  " << name << ": processed=" << stats.processed
              << " errors=" << stats.handler_errors
              << " max_queue=" << stats.max_queue_depth
              << " rejected_full=" << stats.rejected_full << '\n';
    print_latency(std::string(name) + " dispatch", stats.dispatch_latency);
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const auto config = trading::AppConfig::from_command_line(argc, argv);
        if (config.show_help) {
            std::cout << trading::AppConfig::usage(argv[0]);
            return 0;
        }

        trading::EventBus event_bus;
        auto mock_api = std::make_shared<trading::MockTradingApi>(config.mock_api);
        trading::ApiRequestManager request_manager(mock_api, config.api_manager);
        trading::PositionBook position_book;

        trading::SignalProcessor signal_processor(event_bus, config.signal);
        trading::OrderExecutionWorker execution_worker(
            event_bus, request_manager, config.execution);
        trading::PositionMonitor position_monitor(
            event_bus, position_book, config.position_monitor);
        trading::MarketDataProducer market_data(event_bus, config.market_data);

        position_monitor.start();
        execution_worker.start();
        signal_processor.start();
        market_data.start();
        market_data.wait();

        // This order drains each pipeline stage before closing the next one.
        signal_processor.stop();
        execution_worker.stop();
        request_manager.shutdown(trading::ApiShutdownMode::drain);
        position_monitor.stop();

        const auto producer_stats = market_data.stats();
        const auto bus_stats = event_bus.stats();
        const auto api_stats = request_manager.stats();
        const auto signal_stats = signal_processor.stats();
        const auto execution_stats = execution_worker.stats();
        const auto monitor_stats = position_monitor.stats();

        std::cout << "Trading PoC completed\n";
        std::cout << "Pipeline:\n";
        std::cout << "  market events=" << producer_stats.events_published
                  << " producer_errors=" << producer_stats.failures
                  << " signals=" << signal_processor.signals_published()
                  << " API submissions=" << execution_worker.requests_submitted()
                  << " fills=" << position_book.fills_applied() << '\n';
        for (const auto& [symbol, quantity] : position_book.snapshot()) {
            std::cout << "  position[" << symbol << "]=" << quantity << '\n';
        }

        std::cout << "Workers:\n";
        print_worker("signal", signal_stats);
        print_worker("execution", execution_stats);
        print_worker("position", monitor_stats);

        std::cout << "Infrastructure:\n";
        std::cout << "  event_bus events=" << bus_stats.events_published
                  << " deliveries=" << bus_stats.subscriber_deliveries
                  << " full=" << bus_stats.rejected_full
                  << " closed=" << bus_stats.rejected_closed << '\n';
        std::cout << "  api accepted=" << api_stats.accepted
                  << " completed=" << api_stats.completed << " failed=" << api_stats.failed
                  << " cancelled=" << api_stats.cancelled << " retries=" << api_stats.retries
                  << " max_pending=" << api_stats.max_pending << '\n';
        print_latency("market-to-position", position_monitor.end_to_end_latency());
        print_latency("API queue delay", position_monitor.api_queue_latency());
        print_latency("rate-limit wait", position_monitor.rate_limit_wait());

        const auto failures = producer_stats.failures + producer_stats.subscriber_rejections +
                              bus_stats.rejected_full + bus_stats.rejected_closed +
                              signal_stats.handler_errors +
                              execution_stats.handler_errors + monitor_stats.handler_errors +
                              execution_worker.requests_rejected() +
                              execution_worker.completion_publish_rejections() +
                              api_stats.failed + api_stats.cancelled;
        return failures == 0U ? 0 : 2;
    } catch (const std::exception& exception) {
        std::cerr << "fatal: " << exception.what() << '\n';
        std::cerr << trading::AppConfig::usage(argv[0]);
        return 1;
    }
}
