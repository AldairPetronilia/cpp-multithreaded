#include "trading/config.hpp"

#include <charconv>
#include <cstddef>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace trading {
namespace {

template <typename Integer>
Integer parse_integer(std::string_view value, std::string_view option) {
    Integer parsed{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size()) {
        throw std::invalid_argument("invalid value for " + std::string(option));
    }
    return parsed;
}

double parse_double(std::string_view value, std::string_view option) {
    std::string owned(value);
    char* end = nullptr;
    const auto parsed = std::strtod(owned.c_str(), &end);
    if (end != owned.c_str() + owned.size()) {
        throw std::invalid_argument("invalid value for " + std::string(option));
    }
    return parsed;
}

std::string_view next_value(int& index, int argc, char* argv[], std::string_view option) {
    if (index + 1 >= argc) {
        throw std::invalid_argument("missing value for " + std::string(option));
    }
    ++index;
    return argv[index];
}

}  // namespace

AppConfig AppConfig::from_command_line(int argc, char* argv[]) {
    AppConfig config;
    config.market_data.midpoint = config.signal.threshold;

    for (int index = 1; index < argc; ++index) {
        const std::string_view option(argv[index]);
        if (option == "--help" || option == "-h") {
            config.show_help = true;
        } else if (option == "--events") {
            config.market_data.event_count = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else if (option == "--interval-us") {
            config.market_data.interval = std::chrono::microseconds(
                parse_integer<std::int64_t>(next_value(index, argc, argv, option), option));
        } else if (option == "--symbol") {
            config.market_data.symbol = next_value(index, argc, argv, option);
        } else if (option == "--threshold") {
            const auto threshold =
                parse_double(next_value(index, argc, argv, option), option);
            config.signal.threshold = threshold;
            config.market_data.midpoint = threshold;
        } else if (option == "--amplitude") {
            config.market_data.amplitude =
                parse_double(next_value(index, argc, argv, option), option);
        } else if (option == "--quantity") {
            config.signal.order_quantity =
                parse_double(next_value(index, argc, argv, option), option);
        } else if (option == "--mailbox-capacity") {
            const auto capacity = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
            config.signal.mailbox_capacity = capacity;
            config.execution.mailbox_capacity = capacity;
            config.position_monitor.mailbox_capacity = capacity;
        } else if (option == "--rate-limit") {
            config.api_manager.rate_limit.max_requests = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else if (option == "--rate-window-ms") {
            config.api_manager.rate_limit.window = std::chrono::milliseconds(
                parse_integer<std::int64_t>(next_value(index, argc, argv, option), option));
        } else if (option == "--api-threads") {
            config.api_manager.io_threads = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else if (option == "--api-queue-capacity") {
            config.api_manager.max_outstanding_requests = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else if (option == "--api-latency-ms") {
            config.mock_api.latency = std::chrono::milliseconds(
                parse_integer<std::int64_t>(next_value(index, argc, argv, option), option));
        } else if (option == "--request-timeout-ms") {
            config.execution.request_timeout = std::chrono::milliseconds(
                parse_integer<std::int64_t>(next_value(index, argc, argv, option), option));
        } else if (option == "--max-retries") {
            config.api_manager.max_retries = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else if (option == "--mock-429-every") {
            config.mock_api.respond_429_every = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else if (option == "--mock-connection-failure-every") {
            config.mock_api.fail_connection_every = parse_integer<std::size_t>(
                next_value(index, argc, argv, option), option);
        } else {
            throw std::invalid_argument("unknown option: " + std::string(option));
        }
    }

    return config;
}

std::string AppConfig::usage(const char* executable_name) {
    return std::string("Usage: ") + executable_name + R"( [options]
  --events N                         Market-data events (default 500)
  --interval-us N                    Producer interval in microseconds (default 1000)
  --symbol NAME                      Simulated instrument (default DEMO)
  --threshold PRICE                  Signal threshold and price midpoint (default 100)
  --amplitude PRICE                  Simulated price amplitude (default 2)
  --quantity N                       Quantity per signal (default 1)
  --mailbox-capacity N               Capacity of each worker mailbox (default 1024)
  --rate-limit N                     Shared request allowance (default 10)
  --rate-window-ms N                 Rolling rate-limit window (default 1000)
  --api-threads N                    Mock API I/O threads (default 2)
  --api-queue-capacity N             Maximum outstanding API requests (default 1024)
  --api-latency-ms N                 Mock API latency (default 5)
  --request-timeout-ms N             Per-request timeout (default 500)
  --max-retries N                    Safe retry count (default 2)
  --mock-429-every N                 Return 429 on every Nth call (default disabled)
  --mock-connection-failure-every N  Fail every Nth call (default disabled)
  --help                             Show this help
)";
}

}  // namespace trading

