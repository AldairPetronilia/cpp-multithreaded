#include "trading/config.hpp"

#include <charconv>
#include <cstdlib>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace trading {
namespace {

template <typename Integer>
Integer parse_integer(std::string_view value, std::string_view option) {
    Integer parsed{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || error != std::errc{} || end != value.data() + value.size()) {
        throw std::invalid_argument("invalid value for " + std::string(option) + ": " + std::string(value));
    }
    return parsed;
}

double parse_double(std::string_view value, std::string_view option) {
    const std::string owned(value);
    char* end = nullptr;
    const auto parsed = std::strtod(owned.c_str(), &end);
    if (owned.empty() || end != owned.c_str() + owned.size()) {
        throw std::invalid_argument("invalid value for " + std::string(option) + ": " + owned);
    }
    return parsed;
}

std::vector<std::string_view> split(std::string_view text, char separator) {
    std::vector<std::string_view> parts;
    for (;;) {
        const auto position = text.find(separator);
        parts.push_back(text.substr(0, position));
        if (position == std::string_view::npos) {
            return parts;
        }
        text.remove_prefix(position + 1);
    }
}

// NAME:SYMBOL[@PRICE][,SYMBOL[@PRICE]...]
MarketDataProducerConfig parse_feed(std::string_view spec, std::uint64_t seed) {
    const auto parts = split(spec, ':');
    if (parts.size() != 2U || parts[0].empty() || parts[1].empty()) {
        throw std::invalid_argument("--feed expects NAME:SYMBOL[@PRICE],...; got " + std::string(spec));
    }
    MarketDataProducerConfig feed;
    feed.name = parts[0];
    feed.seed = seed;
    for (const auto instrument : split(parts[1], ',')) {
        const auto at = instrument.find('@');
        InstrumentConfig config;
        config.symbol = instrument.substr(0, at);
        if (at != std::string_view::npos) {
            config.reference_price = parse_double(instrument.substr(at + 1), "--feed price");
        }
        feed.instruments.push_back(std::move(config));
    }
    return feed;
}

// NAME:KIND[:SYMBOL,SYMBOL...|*[:QUANTITY]]
StrategyConfig parse_strategy(std::string_view spec) {
    const auto parts = split(spec, ':');
    if (parts.size() < 2U || parts.size() > 4U || parts[0].empty() || parts[1].empty()) {
        throw std::invalid_argument("--strategy expects NAME:KIND[:SYMBOLS[:QUANTITY]]; got " +
                                    std::string(spec));
    }
    StrategyConfig strategy;
    strategy.name = parts[0];
    strategy.kind = parts[1];
    if (parts.size() >= 3U && parts[2] != "*" && !parts[2].empty()) {
        for (const auto symbol : split(parts[2], ',')) {
            strategy.symbols.emplace_back(symbol);
        }
    }
    if (parts.size() == 4U) {
        strategy.quantity = parse_double(parts[3], "--strategy quantity");
    }
    return strategy;
}

std::string_view next_value(int& index, int argc, char* argv[], std::string_view option) {
    if (index + 1 >= argc) {
        throw std::invalid_argument("missing value for " + std::string(option));
    }
    return argv[++index];
}

}  // namespace

AppConfig AppConfig::from_command_line(int argc, char* argv[]) {
    AppConfig config;
    std::vector<MarketDataProducerConfig> feeds;
    std::vector<StrategyConfig> strategies;
    std::optional<std::size_t> events;
    std::optional<std::chrono::microseconds> interval;
    auto& system = config.system;

    for (int index = 1; index < argc; ++index) {
        const std::string_view option(argv[index]);
        const auto value = [&] { return next_value(index, argc, argv, option); };
        if (option == "--help" || option == "-h") {
            config.show_help = true;
        } else if (option == "--events") {
            events = parse_integer<std::size_t>(value(), option);
        } else if (option == "--interval-us") {
            interval = std::chrono::microseconds(parse_integer<std::int64_t>(value(), option));
        } else if (option == "--feed") {
            feeds.push_back(parse_feed(value(), feeds.size() + 1U));
        } else if (option == "--strategy") {
            strategies.push_back(parse_strategy(value()));
        } else if (option == "--mailbox-capacity") {
            const auto capacity = parse_integer<std::size_t>(value(), option);
            system.strategy_worker.mailbox_capacity = capacity;
            system.execution.worker.mailbox_capacity = capacity;
            system.position_monitor.worker.mailbox_capacity = capacity;
        } else if (option == "--rate-limit") {
            system.api_manager.rate_limit.max_requests = parse_integer<std::size_t>(value(), option);
        } else if (option == "--rate-window-ms") {
            system.api_manager.rate_limit.window =
                std::chrono::milliseconds(parse_integer<std::int64_t>(value(), option));
        } else if (option == "--api-threads") {
            system.api_manager.io_threads = parse_integer<std::size_t>(value(), option);
        } else if (option == "--api-queue-capacity") {
            system.api_manager.max_outstanding_requests = parse_integer<std::size_t>(value(), option);
        } else if (option == "--api-latency-ms") {
            system.mock_api.latency = std::chrono::milliseconds(parse_integer<std::int64_t>(value(), option));
        } else if (option == "--request-timeout-ms") {
            system.execution.request_timeout =
                std::chrono::milliseconds(parse_integer<std::int64_t>(value(), option));
        } else if (option == "--max-retries") {
            system.api_manager.max_retries = parse_integer<std::size_t>(value(), option);
        } else if (option == "--mock-429-every") {
            system.mock_api.respond_429_every = parse_integer<std::size_t>(value(), option);
        } else if (option == "--mock-connection-failure-every") {
            system.mock_api.fail_connection_every = parse_integer<std::size_t>(value(), option);
        } else if (option == "--drain-timeout-ms") {
            system.drain_timeout = std::chrono::milliseconds(parse_integer<std::int64_t>(value(), option));
        } else if (option == "--dashboard") {
            config.dashboard = true;
        } else if (option == "--port") {
            config.dashboard_port = parse_integer<std::uint16_t>(value(), option);
        } else if (option == "--duration-s") {
            config.duration = std::chrono::seconds(parse_integer<std::int64_t>(value(), option));
        } else {
            throw std::invalid_argument("unknown option: " + std::string(option));
        }
    }

    if (!feeds.empty()) {
        system.feeds = std::move(feeds);
    }
    if (!strategies.empty()) {
        system.strategies = std::move(strategies);
    }
    // With the dashboard the demo runs until stopped unless an event count is given.
    const auto events_per_instrument = events.value_or(config.dashboard ? 0U : system.feeds.front().events_per_instrument);
    for (auto& feed : system.feeds) {
        feed.events_per_instrument = events_per_instrument;
        if (interval) {
            feed.interval = *interval;
        }
    }
    system.record_activity = config.dashboard;
    if (config.duration && config.duration->count() <= 0) {
        throw std::invalid_argument("--duration-s must be positive");
    }
    return config;
}

std::string AppConfig::usage(const char* executable_name) {
    return std::string("Usage: ") + executable_name + R"( [options]

Topology (each option is repeatable and replaces the built-in demo topology):
  --feed NAME:SYM[@PRICE],...        Market-data feed thread publishing these instruments
  --strategy NAME:KIND[:SYMS[:QTY]]  Signal processor; KIND is threshold or ema-cross,
                                     SYMS is SYM,SYM,... or '*' (quoted) for every
                                     instrument; omit SYMS for every instrument too

Run control:
  --events N                         Quotes per instrument, 0 = until stopped
                                     (default 3000, or 0 with --dashboard)
  --interval-us N                    Delay between feed rounds (default 1000)
  --duration-s N                     Stop after N seconds
  --dashboard                        Serve the live dashboard on 127.0.0.1
  --port N                           Dashboard port (default 8080)

Pipeline and API:
  --mailbox-capacity N               Strategy, execution and position mailboxes (default 1024)
  --rate-limit N                     Shared request allowance per window (default 10)
  --rate-window-ms N                 Rolling rate-limit window (default 1000)
  --api-threads N                    API I/O threads (default 2)
  --api-queue-capacity N             Maximum outstanding API requests (default 1024)
  --api-latency-ms N                 Mock API latency (default 5)
  --request-timeout-ms N             Per-request timeout (default 500)
  --max-retries N                    Retries for idempotent requests (default 2)
  --mock-429-every N                 Mock returns HTTP 429 on every Nth call
  --mock-connection-failure-every N  Mock fails every Nth call
  --drain-timeout-ms N               Cancel API requests unfinished this long into
                                     shutdown (default 5000)
  --help                             Show this help
)";
}

}  // namespace trading
