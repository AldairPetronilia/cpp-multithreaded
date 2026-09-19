#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/metrics.hpp"
#include "trading/strategies.hpp"
#include "trading/system.hpp"
#include "trading/telemetry.hpp"
#include "trading/workers.hpp"

#if TRADING_HAS_DASHBOARD
#include "trading/dashboard_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

#define CHECK(expression)                                                                  \
    do {                                                                                   \
        if (!(expression)) {                                                               \
            std::ostringstream failure_message;                                            \
            failure_message << __FILE__ << ':' << __LINE__ << ": check failed: "          \
                            << #expression;                                                \
            throw TestFailure(failure_message.str());                                      \
        }                                                                                  \
    } while (false)

// Polls a condition in tests only; production code waits on condition variables instead.
bool eventually(const std::function<bool()>& condition, std::chrono::milliseconds timeout) {
    const auto deadline = trading::MonotonicClock::now() + timeout;
    while (!condition()) {
        if (trading::MonotonicClock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

trading::MarketDataEvent quote(std::string symbol, double price, std::uint64_t sequence = 1) {
    return trading::MarketDataEvent{std::move(symbol), price, sequence, trading::MonotonicClock::now()};
}

std::vector<trading::EventEnvelope> drain(trading::BoundedMailbox& mailbox) {
    std::vector<trading::EventEnvelope> events;
    trading::EventEnvelope event;
    while (mailbox.try_pop(event)) {
        events.push_back(event);
    }
    return events;
}

class CompletionGroup {
public:
    explicit CompletionGroup(std::size_t expected) : expected_(expected) {}

    void complete(trading::ApiResponse response) {
        {
            std::lock_guard lock(mutex_);
            responses_.push_back(std::move(response));
        }
        wakeup_.notify_all();
    }

    bool wait_for(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return wakeup_.wait_for(lock, timeout, [this] { return responses_.size() == expected_; });
    }

    [[nodiscard]] std::vector<trading::ApiResponse> responses() const {
        std::lock_guard lock(mutex_);
        return responses_;
    }

    [[nodiscard]] trading::ApiCompletion callback() {
        return [this](trading::ApiResponse response) { complete(std::move(response)); };
    }

private:
    const std::size_t expected_;
    mutable std::mutex mutex_;
    std::condition_variable wakeup_;
    std::vector<trading::ApiResponse> responses_;
};

// Deterministic API double: records every call and optionally waits (cancellably).
class RecordingClient final : public trading::IApiClient {
public:
    explicit RecordingClient(std::chrono::milliseconds latency = 0ms) : latency_(latency) {}

    trading::ApiResponse perform(const trading::ApiRequest& request, std::stop_token cancellation) override {
        {
            std::lock_guard lock(mutex_);
            starts_.push_back(trading::MonotonicClock::now());
            keys_.push_back(request.idempotency_key);
        }
        started_wakeup_.notify_all();

        if (latency_ > 0ms) {
            std::mutex sleep_mutex;
            std::condition_variable_any sleeper;
            std::unique_lock lock(sleep_mutex);
            sleeper.wait_for(lock, cancellation, latency_, [] { return false; });
        }
        trading::ApiResponse response;
        if (cancellation.stop_requested()) {
            response.error = trading::ApiError::cancelled;
            response.message = "cancelled";
            return response;
        }
        response.status_code = 200;
        response.message = "ok";
        return response;
    }

    bool wait_for_starts(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return started_wakeup_.wait_for(lock, timeout, [this, count] { return starts_.size() >= count; });
    }

    [[nodiscard]] std::vector<trading::TimePoint> starts() const {
        std::lock_guard lock(mutex_);
        return starts_;
    }

    [[nodiscard]] std::vector<std::string> keys() const {
        std::lock_guard lock(mutex_);
        return keys_;
    }

private:
    const std::chrono::milliseconds latency_;
    mutable std::mutex mutex_;
    std::condition_variable started_wakeup_;
    std::vector<trading::TimePoint> starts_;
    std::vector<std::string> keys_;
};

class RetryClient final : public trading::IApiClient {
public:
    trading::ApiResponse perform(const trading::ApiRequest& request, std::stop_token) override {
        std::lock_guard lock(mutex_);
        trading::ApiResponse response;
        if (++calls_[request.endpoint] == 1U) {
            response.error = trading::ApiError::connection_failure;
            response.message = "first call fails";
            return response;
        }
        response.status_code = 200;
        response.message = "ok";
        return response;
    }

    [[nodiscard]] std::size_t calls(const std::string& endpoint) const {
        std::lock_guard lock(mutex_);
        const auto found = calls_.find(endpoint);
        return found == calls_.end() ? 0U : found->second;
    }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::size_t> calls_;
};

class RetryAfterClient final : public trading::IApiClient {
public:
    trading::ApiResponse perform(const trading::ApiRequest&, std::stop_token) override {
        std::lock_guard lock(mutex_);
        starts_.push_back(trading::MonotonicClock::now());
        trading::ApiResponse response;
        if (starts_.size() == 1U) {
            response.status_code = 429;
            response.error = trading::ApiError::rate_limited;
            response.retry_after = 40ms;
            return response;
        }
        response.status_code = 200;
        return response;
    }

    [[nodiscard]] std::vector<trading::TimePoint> starts() const {
        std::lock_guard lock(mutex_);
        return starts_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<trading::TimePoint> starts_;
};

trading::ApiManagerConfig fast_api_config(std::size_t io_threads = 2) {
    trading::ApiManagerConfig config;
    config.rate_limit = {1'000, 10ms};
    config.io_threads = io_threads;
    config.max_outstanding_requests = 64;
    config.max_retries = 0;
    return config;
}

// --- Messaging -------------------------------------------------------------------------

void messaging_delivers_to_every_matching_subscriber() {
    trading::EventBus bus;
    auto first = std::make_shared<trading::BoundedMailbox>(8);
    auto second = std::make_shared<trading::BoundedMailbox>(8);
    auto signals_only = std::make_shared<trading::BoundedMailbox>(8);
    const auto first_subscription = bus.subscribe<trading::MarketDataEvent>(first);
    const auto second_subscription = bus.subscribe<trading::MarketDataEvent>(second);
    const auto signal_subscription = bus.subscribe<trading::TradingSignalEvent>(signals_only);

    const auto result = bus.publish(quote("TEST", 101.5));
    CHECK(result.subscribers == 2U);
    CHECK(result.all_delivered());

    trading::EventEnvelope event;
    CHECK(first->try_pop(event));
    CHECK(event.as<trading::MarketDataEvent>() != nullptr);
    CHECK(event.as<trading::MarketDataEvent>()->price == 101.5);
    CHECK(event.as<trading::TradingSignalEvent>() == nullptr);
    CHECK(second->try_pop(event));
    CHECK(!signals_only->try_pop(event));
}

void topics_partition_market_data_between_strategies() {
    trading::EventBus bus;
    const auto aapl = bus.topic("AAPL");
    const auto msft = bus.topic("MSFT");
    CHECK(bus.topic("AAPL") == aapl);
    CHECK(bus.topic_name(msft) == "MSFT");

    auto aapl_strategy = std::make_shared<trading::BoundedMailbox>(8);
    auto msft_strategy = std::make_shared<trading::BoundedMailbox>(8);
    auto recorder = std::make_shared<trading::BoundedMailbox>(8);
    const auto a = bus.subscribe<trading::MarketDataEvent>(aapl_strategy, aapl, "aapl-strategy");
    const auto b = bus.subscribe<trading::MarketDataEvent>(msft_strategy, msft, "msft-strategy");
    const auto c = bus.subscribe<trading::MarketDataEvent>(recorder, "recorder");

    CHECK(bus.publish(quote("AAPL", 190.0), aapl).delivered == 2U);
    CHECK(bus.publish(quote("MSFT", 420.0), msft).delivered == 2U);
    CHECK(bus.publish(quote("UNTAGGED", 1.0)).delivered == 1U);

    const auto aapl_events = drain(*aapl_strategy);
    CHECK(aapl_events.size() == 1U);
    CHECK(aapl_events.front().as<trading::MarketDataEvent>()->symbol == "AAPL");
    CHECK(aapl_events.front().topic == aapl);
    const auto msft_events = drain(*msft_strategy);
    CHECK(msft_events.size() == 1U);
    CHECK(msft_events.front().as<trading::MarketDataEvent>()->symbol == "MSFT");
    CHECK(drain(*recorder).size() == 3U);

    const auto routes = bus.routes();
    CHECK(routes.size() == 3U);
    CHECK(std::any_of(routes.begin(), routes.end(), [](const trading::RouteInfo& route) {
        return route.topic == "AAPL" && route.subscriber == "aapl-strategy" && route.event_type == "MarketData";
    }));
    CHECK(std::any_of(routes.begin(), routes.end(),
                      [](const trading::RouteInfo& route) { return route.topic == "*"; }));

    bool rejected_unknown_topic = false;
    try {
        [[maybe_unused]] const auto invalid = bus.subscribe<trading::MarketDataEvent>(recorder, trading::Topic{99});
    } catch (const std::invalid_argument&) {
        rejected_unknown_topic = true;
    }
    CHECK(rejected_unknown_topic);
}

void unsubscribing_stops_delivery() {
    trading::EventBus bus;
    const auto topic = bus.topic("AAPL");
    auto mailbox = std::make_shared<trading::BoundedMailbox>(8);
    auto subscription = bus.subscribe<trading::MarketDataEvent>(mailbox, topic);
    CHECK(bus.publish(quote("AAPL", 1.0), topic).delivered == 1U);
    subscription.reset();
    CHECK(bus.publish(quote("AAPL", 2.0), topic).subscribers == 0U);
    CHECK(bus.stats().events_without_subscribers == 1U);
    CHECK(mailbox->size() == 1U);
}

void messaging_is_safe_with_concurrent_publishers() {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t events_per_producer = 2'000;
    constexpr std::size_t total_events = producer_count * events_per_producer;

    trading::EventBus bus;
    auto mailbox = std::make_shared<trading::BoundedMailbox>(total_events);
    const auto subscription = bus.subscribe<trading::BenchmarkEvent>(mailbox);
    std::atomic<std::size_t> publication_failures{0};
    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&bus, &publication_failures, producer] {
            for (std::size_t sequence = 0; sequence < events_per_producer; ++sequence) {
                if (bus.publish(trading::BenchmarkEvent{producer, sequence}).delivered != 1U) {
                    publication_failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }

    CHECK(publication_failures.load() == 0U);
    std::set<std::pair<std::uint64_t, std::uint64_t>> received;
    std::map<std::uint64_t, std::uint64_t> last_sequence;
    bool per_producer_order_kept = true;
    for (const auto& envelope : drain(*mailbox)) {
        const auto* event = envelope.as<trading::BenchmarkEvent>();
        CHECK(event != nullptr);
        received.emplace(event->producer_id, event->sequence);
        const auto previous = last_sequence.find(event->producer_id);
        if (previous != last_sequence.end() && previous->second >= event->sequence) {
            per_producer_order_kept = false;
        }
        last_sequence[event->producer_id] = event->sequence;
    }
    CHECK(received.size() == total_events);
    CHECK(per_producer_order_kept);
    CHECK(mailbox->stats().accepted == total_events);
}

void full_mailboxes_follow_their_overflow_policy() {
    trading::EventBus bus;
    auto orders = std::make_shared<trading::BoundedMailbox>(2, trading::OverflowPolicy::reject_newest);
    auto quotes = std::make_shared<trading::BoundedMailbox>(2, trading::OverflowPolicy::drop_oldest);
    const auto order_subscription = bus.subscribe<trading::BenchmarkEvent>(orders);
    const auto quote_subscription = bus.subscribe<trading::BenchmarkEvent>(quotes);

    for (std::uint64_t sequence = 1; sequence <= 5; ++sequence) {
        const auto result = bus.publish(trading::BenchmarkEvent{1, sequence});
        CHECK(result.rejected_full == (sequence > 2U ? 1U : 0U));
    }

    const auto sequences = [](trading::BoundedMailbox& mailbox) {
        std::vector<std::uint64_t> values;
        for (const auto& envelope : drain(mailbox)) {
            values.push_back(envelope.as<trading::BenchmarkEvent>()->sequence);
        }
        return values;
    };
    CHECK(orders->stats().rejected_full == 3U);
    CHECK(sequences(*orders) == (std::vector<std::uint64_t>{1, 2}));
    CHECK(quotes->stats().evicted_oldest == 3U);
    CHECK(sequences(*quotes) == (std::vector<std::uint64_t>{4, 5}));
}

void mailbox_batches_preserve_order_and_drain_after_close() {
    trading::BoundedMailbox mailbox(16);
    for (std::uint64_t sequence = 0; sequence < 10; ++sequence) {
        CHECK(mailbox.try_enqueue(trading::EventEnvelope{
                  typeid(trading::BenchmarkEvent), {}, trading::MonotonicClock::now(),
                  std::make_shared<const trading::BenchmarkEvent>(trading::BenchmarkEvent{0, sequence})}) ==
              trading::EnqueueResult::accepted);
    }
    mailbox.close();
    CHECK(mailbox.try_enqueue(trading::EventEnvelope{}) == trading::EnqueueResult::closed);

    std::vector<trading::EventEnvelope> batch;
    std::vector<std::uint64_t> seen;
    while (mailbox.wait_pop_batch(batch, 4)) {
        CHECK(batch.size() <= 4U);
        for (const auto& envelope : batch) {
            seen.push_back(envelope.as<trading::BenchmarkEvent>()->sequence);
        }
    }
    CHECK(seen == (std::vector<std::uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    CHECK(mailbox.stats().rejected_closed == 1U);
}

// --- Metrics ---------------------------------------------------------------------------

void histogram_percentiles_are_within_bucket_precision() {
    trading::LatencyHistogram histogram;
    for (std::int64_t value = 1; value <= 10'000; ++value) {
        histogram.record(std::chrono::microseconds(value));
    }
    const auto summary = histogram.summary();
    const auto near = [](double actual, double expected) {
        return std::abs(actual - expected) <= expected * 0.035;
    };
    CHECK(summary.count == 10'000U);
    CHECK(near(summary.p50_us, 5'000.0));
    CHECK(near(summary.p99_us, 9'900.0));
    CHECK(summary.max_us == 10'000.0);
    CHECK(near(summary.mean_us, 5'000.5));

    for (std::uint64_t value : {0ULL, 1ULL, 31ULL, 63ULL, 64ULL, 1'000'000ULL, (1ULL << 36U) - 1U}) {
        const auto index = trading::LatencyHistogram::bucket_index(value);
        CHECK(index < trading::LatencyHistogram::kBucketCount);
        CHECK(trading::LatencyHistogram::bucket_upper_bound(index) >= value);
        CHECK(index == 0U || trading::LatencyHistogram::bucket_upper_bound(index - 1U) < value);
    }

    const auto before = histogram.snapshot();
    histogram.record(50ms);
    const auto recent = histogram.snapshot().since(before);
    CHECK(recent.count() == 1U);
    CHECK(near(recent.percentile_ns(0.5) / 1'000.0, 50'000.0));
}


// --- Strategies and workers ------------------------------------------------------------

void threshold_strategy_signals_on_band_breaks_only() {
    trading::ThresholdStrategy strategy(trading::ThresholdStrategyConfig{10.0});
    CHECK(!strategy.on_market_data(quote("X", 100.00)));  // anchor
    CHECK(!strategy.on_market_data(quote("X", 100.05)));  // inside the 10 bps band
    CHECK(strategy.on_market_data(quote("X", 100.11)) == trading::Side::buy);
    CHECK(!strategy.on_market_data(quote("X", 100.20)));  // already signalled long
    CHECK(!strategy.on_market_data(quote("X", 99.95)));   // back inside the band
    CHECK(strategy.on_market_data(quote("X", 99.89)) == trading::Side::sell);
    CHECK(!strategy.on_market_data(quote("Y", 50.0)));    // state is per symbol
}

void ema_cross_strategy_trades_crossovers_after_warm_up() {
    trading::EmaCrossStrategy strategy(trading::EmaCrossStrategyConfig{2, 4, 0.0});
    std::vector<trading::Side> signals;
    const auto feed = [&](double price) {
        if (const auto side = strategy.on_market_data(quote("X", price))) {
            signals.push_back(*side);
        }
    };
    for (int step = 0; step < 10; ++step) {
        feed(100.0 + step);  // the first regime after warm-up is observed, not traded
    }
    for (int step = 0; step < 10; ++step) {
        feed(110.0 - 2.0 * step);
    }
    for (int step = 0; step < 10; ++step) {
        feed(90.0 + 2.0 * step);
    }
    CHECK(signals == (std::vector<trading::Side>{trading::Side::sell, trading::Side::buy}));
}

void slow_strategy_does_not_delay_other_strategies() {
    trading::EventBus bus;
    const auto topic = bus.topic("AAPL");
    auto slow_strategy = std::make_unique<trading::SimulatedLoadStrategy>(std::make_unique<trading::ThresholdStrategy>());
    slow_strategy->set_work_per_event(5ms);
    trading::SignalProcessor slow(
        bus, trading::SignalProcessorConfig{"slow", {"AAPL"}, 1.0, {8, trading::OverflowPolicy::drop_oldest, 1}},
        std::move(slow_strategy));
    trading::SignalProcessor fast(
        bus, trading::SignalProcessorConfig{"fast", {"AAPL"}, 1.0, {1'024, trading::OverflowPolicy::drop_oldest, 64}},
        std::make_unique<trading::ThresholdStrategy>());
    slow.start();
    fast.start();

    const auto started = trading::MonotonicClock::now();
    for (std::uint64_t sequence = 1; sequence <= 200; ++sequence) {
        CHECK(bus.publish(quote("AAPL", 100.0, sequence), topic).delivered == 2U);
    }
    // Had publishing waited for the slow consumer it would have taken at least 1 s.
    CHECK(trading::MonotonicClock::now() - started < 100ms);
    CHECK(eventually([&] { return fast.stats().worker.processed == 200U; }, 1s));
    CHECK(slow.stats().worker.mailbox.evicted_oldest > 0U);
    CHECK(fast.stats().worker.mailbox.evicted_oldest == 0U);
    slow.stop();
    fast.stop();
}

void order_ids_are_unique_across_strategies() {
    trading::EventBus bus;
    auto client = std::make_shared<RecordingClient>();
    trading::ApiRequestManager manager(client, fast_api_config());
    trading::OrderExecutionWorker execution(bus, manager, trading::OrderExecutionConfig{1s, {}});
    auto reports = std::make_shared<trading::BoundedMailbox>(8);
    const auto subscription = bus.subscribe<trading::ExecutionReportEvent>(reports);
    execution.start();

    // Both strategies number their first signal 1; their orders must not share a key.
    for (const char* strategy : {"alpha", "beta"}) {
        const auto now = trading::MonotonicClock::now();
        CHECK(bus.publish(trading::TradingSignalEvent{
                              .strategy = strategy,
                              .signal_id = 1,
                              .symbol = "AAPL",
                              .side = trading::Side::buy,
                              .quantity = 1.0,
                              .reference_price = 100.0,
                              .market_data_time = now,
                              .signal_time = now,
                          })
                  .delivered == 1U);
    }
    CHECK(eventually([&] { return reports->size() == 2U; }, 1s));
    execution.stop();
    manager.shutdown();

    auto keys = client->keys();
    std::sort(keys.begin(), keys.end());
    CHECK(keys == (std::vector<std::string>{"alpha-1", "beta-1"}));
    for (const auto& envelope : drain(*reports)) {
        CHECK(envelope.as<trading::ExecutionReportEvent>()->filled);
    }
}

void position_book_is_thread_safe() {
    trading::PositionBook book;
    constexpr std::size_t thread_count = 8;
    constexpr std::size_t fills_per_thread = 1'000;
    std::vector<std::thread> threads;
    for (std::size_t thread = 0; thread < thread_count; ++thread) {
        threads.emplace_back([&book, thread] {
            for (std::size_t index = 0; index < fills_per_thread; ++index) {
                book.apply(trading::ExecutionReportEvent{
                    .strategy = thread < 4U ? "alpha" : "beta",
                    .signal_id = index,
                    .symbol = "TEST",
                    .side = thread % 2U == 0U ? trading::Side::buy : trading::Side::sell,
                    .quantity = 1.0,
                    .fill_price = 100.0,
                    .filled = true,
                });
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    CHECK(book.fills_applied() == thread_count * fills_per_thread);
    CHECK(book.net_position("TEST") == 0.0);
    CHECK(book.position("alpha", "TEST") == 0.0);
    for (const auto& entry : book.snapshot()) {
        CHECK(entry.fills == 4'000U);
        CHECK(entry.cash == 0.0);
    }
}

// --- Shared API manager ----------------------------------------------------------------

void shared_rate_limit_applies_across_submitters() {
    auto client = std::make_shared<RecordingClient>();
    trading::ApiManagerConfig config;
    config.rate_limit = {2, 60ms};
    config.io_threads = 4;
    config.max_outstanding_requests = 16;
    config.max_retries = 0;
    trading::ApiRequestManager manager(client, config);
    CompletionGroup completions(6);

    std::vector<std::thread> submitters;
    std::atomic<std::size_t> submission_failures{0};
    for (std::size_t submitter = 0; submitter < 3; ++submitter) {
        submitters.emplace_back([&] {
            for (std::size_t index = 0; index < 2; ++index) {
                if (!manager.submit(trading::ApiRequest{}, completions.callback()).accepted()) {
                    submission_failures.fetch_add(1);
                }
            }
        });
    }
    for (auto& submitter : submitters) {
        submitter.join();
    }
    CHECK(submission_failures.load() == 0U);
    manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(completions.wait_for(10ms));

    auto starts = client->starts();
    CHECK(starts.size() == 6U);
    std::sort(starts.begin(), starts.end());
    // At most two requests may start in any 60 ms window (small allowance for jitter in
    // when the client records its start).
    for (std::size_t index = 2; index < starts.size(); ++index) {
        CHECK(starts[index] - starts[index - 2] >= 50ms);
    }
    CHECK(manager.stats().limiter.max_requests == 2U);
}

void slow_api_does_not_block_unrelated_event_processing() {
    trading::EventBus bus;
    auto client = std::make_shared<RecordingClient>(250ms);
    trading::ApiRequestManager manager(client, fast_api_config(1));
    trading::OrderExecutionWorker execution(bus, manager, trading::OrderExecutionConfig{1s, {64}});

    std::atomic<std::size_t> market_events{0};
    trading::EventWorker market_worker("market", bus, trading::WorkerOptions{64},
                                       [&](const trading::EventEnvelope&) { market_events.fetch_add(1); });
    market_worker.subscribe<trading::MarketDataEvent>();
    market_worker.start();
    execution.start();

    const auto now = trading::MonotonicClock::now();
    CHECK(bus.publish(trading::TradingSignalEvent{"s", 1, "TEST", trading::Side::buy, 1.0, 100.0, now, now})
              .delivered == 1U);
    CHECK(client->wait_for_starts(1, 500ms));

    const auto processing_started = trading::MonotonicClock::now();
    for (std::uint64_t index = 0; index < 20; ++index) {
        CHECK(bus.publish(quote("TEST", 100.0, index)).delivered == 1U);
    }
    CHECK(eventually([&] { return market_events.load() == 20U; }, 200ms));
    // The API call is still in flight; market data was never queued behind it.
    CHECK(trading::MonotonicClock::now() - processing_started < 200ms);
    CHECK(manager.stats().active == 1U);

    market_worker.stop();
    execution.stop();
    manager.shutdown(trading::ApiShutdownMode::drain);
}

void retry_policy_requires_idempotency() {
    auto client = std::make_shared<RetryClient>();
    auto config = fast_api_config();
    config.max_retries = 2;
    config.initial_retry_backoff = 1ms;
    config.max_retry_backoff = 2ms;
    trading::ApiRequestManager manager(client, config);
    CompletionGroup completions(2);

    trading::ApiRequest safe;
    safe.endpoint = "/safe";
    safe.idempotent = true;
    safe.idempotency_key = "safe-key";
    CHECK(manager.submit(std::move(safe), completions.callback()).accepted());
    trading::ApiRequest unsafe;
    unsafe.endpoint = "/unsafe";
    CHECK(manager.submit(std::move(unsafe), completions.callback()).accepted());

    manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(completions.wait_for(10ms));
    CHECK(client->calls("/safe") == 2U);
    CHECK(client->calls("/unsafe") == 1U);
    const auto responses = completions.responses();
    CHECK(std::count_if(responses.begin(), responses.end(), [](const auto& response) { return response.ok(); }) == 1);
    CHECK(manager.stats().connection_failures == 2U);
}

void retry_after_and_timeouts_are_honoured() {
    auto retry_client = std::make_shared<RetryAfterClient>();
    auto retry_config = fast_api_config(1);
    retry_config.max_retries = 1;
    trading::ApiRequestManager retry_manager(retry_client, retry_config);
    CompletionGroup retry_completion(1);
    trading::ApiRequest retry_request;
    retry_request.idempotent = true;
    retry_request.idempotency_key = "retry-after-key";
    CHECK(retry_manager.submit(std::move(retry_request), retry_completion.callback()).accepted());
    retry_manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(retry_completion.wait_for(10ms));
    const auto retry_starts = retry_client->starts();
    CHECK(retry_starts.size() == 2U);
    CHECK(retry_starts[1] - retry_starts[0] >= 35ms);
    CHECK(retry_completion.responses().front().ok());
    CHECK(retry_completion.responses().front().attempts == 2U);
    CHECK(retry_manager.stats().rate_limited_responses == 1U);

    auto timeout_client = std::make_shared<trading::MockTradingApi>(trading::MockApiConfig{50ms});
    trading::ApiRequestManager timeout_manager(timeout_client, fast_api_config(1));
    CompletionGroup timeout_completion(1);
    trading::ApiRequest timeout_request;
    timeout_request.timeout = 10ms;
    CHECK(timeout_manager.submit(std::move(timeout_request), timeout_completion.callback()).accepted());
    timeout_manager.shutdown(trading::ApiShutdownMode::drain);
    CHECK(timeout_completion.wait_for(10ms));
    CHECK(timeout_completion.responses().front().error == trading::ApiError::timeout);
    CHECK(timeout_manager.stats().timeouts == 1U);
}

void cancellation_and_shutdown_are_bounded() {
    auto client = std::make_shared<RecordingClient>(5s);
    auto config = fast_api_config(2);
    trading::ApiRequestManager manager(client, config);
    CompletionGroup completions(10);
    for (std::size_t index = 0; index < 10; ++index) {
        trading::ApiRequest request;
        request.timeout = 10s;
        CHECK(manager.submit(std::move(request), completions.callback()).accepted());
    }
    CHECK(client->wait_for_starts(2, 500ms));

    const auto shutdown_started = trading::MonotonicClock::now();
    manager.shutdown(trading::ApiShutdownMode::cancel_pending);
    CHECK(trading::MonotonicClock::now() - shutdown_started < 500ms);
    CHECK(completions.wait_for(10ms));
    CHECK(manager.stats().cancelled == 10U);
    CHECK(!manager.submit(trading::ApiRequest{}, completions.callback()).accepted());

    trading::EventBus bus;
    trading::EventWorker idle("idle", bus, trading::WorkerOptions{4}, [](const trading::EventEnvelope&) {});
    idle.subscribe<trading::BenchmarkEvent>();
    idle.start();
    const auto worker_stop_started = trading::MonotonicClock::now();
    idle.stop();
    CHECK(trading::MonotonicClock::now() - worker_stop_started < 200ms);
}

void drain_timeout_cancels_requests_that_cannot_finish() {
    auto client = std::make_shared<RecordingClient>(5s);
    trading::ApiRequestManager manager(client, fast_api_config(1));
    CompletionGroup completions(3);
    for (std::size_t index = 0; index < 3; ++index) {
        trading::ApiRequest request;
        request.timeout = 10s;
        CHECK(manager.submit(std::move(request), completions.callback()).accepted());
    }
    CHECK(client->wait_for_starts(1, 500ms));
    const auto started = trading::MonotonicClock::now();
    manager.shutdown(trading::ApiShutdownMode::drain, 50ms);
    CHECK(trading::MonotonicClock::now() - started < 1s);
    CHECK(completions.wait_for(100ms));
    CHECK(manager.stats().cancelled == 3U);
}

void concurrent_shutdown_calls_are_safe() {
    auto client = std::make_shared<RecordingClient>(20ms);
    trading::ApiRequestManager manager(client, fast_api_config(2));
    CompletionGroup completions(4);
    for (std::size_t index = 0; index < 4; ++index) {
        CHECK(manager.submit(trading::ApiRequest{}, completions.callback()).accepted());
    }
    std::thread drainer([&] { manager.shutdown(trading::ApiShutdownMode::drain); });
    std::thread canceller([&] { manager.shutdown(trading::ApiShutdownMode::cancel_pending); });
    drainer.join();
    canceller.join();
    CHECK(completions.wait_for(100ms));
    CHECK(manager.stats().outstanding == 0U);
}

void mock_api_deduplicates_within_a_bounded_window() {
    trading::MockTradingApi api(trading::MockApiConfig{0ms, 2, 0ms, 0, 2});
    const auto call = [&api](const char* key) {
        trading::ApiRequest request;
        request.idempotent = true;
        request.idempotency_key = key;
        return api.perform(request, {}).status_code;
    };
    CHECK(call("a") == 200);  // call 1
    CHECK(call("a") == 200);  // call 2 would be a 429; the remembered result is returned
    CHECK(call("b") == 200);  // call 3
    CHECK(call("c") == 429);  // call 4: failures are not remembered
    CHECK(call("c") == 200);  // call 5: remembers c and forgets a (capacity 2)
    CHECK(call("a") == 429);  // call 6: a is no longer deduplicated
}

// --- Market data, system composition and telemetry ------------------------------------

void producer_publishes_each_instrument_on_its_own_topic() {
    trading::EventBus bus;
    trading::MarketDataProducer producer(bus, trading::MarketDataProducerConfig{
                                                  .name = "feed",
                                                  .instruments = {{"AAA", 10.0, 1.0}, {"BBB", 20.0, 1.0}},
                                                  .events_per_instrument = 100,
                                                  .interval = 0us,
                                                  .seed = 3,
                                              });
    auto aaa = std::make_shared<trading::BoundedMailbox>(1'024);
    auto everything = std::make_shared<trading::BoundedMailbox>(1'024);
    const auto aaa_subscription = bus.subscribe<trading::MarketDataEvent>(aaa, bus.topic("AAA"));
    const auto all_subscription = bus.subscribe<trading::MarketDataEvent>(everything);
    producer.start();
    producer.wait();

    const auto stats = producer.stats();
    CHECK(stats.events_published == 200U);
    CHECK(stats.finished);
    std::uint64_t expected_sequence = 1;
    for (const auto& envelope : drain(*aaa)) {
        const auto* event = envelope.as<trading::MarketDataEvent>();
        CHECK(event->symbol == "AAA");
        CHECK(event->sequence == expected_sequence++);
        CHECK(event->price > 0.0);
    }
    CHECK(expected_sequence == 101U);
    CHECK(drain(*everything).size() == 200U);
}

void producer_pauses_and_resumes_without_polling() {
    trading::EventBus bus;
    trading::MarketDataProducer producer(bus, trading::MarketDataProducerConfig{
                                                  .name = "feed",
                                                  .instruments = {{"AAA", 10.0, 1.0}},
                                                  .events_per_instrument = 0,
                                                  .interval = 200us,
                                              });
    producer.set_paused(true);
    producer.start();
    std::this_thread::sleep_for(20ms);
    CHECK(producer.stats().events_published == 0U);
    CHECK(producer.stats().paused);

    producer.set_paused(false);
    CHECK(eventually([&] { return producer.stats().events_published > 10U; }, 1s));
    producer.set_paused(true);
    std::this_thread::sleep_for(5ms);
    const auto paused_count = producer.stats().events_published;
    std::this_thread::sleep_for(20ms);
    CHECK(producer.stats().events_published == paused_count);

    const auto stop_started = trading::MonotonicClock::now();
    producer.stop();
    producer.wait();
    CHECK(trading::MonotonicClock::now() - stop_started < 200ms);
}

// Small recursive-descent validator, enough to prove the telemetry document is well formed.
class JsonValidator {
public:
    explicit JsonValidator(std::string_view text) : text_(text) {}

    bool valid() {
        const bool parsed = value();
        skip_space();
        return parsed && position_ == text_.size();
    }

private:
    bool value() {
        skip_space();
        if (position_ >= text_.size()) {
            return false;
        }
        switch (text_[position_]) {
            case '{':
                return container('}', true);
            case '[':
                return container(']', false);
            case '"':
                return string();
            case 't':
                return literal("true");
            case 'f':
                return literal("false");
            case 'n':
                return literal("null");
            default:
                return number();
        }
    }

    bool container(char close, bool is_object) {
        ++position_;
        skip_space();
        if (consume(close)) {
            return true;
        }
        for (;;) {
            if (is_object) {
                skip_space();
                if (!string()) {
                    return false;
                }
                skip_space();
                if (!consume(':')) {
                    return false;
                }
            }
            if (!value()) {
                return false;
            }
            skip_space();
            if (consume(',')) {
                continue;
            }
            return consume(close);
        }
    }

    bool string() {
        if (!consume('"')) {
            return false;
        }
        while (position_ < text_.size()) {
            const char character = text_[position_++];
            if (character == '"') {
                return true;
            }
            if (character == '\\') {
                ++position_;
            } else if (static_cast<unsigned char>(character) < 0x20U) {
                return false;
            }
        }
        return false;
    }

    bool number() {
        const auto start = position_;
        while (position_ < text_.size() &&
               std::string_view("0123456789+-.eE").find(text_[position_]) != std::string_view::npos) {
            ++position_;
        }
        return position_ > start;
    }

    bool literal(std::string_view word) {
        if (text_.substr(position_, word.size()) != word) {
            return false;
        }
        position_ += word.size();
        return true;
    }

    void skip_space() {
        while (position_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[position_])) != 0) {
            ++position_;
        }
    }

    bool consume(char expected) {
        if (position_ < text_.size() && text_[position_] == expected) {
            ++position_;
            return true;
        }
        return false;
    }

    std::string_view text_;
    std::size_t position_{0};
};

void json_writer_escapes_and_nests() {
    trading::JsonWriter json;
    json.begin_object()
        .key("text").value("quote \" backslash \\ newline \n tab \t control \x01")
        .key("list").begin_array().value(1).value(2.5).value(true).null().end_array()
        .key("nan").value(std::nan(""))
        .key("empty").begin_object().end_object()
        .end_object();
    const auto document = json.take();
    CHECK(JsonValidator(document).valid());
    CHECK(document.find(R"("text":"quote \" backslash \\ newline \n tab \t control \u0001")") != std::string::npos);
    CHECK(document.find(R"("list":[1,2.5,true,null])") != std::string::npos);
    CHECK(document.find(R"("nan":null)") != std::string::npos);
    CHECK(!JsonValidator(R"({"a":1,})").valid());
}

void trading_system_runs_end_to_end() {
    trading::SystemConfig config;
    config.feeds = {trading::MarketDataProducerConfig{
        .name = "feed",
        .instruments = {{"AAA", 100.0, 1.0}, {"BBB", 50.0, 1.0}},
        .events_per_instrument = 6'000,
        .interval = 0us,
        .seed = 5,
    }};
    config.strategies = {
        trading::StrategyConfig{"trend", "ema-cross", {"AAA", "BBB"}, 1.0},
        trading::StrategyConfig{"levels", "threshold", {"AAA"}, 2.0},
    };
    // Capacity above the event count and reject-newest: nothing may be shed, so every
    // count below has to reconcile exactly.
    config.strategy_worker = {16'384, trading::OverflowPolicy::reject_newest, 64};
    config.api_manager = fast_api_config(2);
    config.api_manager.max_outstanding_requests = 1'024;
    config.mock_api.latency = 0ms;
    config.record_activity = true;

    trading::TradingSystem system(config);
    trading::TelemetryRenderer renderer;
    system.start();
    CHECK(JsonValidator(renderer.render(system.snapshot())).valid());
    CHECK(eventually([&] { return system.feeds_finished(); }, 20s));
    system.stop();
    system.stop();  // idempotent

    const auto snapshot = system.snapshot();
    CHECK(!snapshot.running);
    std::uint64_t signals = 0;
    for (const auto& strategy : snapshot.strategies) {
        signals += strategy.signals_published;
        CHECK(strategy.publish_rejections == 0U);
        CHECK(strategy.worker.mailbox.rejected_full == 0U);
    }
    CHECK(snapshot.strategies[0].worker.processed == 12'000U);
    CHECK(snapshot.strategies[1].worker.processed == 6'000U);
    CHECK(signals > 0U);
    CHECK(snapshot.execution.requests_submitted == signals);
    CHECK(snapshot.api.completed == signals);
    CHECK(snapshot.position_monitor.fills == signals);
    std::uint64_t position_fills = 0;
    for (const auto& position : snapshot.positions) {
        position_fills += position.fills;
    }
    CHECK(position_fills == signals);
    CHECK(snapshot.activity.has_value());
    CHECK(snapshot.activity->instruments.size() == 2U);
    CHECK(!snapshot.activity->tape.empty());

    const auto document = renderer.render(snapshot);
    CHECK(JsonValidator(document).valid());
    for (const char* key : {"\"feeds\":[", "\"strategies\":[", "\"api\":{", "\"prices\":[", "\"tape\":[", "\"routes\":["}) {
        CHECK(document.find(key) != std::string::npos);
    }
}

void system_configuration_is_validated() {
    const auto rejects = [](trading::SystemConfig config) {
        try {
            trading::TradingSystem system(std::move(config));
        } catch (const std::invalid_argument&) {
            return true;
        }
        return false;
    };
    auto unknown_symbol = trading::SystemConfig::demo();
    unknown_symbol.strategies.push_back({"typo", "threshold", {"APPL"}, 1.0});
    CHECK(rejects(unknown_symbol));
    auto unknown_kind = trading::SystemConfig::demo();
    unknown_kind.strategies.push_back({"mystery", "neural-net", {}, 1.0});
    CHECK(rejects(unknown_kind));
    auto duplicate_symbol = trading::SystemConfig::demo();
    duplicate_symbol.feeds[1].instruments.push_back({"AAPL", 1.0, 1.0});
    CHECK(rejects(duplicate_symbol));
    auto duplicate_strategy = trading::SystemConfig::demo();
    duplicate_strategy.strategies.push_back(duplicate_strategy.strategies.front());
    CHECK(rejects(duplicate_strategy));
    CHECK(!rejects(trading::SystemConfig::demo()));
}

#if TRADING_HAS_DASHBOARD
std::string http_exchange(std::uint16_t port,
                          const std::string& request,
                          const std::function<bool(const std::string&)>& complete = {}) {
    const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(descriptor >= 0);
    timeval timeout{2, 0};
    ::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    std::string response;
    if (::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0 &&
        ::send(descriptor, request.data(), request.size(), 0) == static_cast<ssize_t>(request.size())) {
        char buffer[4'096];
        for (;;) {
            const auto received = ::recv(descriptor, buffer, sizeof(buffer), 0);
            if (received <= 0) {
                break;
            }
            response.append(buffer, static_cast<std::size_t>(received));
            if (complete && complete(response)) {
                break;
            }
        }
    }
    ::close(descriptor);
    return response;
}

void dashboard_server_serves_page_stream_and_controls() {
    std::atomic<int> snapshots{0};
    std::mutex controls_mutex;
    std::vector<std::pair<std::string, std::string>> controls;
    trading::DashboardServer server(
        trading::DashboardServerConfig{"127.0.0.1", 0, 20ms, 4}, "<html>page</html>",
        [&] { return "{\"n\":" + std::to_string(++snapshots) + "}"; },
        [&](std::string_view name, std::string_view value) {
            std::lock_guard lock(controls_mutex);
            controls.emplace_back(name, value);
            return trading::ControlResult{};
        });
    server.start();
    const auto port = server.port();
    CHECK(port != 0U);
    const auto host = "Host: 127.0.0.1:" + std::to_string(port) + "\r\n";

    const auto page = http_exchange(port, "GET / HTTP/1.1\r\n" + host + "\r\n");
    CHECK(page.starts_with("HTTP/1.1 200"));
    CHECK(page.find("<html>page</html>") != std::string::npos);

    CHECK(eventually([&] { return snapshots.load() > 0; }, 1s));
    const auto snapshot = http_exchange(port, "GET /api/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(snapshot.find("application/json") != std::string::npos);
    CHECK(snapshot.find("{\"n\":") != std::string::npos);

    const auto count_events = [](const std::string& text) {
        std::size_t count = 0;
        for (auto position = text.find("data: "); position != std::string::npos; position = text.find("data: ", position + 1)) {
            ++count;
        }
        return count;
    };
    const auto stream = http_exchange(port, "GET /events HTTP/1.1\r\n" + host + "\r\n",
                                      [&](const std::string& text) { return count_events(text) >= 3U; });
    CHECK(stream.find("text/event-stream") != std::string::npos);
    CHECK(count_events(stream) >= 3U);

    const auto control = http_exchange(port,
                                       "POST /api/control?name=feed.paused%3Afx&value=1 HTTP/1.1\r\n" + host +
                                           "Origin: http://127.0.0.1:" + std::to_string(port) +
                                           "\r\nContent-Length: 0\r\n\r\n");
    CHECK(control.starts_with("HTTP/1.1 200"));
    {
        std::lock_guard lock(controls_mutex);
        CHECK(controls == (std::vector<std::pair<std::string, std::string>>{{"feed.paused:fx", "1"}}));
    }

    const auto cross_site = http_exchange(port, "POST /api/control?name=x&value=1 HTTP/1.1\r\n" + host +
                                                    "Origin: http://evil.example\r\n\r\n");
    CHECK(cross_site.starts_with("HTTP/1.1 403"));
    const auto rebinding = http_exchange(port, "GET / HTTP/1.1\r\nHost: evil.example\r\n\r\n");
    CHECK(rebinding.starts_with("HTTP/1.1 403"));
    CHECK(http_exchange(port, "GET /missing HTTP/1.1\r\n" + host + "\r\n").starts_with("HTTP/1.1 404"));
    CHECK(http_exchange(port, "GET /api/control HTTP/1.1\r\n" + host + "\r\n").starts_with("HTTP/1.1 405"));

    const auto stop_started = trading::MonotonicClock::now();
    server.stop();
    CHECK(trading::MonotonicClock::now() - stop_started < 500ms);
}
#endif

}  // namespace

int main() {
    const std::vector<std::pair<std::string, std::function<void()>>> tests{
        {"messaging fan-out and type filtering", messaging_delivers_to_every_matching_subscriber},
        {"topic routing per strategy", topics_partition_market_data_between_strategies},
        {"unsubscribe stops delivery", unsubscribing_stops_delivery},
        {"concurrent publication", messaging_is_safe_with_concurrent_publishers},
        {"mailbox overflow policies", full_mailboxes_follow_their_overflow_policy},
        {"mailbox batching and close", mailbox_batches_preserve_order_and_drain_after_close},
        {"latency histogram precision", histogram_percentiles_are_within_bucket_precision},
        {"threshold strategy hysteresis", threshold_strategy_signals_on_band_breaks_only},
        {"EMA crossover strategy", ema_cross_strategy_trades_crossovers_after_warm_up},
        {"slow strategy isolation", slow_strategy_does_not_delay_other_strategies},
        {"order ids unique across strategies", order_ids_are_unique_across_strategies},
        {"thread-safe position state", position_book_is_thread_safe},
        {"shared rate limiting", shared_rate_limit_applies_across_submitters},
        {"asynchronous API isolation", slow_api_does_not_block_unrelated_event_processing},
        {"idempotency-gated retries", retry_policy_requires_idempotency},
        {"Retry-After and timeout handling", retry_after_and_timeouts_are_honoured},
        {"bounded cancellation and shutdown", cancellation_and_shutdown_are_bounded},
        {"drain timeout cancels stragglers", drain_timeout_cancels_requests_that_cannot_finish},
        {"concurrent shutdown", concurrent_shutdown_calls_are_safe},
        {"bounded mock idempotency window", mock_api_deduplicates_within_a_bounded_window},
        {"producer topics and sequencing", producer_publishes_each_instrument_on_its_own_topic},
        {"producer pause and resume", producer_pauses_and_resumes_without_polling},
        {"JSON writer", json_writer_escapes_and_nests},
        {"end-to-end trading system", trading_system_runs_end_to_end},
        {"system configuration validation", system_configuration_is_validated},
#if TRADING_HAS_DASHBOARD
        {"dashboard server", dashboard_server_serves_page_stream_and_controls},
#endif
    };

    std::size_t failures = 0;
    for (const auto& [name, test] : tests) {
        const auto started = trading::MonotonicClock::now();
        try {
            test();
            std::cout << "[PASS] " << name << " ("
                      << std::chrono::duration_cast<std::chrono::milliseconds>(trading::MonotonicClock::now() - started).count()
                      << " ms)\n";
        } catch (const std::exception& exception) {
            ++failures;
            std::cerr << "[FAIL] " << name << ": " << exception.what() << '\n';
        } catch (...) {
            ++failures;
            std::cerr << "[FAIL] " << name << ": unknown exception\n";
        }
    }
    std::cout << tests.size() - failures << '/' << tests.size() << " tests passed\n";
    return failures == 0U ? 0 : 1;
}
