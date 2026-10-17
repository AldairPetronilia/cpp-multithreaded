// Tests for the journal, replay, supervision and gateway failover. Several run a whole
// TradingSystem against the clock, so they wait with generous timeouts and assert on
// invariants (nothing lost, nothing sent twice) rather than on exact timings.

#include "check.hpp"
#include "trading/api.hpp"
#include "trading/components.hpp"
#include "trading/config.hpp"
#include "trading/fencing.hpp"
#include "trading/gateway_pool.hpp"
#include "trading/journal.hpp"
#include "trading/messaging.hpp"
#include "trading/replay.hpp"
#include "trading/resilience.hpp"
#include "trading/session.hpp"
#include "trading/system.hpp"
#include "trading/telemetry.hpp"
#include "trading/workers.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;
using testing_support::eventually;
namespace fs = std::filesystem;

// --- Helpers -------------------------------------------------------------------------------

class TempDir {
public:
    TempDir() {
        std::random_device entropy;
        path_ = fs::temp_directory_path() / ("trading-test-" + std::to_string(entropy()) + std::to_string(entropy()));
        fs::create_directories(path_);
    }
    ~TempDir() {
        std::error_code error;
        fs::remove_all(path_, error);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    [[nodiscard]] const fs::path& path() const { return path_; }

private:
    fs::path path_;
};

trading::MarketDataEvent make_quote(const std::string& symbol, double price, std::uint64_t sequence) {
    return trading::MarketDataEvent{symbol, price, sequence, trading::MonotonicClock::now()};
}

trading::TradingSignalEvent make_signal(const std::string& strategy, const std::string& symbol, std::uint64_t id,
                                        trading::Side side = trading::Side::buy) {
    const auto now = trading::MonotonicClock::now();
    return trading::TradingSignalEvent{.strategy = strategy,
                                       .signal_id = id,
                                       .symbol = symbol,
                                       .side = side,
                                       .quantity = 2.0,
                                       .reference_price = 100.0,
                                       .market_data_time = now - 40us,
                                       .signal_time = now};
}

std::vector<trading::JournalRecord> read_all(const fs::path& directory) {
    trading::JournalReader reader(directory);
    std::vector<trading::JournalRecord> records;
    while (auto record = reader.next()) {
        records.push_back(std::move(*record));
    }
    CHECK(!reader.corrupt());
    return records;
}

// Returns the API key of every request it receives; always succeeds.
class KeyRecorder final : public trading::IApiClient {
public:
    trading::ApiResponse perform(const trading::ApiRequest& request, std::stop_token) override {
        {
            std::lock_guard lock(mutex_);
            keys_.push_back(request.idempotency_key);
        }
        trading::ApiResponse response;
        response.status_code = 200;
        response.body = "filled";
        return response;
    }
    [[nodiscard]] std::vector<std::string> keys() const {
        std::lock_guard lock(mutex_);
        return keys_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> keys_;
};

class Completions {
public:
    trading::ApiCompletion callback() {
        return [this](trading::ApiResponse response) {
            {
                std::lock_guard lock(mutex_);
                responses_.push_back(std::move(response));
            }
            wakeup_.notify_all();
        };
    }
    bool wait_for(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return wakeup_.wait_for(lock, timeout, [&] { return responses_.size() >= count; });
    }
    [[nodiscard]] std::vector<trading::ApiResponse> responses() const {
        std::lock_guard lock(mutex_);
        return responses_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable wakeup_;
    std::vector<trading::ApiResponse> responses_;
};

trading::ApiManagerConfig quick_gateway() {
    trading::ApiManagerConfig config;
    config.rate_limit = {100'000, 1000ms};
    config.io_threads = 2;
    config.max_retries = 0;
    config.initial_retry_backoff = 1ms;
    config.max_retry_backoff = 2ms;
    return config;
}

trading::ApiRequest order(const std::string& key) {
    trading::ApiRequest request;
    request.endpoint = "/v1/orders";
    request.idempotency_key = key;
    request.idempotent = true;
    request.timeout = 500ms;
    return request;
}

std::string render_json(const trading::SystemSnapshot& snapshot) {
    trading::TelemetryRenderer renderer;
    return renderer.render(snapshot);
}

// Structural check only: strings are skipped, brackets must nest and no comma may be left dangling.
bool looks_like_json(const std::string& text) {
    std::vector<char> stack;
    bool in_string = false;
    char previous = ' ';
    for (std::size_t index = 0; index < text.size(); ++index) {
        const char c = text[index];
        if (in_string) {
            if (c == '\\') {
                ++index;
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }
        if (c == '"') {
            in_string = true;
        } else if (c == '{' || c == '[') {
            stack.push_back(c);
        } else if (c == '}' || c == ']') {
            if (stack.empty() || (c == '}') != (stack.back() == '{') || previous == ',') {
                return false;
            }
            stack.pop_back();
        }
        previous = c;
    }
    return stack.empty() && !in_string;
}

// A whole system that runs until stopped, journals, and supervises quickly.
trading::SystemConfig resilient_config(const fs::path& journal_root) {
    auto config = trading::SystemConfig::demo();
    for (auto& feed : config.feeds) {
        feed.events_per_instrument = 0;
        feed.interval = 500us;
    }
    config.journal.enabled = true;
    config.journal.root = journal_root;
    config.journal.fsync_interval = 0ms;
    config.supervision.enabled = true;
    auto& supervision = config.supervision.config;
    supervision.probe_interval = 10ms;
    supervision.stall_timeout = 120ms;
    supervision.handler_deadline = 500ms;
    supervision.poison_threshold = 5;
    config.gateways.count = 2;
    config.api_manager.rate_limit = {10'000, 1000ms};
    config.mock_api.latency = 1ms;
    return config;
}

const trading::ComponentStatus* find_component(const trading::SystemSnapshot& snapshot, const std::string& name) {
    for (const auto& component : snapshot.components) {
        if (component.name == name) {
            return &component;
        }
    }
    return nullptr;
}

bool restarted(trading::TradingSystem& system, const std::string& name, std::uint64_t times) {
    const auto snapshot = system.snapshot();
    const auto* component = find_component(snapshot, name);
    return component != nullptr && component->restarts >= times && component->state == trading::ComponentState::healthy;
}

std::uint64_t total_signals(const trading::SystemSnapshot& snapshot) {
    std::uint64_t total = 0;
    for (const auto& strategy : snapshot.strategies) {
        total += strategy.signals_published;
    }
    return total;
}

// --- Journal -------------------------------------------------------------------------------

void journal_round_trips_every_event_type_and_resumes_its_sequence() {
    TempDir dir;
    const auto session = dir.path() / "s1";
    trading::JournalConfig config;
    config.directory = session;
    config.session = "s1";
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        const auto topic = bus.topic("AAPL");
        bus.publish(make_quote("AAPL", 101.5, 7), topic);
        bus.publish(make_signal("alpha", "AAPL", 7, trading::Side::sell));
        const auto now = trading::MonotonicClock::now();
        bus.publish(trading::ExecutionReportEvent{.strategy = "alpha",
                                                  .signal_id = 7,
                                                  .client_order_id = "s1:alpha:AAPL:7",
                                                  .symbol = "AAPL",
                                                  .side = trading::Side::sell,
                                                  .quantity = 2.0,
                                                  .fill_price = 101.5,
                                                  .filled = true,
                                                  .message = "simulated order filled",
                                                  .market_data_time = now - 5ms,
                                                  .execution_time = now,
                                                  .api_service_time = 4ms});
        bus.publish(trading::SupervisionEvent{"alpha", "restarted", "caught up", 2, 3.5, 12.5, now});
        // Readable while the writer is still open.
        CHECK(journal.sync());
        CHECK(read_all(session).size() == 4U);
        journal.stop();
    }

    const auto records = read_all(session);
    CHECK(records.size() == 4U);
    for (std::size_t index = 0; index < records.size(); ++index) {
        CHECK(records[index].sequence == index + 1U);
    }
    CHECK(records[0].kind == trading::JournalKind::market_data);
    CHECK(records[0].topic == "AAPL");
    const auto stamp = trading::MonotonicClock::now();
    const auto quote = trading::decode_market_data(records[0], stamp);
    CHECK(quote && quote->symbol == "AAPL" && quote->price == 101.5 && quote->sequence == 7U);
    const auto signal = trading::decode_signal(records[1], stamp);
    CHECK(signal && signal->strategy == "alpha" && signal->signal_id == 7U && signal->side == trading::Side::sell);
    // Latencies are stored as durations and re-based on whatever stamp the reader gives.
    CHECK(signal->signal_time - signal->market_data_time == 40us);
    const auto report = trading::decode_execution_report(records[2], stamp);
    CHECK(report && report->filled && report->client_order_id == "s1:alpha:AAPL:7" && report->fill_price == 101.5);
    CHECK(report->message == "simulated order filled" && report->api_service_time == 4ms);
    const auto event = trading::decode_supervision(records[3], stamp);
    CHECK(event && event->component == "alpha" && event->action == "restarted" && event->generation == 2U);
    CHECK(event->detect_ms == 3.5 && event->recovery_ms == 12.5);

    // Reopening the session continues its sequence instead of starting over.
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        bus.publish(make_quote("MSFT", 300.0, 1), bus.topic("MSFT"));
        CHECK(journal.sync());
        CHECK(journal.stats().last_sequence == 5U);
        journal.stop();
    }
    const auto bounds = trading::JournalReader::bounds(session);
    CHECK(bounds.first == 1U && bounds.last == 5U && bounds.count() == 5U);
}

void journal_repairs_a_torn_tail_and_sets_aside_a_damaged_segment() {
    TempDir dir;
    trading::JournalConfig config;
    config.session = "torn";
    const auto write_quotes = [&](const fs::path& directory, std::uint64_t count) {
        config.directory = directory;
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        const auto topic = bus.topic("AAPL");
        for (std::uint64_t index = 1; index <= count; ++index) {
            bus.publish(make_quote("AAPL", 100.0 + static_cast<double>(index), index), topic);
        }
        CHECK(journal.sync());
        journal.stop();
    };
    const auto segment = [](const fs::path& directory) {
        for (const auto& entry : fs::directory_iterator(directory)) {
            if (entry.path().extension() == ".jnl") {
                return entry.path();
            }
        }
        return fs::path{};
    };

    // A crash in the middle of an append leaves part of the last record.
    const auto torn = dir.path() / "torn";
    write_quotes(torn, 10);
    const auto file = segment(torn);
    fs::resize_file(file, fs::file_size(file) - 7);
    {
        trading::EventBus bus;
        config.directory = torn;
        trading::JournalWriter journal(bus, config);
        const auto stats = journal.stats();
        CHECK(stats.repaired_bytes > 0U);
        CHECK(stats.records == 9U);
        CHECK(stats.last_sequence == 9U);
        CHECK(stats.quarantined_segments == 0U);
        journal.start();
        bus.publish(make_quote("AAPL", 1.0, 10), bus.topic("AAPL"));
        CHECK(journal.sync());
        journal.stop();
    }
    auto records = read_all(torn);
    CHECK(records.size() == 10U);
    CHECK(records.back().sequence == 10U);

    // Damage in the middle is different: records after it cannot be trusted, and silently
    // discarding them would destroy good data, so the segment is set aside whole.
    const auto damaged = dir.path() / "damaged";
    write_quotes(damaged, 30);
    {
        const auto path = segment(damaged);
        std::fstream stream(path, std::ios::in | std::ios::out | std::ios::binary);
        stream.seekp(static_cast<std::streamoff>(fs::file_size(path) / 2));
        const char byte = static_cast<char>(0xA5);
        stream.write(&byte, 1);
    }
    {
        trading::EventBus bus;
        config.directory = damaged;
        trading::JournalWriter journal(bus, config);
        CHECK(journal.stats().quarantined_segments == 1U);
        journal.start();
        bus.publish(make_quote("AAPL", 1.0, 31), bus.topic("AAPL"));
        CHECK(journal.sync());
        journal.stop();
    }
    std::size_t set_aside = 0;
    for (const auto& entry : fs::directory_iterator(damaged)) {
        set_aside += entry.path().extension() == ".corrupt" ? 1U : 0U;
    }
    CHECK(set_aside == 1U);
    records = read_all(damaged);  // reads cleanly: the damaged file is no longer part of the session
    CHECK(!records.empty() && records.back().sequence > 1U);
}

void journal_rolls_segments_and_seeks_by_sequence() {
    TempDir dir;
    trading::JournalConfig config;
    config.directory = dir.path() / "rolling";
    config.session = "rolling";
    config.segment_bytes = 4096;
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        const auto topic = bus.topic("AAPL");
        for (std::uint64_t index = 1; index <= 400; ++index) {
            bus.publish(make_quote("AAPL", 100.0, index), topic);
        }
        CHECK(journal.sync());
        CHECK(journal.stats().segments > 3U);
        journal.stop();
    }
    const auto records = read_all(config.directory);
    CHECK(records.size() == 400U);
    for (std::size_t index = 0; index < records.size(); ++index) {
        CHECK(records[index].sequence == index + 1U);
    }
    trading::JournalReader reader(config.directory);
    reader.seek(250);
    const auto found = reader.next();
    CHECK(found && found->sequence == 250U);
    std::uint64_t remaining = 1;
    while (reader.next()) {
        ++remaining;
    }
    CHECK(remaining == 151U);
}

void journal_marks_a_gap_when_its_tap_overflows() {
    TempDir dir;
    trading::JournalConfig config;
    config.directory = dir.path() / "gap";
    config.session = "gap";
    config.mailbox_capacity = 4;
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        // Not started yet, so nothing drains the tap: only four of the twenty fit.
        const auto topic = bus.topic("AAPL");
        for (std::uint64_t index = 1; index <= 20; ++index) {
            bus.publish(make_quote("AAPL", 100.0, index), topic);
        }
        journal.start();
        journal.stop();
    }
    std::uint64_t quotes = 0;
    std::uint64_t lost = 0;
    for (const auto& record : read_all(config.directory)) {
        if (record.kind == trading::JournalKind::market_data) {
            ++quotes;
        } else if (record.kind == trading::JournalKind::gap) {
            lost += trading::decode_gap(record);
        }
    }
    // The journal says exactly how much it is missing, so a replay is visibly incomplete.
    CHECK(quotes == 4U);
    CHECK(lost == 16U);
}

// --- Fencing and deduplication -------------------------------------------------------------

void generation_gate_rejects_writes_from_stale_generations() {
    trading::GenerationGate gate;
    int written = 0;
    CHECK(gate.run_if_current(1, [&] { ++written; }));
    const auto next = gate.advance();
    CHECK(next == 2U);
    CHECK(!gate.run_if_current(1, [&] { ++written; }));
    CHECK(gate.run_if_current(2, [&] { ++written; }));
    CHECK(written == 2);
    CHECK(gate.fenced_writes() == 1U);

    // advance() is a barrier: it waits for a write already inside the gate.
    std::atomic<bool> inside{false};
    std::atomic<bool> release{false};
    std::thread writer([&] {
        gate.run_if_current(2, [&] {
            inside = true;
            while (!release) {
                std::this_thread::sleep_for(1ms);
            }
        });
    });
    CHECK(eventually([&] { return inside.load(); }, 1s));
    std::atomic<bool> advanced{false};
    std::thread fencer([&] {
        gate.advance();
        advanced = true;
    });
    std::this_thread::sleep_for(30ms);
    CHECK(!advanced);
    release = true;
    writer.join();
    fencer.join();
    CHECK(advanced);
}

class BlockingBuyer final : public trading::ISignalStrategy {
public:
    std::optional<trading::Side> on_market_data(const trading::MarketDataEvent&) override {
        std::unique_lock lock(mutex_);
        entered_ = true;
        wakeup_.notify_all();
        wakeup_.wait(lock, [this] { return released_; });
        return trading::Side::buy;
    }
    std::string_view kind() const noexcept override { return "blocking"; }
    bool wait_entered() {
        std::unique_lock lock(mutex_);
        return wakeup_.wait_for(lock, 2s, [this] { return entered_; });
    }
    void release() {
        {
            std::lock_guard lock(mutex_);
            released_ = true;
        }
        wakeup_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable wakeup_;
    bool entered_{false};
    bool released_{false};
};

// The shared pointer lets the test keep hold of the strategy that the processor owns.
class SharedStrategy final : public trading::ISignalStrategy {
public:
    explicit SharedStrategy(std::shared_ptr<BlockingBuyer> inner) : inner_(std::move(inner)) {}
    std::optional<trading::Side> on_market_data(const trading::MarketDataEvent& quote) override {
        return inner_->on_market_data(quote);
    }
    std::string_view kind() const noexcept override { return inner_->kind(); }

private:
    std::shared_ptr<BlockingBuyer> inner_;
};

void a_replaced_processor_cannot_publish_when_it_wakes_up() {
    trading::EventBus bus;
    auto signals = std::make_shared<trading::BoundedMailbox>(16);
    const auto subscription = bus.subscribe<trading::TradingSignalEvent>(signals);
    auto gate = std::make_shared<trading::GenerationGate>();
    auto strategy = std::make_shared<BlockingBuyer>();

    auto zombie = std::make_shared<trading::SignalProcessor>(
        bus, trading::SignalProcessorConfig{"stuck", {}, 1.0, {64}}, std::make_unique<SharedStrategy>(strategy));
    zombie->set_fence(gate, 1);
    zombie->start();
    bus.publish(make_quote("AAPL", 100.0, 1));
    CHECK(strategy->wait_entered());

    // The supervisor's view: the thread is stuck, so it is abandoned and fenced, not joined.
    zombie->abandon();
    gate->advance();
    CHECK(!zombie->finished());

    // It wakes up later, finishes the event it was holding and tries to publish.
    strategy->release();
    CHECK(eventually([&] { return zombie->finished(); }, 2s));
    CHECK(signals->size() == 0U);
    CHECK(zombie->stats().fenced_writes == 1U);
    CHECK(zombie->stats().signals_published == 0U);
}

void position_book_applies_each_order_once() {
    trading::PositionBook book;
    trading::ExecutionReportEvent fill{.strategy = "alpha",
                                       .symbol = "AAPL",
                                       .side = trading::Side::buy,
                                       .quantity = 3.0,
                                       .fill_price = 10.0,
                                       .filled = true};
    fill.client_order_id = "s:alpha:AAPL:1";
    CHECK(book.apply(fill) == trading::ApplyResult::applied);
    // The same order reported again, as redelivery after a restart does.
    CHECK(book.apply(fill) == trading::ApplyResult::duplicate);
    CHECK(book.apply(fill) == trading::ApplyResult::duplicate);
    CHECK(book.position("alpha", "AAPL") == 3.0);
    CHECK(book.fills_applied() == 1U && book.duplicates_ignored() == 2U);

    fill.client_order_id = "s:alpha:AAPL:2";
    CHECK(book.apply(fill) == trading::ApplyResult::applied);
    CHECK(book.position("alpha", "AAPL") == 6.0);

    auto rejection = fill;
    rejection.filled = false;
    rejection.client_order_id = "s:alpha:AAPL:3";
    CHECK(book.apply(rejection) == trading::ApplyResult::not_filled);
    // The same rejection reported again is one outcome, not two.
    CHECK(book.apply(rejection) == trading::ApplyResult::duplicate);
    // A rejection does not use up the id, so the order can still fill after a resubmission.
    rejection.filled = true;
    CHECK(book.apply(rejection) == trading::ApplyResult::applied);
    // And an order that filled cannot then be rejected as well.
    rejection.filled = false;
    CHECK(book.apply(rejection) == trading::ApplyResult::duplicate);
    CHECK(book.fills_applied() == 3U && book.duplicates_ignored() == 4U);
}

void stall_detector_tells_idle_from_stuck() {
    trading::StallDetector detector;
    trading::WorkerHealth health;
    health.ready = true;
    health.processed = 5;

    // Idle: nothing waiting, no handler running, nothing completing, forever. Not a fault.
    health.backlog = 0;
    CHECK(detector.observe(health) == 0ms);
    std::this_thread::sleep_for(40ms);
    CHECK(detector.observe(health) == 0ms);

    // Stuck: work is waiting and nothing completes.
    health.backlog = 3;
    CHECK(detector.observe(health) == 0ms);
    std::this_thread::sleep_for(40ms);
    CHECK(detector.observe(health) >= 40ms);

    // Slow but alive: work is waiting and the count keeps moving.
    health.processed = 6;
    CHECK(detector.observe(health) == 0ms);
    std::this_thread::sleep_for(40ms);
    health.processed = 7;
    CHECK(detector.observe(health) == 0ms);

    // Hung on its last event: nothing queued behind it, but a handler is running and
    // nothing completes. An empty mailbox must not hide that.
    health.backlog = 0;
    health.handler_running_for = 1ms;
    CHECK(detector.observe(health) == 0ms);
    std::this_thread::sleep_for(40ms);
    health.handler_running_for = 41ms;
    CHECK(detector.observe(health) >= 40ms);

    // Still rebuilding its state: working, never stalled.
    health.ready = false;
    CHECK(detector.observe(health) == 0ms);
}

void a_worker_hung_on_its_last_event_is_stalled_not_ignored() {
    trading::SupervisorConfig config;
    config.stall_timeout = 40ms;
    config.handler_deadline = 80ms;
    trading::StallDetector detector;
    trading::WorkerHealth health;
    health.ready = true;
    health.handler_running_for = 1ms;
    CHECK(trading::evaluate_worker(health, detector, config).verdict == trading::ProbeResult::Verdict::ok);
    std::this_thread::sleep_for(50ms);
    // Past half the handler deadline: a warning first.
    auto probe = trading::evaluate_worker(health, detector, config);
    CHECK(probe.verdict == trading::ProbeResult::Verdict::degraded);
    CHECK(probe.state == trading::ComponentState::degraded);
    std::this_thread::sleep_for(40ms);
    probe = trading::evaluate_worker(health, detector, config);
    CHECK(probe.verdict == trading::ProbeResult::Verdict::fault);
    CHECK(probe.state == trading::ComponentState::stalled);
    CHECK(probe.observed_ms >= 80.0);

    // A replacement that is still rebuilding its state shows as restarting, not as stuck.
    trading::StallDetector fresh;
    trading::WorkerHealth rebuilding;
    rebuilding.backlog = 500;
    CHECK(trading::evaluate_worker(rebuilding, fresh, config).state == trading::ComponentState::restarting);
}

// --- Order recovery ------------------------------------------------------------------------

void gateway_lookup_reports_in_flight_and_completed_orders() {
    trading::MockApiConfig mock_config;
    mock_config.latency = 120ms;
    auto broker = std::make_shared<trading::MockTradingApi>(mock_config);
    trading::ApiRequestManager manager(broker, quick_gateway());
    Completions done;

    CHECK(manager.lookup("k1").state == trading::OrderLookup::unknown);
    CHECK(manager.submit(order("k1"), done.callback()).accepted());
    CHECK(manager.lookup("k1").state == trading::OrderLookup::in_flight);
    CHECK(done.wait_for(1, 2s));
    const auto completed = manager.lookup("k1");
    CHECK(completed.state == trading::OrderLookup::completed);
    CHECK(completed.response.ok());
    CHECK(manager.lookup("never-sent").state == trading::OrderLookup::unknown);
}

void a_restarted_execution_worker_sends_every_missed_order_exactly_once() {
    TempDir dir;
    trading::EventBus bus;
    trading::JournalConfig journal_config;
    journal_config.directory = dir.path() / "orders";
    journal_config.session = "orders";
    trading::JournalWriter journal(bus, journal_config);
    journal.start();

    auto broker = std::make_shared<KeyRecorder>();
    trading::ApiRequestManager manager(broker, quick_gateway());
    trading::Graveyard graveyard;
    const trading::ComponentContext context{bus, graveyard, trading::SupervisorConfig{}, &journal};
    trading::OrderExecutionConfig execution_config;
    execution_config.session = "t";
    trading::ExecutionComponent execution(context, manager, execution_config);
    auto reports = std::make_shared<trading::BoundedMailbox>(64);
    const auto subscription = bus.subscribe<trading::ExecutionReportEvent>(reports);
    execution.start();

    for (std::uint64_t id = 1; id <= 3; ++id) {
        bus.publish(make_signal("alpha", "AAPL", id));
    }
    CHECK(eventually([&] { return reports->size() == 3U; }, 2s));

    // The next signal kills the worker as it starts handling it; two more arrive while it is dead.
    execution.fault().arm_crash();
    bus.publish(make_signal("alpha", "AAPL", 4));
    CHECK(eventually([&] { return execution.probe().verdict == trading::ProbeResult::Verdict::fault; }, 2s));
    bus.publish(make_signal("alpha", "AAPL", 5));
    bus.publish(make_signal("alpha", "AAPL", 6));
    CHECK(broker->keys().size() == 3U);

    const auto result = execution.recover("test crash");
    CHECK(result.ok);
    CHECK(eventually([&] { return reports->size() == 6U; }, 2s));

    // Every order reached the broker, and none reached it twice.
    auto keys = broker->keys();
    std::sort(keys.begin(), keys.end());
    CHECK(keys == (std::vector<std::string>{"t:alpha:AAPL:1", "t:alpha:AAPL:2", "t:alpha:AAPL:3", "t:alpha:AAPL:4",
                                            "t:alpha:AAPL:5", "t:alpha:AAPL:6"}));
    const auto stats = execution.stats();
    CHECK(stats.recovery.examined == 3U);
    CHECK(stats.recovery.resubmitted == 3U);
    execution.stop();
    manager.shutdown();
    journal.stop();
}

void recovery_asks_the_broker_before_it_resubmits() {
    TempDir dir;
    trading::EventBus bus;
    trading::JournalConfig journal_config;
    journal_config.directory = dir.path() / "reconcile";
    journal_config.session = "reconcile";
    trading::JournalWriter journal(bus, journal_config);
    journal.start();

    // The broker already holds two orders the execution worker will never have reported:
    // number 1 is filled, number 2 is still being worked on.
    trading::MockApiConfig mock_config;
    mock_config.latency = 400ms;
    auto broker = std::make_shared<trading::MockTradingApi>(mock_config);
    trading::ApiRequestManager manager(broker, quick_gateway());
    Completions direct;
    CHECK(manager.submit(order("t:alpha:AAPL:1"), direct.callback()).accepted());
    CHECK(direct.wait_for(1, 3s));
    CHECK(manager.submit(order("t:alpha:AAPL:2"), direct.callback()).accepted());

    trading::Graveyard graveyard;
    const trading::ComponentContext context{bus, graveyard, trading::SupervisorConfig{}, &journal};
    trading::OrderExecutionConfig execution_config;
    execution_config.session = "t";
    trading::ExecutionComponent execution(context, manager, execution_config);
    auto reports = std::make_shared<trading::BoundedMailbox>(64);
    const auto subscription = bus.subscribe<trading::ExecutionReportEvent>(reports);
    execution.start();

    // The worker dies on signal 10, so it never sends it; signals 1 and 2 arrive while it is dead.
    execution.fault().arm_crash();
    bus.publish(make_signal("alpha", "AAPL", 10));
    CHECK(eventually([&] { return execution.probe().verdict == trading::ProbeResult::Verdict::fault; }, 2s));
    bus.publish(make_signal("alpha", "AAPL", 1));
    bus.publish(make_signal("alpha", "AAPL", 2));

    CHECK(execution.recover("test").ok);
    const auto stats = execution.stats();
    CHECK(stats.recovery.examined == 3U);
    // 1 is already filled at the broker and 2 is in flight: neither may be sent again.
    CHECK(stats.recovery.already_completed == 1U);
    CHECK(stats.recovery.already_in_flight == 1U);
    CHECK(stats.recovery.resubmitted == 1U);
    CHECK(direct.wait_for(2, 3s));
    CHECK(eventually([&] { return reports->size() >= 2U; }, 3s));  // 10 filled, 1 restored from the broker

    execution.stop();
    manager.shutdown();
    // Two direct submissions and the one order that really had been missed: three calls, not five.
    CHECK(broker->invocation_count() == 3U);
    journal.stop();
}

void stale_signals_are_expired_not_traded_after_recovery() {
    TempDir dir;
    trading::EventBus bus;
    trading::JournalConfig journal_config;
    journal_config.directory = dir.path() / "stale";
    journal_config.session = "stale";
    trading::JournalWriter journal(bus, journal_config);
    journal.start();
    auto broker = std::make_shared<KeyRecorder>();
    trading::ApiRequestManager manager(broker, quick_gateway());
    trading::Graveyard graveyard;
    const trading::ComponentContext context{bus, graveyard, trading::SupervisorConfig{}, &journal};
    trading::OrderExecutionConfig execution_config;
    execution_config.session = "t";
    execution_config.stale_signal_after = 50ms;
    trading::ExecutionComponent execution(context, manager, execution_config);
    auto reports = std::make_shared<trading::BoundedMailbox>(16);
    const auto subscription = bus.subscribe<trading::ExecutionReportEvent>(reports);
    execution.start();
    execution.fault().arm_crash();
    bus.publish(make_signal("alpha", "AAPL", 1));
    CHECK(eventually([&] { return execution.probe().verdict == trading::ProbeResult::Verdict::fault; }, 2s));
    std::this_thread::sleep_for(120ms);  // the outage outlasts the signal's usefulness

    CHECK(execution.recover("test").ok);
    CHECK(eventually([&] { return reports->size() == 1U; }, 2s));
    trading::EventEnvelope envelope;
    CHECK(reports->try_pop(envelope));
    const auto* report = envelope.as<trading::ExecutionReportEvent>();
    CHECK(report != nullptr && !report->filled);
    CHECK(report->message.find("expired") != std::string::npos);
    CHECK(broker->keys().empty());
    CHECK(execution.stats().recovery.expired == 1U);
    execution.stop();
    manager.shutdown();
    journal.stop();
}

// --- Gateway pool --------------------------------------------------------------------------

struct PoolFixture {
    std::shared_ptr<trading::MockTradingApi> broker;
    std::unique_ptr<trading::GatewayPool> pool;
};

PoolFixture make_pool(bool spread = false, std::size_t mock_429_every = 0) {
    trading::MockApiConfig mock_config;
    mock_config.latency = 1ms;
    mock_config.respond_429_every = mock_429_every;
    mock_config.retry_after = 1ms;
    PoolFixture fixture;
    fixture.broker = std::make_shared<trading::MockTradingApi>(mock_config);
    std::vector<trading::GatewayPool::Member> members;
    for (int index = 1; index <= 2; ++index) {
        auto fault = std::make_shared<trading::FaultableApiClient>(fixture.broker);
        members.push_back({"gateway-" + std::to_string(index),
                           std::make_shared<trading::ApiRequestManager>(fault, quick_gateway()), fault});
    }
    trading::GatewayPoolConfig config;
    config.spread = spread;
    config.failure_threshold = 2;
    config.base_ejection = 60ms;
    config.max_ejection = 200ms;
    config.ejection_jitter = 0.0;
    config.probe_interval = 10ms;
    config.probe_successes = 2;
    config.retry_budget_cap = 1'000;
    config.retry_budget_ratio = 1.0;
    fixture.pool = std::make_unique<trading::GatewayPool>(std::move(members), config);
    return fixture;
}

void the_pool_fails_over_ejects_the_bad_gateway_and_fails_back() {
    // Declared first, so it outlives the pool, whose destructor still completes requests.
    Completions done;
    auto fixture = make_pool();
    auto& pool = *fixture.pool;

    pool.set_down(0, true);
    for (int index = 0; index < 12; ++index) {
        CHECK(pool.submit(order("down-" + std::to_string(index)), done.callback()).accepted());
    }
    CHECK(done.wait_for(12, 5s));
    // Every order succeeded even though the preferred gateway was unreachable.
    for (const auto& response : done.responses()) {
        CHECK(response.ok());
    }
    auto gateways = pool.gateways();
    CHECK(gateways[0].state != trading::GatewayState::closed);
    CHECK(gateways[0].ejections >= 1U);
    CHECK(gateways[1].succeeded >= 12U);
    CHECK(pool.pool_stats().reroutes >= 1U);
    // The orders that moved were counted once, not twice, and none are reported as failed.
    CHECK(pool.stats().failed == 0U);

    // The gateway comes back: probes find it healthy and it takes traffic again.
    pool.set_down(0, false);
    int sent = 0;
    const bool closed = eventually(
        [&] {
            pool.submit(order("back-" + std::to_string(sent++)), done.callback());
            std::this_thread::sleep_for(5ms);
            return pool.gateways()[0].state == trading::GatewayState::closed;
        },
        5s);
    CHECK(closed);
    const auto before = pool.gateways()[0].submitted;
    CHECK(pool.submit(order("after-failback"), done.callback()).accepted());
    CHECK(eventually([&] { return pool.gateways()[0].submitted > before; }, 1s));
}

void the_pool_never_ejects_its_last_healthy_gateway() {
    Completions done;
    auto fixture = make_pool();
    auto& pool = *fixture.pool;
    pool.set_down(0, true);
    pool.set_down(1, true);
    for (int index = 0; index < 12; ++index) {
        pool.submit(order("all-down-" + std::to_string(index)), done.callback());
    }
    CHECK(done.wait_for(12, 5s));
    for (const auto& response : done.responses()) {
        CHECK(!response.ok());
    }
    const auto gateways = pool.gateways();
    const auto open = std::count_if(gateways.begin(), gateways.end(),
                                    [](const auto& gateway) { return gateway.state == trading::GatewayState::open; });
    CHECK(open <= 1);
    CHECK(pool.pool_stats().healthy >= 1U);
    // As soon as a path works again the pool serves from it, with no manual reset.
    pool.set_down(1, false);
    CHECK(eventually(
        [&] {
            Completions probe;
            pool.submit(order("recovery-probe-" + std::to_string(done.responses().size())), probe.callback());
            return probe.wait_for(1, 1s) && probe.responses().front().ok();
        },
        5s));
}

void rate_limiting_is_not_a_reason_to_fail_over() {
    Completions done;
    auto fixture = make_pool(false, 1);  // the broker answers every call with HTTP 429
    auto& pool = *fixture.pool;
    for (int index = 0; index < 6; ++index) {
        pool.submit(order("limited-" + std::to_string(index)), done.callback());
    }
    CHECK(done.wait_for(6, 5s));
    const auto gateways = pool.gateways();
    CHECK(gateways[0].state == trading::GatewayState::closed);
    CHECK(gateways[0].ejections == 0U);
    CHECK(pool.pool_stats().reroutes == 0U);
    CHECK(gateways[1].submitted == 0U);
}

// --- Supervision of a whole system ---------------------------------------------------------

void a_crashed_strategy_is_replaced_and_rebuilds_its_state_from_the_journal() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 3000U; }, 5s));

    CHECK(system.inject_fault("trend-fx", trading::Fault::crash));
    CHECK(eventually([&] { return restarted(system, "trend-fx", 1); }, 5s));

    const auto after = system.snapshot();
    const auto* component = find_component(after, "trend-fx");
    CHECK(component != nullptr);
    CHECK(component->generation == 2U);
    CHECK(component->restarts == 1U);
    CHECK(component->recovered_records > 1000U);  // history replayed silently from the journal
    CHECK(component->last_recovery_ms > 0.0);
    CHECK(component->detail.find("replayed") != std::string::npos);

    // The replacement is alive: it keeps consuming, and the others never noticed.
    const auto fx_processed = [&] {
        for (const auto& strategy : system.snapshot().strategies) {
            if (strategy.worker.name == "trend-fx") {
                return strategy.worker.processed;
            }
        }
        return std::uint64_t{0};
    };
    const auto before = fx_processed();
    CHECK(eventually([&] { return fx_processed() > before + 500U; }, 3s));
    for (const auto& other : {std::string("trend-tech"), std::string("levels")}) {
        const auto current = system.snapshot();
        const auto* status = find_component(current, other);
        CHECK(status != nullptr && status->restarts == 0U);
    }
    system.stop();

    // Exactly-once: every signal became exactly one order and one fill.
    const auto final_state = system.snapshot();
    const auto signals = total_signals(final_state);
    CHECK(final_state.position_monitor.fills == signals);
    CHECK(final_state.mock_api_calls == signals);
    CHECK(final_state.api.failed == 0U);
}

void a_hung_strategy_is_abandoned_fenced_and_replaced() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 2000U; }, 5s));

    CHECK(system.inject_fault("trend-tech", trading::Fault::hang, 1'200ms));
    // The supervisor must act without waiting for the stuck thread.
    CHECK(eventually([&] { return restarted(system, "trend-tech", 1); }, 3s));
    auto snapshot = system.snapshot();
    const auto* component = find_component(snapshot, "trend-tech");
    CHECK(component != nullptr);
    CHECK(component->last_detect_ms >= 100.0);  // it had to be stuck for the stall timeout first
    CHECK(component->last_cause.find("stalled") != std::string::npos);
    CHECK(snapshot.supervisor.zombies >= 1U);  // the old thread is still stuck

    // Once the stuck thread wakes up and finishes, it is cleaned up rather than leaked.
    CHECK(eventually([&] { return system.snapshot().supervisor.zombies == 0U; }, 4s));
    system.stop();
    const auto final_state = system.snapshot();
    CHECK(final_state.position_monitor.fills == total_signals(final_state));
}

void a_poisoned_worker_is_restarted_after_consecutive_failures() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 1500U; }, 5s));
    CHECK(system.inject_fault("levels", trading::Fault::poison));
    CHECK(eventually([&] { return restarted(system, "levels", 1); }, 3s));
    const auto snapshot = system.snapshot();
    const auto* component = find_component(snapshot, "levels");
    CHECK(component != nullptr);
    CHECK(component->last_cause.find("poison") != std::string::npos);
    // And it works afterwards: the poison did not follow it into the replacement.
    CHECK(eventually([&] { return find_component(system.snapshot(), "levels")->state == trading::ComponentState::healthy; },
                     1s));
    std::this_thread::sleep_for(150ms);
    CHECK(find_component(system.snapshot(), "levels")->restarts == 1U);
    system.stop();
}

void the_execution_worker_and_position_monitor_are_supervised_too() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 2000U; }, 5s));
    CHECK(system.inject_fault("order-execution", trading::Fault::crash));
    CHECK(system.inject_fault("position-monitor", trading::Fault::crash));
    // They fire when the next signal / fill reaches them, so keep the pipeline moving.
    CHECK(eventually([&] { return restarted(system, "order-execution", 1) && restarted(system, "position-monitor", 1); },
                     20s));
    system.stop();
    // Each crash fires on the event its worker was about to handle, so the replacements had
    // something to recover: the position monitor a report it never applied, which it took
    // from the journal. Every signal still has exactly one outcome, and the counters show it.
    const auto final_state = system.snapshot();
    const auto& monitor = final_state.position_monitor;
    CHECK(total_signals(final_state) > 0U);
    CHECK(monitor.fills + monitor.rejections == total_signals(final_state));
    std::uint64_t book_fills = 0;
    for (const auto& position : final_state.positions) {
        book_fills += position.fills;
    }
    CHECK(book_fills == monitor.fills);
}

void a_crashed_feed_resumes_its_sequence() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 1500U; }, 5s));
    CHECK(system.inject_fault("equities", trading::Fault::crash));
    CHECK(eventually([&] { return restarted(system, "equities", 1); }, 5s));
    CHECK(eventually([&] { return system.snapshot().journal->records > 6000U; }, 5s));
    const auto directory = system.journal_directory();
    system.stop();

    // Per instrument the sequence numbers run 1, 2, 3, ... with no repeat and no hole, across the crash.
    std::map<std::string, std::uint64_t> last;
    for (const auto& record : read_all(directory)) {
        if (record.kind != trading::JournalKind::market_data) {
            continue;
        }
        const auto quote = trading::decode_market_data(record, trading::MonotonicClock::now());
        CHECK(quote.has_value());
        auto& previous = last[quote->symbol];
        CHECK(quote->sequence == previous + 1U);
        previous = quote->sequence;
    }
    CHECK(last.size() == 5U);
}

void a_component_that_keeps_crashing_is_quarantined_not_restarted_forever() {
    TempDir dir;
    auto config = resilient_config(dir.path());
    config.supervision.config.max_restarts = 2;
    config.supervision.config.restart_window = 60s;
    trading::TradingSystem system(config);
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 1500U; }, 5s));

    for (std::uint64_t round = 1; round <= 3; ++round) {
        CHECK(system.inject_fault("trend-fx", trading::Fault::crash));
        CHECK(eventually(
            [&] {
                const auto current = system.snapshot();
                const auto* component = find_component(current, "trend-fx");
                return component->restarts >= round || component->state == trading::ComponentState::quarantined;
            },
            5s));
    }
    const auto snapshot = system.snapshot();
    const auto* component = find_component(snapshot, "trend-fx");
    CHECK(component->state == trading::ComponentState::quarantined);
    CHECK(component->restarts == 2U);
    CHECK(snapshot.supervisor.quarantines == 1U);
    // The rest of the system carries on without it.
    CHECK(find_component(snapshot, "trend-tech")->state == trading::ComponentState::healthy);
    const auto has_entry = std::any_of(snapshot.supervision_log.begin(), snapshot.supervision_log.end(),
                                       [](const auto& entry) { return entry.action == "quarantined"; });
    CHECK(has_entry);
    system.stop();
}

void shutdown_does_not_wait_for_a_hung_worker() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal && system.snapshot().journal->records > 500U; }, 5s));
    CHECK(system.inject_fault("trend-tech", trading::Fault::hang, 60'000ms));
    std::this_thread::sleep_for(20ms);
    const auto started = trading::MonotonicClock::now();
    system.stop();
    CHECK(trading::MonotonicClock::now() - started < 5s);
}

// --- Replay --------------------------------------------------------------------------------

std::map<std::string, std::pair<double, std::uint64_t>> positions_of(const trading::SystemSnapshot& snapshot) {
    std::map<std::string, std::pair<double, std::uint64_t>> positions;
    for (const auto& position : snapshot.positions) {
        positions[position.strategy + "|" + position.symbol] = {position.quantity, position.fills};
    }
    return positions;
}

trading::SystemConfig finite_config(const fs::path& journal_root) {
    auto config = resilient_config(journal_root);
    for (auto& feed : config.feeds) {
        feed.events_per_instrument = 4'000;
        feed.interval = 100us;
    }
    config.supervision.enabled = false;
    return config;
}

void a_replay_of_a_clean_run_reproduces_every_signal_and_position() {
    TempDir dir;
    auto config = finite_config(dir.path());
    trading::TradingSystem live(config);
    live.start();
    CHECK(eventually([&] { return live.feeds_finished(); }, 20s));
    live.stop();
    const auto live_state = live.snapshot();
    CHECK(total_signals(live_state) > 0U);
    CHECK(live_state.journal && live_state.journal->gaps == 0U);
    for (const auto& strategy : live_state.strategies) {
        CHECK(strategy.worker.mailbox.evicted_oldest == 0U);  // nothing was shed, so the answer key is exact
    }

    trading::ReplayOptions options;
    options.directory = live.journal_directory();
    options.mode = trading::ReplayMode::rederive;
    options.pacing = trading::ReplayPacing::lockstep;
    auto base = live.config();
    base.session = live.session();
    trading::TradingSystem replay(trading::SystemConfig::for_replay(base, options));
    replay.start();
    CHECK(eventually([&] { return replay.feeds_finished(); }, 20s));
    replay.stop();

    const auto state = replay.snapshot();
    CHECK(state.mode == "replay");
    CHECK(state.replay.has_value());
    const auto& check = state.replay->verification;
    CHECK(check.verdict == "match");
    CHECK(check.recorded == total_signals(live_state));
    CHECK(check.matched == check.recorded && check.missing == 0U && check.extra == 0U);
    // Lockstep never sheds a quote, whatever the speed.
    for (const auto& strategy : state.strategies) {
        CHECK(strategy.worker.mailbox.evicted_oldest == 0U);
    }
    // And the orders the replay would have placed leave the same positions behind.
    CHECK(positions_of(state) == positions_of(live_state));
    // The replay never touched the live journal.
    CHECK(trading::JournalReader::bounds(live.journal_directory()).last == live_state.journal->last_sequence);
}

void a_playback_shows_the_recording_without_recomputing_anything() {
    TempDir dir;
    trading::TradingSystem live(finite_config(dir.path()));
    live.start();
    CHECK(eventually([&] { return live.feeds_finished(); }, 20s));
    live.stop();
    const auto live_state = live.snapshot();

    trading::ReplayOptions options;
    options.directory = live.journal_directory();
    options.mode = trading::ReplayMode::playback;
    options.pacing = trading::ReplayPacing::lockstep;
    auto base = live.config();
    base.session = live.session();
    trading::TradingSystem playback(trading::SystemConfig::for_replay(base, options));
    playback.start();
    CHECK(eventually([&] { return playback.feeds_finished(); }, 20s));
    CHECK(eventually([&] { return positions_of(playback.snapshot()) == positions_of(live_state); }, 5s));
    playback.stop();
    const auto state = playback.snapshot();
    // Nothing was recomputed: no strategies, no orders to any broker.
    CHECK(state.strategies.empty());
    CHECK(state.mock_api_calls == 0U);
    CHECK(positions_of(state) == positions_of(live_state));
}

void a_replay_explains_the_signals_a_restart_skipped() {
    TempDir dir;
    auto config = resilient_config(dir.path());
    for (auto& feed : config.feeds) {
        feed.events_per_instrument = 6'000;
        feed.interval = 150us;
    }
    trading::TradingSystem live(config);
    live.start();
    CHECK(eventually([&] { return live.snapshot().journal && live.snapshot().journal->records > 3000U; }, 10s));
    CHECK(live.inject_fault("trend-tech", trading::Fault::crash));
    CHECK(eventually([&] { return live.feeds_finished(); }, 30s));
    live.stop();
    const auto live_state = live.snapshot();
    CHECK(find_component(live_state, "trend-tech")->restarts == 1U);

    trading::ReplayOptions options;
    options.directory = live.journal_directory();
    options.pacing = trading::ReplayPacing::lockstep;
    auto base = live.config();
    base.session = live.session();
    trading::TradingSystem replay(trading::SystemConfig::for_replay(base, options));
    replay.start();
    CHECK(eventually([&] { return replay.feeds_finished(); }, 30s));
    replay.stop();
    const auto verdict = replay.snapshot().replay->verification;
    // Either nothing fired during the short outage, or what differs is on the restarted
    // strategy and says so. It is never an unexplained divergence.
    CHECK(verdict.verdict == "match" || verdict.verdict == "explained");
    CHECK(std::count(verdict.outage_components.begin(), verdict.outage_components.end(), "trend-tech") == 1);
}

void replay_pacing_pause_seek_and_speed_are_controllable() {
    TempDir dir;
    trading::TradingSystem live(finite_config(dir.path()));
    live.start();
    CHECK(eventually([&] { return live.feeds_finished(); }, 20s));
    live.stop();

    trading::ReplayOptions options;
    options.directory = live.journal_directory();
    options.pacing = trading::ReplayPacing::realtime;
    options.speed = 1.0;
    auto base = live.config();
    base.session = live.session();
    trading::TradingSystem replay(trading::SystemConfig::for_replay(base, options));
    replay.start();
    CHECK(eventually([&] { return replay.snapshot().replay->progress.published > 100U; }, 5s));

    CHECK(replay.replay_set_paused(true));
    CHECK(eventually([&] { return replay.snapshot().replay->progress.state == trading::ReplayState::paused; }, 1s));
    std::this_thread::sleep_for(60ms);  // let anything in flight land
    const auto frozen = replay.snapshot().replay->progress.published;
    std::this_thread::sleep_for(150ms);
    CHECK(replay.snapshot().replay->progress.published == frozen);

    // Moving the replay makes the comparison meaningless, and it says so.
    CHECK(replay.replay_seek(0.5));
    CHECK(replay.replay_set_paused(false));
    CHECK(replay.replay_set_pacing(trading::ReplayPacing::lockstep));
    CHECK(eventually([&] { return replay.feeds_finished(); }, 20s));
    replay.stop();
    const auto state = replay.snapshot();
    CHECK(state.replay->progress.seeked);
    CHECK(state.replay->verification.verdict == "not-comparable");
    CHECK(state.replay->progress.state == trading::ReplayState::finished);
}

void a_live_system_does_not_accept_replay_controls() {
    TempDir dir;
    trading::TradingSystem live(resilient_config(dir.path()));
    CHECK(!live.replay_set_paused(true));
    CHECK(!live.replay_seek(0.5));
    CHECK(!live.inject_fault("no-such-component", trading::Fault::crash));
    CHECK(!live.inject_fault("equities", trading::Fault::hang));  // feeds can only crash
}

// --- Session and dashboard controls --------------------------------------------------------

void the_session_routes_fault_and_replay_controls() {
    TempDir dir;
    trading::LiveSession session(resilient_config(dir.path()));
    session.start();
    CHECK(eventually([&] { return session.live().snapshot().journal->records > 2500U; }, 5s));

    CHECK(session.apply_control("fault.crash:trend-fx", "1").status == 200);
    CHECK(session.apply_control("fault.crash:nobody", "1").status == 404);
    CHECK(session.apply_control("fault.gateway:0", "1").status == 200);
    CHECK(session.apply_control("fault.gateway:9", "1").status == 404);
    CHECK(session.apply_control("fault.gateway:0", "0").status == 200);
    CHECK(session.apply_control("no.such.control", "1").status == 404);
    CHECK(session.apply_control("api.latency_ms", "abc").status == 400);
    CHECK(session.apply_control("replay.pause", "1").status == 409);  // nothing to pause yet
    CHECK(eventually([&] { return restarted(session.live(), "trend-fx", 1); }, 5s));

    // Watch a replay of what has been recorded so far, while the live system keeps running.
    CHECK(session.apply_control("replay.start:rederive", "max").status == 200);
    auto* replay = session.replay();
    CHECK(replay != nullptr);
    CHECK(eventually([&] { return replay->feeds_finished(); }, 20s));
    auto json = session.render();
    CHECK(looks_like_json(json));
    CHECK(json.find("\"view\":\"replay\"") != std::string::npos);
    CHECK(json.find("\"verification\"") != std::string::npos);

    CHECK(session.apply_control("view.show", "live").status == 200);
    json = session.render();
    CHECK(looks_like_json(json));
    CHECK(json.find("\"view\":\"live\"") != std::string::npos);
    CHECK(json.find("\"other_replay\"") != std::string::npos);
    CHECK(json.find("\"can_replay\":true") != std::string::npos);

    CHECK(session.apply_control("replay.stop", "1").status == 200);
    CHECK(session.replay() == nullptr);
    CHECK(session.apply_control("replay.start:sideways", "1").status == 400);
    session.stop();
}

void replay_needs_a_journal() {
    auto config = trading::SystemConfig::demo();
    for (auto& feed : config.feeds) {
        feed.events_per_instrument = 0;
    }
    trading::LiveSession session(config);
    session.start();
    CHECK(session.apply_control("replay.start:rederive", "1").status == 409);
    session.stop();
}

void telemetry_describes_resilience_journal_gateways_and_replay() {
    TempDir dir;
    trading::TradingSystem live(finite_config(dir.path()));
    live.start();
    CHECK(eventually([&] { return live.feeds_finished(); }, 20s));
    live.stop();
    auto json = render_json(live.snapshot());
    CHECK(looks_like_json(json));
    for (const char* key : {"\"resilience\"", "\"components\"", "\"journal\"", "\"gateways\"", "\"members\"",
                            "\"order-execution\"", "\"position-monitor\"", "\"recovery\""}) {
        CHECK(json.find(key) != std::string::npos);
    }
    CHECK(json.find("\"replay\"") == std::string::npos);

    trading::ReplayOptions options;
    options.directory = live.journal_directory();
    options.pacing = trading::ReplayPacing::lockstep;
    auto base = live.config();
    base.session = live.session();
    trading::TradingSystem replay(trading::SystemConfig::for_replay(base, options));
    replay.start();
    CHECK(eventually([&] { return replay.feeds_finished(); }, 20s));
    replay.stop();
    json = render_json(replay.snapshot());
    CHECK(looks_like_json(json));
    CHECK(json.find("\"replay\"") != std::string::npos);
    CHECK(json.find("\"verdict\":\"match\"") != std::string::npos);
}

}  // namespace

// --- Regressions from review ---------------------------------------------------------------

void position_recovery_never_counts_an_old_fill_twice() {
    TempDir dir;
    trading::EventBus bus;
    trading::JournalConfig config;
    config.directory = dir.path() / "fills";
    config.session = "fills";
    config.fsync_interval = 0ms;
    trading::JournalWriter journal(bus, config);
    journal.start();
    trading::PositionBook book;
    // More distinct fills than the book remembers, and the newest few reached the journal
    // but not the book: the monitor died before applying them.
    const std::uint64_t total = trading::PositionBook::kDedupeWindow + 4'000;
    const std::uint64_t missed = 3;
    const auto now = trading::MonotonicClock::now();
    for (std::uint64_t index = 1; index <= total; ++index) {
        trading::ExecutionReportEvent fill{.strategy = "alpha",
                                           .signal_id = index,
                                           .client_order_id = "f:" + std::to_string(index),
                                           .symbol = "AAPL",
                                           .side = trading::Side::buy,
                                           .quantity = 1.0,
                                           .fill_price = 100.0,
                                           .filled = true,
                                           .market_data_time = now,
                                           .execution_time = now};
        if (index <= total - missed) {
            book.apply(fill);
        }
        bus.publish(std::move(fill));
        if (index % 16'384U == 0U) {
            CHECK(journal.sync());  // keeps the tap from overflowing
        }
    }
    // Two rejections at the end: the book saw the first, and the monitor died before the second.
    for (std::uint64_t index = total + 1; index <= total + 2; ++index) {
        trading::ExecutionReportEvent rejection{.strategy = "alpha",
                                                .signal_id = index,
                                                .client_order_id = "f:" + std::to_string(index),
                                                .symbol = "AAPL",
                                                .side = trading::Side::buy,
                                                .quantity = 1.0,
                                                .filled = false,
                                                .market_data_time = now,
                                                .execution_time = now};
        if (index == total + 1) {
            CHECK(book.apply(rejection) == trading::ApplyResult::not_filled);
        }
        bus.publish(std::move(rejection));
    }
    CHECK(journal.sync());
    CHECK(journal.stats().gaps == 0U);

    const auto recovered = trading::reapply_recent_reports(journal, book);
    // Exactly the missed fills were added; nothing older was applied a second time.
    CHECK(book.position("alpha", "AAPL") == static_cast<double>(total));
    CHECK(book.fills_applied() == total);
    // And recovery reports what it added, so the monitor can count those outcomes.
    CHECK(recovered.fills == missed);
    CHECK(recovered.rejections == 1U);
    CHECK(recovered.already_applied > 0U);
    journal.stop();
}

void a_replacement_that_cannot_rebuild_its_state_dies_instead_of_going_live() {
    trading::EventBus bus;
    std::atomic<int> handled{0};
    std::atomic<int> rang{0};
    trading::EventWorker worker("rebuilding", bus, {}, [&](const trading::EventEnvelope&) { ++handled; });
    worker.subscribe<trading::MarketDataEvent>();
    worker.set_start_hook([] { throw trading::JournalError("journal damaged during recovery"); });
    worker.set_crash_hook([&] { ++rang; });
    worker.start();
    CHECK(worker.wait_ready(2s));  // returns once the worker has ended, ready or not
    bus.publish(make_quote("AAPL", 1.0, 1));
    const auto health = worker.health();
    CHECK(health.crashed && !health.ready);
    CHECK(health.error.find("state recovery failed") != std::string::npos);
    CHECK(eventually([&] { return rang.load() == 1; }, 1s));
    CHECK(handled.load() == 0);
    worker.stop();
}

void a_crash_wakes_the_supervisor_without_waiting_for_a_probe() {
    TempDir dir;
    auto config = resilient_config(dir.path());
    // Probes far apart: only the doorbell can explain a fast reaction.
    auto& supervision = config.supervision.config;
    supervision.probe_interval = 2'000ms;
    supervision.stall_timeout = 4'000ms;
    supervision.handler_deadline = 4'000ms;
    trading::TradingSystem system(config);
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal->records > 1500U; }, 5s));
    CHECK(system.inject_fault("trend-fx", trading::Fault::crash));
    CHECK(eventually([&] { return restarted(system, "trend-fx", 1); }, 1'000ms));
    const auto snapshot = system.snapshot();
    const auto* component = find_component(snapshot, "trend-fx");
    CHECK(component != nullptr && component->last_detect_ms < 500.0);
    system.stop();
}

void a_hung_position_monitor_cannot_touch_the_book_when_it_wakes() {
    TempDir dir;
    trading::TradingSystem system(resilient_config(dir.path()));
    system.start();
    CHECK(eventually([&] { return system.snapshot().journal->records > 2000U; }, 5s));
    // It hangs on the next fill it handles, and wakes long after it has been replaced.
    CHECK(system.inject_fault("position-monitor", trading::Fault::hang, 1'500ms));
    CHECK(eventually([&] { return restarted(system, "position-monitor", 1); }, 10s));
    CHECK(eventually([&] { return system.snapshot().supervisor.zombies == 0U; }, 5s));
    const auto snapshot = system.snapshot();
    const auto* component = find_component(snapshot, "position-monitor");
    CHECK(component != nullptr && component->fenced_writes >= 1U);
    system.stop();
    // The fill the stale monitor was holding was applied once, by its replacement.
    const auto final_state = system.snapshot();
    std::uint64_t fills = 0;
    for (const auto& position : final_state.positions) {
        fills += position.fills;
    }
    CHECK(fills == total_signals(final_state));
    // The monitor's own count agrees with the book, the recovered fill included.
    CHECK(final_state.position_monitor.fills + final_state.position_monitor.rejections == total_signals(final_state));
}

trading::GatewayPool::Member pool_member(const std::string& name, std::shared_ptr<trading::IApiClient> broker,
                                         trading::ApiManagerConfig config) {
    auto fault = std::make_shared<trading::FaultableApiClient>(std::move(broker));
    return {name, std::make_shared<trading::ApiRequestManager>(fault, config), fault};
}

void the_pool_only_resends_what_the_broker_can_deduplicate() {
    Completions done;
    auto fixture = make_pool();
    auto& pool = *fixture.pool;
    pool.set_down(0, true);
    auto request = order("not-idempotent");
    request.idempotent = false;
    CHECK(pool.submit(std::move(request), done.callback()).accepted());
    CHECK(done.wait_for(1, 2s));
    // The first attempt may have executed, so it fails rather than go out a second time.
    CHECK(!done.responses().front().ok());
    CHECK(pool.pool_stats().reroutes == 0U);
    CHECK(fixture.broker->invocation_count() == 0U);
}

void cancelling_a_rerouted_order_cancels_the_attempt_standing_in_for_it() {
    Completions done;
    trading::MockApiConfig mock_config;
    mock_config.latency = 300ms;
    auto broker = std::make_shared<trading::MockTradingApi>(mock_config);
    auto single_thread = quick_gateway();
    single_thread.io_threads = 1;
    std::vector<trading::GatewayPool::Member> members{pool_member("gateway-1", broker, quick_gateway()),
                                                      pool_member("gateway-2", broker, single_thread)};
    auto second = members[1].manager;
    trading::GatewayPool pool(std::move(members), trading::GatewayPoolConfig{});
    pool.set_down(0, true);
    // Keep gateway-2's only I/O thread busy, so the re-routed order waits in its queue.
    CHECK(second->submit(order("blocker"), done.callback()).accepted());
    CHECK(eventually([&] { return broker->invocation_count() == 1U; }, 1s));
    const auto handle = pool.submit(order("rerouted"), done.callback()).handle;
    CHECK(eventually([&] { return pool.pool_stats().reroutes == 1U; }, 1s));
    handle.cancel();
    CHECK(done.wait_for(2, 3s));
    const auto responses = done.responses();
    const auto cancelled = std::count_if(responses.begin(), responses.end(), [](const trading::ApiResponse& response) {
        return response.error == trading::ApiError::cancelled;
    });
    CHECK(cancelled == 1);
    CHECK(broker->invocation_count() == 1U);  // the re-routed order never reached the broker
    pool.shutdown();
}

void a_dropped_connection_fails_over_at_once() {
    Completions done;
    trading::MockApiConfig mock_config;
    mock_config.latency = 1ms;
    auto broker = std::make_shared<trading::MockTradingApi>(mock_config);
    auto config = quick_gateway();
    // Retries would be slow and visible: the dead path must not be retried at all.
    config.max_retries = 3;
    config.initial_retry_backoff = 300ms;
    config.max_retry_backoff = 300ms;
    config.retry_connection_failures = false;
    std::vector<trading::GatewayPool::Member> members{pool_member("gateway-1", broker, config),
                                                      pool_member("gateway-2", broker, config)};
    auto first = members[0].manager;
    trading::GatewayPool pool(std::move(members), trading::GatewayPoolConfig{});
    pool.set_down(0, true);
    const auto started = trading::MonotonicClock::now();
    CHECK(pool.submit(order("fast-failover"), done.callback()).accepted());
    CHECK(done.wait_for(1, 2s));
    CHECK(done.responses().front().ok());
    CHECK(trading::MonotonicClock::now() - started < 200ms);
    CHECK(first->stats().retries == 0U);
    pool.shutdown();
}

void the_reader_reports_records_missing_from_the_journal() {
    TempDir dir;
    trading::JournalConfig config;
    config.directory = dir.path() / "holes";
    config.session = "holes";
    config.segment_bytes = 4096;
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        const auto topic = bus.topic("AAPL");
        for (std::uint64_t index = 1; index <= 300; ++index) {
            bus.publish(make_quote("AAPL", 100.0, index), topic);
        }
        CHECK(journal.sync());
        journal.stop();
    }
    std::vector<fs::path> segments;
    for (const auto& entry : fs::directory_iterator(config.directory)) {
        if (entry.path().extension() == ".jnl") {
            segments.push_back(entry.path());
        }
    }
    std::sort(segments.begin(), segments.end());
    CHECK(segments.size() > 3U);
    fs::remove(segments[1]);  // as if it had been set aside as damaged

    trading::JournalReader reader(config.directory);
    std::uint64_t read = 0;
    while (reader.next()) {
        ++read;
    }
    CHECK(!reader.corrupt());
    CHECK(reader.missing() > 0U);
    CHECK(read + reader.missing() == 300U);
}

trading::VerificationReport verify_against(const fs::path& directory,
                                           const std::vector<trading::TradingSignalEvent>& derived) {
    const auto bounds = trading::JournalReader::bounds(directory);
    trading::EventBus bus;
    trading::ReplayVerifier verifier(bus, directory, bounds.first, bounds.last, true);
    verifier.start();
    for (auto signal : derived) {
        bus.publish(std::move(signal));
    }
    CHECK(verifier.wait_until_drained(2s));
    auto report = verifier.report(true);
    verifier.stop();
    return report;
}

void verification_counts_every_signal_and_compares_order_fields() {
    TempDir dir;
    trading::JournalConfig config;
    config.directory = dir.path() / "verify";
    config.session = "verify";
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        const auto topic = bus.topic("AAPL");
        for (std::uint64_t sequence = 1; sequence <= 3; ++sequence) {
            bus.publish(make_quote("AAPL", 100.0, sequence), topic);
        }
        bus.publish(make_signal("alpha", "AAPL", 1));
        bus.publish(make_signal("alpha", "AAPL", 2));
        CHECK(journal.sync());
        journal.stop();
    }
    auto report = verify_against(config.directory, {make_signal("alpha", "AAPL", 1), make_signal("alpha", "AAPL", 2)});
    CHECK(report.verdict == "match" && report.matched == 2U);

    // The same signal twice cannot stand in for one that is missing.
    report = verify_against(config.directory, {make_signal("alpha", "AAPL", 1), make_signal("alpha", "AAPL", 1)});
    CHECK(report.verdict == "diverged");
    CHECK(report.matched == 1U && report.missing == 1U && report.extra == 1U);

    // Same identity, different quantity: a different order, so not a match.
    auto bigger = make_signal("alpha", "AAPL", 2);
    bigger.quantity = 3.0;
    report = verify_against(config.directory, {make_signal("alpha", "AAPL", 1), bigger});
    CHECK(report.verdict == "diverged" && report.missing == 1U && report.extra == 1U);

    // A signal answering a quote past the end of the replayed range is not expected.
    report = verify_against(config.directory,
                            {make_signal("alpha", "AAPL", 1), make_signal("alpha", "AAPL", 2), make_signal("alpha", "AAPL", 9)});
    CHECK(report.extra == 1U && report.recorded == 2U);
}

void only_differences_inside_an_outage_are_explained() {
    TempDir dir;
    trading::JournalConfig config;
    config.directory = dir.path() / "outage";
    config.session = "outage";
    {
        trading::EventBus bus;
        trading::JournalWriter journal(bus, config);
        journal.start();
        const auto topic = bus.topic("AAPL");
        const auto now = trading::MonotonicClock::now();
        bus.publish(make_quote("AAPL", 100.0, 1), topic);  // well before the fault
        std::this_thread::sleep_for(300ms);
        bus.publish(trading::SupervisionEvent{"alpha", "fault", "worker thread died", 1, 0.0, 0.0, now});
        bus.publish(make_quote("AAPL", 100.0, 2), topic);  // while alpha was down
        bus.publish(trading::SupervisionEvent{"alpha", "restarted", "caught up", 2, 0.0, 4.0, now});
        bus.publish(make_quote("AAPL", 100.0, 3), topic);  // after it came back
        CHECK(journal.sync());
        journal.stop();
    }
    auto report = verify_against(config.directory, {make_signal("alpha", "AAPL", 2)});
    CHECK(report.verdict == "explained" && report.explained == 1U);
    for (const std::uint64_t outside : {1U, 3U}) {
        report = verify_against(config.directory, {make_signal("alpha", "AAPL", outside)});
        CHECK(report.verdict == "diverged" && report.explained == 0U);
    }
}

void a_finished_replay_can_still_be_moved_and_respects_small_mailboxes() {
    TempDir dir;
    trading::TradingSystem live(finite_config(dir.path()));
    live.start();
    CHECK(eventually([&] { return live.feeds_finished(); }, 20s));
    live.stop();

    trading::ReplayOptions options;
    options.directory = live.journal_directory();
    options.pacing = trading::ReplayPacing::lockstep;
    auto base = live.config();
    base.session = live.session();
    // One-slot mailboxes: lockstep must still never shed a quote.
    base.strategy_worker.mailbox_capacity = 1;
    trading::TradingSystem replay(trading::SystemConfig::for_replay(base, options));
    replay.start();
    CHECK(eventually([&] { return replay.feeds_finished(); }, 30s));
    auto state = replay.snapshot();
    CHECK(state.replay->verification.verdict == "match");
    for (const auto& strategy : state.strategies) {
        CHECK(strategy.worker.mailbox.evicted_oldest == 0U);
    }

    // At the end the replay waits, so it can be taken back.
    const auto first_pass = state.replay->progress.published;
    CHECK(replay.replay_seek(0.0));
    CHECK(eventually(
        [&] {
            const auto progress = replay.snapshot().replay->progress;
            return progress.state == trading::ReplayState::finished && progress.published > first_pass;
        },
        30s));
    replay.stop();
    state = replay.snapshot();
    CHECK(state.replay->verification.verdict == "not-comparable");
}

void a_replay_honours_an_explicit_topology_and_the_manifest_keeps_precision() {
    const char* arguments[] = {"trading_demo", "--replay", "/nonexistent", "--feed", "custom:XYZ@10",
                               "--strategy", "solo:threshold:XYZ"};
    const auto config = trading::AppConfig::from_command_line(7, const_cast<char**>(arguments));
    CHECK(config.replay);
    CHECK(config.system.feeds.size() == 1U && config.system.feeds.front().name == "custom");
    CHECK(config.system.strategies.size() == 1U && config.system.strategies.front().name == "solo");

    TempDir dir;
    trading::SessionManifest manifest;
    manifest.session = "precise";
    manifest.feeds.push_back({"fx", {{"EURUSD", 1.0853719}}});
    manifest.strategies.push_back({"alpha", "threshold", {"EURUSD"}, 1.23456789});
    trading::write_manifest(dir.path(), manifest);
    const auto read = trading::read_manifest(dir.path());
    CHECK(read && read->strategies.front().quantity == 1.23456789);
    CHECK(read->feeds.front().instruments.front().second == 1.0853719);
}

std::vector<std::pair<std::string, std::function<void()>>> resilience_tests() {
    return {
        {"journal round trip and resume", journal_round_trips_every_event_type_and_resumes_its_sequence},
        {"journal torn tail and damage", journal_repairs_a_torn_tail_and_sets_aside_a_damaged_segment},
        {"journal segments and seek", journal_rolls_segments_and_seeks_by_sequence},
        {"journal gap marker", journal_marks_a_gap_when_its_tap_overflows},
        {"generation gate fencing", generation_gate_rejects_writes_from_stale_generations},
        {"zombie processor is fenced", a_replaced_processor_cannot_publish_when_it_wakes_up},
        {"position book idempotency", position_book_applies_each_order_once},
        {"stall detector idle vs stuck", stall_detector_tells_idle_from_stuck},
        {"hung on the last event", a_worker_hung_on_its_last_event_is_stalled_not_ignored},
        {"gateway order lookup", gateway_lookup_reports_in_flight_and_completed_orders},
        {"execution recovery exactly once", a_restarted_execution_worker_sends_every_missed_order_exactly_once},
        {"recovery reconciles with broker", recovery_asks_the_broker_before_it_resubmits},
        {"stale signals expire", stale_signals_are_expired_not_traded_after_recovery},
        {"gateway pool failover", the_pool_fails_over_ejects_the_bad_gateway_and_fails_back},
        {"gateway pool keeps last gateway", the_pool_never_ejects_its_last_healthy_gateway},
        {"gateway pool ignores 429", rate_limiting_is_not_a_reason_to_fail_over},
        {"strategy crash recovery", a_crashed_strategy_is_replaced_and_rebuilds_its_state_from_the_journal},
        {"strategy hang recovery", a_hung_strategy_is_abandoned_fenced_and_replaced},
        {"poisoned worker restart", a_poisoned_worker_is_restarted_after_consecutive_failures},
        {"execution and position supervision", the_execution_worker_and_position_monitor_are_supervised_too},
        {"feed crash resumes sequence", a_crashed_feed_resumes_its_sequence},
        {"restart budget quarantine", a_component_that_keeps_crashing_is_quarantined_not_restarted_forever},
        {"shutdown with hung worker", shutdown_does_not_wait_for_a_hung_worker},
        {"replay reproduces a clean run", a_replay_of_a_clean_run_reproduces_every_signal_and_position},
        {"playback shows the recording", a_playback_shows_the_recording_without_recomputing_anything},
        {"replay explains restarts", a_replay_explains_the_signals_a_restart_skipped},
        {"replay controls", replay_pacing_pause_seek_and_speed_are_controllable},
        {"live system rejects replay controls", a_live_system_does_not_accept_replay_controls},
        {"session controls", the_session_routes_fault_and_replay_controls},
        {"replay needs a journal", replay_needs_a_journal},
        {"telemetry sections", telemetry_describes_resilience_journal_gateways_and_replay},
        {"position recovery past the dedupe window", position_recovery_never_counts_an_old_fill_twice},
        {"failed recovery dies", a_replacement_that_cannot_rebuild_its_state_dies_instead_of_going_live},
        {"crash rings the doorbell", a_crash_wakes_the_supervisor_without_waiting_for_a_probe},
        {"stale position monitor is fenced", a_hung_position_monitor_cannot_touch_the_book_when_it_wakes},
        {"pool resends only idempotent requests", the_pool_only_resends_what_the_broker_can_deduplicate},
        {"pool forwards cancellation", cancelling_a_rerouted_order_cancels_the_attempt_standing_in_for_it},
        {"pool fails over at once", a_dropped_connection_fails_over_at_once},
        {"reader reports missing records", the_reader_reports_records_missing_from_the_journal},
        {"verification multiset and fields", verification_counts_every_signal_and_compares_order_fields},
        {"outage windows explain differences", only_differences_inside_an_outage_are_explained},
        {"replay seek after end, small mailboxes", a_finished_replay_can_still_be_moved_and_respects_small_mailboxes},
        {"replay topology and manifest precision", a_replay_honours_an_explicit_topology_and_the_manifest_keeps_precision},
    };
}
