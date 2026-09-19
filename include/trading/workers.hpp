#pragma once

#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/metrics.hpp"
#include "trading/strategies.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace trading {

struct WorkerOptions {
    std::size_t mailbox_capacity{1'024};
    OverflowPolicy overflow{OverflowPolicy::reject_newest};
    std::size_t max_batch{64};
};

struct WorkerStats {
    std::string name{};
    std::uint64_t processed{};
    std::uint64_t handler_errors{};
    std::chrono::nanoseconds busy_time{};
    MailboxStats mailbox{};
    HistogramSnapshot dispatch_latency{};
};

// One thread running one handler, fed by one bounded mailbox. State used only by the
// handler is therefore single-threaded; statistics are exposed through atomics.
class EventWorker {
public:
    using Handler = std::function<void(const EventEnvelope&)>;

    EventWorker(std::string name, EventBus& event_bus, WorkerOptions options, Handler handler);
    ~EventWorker();
    EventWorker(const EventWorker&) = delete;
    EventWorker& operator=(const EventWorker&) = delete;

    template <BusEvent E>
    void subscribe() {
        std::lock_guard lock(lifecycle_mutex_);
        ensure_not_started_locked();
        subscriptions_.push_back(event_bus_.subscribe<E>(mailbox_, name_));
    }

    template <BusEvent E>
    void subscribe(Topic topic) {
        std::lock_guard lock(lifecycle_mutex_);
        ensure_not_started_locked();
        subscriptions_.push_back(event_bus_.subscribe<E>(mailbox_, topic, name_));
    }

    void start();
    // Unsubscribes, then handles every event already queued before the thread exits.
    void stop();
    [[nodiscard]] WorkerStats stats() const;
    [[nodiscard]] const std::string& name() const noexcept { return name_; }

private:
    void ensure_not_started_locked() const;
    void run();

    const std::string name_;
    EventBus& event_bus_;
    const WorkerOptions options_;
    std::shared_ptr<BoundedMailbox> mailbox_;
    Handler handler_;
    mutable std::mutex lifecycle_mutex_;
    std::vector<EventBus::Subscription> subscriptions_;
    std::thread thread_;
    bool started_{false};
    bool stopped_{false};
    std::atomic<std::uint64_t> processed_{0};
    std::atomic<std::uint64_t> handler_errors_{0};
    std::atomic<std::int64_t> busy_ns_{0};
    LatencyHistogram dispatch_latency_;
};

struct InstrumentConfig {
    std::string symbol{};
    double reference_price{100.0};
    // Standard deviation of each tick's return.
    double volatility_bps{1.0};
};

struct MarketDataProducerConfig {
    std::string name{"feed"};
    std::vector<InstrumentConfig> instruments{};
    // Quotes per instrument; 0 publishes until stopped.
    std::size_t events_per_instrument{3'000};
    // Pause between rounds; each round publishes one quote for every instrument.
    std::chrono::microseconds interval{1'000};
    std::uint64_t seed{1};
};

struct ProducerStats {
    std::string name{};
    std::vector<std::string> symbols{};
    std::uint64_t events_published{};
    std::uint64_t subscriber_rejections{};
    std::uint64_t failures{};
    std::chrono::microseconds interval{};
    bool paused{};
    bool finished{};
};

// Simulated feed handler: one thread publishing quotes for several instruments, each on
// its own topic, so a strategy's mailbox only receives the instruments it subscribed to.
class MarketDataProducer {
public:
    MarketDataProducer(EventBus& event_bus, MarketDataProducerConfig config);
    ~MarketDataProducer();
    MarketDataProducer(const MarketDataProducer&) = delete;
    MarketDataProducer& operator=(const MarketDataProducer&) = delete;

    void start();
    void stop();
    void wait();

    // Runtime controls; both take effect from the next round.
    void set_interval(std::chrono::microseconds interval);
    void set_paused(bool paused);

    [[nodiscard]] ProducerStats stats() const;
    [[nodiscard]] const std::string& name() const noexcept { return config_.name; }

private:
    void run(std::stop_token stop_token);
    bool wait_for_next_round(std::stop_token stop_token, TimePoint& next_round);

    EventBus& event_bus_;
    const MarketDataProducerConfig config_;
    std::vector<Topic> topics_;
    std::atomic<std::int64_t> interval_us_;
    std::atomic<bool> finished_{false};
    std::atomic<std::uint64_t> events_published_{0};
    std::atomic<std::uint64_t> subscriber_rejections_{0};
    std::atomic<std::uint64_t> failures_{0};
    // paused_ is written under control_mutex_ so a resume cannot slip past the wait.
    mutable std::mutex control_mutex_;
    std::condition_variable_any control_wakeup_;
    bool paused_{false};
    std::jthread thread_;
};

struct SignalProcessorConfig {
    std::string name{"strategy"};
    // Instruments to subscribe to; empty subscribes to every instrument.
    std::vector<std::string> symbols{};
    double order_quantity{1.0};
    // Market data is the one stream where skipping stale events beats rejecting new ones.
    WorkerOptions worker{1'024, OverflowPolicy::drop_oldest, 64};
};

struct SignalProcessorStats {
    WorkerStats worker{};
    std::string kind{};
    std::vector<std::string> symbols{};
    std::uint64_t signals_published{};
    std::uint64_t publish_rejections{};
    HistogramSnapshot tick_to_signal{};
};

// Runs one strategy on its own thread against its own market-data mailbox. Adding a
// strategy adds a processor; no other component changes.
class SignalProcessor {
public:
    SignalProcessor(EventBus& event_bus,
                    SignalProcessorConfig config,
                    std::unique_ptr<ISignalStrategy> strategy);

    void start();
    void stop();
    [[nodiscard]] SignalProcessorStats stats() const;
    [[nodiscard]] const std::string& name() const noexcept { return config_.name; }

private:
    void on_event(const EventEnvelope& envelope);

    EventBus& event_bus_;
    const SignalProcessorConfig config_;
    std::unique_ptr<ISignalStrategy> strategy_;
    const std::string kind_;
    std::uint64_t next_signal_id_{1};
    std::atomic<std::uint64_t> signals_published_{0};
    std::atomic<std::uint64_t> publish_rejections_{0};
    LatencyHistogram tick_to_signal_;
    // Declared last so its thread stops before the state it uses is destroyed.
    EventWorker worker_;
};

struct OrderExecutionConfig {
    std::chrono::milliseconds request_timeout{500};
    WorkerOptions worker{};
};

struct ExecutionStats {
    WorkerStats worker{};
    std::uint64_t requests_submitted{};
    std::uint64_t requests_rejected{};
    std::uint64_t report_publish_rejections{};
};

// Turns signals from every strategy into orders on the shared request manager. It never
// waits for the API: completions arrive on API threads and are published as reports.
class OrderExecutionWorker {
public:
    OrderExecutionWorker(EventBus& event_bus,
                         IRequestManager& request_manager,
                         OrderExecutionConfig config);

    void start();
    void stop();
    [[nodiscard]] ExecutionStats stats() const;

private:
    void on_event(const EventEnvelope& envelope);

    EventBus& event_bus_;
    IRequestManager& request_manager_;
    const OrderExecutionConfig config_;
    std::atomic<std::uint64_t> requests_submitted_{0};
    std::atomic<std::uint64_t> requests_rejected_{0};
    // Shared with in-flight completions, which may outlive this worker.
    std::shared_ptr<std::atomic<std::uint64_t>> report_publish_rejections_;
    EventWorker worker_;
};

struct PositionEntry {
    std::string strategy{};
    std::string symbol{};
    double quantity{};
    // Signed cash flow of all fills; profit and loss is cash + quantity * mark price.
    double cash{};
    std::uint64_t fills{};
};

class PositionBook {
public:
    void apply(const ExecutionReportEvent& execution);
    [[nodiscard]] std::vector<PositionEntry> snapshot() const;
    [[nodiscard]] double position(const std::string& strategy, const std::string& symbol) const;
    [[nodiscard]] double net_position(const std::string& symbol) const;
    [[nodiscard]] std::uint64_t fills_applied() const;

private:
    mutable std::mutex mutex_;
    std::map<std::pair<std::string, std::string>, PositionEntry> positions_;
    std::uint64_t fills_applied_{};
};

struct PositionMonitorConfig {
    WorkerOptions worker{};
};

struct PositionMonitorStats {
    WorkerStats worker{};
    std::uint64_t fills{};
    std::uint64_t rejections{};
    HistogramSnapshot end_to_end{};
    HistogramSnapshot api_queue{};
    HistogramSnapshot rate_limit_wait{};
    HistogramSnapshot api_service{};
};

class PositionMonitor {
public:
    PositionMonitor(EventBus& event_bus, PositionBook& position_book, PositionMonitorConfig config);

    void start();
    void stop();
    [[nodiscard]] PositionMonitorStats stats() const;

private:
    void on_event(const EventEnvelope& envelope);

    PositionBook& position_book_;
    std::atomic<std::uint64_t> fills_{0};
    std::atomic<std::uint64_t> rejections_{0};
    LatencyHistogram end_to_end_;
    LatencyHistogram api_queue_;
    LatencyHistogram rate_limit_wait_;
    LatencyHistogram api_service_;
    EventWorker worker_;
};

struct ActivityRecorderConfig {
    // An observer must never slow the pipeline down, so it sheds its oldest events.
    WorkerOptions worker{16'384, OverflowPolicy::drop_oldest, 256};
    std::chrono::milliseconds sample_interval{50};
    std::size_t history_points{400};
    std::size_t tape_entries{60};
};

struct PricePoint {
    double time_s{};
    double price{};
};

struct InstrumentActivity {
    std::string symbol{};
    double last_price{};
    std::uint64_t updates{};
    std::vector<PricePoint> history{};
};

struct TapeEntry {
    enum class Kind : std::uint8_t { signal, fill, rejection };

    Kind kind{Kind::signal};
    double time_s{};
    std::string strategy{};
    std::string symbol{};
    Side side{Side::buy};
    double quantity{};
    double price{};
    // Tick-to-signal for signals; market data to execution report for executions.
    double latency_ms{};
    std::string detail{};
};

struct ActivitySnapshot {
    WorkerStats worker{};
    std::vector<InstrumentActivity> instruments{};
    std::vector<TapeEntry> tape{};
};

// Dashboard tap: an ordinary subscriber to every event type that keeps a downsampled price
// history and a short tape of signals and executions. Being just another mailbox, it
// shows that observers can be added without touching the trading components.
class ActivityRecorder {
public:
    ActivityRecorder(EventBus& event_bus, ActivityRecorderConfig config, TimePoint epoch);

    void start();
    void stop();
    [[nodiscard]] ActivitySnapshot snapshot() const;

private:
    struct Series {
        double last_price{};
        std::uint64_t updates{};
        std::deque<PricePoint> history;
        std::int64_t last_bucket{-1};
    };

    void on_event(const EventEnvelope& envelope);
    void on_market_data(const MarketDataEvent& quote);
    void append_tape(TapeEntry entry);
    [[nodiscard]] double seconds_since_epoch(TimePoint time) const;

    const ActivityRecorderConfig config_;
    const TimePoint epoch_;
    mutable std::mutex mutex_;
    std::map<std::string, Series> series_;
    std::deque<TapeEntry> tape_;
    EventWorker worker_;
};

}  // namespace trading
