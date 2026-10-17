#pragma once

#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/fault.hpp"
#include "trading/fencing.hpp"
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
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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
    std::uint64_t consecutive_errors{};
    bool crashed{};
};

// What a supervisor needs to tell a hung worker from an idle one: idle means an empty
// mailbox; hung means work is waiting while nothing completes.
struct WorkerHealth {
    bool crashed{};
    bool finished{};
    bool abandoned{};
    // False while the start hook (state recovery) is still running.
    bool ready{};
    std::uint64_t processed{};
    std::size_t backlog{};
    // How long the handler currently running has been running; zero when none is.
    std::chrono::nanoseconds handler_running_for{};
    // How long ago the thread died; zero while it is alive.
    std::chrono::nanoseconds crashed_for{};
    std::uint64_t consecutive_errors{};
    std::string error{};
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

    // Runs on the worker thread before it consumes its mailbox, which keeps buffering
    // meanwhile. Used to rebuild a replacement worker's state from the journal. If it
    // throws, the worker dies instead of going live with half-rebuilt state.
    void set_start_hook(std::function<void()> hook);
    // Runs on the worker thread as it dies, so a supervisor can act at once rather than at
    // its next probe. Must not block.
    void set_crash_hook(std::function<void()> hook);

    void start();
    // Unsubscribes, then handles every event already queued before the thread exits.
    void stop();
    // For a worker that may be stuck inside its handler: unsubscribes, closes the mailbox
    // and tells the thread to discard what is left, but never waits for it. The object
    // must stay alive until finished() is true; the destructor then joins immediately.
    void abandon();

    // Blocks until the start hook has completed (or the worker ended). Returns false on
    // timeout.
    bool wait_ready(std::chrono::milliseconds timeout);
    // Blocks until every event accepted so far has been handled (or shed). Returns false
    // on timeout. Meaningful for healthy workers only.
    bool wait_until_drained(std::chrono::milliseconds timeout);

    [[nodiscard]] WorkerStats stats() const;
    // The mailbox counters alone: cheaper than stats(), which also copies the histogram.
    [[nodiscard]] MailboxStats mailbox_stats() const { return mailbox_->stats(); }
    [[nodiscard]] WorkerHealth health() const;
    [[nodiscard]] bool finished() const noexcept { return finished_.load(std::memory_order_acquire); }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }

private:
    void ensure_not_started_locked() const;
    void run();
    void run_loop();
    void mark_ready();
    void note_handler_error(const char* what);
    void record_crash(const char* what);

    const std::string name_;
    EventBus& event_bus_;
    const WorkerOptions options_;
    std::shared_ptr<BoundedMailbox> mailbox_;
    Handler handler_;
    mutable std::mutex lifecycle_mutex_;
    std::vector<EventBus::Subscription> subscriptions_;
    std::thread thread_;
    std::function<void()> start_hook_;
    std::function<void()> crash_hook_;
    bool started_{false};
    bool stopped_{false};
    std::atomic<std::uint64_t> processed_{0};
    std::atomic<std::uint64_t> handler_errors_{0};
    std::atomic<std::uint64_t> consecutive_errors_{0};
    std::atomic<std::int64_t> busy_ns_{0};
    // Steady-clock time at which the running handler began; zero while none is running.
    std::atomic<std::int64_t> handler_since_ns_{0};
    // Steady-clock time at which the thread died; zero while it is alive.
    std::atomic<std::int64_t> crashed_at_ns_{0};
    std::atomic<bool> crashed_{false};
    std::atomic<bool> abandoned_{false};
    std::atomic<bool> finished_{false};
    std::atomic<bool> ready_{false};
    mutable std::mutex error_mutex_;
    std::string last_error_;
    std::mutex wait_mutex_;
    std::condition_variable wait_cv_;
    std::atomic<int> waiters_{0};
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
    // A restarted feed continues the sequence and prices of the instance it replaces, the
    // way a reconnecting feed handler resumes from its last sequence number.
    std::uint64_t start_round{0};
    std::vector<double> start_prices{};
};

struct FeedResumePoint {
    std::uint64_t next_round{};
    std::vector<double> prices{};
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
    // The feed thread died with an error (finished is also set).
    bool crashed{};
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
    // Fault injection: the feed thread fails at the start of its next round.
    void inject_crash() noexcept { crash_requested_.store(true, std::memory_order_relaxed); }
    // Runs on the feed thread as it dies; must not block. Set before start().
    void set_crash_hook(std::function<void()> hook) { crash_hook_ = std::move(hook); }
    // Where a replacement should continue from; read it after the feed has ended.
    [[nodiscard]] FeedResumePoint resume_point() const;
    // How long ago the feed thread died; zero while it is alive.
    [[nodiscard]] std::chrono::nanoseconds crashed_for() const noexcept;

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
    std::atomic<bool> crashed_{false};
    std::atomic<bool> crash_requested_{false};
    std::atomic<std::int64_t> crashed_at_ns_{0};
    std::function<void()> crash_hook_;
    std::atomic<std::uint64_t> next_round_{0};
    std::unique_ptr<std::atomic<double>[]> last_prices_;
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
    // Recovery: quotes replayed from the journal into a replacement, the ones its strategy
    // refused (dead-lettered), and signals dropped because this instance had been fenced.
    std::uint64_t recovered_records{};
    std::uint64_t dead_letters{};
    std::uint64_t fenced_writes{};
    double catch_up_ms{};
};

// Streams the quotes a strategy trades from durable storage, oldest first, into sink, and
// returns how many it delivered. An empty symbol list means every instrument.
using CatchUpSource = std::function<std::uint64_t(
    const std::vector<std::string>& symbols, const std::function<void(const MarketDataEvent&)>& sink)>;

// Runs one strategy on its own thread against its own market-data mailbox. Adding a
// strategy adds a processor; no other component changes.
class SignalProcessor {
public:
    SignalProcessor(EventBus& event_bus,
                    SignalProcessorConfig config,
                    std::unique_ptr<ISignalStrategy> strategy);

    // Both must be set before start(). With a gate, signals are published only while
    // generation is still the gate's current one. With a catch-up source, the strategy
    // first replays history silently (no signals: acting on stale quotes would be wrong)
    // and the mailbox's overlap with that history is skipped by sequence number.
    void set_fence(std::shared_ptr<GenerationGate> gate, std::uint64_t generation);
    void set_catch_up(CatchUpSource source);
    void set_crash_hook(std::function<void()> hook) { worker_.set_crash_hook(std::move(hook)); }

    void start();
    void stop();
    void abandon();
    [[nodiscard]] SignalProcessorStats stats() const;
    [[nodiscard]] WorkerHealth health() const { return worker_.health(); }
    [[nodiscard]] bool finished() const noexcept { return worker_.finished(); }
    [[nodiscard]] bool wait_ready(std::chrono::milliseconds timeout) { return worker_.wait_ready(timeout); }
    [[nodiscard]] bool wait_until_drained(std::chrono::milliseconds timeout) {
        return worker_.wait_until_drained(timeout);
    }
    [[nodiscard]] const std::string& name() const noexcept { return config_.name; }

private:
    void on_event(const EventEnvelope& envelope);
    void catch_up();

    EventBus& event_bus_;
    const SignalProcessorConfig config_;
    std::unique_ptr<ISignalStrategy> strategy_;
    const std::string kind_;
    std::shared_ptr<GenerationGate> gate_;
    std::uint64_t generation_{1};
    CatchUpSource catch_up_source_;
    // Highest sequence replayed per symbol; only touched by the worker thread. Live quotes
    // at or below it are the overlap between the journal and the mailbox.
    std::unordered_map<std::string, std::uint64_t> applied_;
    bool dedupe_active_{false};
    std::atomic<std::uint64_t> signals_published_{0};
    std::atomic<std::uint64_t> publish_rejections_{0};
    std::atomic<std::uint64_t> recovered_records_{0};
    std::atomic<std::uint64_t> dead_letters_{0};
    std::atomic<std::int64_t> catch_up_ns_{0};
    LatencyHistogram tick_to_signal_;
    // Declared last so its thread stops before the state it uses is destroyed.
    EventWorker worker_;
};

struct OrderExecutionConfig {
    std::chrono::milliseconds request_timeout{500};
    WorkerOptions worker{};
    // Prefixes every order id so ids from different runs never meet in a broker's
    // deduplication window.
    std::string session{};
    // After a restart, a journaled signal the broker has never heard of is resubmitted only
    // while it is younger than this; an older one is reported as expired instead.
    std::chrono::milliseconds stale_signal_after{5'000};
};

struct ExecutionRecoveryStats {
    std::uint64_t examined{};
    std::uint64_t resubmitted{};
    std::uint64_t already_in_flight{};
    std::uint64_t already_completed{};
    std::uint64_t expired{};
};

struct ExecutionStats {
    WorkerStats worker{};
    std::uint64_t requests_submitted{};
    std::uint64_t requests_rejected{};
    std::uint64_t report_publish_rejections{};
    ExecutionRecoveryStats recovery{};
    std::uint64_t fenced_writes{};
};

// Turns signals from every strategy into orders on the shared request manager. It never
// waits for the API: completions arrive on API threads and are published as reports.
class OrderExecutionWorker {
public:
    OrderExecutionWorker(EventBus& event_bus,
                         IRequestManager& request_manager,
                         OrderExecutionConfig config);

    // Must be called before start(). Orders are submitted only while generation is the
    // gate's current one, so a replaced worker that wakes up cannot send stale orders.
    void set_fence(std::shared_ptr<GenerationGate> gate, std::uint64_t generation);
    // Runs on the worker thread before it consumes its mailbox, and may call reconcile().
    void set_recovery(std::function<void(OrderExecutionWorker&)> recovery);
    // Fault injection from the dashboard.
    void set_fault(std::shared_ptr<FaultControl> fault) { fault_ = std::move(fault); }
    void set_crash_hook(std::function<void()> hook) { worker_.set_crash_hook(std::move(hook)); }

    void start();
    void stop();
    void abandon();

    // The broker idempotency key for a signal: stable across restarts and replays.
    [[nodiscard]] std::string order_id_for(const TradingSignalEvent& signal) const;
    // Recovery of one signal that has no execution report: asks the broker side what it
    // knows, then publishes the known outcome, resubmits, or expires the signal. Call from
    // the recovery function only (it runs on the worker thread).
    void reconcile(const TradingSignalEvent& signal, std::chrono::nanoseconds age);

    [[nodiscard]] ExecutionStats stats() const;
    [[nodiscard]] WorkerHealth health() const { return worker_.health(); }
    [[nodiscard]] bool finished() const noexcept { return worker_.finished(); }
    [[nodiscard]] bool wait_ready(std::chrono::milliseconds timeout) { return worker_.wait_ready(timeout); }
    [[nodiscard]] bool wait_until_drained(std::chrono::milliseconds timeout) {
        return worker_.wait_until_drained(timeout);
    }

private:
    void on_event(const EventEnvelope& envelope);
    void submit_order(const TradingSignalEvent& signal, const std::string& order_id);

    EventBus& event_bus_;
    IRequestManager& request_manager_;
    const OrderExecutionConfig config_;
    std::shared_ptr<GenerationGate> gate_;
    std::shared_ptr<FaultControl> fault_;
    std::uint64_t generation_{1};
    std::function<void(OrderExecutionWorker&)> recovery_;
    // Order ids settled during recovery; live signals that overlap the journal are skipped.
    std::unordered_set<std::string> recovered_ids_;
    std::atomic<std::uint64_t> recovery_examined_{0};
    std::atomic<std::uint64_t> recovery_resubmitted_{0};
    std::atomic<std::uint64_t> recovery_in_flight_{0};
    std::atomic<std::uint64_t> recovery_completed_{0};
    std::atomic<std::uint64_t> recovery_expired_{0};
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

enum class ApplyResult {
    applied,
    not_filled,
    // A fill for an order id that was already applied: redelivery after a restart, or a
    // report published by both a recovered and an original completion.
    duplicate,
};

// Positions derived from fills. Applying is idempotent per client order id, which is what
// lets the order path be at-least-once (journal replay, resubmission) without double
// counting. Reports without an order id are always applied.
class PositionBook {
public:
    // Remembers this many order ids; older ones are forgotten, like a broker's window. A
    // duplicate arrives close behind its original (a redelivery, a recovered completion),
    // so the window only has to span that gap, never the whole session.
    static constexpr std::size_t kDedupeWindow = 65'536;

    ApplyResult apply(const ExecutionReportEvent& execution);
    [[nodiscard]] std::vector<PositionEntry> snapshot() const;
    [[nodiscard]] double position(const std::string& strategy, const std::string& symbol) const;
    [[nodiscard]] double net_position(const std::string& symbol) const;
    [[nodiscard]] std::uint64_t fills_applied() const;
    [[nodiscard]] std::uint64_t duplicates_ignored() const;

private:
    mutable std::mutex mutex_;
    std::map<std::pair<std::string, std::string>, PositionEntry> positions_;
    std::uint64_t fills_applied_{};
    std::uint64_t duplicates_ignored_{};
    std::unordered_set<std::string> applied_ids_;
    std::deque<std::string> applied_order_;
};

struct PositionMonitorConfig {
    WorkerOptions worker{};
};

struct PositionMonitorStats {
    WorkerStats worker{};
    std::uint64_t fills{};
    std::uint64_t rejections{};
    std::uint64_t duplicates{};
    HistogramSnapshot end_to_end{};
    HistogramSnapshot api_queue{};
    HistogramSnapshot rate_limit_wait{};
    HistogramSnapshot api_service{};
};

class PositionMonitor {
public:
    PositionMonitor(EventBus& event_bus, PositionBook& position_book, PositionMonitorConfig config);

    // Must be called before start(). Reports are applied only while generation is the
    // gate's current one, so a replaced monitor that wakes up cannot touch the book.
    void set_fence(std::shared_ptr<GenerationGate> gate, std::uint64_t generation);
    // Runs on the worker thread before it consumes its mailbox; a replacement uses it to
    // reapply the journaled execution reports its predecessor missed.
    void set_recovery(std::function<void()> recovery);
    void set_fault(std::shared_ptr<FaultControl> fault) { fault_ = std::move(fault); }
    void set_crash_hook(std::function<void()> hook) { worker_.set_crash_hook(std::move(hook)); }

    void start();
    void stop();
    void abandon();
    [[nodiscard]] PositionMonitorStats stats() const;
    [[nodiscard]] WorkerHealth health() const { return worker_.health(); }
    [[nodiscard]] bool finished() const noexcept { return worker_.finished(); }
    [[nodiscard]] bool wait_ready(std::chrono::milliseconds timeout) { return worker_.wait_ready(timeout); }
    [[nodiscard]] bool wait_until_drained(std::chrono::milliseconds timeout) {
        return worker_.wait_until_drained(timeout);
    }

private:
    void on_event(const EventEnvelope& envelope);

    PositionBook& position_book_;
    std::shared_ptr<FaultControl> fault_;
    std::shared_ptr<GenerationGate> gate_;
    std::uint64_t generation_{1};
    std::atomic<std::uint64_t> fills_{0};
    std::atomic<std::uint64_t> rejections_{0};
    std::atomic<std::uint64_t> duplicates_{0};
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
    enum class Kind : std::uint8_t { signal, fill, rejection, supervision };

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
    // Events are placed at their own time since epoch, or, given a timeline, at the
    // timeline's position when they arrive: a replay passes its position in the recording,
    // so its charts show recorded time however fast the replay runs.
    ActivityRecorder(EventBus& event_bus, ActivityRecorderConfig config, TimePoint epoch,
                     std::function<double()> timeline = {});

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
    const std::function<double()> timeline_;
    mutable std::mutex mutex_;
    std::map<std::string, Series> series_;
    std::deque<TapeEntry> tape_;
    EventWorker worker_;
};

}  // namespace trading
