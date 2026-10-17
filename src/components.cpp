#include "trading/components.hpp"

#include <deque>
#include <set>
#include <sstream>
#include <unordered_set>

namespace trading {
namespace {

// Recovery reads the journal, so it must be able to trust it: everything published before
// now has to be readable, and nothing may have been lost on the way to disk.
void sync_for_recovery(JournalWriter& journal) {
    if (!journal.sync()) {
        const auto stats = journal.stats();
        throw JournalError(stats.failed ? "the journal has failed: " + stats.error
                                        : std::string("the journal did not catch up with the bus"));
    }
}

void check_readable(const JournalReader& reader) {
    if (reader.corrupt()) {
        throw JournalError("journal damaged during recovery: " + reader.error());
    }
}

// Streams the market data a strategy trades out of the journal, oldest first. The journal
// keeps growing while it is read, so each pass starts with a sync and the reader goes round
// again until a pass finds little new.
std::uint64_t stream_market_data(JournalWriter& journal, const std::vector<std::string>& symbols,
                                 const std::function<void(const MarketDataEvent&)>& sink) {
    const std::set<std::string> wanted(symbols.begin(), symbols.end());
    JournalReader reader(journal.directory());
    std::uint64_t delivered = 0;
    for (int pass = 0; pass < 8; ++pass) {
        sync_for_recovery(journal);
        std::uint64_t this_pass = 0;
        while (const auto record = reader.next()) {
            if (record->kind != JournalKind::market_data) {
                continue;
            }
            if (!wanted.empty() && !wanted.contains(record->topic)) {
                continue;
            }
            if (const auto quote = decode_market_data(*record, MonotonicClock::now())) {
                sink(*quote);
                ++delivered;
                ++this_pass;
            }
        }
        check_readable(reader);
        if (this_pass < 32U) {
            break;  // caught up with the tail
        }
    }
    return delivered;
}

// Reconciles every journaled signal that has no execution report against the broker.
// Signals keep arriving meanwhile and wait in the replacement's mailbox, which could fill up
// and reject some; those are still journaled. So the reader goes back over the journal's
// tail until a pass finds no new signal, and the live path skips whatever was settled here.
void reconcile_unreported(JournalWriter& journal, OrderExecutionWorker& worker) {
    JournalReader reader(journal.directory());
    std::unordered_set<std::string> reported;
    for (int pass = 0; pass < 8; ++pass) {
        sync_for_recovery(journal);
        const auto stamp = MonotonicClock::now();
        std::vector<std::pair<TradingSignalEvent, std::int64_t>> signals;
        while (const auto record = reader.next()) {
            if (record->kind == JournalKind::trading_signal) {
                if (auto signal = decode_signal(*record, stamp)) {
                    signals.emplace_back(std::move(*signal), record->wall_ns);
                }
            } else if (record->kind == JournalKind::execution_report) {
                if (const auto report = decode_execution_report(*record, stamp)) {
                    reported.insert(report->client_order_id);
                }
            }
        }
        check_readable(reader);
        const auto now = wall_clock_ns();
        for (const auto& [signal, wall_ns] : signals) {
            if (!reported.contains(worker.order_id_for(signal))) {
                worker.reconcile(signal, std::chrono::nanoseconds(now - wall_ns));
            }
        }
        if (signals.empty()) {
            break;
        }
    }
}

// Health of a replacement once the supervisor stops waiting for it: a replacement that died
// while rebuilding its state is a failed recovery, never a restart.
RecoveryResult started(const WorkerHealth& health, bool ready) {
    RecoveryResult result;
    if (health.crashed) {
        result.ok = false;
        result.detail = health.error;
    }
    result.ready = ready && !health.crashed;
    return result;
}

std::string with_commas(std::uint64_t value) {
    auto text = std::to_string(value);
    for (auto position = static_cast<std::ptrdiff_t>(text.size()) - 3; position > 0; position -= 3) {
        text.insert(static_cast<std::size_t>(position), ",");
    }
    return text;
}

std::string milliseconds_text(double ms) {
    std::ostringstream text;
    text.setf(std::ios::fixed);
    text.precision(ms < 10.0 ? 1 : 0);
    text << ms << " ms";
    return text.str();
}

}  // namespace

void reapply_recent_reports(JournalWriter& journal, PositionBook& book) {
    sync_for_recovery(journal);
    JournalReader reader(journal.directory());
    const auto stamp = MonotonicClock::now();
    // Half a window's worth, so that every fill among them that the book has already applied
    // is still among the ids it remembers, even allowing for small reorderings.
    constexpr std::size_t keep = PositionBook::kDedupeWindow / 2U;
    std::deque<ExecutionReportEvent> recent;
    while (const auto record = reader.next()) {
        if (record->kind == JournalKind::execution_report) {
            if (auto report = decode_execution_report(*record, stamp)) {
                recent.push_back(std::move(*report));
                if (recent.size() > keep) {
                    recent.pop_front();
                }
            }
        }
    }
    check_readable(reader);
    for (const auto& report : recent) {
        book.apply(report);
    }
}

// --- StrategyComponent ---------------------------------------------------------------------

StrategyComponent::StrategyComponent(const ComponentContext& context, StrategyConfig config, WorkerOptions worker)
    : SupervisedComponent(config.name, "strategy"),
      context_(context),
      config_(std::move(config)),
      worker_options_(worker),
      controls_(std::make_shared<StrategyControls>()),
      gate_(std::make_shared<GenerationGate>()) {
    processor_ = build(gate_->current(), false);
}

StrategyComponent::~StrategyComponent() = default;

std::shared_ptr<SignalProcessor> StrategyComponent::processor() const {
    std::lock_guard lock(mutex_);
    return processor_;
}

std::shared_ptr<SignalProcessor> StrategyComponent::build(std::uint64_t generation, bool catch_up) {
    auto strategy = std::make_unique<ControlledStrategy>(make_strategy(config_.kind), controls_);
    auto built = std::make_shared<SignalProcessor>(
        context_.bus,
        SignalProcessorConfig{config_.name, config_.symbols, config_.quantity, worker_options_},
        std::move(strategy));
    built->set_fence(gate_, generation);
    if (context_.doorbell) {
        built->set_crash_hook([doorbell = context_.doorbell] { doorbell->ring(); });
    }
    if (catch_up && context_.journal != nullptr) {
        built->set_catch_up([journal = context_.journal, controls = controls_](
                                const std::vector<std::string>& symbols,
                                const std::function<void(const MarketDataEvent&)>& sink) {
            // Replaying history must not sleep or fault: those are for live events.
            controls->catching_up.store(true, std::memory_order_relaxed);
            struct Reset {
                StrategyControls& controls;
                ~Reset() { controls.catching_up.store(false, std::memory_order_relaxed); }
            } reset{*controls};
            return stream_market_data(*journal, symbols, sink);
        });
    }
    return built;
}

void StrategyComponent::start() {
    processor()->start();
}

void StrategyComponent::stop() {
    processor()->stop();
}

bool StrategyComponent::wait_until_drained(std::chrono::milliseconds timeout) {
    return processor()->wait_until_drained(timeout);
}

SignalProcessorStats StrategyComponent::stats() const {
    std::shared_ptr<SignalProcessor> current;
    SignalProcessorStats total;
    {
        std::lock_guard lock(mutex_);
        current = processor_;
        total = carried_;
    }
    auto stats = current->stats();
    stats.worker.processed += total.worker.processed;
    stats.worker.mailbox.accepted += total.worker.mailbox.accepted;
    stats.worker.mailbox.evicted_oldest += total.worker.mailbox.evicted_oldest;
    stats.worker.mailbox.rejected_full += total.worker.mailbox.rejected_full;
    stats.signals_published += total.signals_published;
    stats.publish_rejections += total.publish_rejections;
    return stats;
}

void StrategyComponent::fill_status(ComponentStatus& status) const {
    status.fenced_writes = gate_->fenced_writes();
}

ProbeResult StrategyComponent::probe() {
    return evaluate_worker(processor()->health(), stall_, context_.supervision);
}

void StrategyComponent::retire(const std::shared_ptr<SignalProcessor>& retired) {
    retired->abandon();
    const auto last = retired->stats();
    {
        std::lock_guard lock(mutex_);
        carried_.worker.processed += last.worker.processed;
        carried_.worker.mailbox.accepted += last.worker.mailbox.accepted;
        carried_.worker.mailbox.evicted_oldest += last.worker.mailbox.evicted_oldest;
        carried_.worker.mailbox.rejected_full += last.worker.mailbox.rejected_full;
        carried_.signals_published += last.signals_published;
        carried_.publish_rejections += last.publish_rejections;
    }
    context_.graveyard.bury(config_.name, retired, [retired] { return retired->finished(); });
}

RecoveryResult StrategyComponent::recover(const std::string& cause) {
    static_cast<void>(cause);
    controls_->fault.disarm();
    retire(processor());
    // From here on, nothing the failed instance does can reach the bus.
    const auto generation = gate_->advance();
    auto fresh = build(generation, true);
    {
        std::lock_guard lock(mutex_);
        processor_ = fresh;
    }
    fresh->start();
    const bool ready = fresh->wait_ready(context_.supervision.recovery_timeout);

    auto result = started(fresh->health(), ready);
    if (!result.ok) {
        return result;
    }
    const auto stats = fresh->stats();
    result.recovered_records = stats.recovered_records;
    result.dead_letters = stats.dead_letters;
    if (context_.journal == nullptr) {
        result.detail = "restarted cold: no journal to rebuild state from";
    } else if (!ready) {
        result.detail = "still replaying history after " + milliseconds_text(static_cast<double>(
                                                              context_.supervision.recovery_timeout.count()));
    } else {
        result.detail = "replayed " + with_commas(stats.recovered_records) + " quotes silently in " +
                        milliseconds_text(stats.catch_up_ms) + ", then went live";
        if (stats.dead_letters > 0U) {
            result.detail += "; dead-lettered " + std::to_string(stats.dead_letters);
        }
    }
    return result;
}

void StrategyComponent::quarantine() {
    retire(processor());
    gate_->advance();
}

// --- ExecutionComponent --------------------------------------------------------------------

ExecutionComponent::ExecutionComponent(const ComponentContext& context, IRequestManager& gateway,
                                       OrderExecutionConfig config)
    : SupervisedComponent("order-execution", "execution"),
      context_(context),
      gateway_(gateway),
      config_(std::move(config)),
      fault_(std::make_shared<FaultControl>()),
      gate_(std::make_shared<GenerationGate>()) {
    worker_ = build(gate_->current(), false);
}

ExecutionComponent::~ExecutionComponent() = default;

std::shared_ptr<OrderExecutionWorker> ExecutionComponent::worker() const {
    std::lock_guard lock(mutex_);
    return worker_;
}

std::shared_ptr<OrderExecutionWorker> ExecutionComponent::build(std::uint64_t generation, bool recover) {
    auto built = std::make_shared<OrderExecutionWorker>(context_.bus, gateway_, config_);
    built->set_fence(gate_, generation);
    built->set_fault(fault_);
    if (context_.doorbell) {
        built->set_crash_hook([doorbell = context_.doorbell] { doorbell->ring(); });
    }
    if (recover && context_.journal != nullptr) {
        built->set_recovery([journal = context_.journal](OrderExecutionWorker& worker) {
            reconcile_unreported(*journal, worker);
        });
    }
    return built;
}

void ExecutionComponent::start() {
    worker()->start();
}

void ExecutionComponent::stop() {
    worker()->stop();
}

bool ExecutionComponent::wait_until_drained(std::chrono::milliseconds timeout) {
    return worker()->wait_until_drained(timeout);
}

ExecutionStats ExecutionComponent::stats() const {
    std::shared_ptr<OrderExecutionWorker> current;
    ExecutionStats total;
    {
        std::lock_guard lock(mutex_);
        current = worker_;
        total = carried_;
    }
    auto stats = current->stats();
    stats.worker.processed += total.worker.processed;
    stats.worker.mailbox.accepted += total.worker.mailbox.accepted;
    stats.worker.mailbox.rejected_full += total.worker.mailbox.rejected_full;
    stats.requests_submitted += total.requests_submitted;
    stats.requests_rejected += total.requests_rejected;
    stats.report_publish_rejections += total.report_publish_rejections;
    stats.recovery.examined += total.recovery.examined;
    stats.recovery.resubmitted += total.recovery.resubmitted;
    stats.recovery.already_in_flight += total.recovery.already_in_flight;
    stats.recovery.already_completed += total.recovery.already_completed;
    stats.recovery.expired += total.recovery.expired;
    return stats;
}

void ExecutionComponent::fill_status(ComponentStatus& status) const {
    status.fenced_writes = gate_->fenced_writes();
}

ProbeResult ExecutionComponent::probe() {
    return evaluate_worker(worker()->health(), stall_, context_.supervision);
}

RecoveryResult ExecutionComponent::recover(const std::string& cause) {
    static_cast<void>(cause);
    fault_->disarm();
    auto failed = worker();
    failed->abandon();
    {
        const auto last = failed->stats();
        std::lock_guard lock(mutex_);
        carried_.worker.processed += last.worker.processed;
        carried_.worker.mailbox.accepted += last.worker.mailbox.accepted;
        carried_.worker.mailbox.rejected_full += last.worker.mailbox.rejected_full;
        carried_.requests_submitted += last.requests_submitted;
        carried_.requests_rejected += last.requests_rejected;
        carried_.report_publish_rejections += last.report_publish_rejections;
        carried_.recovery.examined += last.recovery.examined;
        carried_.recovery.resubmitted += last.recovery.resubmitted;
        carried_.recovery.already_in_flight += last.recovery.already_in_flight;
        carried_.recovery.already_completed += last.recovery.already_completed;
        carried_.recovery.expired += last.recovery.expired;
    }
    context_.graveyard.bury(name(), failed, [failed] { return failed->finished(); });

    // A hung worker may wake up holding an order it never sent; the gate stops it, and
    // recovery below finds that signal in the journal and handles it exactly once.
    const auto generation = gate_->advance();
    auto fresh = build(generation, true);
    {
        std::lock_guard lock(mutex_);
        worker_ = fresh;
    }
    fresh->start();
    const bool ready = fresh->wait_ready(context_.supervision.recovery_timeout);

    auto result = started(fresh->health(), ready);
    if (!result.ok) {
        return result;
    }
    const auto recovery = fresh->stats().recovery;
    if (context_.journal == nullptr) {
        result.detail = "restarted cold: no journal, so signals missed while down are lost";
    } else if (!ready) {
        result.detail = "still reconciling with the broker";
    } else {
        result.detail = "reconciled " + std::to_string(recovery.examined) + " unreported signal" +
                        (recovery.examined == 1U ? "" : "s") + " against the broker: " +
                        std::to_string(recovery.resubmitted) + " resubmitted, " +
                        std::to_string(recovery.already_in_flight) + " already in flight, " +
                        std::to_string(recovery.already_completed) + " already filled, " +
                        std::to_string(recovery.expired) + " expired";
    }
    result.recovered_records = recovery.examined;
    return result;
}

void ExecutionComponent::quarantine() {
    auto failed = worker();
    failed->abandon();
    gate_->advance();
    context_.graveyard.bury(name(), failed, [failed] { return failed->finished(); });
}

// --- PositionComponent ---------------------------------------------------------------------

PositionComponent::PositionComponent(const ComponentContext& context, PositionBook& book,
                                     PositionMonitorConfig config)
    : SupervisedComponent("position-monitor", "position-monitor"),
      context_(context),
      book_(book),
      config_(config),
      fault_(std::make_shared<FaultControl>()),
      gate_(std::make_shared<GenerationGate>()) {
    monitor_ = build(gate_->current(), false);
}

PositionComponent::~PositionComponent() = default;

std::shared_ptr<PositionMonitor> PositionComponent::monitor() const {
    std::lock_guard lock(mutex_);
    return monitor_;
}

std::shared_ptr<PositionMonitor> PositionComponent::build(std::uint64_t generation, bool recover) {
    auto built = std::make_shared<PositionMonitor>(context_.bus, book_, config_);
    built->set_fence(gate_, generation);
    built->set_fault(fault_);
    if (context_.doorbell) {
        built->set_crash_hook([doorbell = context_.doorbell] { doorbell->ring(); });
    }
    if (recover && context_.journal != nullptr) {
        built->set_recovery([journal = context_.journal, &book = book_] { reapply_recent_reports(*journal, book); });
    }
    return built;
}

void PositionComponent::start() {
    monitor()->start();
}

void PositionComponent::stop() {
    monitor()->stop();
}

bool PositionComponent::wait_until_drained(std::chrono::milliseconds timeout) {
    return monitor()->wait_until_drained(timeout);
}

PositionMonitorStats PositionComponent::stats() const {
    std::shared_ptr<PositionMonitor> current;
    PositionMonitorStats total;
    {
        std::lock_guard lock(mutex_);
        current = monitor_;
        total = carried_;
    }
    auto stats = current->stats();
    stats.worker.processed += total.worker.processed;
    stats.worker.mailbox.accepted += total.worker.mailbox.accepted;
    stats.worker.mailbox.rejected_full += total.worker.mailbox.rejected_full;
    stats.fills += total.fills;
    stats.rejections += total.rejections;
    stats.duplicates += total.duplicates;
    return stats;
}

void PositionComponent::fill_status(ComponentStatus& status) const {
    status.fenced_writes = gate_->fenced_writes();
}

ProbeResult PositionComponent::probe() {
    return evaluate_worker(monitor()->health(), stall_, context_.supervision);
}

RecoveryResult PositionComponent::recover(const std::string& cause) {
    static_cast<void>(cause);
    fault_->disarm();
    auto failed = monitor();
    failed->abandon();
    {
        const auto last = failed->stats();
        std::lock_guard lock(mutex_);
        carried_.worker.processed += last.worker.processed;
        carried_.worker.mailbox.accepted += last.worker.mailbox.accepted;
        carried_.worker.mailbox.rejected_full += last.worker.mailbox.rejected_full;
        carried_.fills += last.fills;
        carried_.rejections += last.rejections;
        carried_.duplicates += last.duplicates;
    }
    context_.graveyard.bury(name(), failed, [failed] { return failed->finished(); });

    // A hung monitor may wake up holding a report; the gate keeps it away from the book, and
    // recovery below applies that report from the journal instead.
    const auto generation = gate_->advance();
    const auto before = book_.fills_applied();
    const auto duplicates_before = book_.duplicates_ignored();
    auto fresh = build(generation, true);
    {
        std::lock_guard lock(mutex_);
        monitor_ = fresh;
    }
    fresh->start();
    const bool ready = fresh->wait_ready(context_.supervision.recovery_timeout);

    auto result = started(fresh->health(), ready);
    if (!result.ok) {
        return result;
    }
    const auto applied = book_.fills_applied() - before;
    const auto recognised = book_.duplicates_ignored() - duplicates_before;
    if (context_.journal == nullptr) {
        result.detail = "restarted cold: fills reported while down are lost";
    } else if (!ready) {
        result.detail = "still reapplying journaled fills";
    } else {
        result.detail = "reapplied the newest journaled fills: " + std::to_string(applied) +
                        " missed while down, " + with_commas(recognised) + " recognised as already applied";
    }
    result.recovered_records = applied;
    return result;
}

void PositionComponent::quarantine() {
    auto failed = monitor();
    failed->abandon();
    gate_->advance();
    context_.graveyard.bury(name(), failed, [failed] { return failed->finished(); });
}

// --- FeedComponent -------------------------------------------------------------------------

FeedComponent::FeedComponent(const ComponentContext& context, MarketDataProducerConfig config)
    : SupervisedComponent(config.name, "feed"), context_(context), config_(std::move(config)) {
    producer_ = build(config_);
}

std::shared_ptr<MarketDataProducer> FeedComponent::build(const MarketDataProducerConfig& config) const {
    auto built = std::make_shared<MarketDataProducer>(context_.bus, config);
    if (context_.doorbell) {
        built->set_crash_hook([doorbell = context_.doorbell] { doorbell->ring(); });
    }
    return built;
}

FeedComponent::~FeedComponent() = default;

std::shared_ptr<MarketDataProducer> FeedComponent::producer() const {
    std::lock_guard lock(mutex_);
    return producer_;
}

void FeedComponent::start() {
    producer()->start();
}

void FeedComponent::stop() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    producer()->stop();
}

void FeedComponent::wait() {
    producer()->wait();
}

ProducerStats FeedComponent::stats() const {
    std::shared_ptr<MarketDataProducer> current;
    std::uint64_t published = 0;
    std::uint64_t rejections = 0;
    {
        std::lock_guard lock(mutex_);
        current = producer_;
        published = carried_published_;
        rejections = carried_rejections_;
    }
    auto stats = current->stats();
    stats.events_published += published;
    stats.subscriber_rejections += rejections;
    return stats;
}

bool FeedComponent::finished_normally() const {
    const auto stats = producer()->stats();
    return stats.finished && !stats.crashed;
}

bool FeedComponent::crashed() const {
    return producer()->stats().crashed;
}

bool FeedComponent::set_interval(std::chrono::microseconds interval) {
    producer()->set_interval(interval);
    return true;
}

bool FeedComponent::set_paused(bool paused) {
    producer()->set_paused(paused);
    return true;
}

void FeedComponent::inject_crash() {
    producer()->inject_crash();
}

ProbeResult FeedComponent::probe() {
    ProbeResult result;
    bool stopping = false;
    {
        std::lock_guard lock(mutex_);
        stopping = stopping_;
    }
    const auto current = producer();
    if (!stopping && current->stats().crashed) {
        result.verdict = ProbeResult::Verdict::fault;
        result.state = ComponentState::crashed;
        result.cause = "feed thread died";
        result.observed_ms = std::chrono::duration<double, std::milli>(current->crashed_for()).count();
    }
    return result;
}

RecoveryResult FeedComponent::recover(const std::string& cause) {
    static_cast<void>(cause);
    auto failed = producer();
    failed->wait();
    const auto resume = failed->resume_point();
    const auto last = failed->stats();

    auto next = config_;
    next.start_round = resume.next_round;
    next.start_prices = resume.prices;
    next.interval = last.interval;
    // The random walk continues from the same prices with a fresh stream of shocks.
    next.seed = config_.seed + 7'919U;
    auto fresh = build(next);
    if (last.paused) {
        fresh->set_paused(true);
    }
    {
        std::lock_guard lock(mutex_);
        config_ = next;
        carried_published_ += last.events_published;
        carried_rejections_ += last.subscriber_rejections;
        producer_ = fresh;
    }
    fresh->start();

    RecoveryResult result;
    result.detail = "resumed at round " + with_commas(resume.next_round + 1U) +
                    ": sequence numbers continue and prices carry over";
    result.recovered_records = resume.next_round;
    return result;
}

}  // namespace trading
