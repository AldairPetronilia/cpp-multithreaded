#include "trading/workers.hpp"

#include <algorithm>
#include <cmath>
#include <exception>
#include <iostream>
#include <numbers>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace trading {
namespace {

void report_failure(std::string_view component, const char* what) {
    std::cerr << component << " failed: " << what << '\n';
}

void publish_execution_report(EventBus& bus,
                              std::atomic<std::uint64_t>& publish_rejections,
                              const TradingSignalEvent& signal,
                              const std::string& order_id,
                              const ApiResponse& response) {
    const bool filled = response.ok();
    const auto result = bus.publish(ExecutionReportEvent{
        .strategy = signal.strategy,
        .signal_id = signal.signal_id,
        .client_order_id = order_id,
        .symbol = signal.symbol,
        .side = signal.side,
        .quantity = signal.quantity,
        .fill_price = filled ? signal.reference_price : 0.0,
        .filled = filled,
        .message = response.message.empty() ? std::string(to_string(response.error))
                                            : response.message,
        .market_data_time = signal.market_data_time,
        .execution_time = MonotonicClock::now(),
        .api_queue_delay = response.timing.queue_delay,
        .rate_limit_wait = response.timing.rate_limit_wait,
        .api_service_time = response.timing.service_time,
    });
    publish_rejections.fetch_add(result.rejected_full + result.rejected_closed,
                                 std::memory_order_relaxed);
}

}  // namespace

EventWorker::EventWorker(std::string name, EventBus& event_bus, WorkerOptions options, Handler handler)
    : name_(std::move(name)),
      event_bus_(event_bus),
      options_(options),
      mailbox_(std::make_shared<BoundedMailbox>(options.mailbox_capacity, options.overflow)),
      handler_(std::move(handler)) {
    if (!handler_) {
        throw std::invalid_argument("worker handler must not be empty");
    }
    if (options_.max_batch == 0U) {
        throw std::invalid_argument("worker batch size must be positive");
    }
}

EventWorker::~EventWorker() {
    stop();
    // An abandoned worker's thread is still owned here; its owner keeps the object alive
    // until the thread has finished, so this join does not wait.
    if (thread_.joinable()) {
        thread_.join();
    }
}

void EventWorker::ensure_not_started_locked() const {
    if (started_ || stopped_) {
        throw std::logic_error("subscriptions must be registered before worker " + name_ +
                               " starts");
    }
}

void EventWorker::set_start_hook(std::function<void()> hook) {
    std::lock_guard lock(lifecycle_mutex_);
    ensure_not_started_locked();
    start_hook_ = std::move(hook);
}

void EventWorker::start() {
    std::lock_guard lock(lifecycle_mutex_);
    if (started_) {
        return;
    }
    if (stopped_) {
        throw std::logic_error("a stopped worker cannot be restarted");
    }
    started_ = true;
    thread_ = std::thread([this] { run(); });
}

void EventWorker::stop() {
    std::thread thread;
    {
        std::lock_guard lock(lifecycle_mutex_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
        // Unsubscribing first means no publisher is delivering once close() runs, so the
        // thread drains a mailbox that can no longer grow.
        subscriptions_.clear();
        mailbox_->close();
        thread = std::move(thread_);
    }
    if (thread.joinable()) {
        thread.join();
    }
}

void EventWorker::abandon() {
    std::lock_guard lock(lifecycle_mutex_);
    if (stopped_) {
        return;
    }
    stopped_ = true;
    abandoned_.store(true, std::memory_order_release);
    subscriptions_.clear();
    mailbox_->close();
}

bool EventWorker::wait_ready(std::chrono::milliseconds timeout) {
    waiters_.fetch_add(1, std::memory_order_relaxed);
    std::unique_lock lock(wait_mutex_);
    const auto ready = wait_cv_.wait_for(lock, timeout, [this] {
        return ready_.load(std::memory_order_acquire) || finished_.load(std::memory_order_acquire);
    });
    waiters_.fetch_sub(1, std::memory_order_relaxed);
    return ready;
}

bool EventWorker::wait_until_drained(std::chrono::milliseconds timeout) {
    const auto drained = [this] {
        // Everything accepted is either handled or was evicted before it could be.
        const auto mailbox = mailbox_->stats();
        return processed_.load(std::memory_order_acquire) + mailbox.evicted_oldest >= mailbox.accepted ||
               finished_.load(std::memory_order_acquire);
    };
    const auto deadline = MonotonicClock::now() + timeout;
    waiters_.fetch_add(1, std::memory_order_relaxed);
    std::unique_lock lock(wait_mutex_);
    bool result = true;
    // Short slices make a missed notification cost microseconds, not the whole timeout.
    while (!drained()) {
        if (MonotonicClock::now() >= deadline) {
            result = false;
            break;
        }
        wait_cv_.wait_for(lock, std::chrono::milliseconds(2));
    }
    waiters_.fetch_sub(1, std::memory_order_relaxed);
    return result;
}

WorkerStats EventWorker::stats() const {
    return WorkerStats{
        name_,
        processed_.load(std::memory_order_relaxed),
        handler_errors_.load(std::memory_order_relaxed),
        std::chrono::nanoseconds(busy_ns_.load(std::memory_order_relaxed)),
        mailbox_->stats(),
        dispatch_latency_.snapshot(),
        consecutive_errors_.load(std::memory_order_relaxed),
        crashed_.load(std::memory_order_relaxed),
    };
}

WorkerHealth EventWorker::health() const {
    WorkerHealth health;
    health.crashed = crashed_.load(std::memory_order_acquire);
    health.finished = finished_.load(std::memory_order_acquire);
    health.abandoned = abandoned_.load(std::memory_order_acquire);
    health.ready = ready_.load(std::memory_order_acquire);
    health.processed = processed_.load(std::memory_order_relaxed);
    health.backlog = mailbox_->size();
    health.consecutive_errors = consecutive_errors_.load(std::memory_order_relaxed);
    const auto since = handler_since_ns_.load(std::memory_order_relaxed);
    if (since != 0) {
        health.handler_running_for =
            std::chrono::nanoseconds(MonotonicClock::now().time_since_epoch().count() - since);
    }
    std::lock_guard lock(error_mutex_);
    health.error = last_error_;
    return health;
}

void EventWorker::mark_ready() {
    ready_.store(true, std::memory_order_release);
    { std::lock_guard lock(wait_mutex_); }
    wait_cv_.notify_all();
}

void EventWorker::note_handler_error(const char* what) {
    const auto errors = handler_errors_.fetch_add(1, std::memory_order_relaxed) + 1U;
    consecutive_errors_.fetch_add(1, std::memory_order_relaxed);
    // A worker failing on every event would otherwise fill the console at event rate.
    if (errors <= 3U || errors % 1'000U == 0U) {
        report_failure(name_, (std::string(what) + " (" + std::to_string(errors) + " errors so far)").c_str());
    }
}

void EventWorker::record_crash(const char* what) {
    {
        std::lock_guard lock(error_mutex_);
        last_error_ = what;
    }
    handler_since_ns_.store(0, std::memory_order_relaxed);
    crashed_.store(true, std::memory_order_release);
    report_failure(name_, what);
}

void EventWorker::run() {
    // Nothing may escape the thread: an uncaught exception would terminate the process.
    // A worker that dies is left in place, marked crashed, for its supervisor to replace.
    try {
        if (start_hook_) {
            try {
                start_hook_();
            } catch (const std::exception& exception) {
                // Recovery is best effort; the worker still serves live traffic.
                handler_errors_.fetch_add(1, std::memory_order_relaxed);
                report_failure(name_, exception.what());
            }
        }
        mark_ready();
        run_loop();
    } catch (const std::exception& exception) {
        record_crash(exception.what());
    } catch (...) {
        record_crash("unknown exception");
    }
    finished_.store(true, std::memory_order_release);
    { std::lock_guard lock(wait_mutex_); }
    wait_cv_.notify_all();
}

void EventWorker::run_loop() {
    std::vector<EventEnvelope> batch;
    batch.reserve(options_.max_batch);
    while (mailbox_->wait_pop_batch(batch, options_.max_batch)) {
        auto handler_started = MonotonicClock::now();
        for (const auto& envelope : batch) {
            // An abandoned worker is a zombie: it must not touch state any more.
            if (abandoned_.load(std::memory_order_relaxed)) {
                break;
            }
            dispatch_latency_.record(handler_started - envelope.published_at);
            handler_since_ns_.store(handler_started.time_since_epoch().count(), std::memory_order_relaxed);
            try {
                handler_(envelope);
                if (consecutive_errors_.load(std::memory_order_relaxed) != 0U) {
                    consecutive_errors_.store(0, std::memory_order_relaxed);
                }
            } catch (const FatalWorkerError&) {
                throw;
            } catch (const std::exception& exception) {
                note_handler_error(exception.what());
            } catch (...) {
                note_handler_error("unknown exception");
            }
            // Per-event updates keep rates and utilisation smooth when handlers are slow.
            // This thread is the only writer, so the relaxed increments are uncontended.
            const auto handler_finished = MonotonicClock::now();
            busy_ns_.fetch_add((handler_finished - handler_started).count(), std::memory_order_relaxed);
            processed_.fetch_add(1, std::memory_order_relaxed);
            handler_started = handler_finished;
        }
        handler_since_ns_.store(0, std::memory_order_relaxed);
        // Release payloads now rather than when the next batch overwrites them.
        batch.clear();
        if (waiters_.load(std::memory_order_relaxed) > 0) {
            { std::lock_guard lock(wait_mutex_); }
            wait_cv_.notify_all();
        }
    }
}

MarketDataProducer::MarketDataProducer(EventBus& event_bus, MarketDataProducerConfig config)
    : event_bus_(event_bus),
      config_(std::move(config)),
      interval_us_(config_.interval.count()),
      next_round_(config_.start_round) {
    if (config_.instruments.empty() || config_.interval < std::chrono::microseconds::zero()) {
        throw std::invalid_argument("feed " + config_.name + " needs instruments and a non-negative interval");
    }
    if (!config_.start_prices.empty() && config_.start_prices.size() != config_.instruments.size()) {
        throw std::invalid_argument("feed " + config_.name + " resume prices must match its instruments");
    }
    std::set<std::string> seen;
    last_prices_ = std::make_unique<std::atomic<double>[]>(config_.instruments.size());
    for (std::size_t index = 0; index < config_.instruments.size(); ++index) {
        const auto& instrument = config_.instruments[index];
        if (instrument.symbol.empty() || instrument.reference_price <= 0.0 ||
            instrument.volatility_bps < 0.0 || !seen.insert(instrument.symbol).second) {
            throw std::invalid_argument("feed " + config_.name + " has an invalid or duplicate instrument");
        }
        topics_.push_back(event_bus_.topic(instrument.symbol));
        last_prices_[index].store(config_.start_prices.empty() ? instrument.reference_price
                                                               : config_.start_prices[index],
                                  std::memory_order_relaxed);
    }
}

MarketDataProducer::~MarketDataProducer() {
    stop();
    wait();
}

void MarketDataProducer::start() {
    if (thread_.joinable()) {
        throw std::logic_error("market data producer already started");
    }
    thread_ = std::jthread([this](std::stop_token token) { run(token); });
}

void MarketDataProducer::stop() {
    if (thread_.joinable()) {
        thread_.request_stop();
    }
}

void MarketDataProducer::wait() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

void MarketDataProducer::set_interval(std::chrono::microseconds interval) {
    if (interval < std::chrono::microseconds::zero()) {
        throw std::invalid_argument("feed interval must not be negative");
    }
    interval_us_.store(interval.count(), std::memory_order_relaxed);
}

void MarketDataProducer::set_paused(bool paused) {
    {
        std::lock_guard lock(control_mutex_);
        paused_ = paused;
    }
    control_wakeup_.notify_all();
}

ProducerStats MarketDataProducer::stats() const {
    ProducerStats stats;
    stats.name = config_.name;
    for (const auto& instrument : config_.instruments) {
        stats.symbols.push_back(instrument.symbol);
    }
    stats.events_published = events_published_.load(std::memory_order_relaxed);
    stats.subscriber_rejections = subscriber_rejections_.load(std::memory_order_relaxed);
    stats.failures = failures_.load(std::memory_order_relaxed);
    stats.interval = std::chrono::microseconds(interval_us_.load(std::memory_order_relaxed));
    stats.finished = finished_.load(std::memory_order_acquire);
    stats.crashed = crashed_.load(std::memory_order_acquire);
    std::lock_guard lock(control_mutex_);
    stats.paused = paused_;
    return stats;
}

FeedResumePoint MarketDataProducer::resume_point() const {
    FeedResumePoint point;
    point.next_round = next_round_.load(std::memory_order_acquire);
    for (std::size_t index = 0; index < config_.instruments.size(); ++index) {
        point.prices.push_back(last_prices_[index].load(std::memory_order_relaxed));
    }
    return point;
}

bool MarketDataProducer::wait_for_next_round(std::stop_token stop_token, TimePoint& next_round) {
    std::unique_lock lock(control_mutex_);
    for (;;) {
        if (stop_token.stop_requested()) {
            return false;
        }
        if (paused_) {
            control_wakeup_.wait(lock, stop_token, [this] { return !paused_; });
            next_round = MonotonicClock::now();
            continue;
        }
        if (next_round <= MonotonicClock::now()) {
            return true;
        }
        control_wakeup_.wait_until(lock, stop_token, next_round, [this] { return paused_; });
    }
}

void MarketDataProducer::run(std::stop_token stop_token) {
    // Mean-reverting random walk around a slowly oscillating centre: it produces trends and
    // reversals for the strategies to react to, and is deterministic for a given seed.
    constexpr double cycle_rounds = 6'000.0;
    constexpr double swing = 0.004;
    constexpr double reversion = 0.01;
    constexpr double phase_step = 1.7;

    try {
        std::mt19937_64 random(config_.seed);
        std::normal_distribution<double> shock(0.0, 1.0);
        std::vector<double> prices;
        for (std::size_t index = 0; index < config_.instruments.size(); ++index) {
            prices.push_back(config_.start_prices.empty() ? config_.instruments[index].reference_price
                                                          : config_.start_prices[index]);
        }

        auto next_round = MonotonicClock::now();
        for (std::uint64_t round = config_.start_round;
             config_.events_per_instrument == 0U || round < config_.events_per_instrument; ++round) {
            if (!wait_for_next_round(stop_token, next_round)) {
                break;
            }
            if (crash_requested_.exchange(false, std::memory_order_relaxed)) {
                throw std::runtime_error("injected feed failure");
            }
            for (std::size_t index = 0; index < prices.size(); ++index) {
                const auto& instrument = config_.instruments[index];
                const auto phase = 2.0 * std::numbers::pi * static_cast<double>(round) / cycle_rounds +
                                   phase_step * static_cast<double>(index);
                const auto centre = instrument.reference_price * (1.0 + swing * std::sin(phase));
                auto& price = prices[index];
                price += reversion * (centre - price) +
                         price * instrument.volatility_bps / 10'000.0 * shock(random);

                const auto result = event_bus_.publish(
                    MarketDataEvent{instrument.symbol, price, round + 1U, MonotonicClock::now()},
                    topics_[index]);
                events_published_.fetch_add(1, std::memory_order_relaxed);
                subscriber_rejections_.fetch_add(result.rejected_full + result.rejected_closed,
                                                 std::memory_order_relaxed);
                last_prices_[index].store(price, std::memory_order_relaxed);
            }
            next_round_.store(round + 1U, std::memory_order_release);

            // An absolute schedule keeps the rate independent of publish cost; after a stall
            // longer than one interval the schedule restarts instead of bursting to catch up.
            const auto interval = std::chrono::microseconds(interval_us_.load(std::memory_order_relaxed));
            const auto now = MonotonicClock::now();
            next_round = now - next_round > interval ? now + interval : next_round + interval;
        }
    } catch (const std::exception& exception) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        crashed_.store(true, std::memory_order_release);
        report_failure(config_.name, exception.what());
    } catch (...) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        crashed_.store(true, std::memory_order_release);
        report_failure(config_.name, "unknown exception");
    }
    finished_.store(true, std::memory_order_release);
}

SignalProcessor::SignalProcessor(EventBus& event_bus,
                                 SignalProcessorConfig config,
                                 std::unique_ptr<ISignalStrategy> strategy)
    : event_bus_(event_bus),
      config_(std::move(config)),
      strategy_(std::move(strategy)),
      kind_(strategy_ ? std::string(strategy_->kind()) : std::string()),
      worker_(config_.name, event_bus, config_.worker,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    if (!strategy_) {
        throw std::invalid_argument("signal processor " + config_.name + " needs a strategy");
    }
    if (config_.order_quantity <= 0.0) {
        throw std::invalid_argument("order quantity must be positive");
    }
    if (config_.symbols.empty()) {
        worker_.subscribe<MarketDataEvent>();
    }
    for (const auto& symbol : config_.symbols) {
        worker_.subscribe<MarketDataEvent>(event_bus_.topic(symbol));
    }
}

void SignalProcessor::set_fence(std::shared_ptr<GenerationGate> gate, std::uint64_t generation) {
    gate_ = std::move(gate);
    generation_ = generation;
}

void SignalProcessor::set_catch_up(CatchUpSource source) {
    catch_up_source_ = std::move(source);
    if (catch_up_source_) {
        worker_.set_start_hook([this] { catch_up(); });
    }
}

void SignalProcessor::start() {
    worker_.start();
}

void SignalProcessor::stop() {
    worker_.stop();
}

void SignalProcessor::abandon() {
    worker_.abandon();
}

SignalProcessorStats SignalProcessor::stats() const {
    return SignalProcessorStats{
        worker_.stats(),
        kind_,
        config_.symbols,
        signals_published_.load(std::memory_order_relaxed),
        publish_rejections_.load(std::memory_order_relaxed),
        tick_to_signal_.snapshot(),
        recovered_records_.load(std::memory_order_relaxed),
        dead_letters_.load(std::memory_order_relaxed),
        gate_ ? gate_->fenced_writes() : 0U,
        static_cast<double>(catch_up_ns_.load(std::memory_order_relaxed)) / 1e6,
    };
}

void SignalProcessor::catch_up() {
    const auto started = MonotonicClock::now();
    std::uint64_t delivered = 0;
    // The strategy sees the history exactly as the failed instance did, but its signals are
    // discarded: they were either already published or are too old to act on.
    catch_up_source_(config_.symbols, [this, &delivered](const MarketDataEvent& quote) {
        try {
            static_cast<void>(strategy_->on_market_data(quote));
        } catch (...) {
            // A quote that makes the strategy throw is dead-lettered, so a deterministic
            // poison event cannot send the replacement into a restart loop.
            dead_letters_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        auto& applied = applied_[quote.symbol];
        applied = std::max(applied, quote.sequence);
        ++delivered;
    });
    dedupe_active_ = !applied_.empty();
    recovered_records_.store(delivered, std::memory_order_relaxed);
    catch_up_ns_.store((MonotonicClock::now() - started).count(), std::memory_order_relaxed);
}

void SignalProcessor::on_event(const EventEnvelope& envelope) {
    const auto* quote = envelope.as<MarketDataEvent>();
    if (quote == nullptr) {
        return;
    }
    if (dedupe_active_) {
        const auto applied = applied_.find(quote->symbol);
        if (applied != applied_.end()) {
            if (quote->sequence <= applied->second) {
                return;
            }
            applied_.erase(applied);
            dedupe_active_ = !applied_.empty();
        }
    }
    const auto side = strategy_->on_market_data(*quote);
    if (!side) {
        return;
    }

    const auto now = MonotonicClock::now();
    tick_to_signal_.record(now - quote->source_time);
    TradingSignalEvent signal{
        .strategy = config_.name,
        .signal_id = quote->sequence,
        .symbol = quote->symbol,
        .side = *side,
        .quantity = config_.order_quantity,
        .reference_price = quote->price,
        .market_data_time = quote->source_time,
        .signal_time = now,
    };
    PublishResult result;
    const auto publish = [&] { result = event_bus_.publish(std::move(signal)); };
    if (gate_ != nullptr) {
        if (!gate_->run_if_current(generation_, publish)) {
            return;
        }
    } else {
        publish();
    }
    signals_published_.fetch_add(1, std::memory_order_relaxed);
    publish_rejections_.fetch_add(result.rejected_full + result.rejected_closed,
                                  std::memory_order_relaxed);
}

OrderExecutionWorker::OrderExecutionWorker(EventBus& event_bus,
                                           IRequestManager& request_manager,
                                           OrderExecutionConfig config)
    : event_bus_(event_bus),
      request_manager_(request_manager),
      config_(config),
      report_publish_rejections_(std::make_shared<std::atomic<std::uint64_t>>(0)),
      worker_("order-execution", event_bus, config.worker,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    if (config_.request_timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("order request timeout must be positive");
    }
    worker_.subscribe<TradingSignalEvent>();
}

void OrderExecutionWorker::set_fence(std::shared_ptr<GenerationGate> gate, std::uint64_t generation) {
    gate_ = std::move(gate);
    generation_ = generation;
}

void OrderExecutionWorker::set_recovery(std::function<void(OrderExecutionWorker&)> recovery) {
    recovery_ = std::move(recovery);
    if (recovery_) {
        worker_.set_start_hook([this] { recovery_(*this); });
    }
}

void OrderExecutionWorker::start() {
    worker_.start();
}

void OrderExecutionWorker::stop() {
    worker_.stop();
}

void OrderExecutionWorker::abandon() {
    worker_.abandon();
}

ExecutionStats OrderExecutionWorker::stats() const {
    return ExecutionStats{
        worker_.stats(),
        requests_submitted_.load(std::memory_order_relaxed),
        requests_rejected_.load(std::memory_order_relaxed),
        report_publish_rejections_->load(std::memory_order_relaxed),
        ExecutionRecoveryStats{
            recovery_examined_.load(std::memory_order_relaxed),
            recovery_resubmitted_.load(std::memory_order_relaxed),
            recovery_in_flight_.load(std::memory_order_relaxed),
            recovery_completed_.load(std::memory_order_relaxed),
            recovery_expired_.load(std::memory_order_relaxed),
        },
        gate_ ? gate_->fenced_writes() : 0U,
    };
}

std::string OrderExecutionWorker::order_id_for(const TradingSignalEvent& signal) const {
    // Order ids double as broker idempotency keys. The signal id is the triggering quote's
    // sequence, so the id is the same for a restarted or replayed strategy, and it is unique
    // across strategies and symbols. The session keeps one run's ids from meeting another's
    // inside the broker's deduplication window.
    std::string id;
    if (!config_.session.empty()) {
        id += config_.session;
        id += ':';
    }
    id += signal.strategy;
    id += ':';
    id += signal.symbol;
    id += ':';
    id += std::to_string(signal.signal_id);
    return id;
}

void OrderExecutionWorker::submit_order(const TradingSignalEvent& signal, const std::string& order_id) {
    std::ostringstream body;
    body << "symbol=" << signal.symbol << "&side=" << to_string(signal.side)
         << "&quantity=" << signal.quantity << "&price=" << signal.reference_price;

    ApiRequest request;
    request.endpoint = "/v1/orders";
    request.body = body.str();
    request.idempotency_key = order_id;
    request.idempotent = true;
    request.timeout = config_.request_timeout;

    const auto submit = [&] {
        const auto result = request_manager_.submit(
            std::move(request),
            [bus = &event_bus_, rejections = report_publish_rejections_, signal_copy = signal,
             order_id](ApiResponse response) {
                publish_execution_report(*bus, *rejections, signal_copy, order_id, response);
            });

        if (result.accepted()) {
            requests_submitted_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        ApiResponse rejection;
        rejection.error = ApiError::internal_error;
        rejection.message = result.status == SubmitStatus::queue_full ? "API request queue full"
                                                                      : "API request manager shutting down";
        publish_execution_report(event_bus_, *report_publish_rejections_, signal, order_id, rejection);
    };
    // A replaced worker must not place orders: the check and the submit are one step.
    if (gate_ != nullptr) {
        gate_->run_if_current(generation_, submit);
    } else {
        submit();
    }
}

void OrderExecutionWorker::on_event(const EventEnvelope& envelope) {
    const auto* signal = envelope.as<TradingSignalEvent>();
    if (signal == nullptr) {
        return;
    }
    if (fault_ != nullptr) {
        fault_->on_event();
    }
    const auto order_id = order_id_for(*signal);
    if (!recovered_ids_.empty() && recovered_ids_.contains(order_id)) {
        return;
    }
    submit_order(*signal, order_id);
}

void OrderExecutionWorker::reconcile(const TradingSignalEvent& signal, std::chrono::nanoseconds age) {
    const auto order_id = order_id_for(signal);
    if (!recovered_ids_.insert(order_id).second) {
        return;
    }
    recovery_examined_.fetch_add(1, std::memory_order_relaxed);

    // The journal alone cannot say whether the order reached the broker before the failure,
    // so ask before sending anything: a duplicate window is only as good as the venue's.
    const auto known = request_manager_.lookup(order_id);
    switch (known.state) {
        case OrderLookup::in_flight:
            // Still travelling through the gateway; its completion will publish the report.
            recovery_in_flight_.fetch_add(1, std::memory_order_relaxed);
            return;
        case OrderLookup::completed:
            recovery_completed_.fetch_add(1, std::memory_order_relaxed);
            publish_execution_report(event_bus_, *report_publish_rejections_, signal, order_id, known.response);
            return;
        case OrderLookup::unknown:
            break;
    }

    if (age > config_.stale_signal_after) {
        recovery_expired_.fetch_add(1, std::memory_order_relaxed);
        ApiResponse expired;
        expired.error = ApiError::cancelled;
        expired.message = "signal expired during recovery; not resubmitted";
        publish_execution_report(event_bus_, *report_publish_rejections_, signal, order_id, expired);
        return;
    }
    recovery_resubmitted_.fetch_add(1, std::memory_order_relaxed);
    submit_order(signal, order_id);
}

ApplyResult PositionBook::apply(const ExecutionReportEvent& execution) {
    if (!execution.filled) {
        return ApplyResult::not_filled;
    }
    const auto signed_quantity = execution.side == Side::buy ? execution.quantity : -execution.quantity;
    std::lock_guard lock(mutex_);
    if (!execution.client_order_id.empty()) {
        if (!applied_ids_.insert(execution.client_order_id).second) {
            ++duplicates_ignored_;
            return ApplyResult::duplicate;
        }
        applied_order_.push_back(execution.client_order_id);
        while (applied_order_.size() > kDedupeWindow) {
            applied_ids_.erase(applied_order_.front());
            applied_order_.pop_front();
        }
    }
    auto& entry = positions_[{execution.strategy, execution.symbol}];
    if (entry.fills == 0U) {
        entry.strategy = execution.strategy;
        entry.symbol = execution.symbol;
    }
    entry.quantity += signed_quantity;
    entry.cash -= signed_quantity * execution.fill_price;
    ++entry.fills;
    ++fills_applied_;
    return ApplyResult::applied;
}

std::vector<PositionEntry> PositionBook::snapshot() const {
    std::lock_guard lock(mutex_);
    std::vector<PositionEntry> entries;
    entries.reserve(positions_.size());
    for (const auto& [key, entry] : positions_) {
        static_cast<void>(key);
        entries.push_back(entry);
    }
    return entries;
}

double PositionBook::position(const std::string& strategy, const std::string& symbol) const {
    std::lock_guard lock(mutex_);
    const auto found = positions_.find({strategy, symbol});
    return found == positions_.end() ? 0.0 : found->second.quantity;
}

double PositionBook::net_position(const std::string& symbol) const {
    std::lock_guard lock(mutex_);
    double total = 0.0;
    for (const auto& [key, entry] : positions_) {
        if (key.second == symbol) {
            total += entry.quantity;
        }
    }
    return total;
}

std::uint64_t PositionBook::fills_applied() const {
    std::lock_guard lock(mutex_);
    return fills_applied_;
}

std::uint64_t PositionBook::duplicates_ignored() const {
    std::lock_guard lock(mutex_);
    return duplicates_ignored_;
}

PositionMonitor::PositionMonitor(EventBus& event_bus,
                                 PositionBook& position_book,
                                 PositionMonitorConfig config)
    : position_book_(position_book),
      worker_("position-monitor", event_bus, config.worker,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    worker_.subscribe<ExecutionReportEvent>();
}

void PositionMonitor::set_recovery(std::function<void()> recovery) {
    if (recovery) {
        worker_.set_start_hook(std::move(recovery));
    }
}

void PositionMonitor::start() {
    worker_.start();
}

void PositionMonitor::stop() {
    worker_.stop();
}

void PositionMonitor::abandon() {
    worker_.abandon();
}

PositionMonitorStats PositionMonitor::stats() const {
    return PositionMonitorStats{
        worker_.stats(),
        fills_.load(std::memory_order_relaxed),
        rejections_.load(std::memory_order_relaxed),
        duplicates_.load(std::memory_order_relaxed),
        end_to_end_.snapshot(),
        api_queue_.snapshot(),
        rate_limit_wait_.snapshot(),
        api_service_.snapshot(),
    };
}

void PositionMonitor::on_event(const EventEnvelope& envelope) {
    const auto* execution = envelope.as<ExecutionReportEvent>();
    if (execution == nullptr) {
        return;
    }
    if (fault_ != nullptr) {
        fault_->on_event();
    }
    if (position_book_.apply(*execution) == ApplyResult::duplicate) {
        duplicates_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    (execution->filled ? fills_ : rejections_).fetch_add(1, std::memory_order_relaxed);
    end_to_end_.record(MonotonicClock::now() - execution->market_data_time);
    // Requests rejected before reaching the API have no API timings worth recording.
    if (execution->api_service_time > std::chrono::nanoseconds::zero()) {
        api_queue_.record(execution->api_queue_delay);
        rate_limit_wait_.record(execution->rate_limit_wait);
        api_service_.record(execution->api_service_time);
    }
}

ActivityRecorder::ActivityRecorder(EventBus& event_bus, ActivityRecorderConfig config, TimePoint epoch)
    : config_(config),
      epoch_(epoch),
      worker_("activity-recorder", event_bus, config.worker,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    if (config_.sample_interval <= std::chrono::milliseconds::zero() ||
        config_.history_points == 0U || config_.tape_entries == 0U) {
        throw std::invalid_argument("invalid activity recorder configuration");
    }
    worker_.subscribe<MarketDataEvent>();
    worker_.subscribe<TradingSignalEvent>();
    worker_.subscribe<ExecutionReportEvent>();
    worker_.subscribe<SupervisionEvent>();
}

void ActivityRecorder::start() {
    worker_.start();
}

void ActivityRecorder::stop() {
    worker_.stop();
}

ActivitySnapshot ActivityRecorder::snapshot() const {
    ActivitySnapshot snapshot;
    snapshot.worker = worker_.stats();
    std::lock_guard lock(mutex_);
    for (const auto& [symbol, series] : series_) {
        snapshot.instruments.push_back(InstrumentActivity{
            symbol,
            series.last_price,
            series.updates,
            {series.history.begin(), series.history.end()},
        });
    }
    snapshot.tape.assign(tape_.begin(), tape_.end());
    return snapshot;
}

double ActivityRecorder::seconds_since_epoch(TimePoint time) const {
    return std::chrono::duration<double>(time - epoch_).count();
}

void ActivityRecorder::on_event(const EventEnvelope& envelope) {
    if (const auto* quote = envelope.as<MarketDataEvent>()) {
        on_market_data(*quote);
    } else if (const auto* signal = envelope.as<TradingSignalEvent>()) {
        append_tape(TapeEntry{
            TapeEntry::Kind::signal,
            seconds_since_epoch(signal->signal_time),
            signal->strategy,
            signal->symbol,
            signal->side,
            signal->quantity,
            signal->reference_price,
            std::chrono::duration<double, std::milli>(signal->signal_time - signal->market_data_time).count(),
            "signal #" + std::to_string(signal->signal_id),
        });
    } else if (const auto* execution = envelope.as<ExecutionReportEvent>()) {
        append_tape(TapeEntry{
            execution->filled ? TapeEntry::Kind::fill : TapeEntry::Kind::rejection,
            seconds_since_epoch(execution->execution_time),
            execution->strategy,
            execution->symbol,
            execution->side,
            execution->quantity,
            execution->fill_price,
            std::chrono::duration<double, std::milli>(execution->execution_time -
                                                      execution->market_data_time).count(),
            execution->client_order_id + ": " + execution->message,
        });
    } else if (const auto* supervision = envelope.as<SupervisionEvent>()) {
        append_tape(TapeEntry{
            TapeEntry::Kind::supervision,
            seconds_since_epoch(supervision->at),
            supervision->component,
            {},
            Side::buy,
            0.0,
            0.0,
            supervision->recovery_ms,
            supervision->action + ": " + supervision->detail,
        });
    }
}

void ActivityRecorder::on_market_data(const MarketDataEvent& quote) {
    const auto time_s = seconds_since_epoch(quote.source_time);
    const auto bucket = static_cast<std::int64_t>(
        std::floor(time_s * 1'000.0 / static_cast<double>(config_.sample_interval.count())));
    std::lock_guard lock(mutex_);
    auto& series = series_[quote.symbol];
    series.last_price = quote.price;
    ++series.updates;
    // One point per sample interval, holding the last price seen in that interval.
    if (bucket == series.last_bucket && !series.history.empty()) {
        series.history.back() = PricePoint{time_s, quote.price};
        return;
    }
    series.last_bucket = bucket;
    series.history.push_back(PricePoint{time_s, quote.price});
    if (series.history.size() > config_.history_points) {
        series.history.pop_front();
    }
}

void ActivityRecorder::append_tape(TapeEntry entry) {
    std::lock_guard lock(mutex_);
    tape_.push_back(std::move(entry));
    if (tape_.size() > config_.tape_entries) {
        tape_.pop_front();
    }
}

}  // namespace trading
