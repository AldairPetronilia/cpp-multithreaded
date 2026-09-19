#include "trading/workers.hpp"

#include <cmath>
#include <iostream>
#include <numbers>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace trading {

EventWorker::EventWorker(std::string name,
                         EventBus& event_bus,
                         std::size_t mailbox_capacity,
                         Handler handler)
    : name_(std::move(name)),
      event_bus_(event_bus),
      mailbox_(std::make_shared<BoundedMailbox>(mailbox_capacity)),
      handler_(std::move(handler)) {
    if (!handler_) {
        throw std::invalid_argument("worker handler must not be empty");
    }
}

EventWorker::~EventWorker() {
    stop();
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
        for (auto& subscription : subscriptions_) {
            subscription.reset();
        }
        subscriptions_.clear();
        mailbox_->close();
        thread = std::move(thread_);
    }
    if (thread.joinable()) {
        thread.join();
    }
}

WorkerStats EventWorker::stats() const {
    return WorkerStats{
        processed_.load(std::memory_order_relaxed),
        handler_errors_.load(std::memory_order_relaxed),
        mailbox_->size(),
        mailbox_->max_depth(),
        mailbox_->rejected_full(),
        dispatch_latency_.summarize(),
    };
}

const std::string& EventWorker::name() const noexcept {
    return name_;
}

void EventWorker::run() {
    EventEnvelope envelope;
    while (mailbox_->wait_pop(envelope)) {
        dispatch_latency_.record(MonotonicClock::now() - envelope.published_at);
        try {
            handler_(envelope);
        } catch (const std::exception& exception) {
            handler_errors_.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "worker " << name_ << " handler failed: " << exception.what()
                      << '\n';
        } catch (...) {
            handler_errors_.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "worker " << name_ << " handler failed with an unknown exception\n";
        }
        processed_.fetch_add(1, std::memory_order_relaxed);
    }
}

MarketDataProducer::MarketDataProducer(EventBus& event_bus,
                                       MarketDataProducerConfig config)
    : event_bus_(event_bus), config_(std::move(config)) {
    if (config_.symbol.empty() || config_.event_count == 0U || config_.amplitude < 0.0 ||
        config_.interval < std::chrono::microseconds::zero()) {
        throw std::invalid_argument("invalid market data producer configuration");
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
        interval_wakeup_.notify_all();
    }
}

void MarketDataProducer::wait() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

ProducerStats MarketDataProducer::stats() const noexcept {
    return ProducerStats{
        events_published_.load(std::memory_order_relaxed),
        subscriber_rejections_.load(std::memory_order_relaxed),
        failures_.load(std::memory_order_relaxed),
    };
}

void MarketDataProducer::run(std::stop_token stop_token) {
    try {
        constexpr double phase_increment = 0.12;
        for (std::size_t index = 0;
             index < config_.event_count && !stop_token.stop_requested(); ++index) {
            const auto phase = static_cast<double>(index) * phase_increment -
                               std::numbers::pi_v<double> / 2.0;
            const auto price = config_.midpoint + config_.amplitude * std::sin(phase);
            const auto result = event_bus_.publish(MarketDataEvent{
                config_.symbol,
                price,
                static_cast<std::uint64_t>(index + 1U),
                MonotonicClock::now(),
            });
            events_published_.fetch_add(1, std::memory_order_relaxed);
            subscriber_rejections_.fetch_add(result.rejected_full + result.rejected_closed,
                                             std::memory_order_relaxed);

            if (config_.interval > std::chrono::microseconds::zero()) {
                std::unique_lock lock(interval_mutex_);
                interval_wakeup_.wait_for(
                    lock, stop_token, config_.interval, [] { return false; });
            }
        }
    } catch (const std::exception& exception) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        std::cerr << "market-data producer failed: " << exception.what() << '\n';
    } catch (...) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        std::cerr << "market-data producer failed with an unknown exception\n";
    }
}

SignalProcessor::SignalProcessor(EventBus& event_bus, SignalProcessorConfig config)
    : event_bus_(event_bus),
      config_(config),
      worker_("signal-processor",
              event_bus,
              config.mailbox_capacity,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    if (config_.order_quantity <= 0.0) {
        throw std::invalid_argument("order quantity must be positive");
    }
    worker_.subscribe<MarketDataEvent>();
}

void SignalProcessor::start() {
    worker_.start();
}

void SignalProcessor::stop() {
    worker_.stop();
}

WorkerStats SignalProcessor::stats() const {
    return worker_.stats();
}

std::uint64_t SignalProcessor::signals_published() const noexcept {
    return signals_published_.load(std::memory_order_relaxed);
}

std::uint64_t SignalProcessor::publish_rejections() const noexcept {
    return publish_rejections_.load(std::memory_order_relaxed);
}

void SignalProcessor::on_event(const EventEnvelope& envelope) {
    const auto market_data = envelope.as<MarketDataEvent>();
    if (!market_data) {
        return;
    }

    const auto previous = previous_prices_.find(market_data->symbol);
    if (previous == previous_prices_.end()) {
        previous_prices_.emplace(market_data->symbol, market_data->price);
        return;
    }

    const bool crossed_up = previous->second < config_.threshold &&
                            market_data->price >= config_.threshold;
    const bool crossed_down = previous->second >= config_.threshold &&
                              market_data->price < config_.threshold;
    previous->second = market_data->price;
    if (!crossed_up && !crossed_down) {
        return;
    }

    const auto result = event_bus_.publish(TradingSignalEvent{
        next_signal_id_++,
        market_data->symbol,
        crossed_up ? Side::buy : Side::sell,
        config_.order_quantity,
        market_data->price,
        market_data->source_time,
        MonotonicClock::now(),
    });
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
      completion_publish_rejections_(
          std::make_shared<std::atomic<std::uint64_t>>(0)),
      worker_("order-execution",
              event_bus,
              config.mailbox_capacity,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    if (config_.request_timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("order request timeout must be positive");
    }
    worker_.subscribe<TradingSignalEvent>();
}

void OrderExecutionWorker::start() {
    worker_.start();
}

void OrderExecutionWorker::stop() {
    worker_.stop();
}

WorkerStats OrderExecutionWorker::stats() const {
    return worker_.stats();
}

std::uint64_t OrderExecutionWorker::requests_submitted() const noexcept {
    return requests_submitted_.load(std::memory_order_relaxed);
}

std::uint64_t OrderExecutionWorker::requests_rejected() const noexcept {
    return requests_rejected_.load(std::memory_order_relaxed);
}

std::uint64_t OrderExecutionWorker::completion_publish_rejections() const noexcept {
    return completion_publish_rejections_->load(std::memory_order_relaxed);
}

void OrderExecutionWorker::on_event(const EventEnvelope& envelope) {
    const auto signal = envelope.as<TradingSignalEvent>();
    if (!signal) {
        return;
    }

    const auto order_id = "order-" + std::to_string(signal->signal_id);
    std::ostringstream body;
    body << "symbol=" << signal->symbol << "&side=" << to_string(signal->side)
         << "&quantity=" << signal->quantity << "&price=" << signal->reference_price;

    ApiRequest request;
    request.endpoint = "/v1/orders";
    request.body = body.str();
    request.idempotency_key = order_id;
    request.idempotent = true;
    request.timeout = config_.request_timeout;

    auto* const bus = &event_bus_;
    const auto publish_rejections = completion_publish_rejections_;
    const auto signal_copy = *signal;
    const auto result = request_manager_.submit(
        std::move(request),
        [bus, publish_rejections, signal_copy, order_id](ApiResponse response) {
            const bool filled = response.ok();
            const auto publish_result = bus->publish(ExecutionReportEvent{
                signal_copy.signal_id,
                order_id,
                signal_copy.symbol,
                signal_copy.side,
                signal_copy.quantity,
                filled ? signal_copy.reference_price : 0.0,
                filled,
                response.message.empty() ? to_string(response.error) : response.message,
                signal_copy.market_data_time,
                MonotonicClock::now(),
                response.timing.queue_delay,
                response.timing.rate_limit_wait,
                response.timing.service_time,
            });
            publish_rejections->fetch_add(
                publish_result.rejected_full + publish_result.rejected_closed,
                std::memory_order_relaxed);
        });

    if (result.accepted()) {
        requests_submitted_.fetch_add(1, std::memory_order_relaxed);
    } else {
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        publish_submission_failure(*signal, order_id, result.status);
    }
}

void OrderExecutionWorker::publish_submission_failure(const TradingSignalEvent& signal,
                                                       const std::string& order_id,
                                                       SubmitStatus status) {
    const auto message = status == SubmitStatus::queue_full
                             ? "API request queue full"
                             : "API request manager shutting down";
    const auto result = event_bus_.publish(ExecutionReportEvent{
        signal.signal_id,
        order_id,
        signal.symbol,
        signal.side,
        signal.quantity,
        0.0,
        false,
        message,
        signal.market_data_time,
        MonotonicClock::now(),
    });
    completion_publish_rejections_->fetch_add(
        result.rejected_full + result.rejected_closed, std::memory_order_relaxed);
}

void PositionBook::apply(const ExecutionReportEvent& execution) {
    if (!execution.filled) {
        return;
    }
    std::lock_guard lock(mutex_);
    const auto signed_quantity =
        execution.side == Side::buy ? execution.quantity : -execution.quantity;
    positions_[execution.symbol] += signed_quantity;
    ++fills_applied_;
}

std::map<std::string, double> PositionBook::snapshot() const {
    std::lock_guard lock(mutex_);
    return positions_;
}

double PositionBook::position(const std::string& symbol) const {
    std::lock_guard lock(mutex_);
    const auto found = positions_.find(symbol);
    return found == positions_.end() ? 0.0 : found->second;
}

std::uint64_t PositionBook::fills_applied() const {
    std::lock_guard lock(mutex_);
    return fills_applied_;
}

PositionMonitor::PositionMonitor(EventBus& event_bus,
                                 PositionBook& position_book,
                                 PositionMonitorConfig config)
    : position_book_(position_book),
      worker_("position-monitor",
              event_bus,
              config.mailbox_capacity,
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    worker_.subscribe<ExecutionReportEvent>();
}

void PositionMonitor::start() {
    worker_.start();
}

void PositionMonitor::stop() {
    worker_.stop();
}

WorkerStats PositionMonitor::stats() const {
    return worker_.stats();
}

LatencySummary PositionMonitor::end_to_end_latency() const {
    return end_to_end_latency_.summarize();
}

LatencySummary PositionMonitor::api_queue_latency() const {
    return api_queue_latency_.summarize();
}

LatencySummary PositionMonitor::rate_limit_wait() const {
    return rate_limit_wait_.summarize();
}

void PositionMonitor::on_event(const EventEnvelope& envelope) {
    const auto execution = envelope.as<ExecutionReportEvent>();
    if (!execution) {
        return;
    }
    position_book_.apply(*execution);
    end_to_end_latency_.record(MonotonicClock::now() - execution->market_data_time);
    api_queue_latency_.record(execution->api_queue_delay);
    rate_limit_wait_.record(execution->rate_limit_wait);
}

}  // namespace trading
