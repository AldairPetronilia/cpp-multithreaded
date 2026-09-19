#pragma once

#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/metrics.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace trading {

struct WorkerStats {
    std::uint64_t processed{};
    std::uint64_t handler_errors{};
    std::size_t queue_depth{};
    std::size_t max_queue_depth{};
    std::uint64_t rejected_full{};
    LatencySummary dispatch_latency;
};

class EventWorker {
public:
    using Handler = std::function<void(const EventEnvelope&)>;

    EventWorker(std::string name,
                EventBus& event_bus,
                std::size_t mailbox_capacity,
                Handler handler);
    ~EventWorker();
    EventWorker(const EventWorker&) = delete;
    EventWorker& operator=(const EventWorker&) = delete;

    template <typename EventType>
    void subscribe() {
        std::lock_guard lock(lifecycle_mutex_);
        if (started_) {
            throw std::logic_error("subscriptions must be registered before worker start");
        }
        subscriptions_.push_back(event_bus_.subscribe<EventType>(mailbox_));
    }

    void start();
    void stop();
    [[nodiscard]] WorkerStats stats() const;
    [[nodiscard]] const std::string& name() const noexcept;

private:
    void run();

    std::string name_;
    EventBus& event_bus_;
    std::shared_ptr<BoundedMailbox> mailbox_;
    Handler handler_;
    std::vector<EventBus::Subscription> subscriptions_;
    mutable std::mutex lifecycle_mutex_;
    std::thread thread_;
    bool started_{false};
    bool stopped_{false};
    std::atomic<std::uint64_t> processed_{0};
    std::atomic<std::uint64_t> handler_errors_{0};
    LatencyRecorder dispatch_latency_;
};

struct MarketDataProducerConfig {
    std::string symbol{"DEMO"};
    double midpoint{100.0};
    double amplitude{2.0};
    std::size_t event_count{500};
    std::chrono::microseconds interval{1'000};
};

struct ProducerStats {
    std::uint64_t events_published{};
    std::uint64_t subscriber_rejections{};
    std::uint64_t failures{};
};

class MarketDataProducer {
public:
    MarketDataProducer(EventBus& event_bus, MarketDataProducerConfig config);
    ~MarketDataProducer();
    MarketDataProducer(const MarketDataProducer&) = delete;
    MarketDataProducer& operator=(const MarketDataProducer&) = delete;

    void start();
    void stop();
    void wait();
    [[nodiscard]] ProducerStats stats() const noexcept;

private:
    void run(std::stop_token stop_token);

    EventBus& event_bus_;
    MarketDataProducerConfig config_;
    std::jthread thread_;
    std::condition_variable_any interval_wakeup_;
    std::mutex interval_mutex_;
    std::atomic<std::uint64_t> events_published_{0};
    std::atomic<std::uint64_t> subscriber_rejections_{0};
    std::atomic<std::uint64_t> failures_{0};
};

struct SignalProcessorConfig {
    double threshold{100.0};
    double order_quantity{1.0};
    std::size_t mailbox_capacity{1'024};
};

class SignalProcessor {
public:
    SignalProcessor(EventBus& event_bus, SignalProcessorConfig config);

    void start();
    void stop();
    [[nodiscard]] WorkerStats stats() const;
    [[nodiscard]] std::uint64_t signals_published() const noexcept;
    [[nodiscard]] std::uint64_t publish_rejections() const noexcept;

private:
    void on_event(const EventEnvelope& envelope);

    EventBus& event_bus_;
    SignalProcessorConfig config_;
    std::unordered_map<std::string, double> previous_prices_;
    std::uint64_t next_signal_id_{1};
    std::atomic<std::uint64_t> signals_published_{0};
    std::atomic<std::uint64_t> publish_rejections_{0};
    EventWorker worker_;
};

struct OrderExecutionConfig {
    std::chrono::milliseconds request_timeout{500};
    std::size_t mailbox_capacity{1'024};
};

class OrderExecutionWorker {
public:
    OrderExecutionWorker(EventBus& event_bus,
                         IRequestManager& request_manager,
                         OrderExecutionConfig config);

    void start();
    void stop();
    [[nodiscard]] WorkerStats stats() const;
    [[nodiscard]] std::uint64_t requests_submitted() const noexcept;
    [[nodiscard]] std::uint64_t requests_rejected() const noexcept;
    [[nodiscard]] std::uint64_t completion_publish_rejections() const noexcept;

private:
    void on_event(const EventEnvelope& envelope);
    void publish_submission_failure(const TradingSignalEvent& signal,
                                    const std::string& order_id,
                                    SubmitStatus status);

    EventBus& event_bus_;
    IRequestManager& request_manager_;
    OrderExecutionConfig config_;
    std::atomic<std::uint64_t> requests_submitted_{0};
    std::atomic<std::uint64_t> requests_rejected_{0};
    std::shared_ptr<std::atomic<std::uint64_t>> completion_publish_rejections_;
    EventWorker worker_;
};

class PositionBook {
public:
    void apply(const ExecutionReportEvent& execution);
    [[nodiscard]] std::map<std::string, double> snapshot() const;
    [[nodiscard]] double position(const std::string& symbol) const;
    [[nodiscard]] std::uint64_t fills_applied() const;

private:
    mutable std::mutex mutex_;
    std::map<std::string, double> positions_;
    std::uint64_t fills_applied_{};
};

struct PositionMonitorConfig {
    std::size_t mailbox_capacity{1'024};
};

class PositionMonitor {
public:
    PositionMonitor(EventBus& event_bus,
                    PositionBook& position_book,
                    PositionMonitorConfig config);

    void start();
    void stop();
    [[nodiscard]] WorkerStats stats() const;
    [[nodiscard]] LatencySummary end_to_end_latency() const;
    [[nodiscard]] LatencySummary api_queue_latency() const;
    [[nodiscard]] LatencySummary rate_limit_wait() const;

private:
    void on_event(const EventEnvelope& envelope);

    PositionBook& position_book_;
    LatencyRecorder end_to_end_latency_;
    LatencyRecorder api_queue_latency_;
    LatencyRecorder rate_limit_wait_;
    EventWorker worker_;
};

}  // namespace trading
