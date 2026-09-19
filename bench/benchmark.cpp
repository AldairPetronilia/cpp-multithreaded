#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/metrics.hpp"
#include "trading/workers.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

std::size_t parse_size(std::string_view value, std::string_view name) {
    std::size_t parsed{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() || parsed == 0U) {
        throw std::invalid_argument("invalid " + std::string(name));
    }
    return parsed;
}

void print_latency(std::string_view name, const trading::LatencySummary& summary) {
    std::cout << "    " << std::left << std::setw(26) << name << std::right << std::fixed << std::setprecision(2)
              << " n=" << summary.count << " mean=" << summary.mean_us << "us p50=" << summary.p50_us
              << "us p99=" << summary.p99_us << "us max=" << summary.max_us << "us\n";
}

// Counts handled events and lets the benchmark wait for a target without polling.
class Consumed {
public:
    void add(std::size_t count = 1) {
        {
            std::lock_guard lock(mutex_);
            value_ += count;
        }
        wakeup_.notify_all();
    }

    void wait_for(std::size_t target) {
        std::unique_lock lock(mutex_);
        if (!wakeup_.wait_for(lock, 60s, [&] { return value_ >= target; })) {
            throw std::runtime_error("timed out waiting for consumers");
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable wakeup_;
    std::size_t value_{0};
};

struct Consumer {
    std::unique_ptr<trading::EventWorker> worker;
    std::unique_ptr<trading::LatencyHistogram> latency;
};

// Workers that record publish-to-handler latency; notifying on every event would itself
// dominate the measurement, so completion is signalled once per consumer.
std::vector<Consumer> make_consumers(trading::EventBus& bus,
                                     std::size_t count,
                                     std::size_t capacity,
                                     std::size_t expected_each,
                                     Consumed& consumed,
                                     const std::vector<trading::Topic>& topics = {}) {
    std::vector<Consumer> consumers;
    for (std::size_t index = 0; index < count; ++index) {
        auto latency = std::make_unique<trading::LatencyHistogram>();
        auto seen = std::make_shared<std::size_t>(0);
        auto worker = std::make_unique<trading::EventWorker>(
            "consumer-" + std::to_string(index), bus, trading::WorkerOptions{capacity},
            [&consumed, histogram = latency.get(), seen, expected_each](const trading::EventEnvelope& envelope) {
                histogram->record(trading::MonotonicClock::now() - envelope.published_at);
                if (++*seen == expected_each) {
                    consumed.add();
                }
            });
        if (topics.empty()) {
            worker->subscribe<trading::BenchmarkEvent>();
        } else {
            worker->subscribe<trading::BenchmarkEvent>(topics[index]);
        }
        worker->start();
        consumers.push_back(Consumer{std::move(worker), std::move(latency)});
    }
    return consumers;
}

double seconds(trading::MonotonicClock::duration duration) {
    return std::chrono::duration<double>(duration).count();
}

// Saturated throughput: publishers go as fast as they can into one subscriber.
void run_fan_in(std::size_t event_count, std::size_t publisher_count) {
    trading::EventBus bus;
    Consumed consumed;
    auto consumers = make_consumers(bus, 1, event_count, event_count, consumed);
    std::atomic<std::size_t> rejected{0};

    const auto started = trading::MonotonicClock::now();
    std::vector<std::thread> publishers;
    for (std::size_t publisher = 0; publisher < publisher_count; ++publisher) {
        publishers.emplace_back([&, publisher] {
            const auto first = event_count * publisher / publisher_count;
            const auto last = event_count * (publisher + 1U) / publisher_count;
            for (auto sequence = first; sequence < last; ++sequence) {
                if (!bus.publish(trading::BenchmarkEvent{publisher, sequence}).all_delivered()) {
                    rejected.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& publisher : publishers) {
        publisher.join();
    }
    consumed.wait_for(1);
    const auto elapsed = seconds(trading::MonotonicClock::now() - started);
    if (rejected.load() != 0U) {
        throw std::runtime_error("fan-in benchmark mailbox overflowed");
    }
    const auto stats = consumers.front().worker->stats();
    std::cout << "  fan-in  " << publisher_count << " publishers -> 1 subscriber: " << std::fixed
              << std::setprecision(0) << static_cast<double>(event_count) / elapsed
              << " events/s, max queue " << stats.mailbox.max_depth << '\n';
    print_latency("publish-to-handler", consumers.front().latency->summary());
}

// One publisher delivering to `subscriber_count` workers, either all on one topic (fan-out)
// or each on its own topic (the per-strategy market-data layout).
void run_scaling(std::size_t event_count, std::size_t subscriber_count, bool partition_by_topic) {
    trading::EventBus bus;
    std::vector<trading::Topic> topics;
    if (partition_by_topic) {
        for (std::size_t index = 0; index < subscriber_count; ++index) {
            topics.push_back(bus.topic("SYM" + std::to_string(index)));
        }
    }
    const auto expected_each = partition_by_topic ? event_count / subscriber_count : event_count;
    Consumed consumed;
    auto consumers = make_consumers(bus, subscriber_count, expected_each, expected_each, consumed, topics);

    const auto started = trading::MonotonicClock::now();
    for (std::size_t sequence = 0; sequence < expected_each * (partition_by_topic ? subscriber_count : 1U);
         ++sequence) {
        const auto topic = partition_by_topic ? topics[sequence % subscriber_count] : trading::Topic{};
        if (!bus.publish(trading::BenchmarkEvent{0, sequence}, topic).all_delivered()) {
            throw std::runtime_error("scaling benchmark mailbox overflowed");
        }
    }
    const auto publish_elapsed = seconds(trading::MonotonicClock::now() - started);
    consumed.wait_for(subscriber_count);
    const auto elapsed = seconds(trading::MonotonicClock::now() - started);
    const auto published = expected_each * (partition_by_topic ? subscriber_count : 1U);
    const auto deliveries = expected_each * subscriber_count;

    std::uint64_t worst_p99 = 0;
    for (const auto& consumer : consumers) {
        worst_p99 = std::max<std::uint64_t>(worst_p99, static_cast<std::uint64_t>(consumer.latency->summary().p99_us));
    }
    std::cout << "  " << (partition_by_topic ? "topics " : "fan-out") << ' ' << std::setw(2) << subscriber_count
              << " subscriber(s): " << std::fixed << std::setprecision(0)
              << static_cast<double>(deliveries) / elapsed << " deliveries/s, "
              << std::setprecision(1) << publish_elapsed * 1e9 / static_cast<double>(published)
              << " ns per publish, worst consumer p99 " << worst_p99 << "us\n";
}

// Latency at a sustainable rate: the consumer is idle between events, so this includes the
// cost of waking a blocked thread, unlike the saturated runs above.
void run_paced_latency(std::size_t rate_per_second, std::chrono::milliseconds duration) {
    trading::EventBus bus;
    const auto total = static_cast<std::size_t>(rate_per_second * static_cast<std::size_t>(duration.count()) / 1'000U);
    Consumed consumed;
    auto consumers = make_consumers(bus, 1, 4'096, total, consumed);

    const auto interval = std::chrono::nanoseconds(1'000'000'000LL / static_cast<long long>(rate_per_second));
    auto next = trading::MonotonicClock::now();
    for (std::size_t sequence = 0; sequence < total; ++sequence) {
        // The benchmark paces itself by spinning: sleeping is far coarser than the interval.
        while (trading::MonotonicClock::now() < next) {
            std::this_thread::yield();
        }
        next += interval;
        bus.publish(trading::BenchmarkEvent{0, sequence});
    }
    consumed.wait_for(1);
    std::cout << "  paced " << rate_per_second << " events/s for " << duration.count() << " ms:\n";
    print_latency("publish-to-handler", consumers.front().latency->summary());
}

void run_api_benchmark(std::size_t request_count) {
    auto client = std::make_shared<trading::MockTradingApi>(trading::MockApiConfig{1ms});
    trading::ApiManagerConfig config;
    config.rate_limit = {50, 50ms};
    config.io_threads = 4;
    config.max_outstanding_requests = request_count;
    config.max_retries = 0;
    trading::ApiRequestManager manager(client, config);

    trading::LatencyHistogram queue_delay;
    trading::LatencyHistogram limiter_wait;
    trading::LatencyHistogram service_time;
    trading::LatencyHistogram total_time;
    Consumed completed;

    const auto started = trading::MonotonicClock::now();
    for (std::size_t index = 0; index < request_count; ++index) {
        trading::ApiRequest request;
        request.endpoint = "/benchmark";
        request.idempotent = true;
        request.idempotency_key = "benchmark-" + std::to_string(index);
        const auto result = manager.submit(std::move(request), [&](trading::ApiResponse response) {
            queue_delay.record(response.timing.queue_delay);
            limiter_wait.record(response.timing.rate_limit_wait);
            service_time.record(response.timing.service_time);
            total_time.record(response.timing.total_time);
            completed.add();
        });
        if (!result.accepted()) {
            throw std::runtime_error("API benchmark queue unexpectedly rejected a request");
        }
    }
    completed.wait_for(request_count);
    manager.shutdown(trading::ApiShutdownMode::drain);
    const auto elapsed = seconds(trading::MonotonicClock::now() - started);

    std::cout << "  " << request_count << " requests, 4 I/O threads, mock latency 1 ms, limit 50/50 ms: "
              << std::fixed << std::setprecision(0) << static_cast<double>(request_count) / elapsed
              << " requests/s (window: 50 at once, then 50 per 50 ms)\n";
    print_latency("queue delay", queue_delay.summary());
    print_latency("rate-limit wait", limiter_wait.summary());
    print_latency("mock service", service_time.summary());
    print_latency("submission-to-completion", total_time.summary());
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const auto event_count = argc > 1 ? parse_size(argv[1], "event count") : 200'000U;
        const auto publisher_count = argc > 2 ? parse_size(argv[2], "publisher count") : 4U;
        const auto request_count = argc > 3 ? parse_size(argv[3], "request count") : 200U;
        std::cout << "Internal messaging (histogram percentiles are within ~3%):\n";
        run_fan_in(event_count, 1);
        run_fan_in(event_count, publisher_count);
        for (const std::size_t subscribers : {1U, 2U, 4U, 8U}) {
            run_scaling(event_count, subscribers, false);
        }
        for (const std::size_t subscribers : {2U, 4U, 8U}) {
            run_scaling(event_count, subscribers, true);
        }
        run_paced_latency(20'000, 1'000ms);
        std::cout << "Shared API manager (separate from messaging):\n";
        run_api_benchmark(request_count);
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "benchmark failed: " << exception.what() << '\n';
        std::cerr << "Usage: trading_benchmark [event-count] [publisher-count] [request-count]\n";
        return 1;
    }
}
