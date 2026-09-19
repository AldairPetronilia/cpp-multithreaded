#include "trading/api.hpp"
#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/metrics.hpp"
#include "trading/workers.hpp"

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
#include <string_view>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

std::size_t parse_size(std::string_view value, std::string_view name) {
    std::size_t parsed{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() || parsed == 0U) {
        throw std::invalid_argument("invalid " + std::string(name));
    }
    return parsed;
}

void print_summary(std::string_view name, const trading::LatencySummary& summary) {
    std::cout << "  " << name << ": count=" << summary.count << std::fixed
              << std::setprecision(2) << " mean=" << summary.mean_us
              << "us p50=" << summary.p50_us << "us p95=" << summary.p95_us
              << "us p99=" << summary.p99_us << "us max=" << summary.maximum_us
              << "us\n";
}

void run_event_benchmark(std::size_t event_count, std::size_t producer_count) {
    trading::EventBus bus;
    trading::LatencyRecorder latency;
    std::atomic<std::size_t> consumed{0};
    std::mutex completion_mutex;
    std::condition_variable completion;

    trading::EventWorker consumer(
        "benchmark-consumer", bus, event_count,
        [&](const trading::EventEnvelope& envelope) {
            latency.record(trading::MonotonicClock::now() - envelope.published_at);
            if (consumed.fetch_add(1, std::memory_order_acq_rel) + 1U == event_count) {
                completion.notify_one();
            }
        });
    consumer.subscribe<trading::BenchmarkEvent>();
    consumer.start();

    const auto started = trading::MonotonicClock::now();
    std::atomic<std::size_t> rejected{0};
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        const auto first = event_count * producer / producer_count;
        const auto last = event_count * (producer + 1U) / producer_count;
        producers.emplace_back([&bus, &rejected, producer, first, last] {
            for (auto sequence = first; sequence < last; ++sequence) {
                const auto result = bus.publish(trading::BenchmarkEvent{producer, sequence});
                if (!result.all_delivered()) {
                    rejected.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    if (rejected.load(std::memory_order_relaxed) != 0U) {
        throw std::runtime_error("benchmark mailbox unexpectedly overflowed");
    }
    {
        std::unique_lock lock(completion_mutex);
        if (!completion.wait_for(lock, 30s, [&] {
                return consumed.load(std::memory_order_acquire) == event_count;
            })) {
            throw std::runtime_error("timed out waiting for benchmark consumer");
        }
    }
    const auto elapsed = trading::MonotonicClock::now() - started;
    consumer.stop();

    const auto seconds = std::chrono::duration<double>(elapsed).count();
    const auto stats = consumer.stats();
    std::cout << "Internal event bus (one subscriber, " << producer_count
              << " concurrent publisher(s)):\n";
    std::cout << "  events=" << event_count << " elapsed=" << std::fixed
              << std::setprecision(4) << seconds << "s throughput="
              << static_cast<double>(event_count) / seconds << " events/s"
              << " max_queue=" << stats.max_queue_depth << '\n';
    print_summary("publish-to-handler", latency.summarize());
}

void run_api_benchmark(std::size_t request_count) {
    auto client = std::make_shared<trading::MockTradingApi>(
        trading::MockApiConfig{1ms, 0, 0ms, 0});
    trading::ApiManagerConfig config;
    config.rate_limit = {50, 50ms};
    config.io_threads = 4;
    config.max_outstanding_requests = request_count;
    config.max_retries = 0;
    trading::ApiRequestManager manager(client, config);

    trading::LatencyRecorder queue_delay;
    trading::LatencyRecorder limiter_wait;
    trading::LatencyRecorder service_time;
    trading::LatencyRecorder total_time;
    std::atomic<std::size_t> completed{0};
    std::mutex completion_mutex;
    std::condition_variable completion;

    const auto started = trading::MonotonicClock::now();
    for (std::size_t index = 0; index < request_count; ++index) {
        trading::ApiRequest request;
        request.endpoint = "/benchmark";
        request.idempotent = true;
        request.idempotency_key = "benchmark-" + std::to_string(index);
        const auto result = manager.submit(
            std::move(request), [&](trading::ApiResponse response) {
                queue_delay.record(response.timing.queue_delay);
                limiter_wait.record(response.timing.rate_limit_wait);
                service_time.record(response.timing.service_time);
                total_time.record(response.timing.total_time);
                if (completed.fetch_add(1, std::memory_order_acq_rel) + 1U == request_count) {
                    completion.notify_one();
                }
            });
        if (!result.accepted()) {
            throw std::runtime_error("API benchmark queue unexpectedly rejected a request");
        }
    }

    {
        std::unique_lock lock(completion_mutex);
        if (!completion.wait_for(lock, 30s, [&] {
                return completed.load(std::memory_order_acquire) == request_count;
            })) {
            throw std::runtime_error("timed out waiting for API benchmark");
        }
    }
    manager.shutdown(trading::ApiShutdownMode::drain);
    const auto elapsed = trading::MonotonicClock::now() - started;
    const auto seconds = std::chrono::duration<double>(elapsed).count();

    std::cout << "Shared API manager (4 I/O threads, mock API 1ms, limit 50/50ms):\n";
    std::cout << "  requests=" << request_count << " elapsed=" << std::fixed
              << std::setprecision(4) << seconds << "s throughput="
              << static_cast<double>(request_count) / seconds << " requests/s\n";
    print_summary("queue delay", queue_delay.summarize());
    print_summary("rate-limit wait", limiter_wait.summarize());
    print_summary("mock service", service_time.summarize());
    print_summary("submission-to-completion", total_time.summarize());
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const auto event_count = argc > 1 ? parse_size(argv[1], "event count") : 200'000U;
        const auto producer_count = argc > 2 ? parse_size(argv[2], "producer count") : 4U;
        const auto request_count = argc > 3 ? parse_size(argv[3], "request count") : 200U;
        run_event_benchmark(event_count, producer_count);
        run_api_benchmark(request_count);
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "benchmark failed: " << exception.what() << '\n';
        std::cerr << "Usage: trading_benchmark [event-count] [producer-count] [request-count]\n";
        return 1;
    }
}
