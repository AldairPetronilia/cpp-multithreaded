#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <numeric>
#include <vector>

namespace trading {

struct LatencySummary {
    std::size_t count{};
    double minimum_us{};
    double mean_us{};
    double p50_us{};
    double p95_us{};
    double p99_us{};
    double maximum_us{};
};

class LatencyRecorder {
public:
    void record(std::chrono::nanoseconds value) {
        std::lock_guard lock(mutex_);
        samples_.push_back(value.count());
    }

    [[nodiscard]] std::size_t size() const {
        std::lock_guard lock(mutex_);
        return samples_.size();
    }

    [[nodiscard]] LatencySummary summarize() const {
        std::vector<std::int64_t> values;
        {
            std::lock_guard lock(mutex_);
            values = samples_;
        }

        if (values.empty()) {
            return {};
        }

        std::sort(values.begin(), values.end());
        const auto percentile = [&values](double fraction) {
            const auto raw_index = fraction * static_cast<double>(values.size() - 1U);
            const auto index = static_cast<std::size_t>(std::ceil(raw_index));
            return static_cast<double>(values[index]) / 1'000.0;
        };

        const auto total = std::accumulate(
            values.begin(), values.end(), static_cast<long double>(0.0));
        return LatencySummary{
            values.size(),
            static_cast<double>(values.front()) / 1'000.0,
            static_cast<double>(total / static_cast<long double>(values.size())) / 1'000.0,
            percentile(0.50),
            percentile(0.95),
            percentile(0.99),
            static_cast<double>(values.back()) / 1'000.0,
        };
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::int64_t> samples_;
};

}  // namespace trading

