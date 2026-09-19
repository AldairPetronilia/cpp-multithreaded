#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace trading {

struct LatencySummary {
    std::uint64_t count{};
    double mean_us{};
    double p50_us{};
    double p95_us{};
    double p99_us{};
    double max_us{};
};

// Point-in-time copy of a LatencyHistogram. Subtracting an earlier snapshot of the same
// histogram yields the distribution of the samples recorded in between, which is how the
// dashboard reports live (per-interval) percentiles next to cumulative ones.
class HistogramSnapshot {
public:
    [[nodiscard]] std::uint64_t count() const noexcept { return count_; }
    [[nodiscard]] double percentile_ns(double fraction) const noexcept;
    [[nodiscard]] double mean_ns() const noexcept;
    [[nodiscard]] double max_ns() const noexcept;
    [[nodiscard]] LatencySummary summary() const noexcept;
    [[nodiscard]] HistogramSnapshot since(const HistogramSnapshot& earlier) const;

private:
    friend class LatencyHistogram;

    std::vector<std::uint64_t> counts_;
    std::uint64_t count_{};
    std::uint64_t sum_ns_{};
    std::uint64_t max_ns_{};
    bool exact_max_{true};
};

// Log-linear histogram in the style of HdrHistogram: 32 linear sub-buckets per power of two,
// so every value is reported within ~3% of what was recorded. Memory is fixed (8 KiB) and
// recording is a handful of relaxed atomic increments, so monitoring can read it while
// workers record, without the unbounded growth and hot-path locking of storing samples.
class LatencyHistogram {
public:
    static constexpr unsigned kSubBucketBits = 5;
    static constexpr std::uint64_t kSubBucketCount = std::uint64_t{1} << kSubBucketBits;
    // Values at or above 2^36 ns (about 68.7 s) are recorded in the last bucket.
    static constexpr unsigned kValueBits = 36;
    static constexpr std::size_t kBucketCount =
        (kValueBits - kSubBucketBits + 1U) * kSubBucketCount;

    void record(std::chrono::nanoseconds value) noexcept;
    [[nodiscard]] HistogramSnapshot snapshot() const;
    [[nodiscard]] LatencySummary summary() const { return snapshot().summary(); }

    [[nodiscard]] static std::size_t bucket_index(std::uint64_t value_ns) noexcept;
    [[nodiscard]] static std::uint64_t bucket_upper_bound(std::size_t index) noexcept;

private:
    std::array<std::atomic<std::uint64_t>, kBucketCount> counts_{};
    std::atomic<std::uint64_t> sum_ns_{0};
    std::atomic<std::uint64_t> max_ns_{0};
};

}  // namespace trading
