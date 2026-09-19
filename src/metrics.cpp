#include "trading/metrics.hpp"

#include <algorithm>
#include <bit>
#include <cmath>

namespace trading {

std::size_t LatencyHistogram::bucket_index(std::uint64_t value_ns) noexcept {
    constexpr auto max_value = (std::uint64_t{1} << kValueBits) - 1U;
    const auto value = std::min(value_ns, max_value);
    const auto highest_bit = static_cast<unsigned>(std::bit_width(value));
    // Values below 2^(sub bits + 1) map one-to-one; above that, each power of two is split
    // into kSubBucketCount equal-width buckets.
    const auto shift = highest_bit > kSubBucketBits + 1U ? highest_bit - kSubBucketBits - 1U : 0U;
    return static_cast<std::size_t>(shift * kSubBucketCount + (value >> shift));
}

std::uint64_t LatencyHistogram::bucket_upper_bound(std::size_t index) noexcept {
    if (index < 2U * kSubBucketCount) {
        return index;
    }
    const auto shift = index / kSubBucketCount - 1U;
    const auto mantissa = index % kSubBucketCount + kSubBucketCount;
    return ((mantissa + 1U) << shift) - 1U;
}

void LatencyHistogram::record(std::chrono::nanoseconds value) noexcept {
    const auto value_ns = value.count() > 0 ? static_cast<std::uint64_t>(value.count()) : 0U;
    counts_[bucket_index(value_ns)].fetch_add(1, std::memory_order_relaxed);
    sum_ns_.fetch_add(value_ns, std::memory_order_relaxed);
    auto observed_max = max_ns_.load(std::memory_order_relaxed);
    while (value_ns > observed_max &&
           !max_ns_.compare_exchange_weak(observed_max, value_ns, std::memory_order_relaxed)) {
    }
}

HistogramSnapshot LatencyHistogram::snapshot() const {
    // Relaxed reads: a snapshot taken during concurrent recording may miss samples that are
    // in flight, but the count is derived from the buckets so percentiles stay consistent.
    HistogramSnapshot snapshot;
    snapshot.counts_.resize(kBucketCount);
    for (std::size_t index = 0; index < kBucketCount; ++index) {
        const auto count = counts_[index].load(std::memory_order_relaxed);
        snapshot.counts_[index] = count;
        snapshot.count_ += count;
    }
    snapshot.sum_ns_ = sum_ns_.load(std::memory_order_relaxed);
    snapshot.max_ns_ = max_ns_.load(std::memory_order_relaxed);
    return snapshot;
}

double HistogramSnapshot::percentile_ns(double fraction) const noexcept {
    if (count_ == 0U) {
        return 0.0;
    }
    const auto clamped = std::clamp(fraction, 0.0, 1.0);
    const auto rank = std::max<std::uint64_t>(
        1U, static_cast<std::uint64_t>(std::ceil(clamped * static_cast<double>(count_))));
    std::uint64_t cumulative = 0;
    for (std::size_t index = 0; index < counts_.size(); ++index) {
        cumulative += counts_[index];
        if (cumulative >= rank) {
            const auto bound = static_cast<double>(LatencyHistogram::bucket_upper_bound(index));
            return exact_max_ ? std::min(bound, static_cast<double>(max_ns_)) : bound;
        }
    }
    return max_ns();
}

double HistogramSnapshot::mean_ns() const noexcept {
    return count_ == 0U ? 0.0 : static_cast<double>(sum_ns_) / static_cast<double>(count_);
}

double HistogramSnapshot::max_ns() const noexcept {
    if (count_ == 0U) {
        return 0.0;
    }
    if (exact_max_) {
        return static_cast<double>(max_ns_);
    }
    for (auto index = counts_.size(); index > 0U; --index) {
        if (counts_[index - 1U] != 0U) {
            return static_cast<double>(LatencyHistogram::bucket_upper_bound(index - 1U));
        }
    }
    return 0.0;
}

LatencySummary HistogramSnapshot::summary() const noexcept {
    constexpr double ns_per_us = 1'000.0;
    return LatencySummary{
        count_,
        mean_ns() / ns_per_us,
        percentile_ns(0.50) / ns_per_us,
        percentile_ns(0.95) / ns_per_us,
        percentile_ns(0.99) / ns_per_us,
        max_ns() / ns_per_us,
    };
}

HistogramSnapshot HistogramSnapshot::since(const HistogramSnapshot& earlier) const {
    HistogramSnapshot delta;
    delta.counts_.resize(counts_.size());
    for (std::size_t index = 0; index < counts_.size(); ++index) {
        const auto before = index < earlier.counts_.size() ? earlier.counts_[index] : 0U;
        delta.counts_[index] = counts_[index] >= before ? counts_[index] - before : 0U;
        delta.count_ += delta.counts_[index];
    }
    delta.sum_ns_ = sum_ns_ >= earlier.sum_ns_ ? sum_ns_ - earlier.sum_ns_ : 0U;
    // The cumulative maximum says nothing about the interval, so fall back to bucket bounds.
    delta.exact_max_ = false;
    return delta;
}

}  // namespace trading
