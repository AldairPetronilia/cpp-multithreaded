#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace trading {

// Sink-side fencing for components that can be replaced while a stale instance is still
// running. C++ cannot kill a hung thread, so a supervisor abandons it and starts a
// replacement; the abandoned thread may wake up later and try to publish. A flag checked
// before the write would not be safe (the thread can pause between the check and the
// write), so the check and the write both happen under a shared lock, and advance() takes
// the lock exclusively. Once advance() returns, no earlier generation is inside an action
// and none can enter one.
class GenerationGate {
public:
    [[nodiscard]] std::uint64_t current() const noexcept { return current_.load(std::memory_order_acquire); }

    // Makes every earlier generation stale and returns the new current one.
    std::uint64_t advance() {
        std::unique_lock lock(mutex_);
        return current_.fetch_add(1, std::memory_order_acq_rel) + 1U;
    }

    // Runs action only if generation is still the current one; otherwise counts a fenced
    // write. Actions must not block: they run under the shared lock.
    template <typename Action>
    bool run_if_current(std::uint64_t generation, Action&& action) {
        std::shared_lock lock(mutex_);
        if (current_.load(std::memory_order_relaxed) != generation) {
            fenced_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        std::forward<Action>(action)();
        return true;
    }

    [[nodiscard]] std::uint64_t fenced_writes() const noexcept { return fenced_.load(std::memory_order_relaxed); }

private:
    mutable std::shared_mutex mutex_;
    std::atomic<std::uint64_t> current_{1};
    std::atomic<std::uint64_t> fenced_{0};
};

}  // namespace trading
