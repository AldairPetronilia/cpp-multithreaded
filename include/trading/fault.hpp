#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <stdexcept>

namespace trading {

// A handler throws this to say the worker itself is no longer trustworthy. Ordinary
// exceptions are contained per event; this one ends the worker thread so that a supervisor
// can replace the worker.
class FatalWorkerError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// One-shot fault injection for a worker, driven from the dashboard. A fault is armed from
// any thread and fires on the worker thread the next time on_event() is called, then
// disarms itself; the control outlives the worker instance, so the replacement is healthy.
//
//   crash   the worker thread dies (FatalWorkerError)
//   hang    the handler blocks, as a deadlock or an endless loop would; it resumes when
//           released, or after the given time, as a stuck thread that wakes up later
//   poison  the next N events make the handler throw
class FaultControl {
public:
    void arm_crash() noexcept { armed_.store(kCrash, std::memory_order_release); }

    void arm_hang(std::chrono::milliseconds auto_release) {
        {
            std::lock_guard lock(mutex_);
            hang_for_ = auto_release;
            released_ = false;
        }
        armed_.store(kHang, std::memory_order_release);
    }

    void arm_poison(std::uint64_t events) noexcept {
        poison_remaining_.store(events, std::memory_order_relaxed);
        armed_.store(kPoison, std::memory_order_release);
    }

    // Called by the worker thread at the start of each event. The common case, nothing
    // armed, is one relaxed load.
    void on_event() {
        const auto armed = armed_.load(std::memory_order_relaxed);
        if (armed == kNone) {
            return;
        }
        fire(armed);
    }

    // Disarms without waking a hung thread. A replacement calls this: the event that made
    // the failed instance throw has, in real life, been dead-lettered by then.
    void disarm() noexcept { armed_.store(kNone, std::memory_order_relaxed); }

    // Wakes anything hung and disarms. Used at shutdown, and by tests, so that no thread is
    // left blocked.
    void release_all() {
        armed_.store(kNone, std::memory_order_relaxed);
        {
            std::lock_guard lock(mutex_);
            released_ = true;
        }
        wakeup_.notify_all();
    }

    [[nodiscard]] bool armed() const noexcept { return armed_.load(std::memory_order_relaxed) != kNone; }
    [[nodiscard]] std::uint64_t fired() const noexcept { return fired_.load(std::memory_order_relaxed); }

private:
    static constexpr int kNone = 0;
    static constexpr int kCrash = 1;
    static constexpr int kHang = 2;
    static constexpr int kPoison = 3;

    void fire(int armed) {
        if (armed == kPoison) {
            if (poison_remaining_.fetch_sub(1, std::memory_order_relaxed) <= 1U) {
                int expected = kPoison;
                armed_.compare_exchange_strong(expected, kNone, std::memory_order_acq_rel);
            }
            fired_.fetch_add(1, std::memory_order_relaxed);
            throw std::runtime_error("injected poison event");
        }
        int expected = armed;
        if (!armed_.compare_exchange_strong(expected, kNone, std::memory_order_acq_rel)) {
            return;  // another thread consumed it
        }
        fired_.fetch_add(1, std::memory_order_relaxed);
        if (armed == kCrash) {
            throw FatalWorkerError("injected worker crash");
        }
        std::unique_lock lock(mutex_);
        wakeup_.wait_for(lock, hang_for_, [this] { return released_; });
    }

    std::atomic<int> armed_{kNone};
    std::atomic<std::uint64_t> poison_remaining_{0};
    std::atomic<std::uint64_t> fired_{0};
    std::mutex mutex_;
    std::condition_variable wakeup_;
    std::chrono::milliseconds hang_for_{0};
    bool released_{false};
};

}  // namespace trading
