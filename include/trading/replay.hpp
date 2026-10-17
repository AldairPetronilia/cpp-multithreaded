#pragma once

#include "trading/journal.hpp"
#include "trading/messaging.hpp"
#include "trading/workers.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace trading {

enum class ReplayMode {
    // Publish only the recorded market data and let the live strategies derive their
    // signals again (against a mock broker). The recorded signals are the answer key.
    rederive,
    // Publish everything that was recorded, as recorded: market data, signals, fills.
    // Nothing is recomputed; the dashboard and position book show what happened.
    playback,
};

enum class ReplayPacing {
    // Recorded spacing divided by the speed. Downstream mailboxes may shed under load, as
    // they do live.
    realtime,
    // As fast as downstream keeps up: after every few records the feed waits for the
    // consumers to drain, so no quote is ever shed and a re-derivation is exact.
    lockstep,
};

enum class ReplayState { idle, running, paused, finished, failed };

[[nodiscard]] const char* to_string(ReplayMode mode) noexcept;
[[nodiscard]] const char* to_string(ReplayPacing pacing) noexcept;
[[nodiscard]] const char* to_string(ReplayState state) noexcept;

struct ReplayOptions {
    std::filesystem::path directory{};
    ReplayMode mode{ReplayMode::rederive};
    ReplayPacing pacing{ReplayPacing::realtime};
    double speed{1.0};
    // Zero means the first / last record of the session.
    std::uint64_t from_sequence{0};
    std::uint64_t to_sequence{0};
    // In lockstep, how many market-data records are published between waits for downstream.
    std::size_t lockstep_batch{16};
    // Recorded silences (a paused feed, say) longer than this are compressed to it.
    std::chrono::milliseconds max_idle_gap{250};
};

struct ReplayProgress {
    ReplayState state{ReplayState::idle};
    ReplayMode mode{ReplayMode::rederive};
    ReplayPacing pacing{ReplayPacing::realtime};
    double speed{1.0};
    std::uint64_t first_sequence{};
    std::uint64_t last_sequence{};
    std::uint64_t current_sequence{};
    std::uint64_t published{};
    // Records the journal marks as lost to tap overflow while it was recorded.
    std::uint64_t gaps{};
    std::uint64_t gap_events{};
    double span_s{};
    double position_s{};
    double elapsed_s{};
    bool seeked{};
    std::string error{};
    // Market-data records published per topic, for the feed nodes of the dashboard.
    std::map<std::string, std::uint64_t> by_topic{};
};

// A publisher that reads the journal instead of generating quotes. It plugs in where the
// live feeds do, so everything downstream is exactly the code that ran live.
class ReplayFeed {
public:
    // Called after batches (lockstep) and at the end: returns once downstream has caught up.
    using Barrier = std::function<void()>;

    // Throws JournalError if the session is empty or unreadable.
    ReplayFeed(EventBus& event_bus, ReplayOptions options, Barrier barrier = {});
    ~ReplayFeed();
    ReplayFeed(const ReplayFeed&) = delete;
    ReplayFeed& operator=(const ReplayFeed&) = delete;

    void start();
    void stop();
    void wait();

    void set_paused(bool paused);
    void set_speed(double speed);
    void set_pacing(ReplayPacing pacing);
    // Jumps to the first record at or after sequence. A re-derivation that skips quotes
    // starts its strategies cold, so it can no longer be compared with the recording.
    void seek(std::uint64_t sequence);

    [[nodiscard]] ReplayProgress progress() const;
    [[nodiscard]] bool finished() const noexcept;
    [[nodiscard]] bool seeked() const noexcept { return seeked_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t first_sequence() const noexcept { return first_; }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept { return last_; }
    [[nodiscard]] const ReplayOptions& options() const noexcept { return options_; }

private:
    enum class Pace { proceed, restart, stop };

    // Where the realtime schedule stands: the previous record's recorded time and the
    // moment it was due.
    struct PaceState {
        bool anchored{false};
        std::int64_t previous_wall_ns{0};
        std::chrono::steady_clock::time_point previous_due{};
    };

    void run(std::stop_token stop);
    Pace pace(std::stop_token stop, const JournalRecord& record, PaceState& state);
    void fail(const std::string& message);

    EventBus& event_bus_;
    const ReplayOptions options_;
    Barrier barrier_;
    std::uint64_t first_{0};
    std::uint64_t last_{0};
    std::int64_t first_wall_ns_{0};
    std::int64_t last_wall_ns_{0};

    mutable std::mutex mutex_;
    std::condition_variable_any wakeup_;
    bool paused_{false};
    double speed_{1.0};
    ReplayPacing pacing_{ReplayPacing::realtime};
    bool seek_pending_{false};
    std::uint64_t seek_target_{0};
    std::string error_;
    std::map<std::string, std::uint64_t> by_topic_;

    std::atomic<bool> started_{false};
    std::atomic<bool> finished_{false};
    std::atomic<bool> failed_{false};
    std::atomic<bool> seeked_{false};
    std::atomic<std::uint64_t> current_sequence_{0};
    std::atomic<std::uint64_t> published_{0};
    std::atomic<std::uint64_t> gaps_{0};
    std::atomic<std::uint64_t> gap_events_{0};
    std::atomic<std::int64_t> position_wall_ns_{0};
    std::chrono::steady_clock::time_point started_at_{};
    std::atomic<std::int64_t> finished_after_ns_{0};

    std::jthread thread_;
};

struct VerificationReport {
    bool enabled{false};
    // False when the replay did not run the whole recording from the start.
    bool comparable{true};
    bool complete{false};
    std::uint64_t recorded{};
    std::uint64_t rederived{};
    std::uint64_t matched{};
    // Recorded signals the replay did not produce, and signals it produced that were not
    // recorded.
    std::uint64_t missing{};
    std::uint64_t extra{};
    // Differences that fall on a strategy the supervisor had to restart during the
    // recording: a restarted strategy catches up silently, so it skips the signals that
    // fired while it was down. Those differences are expected.
    std::uint64_t explained{};
    std::vector<std::string> outage_components{};
    // running | match | explained | diverged | not-comparable
    std::string verdict{"running"};
    std::string note{};
};

// Compares the signals a replay derives with the ones the journal recorded. A signal is
// identified by strategy, symbol, triggering-quote sequence and side, which is also what
// the order id is built from, so a match means the same orders would have been placed.
class ReplayVerifier {
public:
    // Reads the recorded signals in [from, to] from the journal and subscribes to the
    // replay bus's signals. comparable=false marks a replay that cannot be compared.
    ReplayVerifier(EventBus& event_bus, const std::filesystem::path& directory, std::uint64_t from,
                   std::uint64_t to, bool comparable);

    void start();
    void stop();
    bool wait_until_drained(std::chrono::milliseconds timeout) { return worker_.wait_until_drained(timeout); }
    void mark_incomparable(const std::string& reason);
    // complete: the replay has finished and downstream has drained, so absences are final.
    [[nodiscard]] VerificationReport report(bool complete) const;

private:
    void on_event(const EventEnvelope& envelope);

    mutable std::mutex mutex_;
    std::unordered_set<std::string> expected_;
    std::unordered_map<std::string, std::uint64_t> expected_by_strategy_;
    std::unordered_map<std::string, std::uint64_t> matched_by_strategy_;
    std::unordered_map<std::string, std::uint64_t> extra_by_strategy_;
    std::set<std::string> outages_;
    std::uint64_t recorded_{0};
    std::uint64_t rederived_{0};
    std::uint64_t matched_{0};
    std::uint64_t extra_{0};
    std::uint64_t journal_gaps_{0};
    bool comparable_{true};
    std::string note_;
    EventWorker worker_;
};

}  // namespace trading
