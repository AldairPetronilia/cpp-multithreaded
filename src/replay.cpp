#include "trading/replay.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace trading {

const char* to_string(ReplayMode mode) noexcept {
    return mode == ReplayMode::rederive ? "rederive" : "playback";
}

const char* to_string(ReplayPacing pacing) noexcept {
    return pacing == ReplayPacing::realtime ? "realtime" : "lockstep";
}

const char* to_string(ReplayState state) noexcept {
    switch (state) {
        case ReplayState::idle:
            return "idle";
        case ReplayState::running:
            return "running";
        case ReplayState::paused:
            return "paused";
        case ReplayState::finished:
            return "finished";
        case ReplayState::failed:
            return "failed";
    }
    return "idle";
}

// --- ReplayFeed ----------------------------------------------------------------------------

ReplayFeed::ReplayFeed(EventBus& event_bus, ReplayOptions options, Barrier barrier)
    : event_bus_(event_bus),
      options_(std::move(options)),
      barrier_(std::move(barrier)),
      speed_(options_.speed),
      pacing_(options_.pacing) {
    if (options_.speed <= 0.0) {
        throw std::invalid_argument("replay speed must be positive");
    }
    if (options_.lockstep_batch == 0U) {
        throw std::invalid_argument("replay lockstep batch must be positive");
    }
    const auto bounds = JournalReader::bounds(options_.directory);
    if (bounds.count() == 0U) {
        throw JournalError("no journal records to replay in " + options_.directory.string());
    }
    first_ = options_.from_sequence == 0U ? bounds.first : std::max(options_.from_sequence, bounds.first);
    last_ = options_.to_sequence == 0U ? bounds.last : std::min(options_.to_sequence, bounds.last);
    if (first_ > last_) {
        throw JournalError("replay range is empty");
    }

    // The recorded time span, for progress and for the position display.
    JournalReader reader(options_.directory);
    reader.seek(first_);
    if (const auto head = reader.next()) {
        first_wall_ns_ = head->wall_ns;
        last_wall_ns_ = head->wall_ns;
    }
    reader.seek(last_);
    while (const auto record = reader.next()) {
        if (record->sequence > last_) {
            break;
        }
        last_wall_ns_ = std::max(last_wall_ns_, record->wall_ns);
    }
    position_wall_ns_.store(first_wall_ns_, std::memory_order_relaxed);
}

ReplayFeed::~ReplayFeed() {
    stop();
    wait();
}

void ReplayFeed::start() {
    if (started_.exchange(true)) {
        throw std::logic_error("replay feed already started");
    }
    started_at_ = std::chrono::steady_clock::now();
    thread_ = std::jthread([this](std::stop_token stop) { run(stop); });
}

void ReplayFeed::stop() {
    if (thread_.joinable()) {
        thread_.request_stop();
    }
}

void ReplayFeed::wait() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

void ReplayFeed::set_paused(bool paused) {
    {
        std::lock_guard lock(mutex_);
        paused_ = paused;
    }
    wakeup_.notify_all();
}

void ReplayFeed::set_speed(double speed) {
    if (speed <= 0.0) {
        throw std::invalid_argument("replay speed must be positive");
    }
    {
        std::lock_guard lock(mutex_);
        speed_ = speed;
    }
    wakeup_.notify_all();
}

void ReplayFeed::set_pacing(ReplayPacing pacing) {
    {
        std::lock_guard lock(mutex_);
        pacing_ = pacing;
    }
    wakeup_.notify_all();
}

void ReplayFeed::seek(std::uint64_t sequence) {
    {
        std::lock_guard lock(mutex_);
        seek_pending_ = true;
        seek_target_ = std::clamp(sequence, first_, last_);
    }
    seeked_.store(true, std::memory_order_relaxed);
    wakeup_.notify_all();
}

bool ReplayFeed::finished() const noexcept {
    return finished_.load(std::memory_order_acquire) || failed_.load(std::memory_order_acquire);
}

void ReplayFeed::fail(const std::string& message) {
    {
        std::lock_guard lock(mutex_);
        error_ = message;
    }
    failed_.store(true, std::memory_order_release);
    std::cerr << "replay failed: " << message << '\n';
}

ReplayProgress ReplayFeed::progress() const {
    ReplayProgress progress;
    progress.mode = options_.mode;
    progress.first_sequence = first_;
    progress.last_sequence = last_;
    progress.current_sequence = current_sequence_.load(std::memory_order_relaxed);
    progress.published = published_.load(std::memory_order_relaxed);
    progress.gaps = gaps_.load(std::memory_order_relaxed);
    progress.gap_events = gap_events_.load(std::memory_order_relaxed);
    progress.seeked = seeked_.load(std::memory_order_relaxed);
    progress.span_s = static_cast<double>(last_wall_ns_ - first_wall_ns_) / 1e9;
    progress.position_s =
        std::max(0.0, static_cast<double>(position_wall_ns_.load(std::memory_order_relaxed) - first_wall_ns_) / 1e9);
    const auto frozen = finished_after_ns_.load(std::memory_order_relaxed);
    progress.elapsed_s =
        !started_.load(std::memory_order_acquire)
            ? 0.0
            : frozen != 0 ? static_cast<double>(frozen) / 1e9
                          : std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at_).count();
    {
        std::lock_guard lock(mutex_);
        progress.speed = speed_;
        progress.pacing = pacing_;
        progress.error = error_;
        progress.by_topic = by_topic_;
        progress.state = failed_.load(std::memory_order_acquire)     ? ReplayState::failed
                         : finished_.load(std::memory_order_acquire) ? ReplayState::finished
                         : !started_.load(std::memory_order_acquire) ? ReplayState::idle
                         : paused_                                    ? ReplayState::paused
                                                                      : ReplayState::running;
    }
    return progress;
}

ReplayFeed::Pace ReplayFeed::pace(std::stop_token stop, const JournalRecord& record, PaceState& state) {
    std::unique_lock lock(mutex_);
    for (;;) {
        if (stop.stop_requested()) {
            return Pace::stop;
        }
        if (seek_pending_) {
            return Pace::restart;
        }
        if (paused_) {
            wakeup_.wait(lock, stop, [this] { return !paused_ || seek_pending_; });
            state.anchored = false;
            continue;
        }
        if (pacing_ == ReplayPacing::lockstep) {
            return Pace::proceed;
        }

        // The time this record is due, relative to the previous one. Accumulating the due
        // times rather than sleeping a relative interval keeps the replay from drifting.
        const auto now = std::chrono::steady_clock::now();
        if (!state.anchored) {
            state.anchored = true;
            state.previous_wall_ns = record.wall_ns;
            state.previous_due = now;
            return Pace::proceed;
        }
        const auto speed = speed_;
        const auto recorded_gap = std::max<std::int64_t>(0, record.wall_ns - state.previous_wall_ns);
        const auto capped_gap = std::min<std::int64_t>(
            recorded_gap, std::chrono::duration_cast<std::chrono::nanoseconds>(options_.max_idle_gap).count());
        const auto due = state.previous_due + std::chrono::nanoseconds(static_cast<std::int64_t>(
                                                  static_cast<double>(capped_gap) / speed));
        if (now >= due) {
            // After a stall, restart the schedule instead of bursting to catch up.
            state.previous_due = now - due > std::chrono::milliseconds(100) ? now : due;
            state.previous_wall_ns = std::max(state.previous_wall_ns, record.wall_ns);
            return Pace::proceed;
        }
        wakeup_.wait_until(lock, stop, due, [this, speed] { return paused_ || seek_pending_ || speed_ != speed; });
    }
}

void ReplayFeed::run(std::stop_token stop) {
    try {
        JournalReader reader(options_.directory);
        reader.seek(first_);
        PaceState pace_state;
        std::size_t since_barrier = 0;

        while (!stop.stop_requested()) {
            std::optional<JournalRecord> record = reader.next();
            if (!record) {
                if (reader.corrupt()) {
                    fail(reader.error());
                }
                break;
            }
            if (record->sequence > last_) {
                break;
            }
            current_sequence_.store(record->sequence, std::memory_order_relaxed);
            position_wall_ns_.store(std::max(record->wall_ns, position_wall_ns_.load(std::memory_order_relaxed)),
                                    std::memory_order_relaxed);

            if (record->kind == JournalKind::gap) {
                gaps_.fetch_add(1, std::memory_order_relaxed);
                gap_events_.fetch_add(decode_gap(*record), std::memory_order_relaxed);
                continue;
            }
            const bool wanted = options_.mode == ReplayMode::playback || record->kind == JournalKind::market_data;
            if (!wanted) {
                continue;
            }

            const auto pace_result = pace(stop, *record, pace_state);
            if (pace_result == Pace::stop) {
                break;
            }
            if (pace_result == Pace::restart) {
                std::uint64_t target = 0;
                {
                    std::lock_guard lock(mutex_);
                    target = seek_target_;
                    seek_pending_ = false;
                }
                reader.seek(target);
                pace_state.anchored = false;
                since_barrier = 0;
                continue;
            }

            publish_record(event_bus_, *record);
            published_.fetch_add(1, std::memory_order_relaxed);
            if (record->kind == JournalKind::market_data) {
                std::lock_guard lock(mutex_);
                ++by_topic_[record->topic];
            }
            bool lockstep = false;
            {
                std::lock_guard lock(mutex_);
                lockstep = pacing_ == ReplayPacing::lockstep;
            }
            if (lockstep && barrier_ && ++since_barrier >= options_.lockstep_batch) {
                since_barrier = 0;
                barrier_();
            }
        }
        if (barrier_ && !stop.stop_requested()) {
            barrier_();
        }
    } catch (const std::exception& exception) {
        fail(exception.what());
    } catch (...) {
        fail("unknown exception");
    }
    finished_after_ns_.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at_).count(),
        std::memory_order_relaxed);
    if (!failed_.load(std::memory_order_acquire)) {
        finished_.store(true, std::memory_order_release);
    }
}

// --- ReplayVerifier ------------------------------------------------------------------------

namespace {

std::string signal_key(const std::string& strategy, const std::string& symbol, std::uint64_t signal_id, Side side) {
    std::string key = strategy;
    key += '|';
    key += symbol;
    key += '|';
    key += std::to_string(signal_id);
    key += '|';
    key += to_string(side);
    return key;
}

}  // namespace

ReplayVerifier::ReplayVerifier(EventBus& event_bus, const std::filesystem::path& directory, std::uint64_t from,
                               std::uint64_t to, bool comparable)
    : comparable_(comparable),
      worker_("replay-verifier", event_bus, WorkerOptions{16'384, OverflowPolicy::reject_newest, 256},
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    JournalReader reader(directory);
    reader.seek(from);
    while (const auto record = reader.next()) {
        if (record->sequence > to) {
            break;
        }
        const auto stamp = MonotonicClock::now();
        if (record->kind == JournalKind::trading_signal) {
            if (const auto signal = decode_signal(*record, stamp)) {
                expected_.insert(signal_key(signal->strategy, signal->symbol, signal->signal_id, signal->side));
                ++expected_by_strategy_[signal->strategy];
                ++recorded_;
            }
        } else if (record->kind == JournalKind::supervision) {
            if (const auto event = decode_supervision(*record, stamp)) {
                if (event->action == "restarted" || event->action == "quarantined") {
                    outages_.insert(event->component);
                }
            }
        } else if (record->kind == JournalKind::gap) {
            ++journal_gaps_;
        }
    }
    worker_.subscribe<TradingSignalEvent>();
}

void ReplayVerifier::start() {
    worker_.start();
}

void ReplayVerifier::stop() {
    worker_.stop();
}

void ReplayVerifier::mark_incomparable(const std::string& reason) {
    std::lock_guard lock(mutex_);
    comparable_ = false;
    note_ = reason;
}

void ReplayVerifier::on_event(const EventEnvelope& envelope) {
    const auto* signal = envelope.as<TradingSignalEvent>();
    if (signal == nullptr) {
        return;
    }
    const auto key = signal_key(signal->strategy, signal->symbol, signal->signal_id, signal->side);
    std::lock_guard lock(mutex_);
    ++rederived_;
    if (expected_.contains(key)) {
        ++matched_;
        ++matched_by_strategy_[signal->strategy];
    } else {
        ++extra_;
        ++extra_by_strategy_[signal->strategy];
    }
}

VerificationReport ReplayVerifier::report(bool complete) const {
    std::lock_guard lock(mutex_);
    VerificationReport report;
    report.enabled = true;
    report.comparable = comparable_;
    report.complete = complete;
    report.recorded = recorded_;
    report.rederived = rederived_;
    report.matched = matched_;
    report.extra = extra_;
    report.outage_components.assign(outages_.begin(), outages_.end());

    // Absences are only final once the replay has finished; until then "missing" is just
    // "not seen yet".
    std::uint64_t missing = 0;
    std::uint64_t explained = 0;
    for (const auto& [strategy, count] : expected_by_strategy_) {
        const auto matched = matched_by_strategy_.contains(strategy) ? matched_by_strategy_.at(strategy) : 0U;
        const auto absent = count - std::min(count, matched);
        missing += absent;
        if (complete && outages_.contains(strategy)) {
            explained += absent;
        }
    }
    if (complete) {
        for (const auto& [strategy, count] : extra_by_strategy_) {
            if (outages_.contains(strategy)) {
                explained += count;
            }
        }
        report.missing = missing;
        report.explained = explained;
    } else {
        report.missing = 0;
    }

    report.note = note_;
    if (!comparable_) {
        report.verdict = "not-comparable";
        if (report.note.empty()) {
            report.note = "the replay did not run the whole recording from its start";
        }
    } else if (!complete) {
        report.verdict = "running";
    } else if (report.missing == 0U && report.extra == 0U) {
        report.verdict = "match";
    } else if (report.missing + report.extra == report.explained) {
        report.verdict = "explained";
        report.note = "every difference is on a strategy that was restarted during the recording";
    } else {
        report.verdict = "diverged";
        if (journal_gaps_ > 0U) {
            report.note = "the journal has gaps (events lost to tap overflow), so a re-derivation cannot be exact";
        } else {
            report.note = "the live strategies saw a different input than the journal holds (shedding under load?)";
        }
    }
    return report;
}

}  // namespace trading
