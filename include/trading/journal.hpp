#pragma once

#include "trading/events.hpp"
#include "trading/messaging.hpp"
#include "trading/workers.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace trading {

// Durable, ordered record of everything that crosses the event bus. It serves two purposes
// with one mechanism: replaying a session, and rebuilding the state of a restarted
// component. See README, "Journal".
//
// On disk a session is a directory of segment files, seg-<base sequence>.jnl. A segment is
//
//   header   magic "TRDJRNL\0" | u16 version | u16 reserved | u64 epoch | u64 base sequence
//            | u16 session length | session
//   frames   u32 body length | u32 CRC-32 | body
//   body     u64 sequence | i64 wall-clock ns | u8 kind | u16 topic length | topic | payload
//
// The CRC is seeded with the segment's random epoch, so bytes left behind in a recycled
// file can never validate. Topics are stored by name, because topic ids are interned per
// process. Times are wall-clock, because the bus's steady-clock times mean nothing after a
// restart. Numbers are little-endian.

enum class JournalKind : std::uint8_t {
    // The tap's mailbox overflowed: this many events were not recorded.
    gap = 0,
    market_data = 1,
    trading_signal = 2,
    execution_report = 3,
    supervision = 4,
};

struct JournalRecord {
    std::uint64_t sequence{};
    std::int64_t wall_ns{};
    JournalKind kind{JournalKind::gap};
    std::string topic{};
    std::string payload{};
};

class JournalError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// --- Event codec ---------------------------------------------------------------------------
// Payloads begin with a schema version byte. Decoders return nullopt for a payload they do
// not understand. Steady-clock fields are not stored: a decoded event is stamped with the
// time given, and latencies are stored as durations and re-based onto that stamp.

[[nodiscard]] std::optional<JournalRecord> encode_record(const EventEnvelope& envelope, std::string topic_name);
[[nodiscard]] std::optional<MarketDataEvent> decode_market_data(const JournalRecord& record, TimePoint stamp);
[[nodiscard]] std::optional<TradingSignalEvent> decode_signal(const JournalRecord& record, TimePoint stamp);
[[nodiscard]] std::optional<ExecutionReportEvent> decode_execution_report(const JournalRecord& record,
                                                                          TimePoint stamp);
[[nodiscard]] std::optional<SupervisionEvent> decode_supervision(const JournalRecord& record, TimePoint stamp);
// Number of events a gap record stands for.
[[nodiscard]] std::uint64_t decode_gap(const JournalRecord& record);
// Decodes the record and publishes it, stamped now, on its own topic. Gap records and
// undecodable payloads publish nothing and return nullopt.
std::optional<PublishResult> publish_record(EventBus& bus, const JournalRecord& record);

[[nodiscard]] std::int64_t wall_clock_ns() noexcept;

// --- Writer --------------------------------------------------------------------------------

struct JournalConfig {
    std::filesystem::path directory{};
    std::string session{};
    std::size_t segment_bytes{16U << 20U};
    // The tap must not lose events silently, so its mailbox is large and rejects instead
    // of evicting; any overflow is recorded as a gap.
    std::size_t mailbox_capacity{65'536};
    std::size_t max_batch{256};
    // Buffered records reach the OS (surviving a process crash) at this interval...
    std::chrono::milliseconds flush_interval{20};
    // ...and reach the disk (surviving power loss) at this one. Zero never syncs.
    std::chrono::milliseconds fsync_interval{1'000};
    // On macOS, fsync does not flush the drive's cache; F_FULLFSYNC does, but is slow.
    bool full_fsync{false};
};

struct JournalStats {
    std::string directory{};
    std::string session{};
    std::uint64_t records{};
    std::uint64_t bytes{};
    std::uint64_t segments{};
    std::uint64_t gaps{};
    std::uint64_t gap_events{};
    std::uint64_t flushes{};
    std::uint64_t fsyncs{};
    std::uint64_t last_sequence{};
    std::size_t tap_depth{};
    std::size_t tap_capacity{};
    std::uint64_t tap_rejected{};
    // Found on opening an existing session: bytes of torn tail truncated, and segments set
    // aside because a complete record failed its checksum.
    std::uint64_t repaired_bytes{};
    std::uint64_t quarantined_segments{};
};

// Subscribes to every journaled event type and appends them from one writer thread. An
// ordinary subscriber: publishers never wait for the disk.
class JournalWriter {
public:
    // Creates the session directory, or reopens one, repairing a torn tail left by a crash.
    JournalWriter(EventBus& event_bus, JournalConfig config);
    ~JournalWriter();
    JournalWriter(const JournalWriter&) = delete;
    JournalWriter& operator=(const JournalWriter&) = delete;

    void start();
    // Drains the tap, then flushes and syncs everything.
    void stop();

    // Barrier: returns once every event published before the call has been written and
    // handed to the OS, so a reader opened afterwards sees it.
    void sync();

    [[nodiscard]] JournalStats stats() const;
    [[nodiscard]] const std::filesystem::path& directory() const noexcept { return config_.directory; }
    [[nodiscard]] const std::string& session() const noexcept { return config_.session; }
    [[nodiscard]] std::uint64_t last_sequence() const;

private:
    void on_event(const EventEnvelope& envelope);
    void append_locked(JournalKind kind, std::int64_t wall_ns, std::string_view topic, std::string_view payload);
    void open_segment_locked(std::uint64_t base_sequence);
    void close_segment_locked();
    void flush_locked();
    void fsync_locked();
    void record_gap();
    void flusher_loop(std::stop_token stop);
    void recover_existing();

    EventBus& event_bus_;
    const JournalConfig config_;
    mutable std::mutex mutex_;
    std::FILE* file_{nullptr};
    std::string buffer_;
    std::uint64_t epoch_{0};
    std::uint64_t segment_size_{0};
    std::uint64_t next_sequence_{1};
    std::uint64_t seen_rejections_{0};
    std::chrono::steady_clock::time_point last_fsync_{};
    JournalStats stats_{};
    std::vector<std::string> topic_names_;  // indexed by topic id; only the tap thread uses it
    std::jthread flusher_;
    bool started_{false};
    bool stopped_{false};
    // Declared last so the tap thread stops before the state it uses is destroyed.
    EventWorker worker_;
};

// --- Reader --------------------------------------------------------------------------------

// Reads a session in order. It tolerates a writer appending at the same time: at the end of
// what is on disk, next() returns nullopt and a later call returns the records written since.
class JournalReader {
public:
    explicit JournalReader(std::filesystem::path directory);
    ~JournalReader();
    JournalReader(const JournalReader&) = delete;
    JournalReader& operator=(const JournalReader&) = delete;

    // Positions the reader so the next record returned has sequence >= wanted.
    void seek(std::uint64_t wanted);
    [[nodiscard]] std::optional<JournalRecord> next();

    // True once a complete record failed its checksum or framing: the data after it cannot
    // be trusted, and next() returns nothing more.
    [[nodiscard]] bool corrupt() const noexcept { return corrupt_; }
    [[nodiscard]] const std::string& error() const noexcept { return error_; }
    [[nodiscard]] const std::string& session() const noexcept { return session_; }

    struct Bounds {
        std::uint64_t first{};
        std::uint64_t last{};
        [[nodiscard]] std::uint64_t count() const noexcept { return last >= first && last != 0U ? last - first + 1U : 0U; }
    };
    // First and last sequence on disk (zero when the session is empty).
    [[nodiscard]] static Bounds bounds(const std::filesystem::path& directory);

private:
    struct SegmentFile {
        std::filesystem::path path;
        std::uint64_t base_sequence;
    };

    void refresh_segments();
    bool open_segment(std::size_t index);
    bool refill();
    [[nodiscard]] bool has_later_segment();

    std::filesystem::path directory_;
    std::vector<SegmentFile> segments_;
    std::size_t index_{0};
    bool open_{false};
    std::string buffer_;
    std::size_t offset_{0};
    std::uint64_t epoch_{0};
    std::string session_;
    std::uint64_t seek_target_{0};
    bool corrupt_{false};
    std::string error_;
};

// --- Session manifest ----------------------------------------------------------------------
// The topology that produced a session, written next to its segments so that a later
// process can replay it without being told the feeds and strategies again.

struct ManifestFeed {
    std::string name{};
    std::vector<std::pair<std::string, double>> instruments{};
};

struct ManifestStrategy {
    std::string name{};
    std::string kind{};
    std::vector<std::string> symbols{};
    double quantity{1.0};
};

struct SessionManifest {
    std::string session{};
    std::vector<ManifestFeed> feeds{};
    std::vector<ManifestStrategy> strategies{};
};

void write_manifest(const std::filesystem::path& directory, const SessionManifest& manifest);
[[nodiscard]] std::optional<SessionManifest> read_manifest(const std::filesystem::path& directory);

}  // namespace trading
