#include "trading/journal.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstring>
#include <limits>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <system_error>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#define TRADING_JOURNAL_POSIX 1
#else
#define TRADING_JOURNAL_POSIX 0
#endif

namespace trading {
namespace {

static_assert(std::endian::native == std::endian::little, "journal files are little-endian");

namespace fs = std::filesystem;

constexpr char kMagic[8] = {'T', 'R', 'D', 'J', 'R', 'N', 'L', '\0'};
constexpr std::uint16_t kFormatVersion = 1;
constexpr std::uint8_t kSchemaVersion = 1;
// u64 sequence + i64 wall time + u8 kind + u16 topic length.
constexpr std::size_t kBodyFixed = 8 + 8 + 1 + 2;
constexpr std::size_t kFrameHeader = 8;
constexpr std::uint32_t kMaxBody = 16U << 20U;
constexpr std::size_t kFlushThreshold = 64U << 10U;

// --- Little-endian primitives -------------------------------------------------------------

class ByteWriter {
public:
    void u8(std::uint8_t value) { out_.push_back(static_cast<char>(value)); }
    void u16(std::uint16_t value) { raw(&value, sizeof value); }
    void u32(std::uint32_t value) { raw(&value, sizeof value); }
    void u64(std::uint64_t value) { raw(&value, sizeof value); }
    void i64(std::int64_t value) { raw(&value, sizeof value); }
    void f64(double value) { raw(&value, sizeof value); }
    void str(std::string_view text) {
        const auto length = static_cast<std::uint16_t>(std::min<std::size_t>(text.size(), 0xFFFFU));
        u16(length);
        out_.append(text.data(), length);
    }
    [[nodiscard]] std::string take() { return std::move(out_); }

private:
    void raw(const void* data, std::size_t size) { out_.append(static_cast<const char*>(data), size); }
    std::string out_;
};

class ByteReader {
public:
    explicit ByteReader(std::string_view data) : data_(data) {}
    std::uint8_t u8() { return take<std::uint8_t>(); }
    std::uint16_t u16() { return take<std::uint16_t>(); }
    std::uint32_t u32() { return take<std::uint32_t>(); }
    std::uint64_t u64() { return take<std::uint64_t>(); }
    std::int64_t i64() { return take<std::int64_t>(); }
    double f64() { return take<double>(); }
    std::string str() {
        const auto length = u16();
        if (!ok_ || data_.size() - position_ < length) {
            ok_ = false;
            return {};
        }
        std::string text(data_.substr(position_, length));
        position_ += length;
        return text;
    }
    [[nodiscard]] bool ok() const noexcept { return ok_; }
    [[nodiscard]] std::string_view rest() const noexcept { return data_.substr(position_); }

private:
    template <typename T>
    T take() {
        T value{};
        if (!ok_ || data_.size() - position_ < sizeof(T)) {
            ok_ = false;
            return value;
        }
        std::memcpy(&value, data_.data() + position_, sizeof(T));
        position_ += sizeof(T);
        return value;
    }

    std::string_view data_;
    std::size_t position_{0};
    bool ok_{true};
};

std::uint32_t crc32(std::string_view data, std::uint32_t seed) {
    static const auto table = [] {
        std::array<std::uint32_t, 256> entries{};
        for (std::uint32_t index = 0; index < 256U; ++index) {
            auto value = index;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value & 1U) != 0U ? 0xEDB88320U ^ (value >> 1U) : value >> 1U;
            }
            entries[index] = value;
        }
        return entries;
    }();
    auto crc = ~seed;
    for (const char character : data) {
        const auto byte = static_cast<unsigned char>(character);
        crc = table[(crc ^ byte) & 0xFFU] ^ (crc >> 8U);
    }
    return ~crc;
}

template <typename T>
T load(const char* where) {
    T value{};
    std::memcpy(&value, where, sizeof value);
    return value;
}

std::string segment_name(std::uint64_t base_sequence) {
    char name[40];
    std::snprintf(name, sizeof name, "seg-%016llu.jnl", static_cast<unsigned long long>(base_sequence));
    return name;
}

std::optional<std::uint64_t> parse_segment_name(const fs::path& path) {
    const auto name = path.filename().string();
    if (name.size() != 4 + 16 + 4 || !name.starts_with("seg-") || !name.ends_with(".jnl")) {
        return std::nullopt;
    }
    std::uint64_t value = 0;
    for (std::size_t index = 4; index < 20; ++index) {
        if (name[index] < '0' || name[index] > '9') {
            return std::nullopt;
        }
        value = value * 10U + static_cast<std::uint64_t>(name[index] - '0');
    }
    return value;
}

std::string read_file(const fs::path& path, std::size_t from = 0) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return {};
    }
    in.seekg(0, std::ios::end);
    const auto size = static_cast<std::size_t>(in.tellg());
    if (size <= from) {
        return {};
    }
    std::string data(size - from, '\0');
    in.seekg(static_cast<std::streamoff>(from));
    in.read(data.data(), static_cast<std::streamsize>(data.size()));
    data.resize(static_cast<std::size_t>(in.gcount()));
    return data;
}

// --- Segment header and frames ------------------------------------------------------------

struct SegmentHeader {
    std::uint64_t epoch{};
    std::uint64_t base_sequence{};
    std::string session{};
    std::size_t size{};
};

std::string encode_header(const SegmentHeader& header) {
    ByteWriter writer;
    for (const char c : kMagic) {
        writer.u8(static_cast<std::uint8_t>(c));
    }
    writer.u16(kFormatVersion);
    writer.u16(0);
    writer.u64(header.epoch);
    writer.u64(header.base_sequence);
    writer.str(header.session);
    return writer.take();
}

enum class HeaderStatus { ok, incomplete, bad };

HeaderStatus parse_header(std::string_view data, SegmentHeader& header) {
    constexpr std::size_t fixed = 8 + 2 + 2 + 8 + 8 + 2;
    if (data.size() < fixed) {
        return data.size() >= 8 && std::memcmp(data.data(), kMagic, 8) != 0 ? HeaderStatus::bad
                                                                               : HeaderStatus::incomplete;
    }
    if (std::memcmp(data.data(), kMagic, 8) != 0 || load<std::uint16_t>(data.data() + 8) != kFormatVersion) {
        return HeaderStatus::bad;
    }
    header.epoch = load<std::uint64_t>(data.data() + 12);
    header.base_sequence = load<std::uint64_t>(data.data() + 20);
    const auto session_length = load<std::uint16_t>(data.data() + 28);
    if (data.size() < fixed + session_length) {
        return HeaderStatus::incomplete;
    }
    header.session.assign(data.data() + fixed, session_length);
    header.size = fixed + session_length;
    return HeaderStatus::ok;
}

enum class FrameStatus { ok, incomplete, bad_checksum, bad_length };

struct Frame {
    FrameStatus status{FrameStatus::incomplete};
    std::size_t size{};
    JournalRecord record{};
};

Frame parse_frame(std::string_view data, std::uint64_t epoch) {
    Frame frame;
    if (data.size() < kFrameHeader) {
        return frame;
    }
    const auto length = load<std::uint32_t>(data.data());
    const auto checksum = load<std::uint32_t>(data.data() + 4);
    if (length < kBodyFixed || length > kMaxBody) {
        frame.status = FrameStatus::bad_length;
        return frame;
    }
    frame.size = kFrameHeader + length;
    if (data.size() < frame.size) {
        return frame;
    }
    const auto body = data.substr(kFrameHeader, length);
    if (crc32(body, static_cast<std::uint32_t>(epoch)) != checksum) {
        frame.status = FrameStatus::bad_checksum;
        return frame;
    }
    ByteReader reader(body);
    frame.record.sequence = reader.u64();
    frame.record.wall_ns = reader.i64();
    frame.record.kind = static_cast<JournalKind>(reader.u8());
    frame.record.topic = reader.str();
    frame.record.payload = std::string(reader.rest());
    frame.status = reader.ok() ? FrameStatus::ok : FrameStatus::bad_length;
    return frame;
}

struct SegmentScan {
    bool header_ok{false};
    SegmentHeader header{};
    std::uint64_t file_size{};
    std::uint64_t valid_end{};
    std::uint64_t last_sequence{};
    std::uint64_t records{};
    enum class Tail { clean, torn, corrupt } tail{Tail::clean};
};

SegmentScan scan_segment(const fs::path& path) {
    SegmentScan scan;
    const auto data = read_file(path);
    scan.file_size = data.size();
    if (parse_header(data, scan.header) != HeaderStatus::ok) {
        return scan;
    }
    scan.header_ok = true;
    auto offset = scan.header.size;
    scan.valid_end = offset;
    while (offset < data.size()) {
        const auto frame = parse_frame(std::string_view(data).substr(offset), scan.header.epoch);
        if (frame.status == FrameStatus::ok) {
            offset += frame.size;
            scan.valid_end = offset;
            scan.last_sequence = frame.record.sequence;
            ++scan.records;
            continue;
        }
        // A short record, or a bad one that runs exactly to the end of the file, is what a
        // crash in the middle of an append leaves. A bad record with more data after it is
        // not: something damaged the middle of the file.
        const bool reaches_end = frame.status == FrameStatus::incomplete ||
                                 (frame.status == FrameStatus::bad_checksum && offset + frame.size == data.size());
        scan.tail = reaches_end ? SegmentScan::Tail::torn : SegmentScan::Tail::corrupt;
        return scan;
    }
    return scan;
}

std::vector<fs::path> list_segments(const fs::path& directory) {
    std::vector<std::pair<std::uint64_t, fs::path>> found;
    std::error_code error;
    for (const auto& entry : fs::directory_iterator(directory, error)) {
        if (const auto base = parse_segment_name(entry.path())) {
            found.emplace_back(*base, entry.path());
        }
    }
    std::sort(found.begin(), found.end());
    std::vector<fs::path> paths;
    paths.reserve(found.size());
    for (auto& [base, path] : found) {
        static_cast<void>(base);
        paths.push_back(std::move(path));
    }
    return paths;
}

}  // namespace

std::int64_t wall_clock_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// --- Codec ---------------------------------------------------------------------------------

namespace {

std::int64_t ns(std::chrono::nanoseconds duration) {
    return duration.count();
}

std::chrono::nanoseconds from_ns(std::int64_t value) {
    return std::chrono::nanoseconds(value);
}

bool starts_payload(ByteReader& reader) {
    return reader.u8() == kSchemaVersion && reader.ok();
}

}  // namespace

std::optional<JournalRecord> encode_record(const EventEnvelope& envelope, std::string topic_name) {
    JournalRecord record;
    record.topic = std::move(topic_name);
    ByteWriter writer;
    writer.u8(kSchemaVersion);
    if (const auto* quote = envelope.as<MarketDataEvent>()) {
        record.kind = JournalKind::market_data;
        writer.str(quote->symbol);
        writer.f64(quote->price);
        writer.u64(quote->sequence);
    } else if (const auto* signal = envelope.as<TradingSignalEvent>()) {
        record.kind = JournalKind::trading_signal;
        writer.str(signal->strategy);
        writer.u64(signal->signal_id);
        writer.str(signal->symbol);
        writer.u8(static_cast<std::uint8_t>(signal->side));
        writer.f64(signal->quantity);
        writer.f64(signal->reference_price);
        writer.i64(ns(signal->signal_time - signal->market_data_time));
    } else if (const auto* report = envelope.as<ExecutionReportEvent>()) {
        record.kind = JournalKind::execution_report;
        writer.str(report->strategy);
        writer.u64(report->signal_id);
        writer.str(report->client_order_id);
        writer.str(report->symbol);
        writer.u8(static_cast<std::uint8_t>(report->side));
        writer.f64(report->quantity);
        writer.f64(report->fill_price);
        writer.u8(report->filled ? 1U : 0U);
        writer.str(report->message);
        writer.i64(ns(report->execution_time - report->market_data_time));
        writer.i64(ns(report->api_queue_delay));
        writer.i64(ns(report->rate_limit_wait));
        writer.i64(ns(report->api_service_time));
    } else if (const auto* supervision = envelope.as<SupervisionEvent>()) {
        record.kind = JournalKind::supervision;
        writer.str(supervision->component);
        writer.str(supervision->action);
        writer.str(supervision->detail);
        writer.u64(supervision->generation);
        writer.f64(supervision->recovery_ms);
        // Added after the first recordings; decoders treat it as optional.
        writer.f64(supervision->detect_ms);
    } else {
        return std::nullopt;
    }
    record.payload = writer.take();
    return record;
}

std::optional<MarketDataEvent> decode_market_data(const JournalRecord& record, TimePoint stamp) {
    if (record.kind != JournalKind::market_data) {
        return std::nullopt;
    }
    ByteReader reader(record.payload);
    if (!starts_payload(reader)) {
        return std::nullopt;
    }
    MarketDataEvent quote;
    quote.symbol = reader.str();
    quote.price = reader.f64();
    quote.sequence = reader.u64();
    quote.source_time = stamp;
    return reader.ok() ? std::optional(std::move(quote)) : std::nullopt;
}

std::optional<TradingSignalEvent> decode_signal(const JournalRecord& record, TimePoint stamp) {
    if (record.kind != JournalKind::trading_signal) {
        return std::nullopt;
    }
    ByteReader reader(record.payload);
    if (!starts_payload(reader)) {
        return std::nullopt;
    }
    TradingSignalEvent signal;
    signal.strategy = reader.str();
    signal.signal_id = reader.u64();
    signal.symbol = reader.str();
    signal.side = static_cast<Side>(reader.u8());
    signal.quantity = reader.f64();
    signal.reference_price = reader.f64();
    const auto latency = from_ns(reader.i64());
    signal.signal_time = stamp;
    signal.market_data_time = stamp - latency;
    return reader.ok() ? std::optional(std::move(signal)) : std::nullopt;
}

std::optional<ExecutionReportEvent> decode_execution_report(const JournalRecord& record, TimePoint stamp) {
    if (record.kind != JournalKind::execution_report) {
        return std::nullopt;
    }
    ByteReader reader(record.payload);
    if (!starts_payload(reader)) {
        return std::nullopt;
    }
    ExecutionReportEvent report;
    report.strategy = reader.str();
    report.signal_id = reader.u64();
    report.client_order_id = reader.str();
    report.symbol = reader.str();
    report.side = static_cast<Side>(reader.u8());
    report.quantity = reader.f64();
    report.fill_price = reader.f64();
    report.filled = reader.u8() != 0U;
    report.message = reader.str();
    const auto tick_to_execution = from_ns(reader.i64());
    report.api_queue_delay = from_ns(reader.i64());
    report.rate_limit_wait = from_ns(reader.i64());
    report.api_service_time = from_ns(reader.i64());
    report.execution_time = stamp;
    report.market_data_time = stamp - tick_to_execution;
    return reader.ok() ? std::optional(std::move(report)) : std::nullopt;
}

std::optional<SupervisionEvent> decode_supervision(const JournalRecord& record, TimePoint stamp) {
    if (record.kind != JournalKind::supervision) {
        return std::nullopt;
    }
    ByteReader reader(record.payload);
    if (!starts_payload(reader)) {
        return std::nullopt;
    }
    SupervisionEvent event;
    event.component = reader.str();
    event.action = reader.str();
    event.detail = reader.str();
    event.generation = reader.u64();
    event.recovery_ms = reader.f64();
    if (reader.ok() && !reader.rest().empty()) {
        event.detect_ms = reader.f64();
    }
    event.at = stamp;
    return reader.ok() ? std::optional(std::move(event)) : std::nullopt;
}

std::uint64_t decode_gap(const JournalRecord& record) {
    if (record.kind != JournalKind::gap) {
        return 0;
    }
    ByteReader reader(record.payload);
    if (!starts_payload(reader)) {
        return 0;
    }
    const auto count = reader.u64();
    return reader.ok() ? count : 0U;
}

std::optional<PublishResult> publish_record(EventBus& bus, const JournalRecord& record) {
    const auto stamp = MonotonicClock::now();
    const auto topic = record.topic.empty() ? Topic{} : bus.topic(record.topic);
    switch (record.kind) {
        case JournalKind::market_data:
            if (auto event = decode_market_data(record, stamp)) {
                return bus.publish(std::move(*event), topic);
            }
            break;
        case JournalKind::trading_signal:
            if (auto event = decode_signal(record, stamp)) {
                return bus.publish(std::move(*event), topic);
            }
            break;
        case JournalKind::execution_report:
            if (auto event = decode_execution_report(record, stamp)) {
                return bus.publish(std::move(*event), topic);
            }
            break;
        case JournalKind::supervision:
            if (auto event = decode_supervision(record, stamp)) {
                return bus.publish(std::move(*event), topic);
            }
            break;
        case JournalKind::gap:
            break;
    }
    return std::nullopt;
}

// --- Writer --------------------------------------------------------------------------------

JournalWriter::JournalWriter(EventBus& event_bus, JournalConfig config)
    : event_bus_(event_bus),
      config_(std::move(config)),
      worker_("journal-writer", event_bus,
              WorkerOptions{config_.mailbox_capacity, OverflowPolicy::reject_newest, config_.max_batch},
              [this](const EventEnvelope& envelope) { on_event(envelope); }) {
    if (config_.directory.empty()) {
        throw JournalError("journal directory must not be empty");
    }
    if (config_.segment_bytes < 4096U) {
        throw JournalError("journal segments must be at least 4 KiB");
    }
    std::error_code error;
    fs::create_directories(config_.directory, error);
    if (error) {
        throw JournalError("cannot create journal directory " + config_.directory.string() + ": " + error.message());
    }
    stats_.directory = config_.directory.string();
    stats_.session = config_.session;
    recover_existing();

    worker_.subscribe<MarketDataEvent>();
    worker_.subscribe<TradingSignalEvent>();
    worker_.subscribe<ExecutionReportEvent>();
    worker_.subscribe<SupervisionEvent>();
}

JournalWriter::~JournalWriter() {
    try {
        stop();
    } catch (const std::exception& exception) {
        std::cerr << "journal shutdown failed: " << exception.what() << '\n';
    }
}

void JournalWriter::recover_existing() {
    const auto files = list_segments(config_.directory);
    std::uint64_t last_sequence = 0;
    std::optional<fs::path> reusable;
    SegmentScan reusable_scan;

    for (std::size_t index = 0; index < files.size(); ++index) {
        const auto& path = files[index];
        const auto scan = scan_segment(path);
        const bool is_last = index + 1U == files.size();
        const auto quarantine = [&] {
            auto set_aside = path;
            set_aside += ".corrupt";
            std::error_code error;
            fs::rename(path, set_aside, error);
            ++stats_.quarantined_segments;
            std::cerr << "journal: set aside damaged segment " << path.filename().string() << '\n';
        };
        if (!scan.header_ok) {
            quarantine();
            continue;
        }
        last_sequence = std::max(last_sequence, scan.last_sequence);
        if (scan.tail == SegmentScan::Tail::corrupt) {
            quarantine();
            continue;
        }
        if (scan.tail == SegmentScan::Tail::torn && is_last) {
            std::error_code error;
            fs::resize_file(path, scan.valid_end, error);
            stats_.repaired_bytes += scan.file_size - scan.valid_end;
        }
        ++stats_.segments;
        stats_.records += scan.records;
        stats_.bytes += scan.valid_end;
        if (is_last) {
            reusable = path;
            reusable_scan = scan;
        }
    }
    next_sequence_ = last_sequence + 1U;

    if (reusable && reusable_scan.valid_end < config_.segment_bytes) {
        file_ = std::fopen(reusable->string().c_str(), "ab");
        if (file_ == nullptr) {
            throw JournalError("cannot reopen journal segment " + reusable->string());
        }
        epoch_ = reusable_scan.header.epoch;
        segment_size_ = reusable_scan.valid_end;
        return;
    }
    open_segment_locked(next_sequence_);
}

void JournalWriter::open_segment_locked(std::uint64_t base_sequence) {
    std::random_device entropy;
    epoch_ = (static_cast<std::uint64_t>(entropy()) << 32U) ^ entropy() ^
             static_cast<std::uint64_t>(wall_clock_ns());
    const auto path = config_.directory / segment_name(base_sequence);
    file_ = std::fopen(path.string().c_str(), "wb");
    if (file_ == nullptr) {
        throw JournalError("cannot create journal segment " + path.string());
    }
    const auto header = encode_header(SegmentHeader{epoch_, base_sequence, config_.session, 0});
    if (std::fwrite(header.data(), 1, header.size(), file_) != header.size() || std::fflush(file_) != 0) {
        throw JournalError("cannot write journal segment header " + path.string());
    }
    segment_size_ = header.size();
    stats_.bytes += header.size();
    ++stats_.segments;
}

void JournalWriter::close_segment_locked() {
    if (file_ != nullptr) {
        flush_locked();
        fsync_locked();
        std::fclose(file_);
        file_ = nullptr;
    }
}

void JournalWriter::flush_locked() {
    if (buffer_.empty() || file_ == nullptr || stats_.failed) {
        return;
    }
    if (std::fwrite(buffer_.data(), 1, buffer_.size(), file_) != buffer_.size() || std::fflush(file_) != 0) {
        // Part of the buffer may have reached the file: a torn record that reopening
        // truncates. Nothing more is appended after it.
        fail_locked(std::string("write failed: ") + std::strerror(errno));
        return;
    }
    stats_.bytes += buffer_.size();
    ++stats_.flushes;
    buffer_.clear();
    buffered_records_ = 0;
}

void JournalWriter::fsync_locked() {
    if (file_ == nullptr || stats_.failed) {
        return;
    }
    bool synced = std::fflush(file_) == 0;
#if TRADING_JOURNAL_POSIX
    const int descriptor = ::fileno(file_);
#if defined(__APPLE__) && defined(F_FULLFSYNC)
    if (config_.full_fsync) {
        synced = synced && ::fcntl(descriptor, F_FULLFSYNC) != -1;
    } else {
        synced = synced && ::fsync(descriptor) == 0;
    }
#else
    synced = synced && ::fsync(descriptor) == 0;
#endif
#endif
    if (!synced) {
        fail_locked(std::string("sync to disk failed: ") + std::strerror(errno));
        return;
    }
    ++stats_.fsyncs;
    last_fsync_ = std::chrono::steady_clock::now();
}

void JournalWriter::fail_locked(const std::string& what) {
    if (!stats_.failed) {
        stats_.failed = true;
        stats_.error = what;
        std::cerr << "journal failed: " << what << "; no longer recording\n";
    }
    stats_.lost_records += buffered_records_;
    buffered_records_ = 0;
    buffer_.clear();
}

void JournalWriter::append_locked(JournalKind kind, std::int64_t wall_ns, std::string_view topic,
                                  std::string_view payload) {
    const std::size_t body_size = kBodyFixed + topic.size() + payload.size();
    const std::size_t frame_size = kFrameHeader + body_size;
    if (stats_.failed) {
        ++stats_.lost_records;
        return;
    }
    if (file_ == nullptr) {
        return;  // stopped
    }
    if (segment_size_ + frame_size > config_.segment_bytes && segment_size_ > 512U) {
        try {
            close_segment_locked();
            open_segment_locked(next_sequence_);
        } catch (const std::exception& exception) {
            fail_locked(exception.what());
            ++stats_.lost_records;
            return;
        }
        if (stats_.failed) {
            ++stats_.lost_records;
            return;
        }
    }

    ByteWriter body;
    body.u64(next_sequence_);
    body.i64(wall_ns);
    body.u8(static_cast<std::uint8_t>(kind));
    body.u16(static_cast<std::uint16_t>(topic.size()));
    auto text = body.take();
    text.append(topic);
    text.append(payload);

    const auto checksum = crc32(text, static_cast<std::uint32_t>(epoch_));
    const auto length = static_cast<std::uint32_t>(text.size());
    buffer_.append(reinterpret_cast<const char*>(&length), sizeof length);
    buffer_.append(reinterpret_cast<const char*>(&checksum), sizeof checksum);
    buffer_.append(text);
    segment_size_ += frame_size;
    ++next_sequence_;
    ++stats_.records;
    ++buffered_records_;
    if (buffer_.size() >= kFlushThreshold) {
        flush_locked();
    }
}

void JournalWriter::on_event(const EventEnvelope& envelope) {
    std::string topic;
    if (envelope.topic.id != 0U) {
        if (envelope.topic.id >= topic_names_.size()) {
            topic_names_.resize(envelope.topic.id + 1U);
        }
        auto& cached = topic_names_[envelope.topic.id];
        if (cached.empty()) {
            cached = event_bus_.topic_name(envelope.topic);
        }
        topic = cached;
    }
    auto record = encode_record(envelope, std::move(topic));
    if (!record) {
        return;
    }
    // Stamp the time the event was published, not the time the tap got around to writing
    // it: a backlog would otherwise squash the recorded spacing that realtime replay uses.
    const auto age = std::chrono::duration_cast<std::chrono::nanoseconds>(MonotonicClock::now() - envelope.published_at);
    const auto wall_ns = wall_clock_ns() - age.count();
    std::lock_guard lock(mutex_);
    append_locked(record->kind, wall_ns, record->topic, record->payload);
}

void JournalWriter::record_gap() {
    const auto rejected = worker_.mailbox_stats().rejected_full;
    std::lock_guard lock(mutex_);
    if (rejected <= seen_rejections_) {
        return;
    }
    const auto missing = rejected - seen_rejections_;
    seen_rejections_ = rejected;
    ByteWriter payload;
    payload.u8(kSchemaVersion);
    payload.u64(missing);
    append_locked(JournalKind::gap, wall_clock_ns(), {}, payload.take());
    ++stats_.gaps;
    stats_.gap_events += missing;
}

void JournalWriter::flusher_loop(std::stop_token stop) {
    std::mutex wait_mutex;
    std::condition_variable_any wakeup;
    std::unique_lock wait_lock(wait_mutex);
    while (!stop.stop_requested()) {
        wakeup.wait_for(wait_lock, stop, config_.flush_interval, [] { return false; });
        try {
            record_gap();
            std::lock_guard lock(mutex_);
            flush_locked();
            if (config_.fsync_interval > std::chrono::milliseconds::zero() &&
                std::chrono::steady_clock::now() - last_fsync_ >= config_.fsync_interval) {
                fsync_locked();
            }
        } catch (const std::exception& exception) {
            std::cerr << "journal flush failed: " << exception.what() << '\n';
        }
    }
}

void JournalWriter::start() {
    std::lock_guard lock(mutex_);
    if (started_) {
        return;
    }
    started_ = true;
    last_fsync_ = std::chrono::steady_clock::now();
    worker_.start();
    flusher_ = std::jthread([this](std::stop_token stop) { flusher_loop(stop); });
}

void JournalWriter::stop() {
    {
        std::lock_guard lock(mutex_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
    }
    worker_.stop();
    if (flusher_.joinable()) {
        flusher_.request_stop();
        flusher_.join();
    }
    record_gap();
    std::lock_guard lock(mutex_);
    close_segment_locked();
}

bool JournalWriter::sync() {
    const bool drained = worker_.wait_until_drained(std::chrono::seconds(5));
    record_gap();
    std::lock_guard lock(mutex_);
    flush_locked();
    return drained && !stats_.failed;
}

std::uint64_t JournalWriter::last_sequence() const {
    std::lock_guard lock(mutex_);
    return next_sequence_ - 1U;
}

JournalStats JournalWriter::stats() const {
    const auto mailbox = worker_.mailbox_stats();
    std::lock_guard lock(mutex_);
    auto stats = stats_;
    stats.last_sequence = next_sequence_ - 1U;
    stats.tap_depth = mailbox.depth;
    stats.tap_capacity = mailbox.capacity;
    stats.tap_rejected = mailbox.rejected_full;
    return stats;
}

// --- Reader --------------------------------------------------------------------------------

JournalReader::JournalReader(fs::path directory) : directory_(std::move(directory)) {
    refresh_segments();
}

JournalReader::~JournalReader() = default;

void JournalReader::refresh_segments() {
    std::vector<SegmentFile> found;
    for (auto& path : list_segments(directory_)) {
        if (const auto base = parse_segment_name(path)) {
            found.push_back(SegmentFile{std::move(path), *base});
        }
    }
    segments_ = std::move(found);
}

bool JournalReader::has_later_segment() {
    if (index_ + 1U < segments_.size()) {
        return true;
    }
    refresh_segments();
    return index_ + 1U < segments_.size();
}

bool JournalReader::open_segment(std::size_t index) {
    if (index >= segments_.size()) {
        refresh_segments();
        if (index >= segments_.size()) {
            return false;
        }
    }
    auto data = read_file(segments_[index].path);
    SegmentHeader header;
    const auto status = parse_header(data, header);
    if (status == HeaderStatus::incomplete) {
        return false;  // the writer is still creating it
    }
    if (status == HeaderStatus::bad) {
        corrupt_ = true;
        error_ = "bad segment header in " + segments_[index].path.filename().string();
        return false;
    }
    index_ = index;
    buffer_ = std::move(data);
    offset_ = header.size;
    epoch_ = header.epoch;
    session_ = header.session;
    open_ = true;
    return true;
}

bool JournalReader::refill() {
    if (!open_) {
        return false;
    }
    auto more = read_file(segments_[index_].path, buffer_.size());
    if (more.empty()) {
        return false;
    }
    buffer_ += more;
    return true;
}

void JournalReader::seek(std::uint64_t wanted) {
    refresh_segments();
    seek_target_ = wanted;
    previous_sequence_ = 0;
    open_ = false;
    corrupt_ = false;
    error_.clear();
    buffer_.clear();
    index_ = 0;
    for (std::size_t index = 0; index < segments_.size(); ++index) {
        if (segments_[index].base_sequence <= wanted) {
            index_ = index;
        }
    }
}

std::optional<JournalRecord> JournalReader::next() {
    if (corrupt_) {
        return std::nullopt;
    }
    for (;;) {
        if (!open_ && !open_segment(index_)) {
            return std::nullopt;
        }
        const auto frame = parse_frame(std::string_view(buffer_).substr(offset_), epoch_);
        switch (frame.status) {
            case FrameStatus::ok:
                offset_ += frame.size;
                if (frame.record.sequence < seek_target_) {
                    continue;
                }
                if (previous_sequence_ != 0U && frame.record.sequence > previous_sequence_ + 1U) {
                    missing_ += frame.record.sequence - previous_sequence_ - 1U;
                }
                previous_sequence_ = frame.record.sequence;
                return frame.record;
            case FrameStatus::incomplete:
                if (refill()) {
                    continue;
                }
                if (has_later_segment()) {
                    open_ = false;
                    ++index_;
                    buffer_.clear();
                    continue;
                }
                return std::nullopt;
            case FrameStatus::bad_checksum:
                // The last record of the newest segment may still be being written.
                if (offset_ + frame.size >= buffer_.size() && !has_later_segment()) {
                    if (refill()) {
                        continue;
                    }
                    return std::nullopt;
                }
                [[fallthrough]];
            case FrameStatus::bad_length:
                corrupt_ = true;
                error_ = "damaged record at offset " + std::to_string(offset_) + " of " +
                         segments_[index_].path.filename().string();
                return std::nullopt;
        }
    }
}

JournalReader::Bounds JournalReader::bounds(const fs::path& directory) {
    Bounds bounds;
    const auto files = list_segments(directory);
    if (files.empty()) {
        return bounds;
    }
    // Segments are named by the sequence of their first record; only the newest needs a scan.
    if (const auto base = parse_segment_name(files.front())) {
        bounds.first = *base;
    }
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
        const auto scan = scan_segment(*it);
        if (scan.header_ok && scan.records > 0U) {
            bounds.last = scan.last_sequence;
            break;
        }
    }
    if (bounds.last == 0U) {
        bounds.first = 0;
    }
    return bounds;
}

// --- Manifest ------------------------------------------------------------------------------

void write_manifest(const fs::path& directory, const SessionManifest& manifest) {
    std::ofstream out(directory / "manifest.txt", std::ios::trunc);
    if (!out) {
        throw JournalError("cannot write session manifest in " + directory.string());
    }
    // Round-trip precision: a replay in another process must trade the same quantities.
    out.precision(std::numeric_limits<double>::max_digits10);
    out << "session " << manifest.session << '\n';
    for (const auto& feed : manifest.feeds) {
        out << "feed " << feed.name;
        for (const auto& [symbol, price] : feed.instruments) {
            out << ' ' << symbol << '@' << price;
        }
        out << '\n';
    }
    for (const auto& strategy : manifest.strategies) {
        out << "strategy " << strategy.name << ' ' << strategy.kind << ' ' << strategy.quantity;
        if (strategy.symbols.empty()) {
            out << " *";
        }
        for (const auto& symbol : strategy.symbols) {
            out << ' ' << symbol;
        }
        out << '\n';
    }
}

std::optional<SessionManifest> read_manifest(const fs::path& directory) {
    std::ifstream in(directory / "manifest.txt");
    if (!in) {
        return std::nullopt;
    }
    SessionManifest manifest;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream fields(line);
        std::string word;
        fields >> word;
        if (word == "session") {
            fields >> manifest.session;
        } else if (word == "feed") {
            ManifestFeed feed;
            fields >> feed.name;
            std::string instrument;
            while (fields >> instrument) {
                const auto at = instrument.find('@');
                if (at == std::string::npos) {
                    continue;
                }
                feed.instruments.emplace_back(instrument.substr(0, at), std::strtod(instrument.c_str() + at + 1, nullptr));
            }
            manifest.feeds.push_back(std::move(feed));
        } else if (word == "strategy") {
            ManifestStrategy strategy;
            fields >> strategy.name >> strategy.kind >> strategy.quantity;
            std::string symbol;
            while (fields >> symbol) {
                if (symbol != "*") {
                    strategy.symbols.push_back(symbol);
                }
            }
            manifest.strategies.push_back(std::move(strategy));
        }
    }
    return manifest;
}

}  // namespace trading
