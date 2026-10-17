#include "trading/telemetry.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <map>

namespace trading {

JsonWriter& JsonWriter::begin_object() {
    before_value();
    out_ += '{';
    scope_has_items_.push_back(false);
    return *this;
}

JsonWriter& JsonWriter::end_object() {
    out_ += '}';
    scope_has_items_.pop_back();
    return *this;
}

JsonWriter& JsonWriter::begin_array() {
    before_value();
    out_ += '[';
    scope_has_items_.push_back(false);
    return *this;
}

JsonWriter& JsonWriter::end_array() {
    out_ += ']';
    scope_has_items_.pop_back();
    return *this;
}

JsonWriter& JsonWriter::key(std::string_view name) {
    before_value();
    write_string(name);
    out_ += ':';
    after_key_ = true;
    return *this;
}

JsonWriter& JsonWriter::value(std::string_view text) {
    before_value();
    write_string(text);
    return *this;
}

JsonWriter& JsonWriter::value(bool flag) {
    before_value();
    out_ += flag ? "true" : "false";
    return *this;
}

JsonWriter& JsonWriter::value(double number) {
    before_value();
    if (!std::isfinite(number)) {
        out_ += "null";
        return *this;
    }
    // Seven significant digits keep prices and microsecond latencies exact enough for
    // display while keeping the document small.
    std::array<char, 32> buffer{};
    const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), number,
                                            std::chars_format::general, 7);
    out_.append(buffer.data(), error == std::errc{} ? end : buffer.data());
    return *this;
}

JsonWriter& JsonWriter::null() {
    before_value();
    out_ += "null";
    return *this;
}

void JsonWriter::before_value() {
    if (after_key_) {
        after_key_ = false;
        return;
    }
    if (!scope_has_items_.empty()) {
        if (scope_has_items_.back()) {
            out_ += ',';
        }
        scope_has_items_.back() = true;
    }
}

void JsonWriter::write_string(std::string_view text) {
    constexpr char hex[] = "0123456789abcdef";
    out_ += '"';
    for (const char character : text) {
        switch (character) {
            case '"':
                out_ += "\\\"";
                break;
            case '\\':
                out_ += "\\\\";
                break;
            case '\n':
                out_ += "\\n";
                break;
            case '\r':
                out_ += "\\r";
                break;
            case '\t':
                out_ += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(character) < 0x20U) {
                    const auto code = static_cast<unsigned char>(character);
                    out_ += "\\u00";
                    out_ += hex[code >> 4U];
                    out_ += hex[code & 0x0FU];
                } else {
                    out_ += character;
                }
        }
    }
    out_ += '"';
}

namespace {

class RateCalculator {
public:
    explicit RateCalculator(double elapsed_s) : elapsed_s_(elapsed_s) {}

    [[nodiscard]] double per_second(std::uint64_t now, std::optional<std::uint64_t> before) const {
        if (!before || elapsed_s_ <= 0.0 || now < *before) {
            return 0.0;
        }
        return static_cast<double>(now - *before) / elapsed_s_;
    }

    [[nodiscard]] double busy_fraction(std::chrono::nanoseconds now,
                                       std::optional<std::chrono::nanoseconds> before) const {
        if (!before || elapsed_s_ <= 0.0) {
            return 0.0;
        }
        return std::clamp(std::chrono::duration<double>(now - *before).count() / elapsed_s_, 0.0, 1.0);
    }

private:
    double elapsed_s_;
};

template <typename T, typename Field>
std::optional<std::uint64_t> earlier(const T* before, Field field) {
    return before ? std::optional<std::uint64_t>(field(*before)) : std::nullopt;
}

void write_summary(JsonWriter& json, const LatencySummary& summary) {
    json.begin_object()
        .key("count").value(summary.count)
        .key("mean").value(summary.mean_us)
        .key("p50").value(summary.p50_us)
        .key("p99").value(summary.p99_us)
        .key("max").value(summary.max_us)
        .end_object();
}

// Latencies are in microseconds. "recent" covers only the samples since the last render.
void write_latency(JsonWriter& json, const HistogramSnapshot& now, const HistogramSnapshot* before) {
    json.begin_object().key("all");
    write_summary(json, now.summary());
    json.key("recent");
    write_summary(json, before ? now.since(*before).summary() : now.summary());
    json.end_object();
}

void write_worker(JsonWriter& json,
                  const WorkerStats& now,
                  const WorkerStats* before,
                  const RateCalculator& rates) {
    json.key("processed").value(now.processed)
        .key("rate").value(rates.per_second(now.processed, earlier(before, [](const WorkerStats& s) { return s.processed; })))
        .key("errors").value(now.handler_errors)
        .key("depth").value(now.mailbox.depth)
        .key("capacity").value(now.mailbox.capacity)
        .key("max_depth").value(now.mailbox.max_depth)
        .key("rejected").value(now.mailbox.rejected_full)
        .key("evicted").value(now.mailbox.evicted_oldest)
        .key("closed").value(now.mailbox.rejected_closed)
        .key("drop_rate").value(rates.per_second(
            now.mailbox.rejected_full + now.mailbox.evicted_oldest,
            earlier(before, [](const WorkerStats& s) {
                return s.mailbox.rejected_full + s.mailbox.evicted_oldest;
            })))
        .key("utilization").value(rates.busy_fraction(
            now.busy_time,
            before ? std::optional<std::chrono::nanoseconds>(before->busy_time) : std::nullopt))
        .key("dispatch");
    write_latency(json, now.dispatch_latency, before ? &before->dispatch_latency : nullptr);
}

template <typename T>
const T* at_or_null(const std::vector<T>* items, std::size_t index) {
    return items != nullptr && index < items->size() ? &(*items)[index] : nullptr;
}

void write_verification(JsonWriter& json, const VerificationReport& report) {
    json.begin_object()
        .key("enabled").value(report.enabled)
        .key("comparable").value(report.comparable)
        .key("complete").value(report.complete)
        .key("recorded").value(report.recorded)
        .key("rederived").value(report.rederived)
        .key("matched").value(report.matched)
        .key("missing").value(report.missing)
        .key("extra").value(report.extra)
        .key("explained").value(report.explained)
        .key("verdict").value(report.verdict)
        .key("note").value(report.note)
        .key("outages").begin_array();
    for (const auto& component : report.outage_components) {
        json.value(component);
    }
    json.end_array().end_object();
}

void write_replay(JsonWriter& json, const ReplaySnapshot& replay) {
    const auto& progress = replay.progress;
    const auto range = progress.last_sequence > progress.first_sequence
                           ? static_cast<double>(progress.last_sequence - progress.first_sequence)
                           : 1.0;
    const auto done = progress.current_sequence > progress.first_sequence
                          ? static_cast<double>(progress.current_sequence - progress.first_sequence)
                          : 0.0;
    json.begin_object()
        .key("state").value(to_string(progress.state))
        .key("mode").value(to_string(progress.mode))
        .key("pacing").value(to_string(progress.pacing))
        .key("speed").value(progress.speed)
        .key("first").value(progress.first_sequence)
        .key("last").value(progress.last_sequence)
        .key("current").value(progress.current_sequence)
        .key("progress").value(progress.state == ReplayState::finished ? 1.0 : std::clamp(done / range, 0.0, 1.0))
        .key("published").value(progress.published)
        .key("gaps").value(progress.gaps)
        .key("gap_events").value(progress.gap_events)
        .key("span_s").value(progress.span_s)
        .key("position_s").value(progress.position_s)
        .key("elapsed_s").value(progress.elapsed_s)
        .key("seeked").value(progress.seeked)
        .key("error").value(progress.error)
        .key("verification");
    write_verification(json, replay.verification);
    json.end_object();
}

}  // namespace

std::string TelemetryRenderer::render(const SystemSnapshot& now, const ViewInfo& view) {
    const SystemSnapshot* before = previous_ ? &*previous_ : nullptr;
    const RateCalculator rates(before ? now.uptime_s - before->uptime_s : 0.0);
    JsonWriter json;
    json.begin_object();
    json.key("uptime_s").value(now.uptime_s).key("running").value(now.running);
    json.key("view").value(view.view)
        .key("mode").value(now.mode)
        .key("session").value(now.session)
        .key("can_replay").value(view.can_replay)
        .key("live_available").value(view.live_available);

    json.key("bus").begin_object()
        .key("published").value(now.bus.events_published)
        .key("rate").value(rates.per_second(now.bus.events_published,
                                            earlier(before, [](const SystemSnapshot& s) { return s.bus.events_published; })))
        .key("without_subscribers").value(now.bus.events_without_subscribers)
        .key("subscriptions").value(now.bus.subscriptions)
        .key("topics").value(now.bus.topics)
        .end_object();

    json.key("feeds").begin_array();
    for (std::size_t index = 0; index < now.feeds.size(); ++index) {
        const auto& feed = now.feeds[index];
        const auto* old = at_or_null(before ? &before->feeds : nullptr, index);
        json.begin_object()
            .key("name").value(feed.name)
            .key("published").value(feed.events_published)
            .key("rate").value(rates.per_second(feed.events_published,
                                                earlier(old, [](const ProducerStats& s) { return s.events_published; })))
            .key("rejections").value(feed.subscriber_rejections)
            .key("failures").value(feed.failures)
            .key("interval_us").value(feed.interval.count())
            .key("paused").value(feed.paused)
            .key("finished").value(feed.finished)
            .key("crashed").value(feed.crashed)
            .key("symbols").begin_array();
        for (const auto& symbol : feed.symbols) {
            json.value(symbol);
        }
        json.end_array().end_object();
    }
    json.end_array();

    json.key("strategies").begin_array();
    for (std::size_t index = 0; index < now.strategies.size(); ++index) {
        const auto& strategy = now.strategies[index];
        const auto* old = at_or_null(before ? &before->strategies : nullptr, index);
        json.begin_object()
            .key("name").value(strategy.worker.name)
            .key("kind").value(strategy.kind)
            .key("signals").value(strategy.signals_published)
            .key("signal_rate").value(rates.per_second(strategy.signals_published,
                                                       earlier(old, [](const SignalProcessorStats& s) { return s.signals_published; })))
            .key("publish_rejections").value(strategy.publish_rejections)
            .key("recovered_records").value(strategy.recovered_records)
            .key("dead_letters").value(strategy.dead_letters)
            .key("fenced_writes").value(strategy.fenced_writes)
            .key("work_us").value(index < now.strategy_work.size() ? now.strategy_work[index].count() : 0)
            .key("symbols").begin_array();
        for (const auto& symbol : strategy.symbols) {
            json.value(symbol);
        }
        json.end_array();
        write_worker(json, strategy.worker, old ? &old->worker : nullptr, rates);
        json.key("tick_to_signal");
        write_latency(json, strategy.tick_to_signal, old ? &old->tick_to_signal : nullptr);
        json.end_object();
    }
    json.end_array();

    json.key("execution").begin_object()
        .key("name").value(now.execution.worker.name)
        .key("submitted").value(now.execution.requests_submitted)
        .key("rejected_submissions").value(now.execution.requests_rejected)
        .key("report_rejections").value(now.execution.report_publish_rejections)
        .key("fenced_writes").value(now.execution.fenced_writes)
        .key("recovery").begin_object()
        .key("examined").value(now.execution.recovery.examined)
        .key("resubmitted").value(now.execution.recovery.resubmitted)
        .key("in_flight").value(now.execution.recovery.already_in_flight)
        .key("completed").value(now.execution.recovery.already_completed)
        .key("expired").value(now.execution.recovery.expired)
        .end_object();
    write_worker(json, now.execution.worker, before ? &before->execution.worker : nullptr, rates);
    json.end_object();

    const auto& api = now.api;
    const auto* old_monitor = before ? &before->position_monitor : nullptr;
    json.key("api").begin_object()
        .key("accepted").value(api.accepted)
        .key("request_rate").value(rates.per_second(api.accepted, earlier(before, [](const SystemSnapshot& s) { return s.api.accepted; })))
        .key("completed").value(api.completed)
        .key("failed").value(api.failed)
        .key("cancelled").value(api.cancelled)
        .key("retries").value(api.retries)
        .key("rejected_full").value(api.rejected_full)
        .key("rejected_shutdown").value(api.rejected_shutdown)
        .key("rate_limited").value(api.rate_limited_responses)
        .key("timeouts").value(api.timeouts)
        .key("connection_failures").value(api.connection_failures)
        .key("outstanding").value(api.outstanding)
        .key("pending").value(api.pending)
        .key("active").value(api.active)
        .key("max_pending").value(api.max_pending)
        .key("capacity").value(api.capacity)
        .key("io_threads").value(api.io_threads)
        .key("window_used").value(api.limiter.in_window)
        .key("window_limit").value(api.limiter.max_requests)
        .key("window_ms").value(api.limiter.window.count())
        .key("paused_ms").value(api.limiter.paused_for.count())
        .key("mock_calls").value(now.mock_api_calls)
        .key("mock_latency_ms").value(now.mock_api_latency.count())
        .key("mock_429_every").value(now.mock_api_429_every)
        .key("queue_delay");
    write_latency(json, now.position_monitor.api_queue, old_monitor ? &old_monitor->api_queue : nullptr);
    json.key("limiter_wait");
    write_latency(json, now.position_monitor.rate_limit_wait, old_monitor ? &old_monitor->rate_limit_wait : nullptr);
    json.key("service");
    write_latency(json, now.position_monitor.api_service, old_monitor ? &old_monitor->api_service : nullptr);
    json.end_object();

    json.key("position_monitor").begin_object()
        .key("name").value(now.position_monitor.worker.name)
        .key("fills").value(now.position_monitor.fills)
        .key("rejections").value(now.position_monitor.rejections)
        .key("duplicates").value(now.position_monitor.duplicates);
    write_worker(json, now.position_monitor.worker, old_monitor ? &old_monitor->worker : nullptr, rates);
    json.key("end_to_end");
    write_latency(json, now.position_monitor.end_to_end, old_monitor ? &old_monitor->end_to_end : nullptr);
    json.end_object();

    std::map<std::string, double> marks;
    if (now.activity) {
        for (const auto& instrument : now.activity->instruments) {
            marks[instrument.symbol] = instrument.last_price;
        }
    }
    json.key("positions").begin_array();
    for (const auto& position : now.positions) {
        json.begin_object()
            .key("strategy").value(position.strategy)
            .key("symbol").value(position.symbol)
            .key("quantity").value(position.quantity)
            .key("fills").value(position.fills)
            .key("pnl");
        const auto mark = marks.find(position.symbol);
        if (mark != marks.end()) {
            json.value(position.cash + position.quantity * mark->second);
        } else {
            json.null();
        }
        json.end_object();
    }
    json.end_array();

    json.key("routes").begin_array();
    for (const auto& route : now.routes) {
        json.begin_object()
            .key("event").value(route.event_type)
            .key("topic").value(route.topic)
            .key("subscriber").value(route.subscriber)
            .end_object();
    }
    json.end_array();

    if (now.activity) {
        const auto& activity = *now.activity;
        const auto* old_activity = before && before->activity ? &before->activity->worker : nullptr;
        json.key("recorder").begin_object().key("name").value(activity.worker.name);
        write_worker(json, activity.worker, old_activity, rates);
        json.end_object();

        json.key("prices").begin_array();
        for (const auto& instrument : activity.instruments) {
            json.begin_object()
                .key("symbol").value(instrument.symbol)
                .key("last").value(instrument.last_price)
                .key("updates").value(instrument.updates)
                .key("t").begin_array();
            for (const auto& point : instrument.history) {
                json.value(point.time_s);
            }
            json.end_array().key("p").begin_array();
            for (const auto& point : instrument.history) {
                json.value(point.price);
            }
            json.end_array().end_object();
        }
        json.end_array();

        json.key("tape").begin_array();
        for (const auto& entry : activity.tape) {
            const char* kind = entry.kind == TapeEntry::Kind::signal        ? "signal"
                               : entry.kind == TapeEntry::Kind::fill        ? "fill"
                               : entry.kind == TapeEntry::Kind::supervision ? "supervision"
                                                                            : "rejection";
            json.begin_object()
                .key("kind").value(kind)
                .key("t").value(entry.time_s)
                .key("strategy").value(entry.strategy)
                .key("symbol").value(entry.symbol)
                .key("side").value(to_string(entry.side))
                .key("qty").value(entry.quantity)
                .key("price").value(entry.price)
                .key("latency_ms").value(entry.latency_ms)
                .key("detail").value(entry.detail)
                .end_object();
        }
        json.end_array();
    }

    json.key("resilience").begin_object()
        .key("supervised").value(now.supervised)
        .key("probes").value(now.supervisor.probes)
        .key("faults").value(now.supervisor.faults)
        .key("restarts").value(now.supervisor.restarts)
        .key("quarantines").value(now.supervisor.quarantines)
        .key("zombies").value(now.supervisor.zombies)
        .key("buried").value(now.supervisor.buried)
        .key("escalated").value(now.supervisor.escalated)
        .key("escalation").value(now.supervisor.escalation)
        .key("components").begin_array();
    for (const auto& component : now.components) {
        json.begin_object()
            .key("name").value(component.name)
            .key("kind").value(component.kind)
            .key("state").value(to_string(component.state))
            .key("generation").value(component.generation)
            .key("restarts").value(component.restarts)
            .key("last_cause").value(component.last_cause)
            .key("last_error").value(component.last_error)
            .key("detail").value(component.detail)
            .key("last_detect_ms").value(component.last_detect_ms)
            .key("last_recovery_ms").value(component.last_recovery_ms)
            .key("down_for_ms").value(component.down_for_ms)
            .key("recovered_records").value(component.recovered_records)
            .key("dead_letters").value(component.dead_letters)
            .key("fenced_writes").value(component.fenced_writes)
            .end_object();
    }
    json.end_array().key("log").begin_array();
    for (const auto& entry : now.supervision_log) {
        json.begin_object()
            .key("t").value(entry.time_s)
            .key("component").value(entry.component)
            .key("action").value(entry.action)
            .key("detail").value(entry.detail)
            .key("generation").value(entry.generation)
            .key("recovery_ms").value(entry.recovery_ms)
            .end_object();
    }
    json.end_array().end_object();

    json.key("gateways").begin_object()
        .key("reroutes").value(now.gateway_pool.reroutes)
        .key("reroutes_denied").value(now.gateway_pool.reroutes_denied)
        .key("retry_budget").value(now.gateway_pool.retry_budget)
        .key("healthy").value(now.gateway_pool.healthy)
        .key("members").begin_array();
    for (const auto& gateway : now.gateways) {
        json.begin_object()
            .key("name").value(gateway.name)
            .key("state").value(to_string(gateway.state))
            .key("preferred").value(gateway.preferred)
            .key("down").value(gateway.down_injected)
            .key("submitted").value(gateway.submitted)
            .key("succeeded").value(gateway.succeeded)
            .key("failed").value(gateway.failed)
            .key("rerouted").value(gateway.rerouted_away)
            .key("ejections").value(gateway.ejections)
            .key("consecutive_failures").value(gateway.consecutive_failures)
            .key("reopens_in_ms").value(gateway.reopens_in.count())
            .key("pending").value(gateway.api.pending)
            .key("active").value(gateway.api.active)
            .key("window_used").value(gateway.api.limiter.in_window)
            .key("window_limit").value(gateway.api.limiter.max_requests)
            .key("paused_ms").value(gateway.api.limiter.paused_for.count())
            .end_object();
    }
    json.end_array().end_object();

    if (now.journal) {
        const auto& journal = *now.journal;
        json.key("journal").begin_object()
            .key("directory").value(journal.directory)
            .key("session").value(journal.session)
            .key("records").value(journal.records)
            .key("rate").value(rates.per_second(journal.records,
                                                earlier(before, [](const SystemSnapshot& s) {
                                                    return s.journal ? s.journal->records : 0U;
                                                })))
            .key("bytes").value(journal.bytes)
            .key("segments").value(journal.segments)
            .key("gaps").value(journal.gaps)
            .key("gap_events").value(journal.gap_events)
            .key("flushes").value(journal.flushes)
            .key("fsyncs").value(journal.fsyncs)
            .key("last_sequence").value(journal.last_sequence)
            .key("tap_depth").value(journal.tap_depth)
            .key("tap_capacity").value(journal.tap_capacity)
            .key("tap_rejected").value(journal.tap_rejected)
            .key("repaired_bytes").value(journal.repaired_bytes)
            .key("quarantined").value(journal.quarantined_segments)
            .end_object();
    }
    if (now.replay) {
        json.key("replay");
        write_replay(json, *now.replay);
    }
    if (view.other_replay) {
        json.key("other_replay");
        write_replay(json, *view.other_replay);
    }

    json.end_object();
    previous_ = now;
    return json.take();
}

}  // namespace trading
