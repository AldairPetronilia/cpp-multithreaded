#include "trading/session.hpp"

#include <charconv>
#include <cstdlib>
#include <stdexcept>

namespace trading {
namespace {

ControlResult failure(int status, std::string_view message) {
    std::string body = R"({"ok":false,"error":")";
    for (const char character : message) {
        if (character == '"' || character == '\\') {
            body += '\\';
        }
        body += character == '\n' ? ' ' : character;
    }
    body += "\"}";
    return {status, std::move(body)};
}

std::int64_t parse_integer(std::string_view value) {
    std::int64_t parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || error != std::errc{} || end != value.data() + value.size() || parsed < 0) {
        throw std::invalid_argument("control value must be a non-negative integer");
    }
    return parsed;
}

double parse_real(std::string_view value) {
    const std::string owned(value);
    char* end = nullptr;
    const auto parsed = std::strtod(owned.c_str(), &end);
    if (owned.empty() || end != owned.c_str() + owned.size() || parsed <= 0.0) {
        throw std::invalid_argument("control value must be a positive number");
    }
    return parsed;
}

}  // namespace

LiveSession::LiveSession(SystemConfig live_config) : live_(std::make_unique<TradingSystem>(std::move(live_config))) {}

LiveSession::~LiveSession() {
    stop();
}

void LiveSession::start() {
    live_->start();
}

void LiveSession::stop() {
    stop_replay();
    live_->stop();
}

TradingSystem* LiveSession::replay() noexcept {
    return replay_.get();
}

TradingSystem* LiveSession::replay_target() {
    if (replay_) {
        return replay_.get();
    }
    return live_->is_replay() ? live_.get() : nullptr;
}

std::string LiveSession::render() {
    std::lock_guard render_lock(render_mutex_);
    std::shared_lock lock(replay_mutex_);
    if (replay_ && show_replay_.load(std::memory_order_relaxed)) {
        ViewInfo info;
        info.view = "replay";
        info.live_available = true;
        return replay_renderer_.render(replay_->snapshot(), info);
    }
    ViewInfo info;
    info.view = live_->is_replay() ? "replay" : "live";
    info.can_replay = live_->journal() != nullptr;
    if (replay_) {
        info.other_replay = replay_->snapshot().replay;
    }
    return live_renderer_.render(live_->snapshot(), info);
}

ControlResult LiveSession::start_replay(std::string_view mode, std::string_view speed) {
    if (live_->is_replay()) {
        return failure(409, "this process is already a replay");
    }
    auto* journal = live_->journal();
    if (journal == nullptr) {
        return failure(409, "no journal is being recorded; start with --journal DIR");
    }
    ReplayOptions options;
    if (mode == "rederive") {
        options.mode = ReplayMode::rederive;
    } else if (mode == "playback") {
        options.mode = ReplayMode::playback;
    } else {
        return failure(400, "replay mode must be rederive or playback");
    }
    if (speed == "max" || speed.empty()) {
        options.pacing = ReplayPacing::lockstep;
    } else {
        options.speed = parse_real(speed);
    }
    // Everything published so far must be readable, and the replay stops where the
    // recording stands now.
    journal->sync();
    options.directory = journal->directory();

    auto base = live_->config();
    base.session = live_->session();
    auto config = SystemConfig::for_replay(base, std::move(options));
    try {
        std::unique_lock lock(replay_mutex_);
        if (replay_) {
            replay_->stop();
            replay_.reset();
        }
        replay_ = std::make_unique<TradingSystem>(std::move(config));
        replay_->start();
        show_replay_.store(true, std::memory_order_relaxed);
    } catch (const std::exception& exception) {
        return failure(409, exception.what());
    }
    return {};
}

void LiveSession::stop_replay() {
    std::unique_lock lock(replay_mutex_);
    show_replay_.store(false, std::memory_order_relaxed);
    if (replay_) {
        replay_->stop();
        replay_.reset();
    }
}

ControlResult LiveSession::apply_control(std::string_view name, std::string_view value) {
    // Controls are addressed as "<control>" or "<control>:<target>", e.g. "feed.paused:fx".
    const auto separator = name.find(':');
    const auto control = name.substr(0, separator);
    const std::string target(separator == std::string_view::npos ? "*" : name.substr(separator + 1));

    try {
        bool matched = true;
        if (control == "feed.interval_us") {
            matched = live_->set_feed_interval(target, std::chrono::microseconds(parse_integer(value)));
        } else if (control == "feed.paused") {
            matched = live_->set_feed_paused(target, parse_integer(value) != 0);
        } else if (control == "strategy.work_us") {
            matched = live_->set_strategy_work(target, std::chrono::microseconds(parse_integer(value)));
        } else if (control == "api.latency_ms") {
            live_->set_api_latency(std::chrono::milliseconds(parse_integer(value)));
        } else if (control == "api.inject_429_every") {
            live_->set_api_429_every(static_cast<std::size_t>(parse_integer(value)));
        } else if (control == "fault.crash") {
            matched = live_->inject_fault(target, Fault::crash);
        } else if (control == "fault.hang") {
            const auto hang = value.empty() ? 8'000 : parse_integer(value);
            matched = live_->inject_fault(target, Fault::hang, std::chrono::milliseconds(hang));
        } else if (control == "fault.poison") {
            matched = live_->inject_fault(target, Fault::poison);
        } else if (control == "fault.gateway") {
            matched = live_->set_gateway_down(static_cast<std::size_t>(parse_integer(target)), parse_integer(value) != 0);
        } else if (control == "replay.start") {
            return start_replay(target, value);
        } else if (control == "replay.stop") {
            stop_replay();
        } else if (control == "view.show") {
            std::shared_lock lock(replay_mutex_);
            if (value == "replay" && replay_) {
                show_replay_.store(true, std::memory_order_relaxed);
            } else if (value == "live") {
                show_replay_.store(false, std::memory_order_relaxed);
            } else {
                matched = false;
            }
        } else if (control == "replay.pause" || control == "replay.speed" || control == "replay.seek") {
            std::shared_lock lock(replay_mutex_);
            auto* system = replay_target();
            if (system == nullptr) {
                return failure(409, "no replay is running");
            }
            if (control == "replay.pause") {
                matched = system->replay_set_paused(parse_integer(value) != 0);
            } else if (control == "replay.speed") {
                // A numeric speed paces by the recording; "max" runs in lockstep instead.
                matched = system->replay_set_speed(value == "max" ? 1.0 : parse_real(value)) &&
                          system->replay_set_pacing(value == "max" ? ReplayPacing::lockstep : ReplayPacing::realtime);
            } else {
                matched = system->replay_seek(static_cast<double>(parse_integer(value)) / 1'000.0);
            }
        } else {
            return failure(404, "unknown control");
        }
        return matched ? ControlResult{} : failure(404, "unknown target");
    } catch (const std::exception& exception) {
        return failure(400, exception.what());
    }
}

}  // namespace trading
