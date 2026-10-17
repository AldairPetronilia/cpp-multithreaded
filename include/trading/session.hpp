#pragma once

#include "trading/control.hpp"
#include "trading/system.hpp"
#include "trading/telemetry.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace trading {

// What the dashboard talks to: the live system, and optionally a replay system running
// beside it in the same process. The replay reads the live journal and has its own bus,
// strategies and mock broker, so it can run while the live system carries on, and what the
// dashboard shows can be switched between the two.
//
// Controls are addressed as "<control>" or "<control>:<target>":
//   feed.interval_us:<feed|*>  feed.paused:<feed|*>  strategy.work_us:<strategy|*>
//   api.latency_ms  api.inject_429_every
//   fault.crash:<component>  fault.hang:<component> (value: ms, default 8000)
//   fault.poison:<component>  fault.gateway:<index> (value: 1 down, 0 up)
//   replay.start:<rederive|playback> (value: speed, or "max" to run in lockstep)
//   replay.pause  replay.speed (number or "max")  replay.seek (value: 0-1000)  replay.stop
//   view.show (value: live | replay)
class LiveSession {
public:
    explicit LiveSession(SystemConfig live_config);
    ~LiveSession();
    LiveSession(const LiveSession&) = delete;
    LiveSession& operator=(const LiveSession&) = delete;

    void start();
    void stop();

    [[nodiscard]] TradingSystem& live() noexcept { return *live_; }
    [[nodiscard]] const TradingSystem& live() const noexcept { return *live_; }
    [[nodiscard]] bool feeds_finished() const { return live_->feeds_finished(); }

    // The JSON document for the view currently shown. Call from one thread at a time.
    [[nodiscard]] std::string render();
    [[nodiscard]] ControlResult apply_control(std::string_view name, std::string_view value);

    // The replay system, if one exists, for tests. Valid until the replay is stopped.
    [[nodiscard]] TradingSystem* replay() noexcept;

private:
    ControlResult start_replay(std::string_view mode, std::string_view speed);
    void stop_replay();
    [[nodiscard]] TradingSystem* replay_target();

    std::unique_ptr<TradingSystem> live_;
    mutable std::shared_mutex replay_mutex_;
    std::unique_ptr<TradingSystem> replay_;
    std::atomic<bool> show_replay_{false};
    std::mutex render_mutex_;
    TelemetryRenderer live_renderer_;
    TelemetryRenderer replay_renderer_;
};

}  // namespace trading
