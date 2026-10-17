#pragma once

#include "trading/control.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace trading {

struct DashboardServerConfig {
    // Loopback only: the dashboard exposes controls and has no authentication.
    std::string bind_address{"127.0.0.1"};
    // 0 binds an ephemeral port; port() reports the one chosen.
    std::uint16_t port{8080};
    std::chrono::milliseconds publish_interval{200};
    std::size_t max_clients{32};
};

// Minimal single-threaded HTTP/1.1 server for the live dashboard (POSIX sockets, poll).
//   GET  /                 the dashboard page
//   GET  /events           Server-Sent Events stream of JSON snapshots
//   GET  /api/snapshot     the latest snapshot
//   POST /api/control?name=..&value=..
// One snapshot is rendered per interval and shared by every client. A client whose
// previous snapshot is still unsent skips the new one rather than growing a buffer, the
// same bounded-backpressure policy the trading pipeline uses.
class DashboardServer {
public:
    using SnapshotProvider = std::function<std::string()>;
    using ControlHandler = std::function<ControlResult(std::string_view name, std::string_view value)>;

    DashboardServer(DashboardServerConfig config,
                    std::string page,
                    SnapshotProvider snapshot,
                    ControlHandler control);
    ~DashboardServer();
    DashboardServer(const DashboardServer&) = delete;
    DashboardServer& operator=(const DashboardServer&) = delete;

    // Binds and starts the server thread; throws std::system_error if binding fails.
    void start();
    void stop();
    [[nodiscard]] std::uint16_t port() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// The dashboard page (app/dashboard.html), embedded in the binary at build time.
[[nodiscard]] std::string_view embedded_dashboard_page() noexcept;

}  // namespace trading
