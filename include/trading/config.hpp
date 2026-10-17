#pragma once

#include "trading/system.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace trading {

struct AppConfig {
    SystemConfig system{SystemConfig::demo()};
    bool dashboard{false};
    std::uint16_t dashboard_port{8080};
    std::chrono::milliseconds dashboard_interval{200};
    // Stop after this long even if the feeds have more to publish.
    std::optional<std::chrono::seconds> duration{};
    bool show_help{false};
    // --replay: the process replays a recorded session instead of generating quotes.
    bool replay{false};

    static AppConfig from_command_line(int argc, char* argv[]);
    [[nodiscard]] static std::string usage(const char* executable_name);
};

}  // namespace trading
