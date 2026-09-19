#pragma once

#include "trading/api.hpp"
#include "trading/workers.hpp"

#include <string>

namespace trading {

struct AppConfig {
    MarketDataProducerConfig market_data;
    SignalProcessorConfig signal;
    OrderExecutionConfig execution;
    PositionMonitorConfig position_monitor;
    ApiManagerConfig api_manager;
    MockApiConfig mock_api;
    bool show_help{false};

    static AppConfig from_command_line(int argc, char* argv[]);
    [[nodiscard]] static std::string usage(const char* executable_name);
};

}  // namespace trading

