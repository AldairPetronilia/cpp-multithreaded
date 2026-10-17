#pragma once

#include <string>

namespace trading {

// Outcome of a runtime control, as the dashboard receives it.
struct ControlResult {
    int status{200};
    std::string body{"{\"ok\":true}"};
};

}  // namespace trading
