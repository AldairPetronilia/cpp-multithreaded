#pragma once

#include "trading/events.hpp"

#include <chrono>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace testing_support {

using namespace std::chrono_literals;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

#define CHECK(expression)                                                                  \
    do {                                                                                   \
        if (!(expression)) {                                                               \
            std::ostringstream failure_message;                                            \
            failure_message << __FILE__ << ':' << __LINE__ << ": check failed: "          \
                            << #expression;                                                \
            throw testing_support::TestFailure(failure_message.str());                     \
        }                                                                                  \
    } while (false)

// Polls a condition in tests only; production code waits on condition variables instead.
inline bool eventually(const std::function<bool()>& condition, std::chrono::milliseconds timeout) {
    const auto deadline = trading::MonotonicClock::now() + timeout;
    while (!condition()) {
        if (trading::MonotonicClock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

}  // namespace testing_support
