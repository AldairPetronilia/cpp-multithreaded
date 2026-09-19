#pragma once

#include "trading/system.hpp"

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace trading {

// Minimal streaming JSON writer for telemetry: escapes strings correctly and writes
// non-finite numbers as null. Structural misuse (e.g. a value without a key inside an
// object) is the caller's responsibility.
class JsonWriter {
public:
    JsonWriter& begin_object();
    JsonWriter& end_object();
    JsonWriter& begin_array();
    JsonWriter& end_array();
    JsonWriter& key(std::string_view name);

    JsonWriter& value(std::string_view text);
    JsonWriter& value(const std::string& text) { return value(std::string_view(text)); }
    // Without this overload a string literal would convert to bool.
    JsonWriter& value(const char* text) { return value(std::string_view(text)); }
    JsonWriter& value(bool flag);
    JsonWriter& value(double number);
    JsonWriter& null();

    template <std::integral Integer>
        requires(!std::same_as<Integer, bool>)
    JsonWriter& value(Integer number) {
        before_value();
        out_ += std::to_string(number);
        return *this;
    }

    [[nodiscard]] std::string take() { return std::move(out_); }

private:
    void before_value();
    void write_string(std::string_view text);

    std::string out_;
    std::vector<bool> scope_has_items_;
    bool after_key_{false};
};

// Renders system snapshots as the dashboard's JSON document. It keeps the previous
// snapshot so it can add per-second rates and latency percentiles for the last interval
// alongside cumulative totals. Not thread-safe: call it from one thread.
class TelemetryRenderer {
public:
    [[nodiscard]] std::string render(const SystemSnapshot& snapshot);

private:
    std::optional<SystemSnapshot> previous_;
};

}  // namespace trading
