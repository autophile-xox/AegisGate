#pragma once

#include "aegisgate/config.hpp"

#include <cstdint>

namespace aegisgate {

enum class CircuitState { closed, open, half_open };
enum class AttemptOutcome { success, cancelled, upstream_5xx, transport_error, invalid_response, timeout };

[[nodiscard]] std::string_view name(CircuitState state);
[[nodiscard]] std::string_view name(AttemptOutcome outcome);

class CircuitBreaker {
public:
    using Time = std::chrono::steady_clock::time_point;

    struct Permit {
        std::uint64_t generation;
    };

    explicit CircuitBreaker(CircuitSettings settings);

    [[nodiscard]] CircuitState state() const { return state_; }
    [[nodiscard]] bool available(Time now) const;
    [[nodiscard]] std::optional<Permit> acquire(Time now);
    void complete(Permit permit, AttemptOutcome outcome, Time now);

private:
    void transition(CircuitState next, Time now);

    CircuitSettings settings_;
    std::vector<std::uint8_t> outcomes_;
    CircuitState state_{CircuitState::closed};
    Time retry_at_{};
    std::uint64_t generation_{};
    std::size_t events_{};
    std::size_t failures_{};
    std::size_t cursor_{};
    std::size_t probes_{};
    std::size_t successes_{};
};

}  // namespace aegisgate
