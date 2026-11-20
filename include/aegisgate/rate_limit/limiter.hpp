#pragma once

#include "aegisgate/config.hpp"

#include <memory>

namespace aegisgate {

enum class RateDecision {
    allow, unauthorized, exhausted, pending, timeout, disconnected, invalid, fallback_exhausted
};

[[nodiscard]] std::string_view name(RateDecision decision);

class RateLimiter {
public:
    explicit RateLimiter(const RateSettings& settings);
    ~RateLimiter();
    RateLimiter(const RateLimiter&) = delete;
    RateLimiter& operator=(const RateLimiter&) = delete;

    [[nodiscard]] RateDecision admit(const Route& route, std::optional<std::string_view> api_key);

private:
    class State;
    std::unique_ptr<State> state_;
};

}  // namespace aegisgate
