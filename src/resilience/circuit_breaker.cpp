#include "aegisgate/resilience/circuit_breaker.hpp"

#include <cassert>

namespace aegisgate {

std::string_view name(CircuitState state) {
    switch (state) {
    case CircuitState::closed: return "closed";
    case CircuitState::open: return "open";
    case CircuitState::half_open: return "half_open";
    }
    return "unknown";
}

std::string_view name(AttemptOutcome outcome) {
    switch (outcome) {
    case AttemptOutcome::success: return "success";
    case AttemptOutcome::cancelled: return "cancelled";
    case AttemptOutcome::upstream_5xx: return "upstream_5xx";
    case AttemptOutcome::transport_error: return "transport_error";
    case AttemptOutcome::invalid_response: return "invalid_response";
    case AttemptOutcome::timeout: return "timeout";
    }
    return "unknown";
}

CircuitBreaker::CircuitBreaker(CircuitSettings settings)
    : settings_(settings), outcomes_(settings.enabled ? settings.window : 0) {}

bool CircuitBreaker::available(Time now) const {
    switch (state_) {
    case CircuitState::closed: return true;
    case CircuitState::open: return now >= retry_at_;
    case CircuitState::half_open:
        return probes_ < settings_.max_probes &&
               successes_ + probes_ < settings_.recovery_successes;
    }
    return false;
}

std::optional<CircuitBreaker::Permit> CircuitBreaker::acquire(Time now) {
    if (!available(now)) {
        return std::nullopt;
    }
    if (state_ == CircuitState::open) {
        transition(CircuitState::half_open, now);
    }
    if (state_ == CircuitState::half_open) {
        ++probes_;
    }
    return Permit{generation_};
}

void CircuitBreaker::complete(Permit permit, AttemptOutcome outcome, Time now) {
    if (!settings_.enabled || permit.generation != generation_) {
        return;
    }
    const bool cancelled = outcome == AttemptOutcome::cancelled;
    const bool failure = outcome != AttemptOutcome::success && !cancelled;
    if (state_ == CircuitState::half_open) {
        assert(probes_ != 0);
        --probes_;
        if (failure) {
            transition(CircuitState::open, now);
        } else if (!cancelled && ++successes_ == settings_.recovery_successes) {
            transition(CircuitState::closed, now);
        }
        return;
    }
    if (state_ != CircuitState::closed || cancelled) {
        return;
    }
    if (events_ == outcomes_.size()) {
        failures_ -= outcomes_[cursor_];
    } else {
        ++events_;
    }
    outcomes_[cursor_] = static_cast<std::uint8_t>(failure);
    cursor_ = (cursor_ + 1) % outcomes_.size();
    failures_ += static_cast<std::size_t>(failure);
    if (events_ >= settings_.min_samples && failures_ * 100 >= events_ * settings_.failure_percent) {
        transition(CircuitState::open, now);
    }
}

void CircuitBreaker::transition(CircuitState next, Time now) {
    state_ = next;
    ++generation_;
    events_ = failures_ = cursor_ = probes_ = successes_ = 0;
    if (next == CircuitState::open) {
        retry_at_ = now + settings_.cooldown;
    }
}

}  // namespace aegisgate
