#include "aegisgate/resilience/circuit_breaker.hpp"

#include <gtest/gtest.h>

namespace {

using namespace aegisgate;
using namespace std::chrono_literals;

using Outcome = AttemptOutcome;
using State = CircuitState;
constexpr CircuitBreaker::Time now{};

TEST(CircuitBreaker, RequiresMinimumSamplesAndFailureRatio) {
    CircuitBreaker breaker({true, 4, 3, 67, 50ms, 1, 2});
    for (const auto result : {Outcome::upstream_5xx, Outcome::timeout, Outcome::cancelled}) {
        breaker.complete(*breaker.acquire(now), result, now);
        EXPECT_EQ(breaker.state(), State::closed);
    }
    breaker.complete(*breaker.acquire(now), Outcome::success, now);
    EXPECT_EQ(breaker.state(), State::closed);
    breaker.complete(*breaker.acquire(now), Outcome::transport_error, now);
    EXPECT_EQ(breaker.state(), State::open);
    EXPECT_FALSE(breaker.acquire(now + 49ms));
    EXPECT_TRUE(breaker.acquire(now + 50ms));
    EXPECT_EQ(breaker.state(), State::half_open);
}

TEST(CircuitBreaker, EvictsOldOutcomesFromRollingWindow) {
    CircuitBreaker breaker({true, 3, 3, 67, 50ms, 1, 2});
    for (const auto result : {Outcome::upstream_5xx, Outcome::success, Outcome::success,
                              Outcome::timeout, Outcome::timeout}) {
        breaker.complete(*breaker.acquire(now), result, now);
        EXPECT_EQ(breaker.state(), State::closed);
    }
    breaker.complete(*breaker.acquire(now), Outcome::invalid_response, now);
    EXPECT_EQ(breaker.state(), State::open);
}

TEST(CircuitBreaker, BoundsProbationAndRequiresMultipleSuccesses) {
    CircuitBreaker breaker({true, 1, 1, 100, 50ms, 2, 3});
    const auto pre_open = *breaker.acquire(now);
    breaker.complete(*breaker.acquire(now), Outcome::upstream_5xx, now);
    const auto first = *breaker.acquire(now + 50ms);
    const auto second = *breaker.acquire(now + 50ms);
    EXPECT_FALSE(breaker.available(now + 50ms));
    breaker.complete(pre_open, Outcome::timeout, now + 51ms);
    EXPECT_EQ(breaker.state(), State::half_open);

    breaker.complete(first, Outcome::cancelled, now + 52ms);
    const auto replacement = *breaker.acquire(now + 52ms);
    breaker.complete(second, Outcome::success, now + 53ms);
    const auto last = *breaker.acquire(now + 54ms);
    breaker.complete(replacement, Outcome::success, now + 55ms);
    EXPECT_FALSE(breaker.available(now + 55ms));
    breaker.complete(last, Outcome::success, now + 56ms);
    EXPECT_EQ(breaker.state(), State::closed);
    EXPECT_TRUE(breaker.available(now + 56ms));
}

TEST(CircuitBreaker, AFailedProbeReopensAndInvalidatesOtherProbes) {
    CircuitBreaker breaker({true, 1, 1, 100, 50ms, 2, 2});
    breaker.complete(*breaker.acquire(now), Outcome::transport_error, now);
    const auto failed = *breaker.acquire(now + 50ms);
    const auto late = *breaker.acquire(now + 50ms);
    breaker.complete(failed, Outcome::timeout, now + 52ms);
    breaker.complete(late, Outcome::success, now + 53ms);
    EXPECT_EQ(breaker.state(), State::open);
    EXPECT_FALSE(breaker.available(now + 101ms));
    EXPECT_TRUE(breaker.available(now + 102ms));
}

TEST(CircuitBreaker, DisabledCircuitDoesNotRejectOnFailures) {
    CircuitBreaker breaker({false, 1, 1, 100, 50ms, 1, 1});
    for (int count = 0; count < 20; ++count) {
        breaker.complete(*breaker.acquire(now), Outcome::timeout, now);
        EXPECT_EQ(breaker.state(), State::closed);
    }
}

}  // namespace
