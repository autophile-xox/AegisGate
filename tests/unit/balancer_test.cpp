#include "aegisgate/routing/balancer.hpp"

#include <array>
#include <gtest/gtest.h>

namespace {

using namespace aegisgate;
using namespace std::chrono_literals;

Config group_config(BalancePolicy policy) {
    Config config;
    config.backends.resize(3);
    config.groups.push_back({"api", policy, {0, 1, 2}});
    config.latency = {100ms, 300ms, 10ms};
    return config;
}

void sample(Balancer& balancer, std::size_t backend, Balancer::Clock::duration duration,
            Balancer::Time now) {
    balancer.begin(backend);
    balancer.finish(backend, duration, now);
}

TEST(Balancer, RoundRobinCyclesOnlyThroughEligibleEndpoints) {
    const auto config = group_config(BalancePolicy::round_robin);
    Balancer balancer(config, 1);
    constexpr std::array<std::size_t, 2> eligible{0, 2};
    EXPECT_EQ(balancer.choose(0, eligible, {}), 0U);
    EXPECT_EQ(balancer.choose(0, eligible, {}), 2U);
    EXPECT_EQ(balancer.choose(0, eligible, {}), 0U);
}

TEST(Balancer, PeakAndActiveLoadChangeThePreferredEndpoint) {
    const auto config = group_config(BalancePolicy::peak_ewma);
    Balancer balancer(config, 1);
    constexpr std::array<std::size_t, 2> eligible{0, 1};
    const Balancer::Time now{};
    sample(balancer, 0, 2ms, now);
    sample(balancer, 1, 3ms, now);
    EXPECT_EQ(balancer.choose(0, eligible, now), 0U);
    balancer.begin(0);
    EXPECT_EQ(balancer.choose(0, eligible, now), 1U);
    balancer.finish(0, std::nullopt, now);
    EXPECT_EQ(balancer.choose(0, eligible, now), 0U);
    sample(balancer, 0, 30ms, now + 1ms);
    sample(balancer, 0, 1ms, now + 2ms);
    EXPECT_EQ(balancer.choose(0, eligible, now + 2ms), 1U);
}

TEST(Balancer, StaleEstimateIsReplacedByTheNextSample) {
    const auto config = group_config(BalancePolicy::peak_ewma);
    Balancer balancer(config, 1);
    constexpr std::array<std::size_t, 2> eligible{0, 2};
    const Balancer::Time now{};
    sample(balancer, 0, 40ms, now);
    sample(balancer, 2, 5ms, now + 299ms);
    EXPECT_EQ(balancer.choose(0, eligible, now + 300ms), 2U);
    sample(balancer, 0, 1ms, now + 300ms);
    EXPECT_EQ(balancer.choose(0, eligible, now + 300ms), 0U);
}

TEST(Balancer, AnIdleUnsampledEndpointReceivesAnExploratoryRequest) {
    const auto config = group_config(BalancePolicy::peak_ewma);
    Balancer balancer(config, 1);
    constexpr std::array<std::size_t, 2> eligible{0, 1};
    const Balancer::Time now{};
    sample(balancer, 0, 1ms, now);
    for (int i = 0; i < 15; ++i) {
        EXPECT_EQ(balancer.choose(0, eligible, now), 0U);
    }
    EXPECT_EQ(balancer.choose(0, eligible, now), 1U);
}

}  // namespace
