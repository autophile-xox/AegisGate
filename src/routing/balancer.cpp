#include "aegisgate/routing/balancer.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>

namespace aegisgate {
namespace {

using Microseconds = std::chrono::duration<double, std::micro>;

}  // namespace

Balancer::Balancer(const Config& config, std::size_t seed)
    : config_(config), loads_(config.backends.size()), cursors_(config.groups.size()),
      random_(seed) {
    for (const auto& group : config.groups) {
        if (group.policy == BalancePolicy::peak_ewma) {
            for (const auto backend : group.backends) {
                loads_[backend].measure = true;
            }
        }
    }
}

std::size_t Balancer::choose(std::size_t group, std::span<const std::size_t> eligible, Time now) {
    assert(!eligible.empty());
    if (eligible.size() == 1) {
        return eligible.front();
    }
    if (config_.groups[group].policy == BalancePolicy::round_robin) {
        return eligible[cursors_[group]++ % eligible.size()];
    }

    const auto first = std::uniform_int_distribution<std::size_t>(0, eligible.size() - 1)(random_);
    auto second = std::uniform_int_distribution<std::size_t>(0, eligible.size() - 2)(random_);
    if (second >= first) {
        ++second;
    }
    const auto a = eligible[first];
    const auto b = eligible[second];
    if (++cursors_[group] % 16 == 0) {
        for (const auto backend : {a, b}) {
            const auto& load = loads_[backend];
            if (load.active == 0 &&
                (!load.last_sample || now - *load.last_sample >= config_.latency.stale)) {
                return backend;
            }
        }
    }
    return score(a, now) <= score(b, now) ? a : b;
}

void Balancer::begin(std::size_t backend) {
    ++loads_[backend].active;
}

void Balancer::finish(std::size_t backend, std::optional<Clock::duration> sample, Time now) {
    auto& load = loads_[backend];
    assert(load.active != 0);
    --load.active;
    if (!sample || !load.measure) {
        return;
    }
    const auto measured = std::max(1.0, Microseconds(*sample).count());
    if (!load.last_sample || now - *load.last_sample >= config_.latency.stale) {
        load.latency_us = measured;
    } else {
        const auto old = weight(now - *load.last_sample);
        // Rising latency is absorbed immediately. Falling latency is smoothed over time.
        load.latency_us = std::max(measured, old * load.latency_us + (1.0 - old) * measured);
    }
    load.last_sample = now;
}

void Balancer::reset_latency(std::size_t backend) {
    loads_[backend].last_sample.reset();
}

double Balancer::weight(Clock::duration age) const {
    return std::exp(-Microseconds(age).count() / Microseconds(config_.latency.decay).count());
}

double Balancer::score(std::size_t backend, Time now) const {
    const auto& load = loads_[backend];
    const auto initial = Microseconds(config_.latency.initial).count();
    double latency = initial;
    if (load.last_sample && now - *load.last_sample < config_.latency.stale) {
        latency += (load.latency_us - initial) * weight(now - *load.last_sample);
    }
    return latency * static_cast<double>(load.active + 1);
}

}  // namespace aegisgate
