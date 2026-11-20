#pragma once

#include "aegisgate/config.hpp"

#include <random>
#include <span>

namespace aegisgate {

class Balancer {
public:
    using Clock = std::chrono::steady_clock;
    using Time = Clock::time_point;

    Balancer(const Config& config, std::size_t seed);
    [[nodiscard]] std::size_t choose(std::size_t group, std::span<const std::size_t> eligible,
                                     Time now);
    void begin(std::size_t backend);
    void finish(std::size_t backend, std::optional<Clock::duration> sample, Time now);

private:
    struct BackendLoad {
        std::optional<Time> last_sample;
        double latency_us{};
        std::size_t active{};
    };

    [[nodiscard]] double weight(Clock::duration age) const;
    [[nodiscard]] double score(std::size_t backend, Time now) const;

    const Config& config_;
    std::vector<BackendLoad> loads_;
    std::vector<std::size_t> cursors_;
    std::minstd_rand random_;
};

}  // namespace aegisgate
