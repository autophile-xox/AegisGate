#pragma once

#include "aegisgate/config.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>

namespace aegisgate {

using Counter = std::atomic<std::uint64_t>;

class Histogram {
public:
    static constexpr std::array<std::uint64_t, 14> bounds_us{
        100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000,
        500000, 1000000, 5000000};
    struct Snapshot {
        std::array<std::uint64_t, bounds_us.size() + 1> bins{};
        std::uint64_t sum_us{};
    };
    void observe(std::chrono::steady_clock::duration duration);
    void add_to(Snapshot& result) const;

private:
    std::array<Counter, bounds_us.size() + 1> bins_{};
    Counter sum_us_{};
};

struct BackendMetrics {
    Histogram duration;
    std::array<Counter, 4> failures{};
    std::array<Counter, 3> transitions{};
    Counter circuit{};
    Counter healthy{1};
    Counter active{};
    Counter connections{};
    Counter idle{};
};

struct WorkerMetrics {
    WorkerMetrics(std::size_t routes, std::size_t backends);
    Counter requests{};
    Counter successful{};
    Counter rejected{};
    Counter cancelled{};
    Counter connections{};
    Counter upstream_pending{};
    Counter refill_pending{};
    std::array<Counter, 600> statuses{};
    std::array<Counter, 8> rate_rejections{};
    Histogram duration;
    std::unique_ptr<Histogram[]> route_duration;
    std::unique_ptr<BackendMetrics[]> backend;
};

struct RedisMetrics {
    Counter commands{};
    Counter connected{};
    Counter queued{};
    Counter refills{};
    Counter granted{};
    Counter spent{};
    Counter fallback{};
    Counter fail_open{};
    Counter local_shortage{};
    std::array<Counter, 8> outcomes{};
    Histogram duration;
};

class Metrics {
public:
    explicit Metrics(const Config& config);
    ~Metrics();
    Metrics(const Metrics&) = delete;
    Metrics& operator=(const Metrics&) = delete;
    [[nodiscard]] WorkerMetrics& worker(std::size_t index) { return *workers_[index]; }
    [[nodiscard]] std::string scrape() const;
    void set_ready(bool ready) { ready_.store(ready, std::memory_order_relaxed); }
    [[nodiscard]] bool ready() const { return ready_.load(std::memory_order_relaxed); }
    RedisMetrics redis;

private:
    class Listener;
    const Config& config_;
    std::vector<std::unique_ptr<WorkerMetrics>> workers_;
    std::atomic<bool> ready_{};
    std::unique_ptr<Listener> listener_;
};

}  // namespace aegisgate
