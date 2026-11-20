#include "aegisgate/rate_limit/limiter.hpp"
#include "redis.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <iostream>
#include <mutex>
#include <poll.h>
#include <sys/eventfd.h>
#include <thread>

namespace aegisgate {
namespace {

using Clock = RedisConnection::Clock;
using Time = RedisConnection::Time;
using namespace std::chrono_literals;

struct Lease {
    std::size_t tokens{};
    Time expires{};
};

struct Job {
    std::size_t pool{};
    std::uint64_t generation{};
    std::size_t count{};
    Time created{};
};

struct Pool {
    std::mutex mutex;
    std::array<Lease, 2> leases{};
    std::uint64_t generation{1};
    bool pending{};
    bool empty_at_redis{};
    RateDecision error{RateDecision::allow};
    Time retry_at{};
    std::size_t fallback_units{};
    Time fallback_updated{};
    std::uint64_t granted{};
    std::uint64_t spent{};
    std::uint64_t expired{};
    std::uint64_t fallback_spent{};
    std::uint64_t fail_open{};

    void expire(Time now) {
        for (auto& lease : leases) {
            if (lease.tokens && lease.expires <= now) {
                expired += std::exchange(lease.tokens, 0);
            }
        }
    }

    [[nodiscard]] std::size_t tokens() const { return leases[0].tokens + leases[1].tokens; }

    void spend() {
        const auto older = leases[0].tokens && (!leases[1].tokens || leases[0].expires <= leases[1].expires);
        --leases[older ? 0 : 1].tokens;
        ++spent;
    }
};

enum class Source { lease, fallback, open, unavailable };

}  // namespace

std::string_view name(RateDecision decision) {
    switch (decision) {
    case RateDecision::allow: return "allowed";
    case RateDecision::unauthorized: return "invalid_api_key";
    case RateDecision::exhausted: return "quota_exhausted";
    case RateDecision::pending: return "refill_pending";
    case RateDecision::timeout: return "redis_timeout";
    case RateDecision::disconnected: return "redis_disconnected";
    case RateDecision::invalid: return "redis_invalid_response";
    case RateDecision::fallback_exhausted: return "fallback_exhausted";
    }
    return "unknown";
}

class RateLimiter::State {
public:
    explicit State(const RateSettings& settings)
        : config_(settings), queue_(settings.queue),
          ready_(checked_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC), "rate queue eventfd")),
          stop_(checked_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC), "rate stop eventfd")) {
        for (std::size_t index = 0; index < config_.quotas.size(); ++index) {
            auto pool = std::make_unique<Pool>();
            pool->fallback_units = config_.fallback_capacity * 1000;
            pool->fallback_updated = Clock::now();
            pools_.push_back(std::move(pool));
        }
        thread_ = std::jthread([this] { synchronize(); });
    }

    ~State() {
        stopping_.store(true, std::memory_order_relaxed);
        notify_event(stop_.get());
        for (auto& pool : pools_) {
            std::lock_guard lock(pool->mutex);
            ++pool->generation;
            pool->pending = false;
        }
        thread_.join();
        if (config_.diagnostics) {
            const auto now = Clock::now();
            for (auto& pool : pools_) {
                pool->expire(now);
            }
            report();
        }
    }

    RateDecision admit(const Route& route, std::optional<std::string_view> api_key) {
        const auto now = Clock::now();
        const auto outcome = decide(route, api_key);
        if (config_.diagnostics) {
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - now);
            admission_ns_.fetch_add(static_cast<std::uint64_t>(ns.count()), std::memory_order_relaxed);
            checks_.fetch_add(1, std::memory_order_relaxed);
        }
        return outcome;
    }

private:
    RateDecision decide(const Route& route, std::optional<std::string_view> api_key) {
        std::array<std::size_t, 3> ids{};
        std::size_t count = 0;
        if (!config_.tenants.empty()) {
            const auto tenant = api_key ? config_.tenants.find(*api_key) : config_.tenants.end();
            if (tenant == config_.tenants.end()) {
                return RateDecision::unauthorized;
            }
            ids[count++] = tenant->second;
        }
        if (config_.global) {
            ids[count++] = *config_.global;
        }
        if (route.quota) {
            ids[count++] = *route.quota;
        }
        // Fixed lock order and try-locks keep worker admission independent of Redis progress.
        std::sort(ids.begin(), ids.begin() + static_cast<std::ptrdiff_t>(count));
        std::array<std::unique_lock<std::mutex>, 3> locks;
        for (std::size_t i = 0; i < count; ++i) {
            locks[i] = std::unique_lock(pools_[ids[i]]->mutex, std::try_to_lock);
            if (!locks[i]) {
                return RateDecision::pending;
            }
        }
        const auto now = Clock::now();
        std::array<Source, 3> sources{};
        auto blocked = RateDecision::allow;
        for (std::size_t i = 0; i < count; ++i) {
            auto& pool = *pools_[ids[i]];
            pool.expire(now);
            prefetch(ids[i], pool, now);
            if (pool.tokens()) {
                sources[i] = Source::lease;
                continue;
            }
            auto reason = RateDecision::pending;
            if (pool.empty_at_redis && now < pool.retry_at) {
                reason = RateDecision::exhausted;
            } else if (pool.error != RateDecision::allow) {
                if (config_.failure == RateFailure::open) {
                    sources[i] = Source::open;
                    continue;
                }
                if (config_.failure == RateFailure::fallback) {
                    replenish_fallback(pool, now);
                    if (pool.fallback_units >= 1000) {
                        sources[i] = Source::fallback;
                        continue;
                    }
                    reason = RateDecision::fallback_exhausted;
                } else {
                    reason = pool.error;
                }
            }
            sources[i] = Source::unavailable;
            if (blocked == RateDecision::allow || blocked == RateDecision::pending ||
                reason == RateDecision::exhausted) {
                blocked = reason;
            }
        }
        if (blocked != RateDecision::allow) {
            return blocked;
        }
        // Verify lease deadlines at the spend point, after all queue interactions.
        const auto spend_at = Clock::now();
        for (std::size_t i = 0; i < count; ++i) {
            auto& pool = *pools_[ids[i]];
            pool.expire(spend_at);
            if (sources[i] == Source::lease && !pool.tokens()) {
                return RateDecision::pending;
            }
        }
        for (std::size_t i = 0; i < count; ++i) {
            auto& pool = *pools_[ids[i]];
            if (sources[i] == Source::lease) {
                pool.spend();
                if (config_.quotas[ids[i]].batch > 1) {
                    prefetch(ids[i], pool, spend_at);
                }
            } else if (sources[i] == Source::fallback) {
                pool.fallback_units -= 1000;
                ++pool.fallback_spent;
            } else {
                ++pool.fail_open;
            }
        }
        return RateDecision::allow;
    }

    void replenish_fallback(Pool& pool, Time now) const {
        if (config_.fallback_rate == 0) {
            return;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - pool.fallback_updated);
        const auto limit = config_.fallback_capacity * 1000;
        const auto ticks = std::min(static_cast<std::size_t>(elapsed.count()),
                                    (limit + config_.fallback_rate - 1) / config_.fallback_rate);
        pool.fallback_units = std::min(limit, pool.fallback_units + ticks * config_.fallback_rate);
        pool.fallback_updated += elapsed;
    }

    void prefetch(std::size_t index, Pool& pool, Time now) {
        const auto batch = config_.quotas[index].batch;
        const auto available = pool.tokens();
        if (pool.pending || now < pool.retry_at || available > batch / 5 ||
            (pool.leases[0].tokens && pool.leases[1].tokens)) {
            return;
        }
        std::unique_lock lock(queue_mutex_, std::try_to_lock);
        if (!lock || queue_size_ == queue_.size()) {
            return;
        }
        queue_[(queue_head_ + queue_size_) % queue_.size()] =
            Job{index, pool.generation, batch - available, now};
        ++queue_size_;
        pool.pending = true;
        lock.unlock();
        notify_event(ready_.get());
    }

    std::optional<Job> pop() {
        std::lock_guard lock(queue_mutex_);
        if (queue_size_ == 0) {
            return std::nullopt;
        }
        const auto job = queue_[queue_head_];
        queue_head_ = (queue_head_ + 1) % queue_.size();
        --queue_size_;
        return job;
    }

    void synchronize() {
        RedisConnection connection(*config_.redis, stop_.get());
        while (!stopping_.load(std::memory_order_relaxed)) {
            const auto job = pop();
            if (!job) {
                std::array<pollfd, 2> ready{{{stop_.get(), POLLIN, 0}, {ready_.get(), POLLIN, 0}}};
                if (poll(ready.data(), ready.size(), -1) < 0 && errno != EINTR) {
                    return;
                }
                if (ready[0].revents) {
                    return;
                }
                std::uint64_t notifications{};
                if (ready[1].revents) {
                    const auto result = read(ready_.get(), &notifications, sizeof(notifications));
                    if (result < 0 && !transient_io()) {
                        return;
                    }
                }
                continue;
            }
            const auto started = Clock::now();
            const auto reply = connection.reserve(config_.quotas[job->pool], job->count,
                                                  job->created + config_.timeout);
            const auto finished = Clock::now();
            if (reply.command) {
                ++commands_;
                reservation_ns_ += static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
            }
            ++attempts_;
            publish(*job, reply, finished);
        }
    }

    void publish(const Job& job, const Reservation& reply, Time now) {
        auto& pool = *pools_[job.pool];
        std::lock_guard lock(pool.mutex);
        if (job.generation != pool.generation || !pool.pending) {
            return;
        }
        pool.pending = false;
        pool.expire(now);
        pool.error = reply.result;
        pool.empty_at_redis = reply.result == RateDecision::allow && reply.wait_ms != 0;
        if (reply.result != RateDecision::allow) {
            pool.retry_at = now + 50ms;
            return;
        }
        const auto wait = reply.wait_ms < 0 ? 250ms : std::chrono::milliseconds(reply.wait_ms);
        pool.retry_at = now + wait;
        pool.granted += reply.tokens;
        const auto expires = job.created + config_.lease;
        if (expires <= now) {
            pool.expired += reply.tokens;
        } else if (reply.tokens) {
            auto& slot = pool.leases[pool.leases[0].tokens ? 1 : 0];
            slot = {reply.tokens, expires};
        }
    }

    void report() const {
        std::cerr << "rate_summary checks=" << checks_.load(std::memory_order_relaxed)
                  << " admission_ns=" << admission_ns_.load(std::memory_order_relaxed)
                  << " attempts=" << attempts_ << " commands=" << commands_
                  << " reservation_ns=" << reservation_ns_ << '\n';
        for (std::size_t i = 0; i < pools_.size(); ++i) {
            const auto& pool = *pools_[i];
            std::cerr << "rate_pool index=" << i << " granted=" << pool.granted
                      << " spent=" << pool.spent << " expired=" << pool.expired
                      << " unused=" << pool.tokens() << " fallback=" << pool.fallback_spent
                      << " fail_open=" << pool.fail_open << '\n';
        }
    }

    const RateSettings& config_;
    std::vector<std::unique_ptr<Pool>> pools_;
    std::vector<Job> queue_;
    std::mutex queue_mutex_;
    std::size_t queue_head_{};
    std::size_t queue_size_{};
    Fd ready_;
    Fd stop_;
    std::atomic<bool> stopping_{};
    std::atomic<std::uint64_t> checks_{};
    std::atomic<std::uint64_t> admission_ns_{};
    std::uint64_t attempts_{};
    std::uint64_t commands_{};
    std::uint64_t reservation_ns_{};
    std::jthread thread_;
};

RateLimiter::RateLimiter(const RateSettings& settings) : state_(std::make_unique<State>(settings)) {}
RateLimiter::~RateLimiter() = default;

RateDecision RateLimiter::admit(const Route& route, std::optional<std::string_view> api_key) {
    return state_->admit(route, api_key);
}

}  // namespace aegisgate
