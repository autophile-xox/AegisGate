#pragma once

#include "../network/socket.hpp"
#include "aegisgate/rate_limit/limiter.hpp"

#include <hiredis.h>

namespace aegisgate {

struct Reservation {
    RateDecision result;
    std::size_t tokens{};
    long long wait_ms{};
    bool command{};
};

class RedisConnection {
public:
    using Clock = std::chrono::steady_clock;
    using Time = Clock::time_point;

    RedisConnection(const Endpoint& endpoint, int stop_fd);
    [[nodiscard]] Reservation reserve(const QuotaSettings& quota, std::size_t wanted, Time deadline);
    [[nodiscard]] bool connected() const { return static_cast<bool>(socket_); }

private:
    RateDecision connect(Time deadline);
    RateDecision poll_socket(short events, Time deadline) const;
    [[nodiscard]] Reservation error(RateDecision result, bool command);
    [[nodiscard]] Reservation exchange(std::string_view command, std::size_t wanted, Time deadline);

    const Endpoint& endpoint_;
    int stop_fd_;
    Fd socket_;
    std::unique_ptr<redisReader, decltype(&redisReaderFree)> reader_{nullptr, &redisReaderFree};
};

}  // namespace aegisgate
