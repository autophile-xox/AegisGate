#include "redis.hpp"
#include "reservation_script.hpp"

#include <array>
#include <poll.h>

namespace aegisgate {

RedisConnection::RedisConnection(const Endpoint& endpoint, int stop_fd)
    : endpoint_(endpoint), stop_fd_(stop_fd) {}

RateDecision RedisConnection::poll_socket(short events, Time deadline) const {
    for (;;) {
        const auto now = Clock::now();
        if (now >= deadline) {
            return RateDecision::timeout;
        }
        std::array<pollfd, 2> ready{{{stop_fd_, POLLIN, 0}, {socket_.get(), events, 0}}};
        const auto wait = std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
        const auto result = poll(ready.data(), ready.size(), static_cast<int>(wait.count()));
        if (result > 0) {
            return ready[0].revents == 0 ? RateDecision::allow : RateDecision::disconnected;
        }
        if (result < 0 && errno != EINTR) {
            return RateDecision::disconnected;
        }
    }
}

RateDecision RedisConnection::connect(Time deadline) {
    if (socket_) {
        return RateDecision::allow;
    }
    socket_ = Fd(socket(endpoint_.address.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!socket_) {
        return RateDecision::disconnected;
    }
    const auto* address = reinterpret_cast<const sockaddr*>(&endpoint_.address);
    if (::connect(socket_.get(), address, endpoint_.length) != 0) {
        if (errno != EINPROGRESS && errno != EINTR) {
            return RateDecision::disconnected;
        }
        const auto result = poll_socket(POLLOUT, deadline);
        if (result != RateDecision::allow) {
            return result;
        }
        if (socket_error(socket_.get()) != 0) {
            return RateDecision::disconnected;
        }
    }
    reader_.reset(redisReaderCreate());
    if (!reader_) {
        return RateDecision::invalid;
    }
    reader_->maxelements = 8;
    return RateDecision::allow;
}

Reservation RedisConnection::error(RateDecision result, bool command) {
    socket_.reset();
    reader_.reset();
    return {result, 0, 0, command};
}

Reservation RedisConnection::reserve(const QuotaSettings& quota, std::size_t wanted, Time deadline) {
    if (Clock::now() >= deadline) {
        return {RateDecision::timeout};
    }
    if (const auto result = connect(deadline); result != RateDecision::allow) {
        return error(result, false);
    }

    const auto capacity = std::to_string(quota.capacity);
    const auto rate = std::to_string(quota.rate);
    const auto count = std::to_string(wanted);
    const char* args[] = {"EVAL", reservation_script.data(), "1", quota.key.c_str(),
                          capacity.c_str(), rate.c_str(), count.c_str()};
    const std::size_t sizes[] = {4, reservation_script.size(), 1, quota.key.size(),
                                capacity.size(), rate.size(), count.size()};
    char* raw = nullptr;
    const auto length = redisFormatCommandArgv(&raw, 7, args, sizes);
    const std::unique_ptr<char, decltype(&redisFreeCommand)> command(raw, &redisFreeCommand);
    if (length < 0) {
        return error(RateDecision::invalid, false);
    }
    return exchange({command.get(), static_cast<std::size_t>(length)}, wanted, deadline);
}

Reservation RedisConnection::exchange(std::string_view command, std::size_t wanted, Time deadline) {
    bool submitted = false;
    while (!command.empty()) {
        if (const auto result = poll_socket(POLLOUT, deadline); result != RateDecision::allow) {
            return error(result, submitted);
        }
        const auto bytes = send(socket_.get(), command.data(), command.size(), MSG_NOSIGNAL);
        if (bytes > 0) {
            command.remove_prefix(static_cast<std::size_t>(bytes));
            submitted = command.empty();
        } else if (bytes == 0 || !transient_io()) {
            return error(RateDecision::disconnected, submitted);
        }
    }

    std::array<char, 2048> buffer{};
    std::size_t received{};
    for (;;) {
        if (const auto result = poll_socket(POLLIN, deadline); result != RateDecision::allow) {
            return error(result, submitted);
        }
        const auto bytes = recv(socket_.get(), buffer.data(), buffer.size(), 0);
        if (bytes <= 0) {
            if (bytes < 0 && transient_io()) {
                continue;
            }
            return error(RateDecision::disconnected, submitted);
        }
        const auto count = static_cast<std::size_t>(bytes);
        received += count;
        void* object = nullptr;
        if (received > 8192 || redisReaderFeed(reader_.get(), buffer.data(), count) != REDIS_OK ||
            redisReaderGetReply(reader_.get(), &object) != REDIS_OK) {
            return error(RateDecision::invalid, submitted);
        }
        const std::unique_ptr<redisReply, decltype(&freeReplyObject)> reply(
            static_cast<redisReply*>(object), &freeReplyObject);
        if (!reply) {
            continue;
        }
        if (reader_->pos != reader_->len || reply->type != REDIS_REPLY_ARRAY ||
            reply->elements != 2 || reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER) {
            return error(RateDecision::invalid, submitted);
        }
        const auto grant = reply->element[0]->integer;
        const auto wait = reply->element[1]->integer;
        if (grant < 0 || grant > static_cast<long long>(wanted) || wait < -1 || wait > 1000 ||
            (grant == 0 && wait == 0)) {
            return error(RateDecision::invalid, submitted);
        }
        if (Clock::now() >= deadline) {
            return error(RateDecision::timeout, submitted);
        }
        return {RateDecision::allow, static_cast<std::size_t>(grant), wait, submitted};
    }
}

}  // namespace aegisgate
