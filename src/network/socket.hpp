#pragma once

#include <cerrno>
#include <cstdint>
#include <system_error>
#include <utility>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace aegisgate {

class Fd {
public:
    explicit Fd(int fd = -1) : fd_(fd) {}
    ~Fd() { reset(); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    Fd& operator=(Fd&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    [[nodiscard]] int get() const { return fd_; }
    explicit operator bool() const { return fd_ >= 0; }
    void reset() {
        if (fd_ >= 0) {
            close(std::exchange(fd_, -1));
        }
    }

private:
    int fd_;
};

inline Fd checked_fd(int fd, const char* operation) {
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(), operation);
    }
    return Fd(fd);
}

inline void check_syscall(int result, const char* operation) {
    if (result < 0) {
        throw std::system_error(errno, std::system_category(), operation);
    }
}

inline void notify_event(int fd) noexcept {
    const std::uint64_t one = 1;
    while (write(fd, &one, sizeof(one)) < 0 && errno == EINTR) {}
}

inline bool transient_io() {
    return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
}

inline int socket_error(int fd) {
    int error{};
    socklen_t length = sizeof(error);
    return getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) == 0 ? error : errno;
}

struct WatchedFd {
    explicit WatchedFd(Fd descriptor) : fd(std::move(descriptor)) {}

    void watch(int epoll, std::uint64_t token, std::uint32_t desired) {
        if (desired == interests) {
            return;
        }
        epoll_event event{};
        event.events = desired;
        event.data.u64 = token;
        const int operation = desired == 0 ? EPOLL_CTL_DEL
            : (interests == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD);
        check_syscall(epoll_ctl(epoll, operation, fd.get(), &event), "epoll_ctl");
        interests = desired;
    }

    Fd fd;
    std::uint32_t interests{};
};

}  // namespace aegisgate
