#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <sys/socket.h>

extern "C" ssize_t __real_send(int fd, const void* bytes, std::size_t size, int flags);
extern "C" ssize_t __real_recv(int fd, void* bytes, std::size_t size, int flags);

namespace {

constexpr std::size_t span = 257;
std::atomic<bool> short_send{};
std::atomic<bool> short_receive{};
std::atomic<bool> interrupted{};
std::atomic<bool> would_block{};

bool transient(unsigned& counter) {
    switch (++counter % 5) {
    case 1:
        errno = EINTR;
        interrupted.store(true, std::memory_order_relaxed);
        return true;
    case 2:
        errno = EAGAIN;
        would_block.store(true, std::memory_order_relaxed);
        return true;
    default:
        return false;
    }
}

struct Report {
    ~Report() {
        if (short_send && short_receive && interrupted && would_block) {
            std::fputs("short I/O exercised\n", stderr);
        }
    }
} report;

}  // namespace

extern "C" ssize_t __wrap_send(int fd, const void* bytes, std::size_t size, int flags) {
    thread_local unsigned calls{};
    if (transient(calls)) {
        return -1;
    }
    const auto written = __real_send(fd, bytes, std::min(size, span), flags);
    if (written > 0 && static_cast<std::size_t>(written) < size) {
        short_send.store(true, std::memory_order_relaxed);
    }
    return written;
}

extern "C" ssize_t __wrap_recv(int fd, void* bytes, std::size_t size, int flags) {
    if ((flags & MSG_PEEK) != 0) {
        return __real_recv(fd, bytes, size, flags);
    }
    thread_local unsigned calls{};
    if (transient(calls)) {
        return -1;
    }
    const auto received = __real_recv(fd, bytes, std::min(size, span), flags);
    if (received > 0 && static_cast<std::size_t>(received) < size) {
        short_receive.store(true, std::memory_order_relaxed);
    }
    return received;
}
