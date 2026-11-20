#include "../network/worker.hpp"

#include <array>
#include <csignal>
#include <iostream>
#include <memory>
#include <poll.h>
#include <pthread.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>

namespace {

int serve(const aegisgate::Config& config) {
    using namespace aegisgate;
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    if (const int error = pthread_sigmask(SIG_BLOCK, &signals, nullptr); error != 0) {
        throw std::system_error(error, std::system_category(), "pthread_sigmask");
    }
    auto signal_fd = checked_fd(signalfd(-1, &signals, SFD_NONBLOCK | SFD_CLOEXEC), "signalfd");
    auto failure_fd = checked_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC), "supervisor eventfd");
    auto limiter = config.rate_limit.redis ? std::make_unique<RateLimiter>(config.rate_limit) : nullptr;
    std::vector<std::unique_ptr<Worker>> workers;
    for (std::size_t i = 0; i < config.workers; ++i) {
        workers.push_back(std::make_unique<Worker>(config, failure_fd.get(), i, limiter.get()));
    }
    for (auto& worker : workers) {
        worker->start();
    }
    std::cout << "AegisGate ready with " << workers.size() << " worker(s)\n" << std::flush;

    std::array<pollfd, 2> watched{{{signal_fd.get(), POLLIN, 0}, {failure_fd.get(), POLLIN, 0}}};
    int ready;
    do {
        ready = poll(watched.data(), watched.size(), -1);
    } while (ready < 0 && errno == EINTR);
    check_syscall(ready, "supervisor poll");
    for (auto& worker : workers) {
        worker->stop();
    }
    for (auto& worker : workers) {
        worker->join();
    }
    // A worker can fail during the drain after SIGTERM has already woken poll().
    watched[1].revents = 0;
    check_syscall(poll(&watched[1], 1, 0), "check worker failures");
    return watched[1].revents != 0 ? 1 : 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: aegisgate CONFIG_FILE\n";
        return 2;
    }
    try {
        return serve(aegisgate::load_config(argv[1]));
    } catch (const std::exception& error) {
        std::cerr << "aegisgate: " << error.what() << '\n';
        return 1;
    }
}
