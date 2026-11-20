#include "worker.hpp"

#include <algorithm>
#include <iostream>
#include <netdb.h>
#include <stdexcept>
#include <syncstream>

namespace aegisgate {
namespace {

constexpr std::uint32_t probe_read_events = EPOLLIN | EPOLLRDHUP;

std::string authority(const Endpoint& endpoint) {
    char host[NI_MAXHOST]{};
    char port[NI_MAXSERV]{};
    const auto* address = reinterpret_cast<const sockaddr*>(&endpoint.address);
    if (getnameinfo(address, endpoint.length, host, sizeof(host), port, sizeof(port),
                    NI_NUMERICHOST | NI_NUMERICSERV) != 0) {
        throw std::runtime_error("could not format health probe address");
    }
    if (endpoint.address.ss_family == AF_INET6) {
        return "[" + std::string(host) + "]:" + port;
    }
    return std::string(host) + ":" + port;
}

}  // namespace

void Worker::configure_health() {
    if (config_.health.interval.count() == 0) {
        return;
    }
    health_due_ = Time::min();
    for (std::size_t i = 0; i < pools_.size(); ++i) {
        auto& health = pools_[i].health;
        health.due = health_due_;
        health.request = "GET " + config_.health.path + " HTTP/1.1\r\nHost: " +
                         authority(config_.backends[i].endpoint) + "\r\nConnection: close\r\n\r\n";
    }
}

void Worker::tick_health() {
    if (now_ < health_due_) {
        return;
    }
    auto next = never;
    for (std::size_t i = 0; i < pools_.size(); ++i) {
        const auto& health = pools_[i].health;
        if (now_ >= health.due) {
            if (health.probe != 0) {
                finish_probe(health.probe, false, "timeout");
            } else {
                start_probe(i);
            }
        }
        next = std::min(next, health.due);
    }
    health_due_ = next;
}

void Worker::start_probe(std::size_t backend) {
    auto& health = pools_[backend].health;
    health.due = now_ + config_.health.interval;
    const auto& endpoint = config_.backends[backend].endpoint;
    const auto* address = reinterpret_cast<const sockaddr*>(&endpoint.address);
    Fd fd(socket(endpoint.address.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd) {
        // A local descriptor shortage says nothing about the upstream.
        return;
    }
    const bool connecting = connect(fd.get(), address, endpoint.length) < 0;
    if (connecting && errno != EINPROGRESS && errno != EINTR) {
        observe_health(backend, false, "connect");
        return;
    }
    const auto token = next_token_++;
    const auto probe = probes_.try_emplace(token, std::move(fd), backend, connecting).first;
    probe->second.socket.watch(epoll_.get(), token, EPOLLOUT | probe_read_events);
    health.probe = token;
    health.due = now_ + config_.health.timeout;
}

void Worker::on_probe(Token token, std::uint32_t events) {
    auto& probe = probes_.at(token);
    const auto& health = pools_[probe.backend].health;
    if (now_ >= health.due) {
        finish_probe(token, false, "timeout");
        return;
    }
    if (probe.connecting) {
        if (socket_error(probe.socket.fd.get()) != 0) {
            finish_probe(token, false, "connect");
            return;
        }
        probe.connecting = false;
    }
    if ((events & EPOLLOUT) && probe.written < health.request.size()) {
        const auto remaining = std::string_view(health.request).substr(probe.written);
        const auto written = send(probe.socket.fd.get(), remaining.data(), remaining.size(), MSG_NOSIGNAL);
        if (written > 0) {
            probe.written += static_cast<std::size_t>(written);
        } else if (written < 0 && !transient_io()) {
            finish_probe(token, false, "write");
            return;
        }
    }
    if (events & (probe_read_events | EPOLLERR | EPOLLHUP)) {
        if (probe.written != health.request.size()) {
            finish_probe(token, false, "early_response");
            return;
        }
        const auto limit = probe.response.allowance(4096);
        const auto bytes = recv(probe.socket.fd.get(), scratch_.data(), limit, 0);
        if (bytes > 0) {
            (void)probe.response.consume({scratch_.data(), static_cast<std::size_t>(bytes)});
            if (probe.response.error_status()) {
                finish_probe(token, false, "invalid_response");
                return;
            }
            if (probe.response.has_headers()) {
                const auto status = probe.response.status();
                finish_probe(token, status >= 200 && status < 300, "http_status");
                return;
            }
        } else if (bytes == 0 || !transient_io()) {
            finish_probe(token, false, "read");
            return;
        }
    }
    if (probe.written == health.request.size()) {
        probe.socket.watch(epoll_.get(), token, probe_read_events);
    }
}

void Worker::finish_probe(Token token, bool success, std::string_view reason) {
    const auto backend = probes_.at(token).backend;
    probes_.erase(token);
    auto& health = pools_[backend].health;
    health.probe = 0;
    health.due = now_ + config_.health.interval;
    health_due_ = std::min(health_due_, health.due);
    observe_health(backend, success, reason);
}

void Worker::observe_health(std::size_t backend, bool success, std::string_view reason) {
    auto& health = pools_[backend].health;
    const auto previous = health.healthy;
    if (success) {
        health.failures = 0;
        if (!health.healthy && ++health.successes == config_.health.successes) {
            health.healthy = true;
            balancer_.reset_latency(backend);
        }
    } else {
        health.successes = 0;
        if (health.healthy && ++health.failures == config_.health.failures) {
            health.healthy = false;
        }
    }
    if (previous != health.healthy) {
        if (metrics_) {
            metrics_->backend[backend].healthy.store(health.healthy, std::memory_order_relaxed);
        }
        const auto tick = std::chrono::duration_cast<std::chrono::milliseconds>(now_.time_since_epoch());
        std::osyncstream(std::cerr) << "worker=" << worker_id_ << " backend=" << config_.backends[backend].name
            << " health=" << (previous ? "healthy->unhealthy" : "unhealthy->healthy")
            << " reason=" << reason << " monotonic_ms=" << tick.count() << '\n';
        schedule(backend);
    }
}

}  // namespace aegisgate
