#include "worker.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <syncstream>
#include <sys/eventfd.h>

namespace aegisgate {
namespace {

constexpr std::uint32_t read_events = EPOLLIN | EPOLLRDHUP;
constexpr std::uint32_t read_ready = read_events | EPOLLHUP | EPOLLERR;

Fd open_listener(const Endpoint& endpoint) {
    const auto* address = reinterpret_cast<const sockaddr*>(&endpoint.address);
    auto fd = checked_fd(socket(endpoint.address.ss_family,
                                SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0), "listener socket");
    const int enabled = 1;
    check_syscall(setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)),
                  "SO_REUSEADDR");
    check_syscall(setsockopt(fd.get(), SOL_SOCKET, SO_REUSEPORT, &enabled, sizeof(enabled)),
                  "SO_REUSEPORT");
    if (endpoint.address.ss_family == AF_INET6) {
        check_syscall(setsockopt(fd.get(), IPPROTO_IPV6, IPV6_V6ONLY, &enabled, sizeof(enabled)),
                      "IPV6_V6ONLY");
    }
    check_syscall(bind(fd.get(), address, endpoint.length), "bind listener");
    check_syscall(listen(fd.get(), SOMAXCONN), "listen");
    return fd;
}

void set_nodelay(int fd) {
    const int enabled = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
}

bool idle_socket_usable(int fd) {
    char byte;
    const auto result = recv(fd, &byte, 1, MSG_PEEK);
    return result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

int socket_error(int fd) {
    int error{};
    socklen_t length = sizeof(error);
    return getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) == 0 ? error : errno;
}

}  // namespace

void Worker::Buffer::clear() {
    bytes.clear();
    offset = 0;
}

void Worker::Buffer::consume(std::size_t count) {
    offset += count;
    if (offset == bytes.size()) {
        clear();
    }
}

void Worker::Buffer::prepare(std::size_t incoming, std::size_t limit) {
    if (offset != 0 && bytes.size() + incoming > limit) {
        bytes.erase(0, offset);
        offset = 0;
    }
}

void Worker::Buffer::append(std::string_view value, std::size_t limit) {
    prepare(value.size(), limit);
    if (size() + value.size() > limit) {
        throw std::logic_error("forwarding buffer limit exceeded");
    }
    bytes.append(value);
}

void Worker::Buffer::append_body(const http::Message& message, std::size_t budget,
                                 std::size_t limit) {
    prepare(budget + framing_room, limit);
    message.append_body(bytes);
    if (size() > limit) {
        throw std::logic_error("body forwarding buffer limit exceeded");
    }
}

Worker::Client::Client(Fd fd, const Config& config)
    : socket(std::move(fd)), request(HTTP_REQUEST, config.header_bytes, config.body_bytes),
      response(HTTP_RESPONSE, config.header_bytes, config.body_bytes) {}

Worker::Worker(const Config& config, int failure_event)
    : config_(config), failure_event_(failure_event),
      epoll_(checked_fd(epoll_create1(EPOLL_CLOEXEC), "epoll_create1")),
      wakeup_(checked_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC), "worker eventfd")),
      listener_(open_listener(config.listener)), pools_(config.backends.size()) {
    listener_.watch(epoll_.get(), listener_token, EPOLLIN);
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.u64 = wakeup_token;
    check_syscall(epoll_ctl(epoll_.get(), EPOLL_CTL_ADD, wakeup_.get(), &event), "watch worker eventfd");
}

Worker::~Worker() {
    if (thread_.joinable()) {
        stop();
        join();
    }
}

void Worker::start() {
    thread_ = std::jthread([this] {
        try {
            run();
        } catch (const std::exception& error) {
            std::osyncstream(std::cerr) << "worker: " << error.what() << '\n';
            notify_event(failure_event_);
        }
    });
}

void Worker::stop() {
    notify_event(wakeup_.get());
}

void Worker::join() {
    thread_.join();
}

void Worker::run() {
    std::array<epoll_event, 64> events{};
    for (;;) {
        now_ = Clock::now();
        if (draining_ && (engaged_ == 0 || now_ >= shutdown_deadline_)) {
            return;
        }
        expire();
        dispatch();
        if (now_ >= accept_retry_) {
            accept_retry_ = never;
            listener_.watch(epoll_.get(), listener_token, EPOLLIN);
        }
        const auto count = epoll_wait(epoll_.get(), events.data(), events.size(), wait_time());
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            check_syscall(count, "epoll_wait");
        }
        now_ = Clock::now();
        // Apply shutdown and expired timers before forwarding more socket data.
        for (int i = 0; i < count; ++i) {
            if (events[static_cast<std::size_t>(i)].data.u64 == wakeup_token) {
                begin_shutdown();
                break;
            }
        }
        expire();
        for (int i = 0; i < count; ++i) {
            const auto& event = events[static_cast<std::size_t>(i)];
            const Token token = event.data.u64;
            if (token == listener_token && !draining_ && accept_retry_ == never) {
                accept_clients();
            } else if (clients_.contains(token)) {
                on_client(token, event.events);
            } else if (upstreams_.contains(token)) {
                on_upstream(token, event.events);
            }
        }
    }
}

void Worker::begin_shutdown() {
    std::uint64_t ignored;
    const auto result = read(wakeup_.get(), &ignored, sizeof(ignored));
    if (result < 0 && errno != EAGAIN && errno != EINTR) {
        check_syscall(-1, "read worker eventfd");
    }
    if (!draining_) {
        draining_ = true;
        shutdown_deadline_ = now_ + config_.shutdown_timeout;
        accept_retry_ = never;
        listener_.fd.reset();
    }
}

void Worker::accept_clients() {
    for (int attempt = 0; attempt < 32; ++attempt) {
        Fd fd(accept4(listener_.fd.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));
        if (!fd) {
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            if (!transient_io()) {
                accept_retry_ = now_ + std::chrono::milliseconds(100);
                listener_.watch(epoll_.get(), listener_token, 0);
            }
            return;
        }
        if (clients_.size() >= config_.clients_per_worker) {
            continue;
        }
        set_nodelay(fd.get());
        const auto token = next_token_++;
        clients_.try_emplace(token, std::move(fd), config_);
        settle(token);
    }
}

bool Worker::can_read_client(const Client& client) const {
    return !client.terminal && !client.request.complete() && !client.upload_cancelled &&
           !client.pending && client.input.empty() &&
           client.to_upstream.size() + framing_room < config_.stream_buffer_bytes;
}

void Worker::on_client(Token token, std::uint32_t events) {
    if (expire_client(token)) {
        return;
    }
    auto& client = clients_.at(token);
    if ((events & (EPOLLERR | EPOLLHUP)) || (draining_ && !client.engaged)) {
        client.dead = true;
        settle(token);
        return;
    }
    if (client.linger_deadline != never) {
        if (events & read_events) {
            const auto received = recv(client.socket.fd.get(), scratch_.data(), scratch_.size(), 0);
            if (received == 0 || (received < 0 && !transient_io())) {
                client.dead = true;
                settle(token);
            }
        }
        return;
    }
    if ((events & EPOLLOUT) && !client.output.empty()) {
        const auto data = client.output.view().substr(0, io_bytes);
        const auto written = send(client.socket.fd.get(), data.data(), data.size(), MSG_NOSIGNAL);
        if (written > 0) {
            client.output.consume(static_cast<std::size_t>(written));
            client.response_sent = true;
            client.write_deadline = now_ + config_.client_idle;
        } else if (written < 0 && !transient_io()) {
            client.dead = true;
            settle(token);
            return;
        }
    }
    client.peer_fin = client.peer_fin || (events & EPOLLRDHUP);
    if ((events & read_events) && can_read_client(client)) {
        read_client(token, client);
    }
    settle(token);
}

void Worker::read_client(Token token, Client& client) {
    const auto room = config_.stream_buffer_bytes - client.to_upstream.size() - framing_room;
    const auto size = client.request.allowance(std::min(room, scratch_.size()));
    const auto received = recv(client.socket.fd.get(), scratch_.data(), size, 0);
    if (received > 0) {
        client.read_deadline = now_ + config_.client_idle;
        client.input.append({scratch_.data(), static_cast<std::size_t>(received)}, io_bytes);
        pump_request(token, client);
    } else if (received == 0) {
        if (client.request.started()) {
            fail(client, 400);
        } else {
            client.dead = true;
        }
    } else if (!transient_io()) {
        client.dead = true;
    }
}

void Worker::pump_request(Token token, Client& client) {
    // One span can finish the headers and leave a partial body in the receive buffer.
    for (int span = 0; span < 2 && !client.input.empty() && !client.pending &&
         !client.terminal && !client.request.complete() && !client.upload_cancelled; ++span) {
        const auto remaining = config_.stream_buffer_bytes - client.to_upstream.size();
        if (remaining <= framing_room) {
            return;
        }
        const auto had_headers = client.request.has_headers();
        if (!client.request.started()) {
            client.header_deadline = now_ + config_.header_timeout;
        }
        const auto bytes = client.input.view().substr(0, remaining - framing_room);
        const auto consumed = client.request.consume(bytes);
        client.input.consume(consumed);
        if (client.request.error_status()) {
            fail(client, client.request.error_status());
            return;
        }
        if (!had_headers && client.request.has_headers()) {
            client.header_deadline = never;
            const auto backend = config_.route(client.request.host(), client.request.target());
            if (!backend) {
                fail(client, 404);
                return;
            }
            client.engaged = true;
            ++engaged_;
            client.backend = *backend;
            client.close_after = !client.request.keep_alive();
            client.exchange_deadline = now_ + config_.response_timeout;
            client.response.reset(client.request.is_head());
            client.to_upstream.append(client.request.forward_headers(false),
                                      config_.stream_buffer_bytes);
            admit(token, client);
            if (client.terminal) {
                return;
            }
        }
        if (client.request.has_headers()) {
            client.to_upstream.append_body(client.request, consumed, config_.stream_buffer_bytes);
        }
    }
}

std::size_t Worker::response_read_size(const Client& client) const {
    const auto room = config_.stream_buffer_bytes - client.output.size();
    return room > framing_room
        ? client.response.allowance(std::min(room - framing_room, scratch_.size())) : 0;
}

void Worker::on_upstream(Token token, std::uint32_t events) {
    auto& upstream = upstreams_.at(token);
    if (upstream.idle) {
        // A notification can outlive a change of interest. Check actual socket state.
        if (!idle_socket_usable(upstream.socket.fd.get())) {
            retire(token);
        }
        return;
    }
    const auto owner = upstream.client;
    if (expire_client(owner)) {
        return;
    }
    auto& client = clients_.at(owner);
    if (upstream.connecting()) {
        if (socket_error(upstream.socket.fd.get()) != 0) {
            fail(client, 502);
            settle(owner);
            return;
        }
        upstream.connect_deadline = never;
    }
    if ((events & read_ready) && response_read_size(client) != 0) {
        read_response(client, upstream, (events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) != 0);
    }
    if (client.upstream != token) {
        settle(owner);
        return;
    }
    if (events & EPOLLERR) {
        fail(client, 502);
    } else if ((events & EPOLLOUT) && !client.to_upstream.empty()) {
        const auto data = client.to_upstream.view().substr(0, io_bytes);
        const auto written = send(upstream.socket.fd.get(), data.data(), data.size(), MSG_NOSIGNAL);
        if (written > 0) {
            client.request_sent = true;
            client.to_upstream.consume(static_cast<std::size_t>(written));
            pump_request(owner, client);
        } else if (written < 0 && !transient_io()) {
            fail(client, 502);
        }
    }
    settle(owner);
}

void Worker::read_response(Client& client, Upstream& upstream, bool hangup) {
    const auto received = recv(upstream.socket.fd.get(), scratch_.data(), response_read_size(client), 0);
    if (received < 0) {
        if (!transient_io()) {
            fail(client, 502);
        }
        return;
    }
    if (received == 0) {
        client.response.finish();
        if (client.response.error_status()) {
            fail(client, 502);
        } else {
            client.output.append_body(client.response, 0, config_.stream_buffer_bytes);
            finish_response(client, false);
        }
        return;
    }
    if (!client.request_sent) {
        fail(client, 502);
        return;
    }
    const auto had_headers = client.response.has_headers();
    const auto size = static_cast<std::size_t>(received);
    const auto used = client.response.consume({scratch_.data(), size});
    if (client.response.error_status()) {
        fail(client, 502);
        return;
    }
    if (!had_headers && client.response.has_headers()) {
        if (!client.request.complete() || !client.to_upstream.empty()) {
            client.upload_cancelled = true;
            client.close_after = true;
            client.to_upstream.clear();
        }
        client.output.append(client.response.forward_headers(client.close_after || draining_),
                             config_.stream_buffer_bytes);
    }
    if (client.response.has_headers()) {
        client.output.append_body(client.response, used, config_.stream_buffer_bytes);
    }
    if (client.response.complete()) {
        finish_response(client, !hangup && used == size && !client.upload_cancelled &&
                                client.response.keep_alive());
    }
}

void Worker::finish_response(Client& client, bool reusable) {
    release(client, reusable);
    client.exchange_deadline = never;
    client.terminal = true;
}

void Worker::fail(Client& client, int status) {
    release(client, false);
    client.exchange_deadline = client.header_deadline = never;
    client.to_upstream.clear();
    if (client.response_sent) {
        client.dead = true;
        return;
    }
    if (!client.engaged) {
        client.engaged = true;
        ++engaged_;
    }
    client.terminal = client.close_after = true;
    client.output.clear();
    client.output.append(http::error_response(status), config_.stream_buffer_bytes);
}

void Worker::clear_exchange(Client& client) {
    if (client.engaged) {
        --engaged_;
        client.engaged = false;
    }
    client.request.reset();
    client.response.reset();
    client.terminal = client.close_after = client.request_sent = client.response_sent = false;
    client.upload_cancelled = false;
}

void Worker::settle(Token token) {
    auto iterator = clients_.find(token);
    auto& client = iterator->second;
    if (!client.dead && client.terminal && client.output.empty() && client.linger_deadline == never) {
        if (draining_ || client.close_after) {
            // Send FIN after the response, then discard unread inbound data for a short period.
            // Closing with unread TCP data can reset the connection and hide the error response.
            if (shutdown(client.socket.fd.get(), SHUT_WR) < 0) {
                client.dead = true;
            } else {
                client.input.clear();
                client.linger_deadline = now_ + std::min(config_.client_idle,
                                                         std::chrono::milliseconds(200));
            }
        } else {
            clear_exchange(client);
            pump_request(token, client);
        }
    }
    if (client.dead) {
        release(client, false);
        if (client.timer) {
            deadlines_.erase(*client.timer);
        }
        if (client.engaged) {
            --engaged_;
        }
        clients_.erase(iterator);
        return;
    }
    std::uint32_t desired = client.peer_fin ? 0U : EPOLLRDHUP;
    if (client.linger_deadline != never) {
        desired |= EPOLLIN;
    }
    if (can_read_client(client)) {
        desired |= EPOLLIN;
        if (client.read_deadline == never) {
            client.read_deadline = now_ + config_.client_idle;
        }
    } else {
        client.read_deadline = never;
    }
    if (!client.output.empty()) {
        desired |= EPOLLOUT;
        if (client.write_deadline == never) {
            client.write_deadline = now_ + config_.client_idle;
        }
    } else {
        client.write_deadline = never;
    }
    client.socket.watch(epoll_.get(), token, desired);

    auto connect_deadline = never;
    if (client.upstream != 0) {
        auto& upstream = upstreams_.at(client.upstream);
        connect_deadline = upstream.connect_deadline;
        std::uint32_t upstream_events = 0;
        if (upstream.connecting() || !client.to_upstream.empty()) {
            upstream_events |= EPOLLOUT;
        }
        if (upstream.connecting() || response_read_size(client) != 0) {
            upstream_events |= read_events;
        }
        upstream.socket.watch(epoll_.get(), client.upstream, upstream_events);
    }
    update_deadline(token, client, connect_deadline);
}

void Worker::update_deadline(Token token, Client& client, Time connect_deadline) {
    const auto deadline = std::min({client.header_deadline, client.read_deadline,
        client.write_deadline, client.exchange_deadline, connect_deadline, client.linger_deadline});
    if (client.timer) {
        if ((*client.timer)->first == deadline) {
            return;
        }
        deadlines_.erase(*client.timer);
        client.timer.reset();
    }
    if (deadline != never) {
        client.timer = deadlines_.emplace(deadline, token);
    }
}

void Worker::expire() {
    for (int remaining = 64; remaining > 0 && !deadlines_.empty(); --remaining) {
        const auto [deadline, token] = *deadlines_.begin();
        if (deadline > now_) {
            return;
        }
        (void)expire_client(token);
    }
}

bool Worker::expire_client(Token token) {
    auto& client = clients_.at(token);
    if (!client.timer || (*client.timer)->first > now_) {
        return false;
    }
    if (client.linger_deadline <= now_ || client.write_deadline <= now_) {
        client.dead = true;
    } else if (client.header_deadline <= now_ || client.read_deadline <= now_) {
        if (client.request.started()) {
            fail(client, 408);
        } else {
            client.dead = true;
        }
    } else {
        fail(client, 504);
    }
    settle(token);
    return true;
}

int Worker::wait_time() const {
    if (!dispatch_queue_.empty() || (draining_ && engaged_ == 0)) {
        return 0;
    }
    auto deadline = std::min(accept_retry_, shutdown_deadline_);
    if (!deadlines_.empty()) {
        deadline = std::min(deadline, deadlines_.begin()->first);
    }
    if (deadline == never) {
        return -1;
    }
    return static_cast<int>(std::max(std::chrono::milliseconds(0),
                                    std::chrono::ceil<std::chrono::milliseconds>(deadline - now_)).count());
}

bool Worker::capacity(std::size_t backend) const {
    const auto& pool = pools_[backend];
    return !pool.idle.empty() || pool.count < config_.backends[backend].connections;
}

void Worker::admit(Token token, Client& client) {
    auto& pool = pools_[client.backend];
    if (pool.pending.empty() && capacity(client.backend)) {
        assign(token, client);
    } else if (pool.pending.size() < config_.backends[client.backend].pending) {
        client.pending = pool.pending.insert(pool.pending.end(), token);
        schedule(client.backend);
    } else {
        fail(client, 503);
    }
}

void Worker::assign(Token token, Client& client) {
    auto& pool = pools_[client.backend];
    if (!pool.idle.empty()) {
        const auto idle = pool.idle.back();
        auto& upstream = upstreams_.at(idle);
        if (idle_socket_usable(upstream.socket.fd.get())) {
            pool.idle.pop_back();
            upstream.idle.reset();
            upstream.client = token;
            client.upstream = idle;
            return;
        }
        retire(idle);
    }

    const auto& endpoint = config_.backends[client.backend].endpoint;
    const auto* address = reinterpret_cast<const sockaddr*>(&endpoint.address);
    Fd fd(socket(endpoint.address.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd) {
        fail(client, 502);
        return;
    }
    set_nodelay(fd.get());
    auto connect_deadline = never;
    if (connect(fd.get(), address, endpoint.length) < 0) {
        if (errno != EINPROGRESS && errno != EINTR) {
            fail(client, 502);
            return;
        }
        connect_deadline = now_ + config_.connect_timeout;
    }
    const auto upstream = next_token_++;
    upstreams_.try_emplace(upstream, std::move(fd), client.backend, token, connect_deadline);
    ++pool.count;
    client.upstream = upstream;
}

void Worker::release(Client& client, bool reusable) {
    auto& pool = pools_[client.backend];
    if (client.pending) {
        pool.pending.erase(*client.pending);
        client.pending.reset();
    }
    if (client.upstream == 0) {
        return;
    }
    const Token token = std::exchange(client.upstream, 0);
    auto& upstream = upstreams_.at(token);
    if (reusable && pool.idle.size() < config_.backends[client.backend].idle &&
        idle_socket_usable(upstream.socket.fd.get())) {
        upstream.client = 0;
        upstream.idle = pool.idle.insert(pool.idle.end(), token);
        upstream.socket.watch(epoll_.get(), token, read_events);
    } else {
        retire(token);
    }
    schedule(client.backend);
}

void Worker::retire(Token token) {
    const auto iterator = upstreams_.find(token);
    const auto backend = iterator->second.backend;
    auto& pool = pools_[backend];
    if (iterator->second.idle) {
        pool.idle.erase(*iterator->second.idle);
    }
    --pool.count;
    upstreams_.erase(iterator);
    schedule(backend);
}

void Worker::schedule(std::size_t backend) {
    auto& pool = pools_[backend];
    if (!pool.scheduled && !pool.pending.empty() && capacity(backend)) {
        pool.scheduled = true;
        dispatch_queue_.push_back(backend);
    }
}

void Worker::dispatch() {
    for (int remaining = 32; remaining > 0 && !dispatch_queue_.empty(); --remaining) {
        const auto backend = dispatch_queue_.front();
        dispatch_queue_.pop_front();
        auto& pool = pools_[backend];
        pool.scheduled = false;
        if (pool.pending.empty() || !capacity(backend)) {
            continue;
        }
        const auto token = pool.pending.front();
        pool.pending.pop_front();
        auto& client = clients_.at(token);
        client.pending.reset();
        if (client.exchange_deadline <= now_) {
            fail(client, 504);
        } else {
            assign(token, client);
            pump_request(token, client);
        }
        settle(token);
        schedule(backend);
    }
}

}  // namespace aegisgate
