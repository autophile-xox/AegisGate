#pragma once

#include "socket.hpp"

#include "aegisgate/config.hpp"
#include "aegisgate/http/message.hpp"
#include "aegisgate/resilience/circuit_breaker.hpp"
#include "aegisgate/routing/balancer.hpp"

#include <array>
#include <deque>
#include <list>
#include <map>
#include <optional>
#include <thread>
#include <unordered_map>

namespace aegisgate {

class Worker {
public:
    Worker(const Config& config, int failure_event, std::size_t worker_id);
    ~Worker();
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    void start();
    void stop();
    void join();

private:
    using Clock = std::chrono::steady_clock;
    using Time = Clock::time_point;
    using Token = std::uint64_t;
    using Queue = std::list<Token>;
    using Deadlines = std::multimap<Time, Token>;
    static constexpr auto never = Time::max();
    static constexpr Token listener_token = 1;
    static constexpr Token wakeup_token = 2;
    static constexpr std::size_t io_bytes = 16384;
    static constexpr std::size_t framing_room = 32;

    struct Buffer {
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] std::size_t size() const { return bytes.size() - offset; }
        [[nodiscard]] std::string_view view() const { return std::string_view(bytes).substr(offset); }
        void consume(std::size_t count);
        void clear();
        void append(std::string_view value, std::size_t limit);
        void append_body(const http::Message& message, std::size_t budget, std::size_t limit);

    private:
        void prepare(std::size_t incoming, std::size_t limit);
        std::string bytes;
        std::size_t offset{};
    };

    struct Client {
        Client(Fd fd, const Config& config);
        WatchedFd socket;
        http::Message request;
        http::Message response;
        Buffer input;
        Buffer to_upstream;
        Buffer output;
        Token upstream{};
        std::size_t backend{};
        std::optional<Queue::iterator> pending;
        std::optional<Deadlines::iterator> timer;
        std::optional<CircuitBreaker::Permit> permit;
        Time header_deadline{never};
        Time exchange_deadline{never};
        Time read_deadline{never};
        Time write_deadline{never};
        Time linger_deadline{never};
        Time attempt_started{never};
        Time response_headers_at{never};
        bool engaged{};
        bool terminal{};
        bool close_after{};
        bool peer_fin{};
        bool response_sent{};
        bool request_sent{};
        bool upload_cancelled{};
        bool dead{};
    };

    struct Upstream {
        Upstream(Fd fd, std::size_t backend_index, Token client_id, Time deadline)
            : socket(std::move(fd)), backend(backend_index), client(client_id),
              connect_deadline(deadline) {}
        WatchedFd socket;
        std::size_t backend;
        Token client;
        Time connect_deadline;
        std::optional<Queue::iterator> idle;

        [[nodiscard]] bool connecting() const { return connect_deadline != never; }
    };

    struct Pool {
        explicit Pool(const CircuitSettings& settings) : breaker(settings) {}
        CircuitBreaker breaker;
        struct {
            std::string request;
            Time due{never};
            Token probe{};
            std::size_t failures{};
            std::size_t successes{};
            bool healthy{true};
        } health;
        Queue idle;
        Queue pending;
        std::size_t count{};
        bool scheduled{};
    };

    struct Probe {
        Probe(Fd fd, std::size_t index, bool in_progress)
            : socket(std::move(fd)), backend(index), connecting(in_progress),
              response(HTTP_RESPONSE, 8192, 0) {
            response.reset(true);
        }
        WatchedFd socket;
        std::size_t backend;
        bool connecting;
        http::Message response;
        std::size_t written{};
    };

    void run();
    void begin_shutdown();
    void accept_clients();
    void on_client(Token token, std::uint32_t events);
    void on_upstream(Token token, std::uint32_t events);
    void read_client(Token token, Client& client);
    void pump_request(Token token, Client& client);
    void read_response(Client& client, Upstream& upstream, bool hangup);
    void finish_response(Client& client, bool reusable);
    void fail(Client& client, int status, AttemptOutcome outcome = AttemptOutcome::cancelled);
    void clear_exchange(Client& client);
    void settle(Token token);

    [[nodiscard]] bool can_read_client(const Client& client) const;
    [[nodiscard]] std::size_t response_read_size(const Client& client) const;
    [[nodiscard]] int wait_time() const;
    void expire();
    [[nodiscard]] bool expire_client(Token token);
    void update_deadline(Token token, Client& client, Time connect_deadline);

    void admit(Token token, Client& client);
    [[nodiscard]] std::optional<std::size_t> choose_backend(std::size_t group);
    [[nodiscard]] bool eligible(std::size_t backend) const;
    [[nodiscard]] bool capacity(std::size_t backend) const;
    void assign(Token token, Client& client);
    void account_attempt(Client& client, AttemptOutcome outcome);
    void log_transition(std::size_t backend, CircuitState previous, std::string_view reason);
    void release(Client& client, bool reusable);
    void retire(Token upstream);
    void schedule(std::size_t backend);
    void dispatch();
    void configure_health();
    void tick_health();
    void start_probe(std::size_t backend);
    void on_probe(Token token, std::uint32_t events);
    void finish_probe(Token token, bool success, std::string_view reason);
    void observe_health(std::size_t backend, bool success, std::string_view reason);

    const Config& config_;
    const int failure_event_;
    const std::size_t worker_id_;
    Fd epoll_;
    Fd wakeup_;
    WatchedFd listener_;
    std::jthread thread_;
    std::unordered_map<Token, Client> clients_;
    std::unordered_map<Token, Upstream> upstreams_;
    std::unordered_map<Token, Probe> probes_;
    Deadlines deadlines_;
    std::vector<Pool> pools_;
    Balancer balancer_;
    std::deque<std::size_t> dispatch_queue_;
    std::array<char, io_bytes> scratch_{};
    Token next_token_ = wakeup_token + 1;
    std::size_t engaged_{};
    Time now_{};
    Time accept_retry_{never};
    Time shutdown_deadline_{never};
    Time health_due_{never};
    bool draining_{};
};

}  // namespace aegisgate
