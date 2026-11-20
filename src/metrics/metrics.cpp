#include "aegisgate/metrics/metrics.hpp"
#include "../network/socket.hpp"
#include "aegisgate/http/message.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <locale>
#include <poll.h>
#include <sstream>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <thread>

namespace aegisgate {
namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t read(const Counter& counter) {
    return counter.load(std::memory_order_relaxed);
}

std::string label(std::string_view name, std::string_view value) {
    std::string escaped(name);
    escaped += "=\"";
    for (const auto c : value) {
        if (c == '\n') {
            escaped += "\\n";
        } else {
            if (c == '\\' || c == '"') {
                escaped += '\\';
            }
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

void type(std::ostream& out, std::string_view name, std::string_view kind) {
    using Description = std::pair<std::string_view, std::string_view>;
    static constexpr Description descriptions[]{
        {"aegisgate_backend_active_requests", "Active application attempts per backend."},
        {"aegisgate_backend_failures_total", "Backend attempt failures by reason."},
        {"aegisgate_backend_healthy", "Worker-local active health eligibility (1 eligible, 0 unhealthy)."},
        {"aegisgate_circuit_state", "Circuit state per backend and worker (0 closed, 1 open, 2 half-open)."},
        {"aegisgate_circuit_transitions_total", "Circuit transitions grouped by their destination state."},
        {"aegisgate_http_connections", "Accepted downstream TCP connections currently owned by workers."},
        {"aegisgate_http_responses_total", "Complete HTTP responses written to downstream sockets by status."},
        {"aegisgate_local_token_shortages_total", "Admission checks with at least one missing local quota token."},
        {"aegisgate_pending_requests", "Requests currently queued for rate admission or upstream assignment."},
        {"aegisgate_rate_rejections_total", "Requests rejected by the rate admission layer by reason."},
        {"aegisgate_redis_commands_total", "Complete reservation commands submitted to Redis."},
        {"aegisgate_redis_connected", "Last observed Redis transport state (1 connected, 0 disconnected)."},
        {"aegisgate_redis_queue_depth", "Refill jobs currently queued for the Redis synchronizer."},
        {"aegisgate_redis_reservation_duration_seconds", "Submitted Redis reservation operation duration."},
        {"aegisgate_redis_reservations_total", "Reservation attempts by outcome, including transport failures."},
        {"aegisgate_request_duration_seconds", "Time from parsing start to completed or cancelled exchange."},
        {"aegisgate_requests_cancelled_total", "Started exchanges without a completely written response."},
        {"aegisgate_requests_rejected_total", "Gateway-generated 4xx and admission 503 responses."},
        {"aegisgate_requests_successful_total", "Completely written HTTP 2xx responses."},
        {"aegisgate_requests_total", "Downstream exchanges started by the HTTP parser."},
        {"aegisgate_route_duration_seconds", "Exchange duration grouped by configured host and path prefix."},
        {"aegisgate_token_refills_total", "Valid Redis reservations that returned a nonempty grant."},
        {"aegisgate_tokens_consumed_total", "Quota units consumed by lease or explicit failure policy."},
        {"aegisgate_tokens_granted_total", "Quota tokens granted by valid Redis reservations."},
        {"aegisgate_upstream_connections", "Active and idle pooled TCP sockets per backend."},
        {"aegisgate_upstream_duration_seconds", "Time from backend assignment to validated non-5xx response."},
        {"process_cpu_seconds_total", "Total process user and system CPU time in seconds."},
        {"process_open_fds", "Number of file descriptors currently open by the process."},
        {"process_resident_memory_bytes", "Resident memory size of the process in bytes."},
    };
    const auto description = std::ranges::find(descriptions, name, &Description::first);
    if (description != std::end(descriptions)) {
        out << "# HELP " << name << ' ' << description->second << '\n';
    }
    out << "# TYPE " << name << ' ' << kind << '\n';
}

void sample(std::ostream& out, std::string_view name, std::string_view labels, auto value) {
    out << name;
    if (!labels.empty()) {
        out << '{' << labels << '}';
    }
    out << ' ' << value << '\n';
}

void histogram(std::ostream& out, std::string_view name, const std::string& labels,
               const Histogram::Snapshot& snapshot) {
    std::uint64_t count = 0;
    const auto prefix = labels.empty() ? labels : labels + ",";
    for (std::size_t i = 0; i < snapshot.bins.size(); ++i) {
        count += snapshot.bins[i];
        const auto bound = i == Histogram::bounds_us.size() ? "+Inf"
            : std::to_string(static_cast<double>(Histogram::bounds_us[i]) / 1e6);
        sample(out, std::string(name) + "_bucket", prefix + label("le", bound), count);
    }
    sample(out, std::string(name) + "_sum", labels, static_cast<double>(snapshot.sum_us) / 1e6);
    sample(out, std::string(name) + "_count", labels, count);
}

void process_metrics(std::ostream& out) {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        const auto seconds = [](const timeval& time) {
            return static_cast<double>(time.tv_sec) + static_cast<double>(time.tv_usec) / 1e6;
        };
        type(out, "process_cpu_seconds_total", "counter");
        sample(out, "process_cpu_seconds_total", "", seconds(usage.ru_utime) + seconds(usage.ru_stime));
    }
    std::ifstream statm("/proc/self/statm");
    std::uint64_t pages{}, resident{};
    const auto page_size = sysconf(_SC_PAGESIZE);
    if (page_size > 0 && statm >> pages >> resident) {
        type(out, "process_resident_memory_bytes", "gauge");
        sample(out, "process_resident_memory_bytes", "", resident * static_cast<std::uint64_t>(page_size));
    }
    statm.close();
    std::error_code error;
    std::size_t count = 0;
    for (auto iter = std::filesystem::directory_iterator("/proc/self/fd", error);
         !error && iter != std::filesystem::directory_iterator(); iter.increment(error)) {
        ++count;
    }
    if (!error && count != 0) {
        type(out, "process_open_fds", "gauge");
        sample(out, "process_open_fds", "", count - 1);
    }
}

Fd open_listener(const Endpoint& endpoint) {
    auto fd = checked_fd(socket(endpoint.address.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0),
                         "metrics listener socket");
    const int yes = 1;
    check_syscall(setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)), "metrics SO_REUSEADDR");
    check_syscall(bind(fd.get(), reinterpret_cast<const sockaddr*>(&endpoint.address), endpoint.length),
                  "bind metrics listener");
    check_syscall(listen(fd.get(), 16), "listen for metrics");
    return fd;
}

}  // namespace

class Metrics::Listener {
public:
    Listener(const Endpoint& endpoint, const Metrics& owner)
        : stop_(checked_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC), "metrics eventfd")),
          listener_(open_listener(endpoint)), owner_(owner), thread_([this] { run(); }) {}

    ~Listener() {
        notify_event(stop_.get());
        thread_.join();
    }

private:
    [[nodiscard]] bool poll_until(int fd, short events, Clock::time_point end) const {
        std::array<pollfd, 2> fds{{{stop_.get(), POLLIN, 0}, {fd, events, 0}}};
        for (;;) {
            const auto now = Clock::now();
            if (now >= end) {
                return false;
            }
            const auto time = end == Clock::time_point::max() ? -1 :
                static_cast<int>(std::chrono::ceil<std::chrono::milliseconds>(end - now).count());
            const auto result = poll(fds.data(), fds.size(), time);
            if (result > 0) {
                return fds[0].revents == 0 && fds[1].revents != 0;
            }
            if (result == 0 || errno != EINTR) {
                return false;
            }
        }
    }

    void run() const {
        while (poll_until(listener_.get(), POLLIN, Clock::time_point::max())) {
            Fd client(accept4(listener_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));
            if (client) {
                try {
                    serve(client.get());
                } catch (const std::exception&) {
                    // Closing the connection reports scrape failure to Prometheus.
                }
            } else if (!transient_io() && errno != ECONNABORTED) {
                break;
            }
        }
    }

    void serve(int client) const {
        const auto deadline = Clock::now() + std::chrono::seconds(1);
        http::Message request(HTTP_REQUEST, 4096, 0);
        std::array<char, 1024> buffer{};
        while (!request.complete() && !request.error_status()) {
            if (!poll_until(client, POLLIN, deadline)) {
                return;
            }
            const auto n = recv(client, buffer.data(), request.allowance(buffer.size()), 0);
            if (n <= 0) {
                if (n < 0 && transient_io()) {
                    continue;
                }
                return;
            }
            (void)request.consume({buffer.data(), static_cast<std::size_t>(n)});
        }
        std::string status = "200 OK";
        std::string body;
        if (request.error_status()) {
            status = "400 Bad Request";
        } else if (!request.is_get()) {
            status = "405 Method Not Allowed";
        } else if (request.target() == "/metrics") {
            body = owner_.scrape();
        } else if (request.target() == "/healthz") {
            if (owner_.ready()) {
                body = "ready\n";
            } else {
                status = "503 Service Unavailable";
            }
        } else {
            status = "404 Not Found";
        }
        const auto response = "HTTP/1.1 " + status +
            "\r\nConnection: close\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: " +
            std::to_string(body.size()) + "\r\n\r\n" + body;
        std::string_view pending(response);
        while (!pending.empty() && poll_until(client, POLLOUT, deadline)) {
            const auto n = send(client, pending.data(), pending.size(), MSG_NOSIGNAL);
            if (n > 0) {
                pending.remove_prefix(static_cast<std::size_t>(n));
            } else if (n == 0 || !transient_io()) {
                break;
            }
        }
    }

    Fd stop_;
    Fd listener_;
    const Metrics& owner_;
    std::jthread thread_;
};

void Histogram::observe(Clock::duration duration) {
    const auto us = static_cast<std::uint64_t>(std::max<std::int64_t>(
        0, std::chrono::duration_cast<std::chrono::microseconds>(duration).count()));
    const auto bucket = std::lower_bound(bounds_us.begin(), bounds_us.end(), us);
    sum_us_.fetch_add(us, std::memory_order_relaxed);
    bins_[static_cast<std::size_t>(bucket - bounds_us.begin())].fetch_add(1, std::memory_order_relaxed);
}

void Histogram::add_to(Snapshot& result) const {
    for (std::size_t i = 0; i < bins_.size(); ++i) {
        result.bins[i] += read(bins_[i]);
    }
    result.sum_us += read(sum_us_);
}

WorkerMetrics::WorkerMetrics(std::size_t routes, std::size_t backends)
    : route_duration(std::make_unique<Histogram[]>(routes)),
      backend(std::make_unique<BackendMetrics[]>(backends)) {}

Metrics::Metrics(const Config& config) : config_(config) {
    for (std::size_t i = 0; i < config.workers; ++i) {
        workers_.push_back(std::make_unique<WorkerMetrics>(config.routes.size(), config.backends.size()));
    }
    listener_ = std::make_unique<Listener>(*config.metrics_listener, *this);
}

Metrics::~Metrics() = default;

std::string Metrics::scrape() const {
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << std::setprecision(15);
    const auto sum = [&](auto getter) {
        std::uint64_t value = 0;
        for (const auto& worker : workers_) {
            value += read(getter(*worker));
        }
        return value;
    };
    const auto traffic = [&](std::string_view name, std::string_view kind,
                             Counter WorkerMetrics::*field) {
        type(out, name, kind);
        sample(out, name, "", sum([field](const auto& worker) -> const Counter& { return worker.*field; }));
    };
    traffic("aegisgate_requests_total", "counter", &WorkerMetrics::requests);
    traffic("aegisgate_requests_successful_total", "counter", &WorkerMetrics::successful);
    traffic("aegisgate_requests_rejected_total", "counter", &WorkerMetrics::rejected);
    traffic("aegisgate_requests_cancelled_total", "counter", &WorkerMetrics::cancelled);
    traffic("aegisgate_http_connections", "gauge", &WorkerMetrics::connections);
    type(out, "aegisgate_pending_requests", "gauge");
    sample(out, "aegisgate_pending_requests", "stage=\"upstream\"",
           sum([](const auto& w) -> const Counter& { return w.upstream_pending; }));
    sample(out, "aegisgate_pending_requests", "stage=\"rate_limit\"",
           sum([](const auto& w) -> const Counter& { return w.refill_pending; }));
    type(out, "aegisgate_http_responses_total", "counter");
    for (std::size_t i = 100; i < 600; ++i) {
        const auto value = sum([i](const auto& w) -> const Counter& { return w.statuses[i]; });
        if (value != 0) {
            sample(out, "aegisgate_http_responses_total", label("status", std::to_string(i)), value);
        }
    }
    Histogram::Snapshot overall;
    for (const auto& worker : workers_) {
        worker->duration.add_to(overall);
    }
    type(out, "aegisgate_request_duration_seconds", "histogram");
    histogram(out, "aegisgate_request_duration_seconds", "", overall);
    type(out, "aegisgate_route_duration_seconds", "histogram");
    for (std::size_t i = 0; i < config_.routes.size(); ++i) {
        Histogram::Snapshot duration;
        for (const auto& worker : workers_) {
            worker->route_duration[i].add_to(duration);
        }
        const auto& route = config_.routes[i];
        histogram(out, "aegisgate_route_duration_seconds", label("route", route.host + " " + route.prefix),
                  duration);
    }
    constexpr std::array failures{"upstream_5xx", "transport_error", "invalid_response", "timeout"};
    constexpr std::array states{"closed", "open", "half_open"};
    type(out, "aegisgate_upstream_duration_seconds", "histogram");
    type(out, "aegisgate_backend_failures_total", "counter");
    type(out, "aegisgate_circuit_transitions_total", "counter");
    type(out, "aegisgate_circuit_state", "gauge");
    type(out, "aegisgate_backend_healthy", "gauge");
    type(out, "aegisgate_backend_active_requests", "gauge");
    type(out, "aegisgate_upstream_connections", "gauge");
    for (std::size_t i = 0; i < config_.backends.size(); ++i) {
        const auto backend = label("backend", config_.backends[i].name);
        Histogram::Snapshot duration;
        for (std::size_t worker = 0; worker < workers_.size(); ++worker) {
            const auto& data = workers_[worker]->backend[i];
            data.duration.add_to(duration);
            const auto worker_labels = backend + "," + label("worker", std::to_string(worker));
            sample(out, "aegisgate_circuit_state", worker_labels, read(data.circuit));
            sample(out, "aegisgate_backend_healthy", worker_labels, read(data.healthy));
        }
        histogram(out, "aegisgate_upstream_duration_seconds", backend, duration);
        for (std::size_t j = 0; j < failures.size(); ++j) {
            sample(out, "aegisgate_backend_failures_total", backend + "," + label("reason", failures[j]),
                   sum([i, j](const auto& w) -> const Counter& { return w.backend[i].failures[j]; }));
        }
        for (std::size_t j = 0; j < states.size(); ++j) {
            sample(out, "aegisgate_circuit_transitions_total", backend + "," + label("state", states[j]),
                   sum([i, j](const auto& w) -> const Counter& { return w.backend[i].transitions[j]; }));
        }
        sample(out, "aegisgate_backend_active_requests", backend,
               sum([i](const auto& w) -> const Counter& { return w.backend[i].active; }));
        const auto idle = sum([i](const auto& w) -> const Counter& { return w.backend[i].idle; });
        const auto total = sum([i](const auto& w) -> const Counter& { return w.backend[i].connections; });
        sample(out, "aegisgate_upstream_connections", backend + ",state=\"idle\"", idle);
        sample(out, "aegisgate_upstream_connections", backend + ",state=\"active\"",
               total > idle ? total - idle : 0);
    }

    constexpr std::array outcomes{"allowed", "invalid_api_key", "quota_exhausted", "refill_pending",
                                   "redis_timeout", "redis_disconnected", "redis_invalid_response",
                                   "fallback_exhausted"};
    type(out, "aegisgate_rate_rejections_total", "counter");
    type(out, "aegisgate_redis_reservations_total", "counter");
    for (std::size_t i = 0; i < outcomes.size(); ++i) {
        const auto reason = label("reason", outcomes[i]);
        if (i != 0) {
            sample(out, "aegisgate_rate_rejections_total", reason,
                   sum([i](const auto& w) -> const Counter& { return w.rate_rejections[i]; }));
        }
        sample(out, "aegisgate_redis_reservations_total", reason, read(redis.outcomes[i]));
    }
    const auto counter = [&](std::string_view name, const Counter& field) {
        type(out, name, "counter");
        sample(out, name, "", read(field));
    };
    counter("aegisgate_redis_commands_total", redis.commands);
    counter("aegisgate_token_refills_total", redis.refills);
    counter("aegisgate_tokens_granted_total", redis.granted);
    counter("aegisgate_local_token_shortages_total", redis.local_shortage);
    type(out, "aegisgate_tokens_consumed_total", "counter");
    sample(out, "aegisgate_tokens_consumed_total", "source=\"lease\"", read(redis.spent));
    sample(out, "aegisgate_tokens_consumed_total", "source=\"fallback\"", read(redis.fallback));
    sample(out, "aegisgate_tokens_consumed_total", "source=\"fail_open\"", read(redis.fail_open));
    type(out, "aegisgate_redis_connected", "gauge");
    sample(out, "aegisgate_redis_connected", "", read(redis.connected));
    type(out, "aegisgate_redis_queue_depth", "gauge");
    sample(out, "aegisgate_redis_queue_depth", "", read(redis.queued));
    Histogram::Snapshot reservation;
    redis.duration.add_to(reservation);
    type(out, "aegisgate_redis_reservation_duration_seconds", "histogram");
    histogram(out, "aegisgate_redis_reservation_duration_seconds", "", reservation);
    process_metrics(out);
    return out.str();
}

}  // namespace aegisgate
