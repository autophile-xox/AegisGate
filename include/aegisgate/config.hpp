#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <vector>

namespace aegisgate {

struct Endpoint {
    sockaddr_storage address{};
    socklen_t length{};
};

struct Backend {
    std::string name;
    Endpoint endpoint;
    std::size_t connections;
    std::size_t idle;
    std::size_t pending;
};

struct Route {
    std::string host;
    std::string prefix;
    std::size_t group;
};

enum class BalancePolicy { round_robin, peak_ewma };

struct BackendGroup {
    std::string name;
    BalancePolicy policy;
    std::vector<std::size_t> backends;
};

struct LatencySettings {
    std::chrono::milliseconds decay{500};
    std::chrono::milliseconds stale{3000};
    std::chrono::milliseconds initial{10};
};

struct CircuitSettings {
    bool enabled{};
    std::size_t window{16};
    std::size_t min_samples{4};
    std::size_t failure_percent{50};
    std::chrono::milliseconds cooldown{1000};
    std::size_t max_probes{1};
    std::size_t recovery_successes{2};
};

struct HealthSettings {
    std::string path{"/health"};
    std::chrono::milliseconds interval{0};
    std::chrono::milliseconds timeout{250};
    std::size_t failures{2};
    std::size_t successes{2};
};

struct Config {
    Endpoint listener;
    std::size_t workers = 2;
    std::size_t clients_per_worker = 1024;
    std::size_t header_bytes = 16384;
    std::size_t body_bytes = 16 * 1024 * 1024;
    std::size_t stream_buffer_bytes = 65536;
    std::chrono::milliseconds client_idle{15000};
    std::chrono::milliseconds header_timeout{5000};
    std::chrono::milliseconds connect_timeout{1500};
    std::chrono::milliseconds response_timeout{10000};
    std::chrono::milliseconds shutdown_timeout{5000};
    LatencySettings latency;
    CircuitSettings circuit;
    HealthSettings health;
    std::vector<Backend> backends;
    std::vector<BackendGroup> groups;
    std::vector<Route> routes;

    [[nodiscard]] std::optional<std::size_t> route(
        std::string_view host, std::string_view target) const;
};

[[nodiscard]] Config load_config(const std::filesystem::path& path);
[[nodiscard]] std::optional<std::string> normalize_host(std::string_view authority);

}  // namespace aegisgate
