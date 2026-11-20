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
    std::size_t backend;
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
    std::vector<Backend> backends;
    std::vector<Route> routes;

    [[nodiscard]] std::optional<std::size_t> route(
        std::string_view host, std::string_view target) const;
};

[[nodiscard]] Config load_config(const std::filesystem::path& path);
[[nodiscard]] std::optional<std::string> normalize_host(std::string_view authority);

}  // namespace aegisgate
