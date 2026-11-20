#include "aegisgate/config.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace aegisgate {
namespace {

std::size_t number(std::string_view value, std::size_t minimum, std::size_t maximum) {
    std::size_t result{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size() ||
        result < minimum || result > maximum) {
        throw std::runtime_error("expected integer in range " + std::to_string(minimum) +
                                 ".." + std::to_string(maximum));
    }
    return result;
}

Endpoint endpoint(const std::string& ip, std::string_view port) {
    const auto parsed_port = htons(static_cast<in_port_t>(number(port, 1, 65535)));
    Endpoint result;
    sockaddr_in v4{};
    if (inet_pton(AF_INET, ip.c_str(), &v4.sin_addr) == 1) {
        v4.sin_family = AF_INET;
        v4.sin_port = parsed_port;
        std::memcpy(&result.address, &v4, sizeof(v4));
        result.length = sizeof(v4);
        return result;
    }
    sockaddr_in6 v6{};
    if (inet_pton(AF_INET6, ip.c_str(), &v6.sin6_addr) == 1) {
        v6.sin6_family = AF_INET6;
        v6.sin6_port = parsed_port;
        std::memcpy(&result.address, &v6, sizeof(v6));
        result.length = sizeof(v6);
        return result;
    }
    throw std::runtime_error("expected a numeric IPv4 or IPv6 address");
}

bool valid_port(std::string_view port) {
    unsigned result{};
    const auto [end, error] = std::from_chars(port.data(), port.data() + port.size(), result);
    return error == std::errc{} && end == port.data() + port.size() && result <= 65535;
}

bool path_matches(std::string_view prefix, std::string_view path) {
    return path.starts_with(prefix) &&
           (path.size() == prefix.size() || prefix.ends_with('/') || path[prefix.size()] == '/');
}

bool rate_name(std::string_view value) {
    return !value.empty() && value.size() <= 64 &&
           value.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") ==
               std::string_view::npos;
}

}  // namespace

std::optional<std::string> normalize_host(std::string_view authority) {
    if (authority.empty() || authority.size() > 261) {
        return std::nullopt;
    }
    if (authority.front() == '[') {
        const auto bracket = authority.find(']');
        if (bracket == std::string_view::npos) {
            return std::nullopt;
        }
        const auto suffix = authority.substr(bracket + 1);
        if (!suffix.empty() && (suffix.front() != ':' || !valid_port(suffix.substr(1)))) {
            return std::nullopt;
        }
        in6_addr address{};
        const std::string literal(authority.substr(1, bracket - 1));
        if (inet_pton(AF_INET6, literal.c_str(), &address) != 1) {
            return std::nullopt;
        }
        char canonical[INET6_ADDRSTRLEN]{};
        inet_ntop(AF_INET6, &address, canonical, sizeof(canonical));
        return "[" + std::string(canonical) + "]";
    }
    const auto colon = authority.find(':');
    if (colon != std::string_view::npos && !valid_port(authority.substr(colon + 1))) {
        return std::nullopt;
    }
    std::string host(authority.substr(0, colon));
    if (host.empty() || host.size() > 253 || host.front() == '.') {
        return std::nullopt;
    }
    for (char& c : host) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.')) {
            return std::nullopt;
        }
    }
    return host;
}

const Route* Config::match_route(std::string_view host, std::string_view target) const {
    const auto path = target.substr(0, target.find('?'));
    const Route* best = nullptr;
    for (const auto& candidate : routes) {
        const bool exact_host = candidate.host == host;
        if (!(exact_host || candidate.host == "*") || !path_matches(candidate.prefix, path)) {
            continue;
        }
        if (best == nullptr || (exact_host && best->host == "*") ||
            (candidate.host == best->host && candidate.prefix.size() > best->prefix.size())) {
            best = &candidate;
        }
    }
    return best;
}

std::optional<std::size_t> Config::route(std::string_view host, std::string_view target) const {
    const auto* match = match_route(host, target);
    return match ? std::optional(match->group) : std::nullopt;
}

Config load_config(const std::filesystem::path& path) {
    std::ifstream file(path);
    if (!file) {
        throw std::runtime_error("cannot open configuration " + path.string());
    }
    Config config;
    std::unordered_set<std::string> seen;
    std::vector<std::string> route_groups;
    std::vector<std::vector<std::string>> group_members;
    std::map<std::pair<std::string, std::string>, std::size_t> route_quotas;
    std::map<std::string, std::size_t, std::less<>> tenant_quotas;
    std::vector<std::pair<std::string, std::string>> tenant_keys;
    std::string line;
    std::size_t line_number{};
    try {
        while (std::getline(file, line)) {
            ++line_number;
            if (const auto comment = line.find('#'); comment != std::string::npos) {
                line.resize(comment);
            }
            std::istringstream tokens(line);
            std::vector<std::string> parts;
            for (std::string part; tokens >> part;) {
                parts.push_back(std::move(part));
            }
            if (parts.empty()) {
                continue;
            }
            const auto& key = parts.front();
            const auto require_count = [&](std::size_t expected) {
                if (parts.size() != expected + 1) {
                    throw std::runtime_error(key + " requires " + std::to_string(expected) +
                                             " arguments");
                }
            };
            auto& rate_limit = config.rate_limit;
            if (key == "quota") {
                if (parts.size() < 2 || rate_limit.quotas.size() == 512) {
                    throw std::runtime_error("expected a quota scope and at most 512 quotas");
                }
                const auto index = rate_limit.quotas.size();
                std::string bucket;
                if (parts[1] == "global") {
                    require_count(4);
                    if (rate_limit.global) {
                        throw std::runtime_error("duplicate global quota");
                    }
                    rate_limit.global = index;
                    bucket = "global";
                } else if (parts[1] == "tenant") {
                    require_count(5);
                    if (!rate_name(parts[2]) || !tenant_quotas.emplace(parts[2], index).second) {
                        throw std::runtime_error("expected a unique tenant name");
                    }
                    bucket = "tenant:" + parts[2];
                } else if (parts[1] == "route") {
                    require_count(6);
                    if (!route_quotas.emplace(std::pair(parts[2], parts[3]), index).second) {
                        throw std::runtime_error("duplicate route quota");
                    }
                    bucket = "route:" + parts[2] + parts[3];
                } else {
                    throw std::runtime_error("quota scope must be global, tenant or route");
                }
                const auto capacity = number(parts[parts.size() - 3], 1, 1000000000);
                rate_limit.quotas.push_back({std::move(bucket), capacity,
                    number(parts[parts.size() - 2], 0, 1000000000),
                    number(parts.back(), 1, capacity)});
                continue;
            }
            if (key == "tenant_key") {
                require_count(2);
                if (tenant_keys.size() == 1024 || !rate_name(parts[2])) {
                    throw std::runtime_error("expected an environment variable and at most 1024 tenant keys");
                }
                tenant_keys.emplace_back(parts[1], parts[2]);
                continue;
            }
            if (key == "backend") {
                require_count(6);
                if (config.backends.size() == 64 ||
                    std::ranges::find(config.backends, parts[1], &Backend::name) !=
                        config.backends.end()) {
                    throw std::runtime_error("duplicate backend or more than 64 backends");
                }
                const auto maximum = number(parts[4], 1, 16384);
                config.backends.push_back({parts[1], endpoint(parts[2], parts[3]), maximum,
                                           number(parts[5], 0, maximum),
                                           number(parts[6], 0, 16384)});
                continue;
            }
            if (key == "group") {
                if (parts.size() < 4 || parts.size() > 67 || config.groups.size() == 64 ||
                    std::ranges::find(config.groups, parts[1], &BackendGroup::name) !=
                        config.groups.end()) {
                    throw std::runtime_error("group requires a unique name, policy and 1..64 backends");
                }
                BalancePolicy policy;
                if (parts[2] == "round_robin") {
                    policy = BalancePolicy::round_robin;
                } else if (parts[2] == "peak_ewma") {
                    policy = BalancePolicy::peak_ewma;
                } else {
                    throw std::runtime_error("group policy must be round_robin or peak_ewma");
                }
                group_members.emplace_back(parts.begin() + 3, parts.end());
                config.groups.push_back({parts[1], policy, {}});
                continue;
            }
            if (key == "route") {
                require_count(3);
                const auto host = parts[1] == "*" ? std::optional(parts[1])
                                                   : normalize_host(parts[1]);
                if (!host || parts[1] != *host || !parts[2].starts_with('/') ||
                    parts[2].size() > 1024 || parts[2].find('?') != std::string::npos) {
                    throw std::runtime_error("expected a normalized host (or *) and a path prefix");
                }
                if (config.routes.size() == 256 || std::ranges::any_of(config.routes,
                        [&](const Route& route) {
                            return route.host == *host && route.prefix == parts[2];
                        })) {
                    throw std::runtime_error("duplicate route or more than 256 routes");
                }
                config.routes.push_back({*host, parts[2], 0});
                route_groups.push_back(parts[3]);
                continue;
            }
            if (!seen.insert(key).second) {
                throw std::runtime_error("duplicate setting " + key);
            }
            if (key == "listen") {
                require_count(2);
                config.listener = endpoint(parts[1], parts[2]);
                continue;
            }
            if (key == "metrics_listen") {
                require_count(2);
                config.metrics_listener = endpoint(parts[1], parts[2]);
                continue;
            }
            if (key == "redis") {
                require_count(2);
                rate_limit.redis = endpoint(parts[1], parts[2]);
                continue;
            }
            if (key == "rate_fallback") {
                require_count(2);
                rate_limit.fallback_capacity = number(parts[1], 1, 1000000000);
                rate_limit.fallback_rate = number(parts[2], 0, 1000000000);
                continue;
            }
            require_count(1);
            const auto& value = parts[1];
            const auto milliseconds = [&] {
                return std::chrono::milliseconds(number(value, 1, 3600000));
            };
            if (key == "workers") {
                config.workers = number(value, 1, 64);
            } else if (key == "clients_per_worker") {
                config.clients_per_worker = number(value, 1, 65536);
            } else if (key == "header_bytes") {
                config.header_bytes = number(value, 256, 65536);
            } else if (key == "body_bytes") {
                config.body_bytes = number(value, 0, 1024 * 1024 * 1024);
            } else if (key == "stream_buffer_bytes") {
                config.stream_buffer_bytes = number(value, 512, 262144);
            } else if (key == "client_idle_ms") {
                config.client_idle = milliseconds();
            } else if (key == "header_timeout_ms") {
                config.header_timeout = milliseconds();
            } else if (key == "connect_timeout_ms") {
                config.connect_timeout = milliseconds();
            } else if (key == "response_timeout_ms") {
                config.response_timeout = milliseconds();
            } else if (key == "shutdown_timeout_ms") {
                config.shutdown_timeout = milliseconds();
            } else if (key == "ewma_decay_ms") {
                config.latency.decay = milliseconds();
            } else if (key == "ewma_stale_ms") {
                config.latency.stale = milliseconds();
            } else if (key == "ewma_initial_ms") {
                config.latency.initial = milliseconds();
            } else if (key == "circuit_breaker") {
                if (value != "on" && value != "off") {
                    throw std::runtime_error("circuit_breaker must be on or off");
                }
                config.circuit.enabled = value == "on";
            } else if (key == "circuit_window") {
                config.circuit.window = number(value, 1, 1024);
            } else if (key == "circuit_min_samples") {
                config.circuit.min_samples = number(value, 1, 1024);
            } else if (key == "circuit_failure_percent") {
                config.circuit.failure_percent = number(value, 1, 100);
            } else if (key == "circuit_cooldown_ms") {
                config.circuit.cooldown = milliseconds();
            } else if (key == "circuit_max_probes") {
                config.circuit.max_probes = number(value, 1, 64);
            } else if (key == "circuit_recovery_successes") {
                config.circuit.recovery_successes = number(value, 1, 64);
            } else if (key == "health_path") {
                if (!value.starts_with('/') || value.size() > 1024 ||
                    value.find_first_not_of("/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~") !=
                        std::string::npos) {
                    throw std::runtime_error("health_path must be a simple absolute HTTP path");
                }
                config.health.path = value;
            } else if (key == "health_interval_ms") {
                config.health.interval = std::chrono::milliseconds(number(value, 0, 3600000));
            } else if (key == "health_timeout_ms") {
                config.health.timeout = milliseconds();
            } else if (key == "health_failures") {
                config.health.failures = number(value, 1, 64);
            } else if (key == "health_successes") {
                config.health.successes = number(value, 1, 64);
            } else if (key == "rate_namespace") {
                if (!rate_name(value)) {
                    throw std::runtime_error("invalid rate_namespace");
                }
                rate_limit.key_namespace = value;
            } else if (key == "rate_timeout_ms") {
                rate_limit.timeout = std::chrono::milliseconds(number(value, 1, 10000));
            } else if (key == "rate_wait_ms") {
                rate_limit.wait = std::chrono::milliseconds(number(value, 0, 10000));
            } else if (key == "rate_lease_ms") {
                rate_limit.lease = milliseconds();
            } else if (key == "rate_queue") {
                rate_limit.queue = number(value, 1, 1024);
            } else if (key == "rate_failure") {
                if (value == "closed") {
                    rate_limit.failure = RateFailure::closed;
                } else if (value == "fallback") {
                    rate_limit.failure = RateFailure::fallback;
                } else if (value == "open") {
                    rate_limit.failure = RateFailure::open;
                } else {
                    throw std::runtime_error("rate_failure must be closed, fallback or open");
                }
            } else if (key == "rate_diagnostics") {
                if (value != "on" && value != "off") {
                    throw std::runtime_error("rate_diagnostics must be on or off");
                }
                rate_limit.diagnostics = value == "on";
            } else {
                throw std::runtime_error("unknown setting " + key);
            }
        }
        if (file.bad()) {
            throw std::runtime_error("error reading configuration");
        }
    } catch (const std::runtime_error& error) {
        throw std::runtime_error(path.string() + ":" + std::to_string(line_number) +
                                 ": " + error.what());
    }

    if (!seen.contains("listen") || config.backends.empty() || config.routes.empty()) {
        throw std::runtime_error("configuration needs a listener, a backend and a route");
    }
    if (config.stream_buffer_bytes < config.header_bytes + 128) {
        throw std::runtime_error("stream_buffer_bytes must be at least header_bytes + 128");
    }
    if (config.latency.stale < config.latency.decay) {
        throw std::runtime_error("ewma_stale_ms must be at least ewma_decay_ms");
    }
    if (config.circuit.min_samples > config.circuit.window) {
        throw std::runtime_error("circuit_min_samples must fit inside circuit_window");
    }

    // Each backend name is also an implicit single-backend group for existing configs.
    std::vector<BackendGroup> groups;
    for (std::size_t i = 0; i < config.backends.size(); ++i) {
        groups.push_back({config.backends[i].name, BalancePolicy::round_robin, {i}});
    }
    for (std::size_t i = 0; i < config.groups.size(); ++i) {
        auto& group = config.groups[i];
        if (std::ranges::find(groups, group.name, &BackendGroup::name) != groups.end()) {
            throw std::runtime_error("group name conflicts with backend " + group.name);
        }
        for (const auto& member : group_members[i]) {
            const auto backend = std::ranges::find(config.backends, member, &Backend::name);
            if (backend == config.backends.end()) {
                throw std::runtime_error("unknown group backend " + member);
            }
            const auto index = static_cast<std::size_t>(backend - config.backends.begin());
            if (std::ranges::find(group.backends, index) != group.backends.end()) {
                throw std::runtime_error("duplicate backend " + member + " in group " + group.name);
            }
            group.backends.push_back(index);
        }
        groups.push_back(std::move(group));
    }
    config.groups = std::move(groups);
    for (std::size_t i = 0; i < config.routes.size(); ++i) {
        const auto group = std::ranges::find(config.groups, route_groups[i], &BackendGroup::name);
        if (group == config.groups.end()) {
            throw std::runtime_error("unknown route backend or group " + route_groups[i]);
        }
        config.routes[i].group = static_cast<std::size_t>(group - config.groups.begin());
        auto& route = config.routes[i];
        if (const auto quota = route_quotas.find({route.host, route.prefix}); quota != route_quotas.end()) {
            route.quota = quota->second;
            route_quotas.erase(quota);
        }
    }
    auto& rate_limit = config.rate_limit;
    if (!route_quotas.empty() || rate_limit.redis.has_value() != !rate_limit.quotas.empty()) {
        throw std::runtime_error("quotas require Redis and route quotas must match configured routes");
    }
    if (rate_limit.failure == RateFailure::fallback && rate_limit.fallback_capacity == 0) {
        throw std::runtime_error("rate_failure fallback requires rate_fallback capacity and rate");
    }
    for (auto& quota : rate_limit.quotas) {
        quota.key = rate_limit.key_namespace + ":" + quota.key;
    }
    std::unordered_set<std::size_t> keyed_tenants;
    for (const auto& [tenant, variable] : tenant_keys) {
        const auto quota = tenant_quotas.find(tenant);
        const char* raw = std::getenv(variable.c_str());
        const std::string_view value = raw ? raw : "";
        if (quota == tenant_quotas.end() || value.empty() || value.size() > 256 ||
            !std::ranges::all_of(value, [](unsigned char c) { return c > 32 && c < 127; }) ||
            !rate_limit.tenants.emplace(value, quota->second).second) {
            throw std::runtime_error("tenant_key requires a known tenant and a unique printable key in " +
                                     variable);
        }
        keyed_tenants.insert(quota->second);
    }
    if (keyed_tenants.size() != tenant_quotas.size()) {
        throw std::runtime_error("each tenant quota needs a tenant_key environment mapping");
    }
    return config;
}

}  // namespace aegisgate
