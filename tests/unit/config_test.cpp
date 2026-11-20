#include "aegisgate/config.hpp"

#include <fstream>
#include <gtest/gtest.h>
#include <unistd.h>

namespace {

class ConfigTest : public testing::Test {
protected:
    void SetUp() override {
        char filename[] = "/tmp/aegisgate-config-test-XXXXXX";
        const auto fd = mkstemp(filename);
        ASSERT_GE(fd, 0);
        close(fd);
        path = filename;
    }
    void TearDown() override { std::filesystem::remove(path); }

    aegisgate::Config load(std::string_view content) const {
        std::ofstream(path) << content;
        return aegisgate::load_config(path);
    }

    std::filesystem::path path;
};

TEST_F(ConfigTest, ParsesConfigurationAndSelectsHostThenLongestPath) {
    const auto config = load(
        "listen ::1 8080 # comment\nworkers 3\n"
        "route * /api/v1 fallback\nroute app.test /api short\nroute app.test /api/v2 deep\n"
        "backend fallback 127.0.0.1 9000 4 2 2\n"
        "backend short ::1 9001 1 1 0\nbackend deep 127.0.0.1 9002 10 0 4\n");
    EXPECT_EQ(config.workers, 3U);
    EXPECT_EQ(config.listener.address.ss_family, AF_INET6);
    EXPECT_EQ(config.route("app.test", "/api/v1?id=3"), 1U);
    EXPECT_EQ(config.route("app.test", "/api/v2/list"), 2U);
    EXPECT_EQ(config.route("other.test", "/api/v1?foo"), 0U);
    EXPECT_EQ(config.route("app.test", "/apiary"), std::nullopt);
}

TEST_F(ConfigTest, RejectsInvalidSettingsAndReferences) {
    constexpr std::string_view base = "listen 127.0.0.1 8080\nbackend local ::1 9000 8 4 2\n";
    for (const std::string_view extra : {
        "route * / missing\n",
        "route * / local\nroute * / local\n",
        "route * / local\nworkers 0\n",
        "route * / local\nheader_bytes 1024\nstream_buffer_bytes 1100\n",
        "route * / local\nbackend other 127.0.0.1 9001 2 3 1\n",
        "route * / local\nconnect_timeout_ms 1\nconnect_timeout_ms 2\n",
        "route * / local\nenable_magic true\n",
    }) {
        EXPECT_THROW(load(std::string(base) + std::string(extra)), std::runtime_error) << extra;
    }
}

TEST_F(ConfigTest, ResolvesNamedGroupsAndPreservesDirectRoutes) {
    const auto config = load(
        "listen 127.0.0.1 8080\nroute * / merged\nroute * /one local\n"
        "group merged peak_ewma other local\n"
        "backend local 127.0.0.1 9000 8 4 2\nbackend other 127.0.0.1 9001 8 4 2\n"
        "ewma_initial_ms 5\newma_decay_ms 80\newma_stale_ms 400\n");
    ASSERT_EQ(config.route("x", "/bar"), 2U);
    EXPECT_EQ(config.groups[2].policy, aegisgate::BalancePolicy::peak_ewma);
    EXPECT_EQ(config.groups[2].backends, (std::vector<std::size_t>{1, 0}));
    EXPECT_EQ(config.route("x", "/one"), 0U);
    EXPECT_EQ(config.latency.decay.count(), 80);
}

TEST_F(ConfigTest, RejectsInvalidGroupsAndSmoothing) {
    const std::string base = "listen 127.0.0.1 8080\nbackend one 127.0.0.1 9000 8 4 2\nroute * / one\n";
    for (const auto* line : {"group one round_robin one\n", "group g peak_ewma one one\n",
                            "group g round_robin missing\n", "group g bad one\n",
                            "ewma_decay_ms 100\newma_stale_ms 20\n"}) {
        EXPECT_THROW(load(base + line), std::runtime_error);
    }
}

TEST(Config, NormalizesHostAndValidatesAuthority) {
    EXPECT_EQ(aegisgate::normalize_host("Test.Example:8080"), "test.example");
    EXPECT_EQ(aegisgate::normalize_host("[0:0:0:0:0:0:0:1]:80"), "[::1]");
    for (const auto invalid : {"", "a:", "a:65536", "a, b", "x@y", "[]", "[::1]oops", "::1"}) {
        EXPECT_EQ(aegisgate::normalize_host(invalid), std::nullopt) << invalid;
    }
}

}  // namespace
