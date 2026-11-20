#include "aegisgate/http/message.hpp"

#include <gtest/gtest.h>

using aegisgate::http::Message;

namespace {

constexpr std::string_view get = "GET /api?q=1 HTTP/1.1\r\nHost: Example.Org:8080\r\n\r\n";

std::string parse_and_forward(Message& message, std::string_view input, std::size_t step) {
    std::string output;
    while (!input.empty() && !message.complete() && !message.error_status()) {
        const bool had_headers = message.has_headers();
        const auto consumed = message.consume(input.substr(0, step));
        if (message.error_status() || consumed == 0) {
            break;
        }
        input.remove_prefix(consumed);
        if (!had_headers && message.has_headers()) {
            output = message.forward_headers(false);
        }
        if (message.has_headers()) {
            message.append_body(output);
        }
    }
    return output;
}

TEST(Http, ParsesOneByteAtATime) {
    Message request(HTTP_REQUEST, 1024, 1024);
    const auto output = parse_and_forward(request, get, 1);
    EXPECT_TRUE(request.complete());
    EXPECT_TRUE(request.keep_alive());
    EXPECT_EQ(request.host(), "example.org");
    EXPECT_EQ(request.target(), "/api?q=1");
    EXPECT_EQ(output, "GET /api?q=1 HTTP/1.1\r\nhost:Example.Org:8080\r\nVia:1.1 aegisgate\r\n\r\n");
}

TEST(Http, LeavesFollowingRequestsUnconsumed) {
    Message request(HTTP_REQUEST, 1024, 1024);
    const std::string pipeline = std::string(get) + std::string(get);
    EXPECT_EQ(request.consume(pipeline), get.size());
    EXPECT_TRUE(request.complete());
    request.reset();
    EXPECT_EQ(request.consume(std::string_view(pipeline).substr(get.size())), get.size());
    EXPECT_TRUE(request.complete());
}

TEST(Http, PreservesFixedLengthBinaryBodyAndBoundary) {
    Message request(HTTP_REQUEST, 1024, 1024);
    std::string input = "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n";
    input.append("a\0b\n", 4);
    input.append(get);
    const auto output = parse_and_forward(request, input, 5);
    EXPECT_TRUE(request.complete());
    EXPECT_TRUE(output.ends_with(std::string_view("a\0b\n", 4)));
    EXPECT_NE(output.find("Content-Length:4"), std::string::npos);
}

TEST(Http, NormalizesChunkedBodyAndDropsTrailers) {
    Message request(HTTP_REQUEST, 1024, 1024);
    const auto output = parse_and_forward(request,
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: Chunked\r\nTrailer: X-Proof\r\n\r\n"
        "3;hello=world\r\nabc\r\n0\r\nX-Proof: sample\r\n\r\n", 8192);
    EXPECT_TRUE(request.complete());
    EXPECT_TRUE(output.ends_with("3\r\nabc\r\n0\r\n\r\n"));
    EXPECT_EQ(output.find("x-proof"), std::string::npos);
    EXPECT_NE(output.find("host:x\r\n"), std::string::npos);
}

TEST(Http, FiltersConnectionOptionsAndStandardHopHeaders) {
    Message request(HTTP_REQUEST, 1024, 1024);
    const auto output = parse_and_forward(request,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: Keep-Alive, X-Hop, CLOSE\r\n"
        "X-Hop: skip\r\nKeep-Alive: 100\r\nTE: trailers\r\nX-End: keep\r\n\r\n", 1024);
    EXPECT_TRUE(request.complete());
    EXPECT_FALSE(request.keep_alive());
    EXPECT_EQ(output, "GET / HTTP/1.1\r\nhost:x\r\nx-end:keep\r\nVia:1.1 aegisgate\r\n\r\n");
}

TEST(Http, RejectsInvalidFramingAndHeaders) {
    for (const auto input : {
        "GET / HTTP/1.1\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\nBad Header: invalid\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: Host\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 2\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\nX-Bad: a\r\n continuation\r\n\r\n",
    }) {
        Message request(HTTP_REQUEST, 1024, 1024);
        parse_and_forward(request, input, 7);
        EXPECT_EQ(request.error_status(), 400) << input;
        EXPECT_FALSE(request.complete()) << input;
    }
}

TEST(Http, EnforcesExactHeaderAndDecodedBodyLimits) {
    Message exact(HTTP_REQUEST, get.size(), 8);
    EXPECT_EQ(exact.consume(get), get.size());
    EXPECT_TRUE(exact.complete());

    Message headers(HTTP_REQUEST, get.size() - 1, 8);
    parse_and_forward(headers, get, 3);
    EXPECT_EQ(headers.error_status(), 431);

    Message fixed(HTTP_REQUEST, 1024, 8);
    parse_and_forward(fixed, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 9\r\n\r\n", 1024);
    EXPECT_EQ(fixed.error_status(), 413);

    Message chunked(HTTP_REQUEST, 1024, 8);
    parse_and_forward(chunked,
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n9\r\n", 9);
    EXPECT_EQ(chunked.error_status(), 413);
}

TEST(Http, HeadAndNoContentResponsesEndAtHeaders) {
    Message response(HTTP_RESPONSE, 1024, 1);
    response.reset(true);
    constexpr std::string_view head = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
    EXPECT_EQ(response.consume(std::string(head) + "extra"), head.size());
    EXPECT_TRUE(response.complete());
    EXPECT_NE(response.forward_headers(false).find("Content-Length:100"), std::string::npos);

    response.reset();
    constexpr std::string_view no_body = "HTTP/1.1 204 No Content\r\n\r\n";
    EXPECT_EQ(response.consume(no_body), no_body.size());
    EXPECT_TRUE(response.complete());

    response.reset();
    constexpr std::string_view not_modified = "HTTP/1.1 304 Not Modified\r\nContent-Length: 50\r\n\r\n";
    EXPECT_EQ(response.consume(not_modified), not_modified.size());
    EXPECT_TRUE(response.complete());
}

TEST(Http, EofFinishesOnlyCloseDelimitedBodies) {
    Message response(HTTP_RESPONSE, 1024, 8);
    auto output = parse_and_forward(response, "HTTP/1.0 200 OK\r\n\r\nabc", 1024);
    EXPECT_FALSE(response.complete());
    EXPECT_FALSE(response.keep_alive());
    EXPECT_TRUE(output.ends_with("3\r\nabc\r\n"));
    response.finish();
    EXPECT_TRUE(response.complete());
    EXPECT_EQ(response.error_status(), 0);
    response.append_body(output);
    EXPECT_TRUE(output.ends_with("0\r\n\r\n"));

    response.reset();
    parse_and_forward(response, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabc", 1024);
    response.finish();
    EXPECT_FALSE(response.complete());
    EXPECT_EQ(response.error_status(), 502);
}

TEST(Http, RejectsUnsupportedProtocolBehaviors) {
    Message request(HTTP_REQUEST, 1024, 8);
    parse_and_forward(request, "POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\r\n", 1024);
    EXPECT_EQ(request.error_status(), 417);
    request.reset();
    parse_and_forward(request, "GET / HTTP/1.0\r\nHost: x\r\n\r\n", 1024);
    EXPECT_EQ(request.error_status(), 505);
    request.reset();
    parse_and_forward(request, "CONNECT x:443 HTTP/1.1\r\nHost: x\r\n\r\n", 1024);
    EXPECT_EQ(request.error_status(), 501);
}

}  // namespace
