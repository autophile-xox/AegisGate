#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <llhttp.h>

namespace aegisgate::http {

enum class Framing { none, fixed, chunked, until_eof };

struct Header {
    std::string name;
    std::string value;
};

class Message {
public:
    Message(llhttp_type_t type, std::size_t header_limit, std::size_t body_limit);
    Message(const Message&) = delete;
    Message& operator=(const Message&) = delete;

    void reset(bool head_response = false);
    [[nodiscard]] std::size_t allowance(std::size_t maximum) const;
    // Parse at most one message. A pause at its end leaves the next message untouched.
    [[nodiscard]] std::size_t consume(std::string_view bytes);
    void finish();

    [[nodiscard]] bool has_headers() const { return headers_complete_; }
    [[nodiscard]] bool complete() const { return complete_; }
    [[nodiscard]] int error_status() const { return error_status_; }
    [[nodiscard]] int status() const { return status_; }
    [[nodiscard]] bool keep_alive() const { return keep_alive_; }
    [[nodiscard]] bool is_head() const { return parser_.method == HTTP_HEAD; }
    [[nodiscard]] std::string_view target() const { return target_; }
    [[nodiscard]] std::string_view host() const { return host_; }
    [[nodiscard]] bool started() const { return started_; }

    [[nodiscard]] std::string forward_headers(bool close) const;
    void append_body(std::string& output) const;

private:
    static const llhttp_settings_t settings_;
    static Message& self(llhttp_t* parser) { return *static_cast<Message*>(parser->data); }
    int validate_headers();
    int count_extension(std::size_t length);
    int on_body(const char* bytes, std::size_t length);
    void reject(int status);

    llhttp_t parser_{};
    llhttp_type_t type_;
    std::size_t header_limit_;
    std::size_t body_limit_;
    std::size_t header_bytes_{};
    std::size_t body_bytes_{};
    std::uint64_t wire_bytes_{};
    std::size_t extension_bytes_{};
    std::size_t trailer_bytes_{};
    bool head_response_{};
    bool started_{};
    bool headers_complete_{};
    bool complete_{};
    bool keep_alive_{};
    bool emit_length_{};
    int error_status_{};
    int status_{};
    Framing framing_{Framing::none};
    std::uint64_t content_length_{};
    std::string target_;
    std::string host_;
    std::string protocol_;
    std::string reason_;
    std::string body_;
    std::string trailer_name_;
    std::vector<Header> headers_;
    std::vector<std::string> connection_options_;
};

[[nodiscard]] std::string error_response(int status);

}  // namespace aegisgate::http
