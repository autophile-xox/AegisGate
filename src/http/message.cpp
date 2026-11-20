#include "aegisgate/http/message.hpp"

#include "aegisgate/config.hpp"

#include <algorithm>
#include <charconv>
#include <ranges>

namespace aegisgate::http {
namespace {

char lower(char character) {
    return character >= 'A' && character <= 'Z'
               ? static_cast<char>(character + ('a' - 'A')) : character;
}

std::string_view trim(std::string_view text) {
    const auto first = text.find_first_not_of(" \t");
    if (first == std::string_view::npos) {
        return {};
    }
    return text.substr(first, text.find_last_not_of(" \t") - first + 1);
}

bool token_char(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
           std::string_view("!#$%&'*+-.^_\x60|~").find(static_cast<char>(c)) !=
               std::string_view::npos;
}

bool value_char(unsigned char c) {
    return c == '\t' || (c >= ' ' && c != 127);
}

bool hop_header(std::string_view name) {
    return name == "connection" || name == "keep-alive" || name == "proxy-connection" ||
           name == "proxy-authorization" || name == "proxy-authenticate" || name == "te" ||
           name == "trailer" || name == "transfer-encoding" || name == "upgrade";
}

bool framing_header(std::string_view name) {
    return name == "content-length" || name == "transfer-encoding";
}

bool hex_digit(char c) {
    return (c >= '0' && c <= '9') || (lower(c) >= 'a' && lower(c) <= 'f');
}

bool origin_form(std::string_view target) {
    if (!target.starts_with('/')) {
        return false;
    }
    for (std::size_t i = 0; i < target.size(); ++i) {
        const auto c = static_cast<unsigned char>(target[i]);
        if (c <= ' ' || c >= 127 || c == '#' || c == '\\') {
            return false;
        }
        if (c == '%' && (i + 2 >= target.size() || !hex_digit(target[i + 1]) ||
                         !hex_digit(target[i + 2]))) {
            return false;
        }
    }
    return true;
}

}  // namespace

const llhttp_settings_t Message::settings_ = [] {
    llhttp_settings_t settings{};
    settings.on_protocol = [](llhttp_t* parser, const char* at, std::size_t length) {
        self(parser).protocol_.append(at, length);
        return 0;
    };
    settings.on_url = [](llhttp_t* parser, const char* at, std::size_t length) {
        self(parser).target_.append(at, length);
        return 0;
    };
    settings.on_status = [](llhttp_t* parser, const char* at, std::size_t length) {
        self(parser).reason_.append(at, length);
        return 0;
    };
    settings.on_header_field = [](llhttp_t* parser, const char* at, std::size_t length) {
        auto& message = self(parser);
        if (!message.headers_complete_ && message.headers_.empty()) {
            message.headers_.emplace_back();
        }
        if (message.headers_complete_) {
            message.trailer_bytes_ += length;
            if (message.trailer_bytes_ > message.header_limit_) {
                return static_cast<int>(HPE_USER);
            }
        }
        auto& name = message.headers_complete_ ? message.trailer_name_
                                               : message.headers_.back().name;
        for (std::size_t i = 0; i < length; ++i) {
            if (!token_char(static_cast<unsigned char>(at[i]))) {
                return static_cast<int>(HPE_USER);
            }
            name.push_back(lower(at[i]));
        }
        return 0;
    };
    settings.on_header_field_complete = [](llhttp_t* parser) {
        const auto& message = self(parser);
        if (message.headers_complete_) {
            const auto& name = message.trailer_name_;
            if (framing_header(name) || hop_header(name) || name == "host") {
                return -1;
            }
        }
        return 0;
    };
    settings.on_header_value = [](llhttp_t* parser, const char* at, std::size_t length) {
        if (!std::ranges::all_of(std::string_view(at, length), value_char)) {
            return static_cast<int>(HPE_USER);
        }
        auto& message = self(parser);
        if (message.headers_complete_) {
            message.trailer_bytes_ += length;
            return message.trailer_bytes_ <= message.header_limit_ ? 0 : static_cast<int>(HPE_USER);
        }
        message.headers_.back().value.append(at, length);
        return 0;
    };
    settings.on_header_value_complete = [](llhttp_t* parser) {
        auto& message = self(parser);
        if (message.headers_complete_) {
            message.trailer_bytes_ += 3;
            message.trailer_name_.clear();
            return message.trailer_bytes_ <= message.header_limit_ ? 0 : -1;
        }
        message.headers_.emplace_back();
        return 0;
    };
    settings.on_headers_complete = [](llhttp_t* parser) {
        return self(parser).validate_headers();
    };
    settings.on_chunk_extension_name = [](llhttp_t* parser, const char*, std::size_t length) {
        return self(parser).count_extension(length);
    };
    settings.on_chunk_extension_value = settings.on_chunk_extension_name;
    settings.on_chunk_header = [](llhttp_t* parser) {
        auto& message = self(parser);
        if (parser->content_length > message.body_limit_ - message.body_bytes_) {
            message.reject(413);
            return -1;
        }
        return 0;
    };
    settings.on_body = [](llhttp_t* parser, const char* at, std::size_t length) {
        return self(parser).on_body(at, length);
    };
    settings.on_message_complete = [](llhttp_t* parser) {
        self(parser).complete_ = true;
        return static_cast<int>(HPE_PAUSED);
    };
    return settings;
}();

Message::Message(llhttp_type_t type, std::size_t header_limit, std::size_t body_limit)
    : type_(type), header_limit_(header_limit), body_limit_(body_limit) {
    reset();
}

void Message::reset(bool head_response) {
    llhttp_init(&parser_, type_, &settings_);
    parser_.data = this;
    head_response_ = head_response;
    started_ = headers_complete_ = complete_ = keep_alive_ = emit_length_ = false;
    header_bytes_ = body_bytes_ = extension_bytes_ = trailer_bytes_ = 0;
    wire_bytes_ = content_length_ = 0;
    error_status_ = status_ = 0;
    framing_ = Framing::none;
    target_.clear();
    host_.clear();
    protocol_.clear();
    reason_.clear();
    body_.clear();
    trailer_name_.clear();
    headers_.clear();
    connection_options_.clear();
}

int Message::validate_headers() {
    if (!headers_.empty()) {
        headers_.pop_back();  // Placeholder created after the last complete field.
    }
    const bool request = type_ == HTTP_REQUEST;
    const bool http_11 = parser_.http_minor == 1;
    if (protocol_ != "HTTP" || parser_.http_major != 1 ||
        (!http_11 && (request || parser_.http_minor != 0))) {
        reject(505);
        return -1;
    }
    if (request && (parser_.method == HTTP_CONNECT || parser_.method == HTTP_PRI)) {
        reject(501);
        return -1;
    }
    if (request && !origin_form(target_)) {
        return -1;
    }
    status_ = parser_.status_code;
    if (!request && (status_ < 200 || status_ > 599 ||
                     !std::ranges::all_of(reason_, value_char))) {
        return -1;
    }

    bool chunked = false;
    bool close = !http_11;
    for (const auto& [name, raw_value] : headers_) {
        const auto value = trim(raw_value);
        if (name == "content-length") {
            if (emit_length_ || value.empty()) {
                return -1;
            }
            const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(),
                                                      content_length_);
            if (error != std::errc{} || end != value.data() + value.size()) {
                return -1;
            }
            emit_length_ = true;
        } else if (name == "transfer-encoding") {
            std::string coding(value);
            std::ranges::transform(coding, coding.begin(), lower);
            if (chunked || coding != "chunked" || !http_11) {
                return -1;
            }
            chunked = true;
        } else if (name == "host" && request) {
            auto host = normalize_host(value);
            if (!host || !host_.empty()) {
                return -1;
            }
            host_ = std::move(*host);
        } else if (name == "connection") {
            for (auto remaining = value;;) {
                const auto comma = remaining.find(',');
                const auto item = trim(remaining.substr(0, comma));
                if (item.empty() || !std::ranges::all_of(item, token_char)) {
                    return -1;
                }
                std::string option(item);
                std::ranges::transform(option, option.begin(), lower);
                if (framing_header(option) || (request && option == "host")) {
                    return -1;
                }
                if (option == "upgrade") {
                    reject(501);
                    return -1;
                }
                close = close || option == "close";
                connection_options_.push_back(std::move(option));
                if (comma == std::string_view::npos) {
                    break;
                }
                remaining.remove_prefix(comma + 1);
            }
        } else if (name == "upgrade" || (request && name == "expect")) {
            reject(name == "expect" ? 417 : 501);
            return -1;
        }
    }
    if ((request && host_.empty()) || (emit_length_ && chunked) ||
        (status_ == 204 && (emit_length_ || chunked))) {
        return -1;
    }

    const bool bodyless = !request && (head_response_ || status_ == 204 || status_ == 304);
    if (!bodyless) {
        if (emit_length_ && content_length_ > body_limit_) {
            reject(413);
            return -1;
        }
        if (chunked) {
            framing_ = Framing::chunked;
        } else if (emit_length_) {
            framing_ = Framing::fixed;
        } else if (!request) {
            framing_ = Framing::until_eof;
        }
    }
    std::ranges::sort(connection_options_);
    headers_complete_ = true;
    keep_alive_ = !close && framing_ != Framing::until_eof;
    return bodyless ? 1 : 0;
}

void Message::reject(int status) {
    error_status_ = type_ == HTTP_REQUEST ? status : 502;
}

std::size_t Message::allowance(std::size_t maximum) const {
    return headers_complete_ ? maximum : std::min(maximum, header_limit_ - header_bytes_);
}

std::size_t Message::consume(std::string_view bytes) {
    bytes = bytes.substr(0, allowance(bytes.size()));
    if (bytes.empty() || complete_ || error_status_) {
        return 0;
    }
    started_ = true;
    body_.clear();
    const bool had_headers = headers_complete_;
    const auto result = llhttp_execute(&parser_, bytes.data(), bytes.size());
    if (result != HPE_OK && !(result == HPE_PAUSED && complete_)) {
        if (!error_status_) {
            reject(400);
        }
        return bytes.size();
    }

    const auto used = result == HPE_OK ? bytes.size()
        : static_cast<std::size_t>(llhttp_get_error_pos(&parser_) - bytes.data());
    if (!headers_complete_) {
        header_bytes_ += used;
        if (header_bytes_ == header_limit_) {
            reject(431);
        }
    } else if (framing_ == Framing::chunked && had_headers) {
        wire_bytes_ += used;
        if (wire_bytes_ > static_cast<std::uint64_t>(body_limit_) * 8 + header_limit_) {
            reject(413);
        }
    }
    return used;
}

void Message::finish() {
    body_.clear();
    const auto result = llhttp_finish(&parser_);
    if (!complete_ || (result != HPE_OK && result != HPE_PAUSED)) {
        reject(400);
    }
}

int Message::count_extension(std::size_t length) {
    extension_bytes_ += length;
    if (extension_bytes_ > header_limit_) {
        reject(413);
        return HPE_USER;
    }
    return 0;
}

int Message::on_body(const char* bytes, std::size_t length) {
    if (length > body_limit_ - body_bytes_) {
        reject(413);
        return HPE_USER;
    }
    body_bytes_ += length;
    body_.append(bytes, length);
    return 0;
}

std::optional<std::string_view> Message::unique_header(std::string_view lowercase) const {
    std::optional<std::string_view> result;
    for (const auto& [name, value] : headers_) {
        if (name == lowercase) {
            if (result) {
                return std::nullopt;
            }
            result = trim(value);
        }
    }
    return result;
}

std::string Message::forward_headers(bool close, bool strip_api_key) const {
    std::string output;
    if (type_ == HTTP_REQUEST) {
        output.append(llhttp_method_name(static_cast<llhttp_method_t>(parser_.method)));
        output.append(" ").append(target_).append(" HTTP/1.1\r\n");
    } else {
        output.append("HTTP/1.1 ").append(std::to_string(status_)).append(" ");
        output.append(reason_).append("\r\n");
    }
    for (const auto& [name, value] : headers_) {
        if (!name.empty() && !(strip_api_key && name == "x-api-key") &&
            !hop_header(name) && !framing_header(name) &&
            !std::ranges::binary_search(connection_options_, name)) {
            output.append(name).append(":").append(trim(value)).append("\r\n");
        }
    }
    if (emit_length_) {
        output.append("Content-Length:").append(std::to_string(content_length_)).append("\r\n");
    } else if (framing_ == Framing::chunked || framing_ == Framing::until_eof) {
        output.append("Transfer-Encoding:chunked\r\n");
    }
    if (close) {
        output.append("Connection:close\r\n");
    }
    output.append("Via:1.1 aegisgate\r\n\r\n");
    return output;
}

void Message::append_body(std::string& output) const {
    const bool chunked = framing_ == Framing::chunked || framing_ == Framing::until_eof;
    if (!body_.empty()) {
        if (chunked) {
            char size[32];
            const auto [end, error] = std::to_chars(size, size + sizeof(size), body_.size(), 16);
            if (error == std::errc{}) {
                output.append(size, end).append("\r\n");
            }
        }
        output.append(body_);
        if (chunked) {
            output.append("\r\n");
        }
    }
    if (chunked && complete_) {
        output.append("0\r\n\r\n");
    }
}

std::string error_response(int status, std::string_view rate_reason) {
    const auto* reason = llhttp_status_name(static_cast<llhttp_status_t>(status));
    auto output = "HTTP/1.1 " + std::to_string(status) + " " + reason +
                  "\r\nContent-Length:0\r\nConnection:close\r\n";
    if (!rate_reason.empty()) {
        output.append("X-AegisGate-Rate-Limit: ").append(rate_reason).append("\r\n");
    }
    return output + "\r\n";
}

}  // namespace aegisgate::http
