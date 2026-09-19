#include "trading/dashboard_server.hpp"

#include "trading/telemetry.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace trading {
namespace {

using Clock = std::chrono::steady_clock;

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;  // SO_NOSIGPIPE is set per socket instead (macOS/BSD).
#endif

constexpr std::size_t kMaxRequestBytes = 8U * 1024U;

[[noreturn]] void throw_errno(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

class FileDescriptor {
public:
    FileDescriptor() = default;
    explicit FileDescriptor(int descriptor) noexcept : descriptor_(descriptor) {}
    ~FileDescriptor() { reset(); }
    FileDescriptor(FileDescriptor&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}
    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            reset();
            descriptor_ = std::exchange(other.descriptor_, -1);
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return descriptor_; }
    [[nodiscard]] bool valid() const noexcept { return descriptor_ >= 0; }
    void reset() noexcept {
        if (descriptor_ >= 0) {
            ::close(descriptor_);
            descriptor_ = -1;
        }
    }

private:
    int descriptor_{-1};
};

void set_non_blocking(int descriptor) {
    const int flags = ::fcntl(descriptor, F_GETFL, 0);
    if (flags < 0 || ::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw_errno("fcntl");
    }
}

std::string_view reason_phrase(int status) {
    switch (status) {
        case 200:
            return "OK";
        case 400:
            return "Bad Request";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        case 431:
            return "Request Header Fields Too Large";
        default:
            return "Error";
    }
}

std::string http_response(int status, std::string_view content_type, std::string_view body) {
    std::string response = "HTTP/1.1 " + std::to_string(status) + ' ' + std::string(reason_phrase(status));
    response += "\r\nContent-Type: ";
    response += content_type;
    response += "\r\nContent-Length: " + std::to_string(body.size());
    response += "\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
    response += body;
    return response;
}

std::string json_error(std::string_view message) {
    JsonWriter json;
    json.begin_object().key("ok").value(false).key("error").value(message).end_object();
    return json.take();
}

std::string url_decode(std::string_view text) {
    const auto hex = [](char digit) -> int {
        if (digit >= '0' && digit <= '9') {
            return digit - '0';
        }
        const auto lower = static_cast<char>(std::tolower(static_cast<unsigned char>(digit)));
        return lower >= 'a' && lower <= 'f' ? lower - 'a' + 10 : -1;
    };
    std::string decoded;
    for (std::size_t index = 0; index < text.size(); ++index) {
        if (text[index] == '+') {
            decoded += ' ';
        } else if (text[index] == '%' && index + 2 < text.size() && hex(text[index + 1]) >= 0 &&
                   hex(text[index + 2]) >= 0) {
            decoded += static_cast<char>(hex(text[index + 1]) * 16 + hex(text[index + 2]));
            index += 2;
        } else {
            decoded += text[index];
        }
    }
    return decoded;
}

std::string query_parameter(std::string_view query, std::string_view name) {
    while (!query.empty()) {
        const auto separator = query.find('&');
        const auto pair = query.substr(0, separator);
        const auto equals = pair.find('=');
        if (pair.substr(0, equals) == name) {
            return equals == std::string_view::npos ? std::string{} : url_decode(pair.substr(equals + 1));
        }
        query = separator == std::string_view::npos ? std::string_view{} : query.substr(separator + 1);
    }
    return {};
}

std::string_view header_value(std::string_view headers, std::string_view name) {
    while (!headers.empty()) {
        const auto line_end = headers.find("\r\n");
        const auto line = headers.substr(0, line_end);
        const auto colon = line.find(':');
        if (colon == name.size() &&
            std::equal(name.begin(), name.end(), line.begin(), [](char expected, char actual) {
                return expected == std::tolower(static_cast<unsigned char>(actual));
            })) {
            auto value = line.substr(colon + 1);
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
                value.remove_prefix(1);
            }
            return value;
        }
        headers = line_end == std::string_view::npos ? std::string_view{} : headers.substr(line_end + 2);
    }
    return {};
}

// Requests must come from a page served by this loopback server: a non-loopback Host
// suggests DNS rebinding, and a foreign Origin suggests another site driving the controls.
bool is_loopback_authority(std::string_view authority) {
    if (const auto scheme = authority.find("://"); scheme != std::string_view::npos) {
        authority.remove_prefix(scheme + 3);
    }
    auto host = authority;
    if (host.starts_with('[')) {
        host = host.substr(0, host.find(']') + 1);
    } else if (const auto colon = host.rfind(':'); colon != std::string_view::npos) {
        host = host.substr(0, colon);
    }
    return host == "127.0.0.1" || host == "localhost" || host == "[::1]";
}

}  // namespace

class DashboardServer::Impl {
public:
    Impl(DashboardServerConfig config, std::string page, SnapshotProvider snapshot, ControlHandler control)
        : config_(std::move(config)),
          page_(std::move(page)),
          snapshot_(std::move(snapshot)),
          control_(std::move(control)) {
        if (!snapshot_ || !control_) {
            throw std::invalid_argument("dashboard server needs snapshot and control handlers");
        }
        if (config_.publish_interval <= std::chrono::milliseconds::zero() || config_.max_clients == 0U) {
            throw std::invalid_argument("invalid dashboard server configuration");
        }
    }

    ~Impl() { stop(); }

    void start() {
        std::lock_guard lock(lifecycle_mutex_);
        if (thread_.joinable()) {
            throw std::logic_error("dashboard server already started");
        }

        FileDescriptor listener(::socket(AF_INET, SOCK_STREAM, 0));
        if (!listener.valid()) {
            throw_errno("socket");
        }
        const int enable = 1;
        ::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(config_.port);
        if (::inet_pton(AF_INET, config_.bind_address.c_str(), &address.sin_addr) != 1) {
            throw std::invalid_argument("dashboard bind address must be an IPv4 address");
        }
        if (::bind(listener.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
            throw_errno("bind");
        }
        if (::listen(listener.get(), 16) < 0) {
            throw_errno("listen");
        }
        set_non_blocking(listener.get());
        socklen_t length = sizeof(address);
        if (::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &length) < 0) {
            throw_errno("getsockname");
        }

        int pipe_descriptors[2];
        if (::pipe(pipe_descriptors) < 0) {
            throw_errno("pipe");
        }
        wake_read_ = FileDescriptor(pipe_descriptors[0]);
        wake_write_ = FileDescriptor(pipe_descriptors[1]);
        set_non_blocking(wake_read_.get());
        set_non_blocking(wake_write_.get());

        listener_ = std::move(listener);
        port_.store(ntohs(address.sin_port));
        stopping_.store(false);
        thread_ = std::thread([this] { run(); });
    }

    void stop() {
        std::lock_guard lock(lifecycle_mutex_);
        if (!thread_.joinable()) {
            return;
        }
        stopping_.store(true);
        const char wake = 0;
        [[maybe_unused]] const auto written = ::write(wake_write_.get(), &wake, 1);
        thread_.join();
        listener_.reset();
        wake_read_.reset();
        wake_write_.reset();
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return port_.load(); }

private:
    struct Client {
        FileDescriptor socket;
        std::string input{};
        std::string output{};
        std::size_t sent{0};
        bool streaming{false};
        bool close_after_send{false};
        bool closed{false};
    };

    void run() {
        std::vector<pollfd> descriptors;
        auto next_publish = Clock::now();
        while (!stopping_.load()) {
            if (Clock::now() >= next_publish) {
                publish_snapshot();
                next_publish = Clock::now() + config_.publish_interval;
            }

            descriptors.clear();
            descriptors.push_back(pollfd{wake_read_.get(), POLLIN, 0});
            descriptors.push_back(pollfd{listener_.get(), POLLIN, 0});
            for (const auto& client : clients_) {
                const bool sending = client.sent < client.output.size();
                descriptors.push_back(pollfd{client.socket.get(),
                                             static_cast<short>(sending ? (POLLIN | POLLOUT) : POLLIN), 0});
            }
            const auto wait = std::chrono::ceil<std::chrono::milliseconds>(next_publish - Clock::now());
            const auto timeout = static_cast<int>(std::clamp<std::int64_t>(wait.count(), 0, 1'000));
            if (::poll(descriptors.data(), static_cast<nfds_t>(descriptors.size()), timeout) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                std::cerr << "dashboard poll failed: " << std::generic_category().message(errno) << '\n';
                break;
            }

            if (descriptors[0].revents != 0) {
                char drained[64];
                while (::read(wake_read_.get(), drained, sizeof(drained)) > 0) {
                }
            }
            // Clients accepted below are polled from the next iteration on.
            const auto polled_clients = descriptors.size() - 2U;
            if ((descriptors[1].revents & POLLIN) != 0) {
                accept_clients();
            }
            for (std::size_t index = 0; index < polled_clients; ++index) {
                auto& client = clients_[index];
                const auto events = descriptors[index + 2U].revents;
                if ((events & (POLLERR | POLLNVAL)) != 0) {
                    client.closed = true;
                    continue;
                }
                if ((events & (POLLIN | POLLHUP)) != 0) {
                    read_request(client);
                }
                if (!client.closed && (events & POLLOUT) != 0) {
                    flush(client);
                }
            }
            std::erase_if(clients_, [](const Client& client) { return client.closed; });
        }
        clients_.clear();
    }

    void publish_snapshot() {
        try {
            latest_snapshot_ = snapshot_();
        } catch (const std::exception& exception) {
            if (!reported_snapshot_failure_) {
                reported_snapshot_failure_ = true;
                std::cerr << "dashboard snapshot failed: " << exception.what() << '\n';
            }
            return;
        }
        const auto event = "data: " + latest_snapshot_ + "\n\n";
        for (auto& client : clients_) {
            // A client still sending the previous snapshot skips this one: the newest
            // snapshot replaces stale ones instead of queueing behind them.
            if (!client.streaming || client.closed || client.sent < client.output.size()) {
                continue;
            }
            client.output = event;
            client.sent = 0;
            flush(client);
        }
    }

    void accept_clients() {
        for (;;) {
            const int descriptor = ::accept(listener_.get(), nullptr, nullptr);
            if (descriptor < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return;
            }
            FileDescriptor socket(descriptor);
            if (clients_.size() >= config_.max_clients) {
                continue;
            }
            try {
                set_non_blocking(descriptor);
            } catch (const std::system_error&) {
                continue;
            }
#ifdef SO_NOSIGPIPE
            const int enable = 1;
            ::setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enable, sizeof(enable));
#endif
            clients_.push_back(Client{std::move(socket)});
        }
    }

    void read_request(Client& client) {
        char buffer[4'096];
        for (;;) {
            const auto received = ::recv(client.socket.get(), buffer, sizeof(buffer), 0);
            if (received > 0) {
                // Streams and answered requests have nothing more to say; input is discarded.
                if (!client.streaming && !client.close_after_send) {
                    client.input.append(buffer, static_cast<std::size_t>(received));
                    if (client.input.size() > kMaxRequestBytes) {
                        respond(client, 431, "application/json", json_error("request too large"));
                        return;
                    }
                }
                continue;
            }
            if (received == 0) {
                client.closed = true;
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                client.closed = true;
                return;
            }
            break;
        }
        if (client.streaming || client.close_after_send) {
            return;
        }
        const auto head_end = client.input.find("\r\n\r\n");
        if (head_end != std::string::npos) {
            const std::string head = client.input.substr(0, head_end);
            handle_request(client, head);
        }
    }

    void handle_request(Client& client, std::string_view head) {
        const auto line_end = head.find("\r\n");
        const auto request_line = head.substr(0, line_end);
        const auto headers = line_end == std::string_view::npos ? std::string_view{} : head.substr(line_end + 2);
        const auto first_space = request_line.find(' ');
        const auto second_space = request_line.find(' ', first_space + 1);
        if (first_space == std::string_view::npos || second_space == std::string_view::npos) {
            respond(client, 400, "application/json", json_error("malformed request line"));
            return;
        }
        const auto method = request_line.substr(0, first_space);
        const auto target = request_line.substr(first_space + 1, second_space - first_space - 1);
        const auto query_start = target.find('?');
        const auto path = target.substr(0, query_start);
        const auto query = query_start == std::string_view::npos ? std::string_view{} : target.substr(query_start + 1);

        const auto origin = header_value(headers, "origin");
        if (!is_loopback_authority(header_value(headers, "host")) ||
            (!origin.empty() && !is_loopback_authority(origin))) {
            respond(client, 403, "application/json", json_error("dashboard is loopback-only"));
            return;
        }

        if (path == "/api/control") {
            if (method != "POST") {
                respond(client, 405, "application/json", json_error("use POST"));
                return;
            }
            ControlResult result;
            try {
                result = control_(query_parameter(query, "name"), query_parameter(query, "value"));
            } catch (const std::exception& exception) {
                result = ControlResult{400, json_error(exception.what())};
            }
            respond(client, result.status, "application/json", result.body);
        } else if (method != "GET") {
            respond(client, 405, "application/json", json_error("use GET"));
        } else if (path == "/" || path == "/index.html") {
            respond(client, 200, "text/html; charset=utf-8", page_);
        } else if (path == "/api/snapshot") {
            respond(client, 200, "application/json", latest_snapshot_.empty() ? "{}" : latest_snapshot_);
        } else if (path == "/events") {
            client.streaming = true;
            client.output =
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\n"
                "Connection: keep-alive\r\n\r\nretry: 1000\n\n";
            if (!latest_snapshot_.empty()) {
                client.output += "data: " + latest_snapshot_ + "\n\n";
            }
            client.sent = 0;
            flush(client);
        } else {
            respond(client, 404, "application/json", json_error("not found"));
        }
    }

    void respond(Client& client, int status, std::string_view content_type, std::string_view body) {
        client.output = http_response(status, content_type, body);
        client.sent = 0;
        client.close_after_send = true;
        flush(client);
    }

    void flush(Client& client) {
        while (client.sent < client.output.size()) {
            const auto sent = ::send(client.socket.get(), client.output.data() + client.sent,
                                     client.output.size() - client.sent, kSendFlags);
            if (sent > 0) {
                client.sent += static_cast<std::size_t>(sent);
                continue;
            }
            if (sent < 0 && errno == EINTR) {
                continue;
            }
            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return;
            }
            client.closed = true;
            return;
        }
        client.output.clear();
        client.sent = 0;
        if (client.close_after_send) {
            client.closed = true;
        }
    }

    const DashboardServerConfig config_;
    const std::string page_;
    SnapshotProvider snapshot_;
    ControlHandler control_;
    std::mutex lifecycle_mutex_;
    std::thread thread_;
    std::atomic<bool> stopping_{false};
    std::atomic<std::uint16_t> port_{0};
    FileDescriptor listener_;
    FileDescriptor wake_read_;
    FileDescriptor wake_write_;
    // Owned by the server thread.
    std::vector<Client> clients_;
    std::string latest_snapshot_;
    bool reported_snapshot_failure_{false};
};

DashboardServer::DashboardServer(DashboardServerConfig config,
                                 std::string page,
                                 SnapshotProvider snapshot,
                                 ControlHandler control)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(page), std::move(snapshot),
                                   std::move(control))) {}

DashboardServer::~DashboardServer() = default;

void DashboardServer::start() {
    impl_->start();
}

void DashboardServer::stop() {
    impl_->stop();
}

std::uint16_t DashboardServer::port() const noexcept {
    return impl_->port();
}

}  // namespace trading
