#ifndef DATAGEN_SLOW_CONSUMER_HPP
#define DATAGEN_SLOW_CONSUMER_HPP

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <istream>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#ifdef __linux__
#include <sys/ioctl.h>
#include <linux/sockios.h>
#endif

#include "configLoader.hpp"

using asio::ip::tcp;

struct SlowConsumerState {
    explicit SlowConsumerState(std::string peer_name = {}) : peer(std::move(peer_name)) {
        const auto now = steady_ms();
        last_successful_write_ms.store(now, std::memory_order_relaxed);
        last_ack_ms.store(now, std::memory_order_relaxed);
    }

    static std::int64_t steady_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    std::string peer;
    std::atomic<std::uint64_t> sent_messages{0};
    std::atomic<std::uint64_t> acknowledged_messages{0};
    std::atomic<std::uint64_t> pending_writes{0};
    std::atomic<std::uint64_t> nonblocking_send_waits{0};
    std::atomic<std::int64_t> last_successful_write_ms{0};
    std::atomic<std::int64_t> last_ack_ms{0};
    std::atomic<std::int64_t> last_log_ms{0};
    std::atomic<bool> slow_flag{false};
};

inline std::string peer_name(tcp::socket& socket) {
    asio::error_code ec;
    const auto endpoint = socket.remote_endpoint(ec);
    return ec ? std::string{"unknown-peer"} : endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
}

inline std::size_t pending_socket_bytes(tcp::socket& socket) {
#ifdef __linux__
    int bytes = 0;
    if (::ioctl(socket.native_handle(), SIOCOUTQ, &bytes) == 0 && bytes > 0) {
        return static_cast<std::size_t>(bytes);
    }
#endif
    return 0;
}

inline std::string with_sequence_header(const std::string& payload, std::uint64_t sequence) {
    std::string framed;
    framed.reserve(payload.size() + 32);
    framed.append("# datagen_seq=");
    framed.append(std::to_string(sequence));
    framed.push_back('\n');
    framed.append(payload);
    return framed;
}

inline bool parse_ack_line(const std::string& line, std::uint64_t& sequence) {
    constexpr std::string_view ack_prefix{"ACK "};
    constexpr std::string_view kv_prefix{"datagen_ack="};

    std::string_view view(line);
    while (!view.empty() && (view.back() == '\n' || view.back() == '\r' || view.back() == ' ')) {
        view.remove_suffix(1);
    }

    if (view.rfind(ack_prefix, 0) == 0) {
        view.remove_prefix(ack_prefix.size());
    } else if (view.rfind(kv_prefix, 0) == 0) {
        view.remove_prefix(kv_prefix.size());
    } else {
        return false;
    }

    std::uint64_t value = 0;
    for (const char c : view) {
        if (c < '0' || c > '9') {
            return false;
        }
        value = value * 10 + static_cast<std::uint64_t>(c - '0');
    }
    sequence = value;
    return true;
}

inline void record_ack(SlowConsumerState& state, std::uint64_t sequence) {
    std::uint64_t current = state.acknowledged_messages.load(std::memory_order_relaxed);
    while (sequence > current &&
           !state.acknowledged_messages.compare_exchange_weak(
               current, sequence, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    state.last_ack_ms.store(SlowConsumerState::steady_ms(), std::memory_order_relaxed);
}

inline bool slow_consumer_ack_tracking_enabled(const SlowConsumerConfig& cfg) {
    return cfg.enabled && (cfg.ack_timeout_ms > 0 || cfg.max_unacked_messages > 0);
}

inline void ack_read_loop(tcp::socket& socket, SlowConsumerState& state, std::atomic<bool>& alive) {
    asio::streambuf buffer;
    while (alive.load(std::memory_order_relaxed)) {
        asio::error_code ec;
        asio::read_until(socket, buffer, '\n', ec);
        if (ec) {
            break;
        }

        std::istream input(&buffer);
        std::string line;
        std::getline(input, line);

        std::uint64_t sequence = 0;
        if (parse_ack_line(line, sequence)) {
            record_ack(state, sequence);
        }
    }
}

class ScopedAckReader {
public:
    ScopedAckReader(tcp::socket& socket,
                    SlowConsumerState& state,
                    const SlowConsumerConfig& cfg,
                    std::atomic<bool>& alive)
        : socket_(socket), alive_(alive) {
        if (slow_consumer_ack_tracking_enabled(cfg)) {
            thread_ = std::thread([this, &state] {
                ack_read_loop(socket_, state, alive_);
            });
        }
    }

    ~ScopedAckReader() {
        alive_.store(false, std::memory_order_relaxed);
        asio::error_code ignored_ec;
        socket_.cancel(ignored_ec);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    ScopedAckReader(const ScopedAckReader&) = delete;
    ScopedAckReader& operator=(const ScopedAckReader&) = delete;

private:
    tcp::socket& socket_;
    std::atomic<bool>& alive_;
    std::thread thread_;
};

inline bool should_log_slow_consumer(const SlowConsumerConfig& cfg, SlowConsumerState& state) {
    const auto now = SlowConsumerState::steady_ms();
    auto last = state.last_log_ms.load(std::memory_order_relaxed);
    if (now - last < cfg.log_interval_ms) {
        return false;
    }
    return state.last_log_ms.compare_exchange_strong(
        last, now, std::memory_order_relaxed, std::memory_order_relaxed);
}

inline bool evaluate_slow_consumer(const SlowConsumerConfig& cfg,
                                   SlowConsumerState& state,
                                   std::size_t pending_bytes,
                                   std::int64_t write_ms) {
    if (!cfg.enabled) {
        return false;
    }

    const auto sent = state.sent_messages.load(std::memory_order_relaxed);
    const auto acked = state.acknowledged_messages.load(std::memory_order_relaxed);
    const auto unacked = sent >= acked ? sent - acked : 0;
    const auto now = SlowConsumerState::steady_ms();
    const auto last_ack = state.last_ack_ms.load(std::memory_order_relaxed);

    const bool pending_bytes_high =
        cfg.pending_socket_bytes_limit > 0 && pending_bytes > cfg.pending_socket_bytes_limit;
    const bool write_slow =
        cfg.write_duration_warn_ms > 0 && write_ms > cfg.write_duration_warn_ms;
    const bool ack_old =
        cfg.ack_timeout_ms > 0 && sent > acked && now - last_ack > cfg.ack_timeout_ms;
    const bool ack_lag_high =
        cfg.max_unacked_messages > 0 && unacked > cfg.max_unacked_messages;
    const bool pending_queue_high = state.pending_writes.load(std::memory_order_relaxed) > 1;

    const bool slow = pending_bytes_high || write_slow || ack_old || ack_lag_high || pending_queue_high;
    state.slow_flag.store(slow, std::memory_order_relaxed);

    if (slow && should_log_slow_consumer(cfg, state)) {
        std::cerr << "Slow consumer detected"
                  << " peer=" << (state.peer.empty() ? "unknown-peer" : state.peer)
                  << " pending_socket_bytes=" << pending_bytes
                  << " write_ms=" << write_ms
                  << " pending_writes=" << state.pending_writes.load(std::memory_order_relaxed)
                  << " sent_seq=" << sent
                  << " acked_seq=" << acked
                  << " unacked=" << unacked
                  << " nonblocking_send_waits=" << state.nonblocking_send_waits.load(std::memory_order_relaxed)
                  << "\n";
    }

    return slow;
}

inline void raw_nonblocking_write(tcp::socket& socket,
                                  const asio::const_buffer& buffer,
                                  SlowConsumerState& state) {
    const bool previous_non_blocking = socket.non_blocking();
    socket.non_blocking(true);

    try {
        std::size_t sent = 0;
        const auto* data = static_cast<const unsigned char*>(buffer.data());
        const auto size = buffer.size();

        while (sent < size) {
            asio::error_code ec;
            const std::size_t n = socket.send(asio::buffer(data + sent, size - sent), 0, ec);
            if (!ec) {
                sent += n;
                continue;
            }

            if (ec == asio::error::would_block || ec == asio::error::try_again) {
                state.nonblocking_send_waits.fetch_add(1, std::memory_order_relaxed);
                socket.wait(tcp::socket::wait_write);
                continue;
            }

            throw asio::system_error(ec);
        }
    } catch (...) {
        socket.non_blocking(previous_non_blocking);
        throw;
    }

    socket.non_blocking(previous_non_blocking);
}

inline bool monitored_write(tcp::socket& socket,
                            const std::string& payload,
                            const SlowConsumerConfig& cfg,
                            SlowConsumerState& state) {
    const auto sequence = state.sent_messages.fetch_add(1, std::memory_order_relaxed) + 1;
    const std::string framed = (cfg.enabled && cfg.sequence_header)
        ? with_sequence_header(payload, sequence)
        : std::string{};
    const std::string& outbound = framed.empty() ? payload : framed;

    state.pending_writes.fetch_add(1, std::memory_order_relaxed);
    const auto before = SlowConsumerState::steady_ms();

    try {
        if (cfg.enabled && cfg.nonblocking_send) {
            raw_nonblocking_write(socket, asio::buffer(outbound), state);
        } else {
            asio::write(socket, asio::buffer(outbound));
        }
    } catch (...) {
        state.pending_writes.fetch_sub(1, std::memory_order_relaxed);
        throw;
    }

    const auto after = SlowConsumerState::steady_ms();
    state.pending_writes.fetch_sub(1, std::memory_order_relaxed);
    state.last_successful_write_ms.store(after, std::memory_order_relaxed);

    const auto pending = pending_socket_bytes(socket);
    return evaluate_slow_consumer(cfg, state, pending, after - before);
}

#endif
