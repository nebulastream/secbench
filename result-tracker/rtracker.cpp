#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include <atomic>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace {

std::atomic<bool> g_stop_requested{false};
std::atomic<int> g_listen_fd{-1};
std::atomic<std::uint64_t> g_sample_counter{0};

std::string trim(const std::string& input) {
    const auto begin = input.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return {};
    }
    const auto end = input.find_last_not_of(" \t\r\n");
    return input.substr(begin, end - begin + 1);
}

std::string strip_quotes(const std::string& input) {
    if (input.size() >= 2) {
        const char first = input.front();
        const char last = input.back();
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            return input.substr(1, input.size() - 2);
        }
    }
    return input;
}

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void signal_handler(int) {
    g_stop_requested.store(true);
    const int fd = g_listen_fd.exchange(-1);
    if (fd >= 0) {
        close(fd);
    }
}

struct AppConfig {
    std::string connection_type = "tcp";
    std::string connection_mode = "server";
    std::string tcp_host = "0.0.0.0";
    int tcp_port = 9000;
    int tcp_backlog = 64;
    int tcp_read_buffer_bytes = 4096;
    int sample = 100;
    int latency_worker_threads = 2;
    std::string spool_file = "timestamped_tuples.log";
    std::string latency_output_file = "latency_output.log";
    bool print_to_terminal = true;
    bool print_only_latency = false;
    bool print_consumed_tuples = false;
};

class SimpleYamlConfig {
public:
    static SimpleYamlConfig load(const std::string& path) {
        std::ifstream input(path);
        if (!input) {
            throw std::runtime_error("Could not open config file: " + path);
        }

        SimpleYamlConfig config;
        std::string line;
        std::string current_section;
        std::size_t line_number = 0;

        while (std::getline(input, line)) {
            ++line_number;
            const auto comment_pos = line.find('#');
            if (comment_pos != std::string::npos) {
                line = line.substr(0, comment_pos);
            }

            const std::string cleaned = trim(line);
            if (cleaned.empty()) {
                continue;
            }

            if (cleaned.back() == ':' && cleaned.find(':') == cleaned.size() - 1) {
                current_section = trim(cleaned.substr(0, cleaned.size() - 1));
                continue;
            }

            const auto colon_pos = cleaned.find(':');
            if (colon_pos == std::string::npos) {
                throw std::runtime_error("Invalid YAML line " + std::to_string(line_number) + ": " + cleaned);
            }

            std::string key = trim(cleaned.substr(0, colon_pos));
            std::string value = strip_quotes(trim(cleaned.substr(colon_pos + 1)));
            if (!current_section.empty()) {
                key = current_section + "." + key;
            }
            config.values_[key] = value;
        }

        return config;
    }

    std::string get_string(const std::string& key, const std::string& fallback) const {
        const auto it = values_.find(key);
        return it == values_.end() ? fallback : it->second;
    }

    int get_int(const std::string& key, int fallback) const {
        const auto it = values_.find(key);
        if (it == values_.end()) {
            return fallback;
        }
        return std::stoi(it->second);
    }

    bool get_bool(const std::string& key, bool fallback) const {
        const auto it = values_.find(key);
        if (it == values_.end()) {
            return fallback;
        }

        const std::string value = it->second;
        if (value == "true" || value == "True" || value == "1" || value == "yes") {
            return true;
        }
        if (value == "false" || value == "False" || value == "0" || value == "no") {
            return false;
        }
        throw std::runtime_error("Invalid boolean for key " + key + ": " + value);
    }

private:
    std::unordered_map<std::string, std::string> values_;
};

AppConfig load_app_config(const std::string& path) {
    const SimpleYamlConfig yaml = SimpleYamlConfig::load(path);

    AppConfig config;
    config.connection_type = yaml.get_string("connection.type", config.connection_type);
    config.connection_mode = yaml.get_string("connection.mode", config.connection_mode);
    config.tcp_host = yaml.get_string("connection.host", config.tcp_host);
    config.tcp_port = yaml.get_int("connection.port", config.tcp_port);
    config.tcp_backlog = yaml.get_int("connection.backlog", config.tcp_backlog);
    config.tcp_read_buffer_bytes = yaml.get_int("connection.read_buffer_bytes", config.tcp_read_buffer_bytes);
    config.sample = yaml.get_int("sample", config.sample);
    config.sample = yaml.get_int("processing.sample", config.sample);
    config.latency_worker_threads = yaml.get_int("processing.latency_worker_threads", config.latency_worker_threads);
    config.spool_file = yaml.get_string("processing.spool_file", config.spool_file);
    config.latency_output_file = yaml.get_string("processing.latency_output_file", config.latency_output_file);
    config.print_to_terminal = yaml.get_bool("processing.print_to_terminal", config.print_to_terminal);
    config.print_only_latency = yaml.get_bool("processing.print_only_latency", config.print_only_latency);
    config.print_consumed_tuples = yaml.get_bool("processing.print_consumed_tuples", config.print_consumed_tuples);

    if (config.tcp_port <= 0 || config.tcp_port > 65535) {
        throw std::runtime_error("TCP port must be between 1 and 65535");
    }
    if (config.latency_worker_threads <= 0) {
        throw std::runtime_error("processing.latency_worker_threads must be positive");
    }
    if (config.tcp_read_buffer_bytes <= 0) {
        throw std::runtime_error("connection.read_buffer_bytes must be positive");
    }
    if (config.connection_mode != "server" && config.connection_mode != "client") {
        throw std::runtime_error("connection.mode must be either server or client");
    }
    if (config.sample < 0 || config.sample > 100) {
        throw std::runtime_error("processing.sample must be between 0 and 100");
    }

    return config;
}

template <typename T>
class BlockingQueue {
public:
    void push(T value) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(std::move(value));
        }
        cv_.notify_one();
    }

    bool pop(T& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false;
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        cv_.notify_all();
    }

private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool closed_ = false;
};

class SpoolFile {
public:
    explicit SpoolFile(const std::string& path) : path_(path), output_(path, std::ios::trunc) {
        if (!output_) {
            throw std::runtime_error("Could not open spool file: " + path);
        }
    }

    void append_line(const std::string& line) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            output_ << line << '\n';
            output_.flush();
            ++line_count_;
        }
        cv_.notify_one();
    }

    bool wait_for_new_data(std::size_t consumed_lines, std::size_t& available_lines) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return closed_ || consumed_lines < line_count_; });
        available_lines = line_count_;
        return !(closed_ && consumed_lines >= line_count_);
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
            output_.flush();
        }
        cv_.notify_all();
    }

    const std::string& path() const {
        return path_;
    }

private:
    std::string path_;
    std::ofstream output_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t line_count_ = 0;
    bool closed_ = false;
};

class LatencySink {
public:
    LatencySink(const std::string& path, bool print_to_terminal)
        : output_(path, std::ios::trunc), print_to_terminal_(print_to_terminal) {
        if (!output_) {
            throw std::runtime_error("Could not open latency output file: " + path);
        }
    }

    void write(const std::string& line) {
        std::lock_guard<std::mutex> lock(mutex_);
        output_ << line << '\n';
        output_.flush();
        if (print_to_terminal_) {
            std::cout << line << '\n' << std::flush;
        }
    }

private:
    std::ofstream output_;
    bool print_to_terminal_;
    std::mutex mutex_;
};

class ConsumedTupleSink {
public:
    explicit ConsumedTupleSink(const std::string& path) : output_(path, std::ios::trunc) {
        if (!output_) {
            throw std::runtime_error("Could not open consumed tuple output file: " + path);
        }
    }

    void increment() {
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    void stop() {
        stop_requested_.store(true, std::memory_order_relaxed);
    }

    void run() {
        while (!stop_requested_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            flush_current_second();
        }
        flush_current_second();
    }

private:
    void flush_current_second() {
        const auto count = count_.exchange(0, std::memory_order_relaxed);
        output_ << now_ms() << "," << count << '\n';
        output_.flush();
    }

    std::ofstream output_;
    std::atomic<std::uint64_t> count_{0};
    std::atomic<bool> stop_requested_{false};
};

std::optional<std::pair<std::int64_t, std::int64_t>> extract_timestamps(const std::string& staged_tuple) {
    const auto last_comma = staged_tuple.rfind(',');
    if (last_comma == std::string::npos) {
        return std::nullopt;
    }

    const auto previous_comma = staged_tuple.rfind(',', last_comma - 1);
    if (previous_comma == std::string::npos) {
        return std::nullopt;
    }

    try {
        const auto original_ts = std::stoll(trim(staged_tuple.substr(previous_comma + 1, last_comma - previous_comma - 1)));
        const auto assigned_ts = std::stoll(trim(staged_tuple.substr(last_comma + 1)));
        return std::make_pair(original_ts, assigned_ts);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

void spool_reader_loop(SpoolFile& spool, BlockingQueue<std::string>& latency_queue) {
    std::ifstream input(spool.path());
    if (!input) {
        throw std::runtime_error("Could not open spool file for reading: " + spool.path());
    }

    std::size_t consumed_lines = 0;
    std::size_t available_lines = 0;
    std::string line;

    while (spool.wait_for_new_data(consumed_lines, available_lines)) {
        input.clear();
        while (consumed_lines < available_lines && std::getline(input, line)) {
            latency_queue.push(line);
            ++consumed_lines;
        }
    }

    input.clear();
    while (std::getline(input, line)) {
        latency_queue.push(line);
    }

    latency_queue.close();
}

void latency_worker_loop(BlockingQueue<std::string>& latency_queue, LatencySink& sink, bool print_only_latency) {
    std::string staged_tuple;
    while (latency_queue.pop(staged_tuple)) {
        const auto timestamps = extract_timestamps(staged_tuple);
        if (!timestamps.has_value()) {
            sink.write("invalid_tuple," + staged_tuple);
            continue;
        }

        const auto [source_ts, assigned_ts] = timestamps.value();
        const auto latency_ms = assigned_ts - source_ts;
        if (print_only_latency) {
            sink.write(std::to_string(latency_ms));
        } else {
            sink.write(std::to_string(latency_ms) + "," + staged_tuple);
        }
    }
}

bool should_sample_tuple(int sample) {
    if (sample >= 100) {
        return true;
    }
    if (sample <= 0) {
        return false;
    }

    const auto sequence = g_sample_counter.fetch_add(1, std::memory_order_relaxed);
    return sequence % 100 < static_cast<std::uint64_t>(sample);
}

int create_server_socket(const AppConfig& config) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("socket() failed: " + std::string(std::strerror(errno)));
    }

    int enable = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0) {
        close(fd);
        throw std::runtime_error("setsockopt(SO_REUSEADDR) failed: " + std::string(std::strerror(errno)));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(config.tcp_port));
    if (inet_pton(AF_INET, config.tcp_host.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        throw std::runtime_error("Invalid IPv4 host: " + config.tcp_host);
    }

    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        const std::string error = std::strerror(errno);
        close(fd);
        throw std::runtime_error("bind() failed: " + error);
    }

    if (listen(fd, config.tcp_backlog) < 0) {
        const std::string error = std::strerror(errno);
        close(fd);
        throw std::runtime_error("listen() failed: " + error);
    }

    return fd;
}

void handle_client(int client_fd, const AppConfig& config, SpoolFile& spool, ConsumedTupleSink* consumed_tuple_sink) {
    timeval timeout{};
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    std::vector<char> buffer(static_cast<std::size_t>(config.tcp_read_buffer_bytes));
    std::string pending;

    while (!g_stop_requested.load()) {
        const ssize_t bytes = recv(client_fd, buffer.data(), buffer.size(), 0);
        if (bytes == 0) {
            break;
        }
        if (bytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                continue;
            }
            std::cerr << "recv() failed: " << std::strerror(errno) << '\n';
            break;
        }

        pending.append(buffer.data(), static_cast<std::size_t>(bytes));
        std::size_t newline_pos = 0;
        while ((newline_pos = pending.find('\n')) != std::string::npos) {
            std::string tuple = pending.substr(0, newline_pos);
            pending.erase(0, newline_pos + 1);

            if (!tuple.empty() && tuple.back() == '\r') {
                tuple.pop_back();
            }
            tuple = trim(tuple);
            if (tuple.empty()) {
                continue;
            }
            if (consumed_tuple_sink != nullptr) {
                consumed_tuple_sink->increment();
            }
            if (!should_sample_tuple(config.sample)) {
                continue;
            }

            const std::string staged_tuple = tuple + "," + std::to_string(now_ms());
            spool.append_line(staged_tuple);
        }
    }

    close(client_fd);
}

void run_tcp_server(const AppConfig& config, SpoolFile& spool, ConsumedTupleSink* consumed_tuple_sink) {
    const int server_fd = create_server_socket(config);
    g_listen_fd.store(server_fd);

    std::cout << "Listening on " << config.tcp_host << ":" << config.tcp_port << '\n' << std::flush;

    std::vector<std::thread> client_threads;
    while (!g_stop_requested.load()) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        const int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (g_stop_requested.load() || errno == EINTR) {
                continue;
            }
            if (errno == EBADF || errno == EINVAL) {
                break;
            }
            std::cerr << "accept() failed: " << std::strerror(errno) << '\n';
            continue;
        }

        client_threads.emplace_back(handle_client, client_fd, std::cref(config), std::ref(spool), consumed_tuple_sink);
    }

    const int fd = g_listen_fd.exchange(-1);
    if (fd >= 0) {
        close(fd);
    }

    for (auto& thread : client_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

int create_client_socket(const AppConfig& config) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("socket() failed: " + std::string(std::strerror(errno)));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(config.tcp_port));
    if (inet_pton(AF_INET, config.tcp_host.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        throw std::runtime_error("Invalid IPv4 host: " + config.tcp_host);
    }

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        const std::string error = std::strerror(errno);
        close(fd);
        throw std::runtime_error("connect() failed: " + error);
    }

    return fd;
}

void run_tcp_client(const AppConfig& config, SpoolFile& spool, ConsumedTupleSink* consumed_tuple_sink) {
    const int client_fd = create_client_socket(config);
    std::cout << "Connected to " << config.tcp_host << ":" << config.tcp_port << '\n' << std::flush;
    handle_client(client_fd, config, spool, consumed_tuple_sink);
}

} // namespace

int main(int argc, char** argv) {
    const std::string config_path = argc > 1 ? argv[1] : "config.yaml";

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        const AppConfig config = load_app_config(config_path);
        if (config.connection_type != "tcp") {
            std::cerr << "Configured connection.type=" << config.connection_type
                      << ". Only TCP is implemented in this version.\n";
            return EXIT_FAILURE;
        }

        SpoolFile spool(config.spool_file);
        BlockingQueue<std::string> latency_queue;
        LatencySink sink(config.latency_output_file, config.print_to_terminal);
        std::optional<ConsumedTupleSink> consumed_tuple_sink;
        std::optional<std::thread> consumed_tuple_thread;
        if (config.print_consumed_tuples) {
            consumed_tuple_sink.emplace("consumed_tuples.txt");
            consumed_tuple_thread.emplace(&ConsumedTupleSink::run, &consumed_tuple_sink.value());
        }

        std::thread spool_reader(spool_reader_loop, std::ref(spool), std::ref(latency_queue));

        std::vector<std::thread> latency_workers;
        latency_workers.reserve(static_cast<std::size_t>(config.latency_worker_threads));
        for (int i = 0; i < config.latency_worker_threads; ++i) {
            latency_workers.emplace_back(
                latency_worker_loop,
                std::ref(latency_queue),
                std::ref(sink),
                config.print_only_latency);
        }

        int exit_code = EXIT_SUCCESS;
        try {
            if (config.connection_mode == "server") {
                run_tcp_server(config, spool, consumed_tuple_sink ? &consumed_tuple_sink.value() : nullptr);
            } else {
                run_tcp_client(config, spool, consumed_tuple_sink ? &consumed_tuple_sink.value() : nullptr);
            }
        } catch (const std::exception& ex) {
            std::cerr << "Fatal error: " << ex.what() << '\n';
            exit_code = EXIT_FAILURE;
        }

        spool.close();
        if (consumed_tuple_sink) {
            consumed_tuple_sink->stop();
        }
        if (spool_reader.joinable()) {
            spool_reader.join();
        }
        if (consumed_tuple_thread && consumed_tuple_thread->joinable()) {
            consumed_tuple_thread->join();
        }

        for (auto& worker : latency_workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        return exit_code;
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << '\n';
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
