#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <asio.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <atomic>
#include <cstdint>
#include <mosquitto.h>
#include <random>
#include "include/yaml-cpp/yaml.h"
#include "Util/Yaml.h"

#include "configLoader.hpp"

using asio::ip::tcp;

// thread-safe queue for events
using MessageQueue = std::queue<std::string>;
static MessageQueue mqtt_queue;
static std::mutex queue_mutex;
static std::condition_variable queue_cv;

using AtomicBool = std::atomic<bool>;

// nanoseconds: 19
// microseconds: 16
// milliseconds: 13
constexpr int TS_WIDTH = 13;

inline int64_t now_nanos() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// this builds a batch buffer with placeholders for timestamps
// idx is advanced circularly through dataset.
struct Batch {
    std::string buf; // the serialized batch
    std::vector<size_t> ts_pos; // offsets in buf where timestamps must be written
};

static thread_local std::mt19937_64 rng{
    (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count()
};

Batch build_batch_with_placeholders(const std::vector<std::string>& dataset,
                                    size_t& idx,
                                    int m)
{
    Batch b;
    b.ts_pos.reserve(m);

    // Rough reserve to avoid reallocations
    size_t reserve_bytes = 0;
    for (int i = 0, j = idx; i < m; ++i) {
        reserve_bytes += dataset[j].size() + 1 /*comma*/ + TS_WIDTH + 1 /*newline*/;
        j = (j + 1) % dataset.size();
    }
    b.buf.reserve(reserve_bytes);

    for (int i = 0; i < m; ++i) {
        const std::string& line = dataset[idx];
        b.buf.append(line);
        b.buf.push_back(',');

        // record where the timestamp will start in the buffer
        size_t pos = b.buf.size();
        b.ts_pos.push_back(pos);

        // write placeholder bytes (to be overridden)
        b.buf.append(TS_WIDTH, '0');

        b.buf.push_back('\n');

        idx = (idx + 1) % dataset.size();
    }
    return b;
}

/// stamps the same timestamp into all placeholder positions
void fillTimestamps(std::string& buf,
                             const std::vector<size_t>& ts_pos,
                             int64_t ts_value)
{
    char ts[32];
    auto [ptr, ec] = std::to_chars(ts, ts + sizeof(ts), ts_value);
    (void)ec; // integer to_chars won't fail with sufficient buffer
    const auto ts_len = static_cast<size_t>(ptr - ts);

    // Left‑pad with '0' if timestamp has fewer digits than TS_WIDTH
    // (keeps field width constant; CSV parsers accept leading zeros)
    for (const auto pos : ts_pos) {
        if (ts_len >= static_cast<size_t>(TS_WIDTH)) {
            // If ever longer (unlikely), copy the rightmost TS_WIDTH digits
            std::memcpy(&buf[pos], ts + (ts_len - TS_WIDTH), TS_WIDTH);
        } else {
            size_t pad = TS_WIDTH - ts_len;
            std::memset(&buf[pos], '0', pad);
            std::memcpy(&buf[pos + pad], ts, ts_len);
        }
    }
}

// disable Nagle to avoid any interaction with batching
void configure_socket(tcp::socket& socket) {
    const asio::ip::tcp::no_delay nd(true);
    socket.set_option(nd);
}

// Minimal usage example (no server loop here):
// Given a connected socket, dataset, and starting index, send m tuples
// with one timestamp (captured immediately before publish).
void send_batch_same_timestamp(tcp::socket& sock,
                               const std::vector<std::string>& dataset,
                               size_t& idx,
                               int m)
{
    // 1) Build batch with placeholders
    Batch b = build_batch_with_placeholders(dataset, idx, m);

    // 2) Capture *one* timestamp immediately before publish
    int64_t t = now_nanos();

    // 3) Overwrite placeholders with the same timestamp
    fillTimestamps(b.buf, b.ts_pos, t);

    // 4) Single syscall to publish
    asio::write(sock, asio::buffer(b.buf));
}



Config load_config(const std::string& path);

// read CSV file into memory (skip header if present)
std::vector<std::string> read_csv(const std::string& filename) {
    std::vector<std::string> dataset;
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << "\n";
        return dataset;
    }

    std::string line;

    // skip header line
    std::getline(file, line);

    std::cout << "Loading the dataset into memory..." << "\n";

    while (std::getline(file, line)) {
        // trim '\r' from Windows-style files
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) continue;
        dataset.emplace_back(line);
    }
    std::cout << "Loaded " << dataset.size()
              << " rows into memory, first row='" << dataset.front() << "'\n";
    return dataset;
}

// Handle TCP clients: send each tuple individually and enqueue for MQTT
void handle_client(tcp::socket socket,
                   const std::vector<std::string>& dataset,
                   int chunk_size,
                   int data_rate,
                   AtomicBool& running,
                   bool mqtt_enabled) {
    try {
        std::cout << "Client connected: " << socket.remote_endpoint() << "\n";
        size_t idx = 0;
        const size_t total = dataset.size();
        const int interval_ms = data_rate;

        while (running.load()) {
            for (int i = 0; i < chunk_size; ++i) {
                // generate timestamp in ms
                auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                std::ostringstream oss;
                oss << dataset[idx] << "," << now_ms/10;
                const std::string msg = oss.str();

                // send tuple over TCP
                asio::write(socket, asio::buffer(msg));

                // enqueue tuple for MQTT
                if (mqtt_enabled) {
                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        mqtt_queue.push(msg);
                    }
                    queue_cv.notify_one();
                }

                idx = (idx + 1) % total;

            }

        // optional per-tuple rate control
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        }

        socket.close();
        std::cout << "Client disconnected.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in client connection: " << e.what() << "\n";
    }
}

// new
void handle_client_new(tcp::socket socket,
                   const std::vector<std::string>& dataset,
                   int chunk_size,
                   int data_rate,
                   AtomicBool& running,
                   bool mqtt_enabled,
                   int interval_ms) {
    try {
        configure_socket(socket); // disable Nagle; reduce jitter
        std::cout << "Client connected: " << socket.remote_endpoint() << "\n";

        size_t idx = 0;
        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        // Derive batch interval from desired tuple rate.
        // data_rate = tuples/sec, chunk_size = tuples/batch
        // => batches/sec = data_rate / chunk_size
        // => interval_ms = 1000 * chunk_size / data_rate
        //const int tuples_per_sec   = std::max(1, data_rate);
        //const int tuples_per_batch = std::max(1, chunk_size);
        const auto interval = std::chrono::milliseconds(
            std::max(1,interval_ms));

        auto next = std::chrono::steady_clock::now();

        while (running.load()) {
            // 1) Build batch with placeholders; advances idx circularly
            Batch b = build_batch_with_placeholders(dataset, idx, chunk_size);

            // 2) Capture ONE timestamp right before publish
            const int64_t t = now_nanos();

            // 3) Fill the same timestamp into all tuples
            fillTimestamps(b.buf, b.ts_pos, t);

            // 4) Publish in one syscall
            asio::write(socket, asio::buffer(b.buf));

            // 5) Optionally mirror to MQTT (move to avoid copy)
            if (mqtt_enabled) {
                {
                    std::lock_guard<std::mutex> lk(queue_mutex);
                    mqtt_queue.emplace(std::move(b.buf));
                }
                queue_cv.notify_one();
            }

            // 6) Pace sending (sleep-until to minimize drift)
            next += interval;
            std::this_thread::sleep_until(next);
        }

        socket.close();
        std::cout << "Client disconnected.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in client connection: " << e.what() << "\n";
    }
}

void handle_client_poisson(tcp::socket socket,
                   const std::vector<std::string>& dataset,
                   int data_rate_tuples_per_sec,
                   int max_batch_size,                 // cap burst (safety)
                   std::chrono::milliseconds tick,     // e.g., 1ms, 10ms, 100ms, 1s
                   std::atomic<bool>& running)
{
    configure_socket(socket);

    size_t idx = 0;
    const double dt_sec = tick.count() / 1000.0;
    const double lambda_tick = std::max(0.0, data_rate_tuples_per_sec * dt_sec);

    std::poisson_distribution<int> pois(lambda_tick);

    auto next = std::chrono::steady_clock::now();

    while (running.load(std::memory_order_relaxed)) {
        next += tick;

        // Sample how many tuples to send in THIS tick
        int k = pois(rng);
        if (max_batch_size > 0) k = std::min(k, max_batch_size);

        if (k > 0) {
            // Build k tuples with reserved timestamp slots
            Batch b = build_batch_with_placeholders(dataset, idx, k);

            // One timestamp for the whole tick, taken right before publish
            const int64_t ts = now_nanos();
            fillTimestamps(b.buf, b.ts_pos, ts);

            asio::write(socket, asio::buffer(b.buf));
        }

        std::this_thread::sleep_until(next);
    }
}

// TCP server thread: accept connections and spawn handlers
void startTcpServer(const std::string& host,
                      int port,
                      const std::vector<std::string>& dataset,
                      int chunk_size,
                      int data_rate,
                      AtomicBool& running,
                      bool mqtt_enabled,
                      int interval_ms) {
    try {
        asio::io_context io_context;
        tcp::acceptor acceptor(io_context, tcp::endpoint(asio::ip::make_address(host), port));
        std::cout << "TCP connection ready at " << host << ":" << port << "\n";
        while (running.load()) {
            tcp::socket sock(io_context);
            acceptor.accept(sock);
            std::thread(
                  handle_client_poisson,
                        std::move(sock),
                        std::cref(dataset),
                        data_rate,
                        data_rate*4,
                        std::chrono::milliseconds(100),
                        std::ref(running)
                        ).detach();
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in TCP server: " << e.what() << "\n";
    }
}

// MQTT publisher thread: dequeue and publish messages one by one
void mqtt_publisher(mosquitto* client,
                    AtomicBool& running,
                    const std::string& topic) {
    while (running.load() || !mqtt_queue.empty()) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cv.wait(lock, []{
            return !mqtt_queue.empty() || std::uncaught_exceptions() > 0;
        });
        while (!mqtt_queue.empty()) {
            std::string msg = std::move(mqtt_queue.front());
            mqtt_queue.pop();
            lock.unlock();

            int ret = mosquitto_publish(client,
                                        nullptr,
                                        topic.c_str(),
                                        static_cast<int>(msg.size()),
                                        msg.c_str(),
                                        1,
                                        false);
            if (ret != MOSQ_ERR_SUCCESS) {
                std::cerr << "MQTT publish error: " << mosquitto_strerror(ret) << "\n";
            }

            lock.lock();
        }
    }
}

// Read YAML configuration
bool read_config(const std::string& cfg,
                 std::string& host,
                 int& interval,
                 std::string& data_file,
                 int& duration,
                 int& data_rate,
                 int& chunk_size,
                 std::vector<int>& ports,
                 bool& mqtt_enabled,
                 std::string& mqtt_host,
                 int& mqtt_port,
                 std::string& mqtt_topic) {
    try {
        YAML::Node config = YAML::LoadFile(cfg);
        host        = config["host"].as<std::string>();
        interval    = config["interval"].as<int>();
        data_file   = config["data_source"]["file"].as<std::string>();
        duration    = config["duration"].as<int>();
        data_rate   = config["data_rate"].as<int>();
        chunk_size  = config["chunk_size"].as<int>();
        for (auto p : config["ports"]) {
            ports.push_back(p.as<int>());
        }
        if (config["mqtt"]["enabled"].IsDefined()) {
            mqtt_enabled = config["mqtt"]["enabled"].as<bool>();
            if (mqtt_enabled) {
                mqtt_host  = config["mqtt"]["host"].as<std::string>();
                mqtt_port  = config["mqtt"]["port"].as<int>();
                mqtt_topic = config["mqtt"]["topic"].as<std::string>();
            }
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Config read error: " << e.what() << "\n";
        return false;
    }
}

int main(int argc, char *argv[]) {

    std::vector<std::string> ds = {"1220,0,77,53.0", "1240,0,70,78.0", "1260,0,78,103.0", "1280,0,79,102.3"};
    size_t idx = 0;
    Batch b = build_batch_with_placeholders(ds, idx, 3);
    int64_t t = now_nanos();
    fillTimestamps(b.buf, b.ts_pos, t);

    std::cout << now_nanos() << std::endl;

    std::cout << b.buf;

    auto configPath = "config.yaml";
    if(argc > 1)
    {
        configPath = argv[1];
    }

    Config config;
    try {
        config = load_config(configPath);
    } catch (const std::exception& e) {
        std::cerr << "Config error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }

    std::string host, data_file, mqtt_host, mqtt_topic;
    int interval = 0, duration = 0, data_rate = 0, chunk_size = 0, mqtt_port = 0;
    std::vector<int> ports;
    bool mqtt_enabled = false;


    if (!read_config(configPath, host, interval, data_file,
                     duration, data_rate, chunk_size,
                     ports, mqtt_enabled,
                     mqtt_host, mqtt_port, mqtt_topic)) {
        return EXIT_FAILURE;
    }

    //auto dataset = config.dataset;
    auto dataset = read_csv(data_file);
//    if (dataset.empty()) {
//        std::cerr << "No data loaded.\n";
//        return EXIT_FAILURE;
//    }

    mosquitto* mqtt_client = nullptr;
    std::thread mqtt_thread;
    AtomicBool running(true);

    if (mqtt_enabled) {
        mosquitto_lib_init();
        mqtt_client = mosquitto_new(nullptr, true, nullptr);
        if (!mqtt_client ||
            mosquitto_connect(mqtt_client, mqtt_host.c_str(), mqtt_port, 60) != MOSQ_ERR_SUCCESS) {
            std::cerr << "MQTT init error\n";
            return EXIT_FAILURE;
        }
        mosquitto_loop_start(mqtt_client);
        mqtt_thread = std::thread(mqtt_publisher,
                                  mqtt_client,
                                  std::ref(running),
                                  mqtt_topic);
    }

    std::vector<std::thread> tcp_threads;
    for (int port : config.ports) {
        tcp_threads.emplace_back(startTcpServer,
                                 config.host,
                                 port,
                                 dataset,
                                 config.chunk_size,
                                 config.data_rate,
                                 std::ref(running),
                                 config.mqtt->enabled,
                                 interval);
    }

    std::this_thread::sleep_for(std::chrono::seconds(config.duration));
    running.store(false);
    queue_cv.notify_all();

    for (auto& t : tcp_threads) {
        if (t.joinable()) t.join();
    }

    if (mqtt_enabled) {
        if (mqtt_thread.joinable()) mqtt_thread.join();
        mosquitto_loop_stop(mqtt_client, true);
        mosquitto_disconnect(mqtt_client);
        mosquitto_destroy(mqtt_client);
        mosquitto_lib_cleanup();
    }

    std::cout << "exiting datagen...\n";
    return EXIT_SUCCESS;
}