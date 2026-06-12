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

    // TODO: Skip header line
    std::getline(file, line);

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

// TCP server thread: accept connections and spawn handlers
void startTcpServer(const std::string& host,
                      int port,
                      const std::vector<std::string>& dataset,
                      int chunk_size,
                      int data_rate,
                      AtomicBool& running,
                      bool mqtt_enabled) {
    try {
        asio::io_context io_context;
        tcp::acceptor acceptor(io_context, tcp::endpoint(asio::ip::make_address(host), port));
        std::cout << "TCP connection ready at " << host << ":" << port << "\n";
        while (running.load()) {
            tcp::socket sock(io_context);
            acceptor.accept(sock);
            std::thread(handle_client,
                        std::move(sock),
                        std::cref(dataset),
                        chunk_size,
                        data_rate,
                        std::ref(running),
                        mqtt_enabled).detach();
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
                                 config.mqtt->enabled);
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