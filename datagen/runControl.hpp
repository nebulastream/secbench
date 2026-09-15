#pragma once

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>

namespace secbench {
inline volatile std::sig_atomic_t stop_requested = 0;
inline void request_stop(int) { stop_requested = 1; }

// Opt-in gate: listeners can accept clients while the generator clock is paused.
class RunControl {
    std::filesystem::path start_file;

public:
    RunControl() {
        if (const char* value = std::getenv("SECBENCH_START_FILE"); value && *value) {
            start_file = value;
            if (!start_file.is_absolute() || !std::filesystem::is_directory(start_file.parent_path())
                || std::filesystem::exists(start_file)) {
                throw std::runtime_error("SECBENCH_START_FILE must be a new absolute path in an existing directory");
            }
            stop_requested = 0;
            std::signal(SIGINT, request_stop);
            std::signal(SIGTERM, request_stop);
        }
    }

    template <typename Ready>
    bool wait(Ready ready) const {
        while (!start_file.empty() && !stop_requested) {
            std::error_code error;
            if (std::filesystem::is_regular_file(start_file, error) && ready()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return !stop_requested;
    }

    void run_for(std::chrono::seconds duration, std::atomic<bool>& running) const {
        const auto end = std::chrono::steady_clock::now() + duration;
        while (!stop_requested && running.load(std::memory_order_relaxed)
               && std::chrono::steady_clock::now() < end) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
};
}
