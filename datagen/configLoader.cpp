//
// Created by ttekdogan on 28/07/25.
//
#include "configLoader.hpp"

#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <stdexcept>

namespace {
    template <typename T>
    T get_or(const YAML::Node& n, const std::string& key, T def) {
        if (n[key]) return n[key].as<T>();
        return def;
    }

    std::vector<uint16_t> parse_ports(const YAML::Node& n) {
        std::vector<uint16_t> out;
        if (!n) return out;

        if (n.IsSequence()) {
            out.reserve(n.size());
            for (const auto& v : n) out.push_back(v.as<uint16_t>());
        } else if (n.IsMap()) {
            // sort by key (port1, port2, ...) to have deterministic order
            std::vector<std::pair<std::string,uint16_t>> kv;
            kv.reserve(n.size());
            for (auto it = n.begin(); it != n.end(); ++it) {
                kv.emplace_back(it->first.as<std::string>(), it->second.as<uint16_t>());
            }
            std::sort(kv.begin(), kv.end(), [](auto& a, auto& b){ return a.first < b.first; });
            for (auto& [_, p] : kv) out.push_back(p);
        } else {
            throw std::runtime_error("ports must be a sequence or map");
        }
        return out;
    }

    void validate(const Config& c) {
        if (c.chunk_size <= 0)              throw std::runtime_error("chunk_size must be > 0");
        if (c.data_rate < 0)                throw std::runtime_error("data_rate must be >= 0");
        if (c.duration.count() <= 0)        throw std::runtime_error("duration must be > 0");
        if (c.period.count() <= 0)          throw std::runtime_error("period must be > 0");
        if (c.ports.empty())                throw std::runtime_error("at least one port must be provided");
        if (c.dataset.type != "csv" && c.dataset.type != "image_folder") {
            throw std::runtime_error("data_source.type must be 'csv' or 'image_folder'");
        }
        if (c.dataset.file.empty())         throw std::runtime_error("data_source.file is required");
        if (c.dataset.type == "image_folder"
            && c.dataset.encoding != "base64"
            && c.dataset.encoding != "hex") {
            throw std::runtime_error("image_folder encoding must be 'base64' or 'hex'");
        }
        if (c.mqtt && c.mqtt->enabled) {
            if (c.mqtt->topic.empty())      throw std::runtime_error("mqtt.topic required when mqtt.enabled=true");
        }
        if (c.kafka && c.kafka->enabled) {
            if (c.kafka->brokers.empty())   throw std::runtime_error("kafka.brokers required when kafka.enabled=true");
            if (c.kafka->topic.empty())     throw std::runtime_error("kafka.topic required when kafka.enabled=true");
            if (c.kafka->flush_timeout_ms < 0) throw std::runtime_error("kafka.flush_timeout_ms must be >= 0");
        }
        if (c.slow_consumer.enabled
            && c.slow_consumer.pending_socket_bytes_limit == 0
            && c.slow_consumer.write_duration_warn_ms == 0
            && c.slow_consumer.ack_timeout_ms == 0
            && c.slow_consumer.max_unacked_messages == 0) {
            throw std::runtime_error("slow_consumer must enable at least one threshold when enabled=true");
        }
        if (c.slow_consumer.write_duration_warn_ms < 0) {
            throw std::runtime_error("slow_consumer.write_duration_warn_ms must be >= 0");
        }
        if (c.slow_consumer.ack_timeout_ms < 0) {
            throw std::runtime_error("slow_consumer.ack_timeout_ms must be >= 0");
        }
        if (c.slow_consumer.log_interval_ms < 0) {
            throw std::runtime_error("slow_consumer.log_interval_ms must be >= 0");
        }
    }

    // parse 'pattern' which can be scalar ("zipfian") or map/sequence with "type" plus params
    void parse_pattern(const YAML::Node& p, Config& cfg) {
        if (!p) return;

        auto apply_kv = [&](const std::string& k, const YAML::Node& v) {
            if (k == "type") {
                cfg.pattern = pattern_from_string(v.as<std::string>());
            } else if (k == "fraction") {
                cfg.pattern_params.fraction = v.as<double>();
            } else if (k == "distribution") {
                cfg.pattern_params.distribution = v.as<double>();
            }
        };

        if (p.IsScalar()) {
            cfg.pattern = pattern_from_string(p.as<std::string>());
        } else if (p.IsMap()) {
            for (auto it = p.begin(); it != p.end(); ++it) {
                apply_kv(it->first.as<std::string>(), it->second);
            }
        } else if (p.IsSequence()) {
            for (const auto& item : p) {
                if (!item.IsMap()) continue;
                for (auto it = item.begin(); it != item.end(); ++it) {
                    apply_kv(it->first.as<std::string>(), it->second);
                }
            }
        }
    }
} // namespace

Config load_config(const std::string& path) {
    YAML::Node root = YAML::LoadFile(path);

    Config cfg;

    // Host + duration
    if (root["host"]) cfg.host = root["host"].as<std::string>();
    cfg.duration = std::chrono::seconds(get_or<int>(root, "duration", 60));

    // NEW: period (ms)
    int period_ms = get_or<int>(root, "period", 1000);
    if (!root["period"] && root["interval"]) {
        // legacy alias; keep compatibility but warn
        period_ms = root["interval"].as<int>();
    }
    cfg.period = std::chrono::milliseconds(std::max(1, period_ms));

    // NEW: data_rate = tuples per period
    cfg.data_rate  = get_or<int>(root, "data_rate", 1000);
    cfg.chunk_size = get_or<int>(root, "chunk_size", 10);

    // Pattern + optional params
    if (root["pattern"]) parse_pattern(root["pattern"], cfg);

    // data_source
    if (auto ds = root["data_source"]) {
        cfg.dataset.type = get_or<std::string>(ds, "type", "csv");
        if (ds["file"]) cfg.dataset.file = ds["file"].as<std::string>();
        cfg.dataset.encoding = get_or<std::string>(ds, "encoding", "base64");
    }

    // ports
    cfg.ports = parse_ports(root["ports"]);

    // mqtt (optional)
    if (auto mq = root["mqtt"]) {
        Mqtt m;
        m.enabled = get_or<bool>(mq, "enabled", false);
        m.host    = get_or<std::string>(mq, "host", "localhost");
        m.port    = static_cast<uint16_t>(get_or<int>(mq, "port", 1883));
        if (mq["topic"]) m.topic = mq["topic"].as<std::string>();
        cfg.mqtt = m;
    }

    // kafka (optional)
    if (auto kf = root["kafka"]) {
        Kafka k;
        k.enabled = get_or<bool>(kf, "enabled", false);
        k.brokers = get_or<std::string>(kf, "brokers",
                    get_or<std::string>(kf, "bootstrap_servers", "localhost:9092"));
        if (kf["topic"]) k.topic = kf["topic"].as<std::string>();
        k.flush_timeout_ms = get_or<int>(kf, "flush_timeout_ms", 10000);
        if (auto props = kf["properties"]) {
            if (!props.IsMap()) {
                throw std::runtime_error("kafka.properties must be a map");
            }
            for (auto it = props.begin(); it != props.end(); ++it) {
                k.properties.emplace_back(it->first.as<std::string>(), it->second.as<std::string>());
            }
        }
        cfg.kafka = k;
    }

    // zipf options (optional)
    if (auto z = root["zipf"]) {
        cfg.zipf.period_ms = get_or<int>(z, "period_ms", cfg.period.count());
        cfg.zipf.s = get_or<double>(z, "s", cfg.zipf.s);
        cfg.zipf.stochastic = get_or<bool>(z, "stochastic", cfg.zipf.stochastic);
    } else {
        cfg.zipf.period_ms = cfg.period.count();
    }

    if (auto sc = root["slow_consumer"]) {
        cfg.slow_consumer.enabled = get_or<bool>(sc, "enabled", false);
        const int pending_socket_bytes_limit =
            get_or<int>(sc, "pending_socket_bytes_limit", 1024 * 1024);
        if (pending_socket_bytes_limit < 0) {
            throw std::runtime_error("slow_consumer.pending_socket_bytes_limit must be >= 0");
        }
        cfg.slow_consumer.pending_socket_bytes_limit = static_cast<std::size_t>(pending_socket_bytes_limit);
        cfg.slow_consumer.write_duration_warn_ms =
            get_or<int>(sc, "write_duration_warn_ms", cfg.slow_consumer.write_duration_warn_ms);
        cfg.slow_consumer.ack_timeout_ms =
            get_or<int>(sc, "ack_timeout_ms", cfg.slow_consumer.ack_timeout_ms);
        const int max_unacked_messages = get_or<int>(sc, "max_unacked_messages", 0);
        if (max_unacked_messages < 0) {
            throw std::runtime_error("slow_consumer.max_unacked_messages must be >= 0");
        }
        cfg.slow_consumer.max_unacked_messages = static_cast<std::uint64_t>(max_unacked_messages);
        cfg.slow_consumer.log_interval_ms =
            get_or<int>(sc, "log_interval_ms", cfg.slow_consumer.log_interval_ms);
        cfg.slow_consumer.disconnect_on_slow =
            get_or<bool>(sc, "disconnect_on_slow", cfg.slow_consumer.disconnect_on_slow);
        cfg.slow_consumer.sequence_header =
            get_or<bool>(sc, "sequence_header", cfg.slow_consumer.sequence_header);
        cfg.slow_consumer.nonblocking_send =
            get_or<bool>(sc, "nonblocking_send", cfg.slow_consumer.nonblocking_send);
    }

    validate(cfg);
    return cfg;
}
