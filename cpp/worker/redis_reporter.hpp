/**
 * redis_reporter.hpp — Push worker metrics to Redis and receive controller commands.
 *
 * Writes the same JSON key format as Python's state/store.py so that
 * the Python FastAPI, Router, and Controller see no difference.
 *
 * Redis key layout (mirrors store.py):
 *   worker:{id}:metrics   → JSON hash of all metrics (SETEX with 30s TTL)
 *   ctrl:commands:{id}    → Pub/Sub channel for controller → worker commands
 *   ctrl:history          → LPUSH/LTRIM list of controller decisions
 *   query:routing:log     → LPUSH/LTRIM list of routing decisions
 */

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>
#include <sw/redis++/redis++.h>

using json = nlohmann::json;

// Metrics snapshot — all fields match what store.py writes
struct WorkerMetrics {
    std::string worker_id;
    double      timestamp;
    int64_t     event_count;
    std::string templ;          // "low" | "medium" | "high"

    // Latency
    double p50_latency_ms;
    double p95_latency_ms;
    double p99_latency_ms;

    // Memory
    double memory_mb;
    double memory_percent;

    // CMS
    double  cms_error_estimate;
    int64_t cms_total_count;
    uint32_t cms_width;
    uint32_t cms_depth;

    // HLL
    int64_t hll_cardinality;
    double  hll_relative_error;
    int     hll_precision;

    // MG
    json    mg_heavy_hitters;  // array of [item, count] pairs
    int64_t mg_window_total;
    double  mg_error_bound;
    int     mg_k;
};

class RedisReporter {
public:
    using CommandCallback = std::function<void(const std::string& action,
                                               const json& payload)>;

    /**
     * @param redis_url      e.g. "redis://localhost:6379"
     * @param worker_id      e.g. "worker-cpp"
     * @param on_command     Callback invoked when a controller command arrives
     */
    RedisReporter(const std::string& redis_url,
                  const std::string& worker_id,
                  CommandCallback    on_command)
        : redis_(redis_url),
          worker_id_(worker_id),
          on_command_(std::move(on_command)),
          running_(true)
    {
        // Start background thread for pub/sub command listening
        sub_thread_ = std::thread([this] { listen_commands(); });
    }

    ~RedisReporter() {
        running_ = false;
        if (sub_thread_.joinable()) sub_thread_.join();
    }

    // Push metrics snapshot to Redis (mirror of store.save_worker_metrics)
    void push_metrics(const WorkerMetrics& m) {
        const std::string key = "worker:" + m.worker_id + ":metrics";
        json payload = {
            {"worker_id",          m.worker_id},
            {"timestamp",          m.timestamp},
            {"event_count",        m.event_count},
            {"template",           m.templ},
            {"p50_latency_ms",     m.p50_latency_ms},
            {"p95_latency_ms",     m.p95_latency_ms},
            {"p99_latency_ms",     m.p99_latency_ms},
            {"memory_mb",          m.memory_mb},
            {"memory_percent",     m.memory_percent},
            {"cms_error_estimate", m.cms_error_estimate},
            {"cms_total_count",    m.cms_total_count},
            {"cms_width",          m.cms_width},
            {"cms_depth",          m.cms_depth},
            {"hll_cardinality",    m.hll_cardinality},
            {"hll_relative_error", m.hll_relative_error},
            {"hll_precision",      m.hll_precision},
            {"mg_heavy_hitters",   m.mg_heavy_hitters},
            {"mg_window_total",    m.mg_window_total},
            {"mg_error_bound",     m.mg_error_bound},
            {"mg_k",               m.mg_k},
        };

        // SET with 30-second expiry, same as store.py
        redis_.setex(key, std::chrono::seconds(30), payload.dump());

        // Maintain latency history list
        const std::string hist_key = "worker:" + m.worker_id + ":latency_history";
        redis_.lpush(hist_key, std::to_string(m.p99_latency_ms));
        redis_.ltrim(hist_key, 0, 299);  // keep last 300 samples
    }

private:
    // Background thread: subscribe to Redis pub/sub and dispatch commands
    void listen_commands() {
        try {
            auto sub = redis_.subscriber();
            const std::string channel = "ctrl:commands:" + worker_id_;
            sub.subscribe(channel);

            sub.on_message([this](std::string /*channel*/, std::string msg) {
                if (!running_) return;
                try {
                    json cmd = json::parse(msg);
                    on_command_(cmd.value("action", ""), cmd);
                } catch (const std::exception& e) {
                    std::cerr << "[RedisReporter] Command parse error: "
                              << e.what() << '\n';
                }
            });

            while (running_) {
                try {
                    sub.consume();
                } catch (const sw::redis::Error& e) {
                    if (running_)
                        std::cerr << "[RedisReporter] Subscriber error: "
                                  << e.what() << '\n';
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[RedisReporter] Listen thread crashed: " << e.what() << '\n';
        }
    }

    sw::redis::Redis  redis_;
    std::string       worker_id_;
    CommandCallback   on_command_;
    std::atomic<bool> running_;
    std::thread       sub_thread_;
};
