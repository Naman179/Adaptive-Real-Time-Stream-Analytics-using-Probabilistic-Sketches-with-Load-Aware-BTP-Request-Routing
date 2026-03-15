/**
 * worker.cpp — High-performance C++ Analytics Worker.
 *
 * Consumes from a Kafka topic and updates three probabilistic sketches:
 *   - CountMinSketch  (frequency estimation)
 *   - HyperLogLog     (cardinality estimation)
 *   - SlidingWindowMisraGries (heavy-hitter detection)
 *
 * Periodically reports metrics to Redis so the Python FastAPI/Router/Controller
 * can read them — using the exact same Redis key format as the Python worker.
 *
 * Listens on a Redis Pub/Sub channel for template change commands from
 * the Python AdaptiveController.
 *
 * Usage:
 *   btp_worker --worker-id worker-cpp --template medium
 */

#include <algorithm>
#include <iomanip>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

#include "../sketches/count_min_sketch.hpp"
#include "../sketches/hyperloglog.hpp"
#include "../sketches/misra_gries.hpp"
#include "redis_reporter.hpp"

using json = nlohmann::json;
// NOTE: Clock is a private alias inside SlidingWindowMisraGries.
// Use std::chrono::steady_clock directly in this translation unit.
using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::optional<std::string> getenv_str(const char* name) {
    const char* val = std::getenv(name);
    if (val) return std::string(val);
    return std::nullopt;
}



// ---------------------------------------------------------------------------
// Template definitions (match Python worker.py TEMPLATES dict)
// ---------------------------------------------------------------------------

struct TemplateParams {
    uint32_t cms_width;
    uint32_t cms_depth;
    int      hll_precision;
    int      mg_k;
};

static const std::unordered_map<std::string, TemplateParams> TEMPLATES = {
    {"low",    {512,  3, 10, 50 }},
    {"medium", {2048, 5, 12, 200}},
    {"high",   {8192, 7, 14, 500}},
};

// ---------------------------------------------------------------------------
// Percentile helper
// ---------------------------------------------------------------------------

static double percentile(std::vector<double> data, double p) {
    if (data.empty()) return 0.0;
    std::sort(data.begin(), data.end());
    const size_t idx = static_cast<size_t>(data.size() * p / 100.0);
    return data[std::min(idx, data.size() - 1)];
}

// ---------------------------------------------------------------------------
// Memory helper (Linux: reads /proc/self/status)
// ---------------------------------------------------------------------------

static double rss_mb() {
    // Simple cross-platform fallback — on Linux reads /proc/self/status
#if defined(__linux__)
    FILE* f = std::fopen("/proc/self/status", "r");
    if (!f) return 0.0;
    char line[256];
    while (std::fgets(line, sizeof(line), f)) {
        if (std::strncmp(line, "VmRSS:", 6) == 0) {
            std::fclose(f);
            long kb = 0;
            std::sscanf(line + 6, " %ld", &kb);
            return static_cast<double>(kb) / 1024.0;
        }
    }
    std::fclose(f);
#endif
    return 0.0;
}

// ---------------------------------------------------------------------------
// Analytics Worker
// ---------------------------------------------------------------------------

class AnalyticsWorker {
public:
    AnalyticsWorker(std::string worker_id, const std::string& templ_name)
        : worker_id_(std::move(worker_id)),
          running_(true),
          event_count_(0)
    {
        apply_template(templ_name);
        connect_kafka();

        // Redis reporter + command callback
        const std::string redis_url =
            getenv_str("REDIS_URL").value_or("redis://localhost:6379");

        reporter_ = std::make_unique<RedisReporter>(
            redis_url, worker_id_,
            [this](const std::string& action, const json& payload) {
                handle_command(action, payload);
            }
        );

        std::cout << "[" << worker_id_ << "] Ready. Template=" << current_template_
                  << " Kafka=" << kafka_servers_ << "\n";
    }

    ~AnalyticsWorker() {
        running_ = false;
        if (consumer_) consumer_->close();
    }

    void run() {
        std::cout << "[" << worker_id_ << "] Starting event processing loop ...\n";

        auto last_report   = Clock::now();
        constexpr auto kReportInterval = std::chrono::seconds(5);

        while (running_) {
            // Poll Kafka (1-second timeout so we can check running_ regularly)
            std::unique_ptr<RdKafka::Message> msg(
                consumer_->consume(/*timeout_ms=*/1000)
            );

            if (msg->err() == RdKafka::ERR_NO_ERROR) {
                const auto t0 = Clock::now();

                // Parse JSON event: {"key": "some_item"}
                const char* data = static_cast<const char*>(msg->payload());
                try {
                    const json evt  = json::parse(data, data + msg->len());
                    const std::string key = evt.value("key", "");

                    if (!key.empty()) {
                        std::unique_lock lock(sketch_mutex_);
                        cms_->update(key);
                        hll_->add(key);
                        mg_->add(key);
                    }
                } catch (...) {
                    // Malformed message — skip
                }

                const double latency_ms =
                    std::chrono::duration<double, std::milli>(Clock::now() - t0).count();

                {
                    std::lock_guard lg(latency_mutex_);
                    latency_samples_.push_back(latency_ms);
                    if (latency_samples_.size() > 1000)
                        latency_samples_.erase(latency_samples_.begin());
                }

                event_count_++;
            }
            // (Ignore timeout / EOF — they are normal)

            // Periodic metrics report
            const auto now = Clock::now();
            if (now - last_report >= kReportInterval) {
                report_metrics();
                last_report = now;
            }
        }
    }

private:
    // ------------------------------------------------------------------
    // Template application
    // ------------------------------------------------------------------

    void apply_template(const std::string& templ_name) {
        auto it = TEMPLATES.find(templ_name);
        const TemplateParams& p = (it != TEMPLATES.end())
                                  ? it->second
                                  : TEMPLATES.at("medium");

        std::unique_lock lock(sketch_mutex_);
        cms_ = std::make_unique<CountMinSketch>(p.cms_width, p.cms_depth);
        hll_ = std::make_unique<HyperLogLog>(p.hll_precision);
        mg_  = std::make_unique<SlidingWindowMisraGries>(p.mg_k);
        current_template_ = templ_name;

        std::cout << "[" << worker_id_ << "] Applied template '" << templ_name
                  << "': CMS(" << p.cms_width << "x" << p.cms_depth
                  << ") HLL(p=" << p.hll_precision
                  << ") MG(k=" << p.mg_k << ")\n";
    }

    // ------------------------------------------------------------------
    // Controller command handler (called from RedisReporter background thread)
    // ------------------------------------------------------------------

    void handle_command(const std::string& action, const json& payload) {
        if (action == "set_template") {
            const std::string templ = payload.value("template", "medium");
            apply_template(templ);
        } else if (action == "resize_mg") {
            const int new_k = payload.value("k", 50);
            std::unique_lock lock(sketch_mutex_);
            mg_->resize(new_k);
            std::cout << "[" << worker_id_ << "] Resized MisraGries k=" << new_k << "\n";
        }
    }

    // ------------------------------------------------------------------
    // Kafka connection
    // ------------------------------------------------------------------

    void connect_kafka(int retries = 15) {
        kafka_servers_ = getenv_str("KAFKA_BOOTSTRAP_SERVERS").value_or("localhost:9092");
        const std::string topic = getenv_str("KAFKA_TOPIC").value_or("stream_events");

        for (int attempt = 1; attempt <= retries; ++attempt) {
            std::string errstr;
            auto conf = std::unique_ptr<RdKafka::Conf>(
                RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)
            );
            conf->set("bootstrap.servers", kafka_servers_, errstr);
            conf->set("group.id", "btp-" + worker_id_, errstr);
            conf->set("auto.offset.reset", "latest", errstr);
            conf->set("enable.auto.commit", "true", errstr);

            consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
            if (consumer_) {
                const std::vector<std::string> topics = {topic};
                const RdKafka::ErrorCode ec = consumer_->subscribe(topics);
                if (ec == RdKafka::ERR_NO_ERROR) {
                    std::cout << "[" << worker_id_
                              << "] Connected to Kafka topic '" << topic << "'\n";
                    return;
                }
            }
            std::cerr << "[" << worker_id_ << "] Kafka not ready ("
                      << attempt << "/" << retries << "), retrying...\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
        throw std::runtime_error("Could not connect to Kafka after " +
                                 std::to_string(retries) + " attempts");
    }

    // ------------------------------------------------------------------
    // Metrics reporting
    // ------------------------------------------------------------------

    void report_metrics() {
        std::vector<double> samples;
        {
            std::lock_guard lg(latency_mutex_);
            samples = latency_samples_;
        }

        WorkerMetrics m;
        {
            std::shared_lock lock(sketch_mutex_);
            m.worker_id          = worker_id_;
            m.timestamp          = std::chrono::duration<double>(
                                     Clock::now().time_since_epoch()).count();
            m.event_count        = event_count_.load();
            m.templ              = current_template_;

            m.p50_latency_ms     = percentile(samples, 50);
            m.p95_latency_ms     = percentile(samples, 95);
            m.p99_latency_ms     = percentile(samples, 99);
            m.memory_mb          = rss_mb();
            m.memory_percent     = 0.0;  // psutil not available in C++; set 0

            m.cms_error_estimate = cms_->estimated_error();
            m.cms_total_count    = cms_->total_count();
            m.cms_width          = cms_->width();
            m.cms_depth          = cms_->depth();

            m.hll_cardinality    = hll_->count();
            m.hll_relative_error = hll_->relative_error();
            m.hll_precision      = hll_->precision();

            const auto hh = mg_->top_n(10);
            m.mg_heavy_hitters = json::array();
            for (const auto& [item, cnt] : hh)
                m.mg_heavy_hitters.push_back({item, cnt});
            m.mg_window_total    = mg_->window_total();
            m.mg_error_bound     = mg_->error_bound();
            m.mg_k               = mg_->k();
        }

        reporter_->push_metrics(m);

        std::cout << "[" << worker_id_ << "] metrics"
                  << " | P99="   << std::fixed << std::setprecision(2)
                  << m.p99_latency_ms   << "ms"
                  << " | mem="   << m.memory_mb << "MB"
                  << " | HLL≈"  << m.hll_cardinality
                  << " | tpl="  << m.templ << "\n";
    }

    // ------------------------------------------------------------------
    // Members
    // ------------------------------------------------------------------

    std::string  worker_id_;
    std::string  kafka_servers_;
    std::string  current_template_;
    std::atomic<bool>    running_;
    std::atomic<int64_t> event_count_;

    // Sketches — protected by shared_mutex (readers: report; writers: update/resize)
    mutable std::shared_mutex          sketch_mutex_;
    std::unique_ptr<CountMinSketch>    cms_;
    std::unique_ptr<HyperLogLog>       hll_;
    std::unique_ptr<SlidingWindowMisraGries> mg_;

    // Kafka consumer
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;

    // Latency samples — protected by a plain mutex
    std::mutex           latency_mutex_;
    std::vector<double>  latency_samples_;

    // Redis reporter (runs its own pub/sub thread internally)
    std::unique_ptr<RedisReporter> reporter_;
};

// ---------------------------------------------------------------------------
// Signal handler for graceful shutdown
// ---------------------------------------------------------------------------

static std::atomic<bool> g_running{true};

static void sig_handler(int) { g_running = false; }

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    std::signal(SIGINT,  sig_handler);
    std::signal(SIGTERM, sig_handler);

    // Simple CLI argument parsing (--worker-id  --template)
    std::string worker_id   = "worker-cpp";
    std::string templ_name  = "medium";

    for (int i = 1; i < argc - 1; ++i) {
        const std::string_view arg(argv[i]);
        if (arg == "--worker-id")  worker_id  = argv[i + 1];
        if (arg == "--template")   templ_name = argv[i + 1];
    }

    try {
        AnalyticsWorker worker(worker_id, templ_name);
        // Run until Ctrl-C or SIGTERM
        // We poll a small sleep so g_running gets checked
        std::thread run_thread([&worker] { worker.run(); });
        while (g_running)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

        run_thread.join();
    } catch (const std::exception& e) {
        std::cerr << "[FATAL] " << e.what() << '\n';
        return 1;
    }
    return 0;
}
