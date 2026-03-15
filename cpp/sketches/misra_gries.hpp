/**
 * misra_gries.hpp — Sliding-Window Misra–Gries heavy-hitter detector.
 *
 * Theory:
 *   Maintains at most (k-1) candidate items with approximate counts.
 *   Any item with true frequency > N/k is guaranteed to appear.
 *   For sliding windows, timestamped buckets are used for eviction.
 *   Error bound: N_window / k per query.
 *
 * References:
 *   Misra & Gries, "Finding repeated items", Sci. of Comp. Prog. 1982.
 *   Arasu & Manku, "Approximate counts and quantiles over sliding windows",
 *   PODS 2004.
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <deque>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

class SlidingWindowMisraGries {
public:
    /**
     * @param k           Candidate-set size; items with freq > N/k reported.
     * @param window_sec  Sliding window length in seconds.
     * @param bucket_sec  Bucket granularity in seconds (must divide window_sec).
     */
    explicit SlidingWindowMisraGries(
        int    k           = 200,
        double window_sec  = 60.0,
        double bucket_sec  = 5.0
    )
        : k_(k),
          window_ns_(to_ns(window_sec)),
          bucket_ns_(to_ns(bucket_sec)),
          window_total_(0)
    {
        if (k < 2)
            throw std::invalid_argument("k must be >= 2");
        if (bucket_sec > window_sec)
            throw std::invalid_argument("bucket_sec must be <= window_sec");

        current_start_ = Clock::now();
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /** Record an occurrence of item. O(1) amortised; O(k log k) per bucket flush. */
    void add(std::string_view item) {
        const auto now = Clock::now();

        // Rotate to a new bucket if the current one has expired
        if (now - current_start_ >= bucket_ns_) {
            flush_current_bucket(now);
            evict_old_buckets(now);
            rebuild_candidates();
        }

        current_bucket_[std::string(item)]++;
        // NOTE: window_total_ only tracks committed (flushed) bucket events.
        // Current bucket events are summed live in window_total().
    }

    /**
     * Return (item, estimated_count) pairs sorted by count descending.
     * Triggers a window eviction + candidate rebuild.
     */
    [[nodiscard]] std::vector<std::pair<std::string, int64_t>>
    heavy_hitters(int64_t min_frequency = 0) {
        evict_old_buckets(Clock::now());
        rebuild_candidates();

        std::vector<std::pair<std::string, int64_t>> result;
        result.reserve(candidates_.size());
        for (const auto& [item, cnt] : candidates_)
            if (cnt >= min_frequency)
                result.emplace_back(item, cnt);

        std::sort(result.begin(), result.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });
        return result;
    }

    /** Return the top-n estimated heavy hitters. */
    [[nodiscard]] std::vector<std::pair<std::string, int64_t>>
    top_n(int n = 10) {
        auto hh = heavy_hitters();
        if (static_cast<int>(hh.size()) > n)
            hh.resize(static_cast<size_t>(n));
        return hh;
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    /**
     * Total events inside the sliding window.
     * window_total_ counts committed buckets; current bucket events are added live.
     */
    [[nodiscard]] int64_t window_total() const noexcept {
        int64_t cur = 0;
        for (const auto& [item, cnt] : current_bucket_)
            cur += cnt;
        return window_total_ + cur;
    }

    /** Per-item count error bound = window_total / k. */
    [[nodiscard]] double error_bound() const noexcept {
        return static_cast<double>(window_total()) / static_cast<double>(k_);
    }

    /** Memory estimate: k candidates × ~128 bytes each. */
    [[nodiscard]] size_t memory_bytes() const noexcept {
        return static_cast<size_t>(k_) * 128;
    }

    [[nodiscard]] int k() const noexcept { return k_; }

    // -----------------------------------------------------------------------
    // Dynamic reconfiguration
    // -----------------------------------------------------------------------

    /** Update k (and optionally window length) in-place. Rebuilds candidates. */
    void resize(int new_k, double new_window_sec = -1.0) {
        k_ = new_k;
        if (new_window_sec > 0)
            window_ns_ = to_ns(new_window_sec);
        evict_old_buckets(Clock::now());
        rebuild_candidates();
    }

private:
    // -----------------------------------------------------------------------
    // Private type aliases (kept inside class to avoid polluting global namespace)
    // -----------------------------------------------------------------------
    using Clock     = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Bucket    = std::unordered_map<std::string, int64_t>;

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    static std::chrono::nanoseconds to_ns(double seconds) {
        return std::chrono::nanoseconds(
            static_cast<int64_t>(seconds * 1e9)
        );
    }

    void flush_current_bucket(TimePoint now) {
        if (!current_bucket_.empty()) {
            // Accumulate events from the outgoing bucket into window_total_
            int64_t bucket_events = 0;
            for (const auto& [item, cnt] : current_bucket_)
                bucket_events += cnt;
            window_total_ += bucket_events;

            buckets_.emplace_back(current_start_, std::move(current_bucket_));
            // current_bucket_ is in a valid-but-unspecified state after move;
            // re-assign to a fresh empty map.
            current_bucket_ = Bucket{};
        }
        current_start_ = now;
    }

    void evict_old_buckets(TimePoint now) {
        const auto cutoff = now - window_ns_;
        while (!buckets_.empty() && buckets_.front().first < cutoff) {
            for (const auto& [item, cnt] : buckets_.front().second)
                window_total_ -= cnt;
            buckets_.pop_front();
        }
    }

    void rebuild_candidates() {
        // Aggregate raw counts across all committed buckets + current bucket
        Bucket raw;
        for (const auto& [ts, bucket] : buckets_)
            for (const auto& [item, cnt] : bucket)
                raw[item] += cnt;
        for (const auto& [item, cnt] : current_bucket_)
            raw[item] += cnt;

        // Sort descending by count for greedy Misra-Gries selection
        std::vector<std::pair<std::string, int64_t>> sorted_raw(raw.begin(), raw.end());
        std::sort(sorted_raw.begin(), sorted_raw.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        // Misra-Gries compression: keep at most k-1 candidates
        candidates_.clear();
        for (const auto& [item, cnt] : sorted_raw) {
            if (static_cast<int>(candidates_.size()) < k_ - 1) {
                candidates_[item] = cnt;
            } else {
                if (candidates_.empty()) break;

                // Decrement all by the minimum candidate count; remove zeros
                int64_t min_val = INT64_MAX;
                for (const auto& [key, c] : candidates_)
                    min_val = std::min(min_val, c);

                Bucket next;
                for (const auto& [key, c] : candidates_)
                    if (c > min_val) next[key] = c - min_val;

                if (cnt > min_val)
                    next[item] = cnt - min_val;

                candidates_ = std::move(next);
            }
        }
    }

    int    k_;
    std::chrono::nanoseconds window_ns_;
    std::chrono::nanoseconds bucket_ns_;

    std::deque<std::pair<TimePoint, Bucket>> buckets_;
    Bucket    current_bucket_;
    TimePoint current_start_;

    Bucket    candidates_;
    int64_t   window_total_;  // tracks only committed (flushed) bucket events
};
