/**
 * count_min_sketch.hpp — Memory-efficient frequency estimator.
 *
 * Theory:
 *   Uses d hash functions and a w-wide counter array.
 *   Guarantees: f_hat(x) <= f(x) + epsilon * N  with prob (1 - delta)
 *   where w = ceil(e / epsilon),  d = ceil(ln(1/delta))
 *
 * Reference:
 *   Cormode & Muthukrishnan, "An improved data stream summary:
 *   the Count-Min Sketch and its applications", J. Algorithms 2005.
 *
 * Implementation notes:
 *   - Uses xxHash (XXH64) for blazing-fast non-cryptographic hashing.
 *   - Each row uses a different seed so hash families are independent.
 *   - Header-only for easy inclusion.
 */

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <vector>

// xxHash — included via FetchContent in CMakeLists.txt
#include <xxhash.h>

class CountMinSketch {
public:
    /**
     * Construct a Count-Min Sketch.
     * @param width  Number of counters per row (controls additive error epsilon).
     * @param depth  Number of hash rows (controls failure probability delta).
     */
    explicit CountMinSketch(uint32_t width = 2048, uint32_t depth = 5)
        : width_(width), depth_(depth),
          table_(static_cast<size_t>(depth) * width, 0),
          total_count_(0)
    {
        if (width < 1 || depth < 1)
            throw std::invalid_argument("width and depth must be >= 1");
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /** Insert key (or increment by count). O(depth) time. */
    void update(std::string_view key, int64_t count = 1) noexcept {
        for (uint32_t row = 0; row < depth_; ++row) {
            const uint64_t col = hash(key, row);
            table_[row * width_ + col] += count;
        }
        total_count_ += count;
    }

    /**
     * Return the estimated frequency of key.
     * Always >= true frequency; over-estimate bounded by epsilon * N.
     */
    [[nodiscard]] int64_t query(std::string_view key) const noexcept {
        int64_t result = INT64_MAX;
        for (uint32_t row = 0; row < depth_; ++row) {
            const uint64_t col = hash(key, row);
            result = std::min(result, table_[row * width_ + col]);
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Error / memory metrics
    // -----------------------------------------------------------------------

    /** Theoretical additive error factor: e / width. */
    [[nodiscard]] double epsilon() const noexcept {
        return M_E / static_cast<double>(width_);
    }

    /** Failure probability: e^(-depth). */
    [[nodiscard]] double delta() const noexcept {
        return std::exp(-static_cast<double>(depth_));
    }

    /**
     * Worst-case absolute additive error = epsilon * N  (with prob 1 - delta).
     */
    [[nodiscard]] double estimated_error() const noexcept {
        return epsilon() * static_cast<double>(total_count_);
    }

    /** Memory used by counter table (8 bytes per int64_t). */
    [[nodiscard]] size_t memory_bytes() const noexcept {
        return static_cast<size_t>(width_) * depth_ * sizeof(int64_t);
    }

    [[nodiscard]] uint32_t width() const noexcept { return width_; }
    [[nodiscard]] uint32_t depth() const noexcept { return depth_; }
    [[nodiscard]] int64_t  total_count() const noexcept { return total_count_; }

    // -----------------------------------------------------------------------
    // Dynamic reconfiguration (called by Adaptive Controller)
    // -----------------------------------------------------------------------

    /**
     * Return a NEW CountMinSketch with updated parameters.
     * The old sketch is discarded; the worker re-warms from the stream.
     */
    [[nodiscard]] static CountMinSketch resize(uint32_t new_width, uint32_t new_depth) {
        return CountMinSketch(new_width, new_depth);
    }

private:
    // -----------------------------------------------------------------------
    // Hash helper: XXH64 with seed = row index for independent hash families.
    // Replaces the slow SHA-256 used in the Python version.
    // -----------------------------------------------------------------------
    [[nodiscard]] uint64_t hash(std::string_view key, uint32_t seed) const noexcept {
        return XXH64(key.data(), key.size(), static_cast<uint64_t>(seed)) % width_;
    }

    uint32_t            width_;
    uint32_t            depth_;
    std::vector<int64_t> table_;   // flattened depth x width
    int64_t             total_count_;
};

// ---------------------------------------------------------------------------
// Factory: build from (epsilon, delta) targets
// ---------------------------------------------------------------------------

/**
 * Construct a CMS meeting the given error and confidence requirements.
 * @param eps   Desired additive-error fraction (e.g., 0.01 for 1%).
 * @param delta Failure probability (e.g., 0.01 for 99% confidence).
 */
[[nodiscard]] inline CountMinSketch cms_from_error(double eps, double delta) {
    if (eps <= 0 || delta <= 0)
        throw std::invalid_argument("eps and delta must be > 0");
    const auto width = static_cast<uint32_t>(std::ceil(M_E / eps));
    const auto depth = static_cast<uint32_t>(std::ceil(std::log(1.0 / delta)));
    return CountMinSketch(width, depth);
}
