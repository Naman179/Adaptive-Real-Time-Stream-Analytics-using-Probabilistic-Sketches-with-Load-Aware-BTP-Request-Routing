/**
 * hyperloglog.hpp — Cardinality (distinct count) estimator.
 *
 * Theory:
 *   Uses m = 2^p registers. Each item is hashed; the position of the
 *   leftmost 1-bit determines the register to update. The harmonic mean
 *   over all registers gives the estimate. Relative std error ≈ 1.04/√m.
 *
 * Reference:
 *   Flajolet, Fusy, Gandouet, Meunier, "HyperLogLog: the analysis of a
 *   near-optimal cardinality estimation algorithm", AOFA 2007.
 *
 * Key upgrade over Python version:
 *   - Uses C++20 std::countl_zero() — compiles to a single BSR/LZCNT CPU
 *     instruction. The Python version had to manually loop bit-by-bit.
 *   - Uses XXH64 instead of SHA-256 for hashing (~10x faster).
 */

#pragma once

#include <algorithm>
#include <bit>        // C++20: std::countl_zero
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <vector>

#include <xxhash.h>

class HyperLogLog {
public:
    /**
     * Construct a HyperLogLog estimator.
     * @param precision  Number of bits used to index registers (p).
     *                   m = 2^p registers. Typical range: 4–18.
     *                   Relative error ≈ 1.04 / sqrt(2^p).
     */
    explicit HyperLogLog(int precision = 12)
        : precision_(precision),
          m_(1u << precision),
          registers_(1u << precision, 0)
    {
        if (precision < 4 || precision > 18)
            throw std::invalid_argument("precision must be between 4 and 18");
        alpha_ = compute_alpha(m_);
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /** Register item in the sketch. O(1) time. */
    void add(std::string_view item) noexcept {
        const uint64_t h = XXH64(item.data(), item.size(), /*seed=*/0);

        // Top-p bits → register index
        const uint32_t idx = static_cast<uint32_t>(h >> (64 - precision_));

        // Remaining (64-p) bits → count leading zeros + 1  (rho function).
        // std::countl_zero operates on the full 64-bit value;
        // shift the remaining bits to the MSB position first.
        const uint64_t remaining = (h << precision_) | ((1ULL << precision_) - 1);
        // +1 because rho counts position of first 1-bit (1-indexed)
        const uint8_t  rho = static_cast<uint8_t>(
            std::countl_zero(remaining) + 1
        );

        if (rho > registers_[idx])
            registers_[idx] = rho;
    }

    /**
     * Return the estimated cardinality.
     * Applies small-range (linear counting) and large-range corrections.
     */
    [[nodiscard]] int64_t count() const noexcept {
        // Raw harmonic-mean estimate
        double sum = 0.0;
        for (uint8_t r : registers_)
            sum += std::ldexp(1.0, -static_cast<int>(r));   // 2^(-r)

        double raw = alpha_ * static_cast<double>(m_) * static_cast<double>(m_) / sum;

        // Small-range correction: linear counting
        const int zeros = static_cast<int>(
            std::count(registers_.begin(), registers_.end(), uint8_t{0})
        );
        if (raw <= 2.5 * m_ && zeros > 0) {
            return static_cast<int64_t>(
                std::round(m_ * std::log(static_cast<double>(m_) / zeros))
            );
        }

        // Large-range correction
        constexpr double two32 = 1LL << 32;
        if (raw > two32 / 30.0) {
            return static_cast<int64_t>(
                std::round(-two32 * std::log(1.0 - raw / two32))
            );
        }

        return static_cast<int64_t>(std::round(raw));
    }

    // -----------------------------------------------------------------------
    // Accuracy metrics
    // -----------------------------------------------------------------------

    /** Theoretical relative standard error: 1.04 / sqrt(m). */
    [[nodiscard]] double relative_error() const noexcept {
        return 1.04 / std::sqrt(static_cast<double>(m_));
    }

    /** Memory usage: 1 byte per register. */
    [[nodiscard]] size_t memory_bytes() const noexcept { return m_; }

    [[nodiscard]] int      precision() const noexcept { return precision_; }
    [[nodiscard]] uint32_t num_registers() const noexcept { return m_; }

    // -----------------------------------------------------------------------
    // Merge (for distributed aggregation across workers)
    // -----------------------------------------------------------------------

    /**
     * Merge another HLL of the same precision into this one (in-place).
     * Used when aggregating partial estimates from multiple workers.
     */
    void merge(const HyperLogLog& other) {
        if (precision_ != other.precision_)
            throw std::invalid_argument(
                "Cannot merge HyperLogLog sketches of different precision"
            );
        for (uint32_t i = 0; i < m_; ++i)
            registers_[i] = std::max(registers_[i], other.registers_[i]);
    }

    // -----------------------------------------------------------------------
    // Dynamic reconfiguration
    // -----------------------------------------------------------------------

    /**
     * Return a new HLL with updated precision.
     * Downgrade: fold registers (take max within each bin).
     * Upgrade: expand with zero-filled new registers.
     */
    [[nodiscard]] HyperLogLog change_precision(int new_precision) const {
        HyperLogLog result(new_precision);
        const uint32_t new_m = result.m_;

        if (new_m <= m_) {
            // Downgrade: bin old registers → new (take max within each bin)
            const uint32_t factor = m_ / new_m;
            for (uint32_t i = 0; i < new_m; ++i) {
                uint8_t mx = 0;
                for (uint32_t j = i * factor; j < (i + 1) * factor; ++j)
                    mx = std::max(mx, registers_[j]);
                result.registers_[i] = mx;
            }
        } else {
            // Upgrade: copy old registers, rest remain 0
            std::copy(registers_.begin(), registers_.end(),
                      result.registers_.begin());
        }
        return result;
    }

private:
    // Bias-correction constant (standard HLL formula)
    [[nodiscard]] static double compute_alpha(uint32_t m) noexcept {
        if (m == 16) return 0.673;
        if (m == 32) return 0.697;
        if (m == 64) return 0.709;
        return 0.7213 / (1.0 + 1.079 / m);
    }

    int                  precision_;
    uint32_t             m_;          // 2^precision
    std::vector<uint8_t> registers_;  // 1 byte each — maximally cache-friendly
    double               alpha_;
};
