/**
 * test_sketches.cpp — Catch2 unit tests for all three C++ sketch implementations.
 *
 * Mirrors the guarantees verified by tests/test_sketches.py (Python).
 * Run:
 *   cd cpp && cmake -B build && cmake --build build -- -j4
 *   ./build/tests/test_sketches
 */

#define CATCH_CONFIG_MAIN
#include <catch2/catch_all.hpp>

#include "../sketches/count_min_sketch.hpp"
#include "../sketches/hyperloglog.hpp"
#include "../sketches/misra_gries.hpp"

#include <cmath>
#include <string>

// ===========================================================================
// Count-Min Sketch
// ===========================================================================

TEST_CASE("CMS: basic update and query", "[cms]") {
    CountMinSketch cms(2048, 5);
    for (int i = 0; i < 100; ++i) cms.update("apple");
    for (int i = 0; i < 50;  ++i) cms.update("banana");

    REQUIRE(cms.query("apple")  >= 100);
    REQUIRE(cms.query("banana") >= 50);
}

TEST_CASE("CMS: never under-estimates", "[cms]") {
    CountMinSketch cms(512, 3);
    std::unordered_map<std::string, int64_t> true_counts;

    for (int i = 0; i < 500; ++i) {
        const std::string key = "k_" + std::to_string(i % 50);
        cms.update(key);
        true_counts[key]++;
    }

    for (const auto& [key, cnt] : true_counts) {
        REQUIRE(cms.query(key) >= cnt);
    }
}

TEST_CASE("CMS: over-estimate bounded by epsilon * N", "[cms]") {
    CountMinSketch cms(2048, 5);
    const int n_items = 1000;
    for (int i = 0; i < n_items; ++i)
        cms.update("item_" + std::to_string(i % 200));

    const double eps = cms.epsilon();
    const double N   = static_cast<double>(cms.total_count());

    // Loose upper bound: estimate <= true_count + eps * N
    // (individual true counts are n_items/200 = 5)
    for (int i = 0; i < 200; ++i) {
        const auto est = static_cast<double>(cms.query("item_" + std::to_string(i)));
        REQUIRE(est <= 5.0 + eps * N + 1.0);  // +1 for rounding
    }
}

TEST_CASE("CMS: factory from error/delta targets", "[cms]") {
    auto cms = cms_from_error(0.01, 0.01);
    REQUIRE(cms.epsilon() <= 0.01 + 1e-9);
    REQUIRE(cms.delta()   <= 0.01 + 1e-9);
}

TEST_CASE("CMS: resize returns new instance", "[cms]") {
    CountMinSketch cms(512, 3);
    cms.update("x");
    auto new_cms = CountMinSketch::resize(2048, 5);
    REQUIRE(new_cms.width() == 2048);
    REQUIRE(new_cms.depth() == 5);
    REQUIRE(new_cms.total_count() == 0);  // fresh sketch
}

// ===========================================================================
// HyperLogLog
// ===========================================================================

TEST_CASE("HLL: empty sketch returns 0", "[hll]") {
    HyperLogLog hll(12);
    REQUIRE(hll.count() == 0);
}

TEST_CASE("HLL: small cardinality within 10%", "[hll]") {
    HyperLogLog hll(14);
    const int n = 1000;
    for (int i = 0; i < n; ++i)
        hll.add("unique_item_" + std::to_string(i));

    const double estimate = static_cast<double>(hll.count());
    const double rel_err  = std::abs(estimate - n) / n;
    REQUIRE(rel_err < 0.10);
}

TEST_CASE("HLL: large cardinality within 3x theoretical std error", "[hll]") {
    HyperLogLog hll(12);
    const int n = 50000;
    for (int i = 0; i < n; ++i)
        hll.add("user_" + std::to_string(i));

    const double estimate = static_cast<double>(hll.count());
    const double rel_err  = std::abs(estimate - n) / n;
    REQUIRE(rel_err < 3.0 * hll.relative_error());
}

TEST_CASE("HLL: duplicates not counted", "[hll]") {
    HyperLogLog hll(12);
    for (int i = 0; i < 1000; ++i)
        hll.add("same_key");
    REQUIRE(hll.count() <= 5);
}

TEST_CASE("HLL: merge two disjoint sets", "[hll]") {
    HyperLogLog hll_a(14), hll_b(14);
    for (int i = 0; i < 10000; ++i) hll_a.add("setA_" + std::to_string(i));
    for (int i = 0; i < 10000; ++i) hll_b.add("setB_" + std::to_string(i));
    hll_a.merge(hll_b);

    const double estimate = static_cast<double>(hll_a.count());
    const double rel_err  = std::abs(estimate - 20000.0) / 20000.0;
    REQUIRE(rel_err < 0.05);
}

TEST_CASE("HLL: change_precision does not crash", "[hll]") {
    HyperLogLog hll(12);
    for (int i = 0; i < 500; ++i) hll.add("item_" + std::to_string(i));
    auto new_hll = hll.change_precision(10);
    REQUIRE(new_hll.precision() == 10);
    REQUIRE(new_hll.count() >= 0);
}

// ===========================================================================
// SlidingWindowMisraGries
// ===========================================================================

TEST_CASE("MG: single heavy hitter detected", "[mg]") {
    SlidingWindowMisraGries mg(10, 60.0, 5.0);
    for (int i = 0; i < 1000; ++i) mg.add("dominant");
    for (int i = 0; i < 20;   ++i) mg.add("rare_" + std::to_string(i));

    auto hh = mg.heavy_hitters();
    const bool found = std::any_of(hh.begin(), hh.end(),
        [](const auto& p) { return p.first == "dominant"; });
    REQUIRE(found);
}

TEST_CASE("MG: empty returns no hitters", "[mg]") {
    SlidingWindowMisraGries mg(5);
    REQUIRE(mg.heavy_hitters().empty());
}

TEST_CASE("MG: top_n bounded by n", "[mg]") {
    SlidingWindowMisraGries mg(100);
    for (int i = 0; i < 500; ++i)
        mg.add("item_" + std::to_string(i % 50));
    REQUIRE(mg.top_n(10).size() <= 10);
}

TEST_CASE("MG: error_bound formula = window_total / k", "[mg]") {
    SlidingWindowMisraGries mg(20);
    for (int i = 0; i < 200; ++i)
        mg.add("k_" + std::to_string(i));

    const double expected = static_cast<double>(mg.window_total()) / 20.0;
    REQUIRE(std::abs(mg.error_bound() - expected) < 1e-6);
}

TEST_CASE("MG: resize changes k", "[mg]") {
    SlidingWindowMisraGries mg(50);
    for (int i = 0; i < 100; ++i) mg.add("i_" + std::to_string(i));
    mg.resize(20);
    REQUIRE(mg.k() == 20);
}
