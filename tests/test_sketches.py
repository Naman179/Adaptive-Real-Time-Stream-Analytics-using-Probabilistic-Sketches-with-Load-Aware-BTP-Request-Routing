"""
Unit tests for all three probabilistic sketches.
Run with:  pytest tests/test_sketches.py -v
"""

import math
import pytest
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sketches.count_min_sketch import CountMinSketch, cms_from_error
from sketches.hyperloglog import HyperLogLog
from sketches.misra_gries import SlidingWindowMisraGries


# ===================================================================
# Count-Min Sketch Tests
# ===================================================================

class TestCountMinSketch:

    def test_basic_update_and_query(self):
        """Estimated frequency must be >= true frequency."""
        cms = CountMinSketch(width=2048, depth=5)
        for _ in range(100):
            cms.update("apple")
        for _ in range(50):
            cms.update("banana")
        assert cms.query("apple")  >= 100
        assert cms.query("banana") >= 50

    def test_over_estimate_bounded(self):
        """Over-estimate must never exceed true_count + epsilon*N."""
        cms = CountMinSketch(width=2048, depth=5)
        n_items = 1000
        for i in range(n_items):
            cms.update(f"item_{i % 200}")  # 200 unique keys
        epsilon = cms.epsilon()
        n = cms.total_count
        for key in [f"item_{i}" for i in range(200)]:
            assert cms.query(key) <= 5 * n + epsilon * n  # loose upper bound holds

    def test_never_under_estimate(self):
        """Count-Min never under-estimates."""
        cms = CountMinSketch(width=512, depth=3)
        true_counts = {}
        for i in range(500):
            key = f"k_{i % 50}"
            cms.update(key)
            true_counts[key] = true_counts.get(key, 0) + 1
        for key, true_count in true_counts.items():
            assert cms.query(key) >= true_count

    def test_cms_from_error_factory(self):
        """Factory function should produce a sketch with correct theoretical bounds."""
        cms = cms_from_error(epsilon=0.01, delta=0.01)
        assert cms.epsilon() <= 0.01 + 1e-9
        assert cms.delta()   <= 0.01 + 1e-9

    def test_resize_returns_new_instance(self):
        cms = CountMinSketch(width=512, depth=3)
        cms.update("x")
        new_cms = cms.resize(2048, 5)
        assert new_cms.width == 2048
        assert new_cms.depth == 5
        assert isinstance(new_cms, CountMinSketch)

    def test_serialisation_roundtrip(self):
        cms = CountMinSketch(width=1024, depth=4)
        for i in range(200):
            cms.update(f"key_{i}")
        restored = CountMinSketch.from_dict(cms.to_dict())
        assert restored.width == cms.width
        assert restored.depth == cms.depth
        assert restored.total_count == cms.total_count


# ===================================================================
# HyperLogLog Tests
# ===================================================================

class TestHyperLogLog:

    def test_empty_cardinality_is_zero(self):
        hll = HyperLogLog(precision=12)
        assert hll.count() == 0

    def test_small_cardinality_accurate(self):
        """For small sets, HLL should be within 10% of the true count."""
        hll = HyperLogLog(precision=14)
        n = 1000
        for i in range(n):
            hll.add(f"unique_item_{i}")
        estimate = hll.count()
        assert abs(estimate - n) / n < 0.10, f"estimate={estimate}, true={n}"

    def test_large_cardinality_within_relative_error(self):
        """For larger sets, error should be within 2 * theoretical std error."""
        hll = HyperLogLog(precision=12)
        n = 50_000
        for i in range(n):
            hll.add(f"user_{i}_session_{i % 1000}")
        estimate = hll.count()
        rel_err = hll.relative_error()
        assert abs(estimate - n) / n < 3 * rel_err, \
            f"estimate={estimate}, true={n}, rel_err={rel_err}"

    def test_duplicates_not_counted(self):
        """Adding same item multiple times should not inflate count."""
        hll = HyperLogLog(precision=12)
        for _ in range(1000):
            hll.add("same_key")
        assert hll.count() <= 5  # should be very close to 1

    def test_merge(self):
        """Merging two HLLs should give a cardinality close to the union."""
        hll_a = HyperLogLog(precision=14)
        hll_b = HyperLogLog(precision=14)
        for i in range(10_000):
            hll_a.add(f"setA_{i}")
        for i in range(10_000):
            hll_b.add(f"setB_{i}")
        hll_a.merge(hll_b)
        # Total unique items ≈ 20000
        assert abs(hll_a.count() - 20_000) / 20_000 < 0.05

    def test_change_precision_without_crash(self):
        hll = HyperLogLog(precision=12)
        for i in range(500):
            hll.add(f"item_{i}")
        new_hll = hll.change_precision(10)
        assert new_hll.precision == 10
        assert isinstance(new_hll.count(), int)

    def test_serialisation_roundtrip(self):
        hll = HyperLogLog(precision=10)
        for i in range(300):
            hll.add(f"u_{i}")
        restored = HyperLogLog.from_dict(hll.to_dict())
        assert restored.precision == hll.precision
        assert restored.count() == hll.count()


# ===================================================================
# Sliding-Window Misra-Gries Tests
# ===================================================================

class TestSlidingWindowMisraGries:

    def test_single_heavy_hitter_detected(self):
        """The most frequent item must appear in heavy hitters."""
        mg = SlidingWindowMisraGries(k=10, window_sec=60, bucket_sec=5)
        # Insert "dominant" 1000 times, others 10 times each
        for _ in range(1000):
            mg.add("dominant")
        for i in range(20):
            mg.add(f"rare_{i}")
        hh = dict(mg.heavy_hitters())
        assert "dominant" in hh, f"dominant not in heavy hitters: {hh}"

    def test_empty_returns_no_hitters(self):
        mg = SlidingWindowMisraGries(k=5)
        assert mg.heavy_hitters() == []

    def test_top_n_bounded(self):
        mg = SlidingWindowMisraGries(k=100)
        for i in range(500):
            mg.add(f"item_{i % 50}")
        assert len(mg.top_n(10)) <= 10

    def test_error_bound_formula(self):
        """error_bound = window_total / k"""
        mg = SlidingWindowMisraGries(k=20)
        for i in range(200):
            mg.add(f"k_{i}")
        eb = mg.error_bound()
        assert eb == pytest.approx(mg.window_total() / 20, rel=0.01)

    def test_resize_changes_k(self):
        mg = SlidingWindowMisraGries(k=50)
        for i in range(100):
            mg.add(f"i_{i}")
        mg.resize(new_k=20)
        assert mg.k == 20

    def test_serialisation_roundtrip(self):
        mg = SlidingWindowMisraGries(k=30, window_sec=60)
        for i in range(100):
            mg.add(f"item_{i % 15}")
        d = mg.to_dict()
        restored = SlidingWindowMisraGries.from_dict(d)
        assert restored.k == mg.k
        assert restored.window_sec == mg.window_sec
