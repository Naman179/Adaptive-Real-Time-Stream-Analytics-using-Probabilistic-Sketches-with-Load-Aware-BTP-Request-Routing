"""
Unit tests for Python sketch implementations (no Kafka/Redis required).

Run:
    pytest tests/test_sketches.py -v
"""

import sys
import os
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from stream.worker import CountMinSketch, HyperLogLog, MisraGries


# ── Count-Min Sketch ──────────────────────────────────────────────────

class TestCountMinSketch:
    def test_empty_sketch_returns_zero(self):
        cms = CountMinSketch(width=1024, depth=5)
        assert cms.query("nonexistent") == 0
        assert cms.total == 0

    def test_single_update(self):
        cms = CountMinSketch(width=1024, depth=5)
        cms.update("hello")
        assert cms.query("hello") >= 1  # CMS can overcount, never undercount
        assert cms.total == 1

    def test_multiple_updates_same_key(self):
        cms = CountMinSketch(width=2048, depth=5)
        for _ in range(100):
            cms.update("apple")
        estimate = cms.query("apple")
        assert estimate >= 100  # Must be at least the true count
        assert cms.total == 100

    def test_error_bound(self):
        """CMS error should be <= (e/w) * N."""
        cms = CountMinSketch(width=2048, depth=5)
        for i in range(10000):
            cms.update(f"item_{i % 100}")
        error = cms.error_estimate()
        expected_bound = (math.e / 2048) * 10000
        assert abs(error - expected_bound) < 0.01, f"Error {error} != expected {expected_bound}"

    def test_memory_bytes(self):
        cms = CountMinSketch(width=512, depth=3)
        assert cms.memory_bytes() == 512 * 3 * 8

    def test_never_undercounts(self):
        """CMS guarantees: f̂(x) >= f(x)."""
        cms = CountMinSketch(width=1024, depth=5)
        true_counts = {}
        import random
        random.seed(42)
        for _ in range(5000):
            key = f"item_{random.randint(1, 50)}"
            true_counts[key] = true_counts.get(key, 0) + 1
            cms.update(key)
        for key, true_count in true_counts.items():
            assert cms.query(key) >= true_count, f"CMS undercount for {key}: {cms.query(key)} < {true_count}"


# ── HyperLogLog ───────────────────────────────────────────────────────

class TestHyperLogLog:
    def test_empty_hll(self):
        hll = HyperLogLog(precision=12)
        # Empty HLL should return 0 or very small count
        assert hll.count() <= 1

    def test_single_element(self):
        hll = HyperLogLog(precision=12)
        hll.add("hello")
        assert hll.count() >= 1

    def test_cardinality_estimation(self):
        """HLL estimate should be within ±20% for 10K unique items at p=12."""
        hll = HyperLogLog(precision=12)
        n = 10000
        for i in range(n):
            hll.add(f"unique_{i}")
        estimate = hll.count()
        error = abs(estimate - n) / n
        assert error < 0.20, f"HLL estimate {estimate} too far from {n} (error={error:.2%})"

    def test_duplicate_elements(self):
        """Adding the same element multiple times should not increase cardinality significantly."""
        hll = HyperLogLog(precision=12)
        for _ in range(10000):
            hll.add("same_element")
        assert hll.count() <= 5  # Should be ~1

    def test_relative_error_formula(self):
        """Relative error should be 1.04 / sqrt(m)."""
        hll = HyperLogLog(precision=12)
        m = 1 << 12
        expected = 1.04 / math.sqrt(m)
        assert abs(hll.relative_error() - expected) < 1e-6

    def test_memory_bytes(self):
        hll = HyperLogLog(precision=10)
        assert hll.memory_bytes() == 1024  # 2^10

    def test_precision_affects_accuracy(self):
        """Higher precision should give better accuracy."""
        n = 5000
        errors = {}
        for p in [10, 12, 14]:
            hll = HyperLogLog(precision=p)
            for i in range(n):
                hll.add(f"item_{i}")
            errors[p] = abs(hll.count() - n) / n
        # Higher precision should generally give lower error
        assert errors[14] <= errors[10] + 0.05  # Allow some variance


# ── Misra-Gries ───────────────────────────────────────────────────────

class TestMisraGries:
    def test_empty_mg(self):
        mg = MisraGries(k=10)
        assert mg.heavy_hitters() == []
        assert mg.window_total == 0

    def test_single_item(self):
        mg = MisraGries(k=10)
        mg.add("hello")
        hh = mg.heavy_hitters()
        assert len(hh) >= 1
        assert hh[0][0] == "hello"

    def test_heavy_hitter_detection(self):
        """Items appearing more than N/k times must appear in the output."""
        mg = MisraGries(k=10, window_size=100000)
        # Insert a dominant item
        for _ in range(5000):
            mg.add("dominant")
        # Insert noise
        for i in range(500):
            mg.add(f"noise_{i}")
        hh = dict(mg.heavy_hitters())
        assert "dominant" in hh, "Dominant item should be detected as heavy hitter"

    def test_window_eviction(self):
        """Window should respect the window_size limit."""
        window_size = 100
        mg = MisraGries(k=10, window_size=window_size)
        for i in range(200):
            mg.add(f"item_{i}")
        assert len(mg.window) <= window_size

    def test_error_bound(self):
        mg = MisraGries(k=200, window_size=10000)
        for i in range(1000):
            mg.add(f"item_{i % 50}")
        error = mg.error_bound()
        assert error == 1000 / 200  # N / k

    def test_sorted_output(self):
        """Heavy hitters should be sorted by count descending."""
        mg = MisraGries(k=50, window_size=100000)
        for _ in range(1000):
            mg.add("A")
        for _ in range(500):
            mg.add("B")
        for _ in range(100):
            mg.add("C")
        hh = mg.heavy_hitters()
        counts = [c for _, c in hh]
        assert counts == sorted(counts, reverse=True)

    def test_memory_bytes(self):
        mg = MisraGries(k=200)
        assert mg.memory_bytes() == 200 * 100
