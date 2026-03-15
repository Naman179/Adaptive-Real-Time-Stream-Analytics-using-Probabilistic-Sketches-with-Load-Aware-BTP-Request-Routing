"""
Comparative Experiment Runner — evaluates the 4 configurations from Section 7.2.

  Config 1: Static params + Round-robin router    (Baseline)
  Config 2: Adaptive params + Static router
  Config 3: Static params + Smart router
  Config 4: Adaptive params + Smart router         (Fully adaptive)

NOTE: This is an in-process simulation using lightweight native Python sketch
implementations (no Kafka/Redis required). The production sketches are the C++
implementations in cpp/sketches/.

Usage:
  python scripts/run_simulation.py --events 100000 --nodes 3 --zipf 1.2
"""

import argparse
import json
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# Lightweight in-process sketch stubs for simulation only
# (The real sketches are the C++ header-only implementations in cpp/sketches/)
# ---------------------------------------------------------------------------

class _CMS:
    """Minimal Count-Min Sketch for simulation benchmarking."""
    def __init__(self, width, depth):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
        self.total = 0
    def update(self, key):
        for row in range(self.depth):
            self.table[row][hash((key, row)) % self.width] += 1
        self.total += 1
    def estimated_error(self):
        return (math.e / self.width) * self.total
    def memory_bytes(self):
        return self.width * self.depth * 8


class _HLL:
    """Minimal HyperLogLog for simulation benchmarking."""
    def __init__(self, precision):
        self.precision = precision
        self.m = 1 << precision
        self.registers = [0] * self.m
    def add(self, item):
        h = hash(item) & 0xFFFFFFFFFFFFFFFF
        idx = h >> (64 - self.precision)
        w = h & ((1 << (64 - self.precision)) - 1)
        rho = 1
        while rho <= 64 - self.precision and not (w & (1 << (64 - self.precision - rho))):
            rho += 1
        self.registers[idx] = max(self.registers[idx], rho)
    def relative_error(self):
        return 1.04 / math.sqrt(self.m)
    def memory_bytes(self):
        return self.m


class _MG:
    """Minimal Misra-Gries for simulation benchmarking."""
    def __init__(self, k):
        self.k = k
        self.counts = {}
    def add(self, item):
        self.counts[item] = self.counts.get(item, 0) + 1
        if len(self.counts) >= self.k:
            min_val = min(self.counts.values())
            self.counts = {k: v - min_val for k, v in self.counts.items() if v > min_val}
    def memory_bytes(self):
        return self.k * 100


# ---------------------------------------------------------------------------
# Zipfian sampler (from stream/producer.py — duplicated here for standalone use)
# ---------------------------------------------------------------------------

class ZipfSampler:
    def __init__(self, n: int = 10_000, s: float = 1.2):
        weights = [1.0 / (k ** s) for k in range(1, n + 1)]
        total = sum(weights)
        self._cdf, cum = [], 0.0
        for w in weights:
            cum += w / total
            self._cdf.append(cum)
        self.n = n

    def sample(self) -> str:
        r = random.random()
        lo, hi = 0, self.n - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if self._cdf[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        return f"item_{lo + 1}"


# ---------------------------------------------------------------------------
# Template definitions (mirror cpp/worker/worker.cpp TEMPLATES)
# ---------------------------------------------------------------------------

TEMPLATES = {
    "low":    {"cms_width": 512,  "cms_depth": 3,  "hll_p": 10, "mg_k": 50},
    "medium": {"cms_width": 2048, "cms_depth": 5,  "hll_p": 12, "mg_k": 200},
    "high":   {"cms_width": 8192, "cms_depth": 7,  "hll_p": 14, "mg_k": 500},
}


# ---------------------------------------------------------------------------
# Simulated node
# ---------------------------------------------------------------------------

class SimulatedNode:
    """A single in-process simulated worker node."""

    _WINDOW = 1000  # keep only last N latency samples (bounds sort to O(1000))

    def __init__(self, node_id: str, template: str = "medium"):
        self.node_id = node_id
        self.template = template
        self.latencies: list = []   # sliding window, max _WINDOW entries
        self._all_latencies: list = []  # full list only used for final p99
        self._cached_p99: float = 0.0
        self._apply(template)

    def _apply(self, template: str):
        p = TEMPLATES[template]
        self.cms = _CMS(p["cms_width"], p["cms_depth"])
        self.hll = _HLL(p["hll_p"])
        self.mg  = _MG(p["mg_k"])
        self.template = template

    def process(self, key: str) -> float:
        t0 = time.perf_counter()
        self.cms.update(key)
        self.hll.add(key)
        self.mg.add(key)
        lat = (time.perf_counter() - t0) * 1000
        # Sliding window — evict oldest if full
        if len(self.latencies) >= self._WINDOW:
            self.latencies.pop(0)
        self.latencies.append(lat)
        self._all_latencies.append(lat)
        return lat

    def p99(self) -> float:
        """P99 over the current sliding window (fast — always O(_WINDOW))."""
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        return s[int(len(s) * 0.99)]

    def final_p99(self) -> float:
        """P99 over all samples (only called once at end of experiment)."""
        if not self._all_latencies:
            return 0.0
        s = sorted(self._all_latencies)
        return s[int(len(s) * 0.99)]

    def memory_mb(self) -> float:
        return (self.cms.memory_bytes() + self.hll.memory_bytes() +
                self.mg.memory_bytes()) / 1024 / 1024

    def hll_rel_error(self) -> float:
        return self.hll.relative_error()


# ---------------------------------------------------------------------------
# Experiment runner
# ---------------------------------------------------------------------------

def run_config(name: str, n_nodes: int, adaptive: bool, smart_router: bool,
               n_events: int, sampler: ZipfSampler) -> dict:
    """Run a single experimental configuration and return result metrics."""
    nodes = [SimulatedNode(f"node-{i}", "medium") for i in range(n_nodes)]
    rr_idx = 0

    for event_idx in range(n_events):
        key = sampler.sample()

        if smart_router:
            # Route to node with lowest P99 latency (simplified smart routing)
            best = min(nodes, key=lambda n: n.p99() if n.latencies else 0)
            best.process(key)
        else:
            # Round-robin
            nodes[rr_idx % n_nodes].process(key)
            rr_idx += 1

        # Adaptive controller: every 5000 events, downgrade if high latency
        if adaptive and event_idx % 5000 == 0 and event_idx > 0:
            for node in nodes:
                if node.p99() > 0.5:   # 0.5ms threshold in simulation
                    node._apply("low")

    avg_p99 = sum(n.final_p99()    for n in nodes) / n_nodes
    avg_mem  = sum(n.memory_mb()   for n in nodes) / n_nodes
    avg_err  = sum(n.hll_rel_error() for n in nodes) / n_nodes
    return {"config": name, "avg_p99_ms": avg_p99, "avg_mem_mb": avg_mem, "avg_hll_err": avg_err}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="BTP Comparative Simulation (Section 7.2)"
    )
    parser.add_argument("--events", type=int,   default=100_000, help="Total events to simulate")
    parser.add_argument("--nodes",  type=int,   default=3,       help="Number of worker nodes")
    parser.add_argument("--zipf",   type=float, default=1.2,     help="Zipf skewness parameter")
    parser.add_argument("--items",  type=int,   default=10_000,  help="Vocabulary size")
    args = parser.parse_args()

    sampler = ZipfSampler(n=args.items, s=args.zipf)

    configs = [
        ("1. Baseline (static + round-robin)",   False, False),
        ("2. Adaptive params + round-robin",      True,  False),
        ("3. Static params + smart router",       False, True),
        ("4. Fully adaptive (adaptive + smart)",  True,  True),
    ]

    print(f"\nRunning {args.events:,} events across {args.nodes} nodes "
          f"(Zipf s={args.zipf}, vocab={args.items:,})\n")
    print(f"{'Config':<45} {'P99 (ms)':>10} {'Mem (MB)':>10} {'HLL err':>10}")
    print("-" * 80)

    results = []
    for name, adaptive, smart in configs:
        r = run_config(name, args.nodes, adaptive, smart, args.events, sampler)
        results.append(r)
        print(f"{r['config']:<45} {r['avg_p99_ms']:>10.4f} "
              f"{r['avg_mem_mb']:>10.4f} {r['avg_hll_err']:>10.4f}")

    print("\n✓ Simulation complete.")
    out_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "experiment_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
