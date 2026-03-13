"""
Comparative Experiment Runner — evaluates the 4 configurations from Section 7.2.

  Config 1: Static params + Round-robin router    (Baseline)
  Config 2: Adaptive params + Static router
  Config 3: Static params + Smart router
  Config 4: Adaptive params + Smart router         (Fully adaptive)

Usage:
  python scripts/run_simulation.py --duration 60 --rate 3000

Output:
  Prints a summary table of avg P99 latency, memory, and HLL error per config.
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sketches.count_min_sketch import CountMinSketch
from sketches.hyperloglog import HyperLogLog
from sketches.misra_gries import SlidingWindowMisraGries
from stream.producer import ZipfSampler


TEMPLATES = {
    "low":    {"cms_width": 512,  "cms_depth": 3,  "hll_p": 10, "mg_k": 50},
    "medium": {"cms_width": 2048, "cms_depth": 5,  "hll_p": 12, "mg_k": 200},
    "high":   {"cms_width": 8192, "cms_depth": 7,  "hll_p": 14, "mg_k": 500},
}


class SimulatedNode:
    """A single in-process simulated worker node."""

    def __init__(self, node_id: str, template: str = "medium"):
        self.node_id = node_id
        self.template = template
        p = TEMPLATES[template]
        self.cms = CountMinSketch(p["cms_width"], p["cms_depth"])
        self.hll = HyperLogLog(p["hll_p"])
        self.mg  = SlidingWindowMisraGries(p["mg_k"])
        self.latencies = []

    def process(self, key: str) -> float:
        t0 = time.perf_counter()
        self.cms.update(key)
        self.hll.add(key)
        self.mg.add(key)
        lat = (time.perf_counter() - t0) * 1000
        self.latencies.append(lat)
        return lat

    def p99(self):
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        return s[int(len(s) * 0.99)]

    def memory_mb(self):
        cms_bytes = self.cms.memory_bytes()
        hll_bytes = self.hll.memory_bytes()
        mg_bytes  = self.mg.memory_bytes()
        return (cms_bytes + hll_bytes + mg_bytes) / 1024 / 1024

    def hll_rel_error(self):
        return self.hll.relative_error()


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
                if node.p99() > 0.5:  # 0.5ms threshold in simulation
                    p = TEMPLATES["low"]
                    node.cms = CountMinSketch(p["cms_width"], p["cms_depth"])
                    node.template = "low"

    avg_p99 = sum(n.p99() for n in nodes) / n_nodes
    avg_mem  = sum(n.memory_mb() for n in nodes) / n_nodes
    avg_err  = sum(n.hll_rel_error() for n in nodes) / n_nodes
    return {"config": name, "avg_p99_ms": avg_p99, "avg_mem_mb": avg_mem, "avg_hll_err": avg_err}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events",   type=int, default=100_000, help="Total events to simulate")
    parser.add_argument("--nodes",    type=int, default=3,       help="Number of worker nodes")
    parser.add_argument("--zipf",     type=float, default=1.2,   help="Zipf skewness")
    parser.add_argument("--items",    type=int, default=10_000,  help="Vocabulary size")
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
        print(f"{r['config']:<45} {r['avg_p99_ms']:>10.4f} {r['avg_mem_mb']:>10.4f} {r['avg_hll_err']:>10.4f}")

    print("\n✓ Simulation complete.")
    out_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "experiment_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
