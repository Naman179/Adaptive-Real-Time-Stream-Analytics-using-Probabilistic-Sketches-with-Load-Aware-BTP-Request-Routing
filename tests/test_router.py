"""
Unit tests for the Load-Aware Router (no Redis required — uses mock metrics).
Run with:  pytest tests/test_router.py -v
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from router.router import LoadAwareRouter, CAPABILITY_SCORES, W1, W2, W3


def make_metrics(template="medium", p99=10.0, memory_pct=30.0):
    """Build a minimal fake worker-metrics dict."""
    return {
        "template":        template,
        "p99_latency_ms":  p99,
        "memory_percent":  memory_pct,
    }


class TestRouterScoring:

    def setup_method(self):
        self.router = LoadAwareRouter()

    def _score(self, metrics, alpha_q=0.95, lsla=50.0):
        return self.router._score(metrics, alpha_q, lsla)

    def test_high_template_scores_higher_than_low(self):
        """A high-precision node should score higher than a low-precision one
        when other metrics are equal."""
        high = make_metrics("high",   p99=10, memory_pct=20)
        low  = make_metrics("low",    p99=10, memory_pct=20)
        assert self._score(high) > self._score(low)

    def test_lower_latency_scores_higher(self):
        """A faster node should rank higher when templates are equal."""
        fast = make_metrics("medium", p99=5,  memory_pct=30)
        slow = make_metrics("medium", p99=100, memory_pct=30)
        assert self._score(fast) > self._score(slow)

    def test_higher_memory_scores_lower(self):
        """A memory-saturated node should rank lower."""
        free = make_metrics("medium", p99=10, memory_pct=10)
        full = make_metrics("medium", p99=10, memory_pct=90)
        assert self._score(free) > self._score(full)

    def test_rank_nodes_returns_sorted_list(self):
        """_rank_nodes should return nodes sorted by score descending."""
        cluster = {
            "w0": make_metrics("high",   p99=5,   memory_pct=10),
            "w1": make_metrics("low",    p99=200, memory_pct=80),
            "w2": make_metrics("medium", p99=50,  memory_pct=50),
        }
        ranked = self.router._rank_nodes(cluster, alpha_q=0.95, latency_sla_ms=50)
        scores = [s for _, s in ranked]
        assert scores == sorted(scores, reverse=True), "Not sorted descending"

    def test_best_node_is_first(self):
        """The best overall node (high + fast + free) should be ranked first."""
        cluster = {
            "best_node":  make_metrics("high",   p99=3,   memory_pct=5),
            "worst_node": make_metrics("low",    p99=300, memory_pct=95),
        }
        ranked = self.router._rank_nodes(cluster, alpha_q=0.95, latency_sla_ms=50)
        assert ranked[0][0] == "best_node"

    def test_single_node_always_selected(self):
        """With one node, it must always be selected regardless of quality."""
        cluster = {"only_worker": make_metrics("low", p99=999, memory_pct=99)}
        ranked = self.router._rank_nodes(cluster, alpha_q=0.95, latency_sla_ms=50)
        assert len(ranked) == 1
        assert ranked[0][0] == "only_worker"

    def test_score_formula_correctness(self):
        """Manually verify the scoring formula matches equation (3) from the report."""
        m = make_metrics("medium", p99=10.0, memory_pct=40.0)
        cap   = CAPABILITY_SCORES["medium"]
        alpha = 0.95
        lsla  = 50.0
        mem_f = 40.0 / 100.0
        expected = W1*(cap/alpha) - W2*(10.0/lsla) - W3*mem_f
        assert abs(self._score(m, alpha, lsla) - expected) < 1e-9

    def test_empty_cluster_rank(self):
        """Empty cluster should return empty ranking."""
        ranked = self.router._rank_nodes({}, alpha_q=0.9, latency_sla_ms=100)
        assert ranked == []
