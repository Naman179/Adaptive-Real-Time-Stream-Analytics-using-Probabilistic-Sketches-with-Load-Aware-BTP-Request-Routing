"""
Load-Aware Request Router — implements the node-scoring function from Section 5.5.

For each incoming query the router:
  1. Reads all live worker health vectors from Redis.
  2. Computes score_i(q) per Equation (3) of the report.
  3. Returns the worker_id of the highest-scoring node.

Router health vector per node:
  h_i = (P95_latency, memory_usage_%, sketch_capability, cpu_util)

Score function (Eq. 3):
  score_i(q) = w1 * (A_capability_i / alpha_q)
             - w2 * (P95_i / L_q)
             - w3 * M_usage_i

where:
  A_capability_i  = normalised sketch precision score for worker i
  alpha_q         = accuracy requirement of the query
  L_q             = latency SLA of the query (ms)
  P95_i           = 95th percentile latency of worker i
  M_usage_i       = memory usage fraction of worker i
"""

import os
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from state.store import StateStore

load_dotenv()

W1 = float(os.getenv("ROUTER_W1", "0.5"))  # accuracy weight
W2 = float(os.getenv("ROUTER_W2", "0.3"))  # latency penalty weight
W3 = float(os.getenv("ROUTER_W3", "0.2"))  # memory penalty weight

# Capability score for each precision template (0..1, higher = more accurate)
CAPABILITY_SCORES = {
    "low":    0.30,
    "medium": 0.65,
    "high":   1.00,
}


class LoadAwareRouter:
    """
    Custom load-aware router implementing Equation (3) from the BTP report.

    Not a reverse-proxy — sits inside the FastAPI request path and returns
    the best worker_id; the API then reads sketch state from that worker.
    """

    def __init__(self):
        self.store = StateStore()

    # ------------------------------------------------------------------
    # Query types → accuracy and latency SLA defaults
    # ------------------------------------------------------------------

    _QUERY_PROFILES = {
        "frequency":    {"accuracy": 0.95, "latency_sla_ms": 50.0},
        "cardinality":  {"accuracy": 0.90, "latency_sla_ms": 100.0},
        "heavy_hitters":{"accuracy": 0.85, "latency_sla_ms": 200.0},
    }

    # ------------------------------------------------------------------
    # Core scoring
    # ------------------------------------------------------------------

    def _score(
        self,
        worker_metrics: dict,
        alpha_q: float,
        latency_sla_ms: float,
    ) -> float:
        """
        Compute the routing score for a single worker node.

        Higher score = better candidate for this query.

        Parameters
        ----------
        worker_metrics  : latest metrics dict from Redis
        alpha_q         : query accuracy requirement (0..1)
        latency_sla_ms  : query latency SLA in milliseconds
        """
        template    = worker_metrics.get("template", "medium")
        capability  = CAPABILITY_SCORES.get(template, 0.65)
        p95_ms      = max(worker_metrics.get("p99_latency_ms", 1.0), 0.001)
        memory_frac = worker_metrics.get("memory_percent", 0.0) / 100.0
        alpha_q     = max(alpha_q, 1e-6)
        latency_sla = max(latency_sla_ms, 1.0)

        accuracy_score   = W1 * (capability / alpha_q)
        latency_penalty  = W2 * (p95_ms / latency_sla)
        memory_penalty   = W3 * memory_frac

        return accuracy_score - latency_penalty - memory_penalty

    def _rank_nodes(
        self,
        cluster: Dict[str, dict],
        alpha_q: float,
        latency_sla_ms: float,
    ) -> List[Tuple[str, float]]:
        """Return workers sorted by score descending."""
        scored = [
            (wid, self._score(metrics, alpha_q, latency_sla_ms))
            for wid, metrics in cluster.items()
        ]
        return sorted(scored, key=lambda x: -x[1])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def route(
        self,
        query_type: str = "frequency",
        alpha_q: Optional[float] = None,
        latency_sla_ms: Optional[float] = None,
    ) -> Tuple[Optional[str], float, List[Tuple[str, float]]]:
        """
        Select the best worker for the given query.

        Returns
        -------
        (best_worker_id, best_score, all_ranked_nodes)
        Returns (None, 0.0, []) if no workers are alive.
        """
        cluster = self.store.get_cluster_metrics()
        if not cluster:
            return None, 0.0, []

        profile = self._QUERY_PROFILES.get(query_type, self._QUERY_PROFILES["frequency"])
        aq  = alpha_q       if alpha_q is not None       else profile["accuracy"]
        lsla= latency_sla_ms if latency_sla_ms is not None else profile["latency_sla_ms"]

        ranked = self._rank_nodes(cluster, aq, lsla)
        best_wid, best_score = ranked[0]

        # Log routing decision
        self.store.log_query_routing(query_type, best_wid, best_score)

        return best_wid, best_score, ranked

    def get_node_scores(self, query_type: str = "frequency") -> List[dict]:
        """
        Return scored list of all nodes for dashboard display.
        """
        cluster = self.store.get_cluster_metrics()
        profile = self._QUERY_PROFILES.get(query_type, self._QUERY_PROFILES["frequency"])
        aq   = profile["accuracy"]
        lsla = profile["latency_sla_ms"]
        ranked = self._rank_nodes(cluster, aq, lsla)
        return [
            {
                "worker_id": wid,
                "score":     round(score, 4),
                "metrics":   cluster.get(wid, {}),
            }
            for wid, score in ranked
        ]
