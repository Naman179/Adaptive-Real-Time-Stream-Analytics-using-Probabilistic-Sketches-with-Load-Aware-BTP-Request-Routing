"""
Adaptive Controller — periodic rule-based sketch parameter controller.

Runs as a background asyncio task alongside the FastAPI server.
Every CONTROLLER_INTERVAL seconds it:
  1. Reads cluster metrics from Redis.
  2. Applies the cost-minimisation rules from Section 5.4 of the report.
  3. Issues parameter-change commands to workers via Redis pub/sub.

Rules (implementing the pseudocode in Appendix A of the report):
  - P99 > LATENCY_HIGH   →  reduce precision (downgrade template)
  - error > ERROR_HIGH AND memory OK  →  increase precision (upgrade template)
  - memory > MEMORY_HIGH  →  offload/shrink MG window

This is Equation (1) minimisation via discrete hill-climbing over templates.
"""

import asyncio
import os
import time
from typing import Dict

from dotenv import load_dotenv
from state.store import StateStore

load_dotenv()

LATENCY_HIGH_MS    = float(os.getenv("LATENCY_HIGH_MS",    "200"))
MEMORY_HIGH_PCT    = float(os.getenv("MEMORY_HIGH_PERCENT","80"))
ERROR_HIGH         = float(os.getenv("ERROR_HIGH",         "0.05"))
CONTROLLER_INTERVAL= float(os.getenv("CONTROLLER_INTERVAL_SEC", "5"))

# Ordered precision levels (ascending)
TEMPLATE_ORDER = ["low", "medium", "high"]


def _upgrade(current: str) -> str:
    """Upgrade to the next precision level, if possible."""
    idx = TEMPLATE_ORDER.index(current)
    return TEMPLATE_ORDER[min(idx + 1, len(TEMPLATE_ORDER) - 1)]


def _downgrade(current: str) -> str:
    """Downgrade to the previous precision level, if possible."""
    idx = TEMPLATE_ORDER.index(current)
    return TEMPLATE_ORDER[max(idx - 1, 0)]


class AdaptiveController:
    """
    Adaptive controller that tunes sketch parameters across all workers.

    Implements the rule-based decision logic from Section 5.4 of the report,
    corresponding to the cost function C(θ, r) in Equation (1).
    """

    def __init__(self):
        self.store = StateStore()
        self._running = False
        # Track last decision per worker to avoid flapping
        self._last_decision: Dict[str, float] = {}
        self._cooldown_sec = 15  # minimum seconds between decisions per worker

    async def run(self) -> None:
        """Main controller loop — run as an asyncio background task."""
        self._running = True
        print(f"[Controller] Started. Interval={CONTROLLER_INTERVAL}s, "
              f"Thresholds: P99>{LATENCY_HIGH_MS}ms, "
              f"mem>{MEMORY_HIGH_PCT}%, err>{ERROR_HIGH}")
        while self._running:
            await asyncio.sleep(CONTROLLER_INTERVAL)
            await self._control_cycle()

    async def _control_cycle(self) -> None:
        """Single control cycle: evaluate all workers and issue commands."""
        cluster = self.store.get_cluster_metrics()
        if not cluster:
            return

        for worker_id, metrics in cluster.items():
            # Respect cooldown to avoid thrashing
            now = time.time()
            last = self._last_decision.get(worker_id, 0)
            if now - last < self._cooldown_sec:
                continue

            current_template = metrics.get("template", "medium")
            p99              = metrics.get("p99_latency_ms", 0)
            memory_pct       = metrics.get("memory_percent", 0)
            # Normalise CMS error: estimated_error / total_count (relative error)
            total            = metrics.get("cms_total_count", 1) or 1
            rel_error        = metrics.get("cms_error_estimate", 0) / total

            decision = None

            # Rule 1: Latency constraint violated → reduce precision
            if p99 > LATENCY_HIGH_MS:
                new_template = _downgrade(current_template)
                if new_template != current_template:
                    decision = (
                        f"P99={p99:.1f}ms > {LATENCY_HIGH_MS}ms → "
                        f"downgrade {current_template}→{new_template}"
                    )
                    self._send_template(worker_id, new_template)

            # Rule 2: Accuracy degraded AND memory available → increase precision
            elif rel_error > ERROR_HIGH and memory_pct < MEMORY_HIGH_PCT:
                new_template = _upgrade(current_template)
                if new_template != current_template:
                    decision = (
                        f"rel_error={rel_error:.4f} > {ERROR_HIGH} "
                        f"AND mem={memory_pct:.1f}% OK → "
                        f"upgrade {current_template}→{new_template}"
                    )
                    self._send_template(worker_id, new_template)

            # Rule 3: Memory pressure → offload (shrink MG window)
            elif memory_pct > MEMORY_HIGH_PCT:
                decision = (
                    f"memory={memory_pct:.1f}% > {MEMORY_HIGH_PCT}% → "
                    f"shrink MG window"
                )
                self._send_mg_resize(worker_id, new_k=50)

            if decision:
                self._last_decision[worker_id] = now
                self.store.log_controller_decision(worker_id, decision)
                print(f"[Controller] {worker_id}: {decision}")

    def _send_template(self, worker_id: str, template: str) -> None:
        """Issue a set_template command to a worker."""
        self.store.send_command(worker_id, {
            "action": "set_template",
            "template": template,
        })

    def _send_mg_resize(self, worker_id: str, new_k: int) -> None:
        """Issue a resize_mg command to a worker."""
        self.store.send_command(worker_id, {
            "action": "resize_mg",
            "k": new_k,
        })

    def stop(self) -> None:
        self._running = False
