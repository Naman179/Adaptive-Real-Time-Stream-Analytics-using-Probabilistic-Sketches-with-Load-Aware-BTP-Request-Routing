"""
Analytics Worker — consumes Kafka events and updates probabilistic sketches.

Each worker instance:
  1. Subscribes to the Kafka topic.
  2. For every event, updates CMS (frequency), HLL (cardinality), MG (heavy hitters).
  3. Every REPORT_INTERVAL seconds, publishes metrics to Redis.
  4. Listens on a Redis pub/sub channel for controller commands to change parameters.

Run:  python stream/worker.py --worker-id worker-0
"""

import argparse
import json
import os
import sys
import time
import threading
import psutil

from dotenv import load_dotenv

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from sketches.count_min_sketch import CountMinSketch
from sketches.hyperloglog import HyperLogLog
from sketches.misra_gries import SlidingWindowMisraGries
from state.store import StateStore

load_dotenv()

KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "stream_events")
REPORT_INTERVAL  = 5   # seconds between metric pushes to Redis

# Precision parameter templates (Low / Medium / High)
TEMPLATES = {
    "low":    {"cms_width": 512,  "cms_depth": 3,  "hll_precision": 10, "mg_k": 50},
    "medium": {"cms_width": 2048, "cms_depth": 5,  "hll_precision": 12, "mg_k": 200},
    "high":   {"cms_width": 8192, "cms_depth": 7,  "hll_precision": 14, "mg_k": 500},
}


class AnalyticsWorker:
    """
    Single-node analytics worker.  Holds one instance of each sketch.
    """

    def __init__(self, worker_id: str, template: str = "medium"):
        self.worker_id = worker_id
        self.store = StateStore()
        self._latency_samples: list[float] = []
        self._lock = threading.Lock()

        self._apply_template(template)
        self._connect_kafka()

        # Subscribe to controller commands via Redis pub/sub
        self._cmd_thread = threading.Thread(
            target=self._listen_commands, daemon=True
        )
        self._cmd_thread.start()

    # ------------------------------------------------------------------
    # Sketch initialisation / reconfiguration
    # ------------------------------------------------------------------

    def _apply_template(self, template: str) -> None:
        """Instantiate or replace sketches using the given template name."""
        params = TEMPLATES.get(template, TEMPLATES["medium"])
        with self._lock:
            self.cms = CountMinSketch(
                width=params["cms_width"],
                depth=params["cms_depth"],
            )
            self.hll = HyperLogLog(precision=params["hll_precision"])
            self.mg  = SlidingWindowMisraGries(k=params["mg_k"])
            self.current_template = template
        print(f"[{self.worker_id}] Applied template '{template}': {params}")

    # ------------------------------------------------------------------
    # Kafka connection
    # ------------------------------------------------------------------

    def _connect_kafka(self, retries: int = 15) -> None:
        for attempt in range(1, retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_SERVERS,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id=f"btp-{self.worker_id}",
                    consumer_timeout_ms=1000,
                )
                print(f"[{self.worker_id}] Connected to Kafka topic '{KAFKA_TOPIC}'")
                return
            except NoBrokersAvailable:
                print(f"[{self.worker_id}] Kafka not ready ({attempt}/{retries}), retrying...")
                time.sleep(3)
        raise RuntimeError("Could not connect to Kafka")

    # ------------------------------------------------------------------
    # Controller command listener (Redis pub/sub)
    # ------------------------------------------------------------------

    def _listen_commands(self) -> None:
        """Background thread: receive parameter-change commands from the controller."""
        pubsub = self.store.redis.pubsub()
        channel = f"ctrl:commands:{self.worker_id}"
        pubsub.subscribe(channel)
        print(f"[{self.worker_id}] Listening for commands on '{channel}'")
        for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                cmd = json.loads(message["data"])
                action = cmd.get("action")
                if action == "set_template":
                    self._apply_template(cmd["template"])
                    self.store.log_controller_decision(
                        self.worker_id,
                        f"Template changed to '{cmd['template']}' by controller"
                    )
                elif action == "resize_mg":
                    with self._lock:
                        self.mg.resize(cmd["k"])
                    print(f"[{self.worker_id}] Resized MisraGries k={cmd['k']}")
            except Exception as e:
                print(f"[{self.worker_id}] Command error: {e}")

    # ------------------------------------------------------------------
    # Main processing loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        print(f"[{self.worker_id}] Starting event processing loop ...")
        last_report = time.time()
        event_count = 0

        try:
            while True:
                for message in self.consumer:
                    t0 = time.perf_counter()
                    event = message.value
                    key   = event.get("key", "")

                    # Update all three sketches under lock
                    with self._lock:
                        self.cms.update(key)
                        self.hll.add(key)
                        self.mg.add(key)

                    latency_ms = (time.perf_counter() - t0) * 1000
                    self._latency_samples.append(latency_ms)
                    # Keep at most 1000 samples for percentile calculation
                    if len(self._latency_samples) > 1000:
                        self._latency_samples.pop(0)

                    event_count += 1

                    # Periodic metric reporting
                    now = time.time()
                    if now - last_report >= REPORT_INTERVAL:
                        self._report_metrics(event_count)
                        last_report = now

        except KeyboardInterrupt:
            print(f"\n[{self.worker_id}] Shutting down")
        finally:
            self.consumer.close()

    # ------------------------------------------------------------------
    # Metric reporting
    # ------------------------------------------------------------------

    def _percentile(self, data: list, p: float) -> float:
        """Calculate p-th percentile from a list of values."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * p / 100)
        return sorted_data[min(idx, len(sorted_data) - 1)]

    def _report_metrics(self, event_count: int) -> None:
        """Push current metrics snapshot to Redis for the controller/router."""
        samples = list(self._latency_samples)
        proc = psutil.Process()

        with self._lock:
            metrics = {
                "worker_id":      self.worker_id,
                "timestamp":      time.time(),
                "event_count":    event_count,
                "template":       self.current_template,
                # Latency percentiles (ms)
                "p50_latency_ms": self._percentile(samples, 50),
                "p95_latency_ms": self._percentile(samples, 95),
                "p99_latency_ms": self._percentile(samples, 99),
                # Memory
                "memory_mb":      proc.memory_info().rss / 1024 / 1024,
                "memory_percent": proc.memory_percent(),
                # Sketch metrics
                "cms_error_estimate": self.cms.estimated_error(),
                "cms_total_count":    self.cms.total_count,
                "hll_cardinality":    self.hll.count(),
                "hll_relative_error": self.hll.relative_error(),
                "mg_heavy_hitters":   self.mg.top_n(10),
                "mg_window_total":    self.mg.window_total(),
                "mg_error_bound":     self.mg.error_bound(),
                # Sketch parameters
                "cms_width": self.cms.width,
                "cms_depth": self.cms.depth,
                "hll_precision": self.hll.precision,
                "mg_k": self.mg.k,
            }

        self.store.save_worker_metrics(self.worker_id, metrics)
        print(
            f"[{self.worker_id}] metrics | "
            f"P99={metrics['p99_latency_ms']:.2f}ms | "
            f"mem={metrics['memory_mb']:.1f}MB | "
            f"HLL≈{metrics['hll_cardinality']:,} | "
            f"CMS_err≈{metrics['cms_error_estimate']:.1f} | "
            f"tpl={metrics['template']}"
        )


# ---- CLI entry point ------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BTP Analytics Worker")
    parser.add_argument("--worker-id", default="worker-0", help="Unique worker identifier")
    parser.add_argument("--template",  default="medium",   choices=list(TEMPLATES.keys()),
                        help="Initial precision template")
    args = parser.parse_args()

    worker = AnalyticsWorker(worker_id=args.worker_id, template=args.template)
    worker.run()
