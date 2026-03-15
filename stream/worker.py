"""
Python Analytics Worker — consumes from Kafka, runs CMS/HLL/Misra-Gries,
writes metrics to Redis every REPORT_INTERVAL_SEC seconds.

Run:
    python stream/worker.py --worker-id worker-0 --template medium

Multiple instances can run simultaneously (each with a unique --worker-id).
"""

import argparse
import hashlib
import json
import math
import os
import random
import sys
import time
import threading
from collections import deque
from pathlib import Path
from typing import Dict, List, Tuple

import psutil
import redis
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "stream_events")
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT", 6379))
REPORT_INTERVAL = float(os.getenv("WORKER_REPORT_INTERVAL_SEC", "2"))
GROUP_ID        = os.getenv("KAFKA_GROUP_ID", "btp-analytics-workers")

# ---------------------------------------------------------------------------
# Precision templates (mirrors cpp/worker/worker.cpp)
# ---------------------------------------------------------------------------
TEMPLATES = {
    "low":    {"cms_width": 512,  "cms_depth": 3, "hll_p": 10, "mg_k": 50},
    "medium": {"cms_width": 2048, "cms_depth": 5, "hll_p": 12, "mg_k": 200},
    "high":   {"cms_width": 8192, "cms_depth": 7, "hll_p": 14, "mg_k": 500},
}

# ---------------------------------------------------------------------------
# Count-Min Sketch
# ---------------------------------------------------------------------------
class CountMinSketch:
    def __init__(self, width: int, depth: int):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
        self.total = 0

    def _hash(self, key: str, row: int) -> int:
        h = hashlib.md5(f"{key}:{row}".encode()).digest()
        return int.from_bytes(h[:8], "little") % self.width

    def update(self, key: str, count: int = 1):
        for row in range(self.depth):
            self.table[row][self._hash(key, row)] += count
        self.total += count

    def query(self, key: str) -> int:
        return min(self.table[row][self._hash(key, row)] for row in range(self.depth))

    def error_estimate(self) -> float:
        return (math.e / self.width) * self.total

    def memory_bytes(self) -> int:
        return self.width * self.depth * 8

# ---------------------------------------------------------------------------
# HyperLogLog
# ---------------------------------------------------------------------------
class HyperLogLog:
    def __init__(self, precision: int):
        self.precision = precision
        self.m = 1 << precision
        self.registers = [0] * self.m
        # Bias correction constant
        if self.m >= 128:
            self._alpha = 0.7213 / (1 + 1.079 / self.m)
        elif self.m == 64:
            self._alpha = 0.709
        elif self.m == 32:
            self._alpha = 0.697
        else:
            self._alpha = 0.673

    def _hash64(self, item: str) -> int:
        h = hashlib.md5(item.encode()).digest()
        return int.from_bytes(h[:8], "little")

    def add(self, item: str):
        h = self._hash64(item)
        idx = h >> (64 - self.precision)
        w   = h & ((1 << (64 - self.precision)) - 1)
        rho = 1
        while rho <= (64 - self.precision) and not (w & (1 << (64 - self.precision - rho))):
            rho += 1
        self.registers[idx] = max(self.registers[idx], rho)

    def count(self) -> int:
        z = sum(2.0 ** (-r) for r in self.registers)
        est = self._alpha * self.m * self.m / z
        # Small range correction
        if est <= 2.5 * self.m:
            zeros = self.registers.count(0)
            if zeros > 0:
                est = self.m * math.log(self.m / zeros)
        # Large range correction
        elif est > (1 << 32) / 30:
            est = -(1 << 32) * math.log(1 - est / (1 << 32))
        return int(est)

    def relative_error(self) -> float:
        return 1.04 / math.sqrt(self.m)

    def memory_bytes(self) -> int:
        return self.m  # 1 byte per register

# ---------------------------------------------------------------------------
# Sliding-window Misra-Gries
# ---------------------------------------------------------------------------
class MisraGries:
    def __init__(self, k: int, window_size: int = 10000):
        self.k = k
        self.window_size = window_size
        self.counts: Dict[str, int] = {}
        self.window: deque = deque()   # (item, delta) tuples
        self.window_total = 0

    def add(self, item: str):
        # Add to window
        self.window.append(item)
        self.window_total += 1
        # Evict if over window size
        if len(self.window) > self.window_size:
            old = self.window.popleft()
            self.window_total -= 1
            if old in self.counts:
                self.counts[old] -= 1
                if self.counts[old] <= 0:
                    del self.counts[old]

        # Standard MG update
        if item in self.counts:
            self.counts[item] += 1
        elif len(self.counts) < self.k - 1:
            self.counts[item] = 1
        else:
            min_val = min(self.counts.values()) if self.counts else 0
            self.counts = {k: v - min_val for k, v in self.counts.items() if v > min_val}

    def heavy_hitters(self, n: int = 20) -> List[Tuple[str, int]]:
        return sorted(self.counts.items(), key=lambda x: x[1], reverse=True)[:n]

    def error_bound(self) -> float:
        return self.window_total / max(self.k, 1)

    def memory_bytes(self) -> int:
        return self.k * 100

# ---------------------------------------------------------------------------
# Worker Node
# ---------------------------------------------------------------------------
class AnalyticsWorker:
    def __init__(self, worker_id: str, template: str = "medium"):
        self.worker_id = worker_id
        self.template  = template
        self._latencies: deque = deque(maxlen=500)
        self._lock = threading.Lock()
        self._running = True
        self._cmd_thread = None

        # Redis
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.redis.ping()
        print(f"[Worker:{worker_id}] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Sketches
        self._apply_template(template)

    def _apply_template(self, tpl: str):
        p = TEMPLATES.get(tpl, TEMPLATES["medium"])
        with self._lock:
            self.cms = CountMinSketch(p["cms_width"],  p["cms_depth"])
            self.hll = HyperLogLog(p["hll_p"])
            self.mg  = MisraGries(p["mg_k"])
            self.template = tpl
        print(f"[Worker:{self.worker_id}] Applied template '{tpl}': "
              f"CMS {p['cms_width']}×{p['cms_depth']}, HLL p={p['hll_p']}, MG k={p['mg_k']}")

    def process(self, key: str):
        t0 = time.perf_counter()
        with self._lock:
            self.cms.update(key)
            self.hll.add(key)
            self.mg.add(key)
        lat_ms = (time.perf_counter() - t0) * 1000
        self._latencies.append(lat_ms)
        return lat_ms

    def p99(self) -> float:
        lats = list(self._latencies)
        if not lats:
            return 0.0
        s = sorted(lats)
        return s[int(len(s) * 0.99)]

    def report_metrics(self):
        """Write current sketch state to Redis."""
        with self._lock:
            hh = self.mg.heavy_hitters(20)
            metrics = {
                "worker_id":          self.worker_id,
                "template":           self.template,
                "ts":                 time.time(),
                # CMS
                "cms_width":          self.cms.width,
                "cms_depth":          self.cms.depth,
                "cms_total_count":    self.cms.total,
                "cms_error_estimate": round(self.cms.error_estimate(), 2),
                # HLL
                "hll_precision":      self.hll.precision,
                "hll_cardinality":    self.hll.count(),
                "hll_relative_error": round(self.hll.relative_error(), 6),
                # MG
                "mg_k":               self.mg.k,
                "mg_heavy_hitters":   hh,
                "mg_window_total":    self.mg.window_total,
                "mg_error_bound":     round(self.mg.error_bound(), 2),
                # System
                "p99_latency_ms":     round(self.p99(), 4),
                "memory_percent":     round(psutil.virtual_memory().percent, 1),
                "cpu_percent":        round(psutil.cpu_percent(interval=None), 1),
            }

        key = f"worker:{self.worker_id}:metrics"
        self.redis.set(key, json.dumps(metrics), ex=30)
        self.redis.sadd("btp:workers", self.worker_id)

        # Latency history ring buffer
        hist_key = f"worker:{self.worker_id}:latency_history"
        self.redis.rpush(hist_key, metrics["p99_latency_ms"])
        self.redis.ltrim(hist_key, -60, -1)
        self.redis.expire(hist_key, 120)

    def _listen_for_commands(self):
        """Poll Redis for controller commands."""
        channel = f"ctrl:commands:{self.worker_id}"
        pubsub  = self.redis.pubsub()
        pubsub.subscribe(channel)
        print(f"[Worker:{self.worker_id}] Listening for commands on {channel}")
        for msg in pubsub.listen():
            if not self._running:
                break
            if msg["type"] != "message":
                continue
            try:
                cmd = json.loads(msg["data"])
                if cmd.get("action") == "set_template":
                    self._apply_template(cmd["template"])
                elif cmd.get("action") == "resize_mg":
                    new_k = cmd.get("k", 50)
                    with self._lock:
                        self.mg = MisraGries(new_k)
                    print(f"[Worker:{self.worker_id}] Resized MG to k={new_k}")
            except Exception as e:
                print(f"[Worker:{self.worker_id}] Bad command: {e}")

    def run(self, kafka_servers: str, topic: str):
        from kafka import KafkaConsumer

        # Start command listener thread
        self._cmd_thread = threading.Thread(target=self._listen_for_commands, daemon=True)
        self._cmd_thread.start()

        # Connect to Kafka with retries
        # Each worker needs ALL events (analytics fan-out), so use unique group_id
        unique_group = f"{GROUP_ID}-{self.worker_id}"
        consumer = None
        for attempt in range(1, 11):
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=kafka_servers,
                    group_id=unique_group,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                )
                print(f"[Worker:{self.worker_id}] Connected to Kafka at {kafka_servers}")
                break
            except Exception as e:
                print(f"[Worker:{self.worker_id}] Kafka not ready (attempt {attempt}/10): {e}")
                time.sleep(3)

        if consumer is None:
            raise RuntimeError("Could not connect to Kafka after retries")

        last_report = time.time()
        total       = 0

        print(f"[Worker:{self.worker_id}] Consuming from topic '{topic}'…")
        try:
            while self._running:
                for msg in consumer:
                    if not self._running:
                        break
                    key = msg.value.get("key", "")
                    if key:
                        self.process(key)
                        total += 1

                    # Report metrics every REPORT_INTERVAL seconds
                    now = time.time()
                    if now - last_report >= REPORT_INTERVAL:
                        self.report_metrics()
                        last_report = now
                        print(f"[Worker:{self.worker_id}] Processed {total:,} events | "
                              f"HLL={self.hll.count():,} | "
                              f"P99={self.p99():.3f}ms | "
                              f"Template={self.template}")

                # Also report when idle (consumer poll timeout) to keep Redis key alive
                now = time.time()
                if now - last_report >= REPORT_INTERVAL:
                    self.report_metrics()
                    last_report = now

        except KeyboardInterrupt:
            print(f"\n[Worker:{self.worker_id}] Interrupted")
        finally:
            self._running = False
            consumer.close()
            self.report_metrics()  # Final flush
            print(f"[Worker:{self.worker_id}] Done. Total events: {total:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BTP Python Analytics Worker")
    parser.add_argument("--worker-id", default="worker-0",   help="Unique worker identifier")
    parser.add_argument("--template",  default="medium",      choices=["low", "medium", "high"])
    parser.add_argument("--servers",   default=KAFKA_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic",     default=KAFKA_TOPIC,   help="Kafka topic to consume")
    args = parser.parse_args()

    worker = AnalyticsWorker(args.worker_id, args.template)
    worker.run(args.servers, args.topic)
