"""
Kafka Stream Producer — synthetic workload generator.

Generates events following a Zipfian distribution (or uniform) and
publishes them to a Kafka topic as JSON messages.

Run:  python stream/producer.py [--rate 5000] [--zipf 1.2] [--duration 60]
"""

import argparse
import json
import math
import os
import random
import time

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "stream_events")

# ---- Zipfian distribution sampler ------------------------------------

class ZipfSampler:
    """
    Draw samples from a Zipf(s) distribution over `n` items.
    P(rank k) ∝ 1 / k^s

    Parameters
    ----------
    n : int   — vocabulary size (number of distinct items)
    s : float — skewness parameter (s=1.0 is standard Zipf)
    """

    def __init__(self, n: int = 10_000, s: float = 1.2, offset: int = 0):
        self.n = n
        self.s = s
        self.offset = offset
        # Pre-compute CDF for fast sampling
        weights = [1.0 / (k ** s) for k in range(1, n + 1)]
        total = sum(weights)
        self._cdf = []
        cumulative = 0.0
        for w in weights:
            cumulative += w / total
            self._cdf.append(cumulative)

    def sample(self) -> str:
        """Return a random item key drawn from the Zipf distribution."""
        r = random.random()
        # Binary search for the rank
        lo, hi = 0, self.n - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if self._cdf[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        return f"item_{((lo + self.offset) % self.n) + 1}"


# ---- Producer -------------------------------------------------------

class StreamProducer:
    """
    Publishes synthetic stream events to a Kafka topic.

    Each event message:
      {"key": "item_42", "ts": 1710000000.123, "source": "producer-0"}
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_SERVERS,
        topic: str = KAFKA_TOPIC,
        n_items: int = 10_000,
        zipf_s: float = 1.2,
        shift_interval_sec: float = 0,
    ):
        self.topic = topic
        self.shift_interval_sec = shift_interval_sec
        self.sampler = ZipfSampler(n=n_items, s=zipf_s)
        self._connect(bootstrap_servers)

    def _connect(self, servers: str, retries: int = 10) -> None:
        for attempt in range(1, retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks=1,
                    linger_ms=5,
                    batch_size=16384,
                )
                print(f"[Producer] Connected to Kafka at {servers}")
                return
            except NoBrokersAvailable:
                print(f"[Producer] Kafka not ready (attempt {attempt}/{retries}), retrying in 3s ...")
                time.sleep(3)
        raise RuntimeError("Could not connect to Kafka after retries")

    def run(self, rate: int = 5_000, duration_sec: float = float("inf")) -> None:
        """
        Send events at the target `rate` (events/sec) for `duration_sec` seconds.
        rate=0 means as fast as possible.
        """
        print(f"[Producer] Streaming to topic '{self.topic}' at {rate} events/sec ...")
        if self.shift_interval_sec > 0:
            print(f"[Producer] CONCEPT DRIFT ENABLED: Distribution shifts every {self.shift_interval_sec}s")
            
        start = time.time()
        last_shift = start
        sent = 0
        interval = 1.0 / rate if rate > 0 else 0

        try:
            while True:
                elapsed = time.time() - start
                if elapsed >= duration_sec:
                    break
                    
                # Concept drift logic
                if self.shift_interval_sec > 0 and (time.time() - last_shift) >= self.shift_interval_sec:
                    new_offset = random.randint(0, self.sampler.n - 1)
                    self.sampler.offset = new_offset
                    last_shift = time.time()
                    print(f"\n[Producer] ⚠️ CONCEPT DRIFT: Distribution shifted! "
                          f"New top item is item_{new_offset + 1}\n")

                event = {
                    "key": self.sampler.sample(),
                    "ts": time.time(),
                }
                self.producer.send(self.topic, event)
                sent += 1

                if sent % 10_000 == 0:
                    print(f"[Producer] Sent {sent:,} events in {elapsed:.1f}s "
                          f"({sent/elapsed:.0f} events/sec)")

                if interval > 0:
                    time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[Producer] Interrupted by user")
        finally:
            self.producer.flush()
            print(f"[Producer] Done. Total sent: {sent:,}")


# ---- CLI entry point ------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BTP Stream Producer")
    parser.add_argument("--rate",     type=int,   default=5000,  help="Events per second (0 = unlimited)")
    parser.add_argument("--zipf",     type=float, default=1.2,   help="Zipf skewness parameter")
    parser.add_argument("--items",    type=int,   default=10000, help="Vocabulary size")
    parser.add_argument("--duration", type=float, default=0,     help="Duration in seconds (0 = infinite)")
    parser.add_argument("--shift-interval", type=float, default=0, help="Shift distribution every X seconds (concept drift)")
    args = parser.parse_args()

    producer = StreamProducer(n_items=args.items, zipf_s=args.zipf, shift_interval_sec=args.shift_interval)
    producer.run(
        rate=args.rate,
        duration_sec=args.duration if args.duration > 0 else float("inf"),
    )
