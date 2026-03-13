"""
Sliding-Window Misra–Gries — heavy-hitter detection over a time window.

Theory:
  The classic Misra-Gries algorithm maintains at most (k-1) candidate items
  with approximate counts. Any item with true frequency > N/k is guaranteed
  to appear in the output.

  For sliding windows we use a timestamped-bucket approach:
    - Events are stored in fixed-size time buckets.
    - When a bucket falls outside the window, it is evicted.
    - Counts are accumulated across active buckets and then compressed using
      the Misra-Gries rule (global decrement when the candidate set is full).

  Error bound: any heavy hitter with frequency > N/k will be reported.
  False positive frequency error ≤ N/k per query.

Reference:
  Misra & Gries, "Finding repeated items", Science of Computer Programming 1982.
  Arasu & Manku, "Approximate counts and quantiles over sliding windows", PODS 2004.
"""

import time
from collections import defaultdict, deque
from typing import Dict, List, Tuple


class SlidingWindowMisraGries:
    """
    Approximate heavy-hitter detection over a sliding time window.

    Parameters
    ----------
    k           : int   — size of candidate set; items with freq > N/k reported
    window_sec  : float — length of the sliding window in seconds
    bucket_sec  : float — granularity of time buckets (must divide window_sec)
    """

    def __init__(
        self,
        k: int = 200,
        window_sec: float = 60.0,
        bucket_sec: float = 5.0,
    ):
        if k < 2:
            raise ValueError("k must be >= 2")
        if bucket_sec > window_sec:
            raise ValueError("bucket_sec must be <= window_sec")

        self.k = k
        self.window_sec = window_sec
        self.bucket_sec = bucket_sec

        # Each bucket is a dict {item: count}.
        # We use a deque of (bucket_start_time, dict) tuples.
        self._buckets: deque = deque()
        self._current_bucket_start: float = time.time()
        self._current_bucket: Dict[str, int] = defaultdict(int)

        # The merged candidate set across all active buckets (Misra-Gries style).
        # Rebuilt on demand when window is pruned.
        self._candidates: Dict[str, int] = {}

        # Running total of events inside the window
        self._window_total: int = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flush_current_bucket(self) -> None:
        """Move the current bucket into the deque."""
        if self._current_bucket:
            self._buckets.append(
                (self._current_bucket_start, dict(self._current_bucket))
            )
            self._current_bucket = defaultdict(int)
        self._current_bucket_start = time.time()

    def _evict_old_buckets(self, current_time: float) -> None:
        """Remove buckets that have slid out of the window."""
        cutoff = current_time - self.window_sec
        while self._buckets and self._buckets[0][0] < cutoff:
            _, evicted = self._buckets.popleft()
            for item, cnt in evicted.items():
                self._window_total -= cnt

    def _rebuild_candidates(self) -> None:
        """
        Re-apply the Misra-Gries compression to all active bucket counts.
        Called after window eviction to get a fresh candidate set.
        """
        # Aggregate raw counts across all active buckets
        raw: Dict[str, int] = defaultdict(int)
        for _, bucket in self._buckets:
            for item, cnt in bucket.items():
                raw[item] += cnt
        # Also include current (not yet flushed) bucket
        for item, cnt in self._current_bucket.items():
            raw[item] += cnt

        # Misra-Gries compression: keep at most k-1 candidates
        candidates: Dict[str, int] = {}
        for item, cnt in sorted(raw.items(), key=lambda x: -x[1]):
            if len(candidates) < self.k - 1:
                candidates[item] = cnt
            else:
                # Decrement all by the smallest candidate count
                if not candidates:
                    break
                min_val = min(candidates.values())
                candidates = {
                    it: c - min_val
                    for it, c in candidates.items()
                    if c > min_val
                }
                if cnt > min_val:
                    candidates[item] = cnt - min_val
        self._candidates = candidates

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def add(self, item: str, timestamp: float | None = None) -> None:
        """
        Record an occurrence of item at the given timestamp (default: now).
        O(1) amortised per event; O(k log k) per bucket flush.
        """
        ts = timestamp if timestamp is not None else time.time()

        # Check if we need to start a new bucket
        if ts - self._current_bucket_start >= self.bucket_sec:
            self._flush_current_bucket()
            self._evict_old_buckets(ts)
            self._rebuild_candidates()

        self._current_bucket[item] += 1
        self._window_total += 1

    def heavy_hitters(self, min_frequency: int | None = None) -> List[Tuple[str, int]]:
        """
        Return list of (item, estimated_count) sorted by count descending.

        Parameters
        ----------
        min_frequency : optional filter — only return items with count >= this value.
                        If None, returns all candidates.
        """
        now = time.time()
        self._evict_old_buckets(now)
        self._rebuild_candidates()

        results = sorted(self._candidates.items(), key=lambda x: -x[1])
        if min_frequency is not None:
            results = [(it, cnt) for it, cnt in results if cnt >= min_frequency]
        return results

    def top_n(self, n: int = 10) -> List[Tuple[str, int]]:
        """Return the top-n estimated heavy hitters."""
        return self.heavy_hitters()[:n]

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def window_total(self) -> int:
        """Approximate total events within the sliding window."""
        return self._window_total + sum(self._current_bucket.values())

    def error_bound(self) -> float:
        """
        Per-item count error bound: window_total / k.
        Any item with true count > this is guaranteed to be reported.
        """
        return self.window_total() / self.k

    def memory_bytes(self) -> int:
        """Rough memory estimate: k candidates * ~100 bytes each."""
        return self.k * 100

    # ------------------------------------------------------------------
    # Dynamic reconfiguration
    # ------------------------------------------------------------------

    def resize(self, new_k: int, new_window_sec: float | None = None) -> None:
        """
        Update k (candidate size) and/or window length in-place.
        Immediately triggers a candidate rebuild.
        """
        self.k = new_k
        if new_window_sec is not None:
            self.window_sec = new_window_sec
        self._evict_old_buckets(time.time())
        self._rebuild_candidates()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        self._flush_current_bucket()
        return {
            "k": self.k,
            "window_sec": self.window_sec,
            "bucket_sec": self.bucket_sec,
            "buckets": [(ts, d) for ts, d in self._buckets],
            "candidates": self._candidates,
            "window_total": self._window_total,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SlidingWindowMisraGries":
        obj = cls(
            k=d["k"],
            window_sec=d["window_sec"],
            bucket_sec=d["bucket_sec"],
        )
        obj._buckets = deque(
            (ts, cnt) for ts, cnt in d["buckets"]
        )
        obj._candidates = d["candidates"]
        obj._window_total = d["window_total"]
        return obj

    def __repr__(self) -> str:
        return (
            f"SlidingWindowMisraGries(k={self.k}, "
            f"window={self.window_sec}s, "
            f"candidates={len(self._candidates)}, "
            f"N_window≈{self.window_total()})"
        )
