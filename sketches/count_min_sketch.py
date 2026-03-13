"""
Count-Min Sketch (CMS) — memory-efficient frequency estimator.

Theory:
  Uses d hash functions and a w-wide counter array.
  For key x, estimate f(x) = min over d rows of counters[row][h_row(x)].
  Guarantees: f(x) <= true_f(x) + epsilon * N  with probability (1 - delta)
  where w = ceil(e / epsilon),  d = ceil(ln(1/delta))

Reference:
  Cormode & Muthukrishnan, "An improved data stream summary:
  the Count-Min Sketch and its applications", J. Algorithms 2005.
"""

import hashlib
import math
import struct
from typing import List


class CountMinSketch:
    """
    A two-dimensional counter array sketch for approximate frequency counting.

    Parameters
    ----------
    width  : int  — number of counters per row (controls error epsilon)
    depth  : int  — number of hash functions / rows (controls confidence delta)
    """

    def __init__(self, width: int = 2048, depth: int = 5):
        if width < 1 or depth < 1:
            raise ValueError("width and depth must be >= 1")
        self.width = width
        self.depth = depth
        # 2-D counter table: depth rows x width columns, initialised to zero
        self.table: List[List[int]] = [[0] * width for _ in range(depth)]
        self.total_count: int = 0  # N — total items inserted

    # ------------------------------------------------------------------
    # Hash helpers
    # ------------------------------------------------------------------

    def _hash(self, key: str, seed: int) -> int:
        """
        Deterministic hash of (key, seed) → bucket index in [0, width).

        We concatenate the seed as a 4-byte prefix so each row gets an
        independent hash family without needing external libraries.
        """
        raw = struct.pack(">I", seed) + key.encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        # Use the first 8 bytes as a 64-bit integer
        val = struct.unpack(">Q", digest[:8])[0]
        return val % self.width

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def update(self, key: str, count: int = 1) -> None:
        """Insert key (or increment by count). O(depth) time."""
        for row in range(self.depth):
            col = self._hash(key, row)
            self.table[row][col] += count
        self.total_count += count

    def query(self, key: str) -> int:
        """
        Return the estimated frequency of key.
        Always >= true frequency; over-estimate bounded by epsilon * N.
        """
        return min(
            self.table[row][self._hash(key, row)] for row in range(self.depth)
        )

    # ------------------------------------------------------------------
    # Error / memory metrics
    # ------------------------------------------------------------------

    def epsilon(self) -> float:
        """Theoretical additive error factor: e / width."""
        return math.e / self.width

    def delta(self) -> float:
        """Failure probability: e^(-depth)."""
        return math.exp(-self.depth)

    def estimated_error(self) -> float:
        """
        Worst-case absolute additive error for any single key:
            epsilon * N  (with probability 1 - delta)
        """
        return self.epsilon() * self.total_count

    def memory_bytes(self) -> int:
        """Approximate memory used by the counter table (assuming 8-byte ints)."""
        return self.width * self.depth * 8

    # ------------------------------------------------------------------
    # Dynamic reconfiguration (called by the Adaptive Controller)
    # ------------------------------------------------------------------

    def resize(self, new_width: int, new_depth: int) -> "CountMinSketch":
        """
        Return a NEW CountMinSketch with updated parameters.
        The old sketch is discarded; the worker will re-warm from the stream.
        This is safe because sketches are append-only and can be rebuilt.
        """
        return CountMinSketch(width=new_width, depth=new_depth)

    # ------------------------------------------------------------------
    # Serialisation (for state checkpointing to Redis)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "width": self.width,
            "depth": self.depth,
            "total_count": self.total_count,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CountMinSketch":
        obj = cls(width=d["width"], depth=d["depth"])
        obj.total_count = d["total_count"]
        obj.table = d["table"]
        return obj

    def __repr__(self) -> str:
        return (
            f"CountMinSketch(width={self.width}, depth={self.depth}, "
            f"N={self.total_count}, ε={self.epsilon():.4f}, δ={self.delta():.4f})"
        )


# ---------------------------------------------------------------------------
# Factory: build from (epsilon, delta) targets
# ---------------------------------------------------------------------------

def cms_from_error(epsilon: float, delta: float) -> CountMinSketch:
    """
    Construct a CMS satisfying the given error and confidence requirements.

    Parameters
    ----------
    epsilon : float — desired additive error fraction (e.g., 0.01 for 1%)
    delta   : float — failure probability (e.g., 0.01 for 99% confidence)
    """
    width = math.ceil(math.e / epsilon)
    depth = math.ceil(math.log(1.0 / delta))
    return CountMinSketch(width=width, depth=depth)
