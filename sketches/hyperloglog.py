"""
HyperLogLog (HLL) — cardinality (distinct count) estimator.

Theory:
  Uses m = 2^p registers. Each new item is hashed; the position of the
  leftmost 1-bit in the hash determines the register to update.
  The harmonic mean of 2^register_value over all m registers gives the
  cardinality estimate.  Relative std error ≈ 1.04 / sqrt(m).

Reference:
  Flajolet, Fusy, Gandouet, Meunier, "HyperLogLog: the analysis of a
  near-optimal cardinality estimation algorithm", AOFA 2007.
"""

import hashlib
import math
import struct
from typing import List


# Bias-correction constants (standard HLL)
_ALPHA = {
    16: 0.673,
    32: 0.697,
    64: 0.709,
}


def _alpha(m: int) -> float:
    """Bias-correction constant for m registers."""
    if m in _ALPHA:
        return _ALPHA[m]
    return 0.7213 / (1.0 + 1.079 / m)


class HyperLogLog:
    """
    Probabilistic cardinality estimator using the HyperLogLog algorithm.

    Parameters
    ----------
    precision : int — number of bits used to index registers (p).
                      m = 2^p registers are maintained.
                      Typical range: 4 (low accuracy) to 18 (high accuracy).
                      Relative error ≈ 1.04 / sqrt(2^p).
    """

    def __init__(self, precision: int = 12):
        if not (4 <= precision <= 18):
            raise ValueError("precision must be between 4 and 18")
        self.precision = precision
        self.m: int = 1 << precision          # number of registers = 2^p
        self.registers: List[int] = [0] * self.m
        self._alpha: float = _alpha(self.m)

    # ------------------------------------------------------------------
    # Hash helper
    # ------------------------------------------------------------------

    def _hash64(self, item: str) -> int:
        """64-bit hash of the item string."""
        raw = item.encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        return struct.unpack(">Q", digest[:8])[0]

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def add(self, item: str) -> None:
        """
        Register item in the sketch. O(1) time, O(m) space total.
        The top-p bits select the register; the remaining bits determine
        the run of leading zeros (rho function).
        """
        h = self._hash64(item)
        # Top p bits → register index
        register_idx = h >> (64 - self.precision)
        # Remaining 64-p bits → count leading zeros (+ 1)
        remaining = h & ((1 << (64 - self.precision)) - 1)
        # Shift remaining into the high bits of a 64-bit word
        if remaining == 0:
            rho = 64 - self.precision + 1
        else:
            shifted = remaining << self.precision
            # Count leading zeros in the 64-bit shifted value
            rho = 1
            while rho <= (64 - self.precision) and not (shifted & (1 << 63)):
                rho += 1
                shifted <<= 1
        self.registers[register_idx] = max(self.registers[register_idx], rho)

    def count(self) -> int:
        """
        Return the estimated cardinality of the set of items added so far.
        Applies small-range and large-range corrections as per the paper.
        """
        m = self.m
        # Raw harmonic mean estimate
        raw = self._alpha * m * m / sum(
            2.0 ** (-r) for r in self.registers
        )

        # Small range correction (linear counting)
        zeros = self.registers.count(0)
        if raw <= 2.5 * m and zeros > 0:
            return round(m * math.log(m / zeros))

        # Large range correction
        two_32 = 1 << 32
        if raw > two_32 / 30.0:
            return round(-two_32 * math.log(1.0 - raw / two_32))

        return round(raw)

    # ------------------------------------------------------------------
    # Accuracy metrics
    # ------------------------------------------------------------------

    def relative_error(self) -> float:
        """Theoretical relative standard error: 1.04 / sqrt(m)."""
        return 1.04 / math.sqrt(self.m)

    def memory_bytes(self) -> int:
        """Approximate memory: each register is a small int (1 byte typical)."""
        return self.m  # 1 byte per register

    # ------------------------------------------------------------------
    # Merge (for distributed aggregation across workers)
    # ------------------------------------------------------------------

    def merge(self, other: "HyperLogLog") -> None:
        """
        Merge another HLL of the same precision into this one (in-place).
        Used when aggregating partial estimates from multiple workers.
        """
        if self.precision != other.precision:
            raise ValueError("Cannot merge HyperLogLog sketches of different precision")
        for i in range(self.m):
            self.registers[i] = max(self.registers[i], other.registers[i])

    # ------------------------------------------------------------------
    # Dynamic reconfiguration
    # ------------------------------------------------------------------

    def change_precision(self, new_precision: int) -> "HyperLogLog":
        """
        Return a new HLL with updated precision.
        Downgrade: fold registers (average lower half into upper half).
        Upgrade: expand (zero-fill new registers).
        Worker will rebuild from stream for accuracy; this preserves rough state.
        """
        new_hll = HyperLogLog(new_precision)
        new_m = new_hll.m
        if new_m <= self.m:
            # Downgrade: bin old registers into new (take max within each bin)
            factor = self.m // new_m
            for i in range(new_m):
                new_hll.registers[i] = max(
                    self.registers[i * factor: (i + 1) * factor]
                )
        else:
            # Upgrade: copy old registers, rest stay 0
            new_hll.registers[: self.m] = self.registers[:]
        return new_hll

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {"precision": self.precision, "registers": self.registers}

    @classmethod
    def from_dict(cls, d: dict) -> "HyperLogLog":
        obj = cls(precision=d["precision"])
        obj.registers = d["registers"]
        return obj

    def __repr__(self) -> str:
        return (
            f"HyperLogLog(p={self.precision}, m={self.m}, "
            f"cardinality≈{self.count()}, rel_error≈{self.relative_error():.3f})"
        )
