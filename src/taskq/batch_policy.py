"""Batch failure policy types.

A :class:`BatchFailurePolicy` decides whether a batch should be aborted
after observing a run of consecutive job failures.  The base class is
abstract; :class:`AbortBatchAfter` is the built-in concrete policy that
aborts once the consecutive-failure count reaches a configured threshold.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "AbortBatchAfter",
    "BatchFailurePolicy",
]


@dataclass(frozen=True, slots=True)
class BatchFailurePolicy:
    """Abstract base class for batch failure policies.

    Subclasses implement :meth:`should_abort` to decide whether a batch
    should be aborted given the current count of consecutive failures.
    """

    def should_abort(self, consecutive_failures: int) -> bool:
        """Return ``True`` if the batch should be aborted.

        Subclasses must override this method.
        """
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class AbortBatchAfter(BatchFailurePolicy):
    """Abort the batch after ``consecutive_failures`` consecutive failures.

    ``should_abort(n)`` returns ``True`` when ``n >= consecutive_failures``.
    """

    consecutive_failures: int

    def __post_init__(self) -> None:
        if self.consecutive_failures < 1:
            raise ValueError(f"consecutive_failures must be >= 1, got {self.consecutive_failures}")

    def should_abort(self, consecutive_failures: int) -> bool:
        return consecutive_failures >= self.consecutive_failures
