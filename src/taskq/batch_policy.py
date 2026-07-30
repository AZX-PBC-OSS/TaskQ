"""Batch failure policy types.

A :class:`BatchFailurePolicy` decides whether a batch should be aborted
after observing a run of consecutive job failures.  The base class is
abstract; :class:`AbortBatchAfter` is the built-in concrete policy that
aborts once the consecutive-failure count reaches a configured threshold.

Custom policies should subclass :class:`BatchFailurePolicy`, set
``failure_threshold`` in ``__post_init__``, and override
:meth:`should_abort`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = [
    "AbortBatchAfter",
    "BatchFailurePolicy",
]


@dataclass(frozen=True, slots=True)
class BatchFailurePolicy:
    """Abstract base class for batch failure policies.

    Subclasses implement :meth:`should_abort` to decide whether a batch
    should be aborted given the current count of consecutive failures.

    ``failure_threshold`` is the consecutive-failure count at which the
    batch should be aborted.  ``None`` disables abort (the default on the
    base class).  :class:`AbortBatchAfter` sets this to its
    ``consecutive_failures`` value in ``__post_init__``; custom policies
    should set it to their computed threshold.  The client reads
    ``failure_policy.failure_threshold`` polymorphically — no
    ``isinstance`` check is needed.
    """

    failure_threshold: int | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        raise TypeError("BatchFailurePolicy is abstract; use AbortBatchAfter or a custom subclass")

    def should_abort(self, consecutive_failures: int) -> bool:
        """Return ``True`` if the batch should be aborted.

        Subclasses must override this method.
        """
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class AbortBatchAfter(BatchFailurePolicy):
    """Abort the batch after ``consecutive_failures`` consecutive failures.

    ``should_abort(n)`` returns ``True`` when ``n >= consecutive_failures``.
    ``failure_threshold`` is set to ``consecutive_failures`` in
    ``__post_init__`` so the client can read it polymorphically via
    ``policy.failure_threshold``.

    Running jobs are NOT cancelled by the abort — only pending and
    scheduled jobs are cancelled.  Running jobs continue to completion.
    This matches the post-terminal-write hook design: the hook runs after
    the terminal write, so a job that was dispatched before the abort
    triggered will run to completion.
    """

    consecutive_failures: int

    def __post_init__(self) -> None:
        if self.consecutive_failures < 1:
            raise ValueError(f"consecutive_failures must be >= 1, got {self.consecutive_failures}")
        object.__setattr__(self, "failure_threshold", self.consecutive_failures)

    def should_abort(self, consecutive_failures: int) -> bool:
        return consecutive_failures >= self.consecutive_failures
