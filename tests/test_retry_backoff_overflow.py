"""Regression tests: exponential backoff must saturate, never overflow.

``kind="indefinite"`` policies are documented to retry forever, so the
attempt counter is unbounded.  ``base * 2 ** (attempt - 1)`` computed
without clamping raises ``OverflowError`` once the exponent exceeds the
float range (attempt >= 1025), and that exception escapes through
``RetryClassifier.classify`` into the consumer's failure dispatch.
"""

import random
from datetime import timedelta

import pytest

from taskq.retry import Fail, Retry, RetryClassifier, RetryPolicy, compute_backoff

_NO_JITTER = RetryPolicy(
    kind="indefinite",
    backoff="exponential",
    base=timedelta(seconds=5),
    cap=timedelta(hours=1),
    jitter=0.0,
)


@pytest.mark.parametrize("attempt", [1025, 2048, 100_000])
def test_exponential_backoff_saturates_at_cap_for_huge_attempts(attempt: int) -> None:
    """A far-future attempt returns the cap instead of raising OverflowError."""
    assert compute_backoff(_NO_JITTER, attempt=attempt) == timedelta(hours=1)


def test_indefinite_classification_survives_huge_attempt() -> None:
    """The classifier still returns a Retry decision at attempt 1025."""
    decision = RetryClassifier.classify(
        _NO_JITTER,
        (),
        RuntimeError("boom"),
        attempt=1025,
    )
    assert isinstance(decision, Retry)
    assert not isinstance(decision, Fail)
    assert decision.retry_delay == timedelta(hours=1)


def test_zero_base_saturates_without_overflow() -> None:
    """base=0 must not overflow either — the delay is simply zero."""
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(0),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    assert compute_backoff(policy, attempt=5000) == timedelta(0)


def test_backoff_curve_below_overflow_threshold_is_unchanged() -> None:
    """Clamping must not alter any attempt whose exponent is representable."""
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=1),
        cap=timedelta(days=365),
        jitter=0.0,
    )
    ceiling = timedelta(days=365)
    for attempt in range(1, 40):
        expected = min(365 * 86400.0, 1.0 * 2 ** (attempt - 1))
        got = compute_backoff(policy, attempt=attempt, max_retry_backoff=ceiling)
        assert got == timedelta(seconds=expected), f"attempt={attempt}"


def test_jitter_still_applied_at_saturation() -> None:
    """Saturated delays keep the jitter multiplier (no special-cased branch)."""
    policy = RetryPolicy(
        kind="indefinite",
        backoff="exponential",
        base=timedelta(seconds=5),
        cap=timedelta(hours=1),
        jitter=0.5,
    )
    rng = random.Random(1234)
    delay = compute_backoff(policy, attempt=5000, rng=rng)
    assert timedelta(minutes=30) <= delay <= timedelta(hours=1)
