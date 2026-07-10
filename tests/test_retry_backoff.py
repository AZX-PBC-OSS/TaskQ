"""Unit tests for compute_backoff (no PG required)."""

import random
from datetime import timedelta
from typing import Literal

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from taskq.retry import RetryPolicy, compute_backoff

# ── global max_retry_backoff ceiling overrides per-actor cap ─────────


def test_global_ceiling_overrides_per_actor_cap() -> None:
    """max_retry_backoff=24h caps a policy with cap=365d at high attempt numbers.

    Verifies the Dramatiq-inspired global ceiling ():
    ``effective_cap = min(policy.cap, max_retry_backoff)`` so a misconfigured
    actor cannot strand jobs for an unreasonably long time.
    """
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=5),
        cap=timedelta(days=365),
        jitter=0.0,
    )
    ceiling = timedelta(hours=24)

    # Try a range of high attempt numbers that would exceed 24h without the ceiling.
    for attempt in (1, 5, 10, 20, 50, 100):
        result = compute_backoff(policy, attempt=attempt, max_retry_backoff=ceiling)
        assert result <= ceiling, f"attempt={attempt}: {result} exceeds max_retry_backoff={ceiling}"


# ── exponential, deterministic with seeded RNG ───────────────────────


def test_exponential_deterministic_seeded() -> None:
    """exponential backoff with jitter=0.0 and seeded RNG returns timedelta(seconds=20)."""
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=5),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    rng = random.Random(42)
    result_a = compute_backoff(policy, attempt=3, rng=rng)
    result_b = compute_backoff(policy, attempt=3, rng=rng)
    expected = timedelta(seconds=20)
    assert result_a == expected
    assert result_b == expected


# ── linear, jitter=0 ────────────────────────────────────────────────


def test_linear_jitter_zero() -> None:
    """linear backoff with jitter=0.0, base=5s, attempt=2 → timedelta(seconds=10)."""
    policy = RetryPolicy(
        backoff="linear",
        base=timedelta(seconds=5),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    assert compute_backoff(policy, attempt=2) == timedelta(seconds=10)


# ── fixed, jitter=0, large attempt ──────────────────────────────────


def test_fixed_jitter_zero_large_attempt() -> None:
    """fixed backoff with jitter=0.0, base=5s, attempt=99 → timedelta(seconds=5)."""
    policy = RetryPolicy(
        backoff="fixed",
        base=timedelta(seconds=5),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    assert compute_backoff(policy, attempt=99) == timedelta(seconds=5)


# ── jitter bounds with seed ─────────────────────────────────────────


def test_jitter_bounds_seeded() -> None:
    """100 calls with jitter=0.2, base=10s, exponential, attempt=1, seeded RNG → all in [8s, 12s]."""
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=10),
        cap=timedelta(hours=1),
        jitter=0.2,
    )
    rng = random.Random(123)
    lower = timedelta(seconds=8)
    upper = timedelta(seconds=12)
    for _ in range(100):
        result = compute_backoff(policy, attempt=1, rng=rng)
        assert lower <= result <= upper


# ── multiplicative not additive ─────────────────────────────────────


def test_multiplicative_not_additive() -> None:
    """100 samples with jitter=0.5, base=10s, attempt=1; delay/raw ratios in [0.5, 1.5]."""
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=10),
        cap=timedelta(hours=1),
        jitter=0.5,
    )
    rng = random.Random(456)
    raw = 10.0
    for _ in range(100):
        result = compute_backoff(policy, attempt=1, rng=rng)
        ratio = result.total_seconds() / raw
        assert 0.5 <= ratio <= 1.5, f"ratio {ratio} outside [0.5, 1.5]"


# ── Hypothesis property test ─────────────────────────────────────────

_BACKOFF_KINDS = st.sampled_from(["exponential", "linear", "fixed"])
BackoffKind = Literal["exponential", "linear", "fixed"]


@settings(max_examples=200)
@given(
    kind=_BACKOFF_KINDS,
    base_s=st.floats(min_value=1.0, max_value=3600.0, allow_nan=False, allow_infinity=False),
    cap_s=st.floats(min_value=1.0, max_value=86400.0, allow_nan=False, allow_infinity=False),
    jitter=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
    attempt=st.integers(min_value=1, max_value=15),
    seed=st.integers(min_value=0, max_value=2**63 - 1),
)
def test_backoff_properties(
    kind: BackoffKind,
    base_s: float,
    cap_s: float,
    jitter: float,
    attempt: int,
    seed: int,
) -> None:
    """for all valid inputs, delay bounded by cap, non-negative, and mean non-decreasing in attempt."""
    assume(cap_s >= base_s)

    policy = RetryPolicy(
        backoff=kind,
        base=timedelta(seconds=base_s),
        cap=timedelta(seconds=cap_s),
        jitter=jitter,
    )
    cap_td = timedelta(seconds=cap_s)

    for _ in range(10):
        result = compute_backoff(policy, attempt=attempt, rng=random.Random(seed))
        assert timedelta(0) <= result <= cap_td

    n_runs = 100
    means: list[float] = []
    for a in range(1, 6):
        total = 0.0
        for i in range(n_runs):
            r = compute_backoff(policy, attempt=a, rng=random.Random(seed + i))
            total += r.total_seconds()
        means.append(total / n_runs)

    if kind == "fixed":
        for i in range(len(means) - 1):
            assert abs(means[i] - means[i + 1]) < base_s * 0.5
    else:
        for i in range(len(means) - 1):
            assert means[i] <= means[i + 1] + base_s * 0.1


# ── Backoff overflow: high attempt counts are capped, not panicking ──────────


def test_exponential_high_attempt_capped_not_overflow() -> None:
    """Large exponents (2^100) are clamped to effective_cap, not floats that overflow.

    Python integer exponentiation is arbitrary-precision; the min(cap_s,...)
    guard fires before the huge integer reaches timedelta(). This is an
    explicit regression guard for the backoff-overflow edge case (Focus K).
    """
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=5),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    result = compute_backoff(policy, attempt=100)
    assert result == timedelta(hours=1), f"expected 1h cap, got {result}"


# ── B-TG-13: jitter=0.0 special case (raw timedelta returned, not zero) ─────


def test_jitter_zero_returns_raw_not_zero_exponential() -> None:
    """B-TG-13: with jitter=0.0, compute_backoff returns the raw delay
    (timedelta(seconds=10) for attempt=1, base=10s, exponential), not zero.

    The multiplicative-symmetric formula: delay = raw * uniform(1-0, 1+0) = raw*1.0.
    This guards against a regression where jitter=0 collapses to zero.
    """
    policy = RetryPolicy(
        backoff="exponential",
        base=timedelta(seconds=10),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    result = compute_backoff(policy, attempt=1)
    assert result == timedelta(seconds=10), f"expected 10s, got {result}"


def test_jitter_zero_returns_raw_not_zero_linear() -> None:
    """B-TG-13 linear variant: with jitter=0.0, compute_backoff returns base*attempt."""
    policy = RetryPolicy(
        backoff="linear",
        base=timedelta(seconds=5),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    result = compute_backoff(policy, attempt=3)
    assert result == timedelta(seconds=15), f"expected 15s, got {result}"


def test_jitter_zero_returns_raw_not_zero_fixed() -> None:
    """B-TG-13 fixed variant: with jitter=0.0, compute_backoff returns base."""
    policy = RetryPolicy(
        backoff="fixed",
        base=timedelta(seconds=7),
        cap=timedelta(hours=1),
        jitter=0.0,
    )
    result = compute_backoff(policy, attempt=100)
    assert result == timedelta(seconds=7), f"expected 7s, got {result}"


@given(
    base_s=st.floats(min_value=1.0, max_value=3600.0, allow_nan=False, allow_infinity=False),
    attempt=st.integers(min_value=1, max_value=15),
    kind=_BACKOFF_KINDS,
)
def test_jitter_zero_property_never_returns_zero_when_base_positive(
    base_s: float,
    attempt: int,
    kind: BackoffKind,
) -> None:
    """B-TG-13 property: for all positive base values and jitter=0.0,
    compute_backoff must return > 0. The result should equal the raw delay
    formula (up to floating-point precision).
    """
    policy = RetryPolicy(
        backoff=kind,
        base=timedelta(seconds=base_s),
        cap=timedelta(seconds=max(base_s, 86400.0)),
        jitter=0.0,
    )
    result = compute_backoff(policy, attempt=attempt)
    assert result.total_seconds() > 0.0, (
        f"jitter=0.0 with base={base_s}s returned zero for {kind} at attempt={attempt}"
    )
