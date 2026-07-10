"""Negative tests for TokenBucket constructor validation (..)."""

from datetime import timedelta

import pytest

from taskq.ratelimit import TokenBucket

# ── capacity=0 → ValueError ────────────────────────────────────


def test_capacity_zero_raises_value_error() -> None:
    """capacity=0 raises ValueError."""
    with pytest.raises(ValueError, match=r"capacity must be > 0, got 0"):
        TokenBucket(name="b", capacity=0, refill_per_second=10, backend="memory")


# ── capacity=-1 → ValueError ──────────────────────────────────


def test_capacity_negative_raises_value_error() -> None:
    """capacity=-1 raises ValueError."""
    with pytest.raises(ValueError, match=r"capacity must be > 0, got -1"):
        TokenBucket(name="b", capacity=-1, refill_per_second=10, backend="memory")


# ── refill_per_second=-0.01 → ValueError ──────────────────────


def test_refill_negative_raises_value_error() -> None:
    """refill_per_second=-0.01 raises ValueError."""
    with pytest.raises(ValueError, match=r"refill_per_second must be >= 0, got -0.01"):
        TokenBucket(name="b", capacity=100, refill_per_second=-0.01, backend="memory")


# ── refill_per_second=0 → no error; ttl defaults to 86400s ────


def test_refill_zero_no_error_ttl_86400() -> None:
    """refill_per_second=0 is valid; ttl defaults to 86400s."""
    tb = TokenBucket(name="fixed", capacity=50, refill_per_second=0, backend="memory")
    assert tb.ttl == timedelta(seconds=86400)
