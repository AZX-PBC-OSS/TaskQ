"""Unit tests for batch failure policy types (no PG required)."""

import dataclasses

import pytest

from taskq.batch_policy import AbortBatchAfter, BatchFailurePolicy

# ── should_abort below / at / above threshold ─────────────────────


def test_should_abort_below_threshold() -> None:
    """Below threshold: should_abort returns False."""
    policy = AbortBatchAfter(consecutive_failures=3)
    assert policy.should_abort(1) is False
    assert policy.should_abort(2) is False


def test_should_abort_at_threshold() -> None:
    """At exactly the threshold: should_abort returns True."""
    policy = AbortBatchAfter(consecutive_failures=3)
    assert policy.should_abort(3) is True


def test_should_abort_above_threshold() -> None:
    """Above threshold: should_abort returns True."""
    policy = AbortBatchAfter(consecutive_failures=3)
    assert policy.should_abort(4) is True
    assert policy.should_abort(100) is True


# ── zero / negative threshold raises ValueError ───────────────────


def test_zero_threshold_raises() -> None:
    """AbortBatchAfter(consecutive_failures=0) raises ValueError."""
    with pytest.raises(ValueError, match="consecutive_failures"):
        AbortBatchAfter(consecutive_failures=0)


def test_negative_threshold_raises() -> None:
    """AbortBatchAfter(consecutive_failures=-1) raises ValueError."""
    with pytest.raises(ValueError, match="consecutive_failures"):
        AbortBatchAfter(consecutive_failures=-1)


# ── frozen dataclass immutability ─────────────────────────────────


def test_frozen_immutability() -> None:
    """Mutating a frozen field raises dataclasses.FrozenInstanceError."""
    policy = AbortBatchAfter(consecutive_failures=3)
    with pytest.raises(dataclasses.FrozenInstanceError):
        policy.consecutive_failures = 5  # type: ignore[misc]


# ── isinstance check ──────────────────────────────────────────────


def test_isinstance_base_class() -> None:
    """AbortBatchAfter is an instance of BatchFailurePolicy."""
    policy = AbortBatchAfter(consecutive_failures=3)
    assert isinstance(policy, BatchFailurePolicy)


# ── base class should_abort raises NotImplementedError ────────────


def test_base_class_should_abort_raises() -> None:
    """BatchFailurePolicy.should_abort raises NotImplementedError."""
    policy = BatchFailurePolicy()
    with pytest.raises(NotImplementedError):
        policy.should_abort(1)


# ── slotted: no __dict__ ──────────────────────────────────────────


def test_slotted_no_dict() -> None:
    """Slotted dataclass instances do not carry a __dict__."""
    policy = AbortBatchAfter(consecutive_failures=3)
    assert not hasattr(policy, "__dict__")
