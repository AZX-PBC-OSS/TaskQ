"""Unit tests for retry data models (no PG required)."""

from datetime import datetime, timedelta

import pydantic
import pytest
from pydantic import TypeAdapter

from taskq.retry import (
    Fail,
    JobRetryState,
    Retry,
    RetryDecision,
    RetryPolicy,
    time_budget_as_interval,
)

# ── RetryPolicy defaults match ──────────────────────


def test_retry_policy_defaults() -> None:
    """RetryPolicy() defaults match exactly."""
    p = RetryPolicy()
    assert p.kind == "transient"
    assert p.max_attempts == 3
    assert p.time_budget is None
    assert p.backoff == "exponential"
    assert p.base == timedelta(seconds=5)
    assert p.cap == timedelta(hours=1)
    assert p.jitter == 0.2


# ── max_attempts >= 1 validator ─────────────────────────────────────


def test_max_attempts_zero_raises() -> None:
    """RetryPolicy(max_attempts=0) raises ValidationError with 'max_attempts'."""
    with pytest.raises(pydantic.ValidationError, match="max_attempts"):
        RetryPolicy(max_attempts=0)


# ── max_attempts=1 edge case (no retries allowed) ─────────────────────────


def test_max_attempts_one_is_valid() -> None:
    """max_attempts=1 is valid at construction (boundary — means no retries: first attempt is final)."""
    p = RetryPolicy(max_attempts=1)
    assert p.max_attempts == 1


# ── jitter in [0.0, 1.0] validator ──────────────────────────────────


def test_jitter_above_one_raises() -> None:
    """RetryPolicy(jitter=1.5) raises ValidationError with 'jitter'."""
    with pytest.raises(pydantic.ValidationError, match="jitter"):
        RetryPolicy(jitter=1.5)


# ── cap >= base cross-field validator ───────────────────────


def test_cap_less_than_base_raises() -> None:
    """RetryPolicy with cap < base raises ValidationError from model_validator."""
    with pytest.raises(pydantic.ValidationError, match="cap"):
        RetryPolicy(cap=timedelta(seconds=1), base=timedelta(seconds=5))


# ── JobRetryState projection ──────────────────────────────────────


def test_job_retry_state_projection() -> None:
    """JobRetryState projects from a JobRow-shaped tuple with all five fields."""
    now = datetime(2026, 1, 1)
    state = JobRetryState(
        attempt=3,
        max_attempts=5,
        retry_kind="transient",
        schedule_to_close=now + timedelta(hours=1),
        start_to_close=timedelta(hours=2),
    )
    assert state.attempt == 3
    assert state.max_attempts == 5
    assert state.retry_kind == "transient"
    assert state.schedule_to_close == now + timedelta(hours=1)
    assert state.start_to_close == timedelta(hours=2)


# ── Smart-mode union round-trip ────────────────────────────────────────────


def test_retry_decision_smart_mode_round_trip() -> None:
    """Retry and Fail both resolve via RetryDecision TypeAdapter smart-mode."""
    adapter: TypeAdapter[Retry | Fail] = TypeAdapter(RetryDecision)

    now = datetime(2026, 1, 1)
    retry = Retry(next_scheduled_at=now + timedelta(seconds=5))
    fail = Fail(error_class="ValueError", retryable=False)

    retry_out: Retry | Fail = adapter.validate_python(retry.model_dump())
    assert isinstance(retry_out, Retry)
    assert retry_out.next_scheduled_at == retry.next_scheduled_at

    fail_out: Retry | Fail = adapter.validate_python(fail.model_dump())
    assert isinstance(fail_out, Fail)
    assert fail_out.error_class == fail.error_class
    assert fail_out.retryable == fail.retryable


# ── BLK-1: Retry has no attempt_after field ────────────────────────────────


def test_retry_has_no_attempt_after() -> None:
    """BLK-1: Retry does not have an attempt_after field."""
    assert "attempt_after" not in Retry.model_fields


# ── time_budget_as_interval indefinite with time_budget ──────────


def test_time_budget_as_interval_indefinite() -> None:
    """time_budget_as_interval(RetryPolicy(kind='indefinite',
    time_budget=timedelta(hours=1))) → timedelta(hours=1)."""
    result = time_budget_as_interval(RetryPolicy(kind="indefinite", time_budget=timedelta(hours=1)))
    assert result == timedelta(hours=1)


# ── time_budget_as_interval transient → None ─────────────────────


def test_time_budget_as_interval_transient() -> None:
    """time_budget_as_interval(RetryPolicy(kind='transient',
    time_budget=timedelta(hours=1))) → None."""
    result = time_budget_as_interval(RetryPolicy(kind="transient", time_budget=timedelta(hours=1)))
    assert result is None


# ── time_budget_as_interval: indefinite with time_budget=None ────────────


def test_time_budget_as_interval_indefinite_none_budget() -> None:
    """Edge: kind='indefinite', time_budget=None → None."""
    result = time_budget_as_interval(RetryPolicy(kind="indefinite", time_budget=None))
    assert result is None
