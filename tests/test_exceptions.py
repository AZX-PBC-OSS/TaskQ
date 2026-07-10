"""Unit tests for control-flow exception constructor validation (no PG required)."""

from datetime import timedelta
from uuid import UUID

import pytest

from taskq._di.scope import Scope
from taskq.constants import DEFAULT_RESERVATION_BACKOFF
from taskq.exceptions import (
    ActorConfigDriftError,
    ActorConfigDriftList,
    BackpressureError,
    DependencyCycle,
    DIError,
    MaxPendingExceededError,
    MissingProvider,
    ReservationUnavailable,
    RetryAfter,
    ScopeViolation,
    SingletonCollisionError,
    Snooze,
    TaskQError,
)

# ── Snooze delay validation ────────────────────────────────


def test_snooze_negative_delay_raises_value_error() -> None:
    """Snooze rejects negative delay at construction."""
    with pytest.raises(ValueError, match=r"delay must be non-negative"):
        Snooze(timedelta(seconds=-1))


def test_snooze_zero_delay_accepted() -> None:
    """Snooze accepts zero delay (immediate retry)."""
    exc = Snooze(timedelta(0))
    assert exc.delay == timedelta(0)


# ── RetryAfter delay validation ─────────────────────────────────────────────


def test_retry_after_negative_delay_raises_value_error() -> None:
    """RetryAfter rejects negative delay at construction."""
    with pytest.raises(ValueError, match=r"delay must be non-negative"):
        RetryAfter(timedelta(seconds=-1))


def test_retry_after_zero_delay_accepted() -> None:
    """RetryAfter accepts zero delay (immediate retry)."""
    exc = RetryAfter(timedelta(0))
    assert exc.delay == timedelta(0)


# ── ReservationUnavailable retry_after validation ───────────────────────────


def test_reservation_unavailable_zero_retry_after_accepted() -> None:
    """ReservationUnavailable accepts zero retry_after (immediate retry)."""
    exc = ReservationUnavailable("bucket", timedelta(0))
    assert exc.retry_after == timedelta(0)


# ── retry_after coalesce contract (is None, NOT truthiness) ────────


def _coalesce(retry_after_in: timedelta | None) -> timedelta:
    return retry_after_in if retry_after_in is not None else DEFAULT_RESERVATION_BACKOFF


def test_coalesce_preserves_timedelta_zero() -> None:
    """Coalesce uses `is None`, not truthiness — `timedelta(0)` MUST pass through unchanged."""
    coerced = _coalesce(timedelta(0))
    assert isinstance(coerced, timedelta)
    assert coerced.total_seconds() >= 0
    assert coerced == timedelta(0)
    exc = ReservationUnavailable("b", retry_after=coerced)
    assert exc.retry_after == timedelta(0)


def test_coalesce_substitutes_default_when_none() -> None:
    """When retry_after is None, DEFAULT_RESERVATION_BACKOFF (5 s) is substituted."""
    coerced = _coalesce(None)
    assert isinstance(coerced, timedelta)
    assert coerced.total_seconds() >= 0
    assert coerced == DEFAULT_RESERVATION_BACKOFF
    assert coerced.total_seconds() == 5.0
    exc = ReservationUnavailable("b", retry_after=coerced)
    assert exc.retry_after == DEFAULT_RESERVATION_BACKOFF


def test_reservation_unavailable_negative_retry_after_raises_value_error() -> None:
    """ReservationUnavailable rejects negative retry_after at construction."""
    with pytest.raises(ValueError, match=r"retry_after must be non-negative"):
        ReservationUnavailable("bucket", timedelta(seconds=-1))


# ── public import surface ────────────────────────────────────────────


def test_exceptions_importable_from_taskq() -> None:
    """Control-flow exceptions are importable from taskq top-level."""
    import taskq

    assert taskq.Snooze is Snooze
    assert taskq.RetryAfter is RetryAfter
    assert taskq.ReservationUnavailable is ReservationUnavailable
    assert taskq.BackpressureError is BackpressureError
    assert taskq.SingletonCollisionError is SingletonCollisionError


# ── ActorConfigDriftError construction and message shape ─────────────────────


def test_drift_error_max_concurrent() -> None:
    """str() contains actor, field, registered, and stored values."""
    exc = ActorConfigDriftError(
        actor="my_actor",
        field="max_concurrent",
        registered=5,
        stored=10,
    )
    msg = str(exc)
    assert "my_actor" in msg
    assert "max_concurrent" in msg
    assert "5" in msg
    assert "10" in msg


def test_drift_error_queue() -> None:
    """str() contains the queue field and string registered/stored values."""
    exc = ActorConfigDriftError(
        actor="my_actor",
        field="queue",
        registered="high_priority",
        stored="default",
    )
    msg = str(exc)
    assert "queue" in msg
    assert "high_priority" in msg
    assert "default" in msg


def test_drift_error_metadata() -> None:
    """str() contains the metadata field and dict registered/stored values."""
    exc = ActorConfigDriftError(
        actor="my_actor",
        field="metadata",
        registered={"key1": "val1"},
        stored={"key2": "val2"},
    )
    msg = str(exc)
    assert "metadata" in msg
    assert "key1" in msg
    assert "val1" in msg
    assert "key2" in msg
    assert "val2" in msg


def test_drift_error_none_values() -> None:
    """str() renders None registered/stored as None."""
    exc = ActorConfigDriftError(
        actor="my_actor",
        field="max_concurrent",
        registered=None,
        stored=None,
    )
    msg = str(exc)
    assert "None" in msg


# ── ActorConfigDriftList construction and message shape ──────────────────────


def test_drift_list_zero_drifts() -> None:
    """str() shows zero count and no drift lines."""
    exc = ActorConfigDriftList(())
    msg = str(exc)
    assert "0 actor(s) have config drift:" in msg


def test_drift_list_one_drift() -> None:
    """str() shows count and one indented drift line."""
    drift = ActorConfigDriftError(actor="a", field="queue", registered="q1", stored="q2")
    exc = ActorConfigDriftList((drift,))
    msg = str(exc)
    assert "1 actor(s) have config drift:" in msg
    assert "  ActorConfigDrift: actor=a" in msg


def test_drift_list_three_drifts() -> None:
    """str() shows three indented drift lines."""
    d1 = ActorConfigDriftError(actor="a1", field="max_concurrent", registered=1, stored=2)
    d2 = ActorConfigDriftError(actor="a2", field="queue", registered="q1", stored="q2")
    d3 = ActorConfigDriftError(actor="a3", field="metadata", registered={"k": "v"}, stored=None)
    exc = ActorConfigDriftList((d1, d2, d3))
    msg = str(exc)
    assert "3 actor(s) have config drift:" in msg
    assert "  ActorConfigDrift: actor=a1" in msg
    assert "  ActorConfigDrift: actor=a2" in msg
    assert "  ActorConfigDrift: actor=a3" in msg


# ── isinstance invariants ────────────────────────────────────────────────────


def test_drift_error_isinstance_taskqerror() -> None:
    """ActorConfigDriftError is a TaskQError subclass."""
    exc = ActorConfigDriftError(actor="a", field="max_concurrent", registered=1, stored=2)
    assert isinstance(exc, TaskQError)


def test_drift_list_isinstance_taskqerror() -> None:
    """ActorConfigDriftList is a TaskQError subclass."""
    exc = ActorConfigDriftList(())
    assert isinstance(exc, TaskQError)


# ── drifts tuple invariant ───────────────────────────────────────────────────


def test_drift_list_drifts_is_tuple() -> None:
    """The drifts field is a tuple, not a list."""
    d1 = ActorConfigDriftError(actor="a", field="max_concurrent", registered=1, stored=2)
    exc = ActorConfigDriftList((d1,))
    assert type(exc.drifts) is tuple


def test_drift_list_drifts_is_iterable() -> None:
    """The drifts tuple is iterable for diagnostic logging."""
    d1 = ActorConfigDriftError(actor="a1", field="queue", registered="q1", stored="q2")
    d2 = ActorConfigDriftError(actor="a2", field="max_concurrent", registered=3, stored=4)
    exc = ActorConfigDriftList((d1, d2))
    items = list(exc.drifts)
    assert len(items) == 2
    assert items[0] is d1
    assert items[1] is d2


# ── BackpressureError constructor ─────────────────────────────────────


def test_backpressure_error_defaults() -> None:
    """BackpressureError(actor="x") sets actor, pending=0, max_pending=None."""
    exc = BackpressureError(actor="x")
    assert exc.actor == "x"
    assert exc.pending == 0
    assert exc.max_pending is None
    assert "actor=x" in str(exc)


def test_backpressure_error_full_args() -> None:
    """BackpressureError(actor="x", pending=3, max_pending=5) stores all attributes."""
    exc = BackpressureError(actor="x", pending=3, max_pending=5)
    assert exc.actor == "x"
    assert exc.pending == 3
    assert exc.max_pending == 5
    assert "pending=3" in str(exc)
    assert "max_pending=5" in str(exc)


# ── SingletonCollisionError construction and hierarchy ────────────────────────


def test_singleton_collision_defaults() -> None:
    """SingletonCollisionError(actor="a") has blocking_job_id=None, retry_after=None."""
    exc = SingletonCollisionError(actor="a")
    assert exc.actor == "a"
    assert exc.blocking_job_id is None
    assert exc.retry_after is None


def test_singleton_collision_is_subclass_of_backpressure_error() -> None:
    """SingletonCollisionError caught as BackpressureError."""
    exc = SingletonCollisionError(actor="a")
    assert isinstance(exc, BackpressureError)
    assert isinstance(exc, TaskQError)


def test_singleton_collision_full_args() -> None:
    """SingletonCollisionError with blocking_job_id and retry_after stores verbatim."""
    jid = UUID("12345678-1234-5678-1234-567812345678")
    exc = SingletonCollisionError(
        actor="a",
        blocking_job_id=jid,
        retry_after=timedelta(seconds=30),
    )
    assert exc.actor == "a"
    assert exc.blocking_job_id == jid
    assert exc.retry_after == timedelta(seconds=30)


# ── MaxPendingExceededError construction and hierarchy ──────────────────────


def test_max_pending_constructor_stores_all_attributes() -> None:
    """MaxPendingExceededError stores actor, current_count, max_pending, and pending."""
    exc = MaxPendingExceededError("my_actor", current_count=10, max_pending=10)
    assert exc.actor == "my_actor"
    assert exc.current_count == 10
    assert exc.max_pending == 10
    assert exc.pending == 10


def test_max_pending_is_subclass_of_backpressure_error() -> None:
    """MaxPendingExceededError caught as BackpressureError (LSP compat)."""
    exc = MaxPendingExceededError("my_actor", 5, 20)
    assert isinstance(exc, BackpressureError)
    assert isinstance(exc, TaskQError)


def test_max_pending_importable_from_taskq() -> None:
    """MaxPendingExceededError is importable from taskq (top-level re-export)."""
    import taskq

    assert taskq.MaxPendingExceededError is MaxPendingExceededError


def test_max_pending_importable_from_taskq_exceptions() -> None:
    """MaxPendingExceededError is importable from taskq.exceptions."""
    from taskq.exceptions import MaxPendingExceededError as DirectImport

    assert DirectImport is MaxPendingExceededError


def test_max_pending_pending_matches_current_count() -> None:
    """BackpressureError.pending equals the current_count passed (LSP compat)."""
    exc = MaxPendingExceededError("actor", current_count=42, max_pending=100)
    assert exc.pending == exc.current_count
    assert exc.pending == 42


def test_max_pending_with_zero_current_count() -> None:
    """MaxPendingExceededError handles zero current_count."""
    exc = MaxPendingExceededError("actor", current_count=0, max_pending=5)
    assert exc.current_count == 0
    assert exc.pending == 0
    assert exc.max_pending == 5


# ── DIError exception surface ────────────────────────────────────────────────


def test_di_error_importable_from_taskq() -> None:
    """DIError is importable from the taskq top-level package."""
    import taskq

    assert taskq.DIError is DIError


def test_di_error_is_subclass_of_taskq_error() -> None:
    """DIError inherits from TaskQError."""
    assert issubclass(DIError, TaskQError)


def test_di_error_message_in_str() -> None:
    """Raising DIError with a message produces an exception whose str() contains it."""
    msg = "multiple Scope markers on parameter 'db'"
    exc = DIError(msg)
    assert msg in str(exc)


def test_di_error_caught_as_taskq_error() -> None:
    """try/except TaskQError catches a raised DIError (hierarchy contract)."""
    with pytest.raises(TaskQError):
        raise DIError("unresolvable forward reference 'DbConn'")


def test_di_error_not_subclass_of_di_peers() -> None:
    """DIError is a peer of MissingProvider/ScopeViolation/DependencyCycle, not a subclass."""
    assert not issubclass(DIError, MissingProvider)
    assert not issubclass(DIError, ScopeViolation)
    assert not issubclass(DIError, DependencyCycle)


# ── ScopeViolation construction and message shape ──────────────────────────────


def test_scope_violation_construction() -> None:
    """ScopeViolation stores all attributes and message contains scope names and type strings."""
    exc = ScopeViolation(
        from_scope=Scope.LOOP,
        to_scope=Scope.TRANSIENT,
        type_name="MyType",
        dependent="my_actor",
    )
    assert exc.from_scope is Scope.LOOP
    assert exc.to_scope is Scope.TRANSIENT
    assert exc.type_name == "MyType"
    assert exc.dependent == "my_actor"
    msg = str(exc)
    assert "LOOP" in msg
    assert "TRANSIENT" in msg
    assert "MyType" in msg
    assert "my_actor" in msg


def test_scope_violation_isinstance_taskqerror() -> None:
    """ScopeViolation is a TaskQError subclass."""
    exc = ScopeViolation(
        from_scope=Scope.LOOP,
        to_scope=Scope.TRANSIENT,
        type_name="X",
        dependent="Y",
    )
    assert isinstance(exc, TaskQError)


# ── DependencyCycle construction and message shape ─────────────────────────────


def test_dependency_cycle_construction() -> None:
    """DependencyCycle stores cycle_path and message uses ' -> ' joining."""
    exc = DependencyCycle(["A", "B", "A"])
    assert exc.cycle_path == ["A", "B", "A"]
    msg = str(exc)
    assert "A -> B -> A" in msg


def test_dependency_cycle_isinstance_taskqerror() -> None:
    """DependencyCycle is a TaskQError subclass."""
    exc = DependencyCycle(["A", "B", "A"])
    assert isinstance(exc, TaskQError)


def test_dependency_cycle_defensive_copy() -> None:
    """Mutating the original list does not affect the exception's cycle_path."""
    original = ["A", "B", "A"]
    exc = DependencyCycle(original)
    original.append("C")
    assert exc.cycle_path == ["A", "B", "A"]


def test_dependency_cycle_rejects_empty_path() -> None:
    """cycle_path=[] raises ValueError."""
    with pytest.raises(ValueError, match=r"cycle_path must contain at least 2 entries"):
        DependencyCycle([])


def test_dependency_cycle_rejects_single_entry_path() -> None:
    """cycle_path=['A'] raises ValueError."""
    with pytest.raises(ValueError, match=r"cycle_path must contain at least 2 entries"):
        DependencyCycle(["A"])


def test_dependency_cycle_self_loop_accepted() -> None:
    """cycle_path=['A', 'A'] is accepted (self-dependency in repeat-first-and-last form)."""
    exc = DependencyCycle(["A", "A"])
    assert exc.cycle_path == ["A", "A"]
    assert "A -> A" in str(exc)


# ── MissingProvider construction and message shape ────────────────────────────


def test_missing_provider_construction() -> None:
    """MissingProvider stores attributes and message contains both strings."""
    exc = MissingProvider(type_name="asyncpg.Connection", required_by="my_actor")
    assert exc.type_name == "asyncpg.Connection"
    assert exc.required_by == "my_actor"
    msg = str(exc)
    assert "asyncpg.Connection" in msg
    assert "my_actor" in msg


def test_missing_provider_isinstance_taskqerror() -> None:
    """MissingProvider is a TaskQError subclass."""
    exc = MissingProvider(type_name="X", required_by="Y")
    assert isinstance(exc, TaskQError)
