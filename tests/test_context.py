"""Unit tests for production JobContext (no PG required)."""

import asyncio
from dataclasses import FrozenInstanceError
from uuid import UUID

import pytest
import structlog
from opentelemetry.trace import NonRecordingSpan, Span, SpanContext
from pydantic import BaseModel, ConfigDict

from taskq._ids import new_uuid
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context


class _Payload(BaseModel):
    """Minimal payload for context tests."""

    model_config = ConfigDict(frozen=True)
    key: str = "value"


def _make_enqueuer() -> SubJobEnqueuer:
    from datetime import UTC, datetime

    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    return SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=None,
        backend=backend,
        clock=clock,
    )


def _make_context(**overrides: object) -> JobContext[BaseModel]:
    _job_id = new_uuid()
    defaults: dict[str, object] = {
        "job_id": _job_id,
        "actor": "test_actor",
        "queue": "default",
        "attempt": 1,
        "worker_id": new_uuid(),
        "payload": _Payload(),
        "jobs": _make_enqueuer(),
        "log": bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=_job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    }
    return JobContext(**(defaults | overrides))  # type: ignore[arg-type] # Why: overrides are caller-controlled test values; the broad dict merge is intentional for test flexibility.


# ── Construction and field access ───────────────────────────────────────


def test_construction_populates_all_fields() -> None:
    ctx = _make_context()

    assert isinstance(ctx.job_id, UUID)
    assert ctx.actor == "test_actor"
    assert ctx.queue == "default"
    assert ctx.attempt == 1
    assert isinstance(ctx.worker_id, UUID)
    assert isinstance(ctx.payload, _Payload)
    assert isinstance(ctx.cancel_event, asyncio.Event)
    assert isinstance(ctx.jobs, SubJobEnqueuer)


# ── jobs field ────────────────────────────────────────────────────────────


def test_jobs_field_type_is_sub_job_enqueuer() -> None:
    ctx = _make_context()
    assert type(ctx.jobs) is SubJobEnqueuer


def test_jobs_field_is_required() -> None:
    """JobContext requires the jobs field — no default."""
    with pytest.raises(TypeError):
        JobContext(  # type: ignore[call-arg]
            job_id=new_uuid(),
            actor="test_actor",
            queue="default",
            attempt=1,
            worker_id=new_uuid(),
            payload=_Payload(),
            log=bind_job_context(
                structlog.get_logger("taskq.test"),
                job_id=new_uuid(),
                actor="test_actor",
                queue="default",
                attempt=1,
                identity_key=None,
                trace_id="",
            ),
        )


def test_frozen_dataclass_cannot_mutate_jobs() -> None:
    """JobContext is frozen — cannot reassign ctx.jobs."""
    ctx = _make_context()
    with pytest.raises(FrozenInstanceError):
        ctx.jobs = _make_enqueuer()  # type: ignore[misc] # Why: assigning to frozen dataclass field to assert FrozenInstanceError at runtime


def test_frozen_dataclass_unhashable_due_to_log() -> None:
    with pytest.raises(TypeError, match="unhashable"):
        hash(_make_context())


def test_frozen_dataclass_has_repr() -> None:
    ctx = _make_context()
    r = repr(ctx)
    assert "JobContext" in r
    assert "test_actor" in r


def test_default_factory_generates_distinct_events() -> None:
    ctx_a = _make_context()
    ctx_b = _make_context()
    assert ctx_a.cancel_event is not ctx_b.cancel_event


# ── cancellation_requested property ─────────────────────────────────────


def test_cancellation_not_requested_initially() -> None:
    ctx = _make_context()
    assert ctx.cancellation_requested is False


def test_cancellation_requested_after_set() -> None:
    ctx = _make_context()
    ctx.cancel_event.set()
    assert ctx.cancellation_requested is True


# ── check_cancelled method ──────────────────────────────────────────────


def test_check_cancelled_returns_none_when_not_requested() -> None:
    ctx = _make_context()
    result = ctx.check_cancelled()
    assert result is None


def test_check_cancelled_raises_cancelled_error_when_requested() -> None:
    ctx = _make_context()
    ctx.cancel_event.set()
    with pytest.raises(asyncio.CancelledError):
        ctx.check_cancelled()


async def test_await_cancel_event_after_set_returns_immediately() -> None:
    ctx = _make_context()
    ctx.cancel_event.set()
    await ctx.cancel_event.wait()


# ── Frozen-ness ─────────────────────────────────────────────────────────


def test_setattr_raises_frozen_instance_error() -> None:
    ctx = _make_context()
    with pytest.raises(FrozenInstanceError):
        ctx.attempt = 2  # pyright: ignore[reportAttributeAccessIssue] — Why: deliberately assigning to frozen field to verify FrozenInstanceError at runtime


# ── span attribute (,) ──────────────────────────────────────────


def test_span_defaults_to_none() -> None:
    """JobContext.span defaults to None when not provided."""
    ctx = _make_context()
    assert ctx.span is None


def test_span_accepts_span_object() -> None:
    """JobContext accepts a Span at construction time."""
    noop_span: Span = NonRecordingSpan(SpanContext(0, 0, is_remote=False))
    ctx = _make_context(span=noop_span)
    assert ctx.span is noop_span


def test_span_preserves_frozen() -> None:
    """JobContext remains frozen when span is provided."""
    noop_span: Span = NonRecordingSpan(SpanContext(0, 0, is_remote=False))
    ctx = _make_context(span=noop_span)
    with pytest.raises(FrozenInstanceError):
        ctx.span = None  # type: ignore[misc] # Why: assigning to frozen dataclass field to assert FrozenInstanceError at runtime


def test_span_none_context_works() -> None:
    """JobContext with span=None is usable."""
    ctx = _make_context(span=None)
    assert ctx.span is None
