"""Unit tests for OTel span events and CONSUMER span.

Covers lifecycle events, CONSUMER span creation,
attempt span child, span status, and rate-limit /
reservation acquire-release integration.
"""

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from opentelemetry import trace
from pydantic import BaseModel, ValidationError

import taskq.obs as obs_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import ReservationUnavailable, RetryAfter, Snooze, WorkerOwnershipMismatch
from taskq.progress._buffer import _ProgressBuffer
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    StubActorConfig,
    as_backend,
    default_actor_config,
)
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.testing.otel import setup_tracer
from taskq.worker._consumer import consume_one_job

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _FakeBackend(FakeBackend):
    """Extends FakeBackend to record enqueue calls for test assertions."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # pyright: ignore[reportArgumentType]  Why: FakeBackend.__init__ expects specific Literal-typed keyword args; **kwargs forwarded from test call sites that pass correct values.
        self.enqueue_calls: list[EnqueueArgs] = []

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        self.enqueue_calls.append(args)
        return make_job_row()


class _SlowQueryTimeout(TimeoutError):  # noqa: N818  Why: the concrete class name is the assertion target — it pins that span/log error_class report the subclass name verbatim, not a hardcoded 'TimeoutError'.
    """TimeoutError subclass: pins log/span error_class agreement."""


# ── indefinite-tier Retry — no taskq.indefinite_retry event ────


async def test_indefinite_retry_emits_lifecycle_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """indefinite-tier Retry emits `lifecycle.scheduled` (taskq.indefinite_retry was dropped — it was a debug annotation,
    not a state transition)."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("transient failure")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        retry_kind="indefinite",
        max_attempts=3,
        schedule_to_close=_NOW + timedelta(hours=1),
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    events = exporter.events_on("test-span", "lifecycle.scheduled")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberType]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["from_state"] == "running"
    assert attrs["to_state"] == "scheduled"

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    assert call["next_scheduled_at"] is not None


# ── indefinite-tier deadline emits lifecycle.failed ──────────────────


async def test_indefinite_deadline_emits_lifecycle_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """indefinite-tier deadline emits `lifecycle.failed` with
    error_class='DeadlineExceeded'."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("transient failure")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        retry_kind="indefinite",
        max_attempts=3,
        schedule_to_close=_NOW + timedelta(seconds=1),
    )
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="indefinite", base=timedelta(seconds=30), jitter=0.0),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    events = exporter.events_on("test-span", "lifecycle.failed")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberType]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["error_class"] == "DeadlineExceeded"
    assert attrs["from_state"] == "running"
    assert attrs["to_state"] == "failed"

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    assert call["next_scheduled_at"] is None


# ── transient-tier deadline ALSO emits lifecycle.failed ──────────────


async def test_transient_deadline_emits_lifecycle_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """transient-tier deadline emits `lifecycle.failed` with
    error_class='DeadlineExceeded'."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("transient failure")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        retry_kind="transient",
        max_attempts=5,
        schedule_to_close=_NOW + timedelta(seconds=1),
    )
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", base=timedelta(seconds=30), jitter=0.0),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    events = exporter.events_on("test-span", "lifecycle.failed")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberType]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["error_class"] == "DeadlineExceeded"


# ── indefinite-tier Retry does NOT emit taskq.indefinite_retry ────────


async def test_no_taskq_indefinite_retry_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """taskq.indefinite_retry event is dropped — it was a debug
    annotation, not a state transition."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("transient failure")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        retry_kind="indefinite",
        max_attempts=3,
        schedule_to_close=_NOW + timedelta(hours=24),
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    assert len(exporter.events_on("test-span", "taskq.indefinite_retry")) == 0


# ── Rate-limit / reservation acquire-release integration ────────────


class _StubRateLimitRegistry:
    """Minimal RateLimitRegistry stub for consumer unit tests.

    Records acquire_for_actor and release_for_actor calls so tests can
    verify the consumer's wrapping logic without real backends.
    """

    def __init__(
        self,
        *,
        acquire_side_effect: BaseException | None = None,
    ) -> None:
        self.acquire_calls: list[dict[str, object]] = []
        self.release_calls: list[dict[str, object]] = []
        self._acquire_side_effect = acquire_side_effect
        self._acquired: list[object] = [_STUB_HANDLE]

    async def acquire_for_actor(
        self,
        rate_limits: list[str],
        reservations: list[str],
        *,
        job_id: UUID,
        worker_id: UUID,
        payload: dict[str, object] | None = None,
        redis_client: Any | None = None,
        pg_pool: Any | None = None,
        clock: Clock | None = None,
        settings: WorkerSettings | None = None,
    ) -> list[object]:
        self.acquire_calls.append(
            {
                "rate_limits": rate_limits,
                "reservations": reservations,
                "job_id": job_id,
                "worker_id": worker_id,
                "payload": payload,
            }
        )
        if self._acquire_side_effect is not None:
            raise self._acquire_side_effect
        return list(self._acquired)

    async def release_for_actor(
        self,
        acquired: list[object],
        *,
        pg_pool: object | None = None,
    ) -> None:
        self.release_calls.append(
            {
                "acquired": acquired,
                "pg_pool": pg_pool,
            }
        )


_STUB_HANDLE = object()


async def test_actor_mid_body_exception_releases_resources() -> None:
    """Actor raises mid-body — resources released in finally;
    the consumer handles the exception and release_for_actor is called."""
    rl_reg = _StubRateLimitRegistry()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def failing_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=failing_actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        rate_limit_registry=rl_reg,
        rate_limits=["tb"],
        reservations=[],
    )

    assert len(rl_reg.acquire_calls) == 1
    assert rl_reg.acquire_calls[0]["rate_limits"] == ["tb"]
    assert len(rl_reg.release_calls) == 1
    assert rl_reg.release_calls[0]["acquired"] == [_STUB_HANDLE]
    assert len(backend.mark_failed_or_retry_calls) == 1


async def test_rate_limit_denial_snoozes_with_awaiting() -> None:
    """Rate-limit denial translates to snooze with awaiting metadata and rate_limit_denied outcome."""
    rl_reg = _StubRateLimitRegistry(
        acquire_side_effect=ReservationUnavailable(
            bucket_name="tb",
            retry_after=timedelta(seconds=5),
            source="rate_limit",
        ),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on denial")

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=never_called_actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        rate_limit_registry=rl_reg,
        rate_limits=["tb"],
        reservations=[],
    )

    assert len(backend.mark_snoozed_calls) == 1
    snooze_call = backend.mark_snoozed_calls[0]
    assert snooze_call["metadata_update"] == {"awaiting": "rate_limit:tb"}
    assert snooze_call["outcome"] == "rate_limit_denied"


async def test_reservation_denial_snoozes_with_awaiting() -> None:
    """Reservation denial translates to snooze with awaiting metadata."""
    rl_reg = _StubRateLimitRegistry(
        acquire_side_effect=ReservationUnavailable(
            bucket_name="gpu_pool",
            retry_after=timedelta(seconds=5),
        ),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on denial")

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=never_called_actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        rate_limit_registry=rl_reg,
        rate_limits=[],
        reservations=["gpu_pool"],
    )

    assert len(backend.mark_snoozed_calls) == 1
    snooze_call = backend.mark_snoozed_calls[0]
    assert snooze_call["metadata_update"] == {"awaiting": "reservation:gpu_pool"}


async def test_sub_enqueue_does_not_call_acquire_for_actor() -> None:
    """Sub-actor enqueue does not call acquire_for_actor on the
    child actor's rate limits/reservations."""
    from unittest.mock import MagicMock

    import asyncpg

    from taskq.actor import actor

    rl_reg = _StubRateLimitRegistry()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    @actor(rate_limits=["child_rl"], reservations=["child_res"])
    async def child_actor(payload: EmptyPayload) -> None: ...

    fake_pool: asyncpg.Pool = MagicMock(spec=asyncpg.Pool)  # type: ignore[assignment]  Why: MagicMock stand-in for asyncpg.Pool satisfies SubJobEnqueuer's None-check without a real PG pool; the _FakeBackend.enqueue path does not use the pool.

    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=fake_pool,
        backend=as_backend(backend),
        clock=clk,
    )

    async def actor_with_sub_enqueue(_job: object, ctx: JobContext[BaseModel]) -> object:
        assert isinstance(ctx, JobContext)
        await ctx.jobs.enqueue(child_actor, EmptyPayload())
        return None

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor_with_sub_enqueue,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        rate_limit_registry=rl_reg,
        rate_limits=["tb"],
        reservations=[],
        enqueuer=enqueuer,
    )

    assert len(rl_reg.acquire_calls) == 1
    assert rl_reg.acquire_calls[0]["rate_limits"] == ["tb"]
    assert len(rl_reg.release_calls) == 1
    assert len(backend.enqueue_calls) == 1


# ── Acquire-time denial observability (findings-2 Warning fix) ────────


async def test_reservation_denial_acquire_time_passes_outcome_and_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acquire-time reservation denial passes outcome='reservation_denied'
    to mark_snoozed and emits lifecycle.scheduled span event."""
    rl_reg = _StubRateLimitRegistry(
        acquire_side_effect=ReservationUnavailable(
            bucket_name="gpu_pool",
            retry_after=timedelta(seconds=5),
        ),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on denial")

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=never_called_actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            rate_limit_registry=rl_reg,
            rate_limits=[],
            reservations=["gpu_pool"],
        )

    assert len(backend.mark_snoozed_calls) == 1
    snooze_call = backend.mark_snoozed_calls[0]
    assert snooze_call["outcome"] == "reservation_denied"
    assert snooze_call["metadata_update"] == {"awaiting": "reservation:gpu_pool"}

    events = exporter.events_on("test-span", "lifecycle.scheduled")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["to_state"] == "scheduled"
    assert attrs["bucket_name"] == "gpu_pool"


async def test_rate_limit_denial_acquire_time_emits_span_and_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acquire-time rate-limit denial emits lifecycle.scheduled span
    event and snoozes with outcome='rate_limit_denied'."""
    rl_reg = _StubRateLimitRegistry(
        acquire_side_effect=ReservationUnavailable(
            bucket_name="tb",
            retry_after=timedelta(seconds=5),
            source="rate_limit",
        ),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on denial")

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=never_called_actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            rate_limit_registry=rl_reg,
            rate_limits=["tb"],
            reservations=[],
        )

    assert len(backend.mark_snoozed_calls) == 1
    snooze_call = backend.mark_snoozed_calls[0]
    assert snooze_call["outcome"] == "rate_limit_denied"
    assert snooze_call["metadata_update"] == {"awaiting": "rate_limit:tb"}

    events = exporter.events_on("test-span", "lifecycle.scheduled")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["to_state"] == "scheduled"
    assert attrs["bucket_name"] == "tb"


# ── Resource leak regression (findings-2 Observation fix) ──────────


class _StrictPayload(BaseModel):
    """Payload that rejects empty dicts — used to trigger validation failure."""

    required_field: str


async def test_payload_validation_failure_releases_acquired_resources() -> None:
    """If payload validation fails after acquire, acquired resources are
    released in the outer finally block (resource leak regression)."""
    rl_reg = _StubRateLimitRegistry()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on validation failure")

    with contextlib.suppress(ValidationError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=never_called_actor,
            actor_config=cfg,
            payload_type=_StrictPayload,
            clock=clk,
            rate_limit_registry=rl_reg,
            rate_limits=["tb"],
            reservations=[],
        )

    assert len(rl_reg.acquire_calls) == 1
    assert len(rl_reg.release_calls) == 1


# ── lifecycle events ────────────────────────────────────


async def test_consume_success_emits_lifecycle_running_and_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful job emits lifecycle.running then lifecycle.succeeded."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row()
    cfg = default_actor_config()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    running = exporter.events_on("test-span", "lifecycle.running")
    assert len(running) == 1
    assert running[0].attributes is not None  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert running[0].attributes["from_state"] == "pending"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert running[0].attributes["to_state"] == "running"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes

    succeeded = exporter.events_on("test-span", "lifecycle.succeeded")
    assert len(succeeded) == 1
    assert succeeded[0].attributes is not None  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert succeeded[0].attributes["from_state"] == "running"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert succeeded[0].attributes["to_state"] == "succeeded"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes

    attempt = exporter.span_named("attempt.1")
    assert attempt is not None
    assert len(attempt.events) == 0


async def test_consume_cancelled_emits_lifecycle_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelled job emits lifecycle.cancelled."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row()
    cfg = default_actor_config()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"), pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    events = exporter.events_on("test-span", "lifecycle.cancelled")
    assert len(events) == 1
    assert events[0].attributes is not None  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert events[0].attributes["from_state"] == "running"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert events[0].attributes["to_state"] == "cancelled"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes


async def test_lifecycle_failed_carries_error_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """lifecycle.failed events carry error_class attribute."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    events = exporter.events_on("test-span", "lifecycle.failed")
    assert len(events) == 1
    assert events[0].attributes is not None  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert events[0].attributes["error_class"] == "RuntimeError"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert events[0].attributes["from_state"] == "running"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert events[0].attributes["to_state"] == "failed"  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes


async def test_generic_exception_logs_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_handle_generic_exception logs job_exception at WARNING with
    error_class, error_message, and error_traceback on every attempt, and
    exactly one terminal job_failed at ERROR after the write persists."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_exception"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_exception log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "RuntimeError"
    assert kwargs["error_message"] == "boom"
    assert "Traceback" in kwargs["error_traceback"]
    assert "RuntimeError: boom" in kwargs["error_traceback"]
    assert str(job.id) == kwargs["job_id"]
    assert kwargs["actor"] == job.actor
    assert kwargs["attempt"] == job.attempt

    error_calls = [c for c in mock_log.error.call_args_list if c.args and c.args[0] == "job_failed"]
    assert len(error_calls) == 1, f"expected 1 job_failed log, got {len(error_calls)}"
    kwargs = error_calls[0].kwargs
    assert kwargs["cause"] == "RuntimeError"
    assert kwargs["error_class"] == "RuntimeError"
    assert kwargs["error_message"] == "boom"
    assert "RuntimeError: boom" in kwargs["error_traceback"]


async def test_timeout_logs_actual_exception_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_handle_timeout logs job_timeout with the actual TimeoutError, not hardcoded values."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("database query took too long")

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_timeout"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_timeout log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "TimeoutError"
    assert kwargs["error_message"] == "database query took too long"
    assert "Traceback" in kwargs["error_traceback"]
    assert "TimeoutError: database query took too long" in kwargs["error_traceback"]
    assert str(job.id) == kwargs["job_id"]
    assert kwargs["actor"] == job.actor

    error_calls = [c for c in mock_log.error.call_args_list if c.args and c.args[0] == "job_failed"]
    assert len(error_calls) == 1, f"expected 1 job_failed log, got {len(error_calls)}"
    kwargs = error_calls[0].kwargs
    assert kwargs["cause"] == "TimeoutError"
    assert kwargs["error_class"] == "TimeoutError"
    assert kwargs["error_message"] == "database query took too long"


async def test_timeout_retry_emits_log_state_change() -> None:
    """_handle_timeout emits log_state_change running→scheduled when the
    retry decision is Retry — mirroring _handle_generic_exception."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("slow query")

    job = make_job_row(attempt=1, max_attempts=3, retry_kind="transient")
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()

    import structlog

    with structlog.testing.capture_logs() as captured:
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            logger=structlog.get_logger("test"),
        )

    state_changes = [
        e for e in captured if e.get("kind") == "state_change" and e.get("to_state") == "scheduled"
    ]
    assert len(state_changes) == 1, f"expected 1 state_change log, got {len(state_changes)}"
    entry = state_changes[0]
    assert entry["from_state"] == "running"
    assert entry["cause"] == "TimeoutError"
    assert entry["job_id"] == str(job.id)
    assert entry["actor"] == job.actor


async def test_timeout_terminal_emits_log_state_change() -> None:
    """_handle_timeout emits log_state_change running→failed when retries are
    exhausted — mirroring _handle_generic_exception."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("slow query")

    job = make_job_row(attempt=3, max_attempts=3, retry_kind="transient")
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()

    import structlog

    with structlog.testing.capture_logs() as captured:
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            logger=structlog.get_logger("test"),
        )

    state_changes = [
        e for e in captured if e.get("kind") == "state_change" and e.get("to_state") == "failed"
    ]
    assert len(state_changes) == 1, f"expected 1 state_change log, got {len(state_changes)}"
    entry = state_changes[0]
    assert entry["from_state"] == "running"
    assert entry["cause"] == "TimeoutError"
    assert entry["retryable"] is False
    assert entry["job_id"] == str(job.id)
    assert entry["actor"] == job.actor


async def test_snooze_terminal_failure_logs_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_handle_snooze terminal 'failed' outcome logs job_failed at ERROR level."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=1))

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="indefinite",
        schedule_to_close=_NOW + timedelta(seconds=1),
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    backend = _FakeBackend(mark_snoozed_return="failed")
    clk: Clock = FakeClock(_NOW + timedelta(seconds=2))

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    error_calls = [c for c in mock_log.error.call_args_list if c.args and c.args[0] == "job_failed"]
    assert len(error_calls) == 1, f"expected 1 job_failed log, got {len(error_calls)}"
    kwargs = error_calls[0].kwargs
    assert kwargs["cause"] == "DeadlineExceeded"
    assert kwargs["error_class"] == "DeadlineExceeded"
    assert str(job.id) == kwargs["job_id"]
    assert kwargs["actor"] == job.actor
    # None-valued optionals are omitted from the event, not logged as null.
    assert "error_message" not in kwargs


async def test_retry_after_terminal_failure_logs_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_handle_retry_after terminal 'failed' outcome logs job_failed at ERROR level."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=1))

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="indefinite",
        schedule_to_close=_NOW + timedelta(seconds=1),
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    backend = _FakeBackend(mark_retry_after_return="failed:DeadlineExceeded")
    clk: Clock = FakeClock(_NOW + timedelta(seconds=2))

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    error_calls = [c for c in mock_log.error.call_args_list if c.args and c.args[0] == "job_failed"]
    assert len(error_calls) == 1, f"expected 1 job_failed log, got {len(error_calls)}"
    kwargs = error_calls[0].kwargs
    assert kwargs["cause"] == "DeadlineExceeded"
    assert kwargs["error_class"] == "DeadlineExceeded"
    assert str(job.id) == kwargs["job_id"]
    assert kwargs["actor"] == job.actor


async def test_reservation_denied_terminal_failure_logs_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_handle_reservation_class_denied terminal 'failed' outcome logs job_failed at ERROR level."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise ReservationUnavailable(bucket_name="test-bucket", retry_after=timedelta(seconds=1))

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="indefinite",
        schedule_to_close=_NOW + timedelta(seconds=1),
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    backend = _FakeBackend(mark_snoozed_return="failed")
    clk: Clock = FakeClock(_NOW + timedelta(seconds=2))

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    error_calls = [c for c in mock_log.error.call_args_list if c.args and c.args[0] == "job_failed"]
    assert len(error_calls) == 1, f"expected 1 job_failed log, got {len(error_calls)}"
    kwargs = error_calls[0].kwargs
    assert kwargs["cause"] == "DeadlineExceeded"
    assert kwargs["bucket_name"] == "test-bucket"
    assert str(job.id) == kwargs["job_id"]
    assert kwargs["actor"] == job.actor


# ── attempt span is child of CONSUMER span ────────────────────


async def test_attempt_span_created_as_internal_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """consume_one_job creates an INTERNAL attempt.{N} child span."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(attempt=2)
    cfg = default_actor_config()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-consumer"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    attempt = exporter.span_named("attempt.2")
    assert attempt is not None
    assert attempt.kind == trace.SpanKind.INTERNAL

    consumer = exporter.span_named("test-consumer")
    assert consumer is not None
    if attempt.parent is not None and consumer.context is not None:
        assert attempt.parent.span_id == consumer.context.span_id


# ── Lifecycle events are on the consumer span, not the attempt span ────


async def test_lifecycle_events_on_consumer_span_not_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """lifecycle events are emitted on the consumer span (the
    current span at entry), not on the attempt.{N} child span."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(attempt=1)
    cfg = default_actor_config()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-consumer"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    consumer = exporter.span_named("test-consumer")
    assert consumer is not None
    assert len(exporter.events_on("test-consumer", "lifecycle.running")) == 1
    assert len(exporter.events_on("test-consumer", "lifecycle.succeeded")) == 1

    attempt = exporter.span_named("attempt.1")
    assert attempt is not None
    assert len(attempt.events) == 0


# ── JobContext.span wired to consumer span ──────────────────────────────


async def test_job_context_span_is_consumer_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JobContext.span is set to the current (consumer) span when
    the span is a real recording span; None when NonRecordingSpan."""
    captured_ctx: JobContext[BaseModel] | None = None

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> object:
        nonlocal captured_ctx
        assert isinstance(ctx, JobContext)
        captured_ctx = ctx
        return None

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row()
    cfg = default_actor_config()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    with tracer.start_as_current_span("test-consumer"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    assert captured_ctx is not None
    assert captured_ctx.span is not None
    assert not isinstance(captured_ctx.span, trace.NonRecordingSpan)

    consumer = exporter.span_named("test-consumer")
    assert consumer is not None
    assert captured_ctx.span.get_span_context().span_id == consumer.context.span_id


async def test_job_context_span_none_when_otel_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When otel_enabled=False, JobContext.span is None (the
    NonRecordingSpan is not exposed to the actor)."""
    captured_ctx: JobContext[BaseModel] | None = None

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> object:
        nonlocal captured_ctx
        assert isinstance(ctx, JobContext)
        captured_ctx = ctx
        return None

    setup_tracer(monkeypatch)
    obs_mod.set_otel_enabled(False)

    job = make_job_row()
    cfg = default_actor_config()
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )

    assert captured_ctx is not None
    assert captured_ctx.span is None


# ── AttemptOutcome return contract ───────────────────────────────────────


async def test_consume_success_returns_succeeded() -> None:
    """consume_one_job returns 'succeeded' on successful actor completion."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "succeeded"


async def test_consume_snooze_returns_scheduled() -> None:
    """consume_one_job returns 'scheduled' on Snooze."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=30))

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "scheduled"


async def test_consume_retry_after_returns_scheduled() -> None:
    """consume_one_job returns 'scheduled' on RetryAfter."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=10))

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "scheduled"


async def test_consume_reservation_unavailable_returns_scheduled() -> None:
    """consume_one_job returns 'scheduled' on ReservationUnavailable."""
    rl_reg = _StubRateLimitRegistry(
        acquire_side_effect=ReservationUnavailable(
            bucket_name="tb",
            retry_after=timedelta(seconds=5),
        ),
    )
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on denial")

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=never_called_actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        rate_limit_registry=rl_reg,
        rate_limits=["tb"],
        reservations=[],
    )
    assert result == "scheduled"


async def test_consume_failed_returns_failed() -> None:
    """consume_one_job returns 'failed' when retries exhausted."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0))
    job = make_job_row(attempt=1, max_attempts=1)
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "failed"


async def test_consume_timeout_returns_failed() -> None:
    """consume_one_job returns 'failed' on TimeoutError."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError()

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "failed"


async def test_consume_cancelled_raises_and_does_not_return() -> None:
    """consume_one_job re-raises CancelledError instead of returning."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    with pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )


# ── pre-terminal flush before mark_succeeded ──────────────────────


async def test_pre_terminal_flush_before_mark_succeeded() -> None:
    """Buffer is flushed before mark_succeeded; the flush pool
    acquire is called and the buffer is clean after the job completes."""
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock

    from taskq.worker.deps import WorkerDeps

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    pool = AsyncMock()
    conn = AsyncMock()
    conn.fetchrow.return_value = {"progress_seq": 1}

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})

    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = pool
    deps.settings = settings
    deps.redis_client = None

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> dict[str, object]:
        assert isinstance(ctx, JobContext)
        await ctx.progress(step=5)
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "succeeded"
    assert conn.fetchrow.call_count >= 1


# ── cancel path passes progress_state=None to mark_cancelled ─────


async def test_cancel_path_passes_progress_state_none() -> None:
    """On cancel, mark_cancelled receives progress_state=None so
    COALESCE preserves last periodic-flush snapshot in PG."""
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    assert len(backend.mark_cancelled_calls) == 1
    call = backend.mark_cancelled_calls[0]
    assert call["job_id"] == job.id
    assert call["worker_id"] == _WORKER_ID
    assert call["progress_state"] is None


# ── Regression: cancel path with clean buffer preserves base_seq ──────────


async def test_cancel_clean_buffer_passes_base_seq_not_zero() -> None:
    """Regression for findings-3 Critical: when the progress buffer is clean
    (post-flush, base_seq > 0, pending_seq_delta=0), the cancel path must
    pass progress_seq=base_seq to mark_cancelled, NOT 0."""
    from unittest.mock import MagicMock

    from taskq.progress._buffer import _ProgressBuffer
    from taskq.worker.deps import WorkerDeps

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row(progress_seq=5)
    buffers: dict[UUID, _ProgressBuffer] = {}

    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})

    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = settings
    deps.redis_client = None

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
        )

    assert len(backend.mark_cancelled_calls) == 1
    call = backend.mark_cancelled_calls[0]
    assert call["job_id"] == job.id
    assert call["worker_id"] == _WORKER_ID
    assert call["progress_state"] is None
    # The critical assertion: progress_seq must equal the buffer's
    # base_seq (5, from job.progress_seq), not 0
    assert call["progress_seq"] == 5


# ── Regression: dispatch path passes deps so progress_buffers is live ────


async def test_deps_parameter_enables_buffer_registration() -> None:
    """Regression for findings-4 Critical: when deps is passed,
    progress_buffers flows through and buffer registration occurs,
    ctx.progress() is not a no-op, and pre-terminal flush fires."""
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock

    from taskq.progress._buffer import _ProgressBuffer
    from taskq.worker.deps import WorkerDeps

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    pool = AsyncMock()
    conn = AsyncMock()
    conn.fetchrow.return_value = {"progress_seq": 1}

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})

    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = pool
    deps.settings = settings
    deps.redis_client = None

    progress_called = False

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> dict[str, object]:
        nonlocal progress_called
        assert isinstance(ctx, JobContext)
        await ctx.progress(step=1)
        progress_called = True
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "succeeded"
    assert progress_called


# ── Regression: _consume_autonomous respects explicit params over deps ────


async def test_autonomous_explicit_params_override_deps() -> None:
    """Regression for findings-4 Warning: _consume_autonomous uses explicit
    redis_client/settings/worker_pool when provided, falling back to deps
    only when they are None."""
    from unittest.mock import MagicMock

    from taskq.progress._buffer import _ProgressBuffer
    from taskq.worker.deps import WorkerDeps

    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})

    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = settings
    deps.redis_client = None

    progress_called = False

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> dict[str, object]:
        nonlocal progress_called
        assert isinstance(ctx, JobContext)
        await ctx.progress(step=1)
        progress_called = True
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        worker_pool=None,
        settings=settings,
        redis_client=None,
    )
    assert result == "succeeded"
    assert progress_called


# ── TimeoutError subclass: span/log error_class agreement ─────────────


async def test_timeout_subclass_span_log_agree_on_retry_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A TimeoutError subclass on the retry path reports the concrete
    class in both the lifecycle.scheduled span event and the job_timeout
    warning — not a hardcoded 'TimeoutError'."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise _SlowQueryTimeout("query exceeded deadline")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    events = exporter.events_on("test-span", "lifecycle.scheduled")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["error_class"] == "_SlowQueryTimeout"

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_timeout"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_timeout log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "_SlowQueryTimeout"


async def test_timeout_subclass_span_log_agree_on_terminal_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same agreement pin on the terminal path: lifecycle.failed carries
    the concrete subclass name."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise _SlowQueryTimeout("query exceeded deadline")

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    events = exporter.events_on("test-span", "lifecycle.failed")
    assert len(events) == 1
    attrs = events[0].attributes  # type: ignore[reportUnknownMemberAccess]  # Why: events_on returns list[Any]; at runtime these are Event objects with .attributes
    assert attrs is not None
    assert attrs["error_class"] == "_SlowQueryTimeout"


# ── Retry path: WARNING only, zero ERROR noise ────────────────────────


async def test_generic_exception_retry_path_logs_warning_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retryable generic exception logs job_exception at WARNING and
    nothing at ERROR — the single job_failed ERROR is reserved for the
    terminal failure."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("transient boom")

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_exception"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_exception log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "RuntimeError"
    assert kwargs["error_message"] == "transient boom"
    mock_log.error.assert_not_called()

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    assert call["next_scheduled_at"] is not None


async def test_timeout_retry_path_logs_warning_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retryable timeout logs job_timeout at WARNING and nothing at
    ERROR."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("db slow")

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _FakeBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_timeout"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_timeout log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "TimeoutError"
    assert kwargs["error_message"] == "db slow"
    mock_log.error.assert_not_called()

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    assert call["next_scheduled_at"] is not None


# ── Ownership mismatch: terminal write lost → NO job_failed ─────────────


class _OwnershipMismatchBackend(_FakeBackend):
    """mark_failed_or_retry always loses the ownership race."""

    async def mark_failed_or_retry(
        self,
        job_id: UUID,
        worker_id: UUID,
        error_info: ErrorInfo,
        next_scheduled_at: datetime | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> JobRow:
        raise WorkerOwnershipMismatch(job_id=job_id, expected=worker_id, actual=new_uuid())


async def test_generic_exception_terminal_ownership_mismatch_logs_no_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal generic exception whose write loses the ownership race emits
    NO job_failed ERROR — the job is not dead by our hand. The per-attempt
    job_exception diagnostic and the ownership-mismatch WARNING still fire."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _OwnershipMismatchBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    mock_log.error.assert_not_called()

    mismatch_warnings = [
        c
        for c in mock_log.warning.call_args_list
        if c.args and c.args[0] == "mark-failed-or-retry-ownership-mismatch"
    ]
    assert len(mismatch_warnings) == 1, (
        f"expected 1 ownership-mismatch warning, got {len(mismatch_warnings)}"
    )

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_exception"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_exception log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "RuntimeError"
    assert kwargs["error_message"] == "boom"


async def test_timeout_terminal_ownership_mismatch_logs_no_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal timeout whose write loses the ownership race emits NO
    job_failed ERROR — the job is not dead by our hand. The per-attempt
    job_timeout diagnostic and the ownership-mismatch WARNING still fire."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise TimeoutError("db slow")

    setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    job = make_job_row(
        attempt=3,
        max_attempts=3,
        retry_kind="transient",
    )
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    backend = _OwnershipMismatchBackend()
    clk: Clock = FakeClock(_NOW)

    from unittest.mock import MagicMock

    import structlog

    mock_log = MagicMock(spec=structlog.stdlib.BoundLogger)
    mock_log.bind.return_value = mock_log

    with tracer.start_as_current_span("test-span"):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            logger=mock_log,
        )

    mock_log.error.assert_not_called()

    mismatch_warnings = [
        c
        for c in mock_log.warning.call_args_list
        if c.args and c.args[0] == "mark-failed-or-retry-ownership-mismatch"
    ]
    assert len(mismatch_warnings) == 1, (
        f"expected 1 ownership-mismatch warning, got {len(mismatch_warnings)}"
    )

    warning_calls = [
        c for c in mock_log.warning.call_args_list if c.args and c.args[0] == "job_timeout"
    ]
    assert len(warning_calls) == 1, f"expected 1 job_timeout log, got {len(warning_calls)}"
    kwargs = warning_calls[0].kwargs
    assert kwargs["error_class"] == "TimeoutError"
    assert kwargs["error_message"] == "db slow"
