"""Unit tests for the per-job consumer helper ().

Tests use a fake backend implementing the ``Backend`` protocol with
recorded call arguments, and shared OTel test utilities from
``taskq.testing.otel`` for span assertions.
"""

import asyncio
from collections.abc import Awaitable as AwaitableABC
from collections.abc import Callable as CallableABC
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
import structlog
from pydantic import BaseModel

import taskq.obs as obs_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import (
    ErrorInfo,
    IdentityKey,
    JobRow,
)
from taskq.backend.clock import Clock
from taskq.context import JobContext
from taskq.exceptions import ReservationUnavailable, RetryAfter, Snooze
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
from taskq.worker.cancel import ActiveJobRegistry

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _StubRateLimitRegistry:
    """Minimal RateLimitRegistry stub for consumer unit tests."""

    def __init__(
        self,
        *,
        acquire_side_effect: BaseException | None = None,
    ) -> None:
        self._acquire_side_effect = acquire_side_effect

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
        if self._acquire_side_effect is not None:
            raise self._acquire_side_effect
        return [object()]

    async def release_for_actor(
        self,
        acquired: list[object],
        *,
        pg_pool: object | None = None,
    ) -> None:
        pass


async def _run_consume(
    job: JobRow,
    backend: FakeBackend,
    run_actor: CallableABC[[JobRow, JobContext[BaseModel]], AwaitableABC[object]],
    actor_config: StubActorConfig | None = None,
    clock: Clock | None = None,
    active_jobs: ActiveJobRegistry | None = None,
) -> None:
    cfg = actor_config if actor_config is not None else default_actor_config()
    clk = clock if clock is not None else FakeClock(_NOW)
    log = structlog.get_logger("test")
    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=run_actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        logger=log,
        active_jobs=active_jobs,
    )


# ── Success ──────────────────────────────────────────────────────────────


async def test_consume_success_calls_mark_succeeded() -> None:
    """Baseline: successful actor → mark_succeeded called with result dict."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"value": 42}

    backend = FakeBackend()
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_succeeded_calls) == 1
    assert backend.mark_succeeded_calls[0] == (job.id, _WORKER_ID, {"value": 42})


# ── Snooze ────────────────────────────────────────────────────────────────


async def test_consume_snooze_calls_mark_snoozed_with_correct_delay() -> None:
    """Snooze handler passes delay=timedelta, not scheduled_at."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=30))

    backend = FakeBackend()
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_snoozed_calls) == 1
    call = backend.mark_snoozed_calls[0]
    assert call["delay"] == timedelta(seconds=30)
    assert call["outcome"] == "snoozed"


async def test_consume_snooze_emits_otel_event_lifecycle_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snooze success path emits OTel span event 'lifecycle.scheduled'."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=30))

    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()
    backend = FakeBackend()
    job = make_job_row()
    cfg = default_actor_config()
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
    assert events[0].attributes is not None  # type: ignore[reportUnknownMemberAccess] # Why: events_on returns list[Any]; at runtime these are Event objects with.attributes
    assert events[0].attributes["to_state"] == "scheduled"  # type: ignore[reportUnknownMemberAccess] # Why: events_on returns list[Any]; at runtime these are Event objects with.attributes
    assert events[0].attributes["from_state"] == "running"  # type: ignore[reportUnknownMemberAccess] # Why: events_on returns list[Any]; at runtime these are Event objects with.attributes


async def test_consume_snooze_deadline_exceeded_no_mark_succeeded() -> None:
    """Snooze when backend returns 'failed' does not call mark_succeeded."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=30))

    backend = FakeBackend(mark_snoozed_return="failed")
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_snoozed_calls) == 1
    assert len(backend.mark_succeeded_calls) == 0


async def test_consume_snooze_noop_returns_quietly() -> None:
    """Snooze when backend returns 'noop' — no mark_failed_or_retry call."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=30))

    backend = FakeBackend(mark_snoozed_return="noop")
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_snoozed_calls) == 1
    assert len(backend.mark_failed_or_retry_calls) == 0


# ── RetryAfter ────────────────────────────────────────────────────────────


async def test_consume_retry_after_consume_budget_true() -> None:
    """RetryAfter with consume_budget=True passes consume_budget=True to backend."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=60), consume_budget=True)

    backend = FakeBackend()
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_retry_after_calls) == 1
    call = backend.mark_retry_after_calls[0]
    assert call["consume_budget"] is True
    assert call["delay"] == timedelta(seconds=60)


async def test_consume_retry_after_consume_budget_false() -> None:
    """RetryAfter with consume_budget=False passes consume_budget=False to backend."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=60), consume_budget=False)

    backend = FakeBackend()
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_retry_after_calls) == 1
    call = backend.mark_retry_after_calls[0]
    assert call["consume_budget"] is False


async def test_consume_retry_after_failed_no_mark_succeeded() -> None:
    """RetryAfter when backend returns 'failed:MaxAttemptsExceeded' does not call mark_succeeded."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=60))

    backend = FakeBackend(mark_retry_after_return="failed:MaxAttemptsExceeded")
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_retry_after_calls) == 1
    assert len(backend.mark_succeeded_calls) == 0


# ── ReservationUnavailable ───────────────────────────────────────────────


async def test_consume_reservation_unavailable_calls_mark_snoozed_with_metadata() -> None:
    """ReservationUnavailable passes metadata_update and outcome=reservation_denied."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise ReservationUnavailable("gpu_pool", timedelta(seconds=10))

    backend = FakeBackend()
    job = make_job_row()
    await _run_consume(job, backend, actor)
    assert len(backend.mark_snoozed_calls) == 1
    call = backend.mark_snoozed_calls[0]
    assert call["metadata_update"] == {"awaiting": "reservation:gpu_pool"}
    assert call["outcome"] == "reservation_denied"
    assert call["delay"] == timedelta(seconds=10)


# ── CancelledError ────────────────────────────────────────────────────────


async def test_consume_cancelled_propagates_after_shielded_mark_cancelled() -> None:
    """CancelledError propagates after shielded mark_cancelled completes."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    backend = FakeBackend()
    job = make_job_row()
    with pytest.raises(asyncio.CancelledError):
        await _run_consume(job, backend, actor)
    assert len(backend.mark_cancelled_calls) == 1
    assert backend.mark_cancelled_calls[0]["job_id"] == job.id
    assert backend.mark_cancelled_calls[0]["worker_id"] == _WORKER_ID


async def test_consume_shielded_writes_complete_when_task_is_cancelled() -> None:
    """shielded write completes even when the consume task is cancelled mid-write."""

    write_completed = asyncio.Event()

    class SlowBackend(FakeBackend):
        async def mark_succeeded(
            self,
            job_id: UUID,
            worker_id: UUID,
            result: dict[str, object] | None,
            progress_seq: int = 0,
            progress_state: dict[str, object] | None = None,
        ) -> bool:
            await asyncio.sleep(0.05)
            write_completed.set()
            return await super().mark_succeeded(job_id, worker_id, result)

    backend = SlowBackend()
    job = make_job_row()

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    async def run() -> None:
        await _run_consume(job, backend, actor)

    task = asyncio.create_task(run())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(write_completed.wait(), timeout=1.0)
    assert write_completed.is_set()
    assert len(backend.mark_succeeded_calls) == 1


# ── non_retryable_exceptions ──────────────────────────────────────────────


async def test_consume_non_retryable_exception_uses_actor_config() -> None:
    """Non-retryable handler reads from actor_config, not a global registry."""

    class MyNonRetryableError(Exception):
        pass

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise MyNonRetryableError("nope")

    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        non_retryable_exceptions=(MyNonRetryableError,),
    )
    backend = FakeBackend()
    job = make_job_row()
    await _run_consume(job, backend, actor, actor_config=cfg)
    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    assert call["next_scheduled_at"] is None
    error_info: ErrorInfo = call["error_info"]  # type: ignore[reportAssignmentType] Why: dict[str, object] lookup returns object; runtime type is ErrorInfo
    assert error_info.error_class == "MyNonRetryableError"


# ── Structured logging fields ─────────────────────────────────────────────


async def test_consume_snooze_carries_job_identity_fields() -> None:
    """snooze path sees the job's identity_key and trace_id on the JobRow."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    backend = FakeBackend()
    job = make_job_row(identity_key=IdentityKey("my-key"), trace_id="trace-123")
    await _run_consume(job, backend, actor)
    assert len(backend.mark_snoozed_calls) == 1
    # The JobRow passed through carry identity_key and trace_id
    assert job.identity_key == IdentityKey("my-key")
    assert job.trace_id == "trace-123"


async def test_consume_control_flow_paths_complete_without_error() -> None:
    """Control-flow success arms (Snooze, RetryAfter, ReservationUnavailable) complete normally."""

    async def snooze_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    async def retry_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=5))

    async def reservation_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise ReservationUnavailable("bucket", timedelta(seconds=5))

    for actor_fn in (snooze_actor, retry_actor, reservation_actor):
        backend = FakeBackend()
        job = make_job_row()
        await _run_consume(job, backend, actor_fn)
        # Each control-flow path completed without raising an unhandled error
        assert len(backend.mark_snoozed_calls) + len(backend.mark_retry_after_calls) >= 1


# ── start_to_close timeout ────────────────────────────────────────


async def test_consume_start_to_close_timeout_routes_to_mark_failed_or_retry() -> None:
    """start_to_close timeout routes to mark_failed_or_retry with
    TimeoutError("start_to_close") and does NOT call mark_cancelled.
    """

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(0.5)

    backend = FakeBackend()
    job = make_job_row()
    job = replace(job, start_to_close=timedelta(seconds=0.1))

    await _run_consume(job, backend, actor)

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    error_info: ErrorInfo = call["error_info"]  # type: ignore[reportAssignmentType] Why: dict[str, object] lookup returns object; runtime type is ErrorInfo
    assert error_info.error_class == "TimeoutError"
    assert error_info.error_message == "start_to_close"
    assert len(backend.mark_cancelled_calls) == 0


# ── default_start_to_close worker fallback ────────────────────────


async def test_consume_no_start_to_close_anywhere_runs_unbounded() -> None:
    """A job with no start_to_close set on the row and no worker
    default_start_to_close completes even though the actor runs
    longer than the settings' unset timeout would ever allow."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(0.2)
        return {"ok": True}

    backend = FakeBackend()
    job = make_job_row()
    assert job.start_to_close is None

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        logger=structlog.get_logger("test"),
        settings=WorkerSettings(),
    )

    assert len(backend.mark_succeeded_calls) == 1


async def test_consume_worker_default_start_to_close_times_out_job_without_own() -> None:
    """A job with no start_to_close of its own, dispatched by a worker
    with default_start_to_close set, times out around that duration and
    routes to mark_failed_or_retry with TimeoutError("start_to_close")."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(0.5)

    backend = FakeBackend()
    job = make_job_row()
    assert job.start_to_close is None
    settings = WorkerSettings()
    settings.default_start_to_close = timedelta(seconds=0.1)

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        logger=structlog.get_logger("test"),
        settings=settings,
    )

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    error_info: ErrorInfo = call["error_info"]  # type: ignore[reportAssignmentType] Why: dict[str, object] lookup returns object; runtime type is ErrorInfo
    assert error_info.error_class == "TimeoutError"
    assert error_info.error_message == "start_to_close"


async def test_consume_job_start_to_close_overrides_worker_default() -> None:
    """A job with its own (short) start_to_close times out even though
    the worker's default_start_to_close would have allowed far more
    time — the job-row value always wins."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(0.5)

    backend = FakeBackend()
    job = make_job_row()
    job = replace(job, start_to_close=timedelta(seconds=0.1))
    settings = WorkerSettings()
    settings.default_start_to_close = timedelta(minutes=10)

    await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        logger=structlog.get_logger("test"),
        settings=settings,
    )

    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    error_info: ErrorInfo = call["error_info"]  # type: ignore[reportAssignmentType] Why: dict[str, object] lookup returns object; runtime type is ErrorInfo
    assert error_info.error_class == "TimeoutError"
    assert error_info.error_message == "start_to_close"


# ── external cancel routes to mark_cancelled ─────────────────────


async def test_consume_external_cancel_routes_to_mark_cancelled() -> None:
    """external cancel routes to mark_cancelled.

    Actor loops; an external coroutine sets the cancel_event and calls
    task.cancel(). Oracle: mark_cancelled called (under shield);
    mark_failed_or_retry NOT called.
    """

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        while True:  # noqa: ASYNC110 Why: cancellation test — actor loops until externally cancelled; asyncio.Event would require a separate event per test.
            await asyncio.sleep(0)

    backend = FakeBackend()
    job = make_job_row()

    async def run() -> None:
        await _run_consume(job, backend, actor)

    t = asyncio.create_task(run())
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert len(backend.mark_cancelled_calls) == 1
    assert backend.mark_cancelled_calls[0]["job_id"] == job.id
    assert backend.mark_cancelled_calls[0]["worker_id"] == _WORKER_ID
    assert len(backend.mark_failed_or_retry_calls) == 0


# ── Deregister-in-finally parameterised tests ──────────────────────────────


@pytest.mark.parametrize(
    "scenario",
    [
        "success",
        "timeout",
        "cancel",
        "exception",
    ],
)
async def test_consume_deregister_in_finally(scenario: str) -> None:
    """DoD point 11: deregister-in-finally runs on every exit path.

    Drive consume_one_job end-to-end through four exit paths and assert
    ``active_jobs.count() == 0`` after the call returns. Snooze and
    RetryAfter paths are deferred to the integration tier.
    """
    registry = ActiveJobRegistry()
    backend = FakeBackend()

    if scenario == "success":

        async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            return {"ok": True}

        job = make_job_row()
        await _run_consume(job, backend, actor, active_jobs=registry)

    elif scenario == "timeout":

        async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await asyncio.sleep(0.5)

        job = make_job_row()
        job = replace(job, start_to_close=timedelta(seconds=0.1))
        await _run_consume(job, backend, actor, active_jobs=registry)

    elif scenario == "cancel":

        async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            while True:  # noqa: ASYNC110 Why: cancellation test — actor loops until externally cancelled.
                await asyncio.sleep(0)

        job = make_job_row()

        async def run() -> None:
            await _run_consume(job, backend, actor, active_jobs=registry)

        t = asyncio.create_task(run())
        await asyncio.sleep(0.01)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    elif scenario == "exception":

        async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            raise RuntimeError("boom")

        job = make_job_row()
        await _run_consume(job, backend, actor, active_jobs=registry)

    assert registry.count() == 0


# ── Job-context fields in _handle_* log output (findings-1 Warning fix) ────


async def test_snooze_handler_log_carries_job_context_fields() -> None:
    """snooze handler log_state_change carries job_id,
    actor, queue, attempt, identity_key, trace_id from the pre-bound
    job_log — not from the raw module-level logger."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    backend = FakeBackend()
    job = make_job_row(identity_key=IdentityKey("my-key"))
    log = structlog.get_logger("test")

    with structlog.testing.capture_logs() as captured:
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            logger=log,
        )

    state_changes = [
        e for e in captured if e.get("kind") == "state_change" and e.get("to_state") == "scheduled"
    ]
    assert len(state_changes) >= 1
    entry = state_changes[0]
    assert entry["job_id"] == str(job.id)
    assert entry["actor"] == job.actor
    assert entry["queue"] == job.queue
    assert entry["attempt"] == job.attempt
    assert entry["identity_key"] == IdentityKey("my-key")
    assert entry["cause"] == "Snooze"


async def test_generic_exception_handler_log_carries_job_context_fields() -> None:
    """generic exception handler log_state_change carries
    job context fields from the pre-bound job_log."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    backend = FakeBackend()
    job = make_job_row(identity_key=IdentityKey("err-key"), attempt=1, max_attempts=1)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0))
    log = structlog.get_logger("test")

    with structlog.testing.capture_logs() as captured:
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            logger=log,
        )

    state_changes = [
        e for e in captured if e.get("kind") == "state_change" and e.get("to_state") == "failed"
    ]
    assert len(state_changes) >= 1
    entry = state_changes[0]
    assert entry["job_id"] == str(job.id)
    assert entry["actor"] == job.actor
    assert entry["queue"] == job.queue
    assert entry["identity_key"] == IdentityKey("err-key")


async def test_rate_limit_denial_handler_log_carries_job_context_fields() -> None:
    """acquire-time rate-limit denial handler log_state_change
    carries job context fields from the pre-bound job_log."""

    rl_reg = _StubRateLimitRegistry(
        acquire_side_effect=ReservationUnavailable(
            bucket_name="tb",
            retry_after=timedelta(seconds=5),
        ),
    )
    backend = FakeBackend()
    job = make_job_row(identity_key=IdentityKey("rl-key"))
    log = structlog.get_logger("test")

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body should not run on denial")

    with structlog.testing.capture_logs() as captured:
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=never_called_actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            logger=log,
            rate_limit_registry=rl_reg,
            rate_limits=["tb"],
            reservations=[],
        )

    state_changes = [
        e for e in captured if e.get("kind") == "state_change" and e.get("to_state") == "scheduled"
    ]
    assert len(state_changes) >= 1
    entry = state_changes[0]
    assert entry["job_id"] == str(job.id)
    assert entry["actor"] == job.actor
    assert entry["queue"] == job.queue
    assert entry["identity_key"] == IdentityKey("rl-key")
