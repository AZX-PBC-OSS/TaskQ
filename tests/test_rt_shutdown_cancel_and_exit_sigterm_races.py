"""Regression pins for two system-e2e shutdown races.

1. Cancel-racing-SIGTERM strand (Flake A):
   - ``CancelController`` vs ``orchestrate_shutdown`` (``FORCING``): when
     ``SIGTERM`` lands before ``CancelController`` observes ``cancel_phase = 1``,
     ``CANCELLING`` stamps ``cancel_phase = COOPERATIVE`` and
     ``cancel_origin = SHUTDOWN``. ``CancelController.run_in_tx`` then skips
     its Phase-1 origin override (gated on ``active.cancel_phase < COOPERATIVE``)
     and leaves ``cancel_origin = SHUTDOWN``. At ``cancellation_grace_period``,
     if ``CancelController`` fires Phase 2 just ahead of ``FORCING``'s 0.1s poll,
     the consumer catches ``CancelledError`` and enters ``mark_interrupted`` (or
     ``mark_cancelled`` when ``origin`` was already ``OPERATOR``). ``FORCING``
     then unconditionally calls ``active.task.cancel()`` a second time,
     interrupting the in-flight terminal write, disowning the job, skipping
     ``mark_cancelled``, and deregistering from ``active_jobs`` — so
     ``orchestrate_shutdown`` exits ``rc=0`` with the row stranded ``running``.
   - Take-to-register shutdown seam: if ``SIGTERM`` lands while a claimed row
     carrying ``cancel_phase >= 1`` is in the take-to-register window,
     ``consume_one_job``'s shutdown seam calls ``mark_snoozed`` (which no-ops on
     ``cancel_phase != 0``) and returns ``"scheduled"`` without calling
     ``mark_cancelled``, stranding the row ``running``.

2. Post-drain SIGTERM ``SIG_DFL`` teardown window (Flake B):
   - Worker entrypoints (``tests.system_e2e._worker_entry`` and ``taskq.cli``'s
     ``worker`` command) install ``signal.signal(SIGTERM, SIG_IGN)`` only AFTER
     ``asyncio.Runner.__exit__()`` closes the event loop. Inside
     ``loop.close()``, ``_UnixSelectorEventLoop.remove_signal_handler(SIGTERM)``
     restores ``SIG_DFL``, opening an unguarded window where a redundant
     ``SIGTERM`` kills an already-drained worker with ``rc = -15``.

3. Pre-registration release ownership:
   - The rows the shutdown seam and the acquire-denial arms release carry no
     ``active_jobs`` entry, so the release write is their only writer, and the
     pool it writes through is closed by the deps teardown ordered after the
     consumer's TaskGroup join. A release detached at the cancellation runs on
     past that join and meets the closing pool; the consumer therefore finishes
     the release before its own cancellation is re-raised.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import asyncpg
import pytest
from pydantic import BaseModel

import taskq.worker.shutdown as shutdown_mod
from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.backend._protocol import CancelPhase, EnqueueArgs, JobId
from taskq.backend.clock import SystemClock
from taskq.constants import CANCEL_ORIGIN_FORCED
from taskq.context import CancelOrigin, JobContext
from taskq.exceptions import ReservationUnavailable
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry, make_cancel_controller
from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown


class _SlowPayload(BaseModel):
    sleep: float = 60.0


class _MinimalLiveness:
    def tick(self, *args: object, **kwargs: object) -> None:
        return None


class _MinimalDeps:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.active_jobs = ActiveJobRegistry()
        self.disowned_jobs: set[JobId] = set()
        self.progress_buffers: dict[JobId, Any] = {}
        self.shutdown_phase = ShutdownPhase.NONE
        self.shutdown_started_at: float | None = None
        self.shutdown_start_event = asyncio.Event()
        self.producer_stop_event = asyncio.Event()
        self.leader_conn: Any = None
        self.owns_leader_conn = False
        self.dispatcher_pool: Any = None
        self.worker_pool: Any = None
        self.redis_client: Any = None
        self.pending_publish_tasks: set[asyncio.Task[Any]] = set()
        self.liveness = _MinimalLiveness()


class _MinimalActorConfig:
    max_attempts = 5
    retry_kind = "transient"
    retry_base = timedelta(seconds=1)
    retry_jitter = 0.0
    max_retry_backoff = timedelta(seconds=60)
    start_to_close = None
    on_cancel = None
    on_cancel_timeout = 5.0
    result_ttl = None
    rate_limits: tuple[object, ...] = ()
    reservations: tuple[object, ...] = ()
    retry = None


class _SlowTerminalBackend(InMemoryBackend):
    """InMemoryBackend whose interrupt/cancel writes pause briefly to model a
    real Postgres round-trip under load during shutdown."""

    def __init__(self, *, write_delay: float = 0.15) -> None:
        super().__init__(
            SystemClock(),
            actor_configs=[ActorConfig(actor="sys_slow", queue="default", max_concurrent=None)],
        )
        self._write_delay = write_delay
        self.mark_interrupted_calls = 0
        self.mark_cancelled_calls = 0
        self.mark_cancelled_entered = asyncio.Event()
        self._inflight_writes: list[asyncio.Task[Any]] = []

    async def mark_interrupted(self, *args: Any, **kwargs: Any) -> Any:
        self.mark_interrupted_calls += 1
        current = asyncio.current_task()
        if current is not None:
            self._inflight_writes.append(current)
        await asyncio.sleep(self._write_delay)
        return await super().mark_interrupted(*args, **kwargs)

    async def mark_cancelled(self, *args: Any, **kwargs: Any) -> Any:
        self.mark_cancelled_calls += 1
        current = asyncio.current_task()
        if current is not None:
            self._inflight_writes.append(current)
        self.mark_cancelled_entered.set()
        await asyncio.sleep(self._write_delay)
        return await super().mark_cancelled(*args, **kwargs)

    async def drain_inflight(self) -> None:
        """Await any detached shield_with_retrieval writes in test teardown
        so the module-loop pending-task guard never masks the test's own assertion."""
        pending = [t for t in self._inflight_writes if not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


def _load_worker_settings(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> WorkerSettings:
    env = {
        "TASKQ_PG_DSN": "postgresql://u:p@localhost/db",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.25",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.25",
        "TASKQ_LOCK_LEASE": "15.0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "20.0",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
        "TASKQ_WATCHDOG_ENABLED": "false",
        "TASKQ_ENVIRONMENT": "dev",
        **overrides,
    }
    for key, val in env.items():
        monkeypatch.setenv(key, val)
    return WorkerSettings.load()


class _FakeCancelConn:
    """Minimal asyncpg connection stand-in driving ``CancelController.run_in_tx``
    against an ``InMemoryBackend``."""

    def __init__(self, backend: InMemoryBackend, job_id: JobId, worker_id: Any) -> None:
        self._backend = backend
        self._job_id = job_id
        self._worker_id = worker_id

    async def fetch(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
        row = await self._backend.get(self._job_id)
        if row is None or row.status != "running" or row.cancel_phase == 0:
            return []
        return [{"id": self._job_id, "cancel_phase": row.cancel_phase}]

    async def execute(self, sql: str, *args: object) -> str:
        if "SET cancel_phase = 2" in sql:
            ok = await self._backend.write_cancel_escalation(self._job_id, self._worker_id, phase=2)
            return "UPDATE 1" if ok else "UPDATE 0"
        if "INSERT INTO" in sql and "job_events" in sql:
            return "INSERT 0 1"
        raise AssertionError(f"Unexpected SQL in _FakeCancelConn.execute: {sql[:120]!r}")


async def test_cancel_controller_and_forcing_race_terminalises_operator_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When SIGTERM arrives before CancelController sees an operator cancel,
    and CancelController's phase-2 tick lands just ahead of FORCING, the job
    must still terminalise ``cancelled`` and never be disowned as ``running``."""
    settings = _load_worker_settings(monkeypatch, TASKQ_CANCELLATION_GRACE_PERIOD="0.205")
    deps = _MinimalDeps(settings)
    backend = _SlowTerminalBackend(write_delay=0.25)
    worker_id = new_uuid()

    enq = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job_id = enq.id
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]

    body_started = asyncio.Event()

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        body_started.set()
        await asyncio.sleep(60.0)
        return None

    consumer_task = asyncio.create_task(
        consume_one_job(
            backend=backend,
            job=job,
            worker_id=worker_id,
            run_actor=_run_actor,
            payload_type=_SlowPayload,
            actor_config=_MinimalActorConfig(),  # type: ignore[arg-type]
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
            deps=deps,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(body_started.wait(), timeout=2.0)

    # 1. Operator cancel stamps cancel_phase = 1 on the running row.
    await backend.write_cancel_request(job_id, reason="offboard")

    # 2. SIGTERM starts orchestrate_shutdown before CancelController polls.
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))
    shutdown_event = asyncio.Event()
    shut_task = asyncio.create_task(
        orchestrate_shutdown(
            deps,  # type: ignore[arg-type]
            settings,
            worker_id,
            shutdown_event,
            None,
            backend=backend,
        )
    )
    while deps.shutdown_phase != ShutdownPhase.CANCELLING:  # noqa: ASYNC110  # Why: shutdown_phase is a plain enum field polled until the orchestrator enters CANCELLING.
        await asyncio.sleep(0.005)

    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    fake_conn = _FakeCancelConn(backend, job_id, worker_id)

    # Heartbeat tick during CANCELLING observes cancel_phase = 1.
    await controller.run_in_tx(fake_conn)

    # Sleep to 0.215s: past cancel_grace (0.205s) while orchestrate_shutdown is
    # still inside its 3rd 0.1s sleep (0.20..0.30s), leaving ~85ms margin before
    # FORCING wakes while the 0.25s terminal write is in flight.
    await asyncio.sleep(0.215)
    await controller.run_in_tx(fake_conn)

    rc = await asyncio.wait_for(shut_task, timeout=5.0)
    try:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=2.0)

        final_row = await backend.get(job_id)
        assert rc == 0
        assert final_row is not None
        assert final_row.status == "cancelled", (
            f"operator cancel racing SIGTERM stranded row in status={final_row.status!r} "
            f"(cancel_phase={final_row.cancel_phase}, mark_interrupted_calls="
            f"{backend.mark_interrupted_calls}, mark_cancelled_calls={backend.mark_cancelled_calls}, "
            f"disowned={job_id in deps.disowned_jobs})"
        )
        assert job_id not in deps.disowned_jobs, (
            "FORCING must not double-cancel and disown a consumer already unwinding its terminal write"
        )
    finally:
        await backend.drain_inflight()


async def test_forcing_does_not_double_cancel_consumer_mid_mark_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When CancelController observed ``cancel_phase = 1`` before SIGTERM and
    already escalated + cancelled the consumer task into ``mark_cancelled``,
    ``orchestrate_shutdown``'s ``FORCING`` phase must not deliver a second
    ``task.cancel()`` that interrupts the in-flight ``mark_cancelled`` write
    and disowns the job."""
    settings = _load_worker_settings(monkeypatch, TASKQ_CANCELLATION_GRACE_PERIOD="0.35")
    deps = _MinimalDeps(settings)
    backend = _SlowTerminalBackend(write_delay=0.30)
    worker_id = new_uuid()

    enq = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job_id = enq.id
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]

    body_started = asyncio.Event()

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        body_started.set()
        await asyncio.sleep(60.0)
        return None

    consumer_task = asyncio.create_task(
        consume_one_job(
            backend=backend,
            job=job,
            worker_id=worker_id,
            run_actor=_run_actor,
            payload_type=_SlowPayload,
            actor_config=_MinimalActorConfig(),  # type: ignore[arg-type]
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
            deps=deps,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(body_started.wait(), timeout=2.0)

    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    fake_conn = _FakeCancelConn(backend, job_id, worker_id)

    # 1. Operator cancel is observed by CancelController BEFORE SIGTERM.
    await backend.write_cancel_request(job_id, reason="offboard")
    await controller.run_in_tx(fake_conn)
    entry = deps.active_jobs.get(job_id)
    assert entry is not None and entry.cancel_origin is CancelOrigin.OPERATOR

    # 2. SIGTERM starts orchestrate_shutdown 0.15s later so CancelController's
    #    cancel_grace (0.35s from T_cancel) lands well ahead of FORCING (0.40s
    #    from T_sigterm) while the 0.30s mark_cancelled write is in flight.
    await asyncio.sleep(0.15)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))
    shutdown_event = asyncio.Event()
    shut_task = asyncio.create_task(
        orchestrate_shutdown(
            deps,  # type: ignore[arg-type]
            settings,
            worker_id,
            shutdown_event,
            None,
            backend=backend,
        )
    )

    # 3. CancelController reaches cancel_grace (0.35s from T_cancel = 0.20s from
    #    T_sigterm) BEFORE FORCING (~0.40s from T_sigterm) and cancels the
    #    consumer into mark_cancelled.
    await asyncio.sleep(0.22)
    await controller.run_in_tx(fake_conn)
    await asyncio.wait_for(backend.mark_cancelled_entered.wait(), timeout=2.0)

    rc = await asyncio.wait_for(shut_task, timeout=5.0)
    try:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=2.0)

        final_row = await backend.get(job_id)
        assert rc == 0
        assert final_row is not None
        assert final_row.status == "cancelled"
        assert job_id not in deps.disowned_jobs, (
            "FORCING double-cancelled the consumer mid-mark_cancelled and disowned the job"
        )
    finally:
        await backend.drain_inflight()


async def test_take_to_register_shutdown_seam_cancels_operator_cancelled_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When SIGTERM lands while a claimed row carrying ``cancel_phase >= 1`` is
    in the take-to-register window, ``consume_one_job``'s shutdown seam must
    terminalise it ``cancelled`` instead of returning ``"scheduled"`` on
    ``mark_snoozed``'s cancel-fenced ``"noop"``."""
    settings = _load_worker_settings(monkeypatch)
    deps = _MinimalDeps(settings)
    deps.shutdown_phase = ShutdownPhase.DRAINING
    deps.shutdown_started_at = asyncio.get_running_loop().time()

    backend = InMemoryBackend(
        SystemClock(),
        actor_configs=[ActorConfig(actor="sys_slow", queue="default", max_concurrent=None)],
    )
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("body must not execute under shutdown")

    outcome = await consume_one_job(
        backend=backend,
        job=job,
        worker_id=worker_id,
        run_actor=_run_actor,
        payload_type=_SlowPayload,
        actor_config=_MinimalActorConfig(),  # type: ignore[arg-type]
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
        deps=deps,  # type: ignore[arg-type]
    )
    final_row = await backend.get(job.id)
    assert final_row is not None
    assert final_row.status == "cancelled", (
        f"shutdown seam stranded operator-cancelled row in status={final_row.status!r} "
        f"(outcome={outcome!r}, cancel_phase={final_row.cancel_phase})"
    )
    assert final_row.locked_by_worker is None


class _BlockingCancelBackend(InMemoryBackend):
    """Hold the fenced cancel write until the test releases its round trip."""

    def __init__(self) -> None:
        super().__init__(
            SystemClock(),
            actor_configs=[ActorConfig(actor="sys_slow", queue="default", max_concurrent=None)],
        )
        self.cancel_entered = asyncio.Event()
        self.cancel_release = asyncio.Event()
        self.cancel_finished = asyncio.Event()
        self._writes: list[asyncio.Task[Any]] = []

    async def mark_cancelled(self, *args: Any, **kwargs: Any) -> Any:
        current = asyncio.current_task()
        if current is not None:
            self._writes.append(current)
        self.cancel_entered.set()
        await self.cancel_release.wait()
        landed = await super().mark_cancelled(*args, **kwargs)
        self.cancel_finished.set()
        return landed

    async def drain_writes(self) -> None:
        pending = [task for task in self._writes if not task.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _assert_release_still_owned(task: asyncio.Task[Any], *, what: str) -> None:
    """A cancelled consumer must still own *what* until its write lands.

    The pre-registration rows have no ``active_jobs`` entry, so the consumer
    is the write's only owner. Detaching it at the cancellation ends the host
    task immediately (asyncio.shield semantics) and leaves the write running
    against a pool the deps teardown closes after that task is gone.
    """
    with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(task), timeout=0.25)
    assert not task.done(), (
        f"the consumer dropped ownership of the {what} at cancellation: the write "
        "is now detached from the pool the teardown closes after this task"
    )


async def test_shutdown_seam_cancel_write_survives_consumer_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new pre-registration terminal write must be shutdown-safe.

    Orchestration cannot see this unregistered row, so it may finish and let
    the worker's TaskGroup cancel the consumer while its fenced cancel write
    is still on the wire. The consumer owns that write through its own
    cancellation - the join that precedes the pool close therefore waits for
    it - instead of detaching it from the pool it still needs.
    """
    settings = _load_worker_settings(monkeypatch)
    deps = _MinimalDeps(settings)
    deps.shutdown_phase = ShutdownPhase.DRAINING
    deps.shutdown_started_at = asyncio.get_running_loop().time()
    backend = _BlockingCancelBackend()
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("body must not execute under shutdown")

    consumer_task = asyncio.create_task(
        consume_one_job(
            backend=backend,
            job=job,
            worker_id=worker_id,
            run_actor=_run_actor,
            payload_type=_SlowPayload,
            actor_config=_MinimalActorConfig(),  # type: ignore[arg-type]
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
            deps=deps,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(backend.cancel_entered.wait(), timeout=2.0)
    consumer_task.cancel()
    await _assert_release_still_owned(consumer_task, what="fenced cancel write")

    backend.cancel_release.set()
    try:
        await asyncio.wait_for(backend.cancel_finished.wait(), timeout=1.0)
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=2.0)
        assert consumer_task.cancelled(), "the held cancellation must still be re-raised"
        final_row = await backend.get(job.id)
        assert final_row is not None
        assert final_row.status == "cancelled", (
            "consumer teardown cancelled the only terminal writer for the "
            f"operator-cancelled row: status={final_row.status!r}"
        )
    finally:
        backend.cancel_release.set()
        await backend.drain_writes()
        await asyncio.gather(consumer_task, return_exceptions=True)


class _BlockingSnoozeBackend(_BlockingCancelBackend):
    """Hold the release write before its cancel fall-through can run."""

    def __init__(self) -> None:
        super().__init__()
        self.snooze_entered = asyncio.Event()
        self.snooze_release = asyncio.Event()
        self.snooze_finished = asyncio.Event()

    async def mark_snoozed(self, *args: Any, **kwargs: Any) -> Any:
        self.snooze_entered.set()
        await self.snooze_release.wait()
        try:
            return await super().mark_snoozed(*args, **kwargs)
        finally:
            self.snooze_finished.set()


async def test_shutdown_seam_release_and_cancel_writes_survive_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation during the release must not skip its cancel fall-through."""
    settings = _load_worker_settings(monkeypatch)
    deps = _MinimalDeps(settings)
    deps.shutdown_phase = ShutdownPhase.DRAINING
    deps.shutdown_started_at = asyncio.get_running_loop().time()
    backend = _BlockingSnoozeBackend()
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("body must not execute under shutdown")

    consumer_task = asyncio.create_task(
        consume_one_job(
            backend=backend,
            job=job,
            worker_id=worker_id,
            run_actor=_run_actor,
            payload_type=_SlowPayload,
            actor_config=_MinimalActorConfig(),  # type: ignore[arg-type]
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
            deps=deps,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(backend.snooze_entered.wait(), timeout=2.0)
    consumer_task.cancel()
    await _assert_release_still_owned(
        consumer_task, what="release write and its cancel fall-through"
    )

    backend.snooze_release.set()
    backend.cancel_release.set()
    try:
        await asyncio.wait_for(backend.cancel_finished.wait(), timeout=1.0)
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=2.0)
        final_row = await backend.get(job.id)
        assert final_row is not None
        assert final_row.status == "cancelled", (
            "cancelling the release write skipped the only remaining cancel "
            f"writer: status={final_row.status!r}"
        )
    finally:
        backend.snooze_release.set()
        backend.cancel_release.set()
        await backend.drain_writes()
        await asyncio.gather(consumer_task, return_exceptions=True)


class _PoolClosingSnoozeBackend(_BlockingSnoozeBackend):
    """Model the worker pool closing at the point deps teardown would."""

    def __init__(self) -> None:
        super().__init__()
        self.pool_closed = False
        self.cancel_wrote_before_close = False

    def close_pool(self) -> None:
        self.pool_closed = True

    async def mark_cancelled(self, *args: Any, **kwargs: Any) -> Any:
        if self.pool_closed:
            raise asyncpg.InterfaceError("pool closed before the cancel fallback")
        landed = await super().mark_cancelled(*args, **kwargs)
        self.cancel_wrote_before_close = bool(landed)
        return landed


class _DenyingRateLimitRegistry:
    """Acquire always denied: the acquire-time denial arm, nothing else."""

    async def acquire_for_actor(self, *_args: object, **_kwargs: object) -> list[object]:
        raise ReservationUnavailable(
            bucket_name="tb",
            retry_after=timedelta(seconds=5),
            source="rate_limit",
        )

    async def release_for_actor(self, *_args: object, **_kwargs: object) -> None:
        return None


async def test_shutdown_seam_cancel_fallback_survives_pool_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled release must be finished before the pool that carries it closes.

    ``_main``'s TaskGroup join completes before ``open_worker_deps`` closes the
    role pools, so a consumer that keeps ownership of its release through the
    cancellation also keeps the pool open until the fall-through's cancel
    write lands. A detached release has no such owner: the cancel write runs
    on after the join and meets a closing pool, and the row stays running
    under a worker that exits ``rc == 0`` with no successor to reclaim it.
    """
    settings = _load_worker_settings(monkeypatch)
    deps = _MinimalDeps(settings)
    deps.shutdown_phase = ShutdownPhase.DRAINING
    deps.shutdown_started_at = asyncio.get_running_loop().time()
    backend = _PoolClosingSnoozeBackend()
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("body must not execute under shutdown")

    consumer_task = asyncio.create_task(
        consume_one_job(
            backend=backend,
            job=job,
            worker_id=worker_id,
            run_actor=_run_actor,
            payload_type=_SlowPayload,
            actor_config=_MinimalActorConfig(),  # type: ignore[arg-type]
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
            deps=deps,  # type: ignore[arg-type]
        )
    )
    await asyncio.wait_for(backend.snooze_entered.wait(), timeout=2.0)
    consumer_task.cancel()
    # This is the join _main performs before open_worker_deps closes the pool:
    # the pool close below cannot precede the end of this task, so the release
    # has to be owned by it rather than detached from it.
    await _assert_release_still_owned(
        consumer_task, what="release write and its cancel fall-through"
    )
    backend.snooze_release.set()
    backend.cancel_release.set()
    try:
        await asyncio.wait_for(backend.cancel_finished.wait(), timeout=1.0)
        await asyncio.wait_for(backend.snooze_finished.wait(), timeout=1.0)
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=2.0)
        backend.close_pool()
        final_row = await backend.get(job.id)
        assert final_row is not None
        assert backend.cancel_wrote_before_close, (
            "the cancel fall-through landed after the pool closed: the release was "
            "detached from the pool the teardown closes after the TaskGroup join"
        )
        assert final_row.status == "cancelled", (
            f"pool teardown closed the only remaining cancel writer: status={final_row.status!r}"
        )
    finally:
        backend.snooze_release.set()
        backend.cancel_release.set()
        await backend.drain_writes()
        await asyncio.gather(consumer_task, return_exceptions=True)


async def test_acquire_denial_seam_cancels_operator_cancelled_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An acquire-time denial must not strand an operator-cancelled row.

    The denial arm's release write is the same ``mark_snoozed`` the shutdown
    seam uses, fenced on ``cancel_phase = 0``: on a row the operator cancelled
    it no-ops, and this path is still pre-registration, so the cancel ladder
    has no entry to walk. Left at the noop the row stays ``running`` under this
    worker's lock - the unheld walk later abandons it as ``failed`` /
    ``CancellationAbandoned``, or the worker dies first and the lease reclaim
    re-runs a body the operator cancelled.
    """
    settings = _load_worker_settings(monkeypatch)
    deps = _MinimalDeps(settings)
    backend = InMemoryBackend(
        SystemClock(),
        actor_configs=[ActorConfig(actor="sys_slow", queue="default", max_concurrent=None)],
    )
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("body must not run on an acquire-time denial")

    outcome = await consume_one_job(
        backend=backend,
        job=job,
        worker_id=worker_id,
        run_actor=_run_actor,
        payload_type=_SlowPayload,
        actor_config=default_actor_config(),  # type: ignore[arg-type]
        clock=SystemClock(),
        active_jobs=deps.active_jobs,
        deps=deps,  # type: ignore[arg-type]
        rate_limit_registry=_DenyingRateLimitRegistry(),  # type: ignore[arg-type]
        rate_limits=["tb"],
        reservations=[],
    )
    final_row = await backend.get(job.id)
    assert final_row is not None
    assert final_row.status == "cancelled", (
        f"acquire-denial seam stranded operator-cancelled row in status={final_row.status!r} "
        f"(outcome={outcome!r}, cancel_phase={final_row.cancel_phase})"
    )
    assert final_row.locked_by_worker is None
    assert job.id not in deps.disowned_jobs


async def test_acquire_denial_cancelled_during_release_still_terminalises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation during the denial's release must not skip its cancel fall-through.

    The acquire-time arm runs before registration too. Its ``mark_snoozed``
    write is shielded individually, but if the consumer is cancelled while
    that write is in flight, the consumer never resumes to observe ``noop``
    and call ``_cancel_fenced_unstarted``. The detached release therefore
    leaves the operator-cancelled row running unless the whole denial release
    owns the fall-through.
    """
    settings = _load_worker_settings(monkeypatch)
    deps = _MinimalDeps(settings)
    backend = _BlockingSnoozeBackend()
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _run_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("body must not run on an acquire-time denial")

    consumer_task = asyncio.create_task(
        consume_one_job(
            backend=backend,
            job=job,
            worker_id=worker_id,
            run_actor=_run_actor,
            payload_type=_SlowPayload,
            actor_config=default_actor_config(),  # type: ignore[arg-type]
            clock=SystemClock(),
            active_jobs=deps.active_jobs,
            deps=deps,  # type: ignore[arg-type]
            rate_limit_registry=_DenyingRateLimitRegistry(),  # type: ignore[arg-type]
            rate_limits=["tb"],
            reservations=[],
        )
    )
    await asyncio.wait_for(backend.snooze_entered.wait(), timeout=2.0)
    consumer_task.cancel()
    await _assert_release_still_owned(
        consumer_task, what="denial release and its cancel fall-through"
    )

    backend.snooze_release.set()
    backend.cancel_release.set()
    try:
        await asyncio.wait_for(backend.snooze_finished.wait(), timeout=1.0)
        # Settle past the release's cancel continuation, not just its first
        # write, before judging the row.
        await asyncio.wait_for(backend.cancel_finished.wait(), timeout=1.0)
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=2.0)
        final_row = await backend.get(job.id)
        assert final_row is not None
        assert final_row.status == "cancelled", (
            "cancelling the acquire-denial consumer during mark_snoozed skipped "
            f"the only remaining cancel writer: status={final_row.status!r}"
        )
    finally:
        backend.snooze_release.set()
        backend.cancel_release.set()
        await backend.drain_writes()
        await asyncio.gather(consumer_task, return_exceptions=True)


async def test_forcing_reissues_a_rolled_back_escalation_before_terminalising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A local FORCED stamp is not proof that PG holds phase 2.

    The cancel controller sets that stamp inside the heartbeat transaction;
    its own rollback recovery arm re-issues the escalation on a later tick.
    Shutdown can enter FORCING before that later tick. It must make the same
    re-issue attempt rather than letting the terminal write record a forced
    local cancellation as a cooperative database cancellation.
    """
    settings = _load_worker_settings(
        monkeypatch,
        TASKQ_CANCELLATION_GRACE_PERIOD="0",
        TASKQ_CLEANUP_GRACE_PERIOD="0",
    )
    deps = _MinimalDeps(settings)
    backend = InMemoryBackend(
        SystemClock(),
        actor_configs=[ActorConfig(actor="sys_slow", queue="default", max_concurrent=None)],
    )
    worker_id = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="sys_slow",
            queue="default",
            payload={"sleep": 60.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    job = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(job.id, reason="offboard")

    async def _running_attempt() -> None:
        await asyncio.Event().wait()

    attempt_task = asyncio.create_task(_running_attempt())
    ctx = JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        claim_epoch=job.claim_epoch,
        worker_id=worker_id,
        payload=_SlowPayload(),
        jobs=None,  # type: ignore[arg-type]  # Why: no sub-enqueue occurs on this registry-only shutdown path.
        log=shutdown_mod._log,  # pyright: ignore[reportPrivateUsage]  # Why: reuse the production logger for this minimal context.
    )
    entry = await deps.active_jobs.register(job.id, attempt_task, ctx)
    # The controller's phase-2 write landed in memory, then its heartbeat
    # transaction rolled back: local FORCED, durable COOPERATIVE.
    entry.cancel_phase = CancelPhase.FORCED
    entry.cancel_origin = CancelOrigin.OPERATOR
    entry.ctx._set_cancel_origin(CancelOrigin.OPERATOR)  # pyright: ignore[reportPrivateUsage]  # Why: mirror the controller's paired origin stamp.
    entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0

    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))
    try:
        rc = await orchestrate_shutdown(
            deps,  # type: ignore[arg-type]
            settings,
            worker_id,
            asyncio.Event(),
            None,
            backend=backend,
        )
        final_row = await backend.get(job.id)
        assert rc == 0
        assert final_row is not None
        assert final_row.status == "cancelled"
        assert final_row.cancel_phase == CancelPhase.FORCED
        assert final_row.error_class == CANCEL_ORIGIN_FORCED
    finally:
        attempt_task.cancel()
        await asyncio.gather(attempt_task, return_exceptions=True)


_SIG_CLOSE_HOOK_PRELUDE = """
import asyncio, asyncio.unix_events, os, signal

_sent = False
_orig_remove = asyncio.unix_events._UnixSelectorEventLoop.remove_signal_handler
_orig_close = asyncio.unix_events._UnixSelectorEventLoop.close

def _hooked_remove(self, sig):
    global _sent
    res = _orig_remove(self, sig)
    if sig == signal.SIGTERM and not _sent:
        _sent = True
        os.kill(os.getpid(), signal.SIGTERM)
    return res

def _hooked_close(self):
    global _sent
    res = _orig_close(self)
    if not _sent:
        _sent = True
        os.kill(os.getpid(), signal.SIGTERM)
    return res

asyncio.unix_events._UnixSelectorEventLoop.remove_signal_handler = _hooked_remove
asyncio.unix_events._UnixSelectorEventLoop.close = _hooked_close
"""


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="requires POSIX signal masks and asyncio's Unix event loop",
)
def test_system_e2e_worker_entry_survives_sigterm_during_loop_close() -> None:
    """A late SIGTERM arriving inside ``asyncio.Runner.close()`` / ``loop.close()``
    (after ``_main`` returned ``0``) must not kill ``tests.system_e2e._worker_entry``
    with ``rc = -15``."""
    script = (
        _SIG_CLOSE_HOOK_PRELUDE
        + """
import runpy, taskq.worker.run as _run

async def _fake_main(*args, **kwargs):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: None)
    return 0

_run._main = _fake_main
runpy.run_module("tests.system_e2e._worker_entry", run_name="__main__")
"""
    )
    env = {
        **os.environ,
        "TASKQ_PG_DSN": "postgresql://u:p@localhost/db",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_WATCHDOG_ENABLED": "false",
    }
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, (
        f"_worker_entry died rc={proc.returncode} on late SIGTERM during loop.close(); "
        f"stderr={proc.stderr!r}"
    )


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="requires POSIX signal masks and asyncio's Unix event loop",
)
def test_cli_worker_entrypoint_survives_sigterm_during_loop_close() -> None:
    """A late SIGTERM arriving inside ``asyncio.Runner.close()`` / ``loop.close()``
    (after ``_main`` returned ``0``) must not kill ``taskq worker`` with ``rc = -15``."""
    script = (
        _SIG_CLOSE_HOOK_PRELUDE
        + """
import sys
import taskq.worker._bootstrap as _bootstrap
from taskq.cli import app

async def _fake_main(*args, **kwargs):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: None)
    return 0

_bootstrap._main = _fake_main
sys.argv = ["taskq", "worker", "--actors", "tests.system_e2e.actors:ACTORS"]
try:
    app(standalone_mode=True)
except SystemExit as exc:
    sys.exit(exc.code)
"""
    )
    env = {
        **os.environ,
        "TASKQ_PG_DSN": "postgresql://u:p@localhost/db",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_WATCHDOG_ENABLED": "false",
    }
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, (
        f"taskq worker CLI died rc={proc.returncode} on late SIGTERM during loop.close(); "
        f"stderr={proc.stderr!r}"
    )
