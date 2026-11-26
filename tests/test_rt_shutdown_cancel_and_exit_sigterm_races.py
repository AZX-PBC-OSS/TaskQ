"""Regression pins for two system-e2e shutdown races.

1. Cancel-racing-SIGTERM strand (Flake A), now closed by two gates:
   - ``CancelController`` vs ``orchestrate_shutdown`` (``FORCING``): a
     ``SIGTERM`` landing before ``CancelController`` observed
     ``cancel_phase = 1`` used to leave the row on the shutdown's routing.
     ``CANCELLING`` stamped ``cancel_phase = COOPERATIVE`` and
     ``cancel_origin = SHUTDOWN``, and the phase-1 arm was gated on
     ``active.cancel_phase < COOPERATIVE``, so it skipped the operator
     takeover. At ``cancellation_grace_period``, when ``CancelController``
     fired Phase 2 just ahead of ``FORCING``'s 0.1s poll, the consumer
     entered ``mark_interrupted`` instead of the operator's own verdict, and
     ``FORCING`` then unconditionally called ``active.task.cancel()`` a
     second time, interrupting the in-flight terminal write, disowning the
     job, skipping ``mark_cancelled``, and deregistering from
     ``active_jobs`` - so ``orchestrate_shutdown`` exited ``rc=0`` with the
     row stranded ``running``.
   - The shipped gates: the phase-1 arm's predicate is
     ``or active.cancel_origin is not CancelOrigin.OPERATOR`` (a row already
     at ``COOPERATIVE`` still takes the operator takeover), and every
     delivery goes through ``deliver_first_cancel`` (first delivery only, so
     ``FORCING`` cannot interrupt a consumer already unwinding its terminal
     write).
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
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

import taskq.obs._otel as otel_mod
import taskq.worker.cancel as cancel_mod
import taskq.worker.shutdown as shutdown_mod
from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.backend._protocol import Backend, CancelPhase, EnqueueArgs, JobId
from taskq.backend.clock import SystemClock
from taskq.constants import CANCEL_ORIGIN_FORCED
from taskq.context import CancelOrigin, JobContext
from taskq.exceptions import ReservationUnavailable
from taskq.obs import get_logger
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import (
    _cancel_fenced_unstarted,
    _release_shutdown_unstarted,
    consume_one_job,
)
from taskq.worker.cancel import ActiveJobRegistry, make_cancel_controller
from taskq.worker.run import _release_unstarted_claim_write
from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown
from tests._ns_patch import module_ns_proxy
from tests.test_shutdown_orchestrator import (
    FakeActiveJobRegistry,
    FakeClock,
    _make_deps,
    _make_fake_active_job,
    _patch_clock,
)
from tests.test_shutdown_orchestrator import (
    _worker_settings as _orchestrator_settings,
)
from tests.test_worker_di_bootstrap import _running_unstarted_job, _stub_deps


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
    real Postgres round-trip under load during shutdown.

    ``release_mark_cancelled`` turns the cancel write into a gate: the write
    announces itself through ``mark_cancelled_entered`` and then blocks until
    the test releases it. That holds the terminal write open across the
    orchestrator's FORCING delivery pass without any timing assumption, so
    the test observes the real interleaving instead of racing a sleep.
    ``phase_probe`` records the shutdown phase at the instant the write
    entered, proving the race actually happened before FORCING.
    """

    def __init__(
        self,
        *,
        write_delay: float = 0.15,
        release_mark_cancelled: asyncio.Event | None = None,
        phase_probe: Callable[[], ShutdownPhase] | None = None,
    ) -> None:
        super().__init__(
            SystemClock(),
            actor_configs=[ActorConfig(actor="sys_slow", queue="default", max_concurrent=None)],
        )
        self._write_delay = write_delay
        self._release_mark_cancelled = release_mark_cancelled
        self._phase_probe = phase_probe
        self.mark_interrupted_calls = 0
        self.mark_cancelled_calls = 0
        self.mark_cancelled_entered = asyncio.Event()
        self.mark_cancelled_phase: ShutdownPhase | None = None
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
        if self._phase_probe is not None:
            self.mark_cancelled_phase = self._phase_probe()
        self.mark_cancelled_entered.set()
        if self._release_mark_cancelled is not None:
            await self._release_mark_cancelled.wait()
        else:
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


async def _wait_for_forcing_delivery_summary(
    logs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Wait until FORCING's post-delivery summary is in the captured stream."""
    while True:
        for record in logs:
            if (
                record.get("event") == "shutdown-phase"
                and record.get("phase") == "FORCING"
                and "deliveries_refused" in record
            ):
                return record
        await asyncio.sleep(0.005)


async def test_cancel_controller_and_forcing_race_terminalises_operator_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When SIGTERM arrives before CancelController sees an operator cancel,
    and CancelController's phase-2 tick lands just ahead of FORCING, the job
    must still terminalise ``cancelled`` and never be disowned as ``running``.

    The terminal write is held open on a gate until FORCING's delivery pass has
    run, so the interleaving is observed rather than raced: a fixed sleep let a
    loaded host finish the write before FORCING woke, and the pin then passed
    on the double-cancel code.
    """
    settings = _load_worker_settings(monkeypatch, TASKQ_CANCELLATION_GRACE_PERIOD="0.205")
    deps = _MinimalDeps(settings)
    release_mark_cancelled = asyncio.Event()
    backend = _SlowTerminalBackend(
        release_mark_cancelled=release_mark_cancelled,
        phase_probe=lambda: deps.shutdown_phase,
    )
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
    with structlog.testing.capture_logs() as logs:
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
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        # The phase-2 escalation is due once cancel_grace has elapsed. Backdate
        # the observation rather than racing a fixed sleep, so the tick lands
        # strictly inside CANCELLING and the race is reproducible by
        # construction.
        entry.cancel_observed_at = asyncio.get_running_loop().time() - (
            settings.cancellation_grace_period + 0.01
        )
        try:
            # The tick escalates and delivers the first cancel; the terminal
            # write announces itself and blocks on the gate.
            await controller.run_in_tx(fake_conn)
            await asyncio.wait_for(backend.mark_cancelled_entered.wait(), timeout=2.0)
            assert backend.mark_cancelled_phase is ShutdownPhase.CANCELLING, (
                "the terminal write entered after FORCING had already started: "
                "this run did not reproduce the cancel-racing-SIGTERM ordering"
            )
            summary = await asyncio.wait_for(_wait_for_forcing_delivery_summary(logs), timeout=5.0)
            assert summary["deliveries_refused"] == 1, (
                "FORCING's delivery pass ran while the consumer was mid-write "
                f"and did not refuse the second delivery: {summary!r}"
            )
        finally:
            # FORCING's delivery pass has run (or an assertion failed): let the
            # held write finish so the consumer settles and the orchestrator
            # joins.
            release_mark_cancelled.set()

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


# ── Cancel counters, wire seq, and release-failure signals ─────────────


class _CountingCounter:
    """Stand-in for the ``taskq.jobs.cancels_actored`` instrument."""

    def __init__(self) -> None:
        self.points: list[dict[str, object]] = []

    def add(self, amount: int, attributes: dict[str, object] | None = None) -> None:
        self.points.append({"amount": amount, "attributes": dict(attributes or {})})


class _LadderRegistry(FakeActiveJobRegistry):
    """The orchestrator harness's registry, plus the reads the cancel ladder makes."""

    def get(self, job_id: object) -> object:
        for job in self._jobs:
            if job.job_id == job_id:
                return job
        return None

    def held_ids(self) -> set[JobId]:
        return {job.job_id for job in self._jobs}

    def queued_ids(self) -> set[JobId]:
        return set()


class _ForcedRowConn:
    """The heartbeat connection once the row is durably at ``cancel_phase = 2``."""

    def __init__(self, job_id: JobId) -> None:
        self._job_id = job_id

    async def fetch(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
        return [{"id": self._job_id, "cancel_phase": 2}]

    async def execute(self, sql: str, *args: object) -> str:
        return "UPDATE 0"


async def test_forcing_probe_counts_the_operator_cancel_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The FORCING probe is a fourth first-observer, so it must count the request.

    When the operator's request lands inside the last heartbeat interval of
    the CANCELLING grace, the shutdown orchestrator's escalation write is the
    first to observe it: the row still carries ``cancel_phase = 1``, the probe
    restamps the origin OPERATOR, and the attempt is cancelled. The
    controller's arms are closed afterwards (the OPERATOR stamp, the FORCED
    phase), so a cancel the worker demonstrably acted on would otherwise
    increment ``taskq.jobs.cancels_actored_total`` zero times, permanently.
    """
    counter = _CountingCounter()
    monkeypatch.setattr(otel_mod, "_cancels_actored", counter)

    job = _make_fake_active_job(job_id=new_uuid())
    registry = _LadderRegistry([job])
    settings = _orchestrator_settings(cancellation_grace=0.05, cleanup_grace=0.05)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    # The probe lands: the row still carried cancel_phase = 1, an operator
    # request that appeared between the cancel ladder's poll and this write.
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_interrupted = AsyncMock(return_value="scheduled")
    backend.mark_cancelled = AsyncMock(return_value=True)

    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))
    _patch_clock(monkeypatch, FakeClock(), Mock())

    with structlog.testing.capture_logs() as logs:
        rc = await orchestrate_shutdown(
            deps, settings, new_uuid(), asyncio.Event(), None, backend=backend
        )

    assert rc == 0
    assert [r for r in logs if r.get("event") == "force-cancel-escalation-observed-operator-cancel"]
    assert job.cancel_origin is CancelOrigin.OPERATOR
    assert job.cancel_phase >= CancelPhase.FORCED
    assert sum(int(p["amount"]) for p in counter.points) == 1, counter.points
    assert counter.points[0]["attributes"] == {"actor": job.ctx.actor}

    # No later tick recovers or doubles it: the controller re-walks the row it
    # no longer recognises as a fresh operator request.
    fake_loop = Mock()
    fake_loop.time = lambda: 100.0
    monkeypatch.setattr(
        cancel_mod,
        "asyncio",
        module_ns_proxy(asyncio, get_running_loop=lambda: fake_loop),
    )
    controller = make_cancel_controller(deps, new_uuid(), backend)
    await controller.run_in_tx(cast("Any", _ForcedRowConn(job.job_id)))
    assert sum(int(p["amount"]) for p in counter.points) == 1, counter.points


class _FakeRedis:
    """Record the state-change events a publish site emits."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def publish(self, channel: str, payload: str) -> int:
        import json

        self.published.append({"channel": channel, **json.loads(payload)})
        return 1


async def test_cancel_fall_through_publishes_the_consumed_seq() -> None:
    """The terminal event must carry the seq the write consumes, not the head.

    A row that reported progress on an earlier attempt keeps its durable
    ``progress_seq`` as the stream head. A terminal event consumes one past it
    (``head + 1``), exactly as every other terminal write does; publishing the
    head itself (or, worse, the bufferless default ``0``) is behind events a
    seq-cursor subscriber already consumed, so the cancellation would be
    dropped as a duplicate while Postgres correctly says the row is terminal.
    """
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
            payload={"sleep": 0.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    claimed = (await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=10)))[0]
    await backend.write_cancel_request(claimed.id, reason="offboard")
    stored = await backend.get(claimed.id)
    assert stored is not None
    backend._jobs[claimed.id] = replace(stored, progress_seq=7)  # pyright: ignore[reportPrivateUsage]  # Why: the store's own map seeds the durable seq a retried row would carry.
    job = await backend.get(claimed.id)
    assert job is not None

    redis = _FakeRedis()
    landed = await _cancel_fenced_unstarted(
        backend,
        job,
        worker_id,
        log=get_logger("taskq.test"),
        redis_client=redis,  # type: ignore[arg-type]  # Why: the fake implements publish, the only method the publisher calls.
        settings=SimpleNamespace(  # type: ignore[arg-type]  # Why: the publish path reads schema_name and progress_publish_global only.
            schema_name="taskq", progress_publish_global=False
        ),
        progress_buffers={},
        disowned_jobs=set(),
    )

    assert landed
    terminal_seq = job.progress_seq + 1
    persisted = await backend.get(job.id)
    assert persisted is not None
    assert persisted.progress_seq == terminal_seq, (
        "the fenced write must SET the consumed seq durably: "
        f"row has progress_seq={persisted.progress_seq}, expected {terminal_seq}"
    )
    cancelled = [event for event in redis.published if event.get("status") == "cancelled"]
    assert cancelled, "no terminal state-change event was published"
    assert cancelled[0]["seq"] == terminal_seq, (
        "the published terminal event carries seq="
        f"{cancelled[0]['seq']} while the row's stream head was {job.progress_seq} "
        f"(the consumed seq is {terminal_seq}): a seq-cursor subscriber discards "
        "any event at or below its last emitted seq"
    )


async def test_shutdown_release_failure_carries_its_cause() -> None:
    """The shutdown seam's release failure must carry the exception.

    On this arm a failed ``mark_snoozed`` disowns the row: it stays
    ``running`` under this worker's lock until the lease reclaim, the strand
    shape the release exists to prevent. The record must therefore name the
    cause beside the taxonomy, or the responder cannot tell a closed pool
    from a constraint violation.
    """
    job = _running_unstarted_job(actor="test_actor")
    backend = AsyncMock(spec=Backend)
    backend.mark_snoozed = AsyncMock(side_effect=RuntimeError("pool is closed"))
    disowned: set[UUID] = set()

    with structlog.testing.capture_logs() as logs:
        outcome = await _release_shutdown_unstarted(
            backend,
            job,
            new_uuid(),
            log=get_logger("taskq.test"),
            redis_client=None,
            settings=None,
            progress_buffers=None,
            disowned_jobs=disowned,
        )

    assert outcome == "scheduled"
    assert job.id in disowned
    record = next(r for r in logs if r.get("event") == "shutdown-release-failed")
    assert record.get("kind") == "shutdown_release_failed"
    assert record.get("error_class") == "infra"
    carried = any(key in record for key in ("exc_info", "error_type", "error", "exception.message"))
    assert carried, (
        "shutdown-release-failed records the failure and discards the exception "
        f"that caused it: {record!r}"
    )


async def test_di_release_failure_carries_the_release_failure_taxonomy() -> None:
    """The DI twin of the same failure must carry the same queryable fields.

    One failure, one shape: the literal failure event plus ``kind`` and
    ``error_class="infra"`` beside the cause, so a query filtering either
    field finds both the consumer's and the dispatch side's release failures.
    """
    settings = _orchestrator_settings()
    deps = _stub_deps(settings)
    job = _running_unstarted_job(actor="test_actor")
    backend = AsyncMock(spec=Backend)
    backend.mark_snoozed = AsyncMock(side_effect=RuntimeError("pool is closed"))

    with structlog.testing.capture_logs() as logs:
        outcome = await _release_unstarted_claim_write(
            backend,
            job,
            new_uuid(),
            deps,  # type: ignore[arg-type]  # Why: the helper reads settings, redis_client, progress_buffers, and disowned_jobs, all present on the stub.
            event_prefix="dispatch-actor-not-found",
            snooze_for=timedelta(seconds=10),
            metadata_update={"released_reason": "actor-not-found"},
        )

    assert outcome is None
    assert job.id in deps.disowned_jobs
    record = next(r for r in logs if r.get("event") == "unstarted-claim-release-failed")
    assert record.get("error_class") == "infra"
    assert record.get("kind") == "dispatch_actor_not_found_release_failed"
    carried = any(key in record for key in ("exc_info", "error_type", "error", "exception.message"))
    assert carried, f"the release-failure record discards its cause: {record!r}"


async def _run_forcing_stream(
    monkeypatch: pytest.MonkeyPatch, *, cancelling: int
) -> tuple[list[dict[str, object]], MagicMock]:
    """One shutdown over a single registered attempt, at FORCING's delivery.

    ``cancelling`` is the attempt's ``Task.cancelling()``: ``0`` is an attempt
    that has not taken a cancellation yet (FORCING delivers), ``1`` is one
    already unwinding its terminal write (FORCING refuses). Everything else is
    identical, so any difference in the emitted stream is attributable to the
    delivery decision alone.
    """
    job = _make_fake_active_job(job_id=new_uuid())
    task = cast("MagicMock", job.task)
    task.done.return_value = False
    task.cancelling.return_value = cancelling
    registry = FakeActiveJobRegistry([job])
    settings = _orchestrator_settings(cancellation_grace=0.05, cleanup_grace=0.05)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    # The probe declines: this row carries no operator cancel.
    backend.write_cancel_escalation = AsyncMock(return_value=False)
    backend.mark_interrupted = AsyncMock(return_value="scheduled")

    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))
    _patch_clock(monkeypatch, FakeClock(), Mock())

    with structlog.testing.capture_logs() as logs:
        rc = await orchestrate_shutdown(
            deps, settings, new_uuid(), asyncio.Event(), None, backend=backend
        )
    assert rc == 0
    return [dict(record) for record in logs], task


async def test_a_refused_forcing_delivery_is_visible(monkeypatch: pytest.MonkeyPatch) -> None:
    """The FORCING delivery decision must be reconstructable from the stream.

    Refusing a second delivery is a deliberate degradation (the attempt is
    mid-terminal-write). A delivered cancel the actor swallowed and a refused
    one have opposite remedies, so the stream has to say which branch ran.
    """
    delivered, delivered_task = await _run_forcing_stream(monkeypatch, cancelling=0)
    refused, refused_task = await _run_forcing_stream(monkeypatch, cancelling=1)

    delivered_task.cancel.assert_called_once()
    refused_task.cancel.assert_not_called()
    assert delivered != refused, (
        "the FORCING delivery was refused for an attempt already unwinding its "
        "terminal write, and the shutdown's log stream is identical to the one "
        "where the cancel was delivered"
    )
    refused_records = [r for r in refused if r.get("event") == "force-cancel-delivery-refused"]
    assert refused_records, refused
    assert refused_records[0]["deliveries_refused"] == 1
    summaries = [
        r
        for r in refused
        if r.get("event") == "shutdown-phase"
        and r.get("phase") == "FORCING"
        and "deliveries_refused" in r
    ]
    assert summaries, "the FORCING phase emits no delivery summary"
    assert summaries[-1]["deliveries_refused"] == 1
    assert summaries[-1]["deliveries_delivered"] == 0


async def test_di_release_fall_through_returns_cancelled_and_publishes() -> None:
    """A landed cancel fall-through is reported and published, not a noop.

    The dispatch-side release is the same operation as the consumer's: when
    ``mark_snoozed`` fences out an operator-cancelled row, the fenced
    ``mark_cancelled`` terminalises it. The caller must read ``"cancelled"``
    (not the snooze's ``"noop"``), and the wire event must carry the row's
    durable seq and ``terminal=True`` so watchers see the verdict.
    """
    settings = _orchestrator_settings()
    settings.progress_publish_global = False
    deps = _stub_deps(settings)
    redis = _FakeRedis()
    deps.redis_client = redis  # type: ignore[assignment]  # Why: the fake implements publish, the only method the publisher calls.
    job = replace(
        _running_unstarted_job(actor="test_actor", cancel_phase=CancelPhase.COOPERATIVE),
        progress_seq=7,
    )
    backend = AsyncMock(spec=Backend)
    backend.mark_snoozed = AsyncMock(return_value="noop")
    backend.mark_cancelled = AsyncMock(return_value=True)

    outcome = await _release_unstarted_claim_write(
        backend,
        job,
        new_uuid(),
        deps,  # type: ignore[arg-type]  # Why: the helper reads settings, redis_client, progress_buffers, and disowned_jobs, all present on the stub.
        event_prefix="dispatch-actor-not-found",
        snooze_for=timedelta(seconds=10),
        metadata_update={"released_reason": "actor-not-found"},
    )

    assert outcome == "cancelled"
    backend.mark_cancelled.assert_awaited_once()
    assert backend.mark_cancelled.await_args is not None
    assert backend.mark_cancelled.await_args.kwargs["progress_seq"] == job.progress_seq + 1
    cancelled = [event for event in redis.published if event.get("status") == "cancelled"]
    assert cancelled, "the landed cancel was not published"
    assert cancelled[0]["terminal"] is True
    assert cancelled[0]["seq"] == job.progress_seq + 1


async def test_failed_snooze_release_publishes_terminal_failed_at_consumed_seq() -> None:
    """A deadline-exceeded snooze is terminal and must publish as such.

    ``mark_snoozed`` returns ``"failed"`` on its deadline arm, which
    terminalises the row. The publish must say ``status="failed",
    terminal=True`` at the consumed seq (``job.progress_seq + 1``), not a
    non-terminal ``"scheduled"`` at ``seq=0``.
    """
    settings = _orchestrator_settings()
    settings.progress_publish_global = False
    deps = _stub_deps(settings)
    redis = _FakeRedis()
    deps.redis_client = redis  # type: ignore[assignment]  # Why: the fake implements publish, the only method the publisher calls.
    job = replace(_running_unstarted_job(actor="test_actor"), progress_seq=7)
    backend = AsyncMock(spec=Backend)
    backend.mark_snoozed = AsyncMock(return_value="failed")

    outcome = await _release_unstarted_claim_write(
        backend,
        job,
        new_uuid(),
        deps,  # type: ignore[arg-type]  # Why: the helper reads settings, redis_client, progress_buffers, and disowned_jobs, all present on the stub.
        event_prefix="dispatch-actor-not-found",
        snooze_for=timedelta(seconds=10),
        metadata_update={"released_reason": "actor-not-found"},
    )

    assert outcome == "failed"
    backend.mark_snoozed.assert_awaited_once()
    assert backend.mark_snoozed.await_args is not None
    assert backend.mark_snoozed.await_args.kwargs["progress_seq"] == job.progress_seq + 1
    published = [event for event in redis.published if event.get("status") == "failed"]
    assert published, f"the terminal snooze published nothing: {redis.published!r}"
    assert published[0]["terminal"] is True
    assert published[0]["seq"] == job.progress_seq + 1
