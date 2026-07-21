"""Integration tests for the cancellation hook against a real Postgres.

cancellation flow, recovery sweeps.
integration tests, chaos tests required for cancellation.
"""

import asyncio
import contextlib
import math
import threading
import time
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq._json import loads
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.client._jobs import JobsClient
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.settings import make_integration_settings
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import CancelController, make_cancel_controller
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend

pytestmark = pytest.mark.integration


# ── Infrastructure ─────────────────────────────────────────────────────────


@asynccontextmanager
async def _test_infra(
    pg_dsn: str, worker_id: UUID, **settings_overrides: str
) -> AsyncGenerator[tuple[WorkerDeps, "PostgresBackend", WorkerSettings], None]:
    from taskq.backend.postgres import PostgresBackend as Backend

    settings = make_integration_settings(pg_dsn, **settings_overrides)
    assert settings.pg_dsn_direct is not None

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
        await conn.execute(
            f'INSERT INTO "{settings.schema_name}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608 # Why: schema identifier from WorkerSettings validated against _IDENT_RE; asyncpg has no parameter binding for identifiers
            worker_id,
            "test",
            12345,
            ["default"],
        )
    finally:
        await conn.close()

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))

    deps.settings.heartbeat_interval = 0.2

    cancellation_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
    cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)

    backend: Backend = Backend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=cancellation_grace,
        cleanup_grace_period=cleanup_grace,
    )

    try:
        yield deps, backend, settings
    finally:
        await stack.aclose()


def _now_utc() -> datetime:
    return datetime.now(UTC)


async def _enqueue_and_dispatch(
    client: JobsClient,
    backend: "PostgresBackend",
    worker_id: UUID,
    lock_lease: timedelta,
) -> tuple[JobId, JobRow]:
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_now_utc(),
    )
    row = await backend.enqueue(args)
    job_id = row.id

    schema = backend._schema_name
    async with backend._worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'running', locked_by_worker = $1, lock_expires_at = now() + $2::interval, started_at = now(), last_heartbeat_at = now() WHERE id = $3",  # noqa: S608 # Why: schema identifier from validated WorkerSettings; asyncpg has no parameter binding for identifiers
            worker_id,
            lock_lease,
            job_id,
        )

    job = await backend.get(job_id)
    assert job is not None
    assert job.status == "running"
    return job_id, job


async def _wait_for_cancel_phase(
    backend: "PostgresBackend",
    job_id: JobId,
    phase: int,
    timeout: float = 5.0,  # noqa: ASYNC109 # Why: polling-loop timeout for _wait_for_cancel_phase; not an anyio cancel scope
) -> JobRow:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await backend.get(job_id)
        if row is not None and row.cancel_phase >= phase:
            return row
        await asyncio.sleep(0.05)
    row = await backend.get(job_id)
    assert row is not None
    raise TimeoutError(
        f"cancel_phase did not reach {phase} within {timeout}s (current={row.cancel_phase})"
    )


async def _poll_until_status(
    backend: "PostgresBackend",
    job_id: JobId,
    statuses: set[str],
    timeout: float = 10.0,  # noqa: ASYNC109 # Why: polling-loop timeout for _poll_until_status; not an anyio cancel scope
) -> JobRow:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await backend.get(job_id)
        if row is not None and row.status in statuses:
            return row
        await asyncio.sleep(0.05)
    row = await backend.get(job_id)
    assert row is not None
    raise TimeoutError(f"status did not reach {statuses} within {timeout}s (current={row.status})")


async def _read_job_events(backend: "PostgresBackend", job_id: JobId) -> list[dict[str, object]]:
    schema = backend._schema_name
    async with backend._worker_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',  # noqa: S608 # Why: schema identifier from validated WorkerSettings; asyncpg has no parameter binding for identifiers
            job_id,
        )
    result: list[dict[str, object]] = []
    for rec in rows:
        raw = rec["detail"]
        detail = loads(raw) if isinstance(raw, str) else raw
        result.append({"kind": rec["kind"], "detail": detail})
    return result


async def _read_job_attempts(backend: "PostgresBackend", job_id: JobId) -> list[dict[str, object]]:
    schema = backend._schema_name
    async with backend._worker_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT outcome, worker_id FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',  # noqa: S608 # Why: schema identifier from validated WorkerSettings; asyncpg has no parameter binding for identifiers
            job_id,
        )
    return [{"outcome": rec["outcome"], "worker_id": rec["worker_id"]} for rec in rows]


# ── Chaos helpers ──────────────────────────────────────────────────


class _FlakyConnection:
    def __init__(self, real_conn: Any, error_sql_trigger: str, owner: "_ChaosPool") -> None:
        self._conn = real_conn
        self._trigger = error_sql_trigger
        self._owner = owner

    async def execute(self, sql: str, *args: object, **kwargs: Any) -> str:
        if not self._owner._error_has_fired and self._trigger in sql:
            self._owner._error_has_fired = True
            raise asyncpg.exceptions.PostgresConnectionError("simulated PG connection drop")
        return await self._conn.execute(sql, *args, **kwargs)

    async def fetch(self, sql: str, *args: object, **kwargs: Any) -> list[asyncpg.Record]:
        return await self._conn.fetch(sql, *args, **kwargs)

    async def fetchrow(self, sql: str, *args: object, **kwargs: Any) -> asyncpg.Record | None:
        return await self._conn.fetchrow(sql, *args, **kwargs)

    async def fetchval(self, sql: str, *args: object, **kwargs: Any) -> Any:
        return await self._conn.fetchval(sql, *args, **kwargs)

    def transaction(self) -> Any:
        return self._conn.transaction()


class _ChaosPool:
    def __init__(self, real_pool: asyncpg.Pool) -> None:
        self._real = real_pool
        self._error_sql_trigger: str = ""
        self._error_has_fired: bool = False

    def set_error_trigger(self, sql_trigger: str) -> None:
        self._error_sql_trigger = sql_trigger
        self._error_has_fired = False

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[object, None]:  # noqa: ASYNC109 # Why: timeout is passed through to asyncpg's Pool.acquire; not a real anyio cancel scope
        async with self._real.acquire(timeout=timeout) as conn:
            if self._error_sql_trigger:
                yield _FlakyConnection(conn, self._error_sql_trigger, self)
            else:
                yield conn


# Derived from src/taskq/backend/postgres.py:84-119 (_SWEEP_1_SQL).
# TODO: switch to the public helper once one is exposed.
_SWEEP_1_SQL_TEST = """\
WITH snap AS (
    SELECT id, locked_by_worker
    FROM \"{schema}\".jobs
    WHERE status = 'running'
      AND lock_expires_at < now()
      AND (cancel_phase = 0
           OR lock_expires_at < now() - $1::interval - $2::interval - interval '60 seconds')
    FOR UPDATE SKIP LOCKED
)
UPDATE \"{schema}\".jobs j
SET status = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN 'pending'::\"{schema}\".job_status
        ELSE 'crashed'::\"{schema}\".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    scheduled_at = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN now() + interval '5 seconds'
        ELSE j.scheduled_at
    END,
    finished_at = CASE
        WHEN NOT (j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable')
            THEN now()
        ELSE j.finished_at
    END
FROM snap
WHERE j.id = snap.id
RETURNING j.id, j.status, j.attempt, j.started_at, snap.locked_by_worker"""


# ════════════════════════════════════════════════════════════════════════════
# End-to-end cooperative cancel
# ════════════════════════════════════════════════════════════════════════════


async def test_cooperative_cancel(pg_dsn: str) -> None:
    """End-to-end cooperative cancel."""
    worker_id = new_uuid()
    async with _test_infra(pg_dsn, worker_id) as (deps, backend, settings):
        client = JobsClient(backend)
        lock_lease = timedelta(seconds=settings.lock_lease)
        job_id, job = await _enqueue_and_dispatch(client, backend, worker_id, lock_lease)

        cancel_seen = asyncio.Event()

        async def actor(_job: JobRow, ctx: JobContext[BaseModel]) -> str:
            while not ctx.cancellation_requested:  # noqa: ASYNC110 # Why: intentional poll loop checking cancellation_requested; observable test behaviour
                await asyncio.sleep(0.01)
            cancel_seen.set()
            return "exit"

        actor_config = StubActorConfig(retry=RetryPolicy(kind="non_retryable", max_attempts=1))

        shutdown = asyncio.Event()
        controller: CancelController = make_cancel_controller(deps, worker_id, backend)
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job,
                    worker_id,
                    run_actor=actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)
                result = await client.cancel(job_id)
                assert result.cancellation_initiated is True
                await asyncio.wait_for(consumer_task, timeout=5.0)
            finally:
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            shutdown.set()
            await heartbeat_task

        assert cancel_seen.is_set()
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.cancel_phase == 1

        attempts = await _read_job_attempts(backend, job_id)
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "cancelled"


# ════════════════════════════════════════════════════════════════════════════
# Force-cancel path
# ════════════════════════════════════════════════════════════════════════════


async def test_force_cancel(pg_dsn: str) -> None:
    """Force-cancel path."""
    worker_id = new_uuid()
    async with _test_infra(pg_dsn, worker_id) as (deps, backend, settings):
        client = JobsClient(backend)
        lock_lease = timedelta(seconds=settings.lock_lease)
        job_id, job = await _enqueue_and_dispatch(client, backend, worker_id, lock_lease)

        async def blocking_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            while True:  # noqa: ASYNC110 # Why: intentional infinite loop that yields to event-loop; force-cancel test target
                await asyncio.sleep(0.05)

        actor_config = StubActorConfig(retry=RetryPolicy(kind="non_retryable", max_attempts=1))

        shutdown = asyncio.Event()
        controller = make_cancel_controller(deps, worker_id, backend)
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job,
                    worker_id,
                    run_actor=blocking_actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)
                result = await client.cancel(job_id)
                assert result.cancellation_initiated

                row_p1 = await _wait_for_cancel_phase(backend, job_id, 1)
                lock_at_phase_1 = row_p1.lock_expires_at
                assert lock_at_phase_1 is not None

                active = None
                for _ in range(50):
                    active = deps.active_jobs.get(job_id)
                    if active is not None and active.cancel_observed_at is not None:
                        break
                    await asyncio.sleep(0.02)
                assert active is not None
                assert active.cancel_observed_at is not None
                assert isinstance(active.cancel_observed_at, float)
                assert math.isfinite(active.cancel_observed_at)
                assert active.cancel_observed_at > 0, (
                    "cancel_observed_at sanity check: must be a finite, positive float (loop.time())"
                )

                row_p2 = await _wait_for_cancel_phase(backend, job_id, 2, timeout=3.0)
                assert row_p2.cancel_phase == 2

                if row_p2.status == "running":
                    await asyncio.sleep(settings.heartbeat_interval * 2 + 0.1)
                    row_after_renewals = await backend.get(job_id)
                    assert row_after_renewals is not None
                    if row_after_renewals.status == "running":
                        lock_after_renewals = row_after_renewals.lock_expires_at
                        assert lock_after_renewals is not None
                        assert lock_after_renewals > lock_at_phase_1

                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(consumer_task, timeout=3.0)
            finally:
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            shutdown.set()
            await heartbeat_task

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.cancel_phase == 2

        attempts = await _read_job_attempts(backend, job_id)
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "cancelled"

        events = await _read_job_events(backend, job_id)
        # Find the phase-2 escalation event (written by the hook, not mark_cancelled).
        escalation_events = [
            e
            for e in events
            if e["kind"] == "state_change"
            and isinstance(e["detail"], dict)
            and e["detail"].get("cancel_phase_from") == 1
        ]
        assert len(escalation_events) >= 1, f"no phase-2 escalation event found in {events}"
        detail = escalation_events[-1]["detail"]
        assert isinstance(detail, dict), f"expected dict, got {type(detail)}"
        assert detail.get("cancel_phase_from") == 1
        assert detail.get("cancel_phase_to") == 2
        assert "worker_id" in detail


# ════════════════════════════════════════════════════════════════════════════
# Abandoned path
# ════════════════════════════════════════════════════════════════════════════


async def test_abandoned(pg_dsn: str) -> None:
    """Abandoned path.

    Uses cancel_grace=0.1s and cleanup_grace=0.05s (total 0.15s) with
    heartbeat_interval=0.2s so the combined deadline is reliably reached
    on the same tick as phase-2, guaranteeing ABANDON_PENDING is set
    before task.cancel() and the consumer skips mark_cancelled.
    """
    worker_id = new_uuid()
    async with _test_infra(
        pg_dsn,
        worker_id,
        cancellation_grace_period="0.1",
        cleanup_grace_period="0.05",
        lock_lease="2.0",
    ) as (deps, backend, settings):
        client = JobsClient(backend)
        lock_lease = timedelta(seconds=settings.lock_lease)
        job_id, job = await _enqueue_and_dispatch(client, backend, worker_id, lock_lease)

        unblock = threading.Event()

        async def wedged_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, unblock.wait)
            return "never"

        actor_config = StubActorConfig(retry=RetryPolicy(kind="non_retryable", max_attempts=1))

        shutdown = asyncio.Event()
        controller = make_cancel_controller(deps, worker_id, backend)
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job,
                    worker_id,
                    run_actor=wedged_actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)
                result = await client.cancel(job_id)
                assert result.cancellation_initiated
                await _poll_until_status(backend, job_id, {"abandoned"}, timeout=10.0)
            finally:
                unblock.set()
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            shutdown.set()
            await heartbeat_task

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "abandoned"

        attempts = await _read_job_attempts(backend, job_id)
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "cancelled"
        assert deps.active_jobs.count() == 0


# ════════════════════════════════════════════════════════════════════════════
# Idempotent repeat cancel
# ════════════════════════════════════════════════════════════════════════════


async def test_idempotent_repeat_cancel(pg_dsn: str) -> None:
    """Idempotent repeat cancel."""
    worker_id = new_uuid()
    async with _test_infra(pg_dsn, worker_id) as (deps, backend, settings):
        client = JobsClient(backend)
        lock_lease = timedelta(seconds=settings.lock_lease)
        job_id, job = await _enqueue_and_dispatch(client, backend, worker_id, lock_lease)

        async def actor(_job: JobRow, ctx: JobContext[BaseModel]) -> str:
            while not ctx.cancellation_requested:  # noqa: ASYNC110 # Why: intentional poll loop checking cancellation_requested; observable test behaviour
                await asyncio.sleep(0.01)
            return "stopped"

        actor_config = StubActorConfig(retry=RetryPolicy(kind="non_retryable", max_attempts=1))

        shutdown = asyncio.Event()
        controller = make_cancel_controller(deps, worker_id, backend)
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job,
                    worker_id,
                    run_actor=actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)
                r1 = await client.cancel(job_id)
                assert r1.cancellation_initiated is True
                r2 = await client.cancel(job_id)
                assert r2.cancellation_initiated is False
                await asyncio.wait_for(consumer_task, timeout=5.0)
            finally:
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            shutdown.set()
            await heartbeat_task

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == 1

        events = await _read_job_events(backend, job_id)
        cancel_request_events = [e for e in events if e["kind"] == "cancel_request"]
        assert len(cancel_request_events) == 1


# ════════════════════════════════════════════════════════════════════════════
# Worker dies during phase 1 (chaos)
# ════════════════════════════════════════════════════════════════════════════


async def test_worker_dies_phase1(pg_dsn: str) -> None:
    """Worker dies during phase 1 (chaos)."""
    worker_id = new_uuid()
    async with _test_infra(
        pg_dsn,
        worker_id,
        cancellation_grace_period="1.0",
        cleanup_grace_period="0.5",
        lock_lease="2.0",
    ) as (deps, backend, settings):
        client = JobsClient(backend)
        lock_lease = timedelta(seconds=settings.lock_lease)
        job_id, job = await _enqueue_and_dispatch(client, backend, worker_id, lock_lease)

        async def cooperative_actor(_job: JobRow, ctx: JobContext[BaseModel]) -> str:
            while not ctx.cancellation_requested:  # noqa: ASYNC110 # Why: intentional poll loop checking cancellation_requested; observable test behaviour
                await asyncio.sleep(0.01)
            await asyncio.sleep(1)  # keep actor busy during cancel; never completes
            return "should_not_reach"

        actor_config = StubActorConfig(retry=RetryPolicy(kind="non_retryable", max_attempts=1))

        shutdown = asyncio.Event()
        controller = make_cancel_controller(deps, worker_id, backend)
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job,
                    worker_id,
                    run_actor=cooperative_actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)
                result = await client.cancel(job_id)
                assert result.cancellation_initiated

                await _wait_for_cancel_phase(backend, job_id, 1, timeout=2.0)

                consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await consumer_task

                shutdown.set()
                await heartbeat_task
                await asyncio.sleep(0.2)

                schema = settings.schema_name
                sweep_sql = _SWEEP_1_SQL_TEST.format(schema=schema)

                async with deps.worker_pool.acquire() as sweep_conn:
                    # Simulate process death: reset the row to running + expired lock.
                    # In a real SIGKILL, the terminal write never commits; here the
                    # asyncio.shield in mark_cancelled already committed, so we undo
                    # it to faithfully test the sweep's reclaim path (_SWEEP_1_SQL).
                    await sweep_conn.execute(
                        f"UPDATE \"{schema}\".jobs SET status = 'running'::\"{schema}\".job_status, locked_by_worker = $1, cancel_phase = 1, lock_expires_at = now() - interval '70 seconds' WHERE id = $2",  # noqa: S608 # Why: schema identifier from validated WorkerSettings; asyncpg has no parameter binding for identifiers
                        worker_id,
                        job_id,
                    )
                    await sweep_conn.execute(
                        sweep_sql,
                        timedelta(seconds=settings.cancellation_grace_period),
                        timedelta(seconds=settings.cleanup_grace_period),
                    )

                row = await backend.get(job_id)
                assert row is not None
                assert row.status in ("pending", "crashed")
            finally:
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            if not shutdown.is_set():
                shutdown.set()
                if not heartbeat_task.done():
                    with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                        await asyncio.wait_for(heartbeat_task, timeout=2.0)
            elif not heartbeat_task.done():
                await heartbeat_task


# ════════════════════════════════════════════════════════════════════════════
# PG connection drop during phase-2 write (chaos)
# ════════════════════════════════════════════════════════════════════════════


async def test_pg_drop_during_phase2(pg_dsn: str) -> None:
    """PG connection drop during phase-2 write (chaos)."""
    worker_id = new_uuid()
    async with _test_infra(pg_dsn, worker_id) as (deps, backend, settings):
        client = JobsClient(backend)
        lock_lease = timedelta(seconds=settings.lock_lease)
        job_id, job = await _enqueue_and_dispatch(client, backend, worker_id, lock_lease)

        chaos_pool = _ChaosPool(deps.heartbeat_pool)

        async def blocking_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            while True:  # noqa: ASYNC110 # Why: intentional infinite loop that yields to event-loop; chaos test target
                await asyncio.sleep(0.05)

        actor_config = StubActorConfig(retry=RetryPolicy(kind="non_retryable", max_attempts=1))

        shutdown = asyncio.Event()
        controller = make_cancel_controller(deps, worker_id, backend)
        deps.heartbeat_pool = chaos_pool  # type: ignore[assignment] # Why: _ChaosPool is a structural substitute for asyncpg.Pool; does not inherit so pyright cannot verify structural compatibility
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job,
                    worker_id,
                    run_actor=blocking_actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)
                result = await client.cancel(job_id)
                assert result.cancellation_initiated

                await _wait_for_cancel_phase(backend, job_id, 1, timeout=2.0)

                chaos_pool.set_error_trigger("cancel_phase = 2")
                failures_before = deps.heartbeat_failures

                deadline = time.monotonic() + settings.cancellation_grace_period + 2.0
                while time.monotonic() < deadline:
                    if deps.heartbeat_failures > failures_before:
                        break
                    await asyncio.sleep(0.05)
                assert deps.heartbeat_failures > failures_before, (
                    "expected heartbeat_failures to increment"
                )
                assert not consumer_task.done(), (
                    "consumer_task should not be cancelled before task.cancel() fires"
                )

                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(consumer_task, timeout=5.0)
            finally:
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            shutdown.set()
            await heartbeat_task

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"
        assert row.cancel_phase == 2

        attempts = await _read_job_attempts(backend, job_id)
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "cancelled"
