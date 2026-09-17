"""Integration tests for MaintenanceLeader.

Tests cover the election lifecycle through failover, plus the
acceptance-definition assertion.

Each test uses per-test schema isolation against the session-scoped PG
container. Short heartbeat intervals (0.5 s) keep the suite fast; default
intervals (10.0 s) would make each test wait ~12 s.
"""

import asyncio
import logging
from contextlib import AsyncExitStack, suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import schema_lock_name, wake_channel
from taskq.obs import setup_logging
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import _create_worker
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop, isolate_self
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
_LOCK_LEASE = 3.0


def _build_short_settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
            "TASKQ_LOCK_LEASE": str(_LOCK_LEASE),
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
        }
    )


async def _open_single(
    pg_dsn: str, schema: str
) -> tuple[str, AsyncExitStack, WorkerDeps, PostgresBackend, UUID]:
    from taskq.migrate import apply_pending

    settings = _build_short_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        cancellation_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)
        backend: PostgresBackend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=cancellation_grace,
            cleanup_grace_period=cleanup_grace,
        )
    except BaseException:
        await stack.aclose()
        raise

    worker_id = new_uuid()
    async with deps.dispatcher_pool.acquire() as conn:
        await _create_worker(conn, settings.schema_name, worker_id)

    return settings.schema_name, stack, deps, backend, worker_id


async def _open_two(
    pg_dsn: str, schema: str
) -> tuple[
    str,
    AsyncExitStack,
    WorkerDeps,
    PostgresBackend,
    UUID,
    AsyncExitStack,
    WorkerDeps,
    PostgresBackend,
    UUID,
]:
    from taskq.migrate import apply_pending

    settings = _build_short_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None

    wid_a, wid_b = new_uuid(), new_uuid()

    stack_a = AsyncExitStack()
    deps_a: WorkerDeps = await stack_a.enter_async_context(open_worker_deps(settings))
    try:
        backend_a: PostgresBackend = PostgresBackend(
            deps_a,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
    except BaseException:
        await stack_a.aclose()
        raise

    stack_b = AsyncExitStack()
    deps_b: WorkerDeps = await stack_b.enter_async_context(open_worker_deps(settings))
    try:
        backend_b: PostgresBackend = PostgresBackend(
            deps_b,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
    except BaseException:
        await stack_b.aclose()
        await stack_a.aclose()
        raise

    async with deps_a.dispatcher_pool.acquire() as c_a:
        await _create_worker(c_a, settings.schema_name, wid_a)
    async with deps_b.dispatcher_pool.acquire() as c_b:
        await _create_worker(c_b, settings.schema_name, wid_b)

    return (
        settings.schema_name,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    )


# ── Election from cold start ──────────────────────────────────────


@pytest.mark.asyncio
async def test_ti1_election_cold_start(pg_dsn: str) -> None:
    """Election from cold start sets is_leader and upserts maintenance_leader."""
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader.run(shutdown))
        try:
            await asyncio.wait_for(deps.is_leader.wait(), timeout=_HEARTBEAT_INTERVAL + 2)
            assert deps.is_leader.is_set()
            async with deps.dispatcher_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
                )
            assert row is not None
            assert UUID(str(row["worker_id"])) == worker_id
        finally:
            shutdown.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    finally:
        await stack.aclose()


# ── Two pods race; exactly one wins ───────────────────────────────


@pytest.mark.asyncio
async def test_ti2_two_pods_race(pg_dsn: str) -> None:
    """Two pods race; exactly one wins..

    Contributes to the acceptance_definition.
    """
    (
        schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")

    try:
        leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
        shutdown = asyncio.Event()
        task_a = asyncio.create_task(leader_a.run(shutdown))
        task_b = asyncio.create_task(leader_b.run(shutdown))
        try:
            await asyncio.sleep(3 * _HEARTBEAT_INTERVAL)
            count = int(deps_a.is_leader.is_set()) + int(deps_b.is_leader.is_set())
            assert count == 1
            async with deps_a.dispatcher_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT count(*) as cnt FROM "{schema}".maintenance_leader'
                )
            assert row is not None
            assert row["cnt"] == 1
        finally:
            shutdown.set()
            task_a.cancel()
            task_b.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(task_a, task_b, return_exceptions=True)
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


# ── Second pod becomes leader after first closes leader_conn ──────


@pytest.mark.asyncio
async def test_ti3_failover_after_leader_conn_close(pg_dsn: str) -> None:
    """Pod B becomes leader after pod A's leader_conn closes."""
    (
        schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")

    try:
        leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
        shutdown = asyncio.Event()
        task_a = asyncio.create_task(leader_a.run(shutdown))
        task_b = asyncio.create_task(leader_b.run(shutdown))
        try:
            await asyncio.sleep(3 * _HEARTBEAT_INTERVAL)

            winner_a = deps_a.is_leader.is_set()
            winner_b = deps_b.is_leader.is_set()
            assert (winner_a + winner_b) == 1, "Exactly one pod should be leader"

            if winner_a:
                winner_task = task_a
                loser_deps = deps_b
                loser_wid = wid_b
                winner_conn = deps_a.leader_conn
            else:
                winner_task = task_b
                loser_deps = deps_a
                loser_wid = wid_a
                winner_conn = deps_b.leader_conn

            winner_task.cancel()
            with suppress(asyncio.CancelledError):
                await winner_task

            if winner_conn is not None and not winner_conn.is_closed():
                await winner_conn.close()

            await asyncio.wait_for(loser_deps.is_leader.wait(), timeout=2 * _HEARTBEAT_INTERVAL + 3)
            assert loser_deps.is_leader.is_set()

            async with deps_b.dispatcher_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
                )
            assert row is not None
            assert UUID(str(row["worker_id"])) == loser_wid
        finally:
            shutdown.set()
            task_a.cancel()
            task_b.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(task_a, task_b, return_exceptions=True)
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


# ── Advisory lock released on connection close ────────────────────


@pytest.mark.asyncio
async def test_ti4_advisory_lock_release_on_close(pg_dsn: str) -> None:
    """Session-bound advisory lock releases on connection close.

    ;.
    """
    # No per-test schema is in play here (the test never opens WorkerDeps),
    # so pin the lock of the default-configured leader: the schema-qualified
    # maintenance lock for the default schema_name ("taskq").
    lock_name = schema_lock_name("maintenance_leader", "taskq")
    conn1 = await asyncpg.connect(pg_dsn)
    try:
        got = await conn1.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            lock_name,
        )
        assert got is True
    finally:
        await conn1.close()

    conn2 = await asyncpg.connect(pg_dsn)
    try:
        got2 = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            lock_name,
        )
        assert got2 is True
        await conn2.execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            lock_name,
        )
    finally:
        await conn2.close()


# ── Sweep 1 reclaims job with expired lock ────────────────


@pytest.mark.asyncio
async def test_ti5_sweep_1_reclaims_expired_lock(pg_dsn: str) -> None:
    """Sweep 1 reclaims a job whose lock has expired.

    Contributes to acceptance_definition.
    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        job_id = new_uuid()
        now = datetime.now(UTC)
        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, cancel_phase) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'running', 0, $7, $8, $9, $10, $11, $12, $13)",
                job_id,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                1,
                now - timedelta(minutes=5),
                worker_id,
                now - timedelta(minutes=1),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                0,
            )

        count = await backend.reclaim_expired_locks(
            timedelta(seconds=deps.settings.cancellation_grace_period),
            timedelta(seconds=deps.settings.cleanup_grace_period),
        )
        assert count > 0

        async with deps.dispatcher_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
        assert row is not None
        assert row["status"] in ("pending", "crashed")
    finally:
        await stack.aclose()


# ── Scheduled-wake promotes job and NOTIFY fires ──────────


@pytest.mark.asyncio
async def test_ti6_scheduled_wake_promotes_and_notify(pg_dsn: str) -> None:
    """Scheduled-wake promotes a job to pending and NOTIFY fires.

    Contributes to acceptance_definition.
    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        channel = wake_channel(schema)
        notify_count = 0

        def _on_notify(
            connection: object,
            pid: int,
            ch: str,
            payload: object,  # pyright: ignore[reportMissingParameterType] # Why: asyncpg callback signature requires Connection | PoolConnectionProxy; importing just for type annotation is heavyweight — use object.
        ) -> None:
            nonlocal notify_count
            if ch == channel:
                notify_count += 1

        listen_conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
        await listen_conn.add_listener(channel, _on_notify)  # type: ignore[reportArgumentType] # Why: asyncpg-stubs expects Awaitable | Generator return; this callback returns None — runtime asyncpg accepts both.

        try:
            job_id = new_uuid()
            now = datetime.now(UTC)
            async with deps.dispatcher_pool.acquire() as conn:
                await conn.execute(
                    f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, cancel_phase) '
                    "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'scheduled', 0, 1, $7, 0)",
                    job_id,
                    "test_actor",
                    "default",
                    "{}",
                    3,
                    "transient",
                    now + timedelta(milliseconds=500),
                )

            leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
            shutdown = asyncio.Event()
            task = asyncio.create_task(leader.run(shutdown))
            try:
                await asyncio.wait_for(deps.is_leader.wait(), timeout=_HEARTBEAT_INTERVAL + 2)

                # Poll until the wake loop promotes the job (scheduled_at is
                # +500ms) instead of a fixed 1.5s sleep — promotion latency
                # varies under parallel test load.
                status: str | None = None
                for _ in range(150):
                    async with deps.dispatcher_pool.acquire() as conn:
                        row = await conn.fetchrow(
                            f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                            job_id,
                        )
                    status = None if row is None else row["status"]
                    if status == "pending" and notify_count >= 1:
                        break
                    await asyncio.sleep(0.1)
                assert status == "pending", f"job not promoted within 15s (status={status})"
                assert notify_count >= 1
            finally:
                shutdown.set()
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        finally:
            await listen_conn.remove_listener(channel, _on_notify)  # type: ignore[reportArgumentType] # Why: asyncpg-stubs expects Awaitable | Generator return; callback return type mismatch — see add_listener above.
            await listen_conn.close()
    finally:
        await stack.aclose()


# ── Equivalence — isolate_self vs sweep_expired_locks ─────────────


@pytest.mark.asyncio
async def test_ti7_equivalence_isolate_self_vs_sweep_cancel_phase_0(pg_dsn: str) -> None:
    """cancel_phase=0: isolate_self and sweep_expired_locks produce identical row state.

    forward-compat.
    """
    schema, stack, deps, _backend, worker_id_a = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        worker_id_b = new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await _create_worker(conn, schema, worker_id_b)

        job_a = new_uuid()
        job_b = new_uuid()
        now = datetime.now(UTC)

        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, cancel_phase) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'running', 0, $7, $8, $9, $10, $11, $12, $13)",
                job_a,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                2,
                now - timedelta(minutes=5),
                worker_id_a,
                now - timedelta(seconds=1),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                0,
            )
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, cancel_phase) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'running', 0, $7, $8, $9, $10, $11, $12, $13)",
                job_b,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                2,
                now - timedelta(minutes=5),
                worker_id_b,
                now - timedelta(seconds=1),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                0,
            )

        shutdown_isolate = asyncio.Event()
        await isolate_self(deps, worker_id_a, shutdown_isolate)

        async with deps.dispatcher_pool.acquire() as conn:
            await PostgresBackend.sweep_expired_locks(
                conn,
                timedelta(seconds=deps.settings.cancellation_grace_period),
                timedelta(seconds=deps.settings.cleanup_grace_period),
                schema=schema,
            )

        async with deps.dispatcher_pool.acquire() as conn:
            row_a = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at, scheduled_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_a,
            )
            row_b = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at, scheduled_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_b,
            )

        assert row_a is not None
        assert row_b is not None
        assert row_a["status"] == row_b["status"]
        assert row_a["locked_by_worker"] == row_b["locked_by_worker"]
        assert row_a["lock_expires_at"] == row_b["lock_expires_at"]

        if row_a["status"] == "pending":
            assert row_a["finished_at"] == row_b["finished_at"]
    finally:
        await stack.aclose()


@pytest.mark.asyncio
async def test_ti7_equivalence_cancel_phase_1_grace_divergence(pg_dsn: str) -> None:
    """cancel_phase=1 within grace: isolate_self reclaims, sweep_expired_locks does NOT.

    Documented divergence — vs sweep 1 carve-out.
    """
    schema, stack, deps, _backend, worker_id_a = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        worker_id_b = new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await _create_worker(conn, schema, worker_id_b)

        job_a = new_uuid()
        job_b = new_uuid()
        now = datetime.now(UTC)

        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, cancel_phase) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'running', 0, $7, $8, $9, $10, $11, $12, $13)",
                job_a,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                2,
                now - timedelta(minutes=5),
                worker_id_a,
                now - timedelta(seconds=1),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                1,
            )
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, cancel_phase) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'running', 0, $7, $8, $9, $10, $11, $12, $13)",
                job_b,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                2,
                now - timedelta(minutes=5),
                worker_id_b,
                now - timedelta(seconds=1),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                1,
            )

        shutdown_isolate = asyncio.Event()
        await isolate_self(deps, worker_id_a, shutdown_isolate)

        async with deps.dispatcher_pool.acquire() as conn:
            count = await PostgresBackend.sweep_expired_locks(
                conn,
                timedelta(seconds=deps.settings.cancellation_grace_period),
                timedelta(seconds=deps.settings.cleanup_grace_period),
                schema=schema,
            )

        assert count == 0

        async with deps.dispatcher_pool.acquire() as conn:
            row_a = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_a)
            row_b = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_b)

        assert row_a is not None
        assert row_b is not None
        assert row_a["status"] in ("pending", "crashed")
        assert row_b["status"] == "running"
    finally:
        await stack.aclose()


@pytest.mark.asyncio
async def test_isolate_self_hands_back_an_indefinite_job_past_max_attempts(
    pg_dsn: str,
) -> None:
    """An ``indefinite`` job whose attempt counter already passed
    ``max_attempts`` is re-pended by ``isolate_self``, not terminalised.

    A running row can legitimately sit past its ``max_attempts``: the
    indefinite kind's budget is its ``schedule_to_close`` deadline, not
    the attempt count, so the consumer's own retry path keeps
    rescheduling it, and the crash-reclaim sweep hands such a job back
    (pinned for both backends in the reclaim retry-budget parity tests).
    A heartbeat-lost worker isolating itself is the same class of event —
    infrastructure, not a job failure — so the same hand-back must hold
    on this path, leaving terminalisation to the deadline sweep where an
    indefinite job's budget actually runs out.
    """
    schema, stack, deps, _backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        job_id = new_uuid()
        now = datetime.now(UTC)
        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, schedule_to_close, locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, cancel_phase) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'running', 0, $7, $8, $9, $10, $11, $12, $13, $14)",
                job_id,
                "test_actor",
                "default",
                "{}",
                2,
                "indefinite",
                3,  # already past max_attempts — the state only this kind reaches
                now - timedelta(minutes=5),
                now + timedelta(hours=6),  # the kind's real budget, still open
                worker_id,
                now + timedelta(minutes=5),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                0,
            )

        await isolate_self(deps, worker_id, asyncio.Event())

        async with deps.dispatcher_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "pending", (
            f"isolate_self terminalised an indefinite job as {row['status']!r} at "
            "attempt 3 with max_attempts 2 while its schedule_to_close deadline "
            "was still open: max_attempts is not this kind's budget, so a "
            "heartbeat-lost worker must hand the job back, not end it"
        )
        assert row["finished_at"] is None, (
            "a job handed back for another attempt must not carry a finished_at"
        )
    finally:
        await stack.aclose()


# ── (deadline sweep): _sweep_loop drives deadline_exceeded end-to-end ──


@pytest.mark.asyncio
async def test_sweep_loop_transitions_deadline_exceeded_pending_job(
    pg_dsn: str,
) -> None:
    """acceptance: MaintenanceLeader._sweep_loop drives PG deadline_sweep
    end-to-end, transitioning a pending job with expired schedule_to_close
    to 'failed' with the correct job_attempts and job_events rows.


    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        job_id = new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, attempt, scheduled_at, schedule_to_close) '
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'pending', 0, 1, now(), now() + interval '10 seconds')",
                job_id,
                "deadline_actor",
                "default",
                "{}",
                3,
                "transient",
            )
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '1 second' WHERE id = $1",
                job_id,
            )

        deps.is_leader.set()
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader._sweep_loop(shutdown))
        try:
            await asyncio.sleep(0.5)
        finally:
            shutdown.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        async with deps.dispatcher_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, error_class, error_message, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempt_row = await conn.fetchrow(
                f'SELECT count(*) AS cnt, bool_and(started_at IS NOT NULL) AS has_started_at, bool_and(worker_id IS NULL) AS no_worker FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
            event_row = await conn.fetchrow(
                f"SELECT count(*) AS cnt FROM \"{schema}\".job_events WHERE job_id = $1 AND kind = 'state_change' AND detail->>'to_state' = 'failed' AND detail->>'error_class' = 'DeadlineExceeded'",
                job_id,
            )

        assert row is not None
        assert row["status"] == "failed"
        assert row["error_class"] == "DeadlineExceeded"
        assert row["error_message"] == "schedule_to_close reached before next dispatch"
        assert row["finished_at"] is not None

        assert attempt_row is not None
        assert attempt_row["cnt"] == 1
        assert attempt_row["has_started_at"] is True
        assert attempt_row["no_worker"] is True

        assert event_row is not None
        assert event_row["cnt"] == 1
    finally:
        await stack.aclose()


# ── FK violation on UPSERT triggers shutdown ─────────────────────


@pytest.mark.asyncio
async def test_tn5_fk_violation_triggers_shutdown(pg_dsn: str) -> None:
    """FK violation on UPSERT triggers clean shutdown.."""
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader.run(shutdown))
        try:
            await asyncio.wait_for(deps.is_leader.wait(), timeout=_HEARTBEAT_INTERVAL + 2)
            assert deps.is_leader.is_set()

            raw_conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
            try:
                await raw_conn.execute(
                    f'DELETE FROM "{schema}".workers WHERE id = $1',
                    worker_id,
                )
            finally:
                await raw_conn.close()

            # is_leader.is_set() guarantees leader_conn is open and non-None.
            await deps.leader_conn.close()  # pyright: ignore[reportOptionalMemberAccess] # Why: is_leader.set() guarantees leader_conn was opened; pyright cannot narrow across the Event boundary.
            deps.leader_conn = None

            await asyncio.wait_for(shutdown.wait(), timeout=_HEARTBEAT_INTERVAL + 3)
            assert shutdown.is_set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        finally:
            if not shutdown.is_set():
                shutdown.set()
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
    finally:
        await stack.aclose()


# ── Crash-reclaim observability via poll_reclaim_events ──────────


@pytest.mark.asyncio
async def test_ti7_crash_reclaim_observable_via_poll(pg_dsn: str) -> None:
    """Real-crash end-to-end: a worker's connection is killed via
    pg_terminate_backend, the lock expires, Sweep 1 reclaims the job,
    and a consumer observes the terminal transition via
    poll_reclaim_events without polling list_jobs/get itself.

    Contributes to acceptance_definition.
    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        now = datetime.now(UTC)

        # Open a dedicated connection simulating the worker's session
        worker_conn = await asyncpg.connect(str(deps.settings.pg_dsn))
        # Fetched immediately after connect (before anything that could
        # fail) so it's unconditionally bound for the finally block below.
        worker_pid = await worker_conn.fetchval("SELECT pg_backend_pid()")
        try:
            job_id = new_uuid()
            await worker_conn.execute(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, '
                f"max_attempts, retry_kind, status, priority, attempt, "
                f"scheduled_at, locked_by_worker, lock_expires_at, "
                f"started_at, last_heartbeat_at, cancel_phase) "
                f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                f"'running', 0, $7, $8, $9, $10, $11, $12, $13)",
                job_id,
                "test_actor",
                "default",
                "{}",
                1,
                "transient",
                1,
                now - timedelta(minutes=5),
                worker_id,
                now - timedelta(minutes=1),
                now - timedelta(minutes=2),
                now - timedelta(minutes=2),
                0,
            )
        finally:
            # Kill the worker's connection (simulating SIGKILL)
            killer_conn = await asyncpg.connect(str(deps.settings.pg_dsn))
            try:
                await killer_conn.fetchval("SELECT pg_terminate_backend($1)", worker_pid)
            finally:
                await killer_conn.close()
            with suppress(Exception):
                await worker_conn.close()

        # Run Sweep 1
        count = await backend.reclaim_expired_locks(
            timedelta(seconds=deps.settings.cancellation_grace_period),
            timedelta(seconds=deps.settings.cleanup_grace_period),
        )
        assert count == 1

        # Consumer observes the terminal transition via poll_reclaim_events.
        # visibility_delay=0: no concurrent sweep in this test, so there is
        # no out-of-order-commit hazard to guard against — see
        # taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY.
        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 1
        evt = events[0]
        assert evt.detail["to_state"] == "crashed"
        assert evt.detail["reason"] == "lock_expired"
        assert evt.job_id == JobId(job_id)
    finally:
        await stack.aclose()


@pytest.mark.asyncio
async def test_ti8_fanout_outstanding_counter_reaches_zero(
    pg_dsn: str,
) -> None:
    """Fan-out scenario: enqueue N jobs, mark most succeeded, crash one,
    run the sweep, and drive an outstanding-work counter purely off
    poll_reclaim_events + the normal succeeded-path checks. The counter
    must reach zero — without poll_reclaim_events it would be stuck at 1
    forever (the regression described in the issue).

    Contributes to acceptance_definition.
    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        now = datetime.now(UTC)
        num_jobs = 5
        job_ids: list[UUID] = []

        # Create N running jobs with valid locks
        async with deps.dispatcher_pool.acquire() as conn:
            for _ in range(num_jobs):
                jid = new_uuid()
                job_ids.append(jid)
                await conn.execute(
                    f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, '
                    f"max_attempts, retry_kind, status, priority, attempt, "
                    f"scheduled_at, locked_by_worker, lock_expires_at, "
                    f"started_at, last_heartbeat_at, cancel_phase) "
                    f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                    f"'running', 0, 1, $7, $8, $9, $10, $11, 0)",
                    jid,
                    "test_actor",
                    "default",
                    "{}",
                    3,
                    "transient",
                    now - timedelta(minutes=5),
                    worker_id,
                    now + timedelta(seconds=60),
                    now - timedelta(minutes=2),
                    now - timedelta(minutes=2),
                )

        # Mark first N-1 as succeeded
        for jid in job_ids[:-1]:
            await backend.mark_succeeded(JobId(jid), worker_id, None, attempt=1)

        # Force-expire the lock on the last job (simulates a crashed worker)
        crashed_jid = job_ids[-1]
        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET lock_expires_at = $1 WHERE id = $2',
                now - timedelta(seconds=10),
                crashed_jid,
            )

        # Run Sweep 1
        count = await backend.reclaim_expired_locks(
            timedelta(seconds=deps.settings.cancellation_grace_period),
            timedelta(seconds=deps.settings.cleanup_grace_period),
        )
        assert count == 1

        # Drive outstanding-work counter — a consumer tracking completion
        # of a fan-out of N jobs via an outstanding-work counter.
        outstanding = num_jobs

        # Normal succeeded-path: check each non-crashed job's status
        for jid in job_ids[:-1]:
            row = await backend.get(JobId(jid))
            assert row is not None
            if row.status in (
                "succeeded",
                "failed",
                "crashed",
                "cancelled",
                "abandoned",
            ):
                outstanding -= 1

        # Crash-reclaim path: poll_reclaim_events observes the crashed job.
        # visibility_delay=0: single sweep, no concurrent-commit hazard here.
        events = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
        assert len(events) == 1
        assert events[0].job_id == JobId(crashed_jid)
        outstanding -= 1

        assert outstanding == 0
    finally:
        await stack.aclose()


# ── Losing an election is normal, not a fault ─────────────────────


async def _run_pod(
    deps: WorkerDeps,
    backend: PostgresBackend,
    worker_id: UUID,
    shutdown: asyncio.Event,
) -> tuple[asyncio.Task[None], asyncio.Task[None]]:
    """Start one pod's leader runtime plus its heartbeat loop.

    The production pod shape: the lease row is renewed by the election
    loop itself, while the heartbeat keeps the pod's ``workers`` row live
    (stale-worker cleanup and the admin liveness verdicts read it). A
    test that started only the election loop would be exercising a pod
    shape production never runs.
    """
    leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
    election_task = asyncio.create_task(leader.run(shutdown))
    heartbeat_task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    return election_task, heartbeat_task


def _events(caplog: pytest.LogCaptureFixture, event: str) -> list[logging.LogRecord]:
    """Records whose rendered message carries *event*.

    Read off the stdlib logging stream rather than ``structlog.testing.
    capture_logs``: whether that capture sees anything depends on whether
    some earlier test in the session happened to configure structlog, so a
    log assertion built on it passes or fails by test ordering. Pairs with
    ``_route_logs_to_stdlib``, which makes the routing explicit.
    """
    return [record for record in caplog.records if event in record.getMessage()]


def _route_logs_to_stdlib() -> None:
    """Bind structlog to the stdlib logging stream ``caplog`` observes.

    ``setup_logging`` is the production configurator and is idempotent, so
    this is a no-op once anything (a worker boot, an earlier test) has
    already configured logging — the point is that these tests never depend
    on that having happened.
    """
    setup_logging(level="DEBUG", log_format="json")


@pytest.mark.asyncio
async def test_losing_pod_emits_no_error_and_keeps_retrying(
    pg_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    """In a fleet, all but one pod loses every election, forever. That is the
    normal steady state and must never surface as an error.

    If losing looked like a fault, every multi-pod deployment would page on
    every heartbeat interval, and the one signal that actually matters — a
    fleet with no leader at all — would be buried under noise from the
    healthy majority. The losing pod must also keep its election loop alive
    so it can take over when the leader dies.
    """
    (
        _schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")

    try:
        shutdown = asyncio.Event()

        _route_logs_to_stdlib()
        with caplog.at_level(logging.DEBUG):
            election_a, hb_a = await _run_pod(deps_a, backend_a, wid_a, shutdown)
            election_b, hb_b = await _run_pod(deps_b, backend_b, wid_b, shutdown)
            tasks = [election_a, hb_a, election_b, hb_b]
            try:
                # Long enough for several full election cycles, so the loser
                # has lost repeatedly rather than once.
                await asyncio.sleep(6 * _HEARTBEAT_INTERVAL)

                assert int(deps_a.is_leader.is_set()) + int(deps_b.is_leader.is_set()) == 1, (
                    "exactly one pod must hold leadership"
                )
                loser_task = election_a if deps_b.is_leader.is_set() else election_b
                assert not loser_task.done(), (
                    "the losing pod's election loop must stay alive so it can take over"
                )
            finally:
                shutdown.set()
                for task in tasks:
                    task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(*tasks, return_exceptions=True)

        errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
        assert errors == [], (
            "a pod that merely lost the election must not emit an error-level "
            f"signal; got {[record.getMessage() for record in errors]}"
        )
        assert _events(caplog, "leader-elected"), (
            "sanity: the captured log stream must contain the election activity "
            "this test is asserting over"
        )
        assert _events(caplog, "leader-retry"), (
            "sanity: the losing pod's retry activity must be present in the stream"
        )
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_healthy_fleet_elects_once_with_no_leadership_churn(
    pg_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Two healthy pods must produce exactly one election, not a handover
    cycle where each pod repeatedly mistakes the other for dead.

    Leadership churn is invisible in a single-pod test and expensive in
    production: every handover tears down the leader's dedicated connection
    and restarts sweeps mid-flight, so a fleet that churns does maintenance
    work in permanently interrupted slices while looking elected throughout.
    """
    (
        _schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")

    try:
        shutdown = asyncio.Event()

        _route_logs_to_stdlib()
        with caplog.at_level(logging.DEBUG):
            election_a, hb_a = await _run_pod(deps_a, backend_a, wid_a, shutdown)
            election_b, hb_b = await _run_pod(deps_b, backend_b, wid_b, shutdown)
            tasks = [election_a, hb_a, election_b, hb_b]
            try:
                await asyncio.sleep(8 * _HEARTBEAT_INTERVAL)
            finally:
                shutdown.set()
                for task in tasks:
                    task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(*tasks, return_exceptions=True)

        elections = _events(caplog, "leader-elected")
        assert len(elections) == 1, (
            "a healthy two-pod fleet must elect exactly once over its lifetime; "
            f"got {len(elections)} elections, indicating leadership churn"
        )
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_joining_pod_does_not_displace_the_incumbent_leader(pg_dsn: str) -> None:
    """Scaling a fleet up must not cause leadership churn.

    An operator adding capacity expects the existing leader to keep leading.
    A design where the newcomer wins — or where both briefly believe they
    lead — would run leader-only work twice or stall it mid-sweep at every
    deploy, which is exactly when the fleet is least able to absorb it.
    """
    (
        schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")

    try:
        shutdown = asyncio.Event()
        election_a, hb_a = await _run_pod(deps_a, backend_a, wid_a, shutdown)
        tasks: list[asyncio.Task[None]] = [election_a, hb_a]
        try:
            await asyncio.wait_for(deps_a.is_leader.wait(), timeout=5 * _HEARTBEAT_INTERVAL + 3)

            election_b, hb_b = await _run_pod(deps_b, backend_b, wid_b, shutdown)
            tasks.extend([election_b, hb_b])
            await asyncio.sleep(4 * _HEARTBEAT_INTERVAL)

            assert deps_a.is_leader.is_set(), "the incumbent must keep leadership"
            assert not deps_b.is_leader.is_set(), "the joining pod must not displace the incumbent"
            assert not election_a.done(), "the incumbent's loop must survive the newcomer joining"

            async with deps_a.dispatcher_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
                )
            assert row is not None
            assert UUID(str(row["worker_id"])) == wid_a, (
                "the stored leader row must still name the incumbent"
            )
        finally:
            shutdown.set()
            for task in tasks:
                task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


# ── Lease lifecycle: resign handover, trust window, term loss ────────
#
# The election loop's own contract over the lease row (the statement-level
# fences are pinned in tests/test_leader_lease_contract.py): a graceful
# stop hands the role over on the successor's next cycle; a leader whose
# renewal path has failed past its trust window stands down on its own
# clock, strictly before the server-side expiry it wrote, without touching
# the database; and a leader whose row was taken over under it reads the
# fence's zero rows and stands down.


@pytest.mark.asyncio
async def test_orchestrated_shutdown_still_resigns_the_lease(pg_dsn: str) -> None:
    """The production shutdown path must free the row, not just the conn.

    orchestrate_shutdown closes and nulls a TaskQ-owned ``leader_conn``
    before the leader runtime's teardown runs, so a resign that reads only
    ``deps.leader_conn`` would find nothing to write through — the row
    would be left to lapse and every graceful deploy would pay a whole
    lease of no-leader time. The resign must ride a connection that
    survives to teardown.
    """
    from taskq.worker.shutdown import orchestrate_shutdown

    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader.run(shutdown))
        try:
            await asyncio.wait_for(deps.is_leader.wait(), timeout=5 * _HEARTBEAT_INTERVAL + 3)

            # The real orchestrated path: phases, then the owned leader_conn
            # is closed and nulled, then the event fires.
            result = await orchestrate_shutdown(
                deps,
                deps.settings,
                worker_id,
                shutdown,
                None,
                backend=backend,
            )
            assert result == 0
            assert deps.leader_conn is None, (
                "test premise: the orchestrator takes leader_conn down first"
            )

            await asyncio.wait_for(task, timeout=5 * _HEARTBEAT_INTERVAL + 3)

            async with deps.dispatcher_pool.acquire() as conn:
                count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".maintenance_leader')
            assert count == 0, (
                "the resign must land even though the orchestrator closed "
                "leader_conn first — otherwise the row lapses only after a "
                "whole leader_lease, and every rolling deploy pays it"
            )
        finally:
            shutdown.set()
            if not task.done():
                task.cancel()
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await task
    finally:
        await stack.aclose()


@pytest.mark.asyncio
async def test_graceful_leader_shutdown_hands_over_within_one_election_cycle(
    pg_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The resign at shutdown deletes the row, so the surviving follower
    wins on its very next election attempt — a clean exit must cost the
    fleet one election cycle, never a lease's lapse."""
    (
        schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")

    try:
        _route_logs_to_stdlib()
        with caplog.at_level(logging.DEBUG):
            # Per-pod shutdown events: the incumbent is stopped alone, the
            # follower keeps running — the deploy shape.
            shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
            leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
            leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
            task_a = asyncio.create_task(leader_a.run(shutdown_a))
            task_b = asyncio.create_task(leader_b.run(shutdown_b))
            try:
                # Whichever pod won is the incumbent this test stops.
                await wait_for_condition(
                    lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                    description="one pod won the initial election",
                    timeout=5 * _HEARTBEAT_INTERVAL + 3,
                )
                if deps_a.is_leader.is_set():
                    winner_deps, winner_task, winner_shutdown = deps_a, task_a, shutdown_a
                    follower_deps, follower_wid = deps_b, wid_b
                else:
                    winner_deps, winner_task, winner_shutdown = deps_b, task_b, shutdown_b
                    follower_deps, follower_wid = deps_a, wid_a

                # The graceful stop: run()'s teardown resigns the lease
                # before the connections go. The row's absence between the
                # resign and the follower's win is deliberately NOT asserted
                # — the handover is designed to be faster than any
                # post-hoc read of it; what proves the resign landed is the
                # bound below: without it the follower would wait out the
                # whole lease (40s at these settings) instead of one cycle.
                winner_shutdown.set()
                await asyncio.wait_for(winner_task, timeout=5 * _HEARTBEAT_INTERVAL + 3)
                assert not winner_deps.is_leader.is_set()

                # One cycle = the follower's next election attempt.
                await wait_for_condition(
                    follower_deps.is_leader.is_set,
                    description="the follower took over on its next election cycle",
                    timeout=_HEARTBEAT_INTERVAL + 2.0,
                )
                async with follower_deps.dispatcher_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
                    )
                assert row is not None and UUID(str(row["worker_id"])) == follower_wid
            finally:
                shutdown_a.set()
                shutdown_b.set()
                for task in (task_a, task_b):
                    if not task.done():
                        task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(task_a, task_b, return_exceptions=True)

        assert _events(caplog, "leader-resigned"), (
            "the graceful stop must log its resign — a handover with no resign "
            "event means the fleet paid the full lease lapse instead"
        )
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


class _HungRenewConn:
    """A leader_conn stand-in that hangs the lease renewal only.

    Every other call delegates to the real connection (the double's
    surface is derived from the real thing, not hand-listed). The hung
    renewal is cut off by the renewal's own trust-window budget — the
    leader must stand down on its own clock while the server still shows
    its lease live.
    """

    def __init__(self, real: asyncpg.Connection) -> None:
        self._real = real

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def fetchval(self, sql: str, *args: object) -> object:
        if "maintenance_leader" in sql and sql.lstrip().upper().startswith("UPDATE"):
            await asyncio.Future()  # never settles; the renewal's own deadline cuts it off
        return await self._real.fetchval(sql, *args)


class _HungElectConn:
    """A leader_conn stand-in whose elect never answers.

    Used after a demotion so the election loop parks inside the elect:
    nothing can rewrite the lease row while the test reads it, which is
    what makes "the step-down touched no database state" observable
    without racing a re-election.
    """

    def __init__(self) -> None:
        self._closed = False

    async def fetchval(self, sql: str, *args: object) -> object:
        if "maintenance_leader" in sql:
            await asyncio.Future()  # never settles; released by task cancel
        return None

    async def execute(self, sql: str, *args: object) -> str:
        return "DELETE 0"  # the teardown resign: bounded and inert here

    def is_closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        self._closed = True

    def terminate(self) -> None:
        self._closed = True


@pytest.mark.asyncio
async def test_leader_steps_down_on_its_own_clock_before_the_server_side_expiry(
    pg_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The split-brain guard: the leader stops trusting its term at
    ``attempt_started + leader_lease - margin`` on its own monotonic clock —
    strictly before the ``expires_at`` the server holds — and standing down
    touches no database state.

    A renewal that cannot land (here: the connection never answers) must
    end in demotion once the remaining trust is spent, while the row the
    leader wrote is still unexpired at the server and still naming it —
    the gap that keeps a peer's takeover and this pod's leadership from
    ever overlapping.
    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        # The shortest honoured lease: trusted_until closes a full margin
        # before the server-side expiry the elect wrote.
        deps.settings.leader_lease = 4 * deps.settings.heartbeat_interval
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader.run(shutdown))
        try:
            await asyncio.wait_for(deps.is_leader.wait(), timeout=5 * _HEARTBEAT_INTERVAL + 3)

            async with deps.dispatcher_pool.acquire() as conn:
                before = await conn.fetchrow(
                    f'SELECT elected_at, expires_at FROM "{schema}".maintenance_leader '
                    f"WHERE singleton = true"
                )
            assert before is not None

            # Renewal answers stop here; the row at the server stays as the
            # elect wrote it until the lapse. The factory replacement makes
            # the post-demotion re-election park inside a hung elect, so the
            # row cannot be rewritten between the demotion and the reads
            # below — that is what makes "the step-down touched no database
            # state" observable without racing the loop's next cycle.
            real_conn = deps.leader_conn
            assert real_conn is not None
            deps.leader_conn = _HungRenewConn(real_conn)  # type: ignore[assignment]  # Why: a connection-boundary double; the renewal hangs while every other call delegates.

            async def _hung_factory() -> asyncpg.Connection:
                return _HungElectConn()  # type: ignore[return-value]  # Why: a connection-boundary double; see above.

            deps.leader_conn_factory = _hung_factory

            _route_logs_to_stdlib()
            with caplog.at_level(logging.DEBUG):
                await wait_for_condition(
                    lambda: any(
                        "leadership-lost" in record.getMessage() for record in caplog.records
                    ),
                    description="the leader stood down once its trust window was spent",
                    timeout=5 * _HEARTBEAT_INTERVAL + 3,
                )

            assert not deps.is_leader.is_set()
            assert deps.leader_term is None
            assert not deps.leading()

            # The ordering the trust window exists for: the demotion is
            # already latched while the server still holds the lease live.
            async with deps.dispatcher_pool.acquire() as conn:
                still_live = await conn.fetchval(
                    f'SELECT expires_at > clock_timestamp() FROM "{schema}".maintenance_leader '
                    f"WHERE singleton = true"
                )
                after = await conn.fetchrow(
                    f'SELECT elected_at, expires_at FROM "{schema}".maintenance_leader '
                    f"WHERE singleton = true"
                )
            assert still_live is True, (
                "the leader must stand down BEFORE the server-side expiry "
                "it wrote — standing down past it overlaps a peer's legal "
                "takeover"
            )
            assert after is not None
            assert after["elected_at"] == before["elected_at"], (
                "a trust-window step-down must not touch the row: no renewal "
                "landed and no resign deleted it"
            )
            assert after["expires_at"] == before["expires_at"]
        finally:
            shutdown.set()
            task.cancel()
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await task
    finally:
        await stack.aclose()


@pytest.mark.asyncio
async def test_leader_whose_row_was_taken_over_steps_down_on_the_next_renewal(
    pg_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A renewal fenced on ``(worker_id, elected_at)`` that matches no row
    means a successor holds the lease: the deposed leader stands down and
    leaves the successor's row untouched.

    The takeover itself cannot be produced by the elect statement against
    a live lease — that is the exclusion property — so the successor's row
    is seeded directly, exactly the state the fence exists to be read
    against.
    """
    schema, stack, deps, backend, worker_id = await _open_single(
        pg_dsn, f"test_leader_{new_base62()}"
    )
    try:
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader.run(shutdown))
        try:
            await asyncio.wait_for(deps.is_leader.wait(), timeout=5 * _HEARTBEAT_INTERVAL + 3)

            # A successor's row appears under the incumbent (its own lease
            # having lapsed is how production reaches this state).
            successor = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                await _create_worker(conn, schema, successor)
                await conn.execute(
                    f'UPDATE "{schema}".maintenance_leader SET '
                    f"worker_id = $1, elected_at = clock_timestamp(), "
                    f"last_seen_at = clock_timestamp(), "
                    f"expires_at = clock_timestamp() + interval '1 hour' "
                    f"WHERE singleton = true",
                    successor,
                )

            _route_logs_to_stdlib()
            with caplog.at_level(logging.DEBUG):
                await wait_for_condition(
                    lambda: any(
                        "leadership-lost" in message and "term_lost" in message
                        for message in (record.getMessage() for record in caplog.records)
                    ),
                    description="the deposed leader stood down on its next fenced renewal",
                    timeout=5 * _HEARTBEAT_INTERVAL + 3,
                )

            assert not deps.is_leader.is_set()
            assert deps.leader_term is None
            # The demotion touched nothing: the successor's row stands.
            async with deps.dispatcher_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
                )
            assert row is not None and UUID(str(row["worker_id"])) == successor, (
                "a deposed leader must leave the successor's row alone"
            )
        finally:
            shutdown.set()
            task.cancel()
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await task
    finally:
        await stack.aclose()


# ── A won-but-unassumable lease goes back to the fleet (#234) ───────────


class _UnassumableLeader(MaintenanceLeader):
    """A pod that keeps WINNING elections but can never open the dedicated
    conns an assume requires — the conn-count-pressure shape from #234:
    the one leader-conn slot the election itself uses opens fine, the two
    extra dedicated conns are refused every cycle."""

    async def _open_dedicated_conn(self, label: str) -> asyncpg.Connection:
        raise asyncpg.TooManyConnectionsError("remaining connection slots are reserved")


@pytest.mark.asyncio
async def test_won_but_unassumable_leader_hands_the_lease_to_a_peer(
    pg_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    """#234, end to end on real Postgres: a pod whose elections keep
    succeeding but whose dedicated-conn opens keep failing must not hold
    the lease forever.

    Every own-row re-win re-falsifies the peers' lapse predicate, so
    before the fix nobody led for as long as the failure lasted — the
    whole maintenance plane, reclaim sweep included, stopped fleet-wide.
    The fix hands the row back once the episode's trust budget (one
    ``leader_lease`` window) is spent, so the healthy peer takes over on
    its next cycle — bounded by the same SLA a dead leader already had —
    and the broken pod's LATER teardown resign stays fenced out of the
    successor's row.
    """
    (
        schema,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _open_two(pg_dsn, f"test_leader_{new_base62()}")
    try:
        # The shortest honoured lease at this heartbeat: the episode's
        # trust budget (lease - margin) is one second, so the hand-back
        # fires on the second or third failed cycle instead of paying the
        # default 40 s window in a test.
        deps_a.settings.leader_lease = 2.0

        # Per-pod shutdown events, the production shape: the red-team
        # phase below shuts down ONLY the broken pod, and a shared event
        # would tear the healthy peer down with it.
        shutdown_a = asyncio.Event()
        shutdown_b = asyncio.Event()
        # Pod A starts ALONE so it is the certain first winner; pod B
        # joins only once A is demonstrably in the won-but-unassumable
        # state, which is the state whose fleet-wide cost is the issue.
        leader_a = _UnassumableLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        election_a = asyncio.create_task(leader_a.run(shutdown_a))
        heartbeat_a = asyncio.create_task(heartbeat_loop(deps_a, wid_a, shutdown_a))
        election_b: asyncio.Task[None] | None = None
        heartbeat_b: asyncio.Task[None] | None = None
        try:
            _route_logs_to_stdlib()
            with caplog.at_level(logging.DEBUG):
                await wait_for_condition(
                    lambda: any(
                        "leader-dedicated-conn-failed" in record.getMessage()
                        and str(wid_a) in record.getMessage()
                        for record in caplog.records
                    ),
                    description="pod A must win its elect and fail the dedicated "
                    "opens before the peer joins",
                    timeout=10.0,
                )

                election_b, heartbeat_b = await _run_pod(deps_b, backend_b, wid_b, shutdown_b)

                await wait_for_condition(
                    lambda: any(
                        "leader-resigned-unassumable" in record.getMessage()
                        and str(wid_a) in record.getMessage()
                        for record in caplog.records
                    ),
                    description="the trust-spent hand-back never resigned the "
                    "won-but-unassumable lease",
                    timeout=10.0,
                )
                await wait_for_condition(
                    lambda: deps_b.is_leader.is_set(),
                    description="the healthy peer never took over the lease the "
                    "broken pod kept re-winning",
                    timeout=15.0,
                )

                assert not deps_a.is_leader.is_set(), (
                    "a pod that cannot open its dedicated conns never led"
                )
                assert int(deps_a.is_leader.is_set()) + int(deps_b.is_leader.is_set()) == 1, (
                    "exactly one leader after the hand-back"
                )

                async with deps_b.dispatcher_pool.acquire() as conn:
                    holder = await conn.fetchval(
                        f'SELECT worker_id FROM "{schema}".maintenance_leader '  # Why: schema is a test-minted identifier, never user input.
                        f"WHERE singleton = true"
                    )
                assert holder is not None and UUID(str(holder)) == wid_b, (
                    "the lease row must name the healthy peer after the hand-back"
                )

            # Red-team double-resign: shut down ONLY the broken pod. Its
            # teardown resign runs AFTER the takeover, fenced on its own
            # last won term — a different (worker_id, elected_at) from the
            # successor's — so B's row must survive it while B keeps
            # renewing. (A shared shutdown event here would resign B's own
            # row through B's teardown too, and prove nothing.)
            shutdown_a.set()
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await election_a
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await heartbeat_a

            async with deps_b.dispatcher_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'  # Why: schema is a test-minted identifier, never user input.
                )
            assert row is not None and UUID(str(row["worker_id"])) == wid_b, (
                "the broken pod's teardown resign deleted the successor's lease "
                "row — the fence exists to make that impossible"
            )
            assert deps_b.is_leader.is_set(), "the successor must still be leading"
        finally:
            shutdown_a.set()
            shutdown_b.set()
            leftover = [
                task
                for task in (election_b, heartbeat_b, election_a, heartbeat_a)
                if task is not None
            ]
            for task in leftover:
                task.cancel()
            with suppress(asyncio.CancelledError, ExceptionGroup):
                await asyncio.gather(*leftover, return_exceptions=True)
    finally:
        await stack_b.aclose()
        await stack_a.aclose()
