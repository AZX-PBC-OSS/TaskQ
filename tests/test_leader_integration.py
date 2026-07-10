"""Integration tests for MaintenanceLeader.

Tests cover the election lifecycle through failover, plus the
acceptance-definition assertion.

Each test uses per-test schema isolation against the session-scoped PG
container. Short heartbeat intervals (1.0 s) keep the suite fast; default
intervals (10.0 s) would make each test wait ~12 s.
"""

import asyncio
from contextlib import AsyncExitStack, suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import wake_channel
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import _create_worker
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import isolate_self
from taskq.worker.leader import MAINTENANCE_LEADER_LOCK_NAME, MaintenanceLeader

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
    conn1 = await asyncpg.connect(pg_dsn)
    try:
        got = await conn1.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            MAINTENANCE_LEADER_LOCK_NAME,
        )
        assert got is True
    finally:
        await conn1.close()

    conn2 = await asyncpg.connect(pg_dsn)
    try:
        got2 = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            MAINTENANCE_LEADER_LOCK_NAME,
        )
        assert got2 is True
        await conn2.execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            MAINTENANCE_LEADER_LOCK_NAME,
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
            now,
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
                now,
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
                now,
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
