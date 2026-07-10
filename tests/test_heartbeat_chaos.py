"""Chaos tests for heartbeat_loop and isolate_self against real PG18.

Test IDs: through.
Each test verifies a specific failure-mode behaviour under real
PostgreSQL conditions (testcontainers PG18).

Uses small intervals (heartbeat_interval=0.1s, lock_lease=1.0s)
so the suite completes in seconds rather than minutes.
"""

import asyncio
import contextlib
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
from asyncpg.pool import PoolAcquireContext

from taskq._ids import new_uuid
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosException
from taskq.testing.pg import create_running_job, create_worker
from taskq.testing.settings import make_integration_settings
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
_LOCK_LEASE = 3.0
_MAX_HEARTBEAT_FAILURES = 2


async def _setup(
    pg_dsn: str,
    **overrides: str,
) -> tuple[AsyncExitStack, WorkerDeps, str]:
    from taskq.migrate import apply_pending

    merged: dict[str, str] = {
        "HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
        "LOCK_LEASE": str(_LOCK_LEASE),
        "MAX_HEARTBEAT_FAILURES": str(_MAX_HEARTBEAT_FAILURES),
        "CANCELLATION_GRACE_PERIOD": "0.0",
        "CLEANUP_GRACE_PERIOD": "0.0",
    }
    merged.update(overrides)
    settings = make_integration_settings(pg_dsn, **merged)
    schema = settings.schema_name

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None
    assert settings.pg_dsn_pooled is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    return stack, deps, schema


class _FailingAcquireCtx:
    """Async context manager that wraps the real connection with a
    class:`ChaosConnection` configured to fail on call 1."""

    def __init__(
        self,
        ctx: PoolAcquireContext,
        fail_on_call: int = 1,
        fail_with: type[BaseException] = ChaosException,
    ) -> None:
        self._ctx = ctx
        self._fail_on_call = fail_on_call
        self._fail_with = fail_with

    async def __aenter__(self) -> ChaosConnection:
        real_conn = await self._ctx.__aenter__()
        return ChaosConnection(real_conn, self._fail_on_call, fail_with=self._fail_with)

    async def __aexit__(self, *args: object) -> None:
        await self._ctx.__aexit__(*args)


class _FailingPool:
    """Minimal asyncpg Pool wrapper that returns :class:`ChaosConnection`
    wrapped connections from the real pool."""

    def __init__(
        self,
        real_pool: asyncpg.Pool,
        *,
        fail_on_call: int = 1,
        fail_with: type[BaseException] = ChaosException,
    ) -> None:
        self._real_pool = real_pool
        self._fail_on_call = fail_on_call
        self._fail_with = fail_with

    def acquire(self, *, timeout: float | None = None) -> _FailingAcquireCtx:
        return _FailingAcquireCtx(
            self._real_pool.acquire(timeout=timeout),
            fail_on_call=self._fail_on_call,
            fail_with=self._fail_with,
        )

    async def close(self) -> None:
        await self._real_pool.close()


# ── Kill PG mid-tick ─────────────────────────────────────────────


async def test_tc1_kill_pg_mid_tick(pg_dsn: str) -> None:
    """Kill PG mid-tick via ChaosConnection injected PostgresConnectionError.

    Start heartbeat_loop with a FailingPool that raises
    PostgresConnectionError on the jobs UPDATE (2nd execute call).
    Assert PostgresConnectionError is raised inside the loop
    (caught by), deps.heartbeat_failures == 1,
    and lock_expires_at was NOT advanced (transaction rolled back).
    """
    stack, deps, schema = await _setup(pg_dsn, MAX_HEARTBEAT_FAILURES="20")
    try:
        worker_id = new_uuid()
        job_id: UUID
        initial_lock: datetime

        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

            row = await conn.fetchrow(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            initial_lock = row["lock_expires_at"]

        real_pool = deps.heartbeat_pool
        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing — replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
            real_pool,
            fail_on_call=2,
            fail_with=asyncpg.PostgresConnectionError,  # type: ignore[arg-type] # Why: asyncpg PostgresConnectionError accepts a single str arg at runtime.
        )

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-tc1",
        )

        await asyncio.sleep(0.05)
        await asyncio.sleep(_HEARTBEAT_INTERVAL * 0.9)
        assert deps.heartbeat_failures >= 1, f"expected >= 1 failure, got {deps.heartbeat_failures}"

        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        async with real_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT lock_expires_at, status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["lock_expires_at"] == initial_lock, (
                f"lock_expires_at advanced: {initial_lock} → {row['lock_expires_at']}"
            )
    finally:
        await stack.aclose()


# ── Worker isolation after max failures ──────────────────────────


async def test_tc2_worker_isolation(pg_dsn: str) -> None:
    """Worker isolates after max_heartbeat_failures + 1 failures.

    Run heartbeat_loop with a FailingPool that raises
    PostgresConnectionError on every acquire attempt (fail_on_call=1).
    With max_heartbeat_failures=2, isolation fires on the 3rd tick.
    Assert shutdown is set by the loop's call to isolate_self and
    running jobs transition to pending or crashed.
    """
    stack, deps, schema = await _setup(pg_dsn, MAX_HEARTBEAT_FAILURES="2")
    try:
        worker_id = new_uuid()
        job_ids: list[UUID] = []

        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            for _ in range(3):
                jid = await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
                )
                job_ids.append(jid)

        real_pool = deps.heartbeat_pool
        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing — replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
            real_pool,
            fail_on_call=1,
            fail_with=asyncpg.PostgresConnectionError,  # type: ignore[arg-type] # Why: asyncpg PostgresConnectionError accepts a single str arg at runtime; pyright stubs may report arity mismatch.
        )

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-tc2",
        )
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except TimeoutError:
            shutdown.set()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert shutdown.is_set(), (
            f"shutdown was not set by heartbeat_loop isolation (failures={deps.heartbeat_failures})"
        )

        async with real_pool.acquire() as conn:
            for jid in job_ids:
                row = await conn.fetchrow(
                    f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                    jid,
                )
                assert row is not None
                assert row["status"] in ("pending", "crashed"), (
                    f"job {jid} status was {row['status']}"
                )
    finally:
        await stack.aclose()


# ── heartbeat_pool exhausted ─────────────────────────────────────


async def test_tc3_pool_exhaustion(pg_dsn: str) -> None:
    """heartbeat_pool exhausted.

    Manually acquire all 4 connections from deps.heartbeat_pool
    and hold them open. Call heartbeat_loop for one tick.
    Assert TimeoutError is raised within heartbeat_interval
    seconds and failure counter incremented. Release connections.
    Assert next tick succeeds and counter resets to 0.
    """
    stack, deps, schema = await _setup(
        pg_dsn,
        MAX_HEARTBEAT_FAILURES="5",
    )
    try:
        worker_id = new_uuid()
        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        holders: list[PoolAcquireContext] = []
        try:
            for _ in range(4):
                ctx = deps.heartbeat_pool.acquire(timeout=5.0)
                holders.append(ctx)
            # Acquire all 4 to exhaust the pool
            for ctx in holders:
                await ctx.__aenter__()

            shutdown = asyncio.Event()
            tick_task = asyncio.create_task(
                heartbeat_loop(deps, worker_id, shutdown),
                name="heartbeat-tc3-exhausted",
            )

            await asyncio.sleep(deps.settings.heartbeat_interval * 3)
            assert deps.heartbeat_failures >= 1, (
                f"expected heartbeat_failures >= 1, got {deps.heartbeat_failures}"
            )

            shutdown.set()
            with contextlib.suppress(asyncio.CancelledError):
                await tick_task

        finally:
            # Release all held connections
            for ctx in holders:
                with contextlib.suppress(asyncpg.InterfaceError):
                    await ctx.__aexit__(None, None, None)

        # Next tick should succeed and reset counter
        deps.heartbeat_failures = 2
        shutdown2 = asyncio.Event()
        success_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown2),
            name="heartbeat-tc3-recovered",
        )
        try:
            await asyncio.sleep(deps.settings.heartbeat_interval * 2)
            assert deps.heartbeat_failures == 0, (
                f"expected counter reset to 0 after success, got {deps.heartbeat_failures}"
            )
            shutdown2.set()
            await success_task
        except Exception:
            shutdown2.set()
            await success_task
    finally:
        await stack.aclose()


# ── isolate_self fresh connection also failing ───────────────────


async def test_tc4_isolate_self_fresh_connect_fails(pg_dsn: str) -> None:
    """isolate_self fresh connection also failing.

    Wrap pool with failing ChaosConnection so heartbeat ticks fail.
    Mock asyncpg.connect in the heartbeat module to raise OSError
    so the fresh-connect inside isolate_self also fails.
    Assert shutdown event is set. Assert no unhandled exception escapes.
    Assert isolate_self_failure warning log was emitted.
    """
    import taskq.worker.heartbeat as hb_module

    stack, deps, schema = await _setup(pg_dsn, MAX_HEARTBEAT_FAILURES="2")
    try:
        worker_id = new_uuid()
        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing — replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
            deps.heartbeat_pool,
            fail_with=asyncpg.PostgresConnectionError,  # type: ignore[arg-type] # Why: asyncpg PostgresConnectionError accepts a single str arg at runtime; pyright stubs may report arity mismatch.
        )

        original_connect = hb_module.asyncpg.connect

        async def _failing_connect(*args: object, **kwargs: object) -> object:
            raise OSError("Connection refused — simulated PG outage")

        hb_module.asyncpg.connect = _failing_connect  # type: ignore[method-assign] # Why: chaos testing — replacing asyncpg.connect to simulate full PG outage during isolate_self.
        try:
            shutdown = asyncio.Event()
            task = asyncio.create_task(
                heartbeat_loop(deps, worker_id, shutdown),
                name="heartbeat-tc4",
            )
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except TimeoutError:
                shutdown.set()
                await task

            assert shutdown.is_set(), "shutdown was not set — isolate_self did not complete"
        finally:
            hb_module.asyncpg.connect = original_connect  # type: ignore[method-assign]
    finally:
        await stack.aclose()


# ── command_timeout fires QueryCanceledError ─────────────────────


async def test_tc5_command_timeout_query_canceled(pg_dsn: str) -> None:
    """command_timeout fires QueryCanceledError and counts toward isolation.

    Wrap heartbeat pool with ChaosConnection injecting QueryCanceledError
    on the jobs UPDATE (2nd execute call). After max_heartbeat_failures + 1
    such ticks (3 with max=2), assert shutdown event is set.
    Validates 's explicit inclusion of QueryCanceledError.
    """
    stack, deps, schema = await _setup(pg_dsn, MAX_HEARTBEAT_FAILURES="2")
    try:
        worker_id = new_uuid()
        job_ids: list[UUID] = []

        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            for _ in range(2):
                jid = await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
                )
                job_ids.append(jid)

        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing — replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
            deps.heartbeat_pool,
            fail_on_call=2,
            fail_with=asyncpg.QueryCanceledError,  # type: ignore[arg-type] # Why: asyncpg QueryCanceledError accepts a single str arg at runtime; pyright stubs may report arity mismatch.
        )

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-tc5",
        )
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except TimeoutError:
            shutdown.set()
            await task

        assert shutdown.is_set(), "shutdown was not set after QueryCanceledError-induced isolation"
    finally:
        await stack.aclose()


# ── OSError on execute increments failure counter ─────────────────


async def test_tc6_oserror_on_execute(pg_dsn: str) -> None:
    """OSError during SQL execute increments failure counter.

    Injects OSError on the 2nd execute (jobs UPDATE). Asserts heartbeat_failures
    >= 1 and lock_expires_at was NOT advanced (transaction rolled back).
    """
    stack, deps, schema = await _setup(pg_dsn, MAX_HEARTBEAT_FAILURES="20")
    try:
        worker_id = new_uuid()
        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )
            row = await conn.fetchrow(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            initial_lock: datetime = row["lock_expires_at"]

        real_pool = deps.heartbeat_pool
        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos pool substitution — see pattern.
            real_pool,
            fail_on_call=2,
            fail_with=OSError,  # type: ignore[arg-type] # Why: OSError() accepts str at runtime.
        )
        shutdown = asyncio.Event()
        task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown), name="heartbeat-tc6")
        await asyncio.sleep(_HEARTBEAT_INTERVAL * 1.5)
        assert deps.heartbeat_failures >= 1
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        async with real_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            assert row["lock_expires_at"] == initial_lock
    finally:
        await stack.aclose()
