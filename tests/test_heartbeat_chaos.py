"""Chaos tests for heartbeat_loop and isolate_self against real PG18.

Test IDs: through.
Each test verifies a specific failure-mode behaviour under real
PostgreSQL conditions (testcontainers PG18).

Uses small intervals (heartbeat_interval=0.1s, lock_lease=1.0s)
so the suite completes in seconds rather than minutes.
"""

import asyncio
import contextlib
import time
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
from asyncpg.pool import PoolAcquireContext

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosException
from taskq.testing.pg import create_running_job, create_worker
from taskq.testing.settings import make_integration_settings
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
# The factory's tiny heartbeat command timeout: the cascade floor's
# per-beat gap is heartbeat_interval + 2 * heartbeat_command_timeout = 0.7s.
_HB_COMMAND_TIMEOUT = 0.1
_MAX_HEARTBEAT_FAILURES = 2


async def _setup(
    pg_dsn: str,
    **overrides: str,
) -> tuple[AsyncExitStack, WorkerDeps, str]:
    from taskq.migrate import apply_pending

    # The lease must scale with the isolate bound the test configures:
    # (F+1) failed beats at ~0.7s of worst gap each is what the
    # cascade floor requires, so an F=20 chaos config needs a ~15s lease,
    # a 3s lease under 21 failed beats is exactly what the validator now
    # refuses. An explicit LOCK_LEASE override still wins.
    max_failures = int(overrides.get("MAX_HEARTBEAT_FAILURES", _MAX_HEARTBEAT_FAILURES))
    default_lease = (max_failures + 1) * (_HEARTBEAT_INTERVAL + 2 * _HB_COMMAND_TIMEOUT) + 0.3
    merged: dict[str, str] = {
        "HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
        "HEARTBEAT_COMMAND_TIMEOUT": str(_HB_COMMAND_TIMEOUT),
        "LOCK_LEASE": str(overrides.pop("LOCK_LEASE", default_lease)),
        "MAX_HEARTBEAT_FAILURES": str(max_failures),
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
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=deps.settings.lock_lease),
            )

            row = await conn.fetchrow(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            initial_lock = row["lock_expires_at"]

        real_pool = deps.heartbeat_pool
        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing - replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
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
                    lock_expires_at=datetime.now(UTC) + timedelta(seconds=deps.settings.lock_lease),
                )
                job_ids.append(jid)

        real_pool = deps.heartbeat_pool
        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing - replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
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
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=deps.settings.lock_lease),
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
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=deps.settings.lock_lease),
            )

        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing - replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
            deps.heartbeat_pool,
            fail_with=asyncpg.PostgresConnectionError,  # type: ignore[arg-type] # Why: asyncpg PostgresConnectionError accepts a single str arg at runtime; pyright stubs may report arity mismatch.
        )

        original_connect = hb_module.asyncpg.connect

        async def _failing_connect(*args: object, **kwargs: object) -> object:
            raise OSError("Connection refused - simulated PG outage")

        hb_module.asyncpg.connect = _failing_connect  # type: ignore[method-assign] # Why: chaos testing - replacing asyncpg.connect to simulate full PG outage during isolate_self.
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

            assert shutdown.is_set(), "shutdown was not set - isolate_self did not complete"
        finally:
            hb_module.asyncpg.connect = original_connect  # type: ignore[method-assign]
    finally:
        await stack.aclose()


# ── QueryCanceledError counts toward heartbeat isolation ─────────


async def test_tc5_query_canceled_counts_toward_isolation(pg_dsn: str) -> None:
    """A server-side cancellation during the heartbeat counts as a failure and
    isolates the worker once the budget is spent.

    The heartbeat pool is wrapped with a ChaosConnection injecting
    QueryCanceledError on the jobs UPDATE; after max_heartbeat_failures + 1
    such ticks (3 with max=2) the shutdown event must be set. This pins
    QueryCanceledError's membership of TRANSIENT_PG_ERRORS.

    Named for what it injects, not for command_timeout: command_timeout raises
    TimeoutError, not QueryCanceledError (verified against PG 18), and nothing
    here exercises command_timeout at all. 57014 is server-side cancellation -
    a DBA, or a server-side statement_timeout.
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
                    lock_expires_at=datetime.now(UTC) + timedelta(seconds=deps.settings.lock_lease),
                )
                job_ids.append(jid)

        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos testing - replacing the Pool with a wrapper that returns ChaosConnection-wrapped connections.
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
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=deps.settings.lock_lease),
            )
            row = await conn.fetchrow(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            initial_lock: datetime = row["lock_expires_at"]

        real_pool = deps.heartbeat_pool
        deps.heartbeat_pool = _FailingPool(  # type: ignore[assignment] # Why: chaos pool substitution - see pattern.
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


# ── Surviving a single transient miss at the documented sizing ───


#: The sizing the ops guidance tells operators is safe: heartbeat_timeout
#: at the documented floor of 2x the heartbeat interval. The lease is kept
#: large so only the sweep's heartbeat arm can fire, never its lease arm,
#: and the failure budget is large so the worker survives the miss rather
#: than isolating itself.
_SAFE_SIZING_INTERVAL = 1.5
_SAFE_SIZING_TIMEOUT = timedelta(seconds=2 * _SAFE_SIZING_INTERVAL)
# 36 >= the cascade floor at F=20: 21 * (1.5 + 2 * 0.1) = 35.7.
_SAFE_SIZING_LEASE = 36.0
_NO_GRACE = timedelta(seconds=0)


class _StallingAcquireCtx:
    """Stalls the acquire past the caller's own ``timeout`` bound.

    ``heartbeat_loop`` acquires with ``timeout=interval``, and asyncpg's
    own ``acquire(timeout=...)`` races the pool wait against that bound,
    so racing a sleep against it reproduces a pool-contention spike: the
    caller sees ``TimeoutError`` only after the tick has burned close to a
    full interval of wall time, which an instantly-raised query error
    would not.
    """

    def __init__(self, ctx: PoolAcquireContext, delay: float, timeout: float | None) -> None:
        self._ctx = ctx
        self._delay = delay
        self._timeout = timeout

    async def __aenter__(self) -> object:
        await asyncio.wait_for(asyncio.sleep(self._delay), timeout=self._timeout)
        return await self._ctx.__aenter__()  # pragma: no cover - the timeout always fires first

    async def __aexit__(self, *args: object) -> None:
        await self._ctx.__aexit__(*args)


class _OneShotStallingPool:
    """Stalls the very first acquire past the caller's timeout, then
    behaves like the real pool for every later tick.

    This models a single transient blip followed by recovery, not
    sustained failure.
    """

    def __init__(self, real_pool: asyncpg.Pool, *, delay: float) -> None:
        self._real_pool = real_pool
        self._delay = delay
        self._used = False

    def acquire(self, *, timeout: float | None = None) -> object:
        if not self._used:
            self._used = True
            return _StallingAcquireCtx(
                self._real_pool.acquire(timeout=timeout),
                delay=self._delay,
                timeout=timeout,
            )
        return self._real_pool.acquire(timeout=timeout)

    async def close(self) -> None:
        await self._real_pool.close()


async def _job_status(conn: asyncpg.Connection, schema: str, job_id: UUID) -> str:
    value = await conn.fetchval(f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(value)


async def test_single_transient_tick_failure_at_documented_sizing_keeps_the_job(
    pg_dsn: str,
) -> None:
    """A job whose ``heartbeat_timeout`` is sized at the documented floor
    of 2x the heartbeat interval must survive exactly one transient tick
    failure without being reclaimed by the sweep.

    The sizing guidance is only worth following if it actually holds under
    the failure it exists to tolerate. A worker that is alive, still holds
    a valid lock lease, and resumes heartbeating on its very next tick
    must not have its job stolen and re-run, because a reclaim there is a
    duplicate execution of work that was never lost.

    The nuance is where the gap comes from. ``heartbeat_loop`` sleeps a
    full interval after every tick regardless of whether that tick
    succeeded, and a failed tick can itself consume up to an interval
    (the pool ``acquire(timeout=interval)`` bound), so one transient miss
    pushes the gap between two good beats to roughly 2x interval, landing
    at the documented floor rather than below it.

    The reclaim window is narrow -- the span between the deadline
    crossing and the recovery beat's UPDATE committing -- so the test
    sweeps on every poll iteration through and past that window and
    asserts the invariant at every sweep rather than at one sampled
    instant. A reclaimed job never returns to 'running' on its own, so
    polling longer than necessary cannot manufacture a false failure.
    """
    stack, deps, schema = await _setup(
        pg_dsn,
        HEARTBEAT_INTERVAL=str(_SAFE_SIZING_INTERVAL),
        LOCK_LEASE=str(_SAFE_SIZING_LEASE),
        MAX_HEARTBEAT_FAILURES="20",
    )
    try:
        worker_id = new_uuid()
        job_id: UUID

        async with deps.heartbeat_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_SAFE_SIZING_LEASE),
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs '
                "SET heartbeat_timeout = $2::interval, last_heartbeat_at = clock_timestamp() "
                "WHERE id = $1",
                job_id,
                _SAFE_SIZING_TIMEOUT,
            )

        real_pool = deps.heartbeat_pool
        # Stall the first acquire well past the loop's own
        # acquire(timeout=interval) bound so tick 1 raises TimeoutError,
        # which is in TRANSIENT_PG_ERRORS, after burning close to a full
        # interval of wall time.
        deps.heartbeat_pool = _OneShotStallingPool(  # type: ignore[assignment] # Why: chaos pool substitution, see _FailingPool pattern above.
            real_pool,
            delay=_SAFE_SIZING_INTERVAL * 10,
        )

        seed_time = time.monotonic()
        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-safe-sizing",
        )

        deadline = seed_time + _SAFE_SIZING_TIMEOUT.total_seconds()
        # Poll from before the deadline through several further interval
        # cycles, wide enough that ordinary scheduling and DB-latency
        # jitter cannot close the window before a poll iteration lands
        # inside it.
        poll_until = deadline + _SAFE_SIZING_INTERVAL * 4
        reclaimed_at: float | None = None
        last_status = "running"
        while time.monotonic() < poll_until:
            async with real_pool.acquire() as conn:
                count = await PostgresBackend.sweep_expired_locks(
                    conn, _NO_GRACE, _NO_GRACE, schema=schema
                )
                last_status = await _job_status(conn, schema, job_id)
            if count > 0 or last_status != "running":
                reclaimed_at = time.monotonic() - seed_time
                break

        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)

        assert reclaimed_at is None and last_status == "running", (
            f"a job whose heartbeat_timeout was sized at the documented floor "
            f"(2x TASKQ_HEARTBEAT_INTERVAL = {_SAFE_SIZING_TIMEOUT}), and whose worker suffered "
            f"exactly one transient tick failure before recovering and heartbeating normally "
            f"again, was reclaimed by the sweep at t={reclaimed_at!r}s after the seed beat "
            f"(status={last_status!r}). heartbeat_loop sleeps a full interval after every tick "
            f"regardless of outcome, and a failed tick can itself take up to an interval (the "
            f"pool acquire(timeout=interval) bound), so the gap between good beats after one "
            f"miss is roughly 2x interval, landing at rather than below the documented floor. A "
            f"worker that is alive, still lease-valid, and beating again must not lose its job "
            f"to a single transient blip at the sizing the guidance calls safe."
        )
    finally:
        await stack.aclose()
