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
# per-beat cycle is heartbeat_interval + heartbeat_command_timeout = 0.6s.
_HB_COMMAND_TIMEOUT = 0.1
_MAX_HEARTBEAT_FAILURES = 2


async def _setup(
    pg_dsn: str,
    **overrides: str,
) -> tuple[AsyncExitStack, WorkerDeps, str]:
    from taskq.migrate import apply_pending

    # The lease must scale with the isolate bound the test configures:
    # the tail (max(interval, command_timeout)) plus (F+1) failed cycles
    # at ~0.6s of worst cycle each is what the cascade floor requires,
    # so an F=20 chaos config needs a ~13s lease, a 3s lease under 21
    # failed beats is exactly what the validator now refuses. An
    # explicit LOCK_LEASE override still wins.
    max_failures = int(overrides.get("MAX_HEARTBEAT_FAILURES", _MAX_HEARTBEAT_FAILURES))
    default_lease = (
        max(_HEARTBEAT_INTERVAL, _HB_COMMAND_TIMEOUT)
        + (max_failures + 1) * (_HEARTBEAT_INTERVAL + _HB_COMMAND_TIMEOUT)
        + 0.3
    )
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


class _SustainedContentionPool:
    """Stalls EVERY acquire past the caller's timeout: a sustained
    inability to reach the database, not a one-tick blip.

    Each tick's ``acquire(timeout=interval)`` burns its full allowance
    against an unmoving stall and raises ``TimeoutError`` - the
    deterministic equivalent of the chained acquire timeouts a loaded
    CI runner once produced by accident (consecutive_failures=1,2,3,
    each a real ``acquire(timeout=interval)`` TimeoutError). This is
    the boundary pin from the OTHER side of the reclaim line: a worker
    that genuinely cannot reach the DB across consecutive ticks is
    silent past the ``heartbeat_timeout`` floor, and the sweep
    reclaiming it is the operator-correct semantics, not a flake.
    """

    def __init__(self, real_pool: asyncpg.Pool, *, delay: float) -> None:
        self._real_pool = real_pool
        self._delay = delay

    def acquire(self, *, timeout: float | None = None) -> object:
        return _StallingAcquireCtx(
            self._real_pool.acquire(timeout=timeout),
            delay=self._delay,
            timeout=timeout,
        )

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

    The injection is a CONN-level fault, not pool contention: the very
    first loop acquire passes through (the seed beat), the second raises
    a refused-connection error INSTANTLY (a transient that fails fast -
    it burns none of the tick's wall clock), and every later acquire
    passes straight through to the healthy pool. Exactly one tick fails
    and it fails fast, so no later tick can chain an acquire timeout and
    the scenario cannot silently become "sustained contention" - which
    is what the one-shot STALL injection this test once used did under
    CI load: the injected stall only ever stalled one acquire, but the
    loaded runner made the real pool's own subsequent acquires time out
    too (consecutive_failures=1,2,3, each a real
    ``acquire(timeout=interval)`` TimeoutError), the worker was silent
    for ~3s, and the sweep correctly reclaimed it - a lottery between
    "one transient blip" and a scenario where reclaim is the RIGHT
    answer. Sustained inability to reach the DB has its own honest
    boundary pin below (``test_sustained_acquire_timeouts_...``); here
    the pool itself stays healthy, so the one-blip promise is asserted
    deterministically: no reclaim anywhere in the window, at most one
    failed tick ever, and the beats resumed.

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
            row = await conn.fetchrow(
                f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            seed_heartbeat_at: datetime = row["last_heartbeat_at"]

        real_pool = deps.heartbeat_pool
        # One FAST conn-level blip on the first loop tick after the seed
        # beat, then the healthy pool serves: the pool never stalls, so
        # no later acquire can chain a timeout.
        deps.heartbeat_pool = _OneShotBlipPool(real_pool)  # type: ignore[assignment] # Why: chaos pool substitution, see _FailingPool pattern above.

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
            # A short breath between sweeps: the sweep must keep pace
            # with the deadline window, but it shares the event loop
            # with the heartbeat under test, and a bare hammer would
            # starve the very timers whose pacing is being pinned.
            await asyncio.sleep(0.01)

        shutdown.set()
        # Janitorial exit, not a pin: the loop's exit rides the shim's
        # per-acquire delays and the runner's scheduler, and under a
        # coverage-instrumented -n 4 runner 5s was a lottery (CI: a
        # TimeoutError on this wait_for and the worker task left pending,
        # reding the coverage leg while the same test stayed green
        # everywhere else). The pacing pins above are already recorded;
        # a loop that genuinely cannot exit still reds - loudly - below.
        try:
            await asyncio.wait_for(task, timeout=60.0)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await task
            raise AssertionError(
                f"the {task.get_name()} heartbeat loop did not exit within "
                "60s of shutdown - a hang, not a pace"
            ) from None

        assert reclaimed_at is None and last_status == "running", (
            f"a job whose heartbeat_timeout was sized at the documented floor "
            f"(2x TASKQ_HEARTBEAT_INTERVAL = {_SAFE_SIZING_TIMEOUT}), and whose worker suffered "
            f"exactly one transient tick failure before recovering and heartbeating normally "
            f"again, was reclaimed by the sweep at t={reclaimed_at!r}s after the seed beat "
            f"(status={last_status!r}). A failed heartbeat tick is not a beat - it stamps "
            f"nothing - so it retries promptly instead of consuming the full inter-beat wait, "
            f"and the recovery beat after one fast failure lands at <= 1.25x interval, inside "
            f"the 2x interval floor the sizing guidance calls safe. A worker that is alive, "
            f"still lease-valid, and beating again must not lose its job to a single transient "
            f"blip."
        )
        # The injection was one-shot and the pool stayed healthy: at most
        # ONE tick ever failed. Under the old stall injection a loaded
        # runner chained real acquire timeouts here (consecutive_failures
        # = 1,2,3) - that chain is the sustained-contention scenario the
        # boundary test below pins, not this one.
        assert deps.heartbeat_failures <= 1, (
            f"expected at most one failed tick (one injected fast blip), "
            f"got {deps.heartbeat_failures} - the pool did not stay healthy after the blip"
        )
        # The worker kept beating: a fresh stamp landed, and it is newer
        # than the old beat's reclaim deadline (both stamps are DB
        # clock_timestamp() values, so no wall-clock basis is mixed).
        async with real_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert row is not None
            final_heartbeat_at: datetime = row["last_heartbeat_at"]
        assert final_heartbeat_at > seed_heartbeat_at + _SAFE_SIZING_TIMEOUT, (
            f"the recovery beat never landed past the old beat's deadline "
            f"(seed={seed_heartbeat_at!r}, final={final_heartbeat_at!r}) - the worker did "
            f"not resume heartbeating, so 'not reclaimed' proves nothing"
        )
    finally:
        await stack.aclose()


class _BlipAcquireCtx:
    """One acquire that either raises instantly (the fast transient
    blip - a refused connection raises the moment it is attempted, it
    does not burn the caller's whole ``acquire`` timeout) or delays a
    bounded spell before passing through (the loaded recovery beat:
    under load the gap only gets worse, so the recovery beat itself is
    modelled as contended)."""

    def __init__(
        self,
        ctx: PoolAcquireContext,
        *,
        blip: bool,
        delay: float,
        timeout: float | None,
    ) -> None:
        self._ctx = ctx
        self._blip = blip
        self._delay = delay
        self._timeout = timeout

    async def __aenter__(self) -> object:
        if self._blip:
            raise asyncpg.PostgresConnectionError("chaos: instant connection blip")
        if self._delay > 0:
            # Bounded by the caller's own acquire timeout, exactly like
            # _StallingAcquireCtx: a contention stall, not an unbounded hang.
            await asyncio.wait_for(asyncio.sleep(self._delay), timeout=self._timeout)
        return await self._ctx.__aenter__()

    async def __aexit__(self, *args: object) -> None:
        await self._ctx.__aexit__(*args)


class _OneShotBlipPool:
    """acquire 1 passes through (the seed beat), acquire 2 raises a
    transient connection error INSTANTLY (exactly one fast-failed tick),
    and every later acquire passes straight through to the healthy pool.

    The conn-level one-blip injection: the fault is a refused
    connection, not pool contention, so the pool itself never stalls and
    no later tick can chain an ``acquire(timeout=interval)`` timeout.
    """

    def __init__(self, real_pool: asyncpg.Pool) -> None:
        self._real_pool = real_pool
        self._calls = 0

    def acquire(self, *, timeout: float | None = None) -> object:
        self._calls += 1
        if self._calls == 2:
            return _BlipAcquireCtx(
                self._real_pool.acquire(timeout=timeout),
                blip=True,
                delay=0.0,
                timeout=timeout,
            )
        return self._real_pool.acquire(timeout=timeout)

    async def close(self) -> None:
        await self._real_pool.close()


class _BlipThenLoadedPool:
    """acquire 1 passes through (the seed beat), acquire 2 raises a
    transient error INSTANTLY (exactly one fast transient failure - the
    common blip shape), and every later acquire is delayed by
    ``recovery_delay`` before passing through (the loaded recovery)."""

    def __init__(self, real_pool: asyncpg.Pool, *, recovery_delay: float) -> None:
        self._real_pool = real_pool
        self._recovery_delay = recovery_delay
        self._calls = 0

    def acquire(self, *, timeout: float | None = None) -> _BlipAcquireCtx:
        self._calls += 1
        return _BlipAcquireCtx(
            self._real_pool.acquire(timeout=timeout),
            blip=self._calls == 2,
            delay=self._recovery_delay if self._calls >= 3 else 0.0,
            timeout=timeout,
        )

    async def close(self) -> None:
        await self._real_pool.close()


async def test_one_fast_transient_failure_at_documented_sizing_keeps_the_job(
    pg_dsn: str,
) -> None:
    """The behavior pin for the failed-tick pacing contract: at the
    documented ``heartbeat_timeout = 2x heartbeat_interval`` sizing, ONE
    fast transient tick failure must not cost the worker its job, the
    recovery beat must land well inside the 2x floor, and the ledger
    must not treat the blip as the start of a cascade.

    Deterministic by construction, not by racing. The pre-fix loop
    slept the FULL remaining interval after a tick that failed
    instantly, so after one good beat (t=0) and one fast failed tick
    (t≈interval) the recovery beat only STARTED at ≈2x interval - at
    the reclaim deadline, not inside it - and a bounded contention
    stall on the recovery acquire (0.4s, well under the loop's own
    ``acquire(timeout=interval)`` bound; the CI measurement this pins
    saw the recovery commit at 3.064s against a 3.0s floor) holds its
    commit past the deadline through a window a production sweep -
    running continuously on the leader - provably fires in. The test
    sweeps on a short poll through and past that window, so on the
    pre-fix pacing the reclaim is observed at ≈2x interval every run.
    Post-fix, a failed tick is not a beat: it retries after a quarter
    interval, the recovery beat commits at ≈(1.25x interval + stall),
    comfortably inside the deadline, and every sweep observes a fresh
    stamp on a still-running job.
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
        deps.heartbeat_pool = _BlipThenLoadedPool(  # type: ignore[assignment] # Why: chaos pool substitution, see the _FailingPool / _OneShotStallingPool patterns above.
            real_pool,
            # Bounded, and under half the interval: enough that the
            # pre-fix recovery beat (which only STARTS at ≈2x interval)
            # commits clearly past the deadline, while the post-fix
            # recovery beat (which starts at ≈1.25x interval) still
            # commits clearly inside it.
            recovery_delay=_SAFE_SIZING_INTERVAL * 0.25,
        )

        seed_time = time.monotonic()
        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-fast-blip",
        )

        deadline = seed_time + _SAFE_SIZING_TIMEOUT.total_seconds()
        poll_until = deadline + _SAFE_SIZING_INTERVAL * 2
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
            # A short breath between sweeps: the sweep must keep pace
            # with the deadline window, but it shares the event loop
            # with the heartbeat under test, and a bare hammer would
            # starve the very timers whose pacing is being pinned.
            await asyncio.sleep(0.01)

        shutdown.set()
        # Janitorial exit, not a pin: the loop's exit rides the shim's
        # per-acquire delays and the runner's scheduler, and under a
        # coverage-instrumented -n 4 runner 5s was a lottery (CI: a
        # TimeoutError on this wait_for and the worker task left pending,
        # reding the coverage leg while the same test stayed green
        # everywhere else). The pacing pins above are already recorded;
        # a loop that genuinely cannot exit still reds - loudly - below.
        try:
            await asyncio.wait_for(task, timeout=60.0)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await task
            raise AssertionError(
                f"the {task.get_name()} heartbeat loop did not exit within "
                "60s of shutdown - a hang, not a pace"
            ) from None

        assert reclaimed_at is None and last_status == "running", (
            f"a job whose heartbeat_timeout was sized at the documented floor "
            f"(2x TASKQ_HEARTBEAT_INTERVAL = {_SAFE_SIZING_TIMEOUT}) was reclaimed at "
            f"t={reclaimed_at!r}s after the seed beat (status={last_status!r}) even though "
            f"its worker suffered exactly one INSTANT transient tick failure - a refused "
            f"connection, not a slow one - and beat again promptly. A failed tick is not a "
            f"beat: it stamps nothing, so it must not also consume the full inter-beat wait, "
            f"or the recovery beat lands at twice the interval - the reclaim deadline itself - "
            f"and any sweep that fires in the crossing window steals the job from a worker "
            f"that is alive and lease-valid. The retry pacing must keep the recovery beat "
            f"well inside the 2x floor the sizing guidance calls safe."
        )
    finally:
        await stack.aclose()


async def test_sustained_acquire_timeouts_across_ticks_gets_reclaimed(
    pg_dsn: str,
) -> None:
    """The honest boundary on the other side of the reclaim line: a
    worker that genuinely cannot reach the database across CONSECUTIVE
    ticks - each tick's ``acquire(timeout=interval)`` burning its whole
    allowance and raising ``TimeoutError`` - is silent past the
    ``heartbeat_timeout`` floor, and the sweep MUST reclaim its job.

    This is the deterministic shape of what a loaded CI runner once
    produced by accident (consecutive_failures=1,2,3, each a real
    acquire timeout) and what the one-blip test above is careful to
    NEVER inject: sustained inability to reach the DB is exactly the
    scenario the reclaim exists for - the worker may be dead, hung, or
    partitioned, and letting a live-but-partitioned worker's lease be
    handed back is the operator-correct semantics. The boundary is
    therefore explicit on both sides instead of a lottery in between:
    one fast failed tick -> keep (the test above); repeated
    full-interval acquire timeouts -> reclaim (this test).

    Deterministic by construction: every acquire is stalled ten
    intervals against the caller's one-interval bound, so every tick
    raises ``TimeoutError`` (in TRANSIENT_PG_ERRORS) after exactly one
    interval of wall time, no scheduling can produce a successful beat,
    and the silence crosses the 2x deadline on the second failed tick.
    The poll loop sweeps through and past the crossing; a reclaim is
    asserted, and it must be observed while the heartbeat loop is still
    alive and still failing (not isolated, not exited) - the sweep did
    the reclaiming, not the worker giving up.
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
        # EVERY acquire stalls far past the loop's own
        # acquire(timeout=interval) bound: the worker cannot reach the DB
        # on any tick, while the sweep (polling the real pool directly)
        # can - the partitioned-worker shape.
        deps.heartbeat_pool = _SustainedContentionPool(  # type: ignore[assignment] # Why: chaos pool substitution, see _FailingPool pattern above.
            real_pool,
            delay=_SAFE_SIZING_INTERVAL * 10,
        )

        seed_time = time.monotonic()
        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-sustained-timeouts",
        )

        deadline = seed_time + _SAFE_SIZING_TIMEOUT.total_seconds()
        # The deadline is crossed by the second failed tick (each burns a
        # full interval); sweep a little past it with margin for
        # scheduling jitter, but not so long that a pathological runner
        # masks a genuinely missing reclaim.
        poll_until = deadline + _SAFE_SIZING_INTERVAL * 3
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
            await asyncio.sleep(0.01)

        shutdown.set()
        # Janitorial exit, not a pin: the loop's exit rides the shim's
        # per-acquire delays and the runner's scheduler, and under a
        # coverage-instrumented -n 4 runner 5s was a lottery (CI: a
        # TimeoutError on this wait_for and the worker task left pending,
        # reding the coverage leg while the same test stayed green
        # everywhere else). The pacing pins above are already recorded;
        # a loop that genuinely cannot exit still reds - loudly - below.
        try:
            await asyncio.wait_for(task, timeout=60.0)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await task
            raise AssertionError(
                f"the {task.get_name()} heartbeat loop did not exit within "
                "60s of shutdown - a hang, not a pace"
            ) from None

        assert reclaimed_at is not None and last_status != "running", (
            f"a worker that could not reach the database on ANY tick - every "
            f"acquire(timeout=interval) burned its full allowance and raised TimeoutError, "
            f"so the worker was silent from the seed beat at t=0 well past the "
            f"heartbeat_timeout deadline at t={deadline - seed_time:.2f}s - was NOT reclaimed "
            f"by the sweep (polled through t={poll_until - seed_time:.2f}s, final status="
            f"{last_status!r}). Sustained inability to reach the DB is the scenario the "
            f"heartbeat arm of the reclaim sweep exists for: one fast transient failure must "
            f"keep the job (see the one-blip pins above), but consecutive ticks that cannot "
            f"even acquire a connection must have the lease handed back, or a dead/hung/"
            f"partitioned worker's job is never re-run."
        )
        # The sweep did the reclaiming: the heartbeat loop was still
        # alive and still failing (at least one tick had burned its full
        # acquire allowance and raised) when the job was taken - the
        # worker did not hand the job back itself.
        assert deps.heartbeat_failures >= 1, (
            "the job was reclaimed but the heartbeat loop never recorded a failed tick - "
            "the injection did not produce the sustained-acquire-timeout scenario it names"
        )
    finally:
        await stack.aclose()
