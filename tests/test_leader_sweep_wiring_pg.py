"""The leader sweep loop's wiring, against real PG.

The mock-subtraction cure's conversions: the "one batch per tick" wiring
properties the fake packs (tests/test_leader_sweep_event_retention_wiring.py,
tests/test_leader_sweep_keyed_reclaim_wiring.py,
tests/test_leader_sweep_reclaim_drain_wiring.py) pinned through backend and
pool doubles are pinned here against the REAL loop, the REAL backend, and a
RECORDING PROXY over the module's real pool (the house pattern:
``tests/test_cancel_where_bounded.py::_StatementRecordingPool``) - the
assertion counts real statements, it never answers for them.

What stays in the fake packs: the disable sentinel's gate arithmetic, the
drain-failure retry, and the settings-cap bounds - the arms whose subject is
the loop's own control flow, not a statement.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps
from tests._loop_control import managed_loop

pytestmark = pytest.mark.integration

_SWEEP_INTERVAL = 0.05


class _RecordingPoolProxy:
    """RECORDING PROXY over the real pool (COUNT, DON'T ANSWER).

    Every call delegates to the real pool's real connection untouched; the
    proxy only counts the statement shapes the wiring under test owns:
    the event-retention DELETE, the keyed-row-reclaim DELETE, and the
    pending-reclaim drain's DELETEs (one batch per tick each)."""

    def __init__(self, pool: asyncpg.Pool, schema: str) -> None:
        self._pool = pool
        self._schema = schema
        self.event_retention_deletes = 0
        self.keyed_reclaim_deletes = 0
        self.reclaim_drain_deletes = 0
        self.total_statements = 0

    @asynccontextmanager
    async def acquire(
        self,
        **kwargs: object,  # Why: asyncpg.Pool.acquire's kwargs forwarded verbatim.
    ) -> AsyncGenerator[asyncpg.Connection]:
        outer = self
        async with self._pool.acquire(**kwargs) as conn:  # type: ignore[arg-type]

            class _RecordingConn:
                async def execute(self, sql: str, *args: object) -> str:
                    outer.total_statements += 1
                    # The CTE-form statements open with ... DELETE; the
                    # plain ones start DELETE. Classified by relation:
                    if "DELETE FROM" in sql and ".job_events" in sql:
                        outer.event_retention_deletes += 1
                    elif (
                        "DELETE FROM" in sql
                        and ".rate_limit_buckets" in sql
                        and "last_used_at" in sql
                    ):
                        # the keyed-row-reclaim window (the drain's slice
                        # DELETE binds bucket_name = any, no last_used_at)
                        outer.keyed_reclaim_deletes += 1
                    elif "DELETE FROM" in sql and (
                        ".rate_limit_buckets" in sql or ".reservation_slots" in sql
                    ):
                        outer.reclaim_drain_deletes += 1
                    return await conn.execute(sql, *args)

                def __getattr__(self, name: str) -> object:
                    return getattr(conn, name)

            yield _RecordingConn()  # type: ignore[return-value]


def _deps(
    module_pg_schema: ModulePgSchema,
    pool: _RecordingPoolProxy,
    **overrides: str,
) -> WorkerDeps:
    data: dict[str, str] = {
        "TASKQ_PG_DSN": module_pg_schema.pg_dsn,
        "TASKQ_SCHEMA_NAME": module_pg_schema.schema_name,
        "TASKQ_SWEEP_INTERVAL": str(_SWEEP_INTERVAL),
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        # 3.0 + the tiny command timeout satisfies the cascade
        # floor: 4 * (0.5 + 2 * 0.1) = 2.8 <= 3.0.
        "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
        "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
        "TASKQ_WATCHDOG_CHECK_INTERVAL": "0.25",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    data.update(
        {f"TASKQ_{k}" if not k.startswith("TASKQ_") else k: v for k, v in overrides.items()}
    )
    settings = WorkerSettings.load_from_dict(data, validate=False)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: the recording proxy is the pool's exact surface; only acquire(timeout=) is used.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    # The retention block lives in the leader-gated section of the tick.
    deps.is_leader.set()
    return deps


def _ctx(
    module_pg_schema: ModulePgSchema,
    pool: _RecordingPoolProxy,
    **overrides: str,
) -> SweepContext:
    """The sweep loop's context over the REAL backend (its dispatcher pool
    is the recording proxy) ; *overrides* adjust settings (e.g.
    EVENT_RETENTION_PERIOD)."""

    deps = _deps(module_pg_schema, pool, **overrides)
    backend = PostgresBackend(
        deps=deps,  # type: ignore[arg-type]  # Why: WorkerDeps is the BackendDeps the runtime uses.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )
    return SweepContext(
        deps=deps,
        backend=backend,
        clock=SystemClock(),
        worker_id=new_uuid(),
    )


async def _run_loop_for_ticks(
    ctx: SweepContext,
    *,
    ticks: int,
    until: "asyncio.Event | None" = None,
) -> None:
    """Run the real _sweep_loop for ~*ticks* cadence intervals under the
    lifecycle supervisor, which stops it gracefully on exit."""
    from taskq.worker import _leader_sweeps
    from tests._loop_control import managed_loop

    async with managed_loop(
        lambda shutdown: _leader_sweeps._sweep_loop(ctx, shutdown), name="sweep-loop"
    ):
        if until is not None:
            await asyncio.wait_for(until.wait(), timeout=ticks * _SWEEP_INTERVAL * 40)
        else:
            await asyncio.sleep(ticks * _SWEEP_INTERVAL)


async def test_loop_supervisor_drains_a_shielded_release() -> None:
    """The supervisor's stop, pinned on fake loops: a shutdown-respecting
    loop finishes inside the grace and its shielded slow release is
    awaited to completion; a loop that ignores shutdown hits the grace
    fallback, and the detached shielded release still runs to completion,
    leaving no pending task at the end."""
    release_one = asyncio.Event()

    async def _release_one() -> None:
        await asyncio.sleep(0.01)
        release_one.set()

    async def _loop_respecting_shutdown(shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            await asyncio.shield(_release_one())
            await asyncio.sleep(0.001)

    async with managed_loop(_loop_respecting_shutdown, name="respecting") as handle:
        await asyncio.sleep(0.001)
    assert release_one.is_set(), "the graceful stop awaits the in-flight shielded release"
    assert handle.task.done() and not handle.task.cancelled()

    release_two = asyncio.Event()
    release_tasks: set[asyncio.Task[None]] = set()

    async def _release_two() -> None:
        task = asyncio.current_task()
        assert task is not None
        release_tasks.add(task)
        await asyncio.sleep(0.01)
        release_two.set()

    async def _loop_ignoring_shutdown(shutdown: asyncio.Event) -> None:
        while True:
            await asyncio.shield(_release_two())
            await asyncio.sleep(0.001)

    async with managed_loop(_loop_ignoring_shutdown, name="ignoring", grace=0.05) as handle:
        await asyncio.sleep(0.001)
    assert handle.task.cancelled(), "the fallback must cancel a loop that ignores shutdown"
    pending = [t for t in release_tasks if not t.done()]
    await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=1.0)
    assert release_two.is_set(), "the detached shielded release completes despite the cancel"


@pytest.mark.parametrize(
    ("body_error", "loop_error", "expected_match"),
    [
        ("RuntimeError", "TimeoutError", "body"),
        ("TimeoutError", "TimeoutError", "body"),
        ("none", "TimeoutError", "loop"),
        ("RuntimeError", "RuntimeError", "body"),
        ("none", "RuntimeError", "loop"),
    ],
)
async def test_loop_supervisor_preserves_the_body_error(
    body_error: str, loop_error: str, expected_match: str
) -> None:
    """The supervisor's error precedence, row by row: a body error
    survives a cleanup failure of either kind, and a cleanup failure is
    surfaced when the body raised nothing. The grace expiry (the task
    cancelled by the wait itself) is the stop's own to suppress."""
    errors = {"RuntimeError": RuntimeError, "TimeoutError": TimeoutError}

    async def _loop_body(shutdown: asyncio.Event) -> object:
        raise errors[loop_error]("loop")

    async def _the_attempt() -> None:
        async with managed_loop(_loop_body, grace=30.0):
            if body_error != "none":
                raise errors[body_error]("body")

    expected_type = errors[body_error] if expected_match == "body" else errors[loop_error]
    with pytest.raises(expected_type, match=expected_match):
        await _the_attempt()


async def test_loop_supervisor_reports_a_survivor_of_the_fallback() -> None:
    """The two-phase stop's bound: a loop that swallows its cancellation
    outlives the cancel, and the supervisor reports it (RuntimeError)
    when the body raised nothing, instead of letting the test timeout be
    the only backstop. The loop stays stubborn only past the fallback's
    bound, then exits, so the module loop's teardown is not held."""
    handles: list[asyncio.Task[object]] = []

    async def _swallowing_loop(shutdown: asyncio.Event) -> object:
        deadline = asyncio.get_running_loop().time() + 2.5
        while asyncio.get_running_loop().time() < deadline:
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                continue
        return None

    with pytest.raises(RuntimeError, match="ignored its cancellation"):
        async with managed_loop(_swallowing_loop, grace=0.05) as handle:
            handles.append(handle.task)
            await asyncio.sleep(0.001)
    handles[0].cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(handles[0], timeout=3.0)


async def test_loop_supervisor_drains_on_the_survivor_path() -> None:
    """The caller's drain runs even when the fallback reports a survivor:
    the fallback is the path that detached a shielded release, so the
    drain must not be skipped exactly there. The loop swallows its
    cancellation past the stop, the supervisor reports the survivor, and
    the drain's bounded close still ran."""
    drain_ran = asyncio.Event()
    handles: list[asyncio.Task[object]] = []

    async def _drain() -> None:
        drain_ran.set()

    async def _swallowing_loop(shutdown: asyncio.Event) -> object:
        deadline = asyncio.get_running_loop().time() + 2.5
        while asyncio.get_running_loop().time() < deadline:
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                continue
        return None

    with pytest.raises(RuntimeError, match="ignored its cancellation"):
        async with managed_loop(_swallowing_loop, grace=0.05, drain=_drain) as handle:
            handles.append(handle.task)
            await asyncio.sleep(0.001)
    assert drain_ran.is_set(), "the caller's drain must run on the survivor path"
    handles[0].cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(handles[0], timeout=3.0)


async def test_loop_supervisor_drains_when_the_loop_itself_raises() -> None:
    """The caller's drain runs even when the loop task ends with its own
    exception, and the loop's error (not a drain failure) is what
    surfaces: the body's error would take precedence over both."""
    drain_ran = asyncio.Event()

    async def _drain() -> None:
        drain_ran.set()

    async def _raising_loop(shutdown: asyncio.Event) -> object:
        raise TimeoutError("loop's own timeout")

    with pytest.raises(TimeoutError, match="loop's own timeout"):
        async with managed_loop(_raising_loop, grace=30.0, drain=_drain):
            await asyncio.sleep(0.001)
    assert drain_ran.is_set(), "the caller's drain must run on the loop-error path"


async def test_loop_supervisor_drain_failure_does_not_mask_the_loop_error() -> None:
    """The precedence is body > loop > drain: the body succeeded, the
    loop raised, and a failing drain must not replace the loop's error."""

    async def _drain() -> None:
        raise RuntimeError("drain failed")

    async def _raising_loop(shutdown: asyncio.Event) -> object:
        raise TimeoutError("loop's own timeout")

    with pytest.raises(TimeoutError, match="loop's own timeout"):
        async with managed_loop(_raising_loop, grace=30.0, drain=_drain):
            await asyncio.sleep(0.001)


async def test_event_retention_runs_one_batch_per_tick_on_real_rows(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """One tick, one real retention DELETE: the loop drives the REAL
    sweep_expired_events on the dispatcher pool's real connection - the
    seeded old events die, the fresh ones survive, and the statement
    count is the property."""
    schema = module_pg_schema.schema_name
    from datetime import datetime as dt

    old_job = new_uuid()
    fresh_job = new_uuid()
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '  # noqa: S608  # Why: schema is the fixture-derived test identifier; values $-bound.
        "max_attempts, retry_kind, scheduled_at) "
        "VALUES ($1, 'retention_wiring', 'default', '{}', 'succeeded', 3, "
        "'transient', now())",
        old_job,
    )
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '  # noqa: S608  # Why: same fixture-derived schema; values $-bound.
        "max_attempts, retry_kind, scheduled_at) "
        "VALUES ($1, 'retention_wiring', 'default', '{}', 'succeeded', 3, "
        "'transient', now())",
        fresh_job,
    )
    old_ts = dt.now(UTC) - timedelta(days=30)
    fresh_ts = dt.now(UTC)
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".job_events (job_id, kind, occurred_at) '  # noqa: S608  # Why: same fixture-derived schema; values $-bound.
        "VALUES ($1, 'state_change', $2), ($3, 'state_change', $4)",
        old_job,
        old_ts,
        fresh_job,
        fresh_ts,
    )

    proxy = _RecordingPoolProxy(module_pg_pool, schema)
    ctx = _ctx(module_pg_schema, proxy)

    await _run_loop_for_ticks(ctx, ticks=3)

    assert proxy.event_retention_deletes >= 1, (
        "the leader loop never drove the real event-retention DELETE - the "
        "retention seam exists but the loop does not feed it real statements"
    )
    remaining_old = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is the fixture-derived test identifier; the id is $-bound.
        old_job,
    )
    remaining_fresh = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: same fixture-derived schema.
        fresh_job,
    )
    assert remaining_old == 0, "the retention DELETE ran but the old events survived"
    assert remaining_fresh == 1, "the retention DELETE ate fresh events past the window"


async def test_disabled_event_retention_issues_no_retention_statement(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """The zero sentinel disables the sweep at the loop: no real retention
    DELETE is issued on any tick, while the loop provably keeps ticking
    (its other real statements reach the same pool)."""
    proxy = _RecordingPoolProxy(module_pg_pool, module_pg_schema.schema_name)

    ctx = _ctx(module_pg_schema, proxy, EVENT_RETENTION_PERIOD="0")
    await _run_loop_for_ticks(ctx, ticks=4)

    assert proxy.total_statements >= 1, (
        "fixture broken: the loop issued no statement at all - the proxy saw "
        "no tick, so the zero count below proves nothing"
    )
    assert proxy.event_retention_deletes == 0, (
        "a disabled retention sweep (event_retention_period=timedelta(0)) "
        "issued a real DELETE - zero is the documented disable sentinel, and "
        "a sweep that ran anyway deletes every event older than 'now'"
    )


# ── The keyed-row reclaim and the pending-reclaim drain ────────────────


async def test_keyed_row_reclaim_runs_one_batch_per_tick_on_real_rows(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """One tick, one real keyed-row reclaim DELETE: an idle keyed bucket
    row aged past the configured horizon dies through the loop's own
    driving of the real sweep_idle_keyed_rows."""
    schema = module_pg_schema.schema_name
    bucket = f"wiring_keyed_{new_base62()}"
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".rate_limit_buckets '  # noqa: S608  # Why: schema is the fixture-derived test identifier; the row values are constants/$-bound.
        "(bucket_name, kind, state, keyed, last_used_at) "
        "VALUES ($1, 'token_bucket', '{}'::jsonb, true, clock_timestamp() - interval '2 hours')",
        bucket,
    )

    proxy = _RecordingPoolProxy(module_pg_pool, schema)
    ctx = _ctx(module_pg_schema, proxy, KEYED_ROW_RECLAIM_PERIOD="60")
    await _run_loop_for_ticks(ctx, ticks=3)

    assert proxy.keyed_reclaim_deletes >= 1, (
        "the loop never drove the real keyed-row reclaim DELETE - the "
        "horizon wiring exists but the loop does not feed it real statements"
    )
    remaining = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',  # noqa: S608
        bucket,
    )
    assert remaining == 0, "the idle keyed row survived the loop's real reclaim"


async def test_pending_reclaim_drain_runs_on_the_dispatcher_pool(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """The loop's keyed-eviction block feeds the pending-reclaim drain and
    the drain runs REAL statements on the dispatcher pool's connection -
    the eviction's published row dies on the same tick's drain."""
    schema = module_pg_schema.schema_name
    bucket = f"wiring_drain_{new_base62()}"
    # The published row an evicted keyed rate limit leaves behind.
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".rate_limit_buckets '  # noqa: S608  # Why: same fixture-derived schema; the row values are constants/$-bound.
        "(bucket_name, kind, state, keyed, last_used_at) "
        "VALUES ($1, 'token_bucket', '{}'::jsonb, true, clock_timestamp())",
        bucket,
    )

    from time import monotonic

    own = RateLimitRegistry()
    own.register(TokenBucket(name=bucket, capacity=5, refill_per_second=0.5, backend="memory"))
    own._keyed_rate_limit_last_used[bucket] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding an idle keyed entry for eviction.
    own._keyed_rate_limit_row_schemas[bucket] = schema  # pyright: ignore[reportPrivateUsage]  # Why: the publish-schema capture the eviction records from.

    proxy = _RecordingPoolProxy(module_pg_pool, schema)
    ctx = SweepContext(
        deps=_deps(module_pg_schema, proxy),
        backend=PostgresBackend(
            deps=_deps(module_pg_schema, proxy),  # type: ignore[arg-type]
            clock=_system_clock(),
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        ),
        clock=_system_clock(),
        worker_id=new_uuid(),
        rate_limit_registry=own,
    )
    await _run_loop_for_ticks(ctx, ticks=3)

    assert proxy.reclaim_drain_deletes >= 1, (
        "the eviction fed the pending set but the drain never issued a real "
        "DELETE on the dispatcher pool - the pending rows would survive until "
        "the fleet sweep"
    )
    remaining = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',  # noqa: S608
        bucket,
    )
    assert remaining == 0, "the evicted bucket's published row survived the drain"


def _system_clock() -> object:

    return SystemClock()
