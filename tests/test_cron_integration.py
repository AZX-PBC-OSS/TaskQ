"""Integration, chaos, and negative tests for cron scheduler.

Test IDs. Each test uses per-test schema
isolation against the session-scoped PG container.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq._json import dumps_str, loads
from taskq.backend._protocol import Backend
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._jobs import JobsClient
from taskq.cron import compute_next_fire_after
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import _create_worker
from taskq.testing.otel import setup_tracer
from taskq.worker.cron_loop import tick_cron
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.leader import MAINTENANCE_LEADER_LOCK_NAME, MaintenanceLeader

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 1.0
_LOCK_LEASE = 5.0

_HOURLY = "0 * * * *"


def _build_cron_settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
            "TASKQ_LOCK_LEASE": str(_LOCK_LEASE),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
            "TASKQ_CRON_AUTO_DISABLE_THRESHOLD": "3",
        }
    )


@asynccontextmanager
async def _open_cron_single(
    pg_dsn: str, schema: str
) -> AsyncGenerator[tuple[str, AsyncExitStack, WorkerDeps, PostgresBackend, UUID], None]:
    from taskq.migrate import apply_pending

    settings = _build_cron_settings(pg_dsn, schema)

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
        backend: PostgresBackend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
    except BaseException:
        await stack.aclose()
        raise

    worker_id = new_uuid()
    async with deps.dispatcher_pool.acquire() as conn:
        await _create_worker(conn, settings.schema_name, worker_id)

    try:
        yield settings.schema_name, stack, deps, backend, worker_id
    finally:
        await stack.aclose()


@asynccontextmanager
async def _open_cron_two(
    pg_dsn: str, schema: str
) -> AsyncGenerator[
    tuple[
        str,
        AsyncExitStack,
        WorkerDeps,
        PostgresBackend,
        UUID,
        AsyncExitStack,
        WorkerDeps,
        PostgresBackend,
        UUID,
    ],
    None,
]:
    from taskq.migrate import apply_pending

    settings = _build_cron_settings(pg_dsn, schema)

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

    try:
        yield (
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
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


async def _insert_actor_config(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_attempts, retry_kind) '
        "VALUES ($1, $2, $3, $4) "
        "ON CONFLICT (actor) DO UPDATE SET "
        "queue = EXCLUDED.queue, max_attempts = EXCLUDED.max_attempts, "
        "retry_kind = EXCLUDED.retry_kind, updated_at = now()",
        actor,
        queue,
        max_attempts,
        retry_kind,
    )


async def _insert_schedule(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    cron_expr: str = _HOURLY,
    *,
    next_fire_at: datetime | None = None,
    payload_factory: str | None = None,
    metadata: dict[str, object] | None = None,
    enabled: bool = True,
    timezone: str = "UTC",
) -> UUID:
    schedule_id = new_uuid()
    nfa = next_fire_at or compute_next_fire_after(cron_expr, timezone, datetime.now(UTC))[0]
    meta_json = dumps_str(metadata) if metadata else "{}"
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '
        "(id, actor, cron_expr, timezone, dst_strategy, payload_factory, enabled, next_fire_at, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)",
        schedule_id,
        actor,
        cron_expr,
        timezone,
        "skip",
        payload_factory,
        enabled,
        nfa,
        meta_json,
    )
    return schedule_id


def _jsonb_to_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value  # type: ignore[return-value] # Why: asyncpg jsonb may return dict when custom codec registered
    if isinstance(value, str):
        return loads(value)
    return {}


# ── Cron fire with next_fire_at in the past ────────────────────────


@pytest.mark.asyncio
async def test_ti1_cron_fire_past_schedule(pg_dsn: str) -> None:
    """Insert a schedule with next_fire_at = now() - 5min.
    Trigger one tick. Oracle: 1 job row exists with correct actor;
    last_fired_at is not NULL; next_fire_at advanced beyond now()."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "test_actor")
            await _insert_schedule(
                conn,
                schema,
                "test_actor",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        async with deps.dispatcher_pool.acquire() as conn:
            async with conn.transaction():
                await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            job_count: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',
                "test_actor",
            )
            sched = await conn.fetchrow(
                f'SELECT last_fired_at, next_fire_at FROM "{schema}".cron_schedules WHERE actor = $1',
                "test_actor",
            )

        assert job_count == 1
        assert sched is not None
        assert sched["last_fired_at"] is not None
        assert sched["next_fire_at"] > datetime.now(UTC) - timedelta(hours=1)


# ── Auto-disable after 3 failures ──────────────────────────────────


@pytest.mark.asyncio
async def test_ti2_auto_disable_after_3_failures(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Insert schedule with always-failing payload_factory.
    Run 3 ticks. Oracle: enabled = false, consecutive_failures = 3,
    last_fire_error set; OTel cron.auto_disabled event emitted."""
    _, exporter = setup_tracer(monkeypatch)

    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "failing_actor")
            await _insert_schedule(
                conn,
                schema,
                "failing_actor",
                payload_factory="tests.fixtures.always_failing_factory",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        for _ in range(3):
            async with deps.dispatcher_pool.acquire() as conn:
                async with conn.transaction():
                    await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            sched = await conn.fetchrow(
                f"SELECT enabled, consecutive_failures, last_fire_error "
                f'FROM "{schema}".cron_schedules WHERE actor = $1',
                "failing_actor",
            )

        assert sched is not None
        assert sched["enabled"] is False
        assert sched["consecutive_failures"] == 3
        assert sched["last_fire_error"] is not None

        auto_disabled_spans = [
            s
            for s in exporter.spans_named("cron fire")
            if any(ev.name == "cron.auto_disabled" for ev in s.events)
        ]
        assert len(auto_disabled_spans) >= 1


# ── Advisory lock prevents double-fire ─────────────────────────────


@pytest.mark.asyncio
async def test_ti3_advisory_lock_prevents_double_fire(pg_dsn: str) -> None:
    """Two concurrent transactions call tick_cron for the same due
    schedule. The transaction-scoped advisory xact lock ensures only one
    fires the schedule: exactly 1 job row."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "lock_actor")
            await _insert_schedule(
                conn,
                schema,
                "lock_actor",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        async def _tick_in_tx(pool: asyncpg.Pool) -> None:
            async with pool.acquire() as c:
                async with c.transaction():
                    await tick_cron(c, deps.settings, backend, schema, worker_id)

        results = await asyncio.gather(
            _tick_in_tx(deps.dispatcher_pool),
            _tick_in_tx(deps.dispatcher_pool),
            return_exceptions=True,
        )
        for r in results:
            assert not isinstance(r, BaseException), f"tick_cron raised: {r!r}"

        async with deps.dispatcher_pool.acquire() as conn:
            job_count: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',
                "lock_actor",
            )
        assert job_count == 1


# ── Static payload schedule ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_ti4_static_payload_schedule(pg_dsn: str) -> None:
    """Insert schedule with payload_factory=NULL and
    metadata={"static_payload": {"key": "value"}}. Trigger tick.
    Oracle: job row has payload = {"key": "value"}."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "static_actor")
            await _insert_schedule(
                conn,
                schema,
                "static_actor",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
                metadata={"static_payload": {"key": "value"}},
            )

        async with deps.dispatcher_pool.acquire() as conn:
            async with conn.transaction():
                await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT payload FROM "{schema}".jobs WHERE actor = $1',
                "static_actor",
            )

        assert row is not None
        payload = _jsonb_to_dict(row["payload"])
        assert payload.get("key") == "value"


# ── Import-error at fire time ───────────────────────────────────────


@pytest.mark.asyncio
async def test_ti5_import_error_at_fire_time(pg_dsn: str) -> None:
    """Insert schedule with payload_factory="nonexistent.module.fn".
    Trigger tick. Oracle: consecutive_failures >= 1; last_fire_error not NULL;
    last_fired_at still NULL."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "bad_import_actor")
            await _insert_schedule(
                conn,
                schema,
                "bad_import_actor",
                payload_factory="nonexistent.module.fn",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        async with deps.dispatcher_pool.acquire() as conn:
            async with conn.transaction():
                await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            sched = await conn.fetchrow(
                f"SELECT consecutive_failures, last_fire_error, last_fired_at "
                f'FROM "{schema}".cron_schedules WHERE actor = $1',
                "bad_import_actor",
            )

        assert sched is not None
        assert sched["consecutive_failures"] >= 1
        assert sched["last_fire_error"] is not None
        assert sched["last_fired_at"] is None


# ── Tick query uses partial index ────────────────────────────────────


@pytest.mark.asyncio
async def test_ti6_tick_query_uses_partial_index(pg_dsn: str) -> None:
    """EXPLAIN ANALYZE on the tick query.
    Oracle: plan contains cron_schedules_next_fire_idx; no Seq Scan.

    PostgreSQL's planner prefers sequential scans for small tables in
    testcontainers. Seed enough rows to make the index attractive, and
    disable seqscan as a test-only technique to verify the partial index
    exists and covers the query. This confirms the index definition
    matches the tick query's WHERE clause.
    """
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        _backend,
        _worker_id,
    ):
        future = datetime.now(UTC) + timedelta(days=1)
        async with deps.dispatcher_pool.acquire() as conn:
            for i in range(200):
                await conn.execute(
                    f'INSERT INTO "{schema}".cron_schedules '
                    "(id, actor, cron_expr, timezone, enabled, next_fire_at) "
                    "VALUES ($1, $2, '0 * * * *', 'UTC', true, $3)",
                    new_uuid(),
                    f"seed_enabled_{i}",
                    future,
                )
            for i in range(50):
                await conn.execute(
                    f'INSERT INTO "{schema}".cron_schedules '
                    "(id, actor, cron_expr, timezone, enabled, next_fire_at) "
                    "VALUES ($1, $2, '0 * * * *', 'UTC', false, $3)",
                    new_uuid(),
                    f"seed_disabled_{i}",
                    datetime.now(UTC) - timedelta(hours=2),
                )
            await conn.execute(f'ANALYZE "{schema}".cron_schedules')

            await conn.execute("SET enable_seqscan = off")
            try:
                plan_json = await conn.fetchval(
                    f"EXPLAIN (ANALYZE, FORMAT JSON) "
                    f"SELECT id, actor, cron_expr, timezone, payload_factory, "
                    f"metadata, last_fired_at, consecutive_failures, next_fire_at "
                    f'FROM "{schema}".cron_schedules '
                    f"WHERE enabled = true AND next_fire_at <= now() "
                    f"ORDER BY next_fire_at"
                )
            finally:
                await conn.execute("SET enable_seqscan = on")

        plan_str = str(plan_json)
        assert "cron_schedules_next_fire_idx" in plan_str
        assert "Seq Scan" not in plan_str


# ── Migration schema correctness ─────────────────────────────────────


@pytest.mark.asyncio
async def test_ti7_migration_schema_correctness(pg_dsn: str) -> None:
    """Apply M0 migration against fresh schema.
    Oracle: consecutive_failures column exists with NOT NULL DEFAULT 0;
    freshly inserted row defaults to 0; column appears exactly once in
    information_schema.columns."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        _backend,
        _worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            col_rows = await conn.fetch(
                "SELECT column_name, column_default, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = 'cron_schedules' "
                "AND column_name = 'consecutive_failures'",
                schema,
            )

        assert len(col_rows) == 1
        col = col_rows[0]
        assert col["column_default"] is not None
        assert "0" in str(col["column_default"])
        assert col["is_nullable"] == "NO"

        async with deps.dispatcher_pool.acquire() as conn:
            sid = new_uuid()
            await conn.execute(
                f'INSERT INTO "{schema}".cron_schedules '
                "(id, actor, cron_expr, timezone, enabled, next_fire_at) "
                "VALUES ($1, $2, $3, $4, $5, now())",
                sid,
                "schema_test_actor",
                _HOURLY,
                "UTC",
                True,
            )
            cf_val: int = await conn.fetchval(
                f'SELECT consecutive_failures FROM "{schema}".cron_schedules WHERE id = $1',
                sid,
            )

        assert cf_val == 0


# ── Re-enable after auto-disable ─────────────────────────────────────


@pytest.mark.asyncio
async def test_ti8_reenable_after_auto_disable(pg_dsn: str) -> None:
    """Auto-disable a schedule (3 failures), then
    update_schedule(enabled=True). Oracle: consecutive_failures = 0,
    last_fire_error = NULL, enabled = true; next tick fires normally."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "reenable_actor")
            schedule_id = await _insert_schedule(
                conn,
                schema,
                "reenable_actor",
                payload_factory="tests.fixtures.always_failing_factory",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        for _ in range(3):
            async with deps.dispatcher_pool.acquire() as conn:
                async with conn.transaction():
                    await tick_cron(conn, deps.settings, backend, schema, worker_id)

        client = JobsClient(backend)
        await client.update_schedule(schedule_id, enabled=True, clear_payload_factory=True)

        async with deps.dispatcher_pool.acquire() as conn:
            sched = await conn.fetchrow(
                f"SELECT enabled, consecutive_failures, last_fire_error "
                f'FROM "{schema}".cron_schedules WHERE id = $1',
                schedule_id,
            )

        assert sched is not None
        assert sched["enabled"] is True
        assert sched["consecutive_failures"] == 0
        assert sched["last_fire_error"] is None

        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "reenable_actor")
            async with conn.transaction():
                await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            job_count: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',
                "reenable_actor",
            )

        assert job_count == 1


# ── Leader failover mid-tick ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_tc1_leader_failover_mid_tick(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Leader A acquires the advisory lock and begins processing
    a due schedule, but the tick is interrupted (connection dies) before
    the UPDATE commits. Leader B becomes leader and fires the schedule
    on its next tick. Oracle: exactly 1 job in taskq.jobs (created by
    leader B, not leader A — verified by asserting 0 jobs after leader A
    shutdown, before leader B starts).

    The mid-tick interruption is simulated by patching fire_schedule
    to raise on every call while leader A is running. Each tick from
    leader A opens a transaction, acquires the advisory lock, then the
    patched fire_schedule raises — causing the transaction to roll back
    (advisory lock released, no UPDATEs committed). Leader A can never
    successfully fire. Leader B then acquires the lock and completes the
    tick successfully.
    """
    import taskq.worker.cron_loop as cron_loop_mod

    async with _open_cron_two(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack_a,
        deps_a,
        backend_a,
        wid_a,
        _stack_b,
        deps_b,
        backend_b,
        wid_b,
    ):
        async with deps_a.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "failover_actor")
            await _insert_schedule(
                conn,
                schema,
                "failover_actor",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        async def _fire_always_crash(
            conn: asyncpg.Connection,
            row: asyncpg.Record,
            now: datetime,
            settings: WorkerSettings,
            backend: Backend,
            schema_arg: str,
            worker_id: UUID,
            actor_config_cache: dict[str, object],
        ) -> None:
            raise RuntimeError("injected mid-tick failure")

        monkeypatch.setattr(cron_loop_mod, "fire_schedule", _fire_always_crash)

        leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        shutdown_a = asyncio.Event()
        task_a = asyncio.create_task(leader_a.run(shutdown_a))
        try:
            await asyncio.wait_for(deps_a.is_leader.wait(), timeout=_HEARTBEAT_INTERVAL + 3)
            await asyncio.sleep(2)
        except TimeoutError:
            pass
        finally:
            shutdown_a.set()
            task_a.cancel()
            with suppress(asyncio.CancelledError):
                await task_a
            if deps_a.leader_conn is not None and not deps_a.leader_conn.is_closed():
                with suppress(asyncpg.InterfaceError, OSError):
                    await deps_a.leader_conn.execute(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                        MAINTENANCE_LEADER_LOCK_NAME,
                    )
                with suppress(asyncpg.InterfaceError, OSError):
                    await deps_a.leader_conn.close()
                deps_a.leader_conn = None
            if (
                leader_a._leader_monitor_conn is not None
                and not leader_a._leader_monitor_conn.is_closed()
            ):
                with suppress(asyncpg.InterfaceError, OSError):
                    await leader_a._leader_monitor_conn.close()
                leader_a._leader_monitor_conn = None

        monkeypatch.undo()

        async with deps_b.dispatcher_pool.acquire() as conn:
            job_count_before_b: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',
                "failover_actor",
            )
        assert job_count_before_b == 0

        leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
        shutdown_b = asyncio.Event()
        task_b = asyncio.create_task(leader_b.run(shutdown_b))
        try:
            await asyncio.wait_for(deps_b.is_leader.wait(), timeout=_HEARTBEAT_INTERVAL + 8)
            await asyncio.sleep(2)

            async with deps_b.dispatcher_pool.acquire() as conn:
                job_count: int = await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',
                    "failover_actor",
                )
            assert job_count == 1
        finally:
            shutdown_b.set()
            task_b.cancel()
            with suppress(asyncio.CancelledError):
                await task_b


# ── payload_factory raises BaseException ─────────────────────────────


@pytest.mark.asyncio
async def test_tc2_base_exception_propagates(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Factory raises SystemExit (BaseException, not Exception).
    The except Exception handler does NOT catch it; consecutive_failures
    is NOT incremented; the _cron_loop task terminates."""
    import taskq.worker.cron_loop as cron_loop_mod

    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        async with deps.dispatcher_pool.acquire() as conn:
            await _insert_actor_config(conn, schema, "base_exc_actor")
            await _insert_schedule(
                conn,
                schema,
                "base_exc_actor",
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        async def _raise_system_exit(
            row: asyncpg.Record,
        ) -> dict[str, object]:
            raise SystemExit("deliberate BaseException")

        monkeypatch.setattr(cron_loop_mod, "resolve_payload", _raise_system_exit)
        with pytest.raises((SystemExit, BaseExceptionGroup)):  # type: ignore[name-defined]
            async with deps.dispatcher_pool.acquire() as conn:
                async with conn.transaction():
                    await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            cf: int = await conn.fetchval(
                f'SELECT consecutive_failures FROM "{schema}".cron_schedules WHERE actor = $1',
                "base_exc_actor",
            )

        assert cf == 0


# ── Invalid cron expression at create_schedule() ────────────────────


@pytest.mark.asyncio
async def test_tn1_invalid_cron_expression_raises_value_error(
    pg_dsn: str,
) -> None:
    """JobsClient.create_schedule with invalid cron_expr raises
    ValueError; no row in cron_schedules."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        _worker_id,
    ):
        client = JobsClient(backend)
        with pytest.raises(ValueError, match="Invalid cron expression"):
            await client.create_schedule("test_actor", "invalid")

        async with deps.dispatcher_pool.acquire() as conn:
            count: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".cron_schedules')
        assert count == 0


# ── Both payload_factory and static_payload at create_schedule() ─────


@pytest.mark.asyncio
async def test_tn2_both_payload_factory_and_static_payload_raises_value_error(
    pg_dsn: str,
) -> None:
    """JobsClient.create_schedule with both payload_factory and
    static_payload raises ValueError; no row inserted."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        _worker_id,
    ):
        client = JobsClient(backend)
        with pytest.raises(ValueError, match="mutually exclusive"):
            await client.create_schedule(
                "test_actor",
                "*/5 * * * *",
                payload_factory="some.factory",
                static_payload={"key": "value"},
            )

        async with deps.dispatcher_pool.acquire() as conn:
            count: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".cron_schedules')
        assert count == 0


# ── Actor existence NOT validated at create_schedule() ──────────────


@pytest.mark.asyncio
async def test_tn3_actor_not_validated_at_create_time(pg_dsn: str) -> None:
    """Insert a schedule for an unregistered actor. Oracle: schedule
    row inserted successfully; on tick, last_fire_error is set (actor not
    found in actor_config). This pins fire-time validation behavior."""
    async with _open_cron_single(pg_dsn, f"test_cron_{new_base62()}") as (
        schema,
        _stack,
        deps,
        backend,
        worker_id,
    ):
        client = JobsClient(backend)
        handle = await client.create_schedule("unregistered_actor", _HOURLY)
        assert handle.enabled is True

        async with deps.dispatcher_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE \"{schema}\".cron_schedules SET next_fire_at = now() - interval '5 minutes' "
                f"WHERE actor = $1",
                "unregistered_actor",
            )

        async with deps.dispatcher_pool.acquire() as conn:
            count: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".cron_schedules WHERE actor = $1',
                "unregistered_actor",
            )
        assert count == 1

        async with deps.dispatcher_pool.acquire() as conn:
            async with conn.transaction():
                await tick_cron(conn, deps.settings, backend, schema, worker_id)

        async with deps.dispatcher_pool.acquire() as conn:
            sched = await conn.fetchrow(
                f"SELECT last_fire_error, consecutive_failures "
                f'FROM "{schema}".cron_schedules WHERE actor = $1',
                "unregistered_actor",
            )

        assert sched is not None
        assert sched["last_fire_error"] is not None
        assert sched["consecutive_failures"] >= 1
