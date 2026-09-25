"""Unit tests for drain_local_queue_to_pending and ShutdownPhase enum."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast
from unittest.mock import MagicMock

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import ShutdownPhase, drain_local_queue_to_pending

# ── Minimal fakes ──────────────────────────────────────────────────────────


class FakeConn:
    """Lightweight asyncpg.Connection stand-in for drain tests."""

    def __init__(self, *, fail_execute_with: BaseException | None = None) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._fail_execute_with = fail_execute_with

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._fail_execute_with is not None:
            raise self._fail_execute_with
        return "UPDATE 3"


class FakePool:
    """Lightweight asyncpg.Pool stand-in for drain tests."""

    def __init__(
        self,
        *,
        fail_acquire_with: BaseException | None = None,
        conn_fail_execute_with: BaseException | None = None,
    ) -> None:
        self._fail_acquire_with = fail_acquire_with
        self._conn_fail_execute_with = conn_fail_execute_with
        self.acquire_count = 0
        self._last_conn: FakeConn | None = None

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109 # Why: asyncpg.Pool.acquire signature takes timeout; FakePool mirrors it.
        self.acquire_count += 1
        self.last_acquire_timeout = timeout
        if self._fail_acquire_with is not None:
            raise self._fail_acquire_with
        conn = FakeConn(fail_execute_with=self._conn_fail_execute_with)
        self._last_conn = conn
        yield conn

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        if self._last_conn is None:
            return []
        return self._last_conn.execute_calls


# ── Helper to build deps ───────────────────────────────────────────────────


def _worker_settings(schema_name: str = "taskq") -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": schema_name},
    )


def _worker_settings_no_validate(schema_name: str = "taskq") -> WorkerSettings:
    """Settings carrying an arbitrary schema name, bypassing field validation.

    `schema_name` is validated by a `validator=` hook, which dotenvmodel runs on
    every load path INCLUDING `validate=False` -- deliberately, because it is
    the one setting that reaches raw SQL as an interpolated identifier. So an
    invalid name can no longer be loaded, and the object is mutated after
    construction instead.

    That is the point of these tests: they assert the shutdown path performs its
    OWN `_IDENT_RE` check rather than trusting settings. That defence in depth
    is what keeps the settings-level guard from being a single point of failure,
    so it still needs proving.
    """
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": "taskq"},
        validate=False,
    )
    settings.schema_name = schema_name
    return settings


# ── Enum-value sanity test ────────────────────────────


def test_shutdown_phase_values() -> None:
    """Assert ShutdownPhase enum values match the ordering."""
    assert ShutdownPhase.NONE == 0
    assert ShutdownPhase.DRAINING == 1
    assert ShutdownPhase.CANCELLING == 2
    assert ShutdownPhase.FORCING == 3
    assert ShutdownPhase.RELEASING == 4


# ── SQL shape ──────────────────────────────────────────────────────


async def test_drain_sql_shape() -> None:
    """drain_local_queue_to_pending drains running jobs via the pool and
    returns the correct count. Verifies the configured schema name is used."""
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type] # Why: heartbeat_pool not used by drain but WorkerDeps requires it.
        worker_pool=pool,  # type: ignore[arg-type] # Why: worker_pool not used by drain but WorkerDeps requires it.
        notify_conn=None,
        leader_conn=None,
    )

    rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 3
    assert pool.acquire_count == 1
    assert pool.last_acquire_timeout == 2.0
    assert len(pool.execute_calls) == 1

    sql, args = pool.execute_calls[0]
    assert args == (worker_id,)
    assert "UPDATE" in sql
    assert "jobs" in sql
    assert settings.schema_name in sql


async def test_drain_pool_exhaustion_returns_zero() -> None:
    """asyncio.TimeoutError on acquire → returns 0, logs warning, no raise."""
    worker_id = new_uuid()
    pool = FakePool(fail_acquire_with=TimeoutError("timed out"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 0


# ── PG connection error ────────────────────────────────────────


async def test_drain_pg_connection_error_returns_zero() -> None:
    """asyncpg.PostgresConnectionError on execute → returns 0, logs warning, no raise."""
    worker_id = new_uuid()
    pool = FakePool(conn_fail_execute_with=asyncpg.PostgresConnectionError("gone"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 0


# ── schema validation ──────────────────────────────────────────


async def test_drain_invalid_schema_raises() -> None:
    """Invalid schema raises ValueError before any pool acquisition."""
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings_no_validate("foo;DROP TABLE")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    with pytest.raises(ValueError) as exc_info:
        await drain_local_queue_to_pending(deps, worker_id)

    assert "invalid schema identifier" in str(exc_info.value)
    assert "foo;DROP TABLE" in str(exc_info.value)
    assert pool.acquire_count == 0


async def test_drain_local_queue_uses_transient_pg_errors_not_handrolled() -> None:
    """asyncpg.InterfaceError (e.g. a closed pool) on execute must be handled
    as transient - logged and rowcount 0 - not propagated as fatal.

    InterfaceError is in TRANSIENT_PG_ERRORS but was missing from an earlier
    hand-rolled (TimeoutError, PostgresConnectionError) tuple; this drives an
    actual InterfaceError through the drain path to pin the fix."""
    import structlog.testing

    worker_id = new_uuid()
    pool = FakePool(conn_fail_execute_with=asyncpg.InterfaceError("pool is closed"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    with structlog.testing.capture_logs() as captured:
        rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 0, "InterfaceError must be treated as transient, not fatal"
    assert any(entry.get("event") == "drain-local-queue-failed" for entry in captured), (
        f"expected a drain-local-queue-failed warning, got: {captured}"
    )


# ── Hand-back excludes jobs a consumer is already executing ────────


async def test_drain_excludes_jobs_a_consumer_is_already_executing() -> None:
    """A job with a live consumer on this worker is never handed back to pending.

    The hand-back exists for rows this worker claimed but never started: the
    local_queue backlog. A row whose consumer is already running is the exact
    opposite case - clearing its lock would publish it to the fleet while this
    worker's consumer is still inside the actor body, so the job body runs a
    second time on the claimer while the first run is still in flight. The
    cancelling / forcing / abandoning phases own those rows and drive them to
    a terminal state; DRAINING must leave them alone.

    The discriminator is this process's own in-flight registry, because the
    row itself carries no "a consumer took it" mark - the dispatch claim
    stamps started_at at claim time for every claimed row alike.
    """
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    running_ids = [JobId(new_uuid()) for _ in range(2)]
    for jid in running_ids:
        await deps.active_jobs.register(
            jid,
            cast("asyncio.Task[object]", MagicMock()),
            cast("JobContext[Any]", MagicMock()),
        )

    await drain_local_queue_to_pending(deps, worker_id)

    assert len(pool.execute_calls) == 1
    sql, args = pool.execute_calls[0]
    assert args[0] == worker_id
    assert len(args) == 2, (
        "the hand-back UPDATE must bind the in-flight job ids so rows with a "
        f"live consumer are excluded from the re-pend; bound args were {args!r}"
    )
    assert set(cast("list[object]", args[1])) == set(running_ids), (
        "every job registered as in-flight on this worker must appear in the "
        f"exclusion list; got {args[1]!r} for registry {running_ids!r}"
    )
    assert "<>" in sql or "NOT" in sql.upper(), (
        f"the bound in-flight ids must be used as an exclusion predicate: {sql!r}"
    )


async def test_drain_hands_back_at_most_once_across_repeated_calls() -> None:
    """A second hand-back pass re-pends nothing that the first already released.

    The hand-back predicate is scoped to rows still locked by this worker, so a
    retried or re-entered DRAINING phase cannot release a row twice - the first
    pass cleared the lock, and the second matches nothing. This is what makes
    "handed back exactly once" a property of the statement rather than of
    shutdown being called exactly once: a drain-monitor trigger racing a
    SIGTERM, or a retry after a transient failure, must not reopen a row that
    another worker has since re-claimed and started.
    """
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    await drain_local_queue_to_pending(deps, worker_id)

    sql, args = pool.execute_calls[0]
    assert args[0] == worker_id
    normalised = " ".join(sql.split())
    assert "locked_by_worker=$1" in normalised.replace(" = ", "=")
    assert "status='running'" in normalised.replace(" = ", "=").replace('"', "'"), (
        "the hand-back must be scoped to rows this worker still holds "
        f"(locked_by_worker + status='running'): {normalised!r}"
    )
    assert "locked_by_worker=NULL" in normalised.replace(" = ", "="), (
        f"the hand-back must clear the lock, so a second pass matches nothing: {normalised!r}"
    )
    assert "status='pending'" in normalised.replace(" = ", "="), (
        f"the hand-back must set the row back to pending: {normalised!r}"
    )


async def test_drain_exclusion_captures_a_take_that_lands_during_the_pool_acquire() -> None:
    """A claim intent marked while the drain waits for its connection is excluded.

    The exclusion arrays are the row's classification ("a consumer of this
    process holds this row"), and the UPDATE is the writer that acts on it.
    The registry's maps are in-memory; the pool acquire is the one await
    between the helper's entry and its statement, and on a contended pool it
    parks the drain for as long as the contention lasts - exactly the window
    a consumer's take (mark_claimed) or registration lands in. A capture
    taken BEFORE the acquire binds a stale exclusion: the UPDATE re-pends the
    row under its live consumer, the consumer's terminal write fences out
    (the fence requires status='running' AND locked_by_worker, both cleared
    by the re-pend), and the row is stranded 'pending' with its body
    mid-flight. The capture must therefore happen AFTER the acquire,
    immediately before the bind: the bound exclusion carries the intent the
    acquire's park let land.

    The fake pool marks the take inside ``acquire`` - the take's exact
    timing, deterministically. RED on a capture-before-acquire tree: the
    exclusion binds empty and the UPDATE runs with no ``<> ALL`` predicate.
    """
    worker_id = new_uuid()
    taken_id = JobId(new_uuid())
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    real_acquire = pool.acquire

    @asynccontextmanager
    async def acquire_marking_the_take(
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's signature, the same convention FakePool.acquires uses.
    ) -> AsyncGenerator[FakeConn, None]:
        # The consumer's take: the row is claimed (running, locked) and this
        # loop's take records the claim intent - the registry's own fast
        # path, no awaits, exactly what mark_claimed does at a real take.
        deps.active_jobs.mark_claimed(taken_id)
        async with real_acquire(timeout=timeout) as conn:
            yield conn

    cast("Any", deps.dispatcher_pool).acquire = acquire_marking_the_take

    await drain_local_queue_to_pending(deps, worker_id)

    sql, args = pool.execute_calls[0]
    assert len(args) == 2, (
        "the drain captured its exclusion BEFORE the pool acquire: the take "
        f"that landed during the acquire's park is missing from the bound "
        f"exclusion (bound args {args!r}), and the UPDATE re-pends a row its "
        "consumer is executing - the terminal write fences out and the row "
        "strands 'pending'"
    )
    assert set(cast("list[object]", args[1])) == {taken_id}, (
        "the take that landed during the pool acquire must appear in the "
        f"exclusion list; got {args[1]!r}"
    )
    assert "<>" in sql or "NOT" in sql.upper(), (
        f"the exclusion must be a bound predicate of the UPDATE: {sql!r}"
    )
