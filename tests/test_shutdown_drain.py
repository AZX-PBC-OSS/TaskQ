"""Unit tests for drain_local_queue_to_pending and ShutdownPhase enum."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
import pytest

from taskq._ids import new_uuid
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
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": schema_name},
        validate=False,  # Why: allows an intentionally-invalid schema name to test the shutdown.py-level _IDENT_RE gate.
    )


# ── Enum-value sanity test ────────────────────────────


def test_shutdown_phase_values() -> None:
    """Assert ShutdownPhase enum values match the ordering."""
    assert ShutdownPhase.NONE == 0
    assert ShutdownPhase.DRAINING == 1
    assert ShutdownPhase.CANCELLING == 2
    assert ShutdownPhase.FORCING == 3
    assert ShutdownPhase.ABANDONING == 4


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
