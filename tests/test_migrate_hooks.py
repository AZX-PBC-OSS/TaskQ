"""Tests for migrate.apply_pending_locked connection hook points.

Verifies that ``conn``, ``conn_factory``, and ``dsn`` are mutually
exclusive and that caller-owned connections are not closed. Uses a fake
connection — no real Postgres required.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from taskq.migrate import apply_pending_locked


class _FakeConn:
    """Fake asyncpg.Connection tracking execute / close calls."""

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((sql, args))
        return "OK"

    async def fetch(self, sql: str, *args: object) -> list[object]:
        return []

    async def fetchval(self, sql: str, *args: object) -> bool:
        return False  # schema_migrations table doesn't exist

    def transaction(self) -> contextlib.AbstractAsyncContextManager[None]:
        @contextlib.asynccontextmanager
        async def _txn() -> AsyncGenerator[None]:
            yield None

        return _txn()

    async def close(self) -> None:
        self.closed = True


def _make_conn_factory(fake: _FakeConn) -> object:
    async def factory() -> asyncpg.Connection:
        return fake  # type: ignore[return-value]

    return factory


# ── Mutual exclusivity ─────────────────────────────────────────────────


async def test_conn_and_conn_factory_mutually_exclusive() -> None:
    """Providing both conn and conn_factory is a ValueError."""
    conn = _FakeConn()
    with pytest.raises(ValueError, match=r"conn.*conn_factory"):
        await apply_pending_locked(
            schema="taskq",
            conn=conn,  # type: ignore[arg-type]
            conn_factory=_make_conn_factory(conn),
        )


async def test_no_connection_source_raises() -> None:
    """Providing no connection source is a ValueError."""
    with pytest.raises(ValueError, match=r"dsn.*conn.*conn_factory"):
        await apply_pending_locked(schema="taskq")


# ── Caller-owned conn not closed ───────────────────────────────────────


async def test_caller_owned_conn_not_closed() -> None:
    """A pre-constructed conn is NOT closed by apply_pending_locked."""
    conn = _FakeConn()
    # Patch apply_pending to avoid real migration logic
    with patch("taskq.migrate.apply_pending", new=AsyncMock(return_value=[])):
        await apply_pending_locked(schema="taskq", conn=conn)  # type: ignore[arg-type]

    # Advisory lock was acquired and released
    lock_sqls = [sql for sql, _ in conn.executed if "pg_advisory_lock" in sql]
    unlock_sqls = [sql for sql, _ in conn.executed if "pg_advisory_unlock" in sql]
    assert len(lock_sqls) == 1
    assert len(unlock_sqls) == 1
    # NOT closed (caller-owned)
    assert not conn.closed


# ── Factory conn closed ────────────────────────────────────────────────


async def test_factory_conn_closed_on_completion() -> None:
    """A factory-produced conn IS closed by apply_pending_locked."""
    conn = _FakeConn()
    with patch("taskq.migrate.apply_pending", new=AsyncMock(return_value=[])):
        await apply_pending_locked(
            schema="taskq",
            conn_factory=_make_conn_factory(conn),
        )

    assert conn.closed


# ── DSN conn closed ────────────────────────────────────────────────────


async def test_dsn_conn_closed_on_completion() -> None:
    """A DSN-built conn IS closed by apply_pending_locked."""
    conn = _FakeConn()
    with (
        patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
        patch("taskq.migrate.apply_pending", new=AsyncMock(return_value=[])),
    ):
        await apply_pending_locked(dsn="postgresql://fake/fake", schema="taskq")

    assert conn.closed


# ── Advisory lock always released ──────────────────────────────────────


async def test_advisory_lock_released_on_failure() -> None:
    """The advisory lock is released even when apply_pending raises."""
    conn = _FakeConn()
    with (
        patch("taskq.migrate.apply_pending", new=AsyncMock(side_effect=RuntimeError("boom"))),
        pytest.raises(SystemExit, match="migration failed"),
    ):
        await apply_pending_locked(
            schema="taskq",
            conn_factory=_make_conn_factory(conn),
        )

    unlock_sqls = [sql for sql, _ in conn.executed if "pg_advisory_unlock" in sql]
    assert len(unlock_sqls) == 1
    # Factory conn still closed despite failure
    assert conn.closed
