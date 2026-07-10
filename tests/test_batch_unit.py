"""Unit tests for batch.wait_for_batch and _dsn edge cases (no PG required).

``wait_for_batch`` is exercised with a fake asyncpg connection/pool to cover
the snooze-via-exception path, the polling path, and the empty-batch warning
without needing a live database.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest

from taskq._dsn import dsn_host
from taskq.batch import wait_for_batch
from taskq.exceptions import Snooze


class FakeRecord:
    def __init__(self, data: dict[str, int]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> int:
        return self._data[key]


class FakeConn:
    def __init__(self, row_data: dict[str, int] | None) -> None:
        self._row_data = row_data

    async def fetchrow(self, sql: str, *args: object) -> FakeRecord | None:
        if self._row_data is None:
            return None
        return FakeRecord(self._row_data)


class FakePool(asyncpg.Pool):  # type: ignore[misc]
    """asyncpg.Pool subclass that bypasses the real constructor."""

    def __init__(self, row_data: dict[str, int] | None) -> None:
        self._conn = FakeConn(row_data)

    def acquire(self) -> _PoolCtx:  # pyright: ignore[reportIncompatibleMethodOverride]  # Why: stub returns a minimal context manager; real Pool.acquire returns PoolAcquireContext with a timeout kwarg the tests never use.
        return _PoolCtx(self._conn)


class _PoolCtx:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> FakeConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


def test_dsn_host_returns_unknown_on_exception() -> None:
    """An object whose str() raises should yield 'unknown'."""

    class Bad:
        def __str__(self) -> str:
            raise RuntimeError("boom")

    assert dsn_host(Bad()) == "unknown"


def test_dsn_host_returns_unknown_on_none_hostname() -> None:
    assert dsn_host("postgresql://user:pass@/mydb") == "unknown"


def test_dsn_host_extracts_ipv4() -> None:
    assert dsn_host("postgresql://u:p@10.0.0.1:5432/db") == "10.0.0.1"


async def test_wait_for_batch_raises_snooze_when_in_flight() -> None:
    conn = FakeConn(
        {
            "total": 3,
            "succeeded": 1,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 2,
        }
    )
    with pytest.raises(Snooze):
        await wait_for_batch(conn, uuid4(), snooze_via_exception=True)


async def test_wait_for_batch_returns_status_when_all_terminal() -> None:
    conn = FakeConn(
        {
            "total": 2,
            "succeeded": 1,
            "failed": 1,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 0,
        }
    )
    status = await wait_for_batch(conn, uuid4(), snooze_via_exception=True)
    assert status.total == 2
    assert status.pending == 0
    assert status.succeeded == 1
    assert status.failed == 1
    assert status.is_complete is True


async def test_wait_for_batch_empty_batch_returns_complete() -> None:
    conn = FakeConn(
        {
            "total": 0,
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 0,
        }
    )
    status = await wait_for_batch(conn, uuid4(), snooze_via_exception=True)
    assert status.total == 0
    assert status.is_complete is True


async def test_wait_for_batch_clamps_small_snooze_interval() -> None:
    """snooze_interval below 1 second is clamped to 1 second."""
    conn = FakeConn(
        {
            "total": 1,
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 1,
        }
    )
    with pytest.raises(Snooze) as exc_info:
        await wait_for_batch(
            conn,
            uuid4(),
            snooze_interval=timedelta(milliseconds=100),
            snooze_via_exception=True,
        )
    assert exc_info.value.delay >= timedelta(seconds=1)


async def test_wait_for_batch_polling_path_terminates() -> None:
    """snooze_via_exception=False polls until all children are terminal."""
    row_data: dict[str, int] = {
        "total": 1,
        "succeeded": 0,
        "failed": 0,
        "cancelled": 0,
        "crashed": 0,
        "abandoned": 0,
        "in_flight": 1,
    }
    conn = FakeConn(row_data)

    # After the first sleep, flip the data to terminal.
    original_sleep = asyncio.sleep
    sleep_calls = 0

    async def fake_sleep(delta: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        conn._row_data = {
            "total": 1,
            "succeeded": 1,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 0,
        }
        await original_sleep(0)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(asyncio, "sleep", fake_sleep)
        status = await wait_for_batch(
            conn,
            uuid4(),
            snooze_interval=timedelta(seconds=1),
            snooze_via_exception=False,
        )
    assert sleep_calls == 1
    assert status.succeeded == 1
    assert status.is_complete is True


async def test_wait_for_batch_with_pool_acquires_connection() -> None:
    pool = FakePool(
        {
            "total": 1,
            "succeeded": 1,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 0,
        }
    )
    status = await wait_for_batch(pool, uuid4(), snooze_via_exception=True)
    assert status.succeeded == 1


async def test_wait_for_batch_fetchrow_none_returns_zero_status() -> None:
    conn = FakeConn(None)
    status = await wait_for_batch(conn, uuid4(), snooze_via_exception=True)
    assert status.total == 0
    assert status.is_complete is True


async def test_wait_for_batch_pool_polling_path_terminates() -> None:
    """snooze_via_exception=False with a Pool acquires a connection each poll."""
    pool = FakePool(
        {
            "total": 1,
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 1,
        }
    )
    original_sleep = asyncio.sleep

    async def fake_sleep(delta: float) -> None:
        pool._conn._row_data = {
            "total": 1,
            "succeeded": 1,
            "failed": 0,
            "cancelled": 0,
            "crashed": 0,
            "abandoned": 0,
            "in_flight": 0,
        }
        await original_sleep(0)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(asyncio, "sleep", fake_sleep)
        status = await wait_for_batch(
            pool,
            uuid4(),
            snooze_interval=timedelta(seconds=1),
            snooze_via_exception=False,
        )
    assert status.succeeded == 1
    assert status.is_complete is True
