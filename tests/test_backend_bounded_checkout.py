"""Unit tests for the bounded checkout on the read/schedule/batch paths.

The enqueue and bulk-cancel paths run their statements inside
``_RetryGuard.checkout``: the acquire is the pool's own
backpressure, but the RELEASE carries
``taskq.connections._POOL_RELEASE_RESET_TIMEOUT_SECS`` and never raises,
because asyncpg's context-manager release passes NO timeout (the holder
falls back to the acquire timeout, which is ``None`` for an unbounded
acquire) and a server that dies silently (no FATAL, no FIN: a
frozen/black-holed endpoint) parks that release's reset forever, wedging
the caller's task AND ``pool.close()``.

Every read, schedule, and batch path still used the bare
``async with pool.acquire()``, the identical unbounded-release hang
shape. These tests drive the swept sites through a pool miniature that
models what asyncpg does at source level (a dual-surface acquire context
whose context-manager release passes no timeout; a holder release that
parks against a silently-dead server and, under a budget, terminates the
connection and re-raises), mirroring
``tests/test_connections.py``'s enqueue-path bound pins.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._reads import _get
from taskq.backend._schedules import ScheduleSql, delete_schedule

_TEST_BUDGET_S = 2.0
_PARK_SECS = 10.0


class _ParkedConn:
    """Stand-in for a checked-out pool connection proxy."""

    def __init__(self) -> None:
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True

    async def fetchrow(self, *args: object) -> None:
        return None

    async def execute(self, *args: object) -> str:
        return "DELETE 0"


class _AcquireContext:
    """asyncpg's ``PoolAcquireContext`` in miniature: BOTH awaitable and
    async context manager. The context manager's ``__aexit__`` is the
    unbounded shape these sites shipped with: it releases with NO timeout,
    which is what parks the reset against a silently-dead server."""

    def __init__(self, pool: _DeadServerPool) -> None:
        self._pool = pool
        self._conn: _ParkedConn | None = None

    def __await__(self) -> Any:
        conn = _ParkedConn()
        self._pool.in_use += 1
        self._conn = conn
        return self._acquire().__await__()

    async def _acquire(self) -> _ParkedConn:
        assert self._conn is not None
        return self._conn

    async def __aenter__(self) -> _ParkedConn:
        return await self

    async def __aexit__(self, *exc: object) -> None:
        assert self._conn is not None
        # asyncpg's context release: pool.release(conn) with NO timeout;
        # the holder falls back to the acquire timeout (None: unbounded).
        await self._pool.release(self._conn)


class _DeadServerPool:
    """Fake ``asyncpg.Pool`` whose release PARKS: a server that never
       answers (no FATAL, no FIN: a frozen/black-holed endpoint), the shape
       the release bound exists for. Models asyncpg's
       ``PoolConnectionHolder.release`` at source level: the reset is awaited
       UNDER the budget; on expiry the connection is TERMINATED and the
       timeout re-raised (asyncpg's timeout handler does exactly this),
       freeing the holder either way; with no budget it parks forever:
    the unbounded hang shape."""

    def __init__(self, *, park_secs: float = _PARK_SECS) -> None:
        self.park_secs = park_secs
        self.in_use = 0
        self.releases = 0
        self.release_timeouts: list[float | None] = []
        self.terminated_conns: list[_ParkedConn] = []

    def acquire(self, *, timeout: float | None = None) -> _AcquireContext:
        self.acquire_timeouts: list[float | None] = getattr(self, "acquire_timeouts", [])
        self.acquire_timeouts.append(timeout)
        return _AcquireContext(self)

    async def release(
        self,
        conn: _ParkedConn,
        *,
        # Why: the parameter models asyncpg's Pool.release(timeout=...) signature: the bound-delivery channel under test, not a cancel scope.
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:
        self.releases += 1
        self.release_timeouts.append(timeout)
        try:
            await asyncio.wait_for(asyncio.sleep(self.park_secs), timeout=timeout)
        except TimeoutError:
            conn.terminate()
            self.terminated_conns.append(conn)
            raise
        finally:
            self.in_use -= 1


async def _run_get_through_dead_server(
    pool: _DeadServerPool,
    shrunk_bound: float,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[object, float, Any]:
    """Drive :func:`taskq.backend._reads._get` through the parked-release
    pool under the shrunk bound, returning (result, elapsed, logs)."""
    from taskq import connections as connections_mod

    monkeypatch.setattr(connections_mod, "_POOL_RELEASE_RESET_TIMEOUT_SECS", shrunk_bound)

    class _Sql:
        get_job = "SELECT 1"
        get_archived_job = "SELECT 1"

    started = time.monotonic()
    with structlog.testing.capture_logs() as logs:
        async with asyncio.timeout(_TEST_BUDGET_S):
            result = await _get(pool, _Sql(), new_uuid())  # pyright: ignore[reportArgumentType]  # Why: the fake pool models asyncpg's dual-surface acquire and holder release faithfully; a real asyncpg.Pool is not needed for the release-bound contract under test.
    elapsed = time.monotonic() - started
    return result, elapsed, logs


async def test_reads_get_release_is_bounded_against_a_silently_dead_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repro on one previously-unbounded read site: ``_get``'s
    release, sent into a server that never answers, must cost the shrunk
    bound: a typed, bounded outcome instead of parking the caller (and
    ``pool.close()``) forever. The unbounded shape this replaces waited
    the full park: the test's own 2 s budget firing IS the red result."""
    shrunk_bound = 0.2
    pool = _DeadServerPool(park_secs=_PARK_SECS)

    result, elapsed, logs = await _run_get_through_dead_server(pool, shrunk_bound, monkeypatch)

    assert result is None, "the op's own result must survive a parked release"
    assert elapsed < 1.0, (
        f"the parked release cost {elapsed:.2f}s; the bound is {shrunk_bound}s; "
        "only the bound plus epsilon should have elapsed"
    )
    assert pool.release_timeouts == [shrunk_bound], (
        "the bound must be the one handed to release: the unbounded "
        "context-manager release never passed one"
    )
    assert len(pool.terminated_conns) == 1, "bound expiry must terminate the parked connection"
    assert pool.terminated_conns[0].terminated
    assert pool.in_use == 0, "the holder must be freed: a wedged holder is pool.close() stuck"
    warnings = [e for e in logs if e.get("event") == "pool-release-failed"]
    assert len(warnings) == 1, "the swallowed timeout is the operator's signal"
    assert warnings[0]["kind"] == "pool_release_failed"
    assert warnings[0]["operation"] == "get"


async def test_schedules_delete_release_is_bounded_against_a_silently_dead_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same bound on a second previously-unbounded family (schedule
    CRUD): ``delete_schedule``'s release must carry the bound, terminate
    the parked connection, and let the op's own outcome stand."""
    from taskq import connections as connections_mod

    shrunk_bound = 0.2
    monkeypatch.setattr(connections_mod, "_POOL_RELEASE_RESET_TIMEOUT_SECS", shrunk_bound)
    pool = _DeadServerPool(park_secs=_PARK_SECS)
    sql = ScheduleSql.build("test_schema")

    started = time.monotonic()
    with structlog.testing.capture_logs() as logs:
        async with asyncio.timeout(_TEST_BUDGET_S):
            await delete_schedule(pool, sql, new_uuid())  # pyright: ignore[reportArgumentType]  # Why: the fake pool models asyncpg's dual-surface acquire and holder release faithfully; a real asyncpg.Pool is not needed for the release-bound contract under test.
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, (
        f"the parked release cost {elapsed:.2f}s; the bound is {shrunk_bound}s; "
        "only the bound plus epsilon should have elapsed"
    )
    assert pool.release_timeouts == [shrunk_bound], (
        "the bound must be the one handed to release: the unbounded "
        "context-manager release never passed one"
    )
    assert pool.in_use == 0, "the holder must be freed: a wedged holder is pool.close() stuck"
    warnings = [e for e in logs if e.get("event") == "pool-release-failed"]
    assert len(warnings) == 1, "the swallowed timeout is the operator's signal"
    assert warnings[0]["operation"] == "delete_schedule"
