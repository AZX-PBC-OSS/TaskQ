# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team locks: a failed prune-session unlock strands the fleet's lock on a live pooled session.

The prune loop takes its session-level advisory lock with
``pg_try_advisory_lock`` and releases it in the attempt's ``finally`` via
``with contextlib.suppress(*TRANSIENT_PG_ERRORS): await conn.execute(
"SELECT pg_advisory_unlock(...)")`` (``worker/_leader_sweeps.py``).
``QueryCanceledError`` (server-side 57014 cancel) and ``TimeoutError``
(client-side command timeout) are BOTH in the transient set
(``worker/_transient.py``), so a cancel landing mid-unlock is swallowed
silently while the connection SURVIVES — the session keeps the lock, the
conn returns to the pool, and every later prune attempt on any pod logs
``prune-skipped-advisory-lock-held`` until that session dies.

The RED contract: a failed unlock on a still-live session must terminate
the session (or otherwise not strand the fleet's lock). The real
``_prune_loop`` is driven end-to-end against real PG with its unlock
statement failing on the pooled conn; the leak is asserted from a second
raw session. Every wait is bounded so the RED failure can never hang the
suite.
"""

import asyncio
import contextlib
from datetime import datetime
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.constants import schema_lock_name
from taskq.migrate import apply_pending
from taskq.testing.settings import make_integration_settings
from taskq.worker import _leader_sweeps
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _prune_loop
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration


class _UnlockFailingConn:
    """Delegates everything to the wrapped real conn; raises *fail_with* on
    the ``pg_advisory_unlock`` statement BEFORE it reaches PG — a cancel
    arriving mid-unlock — and records the failure plus lock attempts."""

    def __init__(self, conn: asyncpg.Connection, fail_with: type[BaseException]) -> None:
        self._conn = conn
        self._fail_with = fail_with
        self.unlock_failed = asyncio.Event()
        self.lock_attempts = 0

    def transaction(self, **kwargs: object) -> Any:
        return self._conn.transaction(**kwargs)

    async def execute(
        self,
        query: str,
        *args: object,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's per-statement timeout parameter (and ChaosConnection's own signature) — a delegating wrapper, not a waitable-with-deadline API.
    ) -> str:
        if "pg_advisory_unlock" in query:
            self.unlock_failed.set()
            raise self._fail_with("chaos: unlock canceled mid-statement")
        return await self._conn.execute(query, *args, timeout=timeout)

    async def fetchval(
        self,
        query: str,
        *args: object,
        column: int = 0,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's per-statement timeout parameter — a delegating wrapper, not a waitable-with-deadline API.
    ) -> object | None:
        if "pg_try_advisory_lock" in query:
            self.lock_attempts += 1
        return await self._conn.fetchval(query, *args, column=column, timeout=timeout)

    async def fetchrow(
        self,
        query: str,
        *args: object,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's per-statement timeout parameter — a delegating wrapper, not a waitable-with-deadline API.
    ) -> object:
        return await self._conn.fetchrow(query, *args, timeout=timeout)

    async def fetch(
        self,
        query: str,
        *args: object,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's per-statement timeout parameter — a delegating wrapper, not a waitable-with-deadline API.
    ) -> list[Any]:
        return await self._conn.fetch(query, *args, timeout=timeout)


class _HeldPool:
    """Pool double yielding the wrapper conn — the dispatcher pool handing
    out (and taking back) the session that holds the prune lock. Release is
    a no-op: the wrapped real conn is test-owned and stays ALIVE, exactly
    the leak shape (the conn returns to the pool with the lock held)."""

    def __init__(self, conn: _UnlockFailingConn) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> "_PoolAcquireCtx":
        return _PoolAcquireCtx(self._conn)


class _PoolAcquireCtx:
    def __init__(self, conn: _UnlockFailingConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _UnlockFailingConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        return None


class _StubBackend:
    """SweepContext.backend stand-in: the prune loop only calls
    prune_old_batches, best-effort inside its own except."""

    async def prune_old_batches(self, cutoff: object) -> int:
        return 0


@pytest.mark.parametrize(
    ("fail_with", "label"),
    [
        (asyncpg.QueryCanceledError, "server-side 57014 cancel (pg_cancel_backend mid-unlock)"),
        (TimeoutError, "client-side command timeout mid-unlock"),
    ],
)
async def test_failed_unlock_leaks_prune_lock_on_live_pooled_session(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch, fail_with: type[BaseException], label: str
) -> None:
    """RED: a failed pg_advisory_unlock on a still-live pooled session must
    not leave the fleet's prune lock stranded.

    Contract: when the unlock statement fails while the connection
    survives, the session must be terminated (or the lock otherwise
    released) so another pod can prune. Today the contract is violated:
    worker/_leader_sweeps.py wraps the unlock in
    ``contextlib.suppress(*TRANSIENT_PG_ERRORS)`` with no other action, and
    both failure shapes here are transient per worker/_transient.py — so
    the lock stayed held on the surviving session and a second session's
    ``pg_try_advisory_lock`` returned false: every pod skips pruning until
    that session dies. The real _prune_loop is driven end-to-end; only its
    cron sleep is patched to fire immediately (timing, not behavior).
    """
    schema = f"tlck_{new_base62()}".lower()
    conn_c: asyncpg.Connection | None = None
    conn_d: asyncpg.Connection | None = None
    loop_task: asyncio.Task[None] | None = None
    shutdown = asyncio.Event()
    lock_name = schema_lock_name("prune", schema)
    try:
        conn_c = await asyncpg.connect(pg_dsn)
        await apply_pending(conn_c, schema=schema)
        wrapper = _UnlockFailingConn(conn_c, fail_with)
        pool = _HeldPool(wrapper)
        settings = make_integration_settings(pg_dsn, schema_name=schema, prune_schedule_utc="03:00")
        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: pool double stands in for asyncpg.Pool at the loop's only pool touchpoint.
            heartbeat_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: same stand-in.
            worker_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: same stand-in.
            notify_conn=None,
            leader_conn=None,
        )
        deps.is_leader.set()
        ctx = SweepContext(
            deps=deps, backend=_StubBackend(), clock=SystemClock(), worker_id=new_uuid()
        )

        async def _immediate_sleep(
            shutdown: asyncio.Event, next_fire: datetime, retry_backoff: float | None
        ) -> bool:
            await asyncio.sleep(0.05)
            return False

        monkeypatch.setattr(_leader_sweeps, "_sleep_until_next_attempt", _immediate_sleep)
        loop_task = asyncio.create_task(_prune_loop(ctx, shutdown), name="prune-unlock-leak")
        conn_d = await asyncpg.connect(pg_dsn)

        await asyncio.wait_for(wrapper.unlock_failed.wait(), timeout=30.0)
        await asyncio.sleep(0.5)
        assert wrapper.lock_attempts == 1, (
            "fixture fidelity: the prune attempt itself must have SUCCEEDED — a failed "
            "attempt retries immediately under the patched sleep and re-acquires the "
            "session lock (attempt count "
            f"{wrapper.lock_attempts})"
        )
        assert conn_c is not None
        c_alive = (await conn_c.fetchval("SELECT 1")) == 1
        acquired = await conn_d.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        assert acquired is True or not c_alive, (
            "Contract: a failed pg_advisory_unlock on a still-live session must not "
            "strand the fleet's prune lock — the session must be terminated (or the "
            "lock otherwise released) so another pod can prune. Today the contract is "
            f"violated ({label}): worker/_leader_sweeps.py's finally wraps the unlock "
            "in `contextlib.suppress(*TRANSIENT_PG_ERRORS)` and both QueryCanceledError "
            "and TimeoutError are transient per worker/_transient.py, so the unlock "
            "failure was swallowed with no other action — the pooled session is still "
            f"alive (SELECT 1 -> {c_alive}) and still holds the prune lock "
            f"(pg_try_advisory_lock from a second session -> {acquired}); every pod "
            "logs prune-skipped-advisory-lock-held until that session dies"
        )
        if acquired is True:
            await conn_d.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)
    finally:
        shutdown.set()
        if loop_task is not None and not loop_task.done():
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
                await asyncio.wait_for(loop_task, timeout=5.0)
        if conn_d is not None:
            with contextlib.suppress(Exception):
                await conn_d.close()
        if conn_c is not None:
            with contextlib.suppress(Exception):
                await conn_c.close()
        with contextlib.suppress(Exception):
            conn = await asyncpg.connect(pg_dsn)
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                with contextlib.suppress(Exception):
                    await conn.close()
