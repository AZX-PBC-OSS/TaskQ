# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""GUC-hygiene pins for the rate limiter's PG fallback acquire paths.

Hunt context: the SET LOCAL capture/restore discipline of the backend
sweeps is pinned by ``test_rt_sweeps_timeout_leak.py`` (statement_timeout);
these files are the OTHER GUC sites. The token-bucket PG acquire
(``TokenBucket._acquire_pg``) and the GCRA sliding-window acquire
(``_sliding_window_pg._acquire_pg_gcra``) each bind a bounded row-lock
wait with ``set_config('lock_timeout', ..., true)`` - SET LOCAL
semantics - issued INSIDE the acquire's own transaction and deliberately
WITHOUT a save/restore cycle ("dies with the transaction's own commit",
per both call sites' comments). That is a different discipline from the
sweeps', so it needs its own pin: the limiter's denial path must not
leak the 250 ms wait bound onto the pooled session it hands back.

The observable is asserted on the SAME session the limiter used: every
acquire goes through a single-connection pool adapter, so after a
fail-closed denial (a racing transaction holds the bucket row's FOR
UPDATE lock past the budget) the test reads
``current_setting('lock_timeout')`` on that exact session. The SET LOCAL
must have died with the denial transaction's commit/rollback; anything
else would silently bound every subsequent statement whoever next
acquires that pooled connection issues.

Green pins (safe-unpinned): both limiter styles' denial transactions
are self-cleaning today. The failure mode they guard against is a
refactor that moves the GUC set outside the transaction (or converts it
to a session-level ``SET``) - then every pooled session that ever hit a
contended bucket permanently runs under a foreign lock budget.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.ratelimit._sliding_window_pg import _acquire_pg_gcra
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration

_LOCK_BUDGET_MS = 250
_BOUNDED_WAIT_S = 8.0


class _SingleConnCtx:
    """async-contextmanager handout of one fixed session."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _SingleConnPool:
    """Duck-typed pool whose every acquire hands out the SAME real session.

    Why not a real pool: the limiter must run on a session the test can
    still address afterwards - a real pool's release makes the session
    anonymous, so a GUC leak would be invisible to exactly the test that
    exists to catch it.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self, timeout: float | None = None) -> _SingleConnCtx:
        return _SingleConnCtx(self._conn)


async def _fresh_schema(pg_dsn: str) -> str:
    schema = f"tpl_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return schema


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema},
        validate=False,
    )


async def _deny_under_held_row(
    pg_dsn: str,
    schema: str,
    acquire: Any,
    hold_row: Any,
) -> tuple[Any, str, asyncpg.Connection]:
    """Run *acquire* twice on one session with the bucket row FOR UPDATE held
    between the runs; return (denied decision, session's lock_timeout after)."""
    conn = await asyncpg.connect(pg_dsn)
    racer = await asyncpg.connect(pg_dsn)
    try:
        pool = _SingleConnPool(conn)
        first = await asyncio.wait_for(acquire(pool), timeout=_BOUNDED_WAIT_S)
        assert first.allowed is True, "the uncontended first acquire must succeed"
        baseline = await conn.fetchval("SELECT current_setting('lock_timeout')")
        assert baseline == "0", (
            f"the limiter's own FIRST acquire already leaked lock_timeout "
            f"({baseline!r}) onto the session - the SET LOCAL did not die with "
            "its transaction, so every statement after it runs under a foreign "
            "wait budget."
        )

        racer_tx = racer.transaction()
        await racer_tx.start()
        await hold_row(racer)
        try:
            denied = await asyncio.wait_for(acquire(pool), timeout=_BOUNDED_WAIT_S)
        finally:
            await racer_tx.rollback()
        after = await conn.fetchval("SELECT current_setting('lock_timeout')")
        return denied, after, conn
    except BaseException:
        await conn.close()
        await racer.close()
        raise


async def test_token_bucket_denial_does_not_leak_lock_timeout_onto_the_session(
    pg_dsn: str,
) -> None:
    """The token-bucket PG acquire's SET LOCAL lock_timeout dies with its
    denial transaction - the pooled session comes back at the default '0'."""
    schema = await _fresh_schema(pg_dsn)
    settings = _settings(pg_dsn, schema)
    bucket = TokenBucket(name="tb_guc_pin", capacity=3, refill_per_second=1.0, backend="postgres")

    async def acquire(pool: Any) -> Any:
        return await bucket._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the private acquire is the exact code path under test - the GUC set lives inside it, not in the public dispatch.
            1.0, pool, settings, lock_timeout_ms=_LOCK_BUDGET_MS
        )

    async def hold_row(racer: asyncpg.Connection) -> None:
        await racer.execute(
            f'SELECT state FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1 FOR UPDATE',
            bucket.name,
        )

    denied, after, conn = await _deny_under_held_row(pg_dsn, schema, acquire, hold_row)
    try:
        assert denied.allowed is False, (
            "a bucket row held FOR UPDATE past the 250 ms budget must produce "
            "the limiter's fail-closed DENIAL, never an admission and never an "
            "exception - the row was never read, so no token was spent."
        )
        assert after == "0", (
            "TokenBucket._acquire_pg bound its row-lock wait with "
            f"set_config('lock_timeout', '{_LOCK_BUDGET_MS}ms', true) inside its "
            "own transaction; the denial path's commit must discard it, but "
            f"the same session now reads lock_timeout={after!r} - the budget "
            "leaked onto the pooled session and now bounds whoever acquires "
            "it next."
        )
    finally:
        await conn.close()


async def test_gcra_window_denial_does_not_leak_lock_timeout_onto_the_session(
    pg_dsn: str,
) -> None:
    """The GCRA sliding-window PG acquire's SET LOCAL lock_timeout dies with
    its denial transaction - same discipline, second code path."""
    schema = await _fresh_schema(pg_dsn)
    settings = _settings(pg_dsn, schema)
    window = SlidingWindow(
        name="sw_guc_pin",
        limit=3,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )

    async def acquire(pool: Any) -> Any:
        return await _acquire_pg_gcra(  # pyright: ignore[reportPrivateUsage]  # Why: same rationale as the token-bucket pin - the GUC set lives inside this private function.
            window, pool, settings, lock_timeout_ms=_LOCK_BUDGET_MS
        )

    async def hold_row(racer: asyncpg.Connection) -> None:
        await racer.execute(
            f'SELECT state FROM "{schema}".rate_limit_buckets '
            "WHERE bucket_name = $1 AND kind = 'gcra' FOR UPDATE",
            window.name,
        )

    denied, after, conn = await _deny_under_held_row(pg_dsn, schema, acquire, hold_row)
    try:
        assert denied.allowed is False, (
            "a GCRA bucket row held FOR UPDATE past the 250 ms budget must "
            "produce the limiter's fail-closed DENIAL - the TAT was never "
            "read, so it must never advance."
        )
        assert after == "0", (
            "_acquire_pg_gcra bound its row-lock wait with "
            f"set_config('lock_timeout', '{_LOCK_BUDGET_MS}ms', true) inside "
            "its own transaction; the denial path's commit must discard it, "
            f"but the same session now reads lock_timeout={after!r} - the "
            "budget leaked onto the pooled session and now bounds whoever "
            "acquires it next."
        )
    finally:
        await conn.close()
