"""Revert-detectors for the batch guards on the two one-shot bounded drains.

The maintenance sweeps' boundary validation, statement-timeout wiring and
save/restore are pinned by ``tests/test_rt_sweeps_boundary.py`` and
``tests/test_rt_sweeps_timeout_leak.py``.  The bulk cancel and the
force-deregistration drain are separate call sites with their own wiring of
the same helpers — a fix that lands on one path and not its sibling is
exactly the drift these tests exist to catch.

Three guards, each with a test that goes red if the guard is removed:

1.  **Boundary validation** — ``batch_size=0`` is a legal rowless query: a
    drain whose window never fills past a zero cap never terminates (the
    infinite-drain shape), while ``statement_timeout_ms=0`` disables the
    batch's safety net outright.  Both must be rejected at the typed
    boundary, BEFORE any database access.
2.  **Statement-timeout enforcement on the cancel drain** — the batch
    transaction runs under ``SET LOCAL statement_timeout``, so a batch whose
    driving statement exceeds the bound is aborted server-side
    (``QueryCanceledError``) rather than silently slow.
3.  **Statement-timeout enforcement on the deregistration drain** — same
    wiring, plus the recovery property: the aborted batch rolls back and the
    finalize transaction (which deletes ``actor_config``) never runs, so a
    mid-drain timeout leaves the actor registered and the already-committed
    batches durable.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import deregister_actor
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

pytestmark = pytest.mark.integration

_SLEEP_SECS = 0.3
_TIMEOUT_MS = 150


class _TouchRaisesConn:
    """A connection stand-in whose every use is a test failure.

    For the boundary-validation pins: the guards must reject degenerate
    bounds BEFORE any database access, so any method call reaching this
    object means validation did not run first.
    """

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            f"boundary validation must reject degenerate bounds before the "
            f"connection is used; got a call to {name!r}"
        )


class _TouchRaisesPool:
    """A pool stand-in whose acquisition is a test failure (see _TouchRaisesConn)."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            f"boundary validation must reject degenerate bounds before the "
            f"pool is used; got a call to {name!r}"
        )


class _SlowBatchConn:
    """Delegates to a real connection, making every row-returning statement
    in the drain slow.

    The batch transaction's own timeout bookkeeping (the capture and the
    restore) is not row-returning, so the first slowed statement is the
    batch's driving statement.  A sleep longer than the batch's
    ``statement_timeout`` is the slow-batch stand-in: if the batch
    transaction actually runs under the bound, the server aborts the sleep
    and the drain surfaces ``QueryCanceledError``; without the wiring the
    sleep runs to completion and the drain succeeds.
    """

    def __init__(self, conn: asyncpg.Connection, *, sleep_secs: float) -> None:
        self._conn = conn
        self._sleep_secs = sleep_secs

    async def fetchrow(self, sql: str, *args: object) -> Any:
        await self._conn.fetchval("SELECT pg_sleep($1)", self._sleep_secs)
        return await self._conn.fetchrow(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _SingleConnPool:
    """Pool stand-in yielding one wrapped connection (see _SlowBatchConn)."""

    def __init__(self, conn: _SlowBatchConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
    ) -> AsyncGenerator[_SlowBatchConn, None]:
        yield self._conn


async def _seed_pending_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: list[UUID],
    *,
    actor: str,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, $2, 'default', '{{}}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds', ARRAY['guards'] "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
        actor,
    )


# ── Guard 1: boundary validation, before any database access ──────────


async def test_cancel_where_rejects_degenerate_batch_bounds_before_db_access() -> None:
    """``batch_size=0`` / ``statement_timeout_ms=0`` (and negatives) raise
    ``ValueError`` naming the field, with the pool never touched."""
    for kwargs in ({"batch_size": 0}, {"batch_size": -1}, {"statement_timeout_ms": 0}):
        with pytest.raises(ValueError, match=r"(batch_size|statement_timeout_ms)"):
            await _cancel_where(
                _TouchRaisesPool(),  # type: ignore[arg-type]  # Why: the pool must never be used — validation precedes acquisition.
                "taskq",
                render("taskq"),
                JobFilter(tags=("guards",)),
                None,
                **kwargs,  # type: ignore[arg-type]  # Why: the degenerate values under test.
            )


async def test_deregister_actor_rejects_degenerate_batch_bounds_before_db_access() -> None:
    """The force-deregistration drain validates the same two bounds at the
    same boundary — a zero batch cap stalls its drain forever and a zero
    timeout disables the batch safety net."""
    for kwargs in ({"batch_size": 0}, {"statement_timeout_ms": 0}):
        with pytest.raises(ValueError, match=r"(batch_size|statement_timeout_ms)"):
            await deregister_actor(
                _TouchRaisesConn(),  # type: ignore[arg-type]  # Why: the connection must never be used — validation precedes any statement.
                "guards_actor",
                force=True,
                schema="taskq",
                **kwargs,  # type: ignore[arg-type]  # Why: the degenerate values under test.
            )


# ── Guard 2/3: the batch transactions actually run under the timeout ──


async def test_cancel_batch_statement_timeout_aborts_a_slow_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A cancel batch whose driving statement outlives its
    ``statement_timeout_ms`` is aborted server-side — ``QueryCanceledError``
    propagates (no silent slow batch), and the aborted batch leaves every
    seeded job untouched: nothing cancelled, no events written."""
    schema = module_pg_schema.schema_name
    job_ids = [new_uuid() for _ in range(3)]
    await _seed_pending_jobs(clean_pg_conn, schema, job_ids, actor="guards_actor")

    slow_pool = _SingleConnPool(_SlowBatchConn(clean_pg_conn, sleep_secs=_SLEEP_SECS))  # pyright: ignore[reportArgumentType]  # Why: pool stand-in yielding the wrapped real connection, the same seam as the window-race harness.
    with pytest.raises(asyncpg.QueryCanceledError):
        await _cancel_where(
            slow_pool,  # type: ignore[arg-type]  # Why: same seam as above.
            schema,
            render(schema),
            JobFilter(tags=("guards",)),
            "offboard",
            batch_size=10,
            statement_timeout_ms=_TIMEOUT_MS,
        )

    still_pending = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert still_pending == len(job_ids), "the aborted batch must roll back in full"
    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events == 0, "no event may survive the aborted batch"


async def test_deregister_batch_statement_timeout_aborts_a_slow_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The same enforcement on the force-deregistration drain, plus its
    recovery property: the aborted batch rolls back, and because the
    ``actor_config`` delete runs only in the finalize transaction after the
    whole drain, a mid-drain timeout leaves the actor REGISTERED — the
    already-committed batches stay durable and a re-run continues."""
    schema = module_pg_schema.schema_name
    actor = "guards_dereg_actor"
    job_ids = [new_uuid() for _ in range(3)]
    await _seed_pending_jobs(clean_pg_conn, schema, job_ids, actor=actor)
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor=actor, max_concurrent=5, queue="default")],
        schema=schema,
    )

    slow_conn = _SlowBatchConn(clean_pg_conn, sleep_secs=_SLEEP_SECS)
    with pytest.raises(asyncpg.QueryCanceledError):
        await deregister_actor(
            slow_conn,  # type: ignore[arg-type]  # Why: same seam as the cancel guard above.
            actor,
            force=True,
            schema=schema,
            batch_size=10,
            statement_timeout_ms=_TIMEOUT_MS,
        )

    still_registered = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        actor,
    )
    assert still_registered == 1, (
        "a mid-drain timeout must not delete actor_config — the finalize "
        "transaction runs only after the whole drain completes"
    )
    still_pending = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert still_pending == len(job_ids), "the aborted batch must roll back in full"
    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events == 0
