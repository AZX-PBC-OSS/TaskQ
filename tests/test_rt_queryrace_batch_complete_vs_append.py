# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on the batch CAS: live count, abort-vs-complete, and the
member-append window.

Hunted interleavings, worked out on paper from ``_batch_sql.py`` first:

1. ``_COMPLETE_BATCH_SQL``'s NOT EXISTS guard vs a CONCURRENT MEMBER APPEND
   (RED).  The guard arbitrates "in this statement's own snapshot", and a
   READ COMMITTED snapshot cannot see another transaction's uncommitted
   member INSERT.  The appender is real: ``enqueue_batch_streaming``
   without a connection commits members chunk by chunk in separate pool
   transactions (its own docstring), a caller-supplied ``batch_id`` of an
   existing batch is forwarded verbatim, and the terminal-outcome hook
   calls ``complete_batch`` on the worker's terminal connection.  So:
   appender inserts member M2 (uncommitted) → the last known member's hook
   runs ``complete_batch`` → NOT EXISTS sees only terminal members → the
   batch flips to 'complete' → the append commits → the invariant
   "a complete batch has no non-terminal members" is violated, and every
   ``wait_for_batch``-style reader that sees 'complete' stops waiting
   while M2 is still pending.  ``complete_batch``'s docstring promises it
   "can delay completion but never complete prematurely" — this is
   precisely a premature completion, and nothing documents it.

2. Concurrent member-terminal vs threshold-abort (GREEN pin — the
   integration-lane race ``tests/test_batch_complete_guard.py``'s
   docstring says "lives in the integration lane" but no test occupies).
   Both hook transactions serialize on the batches-row lock; for a
   threshold-1 batch with one failing and one succeeding member, every
   commit order ends 'aborted': the succeed path's complete is either
   vetoed by the still-running failing member (uncommitted) or no-ops on
   the already-'aborted' row, and the fail path's abort only no-ops if the
   row already went terminal — which the same fail-path transaction is the
   last writer of.  One terminal status, abort wins, both members terminal.

The ``increment_batch_failures`` count-vs-write race (counts CTE snapshot
vs the caller) is safe by construction and NOT attacked here: the hook at
``taskq/batch.py`` discards ``_remaining`` and decides purely on
``count >= threshold``, and completion is re-arbitrated server-side.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq._json import dumps_str
from taskq.backend._batch_sql import (
    complete_batch,
    create_batch,
    get_batch,
    render_batch_sql,
)
from taskq.backend._protocol import BatchRow, EnqueueArgs, JobRow
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch import apply_batch_terminal_outcome
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

_QUEUE = "default"
_BOUNDED_WAIT_SECS = 20.0
_TERMINAL_LIST = ", ".join(f"'{s}'" for s in sorted(TERMINAL_STATUSES))


def _member_args(actor: str, batch_id: UUID) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=_QUEUE,
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        metadata={"batch_id": str(batch_id)},
    )


async def _non_terminal_members(conn: asyncpg.Connection, schema: str, batch_id: UUID) -> int:
    val: Any = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        f"WHERE metadata @> $1::jsonb AND status::text NOT IN ({_TERMINAL_LIST})",
        dumps_str({"batch_id": str(batch_id)}),
    )
    assert isinstance(val, int)
    return val


async def _seed_actor(conn: asyncpg.Connection, schema: str, actor: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        actor,
        _QUEUE,
    )


async def _teardown(stack: Any, pg_dsn: str, schema: str) -> None:
    await stack.aclose()
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


async def test_complete_batch_lands_while_member_append_is_uncommitted(
    pg_dsn: str,
) -> None:
    """RED: the complete CAS cannot see an in-flight member INSERT, so it
    completes the batch; once the append commits, a 'complete' batch holds a
    pending member — the premature completion complete_batch's docstring
    promises can never happen."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    batch_sql = render_batch_sql(schema)
    bid = new_uuid()
    actor = "tqr_bmember_actor"
    try:
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            await create_batch(
                conn,
                batch_sql,
                bid,
                queue=_QUEUE,
                expected_size=1,
                failure_threshold=None,
                finalizer_job_id=None,
                originating_actor=None,
            )
            m1 = _member_args(actor, bid)
            rows: list[JobRow] = await backend.enqueue_batch([m1], connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                "finished_at = clock_timestamp() WHERE id = $1",
                m1.id,
            )
        assert len(rows) == 1, "fixture broken: seeding"

        conn_w = await deps.worker_pool.acquire()
        conn_a = await deps.worker_pool.acquire()
        try:
            tx_a = conn_a.transaction()
            await tx_a.start()
            try:
                m2 = _member_args(actor, bid)
                await backend.enqueue_batch([m2], connection=conn_a)
                await complete_batch(conn_w, batch_sql, bid)
                batch: BatchRow | None = await get_batch(conn_w, batch_sql, bid)
                assert batch is not None, "fixture broken: batch row vanished"
            except BaseException:
                await tx_a.rollback()
                raise
            else:
                await tx_a.commit()

            non_terminal = await _non_terminal_members(conn_w, schema, bid)
            assert not (batch.status == "complete" and non_terminal > 0), (
                f"CONTRACT: a batch that reached status='complete' must have NO "
                f"non-terminal members — complete_batch's docstring promises it 'can "
                f"delay completion but never complete prematurely', and every "
                f"wait_for_batch-style reader treats 'complete' as done. Violated: "
                f"status={batch.status!r} with {non_terminal} non-terminal member(s). "
                f"The NOT EXISTS guard arbitrated in the completer's READ COMMITTED "
                f"snapshot, which cannot see the appender's still-uncommitted member "
                f"INSERT (enqueue_batch on a caller connection — the streaming chunk "
                f"path); the append then committed into an already-terminal batch."
            )
        finally:
            await deps.worker_pool.release(conn_w)
            await deps.worker_pool.release(conn_a)
    finally:
        await _teardown(stack, pg_dsn, schema)


async def test_concurrent_member_terminal_vs_threshold_abort_ends_aborted_once(
    pg_dsn: str,
) -> None:
    """GREEN pin: the abort-vs-complete CAS under two genuinely concurrent
    member-terminal hook transactions — exactly one terminal batch status,
    abort wins, no member left non-terminal."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    batch_sql = render_batch_sql(schema)
    bid = new_uuid()
    actor = "tqr_cas_actor"
    try:
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            await create_batch(
                conn,
                batch_sql,
                bid,
                queue=_QUEUE,
                expected_size=2,
                failure_threshold=1,
                finalizer_job_id=None,
                originating_actor=None,
            )
            m1 = _member_args(actor, bid)
            m2 = _member_args(actor, bid)
            rows: list[JobRow] = await backend.enqueue_batch([m1, m2], connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = $2, "
                "lock_expires_at = clock_timestamp() + interval '60 seconds' "
                "WHERE id = ANY($1::uuid[])",
                [m1.id, m2.id],
                new_uuid(),
            )
        assert len(rows) == 2, "fixture broken: seeding"
        row_m1, row_m2 = rows

        conn_f = await deps.worker_pool.acquire()
        conn_s = await deps.worker_pool.acquire()
        try:

            async def _fail_path(conn: asyncpg.Connection) -> None:
                async with conn.transaction():
                    await conn.execute(
                        f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                        "finished_at = clock_timestamp(), error_class = 'tqr' "
                        "WHERE id = $1",
                        m1.id,
                    )
                    await apply_batch_terminal_outcome(
                        backend, row_m1, "failed", transaction_conn=conn
                    )

            async def _succeed_path(conn: asyncpg.Connection) -> None:
                async with conn.transaction():
                    await conn.execute(
                        f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                        "finished_at = clock_timestamp() WHERE id = $1",
                        m2.id,
                    )
                    await apply_batch_terminal_outcome(
                        backend, row_m2, "succeeded", transaction_conn=conn
                    )

            await asyncio.wait_for(
                asyncio.gather(_fail_path(conn_f), _succeed_path(conn_s)),
                timeout=_BOUNDED_WAIT_SECS,
            )
        finally:
            await deps.worker_pool.release(conn_f)
            await deps.worker_pool.release(conn_s)

        async with deps.worker_pool.acquire() as conn:
            batch: BatchRow | None = await get_batch(conn, batch_sql, bid)
            assert batch is not None, "fixture broken: batch row vanished"
            assert batch.status == "aborted", (
                f"CONTRACT: with failure_threshold=1 reached by a failing member, the "
                f"abort-vs-complete CAS must end 'aborted' under ANY commit order — the "
                f"succeed path's complete is vetoed by the uncommitted failing member or "
                f"no-ops on the aborted row, and 'complete' would mean a threshold abort "
                f"was lost by the CAS. Got {batch.status!r}."
            )
            assert batch.completed_at is not None, "a terminal batch must carry completed_at"
            assert await _non_terminal_members(conn, schema, bid) == 0, (
                "CONTRACT: the aborted batch must leave every member terminal — abort "
                "cancels pending/scheduled members and the two racing terminal writes "
                "both landed."
            )
    finally:
        await _teardown(stack, pg_dsn, schema)
