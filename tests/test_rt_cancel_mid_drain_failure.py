"""Non-deadlock mid-drain failure and re-run semantics of the bounded cancel.

The bounded drain's contract for a failure that is NOT a deadlock (so
no retry): the operation RAISES — no partial-success result object —
the failed batch's driving UPDATE and event writes roll back together
(per-batch atomicity: a batch's events can never commit without the
state change they describe, and vice versa), every EARLIER batch stays
committed, and a re-run resumes exactly where the failed batch stopped
(the EPQ predicates skip the rows earlier batches already cancelled)
with exactly-once events per job across both runs.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, 'rt_actor', 'default', '{{}}'::jsonb, 'pending'::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', ARRAY['bulk']::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
    )


class _FailingConn:
    """Delegates to a real connection except the Nth event INSERT, which
    raises before touching the connection — a statement-level failure
    arriving after the batch's driving UPDATE has already executed."""

    def __init__(self, conn: Any, state: _FailingState) -> None:
        self._conn = conn
        self._state = state

    def transaction(self, **kwargs: object) -> Any:
        outer = self

        @asynccontextmanager
        async def _tx() -> AsyncGenerator[None]:
            async with outer._conn.transaction(**kwargs):
                yield

        return _tx()

    async def fetchrow(self, sql: str, *args: object) -> Any:
        return await self._conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: object) -> Any:
        upper = sql.lstrip().upper()
        if upper.startswith("INSERT INTO") and ".JOB_EVENTS" in upper:
            self._state.event_inserts += 1
            if self._state.event_inserts == self._state.fail_at:
                raise RuntimeError("injected statement failure")
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _FailingState:
    def __init__(self, fail_at: int) -> None:
        self.event_inserts = 0
        self.fail_at = fail_at


class _FailingPool:
    def __init__(self, pool: Any, state: _FailingState) -> None:
        self._pool = pool
        self.state = state

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[_FailingConn]:
        async with self._pool.acquire(**kwargs) as conn:
            yield _FailingConn(conn, self.state)


async def test_mid_drain_statement_failure_raises_keeps_batch1_and_rerun_completes(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A statement error on batch 2's event INSERT: the call raises (no
    result object), batch 1's 100 cancels and 200 events survive, batch
    2's 100 rows are fully rolled back (no cancelled rows without
    events, no events without rows), and a re-run cancels exactly the
    remaining rows with exactly-once events per job across both runs."""
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(200)]
    await _seed_jobs(clean_pg_conn, schema, job_ids)
    batch1, batch2 = set(sorted(job_ids)[:100]), set(sorted(job_ids)[100:])

    # Batch 1 writes event INSERTs #1 (state_change) and #2
    # (cancel_request); the failure lands on batch 2's first INSERT
    # (#3) — after batch 2's driving UPDATE has already mutated and
    # locked its rows inside the open transaction.
    state = _FailingState(fail_at=3)
    pool = _FailingPool(module_pg_pool, state)
    with pytest.raises(RuntimeError, match="injected statement failure"):
        await _cancel_where(
            pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
            schema,
            render(schema),
            JobFilter(tags=("bulk",)),
            "offboard",
            batch_size=100,
        )

    # Batch 1: committed — cancelled with its full event pair.
    b1_rows = await clean_pg_conn.fetch(
        f'SELECT status::text AS status, finished_at FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE id = ANY($1::uuid[])",
        list(batch1),
    )
    assert {r["status"] for r in b1_rows} == {"cancelled"}, "batch 1 must stay committed"
    assert all(r["finished_at"] is not None for r in b1_rows)
    b1_events = await clean_pg_conn.fetch(
        f'SELECT kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE job_id = ANY($1::uuid[]) GROUP BY kind",
        list(batch1),
    )
    assert {r["kind"]: r["n"] for r in b1_events} == {"state_change": 100, "cancel_request": 100}

    # Batch 2: rolled back in full — the driving UPDATE and the event
    # writes left the same trace as if the batch never ran.
    b2_rows = await clean_pg_conn.fetch(
        f'SELECT status::text AS status, finished_at FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE id = ANY($1::uuid[])",
        list(batch2),
    )
    assert {r["status"] for r in b2_rows} == {"pending"}, (
        "batch 2's driving UPDATE must roll back with its failed event write — a "
        "committed cancel without its events would be an untracked state change"
    )
    assert all(r["finished_at"] is None for r in b2_rows)
    stray: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        list(batch2),
    )
    assert stray == 0, "no fragment of the failed batch may survive"

    total_after_failure: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert total_after_failure == 200

    # Re-run: completes the drain, exactly-once events per job across
    # both runs — the EPQ predicates skip batch 1's committed rows.
    result, _notify = await _cancel_where(
        module_pg_pool,
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
        batch_size=100,
    )
    assert result.cancelled_directly == 100, "the re-run must cancel exactly the remainder"
    assert set(result.cancelled_ids) == batch2
    assert result.cancel_requested == 0

    final_statuses = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert {r["status"] for r in final_statuses} == {"cancelled"}

    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "GROUP BY job_id, kind"
    )
    assert len(events) == 2 * 200
    assert all(r["n"] == 1 for r in events), (
        "across the failed run and its re-run, every job must carry exactly one "
        "event of each kind — no duplicates from the re-run's re-selection"
    )
    assert {r["kind"] for r in events} == {"state_change", "cancel_request"}


async def test_running_drain_failure_after_ps_completion_leaves_ps_committed(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The same failure semantics across the TWO drains: a failure in the
    running drain (after the ps drain fully completed) raises, leaves
    every ps cancellation committed, and rolls the running batch's
    phase write back with its event."""
    schema = module_pg_schema.schema_name
    render(schema)
    worker_id = new_uuid()
    pending_ids = [new_uuid() for _ in range(3)]
    running_ids = [new_uuid() for _ in range(2)]
    await _seed_jobs(clean_pg_conn, schema, pending_ids)
    await _seed_jobs(clean_pg_conn, schema, running_ids)
    await clean_pg_conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'running', locked_by_worker = $1 "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE id = ANY($2::uuid[])",
        worker_id,
        running_ids,
    )

    # The ps drain's single batch writes two batched event INSERTs
    # (#1 state_change, #2 cancel_request); the running drain's single
    # batch writes one (#3) — that is the failing statement.
    state = _FailingState(fail_at=3)
    pool = _FailingPool(module_pg_pool, state)
    with pytest.raises(RuntimeError, match="injected statement failure"):
        await _cancel_where(
            pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
            schema,
            render(schema),
            JobFilter(tags=("bulk",)),
            "offboard",
            batch_size=100,
        )

    ps_rows = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        pending_ids,
    )
    assert {r["status"] for r in ps_rows} == {"cancelled"}, (
        "the completed ps drain's commits must survive a later running-drain failure"
    )
    running_rows = await clean_pg_conn.fetch(
        f"SELECT status::text AS status, cancel_phase, cancel_requested_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
        running_ids,
    )
    assert {r["status"] for r in running_rows} == {"running"}
    assert all(r["cancel_phase"] == 0 for r in running_rows), (
        "the running batch's phase write must roll back with its failed event write"
    )
    assert all(r["cancel_requested_at"] is None for r in running_rows)
    running_events: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        running_ids,
    )
    assert running_events == 0

    # Re-run: the ps rows are already terminal (EPQ skips them), the
    # running rows get their cooperative cancel, and the notify target
    # survives the earlier failure.
    result, notify_targets = await _cancel_where(
        module_pg_pool,
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
        batch_size=100,
    )
    assert result.cancelled_directly == 0
    assert result.cancel_requested == 2
    assert set(result.cancel_requested_ids) == set(running_ids)
    assert [(t.job_id, t.worker_id) for t in notify_targets] == [
        (running_ids[0], worker_id),
        (running_ids[1], worker_id),
    ]
