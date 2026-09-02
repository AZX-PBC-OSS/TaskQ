"""Dispatch must write its state-change events in one statement.

Dispatch previously looped `for rec in records: await conn.execute(...)` -- one
awaited round trip per dispatched job, issued INSIDE the transaction still
holding the `FOR UPDATE SKIP LOCKED` row locks. At a batch of 50 against a
managed Postgres (~1-3ms RTT) that is 50-150ms of extra transaction hold per
dispatch cycle, per worker. Correctness was never in question -- a failure rolls
the whole batch back to `pending` -- but lock hold time directly narrows the
window other dispatchers can work in, and this is the hot path.

Every row in one batch shares `kind` and `detail` (same from_state, to_state and
worker_id), so only the job ids vary and a single `unnest` over a `uuid[]`
suffices.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch import _dispatch_batch
from taskq.backend._sql import INSERT_EVENTS_BATCH_SQL
from taskq.backend._sql_templates import render
from taskq.testing.fixtures import ModulePgSchema


def test_batch_sql_is_a_single_multi_row_insert() -> None:
    sql = INSERT_EVENTS_BATCH_SQL.format(schema="taskq")
    assert sql.count("INSERT INTO") == 1
    assert "unnest($1::uuid[])" in sql
    # clock_timestamp() must stay per-row, matching the statement it replaces.
    assert "clock_timestamp()" in sql


def test_rendered_templates_expose_the_batch_form() -> None:
    tmpl = render("taskq")
    assert "unnest" in tmpl.insert_events_batch
    # The single-row form is still used by other call sites (sweeps, terminal
    # writes), so it must not have been removed.
    assert "VALUES ($1, clock_timestamp(), $2, $3::jsonb)" in tmpl.insert_event


class _CountingConn:
    """Delegates to a real connection, counting event-INSERT round trips.

    The statement count IS the property under test: correctness never differed
    between the loop and the batch, only the number of awaited round trips
    taken while the dispatch row locks are still held.
    """

    def __init__(self, conn: Any, event_sql: str) -> None:
        self._conn = conn
        self._event_sql = event_sql
        self.event_executes = 0

    async def execute(self, sql: str, *args: object) -> str:
        if sql == self._event_sql:
            self.event_executes += 1
        result: str = await self._conn.execute(sql, *args)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _CountingPool:
    """Pool stand-in yielding one _CountingConn over a real pooled connection."""

    def __init__(self, pool: Any, event_sql: str) -> None:
        self._pool = pool
        self._event_sql = event_sql
        self.conns: list[_CountingConn] = []

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_CountingConn]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire, which _dispatch_batch calls with timeout=.
        async with self._pool.acquire(timeout=timeout) as conn:
            counting = _CountingConn(conn, self._event_sql)
            self.conns.append(counting)
            yield counting


@pytest.mark.integration
async def test_dispatch_writes_one_event_statement_for_the_whole_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Dispatching N jobs issues ONE event INSERT, not N.

    Driving the real ``_dispatch_batch`` and counting round trips, rather than
    grepping its source for a `for rec in records:` loop: a reintroduced loop
    spelled any other way — enumerate, a comprehension of awaits, a helper —
    is the same regression and the regex would not have seen it.
    """
    schema = module_pg_schema.schema_name
    tmpl = render(schema)
    job_ids = [new_uuid() for _ in range(5)]
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "ON CONFLICT (actor) DO NOTHING",
        "a",
        "default",
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'a', 'default', '{}'::jsonb, 'pending', 3, 'transient', clock_timestamp() "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
    )

    pool = _CountingPool(module_pg_pool, tmpl.insert_events_batch)
    rows = await _dispatch_batch(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        tmpl,
        2,
        5.0,
        schema,
        new_uuid(),
        ["default"],
        len(job_ids),
        timedelta(seconds=60),
    )

    assert {r.id for r in rows} == set(job_ids), "all five jobs must dispatch"
    executes = sum(c.event_executes for c in pool.conns)
    assert executes == 1, (
        f"expected ONE batched event INSERT for {len(job_ids)} jobs, got {executes} — "
        "a per-row loop is back, holding the dispatch row locks across every round trip"
    )
    written = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".job_events WHERE kind = 'state_change'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert written == len(job_ids), "one event per job must still be written"


def test_schema_is_still_validated_in_the_batch_template() -> None:
    with pytest.raises(ValueError, match=r"[Ii]nvalid"):
        render('evil"; DROP SCHEMA public CASCADE; --')


@pytest.mark.integration
async def test_events_land_once_per_dispatched_job(clean_pg_conn, module_pg_schema) -> None:  # type: ignore[no-untyped-def]  # Why: pytest fixtures.
    """Behavioural equivalence: the batch statement writes exactly the rows the
    loop did, with the same kind and detail."""
    schema = module_pg_schema.schema_name
    tmpl = render(schema)

    job_ids = [new_uuid() for _ in range(5)]
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "(id, actor, queue, payload, status, max_attempts, retry_kind) "
        "SELECT id, 'a', 'default', '{}'::jsonb, 'pending', 3, 'transient' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
    )

    detail = '{"from_state": "pending", "to_state": "running", "worker_id": "w1"}'
    await clean_pg_conn.execute(tmpl.insert_events_batch, job_ids, "state_change", detail)

    rows = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events ORDER BY occurred_at'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert len(rows) == len(job_ids)
    assert {r["job_id"] for r in rows} == set(job_ids)
    assert {r["kind"] for r in rows} == {"state_change"}


@pytest.mark.integration
async def test_empty_batch_writes_nothing(clean_pg_conn, module_pg_schema) -> None:  # type: ignore[no-untyped-def]  # Why: pytest fixtures.
    """Dispatch guards on `if records:`, but the statement itself must also be
    a no-op on an empty array rather than erroring."""
    schema = module_pg_schema.schema_name
    tmpl = render(schema)
    await clean_pg_conn.execute(tmpl.insert_events_batch, [], "state_change", "{}")
    count = await clean_pg_conn.fetchval(f'SELECT count(*) FROM "{schema}".job_events')  # noqa: S608  # Why: schema is a test-fixture identifier.
    assert count == 0
