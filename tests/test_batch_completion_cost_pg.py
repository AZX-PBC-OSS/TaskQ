# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Batch completion cost is bounded per terminal write.

The terminal-outcome hook (``taskq/batch.py``) runs on every batched job's
terminal write. Its cost must not grow with the batch's member count, or a
batch of N members costs O(N²) member visits to complete: the
consecutive-failure counter writes are single keyed ``batches``-row
updates, and ``complete_batch``'s "any member still open?" probe is served
by ``jobs_batch_open_members_idx`` — a partial B-tree over exactly the
non-terminal members, keyed by ``batch_id``, which the probe seeks by
equality and leaves at the first hit.

Plans are pinned with EXPLAIN on a seeded schema (the index-audit idiom of
``tests/test_index_audit.py``); the race pin drives genuinely concurrent
hook transactions through the production statements.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq._json import dumps_str
from taskq.backend._batch_sql import (
    _batch_filter_json,  # pyright: ignore[reportPrivateUsage]  # Why: the parity pin compares the production probe against the legacy containment predicate it replaced.
    count_batch_non_terminal,
    increment_batch_failures,
    render_batch_sql,
    reset_batch_failures,
)
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch import apply_batch_terminal_outcome
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

_OPEN_MEMBERS_INDEX = "jobs_batch_open_members_idx"
_TERMINAL_LIST = ", ".join(f"'{s}'" for s in sorted(TERMINAL_STATUSES))
_MEMBERS = 2_000
_OTHER_ROWS = 6_000
_BOUNDED_WAIT_SECS = 30.0


async def _seed_batch(
    conn: asyncpg.Connection,
    schema: str,
    batch_id: UUID,
    *,
    members: int,
    open_members: int,
    other_rows: int,
) -> None:
    """A batch with *members* member rows, the last *open_members* of them
    still pending, plus *other_rows* unrelated jobs so the planner has a
    table worth indexing."""
    await conn.execute(
        f'INSERT INTO "{schema}".batches (id, queue, expected_size) VALUES ($1, $2, $3)',
        batch_id,
        "default",
        members,
    )
    meta = json.dumps({"batch_id": str(batch_id)})
    rows = [
        (new_job_id(), meta, "pending" if k >= members - open_members else "succeeded")
        for k in range(members)
    ]
    rows += [
        (new_job_id(), "{}", "pending" if k % 3 == 0 else "succeeded") for k in range(other_rows)
    ]
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs '
        "(id, queue, actor, payload, max_attempts, retry_kind, metadata, status) "
        "VALUES ($1, 'default', 'seed_actor', '{}'::jsonb, 1, 'non_retryable', $2::jsonb, $3)",
        rows,
    )
    await conn.execute(f'ANALYZE "{schema}".jobs')


async def _explain(conn: asyncpg.Connection, sql: str, *params: object) -> str:
    rows = await conn.fetch(f"EXPLAIN {sql}", *params)
    return "\n".join(r["QUERY PLAN"] for r in rows)


@pytest.fixture
async def seeded_schema(pg_dsn: str) -> Any:
    schema = f"batch_cost_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        bid = new_uuid()
        await _seed_batch(
            conn, schema, bid, members=_MEMBERS, open_members=1, other_rows=_OTHER_ROWS
        )
        yield conn, schema, bid
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── migration ─────────────────────────────────────────────────────────


async def test_open_members_index_exists_and_is_partial_on_open_batch_members(
    pg_dsn: str,
) -> None:
    schema = f"batch_cost_mig_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        # Re-apply is a no-op: the index migration is idempotent.
        await migrate_mod.apply_pending(conn, schema=schema)

        row = await conn.fetchrow(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = $2",
            schema,
            _OPEN_MEMBERS_INDEX,
        )
        assert row is not None, f"{_OPEN_MEMBERS_INDEX} should exist after apply_pending"
        indexdef: str = row["indexdef"]
        assert "batch_id" in indexdef
        assert "WHERE" in indexdef, "the index must be partial on open batch members"
        for terminal in TERMINAL_STATUSES:
            assert f"'{terminal}'" not in indexdef, (
                f"the index population must exclude terminal members; found {terminal!r} "
                f"in its predicate:\n{indexdef}"
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── per-write cost ────────────────────────────────────────────────────


def _assert_member_access_is_open_members_index_only(plan: str, name: str) -> None:
    """Every ``jobs`` node in *plan* must be a scan of the open-members
    index with the batch id as its Index Cond and no per-row status
    Filter (which renders against the ``job_status`` enum): anything else
    walks the batch's full member population on every terminal write."""
    lines = plan.splitlines()
    jobs_nodes = [line for line in lines if " on jobs" in line]
    assert jobs_nodes, f"{name}: expected the member probe in the plan:\n{plan}"
    # An Index Scan names its index on the jobs node; a bitmap plan names
    # it on a child Bitmap Index Scan under a "Bitmap Heap Scan on jobs".
    # Either way, no Seq Scan and no other index may reach jobs.
    other_access = [
        line
        for line in jobs_nodes
        if "Seq Scan" in line or ("using " in line and _OPEN_MEMBERS_INDEX not in line)
    ]
    bitmap_indexes = [line for line in lines if "Bitmap Index Scan on" in line]
    other_access += [line for line in bitmap_indexes if _OPEN_MEMBERS_INDEX not in line]
    assert not other_access, (
        f"{name} reaches member jobs other than through {_OPEN_MEMBERS_INDEX}:\n{plan}"
    )
    assert any("Index Cond:" in line and "batch_id" in line for line in lines), (
        f"{name}: the batch id must be an Index Cond on {_OPEN_MEMBERS_INDEX}:\n{plan}"
    )
    status_filters = [line for line in lines if "Filter:" in line and "job_status" in line]
    assert not status_filters, (
        f"{name}: the member status test must be proven by the index predicate, not "
        f"applied per row:\n{plan}"
    )


async def test_counter_writes_count_members_only_through_the_open_members_index(
    seeded_schema: Any,
) -> None:
    """The consecutive-failure increment and reset return the open-member
    count; that count must come from the open-members index (a range over
    the members still open), never a walk of the batch's whole membership."""
    conn, schema, bid = seeded_schema
    sql = render_batch_sql(schema)

    for name, statement in (
        ("increment_batch_failures", sql.increment_batch_failures),
        ("reset_batch_failures", sql.reset_batch_failures),
    ):
        plan = await _explain(conn, statement, bid, str(bid))
        _assert_member_access_is_open_members_index_only(plan, name)


async def test_counter_writes_return_the_index_served_open_member_count(
    seeded_schema: Any,
) -> None:
    """The ``remaining`` the counter writes return equals the index-served
    ``count_batch_non_terminal`` probe on a mixed member population."""
    conn, schema, bid = seeded_schema
    sql = render_batch_sql(schema)
    meta = json.dumps({"batch_id": str(bid)})
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs '
        "(id, queue, actor, payload, max_attempts, retry_kind, metadata, status) "
        "VALUES ($1, 'default', 'seed_actor', '{}'::jsonb, 1, 'non_retryable', $2::jsonb, $3)",
        [(new_job_id(), meta, status) for status in ("scheduled", "running", "failed", "crashed")],
    )

    probe = await count_batch_non_terminal(conn, sql, bid)
    count, threshold, remaining_after_increment = await increment_batch_failures(conn, sql, bid)
    remaining_after_reset = await reset_batch_failures(conn, sql, bid)

    assert (count, threshold) == (1, None)
    assert remaining_after_increment == remaining_after_reset == probe == 3


async def test_complete_batch_probe_is_served_by_the_open_members_index(
    seeded_schema: Any,
) -> None:
    """The completion probe seeks ``jobs_batch_open_members_idx`` with the
    batch id as an Index Cond and no post-scan status Filter: the index
    holds only open members, so the probe stops at the first entry — or
    at an empty range — instead of visiting every member of the batch."""
    conn, schema, bid = seeded_schema
    sql = render_batch_sql(schema)

    plan = await _explain(conn, sql.complete_batch, bid, str(bid))

    _assert_member_access_is_open_members_index_only(plan, "complete_batch")


async def test_count_batch_non_terminal_matches_the_containment_predicate(
    seeded_schema: Any,
) -> None:
    """Parity: the index-served count equals the legacy ``metadata @>``
    containment count on a mixed member population."""
    conn, schema, bid = seeded_schema
    sql = render_batch_sql(schema)
    meta = json.dumps({"batch_id": str(bid)})
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs '
        "(id, queue, actor, payload, max_attempts, retry_kind, metadata, status) "
        "VALUES ($1, 'default', 'seed_actor', '{}'::jsonb, 1, 'non_retryable', $2::jsonb, $3)",
        [
            (new_job_id(), meta, status)
            for status in ("scheduled", "running", "failed", "cancelled")
        ],
    )

    legacy: Any = await conn.fetchval(
        f'SELECT count(*)::int FROM "{schema}".jobs '
        f"WHERE metadata @> $1::jsonb AND status::text NOT IN ({_TERMINAL_LIST})",
        _batch_filter_json(bid),
    )
    served: Any = await conn.fetchval(sql.count_batch_non_terminal, str(bid))

    assert served == legacy == 3


# ── race ──────────────────────────────────────────────────────────────


def _member_args(actor: str, batch_id: UUID) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        metadata={"batch_id": str(batch_id)},
    )


async def test_concurrent_member_terminal_writes_complete_the_batch_exactly_once(
    pg_dsn: str,
) -> None:
    """Every member's terminal write races the others through the hook on
    its own transaction; the batch ends ``complete`` with one
    ``completed_at`` and no member left open, and never completes while a
    member is still open."""
    schema = f"batch_cost_race_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    batch_sql = render_batch_sql(schema)
    bid = new_uuid()
    actor = "race_actor"
    members = 24
    try:
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
                "ON CONFLICT (actor) DO NOTHING",
                actor,
                "default",
            )
            await backend.create_batch(bid, "default", members, None, None, None)
            args = [_member_args(actor, bid) for _ in range(members)]
            rows: list[JobRow] = await backend.enqueue_batch(args, connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = $2, "
                "lock_expires_at = clock_timestamp() + interval '60 seconds' "
                "WHERE id = ANY($1::uuid[])",
                [a.id for a in args],
                new_uuid(),
            )
        assert len(rows) == members

        premature: list[str] = []

        async def _terminal_path(row: JobRow, outcome: str) -> None:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f'UPDATE "{schema}".jobs SET status = $2::text::"{schema}".job_status, '
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                    outcome,
                )
                await apply_batch_terminal_outcome(
                    backend,
                    row,
                    "succeeded" if outcome == "succeeded" else "failed",
                    transaction_conn=conn,
                )
                # Inside the still-open transaction: if this writer completed
                # the batch, no OTHER committed member may still be open.
                status: Any = await conn.fetchval(
                    f'SELECT status FROM "{schema}".batches WHERE id = $1', bid
                )
                if status == "complete":
                    open_now: Any = await conn.fetchval(
                        f'SELECT count(*) FROM "{schema}".jobs '
                        f"WHERE metadata @> $1::jsonb AND status::text NOT IN ({_TERMINAL_LIST})",
                        dumps_str({"batch_id": str(bid)}),
                    )
                    if open_now:
                        premature.append(f"{row.id}: {open_now} member(s) still open")

        await asyncio.wait_for(
            asyncio.gather(
                *(
                    _terminal_path(row, "failed" if i % 5 == 0 else "succeeded")
                    for i, row in enumerate(rows)
                )
            ),
            timeout=_BOUNDED_WAIT_SECS,
        )

        assert premature == [], f"completion landed while members were open: {premature}"
        async with deps.worker_pool.acquire() as conn:
            batch = await backend.get_batch(bid)
            assert batch is not None
            assert batch.status == "complete", f"expected 'complete', got {batch.status!r}"
            assert batch.completed_at is not None
            remaining = await backend.count_batch_non_terminal(bid)
            assert remaining == 0
            _ = batch_sql
            _ = conn
    finally:
        await stack.aclose()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
