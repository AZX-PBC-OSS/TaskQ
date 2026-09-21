"""Plan pins for the fenced terminal write's probe index (01.00.19_01).

The fenced single-row writes (``mark_succeeded`` / ``mark_failed`` /
``mark_retry`` / ``mark_cancelled``, and the per-job lease checks) target
one row by
``id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $k
AND claim_epoch = $m``.  The row is unique by ``id = $1``, but the planner
is free to drive the UPDATE from any index the quals admit, and after bulk
churn (a large enqueue burst before autovacuum's next ANALYZE - the
measured regime in ``benchmarks/ab_fence_index.py`` and
``perf-evidence-terminal-fence.md``) the estimate collapse mispicks the
running-holder partial index and evaluates ``id = $1`` as a post-scan
Filter over EVERY running row the worker holds: measured at 1.07 ms of
heap-filter walking per terminal write at a 2,000-row running population
(2.56 ms statement total) where the primary-key-driven form costs 0.04 ms
(1.42 ms total).

Migration 01.00.19_01 rebuilds that partial index with the job id as a
trailing KEY column, so the same mispicked plan filters inside the index
(a non-leading key column is still an Index Cond, never a heap-visiting
Filter) and the write's scan touches one index entry and one heap row
regardless of which index the planner picks or how stale the statistics
are.

The pin below bulk-seeds the stale-stats regime directly (fresh schema,
2,000-row running population, no ANALYZE - the bulk-churn window the
window is named for) and asserts the invariant that holds under EVERY
driver choice: no jobs-scan node of the fenced write may heap-filter more
than the fence's own one target row.  It also pins the index definition
(two key columns, one predicate) and, as the red control, replays the
same shape against the pre-01.00.19_01 one-key index, which must exhibit
the O(running) heap-filter walk the migration exists to remove.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant.

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._sql_templates import render

pytestmark = pytest.mark.integration

_SEED_POP = 2_000
"""The running population the stale-stats misplan was measured at (the
bulk-churn window's shape); the one-key form's Removed-by-Filter walks
exactly this many rows, the two-key form walks none."""


async def _fresh_schema(pg_dsn: str) -> tuple[asyncpg.Connection, str]:
    schema = f"fence_probe_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await migrate_mod.apply_pending(conn, schema=schema)
    return conn, schema


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.close()


async def _seed_running_population(conn: asyncpg.Connection, schema: str) -> tuple[UUID, list[Any]]:
    """One worker holding `_SEED_POP` running rows, NO ANALYZE (deliberately:
    the stale-`reltuples` estimate collapse is the regime under test)."""
    wid = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, 0, $3)',
        wid,
        "fence-pin-host",
        ["default"],
    )
    ids = [new_job_id() for _ in range(_SEED_POP)]
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        f"retry_kind, status, attempt, claim_epoch, locked_by_worker, started_at, "
        f"lock_expires_at) VALUES ($1, $2, $3, '{{}}'::jsonb, 3, 'transient', "
        f"'running', 1, 1, $4, clock_timestamp(), clock_timestamp() + interval '5 minutes')",
        [(i, "pin_actor", "default", wid) for i in ids],
    )
    return wid, ids


async def _fence_scan_nodes(
    conn: asyncpg.Connection, schema: str, target: Any, wid: UUID
) -> list[dict[str, Any]]:
    """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) the production mark_succeeded
    fence; ANALYZE really executes the write (the same discipline the depth
    oracles use), returning every scan node over ``jobs``."""
    stmt = render(schema).mark_succeeded
    rows = await conn.fetch(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {stmt}",
        target,
        wid,
        '{"ok":1}',
        8,
        0,
        None,
        None,
        1,
        1,
    )
    plan = json.loads(rows[0][0])[0]
    nodes: list[dict[str, Any]] = []

    def walk(node: dict[str, Any]) -> None:
        if node.get("Relation Name") == "jobs" and node.get("Node Type") in (
            "Index Scan",
            "Index Only Scan",
            "Seq Scan",
            "TID Scan",
        ):
            nodes.append(node)
        for child in node.get("Plans", []):
            walk(child)

    walk(plan["Plan"])
    return nodes


async def _assert_no_heap_filter_walk(
    conn: asyncpg.Connection, schema: str, wid: str, ids: list[Any]
) -> None:
    nodes = await _fence_scan_nodes(conn, schema, ids[0], wid)
    assert nodes, "the fenced write's plan has no scan over jobs at all"
    for node in nodes:
        removed = node.get("Rows Removed by Filter") or 0
        assert removed <= 1, (
            "the fenced terminal write's driving scan heap-filters the holder's "
            f"running population ({removed} rows removed by filter over "
            f"{node.get('Index Name') or node.get('Node Type')}) - the trailing-id "
            "probe index (01.00.19_01) is gone or was reshaped; see "
            "perf-evidence-terminal-fence.md for the measured cost curve"
        )


async def test_fenced_terminal_write_never_heap_filters_the_running_set(pg_dsn: str) -> None:
    """The invariant that holds under every driver choice: after the
    bulk-churn stale-stats seed, no jobs-scan node of the production
    mark_succeeded fence may heap-filter more than the one target row."""
    conn, schema = await _fresh_schema(pg_dsn)
    try:
        wid, ids = await _seed_running_population(conn, schema)
        await _assert_no_heap_filter_walk(conn, schema, wid, ids)
    finally:
        await _drop_schema(conn, schema)


async def test_fence_probe_index_definition_is_the_two_key_form(pg_dsn: str) -> None:
    """01.00.19_01 rebuilds jobs_locked_by_worker_running_idx as
    (locked_by_worker, id) WHERE status = 'running': the key column list and
    the predicate are the load-bearing shapes (id as a trailing KEY column,
    not INCLUDE and not absent), so pin the catalog's own definition."""
    conn, schema = await _fresh_schema(pg_dsn)
    try:
        rec = await conn.fetchrow(
            """
            SELECT pg_get_expr(ix.indpred, ix.indrelid) AS predicate,
                   (SELECT array_agg(a.attname ORDER BY k.ord)
                      FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
                      JOIN pg_attribute a
                        ON a.attrelid = ix.indrelid AND a.attnum = k.attnum) AS keys
            FROM pg_index ix
            JOIN pg_class c ON c.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = 'jobs_locked_by_worker_running_idx'
            """,
            schema,
        )
        assert rec is not None, "jobs_locked_by_worker_running_idx missing after apply_pending"
        assert rec["keys"] == ["locked_by_worker", "id"], (
            f"expected the (locked_by_worker, id) key list, got {rec['keys']}"
        )
        assert rec["predicate"] == f"(status = 'running'::{schema}.job_status)", (
            f"expected the status = 'running' partial predicate, got {rec['predicate']}"
        )
    finally:
        await _drop_schema(conn, schema)


async def test_pre_migration_index_shape_exhibits_the_heap_filter_walk(pg_dsn: str) -> None:
    """Red control: the same stale-stats shape against the pre-01.00.19_01
    one-key index must exhibit the O(running) heap-filter walk.  This is the
    regression surface the two-key index removes; if the planner ever stops
    mispicking here, this test's failure is the planner improvement landing,
    and the control (not the invariant pin) is what should be relaxed."""
    conn, schema = await _fresh_schema(pg_dsn)
    try:
        await conn.execute(f'DROP INDEX "{schema}".jobs_locked_by_worker_running_idx')
        await conn.execute(
            f'CREATE INDEX jobs_locked_by_worker_running_idx ON "{schema}".jobs (locked_by_worker) '
            f"WHERE status = 'running'"
        )
        wid, ids = await _seed_running_population(conn, schema)
        nodes = await _fence_scan_nodes(conn, schema, ids[0], wid)
        assert nodes
        removed = max((n.get("Rows Removed by Filter") or 0) for n in nodes)
        assert removed > 1, (
            "the one-key pre-01.00.19_01 index shape no longer exhibits the "
            "O(running) heap-filter walk - the planner fixed the mispick; relax "
            "this control and re-measure benchmarks/ab_fence_index.py"
        )
    finally:
        await _drop_schema(conn, schema)
