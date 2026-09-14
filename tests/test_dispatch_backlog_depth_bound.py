"""Dispatch's row work is independent of pending-backlog depth.

The shipped dispatch bounds — the STABLE ``scheduled_at`` selection bound
that serves the candidates lateral as an Index Cond, the strict-FIFO
lateral's ``LIMIT``, and ``per_actor_capacity``'s idle-actor EXISTS
prefilter — bound the strict-FIFO candidate *scan*. Two shapes in the
same CTE still do backlog-proportional row work per dispatch round:

* the round-robin candidates lateral computes ``ROW_NUMBER() OVER
  (PARTITION BY COALESCE(fairness_key, '__null__') ...)`` over EVERY due
  pending row of the (actor, queue) pair before the outer
  ``w2.fairness_rank <= residual * oversample`` filter can drop them — a
  window function cannot short-circuit, and the lateral carries no
  LIMIT, so the WindowAgg (and the scan or sort feeding it) processes the
  whole due backlog at every depth;
* the ``locked`` CTE re-joins ``ranked`` back onto ``jobs`` with a plain
  ``j.status = 'pending'`` predicate, and the LIMIT bounds render as
  ``(SELECT ... FROM params)`` subqueries the planner cannot fold — the
  candidate chain is estimated at the whole index range, so the join is
  served as a Seq Scan + Hash over the entire pending backlog.

Oracle: EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of the production
constants, executed once per seeded depth (1k and 30k due pending rows;
one actor, one queue, one fairness_key cohort). A node's ``Actual Rows``
multiplied by its ``Actual Loops`` is its exact total row work —
deterministic for a fixed seed, the same doctrine as the loop-count
oracle in tests/test_sweepaudit_dispatch_bound.py — so the pin asserts
row counts, never wall time. A depth-bounded dispatch's widest node
carries the candidate/locked set: at most residual * oversample rows per
(actor, queue) pair (limit_n * oversample for a NULL-cap actor) plus the
locked/eligible stages at limit_n. The asserted bound holds that with
headroom at every depth; only backlog-proportional work exceeds it.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)

pytestmark = pytest.mark.integration

_SHALLOW_DEPTH = 1_000
_DEEP_DEPTH = 30_000
_LIMIT_N = 50
_OVERSAMPLE = 2
_LOCK_LEASE = timedelta(seconds=30)
_ACTOR = "depth_probe"
_QUEUE = "depth_q"
_FAIRNESS_KEY = "cohort_a"

_VARIANTS = (
    ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
    ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
)

# A depth-bounded dispatch's widest node carries ~limit_n * oversample
# candidate rows plus the locked/eligible stages at limit_n — ~100 rows on
# this module's single (actor, queue, cohort) seed. Six times that headroom
# absorbs any bounded shape's constant factors; only work that grows with
# the backlog (a window over every due row, a scan+hash of the whole
# pending set) exceeds it.
_NODE_ROW_BOUND = 600
# The same contract stated across depths: the deep backlog's widest node
# may not exceed a small multiple of the shallow one's.
_DEPTH_RATIO_BOUND = 3


@pytest.fixture(scope="module")
async def depth_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, one registered actor."""
    schema = f"dispatch_depth_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _ACTOR,
            _QUEUE,
        )
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _seed_due_backlog(conn: asyncpg.Connection, schema: str, depth: int) -> None:
    """Re-seed ``depth`` due pending rows for the one (actor, queue, cohort).

    A single fairness_key cohort: whatever per-partition bound a
    depth-bounded round-robin lateral takes, this seed's candidate work
    stays at limit_n * oversample rows. Every row is due, pending, and
    identity-free, so the candidates/locked stages see the full depth and
    nothing else. VACUUM (ANALYZE) mirrors the steady-state plan surface
    every dispatch measurement in this repository seeds against.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind, fairness_key) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
        "(g % 3)::smallint, clock_timestamp() - interval '1 minute', 3, "
        "'transient', $3 FROM generate_series(1, $4::int) AS g",
        _ACTOR,
        _QUEUE,
        _FAIRNESS_KEY,
        depth,
    )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')


def _plan_node_row_counts(plan: dict[str, Any]) -> list[tuple[float, str]]:
    """(rows * loops, node label) for every plan node, widest first.

    ``Actual Rows`` is per loop and ``Actual Loops`` is the node's
    execution count, so the product is the node's total row work — exact
    counts for a fixed seed, the assertion medium this module shares with
    the loop-count oracle in tests/test_sweepaudit_dispatch_bound.py.
    """
    counted: list[tuple[float, str]] = []
    stack: list[dict[str, Any]] = [plan]
    while stack:
        node = stack.pop()
        rows = float(node.get("Actual Rows", 0) or 0)
        loops = int(node.get("Actual Loops", 1) or 1)
        node_type = str(node.get("Node Type", "unknown node"))
        relation = node.get("Relation Name")
        label = node_type if relation is None else f"{node_type} on {relation}"
        counted.append((rows * loops, label))
        stack.extend(node.get("Plans") or [])
    counted.sort(key=lambda entry: entry[0], reverse=True)
    return counted


async def _explain_row_work(
    conn: asyncpg.Connection, sql: str, worker_id: UUID
) -> tuple[float, list[tuple[float, str]]]:
    """EXPLAIN (ANALYZE) the rendered dispatch CTE.

    Returns (execution milliseconds, per-node row work widest-first).
    EXPLAIN ANALYZE executes the dispatch UPDATE, so each caller re-seeds
    the backlog first — the row counts are then exact for that depth.
    """
    rows = await conn.fetch(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
        [_QUEUE],
        _LIMIT_N,
        worker_id,
        _LOCK_LEASE,
        _OVERSAMPLE,
    )
    raw = rows[0]["QUERY PLAN"]
    document: Any = json.loads(raw) if isinstance(raw, str) else raw
    top: dict[str, Any] = document[0]
    plan: dict[str, Any] = top["Plan"]
    execution_ms = float(top["Execution Time"])
    return execution_ms, _plan_node_row_counts(plan)


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_dispatch_row_work_is_depth_bounded(
    pg_dsn: str, depth_schema: str, variant: str, sql: str
) -> None:
    """Every node of the dispatch plan processes a depth-independent
    number of rows, at a 1k and at a 30k due backlog.

    The dispatch round admits limit_n jobs from a single (actor, queue,
    cohort); a depth-bounded plan's widest node carries the
    candidate/locked set at every depth. A node whose actual row count
    grows with the seeded depth is backlog-proportional work per round —
    the round-robin lateral's unbounded ROW_NUMBER window over every due
    pending row, or the locked CTE's whole-backlog scan+hash
    re-resolution of ``j.status = 'pending'``.
    """
    rendered = sql.format(schema=depth_schema)
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_depth: dict[int, float] = {}
        for depth in (_SHALLOW_DEPTH, _DEEP_DEPTH):
            await _seed_due_backlog(conn, depth_schema, depth)
            execution_ms, nodes = await _explain_row_work(conn, rendered, worker_id)
            widest, label = nodes[0]
            widest_by_depth[depth] = widest
            assert widest <= _NODE_ROW_BOUND, (
                f"{variant}: at a {depth}-row due backlog the dispatch plan's "
                f"widest node ({label}) did {widest:.0f} rows of work — "
                "backlog-proportional, where a depth-bounded dispatch does at "
                f"most ~{_LIMIT_N * _OVERSAMPLE} candidate/locked rows at any "
                f"depth (bound {_NODE_ROW_BOUND}). Widest nodes: {nodes[:5]}; "
                f"execution {execution_ms:.2f} ms."
            )
        deep_widest = widest_by_depth[_DEEP_DEPTH]
        shallow_widest = widest_by_depth[_SHALLOW_DEPTH]
        assert deep_widest <= _DEPTH_RATIO_BOUND * shallow_widest, (
            f"{variant}: the dispatch plan's widest node grows with backlog "
            f"depth — {shallow_widest:.0f} rows of work at {_SHALLOW_DEPTH} "
            f"pending vs {deep_widest:.0f} at {_DEEP_DEPTH} "
            f"(ratio bound {_DEPTH_RATIO_BOUND}x)."
        )
    finally:
        await conn.close()
