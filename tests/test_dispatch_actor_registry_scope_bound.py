"""A dispatch round's cost must not grow with the number of REGISTERED
actors that are unrelated to the round's own polled (actor, queue) pairs.

tests/test_dispatch_fleet_scope_bound.py grows the rest of the fleet's
*pending job backlog* (cohort count and row depth) while holding
``actor_config`` fixed at exactly two rows (the two polled actors) for
every fleet size in ``_FLEET_SIZES`` -- its unpolled "fleet actors" are
referenced only in ``jobs.actor``, never inserted into ``actor_config``.
That fixture therefore cannot see cost that scales with the number of
ROWS IN actor_config itself, as opposed to cost that scales with pending
jobs. This module isolates that axis: pending jobs are held fixed (the
round's own small backlog only, zero unrelated jobs), and only
``actor_config`` row count grows, with actors that have no pending work
at all.

Several CTEs in the round-robin/strict-fifo SQL family
(``src/taskq/backend/_dispatch_sql.py``) read ``actor_config`` directly:
``per_actor_capacity``, ``repend_capacity``, the ``capped_ranked`` filter
join, and the ``eligible_candidates``/``locked`` stage's LEFT JOINs back
to ``actor_config`` for ``max_concurrent``/cap bookkeeping. If any of
these executes as a Seq Scan of the whole table rather than an indexed,
bounded probe keyed to the round's own actors, the round's cost grows
with the fleet's total registered-actor count -- a number an operator
running one small queue does not control and cannot see reflected in
their own queue's metrics.
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

_LIMIT_N = 50
_OVERSAMPLE = 2
_LOCK_LEASE = timedelta(seconds=30)

_VARIANTS = (
    ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
    ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
)

# The round's own slice: two polled actors, two polled queues, a small
# fixed due backlog. Identical at every registry size below.
_POLLED_QUEUES = ("registry_polled_a", "registry_polled_b")
_POLLED_ACTORS = ("registry_polled_actor_a", "registry_polled_actor_b")
_OWN_COHORTS_PER_QUEUE = 3
_OWN_ROWS_PER_COHORT = 20

# Registered actors with NO pending work and NO subscription overlap
# with the polled queues -- pure actor_config bloat. This is the axis
# test_dispatch_fleet_scope_bound.py cannot vary: its fixture seeds
# exactly two actor_config rows at every fleet size.
_REGISTRY_SIZES = (0, 2_000)

# A correctly scoped round's work is bounded by its own polled
# (actor, queue) pairs, not by fleet-wide actor_config size. Generous
# headroom over the round's own few-hundred-row candidate/locked work,
# while still catching a table-wide Seq Scan of thousands of unrelated
# actor_config rows.
_NODE_ROW_BOUND = 1_500
_REGISTRY_RATIO_BOUND = 3


@pytest.fixture
async def registry_schema(pg_dsn: str) -> Any:
    schema = f"dispatch_registry_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _seed(conn: asyncpg.Connection, schema: str, registry_size: int) -> None:
    """Re-seed actor_config and the round's own fixed backlog.

    ``registry_size`` OTHER actors are registered in actor_config, each
    on its own queue this round never polls, with ZERO pending jobs --
    isolating actor_config row count as the only thing that grows.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(f'TRUNCATE TABLE "{schema}".actor_config CASCADE')
    for actor, queue in zip(_POLLED_ACTORS, _POLLED_QUEUES, strict=True):
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            actor,
            queue,
        )
    if registry_size:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) '
            "SELECT 'registry_other_actor_' || g::text, "
            "'registry_other_queue_' || g::text "
            "FROM generate_series(1, $1::int) AS g",
            registry_size,
        )
    for actor, queue in zip(_POLLED_ACTORS, _POLLED_QUEUES, strict=True):
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, fairness_key) "
            "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
            "(g % 3)::smallint, clock_timestamp() - interval '1 minute', 3, "
            "'transient', 'own_cohort_' || (g % $3)::text "
            "FROM generate_series(1, $4::int) AS g",
            actor,
            queue,
            _OWN_COHORTS_PER_QUEUE,
            _OWN_COHORTS_PER_QUEUE * _OWN_ROWS_PER_COHORT,
        )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".actor_config, "{schema}".jobs')


def _plan_node_row_counts(plan: dict[str, Any]) -> list[tuple[float, str]]:
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


def _actor_config_scan_work(plan: dict[str, Any]) -> float:
    """Total row work done by any plan node scanning actor_config."""
    total = 0.0
    stack: list[dict[str, Any]] = [plan]
    while stack:
        node = stack.pop()
        if node.get("Relation Name") == "actor_config":
            rows = float(node.get("Actual Rows", 0) or 0)
            loops = int(node.get("Actual Loops", 1) or 1)
            total += rows * loops
        stack.extend(node.get("Plans") or [])
    return total


async def _explain_row_work(
    conn: asyncpg.Connection, sql: str, worker_id: UUID
) -> tuple[float, int, list[tuple[float, str]], float]:
    rows = await conn.fetch(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
        list(_POLLED_QUEUES),
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
    buffers = int(plan.get("Shared Hit Blocks", 0) or 0) + int(
        plan.get("Shared Read Blocks", 0) or 0
    )
    return (
        execution_ms,
        buffers,
        _plan_node_row_counts(plan),
        _actor_config_scan_work(plan),
    )


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_dispatch_round_cost_is_scoped_to_registered_actor_count(
    pg_dsn: str, registry_schema: str, variant: str, sql: str
) -> None:
    """A round polling two actors pays the same cost whether actor_config
    holds those two rows alone or thousands of unrelated, idle actors.

    Growing actor_config with actors that have no pending work and no
    queue overlap with the round must not change the widest plan node's
    row work: a round's cost should be a function of the (actor, queue)
    pairs it polls, never of how many OTHER actors happen to be
    registered fleet-wide.
    """
    rendered = sql.format(schema=registry_schema)
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_size: dict[int, float] = {}
        ac_work_by_size: dict[int, float] = {}
        buffers_by_size: dict[int, int] = {}
        for registry_size in _REGISTRY_SIZES:
            await _seed(conn, registry_schema, registry_size)
            execution_ms, buffers, nodes, ac_work = await _explain_row_work(
                conn, rendered, worker_id
            )
            widest, label = nodes[0]
            widest_by_size[registry_size] = widest
            ac_work_by_size[registry_size] = ac_work
            buffers_by_size[registry_size] = buffers
            assert widest <= _NODE_ROW_BOUND, (
                f"{variant}: with {registry_size} unrelated registered actors "
                f"(zero pending work, no queue overlap), the dispatch plan's "
                f"widest node ({label}) did {widest:.0f} rows of work "
                f"(bound {_NODE_ROW_BOUND}). Widest nodes: {nodes[:5]}; "
                f"actor_config scan work: {ac_work:.0f}; buffers {buffers}; "
                f"execution {execution_ms:.2f} ms."
            )
        empty_widest = widest_by_size[_REGISTRY_SIZES[0]]
        full_widest = widest_by_size[_REGISTRY_SIZES[-1]]
        empty_ac = ac_work_by_size[_REGISTRY_SIZES[0]]
        full_ac = ac_work_by_size[_REGISTRY_SIZES[-1]]
        assert full_widest <= _REGISTRY_RATIO_BOUND * max(empty_widest, 1.0), (
            f"{variant}: the dispatch plan's widest node grows with registered "
            f"actor count -- {empty_widest:.0f} rows of work with "
            f"{_REGISTRY_SIZES[0]} unrelated actors vs {full_widest:.0f} with "
            f"{_REGISTRY_SIZES[-1]}, while the polled actors/queues never "
            f"changed (ratio bound {_REGISTRY_RATIO_BOUND}x). actor_config scan "
            f"work: {empty_ac:.0f} -> {full_ac:.0f}. Buffers: "
            f"{buffers_by_size[_REGISTRY_SIZES[0]]} -> "
            f"{buffers_by_size[_REGISTRY_SIZES[-1]]}. A dispatch round must probe "
            "actor_config for its own actors, never scan the whole registry."
        )
    finally:
        await conn.close()
