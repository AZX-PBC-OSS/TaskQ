"""A dispatch round's cost is scoped to its own queues, and its yield
survives a generic plan.

Two contracts live here, both about the dispatch CTE family under a
realistic *fleet* shape rather than the single-queue, single-actor,
single-cohort seed the depth oracle uses.

Fleet scope: a worker polls a small subset of the fleet's queues. The
cost of one dispatch round for those queues must not grow with what
other queues and other actors have pending, nor with how deep the
fleet's overall backlog is. Operationally this is the difference
between a queue that keeps dispatching at its own steady latency and
one whose rounds slow down every time an unrelated team's backlog
grows -- a coupling no operator can diagnose from their own queue's
metrics, because nothing about their queue changed. The single-queue
depth oracle cannot see this: it grows one cohort on the polled queue,
so a plan node that walks every pending cohort fleet-wide reads as
merely depth-proportional there, and a plan node scoped to the round's
own queues reads identically. Separating the two requires holding the
round's own backlog fixed while the rest of the fleet grows in both
cohort count and row depth, which is what the seed below does.

Generic-plan yield: the dispatch bounds are parameters rather than
subquery expressions specifically so that a round admits its full
``limit_n`` whether the planner builds a custom plan (bounds folded to
literals) or a generic one (bounds opaque). A previous shape that
expressed those bounds as subqueries under-dispatched badly under a
generic plan -- it returned a couple of rows where fifty were due and
claimable -- which starves a fleet silently: no error, no alert, just
throughput that quietly collapses once a statement has been executed
enough times for Postgres to switch to a generic plan. Forcing
``plan_cache_mode = force_generic_plan`` for the session reproduces
that condition deterministically instead of waiting on the planner's
five-execution heuristic.

Both oracles read EXPLAIN (ANALYZE) output of the production SQL
constants, the same doctrine as tests/test_dispatch_backlog_depth_bound.py:
a node's ``Actual Rows`` multiplied by its ``Actual Loops`` is its exact
total row work, deterministic for a fixed seed, so the assertions are on
row counts and admitted-row counts, never on wall time.
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
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
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

# The round's OWN fleet slice: two actors on two polled queues, each
# holding a handful of due cohorts. Small and identical at every fleet
# size, so any growth in row work comes from outside it.
_POLLED_QUEUES = ("fleet_polled_a", "fleet_polled_b")
_POLLED_ACTORS = ("fleet_actor_a", "fleet_actor_b")
_OWN_COHORTS_PER_QUEUE = 3
_OWN_ROWS_PER_COHORT = 20

# The REST of the fleet: queues this round never polls, actors it never
# dispatches, each with its own cohorts and its own due backlog. Both
# the cohort count and the row depth grow between the two fleet sizes,
# so a plan that walks cohorts fleet-wide and one that scans rows
# fleet-wide are both caught.
_FLEET_SIZES = (0, 400)
_FLEET_ROWS_PER_COHORT = 25

# A correctly scoped round does candidate work on the order of
# limit_n * oversample rows per polled (actor, queue) probe plus the
# locked/eligible stages at limit_n. With two polled actors that is a
# few hundred rows; the bound leaves generous headroom for constant
# per-round overhead while still catching a walk of the thousands of
# rows and hundreds of cohorts the unpolled fleet holds.
_NODE_ROW_BOUND = 1_500
# The same contract stated across fleet sizes: growing the rest of the
# fleet must not multiply the widest node's row work.
_FLEET_RATIO_BOUND = 3

# Generic-plan yield: the polled backlog is far deeper than limit_n, so
# a round that admits fewer than its full limit is under-dispatching.
_GENERIC_PLAN_BACKLOG = 600


@pytest.fixture(scope="module")
async def fleet_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, polled actors registered."""
    schema = f"dispatch_fleet_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        for actor, queue in zip(_POLLED_ACTORS, _POLLED_QUEUES, strict=True):
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
                actor,
                queue,
            )
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _seed_own_slice(conn: asyncpg.Connection, schema: str) -> None:
    """Seed the round's own due backlog: fixed at every fleet size.

    Each polled actor gets ``_OWN_COHORTS_PER_QUEUE`` due fairness-key
    cohorts on its own queue. This is the work the round is entitled to
    look at, and it never changes size, so the fleet comparison below
    isolates cost that comes from elsewhere.
    """
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


async def _seed_fleet(conn: asyncpg.Connection, schema: str, fleet_size: int) -> None:
    """Re-seed the own slice plus ``fleet_size`` unpolled cohorts.

    Every unpolled cohort belongs to its own actor on its own queue --
    neither is anything this round dispatches -- and every one of its
    rows is due and pending, so the only structure that can reach them
    is one whose scope is the whole jobs table rather than the round's
    own (actor, queue) pairs. Growing both the cohort count and the row
    count together mirrors how a real fleet grows: more teams' actors,
    each with more backlog.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await _seed_own_slice(conn, schema)
    if fleet_size:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, fairness_key) "
            "SELECT gen_random_uuid(), 'fleet_actor_' || c::text, "
            "'fleet_q_' || c::text, '{\"v\": 1}'::jsonb, 'pending', "
            "0::smallint, clock_timestamp() - interval '1 minute', 3, "
            "'transient', 'fleet_cohort_' || c::text "
            "FROM generate_series(1, $1::int) AS c, "
            "generate_series(1, $2::int) AS r",
            fleet_size,
            _FLEET_ROWS_PER_COHORT,
        )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')


def _plan_node_row_counts(plan: dict[str, Any]) -> list[tuple[float, str]]:
    """(rows * loops, node label) for every plan node, widest first."""
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
) -> tuple[float, int, list[tuple[float, str]]]:
    """EXPLAIN (ANALYZE) one dispatch round over the polled queues.

    Returns (execution ms, shared buffers touched, per-node row work
    widest-first).
    """
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
    return execution_ms, buffers, _plan_node_row_counts(plan)


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_dispatch_round_cost_is_scoped_to_its_own_queues(
    pg_dsn: str, fleet_schema: str, variant: str, sql: str
) -> None:
    """A round polling two queues pays the same cost whether the rest of
    the fleet is empty or holds hundreds of other actors' cohorts.

    The polled slice is byte-for-byte identical at both fleet sizes, so
    every extra row of plan work measured at the larger size comes from
    rows the round is not entitled to look at: other actors, other
    queues, other cohorts. A dispatcher must be insulated from them --
    otherwise one team's backlog growth degrades every other team's
    dispatch latency, with no signal on the affected queue to explain
    it.
    """
    rendered = sql.format(schema=fleet_schema)
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_size: dict[int, float] = {}
        buffers_by_size: dict[int, int] = {}
        for fleet_size in _FLEET_SIZES:
            await _seed_fleet(conn, fleet_schema, fleet_size)
            execution_ms, buffers, nodes = await _explain_row_work(conn, rendered, worker_id)
            widest, label = nodes[0]
            widest_by_size[fleet_size] = widest
            buffers_by_size[fleet_size] = buffers
            assert widest <= _NODE_ROW_BOUND, (
                f"{variant}: with {fleet_size} unpolled cohorts elsewhere in the "
                f"fleet, the dispatch plan's widest node ({label}) did "
                f"{widest:.0f} rows of work. The round's own backlog is fixed at "
                f"{len(_POLLED_ACTORS) * _OWN_COHORTS_PER_QUEUE * _OWN_ROWS_PER_COHORT} "
                f"due rows, so a round scoped to its own queues does at most a few "
                f"hundred candidate/locked rows (bound {_NODE_ROW_BOUND}). Widest "
                f"nodes: {nodes[:5]}; buffers {buffers}; execution {execution_ms:.2f} ms."
            )
        empty_widest = widest_by_size[_FLEET_SIZES[0]]
        full_widest = widest_by_size[_FLEET_SIZES[-1]]
        assert full_widest <= _FLEET_RATIO_BOUND * max(empty_widest, 1.0), (
            f"{variant}: the dispatch plan's widest node grows with the rest of "
            f"the fleet -- {empty_widest:.0f} rows of work with "
            f"{_FLEET_SIZES[0]} unpolled cohorts vs {full_widest:.0f} with "
            f"{_FLEET_SIZES[-1]}, while the polled queues' own backlog never "
            f"changed (ratio bound {_FLEET_RATIO_BOUND}x). Buffers: "
            f"{buffers_by_size[_FLEET_SIZES[0]]} -> {buffers_by_size[_FLEET_SIZES[-1]]}. "
            "A dispatch round must be scoped to the (actor, queue) pairs it "
            "polls; work proportional to cohorts and rows outside them couples "
            "every queue's latency to fleet-wide backlog."
        )
    finally:
        await conn.close()


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_dispatch_admits_full_limit_under_a_generic_plan(
    pg_dsn: str, fleet_schema: str, variant: str, sql: str
) -> None:
    """A dispatch round admits its full ``limit_n`` even when Postgres
    builds a generic plan for the statement.

    Every dispatch bound is a bound parameter rather than a subquery so
    that the round's yield does not depend on the planner folding those
    bounds to literals. Under ``plan_cache_mode = force_generic_plan``
    the folding does not happen, and a shape that relies on it
    under-dispatches: the round claims a fraction of what is due and
    claimable, and the fleet's throughput collapses with no error and
    no alert to attribute it to. The backlog seeded here is an order of
    magnitude deeper than the limit, so a full round is the only
    correct outcome -- and the same round must yield the same count
    with the planner left to its own devices.
    """
    rendered = sql.format(schema=fleet_schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'TRUNCATE TABLE "{fleet_schema}".jobs CASCADE')
        await conn.execute(
            f'INSERT INTO "{fleet_schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, fairness_key) "
            "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
            "(g % 3)::smallint, clock_timestamp() - interval '1 minute', 3, "
            "'transient', 'generic_cohort_' || (g % 4)::text "
            "FROM generate_series(1, $3::int) AS g",
            _POLLED_ACTORS[0],
            _POLLED_QUEUES[0],
            _GENERIC_PLAN_BACKLOG,
        )
        await conn.execute(f'VACUUM (ANALYZE) "{fleet_schema}".jobs')

        async def _one_round() -> int:
            rows = await dispatch_batch_sql(
                conn,
                sql=rendered,
                queues=list(_POLLED_QUEUES),
                limit_n=_LIMIT_N,
                worker_id=new_uuid(),
                lock_lease=_LOCK_LEASE,
                oversample=_OVERSAMPLE,
            )
            return len(rows)

        custom_plan_yield = await _one_round()
        assert custom_plan_yield == _LIMIT_N, (
            f"{variant}: fixture broken -- with {_GENERIC_PLAN_BACKLOG} due "
            f"claimable rows an uncontended round must admit all {_LIMIT_N}, "
            f"got {custom_plan_yield}"
        )

        await conn.execute("SET plan_cache_mode = force_generic_plan")
        try:
            generic_plan_yield = await _one_round()
        finally:
            await conn.execute("SET plan_cache_mode = auto")

        assert generic_plan_yield == _LIMIT_N, (
            f"{variant}: under plan_cache_mode = force_generic_plan the dispatch "
            f"round admitted {generic_plan_yield} of {_LIMIT_N} rows, with "
            f"{_GENERIC_PLAN_BACKLOG} due, pending, uncontended rows available "
            f"(the same round admitted {custom_plan_yield} with the planner left "
            "on auto). Dispatch bounds must be bound parameters, never subquery "
            "expressions whose value the planner cannot fold into a generic "
            "plan: a round whose yield depends on custom-plan folding silently "
            "collapses fleet throughput once Postgres switches a hot statement "
            "to a generic plan, with no error and no alert."
        )
    finally:
        await conn.close()
