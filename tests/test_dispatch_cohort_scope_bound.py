"""Round-robin dispatch row work is independent of cohort count outside
the round's own (actor, queue) pairs.

The ``rr_keys`` recursive CTE (``src/taskq/backend/_dispatch_sql.py``)
walks pending ``(actor, queue, fairness_key)`` cohorts to enumerate the
round-robin fairness keys; scoped by ``queue = ANY($1)`` to the round's
own queues, so its per-round work tracks only what the round polls. The
per-(actor, queue) and due-time filtering happens in the candidates
lateral's inner probe (``_ROUND_ROBIN_CANDIDATES_LATERAL``), downstream
of the enumeration.

Oracle: same doctrine as tests/test_dispatch_backlog_depth_bound.py —
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of the production
DISPATCH_ROUND_ROBIN_SQL constant, executed once per seeded count of
UNRELATED cohorts (0 vs 2,000, each on a different queue that the
dispatched round never polls). The round's own backlog is held fixed at
one small cohort throughout. A round-robin dispatch whose cost is
correctly scoped to the round's own (actor, queue) pairs shows flat row
work (and buffer counts) across both seeds; a global, unscoped
enumeration instead grows the widest plan node's row work (and buffers)
with the unrelated cohort count.
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
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
)
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)

pytestmark = pytest.mark.integration

_LIMIT_N = 50
_OVERSAMPLE = 2
_LOCK_LEASE = timedelta(seconds=30)

# The round's OWN cohort: small and fixed across both seeds.
_ACTOR = "cohort_scope_probe"
_QUEUE = "cohort_scope_q"
_OWN_FAIRNESS_KEY = "own_cohort"
_OWN_ROW_COUNT = 20

# Unrelated cohorts: a different queue per cohort, future-scheduled (not
# due), so neither the round's (actor, queue) pair nor the due-time bound
# admits them anywhere except rr_keys's global, unfiltered enumeration.
_UNRELATED_COHORT_COUNTS = (0, 2_000)

# A round-robin dispatch correctly scoped to its own (actor, queue) pairs
# does candidate/locked work on the order of limit_n * oversample rows
# regardless of what else is pending elsewhere in the table. Generous
# headroom over that (10x) still catches a cohort-count-proportional scan
# of thousands of unrelated rows without being sensitive to constant
# per-round overhead.
_NODE_ROW_BOUND = 1_000
# The scoped contract stated across cohort counts: growing unrelated
# cohorts from 0 to 2,000 must not multiply the widest node's row work
# by more than this small factor.
_COHORT_RATIO_BOUND = 3


@pytest.fixture(scope="module")
async def cohort_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, one registered actor/queue."""
    schema = f"dispatch_cohort_{new_base62()}".lower()
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


async def _seed(conn: asyncpg.Connection, schema: str, unrelated_cohort_count: int) -> None:
    """Re-seed the round's own small due backlog plus N unrelated cohorts.

    The round's own (actor, queue, fairness_key) cohort stays fixed at
    _OWN_ROW_COUNT due rows throughout. Each unrelated cohort lives on
    its own queue (never polled by this round) with a single
    future-scheduled (not-yet-due) row, so it is excluded by BOTH the
    round's (actor, queue) filter and the due-time bound — the only
    thing that can still see it is rr_keys's global, unfiltered
    enumeration over every pending row table-wide.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind, fairness_key) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
        "0::smallint, clock_timestamp() - interval '1 minute', 3, "
        "'transient', $3 FROM generate_series(1, $4::int) AS g",
        _ACTOR,
        _QUEUE,
        _OWN_FAIRNESS_KEY,
        _OWN_ROW_COUNT,
    )
    if unrelated_cohort_count:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, fairness_key) "
            "SELECT gen_random_uuid(), $1, "
            "'unrelated_q_' || g::text, "
            "'{\"v\": 1}'::jsonb, 'pending', 0::smallint, "
            "clock_timestamp() + interval '1 day', 3, 'transient', "
            "'unrelated_cohort_' || g::text "
            "FROM generate_series(1, $2::int) AS g",
            _ACTOR,
            unrelated_cohort_count,
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
    """EXPLAIN (ANALYZE) the rendered dispatch CTE for the round's own queue.

    Returns (execution ms, shared buffer hits, per-node row work widest-first).
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
    buffers = int(plan.get("Shared Hit Blocks", 0) or 0) + int(
        plan.get("Shared Read Blocks", 0) or 0
    )
    return execution_ms, buffers, _plan_node_row_counts(plan)


async def test_round_robin_dispatch_row_work_is_cohort_scoped(
    pg_dsn: str, cohort_schema: str
) -> None:
    """Round-robin dispatch on one small queue must not pay per-cohort
    cost for OTHER queues' (or future-scheduled) cohorts.

    Widest-node row work must stay flat as unrelated cohort count grows
    from 0 to 2,000 -- those rows belong to queues this round never
    polls and are not yet due, so a correctly scoped round-robin
    dispatch never touches them. An unscoped ``rr_keys`` enumeration
    instead walks one step per unrelated cohort every round.
    """
    rendered = DISPATCH_ROUND_ROBIN_SQL.format(schema=cohort_schema)
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_count: dict[int, float] = {}
        buffers_by_count: dict[int, int] = {}
        for unrelated_count in _UNRELATED_COHORT_COUNTS:
            await _seed(conn, cohort_schema, unrelated_count)
            execution_ms, buffers, nodes = await _explain_row_work(conn, rendered, worker_id)
            widest, label = nodes[0]
            widest_by_count[unrelated_count] = widest
            buffers_by_count[unrelated_count] = buffers
            assert widest <= _NODE_ROW_BOUND, (
                f"with {unrelated_count} unrelated cohorts (other queues, "
                f"not yet due) the round-robin dispatch plan's widest node "
                f"({label}) did {widest:.0f} rows of work -- cohort-count-"
                "proportional, where a correctly scoped dispatch does at "
                f"most ~{_LIMIT_N * _OVERSAMPLE} candidate/locked rows "
                f"regardless of unrelated cohorts (bound {_NODE_ROW_BOUND}). "
                f"Widest nodes: {nodes[:5]}; buffers {buffers}; "
                f"execution {execution_ms:.2f} ms."
            )
        zero_widest = widest_by_count[0]
        many_widest = widest_by_count[_UNRELATED_COHORT_COUNTS[-1]]
        assert many_widest <= _COHORT_RATIO_BOUND * max(zero_widest, 1.0), (
            "the round-robin dispatch plan's widest node grows with the "
            "count of unrelated cohorts (other queues, not yet due) -- "
            f"{zero_widest:.0f} rows of work with 0 unrelated cohorts vs "
            f"{many_widest:.0f} with {_UNRELATED_COHORT_COUNTS[-1]} "
            f"(ratio bound {_COHORT_RATIO_BOUND}x). buffers: "
            f"{buffers_by_count[0]} -> {buffers_by_count[_UNRELATED_COHORT_COUNTS[-1]]}. "
            "The round-robin cohort enumeration (src/taskq/backend/"
            "_dispatch_sql.py, _RR_KEYS_CTE) must stay scoped to the "
            "round's own queues — an unfiltered walk visits every pending "
            "cohort table-wide, not just the round's own."
        )
    finally:
        await conn.close()


# ── The assignment-marker accounting contract ──────────────────────────
#
# A re-pended row whose ``started_at`` is still NULL is a real shape: an
# operator retry of a job that was terminalized BEFORE it was ever claimed
# (a deliberate hand-back that was never started). The routing marker is
# ``assignment_routed``; ``started_at IS NULL`` answers only "was
# claimed", so a probe or enumeration keyed on the proxy mis-files this
# row under its stale queue label: the label-routed arm then enumerates
# the stale label as a cohort key, and the claimable-rows probe reports
# the stale label as routable while going blind to the row's true routing
# (its actor's CURRENT assignment).

_MARKER_ACTOR = "marker_actor"
_MARKER_ASSIGNED_QUEUE = "marker_assigned_q"
_MARKER_STALE_QUEUE = "marker_stale_q"


async def _seed_divergent_repend_row(
    conn: asyncpg.Connection, schema: str, *, with_label_routed_row: bool = False
) -> UUID:
    """One pending, due, assignment_routed row with ``started_at`` NULL,
    carrying a stale queue label its actor is no longer assigned to.

    Returns the row's id. The shape cannot arise from a naive enqueue
    (producer-placed rows are always ``assignment_routed = false``), so
    the seed writes the marker directly — exactly the divergent row an
    operator ``retry_job`` on a never-claimed terminal job produces.

    *with_label_routed_row* adds one ordinary producer-placed due row on
    the actor's assigned queue, so the actor is visible to the
    label-routed capacity path and the cohort walk's content is actually
    consumed by the round (an actor with ONLY the divergent row never
    reaches the label-routed enumeration's consumers under either
    predicate regime, which hides the accounting drift from the plan).
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO UPDATE SET queue = EXCLUDED.queue",
        _MARKER_ACTOR,
        _MARKER_ASSIGNED_QUEUE,
    )
    row_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind, fairness_key, assignment_routed) "
        "VALUES ($1, $2, $3, '{\"v\": 1}'::jsonb, 'pending', 0::smallint, "
        "clock_timestamp() - interval '1 minute', 3, 'transient', "
        "'divergent_cohort', true)",
        row_id,
        _MARKER_ACTOR,
        _MARKER_STALE_QUEUE,
    )
    if with_label_routed_row:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, fairness_key) "
            "VALUES ($1, $2, $3, '{\"v\": 1}'::jsonb, 'pending', 0::smallint, "
            "clock_timestamp() - interval '1 minute', 3, 'transient', "
            "'legit_cohort')",
            new_uuid(),
            _MARKER_ACTOR,
            _MARKER_ASSIGNED_QUEUE,
        )
    return row_id


async def test_claimable_probe_routes_divergent_repend_by_assignment_marker(
    pg_dsn: str, cohort_schema: str
) -> None:
    """The empty-round probe must see the divergent row on the actor's
    ASSIGNED queue and must not see it on its stale label queue.

    The probe gates window expansion on an empty dispatch round, so a
    probe blind to the true routing (answering false on the assigned
    queue) leaves a locked-out window unexpanded while claimable rows
    wait, and a probe answering true on the stale label burns bounded
    expansions on a queue whose consumers can never admit the row.
    """
    probe = DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=cohort_schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _seed_divergent_repend_row(conn, cohort_schema)
        assigned_rows = await conn.fetch(probe, [_MARKER_ASSIGNED_QUEUE])
        assert assigned_rows, (
            "the probe is blind to a claimable re-pended row on the actor's "
            "assigned queue: the assignment-routed arm must match the marker "
            "(assignment_routed), not started_at IS NOT NULL — this row was "
            "never claimed, so its started_at is still NULL"
        )
        stale_rows = await conn.fetch(probe, [_MARKER_STALE_QUEUE])
        assert not stale_rows, (
            "the probe reports the row's STALE queue label as routable: the "
            "label-routed arm must match producer-placed rows by the marker "
            "(NOT assignment_routed), not started_at IS NULL"
        )
    finally:
        await conn.close()


async def test_label_routed_enumeration_skips_divergent_repend_row(
    pg_dsn: str, cohort_schema: str
) -> None:
    """The round-robin label-routed cohort walk must not enumerate the
    stale label as a cohort key, and the round admits both rows by their
    true routing.

    rr_keys is the label-routed cohort enumeration; with the actor
    holding one ordinary row on its assigned queue plus the divergent
    re-pend carrying the stale label, the walk's materialized content is
    exactly one (actor, queue, cohort) key under the marker predicate —
    and two under the started_at proxy (the stale label joins the walk).
    The assertion reads the rr_keys CTE Scan actuals: the widest scan of
    the materialized enumeration is its unfiltered full read, exact for
    a fixed seed under this module's EXPLAIN ANALYZE doctrine.
    """
    rendered = DISPATCH_ROUND_ROBIN_SQL.format(schema=cohort_schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        row_id = await _seed_divergent_repend_row(conn, cohort_schema, with_label_routed_row=True)
        rows = await conn.fetch(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {rendered}",
            [_MARKER_ASSIGNED_QUEUE, _MARKER_STALE_QUEUE],
            _LIMIT_N,
            new_uuid(),
            _LOCK_LEASE,
            _OVERSAMPLE,
        )
        raw = rows[0]["QUERY PLAN"]
        document: Any = json.loads(raw) if isinstance(raw, str) else raw
        top: dict[str, Any] = document[0]

        rr_keys_scan_rows: list[float] = []
        stack: list[dict[str, Any]] = [top["Plan"]]
        while stack:
            node = stack.pop()
            if node.get("Node Type") == "CTE Scan" and node.get("CTE Name") == "rr_keys":
                rr_keys_scan_rows.append(
                    float(node.get("Actual Rows", 0) or 0) * int(node.get("Actual Loops", 1) or 1)
                )
            stack.extend(node.get("Plans") or [])
        assert rr_keys_scan_rows, "expected a CTE Scan over rr_keys in the plan"
        assert max(rr_keys_scan_rows) == 1.0, (
            "the label-routed cohort walk must enumerate exactly the one "
            "legitimate (actor, assigned-queue, cohort) key — a second key "
            "means the divergent re-pend's STALE label entered the walk, "
            "which is the started_at-proxy accounting drift: rr_keys must "
            "select producer-placed rows by the marker (NOT "
            f"assignment_routed). rr_keys scan row work: {rr_keys_scan_rows}"
        )

        # End to end: a fresh round over the same two queues admits both
        # rows — the ordinary row by its label, the divergent re-pend by
        # its actor's current assignment.
        row_id = await _seed_divergent_repend_row(conn, cohort_schema, with_label_routed_row=True)
        claimed = await dispatch_batch_sql(
            conn,
            sql=rendered,
            queues=[_MARKER_ASSIGNED_QUEUE, _MARKER_STALE_QUEUE],
            limit_n=_LIMIT_N,
            worker_id=new_uuid(),
            lock_lease=_LOCK_LEASE,
            oversample=_OVERSAMPLE,
        )
        claimed_ids = {rec["id"] for rec in claimed}
        assert len(claimed) == 2 and row_id in claimed_ids, (
            "the round must admit both rows by their true routing — the "
            "producer-placed row by its label, the divergent re-pend by "
            f"its actor's assignment; claimed ids: {claimed_ids}"
        )
    finally:
        await conn.close()
