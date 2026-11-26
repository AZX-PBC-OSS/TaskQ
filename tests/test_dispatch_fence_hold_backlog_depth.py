"""The hold-stamp leg's ACCEPTED cost: a standing held backlog defeats the
dispatch statement's and the claimable probe's LIMIT bounds, and that cost is
priced rather than denied.

Issue #748 adds ``AND NOT <alias>.metadata ? 'hold'`` to both the dispatch fence
(``_WF_DISPATCH_FENCE_TEMPLATE``) and the claimable probe fence
claimable probe's fence (``_WF_PROBE_FENCE_TEMPLATE``). Both statements are
otherwise built so a round's work is bounded by ``limit_n * oversample``
candidate rows, never by backlog depth: each inner probe is an ordered index
scan whose ``LIMIT`` stops at the first *matching* row. The branch's module
comment admits the exception: standing held rows are "filtered after a heap
visit (issue #748)".

The measurement below is that price, pinned at its accepted shape. A held row
is ``pending`` with ``deps_pending = 0`` and, when the hold is untimed — the
issue's own population — no deadline, so it keeps its ``scheduled_at`` and
``priority`` and sorts ahead of the routable rows behind it. ``metadata ?
'hold'`` is not indexable: ``jobs``' only GIN index is ``jsonb_path_ops``
(``01.00.00_01_pre_initial.sql:174``), which serves ``@>`` and not ``?``, and no
partial index carries the negation. So the fence can only be a Filter, the
scan cannot stop at a held row, and every held row ahead of the first routable
one is visited and discarded on *every* dispatch round — for as long as the
hold stands (forever, for an untimed wait).

These pins are the accept-and-price remedy: the widest node's row work may be
linear in the standing held backlog (the accepted per-round number), and the
CONTROL half — the identical statement with the leg textually removed, over
the same seeded rows — must stay depth-flat, which is what proves the leg,
not the seeded population, is the cause. A future indexable marker (a partial
index on the pending population, or the stamp mirrored into a column) removes
the exception; these bounds then hold with room to spare, and tightening them
back to the old depth contract is that change's own pin.

The oracle is the sibling depth pin's
(``tests/test_dispatch_backlog_depth_bound.py``): ``EXPLAIN (ANALYZE, BUFFERS,
FORMAT JSON)`` of the production constants, and a node's
``(Actual Rows + Rows Removed by Filter) * Actual Loops`` as its exact row work
— deterministic for a fixed seed, never wall time. ``Rows Removed by Filter``
is counted so the oracle sees an Index Scan that reads the whole held backlog
and emits nothing, which is the shape a bounded probe takes under this fence.

The file is integration-only (real PG, real migrations) and its cost tests
seed 10,000 rows per render; run it with the integration lane.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
    WF_LOOP_ESCALATION_STEP_KEY,
)

pytestmark = pytest.mark.integration

_ACTOR = "held_depth"
_QUEUE = "held_q"
_LIMIT_N = 50
_OVERSAMPLE = 2
_LOCK_LEASE = timedelta(seconds=30)

# The sibling depth pin's probe bound, reused verbatim: a depth-bounded probe's
# widest node is the one-row actor_config scan plus one matched probe row.
_PROBE_NODE_ROW_BOUND = 100
# The sibling depth pin's cross-depth contract: the deep backlog's widest node
# may not exceed a small multiple of the shallow one's.
_DEPTH_RATIO_BOUND = 3

# The ACCEPTED cost of the hold-stamp leg (issue #748's accept-and-price
# remedy): the widest node's row work may be linear in the standing held
# backlog, because the unindexable leg is a Filter that cannot honour a LIMIT.
# The constants bound the MEASURED shape with headroom for the planner's
# choice of access path (an ordered Index Scan vs a Bitmap Scan moves the
# count by a small constant factor at these sizes). The control half — the
# same rows with the leg removed — keeps the old depth-flat contract, which is
# what proves the leg is the cause.
_HELD_ROW_FACTOR = 3
_HELD_ROW_FLOOR = _PROBE_NODE_ROW_BOUND

_SHALLOW_DEPTH = 1_000
_DEEP_DEPTH = 10_000


@pytest.fixture(scope="module")
async def held_depth_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, one registered actor."""
    schema = f"dispatch_held_{new_base62()}".lower()
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


async def _seed_held_backlog(
    conn: asyncpg.Connection,
    schema: str,
    *,
    held_depth: int,
    routable_depth: int,
) -> None:
    """``held_depth`` held node rows + ``routable_depth`` claimable ones.

    Every row is a live flow's node row (``step_key`` set, ``metadata.flow_id``
    and ``fairness_key`` pointing at a non-terminal root — the shape
    ``jobs_wf_fairness_cohort_check`` requires), due, ``deps_pending = 0``:
    exactly what ``_HOLD_HOLD_NODE_SQL`` leaves behind for an untimed wait. The
    held rows are seeded one minute older than the routable ones with the same
    ``priority``, so they sit ahead of the routable rows in the
    ``(priority DESC, scheduled_at, id)`` order the probes read — the shape a
    real held backlog takes, since a hold leaves ``scheduled_at`` and
    ``priority`` untouched.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    flow_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, scheduled_at) "
        "VALUES ($1::uuid, 'flow', 'default', '{}'::jsonb, 3, 'transient', 'running', "
        "'__flow__', jsonb_build_object('flow_id', $2::text), now() - interval '2 minutes')",
        flow_id,
        str(flow_id),
    )
    if held_depth:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, step_key, metadata, deps_pending, fairness_key) "
            "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', 0, "
            "clock_timestamp() - interval '1 minute', 3, 'transient', 'approve', "
            "jsonb_build_object('flow_id', $3::text, 'hold', gen_random_uuid()::text, "
            "'held_signal', 'approval'), 0, $3::text "
            "FROM generate_series(1, $4::int) AS g",
            _ACTOR,
            _QUEUE,
            str(flow_id),
            held_depth,
        )
    if routable_depth:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, step_key, metadata, deps_pending, fairness_key) "
            "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', 0, "
            "clock_timestamp(), 3, 'transient', 'work', "
            "jsonb_build_object('flow_id', $3::text), 0, $3::text "
            "FROM generate_series(1, $4::int) AS g",
            _ACTOR,
            _QUEUE,
            str(flow_id),
            routable_depth,
        )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')


def _widest_node(plan: dict[str, Any]) -> tuple[float, str]:
    """The plan's widest node by total row work (``rows * loops``).

    ``Rows Removed by Filter`` counts as work: an Index Scan over the whole
    held backlog that emits nothing reports ``Actual Rows`` of 0, and only
    the removed count shows what it read.
    """
    counted: list[tuple[float, str]] = []
    stack: list[dict[str, Any]] = [plan]
    while stack:
        node = stack.pop()
        rows = float(node.get("Actual Rows", 0) or 0) + float(
            node.get("Rows Removed by Filter", 0) or 0
        )
        loops = int(node.get("Actual Loops", 1) or 1)
        node_type = str(node.get("Node Type", "unknown node"))
        relation = node.get("Relation Name")
        label = node_type if relation is None else f"{node_type} on {relation}"
        counted.append((rows * loops, label))
        stack.extend(node.get("Plans") or [])
    counted.sort(key=lambda entry: entry[0], reverse=True)
    return counted[0]


async def _widest_for_probe(
    conn: asyncpg.Connection, schema: str, *, leg: bool
) -> tuple[float, str]:
    sql = DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=schema)
    if not leg:
        # The control: the identical statement with the hold-stamp leg
        # removed. Only that one clause differs.
        sql = sql.replace("AND NOT j.metadata ? 'hold'", "")
    rows = await conn.fetch(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", [_QUEUE])
    raw = rows[0]["QUERY PLAN"]
    document: Any = json.loads(raw) if isinstance(raw, str) else raw
    return _widest_node(document[0]["Plan"])


async def _widest_for_claim(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: Any,
    *,
    render: str,
    leg: bool,
) -> tuple[float, str]:
    sql = render.format(schema=schema)
    if not leg:
        # The control: every rendered fence seat's hold-stamp leg removed.
        sql = sql.replace("AND NOT j.metadata ? 'hold'", "").replace(
            "AND NOT j2.metadata ? 'hold'", ""
        )
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
    return _widest_node(document[0]["Plan"])


async def _workflow_capable_worker(conn: asyncpg.Connection, schema: str) -> Any:
    """A worker row the execution probe reads as ``workflow_execution = true``."""
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'held-depth', 1, ARRAY[$2]::text[], "
        "'{\"workflow_execution\": true}'::jsonb)",
        worker_id,
        _QUEUE,
    )
    return worker_id


async def test_claimable_probe_pays_a_standing_held_backlog(
    pg_dsn: str, held_depth_schema: str
) -> None:
    """A queue whose pending rows are ALL held is permanently an empty round,
    so the probe is the statement such a queue pays every dispatch poll. Its
    published depth contract ("the probe's work is bounded by registered
    actors x round queues, never by backlog depth") has one accepted
    exception (issue #748): the unindexable hold-stamp leg is a Filter, so
    the LIMIT-1 probe cannot stop at the first pending row and every held row
    is visited on every empty round, for as long as the hold stands.

    This pin prices that exception: row work linear in the held backlog is
    ACCEPTED, and the control (the same rows, the leg removed) staying
    depth-flat is what proves the leg rather than the population is the
    cause."""
    schema = held_depth_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_depth: dict[int, float] = {}
        control_by_depth: dict[int, float] = {}
        for depth in (_SHALLOW_DEPTH, _DEEP_DEPTH):
            await _seed_held_backlog(conn, schema, held_depth=depth, routable_depth=0)
            widest, label = await _widest_for_probe(conn, schema, leg=True)
            widest_by_depth[depth] = widest
            control, _ = await _widest_for_probe(conn, schema, leg=False)
            control_by_depth[depth] = control
            accepted = _HELD_ROW_FACTOR * depth + _HELD_ROW_FLOOR
            assert widest <= accepted, (
                f"claimable probe: with {depth} standing held rows on an idle "
                f"queue the widest plan node ({label}) did {widest:.0f} rows of "
                f"work, above the accepted cost bound {accepted:.0f} (the "
                "hold-stamp leg is an unindexable Filter, so the probe visits "
                "the held backlog — but it may not exceed a small multiple of "
                "it). A growth beyond linear is a different defect: check for a "
                "per-candidate subplan over wf_signals or jobs. Control (the "
                f"same rows, the leg removed): {control:.0f} rows."
            )
            assert control <= _PROBE_NODE_ROW_BOUND, (
                f"claimable probe CONTROL: with {depth} held rows and the "
                f"hold-stamp disjunct textually removed, the widest plan node "
                f"still did {control:.0f} rows of work (bound "
                f"{_PROBE_NODE_ROW_BOUND}). The leg is not the cause of this "
                "pin's numbers, so the pin proves nothing — re-derive the "
                "control against the same seeded rows."
            )
        assert (
            control_by_depth[_DEEP_DEPTH] <= _DEPTH_RATIO_BOUND * control_by_depth[_SHALLOW_DEPTH]
        ), (
            "claimable probe CONTROL: the leg-removed statement is not itself "
            "depth-independent over the same seeded rows, so this pin is not "
            "isolating the leg: "
            f"{control_by_depth[_SHALLOW_DEPTH]:.0f} rows at {_SHALLOW_DEPTH} "
            f"vs {control_by_depth[_DEEP_DEPTH]:.0f} at {_DEEP_DEPTH}."
        )
    finally:
        await conn.close()


@pytest.mark.parametrize(
    ("variant", "render"),
    [("strict_fifo", DISPATCH_STRICT_FIFO_SQL), ("round_robin", DISPATCH_ROUND_ROBIN_SQL)],
    ids=["strict_fifo", "round_robin"],
)
async def test_dispatch_round_pays_a_standing_held_backlog(
    pg_dsn: str, held_depth_schema: str, variant: str, render: str
) -> None:
    """Each claim render's candidate probes are LIMIT-bounded index scans that
    stop at the first matching row; the hold-stamp leg makes a held row
    non-matching, so a held backlog ahead of the routable rows is re-walked by
    every round. Both base renders (strict-FIFO and round-robin) are measured:
    they share the fence seats but not the candidate lateral's access path.

    The accepted cost (issue #748's accept-and-price remedy) is row work
    linear in the standing held backlog. The CONTROL half — the same rows with
    the legs textually removed — keeps the old shape: bounded by the round's
    own candidate window and NOT growing with the held backlog. That contrast
    is what proves the leg, not the population, is the cause; a control that
    grew too would make this pin meaningless.
    """
    schema = held_depth_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        worker_id = await _workflow_capable_worker(conn, schema)
        widest_by_depth: dict[int, float] = {}
        control_by_depth: dict[int, float] = {}
        for depth in (_SHALLOW_DEPTH, _DEEP_DEPTH):
            await _seed_held_backlog(conn, schema, held_depth=depth, routable_depth=_LIMIT_N)
            widest, _ = await _widest_for_claim(conn, schema, worker_id, render=render, leg=True)
            widest_by_depth[depth] = widest
            # Re-seed: EXPLAIN ANALYZE ran the claim UPDATE and consumed the
            # routable rows.
            await _seed_held_backlog(conn, schema, held_depth=depth, routable_depth=_LIMIT_N)
            control, _ = await _widest_for_claim(conn, schema, worker_id, render=render, leg=False)
            control_by_depth[depth] = control
            accepted = _HELD_ROW_FACTOR * depth + _LIMIT_N * _OVERSAMPLE * 2
            assert widest <= accepted, (
                f"{variant} dispatch: the widest node's row work is {widest:.0f} "
                f"with {depth} standing held rows ahead of the round's "
                f"candidates, above the accepted linear cost bound {accepted:.0f}. "
                "The hold-stamp leg is an unindexable Filter, so visiting the "
                "held backlog is the accepted price; growth beyond a small "
                "multiple of it is a different defect (a per-candidate subplan "
                "over jobs or wf_signals). Control (the same rows, the legs "
                f"removed): {control:.0f} rows of work."
            )
        assert (
            control_by_depth[_DEEP_DEPTH] <= _DEPTH_RATIO_BOUND * control_by_depth[_SHALLOW_DEPTH]
        ), (
            "the control (the hold legs removed from the same renders, over the "
            "same seeded rows) is itself depth-dependent, so this pin is not "
            "isolating the leg: "
            f"{control_by_depth[_SHALLOW_DEPTH]:.0f} rows at {_SHALLOW_DEPTH} "
            f"vs {control_by_depth[_DEEP_DEPTH]:.0f} at {_DEEP_DEPTH}."
        )
        assert control_by_depth[_DEEP_DEPTH] <= _LIMIT_N * _OVERSAMPLE * 4, (
            f"{variant} dispatch CONTROL: with the hold legs removed the round's "
            f"widest node did {control_by_depth[_DEEP_DEPTH]:.0f} rows of work at "
            f"{_DEEP_DEPTH} held rows — the control is not bounded by the "
            f"round's own candidate window (~{_LIMIT_N * _OVERSAMPLE} rows), so "
            "the contrast this pin rests on is gone"
        )
    finally:
        await conn.close()


# ── the two fences may not disagree (the hold-stamp direction) ───────────
#
# The module's own law: "the two fences may not disagree: a probe that misses
# the escalation consumer would answer 'nothing remains' for the one row the
# claim fence admits — the worker sleeps, the page never expands a round."
# The branch's own pins exercise a held HITL signal row through one claim
# variant and one probe; the shape the new comment singles out — a *stamped
# escalation row*, fenced by the hold leg although the escalation-kind
# exemption would otherwise admit it — has no pin. Each shape below is seeded
# alone, and the two production statements must agree: the probe's answer and
# the claim's admission are either both true or both false.

#: (row shape, is it routable work?) — the expected answer both fences must
#: give. Agreement alone is not the assertion: two fences that both wrongly
#: admitted a held row would agree, and that shape is the wedge itself.
_ROW_SHAPES: tuple[tuple[str, bool], ...] = (
    ("vanilla", True),
    ("workflow_step", True),
    ("held_workflow_step", False),
    ("root_marker", False),
    ("workflow_step_on_terminal_flow", False),
    ("escalation_on_terminal_flow", True),
    ("held_escalation_on_terminal_flow", False),
)


async def _seed_one_shape(conn: asyncpg.Connection, schema: str, shape: str) -> Any:
    """Seed exactly one routable candidate of *shape*; return its id.

    Every workflow row carries the flow link and the fairness cohort the
    ``jobs_wf_fairness_cohort_check`` constraint requires; a ``held`` stamp is
    the held representation's marker (``metadata.hold``), and a terminal-flow
    shape seeds its root ``cancelled``.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO NOTHING",
        "shape_actor",
        "default",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO NOTHING",
        "flow",
        "default",
    )
    flow_id = new_uuid()
    flow_status = "cancelled" if "terminal_flow" in shape else "running"
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, scheduled_at) "
        "VALUES ($1::uuid, 'flow', 'default', '{}'::jsonb, 3, 'transient', $2, "
        "'__flow__', jsonb_build_object('flow_id', $3::text), now() - interval '2 minutes')",
        flow_id,
        "pending" if shape == "root_marker" else flow_status,
        str(flow_id),
    )
    if shape == "root_marker":
        return flow_id

    row_id = new_uuid()
    if shape == "vanilla":
        # Producer-placed, no flow link, no step key: the fleet's whole
        # population in the depth oracles.
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
            "max_attempts, retry_kind, scheduled_at) "
            "VALUES ($1, 'shape_actor', 'default', '{}'::jsonb, 'pending', 3, "
            "'transient', clock_timestamp())",
            row_id,
        )
        return row_id

    step_key = WF_LOOP_ESCALATION_STEP_KEY if "escalation" in shape else "approve"
    metadata: dict[str, object] = {"flow_id": str(flow_id)}
    if "escalation" in shape:
        # The consumer row the exhaust tx commits into a terminal flow: the
        # escalation-kind marker plus the exemption's own step key.
        metadata["wf_after_terminal_effect"] = True
    if "held" in shape:
        metadata["hold"] = str(new_uuid())
        metadata["held_signal"] = "approval"
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, metadata, deps_pending, fairness_key, "
        "scheduled_at) "
        "VALUES ($1, 'shape_actor', 'default', '{\"v\": 1}'::jsonb, 'pending', 3, "
        "'transient', $2, $3::jsonb, 0, $4::text, clock_timestamp())",
        row_id,
        step_key,
        json.dumps(metadata),
        str(flow_id),
    )
    return row_id


@pytest.mark.parametrize(("shape", "expected"), _ROW_SHAPES, ids=[s for s, _ in _ROW_SHAPES])
async def test_claim_fence_and_probe_fence_agree_on_a_row_shape(
    pg_dsn: str, held_depth_schema: str, shape: str, expected: bool
) -> None:
    """The claim fence's hold-stamp leg and the probe fence's matching
    disjunct must give the SAME routable answer for every row shape.

    A disagreement in either direction is a silent defect: the probe saying
    "nothing routable" for a row the claim would admit idles the worker fleet
    (the page never expands a round), and the reverse keeps an idle queue
    paying expansion rounds forever.
    """
    schema = held_depth_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'TRUNCATE TABLE "{schema}".workers CASCADE')
        worker_id = await _workflow_capable_worker(conn, schema)
        row_id = await _seed_one_shape(conn, schema, shape)

        probed = await conn.fetch(DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=schema), ["default"])
        probed_routable = bool(probed)
        claimed = await dispatch_batch(
            conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
            queues=["default"],
            limit_n=_LIMIT_N,
            worker_id=worker_id,
            lock_lease=_LOCK_LEASE,
        )
        claimed_ids = {str(row["id"]) for row in claimed}
        claimed_this_row = str(row_id) in claimed_ids

        assert probed_routable == expected, (
            f"the claimable probe answered routable={probed_routable} for the "
            f"{shape!r} row shape, expected {expected}. A stamped row is the "
            "held representation and is never routable work; a stamped "
            "escalation row is fenced by the hold leg although the "
            "escalation-kind exemption would otherwise admit it."
        )
        assert claimed_this_row == expected, (
            f"the dispatch claim admitted={claimed_this_row} for the {shape!r} "
            f"row shape, expected {expected}. The claim fence must refuse a "
            "stamped row (the wedge, issue #748) and must refuse a workflow "
            "step on a terminal flow, while still admitting the escalation "
            "consumer born into one."
        )
        assert probed_routable == claimed_this_row, (
            f"the two dispatch fences DISAGREE on the {shape!r} row shape: the "
            f"claimable probe says routable={probed_routable}, the claim "
            f"admitted={claimed_this_row} (claimed={sorted(claimed_ids)}). "
            "The probe fence carries the hold-stamp disjunct beside the "
            "escalation-kind exemption precisely so the two answers coincide; "
            "a gap here idles the fleet (probe false / claim would admit) or "
            "burns expansion rounds forever (probe true / claim never admits)."
        )
    finally:
        await conn.close()
