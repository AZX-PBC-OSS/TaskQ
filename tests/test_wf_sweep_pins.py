"""The sweep-arm-family pins (T04): pins 2+5 (the flow-status leg INSIDE the fire statement — a post-cancel re-derive refuses), pin 8 (MISNAMED-CHILD — blocked-with-reason, never a silent fire), pin 4 (HELD-ROW EXCLUSIVITY), pin 15 (PHANTOM-RUNNING reaped), pin 17 (EMPTY-JOIN — the edge-ledger formula is load-bearing).

Driven against a live Postgres through the REAL engine; the shared seed
helpers + fixtures live in ``tests/_wf_fixtures.py`` (the composed-fixture
home), the red-output sink flushes to ``.measurements/pin-reds.json`` (a
file that gets READ — BUILD-PROTOCOL §2), the shipped invariants green,
the unfenced variants kept in this file forever as the convicted shapes.
"""

# Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
# Why: random module used for timing jitter in race tests, not crypto.

from __future__ import annotations

import json

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import reap_phantom_ledger, sweep_join_rederive
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    RedLog,
    claim_view,
    fire_count,
    node_state,
    seed_edge,
    seed_flow,
    seed_join,
    seed_running_node,
)

# ── Pins 2 + 5: UNFENCED CANCEL / SWEEP-FIRE-POST-CANCEL ────────────────


def _drop_flow_leg_sql(wf_sql: WorkflowSql, statement: str) -> str:
    """THE CONVICTED SHAPE: the fire WITHOUT the flow-status EXISTS leg —
    the any-two-are-insufficient proof's missing leg. The mutation drops
    every ``AND EXISTS (…)``
    clause (balanced parens — the leg's body nests a subquery's own)."""
    mutated = statement
    legs_dropped = 0
    while True:
        head = mutated.find("AND EXISTS")
        if head == -1:
            break
        open_paren = mutated.index("(", head)
        depth = 0
        close = open_paren
        for i in range(open_paren, len(mutated)):
            if mutated[i] == "(":
                depth += 1
            elif mutated[i] == ")":
                depth -= 1
                if depth == 0:
                    close = i
                    break
        mutated = mutated[:head] + mutated[close + 1 :]
        legs_dropped += 1
    assert legs_dropped >= 1, "the flow-leg mutation drill did not arm"
    return mutated


@pytest.mark.integration
async def test_pin_5_sweep_fire_refuses_post_cancel(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The tx1→tx2 crash window x cancel x sweep: the child's tx1 commits,
    the worker dies before tx2, the CANCEL lands (the flow flip — the
    join-wait row's own kill has not reached it), the sweep's re-derive
    finds count=0 and attempts the guarded fire — the fire must REFUSE via
    the flow-status leg INSIDE the sweep's own fire statement."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)

    # tx1 commits (the parent terminalizes); tx2 NEVER runs (the crash
    # window) — the decrement is still pending.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        parent,
    )
    # THE CANCEL lands — the flow flip ONLY (the join-wait row's own kill
    # has not landed; that race is exactly what the leg exists for).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'cancelled' WHERE id = $1", flow_id
    )

    # THE UNFENCED VARIANT REDS: the sweep fire without the flow-status leg
    # fires the join the cancel killed.
    rederive = wf_sql.rederive_sweep  # the reconcile runs first (it is not the fence under test)
    await wf_conn.fetch(rederive, 50, "orphan_parent")
    unfenced = _drop_flow_leg_sql(wf_sql, wf_sql.sweep_fire)
    fire_ids = [new_uuid()]
    winners = await wf_conn.fetch(unfenced, fire_ids, 50)
    engine_redlog.red(
        "pin2-pin5-unfenced-sweep-fire",
        "sweep fire without the flow-status EXISTS leg",
        {"fired_post_cancel": len(winners)},
    )
    assert len(winners) == 1, (
        "the unfenced variant did not fire post-cancel — the red comparator "
        "is broken (the leg must be load-bearing)"
    )

    # THE SHIPPED ARM on an identical state: the flow-status leg INSIDE the
    # fire statement refuses — zero fires, the counter frozen.
    join2 = await seed_join(wf_conn, wf_schema, flow_id, step_key="join2", deps=1)
    parent2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join2, parent2, flow_id)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        parent2,
    )
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.firable == 1, summary  # the count says fire...
    assert await fire_count(wf_conn, wf_schema, join2) == 0, "...but the flow is dead"
    assert (await node_state(wf_conn, wf_schema, join2))["deps_pending"] == 0, (
        "the cache reconciled (count = 0 un-terminal); the FIRE is what the leg refuses"
    )


# ── Pin 8: MISNAMED-CHILD (blocked-with-reason, never a silent fire) ────


def _drop_missing_parent_arm_sql(wf_sql: WorkflowSql) -> str:
    """THE CONVICTED SHAPE (the spike's): the rederive has NO missing-parent
    arm — the count of un-terminal parents reads a MISSING parent as
    counted-and-terminal, and the join whose parent is GONE fires silently
    (the 'record healthy, work wrong' class)."""
    mutated = wf_sql.rederive_sweep.replace(
        "count(*) FILTER (WHERE p.id IS NULL) AS missing_parents,",
        "0::bigint AS missing_parents,",
    )
    assert mutated != wf_sql.rederive_sweep, "the mutation drill did not arm"
    return mutated


def _drop_missing_parent_arm_fire_sql(wf_sql: WorkflowSql) -> str:
    """The FIRE arm's twin of the same conviction: no missing-parent arm in
    its counts either."""
    mutated = wf_sql.sweep_fire.replace(
        "count(*) FILTER (WHERE p.id IS NULL) AS missing_parents,",
        "0::bigint AS missing_parents,",
    )
    assert mutated != wf_sql.sweep_fire, "the fire's mutation drill did not arm"
    return mutated


@pytest.mark.integration
async def test_pin_8_misnamed_child_blocked_with_reason(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """A child edge pointing at a MISSING parent: the shipped arm stamps
    the blocked-with-reason state (metadata.blocking_reason='orphan_parent')
    and NEVER fires; the convicted variant (a JOIN that drops the missing
    parent) fires the orphan silently."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    missing_parent = new_uuid()  # never inserted: the misnamed-child edge
    await seed_edge(wf_conn, wf_schema, join_id, missing_parent, flow_id)

    # THE SHIPPED ARM: blocked-with-reason, no fire.
    shipped = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert shipped.blocked == 1, shipped  # the missing-parent join stamped
    state = await node_state(wf_conn, wf_schema, join_id)
    state_meta = (
        state["metadata"] if isinstance(state["metadata"], dict) else json.loads(state["metadata"])
    )
    assert state_meta["blocking_reason"] == "orphan_parent", state
    assert await fire_count(wf_conn, wf_schema, join_id) == 0, "the orphan never fires"

    # THE CONVICTED VARIANT REDS (on a throwaway twin join -- the shipped
    # arm's stamp REPLACES the join marker, which correctly drops the
    # blocked row out of every later sweep; the twin is the pristine
    # victim): BOTH statements lose the missing-parent arm, the count
    # reads a MISSING parent as counted-and-terminal, and the sweep fires
    # the orphan (the spike's silent-fire shape).
    twin = await seed_join(wf_conn, wf_schema, flow_id, step_key="twin", deps=1)
    await seed_edge(wf_conn, wf_schema, twin, missing_parent, flow_id)
    mutated_summary = await wf_conn.fetchrow(
        _drop_missing_parent_arm_sql(wf_sql), 50, "orphan_parent"
    )
    assert mutated_summary is not None and mutated_summary["firable"] >= 1, mutated_summary
    winners = await wf_conn.fetch(
        _drop_missing_parent_arm_fire_sql(wf_sql),
        [new_uuid() for _ in range(mutated_summary["firable"])],
        50,
    )
    engine_redlog.red(
        "pin8-misnamed-child",
        "the rederive + fire without the missing-parent arm (a missing parent counts as counted-and-terminal)",
        {"orphan_fired": len(winners)},
    )
    assert len(winners) == 1, (
        "the convicted variant did not fire the orphan — the red comparator "
        "is broken (the missing-parent twin fires silently)"
    )


# ── Pin 4: HELD-ROW EXCLUSIVITY (every sweep arm obeys) ─────────────────


@pytest.mark.integration
async def test_pin_4_held_row_invisible_to_the_rederive(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """A HELD row (pending + scheduled_at in the future -- the signal
    deadline, the only live timer on it) is INVISIBLE to the re-derive
    arm: not locked, not reconciled, not fired -- even when the LEDGER
    already says every parent is terminal (the crash-window composition).
    On wake the arm reconciles + fires exactly once. The budget arm (T19)
    carries the same invariant as ``AND NOT budget_paused``."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    held = await seed_join(wf_conn, wf_schema, flow_id, deps=2, scheduled_in=3600)
    parent1 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="p1")
    parent2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="p2")
    await seed_edge(wf_conn, wf_schema, held, parent1, flow_id)
    await seed_edge(wf_conn, wf_schema, held, parent2, flow_id)

    # parent1 finalizes legitimately (tx2 decrements: 2 -> 1).
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent1,
        step_key="p1",
        worker_id=(await claim_view(wf_conn, wf_schema, parent1))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    assert result.applied
    assert (await node_state(wf_conn, wf_schema, held))["deps_pending"] == 1

    # parent2's tx1 commits; tx2 NEVER runs (the crash window) -- the
    # ledger says terminal, the cache says 1. The row is then HELD (the
    # operator's signal deadline lands in the future).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        parent2,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET scheduled_at = now() + interval '3600 seconds' "
        "WHERE id = $1",
        held,
    )

    # WHILE HELD: the arm skips the row entirely -- the stale cache is
    # untouched, nothing fires, whatever the ledger says.
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.firable == 0, summary
    assert await fire_count(wf_conn, wf_schema, held) == 0, "a held row never fires"
    assert (await node_state(wf_conn, wf_schema, held))["deps_pending"] == 1, (
        "the held row is not even reconciled -- the signal deadline is the only live timer on it"
    )

    # ON WAKE (the deadline arrives): the arm reconciles the cache from
    # the ledger (0 un-terminal) and fires exactly once.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET scheduled_at = now() - interval '1 second' WHERE id = $1",
        held,
    )
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.firable == 1, summary
    assert await fire_count(wf_conn, wf_schema, held) == 1
    assert (await node_state(wf_conn, wf_schema, held))["deps_pending"] == 0


# ── Pin 15: PHANTOM-RUNNING (the fenced-attempt sweep arm) ──────────────


@pytest.mark.integration
async def test_pin_15_phantom_running_reaped(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """A fenced/abandoned attempt's ledger row left 'running' forever on a
    terminal flow — the rows-alone reconstruction cannot reconcile it. The
    shipped arm reaps any phantom: status → 'fenced'."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded")  # terminal flow
    phantom = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, status) VALUES ($1, $2, $3, 'a', NULL, 1, 'running')",
        phantom,
        flow_id,
        new_uuid(),
    )
    reaped = await reap_phantom_ledger(module_pg_pool, wf_sql)
    assert reaped == 1, reaped
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE id = $1', phantom
    )
    assert status == "fenced", status


# ── Pin 17: EMPTY-JOIN (the edge-ledger formula is load-bearing) ────────


@pytest.mark.integration
async def test_pin_17_empty_join_never_fires(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """A NESTED join (a join-of-joins) whose children DO NOT EXIST YET must
    NOT fire — the child-row-recount variant reads 0 and fires early (the
    fanout proof's live hit at 1/6 decrements); the edge-ledger formula is
    the cure."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    # J1: the inner join, waiting on parent P (running).
    j1 = await seed_join(wf_conn, wf_schema, flow_id, step_key="j1", deps=1)
    p = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, j1, p, flow_id)
    # J2: the OUTER join, waiting on J1 — whose OWN children (the 200
    # per-property joins of the fanout scenario) DO NOT EXIST YET.
    j2 = await seed_join(wf_conn, wf_schema, flow_id, step_key="j2", deps=1)
    await seed_edge(wf_conn, wf_schema, j2, j1, flow_id)

    # THE CONVICTED VARIANT (the child-row recount): J2's children count = 0
    # (they don't exist yet) — it fires the EMPTY join.
    j2_child_rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1', j2
    )
    engine_redlog.red(
        "pin17-empty-join",
        "child-row recount (count children WHERE non-terminal)",
        {"j2_child_rows": int(j2_child_rows), "fires_early": int(j2_child_rows) == 0},
    )
    assert int(j2_child_rows) == 0, "the convicted variant's blind count"

    # THE SHIPPED FORMULA: J2's parent J1 is still pending (un-terminal) —
    # unterminal = 1, no fire, whatever J2's own children look like.
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.firable == 0, summary
    assert await fire_count(wf_conn, wf_schema, j2) == 0, "the empty join must not fire"

    # P finalizes → J1's counter hits 0 → J1 fires (tx2) — J2's parent is
    # still pending (the fire made J1 CLAIMABLE, not terminal): J2 waits.
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=p,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, p))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    assert result.applied
    assert await fire_count(wf_conn, wf_schema, j1) == 1
    assert await fire_count(wf_conn, wf_schema, j2) == 0, "J2 waits for J1's TERMINAL"

    # J1 is now claimable; claim it (running) and finalize it → tx2
    # decrements J2 → J2 fires exactly once.
    j1_worker, j1_attempt, j1_epoch = new_uuid(), 1, 0
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "
        "locked_by_worker = $2, claim_epoch = 0, lock_expires_at = "
        "now() + interval '90 seconds' WHERE id = $1",
        j1,
        j1_worker,
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=j1,
        step_key="j1",
        worker_id=j1_worker,
        attempt=j1_attempt,
        claim_epoch=j1_epoch,
        outcome="succeeded",
    )
    assert result.applied
    assert await fire_count(wf_conn, wf_schema, j2) == 1, "the outer join fires once"
    assert (await node_state(wf_conn, wf_schema, j2))["deps_pending"] == 0
