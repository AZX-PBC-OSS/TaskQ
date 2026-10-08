"""T20's CORE FIX pins (the spike's live finding): the maintenance leg's
failed arm gates on live UNRESOLVED work.

THE FINDING (observed live in the T20 concept-proof spike, PROOF.md §6b):
the shipped ``WORKFLOW_ROOT_MAINTAIN_SQL`` failed arm gated on
``has_active`` = running/crashed/abandoned ONLY — pending/scheduled
siblings did NOT hold a failing run. A streaming source's FIRST chain
failure flipped the root ``failed`` at the next sweep pass while the other
chains were still pending, and the certified dispatch fence (workflow
children of a terminal flow are unclaimable) stranded them pending forever
— the spike's 149-stranded-chains scenario. For a no-fan-in BATCH run
(independent applications) that is the wrong semantics: one bad
application must not kill the other 199.

THE FIX (one leg, no column, no index, no migration): the failed arm
gates on live UNRESOLVED work —

    bool_or(status IN ('running','crashed','abandoned')
            OR (status IN ('pending','scheduled')
                AND NOT (metadata ? 'blocking_reason')))

The blocked-stamp exclusion is exactly what keeps the phase-2 H1 wedge
cured: a resolved-blocked join row (``blocking_reason`` stamped) is
RESOLVED — it must not hold the run; a pending CHAIN row (no stamp) is
UNRESOLVED work — it must.

Red-first: the 149-siblings pin ran RED against the shipped leg (the root
flipped ``failed`` with 149 live siblings — the stranding reproduced);
the greens below are the fix's evidence. Captured:
``.measurements/t20-maintain-liveness-*.txt``.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from tests._wf_fixtures import (
    RedLog,
    seed_edge,
    seed_flow,
    seed_join,
    seed_running_node,
)


async def seed_chain_node(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    step_key: str = "screen",
    map_index: int | None = None,
    trace_id: str | None = None,
) -> JobId:
    """A pending chain-start row (the emit tx's product: pending, no
    blocking stamp, the flow link + the per-application trace)."""
    node_id = new_uuid()
    meta: dict[str, object] = {"flow_id": str(flow_id)}
    if trace_id is not None:
        meta["trace_id"] = trace_id
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, map_index, metadata, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'wf', 'default', '{}', 3, 'transient', "
        "'pending', $2, $3, $4::jsonb, 'workflow', $5)",
        node_id,
        step_key,
        map_index,
        json.dumps(meta),
        f"wf:{flow_id}:emit:{map_index}:{step_key}",
    )
    return JobId(node_id)


async def root_row(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> dict[str, Any]:
    rec = await conn.fetchrow(
        f'SELECT status, finished_at, error_class FROM "{schema}".jobs WHERE id = $1',
        flow_id,
    )
    assert rec is not None
    return dict(rec)


async def run_maintenance(conn: asyncpg.Connection, wf_sql: WorkflowSql) -> int:
    """One maintenance pass (the sweep's own call shape — the shipped
    statement, batch 200)."""
    rows = await conn.fetchval(wf_sql.workflow_root_maintain, 200)
    return int(rows or 0)


# ── THE 149-STRANDED-CHAINS SCENARIO (the spike's live finding) ─────────


@pytest.mark.integration
async def test_t20_failed_arm_waits_for_live_siblings(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """ONE failed chain + 149 pending siblings: the failed arm must NOT
    finalize the root while live UNRESOLVED work is on the floor.

    RED (the shipped leg, the spike's observation): the root flipped
    ``failed`` at the first maintenance pass while the 149 siblings were
    pending — and the certified dispatch fence then made them unclaimable
    FOREVER (the 149-stranded-chains wedge). GREEN (the fix): the root
    WAITS; the siblings terminalize; the root terminals THEN — every
    chain's outcome on the record."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    siblings = [
        await seed_chain_node(
            wf_conn, wf_schema, flow_id, step_key="screen", map_index=i, trace_id=f"app-{i}"
        )
        for i in range(149)
    ]
    # THE ONE FAILURE: a non-absorbed failed chain (no absorbing edge).
    doomed = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="enrich")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now(), "
        "error_class = 'RuntimeError' WHERE id = $1",
        doomed,
    )

    # ONE maintenance pass with the siblings live: the root must WAIT.
    await run_maintenance(wf_conn, wf_sql)
    root = await root_row(wf_conn, wf_schema, flow_id)
    t20_redlog.red(
        "t20-failed-arm-live-siblings",
        "the failed arm gating on has_active (running/crashed/abandoned) "
        "ONLY — pending/scheduled siblings do not hold a failing run: the "
        "root flips 'failed' while the batch's other chains are live, and "
        "the dispatch fence strands them pending forever "
        "(the 149-stranded-chains wedge)",
        {"root_status_with_149_live_siblings": root["status"]},
    )
    assert root["status"] == "running", (
        f"THE 149-STRANDED-CHAINS WEDGE: the root reports "
        f"{root['status']!r} while 149 pending siblings are live — the "
        "failed arm must gate on live UNRESOLVED work, not on "
        "running/crashed/abandoned alone"
    )
    assert root["finished_at"] is None, "a live run has no finish stamp"

    # THE DISPATCH-FENCE CONSEQUENCE (the wedge's tooth, on the record):
    # a terminal flow's children are unclaimable — the shipped leg's flip
    # would have stranded all 149. (The root did NOT flip, so the
    # siblings must still be claimable-shaped: pending, dispatchable.)
    still_pending = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE id = ANY($1) AND status = 'pending'",
        siblings,
    )
    assert int(still_pending or 0) == 149, "the live siblings must be untouched"

    # THE DRAIN: the siblings terminalize (149 succeed past the one
    # failure), THEN the root terminals — 'failed' (the non-absorbed
    # failure outranks the completed siblings), with the terminals in.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = ANY($1)",
        siblings,
    )
    await run_maintenance(wf_conn, wf_sql)
    root = await root_row(wf_conn, wf_schema, flow_id)
    assert root["status"] == "failed", (
        f"after the drain the root must terminalize 'failed' (the "
        f"non-absorbed failure), got {root['status']!r}"
    )
    assert root["error_class"] == "UnabsorbedNodeFailure", "the maintenance stamp names the reason"


@pytest.mark.integration
async def test_t20_failed_arm_wedge_cure_stands(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """THE BLOCKED-STAMP EXCLUSION IS LOAD-BEARING: the phase-2 H1 wedge's
    cure must survive the fix — a resolved-blocked join row
    (``blocking_reason='failed_parent'`` stamped by the crash-window heal)
    must NOT hold a failing run. With the fix, a stamped pending row is
    not 'unresolved work' — the root terminals 'failed' in the same pass
    the shipped leg did."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="fc_join", deps=2)
    failed_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c0")
    peer = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c1")
    await seed_edge(wf_conn, wf_schema, join_id, failed_child, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, peer, flow_id)

    # THE CRASH WINDOW: tx1's fenced terminal FAILURE lands, tx2 never
    # runs; the sweep's blocked_required heal stamps the join.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now(), "
        "error_class = 'ValueError' WHERE id = $1",
        failed_child,
    )
    from taskq.workflows._sweep import sweep_join_rederive

    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.blocked_required >= 1, "the heal must stamp the join first"
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
        peer,
    )

    await run_maintenance(wf_conn, wf_sql)
    root = await root_row(wf_conn, wf_schema, flow_id)
    t20_redlog.red(
        "t20-failed-arm-blocked-stamp",
        "a has_unresolved predicate WITHOUT the blocked-stamp exclusion — "
        "every pending row (a resolved-blocked join row included) would "
        "hold the run: the original H1 wedge returns (the root wedges "
        "'running' forever over a stamped row nothing will ever "
        "terminalize)",
        {"root_status": root["status"]},
    )
    assert root["status"] == "failed", (
        f"THE WEDGE'S RETURN: a resolved-blocked join row (the "
        f"failed_parent stamp) must not hold a failing run — the root "
        f"reports {root['status']!r}"
    )
    assert root["finished_at"] is not None
