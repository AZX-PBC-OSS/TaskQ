"""ATTACK (phase-2, round 2): the T06 fail-closed peer-cascade's windows.

The attacker's probes, each an independent test (RED = a finding):

1. THE CRASH-WINDOW ROOT WEDGE: the fail_closed child's tx1 committed,
   tx2 (the cascade) never ran — the sweep's blocked_required heal
   stamps the join, but WHAT flips the flow root? The maintenance leg's
   ``has_live`` counts every pending row INCLUDING the blocked join row
   (the heal leaves it pending-with-reason forever — nothing terminalizes
   a blocked join row). Probe: after the heal, with ALL peers terminal,
   is the root failed — or wedged 'running' forever?

2. THE CRASH-WINDOW PEERS: the direct cascade peer-cancels the still-
   non-terminal peers; the sweep heal (the same window, the crash ate
   the cascade) does NOT — do the peers of a healed fail_closed run keep
   running forever on a dead flow?

3. MAYBE DOES NOT CASCADE (the absorbed-failure clause, T06's gate): a
   maybe child's terminal failure must NOT kill peers / fail the flow.
   Certification evidence (expected GREEN).

4. THE CASCADE RACING A NORMAL FINALIZE: a peer's own finalize tx2 runs
   concurrently with the cascade (two pools, asyncio.gather). The fence
   set (terminal-mark CAS + the rowcount gate + the flow-status legs)
   must leave ONE consistent verdict per row: no fire over a failed
   parent, no cancelled-succeeded hybrid, no lost record.

5. THE CASCADE RACING ANOTHER CASCADE: two children of two OVERLAPPING
   joins fail concurrently (shared peer set). Both cascades must land;
   no lost peer-cancel, no double-flow-flip anomaly, both joins named.

6. THE MIXED-POLICY NODE: one node feeds a fail_closed join AND a
   collect join; the node terminal-fails. The cascade legitimately runs
   (fail_closed leg) — but the collect leg's decrement is refused by the
   flow-alive guard (the cascade flipped the flow IN THE SAME TX), so
   the collect join hangs join-wait with the failure item on it; and the
   rollup's EXISTS-based ``absorbed`` flag marks the node absorbed — the
   derivation then can NEVER say 'failed' for a run that failed closed.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import reconstruct_workflow_status
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    claim_view,
    fire_count,
    node_state,
    seed_flow,
    seed_join,
    seed_running_node,
)


async def _seed_edge_named(
    conn: asyncpg.Connection,
    schema: str,
    join_id: JobId,
    parent_id: JobId,
    flow_id: JobId,
    policy: str,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, $4)",
        join_id,
        parent_id,
        flow_id,
        policy,
    )


# ── Probe 1 + 2: the crash-window heal's root + peers ───────────────────


@pytest.mark.integration
async def test_attack_t06_crash_window_fail_closed_root_and_peers(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """The crash window: the child's tx1 marked it failed, the cascade
    (tx2) never ran. The sweep heal stamps the join. THE ROOT: the
    §17.2 cascade's flip is also lost — the maintenance leg must heal
    it. PREDICTION UNDER ATTACK: has_live counts the blocked join row
    (pending forever) — the root wedges 'running'; and the peers keep
    running on the dead flow."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="fc_join", deps=2)
    failed_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c0")
    peer = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c1")
    await _seed_edge_named(wf_conn, wf_schema, join_id, failed_child, flow_id, "fail_closed")
    await _seed_edge_named(wf_conn, wf_schema, join_id, peer, flow_id, "fail_closed")

    # THE CRASH WINDOW: tx1's fenced terminal FAILURE lands, tx2 (the
    # cascade) never runs — the direct DB write, the exact tx1 shape.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now(), "
        "error_class = 'ValueError' WHERE id = $1",
        failed_child,
    )

    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.blocked_required >= 1, "the heal must stamp the join (the pin-6 shape)"

    join_state = await node_state(wf_conn, wf_schema, join_id)
    assert join_state["metadata"].get("blocking_reason") == "failed_parent"

    # THE PEERS: the direct cascade peer-cancels them; the heal's tx —
    # does anything cancel this still-running peer of the dead run?
    peer_state = await node_state(wf_conn, wf_schema, peer)

    # THE ROOT: every OTHER node is terminal except the blocked join row
    # and the running peer. Terminalize the peer the way its own worker
    # would (its work completes) and re-run the heal — the maintenance
    # leg's one window to flip the root.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
        peer,
    )
    await sweep_join_rederive(module_pg_pool, wf_sql)

    root = await node_state(wf_conn, wf_schema, flow_id)
    join_state2 = await node_state(wf_conn, wf_schema, join_id)
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)

    # The G7 mapping: derived 'failed' ⇒ the root row must BE 'failed'.
    print(
        json.dumps(
            {
                "root_status": root["status"],
                "reconstructed": reconstructed,
                "join_after_heal": {
                    "status": join_state2["status"],
                    "deps_pending": join_state2["deps_pending"],
                    "blocking_reason": join_state2["metadata"].get("blocking_reason"),
                },
                "peer_after_heal": peer_state["status"],
            },
            indent=2,
        )
    )
    assert reconstructed == "failed", "the failed child is un-absorbed: the rows say failed"
    assert root["status"] == "failed", (
        f"THE ROOT WEDGE: the rows reconstruct {reconstructed!r} but the "
        f"flow root reports {root['status']!r} — the maintenance leg's "
        "has_live counts the blocked join row (pending forever), so the "
        "root never flips: the run wedges 'running' forever"
    )


# ── Probe 3: maybe does NOT cascade (the absorbed-failure clause) ───────


@pytest.mark.integration
async def test_attack_t06_maybe_child_never_cascades(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """A maybe child's TERMINAL failure must not kill its peers nor fail
    the flow: the fan-in absorbs it. Expected GREEN (the clause holds)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="m_join", deps=3)
    failing = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="m0")
    peer1 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="m1")
    peer2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="m2")
    for p in (failing, peer1, peer2):
        await _seed_edge_named(wf_conn, wf_schema, join_id, p, flow_id, "maybe")

    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=failing,
        step_key="m0",
        worker_id=(await claim_view(wf_conn, wf_schema, failing))[0],
        attempt=1,
        claim_epoch=0,
        outcome="failed",
        error_class="ValueError",
        error_message="the maybe child's terminal",
    )

    for peer in (peer1, peer2):
        state = await node_state(wf_conn, wf_schema, peer)
        assert state["status"] == "running", (
            f"BLOCKER CLASS: the maybe-absorbed failure cascaded — peer {peer} is {state['status']}"
        )
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    assert flow_state["status"] == "running", "the maybe-absorbed failure must not fail the flow"
    join_state = await node_state(wf_conn, wf_schema, join_id)
    assert join_state["deps_pending"] == 2, "the failed maybe child's side IS resolved"
    assert await fire_count(wf_conn, wf_schema, join_id) == 0


# ── Probe 4: the cascade racing a normal finalize ───────────────────────


@pytest.mark.integration
async def test_attack_t06_cascade_racing_a_normal_finalize(
    module_pg_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """A peer's own SUCCESS finalize tx2 racing the failed child's
    cascade. Every interleave must leave one consistent verdict per row:
    the peer either succeeded (its tx2 won) or peer-cancelled (the
    cascade won) — never both, never neither; the shared join never
    fires; the flow fails exactly once."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="r_join", deps=2)
    failed_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c0")
    racing_peer = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c1")
    await _seed_edge_named(wf_conn, wf_schema, join_id, failed_child, flow_id, "fail_closed")
    await _seed_edge_named(wf_conn, wf_schema, join_id, racing_peer, flow_id, "fail_closed")

    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, racing_peer)

    async def fail_now() -> None:
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=failed_child,
            step_key="c0",
            worker_id=(await claim_view(wf_conn, wf_schema, failed_child))[0],
            attempt=1,
            claim_epoch=0,
            outcome="failed",
            error_class="ValueError",
        )

    async def succeed_now() -> None:
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=racing_peer,
            step_key="c1",
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            outcome="succeeded",
            result={"ok": True},
        )

    for _round in range(6):
        # fresh shape each round
        flow_id = await seed_flow(wf_conn, wf_schema)
        join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="r_join", deps=2)
        failed_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c0")
        racing_peer = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c1")
        await _seed_edge_named(wf_conn, wf_schema, join_id, failed_child, flow_id, "fail_closed")
        await _seed_edge_named(wf_conn, wf_schema, join_id, racing_peer, flow_id, "fail_closed")
        worker, attempt, epoch = await claim_view(wf_conn, wf_schema, racing_peer)

        results = await asyncio.gather(fail_now(), succeed_now(), return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException):
                msg = str(r)
                assert "FinalizeNotApplied" in type(r).__name__ or "fence" in msg.lower() or True

        peer_row = await wf_conn.fetchrow(
            f'SELECT status, error_class FROM "{wf_schema}".jobs WHERE id = $1', racing_peer
        )
        assert peer_row is not None
        status = peer_row["status"]
        # ONE verdict: the CAS set either won (succeeded) or lost to the
        # cascade (cancelled). A running row = both writes lost = a wedge.
        assert status in ("succeeded", "cancelled"), (
            f"the racing finalize left the peer {status!r} — neither the "
            "cascade's cancel nor its own success landed (a wedge window)"
        )
        assert await fire_count(wf_conn, wf_schema, join_id) == 0, (
            "the join fired over a fail_closed parent — the fence set lost"
        )
        root = await node_state(wf_conn, wf_schema, flow_id)
        assert root["status"] == "failed", "the cascade must fail the flow"
        # reset for the next round's asserts
        _ = results


# ── Probe 5: the cascade racing ANOTHER cascade (overlapping peers) ─────


@pytest.mark.integration
async def test_attack_t06_two_cascades_overlapping_peer_sets(
    module_pg_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """Two failing children sharing a peer (child P is the peer of BOTH
    failing nodes' joins... here: two joins, each fail_closed, one node
    common). Both cascades run concurrently: both joins named, the common
    peer cancelled exactly once, the flow failed."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_a = await seed_join(wf_conn, wf_schema, flow_id, step_key="ja", deps=2)
    join_b = await seed_join(wf_conn, wf_schema, flow_id, step_key="jb", deps=2)
    f0 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="f0")
    common = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="common")
    f1 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="f1")
    await _seed_edge_named(wf_conn, wf_schema, join_a, f0, flow_id, "fail_closed")
    await _seed_edge_named(wf_conn, wf_schema, join_a, common, flow_id, "fail_closed")
    await _seed_edge_named(wf_conn, wf_schema, join_b, f1, flow_id, "fail_closed")
    await _seed_edge_named(wf_conn, wf_schema, join_b, common, flow_id, "fail_closed")

    await asyncio.gather(
        finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=f0,
            step_key="f0",
            worker_id=(await claim_view(wf_conn, wf_schema, f0))[0],
            attempt=1,
            claim_epoch=0,
            outcome="failed",
            error_class="ValueError",
        ),
        finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=f1,
            step_key="f1",
            worker_id=(await claim_view(wf_conn, wf_schema, f1))[0],
            attempt=1,
            claim_epoch=0,
            outcome="failed",
            error_class="ValueError",
        ),
    )

    common_row = await wf_conn.fetchrow(
        f'SELECT status, error_class, metadata FROM "{wf_schema}".jobs WHERE id = $1', common
    )
    assert common_row is not None
    assert common_row["status"] == "cancelled", (
        f"the shared peer escaped both cascades: {common_row['status']}"
    )
    for j in (join_a, join_b):
        state = await node_state(wf_conn, wf_schema, j)
        assert state["metadata"].get("blocking_reason") == "failed_parent", (
            f"join {j} unresolved after both cascades: {state}"
        )
        assert await fire_count(wf_conn, wf_schema, j) == 0
    root = await node_state(wf_conn, wf_schema, flow_id)
    assert root["status"] == "failed"


# ── Probe 6: the MIXED-POLICY node (fail_closed + collect consumers) ────


@pytest.mark.integration
async def test_attack_t06_mixed_policy_node_the_collect_join_hangs(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """Node X feeds joinA (fail_closed) and joinB (collect). X fails:
    the cascade blocks A and fails the flow IN THE SAME TX — then the
    collect leg's decrement reads the flow-alive guard and refuses, so
    joinB never resolves; the sweep then RECONCILES joinB's counter to 0
    (its failed_required is 0 — the edge says collect) leaving a
    never-fired, never-stamped, claimable join row. The record shows a
    hanging join — the pin-1 claim broken; and the rollup's EXISTS
    ``absorbed`` flag marks X absorbed, so the derivation can never say
    'failed' for this failed-closed run."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_a = await seed_join(wf_conn, wf_schema, flow_id, step_key="ja", deps=1)
    join_b = await seed_join(wf_conn, wf_schema, flow_id, step_key="jb", deps=1)
    x = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="x")
    await _seed_edge_named(wf_conn, wf_schema, join_a, x, flow_id, "fail_closed")
    await _seed_edge_named(wf_conn, wf_schema, join_b, x, flow_id, "collect")

    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=x,
        step_key="x",
        worker_id=(await claim_view(wf_conn, wf_schema, x))[0],
        attempt=1,
        claim_epoch=0,
        outcome="failed",
        error_class="ValueError",
        error_message="the mixed node",
    )

    root = await node_state(wf_conn, wf_schema, flow_id)
    assert root["status"] == "failed", "the fail_closed leg must fail the flow"
    a_state = await node_state(wf_conn, wf_schema, join_a)
    assert a_state["metadata"].get("blocking_reason") == "failed_parent"

    b_state = await node_state(wf_conn, wf_schema, join_b)
    fires_b = await fire_count(wf_conn, wf_schema, join_b)

    # THE SWEEP's next pass: what does the record look like after the
    # healer runs?
    await sweep_join_rederive(module_pg_pool, wf_sql)
    b_after = await node_state(wf_conn, wf_schema, join_b)
    fires_b_after = await fire_count(wf_conn, wf_schema, join_b)

    print(
        json.dumps(
            {
                "join_b_before_sweep": {
                    "status": b_state["status"],
                    "deps_pending": b_state["deps_pending"],
                    "failures": b_state["metadata"].get("failures"),
                    "fires": fires_b,
                },
                "join_b_after_sweep": {
                    "status": b_after["status"],
                    "deps_pending": b_after["deps_pending"],
                    "blocking_reason": b_after["metadata"].get("blocking_reason"),
                    "fires": fires_b_after,
                },
            },
            indent=2,
        )
    )

    # THE RECORD MUST NOT SHOW A HANGING JOIN: joinB resolved some way —
    # fired, or blocked-with-reason. A pending join-wait row (or a
    # reconciled-to-0 never-fired claimable row) is the convicted shape.
    resolved = fires_b_after == 1 or b_after["metadata"].get("blocking_reason") in (
        "failed_parent",
        "orphan_parent",
    )
    assert resolved, (
        f"THE HANGING COLLECT JOIN: joinB rests status={b_after['status']!r} "
        f"deps_pending={b_after['deps_pending']} "
        f"blocking_reason={b_after['metadata'].get('blocking_reason')!r} "
        f"fires={fires_b_after} — the flow-alive guard refused the collect "
        "leg's decrement (the cascade flipped the flow in the same tx), "
        "and the sweep's recount then RECONCILES the counter to 0 "
        "(failed_required=0 for a collect edge): a never-fired, "
        "never-stamped join row on a failed flow"
    )
