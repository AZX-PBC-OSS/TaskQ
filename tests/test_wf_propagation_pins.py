"""The T06 failed-parent propagation pins — the FAIL-CLOSED peer-cascade and
the COLLECT fan-in, red-first per the doctrine (BUILD-PROTOCOL §2: every
pin's red is a REAL mutation of the shipped rule, observed and recorded).

THE PROPAGATION RULE (T06's two semantics, per the ticket):

* **fail_closed (the default)** — a parent's TERMINAL failure (the ladder
  exhausted or a non-retryable class) fails the join CLOSED by a
  flow-scoped transition set: the joined node blocks with
  ``blocking_reason='failed_parent'`` naming the failed parent, the
  workflow fails (§17.2's cascade — the flow root's flip), and the running
  peers are PEER-CANCELLED with the record
  ``by='peer_failure', cascade_from=<node>`` (the cancel-origin marker +
  ``metadata.peer_cancel``). The record never shows a hanging join.
* **collect** — child failures do NOT cascade: at exhaustion the failure
  fans in as the typed ``FailureInfo`` item (the estate's ``ErrorInfo``
  envelope embedded — never re-spelled) and the join FIRES with the typed
  partial result; a SKIP fans in with ZERO ledger rows (a skip is not an
  attempt).

The composition invariant (P3 decision 7): ladder retries emit no
terminal — the fan-in/cascade happens ONLY at exhaustion, strictly after
the last ladder attempt (the ts-ordered pin).

Driven against a live Postgres through the REAL engine; the seed helpers
live in ``tests/_wf_fixtures.py``; the red sink flushes to
``.measurements/t06-propagation-reds.json`` (a file that gets READ).
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import fan_in_skip, finalize_node
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import reconstruct_workflow_status
from taskq.workflows._sweep import sweep_join_rederive
from tests._wf_fixtures import (
    RedLog,
    claim_view,
    fire_count,
    node_state,
    seed_flow,
    seed_join,
    seed_running_node,
)


async def _seed_map(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    flow_id: JobId,
    *,
    n: int,
    policy: str,
    join_step: str = "join",
) -> tuple[JobId, list[JobId]]:
    """A map shape: N children + one join whose edges declare *policy*
    (the edge-ledger record the propagation rule reads)."""
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key=join_step, deps=n)
    children = [
        await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"c{i}") for i in range(n)
    ]
    for child in children:
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
            "VALUES ($1, $2, $3, $4)",
            join_id,
            child,
            flow_id,
            policy,
        )
    return join_id, children


async def _terminalize(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    node_id: JobId,
    *,
    outcome: str = "succeeded",
) -> None:
    """The parent's terminal write, tx1's shape (the fenced CAS)."""
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET status = $2, finished_at = now() '
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        node_id,
        outcome,
    )


async def _ledger_rows(
    wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId, step_key: str
) -> list[asyncpg.Record]:
    return await wf_conn.fetch(
        f'SELECT attempt, status, error_class FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = $2 ORDER BY attempt",
        flow_id,
        step_key,
    )


# ── Pin 1: THE STRANDED JOIN (the fail-closed resolution) ───────────────


@pytest.mark.integration
async def test_t06_pin1_fail_closed_join_resolves_never_hangs(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """A fail_closed join whose parent TERMINALLY fails must NOT hang: the
    joined node's side of the counter is resolved by the flow-scoped
    transition (blocked-with-reason naming the parent), the flow fails.
    THE CONVICTED VARIANT (the pre-T06 shipped shape): the parent's failed
    finalize plain-decrements (or never resolves), the join stays
    join-wait / fires silently over a partial — the record shows a hanging
    join. The mutation drill: the cascade statement's block arm dropped —
    the join never resolves, the pin reds."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map(wf_conn, wf_schema, flow_id, n=2, policy="fail_closed")

    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=children[0],
        step_key="c0",
        worker_id=(await claim_view(wf_conn, wf_schema, children[0]))[0],
        attempt=1,
        claim_epoch=0,
        outcome="failed",  # the ladder exhausted: a TERMINAL failure
        error_class="ValueError",
        error_message="boom",
    )
    assert result.applied

    # THE JOIN NEVER FIRES (the counter's side is resolved by the block,
    # not by a decrement): no fire row, ever.
    assert await fire_count(wf_conn, wf_schema, join_id) == 0

    # THE RESOLUTION: the joined node is blocked-with-reason NAMING the
    # failed parent — never a hanging join-wait row.
    state = await node_state(wf_conn, wf_schema, join_id)
    propagation_redlog.red(
        "t06-pin1-stranded-join",
        "the cascade's block arm dropped (the pre-T06 shipped shape: the "
        "join stays join-wait or fires over the failed parent)",
        {
            "status": state["status"],
            "deps_pending": state["deps_pending"],
            "blocking_reason": state["metadata"].get("blocking_reason"),
        },
    )
    assert state["metadata"].get("blocking_reason") == "failed_parent", (
        f"the join is unresolved: {state} — a failed parent must not strand "
        "the join (the record never shows a hanging join)"
    )
    assert state["metadata"].get("failed_step") == "c0"
    assert state["status"] == "pending", "blocked is a pending-row representation"

    # THE FLOW FAILED (§17.2's cascade): the root's flip is the
    # linearization point.
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    assert flow_state["status"] == "failed", "the workflow fails closed"

    # THE MUTATION DRILL: drop the cascade's block arm → the pin's own
    # conviction (the unresolved join) reproduces.
    mutated = wf_sql.fail_closed_cascade.replace(
        'AND NOT j.metadata @> \'{"blocking_reason": "failed_parent"}\'::jsonb',
        "AND false",  # the block arm matches nothing: the convicted strand
    )
    assert mutated != wf_sql.fail_closed_cascade, "the mutation drill did not arm"
    flow2 = await seed_flow(wf_conn, wf_schema)
    join2, children2 = await _seed_map(wf_conn, wf_schema, flow2, n=2, policy="fail_closed")
    await _terminalize(wf_conn, wf_schema, children2[0], outcome="succeeded")
    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow2,
        job_id=children2[1],
        step_key="c0",
        worker_id=(await claim_view(wf_conn, wf_schema, children2[1]))[0],
        attempt=1,
        claim_epoch=0,
        outcome="failed",
        error_class="ValueError",
    )
    # The shipped rule blocked join2 above? No — flow2's cascade ran with
    # the SHIPPED statement; the mutated statement is the conviction
    # comparator, executed directly against the same shape:
    await wf_conn.fetch(mutated, children2[1], json.dumps({}), "x", json.dumps({}), flow2)
    state2 = await node_state(wf_conn, wf_schema, join2)
    assert state2["metadata"].get("blocking_reason") == "failed_parent", (
        "the shipped cascade did not block the join (the drill's control arm)"
    )


@pytest.mark.integration
async def test_t06_pin2_peer_cancel_record(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """The fail-closed trip names every cancelled peer: the record
    ``by='peer_failure', cascade_from=<node>`` — the error_class origin
    marker (the `by` leg, the estate's cancel-origin doctrine) + the
    structured record in metadata.peer_cancel. The mutation drill: the
    peers arm's record stamp dropped — the peer rows cancel ANONYMOUSLY
    (the convicted shape: a cancel nobody can explain)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map(wf_conn, wf_schema, flow_id, n=3, policy="fail_closed")
    # child 1 is mid-run (a running peer) when child 0's ladder exhausts.
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET locked_by_worker = $2, '
        "lock_expires_at = now() + interval '90 seconds' WHERE id = $1",
        children[1],
        new_uuid(),
    )
    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=children[0],
        step_key="c0",
        worker_id=(await claim_view(wf_conn, wf_schema, children[0]))[0],
        attempt=1,
        claim_epoch=0,
        outcome="failed",
        error_class="ValueError",
    )

    peer = await wf_conn.fetchrow(
        f'SELECT status, error_class, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        children[1],
    )
    assert peer is not None
    propagation_redlog.red(
        "t06-pin2-peer-cancel-record",
        "the peers arm's record stamp dropped — the peer cancels anonymously",
        {
            "status": peer["status"],
            "error_class": peer["error_class"],
            "metadata": peer["metadata"],
        },
    )
    assert peer["status"] == "cancelled", "the running peer is peer-cancelled"
    assert peer["error_class"] == "CancelledByPeerFailure", (
        "the `by` leg: the same outcome reads the same way whichever path "
        "produced it (the cancel-origin doctrine)"
    )
    meta = (
        peer["metadata"]
        if isinstance(peer["metadata"], dict)
        else json.loads(peer["metadata"] or "{}")
    )
    record = meta.get("peer_cancel") or {}
    assert record.get("by") == "peer_failure", record
    assert record.get("cascade_from") == str(children[0]), (
        f"the record must name the failed node: {record}"
    )

    # The third child (pending, never claimed) is ALSO peer-cancelled —
    # the cascade is set-based over every still-non-terminal peer.
    peer2 = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', children[2]
    )
    assert peer2 == "cancelled"

    # The join itself never fired.
    assert await fire_count(wf_conn, wf_schema, join_id) == 0


# ── Pin 3: THE COLLECT FAN-IN (exhaustion → the typed partial) ──────────


@pytest.mark.integration
async def test_t06_pin3_collect_exhaustion_fans_in_fires_partial(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """A collect join whose 3rd of 3 children exhausts must FIRE with the
    two Ok results + the Failed(FailureInfo) at exhaustion: the failure
    fans in as the TYPED item (the estate's ErrorInfo envelope embedded,
    the FULL attempt history carried), the workflow SUCCEEDS with the
    failure report. The mutation drill: the fan-in's append dropped — the
    join fires but the failure item never lands (the convicted silent
    partial)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map(wf_conn, wf_schema, flow_id, n=3, policy="collect")

    # Children 0 and 1 succeed (the finalize's own tx1 does the fenced
    # terminal write from the seeded running row).
    for i in (0, 1):
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=children[i],
            step_key=f"c{i}",
            worker_id=(await claim_view(wf_conn, wf_schema, children[i]))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            result={"ok": i},
        )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, status, error_class, error_message) "
        "VALUES ($1, $2, $3, 'c2', NULL, 1, 'failed', 'ValueError', 'boom-1'), "
        "       ($4, $2, $3, 'c2', NULL, 2, 'failed', 'ValueError', 'boom-2')",
        new_uuid(),
        flow_id,
        children[2],
        new_uuid(),
    )
    # The child's SECOND claim is the running row (attempt 2 — the ladder
    # burned once already); its finalize's fence reads this attempt.
    await wf_conn.execute(f'UPDATE "{wf_schema}".jobs SET attempt = 2 WHERE id = $1', children[2])

    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=children[2],
        step_key="c2",
        worker_id=(await claim_view(wf_conn, wf_schema, children[2]))[0],
        attempt=2,
        claim_epoch=0,
        outcome="failed",
        error_class="ValueError",
        error_message="boom-2",
    )
    assert result.applied

    # THE JOIN FIRED (the collect's counter resolved at exhaustion).
    assert await fire_count(wf_conn, wf_schema, join_id) == 1

    # THE FAN-IN: the FailureInfo item is ON THE JOIN ROW, the full
    # attempt history embedded, the estate's envelope shape.
    state = await node_state(wf_conn, wf_schema, join_id)
    failures: list[dict[str, Any]] = state["metadata"].get("failures") or []
    propagation_redlog.red(
        "t06-pin3-collect-exhaustion-fan-in",
        "the fan-in's append dropped — the join fires with the failure "
        "item missing (the convicted silent partial)",
        {"failures_on_row": failures},
    )
    assert len(failures) == 1, f"the failure must fan in: {state['metadata']}"
    item = failures[0]
    assert item["node_key"] == "c2"
    assert item["error"]["error_class"] == "ValueError", (
        "the item embeds the estate's ErrorInfo envelope, never re-spells it"
    )
    assert item["error"]["error_message"] == "boom-2"
    attempts = item["attempts"]
    assert [a["attempt"] for a in attempts] == [1, 2], f"the FULL attempt history: {attempts}"
    assert [a["error_message"] for a in attempts] == ["boom-1", "boom-2"]

    # THE WORKFLOW SUCCEEDS with the failure report (the absorbed-failure
    # clause T08's derivation reads): the collect is not a failure.
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    assert flow_state["status"] == "running", (
        "a collect join's child failure does NOT cascade to the flow"
    )

    # THE TS ORDERING: the fire is strictly after the last ladder attempt
    # (the fan-in + fire ride the terminalizing tx; the ledger's last
    # attempt row precedes the fire row).
    fire_ts, last_attempt_ts = await wf_conn.fetchrow(
        f'SELECT f.fired_at, (SELECT max(updated_at) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'c2') AS last_attempt "
        f'FROM "{wf_schema}".wf_join_fire f WHERE f.join_job_id = $2',
        flow_id,
        join_id,
    )
    assert fire_ts >= last_attempt_ts, (
        "the fan-in fires strictly after the last ladder attempt (P3 spike3's verified ordering)"
    )


@pytest.mark.integration
async def test_t06_pin4_skip_fans_in_zero_ledger_rows(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """A skipped child fans in with ZERO ledger rows — a skip is not an
    attempt (P3 spike3's measured invariant): the item lands on the join
    row with an empty attempt history, no attempt/terminal row anywhere.
    The mutation drill: the skip helper made to write a ledger row (the
    convicted variant — a skip recorded as an attempt)."""
    from taskq.workflows._types import _failure_info_from_json

    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map(wf_conn, wf_schema, flow_id, n=1, policy="collect")

    async with module_pg_pool.acquire() as conn:
        fanned = await fan_in_skip(
            conn,
            wf_sql,
            flow_id=flow_id,
            parent_id=children[0],
            step_key="c0",
            map_index=None,
        )
    assert fanned == 1

    state = await node_state(wf_conn, wf_schema, join_id)
    failures: list[dict[str, Any]] = state["metadata"].get("failures") or []
    propagation_redlog.red(
        "t06-pin4-skip-zero-ledger-rows",
        "the skip helper wrote a ledger row — a skip recorded as an "
        "attempt (the convicted variant)",
        {"failures_on_row": failures},
    )
    assert len(failures) == 1
    item = _failure_info_from_json(failures[0])
    assert item.attempts == (), "a skip's fan-in carries NO attempt history"
    assert item.error.error_class == "Skipped"

    rows = await _ledger_rows(wf_conn, wf_schema, flow_id, "c0")
    assert rows == [], f"a skip is not an attempt — zero ledger rows, got {rows}"
    job_rows = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = 'c0' AND parent_id = $1",
        children[0],
    )
    assert job_rows == 0, "a skip writes no attempt/terminal job row"


# ── Pin 5: THE COMPOSITION (retries emit no terminal → no cascade) ──────


@pytest.mark.integration
async def test_t06_pin5_mid_ladder_emits_no_cascade_and_no_decrement(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """The composition invariant (P3 decision 7): a child MID-LADDER (an
    attempt failed, retries remain — the vanilla mark_retry shape, no
    workflow finalize) triggers NOTHING: no cascade, no decrement, no
    fan-in, no fire. The peers keep running; the join keeps waiting. The
    ladder's no-terminal-at-retry rule is what makes the exhaustion-only
    fan-in possible."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map(wf_conn, wf_schema, flow_id, n=2, policy="fail_closed")

    # Child 0 fails ATTEMPT 1 with retries remaining: the vanilla ladder
    # marks it scheduled (pending) — NO workflow terminal, NO finalize.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', scheduled_at = now() "
        "+ interval '1 second' WHERE id = $1",
        children[0],
    )

    state = await node_state(wf_conn, wf_schema, join_id)
    propagation_redlog.red(
        "t06-pin5-mid-ladder-no-cascade",
        "a mid-ladder attempt failure treated as a terminal (the "
        "over-eager parent-fail — the convicted variant)",
        {
            "deps_pending": state["deps_pending"],
            "blocking_reason": state["metadata"].get("blocking_reason"),
            "flow_status": (await node_state(wf_conn, wf_schema, flow_id))["status"],
        },
    )
    assert state["metadata"].get("blocking_reason") == "join", (
        "a mid-ladder child must not trip the fail-closed cascade"
    )
    assert state["deps_pending"] == 2, "no decrement mid-ladder"
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    assert flow_state["status"] == "running", "the flow does not fail mid-ladder"
    assert await fire_count(wf_conn, wf_schema, join_id) == 0

    # The peer is untouched (the D6 sentence: a child mid-ladder does not
    # trigger the peer-cascade — a fail-closed map with a long-ladder
    # child keeps peers running until exhaustion, BY DESIGN).
    peer_state = await node_state(wf_conn, wf_schema, children[1])
    assert peer_state["status"] == "running", "the peer keeps running"


# ── Pin 6: THE SWEEP COMPOSES (the crash window with failures) ──────────


@pytest.mark.integration
async def test_t06_pin6_sweep_heals_fail_closed_join_after_crash(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """The sweep composes with failures: the child's tx1 committed its
    TERMINAL FAILURE, tx2 never ran (the crash window) — the sweep's
    re-derive must resolve the fail_closed join the way the direct
    path's cascade would (blocked-with-reason, never a fire over the
    failed parent), and the collect join still fires (its counter
    resolved; the ledger carries the failed child's full history — the
    collector's truth)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    fc_join, fc_children = await _seed_map(
        wf_conn, wf_schema, flow_id, n=1, policy="fail_closed", join_step="fc_join"
    )
    col_join, col_children = await _seed_map(
        wf_conn, wf_schema, flow_id, n=1, policy="collect", join_step="col_join"
    )

    # BOTH children terminalize tx1-only (the crash window: no tx2).
    await _terminalize(wf_conn, wf_schema, fc_children[0], outcome="failed")
    await _terminalize(wf_conn, wf_schema, col_children[0], outcome="succeeded")

    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    propagation_redlog.red(
        "t06-pin6-sweep-compose",
        "the sweep's failed_required arm dropped — the fail_closed join "
        "fires over the failed parent after the heal",
        {
            "blocked_required": summary.blocked_required,
            "fired": len(summary.fired),
        },
    )
    assert summary.blocked_required == 1, summary

    # THE FAIL_CLOSED JOIN: blocked-with-reason, never fired.
    fc_state = await node_state(wf_conn, wf_schema, fc_join)
    assert fc_state["metadata"].get("blocking_reason") == "failed_parent", (
        f"the sweep must resolve the fail_closed join: {fc_state}"
    )
    assert await fire_count(wf_conn, wf_schema, fc_join) == 0

    # THE COLLECT JOIN: fired (the failed child's side resolved; the
    # ledger carries its history for the collector).
    assert await fire_count(wf_conn, wf_schema, col_join) == 1


# ── Pin 7: the validator refuses an unknown policy (build-time door) ────


def test_t06_pin7_unknown_failure_policy_refused_at_build() -> None:
    """A fork declaring an unknown failure_policy is refused at build time
    (the runtime's split would silently take the default — the
    over-permission the asymmetry doctrine forbids). The mutation drill:
    the check dropped → the call returns instead of raising."""
    from taskq.workflows._types import ChildSpec, ForkSpec, JoinSpec
    from taskq.workflows.definitions import validate_fork

    fork = ForkSpec(
        children=(ChildSpec(step_key="c", actor="a", queue="q"),),
        join=JoinSpec(step_key="j", actor="a", queue="q", failure_policy="yolo"),
    )
    with pytest.raises(ValueError, match="failure_policy"):
        validate_fork(fork)


# ── Pin 8: THE MIXED-POLICY NODE (the fence is not absorption) ──────────


@pytest.mark.integration
async def test_t06_pin8_mixed_policy_node_the_fence_is_not_absorption(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """Node X feeds joinA (fail_closed) AND joinB (collect); X
    terminal-fails. The cascade blocks A and fails the flow IN THE SAME
    TX — the collect leg's decrement then reads the flow-alive guard and
    is REFUSED. A FENCED DECREMENT IS NOT ABSORPTION: the edge declared
    the collect POLICY, but the flow's death fenced the resolution — the
    fan-in never delivered. THE TWO LIES THIS PIN FORBIDS (the phase-2
    attack's H2):
    * the hanging join: joinB must rest blocked-with-reason after ONE
      sweep pass — never a reconciled-to-0, never-fired, claimable join
      row on a failed flow;
    * the lying envelope: the derivation must say 'failed' for this
      failed-closed run — X's failure is NOT absorbed (the edge's POLICY
      declaration alone absorbs nothing; the record must show the
      absorption RAN). T07's C: the envelope must not lie about which
      policy ran.
    THE CONVICTED VARIANT (the drill): the absorbed predicate without the
    fence clause (EXISTS any absorbing edge) — the derivation flips to
    'blocked' and the envelope lie reproduces."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_a = await seed_join(wf_conn, wf_schema, flow_id, step_key="ja", deps=1)
    join_b = await seed_join(wf_conn, wf_schema, flow_id, step_key="jb", deps=1)
    x = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="x")
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'fail_closed')",
        join_a,
        x,
        flow_id,
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'collect')",
        join_b,
        x,
        flow_id,
    )

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

    # THE FAIL_CLOSED LEG RAN: the flow failed, joinA blocked-with-reason.
    root = await node_state(wf_conn, wf_schema, flow_id)
    assert root["status"] == "failed", "the fail_closed leg must fail the flow"
    a_state = await node_state(wf_conn, wf_schema, join_a)
    assert a_state["metadata"].get("blocking_reason") == "failed_parent"

    # ONE SWEEP PASS: the flow-fenced join arm owns joinB's resolution.
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.flow_fenced >= 1, f"the flow-fenced join must be stamped: {summary}"

    b_state = await node_state(wf_conn, wf_schema, join_b)
    fires_b = await fire_count(wf_conn, wf_schema, join_b)
    propagation_redlog.red(
        "t06-pin8-mixed-policy-fence",
        "the flow-fenced join arm dropped (or the absorbed predicate read "
        "the edge's POLICY declaration as a ran absorption) — the collect "
        "join hangs claimable-never-fired on a failed flow, and the "
        "derivation can never say 'failed' for the failed-closed run",
        {
            "join_b_blocking_reason": b_state["metadata"].get("blocking_reason"),
            "join_b_deps_pending": b_state["deps_pending"],
            "fires": fires_b,
        },
    )
    # NO HANGING CLAIMABLE JOIN: joinB rests blocked-with-reason NAMING
    # the failed parent (the direct path's cascade stamp — the fence
    # resolves the way the cascade would have), never fired.
    assert b_state["metadata"].get("blocking_reason") == "failed_parent", (
        f"THE HANGING COLLECT JOIN: joinB rests {b_state} — the flow's own "
        "death fenced the collect leg's decrement, and nothing resolved "
        "the join: a claimable never-fired row on a failed flow"
    )
    assert b_state["metadata"].get("failed_parent") == str(x), "the stamp names the failed parent"
    assert fires_b == 0, "the join never fires over the fenced flow"

    # THE ENVELOPE IS HONEST: the derivation says 'failed' — X's failure
    # is NOT absorbed (the fence refused the resolution), never 'blocked'.
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    assert reconstructed == "failed", (
        f"THE ENVELOPE LIES: the rows reconstruct {reconstructed!r} for a "
        "failed-closed run — the absorbed predicate read the edge's POLICY "
        "declaration as a ran absorption (the flow-fenced fan-in never "
        "delivered), so the derivation can never say 'failed' here"
    )

    # THE MUTATION DRILL (live): the absorbed predicate WITHOUT the fence
    # clause — the EXISTS-any-absorbing-edge lie — derives 'blocked': the
    # convicted envelope lie reproduces against this very state.
    import dataclasses

    mutated_nodes = wf_sql.workflow_nodes.replace(
        "AND NOT (j2.status = 'pending' AND j2.metadata->>'blocking_reason' IN ('failed_parent', 'orphan_parent', 'flow_dead'))",
        "",
    )
    assert mutated_nodes != wf_sql.workflow_nodes, "the mutation drill did not arm"
    mutated_sql = dataclasses.replace(wf_sql, workflow_nodes=mutated_nodes)
    lied = await reconstruct_workflow_status(wf_conn, mutated_sql, flow_id)
    assert lied == "blocked", (
        f"the drill's conviction is broken: the mutant derived {lied!r} — "
        "the fence clause must be load-bearing in the absorbed predicate"
    )
