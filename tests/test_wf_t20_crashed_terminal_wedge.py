"""THE CRASHED-TERMINAL WEDGE (T20/T21 fixer's cure pins) — the round's
most severe conviction: a run whose nodes = {succeeded|failed, crashed}
WEDGED RUNNING FOREVER.

THE SEMANTICS DECISION (the design law, stated once here and in
``taskq.workflows._status``): a jobs row in status ``crashed`` is
TERMINAL-FOR-REAL. The state machine's own totality table already said so
(``statemachine.VALID_TRANSITIONS['crashed'] == frozenset()`` — zero
outbound transitions); the derivation's row-1 comment claimed "a
crashed/abandoned node is the reclaim's input — the run is still live",
and that premise was FALSE: the reclaim arms' input is status ``running``
ONLY. A row reaches ``crashed`` through the reclaim's crashed branch —
the attempt budget exhausted (``{has_budget}`` false) — or
``mark_abandoned`` (the rolling-deploy record). NOTHING will ever revive
either: the row's OWN state decides (the budget's death is
deterministic). The REAL crash recovery is the reclaim of a RUNNING row
whose holder died with budget remaining — that row re-pends and never
carries ``crashed``. The asymmetry doctrine: a wedge (the flow never
terminal) is the state an operator must notice FOREVER; an honest
terminal is the runbook's resumable-by-rerun. A corpse that reports
``running`` is the worst of both: it wedges the root (the maintenance
leg's liveness predicates held it forever), holds retention (the prune
touches terminal rows only), and greened G7 on a dead run.

THE CURES PINNED HERE:
* the derivation folds the terminal-crash class into the failed-class
  terminal (the corpse derives ``failed`` — never ``running``);
* the maintenance leg's liveness predicates exclude the terminal-crash
  class and its failed arm INCLUDES it (the wedged root heals on ONE
  sweep pass);
* the G7 assertion can RED a wedge: a live root whose rows reconstruct
  a TERMINAL verdict contradicts the rows — the assertion runs the
  one-pass heal itself and reds when the reported state still disagrees
  (the corpse-stays-green blind spot, closed).

Red-first: the corpse pins ran RED against the pre-cure tree
(reconstruction derived ``running``; the maintenance pass left the root
``running``) — captured in ``.measurements/wedge-pin-reds.json``.
"""

from __future__ import annotations

import dataclasses
import json

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import reconstruct_workflow_status
from tests._wf_fixtures import RedLog, g7_check, seed_flow, seed_running_node


async def seed_crashed_node(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    step_key: str = "screen",
) -> JobId:
    """The budget-exhausted crash terminal (the reclaim's crashed branch
    product): attempt == max_attempts (the ladder burned), finished_at
    stamped, the worker-crash error class on the row."""
    node_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, map_index, metadata, "
        "error_class, error_message, finished_at, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'crashed', 3, $2, 0, "
        "$3::jsonb, 'WorkerCrashedError', 'the lease expired with the retry "
        "budget exhausted', now(), 'workflow', $4)",
        node_id,
        step_key,
        json.dumps({"flow_id": str(flow_id)}),
        f"wf:{flow_id}:emit:0:{step_key}",
    )
    return JobId(node_id)


async def root_row(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> dict[str, object]:
    rec = await conn.fetchrow(
        f'SELECT status, finished_at, error_class FROM "{schema}".jobs WHERE id = $1',
        flow_id,
    )
    assert rec is not None
    return dict(rec)


async def run_maintenance(conn: asyncpg.Connection, wf_sql: WorkflowSql) -> int:
    """One maintenance pass (the sweep's own call shape)."""
    rows = await conn.fetchval(wf_sql.workflow_root_maintain, 200)
    return int(rows or 0)


@pytest.mark.integration
async def test_the_wedged_corpse_derives_terminal(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    wedge_redlog: RedLog,
) -> None:
    """THE WEDGE'S DERIVATION FACE: {succeeded, crashed} derives
    ``failed`` — the terminal-crash class is the failed-class terminal,
    never liveness. RED (the pre-cure tree): the corpse derived
    ``running`` — G7 green on a dead run, retention held forever."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    ok = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="enrich")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
        ok,
    )
    await seed_crashed_node(wf_conn, wf_schema, flow_id)

    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    wedge_redlog.red(
        "t20-wedged-corpse-derivation",
        "the derivation's row-1 liveness (running/crashed/abandoned) reading "
        "the terminal-crash class: the corpse derives 'running' forever — "
        "G7 green on a dead run, retention held forever",
        {"reconstructed": reconstructed},
    )
    assert reconstructed == "failed", (
        f"THE CRASHED-TERMINAL WEDGE: a run whose only live-class row is a "
        f"budget-exhausted crash terminal reconstructed {reconstructed!r} — "
        "the corpse derives 'running' forever: nothing reclaims a crashed "
        "row (the state machine gives it zero outbound transitions), so "
        "the liveness predicate is the wedge"
    )


@pytest.mark.integration
async def test_the_wedged_root_heals_on_one_sweep_pass(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    wedge_redlog: RedLog,
) -> None:
    """THE EVIDENCE ROOTS' HEAL (the att_t20 corpses' cure): the wedged
    root + one maintenance pass → the failed terminal, finished_at
    stamped, retention eligible. RED (the pre-cure tree): the pass left
    the root ``running`` forever."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    ok = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="enrich")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
        ok,
    )
    await seed_crashed_node(wf_conn, wf_schema, flow_id)

    before = await root_row(wf_conn, wf_schema, flow_id)
    assert before["status"] == "running", "the corpse starts wedged (the evidence shape)"

    await run_maintenance(wf_conn, wf_sql)
    after = await root_row(wf_conn, wf_schema, flow_id)
    wedge_redlog.red(
        "t20-wedged-root-one-pass-heal",
        "the liveness predicates (has_unresolved/has_live) counting the "
        "terminal-crash class: the maintenance pass leaves the corpse root "
        "'running' forever (the att_t20 evidence roots' shape)",
        {"root_status_after_one_pass": after["status"]},
    )
    assert after["status"] == "failed", (
        f"THE WEDGE: one maintenance pass over the corpse root left it "
        f"{after['status']!r} — the terminal-crash class must count in the "
        "failed arm (the run's death is deterministic), never in liveness"
    )
    assert after["finished_at"] is not None, "a terminal root has its finish stamp"
    assert after["error_class"] == "UnabsorbedNodeFailure", (
        "the maintenance stamp names the reason (the operator's why)"
    )

    # THE RECONSTRUCTION AGREES (G7's two faces cannot drift): the healed
    # root's status is what the rows alone derive.
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    assert reconstructed == "failed"


@pytest.mark.integration
async def test_g7_reds_a_wedge_the_corpse_stays_green_blind_spot(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    wedge_redlog: RedLog,
) -> None:
    """THE G7 RED-DRILL (the attacker's proof): G7 stayed green ON THE
    CORPSE — the anti-drift assertion could not fail on a wedged run.
    THE CURE'S TEETH: a live root whose rows reconstruct a TERMINAL
    verdict contradicts the rows; the assertion runs the one-pass heal
    and REDS when the reported state still disagrees. The drill disables
    the heal (the maintenance statement's no-op mutant — the wedging
    sweep's shape) and the check MUST red."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    ok = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="enrich")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
        ok,
    )
    await seed_crashed_node(wf_conn, wf_schema, flow_id)
    # the root reports 'running' over a TERMINAL reconstruction — the wedge.

    # THE MUTANT: a maintenance statement that maintains nothing (the
    # wedging sweep's own shape — the pass runs, maintains nothing).
    noop_sql = dataclasses.replace(
        wf_sql,
        # Consumes $1 (the batch arg) — the pass runs, maintains nothing.
        workflow_root_maintain="SELECT 0::int WHERE $1::int IS NOT NULL",
    )
    with pytest.raises(AssertionError) as drill:
        await g7_check(wf_conn, wf_schema, noop_sql)
    assert "WEDGE" in str(drill.value), (
        f"the red-drill's failure is not the wedge leg naming itself: {drill.value}"
    )
    root_after = await root_row(wf_conn, wf_schema, flow_id)
    wedge_redlog.red(
        "g7-corpses-cannot-hide",
        "the G7 assertion WITHOUT the terminal-contradiction leg: a live "
        "root over a terminal reconstruction greens the dead run (the "
        "attacker's proof: G7 stayed green on the corpse) — the drill "
        "ran the mutant check and read the corpse after it",
        {
            "raised": str(drill.value)[:300],
            "root_status_after_the_mutant_pass": root_after["status"],
            "reconstructed": await reconstruct_workflow_status(wf_conn, wf_sql, flow_id),
        },
    )

    # THE SHIPPED SHAPE GREENS: the same corpse through the REAL check —
    # the heal runs, the root terminals, the assertion holds.
    await g7_check(wf_conn, wf_schema, wf_sql)


@pytest.mark.integration
async def test_the_reclaim_input_row_is_still_live(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """THE ASYMMETRY'S OTHER HALF (the user's ruling): a crashed WORKER's
    row is reclaim-eligible — the REAL crash recovery. The reclaim's input
    is the RUNNING row: a row whose holder died WITH budget remaining
    re-pends and the run stays live (the derivation's liveness reads
    THAT row's 'running', and the ledger's attempt-level 'crashed' is the
    reclaim's receipt, not a terminal). The ledger-crashed attempt must
    NOT terminalize the run."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    holder = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="screen")
    # the holder's worker died: the ledger recorded the attempt-level
    # 'crashed' (the reclaim's receipt) — the ROW re-pends scheduled.
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, status) "
        "VALUES ($3, $1, $2, 'screen', 0, 1, 'crashed')",
        flow_id,
        holder,
        new_uuid(),
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', locked_by_worker = NULL, "
        "lock_expires_at = NULL, scheduled_at = now() WHERE id = $1",
        holder,
    )

    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    assert reconstructed in ("running", "pending"), (
        f"the reclaim-eligible row (status 'scheduled', the ledger's "
        f"attempt-level crash as its receipt) reconstructed {reconstructed!r} "
        "— the REAL crash recovery must stay live: the reclaim re-runs it"
    )


# ── THE ZOMBIE AUDIT'S STRUCTURAL PIN ───────────────────────────────────


def test_the_liveness_predicates_exclude_the_terminal_crash_class() -> None:
    """THE ZOMBIE AUDIT'S FENCE (structural): the maintenance leg's
    liveness sets spell the terminal-crash class OUT and the failed arm
    spells it IN — the same class re-derived against the state machine's
    totality table (crashed/abandoned: zero outbound transitions). A
    future edit that re-adds crashed/abandoned to a liveness set reds
    here (the predicate that predates T20 and was never re-attacked, now
    attacked permanently)."""
    from taskq.workflows._sql_status import WORKFLOW_ROOT_MAINTAIN_SQL

    # The liveness sets: running (+ the unresolved pending/scheduled leg)
    # — crashed/abandoned are terminals, never liveness.
    assert "bool_or(n.status = 'running'" in WORKFLOW_ROOT_MAINTAIN_SQL
    assert "'running', 'crashed'" not in WORKFLOW_ROOT_MAINTAIN_SQL
    assert "'running','crashed'" not in WORKFLOW_ROOT_MAINTAIN_SQL
    assert "IN ('pending', 'running', 'scheduled', 'crashed'" not in WORKFLOW_ROOT_MAINTAIN_SQL
    # The failed arm: the terminal-crash class IS the failed class.
    assert "n.status IN ('failed', 'crashed', 'abandoned')" in WORKFLOW_ROOT_MAINTAIN_SQL
