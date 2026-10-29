# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""ATTACK PINS — the rv6 round's ENGINE pack (the pushed head 0e2f5cc09),
PORTED TO the engine-cures lane and FLIPPED GREEN by its cures.

Provenance: the rv6 front's live convictions, each reproduced against the
pinned head BEFORE pinning. Per the doctrine, a LIVE finding is pinned
under ``pytest.mark.xfail(strict=True, reason="LIVE FINDING rv6-e<id>: …")``
asserting the SAFE behavior — the cure flips the pin to XPASS-strict (a
red that says: remove the marker WITH the cure). THE FLIP IS DONE: both
pins ran xfail-strict at the base (the red receipts are in this pack's
RECEIPTS file and the lane's captures) and the cures
(``_sql_runner.py``'s terminal-flow fence on BOTH claim seats;
``_hitl.py``'s hold-stamp-addressed wake) flipped them — the markers are
REMOVED WITH the cure, the assertions stand as the permanent green pins.

The findings (each convicted live):

* F-E1 (SEVERE) — THE IN-PROCESS POST-TERMINAL EXECUTION HOLE: the
  in-process driver's claim path (``CLAIMABLE_NODES_SQL_TEMPLATE`` /
  ``NODE_CLAIM_SQL_TEMPLATE``, ``workflows/api/_sql_runner.py``) carries
  NO flow-status leg; only ``drive()``'s loop-top check covers it. A
  claimable child staged on a CANCELLED flow (the drain's documented
  exemption window: the join fire won, the drain landed after the
  cascade) is claimed and EXECUTED by a bare ``FlowRunner.tick()`` —
  observed: the body ran, the row finalized ``'succeeded'`` on the
  cancelled root. The fleet's dispatch SQL carries the fence
  (``backend/_dispatch_sql.py``'s ``_WF_DISPATCH_FENCE_TEMPLATE``: "a
  workflow row is claimable … never on a TERMINAL flow"); the in-process
  door doesn't. THE PIN: the in-process claim carries the same
  terminal-flow fence — a body never executes on a terminal flow through
  ANY door.
* F-E2 — THE HOLD→NODE WAKE'S MISSING MAP-INDEX LEG (the holdmap lane's
  resume-addressing hole, pinned at THIS head — the map_index cure
  (1528719d2) is NOT an ancestor of this tree; the shape pinned is the
  one this tree ships): the deliver path's wake
  (``workflows/api/_hitl.py``'s ``deliver_payload``) resolves the held
  node by ``step_key + flow + metadata ? 'hold'`` — NO map_index leg, NO
  ORDER BY — so with several map siblings held (one step_key, distinct
  signals), resolving the MIDDLE sibling's hold wakes the ARBITRARY
  first-matching row (observed: sibling 0's hold marker cleared while the
  ANSWERED sibling 1 stays held — the delivered payload's own node never
  resumes; a wrong sibling re-runs). THE PIN: a middle-out resolve wakes
  the CORRECT sibling — the resume addresses the hold's OWN node (the
  hold id is already stamped in the node's ``metadata->>'hold'``; the
  map_index-addressed cure's semantic), never a sibling's.
"""

from __future__ import annotations

import json

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.api._hitl import deliver_payload
from tests._wf_fixtures import seed_flow

#: THE EXECUTION SENTINEL (F-E1): the staged body appends here when it
#: runs. Module-level (the in-process driver executes in THIS process);
#: cleared at the pin's start — the conviction is "the body NEVER ran".
_E1_SENTINEL: list[str] = []


# ── the pinned graph's bodies (module-level: the annotations must resolve —
#    the compile's hint resolution + the §22.1 stamper read the body's own
#    SOURCE, which a function-scope body does not reproducibly carry). ──


async def _e1_child_body(ctx: StepContext) -> str:
    """The born-on-cancelled child: if this EVER runs, the post-terminal
    execution hole is live (the sentinel is the conviction)."""
    _E1_SENTINEL.append("e1-body-ran")
    return "done"


# ── F-E1: the in-process post-terminal execution hole ────────────────────


@pytest.mark.integration
async def test_e1_the_in_process_claim_never_executes_on_a_terminal_flow(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE FENCE IS THE CLAIM'S, not the loop's: a bare ``tick()`` (the
    driver's public dispatch pass — the sweep-cron and the embedders'
    seam, never gated by ``drive()``'s loop-top check) on a flow whose
    root is CANCELLED must claim NOTHING and execute NOTHING. The staged
    shape is the drain's documented exemption window: the child row born
    (or still claimable) when the cascade landed — ``pending``,
    ``deps_pending = 0``, no hold, on the cancelled root. The cure may
    sit in the claimable SELECT or the claim CAS (the fleet's fence
    carries both seats' semantics); the pin reads the OUTCOME — the body
    never ran, the row never finalized executed-succeeded."""
    _E1_SENTINEL.clear()
    app = WorkflowApp()

    @app.workflow("r6e1_flow")
    def e1_flow() -> Promise[object]:
        return build(step(_e1_child_body, key="only"))

    runner = FlowRunner(app.get("r6e1_flow"), wf_pool, wf_schema)
    flow_id = await seed_flow(wf_conn, wf_schema, status="cancelled")
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'pending', 'only', 0, $2::jsonb)",
        new_uuid(),
        json.dumps({"flow_id": str(flow_id)}),
    )
    await runner.tick(flow_id)
    assert _E1_SENTINEL == [], (
        "F-E1: the body EXECUTED on the cancelled flow — the in-process claim "
        f"carries no terminal-flow fence (the fleet's dispatch SQL has it). "
        f"Sentinel: {_E1_SENTINEL!r}"
    )
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'only'",
        flow_id,
    )
    assert row is not None
    assert row["status"] != "succeeded" and row["attempt"] == 0, (
        f"F-E1: the child on the cancelled flow finalized {row['status']!r} "
        f"(attempt {row['attempt']}) — a post-terminal execution through the "
        "in-process door. The fenced claim leaves the row pending (a refused "
        "claim is re-derivable; a claimed one is not un-runnable — the "
        "dispatch fence's own asymmetry doctrine)"
    )


# ── F-E2: the hold→node wake's missing map_index leg ─────────────────────

#: The staged hold's declared contract (the cold-process boundary's
#: structural fit — no wait site ran in this process, so
#: ``payload_schema`` IS the boundary's read).
_E2_HOLD_SCHEMA = {
    "Approval": {
        "type": "object",
        "properties": {"verdict": {"type": "string"}},
        "required": ["verdict"],
    }
}


@pytest.mark.integration
async def test_e2_a_middle_out_resolve_wakes_the_holds_own_node(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """ONE HOLD, ONE NODE, ONE RESUME: three map siblings (one step_key
    ``work.item``, map_index 0/1/2, each held on its OWN signal — the
    pre-cure hold identity ``(workflow_id, node_key, signal_name,
    hold_epoch)`` admits the distinct names). Resolving the MIDDLE
    sibling's hold must resume THE HOLD'S OWN NODE — the row whose
    ``metadata->>'hold'`` IS the resolved hold's id (the stamp
    register_hold writes; the map_index-addressed cure's semantic) — and
    must leave the other siblings' held representations intact. The
    siblings are inserted in order so the arbitrary first-match wake
    lands on sibling 0: the red is deterministic."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="running")
    hold_ids: list[JobId] = []
    node_by_hold: dict[str, str] = {}
    for i in range(3):
        hold_id = new_uuid()
        node_id = new_uuid()
        hold_ids.append(JobId(hold_id))
        node_by_hold[str(hold_id)] = str(node_id)
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, deps_pending, map_index, metadata) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'pending', "
            "'work.item', 0, $2, $3::jsonb)",
            node_id,
            i,
            json.dumps(
                {
                    "flow_id": str(flow_id),
                    "hold": str(hold_id),
                    "held_signal": f"sig-{i}",
                }
            ),
        )
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_signals (id, workflow_id, node_key, '
            "signal_name, hold_epoch, call_id, payload_schema, status) "
            "VALUES ($1, $2, 'work.item', $3, 0, $4, $5::jsonb, 'held')",
            hold_id,
            flow_id,
            f"sig-{i}",
            f"call-{i}",
            json.dumps(_E2_HOLD_SCHEMA),
        )
    # THE MIDDLE-OUT RESOLVE: sibling 1's hold answered first.
    result = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=hold_ids[1],
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert result.status == "delivered", (
        f"the staging must hold: the middle hold's delivery was {result.status!r} "
        f"({result.reason!r}) — the conviction is the WAKE leg, not the CAS"
    )
    rows = await wf_conn.fetch(
        f"SELECT id, map_index, metadata ? 'hold' AS still_held FROM \"{wf_schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'work.item' ORDER BY map_index",
        flow_id,
    )
    resumed = {str(r["id"]) for r in rows if not r["still_held"]}
    own_node = node_by_hold[str(hold_ids[1])]
    assert resumed == {own_node}, (
        "F-E2: the middle-out resolve woke the WRONG sibling — the wake matches "
        f"step_key + flow + hold-marker with no map_index/ORDER BY leg. The "
        f"resolved hold {str(hold_ids[1])[:12]}… owns node {own_node[:12]}… "
        f"(map_index=1); the resumed set is "
        f"{sorted(n[:12] for n in resumed)} — the answered sibling's node stays "
        "held (its delivered payload unconsumable by its own wait site) while "
        "a sibling that was never answered re-runs"
    )
    still_held_count = sum(1 for r in rows if r["still_held"])
    assert still_held_count == 2, (
        f"F-E2: exactly the two UNANSWERED siblings stay held; observed "
        f"{still_held_count} — the resolve's wake disturbed more than its own node"
    )
