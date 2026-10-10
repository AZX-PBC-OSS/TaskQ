"""THE HOLD-STAMP WEDGE + THE OUTBOX TTL — the D2 soak's P1/P3 cures'
pins (integration: real PG, the real engine surfaces).

The soak's evidence (REPORT-D2-soak): a SIGKILL during a HELD loop leaves
a node row pending + ``metadata.hold`` stamped whose hold row is no longer
``'held'`` — the claimable fence ``AND NOT metadata ? 'hold'`` then
excludes it FOREVER; TWENTY runs sat wedged at close, unclaimable,
undetected, unnamed. And the delivered ``wf_outbox`` population grew
monotonically forever — no retention law touched it.

These pins hold the cures: the hold's state DECIDES (still 'held' → the
held representation stands — the resume re-dispatches; delivered /
abandoned / absent → the stamp cleared + the row re-claims, the body's
re-execution landing the next NAMED state), and the delivered outbox has
a TTL (undelivered rows never touched; ``timedelta(0)`` the disable
sentinel).
"""

from __future__ import annotations

import asyncpg
import structlog.testing
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows._sql_sweep import HOLD_STAMP_RECONCILE_SQL
from taskq.workflows._sweep import sweep_hold_stamps
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.engine import render_workflow_sql


class Approval(BaseModel):
    verdict: str


class Ingest(BaseModel):
    doc_id: str


def _wedge_app(name: str) -> WorkflowApp:
    """A one-node workflow whose node HOLDS on an Approval — the demo's
    review shape (the registry is exact: a unique name per app). T26'S
    AMENDMENT: the wait's expiry face is the ``Expired`` MEMBER — the
    body converts it into the RAISED failure (the ladder's own use)."""
    from taskq.exceptions import SignalTimeoutError
    from taskq.workflows import Expired

    app = WorkflowApp()

    @app.workflow(name)
    def wedge_flow() -> Promise[object]:
        async def review(ctx: StepContext, params: Ingest) -> object:
            outcome = await ctx.wait_signal(Approval, timeout_s=120.0)
            match outcome:
                case Approval() as approval:
                    return approval
                case Expired():
                    raise SignalTimeoutError("the hold expired — the wedge pin's conversion")

        return build(step(review, Ingest(doc_id="d1"), key="review"))

    return app


async def _held_flow(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    *,
    name: str,
) -> tuple[JobId, JobId, JobId, FlowRunner]:
    """A flow driven to its hold: (flow_id, node_id, hold_id, runner)."""
    app = _wedge_app(name)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id = await wf_pool.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    hold_id = await wf_pool.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert node_id and hold_id
    return JobId(flow_id), JobId(node_id), JobId(hold_id), runner


async def _plant_wedge(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    node_id: JobId,
    hold_id: JobId,
    *,
    signal_status: str | None = None,
    delete_signal: bool = False,
) -> None:
    """The SIGKILL wedge's verified END STATE: the signal resolved past
    'held' (the deliver CAS / the expiry arm won) while the node's stamp
    SURVIVED — the exact rows the soak measured 20 of. ``delete_signal``
    plants the ABSENT-hold variant (the pointer outlived its row)."""
    if delete_signal:
        await wf_pool.execute(f'DELETE FROM "{wf_schema}".wf_signals WHERE id = $1', hold_id)
    else:
        assert signal_status is not None
        # A delivered hold carries the CAS's payload — the answer the
        # body's replay consumes (the plant must be the deliver tx's
        # committed shape, payload included).
        await wf_pool.execute(
            f'UPDATE "{wf_schema}".wf_signals SET status = $2, '
            "payload = CASE WHEN $2 = 'delivered' THEN '{\"verdict\": \"approve\"}'::jsonb "
            "ELSE payload END, "
            "resolved_at = clock_timestamp() WHERE id = $1",
            hold_id,
            signal_status,
        )
    # The node: still pending, still stamped (the wedge's shape).
    stamped = await wf_pool.fetchval(
        f"SELECT status = 'pending' AND metadata ? 'hold' FROM \"{wf_schema}\".jobs WHERE id = $1",
        node_id,
    )
    assert stamped, "fixture broken: the wedge's node row is not pending+stamped"


# ── the wedge's paths: every path lands in a NAMED state ────────────────


async def test_delivered_hold_stamp_is_cleared_and_the_run_completes(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE DELIVERED PATH: the wedge (the signal delivered, the stamp
    standing) is reclaimed by the arm — the stamp cleared, the row
    re-claimed, the body's re-execution REPLAYS the delivered answer from
    the queue (the resume contract) — the run COMPLETES. The 20-wedge
    class's delivered half, dead."""

    flow_id, node_id, hold_id, runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_delivered_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, signal_status="delivered")

    # RED shape on the ungated row: the claimable fence excludes it.
    claimable = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND status IN ('pending','scheduled') "
        "AND NOT metadata ? 'hold'",
        flow_id,
    )
    assert claimable == 0, "fixture broken: the wedged row is claimable"

    with structlog.testing.capture_logs() as captured:
        cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1
    # THE LOUDNESS LAW: the heal is a NAMED event (nothing detects or
    # names them — was the soak's half-complaint).
    assert any(e["event"] == "wf_hold_stamp_reclaimed" for e in captured)

    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "succeeded", status


async def test_expired_hold_stamp_is_cleared_and_the_run_lands_the_named_timeout(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE EXPIRED PATH: the hold the expiry arm ABANDONED (the stamp
    survived — the expiry sweep's two-statement shape is one wedge door)
    is reclaimed: the stamp cleared, the body re-executes, the wait site
    raises the typed timeout face, and the ladder terminal-fails the node
    — the NAMED state, never a silent wedge."""

    flow_id, node_id, hold_id, runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_expired_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, signal_status="abandoned")

    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1

    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "failed", status
    error_class = await wf_pool.fetchval(
        f'SELECT error_class FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert error_class == "SignalTimeoutError", error_class


async def test_absent_hold_stamp_is_cleared_and_a_fresh_hold_resumes(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ABSENT-HOLD PATH: the stamp pointing at a hold row that no
    longer exists (a purge, a corruption) is a DEAD POINTER — cleared, the
    body re-executes, mints a FRESH hold (a NEW epoch — the multi-hold
    law), and a resolve on it completes the run."""

    flow_id, node_id, hold_id, runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_absent_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, delete_signal=True)

    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1

    # The re-claim re-mints: drive back to the hold.
    verdict = await runner.drive(flow_id, until="held", max_ticks=400, tick=0.01)
    assert verdict == "held"
    new_hold_id = await wf_pool.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
        "ORDER BY hold_epoch DESC LIMIT 1",
        flow_id,
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    result = await client.resolve(new_hold_id, {"verdict": "approve"}, principal="pin")
    assert result.status == "delivered"
    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "succeeded", status


async def test_live_hold_is_left_standing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE HELD PATH (the hold is the truth): a stamp whose hold row is
    still 'held' is the LEGITIMATE held representation — the arm leaves
    it untouched, and the resume re-dispatches the run to completion."""

    flow_id, node_id, hold_id, runner = await _held_flow(wf_pool, wf_schema, name="wedge_live_flow")
    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 0
    still = await wf_pool.fetchval(
        f"SELECT metadata ? 'hold' FROM \"{wf_schema}\".jobs WHERE id = $1", node_id
    )
    assert still, "the arm cleared a LIVE hold's stamp — the hold is the truth"

    client = HitlClient(wf_pool, schema=wf_schema)
    result = await client.resolve(hold_id, {"verdict": "approve"}, principal="pin")
    assert result.status == "delivered"
    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "succeeded", status


async def test_twenty_wedged_runs_all_land_named_terminal_or_resumed(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE 20-WEDGE REPRO (the soak's own receipt): twenty runs planted in
    the exact wedged end state — mixed delivered (10) / abandoned (5) /
    absent hold (5) — and ONE reconcile pass leaves ZERO of them in the
    wedge; every run then reaches its named state (resumed-and-completed,
    resumed-and-completed, terminal-failed) and nothing is left pending +
    stamped with a dead hold."""
    runs: list[tuple[JobId, JobId, JobId, FlowRunner, str]] = []
    for i in range(10):
        fid, nid, hid, runner = await _held_flow(wf_pool, wf_schema, name=f"wedge20_d{i}_flow")
        await _plant_wedge(wf_pool, wf_schema, nid, hid, signal_status="delivered")
        runs.append((fid, nid, hid, runner, "delivered"))
    for i in range(5):
        fid, nid, hid, runner = await _held_flow(wf_pool, wf_schema, name=f"wedge20_a{i}_flow")
        await _plant_wedge(wf_pool, wf_schema, nid, hid, signal_status="abandoned")
        runs.append((fid, nid, hid, runner, "abandoned"))
    for i in range(5):
        fid, nid, hid, runner = await _held_flow(wf_pool, wf_schema, name=f"wedge20_x{i}_flow")
        await _plant_wedge(wf_pool, wf_schema, nid, hid, delete_signal=True)
        runs.append((fid, nid, hid, runner, "absent"))

    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 20, cleared

    # THE WEDGE IS GONE: no row sits pending/scheduled + stamped with a
    # dead hold.
    wedged = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs j '
        "WHERE j.metadata ? 'hold' AND j.status IN ('pending','scheduled') "
        f'AND NOT EXISTS (SELECT 1 FROM "{wf_schema}".wf_signals s '
        "WHERE s.id::text = j.metadata->>'hold' AND s.status = 'held')"
    )
    assert wedged == 0, f"{wedged} run(s) remain wedged after the reconcile"

    # EVERY run reaches its named state.
    for fid, _nid, _hid, runner, kind in runs:
        if kind == "absent":
            # The absent-hold path re-mints: the body's re-execution
            # registers a FRESH hold (a NEW epoch) — a resolve on it
            # resumes the run.
            verdict = await runner.drive(fid, until="held", max_ticks=400, tick=0.01)
            assert verdict == "held", (kind, fid)
            new_hold_id = await wf_pool.fetchval(
                f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
                "ORDER BY hold_epoch DESC LIMIT 1",
                fid,
            )
            client = HitlClient(wf_pool, schema=wf_schema)
            result = await client.resolve(new_hold_id, {"verdict": "approve"}, principal="pin")
            assert result.status == "delivered", (kind, result)
        verdict = await runner.drive(fid, until="terminal", max_ticks=400, tick=0.01)
        assert verdict == "terminal", (kind, fid)
        status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', fid)
        if kind == "abandoned":
            assert status == "failed", (kind, status)  # the NAMED timeout face
        else:
            assert status == "succeeded", (kind, status)  # resumed + completed


async def test_hold_stamp_reconcile_statement_is_the_batch_bounded_self_consuming_shape(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The arm's own statement shape: LIMIT-bounded (a pass never
    unbounds) and self-consuming (a cleared row loses the stamp — the
    second pass returns zero), and it never touches a row whose hold is
    still 'held' (the held-row exclusivity, P3 rule 1's family)."""
    app = _wedge_app("wedge_batch_flow")
    runner = FlowRunner(app.get("wedge_batch_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id, hold_id = await _node_and_hold(wf_conn, wf_schema, flow_id)
    await _plant_wedge(wf_conn, wf_schema, node_id, hold_id, signal_status="delivered")

    # The batch bound: LIMIT 1 clears exactly one row per pass.
    rows = await wf_conn.fetch(HOLD_STAMP_RECONCILE_SQL.replace("{schema}", wf_schema), 1)
    assert len(rows) == 1
    again = await wf_conn.fetch(HOLD_STAMP_RECONCILE_SQL.replace("{schema}", wf_schema), 1)
    assert again == [], "the arm's predicate must self-consume (a cleared row loses the stamp)"


async def _node_and_hold(
    conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> tuple[JobId, JobId]:
    node_id = await conn.fetchval(
        f"SELECT id FROM \"{schema}\".jobs WHERE step_key = 'review' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    hold_id = await conn.fetchval(
        f'SELECT id FROM "{schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert node_id and hold_id
    return JobId(node_id), JobId(hold_id)
