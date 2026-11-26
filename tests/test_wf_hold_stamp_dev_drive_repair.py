# ruff: noqa: S608  # Why: every schema interpolation is the module fixture's validated test identifier; every value is $-bound.
"""Probes for the hold-stamp reconcile's reach outside the leader's sweep loop.

``reassert_hold``'s docstring names its repair for the one race it accepts: "A
delivery that lands between the caller's standing-hold read and this park
leaves a dead stamp; the leader's hold-stamp reconcile clears it." The reconcile
(``sweep_hold_stamps``) has exactly one production caller — the leader's sweep
table — while ``FlowRunner.tick``, the in-process driver ``taskq.workflows.run``
rides, runs five sweep arms (join rederive, child joins, expired signals, the
outbox drain, due timers) and not this one. So in the shape where the re-park
also runs, the race the docstring accepts strands the row with no arm to clear
it.

The state is built through the engine's own surfaces: a real ``ctx.wait_signal``
whose mint commits on its own running claim, then the park's stamp undone (the
re-held row is the input shape the re-entry arm reads — the same residue
``tests/test_wf_claim_epoch_default_reentry.py`` stages), then the in-process
claim CAS the dev drive itself runs, then a real ``HitlClient.resolve`` inside
the read-then-park window, then ``reassert_hold`` at the granted epoch — the
same call ``CtxWaitOps.wait_signal``'s re-entry arm makes. The end state is
byte for byte the one
``tests/test_wf_untimed_join_fleet_control.py::test_a_delivery_inside_the_reentry_window_is_repaired_by_the_reconcile``
asserts (``pending`` + the stamp + a ``delivered`` signal), so nothing here is
an input a real interleaving cannot produce. The two non-production writes are
the park's stamp being undone and the due instant (the dev claim CAS hardcodes
a 90 s lease; a lease lapse is a GC pause or a partition, not a code path).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.exceptions import SignalTimeoutError
from taskq.migrate import apply_pending
from taskq.workflows import Expired, FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows._sweep import sweep_hold_stamps
from taskq.workflows.api._hitl import HitlClient, reassert_hold
from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql
from taskq.workflows.engine import render_workflow_sql

#: Far enough out that no expiry arm resolves the stand mid-probe.
_TIMED_S = 600.0


class _Approval(BaseModel):
    verdict: str


def _compile(suffix: str, stalled: asyncio.Event, release: asyncio.Event) -> Any:
    app = WorkflowApp()

    @app.workflow(f"i748probe_{suffix}")
    def flow() -> Promise[object]:
        async def wrap(ctx: StepContext) -> str:
            if ctx.attempt == 1:
                stalled.set()
                await release.wait()  # the stalled body: paused before its first wait
            answer = await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
            if isinstance(answer, Expired):
                raise SignalTimeoutError("the probe's wait expired")
            return answer.verdict

        return build(step(wrap, key="wrap"))

    return app.get(f"i748probe_{suffix}")


def _meta(raw: object) -> dict[str, Any]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded  # pyright: ignore[reportUnknownVariableType]


async def _wrap(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> asyncpg.Record:
    row = await conn.fetchrow(
        f'SELECT id, status, attempt, claim_epoch, metadata FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'wrap'",
        flow_id,
    )
    assert row is not None
    return row


async def _accepted_race(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    schema: str,
    flow_id: JobId,
    driver: FlowRunner,
    stalled: asyncio.Event,
    release: asyncio.Event,
) -> dict[str, Any]:
    """Leave an unstamped standing hold, claim the row, deliver inside the
    re-entry window, then re-park onto the now-delivered hold. Returns the
    stranded row's facts and the delivery verdict."""
    stand = asyncio.create_task(driver.tick(flow_id))
    await asyncio.wait_for(stalled.wait(), timeout=10.0)
    held_row = await _wrap(conn, schema, flow_id)
    release.set()
    await asyncio.wait_for(stand, timeout=10.0)
    standing = await conn.fetchrow(
        f'SELECT id, signal_name, expires_at, status FROM "{schema}".wf_signals '
        "WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
    )
    assert standing is not None, "the released body minted no standing hold"
    # the body's mint parks the row onto its own hold; the re-entry arm's input
    # shape is that park's stamp undone — the round-1 write below is the only
    # non-production one.
    await conn.execute(
        f'UPDATE "{schema}".jobs '
        "SET metadata = metadata - 'hold' - 'held_signal', scheduled_at = now() "
        "WHERE id = $1",
        held_row["id"],
    )
    repended = await _wrap(conn, schema, flow_id)
    assert repended["status"] == "pending" and "hold" not in _meta(repended["metadata"]), (
        "the stalled body's mint left no stamp-less standing hold: "
        f"status={repended['status']!r} metadata={_meta(repended['metadata'])!r}"
    )

    # the claim the re-entered wait runs under — the in-process claim CAS the
    # dev drive itself takes, and whose granted epoch every fence carries.
    claimed = await conn.fetchrow(render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), repended["id"], None)
    assert claimed is not None, "the unstamped stand's row was not claimable"
    epoch = int(claimed["claim_epoch"])

    # the delivery lands between the standing-hold read and the park.
    verdict = await HitlClient(pool, schema=schema).resolve(
        JobId(standing["id"]), {"verdict": "approve"}, principal="probe"
    )
    # the state the responder's refusal was decided on: the claim's row still
    # running under this claim's epoch, the flow alive.
    at_refusal = await conn.fetchrow(
        f'SELECT n.status AS node_status, j.status AS flow_status FROM "{schema}".jobs n '
        f"JOIN \"{schema}\".jobs j ON j.id = (n.metadata->>'flow_id')::uuid "
        "WHERE n.id = $1",
        JobId(repended["id"]),
    )
    assert at_refusal is not None
    # the re-entered wait's park — the exact call CtxWaitOps.wait_signal makes.
    await reassert_hold(
        conn,
        schema=schema,
        node_id=JobId(repended["id"]),
        hold_id=JobId(standing["id"]),
        signal_name=str(standing["signal_name"]),
        expires_at=standing["expires_at"],
        claim_epoch=epoch,
    )
    parked = await _wrap(conn, schema, flow_id)
    signal_status = await conn.fetchval(
        f'SELECT status FROM "{schema}".wf_signals WHERE id = $1', JobId(standing["id"])
    )
    return {
        "hold_id": str(standing["id"]),
        "verdict": verdict,
        "epoch": epoch,
        "at_refusal": (str(at_refusal["flow_status"]), str(at_refusal["node_status"])),
        "status": parked["status"],
        "stamp": _meta(parked["metadata"]).get("hold"),
        "signal_status": str(signal_status),
    }


@pytest.mark.integration
async def test_the_accepted_race_is_repaired_by_the_dev_drive_s_own_tick(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The dev shape's repair contract, enforced: the driver that can leave a
    dead stamp (the re-park's accepted race) also heals it. ``FlowRunner.tick``
    runs ``sweep_hold_stamps`` beside the join/child/expiry/timer arms, so the
    unclaimable row (the claim CAS's ``AND NOT metadata ? 'hold'``) is cleared
    on the next tick, the delivered answer replays, and the run reaches
    terminal — never ``max_ticks`` with the run alive-but-stopped."""
    suffix = new_uuid().hex[:12]
    stalled, release = asyncio.Event(), asyncio.Event()
    flow = _compile(suffix, stalled, release)
    flow_id = (await FlowRunner(flow, wf_pool, wf_schema).create_flow()).flow_id
    driver = FlowRunner(flow, wf_pool, wf_schema)
    stranded = await _accepted_race(wf_conn, wf_pool, wf_schema, flow_id, driver, stalled, release)
    # the state the accepted race leaves, asserted first so a setup drift
    # cannot masquerade as the pin.
    assert (stranded["status"], stranded["stamp"], stranded["signal_status"]) == (
        "pending",
        stranded["hold_id"],
        "delivered",
    )

    outcome = await driver.drive(flow_id, until="terminal", max_ticks=50, tick=0.0)
    after = await _wrap(wf_conn, wf_schema, flow_id)
    assert outcome == "terminal", (
        "the in-process driver did not heal the dead stamp its own re-park leaves: "
        f"drive ended {outcome!r} with the node row {after['status']!r}, stamped "
        f"{_meta(after['metadata']).get('hold')!r} on a {stranded['signal_status']!r} hold. "
        "FlowRunner.tick's sweep block must run sweep_hold_stamps beside the "
        "join/child/expiry/timer arms; without it the row is unclaimable by the "
        "claim CAS and the run stops with no signal."
    )
    assert after["status"] == "succeeded", after["status"]


@pytest.mark.integration
async def test_the_leader_reconcile_clears_the_stamp_and_the_run_completes(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The same stranded state, repaired by the fleet's arm: one
    ``sweep_hold_stamps`` pass clears the dead stamp, the re-claim replays the
    delivered answer from the queue, and the run completes — so the flow was
    never dead, only unrepaired."""
    suffix = new_uuid().hex[:12]
    stalled, release = asyncio.Event(), asyncio.Event()
    flow = _compile(suffix, stalled, release)
    flow_id = (await FlowRunner(flow, wf_pool, wf_schema).create_flow()).flow_id
    driver = FlowRunner(flow, wf_pool, wf_schema)
    stranded = await _accepted_race(wf_conn, wf_pool, wf_schema, flow_id, driver, stalled, release)
    assert stranded["verdict"].status == "refused"

    assert await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema)) == 1
    assert await driver.drive(flow_id, until="terminal", max_ticks=50, tick=0.0) == "terminal"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "succeeded"


@pytest.mark.integration
async def test_a_refused_delivery_on_a_live_flow_never_names_a_dead_flow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The refusal's cause must be true. On the accepted-race path the answer
    is durably recorded and the flow goes on to complete (see the test above),
    so "the held node is not resumable (the flow died)" sends the responder
    looking for a dead flow — and toward abandoning or re-running a live one."""
    suffix = new_uuid().hex[:12]
    stalled, release = asyncio.Event(), asyncio.Event()
    flow = _compile(suffix, stalled, release)
    flow_id = (await FlowRunner(flow, wf_pool, wf_schema).create_flow()).flow_id
    driver = FlowRunner(flow, wf_pool, wf_schema)
    stranded = await _accepted_race(wf_conn, wf_pool, wf_schema, flow_id, driver, stalled, release)
    verdict = stranded["verdict"]
    # the shape this probe records: the CAS won (the answer stands on the
    # signal row), the resume found a row that was not resumable.
    assert verdict.status == "refused"
    assert stranded["signal_status"] == "delivered"
    flow_state, node_state = stranded["at_refusal"]
    assert (flow_state, node_state) == ("running", "running"), stranded["at_refusal"]
    assert "flow died" not in (verdict.reason or ""), (
        "a delivery refused on a live flow names a dead one: "
        f"reason={verdict.reason!r} while the flow is {flow_state!r} and the node row is "
        f"{node_state!r} under claim epoch {stranded['epoch']} with its answer recorded as "
        "delivered. The real cause is the node row's status (a claim holds it), not the "
        "flow's — and the run goes on to complete once the stamp is reconciled (see the "
        "sibling probe)."
    )


async def _stamped_running_node(
    conn: asyncpg.Connection, pool: asyncpg.Pool, schema: str
) -> tuple[JobId, JobId, JobId]:
    """A flow driven to its hold, then the node row put back to ``running``
    with its stamp KEPT — the state ``_DELIVER_RESUME_SQL``'s ``AND status =
    'pending'`` gate refuses, and the one that reaches the refusal's row read
    (the hold's stamp still names the node)."""
    suffix = new_uuid().hex[:12]
    stalled, release = asyncio.Event(), asyncio.Event()
    release.set()  # the attempt-1 body is past its stall: it waits immediately
    flow = _compile(suffix, stalled, release)
    flow_id = (await FlowRunner(flow, pool, schema).create_flow()).flow_id
    driver = FlowRunner(flow, pool, schema)
    assert await driver.drive(flow_id, until="held") == "held"
    row = await _wrap(conn, schema, flow_id)
    assert row["status"] == "pending" and "hold" in _meta(row["metadata"])
    hold = await conn.fetchrow(
        f"SELECT id FROM \"{schema}\".wf_signals WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
    )
    assert hold is not None
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'running', claim_epoch = $2, "
        "locked_by_worker = $3, lock_expires_at = now() + interval '60 seconds' "
        "WHERE id = $1",
        row["id"],
        7,
        new_uuid(),
    )
    return JobId(flow_id), JobId(row["id"]), JobId(hold["id"])


@pytest.mark.integration
async def test_a_refused_delivery_on_a_stamped_running_node_names_the_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The stamped row's refusal reads the row: a node the flow is currently
    RUNNING (under claim epoch 7) matches the resume's ``status = 'pending'``
    gate with nothing, while the flow is alive, so the refusal must name the
    node's status and claim — never "the flow died"."""
    flow_id, node_id, hold_id = await _stamped_running_node(wf_conn, wf_pool, wf_schema)
    verdict = await HitlClient(wf_pool, schema=wf_schema).resolve(
        hold_id, {"verdict": "approve"}, principal="pin"
    )
    assert verdict.status == "refused", verdict
    reason = verdict.reason or ""
    assert "flow died" not in reason, reason
    assert "'running'" in reason, reason
    assert "claim epoch 7" in reason, reason
    assert "its flow is 'running'" in reason, reason
    assert str(node_id) in reason or "the node row is" in reason, reason
    # the answer is recorded (the resolve CAS won) — the responder is told the
    # truth: it stands, and the run resumes once the row is claimable.
    delivered = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold_id
    )
    assert delivered == "delivered"
    flow_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert flow_status == "running", "the fixture's flow must be live"


@pytest.mark.integration
async def test_a_corrupt_flow_link_is_a_typed_refusal_not_a_cast_error(pg_dsn: str) -> None:
    """A stamped node whose ``metadata.flow_id`` is malformed must refuse
    typed. Both casts are fail-isolated: the hold→node lookup finds no node
    (the door refuses before the resume), and the refusal's own row read
    reports the node state with an unreadable flow — neither raises an
    asyncpg cast error.

    This runs in its OWN throwaway schema. The schema's expression index
    (``jobs_wf_flow_nodes_idx``) casts the flow link on every write, so a
    malformed value is normally UNREPRESENTABLE; dropping that index on the
    shared module schema and recreating it after the corrupt write is unsafe
    under parallel xdist — a peer worker's open transaction keeps an older
    ``OldestXmin``, and the recreated ``CREATE INDEX`` then evaluates the
    recently-dead ``"not-a-uuid"`` heap tuple. Here the index is dropped, the
    legacy/restored-backup shape is staged, and the whole schema is DROPPED,
    so no index build ever sees the corrupt value and the shared schema is
    never mutated."""
    from taskq.workflows.api._hitl import (
        _resume_refusal_reason,  # pyright: ignore[reportPrivateUsage]  # Why: the refusal read is the unit under test (the only path that reads the flow link inside the refusal).
    )

    corrupt_schema = f"tq_corrupt_{new_uuid().hex[:10]}"
    control = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    blocker = await asyncpg.connect(pg_dsn)
    try:
        await control.execute(f'DROP SCHEMA IF EXISTS "{corrupt_schema}" CASCADE')
        await apply_pending(control, schema=corrupt_schema)

        # The CI shape: a peer connection holding an older xmin across the
        # test. Nothing here builds an index over the corrupt tuple, so it is
        # inert — held so a reintroduced shared-schema rebuild reproduces the
        # parallel failure instead of hiding it.
        await blocker.execute("BEGIN")
        await blocker.fetchval("SELECT txid_current()")

        _flow_id, node_id, hold_id = await _stamped_running_node(control, pool, corrupt_schema)
        await control.execute(f'DROP INDEX IF EXISTS "{corrupt_schema}".jobs_wf_flow_nodes_idx')
        # The cohort check keeps `fairness_key` equal to `metadata.flow_id`:
        # the corruption moves both, exactly as a bad writer would.
        await control.execute(
            f'UPDATE "{corrupt_schema}".jobs '
            "SET metadata = jsonb_set(metadata, '{flow_id}', '\"not-a-uuid\"'::jsonb), "
            "fairness_key = 'not-a-uuid' WHERE id = $1",
            node_id,
        )

        # The public door: the stamped row's flow link no longer resolves, so
        # the hold's node lookup finds nothing — the delivery is refused, not
        # raised.
        verdict = await HitlClient(pool, schema=corrupt_schema).resolve(
            hold_id, {"verdict": "approve"}, principal="pin"
        )
        assert verdict.status == "refused", verdict
        assert "flow died" not in (verdict.reason or ""), verdict.reason

        # The refusal's row read (the branch a found node reaches, via the
        # helper directly — the lookup above cannot find this row): the CASE
        # guard yields NULL for the malformed link and the message says so.
        reason = await _resume_refusal_reason(control, corrupt_schema, str(node_id))
        assert "not resumable" in reason, reason
        assert "flow status is unreadable" in reason, reason
        assert "flow died" not in reason, reason
    finally:
        await blocker.execute("ROLLBACK")
        await blocker.close()
        await pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{corrupt_schema}" CASCADE')
        await control.close()
