"""T28 — THE DURABLE TIMER'S PINS: the hold whose resolver is the clock.

``ctx.wait_until(when)`` mints an ORDINARY hold row under the reserved
``__timer__`` signal prefix; the sweep's TIMER arm resolves it
``'held' → 'delivered'`` with the CLOCK'S OWN RECEIPT (the
``TimerWake`` payload — ``fired_by='timer'``, the scheduled vs the
fired-at stamps) and the body RESUMES. THE TIMER'S FACE INVERTS THE
EXPIRY'S: ``wait_signal``'s deadline-pass = the ``Expired`` member (the
fail-close); ``wait_until``'s deadline-pass = THE WAKE — the
continuation, never the fail-close.

Red-first evidence: the pins ran against the PRE-IMPLEMENTATION tree
(no ``wait_until``, no timer arm — the captures:
``.measurements/t28-timer-pin-reds-pre.txt``), then the implementation
flipped them green. The convicted variants recorded through
``timer_redlog`` name the dragon each pin's guard holds shut.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    TimerWake,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.api._ctx_wait import Expired
from taskq.workflows.api._hitl import (
    TIMER_SIGNAL_PREFIX,
    deliver_payload,
    sweep_due_timers,
    sweep_expired_signals,
)


class Nudge(BaseModel):
    note: str = "the clock's knock"


async def _hold_row(wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId) -> Any:
    return await wf_conn.fetchrow(
        f'SELECT id, status, signal_name, expires_at FROM "{wf_schema}".wf_signals '  # noqa: S608
        "WHERE workflow_id = $1 ORDER BY id DESC LIMIT 1",
        flow_id,
    )


# ── pin 1: THE AT-LEAST FACE (never early; the receipt's arithmetic) ────


async def test_timer_wake_is_at_least_at_ts(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, timer_redlog: Any
) -> None:
    """The documented precision (the listener landmine 4's law, the
    timer's own face): the fire lands at the FIRST sweep pass at-or-after
    the deadline — a pass BEFORE the deadline fires NOTHING (the row
    stays 'held'), a pass at/after fires the wake whose ``fired_at`` is
    at-or-after ``scheduled`` (the at-least receipt; ``lateness_s`` is
    the delta's own arithmetic)."""
    seen: dict[str, TimerWake] = {}

    async def timer_body(ctx: StepContext, params: Nudge) -> str:
        wake = await ctx.wait_until(0.25, key="at-least")
        seen["wake"] = wake
        return "woken"

    app = WorkflowApp()

    @app.workflow("timer_at_least")
    def timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("timer_at_least"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    # A SWEEP PASS BEFORE THE DEADLINE: nothing fires — the at-least
    # face's first half (never early).
    fired_early = await sweep_due_timers(wf_pool, schema=wf_schema)
    assert fired_early == 0, "a due-arm pass fired a hold whose deadline had NOT passed"
    hold = await _hold_row(wf_conn, wf_schema, flow_id)
    assert hold is not None and hold["status"] == "held", hold
    assert hold["signal_name"].startswith(TIMER_SIGNAL_PREFIX)

    # THE PASS AT/AFTER THE DEADLINE (the drive's tick runs the timer
    # arm — the dev loop's mirror of the leader's): the wake fires.
    outcome = await runner.drive(flow_id)
    assert outcome == "terminal"
    wake = seen["wake"]
    assert wake.key == "at-least"
    assert wake.fired_by == "timer"
    assert wake.fired_at >= wake.scheduled, "the wake fired BEFORE its scheduled instant"
    assert wake.lateness_s >= 0.0
    timer_redlog.red(
        "pin-at-least-early-fire",
        "the fire arm WITHOUT the `<= clock_timestamp()` gate (the mutation drill — an early wake)",
        {"conviction": "the shipped gate never fires early; the drill red is the pre-cure tree"},
    )


# ── pin 2: THE SLOT IS RELEASED (the hold's law) ─────────────────────────


async def test_timer_hold_releases_the_slot(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The held representation (the slot is NEVER parked): a timer wait
    leaves the node 'pending' + stamped, the worker's lock RELEASED (no
    lease held, no claim, no attempt burned while sleeping), and the
    ledger records 'awaited' (RESUME-NOT-RETRY — the timer's resume
    consumes no ladder attempt)."""

    async def timer_body(ctx: StepContext, params: Nudge) -> str:
        await ctx.wait_until(120.0, key="slot")
        return "woken"

    app = WorkflowApp()

    @app.workflow("timer_slot")
    def timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("timer_slot"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    node = await wf_conn.fetchrow(
        f'SELECT status, locked_by_worker, lock_expires_at, metadata FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE step_key = 'timer_node' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node is not None
    assert node["status"] == "pending", node["status"]
    assert node["locked_by_worker"] is None, "the worker's slot was parked on the timer"
    assert node["lock_expires_at"] is None
    assert node["metadata"] and "hold" in node["metadata"]
    hold = await _hold_row(wf_conn, wf_schema, flow_id)
    assert hold is not None and hold["status"] == "held"
    ledger = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 '  # noqa: S608
        "ORDER BY id DESC LIMIT 1",
        flow_id,
    )
    assert ledger == "awaited", ledger


# ── pin 3: THE DURABILITY (a death mid-wait; the successor's sweep) ─────


async def test_crash_mid_wait_the_successor_sweep_wakes_the_body(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, timer_redlog: Any
) -> None:
    """THE DURABILITY PIN (the row's the truth): the arming process
    DIES mid-wait — the hold tx was its last write; the surviving
    row-state (a held ``__timer__`` row + the stamped node, the arming
    process's objects gone) is exactly a SIGKILL's surviving state by
    construction. The SUCCESSOR's sweep fires the wake; the body
    resumes from the top; the pre-timer substep REPLAYS (never re-runs)
    and the flow completes. Nothing wedges: the durability IS the
    design, not a repair."""
    substep_runs: dict[str, int] = {"n": 0}

    async def log_note() -> str:
        substep_runs["n"] += 1
        return "note"

    async def timer_body(ctx: StepContext, params: Nudge) -> str:
        await ctx.substep("before_timer", log_note)
        wake = await ctx.wait_until(0.05, key="crash")
        return f"woken:{wake.key}"

    app = WorkflowApp()

    @app.workflow("timer_crash")
    def timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("timer_crash"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    # THE DEATH: the arming driver is gone (the process's objects drop;
    # no graceful unwind, no second write — the row-state above IS the
    # crash's residue).
    del runner

    # THE SUCCESSOR (a fresh runner — a different process's claim path)
    # fires the wake through its own sweep and drives to terminal.
    successor = FlowRunner(app.get("timer_crash"), wf_pool, wf_schema)
    await asyncio.sleep(0.12)  # the deadline passes while nobody watches
    fired = await sweep_due_timers(wf_pool, schema=wf_schema)
    assert fired == 1, fired
    outcome = await successor.drive(flow_id)
    assert outcome == "terminal"
    verdict = await successor.result(flow_id)
    assert verdict == "woken:crash", verdict
    # THE REPLAY LAW: the pre-timer substep ran ONCE across the death —
    # the successor's resume consumed the ledgered answer, never re-ran
    # the effect.
    assert substep_runs["n"] == 1, substep_runs
    timer_redlog.red(
        "pin-crash-wedge",
        "the reclaim WITHOUT the successor sweep (the mutation drill — the "
        "wake never fires, the node wedges pending)",
        {
            "conviction": "the shipped arm fires the row's own deadline; the drill red is the pre-cure tree"
        },
    )


# ── pin 4: THE CANCEL INTERPLAY (the hold dies with the flow) ───────────


async def test_cancel_during_the_timer_kills_the_hold(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The cancel cascade's timer leg: a flow cancelled DURING a timer
    wait cascades the held timer row to 'cancelled' (it is an ordinary
    hold row — the same one-tx cascade); the sweep's timer arm — the
    'held'-only CAS — never fires it (no zombie wake), and the flow
    stays dead."""

    async def timer_body(ctx: StepContext, params: Nudge) -> str:
        await ctx.wait_until(120.0, key="doomed")
        return "woken"

    app = WorkflowApp()

    @app.workflow("timer_cancel")
    def timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("timer_cancel"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    await runner.cancel_workflow(flow_id, reason="the operator changed their mind")
    fired = await sweep_due_timers(wf_pool, schema=wf_schema)
    assert fired == 0, "the timer arm fired a CANCELLED flow's hold"
    hold = await _hold_row(wf_conn, wf_schema, flow_id)
    assert hold is not None and hold["status"] == "cancelled", hold
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
        flow_id,
    )
    assert status == "cancelled", status


# ── pin 5: THE EXPIRED/TIMER INTERPLAY (orthogonal waits, one flow) ─────


async def test_expired_and_timer_waits_are_orthogonal(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The two WAIT faces in ONE flow: a node holding on a HUMAN gate
    whose deadline the expiry arm abandons (the ``Expired`` member —
    the fail-close) runs beside a node holding on the CLOCK (the wake
    — the continuation); the fan-in join lands BOTH named outcomes.
    Neither face contaminates the other: the expiry arm never touches
    the timer's prefix, the timer arm never abandons the human's."""

    async def gate_body(ctx: StepContext, params: Nudge) -> str:
        outcome = await ctx.wait_signal(Nudge, timeout_s=0.05)
        if isinstance(outcome, Expired):
            return "fail-close"
        return "answered"

    async def timer_body(ctx: StepContext, params: Nudge) -> str:
        wake = await ctx.wait_until(0.05, key="orthogonal")
        return f"wake:{wake.fired_by}"

    async def join_body(ctx: StepContext, gate: str, timer: str) -> str:
        return f"{gate}+{timer}"

    app = WorkflowApp()

    @app.workflow("timer_orthogonal")
    def timer_flow() -> Promise[object]:
        gate_p = step(gate_body, Nudge(), key="gate_node")
        timer_p = step(timer_body, Nudge(), key="timer_node")
        return build(step(join_body, gate_p, timer_p, key="join_node"))

    runner = FlowRunner(app.get("timer_orthogonal"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    # BOTH deadlines pass (both faces declared 0.05s), then BOTH arms
    # run (the leader's signal sweep would; the pins call the arms
    # directly — the same statements the leader's tick runs).
    await asyncio.sleep(0.12)
    await sweep_expired_signals(wf_pool, schema=wf_schema)
    await sweep_due_timers(wf_pool, schema=wf_schema)
    outcome = await runner.drive(flow_id)
    assert outcome == "terminal"
    verdict = await runner.result(flow_id)
    assert verdict == "fail-close+wake:timer", verdict


# ── pin 6: THE RESOLVE BOUNDARY (the clock is the only resolver) ────────


async def test_hand_delivery_cannot_resolve_a_timer_hold(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A human hand CANNOT resolve the clock's hold: the resolve
    boundary's strict schema fit (the hold row's own payload_schema —
    the ``TimerWake`` contract) refuses any hand-delivered payload; the
    hold SURVIVES 'held' and the timer arm still fires it. No zombie
    wake, no forged receipt."""
    import json

    async def timer_body(ctx: StepContext, params: Nudge) -> str:
        wake = await ctx.wait_until(0.05, key="forged")
        return f"woken:{wake.key}:{wake.fired_by}"

    app = WorkflowApp()

    @app.workflow("timer_forge")
    def timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("timer_forge"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    hold = await _hold_row(wf_conn, wf_schema, flow_id)
    assert hold is not None
    result = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(hold["id"]),
        payload={
            "key": "forged",
            "scheduled": "2026-01-01T00:00:00Z",
            "fired_at": "2026-01-01T00:00:00Z",
        },
        payload_json=json.dumps(
            {
                "key": "forged",
                "scheduled": "2026-01-01T00:00:00Z",
                "fired_at": "2026-01-01T00:00:00Z",
            }
        ),
    )
    assert result.status == "refused", result
    # THE HOLD SURVIVES, and the CLOCK still resolves it (the typed
    # door's refusal is the boundary's, not the row's death).
    await asyncio.sleep(0.12)  # the deadline passes, the refusal notwithstanding
    fired = await sweep_due_timers(wf_pool, schema=wf_schema)
    assert fired == 1, fired
    outcome = await runner.drive(flow_id)
    assert outcome == "terminal"
    verdict = await runner.result(flow_id)
    assert verdict == "woken:forged:timer", verdict
