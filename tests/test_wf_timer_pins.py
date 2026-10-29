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


class SecondSay(BaseModel):
    word: str


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


# ── pin 7: THE TIMER-THEN-SIGNAL STRAND (the I5c cure) ──────────────────
# One PLAIN step, the timer wait BEFORE the signal wait, disjoint names:
# the timer's documented re-consume ("a replay/retry re-consumes it from
# the answer queue") ran on the SHARED per-attempt cursor — it inflated
# the cursor past the signal site's own queue, the DELIVERED answer fell
# behind forever, the wait re-held silently (the plain kind has no E13
# teeth), the deadline swept the row to 'abandoned', the body returned
# the fail-close arm, and THE RUN TERMINALIZED `succeeded` with the
# operator's decision a dead letter (the C7 family's face on an
# unwritten boundary). THE CURE — E13's per-site discipline generalized:
# the answer queue's cursor is keyed to the WAIT SITE (its own signal
# name, beneath the attempt), so every site counts ITS OWN
# consumptions; the loop kind keeps the iteration counter (the ADVANCE
# statement's own write) untouched.


async def test_timer_then_signal_one_plain_step_delivers_the_verdict(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE STRAND'S EXACT REPRO: the timer, then the signal, the
    resolve, the wake — the body receives THE DELIVERED VERDICT (never
    the ``Expired`` fail-close; the delivered answer's row is CONSUMED,
    never abandoned). Red x2 at the shared-cursor head (the timer's
    re-consume inflated the shared cursor past the signal site's own
    queue; the wait re-held; the run terminal-succeeded with the
    operator's decision a dead letter); green at the per-site
    cursor."""
    import json

    async def mixed(ctx: StepContext) -> str:
        _wake = await ctx.wait_until(0.05, key="tick")  # site 1: the clock
        say = await ctx.wait_signal((SecondSay,), timeout_s=30.0)  # site 2: the human
        if isinstance(say, Expired):
            return "expired"
        return f"say:{say.word}"

    app = WorkflowApp()

    @app.workflow("timer_then_signal")
    def timer_flow() -> Promise[object]:
        return build(step(mixed, key="t"))

    runner = FlowRunner(app.get("timer_then_signal"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # 1. The first pass: the body mints the TIMER hold.
    assert await runner.drive(flow_id, until="held") == "held"
    # 2. THE CLOCK FIRES (the sweep's timer arm — the leader's leg; the
    # dev drive's `until="held"` check parks before the tick, so the pin
    # calls the arm the way the leader's sweep does).
    await asyncio.sleep(0.15)  # the deadline passes
    assert await sweep_due_timers(wf_pool, schema=wf_schema, drain=True) == 1
    # 3. The body consumes the wake and mints the SIGNAL hold.
    assert await runner.drive(flow_id, until="held") == "held"
    # 4. THE OPERATOR DELIVERS (the CAS: delivered).
    held = await wf_conn.fetchrow(
        f'SELECT id, signal_name FROM "{wf_schema}".wf_signals '  # noqa: S608
        "WHERE workflow_id = $1 AND status = 'held' ORDER BY id LIMIT 1",
        flow_id,
    )
    assert held is not None and held["signal_name"] == "SecondSay", held
    delivered = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(held["id"]),
        payload={"word": "second"},
        payload_json=json.dumps({"word": "second"}),
    )
    assert delivered.status == "delivered", delivered
    # 5. THE RESUME: the body re-executes from the top — the timer site
    # re-consumes its wake, and the SIGNAL SITE must consume ITS OWN
    # delivered answer.
    out = await runner.drive(flow_id, until="terminal")
    assert out == "terminal"
    verdict = await runner.result(flow_id)
    assert verdict == "say:second", (
        f"THE STRAND: the delivered answer never reached the body (the "
        f"timer's re-consume inflated the shared cursor past this site's "
        f"queue; the wait re-held; the deadline fail-closed; the run "
        f"succeeded with the operator's decision a dead letter): {verdict!r}"
    )
    rows = await wf_conn.fetch(
        f'SELECT signal_name, status, hold_epoch FROM "{wf_schema}".wf_signals '  # noqa: S608
        "WHERE workflow_id = $1 ORDER BY id",
        flow_id,
    )
    abandoned = [dict(r) for r in rows if r["status"] == "abandoned"]
    assert abandoned == [], (
        f"the signal hold was abandoned — the delivered answer stranded: {abandoned}"
    )
    # THE ANSWER'S ROW: delivered AND consumed — the operator's decision
    # reached its site (never a dead letter on the rows).
    answer = [dict(r) for r in rows if r["signal_name"] == "SecondSay"]
    assert answer and answer[-1]["status"] == "delivered", answer


async def test_two_signal_sites_one_plain_step_stays_green(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE CONTROL (the I5b face — the shape that was NEVER broken): one
    plain step, TWO signal waits, disjoint names. The per-site cursor
    keeps each site's own count exactly as the old shared cursor did
    when the sites each consume once per resume — the two delivered
    answers reach their own sites, in order, no strand."""
    import json

    async def two_gates(ctx: StepContext) -> str:
        first = await ctx.wait_signal((SecondSay,), timeout_s=30.0)
        second = await ctx.wait_signal((Nudge,), timeout_s=30.0)
        if isinstance(first, Expired) or isinstance(second, Expired):
            return "expired"
        return f"{first.word}:{second.note}"

    app = WorkflowApp()

    @app.workflow("two_signal_sites")
    def timer_flow() -> Promise[object]:
        return build(step(two_gates, key="t"))

    runner = FlowRunner(app.get("two_signal_sites"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    for _ in range(4):
        out = await runner.drive(flow_id, until="held")
        if out != "held":
            break
        held = await wf_conn.fetchrow(
            f'SELECT id, signal_name FROM "{wf_schema}".wf_signals '  # noqa: S608
            "WHERE workflow_id = $1 AND status = 'held' ORDER BY id LIMIT 1",
            flow_id,
        )
        if held is None:
            break
        payload = {"word": "second"} if held["signal_name"] == "SecondSay" else {"note": "nudge"}
        delivered = await deliver_payload(
            wf_pool,
            schema=wf_schema,
            workflow_id=flow_id,
            hold_id=JobId(held["id"]),
            payload=payload,
            payload_json=json.dumps(payload),
        )
        assert delivered.status == "delivered", delivered

    out = await runner.drive(flow_id, until="terminal")
    assert out == "terminal"
    verdict = await runner.result(flow_id)
    assert verdict == "second:nudge", verdict
    rows = await wf_conn.fetch(
        f'SELECT signal_name, status FROM "{wf_schema}".wf_signals '  # noqa: S608
        "WHERE workflow_id = $1",
        flow_id,
    )
    abandoned = [dict(r) for r in rows if r["status"] == "abandoned"]
    assert abandoned == [], abandoned
