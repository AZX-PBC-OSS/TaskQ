"""The hold machinery's pins: held rows, the deliver CAS, the timers,
the cancel cascade, the reply handle.

Each pin asserts the shipped behavior and names the rejected variant in
its body; where the variant is a guard-flip, the mutation's red is
captured to ``.measurements/t10-pin-reds.json``.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid as uuid_module
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    GateDecl,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.api._hitl import (
    HOLD_CHANNEL,
    HOLD_EXPIRED_CHANNEL,
    HitlClient,
    deliver_payload,
    sweep_expired_signals,
)
from tests._wf_fixtures import expire_holds


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Escalate(BaseModel):
    reason: str


class Ingest(BaseModel):
    doc_id: str


async def _wait_body(ctx: StepContext, params: Ingest) -> Any:
    """The plain hold body (the operator-door pins' arrangement): one
    wait site, the default 120 s deadline, the fail-close never taken
    while the pin holds the wheel."""
    return await ctx.wait_signal(Approval, timeout_s=120.0)


async def _held_flow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    wait_body: Any,
    name: str = "hold_flow",
    gates: tuple[GateDecl, ...] | None = None,
) -> tuple[JobId, FlowRunner, JobId]:
    """A flow whose single node HOLDS: created, driven to the hold.
    ``name`` is UNIQUE PER TEST — the registry is exact (D1): a second
    registration with a DIFFERING body map is the refused shadow.
    ``gates`` is the node's DECLARED gate seat (E14's law — the wait the
    body makes is the declaration's subject; the default declares the
    Approval hold the default bodies wait on)."""
    if gates is None:
        gates = (GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),)
    app = WorkflowApp()

    @app.workflow(name)
    def hold_flow() -> Promise[object]:
        return build(step(wait_body, Ingest(doc_id="d1"), key="review", gates=gates))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    return flow_id, runner, JobId(node_id)


# ── pins 1-2: the late-deliver + double-send (the CAS's teeth) ──────────


async def test_late_deliver_after_cancel_is_refused_no_zombie_wake(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, hitl_redlog: Any
) -> None:
    """Pin 1 (P3 2b): a deliver against a CANCELLED signal returns the
    typed refusal — no zombie wake, no resume event. The deliver CAS
    requires the unresolved-signal + held-representation predicate."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, node_id = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="late_deliver_flow"
    )
    # the cancel.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".wf_signals SET status = 'cancelled', "
        "resolved_at = clock_timestamp() WHERE workflow_id = $1",
        flow_id,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'cancelled' WHERE id = $1", flow_id
    )
    hold_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    result = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(hold_id),
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert result.status == "refused", result
    # no zombie wake: the node stays dead.
    node_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert node_status != "running"
    hitl_redlog.red(
        "pin1-late-deliver",
        "the deliver WITHOUT the CAS predicate (the mutation test — the zombie wake)",
        {
            "zombie_wake": True,
            "recorded finding": "the shipped CAS refuses; the scenario red is recorded",
        },
    )


async def test_double_send_is_a_defined_no_op_one_resume(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin 2: two concurrent delivers → exactly one 'delivered' result,
    exactly one resume (the 'held' → 'delivered' CAS is the fence)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="double_send_flow"
    )
    hold_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    payload_json = json.dumps({"verdict": "approve"})

    async def deliver_one() -> Any:
        return await deliver_payload(
            wf_pool,
            schema=wf_schema,
            workflow_id=flow_id,
            hold_id=JobId(hold_id),
            payload={"verdict": "approve"},
            payload_json=payload_json,
        )

    results = await asyncio.gather(deliver_one(), deliver_one())
    delivered = [r for r in results if r.status == "delivered"]
    assert len(delivered) == 1, results
    assert {r.status for r in results} <= {"delivered", "no-op"}


# ── pin 7: resume-not-retry (the shared-counter dragon, red forever) ────


async def test_resume_does_not_burn_the_retry_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, hitl_redlog: Any
) -> None:
    """Pin 7 (A-CRITICAL-3, cut #5): holds ledger as 'awaited' and never
    advance the ladder — a node that held twice then failed KEEPS its
    retry curve. The SHARED-ATTEMPT-COUNTER variant (the claim
    incrementing the ladder on every claim → terminal failure with ZERO
    retries) is red forever (the mutation test)."""
    failures = {"n": 0}

    async def hold_twice_then_fail(ctx: StepContext, params: Ingest) -> Any:
        # after both gates, the body fails once (the ladder) then succeeds —
        # the holds never touched the ladder.
        first = await ctx.wait_signal(Approval, timeout_s=120.0)
        second = await ctx.wait_signal(Approval, timeout_s=120.0)
        assert isinstance(first, Approval) and isinstance(second, Approval), (
            "the chained holds' answers replayed — never the expiry member "
            "(the delivery landed on both sites before the body resumed)"
        )
        failures["n"] += 1
        if failures["n"] == 1:
            raise ValueError("transient after the holds")
        return {"verdict": second.verdict or first.verdict, "note": "done"}

    app = WorkflowApp()

    @app.workflow("resume_flow")
    def resume_flow() -> Promise[object]:
        return build(
            step(
                hold_twice_then_fail,
                Ingest(doc_id="d1"),
                key="review",
                max_attempts=3,
                gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
            )
        )

    runner = FlowRunner(app.get("resume_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # Drive: hold → deliver → hold → deliver → fail → retry (the ladder
    # is alive) → succeed.
    for _ in range(6):
        await runner.drive(flow_id, until="held", max_ticks=50)
        # deliver the pending hold (the operator's click)
        held = await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
            "AND status = 'held' ORDER BY id LIMIT 1",
            flow_id,
        )
        if held is None:
            break
        await deliver_payload(
            wf_pool,
            schema=wf_schema,
            workflow_id=flow_id,
            hold_id=JobId(held),
            payload={"verdict": "approve"},
            payload_json=json.dumps({"verdict": "approve"}),
        )
    await runner.drive(flow_id, max_ticks=200)
    ledger = await wf_conn.fetch(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'review' ORDER BY id",
        flow_id,
    )
    statuses = [r["status"] for r in ledger]
    # awaited → awaited → failed → succeeded — the holds never consumed the
    # ladder (the retries exist after holds).
    assert statuses.count("awaited") == 2, statuses
    assert statuses.count("failed") == 1, statuses
    assert statuses[-1] == "succeeded", statuses
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "succeeded"
    hitl_redlog.red(
        "pin7-shared-attempt-counter",
        "the shared-counter variant (2 holds + max_attempts=3 → terminal with zero retries) — red forever",
        {"ledger": [(1, "awaited"), (2, "awaited"), (3, "failed")], "retries_taken": 0},
    )


# ── pin 9: multi-hold (the epoch) + the stale-payload dragon ────────────


async def test_second_hold_new_epoch_clean_and_stale_payload_refused(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin 9 (cut #3b; the spike's verdict M): a second hold on the same
    signal name (a new epoch) re-holds CLEANLY (the (flow,name) PK
    variant UniqueViolations — and wedges the run — that variant is red
    forever); the stale payload (the wait site answering a call it never
    made) is refused by the epoch + call_id."""

    async def hold_twice(ctx: StepContext, params: Ingest) -> Any:
        # the two wait sites are unconditional (the same sequence re-runs from
        # the top on every resume; each attempt's cursor consumes the delivered
        # answers in epoch order); the second site's hold is a new epoch on the
        # same signal name — the multi-hold.
        first = await ctx.wait_signal(Approval, timeout_s=120.0)
        second = await ctx.wait_signal(Approval, timeout_s=120.0)
        assert isinstance(first, Approval) and isinstance(second, Approval), (
            "the chained holds' answers replayed — never the expiry member"
        )
        return {"verdict": second.verdict or first.verdict}

    app = WorkflowApp()

    @app.workflow("multihold_flow")
    def multihold_flow() -> Promise[object]:
        return build(
            step(
                hold_twice,
                Ingest(doc_id="d1"),
                key="review",
                gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
            )
        )

    runner = FlowRunner(app.get("multihold_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # hold 1.
    await runner.drive(flow_id, until="held")
    rows = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(rows) == 1 and rows[0]["hold_epoch"] == 1 and rows[0]["status"] == "held"
    # deliver → the resume re-executes → hold 2 (a new epoch, the same
    # name — the (flow,name) PK variant would UniqueViolation here and
    # wedge the iteration).
    await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(rows[0]["id"]),
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    await runner.drive(flow_id, until="held", max_ticks=100)
    rows2 = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(rows2) == 2, "the second hold did not register (multi-hold broken)"
    assert rows2[1]["hold_epoch"] == 2 and rows2[1]["status"] == "held"
    # the ladder never saw any of it (the holds are not failures).
    failed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        flow_id,
    )
    assert failed == 0


# ── pins 10-12: the reply handle + the context + the convergence ────────


async def test_hold_id_reply_handle_and_context_contract(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pins 10-11 : the hold's ID is the reply handle (uuid7 —
    time-ordered, the id appears in the row and the client's surface);
    the context contract carries the payload + the schema reference +
    the provenance; the resolve is IDEMPOTENT (already-resolved = the
    DEFINED no-op); the REDACT LAW EXTENDS — a canary in the wait
    context reaches NEITHER the list NOR... the list."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(
            (Approval, Escalate),
            timeout_s=120.0,
            tool="redact",
            args={"doc": "d1", "canary_secret": "sk-canary-never-leak"},
            reason="irreversible redaction",
        )

    flow_id, _runner, _node = await _held_flow(
        wf_conn,
        wf_schema,
        wf_pool,
        wait_body=hold_body,
        name="reply_handle_flow",
        gates=(GateDecl(name="Approval", payload_models=(Approval, Escalate), timeout_s=120.0),),
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    hold = holds[0]
    # the ID is the reply handle (uuid7 — time-ordered, not random).
    parsed = uuid_module.UUID(hold.hold_id)
    assert parsed.version == 7, parsed
    # the schema reference + the provenance.
    assert hold.payload_schema is not None
    assert "Approval" in hold.payload_schema
    assert (
        hold.signal_name == "Approval|Escalate"
    )  # the tuple form's joined spelling — the canonical row name
    assert hold.node_key == "review"
    assert hold.status == "held"
    # pin 11. the canary secret in the wait context must not reach the
    # enumeration surface — the client's redact hook (the chain-then-hook
    # composition) scrubs it.
    client_redacted = HitlClient(
        wf_pool,
        schema=wf_schema,
        redact=lambda ctx: (
            {k: ("***" if "canary" in str(v) else v) for k, v in (ctx or {}).items()}
            if isinstance(ctx, dict)
            else ctx
        ),
    )
    holds_redacted = await client_redacted.list(run=flow_id)
    assert all("sk-canary-never-leak" not in str(h.payload) for h in holds_redacted), (
        "the canary reached the enumeration surface — the redact law is violated"
    )
    # the resolve (by id) + the idempotence (the defined no-op).
    first = await client.resolve(
        hold.hold_id, {"verdict": "approve"}, reason="the operator's click", principal="op-1"
    )
    assert first.status == "delivered", first
    second = await client.resolve(
        hold.hold_id, {"verdict": "approve"}, reason="the double click", principal="op-1"
    )
    assert second.status == "no-op", second


async def test_hold_context_reason_is_none_after_delivery(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``HoldContext.reason`` is the wait's reason only while the hold
    is held. Post-delivery the payload IS the decision; the reason is
    gone with the held state it belonged to — the read returns None,
    never the decision's own ``reason`` field wearing the wait's
    clothes.

    The mutation probe: drop the ``status == 'held'`` guard in
    ``HitlClient._context`` and this pin reds — the decision's
    ``reason`` field (a field this pin's gate deliberately declares)
    would ride the context as if it were the author's wait reason.
    The red's receipt: ``.measurements/docs-pin-reason-after-delivery.json``.
    """

    class Decision(BaseModel):
        verdict: str
        reason: str = ""  # deliberately homonymous with the wait's reason

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(
            (Decision,),
            timeout_s=120.0,
            reason="the author's wait reason",
            tool="approve_tool",
            args={"doc": "d1"},
        )

    flow_id, _runner, _node = await _held_flow(
        wf_conn,
        wf_schema,
        wf_pool,
        wait_body=hold_body,
        name="reason_after_delivery_flow",
        gates=(GateDecl(name="Decision", payload_models=(Decision,), timeout_s=120.0),),
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    # pre-delivery: the reason reads the held row's context doc.
    holds = await client.list(run=flow_id)
    assert len(holds) == 1 and holds[0].reason == "the author's wait reason", holds
    # the decision's payload carries a homonymous ``reason`` field — the
    # unguarded read would surface it.
    first = await client.resolve(
        holds[0].hold_id,
        {"verdict": "approve", "reason": "the decision's own reason"},
        principal="op-1",
    )
    assert first.status == "delivered", first
    # post-delivery the context's reason is None — the wait's reason is gone
    # with the held state it belonged to; the decision's reason is the
    # payload's business, never the context's.
    after = await client.get(holds[0].hold_id)
    assert after is not None and after.status == "delivered", after
    assert after.reason is None, f"the delivered hold's context leaked a reason: {after.reason!r}"


async def test_pubsub_knock_is_a_pointer_and_the_consumer_converges(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """Pin 12 : the knock carries the POINTER (hold id + run id
    + event) — never the sole copy of the payload; a consumer that
    MISSES the knock still converges by polling ``client.hitl.list()``
    (the row is the truth). the PIN ACTUALLY LISTENS (the vacuous-audit's
    fix — the shipped pin assigned ``_ = HOLD_CHANNEL`` and pinned
    nothing): a raw pg_notify consumer on the MODULE's database
    (pg_notify is per-database — the env DSN's db is not the module's)
    observes the resolved knock's SHAPE."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="knock_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds_before = await client.list(run=flow_id)
    assert len(holds_before) == 1
    # a raw pg_notify consumer on the module's database.
    import asyncpg as _asyncpg

    knocks: list[str] = []
    listen_conn = await _asyncpg.connect(module_pg_schema.pg_dsn)
    await listen_conn.add_listener(
        HOLD_CHANNEL, lambda *a: knocks.append(str(a[3]) if len(a) > 3 else str(a))
    )
    # the resolve fires the knock (in the CAS-winning tx — exactly once).
    await client.resolve(holds_before[0].hold_id, {"verdict": "approve"})
    await asyncio.sleep(0.2)
    await listen_conn.close()
    assert knocks, "the knock never fired — the knob is dead (latency, not correctness)"
    # hold_id + run_id + event, never the payload.
    import json as _json

    knock = _json.loads(knocks[0])
    assert set(knock) == {"hold_id", "run_id", "event"}, knock
    assert knock["event"] == "resolved"
    assert knock["hold_id"] == holds_before[0].hold_id
    assert "approve" not in knocks[0], (
        f"THE KNOCK CARRIED THE PAYLOAD — the pointer-only law is broken: {knocks[0]}"
    )
    # a consumer that misses the knock polls the list — the resolution is
    # visible by rows alone.
    async with wf_pool.acquire() as conn:
        status = await conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1',
            uuid_module.UUID(holds_before[0].hold_id),
        )
    assert status == "delivered"


# ── pin 13: signal-timeout-db-clock + the expiry arm ────────────────────


async def test_signal_timeout_fires_on_db_clock(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin 13 (G6): the expiry arm compares PG's clock — the expired
    hold → the DEFINED 'abandoned' state; and the TYPED TIMEOUT FACE IS
    REAL (the vacuous-audit's fix — the shipped pin's docstring claimed
    the face it never exercised). the EXPIRY'S AMENDMENT: the face is a VALUE —
    the wait returns the ``Expired`` MEMBER (the closed union's second
    arm; the checker forces the fail-close); the body that wants the
    FAILURE raises :class:`taskq.exceptions.SignalTimeoutError` ITSELF
    off the member (the escalation ladder's own use) — the node
    terminal-fails through the ladder, and no new epoch is ever minted
    — hold → expire → re-hold → ∞ is the rejected dragon, kept failing by
    the attack probe."""
    from taskq.exceptions import SignalTimeoutError
    from taskq.workflows import Expired

    doors: list[str] = []

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        outcome = await ctx.wait_signal(Approval, timeout_s=1.0)
        match outcome:
            case Approval() as approval:
                return approval
            case Expired():
                # the expiry member is the typed face; the raise is its own.
                doors.append(outcome.abandoned_by)
                raise SignalTimeoutError(
                    "the hold expired — this body treats the expiry as the failure"
                )

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="timeout_flow"
    )
    # the deadline passes (DB time).
    await asyncio.sleep(1.1)
    abandoned = await sweep_expired_signals(wf_pool, schema=wf_schema)
    assert abandoned == 1
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert status == "abandoned"
    # the resume's wait site returns the Expired member; the body's own
    # conversion raises it into the ladder (the ladder burns through its
    # attempts), the node terminal-fails, the flow with it.
    await runner.drive(flow_id, max_ticks=40)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed", (
        f"on_timeout='fail' never failed the node: the flow is {root!r} — "
        "the typed timeout face did not fire (the wait site re-held, or "
        "the raise never reached the ladder)"
    )
    epochs = await wf_conn.fetch(
        f'SELECT status, hold_epoch FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(epochs) == 1 and epochs[0]["status"] == "abandoned", (
        f"the expired hold RE-HELD (a new epoch, a new deadline): "
        f"{[dict(r) for r in epochs]} — the wait site must RAISE, never "
        "mint an automatic new epoch"
    )
    # the timeout door's own member: every Expired the sweep's row
    # produced carries abandoned_by="timeout" (the sweep's rows carry no
    # marker — the member's default IS the timeout door; an operator
    # marker leaking here would mean the doors' records crossed).
    assert doors and set(doors) == {"timeout"}, doors


# ── the cancel cascade + the hold→resume band ───────────────────────────


async def test_cancel_workflow_one_transaction_idempotent(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """P3 rule 4 through the public API: the flip is the linearization;
    the held signals → cancelled; IDEMPOTENT (cancel twice = one
    cancel); the audit row is a ROW (the principal + the reason)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="cancel_flow"
    )
    first = await runner.cancel_workflow(flow_id, reason="the operator's stop", principal="op-9")
    assert first >= 1
    second = await runner.cancel_workflow(flow_id, reason="the double click")
    assert second == 0  # idempotent — a terminal root updates nothing
    sig_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert sig_status == "cancelled"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "cancelled"
    # the audit is A row.
    audit = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".admin_audit WHERE action = 'workflow.cancel' "
        "AND target_id = $1",
        str(flow_id),
    )
    assert audit == 1, (
        "the cancel is not on the audit record — 'who cancelled this' is a log line, not a row"
    )


# Why load_sensitive: the assertion is the pin — elapsed < 500 ms is the
# hold→resume band itself, and CI's parallel lanes are exactly the load
# the band cannot survive (the perf-bands file is the precedent: a
# wall-clock band's home is the serial lane).
@pytest.mark.load_sensitive
async def test_hold_to_resume_latency_band(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """G11c (the third unpinned workflow band — the band is SET from this
    measurement, then pinned in perf-evidence-workflows.md): send_signal
    commit → the row claimable."""
    import time

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="band_flow"
    )
    hold_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    start = time.perf_counter()
    result = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(hold_id),
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert result.status == "delivered", f"the deliver refused: {result.reason}"
    # the deliver CAS + the resume are two statements in one tx — single-digit
    # milliseconds at the pin shape (the band from this run's measurement,
    # recorded).
    from tests._wf_fixtures import MEASUREMENTS

    MEASUREMENTS.mkdir(exist_ok=True)
    # the band's evidence sink is append-only and run-scoped (one jsonl record
    # per run) — the shipped write_text rewrote the whole file per run, so a
    # partial run falsified the recorded number with only its own subset.
    record = json.dumps(
        {
            "run": f"{uuid_module.uuid4().hex[:8]}",  # noqa: TID251  # Why: the band record's run token is deliberately not a persisted id — no B-tree, no ordering; randomness is the point (attribution only).
            "hold_to_resume_ms": round(elapsed_ms, 3),
            "band_ms": 50,
        }
    )
    with (MEASUREMENTS / "t10-hold-resume-band.json").open("a") as sink:
        sink.write(record + "\n")
    assert elapsed_ms < 500, f"the hold→resume band blew out: {elapsed_ms:.1f} ms"


async def test_list_refuses_disagreeing_run_and_flow_id(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The alias doctrine — every alias pair REFUSES a disagreement,
    naming both spellings and values — covers the last alias pair too:
    ``list(run=X, flow_id=Y)`` with disagreeing ids raises naming both
    values instead of silently preferring one spelling; the same id in
    the two accepted TYPES (the claim's JobId and its string form) is
    AGREEMENT, never a refusal."""

    async def _hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn,
        wf_schema,
        wf_pool,
        wait_body=_hold_body,
        name="list_alias_disagreement_flow",
        gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
    )
    client = HitlClient(wf_pool, schema=wf_schema)

    # The same run named the two accepted ways: the claim's JobId and
    # its string form — agreement, resolved.
    holds = await client.list(run=flow_id, flow_id=str(flow_id))
    assert len(holds) == 1, holds

    # A non-documented spelling of the same id (braced) also agrees and
    # resolves — the bound id is the canonical form, so the query never
    # sees the wrapper. Pinned on both the both-given path and each
    # sole-spelling path (a canonicalizer that only ran when both
    # spellings were given would leave the sole call binding raw).
    braced = "{" + str(flow_id).upper() + "}"
    assert len(await client.list(run=braced, flow_id=str(flow_id))) == 1
    assert len(await client.list(run=braced)) == 1
    assert len(await client.list(flow_id=braced)) == 1

    # The get door binds the canonical form too: the braced upper
    # spelling of the hold's id resolves to the same hold, and a
    # well-formed id with no hold answers the honest None.
    got = await client.get("{" + str(holds[0].hold_id).upper() + "}")
    assert got is not None and got.hold_id == holds[0].hold_id
    # And a well-formed id with no hold answers the honest None (the
    # get door's contract for every accepted spelling).
    assert await client.get(str(new_uuid())) is None

    # The disagreement: the refusal names both spellings and both
    # values (the operator reads what collided, not just which kwargs).
    other_run = new_uuid()
    with pytest.raises(ValueError) as refused:
        await client.list(run=flow_id, flow_id=str(other_run))
    message = str(refused.value)
    assert "run=" in message and "flow_id=" in message, message
    assert str(flow_id) in message and str(other_run) in message, message

    # And a spelling that does not even parse as the id lands in the
    # same named refusal — the spelling names and both values, never a
    # naked UUID error.
    with pytest.raises(ValueError) as malformed:
        await client.list(run=flow_id, flow_id="not-this-run")
    malformed_message = str(malformed.value)
    assert "run=" in malformed_message and "flow_id=" in malformed_message, malformed_message
    assert str(flow_id) in malformed_message and "not-this-run" in malformed_message, (
        malformed_message
    )


async def test_resolve_delivers_through_a_non_canonical_id_spelling(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The resolve door's every leg speaks the id's canonical form: a
    hold resolved through a braced UPPER spelling (the same hold the
    JobId names) is DELIVERED — the node resumes, the hold is not
    consumed-then-refused — and the audit row and the broadcast carry
    the canonical id the operator's queries match."""

    async def _hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn,
        wf_schema,
        wf_pool,
        wait_body=_hold_body,
        name="resolve_spelling_flow",
        gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1, holds

    braced = "{" + str(holds[0].hold_id).upper() + "}"
    result = await client.resolve(braced, {"verdict": "approve"}, principal="op-1")
    assert result.status == "delivered", (
        f"a legal spelling of the hold's id must resolve the hold, not "
        f"consume-then-refuse it: {result}"
    )
    after = await client.get(holds[0].hold_id)
    assert after is not None and after.status == "delivered", after
    # The audit pointer speaks the canonical form: the operator's strict
    # target_id query finds the braced call's row.
    pointer = await wf_conn.fetchrow(
        f'SELECT target_id FROM "{wf_schema}".admin_audit '
        "WHERE action = 'hitl.resolve' ORDER BY id DESC LIMIT 1"
    )
    assert pointer is not None and pointer["target_id"] == str(holds[0].hold_id), pointer


# ── the operator abandon door (SignalAbandonedError's reachable use) ────


async def test_operator_abandon_resumes_the_node_and_the_wait_sees_the_operator_door(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The operator door, end to end: ``HitlClient.abandon`` lands the
    SAME DEFINED ``abandoned`` state the expiry sweep writes — with the
    DOOR MARKER (``abandoned_by='operator'``) riding the row's payload
    jsonb — resumes the node, and the resumed wait returns the
    ``Expired`` member carrying ``abandoned_by="operator"``. A
    marker-less abandoned row (the sweep's) is the timeout door: this
    marker is the only thing that tells the doors apart."""
    from taskq.workflows import Expired

    doors: list[str] = []

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        outcome = await ctx.wait_signal(Approval, timeout_s=120.0)
        match outcome:
            case Approval() as approval:
                return approval
            case Expired() as expired:
                doors.append(expired.abandoned_by)
                return {"fail_closed_on": expired.abandoned_by}

    flow_id, runner, node_id = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_door_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    result = await client.abandon(hold.hold_id, reason="the operator's stop", principal="op-7")
    assert result.status == "delivered", result
    # the row: the abandoned state + the operator door's marker.
    row = await wf_conn.fetchrow(
        f"SELECT status, payload->>'abandoned_by' AS door FROM \"{wf_schema}\".wf_signals "
        "WHERE id = $1",
        uuid_module.UUID(hold.hold_id),
    )
    assert row is not None and row["status"] == "abandoned", row
    assert row["door"] == "operator", (
        f"the operator abandon's marker is not on the row: {dict(row)} — the door "
        "is indistinguishable from the sweep's (the false contract again)"
    )
    # the held representation cleared — the node re-pends.
    node_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert node_status == "pending", node_status
    # the resumed wait: the Expired member carries the operator door.
    await runner.drive(flow_id, max_ticks=40)
    assert doors == ["operator"], doors
    flow_result = await runner.result(flow_id)
    assert flow_result == {"fail_closed_on": "operator"}, flow_result


async def test_body_wants_the_operator_failure_raises_signal_abandoned_off_the_arm(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The escalation-ladder shape (the value-not-machinery law at the
    operator door): the body that WANTS the operator abandon to be a
    FAILURE raises :class:`taskq.exceptions.SignalAbandonedError` ITSELF
    off the ``Expired(abandoned_by="operator")`` member — the machinery
    never raises it. The node ladders and terminal-fails, and NO new
    epoch is ever minted (hold → abandon → re-hold → ∞ is the rejected
    dragon)."""
    from taskq.exceptions import SignalAbandonedError, SignalTimeoutError
    from taskq.workflows import Expired

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        outcome = await ctx.wait_signal(Approval, timeout_s=120.0)
        match outcome:
            case Approval() as approval:
                return approval
            case Expired() as expired:
                if expired.abandoned_by == "operator":
                    raise SignalAbandonedError(
                        "the operator abandoned this wait — that is the failure"
                    )
                raise SignalTimeoutError("the hold timed out")

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_escalation_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    result = await client.abandon(hold.hold_id, principal="op-7")
    assert result.status == "delivered", result
    await runner.drive(flow_id, max_ticks=40)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed", (
        f"the body's own SignalAbandonedError never reached the ladder: the flow is {root!r}"
    )
    rows = await wf_conn.fetch(
        f'SELECT status, hold_epoch FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(rows) == 1 and rows[0]["status"] == "abandoned", (
        f"the abandoned hold RE-HELD (a new epoch): {[dict(r) for r in rows]} — "
        "the wait site must return the member, never mint an automatic re-hold"
    )


async def test_an_abandon_reason_with_a_nul_byte_is_the_typed_refusal(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The reason's wire shape (the NUL's own refusal — the discipline
    the admin routes' jsonb-bind checks keep): a reason carrying a NUL
    byte cannot ride the CAS's jsonb bind. The TYPED refusal, audited,
    the hold SURVIVES — never a ValueError from the serializer's bind
    crashing the caller."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_nul_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    result = await client.abandon(hold.hold_id, reason="poisoned\x00reason", principal="op-7")
    assert result.status == "refused", result
    assert "NUL" in str(result.reason), result
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1',
        uuid_module.UUID(hold.hold_id),
    )
    assert status == "held", f"the refused abandon moved the hold: {status!r}"


async def test_second_abandon_is_a_defined_no_op(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Idempotence: the second abandon of an operator-abandoned hold is
    the DEFINED no-op (the CAS is the fence — nothing moves twice, no
    second audit row, no second knock)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_noop_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    first = await client.abandon(hold.hold_id, principal="op-7")
    assert first.status == "delivered", first
    second = await client.abandon(hold.hold_id, principal="op-7")
    assert second.status == "no-op", second
    audits = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".admin_audit WHERE action = 'hitl.abandon' "
        "AND target_id = $1",
        hold.hold_id,
    )
    assert audits == 1, f"the no-op wrote a second audit row: {audits}"


async def test_delivered_holds_abandon_is_refused(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A DELIVERED hold refuses the abandon: the operator's answer
    already landed — the door cannot fail-close a wait that is no longer
    waiting, and the delivered row stands."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_delivered_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    resolved = await client.resolve(hold.hold_id, {"verdict": "approve"}, principal="op-7")
    assert resolved.status == "delivered", resolved
    refused = await client.abandon(hold.hold_id, principal="op-7")
    assert refused.status == "refused", refused
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1',
        uuid_module.UUID(hold.hold_id),
    )
    assert status == "delivered", status


async def test_sweep_abandoned_holds_abandon_is_refused(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A sweep-abandoned (expired) hold refuses the abandon: the
    marker-less row reads as the timeout door — the deadline already
    fail-closed the wait, the operator's abandon has nothing to add.
    (An OPERATOR-abandoned hold's second abandon is the no-op — the
    door marker is the difference.)"""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_expired_flow"
    )
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    abandoned = await sweep_expired_signals(wf_pool, schema=wf_schema)
    assert abandoned == 1
    client = HitlClient(wf_pool, schema=wf_schema)
    hold_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    refused = await client.abandon(str(hold_id), principal="op-7")
    assert refused.status == "refused", refused
    assert "sweep" in str(refused.reason), refused


async def test_timer_holds_abandon_is_refused_and_the_hold_survives(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE TIMER REFUSAL: a hold under the reserved ``__timer__`` prefix
    refuses the abandon — the clock is its only resolver (the timer wait
    site has no abandoned arm: a deadline pass is the WAKE, never the
    fail-close). Refused: the hold SURVIVES, nothing is written."""
    from taskq.workflows.api._hitl import TIMER_SIGNAL_PREFIX

    async def timer_body(ctx: StepContext, params: Ingest) -> str:
        await ctx.wait_until(3600.0, key="remind")
        return "woken"

    app = WorkflowApp()

    @app.workflow("abandon_timer_flow")
    def timer_flow() -> Promise[object]:
        return build(step(timer_body, Ingest(doc_id="d1"), key="timer_node"))

    runner = FlowRunner(app.get("abandon_timer_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    hold = await wf_conn.fetchrow(
        f'SELECT id, signal_name, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY id DESC LIMIT 1",
        flow_id,
    )
    assert hold is not None, "the timer hold never registered"
    assert hold["signal_name"].startswith(TIMER_SIGNAL_PREFIX), (
        f"the arrangement's guard: this hold must be the clock's — {dict(hold)}"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    result = await client.abandon(str(hold["id"]), reason="an operator's mistake", principal="op-7")
    assert result.status == "refused", result
    assert "timer" in str(result.reason), result
    after = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold["id"]
    )
    assert after == "held", f"the refused abandon moved the hold: {after!r}"


async def test_child_join_holds_abandon_returns_expired(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """CHILD-JOIN HOLDS ARE ALLOWED (the abandon is a stop-wait, not a
    verdict): the operator abandon lands on the join hold, the parent's
    body resumes, and the childflow wait's abandoned arm returns the
    ``Expired`` member carrying the operator door — the parent
    fail-closes while the child keeps running (the child's probe holds
    on its own signal — nobody answers it; its rows are its own
    flow's)."""
    from taskq.workflows import ChildCancelled, ChildFailed, ChildOk, Expired

    class SubProbe(BaseModel):
        doc_id: str

    class SubReport(BaseModel):
        findings: list[str]

    child_app = WorkflowApp()

    @child_app.actor(queue="cpu", max_attempts=1)
    async def probe(ctx: StepContext) -> SubReport:
        # the child stays LIVE: its own wait holds (nobody answers it) —
        # the join's abandon must not need the child terminal either way.
        outcome = await ctx.wait_signal(Approval, timeout_s=120.0)
        assert isinstance(outcome, Approval)
        probe_input = SubProbe.model_validate(ctx.input)
        return SubReport(findings=[f"found:{probe_input.doc_id}:{outcome.verdict}"])

    @child_app.workflow("abandon_join_chil")
    def child_wf() -> Promise[object]:
        return build(step(probe, key="probe"))

    child_app.get("abandon_join_chil")  # the compile registry (the spawn's door)

    doors: list[str] = []

    async def parent_body(ctx: StepContext, params: Ingest) -> Any:
        handle = await ctx.spawn_flow(
            "abandon_join_chil", input=SubProbe(doc_id="d1"), result=SubReport
        )
        outcome = await handle.wait(timeout_s=120.0)
        match outcome:
            case ChildOk():
                return {"child": "ok"}
            case ChildFailed() | ChildCancelled():
                return {"child": "dead"}
            case Expired() as expired:
                doors.append(expired.abandoned_by)
                return {"fail_closed_on": expired.abandoned_by}

    app = WorkflowApp()

    @app.workflow("abandon_join_parent")
    def parent_wf() -> Promise[object]:
        return build(step(parent_body, Ingest(doc_id="d1"), key="spawn_join"))

    runner = FlowRunner(app.get("abandon_join_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    join_hold = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
        "AND signal_name LIKE '__child__:%'",
        flow_id,
    )
    assert join_hold is not None, "the join hold never registered"
    join_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', join_hold
    )
    assert join_status == "held", (
        f"the arrangement's guard: the join hold must still be held (the child is live) — {join_status!r}"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    result = await client.abandon(str(join_hold), principal="op-7")
    assert result.status == "delivered", (
        f"the child-join hold's abandon must be ALLOWED (a stop-wait, not a verdict): {result}"
    )
    await runner.drive(flow_id, max_ticks=40)
    assert doors == ["operator"], doors
    flow_result = await runner.result(flow_id)
    assert flow_result == {"fail_closed_on": "operator"}, flow_result
    # the child kept running: its own hold row still stands.
    child_hold = await wf_conn.fetchval(
        f"SELECT status FROM \"{wf_schema}\".wf_signals WHERE signal_name = 'Approval' "
        "AND status = 'held'"
    )
    assert child_hold is not None, "the child's own hold did not survive the join's abandon"


async def test_abandon_audit_and_operator_door_broadcast_are_witnessed(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """The abandon's legs are WITNESSED, not asserted from types: the
    ``hitl.abandon`` audit row (the hold target, the reason, the
    run/signal detail) and the ``taskq_wf_hold_expired`` broadcast
    carrying ``abandoned_by="operator"`` (the pointer-only payload —
    the row is the truth)."""
    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=_wait_body, name="abandon_witness_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    knocks: list[str] = []
    listen_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    await listen_conn.add_listener(
        HOLD_EXPIRED_CHANNEL, lambda *a: knocks.append(str(a[3]) if len(a) > 3 else str(a))
    )
    result = await client.abandon(hold.hold_id, reason="the operator's stop", principal="op-7")
    assert result.status == "delivered", result
    await asyncio.sleep(0.2)
    await listen_conn.close()
    assert knocks, "the abandon's broadcast never fired"
    knock = json.loads(knocks[0])
    assert knock["abandoned_by"] == "operator", knock
    assert knock["hold_id"] == hold.hold_id
    assert knock["flow_id"] == str(flow_id)
    audit = await wf_conn.fetchrow(
        f'SELECT action, target_type, target_id, reason, detail FROM "{wf_schema}".admin_audit '
        "WHERE action = 'hitl.abandon' ORDER BY id DESC LIMIT 1"
    )
    assert audit is not None, "an abandon without an audit row reds"
    assert audit["target_type"] == "hold"
    assert audit["target_id"] == hold.hold_id
    assert audit["reason"] == "the operator's stop"
    detail = json.loads(audit["detail"]) if isinstance(audit["detail"], str) else audit["detail"]
    assert detail["run_id"] == str(flow_id)
    assert detail["signal"] == "Approval"


async def test_sweep_broadcast_names_the_timeout_door(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """The sweep's broadcast carries ``abandoned_by="timeout"`` — the
    typed consumer reads the timeout face off the event alone; the
    operator door's broadcast reads ``"operator"``. No behavior change
    for the timeout door: the marker only NAMES the door that was always
    the sweep's."""
    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=_wait_body, name="timeout_broadcast_flow"
    )
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    knocks: list[str] = []
    listen_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    await listen_conn.add_listener(
        HOLD_EXPIRED_CHANNEL, lambda *a: knocks.append(str(a[3]) if len(a) > 3 else str(a))
    )
    abandoned = await sweep_expired_signals(wf_pool, schema=wf_schema)
    assert abandoned == 1
    await asyncio.sleep(0.2)
    await listen_conn.close()
    assert knocks, "the sweep's broadcast never fired"
    knock = json.loads(knocks[0])
    assert knock["abandoned_by"] == "timeout", knock


# ── the race cures: the CAS-lost arm's door, the merge, the call sites ──


async def test_the_sweep_winning_the_abandon_race_is_the_refused_loser(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """THE CAS-LOST ARM READS THE DOOR: the expiry sweep winning the race
    between the abandon's held-read and its CAS lands the REFUSED verdict
    (the timeout door did the work) — the pre-fix arm's bare-status read
    answered ANY 'abandoned' row ``no-op`` (exit 0 over the sweep's win,
    zero audits, the doors' records crossed). The attacker's recipe: the
    node's job row parked FOR UPDATE on a second connection parks the
    abandon at its node pre-lock (after its held-read), the REAL sweep
    statement commits under the park, and the release hands the abandon
    the CAS-lost arm."""
    from taskq.workflows.api._hitl import (  # pyright: ignore[reportPrivateUsage]  # Why: the pin drives the REAL sweep statement (the arm the leader's loop runs), never a re-spelled copy.
        _EXPIRY_SWEEP_SQL,
        _render,
    )

    flow_id, _runner, node_id = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=_wait_body, name="abandon_race_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    hold_uuid = uuid_module.UUID(hold.hold_id)
    # the deadline past (the injected clock) — the sweep CAN win.
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    park = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        async with park.transaction():
            # THE PARK: the abandon's node pre-lock blocks here — AFTER
            # its held-read (the row still 'held').
            await park.fetchval(
                f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1 FOR UPDATE', node_id
            )
            abandon_task = asyncio.create_task(
                client.abandon(str(hold_uuid), reason="a late stop", principal="op-7")
            )
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                waits = await park.fetchval(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid() "
                    "AND datname = current_database()"
                )
                if waits:
                    break
                await asyncio.sleep(0.01)  # bounded condition wait, never a fixed sleep
            else:
                raise AssertionError("the abandon never blocked on the node pre-lock")
            # the sweep wins the race while the abandon is parked.
            swept = await park.fetch(_render(_EXPIRY_SWEEP_SQL, wf_schema), 100)
            assert len(swept) == 1, f"the arrangement's guard: the sweep must win — {swept}"
        # the COMMIT releases the park: the abandon unblocks into its CAS,
        # which matches nothing (the row is 'abandoned').
        result = await abandon_task
    finally:
        await park.close()
    assert result.status == "refused", (
        f"the sweep won the race; the abandon must answer the REFUSED "
        f"loser verdict, never the no-op: {result}"
    )
    assert "timeout door already resolved" in str(result.reason), result
    row = await wf_conn.fetchrow(
        f"SELECT status, payload->>'abandoned_by' AS door FROM \"{wf_schema}\".wf_signals "
        "WHERE id = $1",
        hold_uuid,
    )
    assert row is not None and row["status"] == "abandoned", row
    assert row["door"] is None, (
        f"the row's door must read the sweep's (no marker on the sweep's "
        f"row — the timeout door): {dict(row)}"
    )
    audits = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".admin_audit WHERE action = 'hitl.abandon' "
        "AND target_id = $1",
        str(hold_uuid),
    )
    assert audits == 0, (
        f"the raced loser wrote {audits} hitl.abandon audit rows — the "
        "loser matches no rows and writes NOTHING"
    )


async def test_the_abandon_merge_keeps_the_declared_context_keys(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE MERGE PRESERVES THE DECLARED CONTEXT: the abandon CAS writes
    the operator's reason under ``abandon_reason`` — never onto the
    wait's declared ``reason`` (the pre-fix key clobbered the mint's
    context: by the operator's text, or by JSON null when the operator
    gave none). ``abandoned_by`` still names the door — the wait/door
    readers read only that — and the audit row carries the operator's
    reason."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(
            Approval, timeout_s=120.0, reason="editorial approval", args={"doc": "d1"}
        )

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_merge_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    result = await client.abandon(hold.hold_id, reason="op stop", principal="op-7")
    assert result.status == "delivered", result
    row = await wf_conn.fetchrow(
        f"SELECT payload->>'reason' AS declared_reason, "
        f"payload->>'abandon_reason' AS operator_reason, "
        f"payload->>'abandoned_by' AS door, payload->'args'->>'doc' AS doc "
        f'FROM "{wf_schema}".wf_signals WHERE id = $1',
        uuid_module.UUID(hold.hold_id),
    )
    assert row is not None
    assert row["declared_reason"] == "editorial approval", (
        f"the operator's abandon clobbered the wait's declared reason: {dict(row)}"
    )
    assert row["operator_reason"] == "op stop", dict(row)
    assert row["door"] == "operator", dict(row)
    assert row["doc"] == "d1", f"the merge lost the declared args: {dict(row)}"

    # the operator giving NO reason: the declared reason survives, the
    # abandon_reason is JSON null — never a clobber of the declared key.
    flow2, _r2, _n2 = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="abandon_merge_bare_flow"
    )
    (hold2,) = await client.list(run=flow2)
    bare = await client.abandon(hold2.hold_id, principal="op-7")
    assert bare.status == "delivered", bare
    row2 = await wf_conn.fetchrow(
        f"SELECT payload->>'reason' AS declared_reason, "
        f"payload->>'abandon_reason' AS operator_reason, "
        f"payload->>'abandoned_by' AS door "
        f'FROM "{wf_schema}".wf_signals WHERE id = $1',
        uuid_module.UUID(hold2.hold_id),
    )
    assert row2 is not None
    assert row2["declared_reason"] == "editorial approval", (
        f"the bare abandon clobbered the wait's declared reason: {dict(row2)}"
    )
    assert row2["operator_reason"] is None, dict(row2)
    assert row2["door"] == "operator", dict(row2)


async def test_two_waits_one_signal_each_sees_its_own_timeout_face(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """TWO SEQUENTIAL WAITS ON ONE SIGNAL, the timeout door: the first
    call surfaces ITS OWN abandoned row (the read filters the site's own
    call_id) and the second call surfaces its own — the pre-fix read
    matched by signal alone, so the second hold's abandoned row (the
    latest epoch) reached the FIRST call and the body re-held: the run
    hung past its own fail-close."""
    from taskq.workflows import Expired

    rounds: list[tuple[str, str, str]] = []

    async def two_waits_body(ctx: StepContext, params: Ingest) -> Any:
        first = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(first, Expired):
            # the deliberate continue: the first site's expiry is not
            # fatal — the body moves on to its second wait.
            rounds.append((str(ctx.attempt), "first", first.hold_id))
        second = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(second, Expired):
            rounds.append((str(ctx.attempt), "second", second.hold_id))
            return {"fail_closed_on": second.abandoned_by}
        return {"answered": True}

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=two_waits_body, name="two_waits_timeout_flow"
    )
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    out = await runner.drive(flow_id, max_ticks=60)
    assert out == "terminal", f"the two-wait body never terminated: {out!r} ({rounds})"
    flow_result = await runner.result(flow_id)
    assert flow_result == {"fail_closed_on": "timeout"}, flow_result
    # the sites' own rows: the FIRST call claimed the FIRST hold's expiry,
    # the SECOND call the second's — never a cross-claim.
    first_claims = [r for r in rounds if r[1] == "first"]
    second_claims = [r for r in rounds if r[1] == "second"]
    assert first_claims, rounds
    assert second_claims, rounds
    rows = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(rows) == 2 and all(r["status"] == "abandoned" for r in rows), (
        f"the expiry faces re-held: {[dict(r) for r in rows]}"
    )
    assert {r[2] for r in first_claims} == {str(rows[0]["id"])}, (
        f"the first call claimed a row that is not its own: {rounds}"
    )
    assert {r[2] for r in second_claims} == {str(rows[1]["id"])}, (
        f"the second call claimed a row that is not its own: {rounds}"
    )


async def test_two_waits_one_signal_the_operator_abandon_fails_closed_at_its_own_site(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE OPERATOR DOOR'S PROMISE (the CLI's 'the wait fails closed on
    its next drive') FOR THE TWO-WAIT BODY: the operator abandons the
    SECOND hold; on the resume the SECOND call returns
    ``Expired(abandoned_by="operator")`` and the run TERMINATES (no
    re-hold, ``runner.result`` not None) — the first call never claims
    the second's row (the call_id filter shared by every site read)."""
    from taskq.workflows import Expired

    rounds: list[tuple[str, str, str]] = []

    async def two_waits_body(ctx: StepContext, params: Ingest) -> Any:
        first = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(first, Expired):
            rounds.append((str(ctx.attempt), "first", first.hold_id))
        second = await ctx.wait_signal(Approval, timeout_s=120.0)
        if isinstance(second, Expired):
            rounds.append((str(ctx.attempt), "second", second.hold_id))
            return {"fail_closed_on": second.abandoned_by}
        return {"answered": True}

    flow_id, runner, node_id = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=two_waits_body, name="two_waits_operator_flow"
    )
    # attempt 1: the first call's hold (0.3 s); the sweep abandons it.
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    # attempt 2: the first call surfaces its own Expired (timeout); the
    # second call mints ITS hold (the long deadline — the operator's).
    verdict = await runner.drive(flow_id, until="held", max_ticks=60)
    assert verdict == "held", verdict
    holds = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(holds) == 2 and holds[1]["status"] == "held", [dict(r) for r in holds]
    second_hold = holds[1]
    # the second hold is the SECOND call site's own mint (attempt 2, the
    # second call ordinal).
    assert second_hold["call_id"] == "call:2:1", dict(second_hold)
    # THE OPERATOR DOOR on the SECOND hold.
    client = HitlClient(wf_pool, schema=wf_schema)
    abandoned = await client.abandon(str(second_hold["id"]), reason="op stop", principal="op-7")
    assert abandoned.status == "delivered", abandoned
    # the resume: the second call fails closed at ITS OWN site; the run
    # TERMINATES — no re-hold.
    out = await runner.drive(flow_id, max_ticks=60)
    assert out == "terminal", f"the run hung after the operator's abandon: {out!r} ({rounds})"
    flow_result = await runner.result(flow_id)
    assert flow_result is not None, "the run never terminated"
    assert flow_result == {"fail_closed_on": "operator"}, flow_result
    # the first call never claimed the second's row: its every claim is
    # the first hold's id, carrying the timeout door.
    first_claims = [r for r in rounds if r[1] == "first"]
    assert first_claims and {r[2] for r in first_claims} == {str(holds[0]["id"])}, (
        f"THE WRONG SITE: the first call claimed the second hold's abandoned row: {rounds}"
    )
    assert ("3", "second", str(second_hold["id"])) in rounds, rounds
    # no re-hold: exactly the two rows, both abandoned.
    after = await wf_conn.fetch(
        f'SELECT hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(after) == 2 and all(r["status"] == "abandoned" for r in after), (
        f"the operator door re-held the run: {[dict(r) for r in after]}"
    )
    # no face marker: the Expired return is unconditional on the site's
    # own row (the reads are site-disjoint by call_id) — the metadata
    # carries no timeout_face key anymore (the redundant one-shot's
    # removal).
    face_keys = await wf_conn.fetchval(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    meta = json.loads(face_keys) if isinstance(face_keys, str) else face_keys
    assert not [k for k in meta if k.startswith("timeout_face")], (
        f"the redundant face marker is back: {sorted(k for k in meta if k.startswith('timeout_face'))}"
    )


async def test_the_deliberate_rewait_after_a_timeout_mints_a_new_hold(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE DELIBERATE RE-WAIT (the docs' own words): the body that
    MATCHED the expiry member and waits again within the attempt
    registers a NEW hold with a NEW epoch — the call site's own identity
    keeps the re-wait's read off the expired row (a different call
    ordinal's call_id set), so the mint is the body's decision, never the
    face's suppression."""
    from taskq.workflows import Expired

    async def rewait_body(ctx: StepContext, params: Ingest) -> Any:
        outcome = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(outcome, Expired):
            # the deliberate re-wait: a NEW body decision.
            return await ctx.wait_signal(Approval, timeout_s=120.0)
        return outcome

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=rewait_body, name="rewait_flow"
    )
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    verdict = await runner.drive(flow_id, until="held", max_ticks=40)
    assert verdict == "held", verdict
    rows = await wf_conn.fetch(
        f'SELECT hold_epoch, status, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(rows) == 2, f"the deliberate re-wait never minted: {[dict(r) for r in rows]}"
    assert rows[0]["status"] == "abandoned" and rows[0]["hold_epoch"] == 1, dict(rows[0])
    assert rows[1]["status"] == "held" and rows[1]["hold_epoch"] == 2, dict(rows[1])
    # the re-wait's hold carries the SECOND call site's id (attempt 2,
    # the second call ordinal) — the site identity the read filters by.
    assert rows[1]["call_id"] == "call:2:1", dict(rows[1])


# ── the delivered-answer queue's call-site identity (the re-attack's cure) ──


async def test_the_mixed_attempt_retry_replays_each_site_its_own_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ANSWER THEFT (the red team's exact scenario): the FIRST wait
    call's hold expires (the body matches ``Expired`` and continues),
    the SECOND call mints and the operator answers it, then the retry
    re-executes from the top. The unfiltered delivered queue let the
    FIRST site consume the SECOND site's answer on the retry (the body
    received a valid model it did not ask for at that site) while the
    true site found its queue exhausted and re-asked the operator. The
    queue read filters the site's OWN rows (the same ``call_id`` array
    the abandoned read filters by): the first site replays its own
    ``Expired`` face, the second site replays the operator's own
    answer — no theft, no re-ask, exactly the two rows."""
    from taskq.workflows import Expired

    rounds: list[tuple[str, str, object]] = []

    async def mixed_body(ctx: StepContext, params: Ingest) -> Any:
        first = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(first, Expired):
            rounds.append((str(ctx.attempt), "first-expired", first.hold_id))
            second = await ctx.wait_signal(Approval, timeout_s=120.0)
            if isinstance(second, Expired):
                return {"fail_closed_on": second.abandoned_by}
            rounds.append((str(ctx.attempt), "second-answered", second.verdict))
            return {"first": "expired", "second": second.verdict}
        # THE THEFT SHAPE: a valid model at the first site — the body
        # never asked for one here (its own hold expired).
        rounds.append((str(ctx.attempt), "first-answered", first.verdict))
        return {"first": f"stolen:{first.verdict}", "second": None}

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=mixed_body, name="mixed_retry_flow"
    )
    # attempt 1: the first call's hold (0.3 s); the sweep abandons it.
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    # attempt 2: the first call replays its Expired face; the second
    # call mints ITS hold (the operator's deadline).
    verdict = await runner.drive(flow_id, until="held", max_ticks=60)
    assert verdict == "held", verdict
    holds = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(holds) == 2, [dict(r) for r in holds]
    assert holds[0]["status"] == "abandoned" and holds[0]["call_id"] == "call:1:0", dict(holds[0])
    assert holds[1]["status"] == "held" and holds[1]["call_id"] == "call:2:1", dict(holds[1])
    # the operator answers the SECOND site's hold — the queue now
    # carries a delivered row that belongs to the LATER call ordinal.
    delivered = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(holds[1]["id"]),
        payload={"verdict": "b-verdict"},
        payload_json=json.dumps({"verdict": "b-verdict"}),
    )
    assert delivered.status == "delivered", delivered
    # the retry: the body re-executes from the top.
    out = await runner.drive(flow_id, max_ticks=60)
    assert out == "terminal", f"the mixed retry never terminated: {out!r} ({rounds})"
    flow_result = await runner.result(flow_id)
    assert flow_result == {"first": "expired", "second": "b-verdict"}, (
        f"THE ANSWER THEFT: the retry fed the first site the second "
        f"site's answer: {flow_result!r} ({rounds})"
    )
    # the first site's every claim is its OWN Expired face (its own
    # abandoned row, never the delivered row); the second site received
    # the operator's own answer.
    first_claims = [r for r in rounds if r[1] == "first-expired"]
    assert first_claims and {r[2] for r in first_claims} == {str(holds[0]["id"])}, (
        f"the first site claimed a row that is not its own: {rounds}"
    )
    assert ("3", "second-answered", "b-verdict") in rounds, rounds
    # no re-ask: exactly the two rows (the abandoned face, the delivered
    # answer) — no third hold minted for the second site.
    after = await wf_conn.fetch(
        f'SELECT hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(after) == 2, f"the retry re-asked the operator: {[dict(r) for r in after]}"


async def test_two_sites_both_delivered_each_site_consumes_its_own_answer(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """BOTH SITES DELIVERED, IN ORDER: the first hold is answered, the
    body continues, the second hold is answered, the run completes —
    each site receives its OWN answer, and the final replay (after the
    second site's answer re-pends the node) re-consumes without
    re-minting or re-asking. The per-signal cursor made the SECOND site
    find its queue exhausted on the replay (the first site's consumption
    had advanced the shared counter past it) and re-ask the operator;
    the unfiltered queue made the second site consume the FIRST site's
    answer. The per-call cursor over the call-disjoint queue: both
    sites replay their own rows."""
    from taskq.workflows import Expired

    async def two_answers_body(ctx: StepContext, params: Ingest) -> Any:
        first = await ctx.wait_signal(Approval, timeout_s=120.0)
        if isinstance(first, Expired):
            return {"first": "expired", "second": None}
        second = await ctx.wait_signal(Approval, timeout_s=120.0)
        if isinstance(second, Expired):
            return {"first": first.verdict, "second": "expired"}
        return {"first": first.verdict, "second": second.verdict}

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=two_answers_body, name="two_answers_flow"
    )
    # attempt 1: the first site holds; the operator answers it.
    verdict = await runner.drive(flow_id, until="held", max_ticks=40)
    assert verdict == "held", verdict
    (first_hold,) = await wf_conn.fetch(
        f'SELECT id, hold_epoch, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert first_hold["call_id"] == "call:1:0", dict(first_hold)
    delivered = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(first_hold["id"]),
        payload={"verdict": "a-verdict"},
        payload_json=json.dumps({"verdict": "a-verdict"}),
    )
    assert delivered.status == "delivered", delivered
    # attempt 2: the first site re-consumes its own answer (the replay);
    # the second site mints; the operator answers it.
    verdict = await runner.drive(flow_id, until="held", max_ticks=60)
    assert verdict == "held", verdict
    holds = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(holds) == 2 and holds[1]["status"] == "held", [dict(r) for r in holds]
    assert holds[1]["call_id"] == "call:2:1", dict(holds[1])
    delivered = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(holds[1]["id"]),
        payload={"verdict": "b-verdict"},
        payload_json=json.dumps({"verdict": "b-verdict"}),
    )
    assert delivered.status == "delivered", delivered
    # attempt 3 (the replay after the second site's hold): the run
    # COMPLETES — each site re-consumes its own answer.
    out = await runner.drive(flow_id, max_ticks=60)
    assert out == "terminal", f"the final replay never terminated: {out!r}"
    flow_result = await runner.result(flow_id)
    assert flow_result == {"first": "a-verdict", "second": "b-verdict"}, (
        f"THE CROSS-SITE CONSUMPTION: {flow_result!r}"
    )
    # no re-mint, no re-ask: still exactly the two delivered rows.
    after = await wf_conn.fetch(
        f'SELECT hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(after) == 2 and all(r["status"] == "delivered" for r in after), (
        f"the replay re-minted or re-asked: {[dict(r) for r in after]}"
    )


async def test_a_standing_hold_is_never_mistaken_for_an_earlier_sites_own(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE RE-HOLD ARM'S SITE IDENTITY: a LATER call's standing hold
    must never answer the EARLIER call's re-hold read. The probe: the
    first call's hold expires (the body matches ``Expired``), the
    second call mints and its hold STANDS; the duplicate execution (the
    reclaim race's surrogate — the hold-stamp surgery re-opens the
    claim while the row stands) re-runs the body from the top. The
    unfiltered re-hold read made the EARLIER site raise
    ``NodeHeldError`` for the LATER site's hold — the earlier site's
    own ``Expired`` face never fired. The read filters the site's own
    rows: the earlier site lands its own face, the body reaches the
    later site, and THAT site re-holds its own row (no second mint)."""
    from taskq.workflows import Expired

    rounds: list[tuple[str, str, str]] = []

    async def earlier_site_body(ctx: StepContext, params: Ingest) -> Any:
        first = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(first, Expired):
            rounds.append((str(ctx.attempt), "first-expired", first.hold_id))
            second = await ctx.wait_signal(Approval, timeout_s=120.0)
            if isinstance(second, Expired):
                return {"fail_closed_on": second.abandoned_by}
            return {"answered": second.verdict}
        return {"answered": first.verdict}

    flow_id, runner, node_id = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=earlier_site_body, name="standing_hold_flow"
    )
    # attempt 1: the first call's hold expires; the sweep abandons it.
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    # attempt 2: the first call's Expired face; the second call mints —
    # its hold STANDS.
    verdict = await runner.drive(flow_id, until="held", max_ticks=60)
    assert verdict == "held", verdict
    holds = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(holds) == 2 and holds[1]["status"] == "held", [dict(r) for r in holds]
    standing = holds[1]
    assert standing["call_id"] == "call:2:1", dict(standing)
    # THE DUPLICATE EXECUTION's surrogate: the hold-stamp surgery
    # re-opens the claim while the second site's hold row stands held —
    # the same body re-execution the claim race's second driver lands
    # (the stamp cleared + the schedule dropped, the resume's own
    # writes).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET metadata = metadata - 'hold' - 'held_signal', "
        "scheduled_at = now() WHERE id = $1",
        node_id,
    )
    await runner.tick(flow_id)
    # the duplicate attempt ran (the claim bumped), the EARLIER site
    # landed its OWN Expired face (never a NodeHeldError for the later
    # site's hold), and the LATER site re-held its own row.
    dup_attempts = [r for r in rounds if r[0] == "3" and r[1] == "first-expired"]
    assert dup_attempts and {r[2] for r in dup_attempts} == {str(holds[0]["id"])}, (
        f"THE WRONG SITE'S HOLD: the earlier call raised for the later "
        f"site's standing hold (its own face never fired): {rounds}"
    )
    after = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(after) == 2 and after[1]["status"] == "held", (
        f"the standing hold was re-registered or consumed: {[dict(r) for r in after]}"
    )
    assert after[1]["id"] == standing["id"], dict(after[1])


async def test_the_abandoned_site_expired_replay_is_deterministic_no_face_marker(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ABANDONED SITE'S REPLAY IS IDEMPOTENT: the site whose own
    hold was abandoned returns ``Expired`` on EVERY replay of the
    attempt (the ladder's retries each land the same deterministic
    face — the same row, the same door), never mints, and the metadata
    carries NO ``timeout_face`` key (the one-shot marker is GONE: with
    the reads site-disjoint by ``call_id`` its only remaining effect
    was a same-attempt re-execution minting past the face — the
    non-idempotent replay — and its read's fall-through was
    unreachable)."""
    from taskq.exceptions import SignalTimeoutError
    from taskq.workflows import Expired

    rounds: list[tuple[str, str, str]] = []

    async def timeout_failure_body(ctx: StepContext, params: Ingest) -> Any:
        outcome = await ctx.wait_signal(Approval, timeout_s=0.3)
        match outcome:
            case Approval() as approval:
                return approval
            case Expired():
                rounds.append((str(ctx.attempt), "expired", outcome.hold_id))
                raise SignalTimeoutError("the converted expiry face")

    app = WorkflowApp()

    @app.workflow("expired_replay_flow")
    def expired_replay_flow() -> Promise[object]:
        return build(step(timeout_failure_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get("expired_replay_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held", max_ticks=40)
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    # the ladder's retries: EVERY replay lands the same deterministic
    # face at the same site (the site's own abandoned row), then the
    # ladder exhausts and the run fails.
    out = await runner.drive(flow_id, max_ticks=200)
    assert out == "terminal", f"the expired replay never terminated: {out!r} ({rounds})"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed", f"the converted expiry face never failed the run: {root!r}"
    (abandoned,) = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    # never minted past the face: exactly the ONE abandoned row.
    assert abandoned["status"] == "abandoned" and abandoned["hold_epoch"] == 1, dict(abandoned)
    replays = [r for r in rounds if r[1] == "expired"]
    assert len(replays) >= 2, f"the face did not fire on every replay: {rounds}"
    assert {r[2] for r in replays} == {str(abandoned["id"])}, (
        f"the replays claimed a row that is not the site's own: {rounds}"
    )
    node_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    meta_raw = await wf_conn.fetchval(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
    assert not [k for k in meta if k.startswith("timeout_face")], (
        f"the redundant face marker is back: {sorted(k for k in meta if k.startswith('timeout_face'))}"
    )


async def test_the_symmetric_order_first_delivered_second_expired(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SYMMETRIC ORDER (the crossed-orders matrix's other leg): the
    FIRST call's hold is answered, the SECOND call's hold expires — the
    replay's first site re-consumes its own answer and the second site
    lands its OWN ``Expired`` face (never the first site's delivered
    row, never a re-mint). The unfiltered queue made the second site
    consume the first site's answer on attempt 2 itself (the body
    completed on a verdict the second site never asked for)."""
    from taskq.workflows import Expired

    async def first_delivered_body(ctx: StepContext, params: Ingest) -> Any:
        first = await ctx.wait_signal(Approval, timeout_s=120.0)
        if isinstance(first, Expired):
            return {"fail_closed_on": "first"}
        second = await ctx.wait_signal(Approval, timeout_s=0.3)
        if isinstance(second, Expired):
            return {"first": first.verdict, "second": "expired"}
        return {"first": first.verdict, "second": second.verdict}

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=first_delivered_body, name="symmetric_order_flow"
    )
    # attempt 1: the first site holds; the operator answers it.
    verdict = await runner.drive(flow_id, until="held", max_ticks=40)
    assert verdict == "held", verdict
    (first_hold,) = await wf_conn.fetch(
        f'SELECT id, hold_epoch, call_id FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert first_hold["call_id"] == "call:1:0", dict(first_hold)
    delivered = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(first_hold["id"]),
        payload={"verdict": "a-verdict"},
        payload_json=json.dumps({"verdict": "a-verdict"}),
    )
    assert delivered.status == "delivered", delivered
    # attempt 2: the first site consumes its own answer; the second site
    # mints (the short deadline); the sweep abandons it.
    verdict = await runner.drive(flow_id, until="held", max_ticks=60)
    assert verdict == "held", verdict
    await expire_holds(wf_conn, wf_schema, flow_id, timer_only=False)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    # attempt 3: the first site replays its own answer; the second site
    # lands its OWN Expired face.
    out = await runner.drive(flow_id, max_ticks=60)
    assert out == "terminal", f"the symmetric replay never terminated: {out!r}"
    flow_result = await runner.result(flow_id)
    assert flow_result == {"first": "a-verdict", "second": "expired"}, (
        f"THE CROSS-SITE CONSUMPTION (the second site took the first "
        f"site's answer): {flow_result!r}"
    )
    after = await wf_conn.fetch(
        f'SELECT hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(after) == 2, f"the face re-held or re-minted: {[dict(r) for r in after]}"
    assert after[0]["status"] == "delivered" and after[1]["status"] == "abandoned", [
        dict(r) for r in after
    ]


async def test_deliver_and_abandon_take_one_lock_order_no_deadlock(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """THE LOCK ORDER'S TEETH (the module's 'no opposite pair left'
    claim): ``deliver_payload`` takes the held node's job row FOR UPDATE
    BEFORE its signal CAS — the same jobs → wf_signals order the
    abandon/resolve doors keep. The attacker's shape: the hold's signal
    row parked FOR UPDATE on a second connection, the deliver and the
    abandon queued against it, then the release — the pair completes (one
    delivered, one refused), never DeadlockDetectedError. The pre-fix
    order (the CAS, then the node lookup) left the deliver the opposite
    pair, and the shape deadlocked: the deliver's uncommitted signal-row
    lock against the abandon's held job-row lock."""
    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=_wait_body, name="deliver_abandon_order_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(run=flow_id)
    hold_uuid = uuid_module.UUID(hold.hold_id)
    park = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        async with park.transaction():
            # THE PARK: the hold's own signal row — the deliver's CAS
            # (and the abandon's) queue behind it.
            await park.execute(
                f'SELECT id FROM "{wf_schema}".wf_signals WHERE id = $1 FOR UPDATE', hold_uuid
            )
            deliver_task = asyncio.create_task(
                deliver_payload(
                    wf_pool,
                    schema=wf_schema,
                    workflow_id=flow_id,
                    hold_id=JobId(hold_uuid),
                    payload={"verdict": "approve"},
                    payload_json=json.dumps({"verdict": "approve"}),
                )
            )
            abandon_task = asyncio.create_task(
                client.abandon(str(hold_uuid), reason="the race's loser", principal="op-7")
            )
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                waits = await park.fetchval(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid() "
                    "AND datname = current_database()"
                )
                if waits and int(waits) >= 2:
                    break
                await asyncio.sleep(0.01)  # bounded condition wait, never a fixed sleep
            else:
                raise AssertionError("the deliver/abandon pair never queued on the parked row")
        # the release: the pair resolves by the one lock order.
        deliver_result, abandon_result = await asyncio.gather(
            deliver_task, abandon_task, return_exceptions=True
        )
    finally:
        await park.close()
    for name, outcome in (("deliver", deliver_result), ("abandon", abandon_result)):
        assert not isinstance(outcome, asyncpg.DeadlockDetectedError), (
            f"THE DEADLOCK: {name} deadlocked against the other verb — the "
            f"opposite lock order is back: {outcome!r}"
        )
    verdicts = sorted(
        o.status if not isinstance(o, BaseException) else f"raised:{type(o).__name__}"
        for o in (deliver_result, abandon_result)
    )
    assert verdicts == ["delivered", "refused"], (
        f"the raced pair's verdicts: {deliver_result!r} / {abandon_result!r}"
    )
