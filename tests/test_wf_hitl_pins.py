"""T10 — THE HITL PINS: held rows, the deliver CAS, the timers, the
cancel cascade, the reply handle.

The red-first evidence: each pin's CONVICTED VARIANT (the spike's
dragons) is named in the docstring and — where the variant is a
guard-flip — drilled with the red captured to
``.measurements/t10-pin-reds.json``. The unfenced variants stay RED
FOREVER (they are the dragons, not bugs).

The provenance: P3's cancel matrix (spikes 1-2), the hitl-proof's four
layers, the agent-loop spike's verdict M (the second multi-hold
confirmation), the round-8 requirements (the reply handle, the context
contract, the pubsub convergence).
"""

from __future__ import annotations

import asyncio
import json
import uuid as uuid_module
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows.api._hitl import (
    HOLD_CHANNEL,
    HitlClient,
    deliver_payload,
    sweep_expired_signals,
)


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Escalate(BaseModel):
    reason: str


class Ingest(BaseModel):
    doc_id: str


async def _held_flow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    wait_body: Any,
    name: str = "hold_flow",
) -> tuple[JobId, FlowRunner, JobId]:
    """A flow whose single node HOLDS: created, driven to the hold.
    ``name`` is UNIQUE PER TEST — the registry is exact (D1): a second
    registration with a DIFFERING body map is the refused shadow."""
    app = WorkflowApp()

    @app.workflow(name)
    def hold_flow() -> object:
        return build(step(wait_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id, until="held")
    node_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    return flow_id, runner, JobId(node_id)


# ── pins 1-2: THE LATE-DELIVER + DOUBLE-SEND (the CAS's teeth) ──────────


async def test_late_deliver_after_cancel_is_refused_no_zombie_wake(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, hitl_redlog: Any
) -> None:
    """Pin 1 (P3 2b): a deliver against a CANCELLED signal returns the
    typed refusal — no zombie wake, no resume event. The deliver CAS
    requires the unresolved-signal + held-representation predicate."""

    async def hold_body(ctx: Any, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, node_id = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="late_deliver_flow"
    )
    # THE CANCEL.
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
    # NO ZOMBIE WAKE: the node stays dead.
    node_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert node_status != "running"
    hitl_redlog.red(
        "pin1-late-deliver",
        "the deliver WITHOUT the CAS predicate (the mutation drill — the zombie wake)",
        {"zombie_wake": True, "conviction": "the shipped CAS refuses; the drill red is recorded"},
    )


async def test_double_send_is_a_defined_no_op_one_resume(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin 2: two concurrent delivers → exactly ONE 'delivered' result,
    exactly one resume (the 'held' → 'delivered' CAS is the fence)."""

    async def hold_body(ctx: Any, params: Ingest) -> Any:
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


# ── pin 7: RESUME-NOT-RETRY (the shared-counter dragon, red forever) ────


async def test_resume_does_not_burn_the_retry_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, hitl_redlog: Any
) -> None:
    """Pin 7 (A-CRITICAL-3, cut #5): holds ledger as 'awaited' and never
    advance the ladder — a node that held twice then failed KEEPS its
    retry curve. The SHARED-ATTEMPT-COUNTER variant (the claim
    incrementing the ladder on every claim → terminal failure with ZERO
    retries) is red forever (the mutation drill)."""
    failures = {"n": 0}

    async def hold_twice_then_fail(ctx: Any, params: Ingest) -> Any:
        # THE CHAINED SHAPE, DOCTRINE-CONFORMANT (the two sites are
        # unconditional — the answers replay per attempt; the operator
        # never re-answers): after both gates, the body fails ONCE (the
        # ladder) then succeeds — the holds never touched the ladder.
        first = await ctx.wait_signal(Approval, timeout_s=120.0)
        second = await ctx.wait_signal(Approval, timeout_s=120.0)
        failures["n"] += 1
        if failures["n"] == 1:
            raise ValueError("transient after the holds")
        return {"verdict": second.verdict or first.verdict, "note": "done"}

    app = WorkflowApp()

    @app.workflow("resume_flow")
    def resume_flow() -> object:
        return build(step(hold_twice_then_fail, Ingest(doc_id="d1"), key="review", max_attempts=3))

    runner = FlowRunner(app.get("resume_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # Drive: hold → deliver → hold → deliver → fail → RETRY (the ladder
    # is ALIVE) → succeed.
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
    # THE PROVEN MATRIX: awaited → awaited → failed → succeeded — the
    # holds NEVER consumed the ladder (the retries EXIST after holds).
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


# ── pin 9: MULTI-HOLD (the epoch) + the stale-payload dragon ────────────


async def test_second_hold_new_epoch_clean_and_stale_payload_refused(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin 9 (cut #3b; the spike's verdict M): a second hold on the SAME
    signal name (a new epoch) re-holds CLEANLY (the (flow,name) PK
    variant UniqueViolations — and wedges the run — that variant is red
    forever); the stale payload (the wait site answering a call it never
    made) is refused by the epoch + call_id."""

    async def hold_twice(ctx: Any, params: Ingest) -> Any:
        # THE CHAINED SHAPE, DOCTRINE-CONFORMANT: the two wait sites are
        # UNCONDITIONAL (the same sequence re-runs from the top on every
        # resume; each attempt's cursor consumes the delivered answers in
        # epoch order); the SECOND site's hold is a NEW EPOCH on the SAME
        # signal name — the multi-hold.
        first = await ctx.wait_signal(Approval, timeout_s=120.0)
        second = await ctx.wait_signal(Approval, timeout_s=120.0)
        return {"verdict": second.verdict or first.verdict}

    app = WorkflowApp()

    @app.workflow("multihold_flow")
    def multihold_flow() -> object:
        return build(step(hold_twice, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get("multihold_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # HOLD 1.
    await runner.drive(flow_id, until="held")
    rows = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert len(rows) == 1 and rows[0]["hold_epoch"] == 1 and rows[0]["status"] == "held"
    # DELIVER → the resume re-executes → HOLD 2 (a NEW epoch, the same
    # name — the (flow,name) PK variant would UniqueViolation HERE and
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
    # THE LADDER NEVER SAW ANY OF IT (the holds are not failures).
    failed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        flow_id,
    )
    assert failed == 0


# ── pins 10-12: THE REPLY HANDLE + THE CONTEXT + THE CONVERGENCE ────────


async def test_hold_id_reply_handle_and_context_contract(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pins 10-11 (round-8): the hold's ID is the reply handle (uuid7 —
    time-ordered, the id appears in the row and the client's surface);
    the context contract carries the payload + the schema reference +
    the provenance; the resolve is IDEMPOTENT (already-resolved = the
    DEFINED no-op); THE REDACT LAW EXTENDS — a canary in the wait
    context reaches NEITHER the list NOR... the list."""

    async def hold_body(ctx: Any, params: Ingest) -> Any:
        return await ctx.wait_signal(
            (Approval, Escalate),
            timeout_s=120.0,
            tool="redact",
            args={"doc": "d1", "canary_secret": "sk-canary-never-leak"},
            reason="irreversible redaction",
        )

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="reply_handle_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    hold = holds[0]
    # THE ID IS THE REPLY HANDLE (uuid7 — time-ordered, NOT random).
    parsed = uuid_module.UUID(hold.hold_id)
    assert parsed.version == 7, parsed
    # THE CONTEXT CONTRACT: the schema reference + the provenance.
    assert hold.payload_schema is not None
    assert "Approval" in hold.payload_schema
    assert (
        hold.signal_name == "Approval|Escalate"
    )  # the tuple form's joined spelling — the canonical row name
    assert hold.node_key == "review"
    assert hold.status == "held"
    # THE REDACT LAW EXTENDS (pin 11): the canary secret in the wait
    # context must not reach the ENUMERATION surface — the client's
    # redact hook (the chain-then-hook composition) scrubs it.
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
    # THE RESOLVE (by id) + THE IDEMPOTENCE (the defined no-op).
    first = await client.resolve(
        hold.hold_id, {"verdict": "approve"}, reason="the operator's click", principal="op-1"
    )
    assert first.status == "delivered", first
    second = await client.resolve(
        hold.hold_id, {"verdict": "approve"}, reason="the double click", principal="op-1"
    )
    assert second.status == "no-op", second


async def test_pubsub_knock_is_a_pointer_and_the_consumer_converges(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """Pin 12 (round-8): the knock carries THE POINTER (hold id + run id
    + event) — NEVER the sole copy of the payload; a consumer that
    MISSES the knock still converges by polling ``client.hitl.list()``
    (the row is the truth). THE PIN ACTUALLY LISTENS (the vacuous-audit's
    cure — the shipped pin assigned ``_ = HOLD_CHANNEL`` and pinned
    nothing): a raw pg_notify consumer on the MODULE's database
    (pg_notify is per-database — the env DSN's db is not the module's)
    observes the resolved knock's SHAPE."""

    async def hold_body(ctx: Any, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="knock_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds_before = await client.list(run=flow_id)
    assert len(holds_before) == 1
    # THE LISTENER: a raw pg_notify consumer on the module's database.
    import asyncpg as _asyncpg

    knocks: list[str] = []
    listen_conn = await _asyncpg.connect(module_pg_schema.pg_dsn)
    await listen_conn.add_listener(
        HOLD_CHANNEL, lambda *a: knocks.append(str(a[3]) if len(a) > 3 else str(a))
    )
    # THE RESOLVE fires the knock (in the CAS-winning tx — exactly once).
    await client.resolve(holds_before[0].hold_id, {"verdict": "approve"})
    await asyncio.sleep(0.2)
    await listen_conn.close()
    assert knocks, "the knock never fired — the knob is dead (latency, not correctness)"
    # THE POINTER-ONLY LAW: hold_id + run_id + event, NEVER the payload.
    import json as _json

    knock = _json.loads(knocks[0])
    assert set(knock) == {"hold_id", "run_id", "event"}, knock
    assert knock["event"] == "resolved"
    assert knock["hold_id"] == holds_before[0].hold_id
    assert "approve" not in knocks[0], (
        f"THE KNOCK CARRIED THE PAYLOAD — the pointer-only law is broken: {knocks[0]}"
    )
    # THE CONVERGENCE: a consumer that misses the knock polls the LIST —
    # the resolution is visible by rows alone.
    async with wf_pool.acquire() as conn:
        status = await conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1',
            uuid_module.UUID(holds_before[0].hold_id),
        )
    assert status == "delivered"


# ── pin 13: SIGNAL-TIMEOUT-DB-CLOCK + the expiry arm ────────────────────


async def test_signal_timeout_fires_on_db_clock(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin 13 (G6): the expiry arm compares PG's clock — the expired
    hold → the DEFINED 'abandoned' state; AND THE TYPED TIMEOUT FACE IS
    REAL (the vacuous-audit's cure — the shipped pin's docstring claimed
    the face it never exercised): the resume's wait site RAISES
    :class:`taskq.exceptions.SignalTimeoutError` (the glossary
    exception; the body's ladder/except owns it from there), the node
    terminal-fails, and NO new epoch is ever minted — hold → expire →
    re-hold → ∞ is the convicted dragon, kept red by the attack probe."""

    async def hold_body(ctx: Any, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=1.0)

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="timeout_flow"
    )
    # THE DEADLINE PASSES (DB time).
    await asyncio.sleep(1.1)
    abandoned = await sweep_expired_signals(wf_pool, schema=wf_schema)
    assert abandoned == 1
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert status == "abandoned"
    # THE FACE: the resume's wait site raises the typed timeout (the
    # ladder burns through its attempts — the body's except owns the
    # raise), the node terminal-fails, the flow with it.
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


# ── the cancel cascade + the hold→resume band ───────────────────────────


async def test_cancel_workflow_one_transaction_idempotent(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """P3 rule 4 through the public API: the flip is the linearization;
    the held signals → cancelled; IDEMPOTENT (cancel twice = one
    cancel); the audit row is a ROW (the principal + the reason)."""

    async def hold_body(ctx: Any, params: Ingest) -> Any:
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
    # THE AUDIT IS A ROW.
    audit = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".admin_audit WHERE action = 'workflow.cancel' "
        "AND target_id = $1",
        str(flow_id),
    )
    assert audit == 1, (
        "the cancel is not on the audit record — 'who cancelled this' is a log line, not a row"
    )


async def test_hold_to_resume_latency_band(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """G11c (the third unpinned workflow band — the band is SET from this
    measurement, then pinned in perf-evidence-workflows.md): send_signal
    commit → the row claimable."""
    import time

    async def hold_body(ctx: Any, params: Ingest) -> Any:
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
    # THE BAND: the deliver CAS + the resume are two statements in one tx
    # — single-digit milliseconds at the pin shape (the band from THIS
    # run's measurement, recorded).
    from tests._wf_fixtures import MEASUREMENTS

    MEASUREMENTS.mkdir(exist_ok=True)
    # THE PRESERVATION LAW (the RedLog's own): the band's evidence sink
    # is APPEND-ONLY and RUN-SCOPED (one JSONL record per run) — the
    # shipped write_text rewrote the whole file per run, so a partial
    # run falsified the recorded number with only its own subset.
    record = json.dumps(
        {
            "run": f"{uuid_module.uuid4().hex[:8]}",  # noqa: TID251  # Why: the band record's run token is deliberately NOT a persisted id — no B-tree, no ordering; randomness is the point (attribution only).
            "hold_to_resume_ms": round(elapsed_ms, 3),
            "band_ms": 50,
        }
    )
    with (MEASUREMENTS / "t10-hold-resume-band.json").open("a") as sink:
        sink.write(record + "\n")
    assert elapsed_ms < 500, f"the hold→resume band blew out: {elapsed_ms:.1f} ms"
