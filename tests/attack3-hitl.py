"""ATTACK-3 (phase-3 red team) — T10's HITL surface: the hold identity
under concurrent deliver, the epoch on a retried hold-site, the redact
chain on the list() surface, the knock's pointer-only law, the typed
door's runtime boundary, the timeout face, the driver's held check.

Each probe states the contract it attacks (the module docstrings' own
words). RED = the observed half contradicts it. Captured to
.measurements/attack3/.
"""

from __future__ import annotations

import asyncio
import uuid as uuid_module
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.api._hitl import (
    HOLD_CHANNEL,
    HitlClient,
    sweep_expired_signals,
)


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Strict(BaseModel):
    decision: str


class Lenient(BaseModel):
    """All-optional: ANY dict validates into it (the confusion's door)."""

    model_config = {"extra": "ignore"}

    a: str = "default"
    b: str = "default"


class Ingest(BaseModel):
    doc_id: str


async def _held_flow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    wait_body: Any,
    name: str,
) -> tuple[JobId, FlowRunner, JobId]:
    app = WorkflowApp()

    @app.workflow(name)
    def hold_flow() -> Promise[object]:
        return build(step(wait_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    return flow_id, runner, JobId(node_id)


# ── A3-H1: the timeout face is DEAD — an expired hold re-holds forever ──
# The pin's own docstring: "the node resumes (the body re-runs and the
# wait site raises the typed timeout face)" — on_timeout="fail" →
# SignalTimeoutError. The shipped wait_signal NEVER raises it: past the
# queue's end it finds no 'held' row (the sweep set it 'abandoned') and
# mints a NEW hold with a NEW deadline. Hold → expire → re-hold → …
# the workflow never times out.


async def test_a3_expired_hold_reholds_forever_no_timeout_face(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=0.3)

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_timeout_flow"
    )
    await asyncio.sleep(0.4)
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1
    # THE RESUME: the body re-runs. The CONTRACT: the wait site raises
    # the typed timeout face; the node terminal-fails.
    await runner.drive(flow_id, max_ticks=30)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    epochs = await wf_conn.fetch(
        f'SELECT status, hold_epoch FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY hold_epoch",
        flow_id,
    )
    assert root == "failed", (
        f"on_timeout='fail' NEVER failed the node: the flow is {root!r}; "
        f"the signals are {[dict(r) for r in epochs]} — the expired hold "
        "re-registered with a NEW epoch and a NEW deadline: the timeout "
        "face is dead, the hold/expire cycle is infinite"
    )


# ── A3-H2: the DOUBLE concurrent resolve — the audit is NOT exactly-once ─
# The deliver CAS makes the RESUME exactly-once (one 'delivered', one
# 'no-op') — but resolve() writes the audit row + fires the knock in the
# GUARD tx (the row still 'held'), and the deliver CAS runs AFTER that tx
# commits. Two concurrent resolves both pass the guard → TWO audit rows,
# TWO 'resolved' knocks for ONE resolution.


async def test_a3_double_resolve_audit_not_exactly_once(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_double_resolve_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    hold_id = holds[0].hold_id
    # THE DOUBLE: two operators hit the same hold simultaneously.
    results = await asyncio.gather(
        client.resolve(hold_id, {"verdict": "approve"}, principal="op-a"),
        client.resolve(hold_id, {"verdict": "approve"}, principal="op-b"),
    )
    statuses = sorted(r.status for r in results)
    assert statuses == ["delivered", "no-op"], (
        f"the double resolve's verdicts are {statuses} — the CAS itself "
        "did not hold the exactly-once line"
    )
    audits = await wf_conn.fetch(
        f'SELECT principal_subject, detail FROM "{wf_schema}".admin_audit '
        f"WHERE action = 'hitl.resolve' AND target_id = $1",
        hold_id,
    )
    assert len(audits) == 1, (
        f"ONE resolution wrote {len(audits)} audit rows "
        f"({[dict(a) for a in audits]}) — the audit surface is NOT "
        "exactly-once: the guard tx does not own the transition (the "
        "deliver CAS runs after the guard commits), so a concurrent "
        "resolve passes the same 'held' guard twice"
    )


# ── A3-H3: the canary in tool args reaches the client list() RAW ────────
# The module law: "a canary secret in tool args must no more reach the
# client list than the capture row" and "REDACT-BEFORE-PERSIST". The
# shipped wait_signal persists reason/tool/args RAW (no chain, no hook);
# HitlClient redacts ONLY at read time and ONLY when the constructor got
# a hook — and NOTHING in the runner/client wires the workflow's own
# redact hook into the client. The default surface leaks.


async def test_a3_hold_context_canary_leaks_through_list_by_default(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    canary = "sk-canary-a3-4f9d"

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(
            Approval,
            timeout_s=120.0,
            reason="the human must approve",
            tool="grant_refund",
            args={"account": canary, "amount": 100},
        )

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_canary_flow"
    )
    # THE DEFAULT SURFACE (the operator's client — no redact hook passed;
    # nothing in the estate wires one for them).
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    blob = repr(holds[0].payload)
    assert canary not in blob, (
        "THE CANARY REACHED THE CLIENT LIST RAW — the hold context "
        "(reason/tool/args) persists un-redacted and the default "
        "HitlClient applies no redact chain: REDACT-BEFORE-PERSIST does "
        f"NOT extend to the hold context: {blob[:200]}"
    )


# ── A3-H4: the knock is POINTER-ONLY (the confirming probe) ─────────────


async def test_a3_knock_is_pointer_only(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_knock_flow"
    )
    knocks: list[str] = []

    import json as _json

    from taskq.workflows.api._hitl import deliver_payload

    holds = await wf_conn.fetch(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    # THE LISTENER: a raw pg_notify consumer on the hold channel — on the
    # MODULE's database (pg_notify is per-database; the env DSN's db is
    # NOT the module's db — the attacker's own first listener missed it).
    listen_conn = await asyncpg.connect(module_pg_schema.pg_dsn)

    async def listener() -> None:
        await listen_conn.add_listener(HOLD_CHANNEL, lambda *a: knocks.append(str(a)))

    await listener()

    await listener()
    result = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(holds[0]["id"]),
        payload={"verdict": "approve", "note": "the payload that must NOT ride the knock"},
        payload_json=_json.dumps({"verdict": "approve"}),
    )
    await asyncio.sleep(0.2)
    assert result.status == "delivered", result
    assert len(knocks) >= 1, "the knock never fired (the knob is dead — latency, not correctness)"
    knock_blob = repr(knocks)
    assert "approve" not in knock_blob and "the payload that must NOT" not in knock_blob, (
        f"THE KNOCK CARRIED THE PAYLOAD — the pointer-only law is broken: {knock_blob[:300]}"
    )
    await listen_conn.close()


# ── A3-H5: the wrong-model payload is NOT refused at the boundary ───────
# The deliver docstring: "the smuggled Approval.model_construct(verdict=42)
# is refused at the boundary with the named error, the hold SURVIVES".
# The shipped boundary does not exist at runtime: HitlClient.resolve
# delivers ANY dict; the payload is consumed (delivered, cursor
# advanced) and the BODY's re-validation blows up — the ladder burns,
# the flow dies. The hold does NOT survive.


async def test_a3_wrong_model_payload_not_refused_at_boundary(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_wrong_model_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    # THE SMUGGLE: a payload that validates against NOTHING (Approval
    # requires 'verdict'; the smuggled dict lacks it).
    result = await client.resolve(holds[0].hold_id, {"verdict": 42, "note": "smuggled"})
    assert result.status == "refused", (
        f"the smuggled payload was {result.status!r} — the promised "
        "boundary refusal (validate → refuse → the hold SURVIVES) does "
        "not exist at runtime: the payload is consumed and the body "
        "will pay for it"
    )


# ── A3-H6: the union wait narrows to the FIRST model (type confusion) ───
# _coerce_signal iterates the declared models in order and returns the
# first that validates — a lenient first member swallows a strict
# second member's delivery: the body receives the WRONG type instance.


async def test_a3_union_wait_narrows_to_the_first_model(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    received: list[Any] = []

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        answer = await ctx.wait_signal((Lenient, Strict), timeout_s=120.0)
        received.append(answer)
        return answer

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_union_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    # The operator answered the STRICT shape.
    result = await client.resolve(holds[0].hold_id, {"decision": "the strict answer"})
    assert result.status == "delivered", result
    await runner.drive(flow_id, max_ticks=30)
    assert not received or not isinstance(received[0], Lenient), (
        f"the STRICT delivery was narrowed into the LENIENT first member "
        f"({type(received[0]).__name__ if received else 'none'}) — the "
        "union wait mis-narrows: _coerce_signal validates the payload "
        "against the models IN ORDER, not by shape"
    )


# ── A3-H7: drive(until="held") misses the ETERNAL hold ──────────────────
# _any_held counts 'pending' rows with scheduled_at > now() — a hold
# with timeout_s=None never sets a future scheduled_at (the deadline is
# NULL → the held representation keeps the old value): the driver never
# sees the hold and spins to max_ticks.


async def test_a3_drive_held_misses_the_eternal_hold(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=None)

    flow_id, runner, node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_eternal_flow"
    )
    node_row = await wf_conn.fetchrow(
        f"SELECT status, scheduled_at, metadata ? 'hold' AS held FROM \"{wf_schema}\".jobs "
        "WHERE id = $1",
        node,
    )
    verdict = await runner.drive(flow_id, until="held", max_ticks=4)
    assert verdict == "held", (
        f"drive(until='held') returned {verdict!r} on an ETERNAL hold "
        f"(the node is truly held: {dict(node_row) if node_row else None}) — "
        "_any_held only counts future scheduled_at rows: the W1-sanctioned "
        "explicit eternal wait is invisible to the driver"
    )


# ── A3-H8: the epoch on a RETRIED hold-site (the confirming probe) ──────
# The docs' v1 semantics: while a hold stands, a re-execution re-raises
# the SAME hold (idempotent re-hold — same epoch, no second row).


async def test_a3_retried_hold_site_reuses_the_standing_hold(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="a3_epoch_flow"
    )
    before = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',
        flow_id,
    )
    assert len(before) == 1
    # THE RETRY: re-drive (the worker's re-claim path — the body
    # re-executes from the top on the standing hold).
    verdict = await runner.drive(flow_id, until="held", max_ticks=10)
    after = await wf_conn.fetch(
        f'SELECT id, hold_epoch, status FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',
        flow_id,
    )
    assert verdict == "held" and len(after) == 1 and after[0]["id"] == before[0]["id"], (
        f"the retry minted a NEW hold ({len(after)} rows) — the "
        "idempotent re-hold broke: the standing hold must stand"
    )


_ = uuid_module
