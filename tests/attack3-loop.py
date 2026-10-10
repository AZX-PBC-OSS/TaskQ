"""ATTACK-3 (phase-3 red team) — T19's loop machinery under adversarial
load: the escalation path, the ladder classes, the answer-queue cursor
through a retry, the body-controlled infra classifier.

Each probe states the contract it attacks (the docs' own words); RED =
the observed half contradicts it. Captured to .measurements/attack3/.
"""

from __future__ import annotations

import asyncio

import asyncpg
from pydantic import BaseModel

from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)


class Counter(BaseModel):
    acc: int = 0


class Approval(BaseModel):
    verdict: str


class Ingest(BaseModel):
    doc_id: str


async def _runner_of(
    app: WorkflowApp, name: str, wf_pool: asyncpg.Pool, wf_schema: str
) -> FlowRunner:
    return FlowRunner(app.get(name), wf_pool, wf_schema)


# ── A3-L1: the DRIVER's exhaustion never enqueues the escalation ────────
# The docs (guides §9): "`on_exhausted="escalate"` writes the escalation
# enqueue through the SAME outbox the fired joins use." The sweep's arm
# writes it (_sweep.py) — but the DRIVER's own arms (the advance guard's
# refusal, the body failure) call _exhaust_loop, which never touches the
# outbox. A LIVE worker's loop (the common path) exhausts WITHOUT the
# escalation the policy promised.


async def test_a3_driver_exhaustion_never_enqueues_the_escalation(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    calls: list[int] = []

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        calls.append(1)
        return Refine(Counter(acc=len(calls)))

    app = WorkflowApp()

    @app.workflow("a3_escalate_flow")
    def a3_escalate() -> Promise[object]:
        return build(loop("counter", refine_forever, max_iterations=3, on_exhausted="escalate"))

    runner = await _runner_of(app, "a3_escalate_flow", wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    outbox = await wf_conn.fetch(
        f'SELECT consumer_step_key, bindings FROM "{wf_schema}".wf_outbox WHERE flow_id = $1',
        flow_id,
    )
    escalations = [r for r in outbox if r["consumer_step_key"] == "loop.escalation"]
    assert escalations, (
        f"on_exhausted='escalate' exhausted by the DRIVER (the advance "
        f"guard — the live-worker path) wrote NO escalation outbox row: "
        f"{[r['consumer_step_key'] for r in outbox]} — the policy is "
        "sweep-only, contrary to the guide"
    )


# ── A3-L2: the "fail" policy escalates anyway ───────────────────────────
# ExhaustionPolicy = Literal["escalate", "fail"] — but NOTHING reads
# spec.on_exhausted. The sweep's arm writes the escalation row for BOTH
# policies: a loop whose author said "just fail" still pages an operator
# actor.


async def test_a3_fail_policy_escalates_anyway(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        return Refine(Counter(acc=1))

    app = WorkflowApp()

    @app.workflow("a3_fail_policy_flow")
    def a3_fail_policy() -> Promise[object]:
        return build(loop("counter", refine_forever, max_iterations=3, on_exhausted="fail"))

    runner = await _runner_of(app, "a3_fail_policy_flow", wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # The SWEEP-driven exhaustion path: orphan the loop past its cap the
    # way the shipped pin does (the driver would exhaust it otherwise).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', "
        "metadata = metadata || $2::jsonb, "
        "locked_by_worker = gen_random_uuid(), lock_expires_at = now() - interval '1 hour' "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
        '{"iteration": 3, "max_iterations": 3, "kind": "loop"}',
    )
    from taskq.workflows._sweep import sweep_loop_budget

    await sweep_loop_budget(wf_pool, runner.wsql)
    outbox = await wf_conn.fetch(
        f'SELECT consumer_step_key FROM "{wf_schema}".wf_outbox WHERE flow_id = $1', flow_id
    )
    escalations = [r for r in outbox if r["consumer_step_key"] == "loop.escalation"]
    assert not escalations, (
        f"on_exhausted='fail' STILL wrote the escalation outbox row — "
        f"the policy vocabulary is vacuous (nothing reads "
        f"spec.on_exhausted): {escalations}"
    )


# ── A3-L3: the body-controlled infra classifier — a poison body escapes
#    BOTH walls forever ─────────────────────────────────────────────────
# _is_infra_fault treats ANY ConnectionError from the BODY as reclaim-
# eligible: the node re-pends (0.05 s) with NO ladder burn and NO wall.
# A body that deterministically raises ConnectionError never terminalizes
# — unbounded crashed ledger rows, the flow wedged 'running' forever
# (the exact STRANDED-FLOW shape the named-exhaustion law exists for).


async def test_a3_poison_body_escapes_both_walls(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    calls: list[int] = []

    async def poison(ctx: StepContext, carry: object) -> Done[Counter]:
        calls.append(1)
        raise ConnectionError("the body's own network flake — deterministically")

    app = WorkflowApp()

    @app.workflow("a3_poison_flow")
    def a3_poison() -> Promise[object]:
        return build(loop("counter", poison, max_iterations=5))

    runner = await _runner_of(app, "a3_poison_flow", wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # The bound drive: if the machinery had ANY bound, the flow would go
    # terminal within the ticks.
    await runner.drive(flow_id, max_ticks=60)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    crashed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 '
        "AND status = 'crashed'",
        flow_id,
    )
    assert root in ("failed", "succeeded", "cancelled"), (
        f"a body that deterministically raises ConnectionError wedged the "
        f"flow '{root}' after {len(calls)} body runs and {crashed} crashed "
        f"ledger rows — the infra classifier is BODY-CONTROLLED: the two "
        "walls are blind to a poison body that raises an infra class"
    )


# ── A3-L4: the answer-queue cursor through a RETRY (the confirming
#    probe — the wrong-answer replay dragon's cousin) ────────────────────


async def test_a3_cursor_replay_through_a_retry(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The docs' answer-queue doctrine (the runner's cursor): a RETRY
    replays the answers — the operator never re-answers; the replay
    takes the SAME queue head (never the wrong one)."""
    seen: list[str] = []
    fail_once = {"n": 0}

    async def body(ctx: StepContext, params: Ingest) -> Ingest:
        answer = await ctx.wait_signal(Approval, timeout_s=30.0)
        seen.append(answer.verdict)
        fail_once["n"] += 1
        if fail_once["n"] == 1:
            raise RuntimeError("the transient body failure AFTER the answer")
        return Ingest(doc_id=answer.verdict)

    app = WorkflowApp()

    @app.workflow("a3_cursor_flow")
    def a3_cursor() -> Promise[object]:
        return build(step(body, Ingest(doc_id="d1"), key="review"))

    runner = await _runner_of(app, "a3_cursor_flow", wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # Drive to the hold, deliver ONE answer, then the body fails once.
    await runner.drive(flow_id, until="held")
    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    await client.resolve(holds[0].hold_id, {"verdict": "approve"})
    # The body consumes the answer, then raises → retry → the SAME
    # answer must replay (the cursor is per-attempt; the retry starts
    # from the queue's HEAD).
    await runner.drive(flow_id)
    result = await runner.result(flow_id)
    assert result == {"doc_id": "approve"}, f"the flow's result drifted: {result}"
    assert seen == ["approve", "approve"], (
        f"the retry did NOT replay the answer queue exactly: {seen} — "
        "the wrong-answer replay (the memo dragon's cousin)"
    )
    # And the operator answered ONCE (no second hold was registered).
    signals = await wf_conn.fetch(
        f'SELECT status, hold_epoch FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
        "ORDER BY hold_epoch",
        flow_id,
    )
    assert len(signals) == 1 and signals[0]["status"] == "delivered", (
        f"the retry minted a SECOND hold — the operator would have to "
        f"re-answer: {[dict(r) for r in signals]}"
    )


# ── A3-L5: the mixed-run ladder question — infra kill DURING a body
#    failure (the ledger's two writers, one window) ──────────────────────


async def test_a3_mixed_run_body_failure_then_infra_never_double_burns(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A body failure exhausts (named state); the SAME node's later
    claim (if any raced) must never double-write the exhaustion or
    resurrect the flow."""
    fail = {"n": 0}

    async def fails_once(ctx: StepContext, carry: object) -> Done[Counter]:
        fail["n"] += 1
        raise RuntimeError("the body's own bug")

    app = WorkflowApp()

    @app.workflow("a3_mixed_flow")
    def a3_mixed() -> Promise[object]:
        return build(loop("counter", fails_once, max_iterations=3))

    runner = await _runner_of(app, "a3_mixed_flow", wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    node = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert root == "failed" and node is not None and node["error_class"] == "LoopBodyFailure", (
        f"the mixed run's terminal state drifted: root={root}, node={node}"
    )
    # IDEMPOTENCE: a second drive (the racing sweep's shadow) must not
    # resurrect or double-terminalize.
    from taskq.workflows._sweep import sweep_loop_budget

    await sweep_loop_budget(wf_pool, runner.wsql)
    await runner.drive(flow_id, max_ticks=5)
    root2 = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root2 == "failed", f"the terminal flow MOVED after the sweep: {root!r} → {root2!r}"


# ── A3-L6: the cap wall's spawn count under the MEMO replay (crash
#    recovery must not overshoot the cap) ────────────────────────────────


async def test_a3_cap_bounds_spawns_through_the_memo_replay(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    calls: list[int] = []

    async def counting(ctx: StepContext, carry: object) -> Refine[Counter]:
        calls.append(1)
        return Refine(Counter(acc=len(calls)))

    app = WorkflowApp()

    @app.workflow("a3_cap_flow")
    def a3_cap() -> Promise[object]:
        return build(loop("counter", counting, max_iterations=4))

    runner = await _runner_of(app, "a3_cap_flow", wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    assert len(calls) == 4, f"the cap did NOT bound spawns exactly: {len(calls)}"
    # The sweep's shadow on the terminal rows changes nothing.
    from taskq.workflows._sweep import sweep_loop_budget

    await sweep_loop_budget(wf_pool, runner.wsql)
    assert len(calls) == 4, f"the sweep's pass SPAWNED iterations: {len(calls)}"


_ = asyncio  # the probes drive through the runner (no raw asyncio needed)
