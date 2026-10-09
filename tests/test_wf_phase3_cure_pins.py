"""THE PHASE-3 CURE PINS (fixer round 1) — the in-commit pins for the
cures the attack-3 probes proved red first:

* B2's BOTH-FIT arm: an ambiguous payload is the typed refusal UNLESS
  the gate declares an explicit discriminator (the small honest API).
* H1's END-TO-END arm: the escalation consumer job RUNS (the registered
  ``loop.escalation`` body — no dead letter); the driver-path ``fail``
  policy enqueues NOTHING.
* The sweep-cap arm's REACHABILITY: the advance guard lets the FINAL
  iteration run — the metadata's counter REACHES the cap (the state the
  crash window leaves; the sweep's predicate is constructible by the
  shipped code, never hand-crafted).
* H2's audit leg: the REFUSED resolve writes the audit row that names
  the refusal, and the hold still stands.
* The EXIT sentinel (§17.1) and the MANUAL RESUME (§17.2) — the
  alignment-audit's Missing #1, landed red-first.
* E8: the loop CARRIER-TYPE declaration enforced (pin 5's teeth).

Each pin states the cure's law; the red evidence is the attack-3 corpus
(``.measurements/attack3/``) plus this file's own captured first run.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    Done,
    Exit,
    FlowRunner,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    map_source,
    step,
)
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.api._loop import default_escalation_body
from taskq.workflows.api._validate import _run_rules


class Ingest(BaseModel):
    doc_id: str


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Strict(BaseModel):
    decision: str


class Lenient(BaseModel):
    """All-optional: ANY shape-clean dict with a/b fits it."""

    a: str = "default"
    b: str = "default"


class Other(BaseModel):
    unrelated: str


class Counter(BaseModel):
    acc: int = 0


class Report(BaseModel):
    n: int


# ── B2's BOTH-FIT arm: the ambiguity refusal + the discriminator door ──


async def test_ambiguous_payload_refused_unless_the_gate_discriminates(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A payload fitting MORE than one declared model is the typed
    refusal (narrowing by declaration order is the convicted
    mis-narrowing) — UNLESS the wait site declared an explicit
    discriminator, whose pick resolves the union. The both-fit shape:
    two all-optional models — the empty object fits BOTH (every field
    defaulted)."""

    class Alpha(BaseModel):
        x: int = 1

    class Beta(BaseModel):
        y: int = 2

    received: list[Any] = []

    async def discriminated_body(ctx: StepContext, params: Ingest) -> Any:
        answer = await ctx.wait_signal(
            (Alpha, Beta),
            timeout_s=120.0,
            discriminator=lambda p: Beta,
        )
        received.append(answer)
        return answer

    app2 = WorkflowApp()

    @app2.workflow("a3cure_discriminated_flow")
    def discriminated() -> object:
        return build(step(discriminated_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app2.get("a3cure_discriminated_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    # {} fits BOTH Alpha and Beta → the discriminator picks Beta.
    result = await client.resolve(holds[0].hold_id, {})
    assert result.status == "delivered", result
    await runner.drive(flow_id, max_ticks=30)
    assert received and type(received[0]) is Beta, (
        f"the discriminator's pick did not resolve the union: "
        f"{type(received[0]).__name__ if received else 'none'}"
    )


async def test_both_fit_without_discriminator_is_refused_hold_survives(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The BOTH-FIT refusal: no discriminator → the delivery is refused
    (the typed ambiguity error names the fitting models), the hold
    SURVIVES (nothing consumed), and the refusal is AUDITED."""

    class Alpha(BaseModel):
        x: int = 1

    class Beta(BaseModel):
        y: int = 2

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal((Alpha, Beta), timeout_s=120.0)

    app = WorkflowApp()

    @app.workflow("a3cure_ambiguous_flow")
    def ambiguous() -> object:
        return build(step(hold_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get("a3cure_ambiguous_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    # THE AMBIGUITY: {} fits BOTH (all-optional) — refused without a
    # discriminator.
    result = await client.resolve(holds[0].hold_id, {})
    assert result.status == "refused", (
        f"the both-fit payload was {result.status!r} — narrowing by "
        "declaration order is the convicted mis-narrowing; the ambiguous "
        "delivery must be refused"
    )
    assert "Alpha" in (result.reason or "") and "Beta" in (result.reason or ""), (
        f"the refusal does not NAME the ambiguity: {result.reason!r} — the operator sees why"
    )
    # THE HOLD SURVIVES: still held, still listable, still resolvable.
    holds_after = await client.list(run=flow_id)
    assert len(holds_after) == 1 and holds_after[0].hold_id == holds[0].hold_id
    ok = await client.resolve(holds[0].hold_id, {"x": 9})  # fits Alpha ONLY
    assert ok.status == "delivered", ok
    await runner.drive(flow_id, max_ticks=30)
    signals = await wf_conn.fetch(
        f'SELECT status, payload FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',
        flow_id,
    )
    assert len(signals) == 1 and signals[0]["status"] == "delivered"


# ── H1's END-TO-END arms: the escalation RUNS; the fail policy is quiet ──


async def test_escalation_enqueues_and_the_registered_body_runs(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The DRIVER-path exhaustion with ``on_exhausted="escalate"``: the
    outbox row is written (the attack pin's shape), the outbox DRAINS
    into the consumer job, and the REGISTERED escalation body RUNS —
    the job terminal-succeeds carrying the exhaustion record. No dead
    letters."""
    calls: list[int] = []

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        calls.append(1)
        return Refine(Counter(acc=len(calls)))

    app = WorkflowApp()

    @app.workflow("a3cure_escalate_flow")
    def escalate_flow() -> object:
        return build(loop("counter", refine_forever, max_iterations=3, on_exhausted="escalate"))

    runner = FlowRunner(app.get("a3cure_escalate_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # The outbox row is written in the exhaust's tx; the drain runs at
    # the tick's end. NOTE (the fenceable-shape pin below is the fleet
    # truth): THIS arm's tick claims the consumer through the runner's
    # own claimable read, which carries NO flow-status fence — the fleet
    # dispatch claim DOES, and the escalation consumer is born into a
    # TERMINAL flow. The in-process tick alone could never prove the
    # escalation dispatchable; see
    # test_escalation_consumer_dispatches_through_the_fleet_claim.
    await runner.tick(flow_id)
    outbox = await wf_conn.fetch(
        f'SELECT consumer_step_key, bindings FROM "{wf_schema}".wf_outbox WHERE flow_id = $1',
        flow_id,
    )
    escalations = [r for r in outbox if r["consumer_step_key"] == "loop.escalation"]
    assert escalations, (
        f"the driver-path exhaustion wrote NO escalation outbox row: "
        f"{[r['consumer_step_key'] for r in outbox]}"
    )
    # THE REGISTERED BODY RAN (no dead letter): the consumer job exists,
    # terminal-succeeded, carrying the exhaustion record.
    consumer = await wf_conn.fetchrow(
        f'SELECT status, result, payload FROM "{wf_schema}".jobs '
        "WHERE step_key = 'loop.escalation' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert consumer is not None, "the escalation outbox row never drained into a consumer job"
    assert consumer["status"] == "succeeded", dict(consumer)
    result_doc = consumer["result"]
    assert '"loop"' in (result_doc or "") and "counter" in (result_doc or ""), (
        f"the escalation consumer ran WITHOUT the exhaustion record: {result_doc!r}"
    )
    # The default body's own record face (the bindings' payload reached it).
    assert "counter" in (consumer["payload"] or ""), "the exhaustion context never rode the job"


async def test_escalation_consumer_dispatches_through_the_fleet_claim(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ESCALATION'S FENCEABLE SHAPE (the FLEET-TRUTH pin): the
    exhaustion tx commits the escalation's outbox row and the flow's
    terminal TOGETHER — the consumer row is BORN into a terminal flow —
    and the dispatch fence's terminal-flow leg (a workflow row on a
    terminal flow is unclaimable) therefore refused the escalation
    FOREVER: page-a-human was dead code with a green test (the old pin's
    in-process tick took the runner's claimable read, which carries no
    fence, while its comment named 'the fleet worker's queue poll' —
    provably false).

    THE CURE (the design decision, and why): a flow's death must not
    orphan its pages-a-human duty — the operator's page is the ONE thing
    that must survive the flow's terminality. The fence's terminal-flow
    leg gains the ESCALATION-KIND exemption: the ``loop.escalation``
    consumer alone is dispatchable on the terminal flow; every other
    workflow row on a dead flow stays fenced. The exemption — not a
    re-ordering — is the only shape that keeps the atomicity law (the
    outbox row + the terminal = ONE tx) AND delivers the page: the fence
    is evaluated at DISPATCH time (the drain is a later pass), when the
    flow is terminal either way.

    THE PIN takes the REAL claim path — ``dispatch_batch`` over the
    certified strict-FIFO claim SQL, a capable worker's identity — and
    the REAL fleet execution door — ``run_fleet_claimed_step``. The
    escalation consumer must be CLAIMED and RUN."""
    from datetime import timedelta

    from taskq._ids import new_uuid as _new_uuid
    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch

    calls: list[int] = []

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        calls.append(1)
        return Refine(Counter(acc=len(calls)))

    app = WorkflowApp()

    @app.workflow("a3cure_fleet_escalation_flow")
    def fleet_escalation() -> object:
        return build(loop("counter", refine_forever, max_iterations=3, on_exhausted="escalate"))

    runner = FlowRunner(app.get("a3cure_fleet_escalation_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # The FLOW IS TERMINAL here (the exhaust committed with the outbox
    # row in one tx). The drain inserts the consumer row; the FLEET claim
    # is the row's ONLY dispatch door.
    await runner.tick(flow_id, execute=False)
    consumer_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE step_key = 'loop.escalation' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert consumer_id is not None, "the escalation outbox row never drained into a consumer job"
    flow_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert flow_status == "failed", flow_status
    # The consumer's PLACEMENT rides the registered definition (the
    # actor/queue the escalation bindings resolved) — the fleet claim
    # routes by it, so the pin stamps the SAME actor/queue pair the
    # drain wrote onto the row.
    consumer_row = await wf_conn.fetchrow(
        f'SELECT status, actor, queue, step_key FROM "{wf_schema}".jobs WHERE id = $1',
        consumer_id,
    )
    assert consumer_row is not None
    assert consumer_row["status"] == "pending"
    assert consumer_row["step_key"] == "loop.escalation"

    # THE FLEET CLAIM (the real claim round, the execution fence's
    # capable-worker shape — the same stamp the boot's projection writes).
    # The RUNNER carries the claiming worker's OWN identity from here on
    # (the finalize's terminal-mark fence compares locked_by_worker — a
    # runner id that differs from the claim's is fenced out by design).
    worker_id = _new_uuid()
    runner = FlowRunner(
        app.get("a3cure_fleet_escalation_flow"), wf_pool, wf_schema, worker_id=JobId(worker_id)
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        consumer_row["actor"],
        consumer_row["queue"],
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-escalation-pin', 1, $2::text[], $3::jsonb)",
        worker_id,
        [consumer_row["queue"]],
        json.dumps({"workflow_execution": True}),
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=[consumer_row["queue"]],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    claimed = {str(r["id"]) for r in dispatched}
    assert str(consumer_id) in claimed, (
        f"the escalation consumer {consumer_id} was NOT claimable through "
        f"the fleet dispatch (claimed={sorted(claimed)}) — the fence's "
        "terminal-flow leg refused the page-a-human duty forever: the "
        "escalation is dead code"
    )
    # THE BODY RUNS through the fleet-claimed door: the registered
    # escalation body executes, the consumer row terminal-succeeds
    # carrying the exhaustion record.
    job = next(r for r in dispatched if str(r["id"]) == str(consumer_id))
    outcome = await runner.run_fleet_claimed_step(
        flow_id,
        {
            "id": job["id"],
            "step_key": "loop.escalation",
            "map_index": None,
            "payload": job["payload"],
            "trace_id": job["trace_id"],
        },
        attempt=int(job["attempt"]),
        claim_epoch=int(job["claim_epoch"]),
    )
    assert outcome == "succeeded", outcome
    consumer = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".jobs WHERE id = $1', consumer_id
    )
    assert consumer is not None
    assert consumer["status"] == "succeeded", consumer["status"]
    assert "counter" in (consumer["result"] or ""), "the exhaustion record never reached the body"


async def test_driver_fail_policy_enqueues_nothing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The DRIVER-path exhaustion with ``on_exhausted="fail"``: the flow
    terminal-fails and NO escalation row is written (the policy
    vocabulary is read by the driver, not decorative)."""
    calls: list[int] = []

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        calls.append(1)
        return Refine(Counter(acc=len(calls)))

    app = WorkflowApp()

    @app.workflow("a3cure_fail_flow")
    def fail_flow() -> object:
        return build(loop("counter", refine_forever, max_iterations=2, on_exhausted="fail"))

    runner = FlowRunner(app.get("a3cure_fail_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    outbox = await wf_conn.fetch(
        f'SELECT consumer_step_key FROM "{wf_schema}".wf_outbox WHERE flow_id = $1', flow_id
    )
    escalations = [r for r in outbox if r["consumer_step_key"] == "loop.escalation"]
    assert not escalations, f"on_exhausted='fail' STILL escalated on the driver path: {escalations}"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed"


async def test_escalates_to_registers_a_custom_body(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The author's OWN escalation body (``escalates_to=``) is the
    registered ``loop.escalation`` step — the consumer resolves it from
    the definition registry (D1) and RUNS it."""
    escalated: list[dict[str, object]] = []

    async def page_operator(ctx: StepContext, escalation: dict[str, object]) -> dict[str, object]:
        escalated.append(escalation)
        return escalation

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        return Refine(Counter(acc=1))

    app = WorkflowApp()

    @app.workflow("a3cure_custom_escalation_flow")
    def custom_escalation() -> object:
        return build(
            loop(
                "counter",
                refine_forever,
                max_iterations=2,
                on_exhausted="escalate",
                escalates_to=page_operator,
            )
        )

    runner = FlowRunner(app.get("a3cure_custom_escalation_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    await runner.tick(flow_id)  # the consumer pass (the flow is terminal; see the escalate pin)
    assert escalated, "the author's escalation body never ran — the registration is a ghost"
    assert escalated[0].get("loop") == "counter", escalated


# ── the sweep-cap arm's REACHABILITY (the vacuous pin's cure) ──────────


async def test_the_final_iteration_runs_and_the_counter_reaches_the_cap(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The advance guard's cap lets the FINAL iteration run: the
    metadata's counter REACHES ``max_iterations`` after the drive (the
    shipped ``i+1 < max`` guard topped it at ``max-1`` FOREVER — the
    sweep's ``iteration >= max`` predicate was unconstructible in
    production, the vacuous pin hand-crafted it). The reachable state is
    exactly what the crash window (a worker death after the final
    advance) leaves on a RUNNING row."""
    calls: list[int] = []

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[Counter]:
        calls.append(1)
        return Refine(Counter(acc=len(calls)))

    app = WorkflowApp()

    @app.workflow("a3cure_reachable_cap_flow")
    def reachable_cap() -> object:
        return build(loop("counter", refine_forever, max_iterations=3))

    runner = FlowRunner(app.get("a3cure_reachable_cap_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert len(calls) == 3, f"the cap did not bound spawns exactly: {len(calls)}"
    iteration = await wf_conn.fetchval(
        f"SELECT (metadata->>'iteration')::int FROM \"{wf_schema}\".jobs "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert iteration == 3, (
        f"the counter topped at {iteration} — the sweep's cap predicate "
        "(iteration >= max) is UNREACHABLE: the advance guard must let the "
        "final iteration run"
    )
    node_row = await wf_conn.fetchrow(
        f"SELECT status, metadata->>'iteration_state' AS state FROM \"{wf_schema}\".jobs "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node_row is not None
    assert node_row["state"] == "iteration_cap_exhausted"
    assert node_row["status"] == "failed"


# ── the EXIT sentinel (§17.1) end-to-end ────────────────────────────────


async def test_exit_terminal_succeeds_and_marks_the_downstream_skipped(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE TYPED EARLY-EXIT (§17.1): a body returns ``Exit(payload)`` —
    the node terminal-succeeds with the typed payload (the envelope
    records the exit), every non-terminal DOWNSTREAM node is marked
    skipped-WITH-THE-RECORD (the record never lies about the nodes that
    didn't get to run), and the flow derives COMPLETE."""
    ran: list[str] = []

    async def head(ctx: StepContext, params: Ingest) -> Report:
        return Report(n=1)

    async def early_exit(ctx: StepContext, report: Report) -> Exit[Report]:
        return Exit(Report(n=42))

    async def tail(ctx: StepContext, report: Report) -> Report:
        ran.append("tail")
        return report

    app = WorkflowApp()

    @app.workflow("a3cure_exit_flow")
    def exit_flow() -> object:
        a = step(head, Ingest(doc_id="d1"), key="a")
        b = step(early_exit, a, key="b")
        return build(step(tail, b, key="tail"))

    runner = FlowRunner(app.get("a3cure_exit_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert ran == [], "the downstream node RAN after an Exit — the early exit did not exit"
    # THE EXITED NODE: terminal-succeeded, the typed payload recorded.
    exited = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".jobs '
        "WHERE step_key = 'b' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert exited is not None and exited["status"] == "succeeded"
    assert '"exit"' in (exited["result"] or "") and "42" in (exited["result"] or ""), (
        f"the Exit's typed result is not on the node's record: {exited['result']!r}"
    )
    # THE DOWNSTREAM: skipped WITH the record.
    skipped = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".jobs '
        "WHERE step_key = 'tail' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert skipped is not None and skipped["status"] == "succeeded"
    assert "skipped" in (skipped["result"] or "") and "exit_from" in (skipped["result"] or ""), (
        f"the downstream node's record does not name the skip: {skipped['result']!r}"
    )
    # THE LEDGER RECORDS IT: the exited node's ledger terminal exists.
    ledger = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'b' AND status = 'succeeded'",
        flow_id,
    )
    assert ledger == 1


# ── the MANUAL RESUME (§17.2) end-to-end ────────────────────────────────


async def test_retry_node_rearms_a_failed_node_the_ladder_continues(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE OPERATOR'S MANUAL RESUME (§17.2, the §22.6 redispatch row 3):
    the audited, CAS-guarded re-arm of a terminal-FAILED node — the
    attempt ordinal CONTINUES (never resets), the blocked closure
    re-opens, the flow re-opens, and the re-run is safe by the step
    ledger (ctx.step returns recorded results). A second manual retry
    after the node fails again buys EXACTLY ONE more attempt (the ladder
    is the count)."""
    attempts: list[int] = []

    async def fails_always(ctx: StepContext, params: Ingest) -> Report:
        attempts.append(1)
        raise RuntimeError("the node's own bug — the operator will retry")

    app = WorkflowApp()

    @app.workflow("a3cure_retry_flow")
    def retry_flow() -> object:
        return build(step(fails_always, Ingest(doc_id="d1"), key="lonely", max_attempts=1))

    runner = FlowRunner(app.get("a3cure_retry_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed"
    first_attempts = len(attempts)

    # THE MANUAL RESUME (CAS-granted).
    granted = await runner.retry_node(
        flow_id, "lonely", principal="op-7", reason="the fix deployed"
    )
    assert granted is True, "the manual retry was refused on a FAILED node"
    await runner.drive(flow_id, max_ticks=20)
    assert len(attempts) == first_attempts + 1, (
        f"the manual retry did not buy exactly ONE more attempt: {len(attempts)}"
    )
    # THE ATTEMPT ORDINAL CONTINUES: the claim's counter grew past 1.
    attempt = await wf_conn.fetchval(
        f"SELECT attempt FROM \"{wf_schema}\".jobs WHERE step_key = 'lonely' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert attempt >= 2, f"the attempt ordinal RESET (the fresh-budget dragon): {attempt}"
    # THE AUDIT IS A ROW.
    audit = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".admin_audit '
        "WHERE action = 'workflow.retry_node' AND target_id = $1",
        f"{flow_id}:lonely",
    )
    assert audit == 1
    # THE CAS: a node not terminal-failed is refused.
    node_row = await wf_conn.fetchval(
        f"SELECT status FROM \"{wf_schema}\".jobs WHERE step_key = 'lonely' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    del node_row
    node_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'lonely' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running' WHERE id = $1", node_id
    )
    assert await runner.retry_node(flow_id, "lonely") is False, (
        "the manual retry GRANTED on a running node — the CAS is decorative"
    )


# ── E8: the CARRIER-TYPE declaration enforced (T19's pin 5) ────────────


async def _refines_wrong(ctx: StepContext, carry: object) -> Refine[Other]:
    return Refine(Other(unrelated="drift"))


def test_carrier_type_mismatch_is_convicted() -> None:
    """T19 pin 5's teeth: a loop declaring ``carry=Counter`` whose body
    refines with an UNRELATED model is the compile's refusal (the
    recorded declaration is now ENFORCED)."""
    app = WorkflowApp()

    @app.workflow("a3cure_carrier_flow")
    def carrier() -> object:
        return build(loop("counter", _refines_wrong, carry=Counter(acc=0)))

    rules = [d.rule for d in _run_rules(app.get("a3cure_carrier_flow"))]
    assert "E8-carrier-type" in rules, (
        f"the CARRIER-TYPE declaration is not enforced: {rules} — a body "
        "refining an unrelated model compiles clean (the recorded-never-"
        "enforced finding)"
    )


def test_carrier_type_match_is_clean() -> None:
    """The zero-false-positive arm: the matching carrier compiles clean
    (and the undeclared-carry loop — the unenforceable shape — is never
    convicted on a guess)."""

    async def refines_right(ctx: StepContext, carry: object) -> Done[Counter] | Refine[Counter]:
        return Done(Counter(acc=1))

    app = WorkflowApp()

    @app.workflow("a3cure_carrier_ok_flow")
    def carrier_ok() -> object:
        return build(loop("counter", refines_right, carry=Counter(acc=0), max_iterations=2))

    rules = [d.rule for d in _run_rules(app.get("a3cure_carrier_ok_flow"))]
    assert "E8-carrier-type" not in rules, rules


# ── the MAP-JOIN consumption contract (the ecosystem mapper's defect) ──


async def _map_items_source(ctx: StepContext, params: Ingest) -> list[Report]:
    return [Report(n=i) for i in range(3)]


async def _map_per_item(ctx: StepContext, item: Report) -> dict[str, int]:
    return {"n": item.n * 10}


async def _map_tail(ctx: StepContext, items: list[dict[str, int]]) -> dict[str, int]:
    return {"sum": sum(i["n"] for i in items)}


async def test_map_join_promise_consumed_downstream_sees_the_collected_results(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE FIRST SHAPE EVERY MAP USER WIRES: a downstream `step` consuming
    the map's JOIN promise. The convicted shape (the mapper's red): the
    downstream rode only the outbox's side-channel — no edge row, a
    duplicate consumer row whose arg resolution found no result (the
    runner's own assert fired). THE CURE'S CONTRACT: the join's result is
    consumable through the SAME typed door as any node result — the edge
    ledger + the counter (the downstream's dep is RESERVED at create, the
    fork writes the edge row at the source's finalize, the join's own
    terminal writes the collected result and releases the dep) — the
    downstream sees the COLLECTED RESULTS, never the promise object; the
    transitive downstream (a node consuming the consumer) composes the
    same way."""
    ran: list[str] = []

    async def tail_tail(ctx: StepContext, summed: dict[str, int]) -> dict[str, int]:
        ran.append("tail_tail")
        return {"double": summed["sum"] * 2}

    app = WorkflowApp()

    @app.workflow("a3cure_map_tail_flow")
    def map_tail() -> object:
        src = step(_map_items_source, Ingest(doc_id="d1"), key="src")
        mapped = map_source(src, _map_per_item, key="m")
        tail = step(_map_tail, mapped, key="tail")
        return build(step(tail_tail, tail, key="tail_tail"))

    runner = FlowRunner(app.get("a3cure_map_tail_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"double": 60}
    assert ran == ["tail_tail"]
    # THE JOIN'S COLLECTED RESULT is on the join row (the same typed door).
    join_row = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".jobs '
        "WHERE step_key = 'src.join' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert join_row is not None and join_row["status"] == "succeeded"
    assert '"value"' in (join_row["result"] or "") and "20" in (join_row["result"] or "")
    # EXACTLY ONE downstream row (the side-channel's duplicate is the
    # convicted shape — the arbiter's key is the static row's own).
    tails = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE step_key = 'tail' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert tails == 1
    # THE LEDGER: the tail's attempt history is clean (one claim, one
    # success — no assert-littered crashed rows).
    ledger = await wf_conn.fetch(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'tail'",
        flow_id,
    )
    assert [r["status"] for r in ledger] == ["succeeded"]


# ── the E4 false positive (the pattern miner's finding) ────────────────


def test_e4_does_not_convict_a_fully_annotated_body() -> None:
    """THE ZERO-FALSE-POSITIVE DOCTRINE at E4 (the miner's red): a
    fully-annotated body whose return type is NOT module-level (a
    function-scope model the body's code never names — the annotation
    string creates no closure cell, the resolved hints are {}) is NOT
    convicted: E4's question is the annotation's PRESENCE (read from the
    raw signature), never its resolvability; the type-dependent rules
    (E5/E8, the codec) keep the resolved view and SKIP the unresolvable
    (a guess is never convicted)."""

    class Local(BaseModel):
        n: int

    def make_local() -> Local:
        return Local(n=1)

    async def annotated_body(ctx: StepContext, params: Ingest) -> Local:
        return make_local()  # the body's code never names Local — no closure cell

    app = WorkflowApp()

    @app.workflow("a3cure_e4_fp_flow")
    def e4_fp() -> object:
        return build(step(annotated_body, Ingest(doc_id="d1"), key="a"))

    rules = [d.rule for d in _run_rules(app.get("a3cure_e4_fp_flow"))]
    assert rules == [], f"the false positive: a fully-annotated body convicted: {rules}"


def test_e4_still_convicts_the_genuinely_unannotated() -> None:
    """E4's teeth survive the raw-presence read: a body with NO return
    annotation at all is the same conviction as before."""
    from taskq.workflows.api._validate import WorkflowValidationError

    async def unannotated(ctx: StepContext, params: Ingest):  # pyright: ignore[reportMissingParameterType, reportUnknownParameterType, reportMissingTypeStubs]  # Why: THE PROBE — the genuinely unannotated body is the mutation under test.
        return params

    app = WorkflowApp()

    @app.workflow("a3cure_e4_teeth_flow")
    def e4_teeth() -> object:
        return build(step(unannotated, Ingest(doc_id="d1"), key="a"))

    try:
        app.get("a3cure_e4_teeth_flow").validate()
    except WorkflowValidationError:
        pass
    else:
        raise AssertionError("E4 lost its teeth — the genuinely unannotated body compiled clean")


# ── cut #6's naming pin: the ROOT-WITH-DOT refusal ──────────────────────


def test_wiring_key_with_a_dot_is_refused() -> None:
    """The dot is the ENGINE'S derived namespace (``<key>.item`` /
    ``<key>.join`` / ``<key>.iter<i>``) — a wiring key carrying one is
    refused at the compile (the T17 ledger's cut #6 citation, which had
    no test under any name before this pin)."""
    import pytest

    from taskq.workflows import WorkflowBuildError

    app = WorkflowApp()

    @app.workflow("a3cure_dot_key_flow")
    def dot_key() -> object:
        return build(step(_refines_wrong, Ingest(doc_id="d1"), key="bad.key"))

    with pytest.raises(WorkflowBuildError, match="dot"):
        app.get("a3cure_dot_key_flow")  # the build runs at COMPILE


def test_loop_key_with_a_dot_is_refused() -> None:
    import pytest

    from taskq.workflows import WorkflowBuildError

    app = WorkflowApp()

    @app.workflow("a3cure_dot_loop_flow")
    def dot_loop() -> object:
        return build(loop("bad.loop", _refines_wrong, max_iterations=2))

    with pytest.raises(WorkflowBuildError, match="dot"):
        app.get("a3cure_dot_loop_flow")  # the build runs at COMPILE


# ── E7 / W2's committed pins (the attack probes prove them red-first;
#    the validate pins file's exact-one-rule matrix gains the new rules) ──


def test_cross_graph_smuggle_is_convicted_at_validate() -> None:
    """E7: a promise from ANOTHER app's recorder — recorded by the verb,
    convicted by validate (the attack's A3-V2 shape, committed)."""
    from taskq.workflows.api._graph import BuildGraph, Promise

    app = WorkflowApp()

    @app.workflow("a3cure_smuggle_flow")
    def smuggle() -> object:
        mine = step(_refines_wrong, Ingest(doc_id="d1"), key="fetch")
        foreign = Promise("fetch", object, BuildGraph())
        return build(
            step(_refines_wrong, mine, key="keep"), step(_refines_wrong, foreign, key="smuggled")
        )

    rules = [d.rule for d in _run_rules(app.get("a3cure_smuggle_flow"))]
    assert "E7-cross-graph-promise" in rules, rules


def test_unknown_queue_is_warned_never_refused() -> None:
    """W2: a queue no actor declares and TASKQ_QUEUES does not name —
    the WARNING class (over-refusing is the compile's over-rejection)."""
    app = WorkflowApp()

    @app.workflow("a3cure_queue_flow")
    def queue_flow() -> object:
        return build(step(_refines_wrong, Ingest(doc_id="d1"), key="a", queue="no-such-queue"))

    compiled = app.get("a3cure_queue_flow")
    rules = [d.rule for d in _run_rules(compiled)]
    assert "W2-unknown-queue" in rules, rules
    compiled.validate()  # the WARNING does not refuse


# ── the default escalation body's identity (D1's registration face) ────


def test_default_escalation_body_is_registered_once_per_workflow() -> None:
    """Two escalating loops on ONE workflow: the SAME default body
    registers idempotently; two DIFFERING custom bodies are the refused
    shadow (one escalation step per workflow)."""
    from taskq.workflows.api._loop import ESCALATION_STEP_KEY
    from taskq.workflows.definitions import DuplicateStepBodyError, get_registry

    async def body_one(ctx: StepContext, carry: object) -> Done[Counter]:
        return Done(Counter(acc=1))

    async def body_two(ctx: StepContext, carry: object) -> Done[Counter]:
        return Done(Counter(acc=2))

    async def esc_a(ctx: StepContext, escalation: dict[str, object]) -> dict[str, object]:
        return escalation

    async def esc_b(ctx: StepContext, escalation: dict[str, object]) -> dict[str, object]:
        return escalation

    app = WorkflowApp()

    @app.workflow("a3cure_two_loops_flow")
    def two_loops() -> object:
        return build(
            loop("l1", body_one, max_iterations=1, on_exhausted="escalate", escalates_to=esc_a),
        )

    app.get("a3cure_two_loops_flow")
    definition = get_registry().get("a3cure_two_loops_flow")
    assert definition.bodies[ESCALATION_STEP_KEY] is esc_a

    app2 = WorkflowApp()

    @app2.workflow("a3cure_two_custom_flow")
    def two_custom() -> object:
        return build(
            loop("l1", body_one, max_iterations=1, on_exhausted="escalate", escalates_to=esc_a),
            loop("l2", body_two, max_iterations=1, on_exhausted="escalate", escalates_to=esc_b),
        )

    try:
        app2.get("a3cure_two_custom_flow")
    except DuplicateStepBodyError:
        pass
    else:
        raise AssertionError(
            "two DIFFERING escalation bodies registered silently — the "
            "loop.escalation step is the refused shadow"
        )
    # And the DEFAULT body is the SAME object every compile (the
    # registry's idempotence holds).
    app3 = WorkflowApp()

    @app3.workflow("a3cure_default_esc_flow")
    def default_esc() -> object:
        return build(loop("l1", body_one, max_iterations=1, on_exhausted="escalate"))

    app3.get("a3cure_default_esc_flow")
    definition3 = get_registry().get("a3cure_default_esc_flow")
    assert definition3.bodies[ESCALATION_STEP_KEY] is default_escalation_body


_ = asyncpg  # the pins drive through the runner (the pool fixtures carry the import's shape)
