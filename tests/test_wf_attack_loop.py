"""ATTACK PINS — the loop machinery front (the heaviest front of the
hostile review of the consolidated head, af1b8779).

Provenance: the att_loop red-team's probes (a1a through a5, REPORT.md) run live
against PG. Each LANDED finding is pinned here asserting the SAFE
behavior and marked ``pytest.mark.xfail(strict=True)``: the cure flips
the pin to XPASS-strict (a red that says: remove the marker WITH the
cure). The GUARDS at the foot are the attacks the front FAILED — the
machinery held; they run green so the fence cannot rot silently.

The findings, one pin each (the front's REPORT.md carries the evidence):

* F-LOOP-1 — the typed carry is a validate-time fiction:
  ``initial=Counter()`` is jsonabled to a dict at iteration 0, every
  resume/replay reads the jsonb dict, and ``loop_body(ctx, carry)``
  never revalidates. A body trusting ``carry: Counter`` dies on
  iteration 0 with zero faults; the shipped pins' isinstance fallbacks
  silently RESET accumulation on resume (observed [1, 1, 2, 3] for a
  three-step count).
* F-LOOP-2 — the advance/exhaust statements are UNFENCED: their WHERE
  reads ``status='running'`` only — no worker/attempt/claim_epoch legs
  (the fence every other terminal write in the estate carries). A stale
  driver's body failure killed a healthy RECLAIMED loop mid-drive (and
  wrote the escalation row); a stale advance moved the counter BACKWARD
  3→1 over the live carry.
* F-LOOP-3 — the escalation consumer NEVER RUNS in production shape:
  the dispatch fence's terminal-flow leg refuses the row (the
  exhaustion terminalizes the flow in the outbox insert's own tx — the
  row is born unfenceable); the phase-3 pin greens via an in-process
  tick whose comment claims the fleet claims it (false).
* F-LOOP-4 — a body returning neither Done nor Refine is RECORDED as a
  succeeded iteration before the union assert; drive() raises a bare
  AssertionError; on reclaim the memo replays the garbage as a Refine
  and the loop dies as IterationLimitExhausted — the ledger lies and
  the diagnosis never names the shape error.
* F-LOOP-5 — sweep_loop_budget's return counts only the
  escalate-enqueued exhaustions; a fail-policy loop exhausts while the
  sweep returns 0.
* F-LOOP-6 — loop()'s promised "waits forever" validate warning does
  not exist (no rule reads loop_until/max_iterations/budget_s).
* F-LOOP-7 — E8 convicts at .validate()/FlowRunner() but app.get()
  compiles + REGISTERS the bad definition (the boot projection compiles
  without validating): a fleet refuses at first claim, not at compile.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.
import asyncio
import contextlib
import json
from datetime import timedelta

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
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
from taskq.workflows._sweep import drain_outbox, sweep_loop_budget

pytestmark = pytest.mark.integration


class Counter(BaseModel):
    """The declared carry model (F-LOOP-1's subject)."""

    acc: int = 0


class Carry(BaseModel):
    """The zombie pin's carry: the counter + WHO advanced it."""

    acc: int = 0
    by: str = ""


# ── F-LOOP-1: the typed carry never reaches the body ────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [LOOP-CARRY0]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (af1b8779): loop(initial=Counter()) jsonables the model to a …
async def test_f_loop_1_the_typed_carry_reaches_the_body_at_iteration_zero(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The TRUSTING body — no isinstance dance, the annotation's own
    promise (the guide's claim: "a body sees the type it declared, never
    a raw dict"; true for step params via coerce_arg, refuted for the
    loop door by the front's a1b). SAFE: three iterations, each handed a
    Counter, the result {"acc": 3}. TODAY: iteration 0 hands a dict and
    the body dies — LoopBodyFailure (AttributeError), zero faults needed."""
    seen: list[str] = []

    async def trusting_body(ctx: StepContext, carry: Counter) -> Done[Counter] | Refine[Counter]:
        seen.append(type(carry).__name__)
        nxt = carry.acc + 1  # the annotation's promise: carry IS a Counter
        return Done(Counter(acc=nxt)) if nxt >= 3 else Refine(Counter(acc=nxt))

    app = WorkflowApp()

    @app.workflow("aloop_carry_door")
    def _wf() -> Promise[object]:
        return build(loop("counter", trusting_body, initial=Counter(acc=0), max_iterations=5))

    runner = FlowRunner(app.get("aloop_carry_door"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id)
    node = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert verdict == "terminal" and node is not None
    assert node["status"] == "succeeded", (
        f"F-LOOP-1: the trusting body never stood a chance — the loop node is "
        f"{node['status']}/{node['error_class']} ({(node['error_message'] or '')[:100]}); "
        f"the body was handed {seen} — the typed carry is a validate-time fiction "
        "(a body declaring a model type must receive that type at EVERY boundary)"
    )
    assert seen == ["Counter", "Counter", "Counter"], seen
    assert await runner.result(flow_id) == {"acc": 3}


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [LOOP-CARRY]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (af1b8779): the crash-window resume replays the carry as a …
async def test_f_loop_1_the_accumulate_across_resume_sequence_is_exact(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The resume face. The kill lands BETWEEN the iteration's committed
    terminal write and the carry advance (the pg_terminate_backend
    window — the shipped pin #6's own classified seam); the memo heals
    the window (the guard below proves the ledger-level exactly-once) —
    and the replayed carry must arrive AS THE DECLARED MODEL, so the
    accumulate sequence is EXACTLY [(Counter, 1), (Counter, 2),
    (Counter, 3)]. TODAY the defensive-body shape observes the dict and
    resets: [("dict", 1), ("dict", 1), ("Counter", 2), ("Counter", 3)]."""
    from taskq.workflows.api import _runner_loop

    spawns: list[tuple[str, int]] = []

    async def counting_body(ctx: StepContext, carry: object) -> object:
        acc = carry.acc + 1 if isinstance(carry, Counter) else 1
        spawns.append((type(carry).__name__, acc))
        await asyncio.sleep(0)  # a yield point, like any real body
        return Done(Counter(acc=acc)) if acc >= 3 else Refine(Counter(acc=acc))

    app = WorkflowApp()

    @app.workflow("aloop_carry_resume")
    def _wf() -> Promise[object]:
        return build(loop("counter", counting_body, initial=Counter(acc=0), max_iterations=5))

    # THE CRASH, DETERMINISTIC: iteration 0's terminal write COMMITS,
    # then the backend dies before the advance. Once only.
    real_record = _runner_loop.LoopOps._record_iteration_terminal
    kills = {"n": 0}

    async def killing_record(
        self: object, flow_id: object, iter_key: str, attempt: int, outcome: object
    ) -> None:
        await real_record(self, flow_id, iter_key, attempt, outcome)
        kills["n"] += 1
        if kills["n"] == 1:
            raise asyncpg.exceptions.ConnectionDoesNotExistError(
                "pg_terminate_backend landed between the terminal write and the advance"
            )

    monkeypatch.setattr(_runner_loop.LoopOps, "_record_iteration_terminal", killing_record)
    runner = FlowRunner(app.get("aloop_carry_resume"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, max_ticks=30)
    monkeypatch.undo()
    # THE RE-DRIVE (the reclaim's re-claim — a fresh driver identity, as
    # a re-claiming peer would be).
    runner2 = FlowRunner(app.get("aloop_carry_resume"), wf_pool, wf_schema)
    assert await runner2.drive(flow_id, max_ticks=50) == "terminal"
    assert await runner2.result(flow_id) == {"acc": 3}
    assert spawns == [("Counter", 1), ("Counter", 2), ("Counter", 3)], (
        f"F-LOOP-1: the accumulate-across-resume sequence is {spawns} — a dict "
        "carry silently reset the count on the resume (the declared model never "
        "reached the body at the boundary)"
    )


# ── F-LOOP-2: the advance/exhaust statements carry no claim fence ───────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [LOOP-FENCE]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (af1b8779): LOOP_ADVANCE_SQL / LOOP_EXHAUST_SQL fence on …
def test_f_loop_2_the_advance_and_exhaust_statements_carry_the_claim_fence() -> None:
    """The statement face of the unfenced writes (the behavioral face is
    the zombie pin below). The estate's fence vocabulary is fixed — the
    finalize's tx1 reads ``status + locked_by_worker + attempt +
    claim_epoch``; both loop statements must carry the same legs, so a
    stale driver's write updates NOTHING."""
    from taskq.workflows.api import _sql_loop

    missing: list[str] = []
    for name in ("LOOP_ADVANCE_SQL", "LOOP_EXHAUST_SQL"):
        statement = getattr(_sql_loop, name)
        for leg in ("locked_by_worker", "attempt", "claim_epoch"):
            if leg not in statement:
                missing.append(f"{name} carries no {leg} fence leg")
    assert not missing, (
        "F-LOOP-2: the loop's terminal writes are unfenced — "
        + "; ".join(missing)
        + " (every other terminal write in the estate carries all three)"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [LOOP-UNION]; the marker is removed per the designed flip (the confirmation receipt).


# ── F-LOOP-4: the non-union return is recorded as success, then laundered ─


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [LOOP-UNION]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (af1b8779): a body returning neither Done nor Refine is …
async def test_f_loop_4_a_non_union_return_fails_named_at_the_point_of_return(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The shape error (the checker-invisible gap the typeprobe honestly
    names): the body returns a bare model. SAFE: the iteration fails
    with a NAMED error class at the point of return (the message names
    the control union), NOTHING is recorded as succeeded, drive()
    returns the bounded 'terminal' verdict (never a bare
    AssertionError), and no later reclaim can launder the garbage into a
    Refine (the body runs ONCE)."""
    spawns: list[int] = []

    async def raw_body(ctx: StepContext, carry: object) -> object:
        spawns.append(1)
        return {"not": "a-control-union-member"}  # the shape error

    app = WorkflowApp()

    @app.workflow("aloop_shape_error")
    def _wf() -> Promise[object]:
        return build(loop("counter", raw_body, max_iterations=3))

    runner = FlowRunner(app.get("aloop_shape_error"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    drive_exc: Exception | None = None
    verdict: str | None = None
    try:
        verdict = await runner.drive(flow_id, max_ticks=10)
    except Exception as exc:
        drive_exc = exc
    node1 = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    succeeded1 = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key LIKE 'counter.iter%' AND status = 'succeeded'",
        flow_id,
    )
    # THE LAUNDER WINDOW: reclaim the row (the estate's heal) and
    # re-drive — the memo must not replay a garbage 'succeeded' row as a
    # Refine.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', scheduled_at = now(), "
        "locked_by_worker = NULL WHERE step_key = 'counter' "
        "AND (metadata->>'flow_id')::uuid = $1 AND status = 'running'",
        flow_id,
    )
    runner2 = FlowRunner(app.get("aloop_shape_error"), wf_pool, wf_schema)
    with contextlib.suppress(Exception):
        await runner2.drive(flow_id, max_ticks=10)
    succeeded2 = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key LIKE 'counter.iter%' AND status = 'succeeded'",
        flow_id,
    )
    assert node1 is not None
    assert drive_exc is None and verdict == "terminal", (
        f"F-LOOP-4: drive() raised "
        f"{type(drive_exc).__name__ if drive_exc else None} "
        f"({(str(drive_exc)[:120]) if drive_exc else verdict}) — the shape error "
        "must terminalize the flow as a NAMED failure, never escape as a bare "
        "AssertionError (the union's residual machinery is not a diagnosis)"
    )
    assert (
        node1["status"] == "failed"
        and node1["error_class"] not in (None, "IterationLimitExhausted")
        and "Refine" in (node1["error_message"] or "")
    ), (
        f"F-LOOP-4: the shape error was never NAMED — the loop node is "
        f"{node1['status']}/{node1['error_class']} "
        f"({(node1['error_message'] or '')[:120]}); the diagnosis must name the "
        "control-union violation at the point of return, never the cap"
    )
    assert succeeded1 == 0 and succeeded2 == 0, (
        f"F-LOOP-4: the ledger recorded the garbage return as SUCCEEDED "
        f"({succeeded1} iteration rows after the first drive, {succeeded2} after "
        "the re-drive — the memo replayed the garbage as a Refine: the launder)"
    )
    assert len(spawns) == 1, (
        f"F-LOOP-4: the body ran {len(spawns)} times — the reclaim replayed the "
        "laundered memo instead of standing on the named failure"
    )


# ── F-LOOP-5: the sweep's return counts every exhaustion ────────────────
# CURED (the sweep-count cure): the budget sweep's return counts EVERY
# CAS-held exhaustion — the named state + the flow's terminalization
# complete at the exhaust statement; the escalation arm is additional
# work. The marker is gone, the green IS the receipt.


# ── F-LOOP-3: the escalation's claim on the REAL dispatch path ──────────
# CURED (this head): the ESCALATION-KIND exemption is shipped in BOTH
# fences (the claim fence AND the probe fence — the two may not
# disagree): the loop's registered escalation consumer is dispatchable ON
# the terminal flow that spawned it. The marker is gone, the green IS the
# receipt.
async def test_f_loop_3_the_escalation_is_claimed_and_runs_on_the_real_dispatch_path(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE PRODUCTION SHAPE (the front's a3f, extended to the run): a
    loop exhausted under the driver's cap (escalate policy, the author's
    ``escalates_to=`` body), the outbox drained — then the REAL dispatch
    CTE (the round-robin variant, the fleet worker's claim statement)
    with a ``workflow_execution``-capable worker serving 'default'.

    The CONTROL (a live flow's pending loop node) MUST be claimed — the
    probe is never vacuous. The SUBJECT (the escalation row on the
    terminal flow) must be claimed AND run: the registered body receives
    the exhaustion record and the row terminal-succeeds with it. The
    'workflow' actor cohort is seeded up front (a3f's separation probe):
    with capacity present, the ONLY refuser left is the fence leg."""
    from dataclasses import replace

    from taskq._json import loads as _loads
    from taskq.backend._dispatch_sql import DISPATCH_ROUND_ROBIN_SQL, dispatch_batch
    from taskq.workflows._worker_execution import execute_flow_job
    from tests._stub_job_row import make_job

    # THE FLEET SHAPE: a workflow_execution-capable worker row (the boot
    # projection's stamp) + the synced cohorts — 'wf' (the compiled
    # nodes') AND 'workflow' (the WorkflowDef default actor — the
    # escalation row's placement; seeded so the capacity question is OFF
    # the table and the pin isolates the fence leg).
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'att-loop-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )
    for actor in ("wf", "workflow"):
        await wf_conn.execute(
            f"INSERT INTO \"{wf_schema}\".actor_config (actor, queue) VALUES ($1, 'default') "
            "ON CONFLICT (actor) DO NOTHING",
            actor,
        )

    escalated: list[dict[str, object]] = []

    async def page_operator(ctx: StepContext, escalation: dict[str, object]) -> dict[str, object]:
        escalated.append(escalation)
        return escalation

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        return Refine({"n": 1})

    app = WorkflowApp()

    @app.workflow("aloop_escalation_dispatch")
    def _wf() -> Promise[object]:
        return build(
            loop(
                "counter",
                refine_forever,
                max_iterations=2,
                on_exhausted="escalate",
                escalates_to=page_operator,
            )
        )

    runner = FlowRunner(app.get("aloop_escalation_dispatch"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    await drain_outbox(wf_pool, runner.wsql)
    esc = await wf_conn.fetchrow(
        f"SELECT id, status FROM \"{wf_schema}\".jobs WHERE step_key = 'loop.escalation' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert esc is not None and esc["status"] == "pending", (
        f"the escalation never enqueued+drained (the setup's own shape): {esc}"
    )

    # THE CONTROL: a live flow's pending loop node.
    live_flow = (await runner.create_flow()).flow_id
    live_node = await wf_conn.fetchrow(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'counter' "
        "AND (metadata->>'flow_id')::uuid = $1",
        live_flow,
    )
    assert live_node is not None

    # THE REAL DISPATCH CTE — the fleet's claim statement, this schema.
    got = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_ROUND_ROBIN_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=50,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    claimed = {str(g["id"]): g for g in got}
    assert str(live_node["id"]) in claimed, (
        "the CONTROL was not claimed — the probe's worker/capability shape is off "
        "(nothing about the escalation is proven)"
    )
    esc_claimed = claimed.get(str(esc["id"]))
    assert esc_claimed is not None, (
        "F-LOOP-3: the escalation row is a DEAD LETTER on the real dispatch path — "
        "the control (a live flow's node) was claimed in the same round but the "
        "escalation row was refused: the dispatch fence's terminal-flow leg rejects "
        "it (the exhaustion terminalized its flow in the outbox insert's own "
        "transaction). On the fleet, escalates_to= never fires; the row sits "
        "pending until retention."
    )
    # AND RAN: the fleet's execution door runs the claimed row (D1 — the
    # body resolves from the registered definition).
    metadata = esc_claimed["metadata"]
    payload = esc_claimed["payload"]
    job = replace(
        make_job(actor="workflow"),
        id=JobId(esc_claimed["id"]),
        attempt=esc_claimed["attempt"],
        claim_epoch=esc_claimed["claim_epoch"],
        payload=_loads(payload) if isinstance(payload, str) else payload,
        metadata=_loads(metadata) if isinstance(metadata, str) else metadata,
        trace_id=esc_claimed["trace_id"],
    )
    outcome = await execute_flow_job(
        pool=wf_pool, schema=wf_schema, worker_id=JobId(worker_id), job=job
    )
    assert outcome.outcome == "succeeded", outcome
    assert escalated and escalated[0].get("loop") == "counter", escalated
    esc_after = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".jobs WHERE id = $1', esc["id"]
    )
    assert esc_after is not None and esc_after["status"] == "succeeded", (
        f"the escalation row never terminal-succeeded: {esc_after}"
    )
    assert "IterationLimitExhausted" in (esc_after["result"] or ""), (
        f"the escalation body ran WITHOUT the exhaustion record: {esc_after['result']!r}"
    )


# CURED (this head): the claim identity's fence landed on BOTH the
# advance and the exhaust statements (LOOP_ADVANCE_SQL / LOOP_EXHAUST_SQL's
# worker + attempt + claim_epoch legs — the one-tx-finalize doctrine's
# back door closed); the zombie's stale strike is refused at the statement.
# The marker is gone, the green IS the receipt.


async def test_f_loop_2_a_zombie_drivers_strike_updates_nothing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The live interleave (the front's a1c, gated deterministically):
    A claims (attempt 1) and parks in iter0's body past its lease; the
    estate reclaim re-pends the row; B (a fresh driver identity)
    re-claims, runs iter0, advances, parks mid-iter1; A wakes and its
    stale body raises — A's exhaust must update NOTHING. SAFE: the loop
    row still running at iteration 1 with B's carry, the flow root live,
    NO escalation row written; B then completes the loop with its own
    count."""
    gate_a = asyncio.Event()
    gate_b = asyncio.Event()
    parked_a = asyncio.Event()
    parked_b = asyncio.Event()
    spawns: list[tuple[int, int]] = []

    async def gated_body(ctx: StepContext, carry: object) -> Done[Carry] | Refine[Carry]:
        c = carry if isinstance(carry, Carry) else Carry()
        if ctx.attempt == 1:
            # THE ZOMBIE's stale iteration-0 body: parked past the lease,
            # then its timed-out dependency finally errors.
            parked_a.set()
            await gate_a.wait()
            raise ValueError("the stale attempt's dependency timed out")
        iteration = c.acc
        spawns.append((ctx.attempt, iteration))
        if iteration == 1:
            parked_b.set()
            await gate_b.wait()  # B parks MID-LOOP, healthy, the row running
        if iteration >= 2:
            return Done(Carry(acc=iteration + 1, by="B"))
        return Refine(Carry(acc=iteration + 1, by="B"))

    app = WorkflowApp()

    @app.workflow("aloop_zombie")
    def _wf() -> Promise[object]:
        return build(loop("counter", gated_body, max_iterations=5))

    runner_a = FlowRunner(app.get("aloop_zombie"), wf_pool, wf_schema)
    flow_id = (await runner_a.create_flow()).flow_id
    drive_a = asyncio.create_task(runner_a.drive(flow_id, max_ticks=10))
    await asyncio.wait_for(parked_a.wait(), timeout=10)
    loop_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'counter' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    # THE ESTATE RECLAIM (the lease expired — the zombie lost the row):
    # the re-pend shape, claimable again.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', locked_by_worker = NULL, "
        "scheduled_at = now() WHERE id = $1",
        loop_id,
    )
    # B — the healthy re-claimer (a FRESH driver identity).
    runner_b = FlowRunner(app.get("aloop_zombie"), wf_pool, wf_schema)
    drive_b = asyncio.create_task(runner_b.drive(flow_id, max_ticks=60))
    await asyncio.wait_for(parked_b.wait(), timeout=10)
    mid = await wf_conn.fetchrow(
        f"SELECT status, (metadata->>'iteration')::int AS it, metadata->>'carry' AS carry "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        loop_id,
    )
    assert mid is not None and mid["status"] == "running" and mid["it"] == 1, (
        f"the interleave setup failed (B not mid-loop): {dict(mid) if mid else None}"
    )

    # THE ZOMBIE WAKES: its stale body raises; its exhaust must not land.
    gate_a.set()
    verdict_a: object
    try:
        verdict_a = await asyncio.wait_for(drive_a, timeout=30)
    except Exception as exc:
        verdict_a = f"raised {type(exc).__name__}: {exc}"
    strike = await wf_conn.fetchrow(
        f"SELECT status, error_class, (metadata->>'iteration')::int AS it, "
        f"metadata->>'carry' AS carry FROM \"{wf_schema}\".jobs WHERE id = $1",
        loop_id,
    )
    strike_root = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    strike_outbox = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE flow_id = $1', flow_id
    )
    # Let B finish; capture everything, assert at the end (the teardown
    # stays clean on the red path).
    gate_b.set()
    verdict_b = await asyncio.wait_for(drive_b, timeout=30)
    final_root = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    result = await runner_b.result(flow_id) if final_root == "succeeded" else None

    assert strike is not None
    assert strike["status"] == "running" and strike["it"] == 1 and "B" in (strike["carry"] or ""), (
        f"F-LOOP-2: the zombie's stale strike LANDED on the live loop — the row is "
        f"{strike['status']}/{strike['error_class']} at iteration {strike['it']} with "
        f"carry {strike['carry']} (B was mid-drive, healthy). The exhaust accepted a "
        f"writer that had lost the claim (strike root: {strike_root}, outbox rows "
        f"written by the zombie: {strike_outbox}, drive_a: {verdict_a})"
    )
    assert strike_root == "running", strike_root
    assert strike_outbox == 0, "the zombie's refused exhaust still wrote the escalation row"
    assert verdict_b == "terminal" and final_root == "succeeded", (verdict_b, final_root)
    assert result == {"acc": 3, "by": "B"}, result


async def test_f_loop_5_the_sweep_counts_every_exhaustion(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``on_exhausted="fail"``, orphaned at the cap with an expired
    lease: the sweep exhausts the loop (the named state fires, the flow
    terminalizes) and the RETURN counts it — "Returns the number of
    loops exhausted" is the docstring's own claim."""

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        return Refine({"n": 1})

    app = WorkflowApp()

    @app.workflow("aloop_fail_policy")
    def _wf() -> Promise[object]:
        return build(loop("counter", refine_forever, max_iterations=3, on_exhausted="fail"))

    runner = FlowRunner(app.get("aloop_fail_policy"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # ORPHAN the loop at the cap (the worker died mid-iteration; the
    # lease expired, as the real window converges to).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', "
        "metadata = metadata || $2::jsonb, locked_by_worker = $3, "
        "lock_expires_at = now() - interval '1 hour' "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
        json.dumps({"iteration": 3, "max_iterations": 3, "kind": "loop"}),
        new_uuid(),
    )
    exhausted = await sweep_loop_budget(wf_pool, runner.wsql)
    node = await wf_conn.fetchrow(
        f"SELECT status, metadata->>'iteration_state' AS state FROM \"{wf_schema}\".jobs "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node is not None
    assert node["status"] == "failed" and node["state"] == "iteration_cap_exhausted", (
        f"the orphaned at-cap loop was not exhausted by the sweep: {dict(node)}"
    )
    assert exhausted == 1, (
        f"F-LOOP-5: the sweep exhausted the loop (row failed, iteration_cap_exhausted) "
        f"but returned {exhausted} — the count increments only on the "
        "escalate-enqueued path; the return lies for the fail policy"
    )


# ── F-LOOP-6: the promised 'waits forever' validate warning ─────────────
# CURED (this head): the W3-eternal-loop warning rule landed — the
# docstring's promise is the shipped behavior; the marker is gone, the
# green IS the receipt.


def test_f_loop_6_validate_warns_on_the_waits_forever_loop() -> None:
    """The promised guard: no ``until=``, no ``max_iterations``, no
    ``budget_s`` — the "waits forever" class loop()'s own docstring
    names. validate() must WARN (the warning class: probably wrong,
    never a refusal — the zero-false-positive doctrine's sibling)."""
    from taskq.workflows.api._validate import _run_rules

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        return Refine({"n": 1})

    app = WorkflowApp()

    @app.workflow("aloop_nowalls")
    def _wf() -> Promise[object]:
        return build(loop("counter", refine_forever))

    diagnostics = _run_rules(app.get("aloop_nowalls"))
    warnings = [d for d in diagnostics if d.severity == "warning"]
    assert any("forever" in f"{d.rule} {d.message}".lower() for d in warnings), (
        f"F-LOOP-6: the no-walls loop validated with NO 'waits forever' warning "
        f"(diagnostics: {[(d.rule, d.severity) for d in diagnostics]}) — the "
        "docstring's promised validate warning does not exist"
    )


# ── F-LOOP-7: E8 convicts one seam late (registration, not validate) ────


# ── F-LOOP-7: E8 convicts one seam late (registration, not validate) ────
# CURED (this head): the registration door validates — app.get() runs
# validate_compiled before recording/registering, so the mismatched
# carrier is refused before any row exists; the marker is gone, the
# green IS the receipt.


def test_f_loop_7_the_carrier_type_refusal_fires_at_registration() -> None:
    """The seam-late conviction: a body ``-> Refine[Foo] | Done[Foo]``
    with ``initial=Bar()`` must be refused at the compile/registration
    door — "refuse at compile" is E8's own docstring's claim; today the
    refusal lives one seam later (the first execution door)."""

    class Foo(BaseModel):
        a: int = 0

    class Bar(BaseModel):
        b: str = ""

    async def foo_body(ctx: StepContext, carry: object) -> Refine[Foo] | Done[Foo]:
        return Refine(Foo(a=1))

    app = WorkflowApp()

    @app.workflow("aloop_e8_late")
    def _wf() -> Promise[object]:
        return build(loop("counter", foo_body, initial=Bar(), max_iterations=2))

    from taskq.workflows.api._validate import WorkflowValidationError

    with pytest.raises(WorkflowValidationError, match="E8-carrier-type"):
        app.get("aloop_e8_late")


# ── GUARD (a): the walls live in the SWEEP, never the body ──────────────


async def test_guard_the_budget_wall_fires_from_the_sweep_with_the_driver_dead(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """GUARD (the attack FAILED — the front's A2a): the budget wall is
    enforced BY THE SWEEP with the driver hard-killed mid-iteration (the
    SIGKILL shape: the task cancelled, no cleanup runs) — the named
    state, the typed failure class, the flow terminal, and the body
    spawned EXACTLY ONCE (the wall never lived in the body)."""
    gate = asyncio.Event()
    spawns: list[int] = []

    async def blocking_body(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        spawns.append(1)
        gate.set()
        await asyncio.sleep(3600)
        return Refine({"n": len(spawns)})

    app = WorkflowApp()

    @app.workflow("aloop_guard_budget")
    def _wf() -> Promise[object]:
        return build(loop("counter", blocking_body, max_iterations=100, budget_s=0.4))

    runner = FlowRunner(app.get("aloop_guard_budget"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    drive_task = asyncio.create_task(runner.drive(flow_id, max_ticks=5))
    await asyncio.wait_for(gate.wait(), timeout=5)
    drive_task.cancel()  # SIGKILL's in-process shape: no reclaim runs
    with contextlib.suppress(asyncio.CancelledError):
        await drive_task
    # The deadline (PG's clock) passes; NO driver ever runs again.
    await asyncio.sleep(0.7)
    exhausted = await sweep_loop_budget(wf_pool, runner.wsql)
    node = await wf_conn.fetchrow(
        f"SELECT status, error_class, metadata->>'iteration_state' AS state "
        f"FROM \"{wf_schema}\".jobs WHERE step_key = 'counter' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert node is not None
    assert exhausted >= 1 and len(spawns) == 1, (exhausted, len(spawns))
    assert node["state"] == "budget_exhausted", dict(node)
    assert node["error_class"] == "LoopBudgetExhausted", dict(node)
    assert node["status"] == "failed", dict(node)
    assert root == "failed", root


async def test_guard_the_cap_wall_fires_from_the_sweep_on_the_crash_window_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """GUARD (the attack FAILED — the front's A2b): a REAL driver runs
    the always-Refine body to the cap (the advance guard lets the
    counter REACH max), then dies before the top-of-loop cap check (the
    until= predicate raises CancelledError on the killing pass — the
    crash window). The orphaned running row at the cap, lease expired as
    the real window converges to, is exhausted BY THE SWEEP — the body
    spawned EXACTLY max_iterations times."""
    spawns: list[int] = []
    calls = {"n": 0}

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        spawns.append(1)
        return Refine({"n": len(spawns)})

    async def killing_until() -> bool:
        # The pass where iteration == max (the counter reached the cap):
        # the worker dies HERE — before the driver's cap check.
        calls["n"] += 1
        if calls["n"] > 5:
            raise asyncio.CancelledError("the worker died after the final advance")
        return False

    app = WorkflowApp()

    @app.workflow("aloop_guard_cap")
    def _wf() -> Promise[object]:
        return build(loop("counter", refine_forever, max_iterations=5, until=killing_until))

    runner = FlowRunner(app.get("aloop_guard_cap"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    drive_task = asyncio.create_task(runner.drive(flow_id, max_ticks=10))
    with contextlib.suppress(asyncio.CancelledError):
        await drive_task
    # The real window's convergence: the dead worker's lease expires.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET lock_expires_at = now() - interval '1 second' "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    exhausted = await sweep_loop_budget(wf_pool, runner.wsql)
    node = await wf_conn.fetchrow(
        f"SELECT status, error_class, metadata->>'iteration_state' AS state "
        f"FROM \"{wf_schema}\".jobs WHERE step_key = 'counter' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert node is not None
    assert len(spawns) == 5, f"the cap bounds TOTAL SPAWNS — observed {len(spawns)}"
    assert exhausted >= 1, "the sweep missed the orphaned at-cap row"
    assert node["state"] == "iteration_cap_exhausted", dict(node)
    assert node["error_class"] == "IterationLimitExhausted", dict(node)
    assert node["status"] == "failed", dict(node)
    assert root == "failed", root


# ── GUARD (b): the crash window heals exactly-once ──────────────────────


async def test_guard_the_crash_window_between_terminal_and_advance_heals_exactly_once(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GUARD (the attack FAILED — the front's A1a): the kill lands
    BETWEEN the iteration's committed terminal write and the carry
    advance (the pg_terminate_backend window, injected at the classified
    seam the shipped pin #6 uses). The reclaim records 'crashed' (the
    ladder untouched), the memo replay heals the window, the ledger
    shows EXACTLY ONE succeeded row per iteration key, and the result is
    correct. (The RESUME's carry TYPE is F-LOOP-1's pin — this guard
    pins the ledger's exactly-once, which held.)"""
    from taskq.workflows.api import _runner_loop

    async def counting_body(ctx: StepContext, carry: object) -> object:
        acc = carry.acc + 1 if isinstance(carry, Counter) else 1
        await asyncio.sleep(0)
        return Done(Counter(acc=acc)) if acc >= 3 else Refine(Counter(acc=acc))

    app = WorkflowApp()

    @app.workflow("aloop_guard_heal")
    def _wf() -> Promise[object]:
        return build(loop("counter", counting_body, initial=Counter(acc=0), max_iterations=5))

    real_record = _runner_loop.LoopOps._record_iteration_terminal
    kills = {"n": 0}

    async def killing_record(
        self: object, flow_id: object, iter_key: str, attempt: int, outcome: object
    ) -> None:
        await real_record(self, flow_id, iter_key, attempt, outcome)
        kills["n"] += 1
        if kills["n"] == 1:
            raise asyncpg.exceptions.ConnectionDoesNotExistError(
                "pg_terminate_backend landed between the terminal write and the advance"
            )

    monkeypatch.setattr(_runner_loop.LoopOps, "_record_iteration_terminal", killing_record)
    runner = FlowRunner(app.get("aloop_guard_heal"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, max_ticks=30)
    monkeypatch.undo()
    # THE RE-DRIVE (the reclaim's re-claim — a fresh driver identity).
    runner2 = FlowRunner(app.get("aloop_guard_heal"), wf_pool, wf_schema)
    assert await runner2.drive(flow_id, max_ticks=50) == "terminal"
    assert await runner2.result(flow_id) == {"acc": 3}
    timeline = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key LIKE 'counter.iter%' ORDER BY id",
        flow_id,
    )
    succeeded_keys = [r["step_key"] for r in timeline if r["status"] == "succeeded"]
    assert len(succeeded_keys) == len(set(succeeded_keys)) and len(succeeded_keys) >= 3, (
        f"the iteration ledger is not exactly-once: {timeline}"
    )
    crashed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'crashed'",
        flow_id,
    )
    failed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        flow_id,
    )
    assert crashed == 1, f"the reclaim's 'crashed' record is missing ({crashed})"
    assert failed == 0, f"the crash burned the ladder ({failed} failed rows)"


# ── GUARD (c): the held iteration is invisible to the budget arm ────────


async def test_guard_the_held_iteration_is_invisible_to_the_budget_arm(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """GUARD (the attack FAILED — the front's A2d, the arm's heart): a
    ``budget_paused`` row with its deadline FORCED INTO THE PAST
    survives the sweep untouched (holds are free); the un-paused twin at
    the same deadline fires. (The arm WITHOUT ``AND NOT
    budget_paused`` — the CONSUME-BUDGET dragon — is the mutation
    drill's red in the shipped pins; this guard pins the fence as this
    front independently re-proved it.)"""
    app = WorkflowApp()

    @app.workflow("aloop_guard_held")
    def _wf() -> Promise[object]:
        return build(loop("counter", None, budget_s=600.0))

    runner = FlowRunner(app.get("aloop_guard_held"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # Both loop rows: 'running', deadline forced into the past.
    for paused in (True, False):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, budget_deadline, budget_paused) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'heldl', "
            "$2::jsonb, now() - interval '1 hour', $3)",
            new_uuid(),
            json.dumps(
                {"flow_id": str(flow_id), "kind": "loop", "iteration": 0, "max_iterations": 100}
            ),
            paused,
        )
    await sweep_loop_budget(wf_pool, runner.wsql)
    states = await wf_conn.fetch(
        f"SELECT budget_paused, status, metadata->>'iteration_state' AS state "
        f"FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'heldl' ORDER BY budget_paused",
        flow_id,
    )
    by_paused = {r["budget_paused"]: r for r in states}
    assert len(by_paused) == 2
    assert by_paused[True]["status"] == "running", (
        "the HELD iteration's budget fired — the arm's heart (AND NOT budget_paused) "
        "is missing: the CONSUME-BUDGET dragon is loose"
    )
    assert by_paused[False]["status"] == "failed"
    assert by_paused[False]["state"] == "budget_exhausted"


# ── F-LOOP-8: the loop-parents gap — a promise handle as the initial carry ──


def test_f_loop_8_validate_refuses_a_promise_as_the_loop_initial_carry() -> None:
    """THE LOOP-PARENTS GAP, refused at the construction door (the E11
    rule). The convicted shape (probe-convicted at this head's pre-cure
    tree): ``loop("scan", body, initial=some_promise)`` — the natural
    reading of "the loop starts from the parent's result" — threaded the
    PROMISE HANDLE itself into the carry; the first claim died
    ``UnencodableValue: Type is not JSON serializable: Promise`` —
    mid-flow, untyped by any compile rule, after the rows existed. The
    cure: validate() REFUSES the handle (E11-loop-promise-carry, error)
    and names the fix (a first step returns the initial carry; the loop
    starts from that value). The honest alternatives still build: a
    VALUE initial validates clean, and the loop-with-no-walls warning
    (F-LOOP-6's subject) is unaffected."""
    from taskq.workflows.api._validate import _run_rules

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        return Refine({"n": 1})

    async def produce_body(ctx: StepContext) -> dict[str, int]:
        return {"n": 5}

    app = WorkflowApp()

    @app.workflow("aloop_pinit")
    def _wf() -> Promise[object]:
        p = step(produce_body, key="produce")
        lp = loop("scan", refine_forever, initial=p, max_iterations=10)
        return build(lp, p)

    # THE REGISTRATION DOOR (the F-LOOP-7 cure's own seam) refuses the
    # handle BEFORE any row exists — the named rule, the named fix.
    from taskq.workflows.api._validate import WorkflowValidationError

    with pytest.raises(WorkflowValidationError, match="E11-loop-promise-carry"):
        app.get("aloop_pinit")

    @app.workflow("aloop_value_init")
    def _wf_ok() -> Promise[object]:
        return build(loop("counter", refine_forever, initial={"n": 0}, max_iterations=10))

    # The honest alternative still builds clean: a VALUE initial carries
    # through the door, and the clean graph's own diagnostics carry no
    # E11 (the refusal never over-fires onto a value carry).

    ok_diagnostics = _run_rules(app.get("aloop_value_init"))
    assert not any(d.rule == "E11-loop-promise-carry" for d in ok_diagnostics), (
        "a VALUE initial carry was convicted — the refusal over-fires"
    )
