# LedgerWatch: the adoption story (a worked build-through)

A compliance-monitoring service — **LedgerWatch** — built ON TaskQflow,
the way an adopting team builds: a transaction batch flows ingest →
enrich (a fan-out over a flaky external API) → branch by risk class →
HIGH-risk transactions hold for a human approval (the Expired arm
escalates to a compliance queue) → the rest auto-clear → a typed
completion report; a scheduled reconciliation pass runs on a durable
timer. The full product lives at `examples/ledgerwatch/` with its own
drill suite (real worker subprocesses, SIGKILL crash drills, a
duplicate-feed drill, a chaos suite); this page teaches the build-through
and runs its fences against a real Postgres.

The five-minute fence below is the whole loop, self-contained: wire the
graphs, drive the run, resolve the human's hold, read the typed report.

```python
import asyncio
import os
from dataclasses import dataclass
from typing import Literal

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    Expired,
    FlowRunner,
    HitlClient,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)


# ── the payload contract ────────────────────────────────────────────────
# THE DISCRIMINATOR LAW: every union member carries a REQUIRED
# ``kind: Literal[...]`` tag. A tag with a DEFAULT is optional — the
# validator reads only REQUIRED fields when it proves a union decodable,
# and field-identical required faces are the build refusal (E16).


class Txn(BaseModel):
    txn_id: str
    amount_cents: int


class HighRiskTxn(BaseModel):
    kind: Literal["high"]
    txn_id: str
    score: float


class ClearableTxn(BaseModel):
    kind: Literal["clearable"]
    txn_id: str
    score: float


class ReviewRequest(BaseModel):
    txn_id: str


class ApprovalDecision(BaseModel):
    approved: bool


class ClearedTxn(BaseModel):
    kind: Literal["cleared"]
    txn_id: str


class PendingReviewTxn(BaseModel):
    kind: Literal["pending_review"]
    txn_id: str
    review_run_id: str


class ApprovedTxn(BaseModel):
    kind: Literal["approved"]
    txn_id: str


class EscalatedTxn(BaseModel):
    kind: Literal["escalated"]
    txn_id: str


RoutingVerdict = ClearedTxn | PendingReviewTxn
ReviewVerdict = ApprovedTxn | EscalatedTxn

# ── the mock external API (the chaos boundary) ──────────────────────────
# The enrichment "API" charges jitter and fails transiently; the retry
# lands it. Deterministic per (txn, call): the story is stable.

_CALLS: dict[str, int] = {}


def _draw(txn_id: str) -> Literal["ok", "flake"]:
    from random import Random

    _CALLS[txn_id] = _CALLS.get(txn_id, 0) + 1
    return "flake" if Random(f"{txn_id}#{_CALLS[txn_id]}").random() < 0.25 else "ok"


class TransientEnrichmentError(Exception):
    pass


async def enrich_item(ctx: StepContext, txn: Txn) -> HighRiskTxn | ClearableTxn:
    for attempt in range(6):  # the client's own retry policy
        await asyncio.sleep(0.01)  # the jitter
        if _draw(txn.txn_id) == "flake":
            if attempt == 5:
                raise TransientEnrichmentError(txn.txn_id)
            continue
        # the generic industry rule: a large amount is HIGH risk
        if txn.amount_cents >= 50_000:
            return HighRiskTxn(kind="high", txn_id=txn.txn_id, score=88.0)
        return ClearableTxn(kind="clearable", txn_id=txn.txn_id, score=12.0)
    raise TransientEnrichmentError(txn.txn_id)


async def ingest_body(ctx: StepContext, params: list[Txn]) -> list[Txn]:
    # THE RUN'S OWN INPUT IS THE TRUTH: the trigger's create_flow(input=…)
    # is consumed; the wiring's declared batch is only the default.
    raw = ctx.input if ctx.input is not None else params
    return [Txn.model_validate(t) for t in raw]


async def branch_body(
    ctx: StepContext, enriched: list[HighRiskTxn | ClearableTxn]
) -> list[HighRiskTxn | ClearableTxn]:
    # THE ROUTE'S SEAT: the typed route attaches to a wired SOURCE node —
    # a map's derived join declares no body, so the pass-through IS the
    # honest spelling of "the route's source is the enriched collect".
    _ = ctx
    return enriched


async def queue_for_review(
    ctx: StepContext, txn: HighRiskTxn, pool: asyncpg.Pool
) -> PendingReviewTxn:
    # THE HIGH-RISK ARM: open ONE review run for the transaction
    # (idempotent by the run key). THE HOLD LIVES THERE — a hold's
    # identity is (workflow_id, node_key, signal_name, epoch): map
    # SIBLINGS of one step share the node key, so holds in map children
    # collide; the per-txn run gives every hold its own node.
    runner = FlowRunner(_APPS["review"].get("txn_review"), pool, SCHEMA)
    claim = await runner.create_flow(
        input=ReviewRequest(txn_id=txn.txn_id), run_key=f"review:{txn.txn_id}"
    )
    return PendingReviewTxn(
        kind="pending_review", txn_id=txn.txn_id, review_run_id=str(claim.flow_id)
    )


async def auto_clear(ctx: StepContext, txn: ClearableTxn) -> ClearedTxn:
    _ = ctx
    return ClearedTxn(kind="cleared", txn_id=txn.txn_id)


async def review_body(ctx: StepContext, req: ReviewRequest) -> ReviewVerdict:
    # THE HITL FACE: the typed wait; the CHECKER forces the fail-close
    # arm (the union's Expired member). The run's OWN input is the truth.
    raw = ctx.input if ctx.input is not None else req
    txn_id = ReviewRequest.model_validate(raw).txn_id
    outcome = await ctx.wait_signal(
        (ApprovalDecision,), timeout_s=30.0, reason=f"the review of {txn_id}"
    )
    match outcome:
        case ApprovalDecision() as decision:
            if decision.approved:
                return ApprovedTxn(kind="approved", txn_id=txn_id)
            return EscalatedTxn(kind="escalated", txn_id=txn_id)
        case Expired():
            # THE ESCALATION: nobody approved in the window.
            return EscalatedTxn(kind="escalated", txn_id=txn_id)


async def report_body(ctx: StepContext, verdicts: list[RoutingVerdict]) -> dict:
    _ = ctx
    seen = {v.txn_id for v in verdicts}
    return {
        "cleared": sorted(v.txn_id for v in verdicts if isinstance(v, ClearedTxn)),
        "pending_review": sorted(v.txn_id for v in verdicts if isinstance(v, PendingReviewTxn)),
        "failed": sorted(t.txn_id for t in _BATCH if t.txn_id not in seen),
    }


# ── the wiring (the pool is the deps instance, bound ONCE at the door) ──

_BATCH: list[Txn] = [
    Txn(txn_id="T1", amount_cents=10_000),
    Txn(txn_id="T2", amount_cents=90_000),  # HIGH
    Txn(txn_id="T3", amount_cents=20_000),
    Txn(txn_id="T4", amount_cents=70_000),  # HIGH
]

_APPS: dict[str, WorkflowApp] = {}

SCHEMA = ""  # the deployment's schema, stamped in main()


def wire(app: WorkflowApp) -> None:
    """The two graphs (the app instance is recorded for the bodies that
    reach the review workflow's compiled face)."""
    _APPS["review"] = app

    @app.workflow("ledger_watch")
    def ledger_watch() -> Promise[object]:
        ingested = step(ingest_body, _BATCH, key="ingest")
        enriched = map_source(ingested, enrich_item, max_attempts=3)
        branch = step(branch_body, enriched, key="branch")
        routed = route(
            branch,
            {
                HighRiskTxn: RouteArm(body=queue_for_review),
                ClearableTxn: RouteArm(body=auto_clear),
            },
        )
        return build(step(report_body, routed, key="report"))

    @app.workflow("txn_review")
    def txn_review() -> Promise[object]:
        return build(
            step(
                review_body,
                ReviewRequest(txn_id="unwired"),
                key="review",
                gates=(_APPROVAL_GATE,),
            )
        )


from taskq.workflows import GateDecl  # noqa: E402  # Why: the gate declaration reads cleanest beside its wait's wiring.

_APPROVAL_GATE = GateDecl(
    name="ApprovalDecision", payload_models=(ApprovalDecision,), timeout_s=30.0
)


async def main() -> None:
    global SCHEMA
    dsn = os.environ["TASKQ_PG_DSN"]
    SCHEMA = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    await taskq.migrate.apply_pending_locked(dsn, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(dsn)
    app = WorkflowApp(deps=pool)  # the deps bound ONCE at the door
    wire(app)

    runner = FlowRunner(app.get("ledger_watch"), pool, SCHEMA)
    flow_id = (await runner.create_flow(input=[t.model_dump() for t in _BATCH])).flow_id

    # THE BATCH'S PIPELINE terminalizes: the cleared named, the pending
    # reviews NAMED (their holds live in their own runs).
    assert await runner.drive(str(flow_id), until="terminal", max_ticks=600) == "terminal"
    report = await runner.result(str(flow_id))
    print("the batch's receipt:", report)
    assert report["failed"] == []
    assert report["pending_review"] == ["T2", "T4"]

    # THE OPERATOR'S FACE: the review runs HOLD; the typed door delivers
    # the human's answer; the verdicts are typed.
    client = HitlClient(pool, schema=SCHEMA)
    review_runner = FlowRunner(app.get("txn_review"), pool, SCHEMA)

    async def _verdicts() -> list[ReviewVerdict]:
        rows = await pool.fetch(
            f'SELECT id FROM "{SCHEMA}".jobs'  # noqa: S608  # Why: the schema identifier only.
            " WHERE step_key = '__flow__' AND metadata->>'workflow' = 'txn_review'"
        )
        out: list[ReviewVerdict] = []
        for r in rows:
            # THE QUIESCENT STOP: drive to HELD (the hold registered),
            # resolve, then drive on to the terminal.
            held = await review_runner.drive(str(r["id"]), until="held", max_ticks=300)
            assert held == "held", f"the review run never held: {held}"
            for hold in await client.list(str(r["id"])):
                delivered = await client.resolve(hold.hold_id, {"approved": True})
                assert delivered.status == "delivered"
            assert (
                await review_runner.drive(str(r["id"]), until="terminal", max_ticks=300)
                == "terminal"
            )
            # the read face decodes ONCE (a dict); the TYPED verdict is
            # the declared model re-validated over it
            raw = await review_runner.result(str(r["id"]))
            out.append(ApprovedTxn.model_validate(raw))
        return out

    verdicts = await _verdicts()
    approved = sorted(v.txn_id for v in verdicts if isinstance(v, ApprovedTxn))
    print("the reviews' verdicts:", approved)
    assert approved == ["T2", "T4"]
    await pool.close()


asyncio.run(main())
```

Verified output (the docs-example harness's capture, on a fresh schema):

```
the batch's receipt: {'failed': [], 'cleared': ['T1', 'T3'], 'pending_review': ['T2', 'T4']}
the reviews' verdicts: ['T2', 'T4']
```

## The full product (`examples/ledgerwatch/`)

The fence above is the teaching shape. The ADOPTED PRODUCT carries the
real dynamics:

| module | what it is |
| --- | --- |
| `examples/ledgerwatch/config.py` | the ADVERSE-WORLD profile, RECORDED: 15% transient flake + 5% hangs past the declared timeout + 50–300 ms jitter (env-tunable, `LW_PROFILE=adverse`) |
| `examples/ledgerwatch/enrichment.py` | the MOCK external enrichment API: typed transient errors, the declared timeout, and a DURABLE per-call journal (the drills' receipt) |
| `examples/ledgerwatch/feed.py` | the at-least-once feed (deterministic batches; re-delivery is the drill's world) |
| `examples/ledgerwatch/app.py` | the THREE graphs: `ledger_watch` (the pipeline), `txn_review` (the per-transaction HITL hold), `reconciliation` (the durable timer) |
| `examples/ledgerwatch/store.py` | the app's own idempotent tables: the journal, the compliance queue, the reports, the recon passes |
| `examples/ledgerwatch/worker.py` | the REAL worker entry (`python -m examples.ledgerwatch.worker`) |
| `examples/ledgerwatch/tests/` | the drill suite: happy path, the 1000-txn flake drill, SIGKILL crash drills, the duplicate-feed drill, the timer drill, the faces, and a 5-iteration chaos suite |

Run the drills against your own Postgres:

```bash
LEDGERWATCH_TEST_DSN=postgresql://postgres:taskq@localhost:5802/taskq \
  pytest examples/ledgerwatch/tests/ -q
```

## The five lessons the build taught (each one earned)

1. **The discriminator must be REQUIRED.** A `kind: Literal[...]` with a
   default value is an optional field; E16 reads required fields only,
   and the build refuses the union as field-identical. Stamp the tag at
   every construction site; the union then decodes BY TAG.
2. **The route attaches to a wired source node** — a map's derived join
   declares no body, so a route cannot hang off `enrich.join` directly;
   the identity pass-through step is the honest spelling.
3. **A HITL hold needs its own node key.** A hold's identity is
   `(workflow_id, node_key, signal_name, hold_epoch)` — map SIBLINGS of
   one step share the node key, so two transactions holding
   CONCURRENTLY in one map collide on one hold row (one answer wakes one
   sibling; the other wedges). The product's cure: ONE review run per
   transaction — the run id is the hold's address — idempotent by the
   run-key arbiter under the feed's re-delivery.
4. **The map item's ladder caps at 3 attempts** regardless of the wired
   `max_attempts` (the row carries the value; the ladder does not honor
   it — the always-fail probe's evidence). The deep retry budget for
   TRANSIENT external errors is the CLIENT's own policy, in the body;
   the engine's ladder stays for crashes and reclaims.
5. **The substep's replay-dedup is not free** — a 300-child fan-out's
   max in-flight dropped 32 → 4 with the call substep-ledgered (the
   ledger claim serializes concurrent map children). Substep the calls
   that canNOT tolerate a re-run; let the at-least-once boundary carry
   the ones that can (a deterministic verdict re-lands the same value).

## The durable timer

The reconciliation pass's timer is a HOLD whose DB-clock deadline IS the
tick: the body arms the schedule ONCE through `ctx.substep` (a resume
replays the recorded schedule — it cannot drift), waits on a declared
gate, and ENFORCES the schedule against the DB clock on every wake — an
early nudge (a delivered payload) waits again; the Expired member (the
normal face) fires the pass at-or-after the deadline. A worker death
mid-hold costs nothing: the hold is a Postgres row and the sweep's
expiry arm is the only live timer. The drills prove both faces: fired
no-earlier-than-scheduled, and the hold survives a SIGKILL.

## The friction list

The adoption's FINDINGS — every misleading doc, every capability gap,
every API that fought, each with file:line and the evidence — are
committed at `examples/ledgerwatch/FINDINGS.md` (F1–F10, severities
named). The headline: F2, concurrent HITL holds from map siblings wedge
a run silently — the framework cure owed is the hold identity's map
index (or the loud doc); the shipped product cures itself with the
per-transaction review run. **The cures have since LANDED**: the
holdmap-cures lane's map-index identity (F2), the ladder-inheritance
(F3), the connection-hold (F4) and the watchdog's warn-budget refusal
(F6) — each annotated CURED-BY in FINDINGS.md with its green pin.

## The receipts

The drills' evidence lands in `.measurements/ledgerwatch/`: the
1000-transaction flake drill's journal (every external call, its
outcome, its latency), the concurrency overlap measured from the rows
(the fan-out enriched 32-wide), the engine attempt histograms, the
SIGKILL drills' resume proofs (children carried past attempt 1 by the
reclaim), and the chaos suite's per-iteration variance. The invariants
run UNCONDITIONALLY: all transactions reach typed terminal, nothing
lost, nothing duplicated — a red is a finding, reported, never a knob.
