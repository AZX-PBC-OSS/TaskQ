"""LedgerWatch's workflows — the wiring IS the product.

Three graphs:

``ledger_watch``
    ingest -> enrich (the fan-out, per-item ladder) -> branch (identity
    pass-through, the typed sum's seat) -> TYPED ROUTE (high-risk -> the
    review run opens; clearable -> auto-clear) -> report.

``txn_review``
    ONE transaction's human approval: the HITL hold (``wait_signal``
    with a timeout); the resolved arm returns the typed verdict; the
    Expired arm escalates to the compliance queue (durable, idempotent).

``reconciliation``
    the durable timer (a hold whose DB-clock expiry IS the tick — the
    body ENFORCES the schedule, never trusts the wake) -> the pass ->
    the typed report.

WHY THE REVIEW IS ITS OWN RUN (an adoption shape, recorded with the
evidence that forced it): a hold's identity is
``(workflow_id, node_key, signal_name, hold_epoch)`` — map SIBLINGS of
one step share ``node_key``, so two HIGH-risk transactions holding
CONCURRENTLY in one map collide on one hold row: one answer wakes one
sibling, the other wedges (observed both in-process and in worker mode;
filed as friction F2). The per-transaction review run gives every hold
its own node key — the concurrent-holds promise holds — and the review
run's id is the address the operator's console resolves.

THE DEPS SEAM: one ``LedgerWatchDeps`` instance bound at the app's
door; bodies opt in by declaring the parameter. No module globals, no
reaching through ``ctx``.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg
from pydantic import BaseModel

from examples.ledgerwatch.config import (
    QUEUE_CPU,
    QUEUE_ENRICH,
    QUEUE_RECON,
    DynamicsProfile,
    LedgerWatchSettings,
    load_profile,
    load_settings,
)
from examples.ledgerwatch.enrichment import MockEnrichmentAPI, TransientEnrichmentError
from examples.ledgerwatch.models import (
    ApprovalDecision,
    ApprovedTxn,
    Batch,
    ClearableTxn,
    ClearedTxn,
    CompletionReport,
    EscalatedTxn,
    HighRiskTxn,
    PendingReviewTxn,
    ReconReport,
    ReconTick,
    RejectedTxn,
    ReviewRequest,
    ReviewVerdict,
    RoutingVerdict,
    TransactionRecord,
)
from examples.ledgerwatch.pooling import LazyPool
from examples.ledgerwatch.store import LedgerWatchStore
from taskq.workflows import (
    Expired,
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)

# ── the app registry (the wiring's cross-references) ────────────────────

#: The compiled faces by role (build_app records them; a body that
#: triggers ANOTHER workflow's run resolves the compiled face here —
#: the same app, the same definitions' registry).
_APPS: dict[str, WorkflowApp] = {}


@dataclass(frozen=True, slots=True)
class LedgerWatchDeps:
    """The ONE deps instance the app binds (the DI capability's door).

    The pool is handed out LAZILY (:class:`LazyPool`): the deps
    instance is constructed at import time (the worker's boot
    projection reads the binding from the compiled app), while the pool
    belongs to whichever loop first needs it — the worker's boot loop
    is not the worker's run loop.
    """

    dsn: str
    schema: str
    settings: LedgerWatchSettings
    profile: DynamicsProfile
    pools: LazyPool

    async def pool(self) -> asyncpg.Pool:
        return await self.pools.get()

    @property
    def store(self) -> LedgerWatchStore:
        return LedgerWatchStore(self.pools, self.schema, self.settings)

    @property
    def enrichment(self) -> MockEnrichmentAPI:
        return MockEnrichmentAPI(self.pools, self.schema, self.profile)


def build_app(
    dsn: str,
    schema: str,
    settings: LedgerWatchSettings | None = None,
    profile: DynamicsProfile | None = None,
    pools: LazyPool | None = None,
) -> WorkflowApp:
    """The app factory: the deps bound ONCE at the door, all three
    graphs wired."""
    app = WorkflowApp(
        deps=LedgerWatchDeps(
            dsn=dsn,
            schema=schema,
            settings=settings or load_settings(),
            profile=profile or load_profile(),
            pools=pools or LazyPool(dsn),
        )
    )
    # The compiled faces, bound BEFORE any body can reach them.
    _APPS["review"] = app
    _wire_ledger_watch(app)
    _wire_txn_review(app)
    _wire_reconciliation(app)
    return app


# ── the ledger_watch bodies ──────────────────────────────────────────────


class IngestProgress(BaseModel):
    """The ingest node's declared progress shape (the declared-schema
    law: the declaration is what makes a separate UI render it)."""

    txn_count: int


async def ingest_body(
    ctx: StepContext, params: Batch, deps: LedgerWatchDeps
) -> list[TransactionRecord]:
    """The run's OWN input is the truth (the caller's input is never
    accepted-and-ignored): the wiring's declared batch is the default
    the trigger's ``create_flow(input=…)`` overrides. The delivery is
    recorded at the app's own table (the at-least-once feed's dedup
    face)."""
    raw = ctx.input if ctx.input is not None else params
    batch = Batch.model_validate(raw)
    await deps.store.record_batch(batch)
    await ctx.progress(
        10,
        f"ingested {len(batch.transactions)} transactions",
        {"txn_count": len(batch.transactions)},
    )
    return batch.transactions


async def enrich_item(
    ctx: StepContext, txn: TransactionRecord, deps: LedgerWatchDeps
) -> HighRiskTxn | ClearableTxn:
    """Per-transaction enrichment — the map's item child. The EXTERNAL
    call rides the mock API (the adverse world lives there).

    THE CLIENT'S OWN RETRY POLICY (recorded — finding F3): the
    framework's map-item ladder caps at 3 attempts regardless of the
    wired ``max_attempts`` (the row carries the value; the ladder does
    not honor it — the always-fail probe's evidence is in the finding
    ledger), so the deep budget for TRANSIENT enrichment errors is
    here, in the body: every retry is its own journalled call, and the
    engine's ladder stays for what ladders are for (crashes, reclaims).

    THE CALL IS NOT SUBSTEP-LEDGERED (a measured adoption decision):
    the substep's replay-dedup taxed the fan-out EIGHT-fold (A/B on a
    300-child batch: max in-flight 32 -> 4, 10.1 s -> 27.1 s — the
    ledger claim serializes concurrent map children; finding F4), and
    the enrichment call tolerates the at-least-once boundary (the
    verdict is a pure function of the transaction; a body re-run after
    a crash re-calls the API and lands the SAME verdict). A substep
    stays for the calls that canNOT tolerate a re-run."""
    await ctx.progress(None, f"enriching {txn.txn_id}", None)
    last = deps.settings.enrich_client_retries - 1
    for try_index in range(deps.settings.enrich_client_retries):
        try:
            return await deps.enrichment.enrich(txn)
        except TransientEnrichmentError:
            if try_index == last:
                raise  # the engine's ladder takes it from here
            await asyncio.sleep(0.02 + 0.06 * try_index)  # the client's backoff
    raise AssertionError("unreachable: the last retry either returns or raises")


async def branch_body(
    ctx: StepContext, enriched: list[HighRiskTxn | ClearableTxn]
) -> list[HighRiskTxn | ClearableTxn]:
    """The branch's seat: the identity pass-through over the collect.

    WHY THIS NODE EXISTS (a framework shape, recorded): the typed route
    attaches to a wired SOURCE node — a map's derived join declares no
    body, so the route cannot hang off ``enrich.join`` directly. The
    pass-through is the honest spelling of "the route's source is the
    enriched collect".
    """
    _ = ctx
    return enriched


async def queue_for_review(
    ctx: StepContext, txn: HighRiskTxn, deps: LedgerWatchDeps
) -> PendingReviewTxn:
    """THE HIGH-RISK ARM: open ONE review run for the transaction
    (idempotent by the run key — the feed's re-delivery and this body's
    own re-execution land on the SAME run). The hold lives THERE; this
    pipeline continues."""
    batch = Batch.model_validate(ctx.input) if ctx.input is not None else None
    runner = FlowRunner(_APPS["review"].get("txn_review"), await deps.pool(), deps.schema)
    claim = await runner.create_flow(
        input=ReviewRequest(
            txn_id=txn.txn_id,
            batch_id=batch.batch_id if batch else "",
            amount_cents=txn.amount_cents,
            score=txn.score,
            reasons=txn.reasons,
        ),
        run_key=f"txn_review:{txn.txn_id}",
    )
    return PendingReviewTxn(
        kind="pending_review", txn_id=txn.txn_id, review_run_id=str(claim.flow_id)
    )


async def auto_clear(ctx: StepContext, txn: ClearableTxn) -> ClearedTxn:
    """THE CLEAR ARM: below the threshold, the machine's word is final."""
    _ = ctx
    return ClearedTxn(kind="cleared", txn_id=txn.txn_id)


async def report_body(
    ctx: StepContext, verdicts: list[RoutingVerdict], deps: LedgerWatchDeps
) -> CompletionReport:
    """The typed terminal receipt, DERIVED from the run's own collect:
    the failed set is the batch's residual (a transaction that produced
    no verdict) — a report that could not name it would be a report
    that lies."""
    batch = Batch.model_validate(ctx.input)
    seen = {v.txn_id for v in verdicts}
    report = CompletionReport(
        batch_id=batch.batch_id,
        total=len(batch.transactions),
        cleared=sorted(v.txn_id for v in verdicts if isinstance(v, ClearedTxn)),
        pending_review=sorted(v.txn_id for v in verdicts if isinstance(v, PendingReviewTxn)),
        failed=sorted(t.txn_id for t in batch.transactions if t.txn_id not in seen),
    )
    await deps.store.record_report(report)
    await ctx.progress(100, "report written", {"batch_id": batch.batch_id})
    return report


# ── the txn_review bodies ────────────────────────────────────────────────


async def review_body(ctx: StepContext, req: ReviewRequest, deps: LedgerWatchDeps) -> ReviewVerdict:
    """THE HITL FACE: the typed wait with a timeout. The resolved arm
    returns the verdict; the Expired arm escalates to the compliance
    queue (the fail-close the typed union forces) — ONE durable row,
    ever (ON CONFLICT no-op under any re-execution). The run's OWN
    input is the truth (the wiring's declared request is the default)."""
    raw = ctx.input if ctx.input is not None else req
    req = ReviewRequest.model_validate(raw)
    outcome = await ctx.wait_signal(
        (ApprovalDecision,),
        timeout_s=deps.settings.approval_window_s,
        reason=(
            f"transaction {req.txn_id} is HIGH risk "
            f"(score {req.score}); approve to release, ignore to escalate"
        ),
        tool="compliance-console",
        args={"txn_id": req.txn_id, "amount_cents": req.amount_cents},
    )
    match outcome:
        case ApprovalDecision() as decision:
            if decision.approved:
                return ApprovedTxn(kind="approved", txn_id=req.txn_id, reviewer=decision.reviewer)
            return RejectedTxn(kind="rejected", txn_id=req.txn_id, note=decision.note)
        case Expired():
            await deps.store.escalate_to_compliance(req.txn_id, req.batch_id, str(ctx.flow_id))
            return EscalatedTxn(kind="escalated", txn_id=req.txn_id)


# ── the reconciliation bodies ────────────────────────────────────────────


async def _arm_timer(ctx: StepContext, deps: LedgerWatchDeps) -> str:
    """The schedule, memoized ONCE by the step ledger: the timer's
    deadline is the ARM time + the delay — a resume replays this value
    (never re-arms), so the schedule cannot drift under retries."""
    now = await deps.store.db_now()
    return (now + timedelta(seconds=deps.settings.recon_delay_s)).isoformat()


async def _run_recon_pass(deps: LedgerWatchDeps) -> tuple[str, ReconReport, object]:
    """The pass itself: the estate's health, derived from the rows."""
    pool = await deps.pool()
    runs = await pool.fetch(
        f'SELECT status, count(*) AS n FROM "{deps.schema}".jobs'  # noqa: S608  # Why: the schema identifier only; every caller value is bound.
        " WHERE step_key = '__flow__' GROUP BY status"
    )
    statuses = {r["status"]: r["n"] for r in runs}
    pending_holds = await pool.fetchval(
        f'SELECT count(*) FROM "{deps.schema}".wf_signals WHERE status = $1',  # noqa: S608  # Why: as above.
        "held",
    )
    escalated_total = await pool.fetchval(
        f'SELECT count(*) FROM "{deps.schema}".lw_compliance_queue'  # noqa: S608  # Why: as above.
    )
    report = ReconReport(
        pass_id=f"{time.time_ns():x}-{os.urandom(6).hex()}",
        runs_total=sum(statuses.values()),
        runs_succeeded=statuses.get("succeeded", 0),
        runs_failed=statuses.get("failed", 0),
        runs_running=statuses.get("running", 0),
        pending_holds=pending_holds or 0,
        escalated_total=escalated_total or 0,
    )
    fired_at = await deps.store.db_now()
    await deps.store.record_recon_pass(report, fired_at)
    return report.pass_id, report, fired_at


async def recon_body(ctx: StepContext, deps: LedgerWatchDeps) -> ReconReport:
    """THE DURABLE TIMER: the hold's DB-clock deadline IS the tick.

    The wake's face is the typed union: ``Expired`` = the timer fired
    (the normal face — nobody delivers this hold), a delivered
    ``ReconTick`` = an operator's early nudge. EITHER WAY the body
    ENFORCES the schedule against the DB clock: before the deadline the
    pass does not run — the body waits again on a new hold with the
    remaining time. A worker death mid-hold costs nothing: the hold row
    is in Postgres, the sweep's expiry arm is the only live timer, and
    the resume re-executes this body from the top (the armed schedule
    replays from the step ledger — cheap, exactly-once)."""
    _ = ctx
    scheduled_at = datetime.fromisoformat(
        await ctx.substep("arm-recon-timer", _arm_timer, ctx, deps)
    )
    _ = await ctx.wait_signal(
        (ReconTick,),
        timeout_s=deps.settings.recon_delay_s,
        reason="the scheduled reconciliation pass",
    )
    now = await deps.store.db_now()
    while now < scheduled_at:
        # An early wake (the operator's nudge) — the schedule holds.
        remaining = (scheduled_at - now).total_seconds()
        await ctx.wait_signal(
            (ReconTick,),
            timeout_s=max(remaining, 0.1),
            reason="the scheduled reconciliation pass (an early nudge was enforced away)",
        )
        now = await deps.store.db_now()
    _pass_id, report, _fired = await _run_recon_pass(deps)
    return report


# ── the wiring ───────────────────────────────────────────────────────────


def _wire_ledger_watch(app: WorkflowApp) -> None:
    @app.workflow("ledger_watch")
    def ledger_watch() -> Promise[object]:
        # THE SPLIT PLACEMENT (one actor, one queue): the ingest step
        # carries the ENRICH cohort (the map's children inherit the
        # source's actor); the branch, the arms and the report ride the
        # cpu cohort.
        ingested = step(
            ingest_body,
            Batch(batch_id="unwired", transactions=[]),
            key="ingest",
            actor="lw-enrich",
            queue=QUEUE_ENRICH,
            progress_schema=IngestProgress,
        )
        enriched = map_source(ingested, enrich_item, queue=QUEUE_ENRICH, max_attempts=6)
        branch = step(branch_body, enriched, key="branch", actor="lw-cpu", queue=QUEUE_CPU)
        routed = route(
            branch,
            {
                HighRiskTxn: RouteArm(body=queue_for_review, actor="lw-cpu", queue=QUEUE_CPU),
                ClearableTxn: RouteArm(body=auto_clear, actor="lw-cpu", queue=QUEUE_CPU),
            },
            queue=QUEUE_CPU,  # the route's own placement: the derived join's cohort
        )
        return build(step(report_body, routed, key="report", actor="lw-cpu", queue=QUEUE_CPU))


def _wire_txn_review(app: WorkflowApp) -> None:
    @app.workflow("txn_review")
    def txn_review() -> Promise[object]:
        return build(
            step(
                review_body,
                ReviewRequest(txn_id="unwired", batch_id="", amount_cents=0, score=0.0),
                key="review",
                queue=QUEUE_CPU,
                gates=(_approval_gate(),),
            )
        )


def _wire_reconciliation(app: WorkflowApp) -> None:
    @app.workflow("reconciliation")
    def reconciliation() -> Promise[object]:
        return build(
            step(
                recon_body,
                key="recon_timer",
                actor="lw-recon",
                queue=QUEUE_RECON,
                gates=(_recon_gate(),),
            )
        )


def _approval_gate():
    from taskq.workflows import GateDecl

    return GateDecl(
        name="ApprovalDecision",
        payload_models=(ApprovalDecision,),
        timeout_s=load_settings().approval_window_s,
    )


def _recon_gate():
    from taskq.workflows import GateDecl

    return GateDecl(
        name="ReconTick",
        payload_models=(ReconTick,),
        timeout_s=load_settings().recon_delay_s,
    )


#: THE app (the worker entry and the triggers import this): the env's
#: settings + the env's dynamics profile, recorded at the door.
app = build_app(
    dsn=os.environ.get("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq"),
    schema=os.environ.get("TASKQ_SCHEMA_NAME", "taskq"),
)
