"""The workflow sweep arms (T04): the lock-first re-derive + its fire arm,
the outbox drain, the phantom-ledger reaper.

Each arm is its own function on its own statement constants (concerns
separate: the finalize owns tx1/tx2 in ``engine.py``, the arms own the
re-derivation and delivery). The arms share the §22.6 exclusivity law —
they never touch reclaim-owned rows — and the P3 rule 1 held-row
inertness invariant: a row whose ``scheduled_at`` is in the future (the
signal deadline, the only live timer on it) is invisible to every arm.

THE SWEEP = LOCK-FIRST RE-DERIVE: ``FOR UPDATE SKIP LOCKED`` the join-wait
children FIRST, then count un-terminal parents from the edge ledger inside
the same transaction. In-flight decrements are skipped this pass (the
healthy worker wins); committed decrements are visible to the count. Stale
writes impossible by construction — there is no snapshot-derived counter
write. SET-BASED, NOT N+1: ONE batched statement (the per-join round-trip
variant is the fanout proof's cut #4 crime; its MEASURED conviction is
P1's sweep-cost curve — 14.9 ms @ 200 joins, the unscoped seq-scan monster
83.7 ms @ 75k — the CITED-IMPORT artifact
``.measurements/runs/sweep-cost-scale-curve-*.json``: the spike's
session evidence directory is gone; the imported record IS the
provenance. The scope pin
convicts the unscoped shape. An earlier revision of this comment cited
the per-join variant at a p50/p95 figure for which no
capture exists; the dead figure is DELETED — provenance or
silence).

COUNTER-AS-CACHE / LEDGER-AS-TRUTH: the re-derive reconciles the CACHE
(``jobs.deps_pending``) from THE EDGE LEDGER's truth — never from
child-row presence (the EMPTY-JOIN dragon: nested joins' children do not
exist yet at count time).

Missing parents (a MISNAMED-CHILD edge) never fire silently: the blocked
arm stamps ``metadata.blocking_reason='orphan_parent'`` (the
blocked-with-reason state, §17.2's chains render from it) and the firable
arm excludes them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Final

import asyncpg
import structlog

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._progress import PROGRESS_RING_BOUND
from taskq.workflows._reducers import forget_flow_reducers, resolve_flow_reducer
from taskq.workflows._sql import (
    BLOCKING_REASON_BODY_UNAVAILABLE,
    BLOCKING_REASON_FAILED_PARENT,
    BLOCKING_REASON_FLOW_DEAD,
    BLOCKING_REASON_ORPHAN_PARENT,
    WorkflowSql,
)
from taskq.workflows._types import FiredJoin, _consumer_bindings, _jsonb, _metadata

__all__ = [
    "NODELESS_ROOT_REAP_GRACE_S",
    "SweepResult",
    "drain_outbox",
    "prune_delivered_outbox",
    "reap_nodeless_roots",
    "reap_phantom_ledger",
    "sweep_hold_stamps",
    "sweep_join_rederive",
    "sweep_progress_ring_prune",
]

#: The reap belt's grace (seconds): a nodeless root younger than this is
#: an in-flight create of a buggy shape, not debris — the belt never reaps
#: mid-flight. The clock comparison is PG's own (the DB-clock doctrine).
NODELESS_ROOT_REAP_GRACE_S: Final[float] = 300.0

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SweepResult:
    """One re-derive pass's outcome (counts are the statement's own)."""

    blocked: int
    reconciled: int
    firable: int
    #: T06's heal: fail_closed joins whose parent terminal-failed while
    #: their own tx2 never ran — blocked-with-reason by this pass.
    blocked_required: int = 0
    #: The H2 cure: never-fired join rows whose resolution the FLOW'S own
    #: death fenced (the fire's flow-status leg refuses a terminal flow) —
    #: stamped blocked-with-reason by this pass, never a hanging claimable
    #: join on a dead flow.
    flow_fenced: int = 0
    fired: tuple[FiredJoin, ...] = ()


async def sweep_join_rederive(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 200,
    orphan_blocking_reason: str = BLOCKING_REASON_ORPHAN_PARENT,
    failed_parent_blocking_reason: str = BLOCKING_REASON_FAILED_PARENT,
    flow_dead_blocking_reason: str = BLOCKING_REASON_FLOW_DEAD,
) -> SweepResult:
    """The lock-first re-derive arm: ONE batched statement (lock the
    join-wait children SKIP LOCKED → count un-terminal parents from the
    edge ledger → reconcile the cache → block the orphan-parent rows →
    block the fail-closed joins whose parent terminal-failed (T06's heal)
    → stamp the flow-fenced joins (the H2 cure: a never-fired join row on
    a TERMINAL flow can never fire again — the fence is not absorption,
    the arm resolves it blocked-with-reason) → report counts), then the
    set-based fire arm for the firable set
    (the flow-status leg rides INSIDE the fire statement — a post-cancel
    re-derive refuses; the unfenced variant is pin 5's red, kept forever).

    The whole pass runs in ONE transaction: the rederive statement's row
    locks are held until the fire arm + outbox rows commit, so the fire's
    re-derivation of the firable set is deterministic.

    THE FIRED JOIN'S REDUCER BODY (the tx1→tx2 crash window's cure, the
    CROSS-PROCESS form): the winner's body resolves DURABLY — from the
    REGISTERED DEFINITION of the workflow stamped on the flow root's
    metadata (every process carries the same definitions; the healer is
    never the finalizer's process — the fire statement returns the stamped
    name), falling through to the process-local reducer cache (the
    finalize warmed it — workflows/_reducers.py) only for a flow the
    registry cannot resolve. The body runs INSIDE this transaction — the
    same exactly-once boundary the finalize's tx2 states: a raising body
    rolls this tx back (the fire row and the outbox rows with it), the
    next pass re-fires, the body RE-RUNS — at-least-once body execution
    survives the window between the engine's own two transactions. A fired
    join with no resolvable body delivers its declared consumers — and is
    stamped ``metadata.blocking_reason='body_unavailable'`` + warned (the
    loudness asymmetry, R2-2: the delivery continues, the record is loud).
    """
    async with pool.acquire() as conn, conn.transaction():
        summary = await conn.fetchrow(
            wsql.rederive_sweep,
            batch_size,
            orphan_blocking_reason,
            failed_parent_blocking_reason,
            flow_dead_blocking_reason,
        )
        assert summary is not None  # the statement always returns its summary row
        firable = summary["firable"]
        fired: tuple[FiredJoin, ...] = ()
        if firable:
            # Ids minted app-side (uuid7 via the seam); the fire
            # statement re-derives the firable set under the row locks
            # this transaction still holds — deterministic — and
            # refuses rows beyond the minted pool (they stay firable
            # next pass, never NULL-id fired).
            fire_ids = [new_uuid() for _ in range(firable)]
            winners = await conn.fetch(wsql.sweep_fire, fire_ids, batch_size)
            fired_joins: list[FiredJoin] = []
            for w in winners:
                # THE BODY: the winner's reducer runs INSIDE this tx (the
                # exactly-once boundary is the FIRE's, never the body's —
                # a raising body rolls the whole pass back and the
                # re-derive re-fires; at-least-once body execution). The
                # resolution is DURABLE: the flow root's stamped workflow
                # name resolves the body from the REGISTERED DEFINITION —
                # a flow finalized in another process heals with its real
                # body (the memo is a cache, never the source).
                body = resolve_flow_reducer(
                    JobId(w["flow_id"]),
                    w["step_key"],
                    workflow_name=w["workflow_name"],
                )
                if body is not None:
                    await body()
                else:
                    # THE LOUDNESS ASYMMETRY (R2-2): a fired join with no
                    # resolvable body still delivers its declared consumers
                    # (the delivery contract — never a crash), but the
                    # record must not look healthy while the work was
                    # wrong: the join row is stamped
                    # ``blocking_reason='body_unavailable'`` and a WARNING
                    # names it. A stamped name that fails registry
                    # resolution is the deployment defect class (the
                    # definitions not imported in this process — the
                    # fleet's every worker carries them); an anonymous root
                    # or a cold memo is the same silence by another door.
                    # Both are loud here; neither wedges the delivery.
                    await conn.execute(
                        wsql.join_body_unavailable,
                        w["join_job_id"],
                        BLOCKING_REASON_BODY_UNAVAILABLE,
                    )
                    logger.warning(
                        "sweep_join_body_unavailable",
                        kind="sweep_join_body_unavailable",
                        join_job_id=str(w["join_job_id"]),
                        step_key=w["step_key"],
                        flow_id=str(w["flow_id"]),
                        workflow_name=w["workflow_name"],
                    )
                fired_joins.append(
                    FiredJoin(
                        join_job_id=JobId(w["join_job_id"]),
                        step_key=w["step_key"],
                        consumers=_consumer_bindings(w["consumers"]),
                    )
                )
            fired = tuple(fired_joins)
            outbox_rows = [
                (
                    new_uuid(),
                    w["join_job_id"],
                    w["flow_id"],
                    c.step_key,
                    c.map_index,
                    # THE TRACE RIDES THE OUTBOX: the join's own trace when
                    # it has one, else the FIRE's id — the consumers of one
                    # fire share one trace chain (§18.2).
                    _jsonb(
                        {
                            "actor": c.actor,
                            "queue": c.queue,
                            "payload": c.payload,
                            "trace_id": w["trace_id"] or str(w["fire_id"]),
                        }
                    ),
                )
                for w in winners
                for c in _consumer_bindings(w["consumers"])
            ]
            if outbox_rows:
                await conn.executemany(wsql.outbox_insert, outbox_rows)
        # THE DERIVATION'S MAINTENANCE LEG (T08): the reported status is
        # the §17.5 derivation's output — the sweep maintains each flow
        # ROOT's status from the rows (the crash-window heals: a
        # non-absorbed failure → failed; all-terminal → succeeded). Bounded
        # by the root batch; the per-root node scan rides the flow-nodes
        # expression index (01.00.26_02 — the uuid-cast expression, the
        # workflow-rows-only partial) — never a seq scan.
        await conn.execute(wsql.workflow_root_maintain, batch_size)
    return SweepResult(
        blocked=summary["blocked"],
        blocked_required=summary["blocked_required"],
        flow_fenced=summary["flow_fenced"],
        reconciled=summary["reconciled"],
        firable=firable,
        fired=fired,
    )


async def drain_outbox(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 200,
    default_max_attempts: int = 3,
    default_retry_kind: str = "transient",
) -> int:
    """The delivery half of the exactly-once fire (re-red-team F6).

    Read undelivered outbox rows (lock-first), insert the consumer rows
    IDEMPOTENTLY on the consumer step key (the composite
    ``(idempotency_scope, idempotency_key)`` arbiter — ``ON CONFLICT DO
    NOTHING``), flip the undelivered flag IN THE INSERT'S TRANSACTION. A
    crash between fire-commit and consumer-insert re-drains: the drain
    completes the dispatch EXACTLY ONCE (pin 20). The consumer's placement
    (actor/queue/payload) rides the outbox row's ``bindings`` — declared
    on the JoinSpec, stamped at fork, resolved at fire.
    """
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(wsql.outbox_fetch_undelivered, batch_size)
        if not rows:
            return 0
        ids = [new_uuid() for _ in rows]
        # asyncpg returns the jsonb bindings as str on un-coded connections
        # (the estate's _json seam parses; the stdlib import is banned).
        bindings = [_json_loads(r["bindings"] or "{}") for r in rows]
        await conn.execute(
            wsql.outbox_drain_consumers,
            ids,
            [b.get("actor", "workflow") for b in bindings],
            [b.get("queue", "default") for b in bindings],
            # THE PAYLOAD IS THE UNWRAPPED ONE: the consumer body receives
            # the DECLARED payload, never the transport envelope it rode
            # in on (the envelope's actor/queue are the placement columns
            # above; the payload key is the body's own contract).
            [_jsonb(b.get("payload")) for b in bindings],
            [default_max_attempts] * len(rows),
            [default_retry_kind] * len(rows),
            [r["join_job_id"] for r in rows],
            [r["map_index"] for r in rows],
            [r["consumer_step_key"] for r in rows],
            # THE TRACE (§18.2's stamp-at-enqueue): the fire wrote the
            # join's trace — or the fire's own id — into the bindings;
            # the consumer row carries it, never a NULL.
            [b.get("trace_id") for b in bindings],
            # The consumer row's metadata: the flow link, no blocking
            # reason (a normal step, dispatchable like any other).
            [_jsonb(_metadata(JobId(r["flow_id"]), blocking_reason=None)) for r in rows],
            [f"workflow:{r['flow_id']}" for r in rows],
            [
                # THE ARBITER'S KEY — the STATIC ROW's own convention
                # (``step_idempotency_key``): the map-join's downstream is
                # a static row since create (the consumption cure), so the
                # drain's consumer insert for it is the arbiter's CONFLICT
                # (the belt, never a second dispatch). The fired-join
                # consumer rows that DO insert fresh (a fork's own
                # item-shape consumer) keep the map-index leg.
                f"wf:{r['consumer_step_key']}"
                + (f":{r['map_index']}" if r["map_index"] is not None else "")
                for r in rows
            ],
        )
        await conn.execute(wsql.outbox_drain_flip, [r["id"] for r in rows])
    return len(rows)


async def sweep_progress_ring_prune(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    ring_bound: int = PROGRESS_RING_BOUND,
    batch_size: int = 200,
) -> int:
    """THE PROGRESS RING'S PRUNE ARM (T21) — the STREAM channel's
    backstop: every node whose retained ring grew past the ring bound is
    trimmed back to it, rank-based per node (the append statement's own
    trim is the first fence — this arm prunes the LEAKED rings: nodes
    whose emissions stopped before the trim could hold, and any future
    writer bug's unpruned shape, the PoC's red world's 801-row leak
    pruned to the bound in one pass).

    Set-based and bounded twice: the over-bound owner set is LIMITed by
    the batch, and each owner's trim is the append-trim's rank shape —
    a leak drains over passes, never unbounded in one. §22.6's
    exclusivity: the arm never touches reclaim-owned rows (the ring is
    not a jobs row). Returns the pruned row count."""
    async with pool.acquire() as conn:
        pruned: int | None = await conn.fetchval(wsql.progress_ring_prune, ring_bound, batch_size)
    return int(pruned or 0)


async def reap_nodeless_roots(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    grace_s: float = NODELESS_ROOT_REAP_GRACE_S,
    batch_size: int = 100,
) -> int:
    """THE NODELESS-ROOT REAP (the create-seam's belt, the SECOND fence):
    every root row (``step_key = '__flow__'``) with ZERO node rows, in
    ``pending`` or ``running``, past the grace — reaped ``failed`` with
    the LOUD ``NodelessRunReaped`` class, in one set-based bounded
    statement (the event leg rides it).

    THE CREATE ATOMICITY is the first fence: root + nodes + edges +
    ROOT_START are one transaction, so the orphan root is UNREPRESENTABLE
    on the shipped create. THIS arm is what protects against any future
    statement-order regression re-opening the window: the pre-cure
    debris shape (the root insert committed ALONE, the node pass killed)
    was a root the maintenance derivation can never develop (the rollup
    INNER-JOINS the node rows — a nodeless root never derives, never
    terminalizes, never prunes: unbounded retention AND a run key
    squatted forever). The belt reaps it; the typed claim surface then
    answers the squatted key's replay ``existing-terminal`` LOUDLY (the
    caller's re-run is a new key — never a silent no-op).

    Returns the reaped count."""
    async with pool.acquire() as conn:
        reaped: str | None = await conn.fetchval(
            wsql.nodeless_root_reap, timedelta(seconds=grace_s), batch_size
        )
    return int(reaped or 0)


async def reap_phantom_ledger(pool: asyncpg.Pool, wsql: WorkflowSql) -> int:
    """The fenced-attempt sweep arm (hardening H1-H3): fence every
    'running' ledger row whose FLOW is terminal — the rows-alone
    reconstruction reconciles (pin 15). Returns the reaped count.

    THE MEMO'S BOUND rides the same pass: a terminal flow's joins can
    never fire again (the fire's own flow-status leg refuses them), so its
    reducer-cache entry is dead weight — the reaper drops it
    (:func:`taskq.workflows._reducers.forget_flow_reducers`), bounding the
    per-process cache by the live flow runs instead of every flow run
    ever finalized here."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.phantom_reap)
    for flow_id in {r["flow_id"] for r in rows}:
        forget_flow_reducers(JobId(flow_id))
    return len(rows)


async def sweep_hold_stamps(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 200,
) -> int:
    """THE HOLD-STAMP RECONCILE ARM — the SIGKILL-during-a-held-loop
    wedge's fleet-level cure (the D2 soak's P1).

    The held representation is a THREE-ROW contract: the node
    (``pending`` + the ``metadata.hold`` stamp — the claimable fence
    ``NOT metadata ? 'hold'`` excludes it), the ``wf_signals`` row (the
    hold's truth), and the resume (the deliver CAS's clear). A process
    death between the stamp and a resolution leaves the node pending +
    STAMPED with a hold that is no longer 'held' — the soak measured
    TWENTY such runs at close: unclaimable forever, undetected, unnamed
    (the deliver/expiry arms only act when a signal TRANSITIONS; the
    transition had already happened).

    THE HOLD'S STATE DECIDES (:data:`~taskq.workflows._sql_sweep.`
    ``HOLD_STAMP_RECONCILE_SQL``'s contract, restated): a hold still
    ``'held'`` leaves the row standing as the legitimate held
    representation — the resume re-dispatches it; a hold
    delivered/abandoned/cancelled or ABSENT has the stamp CLEARED and the
    row re-claimed (the body's re-execution lands the next NAMED state:
    the answer replay, the typed timeout face, or a fresh hold). Nothing
    wedges; every path lands in a named state.

    Bounded once (LIMIT $1) and self-consuming (a cleared row loses the
    stamp — the predicate empties behind the arm), so one pass never
    unbounds and a deeper backlog drains over passes. Returns the CLEARED
    count (a held-standing row is no work — it is the contract holding)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.hold_stamp_reconcile, batch_size)
    cleared_count = len(rows)
    if cleared_count:
        # THE LOUDNESS LAW: a wedge the arm healed is a NAMED event, not a
        # silent fix — the operator can correlate it with the process
        # death that stamped it.
        logger.warning(
            "wf_hold_stamp_reclaimed",
            kind="wf_hold_stamp_reconcile",
            cleared=cleared_count,
        )
    return cleared_count


async def prune_delivered_outbox(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    retention: timedelta,
    batch_size: int = 200,
) -> int:
    """THE DELIVERED OUTBOX'S TTL ARM (the D2 soak's P3): a delivered
    outbox row is narration — the consumer rows exist (the drain's
    arbiter), ``wf_join_fire`` is the exactly-once ledger — and the soak
    measured the delivered population growing monotonically forever (no
    pruner touched it). This arm deletes delivered rows past *retention*,
    one bounded batch per pass (the tick's drain loop pulls the
    remainder); UNDELIVERED rows are never touched (the drain owns them —
    a deleted undelivered row is a lost delivery). ``retention`` is the
    deletion-sweep family's zero-means-off sentinel: the caller's
    settings gate skips the arm entirely at ``timedelta(0)``."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.outbox_retention, batch_size, retention)
    return len(rows)


async def sweep_loop_budget(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 100,
) -> int:
    """THE BUDGET SWEEP ARM (T19) — the loop walls' enforcer: the BUDGET
    wall (a running loop whose ``budget_deadline`` is past PG's
    ``clock_timestamp()``) and the ITERATION CAP wall (a running loop at/
    over ``max_iterations`` — the crash-dead-worker path; a live worker's
    loop is terminated by the ADVANCE statement's own guard, atomically
    with the carry). THE ARM'S HEART: ``AND NOT budget_paused`` — a loop
    holding on a human is INVISIBLE to the wall even when its deadline is
    FORCED into the past (the CONSUME-BUDGET dragon's cure; the arm
    missing the leg is the mutation the pin drills, kept red forever).

    Exhaustion = the NAMED state (``iteration_cap_exhausted`` /
    ``budget_exhausted`` in the loop's metadata + the typed failure
    class) and the FLOW TERMINALIZES IN THE SAME TRANSACTION
    (STRANDED-FLOW — the spike's cut 5: a wedged ``running`` flow that
    ticks forever is the convicted variant). The escalation outbox row
    (``on_exhausted="escalate"``) rides the same tx — no second delivery
    mechanism. §22.6's exclusivity: the arm never touches a reclaim-owned
    row; the clock comparison is PG's own (the DB-clock doctrine).

    Returns the number of loops exhausted."""
    from taskq._ids import new_uuid
    from taskq.workflows.api._sql_loop import (
        ITERATION_STATE_BUDGET_EXHAUSTED,
        ITERATION_STATE_CAP_EXHAUSTED,
        LOOP_BUDGET_SWEEP_SQL,
        LOOP_ERROR_BUDGET,
        LOOP_ERROR_CAP,
        LOOP_ESCALATION_OUTBOX_SQL,
        LOOP_EXHAUST_SQL,
        LOOP_KIND_MARKER,
        LOOP_NODE_WALL_SQL,
        LOOP_WORKFLOW_NAME_SQL,
        render_loop_sql,
    )

    exhausted = 0
    async with pool.acquire() as conn, conn.transaction():
        loops = await conn.fetch(render_loop_sql(LOOP_BUDGET_SWEEP_SQL, wsql.schema), batch_size)
        for loop_row in loops:
            loop_id = loop_row["id"]
            # WHICH wall (the named state's truth): the metadata's
            # iteration counter vs max_iterations — the cap names
            # ``iteration_cap_exhausted``, the budget its own state.
            state = await conn.fetchrow(
                LOOP_NODE_WALL_SQL.replace("{schema}", wsql.schema), loop_id
            )
            assert state is not None
            cap_hit = (
                state["max_iterations"] is not None
                and (state["iteration"] or 0) >= state["max_iterations"]
            )
            error_class = LOOP_ERROR_CAP if cap_hit else LOOP_ERROR_BUDGET
            iteration_state = (
                ITERATION_STATE_CAP_EXHAUSTED if cap_hit else ITERATION_STATE_BUDGET_EXHAUSTED
            )
            # THE CLAIM IDENTITY'S FENCE (the same legs the driver's
            # exhaust carries): the identity is read under THIS arm's own
            # row lock (FOR UPDATE — stable to statement end), so the
            # exhaust lands exactly on the row the lock admitted — a
            # stale identity is unrepresentable here by construction; the
            # fence's refusal shape (a zombie driver) is the DRIVER's
            # conviction, and the legs make the statement ONE vocabulary.
            result = await conn.fetchrow(
                render_loop_sql(LOOP_EXHAUST_SQL, wsql.schema),
                loop_id,
                error_class,
                f'{{"iteration_state": "{iteration_state}", "kind": "{LOOP_KIND_MARKER}"}}',
                f"the loop's {'iteration cap' if cap_hit else 'budget'} wall fired "
                f"(the sweep's arm; the named state: {iteration_state})",
                loop_row["locked_by_worker"],
                loop_row["attempt"],
                loop_row["claim_epoch"],
            )
            if result is None or not result["loop_exhausted"]:
                continue  # another writer got there first — the CAS held
            # THE COUNT IS THE EXHAUSTION'S (f_loop_5's cure): the named
            # state + the flow's terminalization COMPLETED at the exhaust
            # statement — every CAS-held exhaustion is ONE exhaustion,
            # whatever the policy does next (the escalate arm's enqueue is
            # ADDITIONAL work; a fail-policy loop IS exhausted and the
            # return must say so).
            exhausted += 1
            # THE POLICY (attack-3 H1's cure — the sweep READS the
            # registered declaration, D1): ``fail`` = the named state is
            # the record, NO enqueue; ``escalate`` = the escalation
            # enqueues through the SAME outbox IN THE SAME TX, addressed
            # to the workflow's REGISTERED escalation step (the body
            # resolves from the definition registry at claim — never the
            # ``loop_escalation``-actor ghost).
            from taskq.workflows.api._loop import (
                ESCALATION_STEP_KEY,
                escalation_bindings,
                registered_loop_policy,
            )

            workflow_name = await conn.fetchval(
                render_loop_sql(LOOP_WORKFLOW_NAME_SQL, wsql.schema),
                loop_row["flow_id"],
            )
            if registered_loop_policy(workflow_name, loop_row["step_key"]) != "escalate":
                continue
            bindings = escalation_bindings(
                workflow_name,
                loop_row["step_key"],
                flow_id=loop_row["flow_id"],
                error_class=error_class,
                message=None,
            )
            if bindings is None:
                continue  # no registered escalation body — never a ghost row
            await conn.execute(
                render_loop_sql(LOOP_ESCALATION_OUTBOX_SQL, wsql.schema),
                new_uuid(),
                loop_id,
                loop_row["flow_id"],
                ESCALATION_STEP_KEY,
                _jsonb(bindings),
            )
    return exhausted
