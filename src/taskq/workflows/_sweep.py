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
variant measured p50 477 ms / p95 1.73 s @ ~340 live joins vs the
set-based 14.9 ms @ 200; the scope pin convicts the unscoped seq-scan
monster at 83.7 ms @ 75k).

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

import asyncpg
import structlog

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._reducers import forget_flow_reducers, resolve_flow_reducer
from taskq.workflows._sql import (
    BLOCKING_REASON_BODY_UNAVAILABLE,
    BLOCKING_REASON_ORPHAN_PARENT,
    WorkflowSql,
)
from taskq.workflows._types import FiredJoin, _consumer_bindings, _jsonb, _metadata

__all__ = ["SweepResult", "drain_outbox", "reap_phantom_ledger", "sweep_join_rederive"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SweepResult:
    """One re-derive pass's outcome (counts are the statement's own)."""

    blocked: int
    reconciled: int
    firable: int
    fired: tuple[FiredJoin, ...] = ()


async def sweep_join_rederive(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 200,
    orphan_blocking_reason: str = BLOCKING_REASON_ORPHAN_PARENT,
) -> SweepResult:
    """The lock-first re-derive arm: ONE batched statement (lock the
    join-wait children SKIP LOCKED → count un-terminal parents from the
    edge ledger → reconcile the cache → block the orphan-parent rows →
    report counts), then the set-based fire arm for the firable set (the
    flow-status leg rides INSIDE the fire statement — a post-cancel
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
        summary = await conn.fetchrow(wsql.rederive_sweep, batch_size, orphan_blocking_reason)
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
    return SweepResult(
        blocked=summary["blocked"],
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
                f"wf:{r['flow_id']}:{r['consumer_step_key']}"
                + (f":{r['map_index']}" if r["map_index"] is not None else "")
                for r in rows
            ],
        )
        await conn.execute(wsql.outbox_drain_flip, [r["id"] for r in rows])
    return len(rows)


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
