"""The gauge-fleet actors: a recurring refresh flow in four documented faces.

Reading map (TaskQ docs only — every mechanism here is a doc'd one):

- **The durable cursor** — the tick reads ``gauge_cursor``, processes the
  increment ``updated_at > cursor``, and advances the cursor in the SAME
  transaction as the digest upserts (ops.md §5 Pattern A's discipline: "The
  cursor lives in the payload ... Your cursor is your data", with the
  per-cycle all-or-nothing commit boundary).
- **The durable timer** — the tick self-schedules its successor with
  ``scheduled_at`` + a fresh per-tick ``idempotency_key`` (ops.md §7,
  "Scheduled re-enqueue": "The job queue is your timer, and dedup makes the
  pattern crash-safe"). The successor exists in ``scheduled`` status before
  this tick does any work, so a worker death mid-tick cannot kill the chain:
  the clock holds the door, and the successor wakes the fleet.
- **The overlap guard** — an app-level running-run guard on the
  ``gauge_cycle_guard`` row (ops.md §5's "app-level running-run guard" option):
  a tick landing while one runs PARKS (skip, not raise) and still schedules
  the next tick, so a slow cycle never stacks on the running one and the
  chain never dies.
- **The HITL face** — a threshold breach pauses the gauge for approval: the
  tick enqueues ``approval_escalate`` with ``scheduled_at = window close``.
  When it fires: approved in the window -> success; the window lapsed
  unapproved -> the EXPIRED arm records the escalation durably, then raises
  :class:`ApprovalWindowExpiredError` (listed in ``non_retryable_exceptions``), so
  the job terminal-fails at attempt one — the fail-close.

Why the tick actor is NOT ``singleton=True``: a singleton cannot self-schedule
its own successor — the currently-running tick IS an active job of the actor,
so the successor's enqueue would deterministically collide with itself
(docs: "Active" means ``pending``/``scheduled``/``running``). The chained
shape uses the app-level guard instead, exactly as the docs recommend for
chains; :class:`SingletonCollisionError` is still demonstrated on the
escalation actor, which never self-enqueues.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
from pydantic import BaseModel

from examples.gaugefleet import state
from taskq import JobContext, RetryPolicy, Snooze, actor, new_uuid
from taskq.batch import EnqueueItem

DSN = os.environ.get("TASKQ_PG_DSN", state.DSN)

#: Tag shared by every tick (and inherited by fan-out children), so the
#: operator's ``JobFilter(tags=...)`` / admin-UI view finds the whole run.
RUN_TAG = "gaugefleet"

_pool: asyncpg.Pool | None = None


async def pool() -> asyncpg.Pool:
    """Lazily-shared pool for the example's own tables (both worker + driver)."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    return _pool


# --------------------------------------------------------------------------
# Actors
# --------------------------------------------------------------------------


class TickPayload(BaseModel):
    tick_index: int
    #: A per-run namespace threaded through the whole chain: every idempotency
    #: key and breach id embeds it, so a re-run of the demo (whose previous
    #: jobs still sit in the jobs table until retention) starts a fresh chain
    #: instead of deduplicating against run 1's succeeded ticks.
    run_ns: str
    #: Simulated per-tick work duration (the drills widen this to force a slow
    #: cycle). Kept in the payload so the harness can steer one tick's speed.
    work_seconds: float = 0.0


class TickResult(BaseModel):
    """The typed success receipt: what this cycle did, exactly."""

    tick_index: int
    outcome: str  # "succeeded" | "parked"
    cursor_before: datetime | None = None
    cursor_after: datetime | None = None
    processed: int = 0
    fan_out: int = 0
    breaches: list[str] = []


class GaugeReadingPayload(BaseModel):
    """A fan-out child carries its own data: the reading itself rides in the
    payload, so nothing depends on the source row after enqueue."""

    gauge_id: str
    reading: float
    updated_at: datetime
    tick_index: int


class EscalatePayload(BaseModel):
    breach_id: str


class EscalationResult(BaseModel):
    breach_id: str
    outcome: str  # "approved_in_window"


class ApprovalWindowExpiredError(Exception):
    """The expired arm's typed escalation — fail-close, never retried."""


@actor(
    name="gauge_refresh_tick",
    queue="gaugefleet",
    start_to_close=timedelta(minutes=5),
)
async def refresh_tick(payload: TickPayload, ctx: JobContext[TickPayload]) -> TickResult:
    """One cycle: cursor -> increment -> apply -> advance -> schedule next."""
    db = await pool()
    n = payload.tick_index

    await ctx.progress(step=1, percent=10.0, detail="claim cycle guard", data={"tick": n})
    run_token = str(new_uuid())
    ns = payload.run_ns
    if not await state.claim_cycle(db, run_token):
        # PARKED, not stacked: a cycle is already running (or its claim is
        # fresh). Skip the work, keep the chain alive, return honestly.
        await ctx.progress(step=2, percent=100.0, detail="parked: a cycle is already running")
        cursor = await state.read_cursor(db)
        await state.log_cycle(
            db,
            tick_index=n,
            job_id=str(ctx.job_id),
            attempt=ctx.attempt,
            outcome="parked",
            cursor_before=cursor,
        )
        await _schedule_next(ctx, n)
        return TickResult(tick_index=n, outcome="parked", cursor_before=cursor)

    # The clock holds the door: the successor exists (scheduled, durable)
    # before any work runs, so a worker death mid-tick cannot kill the chain.
    await _schedule_next(ctx, n)

    cursor_before = await state.read_cursor(db)
    await asyncio.sleep(payload.work_seconds)

    await ctx.progress(step=3, percent=40.0, detail="read increment", data={"tick": n})
    increment = await state.fetch_increment(db, cursor_before)

    breaches: list[str] = []
    fan_out = 0
    if len(increment) > state.FAN_OUT_THRESHOLD:
        # WIDE increment: fan out one child per gauge (each child carries its
        # own data + a stable per-item idempotency key), then advance the
        # cursor. Crash-safe both ways per ops.md §5: before the enqueue the
        # cursor never moved; after it, the children are durable and the
        # re-enqueue dedups (was_existing=True) — no fork, no double-work.
        await ctx.progress(
            step=4,
            percent=60.0,
            detail=f"fan-out {len(increment)} gauge children",
            data={"tick": n},
        )
        # Deterministic id: a parent retry must reuse the same batch id (the
        # docs' uuid5 guidance for in-actor batch ids).
        batch_id = uuid.uuid5(uuid.NAMESPACE_URL, f"gaugefleet-fanout:{ns}:{n}")
        items = [
            EnqueueItem(
                actor_ref=refresh_gauge,
                payload=GaugeReadingPayload(
                    gauge_id=r["gauge_id"],
                    reading=r["reading"],
                    updated_at=r["updated_at"],
                    tick_index=n,
                ),
                idempotency_key=f"gaugefleet-gauge:{r['gauge_id']}:{r['updated_at'].isoformat()}",
            )
            for r in increment
        ]
        await ctx.jobs.enqueue_batch(items, batch_id=batch_id)
        fan_out = len(items)
        cursor_after = await state.apply_increment(
            db,
            n,
            increment,
            str(ctx.job_id),
            ctx.attempt,
            "succeeded",
            processed=0,
            fan_out=fan_out,
        )
        processed = 0  # the children do the digesting
    else:
        await ctx.progress(
            step=4, percent=60.0, detail=f"apply increment ({len(increment)} gauges)"
        )
        cursor_after = await state.apply_increment(
            db, n, increment, str(ctx.job_id), ctx.attempt, "succeeded"
        )
        processed = len(increment)

    # HITL face: a threshold breach pauses for approval. The escalation job is
    # enqueued at the window's close — the timer arm the operator can watch
    # sitting in `scheduled`.
    for r in increment:
        if r["reading"] > state.BREACH_THRESHOLD:
            breach_id = f"breach-{ns}-{r['gauge_id']}"
            await state.record_breach(db, breach_id, r["gauge_id"], r["reading"])
            window_close: datetime = (await state.breach_row(db, breach_id))["window_closes_at"]
            await ctx.jobs.enqueue(
                approval_escalate,
                EscalatePayload(breach_id=breach_id),
                scheduled_at=window_close,
                idempotency_key=f"gaugefleet-approval:{breach_id}",
            )
            breaches.append(breach_id)

    await state.release_cycle(db, run_token)
    await ctx.progress(
        step=5,
        percent=100.0,
        detail="cycle complete",
        data={"tick": n, "processed": processed, "fan_out": fan_out},
    )
    return TickResult(
        tick_index=n,
        outcome="succeeded",
        cursor_before=cursor_before,
        cursor_after=cursor_after,
        processed=processed,
        fan_out=fan_out,
        breaches=breaches,
    )


async def _schedule_next(ctx: JobContext[TickPayload], current: int) -> None:
    """The durable timer: enqueue tick ``current + 1`` at ``now + interval``.

    A FRESH key dimension per successor (the docs' rule for self-continuation:
    a repeated key collides with the terminal predecessor and is silently
    dropped), timezone-aware ``scheduled_at`` (naive raises), and the job
    queue is the timer — promotion is the leader's ~1 s sweep.
    """
    await ctx.jobs.enqueue(
        refresh_tick,
        TickPayload(tick_index=current + 1, run_ns=ctx.payload.run_ns),
        scheduled_at=datetime.now(UTC) + timedelta(seconds=state.TICK_INTERVAL_S),
        idempotency_key=f"gaugefleet-tick:{ctx.payload.run_ns}:{current + 1}",
    )


@actor(name="gauge_refresh_gauge", queue="gaugefleet")
async def refresh_gauge(payload: GaugeReadingPayload, ctx: JobContext[GaugeReadingPayload]) -> None:
    """A fan-out child: upsert one gauge's reading into the digest (idempotent)."""
    db = await pool()
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO gauge_digest "
            "(gauge_id, reading, source_updated_at, digested_by_tick) "
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (gauge_id) DO UPDATE SET "
            "  reading = EXCLUDED.reading, "
            "  source_updated_at = EXCLUDED.source_updated_at, "
            "  digested_by_tick = EXCLUDED.digested_by_tick, "
            "  digested_at = now()",
            payload.gauge_id,
            payload.reading,
            payload.updated_at,
            payload.tick_index,
        )


@actor(
    name="gauge_approval_escalate",
    queue="gaugefleet",
    singleton=True,
    start_to_close=timedelta(seconds=30),
    non_retryable_exceptions=(ApprovalWindowExpiredError,),
    retry=RetryPolicy(max_attempts=5),
)
async def approval_escalate(
    payload: EscalatePayload, ctx: JobContext[EscalatePayload]
) -> EscalationResult:
    """The approval window's timer arm: held in ``scheduled`` until the window
    closes, then decides — approved in time, or the EXPIRED arm fails close."""
    db = await pool()
    row = await state.breach_row(db, payload.breach_id)
    approved_at: datetime | None = row["approved_at"]
    window_close: datetime = row["window_closes_at"]
    now = datetime.now(UTC)

    if approved_at is not None:
        return EscalationResult(breach_id=payload.breach_id, outcome="approved_in_window")

    if now < window_close:
        # Fired a hair early (promotion tick precision): snooze to the window's
        # close — a snooze refunds the attempt, it does not consume one.
        raise Snooze(window_close - now)

    # THE EXPIRED ARM: record the escalation durably first (the operator's
    # audit trail must survive the raise), then fail close — the typed error
    # is in non_retryable_exceptions, so attempt one is terminal.
    await state.record_escalation(
        db,
        payload.breach_id,
        f"gauge {row['gauge_id']} breached at {row['reading']:.1f} and was not "
        f"approved within its window (closed {window_close.isoformat()})",
    )
    raise ApprovalWindowExpiredError(
        f"breach {payload.breach_id} unapproved past its window (closed {window_close.isoformat()})"
    )


# The worker's --actors flag resolves this mapping.
registry: dict[str, Any] = {
    "gauge_refresh_tick": refresh_tick,
    "gauge_refresh_gauge": refresh_gauge,
    "gauge_approval_escalate": approval_escalate,
}
