"""The meter-readings sync actors: process, review, finalize, and the cron tick.

Every actor that touches the readings table upserts idempotently
(``INSERT ... ON CONFLICT DO UPDATE ... WHERE`` the content actually
changed), so any re-run of the pipeline — a retried job, a killed worker
whose work is reclaimed, or a whole second pass over the same feed —
changes nothing that is already correct.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg
from pydantic import BaseModel, Field

from taskq import JobContext, actor, cron
from taskq.batch import wait_for_batch

# ---------------------------------------------------------------------------
# Payloads and results
# ---------------------------------------------------------------------------


class MeterReadingPayload(BaseModel):
    """One reading from the source feed."""

    reading_id: str
    meter_id: str
    kind: str  # "interval" | "estimated" | "anomaly"
    kwh: float
    read_at: datetime
    run_id: str = "adhoc"


class ReviewPayload(BaseModel):
    """A reading that needs a downstream review."""

    reading_id: str
    reason: str


class FinalizeSyncPayload(BaseModel):
    """Gate for the fan-out-then-finalize pattern."""

    batch_id: str
    run_id: str
    expected: int


class TickPayload(BaseModel):
    """The scheduled tick's own payload."""

    feed_size: int = Field(default=6, ge=1, le=50)


class ProcessResult(BaseModel):
    reading_id: str
    outcome: str  # "inserted" | "updated" | "unchanged"


class SyncReceipt(BaseModel):
    """The typed terminal verdict for one sync run.

    This is the success face: the pipeline is done when THIS object says
    the counts add up — not merely when a process exits zero.
    """

    run_id: str
    expected: int
    stored: int
    flagged: int
    review_rows: int

    @property
    def is_complete(self) -> bool:
        return self.stored + self.flagged == self.expected


def content_hash(p: MeterReadingPayload) -> str:
    """A stable hash of the reading's *content* (the upsert's change detector)."""
    material = f"{p.reading_id}|{p.meter_id}|{p.kind}|{p.kwh}|{p.read_at.isoformat()}"
    return hashlib.sha256(material.encode()).hexdigest()


def batch_id_for(run_id: str, generation: int = 0) -> str:
    """Deterministic per-run batch id.

    Derived with ``uuid5`` so every attempt of a retried producer (and
    every idempotent re-run of the same ``run_id``) addresses the SAME
    batch — the docs' fan-out guidance for retry-safe batch ids.
    """
    return str(uuid5(NAMESPACE_URL, f"metering-sync:{run_id}:{generation}"))


# ---------------------------------------------------------------------------
# The processing actor — branch on item type, upsert idempotently
# ---------------------------------------------------------------------------


@actor(name="process_reading", queue="ingest")
async def process_reading(
    payload: MeterReadingPayload, ctx: JobContext[MeterReadingPayload], db: asyncpg.Pool
) -> ProcessResult:
    """Land one reading; anomalous readings fan out to the review actor."""
    # The PROGRESS face: observable while the run is still in flight.
    await ctx.progress(
        step=1,
        detail=f"landing {payload.reading_id}",
        data={"reading_id": payload.reading_id, "kind": payload.kind},
    )

    h = content_hash(payload)
    flagged = payload.kind == "anomaly"
    status = "flagged" if flagged else "stored"

    # THE IDEMPOTENT UPSERT: the row is written only when its content
    # actually differs from what is stored. Re-running the same reading
    # — from a retry, a reclaimed attempt, or a second pipeline pass —
    # writes nothing and reports "unchanged".
    row = await db.fetchrow(
        """
        INSERT INTO meter_readings
            (reading_id, meter_id, kind, kwh, read_at, content_hash, run_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (reading_id) DO UPDATE
            SET meter_id = EXCLUDED.meter_id,
                kind = EXCLUDED.kind,
                kwh = EXCLUDED.kwh,
                read_at = EXCLUDED.read_at,
                content_hash = EXCLUDED.content_hash,
                run_id = EXCLUDED.run_id
            WHERE meter_readings.content_hash IS DISTINCT FROM EXCLUDED.content_hash
        RETURNING (xmax = 0) AS inserted
        """,
        payload.reading_id,
        payload.meter_id,
        status,
        payload.kwh,
        payload.read_at,
        h,
        payload.run_id,
    )
    if row is None:
        outcome = "unchanged"
    elif row["inserted"]:
        outcome = "inserted"
    else:
        outcome = "updated"

    # THE BRANCH: anomalies fan out to a second actor. The stable
    # idempotency key means a retried parent cannot double-flag.
    if flagged:
        await ctx.jobs.enqueue(
            flag_for_review,
            ReviewPayload(reading_id=payload.reading_id, reason=f"kind={payload.kind}"),
            idempotency_key=f"review:{payload.reading_id}",
        )

    # Worker-side instrumentation (the drills' evidence): every attempt of
    # every job records that it touched this reading.
    await db.execute(
        """
        INSERT INTO sync_probe_events (reading_id, job_id, attempt, event)
        VALUES ($1, $2, $3, $4)
        """,
        payload.reading_id,
        str(ctx.job_id),
        ctx.attempt,
        f"item-start:{outcome}",
    )

    return ProcessResult(reading_id=payload.reading_id, outcome=outcome)


@actor(name="flag_for_review", queue="review")
async def flag_for_review(
    payload: ReviewPayload, ctx: JobContext[ReviewPayload], db: asyncpg.Pool
) -> None:
    """Record that a reading needs review (idempotent: ON CONFLICT DO NOTHING)."""
    await db.execute(
        """
        INSERT INTO meter_review_queue (reading_id, reason)
        VALUES ($1, $2)
        ON CONFLICT (reading_id) DO NOTHING
        """,
        payload.reading_id,
        payload.reason,
    )
    await ctx.progress(detail=f"review queued: {payload.reading_id}")


# ---------------------------------------------------------------------------
# The finalizer — snoozes until the batch is terminal, pushes progress,
# returns the typed receipt
# ---------------------------------------------------------------------------


@actor(name="finalize_sync", queue="finalize")
async def finalize_sync(
    payload: FinalizeSyncPayload, ctx: JobContext[FinalizeSyncPayload], db: asyncpg.Pool
) -> SyncReceipt:
    """Wait for every child in the batch, then report the exact counts.

    ``wait_for_batch`` raises :class:`~taskq.exceptions.Snooze` while
    children are in flight; the snooze loop drives the wait without
    consuming retry budget. Each wake PUSHES progress first, so an
    operator watching the job sees the batch drain in real time.

    The finalizer carries NO ``metadata.batch_id`` on purpose: it must
    not be counted among the children it waits on.
    """
    stored, flagged = await _counts(db, payload.run_id)
    done = stored + flagged
    await ctx.progress(
        percent=round(done / max(payload.expected, 1) * 100, 1),
        detail=f"{done}/{payload.expected} readings landed",
        data={"stored": stored, "flagged": flagged},
    )

    status = await wait_for_batch(
        db,
        UUID(payload.batch_id),
        snooze_interval=timedelta(seconds=2),
    )

    stored, flagged = await _counts(db, payload.run_id)
    review_rows: int = await db.fetchval(
        """
        SELECT count(*) FROM meter_review_queue q
        JOIN meter_readings r ON r.reading_id = q.reading_id
        WHERE r.run_id = $1
        """,
        payload.run_id,
    )
    receipt = SyncReceipt(
        run_id=payload.run_id,
        expected=payload.expected,
        stored=stored,
        flagged=flagged,
        review_rows=review_rows,
    )
    if not receipt.is_complete or status.failed:
        raise RuntimeError(
            f"sync incomplete: receipt={receipt.model_dump()} batch_failed={status.failed}"
        )
    return receipt


async def _counts(db: asyncpg.Pool, run_id: str) -> tuple[int, int]:
    rows = await db.fetch(
        "SELECT kind, count(*) AS n FROM meter_readings WHERE run_id = $1 GROUP BY kind",
        run_id,
    )
    by_kind = {r["kind"]: r["n"] for r in rows}
    return int(by_kind.get("stored", 0)), int(by_kind.get("flagged", 0))


# ---------------------------------------------------------------------------
# The scheduled face — a cron tick that runs the same sync on a small feed
# ---------------------------------------------------------------------------


@actor(name="sync_readings_tick", queue="ingest", singleton=True)
async def sync_readings_tick(payload: TickPayload, ctx: JobContext[TickPayload]) -> ProcessResult:
    """The scheduled trigger: scan the feed, hand the readings off eagerly.

    Idempotency keys are scoped to the fire's minute bucket, so a fire
    that lands twice (or a fire whose children are re-enqueued by a
    retry) deduplicates instead of duplicating.
    """
    bucket = datetime.now(UTC).strftime("%Y%m%d%H%M")
    run_id = f"cron-{bucket}"
    for i in range(payload.feed_size):
        await ctx.jobs.enqueue(
            process_reading,
            MeterReadingPayload(
                reading_id=f"cron-{bucket}-{i:03d}",
                meter_id=f"M-{i % 3:03d}",
                kind="anomaly" if i == payload.feed_size - 1 else "interval",
                kwh=round(1.0 + i * 0.5, 1),
                read_at=datetime.now(UTC),
                run_id=run_id,
            ),
            idempotency_key=f"reading:{run_id}:{i:03d}",
        )
    return ProcessResult(reading_id=run_id, outcome=f"fan-out {payload.feed_size}")


# The schedule itself: every 30 seconds (the optional 6th field is seconds,
# appended after the standard 5). Declared at import time, auto-discovered
# and fired by the worker's maintenance leader; the schedule row persists
# in ``cron_schedules``.
cron("* * * * * */30", "sync_readings_tick", static_payload={"feed_size": 6})

# The worker's registry (``taskq worker --actors examples.metering_sync.actors:registry``).
registry = [process_reading, flag_for_review, finalize_sync, sync_readings_tick]
