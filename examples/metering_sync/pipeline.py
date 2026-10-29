"""The client side of the meter-readings sync: the producer and its receipts.

The producer is a lazy item list handed to ``enqueue_batch_streaming``:
items are enqueued in chunks, each chunk its own committed transaction,
so the first chunk is dispatchable — and with a worker up, being
*consumed* — while the producer is still enqueuing later chunks. That
eager overlap is MEASURED here, not asserted: an observer task watches
the queue for the first consumed item while the producer call is still
blocked.

Every enqueue carries a stable per-item idempotency key
(``reading:<run_id>:<reading_id>``) and the batch id is derived
deterministically from the run id, so the whole pipeline can be re-run
against the same source data without duplicating anything.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg

from taskq import JobFilter, TaskQ
from taskq.batch import EnqueueItem

from .actors import (
    FinalizeSyncPayload,
    MeterReadingPayload,
    SyncReceipt,
    batch_id_for,
    content_hash,
    finalize_sync,
    process_reading,
)

# ---------------------------------------------------------------------------
# The source feed (a stand-in for whatever a real pipeline scans: a vendor
# API, an SFTP drop, another database). Deterministic per run so the drills
# can hash it.
# ---------------------------------------------------------------------------


def source_readings(
    run_id: str, total: int, *, anomaly_every: int = 10
) -> list[MeterReadingPayload]:
    """The feed: ``total`` deterministic readings, every Nth an anomaly."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        MeterReadingPayload(
            reading_id=f"{run_id}-{i:06d}",
            meter_id=f"M-{i % 40:04d}",
            kind=(
                "anomaly" if i % anomaly_every == 0 else ("estimated" if i % 7 == 0 else "interval")
            ),
            kwh=round(0.5 + (i * 37 % 900) / 100, 2),
            read_at=base + timedelta(minutes=i),
            run_id=run_id,
        )
        for i in range(total)
    ]


def feed_items(run_id: str, readings: list[MeterReadingPayload]) -> list[EnqueueItem]:
    """The source scan's output: one queue item per reading, all keyed."""
    return [
        EnqueueItem(
            actor_ref=process_reading,
            payload=r,
            idempotency_key=f"reading:{run_id}:{r.reading_id}",
        )
        for r in readings
    ]


def feed_checksum(readings: list[MeterReadingPayload]) -> str:
    """The order-stable hash the readings table must match after a run."""
    rows = [(r.reading_id, content_hash(r)) for r in readings]
    material = "".join(f"{rid}={h};" for rid, h in rows)
    return hashlib.sha256(material.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Receipts
# ---------------------------------------------------------------------------


@dataclass
class OverlapReceipt:
    """THE EAGER-OVERLAP PROOF, measured."""

    first_consumed_at: datetime | None = None
    producer_finished_at: datetime | None = None
    overlap_seconds: float | None = None

    @property
    def overlaps(self) -> bool:
        return self.overlap_seconds is not None and self.overlap_seconds > 0.0


@dataclass
class AdminSnapshot:
    """The operator's view of the run, captured mid-flight and at the end."""

    moment: str  # "mid-run" | "terminal"
    batch: dict[str, object] = field(default_factory=dict)
    nodes: list[dict[str, str]] = field(default_factory=list)


@dataclass
class PipelineRunResult:
    """Everything one pipeline run can prove, as receipts."""

    run_id: str
    batch_id: str
    receipt: SyncReceipt
    overlap: OverlapReceipt
    progress_events: list[dict[str, object]] = field(default_factory=list)
    admin_views: list[AdminSnapshot] = field(default_factory=list)


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------

_BATCH_STATUS_RUNNING = ("running", "succeeded")


async def ensure_tables(dsn: str) -> None:
    """Create the example's own tables (the queue schema is migrated separately)."""
    conn = await asyncpg.connect(dsn)
    try:
        for ddl in (
            """
            CREATE TABLE IF NOT EXISTS meter_readings (
                reading_id   TEXT PRIMARY KEY,
                meter_id     TEXT NOT NULL,
                kind         TEXT NOT NULL,
                kwh          DOUBLE PRECISION NOT NULL,
                read_at      TIMESTAMPTZ NOT NULL,
                content_hash TEXT NOT NULL,
                run_id       TEXT NOT NULL,
                written_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS meter_review_queue (
                reading_id TEXT PRIMARY KEY,
                reason     TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sync_probe_events (
                reading_id TEXT NOT NULL,
                job_id     TEXT NOT NULL,
                attempt    INT  NOT NULL,
                event      TEXT NOT NULL,
                seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
        ):
            await conn.execute(ddl)
    finally:
        await conn.close()


async def run_pipeline(
    dsn: str,
    run_id: str,
    total: int,
    *,
    page_size: int = 100,
    page_latency: float = 0.15,
    observe_overlap: bool = True,
) -> PipelineRunResult:
    """Run one full sync: scan, hand off, wait, and return the typed receipts.

    The producer is a paginated source-scan loop: it fetches one page of
    the feed (``page_latency`` models the upstream's response time),
    enqueues that page, and moves on to the next. With a worker up, page
    1 is dispatched — and being consumed — while the producer is still
    scanning page 2: the eager handoff the overlap drill measures.
    """
    readings = source_readings(run_id, total)
    batch_id = batch_id_for(run_id)

    async with TaskQ(dsn=dsn) as tq:
        db = await asyncpg.create_pool(dsn)
        try:
            # ---- the producer: the paginated source-scan loop -------------
            first_page_handle: list[object] = []

            async def produce() -> int:
                enqueued = 0
                for start in range(0, len(readings), page_size):
                    page = feed_items(run_id, readings[start : start + page_size])
                    await asyncio.sleep(page_latency)  # the upstream's fetch latency
                    page_batch = await tq.enqueue_batch(page, batch_id=UUID(batch_id))
                    if not first_page_handle:
                        first_page_handle.append(page_batch.job_handles[0])
                    enqueued += len(page)
                return enqueued

            # ---- the observer: catch the first consumed item while the
            # producer is still scanning ------------------------------------
            overlap = OverlapReceipt()
            admin_views: list[AdminSnapshot] = []
            progress_events: list[dict[str, object]] = []

            async def observe() -> None:
                first_seen = False
                snapshot_taken = False
                while not producer.done():
                    n = await db.fetchval(
                        """
                        SELECT count(*) FROM taskq.jobs
                        WHERE metadata @> $1::jsonb
                          AND status::text = ANY($2::text[])
                        """,
                        f'{{"batch_id": "{batch_id}"}}',
                        list(_BATCH_STATUS_RUNNING),
                    )
                    if n and n > 0:
                        if not first_seen and observe_overlap:
                            first_seen = True
                            overlap.first_consumed_at = datetime.now(UTC)
                        if not snapshot_taken:
                            snapshot_taken = True
                            admin_views.append(await _admin_view(tq, UUID(batch_id), "mid-run"))
                    await asyncio.sleep(0.05)

            producer = asyncio.create_task(produce())
            observer = asyncio.create_task(observe())
            await producer
            producer_end = datetime.now(UTC)
            await observer

            overlap.producer_finished_at = producer_end
            if overlap.first_consumed_at is not None:
                overlap.overlap_seconds = round(
                    (producer_end - overlap.first_consumed_at).total_seconds(), 4
                )

            # ---- the finalizer: enqueued separately, after the scan. (The
            # client batch APIs' ``finalizer=`` parameter forces the atomic
            # single-transaction path, which commits nothing until the whole
            # batch is enqueued — the opposite of the eager handoff being
            # demonstrated here.) ------------------------------------------------
            finalize_handle = await tq.enqueue(
                finalize_sync,
                FinalizeSyncPayload(batch_id=batch_id, run_id=run_id, expected=total),
                idempotency_key=f"finalize:{run_id}",
                tags=(f"run-{run_id}",),
            )

            async def capture_progress() -> None:
                try:
                    async for event in finalize_handle.progress_stream():
                        progress_events.append(
                            {
                                "seq": event.seq,
                                "percent": event.percent,
                                "detail": event.detail,
                                "ts": event.ts.isoformat(),
                                "terminal": event.terminal,
                            }
                        )
                        if event.terminal:
                            return
                except NotImplementedError:  # pragma: no cover - transport guard
                    return

            progress_task = asyncio.create_task(capture_progress())

            # Also ride one WORKER job's stream: the per-item progress the
            # worker pushes while the run is still in flight.
            worker_events: list[dict[str, object]] = []

            async def capture_worker_progress() -> None:
                if not first_page_handle:
                    return
                job_handle = first_page_handle[0]
                try:
                    async for event in job_handle.progress_stream():  # type: ignore[attr-defined]
                        worker_events.append(
                            {
                                "seq": event.seq,
                                "percent": event.percent,
                                "detail": event.detail,
                                "data": event.data,
                                "ts": event.ts.isoformat(),
                                "terminal": event.terminal,
                            }
                        )
                        if event.terminal:
                            return
                except NotImplementedError:  # pragma: no cover - transport guard
                    return

            worker_progress_task = asyncio.create_task(capture_worker_progress())

            # ---- wait the batch out --------------------------------------
            conn = await db.acquire()
            try:
                while True:
                    status = await batch_status(conn, batch_id)
                    if status is not None and int(status["pending"]) == 0:
                        break
                    await asyncio.sleep(0.2)
            finally:
                await db.release(conn)

            # ---- the SUCCESS face: a typed receipt, not an exit code ------
            receipt = await finalize_handle.wait(timeout=120.0)
            admin_views.append(await _admin_view(tq, UUID(batch_id), "terminal"))
            progress_task.cancel()
            worker_progress_task.cancel()
            progress_events.extend({"source": "worker-1", **e} for e in worker_events)

            return PipelineRunResult(
                run_id=run_id,
                batch_id=batch_id,
                receipt=receipt,
                overlap=overlap,
                progress_events=progress_events,
                admin_views=admin_views,
            )
        finally:
            await db.close()


async def batch_status(conn: asyncpg.Connection, batch_id: str) -> dict[str, int] | None:
    """The batch's completion counts. (``BatchHandle.status`` needs the handle
    object; the drills poll the same GIN-indexed predicate the docs teach so a
    producer that died mid-run can still be watched to terminal.)"""
    row = await conn.fetchrow(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status::text IN ('pending', 'scheduled', 'running')) AS pending,
               count(*) FILTER (WHERE status::text = 'succeeded') AS succeeded,
               count(*) FILTER (WHERE status::text = 'failed') AS failed,
               count(*) FILTER (WHERE status::text IN ('cancelled', 'crashed', 'abandoned')) AS dead
        FROM taskq.jobs
        WHERE metadata @> $1::jsonb
        """,
        f'{{"batch_id": "{batch_id}"}}',
    )
    assert row is not None
    counts = dict(row)
    if int(counts["total"]) == 0:
        return None
    return counts


async def readings_fingerprint(db: asyncpg.Connection, run_id: str) -> tuple[int, str]:
    """Row count + an order-stable content hash of one run's readings."""
    rows = await db.fetch(
        """
        SELECT reading_id, content_hash FROM meter_readings
        WHERE run_id = $1 ORDER BY reading_id
        """,
        run_id,
    )
    material = "".join(f"{r['reading_id']}={r['content_hash']};" for r in rows)
    return len(rows), hashlib.sha256(material.encode()).hexdigest()


async def _admin_view(tq: TaskQ, batch_id: UUID, moment: str) -> AdminSnapshot:
    """The operator's display: the run's identity and its nodes' statuses."""
    page = await tq.list(JobFilter(batch_id=batch_id, limit=1000))
    nodes = [
        {
            "job_id": str(j.id),
            "actor": str(j.actor),
            "status": str(j.status),
            "attempt": str(j.attempt),
        }
        for j in page.jobs
    ]
    return AdminSnapshot(moment=moment, batch={}, nodes=nodes)


# ---------------------------------------------------------------------------
# The plain example: one sync, end to end, whose LAST line is the typed
# success receipt. Run with a worker up:
#
#   taskq worker --actors examples.metering_sync.actors:registry &
#   TASKQ_PG_DSN=... python -m examples.metering_sync.pipeline
# ---------------------------------------------------------------------------


async def _main() -> int:
    from taskq.migrate import apply_pending_locked

    dsn = os.environ.get("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq")
    conn = await asyncpg.connect(dsn)
    try:
        await apply_pending_locked(conn=conn, schema="taskq")
    finally:
        await conn.close()
    await ensure_tables(dsn)

    result = await run_pipeline(dsn, run_id=f"demo-{stamp()}", total=200, page_size=50)
    print("eager overlap:", result.overlap.overlap_seconds, "s before producer end")
    print("progress events captured:", len(result.progress_events))
    for view in result.admin_views:
        print(f"admin view ({view.moment}): {len(view.nodes)} nodes visible")
    # THE LAST LINE IS THE PROOF: the typed receipt with the exact counts.
    print(result.receipt.model_dump_json())
    assert result.receipt.is_complete, f"sync incomplete: {result.receipt}"
    return 0


def stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
