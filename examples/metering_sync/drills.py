"""The drills: the pipeline's properties proven at a real Postgres, captured.

Each drill runs the pipeline end to end against a live Postgres and a
live worker, and writes its receipt (JSON) into ``.measurements/``:

- ``overlap``    — the first consumed item starts BEFORE the producer
                   finishes producing (the eager handoff, measured).
- ``idempotency``— a second full run over the same source data changes
                   nothing (row counts + content hashes identical).
- ``resume``     — SIGKILL the worker mid-run; restart; the run completes
                   with nothing lost and nothing duplicated.
- ``load``       — a few thousand items; the throughput reported honestly.
- ``cron``       — the scheduled tick fires while a worker is up.

Usage::

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
        python -m examples.metering_sync.drills --drill all

Starts and stops its own worker processes (``taskq worker``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import asyncpg

from taskq import TaskQ
from taskq.batch import EnqueueItem
from taskq.migrate import apply_pending_locked

from .actors import (
    FinalizeSyncPayload,
    batch_id_for,
    finalize_sync,
    process_reading,
)
from .pipeline import (
    ensure_tables,
    feed_checksum,
    readings_fingerprint,
    run_pipeline,
    source_readings,
)

MEASUREMENTS = Path(".measurements")
DSN = os.environ.get("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq")
SCHEMA = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
QUEUES = ["ingest", "review", "finalize"]
WORKER_CONCURRENCY = "16"


# ---------------------------------------------------------------------------
# Worker lifecycle
# ---------------------------------------------------------------------------


class Worker:
    """A ``taskq worker`` subprocess for the example's registry."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        taskq_bin = shutil.which("taskq")
        if taskq_bin is None:
            raise RuntimeError("taskq CLI not on PATH (install the package first)")
        env = dict(
            os.environ,
            TASKQ_PG_DSN=DSN,
            TASKQ_SCHEMA_NAME=SCHEMA,
            TASKQ_MAX_CONCURRENCY=WORKER_CONCURRENCY,
        )
        self.proc = subprocess.Popen(  # noqa: S603 — Why: fixed argv, the worker CLI IS the drill's job
            [
                taskq_bin,
                "worker",
                "--actors",
                "examples.metering_sync.actors:registry",
                *(f"--queues={q}" for q in QUEUES),
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    def kill9(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None


async def wait_for_worker(db: asyncpg.Pool, within: float = 60.0) -> None:
    """Block until a worker heartbeats into the workers table."""
    async with asyncio.timeout(within):
        while True:
            n = await db.fetchval("SELECT count(*) FROM taskq.workers")
            if n and n > 0:
                return
            await asyncio.sleep(0.1)


async def wait_until_job_running(db: asyncpg.Pool, run_id: str, within: float) -> None:
    """Block until some job of the run is mid-flight (the kill target exists)."""
    async with asyncio.timeout(within):
        while True:
            n = await db.fetchval(
                """
                SELECT count(*) FROM taskq.jobs
                WHERE status::text = 'running' AND payload->>'run_id' = $1
                """,
                run_id,
            )
            if n and n > 0:
                return
            await asyncio.sleep(0.05)


async def wait_batch_terminal(
    db: asyncpg.Pool, batch_id: str, within: float = 300.0
) -> dict[str, int]:
    """Poll the batch to terminal (the docs' GIN-indexed batch predicate)."""
    async with asyncio.timeout(within):
        while True:
            row = await db.fetchrow(
                """
                SELECT count(*) AS total,
                       count(*) FILTER (
                           WHERE status::text IN ('pending', 'scheduled', 'running')
                       ) AS pending,
                       count(*) FILTER (WHERE status::text = 'succeeded') AS succeeded,
                       count(*) FILTER (WHERE status::text = 'failed') AS failed,
                       count(*) FILTER (
                           WHERE status::text IN ('cancelled', 'crashed', 'abandoned')
                       ) AS dead
                FROM taskq.jobs
                WHERE metadata @> $1::jsonb
                """,
                f'{{"batch_id": "{batch_id}"}}',
            )
            assert row is not None
            counts = dict(row)
            if int(counts["total"]) > 0 and int(counts["pending"]) == 0:
                return counts
            await asyncio.sleep(0.2)


# ---------------------------------------------------------------------------
# Setup / receipts
# ---------------------------------------------------------------------------


async def setup() -> None:
    """Migrations + the example's own tables."""
    conn = await asyncpg.connect(DSN)
    try:
        await apply_pending_locked(conn=conn, schema=SCHEMA)
    finally:
        await conn.close()
    await ensure_tables(DSN)


def save(name: str, receipt: dict[str, object]) -> Path:
    MEASUREMENTS.mkdir(exist_ok=True)
    path = MEASUREMENTS / name
    path.write_text(json.dumps(receipt, indent=2, default=str) + "\n")
    print(f"  receipt -> {path}")
    return path


def stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# The drills
# ---------------------------------------------------------------------------


async def drill_overlap() -> None:
    """THE EAGER-OVERLAP PROOF: the first consumed item starts before the
    producer finishes producing."""
    print("[drill] eager overlap")
    run_id = f"ovl-{stamp()}"
    worker = Worker("overlap")
    worker.start()
    db = await asyncpg.create_pool(DSN)
    try:
        await wait_for_worker(db)
        result = await run_pipeline(DSN, run_id, total=600, page_size=100)
        o = result.overlap
        ok = o.overlaps
        mid_views = [v for v in result.admin_views if v.moment == "mid-run"]
        mid_nodes = mid_views[0].nodes if mid_views else []
        receipt = {
            "drill": "overlap",
            "run_id": run_id,
            "first_consumed_at": o.first_consumed_at,
            "producer_finished_at": o.producer_finished_at,
            "overlap_seconds": o.overlap_seconds,
            "overlaps": ok,
            "final_receipt": result.receipt.model_dump(),
            "mid_run_admin_nodes_visible": len(mid_nodes),
            "mid_run_node_statuses": sorted({n["status"] for n in mid_nodes}),
            "progress_events_captured": len(result.progress_events),
            "ok": ok,
        }
        save(f"probe-sync-overlap-{stamp()}.json", receipt)
        assert o.overlaps, (
            f"NO OVERLAP: first consumed {o.first_consumed_at} "
            f">= producer end {o.producer_finished_at}"
        )
        assert result.receipt.is_complete, f"receipt incomplete: {result.receipt}"
        print(f"  overlap measured: {o.overlap_seconds}s before producer end")
    finally:
        await db.close()
        worker.stop()


async def drill_idempotency() -> None:
    """THE IDEMPOTENCY DRILL: run the whole pipeline twice over the same
    source data; the second run's upserts change nothing."""
    print("[drill] idempotency (two full runs, same data)")
    run_id = f"idem-{stamp()}"
    worker = Worker("idempotency")
    worker.start()
    db = await asyncpg.create_pool(DSN)
    try:
        await wait_for_worker(db)
        readings = source_readings(run_id, 300)
        expected_hash = feed_checksum(readings)

        first = await run_pipeline(DSN, run_id, total=300, page_latency=0, observe_overlap=False)
        conn = await db.acquire()
        count_after_first, hash_after_first = await readings_fingerprint(conn, run_id)
        probe_after_first = await db.fetchval(
            "SELECT count(*) FROM sync_probe_events WHERE reading_id LIKE $1 || '%'", run_id
        )
        await db.release(conn)

        # The SECOND run: same run id, same idempotency keys, same data.
        second = await run_pipeline(DSN, run_id, total=300, page_latency=0, observe_overlap=False)
        conn = await db.acquire()
        count_after_second, hash_after_second = await readings_fingerprint(conn, run_id)
        probe_after_second = await db.fetchval(
            "SELECT count(*) FROM sync_probe_events WHERE reading_id LIKE $1 || '%'", run_id
        )
        await db.release(conn)

        ok = (
            first.receipt.is_complete
            and second.receipt.is_complete
            and count_after_first == count_after_second == 300
            and hash_after_first == hash_after_second == expected_hash
            and probe_after_second == probe_after_first
        )
        receipt = {
            "drill": "idempotency",
            "run_id": run_id,
            "expected_rows": 300,
            "rows_after_first_run": count_after_first,
            "rows_after_second_run": count_after_second,
            "hash_after_first_run": hash_after_first,
            "hash_after_second_run": hash_after_second,
            "source_hash": expected_hash,
            "worker_touches_after_first_run": probe_after_first,
            "worker_touches_after_second_run": probe_after_second,
            "first_receipt": first.receipt.model_dump(),
            "second_receipt": second.receipt.model_dump(),
            "ok": ok,
        }
        save(f"probe-sync-idempotency-{stamp()}.json", receipt)
        assert ok, "the second run changed rows or lost rows"
        print(
            f"  second run changed nothing: {count_after_second} rows, hash identical, "
            f"worker touches {probe_after_first} -> {probe_after_second}"
        )
    finally:
        await db.close()
        worker.stop()


async def drill_resume() -> None:
    """THE RESUME DRILL: SIGKILL the worker mid-run; restart; the run
    completes — nothing lost, nothing duplicated."""
    print("[drill] resume after SIGKILL")
    run_id = f"res-{stamp()}"
    total = 400
    readings = source_readings(run_id, total)
    expected_hash = feed_checksum(readings)
    batch_id = batch_id_for(run_id)
    items = [
        EnqueueItem(
            actor_ref=process_reading,
            payload=r,
            idempotency_key=f"reading:{run_id}:{r.reading_id}",
        )
        for r in readings
    ]

    worker = Worker("resume-a")
    worker.start()
    tq = TaskQ(dsn=DSN)
    await tq.open()
    db = await asyncpg.create_pool(DSN)
    try:
        await wait_for_worker(db)
        producer_task = asyncio.create_task(
            tq.enqueue_batch_streaming(iter(items), batch_id=UUID(batch_id), chunk_size=50)
        )

        # Kill mid-flight: some jobs are mid-run when the SIGKILL lands.
        await wait_until_job_running(db, run_id, within=60)
        await asyncio.sleep(0.3)  # let the first wave actually process
        worker.kill9()
        print("  worker SIGKILLed mid-run; restarting")
        worker = Worker("resume-b")
        worker.start()
        await wait_for_worker(db)

        await producer_task
        counts = await wait_batch_terminal(db, batch_id)

        finalizer = await tq.enqueue(
            finalize_sync,
            FinalizeSyncPayload(batch_id=batch_id, run_id=run_id, expected=total),
            idempotency_key=f"finalize:{run_id}",
        )
        receipt = await finalizer.wait(timeout=120.0)

        conn = await db.acquire()
        rows, hash_now = await readings_fingerprint(conn, run_id)
        await db.release(conn)
        max_attempt = await db.fetchval(
            """
            SELECT max(attempt) FROM taskq.jobs WHERE metadata @> $1::jsonb
            """,
            f'{{"batch_id": "{batch_id}"}}',
        )
        distinct_touched = await db.fetchval(
            "SELECT count(DISTINCT reading_id) FROM sync_probe_events WHERE reading_id LIKE $1 || '%'",
            run_id,
        )
    finally:
        worker.stop()
        await db.close()
        await tq.close()

    ok = (
        receipt.is_complete
        and rows == total
        and hash_now == expected_hash
        and distinct_touched == total
    )
    receipt_json = {
        "drill": "resume",
        "run_id": run_id,
        "expected_rows": total,
        "rows_after_restart": rows,
        "distinct_readings_touched": distinct_touched,
        "content_hash_matches_source": hash_now == expected_hash,
        "max_attempt_seen": max_attempt,
        "terminal_batch_counts": counts,
        "final_receipt": receipt.model_dump(),
        "ok": ok,
    }
    save(f"probe-sync-resume-{stamp()}.json", receipt_json)
    assert ok, f"resume failed: rows={rows} hash_ok={hash_now == expected_hash}"
    print(
        f"  resumed after SIGKILL: {rows} rows, {distinct_touched} readings touched, "
        f"hash identical, max attempt {max_attempt}"
    )


async def drill_load() -> None:
    """THE LOAD FACE: a few thousand items; the throughput, honestly."""
    print("[drill] load (3000 items)")
    run_id = f"load-{stamp()}"
    total = 3000
    worker = Worker("load")
    worker.start()
    db = await asyncpg.create_pool(DSN)
    try:
        await wait_for_worker(db)
        t0 = time.monotonic()
        result = await run_pipeline(
            DSN, run_id, total=total, page_size=500, page_latency=0, observe_overlap=False
        )
        wall = time.monotonic() - t0
        ok = result.receipt.is_complete
        receipt = {
            "drill": "load",
            "run_id": run_id,
            "items": total,
            "wall_seconds": round(wall, 3),
            "throughput_items_per_second": round(total / wall, 1),
            "final_receipt": result.receipt.model_dump(),
            "ok": ok,
        }
        save(f"probe-sync-load-{stamp()}.json", receipt)
        assert ok, f"load run incomplete: {result.receipt}"
        print(f"  {total} items in {wall:.1f}s = {total / wall:.0f} items/s")
    finally:
        await db.close()
        worker.stop()


async def drill_cron() -> None:
    """THE SCHEDULED FACE: with a worker up, the tick fires on its cadence."""
    print("[drill] cron face (30s tick)")
    worker = Worker("cron")
    worker.start()
    db = await asyncpg.create_pool(DSN)
    try:
        await wait_for_worker(db)
        print("  waiting 75s for two cron ticks...")
        await asyncio.sleep(75)
        fires = await db.fetchval(
            "SELECT count(*) FROM taskq.jobs WHERE actor = 'sync_readings_tick'"
        )
        tick_readings = await db.fetchval(
            "SELECT count(*) FROM meter_readings WHERE run_id LIKE 'cron-%'"
        )
    finally:
        await db.close()
        worker.stop()
    ok = (fires or 0) > 0 and (tick_readings or 0) > 0
    receipt = {
        "drill": "cron",
        "tick_fires": fires,
        "cron_readings": tick_readings,
        "ok": ok,
    }
    save(f"probe-sync-cron-{stamp()}.json", receipt)
    assert ok, "the cron schedule never fired (or its children never ran)"
    print(f"  cron fired {fires} ticks -> {tick_readings} readings")


async def drill_admin_view() -> None:
    """THE ADMIN VIEW: an operator can SEE the run mid-flight and after."""
    print("[drill] admin view (mid-run + terminal)")
    run_id = f"adm-{stamp()}"
    worker = Worker("admin")
    worker.start()
    db = await asyncpg.create_pool(DSN)
    try:
        await wait_for_worker(db)

        # Drive the pipeline in a task so we can peek mid-run.
        run_task = asyncio.create_task(
            run_pipeline(DSN, run_id, total=600, page_size=100, observe_overlap=False)
        )
        await asyncio.sleep(0)  # yield so the producer starts

        mid_view: dict[str, object] | None = None
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not run_task.done():
            n = await db.fetchval(
                """
                SELECT count(*) FROM taskq.jobs
                WHERE status::text IN ('running', 'succeeded') AND payload->>'run_id' = $1
                """,
                run_id,
            )
            if n and n > 0:
                batch_id = batch_id_for(run_id)
                row = await db.fetchrow(
                    "SELECT id, actor, status, attempt, queue FROM taskq.jobs "
                    "WHERE metadata @> $1::jsonb ORDER BY id LIMIT 5",
                    f'{{"batch_id": "{batch_id}"}}',
                )
                assert row is not None
                mid_view = {
                    "moment": "mid-run",
                    "batch_id": batch_id,
                    "sample_nodes": [dict(row)],
                }
                break
            await asyncio.sleep(0.05)

        result = await run_task
        batch_id = batch_id_for(run_id)
        terminal_rows = await db.fetch(
            """
            SELECT status, count(*) AS n FROM taskq.jobs
            WHERE metadata @> $1::jsonb GROUP BY status ORDER BY status
            """,
            f'{{"batch_id": "{batch_id}"}}',
        )
        finalizer_rows = await db.fetch(
            "SELECT actor, status, attempt FROM taskq.jobs WHERE actor = 'finalize_sync' "
            "AND payload->>'run_id' = $1",
            run_id,
        )
        terminal_view = {
            "moment": "terminal",
            "batch_id": batch_id,
            "status_counts": [dict(r) for r in (terminal_rows or [])],
            "finalizer": [dict(r) for r in finalizer_rows],
            "receipt": result.receipt.model_dump(),
        }
        ok = (
            mid_view is not None
            and result.receipt.is_complete
            and all(str(r["status"]) == "succeeded" for r in finalizer_rows)
        )
        receipt = {
            "drill": "admin-view",
            "run_id": run_id,
            "mid_run": mid_view,
            "terminal": terminal_view,
            "progress_events_captured": len(result.progress_events),
            "ok": ok,
        }
        save(f"probe-sync-admin-view-{stamp()}.json", receipt)
        assert ok, "the admin view could not see the run mid-flight or the terminal state"
        print("  saw the run mid-flight and the terminal state (receipt captured)")
    finally:
        await db.close()
        worker.stop()


# ---------------------------------------------------------------------------


async def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--drill",
        default="all",
        choices=["all", "overlap", "idempotency", "resume", "load", "cron", "admin"],
    )
    args = parser.parse_args(argv)

    print(f"pipeline drills against {DSN}")
    await setup()

    which = args.drill
    if which in ("all", "overlap"):
        await drill_overlap()
    if which in ("all", "idempotency"):
        await drill_idempotency()
    if which in ("all", "resume"):
        await drill_resume()
    if which in ("all", "load"):
        await drill_load()
    if which in ("all", "admin"):
        await drill_admin_view()
    if which in ("all", "cron"):
        await drill_cron()

    print("ALL DRILLS GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
