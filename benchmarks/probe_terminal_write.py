"""Probe: where does a terminal write's per-job cost go?

Isolates pool acquire/release overhead vs the fused statement's own cost,
and the dispatch claim round, on the e2e bench's real-Postgres harness.

Read-only with respect to src/; writes nothing outside results/.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from e2e_dispatch import build_backend, cleanup_schema, make_payload, setup_schema

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs

DSN = "postgresql://taskq:taskq@localhost:55433/taskq"
SCHEMA = "tq_bench_probe"
N = 2000


def med(xs):
    return statistics.median(xs) * 1000  # ms


async def main() -> dict:
    await setup_schema(DSN, SCHEMA)
    backend, _deps, pool = await build_backend(DSN, SCHEMA)
    worker_id = new_uuid()
    try:
        args_list = [
            EnqueueArgs(
                id=new_job_id(),
                actor="bench_actor",
                queue="default",
                payload=make_payload(k),
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(N)
        ]
        inserted = await backend.enqueue_batch_fast(args_list)
        assert inserted == N
        # The repo's perf-evidence protocol: plans are only stable at
        # realistic volumes after a VACUUM ANALYZE; stale reltuples make
        # every eqsel estimate collapse to one row and the planner picks
        # arbitrary indexes on the fence UPDATE.
        async with pool.acquire() as conn:
            await conn.execute('VACUUM ANALYZE "tq_bench_probe".jobs')
            await conn.execute('VACUUM ANALYZE "tq_bench_probe".job_events')
            await conn.execute('VACUUM ANALYZE "tq_bench_probe".job_attempts')

        # 1) pool acquire+release only (no statement)
        t = []
        for _ in range(200):
            t0 = time.perf_counter()
            async with pool.acquire(timeout=5):
                pass
            t.append(time.perf_counter() - t0)

        # 2) round-trip probe: SELECT 1 over acquired pool conn
        t2 = []
        for _ in range(200):
            async with pool.acquire(timeout=5) as conn:
                t0 = time.perf_counter()
                await conn.fetch("SELECT 1")
                t2.append(time.perf_counter() - t0)

        # 3) dispatch_batch rounds of 50
        lease = timedelta(seconds=30)
        t3 = []
        claimed = []
        while len(claimed) < N:
            t0 = time.perf_counter()
            batch = await backend.dispatch_batch(worker_id, ["default"], 50, lease)
            t3.append(time.perf_counter() - t0)
            claimed.extend(batch)
            if not batch:
                break

        # 4) mark_succeeded sequentially (the bench's drain shape)
        t4 = []
        for row in claimed:
            t0 = time.perf_counter()
            ok = await backend.mark_succeeded(
                row.id,
                worker_id,
                {"status": "ok", "n": 1},
                attempt=row.attempt,
                claim_epoch=row.claim_epoch,
            )
            t4.append(time.perf_counter() - t0)
            assert ok

        return {
            "pool_acquire_ms": med(t),
            "select1_over_pool_ms": med(t2),
            "dispatch_batch_50_ms": med(t3),
            "mark_succeeded_ms": med(t4),
            "mark_p50_ms": med(t4),
            "mark_p99_ms": sorted(t4)[int(len(t4) * 0.99)] * 1000,
        }
    finally:
        await pool.close()
        await cleanup_schema(DSN, SCHEMA)


if __name__ == "__main__":
    r = asyncio.run(main())
    out = Path(__file__).parent / "results" / "probe-terminal.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(r, indent=2))
    print(json.dumps(r, indent=2))
