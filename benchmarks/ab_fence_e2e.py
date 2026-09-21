"""Interleaved A/B of the full e2e drain, toggling the fence index in-place.

A = one-key form (pre-01.00.19_01), B = two-key form.  Same harness
(e2e_dispatch.run_e2e), same schema, index swapped between reps;
alternating A/B batches so drift cancels; 5 reps each side; reports
per-side medians of jobs/sec, the mark_succeeded share, and
enqueue->claimed latency percentiles.
"""

# Why: every f-string SQL below interpolates only this module's own benchmark-controlled schema identifier; every value is $n-bound.

from __future__ import annotations

import asyncio
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import asyncpg
from e2e_dispatch import build_backend, cleanup_schema, setup_schema

DSN = "postgresql://taskq:taskq@localhost:55433/taskq"
SCHEMA = "tq_bench_ab_fence"
JOBS = 5000
BATCH = 50
REPS = 5

IDX_A = "CREATE INDEX jobs_locked_by_worker_running_idx ON \"{schema}\".jobs (locked_by_worker) WHERE status = 'running'"
IDX_B = "CREATE INDEX jobs_locked_by_worker_running_idx ON \"{schema}\".jobs (locked_by_worker, id) WHERE status = 'running'"


async def set_index(conn: asyncpg.Connection, schema: str, index_sql: str) -> None:
    await conn.execute(f'DROP INDEX IF EXISTS "{schema}".jobs_locked_by_worker_running_idx')
    await conn.execute(index_sql.format(schema=schema))


async def main() -> None:
    admin = await asyncpg.connect(DSN)
    await setup_schema(DSN, SCHEMA)
    backend, _deps, pool = await build_backend(DSN, SCHEMA)
    samples: dict[str, list[dict]] = {"A": [], "B": []}
    try:
        for rep in range(REPS):
            for tag in ("A", "B") if rep % 2 == 0 else ("B", "A"):
                await set_index(admin, SCHEMA, IDX_A if tag == "A" else IDX_B)
                r = await backend_run(backend)
                samples[tag].append(r)
                print(
                    f"rep{rep} {tag}: jobs/sec={r['jobs_per_sec']:.0f} "
                    f"mark={r['mark_share_pct']:.1f}% dispatch={r['dispatch_share_pct']:.1f}%"
                )
                await admin.execute(
                    f'TRUNCATE "{SCHEMA}".jobs, "{SCHEMA}".job_attempts, "{SCHEMA}".job_events CASCADE'
                )
    finally:
        await pool.close()
        await cleanup_schema(DSN, SCHEMA)
        await admin.close()

    report = {}
    for tag, rs in samples.items():
        report[tag] = {
            "jobs_per_sec_median": statistics.median(r["jobs_per_sec"] for r in rs),
            "mark_share_median_pct": statistics.median(r["mark_share_pct"] for r in rs),
            "dispatch_share_median_pct": statistics.median(r["dispatch_share_pct"] for r in rs),
            "runs": [{k: round(v, 2) for k, v in r.items()} for r in rs],
        }
    print(json.dumps(report, indent=1))
    out = Path(__file__).parent / "results" / "fence-e2e-ab.json"
    out.write_text(json.dumps(report, indent=1))


async def backend_run(backend) -> dict:
    from e2e_dispatch import make_payload

    from taskq._ids import new_job_id
    from taskq.backend._protocol import EnqueueArgs

    n_jobs, batch = JOBS, BATCH
    worker_id = new_job_id()  # uuid, fine as worker id for the bench shape
    import time as _time
    from datetime import timedelta

    lock_lease = timedelta(seconds=30)
    splits: dict[str, float] = {"mark_succeeded": 0.0, "dispatch_batch": 0.0}
    wall_t0 = _time.perf_counter()
    remaining, i = n_jobs, 0
    while remaining > 0:
        take = min(200, remaining)
        args_list = [
            EnqueueArgs(
                id=new_job_id(),
                actor="bench_actor",
                queue="default",
                payload=make_payload(i + k),
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(take)
        ]
        await backend.enqueue_batch(args_list, enforce_max_pending=False)
        i += take
        remaining -= take
    claimed, empty = 0, 0
    while claimed < n_jobs:
        t0 = _time.perf_counter()
        rows = await backend.dispatch_batch(worker_id, ["default"], batch, lock_lease)
        splits["dispatch_batch"] += _time.perf_counter() - t0
        if not rows:
            empty += 1
            if empty > 3:
                break
            await asyncio.sleep(0.01)
            continue
        empty = 0
        for row in rows:
            t2 = _time.perf_counter()
            ok = await backend.mark_succeeded(
                row.id,
                worker_id,
                {"status": "ok"},
                attempt=row.attempt,
                claim_epoch=row.claim_epoch,
            )
            splits["mark_succeeded"] += _time.perf_counter() - t2
            if not ok:
                raise RuntimeError("fenced write no-op - harness drift")
            claimed += 1
    wall = _time.perf_counter() - wall_t0
    return {
        "jobs_per_sec": n_jobs / wall,
        "wall_s": wall,
        "mark_share_pct": 100 * splits["mark_succeeded"] / wall,
        "dispatch_share_pct": 100 * splits["dispatch_batch"] / wall,
    }


if __name__ == "__main__":
    asyncio.run(main())
