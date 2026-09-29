"""Adversarial re-profile of the hot-path audit's ALTERNATIVE load shapes.

The audit's profile (``hotpath_load.py``) drives 8 actors CONTINUOUSLY with
non-batch jobs. This harness asks whether the profile's top costs HOLD under
two shapes the audit did not profile:

* ``batch``  — the REAL batch path: jobs enqueued through the TaskQ client's
  ``enqueue_batch`` (batch_id stamped in metadata, a batches row via
  ``AbortBatchAfter``) so the worker pays ``apply_batch_terminal_outcome``
  per terminal write (the audit's dispatch path had no batch hook).
* ``burst``  — cron-shaped bursts: 400-job slams with ~2 s idle gaps, so the
  worker alternates idle leader ticks with deep claim rounds.

Roles mirror ``hotpath_load.py`` (worker / client / orchestrate). The worker
role reuses the audit's own bootstrap (``hotpath_load.run_worker``) and
actors verbatim — only the CLIENT shape differs — but installs a richer
tracemalloc checkpoint that records EVERY K-job window (cumulative), so the
+52.7 B/job slope's stability across window sizes is measurable in one run
instead of one window per run.

Usage:
    python benchmarks/attack_hotpath_shapes.py orchestrate --shape batch --jobs 6000
    python benchmarks/attack_hotpath_shapes.py orchestrate --shape burst --jobs 6000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

from taskq import EnqueueItem

_BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_BENCH_DIR.parent))
sys.path.insert(0, str(_BENCH_DIR))  # hotpath_load importable

import hotpath_load as hl  # noqa: E402  # Why: reuse the audited bootstrap + actors verbatim.

RESULTS_DIR = _BENCH_DIR / "results"
LOG_DIR = RESULTS_DIR / "attack_shapes_logs"
DSN = hl.DSN

# ── the richer tracemalloc checkpoint (installed over H's) ─────────────
# Records EVERY window: cumulative diffs at K, 2K, 3K, ... so the per-job
# slope's stability across window sizes is one measurement, not one.

_TM_WINDOWS: list[dict[str, object]] = []
_TM_STATE: dict[str, object] = {"snap0": None, "jobs0": 0, "out": None}


def _attack_checkpoint() -> None:
    import tracemalloc

    if not tracemalloc.is_tracing():
        return
    snap = tracemalloc.take_snapshot()
    jobs = hl._JOB_COUNTER["done"]
    if _TM_STATE["snap0"] is None:
        _TM_STATE["snap0"] = snap
        _TM_STATE["jobs0"] = jobs
        return
    base = _TM_STATE["snap0"]
    n = max(1, jobs - int(_TM_STATE["jobs0"]))
    cmp = snap.compare_to(base, "lineno")
    total = sum(s.size_diff for s in cmp)
    count = sum(s.count_diff for s in cmp)
    _TM_WINDOWS.append(
        {
            "window_jobs": n,
            "bytes_per_job": round(total / n, 1),
            "objects_per_job": round(count / n, 1),
        }
    )
    out = _TM_STATE["out"]
    if out is not None:
        Path(str(out)).write_text(json.dumps(_TM_WINDOWS, indent=2))
    print(f"[tracemalloc] window {n} jobs -> {total / n:.0f} B/job, {count / n:.0f} obj/job")


hl._tracemalloc_checkpoint = _attack_checkpoint  # Why: the actors read the module global by name.


# ── client shapes ──────────────────────────────────────────────────────


async def client_batch(dsn: str, schema: str, jobs: int, batch_size: int) -> dict:
    """The REAL batch path: TaskQ client enqueue_batch with batch_id stamped.

    A failure policy is passed (threshold 10_000 — never fires under these
    noop actors) so the batches row exists and the worker's
    apply_batch_terminal_outcome pays reset+complete per succeeded job.
    """
    from taskq import TaskQ
    from taskq.batch_policy import AbortBatchAfter

    tq = TaskQ(dsn=dsn, schema=schema)
    await tq.__aenter__()
    actor_refs = list(hl.ACTORS.values())
    t0 = time.perf_counter()
    enqueued = 0
    n_batches = 0
    try:
        while enqueued < jobs:
            take = min(batch_size, jobs - enqueued)
            items = [
                EnqueueItem(
                    actor_ref=actor_refs[k % len(actor_refs)],
                    payload=hl.HotPayload(n=enqueued + k),
                )
                for k in range(take)
            ]
            await tq.enqueue_batch(
                items, failure_policy=AbortBatchAfter(consecutive_failures=10_000)
            )
            enqueued += take
            n_batches += 1
    finally:
        await tq.__aexit__(None, None, None)
    enqueue_s = time.perf_counter() - t0
    return {
        "shape": "batch",
        "enqueued": enqueued,
        "batches": n_batches,
        "batch_size": batch_size,
        "enqueue_s": round(enqueue_s, 3),
    }


async def client_burst(dsn: str, schema: str, jobs: int, burst_size: int, gap_s: float) -> dict:
    """Cron-shaped bursts: slam `burst_size` jobs, then idle `gap_s`."""
    import asyncpg

    from taskq.backend._protocol import EnqueueArgs
    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

    settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema})
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        deps,
        SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )
    actor_names = list(hl.ACTORS)
    t0 = time.perf_counter()
    enqueued = 0
    bursts = 0
    try:
        while enqueued < jobs:
            take = min(burst_size, jobs - enqueued)
            base = enqueued
            args_list = [
                EnqueueArgs(
                    id=hl.new_job_id(),
                    actor=actor_names[(base + k) % len(actor_names)],
                    queue=hl.QUEUE,
                    payload=hl.HotPayload(n=base + k).model_dump(mode="json"),
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
                for k in range(take)
            ]
            await backend.enqueue_batch(args_list)
            enqueued += take
            bursts += 1
            await asyncio.sleep(gap_s)
    finally:
        await pool.close()
    return {
        "shape": "burst",
        "enqueued": enqueued,
        "bursts": bursts,
        "burst_size": burst_size,
        "wall_s": round(time.perf_counter() - t0, 3),
    }


# ── orchestrate ────────────────────────────────────────────────────────


async def orchestrate(args) -> dict:
    dsn, schema = args.dsn, args.schema
    jobs = args.jobs
    tag = f"{args.shape}-{args.tag}" if args.tag else args.shape

    import asyncpg

    proc_pg = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    await proc_pg.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await proc_pg.execute(f'CREATE SCHEMA "{schema}"')
    await hl._migrate(dsn, schema)
    await proc_pg.execute("SELECT pg_stat_statements_reset()")
    await proc_pg.close()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    profile_out = str(RESULTS_DIR / f"attack-worker-{tag}.pstats")

    worker = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__)),
        "worker",
        "--dsn",
        dsn,
        "--schema",
        schema,
        "--profile-out",
        profile_out,
        "--tracemalloc-jobs",
        str(args.tracemalloc_jobs),
        stdout=open(LOG_DIR / f"worker-{tag}.log", "wb"),  # noqa: SIM115 ASYNC230  # Why: the handle must stay open for the subprocess's lifetime.
        stderr=subprocess.STDOUT,
    )
    print(f"[orchestrate] worker pid={worker.pid} shape={args.shape}")
    await asyncio.sleep(4.0)

    if args.shape == "batch":
        client = asyncio.create_task(client_batch(dsn, schema, jobs, args.batch_size))
    else:
        client = asyncio.create_task(client_burst(dsn, schema, jobs, args.burst_size, args.gap_s))

    t0 = time.perf_counter()
    # py-spy over the BUSY middle of the run.
    pyspy_proc = None
    if args.py_spy_seconds > 0:
        await asyncio.sleep(2.0)
        pyspy_out = str(RESULTS_DIR / f"attack-pyspy-{tag}.json")
        pyspy_proc = await asyncio.create_subprocess_exec(
            str(Path(sys.executable).parent / "py-spy"),
            "record",
            "--pid",
            str(worker.pid),
            "--rate",
            "200",
            "--duration",
            str(int(args.py_spy_seconds)),
            "--format",
            "speedscope",
            "-o",
            pyspy_out,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    enq = await client
    print(f"[orchestrate] enqueued {enq}")

    # Wait for full drain.
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    drained = False
    deadline = time.perf_counter() + 900
    while time.perf_counter() < deadline:
        n = await pool.fetchval(f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'")  # noqa: S608
        if n == 0:
            drained = True
            break
        await asyncio.sleep(0.5)
    busy_s = time.perf_counter() - t0

    if pyspy_proc is not None:
        try:
            _, err = await asyncio.wait_for(
                pyspy_proc.communicate(), timeout=args.py_spy_seconds + 90
            )
            if pyspy_proc.returncode != 0:
                print(f"[py-spy] rc={pyspy_proc.returncode}: {err.decode()[:400]}")
        except TimeoutError:
            pyspy_proc.kill()

    contention = await hl.sample_contention(pool, 0.1)
    stmts = await hl.stmt_by_query(pool)
    await pool.close()
    worker.terminate()
    await worker.wait()

    result = {
        "shape": args.shape,
        "jobs": jobs,
        "enqueued": enq,
        "drained": drained,
        "busy_seconds": round(busy_s, 2),
        "jps_over_busy_window": round(jobs / busy_s, 1) if busy_s else None,
        "contention": contention,
        "statements_top": sorted(
            ({"query": q, **v} for q, v in stmts.items()),
            key=lambda r: -r["calls"],
        )[:20],
        "tracemalloc_windows": _TM_WINDOWS,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "schema": schema,
    }
    out = RESULTS_DIR / f"attack-shape-{tag}.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"[orchestrate] wrote {out}")
    return result


def run_worker(dsn: str, schema: str, profile_out: str | None, tracemalloc_jobs: int) -> int:
    _TM_STATE["out"] = (
        str(
            RESULTS_DIR
            / f"attack-tracemalloc-{Path(profile_out).stem.replace('attack-worker-', '')}.json"
        )
        if profile_out
        else str(RESULTS_DIR / "attack-tracemalloc.json")
    )
    # hl.run_worker installs the hook and calls tracemalloc.start() itself;
    # only the output path and the richer checkpoint differ here.
    return hl.run_worker(dsn, schema, profile_out, tracemalloc_jobs, False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)

    p = sub.add_parser("worker")
    p.add_argument("--dsn", default=DSN)
    p.add_argument("--schema", default=hl.SCHEMA)
    p.add_argument("--profile-out", default=None)
    p.add_argument("--tracemalloc-jobs", type=int, default=0)

    c = sub.add_parser("client")
    c.add_argument("--dsn", default=DSN)
    c.add_argument("--schema", default=hl.SCHEMA)
    c.add_argument("--jobs", type=int, default=6000)
    c.add_argument("--shape", choices=["batch", "burst"], default="batch")
    c.add_argument("--batch-size", type=int, default=200)
    c.add_argument("--burst-size", type=int, default=400)
    c.add_argument("--gap-s", type=float, default=2.0)

    o = sub.add_parser("orchestrate")
    o.add_argument("--dsn", default=DSN)
    o.add_argument("--schema", default=None)
    o.add_argument("--jobs", type=int, default=6000)
    o.add_argument("--shape", choices=["batch", "burst"], default="batch")
    o.add_argument("--batch-size", type=int, default=200)
    o.add_argument("--burst-size", type=int, default=400)
    o.add_argument("--gap-s", type=float, default=2.0)
    o.add_argument("--py-spy-seconds", type=float, default=20.0)
    o.add_argument("--tracemalloc-jobs", type=int, default=1000)
    o.add_argument("--tag", default=None)

    args = parser.parse_args()
    if args.role == "worker":
        sys.exit(run_worker(args.dsn, args.schema, args.profile_out, args.tracemalloc_jobs))
    elif args.role == "client":
        if args.shape == "batch":
            sys.exit(asyncio.run(client_batch(args.dsn, args.schema, args.jobs, args.batch_size)))
        sys.exit(
            asyncio.run(client_burst(args.dsn, args.schema, args.jobs, args.burst_size, args.gap_s))
        )
    else:
        if args.schema is None:
            args.schema = f"tq_bench_attack_{args.shape}"
        sys.exit(asyncio.run(orchestrate(args)))


if __name__ == "__main__":
    main()
