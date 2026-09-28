"""Log-level A/B: the ``TASKQ_LOG_EVENTS_LEVEL`` knob's CPU delta, measured.

The hot-path audit (benchmarks/results/hotpath-profile.{json,md} on the
audit-perf branch) attributed ~23% of the worker's on-CPU time to the
logging pipeline: ~2 ``state-change`` INFO lines per job through
structlog -> stdlib -> orjson, every line a STREAMING DUPLICATE of a
``job_events`` row the same transaction just committed. This harness
measures what the knob's ``warning`` level honestly buys back.

Shape (the audit harness's, benchmarks/hotpath_load.py): a REAL worker
subprocess (the production ``worker_main`` bootstrap, 8 actors, one
queue) draining continuously-enqueued non-batch jobs; a client role
enqueues via the backend; an orchestrate role owns the schema, runs the
A/B INTERLEAVED (a b a b a b - the audit's interleaved discipline, so
drift and thermal effects hit both arms equally), samples the worker's
CPU from /proc/<pid>/stat (utime+stime) around each drain, and - with
``--profile`` - runs the worker under cProfile to attribute the
structlog share directly:

    python benchmarks/loglevel_ab.py orchestrate --jobs 4000

Artifacts: benchmarks/results/loglevel-cpu-delta.{json,md}. The claim
the numbers serve: at ``warning`` the per-job happy-path lines are gone
BEFORE the serialization cost (one frozenset lookup per event at the
filter, then the line is dropped), the anomaly stream still emits, and
the job_events ledger is untouched - the CPU delta is the honest
reduction of the audit's 23%.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_BENCH_DIR.parent))  # taskq importable from the worktree

DSN = os.environ.get("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq")
SCHEMA = "tq_bench_loglevel"
QUEUE = "hotpath"
RESULTS_DIR = _BENCH_DIR / "results"
LOG_DIR = RESULTS_DIR / "loglevel_logs"
N_ACTORS = 8

# ── the actors (module level: the decorator registers real ActorRefs) ──

from pydantic import BaseModel  # noqa: E402

from taskq import actor  # noqa: E402
from taskq._ids import (  # noqa: E402
    new_job_id,  # Why: scratch benchmark rows, PK locality is not the variable under test.
)


class HotPayload(BaseModel):
    n: int = 0


@actor(name="hot_noop_0", queue=QUEUE)
async def hot_noop_0(
    payload: HotPayload,
) -> (
    None
):  # Why: the payload IS the job's work shape; a noop body isolates the framework's per-job cost.
    return


@actor(name="hot_noop_1", queue=QUEUE)
async def hot_noop_1(payload: HotPayload) -> None:
    return


@actor(name="hot_noop_2", queue=QUEUE)
async def hot_noop_2(payload: HotPayload) -> None:
    return


@actor(name="hot_noop_3", queue=QUEUE)
async def hot_noop_3(payload: HotPayload) -> None:
    return


@actor(name="hot_noop_4", queue=QUEUE)
async def hot_noop_4(payload: HotPayload) -> None:
    return


@actor(name="hot_noop_5", queue=QUEUE)
async def hot_noop_5(payload: HotPayload) -> None:
    return


@actor(name="hot_noop_6", queue=QUEUE)
async def hot_noop_6(payload: HotPayload) -> None:
    return


@actor(name="hot_noop_7", queue=QUEUE)
async def hot_noop_7(payload: HotPayload) -> None:
    return


ACTORS = {
    f"hot_noop_{i}": ref
    for i, ref in enumerate(
        [
            hot_noop_0,
            hot_noop_1,
            hot_noop_2,
            hot_noop_3,
            hot_noop_4,
            hot_noop_5,
            hot_noop_6,
            hot_noop_7,
        ]
    )
}


# ── worker role ───────────────────────────────────────────────────────


def run_worker(dsn: str, schema: str, events_level: str, profile_out: str | None) -> int:
    """Real TaskQ worker bootstrap: settings -> worker_main (blocking)."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import worker_main

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_MAX_CONCURRENCY": "8",
            "TASKQ_QUEUES": QUEUE,
            "TASKQ_LOG_EVENTS_LEVEL": events_level,
        }
    )
    prof = None
    if profile_out:
        import cProfile

        prof = cProfile.Profile()
        prof.enable()
    try:
        return worker_main(settings, actor_registry=ACTORS)
    finally:
        if prof is not None:
            prof.disable()
            prof.dump_stats(profile_out)
            print(f"[cProfile] dumped to {profile_out}")
        with contextlib.suppress(ValueError):  # Why: not the main thread -> no window to guard.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)


# ── client role ───────────────────────────────────────────────────────


async def client_enqueue(dsn: str, schema: str, jobs: int) -> dict:
    """Continuous enqueue at the backend layer (the audit harness's
    client shape: non-batch enqueue, batches of 100 to keep the client's
    own overhead off the worker's critical path)."""
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
    actor_names = list(ACTORS)
    t0 = time.perf_counter()
    enqueued = 0
    batch = 100
    try:
        while enqueued < jobs:
            take = min(batch, jobs - enqueued)
            base = enqueued
            args_list = [
                EnqueueArgs(
                    id=new_job_id(),
                    actor=actor_names[(base + k) % len(actor_names)],
                    queue=QUEUE,
                    payload=HotPayload(n=base + k).model_dump(mode="json"),
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
                for k in range(take)
            ]
            rows = await backend.enqueue_batch(args_list)
            enqueued += len(rows)
    finally:
        await pool.close()
    return {"enqueued": enqueued, "enqueue_s": round(time.perf_counter() - t0, 3)}


# ── orchestrate role ──────────────────────────────────────────────────


def _proc_cpu_ticks(pid: int) -> tuple[int, int]:
    """(utime, stime) clock ticks for a live pid, read from /proc."""
    with open(f"/proc/{pid}/stat", "rb") as f:
        fields = f.read().rsplit(b")", 1)[-1].split()
    return int(fields[11]), int(fields[12])


async def _pg(dsn: str):
    import asyncpg

    return await asyncpg.create_pool(dsn, min_size=1, max_size=2)


async def _migrate(dsn: str, schema: str) -> None:
    from taskq.migrate import apply_pending_locked

    await apply_pending_locked(dsn, schema=schema)


async def run_one(
    dsn: str, schema: str, jobs: int, events_level: str, run_id: int, profile: bool
) -> dict:
    """One drain: fresh schema state (truncate), worker at ``events_level``,
    client enqueues ``jobs``; measure drain wall time and worker CPU."""
    pool = await _pg(dsn)
    await pool.execute(f'TRUNCATE "{schema}".jobs, "{schema}".job_events CASCADE')
    await pool.close()

    profile_out = (
        str(RESULTS_DIR / f"loglevel-worker-{events_level}-r{run_id}.pstats") if profile else None
    )
    log_file = open(LOG_DIR / f"worker-{events_level}-r{run_id}.log", "wb")  # noqa: SIM115, ASYNC230  # Why: the handle must stay open for the subprocess's lifetime; one blocking open at spawn, never in the loop.
    worker = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__)),
        "worker",
        "--dsn",
        dsn,
        "--schema",
        schema,
        "--events-level",
        events_level,
        *(("--profile-out", profile_out) if profile_out else ()),
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    try:
        await asyncio.sleep(4.0)  # bootstrap (leader election, registration)
        cpu0 = sum(_proc_cpu_ticks(worker.pid))
        t0 = time.perf_counter()
        enq = await client_enqueue(dsn, schema, jobs)
        deadline = time.perf_counter() + 600
        pool = await _pg(dsn)
        try:
            while time.perf_counter() < deadline:
                n = await pool.fetchval(
                    f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"  # noqa: S608
                )
                if n == 0:
                    break
                await asyncio.sleep(0.25)
            else:
                raise TimeoutError("the drain never finished")
        finally:
            await pool.close()
        drain_s = time.perf_counter() - t0
        cpu1 = sum(_proc_cpu_ticks(worker.pid))
    finally:
        worker.terminate()
        await worker.wait()
        log_file.close()

    ticks = cpu1 - cpu0
    cpu_s = ticks / os.sysconf("SC_CLK_TCK")
    return {
        "level": events_level,
        "run": run_id,
        "jobs": enq["enqueued"],
        "drain_s": round(drain_s, 2),
        "throughput_jps": round(enq["enqueued"] / drain_s, 1),
        "worker_cpu_s": round(cpu_s, 3),
        "cpu_ms_per_job": round(cpu_s * 1000 / enq["enqueued"], 3),
        "profile_out": profile_out,
    }


def _pstats_structlog_share(path: str) -> dict:
    """The structlog pipeline's inclusive share of one cProfile run."""
    import pstats

    st = pstats.Stats(path)
    total = st.total_tt
    if total <= 0:
        return {"total_s": 0.0, "structlog_share_pct": None}
    structlog_s = 0.0
    for (filename, _line, func), (_cc, _nc, _tt, ct, _callers) in st.stats.items():
        if "structlog" in filename and func in ("info", "_proxy_to_logger", "warning", "error"):
            structlog_s += ct
    return {
        "total_s": round(total, 2),
        "structlog_inclusive_s": round(structlog_s, 2),
        "structlog_share_pct": round(100 * structlog_s / total, 1),
    }


async def orchestrate(args) -> dict:
    dsn, schema, jobs = args.dsn, args.schema, args.jobs
    RESULTS_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    pool = await _pg(dsn)
    await pool.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # Why: benchmark-owned schema.
    await pool.execute(f'CREATE SCHEMA "{schema}"')
    await pool.close()
    await _migrate(dsn, schema)

    # Interleaved A/B: i0-info, i0-warning, i1-info, i1-warning, ... - the
    # audit's interleaved discipline, so drift and thermal effects hit
    # both arms equally.
    interleaved: list[tuple[str, int]] = []
    for i in range(args.runs):
        interleaved.append(("info", i))
        interleaved.append(("warning", i))
    runs = [
        await run_one(dsn, schema, jobs, level, i, args.profile and i == 0)
        for level, i in interleaved
    ]

    def _median(level: str, key: str) -> float:
        vals = sorted(r[key] for r in runs if r["level"] == level)
        return vals[len(vals) // 2]

    summary: dict = {
        "what": "TASKQ_LOG_EVENTS_LEVEL=warning vs info: the worker's CPU per drained job, interleaved A/B (the audit harness's shape: real worker_main subprocess, 8 actors, non-batch enqueue)",
        "harness": "benchmarks/loglevel_ab.py (roles: worker / client / orchestrate)",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "jobs_per_run": jobs,
        "runs_per_level": args.runs,
        "runs": runs,
        "median_cpu_ms_per_job": {
            "info": _median("info", "cpu_ms_per_job"),
            "warning": _median("warning", "cpu_ms_per_job"),
        },
        "median_throughput_jps": {
            "info": _median("info", "throughput_jps"),
            "warning": _median("warning", "throughput_jps"),
        },
    }
    info_ms = _median("info", "cpu_ms_per_job")
    warn_ms = _median("warning", "cpu_ms_per_job")
    if info_ms:
        summary["cpu_delta"] = {
            "abs_ms_per_job": round(warn_ms - info_ms, 3),
            "pct_reduction": round(100 * (info_ms - warn_ms) / info_ms, 1),
        }
    profile_attr: dict = {}
    for level in ("info", "warning"):
        p = RESULTS_DIR / f"loglevel-worker-{level}-r0.pstats"
        if p.exists():
            profile_attr[level] = _pstats_structlog_share(str(p))
    if profile_attr:
        summary["cprofile_structlog_share"] = profile_attr
    out = RESULTS_DIR / "loglevel-cpu-delta.json"
    out.write_text(json.dumps(summary, indent=2))
    print(f"[orchestrate] wrote {out}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)

    p = sub.add_parser("worker")
    p.add_argument("--dsn", default=DSN)
    p.add_argument("--schema", default=SCHEMA)
    p.add_argument("--events-level", default="info")
    p.add_argument("--profile-out", default=None)

    c = sub.add_parser("client")
    c.add_argument("--dsn", default=DSN)
    c.add_argument("--schema", default=SCHEMA)
    c.add_argument("--jobs", type=int, default=20000)

    o = sub.add_parser("orchestrate")
    o.add_argument("--dsn", default=DSN)
    o.add_argument("--schema", default=SCHEMA)
    o.add_argument("--jobs", type=int, default=4000)
    o.add_argument("--runs", type=int, default=3)
    o.add_argument("--profile", action="store_true", help="cProfile the first run of each level")

    args = parser.parse_args()
    if args.role == "worker":
        sys.exit(
            run_worker(args.dsn, args.schema, args.events_level, getattr(args, "profile_out", None))
        )
    elif args.role == "client":
        sys.exit(asyncio.run(client_enqueue(args.dsn, args.schema, args.jobs)))
    else:
        sys.exit(asyncio.run(orchestrate(args)))


if __name__ == "__main__":
    main()
