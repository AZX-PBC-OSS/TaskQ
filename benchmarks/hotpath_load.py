"""Hot-path load harness: a real TaskQ worker under sustained load, for
cProfile / py-spy / tracemalloc profiling of the worker's hot paths.

Roles (separate processes, orchestrated by ``orchestrate``):

    python benchmarks/hotpath_load.py worker --dsn ... --schema ...
    python benchmarks/hotpath_load.py client --dsn ... --schema ... --jobs 20000
    python benchmarks/hotpath_load.py orchestrate --dsn ... --schema ... --jobs 20000

The worker role is TaskQ's REAL worker bootstrap (``worker_main`` — same
entry point as ``examples/worker.py``) running N registered actors on one
queue, optionally wrapped in cProfile (``--profile`` writes a
``cProfile`` dump into results/) and with tracemalloc job-count-targeted
snapshots (``--tracemalloc-jobs K``: the actor body counts completed
jobs; at K and 2K the process dumps a tracemalloc snapshot so the
diff / K is the bytes+objects per job).

The client role enqueues continuously (enqueue_batch_fast, batches of
100) until ``--jobs`` are accepted, then exits — the worker keeps
draining.

The orchestrate role owns the schema lifecycle, launches the worker,
samples pg_locks / pg_stat_activity wait events during the drain,
reads pg_stat_statements deltas for statement counts, measures the
idle-loop query rate (no enqueues → dispatch statements/10 s), and
writes ``benchmarks/results/hotpath-profile.json``.

Usage:
    python benchmarks/hotpath_load.py orchestrate [--jobs 20000] [--workers 1]
        [--profile cprofile,py-spy]
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
SCHEMA = "tq_bench_hotpath"
QUEUE = "hotpath"
RESULTS_DIR = _BENCH_DIR / "results"
LOG_DIR = RESULTS_DIR / "hotpath_logs"
N_ACTORS = 8

# ── the actors (module level: the decorator registers real ActorRefs) ──

import asyncpg  # noqa: E402  # Why: the --slot-pool shape's LOOP-scope factory annotation must resolve at get_type_hints time.
from pydantic import BaseModel  # noqa: E402

from taskq import actor  # noqa: E402
from taskq._ids import (  # noqa: E402
    new_job_id,  # Why: scratch benchmark rows, PK locality is not the variable under test.
)


class HotPayload(BaseModel):
    n: int = 0


_JOB_COUNTER = {"done": 0}


def _tracemalloc_checkpoint() -> None:
    """Snapshot hook the worker role installs for --tracemalloc-jobs."""
    raise NotImplementedError  # replaced at runtime by run_worker


@actor(name="hot_noop_0", queue=QUEUE)
async def hot_noop_0(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_1", queue=QUEUE)
async def hot_noop_1(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_2", queue=QUEUE)
async def hot_noop_2(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_3", queue=QUEUE)
async def hot_noop_3(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_4", queue=QUEUE)
async def hot_noop_4(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_5", queue=QUEUE)
async def hot_noop_5(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_6", queue=QUEUE)
async def hot_noop_6(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


@actor(name="hot_noop_7", queue=QUEUE)
async def hot_noop_7(payload: HotPayload) -> None:
    _JOB_COUNTER["done"] += 1
    if _CHECK_EVERY and _JOB_COUNTER["done"] % _CHECK_EVERY == 0:
        _tracemalloc_checkpoint()


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

# tracemalloc checkpoints every K jobs (0 = off; set by run_worker).
_CHECK_EVERY = 0
_TRACEMALLOC_STATE = {"snap0": None, "jobs0": 0, "out": None}


def _real_checkpoint() -> None:
    """Dump a cumulative tracemalloc snapshot at the current job count."""
    import tracemalloc

    if not tracemalloc.is_tracing():
        return
    snap = tracemalloc.take_snapshot()
    jobs = _JOB_COUNTER["done"]
    out = _TRACEMALLOC_STATE["out"]
    if out is None:
        return
    if _TRACEMALLOC_STATE["snap0"] is None:
        _TRACEMALLOC_STATE["snap0"] = snap
        _TRACEMALLOC_STATE["jobs0"] = jobs
        return
    base = _TRACEMALLOC_STATE["snap0"]
    jobs0 = _TRACEMALLOC_STATE["jobs0"]
    n = max(1, jobs - jobs0)
    total = sum(s.size_diff for s in snap.compare_to(base, "lineno"))
    count = sum(s.count_diff for s in snap.compare_to(base, "lineno"))
    top = [
        {"lineno": str(s), "size_diff": s.size_diff, "count_diff": s.count_diff}
        for s in snap.compare_to(base, "lineno")[:25]
    ]
    payload = {
        "jobs_measured": n,
        "bytes_per_job": round(total / n, 1),
        "objects_per_job": round(count / n, 1),
        "top_diffs": top,
    }
    Path(str(out)).write_text(json.dumps(payload, indent=2))
    print(f"[tracemalloc] {n} jobs -> {total / n:.0f} B/job, {count / n:.0f} obj/job")


_tracemalloc_checkpoint = (
    _real_checkpoint  # Why: install the real hook now that the module body is defined.
)

# ── worker role ───────────────────────────────────────────────────────


def run_worker(
    dsn: str, schema: str, profile_out: str | None, tracemalloc_jobs: int, slot_pool: bool
) -> int:
    """Real TaskQ worker bootstrap: settings -> worker_main (blocking)."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import worker_main

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_MAX_CONCURRENCY": "8",
            "TASKQ_QUEUES": QUEUE,
        }
    )
    di_registry = None
    if slot_pool:
        # The production transactional-consume shape: a LOOP-scope
        # asyncpg.Connection registration is what activates the worker's
        # per-slot transaction pool (bootstrap's _maybe_open_slot_pool),
        # so each job dispatches on its own connection/transaction.
        import asyncpg

        from taskq._di.registry import ProviderRegistry
        from taskq._di.scope import Scope

        async def _loop_conn() -> asyncpg.Connection:
            return await asyncpg.connect(dsn)

        di_registry = ProviderRegistry()
        di_registry.register_factory(asyncpg.Connection, Scope.LOOP, _loop_conn)
    code: int
    prof = None
    if profile_out:
        import cProfile

        prof = cProfile.Profile()
        prof.enable()
    if tracemalloc_jobs:
        import tracemalloc

        globals()["_CHECK_EVERY"] = tracemalloc_jobs
        _TRACEMALLOC_STATE["out"] = str(
            Path(profile_out or "benchmarks/results/hotpath-worker.pstats").parent
            / "hotpath-tracemalloc.json"
        )
        tracemalloc.start()
    try:
        code = worker_main(settings, actor_registry=ACTORS, di_registry=di_registry)
    finally:
        if prof is not None:
            prof.disable()
            prof.dump_stats(profile_out)
            print(f"[cProfile] dumped to {profile_out}")
        with contextlib.suppress(ValueError):  # Why: not the main thread -> no window to guard.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return code


# ── client role ───────────────────────────────────────────────────────


async def _migrate(dsn: str, schema: str) -> None:
    from taskq.migrate import apply_pending_locked

    await apply_pending_locked(dsn, schema=schema)


async def client_enqueue(dsn: str, schema: str, jobs: int) -> dict:
    """Continuous enqueue at the backend layer: EnqueueArgs carry NO batch
    metadata, so the worker drains the common non-batch path (the shape
    the hot path is tuned for). Batches of 100 keep the client's own
    overhead off the worker's critical path."""
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
    enqueue_s = time.perf_counter() - t0
    return {"enqueued": enqueued, "enqueue_s": round(enqueue_s, 3)}


# ── orchestrate role ─────────────────────────────────────────────────


async def _pg(dsn: str) -> asyncpg.pool.Pool:
    import asyncpg

    return await asyncpg.create_pool(dsn, min_size=1, max_size=2)


async def sample_contention(pool, seconds: float, every: float = 0.25) -> dict:
    """Sample pg_locks ungranted + pg_stat_activity wait events during load."""
    samples = 0
    ungranted_max = 0
    wait_events: dict[str, int] = {}
    active_max = 0
    deadline = time.perf_counter() + seconds
    while time.perf_counter() < deadline:
        rows = await pool.fetch(
            """
            SELECT a.wait_event_type || ':' || coalesce(a.wait_event, '?') AS we,
                    count(*)::int AS n
            FROM pg_stat_activity a
            WHERE a.datname = current_database() AND a.pid <> pg_backend_pid()
            GROUP BY 1
            """
        )
        active = 0
        for r in rows:
            active += r["n"]
            if r["we"] != "-:":  # running
                wait_events[r["we"]] = wait_events.get(r["we"], 0) + r["n"]
        active_max = max(active_max, active)
        ung = await pool.fetchval(
            "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype <> 'virtualxid'"
        )
        ungranted_max = max(ungranted_max, int(ung or 0))
        samples += 1
        await asyncio.sleep(every)
    return {
        "samples": samples,
        "ungranted_locks_max": ungranted_max,
        "active_conns_max": active_max,
        "wait_events": wait_events,
    }


async def stmt_by_query(pool) -> dict[str, dict]:
    rows = await pool.fetch(
        """
        SELECT query, calls, rows, round(total_exec_time::numeric, 1) AS t_ms
        FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND query NOT LIKE '%pg_stat%'
        """
    )
    return {
        " ".join(r["query"].split())[:120]: {
            "calls": r["calls"],
            "rows": r["rows"],
            "t_ms": float(r["t_ms"]),
        }
        for r in rows
    }


async def measure_idle_query_rate(dsn: str, seconds: float = 15.0) -> dict:
    """Run a REAL worker on an empty queue and count its DB statements/second.

    This is the polling-waste number: with NOTIFY on, the fallback poll is
    5 s, so an idle worker should be near-silent between heartbeats.
    """

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__)),
        "worker",
        "--dsn",
        dsn,
        "--schema",
        SCHEMA,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    await asyncio.sleep(3.0)  # bootstrap
    pool = await _pg(dsn)
    try:
        await pool.execute("SELECT pg_stat_statements_reset()")
        await asyncio.sleep(seconds)
        stmts = await stmt_by_query(pool)
    finally:
        await pool.close()
        proc.kill()
        await proc.wait()

    total_calls = sum(v["calls"] for v in stmts.values())
    top = sorted(stmts.items(), key=lambda kv: -kv[1]["calls"])[:10]
    return {
        "idle_seconds": seconds,
        "total_statements": total_calls,
        "statements_per_second": round(total_calls / seconds, 2),
        "top": [{"query": q, **v} for q, v in top],
    }


async def orchestrate(args) -> dict:
    dsn, schema, jobs = args.dsn, args.schema, args.jobs
    out_json = RESULTS_DIR / "hotpath-profile.json"

    proc_pg = await _pg(dsn)
    await proc_pg.execute(
        f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
    )  # Why: benchmark-owned schema
    await proc_pg.execute(f'CREATE SCHEMA "{schema}"')  # Why: benchmark-owned schema
    await _migrate(dsn, schema)
    await proc_pg.execute("SELECT pg_stat_statements_reset()")
    await proc_pg.close()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    profile_out = str(RESULTS_DIR / "hotpath-worker.pstats")

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
        stdout=open(LOG_DIR / "worker.log", "wb"),  # noqa: SIM115 ASYNC230  # Why: the handle must stay open for the subprocess's lifetime; one blocking open at spawn, never in the loop.
        stderr=subprocess.STDOUT,
    )
    print(f"[orchestrate] worker pid={worker.pid}")
    await asyncio.sleep(4.0)

    # py-spy sample the worker for args.py_spy_seconds while load runs.
    client = asyncio.create_task(client_enqueue(dsn, schema, jobs))
    t0 = time.perf_counter()
    py_spy = None
    if args.py_spy_seconds > 0:
        await asyncio.sleep(2.0)
        pyspy_out = str(RESULTS_DIR / "hotpath-pyspy.svg")
        py_spy = await asyncio.create_subprocess_exec(
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
        try:
            _, err = await asyncio.wait_for(py_spy.communicate(), timeout=args.py_spy_seconds + 60)
            if py_spy.returncode != 0:
                print(f"[py-spy] rc={py_spy.returncode}: {err.decode()[:400]}")
        except TimeoutError:
            py_spy.kill()

    enq = await client
    print(f"[orchestrate] enqueued {enq}")

    # Watch until drained, sampling contention meanwhile.
    pool = await _pg(dsn)
    deadline = time.perf_counter() + 600
    drained = False
    while time.perf_counter() < deadline:
        n = await pool.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"  # noqa: S608
        )
        if n == 0:
            drained = True
            break
        await asyncio.sleep(0.5)
    drain_s = time.perf_counter() - t0
    contention = await sample_contention(pool, 0.1)  # one sample for the record
    stmts = await stmt_by_query(pool)
    await pool.close()

    worker.terminate()
    await worker.wait()

    idle = await measure_idle_query_rate(dsn) if not args.skip_idle else {}

    result = {
        "jobs": jobs,
        "enqueued": enq,
        "drained": drained,
        "drain_seconds": round(drain_s, 2),
        "throughput_jps": round(jobs / drain_s, 1) if drain_s else None,
        "contention": contention,
        "idle_query_rate": idle,
        "statements_top": sorted(
            ({"query": q, **v} for q, v in stmts.items()),
            key=lambda r: -r["calls"],
        )[:25],
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "dsn_schema": schema,
    }
    out_json.write_text(json.dumps(result, indent=2))
    print(f"[orchestrate] wrote {out_json}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)

    p = sub.add_parser("worker")
    p.add_argument("--dsn", default=DSN)
    p.add_argument("--schema", default=SCHEMA)
    p.add_argument("--profile-out", default=None)
    p.add_argument("--tracemalloc-jobs", type=int, default=0)
    p.add_argument("--slot-pool", action="store_true")

    c = sub.add_parser("client")
    c.add_argument("--dsn", default=DSN)
    c.add_argument("--schema", default=SCHEMA)
    c.add_argument("--jobs", type=int, default=20000)

    o = sub.add_parser("orchestrate")
    o.add_argument("--dsn", default=DSN)
    o.add_argument("--schema", default=SCHEMA)
    o.add_argument("--jobs", type=int, default=20000)
    o.add_argument("--py-spy-seconds", type=float, default=20.0)
    o.add_argument("--tracemalloc-jobs", type=int, default=3000)
    o.add_argument("--skip-idle", action="store_true")

    args = parser.parse_args()
    if args.role == "worker":
        sys.exit(
            run_worker(
                args.dsn,
                args.schema,
                args.profile_out,
                args.tracemalloc_jobs,
                getattr(args, "slot_pool", False),
            )
        )
    elif args.role == "client":
        sys.exit(asyncio.run(client_enqueue(args.dsn, args.schema, args.jobs)))
    else:
        sys.exit(asyncio.run(orchestrate(args)))


if __name__ == "__main__":
    main()
