"""SPIKE B — multi-worker contention: N real worker PROCESSES on one shared PG.

Never tested before: everything upstream was single-process. This spike runs
TaskQ's REAL worker bootstrap (``worker_main`` from ``taskq.worker._bootstrap``
— same entry point as ``examples/worker.py``) in N separate python processes
(max_concurrency=4 each) against the shared Postgres, then drains 5000 jobs
from a client process and measures:

  - aggregate throughput scaling (1 worker vs 4 workers)
  - dispatch CTE contention: pg_stat_activity wait events + ungranted
    pg_locks sampled at 250 ms during the drain
  - leader behaviour: who holds ``maintenance_leader`` (sampled), and
    non-leader workers' CPU (``ps -o cputime`` deltas — they should be
    mostly asleep)
  - per-actor fairness HOL: optional round_robin phase compares per-actor
    completion skew across workers vs strict_fifo

Roles (separate processes, orchestrated by ``orchestrate``):

    python benchmarks/multi_worker_spike.py worker   --dsn ... --schema ...
    python benchmarks/multi_worker_spike.py client   --dsn ... --schema ... --phase enqueue --jobs 5000
    python benchmarks/multi_worker_spike.py client   --dsn ... --schema ... --phase watch --jobs 5000 --pids 1,2,3,4
    python benchmarks/multi_worker_spike.py orchestrate --dsn ... --schema ... --workers 1,4 [--fairness]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import subprocess
import sys
import time
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_BENCH_DIR.parent))  # taskq importable from the worktree

DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
SCHEMA = "spike_mw"
LOG_DIR = _BENCH_DIR / "results" / "mw_logs"

# ── the actor (module level: the decorator registers real ActorRefs) ──

from pydantic import BaseModel  # noqa: E402

from taskq import actor  # noqa: E402


class SpikePayload(BaseModel):
    n: int = 0


@actor(name="spike_noop_0", queue="spike")
async def spike_noop_0(payload: SpikePayload) -> None:
    return


@actor(name="spike_noop_1", queue="spike")
async def spike_noop_1(payload: SpikePayload) -> None:
    return


@actor(name="spike_noop_2", queue="spike")
async def spike_noop_2(payload: SpikePayload) -> None:
    return


@actor(name="spike_noop_3", queue="spike")
async def spike_noop_3(payload: SpikePayload) -> None:
    return


ACTORS = {
    "spike_noop_0": spike_noop_0,
    "spike_noop_1": spike_noop_1,
    "spike_noop_2": spike_noop_2,
    "spike_noop_3": spike_noop_3,
}

# ── worker role ───────────────────────────────────────────────────────


def run_worker(dsn: str, schema: str) -> int:
    """Real TaskQ worker bootstrap: settings -> worker_main (blocking)."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import worker_main

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_MAX_CONCURRENCY": "4",
            "TASKQ_QUEUES": "spike",
        }
    )
    return worker_main(settings, actor_registry=ACTORS)


# ── client roles ──────────────────────────────────────────────────────


async def _truncate(schema: str, dsn: str = DSN) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        for table in ("maintenance_leader", "job_events", "jobs", "batches", "workers"):
            await conn.execute(f'DELETE FROM "{schema}".{table}')  # noqa: S608
    finally:
        await conn.close()


async def _set_queue_mode(schema: str, mode: str, dsn: str = DSN) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"INSERT INTO \"{schema}\".queues (name, mode) VALUES ('spike', $1) "  # noqa: S608
            "ON CONFLICT (name) DO UPDATE SET mode = $1",
            mode,
        )
    finally:
        await conn.close()


async def _migrate(dsn: str, schema: str) -> None:
    from taskq.migrate import apply_pending_locked

    await apply_pending_locked(dsn, schema=schema)


async def client_enqueue(dsn: str, schema: str, jobs: int, actor_names: list[str]) -> dict:
    from taskq import TaskQ
    from taskq.batch import EnqueueItem

    items: list[EnqueueItem] = []
    for i in range(jobs):
        ref = ACTORS[actor_names[i % len(actor_names)]]
        items.append(EnqueueItem(actor_ref=ref, payload=SpikePayload(n=i)))
    t0 = time.perf_counter()
    async with TaskQ(dsn=dsn, schema=schema) as tq:
        count = await tq.enqueue_batch_fast(items)
    enqueue_s = time.perf_counter() - t0
    assert count == jobs, (count, jobs)
    return {"enqueued": count, "enqueue_s": round(enqueue_s, 3)}


def _parse_cputime(s: str) -> float:
    """'MM:SS.ss' | 'HH:MM:SS.ss' | 'SS' -> seconds."""
    s = s.strip()
    parts = s.split(":")
    total = 0.0
    for part in parts:
        total = total * 60 + (float(part) if part else 0.0)
    return total


async def client_watch(
    dsn: str,
    schema: str,
    jobs: int,
    worker_pids: list[int],
    track_actors: bool,
) -> dict:
    """Poll completion + contention + leader + CPU until all jobs terminal."""
    import asyncpg

    actors = list(ACTORS) if track_actors else ["spike_noop_0"]
    conn = await asyncpg.connect(dsn)
    t0 = time.perf_counter()
    done = 0
    timeseries: list[dict] = []
    wait_events: dict[str, int] = {}
    max_active = 0
    max_dispatch_active = 0
    max_ungranted_locks = 0
    leader_ids: set[str] = set()
    leader_samples = 0
    per_actor_done: dict[str, int] = dict.fromkeys(actors, 0)
    per_actor_done_at: dict[str, float] = {}

    cpu_first: dict[int, float] = {}
    cpu_last: dict[int, float] = {}
    next_cpu = 0.0

    while time.perf_counter() - t0 < 300:
        loop_t = time.perf_counter()
        row = await conn.fetchrow(
            f"""
            SELECT count(*) FILTER (WHERE status IN ('succeeded','failed','crashed','cancelled','abandoned')) AS done,
                   count(*) FILTER (WHERE status = 'running') AS running,
                   count(*) FILTER (WHERE status = 'pending') AS pending
            FROM "{schema}".jobs
            """  # noqa: S608  # Why: schema is a script-controlled constant; the only caller data is $N-bound.
        )
        done = row["done"]
        timeseries.append(
            {
                "t": round(time.perf_counter() - t0, 3),
                "done": done,
                "running": row["running"],
                "pending": row["pending"],
            }
        )
        if track_actors:
            rows = await conn.fetch(
                f"""
                SELECT actor, count(*) AS d FROM "{schema}".jobs
                WHERE status IN ('succeeded','failed','crashed','cancelled','abandoned')
                GROUP BY actor
                """  # noqa: S608  # Why: schema is a script-controlled constant; the only caller data is $N-bound.
            )
            for r in rows:
                a = r["actor"]
                if per_actor_done[a] < r["d"]:
                    per_actor_done[a] = r["d"]
                    per_actor_done_at.setdefault(a, round(time.perf_counter() - t0, 3))

        acts = await conn.fetch(
            """
            SELECT wait_event_type, wait_event, count(*) AS n
            FROM pg_stat_activity
            WHERE datname = current_database() AND state = 'active'
            GROUP BY 1, 2
            """
        )
        max_active = max(max_active, sum(r["n"] for r in acts))
        for r in acts:
            key = f"{r['wait_event_type'] or 'CPU'}:{r['wait_event'] or '-'}"
            wait_events[key] = max(wait_events.get(key, 0), r["n"])
            if r["wait_event_type"] == "Lock":
                max_ungranted_locks = max(max_ungranted_locks, r["n"])
        dispatch_active = await conn.fetchval(
            f"""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database() AND state = 'active'
              AND query ILIKE '%FROM "{schema}".jobs%'
            """  # noqa: S608  # Why: schema is a script-controlled constant; the pattern matches our own queries.
        )
        max_dispatch_active = max(max_dispatch_active, dispatch_active or 0)

        leader = await conn.fetchval(f'SELECT worker_id FROM "{schema}".maintenance_leader')  # noqa: S608
        if leader:
            leader_ids.add(str(leader))
            leader_samples += 1

        if loop_t >= next_cpu:
            next_cpu = loop_t + 2.0
            if worker_pids:
                proc = await asyncio.to_thread(
                    subprocess.run,
                    ["ps", "-o", "cputime=", *[f"-p {p}" for p in worker_pids]],
                    capture_output=True,
                    text=True,
                )
                vals = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
                if len(vals) == len(worker_pids):
                    for pid, raw in zip(worker_pids, vals, strict=False):
                        secs = _parse_cputime(raw)
                        cpu_first.setdefault(pid, secs)
                        cpu_last[pid] = secs

        if done >= jobs:
            break
        await asyncio.sleep(0.1)

    drain_s = time.perf_counter() - t0
    await conn.close()

    cpu_seconds = {
        str(p): round(cpu_last.get(p, 0.0) - cpu_first.get(p, 0.0), 2) for p in worker_pids
    }
    return {
        "jobs": jobs,
        "drained": done,
        "drain_s": round(drain_s, 3),
        "throughput_jps": round(done / drain_s, 1) if drain_s > 0 else None,
        "timeseries_every_10th": timeseries[::10],
        "wait_events_max_concurrent": dict(sorted(wait_events.items(), key=lambda kv: -kv[1])),
        "max_active_backends": max_active,
        "max_dispatch_active": max_dispatch_active,
        "max_ungranted_locks": max_ungranted_locks,
        "leader_ids": sorted(leader_ids),
        "leader_samples": leader_samples,
        "cpu_seconds_per_worker_pid": cpu_seconds,
        **({"per_actor_done_at_s": per_actor_done_at} if track_actors else {}),
    }


def run_client(args: argparse.Namespace) -> int:
    if args.phase == "enqueue":
        names = list(ACTORS)[: args.actors] if args.actors > 1 else ["spike_noop_0"]
        result = asyncio.run(client_enqueue(args.dsn, args.schema, args.jobs, names))
    else:
        result = asyncio.run(
            client_watch(
                args.dsn,
                args.schema,
                args.jobs,
                args.pids,
                track_actors=args.fairness or args.actors > 1,
            )
        )
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2))
    else:
        print(json.dumps(result, indent=2))
    return 0


# ── orchestrate role ──────────────────────────────────────────────────


def spawn_worker(dsn: str, schema: str, n: int, idx: int) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / f"worker-n{n}-{idx}.log", "w")  # noqa: SIM115
    return subprocess.Popen(  # noqa: S603  # Why: dev spike; spawns this script itself via sys.executable.
        [sys.executable, str(Path(__file__).resolve()), "worker", "--dsn", dsn, "--schema", schema],
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def wait_for_registrations(dsn: str, schema: str, expected: int, timeout_s: float = 20.0) -> None:
    import asyncpg

    async def _wait() -> None:
        conn = await asyncpg.connect(dsn)
        deadline = time.monotonic() + timeout_s
        try:
            while time.monotonic() < deadline:
                n = await conn.fetchval(f'SELECT count(*) FROM "{schema}".workers')  # noqa: S608
                if n >= expected:
                    return
                await asyncio.sleep(0.25)
            raise TimeoutError(f"only {n}/{expected} workers registered in {timeout_s}s")
        finally:
            await conn.close()

    asyncio.run(_wait())


def wait_for_workers_idle_then_terminate(
    procs: list[subprocess.Popen], timeout_s: float
) -> list[int]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and any(p.poll() is None for p in procs):
        time.sleep(0.2)
    codes = [p.poll() for p in procs]
    for p, code in zip(procs, codes, strict=False):
        if code is None:  # still running: graceful TERM, then KILL
            p.send_signal(signal.SIGTERM)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and any(p.poll() is None for p in procs):
        time.sleep(0.2)
    for p in procs:
        if p.poll() is None:
            p.kill()
    return [p.poll() for p in procs]


def run_one_config(
    dsn: str,
    schema: str,
    n: int,
    jobs: int,
    fairness: bool,
    queue_mode: str = "strict_fifo",
    n_actors: int = 1,
) -> dict:
    print(
        f"--- config: {n} worker(s), {jobs} jobs, fairness={fairness}, "
        f"queue_mode={queue_mode}, actors={n_actors} ---"
    )
    asyncio.run(_truncate(schema))
    if queue_mode != "strict_fifo":
        asyncio.run(_set_queue_mode(schema, queue_mode))

    procs = [spawn_worker(dsn, schema, n, i) for i in range(n)]
    time.sleep(0.5)
    wait_for_registrations(dsn, schema, n)
    worker_pids = [p.pid for p in procs]

    out_dir = _BENCH_DIR / "results"
    out_dir.mkdir(exist_ok=True)
    enqueue_json = out_dir / f"mw-enqueue-n{n}.json"
    watch_json = out_dir / f"mw-watch-n{n}.json"

    common = ["--dsn", dsn, "--schema", schema, "--jobs", str(jobs), "--actors", str(n_actors)]
    if fairness:
        common.append("--fairness")
    r1 = subprocess.run(  # noqa: S603  # Why: dev spike; spawns this script itself via sys.executable.
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "client",
            "--phase",
            "enqueue",
            *common,
            "--out",
            str(enqueue_json),
        ],
        capture_output=True,
        text=True,
    )
    if r1.returncode != 0:
        raise RuntimeError(f"enqueue failed: {r1.stdout}\n{r1.stderr}")
    t_enqueue_done = time.time()

    r2 = subprocess.run(  # noqa: S603  # Why: dev spike; spawns this script itself via sys.executable.
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "client",
            "--phase",
            "watch",
            *common,
            "--pids",
            ",".join(map(str, worker_pids)),
            "--out",
            str(watch_json),
        ],
        capture_output=True,
        text=True,
    )
    if r2.returncode != 0:
        raise RuntimeError(f"watch failed: {r2.stdout}\n{r2.stderr}")

    codes = wait_for_workers_idle_then_terminate(procs, timeout_s=60)

    enqueue = json.loads(enqueue_json.read_text())
    watch = json.loads(watch_json.read_text())
    # throughput from enqueue-done to all-terminal (excludes client enqueue)
    jps_excl_enqueue = round(jobs / max(watch["drain_s"], 1e-9), 1)
    result = {
        "workers": n,
        "jobs": jobs,
        "fairness": fairness,
        "queue_mode": queue_mode,
        "n_actors": n_actors,
        "enqueue_s": enqueue["enqueue_s"],
        "drain_s": watch["drain_s"],
        "throughput_jps_from_watch_start": jps_excl_enqueue,
        "worker_exit_codes": codes,
        **watch,
        "t_enqueue_done_epoch": t_enqueue_done,
    }
    ts = time.strftime("%Y%m%d-%H%M%S")
    tag = (
        f"-{queue_mode}-{n_actors}actors"
        if (fairness or n_actors > 1 or queue_mode != "strict_fifo")
        else ""
    )
    path = out_dir / f"multiworker-{ts}-n{n}{tag}.json"
    path.write_text(json.dumps(result, indent=2))
    print(
        f"  drain {watch['drain_s']}s  -> {jps_excl_enqueue} jobs/s  "
        f"(leader distinct: {len(watch['leader_ids'])}, max lock waits: {watch['max_ungranted_locks']})"
    )
    print(f"  wrote {path}")
    return result


def run_orchestrate(args: argparse.Namespace) -> int:
    asyncio.run(_migrate(args.dsn, args.schema))
    results = []
    for n in args.workers:
        results.append(
            run_one_config(
                args.dsn,
                args.schema,
                n,
                args.jobs,
                args.fairness,
                queue_mode=args.queue_mode,
                n_actors=args.actors,
            )
        )
    base = next((r for r in results if r["workers"] == 1), None)
    print("\n== scaling ==")
    for r in results:
        ratio = ""
        if base and base["throughput_jps_from_watch_start"]:
            ratio = f"  ({r['throughput_jps_from_watch_start'] / base['throughput_jps_from_watch_start']:.2f}x single)"
        print(f"  {r['workers']} workers: {r['throughput_jps_from_watch_start']} jobs/s{ratio}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)

    p = sub.add_parser("worker")
    p.add_argument("--dsn", default=DSN)
    p.add_argument("--schema", default=SCHEMA)

    c = sub.add_parser("client")
    c.add_argument("--dsn", default=DSN)
    c.add_argument("--schema", default=SCHEMA)
    c.add_argument("--phase", choices=["enqueue", "watch"], required=True)
    c.add_argument("--jobs", type=int, default=5000)
    c.add_argument("--pids", default="")
    c.add_argument("--fairness", action="store_true")
    c.add_argument("--actors", type=int, default=1)
    c.add_argument("--out", default=None)

    o = sub.add_parser("orchestrate")
    o.add_argument("--dsn", default=DSN)
    o.add_argument("--schema", default=SCHEMA)
    o.add_argument("--workers", default="1,4")
    o.add_argument("--jobs", type=int, default=5000)
    o.add_argument("--fairness", action="store_true")
    o.add_argument("--queue-mode", choices=["strict_fifo", "round_robin"], default="strict_fifo")
    o.add_argument("--actors", type=int, default=1)

    args = parser.parse_args()
    if args.role == "worker":
        sys.exit(run_worker(args.dsn, args.schema))
    elif args.role == "client":
        args.pids = [int(x) for x in args.pids.split(",") if x]
        sys.exit(run_client(args))
    else:
        args.workers = [int(x) for x in args.workers.split(",")]
        sys.exit(run_orchestrate(args))


if __name__ == "__main__":
    main()
