"""The memory soak, compressed time: a real worker drains ~50k jobs while
its RSS is sampled; the slope is pinned as a benchmark artifact.

The ops question: does the dispatch loop's resident set grow with the
jobs it has processed (a leak — a cache keyed by job id, an accumulation
in an OTel span buffer, a context registry nothing drains) or does it
hold flat? At toy scale nothing shows; at 50k jobs a 1 KiB/job leak is
50 MB of slope. This soak runs the REAL worker (``worker_main_async``:
real dispatch loop, real LISTEN/NOTIFY, real terminal writes) over 50k
trivial jobs and samples this process's VmRSS (from ``/proc/self/status``
— the kernel's own accounting, no sampler overhead on the loop) every
``SAMPLE_EVERY`` completed jobs. The worker runs on the caller's event
loop, so the process the soak samples IS the worker's process.

The pinned quantity is the least-squares RSS slope over the samples,
bytes per job, plus the peak. "Flat-or-sublinear" is the artifact's
claim to make, with the arithmetic stated: the soak's own threshold is
``SLOPE_BOUND_BYTES_PER_JOB`` (2 KiB/job — at 50k jobs that is ≤100 MB of
growth, the envelope a bounded loop may legitimately warm up inside:
pool growth, caches with real eviction, JIT/code warm); a slope beyond
the bound means retention proportional to jobs processed. This run's
verdict is recorded in the artifact; the artifact is the pin (there is
deliberately NO CI test asserting another machine's RSS: the number is
load- and allocator-sensitive, and the campaign's contract is that the
MEASURED slope is committed and traceable, per the house rule).

Correctness is asserted before the slope is trusted: all 50k jobs must
be claimed exactly once and reach succeeded (attempts capped at 1).

Read-only with respect to src/; writes only its own artifact
``results/backlog-rss-soak.json``. Container labeled with
``creator_labels()``. SERIAL: load-sensitive.

Run: .venv/bin/python benchmarks/backlog_rss_soak.py [--jobs 50000]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.actor import actor  # pyright: ignore[reportPrivateUsage]

sys.path.insert(0, str(Path(__file__).parent))

from taskq._ids import new_job_id  # pyright: ignore[reportPrivateUsage]
from taskq.backend._enqueue import (  # pyright: ignore[reportPrivateUsage]
    _enqueue_batch_fast,  # Why: the private function IS the COPY enqueue path; the seed must ride the production hot path.
)
from taskq.backend._protocol import EnqueueArgs  # pyright: ignore[reportPrivateUsage]
from taskq.backend._sql_templates import render  # pyright: ignore[reportPrivateUsage]
from taskq.migrate import apply_pending  # pyright: ignore[reportPrivateUsage]
from taskq.settings import WorkerSettings  # pyright: ignore[reportPrivateUsage]
from taskq.testing._shared_containers import creator_labels  # pyright: ignore[reportPrivateUsage]
from taskq.worker import worker_main_async  # pyright: ignore[reportPrivateUsage]

IMAGE = "postgres:18.6"
CONTAINER = "tq-rss-soak"
PORT = 55731
DSN = f"postgresql://taskq:taskq@localhost:{PORT}/taskq"
SCHEMA = "tq_rss_soak"

SERVER_FLAGS = ["-c", "max_connections=100", "-c", "shared_buffers=256MB"]

SOAK_JOBS = 50_000
ENQUEUE_CHUNK = 5_000
SAMPLE_EVERY = 500
SLOPE_BOUND_BYTES_PER_JOB = 2 * 1024  # 2 KiB/job: ≤100 MB over the 50k soak

ACTOR = "soak_probe"
QUEUE = "soak_q"

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_NAME = "backlog-rss-soak.json"

_KEEP = False

# The soak's samples and counter live at module level: the actor body (a
# module-level def — get_type_hints cannot resolve function-local classes,
# so the handler and its payload model must be module-scope) writes them
# and the driver reads them, one event loop, no lock.
SAMPLES: list[tuple[int, int]] = []  # (jobs succeeded, VmRSS KiB)
_COUNT: dict[str, int] = {"done": 0}


class SoakPayload(BaseModel):
    """The soak job's payload shape (the actor contract: pydantic)."""

    i: int


@actor
async def soak_probe(payload: SoakPayload) -> dict[str, Any]:
    """The soaked body: count, sample the RSS on the cadence, return."""
    _COUNT["done"] += 1
    if _COUNT["done"] % SAMPLE_EVERY == 0:
        SAMPLES.append((_COUNT["done"], read_rss_kib()))
    return {"ok": True}


def run(cmd: list[str]) -> str:
    # Why noqa: the benchmark's own fixed docker arguments, never user input.
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603  # Why: benchmark-controlled docker CLI invocation.
    return out.stdout.strip()


def start_container() -> None:
    run(["docker", "rm", "-f", CONTAINER])
    label_args: list[str] = []
    for k, v in creator_labels().items():
        label_args += ["--label", f"{k}={v}"]
    run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER,
            *label_args,
            "-e",
            "POSTGRES_USER=taskq",
            "-e",
            "POSTGRES_PASSWORD=taskq",
            "-e",
            "POSTGRES_DB=taskq",
            "-p",
            f"{PORT}:5432",
            IMAGE,
            "postgres",
            *SERVER_FLAGS,
        ]
    )


def stop_container() -> None:
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)  # noqa: S603, S607  # Why: benchmark teardown of its own fixed-name container.


def read_rss_kib() -> int:
    """VmRSS from /proc/self/status — the kernel's own accounting."""
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("VmRSS not found in /proc/self/status")


def slope_per_job(samples: list[tuple[int, int]]) -> float:
    """Least-squares slope of RSS (KiB) against jobs completed, in
    bytes per job."""
    xs = [x for x, _ in samples]
    ys = [y for _, y in samples]
    mx = statistics.fmean(xs)
    my = statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
    if sxx == 0:
        return 0.0
    return (sxy / sxx) * 1024  # KiB/job → bytes/job


async def main() -> None:
    global _KEEP, SOAK_JOBS
    ap = argparse.ArgumentParser(description="Worker-loop RSS soak")
    ap.add_argument("--jobs", type=int, default=SOAK_JOBS)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    _KEEP = args.keep
    SOAK_JOBS = args.jobs

    t_start = time.perf_counter()
    start_container()
    for _ in range(90):
        try:
            conn = await asyncpg.connect(DSN)
            await conn.close()
            break
        except Exception:  # Why: readiness probe
            await asyncio.sleep(1)
    else:
        raise RuntimeError("postgres never became ready")

    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await apply_pending(conn, schema=SCHEMA)
        await conn.execute(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            "ON CONFLICT DO NOTHING",
            ACTOR,
            QUEUE,
        )
    finally:
        await conn.close()

    # Enqueue the whole soak corpus up front (COPY hot path), then boot
    # the real worker with until_idle: it drains and exits on its own.
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    templates = render(SCHEMA)
    print(f"seeding {SOAK_JOBS:,} jobs", flush=True)
    seeded_ids: list[Any] = []
    done = 0
    while done < SOAK_JOBS:
        n = min(ENQUEUE_CHUNK, SOAK_JOBS - done)
        args_list = [
            EnqueueArgs(
                id=new_job_id(),
                actor=ACTOR,
                queue=QUEUE,
                payload={"i": done + k},
                max_attempts=1,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(n)
        ]
        wrote = await _enqueue_batch_fast(pool, templates, SCHEMA, args_list)
        assert wrote == n
        seeded_ids.extend(a.id for a in args_list)
        done += n

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": DSN,
            "TASKQ_SCHEMA_NAME": SCHEMA,
            "TASKQ_QUEUES": QUEUE,
            "TASKQ_MAX_CONCURRENCY": "4",
            "TASKQ_HEALTH_ENABLED": "false",
        }
    )

    samples = SAMPLES
    _count = _COUNT
    _baseline = read_rss_kib()

    print(f"worker draining (baseline RSS {_baseline:,} KiB)", flush=True)
    t0 = time.perf_counter()
    worker_task = asyncio.create_task(
        worker_main_async(settings, actor_registry={ACTOR: soak_probe}, until_idle=True),
        name="rss-soak-worker",
    )
    await worker_task
    wall = time.perf_counter() - t0
    print(f"drained in {wall:.1f}s", flush=True)
    await pool.close()

    # Correctness before the slope is trusted: every seeded job reached
    # succeeded (attempts capped at 1, so exactly-once is enforced).
    conn = await asyncpg.connect(DSN)
    try:
        succeeded = await conn.fetchval(
            f"SELECT count(*) FROM \"{SCHEMA}\".jobs WHERE status = 'succeeded'"  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        )
        assert succeeded == SOAK_JOBS, (succeeded, SOAK_JOBS)
    finally:
        await conn.close()

    slope = slope_per_job(samples)
    peak_kib = max(y for _, y in samples) if samples else _baseline
    growth_mb = (peak_kib - _baseline) / 1024
    verdict = (
        "flat-or-sublinear" if slope <= SLOPE_BOUND_BYTES_PER_JOB else "LEAK-SHAPED (over bound)"
    )
    print(
        f"RSS: baseline {_baseline:,} KiB → peak {peak_kib:,} KiB (+{growth_mb:,.1f} MB); "
        f"slope {slope:,.0f} bytes/job over {len(samples)} samples — {verdict}",
        flush=True,
    )

    if not _KEEP:
        stop_container()

    results = {
        "schema": 1,
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": {
            "image": IMAGE,
            "container": f"{CONTAINER}:{PORT}",
            "jobs": SOAK_JOBS,
            "concurrency": 4,
            "sample_every": SAMPLE_EVERY,
            "samples": len(samples),
            "worker": "worker_main_async(until_idle=True): the real dispatch loop",
            "rss_source": "/proc/self/status VmRSS (the worker process)",
            "slope_bound_bytes_per_job": SLOPE_BOUND_BYTES_PER_JOB,
            "bound_arithmetic": (
                f"2 KiB/job x {SOAK_JOBS:,} jobs = {2 * SOAK_JOBS / 1024:,.0f} MB growth "
                "envelope: pool/cache warm-up for a bounded loop; a slope beyond it is "
                "retention proportional to jobs processed"
            ),
            "python": sys.version.split()[0],
            "wall_s": time.perf_counter() - t_start,
        },
        "drain_wall_s": wall,
        "rss_baseline_kib": _baseline,
        "rss_peak_kib": peak_kib,
        "rss_growth_mb": growth_mb,
        "slope_bytes_per_job": slope,
        "verdict": verdict,
        "samples": samples,
        "all_jobs_succeeded": True,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / RESULTS_NAME
    path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"\nresults → {path}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if not _KEEP:
            stop_container()
