"""The ops-promised latency ladder, measured end to end against a real stack.

Boots a real in-process worker (``worker_main_async``: real dispatch loop,
real LISTEN/NOTIFY wake, real Redis fanout when a Redis URL is given) and a
real ``TaskQ`` client, then walks a concurrency ladder (1 / 4 / 16 by
default) measuring the three latencies ops promises, in interleaved batches
with correctness asserted after every batch:

- **claim-to-start**: enqueue-call start -> the actor's body entry. The
  dispatch wake: run with ``--arm notify`` and ``--arm poll`` and the
  delta between the two tables IS the LISTEN/NOTIFY promise in
  docs/guides/workers.md ("near-zero-latency dispatch wakeups" with
  ``TASKQ_NOTIFY_ENABLED=true`` vs poll-only dispatch at
  ``TASKQ_POLL_INTERVAL``).
- **sse-first-frame**: enqueue-call start -> the first frame of any kind
  on the job's ``JobHandle.progress_stream()`` (opened immediately after
  enqueue returns, the operator's shape). The probe actor publishes
  progress as its first body statement, so on the Redis path that frame
  is the live fanout; with ``--no-redis`` the same metric degrades to
  the documented 500 ms PG-poll fallback, which is the comparison the
  progress docs promise.
- **e2e-completion**: enqueue-call start -> ``handle.wait()`` returns.

Setup follows the integration-tier conventions (tests/conftest.py): the
DSN defaults to the compose Postgres (``postgresql://taskq:taskq@localhost:5432/taskq``,
override with ``--dsn``/``TASKQ_PG_DSN``), a dedicated schema is created
and migrated with ``taskq.migrate.apply_pending`` and dropped on exit.
Correctness is asserted per job, not assumed: every job must succeed,
every body must have started exactly once, and every result must match
the enqueued payload.

Read-only with respect to src/ and the other benchmarks/ files; writes
only its own artifacts under results/.

Usage:
    python benchmarks/latency_ladder.py                        # both arms
    python benchmarks/latency_ladder.py --arm notify           # notify only
    python benchmarks/latency_ladder.py --arm poll             # poll fallback only
    python benchmarks/latency_ladder.py --no-redis             # PG-poll SSE arm
    python benchmarks/latency_ladder.py --ladder 1 4 16 --rounds 5
    python benchmarks/latency_ladder.py --json                 # machine table
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import statistics
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel

from taskq.actor import ActorRef, actor
from taskq.client import TaskQ
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.health import unique_health_sock_path
from taskq.worker import worker_main_async

# ── Configuration ────────────────────────────────────────────────────────

DEFAULT_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
DEFAULT_SCHEMA = "tq_bench_latency_ladder"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

RESULTS_DIR = Path(__file__).parent / "results"

#: The concurrency ladder ops quotes. Each rung reboots the bench worker
#: with that ``TASKQ_MAX_CONCURRENCY`` so the consumer pool and the
#: dispatch claim budget size to exactly the rung.
DEFAULT_LADDER: tuple[int, ...] = (1, 4, 16)

#: Batches per rung. Correctness is asserted between batches; batches are
#: run back-to-back within a rung (the rung's worker boot dominates its
#: wall clock, so within-rung interleaving adds nothing).
DEFAULT_ROUNDS = 4

#: The probe actor's body shape: one progress publish (the SSE frame's
#: source), then a short sleep so the body is observable but the rung
#: stays wake-dominated rather than body-dominated.
PROBE_BODY_S = 0.02


# ── The probe actor ──────────────────────────────────────────────────────


class ProbePayload(BaseModel):
    """Carries the job's index so results can be asserted per job."""

    n: int


class ProbeResult(BaseModel):
    ok: bool
    n: int


#: body-entry stamps, keyed by job id. Written by the actor body and read
#: by the driver on the SAME event loop — no lock needed, and the same
#: loop discipline the worker itself runs under.
_STARTS: dict[UUID, float] = {}
_ENDS: dict[UUID, float] = {}


@actor
async def latency_probe(payload: ProbePayload, ctx: JobContext[ProbePayload]) -> ProbeResult:
    """The measured body: stamp start, publish one progress event, return."""
    _STARTS[ctx.job_id] = time.perf_counter()
    await ctx.progress(step=1, detail="probe-start")
    await asyncio.sleep(PROBE_BODY_S)
    _ENDS[ctx.job_id] = time.perf_counter()
    return ProbeResult(ok=True, n=payload.n)


# ── Percentiles (same convention as benchmarks/e2e_dispatch.py) ─────────


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    xs = sorted(values)
    idx = min(len(xs) - 1, max(0, round(p / 100.0 * (len(xs) - 1))))
    return xs[idx]


# ── One job's measurements ───────────────────────────────────────────────


class JobTimes:
    """The three measured latencies for one job, in perf_counter seconds."""

    __slots__ = ("body_end", "completion", "enqueue_t0", "first_frame", "n", "start")

    def __init__(self, n: int) -> None:
        self.n = n
        self.enqueue_t0: float | None = None
        self.start: float | None = None
        self.body_end: float | None = None
        self.first_frame: float | None = None
        self.completion: float | None = None


async def _pump_progress_stream(
    handle: Any,  # JobHandle[ProbeResult]; untyped to keep the bench import-light
    times: JobTimes,
) -> None:
    """Read the job's progress stream until its terminal frame.

    First frame of any kind sets ``first_frame``: the probe publishes
    progress as its first body statement, so on the Redis path the first
    frame is the live fanout (or the subscription snapshot that raced it
    — both are the consumer's first byte).
    """
    async for event in handle.progress_stream():
        if times.first_frame is None:
            times.first_frame = time.perf_counter()
        if getattr(event, "terminal", False):
            return


async def _run_one_job(
    tq: TaskQ,
    ref: ActorRef[ProbePayload, ProbeResult],
    n: int,
    *,
    stream: bool,
) -> JobTimes:
    """Enqueue, stream (optional), wait: one job's three latencies.

    Correctness is asserted here per job: the result must succeed and
    must carry back the enqueued index.
    """
    times = JobTimes(n=n)
    t0 = time.perf_counter()
    handle: Any = await tq.enqueue(ref, ProbePayload(n=n))
    times.enqueue_t0 = t0

    stream_task = (
        asyncio.create_task(_pump_progress_stream(handle, times), name=f"sse-{handle.job_id}")
        if stream
        else None
    )
    result = await handle.wait(timeout=120.0)
    times.completion = time.perf_counter()
    if stream_task is not None:
        await asyncio.wait_for(stream_task, timeout=10.0)
    # The body stamps _STARTS (same loop, same process); a terminal result
    # whose stamp is missing would mean the bench never observed the start.
    times.start = _STARTS.pop(handle.job_id, None)
    times.body_end = _ENDS.pop(handle.job_id, None)
    if not result.ok or result.n != n:
        raise AssertionError(f"correctness: job n={n} returned {result!r}")
    return times


async def run_batch(
    tq: TaskQ,
    ref: ActorRef[ProbePayload, ProbeResult],
    *,
    count: int,
    stream: bool,
) -> list[JobTimes]:
    """Enqueue *count* jobs back-to-back (one dispatch wave), await all.

    ``count`` equals the rung's concurrency, so the batch fills the
    worker's budget in one wave instead of queueing behind itself.
    """
    tasks = [
        asyncio.create_task(
            _run_one_job(tq, ref, n, stream=stream),
            name=f"job-{n}",
        )
        for n in range(count)
    ]
    return list(await asyncio.gather(*tasks))


# ── Worker lifecycle ─────────────────────────────────────────────────────


async def _wait_worker_registered(
    pg_dsn: str,
    schema: str,
    *,
    budget_secs: float,
) -> None:
    """Poll for the workers-table row ``register_worker`` commits, then a
    short settle so the consumer loops and the notify listener are live
    before the first measured enqueue."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        deadline = time.monotonic() + budget_secs
        while time.monotonic() < deadline:
            n = await pool.fetchval(
                f'select count(*) from "{schema}"."workers"'  # noqa: S608  # Why: the schema name is this file's own generated constant, the same interpolation the migration itself applies.
            )
            if n:
                await asyncio.sleep(0.5)
                return
            await asyncio.sleep(0.1)
        raise TimeoutError(
            f"worker never registered a row in {schema}.workers within {budget_secs}s"
        )
    finally:
        await pool.close()


async def boot_worker(
    pg_dsn: str, schema: str, redis_url: str | None, *, concurrency: int, notify: bool
) -> asyncio.Task[int]:
    """Boot the bench worker on the current loop; returns the worker task."""
    data: dict[str, str] = {
        "TASKQ_PG_DSN": pg_dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_MAX_CONCURRENCY": str(concurrency),
        "TASKQ_HEALTH_ENABLED": "false",
        "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("latency-ladder"),
    }
    if redis_url is not None:
        data["TASKQ_REDIS_URL"] = redis_url
    if not notify:
        data["TASKQ_NOTIFY_ENABLED"] = "false"
    settings = WorkerSettings.load_from_dict(data)
    task = asyncio.create_task(
        worker_main_async(settings, actor_registry={"latency_probe": latency_probe}),
        name="latency-ladder-worker",
    )
    await _wait_worker_registered(pg_dsn, schema, budget_secs=60.0)
    return task


async def stop_worker(task: asyncio.Task[int]) -> None:
    """Cancel the bench worker; a raw cancel is the documented parent-cancel
    path (shutdown stamped, bounded teardown) — fine between batches, when
    every job is already terminal."""
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── Aggregation ──────────────────────────────────────────────────────────


def _dist(vals: list[float]) -> dict[str, float]:
    return {
        "n": len(vals),
        "p50": pct(vals, 50) * 1000,
        "p95": pct(vals, 95) * 1000,
        "p99": pct(vals, 99) * 1000,
        "min": min(vals) * 1000 if vals else float("nan"),
        "mean": (statistics.fmean(vals) * 1000) if vals else float("nan"),
        "max": max(vals) * 1000 if vals else float("nan"),
    }


def _aggregate(rows: list[JobTimes]) -> dict[str, Any]:
    def series(getter: Callable[[JobTimes], float | None]) -> list[float]:
        return [v for r in rows if (v := getter(r)) is not None]

    def from_enqueue(stamp: float | None, t0: float | None) -> float | None:
        return (stamp - t0) if stamp is not None and t0 is not None else None

    starts = series(lambda r: from_enqueue(r.start, r.enqueue_t0))
    bodies = series(lambda r: from_enqueue(r.body_end, r.enqueue_t0))
    frames = series(lambda r: from_enqueue(r.first_frame, r.enqueue_t0))
    comps = series(lambda r: from_enqueue(r.completion, r.enqueue_t0))
    return {
        "jobs": len(rows),
        "claim_to_start_ms": _dist(starts),
        "body_done_ms": _dist(bodies),
        "sse_first_frame_ms": _dist(frames),
        "e2e_completion_ms": _dist(comps),
        "missing_starts": sum(1 for r in rows if r.start is None),
    }


# ── One rung / one arm ───────────────────────────────────────────────────


async def run_rung(
    pg_dsn: str,
    schema: str,
    redis_url: str | None,
    *,
    concurrency: int,
    rounds: int,
    notify: bool,
) -> dict[str, Any]:
    """One ladder rung under one arm: boot a worker sized to the rung,
    run ``rounds`` interleaved batches, stop the worker, aggregate.

    The SSE stream is always opened (it is one of the three measured
    metrics); the redis_url only decides which transport the stream rides
    — Redis fanout, or the documented 500 ms PG-poll fallback.
    """
    worker = await boot_worker(pg_dsn, schema, redis_url, concurrency=concurrency, notify=notify)
    try:
        rows: list[JobTimes] = []
        async with TaskQ(dsn=pg_dsn, schema=schema, redis_url=redis_url) as tq:
            ref: ActorRef[ProbePayload, ProbeResult] = latency_probe
            for _ in range(rounds):
                rows.extend(await run_batch(tq, ref, count=concurrency, stream=True))
        agg = _aggregate(rows)
        # Correctness gate at the batch boundary: every measured job must
        # have started exactly once. A missing start means the worker ran
        # a job the bench never observed - the numbers would be a lie.
        assert agg["missing_starts"] == 0, (
            f"correctness: {agg['missing_starts']} job(s) of rung "
            f"concurrency={concurrency} never stamped a body start"
        )
        return agg
    finally:
        await stop_worker(worker)


async def run_arm(
    pg_dsn: str,
    schema: str,
    redis_url: str | None,
    *,
    ladder: list[int],
    rounds: int,
    notify: bool,
) -> dict[str, Any]:
    """One arm (notify or poll): every rung gets a freshly booted worker."""
    out: dict[str, Any] = {}
    for conc in ladder:
        out[str(conc)] = await run_rung(
            pg_dsn,
            schema,
            redis_url,
            concurrency=conc,
            rounds=rounds,
            notify=notify,
        )
    return out


# ── Reporting ────────────────────────────────────────────────────────────

_METRICS = ("claim_to_start_ms", "body_done_ms", "sse_first_frame_ms", "e2e_completion_ms")


def _fmt_row(arm: str, conc: str, agg: dict[str, Any]) -> str:
    def fmt(d: dict[str, float]) -> str:
        return f"{d['p50']:8.1f} {d['p95']:8.1f} {d['p99']:8.1f}"

    return (
        f"{arm:>6} | {conc:>4} | {fmt(agg['claim_to_start_ms'])} | "
        f"{fmt(agg['body_done_ms'])} | "
        f"{fmt(agg['sse_first_frame_ms'])} | {fmt(agg['e2e_completion_ms'])}"
    )


def print_table(results: dict[str, Any], *, redis: bool) -> None:
    """The report table: one row per (arm, rung), p50/p95/p99 per metric."""
    print(
        f"\n{'arm':>6} | {'conc':>4} | {'claim->start p50/p95/p99 (ms)':^28} | "
        f"{'body done p50/p95/p99 (ms)':^28} | "
        f"{'SSE first frame p50/p95/p99 (ms)':^28} | {'e2e completion p50/p95/p99 (ms)':^28}"
    )
    print("-" * 146)
    for arm, rungs in results.items():
        for conc, agg in rungs.items():
            print(_fmt_row(arm, conc, agg))
    sse_note = "redis fanout" if redis else "500ms PG-poll fallback (no redis)"
    print(
        f"\nSSE first frame arm: {sse_note}. "
        "claim->start under `poll` carries the TASKQ_POLL_INTERVAL floor (1s default); "
        "the notify-vs-poll delta IS the LISTEN/NOTIFY wake promise. "
        "e2e completion rides handle.wait()'s 500ms row-poll cadence "
        "(body done is the actor's real execution span)."
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dsn", default=None, help="Postgres DSN (default TASKQ_PG_DSN or the compose PG)"
    )
    ap.add_argument(
        "--schema", default=DEFAULT_SCHEMA, help="Dedicated bench schema (dropped on exit)"
    )
    ap.add_argument(
        "--redis-url", default=DEFAULT_REDIS_URL, help="Redis URL for the fanout arm; '' disables"
    )
    ap.add_argument(
        "--no-redis",
        action="store_true",
        help="Same as --redis-url '': the SSE metric degrades to the documented 500ms PG-poll fallback",
    )
    ap.add_argument(
        "--ladder", type=int, nargs="+", default=list(DEFAULT_LADDER), help="Concurrency rungs"
    )
    ap.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="Batches per rung")
    ap.add_argument("--arm", choices=["notify", "poll", "both"], default="both")
    ap.add_argument(
        "--json", action="store_true", help="Print the JSON artifact instead of the table"
    )
    args = ap.parse_args()

    dsn = args.dsn or os.environ.get("TASKQ_PG_DSN", DEFAULT_DSN)
    redis_url: str | None = None if args.no_redis else (args.redis_url or None)
    arms = ["notify", "poll"] if args.arm == "both" else [args.arm]

    results: dict[str, Any] = {}
    schema = f"{args.schema}_{datetime.now(UTC).strftime('%H%M%S')}"

    async def drive() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(conn, schema=schema)
        finally:
            await conn.close()
        try:
            for arm in arms:
                results[arm] = await run_arm(
                    dsn,
                    schema,
                    redis_url,
                    ladder=args.ladder,
                    rounds=args.rounds,
                    notify=arm == "notify",
                )
        finally:
            with contextlib.suppress(Exception):
                cleanup = await asyncpg.connect(dsn)
                try:
                    await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                finally:
                    await cleanup.close()

    try:
        asyncio.run(drive())
    finally:
        _STARTS.clear()
        _ENDS.clear()

    artifact = {
        "generated_at": datetime.now(UTC).isoformat(),
        "dsn_host": dsn.split("@")[-1].split("/")[0],
        "redis": redis_url is not None,
        "ladder": args.ladder,
        "rounds": args.rounds,
        "results": results,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / f"latency-ladder-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}.json"
    out_path.write_text(json.dumps(artifact, indent=2))
    if args.json:
        print(json.dumps(artifact, indent=2))
    else:
        print_table(results, redis=redis_url is not None)
    print(f"\nartifact: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
