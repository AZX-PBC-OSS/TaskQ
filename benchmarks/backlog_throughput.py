"""The backlog campaign: the queue's hot paths measured at REAL backlog depth.

The latency ladder (``latency_ladder.py``) and the ops docs' wakeup promises
were measured against empty or toy backlogs. This campaign re-measures the
same quantities with 100k and 1,000,000 PENDING rows in the jobs table — the
queue depths an actual deployment accumulates — and pins the shape of the
curve:

1. **Claim latency vs depth** — the dispatch round (``dispatch_batch``, the
   real claim machinery from ``taskq.backend``) at depth checkpoints 1k /
   10k / 100k / 1M, p50/p95/p99 per checkpoint, at claim widths 1 (the
   latency shape) and 50 (the throughput shape). Depth labels are the
   checkpoint's INITIAL pending count; each checkpoint's rounds consume the
   queue head (each round resets its claimed rows back to pending, so the
   depth the claim sees is exactly the checkpoint's), with claim widths 1
   and 50 — the latency shape and the throughput shape.
2. **Wakeup latency at depth** — enqueue → actor-body-entry, the
   claim-to-start quantity ``latency_ladder.py`` measures, on a REAL worker
   (``worker_main_async``: real LISTEN/NOTIFY wake, real dispatch loop) with
   the depth backlog parked on a queue the probe worker does not serve (no
   actor_config row ⇒ dispatch never claims it). Arms: notify (the default)
   vs poll (``TASKQ_NOTIFY_ENABLED=false``, the documented
   ``TASKQ_POLL_INTERVAL=1s`` fallback), 30 samples each, interleaved
   sample-by-sample so machine drift cancels, at the 100k and 1M
   checkpoints. The claim stamp rides the actor body's
   ``time.perf_counter()`` — the worker runs in THIS process, so the stamps
   share one clock (the latency ladder's own doctrine).
3. **Dequeue throughput ceiling with N workers** — N concurrent dispatch
   loops (1 / 4 / 16) draining a fresh 200k backlog, claim + terminal write
   (``mark_succeeded``), jobs/s per rung. Answers whether the dequeue
   ceiling scales with workers or saturates.

Read-only with respect to src/; writes only its own artifact
``results/backlog-throughput.json``. Starts and tears down its own Postgres
container, labeled with ``creator_labels()`` so a crashed run's leftovers
stay sweepable by the shared-daemon sweep rules. SERIAL: a load-sensitive
campaign — never run concurrent with anything.

Run: .venv/bin/python benchmarks/backlog_throughput.py [--keep]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import (
    uuid4,  # noqa: TID251  # Why: the bench's synthetic worker ids are not TaskQ job ids; locality is irrelevant off the jobs table.
)

import asyncpg
from pydantic import BaseModel

from taskq.actor import actor  # pyright: ignore[reportPrivateUsage]
from taskq.context import JobContext  # pyright: ignore[reportPrivateUsage]

sys.path.insert(0, str(Path(__file__).parent))

from tors_harness import (
    percentile,  # pyright: ignore[reportMissingImports]  # Why: the sys.path bootstrap above is the benchmarks/ convention; pyright's include is src/tests/examples.
)

from taskq._ids import new_job_id  # pyright: ignore[reportPrivateUsage]
from taskq.backend._enqueue import (  # pyright: ignore[reportPrivateUsage]
    _enqueue_batch_fast,  # Why: the private function IS the COPY enqueue path; the seed must ride the production hot path.
)
from taskq.backend._protocol import EnqueueArgs  # pyright: ignore[reportPrivateUsage]
from taskq.backend._sql_templates import render  # pyright: ignore[reportPrivateUsage]
from taskq.backend.clock import SystemClock  # pyright: ignore[reportPrivateUsage]
from taskq.backend.postgres import PostgresBackend  # pyright: ignore[reportPrivateUsage]
from taskq.migrate import apply_pending  # pyright: ignore[reportPrivateUsage]
from taskq.settings import WorkerSettings  # pyright: ignore[reportPrivateUsage]
from taskq.testing._shared_containers import creator_labels  # pyright: ignore[reportPrivateUsage]
from taskq.worker import worker_main_async  # pyright: ignore[reportPrivateUsage]

# ── Configuration ────────────────────────────────────────────────────────

IMAGE = "postgres:18.6"
CONTAINER = "tq-backlog-bench"
PORT = 55443
DSN = f"postgresql://taskq:taskq@localhost:{PORT}/taskq"
SCHEMA = "tq_backlog"

SERVER_FLAGS = ["-c", "max_connections=200", "-c", "shared_buffers=512MB", "-c", "max_wal_size=8GB"]

#: Depth checkpoints: the ladder the campaign pins. 100k is the biggest
#: backlog the repo's prior evidence covered; 1M is the deployment shape
#: the docs never claimed.
DEPTH_CHECKPOINTS = [1_000, 10_000, 100_000, 1_000_000]
SEED_CHUNK = 5_000  # rows per _enqueue_batch_fast call

CLAIM_ROUNDS = 200  # claim rounds per checkpoint, per width
CLAIM_WIDTHS = [1, 50]  # dispatch_batch limit per shape
LOCK_LEASE = timedelta(seconds=30)

#: Wake samples per arm per checkpoint, enqueue → body entry on a real
#: worker. The depth backlog parks on a queue with NO registered actor
#: (dispatch never claims it); probe jobs ride their own queue, so each
#: sample's claim is uncontended by the backlog while the backlog's plan
#: pressure — the thing being measured — is fully present.
WAKE_CHECKPOINTS = [100_000, 1_000_000]
WAKE_SAMPLES = 30
WAKE_SETTLE_S = 0.05

THROUGHPUT_SEED = 200_000
WORKER_LADDER = [1, 4, 16]

DEPTH_ACTOR = "depth_actor"
DEPTH_QUEUE = "depth_q"

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_NAME = "backlog-throughput.json"

_KEEP = False


# ── Container lifecycle ──────────────────────────────────────────────────


def run(cmd: list[str]) -> str:
    # Why noqa: the benchmark's own fixed docker arguments, never user input.
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603  # Why: benchmark-controlled docker CLI invocation.
    return out.stdout.strip()


def start_container() -> None:
    run(["docker", "rm", "-f", CONTAINER])
    # House rule: every container carries creator_labels() ownership labels,
    # so a crashed run's leftovers are sweepable by the same pid-liveness
    # rules the test suite's shared containers follow.
    label_args: list[str] = []
    for k, v in creator_labels().items():
        label_args += ["--label", f"{k}={v}"]
    cmd = [
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
    run(cmd)


def stop_container() -> None:
    # Teardown of the benchmark's own fixed-name container.
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)  # noqa: S603, S607


async def wait_ready(dsn: str, tries: int = 90) -> None:
    last: Exception | None = None
    for _ in range(tries):
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
            return
        except Exception as exc:  # Why: readiness probe, any failure is retryable
            last = exc
            await asyncio.sleep(1)
    raise RuntimeError(f"postgres never became ready: {last}")


# ── Seed: the COPY hot path, chunked ─────────────────────────────────────


async def seed_jobs(pool: asyncpg.Pool, schema: str, count: int, start_index: int) -> float:
    """Bulk-pend *count* rows via ``_enqueue_batch_fast`` (the production
    COPY enqueue path), on the actor/queue the probe worker never serves."""
    templates = render(schema)
    t0 = time.perf_counter()
    done = 0
    while done < count:
        n = min(SEED_CHUNK, count - done)
        args_list = [
            EnqueueArgs(
                id=new_job_id(),
                actor=DEPTH_ACTOR,
                queue=DEPTH_QUEUE,
                payload={"i": start_index + done + k},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(n)
        ]
        # The 1M seed's later chunks ride long-lived pooled connections;
        # this shared box's docker bridges drop them mid-run (measured:
        # InterfaceError, the underlying connection is closed, at the 1M
        # checkpoint's seed). A failed chunk is atomic (one COPY
        # transaction — either it wrote or it rolled back), so retry the
        # chunk on a fresh connection.
        for attempt in range(3):
            try:
                wrote = await _enqueue_batch_fast(pool, templates, schema, args_list)
                break
            except (asyncpg.exceptions.InterfaceError, ConnectionError, OSError) as exc:
                if attempt == 2:
                    raise
                print(f"      seed chunk retry {attempt + 1} after {exc!r}", flush=True)
                await asyncio.sleep(2)
        assert wrote == n, (wrote, n)
        done += n
    return time.perf_counter() - t0


# ── The shim backend (the tests' duck-typed deps shape) ──────────────────


class _DepsShim:
    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool


def make_backend(pg_dsn: str, pool: asyncpg.Pool) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": SCHEMA}, validate=False
    )
    return PostgresBackend(
        _DepsShim(settings, pool),  # type: ignore[arg-type]  # Why: the same duck-typed deps shape the differential harness and tests/test_typed_outcomes_attacks.py use.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def ensure_worker_row(pool: asyncpg.Pool, worker_id: Any) -> None:
    """Register the synthetic bench worker (the workers-table row the
    dispatch/terminal paths assume exists for the driving worker id)."""
    await pool.execute(
        f'INSERT INTO "{SCHEMA}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        "VALUES ($1, 'backlog-bench', $2, $3) ON CONFLICT (id) DO NOTHING",
        worker_id,
        0,
        [DEPTH_QUEUE],
    )


def dist(vals_ms: list[float]) -> dict[str, float]:
    xs = sorted(vals_ms)
    return {
        "n": len(xs),
        "p50_ms": percentile(xs, 0.50),
        "p95_ms": percentile(xs, 0.95),
        "p99_ms": percentile(xs, 0.99),
        "min_ms": xs[0],
        "max_ms": xs[-1],
        "mean_ms": statistics.fmean(xs),
    }


# ── Stage 1: claim latency vs depth ─────────────────────────────────────


async def claim_ladder(pg_dsn: str, checkpoints: list[int]) -> list[dict[str, Any]]:
    pool = await asyncpg.create_pool(pg_dsn, min_size=2, max_size=4)
    backend = make_backend(pg_dsn, pool)
    worker_id = uuid4()
    await ensure_worker_row(pool, worker_id)
    queues = [DEPTH_QUEUE]
    results: list[dict[str, Any]] = []
    try:
        seeded = 0
        for depth in checkpoints:
            t_seed = await seed_jobs(pool, SCHEMA, depth - seeded, seeded)
            seeded = depth
            await pool.execute(f'ANALYZE "{SCHEMA}".jobs')

            checkpoint: dict[str, Any] = {"depth": depth, "seed_s": round(t_seed, 3), "claim": {}}
            for width in CLAIM_WIDTHS:
                samples: list[float] = []
                for round_no in range(CLAIM_ROUNDS):
                    t0 = time.perf_counter()
                    rows = await backend.dispatch_batch(worker_id, queues, width, LOCK_LEASE)
                    dt = (time.perf_counter() - t0) * 1000
                    if not rows:
                        raise AssertionError(
                            f"claim returned 0 rows at depth {depth} round {round_no}"
                        )
                    assert len(rows) <= width
                    samples.append(dt)
                    # Bookkeeping, not measurement: put the claimed rows
                    # BACK on the queue (pending, attempt reset), so the
                    # depth the next round sees is exactly the
                    # checkpoint's — at depth 1k the 200 x 50 rounds
                    # would otherwise drain their own backlog (measured:
                    # claim returned 0 rows at round 16). The claim above
                    # is the measured quantity; the per-job terminal
                    # write is stage 3's business, not this stage's.
                    await pool.execute(  # Why: schema is a benchmark-controlled constant.
                        f"UPDATE \"{SCHEMA}\".jobs SET status = 'pending',"  # noqa: S608
                        " started_at = NULL, finished_at = NULL, attempt = 0,"
                        " locked_by_worker = NULL, lock_expires_at = NULL"
                        " WHERE id = ANY($1)",
                        [r.id for r in rows],
                    )
                checkpoint["claim"][f"limit_{width}"] = dist(samples)
                d = checkpoint["claim"][f"limit_{width}"]
                print(
                    f"    depth {depth:>9,} limit {width:>2}: "
                    f"p50 {d['p50_ms']:8.2f} ms  p95 {d['p95_ms']:8.2f} ms  "
                    f"p99 {d['p99_ms']:8.2f} ms",
                    flush=True,
                )
            checkpoint["note"] = (
                f"depth is the checkpoint's initial pending count; the {CLAIM_ROUNDS} "
                f"rounds per width claim at exactly this depth; each round "
                "resets its claimed rows to pending before the next"
            )
            results.append(checkpoint)
    finally:
        await pool.close()
    return results


# ── Stage 2: poll-vs-NOTIFY wakeup latency at depth ─────────────────────


class WakePayload(BaseModel):
    """The wake probe's payload shape (the actor contract: pydantic)."""

    probe: bool


#: body-entry stamps keyed by job id, written by the module-level probe
#: actor and popped by the driver on the same event loop (the worker runs
#: in THIS process — the latency ladder's shared-clock doctrine). Samples
#: are sequential, one probe job in flight per arm.
_STARTS: dict[Any, float] = {}


@actor
async def wake_probe_notify(payload: WakePayload, ctx: JobContext[WakePayload]) -> dict[str, Any]:
    """The notify arm's probe: the registry key must equal the ActorRef's
    own name, so each arm is its own named actor over the shared body."""
    return await _wake_body(payload, ctx)


@actor
async def wake_probe_poll(payload: WakePayload, ctx: JobContext[WakePayload]) -> dict[str, Any]:
    """The poll arm's probe (``TASKQ_NOTIFY_ENABLED=false`` on its worker)."""
    return await _wake_body(payload, ctx)


async def _wake_body(payload: WakePayload, ctx: JobContext[WakePayload]) -> dict[str, Any]:
    _STARTS[ctx.job_id] = time.perf_counter()
    return {"ok": True}


class _WakeProbe:
    """One real worker (``worker_main_async``), notify or poll arm.

    Each arm gets its OWN (actor, queue) pair (``wake_probe_notify`` /
    ``wake_probe_poll`` on separate queues) and registers ONLY that actor:
    the arms run interleaved sample by sample, and disjoint queues are what
    keeps one arm's worker from claiming the other's probe jobs. The depth
    backlog (actor ``depth_actor``) has no registered actor on either
    worker, so its dispatch never claims it — the probe's claim is
    uncontended while the backlog's plan pressure is fully present.
    """

    def __init__(self, pg_dsn: str, *, notify: bool) -> None:
        self.arm = "notify" if notify else "poll"
        self.actor_name = f"wake_probe_{self.arm}"
        self.queue = f"wake_q_{self.arm}"
        data: dict[str, str] = {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": SCHEMA,
            # The worker's dispatch claims only its OWN queue set; without
            # this the worker registers queues=['default'] and never
            # claims the wake probes (measured: the soak's worker sat
            # pending on exactly this).
            "TASKQ_QUEUES": self.queue,
            "TASKQ_MAX_CONCURRENCY": "4",
            "TASKQ_HEALTH_ENABLED": "false",
        }
        if not notify:
            data["TASKQ_NOTIFY_ENABLED"] = "false"
        self.settings = WorkerSettings.load_from_dict(data)
        self.task: asyncio.Task[int] | None = None
        self.pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        actor = wake_probe_notify if self.arm == "notify" else wake_probe_poll
        self.pool = await asyncpg.create_pool(self.settings.pg_dsn, min_size=1, max_size=2)
        self.task = asyncio.create_task(
            worker_main_async(self.settings, actor_registry={self.actor_name: actor}),
            name=f"backlog-wake-worker-{self.arm}",
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            # Match by the arm's queue: the synthetic bench worker's row
            # (seeded by ensure_worker_row) would make a bare count useless.
            n = await self.pool.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}"."workers" WHERE $1 = ANY(queues)',  # noqa: S608  # Why: the schema name is this file's own constant.
                self.queue,
            )
            if n:
                await asyncio.sleep(0.5)
                return
            await asyncio.sleep(0.1)
        raise TimeoutError("wake worker never registered")

    async def sample(self, pool: asyncpg.Pool, templates: Any) -> tuple[float, Any]:
        """One enqueue → body-entry sample; returns (ms, job_id)."""
        job_id = new_job_id()
        args = EnqueueArgs(
            id=job_id,
            actor=self.actor_name,
            queue=self.queue,
            payload={"probe": True},
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=None,
        )
        t0 = time.perf_counter()
        _STARTS.pop(job_id, None)
        wrote = await _enqueue_batch_fast(pool, templates, SCHEMA, [args])
        assert wrote == 1
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status = await pool.fetchval(
                f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a benchmark-controlled constant.
                job_id,
            )
            if status == "succeeded":
                break
            await asyncio.sleep(0.002)
        else:
            raise TimeoutError("probe job never succeeded")
        start = _STARTS.pop(job_id, 0.0)
        assert start > 0, "actor body never stamped its start"
        return (start - t0) * 1000, job_id

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            with contextlib.suppress(BaseException):
                await self.task  # Why: teardown, the documented cancel path.
        if self.pool:
            await self.pool.close()


async def wake_arms(pg_dsn: str, checkpoints: list[int]) -> list[dict[str, Any]]:
    """Poll-vs-NOTIFY at each wake checkpoint (the depth backlog in place)."""
    templates = render(SCHEMA)
    seed_pool = await asyncpg.create_pool(pg_dsn, min_size=2, max_size=4)
    results: list[dict[str, Any]] = []
    try:
        seeded = 0
        for depth in checkpoints:
            t_seed = 0.0
            if seeded < depth:
                t_seed = await seed_jobs(seed_pool, SCHEMA, depth - seeded, seeded)
                seeded = depth
                await seed_pool.execute(f'ANALYZE "{SCHEMA}".jobs')

            # Both arms booted together, samples interleaved arm by arm so
            # machine drift cancels — the A/B harness's own doctrine.
            arms: dict[str, list[float]] = {"notify": [], "poll": []}
            workers = {
                arm: _WakeProbe(pg_dsn, notify=(arm == "notify")) for arm in ("notify", "poll")
            }
            for arm in ("notify", "poll"):
                await workers[arm].start()
            probe_ids: dict[str, list[Any]] = {"notify": [], "poll": []}
            probe_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
            try:
                for _ in range(WAKE_SAMPLES):
                    for arm in ("notify", "poll"):
                        dt, job_id = await workers[arm].sample(probe_pool, templates)
                        probe_ids[arm].append(job_id)
                        arms[arm].append(dt)
                        await asyncio.sleep(WAKE_SETTLE_S)
            finally:
                await probe_pool.close()
                for arm in ("notify", "poll"):
                    await workers[arm].stop()

            # Correctness before the timings are trusted: every probe job
            # this checkpoint enqueued must have run exactly once (attempts
            # capped at 1, so no silent double-run).
            conn = await asyncpg.connect(pg_dsn)
            try:
                this_checkpoint = [j for ids in probe_ids.values() for j in ids]
                assert len(this_checkpoint) == 2 * WAKE_SAMPLES
                ran = await conn.fetchval(
                    f'SELECT count(*) FROM "{SCHEMA}".jobs '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
                    "WHERE id = ANY($1) AND status = 'succeeded'",
                    this_checkpoint,
                )
                assert ran == len(this_checkpoint), (ran, len(this_checkpoint))
            finally:
                await conn.close()

            row = {
                "depth": depth,
                "seed_s_for_depth": round(t_seed, 3),
                "samples": WAKE_SAMPLES,
                "notify": dist(arms["notify"]),
                "poll": dist(arms["poll"]),
                "poll_minus_notify_p50_ms": dist(arms["poll"])["p50_ms"]
                - dist(arms["notify"])["p50_ms"],
            }
            results.append(row)
            print(
                f"    depth {depth:>9,}: notify p50 {row['notify']['p50_ms']:7.2f} ms  "
                f"poll p50 {row['poll']['p50_ms']:7.2f} ms  "
                f"(poll - notify = {row['poll_minus_notify_p50_ms']:,.0f} ms)",
                flush=True,
            )
    finally:
        await seed_pool.close()
    return results


# ── Stage 3: dequeue throughput ceiling with N workers ──────────────────


async def throughput_ladder(pg_dsn: str, backlog: int) -> list[dict[str, Any]]:
    pool = await asyncpg.create_pool(pg_dsn, min_size=2, max_size=8)
    results: list[dict[str, Any]] = []
    try:
        for n_workers in WORKER_LADDER:
            # Clean slate per rung: drop the queue's prior rows (terminal
            # and pending) so no rung inherits another's backlog, then
            # seed fresh.
            await pool.execute(  # Why: schema is a benchmark-controlled constant.
                f'DELETE FROM "{SCHEMA}".jobs WHERE queue = $1',  # noqa: S608
                DEPTH_QUEUE,
            )
            t_seed = await seed_jobs(pool, SCHEMA, backlog, 10_000_000)
            await pool.execute(f'ANALYZE "{SCHEMA}".jobs')

            async def drain_one() -> int:
                backend = make_backend(pg_dsn, pool)
                worker_id = uuid4()
                await ensure_worker_row(pool, worker_id)
                drained = 0
                while True:
                    rows = await backend.dispatch_batch(worker_id, [DEPTH_QUEUE], 50, LOCK_LEASE)
                    if not rows:
                        return drained
                    for r in rows:
                        ok = await backend.mark_succeeded(
                            r.id, worker_id, attempt=r.attempt, claim_epoch=r.claim_epoch
                        )
                        assert ok, r.id
                    drained += len(rows)

            t0 = time.perf_counter()
            got = await asyncio.gather(*[drain_one() for _ in range(n_workers)])
            wall = time.perf_counter() - t0
            total = sum(got)
            assert total == backlog, (total, backlog)
            row = {
                "workers": n_workers,
                "backlog": backlog,
                "wall_s": round(wall, 3),
                "jobs_per_s": total / wall,
                "jobs_per_s_per_worker": total / wall / n_workers,
                "claimed": got,
                "seed_s": round(t_seed, 3),
            }
            results.append(row)
            print(
                f"    workers {n_workers:>2}: {row['jobs_per_s']:10,.0f} jobs/s "
                f"({row['jobs_per_s_per_worker']:,.0f} per worker), wall {wall:.1f}s",
                flush=True,
            )
    finally:
        await pool.close()
    return results


# ── Main ─────────────────────────────────────────────────────────────────


async def main() -> None:
    global _KEEP, CLAIM_ROUNDS, WAKE_SAMPLES, THROUGHPUT_SEED
    ap = argparse.ArgumentParser(description="Backlog-depth campaign benchmark")
    ap.add_argument("--depths", type=str, default=",".join(str(d) for d in DEPTH_CHECKPOINTS))
    ap.add_argument("--claim-rounds", type=int, default=CLAIM_ROUNDS)
    ap.add_argument("--wake-samples", type=int, default=WAKE_SAMPLES)
    ap.add_argument("--throughput-seed", type=int, default=THROUGHPUT_SEED)
    ap.add_argument("--keep", action="store_true", help="keep the container after the run")
    args = ap.parse_args()
    _KEEP = args.keep
    CLAIM_ROUNDS = args.claim_rounds
    WAKE_SAMPLES = args.wake_samples
    THROUGHPUT_SEED = args.throughput_seed
    depths = [int(d) for d in args.depths.split(",")]

    t_start = time.perf_counter()
    print(f"backlog campaign: depths {depths}; container {CONTAINER}:{PORT}", flush=True)

    print("[1/5] container + schema", flush=True)
    start_container()
    await wait_ready(DSN)
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await apply_pending(conn, schema=SCHEMA)
        # Each wake arm's (actor, queue) pair: the arms run interleaved on
        # disjoint queues, so neither worker can claim the other's probes.
        await conn.execute(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            "ON CONFLICT DO NOTHING",
            "wake_probe_notify",
            "wake_q_notify",
        )
        await conn.execute(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT DO NOTHING",
            "wake_probe_poll",
            "wake_q_poll",
        )
        # The depth backlog's actor needs a registered (actor, queue) row:
        # dispatch's capacity chain enumerates REGISTERED actors only, so
        # without it the claim admits nothing (measured: 0 rows claimed).
        # The wake worker still never touches this queue — its dispatch
        # rides its OWN registry's queues, and 'depth_actor' is not in it.
        await conn.execute(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT DO NOTHING",
            DEPTH_ACTOR,
            DEPTH_QUEUE,
        )
    finally:
        await conn.close()

    print("[2/5] claim latency vs depth", flush=True)
    claim = await claim_ladder(DSN, depths)

    print("[3/5] wake arms (notify vs poll) at depth", flush=True)
    wake_checkpoints = [d for d in WAKE_CHECKPOINTS if d in depths]
    wake = await wake_arms(DSN, wake_checkpoints)

    print("[4/5] dequeue throughput ladder", flush=True)
    throughput = await throughput_ladder(DSN, THROUGHPUT_SEED)

    print("[5/5] teardown + results", flush=True)
    if not _KEEP:
        stop_container()

    results = {
        "schema": 1,
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": {
            "image": IMAGE,
            "container": f"{CONTAINER}:{PORT}",
            "server_flags": SERVER_FLAGS,
            "depth_checkpoints": depths,
            "claim_rounds_per_checkpoint": CLAIM_ROUNDS,
            "claim_widths": CLAIM_WIDTHS,
            "wake_checkpoints": wake_checkpoints,
            "wake_samples_per_arm": WAKE_SAMPLES,
            "throughput_backlog": THROUGHPUT_SEED,
            "worker_ladder": WORKER_LADDER,
            "seed_path": "_enqueue_batch_fast (the production COPY enqueue path)",
            "python": sys.version.split()[0],
            "wall_s": time.perf_counter() - t_start,
        },
        "claim_latency_vs_depth": claim,
        "wake_latency_vs_depth": wake,
        "throughput_ladder": throughput,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / RESULTS_NAME
    path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"\nresults → {path}")
    print(f"(wall {time.perf_counter() - t_start:.0f}s)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if not _KEEP:
            stop_container()
