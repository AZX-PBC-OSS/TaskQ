"""Round-trip hot-path measurement against a real Postgres (perf-hunt rig).

Measures the hot paths on the current main, per the perf-hotspot campaign:
enqueue single/batch, the drain round trip (dispatch_batch + per-job
mark_succeeded), the heartbeat tick (lease renewal + the cancel ladder's
run_in_tx with a real connection and a full registry), backend.get, and the
admin pages' queries. Every op is timed individually; medians of three
interleaved runs are reported per op.

Setup follows the integration-tier conventions (tests/conftest.py and
benchmarks/e2e_dispatch.py): a dedicated schema is created and migrated with
taskq.migrate.apply_pending and truncated at the end.

Usage:
    python benchmarks/perf_hunt_pg.py --dsn postgresql://taskq:taskq@localhost:55431/taskq
    python benchmarks/perf_hunt_pg.py --dsn ... --profile dispatch   # cProfile the drain loop
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import truncate_schema
from taskq.worker.cancel import _ActiveJob
from taskq.worker.deps import WorkerDeps

DEFAULT_DSN = "postgresql://taskq:taskq@localhost:55431/taskq"
DEFAULT_SCHEMA = "tq_bench_hunt"
ACTOR = "bench_actor"
QUEUE = "default"
N_JOBS = 3000
BATCH = 50
RUNNING_FLOOR = 50  # keep this many rows locked to the worker for the heartbeat ticks

RESULTS_DIR = Path(__file__).parent / "results"


def make_payload(i: int, pad: int = 96) -> dict[str, object]:
    return {
        "order_id": f"ord-{i:08d}",
        "channel": ["web", "ios", "android"][i % 3],
        "customer": {"id": f"cus-{i % 997:05d}", "tier": ["free", "pro", "ent"][i % 3]},
        "items": [{"sku": f"SKU-{(i + k) % 50:03d}", "qty": (i + k) % 5 + 1} for k in range(3)],
        "notes": f"benchmark payload #{i} " + "x" * pad,
        "flags": {"expedited": i % 7 == 0, "gift": i % 11 == 0},
    }


async def setup_schema(admin_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(admin_dsn)
    try:
        await conn.execute(
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )  # Why: benchmark-controlled identifier, conftest convention
        await conn.execute(f'CREATE SCHEMA "{schema}"')  # Why: benchmark-controlled identifier
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608  # Why: schema is a benchmark-controlled identifier, values are bound parameters
            ACTOR,
            QUEUE,
        )
    finally:
        await conn.close()


async def cleanup_schema(admin_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(admin_dsn)
    try:
        await truncate_schema(conn, schema)
    finally:
        await conn.close()


async def build_backend_async(
    dsn: str, schema: str
) -> tuple[PostgresBackend, WorkerDeps, asyncpg.Pool]:
    """Real WorkerDeps + PostgresBackend over one asyncpg pool (worker shape)."""
    settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema})
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    assert pool is not None
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
    return backend, deps, pool


class Times:
    """Per-op samples across interleaved runs."""

    def __init__(self) -> None:
        self.samples: dict[str, list[float]] = {}

    def add(self, name: str, dt: float) -> None:
        self.samples.setdefault(name, []).append(dt)

    def medians(self) -> dict[str, float]:
        return {k: statistics.median(v) for k, v in self.items()}

    def items(self):  # type: ignore[no-untyped-def]
        return self.samples.items()


async def measure(
    backend: PostgresBackend,
    deps: WorkerDeps,
    pool: asyncpg.Pool,
    runs: int,
) -> Times:
    times = Times()
    worker_id = new_uuid()
    lock_lease = timedelta(seconds=30)

    if runs > 1:
        # Warmup: one full round, untimed (pool dial-in, prepared statements).
        await measure(backend, deps, pool, 1)

    # The cancel controller over the real deps: its poll reads the real
    # jobs table. Registered entries are planted directly (idle entries:
    # no cancel flags anywhere in the table), so run_in_tx pays the fetch
    # plus the full held walk over the registry, the heartbeat tick's
    # exact cancel-hook shape at the pinned fleet size.
    from taskq.worker.cancel import ActiveJobRegistry

    registry = ActiveJobRegistry()
    loop = asyncio.get_running_loop()
    fake_entries: list[_ActiveJob] = []
    for _ in range(RUNNING_FLOOR):
        task = loop.create_task(asyncio.Event().wait())  # type: ignore[arg-type]
        entry = _ActiveJob(job_id=new_uuid(), task=task, ctx=None)  # type: ignore[arg-type]
        registry._by_id[entry.job_id] = entry  # pyright: ignore[reportPrivateUsage]
        fake_entries.append(entry)
    deps.active_jobs = registry  # type: ignore[assignment]  # Why: the bench plants its own registry so the ladder's walk is measured at fleet size.
    from taskq.worker.cancel import _CancelController

    controller = _CancelController(deps, worker_id, backend)  # type: ignore[arg-type]

    async def heartbeat_real_conn() -> None:
        async with pool.acquire() as conn, conn.transaction():
            await controller.run_in_tx(conn)

    enqueued = 0
    for _run in range(runs):
        # enqueue single
        for _ in range(10):
            args = EnqueueArgs(
                id=new_job_id(),
                actor=ACTOR,
                queue=QUEUE,
                payload=make_payload(enqueued),
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            t0 = time.perf_counter()
            await backend.enqueue(args)
            times.add("enqueue_single", time.perf_counter() - t0)
            enqueued += 1

        # enqueue batch[50]
        chunk = [
            EnqueueArgs(
                id=new_job_id(),
                actor=ACTOR,
                queue=QUEUE,
                payload=make_payload(enqueued + k),
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(BATCH)
        ]
        t0 = time.perf_counter()
        await backend.enqueue_batch(chunk)
        times.add("enqueue_batch[50]", time.perf_counter() - t0)
        enqueued += BATCH

        # drain round trip: dispatch a capped batch, then complete each job,
        # timing dispatch_batch, backend.get and mark_succeeded per call.
        t0 = time.perf_counter()
        rows = await backend.dispatch_batch(worker_id, [QUEUE], BATCH, lock_lease)
        times.add("dispatch_batch[50, capped]", time.perf_counter() - t0)

        gets = 0
        for row in rows:
            if gets < 10:
                t0 = time.perf_counter()
                await backend.get(row.id)
                times.add("backend.get", time.perf_counter() - t0)
                gets += 1
            t0 = time.perf_counter()
            await backend.mark_succeeded(
                row.id,
                worker_id,
                None,
                attempt=row.attempt,
                claim_epoch=row.claim_epoch,
            )
            times.add("mark_succeeded", time.perf_counter() - t0)

        # heartbeat tick (lease renewal) + the cancel hook, with the
        # registry at fleet size and a real connection.
        t0 = time.perf_counter()
        await backend.heartbeat_jobs(worker_id, lock_lease)
        times.add("heartbeat_jobs[tick]", time.perf_counter() - t0)

        t0 = time.perf_counter()
        await heartbeat_real_conn()
        times.add("cancel_ladder.run_in_tx[50 entries, real conn]", time.perf_counter() - t0)

    for entry in fake_entries:
        entry.task.cancel()

    return times


async def measure_admin(
    pool: asyncpg.Pool,
    schema: str,
    runs: int,
) -> Times:
    """The admin pages' queries against the drained (terminal) rows."""
    times = Times()
    from taskq.web.admin import history as admin_history
    from taskq.web.admin.jobs import (
        _ALL_STATUSES,  # pyright: ignore[reportPrivateUsage]
        _LIVE_COLS,  # pyright: ignore[reportPrivateUsage]
        _SORTABLE_LIVE,  # pyright: ignore[reportPrivateUsage]
        _build_paginated_sql,  # pyright: ignore[reportPrivateUsage]
        _build_where,  # pyright: ignore[reportPrivateUsage]
    )

    statuses = sorted(_ALL_STATUSES)
    where, params = _build_where(statuses, None, None, None, None, None, None, None)
    # The admin seams moved between the perf-proof point and main (the
    # /history keyset seam landed in between); a shape the checked-out tree
    # does not have is skipped, not fatal.
    import contextlib

    for _run in range(runs):
        query_sql, query_params = _build_paginated_sql(
            schema,
            "jobs",
            _LIVE_COLS,
            _SORTABLE_LIVE,
            where,
            params,
            None,
            None,
            "next",
            "",
            "desc",
        )
        t0 = time.perf_counter()
        await pool.fetch(query_sql, *query_params)
        times.add("admin.jobs_list page[50]", time.perf_counter() - t0)

        count_sql = f'SELECT COUNT(*) FROM "{schema}".jobs WHERE {where}'  # noqa: S608  # Why: mirrors the route's assembled count statement
        t0 = time.perf_counter()
        await pool.fetchval(count_sql, *params)
        times.add("admin.jobs_count", time.perf_counter() - t0)

        with contextlib.suppress(AttributeError):
            t0 = time.perf_counter()
            await pool.fetch(
                admin_history._history_list_sql(schema, cursor=False, limit=50),  # pyright: ignore[reportPrivateUsage]
                statuses,
                None,
                None,
            )
            times.add("admin.history page[50, union]", time.perf_counter() - t0)

        with contextlib.suppress(AttributeError):
            t0 = time.perf_counter()
            await pool.fetch(
                admin_history._SUMMARY_SQL.format(schema=schema),  # pyright: ignore[reportPrivateUsage]
                statuses,
                None,
                None,
            )
            times.add("admin.history_summary", time.perf_counter() - t0)

    return times


async def profile_drain(
    backend: PostgresBackend,
    deps: WorkerDeps,
    n_jobs: int,
) -> str:
    """cProfile the worker-shaped drain loop under load."""
    import cProfile
    import io as _io
    import pstats

    worker_id = new_uuid()
    lock_lease = timedelta(seconds=30)
    enqueued = 0

    def args_for(k: int) -> EnqueueArgs:
        return EnqueueArgs(
            id=new_job_id(),
            actor=ACTOR,
            queue=QUEUE,
            payload=make_payload(k),
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )

    profiler = cProfile.Profile()
    profiler.enable()
    while enqueued < n_jobs:
        chunk = [args_for(enqueued + k) for k in range(BATCH)]
        await backend.enqueue_batch(chunk)
        enqueued += BATCH
        rows = await backend.dispatch_batch(worker_id, [QUEUE], BATCH, lock_lease)
        await backend.heartbeat_jobs(worker_id, lock_lease)
        for row in rows:
            await backend.get(row.id)
            await backend.mark_succeeded(
                row.id, worker_id, None, attempt=row.attempt, claim_epoch=row.claim_epoch
            )
    profiler.disable()
    s = _io.StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.sort_stats("cumulative").print_stats(28)
    return s.getvalue()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--jobs", type=int, default=N_JOBS)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--profile", choices=["none", "dispatch"], default="none")
    args = parser.parse_args()

    admin_dsn = args.dsn
    await setup_schema(admin_dsn, args.schema)
    try:
        backend, deps, pool = await build_backend_async(args.dsn, args.schema)

        if args.profile == "dispatch":
            out = await profile_drain(backend, deps, args.jobs)
            print(out)
            return

        times = await measure(backend, deps, pool, args.runs)
        # Leave the terminal rows for the admin shapes, then measure them.
        admin_times = await measure_admin(pool, args.schema, max(args.runs, 5))
        for k, v in admin_times.items():
            times.samples[k] = v

        print(f"\n[perf_hunt_pg] dsn={args.dsn} schema={args.schema} runs={args.runs}")
        print(f"\n{'op':<48} {'median':>12}")
        print("-" * 62)
        for name, samples in times.items():
            med = statistics.median(samples) * 1000
            spread = (
                f" (spread {(max(samples) - min(samples)) / statistics.median(samples):.0%})"
                if len(samples) >= 3
                else ""
            )
            print(f"{name:<48} {med:>10.3f}ms{spread}")

        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        out = RESULTS_DIR / f"perf-hunt-{stamp}.json"
        RESULTS_DIR.mkdir(exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "dsn": args.dsn,
                    "schema": args.schema,
                    "runs": args.runs,
                    "medians_ms": {k: statistics.median(v) * 1000 for k, v in times.items()},
                    "samples": dict(times.items()),
                },
                indent=2,
            )
        )
        print(f"\n[perf_hunt_pg] wrote {out}")
    finally:
        await cleanup_schema(admin_dsn, args.schema)


if __name__ == "__main__":
    asyncio.run(main())
