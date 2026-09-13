"""End-to-end dispatch benchmark against a real Postgres.

Closes the E2E profiling gap left by the micro-benchmarks in
bench_hotspots.py: this runs the real PostgresBackend + asyncpg pool
against a live Postgres (docker-compose.yml's taskq-postgres) and reports

- the CPU-vs-I/O wall-clock split of a worker-shaped dispatch loop
  (enqueue N jobs -> dispatch_batch in a loop -> mark_succeeded each ->
  list_jobs pages -> one full sweep pass),
- per-job latency (enqueue -> claimed) percentiles,
- EXPLAIN (ANALYZE, BUFFERS) of the dispatch CTE, the terminal UPDATE,
  and the sweep statements (``--mode explain``),
- COPY fast path (enqueue_batch_fast) vs regular batch INSERT
  (enqueue_batch) rows/sec at 1k and 10k items (``--mode copy``),
- sweep round-trip overhead: the statement_timeout apply/restore pair
  plus a full no-op sweep pass (``--mode sweep``).

Setup follows the integration-tier conventions (tests/conftest.py): the
DSN defaults to the compose Postgres (``postgresql://taskq:taskq@localhost:5432/taskq``,
override with ``--dsn``/``TASKQ_PG_DSN``), a dedicated schema is created
and migrated with ``taskq.migrate.apply_pending``, and the dynamic tables
are truncated at the end via ``taskq.testing.pg.truncate_schema``.

Read-only with respect to src/ and the other benchmarks/ files; writes
only its own artifacts under results/.

Usage:
    python benchmarks/e2e_dispatch.py                       # e2e loop
    python benchmarks/e2e_dispatch.py --mode explain        # EXPLAIN ANALYZE
    python benchmarks/e2e_dispatch.py --mode copy           # COPY vs INSERT
    python benchmarks/e2e_dispatch.py --mode sweep          # round-trip cost
    python benchmarks/e2e_dispatch.py --profile pyinstrument
    python benchmarks/e2e_dispatch.py --uvloop              # optional extra
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.backend._cursor import encode_job_cursor
from taskq.backend._protocol import ConnLike, EnqueueArgs, JobFilter, JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import truncate_schema
from taskq.worker.deps import WorkerDeps

# ── Configuration ────────────────────────────────────────────────────────

DEFAULT_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
DEFAULT_SCHEMA = "tq_bench_e2e"
ACTOR = "bench_actor"
QUEUE = "default"
PAGES = 2  # list_jobs pages in the e2e loop

RESULTS_DIR = Path(__file__).parent / "results"


def make_payload(i: int, pad: int = 96) -> dict[str, object]:
    """A realistic small job payload (~300 B serialized)."""
    return {
        "order_id": f"ord-{i:08d}",
        "channel": ["web", "ios", "android"][i % 3],
        "customer": {"id": f"cus-{i % 997:05d}", "tier": ["free", "pro", "ent"][i % 3]},
        "items": [{"sku": f"SKU-{(i + k) % 50:03d}", "qty": (i + k) % 5 + 1} for k in range(3)],
        "notes": f"benchmark payload #{i} " + "x" * pad,
        "flags": {"expedited": i % 7 == 0, "gift": i % 11 == 0},
    }


def handle_job(payload: dict[str, object]) -> dict[str, object]:
    """Tiny in-process handler standing in for a real job body (~tens of µs)."""
    blob = json.dumps(payload, separators=(",", ":")).encode()
    return {"status": "ok", "checksum": zlib.crc32(blob), "chars": len(blob)}


# ── Category timer ───────────────────────────────────────────────────────


class Split:
    """Accumulates wall time per awaited region of the loop."""

    __slots__ = ("calls", "name", "secs")

    def __init__(self, name: str) -> None:
        self.name = name
        self.secs = 0.0
        self.calls = 0

    def time(self, t0: float) -> float:
        dt = time.perf_counter() - t0
        self.secs += dt
        self.calls += 1
        return dt


class Splits:
    def __init__(self) -> None:
        self._map: dict[str, Split] = {}

    def split(self, name: str) -> Split:
        s = self._map.get(name)
        if s is None:
            s = self._map[name] = Split(name)
        return s

    def table(self, wall: float) -> list[tuple[str, float, float, int]]:
        rows = [
            (s.name, s.secs, 100.0 * s.secs / wall if wall else 0.0, s.calls)
            for s in self._map.values()
        ]
        rows.sort(key=lambda r: -r[1])
        return rows


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    idx = min(len(xs) - 1, max(0, round(p / 100.0 * (len(xs) - 1))))
    return xs[idx]


def fmt_ms(secs: float) -> str:
    return f"{secs * 1000:.1f} ms"


# ── Setup / teardown ────────────────────────────────────────────────────


async def setup_schema(admin_dsn: str, schema: str) -> None:
    """Fresh schema + migrations + one seeded actor (tests/conftest convention)."""
    conn = await asyncpg.connect(admin_dsn)
    try:
        await conn.execute(  # Why: schema is a benchmark-controlled identifier, matching testing/pg.py's convention
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )
        await conn.execute(f'CREATE SCHEMA "{schema}"')  # Why: benchmark-controlled identifier
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608  # Why: schema is a benchmark-controlled identifier (conftest convention)
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


def build_settings(dsn: str, schema: str) -> WorkerSettings:
    """WorkerSettings via the integration-tier load_from_dict seam."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
        }
    )


async def build_backend(dsn: str, schema: str) -> tuple[PostgresBackend, WorkerDeps, asyncpg.Pool]:
    """Real WorkerDeps + PostgresBackend over one asyncpg pool (worker shape).

    One shared pool stands in for the three production pools: the split
    under measurement is CPU-vs-I/O on the statement path, not pool
    topology, and a single pool keeps connection pressure on the local
    compose Postgres modest.
    """
    settings = build_settings(dsn, schema)
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


# ── Mode: e2e ───────────────────────────────────────────────────────────


async def run_e2e(backend: PostgresBackend, n_jobs: int, batch: int) -> dict[str, Any]:
    worker_id = new_uuid()
    lock_lease = timedelta(seconds=30)
    enqueue_chunk = 200

    splits = Splits()
    latencies: list[float] = []  # enqueue -> claimed, seconds
    enqueued_at: dict[UUID, float] = {}
    claimed_ids: list[UUID] = []

    wall_t0 = time.perf_counter()

    # 1) Enqueue N jobs in realistic batch chunks. Payload/args building is
    #    timed as explicit sync CPU; the awaited call is the enqueue region.
    remaining = n_jobs
    i = 0
    while remaining > 0:
        take = min(enqueue_chunk, remaining)
        t_args = time.perf_counter()
        args_list = [
            EnqueueArgs(
                id=new_job_id(),
                actor=ACTOR,
                queue=QUEUE,
                payload=make_payload(i + k),
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(take)
        ]
        splits.split("payload_build(sync)").time(t_args)
        t0 = time.perf_counter()
        rows = await backend.enqueue_batch(args_list)
        t1 = time.perf_counter()
        splits.split("enqueue_batch").time(t0)
        for row in rows:
            enqueued_at[row.id] = t1
        i += take
        remaining -= take

    # 2) Worker-shaped drain loop: dispatch a batch, complete each job.
    claimed = 0
    empty_rounds = 0
    while claimed < n_jobs:
        t0 = time.perf_counter()
        rows = await backend.dispatch_batch(worker_id, [QUEUE], batch, lock_lease)
        t1 = time.perf_counter()
        splits.split("dispatch_batch").time(t0)
        if not rows:
            empty_rounds += 1
            if empty_rounds > 3:
                break
            await asyncio.sleep(0.01)
            continue
        empty_rounds = 0
        now = t1
        for row in rows:
            enq = enqueued_at.get(row.id)
            if enq is not None:
                latencies.append(now - enq)
            claimed_ids.append(row.id)
            claimed += 1
            # Sync "handler" section — small realistic CPU work.
            t2 = time.perf_counter()
            result = handle_job(row.payload)
            splits.split("handler_cpu(sync)").time(t2)
            t3 = time.perf_counter()
            await backend.mark_succeeded(row.id, worker_id, result)
            splits.split("mark_succeeded").time(t3)

    # 3) A couple of list_jobs pages (admin/inspection shape), cursor-paginated.
    page_rows: list[Any] = []
    cursor: str | None = None
    for _ in range(PAGES):
        t0 = time.perf_counter()
        page = await backend.list_jobs(JobFilter(status="succeeded", limit=100, cursor=cursor))
        splits.split("list_jobs page").time(t0)
        if not page:
            break
        page_rows.extend(page)
        cursor = encode_job_cursor(page[-1], None)

    # 4) One full sweep pass (all five leader sweeps, timed individually).
    t_sweep_start = time.perf_counter()
    t0 = time.perf_counter()
    n1 = await backend.scheduled_to_pending()
    s1 = time.perf_counter() - t0
    t0 = time.perf_counter()
    n2 = await backend.deadline_sweep()
    s2 = time.perf_counter() - t0
    t0 = time.perf_counter()
    n3 = await backend.reclaim_expired_locks(timedelta(seconds=30), timedelta(seconds=30))
    s3 = time.perf_counter() - t0
    t0 = time.perf_counter()
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        n4 = await backend.sweep_leaked_reservation_slots(
            conn,
            schema=backend._schema_name,  # pyright: ignore[reportPrivateUsage]
        )
    s4 = time.perf_counter() - t0
    t0 = time.perf_counter()
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        n5 = await backend.sweep_expired_results(
            conn,
            schema=backend._schema_name,  # pyright: ignore[reportPrivateUsage]
        )
    s5 = time.perf_counter() - t0
    sweep_wall = time.perf_counter() - t_sweep_start
    splits.split("sweep pass (5 sweeps)").secs = sweep_wall
    splits.split("sweep pass (5 sweeps)").calls = 5

    wall = time.perf_counter() - wall_t0

    table = splits.table(wall)
    await_secs = sum(secs for name, secs, _, _ in table if "(sync)" not in name)
    sync_secs = sum(secs for name, secs, _, _ in table if "(sync)" in name)
    # Any wall not attributed to a timed region is event-loop + untimed sync.
    unattributed = wall - await_secs - sync_secs

    return {
        "n_jobs": n_jobs,
        "dispatch_batch": batch,
        "wall_s": wall,
        "splits": [
            {"region": name, "secs": secs, "pct_of_wall": p, "calls": calls}
            for name, secs, p, calls in table
        ],
        "await_secs": await_secs,
        "explicit_sync_secs": sync_secs,
        "unattributed_secs": unattributed,
        "jobs_per_sec": n_jobs / wall,
        "drain_wall_s": wall - (splits.split("enqueue_batch").secs),
        "latency_ms": {
            "p50": pct(latencies, 50) * 1000,
            "p95": pct(latencies, 95) * 1000,
            "p99": pct(latencies, 99) * 1000,
            "mean": (statistics.mean(latencies) if latencies else 0.0) * 1000,
        },
        "sweep_pass": {
            "scheduled_to_pending": {"count": n1, "ms": s1 * 1000},
            "deadline_exceeded": {"count": n2, "ms": s2 * 1000},
            "expired_locks": {"count": n3, "ms": s3 * 1000},
            "leaked_reservation_slots": {"count": n4, "ms": s4 * 1000},
            "expired_results": {"count": n5, "ms": s5 * 1000},
            "total_ms": sweep_wall * 1000,
        },
        "list_pages": {"jobs_returned": len(page_rows)},
    }


# ── Mode: explain ───────────────────────────────────────────────────────


async def explain_conn(conn: ConnLike, label: str, sql_text: str, *params: object) -> list[str]:
    rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS) " + sql_text, *params)
    return [f"── {label} " + "─" * 40] + [r[0] for r in rows]


async def run_explain(backend: PostgresBackend, n_seed: int) -> list[str]:
    worker_id = new_uuid()
    lease = timedelta(seconds=30)
    schema = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: the exact rendered SQL is the unit under test
    sql = backend._sql  # pyright: ignore[reportPrivateUsage]

    out: list[str] = []

    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        # Seed a real pending backlog through the public fast path.
        args_list = [
            EnqueueArgs(
                id=new_job_id(),
                actor=ACTOR,
                queue=QUEUE,
                payload=make_payload(k),
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
            for k in range(n_seed)
        ]
        await backend.enqueue_batch_fast(args_list)

        # 1) The dispatch CTE (strict_fifo — single default queue).
        out += await explain_conn(
            conn,
            f"dispatch CTE (strict_fifo) over {n_seed} pending, limit 50",
            sql.dispatch_strict_fifo,
            [QUEUE],
            50,
            worker_id,
            lease,
            2,  # dispatch_oversample
        )
        # The EXPLAIN really claimed 50 jobs; reclaim them so the terminal
        # write has a running job and the backlog is intact for later modes.
        running = await conn.fetch(
            f"SELECT id FROM \"{schema}\".jobs WHERE status = 'running' LIMIT 1"  # noqa: S608  # Why: benchmark-controlled identifier
        )
        claimed_id: JobId = JobId(running[0]["id"])
        result = json.dumps({"status": "ok", "checksum": 0})

        # 2) One terminal UPDATE (mark_succeeded).
        out += await explain_conn(
            conn,
            "mark_succeeded terminal UPDATE",
            sql.mark_succeeded,
            claimed_id,
            worker_id,
            result,
            len(result),
            0,
            None,
            None,
        )

        # 3) Sweep 3 (scheduled -> pending): seed due scheduled rows first.
        seed_sweep3_sql = f"""INSERT INTO "{schema}".jobs
                (id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at)
                SELECT gen_random_uuid(), $1, $2, '{{}}'::jsonb, 3, 'transient',
                       'scheduled', clock_timestamp() - interval '1 second'
                FROM generate_series(1, 50)"""  # noqa: S608  # Why: schema is a benchmark-controlled identifier (conftest convention)
        await conn.execute(
            seed_sweep3_sql,
            ACTOR,
            QUEUE,
        )
        from taskq.backend._sweeps import _SWEEP_1_SQL, _SWEEP_3_SQL

        out += await explain_conn(
            conn,
            "sweep 3 scheduled->pending (50 due rows)",
            _SWEEP_3_SQL.format(schema=schema),
            100,
        )

        # 4) Sweep 1 (expired locks): seed running rows with expired locks.
        seed_sweep1_sql = f"""INSERT INTO "{schema}".jobs
                (id, actor, queue, payload, max_attempts, retry_kind, status,
                 started_at, locked_by_worker, lock_expires_at)
                SELECT gen_random_uuid(), $1, $2, '{{}}'::jsonb, 3, 'transient',
                       'running', clock_timestamp(), $3,
                       clock_timestamp() - interval '5 seconds'
                FROM generate_series(1, 20)"""  # noqa: S608  # Why: schema is a benchmark-controlled identifier (conftest convention)
        await conn.execute(
            seed_sweep1_sql,
            ACTOR,
            QUEUE,
            worker_id,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: benchmark-controlled identifier
            f"VALUES ($1, 'bench-host', 0, $2) ON CONFLICT (id) DO NOTHING",
            worker_id,
            [QUEUE],
        )
        out += await explain_conn(
            conn,
            "sweep 1 expired locks (20 expired running rows)",
            _SWEEP_1_SQL.format(schema=schema),
            timedelta(seconds=0),
            timedelta(seconds=0),
            100,
        )

    return out


# ── Mode: copy ──────────────────────────────────────────────────────────


async def run_copy_bench(
    backend: PostgresBackend, sizes: list[int], reps: int
) -> list[dict[str, Any]]:
    """Interleaved enqueue_batch (multi-INSERT) vs enqueue_batch_fast (COPY).

    Interleaving follows the benchmarks/README.md house rule; the table is
    truncated between reps so every rep sees the same table size.
    """
    results: list[dict[str, Any]] = []
    schema = backend._schema_name  # pyright: ignore[reportPrivateUsage]

    async with backend._worker_pool.acquire():  # pyright: ignore[reportPrivateUsage]
        pass  # warm the pool

    for n in sizes:
        insert_secs: list[float] = []
        copy_secs: list[float] = []
        for _rep in range(reps):
            # A side: regular batch INSERT.
            await _truncate_jobs(backend, schema)
            args_list = _mk_args(n)
            t0 = time.perf_counter()
            await backend.enqueue_batch(args_list, enforce_max_pending=False)
            insert_secs.append(time.perf_counter() - t0)

            # B side: COPY fast path.
            await _truncate_jobs(backend, schema)
            args_list = _mk_args(n)
            t0 = time.perf_counter()
            await backend.enqueue_batch_fast(args_list, enforce_max_pending=False)
            copy_secs.append(time.perf_counter() - t0)
        await _truncate_jobs(backend, schema)

        ins_med, cpy_med = statistics.median(insert_secs), statistics.median(copy_secs)
        results.append(
            {
                "n": n,
                "reps": reps,
                "insert_ms": ins_med * 1000,
                "insert_rows_per_s": n / ins_med,
                "copy_ms": cpy_med * 1000,
                "copy_rows_per_s": n / cpy_med,
                "copy_speedup": ins_med / cpy_med,
            }
        )
    return results


def _mk_args(n: int) -> list[EnqueueArgs]:
    return [
        EnqueueArgs(
            id=new_job_id(),
            actor=ACTOR,
            queue=QUEUE,
            payload=make_payload(k),
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )
        for k in range(n)
    ]


async def _truncate_jobs(backend: PostgresBackend, schema: str) -> None:
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        await conn.execute(f'TRUNCATE TABLE "{schema}"."jobs" CASCADE')
        await conn.execute(f'TRUNCATE TABLE "{schema}"."job_events" CASCADE')
        await conn.execute(f'TRUNCATE TABLE "{schema}"."job_attempts" CASCADE')


# ── Mode: sweep round-trips ─────────────────────────────────────────────


async def run_sweep_probe(backend: PostgresBackend, iters: int) -> dict[str, Any]:
    """Measure the statement_timeout apply/restore round-trip cost and a
    no-op sweep pass on idle tables, plus one populated scheduled->pending."""
    # Per-round-trip cost of the three timeout-bookkeeping statements.
    samples: dict[str, list[float]] = {
        "current_setting_fetch": [],
        "set_config_apply": [],
        "set_config_restore": [],
    }
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        for _ in range(iters):
            t0 = time.perf_counter()
            await conn.fetch("SELECT current_setting('statement_timeout')")
            samples["current_setting_fetch"].append(time.perf_counter() - t0)

            t0 = time.perf_counter()
            await conn.execute("SELECT set_config('statement_timeout', $1, true)", "30000")
            samples["set_config_apply"].append(time.perf_counter() - t0)

            t0 = time.perf_counter()
            await conn.execute("SELECT set_config('statement_timeout', $1, true)", "0")
            samples["set_config_restore"].append(time.perf_counter() - t0)

    # Full no-op sweep pass (all five, idle tables).
    t_start = time.perf_counter()
    t0 = time.perf_counter()
    await backend.scheduled_to_pending()
    t_s2p = time.perf_counter() - t0
    t0 = time.perf_counter()
    await backend.deadline_sweep()
    t_dead = time.perf_counter() - t0
    t0 = time.perf_counter()
    await backend.reclaim_expired_locks(timedelta(seconds=30), timedelta(seconds=30))
    t_locks = time.perf_counter() - t0
    t0 = time.perf_counter()
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        await backend.sweep_leaked_reservation_slots(
            conn,
            schema=backend._schema_name,  # pyright: ignore[reportPrivateUsage]
        )
    t_slots = time.perf_counter() - t0
    t0 = time.perf_counter()
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        await backend.sweep_expired_results(
            conn,
            schema=backend._schema_name,  # pyright: ignore[reportPrivateUsage]
        )
    t_res = time.perf_counter() - t0
    noop_total = time.perf_counter() - t_start

    noop = {
        "scheduled_to_pending_ms": t_s2p * 1000,
        "deadline_exceeded_ms": t_dead * 1000,
        "expired_locks_ms": t_locks * 1000,
        "leaked_reservation_slots_ms": t_slots * 1000,
        "expired_results_ms": t_res * 1000,
        "total_ms": noop_total * 1000,
    }

    # One populated sweep: 100 due scheduled rows promoted end-to-end.
    schema = backend._schema_name  # pyright: ignore[reportPrivateUsage]
    async with backend._worker_pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]
        seed_sql = f"""INSERT INTO "{schema}".jobs
                (id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at)
                SELECT gen_random_uuid(), $1, $2, '{{}}'::jsonb, 3, 'transient',
                       'scheduled', clock_timestamp() - interval '1 second'
                FROM generate_series(1, 100)"""  # noqa: S608  # Why: schema is a benchmark-controlled identifier (conftest convention)
        await conn.execute(
            seed_sql,
            ACTOR,
            QUEUE,
        )
    t0 = time.perf_counter()
    promoted = await backend.scheduled_to_pending(batch_size=100)
    pop_ms = (time.perf_counter() - t0) * 1000

    return {
        "round_trip_us": {k: statistics.median(v) * 1e6 for k, v in samples.items()},
        "timeout_bookkeeping_statements_per_sweep": 3,
        "timeout_bookkeeping_us_per_sweep": sum(
            statistics.median(v) * 1e6 for v in samples.values()
        ),
        "noop_sweep_pass": noop,
        "populated_sweep3": {"promoted": promoted, "ms": pop_ms},
    }


# ── Reporting ───────────────────────────────────────────────────────────


def print_e2e(res: dict[str, Any]) -> None:
    print("\n=== E2E dispatch loop (real Postgres) ===")
    print(
        f"jobs={res['n_jobs']}  dispatch_batch={res['dispatch_batch']}  "
        f"wall={res['wall_s']:.2f}s  jobs/sec={res['jobs_per_sec']:.0f}"
    )
    lat = res["latency_ms"]
    print(
        f"enqueue->claimed latency: p50={lat['p50']:.1f}ms  p95={lat['p95']:.1f}ms  "
        f"p99={lat['p99']:.1f}ms  mean={lat['mean']:.1f}ms"
    )
    print("\nWall-clock split (timed regions):")
    print(f"  {'region':34s} {'secs':>8s} {'%wall':>7s} {'calls':>7s}")
    for row in res["splits"]:
        print(
            f"  {row['region']:34s} {row['secs']:8.3f} {row['pct_of_wall']:6.1f}% {row['calls']:7d}"
        )
    print(
        f"  {'=> async (DB wait incl. lib CPU)':34s} {res['await_secs']:8.3f} "
        f"{100 * res['await_secs'] / res['wall_s']:6.1f}%"
    )
    print(
        f"  {'=> explicit sync CPU':34s} {res['explicit_sync_secs']:8.3f} "
        f"{100 * res['explicit_sync_secs'] / res['wall_s']:6.1f}%"
    )
    print(
        f"  {'=> unattributed (loop/bookkeeping)':34s} {res['unattributed_secs']:8.3f} "
        f"{100 * res['unattributed_secs'] / res['wall_s']:6.1f}%"
    )
    cpu_share = 100 * (res["explicit_sync_secs"] + res["unattributed_secs"]) / res["wall_s"]
    io_share = 100 * res["await_secs"] / res["wall_s"]
    print(f"\nCPU:IO ratio (sync CPU+loop : awaited I/O) = {cpu_share:.1f} : {io_share:.1f}")
    sw = res["sweep_pass"]
    print(
        f"sweep pass total={sw['total_ms']:.1f}ms  "
        f"(s2p={sw['scheduled_to_pending']['ms']:.1f}  deadline={sw['deadline_exceeded']['ms']:.1f} "
        f"locks={sw['expired_locks']['ms']:.1f}  slots={sw['leaked_reservation_slots']['ms']:.1f} "
        f"results={sw['expired_results']['ms']:.1f})"
    )


def print_explain(lines: list[str]) -> None:
    print("\n=== EXPLAIN (ANALYZE, BUFFERS) ===")
    for line in lines:
        print(line)


def print_copy(rows: list[dict[str, Any]]) -> None:
    print("\n=== enqueue_batch (multi-INSERT) vs enqueue_batch_fast (COPY) ===")
    print(
        f"  {'n':>7s} {'INSERT ms':>10s} {'INSERT rows/s':>14s} "
        f"{'COPY ms':>10s} {'COPY rows/s':>13s} {'speedup':>8s}"
    )
    for r in rows:
        print(
            f"  {r['n']:7d} {r['insert_ms']:10.1f} {r['insert_rows_per_s']:14.0f} "
            f"{r['copy_ms']:10.1f} {r['copy_rows_per_s']:13.0f} {r['copy_speedup']:7.2f}x"
        )


def print_sweep(res: dict[str, Any]) -> None:
    print("\n=== Sweep round-trip overhead ===")
    print(
        "Timeout-bookkeeping statements per sweep call: "
        f"{res['timeout_bookkeeping_statements_per_sweep']} "
        "(current_setting fetch + set_config apply + set_config restore)"
    )
    for name, us in res["round_trip_us"].items():
        print(f"  {name:22s} median {us:6.1f} µs/round-trip")
    print(
        f"  => bookkeeping cost per sweep call: "
        f"{res['timeout_bookkeeping_us_per_sweep'] / 1000:.2f} ms"
    )
    print("No-op sweep pass (idle tables):")
    for k, v in res["noop_sweep_pass"].items():
        print(f"  {k:32s} {v:7.2f} ms")
    pop = res["populated_sweep3"]
    print(f"Populated sweep 3: promoted={pop['promoted']} in {pop['ms']:.2f} ms")


# ── Main ────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", default=os.environ.get("TASKQ_PG_DSN", DEFAULT_DSN))
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--mode", choices=["e2e", "explain", "copy", "sweep"], default="e2e")
    ap.add_argument("--jobs", type=int, default=2000)
    ap.add_argument("--batch", type=int, default=50)
    ap.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000])
    ap.add_argument("--reps", type=int, default=7)
    ap.add_argument("--iters", type=int, default=300)
    ap.add_argument("--profile", choices=["none", "pyinstrument"], default="none")
    ap.add_argument("--uvloop", action="store_true")
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    if args.uvloop:
        import uvloop

        uvloop.install()
        print("[e2e_dispatch] running under uvloop")

    # Production runs at info level, but the benchmark prints its own report;
    # 2k+ per-job log lines would swamp the CPU:IO split with stderr noise.
    import logging

    import structlog

    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))
    dsn = args.dsn
    schema = args.schema
    print(f"[e2e_dispatch] dsn={dsn} schema={schema}")

    async def _setup_and_go() -> dict[str, Any]:
        await setup_schema(dsn, schema)
        backend, _deps, pool = await build_backend(dsn, schema)
        try:
            if args.mode == "e2e":
                return {"e2e": await run_e2e(backend, args.jobs, args.batch)}
            if args.mode == "explain":
                lines = await run_explain(backend, n_seed=min(args.jobs, 2000))
                return {"explain": lines}
            if args.mode == "copy":
                return {"copy": await run_copy_bench(backend, args.sizes, args.reps)}
            return {"sweep": await run_sweep_probe(backend, args.iters)}
        finally:
            await cleanup_schema(dsn, schema)
            await pool.close()

    if args.profile == "pyinstrument":
        from pyinstrument import Profiler

        prof = Profiler(async_mode="enabled")
        prof.start()
        try:
            result = asyncio.run(_setup_and_go())
        finally:
            prof.stop()
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        txt_path = RESULTS_DIR / f"e2e_dispatch-{ts}.txt"
        html_path = RESULTS_DIR / f"e2e_dispatch-{ts}.html"
        txt_path.write_text(prof.output_text(unicode=True, color=False), encoding="utf-8")
        html_path.write_text(prof.output_html(), encoding="utf-8")
        print(f"[e2e_dispatch] pyinstrument artifacts: {txt_path}, {html_path}")
    else:
        result = asyncio.run(_setup_and_go())

    if "e2e" in result:
        print_e2e(result["e2e"])
    if "explain" in result:
        print_explain(result["explain"])
    if "copy" in result:
        print_copy(result["copy"])
    if "sweep" in result:
        print_sweep(result["sweep"])

    if args.json_out:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RESULTS_DIR / args.json_out
        out_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        print(f"[e2e_dispatch] json: {out_path}")

    print(
        f"\n[e2e_dispatch] cleanup: dynamic tables in schema '{schema}' TRUNCATED; "
        "the migrated (empty) schema itself is left in place — drop with "
        f"'DROP SCHEMA \"{schema}\" CASCADE' if unwanted."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
