"""A/B: TimescaleDB hypertables vs plain PostgreSQL for TaskQ's hot admin surfaces.

Companion to the ``docs/guides/timescaledb.md`` opt-in (``src/taskq/timescale.py``):
the two engines are the SAME ``timescale/timescaledb`` image, started as two
disposable containers by this script — one left vanilla (extension uncreated),
one converted with TaskQ's own ``enable_hypertables`` (the real deploy-step DDL,
including the widened unique constraints and retention policies).  Both get
byte-identical seeded data, so every number below is engine-vs-engine on the
same corpus.

Measured, per the retention/admin trade-off question:

A. Historical job retention (the daily sweep batch shapes, run via the REAL
   sweep machinery from ``taskq.worker._leader_shared`` / ``_sweeps``):
   1. prune: 100k aged succeeded jobs drained in 10k-row batches of the
      archive candidate + ``_ARCHIVE_CTE_SQL`` write (jobs -> jobs_archive +
      job_attempts -> job_attempts_archive + cascade + watermark).
   2. archive expiry: the seeded expired cohort drained by
      ``_EXPIRY_CTE_SQL`` in 10k batches at 365d-archive scale.
   3. event TTL: ``sweep_expired_events``' one-10k-batch-per-tick shape.
   4. aftermath: dead tuples + relation sizes after the deletes (the
      DELETE-vs-chunk-drop bloat question), from ``pg_stat_user_tables`` /
      ``pg_total_relation_size`` (chunk-summed on the hypertable side).

B. Admin dashboard queries (the exact WHERE/ORDER shapes built by
   ``taskq.web/admin/jobs.py``'s ``_build_where`` / ``_build_paginated_sql``
   and ``queues.py``'s overview roll-up), interleaved p50/p95 at a 1M-row
   ``jobs`` table: unfiltered recent page, status-filtered, tag-filtered,
   text-search, queue depth, and the ``/jobs/count`` aggregates, plus the
   archive tab's recent page.  A cross-engine row-identity assertion runs on
   every shape (both engines hold byte-identical data) before timings count.

C. Write path: ``enqueue_batch_fast`` (the COPY hot path) and the batched
   ``job_events`` INSERT, interleaved rounds on each engine — does the
   hypertable tax the claim/insert hot path?

House rules (benchmarks/README.md): A/B batches are interleaved (engine by
engine, batch by batch) so machine drift cancels; correctness is asserted
before any timing is trusted; results land in ``benchmarks/results/``
(gitignored history).

The script is rerunnable end-to-end: it starts and tears down its own
containers, drops and rebuilds its schemas.  It writes NOTHING outside
``benchmarks/results/`` and modifies no ``src/`` code.

Run: .venv/bin/python benchmarks/timescale_tradeoffs.py [--keep-containers]

Scale-sweep mode (``--scale-sweep``): runs the HEADLINE shapes only — the
prune drain, the event TTL drain, the archive tab's newest-first page, and
one dashboard count as parity reference — at multiple archive/event scales
(default 10k / 100k / 400k / 2M archive rows; jobs scale with them so the
prune cohort stays 10% of scale), on a fresh identical container pair per
scale, torn down between scales.  Answers: where on the scale curve does the
chunk-pruned read win emerge, and does the hypertable's prune penalty widen
or narrow with scale?  Results land in ``timescale-tradeoffs-sweep.json``
(schema 2: the per-scale dimension).  TimescaleDB chunks by time and the
seed spreads rows over the same 1-360d / 8-29d windows at every scale, so
chunk COUNT is ~constant while rows-per-chunk scales — the intended probe
of per-batch cost vs per-chunk fan-out.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg

sys.path.insert(0, str(Path(__file__).parent))

from tors_harness import (
    percentile,  # pyright: ignore[reportMissingImports]  # Why: the sys.path bootstrap above is the benchmarks/ convention; pyright's include is src/tests/examples.
)

from taskq._ids import new_job_id  # pyright: ignore[reportPrivateUsage]
from taskq.backend._enqueue import (
    _enqueue_batch_fast,  # pyright: ignore[reportPrivateUsage]  # Why: the private function IS the COPY enqueue path; the bench measures it in place.
)
from taskq.backend._protocol import EnqueueArgs
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sql_templates import render
from taskq.backend._sweeps import sweep_expired_events  # pyright: ignore[reportPrivateUsage]
from taskq.constants import DEFAULT_PRUNE_BATCH_SIZE
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.timescale import enable_hypertables, retention_policy_floor
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin module's own WHERE/order builders are the queries being measured; a hand-copied SQL shape would drift from the real page.
    _ARCHIVE_COLS,
    _LIVE_COLS,
    _SORTABLE_ARCHIVE,
    _SORTABLE_LIVE,
    _build_paginated_sql,
    _build_where,
)
from taskq.web.admin.queues import _QUEUE_OVERVIEW_SQL  # pyright: ignore[reportPrivateUsage]
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]
    _ARCHIVE_CANDIDATE_SQL,
    _ARCHIVE_CTE_SQL,
    _compose_expiry_sql,
    _run_prune_archive_batch,
    _run_prune_batch,
)

# ── Benchmark configuration ──────────────────────────────────────────────

IMAGE = "timescale/timescaledb:2.30.1-pg18"
CONTAINER_PLAIN = "tq-tsbench-plain"
CONTAINER_TS = "tq-tsbench-ts"
PORT_PLAIN = 55441
PORT_TS = 55442
DSN_PLAIN = f"postgresql://taskq:taskq@localhost:{PORT_PLAIN}/taskq"
DSN_TS = f"postgresql://taskq:taskq@localhost:{PORT_TS}/taskq"
SCHEMA = "tq_tsab"

# Server flags: IDENTICAL on both containers except the preload the
# extension requires — that difference IS the feature being measured.
SERVER_FLAGS = [
    "-c",
    "max_connections=200",
    "-c",
    "jit=off",
    "-c",
    "shared_buffers=512MB",
    "-c",
    "max_wal_size=4GB",
]

# Seed scales (identical on both engines; state in the results meta).
SEED_JOBS = 1_000_000  # the dashboard's jobs population
SEED_ARCHIVE = 400_000  # 300k in the 1-360d window + 100k expired cohort
SEED_ARCHIVE_EXPIRED = 100_000
ARCHIVE_EXPIRED = SEED_ARCHIVE_EXPIRED  # actual, set by seed_engine (scales down)
SEED_EVENTS = 400_000  # 100k aged (96k TTL-eligible + 4k outbox) + 300k recent
SEED_ATTEMPTS = 100_000  # one attempt per aged-succeeded (prune cohort) job

# The prune cohort: the first 100k job ids, succeeded with finished_at
# 40-49 days old — past the 30d default succeeded retention, exactly the
# daily prune's candidate shape.
PRUNE_ROWS = 100_000  # full-scale cohort; seed_engine derives the actual one
PRUNE_COHORT = PRUNE_ROWS  # aged-succeeded population, set by seed_engine
PRUNE_RETENTION = timedelta(days=30)  # constants.DEFAULT_PRUNE_RETENTION
ARCHIVE_RETENTION = timedelta(days=365)  # WorkerSettings default
EVENT_RETENTION = timedelta(days=7)  # WorkerSettings default
BATCH = DEFAULT_PRUNE_BATCH_SIZE  # 10000, the daily prune's batch size

# The sweep helpers' statement_timeout is the production 4s default; a
# benchmark batch that hits it would abort the drain, so the bench widens it
# (recorded in the meta — batches here measure throughput, not the breaker).
SWEEP_TIMEOUT_MS = 120_000

DASHBOARD_ROUNDS = 11  # odd: the median is an actual round
WRITE_ROUNDS = 15
WRITE_BATCH = 1_000
EVENTS_WRITE_BATCH = 1_000

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_NAME = "timescale-tradeoffs.json"
RESULTS_SWEEP_NAME = "timescale-tradeoffs-sweep.json"

# ── Scale-sweep mode (--scale-sweep) ─────────────────────────────────────
#
# The headline shapes at several archive scales, both engines, to locate the
# crossover on the scale curve.  One knob per scale: jobs = archive = events
# = scale, so the prune cohort is 10% of scale (the seed SQL's proportional
# bands) and the aged-event cohort is a quarter of it — the reference run's
# exact proportions at every point on the curve.  The seed spreads rows over
# the same calendar windows at every scale, so chunk count stays ~constant
# and rows-per-chunk is what scales: the per-batch-cost vs per-chunk-fan-out
# question the sweep is built to answer.
SWEEP_SHAPES = {
    "archive_recent_finished_p51",  # the chunk-pruning win shape
    "jobs_count_all_statuses",  # parity reference: jobs is a plain table on BOTH engines
}
SWEEP_SCALES_DEFAULT = "10000,100000,400000,2000000"

_KEEP_CONTAINERS = False


# ── Small shared helpers ─────────────────────────────────────────────────


def ms(v: float) -> str:
    return f"{v:,.1f} ms"


def run(cmd: list[str]) -> str:
    # Why noqa: the benchmark's own fixed docker arguments, never user input.
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603  # Why: benchmark-controlled docker CLI invocation.
    return out.stdout.strip()


async def wait_ready(dsn: str, label: str, tries: int = 90) -> None:
    last: Exception | None = None
    for _ in range(tries):
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
            return
        except Exception as exc:  # Why: readiness probe, any failure is retryable
            last = exc
            await asyncio.sleep(1)
    raise RuntimeError(f"{label} never became ready: {last}")


async def connected(dsn: str) -> asyncpg.Connection:
    return await asyncpg.connect(dsn)


# ── Database lifecycle (both engines, started and torn down by the run) ──


def start_containers() -> None:
    for name in (CONTAINER_PLAIN, CONTAINER_TS):
        # Why noqa: fixed docker arguments, benchmark-controlled container name.
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607
    run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER_PLAIN,
            "-e",
            "POSTGRES_USER=taskq",
            "-e",
            "POSTGRES_PASSWORD=taskq",
            "-e",
            "POSTGRES_DB=taskq",
            "-p",
            f"{PORT_PLAIN}:5432",
            IMAGE,
            "postgres",
            *SERVER_FLAGS,
        ]
    )
    run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER_TS,
            "-e",
            "POSTGRES_USER=taskq",
            "-e",
            "POSTGRES_PASSWORD=taskq",
            "-e",
            "POSTGRES_DB=taskq",
            "-p",
            f"{PORT_TS}:5432",
            IMAGE,
            "postgres",
            "-c",
            "shared_preload_libraries=timescaledb",
            *SERVER_FLAGS,
        ]
    )


def stop_containers() -> None:
    for name in (CONTAINER_PLAIN, CONTAINER_TS):
        # Why noqa: fixed docker arguments, benchmark-controlled container name.
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607


async def setup_engine(dsn: str, *, hypertables: bool) -> dict[str, Any]:
    """Drop + rebuild the schema; convert with TaskQ's own deploy-step DDL when
    the engine is the hypertable side."""
    conn = await connected(dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
        await apply_pending(conn, schema=SCHEMA)
        if not hypertables:
            return {}
        settings = WorkerSettings.load_from_dict({"TASKQ_TIMESCALEDB_HYPERTABLES": "true"})
        report = await enable_hypertables(conn, schema=SCHEMA, settings=settings)
        # The retention policies enable_hypertables registered run on the
        # TimescaleDB background workers; left running they would delete the
        # aged seed cohorts mid-benchmark and break the drain measurements.
        # Stopping them is the benchmark's own harness choice, NOT a changed
        # feature flag: the row-level sweeps under measurement run unchanged.
        stopped = False
        for stmt in (
            "SELECT _timescaledb_functions.stop_background_workers()",
            "SELECT _timescaledb_internal.stop_background_workers()",
        ):
            try:
                await conn.execute(stmt)
                stopped = True
                break
            except Exception:  # noqa: S110  # Why: probe across extension versions, the next statement is tried
                pass
        assert stopped, "could not stop TimescaleDB background workers"
        return {
            "converted": list(report.converted),
            "retention_policies": list(report.retention_policies),
            "compression_policies": list(report.compression_policies),
            "decompression_guc_warning": report.decompression_guc_warning,
        }
    finally:
        await conn.close()


# ── Seeding: identical byte-for-byte data on both engines ────────────────
#
# Every timestamp derives from ONE fixed base instant bound as $1 (not
# now()), so the two engines receive identical rows and the dashboard's
# cross-engine row-identity assertion can compare row-for-row.


_SEED_JOBS_SQL = """\
-- $3 is the aged-cohort boundary (10% of the seed at any scale: 100k of 1M);
-- the bands below scale from it proportionally.
WITH g AS (SELECT generate_series(1, $2::int) AS i),
b AS (SELECT i, CASE
        WHEN i <= $3::int THEN 1             -- aged succeeded: the prune cohort
        WHEN i <= $3::int * 5 THEN 2         -- recent succeeded (0-6d)
        WHEN i <= $3::int * 6 THEN 3         -- failed (0-20d)
        WHEN i <= $3::int * 64 / 10 THEN 4   -- cancelled
        WHEN i <= $3::int * 645 / 100 THEN 5 -- crashed
        WHEN i <= $3::int * 65 / 10 THEN 6   -- abandoned
        WHEN i <= $3::int * 68 / 10 THEN 7   -- pending
        WHEN i <= $3::int * 69 / 10 THEN 8   -- scheduled
        WHEN i <= $3::int * 695 / 100 THEN 9 -- running
        ELSE 2 END AS kind
      FROM g),
t AS (SELECT b.i, b.kind,
        CASE kind
          WHEN 1 THEN make_interval(days => 40 + (i % 1000) / 100, secs => i % 86400)
          WHEN 2 THEN make_interval(days => i % 6, secs => i % 86400)
          WHEN 3 THEN make_interval(days => i % 20, secs => i % 86400)
          WHEN 4 THEN make_interval(days => i % 15, secs => i % 86400)
          WHEN 5 THEN make_interval(days => i % 10, secs => i % 86400)
          WHEN 6 THEN make_interval(days => i % 10, secs => i % 86400)
          ELSE make_interval(days => i % 30, secs => i % 86400)
        END AS age
      FROM b)
INSERT INTO "{s}".jobs (id, actor, queue, payload, status, attempt, max_attempts, retry_kind,
                        created_at, scheduled_at, started_at, finished_at, last_heartbeat_at,
                        locked_by_worker, lock_expires_at, error_class, error_message, tags, priority)
SELECT md5(i::text)::uuid,
       'actor_' || (i % 8),
       'queue_' || (i % 4),
       jsonb_build_object('i', i, 'pad', repeat('p', 200)),
       (CASE kind WHEN 1 THEN 'succeeded' WHEN 2 THEN 'succeeded' WHEN 3 THEN 'failed'
                  WHEN 4 THEN 'cancelled' WHEN 5 THEN 'crashed' WHEN 6 THEN 'abandoned'
                  WHEN 7 THEN 'pending' WHEN 8 THEN 'scheduled' WHEN 9 THEN 'running'
                  END)::{s}.job_status,
       (CASE WHEN kind IN (7, 8) THEN 0 ELSE 1 END)::smallint,
       3, 'transient',
       $1::timestamptz - age, $1::timestamptz - age,
       CASE WHEN kind NOT IN (7, 8) THEN $1::timestamptz - age + interval '5 min' END,
       CASE WHEN kind <= 6 THEN $1::timestamptz - age + interval '6 min' END,
       CASE WHEN kind = 9 THEN $1::timestamptz - interval '5 min' END,
       CASE WHEN kind = 9 THEN md5(i::text)::uuid END,
       CASE WHEN kind = 9 THEN $1::timestamptz + interval '55 min' END,
       CASE WHEN kind = 3 THEN 'ValueError' END,
       CASE WHEN kind = 3 THEN 'synthetic failure ' || i END,
       CASE WHEN i % 10 = 0 THEN ARRAY['priority']
            WHEN i % 10 = 1 THEN ARRAY['batch', 'nightly']
            ELSE ARRAY['regular'] END,
       (i % 3)::smallint
FROM t
"""  # Why: schema is a benchmark-controlled identifier, all values bound.


_SEED_ATTEMPTS_SQL = """\
-- One attempt per prune-cohort job: the archive CTE's job_attempts move has
-- a realistic population without seeding a row per live job.
INSERT INTO "{s}".job_attempts (job_id, attempt, started_at, finished_at, outcome, duration_ms)
SELECT id, 0, created_at + interval '5 min', finished_at, 'succeeded', 60000
FROM "{s}".jobs
WHERE created_at < $1::timestamptz - interval '35 days'
"""


_SEED_ARCHIVE_SQL = """\
-- jobs_archive at 365d scale: rows in the 1-360d window (expire_at in the
-- future) + an expired cohort (finished_at 370-399d old, so expire_at =
-- finished_at + 365d sits 5-34d in the past: the expiry sweep's
-- exactly-removable cohort).  $3 is the unexpired/expired boundary index.
WITH g AS (SELECT generate_series(1, $2::int) AS i),
t AS (SELECT i,
        CASE WHEN i <= $3::int THEN $1::timestamptz - make_interval(days => 1 + (i % 360))
             ELSE $1::timestamptz - make_interval(days => 370 + (i % 30)) END AS fin
      FROM g)
INSERT INTO "{s}".jobs_archive (id, actor, queue, payload, status, attempt, max_attempts, retry_kind,
                                created_at, scheduled_at, started_at, finished_at,
                                archived_at, expire_at, tags, priority)
SELECT md5(('a' || i)::text)::uuid,
       'actor_' || (i % 8),
       'queue_' || (i % 4),
       jsonb_build_object('i', i, 'pad', repeat('p', 200)),
       (CASE WHEN i % 20 = 0 THEN 'failed' ELSE 'succeeded' END)::{s}.job_status,
       1, 3, 'transient',
       fin - interval '6 min', fin - interval '6 min', fin - interval '5 min', fin,
       fin + interval '1 hour', fin + interval '365 days',
       CASE WHEN i % 10 = 0 THEN ARRAY['priority'] ELSE ARRAY['regular'] END,
       (i % 3)::smallint
FROM t
"""


_SEED_EVENTS_SQL = """\
-- job_events: $4 aged rows (8-29d old, past the 7d retention; every 25th an
-- outbox-shaped lock_expired row the sweep must carve out) + the rest recent.
-- They reference the RECENT-succeeded job band ($3+1 .. $3*5), NOT the prune
-- cohort: the prune's cascade must not remove the TTL sweep's backlog.
WITH g AS (SELECT generate_series(1, $2::int) AS j)
INSERT INTO "{s}".job_events (job_id, occurred_at, kind, detail)
SELECT md5(($3::int + 1 + ((j - 1) % ($3::int * 4)))::text)::uuid,
       CASE WHEN j <= $4::int THEN $1::timestamptz - make_interval(days => 8 + (j % 22), secs => j % 86400)
            ELSE $1::timestamptz - make_interval(secs => j % 86400) END,
       'state_change',
       CASE WHEN j % 25 = 0 THEN '{{"reason":"lock_expired"}}'::jsonb
            ELSE '{{"from_state":"pending","to_state":"running"}}'::jsonb END
FROM g
"""


async def seed_engine(dsn: str, base: datetime) -> dict[str, float]:
    global ARCHIVE_EXPIRED
    timings: dict[str, float] = {}
    conn = await connected(dsn)
    try:
        global PRUNE_COHORT
        t0 = time.perf_counter()
        PRUNE_COHORT = SEED_JOBS // 10
        await conn.execute(_SEED_JOBS_SQL.format(s=SCHEMA), base, SEED_JOBS, PRUNE_COHORT)
        timings["jobs_s"] = time.perf_counter() - t0
        print(f"    jobs {SEED_JOBS:,} rows in {timings['jobs_s']:.0f}s", flush=True)
        t0 = time.perf_counter()
        await conn.execute(_SEED_ATTEMPTS_SQL.format(s=SCHEMA), base)
        # The expired cohort is proportional at small scales: a quarter of the
        # archive, capped by the requested expired count.
        ARCHIVE_EXPIRED = min(SEED_ARCHIVE_EXPIRED, SEED_ARCHIVE // 4)
        await conn.execute(
            _SEED_ARCHIVE_SQL.format(s=SCHEMA), base, SEED_ARCHIVE, SEED_ARCHIVE - ARCHIVE_EXPIRED
        )
        await conn.execute(
            _SEED_EVENTS_SQL.format(s=SCHEMA), base, SEED_EVENTS, PRUNE_COHORT, SEED_EVENTS // 4
        )
        timings["aux_s"] = time.perf_counter() - t0
        print(f"    attempts+archive+events in {timings['aux_s']:.0f}s", flush=True)
        t0 = time.perf_counter()
        for table in ("jobs", "jobs_archive", "job_events", "job_attempts"):
            await conn.execute(f'ANALYZE "{SCHEMA}".{table}')
        timings["analyze_s"] = time.perf_counter() - t0
    finally:
        await conn.close()
    return timings


# ── Correctness asserts (run before any timing is trusted) ───────────────


async def assert_seed_identical() -> dict[str, Any]:
    """Row counts and an ordered-sample checksum must agree across engines."""
    counts: dict[str, dict[str, int]] = {}
    for label, dsn in (("plain", DSN_PLAIN), ("ts", DSN_TS)):
        conn = await connected(dsn)
        counts[label] = {
            t: await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".{t}')  # noqa: S608  # Why: schema and table are benchmark-controlled constants.
            for t in ("jobs", "job_attempts", "jobs_archive", "job_events", "job_attempts_archive")
        }
        await conn.close()
    for t in counts["plain"]:
        assert counts["plain"][t] == counts["ts"][t], f"seed mismatch on {t}: {counts}"
    checksums = {}
    checksum_sql = (
        "SELECT md5(string_agg(sig, ',' ORDER BY id)) FROM ("  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        "SELECT id, id::text || '|' || actor || '|' || status::text || '|' "
        "|| created_at::text AS sig "
        f'FROM "{SCHEMA}".jobs ORDER BY id LIMIT 5000) q'
    )
    for label, dsn in (("plain", DSN_PLAIN), ("ts", DSN_TS)):
        conn = await connected(dsn)
        checksums[label] = await conn.fetchval(checksum_sql)
        await conn.close()
    assert checksums["plain"] == checksums["ts"], f"sample checksum mismatch: {checksums}"

    # The hypertable side must actually BE hypertables; the plain side must not.
    ts_conn = await connected(DSN_TS)
    ht = {
        r["table_name"]
        for r in await ts_conn.fetch(
            "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1", SCHEMA
        )
    }
    chunk_counts = {
        r["hypertable_name"]: r["chunks"]
        for r in await ts_conn.fetch(
            "SELECT hypertable_name, count(*)::int AS chunks FROM timescaledb_information.chunks "
            "WHERE hypertable_schema = $1 GROUP BY 1",
            SCHEMA,
        )
    }
    await ts_conn.close()
    assert {"job_events", "jobs_archive", "job_attempts_archive"} <= ht, ht
    assert "jobs" not in ht, ht
    plain_conn = await connected(DSN_PLAIN)
    # The plain side never CREATE EXTENSIONs timescaledb, so the catalog may
    # not exist there at all — to_regclass guards the probe.
    ht_plain = await plain_conn.fetchval(
        "SELECT CASE WHEN to_regclass('_timescaledb_catalog.hypertable') IS NULL THEN 0 "
        "ELSE (SELECT count(*) FROM _timescaledb_catalog.hypertable "
        "WHERE schema_name = $1) END",
        SCHEMA,
    )
    await plain_conn.close()
    assert ht_plain == 0, "plain engine has hypertables"
    return {
        "counts": counts,
        "sample_checksum": checksums["plain"],
        "hypertables": sorted(ht),
        "chunk_counts": chunk_counts,
    }


# ── Timing record ────────────────────────────────────────────────────────


class Side:
    """One engine's per-round timing samples."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.rounds: list[float] = []

    @property
    def p50(self) -> float:
        return percentile(sorted(self.rounds), 0.50)

    @property
    def p95(self) -> float:
        return percentile(sorted(self.rounds), 0.95)

    @property
    def total(self) -> float:
        return sum(self.rounds)


# ── B: admin dashboard queries ───────────────────────────────────────────
#
# Every shape is built with the admin module's own SQL builders, so the
# measured statement is the statement the page issues.  Each interleaved
# round runs the shape on plain, then on ts; the first round's rows must be
# row-for-row identical (identical seed) or the shape is marked incorrect.


def _norm_rows(rows: list[asyncpg.Record]) -> list[tuple[str, ...]]:
    # running_for_ms / lease_expired are server-side projections evaluated
    # against clock_timestamp() — deliberately different on every execution
    # (the single-arbiter database-clock rule). They are not seed data: the
    # identity assert drops them and compares every stored column.
    dropped = {"running_for_ms", "lease_expired"}
    return [tuple(str(v) for k, v in dict(r).items() if k not in dropped) for r in rows]


async def dashboard_round(
    conn: asyncpg.Connection, sql: str, args: list[Any]
) -> list[asyncpg.Record]:
    return await conn.fetch(sql, *args)


def _page_sql(
    table: str, cols: str, sortable: dict, where: str, params: list[Any]
) -> tuple[str, list[Any]]:
    return _build_paginated_sql(
        SCHEMA,
        table,
        cols,
        sortable,
        where,
        params,
        None,
        None,
        "next",
        "created_at" if table == "jobs" else "finished_at",
        "desc",
    )


async def bench_dashboard(only: set[str] | None = None) -> list[dict[str, Any]]:
    """Interleaved p50/p95 rounds for the admin page shapes.

    ``only`` restricts the run to a subset of the shape names (the scale
    sweep measures the headline subset); ``None`` runs the full battery.
    """
    all_statuses = sorted(
        {
            "pending",
            "scheduled",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "crashed",
            "abandoned",
        }
    )
    terminal = sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"})

    live_where, live_params = _build_where(all_statuses, None, None, None, None, None, None, None)
    failed_where, failed_params = _build_where(["failed"], None, None, None, None, None, None, None)
    tag_where, tag_params = _build_where(
        all_statuses, None, None, None, None, None, None, None, tags=["priority"]
    )
    search_where, search_params = _build_where(
        all_statuses, None, None, None, None, None, None, "actor_3"
    )
    arch_where, arch_params = _build_where(terminal, None, None, None, None, None, None, None)

    shapes: list[tuple[str, str, list[Any]]] = [
        (
            "jobs_unfiltered_recent_p51",
            *_page_sql("jobs", _LIVE_COLS, _SORTABLE_LIVE, live_where, live_params),
        ),
        (
            "jobs_status_failed_p51",
            *_page_sql("jobs", _LIVE_COLS, _SORTABLE_LIVE, failed_where, failed_params),
        ),
        (
            "jobs_tag_priority_p51",
            *_page_sql("jobs", _LIVE_COLS, _SORTABLE_LIVE, tag_where, tag_params),
        ),
        (
            "jobs_search_actor3_p51",
            *_page_sql("jobs", _LIVE_COLS, _SORTABLE_LIVE, search_where, search_params),
        ),
        (
            "jobs_count_all_statuses",
            f'SELECT count(*) FROM "{SCHEMA}".jobs WHERE status = ANY($1)',  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            [all_statuses],
        ),
        (
            "jobs_count_failed",
            f'SELECT count(*) FROM "{SCHEMA}".jobs WHERE status = ANY($1)',  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            [["failed"]],
        ),
        ("queues_overview_depth", _QUEUE_OVERVIEW_SQL.format(schema=SCHEMA), []),
        (
            "archive_recent_finished_p51",
            *_page_sql("jobs_archive", _ARCHIVE_COLS, _SORTABLE_ARCHIVE, arch_where, arch_params),
        ),
    ]

    conn_plain = await connected(DSN_PLAIN)
    conn_ts = await connected(DSN_TS)
    results: list[dict[str, Any]] = []
    try:
        for name, sql, args in shapes:
            if only is not None and name not in only:
                continue
            a_side, b_side = Side("plain"), Side("ts")
            identical = True
            expected: list[tuple[str, ...]] | None = None
            for rnd in range(DASHBOARD_ROUNDS):
                t0 = time.perf_counter()
                rows_a = await dashboard_round(conn_plain, sql, args)
                a_side.rounds.append((time.perf_counter() - t0) * 1000)
                t0 = time.perf_counter()
                rows_b = await dashboard_round(conn_ts, sql, args)
                b_side.rounds.append((time.perf_counter() - t0) * 1000)
                if rnd == 0:
                    expected = _norm_rows(rows_a)
                    identical = _norm_rows(rows_b) == expected
                    if not identical:
                        print(f"    !! {name}: engines returned different rows")
            results.append(
                {
                    "name": name,
                    "plain_p50_ms": a_side.p50,
                    "plain_p95_ms": a_side.p95,
                    "ts_p50_ms": b_side.p50,
                    "ts_p95_ms": b_side.p95,
                    "plain_faster_pct": (
                        100 * (b_side.p50 - a_side.p50) / b_side.p50 if b_side.p50 else 0.0
                    ),
                    "rounds": DASHBOARD_ROUNDS,
                    "rows_identical": identical,
                    "first_round_rows": len(expected) if expected is not None else 0,
                }
            )
            flag = "OK" if identical else "MISMATCH!"
            print(
                f"    {name:<34} plain {ms(a_side.p50):>11}  ts {ms(b_side.p50):>11}  {flag}",
                flush=True,
            )
    finally:
        await conn_plain.close()
        await conn_ts.close()
    return results


# ── C: write path ────────────────────────────────────────────────────────


async def bench_write() -> tuple[dict[str, Any], dict[str, int]]:
    """Interleaved enqueue_batch_fast rounds + batched job_events INSERT rounds.

    Returns the measurements and the per-engine enqueued-job counts for the
    post-bench cleanup assert.  The event write reuses the enqueued ids (the
    job_events FK requires real parents), so the event rounds measure the
    hypertable-side event insert against identical parent sets.
    """
    pools: dict[str, asyncpg.Pool] = {}
    conns: dict[str, asyncpg.Connection] = {}
    enqueued: dict[str, int] = {}
    try:
        for label, dsn in (("plain", DSN_PLAIN), ("ts", DSN_TS)):
            pools[label] = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            conns[label] = await connected(dsn)
        templates = {label: render(SCHEMA) for label in pools}
        events_sql = {
            label: INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=SCHEMA) for label in pools
        }

        enqueue_sides = {"plain": Side("plain"), "ts": Side("ts")}
        event_sides = {"plain": Side("plain"), "ts": Side("ts")}
        ordinals = {"plain": 0, "ts": 0}
        for _ in range(WRITE_ROUNDS):
            for label in ("plain", "ts"):
                base = ordinals[label]
                args_list = [
                    EnqueueArgs(
                        id=new_job_id(),
                        actor="bench_actor",
                        queue="bench_q",
                        payload={"i": base + k, "pad": "x" * 100},
                        max_attempts=3,
                        retry_kind="transient",
                        scheduled_at=None,
                        tags=("bench",),
                    )
                    for k in range(WRITE_BATCH)
                ]
                t0 = time.perf_counter()
                n = await _enqueue_batch_fast(pools[label], templates[label], SCHEMA, args_list)
                dt = time.perf_counter() - t0
                enqueue_sides[label].rounds.append(dt * 1000)
                assert n == WRITE_BATCH, f"enqueue wrote {n} of {WRITE_BATCH}"
                enqueued[label] = enqueued.get(label, 0) + n
                ordinals[label] = base + WRITE_BATCH

                job_ids = [a.id for a in args_list]
                details = [
                    json.dumps({"from_state": "pending", "i": base + k})
                    for k in range(EVENTS_WRITE_BATCH)
                ]
                t0 = time.perf_counter()
                await conns[label].execute(
                    events_sql[label], job_ids[:EVENTS_WRITE_BATCH], details, "state_change"
                )
                event_sides[label].rounds.append((time.perf_counter() - t0) * 1000)

        # Correctness before the timings are trusted: every round wrote its
        # batch, and both engines hold the same bench-row population.
        for label, conn in conns.items():
            n_jobs = await conn.fetchval(
                f"SELECT count(*) FROM \"{SCHEMA}\".jobs WHERE actor = 'bench_actor'"  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            )
            n_events = await conn.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".job_events e '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
                f"JOIN \"{SCHEMA}\".jobs j ON j.id = e.job_id WHERE j.actor = 'bench_actor'"
            )
            assert n_jobs == enqueued[label], (label, n_jobs, enqueued[label])
            assert n_events == WRITE_ROUNDS * EVENTS_WRITE_BATCH, (label, n_events)
        assert enqueued["plain"] == enqueued["ts"], enqueued

        def one(sides: dict[str, Side]) -> dict[str, float]:
            return {
                "plain_p50_ms": sides["plain"].p50,
                "plain_p95_ms": sides["plain"].p95,
                "ts_p50_ms": sides["ts"].p50,
                "ts_p95_ms": sides["ts"].p95,
                "plain_rows_per_s": WRITE_BATCH / (sides["plain"].p50 / 1000),
                "ts_rows_per_s": WRITE_BATCH / (sides["ts"].p50 / 1000),
                "rounds": WRITE_ROUNDS,
            }

        return {
            "enqueue_batch_fast": one(enqueue_sides),
            "job_events_batch_insert": one(event_sides),
        }, enqueued
    finally:
        for c in conns.values():
            await c.close()
        for p in pools.values():
            await p.close()


async def cleanup_write_rows() -> None:
    """Remove the write bench's rows (FK cascade takes their events)."""
    counts = {}
    for label, dsn in (("plain", DSN_PLAIN), ("ts", DSN_TS)):
        conn = await connected(dsn)
        rows = await conn.fetch(
            f"DELETE FROM \"{SCHEMA}\".jobs WHERE actor = 'bench_actor' RETURNING id"  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        )
        counts[label] = len(rows)
        await conn.close()
    assert counts["plain"] == counts["ts"], counts


# ── A: retention drains (interleaved batch-by-batch) ─────────────────────


async def bench_prune() -> dict[str, Any]:
    """The daily prune's batch shape: candidate window + archive CTE write,
    10k jobs per batch, engines interleaved batch-by-batch until drained."""
    candidate_sql = _ARCHIVE_CANDIDATE_SQL.format(schema=SCHEMA)
    write_sql = _ARCHIVE_CTE_SQL.format(schema=SCHEMA)
    conns = {"plain": await connected(DSN_PLAIN), "ts": await connected(DSN_TS)}
    batches: dict[str, list[float]] = {"plain": [], "ts": []}
    moved: dict[str, int] = {"plain": 0, "ts": 0}
    try:
        while True:
            progressed = False
            for label in ("plain", "ts"):
                t0 = time.perf_counter()
                rows = await _run_prune_archive_batch(
                    conns[label],
                    candidate_sql=candidate_sql,
                    write_sql=write_sql,
                    status="succeeded",
                    retention=PRUNE_RETENTION,
                    size=BATCH,
                    archive_interval=ARCHIVE_RETENTION,
                    actor=None,
                    statement_timeout_ms=SWEEP_TIMEOUT_MS,
                    sweep_name="bench_prune",
                    sizer=None,
                )
                batches[label].append((time.perf_counter() - t0) * 1000)
                cnt = sum(int(r["cnt"]) for r in rows)
                moved[label] += cnt
                if cnt:
                    progressed = True
            if not progressed:
                break
        # Correctness: both engines drained exactly the cohort, the live rows
        # are gone, the archive rows + their attempts exist.
        for label, conn in conns.items():
            remaining = await conn.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".jobs '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
                "WHERE status = 'succeeded' AND finished_at < clock_timestamp() - $1::interval",
                PRUNE_RETENTION,
            )
            archived = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".jobs_archive')  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            attempts = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".job_attempts_archive')  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            assert remaining == 0, (label, remaining)
            assert archived >= PRUNE_COHORT, (label, archived)
            assert attempts >= PRUNE_COHORT, (label, attempts)
        # The drain empties every eligible row (the daily prune's own loop
        # shape): the cohort is the seeded aged-succeeded population.
        assert moved["plain"] == moved["ts"] == PRUNE_COHORT, moved

        def stats(xs: list[float]) -> dict[str, float]:
            return {
                "p50_ms": percentile(sorted(xs), 0.50),
                "p95_ms": percentile(sorted(xs), 0.95),
                "total_s": sum(xs) / 1000,
                "batches": len(xs),
            }

        return {
            "rows_target": PRUNE_COHORT,
            "batch_size": BATCH,
            "moved": moved,
            "per_batch_ms": {label: stats(xs) for label, xs in batches.items()},
            "drain_total_s": {label: sum(xs) / 1000 for label, xs in batches.items()},
        }
    finally:
        for conn in conns.values():
            await conn.close()


async def bench_expiry() -> dict[str, Any]:
    """The archive expiry sweep's batch shape, 10k/batch, engines
    interleaved batch-by-batch until the expired cohort is drained.

    Since the retention-policy floor, the two engines run DIFFERENT
    statements here by design — each is the exact statement production
    renders (``_compose_expiry_sql`` over
    ``taskq.timescale.retention_policy_floor``, the same probe and
    composition ``archive_expiry_sweep`` runs):

    * plain: no floor (the probe fails open — the timescale views do not
      exist), the full-range statement, the expired cohort deleted row by
      row.
    * ts: the armed policy's floor (drop_after = 365d on finished_at).
      The expired seed cohort is finished_at 370-399d old — every row
      below the floor, policy-owned — so the sweep's statement deletes
      nothing and the drain collapses to the empty-window index probe.
      The rows stay in the ts archive here only because the benchmark
      stops the background workers; the interplay tests pin that the
      policy run + sweep end state equals today's end state.
    """
    conns = {"plain": await connected(DSN_PLAIN), "ts": await connected(DSN_TS)}
    floors = {
        label: await retention_policy_floor(conn, SCHEMA, "jobs_archive", "finished_at")
        for label, conn in conns.items()
    }
    assert floors["plain"] is None, "the plain engine must have no policy floor"
    assert floors["ts"] is not None, "the ts engine must have an armed policy floor"
    sqls = {label: _compose_expiry_sql(SCHEMA, floor) for label, floor in floors.items()}
    batches: dict[str, list[float]] = {"plain": [], "ts": []}
    deleted: dict[str, int] = {"plain": 0, "ts": 0}
    try:
        while True:
            progressed = False
            for label in ("plain", "ts"):
                t0 = time.perf_counter()
                args: tuple[object, ...] = (
                    (BATCH,) if floors[label] is None else (BATCH, floors[label])
                )
                rows = await _run_prune_batch(
                    conns[label],
                    sqls[label],
                    *args,
                    statement_timeout_ms=SWEEP_TIMEOUT_MS,
                    sweep_name="bench_expiry",
                    sizer=None,
                )
                batches[label].append((time.perf_counter() - t0) * 1000)
                cnt = sum(int(r["cnt"]) for r in rows)
                deleted[label] += cnt
                if cnt:
                    progressed = True
            if not progressed:
                break
        for label, conn in conns.items():
            remaining = await conn.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".jobs_archive '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
                "WHERE expire_at < clock_timestamp()"
            )
            if label == "plain":
                assert remaining == 0, (label, remaining)
            else:
                # The policy-owned tail: untouched by the sweep, dropped by
                # the policy's chunk runs when the background workers run.
                assert remaining == ARCHIVE_EXPIRED, (label, remaining)
        assert deleted["plain"] == ARCHIVE_EXPIRED, deleted
        assert deleted["ts"] == 0, deleted

        def stats(xs: list[float]) -> dict[str, float]:
            return {
                "p50_ms": percentile(sorted(xs), 0.50),
                "p95_ms": percentile(sorted(xs), 0.95),
                "total_s": sum(xs) / 1000,
                "batches": len(xs),
            }

        return {
            "rows_target": ARCHIVE_EXPIRED,
            "batch_size": BATCH,
            "deleted": deleted,
            "per_batch_ms": {label: stats(xs) for label, xs in batches.items()},
            "drain_total_s": {label: sum(xs) / 1000 for label, xs in batches.items()},
        }
    finally:
        for conn in conns.values():
            await conn.close()


async def bench_events() -> dict[str, Any]:
    """The event TTL sweep's tick shape: ONE 10k-row batch per call
    (sweep_expired_events), engines interleaved call-by-call.

    The sweep probes the policy floor itself (once per call, the
    production wiring): plain fails open to None and deletes the full
    aged cohort row by row; the ts side's armed policy (drop_after = 7d
    on occurred_at) owns the whole aged cohort (8-29d old, every row
    below the floor), so the ts tick deletes nothing — the measured cost
    collapses to the probe plus the empty-window index scan. The aged
    rows stay in the ts table only because the benchmark stops the
    background workers; the interplay tests pin the policy-run + sweep
    end-state parity.
    """
    conns = {"plain": await connected(DSN_PLAIN), "ts": await connected(DSN_TS)}
    batches: dict[str, list[float]] = {"plain": [], "ts": []}
    deleted: dict[str, int] = {"plain": 0, "ts": 0}
    try:
        while True:
            progressed = False
            for label in ("plain", "ts"):
                t0 = time.perf_counter()
                cnt = await sweep_expired_events(
                    conns[label], schema=SCHEMA, retention=EVENT_RETENTION
                )
                batches[label].append((time.perf_counter() - t0) * 1000)
                deleted[label] += cnt
                if cnt:
                    progressed = True
            if not progressed:
                break
        # Aged events are the first quarter of the seed, every 25th an outbox
        # row the sweep keeps at this age (deleted only at 100x retention):
        # exactly aged - aged//25 deletable — on the PLAIN side. On the ts
        # side the armed policy owns the whole aged cohort (below the
        # now-7d floor): the sweep deletes nothing and the cohort remains,
        # dropped by the policy's chunk runs when the background workers
        # run.
        aged = SEED_EVENTS // 4
        expected = aged - aged // 25
        for label, conn in conns.items():
            stale = await conn.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".job_events '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
                "WHERE occurred_at < clock_timestamp() - $1::interval "
                "AND NOT (kind = 'state_change' AND COALESCE(detail->>'reason', '') = 'lock_expired')",
                EVENT_RETENTION,
            )
            if label == "plain":
                assert stale == 0, (label, stale)
            else:
                assert stale == expected, (label, stale)
        assert deleted["plain"] == expected, (deleted, expected)
        assert deleted["ts"] == 0, (deleted, expected)

        def stats(xs: list[float]) -> dict[str, float]:
            return {
                "p50_ms": percentile(sorted(xs), 0.50),
                "p95_ms": percentile(sorted(xs), 0.95),
                "total_s": sum(xs) / 1000,
                "batches": len(xs),
            }

        return {
            "rows_target": expected,
            "batch_size": BATCH,
            "deleted": deleted,
            "per_batch_ms": {label: stats(xs) for label, xs in batches.items()},
            "drain_total_s": {label: sum(xs) / 1000 for label, xs in batches.items()},
        }
    finally:
        for conn in conns.values():
            await conn.close()


# ── A4: vacuum/cleanup aftermath ─────────────────────────────────────────


async def aftermath_snapshot(dsn: str) -> dict[str, Any]:
    """Dead tuples + relation sizes after the deletes.

    Plain side: per-relation ``pg_stat_user_tables`` + ``pg_total_relation_size``.
    Hypertable side: the same quantities chunk-summed (the parent of a
    hypertable holds no data) plus TimescaleDB's own ``hypertable_size``.
    """
    conn = await connected(dsn)
    try:
        stats = {
            r["relname"]: {
                "n_live_tup": r["n_live_tup"],
                "n_dead_tup": r["n_dead_tup"],
                "last_autovacuum": str(r["last_autovacuum"]),
            }
            for r in await conn.fetch(
                "SELECT relname, n_live_tup, n_dead_tup, last_autovacuum "
                "FROM pg_stat_user_tables WHERE schemaname = $1 "
                "AND relname = ANY($2)",
                SCHEMA,
                ["jobs", "jobs_archive", "job_events", "job_attempts", "job_attempts_archive"],
            )
        }
        sizes = {
            r["relname"]: r["bytes"]
            for r in await conn.fetch(
                "SELECT relname, sum(pg_total_relation_size(format('%I.%I', schemaname, relname)))::bigint AS bytes "
                "FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = ANY($2) GROUP BY relname",
                SCHEMA,
                ["jobs", "jobs_archive", "job_events"],
            )
        }
        is_ht = await conn.fetchval(
            "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = $1", SCHEMA
        )
        out: dict[str, Any] = {
            "pg_stat_user_tables": stats,
            "total_relation_size": sizes,
            "engine": "hypertable" if is_ht else "plain",
        }
        if is_ht:
            out["hypertable_size"] = {
                r["t"]: r["bytes"]
                for r in await conn.fetch(
                    "SELECT x.hypertable_name AS t, hypertable_size("
                    "format('%I.%I', x.hypertable_schema, x.hypertable_name)::regclass) AS bytes "
                    "FROM (SELECT DISTINCT hypertable_schema, hypertable_name "
                    "FROM timescaledb_information.chunks WHERE hypertable_schema = $1) x",
                    SCHEMA,
                )
            }
            out["chunk_dead_tup"] = {
                r["t"]: r["dead"]
                for r in await conn.fetch(
                    "SELECT c.hypertable_name AS t, sum(s.n_dead_tup)::bigint AS dead "
                    "FROM timescaledb_information.chunks c "
                    "JOIN pg_stat_user_tables s ON s.schemaname = c.chunk_schema "
                    "AND s.relname = c.chunk_name "
                    "WHERE c.hypertable_schema = $1 GROUP BY 1",
                    SCHEMA,
                )
            }
            out["chunk_counts"] = {
                r["t"]: r["chunks"]
                for r in await conn.fetch(
                    "SELECT hypertable_name AS t, count(*)::int AS chunks "
                    "FROM timescaledb_information.chunks WHERE hypertable_schema = $1 GROUP BY 1",
                    SCHEMA,
                )
            }
        return out
    finally:
        await conn.close()


# ── Reporting ────────────────────────────────────────────────────────────


def print_dashboard(results: list[dict[str, Any]]) -> None:
    print(
        f"\n{'dashboard query (1M jobs)':<36} {'plain p50':>11} {'plain p95':>11} "
        f"{'ts p50':>11} {'ts p95':>11} {'plainΔ':>8}  ok"
    )
    print("-" * 104)
    for r in results:
        flag = "OK" if r["rows_identical"] else "MISMATCH!"
        delta = r["plain_faster_pct"]
        print(
            f"{r['name']:<36} {ms(r['plain_p50_ms']):>11} {ms(r['plain_p95_ms']):>11} "
            f"{ms(r['ts_p50_ms']):>11} {ms(r['ts_p95_ms']):>11} {delta:>7.1f}%  {flag}"
        )


def print_drain(title: str, data: dict[str, Any]) -> None:
    print(f"\n{title}  (batch_size {data['batch_size']:,}, target {data['rows_target']:,} rows)")
    print(f"  {'engine':<8} {'batches':>7} {'p50/batch':>11} {'p95/batch':>11} {'drain total':>12}")
    for label in ("plain", "ts"):
        s = data["per_batch_ms"][label]
        print(
            f"  {label:<8} {s['batches']:>7} {ms(s['p50_ms']):>11} {ms(s['p95_ms']):>11} "
            f"{s['total_s']:>10.1f}s"
        )


def print_write(data: dict[str, dict[str, float]]) -> None:
    print(f"\nwrite path (batch {WRITE_BATCH:,}, {WRITE_ROUNDS} interleaved rounds)")
    print(f"  {'path':<28} {'plain p50':>11} {'ts p50':>11} {'plain rows/s':>13} {'ts rows/s':>13}")
    for name, s in data.items():
        print(
            f"  {name:<28} {ms(s['plain_p50_ms']):>11} {ms(s['ts_p50_ms']):>11} "
            f"{s['plain_rows_per_s']:>13,.0f} {s['ts_rows_per_s']:>13,.0f}"
        )


# ── Scale sweep (--scale-sweep): headline shapes at multiple scales ──────


async def run_scale(scale: int, idx: int, total: int) -> dict[str, Any]:
    """One scale point: a fresh identical container pair, seeded, asserted,
    headline-shaped, torn down.  Same machinery as the single-scale run."""
    global SEED_JOBS, SEED_ARCHIVE, SEED_EVENTS
    SEED_JOBS = SEED_ARCHIVE = SEED_EVENTS = scale
    t0 = time.perf_counter()
    base = datetime.now(UTC).replace(microsecond=0)
    print(
        f"\n=== [{idx}/{total}] scale {scale:,}: jobs {scale:,} / archive {scale:,} / "
        f"events {scale:,} / prune cohort {scale // 10:,} (base {base.isoformat()}) ===",
        flush=True,
    )
    try:
        start_containers()
        await wait_ready(DSN_PLAIN, "plain engine")
        await wait_ready(DSN_TS, "ts engine")
        ts_report = await setup_engine(DSN_TS, hypertables=True)
        await setup_engine(DSN_PLAIN, hypertables=False)
        print("    seeding plain engine", flush=True)
        seed_plain = await seed_engine(DSN_PLAIN, base)
        print("    seeding ts engine", flush=True)
        seed_ts = await seed_engine(DSN_TS, base)
        print("    correctness asserts on the seed", flush=True)
        seed_assert = await assert_seed_identical()
        print(
            f"    counts match; hypertables: {seed_assert['hypertables']} "
            f"chunks: {seed_assert['chunk_counts']}",
            flush=True,
        )
        print("    headline dashboard shapes", flush=True)
        dashboard = await bench_dashboard(only=SWEEP_SHAPES)
        print("    retention drains (interleaved batch-by-batch)", flush=True)
        prune = await bench_prune()
        print_drain("prune (aged succeeded jobs -> archive)", prune)
        events = await bench_events()
        print_drain("event TTL (aged job_events)", events)
        return {
            "scale": scale,
            "seed": {
                "jobs": SEED_JOBS,
                "jobs_archive": SEED_ARCHIVE,
                "jobs_archive_expired_cohort": ARCHIVE_EXPIRED,
                "job_events": SEED_EVENTS,
                "job_attempts": PRUNE_COHORT,  # seeded 1:1 with the prune cohort at every scale
                "prune_cohort": PRUNE_COHORT,
                "job_events_aged": SEED_EVENTS // 4,
            },
            "seed_timings": {"plain": seed_plain, "ts": seed_ts},
            "seed_assert": seed_assert,
            "hypertable_conversion": ts_report,
            "dashboard": dashboard,
            "retention": {"prune": prune, "event_ttl": events},
            "wall_s": time.perf_counter() - t0,
        }
    finally:
        # Tear down between scales: the next point starts from a fresh pair,
        # so no scale inherits another's dead tuples or drain state.
        if not _KEEP_CONTAINERS:
            stop_containers()


def analyze_sweep(scale_results: list[dict[str, Any]]) -> dict[str, Any]:
    """The crossover story: per-scale ratios for the three headline shapes."""
    points: list[dict[str, Any]] = []
    for r in scale_results:
        page = next(x for x in r["dashboard"] if x["name"] == "archive_recent_finished_p51")
        count = next(x for x in r["dashboard"] if x["name"] == "jobs_count_all_statuses")
        prune, ev = (
            r["retention"]["prune"]["per_batch_ms"],
            r["retention"]["event_ttl"]["per_batch_ms"],
        )
        points.append(
            {
                "archive_rows": r["scale"],
                "archive_page_plain_p50_ms": page["plain_p50_ms"],
                "archive_page_ts_p50_ms": page["ts_p50_ms"],
                "archive_page_ts_speedup": (
                    page["plain_p50_ms"] / page["ts_p50_ms"] if page["ts_p50_ms"] else None
                ),
                "archive_page_ts_wins": page["ts_p50_ms"] < page["plain_p50_ms"],
                "parity_count_plain_p50_ms": count["plain_p50_ms"],
                "parity_count_ts_p50_ms": count["ts_p50_ms"],
                "prune_p50_batch_plain_ms": prune["plain"]["p50_ms"],
                "prune_p50_batch_ts_ms": prune["ts"]["p50_ms"],
                "prune_penalty_ratio_batch": (
                    prune["ts"]["p50_ms"] / prune["plain"]["p50_ms"]
                    if prune["plain"]["p50_ms"]
                    else None
                ),
                "prune_drain_total_ratio": (
                    r["retention"]["prune"]["drain_total_s"]["ts"]
                    / r["retention"]["prune"]["drain_total_s"]["plain"]
                    if r["retention"]["prune"]["drain_total_s"]["plain"]
                    else None
                ),
                "event_ttl_p50_batch_plain_ms": ev["plain"]["p50_ms"],
                "event_ttl_p50_batch_ts_ms": ev["ts"]["p50_ms"],
                "event_ttl_ratio_batch": (
                    ev["ts"]["p50_ms"] / ev["plain"]["p50_ms"] if ev["plain"]["p50_ms"] else None
                ),
            }
        )
    winners = [p for p in points if p["archive_page_ts_wins"]]
    losers = [p for p in points if not p["archive_page_ts_wins"]]
    # The headline penalty metric is the DRAIN-TOTAL ratio, not the per-batch
    # p50: at scales where the cohort fits in one batch the per-batch samples
    # are [real_batch, ~0 empty trailing batch] and the p50 lands on the empty
    # one.  Batch counts are identical across engines, so total_s ratio IS the
    # mean per-real-batch ratio.
    ratios = [p["prune_drain_total_ratio"] for p in points if p["prune_drain_total_ratio"]]
    if winners and losers:
        band = (
            f"archive page: hypertable wins from {winners[0]['archive_rows']:,} rows "
            f"(smallest losing point {losers[-1]['archive_rows']:,}); below that, plain"
        )
    elif winners:
        band = (
            f"archive page: hypertable wins at EVERY swept scale from "
            f"{winners[0]['archive_rows']:,} rows up"
        )
    else:
        band = "archive page: hypertable never won in the swept range"
    if len(ratios) >= 2:
        trend = (
            "wider"
            if ratios[-1] > ratios[0] * 1.25
            else "narrower"
            if ratios[-1] < ratios[0] / 1.25
            else "~flat"
        )
        prune_story = (
            f"prune drain-total penalty {ratios[0]:.1f}x at {points[0]['archive_rows']:,} rows → "
            f"{ratios[-1]:.1f}x at {points[-1]['archive_rows']:,} rows ({trend} with scale)"
        )
    else:
        prune_story = f"prune penalty ratio {ratios[0]:.1f}x (single scale point)"
    return {
        "points": points,
        "archive_page_crossover": band,
        "prune_penalty_trend": prune_story,
        "prune_penalty_ratios_drain_total": ratios,
        "recommendation": (
            f"{band}. {prune_story}. The hypertable's retention writes cost a "
            f"~constant RATIO (~{sum(ratios) / len(ratios):.0f}x drain time at batch 10k) "
            f"at every swept scale, while its archive-page read win exists from the "
            f"smallest swept scale — so the trade-off above ~100k archive rows is decided "
            f"by read/write mix, not by scale: read-heavy admin surfaces win on the "
            f"hypertable at any size; write-heavy retention drains pay a flat ~8x per "
            f"batch for chunk-drop retention. Below {min(p['archive_rows'] for p in points):,} "
            f"archive rows neither engine showed a measured advantage, and plain stays "
            f"the simpler default there."
        ),
    }


def print_sweep(points: list[dict[str, Any]]) -> None:
    print(
        f"\n{'archive rows':>13} | {'page plain':>11} {'page ts':>11} {'ts speedup':>10} | "
        f"{'prune plain/batch':>18} {'prune ts/batch':>15} {'drain x':>8} | "
        f"{'ttl plain':>10} {'ttl ts':>10} {'ratio':>6} | {'count plain':>11} {'count ts':>11}"
    )
    print("-" * 152)
    for p in points:
        speedup = f"{p['archive_page_ts_speedup']:.2f}x" if p["archive_page_ts_speedup"] else "-"
        print(
            f"{p['archive_rows']:>13,} | {ms(p['archive_page_plain_p50_ms']):>11} "
            f"{ms(p['archive_page_ts_p50_ms']):>11} {speedup:>10} | "
            f"{ms(p['prune_p50_batch_plain_ms']):>18} {ms(p['prune_p50_batch_ts_ms']):>15} "
            f"{p['prune_drain_total_ratio']:>7.1f}x | "
            f"{ms(p['event_ttl_p50_batch_plain_ms']):>10} {ms(p['event_ttl_p50_batch_ts_ms']):>10} "
            f"{p['event_ttl_ratio_batch']:>5.1f}x | "
            f"{ms(p['parity_count_plain_p50_ms']):>11} {ms(p['parity_count_ts_p50_ms']):>11}"
        )


async def main_sweep(scales: list[int]) -> None:
    global DASHBOARD_ROUNDS
    DASHBOARD_ROUNDS = max(3, DASHBOARD_ROUNDS)  # an odd count keeps the median an actual round
    t_start = time.perf_counter()
    print(
        f"scale sweep: archive=events=jobs per point, scales "
        f"{', '.join(f'{s:,}' for s in scales)}; fresh container pair per scale; "
        f"headline shapes only (prune drain, event TTL drain, archive newest-first "
        f"page, parity count)",
        flush=True,
    )
    scale_results: list[dict[str, Any]] = []
    for idx, scale in enumerate(scales, 1):
        scale_results.append(await run_scale(scale, idx, len(scales)))

    analysis = analyze_sweep(scale_results)
    print_sweep(analysis["points"])
    print(f"\ncrossover: {analysis['archive_page_crossover']}")
    print(f"prune penalty: {analysis['prune_penalty_trend']}")
    print(f"recommendation: {analysis['recommendation']}")

    results = {
        "schema": 2,  # v2: the scale dimension (per-scale points, see "scales")
        "mode": "scale-sweep",
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": {
            "image": IMAGE,
            "engines": {
                "plain": f"{CONTAINER_PLAIN}:{PORT_PLAIN} (no hypertables)",
                "ts": f"{CONTAINER_TS}:{PORT_TS} (enable_hypertables + retention policies)",
            },
            "headline_shapes": sorted(SWEEP_SHAPES),
            "sweep_note": "jobs = archive = events = scale per point (prune cohort 10% of "
            "scale, aged events a quarter of it); fresh identical container pair per "
            "scale, torn down between scales; batch 10000; interleaved engines",
            "wall_s": time.perf_counter() - t_start,
        },
        "scales": scale_results,
        "crossover": analysis,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / RESULTS_SWEEP_NAME
    path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"\nresults → {path}")
    print(f"(wall {time.perf_counter() - t_start:.0f}s)")


# ── Main ─────────────────────────────────────────────────────────────────


async def main() -> None:
    global SEED_JOBS, SEED_ARCHIVE, SEED_EVENTS, PRUNE_ROWS, DASHBOARD_ROUNDS, WRITE_ROUNDS
    ap = argparse.ArgumentParser(description="TimescaleDB vs plain Postgres trade-off benchmark")
    ap.add_argument("--jobs", type=int, default=SEED_JOBS)
    ap.add_argument("--archive", type=int, default=SEED_ARCHIVE)
    ap.add_argument("--events", type=int, default=SEED_EVENTS)
    ap.add_argument("--dashboard-rounds", type=int, default=DASHBOARD_ROUNDS)
    ap.add_argument("--write-rounds", type=int, default=WRITE_ROUNDS)
    ap.add_argument(
        "--scale-sweep",
        action="store_true",
        help="run the headline shapes at multiple archive/event scales (mutually "
        "exclusive with --jobs/--archive/--events) and write the schema-2 sweep JSON",
    )
    ap.add_argument(
        "--scales",
        type=str,
        default=SWEEP_SCALES_DEFAULT,
        help="comma-separated archive-row scales for --scale-sweep",
    )
    ap.add_argument(
        "--keep-containers",
        action="store_true",
        help="leave the two benchmark containers running after the run",
    )
    args = ap.parse_args()
    global _KEEP_CONTAINERS
    _KEEP_CONTAINERS = args.keep_containers
    if args.scale_sweep:
        if (
            args.jobs != SEED_JOBS
            or args.archive != SEED_ARCHIVE
            or args.events != SEED_EVENTS
            or args.write_rounds != WRITE_ROUNDS
        ):
            ap.error("--scale-sweep drives --jobs/--archive/--events/--write-rounds itself")
        await main_sweep([int(s) for s in args.scales.split(",")])
        return
    SEED_JOBS, SEED_ARCHIVE, SEED_EVENTS = args.jobs, args.archive, args.events
    DASHBOARD_ROUNDS, WRITE_ROUNDS = args.dashboard_rounds, args.write_rounds

    t_start = time.perf_counter()
    base = datetime.now(UTC).replace(microsecond=0)
    print(
        f"base instant {base.isoformat()} — seed: {SEED_JOBS:,} jobs, "
        f"{SEED_ARCHIVE:,} archive, {SEED_EVENTS:,} events on BOTH engines",
        flush=True,
    )

    print("[1/9] starting containers", flush=True)
    start_containers()
    await wait_ready(DSN_PLAIN, "plain engine")
    await wait_ready(DSN_TS, "ts engine")

    print("[2/9] schema + hypertable conversion (ts engine, via enable_hypertables)", flush=True)
    ts_report = await setup_engine(DSN_TS, hypertables=True)
    await setup_engine(DSN_PLAIN, hypertables=False)

    print("[3/9] seeding plain engine", flush=True)
    seed_plain = await seed_engine(DSN_PLAIN, base)
    print("[3/9] seeding ts engine", flush=True)
    seed_ts = await seed_engine(DSN_TS, base)

    print("[4/9] correctness asserts on the seed", flush=True)
    seed_assert = await assert_seed_identical()
    print(
        f"    counts match; hypertables: {seed_assert['hypertables']} "
        f"chunks: {seed_assert['chunk_counts']}",
        flush=True,
    )

    print("[5/9] dashboard queries", flush=True)
    dashboard = await bench_dashboard()

    print("[6/9] write path", flush=True)
    write, _enqueued = await bench_write()
    await cleanup_write_rows()
    print_write(write)

    print("[7/9] retention drains (interleaved batch-by-batch)", flush=True)
    prune = await bench_prune()
    print_drain("prune (aged succeeded jobs -> archive)", prune)
    expiry = await bench_expiry()
    print_drain("archive expiry (expired archive rows hard-deleted)", expiry)
    events = await bench_events()
    print_drain("event TTL (aged job_events)", events)

    print("[8/9] aftermath (dead tuples + sizes)", flush=True)
    after = {"plain": await aftermath_snapshot(DSN_PLAIN), "ts": await aftermath_snapshot(DSN_TS)}
    for label in ("plain", "ts"):
        print(f"    {label}: {json.dumps(after[label]['pg_stat_user_tables'], default=str)}")

    print("[9/9] teardown + results", flush=True)
    if not _KEEP_CONTAINERS:
        stop_containers()

    results = {
        "schema": 1,
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": {
            "image": IMAGE,
            "engines": {
                "plain": f"{CONTAINER_PLAIN}:{PORT_PLAIN} (no hypertables)",
                "ts": f"{CONTAINER_TS}:{PORT_TS} (enable_hypertables + retention policies)",
            },
            "hypertable_conversion": ts_report,
            "seed": {
                "jobs": SEED_JOBS,
                "jobs_archive": SEED_ARCHIVE,
                "jobs_archive_expired_cohort": ARCHIVE_EXPIRED,
                "job_events": SEED_EVENTS,
                "job_attempts": SEED_ATTEMPTS,
                "prune_cohort": PRUNE_COHORT,
                "job_events_aged": SEED_EVENTS // 4,
                "retention_args": {
                    "prune_succeeded": str(PRUNE_RETENTION),
                    "archive": str(ARCHIVE_RETENTION),
                    "events": str(EVENT_RETENTION),
                },
                "batch_size": BATCH,
                "sweep_statement_timeout_ms": SWEEP_TIMEOUT_MS,
                "note": "both engines byte-identical (fixed base instant); "
                "VACUUM not forced — ANALYZE only, both engines alike",
            },
            "seed_timings": {"plain": seed_plain, "ts": seed_ts},
            "seed_assert": seed_assert,
            "wall_s": time.perf_counter() - t_start,
        },
        "dashboard": dashboard,
        "write": write,
        "retention": {"prune": prune, "archive_expiry": expiry, "event_ttl": events},
        "aftermath": after,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / RESULTS_NAME
    path.write_text(json.dumps(results, indent=2, default=str) + "\n")

    print_dashboard(dashboard)
    print(f"\nresults → {path}")
    print(f"(wall {time.perf_counter() - t_start:.0f}s)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if not _KEEP_CONTAINERS:
            stop_containers()
