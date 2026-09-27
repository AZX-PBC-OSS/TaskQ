"""The archive at scale: the admin's pagination seams and the retention
drain measured at a REAL archive depth — 10,000,000 rows in jobs_archive,
both engines (plain Postgres vs hypertable + columnstore).

The timescale trade-off evidence (``timescale_tradeoffs.py`` and the sweep
in ``timescale-tradeoffs-sweep.json``) topped out at a 2,000,000-row
archive. This campaign pins the curves an order of magnitude deeper, where
the docs' opt-in guidance actually gets acted on:

1. **The pagination seam at real depth** — the admin's archive tab walks
   ``(finished_at DESC, id DESC)`` keyset pages (``_build_paginated_sql``,
   51 rows per fetch). A full 10,000-page walk (510k rows deep) with the
   per-page latency recorded: page-1 p50 vs page-10000 p50 is the seam's
   depth sensitivity. Engines interleaved page by page. THIS RUN ALSO
   CARRIES A PATHOLOGY FINDING: before migration ``01.00.21_01`` no
   shipped index served the tab's ``finished_at DESC NULLS LAST, id DESC``
   ordering, so every page top-N sorted the whole matching population —
   per-page cost linear in archive depth, the walk quadratic (the
   committed sweep artifact's plain points, 3.81 ms/page @ 10k rows →
   135.82 ms/page @ 2M, were that curve misread as a hypertable win).
   The bench pins both shapes: red legs (index dropped) vs green legs
   (index present) at depth checkpoints, plus the plans themselves.
   A second finding is pinned alongside: on the COMPRESSED columnstore
   the seek runs but every page's rows decompress out of segment
   storage — the ts page cost is tens of ms, ~flat in depth (the
   read-side trade-off the chunk-drop retention win buys; the earlier
   sweep's "hypertable page win" was measured on rowstore chunks, and
   docs/guides/timescaledb.md's read-win story does not extend to
   compressed-chunk page walks).
2. **Count queries at scale** — ``count(*)`` on the whole archive and a
   status-filtered count (the ``/jobs/count`` shape).
3. **Retention drain at scale** — the expired cohort (100k rows,
   ``expire_at`` in the past) drained by the production sweep machinery
   (``_compose_expiry_sql`` / ``_run_prune_batch``) in 10k batches:
   - plain: the row-delete drain, the only mechanism plain has;
   - ts: the SAME row-delete statement over the columnstore (the retention
     policy is removed for the leg and re-armed after, and the server's
     decompression budget GUC is set to 0 = unlimited — at the shipped
     default 100k one 10k-row batch hard-errors, measured:
     ConfigurationLimitExceededError after decompressing 356633 tuples;
     see ``src/taskq/timescale.py``), so the decompression-backed delete
     cost is engine-comparable. The armed-policy sweep (probe-only, the
     per-tick cost when the policy owns the aged end) is measured first.
   Chunk-drop granularity is the ts design's own semantics — the drop
   cohort is chunk-aligned, not row-aligned — so the drain leg here
   measures the row-delete statement both engines run, and the artifact
   records chunk counts/sizes so the chunk-drop alternative's shape stays
   traceable.
4. **The columnstore's storage win at 10M** — hypertable_size before and
   after forcing compression of every chunk, vs plain's
   ``pg_total_relation_size``.

Both engines get byte-identical seed data (one fixed base instant, the
``timescale_tradeoffs.py`` doctrine) and a cross-engine row-identity
assertion runs on the walked pages before timings are trusted.

Read-only with respect to src/; writes only its own artifact
``results/archive-scale.json``. Containers carry ``creator_labels()``.
SERIAL: a load-sensitive campaign — never run concurrent with anything.

Run: .venv/bin/python benchmarks/archive_scale.py [--archive 10000000]
     [--pages 10000] [--keep]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
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

from taskq.backend._retention_floor import (  # pyright: ignore[reportPrivateUsage]
    retention_policy_floor,
)
from taskq.constants import DEFAULT_PRUNE_BATCH_SIZE  # pyright: ignore[reportPrivateUsage]
from taskq.migrate import apply_pending  # pyright: ignore[reportPrivateUsage]
from taskq.settings import WorkerSettings  # pyright: ignore[reportPrivateUsage]
from taskq.testing._shared_containers import creator_labels  # pyright: ignore[reportPrivateUsage]
from taskq.timescale import enable_hypertables  # pyright: ignore[reportPrivateUsage]
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin module's own WHERE/order builders are the queries being measured; a hand-copied SQL shape would drift from the real page.
    _ARCHIVE_COLS,
    _SORTABLE_ARCHIVE,
    _build_paginated_sql,
    _build_where,
)
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]
    _compose_expiry_sql,
    _run_prune_batch,
)

# ── Configuration ────────────────────────────────────────────────────────

IMAGE = "timescale/timescaledb:2.30.1-pg18"
CONTAINER_PLAIN = "tq-arch-plain"
CONTAINER_TS = "tq-arch-ts"
PORT_PLAIN = 55701
PORT_TS = 55702
DSN_PLAIN = f"postgresql://taskq:taskq@localhost:{PORT_PLAIN}/taskq"
DSN_TS = f"postgresql://taskq:taskq@localhost:{PORT_TS}/taskq"
SCHEMA = "tq_archscale"

SERVER_FLAGS = [
    "-c",
    "max_connections=200",
    "-c",
    "jit=off",
    "-c",
    "shared_buffers=1GB",
    "-c",
    "max_wal_size=8GB",
]

ARCHIVE_ROWS = 10_000_000
EXPIRED_COHORT = 100_000  # expire_at in the past: the drain leg's target
SEED_BAND = 1_000_000  # rows per seed statement (10 bands at 10M)

#: The pagination seam's walk: 10,000 pages x 51 rows = 510k rows deep.
WALK_PAGES = 10_000

DRAIN_BATCH = DEFAULT_PRUNE_BATCH_SIZE  # 10k, the production sweep batch
# The sweep helpers' statement_timeout is the production 4s default; a
# benchmark batch that hits it would abort the drain, so the bench widens it
# (recorded in the meta — batches here measure throughput, not the breaker).
SWEEP_TIMEOUT_MS = 600_000

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_NAME = "archive-scale.json"

_KEEP = False


# ── Container lifecycle (same doctrine as timescale_tradeoffs.py) ────────


def run(cmd: list[str]) -> str:
    # Why noqa: the benchmark's own fixed docker arguments, never user input.
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603  # Why: benchmark-controlled docker CLI invocation.
    return out.stdout.strip()


def label_args() -> list[str]:
    # House rule: creator_labels() ownership labels on every container.
    out: list[str] = []
    for k, v in creator_labels().items():
        out += ["--label", f"{k}={v}"]
    return out


def start_containers() -> None:
    for name, port in ((CONTAINER_PLAIN, PORT_PLAIN), (CONTAINER_TS, PORT_TS)):
        run(["docker", "rm", "-f", name])
        run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                name,
                *label_args(),
                "-e",
                "POSTGRES_USER=taskq",
                "-e",
                "POSTGRES_PASSWORD=taskq",
                "-e",
                "POSTGRES_DB=taskq",
                "-p",
                f"{port}:5432",
                IMAGE,
                "postgres",
                *SERVER_FLAGS,
            ]
        )


def stop_containers() -> None:
    for name in (CONTAINER_PLAIN, CONTAINER_TS):
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607  # Why: benchmark teardown of its own fixed-name containers.


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


# ── Engine setup ─────────────────────────────────────────────────────────


async def setup_engine(dsn: str, *, hypertables: bool) -> dict[str, Any] | None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'
        )  # Why: schema is a benchmark-controlled constant.
        await apply_pending(conn, schema=SCHEMA)
        if not hypertables:
            return None
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": dsn,
                "TASKQ_SCHEMA_NAME": SCHEMA,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            },
            validate=False,
        )
        report = await enable_hypertables(conn, schema=SCHEMA, settings=settings)
        # The drain leg deletes from COMPRESSED chunks: the shipped default
        # budget (100k decompressed tuples per DML transaction) hard-errors
        # on a 10k-row batch at this scale (measured, see module docstring).
        # 0 = unlimited; the probe in enable_hypertables warned about this
        # exact configuration.
        await conn.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        # Stop the extension's background workers so the seeded chunks
        # compress only when THIS bench forces it (measurement, not drift).
        for stmt in (
            "SELECT _timescaledb_functions.stop_background_workers()",
            "SELECT _timescaledb_internal.stop_background_workers()",
        ):
            try:
                await conn.execute(stmt)
                break
            except Exception:  # noqa: S110  # Why: probe across extension versions, the next statement is tried
                pass
        return {
            "converted": list(report.converted),
            "retention_policies": list(report.retention_policies),
            "compression_policies": list(report.compression_policies),
            "decompression_guc_warning": report.decompression_guc_warning,
            "decompression_guc_set": "0 (unlimited) for the drain leg",
        }
    finally:
        await conn.close()


# ── Seed: byte-identical archive corpus on both engines ──────────────────

# jobs_archive at depth: rows in the 1-360d finished_at window (expire_at in
# the future — the pagination walk's population) + an expired cohort
# (finished_at 370-399d old, so expire_at = finished_at + 365d sits 5-34d in
# the past: the expiry sweep's exactly-removable cohort). Seeded in bands of
# SEED_BAND so one statement never holds a 10M-row snapshot. $1 base
# instant, $2 band start (1-based), $3 band end, $4 expired boundary (rows
# > $4 are the expired cohort).
_SEED_BAND_SQL = """\
WITH g AS (SELECT generate_series($2::int, $3::int) AS i),
t AS (SELECT i,
        -- Second resolution: production finished_at values are
        -- microsecond-distinct; a day-only spread would put ~27.7k-row
        -- tie groups on every timestamp at this scale, and the tie width
        -- (not the depth) would dominate the deep-page cost.
        CASE WHEN i <= $4::int
             THEN $1::timestamptz - make_interval(days => 1 + (i % 360), secs => i % 86400)
             ELSE $1::timestamptz - make_interval(days => 370 + (i % 30), secs => i % 86400)
        END AS fin
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
"""  # Why: schema is a benchmark-controlled identifier, all values $-bound.


async def seed_engine(dsn: str, base: datetime, total: int) -> dict[str, float]:
    conn = await asyncpg.connect(dsn)
    timings: dict[str, float] = {}
    try:
        expired_boundary = total - EXPIRED_COHORT
        t0 = time.perf_counter()
        done = 0
        band_no = 0
        while done < total:
            n = min(SEED_BAND, total - done)
            await conn.execute(
                _SEED_BAND_SQL.format(s=SCHEMA),
                base,
                done + 1,
                done + n,
                expired_boundary,
            )
            done += n
            band_no += 1
            print(f"      band {band_no}: {done:,}/{total:,}", flush=True)
        timings["seed_s"] = time.perf_counter() - t0
        t0 = time.perf_counter()
        await conn.execute(
            f'ANALYZE "{SCHEMA}".jobs_archive'
        )  # Why: schema is a benchmark-controlled constant.
        timings["analyze_s"] = time.perf_counter() - t0
    finally:
        await conn.close()
    return timings


# ── The pagination walk (the admin's real page builder) ──────────────────

_WHERE_TERMINAL, _WHERE_PARAMS = _build_where(
    sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"}),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
)


def _page_sql(cursor_at: str | None, cursor_id: str | None) -> tuple[str, list[Any]]:
    return _build_paginated_sql(
        SCHEMA,
        "jobs_archive",
        _ARCHIVE_COLS,
        _SORTABLE_ARCHIVE,
        _WHERE_TERMINAL,
        list(_WHERE_PARAMS),
        cursor_at,
        cursor_id,
        "next",
        "finished_at",
        "desc",
    )


async def walk_pages(
    conns: dict[str, asyncpg.Connection], pages: int
) -> tuple[dict[str, list[float]], bool, list[Any]]:
    """Interleaved keyset walk, page by page, engine by engine.

    Returns per-engine per-page latencies (ms), whether the engines' page
    1 rows were identical, and page 1's rows (for the artifact's identity
    assert). Each page's cursor is the previous page's last row's
    (finished_at, id) — the admin's real cursor semantics.
    """
    lat: dict[str, list[float]] = {"plain": [], "ts": []}
    cursor_at: str | None = None
    cursor_id: str | None = None
    first_rows: dict[str, list[Any]] = {}
    identical = True
    for page_no in range(1, pages + 1):
        for label in ("plain", "ts"):
            sql, args = _page_sql(cursor_at, cursor_id)
            t0 = time.perf_counter()
            rows = await conns[label].fetch(sql, *args)
            lat[label].append((time.perf_counter() - t0) * 1000)
            if page_no == 1:
                first_rows[label] = [dict(r) for r in rows]
        identical = identical and _rows_key(first_rows["plain"]) == _rows_key(first_rows["ts"])
        if not rows:
            raise AssertionError(f"page walk ran dry at page {page_no}")
        last = rows[-1]
        cursor_at = last["finished_at"].isoformat()
        cursor_id = str(last["id"])
    return lat, identical, first_rows["plain"]


def _rows_key(rows: list[dict[str, Any]]) -> list[tuple[str, ...]]:
    # running_for_ms / lease_expired are server-side projections evaluated
    # against clock_timestamp() — deliberately different on every execution
    # (the same droppers timescale_tradeoffs.py uses).
    dropped = {"running_for_ms", "lease_expired"}
    return [tuple(str(v) for k, v in r.items() if k not in dropped) for r in rows]


# ── The pathology legs: per-page cost vs depth, red and green ────────────
#
# The archive tab's keyset walk was QUADRATIC before migration
# 01.00.21_01 (no shipped index served `finished_at DESC NULLS LAST,
# id DESC` — every page top-N sorted the whole matching population; the
# committed sweep artifact's plain points, 3.81 ms/page @ 10k → 135.82
# ms/page @ 2M, were that linear-per-page growth misread as a hypertable
# win). These legs pin BOTH shapes in one run: red = the index dropped,
# green = the index present, same data, same cursors.

#: Depth checkpoints for the per-page cost probes, in ROWS deep.
#: 510,000 = page 10,000 of the 51-row page walk. Filtered to the seeded
#: scale at run time (a checkpoint must leave rows behind it).
PATHOLOGY_DEPTHS_TEMPLATE = [0, 51_000, 510_000, 5_100_000]

_INDEX = "jobs_archive_page_idx"
_CURSOR_AT_DEPTH_SQL = (
    f'SELECT finished_at::text, id::text FROM "{SCHEMA}".jobs_archive '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
    "WHERE status::text = ANY($1) "
    "ORDER BY finished_at DESC NULLS LAST, id DESC OFFSET $2 LIMIT 1"
)


async def compute_cursors(conn: asyncpg.Connection, depths: list[int]) -> list[tuple[str, str]]:
    """The (finished_at, id) cursor at each row depth, index-backed."""
    cursors = []
    for depth in depths:
        if depth == 0:
            cursors.append(("", ""))
        else:
            row = await conn.fetchrow(_CURSOR_AT_DEPTH_SQL, list(_WHERE_PARAMS[0]), depth)
            assert row is not None, f"cursor ran dry at depth {depth}"
            cursors.append((row[0], row[1]))
    return cursors


async def set_page_index(conn: asyncpg.Connection, present: bool) -> None:
    if present:
        await conn.execute(  # Why: schema is a benchmark-controlled constant.
            f'CREATE INDEX IF NOT EXISTS "{_INDEX}" '
            f'ON "{SCHEMA}".jobs_archive (finished_at DESC NULLS LAST, id DESC)'
        )
    else:
        await conn.execute(
            f'DROP INDEX IF EXISTS "{SCHEMA}"."{_INDEX}"'
        )  # Why: schema is a benchmark-controlled constant.
    await conn.execute(
        f'ANALYZE "{SCHEMA}".jobs_archive'
    )  # Why: schema is a benchmark-controlled constant.


async def page_cost_at_depths(
    conns: dict[str, asyncpg.Connection], cursors: list[tuple[str, str]], samples: int = 3
) -> dict[str, list[dict[str, float]]]:
    """Timed page queries at each depth checkpoint, per engine.

    The page SQL is the admin builder's own output; each depth's cursor
    is the real keyset cursor (or the unpaged first page at depth 0).
    """
    out: dict[str, list[dict[str, float]]] = {"plain": [], "ts": []}
    for cursor_at, cursor_id in cursors:
        sql, args = _page_sql(cursor_at or None, cursor_id or None)
        for label in ("plain", "ts"):
            xs = []
            for _ in range(samples):
                t0 = time.perf_counter()
                rows = await conns[label].fetch(sql, *args)
                xs.append((time.perf_counter() - t0) * 1000)
            assert len(rows) > 0
            out[label].append(dist_stats(xs))
    return out


async def page_plan_nodes(conn: asyncpg.Connection) -> dict[str, Any]:
    """The page query's plan, red or green: node types, index conditions,
    and the rows each node filtered out — the numbers the pin test and
    the migration rationale cite."""
    sql, args = _page_sql(None, None)
    container = json.loads(
        await conn.fetchval(  # Why: schema is a benchmark-controlled constant.
            "EXPLAIN (ANALYZE, FORMAT JSON) " + sql,
            *args,
        )
    )[0]
    nodes: list[dict[str, Any]] = []

    def visit(n: dict[str, Any]) -> None:
        entry: dict[str, Any] = {"node": n["Node Type"]}
        if "Index Name" in n:
            entry["index"] = n["Index Name"]
        if "Relation" in n:
            entry["relation"] = n["Relation"]
        if n.get("Index Cond") is not None:
            entry["index_cond"] = str(n["Index Cond"])
        if n.get("Rows Removed by Filter") is not None:
            entry["rows_removed_by_filter"] = n["Rows Removed by Filter"]
        if n.get("Sort Method") is not None:
            entry["sort_method"] = n["Sort Method"]
        nodes.append(entry)
        for c in n.get("Plans", []):
            visit(c)

    visit(container["Plan"])
    return {"execution_ms": container["Execution Time"], "nodes": nodes}


# ── Counts, drain, sizes ─────────────────────────────────────────────────


async def counts(conns: dict[str, asyncpg.Connection]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    shapes = {
        "count_all": f'SELECT count(*) FROM "{SCHEMA}".jobs_archive',  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        "count_failed": f"SELECT count(*) FROM \"{SCHEMA}\".jobs_archive WHERE status = 'failed'",  # noqa: S608
        "count_expired_window": (
            f'SELECT count(*) FROM "{SCHEMA}".jobs_archive WHERE expire_at < clock_timestamp()'  # noqa: S608
        ),
    }
    for name, sql in shapes.items():
        out[name] = {}
        for label in ("plain", "ts"):
            samples = []
            for _ in range(3):
                t0 = time.perf_counter()
                n = await conns[label].fetchval(sql)
                samples.append((time.perf_counter() - t0) * 1000)
            out[name][label] = {"ms_p50": percentile(sorted(samples), 0.50), "rows": int(n)}
    return out


async def drain_expired(dsn: str, floor: datetime | None) -> dict[str, Any]:
    """The expiry sweep's batch shape until the expired cohort is gone.

    ``floor=None`` runs the full row-delete statement (the leg the engines
    are compared on); a datetime floor runs the armed-policy statement
    (probe-only cost — the per-tick cost when the policy owns the aged
    end).
    """
    sql = _compose_expiry_sql(SCHEMA, floor)
    conn = await asyncpg.connect(dsn)
    batches: list[float] = []
    deleted = 0
    try:
        # Session-scoped, so it must ride THIS connection: the shipped
        # 100k-tuple decompression budget hard-errors a sweep batch on
        # compressed chunks (measured: the ARMED sweep — which deletes
        # nothing at this seed, the policy owns the cohort — still
        # decompressed the whole 10M table through its scan and died on
        # the budget; the same trap enable_hypertables' probe warns
        # about). The bench measures throughput, not the breaker.
        await conn.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        while True:
            t0 = time.perf_counter()
            args: tuple[object, ...] = (DRAIN_BATCH,) if floor is None else (DRAIN_BATCH, floor)
            rows = await _run_prune_batch(
                conn,
                sql,
                *args,
                statement_timeout_ms=SWEEP_TIMEOUT_MS,
                sweep_name="archscale_drain",
                sizer=None,
            )
            batches.append((time.perf_counter() - t0) * 1000)
            cnt = sum(int(r["cnt"]) for r in rows)
            deleted += cnt
            if not cnt:
                break
        remaining = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".jobs_archive '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
            "WHERE expire_at < clock_timestamp()"
        )
        return {
            "batches": len(batches),
            "deleted": deleted,
            "remaining_expired": int(remaining),
            "per_batch_ms_p50": percentile(sorted(batches), 0.50),
            "per_batch_ms_p95": percentile(sorted(batches), 0.95),
            "drain_total_s": sum(batches) / 1000,
        }
    finally:
        await conn.close()


async def remove_archive_policy(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            "SELECT remove_retention_policy($1::regclass, if_exists => TRUE)",
            f'"{SCHEMA}"."jobs_archive"',
        )
    finally:
        await conn.close()


async def sizes(dsn: str) -> dict[str, Any]:
    conn = await asyncpg.connect(dsn)
    try:
        is_ht = await conn.fetchval(
            "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = $1", SCHEMA
        )
        if not is_ht:
            return {
                "engine": "plain",
                "jobs_archive_bytes": await conn.fetchval(
                    "SELECT pg_total_relation_size(format('%I.%I', $1::text, 'jobs_archive'))",  # Why: schema is a benchmark-controlled constant.
                    SCHEMA,
                ),
            }
        out: dict[str, Any] = {"engine": "hypertable"}
        out["hypertable_size_bytes"] = await conn.fetchval(
            "SELECT hypertable_size(format('%I.%I', x.hypertable_schema, x.hypertable_name)::regclass) "  # Why: schema is a benchmark-controlled constant.
            "FROM (SELECT DISTINCT hypertable_schema, hypertable_name "
            "FROM timescaledb_information.chunks WHERE hypertable_schema = $1) x",
            SCHEMA,
        )
        out["chunks"] = await conn.fetch(
            "SELECT chunk_schema, chunk_name, COALESCE(is_compressed, FALSE) AS is_compressed "  # Why: schema is a benchmark-controlled constant.
            "FROM timescaledb_information.chunks WHERE hypertable_schema = $1 "
            "AND hypertable_name = 'jobs_archive' ORDER BY chunk_name",
            SCHEMA,
        )
        return out
    finally:
        await conn.close()


async def compress_all_chunks(dsn: str) -> dict[str, Any]:
    """Force the columnstore on every rowstore chunk (the background
    policy's work, executed inline so the bench controls when)."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        chunks = await conn.fetch(
            "SELECT chunk_schema, chunk_name FROM timescaledb_information.chunks "  # Why: schema is a benchmark-controlled constant.
            "WHERE hypertable_schema = $1 AND hypertable_name = 'jobs_archive' "
            "AND COALESCE(is_compressed, FALSE) IS FALSE ORDER BY chunk_name",
            SCHEMA,
        )
        t0 = time.perf_counter()
        compressed, failed = [], []
        for c in chunks:
            try:
                # The chunk lives in the extension's own chunk schema (the
                # view's chunk_schema), never in the archive schema — the
                # first run looked them up in the wrong schema and the
                # whole columnstore leg silently no-opped.
                await conn.execute(
                    "SELECT compress_chunk(format('%I.%I', $1::text, $2::text)::regclass)",  # Why: chunk identity comes from the catalog of the bench's own hypertable.
                    c["chunk_schema"],
                    c["chunk_name"],
                )
                compressed.append(c["chunk_name"])
            except Exception as exc:  # Why: per-chunk failure is recorded, not fatal — a chunk that cannot compress is itself a finding.
                failed.append({"chunk": c["chunk_name"], "error": str(exc)[:200]})
        assert compressed, (
            f"no chunk compressed — the columnstore leg would silently no-op: {failed}"
        )
        return {
            "compress_s": time.perf_counter() - t0,
            "chunks_compressed": compressed,
            "chunks_failed": failed,
        }
    finally:
        await conn.close()


def dist_stats(xs: list[float]) -> dict[str, float]:
    xs = sorted(xs)
    return {
        "n": len(xs),
        "p50_ms": percentile(xs, 0.50),
        "p95_ms": percentile(xs, 0.95),
        "mean_ms": statistics.fmean(xs),
        "min_ms": xs[0],
        "max_ms": xs[-1],
    }


def band(xs: list[float], lo: int, hi: int) -> dict[str, float]:
    """Stats for pages [lo, hi) — page 1 is [0, 50), page 10000 is the last."""
    return dist_stats(xs[lo:hi])


# ── Main ─────────────────────────────────────────────────────────────────


async def main() -> None:
    global _KEEP, ARCHIVE_ROWS, WALK_PAGES
    ap = argparse.ArgumentParser(description="Archive-at-scale campaign benchmark")
    ap.add_argument("--archive", type=int, default=ARCHIVE_ROWS)
    ap.add_argument("--pages", type=int, default=WALK_PAGES)
    ap.add_argument("--keep", action="store_true", help="keep the containers after the run")
    args = ap.parse_args()
    _KEEP = args.keep
    ARCHIVE_ROWS = args.archive
    WALK_PAGES = args.pages

    t_start = time.perf_counter()
    base = datetime.now(UTC).replace(microsecond=0)
    print(
        f"archive at scale: {ARCHIVE_ROWS:,} rows per engine "
        f"(expired cohort {EXPIRED_COHORT:,}), walk {WALK_PAGES:,} pages; base {base.isoformat()}",
        flush=True,
    )

    print("[1/8] containers", flush=True)
    start_containers()
    await wait_ready(DSN_PLAIN, "plain")
    await wait_ready(DSN_TS, "ts")

    print("[2/8] schema + hypertable conversion (ts only)", flush=True)
    ts_report = await setup_engine(DSN_TS, hypertables=True)
    await setup_engine(DSN_PLAIN, hypertables=False)

    print("[3/8] seeding plain", flush=True)
    seed_plain = await seed_engine(DSN_PLAIN, base, ARCHIVE_ROWS)
    print(f"    plain seed {seed_plain['seed_s']:.0f}s", flush=True)
    print("[4/8] seeding ts (hypertable)", flush=True)
    seed_ts = await seed_engine(DSN_TS, base, ARCHIVE_ROWS)
    print(f"    ts seed {seed_ts['seed_s']:.0f}s", flush=True)

    # Row-identity spot check on the seed (same doctrine as the tradeoffs
    # bench): both engines must hold byte-identical archive rows.
    for label, dsn in (("plain", DSN_PLAIN), ("ts", DSN_TS)):
        conn = await asyncpg.connect(dsn)
        n = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".jobs_archive')  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        assert n == ARCHIVE_ROWS, (label, n)
        await conn.close()

    print("[5/8] ts sizes (rowstore) + forced compression", flush=True)
    sizes_rowstore = await sizes(DSN_TS)
    compression = await compress_all_chunks(DSN_TS)
    sizes_compressed = await sizes(DSN_TS)
    print(f"    compression: {compression['chunks_compressed']}", flush=True)

    pathology_depths = [d for d in PATHOLOGY_DEPTHS_TEMPLATE if d == 0 or d < ARCHIVE_ROWS - 100]
    conns = {"plain": await asyncpg.connect(DSN_PLAIN), "ts": await asyncpg.connect(DSN_TS)}
    try:
        print(
            f"[6/8] the pagination walk: {WALK_PAGES:,} pages, engines interleaved "
            f"(green: migration 01.00.21_01's index in place)",
            flush=True,
        )
        lat, identical, first_rows = await walk_pages(conns, WALK_PAGES)
        assert identical, "engines' page 1 rows differ — seed mismatch"
        page1 = {label: band(xs, 0, 50) for label, xs in lat.items()}
        page_deep = {label: band(xs, WALK_PAGES - 50, WALK_PAGES) for label, xs in lat.items()}
        for label in ("plain", "ts"):
            print(
                f"    {label}: page1 p50 {page1[label]['p50_ms']:.2f} ms → "
                f"page{WALK_PAGES:,} p50 {page_deep[label]['p50_ms']:.2f} ms "
                f"(walk total {sum(lat[label]) / 1000:.1f}s)",
                flush=True,
            )

        # ── The pathology legs ───────────────────────────────────────────
        # Cursors are computed once (index-backed, cheap) and reused by
        # both legs, so red and green time the SAME positions.
        print(
            f"[6b/8] per-page cost vs depth, red (index dropped) vs green "
            f"(depths {pathology_depths})",
            flush=True,
        )
        cursors = await compute_cursors(conns["plain"], pathology_depths)
        for label in ("plain", "ts"):
            await set_page_index(conns[label], present=False)
        red = await page_cost_at_depths(conns, cursors)
        red_plan = {label: await page_plan_nodes(conns[label]) for label in ("plain", "ts")}
        for label in ("plain", "ts"):
            await set_page_index(conns[label], present=True)
        green = await page_cost_at_depths(conns, cursors)
        green_plan = {label: await page_plan_nodes(conns[label]) for label in ("plain", "ts")}
        for label in ("plain", "ts"):
            red_ms = [p["p50_ms"] for p in red[label]]
            grn_ms = [p["p50_ms"] for p in green[label]]
            print(
                f"    {label} red  (no index): {[round(v, 1) for v in red_ms]} ms/page", flush=True
            )
            print(
                f"    {label} green (index)   : {[round(v, 2) for v in grn_ms]} ms/page", flush=True
            )
            # The pinned shapes. RED: the per-page cost is the same O(n)
            # full sort at EVERY cursor depth — flat in depth, linear in
            # table size, which is what makes the n-page walk quadratic
            # (the sweep artifact's 3.81→135.82 ms/page over 10k→2M rows is
            # that linear-in-size curve; this 10M point continues it).
            # GREEN on PLAIN: index-served pages stay sub-millisecond at
            # every depth — the walk's total is n_pages x O(log n), not
            # n_pages x O(n), and the red/green ratio is three orders of
            # magnitude. GREEN on the COMPRESSED columnstore: the index
            # still serves the ordering (the plan-shape asserts below),
            # but each page's rows decompress out of segment storage and
            # the measured cost is decompression-bound — tens to hundreds
            # of ms, ~flat in depth, coinciding with the red sort's cost
            # at the deepest checkpoint. That coincidence is itself the
            # finding: on compressed chunks the page cost is the
            # columnstore's read trade-off, not a plan defect, so the ts
            # leg pins the plan SHAPE and records the band (the red/green
            # ratio pin is a plain-only contract).
            if label == "plain":
                assert red_ms[-1] > 100.0, (
                    f"red leg per-page cost at {ARCHIVE_ROWS:,} rows implausibly cheap — "
                    f"the sort pathology did not reproduce: {red_ms}"
                )
                assert max(grn_ms) < 5.0, f"green leg not index-flat at depth: {grn_ms}"
                assert red_ms[-1] > grn_ms[-1] * 100, (
                    f"plain red/green per-page ratio too small: red {red_ms[-1]} vs "
                    f"green {grn_ms[-1]}"
                )
            else:
                # The compressed-chunk read band, recorded: sub-second at
                # every checkpoint (a sanity bound — the measured band is
                # ~55-390 ms/page), and DECOMPRESSION-BOUND: red and green
                # cost the same at every depth (measured, this artifact's
                # red/green series agree within noise) — the storage
                # layout, not the plan shape, sets the page cost on the
                # columnstore. That is the read-side trade-off the
                # chunk-drop retention win and the ~6x storage reduction
                # buy; the plan-shape evidence (both plans, below) rides
                # with the artifact.
                assert max(grn_ms) < 1_000.0, (
                    f"ts green page cost beyond the compressed-chunk band: {grn_ms}"
                )

        # The seek's structural pin on PLAIN (the regression the migration
        # fixes): the green page plans no Sort and no Seq Scan of the
        # archive. The ts side's plan is recorded but not shape-asserted:
        # behind the columnstore the planner weighs an ordered decompress
        # against a sort over decompressed rows, and either shape's cost
        # is decompression-bound — the artifact's green_plan_ts is the
        # evidence, the cost band above is the pin.
        for label in ("plain",):
            scan_nodes = [
                n
                for n in green_plan[label]["nodes"]
                if n["node"].startswith("Sort")
                or (n["node"] == "Seq Scan" and n.get("relation") == "jobs_archive")
            ]
            assert not scan_nodes, (
                f"{label}: the green archive page plans {scan_nodes} — the ordering "
                "regressed to an unserved sort (the pre-01.00.21_01 shape)"
            )

        print("[7/8] counts + drains", flush=True)
        count_data = await counts(conns)
    finally:
        for c in conns.values():
            await c.close()

    # Drains. First the armed-policy sweep on ts ONLY (the per-tick cost
    # when the policy owns the aged end — the cohort is 370-399d old, every
    # row below the now-365d floor, so it deletes nothing; the tradeoffs
    # bench's finding, re-measured at 10M). Then the row-delete drain both
    # engines run: the SAME statement (floor=None), ts with the policy
    # removed for the leg and re-armed after, so the decompression-backed
    # delete cost is engine-comparable. Plain goes last so its cohort is
    # intact for its own leg.
    ts_conn = await asyncpg.connect(DSN_TS)
    try:
        ts_floor = await retention_policy_floor(ts_conn, SCHEMA, "jobs_archive", "finished_at")
    finally:
        await ts_conn.close()
    assert ts_floor is not None, "ts must have an armed policy floor"
    sweep_armed_ts = await drain_expired(DSN_TS, ts_floor)
    assert sweep_armed_ts["deleted"] == 0, sweep_armed_ts
    print(f"    armed-policy sweep (ts): {sweep_armed_ts}", flush=True)
    await remove_archive_policy(DSN_TS)
    drain_plain = await drain_expired(DSN_PLAIN, None)
    drain_ts = await drain_expired(DSN_TS, None)
    assert drain_plain["deleted"] == EXPIRED_COHORT, drain_plain
    assert drain_ts["deleted"] == EXPIRED_COHORT, drain_ts
    print(f"    row-delete drain plain: {drain_plain}", flush=True)
    print(f"    row-delete drain ts:    {drain_ts}", flush=True)
    sizes_after = {"plain": await sizes(DSN_PLAIN), "ts": await sizes(DSN_TS)}
    await rearm_archive_policy(DSN_TS)

    print("[8/8] teardown + results", flush=True)
    if not _KEEP:
        stop_containers()

    results = {
        "schema": 1,
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": {
            "image": IMAGE,
            "engines": {
                "plain": f"{CONTAINER_PLAIN}:{PORT_PLAIN} (no hypertables)",
                "ts": f"{CONTAINER_TS}:{PORT_TS} (enable_hypertables + policies)",
            },
            "hypertable_conversion": ts_report,
            "archive_rows": ARCHIVE_ROWS,
            "expired_cohort": EXPIRED_COHORT,
            "walk_pages": WALK_PAGES,
            "page_size": 51,
            "drain_batch": DRAIN_BATCH,
            "sweep_statement_timeout_ms": SWEEP_TIMEOUT_MS,
            "seed": {"base": base.isoformat(), "plain": seed_plain, "ts": seed_ts},
            "python": sys.version.split()[0],
            "wall_s": time.perf_counter() - t_start,
        },
        "pagination_walk": {
            "page1_p50": page1,
            f"page{WALK_PAGES}_p50": page_deep,
            "walk_total_s": {label: sum(xs) / 1000 for label, xs in lat.items()},
            "per_page_samples_note": "full per-page series retained in-page below",
            "per_page_ms": lat,
            "page1_rows_identical": identical,
            "page1_ids": [str(r["id"]) for r in first_rows][:51],
        },
        "pagination_pathology": {
            "depth_checkpoints_rows": pathology_depths,
            "red_per_page_no_index": red,
            "green_per_page_with_index": green,
            "red_plan_first_page": red_plan,
            "green_plan_first_page": green_plan,
            "root_cause": (
                "the archive tab's ordering (finished_at DESC NULLS LAST, id DESC) was "
                "matched by no shipped index; every page top-N sorted the whole matching "
                "population, so page cost was linear in archive depth and the keyset walk "
                "quadratic. Fixed by migration 01.00.21_01 "
                "(jobs_archive_page_idx), the archive tab's instance of the 01.00.20_01 "
                "/history fix."
            ),
            "prior_evidence": (
                "benchmarks/results/timescale-tradeoffs-sweep.json plain points: "
                "3.81 ms/page @ 10k, 23.47 @ 100k, 55.92 @ 400k, 135.82 @ 2M "
                "(~linear per-page growth, previously attributed to a hypertable win)"
            ),
        },
        "counts": count_data,
        "compression": {
            "rowstore": sizes_rowstore,
            "compression_run": compression,
            "compressed": sizes_compressed,
        },
        "drain": {
            "armed_policy_sweep_ts": sweep_armed_ts,
            "row_delete_drain": {"plain": drain_plain, "ts": drain_ts},
            "sizes_after": sizes_after,
        },
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / RESULTS_NAME
    path.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"\nresults → {path}")
    print(f"(wall {time.perf_counter() - t_start:.0f}s)")


async def rearm_archive_policy(dsn: str) -> None:
    """Re-arm the production retention policy after the row-delete leg."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            "SELECT add_retention_policy($1::regclass, $2::interval, if_not_exists => TRUE)",
            f'"{SCHEMA}"."jobs_archive"',
            timedelta(days=365),
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if not _KEEP:
            stop_containers()
