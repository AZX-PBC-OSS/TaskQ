"""A/B: columnstore (compression) vs TaskQ's current rowstore hypertable for jobs_archive.

Sibling to ``timescale_tradeoffs.py`` (which measured hypertable-vs-plain and
left the compression lever UNMEASURED): both engines here are the SAME
``timescale/timescaledb`` image with TaskQ's own ``enable_hypertables``
conversion — one left at the current rowstore state, one with a columnstore
(compression) policy on ``jobs_archive``.  The policy wiring follows the Tiger
Data segmenting/ordering guidance: ``orderby = 'finished_at DESC'`` (the
archive tab's newest-first read, and the time column compresses best),
``segmentby = 'actor, queue'`` (low-cardinality columns the admin's real
filters use — the search-by-actor shape).  Deliberately NOT ``id``: a
per-row-unique segmentby key would make every columnstore batch a single row
and collapse the compression ratio, the docs' explicit anti-pattern.

Measured (same byte-identical 400k-row ``jobs_archive`` on both engines,
seeded from the trade-off bench's exact archive seed SQL, one fixed base
instant):

1. storage: ``hypertable_size`` after aging every chunk older than 30d into
   the columnstore — the reduction ratio.
2. the archive tab's newest-first page (young ROWSTORE chunk): parity check —
   it must be unchanged (the chunk-pruning win lives in the rowstore chunk).
3. cold archive reads: a filtered aggregate + an aged-window page over
   COMPRESSED chunks vs the same scan on the rowstore engine, with
   EXPLAIN (ANALYZE, BUFFERS) buffer counts as the I/O evidence.
4. the id point lookup (a NON-segmentby key — the docs' documented weakness):
   fetch one archived row by id from a COMPRESSED chunk vs rowstore.
5. the archive expiry sweep: the REAL ``_EXPIRY_CTE_SQL`` batch machinery
   draining the expired cohort interleaved; plus an EXPLAIN (ANALYZE, BUFFERS)
   bare ``DELETE ... WHERE expire_at < statement_timestamp()`` (rolled back,
   run BEFORE the drain) to see what the 2.21+ batch-deletion fast path does
   for an expire_at-keyed delete on compressed chunks (expire_at is NOT the
   segmentby key: decompress? error? silent row-delete?); plus per-chunk
   sizes before/after the drain to distinguish whole-batch drops from
   row-delete ghosting.
6. the fold-guard's NOT EXISTS probe by job id: ``SELECT EXISTS (... WHERE
   id = $1)`` for an id IN a compressed chunk (hit) and for an id NOT in the
   table (miss — the guard's hot case: the insert proceeds), compressed vs
   rowstore.

The columnstore policy is ARMED by TaskQ's own ``enable_hypertables`` (it
adopts the columnstore on the archive tables at deploy time now — the
measured settings this benchmark validated), but the background workers
are stopped (the trade-off bench's determinism rule: no policy may race
the measurements), so aging is forced deterministically with
``compress_chunk`` over ``show_chunks(older_than 30d)`` — exactly the
chunk set the armed policy would convert at that threshold.  Recorded in
the results meta.

House rules (benchmarks/README.md): engines interleaved; correctness asserted
before any timing is trusted; results land in ``benchmarks/results/``
(gitignored history).  The script is rerunnable end-to-end: it starts and
tears down its own containers, drops and rebuilds its schemas, writes NOTHING
outside ``benchmarks/results/`` and modifies no ``src/`` code.

Run: .venv/bin/python benchmarks/timescale_compression.py [--keep-containers]
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg

sys.path.insert(0, str(Path(__file__).parent))

from timescale_tradeoffs import (  # pyright: ignore[reportMissingImports]  # Why: the sibling benchmark owns the shared harness + the exact archive seed SQL.
    _SEED_ARCHIVE_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the sibling's seed keeps the corpora byte-identical across the two benches.
    connected,
    ms,
    run,
    wait_ready,
)
from tors_harness import (  # pyright: ignore[reportMissingImports]  # Why: the sys.path bootstrap above is the benchmarks/ convention.
    percentile,
)

from taskq.constants import DEFAULT_PRUNE_BATCH_SIZE
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.timescale import enable_hypertables
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the admin module's own WHERE/page builders are the measured statements' source.
    _ARCHIVE_COLS,
    _SORTABLE_ARCHIVE,
    _build_paginated_sql,
    _build_where,
)
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]  # Why: the REAL expiry sweep SQL + runner, not a hand-copied shape.
    _EXPIRY_CTE_SQL,
    _run_prune_batch,
)

# ── Benchmark configuration ──────────────────────────────────────────────

IMAGE = "timescale/timescaledb:2.30.1-pg18"
CONTAINER_ROW = "tq-tscomp-row"
CONTAINER_COL = "tq-tscomp-col"
PORT_ROW = 55443
PORT_COL = 55444
DSN_ROW = f"postgresql://taskq:taskq@localhost:{PORT_ROW}/taskq"
DSN_COL = f"postgresql://taskq:taskq@localhost:{PORT_COL}/taskq"
SCHEMA = "tq_tscm"

SERVER_FLAGS = [
    "-c",
    "max_connections=200",
    "-c",
    "jit=off",
    "-c",
    "shared_buffers=512MB",
    "-c",
    "max_wal_size=4GB",
    # UNLIMITED tuple decompression per DML transaction. The DEFAULT (100k)
    # aborts the expiry sweep's first batch on the columnstore engine with
    # ConfigurationLimitExceededError (measured and recorded by
    # bench_expiry_drain's probe below) — the probe documents the default's
    # failure; this flag lets the drain complete so its actual cost is
    # measurable. Identical on both engines.
    "-c",
    "timescaledb.max_tuples_decompressed_per_dml_transaction=0",
]

ARCHIVE_SEED = 400_000  # 300k in the 1-360d window + 100k expired cohort
ARCHIVE_EXPIRED = 100_000
# Chunks older than this age into the columnstore — the same threshold the
# armed policy uses, and wide enough that the youngest chunk (holding the
# archive tab's newest-first page) stays rowstore on the columnstore engine.
COMPRESS_AFTER = timedelta(days=30)

ROUNDS = 15  # odd: the median is an actual round
LOOKUP_IDS = 25  # per cohort (young / compressed / miss)

BATCH = DEFAULT_PRUNE_BATCH_SIZE  # 10000, the daily prune's batch size
SWEEP_TIMEOUT_MS = 120_000  # the trade-off bench's widened breaker, same reason

TERMINAL = sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"})

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_NAME = "timescale-compression.json"

_KEEP_CONTAINERS = False


# ── Small shared helpers (house pattern, engines interleaved) ────────────


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


def container_id(i: int) -> str:
    """The seed's deterministic archive id for ordinal ``i`` (md5('a'||i)::uuid)."""
    return str(hashlib.md5(f"a{i}".encode()).hexdigest())  # noqa: S324  # Why: must byte-match the seed SQL's md5() id derivation.


# ── Database lifecycle ───────────────────────────────────────────────────


def start_containers() -> None:
    for name in (CONTAINER_ROW, CONTAINER_COL):
        # Why noqa: fixed docker arguments, benchmark-controlled container name.
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607
    for name, port in ((CONTAINER_ROW, PORT_ROW), (CONTAINER_COL, PORT_COL)):
        run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                name,
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
                "-c",
                "shared_preload_libraries=timescaledb",
                *SERVER_FLAGS,
            ]
        )


def stop_containers() -> None:
    for name in (CONTAINER_ROW, CONTAINER_COL):
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607


async def setup_engine(dsn: str, *, columnstore: bool) -> dict[str, Any]:
    """Drop + rebuild; TaskQ's real conversion (which now ARMS the
    columnstore on the archive tables itself); stop the workers; age the
    columnstore side."""
    conn = await connected(dsn)
    out: dict[str, Any] = {"columnstore": columnstore}
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
        await apply_pending(conn, schema=SCHEMA)
        settings = WorkerSettings.load_from_dict({"TASKQ_TIMESCALEDB_HYPERTABLES": "true"})
        report = await enable_hypertables(conn, schema=SCHEMA, settings=settings)
        out["converted"] = list(report.converted)
        out["compression_policies"] = list(report.compression_policies)
        out["decompression_guc_warning"] = report.decompression_guc_warning
        # Stop the background workers: no policy may race the measurements
        # (the trade-off bench's determinism rule). The compression policy
        # enable_hypertables arms on BOTH engines stays dormant — the
        # columnstore difference comes from the forced compress_chunk
        # aging below, on this engine only.
        stopped = False
        for stmt in (
            "SELECT _timescaledb_functions.stop_background_workers()",
            "SELECT _timescaledb_internal.stop_background_workers()",
        ):
            try:
                await conn.execute(stmt)
                stopped = True
                break
            except Exception:  # noqa: S110  # Why: probe across extension versions
                pass
        assert stopped, "could not stop TimescaleDB background workers"
        if not columnstore:
            return out

        # The arming itself is enable_hypertables' now (the measured
        # settings: jobs_archive segmentby (actor, queue), orderby
        # finished_at DESC; job_attempts_archive segmentby job_id,
        # orderby started_at DESC) — verify it took instead of re-arming.
        armed = await conn.fetch(
            "SELECT DISTINCT hypertable_name FROM timescaledb_information.compression_settings "
            "WHERE hypertable_schema = $1",
            SCHEMA,
        )
        assert {r["hypertable_name"] for r in armed} == {
            "jobs_archive",
            "job_attempts_archive",
        }, f"enable_hypertables must have armed the archive columnstore: {armed}"
        out["armed_by"] = "enable_hypertables"
        out["policy"] = [
            r["proc_name"]
            for r in await conn.fetch(
                "SELECT DISTINCT proc_name FROM timescaledb_information.jobs "
                "WHERE hypertable_schema = $1 AND proc_name IN "
                "('policy_compression', 'policy_columnstore')",
                SCHEMA,
            )
        ]
        return out
    finally:
        await conn.close()


async def age_columnstore(dsn: str) -> int:
    """Force the aging step AFTER seeding: compress every chunk older than
    COMPRESS_AFTER — exactly the set the armed policy would convert (the
    background workers are stopped; compress_chunk is the actuation).
    Returns the compressed-chunk count (asserted by the caller)."""
    conn = await connected(dsn)
    try:
        chunks = await conn.fetch(
            "SELECT compress_chunk(c, if_not_compressed => TRUE) AS chunk "
            "FROM show_chunks($1::regclass, older_than => $2::interval) c",
            f'"{SCHEMA}".jobs_archive',
            COMPRESS_AFTER,
        )
        await conn.execute(f'ANALYZE "{SCHEMA}".jobs_archive')
        return len(chunks)
    finally:
        await conn.close()


async def seed_archive(dsn: str, base: datetime) -> float:
    conn = await connected(dsn)
    try:
        t0 = time.perf_counter()
        await conn.execute(
            _SEED_ARCHIVE_SQL.format(s=SCHEMA), base, ARCHIVE_SEED, ARCHIVE_SEED - ARCHIVE_EXPIRED
        )
        await conn.execute(f'ANALYZE "{SCHEMA}".jobs_archive')
        return time.perf_counter() - t0
    finally:
        await conn.close()


async def chunk_state(dsn: str) -> dict[str, Any]:
    """Per-chunk state + sizes for jobs_archive (both engines, same probe)."""
    conn = await connected(dsn)
    try:
        chunks = [
            {
                "chunk": r["chunk_name"],
                "is_compressed": r["is_compressed"],
                "range_start": str(r["range_start"]),
                "bytes": r["bytes"],
            }
            for r in await conn.fetch(
                "SELECT c.chunk_name, c.is_compressed, c.range_start, "
                "pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name))::bigint AS bytes "
                "FROM timescaledb_information.chunks c "
                "WHERE c.hypertable_schema = $1 AND c.hypertable_name = 'jobs_archive' "
                "ORDER BY c.range_start",
                SCHEMA,
            )
        ]
        ht_size = await conn.fetchval(
            "SELECT hypertable_size($1::regclass)",
            f'"{SCHEMA}".jobs_archive',
        )
        return {
            "hypertable_size_bytes": ht_size,
            "chunks": chunks,
            "n_chunks": len(chunks),
            "n_compressed": sum(1 for c in chunks if c["is_compressed"]),
        }
    finally:
        await conn.close()


# ── Measurement shapes ───────────────────────────────────────────────────


def _page_sql(where: str, params: list[Any]) -> tuple[str, list[Any]]:
    """The admin archive tab's exact page shape, via the admin module's builder."""
    return _build_paginated_sql(
        SCHEMA,
        "jobs_archive",
        _ARCHIVE_COLS,
        _SORTABLE_ARCHIVE,
        where,
        params,
        None,
        None,
        "next",
        "finished_at",
        "desc",
    )


async def verify_seed() -> None:
    """Row count + ordered-sample checksum must agree across engines BEFORE
    any timing is trusted (the house rule)."""
    checksum_sql = (
        "SELECT md5(string_agg(sig, ',' ORDER BY id)) FROM ("  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        "SELECT id, id::text || '|' || actor || '|' || status::text || '|' "
        "|| finished_at::text AS sig "
        f'FROM "{SCHEMA}".jobs_archive ORDER BY id LIMIT 5000) q'  # Why: schema is a benchmark-controlled constant.
    )
    counts, checksums = {}, {}
    for label, dsn in (("row", DSN_ROW), ("col", DSN_COL)):
        conn = await connected(dsn)
        counts[label] = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".jobs_archive')  # noqa: S608
        checksums[label] = await conn.fetchval(checksum_sql)
        await conn.close()
    assert counts["row"] == counts["col"] == ARCHIVE_SEED, counts
    assert checksums["row"] == checksums["col"], "sample checksum mismatch"


async def bench_page_young() -> dict[str, Any]:
    """Shape 2: the archive tab's newest-first page — young ROWSTORE chunk."""
    where, params = _build_where(TERMINAL, None, None, None, None, None, None, None)
    sql, args = _page_sql(where, params)
    a_side, b_side = Side("row"), Side("col")
    conn_row, conn_col = await connected(DSN_ROW), await connected(DSN_COL)
    identical, expected = True, None
    try:
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            rows_a = await conn_row.fetch(sql, *args)
            a_side.rounds.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            rows_b = await conn_col.fetch(sql, *args)
            b_side.rounds.append((time.perf_counter() - t0) * 1000)
            if expected is None:
                expected = [tuple(str(v) for v in dict(r).values()) for r in rows_a]
                identical = [tuple(str(v) for v in dict(r).values()) for r in rows_b] == expected
        assert identical, "newest-first page returned different rows across engines"
        return {
            "row_p50_ms": a_side.p50,
            "row_p95_ms": a_side.p95,
            "col_p50_ms": b_side.p50,
            "col_p95_ms": b_side.p95,
            "rounds": ROUNDS,
            "rows_identical": True,
            "page_rows": len(expected or []),
        }
    finally:
        await conn_row.close()
        await conn_col.close()


async def bench_cold_reads(base: datetime) -> dict[str, Any]:
    """Shape 3: cold reads over aged (compressed on col) data, interleaved."""
    lo = base - timedelta(days=130)
    hi = base - timedelta(days=100)
    cutoff = base - timedelta(days=90)
    agg_sql = (
        "SELECT count(*) AS n, min(finished_at) AS lo, max(finished_at) AS hi "  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        f'FROM "{SCHEMA}".jobs_archive '  # Why: schema is a benchmark-controlled constant.
        "WHERE finished_at >= $1::timestamptz AND finished_at < $2::timestamptz "
        "AND status = 'failed'"
    )
    where, params = _build_where(TERMINAL, None, None, None, None, None, None, None)
    page_sql, page_args = _page_sql(
        f"{where} AND finished_at < ${len(params) + 1}::timestamptz",
        [*params, cutoff],
    )
    agg_a, agg_b = Side("row"), Side("col")
    page_a, page_b = Side("row"), Side("col")
    conn_row, conn_col = await connected(DSN_ROW), await connected(DSN_COL)
    identical, expected = True, None
    try:
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            rows_a = await conn_row.fetch(agg_sql, lo, hi)
            agg_a.rounds.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            rows_b = await conn_col.fetch(agg_sql, lo, hi)
            agg_b.rounds.append((time.perf_counter() - t0) * 1000)
            assert rows_a[0]["n"] == rows_b[0]["n"], (rows_a[0], rows_b[0])

            t0 = time.perf_counter()
            p_a = await conn_row.fetch(page_sql, *page_args)
            page_a.rounds.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            p_b = await conn_col.fetch(page_sql, *page_args)
            page_b.rounds.append((time.perf_counter() - t0) * 1000)
            if expected is None:
                expected = [tuple(str(v) for v in dict(r).values()) for r in p_a]
                identical = [tuple(str(v) for v in dict(r).values()) for r in p_b] == expected
        assert identical, "aged-window page returned different rows across engines"
        # Buffer-count evidence (one round, not timed): the col engine should
        # read a fraction of the bytes on the compressed-chunk scan.
        agg_a_plan = await explain_buffers(conn_row, agg_sql, [lo, hi])
        agg_b_plan = await explain_buffers(conn_col, agg_sql, [lo, hi])
        return {
            "aggregate": {
                "row_p50_ms": agg_a.p50,
                "row_p95_ms": agg_a.p95,
                "col_p50_ms": agg_b.p50,
                "col_p95_ms": agg_b.p95,
                "rounds": ROUNDS,
                "row_buffers": buffers_of(agg_a_plan),
                "col_buffers": buffers_of(agg_b_plan),
                "row_plan": agg_a_plan,
                "col_plan": agg_b_plan,
            },
            "aged_page": {
                "row_p50_ms": page_a.p50,
                "row_p95_ms": page_a.p95,
                "col_p50_ms": page_b.p50,
                "col_p95_ms": page_b.p95,
                "rows_identical": identical,
            },
        }
    finally:
        await conn_row.close()
        await conn_col.close()


async def explain_buffers(conn: asyncpg.Connection, sql: str, args: list[Any]) -> list[str]:
    """EXPLAIN (ANALYZE, BUFFERS) text of one execution (not timed)."""
    rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) " + sql, *args)
    return [r[0] for r in rows]


def buffers_of(plan: list[str]) -> int | None:
    """The plan's total shared hit+read buffer count (its summary line)."""
    for line in plan:
        if "shared hit=" not in line:
            continue
        # The canonical tail line: "Buffers: shared hit=N read=M ..."
        seg = line.split("shared hit=", 1)[1]
        hits = _leading_int(seg)
        reads = _leading_int(seg.split("read=", 1)[1]) if "read=" in seg else 0
        return hits + reads
    return None


def _leading_int(seg: str) -> int:
    digits = ""
    for ch in seg:
        if ch.isdigit():
            digits += ch
        else:
            break
    return int(digits or 0)


async def bench_point_lookup(base: datetime) -> dict[str, Any]:
    """Shape 4 + 6: the id point lookup (full row) and the fold-guard's
    NOT EXISTS witness probe (hit + miss), compressed vs rowstore."""
    # Seed ordinals: i in 1..10 are 1-10d old (young chunk, rowstore on col);
    # 36100..36124 have i % 360 in 101..125 → 101-125d old (compressed chunks).
    young_ids = [container_id(i) for i in range(1, 10 + 1)]
    old_ids = [container_id(i) for i in range(36_101, 36_100 + LOOKUP_IDS + 1)]
    miss_ids = [str(hashlib.md5(f"miss{i}".encode()).hexdigest()) for i in range(LOOKUP_IDS)]  # noqa: S324  # Why: shape-only probe ids, never stored.
    lookup_sql = (
        f"SELECT id, actor, queue, payload, status, finished_at, expire_at "  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        f'FROM "{SCHEMA}".jobs_archive WHERE id = $1::uuid'  # Why: schema is a benchmark-controlled constant.
    )
    exists_sql = f'SELECT EXISTS (SELECT 1 FROM "{SCHEMA}".jobs_archive WHERE id = $1::uuid)'  # noqa: S608

    sides = {
        "lookup_young": (Side("row"), Side("col")),
        "lookup_compressed": (Side("row"), Side("col")),
        "exists_hit_compressed": (Side("row"), Side("col")),
        "exists_miss": (Side("row"), Side("col")),
    }
    conn_row, conn_col = await connected(DSN_ROW), await connected(DSN_COL)
    try:
        # Correctness first: every probed row exists on BOTH engines and is
        # byte-identical (the fixed base instant guarantees it).
        for i in (*range(1, 11), *range(36_101, 36_100 + LOOKUP_IDS + 1)):
            jid = container_id(i)
            a = await conn_row.fetchrow(lookup_sql, jid)
            b = await conn_col.fetchrow(lookup_sql, jid)
            assert a is not None and b is not None, (i, a, b)
            assert dict(a) == dict(b), (i, dict(a), dict(b))

        for jid in young_ids:
            for conn, side in (
                (conn_row, sides["lookup_young"][0]),
                (conn_col, sides["lookup_young"][1]),
            ):
                t0 = time.perf_counter()
                rows = await conn.fetch(lookup_sql, jid)
                assert len(rows) == 1
                side.rounds.append((time.perf_counter() - t0) * 1000)
        for jid in old_ids:
            for conn, side in (
                (conn_row, sides["lookup_compressed"][0]),
                (conn_col, sides["lookup_compressed"][1]),
            ):
                t0 = time.perf_counter()
                rows = await conn.fetch(lookup_sql, jid)
                assert len(rows) == 1
                side.rounds.append((time.perf_counter() - t0) * 1000)
        for jid in old_ids:
            for conn, side in (
                (conn_row, sides["exists_hit_compressed"][0]),
                (conn_col, sides["exists_hit_compressed"][1]),
            ):
                t0 = time.perf_counter()
                hit = await conn.fetchval(exists_sql, jid)
                assert hit is True
                side.rounds.append((time.perf_counter() - t0) * 1000)
        for jid in miss_ids:
            for conn, side in (
                (conn_row, sides["exists_miss"][0]),
                (conn_col, sides["exists_miss"][1]),
            ):
                t0 = time.perf_counter()
                hit = await conn.fetchval(exists_sql, jid)
                assert hit is False
                side.rounds.append((time.perf_counter() - t0) * 1000)

        # Plan evidence for the compressed-chunk lookup (what does the col
        # engine actually do to find one non-segmentby id?).
        col_lookup_plan = await explain_buffers(conn_col, lookup_sql, [old_ids[0]])
        row_lookup_plan = await explain_buffers(conn_row, lookup_sql, [old_ids[0]])
        return {
            name: {
                "row_p50_ms": a.p50,
                "row_p95_ms": a.p95,
                "col_p50_ms": b.p50,
                "col_p95_ms": b.p95,
                "samples": len(a.rounds),
            }
            for name, (a, b) in sides.items()
        } | {
            "row_lookup_plan_compressed_chunk": row_lookup_plan,
            "col_lookup_plan_compressed_chunk": col_lookup_plan,
        }
    finally:
        await conn_row.close()
        await conn_col.close()


async def bench_expiry_drain(base: datetime) -> dict[str, Any]:
    """Shape 5: the real expiry sweep drains the expired cohort, interleaved.

    BEFORE the drain, a rolled-back EXPLAIN (ANALYZE, BUFFERS) of the bare
    expire_at-keyed DELETE records what the 2.21+ batch-deletion machinery
    does on compressed chunks; per-chunk sizes (taken by the caller before
    and after) distinguish whole-batch drops from row-delete ghosting.
    """
    conn_row, conn_col = await connected(DSN_ROW), await connected(DSN_COL)
    sql = _EXPIRY_CTE_SQL.format(schema=SCHEMA)
    bare_delete = (
        f'DELETE FROM "{SCHEMA}".jobs_archive '  # noqa: S608  # Why: schema is a benchmark-controlled constant.
        "WHERE expire_at < statement_timestamp()"
    )
    try:
        # The plan probes run FIRST and roll back (they would otherwise
        # consume the cohort the timed drain needs).
        row_plan = await explain_rollback(conn_row, bare_delete)
        col_plan = await explain_rollback(conn_col, bare_delete)

        # Probe: the DEFAULT decompression budget (100k tuples/DML
        # transaction). This is what an operator first flipping the policy
        # on inherits — measured here (rolled back either way: on success it
        # must not consume the cohort, on failure nothing is lost).
        default_limit: dict[str, Any]
        probe = conn_col.transaction()
        await probe.start()
        try:
            await conn_col.execute(
                "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 100000"
            )
            t0 = time.perf_counter()
            try:
                rows = await _run_prune_batch(
                    conn_col,
                    sql,
                    BATCH,
                    statement_timeout_ms=SWEEP_TIMEOUT_MS,
                    sweep_name="bench_compression_expiry_default_limit",
                    sizer=None,
                )
                default_limit = {
                    "ok": True,
                    "deleted": sum(int(r["cnt"]) for r in rows),
                    "ms": (time.perf_counter() - t0) * 1000,
                }
            except asyncpg.PostgresError as exc:
                default_limit = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        finally:
            await probe.rollback()
        # The drain itself runs with the server flag's unlimited budget.

        batches = {"row": [], "col": []}
        deleted = {"row": 0, "col": 0}
        while True:
            progressed = False
            for label, conn in (("row", conn_row), ("col", conn_col)):
                t0 = time.perf_counter()
                rows = await _run_prune_batch(
                    conn,
                    sql,
                    BATCH,
                    statement_timeout_ms=SWEEP_TIMEOUT_MS,
                    sweep_name="bench_compression_expiry",
                    sizer=None,
                )
                batches[label].append((time.perf_counter() - t0) * 1000)
                cnt = sum(int(r["cnt"]) for r in rows)
                deleted[label] += cnt
                if cnt:
                    progressed = True
            if not progressed:
                break
        for label, conn in (("row", conn_row), ("col", conn_col)):
            remaining = await conn.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".jobs_archive '  # noqa: S608
                "WHERE expire_at < clock_timestamp()"
            )
            assert remaining == 0, (label, remaining)
        assert deleted["row"] == deleted["col"] == ARCHIVE_EXPIRED, deleted

        def stats(xs: list[float]) -> dict[str, float]:
            return {
                "p50_ms": percentile(sorted(xs), 0.50),
                "p95_ms": percentile(sorted(xs), 0.95),
                "total_s": sum(xs) / 1000,
                "batches": len(xs),
            }

        return {
            "default_decompression_limit_probe": default_limit,
            "bare_delete_plan_row": row_plan,
            "bare_delete_plan_col": col_plan,
            "rows_target": ARCHIVE_EXPIRED,
            "batch_size": BATCH,
            "deleted": deleted,
            "per_batch_ms": {label: stats(xs) for label, xs in batches.items()},
            "drain_total_s": {label: sum(xs) / 1000 for label, xs in batches.items()},
        }
    finally:
        await conn_row.close()
        await conn_col.close()


async def explain_rollback(conn: asyncpg.Connection, sql: str) -> list[str]:
    """EXPLAIN (ANALYZE, BUFFERS) one DELETE then roll it back."""
    tx = conn.transaction()
    await tx.start()
    try:
        rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) " + sql)
    finally:
        await tx.rollback()
    return [r[0] for r in rows]


# ── Reporting ────────────────────────────────────────────────────────────


def print_page(r: dict[str, Any]) -> None:
    print(
        f"\narchive newest-first page (young rowstore chunk, {r['rounds']} interleaved rounds)"
        f"\n  row {ms(r['row_p50_ms']):>10}  col {ms(r['col_p50_ms']):>10}  "
        f"col/row {r['col_p50_ms'] / r['row_p50_ms']:.2f}x  rows_identical={r['rows_identical']}"
    )


def print_cold(r: dict[str, Any]) -> None:
    agg = r["aggregate"]
    print(
        f"\ncold aggregate over one aged chunk ({agg['rounds']} interleaved rounds)"
        f"\n  row {ms(agg['row_p50_ms']):>10}  col {ms(agg['col_p50_ms']):>10}  "
        f"col/row {agg['col_p50_ms'] / agg['row_p50_ms']:.2f}x  "
        f"buffers row={agg['row_buffers']} col={agg['col_buffers']}"
    )
    pg = r["aged_page"]
    print(
        f"aged-window page (admin shape, compressed chunks on col)"
        f"\n  row {ms(pg['row_p50_ms']):>10}  col {ms(pg['col_p50_ms']):>10}  "
        f"col/row {pg['col_p50_ms'] / pg['row_p50_ms']:.2f}x  rows_identical={pg['rows_identical']}"
    )


def print_lookups(r: dict[str, Any]) -> None:
    print("\nid point lookup + fold-guard probe (rowstore vs columnstore)")
    print(f"  {'shape':<24} {'row p50':>10} {'col p50':>10} {'col/row':>8}")
    for name in (
        "lookup_young",
        "lookup_compressed",
        "exists_hit_compressed",
        "exists_miss",
    ):
        s = r[name]
        print(
            f"  {name:<24} {ms(s['row_p50_ms']):>10} {ms(s['col_p50_ms']):>10} "
            f"{s['col_p50_ms'] / s['row_p50_ms']:>7.2f}x"
        )


def print_drain(title: str, data: dict[str, Any]) -> None:
    print(f"\n{title}  (batch_size {data['batch_size']:,}, target {data['rows_target']:,} rows)")
    print(f"  {'engine':<8} {'batches':>7} {'p50/batch':>11} {'p95/batch':>11} {'drain total':>12}")
    for label in ("row", "col"):
        s = data["per_batch_ms"][label]
        print(
            f"  {label:<8} {s['batches']:>7} {ms(s['p50_ms']):>11} {ms(s['p95_ms']):>11} "
            f"{s['total_s']:>10.1f}s"
        )


def print_storage(row: dict[str, Any], col: dict[str, Any]) -> None:
    ratio = row["hypertable_size_bytes"] / col["hypertable_size_bytes"]
    print(
        f"\nstorage: jobs_archive hypertable_size after aging (>={COMPRESS_AFTER} old → columnstore)"
    )
    print(
        f"  rowstore hypertable: {row['hypertable_size_bytes']:,} bytes "
        f"({row['n_chunks']} chunks, {row['n_compressed']} compressed)"
    )
    print(
        f"  columnstore hypertable: {col['hypertable_size_bytes']:,} bytes "
        f"({col['n_chunks']} chunks, {col['n_compressed']} compressed)"
    )
    print(f"  reduction: {ratio:.2f}x  ({100 * (1 - 1 / ratio):.1f}% smaller)")


# ── Main ─────────────────────────────────────────────────────────────────


async def main() -> None:
    t_start = time.perf_counter()
    base = datetime.now(UTC).replace(microsecond=0)
    print(
        f"base instant {base.isoformat()} — archive {ARCHIVE_SEED:,} rows "
        f"({ARCHIVE_EXPIRED:,} expired) on BOTH engines: rowstore hypertable vs "
        f"columnstore hypertable (segmentby (actor, queue), orderby finished_at DESC)",
        flush=True,
    )

    print("[1/8] starting containers", flush=True)
    start_containers()
    await wait_ready(DSN_ROW, "rowstore engine")
    await wait_ready(DSN_COL, "columnstore engine")

    print("[2/8] schema + hypertable conversion (TaskQ's enable_hypertables)", flush=True)
    setup_row = await setup_engine(DSN_ROW, columnstore=False)
    print("[2/8] arming the columnstore policy on the col engine", flush=True)
    setup_col = await setup_engine(DSN_COL, columnstore=True)
    print(f"    col engine: {json.dumps(setup_col, default=str)}")

    print("[3/8] seeding both engines from the trade-off bench's archive seed", flush=True)
    seed_row = await seed_archive(DSN_ROW, base)
    seed_col = await seed_archive(DSN_COL, base)
    print(f"    row {seed_row:.0f}s  col {seed_col:.0f}s", flush=True)

    print("[4/8] aging + seed correctness asserts", flush=True)
    aged = await age_columnstore(DSN_COL)
    print(f"    columnstore engine: aged {aged} chunks into the columnstore", flush=True)
    storage_before = {"row": await chunk_state(DSN_ROW), "col": await chunk_state(DSN_COL)}
    # Correctness before any timing is trusted: the col engine must ACTUALLY
    # be columnstore now (most chunks compressed, youngest left rowstore), or
    # every number below is measuring nothing.
    assert storage_before["col"]["n_compressed"] > 0, storage_before["col"]["n_compressed"]
    assert storage_before["col"]["n_compressed"] < storage_before["col"]["n_chunks"], (
        "the young chunk must stay rowstore for the page-parity check"
    )
    await verify_seed()

    print("[5/8] archive tab page (young rowstore chunk) + cold reads", flush=True)
    page = await bench_page_young()
    print_page(page)
    cold = await bench_cold_reads(base)
    print_cold(cold)

    print("[6/8] id point lookup + fold-guard probes", flush=True)
    lookups = await bench_point_lookup(base)
    print_lookups(lookups)

    print("[7/8] archive expiry drain (interleaved batch-by-batch)", flush=True)
    drain = await bench_expiry_drain(base)
    print_drain("archive expiry (expired archive rows hard-deleted)", drain)
    storage_after = {"row": await chunk_state(DSN_ROW), "col": await chunk_state(DSN_COL)}

    print("[8/8] teardown + results", flush=True)
    if not _KEEP_CONTAINERS:
        stop_containers()

    print_storage(storage_before["row"], storage_before["col"])

    results = {
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": {
            "image": IMAGE,
            "engines": {
                "row": f"{CONTAINER_ROW}:{PORT_ROW} (hypertable, compression policy "
                "armed-but-dormant (workers stopped), no chunks aged into the columnstore)",
                "col": f"{CONTAINER_COL}:{PORT_COL} (hypertable + columnstore aged in on "
                "jobs_archive: segmentby (actor, queue), orderby finished_at DESC; "
                "job_attempts_archive: segmentby (job_id), orderby started_at DESC)",
            },
            "archive_seed": ARCHIVE_SEED,
            "archive_expired_cohort": ARCHIVE_EXPIRED,
            "compress_after": str(COMPRESS_AFTER),
            "setup": {"row": setup_row, "col": setup_col},
            "note": "the columnstore policy is armed by enable_hypertables itself; background "
            "workers are stopped (determinism rule); aging was forced with compress_chunk over "
            "show_chunks(older_than 30d) — the exact set the policy would convert",
            "wall_s": time.perf_counter() - t_start,
        },
        "storage_before": storage_before,
        "storage_after": storage_after,
        "page_young": page,
        "cold": cold,
        "lookups": lookups,
        "expiry_drain": drain,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / RESULTS_NAME
    path.write_text(json.dumps(results, indent=2, default=str) + "\n")

    print(f"\nresults → {path}")
    print(f"(wall {time.perf_counter() - t_start:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="columnstore vs rowstore hypertable compression A/B")
    ap.add_argument(
        "--keep-containers",
        action="store_true",
        help="leave the two benchmark containers running after the run",
    )
    args = ap.parse_args()
    _KEEP_CONTAINERS = args.keep_containers
    try:
        asyncio.run(main())
    finally:
        if not _KEEP_CONTAINERS:
            stop_containers()
