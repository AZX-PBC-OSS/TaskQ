"""Postgres churn probe for TaskQ's hot-UPDATE job lifecycle.

Measures what the TaskQ DDL (default fillfactor 100, ~15 indexes on jobs)
costs when the real state-transition UPDATE paths run over a scratch
schema:

  A/B schemas: tq_churn_a (stock DDL) vs tq_churn_b (jobs SET fillfactor=50),
  both built with the real migration runner (taskq.migrate.apply_pending).

  Per wave: dispatch pending->running with the REAL dispatch CTE
  (taskq.backend._dispatch_sql.DISPATCH_STRICT_FIFO_SQL), run the REAL
  heartbeat lock-extension UPDATE over the running rows 3x (mimics a job
  spanning ~3 heartbeat ticks), then the REAL terminal write
  (mark_succeeded + job_attempts INSERT + job_events INSERT, the
  _terminal.py transaction shape; dispatch events via the real
  INSERT_EVENTS_BATCH_SQL).

  Measurements per wave: jobs heap size, per-index sizes, n_live_tup /
  n_dead_tup (pg_stat_user_tables), last_autovacuum, dispatch-call
  p50/p95/max.

  After the final wave: VACUUM (ANALYZE) jobs, re-measure sizes/dead
  tuples, and time a fresh dispatch round over the reserved pending
  pool to see post-vacuum recovery.

Read-only with respect to src/; writes only its own artifacts under
results/.  Scratch schemas are dropped on exit (unless --keep-schemas).

Usage:
    python benchmarks/pg_churn_probe.py
    python benchmarks/pg_churn_probe.py --total 60000 --waves 6
    python benchmarks/pg_churn_probe.py --dsn postgresql://... --fillfactor-b 70
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from datetime import timedelta
from pathlib import Path

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._sql import (
    INSERT_EVENTS_BATCH_SQL,
    UPDATE_JOBS_LOCK_SQL_TEMPLATE,
)
from taskq.backend._sql_templates import render
from taskq.migrate import apply_pending
from taskq.testing.pg import truncate_schema

DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
SCHEMA_A = "tq_churn_a"
SCHEMA_B = "tq_churn_b"
ACTOR = "churn_actor"
QUEUE = "churn_q"
RESULTS_DIR = Path(__file__).parent / "results"


def make_payload(i: int) -> str:
    # ~300 B realistic payload, same shape as e2e_dispatch.make_payload.
    notes = "x" * 96
    return (
        f'{{"order_id":"ord-{i:08d}","channel":"web",'
        f'"customer":{{"id":"cus-{i % 997:05d}","tier":"pro"}},'
        f'"items":[{{"sku":"SKU-001","qty":1}},{{"sku":"SKU-002","qty":2}}],'
        f'"notes":"benchmark payload #{i} {notes}","flags":{{"expedited":true}}}}'
    )


async def setup_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await apply_pending(conn, schema=schema)
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608
        ACTOR,
        QUEUE,
    )


async def seed_jobs_insert(
    conn: asyncpg.Connection, schema: str, total: int, chunk: int = 5000
) -> None:
    """Bulk-insert pending jobs with ~300 B payloads (DDL defaults for the rest)."""
    n = 0
    while n < total:
        take = min(chunk, total - n)
        batch_ids = [str(new_uuid()) for _ in range(take)]
        batch_payloads = [make_payload(n + k) for k in range(take)]
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind)
                SELECT t.id, $3, $4, t.payload, 3, 'transient'
                FROM unnest($1::uuid[], $2::jsonb[]) AS t(id, payload)""",  # noqa: S608
            batch_ids,
            batch_payloads,
            ACTOR,
            QUEUE,
        )
        n += take


class Snapshot:
    def __init__(self, label: str, row: dict) -> None:
        self.label = label
        self.row = row

    def __str__(self) -> str:
        r = self.row
        return (
            f"{self.label:<28} heap={r['heap_bytes'] / 1e6:8.2f}MB "
            f"idx={r['index_bytes'] / 1e6:8.2f}MB live={r['n_live_tup']:>7} "
            f"dead={r['n_dead_tup']:>7} avacuums={r['n_auto_vacuums']:>3} "
            f"last_av={r['last_autovacuum'] or '-'}"
        )


TABLES = ("jobs", "job_events", "job_attempts", "jobs_archive", "job_attempts_archive")


async def measure(conn: asyncpg.Connection, schema: str, label: str) -> Snapshot:
    heap = await conn.fetchval("SELECT pg_relation_size($1::regclass)", f'"{schema}".jobs')
    toast = await conn.fetchval(
        "SELECT pg_total_relation_size($1::regclass) - pg_relation_size($1::regclass)"
        " - coalesce((SELECT sum(pg_relation_size(i.indexrelid))::bigint"
        "   FROM pg_index i WHERE i.indrelid = $1::regclass), 0)",
        f'"{schema}".jobs',
    )
    idx = await conn.fetchval(
        "SELECT coalesce(sum(pg_relation_size(i.indexrelid))::bigint, 0)"
        " FROM pg_index i WHERE i.indrelid = $1::regclass",
        f'"{schema}".jobs',
    )
    stats = await conn.fetchrow(
        "SELECT n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count"
        " FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'jobs'",
        schema,
    )
    if stats is None:
        stats = {"n_live_tup": 0, "n_dead_tup": 0, "last_autovacuum": None, "autovacuum_count": 0}
    row = {
        "heap_bytes": heap,
        "toast_bytes": toast,
        "index_bytes": idx,
        "n_live_tup": stats["n_live_tup"],
        "n_dead_tup": stats["n_dead_tup"],
        "last_autovacuum": stats["last_autovacuum"],
        "n_auto_vacuums": stats["autovacuum_count"],
    }
    return Snapshot(label, row)


async def index_sizes(conn: asyncpg.Connection, schema: str) -> dict[str, int]:
    rows = await conn.fetch(
        "SELECT indexrelname, pg_relation_size(indexrelid) AS sz, idx_scan, idx_tup_read, idx_tup_fetch"
        " FROM pg_stat_user_indexes WHERE schemaname = $1 AND relname = 'jobs' ORDER BY indexrelname",
        schema,
    )
    return {r["indexrelname"]: r["sz"] for r in rows}


async def dead_tuple_detail(conn: asyncpg.Connection, schema: str) -> dict[str, int]:
    rows = await conn.fetch(
        "SELECT relname, n_dead_tup FROM pg_stat_user_tables"
        " WHERE schemaname = $1 AND relname = ANY($2::text[])",
        schema,
        list(TABLES),
    )
    return {r["relname"]: r["n_dead_tup"] for r in rows}


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    return xs[min(len(xs) - 1, round(p / 100.0 * (len(xs) - 1)))]


async def run_churn(
    dsn: str,
    schema: str,
    total_jobs: int,
    waves: int,
    dispatch_batch: int,
    heartbeat_ticks: int,
) -> dict:
    """Run the wave protocol on one schema and return the measured record."""
    conn = await asyncpg.connect(dsn)
    dispatch_sql = DISPATCH_STRICT_FIFO_SQL.format(schema=schema)
    events_batch_sql = INSERT_EVENTS_BATCH_SQL.format(schema=schema)
    templates = render(schema)
    heartbeat_sql = UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema=schema)
    worker_id = str(new_uuid())
    lock_lease = timedelta(seconds=60)
    # Leave a pending tail for post-vacuum dispatch timing.
    tail_reserve = 2 * dispatch_batch
    budget = total_jobs - tail_reserve
    per_wave = budget // waves
    wave_rows: list[dict] = []
    idx_base = await index_sizes(conn, schema)

    await seed_jobs_insert(conn, schema, total_jobs)
    base_snap = await measure(conn, schema, "after seed")
    print(f"  [{schema}] {base_snap}")

    try:
        for w in range(waves):
            # ── Phase 1: dispatch pending -> running (real CTE) ──
            call_ms: list[float] = []
            dispatched = 0
            while dispatched < per_wave:
                t0 = time.perf_counter()
                rows = await conn.fetch(
                    dispatch_sql,
                    [QUEUE],
                    dispatch_batch,
                    worker_id,
                    lock_lease,
                    2,
                )
                call_ms.append((time.perf_counter() - t0) * 1000)
                dispatched += len(rows)
                if len(rows) < dispatch_batch:
                    break
                # One batched job_events INSERT per dispatch call — the real
                # dispatch path's event shape (_dispatch.py).
                await conn.execute(
                    events_batch_sql,
                    [r["id"] for r in rows],
                    "state_change",
                    '{"from_state":"pending","to_state":"running"}',
                )
            # ── Phase 2: heartbeat lock-extension over running rows ──
            hb_ms = []
            for _ in range(heartbeat_ticks):
                t0 = time.perf_counter()
                await conn.execute(heartbeat_sql, worker_id, lock_lease)
                hb_ms.append((time.perf_counter() - t0) * 1000)

            # ── Phase 3: terminal writes (mark_succeeded + attempt + event) ──
            running = await conn.fetch(
                f'SELECT id, attempt, started_at FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a probe-controlled identifier validated at apply_pending time
                f"WHERE locked_by_worker = $1 AND status = 'running'",
                worker_id,
            )
            t_term = time.perf_counter()
            event_sql = templates.insert_event
            for r in running:
                async with conn.transaction():
                    await conn.execute(
                        templates.mark_succeeded,
                        r["id"],
                        worker_id,
                        '{"status":"ok"}',
                        14,
                        0,
                        None,
                        None,
                    )
                    await conn.execute(
                        templates.insert_attempt,
                        r["id"],
                        r["attempt"],
                        r["started_at"],
                        "succeeded",
                        None,
                        None,
                        None,
                        None,
                        new_uuid(),  # worker_id (holder CTE resolves it)
                        "{}",
                    )
                    await conn.execute(
                        event_sql,
                        r["id"],
                        "state_change",
                        '{"from_state":"running","to_state":"succeeded"}',
                    )
            term_s = time.perf_counter() - t_term

            snap = await measure(conn, schema, f"after wave {w + 1}")
            wave_rows.append(
                {
                    "wave": w + 1,
                    "dispatched": dispatched,
                    "dispatch_p50_ms": round(statistics.median(call_ms), 2),
                    "dispatch_p95_ms": round(pct(call_ms, 95), 2),
                    "dispatch_max_ms": round(max(call_ms), 2),
                    "heartbeat_ms": round(statistics.median(hb_ms), 2),
                    "terminal_sec": round(term_s, 2),
                    **snap.row,
                }
            )
            print(f"  [{schema}] {wave_rows[-1]}")

        # ── Post-waves: sizes, dead-tuple detail, index growth ──
        pre_vacuum_detail = await dead_tuple_detail(conn, schema)
        idx_after = await index_sizes(conn, schema)

        # Recovery: VACUUM ANALYZE jobs, re-measure, re-time dispatch.
        t0 = time.perf_counter()
        await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')
        vacuum_s = time.perf_counter() - t0
        post = await measure(conn, schema, "post-vacuum")
        post_detail = await dead_tuple_detail(conn, schema)

        # Post-vacuum dispatch timing over the reserved pending tail.
        post_call_ms = []
        for _ in range(10):
            t0 = time.perf_counter()
            await conn.fetch(dispatch_sql, [QUEUE], dispatch_batch, worker_id, lock_lease, 2)
            post_call_ms.append((time.perf_counter() - t0) * 1000)

        # Side-table growth per job.
        counts = {}
        for t in TABLES:
            counts[t] = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".{t}'  # noqa: S608
            )
        ev_bytes = await conn.fetchval(
            "SELECT pg_total_relation_size($1::regclass)", f'"{schema}".job_events'
        )
        at_bytes = await conn.fetchval(
            "SELECT pg_total_relation_size($1::regclass)", f'"{schema}".job_attempts'
        )

        return {
            "schema": schema,
            "waves": wave_rows,
            "pre_vacuum_dead": pre_vacuum_detail,
            "post_vacuum_dead": post_detail,
            "index_bytes_after": idx_after,
            "index_bytes_base": idx_base,
            "vacuum_sec": round(vacuum_s, 2),
            "post_vacuum": post.row,
            "post_vacuum_dispatch_p50_ms": round(statistics.median(post_call_ms), 2),
            "post_vacuum_dispatch_max_ms": round(max(post_call_ms), 2),
            "rows": counts,
            "job_events_total_bytes": ev_bytes,
            "job_attempts_total_bytes": at_bytes,
        }
    finally:
        await conn.close()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=DSN)
    ap.add_argument("--total", type=int, default=100_000, help="jobs seeded per schema")
    ap.add_argument("--waves", type=int, default=8)
    ap.add_argument("--batch", type=int, default=250, help="dispatch batch size")
    ap.add_argument("--heartbeat-ticks", type=int, default=3)
    ap.add_argument("--fillfactor-b", type=int, default=50)
    ap.add_argument("--keep-schemas", action="store_true")
    ap.add_argument(
        "--skip-b", action="store_true", help="skip the fillfactor-50 comparison schema"
    )
    args = ap.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    admin = await asyncpg.connect(args.dsn)
    print(f"seeding {SCHEMA_A} ({args.total} jobs, stock DDL)…")
    await setup_schema(admin, SCHEMA_A)
    if not args.skip_b:
        print(f"seeding {SCHEMA_B} ({args.total} jobs, jobs fillfactor={args.fillfactor_b})…")
        await setup_schema(admin, SCHEMA_B)
        await admin.execute(f'ALTER TABLE "{SCHEMA_B}".jobs SET (fillfactor = {args.fillfactor_b})')
    await admin.close()

    results: dict = {
        "args": vars(args) | {"dsn": "<redacted>"},
        "a": None,
        "b": None,
    }

    print(f"\n== schema A ({SCHEMA_A}): stock DDL (fillfactor 100) ==")
    results["a"] = await run_churn(
        args.dsn, SCHEMA_A, args.total, args.waves, args.batch, args.heartbeat_ticks
    )
    if not args.skip_b:
        print(f"\n== schema B ({SCHEMA_B}): jobs fillfactor={args.fillfactor_b} ==")
        results["b"] = await run_churn(
            args.dsn, SCHEMA_B, args.total, args.waves, args.batch, args.heartbeat_ticks
        )

    out = RESULTS_DIR / "pg_churn_probe.json"
    out.write_text(json.dumps(results, indent=2, default=str))
    print(f"\nwrote {out}")

    if not args.keep_schemas:
        cleanup = await asyncpg.connect(args.dsn)
        for s in (SCHEMA_A, SCHEMA_B):
            if args.skip_b and s == SCHEMA_B:
                continue
            await truncate_schema(cleanup, s)
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{s}" CASCADE')
        await cleanup.close()
        print("dropped scratch schemas")


if __name__ == "__main__":
    asyncio.run(main())
