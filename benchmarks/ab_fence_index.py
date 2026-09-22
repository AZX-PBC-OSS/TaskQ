"""A/B: the fenced terminal write's driver index, on real Postgres.

A = current `jobs_locked_by_worker_running_idx (locked_by_worker)
WHERE status='running'` (the pre-01.00.19_01 form).  B = the same index
with the job id as a trailing KEY column
(`(locked_by_worker, id) WHERE status='running'`).  The statement is
the production fused `sql.mark_succeeded` on both sides; only the index
differs.

Protocol (the perf-evidence house rules): every sample is
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of the production statement in
a migrated throwaway schema; the statement really executes, so the
backlog is re-seeded (TRUNCATE + INSERT + VACUUM ANALYZE) before every
sample; A and B run in interleaved seeds; median of 5 recorded samples.
Two stats regimes: fresh bulk seed analyzed, and the same seed left
unanalyzed (the stale-`reltuples` window a bulk churn opens before
autovacuum's next ANALYZE) — the regime where the planner's estimate
collapse mispicks the driver index.

Read-only with respect to src/.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own benchmark-controlled schema identifier; every value is $n-bound (the same exemption the suite's SQL pins document).

from __future__ import annotations

import asyncio
import json
import statistics
from pathlib import Path

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.migrate import apply_pending

DSN = "postgresql://taskq:taskq@localhost:55433/taskq"
SCHEMA_A = "tq_fence_ab_a"
SCHEMA_B = "tq_fence_ab_b"
ACTOR = "bench_actor"
QUEUE = "default"
WARMUP = 1
SAMPLES = 5


async def make_schema(schema: str, index_sql: str) -> tuple[asyncpg.Connection, str]:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await apply_pending(conn, schema=schema)
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
        ACTOR,
        QUEUE,
    )
    await conn.execute(f'DROP INDEX IF EXISTS "{schema}".jobs_locked_by_worker_running_idx')
    await conn.execute(index_sql.format(schema=schema))
    return conn, schema


IDX_A = "CREATE INDEX jobs_locked_by_worker_running_idx ON \"{schema}\".jobs (locked_by_worker) WHERE status = 'running'"
IDX_B = "CREATE INDEX jobs_locked_by_worker_running_idx ON \"{schema}\".jobs (locked_by_worker, id) WHERE status = 'running'"


async def seed(conn: asyncpg.Connection, schema: str, running_pop: int, analyze: bool) -> tuple:
    # All f-string SQL below interpolates only this module's own
    # benchmark-controlled schema identifier; every value is $n-bound
    # (the same S608 exemption the test suite's SQL pins document).
    wid = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, 0, $3)',
        wid,
        "ab-host",
        [QUEUE],
    )
    ids = [new_job_id() for _ in range(running_pop)]
    await (
        conn.executemany(  # Why: benchmark-controlled schema identifier only; values are $n-bound.
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            f"retry_kind, status, attempt, claim_epoch, locked_by_worker, started_at, "
            f"lock_expires_at) VALUES ($1, $2, $3, '{{}}'::jsonb, 3, 'transient', "
            f"'running', 1, 1, $4, clock_timestamp(), clock_timestamp() + interval '5 minutes')",
            [(i, ACTOR, QUEUE, wid) for i in ids],
        )
    )
    if analyze:
        await conn.execute(f'VACUUM ANALYZE "{schema}".jobs')
    return wid, ids


async def sample(
    conn: asyncpg.Connection, schema: str, running_pop: int, analyze: bool, sql_stmt: str
) -> dict:
    await conn.execute(
        f'TRUNCATE "{schema}".jobs, "{schema}".workers, "{schema}".job_attempts, "{schema}".job_events, "{schema}".maintenance_leader CASCADE'
    )
    wid, ids = await seed(conn, schema, running_pop, analyze)
    rows = await conn.fetch(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql_stmt}",
        ids[0],
        wid,
        '{"ok":1}',
        8,
        0,
        None,
        None,
        1,
        1,
    )
    plan = json.loads(rows[0][0])[0]

    # find the fence scan node
    def walk(n, out):
        out.append(n)
        for c in n.get("Plans", []):
            walk(c, out)

    nodes: list[dict] = []
    walk(plan["Plan"], nodes)
    # Verification gate: EXPLAIN ANALYZE really executed the statement, so
    # every recorded sample must have matched and written the row whole
    # (jobs succeeded + the attempt row + the event row). A sample whose
    # UPDATE matched nothing (a harness/bind bug) must abort the
    # measurement, not report a fictitious sub-millisecond time.
    post = await conn.fetchrow(
        f"SELECT status = 'succeeded' AS ok FROM \"{schema}\".jobs WHERE id = $1",
        ids[0],
    )
    attempts = await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts')
    events = await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_events')
    if not post or not post["ok"] or attempts == 0 or events == 0:
        raise RuntimeError(
            f"sample verification failed: ok={post and post['ok']} "
            f"attempts={attempts} events={events} - discarding run"
        )
    upd = next(n for n in nodes if n.get("Node Type") == "ModifyTable")
    scan = next(
        (
            n
            for n in nodes
            if n.get("Node Type") in ("Index Scan", "Index Only Scan", "Seq Scan", "TID Scan")
            and n.get("Relation Name") == "jobs"
        ),
        {},
    )
    return {
        "exec_ms": plan["Execution Time"],
        "driver_index": scan.get("Index Name"),
        "removed_by_filter": scan.get("Rows Removed by Filter"),
        "fence_scan_ms": scan.get("Actual Total Time"),
        "buffers": plan["Plan"].get("Shared Hit Blocks")
        if "Plan" in plan
        else upd.get("Shared Hit Blocks"),
        "total_buffers": plan["Plan"]["Shared Hit Blocks"],
    }


async def index_size(conn: asyncpg.Connection, schema: str) -> int:
    return await conn.fetchval(
        f"SELECT pg_relation_size('\"{schema}\".jobs_locked_by_worker_running_idx')"
    )


async def main() -> None:
    from taskq.backend._sql_templates import render

    pops = [8, 64, 512, 2000]
    conns = {
        "A": await make_schema(SCHEMA_A, IDX_A),
        "B": await make_schema(SCHEMA_B, IDX_B),
    }
    sql_stmt = {"A": render(SCHEMA_A).mark_succeeded, "B": render(SCHEMA_B).mark_succeeded}
    results: list[dict] = []
    failures = 0
    try:
        for pop in pops:
            for analyze in (True, False):
                a_samples, b_samples = [], []
                for rep in range(WARMUP + SAMPLES):
                    for tag in ("A", "B"):
                        conn, schema = conns[tag]
                        try:
                            r = await sample(conn, schema, pop, analyze, sql_stmt[tag])
                        except RuntimeError:
                            failures += 1
                            n_jobs = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
                            print(
                                f"!! sample no-op: variant={tag} pop={pop} "
                                f"analyzed={analyze} rep={rep} jobs_in_schema={n_jobs}"
                            )
                            continue
                        r.update(variant=tag, running_pop=pop, analyzed=analyze, rep=rep)
                        (a_samples if tag == "A" else b_samples).append(r)
                for tag, ss in (("A", a_samples), ("B", b_samples)):
                    rec = ss[WARMUP:]  # drop warmup
                    if not rec:
                        results.append(
                            {
                                "running_pop": pop,
                                "analyzed": analyze,
                                "variant": tag,
                                "discarded": True,
                            }
                        )
                        continue
                    med = statistics.median(x["exec_ms"] for x in rec)
                    med_scan = statistics.median(x["fence_scan_ms"] or 0 for x in rec)
                    removed = rec[0]["removed_by_filter"]
                    driver = rec[0]["driver_index"]
                    size = await index_size(conns[tag][0], SCHEMA_A if tag == "A" else SCHEMA_B)
                    results.append(
                        {
                            "running_pop": pop,
                            "analyzed": analyze,
                            "variant": tag,
                            "exec_ms_median": med,
                            "fence_scan_ms_median": med_scan,
                            "driver_index": driver,
                            "removed_by_filter": removed,
                            "index_bytes": size,
                        }
                    )
                    print(json.dumps(results[-1]))
    finally:
        print(f"samples discarded (no-op executions): {failures}")
        for conn, schema in conns.values():
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await conn.close()
        Path(__file__).parent.joinpath("results").mkdir(exist_ok=True)
        out = Path(__file__).parent / "results" / "fence-index-ab.json"
        out.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
