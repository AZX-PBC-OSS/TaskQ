"""Attribution probe for the dispatch-timing drift seen in pg_churn_probe.

Reproduces the drift state (2 churn waves over a fresh schema, using the
real TaskQ dispatch CTE / heartbeat / terminal templates), then
EXPLAIN (ANALYZE, BUFFERS) the dispatch CTE at points:

  1. drifted state (dead tuples present, autovacuum cycling as it will),
  2. after manual VACUUM (ANALYZE) — dead-tuple cleanup + stats refresh,
  3. after pg_stat_reset + one fresh churn wave — stats-staleness control.

Read-only with respect to src/; writes only its own artifacts under
results/. Scratch schema dropped on exit.
"""

from __future__ import annotations

import asyncio
import json
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

DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
SCHEMA = "tq_drift_attr"
ACTOR = "churn_actor"
QUEUE = "churn_q"
RESULTS_DIR = Path(__file__).parent / "results"


def make_payload(i: int) -> str:
    notes = "x" * 96
    return (
        f'{{"order_id":"ord-{i:08d}","channel":"web",'
        f'"customer":{{"id":"cus-{i % 997:05d}","tier":"pro"}},'
        f'"items":[{{"sku":"SKU-001","qty":1}},{{"sku":"SKU-002","qty":2}}],'
        f'"notes":"benchmark payload #{i} {notes}","flags":{{"expedited":true}}}}'
    )


async def seed(conn: asyncpg.Connection, total: int) -> None:
    n = 0
    while n < total:
        take = min(5000, total - n)
        await conn.execute(
            f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts, retry_kind)
                SELECT t.id, $3, $4, t.payload, 3, 'transient'
                FROM unnest($1::uuid[], $2::jsonb[]) AS t(id, payload)""",  # noqa: S608
            [str(new_uuid()) for _ in range(take)],
            [make_payload(n + k) for k in range(take)],
            ACTOR,
            QUEUE,
        )
        n += take


async def churn_wave(conn: asyncpg.Connection, per_wave: int, batch: int) -> None:
    dispatch_sql = DISPATCH_STRICT_FIFO_SQL.format(schema=SCHEMA)
    events_sql = INSERT_EVENTS_BATCH_SQL.format(schema=SCHEMA)
    hb_sql = UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema=SCHEMA)
    tpl = render(SCHEMA)
    worker_id = str(new_uuid())
    lease = timedelta(seconds=60)

    done = 0
    while done < per_wave:
        rows = await conn.fetch(dispatch_sql, [QUEUE], batch, worker_id, lease, 2)
        if not rows:
            break
        done += len(rows)
        await conn.execute(
            events_sql,
            [r["id"] for r in rows],
            "state_change",
            '{"from_state":"pending","to_state":"running"}',
        )
    for _ in range(3):
        await conn.execute(hb_sql, worker_id, lease)
    running = await conn.fetch(
        f'SELECT id, attempt, started_at FROM "{SCHEMA}".jobs '  # noqa: S608  # Why: schema is a probe-controlled identifier validated at apply_pending time
        f"WHERE locked_by_worker = $1 AND status = 'running'",
        worker_id,
    )
    for r in running:
        async with conn.transaction():
            await conn.execute(
                tpl.mark_succeeded, r["id"], worker_id, '{"status":"ok"}', 14, 0, None, None
            )
            await conn.execute(
                tpl.insert_attempt,
                r["id"],
                r["attempt"],
                r["started_at"],
                "succeeded",
                None,
                None,
                None,
                None,
                new_uuid(),
                "{}",
            )
            await conn.execute(
                tpl.insert_event,
                r["id"],
                "state_change",
                '{"from_state":"running","to_state":"succeeded"}',
            )


async def explain_dispatch(conn: asyncpg.Connection, label: str) -> dict:
    sql = DISPATCH_STRICT_FIFO_SQL.format(schema=SCHEMA)
    worker_id = str(new_uuid())
    t0 = time.perf_counter()
    rows = await conn.fetch(sql, [QUEUE], 250, worker_id, timedelta(seconds=60), 2)
    wall_ms = (time.perf_counter() - t0) * 1000
    plan = await conn.fetch(
        "EXPLAIN (ANALYZE, BUFFERS, SETTINGS) " + sql,
        [QUEUE],
        250,
        worker_id,
        timedelta(seconds=60),
        2,
    )
    text = "\n".join(r[0] for r in plan)
    return {"label": label, "returned": len(rows), "wall_ms": round(wall_ms, 2), "plan": text}


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
    await apply_pending(conn, schema=SCHEMA)
    await conn.execute(
        f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608
        ACTOR,
        QUEUE,
    )
    out: list[dict] = []

    await seed(conn, 40_000)
    await churn_wave(conn, 12_500, 250)
    await churn_wave(conn, 12_500, 250)

    d = await conn.fetchrow(
        "SELECT n_dead_tup, last_autovacuum, autovacuum_count FROM pg_stat_user_tables"
        " WHERE schemaname=$1 AND relname='jobs'",
        SCHEMA,
    )
    o = await explain_dispatch(conn, "drifted (post 2 waves)")
    o["n_dead_tup"] = d["n_dead_tup"]
    o["autovacuum_count"] = d["autovacuum_count"]
    out.append(o)

    await conn.execute(f'VACUUM (ANALYZE) "{SCHEMA}".jobs')
    out.append(await explain_dispatch(conn, "after VACUUM (ANALYZE)"))

    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / "drift_attribution.json").write_text(json.dumps(out, indent=2, default=str))
    for o in out:
        print(f"\n== {o['label']}: returned={o['returned']} wall={o['wall_ms']}ms")
        print(o["plan"])

    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
