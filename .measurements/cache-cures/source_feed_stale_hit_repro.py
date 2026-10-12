"""THE SOURCE-NODE INPUT BLINDNESS — the live repro (AMENDMENT (A)'s red
receipt, the import-probe's #4 shape).

The cache address covers the WIRED args only. A SOURCE node's body reads
the RUN INPUT (``ctx.input["rows"]`` — the feed spec): the address never
sees it. The 640-row feed stores; the 60-row feed HITS and is served the
640-row result — the content-blind stale hit — terminal-success, audits
clean, until the operator happens to learn to wire the spec in.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, run, step

DSN = "postgresql://postgres:taskq@localhost:5810/taskq"
SCHEMA = "feed_stale_repro"


class Row(BaseModel):
    n: int


class Feed(BaseModel):
    rows: list[Row]


CALLS = {"feed_source": 0}


async def feed_source(ctx: StepContext) -> list[Row]:
    """THE BLIND SOURCE: the run input IS the body's only input — and it
    never enters the address."""
    CALLS["feed_source"] += 1
    rows = ctx.input["rows"]
    return [Row(n=n) for n in rows]


app = WorkflowApp()


@app.workflow("feed_flow")
def flow() -> Promise[object]:
    return build(step(feed_source, key="feed", cache=True))


async def main() -> None:
    conn0 = await asyncpg.connect(DSN)
    await conn0.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn0.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)

    print("=== RUN 1: the 640-ROW feed (the body pays, the store fills) ===")
    r1 = await run(
        app.get("feed_flow"),
        pool,
        SCHEMA,
        input={"rows": list(range(640))},
        key="feed:640",
    )
    print("status:", await _status(pool, str(r1.flow_id)))
    print("result rows:", await _result_rows(pool, str(r1.flow_id), "feed"))
    print("calls:", CALLS)

    print("=== RUN 2: the 60-ROW feed (the SAME address — the stale hit) ===")
    r2 = await run(
        app.get("feed_flow"), pool, SCHEMA, input={"rows": list(range(60))}, key="feed:60"
    )
    print("status:", await _status(pool, str(r2.flow_id)))
    print("result rows:", await _result_rows(pool, str(r2.flow_id), "feed"))
    print("calls:", CALLS, "(the 60-row feed's body NEVER ran)")
    hit = await pool.fetchval(
        f'SELECT metadata->\'wf_cache_hit\'->>\'run_id\' FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'feed'",
        r2.flow_id,
    )
    print("the 60-row run's receipt names:", hit, "(the 640-row run:", str(r1.flow_id) + ")")

    await pool.close()


async def _status(pool: asyncpg.Pool, flow_id: str) -> str:
    return await pool.fetchval(f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1', flow_id)


async def _result_rows(pool: asyncpg.Pool, flow_id: str, step_key: str) -> int:
    raw = await pool.fetchval(
        f'SELECT result FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        step_key,
    )
    doc = json.loads(raw) if isinstance(raw, str) else raw
    return len(doc["value"])


asyncio.run(main())
