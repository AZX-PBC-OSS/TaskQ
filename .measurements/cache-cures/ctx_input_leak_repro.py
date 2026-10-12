"""THE ctx.input CROSS-TENANT LEAK — the live repro (CURE 3's red receipt).

The address covers the WIRED args only. A body reading
``ctx.input["tenant_id"]`` (the FLOW input — never in the address)
serves tenant A's answer to tenant B: terminal-success, audits clean.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, run, step

DSN = "postgresql://postgres:taskq@localhost:5810/taskq"
SCHEMA = "ctx_leak_repro"


class Report(BaseModel):
    answer: str


CALLS = {"tenant_body": 0}


async def tenant_body(ctx: StepContext, name: str) -> Report:
    """THE LEAKY BODY: the tenant identity rides the FLOW input
    (``ctx.input``) — which the address NEVER covers."""
    CALLS["tenant_body"] += 1
    tenant = ctx.input["tenant_id"]
    return Report(answer=f"{tenant}:{name}")


app = WorkflowApp()


@app.workflow("leak_flow")
def flow() -> Promise[object]:
    return build(step(tenant_body, "fixed-name", key="tstep", cache=True))


async def main() -> None:
    conn0 = await asyncpg.connect(DSN)
    await conn0.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn0.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)

    print("=== RUN A (tenant A pays — the store) ===")
    r1 = await run(
        app.get("leak_flow"), pool, SCHEMA, input={"tenant_id": "tenant-A"}, key="leak:a"
    )
    print("status:", await _status(pool, str(r1.flow_id)))
    print("result:", await _result(pool, str(r1.flow_id), "tstep"))
    print("calls:", CALLS)

    print("=== RUN B (tenant B — the SAME wired args, the OTHER flow input) ===")
    r2 = await run(
        app.get("leak_flow"), pool, SCHEMA, input={"tenant_id": "tenant-B"}, key="leak:b"
    )
    print("status:", await _status(pool, str(r2.flow_id)))
    print("result:", await _result(pool, str(r2.flow_id), "tstep"))
    print("calls:", CALLS)
    hit = await pool.fetchval(
        f'SELECT metadata->\'wf_cache_hit\'->>\'run_id\' FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'tstep'",
        r2.flow_id,
    )
    print("B's receipt names:", hit, "(A's run:", str(r1.flow_id) + ")")

    await pool.close()


async def _status(pool: asyncpg.Pool, flow_id: str) -> str:
    return await pool.fetchval(f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1', flow_id)


async def _result(pool: asyncpg.Pool, flow_id: str, step_key: str) -> str:
    raw = await pool.fetchval(
        f'SELECT result FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        step_key,
    )
    return raw if isinstance(raw, str) else json.dumps(raw)


asyncio.run(main())
