"""The CLI face's live demo: fill the cache, hit it, then list + evict."""

from __future__ import annotations

import asyncio

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step

DSN = "postgresql://postgres:taskq@localhost:5810/taskq"
SCHEMA = "cli_face_demo"


class Doc(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


CALLS = {"ocr": 0}


async def ocr(ctx: StepContext, d: Doc) -> Report:
    CALLS["ocr"] += 1
    return Report(ref=d.doc_id)


async def consume(ctx: StepContext, r: Report) -> str:
    return r.ref


app = WorkflowApp()


@app.workflow("cli_flow")
def flow() -> Promise[object]:
    o = step(ocr, Doc(doc_id="d1"), key="ocr", cache=True)
    return build(step(consume, o, key="consume"))


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)
    compiled = app.get("cli_flow")
    for key in ("cli:a", "cli:b"):
        runner = FlowRunner(compiled, pool, SCHEMA)
        claim = await runner.create_flow(input=None, run_key=key)
        await runner.drive(claim.flow_id)
    await pool.close()
    print("calls:", CALLS)


asyncio.run(main())
