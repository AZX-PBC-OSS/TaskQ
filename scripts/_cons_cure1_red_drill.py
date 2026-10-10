# ruff: noqa
"""THE CURE 1 RED DRILL (the pre-cure head's own evidence): run_display
at 37ab05b8 — the display's entries carry NO step_key, NO map_index.
The captured output feeds .measurements/cons-cure1-reds.json."""

import asyncio
import sys

import asyncpg
from pydantic import BaseModel

from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, map_source, step
from taskq.workflows._progress_read import run_display
from taskq.workflows.engine import render_workflow_sql

DSN = "postgresql://postgres:taskq@localhost:5784/taskq"
SCHEMA = "cons_red_drill"


class Ingest(BaseModel):
    doc_id: str


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    from taskq.migrate import apply_pending

    await apply_pending(conn, schema=SCHEMA)
    await conn.close()

    app = WorkflowApp()

    async def fetch(ctx: StepContext, params: Ingest) -> list[int]:
        return [0, 1, 2]

    async def item(ctx: StepContext, value: int) -> dict:
        return {"risk": value}

    @app.workflow("cons_red_drill_flow")
    def f() -> Promise[object]:
        source = step(fetch, Ingest(doc_id="d1"), key="fetch")
        return build(map_source(source, item))

    pool = await asyncpg.create_pool(DSN)
    runner = FlowRunner(app.get("cons_red_drill_flow"), pool, SCHEMA)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)
    disp = await run_display(pool, render_workflow_sql(SCHEMA), flow_id)
    print("ENTRY KEYS:", sorted({k for v in disp.values() for k in v}))
    missing = [v for v in disp.values() if "step_key" not in v or "map_index" not in v]
    print("ENTRIES:", len(disp), "MISSING IDENTITY:", len(missing))
    await pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
