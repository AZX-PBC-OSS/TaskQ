"""V3's spot-verify probe: result()'s WorkflowRunError carries the
FAILING NODE's class — a two-node flow whose SECOND node fails with a
DISTINCTIVE typed exception must name THAT node and THAT class at the
read face."""

import asyncio

import asyncpg
from pydantic import BaseModel

from taskq.exceptions import TaskQError
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step

DSN = "postgresql://postgres:taskq@localhost:5768/taskq"
SCHEMA = "v3probe"


class Ingest(BaseModel):
    n: int = 1


class TheDistinctiveFailure(TaskQError):
    """A named, typed failure the second node raises."""


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    from taskq.migrate import apply_pending

    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await apply_pending(conn, schema=SCHEMA)

    app = WorkflowApp()

    async def fine(ctx: StepContext, params: Ingest) -> Ingest:
        return params

    async def doomed(ctx: StepContext, params: Ingest) -> Ingest:
        raise TheDistinctiveFailure("the second node's own why")

    @app.workflow("v3probe")
    def wf() -> Promise[Ingest]:
        a = step(fine, Ingest(n=1), key="the_fine_node")
        b = step(doomed, a, key="the_doomed_node", max_attempts=1, retry_kind="permanent")
        return build(b)

    pool = await asyncpg.create_pool(DSN)
    runner = FlowRunner(app.get("v3probe"), pool, SCHEMA)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id)
    print("drive:", outcome)
    try:
        await runner.result(flow_id)
        print("V3 REFUTED: result() returned a value on a FAILED run")
    except Exception as exc:
        print("raised:", type(exc).__name__)
        print("message:", str(exc)[:300])
        assert type(exc).__name__ == "WorkflowRunError", type(exc)
        assert "the_doomed_node" in str(exc), "the failing NODE's name is missing"
        assert "TheDistinctiveFailure" in str(exc), "the failing NODE's CLASS is missing"
        print("V3 VERIFIED: WorkflowRunError carries the failing node's class + name")

    await pool.close()
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()


asyncio.run(main())
