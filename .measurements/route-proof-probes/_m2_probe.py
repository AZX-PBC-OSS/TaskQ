"""M2's conviction probe: with the status gate dropped, a DELIVERED hold
whose decision payload carries its own `reason` key serves that decision
reason back as the HOLD's reason — the post-delivery lie, live."""

import asyncio

import asyncpg
from pydantic import BaseModel

from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.api import GateDecl
from taskq.workflows.api._hitl import HitlClient

DSN = "postgresql://postgres:taskq@localhost:5768/taskq"
SCHEMA = "m2probe"


class Ingest(BaseModel):
    doc_id: str = "d1"


class Approval(BaseModel):
    verdict: str
    reason: str = ""  # the decision MAY legitimately carry its own why


class Decision(BaseModel):
    ok: bool


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    from taskq.migrate import apply_pending

    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await apply_pending(conn, schema=SCHEMA)

    app = WorkflowApp()

    async def gated(ctx: StepContext, params: Ingest) -> Decision:
        outcome = await ctx.wait_signal(
            (Approval,), timeout_s=30.0, reason="awaiting compliance sign-off"
        )
        assert isinstance(outcome, Approval)
        return Decision(ok=True)

    @app.workflow("m2probe")
    def wf() -> Promise[Decision]:
        return build(step(gated, Ingest(), gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=30.0),)))

    compiled = app.get("m2probe")
    pool = await asyncpg.create_pool(DSN)
    runner = FlowRunner(compiled, pool, SCHEMA)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    client = HitlClient(pool, schema=SCHEMA)
    pre = await client.list(flow_id)
    print("PRE-delivery reason:", pre[0].reason)

    # THE RESOLVE whose decision payload carries its own reason key.
    res = await client.resolve(pre[0].hold_id, {"verdict": "approve", "reason": "THE DECISION'S WHY"})
    assert res.status == "delivered", res

    post = await client.get(pre[0].hold_id)
    print("POST-delivery status:", post.status if post else None)
    print("POST-delivery reason:", post.reason if post else None)
    await pool.close()
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()


asyncio.run(main())
