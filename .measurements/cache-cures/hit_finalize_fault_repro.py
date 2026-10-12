"""THE HIT-FINALIZE FAULT'S TWO SEATS — the live repro (CURE 6a's red).

A fault escaping the HIT path's finalize (the body NEVER ran) is routed
through the BODY ladder — the same fault on the MISS path escapes the
machinery boundary (the infra class: reclaim, never a terminal). A
transient blip terminal-fails a max_attempts=1 step whose body never
ran — one fault, two seats.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step

DSN = "postgresql://postgres:taskq@localhost:5810/taskq"
SCHEMA = "hit_finalize_repro"


class Report(BaseModel):
    ref: str


CALLS = {"body": 0}


async def body(ctx: StepContext, d: Report) -> Report:
    CALLS["body"] += 1
    return Report(ref=d.ref)


app = WorkflowApp()


@app.workflow("hit_fault_flow")
def flow() -> Promise[object]:
    return build(step(body, Report(ref="r1"), key="step", cache=True, max_attempts=1))


async def main() -> None:
    conn0 = await asyncpg.connect(DSN)
    await conn0.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn0.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)
    compiled = app.get("hit_fault_flow")

    print("=== RUN 1 (the miss — the store fills) ===")
    runner1 = FlowRunner(compiled, pool, SCHEMA)
    claim1 = await runner1.create_flow(input=None, run_key="fault:1")
    await runner1.drive(claim1.flow_id)
    print("status:", await _status(pool, str(claim1.flow_id), "__flow__"))
    print("calls:", CALLS)

    print("=== RUN 2 (the hit — a transient blip inside the finalize) ===")
    from taskq.workflows.api._runner import FlowRunner as FR

    runner2 = FR(compiled, pool, SCHEMA)
    claim2 = await runner2.create_flow(input=None, run_key="fault:2")

    real_finalize = runner2._finalize_success

    async def blipping_finalize(flow_id, row, attempt, node, result, **kw):  # type: ignore[no-untyped-def]
        raise asyncpg.exceptions.InterfaceError("the transient blip")

    runner2._finalize_success = blipping_finalize  # type: ignore[method-assign]
    try:
        await runner2.drive(claim2.flow_id)
        print("drive returned (the blip was ABSORBED by the ladder)")
    except Exception as exc:
        print(f"drive RAISED (the escape-point contract): {type(exc).__name__}")
    print("node status:", await _status(pool, str(claim2.flow_id), "step"))
    print("error rows:", await _errors(pool, str(claim2.flow_id)))
    print("calls:", CALLS)

    await pool.close()


async def _status(pool: asyncpg.Pool, flow_id: str, step_key: str) -> str:
    return await pool.fetchval(
        f'SELECT status::text FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        step_key,
    )


async def _errors(pool: asyncpg.Pool, flow_id: str) -> str:
    rows = await pool.fetch(
        f'SELECT error_class, error_message FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND error_class IS NOT NULL",
        flow_id,
    )
    return json.dumps([dict(r) for r in rows])


asyncio.run(main())
