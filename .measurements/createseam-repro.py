"""THE CREATE-SEAM LIVE REPRODUCTION (the attack corpus's findings,
re-manifested on the pre-cure head): the N+M+2 auto-committed create's
orphan root + the run-key squat + the claim churn.

Run: TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5733/taskq \
     uv run --no-sync python .measurements/createseam-repro.py
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import timedelta

import asyncpg

DSN = os.environ.get("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5733/taskq")
SCHEMA = "createseam_repro"

from pydantic import BaseModel  # noqa: E402

from taskq.backend._dispatch_sql import (  # noqa: E402
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)
from taskq.backend._protocol import JobId  # noqa: E402
from taskq.workflows import FlowRunner, WorkflowApp, build, step  # noqa: E402
from taskq.workflows.engine import render_workflow_sql  # noqa: E402


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


async def _prepare(ctx: object, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _tail(ctx: object, t: Report) -> dict[str, str]:
    return {"tail": t.ref}


app = WorkflowApp()


@app.workflow("repro_flow")
def repro_flow() -> object:
    a = step(_prepare, Ingest(doc_id="d1"), key="a")
    return build(step(_tail, a, key="tail"))


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()

    from taskq.migrate import apply_pending

    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=SCHEMA)
    await conn.close()

    pool = await asyncpg.create_pool(DSN)
    compiled = app.get("repro_flow")
    runner = FlowRunner(compiled, pool, SCHEMA)

    # A workflow-CAPABLE worker on the flow's queue (the churn's subject).
    worker_id = uuid.uuid4()
    await pool.execute(
        f'INSERT INTO "{SCHEMA}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'repro', 1, $2::text[], $3::jsonb)",
        worker_id,
        ["default"],
        json.dumps({"workflow_execution": True}),
    )
    # The boot projection: the workflow cohorts land in actor_config.
    from taskq.workflows import _worker_execution as seam

    for config in seam.project_workflow_actor_configs():
        await pool.execute(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )

    print("== FINDING 1a: the kill after the root insert -> the ORPHAN ROOT ==")

    async def _killed_nodes(conn: object, flow_id: JobId, input: object) -> None:
        raise RuntimeError("the kill: the process dies after the root insert")

    runner._insert_static_nodes = _killed_nodes  # type: ignore[method-assign]
    try:
        await runner.create_flow(input=Ingest(doc_id="d1"), run_key="repro:kill")
    except RuntimeError as exc:
        print(f"  create_flow raised: {exc}")
    runner._insert_static_nodes = FlowRunner._insert_static_nodes.__get__(runner)  # type: ignore[method-assign]

    root = await pool.fetchrow(
        f'SELECT id, status::text AS status, step_key FROM "{SCHEMA}".jobs '
        "WHERE step_key = '__flow__' AND idempotency_key = 'repro:kill'"
    )
    if root is not None:
        print(f"  ORPHAN: root {root['id']} status={root['status']} (the PRE-CURE debris)")
    else:
        print(
            "  CURED: the kill after the root insert committed NOTHING — the "
            "orphan root is UNREPRESENTABLE (the create is one transaction)."
        )

    print("== FINDING 2: the CLAIM CHURN — a capable worker claims the pending root row ==")
    if root is None:
        print("  SKIPPED: no pending root exists to churn (the tx cure) — the")
        print("  ROOT-MARKER fence's own pin carries the drill (the churn")
        print("  observed on the mutated statement, the shipped one green).")
    claimed: list = []
    if root is not None:
        from taskq.backend._dispatch_sql import (  # noqa: F401
            DISPATCH_STRICT_FIFO_SQL,
            dispatch_batch,
        )

        claimed = await dispatch_batch(
            pool,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=SCHEMA),
            queues=["default"],
            limit_n=10,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )
        print(f"  dispatch_batch claimed {len(claimed)} row(s)")
        for record in claimed:
            print(
                f"    claimed row step_key={record['step_key']!r} "
                "(the ROOT row — never real work)"
            )
        if claimed:
            await pool.execute(
                f'UPDATE "{SCHEMA}".jobs SET status = \'pending\', '
                "locked_by_worker = NULL, lock_expires_at = NULL WHERE id = $1",
                record["id"],
            )

    print("== FINDING 1b: the orphan SQUATS THE RUN KEY — the retry never completes ==")
    runner2 = FlowRunner(compiled, pool, SCHEMA)
    claim2 = await runner2.create_flow(input=Ingest(doc_id="d1"), run_key="repro:kill")
    flow_id2 = claim2.flow_id
    count2 = await pool.fetchval(
        f'SELECT count(*) FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        flow_id2,
    )
    print(f"  retry: claim kind={claim2.kind}, flow_id={flow_id2}, node count={count2}")
    verdict = await runner2.drive(flow_id2, max_ticks=50)
    print(f"  drive() -> {verdict}  (max_ticks = the wedged run; terminal = the cure)")

    print("== FINDING 3: the FAILED run + the same key = the silent no-op ==")
    if root is not None:
        await pool.execute(
            f'UPDATE "{SCHEMA}".jobs SET status = \'failed\', finished_at = now() '
            "WHERE id = $1",
            root["id"],
        )
        claim3 = await runner2.create_flow(input=Ingest(doc_id="d1"), run_key="repro:kill")
        flow_id3 = claim3.flow_id
        status3 = await pool.fetchval(
            f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1', flow_id3
        )
        count3 = await pool.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
            flow_id3,
        )
        print(
            f"  retry against the FAILED run: kind={claim3.kind} status={claim3.status}, "
            f"flow_id={flow_id3} (the FAILED run's id), "
            f"nodes={count3} — the TYPED claim states the terminal verdict, nothing re-fired."
        )
    else:
        print("  SKIPPED: no orphan to terminal-fail (the tx cure); the honest")
        print("  claim's pin carries the existing-terminal verdict (typed, loud).")

    await pool.close()
    print(json.dumps({"repro": "complete"}))


if __name__ == "__main__":
    asyncio.run(main())
