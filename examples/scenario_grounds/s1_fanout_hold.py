"""Scenario 1 — the fan-out with the approval hold.

A document batch fans out to two queues: the light items take the fast
lane, the dense ones the heavy lane. When both lanes' work has landed,
the flow stops at an approval step — a human gate — and waits. The
human approves; the flow publishes and finishes.

The evidence: both lanes' rows on their own queues, every one claimed
and finished; the hold's own lifecycle (minted held, then delivered by
the resolve); the run's terminal state with its published result.

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    uv run python -m examples.scenario_grounds.s1_fanout_hold
"""

from __future__ import annotations

import asyncio

import asyncpg

from examples.scenario_grounds._common import (
    ev,
    fresh_schema,
    open_pool,
    pg_dsn,
    say,
)
from examples.scenario_grounds.defs_fanout import Batch
from examples.scenario_grounds.defs_fanout import app as fanout_app

SCHEMA = "grounds_s1"


async def main() -> None:
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    pool = await open_pool(dsn)

    from taskq.workflows import FlowRunner, HitlClient, run

    say("A batch of five documents fans out: three take the fast lane,")
    say("two the heavy lane. The flow then stops at an approval gate.")
    outcome = await run(
        fanout_app.get("ground_fanout"),
        pool,
        SCHEMA,
        input=Batch(fast=["doc-a", "doc-b", "doc-c"], heavy=["doc-d", "doc-e"]),
        until="held",
    )
    flow_id = str(outcome.flow_id)
    say(f"The flow is at its gate (run {flow_id}, the drive stopped: {outcome.outcome}).")

    hitl = HitlClient(pool, SCHEMA)
    pending = await hitl.list(run=flow_id)
    for hold in pending:
        say(f"  a hold is waiting: signal {hold.signal_name} on step {hold.node_key}")
    ev(check="hold_held", pending=len(pending), signal=pending[0].signal_name)

    lanes = await lane_census(pool)
    say("")
    say("Both lanes worked while the flow waited:")
    for lane, counts in sorted(lanes.items()):
        say(f"  {lane}: {counts['succeeded']}/{counts['rows']} items finished")
    ev(check="queues_claimed", lanes=lanes)

    hold_id = pending[0].hold_id
    # THE ROW IS THE WITNESS, BOTH ENDS: the hold's status is read off
    # the signal row BEFORE the resolve (the pinned-before face) and
    # again after — the lifecycle is the row's own testimony.
    before_row = await pool.fetchrow(
        f'SELECT status FROM "{SCHEMA}".wf_signals WHERE id = $1',  # noqa: S608  # Why: the schema is the scenario's own constant identifier; the id is bound.
        hold_id,
    )
    before = str(before_row["status"]) if before_row is not None else "gone"
    say("")
    say(f"The hold's row, read before the resolve: {before!r}.")
    delivery = await hitl.resolve(hold_id, {"verdict": "approve", "note": "ship it"})
    hold_row = await pool.fetchrow(
        f'SELECT status FROM "{SCHEMA}".wf_signals WHERE id = $1',  # noqa: S608  # Why: the schema is the scenario's own constant identifier; the id is bound.
        hold_id,
    )
    after = str(hold_row["status"]) if hold_row is not None else "gone"
    say(f"The human approves (hold {hold_id}): the resolve says {delivery.status!r}.")
    say(f"  the hold's row reads: {before} -> {after}")
    ev(check="hold_resolved", before=before, after=after, verdict=delivery.status)

    say("The flow wakes on the answer and publishes:")
    runner = FlowRunner(fanout_app.get("ground_fanout"), pool, SCHEMA)
    await runner.drive(outcome.flow_id, until="terminal", max_ticks=2000)
    root = await pool.fetchval(
        f'SELECT status FROM "{SCHEMA}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is the scenario's own constant identifier; the id is bound.
        outcome.flow_id,
    )
    decoded = await runner.result(outcome.flow_id)
    say(f"  the run's verdict: {root}")
    say(f"  the published result: {decoded}")
    ev(check="terminal", root=str(root), result=str(decoded))

    await pool.close()
    say("")
    say("The whole arc: two lanes fanned out and finished, the gate held,")
    say("one resolve answered it, and the flow published and finished.")


async def lane_census(pool: asyncpg.Pool) -> dict[str, dict[str, int]]:
    """Each lane's ITEM rows (the map's children — the join row that
    packs a lane's results is not an item) and how many finished green."""
    rows = await pool.fetch(
        f"""
        SELECT queue, count(*) AS rows,
               count(*) FILTER (WHERE status = 'succeeded') AS succeeded
        FROM "{SCHEMA}".jobs
        WHERE queue IN ('grounds-fast', 'grounds-heavy')
          AND step_key LIKE '%.item'
        GROUP BY queue
        """,  # noqa: S608  # Why: the schema is the scenario's own constant identifier.
    )
    return {
        str(r["queue"]): {"rows": int(r["rows"]), "succeeded": int(r["succeeded"])} for r in rows
    }


if __name__ == "__main__":
    asyncio.run(main())
