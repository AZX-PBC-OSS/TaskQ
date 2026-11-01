"""Scenario 3 — the max_concurrent damper.

One actor, capped at two seats. Ten flows arrive at once, each one step
long, each body slow enough that the cap has to bite. The dispatch
claim reads the actor's stored cap and admits at most two of its rows
per round, so at no moment are more than two of the ten running.

The evidence is read off the rows themselves: every step row's own
start and finish instants, folded into one overlap count. It never
rises above two, it does reach two (the cap genuinely bound — an actor
that never ran two at once would prove nothing), and all ten flows end
succeeded.

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_REDIS_URL=redis://localhost:6379/0 \
    uv run python -m examples.scenario_grounds.s3_max_concurrent
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import asyncpg

from examples.scenario_grounds._common import (
    ev,
    flow_root_status,
    fresh_schema,
    open_pool,
    pg_dsn,
    say,
    scratch_dir,
    start_worker,
    wait_all_runs_terminal,
    wait_for_worker_ready,
)
from examples.scenario_grounds.defs_damper import app as damper_app

SCHEMA = "grounds_s3"
ACTORS_REF = "examples.scenario_grounds.defs_damper:ACTORS"
QUEUE = "grounds-damped"
RUNS = 10
CAP = 2


async def main() -> None:
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    pool = await open_pool(dsn)

    from taskq.workflows import FlowRunner

    runner = FlowRunner(damper_app.get("ground_damped"), pool, SCHEMA)

    say(f"Ten flows arrive at once, all for one actor that admits {CAP} at a time;")
    say("each body runs about 0.7 s, so the cap has to hold the line for the rest.")

    flow_ids: list[str] = []
    for n in range(RUNS):
        claim = await runner.create_flow(input={"run": n})
        flow_ids.append(str(claim.flow_id))
    say(f"Created {RUNS} runs on the '{QUEUE}' queue.")

    worker = await start_worker(
        schema=SCHEMA,
        actors_ref=ACTORS_REF,
        queues=[QUEUE],
        log_path=scratch_dir("s3") / "worker.log",
        max_concurrency=RUNS,
        extra_env={"TASKQ_SWEEP_INTERVAL": "1"},
    )
    await wait_for_worker_ready(pool, SCHEMA, worker)
    say(f"One worker with {RUNS} free hands — the actor's own two-seat cap is the only limit.")

    census = await wait_all_runs_terminal(pool, SCHEMA, flow_ids, worker=worker, timeout=180)
    await worker.stop()

    peak_in_flight = await peak_overlap(pool)
    succeeded = 0
    for fid in flow_ids:
        if await flow_root_status(pool, SCHEMA, fid) == "succeeded":
            succeeded += 1

    say("")
    say("The rows' own clocks, folded into one running count:")
    say(f"  peak in flight: {peak_in_flight} (the cap is {CAP})")
    say(f"  the cap was reached: {peak_in_flight >= CAP}")
    say(
        f"  runs through to succeeded: {census.succeeded}/{census.total}"
        f" (failed {census.failed}, cancelled {census.cancelled})"
    )
    ev(check="never_over_cap", peak_in_flight=peak_in_flight, cap=CAP)
    ev(check="cap_reached", reached=peak_in_flight >= CAP)
    ev(
        check="all_completed",
        runs=census.total,
        succeeded=census.succeeded,
        failed=census.failed,
        cancelled=census.cancelled,
    )
    assert succeeded == census.total, "the census and the root rows disagree"

    await pool.close()
    say("")
    say("The damper held: ten runs, two seats, nobody rushed and nobody")
    say("left behind — every flow finished its work.")


async def peak_overlap(pool: asyncpg.Pool) -> int:
    """The most rows ever running at once, folded from the step rows'
    own start and finish instants."""
    rows = await pool.fetch(
        f"""
        SELECT started_at, finished_at FROM "{SCHEMA}".jobs
        WHERE step_key = 'work' ORDER BY started_at
        """,  # noqa: S608  # Why: the schema is the scenario's own constant identifier.
    )
    events: list[tuple[datetime, int]] = []
    for row in rows:
        if row["started_at"] is not None:
            events.append((row["started_at"], 1))
        if row["finished_at"] is not None:
            events.append((row["finished_at"], -1))
    # A finish and a start at the same instant: the seat frees first.
    events.sort(key=lambda pair: (pair[0], pair[1]))
    peak = 0
    running = 0
    for _, delta in events:
        running += delta
        peak = max(peak, running)
    return peak


if __name__ == "__main__":
    asyncio.run(main())
