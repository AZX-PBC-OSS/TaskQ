"""Scenario 5 — retention, live.

Four runs: two plain fork-and-join runs, one whose approval was held
and then answered, and one that stays live, parked at its own gate. The
operator's horizon is five seconds: once a run has been terminal past
that horizon, the maintenance leader's retention sweep retires the
run's history — the edges, the fire ledger, the attempts' ledger rows,
the answered holds — and leaves a live run's history strictly alone.

The evidence: the row counts sampled before any sweep can exist (the
baseline is read while no worker is in the picture — a terminal run's
history is inert until a sweeper appears) and after the sweep prunes,
per table; the survivor's rows still sitting there; and the sweep's own
prune record in the worker's event log (each arm's tick naming the rows
it retired).

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_REDIS_URL=redis://localhost:6379/0 \
    uv run python -m examples.scenario_grounds.s5_retention_live
"""

from __future__ import annotations

import asyncio
import json

import asyncpg

from examples.scenario_grounds._common import (
    WorkerRun,
    ev,
    fresh_schema,
    open_pool,
    pg_dsn,
    say,
    scratch_dir,
    start_worker,
    wait_for_worker_ready,
    wait_until,
)
from examples.scenario_grounds.defs_retained import app as retained_app

SCHEMA = "grounds_s5"
ACTORS_REF = "examples.scenario_grounds.defs_retained:ACTORS"

#: The retention horizon: a run terminal past this age loses its
#: history. Short, so the scenario shows the sweep live — and wide
#: enough that the sweep's first tick lands well past it (the horizon
#: is the story's margin, not a stopwatch).
HORIZON_SECONDS = 5

#: The five flow-history tables the sweep's arms retire, the tick event
#: each arm writes, and each table's flow column (wf_signals names it
#: workflow_id; wf_node_progress has none — it counts in total only).
TABLES: list[tuple[str, str, str | None]] = [
    ("wf_edge", "wf_edge_retention_tick", "flow_id"),
    ("wf_join_fire", "wf_join_fire_retention_tick", "flow_id"),
    ("wf_step_ledger", "wf_step_ledger_retention_tick", "flow_id"),
    ("wf_signals", "wf_signals_retention_tick", "workflow_id"),
    ("wf_node_progress", "wf_node_progress_retention_tick", None),
]


async def main() -> None:
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    pool = await open_pool(dsn)

    from taskq.workflows import FlowRunner, HitlClient, cancel_workflow_run, run

    say("Three runs finish — two plain, one whose approval was held then answered;")
    say(f"their horizon is {HORIZON_SECONDS} seconds. A fourth run stays live at its gate.")
    runner = FlowRunner(retained_app.get("ground_retained"), pool, SCHEMA)
    keeper_runner = FlowRunner(retained_app.get("ground_keeper"), pool, SCHEMA)
    aged_flow_ids: list[str] = []
    for n in range(2):
        claim = await runner.create_flow(input={"subject": f"subject-{n}"})
        aged_flow_ids.append(str(claim.flow_id))
        await runner.drive(claim.flow_id, until="terminal", max_ticks=2000)
    answered = await run(retained_app.get("ground_keeper"), pool, SCHEMA, input=None, until="held")
    aged_flow_ids.append(str(answered.flow_id))

    hitl = HitlClient(pool, SCHEMA)
    holds = await hitl.list(run=str(answered.flow_id))
    delivery = await hitl.resolve(holds[0].hold_id, {"verdict": "approve", "note": "in time"})
    await keeper_runner.drive(answered.flow_id, until="terminal", max_ticks=2000)
    say(f"Two plain runs terminal; the answered run's hold went {delivery.status!r}.")

    keeper = await run(retained_app.get("ground_keeper"), pool, SCHEMA, input=None, until="held")
    keeper_id = str(keeper.flow_id)
    say(f"The keeper run holds at its gate and stays live (run {keeper_id}).")

    # The BASELINE, sampled BEFORE the worker exists: with no worker in
    # the picture nothing can prune, so a slow caller cannot lose the
    # baseline — the rows sit until the sweep's first tick past the
    # horizon. (A terminal run's history is inert; only the sweep
    # retires it.)
    before = await history_counts(pool, aged_flow_ids, keeper_id)
    say("")
    say("The history rows before any sweep (no worker running yet):")
    for table, counts in before.items():
        say(f"  {table}: the aged runs {counts['aged']}, the live run {counts['live']}")
    ev(check="rows_before", counts=dict(before))

    # The leader's sweep runs in the worker; the arms retire a terminal
    # run's history only once the run's own age passes the horizon.
    worker = await start_worker(
        schema=SCHEMA,
        actors_ref=ACTORS_REF,
        queues=["grounds-retained"],
        log_path=scratch_dir("s5") / "worker.log",
        extra_env={
            "TASKQ_SWEEP_INTERVAL": "1",
            "TASKQ_LOG_LEVEL": "DEBUG",
            "TASKQ_WORKFLOW_EDGE_RETENTION_PERIOD": f"PT{HORIZON_SECONDS}S",
            "TASKQ_WORKFLOW_JOIN_FIRE_RETENTION_PERIOD": f"PT{HORIZON_SECONDS}S",
            "TASKQ_WORKFLOW_STEP_LEDGER_RETENTION_PERIOD": f"PT{HORIZON_SECONDS}S",
            "TASKQ_WORKFLOW_SIGNALS_RETENTION_PERIOD": f"PT{HORIZON_SECONDS}S",
            "TASKQ_WORKFLOW_NODE_PROGRESS_RETENTION_PERIOD": f"PT{HORIZON_SECONDS}S",
        },
    )
    await wait_for_worker_ready(pool, SCHEMA, worker)
    say(f"The worker's leader sweeps every second; the horizon is {HORIZON_SECONDS}s.")

    async def aged_history_gone() -> bool:
        after = await history_counts(pool, aged_flow_ids, keeper_id)
        return all(counts["aged"] == 0 for counts in after.values())

    await wait_until(
        aged_history_gone, timeout=60, what="the sweep to retire the aged runs' history"
    )

    after = await history_counts(pool, aged_flow_ids, keeper_id)
    say("")
    say("The history rows after the sweep:")
    for table, counts in after.items():
        say(f"  {table}: the aged runs {counts['aged']}, the live run {counts['live']}")
    ev(check="rows_after", counts=dict(after))

    surviving = sum(counts["live"] for counts in after.values())
    say(f"The live run's history survived untouched: {surviving} rows still there.")
    ev(check="live_run_untouched", surviving_rows=surviving)

    pruned = prune_records(worker)
    say("")
    say("The sweep's own prune record (the leader's event log, per arm):")
    for line in pruned:
        say(f"  {line['event']}: {line['rows_affected']} row(s)")
    ev(check="prune_record", arms_reported=len(pruned))

    # The story leaves the estate as it found it: the keeper's hold is
    # closed by cancelling the run (the operator's own stop).
    stopped = await cancel_workflow_run(
        pool, schema=SCHEMA, flow_id=keeper.flow_id, reason="the scenario's cleanup"
    )
    say(f"The keeper run is cancelled on the way out ({stopped} row(s) flipped).")

    await worker.stop()
    await pool.close()
    say("")
    say("Retention, live: the aged runs' history retired table by table,")
    say("the live run's history untouched, the prune on the record.")


def prune_records(worker: WorkerRun) -> list[dict[str, object]]:
    """The sweep arms' prune record: per arm, the rows it retired (an
    arm's drain may tick more than once; the record sums them)."""
    totals: dict[str, int] = {}
    for _, tick_event, _ in TABLES:
        for line in worker.log_lines_with(tick_event):
            try:
                parsed = json.loads(line)
            except ValueError:
                continue
            totals[tick_event] = totals.get(tick_event, 0) + int(parsed.get("rows_affected", 0))
    records: list[dict[str, object]] = []
    for table, tick_event, _ in TABLES:
        rows = totals.get(tick_event, 0)
        if rows > 0:
            records.append({"event": tick_event, "table": table, "rows_affected": rows})
    return records


async def history_counts(
    pool: asyncpg.Pool, aged_flow_ids: list[str], keeper_id: str
) -> dict[str, dict[str, int]]:
    """Per table: the aged runs' rows and the live run's rows. The
    progress table carries no flow column — its rows are reached
    through their node's jobs row (the sweep's own path)."""
    counts: dict[str, dict[str, int]] = {}
    for table, _, flow_column in TABLES:
        if flow_column is None:
            aged = int(
                await pool.fetchval(
                    f'SELECT count(*) FROM "{SCHEMA}".{table} p '  # noqa: S608  # Why: the schema and table names are this module's own constants.
                    f'JOIN "{SCHEMA}".jobs n ON n.id = p.node_id '
                    "WHERE (n.metadata->>'flow_id')::uuid = ANY($1::uuid[])",
                    aged_flow_ids,
                )
            )
            live = int(
                await pool.fetchval(
                    f'SELECT count(*) FROM "{SCHEMA}".{table} p '  # noqa: S608  # Why: same — the module's own constants.
                    f'JOIN "{SCHEMA}".jobs n ON n.id = p.node_id '
                    "WHERE (n.metadata->>'flow_id')::uuid = $1",
                    keeper_id,
                )
            )
            counts[table] = {"aged": aged, "live": live}
            continue
        aged = int(
            await pool.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".{table} '  # noqa: S608  # Why: the schema, table and column names are this module's own constants; no caller value is interpolated.
                f"WHERE {flow_column} = ANY($1::uuid[])",
                aged_flow_ids,
            )
        )
        live = int(
            await pool.fetchval(
                f'SELECT count(*) FROM "{SCHEMA}".{table} '  # noqa: S608  # Why: same — the module's own constants.
                f"WHERE {flow_column} = $1",
                keeper_id,
            )
        )
        counts[table] = {"aged": aged, "live": live}
    return counts


if __name__ == "__main__":
    asyncio.run(main())
