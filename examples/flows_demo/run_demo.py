"""The flows demo's orchestrator: one command, the whole story, in-process.

Run from the repo root (a reachable Postgres 18 is the only service —
see README.md for the two-minute ``docker run``):

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_ENVIRONMENT=dev \
        uv run python -m examples.flows_demo.run_demo

The script migrates a fresh schema, arms the retention settings (the
demo's launch surface — five ``TASKQ_WORKFLOW_*_RETENTION_PERIOD``
vars, default OFF), seeds the ``actor_config`` rows (the capacity
terms' projection), then drives ONE run of the demo graph IN-PROCESS
(the dev door — ``FlowRunner``/``drive``; the rows are the same rows a
worker process would drive):

  1. create → the source fans the map out (one item doomed);
  2. drive to the HOLD — the review step waits on a typed ``Approval``;
  3. resolve the hold (``HitlClient.resolve`` — the same rows the
     admin's Resolve form writes);
  4. drive to TERMINAL — the timer fires from the sweep arm, the capped
     publish lands, the report is read back off the run;
  5. print WHAT TO OBSERVE: the node table, the collected failure's
     typed envelope, the actor_config rows, the reservation's slot row.

Nothing here talks to anything but Postgres on localhost. The schema is
left in place so the admin webapp (``README.md``'s second command) can
serve the run for inspection.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import asyncpg

#: The retention arms the demo's launch arms (the zero-means-off
#: convention: a missing or zero value keeps the arm OFF). The leader's
#: sweep loop reads these settings every pass; the demo's act 6 fires
#: the same arms directly so the effect is observable in one command.
RETENTION_ENV: dict[str, str] = {
    "TASKQ_WORKFLOW_EDGE_RETENTION_PERIOD": "7d",
    "TASKQ_WORKFLOW_STEP_LEDGER_RETENTION_PERIOD": "7d",
    "TASKQ_WORKFLOW_JOIN_FIRE_RETENTION_PERIOD": "7d",
    "TASKQ_WORKFLOW_SIGNALS_RETENTION_PERIOD": "7d",
    "TASKQ_WORKFLOW_NODE_PROGRESS_RETENTION_PERIOD": "7d",
}

#: The queues the demo's cohorts ride — declared so the graph's
#: ``validate()`` stays at zero findings (W2-unknown-queue).
DEMO_QUEUES = "flows-src,flows-review,flows-timer,flows-pub"

BANNER = f"\n{'═' * 72}\n"


def _arm_env() -> None:
    """The demo's launch surface: the retention arms + the queue
    declaration. ``setdefault`` — an operator's explicit env wins."""
    os.environ.setdefault("TASKQ_QUEUES", DEMO_QUEUES)
    for name, value in RETENTION_ENV.items():
        os.environ.setdefault(name, value)


def _die(message: str) -> None:
    """The named-error door (the docs fence's own shape): the operator
    gets the fix, never a bare traceback."""
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


async def _seed_actor_config_rows(pool: asyncpg.Pool, schema: str) -> None:
    """The capacity terms' actor_config face: the boot projection (the
    F3 law's call site) computes the cohorts + their declared terms;
    the SAME two-phase upsert the worker boot runs lands the rows."""
    from examples.flows_demo.app import flows_app
    from taskq.worker.startup import sync_actor_config
    from taskq.workflows._worker_execution import (
        project_workflow_actor_configs,
        register_app,
    )

    register_app(flows_app)  # the isolation law: register, then project
    configs = list(project_workflow_actor_configs())

    # sync_actor_config takes a CONNECTION (its own transactions), never a pool.
    async with pool.acquire() as conn:
        await sync_actor_config(conn, configs, schema=schema)

    rows = await pool.fetch(
        f"SELECT actor, queue, max_concurrent, max_pending, result_ttl "  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
        f'FROM "{schema}".actor_config ORDER BY actor'
    )
    print("actor_config rows (the capacity terms' landing):")
    for row in rows:
        print(
            f"  {row['actor']:<15} queue={row['queue']:<13} "
            f"max_concurrent={row['max_concurrent']} max_pending={row['max_pending']} "
            f"result_ttl={row['result_ttl']}"
        )


async def _show_slot_rows(pool: asyncpg.Pool, schema: str, name: str) -> None:
    """The reservation's slot rows (the ensure-slots pass' landing)."""
    rows = await pool.fetch(
        f'SELECT slot_index, job_id FROM "{schema}".reservation_slots '  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
        "WHERE bucket_name = $1 ORDER BY slot_index",
        name,
    )
    print(f"reservation slot rows ({name}):")
    for row in rows:
        print(f"  slot {row['slot_index']}: holder={row['job_id'] or '—'}")
    if not rows:
        print("  (none — the ensure-slots pass did not run)")


async def main() -> None:
    from examples.flows_demo.app import (
        SERIAL_GUARD,
        Digest,
        configure,
        flows_app,
    )
    from taskq.migrate import apply_pending
    from taskq.workflows import FlowRunner, HitlClient

    dsn = os.environ.get("TASKQ_PG_DSN", "")
    if not dsn:
        _die(
            "TASKQ_PG_DSN is not set — the demo drives a real Postgres "
            "(README.md's prerequisites block has the two-minute docker run)"
        )
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "flows_demo")

    _arm_env()

    print(BANNER)
    print("flows demo — the workflow feature's end-to-end story\n")
    print(f"schema: {schema}")
    print("retention arms armed at launch (the leader's sweep loop reads these every pass):")
    for name, value in RETENTION_ENV.items():
        print(f"  {name}={value}")

    conn = await asyncpg.connect(dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    print("migrations: applied\n")

    # THE DEMO'S SCHEMA-BOUND PIECES (the reservation's slot rows live here).
    gadget = configure(schema)

    pool = await asyncpg.create_pool(dsn)
    try:
        await gadget.ensure_slots(pool)
        await _seed_actor_config_rows(pool, schema)

        runner = FlowRunner(flows_app.get("flows_demo"), pool, schema)
        claim = await runner.create_flow()
        run_id = claim.flow_id
        print(f"\nrun created: {claim.kind} — {run_id}")

        # THE FIRST WALL: the drive STOPS AT THE HOLD — the review step
        # waits on a typed Approval (the quiescent stop is 'held').
        state = await runner.drive(run_id, until="held", max_ticks=500)
        if state != "held":
            _die(f"the drive ended {state!r} before the review hold — a wedged run is a defect")
        print("drive #1: the run is HELD at the review gate")

        client = HitlClient(pool, schema)
        holds = await client.list(run_id)
        for hold in holds:
            print(f"  hold: {hold.hold_id} — {hold.signal_name}")
        if not holds:
            _die("the run holds nothing — the HITL gate never minted its hold")

        # THE RESOLVE (the admin's Resolve form writes these same rows:
        # the typed payload validates through the gate's declared model,
        # the CAS delivers, the audit row lands).
        (hold,) = holds
        verdict = await client.resolve(hold.hold_id, {"verdict": "approve", "note": "ship it"})
        print(f"resolve: {verdict.status}")

        # THE RESUME: the timer fires from the sweep's arm, the capped
        # publish lands, the run terminalizes.
        state = await runner.drive(run_id, until="terminal", max_ticks=3000)
        if state != "terminal":
            _die(f"the resume ended {state!r} — a bounded stop is a defect with a name")

        digest = Digest.model_validate(await runner.result(run_id))
        print(f"\nthe terminal report: {digest.model_dump()}")

        root = await pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            run_id,
        )
        nodes = await pool.fetch(
            f'SELECT step_key, status, attempt, map_index FROM "{schema}".jobs '  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            "WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY step_key, map_index NULLS FIRST",
            run_id,
        )
        join_row = await pool.fetchrow(
            f'SELECT status, metadata FROM "{schema}".jobs '  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source.join'",
            run_id,
        )
        assert join_row is not None, "the map join row is missing"
        join_meta = join_row["metadata"]
        join_meta = json.loads(join_meta) if isinstance(join_meta, str) else join_meta

        print(BANNER)
        print("WHAT TO OBSERVE (the run's rows)\n")
        print(f"root status: {root}")
        print("nodes:")
        for row in nodes:
            index = "-" if row["map_index"] is None else str(row["map_index"])
            print(
                f"  {row['step_key']:<15} map_index={index:<3} {row['status']:<10} attempt={row['attempt']}"
            )
        print(
            "\nthe doomed item's COLLECT (the map join row's metadata.failures — the typed FailureInfo envelope):"
        )
        for failure in join_meta.get("failures", []):
            print(f"  {json.dumps(failure)}")
        print(
            f"\nthe overlap guard: {SERIAL_GUARD.entries} bodies entered, "
            f"{len(SERIAL_GUARD.tripped)} overlaps (zero is the cap's law)"
        )
        await _show_slot_rows(pool, schema, gadget.name)

        print(BANNER)
        print("act 6 — the retention arms (one pass of the leader's sweeps)\n")

        # THE RETENTION ARMS, LIVE: the launch armed the five settings;
        # here the demo fires ONE pass of each arm — first with the
        # declared 7-day periods (nothing is aged out yet: the counts
        # read zero), then with a 1-microsecond period — the smallest
        # non-OFF value — against TWO runs: the TERMINAL one (its flow
        # history sweeps) and a LIVE one (a second run parked at the
        # review hold — its rows are untouchable while the flow runs).
        # The zero-means-off sentinel: timedelta(0) skips the arm; the
        # demo's second pass deliberately brushes past it.
        from datetime import timedelta

        from taskq.workflows._sql import WorkflowSql
        from taskq.workflows._sweep import (
            prune_wf_edge,
            prune_wf_join_fire,
            prune_wf_node_progress,
            prune_wf_signals,
            prune_wf_step_ledger,
        )

        wsql = WorkflowSql.build(schema)

        async def _counts(flow_id: object) -> dict[str, int]:
            return {
                "wf_edge": int(
                    await pool.fetchval(
                        f'SELECT count(*) FROM "{schema}".wf_edge WHERE flow_id = $1',  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
                        flow_id,
                    )
                ),
                "wf_step_ledger": int(
                    await pool.fetchval(
                        f'SELECT count(*) FROM "{schema}".wf_step_ledger WHERE flow_id = $1',  # noqa: S608  # Why: as above — the bound parameter carries the run id.
                        flow_id,
                    )
                ),
                "wf_signals": int(
                    await pool.fetchval(
                        f'SELECT count(*) FROM "{schema}".wf_signals WHERE workflow_id = $1',  # noqa: S608  # Why: as above — the bound parameter carries the run id.
                        flow_id,
                    )
                ),
            }

        async def _fire(retention: timedelta) -> dict[str, int]:
            return {
                "wf_edge": await prune_wf_edge(pool, wsql, retention=retention),
                "wf_join_fire": await prune_wf_join_fire(pool, wsql, retention=retention),
                "wf_step_ledger": await prune_wf_step_ledger(pool, wsql, retention=retention),
                "wf_signals": await prune_wf_signals(pool, wsql, retention=retention),
                "wf_node_progress": await prune_wf_node_progress(pool, wsql, retention=retention),
            }

        second = await runner.create_flow()
        await runner.drive(second.flow_id, until="held", max_ticks=500)
        print(f"the LIVE run (held at its review gate): {second.flow_id}")
        live_before = await _counts(second.flow_id)

        declared_pass = await _fire(timedelta(days=7))
        print(
            f"one pass at the DECLARED periods (7d): {declared_pass} rows deleted "
            "— nothing is 7 days old"
        )

        swept = await _fire(timedelta(microseconds=1))
        print(f"one pass at 1µs (the smallest non-OFF value): {swept}")

        terminal_counts = await _counts(run_id)
        live_after = await _counts(second.flow_id)
        print(f"the TERMINAL run's rows after the sweep:  {terminal_counts}")
        print(f"the LIVE run's rows after the sweep:      {live_after}")
        if any(v > 0 for v in terminal_counts.values()):
            print("  (the terminal run's flow history swept — the arms' flow-scoped deletion)")
        lost = {
            k: live_before[k] - live_after[k] for k in live_before if live_before[k] > live_after[k]
        }
        if lost:
            _die(
                f"the retention arms touched a LIVE run's rows ({lost}) — "
                "the flow-scoped guard failed"
            )

        print(BANNER)
        print("keep going (README.md's observe list):")
        print("  uv run uvicorn examples.flows_demo.webapp:app --port 8003   # then open")
        print(f"  http://localhost:8003/admin/workflows/{run_id}  (this run's page)")
        print("  http://localhost:8003/admin/actors                       (the rows above)")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
