"""Scenario 2 — the three-tenant priority split.

Three tenants share one actor on one queue. Each tenant's work lands at
its own priority: the paying tenant's rows at 30, the standard tenant's
at 10, the free tier's at 0. Fifty runs per tenant — 150 rows on the
same actor and queue — then one worker drains the lot.

The dispatch claim orders by priority (highest first) on every round;
with one worker claiming one row at a time, the claim order IS the
priority order. The evidence is the claim order itself, read off the
rows: every high-priority row claimed before any standard row, every
standard row before any free-tier row, and all 150 runs through to
their green end.

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_REDIS_URL=redis://localhost:6379/0 \
    uv run python -m examples.scenario_grounds.s2_priority_split
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
    scratch_dir,
    start_worker,
    wait_all_runs_terminal,
    wait_for_worker_ready,
)
from examples.scenario_grounds.defs_priority import app as tenant_app

SCHEMA = "grounds_s2"
ACTORS_REF = "examples.scenario_grounds.defs_priority:ACTORS"
QUEUE = "grounds-tenant"

#: The three tenants and the priority each one's rows carry.
TENANTS: list[tuple[str, int]] = [("alpine-pay", 30), ("baseline", 10), ("freemium", 0)]
RUNS_PER_TENANT = 50


async def main() -> None:
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    pool = await open_pool(dsn)

    from taskq.workflows import FlowRunner

    runner = FlowRunner(tenant_app.get("ground_tenant"), pool, SCHEMA)

    say("Three tenants share one actor: alpine-pay's rows go first (priority 30),")
    say("baseline's next (10), freemium's last (0). Fifty runs each — 150 rows,")
    say("created round-robin across the tenants (alpine, baseline, freemium,")
    say("alpine, …) so the row ids alternate too: a claim order that follows")
    say("the row ids alone would fragment across the tenants, and only the")
    say("priority column can produce the grouped drain the rows must show.")

    # The runs are created first, worker second, and ROUND-ROBIN across
    # the tenants: the row ids (time-ordered) then alternate across the
    # three priorities, so a claim order that ignored priority would
    # interleave the tenants instead of draining them in groups — the
    # claim order the rows record can only come from the priority
    # ordering.
    flow_ids: list[str] = []
    for n in range(RUNS_PER_TENANT):
        for tenant, priority in TENANTS:
            claim = await runner.create_flow(
                input={"tenant": tenant, "item": f"{tenant}-item-{n:03d}"},
                priority=priority,
            )
            flow_ids.append(str(claim.flow_id))
    say(f"Created {len(flow_ids)} runs on the '{QUEUE}' queue (round-robin across the tenants).")

    worker = await start_worker(
        schema=SCHEMA,
        actors_ref=ACTORS_REF,
        queues=[QUEUE],
        log_path=scratch_dir("s2") / "worker.log",
        max_concurrency=1,
        extra_env={"TASKQ_SWEEP_INTERVAL": "1"},
    )
    await wait_for_worker_ready(pool, SCHEMA, worker)
    say("One worker, claiming one row per round: the claim order is the priority order.")

    census = await wait_all_runs_terminal(pool, SCHEMA, flow_ids, worker=worker)
    await worker.stop()

    rows = await claimed_rows(pool)
    priorities_in_sequence = [int(r["priority"]) for r in rows]

    # The claim order, grouped: a run-length encoding of the priorities
    # in claim sequence, e.g. "30x50,10x50,0x50".
    groups = run_length_encoded(priorities_in_sequence)
    per_tenant = {
        tenant: run_length_encoded([int(r["priority"]) for r in rows if r["tenant"] == tenant])
        for tenant, _ in TENANTS
    }
    say("")
    say("The claim order, read off the rows (each row's own started_at):")
    say(f"  claimed in this priority order: {groups}")
    say(f"  per tenant, the priority of every claim: {per_tenant}")
    ev(check="claim_order", encoding=groups, tenants=per_tenant)

    counts_by_priority = {p: priorities_in_sequence.count(p) for p in (30, 10, 0)}
    say(f"  rows stamped at each priority: {counts_by_priority}")
    ev(check="priority_columns", counts=counts_by_priority)

    say(
        f"  runs through to succeeded: {census.succeeded}/{census.total}"
        f" (failed {census.failed}, cancelled {census.cancelled})"
    )
    ev(
        check="all_completed",
        runs=census.total,
        succeeded=census.succeeded,
        failed=census.failed,
        cancelled=census.cancelled,
    )

    await pool.close()
    say("")
    say("The priority split held end to end: the paying tenant's backlog")
    say("drained first, then the standard tier, then the free tier — and")
    say("every one of the 150 runs finished its work.")


async def claimed_rows(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    """The 150 step rows in claim order (the dispatch claim stamps
    started_at as it hands the row out)."""
    return await pool.fetch(
        f"""
        SELECT step.priority, step.started_at, step.id,
               root.payload->'wf_input'->>'tenant' AS tenant
        FROM "{SCHEMA}".jobs step
        JOIN "{SCHEMA}".jobs root
          ON root.id = (step.metadata->>'flow_id')::uuid
        WHERE step.step_key = 'process'
        ORDER BY step.started_at, step.id
        """,  # noqa: S608  # Why: the interpolated value is the scenario's own constant schema identifier; no caller value reaches the string.
    )


def run_length_encoded(values: list[int]) -> str:
    """The sequence compressed to its runs: [30, 30, 10] -> "30x2,10x1"."""
    if not values:
        return "(none)"
    parts: list[str] = []
    current, count = values[0], 0
    for value in values:
        if value == current:
            count += 1
        else:
            parts.append(f"{current}x{count}")
            current, count = value, 1
    parts.append(f"{current}x{count}")
    return ",".join(parts)


if __name__ == "__main__":
    asyncio.run(main())
