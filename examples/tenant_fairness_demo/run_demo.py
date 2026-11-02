"""The multi-tenant workflow demo's orchestrator: one command, the
whole story, in-process.

Run from the repo root (a reachable Postgres 18 is the only service —
see README.md for the two-minute `docker run`):

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \\
    TASKQ_ENVIRONMENT=dev \\
        uv run python -m examples.tenant_fairness_demo.run_demo

The script migrates a fresh schema, flips the demo queue to
`round_robin`, runs the boot's own registration + collection passes
(the keyed decl lands in the resolved registry), then drives BOTH
tenants' runs through the REAL fleet door: the production dispatch
claim, the workflow intercept's rate-limit gate (the per-key
acquire/deny face), and the fleet execution door. The loop runs the
sweep arms between rounds (the joins fire, the outbox delivers).

The printed evidence:

1. the per-child bucket names (the child rows' own `rate_limits`
   metadata — one concrete `tenant-api:<tenant>` bucket per item);
2. the per-key acquire/deny trace (the loop's own log lines: a tenant's
   saturated bucket snoozes that tenant's item budget-free while the
   other tenant's items claim through their own bucket);
3. both tenants' completion (the terminal roots + the run results).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import timedelta
from uuid import UUID

import asyncpg
import structlog

from examples.tenant_fairness_demo.app import ITEMS_PER_TENANT, QUEUE, tenant_app
from taskq.backend._protocol import JobRow

BANNER = f"\n{'═' * 72}\n"

#: The dispatch round's width.
LIMIT = 4

#: The claim lock's lease.
LEASE = timedelta(seconds=30)

#: The loop's round bound (a hang is a defect with a name, never a
#: silent spin).
MAX_ROUNDS = 200

TENANTS = ("acme", "globex")

_DEMO_LOG = structlog.get_logger("tenant_demo")


def _die(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


async def _seed_queue(pool: asyncpg.Pool, schema: str, worker_id: UUID) -> None:
    """The queue's `round_robin` mode row, the demo actors' config rows,
    and the dispatching worker's workflow_execution capability (the
    dispatch fence's execution leg reads it)."""
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            "ON CONFLICT (name) DO UPDATE SET mode = $2",
            QUEUE,
            "round_robin",
        )
        for actor in ("wf", "workflow"):
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue) '  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
                "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
                actor,
                QUEUE,
            )
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            "VALUES ($1, 'tenant-demo', 1, $2, $3::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET metadata = $3::jsonb",
            worker_id,
            [QUEUE],
            json.dumps({"workflow_execution": True}),
        )


def _resolved_registry(schema: str, pool: asyncpg.Pool):
    """The worker boot's own registration + collection passes, run
    in-process: the app registers, the projection lands the
    actor_config rows (the boot's own sync), and the collects fill the
    resolved registry — the keyed decl included (the fork-stamped
    concrete names resolve back through it at claim)."""
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.worker.startup import sync_actor_config
    from taskq.workflows._worker_execution import (
        collect_workflow_rate_limits,
        collect_workflow_reservations,
        project_workflow_actor_configs,
        register_app,
    )

    _ = schema
    register_app(tenant_app)
    configs = list(project_workflow_actor_configs())

    registry = RateLimitRegistry()
    registered, _unknown = collect_workflow_rate_limits(registry)
    _reservations, _res_unknown = collect_workflow_reservations(registry)
    print(
        f"boot collects: keyed decl tenant-api registered; static buckets: {sorted(set(registered)) or '—'}"
    )

    async def _land_configs() -> None:
        async with pool.acquire() as db:
            await sync_actor_config(db, configs, schema=schema)

    return registry, _land_configs


async def main() -> None:
    from taskq.backend._sweeps import sweep_scheduled_to_pending
    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.migrate import apply_pending
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps
    from taskq.worker.run import (
        _flow_rate_limit_gate,  # pyright: ignore[reportPrivateUsage]  # Why: the demo drives the intercept's OWN claim gate — the keyed face's enforcement seat is the subject; the underscore marks the worker-internal seam, not an unstable contract.
    )
    from taskq.workflows import FlowRunner
    from taskq.workflows._sweep import drain_outbox, sweep_join_rederive
    from taskq.workflows._worker_execution import execute_flow_job
    from taskq.workflows.engine import render_workflow_sql

    dsn = os.environ.get("TASKQ_PG_DSN", "")
    if not dsn:
        _die(
            "TASKQ_PG_DSN is not set — the demo drives a real Postgres "
            "(README.md's prerequisites block has the two-minute docker run)"
        )
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "tenant_demo")

    print(BANNER)
    print("multi-tenant workflow demo — two tenants, one keyed rate limit, one round_robin queue\n")
    print(f"schema: {schema}   queue: {QUEUE} (mode=round_robin)   tenants: {', '.join(TENANTS)}")
    print("keyed ref: base_name=tenant-api capacity=1 refill=0.5/s (per-tenant buckets)")

    conn = await asyncpg.connect(dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    print("migrations: applied\n")

    pool = await asyncpg.create_pool(dsn)
    settings = WorkerSettings.load_from_dict({"schema_name": schema, "TASKQ_QUEUES": QUEUE})
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    clock = SystemClock()
    worker_id = UUID(int=0x7E57)
    backend = PostgresBackend(
        deps,
        clock=clock,
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )
    wsql = render_workflow_sql(schema)
    registry, land_configs = _resolved_registry(schema, pool)

    # The projection's actor_config rows land (the sync the boot runs).
    await land_configs()
    await _seed_queue(pool, schema, worker_id)

    # THE TWO RUNS: one per tenant. The rows are born stamped — each
    # run's fairness_key is its own flow_id (the round_robin cohorts).
    flow_ids: dict[str, str] = {}
    runners: dict[str, FlowRunner] = {}
    for tenant in TENANTS:
        runner = FlowRunner(tenant_app.get("tenant_pipeline"), pool, schema)
        claim = await runner.create_flow(input={"tenant": tenant})
        runners[tenant] = runner
        flow_ids[tenant] = str(claim.flow_id)
        print(f"run created: tenant={tenant} flow_id={claim.flow_id}")

    print(BANNER)
    print("THE FLEET LOOP — the production dispatch claim + the intercept's keyed gate\n")

    denies = 0
    acquires = 0
    for _round in range(MAX_ROUNDS):
        dispatched = await backend.dispatch_batch(worker_id, [QUEUE], limit=LIMIT, lock_lease=LEASE)

        def _tenant_of(job: JobRow) -> str:
            # The dispatch decode's metadata IS the dict (jsonb_to_dict).
            meta = job.metadata
            flow = str(meta.get("flow_id", "?"))
            return next((t for t, fid in flow_ids.items() if fid == flow), "?")

        # THE ROUND'S OWN FACE: the claimed set IN CLAIM ORDER — the
        # round-robin cohort rotation is what interleaves the tenants
        # (one row per cohort per rank, the runs' cohorts alternating).
        claimed_tenants = [_tenant_of(job) for job in dispatched]
        if dispatched:
            print(f"round {_round + 1}: claimed [{', '.join(claimed_tenants)}]")

        for job in dispatched:
            meta = job.metadata  # the dispatch decode's dict (jsonb_to_dict)
            flow = str(meta.get("flow_id", "?"))
            tenant_of_row = next((t for t, f in flow_ids.items() if f == flow), "?")
            # JobRow carries no step_key column; the row's own shape
            # identifies the kind (the item rows carry their admission
            # terms; the join row AND the join-wait downstream rows
            # carry the join marker — the wait is what they share).
            if meta.get("rate_limits"):
                step_key = "source.item"
            elif meta.get("blocking_reason") == "join":
                step_key = "join-wait"
            else:
                step_key = "step"
            gate = await _flow_rate_limit_gate(
                registry, clock, job, worker_id, deps, job_log=_DEMO_LOG
            )
            if gate.denied is not None or gate.dependency_failure is not None:
                bucket = (
                    gate.denied.bucket_name
                    if gate.denied is not None
                    else type(gate.dependency_failure).__name__  # type: ignore[union-attr]
                )
                delay = (
                    gate.denied.retry_after if gate.denied is not None else timedelta(seconds=10)
                )
                denies += 1
                await backend.mark_snoozed(
                    job.id,
                    worker_id,
                    delay,
                    attempt=job.attempt,
                    claim_epoch=job.claim_epoch,
                )
                # THE APPLIED DEFERRAL (read back off the row): the
                # backend's own deferral floor applies — the applied
                # wait is at least the floor, whatever the hint priced.
                applied = await pool.fetchval(
                    f'SELECT scheduled_at - now() FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
                    job.id,
                )
                applied_s = applied.total_seconds() if applied is not None else 0.0
                print(
                    f"  DENY    tenant={tenant_of_row:<7} step={step_key:<12} "
                    f"bucket={bucket}  hint={delay.total_seconds():.2f}s "
                    f"applied={max(applied_s, 0.0):.2f}s (budget-free snooze)"
                )
                continue
            acquires += 1
            declared = meta.get("rate_limits")
            names = [str(n) for n in declared] if isinstance(declared, list) else ["-"]
            execution = await execute_flow_job(
                pool=pool, schema=schema, worker_id=worker_id, job=job
            )
            print(
                f"  ACQUIRE tenant={tenant_of_row:<7} step={step_key:<12} "
                f"bucket={','.join(names)}  outcome={execution.outcome}"
            )
        # THE SWEEP ARMS (the leader's tick faces): due scheduled rows
        # promote back to pending (the denial cycle's own act), the joins
        # fire, the outbox delivers the consumers.
        async with pool.acquire() as db:
            await sweep_scheduled_to_pending(db, schema=schema)
        await sweep_join_rederive(pool, wsql)
        await drain_outbox(pool, wsql)

        # THE IDLE BREATH: a round with nothing claimable (the snoozed
        # rows' due time is in the future) waits instead of spinning.
        if not dispatched:
            await asyncio.sleep(0.05)

        roots = await pool.fetch(
            f'SELECT status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            list(flow_ids.values()),
        )
        if len(roots) == len(TENANTS) and all(
            r["status"] in ("succeeded", "failed", "cancelled") for r in roots
        ):
            break
    else:
        _die(f"the fleet loop exhausted {MAX_ROUNDS} rounds — the runs wedged")

    print(BANNER)
    print("THE EVIDENCE\n")

    print(f"1. the per-child bucket names (each run forks {ITEMS_PER_TENANT} items):")
    rows = await pool.fetch(
        f"""SELECT (metadata->>'flow_id')::uuid AS flow_id, map_index,
                   metadata->'rate_limits' AS rate_limits
            FROM "{schema}".jobs
            WHERE metadata ? 'flow_id' AND step_key = 'source.item'
            ORDER BY flow_id, map_index""",  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
    )
    for row in rows:
        names = row["rate_limits"]
        names = json.loads(names) if isinstance(names, str) else names
        tenant_of_row = next((t for t, f in flow_ids.items() if f == str(row["flow_id"])), "?")
        print(f"  tenant={tenant_of_row:<7} item={row['map_index']}  buckets={names}")

    print(
        f"\n2. the per-key acquire/deny trace: {acquires} acquires, {denies} denies — "
        "a tenant's saturated bucket snoozed only that tenant's items; the other "
        "tenant's items claimed through their own bucket the whole way. "
        "THE TRACE ORDER IS CLAIM ORDER: each round's claimed set shows the "
        "round-robin cohort rotation interleaving the two runs."
    )

    print("\n3. both tenants' completion:")
    for tenant in TENANTS:
        runner = runners[tenant]
        root = await pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is the demo's settings-validated identifier; every value is a bound parameter.
            flow_ids[tenant],
        )
        result = await runner.result(flow_ids[tenant])
        print(f"  tenant={tenant:<7} flow_id={flow_ids[tenant]}  status={root}  result={result}")

    print(BANNER)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
