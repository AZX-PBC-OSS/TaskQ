"""The map-on-a-join shape through the fleet's own claim seat: does the
run converge when the claim comes from ``backend.dispatch_batch`` and the
attempt from ``execute_flow_job``, instead of from ``FlowRunner.tick()``?

The pins in ``tests/test_wf_map_on_join_pins.py`` control the drain's
placement inside one event loop. The two legs they never touch are the two
the fleet owns: the CLAIM (a workflow row must pass the dispatch fence, and
the outer join this fix leaves with ``consumers = []`` must still be
claimable by a real dispatch round) and the ATTEMPT (a fork-spawned join
row's body resolves through the fleet's execution door, not the driver's
loop). Both doors share ``_finalize_success`` — the fork/fire/drain chain
is one code path — so what this file adds is the claim seat and the
leader's belt around it, and the assertion that each join row is written
by its own source's fork (the parent-scoped key), never by the drain.

Everything else is the production composition: the cohort's
``actor_config`` rows, ``register_worker(workflow_execution=True)``, the
leader belt's two arms (``sweep_join_rederive`` + ``drain_outbox``) — the
rig the fleet-face pins in ``test_wf_start_to_close_pins.py`` and
``test_wf_policy_fleet.py`` build. The actor rows are seeded per round from
the rows about to be dispatched rather than from the boot projection's
compilations (a harness affordance: the claim bound is the round's
population, and the placement columns are the compile's own).

A zero-consumer join row is claimable because neither claim fence reads
``blocking_reason`` (``CLAIMABLE_NODES_SQL_TEMPLATE`` and the dispatch
fence do not name it): the join-wait marker is diagnostic, not a fence.
This file proves that from the production seat rather than by reading the
fences.

``clean_jobs_app`` truncates the module schema per test, so this file stays
its own module: it must not be merged beside a module-scoped seed.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from taskq.worker.run import register_worker
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from taskq.workflows._sweep import drain_outbox, sweep_join_rederive
from taskq.workflows._worker_execution import execute_flow_job

if TYPE_CHECKING:
    import asyncpg

    from taskq.testing.fixtures import JobsApp, ModulePgSchema
    from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration

WORKFLOW = "fleet_door_map_on_join"


async def seeds(ctx: StepContext) -> list[dict[str, int]]:
    return [{"seed": s} for s in range(10)]


async def expand(ctx: StepContext, seed: dict[str, int]) -> list[str]:
    return [f"s{seed['seed']}-{k}" for k in range(5)]


async def ident(ctx: StepContext, x: object) -> object:
    return x


async def count(ctx: StepContext, xs: list[object]) -> int:
    return len(xs)


def _build(app: WorkflowApp) -> None:
    @app.workflow(WORKFLOW)
    def fleet_door_map_on_join() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        return build(step(count, map_source(first, ident), key="final"))


async def _seed_actor_configs(pool: asyncpg.Pool, schema: str, flow_id: object) -> None:
    # the boot projection writes these from the compile; a claim round needs
    # the row for every actor it dispatches, and the fork-spawned rows share
    # their source node's cohort.
    await pool.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '  # noqa: S608  # Why: fixture-derived schema identifier; every value is $-bound.
        f'SELECT DISTINCT actor, queue FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'pending' "
        "AND deps_pending = 0 AND assignment_routed = false "
        "ON CONFLICT (actor) DO NOTHING",
        flow_id,
    )


async def _due_queues(pool: asyncpg.Pool, schema: str, flow_id: object) -> list[str]:
    rows = await pool.fetch(
        f'SELECT DISTINCT queue FROM "{schema}".jobs '  # noqa: S608  # Why: fixture-derived schema identifier; every value is $-bound.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'pending' "
        "AND deps_pending = 0 AND assignment_routed = false",
        flow_id,
    )
    return sorted({str(r["queue"]) for r in rows}) or ["default"]


async def test_the_nested_join_seam_converges_through_the_fleet_claim_seat(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
    wf_sql: WorkflowSql,
) -> None:
    schema = module_pg_schema.schema_name
    deps = clean_jobs_app.deps
    pool = deps.dispatcher_pool

    app = WorkflowApp()
    _build(app)
    runner = FlowRunner(app.get(WORKFLOW), pool, schema)
    flow_id = (await runner.create_flow()).flow_id

    # the boot's two production registrations: the workers row (capability
    # is DATA — the dispatch fence reads it) and the cohort's actor rows.
    worker_id = await register_worker(pool, deps.settings, workflow_execution=True)

    rounds = 0
    claims = 0
    for _ in range(80):
        rounds += 1
        await _seed_actor_configs(pool, schema, flow_id)
        queues = await _due_queues(pool, schema, flow_id)
        claimed = await clean_jobs_app.backend.dispatch_batch(
            worker_id, queues, 10, timedelta(seconds=30)
        )
        claims += len(claimed)
        for job in claimed:
            await execute_flow_job(pool=pool, schema=schema, worker_id=worker_id, job=job)
        await sweep_join_rederive(pool, wf_sql)
        await drain_outbox(pool, wf_sql)
        status = await pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: fixture-derived schema identifier.
            flow_id,
        )
        if status in ("succeeded", "failed", "cancelled"):
            break

    rows = await pool.fetch(
        f'SELECT step_key, status, idempotency_key, metadata FROM "{schema}".jobs '  # noqa: S608  # Why: fixture-derived schema identifier.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND id <> $1 ORDER BY id",
        flow_id,
    )
    snapshot = [
        {
            "step_key": r["step_key"],
            "status": r["status"],
            "idempotency_key": r["idempotency_key"],
        }
        for r in rows
    ]
    root_status = await pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: fixture-derived schema identifier.
        flow_id,
    )
    result = await runner.result(flow_id)

    assert root_status == "succeeded", (
        f"the fleet door did not terminalize the run after {rounds} rounds "
        f"(root={root_status}); rows={snapshot}"
    )
    for key in ("seeds.join", "seeds.join.join"):
        found = [r for r in snapshot if r["step_key"] == key]
        assert len(found) == 1, f"the join {key!r} has one row; got {found}"
    assert [r for r in snapshot if r["status"] == "pending"] == [], snapshot
    assert claims > 0, "the dispatch round never claimed a row — the door was never exercised"
    assert result == 10, result
    # the nested join's writer was the FORK, not the drain: the fork's key is
    # parent-scoped, the drain's is not (the two shapes the defect hinges on).
    nested_keys = {r["step_key"]: r["idempotency_key"] for r in snapshot}
    assert nested_keys["seeds.join"] == f"wf:{flow_id}:seeds:seeds.join", nested_keys
    assert nested_keys["seeds.join.join"] == f"wf:{flow_id}:seeds.join:seeds.join.join", nested_keys
