"""Probe: pin 6's exact end state — what does G7's teardown see?"""

from __future__ import annotations

import json

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import reconstruct_workflow_status
from taskq.workflows._sweep import sweep_join_rederive
from tests._wf_fixtures import (
    fire_count,
    node_state,
    seed_flow,
    seed_join,
    seed_running_node,
)


async def _seed_map(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    flow_id: JobId,
    *,
    n: int,
    policy: str,
    join_step: str = "join",
) -> tuple[JobId, list[JobId]]:
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key=join_step, deps=n)
    children = [
        await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"c{i}") for i in range(n)
    ]
    for child in children:
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
            "VALUES ($1, $2, $3, $4)",
            join_id,
            child,
            flow_id,
            policy,
        )
    return join_id, children


async def _terminalize(
    wf_conn: asyncpg.Connection, wf_schema: str, node_id: JobId, *, outcome: str = "succeeded"
) -> None:
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET status = $2, finished_at = now() '
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        node_id,
        outcome,
    )


@pytest.mark.integration
async def test_attack_probe_pin6_endstate(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    flow_id = await seed_flow(wf_conn, wf_schema)
    fc_join, fc_children = await _seed_map(
        wf_conn, wf_schema, flow_id, n=1, policy="fail_closed", join_step="fc_join"
    )
    col_join, col_children = await _seed_map(
        wf_conn, wf_schema, flow_id, n=1, policy="collect", join_step="col_join"
    )
    await _terminalize(wf_conn, wf_schema, fc_children[0], outcome="failed")
    await _terminalize(wf_conn, wf_schema, col_children[0], outcome="succeeded")

    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    root = await node_state(wf_conn, wf_schema, flow_id)
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    nodes = await wf_conn.fetch(wf_sql.workflow_nodes, flow_id)
    from tests._wf_fixtures import g7_check

    try:
        await g7_check(wf_conn, wf_schema, wf_sql)
        g7 = "g7 PASSED (silent)"
    except AssertionError as exc:
        g7 = f"g7 RED: {exc}"
    print(
        json.dumps(
            {
                "g7_probe": g7,
                "summary": str(summary),
                "root": root["status"],
                "reconstructed": reconstructed,
                "nodes": [
                    {
                        "step_key": r["step_key"],
                        "status": r["status"],
                        "deps": r["deps_pending"],
                        "blocking": r["blocking_reason"],
                        "absorbed": r["absorbed"],
                    }
                    for r in nodes
                ],
                "fc_fires": await fire_count(wf_conn, wf_schema, fc_join),
                "col_fires": await fire_count(wf_conn, wf_schema, col_join),
            },
            indent=2,
            default=str,
        )
    )
