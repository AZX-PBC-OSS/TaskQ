"""ATTACK: the run-key arbiter's scope is GLOBAL — two different flows, one
key, one silent cross-flow dedup.

``RUN_IDEMPOTENCY_SCOPE = "workflow-run"`` (``src/taskq/workflows/ledger.py:69``)
is a bare constant shared by EVERY workflow in the schema; the run key
alone decides the dedup. T05/G2's cron composition puts the flow name in
the KEY by convention (``key=<flow>:<slot-timestamp>``) — but nothing in
the arbiter enforces it. Two DIFFERENT flow definitions run with the same
naive key (a slot timestamp, ``"nightly"``, ``"daily"`` — the exact shapes
the founding incident's ``trigger_run.py`` used) collide SILENTLY: the
second flow's run returns the FIRST flow's run id + status and launches
nothing. The caller cannot distinguish "your run" from "someone else's
run with the same key" — the founding-incident shape rebuilt at arbiter
tier, one scope up.

RED: the second flow's ``insert_flow_run`` with the same key must either
create ITS OWN run or refuse loudly — it instead returns flow A's run row
(``created=False``, flow A's id).
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq.workflows._sql import WorkflowSql
from taskq.workflows.ledger import insert_flow_run
from tests._wf_fixtures import FlowStandIn


@pytest.mark.integration
async def test_attack_run_key_scope_collision_across_flows(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    flow_a = FlowStandIn(name="nightly-refresh")
    flow_a.actor = "refresh-a"
    flow_b = FlowStandIn(name="nightly-prune")
    flow_b.actor = "prune-b"

    key = "2026-10-07T03:00:00Z"  # the cron-slot key, WITHOUT a flow prefix

    run_a = await insert_flow_run(wf_conn, wf_sql, entry=flow_a, run_key=key)
    assert run_a.created

    # A DIFFERENT workflow, the SAME slot key: the shipped arbiter
    # namespaces by the key ALONE — flow B is silently deduped onto
    # flow A's run.
    run_b = await insert_flow_run(wf_conn, wf_sql, entry=flow_b, run_key=key)

    assert run_b.created, (
        f"flow B's run(key={key!r}) was silently deduped onto flow A's run "
        f"{run_b.flow_id} (status={run_b.status!r}): the 'workflow-run' scope "
        "is global across every workflow — cross-flow key collision, "
        "returned as a successful claim"
    )
