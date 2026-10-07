"""ATTACK: the step ledger's PK omits map_index — map children collide.

The ledger's uniqueness is ``UNIQUE (flow_id, step_key, attempt)``
(``01.00.23_02_pre_workflow_tables.sql``), and the claim's conflict target
(``LEDGER_CLAIM_SQL`` in ``src/taskq/workflows/_sql_ledger.py``) is
``(flow_id, step_key, attempt)`` — ``map_index`` is carried as a bound
value but is NOT part of the arbiter. T05's contract says the map child's
step key is ``(workflow, map node, map_index)``: two map children of one
map node (map_index 0 and 1, first attempt each) are DIFFERENT claims by
that contract, but the ledger PK collapses them into ONE row.

The dragon: child 1's claim returns child 0's row (the ON CONFLICT path),
child 1's finalize (``LEDGER_TERMINAL_SQL``, keyed flow+step+attempt)
OVERWRITES child 0's result on the shared row, and the memoized replay
(``LEDGER_MEMOIZED_SQL``, filtered by map_index) then hands child 0's
replay the WRONG child's result — or nothing. The record lies about the
work: the exact class this layer exists to prevent (T05 REVIEW's
"the record must not lie").

RED: the second map child's claim must return ITS OWN row
(``claim.job_id == node_1``); it returns child 0's row instead.
"""

from __future__ import annotations

import json

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.ledger import claim_step_ledger, memoized_step_result


@pytest.mark.integration
async def test_attack_map_children_ledger_pk_collision(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    flow_id = new_uuid()
    node_0 = new_uuid()  # map child 0's job row
    node_1 = new_uuid()  # map child 1's job row

    claim_0 = await claim_step_ledger(
        wf_conn,
        wf_sql,
        flow_id=flow_id,
        job_id=node_0,
        step_key="enrich",
        map_index=0,
        attempt=1,
    )
    assert claim_0.job_id == node_0  # child 0's own claim — fine.

    # Child 1 claims the SAME step name under ITS OWN map_index — a
    # different business claim per T05's key contract.
    claim_1 = await claim_step_ledger(
        wf_conn,
        wf_sql,
        flow_id=flow_id,
        job_id=node_1,
        step_key="enrich",
        map_index=1,
        attempt=1,
    )

    # THE ATTACK: the arbiter must treat child 1's claim as its own row.
    # The shipped PK collapses it onto child 0's row.
    assert claim_1.job_id == node_1, (
        f"map child 1's ledger claim returned child 0's row "
        f"(job_id={claim_1.job_id}): the UNIQUE (flow_id, step_key, attempt) "
        "omits map_index — map children share ONE ledger row"
    )

    # The overwrite arm: both children finalize; the ledger must hold BOTH
    # results, each replayable under its own map_index.
    for node, result in ((node_0, {"child": 0}), (node_1, {"child": 1})):
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".wf_step_ledger SET status = 'succeeded', result = $4::jsonb "  # noqa: S608  # Why: the module fixture's throwaway schema identifier, _IDENT_RE-validated; all values $n-bound.
            "WHERE flow_id = $1 AND step_key = $2 AND attempt = $3 AND job_id = $5",
            flow_id,
            "enrich",
            1,
            json.dumps(result),
            node,
        )
    replay_0 = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="enrich", map_index=0
    )
    replay_1 = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="enrich", map_index=1
    )
    assert replay_0 is not None and replay_0.result == {"child": 0}, (
        f"child 0's replay returns {replay_0.result if replay_0 else None}: "
        "the last finalize's overwrite replaced it"
    )
    assert replay_1 is not None and replay_1.result == {"child": 1}, (
        f"child 1's replay returns {replay_1.result if replay_1 else None}: "
        "the ledger row's map_index (0) never matches child 1's replay read"
    )
