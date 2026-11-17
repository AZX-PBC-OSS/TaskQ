# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""F11: the maintenance fence's causal child is the CURRENT nonabsorbed
node — the same rows and shared absorption predicate that proved the
root's failure — never the latest failed LEDGER attempt.

The defect this control pins (DeepSeek round-3 F11): the migration
function derived the causal child from `wf_step_ledger.status='failed'`
by latest attempt — an old failed attempt whose job later SUCCEEDED, or
a crashed/abandoned terminal with NO failed ledger row, named a wrong or
NULL child on the refusal's durable record.

The three directions (the ruling's own control list):

(a) an old failed ledger attempt whose job later succeeded beside the
    actual current nonabsorbed failed cause — the cause's id is named;
(b) a crashed current cause with NO failed ledger row — a non-null
    actual id;
(c) an absorbed current failed node is never selected ahead of the
    nonabsorbed cause.

Real PostgreSQL; the REAL maintenance statement (the sweep's own root
maintain leg) drives the fence."""

from __future__ import annotations

import json as _json
from typing import Any

import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


async def _seed_root(conn: Any, schema: str) -> str:
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'f11', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        flow_id,
        _json.dumps({"flow_id": flow_id, "workflow": "f11_flow"}),
        flow_id,
        f"flow:{flow_id}",
    )
    return flow_id


async def _seed_node(
    conn: Any,
    schema: str,
    flow_id: str,
    step_key: str,
    *,
    status: str,
    finished: bool = True,
) -> str:
    node = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, error_class, finished_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'f11', 'default', '{}', $2, 1, 1, 'transient', $3, "
        "$4::jsonb, $5, CASE WHEN $6 THEN clock_timestamp() END, $7, 'workflow-run', $8)",
        node,
        status,
        step_key,
        _json.dumps({"flow_id": flow_id}),
        {"failed": "RuntimeError", "crashed": "WorkerCrashed", "succeeded": None}.get(status),
        finished,
        flow_id,
        f"wf:{flow_id}:{step_key}",
    )
    return node


async def _seed_failed_ledger(
    conn: Any, schema: str, flow_id: str, job_id: str, *, age: bool = False
) -> str:
    ledger_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, claim_epoch, status, error_class, updated_at) "
        "VALUES ($1, $2, $3, $4, NULL, 1, 1, 'failed', 'RuntimeError', "
        "clock_timestamp() - CASE WHEN $5 THEN interval '1 hour' ELSE interval '0 "
        "seconds' END)",
        ledger_id,
        flow_id,
        job_id,
        "retried_then_succeeded",
        age,
    )
    return ledger_id


async def _seed_absorbed_edge(conn: Any, schema: str, flow_id: str, parent: str, join: str) -> None:
    """An ABSORBING (collect) edge whose consumer is live: the parent's
    failure IS absorbed (the fan-in's own delivery)."""
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'collect')",
        join,
        parent,
        flow_id,
    )


async def _run_maintenance(conn: Any, pool: Any, wsql: WorkflowSql) -> dict[str, Any]:
    from taskq.workflows._sweep import sweep_join_rederive

    await sweep_join_rederive(pool, wsql, batch_size=10)
    _ = conn
    return {}


async def _refusal_record(conn: Any, schema: str, source_id: str) -> dict[str, Any] | None:
    """The fenced source's own refusal record (the fence writes it on the
    parked source's row)."""
    raw = await conn.fetchval(
        f"""SELECT metadata->'wf_not_draining' FROM "{schema}".jobs
            WHERE id = $1::uuid AND metadata ? 'wf_not_draining' LIMIT 1""",
        source_id,
    )
    if raw is None:
        return None
    doc = _json.loads(raw) if isinstance(raw, str) else dict(raw)
    return doc


async def _seed_parked_source(conn: Any, schema: str, flow_id: str) -> str:
    """The §5.4 fence's own subject: one exact parked source of the flow
    (the refusal record lands here). Cursor-bearing — the causal-child
    direction is orthogonal to the cursor gate, so the PRE-FIX fence runs
    and names its (wrong) child."""
    source = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, assignment_routed, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'f11', 'default', '{}', 'scheduled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, true, 'workflow-run', $4)",
        source,
        _json.dumps(
            {
                "flow_id": flow_id,
                "blocking_reason": "backpressure",
                "emit_cursor": {"page": 0},
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {
                    "version": 1,
                    "page_width": 2,
                    "bound": 4,
                    "outstanding": 1,
                    "deferrals": 1,
                    "wakes": 0,
                },
                "wf_source_logical_attempt": 1,
            }
        ),
        flow_id,
        f"wf:{flow_id}:source",
    )
    return source


async def test_causal_child_is_the_current_nonabsorbed_node_not_ledger_recency(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """Directions (a) + (c): an OLD failed ledger attempt (aged an hour)
    whose job later SUCCEEDED beside a CURRENT nonabsorbed failed cause
    and an ABSORBED current failed node — the fence names the current
    nonabsorbed cause's own id, never the ledger-recency row and never
    the absorbed node."""
    flow_id = await _seed_root(wf_conn, wf_schema)
    parked = await _seed_parked_source(wf_conn, wf_schema, flow_id)
    # (a)'s decoy: the old failed ledger attempt whose OWN job succeeded.
    retried = await _seed_node(
        wf_conn, wf_schema, flow_id, "retried_then_succeeded", status="succeeded"
    )
    await _seed_failed_ledger(wf_conn, wf_schema, flow_id, retried, age=True)
    # (c)'s absorbed node: a CURRENT failed node with a live collect
    # consumer — its failure is absorbed (never the causal child).
    absorbed = await _seed_node(wf_conn, wf_schema, flow_id, "absorbed", status="failed")
    join = str(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, "
        "scheduled_at, created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'f11', 'default', '{}', 'pending', 2, 'transient', 'join_step', "
        "1, $2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), "
        "$3::uuid, 'workflow-run', $4)",
        join,
        _json.dumps({"flow_id": flow_id, "blocking_reason": "join"}),
        flow_id,
        f"wf:{flow_id}:join",
    )
    await _seed_absorbed_edge(wf_conn, wf_schema, flow_id, absorbed, join)
    # THE ACTUAL CAUSE: the current nonabsorbed failed node.
    cause = await _seed_node(wf_conn, wf_schema, flow_id, "the_cause", status="failed")

    await _run_maintenance(wf_conn, wf_pool, wf_sql)

    # THE REFUSAL RECORD lands on the fenced parked source.
    refusal = await _refusal_record(wf_conn, wf_schema, parked)
    assert refusal is not None, (
        "the maintenance's failure arm must have fenced the parked source "
        "(the root terminal failed behind the exhausted child)"
    )

    # The refusal record's child: the CURRENT nonabsorbed cause.
    assert refusal.get("child_id") == cause, refusal
    assert refusal.get("child_id") != retried, refusal
    assert refusal.get("child_id") != absorbed, refusal
    _ = flow_id


async def test_crashed_cause_with_no_failed_ledger_row_names_the_actual_node(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """Direction (b): a CRASHED current cause with NO failed ledger row —
    the fence's causal record is a non-null ACTUAL node id (the
    ledger-recency derivation named NULL here)."""
    flow_id = await _seed_root(wf_conn, wf_schema)
    parked = await _seed_parked_source(wf_conn, wf_schema, flow_id)
    cause = await _seed_node(wf_conn, wf_schema, flow_id, "the_crash", status="crashed")
    await _run_maintenance(wf_conn, wf_pool, wf_sql)
    refusal = await _refusal_record(wf_conn, wf_schema, parked)
    assert refusal is not None
    assert refusal.get("child_id") == cause, refusal
