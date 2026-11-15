"""The contract §9 control 12: cancellation races, terminal/failed roots,
and sibling claim-exactness. The claim-exact sibling `ctx.substep`
protection: a cancelled or failed job must never fence or rewrite a
SIBLING's ledger row (the cancelled job's own current claim is resolved by
its own id + step_key + null-safe map_index + attempt + epoch — never a
job-wide or flow-wide match). Real PostgreSQL; the real cancel path."""

from __future__ import annotations

import json as _json
from typing import Any

import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


async def _seed_flow_root(conn: Any, schema: str) -> JobId:
    flow = JobId(new_uuid_str())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, metadata, fairness_key) "
        "VALUES ($1, 'c12', 'c12', '{}', 'running', 3, 'transient', '__flow__', "
        "$2::jsonb, $3)",
        flow,
        _json.dumps({"flow_id": str(flow), "workflow": "c12_flow"}),
        str(flow),
    )
    return flow


async def _seed_node(
    conn: Any, schema: str, flow: JobId, step_key: str, *, status: str = "running"
) -> JobId:
    job = JobId(new_uuid_str())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, metadata, fairness_key, attempt, "
        "locked_by_worker, lock_expires_at, claim_epoch) "
        "VALUES ($1, 'c12', 'c12', '{}', $2, 3, 'transient', $3, "
        "$4::jsonb, $5, 1, $1, now() + interval '90 seconds', 1)",
        job,
        status,
        step_key,
        _json.dumps({"flow_id": str(flow)}),
        str(flow),
    )
    return job


async def test_cancel_before_park_and_after_park_never_reenters_and_spares_siblings(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """Cancel immediately before and after the park commit: the node
    terminalizes, its exact ledger claim fences, no later event or due time
    re-enters it, and the SIBLING ctx.substep ledger rows are never
    cross-written. A failed/cancelled root never wakes its parked
    work."""
    from taskq.workflows.api._sql_runner import (
        CANCEL_NODES_SQL_TEMPLATE,
        NODE_CLAIM_SQL_TEMPLATE,
        render_sql,
    )
    from taskq.workflows.engine import finalize_node
    from taskq.workflows.ledger import claim_step_ledger

    # The flow: the parked source + a claimed sibling with TWO ledger rows
    # sharing the job (the ctx.substep shape: same job id, different step
    # keys) + the sibling's own row.
    flow = await _seed_flow_root(wf_conn, wf_schema)
    source = await _seed_node(wf_conn, wf_schema, flow, "source", status="scheduled")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb, '
        "attempt = 0, locked_by_worker = NULL, lock_expires_at = NULL, "
        "last_heartbeat_at = NULL WHERE id = $1",
        source,
        _json.dumps(
            {
                "blocking_reason": "backpressure",
                "emit_cursor": {"page": 0},
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {"version": 1, "page_width": 2, "bound": 2},
                "wf_source_logical_attempt": 1,
            }
        ),
    )
    sibling = await _seed_node(wf_conn, wf_schema, flow, "sibling")
    sub_job = sibling  # the ctx.substep shape: one job, two step keys

    async with wf_pool.acquire() as conn:
        sibling_claim = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=sub_job,
            step_key="sibling",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
        sub_claim = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=sub_job,
            step_key="sibling.sub",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
    assert sibling_claim.ledger_id is not None and sub_claim.ledger_id is not None

    # CANCEL BEFORE the park's own resolution: the operator's flip marks
    # the cooperative phase (the root's own terminal follows its
    # cooperative unwind) and TERMINALIZES the parked row.
    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)
    root = await wf_conn.fetchrow(
        f"SELECT status::text AS status, metadata->>'cancel_phase' AS phase "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        flow,
    )
    assert root is not None and root["status"] in ("running", "cancelled"), root
    assert root["phase"] == "cooperative", root

    # The sibling's OWN ledger rows: the sibling's claim stays 'running'
    # (its job's cancel is the row's own resolution — the substep's row is
    # never rewritten by the sibling's terminal, and neither is fenced by
    # the OTHER's identity).
    rows = await wf_conn.fetch(
        f'SELECT step_key, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 ORDER BY step_key",
        flow,
    )
    by_key = {r["step_key"]: r["status"] for r in rows}
    assert by_key["sibling"] == "running", by_key
    assert by_key["sibling.sub"] == "running", by_key

    # The parked source: the cancel terminalized it (the exact current
    # claim fenced with the cancel's vocabulary) — and it never re-enters:
    # a later claim attempt and the promotion both refuse.
    source_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt::int AS attempt, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source,
    )
    assert source_row is not None
    assert source_row["status"] == "cancelled", source_row
    claim = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), source, JobId(new_uuid_str())
    )
    assert claim is None, "a cancelled parked row is unclaimable"
    # The failed/cancelled root never wakes its parked work: the drain
    # sees real rows — the source's own row is terminal cancelled (no park
    # to promote), so the intent completes without a promotion.
    from taskq.workflows._emit import ensure_emit_capacity, wake_emit_backpressure

    async with wf_pool.acquire() as conn:
        await ensure_emit_capacity(conn, wf_sql, flow_id=flow, bound=2)
    result = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow)
    assert result.promoted == 0, result

    # The substep's zero-cross-write: a terminal on the SIBLING's claim
    # never touches the substep's row (the exact-claim write).
    await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow,
        job_id=sub_job,
        step_key="sibling",
        worker_id=sub_job,
        attempt=1,
        claim_epoch=1,
        outcome="failed",
        error_class="RuntimeError",
        ledger_id=sibling_claim.ledger_id,
        ledger_claimed_at=sibling_claim.claimed_at,
    )
    rows = await wf_conn.fetch(
        f'SELECT step_key, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 ORDER BY step_key",
        flow,
    )
    by_key = {r["step_key"]: r["status"] for r in rows}
    assert by_key["sibling"] == "failed", by_key
    assert by_key["sibling.sub"] == "running", (
        f"the sibling's terminal never rewrites the substep's own claim row: {by_key}"
    )


async def test_park_and_cancel_race_resolves_by_row_state(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """Cancel racing the park: whichever wins, the row state is the truth —
    the park's fenced arm answers cancel_fenced and the runner terminalizes;
    the cancel's flip on a parked row fences its exact awaited claim and
    the row disappears from every derived predicate."""
    from datetime import timedelta

    from taskq.workflows._emit import EmitBackpressure, observe_emit_capacity
    from taskq.workflows.api._runner_defer import defer_row_law
    from taskq.workflows.api._sql_runner import render_sql
    from taskq.workflows.ledger import claim_step_ledger

    flow = await _seed_flow_root(wf_conn, wf_schema)
    source = await _seed_node(wf_conn, wf_schema, flow, "source")
    async with wf_pool.acquire() as conn:
        claim = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=source,
            step_key="source",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
    assert claim.ledger_id is not None

    outcome = await defer_row_law(
        wf_pool,
        wf_sql,
        job_id=source,
        worker_id=source,
        delay=timedelta(0),
        attempt=1,
        claim_epoch=1,
        disposition="emit_backpressure",
        signal_name="EmitBackpressure",
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.claimed_at,
        backpressure=EmitBackpressure(page_width=2, bound=2, outstanding=1),
        source_logical_attempt=1,
        flow_id=flow,
    )
    assert outcome.verdict == "deferred", outcome

    observation = await observe_emit_capacity(
        wf_pool.acquire() if False else _Conn(wf_pool), wf_sql, flow_id=flow
    )
    assert observation.min_waiting_width == 2, observation

    # The cancel lands on the parked row: the exact awaited claim fences.
    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE

    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow)
    source_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt::int AS attempt FROM "{wf_schema}".jobs WHERE id = $1',
        source,
    )
    assert source_row is not None and source_row["status"] == "cancelled", source_row
    assert int(source_row["attempt"]) == 0, "the parked refund stands through the cancel"
    ledger_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert ledger_status == "fenced", ledger_status
    observation = await observe_emit_capacity(_Conn(wf_pool), wf_sql, flow_id=flow)
    assert observation.live_rows == 0 and observation.reservations == 0, observation
    assert not observation.waiter_present


class _Conn:
    """The observation's minimal connection view over the pool."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)

    async def fetchrow(self, *args: Any) -> Any:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(*args)
