# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""Round-4 NEW-8(b): the A9.2 SKIP-LOCKED exemption's BEHAVIOURAL
controls — the two exempt maintenance seats must DEFER (skip) a contended
row, never wait for it.

A9.2's named exemptions (the ruling's N-3, accepted with the lock
evidence): the root-maintenance seat selects its roots ``FOR UPDATE SKIP
LOCKED`` and the rederive/fire seat acquires its join rows the same way
— neither queues behind a root/node a drain, cascade, or park
transaction already holds, which is the reason they need no root-first
re-serialization. A comment is not a control: each seat is driven here
against a row HELD by another open transaction, and the observation is
deterministic in both directions —

* while the holder holds: the seat's real statement completes inside a
  bounded deadline (a waiting seat cannot — the bound is the
  non-waiting proof's own form) and the held row is NOT touched (the
  deferral);
* after the holder releases: the same seat performs the deferred work
  (the deferral is a later bounded pass, never a loss).

The FOR UPDATE (wait) mutant of either statement reds its control at the
bounded deadline."""

from __future__ import annotations

import asyncio
import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration

#: The non-waiting bound: a seat that queues behind the holder cannot
#: complete inside it (the holder's transaction stays open until the
#: control commits it). Not a timeout inflation — it is the deadline the
#: non-waiting proof is made of.
_NON_WAITING_BOUND_S = 5.0


async def _seed_maintained_flow(conn: Any, schema: str) -> tuple[str, str]:
    """A running root whose every node is terminal (one succeeded node):
    the maintenance seat's own derivation finalizes the root
    'succeeded' and records the state_change event — the deferred work
    the second phase observes landing."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'skip', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $1::uuid::text, 'workflow-run', $3)",
        flow_id,
        _json.dumps({"flow_id": flow_id, "workflow": "skip_flow"}),
        f"flow:{flow_id}",
    )
    node_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, finished_at, metadata, fairness_key, "
        "idempotency_scope, idempotency_key) VALUES ($1, 'skip', 'default', '{}', "
        "'succeeded', 1, 1, 'transient', 'screen', clock_timestamp(), $2::jsonb, "
        "$3::text, 'workflow-run', $4)",
        node_id,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:screen",
    )
    return flow_id, node_id


async def _seed_stale_cache_join(conn: Any, schema: str) -> str:
    """A due join-wait row whose deps_pending cache is STALE-HIGH (2) but
    whose parents resolve to one un-terminal (a succeeded parent, a
    pending one): the rederive seat's reconcile arm corrects the cache
    to 1 (never firable — no reducer resolution involved). The deferred
    work the second phase observes landing."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'skip', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $1::uuid::text, 'workflow-run', $3)",
        flow_id,
        _json.dumps({"flow_id": flow_id, "workflow": "skip_flow"}),
        f"flow:{flow_id}",
    )
    done_parent = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, finished_at, metadata, fairness_key, "
        "idempotency_scope, idempotency_key) VALUES ($1, 'skip', 'default', '{}', "
        "'succeeded', 1, 1, 'transient', 'left', clock_timestamp(), $2::jsonb, "
        "$3::text, 'workflow-run', $4)",
        done_parent,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:left",
    )
    live_parent = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, fairness_key, "
        "idempotency_scope, idempotency_key) VALUES ($1, 'skip', 'default', '{}', "
        "'pending', 0, 1, 'transient', 'right', $2::jsonb, $3::text, "
        "'workflow-run', $4)",
        live_parent,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:right",
    )
    join_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key) VALUES "
        "($1, 'skip', 'default', '{}', 'pending', 0, 1, 'transient', 'gather_join', "
        "2, $2::jsonb, clock_timestamp(), clock_timestamp(), $3::text, "
        "'workflow-run', $4)",
        join_id,
        _json.dumps({"flow_id": flow_id, "blocking_reason": "join"}),
        flow_id,
        f"wf:{flow_id}:gather:{join_id}",
    )
    for parent in (done_parent, live_parent):
        await conn.execute(
            f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, '
            "failure_policy) VALUES ($1, $2, $3, 'fail_closed')",
            join_id,
            parent,
            flow_id,
        )
    return join_id


async def _row(conn: Any, schema: str, job_id: str) -> dict[str, Any]:
    raw = await conn.fetchrow(f'SELECT * FROM "{schema}".jobs WHERE id = $1', job_id)
    assert raw is not None
    return dict(raw)


async def test_root_maintenance_defers_a_held_root_and_never_waits(
    wf_conn: Any, wf_schema: str, wf_pool: Any, module_pg_schema: Any
) -> None:
    """The maintenance seat's exemption: with the flow's root row HELD by
    another open transaction, the real `sweep_join_rederive` (whose
    nested maintenance leg selects its roots `FOR UPDATE SKIP LOCKED`)
    completes inside the non-waiting bound and the held root is NOT
    maintained (no derivation, no terminal, no maintenance event); once
    the holder releases, the SAME seat performs the deferred maintenance
    (the root finalizes from its rows with its own event). The FOR
    UPDATE (wait) mutant cannot complete phase 1 inside the bound — the
    control reds there."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, _node_id = await _seed_maintained_flow(wf_conn, wf_schema)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    holder_tx = holder.transaction()
    await holder_tx.start()
    try:
        await holder.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1 FOR UPDATE', flow_id
        )
        # PHASE 1 — the held root is DEFERRED, never waited on.
        result = await asyncio.wait_for(
            sweep_join_rederive(wf_pool, wsql, batch_size=10), timeout=_NON_WAITING_BOUND_S
        )
        assert result is not None
        root_row = await _row(wf_conn, wf_schema, flow_id)
        assert root_row["status"] == "running", (
            f"the held root is deferred to a later pass — never maintained "
            f"under the holder: {root_row['status']}"
        )
        maintained_events = await wf_conn.fetchval(
            f"""SELECT count(*) FROM "{wf_schema}".job_events
                WHERE job_id = $1 AND detail->>'reason' = 'workflow-maintained'""",
            flow_id,
        )
        assert int(maintained_events) == 0, (
            f"no maintenance event for the held root: {maintained_events}"
        )
        await holder_tx.commit()
    finally:
        await holder.close()

    # PHASE 2 — the deferral lands: the released root is maintained.
    result = await asyncio.wait_for(
        sweep_join_rederive(wf_pool, wsql, batch_size=10), timeout=_NON_WAITING_BOUND_S
    )
    assert result is not None
    root_row = await _row(wf_conn, wf_schema, flow_id)
    assert root_row["status"] == "succeeded", root_row["status"]
    maintained_events = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".job_events
            WHERE job_id = $1 AND detail->>'reason' = 'workflow-maintained'""",
        flow_id,
    )
    assert int(maintained_events) == 1, maintained_events


async def test_rederive_seat_defers_a_held_join_row_and_never_waits(
    wf_conn: Any, wf_schema: str, wf_pool: Any, module_pg_schema: Any
) -> None:
    """The rederive/fire seat's exemption: with the join-wait row HELD by
    another open transaction, the real `sweep_join_rederive` acquires its
    join rows `FOR UPDATE SKIP LOCKED` — it completes inside the
    non-waiting bound WITHOUT reconciling the held row (the cache stays
    stale, the summary counts zero reconciled); once the holder
    releases, the SAME seat reconciles the row (the stale-high cache
    corrected to the edge ledger's truth). The FOR UPDATE (wait) mutant
    cannot complete phase 1 inside the bound — the control reds
    there."""
    wsql = render_workflow_sql(wf_schema)
    join_id = await _seed_stale_cache_join(wf_conn, wf_schema)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    holder_tx = holder.transaction()
    await holder_tx.start()
    try:
        await holder.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1 FOR UPDATE', join_id
        )
        # PHASE 1 — the held join row is DEFERRED, never waited on.
        summary = await asyncio.wait_for(
            sweep_join_rederive(wf_pool, wsql, batch_size=10), timeout=_NON_WAITING_BOUND_S
        )
        assert summary is not None and summary.reconciled == 0, summary
        join_row = await _row(wf_conn, wf_schema, join_id)
        assert int(join_row["deps_pending"]) == 2, (
            f"the held row's cache is deferred untouched: {join_row['deps_pending']}"
        )
        await holder_tx.commit()
    finally:
        await holder.close()

    # PHASE 2 — the deferral lands: the released row reconciles.
    summary = await asyncio.wait_for(
        sweep_join_rederive(wf_pool, wsql, batch_size=10), timeout=_NON_WAITING_BOUND_S
    )
    assert summary is not None and summary.reconciled == 1, summary
    join_row = await _row(wf_conn, wf_schema, join_id)
    assert int(join_row["deps_pending"]) == 1, join_row["deps_pending"]
    _ = JobId, wf_conn
