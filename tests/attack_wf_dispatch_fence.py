"""ATTACK: the dispatch fence (P3 rule 4's SECOND leg, in the claim) is
absent — a pending workflow child on a CANCELLED flow is claimed and runs.

P3 rule 4 (T04): "Cancel = one transaction — the flow flip is the
linearization point; every other statement re-checks flow status inside
its own statement. THREE legs: the fire guard …, the DISPATCH FENCE (in
the claim), the finalize fence — any two are insufficient; all three
ship." Pin 2's green requires "a refused post-cancel claim".

The shipped claim gained ``AND deps_pending = 0`` (12 sites in
``backend/_dispatch_sql.py``) but NO flow-status leg — grep the file: no
``flow_id``/``metadata``/``blocking_reason`` reference exists. So every
pending workflow child of a cancelled flow (the common population at
cancel time: the fork's children, the not-yet-fired consumers) is
claimable and EXECUTES on a dead flow — the cancel's linearization point
protects the join's fire and the finalize's decrement, but the leaves run
on.

RED: ``dispatch_batch`` over the real claim path must NOT return a
pending workflow child whose flow row is terminal. It returns it.
"""

from __future__ import annotations

import json
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_uuid
from tests._wf_fixtures import seed_flow


@pytest.mark.integration
async def test_attack_dispatch_claims_workflow_child_of_cancelled_flow(
    clean_jobs_app: object,
    module_pg_schema: object,
) -> None:
    app = clean_jobs_app  # JobsApp(deps, backend) — the REAL PostgresBackend
    backend = app.backend  # pyright: ignore[reportAttributeAccessIssue]
    schema = module_pg_schema.schema_name  # pyright: ignore[reportAttributeAccessIssue]
    dsn = module_pg_schema.pg_dsn  # pyright: ignore[reportAttributeAccessIssue]

    conn = await asyncpg.connect(dsn)
    try:
        # A cancelled flow + one of its pending children (a fork child
        # shape: deps_pending 0 — claimable by the counter's verdict).
        flow_id = await seed_flow(conn, schema, status="cancelled")
        child = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: the module fixture's throwaway schema identifier, _IDENT_RE-validated; all values $n-bound.
            "retry_kind, status, step_key, metadata, scheduled_at) "
            "VALUES ($1, 'actor_a', 'default', '{}', 3, 'transient', 'pending', 'c', "
            "$2::jsonb, now() - interval '1 hour')",
            child,
            json.dumps({"flow_id": str(flow_id)}),
        )
    finally:
        await conn.close()

    # The real claim path (the estate's dispatcher, deps_pending = 0 gate
    # included): the cancelled flow's child must be fenced OUT.
    dispatched = await backend.dispatch_batch(
        new_uuid(),
        ["default"],
        limit=5,
        lock_lease=timedelta(seconds=30),
    )
    claimed_ids = {str(row.id) for row in dispatched}

    assert str(child) not in claimed_ids, (
        f"the dispatch fence (P3 rule 4's second leg) is absent: the pending "
        f"workflow child {child} of a CANCELLED flow was claimed by the real "
        f"claim path (dispatched={sorted(claimed_ids)}) — it will EXECUTE on "
        "a dead flow. Pin 2's 'refused post-cancel claim' is unimplemented."
    )
