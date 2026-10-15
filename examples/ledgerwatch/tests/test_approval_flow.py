"""THE APPROVAL FLOW: a HIGH-risk transaction's approval asked ->
resolved -> the flow continues (the typed verdict); the rejected face;
and the Expired arm — no approval lands in the window — escalates to
the compliance queue (ONE durable row)."""

from __future__ import annotations

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import ApprovedTxn, RejectedTxn
from examples.ledgerwatch.tests.support import Fleet, review_run_ids
from taskq.workflows import HitlClient

pytestmark = pytest.mark.timeout(300)

FLEET_ENV: dict[str, str] = {}  # the clean profile; the approval window is long (300s)


async def test_approval_asked_resolved_flow_continues(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-approve", count=1, high_every=1))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"

    # the batch's pipeline completes; the review run HOLDS
    await await_terminal(pool, schema, str(claim.flow_id), timeout_s=120)
    ids = await _wait_review_runs(pool, schema, batch.batch_id)
    client = HitlClient(pool, schema=schema)
    (hold,) = await client.list(ids[0])
    assert hold.signal_name == "ApprovalDecision"

    # THE HUMAN APPROVES — the typed door delivers; the flow continues
    # to its typed verdict.
    result = await client.resolve(hold.hold_id, {"approved": True, "reviewer": "op-7"})
    assert result.status == "delivered"
    raw = await await_terminal(pool, schema, ids[0], workflow="txn_review", timeout_s=60)
    verdict = ApprovedTxn.model_validate(raw)
    assert verdict.txn_id == "B-approve-T00000"
    assert verdict.reviewer == "op-7"
    # the compliance queue did NOT eat it (scoped to this batch — the
    # estate may carry other batches' escalations)
    n = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".lw_compliance_queue WHERE batch_id = $1',  # noqa: S608  # Why: the schema identifier only.
        "B-approve",
    )
    assert n == 0


async def test_approval_rejected(fleet: Fleet, pool: asyncpg.Pool, schema: str) -> None:
    batch = build_batch(BatchSpec(batch_id="B-reject", count=1, high_every=1))
    claim = await submit_batch(pool, schema, batch)
    await await_terminal(pool, schema, str(claim.flow_id), timeout_s=120)
    (rid,) = await _wait_review_runs(pool, schema, batch.batch_id)
    client = HitlClient(pool, schema=schema)
    (hold,) = await client.list(rid)
    result = await client.resolve(hold.hold_id, {"approved": False, "note": "no"})
    assert result.status == "delivered"
    raw = await await_terminal(pool, schema, rid, workflow="txn_review", timeout_s=60)
    verdict = RejectedTxn.model_validate(raw)
    assert verdict.note == "no"


async def _wait_review_runs(pool: asyncpg.Pool, schema: str, batch_id: str) -> list[str]:
    """THIS batch's review runs (the run's own input carries the batch
    id — never another test's runs)."""
    from examples.ledgerwatch.tests.support import wait_for

    async def _ready() -> list[str] | None:
        ids = await review_run_ids(pool, schema, batch_id=batch_id)
        return ids if ids else None

    return await wait_for(_ready, timeout_s=60, message="the review runs never appeared")
