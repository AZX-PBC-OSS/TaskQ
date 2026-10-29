"""THE EXPIRY ARM: no approval lands in the window — the wait site
returns the union's Expired member; the body escalates to the
compliance queue. This module's fleet runs the SHORT window (3s): the
expiry's precision is AT-LEAST (the DB-clock deadline + the sweep's
cadence) — the fail-close is the guarantee, and the drill asserts it."""

from __future__ import annotations

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import EscalatedTxn
from examples.ledgerwatch.tests.support import Fleet, review_run_ids, wait_for

pytestmark = pytest.mark.timeout(300)

FLEET_ENV: dict[str, str] = {"LW_APPROVAL_WINDOW_S": "3"}


async def test_approval_expired_escalates_to_compliance_queue(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-expired", count=1, high_every=1))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"
    await await_terminal(pool, schema, str(claim.flow_id), timeout_s=120)

    async def _ready() -> list[str] | None:
        ids = await review_run_ids(pool, schema, batch_id=batch.batch_id)
        return ids if ids else None

    (rid,) = await wait_for(_ready, timeout_s=60, message="the review run never appeared")

    # nobody approves. The window is 3s; the sweep's expiry arm is the
    # only live timer on the held row.
    raw = await await_terminal(pool, schema, rid, workflow="txn_review", timeout_s=90)
    verdict = EscalatedTxn.model_validate(raw)
    assert verdict.txn_id == "B-expired-T00000"
    assert verdict.to_queue == "compliance"

    # ONE durable compliance row — the escalation is real and exactly-once.
    rows = await pool.fetch(
        f'SELECT txn_id, batch_id FROM "{schema}".lw_compliance_queue WHERE batch_id = $1',  # noqa: S608  # Why: the schema identifier only.
        "B-expired",
    )
    assert len(rows) == 1
    assert rows[0]["txn_id"] == "B-expired-T00000"
    assert rows[0]["batch_id"] == "B-expired"

    # the hold row is the record: THIS run's hold was created and the
    # sweep abandoned it (never silently orphaned).
    statuses = await pool.fetchval(
        f'SELECT status FROM "{schema}".wf_signals WHERE workflow_id = $1',  # noqa: S608  # Why: as above.
        rid,
    )
    assert statuses == "abandoned", f"the expiry's record for this run's hold: {statuses}"
