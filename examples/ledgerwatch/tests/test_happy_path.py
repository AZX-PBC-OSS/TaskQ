"""THE HAPPY PATH: a batch flows ingest -> enrich -> branch ->
approve/clear -> report, and the typed receipt is EXACT.

Clean profile (the world behaving); the human approval resolved through
the typed door. The receipt's law: cleared + pending_review + failed ==
total, every id unique, the review runs' verdicts typed.
"""

from __future__ import annotations

import json

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import ApprovedTxn, CompletionReport
from taskq.workflows import HitlClient

pytestmark = pytest.mark.timeout(300)

FLEET_ENV: dict[str, str] = {}  # the clean profile (the support default)


async def test_happy_path_typed_receipt_exact(
    fleet: object, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-happy", count=6, high_every=3))
    # 6 txns: T00000 + T00003 high (75500 >= the 50000 threshold).

    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"

    raw = await await_terminal(pool, schema, str(claim.flow_id), timeout_s=180)
    # the read face decodes ONCE (a dict); the TYPED receipt is the
    # declared model re-validated over it — a shape that fails is a
    # shape that is not the receipt.
    report = CompletionReport.model_validate(raw)

    # THE TYPED RECEIPT, EXACT:
    assert report.batch_id == "B-happy"
    assert report.total == 6
    assert report.cleared == [
        "B-happy-T00001",
        "B-happy-T00002",
        "B-happy-T00004",
        "B-happy-T00005",
    ]
    assert report.pending_review == ["B-happy-T00000", "B-happy-T00003"]
    assert report.failed == []  # nothing lost

    # the counts' law (the envelope cannot lie)
    assert len(report.cleared) + len(report.pending_review) + len(report.failed) == report.total
    all_ids = report.cleared + report.pending_review + report.failed
    assert len(all_ids) == len(set(all_ids)), "a transaction counted twice — a double-effect"

    # THE HUMAN'S FACE: the review runs hold; the operator resolves
    # through the typed door; the verdicts are typed.
    client = HitlClient(pool, schema=schema)
    review_ids = await _review_run_ids(pool, schema, "B-happy")
    review_txns = sorted(await _review_txn_ids(pool, schema, review_ids))
    assert review_txns == report.pending_review, (
        "the review runs do not answer the report's pending set"
    )
    for rid in review_ids:
        holds = await client.list(rid)
        assert len(holds) == 1, f"review run {rid} holds {len(holds)} — expected exactly one"
        result = await client.resolve(
            holds[0].hold_id, {"approved": True, "reviewer": "op-1", "note": "clear"}
        )
        assert result.status == "delivered"
        raw = await await_terminal(pool, schema, rid, workflow="txn_review", timeout_s=60)
        verdict = ApprovedTxn.model_validate(raw)
        assert verdict.kind == "approved"
        assert verdict.reviewer == "op-1"


async def _review_run_ids(pool: asyncpg.Pool, schema: str, batch_id: str) -> list[str]:
    rows = await pool.fetch(
        f'SELECT id FROM "{schema}".jobs'  # noqa: S608  # Why: the schema identifier only.
        " WHERE step_key = '__flow__' AND metadata->>'workflow' = 'txn_review'"
        " AND payload->'wf_input'->>'batch_id' = $1 ORDER BY id",
        batch_id,
    )
    return [str(r["id"]) for r in rows]


async def _review_txn_ids(pool: asyncpg.Pool, schema: str, review_ids: list[str]) -> list[str]:
    ids = []
    for rid in review_ids:
        raw = await pool.fetchval(
            f'SELECT payload FROM "{schema}".jobs'  # noqa: S608  # Why: as above.
            " WHERE id = $1 AND step_key = '__flow__'",
            rid,
        )
        ids.append(json.loads(raw)["wf_input"]["txn_id"])
    return ids
