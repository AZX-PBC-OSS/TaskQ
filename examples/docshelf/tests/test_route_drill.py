"""THE ROUTE DRILL — the typed route branches by class and ONLY the
class's arm runs: every contract document's element runs the summary
arm, every invoice's the ledger arm, every unknown's the quarantine
arm — receipts per class, read from the route's child rows."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf.app import deliver_batch
from examples.docshelf.mockapi import roll
from examples.docshelf.models import DeliveryBatch, DocPayload
from examples.docshelf.tests.conftest import await_terminal


def _class_of(doc_id: str) -> str:
    """The classifier's deterministic verdict (the app's own law)."""
    r = roll("class", doc_id)
    if r < 0.45:
        return "contract"
    if r < 0.90:
        return "invoice"
    return "unknown"


def _corpus(n_per_class: int) -> list[DocPayload]:
    """Documents chosen so EVERY class is present (the drill's corpus)."""
    docs: list[DocPayload] = []
    counts = {"contract": 0, "invoice": 0, "unknown": 0}
    i = 0
    while min(counts.values()) < n_per_class:
        doc = DocPayload(doc_id=f"route-{i:04}", content=f"route corpus {i} " * 8)
        cls = _class_of(doc.doc_id)
        if counts[cls] < n_per_class:
            counts[cls] += 1
            docs.append(doc)
        i += 1
    return docs


_ARM_BY_CLASS = {
    # The arm child's step key names the arm by its ELEMENT TYPE
    # (`classify.item:<module.qualname of the union member>`).
    "contract": "ContractDoc",
    "invoice": "InvoiceDoc",
    "unknown": "UnknownDoc",
}


@pytest.mark.timeout(280)
async def test_route_drill_each_class_runs_only_its_arm(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    docs = _corpus(n_per_class=5)
    delivery_id = f"route-{secrets.token_hex(4)}"

    flow_ids = await deliver_batch(pool, schema, DeliveryBatch(delivery_id=delivery_id, docs=docs))
    assert len(flow_ids) == 1
    flow_id = flow_ids[0]
    await workers.spawn_ready(pool, tag="route-a", max_concurrency=8)
    status = await await_terminal(pool, schema, flow_id, timeout_s=240)
    assert status == "succeeded"

    # The route's children: the arm's NAME is the child row's step key;
    # the element rides the fork's wf_item wrapper.
    rows = await pool.fetch(
        # the interpolated value is the app's settings-validated schema
        # identifier; every caller-controlled value is $N-bound.
        f"SELECT step_key, "  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        f"COALESCE(payload->>'doc_id', payload->'wf_item'->>'doc_id') AS doc_id "
        f'FROM "{schema}".jobs '  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = $1 AND step_key LIKE 'classify.item:%'",
        flow_id,
    )
    assert len(rows) == len(docs), f"the route forked {len(rows)} children for {len(docs)} docs"

    seen: dict[str, set[str]] = {doc.doc_id: set() for doc in docs}
    for row in rows:
        arm = row["step_key"].rsplit(".", 1)[-1]
        assert row["doc_id"] in seen, f"the route ran a child for a foreign doc: {row['doc_id']}"
        seen[row["doc_id"]].add(arm)

    receipts: dict[str, int] = {"contract": 0, "invoice": 0, "unknown": 0}
    for doc in docs:
        expected = _ARM_BY_CLASS[_class_of(doc.doc_id)]
        ran = seen[doc.doc_id]
        assert ran == {expected}, (
            f"{doc.doc_id} (class {_class_of(doc.doc_id)}) ran arms {ran}, expected {{{expected}}}"
        )
        receipts[_class_of(doc.doc_id)] += 1

    assert all(v > 0 for v in receipts.values()), f"a class was never exercised: {receipts}"

    run_receipt(
        "route-drill-receipt",
        {
            "delivery_id": delivery_id,
            "flow_id": flow_id,
            "documents": len(docs),
            "per_class_receipts": receipts,
            "arm_children_total": len(rows),
        },
    )
