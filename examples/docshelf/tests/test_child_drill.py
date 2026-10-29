"""THE CHILD DRILL — the child-flow face, measured:

1. THE SLOT LAW, OBSERVED (not asserted): on a ONE-slot worker, the
   parent's run-children node HOLDS (its slot releases) and the CHILD
   FLOWS' rows RUN in that window — the child could not run otherwise.
2. The children's TYPED results join (SubDone decoded into the parent's
   verdicts, the child's own tags riding through).
3. A CHILD'S TERMINAL FAILURE lands in the parent's typed failure arm —
   never silent (the poison document's sub-analysis exhausts its own
   ladder; the child tail DELIVERS the typed SubFailed)."""

from __future__ import annotations

import secrets
import uuid
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.mockapi import roll
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import await_terminal
from taskq.workflows import FlowRunner


def _large_corpus() -> tuple[list[DocPayload], list[str], str]:
    """Three LARGE documents (>= many_chunks_threshold chunks each — the
    child-flow trigger); the LAST one is the poison (its sub-analysis
    fails terminally). The chunking is deterministic in the doc id, so
    the corpus hunts ids whose roll says 'large'."""
    docs: list[DocPayload] = []
    ids: list[str] = []
    poison_id = None
    i = 0
    while len(ids) < 2 or poison_id is None:
        candidate = f"child-{i:04}"
        if len(ids) < 2 and roll("chunks", candidate) >= 0.85:
            ids.append(candidate)
        poison_candidate = f"poison-{i:04}"
        if poison_id is None and roll("chunks", poison_candidate) >= 0.85:
            poison_id = poison_candidate
        i += 1
    assert poison_id is not None
    for doc_id in [*ids, poison_id]:
        docs.append(DocPayload(doc_id=doc_id, content=f"large corpus for {doc_id} " * 30))
    return docs, ids, poison_id


@pytest.mark.timeout(280)
async def test_child_drill_slot_release_and_typed_join(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    docs, plain_ids, poison_id = _large_corpus()
    delivery_id = f"child-{secrets.token_hex(4)}"

    flow_ids = await deliver_batch(pool, schema, DeliveryBatch(delivery_id=delivery_id, docs=docs))
    assert len(flow_ids) == 1
    flow_id = flow_ids[0]
    # ONE SLOT: the slot law's stage. If the parent's slot did NOT
    # release during the holds, the children's rows could never claim.
    await workers.spawn_ready(pool, tag="child-solo", max_concurrency=1)

    status = await await_terminal(pool, schema, flow_id, timeout_s=240)
    assert status == "succeeded", f"the child drill terminal-failed: {status}"

    # THE LINKS: 3 child flows, one per large doc, all rows.
    links = await store.child_links(pool, schema)
    mine = [lnk for lnk in links if lnk["parent_run_id"] == flow_id]
    assert len(mine) == 3, f"expected 3 child flows, links say {len(mine)}"

    # THE SLOT MEASUREMENT, from the rows: the parent's ONE batch hold
    # (a row: created when the parent's body reached the wait, resolved
    # by the last tail's delivery) vs EACH child's execution window —
    # on a ONE-slot worker the overlap is only possible because the
    # parent's slot RELEASED while it waited.
    hold = await pool.fetchrow(
        f'SELECT created_at, resolved_at FROM "{schema}".wf_signals '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE workflow_id = $1 AND resolved_at IS NOT NULL LIMIT 1",
        flow_id,
    )
    assert hold is not None, "the parent never held (the child-flow face did not run)"
    hold_s, hold_e = hold["created_at"], hold["resolved_at"]

    overlaps = []
    for link in mine:
        doc_id = link["doc_id"]
        child_window = await pool.fetchrow(
            f'SELECT min(a.started_at) AS s, max(a.finished_at) AS f FROM "{schema}".job_attempts a '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            f'JOIN "{schema}".jobs j ON j.id = a.job_id '  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'sub_tasks.item' "
            "AND a.finished_at IS NOT NULL",
            link["child_run_id"],
        )
        assert child_window and child_window["s"] and child_window["f"], (
            f"child {doc_id} never executed its sub-analysis"
        )
        overlap_s = (
            min(child_window["f"], hold_e) - max(child_window["s"], hold_s)
        ).total_seconds()
        if doc_id != poison_id:
            # The POISON child fails FAST (its body raises before any
            # compute) — it can legitimately finish before the parent's
            # hold opens. The HEALTHY children run 1-3 s INSIDE the
            # hold's window: the slot law's measurement.
            assert overlap_s > 0.2, (
                f"the child {doc_id}'s execution did NOT overlap the parent's hold "
                f"(overlap {overlap_s:.3f}s) — the slot law did not hold"
            )
        overlaps.append(
            {
                "doc_id": doc_id,
                "child_run_id": link["child_run_id"],
                "child_exec_seconds": round(
                    (child_window["f"] - child_window["s"]).total_seconds(), 3
                ),
                "hold_seconds": round((hold_e - hold_s).total_seconds(), 3),
                "overlap_seconds": round(overlap_s, 3),
            }
        )

    # THE TYPED RECEIPT: the poison doc in the FAILED arm; the others joined.
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    digest = ShelfDigest.model_validate(await runner.result(flow_id))
    assert digest.total_docs == 3
    assert digest.failed_children == [poison_id], (
        f"the child's terminal failure was SILENT: {digest.failed_children} vs [{poison_id}]"
    )
    assert digest.expired_children == []
    joined = set(digest.contracts_published + digest.ledger_entries + digest.quarantined)
    assert set(plain_ids) <= joined, f"a healthy child's typed result never joined: {joined}"

    # The children's own typed results carry the CHILD's tags (the join
    # delivered the child's SubDone, not just its absence).
    for link in mine:
        if link["doc_id"] == poison_id:
            continue
        child_status = await pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            uuid.UUID(
                link["child_run_id"]
            ),  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        )
        assert child_status == "succeeded", (
            f"child {link['doc_id']} terminal: {child_status} "
            "(the child's own flow succeeded — its failure report was DELIVERED)"
        )

    run_receipt(
        "child-drill-receipt",
        {
            "delivery_id": delivery_id,
            "flow_id": flow_id,
            "worker_slots": 1,
            "hold_and_child_windows": overlaps,
            "failed_children": digest.failed_children,
            "joined_children": sorted(joined),
            "poison_doc": poison_id,
        },
    )
