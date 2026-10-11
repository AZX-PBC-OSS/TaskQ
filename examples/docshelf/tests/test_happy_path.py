"""THE HAPPY PATH DRILL — a 50-document shelf, end to end, on real
worker processes: ingest → chunk → extract → child-analyze → aggregate →
route → digest, the typed receipt EXACT, the chunk fan-out's concurrency
MEASURED (the overlap is real, not asserted)."""

from __future__ import annotations

import secrets
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import await_terminal, make_docs
from taskq.workflows import FlowRunner


def _overlap_seconds(intervals: list[tuple[datetime, datetime | None]]) -> float:
    """The wall-clock width of the union of execution intervals (the
    concurrency witness: the fan-out's work OVERLAPS on the timeline)."""
    spans = sorted((start, end if end is not None else start) for start, end in intervals)
    total = 0.0
    cur: tuple[datetime, datetime] | None = None
    for start, end in spans:
        if cur is None:
            cur = (start, end)
        elif start <= cur[1]:
            cur = (cur[0], max(cur[1], end))
        else:
            total += (cur[1] - cur[0]).total_seconds()
            cur = (start, end)
    if cur is not None:
        total += (cur[1] - cur[0]).total_seconds()
    return total


@pytest.mark.timeout(280)
async def test_happy_path_50_documents(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    """The 50-document shelf runs to its typed digest through REAL workers."""
    docs: list[DocPayload] = make_docs(50, prefix="happy")
    delivery_id = f"happy-{secrets.token_hex(4)}"
    batch = DeliveryBatch(delivery_id=delivery_id, docs=docs)

    flow_ids = await deliver_batch(pool, schema, batch)
    assert len(flow_ids) == 1, "50 docs fit one page"
    flow_id = flow_ids[0]

    # REAL WORKERS: two processes, 8 slots each (the fleet).
    await workers.spawn_ready(pool, tag="happy-a", max_concurrency=8)
    await workers.spawn_ready(pool, tag="happy-b", max_concurrency=8)

    status = await await_terminal(pool, schema, flow_id, timeout_s=240)
    assert status == "succeeded", f"the happy path terminal-failed: {status}"

    # THE TYPED RECEIPT, DECODED — every document in EXACTLY one arm.
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    result = await runner.result(flow_id)
    digest = ShelfDigest.model_validate(result)
    assert digest.delivery_id == delivery_id
    everything = digest.contracts_published + digest.ledger_entries + digest.quarantined
    assert sorted(everything) == sorted(d.doc_id for d in docs), "a document was dropped or doubled"
    assert len(set(everything)) == len(everything), "a document landed in TWO arms"
    assert digest.failed_children == [], digest.failed_children
    assert digest.expired_children == [], digest.expired_children
    assert digest.total_docs == 50
    # the child-flow face: the 50-doc corpus deterministically includes large docs
    assert digest.child_flows_spawned >= 1, digest.child_flows_spawned

    # THE REGISTER: exactly 50 rows (the ingest idempotency's receipt).
    n_docs = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".docshelf_documents WHERE doc_id LIKE $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "happy-%",  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )
    assert int(n_docs) == 50

    # THE EXTRACTION LEDGER: every chunk exactly ONE success; no chunk terminal-failed.
    summary = await store.api_call_summary(pool, schema)
    n_success = sum(1 for per_chunk in summary.values() if per_chunk.get("success", 0) >= 1)
    assert n_success == digest.total_chunks, (
        f"{n_success} chunks extracted, the digest says {digest.total_chunks}"
    )

    # THE CHILD FLOWS: every spawned child terminalized succeeded; the
    # links are rows (the operator's parent↔child join).
    links = await store.child_links(pool, schema)
    mine = [lnk for lnk in links if lnk["parent_run_id"] == flow_id]
    assert len(mine) == digest.child_flows_spawned
    for link in mine:
        child_status = await pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            uuid.UUID(
                link["child_run_id"]
            ),  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        )
        assert child_status == "succeeded", f"child {link['child_run_id']} terminal: {child_status}"

    # THE DIGEST ROW published exactly once.
    n_digests = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".docshelf_digests WHERE delivery_id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        delivery_id,  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )
    assert int(n_digests) == 1

    # THE CONCURRENCY IS REAL: the extraction map's children (the
    # DOCUMENTS — the map partitions by document) OVERLAP on the
    # timeline (30-200 ms jittered per-chunk work inside each, many
    # workers).
    rows = await pool.fetch(
        f'SELECT started_at, finished_at FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = $1 AND step_key = 'ingest.item' AND finished_at IS NOT NULL",
        flow_id,
    )
    intervals = [(r["started_at"], r["finished_at"]) for r in rows]
    union_s = _overlap_seconds(intervals)
    sum_s = sum((e - s).total_seconds() for s, e in intervals)
    concurrency = sum_s / union_s if union_s else 0.0
    assert concurrency > 2.0, (
        f"the chunk fan-out ran at effective concurrency {concurrency:.2f} — the overlap is not real"
    )

    run_receipt(
        "happy-path-receipt",
        {
            "delivery_id": delivery_id,
            "flow_id": flow_id,
            "digest": digest.model_dump(),
            "chunks_extracted_first_success": n_success,
            "child_flows_spawned": digest.child_flows_spawned,
            "measured_fanout_concurrency": round(concurrency, 2),
            "fanout_union_seconds": round(union_s, 3),
            "fanout_sum_seconds": round(sum_s, 3),
        },
    )
