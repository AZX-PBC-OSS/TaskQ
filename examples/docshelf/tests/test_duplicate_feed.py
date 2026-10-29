"""THE DUPLICATE-FEED DRILL — the feed is at-least-once; ingestion is
idempotent: zero double-effects under (a) a re-delivered delivery (the
SAME delivery id — the run-key arbiter's claim) and (b) overlapping
documents under a NEW delivery id (the register's idempotency)."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import await_terminal, make_docs
from taskq.workflows import FlowRunner


@pytest.mark.timeout(280)
async def test_duplicate_feed_zero_double_effects(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    docs: list[DocPayload] = make_docs(24, prefix="dup")

    # (a) THE SAME DELIVERY, DELIVERED TWICE (the feed's redelivery).
    delivery_id = f"dup-{secrets.token_hex(4)}"
    batch = DeliveryBatch(delivery_id=delivery_id, docs=docs)
    flow_a_pages = await deliver_batch(pool, schema, batch)
    assert len(flow_a_pages) == 1
    flow_a = flow_a_pages[0]
    flow_b_pages = await deliver_batch(pool, schema, batch)
    flow_b = flow_b_pages[0]

    assert flow_a == flow_b, (
        "the re-delivered delivery started a SECOND run — the run-key arbiter's claim broke"
    )

    await workers.spawn_ready(pool, tag="dup-a", max_concurrency=8)
    await workers.spawn_ready(pool, tag="dup-b", max_concurrency=8)
    status = await await_terminal(pool, schema, flow_a, timeout_s=240)
    assert status == "succeeded"

    # (b) THE SAME DOCUMENTS, re-delivered under a NEW delivery id: the
    # register dedupes; no document re-processes.
    second = DeliveryBatch(delivery_id=f"dup2-{secrets.token_hex(4)}", docs=docs)
    flow_c_pages = await deliver_batch(pool, schema, second)
    assert len(flow_c_pages) == 1
    flow_c = flow_c_pages[0]
    assert flow_c != flow_a, "a new delivery id must be a new run"
    status_c = await await_terminal(pool, schema, flow_c, timeout_s=240)
    assert status_c == "succeeded", f"the re-delivery run terminal-failed: {status_c}"

    # ZERO DOUBLE-EFFECTS, measured:
    # 1. the register holds each document EXACTLY once;
    n_docs = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".docshelf_documents WHERE doc_id LIKE $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "dup-%",  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )
    assert int(n_docs) == len(docs)

    # 2. every chunk extracted successfully EXACTLY once across ALL runs;
    summary = await store.api_call_summary(pool, schema)
    double_success = [k for k, v in summary.items() if v.get("success", 0) > 1]
    assert double_success == [], f"chunks extracted MORE THAN ONCE: {double_success}"

    # 3. the first delivery's digest is the complete typed receipt, and
    #    the re-delivery's digest is the HONEST EMPTY (a re-delivery
    #    processes nothing — the born-zero receipt).
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    digest = ShelfDigest.model_validate(await runner.result(flow_a))
    everything = digest.contracts_published + digest.ledger_entries + digest.quarantined
    assert sorted(everything) == sorted(d.doc_id for d in docs)
    digest_c = ShelfDigest.model_validate(await runner.result(flow_c))
    assert digest_c.total_docs == 0, digest_c
    everything_c = digest_c.contracts_published + digest_c.ledger_entries + digest_c.quarantined
    assert everything_c == [], "the re-delivery re-processed documents"

    # 4. the re-delivery's run spawned no child flows (nothing to do).
    links = await store.child_links(pool, schema)
    assert not [lnk for lnk in links if lnk["parent_run_id"] == flow_c]

    run_receipt(
        "duplicate-feed-receipt",
        {
            "flow_a": flow_a,
            "flow_b_same_run": flow_b == flow_a,
            "flow_c_new_delivery": flow_c,
            "flow_c_digest_total_docs": digest_c.total_docs,
            "registered_rows": int(n_docs),
            "double_extracted_chunks": double_success,
        },
    )
