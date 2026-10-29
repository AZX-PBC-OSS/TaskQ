"""THE FLAKE DRILL — a 500-document batch under the RECORDED adverse
profile (flake 20%, timeouts 7%, delays 30-200 ms). Every chunk reaches
its typed terminal through the ladder; nothing is lost; nothing is
duplicated. The invariant is UNCONDITIONAL, and the drill's receipt
records the retry counts — the dynamics must FIRE (a zero-flake run
would not prove the ladder)."""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import await_terminal, make_docs
from taskq.workflows import FlowRunner


@pytest.mark.timeout(900)
async def test_flake_drill_500_documents(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    docs: list[DocPayload] = make_docs(500, prefix="flake")
    delivery_id = f"flake-{secrets.token_hex(4)}"

    # THE FLEET FIRST: the pages deliver sequentially, each awaiting its
    # own terminal — the workers must be live before the first page.
    await workers.spawn_ready(pool, tag="flake-a", max_concurrency=8)
    await workers.spawn_ready(pool, tag="flake-b", max_concurrency=8)
    flow_ids = await deliver_batch(pool, schema, DeliveryBatch(delivery_id=delivery_id, docs=docs))
    assert len(flow_ids) == 5, f"500 docs page into 5 flows, got {len(flow_ids)}"

    started = time.monotonic()
    statuses = []
    for fid in flow_ids:
        statuses.append(await await_terminal(pool, schema, fid, timeout_s=840))
    wall_s = time.monotonic() - started
    assert all(s == "succeeded" for s in statuses), f"the flake drill terminal-failed: {statuses}"

    # THE TYPED TERMINAL, EXACT — AGGREGATED ACROSS THE PAGES: all 500
    # documents, each in exactly one arm of exactly one page's digest.
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    digests = [ShelfDigest.model_validate(await runner.result(fid)) for fid in flow_ids]
    everything = [
        d
        for dig in digests
        for d in (*dig.contracts_published, *dig.ledger_entries, *dig.quarantined)
    ]
    assert sorted(everything) == sorted(d.doc_id for d in docs), "lost or doubled documents"
    assert len(set(everything)) == 500
    assert sum(dig.total_docs for dig in digests) == 500
    n_chunks = sum(dig.total_chunks for dig in digests)

    # THE EXTRACTION LEDGER — the ladder's receipt:
    summary = await store.api_call_summary(pool, schema)
    per_chunk_attempts = {k: v.get("start", 0) for k, v in summary.items()}
    success_chunks = {k for k, v in summary.items() if v.get("success", 0) >= 1}
    assert len(success_chunks) == n_chunks, "a chunk never extracted successfully"
    # NOTHING LOST: every chunk key names a doc in the batch.
    assert all(k.startswith("flake-") for k in summary), "foreign chunks in the ledger"

    total_attempts = sum(per_chunk_attempts.values())
    retried_chunks = sum(1 for a in per_chunk_attempts.values() if a > 1)
    flakes = sum(v.get("flake", 0) for v in summary.values())
    timeouts = sum(v.get("timeout", 0) for v in summary.values())

    # THE DYNAMICS FIRED — a zero-flake run would not prove the ladder:
    assert flakes > 0, "no transient failures were injected (the profile did not fire)"
    assert timeouts > 0, "no timeouts were injected (the profile did not fire)"
    assert retried_chunks > 0, "no chunk needed a retry"

    # NOTHING DUPLICATED: one SUCCEEDED node row per DOCUMENT (the map's
    # per-item identity — the map partitions by document, the engine's
    # 1000-parent join bound), and every chunk extracted exactly once.
    rows = await pool.fetch(
        f'SELECT count(*) AS n FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE step_key = 'ingest.item' AND status = 'succeeded' "
        "AND metadata->>'flow_id' = ANY($1::text[])",
        flow_ids,
    )
    assert int(rows[0]["n"]) == 500, "a document terminalized twice or was lost"
    double_success = [k for k, v in summary.items() if v.get("success", 0) > 1]
    assert double_success == [], f"chunks extracted MORE THAN ONCE: {double_success[:5]}"

    # THE LADDER LANDED: no chunk died of flakiness (the whole point).
    failed = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = ANY($1::text[]) AND status = 'failed'",
        flow_ids,
    )
    assert int(failed) == 0, f"{failed} nodes terminal-failed — a document died of flakiness"

    run_receipt(
        "flake-drill-receipt",
        {
            "delivery_id": delivery_id,
            "flow_ids": flow_ids,
            "documents": 500,
            "chunks": n_chunks,
            "wall_seconds": round(wall_s, 1),
            "total_extraction_attempts": total_attempts,
            "chunks_that_needed_a_retry": retried_chunks,
            "transient_failures_injected": flakes,
            "timeouts_injected": timeouts,
            "max_attempts_observed": max(per_chunk_attempts.values()),
            "terminal_failed_nodes": 0,
        },
    )
