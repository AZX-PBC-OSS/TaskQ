"""THE CACHE DRILL — the cross-run step cache: a RE-RUN of an unchanged
document hits the step cache; the hits are MEASURED (the producing run's
receipt on the node row; the mock extraction API never re-called)."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP
from examples.docshelf.models import DocRef, IndexedDoc
from examples.docshelf.tests.conftest import await_terminal
from taskq.workflows import FlowRunner


async def _run_reprocess(pool: asyncpg.Pool, schema: str, ref: DocRef, key: str) -> str:
    runner = FlowRunner(SHELF_APP.get("docshelf_reprocess"), pool, schema)
    claim = await runner.create_flow(input=ref, run_key=key)
    flow_id = str(claim.flow_id)
    status = await await_terminal(pool, schema, flow_id, timeout_s=120)
    assert status == "succeeded", f"the reprocess run {key} terminal: {status}"
    return flow_id


@pytest.mark.timeout(280)
async def test_cache_hit_on_unchanged_rerun(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    ref = DocRef(doc_id="cache-doc-1", content="the cache drill's document body " * 20)

    await workers.spawn_ready(pool, tag="cache-a", max_concurrency=4)

    # RUN 1: the cold run — the extraction API is called per chunk.
    flow_1 = await _run_reprocess(pool, schema, ref, f"cache-run-1-{secrets.token_hex(4)}")
    summary_after_1 = await store.api_call_summary(pool, schema)
    n_chunks = sum(1 for k in summary_after_1 if k.startswith("cache-doc-1:"))
    assert n_chunks >= 1, "the cold run never extracted"

    # RUN 2: the SAME document (same body, same input), a NEW run key —
    # the cross-run cache's exact face.
    flow_2 = await _run_reprocess(pool, schema, ref, f"cache-run-2-{secrets.token_hex(4)}")
    summary_after_2 = await store.api_call_summary(pool, schema)

    # THE HITS, MEASURED: the mock extraction API was NEVER re-called.
    calls_before = sum(
        sum(per.values()) for k, per in summary_after_1.items() if k.startswith("cache-doc-1:")
    )
    calls_after = sum(
        sum(per.values()) for k, per in summary_after_2.items() if k.startswith("cache-doc-1:")
    )
    assert calls_after == calls_before, (
        f"the re-run re-called the extraction API ({calls_before} -> {calls_after}) — a cache MISS"
    )

    # THE RECEIPT: the hit's node row names the PRODUCING run.
    hit = await pool.fetchval(
        f"SELECT metadata->'wf_cache_hit'->>'run_id' FROM \"{schema}\".jobs "  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = $1 AND step_key = 'index_doc'",
        flow_2,
    )
    assert hit == flow_1, f"the cache receipt names {hit}, expected the producing run {flow_1}"

    # The typed result decodes the SAME either way.
    runner = FlowRunner(SHELF_APP.get("docshelf_reprocess"), pool, schema)
    result_2 = IndexedDoc.model_validate(await runner.result(flow_2))
    result_1 = IndexedDoc.model_validate(await runner.result(flow_1))
    assert result_2 == result_1
    assert result_2.doc_id == ref.doc_id

    run_receipt(
        "cache-drill-receipt",
        {
            "producing_run": flow_1,
            "hit_run": flow_2,
            "chunks_indexed": n_chunks,
            "api_calls_cold": calls_before,
            "api_calls_warm": calls_after,
            "cache_hits_measured": 1,
            "receipt_on_row": hit,
        },
    )
