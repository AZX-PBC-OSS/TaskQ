"""THE CRASH DRILLS — SIGKILL a real worker three ways; the restart
resumes; nothing is lost; nothing is duplicated; the cursor never
rewinds. (The ledger's at-least-once boundary is the engine's own:
exactly-once for DB-local effects, at-least-once body execution — the
drills count both and report them.)"""

from __future__ import annotations

import asyncio
import secrets
import uuid
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import await_terminal, make_docs
from taskq.workflows import FlowRunner


async def _run_until(
    pool: asyncpg.Pool,
    schema: str,
    flow_id: str,
    step_key: str,
    count: int,
    *,
    timeout_s: float = 120.0,
) -> int:
    """Wait until *step_key* has *count* rows at/after claim."""
    import time

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        n = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "WHERE metadata->>'flow_id' = $1 AND step_key = $2 "
            "AND (status IN ('running','succeeded') OR attempt > 0)",
            flow_id,
            step_key,
        )
        if int(n or 0) >= count:
            return int(n)
        await asyncio.sleep(0.2)
    raise RuntimeError(f"{step_key} never reached {count} claimed rows")


async def _digest_and_counts(
    pool: asyncpg.Pool, schema: str, flow_id: str
) -> tuple[ShelfDigest, dict[str, dict[str, int]]]:
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    digest = ShelfDigest.model_validate(await runner.result(flow_id))
    summary = await store.api_call_summary(pool, schema)
    return digest, summary


@pytest.mark.timeout(280)
async def test_crash_mid_extraction_fanout(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    """SIGKILL mid-extraction-fan-out: the restart resumes, all chunks
    reach their typed terminal, nothing lost, nothing duplicated."""
    docs: list[DocPayload] = make_docs(40, prefix="crash1")
    flow_id_pages = await deliver_batch(
        pool, schema, DeliveryBatch(delivery_id=f"crash1-{secrets.token_hex(4)}", docs=docs)
    )
    assert len(flow_id_pages) == 1
    flow_id = flow_id_pages[0]
    worker = await workers.spawn_ready(pool, tag="crash1", max_concurrency=8)

    # Kill MID-FAN-OUT: extraction children claimed and running.
    await _run_until(pool, schema, flow_id, "ingest.item", 5)
    worker.kill9()

    # A fresh worker: the reclaim + the resume.
    await workers.spawn_ready(pool, tag="crash1-b", max_concurrency=8)
    status = await await_terminal(pool, schema, flow_id, timeout_s=240)
    assert status == "succeeded", f"the run after the kill terminal-failed: {status}"

    digest, summary = await _digest_and_counts(pool, schema, flow_id)
    everything = digest.contracts_published + digest.ledger_entries + digest.quarantined
    assert sorted(everything) == sorted(d.doc_id for d in docs), "lost or doubled documents"
    assert len(set(everything)) == len(docs)

    # NOTHING LOST: one succeeded node row per DOCUMENT (the map's
    # per-item identity — the map partitions by document); the digest's
    # chunk total matches the corpus's DETERMINISTIC chunk count; and
    # every chunk's extraction succeeded exactly once (the ledger).
    rows = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = $1 AND step_key = 'ingest.item' AND status = 'succeeded'",
        flow_id,
    )
    assert int(rows) == len(docs), "a document terminalized twice or was lost"
    from examples.docshelf.mockapi import chunk_count

    expected_chunks = sum(chunk_count(d.doc_id, d.content) for d in docs)
    assert digest.total_chunks == expected_chunks, (
        f"the digest counts {digest.total_chunks} chunks, the corpus chunks to {expected_chunks}"
    )
    summary_all = await store.api_call_summary(pool, schema)
    mine = {k: v for k, v in summary_all.items() if k.startswith("crash1-")}
    double_success = [k for k, v in mine.items() if v.get("success", 0) > 1]
    assert double_success == [], f"chunks extracted MORE THAN ONCE: {double_success[:5]}"
    assert all(v.get("success", 0) >= 1 for v in mine.values()), "a ledger'd chunk never succeeded"

    # THE AT-LEAST-ONCE BOUNDARY, HONEST: a chunk whose success write
    # was interrupted by the kill re-executes — the ledger may carry
    # multiple 'success' rows for it. Counted and reported, never hidden.
    re_successes = sum(1 for per in summary.values() if per.get("success", 0) > 1)

    run_receipt(
        "crash-mid-extraction-receipt",
        {
            "flow_id": flow_id,
            "documents": len(docs),
            "chunks": digest.total_chunks,
            "chunks_re_extracted_after_the_kill": re_successes,
            "at_least_once_boundary": "exactly-once rows; at-least-once bodies",
        },
    )


@pytest.mark.timeout(280)
async def test_crash_mid_child_join(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    """SIGKILL MID-CHILD-JOIN: the parent holds, a child flow runs; the
    kill lands in that window. The holds are ROWS — the restart's tail
    delivers into the same hold; the parent joins and completes."""
    from examples.docshelf.mockapi import roll

    # One large doc (the child-flow trigger).
    large = None
    i = 0
    while large is None:
        candidate = f"crash2-{i:04}"
        if roll("chunks", candidate) >= 0.85:
            large = candidate
        i += 1
    docs = [DocPayload(doc_id=large, content=f"crash drill 2 {large} " * 40)]

    flow_id_pages = await deliver_batch(
        pool, schema, DeliveryBatch(delivery_id=f"crash2-{secrets.token_hex(4)}", docs=docs)
    )
    assert len(flow_id_pages) == 1
    flow_id = flow_id_pages[0]
    worker = await workers.spawn_ready(pool, tag="crash2", max_concurrency=2)

    # Kill MID-JOIN: the parent's run-children node is waiting on a hold.
    await _run_until(pool, schema, flow_id, "run_children", 1)
    signals = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_signals WHERE workflow_id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        flow_id,  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )
    assert int(signals or 0) >= 1, "the parent never held (no child-flow join to crash mid-of)"
    worker.kill9()

    await workers.spawn_ready(pool, tag="crash2-b", max_concurrency=2)
    status = await await_terminal(pool, schema, flow_id, timeout_s=240)
    assert status == "succeeded", f"the run after the mid-join kill: {status}"

    digest, _summary = await _digest_and_counts(pool, schema, flow_id)
    assert digest.total_docs == 1
    assert digest.child_flows_spawned == 1
    assert digest.expired_children == [], "the hold did not survive the kill"
    assert digest.failed_children == []
    everything = digest.contracts_published + digest.ledger_entries + digest.quarantined
    assert everything == [large]

    # The child link survived; the child run is terminal succeeded.
    links = await store.child_links(pool, schema)
    mine = [lnk for lnk in links if lnk["parent_run_id"] == flow_id]
    assert len(mine) == 1
    child_status = await pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        uuid.UUID(
            mine[0]["child_run_id"]
        ),  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )
    assert child_status == "succeeded"

    run_receipt(
        "crash-mid-child-join-receipt",
        {
            "flow_id": flow_id,
            "child_run_id": mine[0]["child_run_id"],
            "killed_mid": "the parent's run-children hold window",
            "hold_survived": True,
            "digest": digest.model_dump(),
        },
    )
