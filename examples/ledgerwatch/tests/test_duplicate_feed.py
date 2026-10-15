"""THE DUPLICATE-FEED DRILL: the feed re-delivers batches (the
at-least-once world). The pipeline is IDEMPOTENT — the re-delivery
changes NOTHING: same claim, zero new effects, one report."""

from __future__ import annotations

from typing import cast

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import CompletionReport
from examples.ledgerwatch.tests.support import (
    Fleet,
    child_jobs,
    journal_receipt,
    journal_rows,
    review_run_ids,
    wait_for,
)

pytestmark = pytest.mark.timeout(300)

FLEET_ENV: dict[str, str] = {"LW_PROFILE": "adverse"}


async def _effects(
    pool: asyncpg.Pool, schema: str, txn_prefix: str, flow_id: str
) -> dict[str, object]:
    rows = await journal_rows(pool, schema, txn_prefix=txn_prefix)
    return {
        "journal": journal_receipt(rows),
        "enrich_children": await child_jobs(pool, schema, "ingest.item", flow_id=flow_id),
        "route_children": await child_jobs(pool, schema, "branch.item:%", flow_id=flow_id),
        "this_batchs_report": await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".lw_reports WHERE batch_id = $1',  # noqa: S608  # Why: as above.
            txn_prefix.rstrip("-"),
        ),
        "this_batchs_review_runs": len(
            await review_run_ids(pool, schema, batch_id=txn_prefix.rstrip("-"))
        ),
    }


async def test_duplicate_feed_post_terminal_changes_nothing(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    """The re-delivery AFTER the run's terminal: the arbiter's
    REFUSED-TO-REUSE verdict, zero new effects."""
    batch = build_batch(BatchSpec(batch_id="B-dup-1", count=5, high_every=2))
    claim1 = await submit_batch(pool, schema, batch)
    assert claim1.kind == "created"
    raw = await await_terminal(pool, schema, str(claim1.flow_id), timeout_s=180)
    report = CompletionReport.model_validate(raw)
    assert report.failed == []
    before = await _effects(pool, schema, "B-dup-1-", str(claim1.flow_id))

    # THE FEED RE-DELIVERS (the same batch, byte for byte).
    claim2 = await submit_batch(pool, schema, batch)
    assert claim2.kind == "existing-terminal", f"the re-delivery was re-fired: {claim2!r}"
    assert str(claim2.flow_id) == str(claim1.flow_id)

    after = await _effects(pool, schema, "B-dup-1-", str(claim1.flow_id))
    assert after == before, f"the re-delivery changed the world: {before} -> {after}"


async def test_duplicate_feed_mid_flight_changes_nothing(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    """The re-delivery WHILE the first run is in flight: the arbiter's
    existing-running verdict — one run, zero double-effects."""
    batch = build_batch(BatchSpec(batch_id="B-dup-2", count=120, high_every=0))

    claim1 = await submit_batch(pool, schema, batch)
    assert claim1.kind == "created"

    # mid-flight: the enrichment is live
    async def _live() -> bool:
        j = journal_receipt(await journal_rows(pool, schema, txn_prefix="B-dup-2-"))
        return j["total_calls"] >= 30

    await wait_for(_live, timeout_s=60, interval=0.1, message="the fan-out never went live")
    # the mid-flight snapshot (the world before the re-delivery)
    await _effects(pool, schema, "B-dup-2-", str(claim1.flow_id))

    claim2 = await submit_batch(pool, schema, batch)
    assert claim2.kind == "existing-running", (
        f"the mid-flight re-delivery started a SECOND run: {claim2!r}"
    )
    assert str(claim2.flow_id) == str(claim1.flow_id)

    raw = await await_terminal(pool, schema, str(claim1.flow_id), timeout_s=300)
    report = CompletionReport.model_validate(raw)
    assert report.failed == []
    assert report.cleared == sorted(t.txn_id for t in batch.transactions)
    after = await _effects(pool, schema, "B-dup-2-", str(claim1.flow_id))

    # ZERO double-effects: the re-delivery added NOTHING beyond the
    # first run's own completion.
    assert after["enrich_children"] == {"succeeded": 120}
    assert after["route_children"] == {"succeeded": 120}
    assert cast("int", after["this_batchs_report"]) == 1
    journal = cast("dict[str, object]", after["journal"])
    per_txn = cast("dict[str, int]", journal["calls_per_txn"])
    assert len(per_txn) == 120, "a transaction never called the API, or the journal lost rows"
    outcomes = cast("dict[str, int]", journal["outcomes"])
    ok_total = outcomes.get("ok", 0)
    assert ok_total == 120, f"exactly one successful call per txn — got {ok_total}"
    assert outcomes.get("inflight", 0) == 0, (
        "a started call never finished — the re-delivery raced a live call into a double-effect"
    )
