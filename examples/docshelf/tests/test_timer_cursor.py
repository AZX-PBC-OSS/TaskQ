"""THE TIMER + CURSOR DRILL — the nightly re-index on the durable timer:

* the schedule fires NO EARLIER than its `next_fire_at` (the tick job's
  enqueue time is at-or-after the scheduled instant);
* a worker death mid-timer-wait is survived: the schedule row is
  durable, the fire happens, the flow resumes;
* the DURABLE CURSOR: only NEW documents re-process; the cursor never
  rewinds."""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ReindexReport
from examples.docshelf.tests.conftest import await_terminal, make_docs


async def _schedule(pool: asyncpg.Pool, schema: str, delay_s: float):
    """Arm the schedule: enable + set next_fire_at now()+delay (the
    drill owns the ROW, the same one the leader reads)."""
    row = await pool.fetchrow(
        f'UPDATE "{schema}".cron_schedules SET enabled = true, next_fire_at = now() '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "+ make_interval(secs => $1) WHERE name = 'nightly-reindex' RETURNING next_fire_at",
        delay_s,
    )
    assert row is not None, (
        "the nightly-reindex schedule is not registered (the worker's boot creates it)"
    )
    return row["next_fire_at"]


async def _disarm(pool: asyncpg.Pool, schema: str) -> None:
    await pool.execute(
        f"UPDATE \"{schema}\".cron_schedules SET enabled = false WHERE name = 'nightly-reindex'"  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )


@pytest.mark.timeout(280)
async def test_timer_and_cursor(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    await _disarm(pool, schema)  # the drill controls the fires

    # Seed the shelf with a FIRST batch (the previous night's corpus).
    docs_a: list[DocPayload] = make_docs(12, prefix="night-a")
    flow_a_pages = await deliver_batch(
        pool, schema, DeliveryBatch(delivery_id=f"night-a-{secrets.token_hex(4)}", docs=docs_a)
    )
    assert len(flow_a_pages) == 1
    flow_a = flow_a_pages[0]
    await workers.spawn_ready(pool, tag="timer-a", max_concurrency=8)
    assert await await_terminal(pool, schema, flow_a) == "succeeded"

    cursor_before = await store.read_cursor(pool, schema)
    assert cursor_before == 0, "the cursor must not move until the nightly pass"

    # THE FIRE, SCHEDULED +2s: arm it, and kill the worker BEFORE the
    # fire (mid-timer-wait). The schedule is a durable row: the fire
    # still happens, no earlier than scheduled.
    worker = await workers.spawn_ready(pool, tag="timer-b", max_concurrency=8)
    scheduled = await _schedule(pool, schema, 2.0)
    await asyncio.sleep(0.5)
    worker.kill9()

    await workers.spawn_ready(pool, tag="timer-c", max_concurrency=8)

    tick_row = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        tick_row = await pool.fetchrow(
            f'SELECT id, created_at FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "WHERE actor = 'docshelf-nightly-tick' ORDER BY created_at DESC LIMIT 1"
        )
        if tick_row is not None and tick_row["created_at"] >= scheduled:
            break
        await asyncio.sleep(0.5)
    assert tick_row is not None, "the schedule never fired"
    assert tick_row["created_at"] >= scheduled, (
        f"the timer fired EARLY: enqueued {tick_row['created_at']} < scheduled {scheduled}"
    )

    # The re-index flow runs; the cursor advances to the register's top seq.
    max_seq = await pool.fetchval(f'SELECT max(seq) FROM "{schema}".docshelf_documents')  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.

    reindex_flow = None
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        reindex_flow = await pool.fetchval(
            f"SELECT metadata->>'flow_id' FROM \"{schema}\".jobs "  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "WHERE step_key = 'advance_cursor' AND status = 'succeeded' LIMIT 1"
        )
        if reindex_flow is not None:
            break
        await asyncio.sleep(0.5)
    assert reindex_flow is not None, "the nightly re-index flow never completed"
    cursor_after = await store.read_cursor(pool, schema)
    assert cursor_after == int(max_seq), (
        f"the cursor advanced to {cursor_after}, the register's top seq is {max_seq}"
    )

    # ONLY THE INCREMENT: a second night with THREE new documents
    # re-processes exactly those three (the register's seq > the cursor).
    docs_b: list[DocPayload] = make_docs(3, prefix="night-b")
    flow_b_pages = await deliver_batch(
        pool, schema, DeliveryBatch(delivery_id=f"night-b-{secrets.token_hex(4)}", docs=docs_b)
    )
    assert len(flow_b_pages) == 1
    flow_b = flow_b_pages[0]
    assert await await_terminal(pool, schema, flow_b) == "succeeded"
    cursor_mid = await store.read_cursor(pool, schema)
    assert cursor_mid == cursor_after, "the cursor moved outside the nightly pass"

    await _schedule(pool, schema, 1.0)
    second_flow = None
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        rows = await pool.fetch(
            f'SELECT result::text AS res FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "WHERE step_key = 'advance_cursor' AND status = 'succeeded' ORDER BY id DESC LIMIT 2"
        )
        if len(rows) >= 2:
            second_flow = rows[0]
            break
        await asyncio.sleep(0.5)
    assert second_flow is not None, "the second nightly pass never ran"
    report = ReindexReport.model_validate(json.loads(second_flow["res"])["value"])
    assert report.processed == 3, (
        f"the second pass re-processed {report.processed} documents, not the increment"
    )
    assert report.new_cursor == cursor_mid + 3

    # THE CURSOR NEVER REWINDS (sampled across the drill's kills).
    cursor_final = await store.read_cursor(pool, schema)
    assert cursor_final >= max(cursor_before, cursor_mid)

    await _disarm(pool, schema)

    run_receipt(
        "timer-cursor-receipt",
        {
            "scheduled_fire_at": str(scheduled),
            "tick_enqueued_at": str(tick_row["created_at"]),
            "cursor_before": cursor_before,
            "cursor_after_first_pass": cursor_after,
            "second_pass_processed": report.processed,
            "second_pass_new_cursor": report.new_cursor,
            "cursor_final": cursor_final,
            "worker_killed_mid_timer_wait": True,
        },
    )
