"""Batch fan-out e2e — ``import_contacts_csv`` fans out 5 chunk jobs as one batch.

Design spec scenario row (docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
2500 rows / 500-chunk → parent fans out 5 chunk jobs →
``wait_for_batch(conn, batch_id, schema=...)`` on an ``e2e_pg_pool``
connection → all complete; 5 chunk effects, rows partitioned without overlap.

Verified against the library, not guessed:

- ``wait_for_batch`` (src/taskq/batch.py): with ``snooze_via_exception=False``
  it blocks (asyncio.sleep loop) until every child is terminal, then returns
  ``BatchCompletionStatus``; ``snooze_interval`` is clamped to >= 1 s. Batch
  children are matched by the GIN-indexed ``metadata @> {"batch_id": ...}``
  containment — the ``batch_id`` metadata key is library-injected by
  ``enqueue_batch`` (callers must not set it themselves).
- The parent records the explicit ``batch_id`` on its ``dispatched`` effect
  (tests/e2e/actors.py), which is how the test correlates.
- Range ground truth: the ``chunk_done`` effect detail carries ``chunk_id``
  and ``rows_processed`` — NOT the ``[start_row, end_row)`` range. The range
  lives in the chunk job's ``jobs.payload`` column, so the partition
  assertion joins effects to payloads by ``job_id``.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from taskq import wait_for_batch

from ._assertions import fetch_effects
from .actors import ImportContactsChunkPayload, ImportContactsPayload, import_contacts_csv

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.batch import BatchCompletionStatus

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_ROWS = 2500
_CHUNK_SIZE = 500
_CHUNKS = 5


async def _run_import_to_completion(
    e2e_client: TaskQ,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> tuple[UUID, BatchCompletionStatus]:
    """Enqueue a 2500-row import, wait for the parent, then for the whole batch.

    Returns the parent's explicit ``batch_id`` (from the ``dispatched``
    effect) and the final ``BatchCompletionStatus``.
    """
    handle = await e2e_client.enqueue(
        import_contacts_csv,
        ImportContactsPayload(
            run_id=run_id,
            upload_id=f"u-{run_id[:8]}",
            rows=_ROWS,
            chunk_size=_CHUNK_SIZE,
        ),
    )
    await handle.wait(timeout=90)

    dispatched = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="dispatched")
    assert len(dispatched) == 1
    detail = json.loads(dispatched[0]["detail"])
    assert isinstance(detail, dict)
    assert detail["chunks"] == _CHUNKS
    batch_id_raw = detail["batch_id"]
    assert isinstance(batch_id_raw, str)
    batch_id = UUID(batch_id_raw)

    # snooze_via_exception=False: outside an actor there is no consumer to
    # translate Snooze into a reschedule, so the helper sleep-polls instead.
    async with e2e_pg_pool.acquire() as conn, asyncio.timeout(60):
        status = await wait_for_batch(
            conn,
            batch_id,
            schema=e2e_schema.schema_name,
            snooze_interval=timedelta(seconds=1),
            snooze_via_exception=False,
        )
    return batch_id, status


async def test_csv_fanout_all_chunks_complete(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """All 5 chunks succeed and their payload ranges partition ``[0, 2500)``."""
    batch_id, status = await _run_import_to_completion(e2e_client, e2e_pg_pool, e2e_schema, run_id)

    assert status.is_complete
    assert status.total == _CHUNKS
    assert status.succeeded == _CHUNKS
    assert status.failed == 0
    assert status.cancelled == 0

    chunk_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="chunk_done"
    )
    assert len(chunk_effects) == _CHUNKS

    # Chunk payloads by batch containment — the authoritative source of the
    # [start_row, end_row) ranges (see module docstring).
    job_rows = await e2e_pg_pool.fetch(
        f"""
        SELECT id, payload
        FROM "{e2e_schema.schema_name}".jobs
        WHERE metadata @> $1::jsonb
        """,
        json.dumps({"batch_id": str(batch_id)}),
    )
    payloads_by_id = {
        row["id"]: ImportContactsChunkPayload.model_validate(json.loads(row["payload"]))
        for row in job_rows
    }
    assert len(payloads_by_id) == _CHUNKS

    ranges: list[tuple[int, int]] = []
    for effect in chunk_effects:
        payload = payloads_by_id[effect["job_id"]]
        ranges.append((payload.start_row, payload.end_row))
    ranges.sort()

    # Sorted ranges tile [0, 2500) exactly — no overlap, no gaps.
    assert ranges == [(0, 500), (500, 1000), (1000, 1500), (1500, 2000), (2000, 2500)]


async def test_batch_rows_accounting(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Chunk ``rows_processed`` in the effects sums exactly to the 2500 input rows."""
    _, status = await _run_import_to_completion(e2e_client, e2e_pg_pool, e2e_schema, run_id)
    assert status.succeeded == _CHUNKS

    chunk_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="chunk_done"
    )
    assert len(chunk_effects) == _CHUNKS

    rows_processed = 0
    for effect in chunk_effects:
        detail = json.loads(effect["detail"])
        assert isinstance(detail, dict)
        count = detail["rows_processed"]
        assert isinstance(count, int)
        rows_processed += count
    assert rows_processed == _ROWS
