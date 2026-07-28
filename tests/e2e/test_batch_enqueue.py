"""Batch enqueue from client e2e — enqueue_batch with explicit batch_id.

Design spec scenario row (docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
client-side ``enqueue_batch`` with an explicit ``batch_id`` UUID; all jobs
share that UUID in ``metadata.batch_id``; ``handle.wait()`` on every child
succeeds; effects rows match the enqueued set 1:1.

Ground truth: ``e2e_effects`` (one ``send`` per job) and ``jobs.metadata``
(the ``batch_id`` key written by the library's batch-INSERT path).

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq import EnqueueItem

from ._assertions import fetch_effects
from .actors import WelcomeEmailPayload, send_welcome_email

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_BATCH_SIZE = 10


async def test_enqueue_batch_with_explicit_batch_id(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """``enqueue_batch`` with an explicit ``batch_id`` → all children succeed
    and carry the same ``metadata.batch_id``.

    10 ``EnqueueItem`` instances are enqueued in a single batched INSERT with
    a caller-supplied ``batch_id``.  ``asyncio.gather`` over every
    ``handle.wait()`` proves all 10 reached ``succeeded``; 10 ``send`` effects
    whose ``job_id`` set matches the enqueued handles proves exactly-once
    execution; and a direct SQL read of ``metadata->>'batch_id'`` for every
    job row confirms the library persisted the explicit UUID.
    """
    batch_id = uuid4()

    items = [
        EnqueueItem(
            actor_ref=send_welcome_email,
            payload=WelcomeEmailPayload(
                run_id=run_id,
                user_id=f"u-{i:02d}",
                email=f"u-{i:02d}@example.com",
            ),
        )
        for i in range(_BATCH_SIZE)
    ]

    batch = await e2e_client.enqueue_batch(items, batch_id=batch_id)
    assert batch.size == _BATCH_SIZE
    assert len(batch.job_handles) == _BATCH_SIZE

    results = await asyncio.gather(*(handle.wait(timeout=60) for handle in batch.job_handles))
    assert len(results) == _BATCH_SIZE
    assert all(r.sent for r in results)

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="send")
    assert len(rows) == _BATCH_SIZE
    effect_job_ids = {row["job_id"] for row in rows}
    handle_job_ids = {handle.job_id for handle in batch.job_handles}
    assert effect_job_ids == handle_job_ids

    job_rows = await e2e_pg_pool.fetch(
        f"""
        SELECT id, metadata->>'batch_id' AS batch_id
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = ANY($1::uuid[])
        """,
        [handle.job_id for handle in batch.job_handles],
    )
    assert len(job_rows) == _BATCH_SIZE
    for row in job_rows:
        assert row["batch_id"] == str(batch_id)
