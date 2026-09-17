"""Red-team probe (Postgres): does the batch enqueue path surface
``JobHandle.deduplicated_onto_terminal`` correctly, matching the
single-enqueue and in-memory-backend behaviour?

``_log_enqueue_dedup`` and the handle computation are exercised directly
by existing pins (``test_silent_failure_guards.py`` for the PG log line,
``test_rt_dedup_terminal_hits.py`` for the in-memory handle field), but
neither drives an idempotency-key collision through
``JobsClient.enqueue_batch`` against a real Postgres backend. This closes
that gap: a multi-item batch where one item's idempotency key collides
with an already-terminal job must produce a ``JobHandle`` whose
``deduplicated_onto_terminal`` is True, with no second read.
"""

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.batch import EnqueueItem
from taskq.client._jobs import JobsClient
from taskq.testing.fixtures import ModulePgSchema, _open_pg_backend_on_schema
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration


class _Payload(BaseModel):
    x: int = 0


@actor(name="rt_batch_terminal_dedup_actor")
async def _rt_batch_actor(payload: _Payload) -> None:
    pass


async def test_batch_enqueue_surfaces_terminal_dedup_on_handle(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    stack, _deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        client = JobsClient(backend=backend)

        # The stored row belongs to the batch's own actor: a same-actor hit
        # dedups, a cross-actor one is refused.
        first = await backend.enqueue(
            make_enqueue_args(
                actor="rt_batch_terminal_dedup_actor", idempotency_key="pg-batch-terminal-1"
            )
        )
        await clean_pg_conn.execute(
            f'UPDATE "{module_pg_schema.schema_name}".jobs '  # noqa: S608  # Why: schema name comes from the test fixture, not user input.
            "SET status = 'failed', finished_at = now() WHERE id = $1::uuid",
            first.id,
        )

        items = [
            EnqueueItem(actor_ref=_rt_batch_actor, payload=_Payload(x=1)),
            EnqueueItem(
                actor_ref=_rt_batch_actor,
                payload=_Payload(x=2),
                idempotency_key="pg-batch-terminal-1",
            ),
        ]
        batch_handle = await client.enqueue_batch(items)
        fresh, collided = batch_handle.job_handles

        assert fresh.was_existing is False
        assert fresh.deduplicated_onto_terminal is False

        assert collided.job_id == first.id, "precondition: the terminal row still dedupes"
        assert collided.was_existing is True
        assert collided.row.status == "failed"
        assert collided.deduplicated_onto_terminal is True, (
            "batch enqueue against Postgres did not surface the terminal dedup "
            "on the returned JobHandle"
        )
    finally:
        await stack.aclose()
