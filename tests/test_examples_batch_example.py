"""The batch example must teach the docs' finalizer law, not fight it.

``docs/guides/jobs-clients.md`` ("Finalizer is NOT stamped with
``batch_id``") is explicit: the finalizer job's metadata does not include
``batch_id`` — if it were, ``wait_for_batch`` would count the finalizer as
its own child and it would wait for itself: a deadlock. On the in-actor
path (``ctx.jobs.enqueue_batch`` + a plain ``ctx.jobs.enqueue``) the
batch row's ``finalizer_job_id`` is NULL, so nothing excludes the stamped
finalizer from the count — the deadlock is real, not theoretical (a live
repro against Postgres shows ``wait_for_batch`` Snoozing forever with all
children terminal).

This pin keeps the shipped example on the docs' side of that law: the
``batch_finalizer`` job that ``batch_counter`` enqueues must not carry
``batch_id`` in its metadata.
"""

from typing import Any
from unittest.mock import MagicMock

from examples.actors.batch import BatchCounterPayload, batch_counter


class _CapturingJobs:
    """The slice of JobsClient batch_counter uses, recording every call."""

    def __init__(self) -> None:
        self.batch_calls: list[dict[str, Any]] = []
        self.enqueue_calls: list[dict[str, Any]] = []

    async def enqueue_batch(self, items: Any, **kwargs: Any) -> None:
        self.batch_calls.append({"items": list(items), **kwargs})

    async def enqueue(self, actor_ref: Any, payload: Any, **kwargs: Any) -> None:
        self.enqueue_calls.append({"actor_ref": actor_ref, "payload": payload, **kwargs})


def test_batch_example_finalizer_is_not_stamped_with_batch_id() -> None:
    """The finalizer job's metadata carries no ``batch_id`` — the docs'
    deadlock law (jobs-clients.md, "Finalizer is NOT stamped with
    batch_id")."""
    jobs = _CapturingJobs()
    ctx = MagicMock()
    ctx.jobs = jobs

    import asyncio

    asyncio.run(batch_counter.fn(BatchCounterPayload(n=3, steps=1), ctx))

    assert len(jobs.batch_calls) == 1, "batch_counter must enqueue one batch"
    assert jobs.enqueue_calls, "batch_counter must enqueue the finalizer job"
    finalizer_call = jobs.enqueue_calls[-1]
    metadata = finalizer_call.get("metadata") or {}
    assert "batch_id" not in metadata, (
        "examples/actors/batch.py stamps metadata.batch_id on the finalizer "
        "job - but batch_id is a reserved key: build_enqueue_args strips any "
        "caller-supplied value, so the example teaches a metadata key that "
        "silently never lands on the row (and the docs' deadlock law says it "
        "must not: wait_for_batch counts every job whose metadata carries "
        "batch_id, and the finalizer must not be one of them)"
    )
