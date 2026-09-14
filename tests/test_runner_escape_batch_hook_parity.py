"""The runner's escape paths skip the batch hook, exactly as production's
escapes jump past ``dispatch_one_job``'s single hook call.

Production applies :func:`apply_batch_terminal_outcome` at exactly one
site — inside ``dispatch_one_job``'s try, after ``consume_one_job``
returns cleanly (``src/taskq/worker/dispatch.py``). Every escape path
re-raises or handles past it: a cooperative cancel sets ``outcome
cancelled`` and re-raises; a pre-actor escape routes through
``_handle_generic_exception`` and returns. The batch then completes via
the next member's terminal hook or the stale-batch sweep — never
immediately on the escape itself.

The runner's two escape mirrors (``PayloadValidationError`` and the
cooperative-cancel absorb in ``run_until_drained``) once called the hook
with ``failed``/``cancelled`` — an observable divergence on the batched
surface: a batch whose last member ends through an escape completed
immediately in the harness and only via the sweep in production. These
pins hold the parity: the member's row reaches its terminal state, and
the batch row stays ``active`` — the completion is the sweep's or the
next member's, not the escape's.
"""

import asyncio
from datetime import UTC, datetime

from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_cooperative_cancel_escape_skips_the_batch_hook() -> None:
    """A cooperatively-cancelled batch member reaches terminal
    ``cancelled`` while its batch stays ``active`` — production's escape
    jumps past the batch hook, so the harness must not complete the
    batch on the escape either. Re-adding the hook call on the absorb
    path completes the batch here immediately and turns this pin red."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    batch_id = new_uuid()
    await backend.create_batch(batch_id, "default", 1, None, None, None)

    def cooperative(payload: object, ctx: object) -> object:
        ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the bare cooperative-exit style is the documented contract under pin.
        return {"unreachable": True}

    backend.register_stub("cooperative", cooperative)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="cooperative",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"batch_id": str(batch_id)},
    )
    cancel_event = asyncio.Event()
    cancel_event.set()
    backend.register_cancel_event(args.id, cancel_event)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "cancelled", f"the member must reach terminal cancelled; got {row.status}"
    batch = await backend.get_batch(batch_id)
    assert batch is not None
    assert batch.status == "active", (
        "the escape must not apply the batch hook: production's dispatch "
        "re-raises past its single hook call, so the batch completes via "
        f"the sweep or the next member — got {batch.status}"
    )


async def test_payload_validation_escape_skips_the_batch_hook() -> None:
    """A pre-actor payload-validation failure reaches terminal ``failed``
    while its batch stays ``active`` — the same production skip, on the
    runner's other escape mirror. Re-adding the hook call on the
    validation escape completes (or failure-counts) the batch here and
    turns this pin red."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    batch_id = new_uuid()
    await backend.create_batch(batch_id, "default", 1, None, None, None)

    def never_runs(payload: object, ctx: object) -> object:
        raise AssertionError("actor body must not run on validation failure")

    class StrictPayload(BaseModel):
        name: str

    backend.register_stub("strict_actor", never_runs, payload_type=StrictPayload)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="strict_actor",
        queue="default",
        payload={"wrong_field": "nope"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"batch_id": str(batch_id)},
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "PayloadValidationError"
    batch = await backend.get_batch(batch_id)
    assert batch is not None
    assert batch.status == "active", (
        "the escape must not apply the batch hook: production's dispatch "
        "handles the escape past its single hook call, so the batch "
        f"completes via the sweep or the next member — got {batch.status}"
    )
