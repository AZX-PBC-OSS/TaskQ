"""The runner's escape paths apply the batch hook, mirroring production's
escape handlers — a batch whose last member ends through an escape
completes immediately, GoodJob-aligned.

Production's ``dispatch_one_job`` applies
:func:`apply_batch_terminal_outcome` on the clean return from
``consume_one_job`` and on every escape, best-effort before the
re-raise/return: the CancelledError handler calls the hook with
``cancelled`` before re-raising, and the generic-exception handler calls
it with ``_handle_generic_exception``'s terminal outcome (a handler
whose terminal write infra-failed leaves the row RUNNING and stays
hook-silent — the sweep is the recovery, and no batch counter may budge
on a non-terminal write). GoodJob's per-job finish hook runs the
batch-completion check for ANY terminal member, discarded included
(``vendor/good_job/app/models/good_job/batch_record.rb``:
``_continue_discard_or_finish`` fires ``on_discard`` and still sets
``jobs_finished_at`` and fires ``on_finish``), so a batch whose last
member ends through an escape completes immediately — never
sweep-deferred on a normal flow.

The runner's two escape mirrors (``PayloadValidationError`` and the
cooperative-cancel absorb in ``run_until_drained``) hold the same
contract: the escape sets the terminal outcome and falls through to the
shared hook call. These pins hold it — the member's row reaches its
terminal state AND the batch row reaches ``complete``; re-skipping the
hook on either mirror strands the batch ``active`` until the
stale-batch sweep and turns these pins red.
"""

import asyncio
from datetime import UTC, datetime

from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_cooperative_cancel_escape_completes_the_batch() -> None:
    """A cooperatively-cancelled batch member reaches terminal
    ``cancelled`` and its batch completes on the escape itself — the
    runner's absorb mirror falls through to the shared batch hook with
    ``cancelled``, exactly as production's CancelledError handler
    applies the hook best-effort before its re-raise (GoodJob completes
    on any terminal member, discarded included). Re-skipping the hook
    call on the absorb path strands the batch ``active`` until the
    stale-batch sweep and turns this pin red."""
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
    assert batch.status == "complete", (
        "the escape must apply the batch hook: production's dispatch "
        "CancelledError handler applies it best-effort before the "
        "re-raise, so a batch whose last member ends through the "
        f"escape completes immediately — got {batch.status}"
    )


async def test_payload_validation_escape_completes_the_batch() -> None:
    """A pre-actor payload-validation failure reaches terminal ``failed``
    and its batch completes on the escape itself — the runner's
    validation mirror falls through to the shared batch hook with
    ``failed``, mirroring production's generic-exception escape, where
    the handler's terminal outcome reaches the hook. Re-skipping the
    hook call on the validation escape strands the batch ``active``
    until the stale-batch sweep and turns this pin red."""
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
    assert batch.status == "complete", (
        "the escape must apply the batch hook: production's dispatch "
        "routes the escape through the generic handler and applies the "
        "hook with its terminal outcome, so a batch whose last member "
        f"ends through the escape completes immediately — got {batch.status}"
    )
