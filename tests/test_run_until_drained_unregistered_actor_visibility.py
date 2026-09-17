"""Pin: run_until_drained must not silently return "drained" while an
enqueued job for an unregistered actor sits pending forever.

Observed behaviour (fixed): enqueuing a job for an actor with no
``register_stub``/``register_actor_config`` call caused
``dispatch_batch`` to treat the actor as having zero capacity (the
candidate-selection loop iterates ``self._actor_configs_meta`` only —
see ``taskq/testing/_dispatch.py`` around the "zero registered actors
means zero capacity rows means zero candidates" comment). Because no
job was ever dispatchable for that actor, ``run_until_drained`` observed
"nothing dispatched, nothing scheduled" and returned normally, treating
"can never run" identically to "fully executed". The job was left
`pending` forever with no exception, no warning, and no log line
distinguishing it from a job that legitimately finished draining. A
caller that then does ``await handle.wait()`` (the pattern
docs/guides/jobs-clients.md teaches for reading an enqueued job's
result) hangs indefinitely, because ``wait()`` with no ``timeout=``
polls without a deadline (``taskq/client/_handle.py::wait``,
``deadline is None`` branch never raises TimeoutError).

The pinned contract: ``run_until_drained`` raises ``RuntimeError`` —
the same "no stub registered for actor" contract it already had for a
*dispatched* job with a missing stub (``taskq/testing/_runner.py``,
``run_until_drained``) — when the drain would otherwise end with
non-terminal work for an actor nothing registered. An actor that can
never be dispatched because it has no registered stub/config is a
strictly worse failure mode than one that dispatches and then
immediately errors, and previously produced no signal at all. The raise
DETECTS, it does not mutate: the stranded row stays ``pending`` (nothing
ran it, and a fabricated terminal write would be its own lie — and would
corrupt retry bookkeeping if the actor is registered later).

Vendor precedent for "how does a test harness handle a job whose
handler isn't wired up for execution":

- A fake test mode that never runs job bodies at all: pushing onto the
  in-memory queue is the entire contract, and nothing pretends a
  push means completion (the push just appends to the
  in-memory array; there is no drain-and-silently-skip step).
- An inline test mode that calls the real job class synchronously at
  enqueue time has no notion of "a job whose class isn't registered
  drains successfully"; if the class constant doesn't resolve, the
  constant lookup raises immediately.
- A test harness that requires the caller to supply the concrete worker
  to invoke has no separate "drain" step that can silently skip an
  unregistered job type.

None of those shapes have a state where "enqueued but nothing
will ever run it" is indistinguishable from "ran to completion".

This test intentionally exercises the failure via the *documented*
adopter-facing path from docs/guides/testing.md (bare
``InMemoryBackend()`` + ``JobsClient`` + ``client.enqueue`` +
``run_until_drained``, no fixture, no ``register_actor_config``) so it
reflects what a first-time adopter's test looks like, not an
artificially constructed edge case.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from taskq import actor
from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.client import JobsClient
from taskq.exceptions import ReservationUnavailable
from taskq.testing._runner import (
    PassthroughPayload,  # pyright: ignore[reportPrivateUsage]  # Why: the explicit passthrough escape hatch keeps the denial stub focused on the starvation guard, not on payload fidelity — the whitebox import names the defining module.
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int


class _Result(BaseModel):
    doubled: int


@actor
async def _unregistered_actor(payload: _Payload) -> _Result:
    return _Result(doubled=payload.value * 2)


@pytest.mark.asyncio
async def test_run_until_drained_does_not_silently_strand_unregistered_actor_jobs() -> None:
    """A job for an actor with no stub/config must not look "drained".

    Before the fix, run_until_drained() returned normally and the job's
    status stayed "pending" -- indistinguishable from "everything that
    was enqueued actually ran". The pinned contract is the raise the
    finding chose: the same RuntimeError the runner already raised for a
    *dispatched* job with a missing stub, now also fired when the drain
    would end beside work nothing can ever dispatch.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    # Deliberately do NOT call backend.register_stub(...) or
    # backend.register_actor_config(...) for _unregistered_actor -- this
    # is the state a first-time adopter following testing.md's own
    # "Registering actor stubs" section reaches for any actor they
    # haven't stubbed yet (e.g. while incrementally testing a chain of
    # actors and only stubbing the leaf).
    handle = await client.enqueue(_unregistered_actor, _Payload(value=21))

    with pytest.raises(RuntimeError, match=r"no stub registered for actor"):
        await backend.run_until_drained()

    row = await backend.get(handle.job_id)
    assert row is not None

    # The raise detects, it does not mutate: nothing ran this job, so no
    # terminal state may be fabricated for it. A fabricated failure row
    # would be its own harness lie — and would corrupt retry bookkeeping
    # if the actor is registered and the job retried later.
    assert row.status == "pending", (
        "the undrainable-work raise must report the stranded job, not "
        "rewrite it: nothing executed it, so nothing may terminal-write it"
    )


def _register_forever_denied_stub(backend: InMemoryBackend, actor_name: str) -> None:
    """Register a stub whose every admission attempt is denied.

    The denial reschedules the job at the limiter's own Retry-After
    promise; the drain honors the promise once (advances the FakeClock,
    re-claims), and a second denial at/after that point marks the job
    starved — the denial-starvation guard's exit. The actor IS
    registered, so the unregistered-actor raise must never fire for it.
    """

    def deny(payload: dict[str, object], ctx: object) -> None:
        raise ReservationUnavailable("pool", timedelta(seconds=30), source="reservation")

    backend.register_stub(actor_name, deny, payload_type=PassthroughPayload)


@pytest.mark.asyncio
async def test_denied_but_recoverable_job_does_not_trip_the_unregistered_raise() -> None:
    """Composition with the denial-starvation guard: a job whose actor IS
    registered but whose admission only ever answers "no" ends the drain
    via the starved path and must return normally — denied is
    recoverable-by-definition (the limiter promised a Retry-After), never
    "nothing registered, will never run". Only the never-registered case
    may raise.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    _register_forever_denied_stub(backend, "denied_actor")

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="denied_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )

    # No RuntimeError: the drain ends via the denial-starvation exit.
    await backend.run_until_drained()

    row = await backend.get(job_id)
    assert row is not None
    # Denied work is never terminalised and never marked as ran — it is
    # rescheduled, with the contention visible on the denial counter.
    assert row.status in ("scheduled", "pending")
    assert row.rate_limit_blocked_count >= 1


@pytest.mark.asyncio
async def test_mixed_drain_raises_only_for_the_never_registered_actor() -> None:
    """The sharp composition pin: one drain holding BOTH a denied
    registered job and a never-registered job must raise — and the raise
    must name the never-registered actor only. A denied job sharing the
    drain must not be swept into the failure (a false positive would
    criminalise ordinary rate-limit saturation)."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    _register_forever_denied_stub(backend, "denied_actor")

    denied_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=denied_id,
            actor="denied_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    ghost_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=ghost_id,
            actor="ghost_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )

    with pytest.raises(RuntimeError, match=r"no stub registered for actor") as exc_info:
        await backend.run_until_drained()

    assert "ghost_actor" in str(exc_info.value)
    assert "denied_actor" not in str(exc_info.value), (
        "the denied registered job was swept into the never-registered "
        "raise — the raise must target only actors with no registry entry"
    )

    # The raise fired before any terminal write for either job: the ghost
    # stays pending, the denied job stays rescheduled.
    denied_row = await backend.get(denied_id)
    ghost_row = await backend.get(ghost_id)
    assert denied_row is not None and denied_row.status in ("scheduled", "pending")
    assert ghost_row is not None and ghost_row.status == "pending"
