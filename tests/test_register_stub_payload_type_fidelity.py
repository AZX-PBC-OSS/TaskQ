"""Pin: register_stub()'s omitted-payload_type path must not let a payload
that violates the actor's REAL declared model pass a green in-memory
test.

Original defect (fixed): ``InMemoryBackend.register_stub`` accepted an
optional ``payload_type`` kwarg and, when omitted, fell back to
``PassthroughPayload`` (``model_config = {"extra": "allow"}``), which
validates ANY dict shape. ``docs/guides/testing.md``'s own "Registering
actor stubs" example called ``register_stub`` without ``payload_type``.
The consequence: adding a required field to a payload model is a
backward-incompatible payload-shape change (an old enqueued row, or any
enqueue call site the field addition missed, no longer validates) that a
real worker MUST catch and fail loudly
(``taskq/worker/_consumer.py::consume_one_job`` calls
``validate_actor_payload(payload_type, job.payload, job.actor)`` using
the actor's declared model on the production path). But
``run_until_drained`` validated against ``actor_cfg.payload_type``, which
was ``PassthroughPayload`` unless the test author remembered to pass the
real model explicitly to every ``register_stub`` call. A test written
before the field was added kept passing after the field was added,
silently, forever -- there was no drift detector. This is the single
most damaging kind of testing gap TaskQ's own "InMemoryBackend simulates
the full ... cycle" and "faithful" backend-parity claim can have: it
converts a green suite into false confidence about exactly the kind of
change (a payload schema migration) adopters make constantly.

The fix pinned here: ``register_stub`` auto-resolves the actor's declared
payload model when ``payload_type`` is omitted, so the runner knows the
actor's contract without per-call repetition. The resolution source is
the :class:`~taskq.actor.ActorRef` itself — pass the ref
(``register_stub(my_actor, ...)``) instead of a bare name and the
declared model is looked up from it. A bare name leaves the runner
unable to see any declared model; that path still falls back to
``PassthroughPayload`` for backward compatibility, but only with a loud
:class:`~taskq.testing._runner.StubPayloadTypeWarning` (pinned below),
and the permissive default remains available as an explicit, warning-free
escape hatch (``payload_type=PassthroughPayload``, also pinned below).

Backend parity is a claimed contract (InMemoryBackend "simulates the
full enqueue -> dispatch -> execute -> terminal-write cycle" --
docs/guides/testing.md, "InMemoryBackend" section). A payload accepted
by one backend and rejected by the actor's own declared model,
inside THE SAME PROCESS, with no real Postgres required to prove it,
is a direct violation of that claim -- this test needs no
testcontainers because ``RealPayload.model_validate()`` and
``consume_one_job``'s ``validate_actor_payload`` call are the same
Pydantic mechanism a real Postgres-backed worker uses; the divergence
was entirely inside ``InMemoryBackend``'s own default, not in Postgres
vs. in-memory storage semantics.

No cross-library precedent applies here -- the common queue libraries
carry untyped or worker-validated payloads, so there is no typed-payload
testing surface to compare against. This is argued from first principles:
a library that advertises "end-to-end type safety" and "Payload...
validated at decoration time" (docs/index.md) as a *selling point*
cannot then default its own test harness to skip that exact
validation without callers opting in per-call.
"""

import warnings
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ValidationError

from taskq import actor
from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing._runner import (
    PassthroughPayload,  # pyright: ignore[reportPrivateUsage]  # Why: the pinned escape-hatch contract names PassthroughPayload explicitly; importing the defining module keeps the pin at the boundary it pins.
    StubPayloadTypeWarning,  # pyright: ignore[reportPrivateUsage]  # Why: the warning class is the pinned loud signal of register_stub's bare-name fallback; same whitebox rationale as above.
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _RealPayload(BaseModel):
    """Stands in for a payload model that gained a required field after
    the original test was written -- the ordinary, expected shape of a
    payload-schema migration."""

    value: int
    required_new_field: str


class _RealResult(BaseModel):
    doubled: int


@actor
async def real_actor(payload: _RealPayload) -> _RealResult:
    return _RealResult(doubled=payload.value * 2)


@pytest.mark.asyncio
async def test_register_stub_default_payload_type_accepts_what_real_model_rejects() -> None:
    """Proves the fidelity gap is closed: the SAME dict that
    ``_RealPayload`` rejects must not run to ``succeeded`` when
    ``register_stub`` is called without an explicit ``payload_type`` --
    the runner resolves the actor's declared model from the passed
    ActorRef and validates against it, exactly as a real worker would.
    """
    incomplete_payload = {"value": 21}  # missing required_new_field

    # Ground truth: the actor's real, declared payload model rejects
    # this payload. Any real worker (or a stub registered WITH
    # payload_type=_RealPayload) would fail this job the same way.
    with pytest.raises(ValidationError):
        _RealPayload.model_validate(incomplete_payload)

    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    calls: list[dict[str, object]] = []

    def stub(payload: dict[str, object], ctx: object) -> dict[str, object]:
        calls.append(payload)
        return {"doubled": 42}

    # The omitted-payload_type path, with the actor's declaration made
    # visible to the runner by passing the ActorRef itself. No
    # StubPayloadTypeWarning may fire: the model IS resolvable here.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        backend.register_stub(real_actor, stub)
    assert not [w for w in caught if issubclass(w.category, StubPayloadTypeWarning)]

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="real_actor",
            queue="default",
            payload=incomplete_payload,
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )

    await backend.run_until_drained()

    row = await backend.get(job_id)
    assert row is not None

    # The pinned contract: an in-memory test run with the actor's REAL
    # payload model contract violated must fail the same way a real
    # worker would -- not silently succeed. Before the fix this was
    # row.status == "succeeded" with calls == [the incomplete dict],
    # because register_stub's default payload_type was
    # PassthroughPayload, not the actor's declared model.
    assert row.status != "succeeded", (
        "InMemoryBackend accepted a payload that the actor's own "
        "declared Pydantic model rejects (ValidationError): the stub was "
        "registered via the ActorRef with no explicit payload_type=, so "
        "the runner must have validated against the resolved declared "
        "model. A payload-schema migration (adding a required field) "
        "must not pass the in-memory suite and fail only in production."
    )
    # Stronger halves of the same contract: the job failed AS a payload
    # validation failure (not some unrelated error), and the stub body
    # never ran — validation happens before invocation, as on a worker.
    assert row.error_class == "PayloadValidationError"
    assert calls == []


@pytest.mark.asyncio
async def test_register_stub_bare_name_without_payload_type_warns_loudly() -> None:
    """The unresolvable fallback: a bare actor name carries no declared
    model, so the permissive default survives (backward compatibility —
    the suite's existing bare-name stubs keep working) but ONLY with a
    loud StubPayloadTypeWarning naming the actor and the consequences.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    with pytest.warns(StubPayloadTypeWarning, match="bare_actor") as record:
        backend.register_stub("bare_actor", lambda p, ctx: {"ok": True})
    assert len(record) == 1

    # The permissive fallback still runs the job — the warning is the
    # signal, not a behaviour break.
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="bare_actor",
            queue="default",
            payload={"anything": "goes"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    await backend.run_until_drained()
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"


@pytest.mark.asyncio
async def test_register_stub_explicit_passthrough_is_the_quiet_escape_hatch() -> None:
    """The escape hatch is explicit: an operator who MEANS passthrough
    says so. ``payload_type=PassthroughPayload`` keeps the permissive
    behaviour with no warning — silence is reserved for deliberate
    choices (this, or a resolved ActorRef), never for an omission."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        backend.register_stub(
            "bare_actor", lambda p, ctx: {"ok": True}, payload_type=PassthroughPayload
        )
    assert not [w for w in caught if issubclass(w.category, StubPayloadTypeWarning)]

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="bare_actor",
            queue="default",
            payload={"anything": "goes"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    await backend.run_until_drained()
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
