"""Pin: register_stub()'s default payload_type must not let a payload
that violates the actor's REAL declared model pass a green in-memory
test.

``InMemoryBackend.register_stub`` (taskq/testing/_runner.py:270-333)
accepts an optional ``payload_type`` kwarg; when omitted it falls back
to ``PassthroughPayload`` (``model_config = {"extra": "allow"}`` --
taskq/testing/_runner.py:164-177), which validates ANY dict shape.
``docs/guides/testing.md``'s own "Registering actor stubs" example
(the ``double_value`` actor) calls ``register_stub`` without
``payload_type``, and the same doc names this explicitly: "Tests that
care about typed payloads pass an explicit payload_type... tests that
don't get this permissive default."

The consequence: adding a required field to a payload model is a
backward-incompatible payload-shape change (an old enqueued row, or
any enqueue call site the field addition missed, no longer validates)
that a real worker MUST catch and fail loudly
(``taskq/worker/_consumer.py::consume_one_job`` calls
``validate_actor_payload(payload_type, job.payload, job.actor)`` using
the actor's declared model on the production path). But
``run_until_drained`` validates against ``actor_cfg.payload_type``
(taskq/testing/_runner.py:772), which is ``PassthroughPayload`` unless
the test author remembered to pass the real model explicitly to every
``register_stub`` call. A test written before the field was added
keeps passing after the field is added, silently, forever -- there is
no drift detector. This is the single most damaging kind of testing
gap TaskQ's own "InMemoryBackend simulates the full ... cycle" and
"faithful" backend-parity claim can have: it converts a green suite
into false confidence about exactly the kind of change (a payload
schema migration) adopters make constantly.

Backend parity is a claimed contract (InMemoryBackend "simulates the
full enqueue -> dispatch -> execute -> terminal-write cycle" --
docs/guides/testing.md, "InMemoryBackend" section). A payload accepted
by one backend and rejected by the actor's own declared model,
inside THE SAME PROCESS, with no real Postgres required to prove it,
is a direct violation of that claim -- this test needs no
testcontainers because ``RealPayload.model_validate()`` and
``consume_one_job``'s ``validate_actor_payload`` call are the same
Pydantic mechanism a real Postgres-backed worker uses; the divergence
is entirely inside ``InMemoryBackend``'s own default, not in Postgres
vs. in-memory storage semantics.

No vendor precedent search applies here -- Celery/Sidekiq/Oban/River
don't have a Pydantic-payload-model concept to validate against in the
first place (Celery tasks take positional/keyword args, not a typed
model; Sidekiq jobs take JSON-serializable args; Oban args are an Ecto
embedded schema validated by the worker's own changeset, which
Oban.Testing does not bypass -- vendor/oban/lib/oban/testing.ex makes
no payload-shape claim at all). This is argued from first principles:
a library that advertises "end-to-end type safety" and "Payload...
validated at decoration time" (docs/index.md) as a *selling point*
cannot then default its own test harness to skip that exact
validation without callers opting in per-call.
"""

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ValidationError

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _RealPayload(BaseModel):
    """Stands in for a payload model that gained a required field after
    the original test was written -- the ordinary, expected shape of a
    payload-schema migration."""

    value: int
    required_new_field: str


@pytest.mark.asyncio
async def test_register_stub_default_payload_type_accepts_what_real_model_rejects() -> None:
    """Proves the fidelity gap directly: the SAME dict that
    ``_RealPayload`` rejects is accepted by ``run_until_drained`` when
    ``register_stub`` is called without an explicit ``payload_type`` --
    reproducing exactly what docs/guides/testing.md's own example does.
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

    # Exactly docs/guides/testing.md's own pattern: register_stub with
    # no payload_type kwarg.
    backend.register_stub("real_actor", stub)

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

    # Desired behaviour: an in-memory test run with the actor's REAL
    # payload model contract violated must fail the same way a real
    # worker would -- not silently succeed. Pin the failure: today this
    # assertion is false (row.status == "succeeded", calls == [the
    # incomplete dict]) because register_stub's default payload_type is
    # PassthroughPayload, not the actor's declared model.
    assert row.status != "succeeded", (
        "InMemoryBackend accepted a payload that the actor's own "
        "declared Pydantic model rejects (ValidationError), because "
        "register_stub() without an explicit payload_type= falls back "
        "to PassthroughPayload (extra='allow', validates any shape). "
        "A payload-schema migration (adding a required field) can pass "
        "every in-memory test and only fail in production."
    )
