"""Pin: run_until_drained must not silently return "drained" while an
enqueued job for an unregistered actor sits pending forever.

Observed behaviour: enqueuing a job for an actor with no
``register_stub``/``register_actor_config`` call causes
``dispatch_batch`` to treat the actor as having zero capacity (the
candidate-selection loop iterates ``self._actor_configs_meta`` only —
see ``taskq/testing/_dispatch.py`` around the "zero registered actors
means zero capacity rows means zero candidates" comment). Because no
job is ever dispatchable for that actor, ``run_until_drained`` observes
"nothing dispatched, nothing scheduled" and returns normally, treating
"can never run" identically to "fully executed". The job is left
`pending` forever with no exception, no warning, and no log line
distinguishing it from a job that legitimately finished draining. A
caller that then does ``await handle.wait()`` (the pattern
``docs/guides/jobs-clients.md`` teaches for reading an enqueued job's
result) hangs indefinitely, because ``wait()`` with no ``timeout=``
polls without a deadline (``taskq/client/_handle.py::wait``,
``deadline is None`` branch never raises TimeoutError).

Desired behaviour, grounded in vendor precedent for "how does a test
harness handle a job whose handler isn't wired up for execution":

- Sidekiq's ``fake!`` mode never runs job bodies at all -- pushing onto
  ``Sidekiq::Queues`` is the entire contract, and nothing pretends a
  push means completion (vendor/sidekiq/lib/sidekiq/test_api.rb:84-90,
  ``atomic_push`` under ``Sidekiq::Testing.fake?`` just appends to the
  in-memory array; there is no drain-and-silently-skip step).
- Sidekiq's ``inline!`` mode calls the real job class synchronously at
  enqueue time (vendor/sidekiq/lib/sidekiq/test_api.rb:91-97,
  ``klass.process_job(job_hash)``) -- there is no notion of "a job
  whose class isn't registered drains successfully"; if the constant
  doesn't resolve, ``Object.const_get`` raises immediately.
- River's rivertest.Worker[T] (vendor/river/rivertest/rivertest.go and
  worker_test.go) requires the caller supply the concrete worker to
  invoke; there is no separate "drain" step that can silently skip an
  unregistered job type.

None of the three vendors have a state where "enqueued but nothing
will ever run it" is indistinguishable from "ran to completion". TaskQ
should raise (matching its own documented contract for a *dispatched*
job with no stub -- see ``run_until_drained`` raising
``RuntimeError(f"no stub registered for actor: {job.actor}")`` in
taskq/testing/_runner.py:732) for this case too: an actor that can
never be dispatched because it has no registered stub/config is a
strictly worse failure mode than one that dispatches and then
immediately errors, and today it produces no signal at all.

This test intentionally exercises the failure via the *documented*
adopter-facing path from docs/guides/testing.md (bare
``InMemoryBackend()`` + ``JobsClient`` + ``client.enqueue`` +
``run_until_drained``, no fixture, no ``register_actor_config``) so it
reflects what a first-time adopter's test looks like, not an
artificially constructed edge case.
"""

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.client import JobsClient
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

    Today: run_until_drained() returns normally and the job's status is
    still "pending" -- there is no way for a caller to distinguish this
    from "everything that was enqueued actually ran". This test pins the
    behaviour TaskQ should have: either run_until_drained raises (the
    same RuntimeError it already raises for a *dispatched* job with a
    missing stub), or it dispatches and fails the job -- anything except
    silently reporting "drained" while the job never ran.
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

    await backend.run_until_drained()

    row = await backend.get(handle.job_id)
    assert row is not None

    # The current (undesired) behaviour is row.status == "pending" after
    # run_until_drained() returns -- i.e. drained lies. Pin the desired
    # contract: a job that can never be dispatched must not be silently
    # indistinguishable from one that ran. It must be visibly wrong --
    # either dispatched-and-failed, or run_until_drained itself must
    # raise. status == "pending" post-drain is the one outcome that is
    # never acceptable, because it is the one a caller cannot detect
    # without already suspecting the bug.
    assert row.status != "pending", (
        "run_until_drained() returned normally but left an unregistered "
        f"actor's job stuck at status={row.status!r} with no exception, "
        "warning, or terminal state -- a caller awaiting this job's "
        "result via handle.wait() hangs forever with no diagnostic."
    )
