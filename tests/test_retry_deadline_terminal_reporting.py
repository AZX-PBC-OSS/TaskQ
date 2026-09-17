"""A retry the deadline refuses is a terminal failure and is reported as one.

``decide_after_failure`` can answer Retry while the row's
``schedule_to_close`` lies before the next dispatch: the retry write then
lands the row ``failed`` with ``DeadlineExceeded`` (the backend's deadline
arm, in SQL and in the in-memory twin). That is as terminal as an
exhausted budget — the ``job-failed`` ERROR line, the ``on_retry_exhausted``
hook and the ``ErrorReporter`` are exactly the signals an operator has for
a dead job, and every terminal failure emits each of them once.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog.testing
from structlog.typing import EventDict

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import consume_one_job

_START = datetime(2026, 1, 1, tzinfo=UTC)
_ACTOR = "deadline_actor"


class _ActorBoomError(RuntimeError):
    pass


class _Recorder:
    """Records the hook and the reporter, the way an alerting adapter would."""

    def __init__(self) -> None:
        self.exhausted: list[tuple[JobRow, BaseException]] = []
        self.reported: list[tuple[JobRow, BaseException]] = []

    async def on_retry_exhausted(self, job: JobRow, exc: BaseException) -> None:
        self.exhausted.append((job, exc))

    async def report(self, job: JobRow, exception: BaseException) -> None:
        self.reported.append((job, exception))


async def _running_job_under_a_deadline(
    backend: InMemoryBackend, *, deadline: timedelta
) -> tuple[JobRow, UUID]:
    backend.register_actor_config(actor=_ACTOR)
    args = EnqueueArgs(
        id=new_job_id(),
        actor=_ACTOR,
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_START,
        schedule_to_close=_START + deadline,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only; the runner's own dispatch uses the same worker id.
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1
    return dispatched[0], worker_id


async def _raising_actor(job_row: object, ctx: object) -> object:
    raise _ActorBoomError("actor failed")


def _events(captured: list[EventDict], name: str) -> list[EventDict]:
    return [e for e in captured if e.get("event") == name]


async def _consume(
    backend: InMemoryBackend, job: JobRow, worker_id: UUID, recorder: _Recorder
) -> tuple[str, list[EventDict]]:
    with structlog.testing.capture_logs() as captured:
        outcome = await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=_raising_actor,
            actor_config=StubActorConfig(
                retry=RetryPolicy(base=timedelta(minutes=5), jitter=0.0),
                on_retry_exhausted=recorder.on_retry_exhausted,
            ),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
            error_reporter=recorder,
        )
    return outcome, captured


async def test_a_retry_refused_by_the_deadline_is_reported_as_a_terminal_failure() -> None:
    """The retry decision says Retry; the row's deadline says no. The job
    ends ``failed`` with ``DeadlineExceeded``, and every terminal-failure
    signal fires exactly once for it."""
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job_under_a_deadline(backend, deadline=timedelta(minutes=1))
    recorder = _Recorder()

    outcome, captured = await _consume(backend, job, worker_id, recorder)

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "failed" and row.error_class == "DeadlineExceeded", (
        "fixture broken: the retry write must be the one the deadline refuses"
    )

    failed = _events(captured, "job-failed")
    assert len(failed) == 1, (
        f"exactly one job-failed line per terminal failure; got {len(failed)} — the "
        "deadline arm of the retry branch terminates the job without reporting it"
    )
    assert failed[0]["log_level"] == "error"
    assert failed[0]["cause"] == "DeadlineExceeded"
    assert failed[0]["error_class"] == _ActorBoomError.__name__

    assert [(r.id, r.status, type(exc)) for r, exc in recorder.exhausted] == [
        (job.id, "failed", _ActorBoomError)
    ], "on_retry_exhausted must fire once with the final row"
    assert [(r.id, r.status, type(exc)) for r, exc in recorder.reported] == [
        (job.id, "failed", _ActorBoomError)
    ], "the ErrorReporter must see the terminal failure"

    # The backend twin logs its own row transition beside the consumer's
    # announcement; both must name the transition the row took.
    transitions = {
        (e["from_state"], e["to_state"]) for e in captured if e.get("kind") == "state_change"
    }
    assert transitions == {("running", "failed")}, (
        "the announced transition must be the one the row took"
    )


async def test_a_retry_the_deadline_allows_stays_a_retry() -> None:
    """The same shape with room before the deadline: scheduled, and none
    of the terminal-failure signals fire."""
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job_under_a_deadline(backend, deadline=timedelta(hours=1))
    recorder = _Recorder()

    outcome, captured = await _consume(backend, job, worker_id, recorder)

    assert outcome == "scheduled"
    row = await backend.get(job.id)
    assert row is not None and row.status == "scheduled"
    assert _events(captured, "job-failed") == []
    assert recorder.exhausted == [] and recorder.reported == []
    transitions = {
        (e["from_state"], e["to_state"]) for e in captured if e.get("kind") == "state_change"
    }
    assert transitions == {("running", "scheduled")}
