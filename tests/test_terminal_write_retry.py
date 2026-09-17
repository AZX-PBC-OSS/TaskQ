"""A terminal write that hits a transient infrastructure error is retried.

A sub-second Postgres blip during the one statement that records a job's
outcome must not cost the fleet a full at-least-once re-execution (or,
before the lease is disowned, a stranded row). The consumer retries the
pool-path terminal writes a bounded number of times with backoff — the
shape River's job completer and Oban's executor use — and only then
reports the write as failed. Fence outcomes are not retried: a write that
matched nothing is an answer, not an outage.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog.testing
from structlog.typing import EventDict

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId, JobRow, RetryKind
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import consume_one_job
from taskq.worker._handlers import AttemptOutcome

_START = datetime(2026, 1, 1, tzinfo=UTC)
_ACTOR = "blip_actor"


class _BlippingBackend(InMemoryBackend):
    """In-memory twin whose terminal writes fail with an infra error the
    first *failures* times they are called, then behave normally — the
    connection-reset-then-recovered shape of a Postgres hiccup."""

    def __init__(self, *, failures: int, clock: FakeClock) -> None:
        super().__init__(clock=clock)
        self._failures_left = failures
        self.write_calls = 0

    def _maybe_blip(self) -> None:
        self.write_calls += 1
        if self._failures_left > 0:
            self._failures_left -= 1
            raise OSError("connection reset by peer")

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
    ) -> JobRow:
        self._maybe_blip()
        return await super().mark_failed_or_retry(
            job_id,
            worker_id,
            error_info,
            retry_delay,
            progress_seq,
            progress_state,
            attempt=attempt,
        )

    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        self._maybe_blip()
        return await super().mark_succeeded(
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            fallback_result_ttl,
            result_bytes=result_bytes,
            attempt=attempt,
        )


async def _running_job(
    backend: InMemoryBackend, *, retry_kind: RetryKind = "non_retryable"
) -> tuple[JobRow, UUID]:
    backend.register_actor_config(actor=_ACTOR)
    args = EnqueueArgs(
        id=new_job_id(),
        actor=_ACTOR,
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind=retry_kind,
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only; the runner's own dispatch uses the same worker id.
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1
    return dispatched[0], worker_id


async def _failing_actor(job_row: object, ctx: object) -> object:
    raise ValueError("actor failed")


async def _succeeding_actor(job_row: object, ctx: object) -> object:
    return {"ok": True}


async def _consume(
    backend: InMemoryBackend, job: JobRow, worker_id: UUID, *, actor: object
) -> tuple[AttemptOutcome, list[EventDict]]:
    with structlog.testing.capture_logs() as captured:
        outcome = await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=actor,  # type: ignore[arg-type]  # Why: the test actors take (job_row, ctx) positionally, the consumer's run_actor contract.
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
        )
    return outcome, captured


def _events(captured: list[EventDict], name: str) -> list[EventDict]:
    return [e for e in captured if e.get("event") == name]


async def test_failure_write_lands_after_two_blips() -> None:
    """Two infra failures then success: the job ends failed, the write
    landed, and each retry is visible as one WARNING with its attempt."""
    backend = _BlippingBackend(failures=2, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)

    outcome, captured = await _consume(backend, job, worker_id, actor=_failing_actor)

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "failed", (
        f"the terminal write never landed (row is {row.status!r}) — a transient "
        "infra error on the write must be retried, not abandoned"
    )
    assert backend.write_calls == 3
    assert _events(captured, "terminal-write-failed") == []
    retries = _events(captured, "terminal-write-retry")
    assert [e["attempt"] for e in retries] == [1, 2]
    assert all(e["log_level"] == "warning" for e in retries)
    assert all(e["infra_error_class"] == "OSError" for e in retries)


async def test_success_write_lands_after_two_blips() -> None:
    """The autonomous success write is retried the same way: the actor's
    result is recorded once and no failure is painted on a job that
    succeeded."""
    backend = _BlippingBackend(failures=2, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)

    outcome, captured = await _consume(backend, job, worker_id, actor=_succeeding_actor)

    assert outcome == "succeeded"
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "succeeded"
    assert backend.write_calls == 3
    assert _events(captured, "terminal-write-failed") == []
    assert [e["attempt"] for e in _events(captured, "terminal-write-retry")] == [1, 2]


async def test_write_that_keeps_failing_is_reported_after_the_budget() -> None:
    """Three failures exhaust the budget: the write is reported as failed
    exactly once (the existing terminal-write-failed contract), the row
    stays running for lease reclaim, and no fourth attempt is made."""
    backend = _BlippingBackend(failures=10, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)

    outcome, captured = await _consume(backend, job, worker_id, actor=_failing_actor)

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "running"
    assert backend.write_calls == 3
    assert [e["attempt"] for e in _events(captured, "terminal-write-retry")] == [1, 2]
    failed = _events(captured, "terminal-write-failed")
    assert len(failed) == 1
    assert failed[0]["infra_error_class"] == "OSError"
    assert failed[0]["job_error_class"] == "ValueError"
