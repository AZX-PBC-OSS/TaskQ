"""A terminal write that hits a transient infrastructure error is retried.

A sub-second Postgres blip during the one statement that records a job's
outcome must not cost the fleet a full at-least-once re-execution (or,
before the lease is disowned, a stranded row). The consumer retries the
pool-path terminal writes a bounded number of times with backoff, and only then
reports the write as failed. Fence outcomes are not retried: a write that
matched nothing is an answer, not an outage.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
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
from taskq.worker.shutdown import ShutdownPhase

_START = datetime(2026, 1, 1, tzinfo=UTC)
_ACTOR = "blip_actor"


class _BlippingBackend(InMemoryBackend):
    """In-memory twin whose terminal writes fail with an infra error the
    first *failures* times they are called, then behave normally - the
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
        claim_epoch: int | None = None,
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
            claim_epoch=claim_epoch,
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
        claim_epoch: int | None = None,
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
            claim_epoch=claim_epoch,
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
        f"the terminal write never landed (row is {row.status!r}) - a transient "
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
    """Four failures exhaust the attempt budget: the write is reported as
    failed exactly once (the existing terminal-write-failed contract), the
    row stays running for lease reclaim, and no fifth attempt is made. The
    third wait (800 ms) is the one that turns a blip of a few hundred
    milliseconds into a landed write instead of an at-least-once re-run."""
    backend = _BlippingBackend(failures=10, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)

    outcome, captured = await _consume(backend, job, worker_id, actor=_failing_actor)

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "running"
    assert backend.write_calls == 4
    retries = _events(captured, "terminal-write-retry")
    assert [e["attempt"] for e in retries] == [1, 2, 3]
    # The event records the COMPUTED wait truncated to an int:
    # base * uniform(1 - _TERMINAL_WRITE_JITTER, 1 + _TERMINAL_WRITE_JITTER),
    # so the shipped band is [0.75*base, 1.25*base] and the int() truncation
    # shaves up to a millisecond off the low edge (a 0.75 draw on the 50 ms
    # base records 37, 0.5 ms below the naive rel=0.25 floor; CI rolled
    # exactly that twice). approx(rel=0.25, abs=1) was the wrong tool for
    # that edge: its tolerance is max(rel*expected, abs), so with expected=50
    # the abs=1 never applies and the truncated 37 flaked the run. So assert
    # the band the recorder can actually produce instead of approximating
    # against the untruncated base: each wait lies in its own rung's band
    # with the truncated floor included, [37, 62], [150, 250] and
    # [600, 1000] here. The bands are disjoint, so a flattened or shifted
    # ladder still fails; the only slack is the shipped band itself plus the
    # single truncated millisecond at each low edge.
    from taskq.worker._handlers import _TERMINAL_WRITE_BACKOFF, _TERMINAL_WRITE_JITTER

    for event, base in zip(retries, _TERMINAL_WRITE_BACKOFF, strict=True):
        base_ms = base.total_seconds() * 1000
        band_floor = int(base_ms * (1 - _TERMINAL_WRITE_JITTER))
        band_ceiling = base_ms * (1 + _TERMINAL_WRITE_JITTER)
        assert band_floor <= event["retry_in_ms"] <= band_ceiling, (
            f"retry {event['attempt']} waited {event['retry_in_ms']} ms, outside the "
            f"band [{band_floor}, {band_ceiling}] that the {base_ms:.0f} ms rung's "
            "jitter spread plus int truncation can produce"
        )
    failed = _events(captured, "terminal-write-failed")
    assert len(failed) == 1
    assert failed[0]["infra_error_class"] == "OSError"
    assert failed[0]["job_error_class"] == "ValueError"


async def test_slow_failures_are_bounded_by_wall_time_not_only_by_attempts() -> None:
    """Each attempt against a black-holed Postgres costs a full statement
    timeout, so counting attempts alone would hold a consumer slot for
    attempts times the timeout. The budget is wall time from the first attempt: a
    retry whose wait would end past it is not made, and the exhausted
    budget is reported the same way."""
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import (
        _TERMINAL_WRITE_BUDGET,
        _terminal_write_with_retry,
    )

    now = 0.0
    attempt_cost = _TERMINAL_WRITE_BUDGET.total_seconds() * 0.6

    def monotonic() -> float:
        return now

    calls = 0

    async def slow_failing_write() -> bool:
        nonlocal now, calls
        calls += 1
        now += attempt_cost
        raise TimeoutError("statement timed out")

    with structlog.testing.capture_logs() as captured, pytest.raises(TimeoutError):
        await _terminal_write_with_retry(
            slow_failing_write,
            log=structlog.get_logger("test"),
            job=make_job_row(),
            write_name="mark_succeeded",
            monotonic=monotonic,
        )

    # Attempt 1 ends at 0.6 of the budget; the 50 ms wait fits, attempt 2 ends
    # at 1.2 of the budget, and no wait fits after that.
    assert calls == 2
    retries = _events(captured, "terminal-write-retry")
    assert [e["attempt"] for e in retries] == [1]
    exhausted = _events(captured, "terminal-write-retry-budget-exhausted")
    assert len(exhausted) == 1
    assert exhausted[0]["attempt"] == 2
    assert exhausted[0]["budget_ms"] == int(_TERMINAL_WRITE_BUDGET.total_seconds() * 1000)


# ── What the retry never touches: fences and defects ────────────────────


async def test_a_fence_outcome_is_an_answer_not_an_outage() -> None:
    """A write that matched nothing reports that once; re-issuing it could
    only match nothing again (or, worse, land on a row a newer attempt now
    owns)."""
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _terminal_write_with_retry

    calls = 0

    async def fenced_write() -> bool:
        nonlocal calls
        calls += 1
        return False

    with structlog.testing.capture_logs() as captured:
        landed = await _terminal_write_with_retry(
            fenced_write,
            log=structlog.get_logger("test"),
            job=make_job_row(),
            write_name="mark_succeeded",
        )

    assert landed is False
    assert calls == 1
    assert _events(captured, "terminal-write-retry") == []


async def test_a_defect_in_the_write_stays_loud_on_the_first_raise() -> None:
    """Only the infrastructure family is retried: anything else is a
    programming error whose first raise is the signal."""
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _terminal_write_with_retry

    calls = 0

    async def broken_write() -> bool:
        nonlocal calls
        calls += 1
        raise TypeError("unencodable result")

    with pytest.raises(TypeError):
        await _terminal_write_with_retry(
            broken_write,
            log=structlog.get_logger("test"),
            job=make_job_row(),
            write_name="mark_succeeded",
        )

    assert calls == 1


# ── The cancel path retries too ─────────────────────────────────────────


class _ConsumerDeps:
    """The WorkerDeps fields consume_one_job's cancel handler reads.

    ``shutdown_phase`` mirrors the production field's default: a worker
    not shutting down is ``ShutdownPhase.NONE``, and the handler's
    missed-stamp guard reads it on every cancellation, so the fake must
    carry the faithful value rather than omit the attribute.
    """

    def __init__(self) -> None:
        self.shutdown_phase: ShutdownPhase = ShutdownPhase.NONE
        self.disowned_jobs: set[UUID] = set()
        self.progress_buffers: dict[UUID, object] = {}
        self.redis_client = None
        self.worker_pool = None
        self.settings = None


async def _cancelled_consume(
    backend: InMemoryBackend, job: JobRow, worker_id: UUID, deps: _ConsumerDeps
) -> list[EventDict]:
    """Run the consumer with a cooperative actor and cancel it once the
    actor is inside its body; returns the captured log events."""
    import asyncio
    from typing import Any, cast

    from taskq.worker.cancel import ActiveJobRegistry
    from taskq.worker.deps import WorkerDeps

    actor_entered = asyncio.Event()

    async def blocking_actor(_job: object, ctx: Any) -> object:
        actor_entered.set()
        await ctx.cancel_event.wait()

    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(
            consume_one_job(
                backend,
                job,
                worker_id,
                deps=cast(WorkerDeps, deps),
                run_actor=blocking_actor,
                actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
                payload_type=EmptyPayload,
                clock=FakeClock(start=_START),
                active_jobs=ActiveJobRegistry(),
            )
        )
        await asyncio.wait_for(actor_entered.wait(), timeout=5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    return captured


class _BlippingCancelBackend(_BlippingBackend):
    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> bool:
        self._maybe_blip()
        return await super().mark_cancelled(
            job_id,
            worker_id,
            progress_seq,
            progress_state,
            attempt=attempt,
            claim_epoch=claim_epoch,
        )


async def test_cancel_write_lands_after_two_blips() -> None:
    """An operator cancel's terminal write is a state write like any
    other: a blip is retried inside the cancellation handler, the row
    ends cancelled, nothing is disowned, and the cancellation still
    propagates."""
    backend = _BlippingCancelBackend(failures=2, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()

    captured = await _cancelled_consume(backend, job, worker_id, deps)

    row = await backend.get(job.id)
    assert row is not None and row.status == "cancelled"
    assert backend.write_calls == 3
    assert [e["attempt"] for e in _events(captured, "terminal-write-retry")] == [1, 2]
    assert _events(captured, "terminal-write-failed") == []
    assert deps.disowned_jobs == set()


async def test_cancel_write_that_keeps_failing_is_disowned_after_the_budget() -> None:
    backend = _BlippingCancelBackend(failures=10, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()

    captured = await _cancelled_consume(backend, job, worker_id, deps)

    row = await backend.get(job.id)
    assert row is not None and row.status == "running"
    assert backend.write_calls == 4
    assert len(_events(captured, "terminal-write-failed")) == 1
    assert deps.disowned_jobs == {job.id}


async def test_a_second_cancel_during_a_cancel_write_retry_wait_disowns_the_job() -> None:
    """A forced escalation (or the shutdown's FORCING phase) cancels the
    task again while the handler is waiting to retry: no write is in
    flight and the row is still this worker's, so it is disowned before
    the cancellation propagates - a retry the escalation cut short must
    not leave the lease renewed for a row nothing will move."""
    import asyncio
    from typing import Any, cast

    from taskq.worker.cancel import ActiveJobRegistry
    from taskq.worker.deps import WorkerDeps

    backend = _BlippingCancelBackend(failures=10, clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()
    actor_entered = asyncio.Event()
    first_blip = asyncio.Event()

    async def blocking_actor(_job: object, ctx: Any) -> object:
        actor_entered.set()
        await ctx.cancel_event.wait()

    original_blip = backend._maybe_blip  # pyright: ignore[reportPrivateUsage]  # Why: the test signals on the first failed write to time the second cancel into the retry wait.

    def _blip_and_signal() -> None:
        first_blip.set()
        original_blip()

    backend._maybe_blip = _blip_and_signal  # type: ignore[method-assign]  # Why: as above.

    task = asyncio.create_task(
        consume_one_job(
            backend,
            job,
            worker_id,
            deps=cast(WorkerDeps, deps),
            run_actor=blocking_actor,
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
            active_jobs=ActiveJobRegistry(),
        )
    )
    await asyncio.wait_for(actor_entered.wait(), timeout=5.0)
    task.cancel()
    await asyncio.wait_for(first_blip.wait(), timeout=5.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.write_calls == 1, "the second cancel must cut the retry short"
    assert deps.disowned_jobs == {job.id}
