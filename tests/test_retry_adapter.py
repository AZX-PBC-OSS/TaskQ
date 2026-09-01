"""Unit tests for the retry adapter layer (decide_after_failure,
invoke_on_retry_exhausted, safe_mark_failed_or_retry)."""

import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import Backend, ErrorInfo, JobRow, RetryKind
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.retry import (
    Fail,
    JobRetryState,
    Retry,
    RetryPolicy,
    decide_after_failure,
    invoke_on_retry_exhausted,
    safe_mark_failed_or_retry,
)
from taskq.testing.actor import StubActorConfig
from taskq.testing.jobs import make_job_row

# ── hook fires once per Fail, not per Retry ───────────────────


async def test_hook_fires_once_per_fail_not_per_retry() -> None:
    """hook fires once per Fail, not per Retry."""
    hook_calls: list[tuple[JobRow, BaseException]] = []

    def hook(job_row: JobRow, exc: BaseException) -> None:
        hook_calls.append((job_row, exc))

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy, on_retry_exhausted=hook)
    exception = RuntimeError("fail")

    for attempt in range(1, 4):
        job_state = JobRetryState(
            attempt=attempt,
            max_attempts=3,
            retry_kind="transient",
            schedule_to_close=None,
            start_to_close=None,
        )
        decision = decide_after_failure(actor_config, exception, job_state)
        if isinstance(decision, Fail):
            job_row = make_job_row(attempt=attempt)
            await invoke_on_retry_exhausted(
                actor_config.on_retry_exhausted,
                job_row,
                exception,
                3.0,
            )

    assert len(hook_calls) == 1


# ── hook exception is swallowed ────────────────────────────────


async def test_hook_exception_is_swallowed() -> None:
    """hook raises RuntimeError; invoke_on_retry_exhausted returns normally."""

    def bad_hook(job_row: JobRow, exc: BaseException) -> None:
        raise RuntimeError("hook crashed")

    job_row = make_job_row()
    exception = RuntimeError("original")

    await invoke_on_retry_exhausted(bad_hook, job_row, exception, 3.0)


# ── actor-config drift ──────────────────────────────────


def test_actor_config_drift_row_authoritative() -> None:
    """actor-config drift — row-stored max_attempts wins over live registration."""
    policy_b = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    cfg_b = StubActorConfig(retry=policy_b)

    job_state = JobRetryState(
        attempt=2,
        max_attempts=5,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )

    decision = decide_after_failure(cfg_b, RuntimeError("x"), job_state)
    assert isinstance(decision, Retry)  # Uses row's max_attempts=5, not cfg_b's 2


# ── async hook ─────────────────────────────────────────────────


async def test_async_hook_executed() -> None:
    """async hook that awaits asyncio.sleep and appends to a list; list has exactly one entry."""
    entries: list[str] = []

    async def slow_hook(job_row: JobRow, exc: BaseException) -> None:
        await asyncio.sleep(0.01)
        entries.append("called")

    job_row = make_job_row()
    exception = RuntimeError("fail")

    await invoke_on_retry_exhausted(slow_hook, job_row, exception, 3.0)
    assert entries == ["called"]


# ── WorkerOwnershipMismatch at adapter layer ───────────────────


class _MismatchBackend:
    """Stub backend whose mark_failed_or_retry raises WorkerOwnershipMismatch."""

    def __init__(self, exc: WorkerOwnershipMismatch) -> None:
        self._exc = exc

    async def mark_failed_or_retry(
        self,
        job_id: UUID,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> JobRow:
        raise self._exc


async def test_worker_ownership_mismatch_at_adapter() -> None:
    """safe_mark_failed_or_retry catches WorkerOwnershipMismatch; returns None."""
    job_id = new_job_id()
    worker_id = new_uuid()
    actual_worker = new_uuid()
    exc = WorkerOwnershipMismatch(job_id, worker_id, actual_worker)

    stub_backend = _MismatchBackend(exc)

    result = await safe_mark_failed_or_retry(
        cast(
            Backend, stub_backend
        ),  # Why: stub provides mark_failed_or_retry; other Protocol methods are never called in this test
        job_id=job_id,
        worker_id=worker_id,
        error_info=ErrorInfo(
            error_class="RuntimeError",
            error_message="fail",
            error_traceback=None,
        ),
        retry_delay=None,
    )

    assert result is None


# ── clock skew between consumer and PG ──────────────────────────


def test_clock_skew_no_panic() -> None:
    """clock skew — no panic; the decision is a DELAY, so no skewed-clock
    stamp is ever computed in Python (the backend derives scheduled_at
    from its own clock; see mark_failed_or_retry)."""
    policy = RetryPolicy(
        kind="transient",
        max_attempts=3,
        base=timedelta(seconds=10),
        jitter=0.0,
    )
    actor_config = StubActorConfig(retry=policy)

    job_state = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )

    decision = decide_after_failure(actor_config, RuntimeError("x"), job_state)
    assert isinstance(decision, Retry)
    assert decision.retry_delay == policy.base


# ── hook hangs longer than timeout ──────────────────────────────


async def test_hook_hangs_longer_than_timeout() -> None:
    """hook hangs longer than timeout; call returns within ~0.6s; no exception."""

    async def hanging_hook(job_row: JobRow, exc: BaseException) -> None:
        await asyncio.sleep(999)

    job_row = make_job_row()
    exception = RuntimeError("fail")

    start = time.monotonic()
    await invoke_on_retry_exhausted(
        hanging_hook,
        job_row,
        exception,
        0.5,
    )
    elapsed = time.monotonic() - start

    assert elapsed < 1.0


# ── unknown retry_kind value surfaces clearly ───────────────────


def test_unknown_retry_kind_surfaces_as_validation_error() -> None:
    """unknown retry_kind value surfaces as pydantic.ValidationError naming the offending kind."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy)

    job_state = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind=cast(
            RetryKind, "this_value_is_not_valid"
        ),  # Why: intentionally invalid value to test validation error surfacing
        schedule_to_close=None,
        start_to_close=None,
    )

    with pytest.raises(ValidationError) as exc_info:
        decide_after_failure(actor_config, RuntimeError("x"), job_state)

    msg = str(exc_info.value)
    assert "this_value_is_not_valid" in msg


# ── B-TG-14: indefinite retry tier returns Retry through the adapter ──────


def test_decide_after_failure_indefinite_returns_retry() -> None:
    """B-TG-14: decide_after_failure with retry_kind='indefinite',
    schedule_to_close=None returns Retry through the adapter layer.
    The indefinite tier ignores max_attempts and retries on any non-fatal
    exception — the deadline is the SQL guard's business, not the
    classifier's."""
    policy = RetryPolicy(kind="indefinite", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy)

    job_state = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="indefinite",
        schedule_to_close=None,
        start_to_close=None,
    )

    decision = decide_after_failure(actor_config, RuntimeError("test"), job_state)
    assert isinstance(decision, Retry)
    assert decision.retry_delay > timedelta(0)


# ── B-TG-11: decide_after_failure with cap < base raises ValidationError ──


def test_decide_after_failure_cap_less_than_base_raises_validation_error() -> None:
    """B-TG-11: decide_after_failure with a live actor config where the
    reconstructed policy has cap < base raises ValidationError (fail-loud).
    Simulates a rolling deploy where actor config changed from (base=1s, cap=60s)
    to (base=60s, cap=1s) — the cap<base invariant is enforced at reconstruction.

    Uses model_construct to bypass RetryPolicy's own validation so the broken
    state is stored in the live actor config's retry field; decide_after_failure
    reconstructs a new RetryPolicy from those scalars, which then raises.
    """
    # Build a broken policy (cap=1s < base=60s) bypassing pydantic validation.
    # This simulates the "live registration changed but row is old" scenario.
    broken_policy = RetryPolicy.model_construct(
        kind="transient",
        max_attempts=3,
        base=timedelta(seconds=60),  # base=60s
        cap=timedelta(seconds=1),  # cap=1s — violates cap >= base
        jitter=0.0,
    )

    @dataclass(frozen=True, slots=True)
    class BrokenCapActorConfig:
        non_retryable_exceptions: tuple[type[Exception], ...] = ()
        retry_classifier: None = None
        on_retry_exhausted: None = None
        on_retry_exhausted_timeout: float = 3.0
        on_success: None = None
        on_success_timeout: float = 3.0

        @property
        def retry(self) -> RetryPolicy:
            return broken_policy

    job_state = JobRetryState(
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )

    with pytest.raises(ValidationError):
        decide_after_failure(BrokenCapActorConfig(), RuntimeError("x"), job_state)


# ── B-TG-15: invoke_on_retry_exhausted hook coroutine raising mid-execution ──


async def test_hook_coroutine_raises_before_sleep_is_swallowed() -> None:
    """B-TG-15: hook coroutine raises before any await — exception caught
    and does not propagate to caller.
    """

    async def raising_hook(job_row: JobRow, exc: BaseException) -> None:
        raise RuntimeError("hook raised immediately")

    job_row = make_job_row()
    exception = RuntimeError("original failure")

    await invoke_on_retry_exhausted(raising_hook, job_row, exception, 3.0)


async def test_hook_coroutine_raises_after_await_is_swallowed() -> None:
    """B-TG-15: hook coroutine raises after an intermediate await — exception
    caught and does not propagate.
    """

    async def raising_after_await_hook(job_row: JobRow, exc: BaseException) -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("hook raised after sleep")

    job_row = make_job_row()
    exception = ValueError("trigger")

    await invoke_on_retry_exhausted(raising_after_await_hook, job_row, exception, 3.0)


# ── classifier has no clock frame ───────────────────────────────
#
# The two former tests here pinned the REMOVED Python deadline arbiter
# (the classifier's clock-frame opinion of schedule_to_close) — exactly
# the C2 hazard: a worker clock disagreeing with the server killed live
# jobs or voided backoff.  classify() now takes neither a deadline nor a
# clock; the SQL guard in mark_failed_or_retry is the single deadline
# arbiter (pinned in tests/test_clock_domain_isolation.py and
# tests/test_retry_classifier.py::test_deadline_is_not_a_classifier_input).
