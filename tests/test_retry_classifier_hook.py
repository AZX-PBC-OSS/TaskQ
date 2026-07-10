"""Unit and in-memory integration tests for the retry_classifier hook seam:
RetryOverride, RetryClassifier.classify(override=...), and the
decide_after_failure adapter's hook-invocation/failure-isolation wiring.
"""

from datetime import datetime, timedelta

import pytest
import structlog
from pydantic import ValidationError

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.exceptions import PayloadValidationError
from taskq.retry import (
    Fail,
    JobRetryState,
    Retry,
    RetryClassifier,
    RetryOverride,
    RetryPolicy,
    decide_after_failure,
)
from taskq.testing.actor import StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_NOW = datetime(2026, 1, 1)


def _job_state(
    *,
    attempt: int = 1,
    max_attempts: int = 3,
    retry_kind: str = "transient",
    schedule_to_close: datetime | None = None,
) -> JobRetryState:
    return JobRetryState(
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind=retry_kind,  # type: ignore[arg-type]  # Why: test call sites only pass valid RetryKind literals
        schedule_to_close=schedule_to_close,
        start_to_close=None,
    )


# ── hook returning None falls through to default behaviour ─────────────


def test_hook_returning_none_falls_through_to_default_policy_kind() -> None:
    """A hook returning None leaves classification unchanged from the
    default policy.kind='transient' behaviour."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy, retry_classifier=lambda exc, attempt: None)

    decision = decide_after_failure(actor_config, RuntimeError("x"), _job_state(), _NOW)

    assert isinstance(decision, Retry)


# ── hook overriding kind="indefinite" on a transient policy ────────────


def test_hook_override_kind_indefinite_wins_over_transient_policy() -> None:
    """A hook returning RetryOverride(kind='indefinite') overrides a
    policy.kind='transient' for that occurrence — attempt >= max_attempts
    still retries because indefinite ignores the attempt budget."""
    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=lambda exc, attempt: RetryOverride(kind="indefinite"),
    )

    job_state = _job_state(attempt=2, max_attempts=2, retry_kind="transient")
    decision = decide_after_failure(actor_config, RuntimeError("x"), job_state, _NOW)

    assert isinstance(decision, Retry)


def test_hook_override_only_affects_that_occurrence() -> None:
    """Subsequent occurrences without an override respect the original
    policy's attempt-budget semantics via job_state.max_attempts."""
    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    actor_config_no_hook = StubActorConfig(retry=policy)

    job_state = _job_state(attempt=2, max_attempts=2, retry_kind="transient")
    decision = decide_after_failure(actor_config_no_hook, RuntimeError("x"), job_state, _NOW)

    assert isinstance(decision, Fail)


# ── hook overriding kind="non_retryable" on transient/indefinite policy ─


@pytest.mark.parametrize("policy_kind", ["transient", "indefinite"])
def test_hook_override_kind_non_retryable_causes_immediate_fail(policy_kind: str) -> None:
    """A hook returning RetryOverride(kind='non_retryable') causes an
    immediate Fail even though policy.kind is transient/indefinite."""
    policy = RetryPolicy(kind=policy_kind, max_attempts=5, jitter=0.0)  # type: ignore[arg-type]  # Why: parametrized literal
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=lambda exc, attempt: RetryOverride(kind="non_retryable"),
    )

    job_state = _job_state(attempt=1, max_attempts=5, retry_kind=policy_kind)
    decision = decide_after_failure(actor_config, RuntimeError("x"), job_state, _NOW)

    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── hook overriding delay ───────────────────────────────────────────────


def test_hook_override_delay_produces_retry_at_now_plus_delay() -> None:
    """A hook returning RetryOverride(delay=timedelta(seconds=X)) produces
    a Retry decision with next_scheduled_at == now + X, not the policy's
    computed exponential/linear backoff."""
    policy = RetryPolicy(kind="transient", max_attempts=3, base=timedelta(seconds=5), jitter=0.0)
    override_delay = timedelta(seconds=42)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=lambda exc, attempt: RetryOverride(delay=override_delay),
    )

    decision = decide_after_failure(actor_config, RuntimeError("x"), _job_state(), _NOW)

    assert isinstance(decision, Retry)
    assert decision.next_scheduled_at == _NOW + override_delay


def test_hook_override_delay_clamped_to_max_retry_backoff() -> None:
    """A huge override delay is clamped to max_retry_backoff, matching
    compute_backoff's own ceiling behaviour."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    huge_delay = timedelta(days=365)
    max_retry_backoff = timedelta(hours=24)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=lambda exc, attempt: RetryOverride(delay=huge_delay),
    )

    decision = decide_after_failure(
        actor_config,
        RuntimeError("x"),
        _job_state(),
        _NOW,
        max_retry_backoff=max_retry_backoff,
    )

    assert isinstance(decision, Retry)
    assert decision.next_scheduled_at == _NOW + max_retry_backoff


def test_negative_delay_raises_validation_error_at_construction() -> None:
    """RetryOverride(delay=negative timedelta) raises pydantic
    ValidationError at construction time."""
    with pytest.raises(ValidationError):
        RetryOverride(delay=timedelta(seconds=-1))


# ── non_retryable_exceptions / PayloadValidationError win regardless ───


def test_non_retryable_exceptions_isinstance_match_wins_over_hook() -> None:
    """non_retryable_exceptions isinstance match wins even when a hook is
    registered and would return an override — the hook must not even be
    consulted for excluded exception types."""
    hook_called = False

    def hook(exc: BaseException, attempt: int) -> RetryOverride:
        nonlocal hook_called
        hook_called = True
        return RetryOverride(kind="indefinite")

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        non_retryable_exceptions=(ValueError,),
        retry_classifier=hook,
    )

    decision = decide_after_failure(actor_config, ValueError("excluded"), _job_state(), _NOW)

    assert not hook_called, "hook must not be called for non_retryable_exceptions"
    assert isinstance(decision, Fail)
    assert decision.error_class == "ValueError"


def test_payload_validation_error_wins_over_hook_in_adapter() -> None:
    """PayloadValidationError must not reach the hook — the hook is
    skipped at the adapter layer before classify, matching the
    non_retryable_exceptions contract."""
    hook_called = False

    def hook(exc: BaseException, attempt: int) -> RetryOverride:
        nonlocal hook_called
        hook_called = True
        return RetryOverride(kind="indefinite")

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=hook,
    )

    decision = decide_after_failure(
        actor_config, PayloadValidationError("bad payload"), _job_state(), _NOW
    )

    assert not hook_called, "hook must not be called for PayloadValidationError"
    assert isinstance(decision, Fail)
    assert decision.error_class == "PayloadValidationError"


def test_payload_validation_error_wins_over_hook_in_pure_classifier() -> None:
    """RetryClassifier.classify: PayloadValidationError always fails
    regardless of override, confirmed at the pure-classifier layer."""
    policy = RetryPolicy(kind="indefinite", max_attempts=3, jitter=0.0)

    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=PayloadValidationError("bad payload"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
        override=RetryOverride(kind="indefinite"),
    )

    assert isinstance(decision, Fail)
    assert decision.error_class == "PayloadValidationError"


# ── hook raising does not propagate ─────────────────────────────────────


def test_hook_raising_exception_falls_back_to_default_classification_and_logs() -> None:
    """A hook that raises does not propagate — decide_after_failure still
    returns a valid RetryDecision using default classification, and a
    warning is logged."""

    def bad_hook(exc: BaseException, attempt: int) -> RetryOverride:
        raise RuntimeError("hook exploded")

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy, retry_classifier=bad_hook)

    with structlog.testing.capture_logs() as captured:
        decision = decide_after_failure(actor_config, RuntimeError("x"), _job_state(), _NOW)

    assert isinstance(decision, Retry)
    warnings = [e for e in captured if e.get("event") == "retry-classifier-hook-failed"]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"


# ── hook returning non-RetryOverride falls back to default (S3) ──────────


def test_hook_returning_dict_falls_back_to_default_and_logs() -> None:
    """A hook that returns a dict (not a RetryOverride) does not crash the
    retry pipeline — decide_after_failure logs a warning and falls back to
    the static policy's default classification."""

    def bad_hook(exc: BaseException, attempt: int) -> object:
        return {"kind": "indefinite"}

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy, retry_classifier=bad_hook)  # type: ignore[arg-type]  # Why: intentionally passing a hook with a wrong return type to test runtime validation

    with structlog.testing.capture_logs() as captured:
        decision = decide_after_failure(actor_config, RuntimeError("x"), _job_state(), _NOW)

    assert isinstance(decision, Retry)
    warnings = [e for e in captured if e.get("event") == "retry-classifier-hook-invalid-return"]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["return_type"] == "dict"


def test_hook_returning_non_retryable_dict_does_not_cause_immediate_fail() -> None:
    """A hook returning a dict with kind='non_retryable' must NOT influence
    classification — the invalid return is discarded and the static policy
    governs the decision (transient → Retry on attempt 1 < max_attempts 3)."""

    def bad_hook(exc: BaseException, attempt: int) -> object:
        return {"kind": "non_retryable"}

    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(retry=policy, retry_classifier=bad_hook)  # type: ignore[arg-type]  # Why: intentionally passing a hook with a wrong return type to test runtime validation

    decision = decide_after_failure(actor_config, RuntimeError("x"), _job_state(), _NOW)

    assert isinstance(decision, Retry), (
        "invalid return must be discarded; static transient policy should retry"
    )


# ── hook inspects exception-instance attributes ─────────────────────────


class HttpError(Exception):
    """Exception carrying an HTTP status code for per-instance branching."""

    def __init__(self, status_code: int | None, message: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code


def _http_hook(exc: BaseException, attempt: int) -> RetryOverride | None:
    if not isinstance(exc, HttpError):
        return None
    if exc.status_code == 404:
        return RetryOverride(kind="non_retryable")
    if exc.status_code == 429:
        return RetryOverride(kind="indefinite")
    if exc.status_code == 500:
        return RetryOverride(kind="transient")
    return None


@pytest.mark.parametrize(
    ("status_code", "expected_type", "expected_retryable", "retry_kind", "max_attempts"),
    [
        (404, Fail, False, "transient", 5),
        (429, Retry, True, "transient", 1),
        (500, Retry, True, "transient", 5),
    ],
)
def test_hook_branches_on_exception_attribute(
    status_code: int,
    expected_type: type[Fail] | type[Retry],
    expected_retryable: bool,
    retry_kind: str,
    max_attempts: int,
) -> None:
    """The hook can inspect exception-instance attributes to branch a
    single exception type into different retry behaviours per occurrence."""
    policy = RetryPolicy(kind="indefinite", max_attempts=max_attempts, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=_http_hook,
    )

    job_state = _job_state(attempt=1, max_attempts=max_attempts, retry_kind=retry_kind)
    decision = decide_after_failure(actor_config, HttpError(status_code), job_state, _NOW)

    assert isinstance(decision, expected_type)
    if isinstance(decision, Fail):
        assert decision.retryable is expected_retryable


def test_hook_returning_none_falls_through_to_default_indefinite_policy() -> None:
    """When the hook returns None (unrecognised status code), the static
    RetryPolicy governs classification — here an indefinite policy retries."""
    policy = RetryPolicy(kind="indefinite", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=_http_hook,
    )

    decision = decide_after_failure(
        actor_config, HttpError(status_code=None), _job_state(retry_kind="indefinite"), _NOW
    )

    assert isinstance(decision, Retry)


# ── hook override delay vs schedule_to_close deadline ───────────────────


def test_hook_override_delay_exceeding_deadline_produces_fail_deadline() -> None:
    """A hook returning RetryOverride(delay=10h) with schedule_to_close
    only 1h away produces Fail(DeadlineExceeded) — the override delay is
    honoured but the deadline still wins."""
    policy = RetryPolicy(kind="transient", max_attempts=5, jitter=0.0)
    schedule_to_close = _NOW + timedelta(hours=1)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=lambda exc, attempt: RetryOverride(delay=timedelta(hours=10)),
    )

    job_state = _job_state(
        attempt=1, max_attempts=5, retry_kind="transient", schedule_to_close=schedule_to_close
    )
    decision = decide_after_failure(actor_config, RuntimeError("x"), job_state, _NOW)

    assert isinstance(decision, Fail)
    assert decision.error_class == "DeadlineExceeded"
    assert decision.retryable is False


# ── end-to-end via InMemoryBackend.register_stub ────────────────────────


async def test_end_to_end_hook_influences_real_dispatch_via_register_stub() -> None:
    """A retry_classifier hook registered via
    InMemoryBackend.register_stub actually influences real dispatch/retry
    behaviour through run_until_drained: the hook forces the first
    failure to be non_retryable, so the job fails after exactly one
    attempt despite a transient policy with max_attempts=5."""
    clock = FakeClock(start=_NOW)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def flaky(payload: object, ctx: object) -> None:
        nonlocal call_count
        call_count += 1
        raise RuntimeError(f"attempt {call_count}")

    def force_non_retryable(exc: BaseException, attempt: int) -> RetryOverride:
        return RetryOverride(kind="non_retryable")

    backend.register_stub(
        "hooked_actor",
        flaky,
        retry=RetryPolicy(kind="transient", max_attempts=5, jitter=0.0),
        retry_classifier=force_non_retryable,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="hooked_actor",
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_NOW,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.attempt == 1
    assert call_count == 1
