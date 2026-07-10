"""Unit tests for RetryClassifier.classify (no PG required)."""

import subprocess
import sys
from datetime import datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.exceptions import PayloadValidationError
from taskq.retry import Fail, Retry, RetryClassifier, RetryPolicy, compute_backoff

_NOW = datetime(2026, 1, 1, tzinfo=None)


# ── non_retryable_exceptions tuple match ──────────────────────────


def test_non_retryable_exceptions_tuple_match() -> None:
    """non_retryable_exceptions=(ValueError,), policy=transient, exception=ValueError → Fail."""
    policy = RetryPolicy(kind="transient", max_attempts=5, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(ValueError,),
        exception=ValueError("test"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False
    assert decision.error_class == "ValueError"


# ── PayloadValidationError always non-retryable ───────────────────


def test_payload_validation_error_always_non_retryable() -> None:
    """PayloadValidationError with empty non_retryable_exceptions and transient policy → Fail."""
    policy = RetryPolicy(kind="transient", max_attempts=5, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=PayloadValidationError("bad payload"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False
    assert decision.error_class == "PayloadValidationError"


# ── kind='non_retryable' policy path ──────────────────────────────


def test_non_retryable_policy_path() -> None:
    """policy.kind='non_retryable', empty non_retryable_exceptions, RuntimeError → Fail."""
    policy = RetryPolicy(kind="non_retryable", jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("oops"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── transient under budget ─────────────────────────────────────────


def test_transient_under_budget() -> None:
    """transient, max_attempts=3, attempt=1, schedule_to_close=None → Retry with next_scheduled_at > now."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Retry)
    assert decision.next_scheduled_at > _NOW


# ── transient exhausted ────────────────────────────────────────────


def test_transient_exhausted() -> None:
    """transient, max_attempts=3, attempt=3 → Fail(retryable=False)."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=3,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── max_attempts=1 edge case (single-attempt actors) ─────────────────────


def test_max_attempts_one_immediate_fail() -> None:
    """max_attempts=1: attempt=1 is already at the limit → Fail immediately (no retries)."""
    policy = RetryPolicy(kind="transient", max_attempts=1, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("once"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── DeadlineExceeded short-circuit fires ───────────────────────────


def test_deadline_exceeded_short_circuit() -> None:
    """transient, base=10s, jitter=0.0, schedule_to_close=now+1s → Fail('DeadlineExceeded')."""
    policy = RetryPolicy(
        kind="transient",
        max_attempts=5,
        base=timedelta(seconds=10),
        jitter=0.0,
    )
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=_NOW + timedelta(seconds=1),
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.error_class == "DeadlineExceeded"
    assert decision.retryable is False


# ── deadline boundary (>= triggers Fail) ─────────────────────────


def test_deadline_boundary_retry_when_before() -> None:
    """schedule_to_close=now+11s (1s past next_scheduled_at of now+10s) → Retry, NOT DeadlineExceeded."""
    policy = RetryPolicy(
        kind="transient",
        max_attempts=5,
        base=timedelta(seconds=10),
        jitter=0.0,
    )
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=_NOW + timedelta(seconds=11),
        now=_NOW,
    )
    assert isinstance(decision, Retry)


# ── no deadline check when schedule_to_close is None ──────────────


def test_no_deadline_check_when_no_schedule_to_close() -> None:
    """same policy as but schedule_to_close=None → Retry (no deadline check)."""
    policy = RetryPolicy(
        kind="transient",
        max_attempts=5,
        base=timedelta(seconds=10),
        jitter=0.0,
    )
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Retry)


# ── indefinite tier with future deadline → Retry ────────────────


def test_indefinite_future_deadline_retry() -> None:
    """indefinite, schedule_to_close=now+1h, attempt=1 → Retry(next_scheduled_at=now+backoff)."""
    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4), jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=_NOW + timedelta(hours=1),
        now=_NOW,
    )
    assert isinstance(decision, Retry)
    assert decision.next_scheduled_at > _NOW


# ── indefinite backoff overshoots deadline → DeadlineExceeded ───


def test_indefinite_backoff_overshoots_deadline() -> None:
    """indefinite, schedule_to_close=now+1s, base=30s, attempt=1
    → Fail(error_class='DeadlineExceeded') — backoff overshoots deadline."""
    policy = RetryPolicy(
        kind="indefinite",
        base=timedelta(seconds=30),
        jitter=0.0,
    )
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=_NOW + timedelta(seconds=1),
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.error_class == "DeadlineExceeded"
    assert decision.retryable is False


# ── indefinite deadline in past → DeadlineExceeded ──────────────


def test_indefinite_deadline_in_past() -> None:
    """indefinite, schedule_to_close=now-1s → Fail(error_class='DeadlineExceeded')."""
    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4), jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
        schedule_to_close=_NOW - timedelta(seconds=1),
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.error_class == "DeadlineExceeded"
    assert decision.retryable is False


# ── indefinite ignores max_attempts ─────────────────────────────


def test_indefinite_ignores_max_attempts() -> None:
    """indefinite, schedule_to_close=None, attempt=1000,
    max_attempts=3 → Retry(...) — max_attempts ignored."""
    policy = RetryPolicy(
        kind="indefinite",
        max_attempts=3,
        time_budget=timedelta(hours=4),
        jitter=0.0,
    )
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1000,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Retry)


# ── non_retryable_exceptions override indefinite ─────────────────


def test_indefinite_non_retryable_exceptions_override() -> None:
    """indefinite + non_retryable_exceptions=(ValueError,)
    raising ValueError → Fail immediately ; error_class is
    'ValueError', NOT 'DeadlineExceeded'."""
    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4))
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(ValueError,),
        exception=ValueError("test"),
        attempt=1,
        schedule_to_close=_NOW + timedelta(hours=1),
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.error_class == "ValueError"
    assert decision.retryable is False


# ── indefinite backoff cap at high attempt counts ────────────────


def test_indefinite_backoff_capped() -> None:
    """indefinite, cap=timedelta(hours=1), attempt=100,
    max_retry_backoff=timedelta(hours=24) → Retry with capped backoff."""
    cap = timedelta(hours=1)
    policy = RetryPolicy(kind="indefinite", cap=cap)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=100,
        schedule_to_close=_NOW + timedelta(hours=24),
        now=_NOW,
        max_retry_backoff=timedelta(hours=24),
    )
    assert isinstance(decision, Retry)
    delta = decision.next_scheduled_at - _NOW
    max_delta = cap.total_seconds() * (1 + policy.jitter)
    assert delta.total_seconds() <= max_delta


# ── subclass of non_retryable matched ─────────────────────────────


def test_subclass_of_non_retryable_matched() -> None:
    """MyValueError(ValueError) with non_retryable_exceptions=(ValueError,) → Fail."""

    class MyValueError(ValueError):
        pass

    policy = RetryPolicy(kind="transient", max_attempts=5, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(ValueError,),
        exception=MyValueError("sub"),
        attempt=1,
        schedule_to_close=None,
        now=_NOW,
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── import boundary ─────────────────────────────────────────


def test_no_backend_import() -> None:
    """taskq.retry only imports adapter-permitted names from taskq.backend (scoped)."""
    script = (
        "import ast, inspect, taskq.retry\n"
        "src = inspect.getsource(taskq.retry)\n"
        "tree = ast.parse(src)\n"
        "allowed = {'Backend', 'ErrorInfo', 'JobId', 'JobRow', 'RetryKind'}\n"
        "for node in ast.walk(tree):\n"
        "    if isinstance(node, ast.ImportFrom) and node.module and 'taskq.backend' in node.module:\n"
        "        for alias in node.names:\n"
        "            name = alias.asname if alias.asname else alias.name\n"
        "            if name not in allowed:\n"
        "                raise AssertionError(f'taskq.retry imports disallowed name {name!r} from {node.module}')\n"
    )
    result = subprocess.run(  # noqa: S603 Why: subprocess used to run a hardcoded introspection script verifying import boundary; no untrusted input
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"subprocess failed: {result.stderr}"


# ── Hypothesis property ───────────────────────────────────────────


@settings(max_examples=200)
@given(
    max_attempts=st.integers(min_value=1, max_value=10),
    attempt=st.integers(min_value=1, max_value=10),
)
def test_classify_transient_retry_vs_fail(max_attempts: int, attempt: int) -> None:
    """for any transient policy, classify returns Retry iff attempt < max_attempts, Fail iff attempt >= max_attempts."""
    policy = RetryPolicy(kind="transient", max_attempts=max_attempts, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=attempt,
        schedule_to_close=None,
        now=_NOW,
    )
    if attempt < max_attempts:
        assert isinstance(decision, Retry)
    else:
        assert isinstance(decision, Fail)
        assert decision.retryable is False


# ── Hypothesis property — indefinite retry invariant ────────────


@settings(max_examples=200)
@given(
    delta_now=st.integers(min_value=-3600, max_value=3600),
    delta_s2c=st.integers(min_value=-3600, max_value=3600),
    attempt=st.integers(min_value=1, max_value=1000),
    s2c_none=st.booleans(),
)
def test_indefinite_retry_invariant(
    delta_now: int,
    delta_s2c: int,
    attempt: int,
    s2c_none: bool,
) -> None:
    """indefinite-retry invariant. For any (schedule_to_close, now, attempt):
    - decision is Retry iff schedule_to_close is None or
      (now < schedule_to_close and now + compute_backoff(policy, attempt) < schedule_to_close).
    - Otherwise decision == Fail(error_class='DeadlineExceeded', retryable=False).

    Verifies indefinite tier ignores max_attempts and uses deadline logic only."""
    base_now = datetime(2026, 1, 1)
    now = base_now + timedelta(seconds=delta_now)

    if s2c_none:
        schedule_to_close: datetime | None = None
    else:
        schedule_to_close = base_now + timedelta(seconds=delta_s2c)

    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4), jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=attempt,
        schedule_to_close=schedule_to_close,
        now=now,
    )

    if schedule_to_close is None:
        assert isinstance(decision, Retry), (
            f"schedule_to_close=None must always Retry, got {decision} "
            f"at attempt={attempt}, now={now.isoformat()}"
        )
    elif now >= schedule_to_close:
        assert isinstance(decision, Fail), (
            f"now >= schedule_to_close must Fail, got {decision} "
            f"at now={now.isoformat()}, s2c={schedule_to_close.isoformat()}"
        )
        assert decision.error_class == "DeadlineExceeded"
        assert decision.retryable is False
    else:
        expected_next = now + compute_backoff(policy, attempt)
        if expected_next < schedule_to_close:
            assert isinstance(decision, Retry), (
                f"backoff within deadline must Retry, got {decision} "
                f"at attempt={attempt}, now={now.isoformat()}, s2c={schedule_to_close.isoformat()}"
            )
            assert abs((decision.next_scheduled_at - expected_next).total_seconds()) < 0.001, (
                f"next_scheduled_at mismatch: expected {expected_next.isoformat()}, "
                f"got {decision.next_scheduled_at.isoformat()}"
            )
        else:
            assert isinstance(decision, Fail), (
                f"backoff overshoots deadline must Fail(DeadlineExceeded), got {decision} "
                f"at attempt={attempt}, now={now.isoformat()}, s2c={schedule_to_close.isoformat()}"
            )
            assert decision.error_class == "DeadlineExceeded"
            assert decision.retryable is False
