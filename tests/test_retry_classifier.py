"""Unit tests for RetryClassifier.classify (no PG required).

The classifier decides retry-kind and backoff only — it is NOT a deadline
arbiter.  ``schedule_to_close`` is arbitrated by the SQL guard in
``mark_failed_or_retry`` (single arbiter, the backend's clock); the
deadline-outcome pins live in tests/test_clock_domain_isolation.py and the
state-transition suites.
"""

import inspect
import subprocess
import sys
from datetime import timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.exceptions import PayloadValidationError
from taskq.retry import Fail, Retry, RetryClassifier, RetryPolicy, compute_backoff

# ── non_retryable_exceptions tuple match ──────────────────────────


def test_non_retryable_exceptions_tuple_match() -> None:
    """non_retryable_exceptions=(ValueError,), policy=transient, exception=ValueError → Fail."""
    policy = RetryPolicy(kind="transient", max_attempts=5, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(ValueError,),
        exception=ValueError("test"),
        attempt=1,
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
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── transient under budget ─────────────────────────────────────────


def test_transient_under_budget() -> None:
    """transient, max_attempts=3, attempt=1 → Retry with a positive delay."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
    )
    assert isinstance(decision, Retry)
    assert decision.retry_delay > timedelta(0)


# ── transient exhausted ────────────────────────────────────────────


def test_transient_exhausted() -> None:
    """transient, max_attempts=3, attempt=3 → Fail(retryable=False)."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=3,
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
    )
    assert isinstance(decision, Fail)
    assert decision.retryable is False


# ── the deadline is not a classifier input ────────────────────────


def test_deadline_is_not_a_classifier_input() -> None:
    """C2: classify takes NO ``schedule_to_close`` and NO ``now`` parameter —
    a Python-side deadline pre-check computed from the worker's clock
    disagrees with the SQL guard under app↔DB skew and kills jobs early
    (or rubber-stamps them).  The structural pin keeps the arbiter from
    silently returning: re-adding either input must be a conscious,
    reviewed change.  The deadline-outcome behavior is pinned against the
    SQL in tests/test_clock_domain_isolation.py
    (test_retry_deadline_arbitrated_server_side)."""
    params = inspect.signature(RetryClassifier.classify).parameters
    assert "schedule_to_close" not in params
    assert "now" not in params


# ── indefinite tier ignores max_attempts and deadlines ───────────


def test_indefinite_future_deadline_retry() -> None:
    """indefinite, attempt=1 → Retry(retry_delay=backoff) — the deadline is
    the SQL guard's business, not the classifier's."""
    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4), jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=1,
    )
    assert isinstance(decision, Retry)
    assert decision.retry_delay > timedelta(0)


def test_indefinite_ignores_max_attempts() -> None:
    """indefinite, attempt=1000, max_attempts=3 → Retry(...) — max_attempts ignored."""
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
    )
    assert isinstance(decision, Retry)


# ── non_retryable_exceptions override indefinite ─────────────────


def test_indefinite_non_retryable_exceptions_override() -> None:
    """indefinite + non_retryable_exceptions=(ValueError,)
    raising ValueError → Fail immediately; error_class is 'ValueError'."""
    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4))
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(ValueError,),
        exception=ValueError("test"),
        attempt=1,
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
        max_retry_backoff=timedelta(hours=24),
    )
    assert isinstance(decision, Retry)
    max_delta = cap.total_seconds() * (1 + policy.jitter)
    assert decision.retry_delay.total_seconds() <= max_delta


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
    )
    if attempt < max_attempts:
        assert isinstance(decision, Retry)
    else:
        assert isinstance(decision, Fail)
        assert decision.retryable is False


# ── Hypothesis property — indefinite retry invariant ────────────


@settings(max_examples=200)
@given(attempt=st.integers(min_value=1, max_value=1000))
def test_indefinite_retry_invariant(attempt: int) -> None:
    """indefinite-retry invariant: for ANY attempt (max_attempts is ignored)
    the decision is Retry with exactly the computed backoff — the tier has
    no budget to exhaust and no deadline opinion (the SQL guard owns the
    deadline; one arbiter per predicate)."""
    policy = RetryPolicy(kind="indefinite", time_budget=timedelta(hours=4), jitter=0.0)
    decision = RetryClassifier.classify(
        policy=policy,
        non_retryable_exceptions=(),
        exception=RuntimeError("fail"),
        attempt=attempt,
    )
    assert isinstance(decision, Retry), f"indefinite must always Retry, got {decision}"
    expected = compute_backoff(policy, attempt)
    assert decision.retry_delay == expected, (
        f"retry_delay mismatch: expected {expected}, got {decision.retry_delay}"
    )
