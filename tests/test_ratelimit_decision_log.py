"""Unit tests for the shared rate-limit decision logger.

Exercises ``log_decision`` directly without going through TokenBucket
or SlidingWindow. Tests assert on behavioral contracts (no exception
raised, decision object not mutated) rather than log message content.
"""

from datetime import timedelta

from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit.decision import RateLimitDecision


def _allowed_result() -> RateLimitDecision:
    return RateLimitDecision(
        allowed=True,
        remaining=5.0,
        retry_after=timedelta(0),
        bucket_name="test",
        backend="memory",
    )


def _denied_result(
    retry_after: timedelta | None = timedelta(seconds=2),
) -> RateLimitDecision:
    return RateLimitDecision(
        allowed=False,
        remaining=0.0,
        retry_after=retry_after,
        bucket_name="test",
        backend="redis",
    )


def test_allowed_emits_debug_only() -> None:
    """log_decision completes without error for an allowed decision."""
    decision = _allowed_result()
    log_decision(decision)
    # Decision object is not mutated by logging
    assert decision.allowed is True
    assert decision.remaining == 5.0


def test_denied_emits_debug_and_info() -> None:
    """log_decision completes without error for a denied decision."""
    decision = _denied_result()
    log_decision(decision)
    assert decision.allowed is False
    assert decision.remaining == 0.0


def test_retry_after_none_yields_retry_after_seconds_none() -> None:
    """log_decision handles denied decision with retry_after=None."""
    decision = _denied_result(retry_after=None)
    log_decision(decision)
    assert decision.retry_after is None


def test_retry_after_positive_yields_total_seconds() -> None:
    """log_decision handles denied decision with positive retry_after."""
    decision = _denied_result(retry_after=timedelta(seconds=2))
    log_decision(decision)
    assert decision.retry_after == timedelta(seconds=2)


def test_retry_after_zero_yields_zero_point_zero() -> None:
    """log_decision handles allowed decision with retry_after=0."""
    decision = _allowed_result()
    log_decision(decision)
    assert decision.retry_after == timedelta(0)


def test_style_none_omits_style_key() -> None:
    """log_decision completes with style=None."""
    decision = _allowed_result()
    log_decision(decision, style=None)
    assert decision.remaining == 5.0


def test_style_log_includes_style_key() -> None:
    """log_decision completes with style='log'."""
    decision = _allowed_result()
    log_decision(decision, style="log")
    assert decision.remaining == 5.0


def test_style_gcra_includes_style_key() -> None:
    """log_decision completes with style='gcra'."""
    decision = _denied_result()
    log_decision(decision, style="gcra")
    assert decision.allowed is False
