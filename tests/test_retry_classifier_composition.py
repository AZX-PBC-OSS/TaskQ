"""Unit tests for compose_retry_classifiers and the built-in
rate_limit_aware_classifier.

Composition semantics: first-override-wins. Each classifier is invoked in
registration order with ``(exception, attempt)``; the first valid
``RetryOverride`` wins and later classifiers are not consulted; a ``None``
falls through to the next classifier; all-``None`` composes to ``None`` so
the actor's declared ``RetryPolicy`` governs. Per-classifier isolation
follows the single-classifier contract the adapter already pins: a buggy
classifier (raises, or returns a non-``RetryOverride``) is logged at
WARNING and skipped, never propagated.
"""

from datetime import timedelta

import asyncio

import pytest
import structlog

from taskq.retry import RetryOverride, compose_retry_classifiers


def _override(tag: str) -> RetryOverride:
    return RetryOverride(kind="indefinite", delay=timedelta(seconds=1))


# ── composition: first-override-wins ──────────────────────────────


def test_composition_first_override_wins_and_short_circuits() -> None:
    """The first classifier returning an override wins; later classifiers
    are not consulted for that exception (short-circuit, not just
    precedence)."""
    calls: list[str] = []

    def first(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("first")
        return RetryOverride(kind="indefinite")

    def second(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("second")
        return RetryOverride(kind="non_retryable")

    composed = compose_retry_classifiers(first, second)
    result = composed(RuntimeError("x"), 1)

    assert result == RetryOverride(kind="indefinite")
    assert calls == ["first"], "second classifier must not be consulted after a win"


def test_composition_none_from_first_falls_through_to_next() -> None:
    """A classifier returning None contributes nothing; composition
    continues with the next classifier, whose override wins."""
    calls: list[str] = []

    def first(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("first")
        return None

    def second(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("second")
        return RetryOverride(kind="non_retryable")

    composed = compose_retry_classifiers(first, second)
    result = composed(RuntimeError("x"), 1)

    assert result == RetryOverride(kind="non_retryable")
    assert calls == ["first", "second"]


def test_composition_all_none_returns_none_declared_policy_governs() -> None:
    """Every classifier returning None composes to None: the adapter
    treats it exactly like a single hook returning None, so the actor's
    declared RetryPolicy governs."""
    calls: list[str] = []

    def first(exc: BaseException, attempt: int) -> None:
        calls.append("first")

    def second(exc: BaseException, attempt: int) -> None:
        calls.append("second")

    composed = compose_retry_classifiers(first, second)  # type: ignore[arg-type]  # Why: hooks typed to return None exercise the all-None passthrough

    assert composed(RuntimeError("x"), 1) is None
    assert calls == ["first", "second"]


def test_composition_with_no_classifiers_returns_none() -> None:
    """``compose_retry_classifiers()`` composes to a hook that always
    returns None, so a call site can compose a possibly-empty list."""
    composed = compose_retry_classifiers()

    assert composed(RuntimeError("x"), 1) is None


def test_composition_passes_exception_and_attempt_through_verbatim() -> None:
    """Each consulted classifier receives the exact exception instance and
    attempt number the adapter passes to the composed hook."""
    seen: list[tuple[BaseException, int]] = []

    def spy(exc: BaseException, attempt: int) -> None:
        seen.append((exc, attempt))

    exc = RuntimeError("x")
    compose_retry_classifiers(spy)(exc, 7)

    assert seen == [(exc, 7)]
    assert seen[0][0] is exc


# ── composition: per-classifier isolation ─────────────────────────


def test_raising_classifier_is_logged_and_skipped_composition_continues() -> None:
    """A classifier that raises is logged at WARNING and skipped; the
    next classifier is still consulted and its override wins. One broken
    classifier must not discard the overrides the healthy ones provide."""
    calls: list[str] = []

    def broken(exc: BaseException, attempt: int) -> RetryOverride | None:
        raise RuntimeError("classifier exploded")

    def healthy(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("healthy")
        return RetryOverride(kind="indefinite")

    composed = compose_retry_classifiers(broken, healthy)

    with structlog.testing.capture_logs() as captured:
        result = composed(RuntimeError("x"), 1)

    assert result == RetryOverride(kind="indefinite")
    assert calls == ["healthy"]
    warnings = [e for e in captured if e.get("event") == "retry-classifier-hook-failed"]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["classifier_index"] == 0


def test_all_classifiers_raising_composes_to_none() -> None:
    """When every classifier raises, the composition returns None (the
    declared policy governs) and never propagates, one warning each."""
    calls: list[str] = []

    def broken_one(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("one")
        raise ValueError("boom 1")

    def broken_two(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("two")
        raise ValueError("boom 2")

    composed = compose_retry_classifiers(broken_one, broken_two)

    with structlog.testing.capture_logs() as captured:
        result = composed(RuntimeError("x"), 1)

    assert result is None
    assert calls == ["one", "two"]
    warnings = [e for e in captured if e.get("event") == "retry-classifier-hook-failed"]
    assert len(warnings) == 2
    assert [w["classifier_index"] for w in warnings] == [0, 1]


def test_invalid_return_type_is_logged_and_skipped_to_next_classifier() -> None:
    """A classifier returning a non-RetryOverride (e.g. a dict) is logged
    at WARNING and skipped; composition continues with the next
    classifier, mirroring the adapter's invalid-return contract."""
    calls: list[str] = []

    def bad_return(exc: BaseException, attempt: int) -> object:
        return {"kind": "indefinite"}

    def healthy(exc: BaseException, attempt: int) -> RetryOverride | None:
        calls.append("healthy")
        return RetryOverride(kind="indefinite")

    composed = compose_retry_classifiers(bad_return, healthy)  # type: ignore[arg-type]  # Why: intentionally passing a classifier with a wrong return type to test runtime validation

    with structlog.testing.capture_logs() as captured:
        result = composed(RuntimeError("x"), 1)

    assert result == RetryOverride(kind="indefinite")
    assert calls == ["healthy"]
    warnings = [e for e in captured if e.get("event") == "retry-classifier-hook-invalid-return"]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["classifier_index"] == 0
    assert warnings[0]["return_type"] == "dict"


@pytest.mark.parametrize(
    "interrupt_exc",
    [
        KeyboardInterrupt,
        asyncio.CancelledError,
    ],
    ids=["keyboard-interrupt", "cancelled-error"],
)
def test_interrupt_grade_raise_propagates_raw(
    interrupt_exc: type[BaseException],
) -> None:
    """KeyboardInterrupt and asyncio.CancelledError are never a classifier
    outcome: they propagate raw through the composition, the same carve-out
    the adapter's single-hook boundary applies."""
    consulted: list[str] = []

    def interrupted(exc: BaseException, attempt: int) -> RetryOverride | None:
        raise interrupt_exc()

    def never_reached(exc: BaseException, attempt: int) -> RetryOverride | None:
        consulted.append("never")
        return RetryOverride(kind="indefinite")

    composed = compose_retry_classifiers(interrupted, never_reached)

    with pytest.raises(interrupt_exc):
        composed(RuntimeError("x"), 1)

    assert consulted == []
