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

import asyncio
from datetime import datetime, timedelta

import pytest
import structlog
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.exceptions import PayloadValidationError, ReservationUnavailable
from taskq.retry import (
    Fail,
    JobRetryState,
    Retry,
    RetryClassifier,
    RetryOverride,
    RetryPolicy,
    compose_retry_classifiers,
    decide_after_failure,
    rate_limit_aware_classifier,
    time_budget_as_interval,
)
from taskq.testing.actor import StubActorConfig

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


# ── rate_limit_aware_classifier: recognition set ──────────────────


class _FakeResponse:
    """Duck-typed stand-in for an httpx/httpx2/requests response."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeAiohttpResponse:
    """Duck-typed stand-in for an aiohttp response (``.status``, not ``.status_code``)."""

    def __init__(self, status: int) -> None:
        self.status = status


class _HttpxStyleError(Exception):
    """The httpx.HTTPStatusError shape: ``.response.status_code``."""

    def __init__(self, status_code: int) -> None:
        self.response = _FakeResponse(status_code)
        super().__init__(f"HTTP {status_code}")


class _AiohttpStyleError(Exception):
    """The aiohttp.ClientResponseError shape: ``.status`` on the exception."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


class _RequestsStyleError(Exception):
    """The requests.HTTPError shape: ``.response.status_code`` (same duck-type as httpx)."""

    def __init__(self, status_code: int) -> None:
        self.response = _FakeResponse(status_code)
        super().__init__(f"HTTP {status_code}")


class _ResponseStatusStyleError(Exception):
    """A response object exposing ``.status`` instead of ``.status_code``."""

    def __init__(self, status: int) -> None:
        self.response = _FakeAiohttpResponse(status)
        super().__init__(f"HTTP {status}")


class _ServiceRateLimitError(Exception):
    """A consumer-defined rate-limit exception with no status attribute at all."""


class RateLimitError(Exception):
    """The common SDK shape (openai/anthropic-style class name), no HTTP attrs."""


def test_library_rate_limit_signal_denial_maps_to_indefinite() -> None:
    """The library's own rate-limit signal, ReservationUnavailable raised
    with source='rate_limit' (a shared limiter's denial surfacing in the
    actor's frame), classifies as an indefinite override."""
    exc = ReservationUnavailable("partner-api", timedelta(seconds=30), source="rate_limit")

    result = rate_limit_aware_classifier(exc, 1)

    assert result == RetryOverride(kind="indefinite")


def test_library_concurrency_reservation_denial_does_not_map() -> None:
    """source='reservation' is a concurrency-slot denial, not a rate
    limit: the classifier returns None so the declared policy governs."""
    exc = ReservationUnavailable("worker-pool", timedelta(seconds=1), source="reservation")

    assert rate_limit_aware_classifier(exc, 1) is None


def test_httpx_shape_429_maps_to_indefinite() -> None:
    """The httpx/httpx2/requests HTTPStatusError duck-type, an exception
    carrying ``.response.status_code == 429``, classifies as indefinite."""

    result = rate_limit_aware_classifier(_HttpxStyleError(429), 1)

    assert result == RetryOverride(kind="indefinite")


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 408, 500, 502, 503])
def test_httpx_shape_non_429_status_passes_through(status_code: int) -> None:
    """Any non-429 status returns None: the declared policy governs (a 500
    keeps its bounded transient budget, a 404 its non-retryable verdict)."""

    assert rate_limit_aware_classifier(_HttpxStyleError(status_code), 1) is None


def test_aiohttp_shape_status_429_maps_to_indefinite() -> None:
    """The aiohttp.ClientResponseError duck-type, ``.status == 429`` on the
    exception itself (no ``.response`` indirection), classifies as indefinite."""

    result = rate_limit_aware_classifier(_AiohttpStyleError(429), 1)

    assert result == RetryOverride(kind="indefinite")


def test_response_object_with_status_attr_429_maps_to_indefinite() -> None:
    """The other response-object duck-type: ``.response.status == 429``
    (responses exposing ``.status`` instead of ``.status_code``)."""

    result = rate_limit_aware_classifier(_ResponseStatusStyleError(429), 1)

    assert result == RetryOverride(kind="indefinite")


def test_requests_shape_429_maps_to_indefinite() -> None:
    """The requests.HTTPError duck-type is the same ``.response.status_code``
    shape httpx exposes; it must classify identically."""

    result = rate_limit_aware_classifier(_RequestsStyleError(429), 1)

    assert result == RetryOverride(kind="indefinite")


def test_exception_named_rate_limit_error_maps_to_indefinite() -> None:
    """The common SDK shape (openai/anthropic-style): an exception whose
    class is literally named RateLimitError classifies as indefinite even
    without any HTTP status attribute."""
    result = rate_limit_aware_classifier(RateLimitError("quota exceeded"), 1)

    assert result == RetryOverride(kind="indefinite")


def test_unnamed_statusless_exception_is_not_recognized() -> None:
    """The complement of the name pin: an arbitrary statusless exception
    class with a different name is not mistaken for a rate-limit signal."""
    assert rate_limit_aware_classifier(_ServiceRateLimitError("x"), 1) is None


def test_exception_with_no_rate_limit_signal_returns_none() -> None:
    """An unrelated exception returns None: no status attributes, no
    RateLimitError name, no library signal."""
    assert rate_limit_aware_classifier(RuntimeError("x"), 1) is None
    assert rate_limit_aware_classifier(ValueError("x"), 1) is None


def test_rate_limit_override_carries_no_delay() -> None:
    """The override changes kind only: the declared policy's backoff curve
    and the schedule_to_close deadline keep governing the delay, so the
    override can never schedule past the job's own budget."""
    result = rate_limit_aware_classifier(_HttpxStyleError(429), 1)

    assert result is not None
    assert result.delay is None


def test_rate_limit_classifier_recognizes_signals_at_any_attempt() -> None:
    """Recognition is per-occurrence, not attempt-dependent: attempt 1 and
    attempt 999 classify the same."""
    assert rate_limit_aware_classifier(_HttpxStyleError(429), 1) is not None
    assert rate_limit_aware_classifier(_HttpxStyleError(429), 999) is not None


# ── rate_limit_aware_classifier: property shape ───────────────────


@given(status=st.integers(min_value=100, max_value=599))
@settings(max_examples=200)
def test_property_http_status_shape_recognition(status: int) -> None:
    """Property: the httpx-shaped duck-type recognizes exactly 429 and
    passes through every other status the declared policy's way."""
    result = rate_limit_aware_classifier(_HttpxStyleError(status), 1)

    if status == 429:
        assert result == RetryOverride(kind="indefinite")
    else:
        assert result is None


@given(
    payload=st.one_of(
        st.integers(),
        st.text(),
        st.booleans(),
        st.none(),
    )
)
@settings(max_examples=100)
def test_property_unrelated_exception_shapes_compose_to_none(payload: object) -> None:
    """Property: classifiers that return None for arbitrary payloads compose
    to None, whatever the exception payload; the declared policy governs."""
    exc = RuntimeError(f"payload: {payload!r}")

    composed = compose_retry_classifiers(rate_limit_aware_classifier)

    assert composed(exc, 1) is None


# ── adapter interaction: the 429-burns-the-budget trap, fixed ─────


def test_transient_policy_with_429_override_retries_past_max_attempts() -> None:
    """THE trap this helper fixes: a declared transient policy burns its
    attempt budget on 429s (attempt >= max_attempts lands Fail). With the
    built-in classifier composed in, the same occurrence at the same
    attempt is an indefinite override, so the failure path still retries
    with the policy's own backoff. The attempt ceiling is gone; the only
    remaining bound is the job's schedule_to_close, arbitrated in SQL —
    which a transient actor never has stamped (see the haunt pin below)."""
    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(rate_limit_aware_classifier),
    )
    job_state = _job_state(attempt=2, max_attempts=2, retry_kind="transient")

    decision = decide_after_failure(actor_config, _HttpxStyleError(429), job_state)

    assert isinstance(decision, Retry), "429 must not burn the transient attempt budget"


def test_transient_policy_governs_when_no_signal_matches() -> None:
    """The complement: the same setup with a non-rate-limit exception at
    attempt >= max_attempts still Fails, the declared budget enforced."""
    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(rate_limit_aware_classifier),
    )
    job_state = _job_state(attempt=2, max_attempts=2, retry_kind="transient")

    decision = decide_after_failure(actor_config, _HttpxStyleError(500), job_state)

    assert isinstance(decision, Fail)


def test_policy_backoff_delays_the_429_retry() -> None:
    """No delay override: the 429 retry lands on the declared policy's own
    backoff curve (jitter=0 pins the exact value), never on a
    server-supplied value that could outrun schedule_to_close."""
    policy = RetryPolicy(
        kind="transient",
        max_attempts=3,
        base=timedelta(seconds=5),
        jitter=0.0,
    )
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=rate_limit_aware_classifier,
    )

    decision = decide_after_failure(actor_config, _HttpxStyleError(429), _job_state())

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=5)


def test_composed_classifier_wins_over_declared_transient_for_library_signal() -> None:
    """The library's own rate-limit signal through the full adapter path:
    ReservationUnavailable(source='rate_limit') at attempt == max_attempts
    retries instead of failing."""
    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(rate_limit_aware_classifier),
    )
    job_state = _job_state(attempt=2, max_attempts=2, retry_kind="transient")
    exc = ReservationUnavailable("partner-api", timedelta(seconds=30), source="rate_limit")

    decision = decide_after_failure(actor_config, exc, job_state)

    assert isinstance(decision, Retry)


# ── the unbounded haunt: an indefinite override needs a deadline ──


def test_haunt_transient_without_deadline_retries_past_any_attempt() -> None:
    """HAZARD PIN: a transient policy with no schedule_to_close (the
    default: time_budget is only honored for indefinite-declared policies,
    so the enqueue path stamps no deadline) composed with the built-in
    retries a sustained 429 past ANY attempt number — no attempt ceiling
    and no wall-clock bound. Documented in retries.md as the hazard of
    composing this built-in into a transient actor; this test pins that
    the hazard is real so the docs cannot drift from the behaviour."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(rate_limit_aware_classifier),
    )
    job_state = _job_state(attempt=1_000_000, max_attempts=3, retry_kind="transient")

    decision = decide_after_failure(actor_config, _HttpxStyleError(429), job_state)

    assert isinstance(decision, Retry), (
        "the haunt is documented: no attempt ceiling and no deadline means "
        "the 429 path never terminates"
    )


def test_time_budget_deadline_is_stamped_only_for_indefinite_declared_policies() -> None:
    """The bounded alternative to the haunt: a deadline exists (and the
    enqueue path stamps it as schedule_to_close) only when the actor is
    declared kind='indefinite' with a time_budget. A transient policy's
    time_budget is dropped at enqueue — this is why the haunt above has
    no stopping condition."""
    assert time_budget_as_interval(
        RetryPolicy(kind="indefinite", time_budget=timedelta(hours=1))
    ) == timedelta(hours=1)
    # A transient policy's time_budget is silently dropped (registration
    # warns actor-config-time-budget-ignored); no deadline is ever stamped.
    assert (
        time_budget_as_interval(RetryPolicy(kind="transient", time_budget=timedelta(hours=1)))
        is None
    )


def test_name_match_false_positive_is_an_indefinite_override() -> None:
    """The name match requires nothing — no HTTP attributes, no 429 — so
    an unrelated domain error that merely carries the name RateLimitError
    is overridden to indefinite too. Pinned because retries.md documents
    this as the loosest signal: the HTTP shapes at least require a 429;
    this one requires only the class name."""
    # A consumer's unrelated payments-domain class; the local definition
    # shadows the test module's SDK-shape RateLimitError on purpose: the
    # classifier matches on the class NAME alone.

    class RateLimitError(Exception):
        """A payments-domain error that happens to carry the name."""

    override = rate_limit_aware_classifier(RateLimitError("payment gateway closed"), 1)

    assert override == RetryOverride(kind="indefinite")


def test_trap_math_default_transient_dies_after_two_delays_fifteen_seconds() -> None:
    """Pins the trap arithmetic retries.md quotes: with the defaults
    (max_attempts=3, base=5s, exponential) a 429 storm costs two delays,
    5s + 10s = 15s — the 20s rung is never reached because the third
    failure is terminal and schedules no delay."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)

    decisions = [
        RetryClassifier.classify(
            policy=policy,
            non_retryable_exceptions=(),
            exception=RuntimeError("429-ish"),
            attempt=attempt,
            override=None,
        )
        for attempt in (1, 2, 3)
    ]

    assert isinstance(decisions[0], Retry) and decisions[0].retry_delay == timedelta(seconds=5)
    assert isinstance(decisions[1], Retry) and decisions[1].retry_delay == timedelta(seconds=10)
    assert isinstance(decisions[2], Fail), "attempt 3 is terminal: no 20s delay exists"


def test_hook_override_cannot_resurrect_unconditional_fail_classes() -> None:
    """SEAM ORDER: a hook — single or composed — is never consulted for
    the adapter's unconditional-Fail classes, and even an override handed
    straight to classify() cannot resurrect them: the isinstance checks
    run before the override is applied. Matches retries.md's claim that
    those classes 'still win over the whole composition'."""
    hook_saw: list[BaseException] = []

    def greedy(exc: BaseException, attempt: int) -> RetryOverride | None:
        hook_saw.append(exc)
        return RetryOverride(kind="non_retryable")

    payload_error = PayloadValidationError("bad payload")
    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        retry_classifier=greedy,
    )

    decision = decide_after_failure(actor_config, payload_error, _job_state())

    assert hook_saw == [], "the adapter must gate the hook on the Fail classes"
    assert isinstance(decision, Fail) and decision.error_class == "PayloadValidationError"

    # The classify()-level belt-and-braces: the override is a parameter
    # there, and the Fail classes still win.
    direct = RetryClassifier.classify(
        policy=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        non_retryable_exceptions=(),
        exception=payload_error,
        attempt=1,
        override=RetryOverride(kind="non_retryable"),
    )
    assert isinstance(direct, Fail) and direct.error_class == "PayloadValidationError"
