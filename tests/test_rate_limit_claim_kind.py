"""``make_rate_limit_aware_classifier``: the rate-limit built-in's claim
kind is configurable.

The adoption blocker this fixes: ``rate_limit_aware_classifier`` returned
``RetryOverride(kind="indefinite")`` UNCONDITIONALLY on a claimed 429, so
a consumer whose actors are ``transient`` with no ``time_budget`` (the
warden's six media jobs) met a sustained 429 storm with indefinite
retries and no deadline — the haunt the §5 danger block documents — and
could not say "429 → BOUNDED retry" without hand-rolling a classifier.
The factory makes the claim a construction-time knob:

* ``claim_kind="indefinite"`` (default) — the built-in, unchanged;
* ``claim_kind="transient"`` — ``RetryOverride(kind="transient",
  delay=hint)``: ``max_attempts`` stays the stopper, the hint sets when;
* ``claim_kind=None`` — the classifier never claims (identity for
  composition).

The parsing, the garbage rules, and the bounds are SHARED across modes
(never forked). This file pins: the warden-shaped red/green storm, every
mode's exact override shape, the identity of the built-in with the
factory at defaults, the verbatim-delay invariant, and the parser's
finite-hint grammar (huge-but-finite clamps to the operator's ceiling —
it is not garbage; decimal fractions are honored; the rest of the
grammar deliberately stays closed).
"""

import email.utils
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.exceptions import ReservationUnavailable
from taskq.retry import (
    Fail,
    JobRetryState,
    Retry,
    RetryClassifierHook,
    RetryKind,
    RetryOverride,
    RetryPolicy,
    decide_after_failure,
    make_rate_limit_aware_classifier,
    rate_limit_aware_classifier,
)
from taskq.testing.actor import StubActorConfig

_NOW = datetime(2026, 1, 1, tzinfo=UTC)

#: The fix under test, at the bounded mode.
_BOUNDED = make_rate_limit_aware_classifier(claim_kind="transient")

#: The factory at defaults — the identity-pin twin of the built-in.
_FACTORY_DEFAULT = make_rate_limit_aware_classifier()


class _Response:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers if headers is not None else {}


class _HttpxStyleError(Exception):
    """The httpx.HTTPStatusError / requests.HTTPError shape."""

    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.response = _Response(status_code, headers)
        super().__init__(f"HTTP {status_code}")


class _AiohttpStyleError(Exception):
    """The aiohttp.ClientResponseError shape: bare ``.headers`` + ``.status``."""

    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.headers = headers if headers is not None else {}
        super().__init__(f"HTTP {status}")


class _NamedRateLimitError(Exception):
    """The openai/anthropic SDK shape: the class NAME is the signal."""

    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers if headers is not None else {}
        super().__init__("rate limited")


def _with_retry_after(value: str) -> _HttpxStyleError:
    return _HttpxStyleError(429, {"Retry-After": value})


def _http_date(offset: timedelta, *, base: datetime = _NOW) -> str:
    return email.utils.format_datetime(base + offset, usegmt=True)


def _job_state(*, attempt: int = 1, max_attempts: int = 3) -> JobRetryState:
    return JobRetryState(
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )


def _warden_policy() -> RetryPolicy:
    """The warden's shape: a transient policy, no ``time_budget`` (a
    transient actor's budget is dropped at enqueue anyway) — the config
    the rev lane was disqualified on."""
    return RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)


# ── the warden-shaped red/green: the storm, bounded at last ────────


def test_warden_red_builtin_cannot_bound_the_storm() -> None:
    """RED (the adoption blocker, the haunt pin's shape replayed as a
    storm walk): warden-shaped config — a transient policy, no
    time_budget, the BUILT-IN — meets a sustained 429 storm and the
    decision ladder NEVER terminates. The built-in cannot express
    '429 → bounded'; every attempt retries, past max_attempts forever."""
    actor = StubActorConfig(
        retry=_warden_policy(),
        retry_classifier=rate_limit_aware_classifier,
    )
    storm = _HttpxStyleError(429)

    for attempt in range(1, 40):
        decision = decide_after_failure(actor, storm, _job_state(attempt=attempt, max_attempts=3))
        assert isinstance(decision, Retry), (
            f"attempt {attempt}: the built-in's indefinite claim keeps the "
            "storm alive — the haunt, exactly as documented"
        )


def test_warden_green_transient_mode_terminates_with_hint_honored() -> None:
    """GREEN: the SAME storm through the factory's transient mode
    terminates at max_attempts, and every retry along the way carries the
    server's hint end-to-end (jitter=0.0: the hint's own number, subject
    to the documented bounds — the 24 h default ceiling above it, the
    MIN_DEFERRAL_INTERVAL floor below it)."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=_BOUNDED)
    storm = _with_retry_after("90")

    for attempt in (1, 2):
        decision = decide_after_failure(actor, storm, _job_state(attempt=attempt, max_attempts=3))
        assert isinstance(decision, Retry), f"attempt {attempt} must retry"
        assert decision.retry_delay == timedelta(seconds=90), "the hint's delay honored end-to-end"

    decision = decide_after_failure(actor, storm, _job_state(attempt=3, max_attempts=3))
    assert isinstance(decision, Fail), "max_attempts is the stopper in transient mode"


def test_warden_green_transient_mode_terminates_along_the_curve_fallback() -> None:
    """GREEN, hintless variant: a 429 with no Retry-After header gets the
    kind-only override (the declared curve computes *when* — 5 s base,
    jitter=0) and max_attempts still stops the storm."""
    policy = RetryPolicy(kind="transient", max_attempts=3, base=timedelta(seconds=5), jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=_BOUNDED)
    storm = _HttpxStyleError(429)

    decision = decide_after_failure(actor, storm, _job_state(attempt=1, max_attempts=3))
    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=5)

    decision = decide_after_failure(actor, storm, _job_state(attempt=3, max_attempts=3))
    assert isinstance(decision, Fail)


# ── mode pins: each claim_kind's exact override shape ──────────────


@pytest.mark.parametrize(
    ("classifier", "kind"),
    [
        (rate_limit_aware_classifier, "indefinite"),
        (_BOUNDED, "transient"),
    ],
    ids=["builtin-indefinite", "factory-transient"],
)
class TestModeOverrideShapes:
    """The exact override shape per mode — kind plus delay presence — so
    reverting the factory's wiring turns these red. Every hint here is
    seconds-form, so no clock injection is needed (the date form is
    pinned in test_retry_after_header_sniffing.py; the parse is shared,
    not forked)."""

    def test_hint_parsed_to_delay(self, classifier: RetryClassifierHook, kind: RetryKind) -> None:
        result = classifier(_with_retry_after("90"), 1)
        assert result == RetryOverride(kind=kind, delay=timedelta(seconds=90))

    def test_hint_seconds_and_x_form(
        self, classifier: RetryClassifierHook, kind: RetryKind
    ) -> None:
        result = classifier(_HttpxStyleError(429, {"X-Retry-After": "45"}), 1)
        assert result == RetryOverride(kind=kind, delay=timedelta(seconds=45))

    def test_no_header_kind_only(self, classifier: RetryClassifierHook, kind: RetryKind) -> None:
        result = classifier(_HttpxStyleError(429), 1)
        assert result == RetryOverride(kind=kind)
        assert result is not None and result.delay is None

    def test_library_rate_limit_signal(
        self, classifier: RetryClassifierHook, kind: RetryKind
    ) -> None:
        exc = ReservationUnavailable("partner-api", timedelta(seconds=30), source="rate_limit")
        result = classifier(exc, 1)
        assert result == RetryOverride(kind=kind)

    def test_reservation_signal_not_recognized(
        self, classifier: RetryClassifierHook, kind: RetryKind
    ) -> None:
        exc = ReservationUnavailable("pool", timedelta(seconds=30), source="reservation")
        assert classifier(exc, 1) is None

    def test_garbage_hint_degrades_the_delay_never_the_kind(
        self, classifier: RetryClassifierHook, kind: RetryKind
    ) -> None:
        """The garbage rules are IDENTICAL across modes (the parsing is
        shared, not forked): zero / negative-shape / unparsable degrade
        to the kind-only override — the curve fallback."""
        for value in ("0", "0.0", "-30", "", "soon", "12 34"):
            result = classifier(_with_retry_after(value), 1)
            assert result == RetryOverride(kind=kind), f"{value!r} must curve-fallback"

    def test_unrecognized_signals_return_none(
        self, classifier: RetryClassifierHook, kind: RetryKind
    ) -> None:
        assert classifier(_HttpxStyleError(500), 1) is None
        assert classifier(_HttpxStyleError(404), 1) is None
        assert classifier(RuntimeError("x"), 1) is None


def test_mode_none_never_claims() -> None:
    """``claim_kind=None``: the identity for composition — the classifier
    returns None for every input, recognized signals included."""
    identity = make_rate_limit_aware_classifier(claim_kind=None)
    recognized = [
        _with_retry_after("90"),
        _with_retry_after("0"),
        _HttpxStyleError(429),
        ReservationUnavailable("partner-api", timedelta(seconds=30), source="rate_limit"),
        _NamedRateLimitError(),
    ]
    for exc in recognized:
        for attempt in (1, 50):
            assert identity(exc, attempt) is None, "the None mode must never claim"


@pytest.mark.parametrize("bad", ["bounded", "INDEFINITE", "transient ", 429, object()])
def test_fail_loud_on_garbage_claim_kind(bad: object) -> None:
    """A claim kind outside the three modes raises at CONSTRUCTION —
    fail loud at build time, never a silently-misclaiming classifier at
    override time."""
    with pytest.raises(ValueError, match="claim_kind"):
        make_rate_limit_aware_classifier(bad)  # type: ignore[arg-type]  # Why: the test feeds deliberately off-contract values to pin the fail-loud validation


# ── the identity pin: the built-in IS the factory at defaults ──────


_IDENTITY_BATTERY: list[BaseException] = [
    _HttpxStyleError(429),
    _with_retry_after("120"),
    _with_retry_after("0"),
    _with_retry_after("0.5"),
    _with_retry_after("-30"),
    _with_retry_after(""),
    _with_retry_after("soon"),
    _with_retry_after("86400"),
    _with_retry_after("100000"),
    _with_retry_after("1" + "0" * 20),
    _HttpxStyleError(429, {"X-Retry-After": "45"}),
    _AiohttpStyleError(429, {"Retry-After": "90"}),
    _NamedRateLimitError(),
    _NamedRateLimitError({"Retry-After": "15"}),
    _HttpxStyleError(500),
    _HttpxStyleError(404),
    RuntimeError("no signal at all"),
    ReservationUnavailable("partner-api", timedelta(seconds=30), source="rate_limit"),
    ReservationUnavailable("pool", timedelta(seconds=30), source="reservation"),
]
# (The battery is seconds-form only: the HTTP-date parse reads the clock,
# and the identity pin runs both classifiers against the SAME inputs
# without injection. The date form's identity is structural — the two
# share one parse — and its behavior is pinned in
# test_retry_after_header_sniffing.py.)


def test_identity_builtin_equals_factory_at_defaults() -> None:
    """IDENTITY PIN: the module-level built-in and the factory at
    defaults produce IDENTICAL decisions over the whole recognized-signal
    surface — the built-in is exactly ``make_rate_limit_aware_classifier()``,
    so zero behavior changed and every existing pin carries over."""
    for exc in _IDENTITY_BATTERY:
        for attempt in (1, 7, 999):
            built = rate_limit_aware_classifier(exc, attempt)
            factory = _FACTORY_DEFAULT(exc, attempt)
            assert built == factory, f"{exc!r} at attempt {attempt}"


@settings(max_examples=100)
@given(attempt=st.integers(min_value=1, max_value=2**20))
def test_identity_builtin_equals_factory_at_defaults_any_attempt(attempt: int) -> None:
    """The identity holds at any attempt number (the classifier never
    reads it — but the pin keeps a signature drift honest)."""
    exc = _with_retry_after("90")
    assert rate_limit_aware_classifier(exc, attempt) == _FACTORY_DEFAULT(exc, attempt)


def test_factory_hooks_compose() -> None:
    """A factory-built classifier is a hook like any other: it composes,
    first-override-wins, and the None-mode instance contributes nothing."""
    from taskq.retry import compose_retry_classifiers

    composed = compose_retry_classifiers(_BOUNDED, make_rate_limit_aware_classifier(None))
    result = composed(_with_retry_after("90"), 1)

    assert result == RetryOverride(kind="transient", delay=timedelta(seconds=90))
    assert composed(_HttpxStyleError(500), 1) is None


# ── the verbatim-delay invariant (transient mode) ──────────────────


def test_verbatim_hint_at_classifier_level() -> None:
    """The override delay IS the hint — the classifier hands the server's
    number through untouched (90 s in, 90 s out)."""
    result = _BOUNDED(_with_retry_after("90"), 1)

    assert result == RetryOverride(kind="transient", delay=timedelta(seconds=90))


def test_verbatim_hint_end_to_end() -> None:
    """End-to-end through the decision ladder (jitter=0.0 policy): the
    retry lands exactly on the hint — the documented ceiling (the 24 h
    default, far above) and floor (MIN_DEFERRAL_INTERVAL, far below) do
    not touch a human-scale hint."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=_BOUNDED)

    decision = decide_after_failure(
        actor, _with_retry_after("90"), _job_state(attempt=1, max_attempts=3)
    )

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=90)


def test_verbatim_hint_clamped_by_max_retry_backoff_end_to_end() -> None:
    """The ceiling is the operator's knob and it bounds the transient
    mode's hint exactly as it bounds the built-in's: hint 90 s against
    ``max_retry_backoff=60 s`` → Retry(60 s)."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=_BOUNDED)

    decision = decide_after_failure(
        actor,
        _with_retry_after("90"),
        _job_state(attempt=1, max_attempts=3),
        max_retry_backoff=timedelta(seconds=60),
    )

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=60)


# ── parser alignment 1: huge-but-finite hints CLAMP, not fall back ─


def test_huge_but_finite_hint_is_honored_verbatim_at_classifier_level() -> None:
    """A finite hint beyond the old one-day parse cap is a DELAY, not
    garbage: 100000 s (~27.8 h) flows through verbatim. Bounding it is
    the operator's ``max_retry_backoff`` ceiling's job — the ceiling is
    the knob that exists to express exactly this, so a hard-coded parse
    cap would steal the clamp from the only place an operator can tune
    it. (The old behavior treated the hint as garbage and curve-fell-
    back — red before the fix.)"""
    result = rate_limit_aware_classifier(_with_retry_after("100000"), 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=100000))


def test_huge_hint_saturates_never_overflows() -> None:
    """A '1e20'-class hint (beyond timedelta's representable range)
    saturates at the documented parse-level bound instead of crashing a
    classifier with OverflowError — the saturation is not a semantic
    cap, it only keeps the parse crash-free."""
    huge = "1" + "0" * 20

    result = rate_limit_aware_classifier(_with_retry_after(huge), 1, now=_NOW)

    assert result is not None
    assert result.delay == timedelta.max


def test_warden_huge_hint_clamps_to_max_retry_backoff_end_to_end() -> None:
    """WARDEN PIN: ``max_retry_backoff=120 s`` + a '1e20'-class hint →
    the decision is Retry(EXACTLY 120 s) — the ceiling clamping an
    oversized hint, not the curve fallback the old parse produced."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=rate_limit_aware_classifier)

    decision = decide_after_failure(
        actor,
        _with_retry_after("1" + "0" * 20),
        _job_state(attempt=1, max_attempts=3),
        max_retry_backoff=timedelta(seconds=120),
    )

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=120), (
        "an oversized-but-finite hint clamps to the operator's ceiling; "
        "curve fallback (the old behavior) would land on the 5 s base"
    )


def test_http_date_beyond_ceiling_clamps_end_to_end() -> None:
    """The HTTP-date twin: a date hint beyond the ceiling parses to its
    delay verbatim (classifier level) and CLAMPS to the ceiling on the
    decision path — the date form and the seconds form share one rule."""
    ten_days_out = _http_date(timedelta(days=10))

    classifier_level = rate_limit_aware_classifier(_with_retry_after(ten_days_out), 1, now=_NOW)
    assert classifier_level == RetryOverride(kind="indefinite", delay=timedelta(days=10))

    # End-to-end: the classifier's default clock reads now(); a date ten
    # days out from the real clock parses to ~ten days and clamps.
    real_soon = email.utils.format_datetime(datetime.now(UTC) + timedelta(days=10), usegmt=True)
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=rate_limit_aware_classifier)

    decision = decide_after_failure(
        actor,
        _with_retry_after(real_soon),
        _job_state(attempt=1, max_attempts=3),
        max_retry_backoff=timedelta(seconds=120),
    )

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=120)


# ── parser alignment 2: the decimal-fraction grammar ───────────────


def test_sub_second_decimal_hint_accepted() -> None:
    """``"0.5"`` is a valid seconds-fraction: the parse honors 0.5 s
    (red before the fix — int() curve-fell-back on it)."""
    result = rate_limit_aware_classifier(_with_retry_after("0.5"), 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=0.5))


def test_decimal_fraction_end_to_end_floored_at_min_deferral() -> None:
    """The 0.5 s hint end-to-end: the decision floor
    (MIN_DEFERRAL_INTERVAL = 1 s) is the monopolisation bound — the
    sub-second hint cannot degenerate the failure cycle into a
    no-period loop."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor = StubActorConfig(retry=policy, retry_classifier=rate_limit_aware_classifier)

    decision = decide_after_failure(actor, _with_retry_after("0.5"), _job_state())

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=1)


@pytest.mark.parametrize("value", ["30.5", "1.25", "00.5"])
def test_decimal_fraction_grammar_honored(value: str) -> None:
    """The fraction part of the grammar: digits, an optional dot, more
    digits — the parsed value is the delay."""
    result = rate_limit_aware_classifier(_with_retry_after(value), 1, now=_NOW)

    assert result is not None
    assert result.delay == timedelta(seconds=float(value))


@pytest.mark.parametrize("value", ["1,5", "1e3", "+30", "-30", "12 34", "NaN", "soon", "0x10"])
def test_grammar_limits_are_garbage_and_fall_back_to_the_curve(value: str) -> None:
    """The grammar stays deliberately closed — a comma decimal, scientific
    notation, signs, embedded whitespace, hex — every non-grammar value
    degrades to the kind-only override (the curve fallback), never to a
    bogus delay and never to a crash."""
    result = rate_limit_aware_classifier(_with_retry_after(value), 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite")
    assert result is not None and result.delay is None


def test_zero_hint_keeps_the_curve_fallback_rule() -> None:
    """The zero rule is unchanged by both parser alignments: a parsed
    zero is garbage (the monopolisation hazard the decision floor exists
    to prevent), so it curve-falls-back — while the DECISION path's
    explicit-zero floor (an override carrying delay=timedelta(0) is
    honored as 'as fast as the deferral floor allows') remains the
    decision path's rule, untouched by the parse."""
    result = rate_limit_aware_classifier(_with_retry_after("0"), 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite")
    assert result is not None and result.delay is None


# ── property: the parse can never crash, only degrade ──────────────


@settings(max_examples=300)
@given(
    value=st.one_of(
        st.integers(min_value=-(2**200), max_value=2**200).map(str),
        st.text(alphabet="0123456789 -:.GMUTWabcdefghiklmnoprstuy,", max_size=40),
    )
)
def test_property_any_header_value_never_crashes_and_stays_finite(value: str) -> None:
    """Property: for ANY header value, a claimed 429 always classifies,
    and the delay — when present — is a finite positive timedelta. No
    input can crash the classifier (the OverflowError saturation), and
    garbage degrades to delay=None, never to a bogus delay."""
    result = rate_limit_aware_classifier(_with_retry_after(value), 1, now=_NOW)

    assert result is not None, "a claimed 429 must always classify"
    assert result.kind == "indefinite"
    assert result.delay is None or (result.delay > timedelta(0) and result.delay <= timedelta.max)
