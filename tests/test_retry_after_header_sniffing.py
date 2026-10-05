"""Retry-After / X-Retry-After header sniffing in rate_limit_aware_classifier.

The 429 classifier claimed the signal but ignored the server's delay: a
provider that answers ``429`` with ``Retry-After: 120`` got the declared
policy's exponential curve (15s to death on the defaults) instead of the
one number the server explicitly asked us to wait. This suite pins the
sniffing contract:

* duck-typed header containers — ``exception.response.headers`` (the
  httpx/httpx2/requests shape) and a bare ``exception.headers`` (the
  aiohttp ``ClientResponseError`` shape, or any exception carrying
  headers directly);
* header names ``retry-after`` and ``x-retry-after``, case-insensitive;
* value forms: seconds-integer and HTTP-date
  (``email.utils.parsedate_to_datetime``);
* the citizen rules: ``Retry-After: 0`` / garbage / negative / absurd
  (beyond one day) fall back to the curve (a kind-only override); a
  recognized delay lands in ``RetryOverride.delay`` and is therefore
  clamped by ``max_retry_backoff`` and floored by
  ``MIN_DEFERRAL_INTERVAL`` downstream — the classifier itself adds no
  second ceiling;
* no header = the pre-sniffing behavior exactly (kind-only override);
  no header sniffing at all happens on an unclaimed signal.

The deadline hazard (§5's danger block) is restated in the classifier's
docstring and pinned by tests/test_retry_classifier_composition.py; the
delay changes *when* the indefinite job retries, never *whether it
stops*.
"""

import email.utils
import tracemalloc
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.retry import (
    _MAX_RETRY_AFTER_DELAY,
    JobRetryState,
    Retry,
    RetryOverride,
    RetryPolicy,
    _parse_retry_after,
    decide_after_failure,
    rate_limit_aware_classifier,
)
from taskq.testing.actor import StubActorConfig

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


# ── duck-typed exception shapes ───────────────────────────────────


class _Response:
    """Duck-typed httpx/requests response: status_code + headers."""

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


class _CaseInsensitiveHeaders(dict[str, str]):
    """Minimal case-insensitive mapping, the real header containers' shape."""

    def get(self, key: object, default: object = None) -> object:  # type: ignore[override]
        target = str(key).lower()
        for k, v in self.items():
            if k.lower() == target:
                return v
        return default


class _HeaderedRateLimitError(Exception):
    """The SDK RateLimitError name carrying response headers directly."""

    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers
        self.response = _Response(429, headers)
        super().__init__("rate limited")


def _with_retry_after(value: str, *, name: str = "Retry-After") -> _HttpxStyleError:
    return _HttpxStyleError(429, {name: value})


def _job_state(*, attempt: int = 1, max_attempts: int = 3) -> JobRetryState:
    return JobRetryState(
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )


def _actor_with(policy: RetryPolicy) -> StubActorConfig:
    return StubActorConfig(retry=policy, retry_classifier=rate_limit_aware_classifier)


# ── recognition: header names, containers, case ───────────────────


def test_429_with_retry_after_seconds_header_carries_delay() -> None:
    """A seconds-integer Retry-After on a claimed 429 becomes the override
    delay: the server said 'come back in 120s', the classifier passes that
    on instead of the policy's 15s-to-death curve."""
    exc = _with_retry_after("120")

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=120))


def test_429_with_x_retry_after_header_carries_delay() -> None:
    """The X-Retry-After de-facto header is honored the same way."""
    exc = _with_retry_after("45", name="X-Retry-After")

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=45))


@pytest.mark.parametrize(
    ("header_name", "value"),
    [
        ("retry-after", "30"),
        ("RETRY-AFTER", "30"),
        ("Retry-After", "30"),
        ("x-retry-after", "30"),
        ("X-RETRY-AFTER", "30"),
    ],
    ids=["lower", "upper", "title", "x-lower", "x-upper"],
)
def test_header_name_lookup_is_case_insensitive(header_name: str, value: str) -> None:
    """Header names match case-insensitively, including on a plain dict
    (no case-insensitive mapping type required)."""
    exc = _with_retry_after(value, name=header_name)

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result is not None and result.delay == timedelta(seconds=30)


def test_retry_after_wins_over_x_retry_after() -> None:
    """Both headers present: the standard Retry-After is read first."""
    exc = _HttpxStyleError(429, {"Retry-After": "10", "X-Retry-After": "999"})

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result is not None and result.delay == timedelta(seconds=10)


def test_aiohttp_shape_bare_headers_on_the_exception() -> None:
    """aiohttp's ClientResponseError carries ``.headers`` directly on the
    exception (no ``.response`` indirection) — the same sniffing applies."""
    exc = _AiohttpStyleError(429, {"Retry-After": "90"})

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=90))


def test_case_insensitive_header_container_shape() -> None:
    """A real header container (httpx.Headers / CaseInsensitiveDict shape):
    the ``.get`` fast path reads it, lowercase name against mixed-case map."""
    exc = Exception("x")
    exc.response = _Response(429)  # type: ignore[attr-defined]
    exc.response.headers = _CaseInsensitiveHeaders({"RETRY-AFTER": "60"})  # type: ignore[attr-defined]

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result is not None and result.delay == timedelta(seconds=60)


def test_ratelimiterror_named_exception_with_headers_honours_retry_after() -> None:
    """The name-matched SDK shape (no usable status attribute path) still
    sniffs a bare ``.headers``: openai-style RateLimitErrors carry them."""
    exc = _HeaderedRateLimitError({"Retry-After": "15"})

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=15))


# ── citizen rules: garbage falls back to the curve ────────────────


class TestGarbageFallsBackToCurve:
    """Every unusable value degrades to the kind-only override: the
    declared policy's curve (jitter, cap, max_retry_backoff) computes the
    delay, exactly the pre-sniffing behavior."""

    def test_zero(self) -> None:
        result = rate_limit_aware_classifier(_with_retry_after("0"), 1, now=_NOW)
        assert result is not None
        assert result == RetryOverride(kind="indefinite")
        assert result.delay is None

    def test_negative(self) -> None:
        result = rate_limit_aware_classifier(_with_retry_after("-30"), 1, now=_NOW)
        assert result is not None
        assert result == RetryOverride(kind="indefinite")
        assert result.delay is None

    def test_empty(self) -> None:
        result = rate_limit_aware_classifier(_with_retry_after(""), 1, now=_NOW)
        assert result is not None
        assert result == RetryOverride(kind="indefinite")
        assert result.delay is None

    @pytest.mark.parametrize("value", ["soon", "30.5", "abc", "12 34", "NaN"])
    def test_garbage_text(self, value: str) -> None:
        result = rate_limit_aware_classifier(_with_retry_after(value), 1, now=_NOW)
        assert result is not None
        assert result == RetryOverride(kind="indefinite")
        assert result.delay is None

    def test_absurd_beyond_one_day(self) -> None:
        """100000s is ~27.8h, beyond the one-day parse cap: curve fallback
        (the downstream max_retry_backoff clamp stays as defense-in-depth)."""
        result = rate_limit_aware_classifier(_with_retry_after("100000"), 1, now=_NOW)
        assert result is not None
        assert result == RetryOverride(kind="indefinite")
        assert result.delay is None

    def test_exactly_one_day_is_honoured(self) -> None:
        """The cap boundary: exactly 86400s is not 'beyond one day'."""
        result = rate_limit_aware_classifier(_with_retry_after("86400"), 1, now=_NOW)
        assert result is not None and result.delay == timedelta(days=1)


# ── HTTP-date form ────────────────────────────────────────────────


def _http_date(dt: datetime) -> str:
    return email.utils.format_datetime(dt, usegmt=True)


def test_http_date_within_cap_becomes_delay() -> None:
    """The IMF-fixdate form: the delay is date minus now, via the injected
    clock (the classifier's only clock read)."""
    header = _http_date(_NOW + timedelta(seconds=90))

    result = rate_limit_aware_classifier(_with_retry_after(header), 1, now=_NOW)

    assert result == RetryOverride(kind="indefinite", delay=timedelta(seconds=90))


def test_http_date_in_the_past_falls_back_to_curve() -> None:
    header = _http_date(_NOW - timedelta(seconds=90))

    result = rate_limit_aware_classifier(_with_retry_after(header), 1, now=_NOW)

    assert result is not None
    assert result == RetryOverride(kind="indefinite")
    assert result.delay is None


def test_http_date_beyond_one_day_falls_back_to_curve() -> None:
    header = _http_date(_NOW + timedelta(days=2))

    result = rate_limit_aware_classifier(_with_retry_after(header), 1, now=_NOW)

    assert result is not None
    assert result == RetryOverride(kind="indefinite")
    assert result.delay is None


def test_parse_helper_assumes_utc_for_naive_dates() -> None:
    """parsedate_to_datetime returns naive for the '-0000' zone: the parse
    treats it as UTC rather than crashing on the subtraction."""
    delay = _parse_retry_after("Wed, 01 Jan 2026 00:01:00 -0000", now=_NOW)
    assert delay == timedelta(seconds=60)


# ── no header = today's behavior; no sniffing on unclaimed signals ──


def test_no_header_keeps_kind_only_override() -> None:
    """A 429 without headers is exactly the pre-sniffing override."""
    result = rate_limit_aware_classifier(_HttpxStyleError(429), 1, now=_NOW)

    assert result is not None
    assert result == RetryOverride(kind="indefinite")
    assert result.delay is None


def test_no_header_sniffing_on_unclaimed_signals() -> None:
    """A non-429 with a Retry-After header is not claimed: the classifier
    returns None (the parse path runs only on a claimed signal)."""
    exc_500 = _HttpxStyleError(500, {"Retry-After": "30"})
    exc_404 = _AiohttpStyleError(404, {"Retry-After": "30"})

    assert rate_limit_aware_classifier(exc_500, 1, now=_NOW) is None
    assert rate_limit_aware_classifier(exc_404, 1, now=_NOW) is None


# ── hypothesis: the delay can never leave the band ────────────────


@settings(max_examples=300)
@given(
    value=st.one_of(
        st.integers(min_value=-(2**63), max_value=2**63).map(str),
        st.text(alphabet="0123456789 -:.GMUTWabcdefghiklmnoprstuy,", max_size=40),
    )
)
def test_property_header_value_never_yields_out_of_band_delay(value: str) -> None:
    """Property: for ANY header value, a claimed 429 stays an indefinite
    override and the delay, when present, lies in (0, one day] — garbage
    must degrade to the curve (delay None), never to a bogus delay."""
    exc = _with_retry_after(value)

    result = rate_limit_aware_classifier(exc, 1, now=_NOW)

    assert result is not None, "a claimed 429 must always classify"
    assert result.kind == "indefinite"
    assert result.delay is None or (
        result.delay > timedelta(0) and result.delay <= _MAX_RETRY_AFTER_DELAY
    )


# ── end-to-end: the delay through the adapter, and its bounds ─────


def test_retry_after_delay_flows_through_adapter() -> None:
    """End-to-end: 429 + Retry-After: 120 produces Retry(retry_delay=120s)
    with a jitter=0 policy — the server's number, not the curve's."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    exc = _with_retry_after("120")

    decision = decide_after_failure(_actor_with(policy), exc, _job_state())

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=120)


def test_retry_after_delay_is_clamped_by_max_retry_backoff() -> None:
    """Citizen rule (verified, not assumed): the classifier hands the
    parsed delay to RetryOverride.delay, and _retry_decision clamps it to
    max_retry_backoff — a malformed header cannot strand a job past the
    worker-wide ceiling."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    exc = _with_retry_after("86400")  # exactly one day: parseable

    decision = decide_after_failure(
        _actor_with(policy),
        exc,
        _job_state(),
        max_retry_backoff=timedelta(hours=2),
    )

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(hours=2)


def test_retry_after_delay_respects_the_min_deferral_floor() -> None:
    """The decision never carries a sub-floor delay: a one-second hint (the
    parse path's minimum) lands exactly on MIN_DEFERRAL_INTERVAL."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)

    decision = decide_after_failure(_actor_with(policy), _with_retry_after("1"), _job_state())

    assert isinstance(decision, Retry)
    assert decision.retry_delay >= timedelta(seconds=1)


# ── jitter on indefinite-override delays, end-to-end ─────────────


def test_policy_jitter_spreads_the_retry_after_delay_end_to_end() -> None:
    """The declared policy's jitter applies to the override delay exactly
    as it applies to the computed curve: a fleet of workers fielding the
    same Retry-After must not all come due at the same instant. The delay
    draws from the multiplicative-symmetric band [raw·(1-j), raw·(1+j)]
    (fitted under max_retry_backoff), so with jitter=0.5 on a 100s hint
    the draws spread across [50s, 150s] and are not all identical."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.5)
    exc = _with_retry_after("100")

    decisions = [decide_after_failure(_actor_with(policy), exc, _job_state()) for _ in range(40)]

    delays = [d.retry_delay for d in decisions if isinstance(d, Retry)]
    assert len(delays) == 40
    for delay in delays:
        assert timedelta(seconds=50) <= delay <= timedelta(seconds=150)
    assert len(set(delays)) > 1, (
        "the policy's jitter must spread the override delay; identical draws "
        "mean jitter is not applied to override delays"
    )


def test_jitter_zero_keeps_exact_retry_after_compliance() -> None:
    """jitter=0.0 is the identity (the deterministic-suite knob): the
    Retry-After delay passes through exactly."""
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    exc = _with_retry_after("120")

    decision = decide_after_failure(_actor_with(policy), exc, _job_state())

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=120)


# ── performance pins ──────────────────────────────────────────────


def test_miss_path_allocates_nothing() -> None:
    """Zero allocation on the miss path: the classifier is invoked for
    EVERY exception the actor raises (most are not rate limits), so the
    not-claimed path must not allocate — and must not touch the header
    parse path. Measured as GROWTH between two traced batches: the first
    traced batch may catch one-off interpreter/tracer warm-up artifacts,
    a per-call allocation would grow linearly and fail the second
    batch."""
    exc = RuntimeError("unrelated failure")
    for _ in range(100):
        rate_limit_aware_classifier(exc, 1)

    tracemalloc.start()
    try:
        for _ in range(10_000):
            rate_limit_aware_classifier(exc, 1)
        _, steady_state = tracemalloc.get_traced_memory()  # absorbs one-off artifacts
        for _ in range(10_000):
            rate_limit_aware_classifier(exc, 1)
        _, after_batch = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert after_batch == steady_state, (
        "the miss path must not allocate per call: traced memory grew from "
        f"{steady_state} to {after_batch} bytes over 10,000 not-claimed calls"
    )
