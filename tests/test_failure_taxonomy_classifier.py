"""The configurable failure-taxonomy classifier.

``failure_taxonomy_classifier`` sniffs the common failure shapes a
declared policy can't name per-instance: connection-class errors,
TimeoutError-shaped errors, and HTTP 5xx/408/425 statuses are claimed
``transient``; other 4xx statuses are claimed ``non_retryable``;
everything else returns ``None`` — the conservative default, because
over-claiming is the haunt class (an ``indefinite`` claim with no
deadline retries forever; this classifier deliberately never returns
``indefinite``, so every claim it makes is bounded by ``max_attempts``).

The timeout NAME signals are a contract: the default factory matches
EXACT curated names only (``DEFAULT_TRANSIENT_EXCEPTION_NAMES``) — a
class name ending in ``Timeout``/``TimeoutError`` is claimed ONLY if the
exact name is listed. The suffix inference is opt-in
(``infer_timeout_by_suffix=True``), documented with its counterexample:
``pymongo.errors.ExecutionTimeout`` is a deadline-exceeded that MEANS
failure (a re-run re-fails deterministically), and the suffix default
burned retry budget claiming it transient. The builtin ``TimeoutError``
``isinstance`` path is TYPE-based, not name-based — it stays default.
Consumers who route these shapes narrowly (static-policy, pinned per
class) are the supported shape.

Configuration is by keyword: ``transient_status`` /
``non_retryable_status`` status sets and ``include_names`` /
``exclude_names`` exception-name lists; ``None`` means the documented
module constants (DEFAULT_* below). Status sets REPLACE the defaults
entirely; name lists replace their defaults too. Precedence, documented
in the factory's docstring and pinned here:

1. ``exclude_names`` — an explicit exclusion wins over every signal,
   the opt-in suffix flag included;
2. transient signals — connection/timeout shape (type-based or exact
   curated name), the opt-in suffix inference, or ``transient_status``;
3. ``non_retryable_status``;
4. unsure → ``None``.

429 is deliberately carved out of the default non-retryable band (the
PR's own §4 table: 429 is the single most important status to retry) so
this classifier never claims it and composes safely with
rate_limit_aware_classifier in EITHER order.
"""

from datetime import timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.retry import (
    DEFAULT_EXCLUDED_EXCEPTION_NAMES,
    DEFAULT_NON_RETRYABLE_STATUSES,
    DEFAULT_TRANSIENT_EXCEPTION_NAMES,
    DEFAULT_TRANSIENT_STATUSES,
    Fail,
    JobRetryState,
    Retry,
    RetryOverride,
    RetryPolicy,
    compose_retry_classifiers,
    decide_after_failure,
    failure_taxonomy_classifier,
    rate_limit_aware_classifier,
)
from taskq.testing.actor import StubActorConfig


class _Response:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _HttpxStyleError(Exception):
    def __init__(self, status_code: int) -> None:
        self.response = _Response(status_code)
        super().__init__(f"HTTP {status_code}")


class _AiohttpStyleError(Exception):
    """Bare ``.status`` on the exception (aiohttp's shape)."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


class ReadTimeout(Exception):  # noqa: N818  Why: the exact class name IS the signal under test (httpx/requests shape)
    """httpx/requests-shaped timeout (name-matched, no import)."""


class ServerTimeoutError(Exception):
    """aiohttp's timeout shape (name-matched)."""


class ConnectError(Exception):
    """httpx's transport connect error (name-matched)."""


class VendorFlake(Exception):  # noqa: N818  Why: a consumer-defined name that intentionally lacks the Error suffix
    """A consumer-defined transient nobody's default set knows."""


class ExecutionTimeout(Exception):  # noqa: N818  Why: the exact class name IS the signal under test (pymongo shape)
    """``pymongo.errors.ExecutionTimeout``-shaped: a deadline-exceeded that
    MEANS failure — the server killed the operation for exceeding
    ``maxTimeMS`` and a re-run of the same query re-fails deterministically.
    Name ends in ``Timeout``, no HTTP attrs, not a builtin ``TimeoutError``
    subclass: exactly the shape the suffix heuristic over-claims."""


class QueryDeadlineExceeded(TimeoutError):  # noqa: N818  Why: the name must NOT be Error-suffixed/suffix-matched — the isinstance signal's shape under test
    """A builtin-``TimeoutError`` subclass with a name that is neither an
    exact curated name nor suffix-matched: the isinstance signal's shape."""


class MyConnError(ConnectionError):
    """A builtin-connection subclass with a non-default name."""


def _job_state(*, attempt: int = 1, max_attempts: int = 3) -> JobRetryState:
    return JobRetryState(
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )


# ── transient: connection-class + TimeoutError-shaped ────────────


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionError("peer closed"),
        ConnectionResetError("reset by peer"),
        ConnectionRefusedError("refused"),
        ConnectionAbortedError("aborted"),
        MyConnError("named subclass of the builtin family"),
        TimeoutError("timed out"),
        ReadTimeout("httpx/requests-shaped"),
        ServerTimeoutError("aiohttp-shaped"),
        ConnectError("httpx transport"),
    ],
    ids=[
        "connection-error",
        "connection-reset",
        "connection-refused",
        "connection-aborted",
        "connection-subclass",
        "builtin-timeout",
        "name-timeout",
        "name-server-timeout",
        "name-connect-error",
    ],
)
def test_connection_and_timeout_shapes_claim_transient(exc: BaseException) -> None:
    hook = failure_taxonomy_classifier()

    result = hook(exc, 1)

    assert result == RetryOverride(kind="transient")


def test_builtin_timeout_via_socket_alias_shape() -> None:
    """socket.timeout IS TimeoutError on py3.10+; the isinstance signal
    covers it without name matching."""

    hook = failure_taxonomy_classifier()

    assert hook(TimeoutError("timed out"), 1) == RetryOverride(kind="transient")


# ── the suffix inference is OPT-IN: exact curated names by default ─


def test_suffix_inference_is_opt_in_execution_timeout_shape_returns_none() -> None:
    """The adoption contract: the DEFAULT factory matches exact curated
    names ONLY. ``ExecutionTimeout`` ends in ``Timeout`` but is not in the
    curated set — a deadline-exceeded that MEANS failure (a re-run
    re-fails deterministically), so the suffix heuristic's transient claim
    burned retry budget on unwinnable work. Under default kwargs the
    declared policy governs: None. Opt in with
    ``infer_timeout_by_suffix=True`` only when the suffix is trusted."""
    hook = failure_taxonomy_classifier()

    assert hook(ExecutionTimeout("maxTimeMS exceeded"), 1) is None


@settings(max_examples=200)
@given(
    prefix=st.text(alphabet=st.characters(min_codepoint=65, max_codepoint=90), min_size=1),
    suffix=st.sampled_from(["Timeout", "TimeoutError"]),
)
def test_property_suffix_names_not_in_the_curated_set_stay_unclaimed(
    prefix: str, suffix: str
) -> None:
    """Property: under default kwargs, a class name ending in
    ``Timeout``/``TimeoutError`` is claimed ONLY if the exact name is in
    the curated set — the suffix alone is never a signal."""
    name = f"{prefix}{suffix}"
    exc = type(name, (Exception,), {})

    hook = failure_taxonomy_classifier()

    if name in DEFAULT_TRANSIENT_EXCEPTION_NAMES:
        assert hook(exc("x"), 1) == RetryOverride(kind="transient")
    else:
        assert hook(exc("x"), 1) is None, f"suffix over-claim for {name!r}"


def _instantiate_client_exception(cls: type[BaseException]) -> BaseException:
    """Instantiate a real client exception without knowing its __init__
    arity (urllib3's RequestError takes ``(pool, url, message)``; botocore's
    timeout errors format ``endpoint_url`` into their message)."""
    for args, kwargs in (
        ((), {}),
        (("x",), {}),
        (("x", "y"), {}),
        (("pool", "url", "message"), {}),
        ((), {"endpoint_url": "https://s3.us-east-1.amazonaws.invalid"}),
    ):
        try:
            return cls(*args, **kwargs)
        except (TypeError, KeyError):
            # botocore's message templates KeyError on missing format
            # fields rather than TypeError on arity — both mean "wrong
            # shape for this constructor", try the next.
            continue
    raise AssertionError(f"could not instantiate {cls.__module__}.{cls.__qualname__}")


@pytest.mark.parametrize(
    ("module_path", "class_name"),
    [
        ("urllib3.exceptions", "ReadTimeoutError"),
        ("urllib3.exceptions", "ConnectTimeoutError"),
        ("botocore.exceptions", "ReadTimeoutError"),
        ("botocore.exceptions", "ConnectTimeoutError"),
    ],
)
def test_major_client_timeout_names_claim_transient_under_default(
    module_path: str, class_name: str
) -> None:
    """The curated set's provenance promise is audited against the real
    clients: ``ReadTimeoutError``/``ConnectTimeoutError`` are what urllib3
    (requests' engine, the most-installed HTTP stack) and botocore (the
    AWS SDK) raise on a read/connect timeout. Neither is a builtin
    ``TimeoutError`` subclass (urllib3's own ``TimeoutError`` and
    botocore's ``BotoCoreError`` bases), so ONLY the exact curated name
    claims them — a set that dropped them would under-claim exactly the
    majors the constant's provenance comment promises, and the pre-#658
    suffix heuristic's only remaining real-world benefit would be lost
    with nothing to show for it (``ExecutionTimeout`` is a synthetic
    shape; these four are the shapes shipping code actually raises)."""
    import importlib

    pytest.importorskip(module_path.split(".")[0])
    cls = getattr(importlib.import_module(module_path), class_name)
    instance = _instantiate_client_exception(cls)

    hook = failure_taxonomy_classifier()

    assert hook(instance, 1) == RetryOverride(kind="transient")


def test_infer_timeout_by_suffix_flag_restores_the_suffix_inference() -> None:
    """The opt-in flag: today's suffix behavior, one keyword away — the
    convenience flag, documented with its ExecutionTimeout cost."""
    hook = failure_taxonomy_classifier(infer_timeout_by_suffix=True)

    assert hook(ExecutionTimeout("maxTimeMS exceeded"), 1) == RetryOverride(kind="transient")


@pytest.mark.parametrize("name", sorted(DEFAULT_TRANSIENT_EXCEPTION_NAMES))
def test_every_curated_exact_name_claims_transient(name: str) -> None:
    """Pin the curated set itself: each exact name in
    DEFAULT_TRANSIENT_EXCEPTION_NAMES claims transient under DEFAULT
    kwargs — and the parametrization doubles as the audit that the set
    holds exact names (a suffix entry like 'Foo' claiming via endswith
    would fail the ExecutionTimeout pin above while this still passes)."""
    exc = type(name, (Exception,), {})
    hook = failure_taxonomy_classifier()

    assert hook(exc("x"), 1) == RetryOverride(kind="transient")


def test_builtin_timeouterror_isinstance_path_is_type_based_and_stays_default() -> None:
    """The isinstance path is type-based, not name-based: a builtin
    ``TimeoutError`` subclass whose name is neither curated nor
    suffix-shaped still claims transient under DEFAULT kwargs. The suffix
    heuristic's removal from the default path does not touch it."""
    hook = failure_taxonomy_classifier()

    assert hook(QueryDeadlineExceeded("x"), 1) == RetryOverride(kind="transient")


def test_exclude_names_outranks_the_opt_in_suffix_flag() -> None:
    """Precedence re-pin: exclude_names outranks EVERYTHING — the
    opt-in suffix flag included."""
    hook = failure_taxonomy_classifier(
        exclude_names={"ExecutionTimeout"},
        infer_timeout_by_suffix=True,
    )

    assert hook(ExecutionTimeout("x"), 1) is None


# ── transient: HTTP statuses ──────────────────────────────────────


@pytest.mark.parametrize("status", [408, 425, 500, 502, 503, 504, 599])
def test_http_5xx_408_425_claim_transient(status: int) -> None:
    hook = failure_taxonomy_classifier()

    result = hook(_HttpxStyleError(status), 1)

    assert result == RetryOverride(kind="transient")


def test_aiohttp_status_shape_claims_transient() -> None:
    hook = failure_taxonomy_classifier()

    assert hook(_AiohttpStyleError(503), 1) == RetryOverride(kind="transient")


# ── non-retryable: other 4xx ──────────────────────────────────────


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422, 499])
def test_other_4xx_claims_non_retryable(status: int) -> None:
    hook = failure_taxonomy_classifier()

    result = hook(_HttpxStyleError(status), 1)

    assert result == RetryOverride(kind="non_retryable")


def test_429_is_carved_out_of_the_default_non_retryable_band() -> None:
    """The carve-out the PR's own §4 table demands: 429 is the single most
    important status to RETRY. The taxonomy returns None for it — the
    rate-limit classifier's signal, or the declared policy's if that
    built-in is not composed in."""
    hook = failure_taxonomy_classifier()

    assert hook(_HttpxStyleError(429), 1) is None


@pytest.mark.parametrize("status", [100, 200, 301, 307, 302])
def test_informational_success_and_redirect_statuses_are_not_claimed(status: int) -> None:
    """Unsure → None: only the 4xx/5xx bands (plus the name/isinstance
    shapes) are claimed; a 3xx surfacing as an exception is not."""
    hook = failure_taxonomy_classifier()

    assert hook(_HttpxStyleError(status), 1) is None


def test_unsure_exceptions_return_none_conservative_default() -> None:
    """The haunt lesson: over-claiming is the haunt class. An exception
    with no recognized shape gets None, the declared policy governs."""
    hook = failure_taxonomy_classifier()

    assert hook(RuntimeError("x"), 1) is None
    assert hook(ValueError("x"), 1) is None
    assert hook(Exception("x"), 1) is None


# ── never indefinite: every claim is bounded ──────────────────────


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionError("x"),
        TimeoutError("x"),
        ReadTimeout("x"),
        _HttpxStyleError(503),
        _HttpxStyleError(404),
        _HttpxStyleError(429),
        RuntimeError("x"),
    ],
    ids=["conn", "timeout", "name-timeout", "5xx", "4xx", "429", "unsure"],
)
def test_taxonomy_never_returns_indefinite(exc: BaseException) -> None:
    """The anti-haunt contract: no signal in the taxonomy's default set
    maps to ``indefinite`` — transient claims stay bounded by
    max_attempts, non-retryable claims terminate, None defers. Only
    rate_limit_aware_classifier claims indefinite (with the deadline
    hazard documented for exactly that reason)."""
    hook = failure_taxonomy_classifier()

    result = hook(exc, 1)

    assert result is None or result.kind != "indefinite"


def test_transient_claim_is_bounded_by_max_attempts_end_to_end() -> None:
    """The contrast that makes the taxonomy safe: a transient override at
    attempt == max_attempts Fails — the budget the actor declared still
    applies (no deadline needed, unlike the indefinite haunt)."""
    policy = RetryPolicy(kind="transient", max_attempts=2, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(failure_taxonomy_classifier()),
    )

    decision = decide_after_failure(
        actor_config, ConnectionError("reset"), _job_state(attempt=2, max_attempts=2)
    )

    assert isinstance(decision, Fail)


def test_transient_claim_below_max_attempts_retries_end_to_end() -> None:
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(failure_taxonomy_classifier()),
    )

    decision = decide_after_failure(actor_config, ConnectionError("reset"), _job_state(attempt=1))

    assert isinstance(decision, Retry)


def test_non_retryable_claim_fails_immediately_end_to_end() -> None:
    policy = RetryPolicy(kind="transient", max_attempts=3, jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(failure_taxonomy_classifier()),
    )

    decision = decide_after_failure(actor_config, _HttpxStyleError(404), _job_state())

    assert isinstance(decision, Fail)
    assert decision.error_class == "_HttpxStyleError"


# ── composition safety with the rate-limit built-in ───────────────


def test_429_reaches_rate_limit_aware_in_either_composition_order() -> None:
    """The 429 carve-out's payoff: because the taxonomy claims nothing for
    429, the composed pair classifies a 429 as the rate-limit built-in
    does whether the taxonomy runs first or second."""
    exc = _HttpxStyleError(429)
    tax_first = compose_retry_classifiers(
        failure_taxonomy_classifier(), rate_limit_aware_classifier
    )
    tax_last = compose_retry_classifiers(rate_limit_aware_classifier, failure_taxonomy_classifier())

    assert tax_first(exc, 1) == RetryOverride(kind="indefinite")
    assert tax_last(exc, 1) == RetryOverride(kind="indefinite")


def test_taxonomy_does_not_override_the_built_ins_429_verdict() -> None:
    """And when both are composed, a 404 still fails fast and a 503 stays
    transient — the taxonomy earns its keep on everything but 429."""
    composed = compose_retry_classifiers(rate_limit_aware_classifier, failure_taxonomy_classifier())

    assert composed(_HttpxStyleError(404), 1) == RetryOverride(kind="non_retryable")
    assert composed(_HttpxStyleError(503), 1) == RetryOverride(kind="transient")


# ── configuration ─────────────────────────────────────────────────


def test_include_names_extend_the_transient_set() -> None:
    """A consumer-defined transient nobody's defaults know: include_names
    adds it by exact class name."""
    hook = failure_taxonomy_classifier(include_names={"VendorFlake"})

    assert hook(VendorFlake("flake"), 1) == RetryOverride(kind="transient")
    assert hook(VendorFlake("flake"), 1) is not None
    # without the include, the same class is unsure → None
    assert failure_taxonomy_classifier()(VendorFlake("flake"), 1) is None


def test_exclude_names_win_over_every_signal() -> None:
    """Precedence pin: an excluded name returns None even when the
    exception would be claimed by the builtin connection family, the
    timeout name shape, AND the status band."""
    hook = failure_taxonomy_classifier(exclude_names={"MyConnError", "ReadTimeout"})

    assert hook(MyConnError("x"), 1) is None, "exclude wins over the isinstance signal"
    assert hook(ReadTimeout("x"), 1) is None, "exclude wins over the name-shape signal"
    assert hook(_HttpxStyleError(503), 1) == RetryOverride(kind="transient")


def test_transient_status_set_replaces_the_default() -> None:
    """Status sets REPLACE the defaults entirely (documented): passing
    transient_status={429} claims ONLY 429 — the 5xx band and 408/425 are
    no longer claimed transient."""
    hook = failure_taxonomy_classifier(transient_status={429})

    assert hook(_HttpxStyleError(429), 1) == RetryOverride(kind="transient")
    assert hook(_HttpxStyleError(503), 1) is None
    assert hook(_HttpxStyleError(408), 1) is None


def test_non_retryable_status_set_replaces_the_default() -> None:
    hook = failure_taxonomy_classifier(non_retryable_status={418, 404})

    assert hook(_HttpxStyleError(418), 1) == RetryOverride(kind="non_retryable")
    assert hook(_HttpxStyleError(404), 1) == RetryOverride(kind="non_retryable")
    assert hook(_HttpxStyleError(401), 1) is None, "replaced, not merged"


def test_transient_status_wins_over_non_retryable_status_on_overlap() -> None:
    """Documented precedence within the status sets: a status in both sets
    is transient (retrying later is the safer wrong answer than killing a
    retryable job)."""
    hook = failure_taxonomy_classifier(
        transient_status={500},
        non_retryable_status={500},
    )

    assert hook(_HttpxStyleError(500), 1) == RetryOverride(kind="transient")


# ── the documented module constants ───────────────────────────────


def test_default_transient_statuses_constant() -> None:
    """408 and 425 plus the whole 5xx band."""
    assert 408 in DEFAULT_TRANSIENT_STATUSES
    assert 425 in DEFAULT_TRANSIENT_STATUSES
    for status in range(500, 600):
        assert status in DEFAULT_TRANSIENT_STATUSES
    assert 404 not in DEFAULT_TRANSIENT_STATUSES
    assert 429 not in DEFAULT_TRANSIENT_STATUSES


def test_default_non_retryable_statuses_constant_is_4xx_minus_carve_outs() -> None:
    """The 4xx band minus {408, 425, 429}: exactly the statuses the §4
    table calls non-retryable."""
    for status in range(400, 500):
        expected = status not in {408, 425, 429}
        assert (status in DEFAULT_NON_RETRYABLE_STATUSES) == expected


def test_default_transient_exception_names_constant() -> None:
    """The name defaults cover the stdlib and major-HTTP-client shapes the
    docstring promises; the empty-exclude default is exposed for
    extension."""
    assert "TimeoutError" in DEFAULT_TRANSIENT_EXCEPTION_NAMES
    assert "ConnectionError" in DEFAULT_TRANSIENT_EXCEPTION_NAMES
    assert "ReadTimeout" in DEFAULT_TRANSIENT_EXCEPTION_NAMES
    assert "ServerTimeoutError" in DEFAULT_TRANSIENT_EXCEPTION_NAMES
    assert frozenset() == DEFAULT_EXCLUDED_EXCEPTION_NAMES


# ── property: the classification is a pure function of the status ──


@settings(max_examples=500)
@given(status=st.integers(min_value=100, max_value=599))
def test_property_status_classification_matches_the_documented_bands(status: int) -> None:
    """Property: for every plausible HTTP status, the default taxonomy's
    verdict is exactly the documented band membership."""
    hook = failure_taxonomy_classifier()

    result = hook(_HttpxStyleError(status), 1)

    if status in DEFAULT_TRANSIENT_STATUSES:
        assert result == RetryOverride(kind="transient")
    elif status in DEFAULT_NON_RETRYABLE_STATUSES:
        assert result == RetryOverride(kind="non_retryable")
    else:
        assert result is None


@settings(max_examples=100)
@given(payload=st.one_of(st.integers(), st.text(), st.none(), st.booleans()))
def test_property_arbitrary_exception_payloads_stay_unclaimed(payload: object) -> None:
    """Property: arbitrary non-HTTP exception payloads stay unclaimed —
    the conservative default is None whatever the payload."""
    hook = failure_taxonomy_classifier()

    assert hook(RuntimeError(f"payload: {payload!r}"), 1) is None


def test_all_signals_at_any_attempt() -> None:
    """Classification is per-occurrence, not attempt-dependent."""
    hook = failure_taxonomy_classifier()

    for attempt in (1, 7, 999):
        assert hook(ConnectionError("x"), attempt) == RetryOverride(kind="transient")
        assert hook(_HttpxStyleError(404), attempt) == RetryOverride(kind="non_retryable")


def test_retry_after_delay_from_taxonomy_transient_override_uses_the_curve_not_the_override() -> (
    None
):
    """A transient override carries no delay: the declared curve computes
    when (jitter=0 pins the value); the taxonomy only refines the kind."""
    policy = RetryPolicy(kind="transient", max_attempts=3, base=timedelta(seconds=7), jitter=0.0)
    actor_config = StubActorConfig(
        retry=policy,
        retry_classifier=compose_retry_classifiers(failure_taxonomy_classifier()),
    )

    decision = decide_after_failure(actor_config, _HttpxStyleError(503), _job_state())

    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=7)
