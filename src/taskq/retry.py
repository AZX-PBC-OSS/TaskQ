"""Retry policy data carriers, decision types, backoff computation, and
consumer-loop adapter wiring.

The data-model layer (RetryPolicy, Retry, Fail, RetryDecision,
JobRetryState, compute_backoff, RetryClassifier) is pure: no I/O, no
clock reads, no backend imports. The adapter layer (OnRetryExhausted,
OnSuccess, OnCancel, ActorConfigLike, decide_after_failure,
invoke_on_retry_exhausted, invoke_on_success, invoke_on_cancel,
safe_mark_failed_or_retry) wires the classifier to the consumer loop
and is permitted backend imports. The built-in classifier hooks
(rate_limit_aware_classifier, failure_taxonomy_classifier) sit between
the two: they are pure functions of ``(exception, attempt)`` except for
one bounded clock read — HTTP-date ``Retry-After`` parsing needs *now*
to turn a date into a delay; the clock is injectable (``now=``) and
never touched on a claimed-without-header or miss path.
"""

import asyncio
import email.utils
import hashlib
import inspect
import random
import re
import secrets
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, NamedTuple, Protocol, Self, get_args
from uuid import UUID

import structlog
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from taskq.backend._protocol import Backend, ErrorInfo, JobId, JobRow, RetryKind
from taskq.constants import (
    DEFAULT_MAX_RETRY_BACKOFF,
    MAX_ATTEMPTS_SMALLINT_CEILING,
    MAX_ENQUEUABLE_MAX_ATTEMPTS,
    MIN_DEFERRAL_INTERVAL,
    check_max_attempts_domain,
)
from taskq.exceptions import (
    PayloadValidationError,
    ReservationUnavailable,
    ResultTooLarge,
    UnencodableValue,
    WorkerOwnershipMismatch,
)
from taskq.obs._redact_exc import safe_repr

__all__ = [
    "MAX_ATTEMPTS_SMALLINT_CEILING",
    "MAX_ENQUEUABLE_MAX_ATTEMPTS",
    "ActorConfigLike",
    "Fail",
    "JobRetryState",
    "OnCancel",
    "OnRetryExhausted",
    "OnSuccess",
    "Retry",
    "RetryClassifier",
    "RetryClassifierHook",
    "RetryDecision",
    "RetryKind",
    "RetryOverride",
    "RetryPolicy",
    "apply_jitter",
    "compose_retry_classifiers",
    "compute_backoff",
    "decide_after_failure",
    "failure_taxonomy_classifier",
    "invoke_on_cancel",
    "invoke_on_retry_exhausted",
    "invoke_on_success",
    "make_rate_limit_aware_classifier",
    "rate_limit_aware_classifier",
    "safe_mark_failed_or_retry",
    "time_budget_as_interval",
]


class RetryPolicy(BaseModel):
    """Policy controlling retry behaviour for an actor."""

    model_config = ConfigDict(frozen=True)

    kind: RetryKind = "transient"
    max_attempts: int = 3
    time_budget: timedelta | None = None
    backoff: Literal["exponential", "linear", "fixed"] = "exponential"
    base: timedelta = timedelta(seconds=5)
    cap: timedelta = timedelta(hours=1)
    jitter: float = 0.2

    @field_validator("max_attempts")
    @classmethod
    def _validate_max_attempts(cls, v: int) -> int:
        check_max_attempts_domain(v)
        return v

    @field_validator("base")
    @classmethod
    def _validate_base_positive(cls, v: timedelta) -> timedelta:
        # The monopolisation boundary: a zero or negative base degenerates
        # the curve (0 * 2**k == 0 at every rung; a negative base lands
        # scheduled_at in the past), and the reclaim path reads the stamped
        # curve on rows this policy produces. The failure-retry decision and
        # the reclaim writes floor sub-floor delays at MIN_DEFERRAL_INTERVAL
        # as defense-in-depth, but refusing here names the mistake at
        # registration instead of silently substituting a curve the actor
        # did not ask for. Mirrors the heartbeat_timeout boundary rule: a
        # non-positive value anchors a deadline in the past.
        if v <= timedelta(0):
            raise ValueError(
                f"base must be > 0, got {v}; a zero or negative base degenerates "
                "the backoff curve to a zero-period retry loop that monopolises "
                "a worker slot with no backoff"
            )
        return v

    @model_validator(mode="after")
    def _validate_cap_ge_base(self) -> Self:
        if self.cap < self.base:
            raise ValueError(f"cap ({self.cap}) must be >= base ({self.base})")
        return self

    @field_validator("jitter")
    @classmethod
    def _validate_jitter(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("jitter must be in [0.0, 1.0]")
        return v


class Retry(BaseModel):
    """Retry decision: reschedule the job after *retry_delay*.

    The delay, not a computed timestamp, is the decision payload: the
    backend derives ``scheduled_at = now() + retry_delay`` and the
    scheduled/pending status from its own clock (single arbiter, immune to
    app↔DB clock skew).  The delay never falls below
    :data:`~taskq.constants.MIN_DEFERRAL_INTERVAL`, the requeue-rate
    floor the deferral arms already apply at their writes, so a
    degenerate curve (``base=timedelta(0)``, where ``0 * 2**k == 0`` at
    every rung) or a zero ``Retry-After`` override cannot turn the
    failure cycle into a claim/run/fail round trip monopolising a worker
    slot with no period and, for an ``indefinite`` kind, no attempt
    ceiling.
    """

    model_config = ConfigDict(frozen=True)

    retry_delay: timedelta


class Fail(BaseModel):
    """Fail decision: the job will not be retried."""

    model_config = ConfigDict(frozen=True)

    error_class: str
    retryable: bool


type RetryDecision = Retry | Fail


class JobRetryState(NamedTuple):
    """Projection of JobRow columns consumed by the retry classifier.

    ``schedule_to_close`` is not an input to classification, the
    SQL deadline guard in ``mark_failed_or_retry`` is the single deadline
    arbiter (one arbiter per predicate; see docs/architecture.md §Clock
    Domains).  The field is retained on the projection for observability
    and hook consumers.  ``start_to_close`` is reserved for per-attempt
    timeout enforcement at the consumer level (asyncio.wait_for); not used
    by the classifier.
    """

    attempt: int
    max_attempts: int
    retry_kind: RetryKind
    schedule_to_close: datetime | None
    start_to_close: timedelta | None


_production_rng = random.Random(secrets.randbits(128))  # noqa: S311  Why: random.Random is for timing jitter, not cryptography; seeded via secrets.randbits(128) by design

# Why: an ``indefinite`` policy has no attempt ceiling, so ``attempt`` is
# unbounded and the exponential arm's exponent must be clamped somewhere
# (the pre-clamp shape, ``base_s * 2 ** (attempt - 1)`` over Python ints,
# built an exact int that raised OverflowError at float conversion once
# attempt >= 1025, escaping compute_backoff → classify →
# _dispatch_exception and crashing the failure path instead of retrying).
# The bound is 67 because it is the smallest clamp that cannot change any
# timedelta-representable curve AND cannot overflow on either evaluator:
#
# * Curve preservation: timedelta's microsecond resolution puts the
#   smallest positive ``base`` at 1e-6 s and the largest representable
#   ``cap`` at ~8.6e13 s (999999999 days), so every representable
#   (base, cap) curve reaches its cap by exponent 66 at the latest
#   (1e-6 * 2**67 >= 1.4e14 >= 8.6e13), min(cap_s, ...) saturates at
#   cap_s with or without the clamp, and base == 0 yields 0 either way.
# * Overflow: the SQL twin (_RECLAIM_RAW_BACKOFF_SQL) evaluates
#   ``base * power(2.0, e)`` in float8, and Postgres RAISES
#   "value out of range: overflow" (SQLSTATE 22003, a data error the
#   leader's transient classification deliberately excludes) where this
#   module's float multiply saturates to inf and lets min() land on the
#   cap.  A clamp at the float ceiling (the historical 1023) kept
#   power() itself in range but let the multiply overflow for any
#   base > ~2 once the attempt passed ~1021, a non-transient error in
#   the leader sweep's failure path AND a parity break against this
#   twin.  base * 2**67 <= ~1.3e34 for any timedelta-representable base,
#   ~274 orders of magnitude below the float8 ceiling (~1.8e308).
_MAX_BACKOFF_EXPONENT: Final[int] = 67

#: The denominator of the deterministic reclaim-jitter fraction: 2**32, the
#: count of values the fraction's 8-hex-digit hash prefix can take.  A float
#: literal so the division in :func:`_reclaim_jitter_fraction` is an IEEE-754
#: float8 true-division, the same operation the SQL twin's
#: ``::float8 / 4294967296.0::float8`` performs, and both are correctly
#: rounded, so the two sides agree bit for bit.
_RECLAIM_JITTER_MODULUS: Final[float] = 4294967296.0


def _reclaim_jitter_fraction(job_id: UUID, attempt: int) -> float:
    """The reclaim curve's jitter fraction for one (job, attempt): a
    deterministic value in [0, 1) derived from the row's own identity.

    ``md5('<job_id>:<attempt>')`` → first 8 hex digits → uint32 → / 2**32.
    The SQL twin ``_RECLAIM_JITTER_FRACTION_SQL`` (see
    ``taskq.backend._sweeps``) computes the identical value in the database:
    same md5 over the same ASCII text (``j.id::text`` is the lowercase
    dashed uuid form ``str(job_id)`` produces, ``j.attempt::text`` the plain
    decimal, both pure ASCII, so the database encoding cannot change the
    hashed bytes), same uint32 interpretation of the prefix, same float8
    division.  The leader's sweep, a partitioned worker's isolate_self, and
    the in-memory mirror therefore all draw the SAME fraction for the same
    row instead of three independent ``random()`` draws.

    Why derived rather than random: a row's reclaim delay is computed by
    more than one statement (isolate_self and the leader sweep can each
    transition the same row within one outage window) and is replayed by
    every later sweep of the same row.  A per-statement ``random()`` makes
    those paths disagree about one row's hand-back instant and makes a
    replay stamp a different instant than the first pass; deriving the draw
    from the row makes reclaim replay-idempotent, same row, same delay,
    every path.  The fleet spread random jitter buys is preserved:
    distinct ids hash to distinct fractions, so a mass-expired cohort
    still arrives across the jitter band instead of at one synchronised
    instant.  (A per-evaluation random draw needs exactly one process ever
    to compute a given row's retry delay; the row-derived hash here is the
    dual-statement, dual-implementation parity requirement, not a
    different spreading goal.)

    md5 is a hash here, not a cipher: the input is a row identity, and
    32 bits of it become scheduling noise.
    """
    digest = hashlib.md5(f"{job_id}:{attempt}".encode(), usedforsecurity=False).hexdigest()
    return int(digest[:8], 16) / _RECLAIM_JITTER_MODULUS


def _raw_backoff_seconds(
    base_s: float,
    cap_s: float,
    backoff: Literal["exponential", "linear", "fixed"],
    attempt: int,
) -> float:
    """The unjittered curve value for *attempt*, the single Python
    implementation of ``_RECLAIM_RAW_BACKOFF_SQL``'s three-way branch,
    shared by :func:`compute_backoff` and :func:`_compute_reclaim_backoff`.

    The exponent floor ``max(attempt - 1, 0)`` mirrors the SQL fragment's
    ``GREATEST(j.attempt - 1, 0)``: the SQL cannot raise on a
    direct-construction ``attempt = 0`` row, so the floor lives here rather
    than at a call site.  On :func:`compute_backoff`'s domain
    (``attempt >= 1``, enforced by its own guard) the floor is the identity.
    """
    if backoff == "exponential":
        return min(cap_s, base_s * 2.0 ** min(max(attempt - 1, 0), _MAX_BACKOFF_EXPONENT))
    if backoff == "linear":
        return min(cap_s, base_s * attempt)
    return base_s


def _jittered_seconds(raw_s: float, jitter: float, source: random.Random) -> float:
    """The one implementation of the multiplicative-symmetric jitter
    multiplication, every delay this package spreads shares it.

    ``raw * source.uniform(1 - jitter, 1 + jitter)``, floored at zero.
    """
    return max(0.0, raw_s * source.uniform(1.0 - jitter, 1.0 + jitter))


def _capped_jitter_band(raw_s: float, cap_s: float, jitter: float) -> tuple[float, float]:
    """The jitter band ``[raw·(1-j), raw·(1+j)]`` fitted under *cap_s*.

     The cap bounds the BAND, not the drawn value. Clipping the drawn value
     (``min(cap, raw·U(1-j, 1+j))``) collapses the upper half of a saturated
     row's band onto ``cap`` exactly, so about half of any cohort at the cap
    , the default exponential policy from attempt 11, every ``fixed`` or
     ``linear`` policy whose base meets the cap, every reclaimed cohort at
     the ceiling, comes due at the same instant: the thundering herd
     jitter exists to prevent, on the retries most likely to be fleet-wide.
     Fitting the band first (``raw`` clamped to the cap, then the band's
     upper edge clamped to it) keeps the draw uniform over what remains ,
     ``[cap·(1-j), cap]`` for a saturated row, with the documented bounds
     ``0 ≤ delay ≤ cap`` intact and ``jitter=0`` still the identity.

     Shared by :func:`compute_backoff` (RNG draw) and
     :func:`_compute_reclaim_backoff` (row-derived fraction); the SQL twin
     ``_RECLAIM_DELAY_SQL`` evaluates the same expressions in the same
     operand order, so the reclaim delays agree bit for bit.
    """
    capped_raw = min(cap_s, raw_s)
    lower = capped_raw * (1.0 - jitter)
    upper = min(capped_raw * (1.0 + jitter), cap_s)
    return lower, upper


def _draw_in_band(lower: float, upper: float, fraction: float, cap_s: float) -> float:
    """``lower + (upper - lower)·fraction``, ``fraction`` in ``[0, 1)``.

    The band already lies inside ``[0, cap]``; the closing ``min`` only
    absorbs float rounding at the top edge and cannot pile draws onto the
    cap the way the old value-clamp did. Operand order is the SQL twin's.
    """
    return min(cap_s, lower + (upper - lower) * fraction)


def apply_jitter(
    delay: timedelta,
    jitter: float,
    rng: random.Random | None = None,
) -> timedelta:
    """Spread an externally supplied advisory delay with *jitter*.

    Same formula as :func:`compute_backoff` (see it for why
    multiplicative-symmetric, not Full Jitter), for delays whose raw value
    comes from outside the policy's own backoff curve, an admission
    denial's ``retry_after``. Every fielder of the same raw hint in one
    round would otherwise re-attempt in lockstep (same token deficit, same
    lease horizon), so the hint is spread across the band exactly as
    failure backoff is; the knob stays the policy's own ``jitter``, so
    ``jitter=0.0`` (``uniform(1, 1)``) is the identity and deterministic
    suites stay deterministic.

    The result is advisory timing only: any downstream floor (the snooze
    arm's ``MIN_DEFERRAL_INTERVAL``) still applies to the returned value.
    """
    if not (0.0 <= jitter <= 1.0):
        raise ValueError(f"jitter must be in [0.0, 1.0], got {jitter}")
    source = rng if rng is not None else _production_rng
    return timedelta(seconds=_jittered_seconds(delay.total_seconds(), jitter, source))


def compute_backoff(
    policy: RetryPolicy,
    attempt: int,
    rng: random.Random | None = None,
    *,
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
) -> timedelta:
    """Compute the backoff delay for a given attempt (1-indexed).

    formula: multiplicative-symmetric jitter ,
      delay = raw * rng.uniform(1 - jitter, 1 + jitter)
    with the band fitted under the cap before the draw (see
    :func:`_capped_jitter_band`), so a saturated row spreads over
    ``[cap·(1-j), cap]`` instead of stacking on the cap.
    This is NOT Full Jitter (uniform(0, raw)) because Full Jitter
    collapses toward zero on attempt 1, causing thundering-herd
    retries. See Marc Brooker, "Exponential Backoff And Jitter",
    AWS Architecture Blog, and the AWS SDKs' published jitter debate.

    ``max_retry_backoff`` is the global ceiling applied *after*
    ``policy.cap``, i.e. ``effective_cap = min(policy.cap, max_retry_backoff)``.
    The global cap prevents a misconfigured per-actor
    ``RetryPolicy(cap=timedelta(days=365))`` from stranding jobs for a year
    with no operator visibility, a defensive layer beyond the policy's own cap.
    Callers that hold ``WorkerSettings`` should pass
    ``settings.max_retry_backoff``; the default 24 h matches
    ``WorkerSettings.max_retry_backoff``.
    """
    source = rng if rng is not None else _production_rng

    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")

    base_s = policy.base.total_seconds()
    # Apply the global ceiling before using cap_s anywhere else.
    cap_s = min(policy.cap.total_seconds(), max_retry_backoff.total_seconds())

    raw = _raw_backoff_seconds(base_s, cap_s, policy.backoff, attempt)
    lower, upper = _capped_jitter_band(raw, cap_s, policy.jitter)
    # One draw, as uniform(a, b) is a + (b - a) * random(): identical RNG
    # consumption to the symmetric multiplication below the cap.
    delay = _draw_in_band(lower, upper, source.random(), cap_s)
    return timedelta(seconds=delay)


def _compute_reclaim_backoff(  # pyright: ignore[reportUnusedFunction]  # Why: consumed cross-module by taskq.testing._sweeps (the in-memory reclaim twin) and the parity pin; pyright's unused-function analysis for private names does not follow cross-module references.
    policy: RetryPolicy,
    attempt: int,
    *,
    job_id: UUID,
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
) -> timedelta:
    """The crash/heartbeat reclaim hand-back delay: :func:`compute_backoff`'s
    exact curve, jittered by the deterministic per-(job, attempt)
    :func:`_reclaim_jitter_fraction` instead of an RNG draw.

    The Python twin of ``_RECLAIM_DELAY_SQL`` (``taskq.backend._sweeps``):
    same three-way raw branch (shared through :func:`_raw_backoff_seconds`),
    same ``min(policy.cap, max_retry_backoff)`` effective ceiling, same
    band fitted under that ceiling (:func:`_capped_jitter_band`) and the
    same ``lower + (upper - lower) * f`` draw evaluated in the SQL's operand
    order, so the two agree bit for bit, pinned by
    ``tests/test_reclaim_backoff_policy_parity.py``.  The jitter is derived
    from the row, never drawn: the leader's sweep and a partitioned worker's
    isolate_self can each transition the same row within one outage window,
    and both must stamp the same delay (replay idempotence, see
    :func:`_reclaim_jitter_fraction` for the full rationale).

    Unlike :func:`compute_backoff` this never raises on ``attempt < 1``: the
    sweep must not crash on a direct-construction row, the SQL floors the
    exponent (``GREATEST(j.attempt - 1, 0)``, mirrored by
    :func:`_raw_backoff_seconds`) and hashes the row's raw stamped attempt
    (``j.attempt::text``), so this function does the same rather than
    rejecting the input.

    The returned delay is floored at :data:`~taskq.constants.MIN_DEFERRAL_INTERVAL`,
    the same monopolisation floor the failure-retry decision applies and the
    SQL twin applies through ``GREATEST``: a degenerate row curve (a zero or
    negative base stamped by an earlier release) draws a sub-floor value from
    its own curve, and handing such a row back with no period turns lease
    expiry into a claim/reclaim loop across the fleet.
    """
    fraction = _reclaim_jitter_fraction(job_id, attempt)
    base_s = policy.base.total_seconds()
    cap_s = min(policy.cap.total_seconds(), max_retry_backoff.total_seconds())
    raw = _raw_backoff_seconds(base_s, cap_s, policy.backoff, attempt)
    lower, upper = _capped_jitter_band(raw, cap_s, policy.jitter)
    # The monopolisation floor, wrapping the draw exactly as the SQL twin
    # wraps it (GREATEST after the band and the cap): a row stamped with a
    # zero or negative base by an earlier release draws zero or a negative
    # value from its own curve, and a sub-floor re-pend delay would make
    # the row claimable at or before the instant the sweep hands it back,
    # a claim/lease-expiry/reclaim loop with no period. Curves above the
    # floor are untouched, so the parity pin's values are unchanged.
    return timedelta(
        seconds=max(
            _draw_in_band(lower, upper, fraction, cap_s), MIN_DEFERRAL_INTERVAL.total_seconds()
        )
    )


class RetryOverride(BaseModel):
    """Per-exception override returned by an actor's ``retry_classifier`` hook.

    Both fields are optional; ``None`` means "use the actor's static
    ``RetryPolicy``/computed backoff for this field." Returning a
    ``RetryOverride`` with only ``kind`` set lets one exception *type*
    branch into different retry behaviour per occurrence, e.g. an HTTP
    429 response goes ``indefinite`` while a 404 response on the same
    exception type goes ``non_retryable``. Returning one with only
    ``delay`` set lets the actor honour a server-provided retry-after
    duration instead of the policy's computed exponential/linear
    backoff, while ``max_retry_backoff`` still applies as a safety
    ceiling so a malicious or malformed header cannot strand a job. The
    delay is honored EXACTLY — no jitter draw: an explicit override
    delay is an explicit direction, and the library never mutates a
    value the classifier specified (jitter spreads only the computed
    curve). A fleet that wants spread on a hint applies it in its own
    classifier (:func:`apply_jitter`).

    A ``delay`` schedules the next attempt; it does not extend the job's
    budget, in either dimension. It does not spare the attempt, the
    retry still counts against ``max_attempts`` unless ``kind`` is also
    set, or the actor raises ``RetryAfter(consume_budget=False)``. And it
    does not move the job's ``schedule_to_close``: an upstream under
    pressure will happily hand back an hour, and if the delay puts the
    next attempt past that deadline the deadline sweep fails the job
    terminally before any worker looks at it. ``max_retry_backoff`` does
    not protect against this, the two bounds mean different things, one
    stopping a single absurd delay and the other stating how long the
    caller still wants the result, and where they disagree
    schedule-to-close wins.
    """

    model_config = ConfigDict(frozen=True)

    kind: RetryKind | None = None
    delay: timedelta | None = Field(
        default=None,
        description=(
            "When to retry, not whether the attempt is charged: a delay alone "
            "still spends one attempt of the job's budget, so a classifier "
            "returning only a delay against a sustained outage exhausts "
            "max_attempts on schedule. Pair it with kind='indefinite' to keep "
            "retrying, or raise RetryAfter(delay, consume_budget=False) from "
            "the actor body for a known-duration wait that spends no budget. "
            "Honored EXACTLY — the library never mutates a value the "
            "classifier specified (jitter spreads only the computed "
            "curve); to spread a hint across a fleet, apply "
            "apply_jitter() to it in your own classifier. Clamped by "
            "max_retry_backoff and floored at MIN_DEFERRAL_INTERVAL — an "
            "explicit delay=timedelta(0) is honored as 'as fast as the "
            "deferral floor allows' (a 1s scheduled requeue), never a "
            "pending-immediate one — but NOT "
            "reconciled with schedule_to_close, a delay landing past that "
            "deadline fails the job terminally through the deadline path."
        ),
    )

    @field_validator("delay")
    @classmethod
    def _validate_delay_non_negative(cls, v: timedelta | None) -> timedelta | None:
        if v is not None and v < timedelta(0):
            raise ValueError(f"delay must be >= 0, got {v}")
        return v


#: The :data:`RetryKind` vocabulary, derived from the alias itself (the
#: ``_OUTCOME_BRANCHES`` pattern in ``taskq.backend._protocol``) so a new kind
#: there is this guard's vocabulary automatically. Consumed only by
#: :func:`_override_shape_error`'s runtime shape check — the ``Literal`` type
#: cannot be consulted at runtime.
_RETRY_KIND_VALUES: Final[frozenset[str]] = frozenset(get_args(RetryKind.__value__))


def _override_shape_error(override: object) -> str | None:
    """The field-shape error of a hook-returned override, or ``None`` when
    the decision path can use it.

    The seams' broken-hook guard (``compose_retry_classifiers`` and
    ``decide_after_failure``) is two-layered: the isinstance half — folded
    in here — catches a return that is not a :class:`RetryOverride` at
    all, and this shape half catches a ``RetryOverride`` built through
    :meth:`RetryOverride.model_construct`, which bypasses pydantic
    validation while passing any isinstance check. A validated
    construction can never fail here (the model's own validators enforce
    both fields), so the check only ever rejects the bypass cases:

    * a ``delay`` that is not a ``timedelta``/``None`` would raise
      (``AttributeError``/``TypeError``) inside
      :meth:`RetryClassifier._retry_decision`'s ceiling arithmetic —
      an escape from the hook-isolation boundary, whose contract is that
      a broken hook is logged and skipped, never propagated;
    * a ``kind`` outside the :data:`RetryKind` vocabulary would fall
      through the decision ladder's kind comparisons into its final
      branch — silently governed as ``indefinite``, the haunt class —
      instead of the declared-policy fallback.

    The returned string names the offending field and what it carried
    (via :func:`safe_repr`, the offending value's own repr can raise) so
    the ``retry-classifier-hook-invalid-return`` warning is actionable.
    """
    if not isinstance(override, RetryOverride):
        return f"not a RetryOverride, a {type(override).__name__}"
    delay = override.delay
    if delay is not None and not isinstance(delay, timedelta):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the declared type is timedelta | None, but this guard exists precisely for a model_construct'd RetryOverride whose fields bypassed pydantic validation — at runtime delay can be anything; the isinstance IS the check.
        return f"delay is a {type(delay).__name__}, not a timedelta | None"
    kind = override.kind
    if kind is None:
        return None
    # The str check first: membership over _RETRY_KIND_VALUES would raise
    # TypeError on an unhashable kind (a list), the very escape this guard
    # exists to absorb.
    if isinstance(kind, str) and kind in _RETRY_KIND_VALUES:  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: same model_construct bypass as the delay check above — the declared RetryKind | None is what an honest construction guarantees, not what a broken hook can deliver.
        return None
    return f"kind is {safe_repr(kind)}, not a RetryKind | None"  # pyright: ignore[reportArgumentType]  # Why: safe_repr is typed BaseException (its callers log hook failures), but repr() itself is object-safe; the guard renders arbitrary hostile field values and must not crash on a raising __repr__.


type RetryClassifierHook = Callable[[BaseException, int], RetryOverride | None]
"""Optional per-actor hook for exception-*instance*-level retry classification.

``non_retryable_exceptions`` and the built-in :class:`PayloadValidationError`
check classify by exception *type* alone. Some integrations need finer
granularity, a single exception type (e.g. an HTTP client's status-code
error) that should retry indefinitely on a 429, fail immediately on a 404,
and use a bounded transient budget on a 5xx, or a server-provided
``Retry-After`` value that should drive the actual backoff delay. Register
one via ``@actor(retry_classifier=...)``.

Invoked with ``(exception, attempt)`` for every exception that survives
the adapter's unconditional-Fail checks: the actor's
``non_retryable_exceptions`` and the built-in ``PayloadValidationError``,
pydantic ``ValidationError``, ``ResultTooLarge``, and
``UnencodableValue`` classes. Return
``None`` to fall back to the actor's static ``RetryPolicy`` unchanged, or a
:class:`RetryOverride` to refine ``kind`` and/or ``delay`` for this specific
occurrence. Exceptions raised by the hook itself are caught and logged by
:func:`decide_after_failure`; classification falls back to the static
policy in that case, a broken hook can never crash the retry pipeline.
The returned override's runtime shape is verified too: an honestly
constructed :class:`RetryOverride` always passes, but a
``model_construct`` bypass of pydantic validation (a ``delay`` that is
not a ``timedelta``, a ``kind`` outside the :data:`RetryKind`
vocabulary) is logged under ``retry-classifier-hook-invalid-return`` and
falls back to the declared policy like any other broken hook — a
malformed override must never raise inside the decision arithmetic or
silently govern as ``indefinite``.
"""


def compose_retry_classifiers(
    *classifiers: RetryClassifierHook,
) -> RetryClassifierHook:
    """Compose classifier hooks into one, first-override-wins.

    Each *classifier* is invoked in registration order with
    ``(exception, attempt)``. The first classifier that returns a
    :class:`RetryOverride` decides the outcome and later classifiers are
    not consulted for that exception; a ``None`` falls through to the
    next classifier; when every classifier returns ``None`` the
    composition returns ``None`` so the actor's declared ``RetryPolicy``
    governs, exactly as a single hook returning ``None`` does.

    Order matters: put the most specific classifier first. A composed
    classifier registered via ``@actor(retry_classifier=...)`` sits at
    the same seam as a single hook, so the adapter's own precedence
    contract is unchanged, ``non_retryable_exceptions`` and the built-in
    unconditional-Fail classes still win over the whole composition.

    Per-classifier isolation (the single-classifier contract, composed):
    a classifier that raises is logged at WARNING and skipped, and
    composition continues with the next classifier, never propagating;
    likewise a classifier returning something that is not a
    :class:`RetryOverride` nor ``None`` — or a ``RetryOverride`` whose
    fields bypassed pydantic validation (``model_construct``), which the
    decision path could not use safely — is logged and skipped. The
    adapter's carve-out applies per classifier: ``KeyboardInterrupt`` and
    ``asyncio.CancelledError`` are never a classifier outcome and
    propagate raw. A classifier that raises is skipped rather than
    aborting the composition because the classifiers are independent
    observers of the same exception, one of them being broken says
    nothing about the others, and falling back to the declared policy on
    the first broken one would silently discard the overrides the healthy
    classifiers were registered to provide.

    ``compose_retry_classifiers()`` with no arguments returns a hook that
    always returns ``None`` (the declared policy governs), so call sites
    can compose a possibly-empty list without a special case.
    """

    def composed(exception: BaseException, attempt: int) -> RetryOverride | None:
        for index, classifier in enumerate(classifiers):
            try:
                override = classifier(exception, attempt)
            except (KeyboardInterrupt, asyncio.CancelledError):
                # The carve-out, exactly as the adapter's single-hook
                # boundary applies it: interpreter/operator intent
                # (KeyboardInterrupt) and shutdown cancellation
                # (CancelledError) are never a classifier outcome; both
                # propagate raw.
                raise
            except BaseException as exc:
                # BaseException, not Exception: classifiers are user code
                # invoked in this frame, and the isolation contract is
                # that a buggy classifier is logged and skipped, never
                # propagated (the same boundary the adapter applies to a
                # single hook). Logged with repr so the record names the
                # classifier's own exception.
                logger: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.retry")
                logger.warning(
                    "retry-classifier-hook-failed",
                    hook="retry_classifier",
                    classifier_index=index,
                    error=safe_repr(exc),
                )
                continue
            if override is None:
                continue
            # The two-layer return guard: not a RetryOverride at all, or a
            # model_construct'd one whose fields bypass pydantic validation
            # (see _override_shape_error for the two hostile shapes — a
            # non-timedelta delay and an off-vocabulary kind — and why each
            # must never reach the decision path). Logged and skipped, the
            # composition continues with the next classifier.
            shape_error = _override_shape_error(override)
            if shape_error is not None:
                logger_invalid: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.retry")
                logger_invalid.warning(
                    "retry-classifier-hook-invalid-return",
                    hook="retry_classifier",
                    classifier_index=index,
                    return_type=type(override).__name__,
                    detail=shape_error,
                )
                continue
            return override
        return None

    return composed


_HTTP_STATUS_429: Final[int] = 429

#: The header names the built-ins sniff, in precedence order: the standard
#: ``Retry-After`` first, the de-facto ``X-Retry-After`` second. Lookup is
#: case-insensitive (see :func:`_header_lookup`).
_RETRY_AFTER_HEADERS: Final[tuple[str, str]] = ("retry-after", "x-retry-after")

#: The seconds-form grammar of a ``Retry-After`` value: digits with an
#: optional ``.``-fraction (``"120"``, ``"0.5"``). A comma decimal
#: (``"1,5"``), a sign (``"+30"``, ``"-30"``), scientific notation
#: (``"1e3"``), and embedded whitespace (``"12 34"``) are NOT the grammar
#: and degrade to the curve fallback like every other garbage value. The
#: grammar is deliberately this small: a rate-limit hint is a human-scale
#: count of seconds, and every parser trick beyond the fraction (locale
#: decimals, exponents) is a divergence surface between consumers, not a
#: feature — a consumer wanting a richer grammar fences the header in
#: their own classifier first (composition order, §5). This grammar is a
#: TIGHTENING of the previous ``int()`` parse, not a preservation: the
#: old parse silently honored a leading sign (``"+30"`` → 30 s), an
#: underscore digit separator (``"1_000"`` → 1000 s), and non-ASCII
#: decimal digits (``"١٢٣"`` → 123 s) — none of them an RFC 9110
#: delta-seconds form (``1*DIGIT``, ASCII); all three are curve-fallback
#: garbage now. Pinned in ``tests/test_rate_limit_claim_kind.py``.
_RETRY_AFTER_SECONDS_PATTERN: Final[re.Pattern[str]] = re.compile(r"[0-9]+(?:\.[0-9]+)?")

#: The parse-level saturation for a finite hint whose value exceeds
#: ``timedelta``'s representable range (a ``timedelta(seconds=10**20)``
#: overflows, and an OverflowError escaping a classifier is exactly the
#: crash the isolation contract exists to absorb). This is NOT a semantic
#: cap: bounding a hint is the operator's ``max_retry_backoff`` ceiling's
#: job (applied to every override delay by the decision path — "a malicious
#: or malformed header cannot strand a job"), and a hint beyond the ceiling
#: CLAMPS there. ``timedelta.max`` is beyond every ceiling a fleet will
#: ever run, so the saturation only keeps the parse crash-free while the
#: operator's knob does the real bounding. (The former one-day parse cap
#: is gone: it misclassified an oversized-but-finite hint as garbage —
#: curve fallback — stealing the clamp from the one knob that exists to
#: express it.)
_REPRESENTABLE_DELAY_MAX: Final[timedelta] = timedelta.max


def _extract_http_status(exception: BaseException) -> int | None:
    """The duck-typed HTTP status extraction shared by the built-in
    classifiers, no import required (nothing under ``taskq`` imports an
    HTTP client).

    Recognized shapes, in check order: an exception carrying
    ``.response.status_code`` (httpx / httpx2 / requests'
    ``HTTPStatusError``/``HTTPError``), ``.response.status`` or a bare
    ``.status`` on the exception (aiohttp's ``ClientResponseError``), or a
    bare ``.status_code``. A non-int attribute (a mock, a property that
    returns a string) is skipped, not trusted.
    """
    response = getattr(exception, "response", None)
    if response is not None:
        for attr in ("status_code", "status"):
            value = getattr(response, attr, None)
            if isinstance(value, int):
                return value
    for attr in ("status_code", "status"):
        value = getattr(exception, attr, None)
        if isinstance(value, int):
            return value
    return None


def _header_lookup(headers: object, name: str) -> str | None:
    """Case-insensitive single-header lookup over the duck-typed header
    containers.

    The real containers (httpx's ``Headers``, requests'
    ``CaseInsensitiveDict``, aiohttp's ``CIMultiDict``) implement a
    case-insensitive ``.get`` — that is the fast path. A plain dict (the
    hand-rolled / test shape) is case-sensitive, so the lookup falls
    through to an ``.items()`` scan. A value that is not a non-empty
    string is not a header value and reads as absent.
    """
    getter = getattr(headers, "get", None)
    if callable(getter):
        value: Any = getter(name)
        if isinstance(value, str) and value:
            return value
    items = getattr(headers, "items", None)
    if callable(items):
        lowered = name.lower()
        pairs: Any = items()
        for key, value in pairs:
            if isinstance(key, str) and key.lower() == lowered and isinstance(value, str) and value:
                return value
    return None


def _extract_retry_after_header(exception: BaseException) -> str | None:
    """The server-supplied retry hint, duck-typed over the common shapes.

    ``exception.response.headers`` (httpx / httpx2 / requests) and a bare
    ``exception.headers`` (aiohttp's ``ClientResponseError``, or any
    exception carrying headers directly). Only ever called on an
    already-claimed signal — the parse path must not run on the miss path
    (every exception an actor raises crosses this classifier; only the
    claimed ones pay for header sniffing).
    """
    response = getattr(exception, "response", None)
    if response is not None:
        headers = getattr(response, "headers", None)
        if headers is not None:
            for name in _RETRY_AFTER_HEADERS:
                value = _header_lookup(headers, name)
                if value is not None:
                    return value
    headers = getattr(exception, "headers", None)
    if headers is not None:
        for name in _RETRY_AFTER_HEADERS:
            value = _header_lookup(headers, name)
            if value is not None:
                return value
    return None


def _parse_retry_after(value: str, *, now: datetime) -> timedelta | None:
    """Parse a ``Retry-After`` header value into a delay.

    Recognized forms: the decimal-fraction seconds form (``"120"``,
    ``"0.5"`` — see :data:`_RETRY_AFTER_SECONDS_PATTERN` for the exact
    grammar and its deliberate limits) and the HTTP-date (RFC 9110
    IMF-fixdate, via ``email.utils.parsedate_to_datetime``; a naive
    date — the ``-0000`` zone — is read as UTC). ``*now`` turns the date
    form into a delay; the classifier injects ``datetime.now(UTC)`` so
    tests (and callers with their own clock domain) can pin it.

    Returns ``None`` — the curve-fallback signal — for every unusable
    value: empty or unparsable text, and a zero or negative delay (zero
    would otherwise degenerate into the monopolisation loop the decision
    floor exists to prevent). A FINITE usable hint is always returned,
    however large — bounding it is not this parse's job: the operator's
    ``max_retry_backoff`` ceiling clamps every override delay at the
    decision path (a hint beyond the ceiling clamps to the ceiling), and
    a value beyond ``timedelta``'s own range saturates at
    :data:`_REPRESENTABLE_DELAY_MAX` so the parse can never crash a
    classifier. Garbage can never *break* the classification, only
    remove the delay half of it.
    """
    text = value.strip()
    if not text:
        return None
    if _RETRY_AFTER_SECONDS_PATTERN.fullmatch(text) is not None:
        # The pattern admits only digits and one dot, so float() yields a
        # finite float or inf and never raises here; the timedelta
        # constructor raises OverflowError beyond its representable range
        # (inf included) and the parse saturates instead of escaping.
        try:
            delay = timedelta(seconds=float(text))
        except OverflowError:
            delay = _REPRESENTABLE_DELAY_MAX
    else:
        try:
            when = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        try:
            delay = when - now
        except OverflowError:  # a date beyond timedelta's range: saturate, never crash
            delay = _REPRESENTABLE_DELAY_MAX
    if delay <= timedelta(0):
        return None
    return delay


def rate_limit_aware_classifier(
    exception: BaseException,
    attempt: int,
    *,
    now: datetime | None = None,
) -> RetryOverride | None:
    """Built-in classifier for *application-level* rate-limit raises.

    TaskQ's own rate limiting denies at claim time (the job is
    rescheduled, no attempt is charged) and never reaches this
    classifier; what this classifier recognizes is the moment an
    application's *outgoing* call is rate-limited and the actor lets the
    signal propagate, the shape the cbre-pfc migration audit (W3.H/F1)
    found consumers hand-rolling: without it a declared ``transient``
    policy burns one attempt of ``max_attempts`` per 429 and the job dies
    after a few tens of seconds (base=5s exponential, max_attempts=3: two
    delays, 5s + 10s — the 20s rung is never reached, the third failure is
    terminal and schedules no delay), when the operator's intent for a rate
    limit is "wait it out", the unbounded-in-attempts behaviour only an
    ``indefinite`` kind provides.

    Recognized signals, in check order:

    * TaskQ's own :class:`~taskq.exceptions.ReservationUnavailable` raised
      with ``source="rate_limit"``: a shared limiter's denial surfacing in
      the actor's frame. A ``source="reservation"`` denial is a
      concurrency-slot condition, not a rate limit, and is NOT recognized.
    * Common HTTP 429 duck-types, no import required: an exception carrying
      ``.response.status_code == 429`` (httpx / httpx2 / requests'
      ``HTTPStatusError``/``HTTPError`` shapes), ``.response.status == 429``
      or a bare ``.status == 429`` on the exception (aiohttp's
      ``ClientResponseError``), or a bare ``.status_code == 429``.
    * An exception whose class is literally named ``RateLimitError`` (the
      openai/anthropic-style SDK shape), even when it carries no HTTP
      status attribute.

    On a claimed signal the classifier sniffs the server's retry hint —
    the ``retry-after`` and ``x-retry-after`` headers, case-insensitive,
    on ``exception.response.headers`` (httpx/requests) or a bare
    ``exception.headers`` (aiohttp) — and parses it as a decimal-fraction
    seconds value (``"120"``, ``"0.5"``) or an HTTP-date (see
    :func:`_parse_retry_after`).

    Everything else returns ``None``: the declared policy governs, so a
    500 keeps its bounded transient budget and a 404 its non-retryable
    verdict even when this classifier is composed in. Header sniffing
    runs only on a claimed signal: the miss path (the path every ordinary
    exception takes) does no ``getattr`` chains beyond the status
    duck-typing and allocates nothing (pinned by
    ``tests/perf/test_retry_classifier_miss_benchmark.py`` and the
    tracemalloc pin in ``tests/test_retry_after_header_sniffing.py``).

    The override and what bounds it (the full table lives in
    ``docs/guides/retries.md`` §5):

    * a parsed hint → ``RetryOverride(kind="indefinite", delay=hint)``:
      the hint flows through as the override delay VERBATIM (honored
      EXACTLY — no jitter draw: an explicit delay is an explicit
      direction, the library never mutates a value the classifier
      specified) and meets the documented bounds on the decision path —
      clamped to ``max_retry_backoff`` by
      :meth:`RetryClassifier._retry_decision` (verified, not assumed —
      the same path any ``RetryOverride.delay`` takes; the clamp reads
      the verbatim raw value), then floored at
      ``MIN_DEFERRAL_INTERVAL``. A finite hint beyond the ceiling CLAMPS
      to it — the ceiling's documented job ("a malicious or malformed
      header cannot strand a job"), the operator's knob doing the
      bounding. The parse treats only garbage shapes as garbage (zero,
      negative, or no grammar match — the closed decimal-fraction
      grammar is a deliberate tightening of the former ``int()`` parse's
      accidents); an oversized-but-finite hint is the ceiling's input,
      not garbage. The classifier adds no ceiling of its own.
    * a claimed signal with no usable hint (no header, or the
      curve-fallback garbage cases: zero / negative / unparsable) →
      ``RetryOverride(kind="indefinite")`` with no ``delay``:
      the declared policy's backoff curve (with jitter, cap, and
      ``max_retry_backoff``) keeps computing *when* to retry, and the
      job's ``schedule_to_close`` (``time_budget`` for an indefinite
      policy) stays the single stopping condition, the same bound every
      ``indefinite`` job has. Kind only, because the trap being fixed is
      the attempt budget, not the curve.

    Hazard: that stopping condition must exist. A ``transient`` actor is
    never stamped with a ``schedule_to_close`` (``time_budget`` is only
    honored for an ``indefinite``-declared policy; see
    :func:`time_budget_as_interval`), so composing this classifier into a
    ``transient`` actor makes a sustained 429 storm retry the job forever
    — no attempt ceiling and no deadline. The parsed ``delay`` does not
    change this: it schedules *when* the next attempt lands, it does not
    move the job's ``schedule_to_close``, and a delay landing past that
    deadline still fails the job terminally in the deadline path. Give
    the actor a stopping condition: declare it ``kind="indefinite"`` with
    a ``time_budget``, put a domain classifier before this one that
    bounds the 429s, pass a per-enqueue ``schedule_to_close``, or use the
    factory's bounded mode —
    :func:`make_rate_limit_aware_classifier(claim_kind="transient")`
    keeps ``max_attempts`` as the stopper while still honoring the
    server's hint.

    This built-in is the ``claim_kind="indefinite"`` instance of
    :func:`make_rate_limit_aware_classifier`, the factory for consumers
    whose intent differs: ``claim_kind="transient"`` for 429 → BOUNDED
    retry (the server's hint honored, ``max_attempts`` the stopper), and
    ``claim_kind=None`` for a never-claiming identity that slots into a
    composition unchanged.

    Compose it after your domain-specific classifiers:
    ``compose_retry_classifiers(my_domain_classifier, rate_limit_aware_classifier)``;
    a more specific classifier registered before it wins by composition
    order, and one registered after it never sees a 429 this classifier
    claimed.

    ``now`` injects the clock for HTTP-date parsing (tests, or a caller
    with its own clock domain); the default reads
    ``datetime.now(UTC)`` — this is the module's one clock read,
    never touched on the miss path.
    """
    return _rate_limit_claim(exception, claim_kind="indefinite", now=now)


def _rate_limit_claim(
    exception: BaseException,
    *,
    claim_kind: Literal["indefinite", "transient"] | None,
    now: datetime | None,
) -> RetryOverride | None:
    """The one claim implementation behind the built-in and the factory:
    recognition, hint sniffing, and parsing are SHARED (never forked per
    mode) — ``claim_kind`` only decides the kind stamped on the override.
    A ``None`` claim kind claims nothing, the identity for composition.
    """
    if claim_kind is None:
        # The identity mode: no claims, so a composition slot holding
        # this classifier behaves exactly like an empty slot.
        return None

    if isinstance(exception, ReservationUnavailable):
        if exception.source == "rate_limit":
            return RetryOverride(kind=claim_kind)
        return None

    status = _extract_http_status(exception)
    if status != _HTTP_STATUS_429 and type(exception).__name__ != "RateLimitError":
        return None

    # Claimed: sniff the server's hint. Unusable → the curve-fallback
    # (kind-only) override, exactly the pre-sniffing behavior.
    header = _extract_retry_after_header(exception)
    if header is not None:
        delay = _parse_retry_after(header, now=now if now is not None else datetime.now(UTC))
        if delay is not None:
            return RetryOverride(kind=claim_kind, delay=delay)
    return RetryOverride(kind=claim_kind)


def make_rate_limit_aware_classifier(
    claim_kind: Literal["indefinite", "transient"] | None = "indefinite",
) -> RetryClassifierHook:
    """Build a rate-limit classifier whose CLAIM is configurable — the
    factory behind :func:`rate_limit_aware_classifier` (which is exactly
    ``make_rate_limit_aware_classifier()`` at the default, kept as a
    module-level name so existing registrations and pins are untouched).

    The recognition surface is identical for every mode — TaskQ's own
    :class:`~taskq.exceptions.ReservationUnavailable` with
    ``source="rate_limit"``, the HTTP 429 duck-types, and the
    ``RateLimitError``-named shape — and so are the hint parsing, the
    garbage rules, and the bounds (the operator's ``max_retry_backoff``
    ceiling and the ``MIN_DEFERRAL_INTERVAL`` floor, applied by the
    decision path to every override delay). Only the override's KIND
    changes; see the mode table:

    ================  =============================================  =============================================  =========================================
    ``claim_kind``    Override on a claimed signal                   What bounds it                                 The haunt hazard (per mode)
    ================  =============================================  =============================================  =========================================
    ``"indefinite"``  ``RetryOverride(kind="indefinite")``,          ``schedule_to_close`` — the delay schedules    A ``transient`` actor has no
                      ``delay=hint`` when the server sent one        *when*, never *whether*: a deadline is         ``schedule_to_close`` (``time_budget`` is
                                                                     REQUIRED. Declare the actor                    only honored for an ``indefinite``-declared
                                                                     ``kind="indefinite"`` with a ``time_budget``,  policy), so a sustained 429 storm retries
                                                                     or pass ``schedule_to_close=`` per enqueue.    the job FOREVER — no attempt ceiling, no
                                                                                                                    deadline. This mode's intent is "wait it
                                                                                                                    out"; a stopping condition must exist.
    ``"transient"``   ``RetryOverride(kind="transient")``,           ``max_attempts`` — the budget STAYS the        None: the attempt budget terminates the
                      ``delay=hint`` when the server sent one        stopper; the hint (when present) sets          storm even with no deadline. The intent
                                                                     *when* within it. The bounded AND              this mode exists for: "429 → bounded
                                                                     server-honoring shape.                         retry, and honor the server's when".
    ``None``          never claims — returns ``None`` for every      the declared policy governs everything         None: the classifier contributes no claims
                      input                                          (the identity for composition)                 to compose over; it only reserves the
                                                                                                                    recognition surface for future modes.
    ================  =============================================  =============================================  =========================================

    Validation is at construction: a ``claim_kind`` outside the three
    modes raises ``ValueError`` here — fail loud at build time, never a
    silently-misclaiming classifier at override time. ``"non_retryable"``
    is deliberately NOT a mode: a rate limit means "retry later" by
    definition (§4), so stamping one non-retryable is the misclaim this
    factory exists to make impossible — build that intent with a domain
    classifier registered *before* this one (composition order), where
    the decision is visible in your own code.

    The returned hook carries the built-in's injectable ``now``
    (keyword-only, defaulting to the real clock) so tests and custom
    clock domains pin HTTP-date parsing exactly as they pin the
    built-in's. The ``attempt`` argument is the
    :data:`RetryClassifierHook` protocol's required arity and is
    deliberately unread — the verdict never depends on the attempt
    count (pinned by the identity battery's ``attempt`` sweep).

    Why the transient mode exists: the built-in's indefinite claim is
    the haunt class for a ``transient`` actor with no ``time_budget``
    (the §5 danger block). A consumer who wants 429 → BOUNDED retry must
    not have to hand-roll a classifier to say so — build this mode, keep
    the shared parsing and bounds, and ``max_attempts`` does the
    stopping.

    Example::

        bounded_rate_limit = make_rate_limit_aware_classifier(claim_kind="transient")
        compose_retry_classifiers(my_domain_classifier, bounded_rate_limit)
    """
    if claim_kind not in ("indefinite", "transient", None):
        raise ValueError(
            f"claim_kind must be 'indefinite', 'transient', or None, got {claim_kind!r}; "
            "a typo would silently misclaim every 429 the classifier sees"
        )

    def classify(
        exception: BaseException,
        attempt: int,
        *,
        now: datetime | None = None,
    ) -> RetryOverride | None:
        return _rate_limit_claim(exception, claim_kind=claim_kind, now=now)

    return classify


# ── failure taxonomy: the configurable common-shapes classifier ────────
#
# The defaults are documented module constants (frozen; build a variant
# from them rather than mutating). The status sets REPLACE the built-in
# defaults wholesale when passed explicitly — there is no implicit merge,
# so what a classifier claims is always exactly what its configuration
# says.


#: Statuses the taxonomy claims ``transient``: 408 Request Timeout and 425
#: Too Early (the two 4xx the §4 table calls retryable-because-timing) plus
#: the whole 5xx band (server-side, usually recovers).
DEFAULT_TRANSIENT_STATUSES: Final[frozenset[int]] = frozenset({408, 425, *range(500, 600)})

#: Statuses the taxonomy claims ``non_retryable``: the 4xx band minus the
#: carve-outs {408, 425} (transient above) and {429} — 429 is the single
#: most important status to RETRY (see the §4 danger block) and belongs to
#: :func:`rate_limit_aware_classifier`; the taxonomy deliberately claims
#: nothing for it so the two compose safely in either order.
DEFAULT_NON_RETRYABLE_STATUSES: Final[frozenset[int]] = frozenset(
    status for status in range(400, 500) if status not in {408, 425, 429}
)

#: Exception class names the taxonomy claims ``transient`` by EXACT name
#: (never by suffix — the suffix inference is opt-in via
#: ``infer_timeout_by_suffix=True``): the stdlib timeout/connection names
#: (``TimeoutError``, the builtin ``ConnectionError`` family — also
#: matched structurally by ``isinstance``, which is type-based and stays
#: default) and the major HTTP clients' exact timeout/transport class
#: names, so httpx/requests/aiohttp outages claim without an import:
#: ``ReadTimeout``/``ConnectError`` (httpx), ``ConnectTimeout``
#: (requests), ``ServerTimeoutError`` (aiohttp), ``WriteTimeout``/
#: ``PoolTimeout`` (httpx pools). ``Timeout`` exact matches the bare
#: class name some AMQP/mqtt clients use. Exact-name matching is the
#: contract: a name ending in ``Timeout`` that is not listed here (the
#: audit's counterexample: ``pymongo.errors.ExecutionTimeout``) is NOT
#: claimed unless the consumer opts into the suffix inference.
DEFAULT_TRANSIENT_EXCEPTION_NAMES: Final[frozenset[str]] = frozenset(
    {
        "Timeout",
        "TimeoutError",
        "ConnectError",
        "ConnectTimeout",
        "ReadTimeout",
        "WriteTimeout",
        "PoolTimeout",
        "ServerTimeoutError",
        "ConnectionError",
        "ConnectionResetError",
        "ConnectionRefusedError",
        "ConnectionAbortedError",
    }
)

#: Exception class names the taxonomy refuses to claim. Empty by default;
#: populated per actor for a domain class whose name matches a default or
#: timeout-shaped signal but whose semantics disagree.
DEFAULT_EXCLUDED_EXCEPTION_NAMES: Final[frozenset[str]] = frozenset()


def failure_taxonomy_classifier(
    *,
    transient_status: Iterable[int] | None = None,
    non_retryable_status: Iterable[int] | None = None,
    include_names: Iterable[str] | None = None,
    exclude_names: Iterable[str] | None = None,
    infer_timeout_by_suffix: bool = False,
) -> RetryClassifierHook:
    """Configurable classifier for the common failure shapes, the
    conservative default the §4 table describes.

    Where :func:`rate_limit_aware_classifier` owns the one signal with
    dedicated semantics (429/rate-limit → ``indefinite``), this classifier
    owns the mundane taxonomy: the shapes a declared ``transient`` policy
    *should* govern but a single exception type cannot express
    per-instance.

    Claims, in check order:

    1. ``exclude_names`` — an exception whose class name is listed returns
       ``None`` whatever else matches (an explicit exclusion outranks
       every signal, status included).
    2. ``transient`` → :class:`RetryOverride(kind="transient")` — claimed
       by any of:
       * connection-class: ``isinstance`` of the builtin
         ``ConnectionError`` family (``ConnectionResetError``,
         ``ConnectionRefusedError``, …);
       * TimeoutError-shaped: ``isinstance`` of the builtin
         ``TimeoutError`` (covers ``socket.timeout`` and, since 3.11,
         ``asyncio.TimeoutError``) — a TYPE-based signal, always on — or
         an exact class name in ``include_names`` or the
         ``DEFAULT_TRANSIENT_EXCEPTION_NAMES`` curated set;
       * ``infer_timeout_by_suffix=True`` → additionally, a class name
         ENDING in ``Timeout``/``TimeoutError`` claims transient. This is
         the opt-in convenience flag — OFF by default, and the default is
         a contract: defaults never override explicit direction, so the
         library will not widen a consumer's narrowly pinned semantics.
         The cost the flag buys: it is a name heuristic, not a semantics
         check, and its proven counterexample is
         ``pymongo.errors.ExecutionTimeout`` — the server killed the
         operation for exceeding ``maxTimeMS``, and a re-run of the same
         query re-fails deterministically — whose name ends in ``Timeout``
         but whose meaning is "this work can never succeed". Under the
         default exact-name matching the taxonomy returns ``None`` for it
         (the declared policy governs); with the flag on it is claimed
         ``transient`` and every such retry burns budget on unwinnable
         work. Consumers who route these shapes narrowly (static-policy,
         pinned per class) keep the default; consumers who want the
         httpx/requests/aiohttp-style convenience without enumerating
         names take the flag knowingly;
       * status in ``transient_status`` (defaults:
         ``DEFAULT_TRANSIENT_STATUSES`` = 408, 425, and the 5xx band),
         duck-typed through the same status shapes the built-in reads.
    3. ``non_retryable`` → ``RetryOverride(kind="non_retryable")`` —
       status in ``non_retryable_status`` (defaults:
       ``DEFAULT_NON_RETRYABLE_STATUSES`` = 4xx minus {408, 425, 429}). A
       status present in both sets is transient — check order decides,
       and "retry later" is the safer wrong answer than killing a
       retryable job.
    4. Anything else → ``None``. The conservative default: over-claiming
       is the haunt class.

    Configuration replaces, never merges: passing
    ``transient_status=frozenset({429})`` claims ONLY 429 (the 5xx band
    and 408/425 stop being transient); ``include_names`` replaces the
    default name set (extend ``DEFAULT_TRANSIENT_EXCEPTION_NAMES | {"Yours"}``
    to keep the defaults). Build the classifier once at registration and
    reuse it — the sets are frozen at build time, per-call cost is a few
    frozenset lookups.

    Why the taxonomy never returns ``indefinite``: an ``indefinite``
    override has no attempt ceiling, and its single stopping condition is
    the job's ``schedule_to_close`` — which a ``transient`` actor never
    has stamped (``time_budget`` is only honored for an
    ``indefinite``-declared policy), so composing an
    ``indefinite``-claiming classifier into a ``transient`` actor retries
    the job forever: no attempt ceiling, no deadline, no warning at
    registration or override time. That is the haunt this module's §5
    danger block documents for the one built-in that genuinely needs the
    kind (a rate limit means "wait it out"). The taxonomy's signals carry
    no such semantics: a connection reset or a 503 is what
    ``max_attempts``-bounded retrying is FOR, so every claim here is
    ``transient`` (bounded by the declared budget) or ``non_retryable``
    (terminal), and the declared policy keeps governing everything the
    taxonomy is unsure about. Pin:
    ``tests/test_failure_taxonomy_classifier.py::test_taxonomy_never_returns_indefinite``.
    """
    transient = (
        DEFAULT_TRANSIENT_STATUSES if transient_status is None else frozenset(transient_status)
    )
    non_retryable = (
        DEFAULT_NON_RETRYABLE_STATUSES
        if non_retryable_status is None
        else frozenset(non_retryable_status)
    )
    include = (
        DEFAULT_TRANSIENT_EXCEPTION_NAMES if include_names is None else frozenset(include_names)
    )
    exclude = (
        DEFAULT_EXCLUDED_EXCEPTION_NAMES if exclude_names is None else frozenset(exclude_names)
    )
    # Opt-in only: the default path matches exact curated names. The
    # suffix heuristic widens retry verdicts for shapes consumers
    # deliberately route narrowly (the ExecutionTimeout counterexample in
    # the docstring), so it never runs unless asked for.
    infer_suffix = infer_timeout_by_suffix

    def classify(exception: BaseException, attempt: int) -> RetryOverride | None:
        name = type(exception).__name__
        if name in exclude:
            return None
        status = _extract_http_status(exception)
        if (
            isinstance(exception, (ConnectionError, TimeoutError))
            or name in include
            or (infer_suffix and name.endswith(("Timeout", "TimeoutError")))
            or (status is not None and status in transient)
        ):
            return RetryOverride(kind="transient")
        if status is not None and status in non_retryable:
            return RetryOverride(kind="non_retryable")
        return None

    return classify


class RetryClassifier:
    """Pure classifier that maps an exception + policy to a RetryDecision.

    The classifier decides retry-*kind* and backoff only, it is
    deliberately NOT a deadline arbiter.  ``schedule_to_close`` is
    arbitrated by the SQL guard in ``mark_failed_or_retry`` (single
    arbiter, the backend's clock); a Python-side pre-check computed from
    the worker's clock would disagree with it under app↔DB skew and kill
    jobs early (or rubber-stamp them).
    """

    @staticmethod
    def _retry_decision(
        policy: RetryPolicy,
        attempt: int,
        *,
        max_retry_backoff: timedelta,
        override_delay: timedelta | None = None,
    ) -> Retry:
        """The Retry decision, computed curve or override delay.

        An override delay (a hook's ``RetryOverride(delay=...)`` — e.g. a
        server's ``Retry-After`` hint) is an explicit direction: it is
        honored EXACTLY, with no jitter draw — the pre-#656 contract,
        restored. #656 spread the override through the same
        multiplicative-symmetric band as a computed curve value, which
        mutated a value the user's classifier specified (a default
        ``jitter=0.2`` turned a 90s ``Retry-After`` into a draw from
        ``[72s, 108s]``, half of it *before* the server's horizon). Under
        the maintainer's law — defaults never get in the way of the
        user's explicit direction — the delay passes through verbatim,
        still clamped to ``max_retry_backoff`` (the operator's safety
        ceiling against a malicious or malformed header) and floored at
        :data:`MIN_DEFERRAL_INTERVAL` like every other delay. A fleet
        that wants spread on a hint applies it in its own classifier
        (:func:`apply_jitter`); jitter keeps spreading only the computed
        curve, where the TaskQ-chosen raw value is the thing being
        softened. ``jitter=0.0`` remains the identity on both paths.
        """
        if override_delay is not None:
            cap_s = max_retry_backoff.total_seconds()
            raw_s = min(override_delay.total_seconds(), cap_s)
            delay = timedelta(seconds=raw_s)
        else:
            delay = compute_backoff(policy, attempt, max_retry_backoff=max_retry_backoff)
        # The monopolisation floor the deferral arms apply at their writes
        # (mark_snoozed / the non-consuming retry-after arm, via
        # MIN_DEFERRAL_INTERVAL): a failure-retry delay below it requeues
        # the job at the head of dispatch order, one claim/run/fail round
        # trip per cycle holding a worker slot, and an ``indefinite``
        # policy has no attempt ceiling to bound the cycle count. A
        # zero/near-zero ``base`` (or a zero Retry-After override)
        # degenerates the curve to exactly that, so the decision itself
        # never carries a sub-floor delay; the write arms floor again as
        # their own defense-in-depth, the same two-layer shape the
        # deferral family ships.
        return Retry(retry_delay=max(delay, MIN_DEFERRAL_INTERVAL))

    @staticmethod
    def classify(
        policy: RetryPolicy,
        non_retryable_exceptions: tuple[type[BaseException], ...],
        exception: BaseException,
        attempt: int,
        *,
        max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
        override: RetryOverride | None = None,
    ) -> RetryDecision:
        if isinstance(exception, non_retryable_exceptions):
            return Fail(error_class=type(exception).__name__, retryable=False)

        if isinstance(exception, PayloadValidationError):
            return Fail(error_class="PayloadValidationError", retryable=False)

        # Why: the actor already ran to completion, the failure is the size
        # of the value it returned, which a re-run reproduces exactly. Left
        # retryable, a single oversized result burns every remaining attempt
        # (re-running the actor's side effects each time) before landing in
        # 'failed' anyway.
        if isinstance(exception, ResultTooLarge):
            return Fail(error_class="ResultTooLarge", retryable=False)

        # Why the same contract for the encoding half: the actor already
        # ran to completion, the value it returned is one no UTF-8 JSON
        # encoding accepts (a lone surrogate, a non-str dict key), and a
        # re-run reproduces it exactly. Left retryable, a single
        # unencodable result burns every remaining attempt re-running the
        # actor's side effects before landing in 'failed' anyway.
        if isinstance(exception, UnencodableValue):
            return Fail(error_class="UnencodableValue", retryable=False)

        if isinstance(exception, ValidationError):
            return Fail(error_class="PayloadValidationError", retryable=False)

        effective_kind = (
            override.kind if override is not None and override.kind is not None else policy.kind
        )
        override_delay = override.delay if override is not None else None

        if effective_kind == "non_retryable":
            return Fail(error_class=type(exception).__name__, retryable=False)

        if effective_kind == "transient":
            if attempt < policy.max_attempts:
                return RetryClassifier._retry_decision(
                    policy,
                    attempt,
                    max_retry_backoff=max_retry_backoff,
                    override_delay=override_delay,
                )
            return Fail(error_class=type(exception).__name__, retryable=False)

        # effective_kind == "indefinite"
        return RetryClassifier._retry_decision(
            policy,
            attempt,
            max_retry_backoff=max_retry_backoff,
            override_delay=override_delay,
        )


def time_budget_as_interval(retry: RetryPolicy) -> timedelta | None:
    """Return retry.time_budget when kind=='indefinite' and time_budget
    is set; otherwise None. Used by the enqueue path to pass
    time_budget as a `$N::interval` parameter so PG can compute
    schedule_to_close = clock_timestamp() + $N::interval."""
    if retry.kind == "indefinite" and retry.time_budget is not None:
        return retry.time_budget
    return None


# ── Adapter layer (consumer-loop wiring) ────────────────────────────────


type OnRetryExhausted = Callable[
    [JobRow, BaseException],
    Awaitable[None] | None,
]
"""Hook fired when a job exhausts its retry budget.

Why ``JobRow`` (not generic ``JobRow[P]``): the hook is dispatched from
the consumer loop, which knows only the raw ``JobRow`` with
``payload: dict[str, object]``. Making the hook generic over ``P``
would require the consumer to track the original ``ActorRef`` for every
in-flight job, possible, but it propagates type parameters into the
registry for negligible benefit. Hooks that need a typed payload
re-validate via ``actor_ref.payload_type.model_validate(job_row.payload)``.
This is the documented payload-erasure boundary; see
the documented payload-erasure boundary.
"""


type OnSuccess = Callable[[JobRow, object], Awaitable[None] | None]
"""Hook fired when a job succeeds. Receives ``(job_row, result)``.

Why ``object`` for the result type (not ``Any`` or generic ``R``): the
hook is dispatched from the consumer loop, which erases the actor's
return type to ``object``. This mirrors the non-generic
:data:`OnRetryExhausted` at the same payload-erasure boundary. Hooks
that need a typed result re-validate via the actor's
``result_adapter``.
"""


type OnCancel = Callable[[JobRow], Awaitable[None] | None]
"""Hook fired when a job ends cancelled. Receives the terminal ``JobRow``.

No result argument, unlike :data:`OnSuccess`: an actor that abandoned
its unit of work produced none. The row is the terminal one, so a hook
reading ``status`` sees ``cancelled``.

The hook fires only for a job that reached a worker and was cancelled
while running. A job cancelled while still ``pending`` or ``scheduled``
never enters a worker, so no hook of any kind can run for it, that
bookkeeping stays with whoever issued the cancel.
"""


class ActorConfigLike(Protocol):
    """Structural shape the adapter needs from the per-actor registration
    record. The eventual concrete ActorConfig class will
    satisfy this protocol structurally.

    Attributes are declared as read-only properties because the concrete
    ActorConfig will be a frozen Pydantic model; writable Protocol
    attributes would not be satisfiable by any immutable class.
    """

    @property
    def retry(self) -> RetryPolicy: ...

    @property
    def non_retryable_exceptions(self) -> tuple[type[BaseException], ...]: ...

    @property
    def retry_classifier(self) -> RetryClassifierHook | None: ...

    @property
    def on_retry_exhausted(self) -> OnRetryExhausted | None: ...

    @property
    def on_retry_exhausted_timeout(self) -> float: ...  # seconds; default 3.0

    @property
    def on_success(self) -> OnSuccess | None: ...

    @property
    def on_success_timeout(self) -> float: ...  # seconds; default 3.0

    @property
    def on_cancel(self) -> OnCancel | None: ...

    @property
    def on_cancel_timeout(self) -> float: ...  # seconds; default 3.0


def decide_after_failure(
    actor_config: ActorConfigLike,
    exception: BaseException,
    job_state: JobRetryState,
    *,
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
    log: structlog.stdlib.BoundLogger | None = None,
) -> RetryDecision:
    """Adapter between the pure classifier and the consumer loop.

    Reconstructs a RetryPolicy from row-stored max_attempts / retry_kind
    (authoritative) combined with live-registration scalars
    (backoff, base, cap, jitter, time_budget) that are not stored on the
    row, reusing the registered policy object directly when the row
    agrees with it and the registered policy satisfies the cap>=base
    invariant, so the common no-drift path skips per-failure pydantic
    validation. Any row/registration mismatch falls through to the full
    constructor, which fails loud. If the actor registered a
    ``retry_classifier`` hook, invokes it to get a per-exception
    :class:`RetryOverride`, then delegates to RetryClassifier.classify.

    ``max_retry_backoff`` is the global ceiling forwarded to
    ``compute_backoff``. The consumer passes
    ``settings.max_retry_backoff`` so the knob is operator-controlled.

    No clock input: the classifier decides retry-kind and backoff only ,
    the ``schedule_to_close`` deadline is arbitrated server-side by
    ``mark_failed_or_retry`` (see RetryClassifier's docstring).
    """
    # row-stored max_attempts and retry_kind are authoritative;
    # live registration is authoritative for the other policy scalars
    # and for exception types.
    registered = actor_config.retry
    if (
        job_state.retry_kind == registered.kind
        and job_state.max_attempts == registered.max_attempts
        and registered.cap >= registered.base
    ):
        # No drift: reconstructing from `registered`'s own scalars would
        # yield a policy field-for-field equal to it, so reuse the frozen
        # registered policy instead of re-validating it per failure
        # (~1.5µs per reconstruction measured). Trust boundary: the
        # cap>=base check preserves the fail-loud contract for a
        # registration that bypassed validation (model_construct; pinned
        # by B-TG-11), and an unknown row retry_kind fails the equality
        # check against a valid registered kind, landing on the
        # constructor path, which still raises ValidationError.
        reconstructed_policy = registered
    else:
        reconstructed_policy = RetryPolicy(
            kind=job_state.retry_kind,
            # Why: clamp the ROW-stored value into the constructor's domain.
            # The enqueue-time guard refuses max_attempts above
            # MAX_ENQUEUABLE_MAX_ATTEMPTS for fresh policies, but committed
            # rows written by earlier releases can legally sit at the
            # smallint ceiling: their snooze arms' saturating increment
            # parked a snoozed 32766-job at 32767. Feeding that row value
            # straight into the fail-loud constructor would crash the
            # consumer's failure path on a row the system itself wrote; the
            # clamp's only semantic cost is the single classification
            # boundary at the very top of the smallint domain, strictly
            # better than turning a legal row state into a ValidationError.
            max_attempts=min(job_state.max_attempts, MAX_ENQUEUABLE_MAX_ATTEMPTS),
            backoff=registered.backoff,
            base=registered.base,
            cap=registered.cap,
            jitter=registered.jitter,
            time_budget=registered.time_budget,
        )

    override: RetryOverride | None = None
    if actor_config.retry_classifier is not None and not isinstance(
        exception,
        (
            *actor_config.non_retryable_exceptions,
            PayloadValidationError,
            ValidationError,
            ResultTooLarge,
            UnencodableValue,
        ),
    ):
        try:
            override = actor_config.retry_classifier(exception, job_state.attempt)
            # The two-layer return guard (see _override_shape_error): not a
            # RetryOverride at all, or a model_construct'd one whose fields
            # bypass pydantic validation. The shape half is what keeps a
            # non-timedelta override delay out of _retry_decision's ceiling
            # arithmetic (an AttributeError/TypeError mid-classify — the
            # escape this boundary exists to absorb) and an off-vocabulary
            # kind out of the decision ladder's final branch (which would
            # silently govern it as indefinite, the haunt class).
            shape_error = None if override is None else _override_shape_error(override)
            if shape_error is not None:
                logger: structlog.stdlib.BoundLogger = (
                    log if log is not None else structlog.get_logger("taskq.retry")
                )
                logger.warning(
                    "retry-classifier-hook-invalid-return",
                    hook="retry_classifier",
                    return_type=type(override).__name__,
                    detail=shape_error,
                )
                override = None
        except (KeyboardInterrupt, asyncio.CancelledError):
            # The carve-out, exactly as the consumer's attempt boundary
            # applies it: interpreter/operator intent (KeyboardInterrupt)
            # and shutdown cancellation (CancelledError) are never a hook
            # outcome; both propagate raw.
            raise
        except BaseException as exc:
            # BaseException, not Exception: the hook is user code invoked
            # in this frame, mid-dispatch, and the boundary's contract is
            # that a buggy hook is logged and ignored, never propagated.
            # A hook raising SystemExit (or a custom BaseException
            # subclass) that escaped would blow out of the dispatch's
            # exception handling itself: the in-flight attempt outcome is
            # dropped, the row strands ``running`` for lease expiry, and
            # the dispatch task ends with the bare re-raise that kills
            # the loop. Logged with repr so the record names the hook's
            # own exception.
            logger: structlog.stdlib.BoundLogger = (
                log if log is not None else structlog.get_logger("taskq.retry")
            )
            logger.warning(
                "retry-classifier-hook-failed",
                hook="retry_classifier",
                # Why safe_repr: the hook raised, its exception's __repr__
                # can raise too, and this handler's contract is to swallow
                # and continue (override = None) - a raising repr() would
                # convert inside the handler and escape it.
                error=safe_repr(exc),
            )
            override = None

    return RetryClassifier.classify(
        policy=reconstructed_policy,
        non_retryable_exceptions=actor_config.non_retryable_exceptions,
        exception=exception,
        attempt=job_state.attempt,
        max_retry_backoff=max_retry_backoff,
        override=override,
    )


async def _invoke_hook(
    call: Callable[[], Awaitable[None] | None],
    job_row: JobRow,
    timeout: float,  # noqa: ASYNC109  Why: parameter name matches the hook contracts; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    name: str,
    log: structlog.stdlib.BoundLogger | None,
) -> None:
    """Run one actor-supplied lifecycle hook, best-effort and bounded.

    Every hook in this module shares one contract: user code runs beside
    a terminal write that has already been decided, so neither a raising
    hook nor a hanging one may change what the job does. Failures are
    logged at WARNING under a name-keyed event and never propagate; a
    hook that returns an awaitable is bounded by *timeout*.

    *call* is a thunk rather than the hook plus its arguments because the
    argument lists differ per hook and a signature union would erase
    them; the thunk keeps each caller's types exact.
    """
    logger: structlog.stdlib.BoundLogger = (
        log if log is not None else structlog.get_logger("taskq.retry")
    )

    try:
        hook_result = call()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # The carve-out, exactly as the consumer's attempt boundary
        # applies it: interpreter/operator intent (KeyboardInterrupt) and
        # shutdown cancellation (CancelledError) are never a hook
        # outcome; both propagate raw.
        raise
    except BaseException as exc:
        # BaseException, not Exception: the hook is user code invoked in
        # this frame beside a terminal write that has already been
        # decided. A hook raising SystemExit (or a custom BaseException
        # subclass) that escaped would tear the terminal path down after
        # the write landed - the success/failure row is already the
        # truth, and an escapee either misroutes it into the failure
        # dispatch (a succeeded job recorded failed, a re-execution
        # risk) or ends the dispatch task with the bare re-raise that
        # kills the loop. Logged with repr so the record names the
        # hook's own exception.
        logger.warning(
            f"{name.replace('_', '-')}-hook-failed",
            job_id=str(job_row.id),
            actor=job_row.actor,
            hook=name,
            error=safe_repr(
                exc
            ),  # Why: hook exceptions are uncontrolled; a raising __repr__ must not escape the swallow (see safe_repr).
        )
        return

    if hook_result is None or not inspect.isawaitable(hook_result):
        return

    try:
        await asyncio.wait_for(hook_result, timeout=timeout)
    except TimeoutError:
        logger.warning(
            f"{name.replace('_', '-')}-hook-timeout",
            job_id=str(job_row.id),
            actor=job_row.actor,
            hook=name,
            timeout_seconds=timeout,
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        # Same carve-out as the sync arm: the await delivers the hook's
        # exception into this frame, and operator intent / shutdown
        # cancellation propagate raw.
        raise
    except BaseException as exc:
        logger.warning(
            f"{name.replace('_', '-')}-hook-failed",
            job_id=str(job_row.id),
            actor=job_row.actor,
            hook=name,
            error=safe_repr(
                exc
            ),  # Why: same uncontrolled-exception contract as the sync arm above.
        )


async def invoke_on_retry_exhausted(
    hook: OnRetryExhausted | None,
    job_row: JobRow,
    exception: BaseException,
    timeout: float,  # noqa: ASYNC109  Why: parameter name matches the on_retry_exhausted contract; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> None:
    """Invoke the on_retry_exhausted hook, best-effort and timeout-bounded.

    If the hook is None, returns immediately. If the hook returns a
    coroutine, wraps the await in asyncio.wait_for with the given
    timeout. TimeoutError and other exceptions are caught and logged at
    WARNING; they never propagate to the caller.
    """
    if hook is None:
        return
    await _invoke_hook(
        lambda: hook(job_row, exception),
        job_row,
        timeout,
        name="on_retry_exhausted",
        log=log,
    )


async def invoke_on_success(
    hook: OnSuccess | None,
    job_row: JobRow,
    result: object,
    timeout: float,  # noqa: ASYNC109  Why: parameter name matches the on_success contract; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> None:
    """Invoke the on_success hook, best-effort and timeout-bounded.

    If the hook is None, returns immediately. If the hook returns an
    awaitable, wraps the await in asyncio.wait_for with the given
    timeout. TimeoutError and other exceptions are caught and logged at
    WARNING; they never propagate to the caller.
    """
    if hook is None:
        return
    await _invoke_hook(
        lambda: hook(job_row, result),
        job_row,
        timeout,
        name="on_success",
        log=log,
    )


async def invoke_on_cancel(
    hook: OnCancel | None,
    job_row: JobRow,
    timeout: float,  # noqa: ASYNC109  Why: parameter name matches the on_cancel contract; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> None:
    """Invoke the on_cancel hook, best-effort and timeout-bounded.

    Runs beside the terminal write that moved the job to ``cancelled``,
    so it can neither block that write nor undo it: work cut short still
    has to release whatever it held, and a hook that could raise into
    this path would leave the row ``running`` behind a lease only the
    reclaim sweep clears.
    """
    if hook is None:
        return
    await _invoke_hook(lambda: hook(job_row), job_row, timeout, name="on_cancel", log=log)


async def safe_mark_failed_or_retry(
    backend: Backend,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    retry_delay: timedelta | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    log: structlog.stdlib.BoundLogger | None = None,
    attempt: int | None = None,
    claim_epoch: int | None = None,
) -> JobRow | None:
    """Wrap mark_failed_or_retry, catching WorkerOwnershipMismatch .

    Returns the persisted JobRow on success, or None on ownership mismatch
    (signals the caller to skip the on_retry_exhausted hook). *attempt* is
    the attempt-identity epoch threaded from the handler's job-row
    snapshot, and *claim_epoch* the claim-identity epoch, see
    ``Backend.mark_failed_or_retry``; a fenced-out epoch surfaces here as
    the same None a worker-fence miss produces.
    """
    logger: structlog.stdlib.BoundLogger = (
        log if log is not None else structlog.get_logger("taskq.retry")
    )
    try:
        return await backend.mark_failed_or_retry(
            job_id=job_id,
            worker_id=worker_id,
            error_info=error_info,
            retry_delay=retry_delay,
            progress_seq=progress_seq,
            progress_state=progress_state,
            attempt=attempt,
            claim_epoch=claim_epoch,
        )
    except WorkerOwnershipMismatch as exc:
        logger.warning(
            "mark-failed-or-retry-ownership-mismatch",
            job_id=str(exc.job_id),
            expected_worker=exc.expected,
            actual_worker=exc.actual,
        )
        return None
