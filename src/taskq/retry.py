"""Retry policy data carriers, decision types, backoff computation, and
consumer-loop adapter wiring.

The data-model layer (RetryPolicy, Retry, Fail, RetryDecision,
JobRetryState, compute_backoff, RetryClassifier) is pure: no I/O, no
clock reads, no backend imports. The adapter layer (OnRetryExhausted,
ActorConfigLike, decide_after_failure, invoke_on_retry_exhausted,
safe_mark_failed_or_retry) wires the classifier to the consumer loop
and is permitted backend imports
"""

import asyncio
import inspect
import random
import secrets
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Literal, NamedTuple, Protocol, Self
from uuid import UUID

import structlog
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from taskq.backend._protocol import Backend, ErrorInfo, JobId, JobRow, RetryKind
from taskq.exceptions import PayloadValidationError, WorkerOwnershipMismatch

__all__ = [
    "ActorConfigLike",
    "Fail",
    "JobRetryState",
    "OnRetryExhausted",
    "OnSuccess",
    "Retry",
    "RetryClassifier",
    "RetryClassifierHook",
    "RetryDecision",
    "RetryKind",
    "RetryOverride",
    "RetryPolicy",
    "compute_backoff",
    "decide_after_failure",
    "invoke_on_retry_exhausted",
    "invoke_on_success",
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
        if v < 1:
            raise ValueError("max_attempts must be >= 1")
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
    """Retry decision: reschedule the job at next_scheduled_at."""

    model_config = ConfigDict(frozen=True)

    next_scheduled_at: datetime


class Fail(BaseModel):
    """Fail decision: the job will not be retried."""

    model_config = ConfigDict(frozen=True)

    error_class: str
    retryable: bool


type RetryDecision = Retry | Fail


class JobRetryState(NamedTuple):
    """Projection of JobRow columns consumed by the retry classifier."""

    attempt: int
    max_attempts: int
    retry_kind: RetryKind
    schedule_to_close: datetime | None
    # Reserved for per-attempt timeout enforcement at the consumer
    # level (asyncio.wait_for); not used by the classifier.
    start_to_close: timedelta | None


_production_rng = random.Random(secrets.randbits(128))  # noqa: S311  Why: random.Random is for timing jitter, not cryptography; seeded via secrets.randbits(128) by design


def compute_backoff(
    policy: RetryPolicy,
    attempt: int,
    rng: random.Random | None = None,
    *,
    max_retry_backoff: timedelta = timedelta(hours=24),
) -> timedelta:
    """Compute the backoff delay for a given attempt (1-indexed).

    formula: multiplicative-symmetric jitter —
      delay = raw * rng.uniform(1 - jitter, 1 + jitter)
    This is NOT Full Jitter (uniform(0, raw)) because Full Jitter
    collapses toward zero on attempt 1, causing thundering-herd
    retries. See Marc Brooker, "Exponential Backoff And Jitter",
    AWS Architecture Blog; and AWS .NET SDK Issue #4341.

    ``max_retry_backoff`` is the global ceiling applied *after*
    ``policy.cap`` — i.e. ``effective_cap = min(policy.cap, max_retry_backoff)``.
    This matches Dramatiq's ``min(max_backoff, DEFAULT_MAX_BACKOFF)`` pattern
    and prevents a
    misconfigured per-actor ``RetryPolicy(cap=timedelta(days=365))`` from
    stranding jobs for a year with no operator visibility.
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

    if policy.backoff == "exponential":
        raw = min(cap_s, base_s * 2 ** (attempt - 1))
    elif policy.backoff == "linear":
        raw = min(cap_s, base_s * attempt)
    else:
        raw = base_s

    delay = raw * source.uniform(1 - policy.jitter, 1 + policy.jitter)
    delay = max(0.0, min(cap_s, delay))
    return timedelta(seconds=delay)


class RetryOverride(BaseModel):
    """Per-exception override returned by an actor's ``retry_classifier`` hook.

    Both fields are optional; ``None`` means "use the actor's static
    ``RetryPolicy``/computed backoff for this field." Returning a
    ``RetryOverride`` with only ``kind`` set lets one exception *type*
    branch into different retry behaviour per occurrence — e.g. an HTTP
    429 response goes ``indefinite`` while a 404 response on the same
    exception type goes ``non_retryable``. Returning one with only
    ``delay`` set lets the actor honour a server-provided retry-after
    duration instead of the policy's computed exponential/linear
    backoff, while ``max_retry_backoff`` still applies as a safety
    ceiling so a malicious or malformed header cannot strand a job.
    """

    model_config = ConfigDict(frozen=True)

    kind: RetryKind | None = None
    delay: timedelta | None = None

    @field_validator("delay")
    @classmethod
    def _validate_delay_non_negative(cls, v: timedelta | None) -> timedelta | None:
        if v is not None and v < timedelta(0):
            raise ValueError(f"delay must be >= 0, got {v}")
        return v


type RetryClassifierHook = Callable[[BaseException, int], RetryOverride | None]
"""Optional per-actor hook for exception-*instance*-level retry classification.

``non_retryable_exceptions`` and the built-in :class:`PayloadValidationError`
check classify by exception *type* alone. Some integrations need finer
granularity — a single exception type (e.g. an HTTP client's status-code
error) that should retry indefinitely on a 429, fail immediately on a 404,
and use a bounded transient budget on a 5xx, or a server-provided
``Retry-After`` value that should drive the actual backoff delay. Register
one via ``@actor(retry_classifier=...)``.

Invoked with ``(exception, attempt)`` for every exception that survives the
``non_retryable_exceptions``/``PayloadValidationError`` checks. Return
``None`` to fall back to the actor's static ``RetryPolicy`` unchanged, or a
:class:`RetryOverride` to refine ``kind`` and/or ``delay`` for this specific
occurrence. Exceptions raised by the hook itself are caught and logged by
:func:`decide_after_failure`; classification falls back to the static
policy in that case — a broken hook can never crash the retry pipeline.
"""


class RetryClassifier:
    """Pure classifier that maps an exception + policy to a RetryDecision."""

    @staticmethod
    def _retry_or_deadline(
        policy: RetryPolicy,
        attempt: int,
        schedule_to_close: datetime | None,
        now: datetime,
        *,
        max_retry_backoff: timedelta,
        override_delay: timedelta | None = None,
    ) -> RetryDecision:
        delay = (
            max(timedelta(0), min(override_delay, max_retry_backoff))
            if override_delay is not None
            else compute_backoff(policy, attempt, max_retry_backoff=max_retry_backoff)
        )
        next_scheduled_at = now + delay
        if schedule_to_close is not None and next_scheduled_at >= schedule_to_close:
            return Fail(error_class="DeadlineExceeded", retryable=False)
        return Retry(next_scheduled_at=next_scheduled_at)

    @staticmethod
    def classify(
        policy: RetryPolicy,
        non_retryable_exceptions: tuple[type[BaseException], ...],
        exception: BaseException,
        attempt: int,
        schedule_to_close: datetime | None,
        now: datetime,
        *,
        max_retry_backoff: timedelta = timedelta(hours=24),
        override: RetryOverride | None = None,
    ) -> RetryDecision:
        if isinstance(exception, non_retryable_exceptions):
            return Fail(error_class=type(exception).__name__, retryable=False)

        if isinstance(exception, PayloadValidationError):
            return Fail(error_class="PayloadValidationError", retryable=False)

        effective_kind = (
            override.kind if override is not None and override.kind is not None else policy.kind
        )
        override_delay = override.delay if override is not None else None

        if effective_kind == "non_retryable":
            return Fail(error_class=type(exception).__name__, retryable=False)

        if effective_kind == "transient":
            if attempt < policy.max_attempts:
                return RetryClassifier._retry_or_deadline(
                    policy,
                    attempt,
                    schedule_to_close,
                    now,
                    max_retry_backoff=max_retry_backoff,
                    override_delay=override_delay,
                )
            return Fail(error_class=type(exception).__name__, retryable=False)

        # effective_kind == "indefinite"
        if schedule_to_close is not None and now >= schedule_to_close:
            return Fail(error_class="DeadlineExceeded", retryable=False)
        return RetryClassifier._retry_or_deadline(
            policy,
            attempt,
            schedule_to_close,
            now,
            max_retry_backoff=max_retry_backoff,
            override_delay=override_delay,
        )


def time_budget_as_interval(retry: RetryPolicy) -> timedelta | None:
    """Return retry.time_budget when kind=='indefinite' and time_budget
    is set; otherwise None. Used by the enqueue path to pass
    time_budget as a `$N::interval` parameter so PG can compute
    schedule_to_close = now() + $N::interval."""
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
in-flight job — possible, but it propagates type parameters into the
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


def decide_after_failure(
    actor_config: ActorConfigLike,
    exception: BaseException,
    job_state: JobRetryState,
    now: datetime,
    *,
    max_retry_backoff: timedelta = timedelta(hours=24),
    log: structlog.stdlib.BoundLogger | None = None,
) -> RetryDecision:
    """Adapter between the pure classifier and the consumer loop.

    Reconstructs a RetryPolicy from row-stored max_attempts / retry_kind
    (authoritative) combined with live-registration scalars
    (backoff, base, cap, jitter, time_budget) that are not stored on the
    row. If the actor registered a ``retry_classifier`` hook, invokes it
    to get a per-exception :class:`RetryOverride`, then delegates to
    RetryClassifier.classify.

    ``max_retry_backoff`` is the global ceiling forwarded to
    ``compute_backoff``. The consumer passes
    ``settings.max_retry_backoff`` so the knob is operator-controlled.
    """
    # row-stored max_attempts and retry_kind are authoritative;
    # live registration is authoritative for the other policy scalars
    # and for exception types.
    reconstructed_policy = RetryPolicy(
        kind=job_state.retry_kind,
        max_attempts=job_state.max_attempts,
        backoff=actor_config.retry.backoff,
        base=actor_config.retry.base,
        cap=actor_config.retry.cap,
        jitter=actor_config.retry.jitter,
        time_budget=actor_config.retry.time_budget,
    )

    override: RetryOverride | None = None
    if actor_config.retry_classifier is not None and not isinstance(
        exception, (*actor_config.non_retryable_exceptions, PayloadValidationError)
    ):
        try:
            override = actor_config.retry_classifier(exception, job_state.attempt)
            if override is not None and not isinstance(override, RetryOverride):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the hook's declared return type is RetryOverride | None, but a buggy hook may return a dict or other type at runtime; this guard prevents AttributeError in RetryClassifier.classify.
                logger: structlog.stdlib.BoundLogger = (
                    log if log is not None else structlog.get_logger("taskq.retry")
                )
                logger.warning(
                    "retry-classifier-hook-invalid-return",
                    hook="retry_classifier",
                    return_type=type(override).__name__,
                )
                override = None
        except Exception as exc:
            logger: structlog.stdlib.BoundLogger = (
                log if log is not None else structlog.get_logger("taskq.retry")
            )
            logger.warning(
                "retry-classifier-hook-failed",
                hook="retry_classifier",
                error=repr(exc),
            )
            override = None

    return RetryClassifier.classify(
        policy=reconstructed_policy,
        non_retryable_exceptions=actor_config.non_retryable_exceptions,
        exception=exception,
        attempt=job_state.attempt,
        schedule_to_close=job_state.schedule_to_close,
        now=now,
        max_retry_backoff=max_retry_backoff,
        override=override,
    )


async def invoke_on_retry_exhausted(
    hook: OnRetryExhausted | None,
    job_row: JobRow,
    exception: BaseException,
    timeout: float,  # noqa: ASYNC109  Why: parameter name matches the on_retry_exhausted contract; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> None:
    """Invoke the on_retry_exhausted hook with timeout guard .

    If the hook is None, returns immediately. If the hook returns a
    coroutine, wraps the await in asyncio.wait_for with the given
    timeout. TimeoutError and other exceptions are caught and logged at
    WARNING; they never propagate to the caller.
    """
    if hook is None:
        return

    logger: structlog.stdlib.BoundLogger = (
        log if log is not None else structlog.get_logger("taskq.retry")
    )

    try:
        result = hook(job_row, exception)
    except Exception as exc:
        logger.warning(
            "on-retry-exhausted-hook-failed",
            job_id=job_row.id,
            actor=job_row.actor,
            hook="on_retry_exhausted",
            error=repr(exc),
        )
        return

    if result is not None and inspect.isawaitable(result):
        try:
            await asyncio.wait_for(result, timeout=timeout)
        except TimeoutError:
            logger.warning(
                "on-retry-exhausted-hook-timeout",
                job_id=job_row.id,
                actor=job_row.actor,
                hook="on_retry_exhausted",
                timeout_seconds=timeout,
            )
        except Exception as exc:
            logger.warning(
                "on-retry-exhausted-hook-failed",
                job_id=job_row.id,
                actor=job_row.actor,
                hook="on_retry_exhausted",
                error=repr(exc),
            )


async def invoke_on_success(
    hook: OnSuccess | None,
    job_row: JobRow,
    result: object,
    timeout: float,  # noqa: ASYNC109  Why: parameter name matches the on_success contract; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> None:
    """Invoke the on_success hook with timeout guard.

    If the hook is None, returns immediately. If the hook returns an
    awaitable, wraps the await in asyncio.wait_for with the given
    timeout. TimeoutError and other exceptions are caught and logged at
    WARNING; they never propagate to the caller.
    """
    if hook is None:
        return

    logger: structlog.stdlib.BoundLogger = (
        log if log is not None else structlog.get_logger("taskq.retry")
    )

    try:
        hook_result = hook(job_row, result)
    except Exception as exc:
        logger.warning(
            "on-success-hook-failed",
            job_id=job_row.id,
            actor=job_row.actor,
            hook="on_success",
            error=repr(exc),
        )
        return

    if hook_result is not None and inspect.isawaitable(hook_result):
        try:
            await asyncio.wait_for(hook_result, timeout=timeout)
        except TimeoutError:
            logger.warning(
                "on-success-hook-timeout",
                job_id=job_row.id,
                actor=job_row.actor,
                hook="on_success",
                timeout_seconds=timeout,
            )
        except Exception as exc:
            logger.warning(
                "on-success-hook-failed",
                job_id=job_row.id,
                actor=job_row.actor,
                hook="on_success",
                error=repr(exc),
            )


async def safe_mark_failed_or_retry(
    backend: Backend,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    next_scheduled_at: datetime | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> JobRow | None:
    """Wrap mark_failed_or_retry, catching WorkerOwnershipMismatch .

    Returns the persisted JobRow on success, or None on ownership mismatch
    (signals the caller to skip the on_retry_exhausted hook).
    """
    logger: structlog.stdlib.BoundLogger = (
        log if log is not None else structlog.get_logger("taskq.retry")
    )
    try:
        return await backend.mark_failed_or_retry(
            job_id=job_id,
            worker_id=worker_id,
            error_info=error_info,
            next_scheduled_at=next_scheduled_at,
            progress_seq=progress_seq,
            progress_state=progress_state,
        )
    except WorkerOwnershipMismatch as exc:
        logger.warning(
            "mark-failed-or-retry-ownership-mismatch",
            job_id=exc.job_id,
            expected_worker=exc.expected,
            actual_worker=exc.actual,
        )
        return None
