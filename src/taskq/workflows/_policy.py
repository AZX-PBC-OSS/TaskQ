"""Typed workflow execution policy and its one effective-target resolver.

The declaration layer deliberately reuses the package's actor cohort,
reservation/rate-limit, deadline, and retry types.  It is not a second policy
registry or a second backend.  ``None`` and omission both inherit;
``CLEAR_POLICY`` is the only spelling that stops inheritance.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from types import MappingProxyType
from typing import Final, Literal, cast

import structlog
from pydantic import BaseModel, ConfigDict, Field

from taskq.actor_config import CapacityOwner, validate_capacity_owner
from taskq.backend._protocol import ErrorInfo, JobId, RetryKind
from taskq.constants import check_max_attempts_domain, check_priority_domain
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryClassifierHook, RetryOverride, RetryPolicy
from taskq.workflows._types import duration_seconds

__all__ = [
    "CLEAR_POLICY",
    "ExecutionPolicy",
    "ExhaustionEvent",
    "ResolvedExecutionPolicy",
    "RetryDeferralBudgetExceeded",
    "RetrySpec",
    "TerminalEffect",
    "WorkflowPolicyError",
    "WorkflowRetryClassifierHook",
    "WorkflowRetryOverride",
    "canonical_retry_kind",
    "resolve_source_execution_policy",
    "validate_retry_after_delay",
]

logger: structlog.stdlib.BoundLogger = structlog.getLogger(__name__)


class WorkflowPolicyError(ValueError):
    """A coherent, actionable declaration error (diagnostic ``E21``)."""

    code: Final[str] = "E21-execution-policy"

    def __init__(self, message: str) -> None:
        super().__init__(f"[{self.code}] {message}")
        self.message = message


type TerminalEffectReason = Literal[
    "retry_exhausted", "non_retryable", "non_consuming_exhausted", "deadline"
]


class ExhaustionEvent(BaseModel):
    """Validated input delivered by one ordinary after-terminal effect job."""

    model_config = ConfigDict(frozen=True)

    flow_id: JobId
    flow_name: str
    node_id: JobId
    node_key: str = Field(min_length=1)
    map_index: int | None = Field(default=None, ge=0)
    attempt: int = Field(gt=0)
    run_epoch: int = Field(gt=0)
    trace_id: str | None = None
    error: ErrorInfo
    record: object
    reason: TerminalEffectReason


class RetryDeferralBudgetExceeded(RuntimeError):  # noqa: N818 — established budget-exhaustion vocabulary
    """A bounded advertised/non-consuming retry sequence reached its limit."""


class _PolicyUnset:
    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET_POLICY"


class _PolicyClear:
    __slots__ = ()

    def __repr__(self) -> str:
        return "CLEAR_POLICY"


_UNSET: Final = _PolicyUnset()
CLEAR_POLICY: Final = _PolicyClear()
"""Explicitly clear an inherited option; ``None`` continues to mean inherit."""

type PolicyTerm[T] = T | _PolicyUnset | _PolicyClear | None
type PolicyRateLimit = str | KeyedRateLimitRef | TokenBucket | SlidingWindow
type PolicyReservation = (
    str | ConcurrencyReservation | KeyedReservationRef | FiniteReservationCohort
)
type PolicyRetryKind = RetryKind | Literal["permanent"]


def canonical_retry_kind(value: str, *, what: str = "retry_kind") -> RetryKind:
    """Normalize the legacy alias at one boundary and validate persistence words."""
    if value == "permanent":
        return "non_retryable"
    if value not in ("transient", "indefinite", "non_retryable"):
        raise WorkflowPolicyError(
            f"{what} must be 'transient', 'non_retryable' (legacy alias 'permanent'), "
            f"or a previously persisted legacy 'indefinite' value; got {value!r}"
        )
    return value


class WorkflowRetryOverride(RetryOverride):
    """Workflow-only refinement, including an explicitly advertised free retry.

    This additive subclass does not alter vanilla ``Retry``/``RetryDecision`` or
    vanilla ``RetryOverride`` consumers.  A non-consuming decision must carry
    its advertised delay and may not attempt to override the terminal kind.
    """

    consume_budget: bool = True

    def model_post_init(self, __context: object, /) -> None:
        super().model_post_init(__context)
        if not self.consume_budget:
            if self.delay is None:
                raise ValueError("consume_budget=False requires an advertised delay")
            if self.kind is not None:
                raise ValueError(
                    "consume_budget=False cannot also change kind; the policy kind remains authoritative"
                )


type WorkflowRetryClassifierHook = Callable[
    [BaseException, int], RetryOverride | WorkflowRetryOverride | None
]
type WorkflowExceptionOverride = RetryOverride | WorkflowRetryOverride | PolicyRetryKind | None


def _mapping_override(
    value: WorkflowExceptionOverride,
) -> WorkflowRetryOverride | RetryOverride | None:
    if value is None or isinstance(value, RetryOverride):
        return value
    return RetryOverride(kind=canonical_retry_kind(value, what="exception retry kind"))


def _selected_exception_override(
    entries: tuple[tuple[type[BaseException], RetryOverride | WorkflowRetryOverride | None], ...],
    exc: BaseException,
) -> tuple[bool, RetryOverride | WorkflowRetryOverride | None]:
    """Most-specific real base class wins; virtual ABCs retain declaration order."""
    matches = [entry for entry in entries if isinstance(exc, entry[0])]
    if not matches:
        return (False, None)
    lineage = type(exc).mro()

    def specificity(
        entry: tuple[type[BaseException], RetryOverride | WorkflowRetryOverride | None],
    ) -> int:
        try:
            return lineage.index(entry[0])
        except ValueError:
            return len(lineage)

    return (True, min(matches, key=specificity)[1])


@dataclass(frozen=True, slots=True)
class RetrySpec:
    """The workflow retry contract, built on the validated shared RetryPolicy.

    ``exceptions`` is lowered once into a deterministic, subtype-precise
    mapping.  A mapped ``None`` is an explicit default-policy result and does
    not fall through to the general hook.  A custom workflow classifier may
    return ``WorkflowRetryOverride(consume_budget=False, delay=...)``; because
    that is possible for every custom hook, such a spec requires the separate
    positive finite ``non_consuming_limit`` at declaration time.
    """

    policy: RetryPolicy = field(default_factory=RetryPolicy)
    classifier: WorkflowRetryClassifierHook | RetryClassifierHook | None = None
    exceptions: Mapping[type[BaseException], WorkflowExceptionOverride] = field(
        default_factory=dict[type[BaseException], WorkflowExceptionOverride]
    )
    non_consuming_limit: int | None = None
    #: One typed effect declaration, not a callable dict or exactly-once flag.
    on_exhaustion: TerminalEffect | None = None
    _exception_entries: tuple[
        tuple[type[BaseException], RetryOverride | WorkflowRetryOverride | None], ...
    ] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.on_exhaustion is not None and not isinstance(self.on_exhaustion, TerminalEffect):  # pyright: ignore[reportUnnecessaryIsInstance]  # runtime declaration boundary
            raise WorkflowPolicyError("RetrySpec.on_exhaustion must be a TerminalEffect")
        if not isinstance(self.policy, RetryPolicy):  # pyright: ignore[reportUnnecessaryIsInstance]  # runtime boundary behind the typed declaration
            raise WorkflowPolicyError("RetrySpec.policy must be a taskq RetryPolicy")
        if self.policy.time_budget is not None:
            raise WorkflowPolicyError(
                "workflow retry does not support RetryPolicy.time_budget: "
                "start_to_close is the supported per-attempt deadline; a full "
                "wall-clock time budget requires workflow deadline integration"
            )
        if self.policy.kind == "indefinite":
            raise WorkflowPolicyError(
                "new workflow RetrySpec declarations require a finite max_attempts "
                "budget; kind='indefinite' is not an advertised-delay bypass"
            )
        if self.classifier is not None and not callable(self.classifier):
            raise WorkflowPolicyError("RetrySpec.classifier must be callable or None")
        if self.non_consuming_limit is not None and (
            type(self.non_consuming_limit) is not int
            or not 1 <= self.non_consuming_limit <= 2_147_483_647
        ):
            raise WorkflowPolicyError(
                "RetrySpec.non_consuming_limit must be a positive signed-int32 integer"
            )
        entries: list[tuple[type[BaseException], RetryOverride | WorkflowRetryOverride | None]] = []
        for exception_type, value in self.exceptions.items():
            if not isinstance(exception_type, type) or not issubclass(  # pyright: ignore[reportUnnecessaryIsInstance]  # runtime declaration guard
                exception_type, BaseException
            ):
                raise WorkflowPolicyError(
                    f"retry exception map key {exception_type!r} is not an exception class"
                )
            entries.append((exception_type, _mapping_override(value)))
        object.__setattr__(self, "_exception_entries", tuple(entries))
        object.__setattr__(self, "exceptions", MappingProxyType(dict(self.exceptions)))
        may_not_consume = self.classifier is not None or any(
            isinstance(override, WorkflowRetryOverride) and not override.consume_budget
            for _exception_type, override in entries
        )
        if may_not_consume and self.non_consuming_limit is None:
            raise WorkflowPolicyError(
                "RetrySpec.non_consuming_limit is required for workflow classifiers "
                "that may return a non-consuming advertised retry; generic Snooze "
                "and legacy RetryAfter(consume_budget=False) are not bounded by this count"
            )

    def _call_classifier(
        self, exc: BaseException, attempt: int
    ) -> RetryOverride | WorkflowRetryOverride | None:
        matched, mapped = _selected_exception_override(self._exception_entries, exc)
        if matched:
            return mapped
        if self.classifier is None:
            return None
        try:
            value = self.classifier(exc, attempt)
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except BaseException as classifier_error:
            # Existing retry hooks are isolated: a broken classifier degrades to
            # the declared static policy instead of killing the daemon/runner.
            logger.warning(
                "workflow-retry-classifier-failed",
                error_class=type(classifier_error).__name__,
                error=str(classifier_error)[:500],
            )
            return None
        if value is not None and not isinstance(value, RetryOverride):  # pyright: ignore[reportUnnecessaryIsInstance]  # hostile hook return boundary
            logger.warning(
                "workflow-retry-classifier-invalid-return",
                actual_type=type(value).__name__,
            )
            return None
        return value

    def classify(
        self, exc: BaseException, attempt: int
    ) -> RetryOverride | WorkflowRetryOverride | None:
        """Safely classify only a body-raised exception selected by the runner."""
        value = self._call_classifier(exc, attempt)
        if value is None:
            return None
        # model_construct can bypass pydantic. Never let such a value select an
        # unsupported bypass or poison delay arithmetic.
        if value.kind is not None and value.kind not in (
            "transient",
            "indefinite",
            "non_retryable",
        ):
            return None
        if value.delay is not None and not isinstance(value.delay, timedelta):  # pyright: ignore[reportUnnecessaryIsInstance]  # model_construct bypass guard
            return None
        if isinstance(value, WorkflowRetryOverride):
            if type(value.consume_budget) is not bool:
                return None
            if not value.consume_budget and (value.delay is None or value.kind is not None):
                return None
        if value.kind == "indefinite":
            # New workflow declarations are finite. The classifier cannot
            # manufacture the legacy unbounded kind behind that validation.
            logger.warning("workflow-retry-classifier-indefinite-refused")
            return None
        return value


@dataclass(frozen=True, slots=True)
class FiniteReservationCohort:
    """An existing keyed reservation ref constrained to explicit finite keys.

    Boot materializes exactly these concrete reservations. Emit/fork verifies
    the payload-derived name against this set instead of minting arbitrary new
    provider capacity for each record identity.
    """

    ref: KeyedReservationRef
    keys: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "keys", tuple(self.keys))
        if not self.keys or len(set(self.keys)) != len(self.keys):
            raise WorkflowPolicyError("FiniteReservationCohort.keys must be non-empty and unique")
        # Use the ref's concrete-name charset/length law, without inventing a
        # second key codec. The existing refs module owns these validators.
        from taskq.constants import (  # pyright: ignore[reportPrivateUsage]  # the shared keyed boundary
            _KEYED_KEY_RE,  # pyright: ignore[reportPrivateUsage]  # the existing key grammar is the normative boundary
            _MAX_KEYED_KEY_LEN,  # pyright: ignore[reportPrivateUsage]  # same keyed declaration's length law
        )

        for key in self.keys:
            if (
                not isinstance(key, str)  # pyright: ignore[reportUnnecessaryIsInstance]  # runtime validation behind the typed declaration
                or not key
                or len(key) > _MAX_KEYED_KEY_LEN
                or not _KEYED_KEY_RE.match(key)
            ):
                raise WorkflowPolicyError(
                    f"finite reservation key {key!r} violates the existing keyed-name law"
                )

    def contains_name(self, name: str) -> bool:
        return any(name == f"{self.ref.base_name}:{key}" for key in self.keys)

    def reservations(self) -> tuple[ConcurrencyReservation, ...]:
        """The exact static slots boot materializes for this finite declaration."""
        return tuple(
            ConcurrencyReservation(
                f"{self.ref.base_name}:{key}",
                slots=self.ref.slots,
                lease=self.ref.lease,
                keyed=False,  # finite declarations are boot-owned, not idle-key-created.
            )
            for key in self.keys
        )


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """An immutable execution-policy layer for a source or chain target."""

    actor: PolicyTerm[str] = _UNSET
    queue: PolicyTerm[str] = _UNSET
    rate_limits: PolicyTerm[Sequence[PolicyRateLimit]] = _UNSET
    reservations: PolicyTerm[Sequence[PolicyReservation]] = _UNSET
    max_concurrent: PolicyTerm[int] = _UNSET
    result_ttl: PolicyTerm[timedelta | float] = _UNSET
    capacity_owner: PolicyTerm[CapacityOwner] = _UNSET
    priority: PolicyTerm[int] = _UNSET
    start_to_close: PolicyTerm[timedelta] = _UNSET
    retry: PolicyTerm[RetrySpec] = _UNSET

    def __post_init__(self) -> None:
        for name in ("actor", "queue"):
            value = getattr(self, name)
            if (
                value is not _UNSET
                and value is not CLEAR_POLICY
                and value is not None
                and (not isinstance(value, str) or not value.strip())
            ):
                raise WorkflowPolicyError(f"{name} must be a non-empty string")
        if (
            self.max_concurrent is not _UNSET
            and self.max_concurrent
            not in (
                None,
                CLEAR_POLICY,
            )
            and (type(self.max_concurrent) is not int or self.max_concurrent < 0)
        ):
            raise WorkflowPolicyError("max_concurrent must be a non-negative integer")
        if self.priority is not _UNSET and self.priority not in (None, CLEAR_POLICY):
            if type(self.priority) is not int:
                raise WorkflowPolicyError("priority must be an integer")
            check_priority_domain(self.priority, what="ExecutionPolicy.priority")
        if self.start_to_close not in (_UNSET, None, CLEAR_POLICY) and (
            not isinstance(self.start_to_close, timedelta) or self.start_to_close <= timedelta(0)
        ):
            raise WorkflowPolicyError("start_to_close must be a positive timedelta")
        if self.capacity_owner not in (_UNSET, None, CLEAR_POLICY):
            validate_capacity_owner(self.capacity_owner, what="ExecutionPolicy")
        if self.retry not in (_UNSET, None, CLEAR_POLICY) and not isinstance(self.retry, RetrySpec):
            raise WorkflowPolicyError("retry must be a workflow RetrySpec")
        for name in ("rate_limits", "reservations"):
            value = getattr(self, name)
            if value not in (_UNSET, None, CLEAR_POLICY):
                if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
                    raise WorkflowPolicyError(f"{name} must be a sequence, not a string")
                normalized = (
                    tuple(cast("Sequence[PolicyRateLimit]", value))
                    if name == "rate_limits"
                    else tuple(cast("Sequence[PolicyReservation]", value))
                )
                object.__setattr__(self, name, normalized)
        if self.result_ttl not in (_UNSET, None, CLEAR_POLICY):
            seconds = duration_seconds(cast("timedelta | float", self.result_ttl))
            if seconds is None or not math.isfinite(seconds) or seconds <= 0:
                raise WorkflowPolicyError("result_ttl must be positive and finite")
            object.__setattr__(self, "result_ttl", seconds)


@dataclass(frozen=True, slots=True)
class TerminalEffect:
    """An ordinary bounded job materialized by one durable exhaustion intent.

    The body receives ``(StepContext, ExhaustionEvent)`` plus the standard
    optional dependency tail. Intent creation and queue materialization are
    idempotent; callback/HTTP execution remains at-least-once. Only guarded
    same-database application work can complete-once through its own atomic
    marker/effects transaction.
    """

    body: Callable[..., Awaitable[object]]
    policy: ExecutionPolicy

    def __post_init__(self) -> None:
        if not callable(self.body):
            raise WorkflowPolicyError("TerminalEffect.body must be async-callable")
        if not isinstance(self.policy, ExecutionPolicy):  # pyright: ignore[reportUnnecessaryIsInstance]  # runtime declaration boundary
            raise WorkflowPolicyError("TerminalEffect.policy must be an ExecutionPolicy")
        for name in ("actor", "queue"):
            value = getattr(self.policy, name)
            if not isinstance(value, str) or not value.strip():
                raise WorkflowPolicyError(
                    f"TerminalEffect.policy.{name} must explicitly name a non-empty {name}"
                )
        if self.policy.retry not in (_UNSET, None, CLEAR_POLICY):
            retry = cast("RetrySpec", self.policy.retry)
            if retry.on_exhaustion is not None:
                raise WorkflowPolicyError(
                    "a terminal effect's independent retry policy cannot recursively declare "
                    "another terminal effect; each ordinary effect job is already bounded"
                )


def handle_policy(body: object) -> ExecutionPolicy | None:
    """Lower an actor handle to defaults; its name/queue are not placement."""
    from taskq.workflows.api._app import (
        ACTOR_MAX_ATTEMPTS_DEFAULT,
        ACTOR_RETRY_KIND_DEFAULT,
        WorkflowActor,
    )

    if not isinstance(body, WorkflowActor):
        return None
    retry: RetrySpec | None = None
    if (
        body.max_attempts != ACTOR_MAX_ATTEMPTS_DEFAULT
        or body.retry_kind != ACTOR_RETRY_KIND_DEFAULT
    ):
        retry = RetrySpec(
            policy=RetryPolicy(
                max_attempts=body.max_attempts,
                kind=canonical_retry_kind(body.retry_kind, what="actor retry_kind"),
            )
        )
    return ExecutionPolicy(
        max_concurrent=body.max_concurrent,
        reservations=tuple(body.reservations) if body.reservations else None,
        result_ttl=body.result_ttl,
        capacity_owner=cast("CapacityOwner", body.capacity_owner),
        priority=body.priority,
        start_to_close=body.start_to_close,
        retry=retry,
    )


def _chosen[T](
    layers: Sequence[ExecutionPolicy | None],
    name: str,
    fallback: T,
) -> T:
    for layer in layers:
        if layer is None:
            continue
        value = cast("PolicyTerm[T]", getattr(layer, name))
        if value is _UNSET or value is None:
            continue
        if value is CLEAR_POLICY:
            return fallback
        return cast("T", value)
    return fallback


@dataclass(frozen=True, slots=True)
class ResolvedExecutionPolicy:
    """The one effective target shared by starts, successors and projection."""

    actor: str
    queue: str
    rate_limit_decls: tuple[PolicyRateLimit, ...] = ()
    reservation_decls: tuple[PolicyReservation, ...] = ()
    rate_limits: tuple[str, ...] = ()
    reservations: tuple[str, ...] = ()
    max_concurrent: int | None = None
    result_ttl: float | None = None
    capacity_owner: CapacityOwner = "operator"
    priority: int | None = None
    start_to_close: timedelta | None = None
    retry: RetrySpec | None = None
    # Compatibility-only budget, not a shadow curve/policy declaration. The
    # legacy public source keeps its original fixed ladder when retry is absent.
    legacy_max_attempts: int = 3
    legacy_retry_kind: RetryKind = "transient"

    @property
    def max_attempts(self) -> int:
        return (
            self.retry.policy.max_attempts if self.retry is not None else self.legacy_max_attempts
        )

    @property
    def retry_kind(self) -> RetryKind:
        return self.retry.policy.kind if self.retry is not None else self.legacy_retry_kind

    @property
    def retry_for_rows(self) -> RetryPolicy | None:
        """Only authored policies restamp the pre-existing row-curve columns."""
        return self.retry.policy if self.retry is not None else None

    def row_metadata(self) -> dict[str, object]:
        """Concrete admission names plus the bounded provider deferral limit."""
        document: dict[str, object] = {}
        if self.rate_limits:
            document["rate_limits"] = list(self.rate_limits)
        if self.reservations:
            document["reservations"] = list(self.reservations)
        if self.retry is not None and self.retry.non_consuming_limit is not None:
            document["wf_retry"] = {"non_consuming_limit": self.retry.non_consuming_limit}
        return document


def row_retry_curve(policy: RetryPolicy | None) -> tuple[float, float, str, float]:
    """Persist exactly the columns the existing reclaim SQL consumes."""
    effective = policy or RetryPolicy()
    return (
        effective.base.total_seconds(),
        effective.cap.total_seconds(),
        effective.backoff,
        effective.jitter,
    )


def resolve_execution_policy(
    *,
    step_policy: ExecutionPolicy | None,
    default_policy: ExecutionPolicy | None = None,
    body: object = None,
    compatibility_actor: str = "wf",
    compatibility_queue: str = "default",
    run_priority: int | None = None,
    payload_values: Sequence[object] = (),
    resolve_names: bool = True,
    legacy_max_attempts: int = 3,
) -> ResolvedExecutionPolicy:
    """Resolve target > handle > chain default, then compatibility defaults."""
    from taskq.workflows.api._graph import (
        keyed_rate_limit_names,
        keyed_reservation_names,
        rate_limit_names,
        reservation_names,
    )

    handle = handle_policy(body)
    layers = (step_policy, handle, default_policy)
    actor = _chosen(layers, "actor", compatibility_actor)
    queue = _chosen(layers, "queue", compatibility_queue)
    rate_decls = cast("Sequence[PolicyRateLimit]", _chosen(layers, "rate_limits", ()))
    reservation_decls = cast("Sequence[PolicyReservation]", _chosen(layers, "reservations", ()))
    rate_decl_tuple = tuple(rate_decls)
    reservation_decl_tuple = tuple(reservation_decls)
    rates = rate_limit_names(rate_decl_tuple) + (
        keyed_rate_limit_names(rate_decl_tuple, payload_values) if resolve_names else ()
    )
    finite_names: list[str] = []
    if resolve_names:
        for declaration in reservation_decl_tuple:
            if not isinstance(declaration, FiniteReservationCohort):
                continue
            selected = keyed_reservation_names((declaration.ref,), payload_values)
            for name in selected:
                if not declaration.contains_name(name):
                    raise WorkflowPolicyError(
                        f"resolved reservation {name!r} is outside the finite cohort "
                        f"{declaration.ref.base_name!r}; refusing to mint an arbitrary "
                        "per-record slot declaration"
                    )
                finite_names.append(name)
    reservations = (
        reservation_names(reservation_decl_tuple)
        + (keyed_reservation_names(reservation_decl_tuple, payload_values) if resolve_names else ())
        + tuple(finite_names)
    )
    priority = _chosen(layers, "priority", None)
    if run_priority is not None:
        priority = run_priority
    return ResolvedExecutionPolicy(
        actor=actor,
        queue=queue,
        rate_limit_decls=rate_decl_tuple,
        reservation_decls=reservation_decl_tuple,
        rate_limits=rates,
        reservations=reservations,
        max_concurrent=_chosen(layers, "max_concurrent", None),
        result_ttl=_chosen(layers, "result_ttl", None),
        capacity_owner=_chosen(layers, "capacity_owner", "operator"),
        priority=priority,
        start_to_close=_chosen(layers, "start_to_close", None),
        retry=_chosen(layers, "retry", None),
        legacy_max_attempts=legacy_max_attempts,
    )


def _compatible_policy(
    policy: ExecutionPolicy | None,
    *,
    actor: str,
    queue: str,
    max_attempts: int,
) -> ExecutionPolicy:
    """Lower old source kwargs and detect contradictory co-declarations."""
    check_max_attempts_domain(max_attempts)
    declared = policy or ExecutionPolicy()
    declared_actor = declared.actor
    declared_queue = declared.queue
    if (
        declared_actor not in (_UNSET, None, CLEAR_POLICY)
        and declared_actor != actor
        and actor != "wf"
    ):
        raise WorkflowPolicyError(
            f"chain_source actor={actor!r} contradicts policy.actor={declared_actor!r}"
        )
    if (
        declared_queue not in (_UNSET, None, CLEAR_POLICY)
        and declared_queue != queue
        and queue != "default"
    ):
        raise WorkflowPolicyError(
            f"chain_source queue={queue!r} contradicts policy.queue={declared_queue!r}"
        )
    retry = declared.retry
    if retry not in (_UNSET, None, CLEAR_POLICY):
        assert isinstance(retry, RetrySpec)
        if retry.policy.max_attempts != max_attempts and max_attempts != 3:
            raise WorkflowPolicyError(
                f"chain_source max_attempts={max_attempts} contradicts "
                f"policy.retry.policy.max_attempts={retry.policy.max_attempts}"
            )
        return declared
    if retry is CLEAR_POLICY:
        if max_attempts != 3:
            raise WorkflowPolicyError(
                "chain_source max_attempts cannot be combined with retry=CLEAR_POLICY"
            )
        return declared
    return declared


def resolve_source_execution_policy(
    *,
    source_body: object,
    policy: ExecutionPolicy | None,
    actor: str,
    queue: str,
    max_attempts: int,
    payload_values: Sequence[object] = (),
    run_priority: int | None = None,
) -> ResolvedExecutionPolicy:
    """Source-only resolution: chain leaf defaults never enter this call."""
    lowered = _compatible_policy(policy, actor=actor, queue=queue, max_attempts=max_attempts)
    # A source argument is its own target layer; the handle still contributes
    # only default terms (never its name/queue as a second placement source).
    return resolve_execution_policy(
        step_policy=lowered,
        default_policy=None,
        body=source_body,
        compatibility_actor=actor,
        compatibility_queue=queue,
        run_priority=run_priority,
        payload_values=payload_values,
        legacy_max_attempts=max_attempts,
    )


def validate_retry_after_delay(delay: timedelta) -> None:
    """Fail deterministically before non-finite/out-of-range timers reach PG."""
    seconds = delay.total_seconds()
    if not math.isfinite(seconds) or seconds < 0:
        raise WorkflowPolicyError("retry delay must be finite and non-negative")
    # A timestamp beyond year 9999 is outside the engine's supported wall-clock
    # vocabulary (and well inside, rather than at, PostgreSQL's own upper edge).
    if seconds > 253_402_300_799:
        raise WorkflowPolicyError("retry delay exceeds the supported timestamp range")
