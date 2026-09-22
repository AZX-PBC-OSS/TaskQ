"""Backend protocol, data carriers, and protocol version constant.

Defines the :class:`Backend` protocol that both :class:`PostgresBackend`
(production) and :class:`InMemoryBackend` (tests) must satisfy, along with
the frozen dataclass carriers that cross the protocol boundary.

This submodule exists so that concrete backend implementations (e.g.
``taskq.backend.postgres``) can import the protocol and carriers without
creating a circular dependency through the re-export boundary in
``taskq.backend.__init__``.
"""

import asyncio
import re
import warnings
from collections.abc import Collection, Container, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, IntEnum
from typing import (
    TYPE_CHECKING,
    Annotated,
    ClassVar,
    Final,
    Literal,
    NewType,
    Protocol,
    cast,
    get_args,
    runtime_checkable,
)
from uuid import UUID

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type ConnLike = asyncpg.Connection | PoolConnectionProxy  # pyright: ignore[reportUnusedImport]  # Why: PoolConnectionProxy is only used in the type alias; pyright may not see it

else:
    type ConnLike = object  # pyright: ignore[reportInvalidTypeForm]  # Why: runtime fallback, asyncpg is TYPE_CHECKING-only to avoid transitive import

from pydantic import AfterValidator, BaseModel, ConfigDict

from taskq._json import check_no_nul_str
from taskq.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_RECLAIM_POLL_LIMIT,
    check_max_attempts_domain,
    check_priority_domain,
)
from taskq.exceptions import MaxPendingExceededError

__all__ = [
    "BACKEND_PROTOCOL_VERSION",
    "DEFAULT_UNIQUE_STATES",
    "DST_STRATEGIES",
    "JOB_STATUS_VALUES",
    "SNOOZE_OUTCOME_VALUES",
    "AttemptOutcome",
    "AttemptRow",
    "Backend",
    "BackendDeps",
    "BatchCounts",
    "BatchFilter",
    "BatchRow",
    "BatchStatus",
    "BulkCancelResult",
    "CancelFlag",
    "CancelPhase",
    "CronScheduleOwner",
    "DenialReason",
    "DstStrategy",
    "EnqueueArgs",
    "ErrorInfo",
    "EventRow",
    "IdempotencyKey",
    "IdentityKey",
    "JobFilter",
    "JobId",
    "JobPage",
    "JobRow",
    "JobSortField",
    "JobStatus",
    "LongRunningJobEventsWriter",
    "QueueMode",
    "QueueName",
    "RateLimitBackend",
    "RetryKind",
    "ScheduleCreateArgs",
    "ScheduleDisabledBy",
    "ScheduleRecord",
    "ScheduleUpdateArgs",
    "SnoozeOutcome",
    "SqlOutcomeBranch",
    "parse_batch_status",
    "parse_cancel_phase",
    "parse_outcome_branch",
    "parse_retry_kind",
    "validate_denial_reason",
    "validate_snooze_outcome",
]

# ── Protocol version ───────────────────────────────────────────────────
# Bump rule: increment when a change alters an existing protocol member's
# observable contract such that an implementation written against the
# previous version would *silently* misbehave (wrong results, ignored
# inputs) instead of failing loudly.  Purely additive changes an old
# implementation can ignore without producing incorrect behaviour do not
# require a bump.  See docs/architecture.md §Backend protocol.
# v3 (unreleased; folds in every protocol change since the last shipped
#     release): list_jobs, JobFilter.status widened to accept a sequence
#     and the `active` meta-filter was added; a v2 implementation returns
#     wrong rows for both shapes without erroring. get_actor_max_pending
#     added (required), a v2 implementation lacks the method, and the
#     client capacity cache's fail-open would otherwise swallow the
#     AttributeError and silently enforce code literals forever.
#     mark_succeeded / mark_succeeded_with_conn gained the
#     `fallback_result_ttl` keyword, without it a v2 implementation
#     keeps the enqueue-pinned result_expires_at when the stored
#     result_ttl is cleared, silently expiring results at completion.
#     mark_succeeded / mark_succeeded_with_conn also gained the
#     `result_bytes` keyword, the result's orjson encoding, produced
#     once by the worker consumer. An implementation that ignores it
#     stores a NULL result (and NULL result_size_bytes) for every
#     consumer-completed job, silently; it must bind
#     result_bytes.decode("utf-8") and store
#     result_size_bytes = len(result_bytes) when it is given, reject a
#     call passing both result and result_bytes, and NUL-guard the
#     bytes exactly as the dict form is guarded.
#     EnqueueArgs.scheduled_at is now optional, None means immediate
#     and the backend's server stamps/decides it. A v2-era implementation
#     fails LOUDLY on None ('>' not supported between NoneType and
#     datetime at its scheduled_at > now checks) rather than silently
#     misbehaving, so per the bump rule above this is a documented
#     no-bump incompatibility.
#     mark_failed_or_retry's next_scheduled_at (datetime | None) is
#     replaced by retry_delay (timedelta | None), the backend derives
#     scheduled_at, the scheduled/pending status, AND the
#     schedule_to_close deadline outcome from its own clock (single
#     arbiter); a v2-era implementation binding a datetime into the
#     interval slot fails loudly at the driver instead of silently
#     misbehaving.
#     list_jobs, every JobFilter.order_by now tie-breaks on `id` in the
#     SAME direction as its primary column (`created_at DESC, id DESC`,
#     not `id ASC`) and accepts a cursor, whose shape is the ordering's
#     own columns. A v2-era implementation still ordering `id ASC` pages
#     silently wrongly against the new cursor, so this belongs in the
#     same unreleased bump rather than being additive.
#     The vestigial `now` parameters are REMOVED from
#     scheduled_to_pending / deadline_sweep / reclaim_expired_locks and
#     the PostgresBackend.sweep_* statics, PG ignored them (the server
#     clock is the arbiter); an implementation still declaring them fails
#     loudly with TypeError on the call.
#     mark_snoozed's `outcome` parameter is narrowed from AttemptOutcome
#     to SnoozeOutcome ('snoozed' | 'reservation_denied' |
#     'rate_limit_denied') and validated at the Python boundary: the
#     five execution outcomes key no arm in the statement, so a caller
#     passing one left PG firing no arm (job stranded 'running', the
#     call returning 'noop') while the in-memory twin silently
#     rescheduled it uncounted.  Both backends now raise ValueError
#     naming the legal set, loud, not silent, so it folds into the
#     unreleased v3 rather than bumping.
#     The non-consuming deferral arms (mark_snoozed's snoozed arm and
#     mark_retry_after's consume_budget=False arm) floor the effective
#     delay at MIN_DEFERRAL_INTERVAL (taskq.constants): a zero-delay
#     deferral reschedules at least that far out instead of parking the
#     job 'pending' at clock_timestamp() at the head of the dispatch
#     order, a claim/refund hot loop monopolising a worker slot.
#     consume_budget=True keeps the raw delay (an immediate consuming
#     retry is a real execution, bounded by the budget it spends).
#     mark_interrupted added (required), the shutdown release primitive:
#     a pre-v3 implementation lacks the method, and the consumer's
#     shutdown routing would otherwise raise AttributeError mid-cancel
#     (loud, not silent), so it folds into the unreleased v3.
BACKEND_PROTOCOL_VERSION: Final[int] = 3

# ── Type aliases (PEP 695) ─────────────────────────────────────────────

type JobStatus = Literal[
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]

# PEP-695 ``type`` aliases are ``TypeAliasType`` objects; ``get_args``
# returns ``()`` on the alias itself, unwrap via ``__value__`` to reach
# the ``Literal[...]`` and enumerate its members at runtime.
JOB_STATUS_VALUES: Final[frozenset[str]] = frozenset(get_args(JobStatus.__value__))
"""Runtime membership set of every :data:`JobStatus` literal value.

Derived from the ``JobStatus`` Literal itself (the canonical declaration)
so validation can never drift from the type.  Used by
:meth:`JobFilter.__post_init__` to reject unknown statuses before they
reach a backend.
"""

DEFAULT_UNIQUE_STATES: Final[tuple[JobStatus, ...]] = (
    "pending",
    "scheduled",
    "running",
    "succeeded",
)
"""Job statuses a ``unique_for`` window matches unless the caller narrows it.

``unique_for`` reads as "at most one job for this identity in this
period", and the reason a caller reaches for it is that the work is not
safe to repeat. ``succeeded`` is therefore in the set: it is the state
that says the work already happened, which is the precise condition the
window exists to detect. Leaving it out would free the identity the
instant the first job completed, so the faster the work succeeds, the
wider the unguarded remainder of the window, and the failure would be
likeliest exactly when the system is healthy.

The other terminal states stay out, and for the mirror-image reason:
``failed``, ``cancelled``, ``crashed`` and ``abandoned`` all mean the
work did NOT happen, so matching them would let one transient failure
suppress every later attempt for the rest of the window.

Callers who want the narrower "block only concurrent execution" rule
spell the three unfinished states explicitly.
"""

type AttemptOutcome = Literal[
    "succeeded",
    "failed",
    "snoozed",
    "cancelled",
    "crashed",
    "scheduled",
    "reservation_denied",
    "rate_limit_denied",
]

type SnoozeOutcome = Literal["snoozed", "reservation_denied", "rate_limit_denied"]
"""The outcomes :meth:`Backend.mark_snoozed`'s statement arms key on.

Why narrower than :data:`AttemptOutcome`: the snooze statement's arms
branch on exactly these three values (the snooze arm's refund/counter
CASE, the denial-keyed counters, and the deadline arm's terminal exit ,
a deferral's only way to fail, since a denial never spends budget and
never terminalises on its own).  The five execution outcomes key no arm
, a caller passing
one on a running job left PG firing no arm at all (the row stranded
``running`` until the lease sweep, the call returning ``"noop"``) while
the in-memory twin silently rescheduled the job with no counter
increment, so the two backends disagreed on the same input.  The
parameter carries this alias at every layer (protocol, both terminals,
the wrappers, ``FakeBackend``) and
:func:`validate_snooze_outcome` rejects anything else at the Python
boundary, PG cannot express a bind-value rejection inside the
statement, so the boundary owns it.
"""

SNOOZE_OUTCOME_VALUES: Final[frozenset[SnoozeOutcome]] = frozenset(
    get_args(SnoozeOutcome.__value__)
)
"""Runtime membership set of every :data:`SnoozeOutcome` literal value.

Derived from the ``SnoozeOutcome`` Literal itself (the canonical
declaration) so the guard's legal set can never drift from the type ,
the same single-source pattern as :data:`JOB_STATUS_VALUES` and
:data:`DST_STRATEGIES`.
"""


def validate_snooze_outcome(outcome: str) -> None:
    """Reject an outcome :meth:`Backend.mark_snoozed` has no arm for.

    Raises :class:`ValueError` naming the legal set and the rejected
    value.  Called by both backends' ``mark_snoozed`` before any state
    is touched, so they fail identically on an illegal outcome whatever
    the job's state, never degrading to the ``"noop"`` a fenced-out
    write would return.
    """
    if outcome not in SNOOZE_OUTCOME_VALUES:
        raise ValueError(
            f"mark_snoozed outcome must be one of {sorted(SNOOZE_OUTCOME_VALUES)}; "
            f"got {outcome!r}, the snooze arms key on exactly these deferral "
            "outcomes; an execution outcome has no arm here"
        )


type DenialReason = Literal["capacity", "unavailable"]
"""Why a denial-class snooze write (:meth:`Backend.mark_snoozed` with a
denial *outcome*) was denied.

``capacity`` is a real saturation denial, the limiter's store answered
and the answer was "full".  The denial is legitimate backpressure about
a job the system chose not to run yet; an operator scaling a bucket on
denial counts is the intended response.

``unavailable`` is the limiter's store failing to answer at all (Redis
unreachable, the PG fallback dead or unwired), infrastructure
backpressure about a job whose actor never executed.

Both reasons take the identical non-consuming path: every denial
carries HTTP-429 semantics, so the claim's attempt increment is
refunded exactly the way an actor-requested ``snoozed`` deferral
refunds it, no terminal arm may fire on budget grounds, and the job
reschedules until capacity frees or its own ``schedule_to_close``
expires.  The reason's only effect is observability: a store-outage
denial stays distinguishable from a saturation denial, so an operator
never answers an outage with more capacity.

Only the consumer's store-failure synthesis site passes ``unavailable``
(explicitly, never inferred from a bucket name); every other caller
rides the ``capacity`` default.
"""

DENIAL_REASON_VALUES: Final[frozenset[DenialReason]] = frozenset(get_args(DenialReason.__value__))
"""Runtime membership set of every :data:`DenialReason` literal value.

Derived from the :data:`DenialReason` Literal itself so the guard's
legal set can never drift from the type, the same single-source
pattern as :data:`SNOOZE_OUTCOME_VALUES`.
"""


def validate_denial_reason(reason: str) -> None:
    """Reject a denial reason :meth:`Backend.mark_snoozed` has no arm for.

    Raises :class:`ValueError` naming the legal set and the rejected
    value.  Called by both backends' ``mark_snoozed`` beside
    :func:`validate_snooze_outcome`, before any state is touched: PG
    cannot reject an unknown bind value inside the statement, so the
    Python boundary owns the check, and an illegal reason must not
    silently degrade to one arm's semantics.
    """
    if reason not in DENIAL_REASON_VALUES:
        raise ValueError(
            f"mark_snoozed denial_reason must be one of {sorted(DENIAL_REASON_VALUES)}; "
            f"got {reason!r}, 'capacity' is a saturation denial (the store "
            "answered 'full'), 'unavailable' is the store failing to answer; "
            "both are non-consuming and non-terminal, the reason only keeps "
            "the two causes distinguishable on the row"
        )


type RetryKind = Literal["transient", "indefinite", "non_retryable"]
"""Closed set of retry tiers.

Why ``Literal`` and not an ``Enum``: serialization round-trips through
``model_dump(mode="json")`` produce plain strings without
``use_enum_values`` configuration; pyright exhaustive matching works
identically for either; no ``.value`` access required at call sites.
"""

type QueueMode = Literal["strict_fifo", "round_robin"]

type RateLimitBackend = Literal["redis", "postgres", "memory"]

type DstStrategy = Literal["skip", "firstof", "allof"]

#: Runtime membership set of every :data:`DstStrategy` literal value, the
#: single source of truth the schedule-write validation
#: (:meth:`ScheduleCreateArgs.__post_init__`) and the row-value coercions
#: (worker cron loop, admin ops) all consult, so none can drift from the
#: Literal or from each other. Re-exported via :mod:`taskq.cron` (the cron
#: public surface those callers already import from).
#:
#: Why: annotated as ``frozenset[DstStrategy]`` (not ``frozenset[str]``) ,
#: pyright narrows ``raw in DST_STRATEGIES`` to the Literal union only with
#: the parameterised element type, which is what lets the coercion sites
#: assign the checked value without a cast.
DST_STRATEGIES: Final[frozenset[DstStrategy]] = frozenset(get_args(DstStrategy.__value__))

type BatchStatus = Literal["active", "complete", "aborted"]
"""Lifecycle status of a batch row in the ``batches`` table."""


type SqlOutcomeBranch = Literal[
    "retried",
    "deadline_failed",
    "snoozed",
    "cancelled",
    "failed",
    "max_attempts_failed",
    "released",
]
"""Closed set of ``outcome_branch`` values the fused terminal statements'
RETURNING arms emit (``backend/_sql_templates.py``).

Why a closed ``Literal`` and not the bare ``str`` the rows used to be read
as: the value decides which observability and return contract a terminal
write reports, and the multi-arm statements emit only a SUBSET of the set
(``mark_retry`` never emits ``"snoozed"``; ``mark_interrupted`` never emits
``"max_attempts_failed"``), so a typo'd arm literal or a renamed branch used
to degrade silently into a fall-through arm's semantics. Parsing at the
read site plus pyright's exhaustiveness checking over the union in the
consumers makes a future or renamed arm a compile error instead.
"""


class JobSortField(Enum):
    """Sort ordering for :meth:`Backend.list_jobs` via :attr:`JobFilter.order_by`.

    ``SCHEDULED_AT_ASC`` (and the default ``None``) preserve the canonical
    dispatch-friendly ordering, ``priority DESC, scheduled_at ASC, id ASC`` ,
    so existing ``list_jobs`` callers see no behaviour change.

    ``CREATED_AT_DESC`` and ``FINISHED_AT_DESC`` serve "latest run by business
    key" queries: newest-created first and most-recently-finished first
    (``NULLS LAST``) respectively.

    Every ordering pages with a cursor.  Each one orders ``id`` *with* its
    primary column rather than against it -- ``created_at DESC, id DESC``,
    not ``created_at DESC, id ASC`` -- which is what makes the page seam a
    single row-wise comparison.  ``id`` is UUIDv7 and therefore
    time-ordered, so running it with a timestamp column reorders nothing
    in practice.  The ordering and the cursor shape it pages with are one
    object, :class:`~taskq.backend._cursor.JobOrdering`; a cursor encodes
    the columns of the ordering it was produced under, so cursors are not
    interchangeable between orderings.
    """

    SCHEDULED_AT_ASC = "scheduled_at_asc"
    CREATED_AT_DESC = "created_at_desc"
    FINISHED_AT_DESC = "finished_at_desc"


class CancelPhase(IntEnum):
    """Phases of cooperative-then-forced cancellation.

    Why ``IntEnum`` and not ``Literal[0, 1, 2]``: the cancel-poll loop
    performs arithmetic comparisons (``db_phase >= 1``,
    ``active.cancel_phase < 2``) that ``Literal[int]`` does not narrow
    correctly under pyright strict. ``IntEnum`` subclasses ``int``, so
    every existing comparison continues to work, while the typed enum
    carries the OTel attribute semantics (``cancel_phase`` attribute on
    transition counters) and prevents bare-int values like ``99`` from
    slipping past the type checker.

    Values ``NONE``, ``COOPERATIVE``, and ``FORCED`` are persistable ,
    they map directly to the PG ``cancel_phase`` column whose check
    constraint is ``BETWEEN 0 AND 2``. ``ABANDON_PENDING`` is an
    in-process sentinel only: the cancel-poll loop sets it on
    ``_ActiveJob`` to mark a job as queued for post-transaction
    abandonment. It is never written to PG. Keeping it on the same
    enum lets ``cancel_phase`` stay strongly typed end-to-end.
    """

    NONE = 0
    COOPERATIVE = 1
    FORCED = 2
    ABANDON_PENDING = 3  # in-process sentinel; never persisted to PG


# ── Opaque identifier types ────────────────────────────────────────────

JobId = NewType("JobId", UUID)
"""Opaque job identifier, prevents ``UUID`` mixups across the API."""

IdempotencyKey = NewType("IdempotencyKey", str)
"""Distinguishes idempotency keys from identity keys at call sites."""

IdentityKey = NewType("IdentityKey", str)
"""Distinguishes identity keys from idempotency keys at call sites."""


#: The two halves of the queue-name rule, kept as separate character classes
#: so the pattern and the "which character lost?" diagnostic below are the
#: SAME rule rather than two copies that can drift apart.
_QUEUE_NAME_FIRST: Final = "[A-Za-z0-9_]"
_QUEUE_NAME_REST: Final = "[A-Za-z0-9_.-]"

_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(rf"\A{_QUEUE_NAME_FIRST}{_QUEUE_NAME_REST}*\Z")
# \A/\Z, not ^/$: Python's `$` also matches immediately before a trailing
# newline, so "default\n" satisfied ^...$ (see _IDENT_RE's docstring in
# taskq.constants for the full rationale, same trap, same fix).
#
# Why ":" is excluded, this is the essential restriction, not the
# charset's general tidiness. A queue's fleet-wide concurrency cap is
# registered under the flat name
# `f"{QUEUE_CONCURRENCY_PREFIX}{queue}"` (ratelimit/registry.py's
# `queue_concurrency_reservation_name`), where the prefix is the
# `taskq:global:queue:` namespace and ":" is that namespace's segment
# separator. A queue named "foo:eu" would therefore register as
# `taskq:global:queue:foo:eu`, indistinguishable, in a namespace that is
# one flat dict keyed by concrete name, from queue "foo" in an "eu"
# sub-namespace. Two queues could then share (or steal) one cap's slots.
# The same separator ambiguity is why `taskq.ratelimit.refs` rejects a
# keyed `base_name` that derives into this prefix. Keep ":" out.
#
# Why the FIRST character allows a digit, the leading-letter rule was
# copied from `_IDENT_RE` (taskq.constants), where it is essential
# because a Postgres identifier genuinely cannot start with a digit and
# `_IDENT_RE` guards names that are INTERPOLATED into SQL as identifiers.
# A queue name is not an identifier: it is always bound as a `$n`
# parameter (jobs.queue, workers.queues, queues.name), and it reaches
# OTel only as a metric *dimension value* (`{"queue": queue}` in
# obs/_otel.py), which carries no such restriction. So "2024-backfill"
# costs nothing and the ban only surprised users.


#: Human-readable form of :data:`_QUEUE_NAME_RE`, quoted in every rejection.
#: "invalid" on its own leaves the operator guessing which character lost --
#: and the ":" ban in particular is surprising enough to need saying, since
#: it is the one exclusion that is about a namespace and not about tidiness.
_QUEUE_NAME_RULE: Final = (
    "a queue name must start with a letter, digit or underscore "
    "and may then contain letters, digits, underscore, dot or hyphen "
    f"({_QUEUE_NAME_FIRST}{_QUEUE_NAME_REST}*); ':' is reserved as the separator "
    "of the 'taskq:global:queue:' reservation namespace"
)

_QUEUE_NAME_FIRST_RE: Final[re.Pattern[str]] = re.compile(_QUEUE_NAME_FIRST)
_QUEUE_NAME_REST_RE: Final[re.Pattern[str]] = re.compile(_QUEUE_NAME_REST)


def _queue_name_offender(v: str) -> str:
    """Name what disqualified *v*, for the tail of the rejection message."""
    if not v:
        return "it is empty"
    if not _QUEUE_NAME_FIRST_RE.match(v[0]):
        return f"the first character {v[0]!r} is not allowed there"
    for i, ch in enumerate(v[1:], start=1):
        if not _QUEUE_NAME_REST_RE.match(ch):
            return f"character {ch!r} at position {i} is not allowed"
    # Only reachable if _QUEUE_NAME_RE grows a constraint that is not one of
    # the two character classes it is built from (a length bound, say).
    return "it does not match the allowed pattern"


def _validate_queue_name(v: str) -> str:
    if not _QUEUE_NAME_RE.match(v):
        raise ValueError(
            f"invalid queue name: {v!r} -- {_queue_name_offender(v)}. {_QUEUE_NAME_RULE}"
        )
    return v


_RETRY_KINDS: Final[frozenset[str]] = frozenset({"transient", "indefinite", "non_retryable"})


def parse_retry_kind(value: str) -> RetryKind:
    """Convert an untrusted ``str`` (from a PG row) into :data:`RetryKind`.

    Pyright cannot narrow ``str`` to a ``Literal`` union by membership
    test alone; this helper performs the runtime check and returns a
    statically-typed ``RetryKind``. Raises :class:`ValueError` if the
    value is not one of the three allowed kinds, that signals schema
    drift between PG and Python.
    """
    if value not in _RETRY_KINDS:
        raise ValueError(f"unknown retry_kind from backend row: {value!r}")
    # The membership check above is the runtime guarantee; cast expresses
    # the narrowing to pyright without a bare ignore.
    return cast(RetryKind, value)


_OUTCOME_BRANCHES: Final[frozenset[str]] = frozenset(get_args(SqlOutcomeBranch.__value__))


def parse_outcome_branch(value: str) -> SqlOutcomeBranch:
    """Convert an untrusted ``str`` (a fused terminal statement's
    ``outcome_branch`` RETURNING column) into :data:`SqlOutcomeBranch`.

    The :func:`parse_retry_kind` pattern; why the union is closed at all
    is the :data:`SqlOutcomeBranch` type docstring's story, not restated
    here. Raises :class:`ValueError` if the value is not one of the seven
    allowed branches; that signals schema drift between the SQL
    templates and Python.
    """
    if value not in _OUTCOME_BRANCHES:
        raise ValueError(f"unknown outcome_branch from backend row: {value!r}")
    # The membership check above is the runtime guarantee; cast expresses
    # the narrowing to pyright without a bare ignore.
    return cast(SqlOutcomeBranch, value)


def parse_cancel_phase(value: int) -> CancelPhase:
    """Convert an untrusted ``int`` (from a PG row) into :class:`CancelPhase`.

    The PG check constraint ``cancel_phase BETWEEN 0 AND 2`` ensures
    only persistable values reach Python; we reject
    :attr:`CancelPhase.ABANDON_PENDING` (3) explicitly because that
    value is an in-process sentinel and must never appear in a row.
    """
    phase = CancelPhase(value)
    if phase is CancelPhase.ABANDON_PENDING:
        raise ValueError(
            f"cancel_phase {value} is an in-process sentinel; PG must never store it",
        )
    return phase


_BATCH_STATUSES: Final[frozenset[str]] = frozenset({"active", "complete", "aborted"})


def parse_batch_status(value: str) -> BatchStatus:
    """Convert an untrusted ``str`` (from a PG row) into :data:`BatchStatus`.

    Pyright cannot narrow ``str`` to a ``Literal`` union by membership
    test alone; this helper performs the runtime check and returns a
    statically-typed ``BatchStatus``. Raises :class:`ValueError` if the
    value is not one of the three allowed statuses, that signals schema
    drift between PG and Python.
    """
    if value not in _BATCH_STATUSES:
        raise ValueError(f"unknown batch status from backend row: {value!r}")
    return cast(BatchStatus, value)


QueueName = Annotated[str, AfterValidator(_validate_queue_name)]
"""Validator alias for queue names, accepts plain ``str`` literals.

Why ``Annotated`` and not ``NewType``: queue names are plain strings
requiring validation, no nominal type because no other ``str`` field
at any call site could be confused with ``queue``. ``Annotated`` gives
runtime validation in Pydantic models without forcing every caller to
wrap literals in ``QueueName("default")``.
"""

# ── Data carriers ──────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class EnqueueArgs:
    """Input struct for :meth:`Backend.enqueue`.  Carries every column the
    caller specifies at enqueue time.  ``scheduled_at=None`` means
    immediate, the backend's server stamps ``now()`` and decides
    ``status``; a non-None value is the caller's explicit absolute intent
    (deprecated cross-domain residue, kept only for explicit scheduling ,
    prefer delay/interval forms where available).
    """

    id: JobId
    actor: str
    queue: str
    payload: dict[str, object]
    max_attempts: int
    retry_kind: RetryKind
    scheduled_at: datetime | None
    payload_schema_ver: int = 1
    priority: int = 0
    max_pending: int | None = None
    schedule_to_close: datetime | None = None
    schedule_to_close_interval: timedelta | None = None
    start_to_close: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    identity_key: IdentityKey | None = None
    fairness_key: str | None = None
    idempotency_key: IdempotencyKey | None = None
    idempotency_scope: str = ""
    trace_id: str | None = None
    span_id: str | None = None
    result_ttl: timedelta | None = None
    unique_for: timedelta | None = None
    unique_states: tuple[JobStatus, ...] = DEFAULT_UNIQUE_STATES
    metadata: dict[str, object] = field(default_factory=dict[str, object])
    tags: tuple[str, ...] = ()
    # RetryPolicy's backoff-curve scalars, stamped from the actor's live
    # registration at enqueue time (taskq.client._args builds this from
    # ``ref.retry``). Crash/heartbeat reclaim reads these columns to
    # reschedule on the job's own curve instead of a hardcoded flat
    # interval, the reclaim sweep runs on a leader that need not have
    # the actor registered at all, so the row is the only source it can
    # reach. Defaults reproduce RetryPolicy's own field defaults.
    retry_base: timedelta = timedelta(seconds=5)
    retry_cap: timedelta = timedelta(hours=1)
    retry_backoff: Literal["exponential", "linear", "fixed"] = "exponential"
    retry_jitter: float = 0.2
    # Lazy jsonb-encoding memos (backend/_records.payload_jsonb_param and
    # metadata_jsonb_param), never constructor input. compare/repr excluded:
    # a cache must not take part in value identity. __post_init__ resets
    # them, see there for why that is not optional.
    payload_jsonb_memo: str | None = field(default=None, compare=False, repr=False)
    metadata_jsonb_memo: str | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        # Why the memos reset here: ``dataclasses.replace`` feeds every
        # current field value back through __init__, so a memo computed for
        # the PREVIOUS payload or metadata would ride along onto the
        # replaced struct and re-bind a stale encoding. The memos are
        # call-time caches, so a replace drops them and the next jsonb
        # binding re-encodes from the live fields.
        object.__setattr__(self, "payload_jsonb_memo", None)
        object.__setattr__(self, "metadata_jsonb_memo", None)
        if self.schedule_to_close is not None and self.schedule_to_close_interval is not None:
            raise ValueError(
                "schedule_to_close and schedule_to_close_interval are mutually exclusive; "
                "if both are desired, pass only schedule_to_close (datetime), "
                "the interval form is the actor-declaration default."
            )
        self._check_column_domains()
        self._check_no_nul_text()

    def _check_column_domains(self) -> None:
        """Reject a value outside the domain of the column it lands in.

        Enforced here, at the struct every enqueue path funnels through
        (single, batch, the ``COPY``-based fast batch, the atomic batch,
        and the InMemory mirror), for the same reason the NUL guard below
        is: a producer building the struct directly, a batch helper and
        the clients all inherit one refusal, so no later path can
        reintroduce the gap.

        Without it the two backends disagree at runtime. Postgres refuses
        an out-of-domain smallint with a raw driver error naming a
        constraint or a column, a bare exception no caller has a handler
        for, while the in-memory twin stores the value, so a suite
        validated in memory certifies an enqueue production rejects. The
        negative durations are worse than either: both backends store
        them, and every dispatch of that job is instantly past its own
        deadline.
        """
        check_max_attempts_domain(self.max_attempts)
        check_priority_domain(self.priority)
        for value, what in (
            (self.start_to_close, "start_to_close"),
            (self.heartbeat_timeout, "heartbeat_timeout"),
            (self.result_ttl, "result_ttl"),
            (self.schedule_to_close_interval, "schedule_to_close_interval"),
            (self.unique_for, "unique_for"),
            # The stamped backoff curve joins the deadline columns: a
            # negative base or cap feeds the reclaim sweep's delay
            # computation a curve anchored in the past, the same
            # instantly-past-deadline shape the checks above refuse. Zero
            # is accepted here and floored at the reclaim writes (a row
            # stamped by an earlier release carries it), while the
            # RetryPolicy boundary refuses non-positive bases before one
            # can be stamped.
            (self.retry_base, "retry_base"),
            (self.retry_cap, "retry_cap"),
        ):
            if value is not None and value < timedelta(0):
                raise ValueError(f"{what} must not be negative, got {value}")

    def _check_no_nul_text(self) -> None:
        """Reject a NUL (U+0000) in any caller-supplied value bound as text.

        Enforced here, at construction, rather than per enqueue path: every
        path (single, batch, the ``COPY``-based fast batch, the atomic batch,
        and the InMemory mirror) funnels through this struct, so a new path
        cannot reintroduce the gap, and each of the previous four
        occurrences of this class of bug was exactly a path the fix had not
        reached.  ``payload`` and ``metadata`` need no check here: they
        transit jsonb via :func:`~taskq.backend._records.jsonb_param`, whose
        :func:`~taskq._json.dumps_jsonb_str` rejects a NUL anywhere in the
        structure.

        Postgres rejects a NUL in ``text`` with
        ``CharacterNotInRepertoireError`` (SQLSTATE 22021), a
        ``PostgresError`` subclass that
        ``worker._handlers._TERMINAL_WRITE_INFRA_EXCEPTIONS`` reads as
        transient infrastructure failure, so an unguarded NUL retries
        forever instead of failing.  ``ValueError`` here keeps that
        classification honest.
        """
        check_no_nul_str(self.actor, what="actor")
        check_no_nul_str(self.queue, what="queue")
        check_no_nul_str(self.idempotency_scope, what="idempotency_scope")
        for value, what in (
            (self.identity_key, "identity_key"),
            (self.fairness_key, "fairness_key"),
            (self.idempotency_key, "idempotency_key"),
            (self.trace_id, "trace_id"),
            (self.span_id, "span_id"),
        ):
            if value is not None:
                check_no_nul_str(value, what=what)
        for tag in self.tags:
            check_no_nul_str(tag, what="tag")


def batch_cap_groups(args_list: list[EnqueueArgs]) -> dict[str, tuple[int, int]]:
    """Group carried ``max_pending`` caps per actor: actor -> (item count, cap).

    Items without a cap are invisible to backpressure. When one batch
    carries different caps for one actor (mixed direct-backend use, the
    clients resolve a single effective cap per actor), the strictest wins:
    admitting up to a looser cap would violate the tighter one. Pure
    function over the args; lives here (not in the PG bulk path) so the
    in-memory mirror, which must not import driver-bound modules ,
    enforces the identical grouping.
    """
    counts: dict[str, int] = {}
    caps: dict[str, int] = {}
    for args in args_list:
        if args.max_pending is None:
            continue
        counts[args.actor] = counts.get(args.actor, 0) + 1
        cap = args.max_pending
        if args.actor not in caps or cap < caps[args.actor]:
            caps[args.actor] = cap
    return {actor: (counts[actor], caps[actor]) for actor in counts}


def cap_keyed_pairs(args_list: list[EnqueueArgs]) -> list[tuple[str, str, str]]:
    """The ``(actor, idempotency_scope, idempotency_key)`` triples the cap
    discount may need to look up in storage.

    Only items that are BOTH capped and idempotency-keyed can have their
    capacity discounted: an uncapped item is invisible to backpressure,
    and an item without a key always writes a fresh row (nothing to
    dedupe against). The pair is the ``(scope, str(key))`` shape the
    storage indexes use on both backends, so one traversal feeds both
    consumers: the PG tier's batched ``fetch_existing`` probe (which
    needs the raw scope/key lists to query with) and
    :func:`batch_cap_refusal_kernel`'s discount arithmetic. Pure function
    over the args; lives here (not in the PG bulk path) so the in-memory
    mirror, which must not import driver-bound modules, drives the
    identical discount from the identical triples.
    """
    return [
        (args.actor, args.idempotency_scope, str(args.idempotency_key))
        for args in args_list
        if args.max_pending is not None and args.idempotency_key is not None
    ]


def batch_cap_refusal_kernel(
    args_list: list[EnqueueArgs],
    *,
    stored_overrides: Mapping[str, int | None],
    stored_pairs: Container[tuple[str, str]],
    existing_counts: Mapping[str, int],
) -> list[MaxPendingExceededError]:
    """The one cap-refusal rule both backends enforce, as a pure function.

    Resolves each capped actor's effective cap (a stored operator
    override wins over the carried literal; an absent or cleared override
    falls back to it), discounts the batch's idempotency pairs (a pair
    already in *stored_pairs*, or repeated within the batch, dedupes
    instead of writing and consumes no capacity -- counted per item, not
    per distinct pair, so a set of repeats discounts each of them), and
    applies the refusal comparison: M1 ``>`` semantics, existing
    pending+scheduled plus net admissions, a batch filling exactly to the
    limit admitted.

    The backend-specific inputs are the three mappings the caller alone
    can see: its stored operator overrides (*stored_overrides*), the
    idempotency pairs its storage already holds (*stored_pairs*), and the
    per-actor live pending+scheduled counts (*existing_counts*). The PG
    tier fetches them as aggregated rows, the in-memory mirror scans its
    own index and job table, but the arithmetic over them is THIS
    function's alone -- previously it was hand-maintained twice, once per
    backend, and only test pins held the copies together. Refusal ORDER
    is batch order (``batch_cap_groups`` insertion order), and each
    refusal carries the resolved effective cap as ``max_pending``, the
    number the caller must relax to admit more.

    Returns one :class:`MaxPendingExceededError` per over-cap actor; an
    empty list admits the whole batch. Raising, logging, and the
    backpressure metric stay with the CALLER: they are per-tier
    observations (each backend's own logger, its own call site), while
    the decision of what to refuse is made exactly once, here.
    """
    groups = batch_cap_groups(args_list)
    if not groups:
        return []
    keyed = cap_keyed_pairs(args_list)
    # Counted per item, not per distinct pair (a set would collapse
    # repeats and under-discount): the first occurrence of a new pair
    # writes one row, every stored-or-repeated occurrence after it
    # dedupes to one that exists.
    deduped_counts: dict[str, int] = {}
    seen_in_batch: set[tuple[str, str]] = set()
    for actor, scope, key in keyed:
        pair = (scope, key)
        if pair in stored_pairs or pair in seen_in_batch:
            deduped_counts[actor] = deduped_counts.get(actor, 0) + 1
        seen_in_batch.add(pair)
    refusals: list[MaxPendingExceededError] = []
    for actor, (batch_count, carried) in groups.items():
        override = stored_overrides.get(actor)
        cap = override if override is not None else carried
        have = existing_counts.get(actor, 0)
        admitted = batch_count - deduped_counts.get(actor, 0)
        if have + admitted > cap:
            refusals.append(
                MaxPendingExceededError(
                    actor=actor,
                    current_count=have,
                    max_pending=cap,
                )
            )
    return refusals


def first_duplicate_idempotency_pair(
    args_list: Iterable[EnqueueArgs],
    stored_pairs: Container[tuple[str, str]],
) -> tuple[str, str] | None:
    """The ``(idempotency_scope, idempotency_key)`` pair a batch write
    aborts on, derived from the batch itself, never from driver text.

    A bulk insert with no ``ON CONFLICT`` arbiter (the COPY fast path)
    aborts at the FIRST item whose pair the unique index already holds,
    and items are written in batch order, so the offending pair is the
    first one that repeats an earlier item or appears among
    *stored_pairs*. Postgres renders the violation's detail with raw,
    unquoted values (a scope containing ``", "`` makes it positionally
    ambiguous, and long values can be truncated), so an attribution that
    parses the server's text mis-names exactly the pairs an operator most
    needs named; the batch's own contents carry the answer losslessly.
    Two different pairs repeated in one batch resolve to the one the
    statement hits first, deterministically.

    Pure function over the args; lives here (not in the PG bulk path) so
    the in-memory mirror, which must not import driver-bound modules ,
    attributes the identical pair (its ``stored_pairs`` is its own
    idempotency index; the PG path's is a targeted post-abort SELECT).
    """
    seen: set[tuple[str, str]] = set()
    for args in args_list:
        if args.idempotency_key is None:
            continue
        pair = (args.idempotency_scope, str(args.idempotency_key))
        if pair in seen or pair in stored_pairs:
            return pair
        seen.add(pair)
    return None


def duplicate_pair_actor_mismatch(
    args_list: Iterable[EnqueueArgs],
    pair: tuple[str, str],
    stored_actor: str | None,
) -> tuple[str, str] | None:
    """``(incoming_actor, existing_actor)`` when the batch write's abort on
    *pair* spans two actors, else ``None`` (a same-actor duplicate).

    The write aborts at the first item holding *pair* when a committed row
    (*stored_actor*) already holds it, and otherwise at the second item
    holding it, whose predecessor in batch order is the holder. The same
    pure rule serves the COPY tier and the in-memory mirror, so the two
    backends name the same actors for the same batch, a cross-actor hit
    is the misuse the single and batch tiers refuse with the typed
    mismatch error, not a same-actor duplicate.
    """
    holders = [
        args.actor
        for args in args_list
        if args.idempotency_key is not None
        and (args.idempotency_scope, str(args.idempotency_key)) == pair
    ]
    if not holders:
        return None
    if stored_actor is not None:
        return (holders[0], stored_actor) if holders[0] != stored_actor else None
    if len(holders) > 1 and holders[1] != holders[0]:
        return (holders[1], holders[0])
    return None


def first_singleton_collision_actor(
    args_list: Iterable[EnqueueArgs],
    stored_actors: Container[str],
) -> str | None:
    """The singleton actor a batch write aborts on, derived from the batch
    itself, never from driver text.

    ``jobs_singleton_uniq`` is keyed on ``(actor)`` over live
    singleton-flagged rows, and a bulk insert writes items in batch order,
    so the violating actor is the first singleton item whose actor repeats
    an earlier singleton item or appears among *stored_actors* (the live
    singleton rows already committed). Same rule, same reasoning as
    :func:`first_duplicate_idempotency_pair`: the batch's own contents
    carry the answer losslessly, where the server's detail text renders
    values raw and unquoted. The ``is True`` predicate matches the partial
    index's ``metadata @> '{"singleton": true}'`` exactly, a
    truthy-but-not-true value never armed the index on either backend.
    Pure function over the args; lives here (not in the PG bulk path) so
    the in-memory mirror, which must not import driver-bound modules ,
    attributes the identical actor.
    """
    seen: set[str] = set()
    for args in args_list:
        if args.metadata.get("singleton") is not True:
            continue
        if args.actor in seen or args.actor in stored_actors:
            return args.actor
        seen.add(args.actor)
    return None


@dataclass(frozen=True, slots=True)
class JobRow:
    """Read-model of a ``taskq.jobs`` row.  Every column the dispatch loop,
    heartbeat, and terminal writes need appears as a typed field.
    ``status`` uses a ``Literal`` union (8 values) matching the
    ``job_status`` enum in ``01.00.00_01_pre_initial.sql``.
    """

    id: JobId
    actor: str
    queue: str
    payload: dict[str, object]
    payload_schema_ver: int
    status: JobStatus
    priority: int
    attempt: int
    max_attempts: int
    retry_kind: RetryKind
    created_at: datetime
    scheduled_at: datetime
    # Every field below reads a column that is nullable, defaulted, or
    # empty-valued in the schema, so its default here is the value the
    # row actually carries when nothing has set it. Keeping them
    # defaulted lets a caller name the columns its case is about (a
    # terminal row for a hook, a claimed row for a fence) without
    # restating three dozen NULLs that carry no meaning.
    identity_key: IdentityKey | None = None
    fairness_key: str | None = None
    schedule_to_close: datetime | None = None
    start_to_close: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    locked_by_worker: UUID | None = None
    lock_expires_at: datetime | None = None
    cancel_requested_at: datetime | None = None
    cancel_phase: CancelPhase = CancelPhase.NONE
    error_class: str | None = None
    error_message: str | None = None
    error_traceback: str | None = None
    progress_state: dict[str, object] = field(default_factory=dict[str, object])
    progress_seq: int = 0
    result: dict[str, object] | None = None
    result_size_bytes: int | None = None
    result_expires_at: datetime | None = None
    idempotency_key: IdempotencyKey | None = None
    idempotency_scope: str = ""
    trace_id: str | None = None
    span_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict[str, object])
    tags: tuple[str, ...] = ()
    snooze_count: int = 0
    """Coalesced count of non-consuming deferrals (``Snooze`` and
    ``RetryAfter(consume_budget=False)``) since enqueue, the job-row
    record of reschedules that consumed no retry budget.  Trailing
    default: rows materialised before the counters existed read 0.
    """
    rate_limit_blocked_count: int = 0
    """Coalesced count of admission denials (reservation / rate-limit)
    since enqueue.  Trailing default: rows materialised before the
    counters existed read 0.
    """
    interrupt_count: int = 0
    """Coalesced count of infrastructure interruptions (a running attempt
    released back to the queue by a worker shutdown) since enqueue, the
    interruptions write no ``job_attempts`` rows and are not execution
    outcomes, so ``attempt`` alone cannot count them.  Trailing default:
    rows materialised before the counter existed read 0.
    """
    retry_base: timedelta = timedelta(seconds=5)
    """``RetryPolicy.base`` stamped at enqueue time, the source crash
    and heartbeat reclaim read to reschedule on this job's own curve.
    Trailing default: rows materialised before the column existed read
    ``RetryPolicy``'s own default.
    """
    retry_cap: timedelta = timedelta(hours=1)
    """``RetryPolicy.cap`` stamped at enqueue time."""
    retry_backoff: Literal["exponential", "linear", "fixed"] = "exponential"
    """``RetryPolicy.backoff`` stamped at enqueue time."""
    retry_jitter: float = 0.2
    """``RetryPolicy.jitter`` stamped at enqueue time."""
    assignment_routed: bool = False
    """Whether dispatch routes this row by its actor's stored assignment
    rather than by its own ``queue`` label.  Producer-placed rows are
    ``False`` (the label governs, so an explicit ``enqueue(queue=...)``
    and a stale producer's post-move enqueue both stay where they were
    put); every re-pend path sets it ``True``, so a row handed back to
    the fleet follows the actor's current queue instead of stranding on
    one the operator has retired.  See the routing contract in
    ``taskq/backend/_dispatch_sql.py``.  Trailing default: rows
    materialised before the marker existed read producer-placed.
    """
    claim_epoch: int = 0
    """The row's non-saturating claim-epoch fence: bumped by exactly 1 on
    every successful dispatch claim, left untouched by the reclaim sweeps
    that clear locks, and fenced on by equality (against the writer's own
    claim view) by every terminal/ownership write that fences on
    ``attempt``. Trailing default: rows materialised before the column
    existed read 0, the one epoch no claim can ever stamp (the first
    claim of any row stamps 1). The invariant lives in
    01.00.18_02_pre_claim_epoch.sql.
    """


@dataclass(frozen=True, slots=True)
class AttemptRow:
    """Read-model of a ``taskq.job_attempts`` row.  ``outcome`` uses
    a ``Literal`` union so pyright catches invalid strings at the protocol
    boundary.
    """

    job_id: JobId
    attempt: int
    started_at: datetime
    finished_at: datetime | None
    outcome: AttemptOutcome
    error_class: str | None
    error_message: str | None
    error_traceback: str | None
    duration_ms: int | None
    worker_id: UUID | None
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class EventRow:
    """Read-model of a ``taskq.job_events`` row.

    Mirrors the ``job_events`` table shape: monotonic ``event_id``,
    the owning job, timestamp, event kind, and a detail payload.
    """

    event_id: int
    job_id: JobId
    occurred_at: datetime
    kind: Literal["state_change", "cancel_request"]
    detail: dict[str, object]


@dataclass(frozen=True, slots=True)
class LongRunningJobEventsWriter:
    """A transaction holding a lock on ``job_events`` for longer than a
    ``poll_reclaim_events`` visibility-delay margin, a candidate cause of
    a silently missed reclaim event (see
    ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``). Diagnostic only:
    reported by ``PostgresBackend.check_reclaim_visibility_delay_risk``,
    not a guarantee that this specific transaction will write to
    ``job_events`` again or actually cause a miss, a proxy signal for an
    operator to investigate, not proof of an incident.

    Not directly ``json.dumps``-safe: ``xact_start`` is a
    :class:`~datetime.datetime`. Serialise with a datetime-aware encoder
    (or ``str()``/``.isoformat()``) in the monitoring loop consuming this.
    """

    pid: int
    xact_start: datetime
    xact_age_seconds: float


@dataclass(frozen=True, slots=True)
class CancelFlag:
    """Carries exactly the two fields returned by the heartbeat cancel-poll
    query: ``job_id`` and ``cancel_phase``.  ``cancel_requested_at``
    is tracked locally by the heartbeat, not read from PG on every poll.
    """

    job_id: JobId
    cancel_phase: CancelPhase


MAX_JOB_LIST_LIMIT: Final[int] = 10_000
"""Largest page ``JobsClient.list`` will ask a backend for.

A page is one round trip that materialises every row it returns, on the
server and in the client; ``cursor`` is what reaches the rows past it. An
unbounded limit let one call ask for the whole table, which is a query
plan and a memory spike no caller can want by accident; ten thousand is
the ceiling peer job queues put on a listed page. Enforced by the
client's list entry, not by :class:`JobFilter`
itself: the same filter drives ``cancel_where``, which ignores ``limit``
and whose in-process implementation lists with no page at all."""


@dataclass(frozen=True, slots=True)
class JobFilter:
    """Filter parameters for :meth:`Backend.list_jobs` and
    :meth:`Backend.cancel_where`.

    For ``cancel_where``, the ``limit``, ``cursor``, and ``order_by``
    fields are ignored, a bulk cancel is not paginated. It is still not
    paginated in outcome, every matching row is cancelled, but the
    write executes as internally bounded committed batches, so a
    mid-operation failure leaves partial progress rather than rolling
    back everything (re-running continues; already-cancelled rows are
    skipped). Use :meth:`has_predicates` to check whether the filter has
    at least one predicate before passing it to ``cancel_where``.

    Heads-up: ``unfinished=True`` means 'not yet finished', a superset of
    non-terminal statuses (``pending`` + ``scheduled`` + ``running``),
    not just 'currently executing'. Read the ``unfinished`` section below
    before relying on the name.

    ``cursor`` is an opaque keyset-pagination token encoding the sort
    columns of ``order_by``'s ordering from the last row of the previous
    page -- ``(priority, scheduled_at, id)`` for the default,
    ``(created_at, id)`` and ``(finished_at, id)`` for the others.  Encode
    it with :func:`~taskq.backend._cursor.encode_job_cursor`, passing the
    same ``order_by``: a cursor is only meaningful under the ordering that
    produced it.  Both backends must agree on cursor encoding and
    comparison semantics.

    ``batch_id`` is a :class:`UUID`. The PG backend converts it to its
    canonical string form at the SQL boundary; the in-memory backend
    compares the UUID directly. Keeping the typed shape here means
    ``JobsClient.list(batch_id=UUID(...))`` flows without an implicit
    ``str(uuid)`` coercion.

    ``status`` accepts either a single :data:`JobStatus` (backwards
    compatible, e.g. ``JobFilter(status="pending")``) or a sequence of
    statuses (e.g. ``JobFilter(status=["pending", "running"])``).
    An empty sequence (``status=[]``) matches no jobs, it is not
    treated as 'no filter'.  Unknown status values raise
    :class:`ValueError` in :meth:`__post_init__`, so untrusted input
    fails identically on both backends instead of surfacing as a PG
    enum-cast error or a silent empty result.
    The PG backend renders a single status as ``status = $n`` and a
    sequence as ``status = ANY($n)``; the in-memory backend performs a
    membership check in both cases.

    ``unfinished`` is a meta-filter that selects statuses by terminality,
    use it to filter by whether a job is still running or has reached
    a terminal state. The name states the predicate it applies:
    'not yet finished' includes both work currently executing and work
    not yet started:

    - ``unfinished=True`` → non-terminal statuses (pending, scheduled,
      running)
    - ``unfinished=False`` → terminal statuses (succeeded, failed,
      cancelled, crashed, abandoned)
    - ``unfinished=None`` (default) → no status-terminality filter

    The non-terminal set is derived from
    :data:`~taskq.backend.statemachine.ACTIVE_STATUSES`, which is itself
    derived from the state machine, adding a new non-terminal state
    updates this filter automatically.

    ``status`` and ``unfinished`` are mutually exclusive; specifying both
    raises :class:`ValueError` in :meth:`__post_init__`.

    Deprecated alias: ``unfinished`` was named ``active``, and that name
    stays accepted as a constructor kwarg (positional or keyword), it
    raises :class:`DeprecationWarning` and feeds the same predicate.
    The rename exists because 'active' read as 'currently executing' to
    every new reader, while the predicate is 'not yet finished'.

    Usage examples::

        JobsClient.list(JobFilter(status="pending"))
        JobsClient.list(JobFilter(status=["pending", "running"]))
        JobsClient.list(JobFilter(unfinished=True))
    """

    queue: str | None = None
    status: JobStatus | Sequence[JobStatus] | None = None
    actor: str | None = None
    identity_key: IdentityKey | None = None
    batch_id: UUID | None = None
    # One list page (JobsClient.list caps it at MAX_JOB_LIST_LIMIT; cursor
    # reaches the rest). Ignored by cancel_where.
    limit: int = 100
    cursor: str | None = None
    tags: tuple[str, ...] | None = None
    order_by: JobSortField | None = None
    # Deprecated constructor alias for `unfinished`, kept as a field
    # (not a property) because a frozen dataclass cannot alias an
    # __init__ parameter. It holds the PRE-RENAME positional slot (10,
    # before created_before) so a positional call written before the
    # rename still binds its 10th argument to the terminality filter
    # and its 11th to created_before. Excluded from eq and repr: the
    # two spellings must compare equal, and the alias must not surface
    # in repr. __post_init__ promotes it onto `unfinished` and then
    # clears it (constructor input only): the dataclasses.replace
    # copies the client's list probe and the bulk-cancel sanitizer
    # make carry the canonical `unfinished` value, so a filter warns
    # exactly once, at construction.
    active: bool | None = field(default=None, compare=False, repr=False)
    # Matches jobs enqueued strictly before this instant (``created_at <
    # created_before``). An aware datetime: the column is timestamptz, so
    # a naive value would silently change meaning with the server's zone.
    # A predicate, not a paging field: it counts for has_predicates() and
    # both backends apply it (build_filter_conditions / _list_jobs), so
    # the bulk-cancel CLI's --older-than previews exactly what it writes.
    created_before: datetime | None = None
    # True selects every non-terminal status (still running or pending);
    # False selects only terminal statuses. See the class docstring.
    # Deliberately the LAST field, after created_before, rather than
    # slotted into `active`'s old position: inserting it mid-list would
    # shift every later positional argument one slot left, so a
    # pre-rename positional call would hand a datetime to the alias and
    # fail on the alias/unfinished disagreement check. Appended, the
    # old positional layout survives untouched.
    unfinished: bool | None = None

    def __post_init__(self) -> None:
        # Deprecated-alias promotion, before every other check so the
        # alias participates in them identically. One-directional
        # (active → unfinished, never back): after promotion the alias
        # is cleared, so a filter built with `unfinished` keeps active
        # None and one built with `active` ends up indistinguishable
        # from one built with `unfinished`. Either way the
        # dataclasses.replace copies the client and the bulk-cancel
        # path make re-enter __post_init__ with active None and stay
        # warning-free: one DeprecationWarning per filter, at
        # construction, no matter how many internal copies follow.
        if self.active is not None:
            if self.unfinished is not None and self.active != self.unfinished:
                raise ValueError(
                    "unfinished and active disagree; pass only unfinished "
                    "(active is a deprecated alias for it)"
                )
            warnings.warn(
                "JobFilter.active is a deprecated alias for JobFilter.unfinished; "
                "pass unfinished=True for non-terminal statuses, unfinished=False "
                "for terminal ones",
                DeprecationWarning,
                stacklevel=3,  # Why: three frames to the caller, warn → __post_init__ → the dataclass-generated __init__ → user code
            )
            object.__setattr__(self, "unfinished", self.active)
            object.__setattr__(self, "active", None)
        # A negative limit diverges across backends: PG raises
        # "LIMIT must not be negative" while the in-memory slice would
        # silently drop rows.  Reject it here so both fail identically.
        if self.limit < 0:
            raise ValueError(f"limit must be >= 0, got {self.limit}")
        if self.status is not None:
            values = (self.status,) if isinstance(self.status, str) else tuple(self.status)
            unknown = [v for v in values if v not in JOB_STATUS_VALUES]
            if unknown:
                raise ValueError(
                    f"unknown job status value(s): {list(dict.fromkeys(unknown))!r}; "
                    f"valid statuses are {sorted(JOB_STATUS_VALUES)}"
                )
        if self.status is not None and self.unfinished is not None:
            raise ValueError(
                "status and unfinished are mutually exclusive; "
                "use status for specific status(es) or unfinished for the "
                "terminal/non-terminal meta-filter"
            )
        # A NUL in a text predicate binds as text on PG (queue/actor/
        # identity_key) or text[] (tags) and surfaces as a raw asyncpg
        # CharacterNotInRepertoireError (SQLSTATE 22021), the same trap
        # EnqueueArgs._check_no_nul_text guards on the write path.
        # Rejecting here makes both backends' list_jobs/cancel_where
        # paths fail identically with a clean ValueError; cancel_where
        # in particular is a write path (the bulk-cancel feature).
        if self.queue is not None:
            check_no_nul_str(self.queue, what="queue")
        if self.actor is not None:
            check_no_nul_str(self.actor, what="actor")
        if self.identity_key is not None:
            check_no_nul_str(self.identity_key, what="identity_key")
        if self.tags is not None:
            for tag in self.tags:
                check_no_nul_str(tag, what="tag")

    def has_predicates(self) -> bool:
        """Return True if at least one filter predicate is set.

        Used by ``JobsClient.cancel_where`` to reject empty filters that
        would match the entire table. New predicate fields added to
        ``JobFilter`` MUST be added here and in
        ``build_filter_conditions``, the two are kept in sync manually.
        Non-predicate fields (``limit``, ``cursor``, ``order_by``) are
        excluded by design.
        """
        return (
            self.queue is not None
            or self.status is not None
            or self.actor is not None
            or self.identity_key is not None
            or self.batch_id is not None
            or (self.tags is not None and len(self.tags) > 0)
            or self.unfinished is not None
            or self.created_before is not None
        )


#: Who owns a cron schedule's enable/disable lifecycle. ``code`` schedules are
#: declared by a ``@cron`` decorator: the registration pass reverts a stale
#: auto-disable when the code re-declares the schedule at startup. ``operator``
#: schedules are managed by an operator (client ``create_schedule``): the
#: registration pass never touches their enable state.
CronScheduleOwner = Literal["code", "operator"]

#: Who disabled a cron schedule: ``auto`` is the cron loop's failure-count
#: auto-disable (recoverable at registration), ``operator`` is a deliberate
#: disable that no boot reverts. NULL on an enabled row.
ScheduleDisabledBy = Literal["auto", "operator"]


@dataclass(frozen=True, slots=True)
class ScheduleCreateArgs:
    """Input struct for :meth:`Backend.create_schedule`.

    Carries every column the caller specifies at schedule creation time.
    ``next_fire_at`` is computed client-side via
    :func:`~taskq.cron.compute_next_fire_after`, seeded from the clock
    that arbitrates the schedule's due-check: a PG-backed client anchors
    on the PG server clock (one-row ``SELECT clock_timestamp()`` via
    ``JobsClient._schedule_seed_now``, the same clock domain as the
    server-side due-check), an in-memory client on its injected ``Clock``.
    The seed is exact, not approximate: the cron loop's normal path
    recomputes every subsequent fire from the STORED fire time (only a
    miss beyond ``cron_catch_up_window`` re-anchors on the server clock),
    so the seed fixes the fire chain's phase for the schedule's life.
    """

    actor: str
    cron_expr: str
    timezone: str
    next_fire_at: datetime
    dst_strategy: DstStrategy = "skip"
    payload_factory: str | None = None
    enabled: bool = True
    owner: CronScheduleOwner = "code"
    name: str = ""
    identity_key: IdentityKey | None = None
    metadata: dict[str, object] = field(default_factory=dict[str, object])

    def __post_init__(self) -> None:
        from croniter import croniter

        if not croniter.is_valid(self.cron_expr):
            raise ValueError(f"Invalid cron expression: {self.cron_expr!r}")
        if self.dst_strategy not in DST_STRATEGIES:
            raise ValueError(
                f"Invalid dst_strategy: {self.dst_strategy!r}; "
                f"valid strategies are {sorted(DST_STRATEGIES)}"
            )
        if self.owner not in get_args(CronScheduleOwner):
            raise ValueError(
                f"Invalid owner: {self.owner!r}; "
                f"valid owners are {sorted(get_args(CronScheduleOwner))}"
            )
        self._check_no_nul_text()

    def _check_no_nul_text(self) -> None:
        """Reject a NUL (U+0000) in caller-supplied text bound as text.

        Mirrors :meth:`EnqueueArgs._check_no_nul_text`: every text column
        in the ``create_schedule`` INSERT is caller text, and an unguarded
        NUL surfaces as asyncpg ``CharacterNotInRepertoireError``
        (SQLSTATE 22021) instead of a clean ``ValueError``.
        ``cron_expr`` needs no check here, ``croniter.is_valid`` above
        already rejects it. ``metadata`` transits jsonb via
        ``jsonb_param``, which guards it at bind time.
        """
        check_no_nul_str(self.actor, what="actor")
        check_no_nul_str(self.name, what="name")
        check_no_nul_str(self.timezone, what="timezone")
        if self.payload_factory is not None:
            check_no_nul_str(self.payload_factory, what="payload_factory")
        if self.identity_key is not None:
            check_no_nul_str(self.identity_key, what="identity_key")


@dataclass(frozen=True, slots=True)
class ScheduleUpdateArgs:
    """Input struct for :meth:`Backend.update_schedule`.

    Only non-None fields are applied in the UPDATE SET clause.
    When ``enabled`` is True, the UPDATE also resets
    ``consecutive_failures = 0`` and ``last_fire_error = NULL``.
    When ``cron_expr`` is provided, ``next_fire_at`` must also be
    provided (recomputed by the caller via
    :func:`~taskq.cron.compute_next_fire_after`).

    To explicitly clear ``payload_factory`` (set the column to NULL),
    set ``clear_payload_factory=True``, ``None`` for payload_factory
    means "don't change this field."
    """

    cron_expr: str | None = None
    next_fire_at: datetime | None = None
    enabled: bool | None = None
    payload_factory: str | None = None
    clear_payload_factory: bool = False
    metadata: dict[str, object] | None = None
    consecutive_failures: int | None = None
    last_fire_error: str | None = None

    def __post_init__(self) -> None:
        if self.cron_expr is not None and self.next_fire_at is None:
            raise ValueError(
                "next_fire_at must be provided when cron_expr is changed; "
                "recompute via compute_next_fire_after"
            )
        if self.clear_payload_factory and self.payload_factory is not None:
            raise ValueError(
                "clear_payload_factory and payload_factory are mutually exclusive; "
                "use clear_payload_factory=True to set the column to NULL, "
                "or payload_factory to assign a new value"
            )
        self._check_no_nul_text()

    def _check_no_nul_text(self) -> None:
        """Reject a NUL (U+0000) in caller-supplied text bound as text.

        Mirrors :meth:`ScheduleCreateArgs._check_no_nul_text`: every text
        field this struct can set is bound directly as a text parameter by
        ``backend/_schedules.update_schedule``, so an unguarded NUL
        surfaces as asyncpg ``CharacterNotInRepertoireError`` (SQLSTATE
        22021) from inside the UPDATE instead of a clean ``ValueError``
        at the boundary. ``cron_expr`` is guarded here even though the
        create twin skips it: that exemption rests on
        ``croniter.is_valid`` running in the create ``__post_init__``,
        and this struct performs no expression validation of its own.
        ``metadata`` transits jsonb via ``jsonb_param``, which guards it
        at bind time.
        """
        if self.cron_expr is not None:
            check_no_nul_str(self.cron_expr, what="cron_expr")
        if self.payload_factory is not None:
            check_no_nul_str(self.payload_factory, what="payload_factory")
        if self.last_fire_error is not None:
            check_no_nul_str(self.last_fire_error, what="last_fire_error")


class ScheduleRecord(BaseModel):
    """Read-only snapshot of a cron schedule row from the database.

    ``model_config = ConfigDict(frozen=True)`` enforces immutability per
    public API discipline.
    """

    model_config = ConfigDict(frozen=True)

    id: UUID
    actor: str
    name: str = ""
    cron_expr: str
    timezone: str
    dst_strategy: DstStrategy = "skip"
    payload_factory: str | None
    identity_key: IdentityKey | None = None
    enabled: bool
    disabled_by: ScheduleDisabledBy | None = None
    last_fired_at: datetime | None
    last_fire_error: str | None
    consecutive_failures: int
    next_fire_at: datetime
    metadata: dict[str, object]


class BulkCancelResult(BaseModel):
    """Structured outcome of a bulk cancellation request.

    Returned by ``JobsClient.cancel_where()`` so callers can inspect
    how many jobs were cancelled directly (pending/scheduled → terminal
    'cancelled') vs how many had cooperative cancel requested (running →
    cancel_phase=1).
    """

    model_config = ConfigDict(frozen=True)

    cancelled_directly: int
    """Count of pending/scheduled jobs moved straight to terminal 'cancelled'."""

    cancel_requested: int
    """Count of running jobs with cancel_phase=1 set (cooperative cancel)."""

    cancelled_ids: tuple[UUID, ...]
    """IDs of jobs cancelled directly (pending/scheduled → cancelled)."""

    cancel_requested_ids: tuple[UUID, ...]
    """IDs of running jobs with cancel requested."""

    @property
    def total_affected(self) -> int:
        """Total jobs affected by the bulk cancel."""
        return self.cancelled_directly + self.cancel_requested


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """Structured error information for terminal writes."""

    error_class: str
    error_message: str
    error_traceback: str | None

    #: Bounds applied at construction (see ``__post_init__``).
    ERROR_CLASS_MAX_CHARS: ClassVar[int] = 500
    ERROR_MESSAGE_MAX_CHARS: ClassVar[int] = 10_000
    ERROR_TRACEBACK_MAX_CHARS: ClassVar[int] = 100_000

    def __post_init__(self) -> None:
        """Reject a NUL (U+0000) in any caller-supplied value bound as text.

        Enforced here, at construction, rather than at each write path:
        the three fields feed the terminal-write UPDATE's ``error_*`` text
        columns, and every construction site funnels through this struct,
        so a new site cannot reintroduce the gap. Text DERIVED from an
        uncontrolled exception is sanitized before construction by
        ``worker._handlers``, this guard is what makes an unsanitized
        value fail loudly instead of silently.

        Postgres rejects a NUL in ``text`` with
        ``CharacterNotInRepertoireError`` (SQLSTATE 22021), a
        ``PostgresError`` subclass that the terminal-write infra error
        classification misreads as transient, so an unguarded value
        retries forever instead of failing.  ``ValueError`` here keeps
        that classification honest.

        Oversized values are truncated (not rejected): a failure must
        still record, just bounded.  The bounds here live at this same
        construction boundary (Python, not SQL CHECK constraints), so
        every construction site, present and future, inherits them,
        and the columns stay schemaless for existing rows a CHECK would
        reject on sight.  A plain slice (no marker): length is the
        contract the suite pins.
        """
        check_no_nul_str(self.error_class, what="error_class")
        check_no_nul_str(self.error_message, what="error_message")
        if self.error_traceback is not None:
            check_no_nul_str(self.error_traceback, what="error_traceback")
        object.__setattr__(self, "error_class", self.error_class[: ErrorInfo.ERROR_CLASS_MAX_CHARS])
        object.__setattr__(
            self, "error_message", self.error_message[: ErrorInfo.ERROR_MESSAGE_MAX_CHARS]
        )
        if self.error_traceback is not None:
            object.__setattr__(
                self,
                "error_traceback",
                self.error_traceback[: ErrorInfo.ERROR_TRACEBACK_MAX_CHARS],
            )


@dataclass(frozen=True, slots=True)
class JobPage:
    """Paged result from :meth:`JobsClient.list`.  Defined at the
    protocol layer because cursor encoding is a cross-backend contract.
    ``next_cursor`` is ``None`` when no more rows exist.
    """

    jobs: list[JobRow]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class BatchRow:
    """Read-model of a ``taskq.batches`` row."""

    id: UUID
    queue: str
    status: BatchStatus
    expected_size: int
    consecutive_failures: int
    failure_threshold: int | None
    finalizer_job_id: UUID | None
    originating_actor: str | None
    created_at: datetime
    completed_at: datetime | None
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class BatchCounts:
    """Live job-count aggregate for one batch.

    Mirrors BatchCompletionStatus fields; defined at the protocol layer
    so backends do not import the client-side batch module.
    """

    total: int
    pending: int
    succeeded: int
    failed: int
    cancelled: int
    crashed: int
    abandoned: int


@dataclass(frozen=True, slots=True)
class BatchFilter:
    """Filter parameters for Backend.list_batches.

    Unlike JobFilter, this only carries fields relevant to batch queries:
    queue, active (status terminality), batch_id, limit, and cursor.
    Job-oriented fields (status, actor, tags, order_by, identity_key) are
    intentionally absent, using JobFilter for batch queries would
    silently ignore those fields, which is a type trap.

    ``cursor`` is the same mechanism as :attr:`JobFilter.cursor`: an
    opaque keyset token encoding the last row of the previous page, which
    both backends must decode identically
    (:func:`~taskq.backend._cursor.encode_batch_cursor`). It encodes
    ``(created_at, id)`` where the job cursor encodes ``(priority,
    scheduled_at, id)``, one field fewer, same ``|``-delimited shape.
    ``created_at`` alone is not a total order (the column defaults to
    ``now()``, the *transaction* timestamp, so one
    ``enqueue_batch_atomic`` stamps every row it writes identically), so
    ``id`` is the tiebreaker; it is UUIDv7 and therefore time-ordered,
    which lets both columns sort DESC together and makes the seam a
    single row-wise comparison.

    There is no ``order_by``: ``list_batches`` has exactly one ordering,
    so the cursor cannot disagree with it. ``list_jobs`` has three, and
    binds each to its own cursor shape through
    :class:`~taskq.backend._cursor.JobOrdering` for the same reason.
    """

    queue: str | None = None
    active: bool | None = None
    batch_id: UUID | None = None
    limit: int = 100
    cursor: str | None = None

    def __post_init__(self) -> None:
        # Why: no upper bound. The limit bounds one page, not the reachable
        # set -- ``cursor`` is what reaches batch 501 -- but an operator
        # asking for a large single page is paying for it themselves, and a
        # cap here would only re-break the callers that predate the cursor.
        if self.limit < 0:
            raise ValueError(f"limit must be >= 0, got {self.limit}")


# ── Backend deps protocol ───────────────────────────────────────────────
# Worker-layer dependencies consumed by PostgresBackend at construction time.
# Typed as a Protocol (not object) so pyright can verify attribute access
# without union-attr suppresssions, WorkerDeps satisfies this at runtime.


@runtime_checkable
class BackendSettings(Protocol):
    """Narrow settings protocol for PostgresBackend constructor consumption.

    WorkerSettings and _ClientSettings both satisfy this interface. The
    event-writer knobs below are declared (not getattr-probed) because the
    backend's bounded sweep paths read them directly: every settings object
    that reaches a PostgresBackend must carry them, so the contract is
    checkable rather than hoped for.
    """

    schema_name: str
    dispatch_oversample: int
    dispatcher_command_timeout: float
    result_max_bytes: int
    # Rows per committed batch for every job_events writer the backend
    # drives (cancel_where, the bounded sweeps); default
    # DEFAULT_EVENT_WRITER_BATCH_SIZE.
    event_writer_batch_size: int
    # Server-side statement_timeout (ms) for one event-writer batch
    # transaction; default DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS.
    event_writer_statement_timeout_ms: float
    # Degradation-tier divisor for the sweep batch breaker; default 4.
    event_writer_reduced_batch_divisor: int
    # Consecutive sweep-batch cancellations before the breaker latches; default 3.
    sweep_breaker_failure_threshold: int
    # Rolling window (seconds) the breaker counts failures within; default 600.0.
    sweep_breaker_window_secs: float
    # Bounded wait (milliseconds) for the max_pending advisory lock on the
    # single-enqueue path; default DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS (5 s).
    # Read by the PostgresBackend enqueue wrappers at the lock use sites.
    max_pending_lock_timeout_ms: float
    # Bounded wait (milliseconds) for the unique_for single-flight advisory
    # lock on the single-enqueue path; default
    # DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS (5 s).
    unique_for_lock_timeout_ms: float
    # Bounded wait (milliseconds) for the idempotency token INSERT's
    # speculative-lock conflict on the single-enqueue path; default
    # DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS (5 s).
    idempotency_lock_timeout_ms: float
    # Global ceiling on one attempt's backoff, applied by the reclaim
    # sweep's reschedule (min of the row's stamped cap and this value);
    # default DEFAULT_MAX_RETRY_BACKOFF (24 h). Read where the sweep's
    # $4 ceiling parameter is bound.
    max_retry_backoff: timedelta


@runtime_checkable
class BackendDeps(Protocol):
    """Protocol satisfied by WorkerDeps, consumed by PostgresBackend.__init__."""

    @property
    def settings(self) -> BackendSettings:
        """Settings carrying the backend-visible knobs (see BackendSettings)."""
        ...

    @property
    def worker_pool(self) -> "asyncpg.Pool":
        """Pool for terminal writes (pg_dsn_pooled)."""
        ...

    @property
    def heartbeat_pool(self) -> "asyncpg.Pool":
        """Pool for heartbeat writes (pg_dsn_direct, heartbeat_command_timeout)."""
        ...

    @property
    def dispatcher_pool(self) -> "asyncpg.Pool | None":
        """Dispatcher pool for session-sensitive operations.

        WorkerDeps provides a non-optional Pool.  Client-side usage
        (``_ClientDeps``) may provide ``None`` when no dispatcher pool
        is needed, the constructor handles this via ``getattr``.
        """
        ...


# ── Backend protocol ───────────────────────────────────────────────────


@runtime_checkable
class Backend(Protocol):
    """Contract that both PostgresBackend and InMemoryBackend satisfy.

    46 async methods plus two sync methods (``subscribe_wake`` and
    ``subscribe_cancel_wake``) (48 methods total) covering enqueue,
    dispatch, heartbeat, terminal writes, attempt history, cancel
    signals, scheduling / sweeps, read, NOTIFY hook, schedule CRUD,
    and batch operations. Method order grouped for review-grep
    ergonomics.

    Why monomorphic (no ``Generic[P, R]``): the backend is the DB
    adapter boundary. Payloads are stored as ``dict[str, object]`` (the
    JSONB ``payload`` column) regardless of the actor's typed payload
    model. Generic parameters here would propagate ``P`` and ``R`` into
    every method (``dispatch_batch``, ``mark_succeeded``, etc.) with no
    safety benefit at the storage layer. The worker consumer
    reconstructs the typed ``JobContext[P]`` at dispatch time using
    ``ActorRef.payload_type.model_validate(row.payload)``.
    """

    BACKEND_PROTOCOL_VERSION: ClassVar[int]

    supports_transactional_simulation: ClassVar[bool] = False
    """Whether this backend simulates transactional sub-enqueue via a
    buffer (True) or relies on real database transactions (False).

    ``PostgresBackend`` returns False, its real PG transaction provides
    the atomicity guarantee directly: sub-job INSERTs run on the open
    LOOP-scope connection and are rolled back along with the parent's
    writes if the actor raises.

    ``InMemoryBackend`` returns True, it has no real transaction
    concept, so ``SubJobEnqueuer`` buffers ``EnqueueArgs`` and flushes on
    actor success / discards on failure. A third-party ``Backend``
    implementation that wants transactional simulation in tests can opt
    in by overriding this to True.
    """

    # ── Enqueue ────────────────────────────────────────────────────────
    async def enqueue(self, args: EnqueueArgs) -> JobRow: ...

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: "ConnLike | None" = None,
        enforce_max_pending: bool = True,
    ) -> list[JobRow]:
        """Insert multiple jobs in a single batched operation.

        All items in *args_list* must be validated before calling this
        method, the backend does not re-validate payloads.  The list
        must be non-empty and contain at most 1000 items (enforced by the
        client layer).

        When *enforce_max_pending* is true (the default), items carrying a
        resolved ``max_pending`` cap are admission-checked as one
        aggregate, existing pending+scheduled per actor plus this batch ,
        and admission PARTITIONS per actor: an over-cap actor's items are
        refused as a whole group, every other actor's items are inserted,
        and :class:`~taskq.exceptions.BatchMaxPendingExceededError` raises
        at the transaction boundary, after the admitted items commit when
        this call owns the transaction, immediately after the insert on a
        caller-supplied connection with an open transaction (that
        transaction's commit/rollback decides their durability). Pass
        ``enforce_max_pending=False`` only when the caller pre-admitted
        every item against current capacity (the cron tick's suppression
        preflight), where a re-check could abort an unrelated batch on a
        race.

        Returns one :class:`JobRow` per item in *args_list*, in the same
        order.  For idempotency-key collisions the existing row is
        returned; its ``id`` will differ from the requested ``args.id``.
        On a cap refusal no rows are returned, the typed error carries
        the refused item indices and the admitted count instead.
        """
        ...

    async def enqueue_batch_fast(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: "ConnLike | None" = None,
        enforce_max_pending: bool = True,
    ) -> int:
        """Insert multiple jobs via the COPY FROM protocol for maximum throughput.

        COPY cannot evaluate expressions or handle conflicts, so the write
        is two statements inside one transaction: a bare COPY of the
        domain-insensitive columns, then a corrective UPDATE
        (``enqueue_batch_fast_fixup``) that stamps/decides the
        clock-sensitive ones, ``status``, ``scheduled_at``,
        ``schedule_to_close``, ``result_expires_at``, from the database
        clock (``clock_timestamp()``); ``created_at`` takes its DDL
        default (``now()``).  Nothing is observable half-fixed: both
        statements commit or abort together.

        Consequences of the COPY-no-conflicts shape:

        - ``scheduled_at=None`` means immediate, the fixup's server-side
          CASE stamps it and decides ``pending``/``scheduled`` (the same
          single-arbiter contract as :meth:`enqueue`/:meth:`enqueue_batch`).
        - ``schedule_to_close_interval``/``result_ttl`` are anchored to the
          server clock at ENQUEUE time by the fixup, a future-scheduled
          item with a short interval can therefore fail DeadlineExceeded
          before it is ever dispatched.
        - A duplicate ``idempotency_key``, within the batch or already
          stored, violates the unique index and aborts the ENTIRE batch
          (all-or-nothing atomicity; nothing is written), surfacing as
          :class:`~taskq.exceptions.DuplicateIdempotencyKeyError`.
        - Items carrying a resolved ``max_pending`` cap are
          admission-checked as one aggregate before the COPY, with the
          same per-actor partition as :meth:`enqueue_batch`, within-cap
          actors' rows are COPY'd, an over-cap actor's items are refused
          and raise
          :class:`~taskq.exceptions.BatchMaxPendingExceededError` after
          the COPY commits; disable with ``enforce_max_pending=False``
          only for pre-admitted internal callers.

        Returns the count of rows written.  On success this is exactly
        ``len(args_list)``, this path never deduplicates, so the count
        never includes pre-existing rows.  The in-memory mirror implements
        the same contract: duplicates raise
        :class:`~taskq.exceptions.DuplicateIdempotencyKeyError` before
        any row is written, and the count is the number of items.

        This is a performance-focused variant of :meth:`enqueue_batch`
        (which DOES deduplicate idempotency-key collisions via ``ON
        CONFLICT``).  Use for bulk import / backfill with 10K+ rows where
        collision handling is not needed.  Max batch size is 50 000
        (client-enforced).
        """
        ...

    async def enqueue_with_conn(
        self,
        conn: "ConnLike",
        args: EnqueueArgs,
    ) -> JobRow:
        """Enqueue a job using the supplied connection.

        The connection MUST already be in an open transaction managed by
        the caller, this method does NOT issue BEGIN/COMMIT. The
        autonomous variant ``enqueue(args)`` acquires its own connection
        and opens a transaction internally.
        """
        ...

    # ── Dispatch ────────────────────────────────────────────────────────
    async def dispatch_batch(
        self,
        worker_id: UUID,
        queues: list[str],
        limit: int,
        lock_lease: timedelta,
    ) -> list[JobRow]: ...

    # ── Heartbeat ───────────────────────────────────────────────────────
    async def heartbeat_jobs(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
        *,
        disowned: Collection[UUID] = (),
    ) -> int:
        """Renew the lock lease of every running job *worker_id* holds,
        except the *disowned* ids, rows the worker could not record an
        outcome for, whose leases must lapse for the reclaim sweep. Returns
        the number of rows renewed."""
        ...

    async def extend_reservation_leases(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
        *,
        disowned: Collection[UUID] = (),
    ) -> int:
        """Renew the reservation-slot leases of every running job
        *worker_id* holds, with the same *disowned* exclusion as
        :meth:`heartbeat_jobs`. Returns the number of slots renewed."""
        ...

    # ── Terminal writes ─────────────────────────────────────────────────
    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> bool:
        """Mark a job succeeded, computing ``result_expires_at`` at completion.

        *attempt* is the attempt-identity epoch: the handler's
        dispatch-time job-row ``attempt`` snapshot. The terminal fence is
        one epoch deeper than the worker fence, the write lands only on
        a row whose current ``attempt`` matches, so a stale handler's
        write after a same-worker reclaim/redispatch (the row re-dispatched
        at ``attempt + 1`` on the same worker) no-ops exactly like a
        different worker's late write. ``None``, a caller that cannot
        present the epoch, also no-ops: a terminal write that cannot
        prove which attempt it terminates must not terminate any attempt.

        *claim_epoch* is the non-saturating claim-identity fence, the
        ``claim_epoch`` value of the handler's own claim view. The
        displayed attempt counter saturates at the smallint ceiling,
        where a reclaim plus a redispatch would hand the stale handler
        and the live one the same (worker, attempt) pair; the claim
        epoch keeps advancing, so the stale write can only no-op.
        ``None`` never satisfies the equality, the same
        cannot-prove-it doctrine *attempt* applies.

        The result reaches the backend in exactly one of two forms:
        ``result``, the actor's result dict, which the backend serializes
        exactly once (orjson via :func:`taskq._json.dumps_jsonb_str`,
        NUL-guarded), or ``result_bytes``, the result already serialized
        to orjson bytes (the exact output of :func:`taskq._json.dumps`),
        which the backend reuses as-is: bound as
        ``result_bytes.decode("utf-8")`` with ``result_size_bytes =
        len(result_bytes)`` and no second serialization.  The worker
        consumer always passes ``result_bytes`` (it serialized the result
        once already); direct callers normally pass ``result``.  Passing
        both non-None raises ``ValueError``; both ``None`` stores a NULL
        result.  The ``result_bytes`` form is NUL-guarded at the same
        boundary, raising the same ``ValueError`` the dict form raises.

        Expiry resolution, first match wins: a non-NULL stored
        ``actor_config.result_ttl`` (operator-owned) applies; otherwise
        *fallback_result_ttl*, the worker-side ``@actor(result_ttl=...)``
        literal, which the terminal-write SQL cannot see, applies;
        otherwise the row's existing ``result_expires_at`` is kept. The
        computed arms use ``clock_timestamp()``, the wall-clock time the
        write executes, not the transaction start, so neither a long
        queue wait nor a long actor runtime can make a job complete
        already expired and have its result reaped immediately.
        """
        ...

    async def mark_succeeded_with_conn(
        self,
        conn: "ConnLike",
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> bool:
        """Mark a job succeeded using the supplied connection.

        Used by the consumer when a LOOP-scope ``asyncpg.Connection`` is
        available so the success status update commits atomically with
        the actor's writes and sub-job INSERTs in the same transaction.
        The connection MUST already be in an open transaction; this
        method does NOT open or close one. The autonomous variant
        ``mark_succeeded(...)`` acquires its own connection.

        ``result`` / ``result_bytes`` follow the same two-form contract as
        :meth:`mark_succeeded`, pass exactly one, or neither for a NULL
        result.  ``fallback_result_ttl`` follows the same resolution rule
        as :meth:`mark_succeeded`.  ``attempt`` and ``claim_epoch`` follow
        the same fencing as :meth:`mark_succeeded`.
        """
        ...

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> JobRow:
        """Mark a running job failed, or schedule a retry *retry_delay* later.

        ``retry_delay=None`` is the terminal-fail arm (``status='failed'``,
        the original ``error_info`` persisted).  A non-None delay is applied
        by the backend's own clock, never the caller's: ``scheduled_at =
        now() + delay``, floored at
        :data:`taskq.constants.MIN_DEFERRAL_INTERVAL`, the same bound the
        deferral arms apply, so a failure-retry decision can never requeue
        below the deferral floor, and the ``scheduled``/``pending`` status
        derives from the effective delay alone (a sub-floor delay still
        lands ``scheduled`` at least the floor out).  The same statement
        arbitrates the ``schedule_to_close`` deadline server-side, when
        ``clock_timestamp() + effective delay`` would land past the
        deadline, the row is failed with ``error_class='DeadlineExceeded'``
        instead of retried, so app↔DB clock skew can neither void the
        retry backoff nor kill a job whose deadline has not actually
        passed.

        *attempt* is the attempt-identity epoch, see
        :meth:`mark_succeeded`. A fenced-out write (stale epoch, wrong
        worker, or a row that moved) raises
        :class:`~taskq.exceptions.WorkerOwnershipMismatch`, which
        :func:`taskq.retry.safe_mark_failed_or_retry` converts to the
        ``None`` the handlers treat as a no-op.
        """
        ...

    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> bool: ...

    async def write_cancel_escalation(
        self,
        job_id: JobId,
        worker_id: UUID,
        phase: Literal[2],
    ) -> bool: ...

    async def mark_abandoned(
        self,
        job_id: JobId,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool: ...

    async def mark_snoozed(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        outcome: SnoozeOutcome = "snoozed",
        attempt: int | None = None,
        claim_epoch: int | None = None,
        denial_reason: DenialReason = "capacity",
    ) -> Literal["scheduled", "failed", "noop"]:
        """Release a running job back to the queue without consuming retry
        budget.

        *attempt* is the attempt-identity epoch, see
        :meth:`mark_succeeded`; a fenced-out write returns ``"noop"``.
        *claim_epoch* is the claim-identity fence, see
        :meth:`mark_succeeded`; it applies to all three arms, the two
        terminal deadline exits included.

        A non-terminal snooze/denial writes NO ``job_attempts`` /
        ``job_events`` rows, it is admission control or a voluntary
        deferral, not an execution, and is counted on the job row
        (``snooze_count``, or ``rate_limit_blocked_count`` when *outcome*
        is a denial) plus OTEL.  ``max_attempts`` is never raised: the
        ceiling is a bound, not a counter.

        *outcome* admits only the three deferral outcomes
        (:data:`SnoozeOutcome`), the statement's arms key on exactly
        those; an execution outcome has no arm (PG would leave the job
        stranded ``running``) and raises ``ValueError`` at the boundary
        on both backends instead.  *delay* is floored at
        :data:`taskq.constants.MIN_DEFERRAL_INTERVAL`: a non-consuming
        deferral reschedules at least that far out, so a zero delay
        cannot park the job at the head of the dispatch order.

        Every deferral shape refunds the claim's attempt increment
        (floored at 0), so no deferral, actor-requested or admission
        denial, spends retry budget.  An admission denial carries HTTP
        429 semantics: it reports that the fleet had no slot, which says
        nothing about the work, so it can neither charge the budget nor
        decide the outcome.  A denied job reschedules until capacity
        frees; its only terminal exit is its own ``schedule_to_close``
        (``"failed"``, ``DeadlineExceeded``), and the counters on the row
        are how sustained contention stays visible.

        *denial_reason* names the cause of a denial-class outcome
        (:data:`DenialReason`), ``"capacity"`` (the default) is a
        saturation denial, the store answering "full"; ``"unavailable"`` is
        the store failing to answer.  Both take the identical non-consuming
        path; the value is validated at the boundary so an undefined reason
        is refused rather than silently accepted.  It is persisted on
        exactly one surface: the ``state_change`` event detail of the
        mark_snoozed deadline arm, and only when that TERMINAL arm fires
        on a denial-keyed outcome (``reservation_denied`` /
        ``rate_limit_denied`` whose ``schedule_to_close`` has lapsed),
        where it names which starvation (saturation vs store outage)
        killed the job.  The discriminator is terminality, not
        denial-ness: every non-terminal deferral, denial or not, writes
        no event row, so grepping events for ``denial_reason`` finds
        nothing on a live denial; the row's outcome counters are the
        live-denial signal.
        """
        ...

    async def mark_retry_after(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        consume_budget: bool = True,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]: ...

    async def mark_interrupted(
        self,
        job_id: JobId,
        worker_id: UUID,
        *,
        attempt: int,
        hold: timedelta,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        claim_epoch: int | None = None,
    ) -> Literal["pending", "scheduled", "failed:DeadlineExceeded", "noop"]:
        """Release a running attempt this worker cannot finish because the
               process is going away.

               The interruption releases a *started* attempt, so it does NOT
               refund the claim's increment: the attempt did start executing, and
               refunding it would re-create the exact attempt epoch the
               interrupted handler still holds; that handler's later terminal
               write would then pass the attempt fence and land on the
               re-dispatched attempt, and the live execution's own terminal write
        would no-op. No ``job_attempts`` row is written (an
               interruption is not an execution outcome), one ``job_events``
               state_change with ``detail.reason = 'interrupted'`` records the
               transition, and the row's ``interrupt_count`` is bumped. The
               interrupted attempt counts against the retry budget; the
               re-dispatch claims a fresh epoch at ``attempt + 1``.

               *hold* > 0 parks the row ``scheduled`` until the releasing process
               is provably gone (a job released while its coroutine may still be
               alive in this process must not be claimable elsewhere until then);
               *hold* = 0 lands the row ``pending`` at the head of the order, the
               row is genuinely free and the actor is gone, so no deferral floor
               applies. A hold that would push the row past its
               ``schedule_to_close`` fails the job on the deadline instead
               (``"failed:DeadlineExceeded"``), the same terminal exit every
               deferral arm honours.

               Fenced on ownership, the attempt epoch, and ``cancel_phase = 0``:
               an operator cancel in flight wins and the call returns ``"noop"``
               so the caller routes to the cancel ladder (the row carries the
               operator's request; the deploy must not launder it into a release:
               a row whose ``cancel_attempted_at`` is set terminalises as
               cancelled, never as available).

               *attempt* is the attempt-identity epoch, see
               :meth:`mark_succeeded`. Here it is required, not optional: a
               release that cannot prove which attempt it is handing back must
               not touch the row (``"noop"``). *claim_epoch* is the
               claim-identity fence, see :meth:`mark_succeeded`, applied to
               both arms, the deadline exit included.
        """
        ...

    # ── Attempt history ─────────────────────────────────────────────────
    async def write_attempt(self, attempt: AttemptRow) -> None: ...

    async def get_attempts(self, job_id: JobId) -> list[AttemptRow]: ...

    async def get_events(self, job_id: JobId) -> list[EventRow]: ...

    async def poll_reclaim_events(
        self,
        after_id: int,
        limit: int = DEFAULT_RECLAIM_POLL_LIMIT,
        *,
        visibility_delay: timedelta | None = None,
    ) -> list[EventRow]:
        """Return up to *limit* crash-reclaim events with ``event_id >
        after_id``, ascending, the durable cursor behind
        ``TaskQ.watch_reclaims``.

        **An event can be silently missed if a ``job_events`` writer
        transaction stays open longer than the visibility-delay margin
        between its INSERT and its COMMIT**, ids are allocated at INSERT
        time but transactions commit out of order, so a late-committing
        lower-id row can land behind an already-advanced cursor.  Rows
        are therefore held back by a trailing-watermark filter
        (*visibility_delay*; backend-configured default when ``None`` ,
        see :data:`taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY` for
        the exact assumption and its violation modes, and
        ``PostgresBackend.check_reclaim_visibility_delay_risk`` for the
        diagnostic that makes a violation operator-visible).
        """
        ...

    # ── Cancel signals ──────────────────────────────────────────────────
    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool: ...

    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter* in a set-based operation.

        Pending/scheduled jobs → terminal 'cancelled'.
        Running jobs → cancel_phase=1 (cooperative cancel + NOTIFY).

        The filter's ``limit``, ``cursor``, and ``order_by`` fields are
        ignored, this is a bulk write, not a paginated read. The write is
        still not paginated in outcome, every matching row is cancelled ,
        but it executes as internally bounded committed batches, so a
        mid-operation failure leaves partial progress rather than rolling
        back everything (re-running continues; already-cancelled rows are
        skipped).

        **Guardrail:** the client layer (:meth:`JobsClient.cancel_where`)
        rejects empty filters (no predicates) with
        :class:`EmptyFilterError`. Backend implementations receive a
        filter that has already been validated. A direct backend call
        with ``JobFilter()`` renders ``WHERE TRUE`` and cancels the
        entire table, callers using the backend directly are
        responsible for validating the filter.

        Returns a :class:`BulkCancelResult` with counts and affected IDs.
        """
        ...

    async def poll_cancel_flags(
        self,
        worker_id: UUID,
    ) -> list[CancelFlag]: ...

    # ── Admin operations ──────────────────────────────────────────────
    async def retry_job(self, job_id: JobId) -> bool:
        """Re-run a job that has come to rest, by re-pending it.

        An operator re-run is "run this again", so every terminal status
        is a valid source: ``failed``/``crashed``/``cancelled``, and also
        ``succeeded`` (the replay path after a bad deploy, the status
        records that the actor returned, never that its side effects were
        right) and ``abandoned`` (an infrastructure interruption, not a
        failure). ``running`` is excluded on correctness grounds:
        re-pending a row while an attempt is live races that attempt's
        terminal write and the job can execute twice concurrently.
        ``pending``/``scheduled`` are excluded because the job is already
        queued, there is nothing to put back, and re-pending would
        discard its place in the dispatch order.

        The attempt counter is NOT reset: an idempotent admin operation
        must not restart the counter, so a re-run job climbs to fresh
        attempt numbers and
        no ``job_attempts`` write can collide on a spent epoch's primary
        key.  ``max_attempts`` rises to ``GREATEST(max_attempts,
        attempt + 1)`` (capped at the smallint bound), which opens the
        budget gates for at least one fresh execution while a
        mid-budget re-run keeps its remaining budget.

        Returns ``True`` if the job was retried, ``False`` if it was not
        in a retryable state, or the smallint-bound ceiling cannot rise
        past the spent attempt, in which case the row stays terminal
        rather than re-pending a job whose next claim would overflow.
        """
        ...

    # ── Scheduling / sweeps ─────────────────────────────────────────────
    # The sweep methods take no ``now`` parameter: the arbiter is the
    # backend's own clock (PG: ``clock_timestamp()`` in the statement;
    # InMemory: the injected Clock), a caller-supplied timestamp would be
    # a second, skewable domain mixed into the predicate.
    async def scheduled_to_pending(self) -> int:
        """Promote ``scheduled`` jobs whose ``scheduled_at`` has passed.

        The backend's own clock is the arbiter (PG evaluates
        ``scheduled_at <= clock_timestamp()`` server-side; InMemory
        compares against its injected Clock).  Returns the count of
        promoted rows.
        """
        ...

    async def deadline_sweep(self) -> int:
        """Fail pending/scheduled jobs whose ``schedule_to_close`` has passed.

        Transitions to ``failed`` with ``error_class='DeadlineExceeded'``,
        arbitrated by the backend's own clock.  Returns the count of swept
        rows.
        """
        ...

    async def reclaim_expired_locks(
        self,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
    ) -> int:
        """Reclaim ``running`` jobs whose lock has expired.

        The expiry check is arbitrated by the backend's own clock; the
        grace parameters only widen the carve-out for jobs with an
        in-flight cancel request.  Returns the count of reclaimed rows.
        """
        ...

    # ── Read ────────────────────────────────────────────────────────────
    async def get(self, job_id: JobId) -> JobRow | None: ...

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        """List jobs matching *filters*, returning at most ``filters.limit``
        rows in keyset-pagination order.

        ``filters.status`` accepts a single :data:`JobStatus` or a
        sequence of statuses; ``filters.unfinished`` is a meta-filter for
        non-terminal (``True``) or terminal (``False``) statuses ,
        'unfinished' here means 'not yet finished' (pending, scheduled, or
        running).  See :class:`JobFilter` for details.
        """
        ...

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        """Return pending+scheduled job counts per actor.

        Returns a dict mapping actor name to count.  Only actors with
        at least one pending or scheduled job appear in the result.
        Actors not in the result have a count of zero.  The ``actors``
        list is used as an ``IN``/``ANY`` filter, pass all distinct actor
        names from a batch to fetch all counts in one round-trip.
        """
        ...

    async def count_active_jobs(self, queues: list[str]) -> int:
        """Count non-terminal jobs (pending, scheduled, running) in the given queues.

        Returns the total count across all specified queues. Used by the
        drain monitor to detect when queues are empty. An empty queues
        list returns 0.
        """
        ...

    async def get_actor_max_pending(self) -> dict[str, int | None]:
        """Return the stored ``actor_config.max_pending`` for every actor
        with a row.

        Key present with an ``int`` value: the stored (operator-owned)
        limit. Key present with ``None``: a row exists but the column is
        NULL (a cleared override). Key absent: no stored row. Client-side
        capacity resolution
        (:class:`taskq.client._capacity.ActorCapacityCache`) treats
        "absent" and "NULL" identically, both fall back to the
        ``@actor(...)`` literal; the distinction is preserved here only
        so observability callers can tell them apart.

        This is the enqueue-path analog of the dispatch CTE's per-cycle
        ``actor_config`` join: one small whole-table read, consumed
        through a TTL-bounded cache so the hot path pays no per-enqueue
        query.
        """
        ...

    # ── NOTIFY hook ─────────────────────────────────────────────────────
    def subscribe_wake(
        self, queues: Iterable[str] | None = None
    ) -> AsyncContextManager[asyncio.Event]: ...

    def subscribe_cancel_wake(self) -> AsyncContextManager[asyncio.Event]:
        """Return an async context manager yielding a fresh ``asyncio.Event``
        that is set whenever a cancel NOTIFY arrives for any job.

        The heartbeat loop uses this to interrupt its sleep immediately on
        cancel, rather than waiting for the next scheduled tick.
        """
        ...

    # ── Schedule CRUD ────────────────────────────────────────────────────
    async def create_schedule(self, args: ScheduleCreateArgs) -> ScheduleRecord: ...

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduleRecord]: ...

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: ScheduleUpdateArgs,
    ) -> ScheduleRecord: ...

    async def delete_schedule(self, schedule_id: UUID) -> None: ...

    # ── Batch operations ────────────────────────────────────────────
    async def enqueue_batch_atomic(
        self,
        items: Iterable[EnqueueArgs],
        *,
        batch_id: UUID,
        queue: str,
        batch_row: BatchRow | None,
        finalizer_args: EnqueueArgs | None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> list[JobRow]: ...

    async def create_batch(
        self,
        batch_id: UUID,
        queue: str,
        expected_size: int,
        failure_threshold: int | None,
        finalizer_job_id: UUID | None,
        originating_actor: str | None,
        *,
        connection: "ConnLike | None" = None,
    ) -> None: ...

    async def increment_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: "ConnLike | None" = None,
    ) -> tuple[int, int | None, int]: ...

    async def reset_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: "ConnLike | None" = None,
    ) -> int: ...

    async def abort_batch(
        self,
        batch_id: UUID,
        *,
        connection: "ConnLike | None" = None,
    ) -> int: ...

    async def complete_batch(
        self,
        batch_id: UUID,
        *,
        connection: "ConnLike | None" = None,
    ) -> None: ...

    async def get_batch(
        self,
        batch_id: UUID,
    ) -> BatchRow | None: ...

    async def list_batches(
        self,
        filter: BatchFilter,
    ) -> list[tuple[BatchRow, BatchCounts]]: ...

    async def count_batch_non_terminal(
        self,
        batch_id: UUID,
        *,
        connection: "ConnLike | None" = None,
    ) -> int: ...

    async def prune_old_batches(
        self,
        cutoff: datetime,
    ) -> int: ...
