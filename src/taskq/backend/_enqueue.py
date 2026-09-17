"""Enqueue operations for PostgresBackend.

``enqueue``, ``enqueue_with_conn``, ``enqueue_batch``, and
``enqueue_batch_fast`` live here as module-level functions taking
``(pool, sql: SqlTemplates, schema, clock, ...)`` parameters.
:class:`~taskq.backend.postgres.PostgresBackend` methods are thin
wrappers that delegate.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, NoReturn
from uuid import UUID

import structlog
from asyncpg.exceptions import (
    LockNotAvailableError,
    UniqueViolationError,
)

from taskq._advisory import (
    _LOCK_TIMEOUT_READ_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the one implementation of the lock_timeout GUC statements, shared with the advisory/sweep machinery — a local copy would drift from the discipline it mirrors.
    _LOCK_TIMEOUT_SET_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: same — the shared set_config statement.
    acquire_advisory_xact_lock_bounded,
)
from taskq.backend._protocol import (
    ConnLike,
    EnqueueArgs,
    JobRow,
    batch_cap_groups,
    duplicate_pair_actor_mismatch,
    first_duplicate_idempotency_pair,
    first_singleton_collision_actor,
)
from taskq.backend._records import (
    _job_row_from_record,
    item_jsonb_param,
    item_tags_jsonb_param,
    jsonb_param,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend.clock import Clock
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.connections import (
    _with_fresh_connection_retry,  # pyright: ignore[reportPrivateUsage]  # Why: the one implementation of the dead-on-acquire retry, shared with the bulk-cancel drain — a local copy would drift from the discipline it documents.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical identifier regex, shared with every schema-qualified SQL site — a local copy would drift.
)
from taskq.exceptions import (
    BatchMaxPendingExceededError,
    DuplicateIdempotencyKeyError,
    IdempotencyKeyActorMismatchError,
    IdempotencyKeyLockTimeoutError,
    MaxPendingExceededError,
    MaxPendingLockTimeoutError,
    ScopedIdempotencyMigrationPendingError,
    SingletonCollisionError,
    UniqueForLockTimeoutError,
)
from taskq.obs import (
    get_logger,
    record_backpressure_error,
    record_enqueue_dedup,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_enqueue",
    "_enqueue_batch",
    "_enqueue_batch_fast",
    "_enqueue_on_conn",
    "_enqueue_with_conn",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_SINGLETON_CONSTRAINT_NAME = "jobs_singleton_uniq"

#: Bounded wait (milliseconds) for the max_pending advisory lock on the
#: single-enqueue path. The lock is held across a count query + INSERT (a
#: couple of round trips -- low single-digit milliseconds on a healthy
#: pool), so 5 s tolerates a burst of hundreds of queued racers while
#: keeping tail latency capped instead of linear in the racer count. A
#: racer that exhausts the budget gets MaxPendingLockTimeoutError -- the
#: same typed backpressure treatment as a cap rejection -- rather than
#: queueing indefinitely. ``0`` (or less) waits indefinitely, matching the
#: ``lock_timeout`` GUC convention used by migrate.py.
DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS: float = 5000.0

#: Bounded wait (milliseconds) for the unique_for single-flight advisory
#: lock on the single-enqueue path. Why a SEPARATE constant rather than
#: reusing DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS: the two budgets bound
#: different semantics (capacity admission vs identity dedup) and are
#: tuned by different operators -- an API path that treats
#: MaxPendingLockTimeoutError as shed-load wants its backpressure wait
#: short, while a unique_for caller whose correct contention outcome is a
#: dedup return may want a longer wait before giving up on the answer.
#: Same 5 s starting point: the holder's critical section is the same
#: scale (one preflight SELECT + one INSERT), so the burst arithmetic
#: carries over. ``0`` (or less) waits indefinitely, matching the
#: ``lock_timeout`` GUC convention shared with the max_pending budget.
DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS: float = 5000.0

#: Bounded wait (milliseconds) for the idempotency token INSERT's
#: speculative-lock conflict — another transaction's UNCOMMITTED row with
#: the same ``(idempotency_scope, idempotency_key)`` pair. On a
#: transactional consumer the holder IS the actor's own open transaction
#: (``default_start_to_close`` = None means unbounded), so this wait is
#: the one enqueue block that can legitimately last minutes; the budget
#: bounds the VICTIM, not the holder, and exhaustion means "the dedup
#: answer could not be determined in time" — the typed
#: :class:`IdempotencyKeyLockTimeoutError` with retry-yields-dedup
#: guidance, the same treatment :data:`DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS`
#: gives its identical situation. Same 5 s starting point as the sibling
#: budgets for one-branch symmetry. ``0`` (or less) waits indefinitely,
#: the ``lock_timeout`` GUC convention shared with the other two.
DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS: Final[float] = 5000.0

# The old single-column idempotency index, still present alongside the new
# composite one during the rolling-deploy window between
# 01.00.03_01_pre_idempotency_scope.sql and
# 01.00.03_01_post_idempotency_scope_drop_old_index.sql. See
# ScopedIdempotencyMigrationPendingError for the full rationale.
_LEGACY_IDEMPOTENCY_KEY_CONSTRAINT_NAME = "jobs_idempotency_key_uniq"

# The composite (idempotency_scope, idempotency_key) arbiter index. The
# non-fast paths' ON CONFLICT targets it and dedupes; the COPY path has
# no arbiter, so a violation reported against it is a same-pair
# duplicate (in-batch or raced against a stored row) and is classified
# to the typed DuplicateIdempotencyKeyError in _enqueue_batch_fast.
_COMPOSITE_IDEMPOTENCY_KEY_CONSTRAINT_NAME = "jobs_idempotency_scope_key_uniq"

# Postgres' unique-violation detail line for the composite index renders
# the colliding (scope, key) values RAW and unquoted (verified against
# live PG 18: commas, spaces, quotes, newlines all pass through
# unescaped), so a scope containing ", " makes the detail positionally
# AMBIGUOUS -- scope "a, b" key "c" reports
# "Key (idempotency_scope, idempotency_key)=(a, b, c) already exists.",
# which a left-to-right split mis-reads as scope "a" key "b, c", and a
# localized or truncated message renders nothing matchable at all. The
# COPY conflict branch therefore never PARSES the detail: attribution is
# resolved from the batch's own contents plus a targeted post-abort
# SELECT (see _attribute_copy_duplicate). This template survives only as
# the residual fallback for the narrow case where the conflicting row
# cannot be resolved from the table either (committed and deleted again
# by a concurrent transaction in the gap between the abort and the
# lookup): it renders each of the batch's own candidates into PG's
# detail format and matches, naming the pair when exactly one rendering
# equals the server's detail and degrading to unattributed-but-typed
# otherwise -- never a wrong pair.
_COMPOSITE_IDEMPOTENCY_DETAIL_TEMPLATE = (
    "Key (idempotency_scope, idempotency_key)=({scope}, {key}) already exists."
)

# The singleton index's detail line is single-column ("Key (actor)=(...)
# already exists."), so it has no positional ambiguity — but values still
# render raw and unquoted, and a localized or truncated message renders
# nothing matchable at all. Like the composite template above, this is
# only the residual fallback of _attribute_singleton_collision, never the
# source of truth.
_SINGLETON_DETAIL_TEMPLATE = "Key (actor)=({actor}) already exists."

#: The actor named when a jobs_singleton_uniq violation cannot be
#: attributed to any batch item even after the post-abort lookup and the
#: detail match (the blocking row committed and left the live status set
#: in the gap between the abort and the lookup, and the server's detail
#: text was unusable). The typed refusal still raises — an operator gets
#: the collision class without a made-up actor name (never a wrong actor).
_UNATTRIBUTED_SINGLETON_ACTOR: Final[str] = "<unattributed>"


@asynccontextmanager
async def _optional_savepoint(conn: ConnLike, *, enabled: bool) -> AsyncGenerator[None, None]:
    """Enter a transaction scope on *conn* when *enabled*, else a no-op scope.

    Nested inside an open transaction asyncpg opens a SAVEPOINT; on a
    bare connection it opens a real short transaction. Either way an
    exception raised inside rolls the scope back before propagating, so
    a statement error can be converted to a typed error WITHOUT aborting
    the surrounding scope — the caller-owned-transaction discipline the
    bounded advisory acquire (``taskq._advisory``) and the token-bucket
    lock read already follow.

    Why a flag-gated context manager instead of an ``if`` at the call
    site: the guarded statement stays a single literal call — the
    twenty-plus-parameter enqueue INSERT must not exist as two copies
    that can drift — while each arm opts in explicitly, keeping the
    arms that need no savepoint on their zero-extra-round-trip path.
    """
    if enabled:
        async with conn.transaction():
            yield
    else:
        yield


_DEDUP_WARN_PER_HIT_LIMIT: Final[int] = 3
"""Per-call ceiling on per-hit terminal-target dedup WARNINGs before the
aggregate summary takes over (``_DedupWarnBudget``). The bound the
batch-scale contract demands is small: a re-submitted 1000-item batch
    against 500 terminal targets must not emit 500 WARNINGs from one call.
    Three per-hit lines plus the one summary line stays at four —
under the pinned bound of five with headroom, and still three full
per-hit samples (actor, key, status) for an operator triaging which
identities are pinned to dead jobs."""


@dataclass
class _DedupWarnBudget:
    """Per-call state bounding the terminal-target WARNING arm of
    :func:`_log_enqueue_dedup` at batch scale.

    Why per-CALL aggregation rather than the dependency-failure
    window-gate's time window (``worker/_consumer.py``, the precedent this
    follows for the flood shape itself): the small-scale per-hit contract
    must survive rapid successive calls — a time window keyed per process
    would silently suppress the second small terminal-target batch logged
    within it, and a window keyed per identity is unbounded in the batch
    dimension the flood lives in. A call-local budget is deterministic,
    needs no clock, and a :class:`contextvars.ContextVar` keeps concurrent
    batch calls isolated per task.
    """

    terminal_hits: int = 0
    warned_hits: int = 0

    def charge_warning(self) -> bool:
        """Count one terminal-target hit; True while a per-hit WARNING slot remains."""
        self.terminal_hits += 1
        if self.warned_hits < _DEDUP_WARN_PER_HIT_LIMIT:
            self.warned_hits += 1
            return True
        return False

    @property
    def suppressed_hits(self) -> int:
        """Terminal-target hits whose per-hit WARNING the budget suppressed."""
        return self.terminal_hits - self.warned_hits


_dedup_warn_budget: ContextVar[_DedupWarnBudget | None] = ContextVar(
    "taskq_enqueue_dedup_warn_budget", default=None
)
"""The dedup WARNING budget of the CURRENT enqueue-batch call, if any.

Set by the batch tiers around their result assembly (both backends); the
single-enqueue paths never set it — a single enqueue is one hit by
construction, no flood is possible, and the per-hit escalation contract
holds there unchanged."""


def _log_enqueue_dedup(row: JobRow, *, dedup_reason: str) -> None:
    """Report one enqueue dedup hit — the log line plus the dedup counter.

    A terminal target never runs the work again — the identity stays
    pinned to a dead job until it ages out of retention (an idempotency
    key until pruned; a unique_for window whose ``unique_states`` fold a
    terminal state in, for the window's remainder) — so the hit is
    louder than the live-job case, which is normal single-flight
    operation. Shared by every dedup site — the idempotency arm and the
    unique_for arm of the single-enqueue path, the batch result
    assembly, and the InMemory mirror — so the sites cannot drift apart
    in fields or volume; the per-site truth is ``dedup_reason`` alone.

    The ``taskq.enqueue.dedups`` counter rides the same shared seam for
    the same reason: every hit counts once on both backends, at every
    scale, regardless of the log arm the hit took — and the batch-scale
    WARNING budget below cannot mute the rate signal with the lines.

    At batch scale the terminal-target WARNING arm is bounded per call:
    when the current call carries a ``_DedupWarnBudget`` (the
    batch tiers set one), the first ``_DEDUP_WARN_PER_HIT_LIMIT``
    terminal hits warn per-hit exactly as before and the rest are
    counted for the call's ONE summary WARNING
    (:func:`_log_enqueue_dedup_warn_summary`) — a per-item WARNING flood
    is channel noise an operator mutes, destroying the signal the
    WARNING exists to raise. The INFO arm (every live-target hit,
    carrying status) is untouched at every scale.
    """
    record_enqueue_dedup(dedup_reason)
    fields: dict[str, object] = {
        "kind": "enqueue_deduplicated",
        "job_id": str(row.id),
        "actor": row.actor,
        "queue": row.queue,
        "identity_key": row.identity_key,
        "idempotency_key": row.idempotency_key,
        "idempotency_scope": row.idempotency_scope,
        "status": row.status,
        "existing_job_id": str(row.id),
        "dedup_reason": dedup_reason,
    }
    if row.status in TERMINAL_STATUSES:
        budget = _dedup_warn_budget.get()
        if budget is None or budget.charge_warning():
            logger.warning("enqueue_deduplicated", **fields)
        # Else: the hit is counted for the call's summary WARNING; its
        # per-hit line is the flood the budget exists to bound.
    else:
        logger.info("enqueue_deduplicated", **fields)


def _log_enqueue_dedup_warn_summary(budget: _DedupWarnBudget, *, dedup_reason: str) -> None:
    """Emit the ONE aggregate WARNING for the terminal-target dedup hits a
    call suppressed — the flood's total, named once.

    No-op when the budget suppressed nothing (small batches keep pure
    per-hit WARNINGs, indistinguishable from the pre-bound contract).
    Emitted on the failure path too (the caller's ``finally``): the
    suppressed hits already happened, and a summary that only fires on
    success is a failure that looks like a success.
    """
    suppressed = budget.suppressed_hits
    if suppressed <= 0:
        return
    logger.warning(
        "enqueue_deduplicated",
        kind="enqueue_deduplicated",
        dedup_reason=dedup_reason,
        aggregate="terminal_dedup",
        terminal_hits=budget.terminal_hits,
        warned_hits=budget.warned_hits,
        suppressed_hits=suppressed,
    )


def _attribute_duplicate_pair(
    detail: str | None,
    candidates: "set[tuple[str, str]]",
) -> tuple[str | None, str | None]:
    """Residual attribution of a composite-index COPY violation from the
    server's detail text — the fallback for when
    :func:`_attribute_copy_duplicate` cannot resolve the conflicting row
    from the table (a committed-and-instantly-deleted racer).

    Returns the unique candidate pair whose rendered detail equals the
    server's *detail*, or (None, None) when no candidate matches or the
    rendering is ambiguous. Callers pass the batch's own (scope, key)
    candidate set -- the violating pair is always among the batch's items
    (an in-batch duplicate or an item raced against a stored row).
    """
    if not detail:
        return (None, None)
    matches = [
        (scope, key)
        for scope, key in candidates
        if _COMPOSITE_IDEMPOTENCY_DETAIL_TEMPLATE.format(scope=scope, key=key) == detail
    ]
    if len(matches) == 1:
        return matches[0]
    return (None, None)


@dataclass(frozen=True, slots=True)
class _CopyDuplicate:
    """The pair a COPY composite-index violation aborted on and, when a
    committed row holds it, that row's actor and id."""

    scope: str | None
    key: str | None
    stored_actor: str | None = None
    stored_job_id: UUID | None = None

    @property
    def pair(self) -> tuple[str, str] | None:
        if self.scope is None or self.key is None:
            return None
        return (self.scope, self.key)


async def _attribute_copy_duplicate(
    conn: ConnLike,
    sql: SqlTemplates,
    admitted_args: list[EnqueueArgs],
    detail: str | None,
) -> _CopyDuplicate:
    """Resolve the pair a COPY composite-index violation aborted on —
    exactly, and independent of the driver's error text — together with
    the committed row holding it, if any.

    Runs on the conflict branch only, after the savepoint wrapping the
    COPY has rolled the statement back, so the caller's transaction scope
    answers queries again. COPY writes the batch in order and aborts at
    the first row whose pair the partial unique index already holds; the
    holder is either a COMMITTED table row or an earlier row of this same
    COPY. A unique-violation report means the conflicting transaction
    committed (had it rolled back, the COPY would have proceeded), so a
    fresh SELECT under READ COMMITTED sees every committed holder — and
    the savepoint rollback has removed this COPY's own rows, so the
    in-batch holders are reconstructed from the batch itself by
    :func:`first_duplicate_idempotency_pair`, the same pure rule the
    in-memory mirror applies over its own index. The result names the
    offending pair exactly for every in-batch duplicate and every
    raced-against-storage duplicate, whether or not the server's detail
    text was ambiguous, localized, or truncated.

    The one shape the resolution cannot see is a conflicting row that a
    concurrent transaction committed and deleted again in the gap between
    the abort and the SELECT; there the detail-text match is the last
    word, degrading to unattributed-but-typed rather than guessing.
    """
    keyed = [
        (args.idempotency_scope, str(args.idempotency_key))
        for args in admitted_args
        if args.idempotency_key is not None
    ]
    stored: dict[tuple[str, str], asyncpg.Record] = {}
    if keyed:
        recs = await conn.fetch(
            sql.enqueue_batch_fetch_existing,
            [scope for scope, _ in keyed],
            [key for _, key in keyed],
        )
        stored = {(str(rec["idempotency_scope"]), str(rec["idempotency_key"])): rec for rec in recs}
    pair = first_duplicate_idempotency_pair(admitted_args, stored.keys())
    if pair is None:
        scope, key = _attribute_duplicate_pair(detail, set(keyed))
        return _CopyDuplicate(scope, key)
    holder = stored.get(pair)
    return _CopyDuplicate(
        pair[0],
        pair[1],
        stored_actor=str(holder["actor"]) if holder is not None else None,
        stored_job_id=holder["id"] if holder is not None else None,
    )


async def _attribute_singleton_collision(
    conn: ConnLike,
    sql: SqlTemplates,
    admitted_args: list[EnqueueArgs],
    detail: str | None,
) -> str:
    """Resolve the actor a ``jobs_singleton_uniq`` violation aborted on —
    exactly, and independent of the driver's error text.

    Same shape as :func:`_attribute_copy_duplicate`: runs on the conflict
    branch only, after the savepoint wrapping the statement has rolled it
    back, so the caller's transaction scope answers queries again. The
    violating actor is always among the batch's singleton items (the
    partial index is keyed on ``(actor)`` and covers no other row), and a
    unique-violation report means the conflicting transaction committed,
    so a fresh SELECT under READ COMMITTED sees every committed holder —
    the batch's own contents plus that lookup name the actor exactly for
    every in-batch repeat and every raced-against-storage collision, via
    :func:`first_singleton_collision_actor`, the same pure rule the
    in-memory mirror's batch preflight applies over its own store.

    The residual cascade, in order, when the lookup comes back empty (the
    blocking row committed and left the live status set in the gap between
    the abort and the SELECT):

    1. A batch carrying exactly ONE singleton actor cannot have violated
       on any other actor — the index key makes that certain.
    2. The server's detail rendering is matched against the batch's own
       candidates (single-column key: no positional ambiguity), the same
       last-word technique ``_attribute_duplicate_pair`` applies.
    3. Both unusable (a localized or truncated message): degrade to the
       ``<unattributed>`` sentinel — typed and honest, never a guessed
       actor name.
    """
    singleton_actors = list(
        dict.fromkeys(
            args.actor for args in admitted_args if args.metadata.get("singleton") is True
        )
    )
    stored: set[str] = set()
    if singleton_actors:
        recs = await conn.fetch(sql.enqueue_batch_fetch_singleton_blockers, singleton_actors)
        stored = {str(rec["actor"]) for rec in recs}
    actor = first_singleton_collision_actor(admitted_args, stored)
    if actor is not None:
        return actor
    if len(singleton_actors) == 1:
        return singleton_actors[0]
    matches = [
        candidate
        for candidate in singleton_actors
        if _SINGLETON_DETAIL_TEMPLATE.format(actor=candidate) == detail
    ]
    if len(matches) == 1:
        return matches[0]
    return _UNATTRIBUTED_SINGLETON_ACTOR


async def _batch_cap_refusals(
    conn: ConnLike,
    sql: SqlTemplates,
    args_list: list[EnqueueArgs],
) -> list[MaxPendingExceededError]:
    """Compute the per-actor cap refusals for a batch without raising.

    One grouped count query for the whole batch (the same aggregated shape
    as the client's pre-check): existing pending+scheduled plus this
    batch's items, M1 ``>`` semantics so a batch filling exactly to the
    limit is admitted. Runs on the inserting connection, so sequential
    chunks sharing one transaction observe each other's rows and enforce
    the true aggregate.

    The cap enforced per actor is the *effective* one: the stored
    operator override wins over the carried literal (same resolution as
    the client's pre-check — a whole-table ``actor_config`` snapshot;
    that table holds one row per actor, so the extra fetch is cheap —
    and only runs when the batch carries caps at all). Items whose
    idempotency (scope, key) pair is already stored are discounted:
    the batch INSERT's ``ON CONFLICT`` arbiter returns the existing row
    instead of writing, so they consume no capacity — mirroring the
    single path, where an idempotency hit returns before any cap
    accounting. (``unique_for`` items are conservatively fully counted:
    only a preflight HIT bypasses the cap on the single path, and the
    batch cannot know hits without a per-item preflight that would
    defeat bulk throughput; a batch mixing unique_for retries near the
    cap may refuse loudly rather than admit silently.) A capacity-slot
    index is the heavyweight version of this guarantee; the count here is
    exact for the single statement it guards. Concurrent bulk batches on separate connections can still
    race (count-then-insert without a serializing lock — the
    single-enqueue path takes one, bulk paths deliberately do not, for
    throughput); that residual is documented, not silent.

    Returns one :class:`MaxPendingExceededError` per over-cap actor (the
    same typed refusal the single path raises); an empty list admits the
    whole batch. Raising is the CALLER's decision: the bulk tier
    partitions admission per actor (over-cap actors' items refused as a
    group, everyone else's admitted — see ``_enqueue_batch``), while the
    atomic chunk arm refuses the whole call before any INSERT.
    """
    groups = batch_cap_groups(args_list)
    if not groups:
        return []
    stored_rows = await conn.fetch(sql.list_actor_max_pending)
    stored = {str(rec["actor"]): rec["max_pending"] for rec in stored_rows}
    effective: dict[str, int] = {}
    for actor, (_, carried) in groups.items():
        override = stored.get(actor)
        effective[actor] = int(override) if override is not None else carried
    recs = await conn.fetch(sql.count_pending_jobs, list(groups))
    existing = {str(rec["actor"]): int(rec["cnt"]) for rec in recs}
    # Pairs already stored write no new row (ON CONFLICT returns the
    # existing one): discount them so a batch of pure retries is not
    # refused for capacity it will not consume. Scoped to capped actors
    # with idempotency keys; the fetch is skipped entirely otherwise.
    keyed = [
        (args.actor, args.idempotency_scope, str(args.idempotency_key))
        for args in args_list
        if args.max_pending is not None and args.idempotency_key is not None
    ]
    deduped_counts: dict[str, int] = {}
    if keyed:
        seen_in_batch: set[tuple[str, str]] = set()
        stored_pairs: set[tuple[str, str]] = set()
        found = await conn.fetch(
            sql.enqueue_batch_fetch_existing,
            [scope for _, scope, _ in keyed],
            [key for _, _, key in keyed],
        )
        for rec in found:
            stored_pairs.add((str(rec["idempotency_scope"]), str(rec["idempotency_key"])))
        for actor, scope, key in keyed:
            pair = (scope, key)
            # Stored pair: dedupes to the existing row. First in-batch
            # occurrence of a new pair: writes one row. Repeats: dedupe
            # to the first. Counted per item, not per distinct pair (a
            # set would collapse repeats and under-discount).
            if pair in stored_pairs or pair in seen_in_batch:
                deduped_counts[actor] = deduped_counts.get(actor, 0) + 1
            seen_in_batch.add(pair)
    refusals: list[MaxPendingExceededError] = []
    for actor, (batch_count, _carried) in groups.items():
        cap = effective[actor]
        have = existing.get(actor, 0)
        admitted = batch_count - deduped_counts.get(actor, 0)
        if have + admitted > cap:
            # Why log + metric here (parity with the single path, which
            # does both before raising): a partitioned bulk refusal is a
            # producer-pressure event per refused actor, not per item.
            logger.warning(
                "max-pending-exceeded",
                actor=actor,
                current_count=have,
                max_pending=cap,
            )
            record_backpressure_error(actor, kind="max_pending")
            refusals.append(
                MaxPendingExceededError(
                    actor=actor,
                    current_count=have,
                    max_pending=cap,
                )
            )
    return refusals


class _LegacyIdempotencyKeyConflictError(Exception):
    """Internal marker: the INSERT violated the legacy single-column
    idempotency index (non-arbiter for this release's ON CONFLICT target).

    Two distinct causes, indistinguishable at the point of the violation:

    1. Genuine cross-scope reuse during the rolling-deploy window: the
       (scope, key) pair is new but the bare key exists under a DIFFERENT
       scope. Must surface as ScopedIdempotencyMigrationPendingError.
    2. A same-pair race: a concurrent transaction was inserting the SAME
       (scope, key) pair (e.g. a not-yet-upgraded worker's old-shape
       INSERT, whose own arbiter is the legacy index, or another upgraded
       worker whose speculative insert touched the legacy index first).
       Postgres reports in-flight conflicts against non-arbiter indexes
       unconditionally, so the legacy index can "win" the report even
       though our own composite arbiter would have deduped cleanly.

    Because a unique-violation report means the conflicting transaction
    COMMITTED (had it rolled back, our insert would have proceeded), the
    pool-owning wrappers (_enqueue / _enqueue_batch) retry exactly once on
    a fresh transaction: cause 2 then dedupes via the composite arbiter,
    cause 1 violates the legacy index again and is converted to the public
    typed error. Callers on a borrowed connection (enqueue_with_conn /
    enqueue_batch(connection=...)) cannot retry -- a retry needs a fresh
    transaction and the caller owns this connection's scope -- so they
    convert immediately, preserving this release's documented behavior for
    that path.
    """

    def __init__(
        self,
        *,
        actor: str | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
        detail: str | None = None,
        original: BaseException | None = None,
    ) -> None:
        self.actor = actor
        self.idempotency_key = idempotency_key
        self.idempotency_scope = idempotency_scope
        self.detail = detail
        self.original = original
        super().__init__(detail or "legacy idempotency_key index conflict")

    def to_public(self) -> ScopedIdempotencyMigrationPendingError:
        return ScopedIdempotencyMigrationPendingError(
            actor=self.actor,
            idempotency_key=self.idempotency_key,
            idempotency_scope=self.idempotency_scope,
            detail=self.detail,
        )


async def _acquire_max_pending_lock(
    conn: ConnLike,
    lock_key: str,
    *,
    timeout_ms: float,
    actor: str,
) -> None:
    """Acquire the capped-actor serialization advisory lock with a bounded wait.

    Two-tier via
    :func:`taskq._advisory.acquire_advisory_xact_lock_bounded`: one
    try-lock statement when uncontended (identical happy-path round-trip
    count to the pre-bounded era), a server-side bounded blocking acquire
    inside a savepoint when contended (Postgres' lock scheduler queues
    the waiters and hands off at holder-release rate — MEASURED ~25x the
    contended throughput of a client-side poll loop at 128 same-key
    racers), and a client-side wait_for backstop for the network black
    hole. ``timeout_ms <= 0`` waits indefinitely (the migrate.py
    ``lock_timeout`` convention).

    Raises :class:`MaxPendingLockTimeoutError` when the budget expires —
    the same typed backpressure treatment as a cap rejection, recorded
    against the same ``taskq.backpressure.errors`` counter. A raw driver
    error never surfaces from contention.
    """
    if not await acquire_advisory_xact_lock_bounded(conn, lock_key, timeout_ms=timeout_ms):
        logger.warning(
            "max-pending-lock-timeout",
            actor=actor,
            lock_timeout_ms=timeout_ms,
        )
        record_backpressure_error(actor, kind="max_pending_lock_timeout")
        raise MaxPendingLockTimeoutError(actor=actor, timeout_ms=timeout_ms)


async def _acquire_unique_for_lock(
    conn: ConnLike,
    lock_key: str,
    *,
    timeout_ms: float,
    actor: str,
    identity_key: str,
) -> None:
    """Acquire the unique_for single-flight advisory lock with a bounded wait.

    Two-tier via
    :func:`taskq._advisory.acquire_advisory_xact_lock_bounded` (same
    machinery as the max_pending lock above — the shared helper's
    docstring has the measured rationale).

    Why exhaustion raises :class:`UniqueForLockTimeoutError` and NOT a
    backpressure-flavored error: the contention scope is one logical
    entity's ``(schema, actor, identity_key)``, not an actor's whole
    producer population, and the outcome the wait existed to produce is
    the DEDUP RETURN below (the winner's row handed back to the loser).
    Exhaustion therefore means "the dedup answer could not be determined
    in time" — the caller's correct response is to retry the same
    enqueue, which typically dedupes against the now-visible winner —
    which no BackpressureError handler expresses (those shed load or
    log queue counts). The refusal is still COUNTED on
    ``taskq.backpressure.errors`` under its own bounded kind
    (``unique_for_lock_timeout``), beside the warning log: a typed
    refusal only a log reader can see is invisible at 3am, and the
    ``kind`` label keeps identity contention off the capacity kinds
    (``max_pending`` / ``max_pending_lock_timeout``) so an alert keyed
    on those is not tripped by it. A raw driver error never surfaces
    from contention.
    """
    if not await acquire_advisory_xact_lock_bounded(conn, lock_key, timeout_ms=timeout_ms):
        logger.warning(
            "unique-for-lock-timeout",
            actor=actor,
            identity_key=identity_key,
            lock_timeout_ms=timeout_ms,
        )
        record_backpressure_error(actor, kind="unique_for_lock_timeout")
        raise UniqueForLockTimeoutError(
            actor=actor,
            identity_key=identity_key,
            timeout_ms=timeout_ms,
        )


def _refuse_cross_actor_idempotency_hit(args: EnqueueArgs, existing: JobRow) -> None:
    """Raise when an idempotency hit resolved to another actor's job.

    Uniqueness is ``(idempotency_scope, idempotency_key)``, schema-wide,
    so the arbiter cannot tell a same-actor re-submit from a key shared
    across actors; only the former is a dedup. Returning the other
    actor's row would hand the caller a handle whose result is not its
    job's, indistinguishable from a successful dedup — so the hit is
    refused with the typed error naming both actors and the existing job
    (nothing was inserted: the arbiter skipped the row). Shared by the
    single and batch tiers, mirrored by the in-memory twin and classified
    the same way on the batch-fast COPY tier (whose abort is total, so the
    error is raised in place of the duplicate error, not instead of a
    returned handle).
    """
    if existing.actor != args.actor:
        logger.warning(
            "idempotency-key-actor-mismatch",
            actor=args.actor,
            existing_actor=existing.actor,
            existing_job_id=str(existing.id),
            idempotency_key=str(args.idempotency_key),
            idempotency_scope=args.idempotency_scope,
        )
        raise IdempotencyKeyActorMismatchError(
            actor=args.actor,
            existing_actor=existing.actor,
            existing_job_id=existing.id,
            idempotency_key=str(args.idempotency_key),
            idempotency_scope=args.idempotency_scope,
        )


def _raise_batch_fast_actor_mismatch(
    mismatch: tuple[str, str],
    *,
    idempotency_scope: str | None,
    idempotency_key: str | None,
    existing_job_id: UUID | None,
    batch_size: int,
    cause: BaseException | None = None,
) -> NoReturn:
    """The batch-fast tier's arm of the cross-actor refusal: the same typed
    error and log line as :func:`_refuse_cross_actor_idempotency_hit`,
    raised in place of the duplicate error because the tier's abort is
    total. ``mismatch`` is :func:`duplicate_pair_actor_mismatch`'s
    ``(incoming_actor, existing_actor)``; ``existing_job_id`` is ``None``
    when the holder was an earlier item of the same batch, whose row never
    persisted. Shared with the in-memory mirror so both backends raise the
    identical refusal for the identical batch.
    """
    incoming_actor, existing_actor = mismatch
    logger.warning(
        "idempotency-key-actor-mismatch",
        actor=incoming_actor,
        existing_actor=existing_actor,
        existing_job_id=str(existing_job_id) if existing_job_id is not None else None,
        idempotency_key=idempotency_key,
        idempotency_scope=idempotency_scope,
        batch_size=batch_size,
        detection_path="batch_fast_unique_violation_catch",
    )
    raise IdempotencyKeyActorMismatchError(
        actor=incoming_actor,
        existing_actor=existing_actor,
        existing_job_id=existing_job_id,
        idempotency_key=idempotency_key or "",
        idempotency_scope=idempotency_scope,
    ) from cause


async def _enqueue_on_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
    *,
    max_pending_lock_timeout_ms: float = DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS,
    unique_for_lock_timeout_ms: float = DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS,
    idempotency_lock_timeout_ms: float = DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS,
    owns_transaction: bool = False,
) -> JobRow:
    """Core enqueue logic running on *conn*.

    Includes unique_for preflight, singleton preflight, max_pending
    count, INSERT (savepoint-isolated on the singleton arm and the
    bounded idempotency arm — see the collision catches below), and the
    idempotency-key SELECT on conflict; the wake is the INSERT trigger's,
    not a statement of this function's. Does NOT acquire
    from ``worker_pool`` — the caller supplies the connection. A
    transaction is opened here when the caller-supplied connection
    carries none and the enqueue needs transaction-scoped serialization:
    a capped actor's count-then-insert, and the unique_for
    check-then-insert. A caller who already holds a transaction owns the
    scope — the advisory locks then span that caller's transaction, so
    single-flight and cap exactness hold until its commit/rollback.

    *owns_transaction*: no transaction on *conn* outlives this call — the
    pool path's bare connection, or the transaction this function opened
    itself for the preflight arms. Then a refusal may abort the scope
    outright and a transaction-local GUC needs no restore, so the
    bounded idempotency arm skips the savepoint and the read-then-restore
    of ``lock_timeout`` that only a caller-owned transaction needs. A bare
    connection always qualifies: any scope opened here ends here.
    """
    owns_transaction = owns_transaction or not conn.is_in_transaction()
    unique_for_single_flight = args.unique_for is not None and args.identity_key is not None
    if (args.max_pending is not None or unique_for_single_flight) and not conn.is_in_transaction():
        # Why a transaction here and not just the lock: pg_advisory_xact_lock
        # releases at transaction end, so on a bare caller connection (every
        # statement its own transaction) the lock below would release before
        # the statement it exists to guard. For max_pending that is the
        # count-then-insert race — overlapping counts each see room. For
        # unique_for it is the same defect on the identity preflight: two
        # dispatchers both run the preflight before either commits, both see
        # nothing, and both insert (measured: 100 concurrent enqueues
        # produced 6 rows). The standard pattern for a check-then-insert
        # guarantee is to run the unique insert inside a transaction where a
        # lock holds across both the preflight/count and the INSERT. Wrapping
        # makes the lock span the preflight/count and the INSERT; the
        # recursion terminates because the inner call observes the open
        # transaction. Callers that already hold a transaction are
        # untouched: the lock then spans THEIR transaction instead, so the
        # guarantee holds until their commit.
        async with conn.transaction():
            return await _enqueue_on_conn(
                conn,
                sql,
                schema,
                clock,
                args,
                max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
                unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
                idempotency_lock_timeout_ms=idempotency_lock_timeout_ms,
                owns_transaction=True,
            )
    if args.unique_for is not None and args.identity_key is not None:
        # Why a lock at all: what follows is a check-then-insert. Under READ
        # COMMITTED two dispatchers enqueuing the same (actor, identity_key)
        # both run the preflight before either commits, both see nothing, and
        # both insert — two jobs sharing an identity_key running at once,
        # which is precisely what the feature exists to prevent. Measured: a
        # warm pool and 100 concurrent enqueues produced 6 rows.
        #
        # Why not a partial unique index (the jobs_singleton_uniq shape):
        #   1. identity_key is ALSO the serialization/fairness cohort key, and
        #      actors without unique_for legitimately keep many active jobs
        #      under one identity_key — jobs_identity_active_idx covers exactly
        #      these columns and this predicate and is deliberately NOT unique.
        #      A unique index would reject all of them.
        #   2. unique_for is a WINDOW (created_at > clock_timestamp() - $n).
        #      An index predicate must be IMMUTABLE, so it cannot express the
        #      window and would keep rejecting long after it elapsed.
        #   3. unique_states is per-actor configurable; one index predicate
        #      cannot vary by actor.
        # A lock, unlike an index, serializes exactly the callers that race
        # and leaves the window and state-set semantics to the preflight.
        # An index predicate cannot express a window or adapt per actor, so
        # queues that dedup by permanent index cannot offer windowed dedup;
        # they dedup forever instead.
        #
        # Transaction-scoped, not session-scoped: it releases on COMMIT with
        # no unlock call to leak on an error path, and it is safe under
        # PgBouncer transaction pooling. hashtextextended(name, 0) follows the
        # convention already used for the prune and archive-expiry locks; a
        # collision between two different identity keys costs a little
        # needless serialization and never correctness.
        #
        # Why a BOUNDED wait (the two-tier acquire in
        # _acquire_unique_for_lock, same machinery as max_pending below):
        # the pre-fix blocking acquire queued same-key racers with
        # unbounded tail latency — N racers serialized meant the last
        # waited ~N holder critical sections, and a black-holed holder (a
        # session the server has not yet reaped) pinned every same-key
        # enqueue until TCP keepalives cleared it. The correct outcome of
        # waiting is usually the dedup return just below (the winner's
        # row), and a holder's critical section is one preflight SELECT +
        # one INSERT, so a bounded budget still delivers that outcome for
        # any realistic burst — the contended tier queues server-side and
        # drains at holder-release rate, so the bound only bites on a
        # pathological holder; there the caller gets the typed
        # UniqueForLockTimeoutError with retry-yields-dedup guidance
        # instead of an unbounded block (see that error for why it is
        # deliberately not backpressure-flavored). Lock order is fixed
        # (this first, max_pending second) so no lock cycle can form.
        await _acquire_unique_for_lock(
            conn,
            f"taskq:unique_for:{schema}:{args.actor}:{args.identity_key}",
            timeout_ms=unique_for_lock_timeout_ms,
            actor=args.actor,
            identity_key=str(args.identity_key),
        )
        existing_rec = await conn.fetchrow(
            sql.enqueue_unique_for_preflight,
            args.actor,
            args.identity_key,
            list(args.unique_states),
            args.unique_for,
        )
        if existing_rec is not None:
            row = _job_row_from_record(existing_rec)
            # Same shared helper as the idempotency seam. A window that
            # matches a terminal state can hand a weeks-old row back as a
            # successful enqueue — for ``succeeded`` that is the intended
            # outcome (the work already happened), but the set is
            # caller-configurable (@actor(unique_states=...)) and a
            # failure state folded in strands the new work, which is the
            # case the helper warns on. The full field set
            # (idempotency_scope included) and the terminal-status
            # escalation both come with the helper; the per-site truth is
            # the dedup_reason alone.
            _log_enqueue_dedup(row, dedup_reason="unique_for")
            return row

    singleton_enqueue = args.metadata.get("singleton") is True
    if singleton_enqueue:
        preflight_rec = await conn.fetchrow(sql.singleton_preflight, args.actor)
        if preflight_rec is not None:
            blocking_id: UUID = preflight_rec["id"]
            schedule_to_close: datetime | None = preflight_rec["schedule_to_close"]
            retry_after = None
            if schedule_to_close is not None:
                # Why: advisory hint only — this mixes domains by design (a
                # server-read schedule_to_close minus a Python now) to steer
                # the caller's retry timing; it is never a stored predicate.
                now_utc = clock.now()
                remaining = schedule_to_close - now_utc
                if remaining.total_seconds() > 0:
                    retry_after = remaining
            logger.info(
                "singleton-collision",
                actor=args.actor,
                blocking_job_id=str(blocking_id),
                detection_path="preflight_check",
            )
            raise SingletonCollisionError(
                actor=args.actor,
                blocking_job_id=blocking_id,
                retry_after=retry_after,
            )

    if args.max_pending is not None:
        # Serialize the count-then-insert below per actor: under READ
        # COMMITTED two concurrent enqueues both count before either
        # commits, both see room, and both insert — overshooting a cap the
        # operator set as backpressure. Same transaction-scoped advisory
        # mechanism as unique_for above (safe under PgBouncer transaction
        # pooling; releases on COMMIT with no unlock to leak), taken in a
        # fixed order here (unique_for first, this second) so no lock cycle
        # can form. A hash collision between actors costs needless
        # serialization, never correctness.
        #
        # Why a BOUNDED wait (see _acquire_max_pending_lock for the
        # two-tier choice): every racer on this lock holds it across its
        # own count + INSERT round trips, so an unbounded blocking acquire
        # makes N concurrent producers serialize with the last one
        # waiting ~N transactions — tail latency linear in the burst
        # size, unbounded. Now the wait is capped at
        # *max_pending_lock_timeout_ms* (5 s default) and an exhausted
        # racer gets the same typed backpressure treatment as a cap
        # rejection, while the contended tier still queues server-side
        # (draining at holder-release rate, not at a client poll cadence)
        # so realistic bursts are admitted rather than shed. The cap
        # stays EXACT either way: once acquired, the lock is held across
        # the count and the INSERT exactly as before.
        await _acquire_max_pending_lock(
            conn,
            f"taskq:max_pending:{schema}:{args.actor}",
            timeout_ms=max_pending_lock_timeout_ms,
            actor=args.actor,
        )
        count_rec = await conn.fetchval(
            sql.enqueue_max_pending_count,
            args.actor,
        )
        current_count: int = int(count_rec)
        if current_count >= args.max_pending:
            logger.warning(
                "max-pending-exceeded",
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )
            record_backpressure_error(args.actor, kind="max_pending")
            raise MaxPendingExceededError(
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )

    is_new = False
    # None means immediate — the server stamps scheduled_at (COALESCE) and
    # decides status in the same statement; there is no Python pre-decision.
    scheduled_at_param: datetime | None = args.scheduled_at

    try:
        # Why the singleton INSERT runs inside a savepoint, and why the
        # bounded idempotency arm joins it: a savepoint-isolated arm is
        # for INSERTs whose DOCUMENTED failure mode is a typed
        # catch-and-continue refusal — the singleton's
        # SingletonCollisionError on jobs_singleton_uniq, and (below) the
        # idempotency token's IdempotencyKeyLockTimeoutError on a bounded
        # speculative-lock wait. The raw error is a STATEMENT error either
        # way (UniqueViolationError aborts the surrounding transaction;
        # 55P03 LockNotAvailableError does the same), and the typed
        # conversion does not undo that, so the refusal would leave the
        # caller's transaction dead on this detection path but alive on
        # the preflight paths. The savepoint rollback restores the scope
        # before the conversion raises. Every other enqueue keeps the bare
        # INSERT: their violation outcomes are raw or migration-window
        # errors, not refusals a caller catches and continues from.
        idempotency_bounded_wait = (
            args.idempotency_key is not None and idempotency_lock_timeout_ms > 0
        )
        # The savepoint and the read-then-restore exist for a caller-owned
        # transaction, which must survive a refusal and keep its own
        # lock_timeout. On a scope this call owns a refusal aborts the
        # scope outright and the transaction-local bound ends with it, so
        # neither is paid; the one scope an owned path still opens is a
        # real short transaction on a bare connection, so that the bounded
        # arm's SET LOCAL spans its INSERT.
        restore_lock_timeout = idempotency_bounded_wait and not owns_transaction
        if owns_transaction:
            scope_needed = idempotency_bounded_wait and not conn.is_in_transaction()
        else:
            scope_needed = singleton_enqueue or idempotency_bounded_wait
        async with _optional_savepoint(conn, enabled=scope_needed):
            # Bound upfront (not only in the branch) so the restore below
            # is provably bound on every path it runs.
            prior_lock_timeout: str | None = None
            if restore_lock_timeout:
                prior_lock_timeout = await conn.fetchval(_LOCK_TIMEOUT_READ_SQL)
            if idempotency_bounded_wait:
                # Bounded speculative-token wait — the RED contract of
                # tests/test_rt_locks_actor_tx_enqueue_serialization.py:
                # the ON CONFLICT (idempotency_scope, idempotency_key) DO
                # NOTHING arbiter blocks on another transaction's
                # UNCOMMITTED same-pair row (Postgres must wait for its
                # uniqueness verdict), and on a transactional consumer
                # the holder is the actor's own OPEN transaction —
                # unbounded by default (default_start_to_close=None) —
                # so the bare INSERT serialized every other producer of
                # that pair for the actor's whole runtime. The GUC
                # discipline is taskq._advisory's, not the limiter's SET
                # LOCAL: this transaction's caller (the actor) keeps
                # using it after the INSERT, so the prior lock_timeout is
                # read, set for exactly this savepoint's span, and
                # restored BEFORE the savepoint's RELEASE — SET LOCAL
                # persists through RELEASE, so skipping the restore
                # would leak the wait bound onto every later statement
                # of the caller's transaction (and clobber a
                # caller-set bound). On the timeout paths no restore is
                # needed: the savepoint ROLLBACK below undoes the set.
                # No client-side backstop: the pool conns carry
                # command_timeout, which owns the network-black-hole
                # regime; the GUC owns the lock-wait regime.
                await conn.execute(_LOCK_TIMEOUT_SET_SQL, f"{round(idempotency_lock_timeout_ms)}ms")
            rec = await conn.fetchrow(
                sql.enqueue,
                args.id,
                args.actor,
                args.queue,
                args.identity_key,
                args.fairness_key,
                jsonb_param(args.payload),
                args.payload_schema_ver,
                args.priority,
                args.max_attempts,
                args.retry_kind,
                args.schedule_to_close_interval,
                args.start_to_close,
                args.heartbeat_timeout,
                scheduled_at_param,
                args.idempotency_scope,
                args.idempotency_key,
                args.trace_id,
                args.span_id,
                jsonb_param(args.metadata),
                args.result_ttl,
                list(args.tags),
                args.schedule_to_close,
                args.retry_base.total_seconds(),
                args.retry_cap.total_seconds(),
                args.retry_backoff,
                args.retry_jitter,
            )
            if restore_lock_timeout:
                # Restore before the savepoint's RELEASE — see above. The
                # None arm is unreachable (the read ran under the same
                # flag); str() tolerates it for the type checker anyway.
                await conn.execute(_LOCK_TIMEOUT_SET_SQL, str(prior_lock_timeout))
    except LockNotAvailableError as exc:
        # The speculative-token wait outlived its budget: the dedup
        # answer could not be determined in time. The savepoint above has
        # already rolled back (restoring the caller's transaction to a
        # usable state and undoing the GUC), nothing was inserted, and
        # the typed refusal carries the retry-yields-dedup guidance —
        # UniqueForLockTimeoutError's treatment for its identical
        # situation. Counted on ``taskq.backpressure.errors`` under its
        # own bounded kind (``idempotency_lock_timeout``), beside the
        # warning log: same asymmetry fix as the unique_for arm, and the
        # kind label keeps one pair's contention off the capacity kinds.
        logger.warning(
            "idempotency-lock-timeout",
            actor=args.actor,
            idempotency_key=str(args.idempotency_key),
            idempotency_scope=args.idempotency_scope,
            lock_timeout_ms=idempotency_lock_timeout_ms,
        )
        record_backpressure_error(args.actor, kind="idempotency_lock_timeout")
        raise IdempotencyKeyLockTimeoutError(
            actor=args.actor,
            idempotency_key=str(args.idempotency_key),
            timeout_ms=idempotency_lock_timeout_ms,
            idempotency_scope=args.idempotency_scope,
        ) from exc
    except UniqueViolationError as exc:
        if exc.constraint_name == _SINGLETON_CONSTRAINT_NAME:
            logger.info(
                "singleton-collision",
                actor=args.actor,
                blocking_job_id=None,
                detection_path="unique_violation_catch",
            )
            raise SingletonCollisionError(
                actor=args.actor,
                blocking_job_id=None,
                retry_after=None,
            ) from exc
        if exc.constraint_name == _LEGACY_IDEMPOTENCY_KEY_CONSTRAINT_NAME:
            # Rolling-deploy overlap window: the old single-column index
            # still exists alongside the new composite one (see
            # 01.00.03_01_pre_idempotency_scope.sql). Raised either by a
            # genuine cross-scope reuse or by a same-pair race against a
            # concurrent old-shape INSERT -- see
            # _LegacyIdempotencyKeyConflictError for how the pool-owning
            # wrapper distinguishes the two. Surfaced explicitly rather
            # than silently resolved against another scope's row; see
            # ScopedIdempotencyMigrationPendingError's docstring for why.
            logger.info(
                "scoped-idempotency-legacy-index-conflict",
                actor=args.actor,
                idempotency_key=args.idempotency_key,
                idempotency_scope=args.idempotency_scope,
            )
            raise _LegacyIdempotencyKeyConflictError(
                actor=args.actor,
                idempotency_key=str(args.idempotency_key),
                idempotency_scope=args.idempotency_scope,
                original=exc,
            ) from exc
        raise
    if rec is not None:
        is_new = True
    else:
        rec = await conn.fetchrow(
            sql.enqueue_select_by_key,
            args.idempotency_scope,
            args.idempotency_key,
        )
        if rec is None:
            raise RuntimeError(
                "enqueue ON CONFLICT fired but follow-up SELECT "
                f"found no row for idempotency_scope={args.idempotency_scope!r} "
                f"idempotency_key={args.idempotency_key!r}"
            )

    row = _job_row_from_record(rec)
    if not is_new:
        _refuse_cross_actor_idempotency_hit(args, row)

    # No app-side pg_notify: the jobs INSERT trigger (tr_notify_job_insert)
    # is the sole wake source for every insert path, gated on the row
    # landing as 'pending' — which the INSERT decides server-side, so a
    # future-dated row wakes nobody. An app-side statement was the same
    # (channel, payload) pair the trigger emits: coalesced with it inside
    # a transaction, a second delivery to every listener outside one.
    if is_new:
        logger.info(
            "enqueue",
            kind="enqueue",
            job_id=str(row.id),
            actor=row.actor,
            queue=row.queue,
            idempotency_key=row.idempotency_key,
        )
    else:
        _log_enqueue_dedup(row, dedup_reason="idempotency_key")

    return row


async def _enqueue_with_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
    *,
    max_pending_lock_timeout_ms: float = DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS,
    unique_for_lock_timeout_ms: float = DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS,
    idempotency_lock_timeout_ms: float = DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS,
) -> JobRow:
    try:
        return await _enqueue_on_conn(
            conn,
            sql,
            schema,
            clock,
            args,
            max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
            unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
            idempotency_lock_timeout_ms=idempotency_lock_timeout_ms,
        )
    except _LegacyIdempotencyKeyConflictError as exc:
        # Caller owns the transaction scope -- a retry needs a fresh one,
        # which this wrapper cannot open on the caller's connection.
        logger.warning(
            "scoped-idempotency-migration-pending",
            actor=exc.actor,
            idempotency_key=exc.idempotency_key,
            idempotency_scope=exc.idempotency_scope,
        )
        raise exc.to_public() from exc.original or exc


async def _enqueue(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
    *,
    max_pending_lock_timeout_ms: float = DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS,
    unique_for_lock_timeout_ms: float = DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS,
    idempotency_lock_timeout_ms: float = DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS,
) -> JobRow:
    # No transaction of its own: the plain arm is one atomic INSERT, and
    # _enqueue_on_conn opens a scope exactly where one is needed — the
    # capped / single-flight preflights, the singleton savepoint, the
    # bounded idempotency wait — so a wrapper here only added BEGIN and
    # COMMIT round trips to every enqueue.
    async def _attempt() -> JobRow:
        try:
            async with pool.acquire() as conn:
                return await _enqueue_on_conn(
                    conn,
                    sql,
                    schema,
                    clock,
                    args,
                    max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
                    unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
                    idempotency_lock_timeout_ms=idempotency_lock_timeout_ms,
                )
        except _LegacyIdempotencyKeyConflictError as exc:
            public = exc.to_public()

        # One retry on a fresh statement. If the violation was a same-pair
        # race, the conflicting row is now committed (a unique-violation report
        # means the other transaction committed) and the composite arbiter
        # dedupes cleanly below. If it was genuine cross-scope reuse, the
        # legacy index violates again and the public typed error is raised.
        try:
            async with pool.acquire() as conn:
                return await _enqueue_on_conn(
                    conn,
                    sql,
                    schema,
                    clock,
                    args,
                    max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
                    unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
                    idempotency_lock_timeout_ms=idempotency_lock_timeout_ms,
                )
        except _LegacyIdempotencyKeyConflictError as exc:
            logger.warning(
                "scoped-idempotency-migration-pending",
                actor=public.actor,
                idempotency_key=public.idempotency_key,
                idempotency_scope=public.idempotency_scope,
            )
            raise public from exc.original or exc

    return await _with_fresh_connection_retry(_attempt, operation="enqueue")


def _membership_batch_ids(args_list: list[EnqueueArgs]) -> list[UUID]:
    """Distinct batch ids whose membership this enqueue writes.

    ``metadata.batch_id`` is the library-injected membership stamp (the
    client surface's ``build_batch_args`` / streaming stamping — callers
    must not set it themselves); a member INSERT is a batch-membership
    write even though it touches only the jobs table, which is exactly
    why the completion race needs the explicit lock below. A junk value
    raises here, at the enqueue boundary, rather than surviving to
    explode the terminal hook's own ``UUID(str(...))`` parse
    (batch.py's apply_batch_terminal_outcome) after the row is already
    committed.
    """
    ids: dict[str, UUID] = {}
    for args in args_list:
        raw = args.metadata.get("batch_id")
        if raw is None:
            continue
        ids[str(raw)] = UUID(str(raw))
    return list(ids.values())


async def _lock_batch_membership(conn: ConnLike, schema: str, batch_ids: list[UUID]) -> None:
    """Hold the batches-row membership lock for *batch_ids* on *conn*.

    The lock is the append side of the complete-vs-append race: a member
    append transaction holds the batches row FOR UPDATE from before its
    member INSERTs until its commit, and ``complete_batch``'s membership
    CTE takes the same row FOR UPDATE NOWAIT — the conflict is the only
    thing that can make a READ COMMITTED completion guard aware of an
    uncommitted member INSERT it cannot see, so the completer delays (see
    ``_COMPLETE_BATCH_SQL``'s comment in _batch_sql.py). Rows that do not
    exist yet (the atomic path inserts members before create_batch, and
    the bulk-import paths never create one) lock nothing — a batch that
    does not exist cannot be completed, so there is no race to close.

    Blocking (no NOWAIT) is deliberate on this side: appenders serialize
    per batch, each hold bounded by its own short chunk transaction, and
    the deadlock detector covers the one exotic inversion (an appender
    waiting on a member's idempotency arbiter while that member's hook
    waits on this lock) by cancelling one side. The statement is
    function-local rather than a module-level constant because the
    bounded-writes audit's own scope rule keeps keyed,
    bounded-by-construction SQL at its call site: the predicate names an
    ANY-array of this chunk's own batch ids, so the row count cannot grow
    with the jobs backlog.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    lock_sql = f'SELECT id FROM "{schema}".batches WHERE id = ANY($1::uuid[]) FOR UPDATE'
    await conn.execute(lock_sql, batch_ids)


async def _enqueue_batch(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    args_list: list[EnqueueArgs],
    *,
    connection: "ConnLike | None" = None,
    enforce_max_pending: bool = True,
    refuse_whole_batch_on_cap: bool = False,
    index_base: int = 0,
) -> list[JobRow]:
    """Insert a batch, partitioning cap admission per actor.

    ``refuse_whole_batch_on_cap`` keeps the legacy all-or-nothing refusal
    for the :func:`enqueue_batch_atomic` chunk arm (its single shared
    transaction must stay atomic — a partial admission would betray the
    atomic contract); every other caller gets the partition: over-cap
    actors' items are refused as a group, all other actors' items are
    inserted, and :class:`BatchMaxPendingExceededError` raises at the
    transaction boundary — AFTER the admitted items commit when this
    call owns the transaction, immediately after the insert on a
    caller-owned open transaction (that transaction's commit/rollback
    decides their durability).

    ``index_base`` is the position of ``args_list[0]`` in the CALLER's
    coordinate space. The default ``0`` is every existing caller (the
    whole list IS the call); the atomic chunk arm passes the consumed
    prefix so a chunk's per-item annotations (the jsonb NUL guard below)
    name STREAM-GLOBAL indices — a chunk-local index from inside the
    backend is unfixable at the client layer, which cannot know the
    backend's chunk base. The cap partition's ``refused_indices`` stay
    per-call deliberately: the only ``index_base != 0`` caller also sets
    ``refuse_whole_batch_on_cap`` and surfaces the index-free
    :class:`MaxPendingExceededError`, so there is no partition to shift.
    """
    if not args_list:
        raise ValueError("args_list must not be empty")

    ids: list[UUID] = []
    actors: list[str] = []
    queues: list[str] = []
    identity_keys: list[str | None] = []
    fairness_keys: list[str | None] = []
    payloads: list[str] = []
    payload_schema_vers: list[int] = []
    priorities: list[int] = []
    max_attempts_list: list[int] = []
    retry_kinds: list[str] = []
    stc_intervals: list[timedelta | None] = []
    stc_raws: list[datetime | None] = []
    start_to_closes: list[object] = []
    heartbeat_timeouts: list[object] = []
    scheduled_ats: list[datetime | None] = []
    metadatas: list[str] = []
    idempotency_keys: list[str | None] = []
    idempotency_scopes: list[str] = []
    trace_ids: list[str | None] = []
    span_ids: list[str | None] = []
    result_ttls: list[timedelta | None] = []
    tag_jsons: list[str] = []
    retry_bases: list[float] = []
    retry_caps: list[float] = []
    retry_backoffs: list[str] = []
    retry_jitters: list[float] = []

    # Why annotate per item during the build: this loop serializes every
    # item BEFORE any SQL runs, so the first NUL-bearing item aborts the
    # whole batch with nothing written. item_jsonb_param /
    # item_tags_jsonb_param attach the per-item annotation (index, actor,
    # field) at that raise — the same contract the client layer's
    # _item_payload_error gives pydantic failures — instead of the bare
    # ValueError(NUL_JSONB_ERROR) that named nothing. Admission semantics
    # ride the partition below: a defective item rejects the whole call
    # (all-or-nothing, caller-space index) before the cap check or INSERT
    # ever runs. The index is index_base + the loop position so a chunk
    # of a larger stream annotates at the STREAM-GLOBAL position; the
    # annotation (message and PayloadValidationError.item_index) is the
    # one coordinate the caller can act on, and the caller cannot
    # reconstruct the chunk base from outside the backend.
    for idx, args in enumerate(args_list):
        ids.append(args.id)
        actors.append(args.actor)
        queues.append(args.queue)
        identity_keys.append(str(args.identity_key) if args.identity_key is not None else None)
        fairness_keys.append(args.fairness_key)
        payloads.append(
            item_jsonb_param(args.payload, idx=index_base + idx, field="payload", actor=args.actor)
        )
        payload_schema_vers.append(args.payload_schema_ver)
        priorities.append(args.priority)
        max_attempts_list.append(args.max_attempts)
        retry_kinds.append(args.retry_kind)
        # schedule_to_close and result_expires_at are resolved server-side
        # (COALESCE(clock_timestamp() + stc_interval, stc_raw) and
        # clock_timestamp() + result_ttl in enqueue_batch) — never in Python.
        stc_intervals.append(args.schedule_to_close_interval)
        stc_raws.append(args.schedule_to_close)
        start_to_closes.append(args.start_to_close)
        heartbeat_timeouts.append(args.heartbeat_timeout)
        # None means immediate — the server stamps/decides (COALESCE in
        # enqueue_batch); there is no Python pre-decision.
        scheduled_ats.append(args.scheduled_at)
        metadatas.append(
            item_jsonb_param(
                args.metadata, idx=index_base + idx, field="metadata", actor=args.actor
            )
        )
        idempotency_keys.append(
            str(args.idempotency_key) if args.idempotency_key is not None else None
        )
        idempotency_scopes.append(args.idempotency_scope)
        trace_ids.append(args.trace_id)
        span_ids.append(args.span_id)
        # result_expires_at is resolved server-side (clock_timestamp() +
        # result_ttl in enqueue_batch) — never in Python.
        result_ttls.append(args.result_ttl)
        # tag_jsons transits the wire as $21::jsonb[] (see enqueue_batch's
        # comment on jagged-array handling) — each element is parsed by
        # Postgres' jsonb_in before jsonb_array_elements_text unpacks it
        # into the text[] `tags` column, so a NUL here hits the same
        # jsonb_in rejection as any other jsonb write; the item-annotated
        # dumps_jsonb_str wrapper guards it before the value ever reaches
        # Postgres.
        tag_jsons.append(item_tags_jsonb_param(args.tags, idx=index_base + idx, actor=args.actor))
        retry_bases.append(args.retry_base.total_seconds())
        retry_caps.append(args.retry_cap.total_seconds())
        retry_backoffs.append(args.retry_backoff)
        retry_jitters.append(args.retry_jitter)

    async def _insert_on_conn(
        conn: ConnLike,
        *,
        owns_transaction: bool,
    ) -> tuple[list[JobRow], list[MaxPendingExceededError], dict[str, list[int]]]:
        # owns_transaction: the transaction on *conn* (if any) is this
        # call's own and ends with it — a refusal may abort it outright.
        # False on a caller-supplied connection, whose transaction (or
        # autocommit state) outlives the call.
        #
        # Never raises the partition refusal itself: the caller raises at
        # the transaction boundary so the admitted items commit first
        # (raising inside would roll them back and re-create the
        # all-or-nothing behavior the partition exists to remove).
        # The membership lock runs FIRST, before the cap preflight and
        # the INSERT, so the whole arbitrate-then-insert span is covered
        # by one hold of the batches-row lock (see
        # _lock_batch_membership). Every caller of this closure runs
        # inside a transaction that also owns the INSERT: the caller's
        # own, the wrapper below, or the pool path's — a bare autocommit
        # connection only reaches here when the chunk carries no batch
        # membership, in which case there is no lock to hold.
        membership_ids = _membership_batch_ids(args_list)
        if membership_ids:
            await _lock_batch_membership(conn, schema, membership_ids)
        refusals: list[MaxPendingExceededError] = []
        refused_indices: dict[str, list[int]] = {}
        refused_names: set[str] = set()
        admitted_args = args_list
        if enforce_max_pending:
            refusals = await _batch_cap_refusals(conn, sql, args_list)
            if refusals:
                if refuse_whole_batch_on_cap:
                    # The atomic chunk arm: one shared transaction owns
                    # every chunk, so partial admission would betray its
                    # all-or-nothing contract. Refuse the WHOLE call at
                    # admission time — nothing is inserted here, and the
                    # atomic wrapper's rollback discards earlier chunks
                    # too. refusals[0] preserves the legacy raise (the
                    # first violating actor in group order).
                    raise refusals[0]
                # Partition: an over-cap actor's items are refused as a
                # whole group, never partially filled up to the cap (the
                # single path refuses a capped enqueue outright; a
                # partial fill would admit an arbitrary prefix of the
                # caller's items that the caller never chose).
                refused_indices = {
                    r.actor: [i for i, a in enumerate(args_list) if a.actor == r.actor]
                    for r in refusals
                }
                refused_names = {r.actor for r in refusals}
                admitted_args = [a for a in args_list if a.actor not in refused_names]
        if not admitted_args:
            # Every item refused: nothing reaches the INSERT. The typed
            # error still raises at the boundary, so caller-visible state
            # is exactly "nothing admitted".
            return [], refusals, refused_indices

        # Why filter the pre-built arrays instead of re-serializing the
        # admitted subset: the annotated build loop above already
        # serialized every item BEFORE any SQL (a NUL-bearing item rejects
        # the whole call with nothing written and the pool never touched —
        # the pinned NUL -> cap -> insert order), so the partition selects
        # positions from those arrays rather than paying a second
        # serialization pass over the admitted subset. Happy path (no
        # refusals): the filter is skipped entirely and the arrays alias
        # through unchanged — the partition costs the common case nothing.
        # Order matches sql.enqueue_batch's binding order exactly (scopes
        # before keys, stc_raws last, the retry-curve scalars appended
        # after it).
        insert_cols: list[list[Any]] = [
            ids,
            actors,
            queues,
            identity_keys,
            fairness_keys,
            payloads,
            payload_schema_vers,
            priorities,
            max_attempts_list,
            retry_kinds,
            stc_intervals,
            start_to_closes,
            heartbeat_timeouts,
            scheduled_ats,
            metadatas,
            idempotency_scopes,
            idempotency_keys,
            trace_ids,
            span_ids,
            result_ttls,
            tag_jsons,
            stc_raws,
            retry_bases,
            retry_caps,
            retry_backoffs,
            retry_jitters,
        ]
        if refusals:
            keep = [i for i, a in enumerate(args_list) if a.actor not in refused_names]
            insert_cols = [[col[i] for i in keep] for col in insert_cols]

        async def _assemble(returning_recs: list[Any]) -> list[JobRow]:
            """Resolve the batch's rows from the INSERT's RETURNING set and the
            existing rows its idempotency hits resolved to. Runs INSIDE the
            savepoint scope so a cross-actor refusal rolls the INSERT back with
            it — nothing from the batch is admitted, as for a singleton
            collision."""
            # The INSERT returns the full rows (RETURNING *), so the new
            # rows need no re-read by id. asyncpg's uuid codec already
            # returns stdlib uuid.UUID, so the ids key directly.
            new_recs_by_id: dict[UUID, object] = {rec["id"]: rec for rec in returning_recs}

            collision_pairs: list[tuple[str, str]] = []
            for args in admitted_args:
                if args.idempotency_key is not None and args.id not in new_recs_by_id:
                    collision_pairs.append((args.idempotency_scope, str(args.idempotency_key)))

            existing_by_idem: dict[tuple[str, str], object] = {}
            if collision_pairs:
                collision_scopes = [p[0] for p in collision_pairs]
                collision_keys = [p[1] for p in collision_pairs]
                recs = await conn.fetch(
                    sql.enqueue_batch_fetch_existing,
                    collision_scopes,
                    collision_keys,
                )
                for rec in recs:
                    pair = (rec["idempotency_scope"], str(rec["idempotency_key"]))
                    existing_by_idem[pair] = rec

            # The per-call dedup WARNING budget: the loop below is the
            # only site this closure logs dedup hits, and exactly one of the
            # caller arms' assemblies runs to completion per call (a legacy
            # retry re-raises at the INSERT before reaching here), so a budget
            # scoped to the assembly IS the call's budget. Reset in the
            # finally so no leak escapes the closure into the caller's
            # context; the summary rides the same finally so the failure path
            # (a RuntimeError mid-assembly) still reports suppressed hits.
            dedup_budget = _DedupWarnBudget()
            dedup_budget_token = _dedup_warn_budget.set(dedup_budget)
            try:
                result: list[JobRow] = []
                for args in admitted_args:
                    new_rec = new_recs_by_id.get(args.id)
                    if new_rec is not None:
                        result.append(_job_row_from_record(new_rec))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
                    elif (
                        args.idempotency_key is not None
                        and (args.idempotency_scope, str(args.idempotency_key)) in existing_by_idem
                    ):
                        rec = existing_by_idem[(args.idempotency_scope, str(args.idempotency_key))]
                        row = _job_row_from_record(rec)  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
                        _refuse_cross_actor_idempotency_hit(args, row)
                        _log_enqueue_dedup(row, dedup_reason="idempotency_key")
                        result.append(row)
                    else:
                        raise RuntimeError(
                            f"enqueue_batch: no row found for args.id={args.id!r} "
                            f"after INSERT; this is a bug"
                        )
            finally:
                _dedup_warn_budget.reset(dedup_budget_token)
                _log_enqueue_dedup_warn_summary(dedup_budget, dedup_reason="idempotency_key")
            return result

        try:
            # Why a savepoint around the INSERT when the batch carries
            # singleton items: a jobs_singleton_uniq violation is a
            # STATEMENT error that poisons the surrounding transaction,
            # and the typed conversion below attributes the colliding
            # actor with a post-abort SELECT that must run inside the
            # caller's scope — the savepoint's rollback restores that
            # scope before the lookup (the same discipline the
            # single-enqueue path's savepoint-isolated singleton arm and
            # the COPY path's keyed-batch wrapper follow). The happy path
            # pays one SAVEPOINT/RELEASE pair per singleton-carrying
            # batch; batches without singleton items cannot violate the
            # partial index (its predicate is metadata @>
            # '{"singleton": true}') and skip the wrapper entirely.
            #
            # A keyed batch on a transaction this call does not own joins
            # the scope for the cross-actor idempotency refusal
            # (_refuse_cross_actor_idempotency_hit, raised from the
            # assembly below): the INSERT has succeeded by then, so only
            # a scope of our own can withdraw the admitted rows — a
            # savepoint inside the caller's transaction, a real
            # transaction on a bare caller connection. In a transaction
            # this call owns the refusal's propagation rolls everything
            # back already, so the pool path pays nothing extra.
            singleton_batch = any(args.metadata.get("singleton") is True for args in admitted_args)
            keyed_batch = any(args.idempotency_key is not None for args in admitted_args)
            async with _optional_savepoint(
                conn, enabled=singleton_batch or (keyed_batch and not owns_transaction)
            ):
                returning_recs = await conn.fetch(
                    sql.enqueue_batch,
                    *insert_cols,
                )
                result = await _assemble(returning_recs)
        except UniqueViolationError as exc:
            if exc.constraint_name == _LEGACY_IDEMPOTENCY_KEY_CONSTRAINT_NAME:
                # Rolling-deploy overlap window (see
                # _enqueue_on_conn's matching except-branch and
                # _LegacyIdempotencyKeyConflictError). Unlike the
                # single-enqueue path, this INSERT is one statement
                # covering the whole admitted batch: a single cross-scope
                # collision against the legacy index aborts the ENTIRE
                # batch, not just the offending item -- Postgres gives us
                # no cheaper way to identify which item(s) caused it
                # without re-inserting one row at a time, which isn't
                # warranted for a purely transitional migration-window
                # condition.
                logger.info(
                    "scoped-idempotency-legacy-index-conflict-batch",
                    batch_size=len(admitted_args),
                )
                raise _LegacyIdempotencyKeyConflictError(detail=str(exc), original=exc) from exc
            if exc.constraint_name == _SINGLETON_CONSTRAINT_NAME:
                # Same typed refusal the single-enqueue path's Layer-2
                # catch raises (blocking_job_id/retry_after stay None:
                # this is a violation catch, not a preflight — no
                # blocking row was fetched on the way in). The actor is
                # resolved exactly from the batch's own contents plus the
                # post-abort lookup, never parsed from the driver's
                # detail text. The savepoint above has already rolled the
                # INSERT back, so the whole-call atomicity is unchanged:
                # nothing from the batch is admitted, and a caller-owned
                # transaction survives the refusal usable.
                actor = await _attribute_singleton_collision(conn, sql, admitted_args, exc.detail)
                logger.info(
                    "singleton-collision",
                    actor=actor,
                    blocking_job_id=None,
                    detection_path="unique_violation_catch",
                    batch_size=len(admitted_args),
                )
                raise SingletonCollisionError(
                    actor=actor,
                    blocking_job_id=None,
                    retry_after=None,
                ) from exc
            raise

        return result, refusals, refused_indices

    if (
        connection is not None
        and not connection.is_in_transaction()
        and (
            (enforce_max_pending and batch_cap_groups(args_list))
            # A membership chunk on a bare caller connection needs the
            # wrapper's transaction for the same reason the cap preflight
            # does: the batches-row lock is only a hold against the
            # completion guard if it lives until the INSERT's commit — a
            # lock taken and released in autocommit before the INSERT
            # closes nothing.
            or _membership_batch_ids(args_list)
        )
    ):
        # Same race as capped singles: the admission count and the INSERT
        # must share one transaction or a concurrent writer slips between
        # them. Pool-acquired connections already wrap below; a
        # caller-supplied connection without an open transaction gets the
        # same treatment here (pool-path parity). The inner call returns
        # its refusals instead of raising, the wrapper commits the
        # admitted items, and the typed error raises only AFTER that
        # commit — raising inside would roll the admitted items back and
        # re-create the all-or-nothing refusal the partition removed.
        # Uncapped batches skip the cap half; membership-only chunks wrap
        # for the lock above.
        async with connection.transaction():
            rows, refusals, refused_indices = await _insert_on_conn(
                connection, owns_transaction=True
            )
        if refusals:
            raise BatchMaxPendingExceededError(
                refusals=refusals,
                refused_indices=refused_indices,
                admitted_count=len(rows),
            )
        return rows

    if connection is not None:
        try:
            rows, refusals, refused_indices = await _insert_on_conn(
                connection, owns_transaction=False
            )
        except _LegacyIdempotencyKeyConflictError as exc:
            # Caller owns the transaction scope -- a retry needs a fresh
            # one, which this wrapper cannot open on the caller's connection.
            logger.warning("scoped-idempotency-migration-pending-batch")
            raise exc.to_public() from exc.original or exc
        # Why raise without committing: the caller owns this open
        # transaction; whether the admitted items persist is that
        # transaction's commit/rollback decision (documented on
        # BatchMaxPendingExceededError).
        if refusals:
            raise BatchMaxPendingExceededError(
                refusals=refusals,
                refused_indices=refused_indices,
                admitted_count=len(rows),
            )
        return rows

    async def _attempt_pool() -> tuple[
        list[JobRow], list[MaxPendingExceededError], dict[str, list[int]]
    ]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await _insert_on_conn(conn, owns_transaction=True)

    async def _attempt_pool_with_legacy_retry() -> tuple[
        list[JobRow], list[MaxPendingExceededError], dict[str, list[int]]
    ]:
        try:
            return await _attempt_pool()
        except _LegacyIdempotencyKeyConflictError as exc:
            public = exc.to_public()
            # One retry on a fresh transaction (see _enqueue for the rationale).
            # The first attempt's statement failure aborted its transaction, so
            # nothing from it persisted and the whole batch re-executes cleanly;
            # same-pair-raced items now dedupe via the composite arbiter and the
            # follow-up fetch, while genuine cross-scope reuse violates the legacy
            # index again and surfaces as the public typed error.
            try:
                return await _attempt_pool()
            except _LegacyIdempotencyKeyConflictError as exc:
                logger.warning("scoped-idempotency-migration-pending-batch")
                raise public from exc.original or exc

    rows, refusals, refused_indices = await _with_fresh_connection_retry(
        _attempt_pool_with_legacy_retry, operation="enqueue_batch"
    )
    # The pool transaction has committed: raise the partition refusal
    # only after the admitted items are durable. A blind whole-batch
    # retry from here would duplicate them — the typed error carries the
    # refused indices precisely so callers retry only those.
    if refusals:
        raise BatchMaxPendingExceededError(
            refusals=refusals,
            refused_indices=refused_indices,
            admitted_count=len(rows),
        )
    return rows


async def _enqueue_batch_fast(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    args_list: list[EnqueueArgs],
    *,
    connection: "ConnLike | None" = None,
    enforce_max_pending: bool = True,
    index_base: int = 0,
) -> int:
    """COPY a batch, partitioning cap admission per actor (see
    :func:`_enqueue_batch`). Idempotency violations remain all-or-nothing
    — COPY has no ON CONFLICT arbiter, so a duplicate key aborts the
    entire statement; only cap admission partitions.

    ``index_base`` is the caller-coordinate shift for the per-item NUL
    annotations (see :func:`_enqueue_batch`); every current caller passes
    the whole list, so the default ``0`` is the live behavior — the
    parameter exists so a future chunked COPY caller gets stream-global
    indices by construction instead of re-growing the local-index defect.
    """
    if not args_list:
        raise ValueError("args_list must not be empty")

    ids: list[UUID] = []
    scheduled_ats: list[datetime | None] = []
    stc_intervals: list[timedelta | None] = []
    stc_raws: list[datetime | None] = []
    result_ttls: list[timedelta | None] = []

    # COPY can only write literal values, so it writes the
    # domain-insensitive columns (sql.copy_enqueue_columns) and the fixup
    # UPDATE below stamps status/scheduled_at/schedule_to_close/
    # result_expires_at from the server clock inside the same transaction —
    # never from this process's Python clock.
    # Same per-item annotation as _enqueue_batch's build loop (including
    # the index_base shift): the COPY record tuples are serialized here,
    # before any statement is issued, so a NUL-bearing item rejects the
    # whole batch (nothing written) with the item index, actor, and field
    # named. Tags bind as text[] (no jsonb hop on this path) and stay
    # guarded by the EnqueueArgs construction chokepoint alone.
    records: list[tuple[object, ...]] = []
    for idx, args in enumerate(args_list):
        ids.append(args.id)
        scheduled_ats.append(args.scheduled_at)
        stc_intervals.append(args.schedule_to_close_interval)
        stc_raws.append(args.schedule_to_close)
        result_ttls.append(args.result_ttl)

        records.append(
            (
                args.id,
                args.actor,
                args.queue,
                str(args.identity_key) if args.identity_key is not None else None,
                args.fairness_key,
                item_jsonb_param(
                    args.payload, idx=index_base + idx, field="payload", actor=args.actor
                ),
                args.payload_schema_ver,
                args.priority,
                0,
                args.max_attempts,
                args.retry_kind,
                args.start_to_close,
                args.heartbeat_timeout,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                None,
                None,
                None,
                "{}",
                0,
                None,
                None,
                args.idempotency_scope,
                str(args.idempotency_key) if args.idempotency_key is not None else None,
                args.trace_id,
                args.span_id,
                item_jsonb_param(
                    args.metadata, idx=index_base + idx, field="metadata", actor=args.actor
                ),
                list(args.tags),
                args.retry_base.total_seconds(),
                args.retry_cap.total_seconds(),
                args.retry_backoff,
                args.retry_jitter,
                # Producer placement: an enqueue IS the placement the row's
                # own queue label records, so dispatch routes it by that
                # label rather than by the actor's stored assignment (the
                # routing contract in taskq/backend/_dispatch_sql.py).
                False,
            )
        )

    async def _copy_on_conn(
        conn: ConnLike,
    ) -> tuple[int, list[MaxPendingExceededError], dict[str, list[int]]]:
        # Never raises the partition refusal itself: the caller raises at
        # the transaction boundary so the admitted rows commit first
        # (see _enqueue_batch's _insert_on_conn for the full rationale).
        # Membership lock first, same rationale and same transaction
        # ownership as _insert_on_conn: the COPY + fixup span must sit
        # under one hold of the batches-row lock for the completion guard
        # to see this append as in-flight.
        membership_ids = _membership_batch_ids(args_list)
        if membership_ids:
            await _lock_batch_membership(conn, schema, membership_ids)
        refusals: list[MaxPendingExceededError] = []
        refused_indices: dict[str, list[int]] = {}
        refused_names: set[str] = set()
        admitted_args = args_list
        if enforce_max_pending:
            refusals = await _batch_cap_refusals(conn, sql, args_list)
            if refusals:
                refused_indices = {
                    r.actor: [i for i, a in enumerate(args_list) if a.actor == r.actor]
                    for r in refusals
                }
                refused_names = {r.actor for r in refusals}
                admitted_args = [a for a in args_list if a.actor not in refused_names]
        if not admitted_args:
            # Every item refused: no COPY, no fixup, no notify. The typed
            # error raises at the boundary; nothing was written.
            return 0, refusals, refused_indices

        # Why filter the pre-built records/arrays instead of re-serializing
        # the admitted subset: same rationale as _enqueue_batch's
        # insert_cols — the annotated build above already serialized every
        # item before any SQL (the pinned NUL -> cap -> COPY order), so
        # the partition selects positions from what is already built; the
        # happy path aliases through with zero extra work.
        copy_records = records
        fixup_cols: list[list[Any]] = [
            ids,
            scheduled_ats,
            stc_intervals,
            stc_raws,
            result_ttls,
        ]
        if refusals:
            keep = [i for i, a in enumerate(args_list) if a.actor not in refused_names]
            copy_records = [records[i] for i in keep]
            fixup_cols = [[col[i] for i in keep] for col in fixup_cols]

        try:
            # Why a savepoint around the COPY when the batch carries
            # idempotency keys or singleton items: a unique violation is
            # a STATEMENT error that poisons the surrounding transaction,
            # and the exact attribution below resolves the conflicting
            # row with a targeted SELECT that must run inside the
            # caller's scope — the savepoint's rollback restores that
            # scope before the lookup (the same discipline the
            # single-enqueue path's savepoint-isolated arms follow). The
            # happy path pays one SAVEPOINT/RELEASE pair per batch, never
            # per row; batches without idempotency keys cannot violate
            # the partial composite index (its predicate is
            # idempotency_key IS NOT NULL), batches without singleton
            # items cannot violate jobs_singleton_uniq (its predicate is
            # metadata @> '{"singleton": true}'), and a batch with
            # neither skips the wrapper entirely.
            keyed_batch = any(args.idempotency_key is not None for args in admitted_args)
            singleton_batch = any(args.metadata.get("singleton") is True for args in admitted_args)
            async with _optional_savepoint(conn, enabled=keyed_batch or singleton_batch):
                result = await conn.copy_records_to_table(
                    "jobs",
                    records=copy_records,
                    columns=sql.copy_enqueue_columns,
                    schema_name=schema,
                )
        except UniqueViolationError as exc:
            if exc.constraint_name == _LEGACY_IDEMPOTENCY_KEY_CONSTRAINT_NAME:
                # Rolling-deploy overlap window (see _enqueue_on_conn's
                # matching except-branch and
                # ScopedIdempotencyMigrationPendingError's docstring): an
                # item's bare idempotency_key already exists under a
                # DIFFERENT scope. Translated here too -- not just in the
                # single/batch paths -- so every enqueue API surfaces the
                # same typed, catchable error during the window instead of
                # a raw driver error. Unlike those paths there is no
                # retry: COPY has no ON CONFLICT arbiter, so a same-pair
                # race cannot dedupe on a second attempt -- the retried
                # COPY would simply violate again (composite or legacy
                # index, raw). Any unique violation aborts the whole COPY
                # before a single row is written, so nothing persists from
                # this attempt either way.
                logger.info(
                    "scoped-idempotency-legacy-index-conflict-batch-fast",
                    batch_size=len(admitted_args),
                )
                raise ScopedIdempotencyMigrationPendingError(detail=str(exc)) from exc
            if exc.constraint_name == _COMPOSITE_IDEMPOTENCY_KEY_CONSTRAINT_NAME:
                # Why classify while keeping the abort: COPY cannot
                # dedupe, so a same-pair duplicate (in-batch or raced
                # against a stored row) has no recovery on this path --
                # the all-or-nothing abort is the documented bulk-import
                # semantics and stays. But the raw
                # asyncpg.UniqueViolationError forced callers to
                # string-match a driver exception to tell "my batch had
                # a duplicate key" apart from every other unique
                # violation (pkey, singleton). The non-fast paths never
                # raise for this condition -- their ON CONFLICT arbiter
                # dedupes and RETURNS the existing row -- so there was
                # no typed error to reuse; DuplicateIdempotencyKeyError
                # is this path's own, a typed domain error for a
                # dedup-constraint violation. The offending pair is
                # resolved exactly from the batch's own contents plus a
                # post-abort lookup of the stored pairs
                # (_attribute_copy_duplicate) — never parsed from the
                # violation's detail text, which renders values raw and
                # unquoted and is ambiguous under positional reading (a
                # comma-bearing scope) or unusable outright (a localized
                # or truncated message). During the 01.00.03 rolling
                # window a same-pair duplicate may instead be reported
                # against the legacy index, which the branch above
                # already converts -- that carve-out is pre-existing
                # documented behavior for this path, unchanged here.
                duplicate = await _attribute_copy_duplicate(conn, sql, admitted_args, exc.detail)
                # A pair the resolution cannot see a holder for (a
                # concurrent commit-and-delete racer) stays unclassified
                # and reports the typed duplicate.
                mismatch = (
                    duplicate_pair_actor_mismatch(
                        admitted_args, duplicate.pair, duplicate.stored_actor
                    )
                    if duplicate.pair is not None
                    else None
                )
                if mismatch is not None:
                    _raise_batch_fast_actor_mismatch(
                        mismatch,
                        idempotency_scope=duplicate.scope,
                        idempotency_key=duplicate.key,
                        existing_job_id=duplicate.stored_job_id,
                        batch_size=len(args_list),
                        cause=exc,
                    )
                logger.info(
                    "batch-fast-duplicate-idempotency-key",
                    batch_size=len(args_list),
                    idempotency_key=duplicate.key,
                    idempotency_scope=duplicate.scope,
                )
                raise DuplicateIdempotencyKeyError(
                    idempotency_key=duplicate.key,
                    idempotency_scope=duplicate.scope,
                    detail=exc.detail,
                ) from exc
            if exc.constraint_name == _SINGLETON_CONSTRAINT_NAME:
                # Same typed refusal the single-enqueue path's Layer-2
                # catch raises (blocking_job_id/retry_after stay None:
                # a violation catch, not a preflight), classified like the
                # composite-key branch above so no caller has to
                # string-match a raw driver error. COPY has no ON
                # CONFLICT arbiter, so the all-or-nothing abort is
                # unchanged — the savepoint above rolled the COPY back
                # before this conversion raised.
                actor = await _attribute_singleton_collision(conn, sql, admitted_args, exc.detail)
                logger.info(
                    "singleton-collision",
                    actor=actor,
                    blocking_job_id=None,
                    detection_path="unique_violation_catch",
                    batch_size=len(admitted_args),
                )
                raise SingletonCollisionError(
                    actor=actor,
                    blocking_job_id=None,
                    retry_after=None,
                ) from exc
            raise
        count = int(result.split()[-1])
        await conn.execute(
            sql.enqueue_batch_fast_fixup,
            *fixup_cols,
        )
        return count, refusals, refused_indices

    def _raise_refusals(
        refusals: list[MaxPendingExceededError],
        refused_indices: dict[str, list[int]],
        admitted_count: int,
    ) -> None:
        raise BatchMaxPendingExceededError(
            refusals=refusals,
            refused_indices=refused_indices,
            admitted_count=admitted_count,
        )

    if (
        connection is not None
        and not connection.is_in_transaction()
        and (
            (enforce_max_pending and batch_cap_groups(args_list))
            # Same membership rationale as _enqueue_batch's wrapper: the
            # lock must live to the COPY's commit, which on a bare caller
            # connection only a transaction scope can give it.
            or _membership_batch_ids(args_list)
        )
    ):
        # Same race as above: the pre-COPY count and the COPY must share
        # one transaction on a caller-supplied bare connection. The inner
        # call returns its refusals instead of raising, the wrapper
        # commits the admitted rows, and the typed error raises only
        # AFTER that commit (pool-path parity).
        async with connection.transaction():
            count, refusals, refused_indices = await _copy_on_conn(connection)
        if refusals:
            _raise_refusals(refusals, refused_indices, count)
        return count

    if connection is not None:
        count, refusals, refused_indices = await _copy_on_conn(connection)
        if refusals:
            # Caller owns this open transaction; its commit/rollback
            # decides the admitted rows' durability (see the typed
            # error's docstring).
            _raise_refusals(refusals, refused_indices, count)
        return count

    async def _attempt_pool() -> tuple[int, list[MaxPendingExceededError], dict[str, list[int]]]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await _copy_on_conn(conn)

    count, refusals, refused_indices = await _with_fresh_connection_retry(
        _attempt_pool, operation="enqueue_batch_fast"
    )
    # Pool transaction committed: the partition refusal raises only after
    # the admitted rows are durable.
    if refusals:
        _raise_refusals(refusals, refused_indices, count)
    return count
