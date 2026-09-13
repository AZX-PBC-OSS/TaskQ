"""Enqueue operations for PostgresBackend.

``enqueue``, ``enqueue_with_conn``, ``enqueue_batch``, and
``enqueue_batch_fast`` live here as module-level functions taking
``(pool, sql: SqlTemplates, schema, clock, ...)`` parameters.
:class:`~taskq.backend.postgres.PostgresBackend` methods are thin
wrappers that delegate.
"""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from asyncpg.exceptions import UniqueViolationError

from taskq._advisory import acquire_advisory_xact_lock_bounded
from taskq.backend._protocol import (
    ConnLike,
    EnqueueArgs,
    JobRow,
    batch_cap_groups,
)
from taskq.backend._records import (
    _job_row_from_record,
    item_jsonb_param,
    item_tags_jsonb_param,
    jsonb_param,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend.clock import Clock
from taskq.constants import wake_channel
from taskq.exceptions import (
    DuplicateIdempotencyKeyError,
    MaxPendingExceededError,
    MaxPendingLockTimeoutError,
    ScopedIdempotencyMigrationPendingError,
    SingletonCollisionError,
    UniqueForLockTimeoutError,
)
from taskq.obs import (
    get_logger,
    record_backpressure_error,
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
# which a left-to-right split mis-reads as scope "a" key "b, c". The
# attribution therefore does not parse the detail at all: it renders each
# of the batch's own (scope, key) candidates into PG's detail format and
# matches. Exactly one rendering equal to the server's detail names the
# pair honestly (comma-space scopes included); zero matches (a localized
# or truncated detail, a non-raw rendering) or more than one (two
# distinct candidate pairs producing the same detail text) degrade to
# unattributed-but-typed -- never a wrong pair. The in-memory mirror
# attributes exactly by construction; PG parity is best-effort with this
# verified fallback.
_COMPOSITE_IDEMPOTENCY_DETAIL_TEMPLATE = (
    "Key (idempotency_scope, idempotency_key)=({scope}, {key}) already exists."
)


def _attribute_duplicate_pair(
    detail: str | None,
    candidates: "set[tuple[str, str]]",
) -> tuple[str | None, str | None]:
    """Best-effort attribution of a composite-index COPY violation.

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


async def _enforce_batch_max_pending(
    conn: ConnLike,
    sql: SqlTemplates,
    args_list: list[EnqueueArgs],
) -> None:
    """Reject a batch whose per-actor aggregate exceeds the effective cap.

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
    cap may refuse loudly rather than admit silently.) pgqueuer's
    capacity-slot indexes (v1.4.0) are the heavyweight version of this
    guarantee; the count here is exact for the single statement it
    guards. Concurrent bulk batches on separate connections can still
    race (count-then-insert without a serializing lock — the
    single-enqueue path takes one, bulk paths deliberately do not, for
    throughput); that residual is documented, not silent.
    """
    groups = batch_cap_groups(args_list)
    if not groups:
        return
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
    for actor, (batch_count, _carried) in groups.items():
        cap = effective[actor]
        have = existing.get(actor, 0)
        admitted = batch_count - deduped_counts.get(actor, 0)
        if have + admitted > cap:
            raise MaxPendingExceededError(
                actor=actor,
                current_count=have,
                max_pending=cap,
            )


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
    enqueue_batch(connection=...)) cannot retry -- their transaction is
    already aborted -- so they convert immediately, preserving this
    release's documented behavior for that path.
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
    log queue counts). Correspondingly NOT recorded against
    ``taskq.backpressure.errors``: identity-key contention is not a
    capacity signal, and spiking that counter would trip capacity
    alerting; the ``unique-for-lock-timeout`` log event carries the
    observability instead. A raw driver error never surfaces from
    contention.
    """
    if not await acquire_advisory_xact_lock_bounded(conn, lock_key, timeout_ms=timeout_ms):
        logger.warning(
            "unique-for-lock-timeout",
            actor=actor,
            identity_key=identity_key,
            lock_timeout_ms=timeout_ms,
        )
        raise UniqueForLockTimeoutError(
            actor=actor,
            identity_key=identity_key,
            timeout_ms=timeout_ms,
        )


async def _enqueue_on_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
    *,
    max_pending_lock_timeout_ms: float = DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS,
    unique_for_lock_timeout_ms: float = DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS,
) -> JobRow:
    """Core enqueue logic running on *conn*.

    Includes unique_for preflight, singleton preflight, max_pending
    count, INSERT, idempotency-key SELECT on conflict, and pg_notify.
    Does NOT acquire from ``worker_pool`` — the caller supplies the
    connection. A transaction is opened here only for capped actors on
    a transaction-less caller connection (so the count-then-insert
    serialization holds); otherwise the caller owns transaction scope.

    The unique_for single-flight guarantee depends on that transaction: the
    advisory lock below is transaction-scoped, so on a caller-supplied
    connection with no open transaction it is released at statement end and
    the preflight is advisory only. That is the same connection on which the
    caller has already taken responsibility for atomicity.

    Capped actors additionally run inside a transaction owned here when the
    caller supplied none (see below): the max_pending count-then-insert must
    be serialized, and a transaction-scoped lock only serializes inside a
    transaction.
    """
    if args.max_pending is not None and not conn.is_in_transaction():
        # Why a transaction here and not just the lock: pg_advisory_xact_lock
        # releases at transaction end, so on a bare caller connection (every
        # statement its own transaction) the lock below would release before
        # the INSERT and overlapping counts would each see room — the
        # count-then-insert race pgqueuer closed with capacity-slot indexes
        # (v1.4.0, #761/#774/#777). Wrapping makes the lock span the
        # count and the INSERT; the recursion terminates because the inner
        # call observes the open transaction. Callers that already hold a
        # transaction are untouched.
        async with conn.transaction():
            return await _enqueue_on_conn(
                conn,
                sql,
                schema,
                clock,
                args,
                max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
                unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
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
        # (Same conclusion as graphile-worker's design by omission: it offers
        # only a permanent job_key upsert index, no windowed dedup, because a
        # window cannot be an index predicate; the queues that dedup by index
        # — graphile-worker, pgqueuer's dedupe_key — dedup forever.)
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
            logger.info(
                "enqueue_deduplicated",
                kind="enqueue_deduplicated",
                job_id=str(row.id),
                actor=row.actor,
                queue=row.queue,
                identity_key=row.identity_key,
                idempotency_key=None,
                existing_job_id=str(row.id),
                dedup_reason="unique_for",
            )
            return row

    if args.metadata.get("singleton") is True:
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
        )
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

    if is_new:
        await conn.execute(
            sql.enqueue_notify,
            wake_channel(schema),
        )
        logger.info(
            "enqueue",
            kind="enqueue",
            job_id=str(row.id),
            actor=row.actor,
            queue=row.queue,
            idempotency_key=row.idempotency_key,
        )
    else:
        logger.info(
            "enqueue_deduplicated",
            kind="enqueue_deduplicated",
            job_id=str(row.id),
            actor=row.actor,
            queue=row.queue,
            identity_key=row.identity_key,
            idempotency_key=row.idempotency_key,
            idempotency_scope=row.idempotency_scope,
            existing_job_id=str(row.id),
            dedup_reason="idempotency_key",
        )

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
        )
    except _LegacyIdempotencyKeyConflictError as exc:
        # Caller owns the (now aborted) transaction -- cannot retry here.
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
) -> JobRow:
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await _enqueue_on_conn(
                    conn,
                    sql,
                    schema,
                    clock,
                    args,
                    max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
                    unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
                )
    except _LegacyIdempotencyKeyConflictError as exc:
        public = exc.to_public()

    # One retry on a fresh transaction. If the violation was a same-pair
    # race, the conflicting row is now committed (a unique-violation report
    # means the other transaction committed) and the composite arbiter
    # dedupes cleanly below. If it was genuine cross-scope reuse, the
    # legacy index violates again and the public typed error is raised.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await _enqueue_on_conn(
                    conn,
                    sql,
                    schema,
                    clock,
                    args,
                    max_pending_lock_timeout_ms=max_pending_lock_timeout_ms,
                    unique_for_lock_timeout_ms=unique_for_lock_timeout_ms,
                )
    except _LegacyIdempotencyKeyConflictError as exc:
        logger.warning(
            "scoped-idempotency-migration-pending",
            actor=public.actor,
            idempotency_key=public.idempotency_key,
            idempotency_scope=public.idempotency_scope,
        )
        raise public from exc.original or exc


async def _enqueue_batch(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    args_list: list[EnqueueArgs],
    *,
    connection: "ConnLike | None" = None,
    enforce_max_pending: bool = True,
) -> list[JobRow]:
    if not args_list:
        raise ValueError("args_list must not be empty")
    if (
        enforce_max_pending
        and connection is not None
        and batch_cap_groups(args_list)
        and not connection.is_in_transaction()
    ):
        # Same race as capped singles: the admission count and the INSERT
        # must share one transaction or a concurrent writer slips between
        # them. Pool-acquired connections already wrap below; a
        # caller-supplied connection without an open transaction gets the
        # same treatment here (all-or-nothing, matching the pool path).
        # The recursion terminates: the inner call observes the open
        # transaction. Uncapped batches skip this entirely.
        async with connection.transaction():
            return await _enqueue_batch(
                pool,
                sql,
                schema,
                args_list,
                connection=connection,
                enforce_max_pending=enforce_max_pending,
            )

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

    # Why annotate per item during the build: this loop serializes every
    # item BEFORE any SQL runs, so the first NUL-bearing item aborts the
    # whole batch with nothing written. item_jsonb_param /
    # item_tags_jsonb_param attach the per-item annotation (index, actor,
    # field) at that raise — the same contract the client layer's
    # _item_payload_error gives pydantic failures — instead of the bare
    # ValueError(NUL_JSONB_ERROR) that named nothing. Admission semantics
    # are unchanged: still all-or-nothing.
    for idx, args in enumerate(args_list):
        ids.append(args.id)
        actors.append(args.actor)
        queues.append(args.queue)
        identity_keys.append(str(args.identity_key) if args.identity_key is not None else None)
        fairness_keys.append(args.fairness_key)
        payloads.append(item_jsonb_param(args.payload, idx=idx, field="payload", actor=args.actor))
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
            item_jsonb_param(args.metadata, idx=idx, field="metadata", actor=args.actor)
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
        tag_jsons.append(item_tags_jsonb_param(args.tags, idx=idx, actor=args.actor))

    async def _enqueue_batch_on_conn(conn: ConnLike) -> list[JobRow]:
        if enforce_max_pending:
            await _enforce_batch_max_pending(conn, sql, args_list)
        try:
            returning_recs = await conn.fetch(
                sql.enqueue_batch,
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
            )
        except UniqueViolationError as exc:
            if exc.constraint_name == _LEGACY_IDEMPOTENCY_KEY_CONSTRAINT_NAME:
                # Rolling-deploy overlap window (see
                # _enqueue_on_conn's matching except-branch and
                # _LegacyIdempotencyKeyConflictError). Unlike the
                # single-enqueue path, this INSERT is one statement
                # covering the whole batch: a single cross-scope collision
                # against the legacy index aborts the ENTIRE batch, not
                # just the offending item -- Postgres gives us no cheaper
                # way to identify which item(s) caused it without
                # re-inserting one row at a time, which isn't warranted
                # for a purely transitional migration-window condition.
                logger.info(
                    "scoped-idempotency-legacy-index-conflict-batch",
                    batch_size=len(args_list),
                )
                raise _LegacyIdempotencyKeyConflictError(detail=str(exc), original=exc) from exc
            raise

        inserted_ids: set[UUID] = {rec["id"] for rec in returning_recs}
        if inserted_ids:
            await conn.execute(
                sql.enqueue_notify,
                wake_channel(schema),
            )

        new_rows_by_id: dict[UUID, object] = {rec["id"]: rec for rec in returning_recs}

        collision_pairs: list[tuple[str, str]] = []
        for args in args_list:
            if args.idempotency_key is not None and args.id not in inserted_ids:
                collision_pairs.append((args.idempotency_scope, str(args.idempotency_key)))

        new_item_ids = list(inserted_ids)
        full_new_recs: dict[UUID, object] = {}
        if new_item_ids:
            recs = await conn.fetch(
                sql.enqueue_batch_fetch_by_ids,
                new_item_ids,
            )
            for rec in recs:
                # Why no UUID(bytes=...) reconstruction: asyncpg's uuid codec
                # already returns stdlib uuid.UUID — same assumption the
                # inserted_ids set above makes.
                full_new_recs[rec["id"]] = rec

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

        result: list[JobRow] = []
        for args in args_list:
            arg_uuid = args.id
            if arg_uuid in full_new_recs:
                result.append(_job_row_from_record(full_new_recs[arg_uuid]))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
            elif (
                args.idempotency_key is not None
                and (args.idempotency_scope, str(args.idempotency_key)) in existing_by_idem
            ):
                rec = existing_by_idem[(args.idempotency_scope, str(args.idempotency_key))]
                result.append(_job_row_from_record(rec))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
            else:
                partial = new_rows_by_id.get(arg_uuid)
                if partial is not None:
                    result.append(_job_row_from_record(partial))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
                else:
                    raise RuntimeError(
                        f"enqueue_batch: no row found for args.id={args.id!r} "
                        f"after INSERT; this is a bug"
                    )
        return result

    if connection is not None:
        try:
            return await _enqueue_batch_on_conn(connection)
        except _LegacyIdempotencyKeyConflictError as exc:
            # Caller owns the (now aborted) transaction -- cannot retry.
            logger.warning("scoped-idempotency-migration-pending-batch")
            raise exc.to_public() from exc.original or exc
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await _enqueue_batch_on_conn(conn)
    except _LegacyIdempotencyKeyConflictError as exc:
        public = exc.to_public()
    # One retry on a fresh transaction (see _enqueue for the rationale).
    # The first attempt's statement failure aborted its transaction, so
    # nothing from it persisted and the whole batch re-executes cleanly;
    # same-pair-raced items now dedupe via the composite arbiter and the
    # follow-up fetch, while genuine cross-scope reuse violates the legacy
    # index again and surfaces as the public typed error.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await _enqueue_batch_on_conn(conn)
    except _LegacyIdempotencyKeyConflictError as exc:
        logger.warning("scoped-idempotency-migration-pending-batch")
        raise public from exc.original or exc


async def _enqueue_batch_fast(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    args_list: list[EnqueueArgs],
    *,
    connection: "ConnLike | None" = None,
    enforce_max_pending: bool = True,
) -> int:
    if not args_list:
        raise ValueError("args_list must not be empty")
    if (
        enforce_max_pending
        and connection is not None
        and batch_cap_groups(args_list)
        and not connection.is_in_transaction()
    ):
        # Same race as above: the pre-COPY count and the COPY must share
        # one transaction on a caller-supplied bare connection.
        async with connection.transaction():
            return await _enqueue_batch_fast(
                pool,
                sql,
                schema,
                args_list,
                connection=connection,
                enforce_max_pending=enforce_max_pending,
            )

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
    # Same per-item annotation as _enqueue_batch's build loop: the COPY
    # record tuples are serialized here, before any statement is issued,
    # so a NUL-bearing item rejects the whole batch (nothing written)
    # with the item index, actor, and field named. Tags bind as text[]
    # (no jsonb hop on this path) and stay guarded by the EnqueueArgs
    # construction chokepoint alone.
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
                item_jsonb_param(args.payload, idx=idx, field="payload", actor=args.actor),
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
                item_jsonb_param(args.metadata, idx=idx, field="metadata", actor=args.actor),
                list(args.tags),
            )
        )

    async def _copy_on_conn(conn: ConnLike) -> int:
        if enforce_max_pending:
            await _enforce_batch_max_pending(conn, sql, args_list)
        try:
            result = await conn.copy_records_to_table(
                "jobs",
                records=records,
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
                    batch_size=len(args_list),
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
                # is this path's own, following pgqueuer's
                # DuplicateJobError precedent (typed domain error for a
                # dedup-constraint violation, raised by their in-memory
                # adapter too). The offending pair is attributed by
                # MATCHING the detail against the batch's own candidates
                # (see _attribute_duplicate_pair): named exactly when the
                # rendering is unambiguous -- including comma-bearing
                # scopes, which a positional parse mis-reads --
                # and unattributed-but-typed on ambiguity (two distinct
                # pairs rendering to the same detail text) or on a
                # localized/truncated detail. During the 01.00.03 rolling
                # window a same-pair duplicate may instead be reported
                # against the legacy index, which the branch above
                # already converts -- that carve-out is pre-existing
                # documented behavior for this path, unchanged here.
                batch_candidates = {
                    (args.idempotency_scope, str(args.idempotency_key))
                    for args in args_list
                    if args.idempotency_key is not None
                }
                dup_scope, dup_key = _attribute_duplicate_pair(exc.detail, batch_candidates)
                logger.info(
                    "batch-fast-duplicate-idempotency-key",
                    batch_size=len(args_list),
                    idempotency_key=dup_key,
                    idempotency_scope=dup_scope,
                )
                raise DuplicateIdempotencyKeyError(
                    idempotency_key=dup_key,
                    idempotency_scope=dup_scope,
                    detail=exc.detail,
                ) from exc
            raise
        count = int(result.split()[-1])
        await conn.execute(
            sql.enqueue_batch_fast_fixup,
            ids,
            scheduled_ats,
            stc_intervals,
            stc_raws,
            result_ttls,
        )
        await conn.execute(
            sql.enqueue_notify,
            wake_channel(schema),
        )
        return count

    if connection is not None:
        return await _copy_on_conn(connection)
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _copy_on_conn(conn)
