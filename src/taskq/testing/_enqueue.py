"""Enqueue operations for InMemoryBackend.

``enqueue``, ``enqueue_with_conn``, ``enqueue_batch``, and
``enqueue_batch_fast`` live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter.
"""

from dataclasses import replace
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq._json import dumps_jsonb_str, loads
from taskq.backend._protocol import (
    CancelPhase,
    EnqueueArgs,
    JobRow,
    batch_cap_groups,
    duplicate_pair_actor_mismatch,
    first_duplicate_idempotency_pair,
    first_singleton_collision_actor,
)
from taskq.backend._records import item_jsonb_param, item_tags_jsonb_param
from taskq.exceptions import (
    BatchMaxPendingExceededError,
    MaxPendingExceededError,
    SingletonCollisionError,
)
from taskq.obs import record_backpressure_error
from taskq.testing._reads import _read_copy

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_enqueue",
    "_enqueue_batch",
    "_enqueue_batch_fast",
    "_enqueue_with_conn",
]

logger = structlog.get_logger("taskq.testing.in_memory")


async def _enqueue(self: "InMemoryBackend", args: EnqueueArgs) -> JobRow:
    # Why a function-level import: the shared dedup-log helper lives with
    # the PG enqueue path (taskq.backend._enqueue), whose module scope
    # imports the asyncpg driver; the testing package's import surface
    # stays driver-free at import time (the taskq._advisory convention),
    # and this call path only ever runs where the driver is installed.
    from taskq.backend._enqueue import (
        _log_enqueue_dedup,
        _refuse_cross_actor_idempotency_hit,
    )

    if args.unique_for is not None and args.identity_key is not None:
        now = self._clock.now()
        cutoff = now - args.unique_for
        candidates = [
            row
            for row in self._jobs.values()
            if row.actor == args.actor
            and row.identity_key == args.identity_key
            and row.status in args.unique_states
            and row.created_at > cutoff
        ]
        if candidates:
            existing_row = max(candidates, key=lambda r: r.created_at)
            # Same shared helper as the idempotency seam below and as the
            # PG path: one field set, one terminal-target escalation, and
            # per-site truth in dedup_reason alone. A dead target must be
            # as loud here as it is on the sibling arm.
            _log_enqueue_dedup(existing_row, dedup_reason="unique_for")
            return _read_copy(existing_row)

    if args.metadata.get("singleton") is True:
        from datetime import timedelta

        for row in self._jobs.values():
            if (
                row.actor == args.actor
                and row.status in ("pending", "scheduled", "running")
                and row.metadata.get("singleton") is True
            ):
                now = self._clock.now()
                retry_after: timedelta | None = None
                if row.schedule_to_close is not None and row.schedule_to_close > now:
                    retry_after = row.schedule_to_close - now
                logger.info(
                    "singleton-collision",
                    actor=args.actor,
                    blocking_job_id=str(row.id),
                    detection_path="preflight_check",
                )
                raise SingletonCollisionError(
                    actor=args.actor,
                    blocking_job_id=row.id,
                    retry_after=retry_after,
                )

    if args.max_pending is not None:
        current_count = sum(
            1
            for row in self._jobs.values()
            if row.actor == args.actor and row.status in ("pending", "scheduled")
        )
        if current_count >= args.max_pending:
            logger.warning(
                "max-pending-exceeded",
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )
            raise MaxPendingExceededError(
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )

    # PG binds payload/metadata through jsonb_param → dumps_jsonb_str at
    # INSERT time, after the preflights above, before any idempotency
    # conflict resolution, and rejects a NUL there. Mirror that exact
    # guard (same function, so the same error) here: without it a payload
    # InMemory accepted raised ValueError on the first real PG enqueue, so
    # an app validated against InMemory broke in production.  The guard
    # lives in this mirror, NOT in EnqueueArgs._check_no_nul_text, because
    # a struct-level check would double-scan the PG hot path, which
    # already guards at bind time.  The guard's serialization is also
    # what PG stores: the jsonb column holds the orjson text and reads it
    # back through loads, so the stored values are its round-trip ,
    # values whose encoding differs from the Python object (NaN/Infinity
    # → null, UUID → string, tuple → array) read back exactly as PG
    # reads them.
    stored_payload = loads(dumps_jsonb_str(args.payload))
    stored_metadata = loads(dumps_jsonb_str(args.metadata))

    if args.idempotency_key is not None:
        # NOTE: InMemoryBackend always simulates the fully-migrated
        # (post-01.00.03_01_post_idempotency_scope_drop_old_index) state --
        # true (idempotency_scope, idempotency_key) isolation, always. It
        # does NOT model the rolling-deploy overlap window where Postgres
        # still has the old global idempotency_key-only index alongside
        # the new composite one (see ScopedIdempotencyMigrationPendingError
        # and _enqueue.py's matching handling). Tests that need to exercise
        # that transitional window must do so against real Postgres
        # (tests/test_idempotency_scope_migrations.py); InMemoryBackend
        # cannot reproduce the cross-scope collision that window raises.
        existing_id = self._idempotency_index.get((args.idempotency_scope, args.idempotency_key))
        if existing_id is not None:
            existing_row = self._jobs.get(existing_id)
            if existing_row is not None:
                _refuse_cross_actor_idempotency_hit(
                    args, existing_actor=existing_row.actor, existing_job_id=existing_row.id
                )
                _log_enqueue_dedup(existing_row, dedup_reason="idempotency_key")
                return _read_copy(existing_row)

    now = self._clock.now()
    # None means immediate: stamp from this backend's own (single-domain)
    # clock, the InMemory mirror of the server's COALESCE stamp.
    stamped_scheduled_at = args.scheduled_at if args.scheduled_at is not None else now
    status: object = "pending" if stamped_scheduled_at <= now else "scheduled"

    # A stub's explicit retry= budget stamps the row here, exactly like
    # the ActorRef stamp that build_enqueue_args already applied.

    resolved_schedule_to_close = (
        now + args.schedule_to_close_interval
        if args.schedule_to_close_interval is not None
        else args.schedule_to_close
    )

    result_expires_at = now + args.result_ttl if args.result_ttl is not None else None

    row = JobRow(
        id=args.id,
        actor=args.actor,
        queue=args.queue,
        identity_key=args.identity_key,
        fairness_key=args.fairness_key,
        payload=stored_payload,
        payload_schema_ver=args.payload_schema_ver,
        status=status,  # type: ignore[arg-type]  # Why: ternary "pending" if ... else "scheduled" is not narrowed to JobStatus by pyright
        priority=args.priority,
        attempt=0,
        max_attempts=args.max_attempts,
        retry_kind=args.retry_kind,
        schedule_to_close=resolved_schedule_to_close,
        start_to_close=args.start_to_close,
        heartbeat_timeout=args.heartbeat_timeout,
        created_at=now,
        scheduled_at=stamped_scheduled_at,
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=CancelPhase.NONE,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=result_expires_at,
        idempotency_key=args.idempotency_key,
        idempotency_scope=args.idempotency_scope,
        trace_id=args.trace_id,
        span_id=args.span_id,
        metadata=stored_metadata,
        tags=args.tags,
        retry_base=args.retry_base,
        retry_cap=args.retry_cap,
        retry_backoff=args.retry_backoff,
        retry_jitter=args.retry_jitter,
    )

    if args.id in self._jobs:
        # Why a function-level import: the driver-free import-surface
        # convention (see the _log_enqueue_dedup import above); this path
        # only ever runs where the driver is installed.
        from asyncpg.exceptions import UniqueViolationError

        # PG's enqueue INSERT has no ON CONFLICT arbiter for the primary
        # key (only the singleton and legacy-idempotency constraints are
        # typed conversions, backend/_enqueue.py), so an INSERT carrying
        # an existing job id propagates the RAW UniqueViolationError from
        # jobs_pkey. The twin refuses identically, never silently
        # overwriting a live row, which certified code that would corrupt
        # on PG.
        raise UniqueViolationError(
            f'duplicate key value violates unique constraint "jobs_pkey" '
            f"(job id {args.id} already stored)"
        )

    self._jobs[args.id] = row

    if args.idempotency_key is not None:
        self._idempotency_index[(args.idempotency_scope, args.idempotency_key)] = args.id

    # Queue-scoped wake, the PG trigger's payload filter mirrored: a
    # subscriber registered with a queue set is not woken by an enqueue
    # to a queue it would never claim (None = wake on everything).
    for event, queues in self._wake_queues.items():
        if queues is None or args.queue in queues:
            event.set()

    logger.debug(
        "state-change",
        kind="state_change",
        from_state=None,
        to_state=status,
        job_id=str(args.id),
        actor=args.actor,
    )

    return _read_copy(row)


async def _enqueue_with_conn(
    self: "InMemoryBackend",
    conn: object,
    args: EnqueueArgs,
) -> JobRow:
    return await _enqueue(self, args)


async def _enqueue_batch(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
    *,
    connection: object = None,
    enforce_max_pending: bool = True,
) -> list[JobRow]:
    """The bulk tier's mirror, atomic for every whole-call refusal.

    Postgres' ``enqueue_batch`` is one ``unnest`` INSERT in one
    transaction, so its observable failure contract is all-or-nothing,
    and this mirror refuses every whole-call defect BEFORE its first
    insert: the jsonb NUL guard, the job-id collision
    (``_check_batch_job_ids``), the singleton collision
    (``_check_batch_singletons``), and the cross-actor idempotency
    mismatch (``_check_batch_idempotency_actors``, stored holders and
    in-batch ones alike) each abort the call with nothing stored, where
    PG's statement abort or transaction rollback admits nothing either.

    The idempotency contract pinned here, matching PG's observable
    behavior item for item:

    - Uniqueness is the ``(idempotency_scope, idempotency_key)`` pair,
      schema-wide. A pair already stored, or held by an EARLIER item of
      the same call, is a dedup hit for the later item.
    - A same-actor dedup hit returns the holder row in the item's
      position: one row per item, order preserved, repeats alias the
      holder's id (PG's ``ON CONFLICT DO NOTHING`` plus post-abort
      fetch). The holder for an in-batch pair is the first earlier item
      holding it, never the stored row's id when the pair was new.
    - A cross-actor dedup hit raises
      :class:`~taskq.exceptions.IdempotencyKeyActorMismatchError` naming
      incoming and holder actors, and the refusal is ATOMIC: no item
      from the call is stored, including the clean items before the
      offending one. ``existing_job_id`` names the holder row's id (for
      an in-batch holder, the earlier item's id, which PG's post-abort
      fetch also reports even though its row is withdrawn with the
      refusal).
    - Attribution order is call order: the first item in *args_list*
      whose holder is another actor's job is the mismatch raised.

    Cap admission partitions per actor (see ``_batch_cap_refusals``):
    over-cap actors' items are refused as a group, the rest are stored,
    and :class:`~taskq.exceptions.BatchMaxPendingExceededError` raises
    AFTER the admitted rows are stored.
    """
    if not args_list:
        raise ValueError("args_list must not be empty")
    # PG-tier parity for jsonb serialization failures: the PG build loop
    # serializes every item BEFORE any SQL runs, so a NUL-bearing item
    # rejects the whole batch with a per-item-annotated
    # PayloadValidationError and nothing written. Without this preflight
    # the per-item loop below admitted items 0..k-1 before item k raised
    # a bare, unattributed ValueError -- diverging from PG on both
    # attribution and admission. Runs before the cap preflight to match
    # the PG statement order (build loop precedes the cap count).
    _check_batch_jsonb(args_list)
    refusals: list[MaxPendingExceededError] = []
    refused_indices: dict[str, list[int]] = {}
    admitted_args = args_list
    if enforce_max_pending:
        # Per-actor partition parity with the PG bulk tier: over-cap
        # actors' items are refused as a group, every other actor's items
        # are admitted, and the typed refusal raises AFTER the admitted
        # rows are stored, matching what PostgresBackend.enqueue_batch
        # enforces (there: after the admitting transaction commits).
        refusals = await _batch_cap_refusals(self, args_list)
        if refusals:
            refused_indices = {
                r.actor: [i for i, a in enumerate(args_list) if a.actor == r.actor]
                for r in refusals
            }
            refused_names = {r.actor for r in refusals}
            admitted_args = [a for a in args_list if a.actor not in refused_names]
    # PG-tier atomicity for job-id collisions: the PG bulk tier is
    # one unnest INSERT in one transaction with no ON CONFLICT arbiter
    # for the primary key, so a duplicate id, against a stored row or
    # another item in this batch, aborts the ENTIRE call with nothing
    # admitted. Pre-validate the whole ADMITTED subset (refused actors'
    # items never reach the PG INSERT either) before the loop below
    # stores its first row, so the same batch raises the same
    # UniqueViolationError with the same empty stored-row state on both
    # backends. The per-item loop previously discovered the collision at
    # the poisoned item's index and left the good prefix stored ,
    # certifying code that leaves phantom rows behind on PG.
    _check_batch_job_ids(self, admitted_args)
    _check_batch_singletons(self, admitted_args)
    _check_batch_idempotency_actors(self, admitted_args)
    # Why a function-level import: the dedup WARNING budget lives with
    # the PG enqueue path (taskq.backend._enqueue), whose module scope
    # imports the asyncpg driver; the testing package's import surface
    # stays driver-free at import time (the same convention as
    # _log_enqueue_dedup above), and this call path only ever runs where
    # the driver is installed.
    from taskq.backend._enqueue import (
        _dedup_warn_budget,
        _DedupWarnBudget,
        _log_enqueue_dedup_warn_summary,
    )

    rows: list[JobRow] = []
    # The per-call dedup WARNING budget, mirroring the PG bulk
    # tier's result-assembly scope: this loop is where the mirror logs
    # its batch dedup hits (inside _enqueue, via the shared helper), so a
    # budget scoped here IS the call's budget. Reset in the finally so no
    # leak escapes into the caller's context; the summary rides the same
    # finally so the failure path still reports suppressed hits.
    dedup_budget = _DedupWarnBudget()
    dedup_budget_token = _dedup_warn_budget.set(dedup_budget)
    try:
        for args in admitted_args:
            # Why strip the carried cap AND unique_for here: the aggregate
            # check above is the batch tier's ONLY admission decision (the PG
            # tier's single unnest INSERT has no per-item cap logic either).
            # Leaving the cap on would re-check per item WITHOUT the
            # aggregate's idempotency discount, refusing pure-retry batches
            # the aggregate just admitted. unique_for is stripped for the
            # same parity reason: the PG batch tiers (the unnest INSERT and
            # the COPY) never run the unique_for preflight, a bulk statement
            # cannot take a per-identity advisory lock without per-item round
            # trips that defeat bulk throughput, so every batch item writes
            # and unique_for items are conservatively fully counted toward
            # the cap instead. A mirror that deduped here would certify dedup
            # production never performs. Whether the batch tier SHOULD honor
            # unique_for is a pending owner decision; bulk operations commonly
            # use partial unique indices with ON CONFLICT to enforce
            # uniqueness at write time rather than preflight checks. This
            # restores parity with current production behavior, it does
            # not adjudicate it.
            row = await _enqueue(self, replace(args, max_pending=None, unique_for=None))
            rows.append(row)
    finally:
        _dedup_warn_budget.reset(dedup_budget_token)
        _log_enqueue_dedup_warn_summary(dedup_budget, dedup_reason="idempotency_key")
    if refusals:
        raise BatchMaxPendingExceededError(
            refusals=refusals,
            refused_indices=refused_indices,
            admitted_count=len(rows),
        )
    return rows


def _check_batch_job_ids(self: "InMemoryBackend", admitted_args: list[EnqueueArgs]) -> None:
    """Reject the whole batch BEFORE any insert when an admitted item's
    job id collides, with a stored row or another item in this batch.

    Same typed error as the single-enqueue path's stored-id collision
    (the raw ``UniqueViolationError`` PG's jobs_pkey raises, no arbiter
    conversion), raised with NOTHING from the batch admitted, the PG
    bulk tier's whole-call atomicity, mirrored. In-batch duplicates
    violate the same constraint in the same single statement on PG.
    """
    from asyncpg.exceptions import UniqueViolationError

    seen: set[UUID] = set()
    for args in admitted_args:
        if args.id in self._jobs:
            raise UniqueViolationError(
                f'duplicate key value violates unique constraint "jobs_pkey" '
                f"(job id {args.id} already stored)"
            )
        if args.id in seen:
            raise UniqueViolationError(
                f'duplicate key value violates unique constraint "jobs_pkey" '
                f"(job id {args.id} appears twice in this batch)"
            )
        seen.add(args.id)


def _check_batch_idempotency_actors(
    self: "InMemoryBackend", admitted_args: list[EnqueueArgs]
) -> None:
    """Reject the whole batch BEFORE any insert when an admitted item's
    idempotency (scope, key) pair resolves to another actor's job,
    whether that job is already stored or is an earlier item of this
    same batch.

    The PG bulk tier is one INSERT in one transaction: the conflicting
    item's pair is skipped by the ``ON CONFLICT`` arbiter, the result
    assembly resolves it to the holder row (a stored row, or the earlier
    item's just-inserted row), and the cross-actor refusal propagates out
    of that transaction, withdrawing the whole INSERT and admitting
    nothing. The per-item loop below would discover an in-batch collision
    at the offending item's index and leave the good prefix stored,
    certifying code that on Postgres leaves phantom rows behind (and,
    for a cross-actor pair, ships a batch production refuses outright).

    The walk is ONE pass over the batch in call order, the order PG's
    result assembly attributes mismatches in: each keyed item resolves to
    its holder (the stored row for its pair, else the first earlier item
    holding the pair) and a holder of another actor raises the shared
    typed mismatch, naming the same actors and holder id the PG tier's
    post-abort fetch reports. Same-actor repeats pass and dedupe in the
    insert loop below, returning the earlier row, exactly PG's
    ``ON CONFLICT`` dedup (in-batch and stored hits alike).
    """
    from taskq.backend._enqueue import _refuse_cross_actor_idempotency_hit

    in_batch_holders: dict[tuple[str, str], tuple[str, UUID]] = {}
    for args in admitted_args:
        if args.idempotency_key is None:
            continue
        pair = (args.idempotency_scope, args.idempotency_key)
        existing_id = self._idempotency_index.get(pair)
        existing_row = self._jobs.get(existing_id) if existing_id is not None else None
        if existing_row is not None:
            # Stored holder: the pair was written before this call, so
            # every repeat of the pair in the batch resolves to this row.
            _refuse_cross_actor_idempotency_hit(
                args, existing_actor=existing_row.actor, existing_job_id=existing_row.id
            )
        elif pair in in_batch_holders:
            holder_actor, holder_job_id = in_batch_holders[pair]
            _refuse_cross_actor_idempotency_hit(
                args, existing_actor=holder_actor, existing_job_id=holder_job_id
            )
        else:
            in_batch_holders[pair] = (args.actor, args.id)


def _check_batch_singletons(self: "InMemoryBackend", admitted_args: list[EnqueueArgs]) -> None:
    """Reject the whole batch BEFORE any insert when an admitted singleton
    item collides, with a live singleton job or another singleton item for
    the same actor in this batch.

    The PG bulk tier is one ``unnest`` INSERT in one transaction, so the
    partial unique index covering live singleton rows aborts the entire
    statement and admits nothing, including the items that precede the
    colliding one. The per-item loop below discovers the collision at the
    offending item's index and would leave the good prefix stored, which
    certifies application code that on Postgres leaves no such rows behind.

    The refusal carries the typed backpressure error the single-enqueue
    preflight raises, not the driver's constraint violation: a singleton
    collision is a retryable admission denial, and a caller must be able
    to branch on it without reading a driver traceback. The colliding
    actor comes from the shared ``first_singleton_collision_actor`` rule ,
    the same rule the PG bulk tier's post-abort attribution applies, so
    the two backends cannot drift on which actor gets named.

    The live-row scan matches on ``is True``, the same predicate the
    single-enqueue preflight and Postgres' partial unique index
    (``metadata @> '{"singleton": true}'``) apply: a truthy-but-not-true
    metadata value (``1``, ``"yes"``) never armed singleton enforcement
    at insert time on either backend, so it must not block a batch here
    either.
    """
    from datetime import timedelta

    now = self._clock.now()
    live: dict[str, JobRow] = {}
    for row in self._jobs.values():
        if (
            row.status in ("pending", "scheduled", "running")
            and row.metadata.get("singleton") is True
        ):
            live.setdefault(row.actor, row)
    actor = first_singleton_collision_actor(admitted_args, live)
    if actor is None:
        return
    blocking = live.get(actor)
    retry_after: timedelta | None = None
    blocking_id: UUID | None = None
    if blocking is not None:
        blocking_id = blocking.id
        if blocking.schedule_to_close is not None and blocking.schedule_to_close > now:
            retry_after = blocking.schedule_to_close - now
    logger.info(
        "singleton-collision",
        actor=actor,
        blocking_job_id=str(blocking_id) if blocking_id is not None else None,
        detection_path="preflight_check",
    )
    raise SingletonCollisionError(
        actor=actor,
        blocking_job_id=blocking_id,
        retry_after=retry_after,
    )


def _check_batch_jsonb(args_list: list[EnqueueArgs], *, index_base: int = 0) -> None:
    """Serialize every batch item's jsonb-bound values with per-item
    attribution, mirroring the PG tier's build loop.

    Same helpers, so the same annotated PayloadValidationError (item
    index, actor, field) and the same NUL_JSONB_ERROR wording; tags
    included because the PG batch path binds them through jsonb[]
    (see item_tags_jsonb_param). ``index_base`` is the position of
    ``args_list[0]`` in the caller's coordinate space, the PG build
    loop adds the same shift via its ``index_base`` parameter, and the
    mirror's atomic chunk loop passes the consumed prefix so both
    backends annotate at identical STREAM-GLOBAL indices.
    """
    for idx, args in enumerate(args_list):
        item_jsonb_param(args.payload, idx=index_base + idx, field="payload", actor=args.actor)
        item_jsonb_param(args.metadata, idx=index_base + idx, field="metadata", actor=args.actor)
        item_tags_jsonb_param(args.tags, idx=index_base + idx, actor=args.actor)


async def _batch_cap_refusals(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
) -> list[MaxPendingExceededError]:
    """Compute the per-actor cap refusals for a batch without raising.

    Same effective-cap rule as the PG tier: a registered operator
    override (``_actor_configs_meta``) wins over the carried literal,
    cleared/unknown falls back to it. Idempotency pairs already stored
    (or repeated in-batch) are discounted, they dedupe instead of
    writing, mirroring the PG tier's ``ON CONFLICT`` discount. Returns
    one :class:`MaxPendingExceededError` per over-cap actor; the caller
    decides partition-vs-abort (see ``_enqueue_batch``).
    """
    counts = batch_cap_groups(args_list)
    deduped_counts: dict[str, int] = {}
    seen_in_batch: set[tuple[str, str]] = set()
    for args in args_list:
        if args.max_pending is None or args.idempotency_key is None:
            continue
        pair = (args.idempotency_scope, str(args.idempotency_key))
        # Counted per item: a set would collapse repeats of one pair
        # and under-discount.
        if pair in self._idempotency_index or pair in seen_in_batch:
            deduped_counts[args.actor] = deduped_counts.get(args.actor, 0) + 1
        seen_in_batch.add(pair)
    refusals: list[MaxPendingExceededError] = []
    for actor, (batch_count, carried) in counts.items():
        stored = self._actor_configs_meta.get(actor)
        cap = (
            stored.max_pending if stored is not None and stored.max_pending is not None else carried
        )
        existing = sum(
            1
            for row in self._jobs.values()
            if row.actor == actor and row.status in ("pending", "scheduled")
        )
        admitted = batch_count - deduped_counts.get(actor, 0)
        if existing + admitted > cap:
            # Why log + metric here (parity with the PG tier's
            # _batch_cap_refusals, which does both before appending, and
            # through it with the single path): a partitioned bulk
            # refusal is a producer-pressure event per refused actor, not
            # per item. A mirror that refused silently would ship an app
            # with blank backpressure dashboards in production: an
            # operator's dashboards read this warning and this counter.
            logger.warning(
                "max-pending-exceeded",
                actor=actor,
                current_count=existing,
                max_pending=cap,
            )
            record_backpressure_error(actor, kind="max_pending")
            refusals.append(
                MaxPendingExceededError(
                    actor=actor,
                    current_count=existing,
                    max_pending=cap,
                )
            )
    return refusals


async def _enqueue_batch_fast(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
    *,
    connection: object = None,
    enforce_max_pending: bool = True,
) -> int:
    if not args_list:
        raise ValueError("args_list must not be empty")
    # COPY has no ON CONFLICT arbiter: any duplicate idempotency key ,
    # within the batch or already stored, aborts the ENTIRE batch on PG
    # (a violation of jobs_idempotency_scope_key_uniq; nothing is
    # written).  Mirror that here instead of silently deduplicating
    # item-by-item, which reported a count that included rows PG would
    # never have written (protocol parity; see
    # Backend.enqueue_batch_fast's docstring). The mirror raises the
    # SAME typed classification the PG COPY path gives
    # (DuplicateIdempotencyKeyError, not a raw asyncpg violation), and
    # names the offending pair through the SAME shared rule
    # (first_duplicate_idempotency_pair: first item in batch order whose
    # pair repeats an earlier item or is already stored), so the two
    # backends cannot drift on which pair gets named.
    from taskq.exceptions import DuplicateIdempotencyKeyError

    # Why this check ORDER: PG's fast path surfaces defects build-loop
    # NUL guard → pre-COPY cap count → COPY duplicate violation, so a
    # multi-defect batch raises PayloadValidationError (or the cap
    # refusal) there, the duplicate is never reached. The mirror checks
    # in the same order so the same batch raises the same typed error on
    # both backends; checking duplicates first made a NUL+duplicate
    # batch raise DuplicateIdempotencyKeyError in memory while PG raised
    # PayloadValidationError.
    _check_batch_jsonb(args_list)
    refusals: list[MaxPendingExceededError] = []
    refused_indices: dict[str, list[int]] = {}
    admitted_args = args_list
    if enforce_max_pending:
        # Per-actor partition parity with the PG fast tier (see
        # _enqueue_batch above and PostgresBackend.enqueue_batch_fast):
        # over-cap actors' items are refused as a whole group, the rest
        # COPY through, and the typed refusal raises AFTER the admitted
        # rows are stored.
        refusals = await _batch_cap_refusals(self, args_list)
        if refusals:
            refused_indices = {
                r.actor: [i for i, a in enumerate(args_list) if a.actor == r.actor]
                for r in refusals
            }
            refused_names = {r.actor for r in refusals}
            admitted_args = [a for a in args_list if a.actor not in refused_names]
    # Only the ADMITTED items' pairs can violate: the PG COPY contains
    # only admitted records, so an in-batch or stored duplicate among
    # refused items never aborts it.
    duplicate_pair = first_duplicate_idempotency_pair(admitted_args, self._idempotency_index.keys())
    if duplicate_pair is not None:
        # A pair held by ANOTHER actor is the cross-actor misuse the single
        # and batch tiers refuse, classified by the same shared rule the
        # COPY tier applies (duplicate_pair_actor_mismatch) so both
        # backends name the same actors for the same batch.
        from taskq.backend._enqueue import _raise_batch_fast_actor_mismatch

        stored_id = self._idempotency_index.get(duplicate_pair)
        stored_row = self._jobs.get(stored_id) if stored_id is not None else None
        mismatch = duplicate_pair_actor_mismatch(
            admitted_args,
            duplicate_pair,
            stored_row.actor if stored_row is not None else None,
        )
        if mismatch is not None:
            _raise_batch_fast_actor_mismatch(
                mismatch,
                idempotency_scope=duplicate_pair[0],
                idempotency_key=duplicate_pair[1],
                existing_job_id=stored_row.id if stored_row is not None else None,
                batch_size=len(args_list),
            )
        logger.info(
            "batch-fast-duplicate-idempotency-key",
            batch_size=len(args_list),
            idempotency_key=duplicate_pair[1],
            idempotency_scope=duplicate_pair[0],
        )
        raise DuplicateIdempotencyKeyError(
            idempotency_key=duplicate_pair[1],
            idempotency_scope=duplicate_pair[0],
        )
    # Insert the admitted subset with cap enforcement off: the partition
    # above is this tier's only admission decision, and re-running the
    # aggregate inside _enqueue_batch would refuse again (it is not the
    # no-op it was pre-partition). An empty admitted list skips storage
    # entirely, the boundary raise below is the whole outcome, matching
    # the PG fast tier's "no COPY, no fixup, no notify" arm.
    rows: list[JobRow] = []
    if admitted_args:
        rows = await _enqueue_batch(self, admitted_args, enforce_max_pending=False)
    if refusals:
        raise BatchMaxPendingExceededError(
            refusals=refusals,
            refused_indices=refused_indices,
            admitted_count=len(rows),
        )
    return len(rows)
