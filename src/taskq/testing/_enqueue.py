"""Enqueue operations for InMemoryBackend.

``enqueue``, ``enqueue_with_conn``, ``enqueue_batch``, and
``enqueue_batch_fast`` live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter.
"""

from typing import TYPE_CHECKING

import structlog

from taskq._json import dumps_jsonb_str, loads
from taskq.backend._protocol import (
    CancelPhase,
    EnqueueArgs,
    JobRow,
    batch_cap_groups,
)
from taskq.backend._records import item_jsonb_param, item_tags_jsonb_param
from taskq.exceptions import (
    MaxPendingExceededError,
    SingletonCollisionError,
)
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
            logger.info(
                "job_enqueue_deduplicated",
                kind="job_enqueue_deduplicated",
                job_id=str(existing_row.id),
                actor=existing_row.actor,
                queue=existing_row.queue,
                identity_key=existing_row.identity_key,
                idempotency_key=None,
                existing_job_id=str(existing_row.id),
                dedup_reason="unique_for",
            )
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
    # INSERT time — after the preflights above, before any idempotency
    # conflict resolution — and rejects a NUL there. Mirror that exact
    # guard (same function, so the same error) here: without it a payload
    # InMemory accepted raised ValueError on the first real PG enqueue, so
    # an app validated against InMemory broke in production.  The guard
    # lives in this mirror, NOT in EnqueueArgs._check_no_nul_text, because
    # a struct-level check would double-scan the PG hot path, which
    # already guards at bind time.  The guard's serialization is also
    # what PG stores: the jsonb column holds the orjson text and reads it
    # back through loads, so the stored values are its round-trip —
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
                logger.info(
                    "job_enqueue_deduplicated",
                    kind="job_enqueue_deduplicated",
                    job_id=str(existing_row.id),
                    actor=existing_row.actor,
                    queue=existing_row.queue,
                    identity_key=existing_row.identity_key,
                    idempotency_key=existing_row.idempotency_key,
                    existing_job_id=str(existing_row.id),
                    dedup_reason="idempotency_key",
                )
                return _read_copy(existing_row)

    now = self._clock.now()
    # None means immediate: stamp from this backend's own (single-domain)
    # clock — the InMemory mirror of the server's COALESCE stamp.
    stamped_scheduled_at = args.scheduled_at if args.scheduled_at is not None else now
    status: object = "pending" if stamped_scheduled_at <= now else "scheduled"

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
    )

    self._jobs[args.id] = row

    if args.idempotency_key is not None:
        self._idempotency_index[(args.idempotency_scope, args.idempotency_key)] = args.id

    for event in self._wake_subscribers:
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
    if enforce_max_pending:
        # All-or-nothing parity with the PG bulk tier: one aggregated
        # pre-check (existing + batch per actor) before any insert, so a
        # violating batch raises having written nothing — matching what
        # PostgresBackend.enqueue_batch enforces. Without this the
        # per-item loop below rejects mid-batch (partial admission),
        # masking PG behavior in tests that run against this mirror.
        await _check_batch_max_pending(self, args_list)
    rows: list[JobRow] = []
    for args in args_list:
        row = await _enqueue(self, args)
        rows.append(row)
    return rows


def _check_batch_jsonb(args_list: list[EnqueueArgs]) -> None:
    """Serialize every batch item's jsonb-bound values with per-item
    attribution, mirroring the PG tier's build loop.

    Same helpers, so the same annotated PayloadValidationError (item
    index, actor, field) and the same NUL_JSONB_ERROR wording; tags
    included because the PG batch path binds them through jsonb[]
    (see item_tags_jsonb_param).
    """
    for idx, args in enumerate(args_list):
        item_jsonb_param(args.payload, idx=idx, field="payload", actor=args.actor)
        item_jsonb_param(args.metadata, idx=idx, field="metadata", actor=args.actor)
        item_tags_jsonb_param(args.tags, idx=idx, actor=args.actor)


async def _check_batch_max_pending(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
) -> None:
    """Aggregated max_pending pre-check shared by the batch mirrors.

    Same effective-cap rule as the PG tier: a registered operator
    override (``_actor_configs_meta``) wins over the carried literal,
    cleared/unknown falls back to it. Idempotency pairs already stored
    (or repeated in-batch) are discounted — they dedupe instead of
    writing — mirroring the PG tier's ``ON CONFLICT`` discount.
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
            raise MaxPendingExceededError(
                actor=actor,
                current_count=existing,
                max_pending=cap,
            )


async def _enqueue_batch_fast(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
    *,
    connection: object = None,
    enforce_max_pending: bool = True,
) -> int:
    if not args_list:
        raise ValueError("args_list must not be empty")
    # COPY has no ON CONFLICT arbiter: any duplicate idempotency key —
    # within the batch or already stored — aborts the ENTIRE batch on PG
    # (a violation of jobs_idempotency_scope_key_uniq; nothing is
    # written).  Mirror that here instead of silently deduplicating
    # item-by-item, which reported a count that included rows PG would
    # never have written (protocol parity; see
    # Backend.enqueue_batch_fast's docstring). The mirror raises the
    # SAME typed classification the PG COPY path now gives
    # (DuplicateIdempotencyKeyError, not a raw asyncpg violation) — and
    # names the offending pair exactly, since the detecting loop knows
    # it (the PG path best-effort matches the violation's detail line
    # against the batch's candidates).
    from taskq.exceptions import DuplicateIdempotencyKeyError

    # Why this check ORDER: PG's fast path surfaces defects build-loop
    # NUL guard → pre-COPY cap count → COPY duplicate violation, so a
    # multi-defect batch raises PayloadValidationError (or the cap
    # refusal) there — the duplicate is never reached. The mirror checks
    # in the same order so the same batch raises the same typed error on
    # both backends; checking duplicates first made a NUL+duplicate
    # batch raise DuplicateIdempotencyKeyError in memory while PG raised
    # PayloadValidationError.
    _check_batch_jsonb(args_list)
    if enforce_max_pending:
        await _check_batch_max_pending(self, args_list)
    seen: set[tuple[str, str]] = set()
    for args in args_list:
        if args.idempotency_key is None:
            continue
        pair = (args.idempotency_scope, str(args.idempotency_key))
        if pair in seen or pair in self._idempotency_index:
            logger.info(
                "batch-fast-duplicate-idempotency-key",
                batch_size=len(args_list),
                idempotency_key=pair[1],
                idempotency_scope=pair[0],
            )
            raise DuplicateIdempotencyKeyError(
                idempotency_key=pair[1],
                idempotency_scope=pair[0],
            )
        seen.add(pair)
    # The insert loop re-runs the two preflights inside _enqueue_batch —
    # pure reads, already passed above, deterministically no-ops here.
    rows = await _enqueue_batch(self, args_list, enforce_max_pending=enforce_max_pending)
    return len(rows)
