"""Batch operations for InMemoryBackend.

All 10 batch protocol methods live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter, following the companion-
module pattern (``_enqueue.py``, ``_terminal.py``, etc.).
"""

from collections.abc import Iterable
from dataclasses import replace
from datetime import datetime
from itertools import islice
from typing import TYPE_CHECKING, get_args
from uuid import UUID

from taskq.backend._cursor import decode_batch_cursor
from taskq.backend._protocol import (
    BatchCounts,
    BatchFilter,
    BatchRow,
    BatchStatus,
    CancelPhase,
    EnqueueArgs,
    JobId,
    JobRow,
)
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import DEFAULT_CHUNK_SIZE, ERROR_CLASS_BATCH_ABORTED
from taskq.testing._enqueue import _check_batch_jsonb, _rollback_inserted_rows
from taskq.testing._reads import _batch_row_read_copy

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_abort_batch",
    "_complete_batch",
    "_count_batch_non_terminal",
    "_create_batch",
    "_enqueue_batch_atomic",
    "_get_batch",
    "_increment_batch_failures",
    "_list_batches",
    "_prune_old_batches",
    "_reset_batch_failures",
]

_BATCH_STATUSES = frozenset(get_args(BatchStatus.__value__))
_BATCH_TERMINAL_STATUSES: frozenset[str] = _BATCH_STATUSES - {"active"}


def _batch_counts_for(backend: "InMemoryBackend", batch_id: UUID) -> BatchCounts:
    batch_id_str = str(batch_id)
    total = pending = succeeded = failed = cancelled = crashed = abandoned = 0
    for r in backend._jobs.values():
        if r.metadata.get("batch_id") != batch_id_str:
            continue
        total += 1
        if r.status == "succeeded":
            succeeded += 1
        elif r.status == "failed":
            failed += 1
        elif r.status == "cancelled":
            cancelled += 1
        elif r.status == "crashed":
            crashed += 1
        elif r.status == "abandoned":
            abandoned += 1
        else:
            pending += 1
    return BatchCounts(
        total=total,
        pending=pending,
        succeeded=succeeded,
        failed=failed,
        cancelled=cancelled,
        crashed=crashed,
        abandoned=abandoned,
    )


def _create_batch(
    backend: "InMemoryBackend",
    batch_id: UUID,
    queue: str,
    expected_size: int,
    failure_threshold: int | None,
    finalizer_job_id: UUID | None,
    originating_actor: str | None,
    connection: object | None = None,
) -> None:
    if batch_id in backend._batches:
        from taskq.exceptions import BatchIdExistsError

        raise BatchIdExistsError(batch_id)
    row = BatchRow(
        id=batch_id,
        queue=queue,
        status="active",
        expected_size=expected_size,
        consecutive_failures=0,
        failure_threshold=failure_threshold,
        finalizer_job_id=finalizer_job_id,
        originating_actor=originating_actor,
        created_at=backend._clock.now(),
        completed_at=None,
        metadata={},
    )
    backend._batches[batch_id] = row


def _get_batch(backend: "InMemoryBackend", batch_id: UUID) -> BatchRow | None:
    row = backend._batches.get(batch_id)
    return None if row is None else _batch_row_read_copy(row)


def _increment_batch_failures(
    backend: "InMemoryBackend",
    batch_id: UUID,
    connection: object | None = None,
) -> tuple[int, int | None, int]:
    row = backend._batches.get(batch_id)
    if row is None:
        return (0, None, 0)
    if row.status != "active":
        return (0, None, 0)

    new_count = row.consecutive_failures + 1
    backend._batches[batch_id] = replace(row, consecutive_failures=new_count)

    remaining = _count_batch_non_terminal(backend, batch_id)
    return (new_count, row.failure_threshold, remaining)


def _reset_batch_failures(
    backend: "InMemoryBackend",
    batch_id: UUID,
    connection: object | None = None,
) -> int:
    row = backend._batches.get(batch_id)
    if row is None:
        return 0
    if row.status != "active":
        return 0

    backend._batches[batch_id] = replace(row, consecutive_failures=0)
    return _count_batch_non_terminal(backend, batch_id)


def _abort_batch(
    backend: "InMemoryBackend",
    batch_id: UUID,
    connection: object | None = None,
) -> int:
    batch_id_str = str(batch_id)
    now = backend._clock.now()
    cancelled = 0

    for job_id, row in list(backend._jobs.items()):
        if row.metadata.get("batch_id") == batch_id_str and row.status in ("pending", "scheduled"):
            backend._jobs[job_id] = replace(
                row,
                status="cancelled",
                finished_at=now,
                error_class=ERROR_CLASS_BATCH_ABORTED,
                error_message="Batch aborted due to consecutive failures",
                cancel_requested_at=now,
                cancel_phase=CancelPhase.FORCED,
            )
            cancelled += 1

    batch_row = backend._batches.get(batch_id)
    if batch_row is not None and batch_row.status == "active":
        backend._batches[batch_id] = replace(
            batch_row,
            status="aborted",
            completed_at=now,
        )

    return cancelled


def _complete_batch(
    backend: "InMemoryBackend",
    batch_id: UUID,
    connection: object | None = None,
) -> None:
    row = backend._batches.get(batch_id)
    if row is None or row.status in _BATCH_TERMINAL_STATUSES:
        return

    # Mirror of the PG NOT EXISTS guard: the completion decision reads
    # the live member set at the moment of the write, never a count the
    # caller computed earlier. Under READ COMMITTED a count statement's
    # snapshot can predate a concurrent member's terminal write, so an
    # over-counted "members remain" must delay completion (the stale-
    # batch sweep is the safety net) while the attempt itself stays safe
    # to issue after any terminal outcome, the same call that was
    # vetoed lands once the last member turns terminal.
    if _count_batch_non_terminal(backend, batch_id) > 0:
        return

    backend._batches[batch_id] = replace(
        row,
        status="complete",
        completed_at=backend._clock.now(),
    )


def _count_batch_non_terminal(backend: "InMemoryBackend", batch_id: UUID) -> int:
    batch_id_str = str(batch_id)
    return sum(
        1
        for r in backend._jobs.values()
        if r.metadata.get("batch_id") == batch_id_str and r.status not in TERMINAL_STATUSES
    )


def _list_batches(
    backend: "InMemoryBackend",
    filter: BatchFilter,
) -> list[tuple[BatchRow, BatchCounts]]:
    candidates = list(backend._batches.values())

    if filter.queue is not None:
        candidates = [b for b in candidates if b.queue == filter.queue]
    if filter.active is not None:
        if filter.active:
            candidates = [b for b in candidates if b.status == "active"]
        else:
            candidates = [b for b in candidates if b.status in _BATCH_TERMINAL_STATUSES]
    if filter.batch_id is not None:
        candidates = [b for b in candidates if b.id == filter.batch_id]

    # Mirror of the PG keyset: (created_at, id) DESC as one total order,
    # with id -- UUIDv7, so time-ordered -- carrying the rows a FakeClock
    # (or PG's transaction-timestamp now()) stamps identically.
    candidates.sort(key=lambda b: (b.created_at, b.id), reverse=True)

    if filter.cursor is not None:
        cursor_key = decode_batch_cursor(filter.cursor)
        candidates = [b for b in candidates if (b.created_at, b.id) < cursor_key]

    candidates = candidates[: filter.limit]

    return [(_batch_row_read_copy(b), _batch_counts_for(backend, b.id)) for b in candidates]


async def _enqueue_batch_atomic(
    backend: "InMemoryBackend",
    items: Iterable[EnqueueArgs],
    *,
    batch_id: UUID,
    queue: str,
    batch_row: BatchRow | None,
    finalizer_args: EnqueueArgs | None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> list[JobRow]:
    batch_id_str = str(batch_id)
    rows: list[JobRow] = []
    inserted: list[tuple[JobId, tuple[str, str] | None]] = []
    item_count = 0
    batch_created = False

    try:
        # PG-tier parity for the chunked consumption + per-item failure
        # coordinates: the PG atomic arm re-chunks the stream inside the
        # backend and each chunk crosses the bulk build loop, whose
        # per-item jsonb NUL guard annotates at index_base + the chunk
        # position, i.e. STREAM-GLOBAL indices, BEFORE the chunk's
        # INSERT. The mirror consumes the same chunks (same islice, same
        # chunk_size) and preflights each stamped chunk through the same
        # shared guards at the same base, so a NUL-bearing item surfaces
        # as the SAME annotated PayloadValidationError naming the SAME
        # caller-global index on both backends, previously this arm
        # surfaced a BARE ValueError(NUL_JSONB_ERROR) with no item
        # attribution at all (the per-item single-enqueue path has no
        # index to name). Check ORDER also matches PG within a chunk
        # (NUL guard precedes the cap/insert decisions), so a
        # multi-defect batch raises the same typed error on both sides.
        it = iter(items)
        while True:
            chunk_raw = list(islice(it, chunk_size))
            if not chunk_raw:
                break
            chunk_base = item_count
            item_count += len(chunk_raw)
            chunk = [
                replace(
                    args,
                    metadata={**args.metadata, "batch_id": batch_id_str},
                )
                for args in chunk_raw
            ]
            _check_batch_jsonb(chunk, index_base=chunk_base)
            for args in chunk:
                # The compensating rollback (``_rollback_inserted_rows``,
                # shared with the batch arm) may only withdraw rows THIS
                # call inserted. The discriminator is the insert contract
                # documented on ``_enqueue_with_conn``: an item stored a
                # row iff its id was absent before its enqueue and present
                # after, so a dedup hit (which returns a holder row and
                # stores nothing) never rides the rollback.
                pair = (
                    (args.idempotency_scope, args.idempotency_key)
                    if args.idempotency_key is not None
                    else None
                )
                pre_existing = args.id in backend._jobs
                row = await backend.enqueue_with_conn(None, args)
                rows.append(row)
                if not pre_existing and args.id in backend._jobs:
                    inserted.append((row.id, pair))

        # Insert finalizer BEFORE creating the batch row so the returned
        # row's id can be used for finalizer_job_id (M4: idempotency
        # collision may return a different id than finalizer_args.id).
        finalizer_row: JobRow | None = None
        if finalizer_args is not None:
            # Same preflight at the finalizer's caller-global coordinate
            # (one past the last stream item, the PG arm passes the same
            # index_base for its finalizer chunk); without it a NUL in
            # the finalizer's payload surfaced as the bare ValueError
            # while PG raised the annotated PayloadValidationError.
            _check_batch_jsonb([finalizer_args], index_base=item_count)
            # Same insert-contract discriminator as the chunk loop above:
            # a dedup hit on the finalizer's pair (M4: the collision may
            # return a different id than finalizer_args.id) returns a
            # pre-existing holder row the rollback must never pop.
            pair = (
                (finalizer_args.idempotency_scope, finalizer_args.idempotency_key)
                if finalizer_args.idempotency_key is not None
                else None
            )
            pre_existing = finalizer_args.id in backend._jobs
            row = await backend.enqueue_with_conn(None, finalizer_args)
            rows.append(row)
            if not pre_existing and finalizer_args.id in backend._jobs:
                inserted.append((row.id, pair))
            finalizer_row = row

        if batch_row is not None:
            # H6: when expected_size is 0 (streaming sentinel), use the
            # actual item count consumed from the iterable.
            expected_size = batch_row.expected_size if batch_row.expected_size > 0 else item_count
            finalizer_job_id = (
                finalizer_row.id if finalizer_row is not None else batch_row.finalizer_job_id
            )
            _create_batch(
                backend,
                batch_id,
                queue,
                expected_size,
                batch_row.failure_threshold,
                finalizer_job_id,
                batch_row.originating_actor,
                None,
            )
            batch_created = True
    except BaseException:
        _rollback_inserted_rows(backend, inserted)
        if batch_created:
            backend._batches.pop(batch_id, None)
        raise

    return rows


def _prune_old_batches(
    backend: "InMemoryBackend",
    cutoff: datetime,
) -> int:
    to_delete: list[UUID] = []
    batch_ids_with_jobs: set[str] = set()

    for row in backend._jobs.values():
        batch_id_val = row.metadata.get("batch_id")
        if isinstance(batch_id_val, str):
            batch_ids_with_jobs.add(batch_id_val)

    for batch_id, batch_row in backend._batches.items():
        if (
            batch_row.completed_at is not None
            and batch_row.completed_at < cutoff
            and str(batch_id) not in batch_ids_with_jobs
        ):
            to_delete.append(batch_id)

    for bid in to_delete:
        del backend._batches[bid]

    return len(to_delete)
