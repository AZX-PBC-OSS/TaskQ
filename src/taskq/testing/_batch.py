"""Batch operations for InMemoryBackend.

All 10 batch protocol methods live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter, following the companion-
module pattern (``_enqueue.py``, ``_terminal.py``, etc.).
"""

from collections.abc import Iterable
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.backend._protocol import (
    BatchCounts,
    BatchFilter,
    BatchRow,
    CancelPhase,
    EnqueueArgs,
    JobId,
    JobRow,
)
from taskq.backend.statemachine import TERMINAL_STATUSES

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

_BATCH_TERMINAL_STATUSES: frozenset[str] = frozenset({"aborted", "complete"})


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
    connection: object,
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
    return backend._batches.get(batch_id)


def _increment_batch_failures(
    backend: "InMemoryBackend",
    batch_id: UUID,
    connection: object,
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
    connection: object,
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
    connection: object,
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
                error_class="BatchAbortedError",
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
    connection: object,
) -> None:
    row = backend._batches.get(batch_id)
    if row is None or row.status in _BATCH_TERMINAL_STATUSES:
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

    candidates.sort(key=lambda b: b.created_at, reverse=True)

    candidates = candidates[: filter.limit]

    return [(b, _batch_counts_for(backend, b.id)) for b in candidates]


async def _enqueue_batch_atomic(
    backend: "InMemoryBackend",
    items: Iterable[EnqueueArgs],
    *,
    batch_id: UUID,
    queue: str,
    batch_row: BatchRow | None,
    finalizer_args: EnqueueArgs | None,
    chunk_size: int = 1000,
) -> list[JobRow]:
    batch_id_str = str(batch_id)
    rows: list[JobRow] = []
    inserted_ids: list[JobId] = []
    item_count = 0
    batch_created = False

    try:
        for args in items:
            args_with_batch = replace(
                args,
                metadata={**args.metadata, "batch_id": batch_id_str},
            )
            row = await backend.enqueue_with_conn(None, args_with_batch)
            rows.append(row)
            inserted_ids.append(row.id)
            item_count += 1

        # Insert finalizer BEFORE creating the batch row so the returned
        # row's id can be used for finalizer_job_id (M4: idempotency
        # collision may return a different id than finalizer_args.id).
        finalizer_row: JobRow | None = None
        if finalizer_args is not None:
            row = await backend.enqueue_with_conn(None, finalizer_args)
            rows.append(row)
            inserted_ids.append(row.id)
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
        for jid in inserted_ids:
            backend._jobs.pop(jid, None)
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
