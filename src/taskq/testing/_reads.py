"""Read operations for InMemoryBackend.

``get``, ``list_jobs``, ``count_pending_jobs``, ``get_attempts``, and
``get_events`` live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter.
"""

from typing import TYPE_CHECKING

from taskq.backend._cursor import decode_cursor
from taskq.backend._protocol import (
    AttemptRow,
    EventRow,
    JobFilter,
    JobId,
    JobRow,
    JobSortField,
)

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_count_pending_jobs",
    "_get",
    "_get_attempts",
    "_get_events",
    "_list_jobs",
]


async def _get(self: "InMemoryBackend", job_id: JobId) -> JobRow | None:
    return self._jobs.get(job_id)


async def _list_jobs(self: "InMemoryBackend", filters: JobFilter) -> list[JobRow]:
    candidates = list(self._jobs.values())

    if filters.queue is not None:
        candidates = [r for r in candidates if r.queue == filters.queue]
    if filters.status is not None:
        candidates = [r for r in candidates if r.status == filters.status]
    if filters.actor is not None:
        candidates = [r for r in candidates if r.actor == filters.actor]
    if filters.identity_key is not None:
        candidates = [r for r in candidates if r.identity_key == filters.identity_key]
    if filters.batch_id is not None:
        batch_id_str = str(filters.batch_id)
        candidates = [r for r in candidates if r.metadata.get("batch_id") == batch_id_str]

    if filters.tags is not None and len(filters.tags) > 0:
        filter_tags = set(filters.tags)
        candidates = [r for r in candidates if filter_tags & set(r.tags)]

    if filters.order_by is JobSortField.CREATED_AT_DESC:
        candidates.sort(key=lambda r: (r.created_at, r.id), reverse=True)
    elif filters.order_by is JobSortField.FINISHED_AT_DESC:
        # NULLS LAST for DESC: non-None finished_at sorts before None via the
        # leading bool (True > False under reverse=True); finished_at is only
        # compared when both rows share the same bool, so None-vs-None never
        # reaches an ordered comparison.
        candidates.sort(
            key=lambda r: (r.finished_at is not None, r.finished_at, r.id),
            reverse=True,
        )
    else:
        candidates.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))

    if filters.cursor is not None:
        cursor_priority, cursor_scheduled_at, cursor_id = decode_cursor(filters.cursor)
        start_idx = 0
        for i, r in enumerate(candidates):
            key = (-r.priority, r.scheduled_at, r.id)
            cursor_key = (-cursor_priority, cursor_scheduled_at, cursor_id)
            if key > cursor_key:
                start_idx = i
                break
        else:
            return []
        candidates = candidates[start_idx:]

    return candidates[: filters.limit]


async def _count_pending_jobs(self: "InMemoryBackend", actors: list[str]) -> dict[str, int]:
    actor_set = set(actors)
    counts: dict[str, int] = {}
    for row in self._jobs.values():
        if row.actor in actor_set and row.status in ("pending", "scheduled"):
            counts[row.actor] = counts.get(row.actor, 0) + 1
    return counts


async def _get_attempts(self: "InMemoryBackend", job_id: JobId) -> list[AttemptRow]:
    return sorted(self._attempts.get(job_id, []), key=lambda a: a.attempt)


async def _get_events(self: "InMemoryBackend", job_id: JobId) -> list[EventRow]:
    from taskq.testing._runner import get_events as _get_events_impl

    return await _get_events_impl(self, job_id)
