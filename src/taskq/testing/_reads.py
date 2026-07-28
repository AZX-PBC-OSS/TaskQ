"""Read operations for InMemoryBackend.

``get``, ``list_jobs``, ``count_pending_jobs``, ``get_attempts``, and
``get_events`` live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter.
"""

from datetime import timedelta
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
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_count_pending_jobs",
    "_get",
    "_get_attempts",
    "_get_events",
    "_list_jobs",
    "_poll_reclaim_events",
]


async def _get(self: "InMemoryBackend", job_id: JobId) -> JobRow | None:
    return self._jobs.get(job_id)


async def _list_jobs(self: "InMemoryBackend", filters: JobFilter) -> list[JobRow]:
    candidates = list(self._jobs.values())

    if filters.queue is not None:
        candidates = [r for r in candidates if r.queue == filters.queue]
    if filters.status is not None:
        if isinstance(filters.status, str):
            candidates = [r for r in candidates if r.status == filters.status]
        else:
            status_set = frozenset(filters.status)
            candidates = [r for r in candidates if r.status in status_set]
    elif filters.active is not None:
        status_set = ACTIVE_STATUSES if filters.active else TERMINAL_STATUSES
        candidates = [r for r in candidates if r.status in status_set]
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
        # PG: ORDER BY created_at DESC, id ASC. A single reverse=True key
        # would invert the id tie-break to DESC, so sort the id ASC
        # tie-breaker first and let sort stability preserve it under the
        # primary DESC sort.
        candidates.sort(key=lambda r: r.id)
        candidates.sort(key=lambda r: r.created_at, reverse=True)
    elif filters.order_by is JobSortField.FINISHED_AT_DESC:
        # PG: ORDER BY finished_at DESC NULLS LAST, id ASC. NULLS LAST for
        # DESC: non-None finished_at sorts before None via the leading bool
        # (True > False under reverse=True); finished_at is only compared
        # when both rows share the same bool, so None-vs-None never reaches
        # an ordered comparison. Same two-pass idiom as CREATED_AT_DESC for
        # the id ASC tie-break.
        candidates.sort(key=lambda r: r.id)
        candidates.sort(
            key=lambda r: (r.finished_at is not None, r.finished_at),
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


async def _poll_reclaim_events(
    self: "InMemoryBackend",
    after_id: int,
    limit: int = 100,
    *,
    visibility_delay: timedelta | None = None,
) -> list[EventRow]:
    """InMemoryBackend is single-threaded and synchronous, so ``event_id``
    order already equals insertion order — there is no concurrent-commit
    race for a *visibility_delay* to guard against here, unlike
    PostgresBackend (see ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``).
    The parameter is accepted and ignored purely so callers can pass it
    uniformly across both backends.
    """
    return [
        e
        for e in sorted(self._events, key=lambda ev: ev.event_id)
        if e.event_id > after_id
        and e.kind == "state_change"
        and e.detail.get("reason") == "lock_expired"
    ][:limit]
