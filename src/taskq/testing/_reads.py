"""Read operations for InMemoryBackend.

``get``, ``list_jobs``, ``count_pending_jobs``, ``get_attempts``, and
``get_events`` live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter.

``get`` additionally applies the PG result-TTL sweep predicate
(:data:`taskq.backend._sweeps._SWEEP_RESULT_TTL_SQL`) against the injected
Clock, so a read past ``result_expires_at`` deterministically reports the
result as unavailable. ``list_jobs`` does not — it exposes raw stored rows,
matching PostgresBackend's pre-sweep list behaviour.
"""

from dataclasses import replace
from datetime import datetime, timedelta
from functools import cmp_to_key
from typing import TYPE_CHECKING

from taskq.backend._cursor import ordering_for
from taskq.backend._protocol import (
    AttemptRow,
    EventRow,
    JobFilter,
    JobId,
    JobRow,
)
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES
from taskq.constants import DEFAULT_RECLAIM_POLL_LIMIT

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_count_pending_jobs",
    "_get",
    "_get_actor_max_pending",
    "_get_attempts",
    "_get_events",
    "_list_jobs",
    "_poll_reclaim_events",
    "_post_sweep_result_view",
]


def _post_sweep_result_view(row: JobRow, now: datetime) -> JobRow:
    """Return *row* as the PG result-TTL sweep would have left it.

    PostgresBackend nulls an expired result via the leader-only
    ``sweep_expired_results`` — but only when that sweep happens to fire,
    so a read landing between expiry and the next sweep still observes the
    result. The in-memory backend has no leader loop to schedule, so
    ``get`` evaluates the same predicate against the injected Clock on
    every read: a row past its result TTL reads back in the exact
    post-sweep shape, deterministically.

    Predicate parity with ``_SWEEP_RESULT_TTL_SQL``, field for field: the
    comparison is strictly ``<`` (a read at exactly ``result_expires_at``
    still sees the result), and a row whose ``result`` is already ``None``
    is untouched. Only ``result`` / ``result_size_bytes`` /
    ``result_expires_at`` are nulled — the stored row is never mutated,
    only the returned copy — so status and every other column (terminal
    ones included) pass through unchanged, and an expired result can
    neither revive nor alter a terminal state. Downstream code that
    handles :class:`~taskq.exceptions.ResultUnavailable` from
    ``JobHandle.wait`` therefore sees the same failure mode on both
    backends.
    """
    if row.result is not None and row.result_expires_at is not None and row.result_expires_at < now:
        return replace(row, result=None, result_size_bytes=None, result_expires_at=None)
    return row


async def _get(self: "InMemoryBackend", job_id: JobId) -> JobRow | None:
    row = self._jobs.get(job_id)
    if row is None:
        return None
    return _post_sweep_result_view(row, self._clock.now())


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

    # One descriptor drives the sort and the cursor seam on both backends
    # (``taskq.backend._cursor``), so the in-memory mirror cannot order
    # rows one way while comparing the cursor another.
    ordering = ordering_for(filters.order_by)
    candidates.sort(key=cmp_to_key(ordering.compare_rows))

    if filters.cursor is not None:
        cursor_values = ordering.decode(filters.cursor)
        candidates = [
            r for r in candidates if ordering.compare(ordering.values(r), cursor_values) > 0
        ]

    return candidates[: filters.limit]


async def _count_pending_jobs(self: "InMemoryBackend", actors: list[str]) -> dict[str, int]:
    actor_set = set(actors)
    counts: dict[str, int] = {}
    for row in self._jobs.values():
        if row.actor in actor_set and row.status in ("pending", "scheduled"):
            counts[row.actor] = counts.get(row.actor, 0) + 1
    return counts


async def _get_actor_max_pending(self: "InMemoryBackend") -> dict[str, int | None]:
    """Mirror of the PG whole-table snapshot: registered actor_config
    meta plays the role of stored rows, including the NULL case."""
    return {actor: cfg.max_pending for actor, cfg in self._actor_configs_meta.items()}


async def _get_attempts(self: "InMemoryBackend", job_id: JobId) -> list[AttemptRow]:
    return sorted(self._attempts.get(job_id, []), key=lambda a: a.attempt)


async def _get_events(self: "InMemoryBackend", job_id: JobId) -> list[EventRow]:
    from taskq.testing._runner import get_events as _get_events_impl

    return await _get_events_impl(self, job_id)


async def _poll_reclaim_events(
    self: "InMemoryBackend",
    after_id: int,
    limit: int = DEFAULT_RECLAIM_POLL_LIMIT,
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
