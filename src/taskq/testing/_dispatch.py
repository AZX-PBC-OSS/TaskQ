"""Dispatch operations for InMemoryBackend.

``dispatch_batch`` lives here as a module-level function taking
``self: InMemoryBackend`` as the first parameter.
"""

from collections import defaultdict as _dd
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.backend._protocol import JobRow, QueueMode

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = ["_dispatch_batch", "_set_queue_mode"]


async def _dispatch_batch(
    self: "InMemoryBackend",
    worker_id: UUID,
    queues: list[str],
    limit: int,
    lock_lease: timedelta,
) -> list[JobRow]:
    now = self._clock.now()

    running_per_actor: dict[str, int] = {}
    running_identities: set[tuple[str, str]] = set()
    for row in self._jobs.values():
        if row.status == "running":
            running_per_actor[row.actor] = running_per_actor.get(row.actor, 0) + 1
            if row.identity_key is not None:
                running_identities.add((row.actor, row.identity_key))

    _has_actor_configs: bool = bool(self._actor_configs_meta)
    candidates = [
        row
        for row in self._jobs.values()
        if row.status == "pending"
        and (not queues or row.queue in queues)
        and row.scheduled_at <= now
        and (row.schedule_to_close is None or row.schedule_to_close > now)
        and (not _has_actor_configs or row.actor in self._actor_configs_meta)
    ]

    _by_actor: dict[str, list[JobRow]] = _dd(list)
    for c in candidates:
        _by_actor[c.actor].append(c)

    _use_round_robin = any(self._queues.get(q) == "round_robin" for q in (queues or []))

    for _rows in _by_actor.values():
        if _use_round_robin:
            _fk_groups: dict[str, list[JobRow]] = _dd(list)
            for r in _rows:
                fk = r.fairness_key if r.fairness_key is not None else f"__null__{r.id}"
                _fk_groups[fk].append(r)
            _fairness_rank: dict[object, int] = {}
            for _fk_rows in _fk_groups.values():
                _fk_rows.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))
                for _rank, _r in enumerate(_fk_rows, 1):
                    _fairness_rank[_r.id] = _rank
            _rows.sort(
                key=lambda r: (
                    _fairness_rank.get(r.id, 0),
                    -r.priority,
                    r.scheduled_at,
                    r.id,
                )
            )
        else:
            _rows.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))
    _ranked_candidates: list[tuple[int, JobRow]] = []
    for _actor_rows in _by_actor.values():
        for _rank, _row in enumerate(_actor_rows, 1):
            _ranked_candidates.append((_rank, _row))
    _by_rank: dict[int, dict[str, list[JobRow]]] = _dd(lambda: _dd(list))
    for _rank, _row in _ranked_candidates:
        _by_rank[_rank][_row.actor].append(_row)
    _interleaved: list[JobRow] = []
    for _rank_val in sorted(_by_rank):
        _actors_at_rank = sorted(_by_rank[_rank_val])
        _remain = True
        _idx = 0
        while _remain:
            _remain = False
            for _actor in _actors_at_rank:
                _actor_jobs = _by_rank[_rank_val][_actor]
                if _idx < len(_actor_jobs):
                    _interleaved.append(_actor_jobs[_idx])
                    _remain = True
            _idx += 1
    candidates = _interleaved

    dispatched_per_actor: dict[str, int] = {}
    newly_dispatched_identities: set[tuple[str, str]] = set()
    dispatched: list[JobRow] = []

    for row in candidates:
        if len(dispatched) >= limit:
            break

        cap: int | None = None
        if _has_actor_configs and row.actor in self._actor_configs_meta:
            cap = self._actor_configs_meta[row.actor].max_concurrent

        per_dispatch_cap = cap if cap is not None else limit
        if dispatched_per_actor.get(row.actor, 0) >= per_dispatch_cap:
            continue

        if cap is not None:
            in_flight = running_per_actor.get(row.actor, 0) + dispatched_per_actor.get(row.actor, 0)
            if in_flight >= cap:
                continue

        if row.identity_key is not None:
            ident = (row.actor, row.identity_key)
            if ident in running_identities or ident in newly_dispatched_identities:
                continue
            newly_dispatched_identities.add(ident)

        dispatched_per_actor[row.actor] = dispatched_per_actor.get(row.actor, 0) + 1

        updated = replace(
            row,
            status="running",
            locked_by_worker=worker_id,
            lock_expires_at=now + lock_lease,
            started_at=now,
            finished_at=None,
            last_heartbeat_at=now,
            error_class=None,
            error_message=None,
            error_traceback=None,
            result=None,
            result_size_bytes=None,
            attempt=row.attempt + 1,
        )
        self._jobs[row.id] = updated
        self._append_state_change_event(
            job_id=row.id,
            from_state="pending",
            to_state="running",
            now=now,
            worker_id=worker_id,
        )
        dispatched.append(updated)

    return dispatched


def _set_queue_mode(self: "InMemoryBackend", queue_name: str, mode: QueueMode) -> None:
    from taskq.testing._runner import set_queue_mode as _set_queue_mode_impl

    _set_queue_mode_impl(self, queue_name, mode)
