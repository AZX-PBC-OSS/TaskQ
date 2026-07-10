"""Shared state for the maintenance leader: sweep context, constants, and
prune/archive-expiry primitives.

Canonical home for everything ``leader.py`` and ``_leader_sweeps.py`` both
need, so neither module has to reach into the other. ``leader.py`` imports
from here to build its public re-exports; ``_leader_sweeps.py`` imports from
here for the sweep loops' shared helpers and SQL. This module must not import
from either of them.
"""

import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog

from taskq.backend._protocol import Backend, ConnLike
from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.obs import (
    get_logger,
    get_meter,
    record_archived_jobs,
    record_expired_archive_jobs,
    record_pruned_jobs,
)
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps

__all__ = [
    "ARCHIVE_EXPIRY_LOCK_NAME",
    "PRUNE_LOCK_NAME",
    "ArchiveExpiryResult",
    "PruneResult",
    "SweepContext",
    "archive_expiry_sweep",
    "cleanup_stale_workers",
    "prune_terminal_jobs",
]

log: structlog.stdlib.BoundLogger = get_logger(__name__)
_meter = get_meter()

PRUNE_LOCK_NAME: str = "taskq:prune"
ARCHIVE_EXPIRY_LOCK_NAME: str = "taskq:archive_expiry"

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
)

_EK1 = "scheduled_wake_backend_unimplemented"
_EK2 = "sweep_expired_locks_backend_unimplemented"
_EK3 = "sweep_deadline_exceeded_backend_unimplemented"

_sweep_duration_hist = _meter.create_histogram(
    name="taskq.maintenance_leader.sweep_duration_ms",
    unit="ms",
    description="Per-sweep-tick wall-clock duration in milliseconds.",
)
_sweep_rows_counter = _meter.create_counter(
    name="taskq.maintenance_leader.sweep_rows",
    description="Rows affected per sweep tick, with sweep_name label.",
)


def _metric(name: str, count: int, start: float) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    elapsed = (time.monotonic() - start) * 1000.0
    _sweep_rows_counter.add(count, {"sweep_name": name})
    _sweep_duration_hist.record(elapsed, {"sweep_name": name})


def _dbg(ev: str, ki: str, co: int, st: float) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    log.debug(ev, kind=ki, rows_affected=co, duration_ms=int((time.monotonic() - st) * 1000))


def _err(ev: str, ki: str, wi: UUID, ex: Exception) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    log.error(ev, kind=ki, worker_id=str(wi), error=repr(ex))


@dataclass(frozen=True, slots=True)
class SweepContext:
    """The subset of ``MaintenanceLeader`` state the sweep loops need.

    Built once by ``MaintenanceLeader`` and passed into the module-level
    sweep-loop functions in ``_leader_sweeps.py`` so those functions do not
    depend on the ``MaintenanceLeader`` type (which would reintroduce the
    circular import this module exists to avoid).
    """

    deps: WorkerDeps
    backend: Backend
    clock: Clock
    worker_id: UUID


_HHMM_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def _schedule_utc_to_cron(hhmm: str) -> str:  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    m = _HHMM_RE.match(hhmm)
    if m is None:
        raise ValueError(f"invalid HH:MM schedule: {hhmm!r}")
    minute, hour = int(m.group(2)), int(m.group(1))
    return f"{minute} {hour} * * *"


def _build_retention_per_status(  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    settings: WorkerSettings,
) -> dict[str, timedelta]:
    return {
        "succeeded": settings.prune_retention_succeeded,
        "failed": settings.prune_retention_failed,
        "cancelled": settings.prune_retention_cancelled,
        "crashed": settings.prune_retention_abandoned,
        "abandoned": settings.prune_retention_abandoned,
    }


_ACTOR_RETENTION_SQL = (
    "SELECT actor, (metadata->>'retention_days')::int AS retention_days "
    'FROM "{schema}".actor_config '
    "WHERE metadata ? 'retention_days' "
    "AND (metadata->>'retention_days') ~ '^\\d+$'"
)


async def _load_actor_retention_overrides(  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    conn: ConnLike,
    schema: str = "taskq",
) -> dict[str, timedelta]:
    if not _IDENT_RE.match(schema):
        return {}
    sql = _ACTOR_RETENTION_SQL.format(schema=schema)
    rows = await conn.fetch(sql)
    result: dict[str, timedelta] = {}
    for row in rows:
        actor: str = row["actor"]
        days: int | None = row["retention_days"]
        if days is not None:
            result[actor] = timedelta(days=days)
    return result


_CLEANUP_STALE_WORKERS_SQL = (
    'DELETE FROM "{schema}".workers WHERE last_seen_at < now() - $1::interval AND id != $2'
)


async def cleanup_stale_workers(
    conn: ConnLike,
    *,
    worker_id: UUID,
    staleness: timedelta,
    schema: str = "taskq",
) -> int:
    """Delete worker rows whose ``last_seen_at`` exceeds *staleness*.

    The caller's *worker_id* is never deleted. Returns the number of rows
    removed. Worker-level cascade (``maintenance_leader``, ``job_attempts``)
    is handled by the DDL ``ON DELETE`` clauses — no extra sweeping needed.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    sql = _CLEANUP_STALE_WORKERS_SQL.format(schema=schema)
    tag = await conn.execute(sql, staleness, worker_id)
    return int(tag.rsplit(" ", 1)[-1]) if tag else 0


@dataclass(frozen=True, slots=True)
class PruneResult:
    total_deleted: int
    archived: int
    by_actor: dict[str, int]
    by_status: dict[str, int]
    cutoffs: dict[str, datetime]
    duration_ms: int


@dataclass(frozen=True, slots=True)
class ArchiveExpiryResult:
    total_deleted: int
    by_status: dict[str, int]
    expire_before: datetime
    duration_ms: int


_QUERY_QUEUE_DEPTH_SQL_TEMPLATE = (
    'SELECT queue, count(*) FROM "{schema}".jobs '
    "WHERE status IN ('pending', 'scheduled') GROUP BY queue"
)
_QUERY_RESERVATION_SLOTS_SQL_TEMPLATE = (
    'SELECT bucket_name, count(*) FROM "{schema}".reservation_slots '
    "WHERE job_id IS NOT NULL GROUP BY bucket_name"
)

_ARCHIVE_CTE_SQL = (
    "WITH candidate_ids AS ("
    '  SELECT id FROM "{schema}".jobs'
    '  WHERE status = $1::"{schema}".job_status'
    "    AND finished_at < $2"
    "  ORDER BY finished_at"
    "  LIMIT $3"
    "), moved AS ("
    '  INSERT INTO "{schema}".jobs_archive'
    "  SELECT j.*, now() AS archived_at, now() + $4 AS expire_at"
    '  FROM "{schema}".jobs j'
    "  JOIN candidate_ids c ON j.id = c.id"
    "  RETURNING id, actor, status"
    "), moved_attempts AS ("
    '  INSERT INTO "{schema}".job_attempts_archive'
    "  SELECT ja.*"
    '  FROM "{schema}".job_attempts ja'
    "  JOIN moved m ON ja.job_id = m.id"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs'
    "  WHERE id IN (SELECT id FROM moved)"
    "  RETURNING id, actor, status"
    ") SELECT actor, status, count(*) AS cnt"
    "  FROM deleted GROUP BY actor, status"
)

_ARCHIVE_CTE_ACTOR_SQL = (
    "WITH candidate_ids AS ("
    '  SELECT id FROM "{schema}".jobs'
    '  WHERE status = $1::"{schema}".job_status'
    "    AND finished_at < $2"
    "    AND actor = $5"
    "  ORDER BY finished_at"
    "  LIMIT $3"
    "), moved AS ("
    '  INSERT INTO "{schema}".jobs_archive'
    "  SELECT j.*, now() AS archived_at, now() + $4 AS expire_at"
    '  FROM "{schema}".jobs j'
    "  JOIN candidate_ids c ON j.id = c.id"
    "  RETURNING id, actor, status"
    "), moved_attempts AS ("
    '  INSERT INTO "{schema}".job_attempts_archive'
    "  SELECT ja.*"
    '  FROM "{schema}".job_attempts ja'
    "  JOIN moved m ON ja.job_id = m.id"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs'
    "  WHERE id IN (SELECT id FROM moved)"
    "  RETURNING id, actor, status"
    ") SELECT actor, status, count(*) AS cnt"
    "  FROM deleted GROUP BY actor, status"
)

_EXPIRY_CTE_SQL = (
    "WITH expired AS ("
    '  SELECT id FROM "{schema}".jobs_archive'
    "  WHERE expire_at < now()"
    "  ORDER BY expire_at"
    "  LIMIT $1"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs_archive'
    "  WHERE id IN (SELECT id FROM expired)"
    "  RETURNING id, status"
    ") SELECT status, count(*) AS cnt FROM deleted GROUP BY status"
)


async def prune_terminal_jobs(
    conn: ConnLike,
    *,
    retention_per_status: dict[str, timedelta],
    archive_retention: timedelta,
    batch_size: int = 10000,
    schema: str = "taskq",
    actor_overrides: dict[str, timedelta] | None = None,
) -> PruneResult:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    start = time.monotonic()
    total_deleted = 0
    total_archived = 0
    by_actor: dict[str, int] = {}
    by_status: dict[str, int] = {}
    cutoffs: dict[str, datetime] = {}
    archive_interval = archive_retention

    for status in _TERMINAL_STATUSES:
        retention = retention_per_status.get(status, timedelta(days=30))
        cutoff = datetime.now(UTC) - retention
        cutoffs[status] = cutoff
        sql = _ARCHIVE_CTE_SQL.format(schema=schema)

        while True:
            rows = await conn.fetch(sql, status, cutoff, batch_size, archive_interval)
            if not rows:
                break
            batch_total = 0
            for row in rows:
                actor_name: str = row["actor"]
                row_status: str = row["status"]
                cnt: int = row["cnt"]
                batch_total += cnt
                by_actor[actor_name] = by_actor.get(actor_name, 0) + cnt
                by_status[row_status] = by_status.get(row_status, 0) + cnt
                record_pruned_jobs(actor_name, row_status, cnt)
                record_archived_jobs(row_status, cnt)
            total_deleted += batch_total
            total_archived += batch_total
            if batch_total < batch_size:
                break

    if actor_overrides:
        for actor_name, actor_retention in actor_overrides.items():
            actor_cutoff = datetime.now(UTC) - actor_retention
            sql = _ARCHIVE_CTE_ACTOR_SQL.format(schema=schema)
            for status in _TERMINAL_STATUSES:
                if actor_retention >= retention_per_status.get(status, timedelta(days=30)):
                    continue
                while True:
                    rows = await conn.fetch(
                        sql,
                        status,
                        actor_cutoff,
                        batch_size,
                        archive_interval,
                        actor_name,
                    )
                    if not rows:
                        break
                    batch_total = 0
                    for row in rows:
                        a_name: str = row["actor"]
                        r_status: str = row["status"]
                        cnt: int = row["cnt"]
                        batch_total += cnt
                        by_actor[a_name] = by_actor.get(a_name, 0) + cnt
                        by_status[r_status] = by_status.get(r_status, 0) + cnt
                        record_pruned_jobs(a_name, r_status, cnt)
                        record_archived_jobs(r_status, cnt)
                    total_deleted += batch_total
                    total_archived += batch_total
                    if batch_total < batch_size:
                        break

    duration_ms = int((time.monotonic() - start) * 1000)
    return PruneResult(
        total_deleted=total_deleted,
        archived=total_archived,
        by_actor=by_actor,
        by_status=by_status,
        cutoffs=cutoffs,
        duration_ms=duration_ms,
    )


async def archive_expiry_sweep(
    conn: ConnLike,
    *,
    batch_size: int = 10000,
    schema: str = "taskq",
) -> ArchiveExpiryResult:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    start = time.monotonic()
    total_deleted = 0
    by_status: dict[str, int] = {}
    expire_before = datetime.now(UTC)
    sql = _EXPIRY_CTE_SQL.format(schema=schema)

    while True:
        rows = await conn.fetch(sql, batch_size)
        if not rows:
            break
        batch_total = 0
        for row in rows:
            row_status: str = row["status"]
            cnt: int = row["cnt"]
            batch_total += cnt
            by_status[row_status] = by_status.get(row_status, 0) + cnt
            record_expired_archive_jobs(row_status, cnt)
        total_deleted += batch_total
        if batch_total < batch_size:
            break

    duration_ms = int((time.monotonic() - start) * 1000)
    return ArchiveExpiryResult(
        total_deleted=total_deleted,
        by_status=by_status,
        expire_before=expire_before,
        duration_ms=duration_ms,
    )
