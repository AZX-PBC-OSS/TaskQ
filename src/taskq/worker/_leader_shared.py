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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

import asyncpg
import structlog

from taskq.backend._protocol import Backend, ConnLike
from taskq.backend._sql_templates import COPY_FROM_COLUMNS
from taskq.backend._sweeps import (
    SweepBatchSizer,
    _apply_batch_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: the prune family runs its batches through the same SET LOCAL batch-timeout machinery the backend sweeps use; importing the helpers (rather than redefining them) is what keeps the two from drifting.
    _restore_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: same helpers, same reason.
    _validate_positive,  # pyright: ignore[reportPrivateUsage]  # Why: the single typed boundary for sweep bounds; a second validator here would drift from the backend's.
)
from taskq.backend.clock import Clock
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_PRUNE_BATCH_SIZE,
    DEFAULT_PRUNE_RETENTION,
    DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS,
)
from taskq.obs import (
    get_logger,
    get_meter,
    record_archived_jobs,
    record_expired_archive_jobs,
    record_pruned_jobs,
    record_sweep_batch_size,
    record_sweep_batch_size_configured,
)
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps

__all__ = [
    "ArchiveExpiryResult",
    "PruneResult",
    "SweepContext",
    "archive_expiry_sweep",
    "cleanup_stale_workers",
    "complete_stale_batches",
    "prune_terminal_jobs",
]

log: structlog.stdlib.BoundLogger = get_logger(__name__)
_meter = get_meter()

_TERMINAL_NOT_IN = "NOT IN (" + ",".join(f"'{s}'" for s in TERMINAL_STATUSES) + ")"

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


# Split so a timed-out sweep call can record its duration WITHOUT a row
# sample: the row count is bound by the awaited call the deadline aborted,
# and a 0-row sample would be indistinguishable from a healthy empty sweep.


def _metric_duration(name: str, start: float) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    _sweep_duration_hist.record((time.monotonic() - start) * 1000.0, {"sweep_name": name})


def _metric_rows(name: str, count: int) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: imported by leader.py and _leader_sweeps.py
    _sweep_rows_counter.add(count, {"sweep_name": name})


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
    # The worker's resolved rate-limit registry, consumed by the de-gated
    # keyed-eviction block in ``_sweep_loop`` (every worker sweeps its own
    # registry each tick, leader or not); None for ad-hoc SweepContext
    # constructions outside worker bootstrap → module-singleton fallback.
    rate_limit_registry: RateLimitRegistry | None = None


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


_CLEANUP_STALE_WORKERS_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as the bounded sweeps in
-- taskq.backend._sweeps: LIMIT $3 caps how many workers one call may
-- delete, and because the DDL ON DELETE clauses fan out per deleted
-- worker (maintenance_leader cascade, job_attempts SET NULL rewrites),
-- the window is what bounds the referential rewrite set per
-- transaction; MATERIALIZED stops the planner from inlining the
-- LIMIT-ed CTE into the DELETE in a way that could delete more rows
-- than the LIMIT. No ORDER BY: the workers table is a small membership
-- table (one row per live worker), so the snap's scan is cheap however
-- it plans, and every windowed row is deleted by this same statement,
-- so the stale set shrinks monotonically per committed batch.
-- statement_timestamp() (STABLE) rather than clock_timestamp() (VOLATILE)
-- lets workers_last_seen_idx serve the staleness bound as an Index Cond
-- instead of a post-scan filter — same derivation as the sweep snaps in
-- taskq.backend._sweeps (see that module's docstring for the measured
-- plans); harmless here at membership-table scale, uniform with the
-- sweeps, and it keeps this snap correct-by-shape if workers ever grows.
WITH stale AS MATERIALIZED (
    SELECT id
    FROM "{schema}".workers
    WHERE last_seen_at < statement_timestamp() - $1::interval
      AND id != $2
    LIMIT $3
)
DELETE FROM "{schema}".workers w
USING stale
WHERE w.id = stale.id"""


async def cleanup_stale_workers(
    conn: ConnLike,
    *,
    worker_id: UUID,
    staleness: timedelta,
    schema: str = "taskq",
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    """Delete worker rows whose ``last_seen_at`` exceeds *staleness*, one
    bounded batch per call.

    One call deletes at most ``batch_size`` workers in one short
    transaction; repeated calls drain the stale set a batch at a time.
    The caller's *worker_id* is never deleted. Returns the number of
    worker rows removed by this call. Worker-level cascade
    (``maintenance_leader``, ``job_attempts``) is handled by the DDL
    ``ON DELETE`` clauses — no extra sweeping needed — and the window
    bounds workers per call, which bounds that per-worker fan-out per
    transaction: a whole-fleet crash drains in committed batches instead
    of one transaction rewriting every stale worker's attempt history.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    sql = _CLEANUP_STALE_WORKERS_SQL.format(schema=schema)
    tag = await conn.execute(sql, staleness, worker_id, batch_size)
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
# Depth and oldest-pending age come out of ONE grouped scan so the two
# gauges can never describe two different moments -- a depth sampled before
# a drain and an age sampled after it would read as an actor recovering when
# it is not. 'pending' only: the unconsumed-actor condition is rows that are
# eligible and nobody takes, which is exactly the pending population;
# scheduled rows are awaiting promotion and are the oldest-DUE-age gauge's
# subject. MIN(scheduled_at) is the eligibility instant of the longest-
# waiting row, so its age is how long this actor has gone unserved.
# clock_timestamp() is a measured value, not a bound, so it stays volatile.
_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE = (
    "SELECT actor, queue, count(*) AS depth, "
    "EXTRACT(EPOCH FROM (clock_timestamp() - MIN(scheduled_at)))::float8 AS oldest_age "
    'FROM "{schema}".jobs '
    "WHERE status = 'pending' "
    "GROUP BY actor, queue"
)

# Explicit (not `j.*` / `ja.*`) column lists for the jobs -> jobs_archive and
# job_attempts -> job_attempts_archive INSERTs below. `jobs_archive` mirrors
# every `jobs` column plus two archive-only trailing columns (archived_at,
# expire_at); `job_attempts_archive` mirrors `job_attempts` column-for-column.
# Postgres ALTER TABLE ADD COLUMN always appends at the end of a table's own
# column order, so a new column added to one side of a mirrored pair (e.g.
# `jobs.idempotency_scope`) lands in a different relative position than on
# the other side once mirrored there — a bare `SELECT source.*` relies on
# the two tables' column orders staying in lockstep, which a future
# single-table ALTER TABLE ADD COLUMN silently breaks. Naming every column
# explicitly on both sides makes this correct regardless of physical order.
_JOBS_COLUMNS_CSV = ", ".join(COPY_FROM_COLUMNS)
_JOBS_COLUMNS_QUALIFIED_CSV = ", ".join(f"j.{c}" for c in COPY_FROM_COLUMNS)

_JOB_ATTEMPTS_COLUMNS: tuple[str, ...] = (
    "job_id",
    "attempt",
    "started_at",
    "finished_at",
    "outcome",
    "error_class",
    "error_message",
    "error_traceback",
    "duration_ms",
    "worker_id",
    "metadata",
)
_JOB_ATTEMPTS_COLUMNS_CSV = ", ".join(_JOB_ATTEMPTS_COLUMNS)
_JOB_ATTEMPTS_COLUMNS_QUALIFIED_CSV = ", ".join(f"ja.{c}" for c in _JOB_ATTEMPTS_COLUMNS)

# Two clocks, one statement. The SELECTION bound is statement_timestamp()
# (STABLE — the database's wall clock at this statement's start): a
# VOLATILE clock_timestamp() comparison cannot be a btree index
# condition, so the candidate scan degrades to a post-scan Filter that
# walks jobs_finished_at_idx's whole terminal population per batch —
# measured on a 70k-terminal-row corpus (PG 18, EXPLAIN ANALYZE,
# BUFFERS): 1,757 buffers / ~11 ms per drained-state call vs 2 buffers /
# ~0.05 ms when the stable bound is an Index Cond that terminates at the
# range boundary (pinned, including the server-prepared form a
# long-lived connection runs past five same-statement executions, by
# tests/test_index_audit.py). The WRITE side — the archived_at/expire_at
# stamps below — stays clock_timestamp(): the same clock that wrote
# finished_at, so a skewed worker host cannot silently extend or shorten
# retention, and the stamps cannot disagree with the clock domain of the
# rows they annotate; statement_timestamp() differs from those stamps
# only by this statement's own execution time (microseconds against a
# days-scale retention cutoff).
#
# MATERIALIZED on candidate_ids (and on the sibling windows below) is
# load-bearing with the same rationale every windowed sweep in
# backend/_sweeps.py documents: the planner may inline a LIMIT-ed CTE
# into the data-modifying statement that joins it and move more rows
# than the LIMIT (the LIMIT then bounds only the CTE's inlined
# appearances, not the archived-and-deleted result), so the keyword
# fences the window and pins that one batch is bounded by its LIMIT.
_ARCHIVE_CTE_SQL = (
    "WITH candidate_ids AS MATERIALIZED ("
    '  SELECT id FROM "{schema}".jobs'
    '  WHERE status = $1::"{schema}".job_status'
    "    AND finished_at < statement_timestamp() - $2::interval"
    "  ORDER BY finished_at"
    "  LIMIT $3"
    "), moved AS ("
    f'  INSERT INTO "{{schema}}".jobs_archive ({_JOBS_COLUMNS_CSV}, archived_at, expire_at)'
    f"  SELECT {_JOBS_COLUMNS_QUALIFIED_CSV}, clock_timestamp(), clock_timestamp() + $4"
    '  FROM "{schema}".jobs j'
    "  JOIN candidate_ids c ON j.id = c.id"
    "  RETURNING id, actor, status"
    "), moved_attempts AS ("
    f'  INSERT INTO "{{schema}}".job_attempts_archive ({_JOB_ATTEMPTS_COLUMNS_CSV})'
    f"  SELECT {_JOB_ATTEMPTS_COLUMNS_QUALIFIED_CSV}"
    '  FROM "{schema}".job_attempts ja'
    "  JOIN moved m ON ja.job_id = m.id"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs'
    "  WHERE id IN (SELECT id FROM moved)"
    "  RETURNING id, actor, status"
    ") SELECT actor, status, count(*) AS cnt"
    "  FROM deleted GROUP BY actor, status"
)

# Same windowing contract as _ARCHIVE_CTE_SQL above, including the
# MATERIALIZED fence on the LIMIT-ed candidate window.
_ARCHIVE_CTE_ACTOR_SQL = (
    "WITH candidate_ids AS MATERIALIZED ("
    '  SELECT id FROM "{schema}".jobs'
    '  WHERE status = $1::"{schema}".job_status'
    "    AND finished_at < statement_timestamp() - $2::interval"
    "    AND actor = $5"
    "  ORDER BY finished_at"
    "  LIMIT $3"
    "), moved AS ("
    f'  INSERT INTO "{{schema}}".jobs_archive ({_JOBS_COLUMNS_CSV}, archived_at, expire_at)'
    f"  SELECT {_JOBS_COLUMNS_QUALIFIED_CSV}, clock_timestamp(), clock_timestamp() + $4"
    '  FROM "{schema}".jobs j'
    "  JOIN candidate_ids c ON j.id = c.id"
    "  RETURNING id, actor, status"
    "), moved_attempts AS ("
    f'  INSERT INTO "{{schema}}".job_attempts_archive ({_JOB_ATTEMPTS_COLUMNS_CSV})'
    f"  SELECT {_JOB_ATTEMPTS_COLUMNS_QUALIFIED_CSV}"
    '  FROM "{schema}".job_attempts ja'
    "  JOIN moved m ON ja.job_id = m.id"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs'
    "  WHERE id IN (SELECT id FROM moved)"
    "  RETURNING id, actor, status"
    ") SELECT actor, status, count(*) AS cnt"
    "  FROM deleted GROUP BY actor, status"
)

_DB_NOW_SQL = "SELECT clock_timestamp()"


# The expire_at bound is statement_timestamp() (STABLE) for the same
# index-cond reason as the archive CTEs above: jobs_archive_expire_at_idx
# serves the bound as an Index Cond instead of a post-scan Filter over
# the whole archive population (measured on a 30k-row jobs_archive:
# 2,039 buffers / ~6.7 ms vs 2 buffers / ~0.03 ms in the drained steady
# state; the server-prepared form a long-lived connection runs past five
# same-statement executions degrades worst, flipping to a generic plan
# that walks the entire population under a Filter — pinned, with the
# eligible-backlog state, by tests/test_index_audit.py). No write side
# here: the CTE only selects and deletes. The MATERIALIZED fence on the
# LIMIT-ed window is the same load-bearing anti-inlining pin as every
# sibling sweep's.
_EXPIRY_CTE_SQL = (
    "WITH expired AS MATERIALIZED ("
    '  SELECT id FROM "{schema}".jobs_archive'
    "  WHERE expire_at < statement_timestamp()"
    "  ORDER BY expire_at"
    "  LIMIT $1"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs_archive'
    "  WHERE id IN (SELECT id FROM expired)"
    "  RETURNING id, status"
    ") SELECT status, count(*) AS cnt FROM deleted GROUP BY status"
)


def _effective_prune_batch_size(batch_size: int, sizer: SweepBatchSizer | None) -> int:
    """The tier one batch uses: the breaker's when draining breaker-wrapped,
    the explicit bound for direct (sizer-less) calls."""
    return sizer.effective_size() if sizer is not None else batch_size


def _record_prune_batch_size(sweep_name: str, size: int, sizer: SweepBatchSizer | None) -> None:
    """Record the used and configured batch-size gauges for one
    prune-family batch — the same label pair ``_run_bounded_sweep`` emits
    for the backend sweeps, so the gauge-to-gauge sweep-degraded alert
    covers the prune family too."""
    record_sweep_batch_size(sweep_name, size)
    record_sweep_batch_size_configured(
        sweep_name, sizer.default_size if sizer is not None else size
    )


async def _run_prune_batch(
    conn: ConnLike,
    sql: str,
    *args: object,
    statement_timeout_ms: int,
    sizer: SweepBatchSizer | None,
) -> Sequence[asyncpg.Record]:
    """Run one prune-family batch statement under the shared batch machinery.

    The batch runs inside one short transaction with a server-side
    ``statement_timeout`` bound via ``SET LOCAL`` semantics (captured and
    restored on the success path; the savepoint rollback restores it on
    the error path) — the same wrapper every bounded backend sweep in
    :mod:`taskq.backend._sweeps` applies, so the prune family gets the
    identical guarantee: one committed, server-bounded statement per
    batch, whatever the backlog behind it. A deadline-family abort
    (``QueryCanceledError`` from the server-side timeout,
    ``TimeoutError`` from a client-side one) counts against *sizer* when
    given and re-raises: the caller's failure path retries later at the
    latched reduced tier, and every batch this call already committed
    stays committed — a stopped drain is a pause, not a rollback.
    """
    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        try:
            rows = await conn.fetch(sql, *args)
        except (asyncpg.QueryCanceledError, TimeoutError):
            if sizer is not None:
                sizer.on_timeout()
            raise
        # Success path only: restore the caller's timeout inside the
        # still-open transaction (a savepoint RELEASE would otherwise
        # keep the batch's bound); on error the savepoint rollback has
        # already restored it.
        await _restore_statement_timeout(conn, prev_timeout)
    if sizer is not None:
        sizer.on_success()
    return rows


async def prune_terminal_jobs(
    conn: ConnLike,
    *,
    retention_per_status: dict[str, timedelta],
    archive_retention: timedelta,
    batch_size: int = DEFAULT_PRUNE_BATCH_SIZE,
    schema: str = "taskq",
    actor_overrides: dict[str, timedelta] | None = None,
    drain_gate: Callable[[], bool] | None = None,
    statement_timeout_ms: int = DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS,
    sizer: SweepBatchSizer | None = None,
) -> PruneResult:
    """Archive-and-delete terminal jobs past their retention, one bounded,
    self-committing batch at a time.

    Each batch is one ``_ARCHIVE_CTE_SQL`` statement (jobs →
    jobs_archive, job_attempts → job_attempts_archive, jobs DELETE,
    inside one statement), committed before the next batch runs. The
    batch runs under a server-side ``statement_timeout`` (the
    :data:`~taskq.constants.DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS`
    derivation), and when *sizer* is given its latched tier — not
    *batch_size* — sizes every window, so a database that keeps aborting
    batches is retried at a reduced tier instead of the same one.

    *drain_gate* is called before every batch; a ``False`` return stops
    the drain (committed batches stay committed; the remainder waits for
    the next attempt). The prune loop passes a gate that returns False on
    shutdown and ticks detector-2 liveness between batches.

    The archive predicate's clock is the database's own
    (``statement_timestamp()`` in the CTE), and the reported cutoffs are
    anchored to a database-side ``clock_timestamp()`` read — see the
    "Anchored to the database clock" comment in the body below.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("statement_timeout_ms", statement_timeout_ms)
    if sizer is None:
        _validate_positive("batch_size", batch_size)
    start = time.monotonic()
    total_deleted = 0
    total_archived = 0
    by_actor: dict[str, int] = {}
    by_status: dict[str, int] = {}
    cutoffs: dict[str, datetime] = {}
    archive_interval = archive_retention

    # Anchored to the database clock, not this process's: the archive
    # predicate is server-side (statement_timestamp() - $2::interval in
    # _ARCHIVE_CTE_SQL), and the caller derives `prune_old_batches`'
    # DELETE cutoff from `max(cutoffs.values())` — so these are NOT
    # display-only, and a Python `now` here would compare an app-clock
    # instant against DB-written `completed_at` values, pruning batches
    # early or late by the skew.
    db_now: datetime = await conn.fetchval(_DB_NOW_SQL)

    for status in TERMINAL_STATUSES:
        retention = retention_per_status.get(status, DEFAULT_PRUNE_RETENTION)
        cutoffs[status] = db_now - retention
        sql = _ARCHIVE_CTE_SQL.format(schema=schema)

        while True:
            if drain_gate is not None and not drain_gate():
                break
            size = _effective_prune_batch_size(batch_size, sizer)
            _record_prune_batch_size("prune", size, sizer)
            rows = await _run_prune_batch(
                conn,
                sql,
                status,
                retention,
                size,
                archive_interval,
                statement_timeout_ms=statement_timeout_ms,
                sizer=sizer,
            )
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
            if batch_total < size:
                break

    if actor_overrides:
        for actor_name, actor_retention in actor_overrides.items():
            sql = _ARCHIVE_CTE_ACTOR_SQL.format(schema=schema)
            for status in TERMINAL_STATUSES:
                if actor_retention >= retention_per_status.get(status, DEFAULT_PRUNE_RETENTION):
                    continue
                while True:
                    if drain_gate is not None and not drain_gate():
                        break
                    size = _effective_prune_batch_size(batch_size, sizer)
                    _record_prune_batch_size("prune", size, sizer)
                    rows = await _run_prune_batch(
                        conn,
                        sql,
                        status,
                        actor_retention,
                        size,
                        archive_interval,
                        actor_name,
                        statement_timeout_ms=statement_timeout_ms,
                        sizer=sizer,
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
                    if batch_total < size:
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
    batch_size: int = DEFAULT_PRUNE_BATCH_SIZE,
    schema: str = "taskq",
    drain_gate: Callable[[], bool] | None = None,
    statement_timeout_ms: int = DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS,
    sizer: SweepBatchSizer | None = None,
) -> ArchiveExpiryResult:
    """Hard-delete expired ``jobs_archive`` rows, one bounded,
    self-committing batch at a time.

    Each batch is one ``_EXPIRY_CTE_SQL`` statement, committed before the
    next runs, under the same server-side ``statement_timeout`` and
    breaker semantics as :func:`prune_terminal_jobs` — see that
    function's docstring and
    :data:`~taskq.constants.DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS` for the
    timeout/breaker contract. *drain_gate* is called before every batch;
    a ``False`` return stops the drain with the batches already
    committed.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("statement_timeout_ms", statement_timeout_ms)
    if sizer is None:
        _validate_positive("batch_size", batch_size)
    start = time.monotonic()
    total_deleted = 0
    by_status: dict[str, int] = {}
    # Reported on the result for observability; the DELETE predicate itself
    # is server-side (`expire_at < statement_timestamp()` in _EXPIRY_CTE_SQL).
    # Read from the database anyway so the reported instant stays in the
    # predicate's own clock domain — the sibling cutoffs above were
    # documented as display-only and then quietly grew a second consumer.
    expire_before: datetime = await conn.fetchval(_DB_NOW_SQL)
    sql = _EXPIRY_CTE_SQL.format(schema=schema)

    while True:
        if drain_gate is not None and not drain_gate():
            break
        size = _effective_prune_batch_size(batch_size, sizer)
        _record_prune_batch_size("archive_expiry", size, sizer)
        rows = await _run_prune_batch(
            conn,
            sql,
            size,
            statement_timeout_ms=statement_timeout_ms,
            sizer=sizer,
        )
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
        if batch_total < size:
            break

    duration_ms = int((time.monotonic() - start) * 1000)
    return ArchiveExpiryResult(
        total_deleted=total_deleted,
        by_status=by_status,
        expire_before=expire_before,
        duration_ms=duration_ms,
    )


_COMPLETE_STALE_BATCHES_SQL = """\
-- MATERIALIZED is load-bearing: without it the planner may inline the
-- LIMIT-ed CTE into the UPDATE and run it as a nested loop, completing
-- more batches than the LIMIT; the window-then-update-by-id shape bounds
-- one call to batch_size rows.
WITH candidate AS MATERIALIZED (
    SELECT b.id
    FROM "{schema}".batches b
    WHERE b.status = 'active'
      AND NOT EXISTS (
        SELECT 1 FROM "{schema}".jobs j
        WHERE j.metadata @> jsonb_build_object('batch_id', b.id::text)
          AND j.status {terminal_not_in}
      )
    LIMIT $1
),
completed AS (
    UPDATE "{schema}".batches b
    SET status = 'complete', completed_at = clock_timestamp()
    FROM candidate c
    WHERE b.id = c.id AND b.status = 'active'
    RETURNING b.id
)
SELECT count(*)::int FROM completed"""


async def complete_stale_batches(
    conn: ConnLike,
    *,
    schema: str = "taskq",
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    """Safety net: mark active batches with zero non-terminal jobs as complete.

    Covers batches whose completion hook was lost (consumer crash) and
    intentionally-empty batches (expected_size=0, no jobs at all). One call
    is one bounded batch: at most *batch_size* completions, committed, so
    the sweep loop drains the remainder one call per tick.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    sql = _COMPLETE_STALE_BATCHES_SQL.format(schema=schema, terminal_not_in=_TERMINAL_NOT_IN)
    completed: int = await conn.fetchval(sql, batch_size)
    return completed
