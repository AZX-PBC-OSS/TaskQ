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

from taskq._advisory import DEADLINE_ERRORS
from taskq.backend._batch_sql import open_member_where
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
    record_sweep_unexpected_error,
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
-- instead of a post-scan filter, same derivation as the sweep snaps in
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
    ``ON DELETE`` clauses, no extra sweeping needed, and the window
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

# Explicit (not `j.*` / `ja.*`) column lists for the jobs -> jobs_archive and
# job_attempts -> job_attempts_archive INSERTs below. `jobs_archive` mirrors
# every `jobs` column plus two archive-only trailing columns (archived_at,
# expire_at); `job_attempts_archive` mirrors `job_attempts` column-for-column.
# Postgres ALTER TABLE ADD COLUMN always appends at the end of a table's own
# column order, so a new column added to one side of a mirrored pair (e.g.
# `jobs.idempotency_scope`) lands in a different relative position than on
# the other side once mirrored there, a bare `SELECT source.*` relies on
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

# Two clocks, one candidate window. The SELECTION bound is
# statement_timestamp() (STABLE, the database's wall clock at this
# statement's start): a VOLATILE clock_timestamp() comparison cannot be a
# btree index condition, so the candidate scan degrades to a post-scan
# Filter that walks jobs_finished_at_idx's whole terminal population per
# batch, measured on a 70k-terminal-row corpus (PG 18, EXPLAIN ANALYZE,
# BUFFERS): 1,757 buffers / ~11 ms per drained-state call vs 2 buffers /
# ~0.05 ms when the stable bound is an Index Cond that terminates at the
# range boundary (pinned, including the server-prepared form a
# long-lived connection runs past five same-statement executions, by
# tests/test_index_audit.py). The WRITE side, the archived_at/expire_at
# stamps in _ARCHIVE_CTE_SQL below, stays clock_timestamp(): the same
# clock that wrote finished_at, so a skewed worker host cannot silently
# extend or shorten retention, and the stamps cannot disagree with the
# clock domain of the rows they annotate. With the window and the write
# split into two statements, the selection bound is the CANDIDATE
# statement's statement_timestamp() (its statement start, before the
# batch's rows are known), and the write statement re-verifies the age
# against its own statement_timestamp() at lock time: a row dropped
# between the two instants only ever tightens the cutoff by the two
# statements' own execution time (microseconds against a days-scale
# retention cutoff); a stamp written is never older than the cutoff
# the candidate window used.
#
# Why the candidate window is its OWN statement, and why it carries no
# row locks: measured on the same corpus, any lock-bearing arm inside
# the same statement (a FOR UPDATE on this window, or a lock-by-id CTE
# joined beside it) pushes the statement's custom-plan cost estimate
# past the generic plan's (the lock prices heap fetches, and the arm's
# own join prices a whole-table scan against the batch's probe count).
# Past five executions the plancache then flips the statement to the
# GENERIC plan, and the generic form cannot prove the partial index's
# predicate for bound parameters: the candidate scan degrades into a
# bitmap-plus-sort population walk (measured: the prepared pin fails
# with LockRows atop the window). As its own statement the window's
# custom plan is the index-only LIMIT-terminated scan at a fraction of
# its generic form's estimate, so the plancache keeps the custom plan
# and the window stays bounded in every plan form (pinned, prepared
# form included, by tests/test_index_audit.py). The race fence this
# window deliberately does not carry lives on the write statement
# below, which locks the small batch by primary key.
# The candidate windows' shared predicate: one terminal status past its
# retention, the age bound carried by the candidate statement's
# statement_timestamp() (the clock contract above). Both windows below
# compose this fragment verbatim, so the selection predicate cannot
# drift between the fleet-wide and per-actor forms; the plan pins bind
# the composed statements (tests/test_index_audit.py), so a composition
# that changed the executed text fails on arrival.
_ARCHIVE_CANDIDATE_PREDICATE_SQL = (
    'SELECT id FROM "{schema}".jobs'
    ' WHERE status = $1::"{schema}".job_status'
    "   AND finished_at < statement_timestamp() - $2::interval"
)

_ARCHIVE_CANDIDATE_SQL = _ARCHIVE_CANDIDATE_PREDICATE_SQL + " ORDER BY finished_at" + " LIMIT $3"

# The per-actor-retention candidate window: the same selection shape
# plus an actor equality (the actor filter rides along as a Filter, like
# status; the plan pins cover this form too).
_ARCHIVE_CANDIDATE_ACTOR_SQL = (
    _ARCHIVE_CANDIDATE_PREDICATE_SQL + "   AND actor = $4" + " ORDER BY finished_at" + " LIMIT $3"
)

# The archive write: one statement per batch, fed the candidate ids the
# candidate window (above) selected in the same transaction. The
# MATERIALIZED fence keeps the lock arm from being inlined into the
# data-modifying statements that join it, the same contract every
# windowed sweep in backend/_sweeps.py pins: without it the planner may
# fold the lock scan into the INSERT/DELETE joins and lock rows beyond
# the batch the candidate window selected.
#
# `locked` is THE lock-time re-read, and the ghost fence: the write
# statement takes the batch's row locks here, by primary key over the
# id array (a pkey probe per candidate, bounded in every plan form,
# custom and generic), and the scan's quals re-evaluate at lock time
# (EvalPlanQual), against the row version the lock sees, not the
# statement snapshot. Both candidate predicates are re-verified: the
# terminal status, AND the retention age. The age re-check is what a
# full retry-then-re-run cycle between the two statements needs: a
# retried job that re-terminalizes in that gap is terminal again (the
# status check alone would pass) but zero seconds old, and archiving
# it would remove a just-finished job from the live tables without
# serving its retention; the age qual fails on the fresh finished_at
# and the row drops out here, to re-enter a later window when it has
# actually aged. The age Filter rides the pkey-probed batch (at most
# LIMIT ids), so it cannot reintroduce the population walk the
# candidate statement exists to prevent (pinned by
# tests/test_index_audit.py). A retry_job that committed between the
# candidate window's snapshot and this lock makes the row live; the
# status re-check fails and the row drops out here, before anything is
# archived. A retry still in flight is skipped (SKIP LOCKED) and keeps
# its own commit. Without this arm, `moved`'s status re-check reads
# the statement snapshot alone: a row archived while a retry left it
# live is a ghost, and when the retried job re-terminalizes and
# re-enters the prune window, `moved` hits jobs_archive's primary key:
# a non-transient error that aborts every batch containing that row,
# the wedge that stops the drain at the head of the window forever.
#
# The moved/deleted arms re-read the rows `locked` already holds; their
# join shape is the planner's choice (a hash join over a Seq Scan is
# legitimately priced when the batch is a large fraction of the table)
# and is not a bound: the write set is exactly `locked`'s rows whatever
# the join shape, and `locked` itself is pkey-probed in every plan form
# (pinned by tests/test_index_audit.py).
#
# The candidate ids bind as an array ($3::uuid[]), so the lock rides
# the primary key: the plan stays index-bounded whatever the plancache
# picks (pinned, prepared form included, by tests/test_index_audit.py).
_ARCHIVE_CTE_SQL = (
    "WITH locked AS MATERIALIZED ("
    "  SELECT j.id"
    '  FROM "{schema}".jobs j'
    "  WHERE j.id = ANY($3::uuid[])"
    '  AND j.status = $1::"{schema}".job_status'
    "  AND j.finished_at < statement_timestamp() - $4::interval"
    "  FOR UPDATE SKIP LOCKED"
    "), moved AS ("
    f'  INSERT INTO "{{schema}}".jobs_archive ({_JOBS_COLUMNS_CSV}, archived_at, expire_at)'
    f"  SELECT {_JOBS_COLUMNS_QUALIFIED_CSV}, clock_timestamp(), clock_timestamp() + $2"
    '  FROM "{schema}".jobs j'
    "  JOIN locked l ON j.id = l.id"
    # Only a row this transaction locked and verified terminal a moment
    # ago is archived; the re-check is retained as belt and braces (the
    # locks just taken guarantee the version cannot have moved).
    '  AND j.status = $1::"{schema}".job_status'
    # The archive-once guard: a job id that already holds an archive row
    # is never archived again (the NOT EXISTS probe rides the archive's
    # id-leading index in every mode). This is what lets the optional
    # hypertable mode widen the archive's uniqueness from PRIMARY KEY
    # (id) to UNIQUE (id, finished_at) - a hypertable requires the
    # partition column in every unique constraint, so bare-id uniqueness
    # is inexpressible there - without changing the re-archive
    # semantics: on vanilla Postgres the same shape met the primary key
    # as a loud non-transient error that wedged every later batch, the
    # guard replaces the wedge with a fold (the existing archive row
    # stands) in BOTH modes, so the prune converges identically and the
    # semantics are pinned by the same tests in both.
    '  AND NOT EXISTS (SELECT 1 FROM "{schema}".jobs_archive a WHERE a.id = j.id)'
    "  RETURNING id, actor, status"
    "), moved_attempts AS ("
    f'  INSERT INTO "{{schema}}".job_attempts_archive ({_JOB_ATTEMPTS_COLUMNS_CSV})'
    f"  SELECT {_JOB_ATTEMPTS_COLUMNS_QUALIFIED_CSV}"
    '  FROM "{schema}".job_attempts ja'
    "  JOIN moved m ON ja.job_id = m.id"
    "), verified AS MATERIALIZED ("
    # Every candidate row this statement locked and still verified
    # terminal, whether or not the INSERT above folded it into an
    # existing archive row: the delete arm removes the live row either
    # way, so a folded id converges (exactly one archive row, no live
    # row) instead of re-entering the window forever. The folded edge
    # loses only the retried job's post-ghost attempts; the standing
    # archive row is the pre-retry version.
    '  SELECT j.id FROM "{schema}".jobs j'
    "  JOIN locked l ON j.id = l.id"
    '  AND j.status = $1::"{schema}".job_status'
    "), cascaded_events AS MATERIALIZED ("
    # The events this statement's delete is about to cascade away
    # (job_events.job_id REFERENCES jobs ON DELETE CASCADE, there is no
    # job_events_archive). Read from the statement snapshot: the delete
    # arm's effects are invisible to a sibling CTE, so the max is the
    # pre-delete truth, whatever order the executor picks. Fed into the
    # prune watermark (migration 01.00.20_02) so the cascade deleter
    # advances the same bound the retention sweep does: a watch_reclaims
    # consumer resuming a cursor strictly below it has lost undelivered
    # events to THIS statement, and the poll side fails visible on it.
    "  SELECT COALESCE(max(e.id), 0) AS max_id"
    '  FROM "{schema}".job_events e'
    "  WHERE e.job_id IN (SELECT id FROM verified)"
    "), event_watermark AS ("
    '  INSERT INTO "{schema}".job_events_prune_state'
    "    (singleton, pruned_through_id, updated_at)"
    "  SELECT true, max_id, clock_timestamp() FROM cascaded_events"
    "  WHERE max_id > 0"
    "  ON CONFLICT (singleton) DO UPDATE"
    "  SET pruned_through_id = GREATEST("
    "          job_events_prune_state.pruned_through_id, EXCLUDED.pruned_through_id"
    "      ),"
    "      updated_at = EXCLUDED.updated_at"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs'
    "  WHERE id IN (SELECT id FROM verified)"
    # The lock-time re-check's second line of defense: the row version
    # this statement deletes must still be the terminal one it archived.
    # Rows were locked and verified in `locked`, so a version change
    # between the arms cannot occur; the guard stays so the delete never
    # removes a row its own statement did not verify.
    '  AND status = $1::"{schema}".job_status'
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
# that walks the entire population under a Filter, pinned, with the
# eligible-backlog state, by tests/test_index_audit.py). No write side
# here: the CTE only selects and deletes. The MATERIALIZED fence on the
# LIMIT-ed window is the same essential anti-inlining pin as every
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
    prune-family batch, the same label pair ``_run_bounded_sweep`` emits
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
    sweep_name: str,
    sizer: SweepBatchSizer | None,
) -> Sequence[asyncpg.Record]:
    """Run one prune-family batch statement under the shared batch machinery.

    The batch runs inside one short transaction with a server-side
    ``statement_timeout`` bound via ``SET LOCAL`` semantics (captured and
    restored on the success path; the savepoint rollback restores it on
    the error path), the same wrapper every bounded backend sweep in
    :mod:`taskq.backend._sweeps` applies, so the prune family gets the
    identical guarantee: one committed, server-bounded statement per
    batch, whatever the backlog behind it.

    Every aborted batch counts against *sizer* when given, and re-raises:
    a deadline-family abort (``QueryCanceledError`` from the server-side
    timeout, ``TimeoutError`` from a client-side one) counts as today,
    and so does ANY other exception - the reduced tier is the safety net
    for the next unknown failure mode, so an archive
    UniqueViolation-class error must count toward the same failure
    threshold too, not leave the breaker unlatched while every retry
    re-runs the same full-size batch. The caller's failure path retries
    later at the latched reduced tier, and every batch this call already
    committed stays committed, a stopped drain is a pause, not a
    rollback. A non-deadline abort also lands on
    ``record_sweep_unexpected_error`` under *sweep_name*, the metric
    plane the deadline family's ``sweep_timeouts`` counter leaves silent.
    """
    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        try:
            rows = await conn.fetch(sql, *args)
        except DEADLINE_ERRORS:
            if sizer is not None:
                sizer.on_timeout()
            raise
        except Exception:
            # The not-deadline arm of the same control signal: any batch
            # this machinery ran and lost must reach the breaker, whatever
            # the fault. CancelledError is not counted: it is a
            # BaseException on this project's Python floor (>= 3.12), so it
            # never enters this arm at all, which is the correct
            # accounting either way - a shutdown cancellation is not a
            # prune failure. The same holds for KeyboardInterrupt and
            # SystemExit: they bypass both arms and the callers' except
            # Exception handlers alike, and propagate to the shutdown
            # chain with the transaction rolled back.
            if sizer is not None:
                sizer.on_timeout()
            record_sweep_unexpected_error(sweep_name)
            raise
        # Success path only: restore the caller's timeout inside the
        # still-open transaction (a savepoint RELEASE would otherwise
        # keep the batch's bound); on error the savepoint rollback has
        # already restored it.
        await _restore_statement_timeout(conn, prev_timeout)
    if sizer is not None:
        sizer.on_success()
    return rows


async def _run_prune_archive_batch(
    conn: ConnLike,
    *,
    candidate_sql: str,
    write_sql: str,
    status: str,
    retention: timedelta,
    size: int,
    archive_interval: timedelta,
    actor: str | None,
    statement_timeout_ms: int,
    sweep_name: str,
    sizer: SweepBatchSizer | None,
) -> Sequence[asyncpg.Record]:
    """Run one archive batch: the candidate window, then the lock-bearing
    write, inside one transaction.

    The two statements exist for the plancache (see the comment above
    ``_ARCHIVE_CANDIDATE_SQL``: a lock-bearing arm priced into the same
    statement flips it to the generic plan past five executions, and the
    generic form walks the terminal population), but they keep the
    single-batch guarantees: the candidate window is a bounded
    LIMIT-terminated read, the write statement locks the batch by
    primary key and re-checks both candidate predicates (terminal
    status and retention age) at lock time, and both run in the
    transaction this helper opens, so the batch commits or not as a
    unit under the same
    server-side ``statement_timeout`` machinery
    :func:`_run_prune_batch` applies.

    Wall-clock bound: the transaction spans TWO statements, each under
    its own ``statement_timeout``, so one batch can take up to
    2x ``statement_timeout_ms`` (plus the timeout-probe round trips) -
    callers sizing outer deadlines against a prune loop must budget the
    2x bound, not one statement's.

    Failure accounting mirrors :func:`_run_prune_batch`: every aborted
    batch (deadline family and any other exception alike) counts against
    *sizer* when given, a non-deadline abort lands on
    ``record_sweep_unexpected_error`` under *sweep_name*, and the
    exception re-raises.

    Returns the write statement's
    deleted groups (empty when the window found nothing eligible, and
    empty when every candidate dropped out at lock time: a row a
    concurrent retry made live, or a batch another transaction is
    already moving).
    """
    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        try:
            candidate_args: tuple[object, ...] = (status, retention, size)
            if actor is not None:
                candidate_args += (actor,)
            candidates = await conn.fetch(candidate_sql, *candidate_args)
            rows: Sequence[asyncpg.Record] = ()
            if candidates:
                ids = [row["id"] for row in candidates]
                rows = await conn.fetch(write_sql, status, archive_interval, ids, retention)
        except DEADLINE_ERRORS:
            if sizer is not None:
                sizer.on_timeout()
            raise
        except Exception:
            # The not-deadline arm of the same control signal, same
            # derivation as _run_prune_batch above: the candidate window
            # and the archive write are ONE batch, so a UniqueViolation
            # (or any other non-deadline fault) aborting either must
            # reach the breaker and the metric plane.
            if sizer is not None:
                sizer.on_timeout()
            record_sweep_unexpected_error(sweep_name)
            raise
        # Success path only, same derivation as _run_prune_batch above.
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

    Each batch is one transaction of two statements (the bounded
    candidate window, then the lock-bearing ``_ARCHIVE_CTE_SQL`` write:
    jobs → jobs_archive, job_attempts → job_attempts_archive, jobs
    DELETE), committed before the next batch runs. The batch runs under
    a server-side ``statement_timeout`` (the
    :data:`~taskq.constants.DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS`
    derivation), and when *sizer* is given its latched tier, not
    *batch_size*, sizes every window, so a database that keeps aborting
    batches is retried at a reduced tier instead of the same one.

    *drain_gate* is called before every batch; a ``False`` return stops
    the drain (committed batches stay committed; the remainder waits for
    the next attempt). The prune loop passes a gate that returns False on
    shutdown and ticks detector-2 liveness between batches.

    The archive predicate's clock is the database's own
    (``statement_timestamp()`` in the candidate statement, re-verified
    at lock time by the write statement's), and the reported cutoffs are
    anchored to a database-side ``clock_timestamp()`` read, see the
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
    # DELETE cutoff from `max(cutoffs.values())`, so these are NOT
    # display-only, and a Python `now` here would compare an app-clock
    # instant against DB-written `completed_at` values, pruning batches
    # early or late by the skew.
    db_now: datetime = await conn.fetchval(_DB_NOW_SQL)

    for status in TERMINAL_STATUSES:
        retention = retention_per_status.get(status, DEFAULT_PRUNE_RETENTION)
        cutoffs[status] = db_now - retention
        candidate_sql = _ARCHIVE_CANDIDATE_SQL.format(schema=schema)
        write_sql = _ARCHIVE_CTE_SQL.format(schema=schema)

        while True:
            if drain_gate is not None and not drain_gate():
                break
            size = _effective_prune_batch_size(batch_size, sizer)
            _record_prune_batch_size("prune", size, sizer)
            rows = await _run_prune_archive_batch(
                conn,
                candidate_sql=candidate_sql,
                write_sql=write_sql,
                status=status,
                retention=retention,
                size=size,
                archive_interval=archive_interval,
                actor=None,
                statement_timeout_ms=statement_timeout_ms,
                sweep_name="prune",
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
            candidate_sql = _ARCHIVE_CANDIDATE_ACTOR_SQL.format(schema=schema)
            write_sql = _ARCHIVE_CTE_SQL.format(schema=schema)
            for status in TERMINAL_STATUSES:
                if actor_retention >= retention_per_status.get(status, DEFAULT_PRUNE_RETENTION):
                    continue
                while True:
                    if drain_gate is not None and not drain_gate():
                        break
                    size = _effective_prune_batch_size(batch_size, sizer)
                    _record_prune_batch_size("prune", size, sizer)
                    rows = await _run_prune_archive_batch(
                        conn,
                        candidate_sql=candidate_sql,
                        write_sql=write_sql,
                        status=status,
                        retention=actor_retention,
                        size=size,
                        archive_interval=archive_interval,
                        actor=actor_name,
                        statement_timeout_ms=statement_timeout_ms,
                        sweep_name="prune",
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
    breaker semantics as :func:`prune_terminal_jobs`, see that
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
    # predicate's own clock domain, the sibling cutoffs above were
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
            sweep_name="archive_expiry",
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
-- MATERIALIZED is essential: without it the planner may inline the
-- LIMIT-ed CTE into the UPDATE and run it as a nested loop, completing
-- more batches than the LIMIT; the window-then-update-by-id shape bounds
-- one call to batch_size rows.
WITH candidate AS MATERIALIZED (
    SELECT b.id
    FROM "{schema}".batches b
    WHERE b.status = 'active'
      AND NOT EXISTS (
        SELECT 1 FROM "{schema}".jobs
        WHERE {open_member}
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
    completed: int = await conn.fetchval(complete_stale_batches_sql(schema), batch_size)
    return completed


def complete_stale_batches_sql(schema: str) -> str:
    """The sweep's statement for *schema*; the member probe is the same
    index-served open-member predicate every terminal write's completion
    probe uses (``open_member_where``), correlated on the candidate row."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return _COMPLETE_STALE_BATCHES_SQL.format(
        schema=schema, open_member=open_member_where("b.id::text")
    )
