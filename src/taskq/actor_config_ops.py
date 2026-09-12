"""Operator surface for reading and tuning stored `{schema}.actor_config` rows.

Complements :func:`taskq.worker.startup.sync_actor_config`: that function
only ever *seeds* a row's capacity fields (``max_concurrent``,
``max_pending``, ``result_ttl``) on first registration and otherwise
leaves them untouched. This module is how an operator changes them
afterwards — on a live deployment, without a code change or a worker
restart. All three are re-read by the engine without a restart:

* ``max_concurrent`` — the dispatch query joins ``actor_config`` fresh
  on every dispatch cycle (``taskq/backend/_dispatch_sql.py``); a change
  is effective immediately.
* ``result_ttl`` — the terminal-write UPDATE recomputes
  ``result_expires_at`` from the stored value for every completing job
  (``taskq/backend/_sql_templates.py::mark_succeeded``); a change is
  effective for jobs completing after the write.
* ``max_pending`` — enqueue-side processes hold a TTL-bounded cache of
  this table (``taskq/client/_capacity.py``, default 5s staleness); a
  change is effective fleet-wide within seconds, with no redeploy.

Clearing semantics differ by field on purpose. ``--clear-max-concurrent``
writes NULL, which the dispatch SQL reads as *unlimited* — the SQL
cannot see the code literal once the row exists. ``--clear-max-pending``
and ``--clear-result-ttl`` write NULL, which their enforcement paths
read as *fall back to the ``@actor(...)`` literal* — clearing reverts an
override to the code default.

This module also provides :func:`deregister_actor` — the transactional
removal of an ``actor_config`` row with safety checks for active jobs
and enabled schedules, optional forced cancellation of pending/scheduled
jobs, optional disabling of cron schedules, and optional purging of
orphaned queues. See :class:`DeregisterResult` for the return contract.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final
from uuid import UUID

from taskq._json import loads
from taskq.backend._protocol import ConnLike
from taskq.backend._records import jsonb_param
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sweeps import (
    _apply_batch_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: the batch statement_timeout capture/restore is shared verbatim by every event-writer batch path; re-defining it here would let the two disciplines drift.
    _restore_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: same shared-discipline rationale as _apply_batch_statement_timeout.
    _validate_positive,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical pre-SQL bound validation, shared with the sweeps and the bulk cancel.
)
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
)
from taskq.exceptions import (
    ActorHasActiveJobsError,
    ActorHasEnabledSchedulesError,
    ActorNotFoundError,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "UNSET",
    "ActorConfigRow",
    "DeregisterResult",
    "Unset",
    "deregister_actor",
    "get_actor_config",
    "list_actor_configs",
    "list_actor_summaries",
    "set_actor_config_capacity",
]


class Unset:
    """Sentinel distinguishing 'leave unchanged' from an explicit ``None`` (clear)."""

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Final = Unset()


@dataclass(frozen=True, slots=True)
class ActorConfigRow:
    """Snapshot of one `{schema}.actor_config` row."""

    actor: str
    max_concurrent: int | None
    max_pending: int | None
    queue: str
    result_ttl: float | None
    metadata: dict[str, object]
    updated_at: str


@dataclass(frozen=True, slots=True)
class DeregisterResult:
    """Outcome of a ``deregister_actor`` call.

    ``actor_config_deleted`` is always ``True`` — if the row is not found,
    ``deregister_actor`` raises :class:`ActorNotFoundError` instead of
    returning a result with ``False``. The field is retained for API
    contract clarity and consumer assertions.
    """

    actor: str
    queue: str
    actor_config_deleted: bool
    schedules_disabled: int
    jobs_cancelled: int
    terminal_jobs_remaining: int
    queue_purged: bool


_LIST_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl,
       metadata::text AS metadata, updated_at::text AS updated_at
  FROM "{schema}".actor_config
 ORDER BY actor
""".strip()

_GET_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl,
       metadata::text AS metadata, updated_at::text AS updated_at
  FROM "{schema}".actor_config
 WHERE actor = $1
""".strip()

# Each capacity column is only overwritten when its paired boolean
# "touch" flag is true; otherwise the CASE expression preserves the
# current value. This lets one statement express "set to N", "clear to
# NULL", and "leave alone" for all three fields without dynamic SQL.
_SET_ACTOR_CONFIG_CAPACITY_SQL = """
UPDATE "{schema}".actor_config
   SET max_concurrent = CASE WHEN $2 THEN $3 ELSE max_concurrent END,
       max_pending    = CASE WHEN $4 THEN $5 ELSE max_pending END,
       result_ttl     = CASE WHEN $6 THEN $7 ELSE result_ttl END,
       updated_at     = clock_timestamp()
 WHERE actor = $1
RETURNING actor, max_concurrent, max_pending, queue, result_ttl,
          metadata::text AS metadata, updated_at::text AS updated_at
""".strip()


def _row_to_dataclass(row: asyncpg.Record) -> ActorConfigRow:
    return ActorConfigRow(
        actor=row["actor"],
        max_concurrent=row["max_concurrent"],
        max_pending=row["max_pending"],
        queue=row["queue"],
        result_ttl=row["result_ttl"],
        metadata=loads(row["metadata"]),
        updated_at=row["updated_at"],
    )


async def list_actor_configs(conn: ConnLike, *, schema: str = "taskq") -> list[ActorConfigRow]:
    """Return every stored `{schema}.actor_config` row, ordered by actor name."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    rows = await conn.fetch(_LIST_ACTOR_CONFIG_SQL.format(schema=schema))
    return [_row_to_dataclass(row) for row in rows]


_ACTOR_SUMMARIES_SQL = """
SELECT ac.actor, ac.max_concurrent, ac.max_pending, ac.queue,
       ac.updated_at::text AS updated_at,
       (SELECT count(*) FROM "{schema}".jobs j
        WHERE j.actor = ac.actor
        AND j.status = ANY($1::"{schema}".job_status[])) AS active_job_count,
       (SELECT count(*) FROM "{schema}".cron_schedules cs
        WHERE cs.actor = ac.actor AND cs.enabled = true) AS enabled_schedule_count
  FROM "{schema}".actor_config ac
 ORDER BY ac.actor
""".strip()


async def list_actor_summaries(conn: ConnLike, *, schema: str = "taskq") -> list[dict[str, object]]:
    """Return actor_config rows with active job and schedule counts for display."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    rows = await conn.fetch(
        _ACTOR_SUMMARIES_SQL.format(schema=schema),
        list(ACTIVE_STATUSES),
    )
    return [dict(r) for r in rows]


async def get_actor_config(
    conn: ConnLike, actor: str, *, schema: str = "taskq"
) -> ActorConfigRow | None:
    """Return the stored row for *actor*, or ``None`` if it has never been synced."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    row = await conn.fetchrow(_GET_ACTOR_CONFIG_SQL.format(schema=schema), actor)
    return _row_to_dataclass(row) if row is not None else None


def _validate_int_field(name: str, value: int | Unset | None) -> None:
    """Reject the two shapes that slip past ``isinstance(x, int) and x < 0``:
    ``bool`` (an ``int`` subclass — ``False`` would be written as 0, flooring
    the dispatch residual ``GREATEST(cap - in_flight, 0)`` and silently
    pausing the actor) and negative values."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer; got {value!r} (bool)")
    if isinstance(value, int) and value < 0:
        raise ValueError(f"{name} must be a non-negative integer; got {value!r}")


def _validate_result_ttl(value: float | Unset | None) -> None:
    """Reject bool, negative, and non-finite ``result_ttl``.

    NaN sails through ``value < 0`` (NaN compares False) and then breaks
    every completion for the actor — ``clock_timestamp() + NaN * interval
    '1 second'`` raises ``interval out of range`` in the terminal-write
    UPDATE. ±inf is rejected on the same grounds (``interval out of range``
    / meaningless expiry)."""
    if isinstance(value, bool):
        raise ValueError(
            f"result_ttl must be a non-negative number of seconds; got {value!r} (bool)"
        )
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise ValueError(f"result_ttl must be finite; got {value!r}")
        if value < 0:
            raise ValueError(f"result_ttl must be a non-negative number of seconds; got {value!r}")


async def set_actor_config_capacity(
    conn: ConnLike,
    actor: str,
    *,
    max_concurrent: int | Unset | None = UNSET,
    max_pending: int | Unset | None = UNSET,
    result_ttl: float | Unset | None = UNSET,
    schema: str = "taskq",
) -> ActorConfigRow | None:
    """Update capacity fields on an existing `{schema}.actor_config` row.

    Only fields passed as something other than :data:`UNSET` are
    changed. Pass ``None`` explicitly to clear a field — precisely what
    that means depends on the field's enforcement path: clearing
    ``max_concurrent`` makes the actor *unlimited* (the dispatch SQL
    reads a stored NULL as no cap), while clearing ``max_pending`` or
    ``result_ttl`` reverts enforcement to the ``@actor(...)`` literal
    (those paths can still see the code default). Returns ``None`` if
    *actor* has no stored row — a row is only created by
    :func:`taskq.worker.startup.sync_actor_config` at worker startup, so
    an actor must have been registered by at least one worker before its
    capacity can be tuned here.

    Raises :class:`ValueError` if ``max_concurrent`` or ``max_pending``
    is a negative integer or a ``bool`` — the same guard ``@actor(...)``
    applies at decoration time (``taskq/actor.py``), plus the bool case
    (``False`` is an ``int`` and would be written as 0). Without these,
    an operator typo here (e.g. ``--max-concurrent -5``) would write
    silently into the dispatch CTE's ``GREATEST(ac.max_concurrent -
    in_flight, 0)`` residual calculation, floor to zero, and pause the
    actor indefinitely with no error anywhere in the path. ``result_ttl``
    is likewise rejected when negative, non-finite, or ``bool`` — a
    negative TTL would set ``result_expires_at`` to a past timestamp in
    the terminal-write UPDATE (silently expiring every result the moment
    it is written), and NaN/±inf raise ``interval out of range`` in that
    same UPDATE, failing every completion for the actor.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_int_field("max_concurrent", max_concurrent)
    _validate_int_field("max_pending", max_pending)
    _validate_result_ttl(result_ttl)

    row = await conn.fetchrow(
        _SET_ACTOR_CONFIG_CAPACITY_SQL.format(schema=schema),
        actor,
        not isinstance(max_concurrent, Unset),
        None if isinstance(max_concurrent, Unset) else max_concurrent,
        not isinstance(max_pending, Unset),
        None if isinstance(max_pending, Unset) else max_pending,
        not isinstance(result_ttl, Unset),
        None if isinstance(result_ttl, Unset) else result_ttl,
    )
    return _row_to_dataclass(row) if row is not None else None


# ── deregister_actor ────────────────────────────────────────────────────

_RUNNING_STATUS: str = "running"

_DEREGISTER_CHECK_ACTOR_EXISTS_SQL = """
SELECT 1 FROM "{schema}".actor_config WHERE actor = $1
""".strip()

_DEREGISTER_CHECK_ACTIVE_JOBS_SQL = """
SELECT status, count(*) AS cnt
  FROM "{schema}".jobs
 WHERE actor = $1 AND status = ANY($2::"{schema}".job_status[])
 GROUP BY status
""".strip()

_DEREGISTER_CHECK_SCHEDULES_SQL = """
SELECT id::text FROM "{schema}".cron_schedules
 WHERE actor = $1 AND enabled = true
""".strip()

# The FROM-CTE snapshot pattern (same as backend/_cancel_bulk.py): the CTE
# captures each row's pre-cancel status — UPDATE ... RETURNING can only see
# the new value — and the repeated status predicate on the target
# re-evaluates rows concurrently modified since the snapshot (EPQ-safe).
# MATERIALIZED is load-bearing: without it the planner may inline the
# LIMIT-ed matching CTE into the UPDATE as a nested loop and update more
# rows than the LIMIT admits. The final SELECT aggregates from both CTEs
# so the same statement returns the WINDOW count (``matched_count``) as
# well as the affected rows — the drain terminates on the window count,
# never the affected count, which an EPQ drop shorts while matching rows
# remain beyond the window.
_DEREGISTER_CANCEL_PENDING_SQL = """
WITH matching AS MATERIALIZED (
    SELECT id, status AS prev_status
      FROM "{schema}".jobs
     WHERE actor = $1
       AND status IN ('pending', 'scheduled')
     ORDER BY id
     LIMIT $2
),
cancelled AS (
    UPDATE "{schema}".jobs AS j
       SET status = 'cancelled',
           finished_at = clock_timestamp(),
           error_class = 'ActorDeregistered',
           error_message = 'Job cancelled by actor deregistration (force=True)'
      FROM matching AS prev
     WHERE j.id = prev.id
       AND j.status IN ('pending', 'scheduled')
    RETURNING j.id, prev.prev_status
)
SELECT
    (SELECT count(*)::int FROM matching) AS matched_count,
    (SELECT count(*)::int FROM cancelled) AS cancelled_directly,
    (SELECT array_agg(id ORDER BY id) FROM cancelled) AS cancelled_ids,
    (SELECT array_agg(prev_status ORDER BY id) FROM cancelled) AS cancelled_prev_statuses
""".strip()

_DEREGISTER_DISABLE_SCHEDULES_SQL = """
UPDATE "{schema}".cron_schedules
   SET enabled = false
 WHERE actor = $1 AND enabled = true
""".strip()

_DEREGISTER_DELETE_ACTOR_CONFIG_SQL = """
DELETE FROM "{schema}".actor_config WHERE actor = $1
RETURNING queue
""".strip()

# The jobs guard exists because jobs.actor/jobs.queue are plain text with no
# FK: jobs enqueued to unregistered actors (an expected state — the CLI warns
# about them) are invisible to the actor_config-only guard, yet still depend
# on the queue row's max_concurrent cap and dispatch mode. A missing row
# means uncapped + strict_fifo, so purging under them silently changes both.
_DEREGISTER_PURGE_QUEUE_SQL = """
DELETE FROM "{schema}".queues
 WHERE name = $1
   AND NOT EXISTS (
       SELECT 1 FROM "{schema}".actor_config WHERE queue = $1
   )
   AND NOT EXISTS (
       SELECT 1 FROM "{schema}".jobs
        WHERE queue = $1
          AND status = ANY($2::"{schema}".job_status[])
   )
RETURNING name
""".strip()

_DEREGISTER_COUNT_TERMINAL_SQL = """
SELECT count(*) FROM "{schema}".jobs
 WHERE actor = $1 AND status = ANY($2::"{schema}".job_status[])
""".strip()


async def _finalize_deregister(
    conn: ConnLike,
    actor: str,
    *,
    purge_queue: bool,
    schema: str,
) -> tuple[str, int, bool]:
    """Delete the ``actor_config`` row and gather the tail of the result.

    Runs inside the caller's transaction: the DELETE with its
    ``ActorNotFoundError``-on-zero-rows race handling, the terminal-history
    count, and the optional queue purge — each an individually bounded
    statement, so the whole tail fits one small transaction however large
    the actor's backlog was.
    """
    deleted_rows = await conn.fetch(
        _DEREGISTER_DELETE_ACTOR_CONFIG_SQL.format(schema=schema),
        actor,
    )
    if not deleted_rows:
        # Handles the concurrent-delete race: under READ COMMITTED, a
        # concurrent transaction could delete the row between our
        # preflight check and this DELETE.
        raise ActorNotFoundError(actor)

    queue_name: str = deleted_rows[0]["queue"]

    terminal_count = await conn.fetchval(
        _DEREGISTER_COUNT_TERMINAL_SQL.format(schema=schema),
        actor,
        list(TERMINAL_STATUSES),
    )

    queue_purged = False
    if purge_queue:
        purged_name = await conn.fetchval(
            _DEREGISTER_PURGE_QUEUE_SQL.format(schema=schema),
            queue_name,
            list(ACTIVE_STATUSES),
        )
        queue_purged = purged_name is not None

    return queue_name, int(terminal_count or 0), queue_purged


async def deregister_actor(
    conn: ConnLike,
    actor: str,
    *,
    force: bool = False,
    purge_queue: bool = False,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    schema: str = "taskq",
) -> DeregisterResult:
    """Deregister an actor: delete its ``actor_config`` row with safety checks.

    **Default (force=False):**
      1. Refuse if any non-terminal jobs (pending/scheduled/running) reference
         the actor — raises :class:`ActorHasActiveJobsError`.
      2. Refuse if any enabled cron schedules reference the actor — raises
         :class:`ActorHasEnabledSchedulesError`.
      3. Delete the ``actor_config`` row.
      4. Optionally purge the orphaned queue (if ``purge_queue=True``, no
          other ``actor_config`` row references the same queue, and no
          non-terminal job in the ``jobs`` table still references it —
          ``jobs.actor``/``jobs.queue`` are plain text with no FK, so jobs
          enqueued to unregistered actors are invisible to the
          actor_config-only guard).

    **force=True:**
      1. Refuse if any running jobs reference the actor — raises
          :class:`ActorHasActiveJobsError`.
      2. Cancel pending/scheduled jobs for this actor.
      3. Disable enabled cron schedules for this actor.
      4. Delete the ``actor_config`` row.
      5. Optionally purge the orphaned queue (same guards as above).

    Terminal job history is never deleted or modified. With ``force=False``
    the whole operation runs inside a single ``conn.transaction()`` block;
    with ``force=True`` the cancel drains as bounded committed batches
    (``batch_size`` driving rows per transaction, each carrying a
    server-side ``statement_timeout`` bound with ``SET LOCAL`` semantics —
    the same capture/restore discipline the maintenance sweeps use)
    followed by one final transaction for the schedule disable, the
    delete, the terminal count, and the optional purge — so a mid-drain
    failure leaves the batches already committed as partial progress, and
    a re-run continues where it stopped (the cancel's EPQ predicates skip
    the rows earlier batches already cancelled). The drain terminates on
    the WINDOW count the driving statement returns from its own
    MATERIALIZED ``matching`` CTE, never the UPDATE's affected-row count:
    an EPQ drop (a dispatcher claiming a windowed row between the
    statement's snapshot and its row lock) shorts the affected count
    while matching rows remain beyond the window, and terminating on it
    would delete the ``actor_config`` row with uncancelled pending jobs
    still stranded against it. ``jobs_cancelled`` counts only affected
    rows. If the actor has no stored ``actor_config`` row, raises
    :class:`ActorNotFoundError`.

    .. warning::
       **Concurrent enqueue / dispatch race (TOCTOU).** The transaction
       uses READ COMMITTED isolation. Callers must quiesce the actor first
       — stop enqueuing, disable cron schedules, and wait for running jobs
       to reach a terminal state — before calling deregister.

       **Concurrent worker startup (sync_actor_config)** can re-create the
       ``actor_config`` row after this function returns, with capacity fields
       reset to ``@actor(...)`` defaults. Stop all workers for this actor
       before calling deregister.

        A job dispatched between the running check and the cancel UPDATE
        (force=True) will be left running with no ``actor_config`` row.
        When its lock expires, the leader sweep's retry branch returns it
        to **pending** with a short backoff (only jobs with no attempts
        remaining — or a cancel request still in flight — transition to a
        terminal crashed/cancelled state). Because dispatch candidates
        are drawn from ``actor_config``, that pending job is then
        stranded: it is never dispatched while the actor stays
        unregistered, and the leader's stranded-jobs detector warns about
        it on its interval. If the actor name is ever re-registered
        (e.g. a redeploy reintroduces it), the stranded job silently
        dispatches against the new code — cancel or retry it manually if
        that is not wanted.

       A cron schedule that fires between the disable UPDATE and the
       ``actor_config`` DELETE will enqueue a job that can never be
       dispatched (same stranding as enqueue-during-deregister).

       Concurrent deregistration of the last two actors sharing a queue
       may leave the queue row orphaned (both transactions see the
       other's ``actor_config`` row as still present under READ
       COMMITTED). The queue can be manually deleted if needed.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    # LIMIT 0 is a legal rowless query that would otherwise stall the
    # force drain forever (an empty window never falls below it); a zero
    # statement_timeout disables the batch's safety net outright.
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    # Actor-exists check first, as its own statement on the caller's
    # connection — ActorNotFoundError takes precedence over all other
    # checks so callers don't get misleading errors for actors that are
    # already deregistered but have stranded jobs.
    exists = await conn.fetchval(
        _DEREGISTER_CHECK_ACTOR_EXISTS_SQL.format(schema=schema),
        actor,
    )
    if not exists:
        raise ActorNotFoundError(actor)

    if not force:
        # Every statement in this branch is small and bounded — one
        # transaction keeps the refusal checks and the delete atomic.
        async with conn.transaction():
            active_rows = await conn.fetch(
                _DEREGISTER_CHECK_ACTIVE_JOBS_SQL.format(schema=schema),
                actor,
                list(ACTIVE_STATUSES),
            )
            if active_rows:
                status_counts: dict[str, int] = {
                    str(row["status"]): int(row["cnt"]) for row in active_rows
                }
                active_count = sum(status_counts.values())
                raise ActorHasActiveJobsError(actor, active_count, status_counts)

            schedule_rows = await conn.fetch(
                _DEREGISTER_CHECK_SCHEDULES_SQL.format(schema=schema),
                actor,
            )
            if schedule_rows:
                schedule_ids = [row["id"] for row in schedule_rows]
                raise ActorHasEnabledSchedulesError(actor, schedule_ids)

            schedules_disabled = 0
            jobs_cancelled = 0
            queue_name, terminal_count, queue_purged = await _finalize_deregister(
                conn, actor, purge_queue=purge_queue, schema=schema
            )
    else:
        running_rows = await conn.fetch(
            _DEREGISTER_CHECK_ACTIVE_JOBS_SQL.format(schema=schema),
            actor,
            [_RUNNING_STATUS],
        )
        if running_rows:
            running_counts: dict[str, int] = {
                str(row["status"]): int(row["cnt"]) for row in running_rows
            }
            active_count = sum(running_counts.values())
            raise ActorHasActiveJobsError(actor, active_count, running_counts, force=True)

        # Bounded drain: each batch commits its driving UPDATE plus the
        # state_change events describing it, so no transaction holds row
        # locks on more than batch_size jobs, and each batch carries a
        # server-side statement_timeout (SET LOCAL semantics, the sweeps'
        # capture/restore discipline) so the event INSERT-to-COMMIT span
        # is enforced, not merely hoped, inside the
        # RECLAIM_EVENT_VISIBILITY_DELAY margin. Termination keys on the
        # WINDOW count the statement returns from its own MATERIALIZED
        # matching CTE — never the UPDATE's affected-row count, which an
        # EPQ drop (a dispatcher claiming a windowed row mid-statement)
        # shorts while matching rows remain beyond the window. The
        # affected count drives only jobs_cancelled, so an EPQ-dropped
        # row is never reported as cancelled.
        event_batch_sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema)
        jobs_cancelled = 0
        while True:
            async with conn.transaction():
                prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
                row = await conn.fetchrow(
                    _DEREGISTER_CANCEL_PENDING_SQL.format(schema=schema),
                    actor,
                    batch_size,
                )
                count = int(row["cancelled_directly"]) if row is not None else 0
                if row is not None and count:
                    batch_ids: list[UUID] = list(row["cancelled_ids"] or [])
                    prev_statuses: dict[UUID, str] = dict(
                        zip(batch_ids, list(row["cancelled_prev_statuses"] or []), strict=True)
                    )
                    await conn.execute(
                        event_batch_sql,
                        batch_ids,
                        [
                            jsonb_param(
                                {
                                    # The row's real prior status, not a
                                    # 'pending_or_scheduled' placeholder —
                                    # same contract as _cancel_bulk.py.
                                    "from_state": prev_statuses[jid],
                                    "to_state": "cancelled",
                                    "reason": "actor_deregistered",
                                }
                            )
                            for jid in batch_ids
                        ],
                        "state_change",
                    )
                    # Counted only now, after the batch's event write
                    # succeeded: an aborted batch contributes no phantom
                    # cancellations.
                    jobs_cancelled += count
                # Success path only: restore the caller's timeout inside
                # the still-open transaction; on error the rollback has
                # already discarded the SET LOCAL.
                await _restore_statement_timeout(conn, prev_timeout)
            matched_count = int(row["matched_count"]) if row is not None else 0
            if matched_count < batch_size:
                break

        async with conn.transaction():
            disable_result = await conn.execute(
                _DEREGISTER_DISABLE_SCHEDULES_SQL.format(schema=schema),
                actor,
            )
            schedules_disabled = int(disable_result.split()[-1]) if disable_result else 0
            queue_name, terminal_count, queue_purged = await _finalize_deregister(
                conn, actor, purge_queue=purge_queue, schema=schema
            )

    return DeregisterResult(
        actor=actor,
        queue=queue_name,
        actor_config_deleted=True,
        schedules_disabled=schedules_disabled,
        jobs_cancelled=jobs_cancelled,
        terminal_jobs_remaining=terminal_count,
        queue_purged=queue_purged,
    )
