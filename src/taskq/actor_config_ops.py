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

Finally, :func:`move_actor_queue` is the one-step queue move: the
actor's pending/scheduled backlog is rewritten onto the target queue as
bounded committed batches, then one final transaction locks and flips the
stored assignment and carries the source queue's ``queues`` row to the
target when the target has none — so old-queue strays drain through the
target's consumers and worker boot stays consistent at every intermediate
state of the rolling deploy. The actor's left-behind running rows are
untouched; each finishes on the worker that claimed it, and any re-pend
of one (failure retry, lease/heartbeat reclaim, operator retry) keeps the
row's queue label as an audit trail while dispatch routes it by the
actor's CURRENT assignment (``taskq/backend/_dispatch_sql.py``'s routing
contract), so the running-job tail drains through the target's consumers
rather than stranding on the retired source queue. A mid-drain abort
re-raises after the ``actor-queue-move-aborted`` event names the
committed-so-far count. See :class:`ActorQueueMoveResult` for the
return contract.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final
from uuid import UUID

from taskq._json import loads
from taskq.backend._protocol import (
    ConnLike,
    _validate_queue_name,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical queue-name rule lives with QueueName; a second copy here would drift from the reservation-namespace ban it encodes
)
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
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

logger = get_logger(__name__)

__all__ = [
    "UNSET",
    "ActorConfigRow",
    "ActorQueueMoveResult",
    "DeregisterResult",
    "Unset",
    "deregister_actor",
    "get_actor_config",
    "list_actor_configs",
    "list_actor_summaries",
    "move_actor_queue",
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

_SELECT_ACTOR_CONFIGS_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl,
       metadata::text AS metadata, updated_at::text AS updated_at
  FROM "{schema}".actor_config
 WHERE actor = ANY($1::text[])
 ORDER BY actor
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


async def select_actor_configs(
    conn: ConnLike, actors: Sequence[str], *, schema: str = "taskq"
) -> list[ActorConfigRow]:
    """Return the stored rows for *actors*, ordered by actor name.

    Bounded by the caller's own list rather than reading the whole table:
    a worker resolves capacity only for the actors it registered, and the
    stored population is fleet-wide.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    if not actors:
        return []
    rows = await conn.fetch(_SELECT_ACTOR_CONFIGS_SQL.format(schema=schema), list(actors))
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


# ── move_actor_queue ────────────────────────────────────────────────────

# Preflight read (no lock): ActorNotFoundError takes precedence over every
# other outcome, exactly as deregister's exists-check does, so callers get
# the right error for an actor that was never registered.
_MOVE_GET_ASSIGNMENT_SQL = """
SELECT queue FROM "{schema}".actor_config WHERE actor = $1
""".strip()

# The assignment lock is taken in the FINAL transaction, after the backlog
# drain, and before the flip — so a concurrent move/deregister/boot-upsert
# for the same actor serializes there instead of interleaving with the
# write. Keyed single row by the table's primary key.
_MOVE_LOCK_ASSIGNMENT_SQL = """
SELECT queue FROM "{schema}".actor_config WHERE actor = $1 FOR UPDATE
""".strip()

# Fill-in, never overwrite: the INSERT .. SELECT carries the source queue's
# row (mode + max_concurrent — the round_robin/cap config a naive move
# silently loses when the target has no row) but only when the target has
# no row of its own; ON CONFLICT DO NOTHING keeps a configured target
# standing. A source queue with no row inserts nothing, leaving the target
# on the defaults — the correct carry of "unconfigured" too.
_MOVE_CARRY_QUEUE_ROW_SQL = """
INSERT INTO "{schema}".queues (name, mode, max_concurrent)
SELECT $2, q.mode, q.max_concurrent
  FROM "{schema}".queues q
 WHERE q.name = $1
ON CONFLICT (name) DO NOTHING
""".strip()

# The backlog rewrite: bounded committed batches, the deregister force-drain
# doctrine verbatim (this module's own precedent for an actor-scoped
# backlog rewrite). MATERIALIZED is load-bearing — without it the planner
# may inline the LIMIT-ed matching CTE into the UPDATE as a nested loop and
# update more rows than the LIMIT admits. The repeated queue/status
# predicate on the UPDATE re-evaluates rows concurrently modified since the
# snapshot (EPQ-safe): a dispatcher claiming a windowed row between the
# statement's snapshot and its lock is dropped from the affected count, and
# the drain terminates on the WINDOW count (matched_count), never the
# affected count, so an EPQ drop cannot end it early with matching rows
# beyond the window. No job_events row is written: a queue re-label is a
# routing change, not a state change — the row stays pending/scheduled and
# the schema's event kinds (state_change | cancel_request | heartbeat_miss
# | progress) have no member for it.
_MOVE_BACKLOG_BATCH_SQL = """
WITH matching AS MATERIALIZED (
    SELECT id
      FROM "{schema}".jobs
     WHERE actor = $1 AND queue = $2 AND status IN ('pending', 'scheduled')
     ORDER BY id
     LIMIT $4
),
moved AS (
    UPDATE "{schema}".jobs AS j
       SET queue = $3
      FROM matching AS m
     WHERE j.id = m.id
       AND j.queue = $2
       AND j.status IN ('pending', 'scheduled')
    RETURNING j.id
)
SELECT
    (SELECT count(*)::int FROM matching) AS matched_count,
    (SELECT count(*)::int FROM moved) AS moved_count
""".strip()

_MOVE_COUNT_RUNNING_SQL = """
SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND status = 'running'
""".strip()

# Measured AFTER the flip: the drain moves every row it can see at batch
# time, but a stale producer can land a fresh row on the source queue in
# the window between the drain's last batch and this count (or, by
# deliberate design, after the flip entirely). The operator's "when can I
# stop consuming the source queue" decision needs this exact residual, not
# the drain's own moved-count, which only ever reports what THIS call
# already rewrote.
_MOVE_COUNT_PENDING_ON_OLD_QUEUE_SQL = """
SELECT count(*) FROM "{schema}".jobs
 WHERE actor = $1 AND queue = $2 AND status IN ('pending', 'scheduled')
""".strip()

# Keyed single row by the table's primary key.
_MOVE_SET_ASSIGNMENT_SQL = """
UPDATE "{schema}".actor_config
   SET queue = $2, updated_at = clock_timestamp()
 WHERE actor = $1
""".strip()


def _affected_count(status: str | None) -> int:
    """Row count from an asyncpg command-status tag (``"UPDATE 3"`` → 3)."""
    if not status:
        return 0
    return int(status.split()[-1])


@dataclass(frozen=True, slots=True)
class ActorQueueMoveResult:
    """Outcome of a ``move_actor_queue`` call.

    ``jobs_moved`` counts the actor's pending+scheduled rows rewritten onto
    the target queue by THIS call (the backlog that now drains through the
    target's consumers; a re-run after an aborted move counts only the
    remainder it moved itself). ``running_jobs_left`` counts the actor's
    running rows, which are deliberately untouched — each finishes on the
    worker that claimed it, and a row that instead re-pends (a failure
    retry, a lease/heartbeat reclaim, an operator retry) keeps its
    original queue label as an audit trail but is ROUTED, at dispatch, by
    the actor's current stored assignment: the tail drains through the
    target queue's consumers and never strands on the retired source
    queue. ``queues_row_carried`` is ``True`` only when the target queue
    had no row and inherited the source queue's mode and max_concurrent;
    a configured target stands unchanged.
    """

    actor: str
    from_queue: str
    to_queue: str
    jobs_moved: int
    running_jobs_left: int
    queues_row_carried: bool
    #: Pending/scheduled rows still carrying ``from_queue``'s label,
    #: counted after the flip. The drain does not chase every row (a
    #: stale producer keeps landing strays on the source queue), so this
    #: is the operator's only supported way to know when the source
    #: queue's consumers can safely stop.
    pending_jobs_on_old_queue: int = 0


async def move_actor_queue(
    conn: ConnLike,
    actor: str,
    new_queue: str,
    *,
    schema: str = "taskq",
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
) -> ActorQueueMoveResult:
    """Move an actor to a different queue in ONE operator action.

    This is the one-step replacement for the four-write lockstep (code
    literal + stored row + consumed-queue set + target ``queues`` row)
    whose fail-closed half refused worker boot mid-move. Two phases:

    * **The backlog drain** runs first, as bounded committed batches (the
      deregister force-drain doctrine: MATERIALIZED window CTE with a
      LIMIT, per-batch server-side ``statement_timeout`` with the sweeps'
      capture/restore discipline, termination on the WINDOW count so an
      EPQ drop cannot end it early). Each batch rewrites up to
      ``batch_size`` of the actor's OWN pending/scheduled rows onto the
      target queue — the actor predicate keeps a neighbor's rows on the
      source queue — so they drain through the target's consumers. Running
      rows are untouched: each finishes on the worker that claimed it, and
      a row that instead re-pends keeps its original queue label as an
      audit trail but is routed at dispatch by the actor's CURRENT
      assignment (``taskq/backend/_dispatch_sql.py``'s routing contract:
      ``started_at IS NOT NULL`` rows follow ``actor_config.queue``), so
      the running-job tail drains through the target's consumers too. A
      crash mid-drain leaves the batches already committed as partial
      progress; a re-run continues where it stopped (the drain's queue
      predicate skips rows earlier batches moved), and the abort itself
      is observable — the ``actor-queue-move-aborted`` event carries the
      committed-so-far count.
    * **The flip** then lands in ONE final transaction: the assignment row
      is locked (``FOR UPDATE``), the target queue's row inherits the
      source queue's ``mode`` and ``max_concurrent`` when (and only when)
      the target has no row — otherwise a round_robin queue's move
      silently degraded the actor to strict_fifo and dropped its cap; a
      configured target is never overwritten — the actor's running count
      is read, and the stored assignment is rewritten. Capacity fields,
      ``max_attempts``/``retry_kind``, and metadata are untouched, and the
      source queue's row is never touched (other actors may still live
      there). The cron leader's fires follow the stored queue, so they
      land on the target from the flip on.

    The flip lands AFTER the drain so a crash between the phases leaves a
    re-runnable state (the stored assignment still names the source queue,
    so a re-run re-drains and re-flips); a crash after the flip means the
    move was already complete. Any failure after the first committed
    batch — the drain's own aborts (a per-batch ``statement_timeout``, a
    lost connection) and the flip's races alike — is re-raised AFTER the
    ``actor-queue-move-aborted`` event logs the durable state: *actor*,
    *from_queue*, *to_queue*, *jobs_moved* (this call's committed count),
    and *error_class*. The committed rows are real partial progress no
    caller can see in a raised exception; the event is the operator's
    evidence that a re-run continues rather than restarts. Preflight
    refusals (unknown actor, same-queue no-op, invalid queue name) raise
    before any write and log nothing. Jobs a stale producer enqueues to
    the source queue after the flip are served by source-queue consumers
    (see the rolling-deploy note below) — never stranded, and never
    served by the target's: producer placement governs a never-claimed
    row's routing, which is exactly what keeps the stray contract
    distinguishable from the re-pended tail's.

    Rolling deploys: run this before, during, or after deploying the
    matching ``@actor(queue=...)`` literal — in any order. At every
    intermediate state the stored row names one queue and a differing
    literal only logs ``actor-config-queue-override`` at boot, so workers
    on either side of the window boot, and the startup UPSERT preserves
    the stored assignment so neither side can undo the move. Keep workers
    consuming the source queue until every producer carries the new
    literal, then drop it.

    Raises :class:`ActorNotFoundError` when the actor has no stored row
    (nothing to move — a row only exists after a worker has synced it),
    :class:`ValueError` when *new_queue* equals the current assignment (a
    move onto itself is a no-op the operator should be told about, not
    silently executed), when *new_queue* is not a valid queue name (the
    same rule every producer's queue name follows), or when the assignment
    changed concurrently mid-move (a second operator's move or a
    deregister won the race).
    """

    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_queue_name(new_queue)
    # LIMIT 0 would stall the drain forever; a zero statement_timeout
    # disables the batch's safety net outright (deregister's guards).
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    # Preflight, no lock: error precedence before any work — an unknown
    # actor must not drain zero rows and then fail confusingly at the flip.
    from_queue = await conn.fetchval(
        _MOVE_GET_ASSIGNMENT_SQL.format(schema=schema),
        actor,
    )
    if from_queue is None:
        raise ActorNotFoundError(actor)
    if new_queue == from_queue:
        raise ValueError(
            f"actor {actor!r} is already assigned to queue {new_queue!r}; "
            "a move onto the same queue is a no-op"
        )

    # Phases 1 and 2 run under one abort reporter: every failure after
    # the first committed batch leaves durable partial progress (the
    # drain commits per batch; a flip failure rolls back only the flip),
    # and a raised exception carries no count — the structured event is
    # the operator's only evidence that a re-run continues rather than
    # restarts. The preflight refusals above raised before any write, so
    # they correctly never reach this handler.
    jobs_moved = 0
    try:
        # Phase 1 — the bounded backlog drain.
        while True:
            async with conn.transaction():
                prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
                row = await conn.fetchrow(
                    _MOVE_BACKLOG_BATCH_SQL.format(schema=schema),
                    actor,
                    from_queue,
                    new_queue,
                    batch_size,
                )
                # Success path only: restore the caller's timeout inside the
                # still-open transaction; on error the rollback has already
                # discarded the SET LOCAL.
                await _restore_statement_timeout(conn, prev_timeout)
            matched = int(row["matched_count"]) if row is not None else 0
            # Affected rows only — an EPQ-dropped row is never reported as moved.
            jobs_moved += int(row["moved_count"]) if row is not None else 0
            if matched < batch_size:
                break

        # Phase 2 — the flip, in one transaction.
        async with conn.transaction():
            locked_from: str | None = await conn.fetchval(
                _MOVE_LOCK_ASSIGNMENT_SQL.format(schema=schema),
                actor,
            )
            if locked_from is None:
                # The concurrent-delete race (a deregister won between the
                # preflight and this lock) — same handling as deregister's
                # DELETE-zero-rows case.
                raise ActorNotFoundError(actor)
            if locked_from != from_queue:
                detail = (
                    "the same move completed concurrently"
                    if locked_from == new_queue
                    else f"it now names {locked_from!r}"
                )
                raise ValueError(
                    f"actor {actor!r}'s assignment changed while the move was "
                    f"draining ({detail}); re-run against the current assignment"
                )

            carry_status = await conn.execute(
                _MOVE_CARRY_QUEUE_ROW_SQL.format(schema=schema),
                from_queue,
                new_queue,
            )
            queues_row_carried = _affected_count(carry_status) > 0

            running_left = await conn.fetchval(
                _MOVE_COUNT_RUNNING_SQL.format(schema=schema),
                actor,
            )

            await conn.execute(
                _MOVE_SET_ASSIGNMENT_SQL.format(schema=schema),
                actor,
                new_queue,
            )

            # After the flip, so a stray landed on the source queue during
            # the drain window is already counted in the residual an
            # operator plans against.
            pending_on_old_queue = await conn.fetchval(
                _MOVE_COUNT_PENDING_ON_OLD_QUEUE_SQL.format(schema=schema),
                actor,
                from_queue,
            )
    except Exception as exc:
        # Log-then-reraise, never swallow: the count names writes that are
        # already committed and that the raised exception cannot carry.
        # error_class only (not the message): the exception text leaves the
        # trust boundary for whatever telemetry backend is configured, and
        # asyncpg str() appends the server's DETAIL quoting row values.
        logger.error(
            "actor-queue-move-aborted",
            actor=actor,
            from_queue=from_queue,
            to_queue=new_queue,
            jobs_moved=jobs_moved,
            error_class=type(exc).__name__,
        )
        raise

    return ActorQueueMoveResult(
        actor=actor,
        from_queue=from_queue,
        to_queue=new_queue,
        jobs_moved=jobs_moved,
        running_jobs_left=int(running_left or 0),
        queues_row_carried=queues_row_carried,
        pending_jobs_on_old_queue=int(pending_on_old_queue or 0),
    )


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
