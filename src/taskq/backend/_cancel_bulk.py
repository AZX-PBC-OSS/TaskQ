"""Bulk cancel SQL implementation for PostgresBackend.

Two-statement pattern — pending/scheduled first, then running — mirroring
the single-job ``write_cancel_request`` path:

1. ``cancel_pending_scheduled`` — UPDATE pending/scheduled rows to
   terminal ``cancelled`` with EPQ-safe predicates on the target table.
2. ``cancel_running`` — UPDATE running rows with ``cancel_phase=0`` to
   ``cancel_phase=1`` (cooperative cancel), using a fresh snapshot that
   catches jobs dispatched between statements.

The two-statement approach eliminates the race where a job transitioning
``pending→running`` mid-statement escapes both CTEs in a single-shot
design: statement 1's EPQ guard rejects the now-running row, and
statement 2's fresh snapshot sees it as running and sets
``cancel_phase=1``.

The pair runs as bounded fixpoint ROUNDS (``_MAX_CANCEL_DRAIN_ROUNDS``):
round N's pending/scheduled arm walks a keyset window forward, and a
matching RUNNING row rescheduled mid-drain (a denial snooze, a shutdown
interrupt, a crash reclaim, a consumer retry) lands pending/scheduled at
an id the pending arm has already passed, where the running arm (strictly
after it, matching only ``running AND cancel_phase=0``) can never see it
(#237: the pre-rounds drain returned normally with such a row uncancelled,
contradicting the "cancels EVERY matching job" contract below). The next
round's fresh cursor re-walks from the bottom of the key space and picks
the straggler up; the drain stops at the first round that matched nothing,
capped at the constant bound so sustained concurrent churn cannot turn
the fixpoint into an unbounded loop.

Completeness is scoped to those rounds, not absolute: each statement
cancels every matching job it windows, and one call returns the complete
:class:`BulkCancelResult` plus NOTIFY targets, but the work executes as
a sequence of bounded committed batches (``batch_size`` driving-CTE rows
per transaction), not one unbounded transaction, and the call's
completeness is bounded by the fixpoint. A match set still being re-fed
by concurrent churn when the round cap is reached returns normally with
a residual a re-run converges (the EPQ predicates skip everything
earlier rounds cancelled), and a row re-pended behind the keyset cursor
inside the final round's own passes is likewise left for that re-run:
the same deliberately non-atomic contract the drain has always documented
for a concurrent enqueue slipping a new matching row in between batches.
The drain terminates on the WINDOW count: the
``matched_count`` aggregate each driving statement returns from its own
MATERIALIZED ``matching`` CTE — never on the UPDATE's affected-row
count: under READ COMMITTED a row windowed by the CTE that a dispatcher
claims (``pending→running``) between the statement's snapshot and the
UPDATE's row lock fails the EPQ re-check on the target and drops out of
the affected count, so an affected-count termination would abandon the
tail of the match set while the window was still full. A batch whose
window was full means more matching rows may remain, so the drain keeps
going; the affected count drives only the result totals and the event
writes. A mid-operation failure therefore leaves partial progress
rather than rolling everything back: a re-run continues where it
stopped, for the same reason. Each batch's ``job_events`` rows are written
by the same bounded transaction as their driving UPDATE, and the batch
carries a server-side ``statement_timeout`` (``SET LOCAL`` semantics,
the same capture/restore discipline the maintenance sweeps use), so the
INSERT-to-COMMIT span is not merely kept inside the margin
:data:`taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY` documents — the
timeout is the enforcement — with one batched ``unnest`` INSERT per
event kind, never one round trip per row.

NOTIFY is sent by the caller (``PostgresBackend.cancel_where``) after the
drain completes, because the ``taskq.cancel.notify_sent`` counter lives
in ``postgres.py``.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Final, NamedTuple
from uuid import UUID

import asyncpg

from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import BulkCancelResult, ConnLike, JobFilter
from taskq.backend._records import jsonb_param
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend._sweeps import (
    _apply_batch_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: the batch statement_timeout capture/restore is shared verbatim by every event-writer batch path; re-defining it here would let the two disciplines drift.
    _restore_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: same shared-discipline rationale as _apply_batch_statement_timeout.
    _validate_positive,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical pre-SQL bound validation, shared with the sweeps and deregistration.
)
from taskq.connections import (
    _RetryGuard,  # pyright: ignore[reportPrivateUsage]  # Why: the per-attempt pool discipline the dead-on-acquire retry hands to this drain's batch op — the annotation seam for _one_batch below; a local copy would drift from the discipline it documents.
    _with_fresh_connection_retry,  # pyright: ignore[reportPrivateUsage]  # Why: the one implementation of the dead-on-acquire retry, shared with the enqueue paths — a local copy would drift from the discipline it documents.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    CANCEL_ORIGIN_PENDING,
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
)

__all__ = ["_cancel_where"]

# Sorts below every UUID, so the first keyset pass in
# `_drain_cancel_batches` is unbounded on the low side. A sentinel rather
# than NULL so the cursor parameter has one type on every pass and the
# statement keeps a single cached plan.
_UUID_MIN = UUID(int=0)

#: The hard bound on the two-arm fixpoint rounds `_cancel_where` runs
#: (see the module docstring). Round 1 is the drain itself; every later
#: round exists to catch rows a concurrent re-pend moved BEHIND a
#: previous round's keyset cursor: a denial snooze, a shutdown
#: interrupt, a crash reclaim, a consumer retry. No single
#: forward-only pass can see them, because the pending arm has already
#: windowed past their ids and the running arm matches only
#: ``status='running' AND cancel_phase=0`` (#237).
#:
#: Why a hard cap rather than "loop while progress": each round can only
#: match rows a concurrent writer re-fed into the pending/scheduled (or
#: freshly claimable running) population DURING the previous round:
#: a claim/fail/retry cycle on matching rows under live dispatch is a
#: steady feed, so an uncapped fixpoint is an unbounded loop under
#: exactly the production churn bulk cancel exists for. The cap keeps
#: one call's total work a constant multiple of one two-arm drain
#: (at most 3 rounds) while every per-batch cost bound is unchanged:
#: the keyset window, the ``= ANY`` restriction clause, the per-batch
#: custom-plan pin, the statement_timeout. Rows still being re-fed when the
#: cap is reached are left for a re-run, the same non-atomic contract
#: the drain already documents for concurrent enqueues: EPQ predicates
#: skip everything earlier rounds cancelled, so a re-run is resumable,
#: never double-counted.
#:
#: THE SHAPE (vendor/river's JobDeleteMany): draining a filtered set as
#: repeated bounded predicate windows rather than one unbounded
#: statement. River's JobDeleteMany
#: (vendor/river/riverdriver/riverpgxv5/internal/dbsqlc/
#: river_job.sql:177-198) is one ``LIMIT``-ed, ``FOR UPDATE SKIP
#: LOCKED`` predicate window whose callers re-invoke until the
#: predicate stops matching: the rescuer's loop
#: (vendor/river/internal/maintenance/job_rescuer.go:195-259) is the
#: in-repo instance of the shape: keep fetching batches, break when one
#: comes back under the limit. The rounds adopt that
#: re-scan-until-satisfied instinct in place of the single forward-only
#: pass the pre-#237 drain made. Two deliberate divergences: every arm
#: pages on a keyset cursor inside its drain (no batch re-walks rows an
#: earlier batch of the same arm already handled, where a bare
#: predicate window re-evaluates them), and the loop is hard-capped:
#: river's loop belongs to a maintenance daemon and legitimately runs
#: as long as the feed does, but for a single API call that shape is an
#: unbounded loop under sustained churn; the cap is what keeps one call
#: a constant multiple of one drain.
_MAX_CANCEL_DRAIN_ROUNDS: Final = 3


class NotifyTarget(NamedTuple):
    """A running job that needs a post-commit NOTIFY."""

    job_id: UUID
    worker_id: UUID


async def _apply_batch_plan_mode(conn: ConnLike) -> str:
    """Pin the batch's statements to custom plans; return the value to restore.

    Each arm issues one fixed statement text per batch on a pooled
    connection, so a deep drain crosses asyncpg's prepare threshold and
    then the plancache's generic-plan threshold mid-drain. A generic plan
    binds the keyset cursor as an unknown: the planner then prices a
    whole-table scan below the cursor-bounded index walk, and the re-walk
    the cursor exists to remove returns from that batch on (measured:
    post-threshold batches discard (N-1) * batch_size rows again while
    custom-plan batches discard none). ``force_custom_plan`` keeps every
    batch on the plan its actual cursor value implies.

    Same capture/restore discipline as the batch ``statement_timeout``
    (``_apply_batch_statement_timeout``), and ``fetch``/``execute`` for
    the same reason it documents: that is the complete duck-typing
    surface every ConnLike wrapper in the suite proxies.
    """
    prev_rows = await conn.fetch("SELECT current_setting('plan_cache_mode')")
    if not prev_rows:
        # plan_cache_mode is a registered GUC with a value in every
        # session; no row here means the server answered something the
        # drain cannot restore, so failing loudly beats guessing.
        raise RuntimeError("current_setting('plan_cache_mode') returned no value")
    prev = str(prev_rows[0]["current_setting"])
    await conn.execute("SELECT set_config('plan_cache_mode', 'force_custom_plan', true)")
    return prev


async def _restore_plan_mode(conn: ConnLike, prev: str) -> None:
    """Restore the ``plan_cache_mode`` captured by :func:`_apply_batch_plan_mode`.

    Success path only, inside the still-open transaction; on the error
    path the rollback has already discarded the SET LOCAL.
    """
    await conn.execute("SELECT set_config('plan_cache_mode', $1, true)", prev)


async def _drain_cancel_batches(
    pool: asyncpg.Pool,
    statement: str,
    params: list[object],
    batch_size: int,
    statement_timeout_ms: int,
    handle_batch: Callable[[ConnLike, asyncpg.Record], Awaitable[None]],
) -> None:
    """Execute *statement* as bounded committed batches until the match set
    is drained.

    Each iteration is one committed transaction: the driving CTE (limited
    to *batch_size* rows) plus whatever event writes *handle_batch* issues
    on the same connection, so a batch's events can never commit without
    the state change they describe. The transaction carries a server-side
    ``statement_timeout`` bound via ``set_config(..., true)`` (``SET
    LOCAL`` semantics with a bindable value) using the same
    capture/restore discipline as the maintenance sweeps: the previous
    value is restored on the success path inside the still-open
    transaction, and the error path needs no restore because the
    rollback discards a ``SET LOCAL``. The batch likewise pins
    ``plan_cache_mode=force_custom_plan`` (same ``SET LOCAL``
    discipline): the drain issues one statement text per arm, so past
    asyncpg's prepare threshold the plancache would flip to a generic
    plan that can no longer see the keyset cursor's selectivity, and the
    re-walk the cursor exists to prevent returns mid-drain.

    Termination keys on the WINDOW count — the ``matched_count``
    aggregate the statement returns from its own MATERIALIZED
    ``matching`` CTE — never on the UPDATE's affected-row count: under
    READ COMMITTED a row windowed by the CTE that a dispatcher claims
    (``pending→running``) between the statement's snapshot and the
    UPDATE's row lock fails the EPQ re-check and drops out of the
    affected count, so an affected-count termination abandons the tail
    of the match set while the window was still full. A full window
    means more matching rows may remain, so the drain keeps going; the
    affected count drives only the result totals and the event writes.

    Deadlock is retried per batch (3 attempts, exponential backoff with
    jitter). *handle_batch* must append its ids only after its event
    writes succeed, so a deadlocked batch contributes no phantom ids; the
    retry re-runs the CTE, which no longer matches rows an earlier
    committed batch cancelled (EPQ predicates) but does re-match this
    batch's rolled-back rows — progress is never lost and nothing is
    counted twice.

    Each batch attempt runs under ``_with_fresh_connection_retry`` so the
    pool handing out a connection the server has already killed (its
    first statement fails locally with ``asyncpg.InternalClientError``
    before ``connection_lost`` lands) costs one transparent retry on a
    genuinely fresh connection instead of escaping ``cancel_where`` as a
    raw driver state error.
    """

    async def _one_batch(guard: _RetryGuard, batch_cursor: UUID) -> asyncpg.Record | None:
        """One committed batch of the drain on a pooled connection.

        Defined once, taking the keyset cursor explicitly: binding it
        per-iteration inside the ``while`` body would capture a variable
        the loop reassigns. The retry guard's flag is marked only after
        the transaction's COMMIT is acknowledged — a parked/dead
        connection failing any statement INSIDE the transaction rolled
        the whole batch back server-side (the drain re-runs it: EPQ
        predicates skip whatever earlier batches committed, the cursor
        is unchanged, nothing is cancelled or counted twice), while past
        the COMMIT the flag refuses the wrapper's retry (#236).
        """
        async with guard.checkout() as conn:
            async with conn.transaction():
                prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
                prev_plan_mode = await _apply_batch_plan_mode(conn)
                batch_row = await conn.fetchrow(statement, *params, batch_cursor, batch_size)
                if batch_row is not None:
                    await handle_batch(conn, batch_row)
                # Success path only: restore the caller's settings
                # inside the still-open transaction; on error the
                # rollback has already discarded the SET LOCALs.
                await _restore_plan_mode(conn, prev_plan_mode)
                await _restore_statement_timeout(conn, prev_timeout)
        guard.mark_wrote()
        return batch_row

    # The keyset cursor: the greatest id this drain has WINDOWED so far.
    # Starts below every UUID so the first pass is unbounded on the low
    # side, then advances past each batch so the next pass resumes where
    # this one stopped instead of re-walking what it already handled.
    cursor = _UUID_MIN
    while True:
        row: asyncpg.Record | None = None
        for attempt in range(3):
            try:
                row = await _with_fresh_connection_retry(
                    pool,
                    partial(_one_batch, batch_cursor=cursor),
                    operation="cancel_where",
                )
                break
            except asyncpg.DeadlockDetectedError:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.1 * (2**attempt) + random.random() * 0.05)
        matched_count = int(row["matched_count"]) if row is not None else 0
        if matched_count < batch_size:
            return
        # Advance past everything this batch WINDOWED, not merely what it
        # cancelled: a row that failed the EPQ re-check was claimed by a
        # dispatcher between the snapshot and the row lock, so it is no
        # longer this statement's to cancel. Re-windowing it would make
        # the drain spin on it forever without matched_count ever falling
        # below batch_size; statement 2's fresh snapshot is what picks it
        # up, which is the hand-off the two-statement design exists for.
        # `last_id` is NULL only for an empty window, which cannot reach
        # here (matched_count == batch_size implies a full one).
        if row is not None and row["last_id"] is not None:
            cursor = row["last_id"]


async def _cancel_where(
    pool: asyncpg.Pool,
    schema: str,
    sql: SqlTemplates,  # kept: the backend caller holds the pre-rendered bundle and passes it through; the batched event INSERT below renders from the schema directly
    filter: JobFilter,
    reason: str | None,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
) -> tuple[BulkCancelResult, list[NotifyTarget]]:
    # Defence-in-depth: re-validate the schema identifier at the call site
    # (docs/architecture.md §Identifier validation) — construction-time
    # validation alone is single-point.
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    # LIMIT 0 is a legal rowless query that would otherwise stall the
    # drain forever (an empty window never falls below it); a zero
    # statement_timeout disables the batch's safety net outright.
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    filter_sql = build_filter_conditions(filter)
    conditions_str = " AND ".join(filter_sql.conditions) if filter_sql.conditions else "TRUE"
    params = list(filter_sql.params)
    # The filter params occupy $1..$n; the batch LIMIT binds as the next
    # positional parameter, appended after them at every execute site.
    cursor_ph = len(params) + 1
    limit_ph = len(params) + 2

    # Statement 1: cancel pending/scheduled → terminal 'cancelled'
    # EPQ-safe: predicates on the target table (j.status) are re-evaluated
    # for concurrently-modified rows.
    cancel_ps_sql = f"""
    -- MATERIALIZED is load-bearing: without it the planner may inline the
    -- LIMIT-ed matching CTE into the UPDATE as a nested loop and update
    -- more rows than the LIMIT admits.
    -- The window is a keyset page, not a fresh scan: `id > cursor` starts
    -- each pass where the previous one stopped, and ORDER BY id is what
    -- gives the cursor its meaning and keeps the scan on an id-ordered
    -- index. Without the cursor, every pass re-walks from the bottom of
    -- the key space and discards the rows earlier batches already moved
    -- to 'cancelled' (measured: batch N discards (N-1) * batch_size —
    -- the drain's quadratic term), and at fleet scale the later batches
    -- trip their own statement_timeout and strand the tail.
    WITH matching AS MATERIALIZED (
        SELECT id, status
        FROM "{schema}".jobs
        WHERE {conditions_str}
          AND status IN ('pending', 'scheduled')
          AND id > ${cursor_ph}::uuid
        ORDER BY id
        LIMIT ${limit_ph}
    ),
    -- This batch's ids collapsed to ONE array value, so the UPDATE below
    -- can restrict on `j.id = ANY (<array>)`. That spelling is the whole
    -- point, and it is a planner-structural choice rather than a hint:
    --
    --   `UPDATE jobs j FROM matching WHERE j.id = matching.id` makes the
    --   id correspondence a JOIN qualifier between two relations, and a
    --   join leaves the planner free to pick the scan method and join
    --   order for `jobs`. At these row counts it picks a Seq Scan of
    --   `jobs` hash-joined against the CTE, with the status predicate
    --   applied as a post-scan filter — so every batch re-visits and
    --   re-rejects every row earlier batches already moved to
    --   'cancelled', and the drain is quadratic in backlog depth.
    --   MATERIALIZED does NOT prevent this: it fixes what the join's
    --   inner side contains, not how the outer side is scanned.
    --
    --   `j.id = ANY (<array>)` is instead a RESTRICTION clause on `jobs`
    --   alone. There is no join to reorder, and the planner answers it
    --   with one primary-key probe per id (measured: `Index Cond: id =
    --   ANY (...)` on jobs_pkey) — the Seq Scan is merely costed, not
    --   chosen, and the batch's plan pin keeps that choice being made
    --   with this batch's real cursor value rather than a generic one.
    batch_ids AS MATERIALIZED (
        -- The last element is the greatest id this batch windowed — the
        -- drain's next cursor. PostgreSQL has no max(uuid) aggregate, so
        -- the ordered array carries it; the ORDER BY is stated explicitly
        -- rather than assumed from `matching`'s scan order because the
        -- cursor is the drain's only re-walk bound.
        SELECT array_agg(id ORDER BY id) AS ids,
               (array_agg(id ORDER BY id))[count(*)] AS last_id
        FROM matching
    ),
    cancelled AS (
        UPDATE "{schema}".jobs AS j
        -- Same cancel-origin marker the single-job cancel_pending_scheduled
        -- path stamps: the same outcome must read the same way whichever
        -- path produced it, or a cancelled-jobs dashboard splits into two
        -- populations that mean one thing and only one of them carries an
        -- explanation. Row-only, like the single-job path — the event
        -- detail shape stays {{from_state, to_state}}.
        SET status = 'cancelled', finished_at = clock_timestamp(),
            error_class = '{CANCEL_ORIGIN_PENDING}'
        WHERE j.id = ANY ((SELECT ids FROM batch_ids)::uuid[])
          AND j.status IN ('pending', 'scheduled')
        RETURNING j.id
    ),
    -- prev_status is recovered by joining the affected ids back to the
    -- snapshot `matching` already holds, rather than by carrying it out
    -- of the UPDATE. Both sides are batch-sized CTEs, so this join costs
    -- the batch and never touches `jobs`. A row that failed the EPQ
    -- re-check is absent from `cancelled` and so drops out here too.
    cancelled_prev AS (
        SELECT c.id, m.status AS prev_status
        FROM cancelled AS c
        JOIN matching AS m ON m.id = c.id
    )
    SELECT
        (SELECT count(*)::int FROM matching) AS matched_count,
        (SELECT last_id FROM batch_ids) AS last_id,
        (SELECT count(*)::int FROM cancelled_prev) AS cancelled_directly,
        (SELECT array_agg(id ORDER BY id) FROM cancelled_prev) AS cancelled_ids,
        (SELECT array_agg(prev_status ORDER BY id) FROM cancelled_prev) AS cancelled_prev_statuses
    """

    # Statement 2: cooperative cancel for running jobs with cancel_phase=0
    # Fresh snapshot — catches jobs dispatched between statements 1 and 2.
    cancel_running_sql = f"""
    -- MATERIALIZED is load-bearing: without it the planner may inline the
    -- LIMIT-ed matching CTE into the UPDATE as a nested loop and update
    -- more rows than the LIMIT admits.
    -- Same keyset window as the pending/scheduled arm, against the same
    -- defect with one twist: a row this arm handles STAYS 'running' (the
    -- worker owns the terminal write), so without `id > cursor` every
    -- pass re-walks the rows it already moved to cancel_phase = 1.
    WITH matching AS MATERIALIZED (
        SELECT id, locked_by_worker
        FROM "{schema}".jobs
        WHERE {conditions_str}
          AND status = 'running'
          AND cancel_phase = 0
          AND id > ${cursor_ph}::uuid
        ORDER BY id
        LIMIT ${limit_ph}
    ),
    -- Same restriction-clause shape as the pending/scheduled arm above,
    -- for the same reason and against the same defect: a FROM-join on
    -- `matching` lets the planner Seq Scan `jobs` and apply the
    -- status/cancel_phase predicates as a post-scan filter, so each batch
    -- re-walks the rows earlier batches already moved to cancel_phase=1.
    -- See that arm's comment for why `= ANY (<array>)` turns the id list
    -- into per-row index probes instead.
    batch_ids AS MATERIALIZED (
        -- Last element is the greatest id windowed; see the other arm for
        -- why the ordering is stated explicitly.
        SELECT array_agg(id ORDER BY id) AS ids,
               (array_agg(id ORDER BY id))[count(*)] AS last_id
        FROM matching
    ),
    cancel_requested AS (
        UPDATE "{schema}".jobs AS j
        SET cancel_requested_at = clock_timestamp(), cancel_phase = 1
        WHERE j.id = ANY ((SELECT ids FROM batch_ids)::uuid[])
          AND j.status = 'running'
          AND j.cancel_phase = 0
        RETURNING j.id
    ),
    -- locked_by_worker recovered from the batch-sized snapshot rather
    -- than carried out of the UPDATE, exactly as prev_status is above.
    cancel_requested_prev AS (
        SELECT c.id, m.locked_by_worker
        FROM cancel_requested AS c
        JOIN matching AS m ON m.id = c.id
    )
    SELECT
        (SELECT count(*)::int FROM matching) AS matched_count,
        (SELECT last_id FROM batch_ids) AS last_id,
        (SELECT count(*)::int FROM cancel_requested_prev) AS cancel_requested,
        (SELECT array_agg(id ORDER BY id) FROM cancel_requested_prev) AS cancel_requested_ids,
        (SELECT array_agg(locked_by_worker ORDER BY id) FROM cancel_requested_prev) AS cancel_requested_workers
    """

    event_batch_sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema)
    cr_detail = jsonb_param({"reason": reason} if reason is not None else {})

    cancelled_ids: list[UUID] = []
    cancel_requested_ids: list[UUID] = []
    notify_targets: list[NotifyTarget] = []

    async def _handle_ps_batch(conn: ConnLike, row: asyncpg.Record) -> None:
        count = int(row["cancelled_directly"])
        if count == 0:
            return
        batch_ids: list[UUID] = list(row["cancelled_ids"] or [])
        prev_statuses: dict[UUID, str] = dict(
            zip(batch_ids, list(row["cancelled_prev_statuses"] or []), strict=True)
        )
        # Per-row detail: from_state is each job's own pre-cancel status —
        # sharing one detail across the unnest would stamp a single
        # from_state on every event in a mixed pending/scheduled batch.
        await conn.execute(
            event_batch_sql,
            batch_ids,
            [
                jsonb_param({"from_state": prev_statuses[jid], "to_state": "cancelled"})
                for jid in batch_ids
            ],
            "state_change",
        )
        # The cancel_request detail is uniform across the batch; the same
        # two-column template keeps the write one statement per batch.
        await conn.execute(
            event_batch_sql,
            batch_ids,
            [cr_detail] * len(batch_ids),
            "cancel_request",
        )
        # Appended only now, after both event writes succeeded: a batch
        # that deadlocks mid-write must contribute no phantom ids.
        cancelled_ids.extend(batch_ids)

    async def _handle_running_batch(conn: ConnLike, row: asyncpg.Record) -> None:
        count = int(row["cancel_requested"])
        if count == 0:
            return
        batch_ids: list[UUID] = list(row["cancel_requested_ids"] or [])
        workers: list[UUID | None] = list(row["cancel_requested_workers"] or [])
        await conn.execute(
            event_batch_sql,
            batch_ids,
            [cr_detail] * len(batch_ids),
            "cancel_request",
        )
        # Same no-phantom rule as the pending/scheduled phase.
        cancel_requested_ids.extend(batch_ids)
        notify_targets.extend(
            NotifyTarget(job_id=jid, worker_id=wid)
            for jid, wid in zip(batch_ids, workers, strict=True)
            if wid is not None
        )

    # The two-arm drain as bounded fixpoint ROUNDS (#237). A single
    # pair of passes loses matching rows that a concurrent re-pend
    # moves BEHIND the pending arm's keyset cursor mid-drain: the row
    # was 'running' (or phase!=0) when the pending arm windowed past
    # its id, so no window ever held it, and the running arm, strictly
    # after the pending arm, matching only running+phase-0, cannot
    # see the re-pended row either. The call used to return normally
    # with such a row uncancelled, contradicting the "cancels EVERY
    # matching job" contract. Each subsequent round re-walks from the
    # bottom of the key space (`_UUID_MIN`), so a re-pended straggler
    # is matched by round N+1's pending arm whatever id it carries.
    #
    # Termination and cost: a round stops the loop when NEITHER arm
    # committed a single row (progress measured on the committed id
    # lists the handlers append to: windowed-but-EPQ-dropped rows are
    # deliberately not progress, they belong to the other arm), and
    # `_MAX_CANCEL_DRAIN_ROUNDS` bounds the loop outright, because a
    # live claim/fail/retry churn on matching rows re-feeds the match
    # set every round and an uncapped fixpoint would chase it forever.
    # Ids cannot repeat across rounds: arm 1's rows land terminal
    # 'cancelled' (EPQ re-check rejects them forever after) and arm 2's
    # rows leave the running arm's phase-0 predicate, so the result
    # totals stay exactly-once.
    for _round in range(_MAX_CANCEL_DRAIN_ROUNDS):
        _before = len(cancelled_ids) + len(cancel_requested_ids)
        await _drain_cancel_batches(
            pool, cancel_ps_sql, params, batch_size, statement_timeout_ms, _handle_ps_batch
        )
        await _drain_cancel_batches(
            pool,
            cancel_running_sql,
            params,
            batch_size,
            statement_timeout_ms,
            _handle_running_batch,
        )
        if len(cancelled_ids) + len(cancel_requested_ids) == _before:
            break

    result = BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=tuple(cancelled_ids),
        cancel_requested_ids=tuple(cancel_requested_ids),
    )
    return result, notify_targets
