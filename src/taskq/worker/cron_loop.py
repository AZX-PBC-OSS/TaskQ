"""Cron tick loop: firing due schedules with advisory lock and miss-handling.

Extracted from :mod:`taskq.worker.leader` per file-size ceiling.  The
leader's ``_cron_loop`` method delegates to :func:`tick_cron` each second;
one tick selects a bounded batch of due schedules, plans every fire in
memory (:func:`resolve_payload` resolves each schedule's payload) and
writes the enqueues plus the schedule advances as a handful of batched
statements inside the caller's transaction.
"""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

import asyncpg
import structlog
from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, StatusCode

from taskq._ids import new_job_id
from taskq._json import sanitize_nul_str
from taskq.backend._protocol import Backend, DstStrategy, EnqueueArgs, IdentityKey, parse_retry_kind
from taskq.backend._records import parse_rowcount
from taskq.backend._sweeps import (
    _validate_positive,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical pre-SQL bound validation, shared by every bounded batch path — a degenerate cap is a caller configuration bug and belongs at the boundary, not inside a tick.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    schema_lock_name,
)
from taskq.cron import (
    DST_STRATEGIES,
    compute_next_fire_after,
    repeated_range_bounds,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical bounds of a repeated wall range; redefining them here would let the tick's delivery hop and compute_next_fire_after drift on what "the repeated range" is.
)
from taskq.cron import (
    resolve_payload as resolve_cron_payload,
)
from taskq.obs import (
    get_logger,
    record_backpressure_error,
    record_cron_failure,
    record_cron_lock_contention,
    record_published_message,
    safe_start_span,
    update_disabled_schedules_count,
)
from taskq.obs._redact_exc import safe_exception_message
from taskq.settings import WorkerSettings

log: structlog.stdlib.BoundLogger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ActorFirePolicy:
    """The singleton / max_pending flags one actor declares, carried from
    the worker bootstrap's ``actor_registry`` into the cron tick.

    The client enqueue path stamps both onto :class:`EnqueueArgs` at build
    time (``client/_args.py``); the tick builds its own args directly, so
    it needs the flags here to reach parity — a cron fire for a singleton
    actor must carry ``metadata["singleton"]`` (the ``jobs_singleton_uniq``
    partial index keys on exactly that flag) and respect ``max_pending``
    like any client enqueue.
    """

    singleton: bool = False
    max_pending: int | None = None


@dataclass(frozen=True, slots=True)
class _ActorConfig:
    queue: str
    max_attempts: int
    retry_kind: str


@dataclass(frozen=True, slots=True)
class _FireSuccess:
    """One planned fire: its enqueue args (one, or two on the DST ``allof``
    overlap), the recomputed ``next_fire_at`` and the fields the post-write
    logs and metrics need.  Planning touches no database."""

    schedule_id: UUID
    row: asyncpg.Record
    enqueue_args: list[EnqueueArgs]
    next_fire_at: datetime
    actor: str
    queue: str
    prev_consecutive: int


@dataclass(frozen=True, slots=True)
class _FireFailure:
    """One failed fire, whether it failed while planning or while writing.

    ``error_text`` is the exception message with NUL codepoints replaced
    by the visible ``\\x00`` escape, with a class-name fallback because
    ``str()`` of a bare ``TimeoutError`` (exactly what a hung payload
    factory produces via ``resolve_payload``'s ``wait_for``) is the empty
    string, and an empty error column records a failure with no reason
    at all.  Sanitized rather than rejected because the text is derived
    from an uncontrolled exception and feeds the batched failures
    UPDATE's ``unnest($2::text[])``: a raw NUL aborts that whole
    statement (SQLSTATE 22021), losing the ``consecutive_failures`` and
    auto-disable bookkeeping for every schedule in the tick — the same
    rationale as ``worker/_handlers.py``'s terminal-write sanitization.
    """

    schedule_id: UUID
    row: asyncpg.Record
    error_text: str
    consecutive: int
    auto_disable: bool


@dataclass(frozen=True, slots=True)
class _SuppressedFire:
    """One planned fire dropped before the enqueue by a policy preflight,
    with its computed ``next_fire_at`` (the suppression UPDATE advances the
    schedule with it) and the fields the post-write log events need."""

    schedule_id: UUID
    actor: str
    next_fire_at: datetime
    reason: Literal["singleton_collision", "max_pending"]
    blocking_job_id: UUID | None
    current_count: int | None
    max_pending: int | None


async def resolve_payload(row: asyncpg.Record) -> dict[str, object]:
    """Resolve payload from ``payload_factory`` or ``static_payload``.

    Delegates to :func:`~taskq.cron.resolve_payload`.  All exceptions
    (including ``TypeError`` from a factory returning an unexpected type)
    propagate to the caller so the per-schedule failure handling inside
    :func:`tick_cron` increments ``consecutive_failures`` and triggers
    auto-disable.
    """
    pf: str | None = row["payload_factory"]
    return await resolve_cron_payload(pf, row["metadata"])


def _record_fire_failure(
    span: Span,
    row: asyncpg.Record,
    exc: Exception,
    settings: WorkerSettings,
) -> _FireFailure:
    """Mirror of the pre-batching per-schedule except-branch: mark the span,
    bump the failure count, decide auto-disable.

    The span status description and the ``cron.auto_disabled`` event carry
    :func:`safe_exception_message` — span text is exported to third-party
    telemetry backends, and ``str()`` of a constraint violation quotes row
    values.  The returned ``error_text`` is NUL-sanitized (see
    :class:`_FireFailure`) because it is bound as ``text`` by the batched
    failures UPDATE.

    Both fall back to the exception's class name when the message is
    empty: ``str(TimeoutError())`` is ``''`` — exactly what
    ``resolve_payload``'s ``wait_for`` raises for a payload factory that
    never returns — and without the fallback a schedule can be failing
    (and auto-disabled) with an empty reason in the column, the log event
    and the exported span status.
    """
    class_name = type(exc).__name__
    span_text = safe_exception_message(exc) or class_name
    span.set_status(StatusCode.ERROR, span_text)
    consecutive: int = (row["consecutive_failures"] or 0) + 1
    auto_disable = consecutive >= settings.cron_auto_disable_threshold
    if auto_disable:
        span.add_event(
            "cron.auto_disabled",
            {
                "schedule_name": row["actor"],
                "last_error": span_text,
                "failure_count": consecutive,
            },
        )
    return _FireFailure(
        schedule_id=row["id"],
        row=row,
        error_text=sanitize_nul_str(str(exc) or class_name),
        consecutive=consecutive,
        auto_disable=auto_disable,
    )


async def _suppress_policy_collisions(
    conn: asyncpg.Connection,
    schema: str,
    successes: list[_FireSuccess],
    actor_policies: Mapping[str, ActorFirePolicy],
) -> tuple[list[_FireSuccess], list[_SuppressedFire]]:
    """Split the planned fires into those the batch may enqueue and those a
    singleton / max_pending preflight suppresses.

    The two queries mirror the enqueue path's own predicates
    (``_sql_templates.py``'s ``singleton_preflight`` and
    ``enqueue_max_pending_count``) so a cron fire and a client enqueue
    answer the same question against the same rows.  Both run BEFORE the
    batched enqueue because the enqueue path classifies these conditions
    as typed *errors*: a singleton fire reaching the batched INSERT while
    a blocker is active violates ``jobs_singleton_uniq`` and aborts the
    whole statement, landing every planned fire in the tick's generic
    failure path — striking schedules whose only defect is a busy actor
    (the auto-disable trap).

    The second singleton gate is in memory, not a query: two due
    schedules for one singleton actor both pass the blocker preflight
    (neither of this tick's jobs exists yet), and enqueueing both would
    violate the same index from inside the batch.  The earlier slot wins;
    the later one is suppressed against the winner's job id.

    The ``max_pending`` gate carries the same in-memory second half: a
    kept plan's enqueue args are pending or scheduled the moment the
    batch commits, so every later plan for the same capped actor is
    evaluated against the DB count PLUS the tick's own kept plans — the
    count a second, sequential client enqueue would see.  The DB count
    alone (still zero mid-tick) admits every plan in the batch and lands
    cap+N jobs from one tick.  A plan whose own args exceed the remaining
    capacity (the DST ``allof`` pair: one immediate occurrence plus one
    future-scheduled) is trimmed to what fits, with the dropped
    future-dated occurrence deferred to its own instant via the plan's
    ``next_fire_at`` — delivered later at capacity, never silently
    dropped and never past the cap.
    """
    singleton_actors: list[str] = sorted(
        {
            plan.actor
            for plan in successes
            if (policy := actor_policies.get(plan.actor)) is not None and policy.singleton
        }
    )
    blocking: dict[str, UUID] = {}
    if singleton_actors:
        # ``min(id::text)`` — Postgres has no ``min(uuid)`` aggregate; the
        # text form orders identically for the UUIDv7 ids TaskQ mints
        # (timestamp in the leading bits), so the blocker picked is the
        # oldest active singleton job, same choice the enqueue path's
        # ``ORDER BY created_at DESC LIMIT 1`` makes per actor.
        blocker_rows: list[asyncpg.Record] = await conn.fetch(
            f"SELECT actor, count(*)::int AS active_count, "
            f"min(id::text)::uuid AS blocking_job_id "
            f'FROM "{schema}".jobs '
            f"WHERE actor = ANY($1::text[]) "
            f"AND status IN ('pending', 'scheduled', 'running') "
            f"AND metadata @> '{{\"singleton\": true}}'::jsonb "
            f"GROUP BY actor",
            singleton_actors,
        )
        blocking = {str(rec["actor"]): rec["blocking_job_id"] for rec in blocker_rows}

    capped: dict[str, int] = {}
    for plan in successes:
        policy = actor_policies.get(plan.actor)
        if policy is not None and policy.max_pending is not None and plan.actor not in blocking:
            capped[plan.actor] = policy.max_pending
    pending_counts: dict[str, int] = {}
    if capped:
        count_rows: list[asyncpg.Record] = await conn.fetch(
            f"SELECT actor, count(*)::int AS pending_count "
            f'FROM "{schema}".jobs '
            f"WHERE actor = ANY($1::text[]) "
            f"AND status IN ('pending', 'scheduled') "
            f"GROUP BY actor",
            sorted(capped),
        )
        pending_counts = {str(rec["actor"]): int(rec["pending_count"]) for rec in count_rows}

    kept: list[_FireSuccess] = []
    suppressed: list[_SuppressedFire] = []
    fired_singleton_job: dict[str, UUID] = {}
    # Pending/scheduled occupancy this tick itself is about to create for a
    # capped actor — the rows do not exist at preflight time, but a second
    # sequential client enqueue would see them (the first has committed).
    intra_pending: dict[str, int] = {}
    for plan in successes:
        policy = actor_policies.get(plan.actor)
        if plan.actor in blocking:
            suppressed.append(_singleton_suppressed(plan, blocking[plan.actor]))
            continue
        if policy is not None and policy.singleton and plan.actor in fired_singleton_job:
            suppressed.append(_singleton_suppressed(plan, fired_singleton_job[plan.actor]))
            continue
        if plan.actor in capped:
            occupied = pending_counts.get(plan.actor, 0) + intra_pending.get(plan.actor, 0)
            cap = capped[plan.actor]
            if occupied >= cap:
                suppressed.append(_max_pending_suppressed(plan, occupied, cap))
                continue
            capacity = cap - occupied
            if len(plan.enqueue_args) > capacity:
                deferred_at = plan.enqueue_args[capacity].scheduled_at
                if deferred_at is None:
                    # Unreachable today: only the DST ``allof`` pair produces
                    # more than one args and its extras are always
                    # future-scheduled. An extra occurrence due NOW cannot be
                    # deferred without exceeding the cap, so the plan
                    # suppresses whole rather than enqueue past the limit.
                    suppressed.append(_max_pending_suppressed(plan, occupied, cap))
                    continue
                log.info(
                    "max-pending-dst-overlap-deferred",
                    kind="cron_fire",
                    actor=plan.actor,
                    schedule_id=str(plan.schedule_id),
                    deferred_at=deferred_at.isoformat(),
                )
                plan = replace(
                    plan,
                    enqueue_args=plan.enqueue_args[:capacity],
                    next_fire_at=deferred_at,
                )
        kept.append(plan)
        if policy is not None and policy.singleton:
            fired_singleton_job[plan.actor] = plan.enqueue_args[0].id
        if plan.actor in capped:
            intra_pending[plan.actor] = intra_pending.get(plan.actor, 0) + len(plan.enqueue_args)
    return kept, suppressed


def _singleton_suppressed(plan: _FireSuccess, blocking_job_id: UUID) -> _SuppressedFire:
    return _SuppressedFire(
        schedule_id=plan.schedule_id,
        actor=plan.actor,
        next_fire_at=plan.next_fire_at,
        reason="singleton_collision",
        blocking_job_id=blocking_job_id,
        current_count=None,
        max_pending=None,
    )


def _max_pending_suppressed(plan: _FireSuccess, current_count: int, cap: int) -> _SuppressedFire:
    return _SuppressedFire(
        schedule_id=plan.schedule_id,
        actor=plan.actor,
        next_fire_at=plan.next_fire_at,
        reason="max_pending",
        blocking_job_id=None,
        current_count=current_count,
        max_pending=cap,
    )


async def tick_cron(
    conn: asyncpg.Connection,
    settings: WorkerSettings,
    backend: Backend,
    schema: str,
    worker_id: UUID,
    *,
    limit: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    actor_policies: Mapping[str, ActorFirePolicy] | None = None,
) -> int:
    """Fire due cron schedules and return how many fired.  Holds
    ``pg_try_advisory_xact_lock`` on the schema-qualified cron lock name to
    prevent double-fire during leader handover.

    *conn* MUST already be in an open transaction — the advisory lock is
    transaction-scoped and releases on COMMIT/ROLLBACK.

    One tick is a bounded batch: at most *limit* due schedules (ordered by
    ``next_fire_at``) are selected, planned in memory, and written with one
    batched enqueue plus one UPDATE per outcome branch.  A catch-up burst
    larger than *limit* drains across successive one-second ticks instead of
    one oversized transaction the leader's command deadline would roll back
    in full; the remainder stays due and untouched until its tick.

    The catch-up cutoff and the beyond-window recompute seed are read from
    the PG server clock inside this transaction: the due-check
    (``next_fire_at <= now()``) is server-side, so every croniter seed must
    come from the same domain.  Seeding from the leader's Python clock
    shifts every recomputed fire by the app↔DB skew and can recompute
    ``next_fire_at`` into the server's past (a fire loop).

    *actor_policies* carries the worker's ``actor_registry`` singleton /
    ``max_pending`` flags (``None`` — the default, and every pre-plumbing
    caller — stamps nothing and enforces nothing, exactly the previous
    behavior).  With flags present, planned fires carry the same stamps a
    client enqueue would get, and a fire blocked by an active singleton
    job or a full pending cap is SUPPRESSED: dropped from the batch,
    ``next_fire_at`` advanced, neither a fire nor a failure (suppressed
    slots are absent from the return count).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("limit", limit)

    lock_name = schema_lock_name("cron", schema)
    lock_acquired: bool = await conn.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
        lock_name,
    )
    if not lock_acquired:
        # Why observable: this branch is benign for the sub-second leader
        # handover it exists to cover, but it is indistinguishable from total
        # cron failure. The lock is transaction-scoped and releases on
        # COMMIT/ROLLBACK -- which never happens if the holding session was
        # partitioned without a FIN (a VNet blip, as opposed to a process kill,
        # which sends a FIN and releases cleanly). Every subsequent tick then
        # returns here, cron stops firing fleet-wide, and neither
        # `taskq.cron.disabled_schedules` nor `consecutive_failures` moves,
        # because the tick never reaches a fire. Silence made that state
        # invisible. The stall is bounded by the server reaping the dead
        # backend (tcp_keepalives_*), not unbounded -- minutes, not forever.
        record_cron_lock_contention(str(worker_id))
        log.debug(
            "cron-tick-lock-contended",
            kind="cron_tick_lock_contended",
            worker_id=str(worker_id),
            lock=lock_name,
        )
        return 0

    server_now: datetime = await conn.fetchval("SELECT clock_timestamp()")
    # statement_timestamp() (STABLE) — not clock_timestamp() (VOLATILE) —
    # for the due bound: a volatile comparison cannot be a btree index
    # condition, so cron_schedules_next_fire_idx (partial on enabled,
    # keyed on next_fire_at) would degrade from an Index Cond that stops
    # at the boundary to a post-scan filter walk of every enabled entry,
    # per tick, every second. Measured at 10k enabled schedules (PG 18,
    # EXPLAIN ANALYZE): clock_timestamp() walks all 10,000 entries
    # (1.04 ms); statement_timestamp() is an Index Cond scan (2 buffers,
    # 0.005 ms). statement_timestamp() is the wall clock at this
    # statement's start — the same domain as server_now above (read
    # moments earlier in this same transaction), differing only by the
    # statement's own execution time.
    rows: list[asyncpg.Record] = await conn.fetch(
        f"SELECT id, actor, cron_expr, timezone, dst_strategy, payload_factory, "
        f"metadata, last_fired_at, consecutive_failures, next_fire_at, identity_key "
        f'FROM "{schema}".cron_schedules '
        f"WHERE enabled = true AND next_fire_at <= statement_timestamp() "
        f"ORDER BY next_fire_at "
        f"LIMIT $1",
        limit,
    )
    if not rows:
        return 0

    # One round trip for every distinct actor in the batch.  A missing actor
    # is not an error here — the planning loop turns each affected schedule
    # into a per-schedule failure with the same message the per-row lookup
    # raised before batching.
    actors: list[str] = sorted({str(row["actor"]) for row in rows})
    ac_rows: list[asyncpg.Record] = await conn.fetch(
        f'SELECT actor, queue, max_attempts, retry_kind FROM "{schema}".actor_config '
        f"WHERE actor = ANY($1::text[])",
        actors,
    )
    actor_configs: dict[str, _ActorConfig] = {
        str(ac_row["actor"]): _ActorConfig(
            queue=ac_row["queue"],
            max_attempts=ac_row["max_attempts"],
            retry_kind=ac_row["retry_kind"],
        )
        for ac_row in ac_rows
    }

    successes: list[_FireSuccess] = []
    failures: list[_FireFailure] = []

    for row in rows:
        current_span = trace.get_current_span()
        current_ctx = current_span.get_span_context()
        links = [trace.Link(current_ctx)] if current_ctx.is_valid else None

        with safe_start_span(
            "cron fire",
            kind=SpanKind.PRODUCER,
            attributes={"cron_schedule_name": row["actor"], "taskq.worker_id": str(worker_id)},
            links=links,
            new_root=True,
        ) as span:
            try:
                successes.append(
                    await _plan_fire(row, server_now, settings, actor_configs, actor_policies)
                )
            except Exception as exc:
                failures.append(_record_fire_failure(span, row, exc, settings))

    # Policy preflight (singleton / max_pending parity with the client
    # enqueue path) — before the batched enqueue, and only when a planned
    # success carries a flag, so ticks without flagged actors spend zero
    # extra statements.  Suppressed plans leave the enqueue list here and
    # never reach the failure path below.
    suppressed: list[_SuppressedFire] = []
    # Overlap-twin delivery is settled BEFORE the policy preflight: a
    # suppressed plan leaves the success list here, and its next_fire
    # flows into the suppression UPDATE below — whether the fold-1 twin
    # was already delivered by an earlier tick is independent of any
    # policy, and both UPDATE paths must carry the same advance.
    successes = await _skip_already_delivered_overlap_twins(conn, schema, successes, actor_policies)
    if actor_policies and successes:
        successes, suppressed = await _suppress_policy_collisions(
            conn, schema, successes, actor_policies
        )

    if successes:
        try:
            await backend.enqueue_batch(
                [args for plan in successes for args in plan.enqueue_args],
                connection=conn,
                # Pre-admitted by _suppress_policy_collisions above, which
                # trims every plan to the remaining capacity: re-checking
                # here would turn a concurrent client enqueue borrowing the
                # last slot into a whole-tick abort, striking schedules
                # whose only defect is a busy actor (the auto-disable trap
                # the preflight exists to prevent). Accepted converse cost:
                # a client commit landing between the preflight SELECT and
                # this INSERT overshoots the cap by that commit, bounded by
                # one tick's kept plans — liveness over strictness, stated.
                enforce_max_pending=False,
            )
        except Exception as exc:
            # One enqueue statement covers every planned fire, so its failure
            # fails them all at once.  Convert each planned success into a
            # per-schedule failure with the same span and metric handling the
            # planning loop uses, so the auto-disable telemetry survives the
            # move off the per-row enqueue path.  On a real connection the
            # failed INSERT has aborted the caller's transaction; the failure
            # UPDATE below then surfaces that error and the caller's rollback
            # discards it — the same outcome the per-schedule error branch
            # produced before batching.
            unsent, successes = successes, []
            for plan in unsent:
                current_span = trace.get_current_span()
                current_ctx = current_span.get_span_context()
                links = [trace.Link(current_ctx)] if current_ctx.is_valid else None
                with safe_start_span(
                    "cron fire",
                    kind=SpanKind.PRODUCER,
                    attributes={
                        "cron_schedule_name": plan.actor,
                        "taskq.worker_id": str(worker_id),
                    },
                    links=links,
                    new_root=True,
                ) as span:
                    failures.append(_record_fire_failure(span, plan.row, exc, settings))

    if successes:
        # One statement advances every fired schedule: server-clock
        # last_fired_at, error cleared, failure count reset, next_fire_at
        # from the plan.
        await conn.execute(
            f'UPDATE "{schema}".cron_schedules s '
            f"SET last_fired_at = clock_timestamp(), last_fire_error = NULL, "
            f"consecutive_failures = 0, next_fire_at = f.next_fire "
            f"FROM unnest($1::uuid[], $2::timestamptz[]) AS f(id, next_fire) "
            f"WHERE s.id = f.id",
            [plan.schedule_id for plan in successes],
            [plan.next_fire_at for plan in successes],
        )

    if suppressed:
        # next_fire_at ONLY: suppression says nothing about the actor's
        # health, so no last_fired_at stamp (nothing fired), no
        # last_fire_error write, no consecutive_failures increment and no
        # reset either — a suppressed slot must neither punish nor amnesty.
        # Advancing next_fire_at alone keeps sequential catch-up moving and
        # prevents a hot re-fire loop against the active blocker.
        await conn.execute(
            f'UPDATE "{schema}".cron_schedules s '
            f"SET next_fire_at = f.next_fire "
            f"FROM unnest($1::uuid[], $2::timestamptz[]) AS f(id, next_fire) "
            f"WHERE s.id = f.id",
            [entry.schedule_id for entry in suppressed],
            [entry.next_fire_at for entry in suppressed],
        )

    if failures:
        # One statement for both failure flavours.  The CASE, not
        # ``enabled = NOT f.disable``, is load-bearing: a plain NOT would
        # re-enable a schedule someone re-enabled between read and write.
        # ``AND s.enabled = true`` keeps the pre-batching guard, so a
        # schedule disabled by anyone else mid-tick is left alone and shows
        # up as the rowcount shortfall below.
        tag: str = await conn.execute(
            f'UPDATE "{schema}".cron_schedules s '
            f"SET last_fire_error = f.err, consecutive_failures = f.consecutive, "
            f"enabled = CASE WHEN f.disable THEN false ELSE s.enabled END "
            f"FROM unnest($1::uuid[], $2::text[], $3::int[], $4::bool[]) "
            f"AS f(id, err, consecutive, disable) "
            f"WHERE s.id = f.id AND s.enabled = true",
            [failure.schedule_id for failure in failures],
            [failure.error_text for failure in failures],
            [failure.consecutive for failure in failures],
            [failure.auto_disable for failure in failures],
        )
        updated = parse_rowcount(tag)
        if updated < len(failures):
            # The per-row ``UPDATE 0`` race of the pre-batching code,
            # preserved at batch grain: a schedule that was re-enabled and
            # disabled again, or disabled by an operator mid-tick, is not
            # touched by the statement above.
            log.warning(
                "cron error UPDATE skipped; schedule no longer enabled",
                kind="cron_fire",
                worker_id=str(worker_id),
                skipped=len(failures) - updated,
            )

        if any(failure.auto_disable for failure in failures):
            disabled_count: int = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema}".cron_schedules WHERE enabled = false'
            )
            update_disabled_schedules_count(disabled_count)

    for plan in successes:
        if plan.prev_consecutive > 0:
            record_cron_failure(str(plan.schedule_id), -plan.prev_consecutive)
        log.info(
            "cron fired",
            kind="cron_fire",
            actor=plan.actor,
            worker_id=str(worker_id),
            schedule_id=str(plan.schedule_id),
            next_fire_at=plan.next_fire_at.isoformat(),
        )
        record_published_message(plan.actor, plan.queue)

    for failure in failures:
        log.error(
            "cron schedule auto-disabled" if failure.auto_disable else "cron fire failed",
            kind="cron_fire",
            actor=failure.row["actor"],
            worker_id=str(worker_id),
            schedule_id=str(failure.schedule_id),
            consecutive_failures=failure.consecutive,
            error=failure.error_text,
        )
        record_cron_failure(str(failure.schedule_id), 1)

    for entry in suppressed:
        if entry.reason == "singleton_collision":
            # Mirrors the enqueue path's own event shape (log only — the
            # enqueue path does not count singleton collisions), with the
            # cron attribution fields.
            log.info(
                "singleton-collision",
                actor=entry.actor,
                blocking_job_id=(
                    str(entry.blocking_job_id) if entry.blocking_job_id is not None else None
                ),
                detection_path="cron_tick_preflight",
                schedule_id=str(entry.schedule_id),
                worker_id=str(worker_id),
            )
        else:
            log.warning(
                "max-pending-exceeded",
                actor=entry.actor,
                current_count=entry.current_count,
                max_pending=entry.max_pending,
                schedule_id=str(entry.schedule_id),
                worker_id=str(worker_id),
            )
            record_backpressure_error(entry.actor, kind="max_pending")

    return len(successes)


async def _skip_already_delivered_overlap_twins(
    conn: asyncpg.Connection,
    schema: str,
    successes: list[_FireSuccess],
    actor_policies: Mapping[str, ActorFirePolicy] | None,
) -> list[_FireSuccess]:
    """Advance past repeated-range occurrences an earlier tick already
    delivered, when the plan's next fire lands inside a repeated range.

    Under ``allof`` a fire's computed next can land inside the repeated
    range — the fired slot's own twin (a single-match range), or the
    fold-1 pass's first match (a multi-match range, once the fold-0
    pass is spent).  But the twin chain may already have delivered
    those instants: a tick that fires a fold-0 slot pre-schedules the
    NEXT slot's fold-1 occurrence, so by the time the last fold-0 slot
    fires, every fold-1 slot can already hold a queued job — firing
    the schedule into that pass would double-deliver it.  The
    distinguishing fact is a query away and only on this rare shape
    (twice a year per schedule per timezone): the schedule's OWN queued
    jobs inside the range tell exactly which instants are already in
    flight — scoped by the ``cron_schedule_id`` metadata stamp, because
    two schedules on one actor are independent (each owes its own
    delivery of every occurrence) and a neighbour's twin chain is not
    this schedule's coverage — and the plan advances past the delivered
    prefix to the first instant nothing holds: the uncovered remainder
    is then delivered by the schedule's own later ticks, each exactly
    once.  ``identity_key`` cannot serve as the scope: it defaults to
    NULL and is a user-facing dedup handle shared with on-demand jobs,
    which are not the schedule's delivery either.

    Singleton-flagged actors are excluded: their delivery is sequential
    by design (nothing is ever pre-scheduled for them), and the singleton
    preflight on each occurrence's own tick already suppresses a fire
    against any active blocker — including one at that instant — while
    leaving the slot retryable, which a permanent skip here would not.
    """
    adjusted: dict[UUID, _FireSuccess] = {}
    queries: list[tuple[_FireSuccess, datetime]] = []
    for plan in successes:
        policy = (actor_policies or {}).get(plan.actor)
        if policy is not None and policy.singleton:
            continue
        if plan.row["dst_strategy"] != "allof":
            continue
        tz = ZoneInfo(plan.row["timezone"])
        bounds = repeated_range_bounds(plan.next_fire_at.astimezone(tz), tz)
        if bounds is None:
            continue
        range_end_utc = bounds[1].replace(tzinfo=tz).astimezone(UTC)
        queries.append((plan, range_end_utc))
    if not queries:
        return successes

    rows = await conn.fetch(
        f"SELECT a.idx AS idx, j.scheduled_at AS scheduled_at "  # Why: schema is a test-fixture identifier validated at the tick's entry; the actors, schedule ids and instants are $-bound arrays.
        f"FROM unnest($1::int[], $2::text[], $3::timestamptz[], $4::timestamptz[], $5::text[]) "
        f"AS a(idx, actor, from_ts, to_ts, schedule_id) "
        f'JOIN "{schema}".jobs j ON j.actor = a.actor '
        f"AND j.metadata->>'cron_schedule_id' = a.schedule_id "
        f"AND j.scheduled_at >= a.from_ts "
        f"AND j.scheduled_at < a.to_ts AND j.status IN ('pending', 'scheduled')",
        [idx for idx, _ in enumerate(queries)],
        [plan.actor for plan, _ in queries],
        [plan.next_fire_at for plan, _ in queries],
        [range_end for _, range_end in queries],
        [str(plan.row["id"]) for plan, _ in queries],
    )
    # Instants, not raw datetimes: the row comes back from the database in
    # UTC and the plans' next fires are schedule-timezone-local, and a
    # cross-zone aware equality is not reliable in this runtime —
    # normalize both sides.
    delivered: dict[int, set[datetime]] = {}
    for row in rows:
        delivered.setdefault(row["idx"], set()).add(row["scheduled_at"].astimezone(UTC))
    for idx, (plan, range_end_utc) in enumerate(queries):
        covered = delivered.get(idx, set())
        current = plan.next_fire_at
        while current.astimezone(UTC) < range_end_utc and current.astimezone(UTC) in covered:
            nxt = compute_next_fire_after(
                plan.row["cron_expr"], plan.row["timezone"], current, dst_strategy="allof"
            )[0]
            if nxt.astimezone(UTC) <= current.astimezone(UTC):
                # Monotonicity belt: the computation is pinned to answer
                # strictly after its seed, so this cannot fire — but a
                # regression there must not turn this hop into a loop.
                break
            current = nxt
        if current.astimezone(UTC) != plan.next_fire_at.astimezone(UTC):
            adjusted[plan.schedule_id] = replace(plan, next_fire_at=current)
            log.info(
                "cron dst overlap occurrence already delivered",
                kind="cron_fire",
                actor=plan.actor,
                schedule_id=str(plan.schedule_id),
                advanced_from=plan.next_fire_at.isoformat(),
                advanced_to=current.isoformat(),
            )
    if not adjusted:
        return successes
    return [adjusted.get(plan.schedule_id, plan) for plan in successes]


async def _plan_fire(
    row: asyncpg.Record,
    server_now: datetime,
    settings: WorkerSettings,
    actor_configs: dict[str, _ActorConfig],
    actor_policies: Mapping[str, ActorFirePolicy] | None = None,
) -> _FireSuccess:
    """Plan one due schedule's fire: resolve the fire time (miss handling),
    payload and enqueue args, and the next ``next_fire_at`` — all in memory,
    inside the caller's per-schedule span.

    *server_now* is the PG server clock read inside the caller's tick
    transaction (``clock_timestamp()``) — the single domain for the
    catch-up cutoff and the beyond-window recompute, matching the
    server-side due-check that selected this row.

    *actor_policies* stamps the planned args with the actor's singleton /
    ``max_pending`` flags exactly the way the client enqueue path stamps
    its own (``client/_args.py``); ``None`` stamps nothing.
    """
    catch_up_cutoff = server_now - settings.cron_catch_up_window
    fire_at: datetime = row["next_fire_at"]
    # Subscript, not .get(default): the tick's SELECT is contracted to provide
    # this column, and a defaulting read is exactly what hid its absence —
    # every schedule silently fired with 'skip' semantics, whatever it stored,
    # and the DST-overlap branch below was unreachable.
    dst_strategy_raw: str = row["dst_strategy"]
    dst_strategy: DstStrategy = dst_strategy_raw if dst_strategy_raw in DST_STRATEGIES else "skip"
    if fire_at < catch_up_cutoff:
        fire_at = compute_next_fire_after(
            row["cron_expr"], row["timezone"], server_now, dst_strategy=dst_strategy
        )[0]
        log.warning(
            "cron missed slots skipped",
            kind="cron_fire",
            actor=row["actor"],
            schedule_id=str(row["id"]),
        )

    actor: str = row["actor"]
    ac = actor_configs.get(actor)
    if ac is None:
        raise LookupError(f"Actor '{actor}' not found in actor_config")

    policy: ActorFirePolicy | None = (
        actor_policies.get(actor) if actor_policies is not None else None
    )
    # Parity stamps: the client path sets metadata["singleton"] from the
    # ActorRef and passes ref.max_pending; without them the
    # jobs_singleton_uniq partial index (keyed on the flag) never covers a
    # cron fire and no cap applies.  "cron_schedule_id" is provenance: the
    # twin-coverage walk scopes delivered instants to the schedule that
    # enqueued them — identity_key cannot serve that scope (it defaults to
    # NULL and is a user-facing dedup handle shared with on-demand jobs).
    stamped_metadata: dict[str, object] = {"cron_schedule_id": str(row["id"])}
    if policy is not None and policy.singleton:
        stamped_metadata["singleton"] = True
    stamped_max_pending: int | None = policy.max_pending if policy is not None else None

    identity_key_raw: object = row["identity_key"]
    schedule_identity_key: IdentityKey | None = (
        IdentityKey(str(identity_key_raw)) if identity_key_raw is not None else None
    )

    payload = await resolve_payload(row)

    enqueue_args = [
        EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue=ac.queue,
            payload=payload,
            max_attempts=ac.max_attempts,
            retry_kind=parse_retry_kind(ac.retry_kind),
            # None = immediate: the enqueue SQL stamps the server clock
            # (COALESCE($n, now())) and decides status in the same
            # statement. Passing a Python-clock stamp here would shift
            # the job's scheduled_at by the app↔DB skew — a leader
            # skewed ahead lands every cron fire 'scheduled' and
            # dispatch-ineligible for the skew duration.
            scheduled_at=None,
            payload_schema_ver=1,
            identity_key=schedule_identity_key,
            metadata=dict(stamped_metadata),
            max_pending=stamped_max_pending,
        )
    ]

    next_fires = compute_next_fire_after(
        row["cron_expr"], row["timezone"], fire_at, dst_strategy=dst_strategy
    )
    next_fire = next_fires[0]

    if len(next_fires) > 1 and dst_strategy == "allof":
        if policy is not None and policy.singleton:
            # Why no second args: both occurrences of the repeated hour
            # would sit active at once (the first pending, the second
            # scheduled) and both carry the singleton flag, so
            # jobs_singleton_uniq — the very guarantee the flag turns on —
            # aborts the whole batched INSERT from inside the batch.
            # Singleton semantics are served sequentially instead:
            # next_fire_at lands on the first occurrence, and the repeated
            # hour fires on its own later tick once the first goes
            # terminal.
            log.info(
                "singleton-dst-overlap-second-deferred",
                kind="cron_fire",
                actor=actor,
                schedule_id=str(row["id"]),
                deferred_at=next_fires[1].isoformat(),
            )
        else:
            enqueue_args.append(
                EnqueueArgs(
                    id=new_job_id(),
                    actor=actor,
                    queue=ac.queue,
                    payload=payload,
                    max_attempts=ac.max_attempts,
                    retry_kind=parse_retry_kind(ac.retry_kind),
                    scheduled_at=next_fires[1],
                    payload_schema_ver=1,
                    identity_key=schedule_identity_key,
                    metadata=dict(stamped_metadata),
                    max_pending=stamped_max_pending,
                )
            )
            log.info(
                "cron dst overlap second fire",
                kind="cron_fire",
                actor=actor,
                schedule_id=str(row["id"]),
            )
    # No singleton twin-override here: the computation itself owns the
    # fold handoff.  A fired fold-0 slot whose fold-0 pass is spent
    # makes ``compute_next_fire_after`` answer the fold-1 pass's first
    # match — for a single-match range that IS the fired slot's own
    # twin (the sequential delivery the deferral promised), and for a
    # multi-match range it is the pass's first unspent occurrence.  An
    # override on the fired slot's own twin alone would send a minutely
    # singleton to the range's LAST occurrence and silently lose every
    # fold-1 occurrence in between.

    return _FireSuccess(
        schedule_id=row["id"],
        row=row,
        enqueue_args=enqueue_args,
        next_fire_at=next_fire,
        actor=actor,
        queue=ac.queue,
        prev_consecutive=row["consecutive_failures"] or 0,
    )
