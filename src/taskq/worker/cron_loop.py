"""Cron tick loop: firing due schedules with advisory lock and miss-handling.

Extracted from :mod:`taskq.worker.leader` per file-size ceiling.  The
leader's ``_cron_loop`` method delegates to :func:`tick_cron` each second;
one tick selects a bounded batch of due schedules, plans every fire in
memory (:func:`resolve_payload` resolves each schedule's payload) and
writes the enqueues plus the schedule advances as a handful of batched
statements inside the caller's transaction.
"""

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Final, Literal
from uuid import UUID
from zoneinfo import ZoneInfo

import asyncpg
import structlog
from asyncpg.exceptions import UniqueViolationError
from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, StatusCode

from taskq._ids import new_job_id, new_uuid
from taskq._json import loads, sanitize_nul_str
from taskq.backend._protocol import Backend, DstStrategy, EnqueueArgs, IdentityKey, parse_retry_kind
from taskq.backend._records import parse_rowcount
from taskq.backend._sweeps import (
    _validate_positive,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical pre-SQL bound validation, shared by every bounded batch path — a degenerate cap is a caller configuration bug and belongs at the boundary, not inside a tick.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    cron_commit_gate_channel,
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
    otel_enabled,
    reconcile_cron_failures,
    record_backpressure_error,
    record_cron_failure,
    record_cron_lock_contention,
    record_published_message,
    safe_start_span,
    update_disabled_schedules_count,
)
from taskq.obs._redact_exc import safe_exception_message
from taskq.settings import WorkerSettings
from taskq.worker._transient import TRANSIENT_PG_ERRORS

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
    """The actor_config columns the tick reads for one actor.  ``max_pending``
    is the OPERATOR-stored cap — NULL stored means no stored override, so the
    registry literal stands (the seed-resolution rule); a non-NULL stored
    value is authoritative over it, exactly as it is for every client
    enqueue arm (``client/_capacity.py``)."""

    queue: str
    max_attempts: int
    retry_kind: str
    max_pending: int | None


_TICK_WRITE_RESERVE_FRACTION: Final = 0.1
"""Share of the whole-tick budget the factory deadline must leave unspent.

The leader wraps the whole tick in one ``asyncio.timeout`` and the
factory call sits inside it.  Armed with the same duration, the two
deadlines race with no deterministic winner, and when the outer one
wins it delivers ``CancelledError`` — a ``BaseException``, so the
per-schedule ``except Exception`` cannot catch it — aborting the tick
and rolling back every healthy peer planned beside the hung schedule.
``next_fire_at`` never advances, the identical batch is selected again,
and the hung schedule accrues no strike: a livelock with no telemetry.
Holding the factory deadline strictly inside the tick's remaining budget
makes the inner deadline the certain winner, so the hang becomes the
named per-schedule failure the strike accounting is built on, and the
reserve leaves room for the tick's own writes and its COMMIT.
"""


class _TickBudgetExhaustedError(TimeoutError):
    """The tick's funded budget was spent before this schedule's payload
    factory could be called, so the schedule is struck without waiting.

    The planning loop awaits each schedule's factory in turn, so the
    batch's AGGREGATE factory wait — not any single factory's — is what
    races the leader's whole-tick ``asyncio.timeout``.  Any per-factory
    floor under the clamp lets enough simultaneously-due hung factories
    sum past that deadline: the outer cancellation then rolls back every
    strike the tick recorded, and the identical batch is re-selected on
    the next tick.  Failing the schedule the moment no funded wait
    remains keeps the tick inside its deadline however many hung
    factories share the batch, and the strike keeps the schedule on its
    path to auto-disable.  A ``TimeoutError`` subclass because the
    outcome is a deadline expiring — but the message names the budget,
    not a hang: the factory never ran, so a "timed out" claim against it
    would be a lie.
    """


CRON_TICK_SQL_TEMPLATE: Final = (
    "WITH lock AS MATERIALIZED ("
    "  SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS got"
    ") "
    "SELECT lock.got, statement_timestamp() AS server_now, s.* "
    "FROM lock LEFT JOIN LATERAL ("
    "  SELECT id, actor, cron_expr, timezone, dst_strategy, payload_factory, "
    "         metadata, last_fired_at, consecutive_failures, next_fire_at, identity_key "
    '  FROM "{schema}".cron_schedules '
    "  WHERE lock.got AND enabled = true AND next_fire_at <= statement_timestamp() "
    "  ORDER BY next_fire_at "
    "  LIMIT $2"
    ") s ON true"
)
"""The tick's one opening statement: the cron try-lock ($1 = the lock
name), the planning clock, and the due read ($2 = the batch limit).

Rendered with ``{schema}`` replaced by the schema name; the plan-shape audit
(``tests/test_index_audit.py``) explains this same text, so the
index-servable due bound cannot drift there unnoticed. See
:func:`tick_cron` for why the three ride one statement and why the due
bound is ``statement_timestamp()``.
"""

# The commit-gate channel — ``cron_commit_gate_channel(schema)`` in
# ``taskq.constants`` — is the self-addressed channel the tick uses to
# learn that its own transaction committed. Postgres delivers a ``NOTIFY``
# to its own session only if the emitting transaction commits, and never
# if it rolls back — the only commit signal available to a function that
# runs INSIDE the caller's transaction and returns before the ``COMMIT``.
# The notification rides back on the same packet as the ``COMMIT``
# response, so the tick's telemetry lands while the caller is still inside
# its transaction block. Schema-scoped because channels share one
# database-wide namespace: a foreign schema's cron session on the same
# channel would not only hear this one's signals — its notification would
# mark THIS session confirmed-listening (the gate records the sender's
# pid) without this session's LISTEN ever having survived a commit.


_armed_commit_emits: dict[int, tuple[str, Callable[[], None]]] = {}
"""The emission waiting on each cron session's commit, keyed by that
session's backend pid and tagged with the arming tick's nonce.

One entry per live cron session — one in the shipped worker — because
each tick replaces its own session's entry.  The pid keys the map rather
than the connection object because a pooled connection is a proxy that
cannot be weak-referenced, and a strong key would pin it past its
checkout; the nonce distinguishes the current tick's notification from
one a rolled-back tick left unanswered.
"""

_confirmed_listening: set[int] = set()
"""Backend pids known to hold a server-side ``LISTEN`` registration that
has actually survived a commit.

``LISTEN`` is itself transactional in Postgres: issued inside a
transaction that later rolls back, the server discards the registration
along with everything else the transaction did.  asyncpg's client-side
bookkeeping (``Connection._listeners``) does not know this happened -- it
records the channel as listened-to unconditionally when ``add_listener``
first issues the ``LISTEN``, and never revisits that record on rollback.
A tick whose COMMIT fails (the exact case the commit gate exists to
handle) therefore leaves asyncpg believing the session is still
listening while Postgres has already forgotten it: every later tick's
``add_listener`` on that channel becomes a silent no-op (the channel is
already in ``self._listeners``), its ``NOTIFY`` is never delivered to a
session the server does not consider a listener, ``_dispatch_commit_gate``
never runs, and the connection reports no cron telemetry ever again --
even for ticks that commit cleanly afterward.  A pid enters this set only
from :func:`_dispatch_commit_gate`, proof a ``NOTIFY`` was actually
received on it; :func:`_emit_on_commit` forces a fresh ``LISTEN`` for any
pid absent from it.
"""


_termination_hooked: set[int] = set()
"""Backend pids whose connection carries the termination listener that
retires this module's per-pid gate state (:func:`_forget_commit_gate_session`).

One hook per connection, marked here so a tick re-arms rather than stacks
a second listener on the same connection.  The set is swept by the same
hook it gates, so it stays proportional to LIVE cron sessions — without
the hook, a tick that armed an emission and then lost its connection (a
rollback the server will never answer) would leave the entry behind, and
every confirmed ``LISTEN`` would outlive its session: under cron
connection churn both maps grow without bound, and a pid the server
recycles would inherit a dead session's "confirmed listening" proof.
"""


def _forget_commit_gate_session(pid: int) -> None:
    """Drop every commit-gate entry keyed by *pid*.

    Runs as the connection's termination listener: a dead session's armed
    emission can never be answered (its ``NOTIFY`` rolled back with the
    connection) and its confirmed ``LISTEN`` died with it, so neither may
    stand as state for a later session that reuses the pid.
    """
    _armed_commit_emits.pop(pid, None)
    _confirmed_listening.discard(pid)
    _termination_hooked.discard(pid)


def _commit_gate_termination_hook(pid: int) -> Callable[[object], None]:
    """Build the termination listener retiring *pid*'s gate state.

    The callback takes ``object`` rather than ``asyncpg.Connection``:
    asyncpg invokes it with the connection (or the pool proxy standing in
    for one), neither of which the hook needs — the pid is captured here.
    """

    def _drop(_conn: object) -> None:
        _forget_commit_gate_session(pid)

    return _drop


def _dispatch_commit_gate(_conn: object, pid: int, _channel: str, payload: str) -> None:
    """Run and retire the emission armed for *pid* under the *payload* nonce.

    A self-notify carries the sending session's own pid, so a
    notification from any other session finds no entry.  A nonce that
    does not match the armed one belongs to a tick whose emission was
    already superseded, and is ignored.  Running at all is proof this
    pid's ``LISTEN`` is genuinely live server-side, so it is recorded in
    :data:`_confirmed_listening`.
    """
    _confirmed_listening.add(pid)
    armed = _armed_commit_emits.get(pid)
    if armed is None or armed[0] != payload:
        return
    del _armed_commit_emits[pid]
    armed[1]()


async def _emit_on_commit(
    conn: asyncpg.Connection, emit: Callable[[], None], *, schema: str
) -> None:
    """Arrange for *emit* to run only if the caller's transaction commits.

    Every claim the tick's telemetry makes — a strike, an auto-disable, a
    published message, a failure-count reset — describes a row the tick
    wrote and the caller has yet to commit.  Emitting before that commit
    lets a failed ``COMMIT`` leave operators reading an auto-disable for a
    schedule the database still has enabled, and a failure count for a
    strike the database says never happened.  Gating on the ``NOTIFY``
    (see the commit-gate channel note above) makes the telemetry say exactly
    what the database kept: a rollback delivers nothing, so nothing is
    reported. The channel is *schema*'s own commit-gate channel.

    A pid not yet in :data:`_confirmed_listening` -- either this is its
    first tick, or an earlier tick's ``LISTEN`` was silently undone by
    that tick's own rollback (see the set's docstring) -- gets its
    client-side registration dropped first, so ``add_listener`` re-issues
    a real ``LISTEN`` inside THIS tick's transaction instead of trusting
    stale bookkeeping.  A confirmed pid skips straight to the cheap path:
    asyncpg already holds the callback, so ``add_listener`` costs no
    round trip.

    A connection that cannot carry a session-scoped ``LISTEN`` — a
    transaction-pooling proxy in front of Postgres, say — gets the
    emission inline instead.  Losing the gate costs the commit-time
    precision; losing the telemetry would cost the whole failure trail,
    and a tick that struck a schedule must always say so.
    """
    nonce = str(new_uuid())
    channel = cron_commit_gate_channel(schema)
    pid: int | None = None
    try:
        pid = conn.get_server_pid()
        if pid not in _confirmed_listening:
            await conn.remove_listener(channel, _dispatch_commit_gate)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow the callback type; asyncpg accepts a sync callback at runtime.
        await conn.add_listener(channel, _dispatch_commit_gate)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow the callback type; asyncpg accepts a sync callback at runtime.
        if pid not in _termination_hooked:
            # One hook per connection: retire this session's gate state when
            # the connection dies, so the maps track live sessions only and a
            # recycled pid never inherits a dead session's proof. Placed
            # after add_listener: a connection that cannot LISTEN (a
            # transaction-pooling proxy) takes the fallback below unchanged.
            conn.add_termination_listener(_commit_gate_termination_hook(pid))
            _termination_hooked.add(pid)
        # Replaces this session's previous entry: a tick whose transaction
        # rolled back left an emission no notification can ever answer.
        _armed_commit_emits[pid] = (nonce, emit)
        await conn.execute("SELECT pg_notify($1, $2)", channel, nonce)
    except (AttributeError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
        if pid is not None:
            _armed_commit_emits.pop(pid, None)
            _confirmed_listening.discard(pid)
        # The fallback trades the commit gate away: the emission below can
        # describe a transaction still in flight. A degraded outcome must
        # not be silent — the warning names why the gate is absent.
        log.warning(
            "cron-commit-gate-unavailable",
            kind="cron_commit_gate_fallback",
            error=safe_exception_message(exc) or type(exc).__name__,
        )
        emit()


def _factory_deadline(settings: WorkerSettings, elapsed_s: float) -> float | None:
    """The per-factory deadline for a factory called *elapsed_s* into the
    tick: the configured budget, clamped to stay strictly inside what is
    left of the leader's whole-tick deadline — or ``None`` when that
    leftover is spent.

    See :data:`_TICK_WRITE_RESERVE_FRACTION` for why the clamp is not
    merely advice to the operator.  ``None`` — never a floor — is the
    batch-safety half of the contract: per-factory waits sum across the
    batch (see :class:`_TickBudgetExhaustedError`), so a tick that cannot
    fund another wait must not grant one.
    """
    whole_tick = settings.dispatcher_command_timeout
    remaining = whole_tick * (1.0 - _TICK_WRITE_RESERVE_FRACTION) - elapsed_s
    budget = min(settings.cron_payload_factory_timeout, remaining)
    return budget if budget > 0 else None


def _resolve_max_pending(stored: int | None, literal: int | None) -> int | None:
    """The capacity resolution rule as one pure function: a non-NULL
    operator-stored ``actor_config.max_pending`` wins over the registry
    literal — tightening or loosening it — while a NULL stored value
    leaves the literal as the cap and neither means no cap.  The same rule
    the client path's ``ActorCapacityCache`` applies to every client
    enqueue, applied here so the tick's admission control answers the
    same question with the same precedence."""
    return stored if stored is not None else literal


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
class _BufferedFailureTelemetry:
    """Deferred failure telemetry for one failed-or-struck schedule.

    Carries everything the post-commit emission needs to open the
    per-failure span (PRODUCER kind, the link captured at failure time,
    schedule/worker attributes), mark it ERROR, attach the
    ``cron.auto_disabled`` event, and emit the metric delta.  Nothing is
    exported at failure time: a strike persists only if the tick's
    failures UPDATE executes AND the caller's transaction commits, so
    exporting at strike time claims schedule failures (and auto-disables)
    the database can still roll back — see the emission section in
    :func:`tick_cron`.
    """

    failure: _FireFailure
    exc: Exception
    links: list[trace.Link] | None
    worker_id: UUID


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


async def resolve_payload(
    row: asyncpg.Record, *, timeout_s: float | None = None
) -> dict[str, object]:
    """Resolve payload from ``payload_factory`` or ``static_payload``.

    Delegates to :func:`~taskq.cron.resolve_payload`, passing the
    caller's per-factory deadline through.  All exceptions (including
    ``TypeError`` from a factory returning an unexpected type)
    propagate to the caller so the per-schedule failure handling inside
    :func:`tick_cron` increments ``consecutive_failures`` and triggers
    auto-disable.
    """
    pf: str | None = row["payload_factory"]
    return await resolve_cron_payload(pf, row["metadata"], timeout_s=timeout_s)


def _compute_fire_failure(
    row: asyncpg.Record,
    exc: Exception,
    settings: WorkerSettings,
) -> _FireFailure:
    """The pure half of the per-failure except-branch: bump the failure
    count, decide auto-disable, sanitize the error text.

    No span, no export — the telemetry half (:func:`_mark_failure_span`)
    runs from the end-of-tick emission only, after every statement of the
    tick has executed; see :class:`_BufferedFailureTelemetry`.

    ``error_text`` is NUL-sanitized (see :class:`_FireFailure`) because it
    is bound as ``text`` by the batched failures UPDATE, and carries the
    exception's class name as a fallback when the message is empty:
    ``str(TimeoutError())`` is ``''`` — exactly what ``resolve_payload``'s
    ``wait_for`` raises for a payload factory that never returns — and
    without the fallback a schedule can be failing (and auto-disabled)
    with an empty reason in the column, the log event and the exported
    span status.
    """
    class_name = type(exc).__name__
    consecutive: int = (row["consecutive_failures"] or 0) + 1
    auto_disable = consecutive >= settings.cron_auto_disable_threshold
    return _FireFailure(
        schedule_id=row["id"],
        row=row,
        error_text=sanitize_nul_str(str(exc) or class_name),
        consecutive=consecutive,
        auto_disable=auto_disable,
    )


def _mark_failure_span(
    span: Span,
    failure: _FireFailure,
    exc: Exception,
) -> None:
    """The telemetry half of the old per-failure except-branch: mark the
    (already-open) failure span ERROR and attach ``cron.auto_disabled``.

    The span status description and the event carry
    :func:`safe_exception_message` — span text is exported to third-party
    telemetry backends, and ``str()`` of a constraint violation quotes row
    values.  Both fall back to the exception's class name when the message
    is empty, for the same reason as :func:`_compute_fire_failure`.
    """
    class_name = type(exc).__name__
    span_text = safe_exception_message(exc) or class_name
    span.set_status(StatusCode.ERROR, span_text)
    if failure.auto_disable:
        span.add_event(
            "cron.auto_disabled",
            {
                "schedule_name": failure.row["actor"],
                "last_error": span_text,
                "failure_count": failure.consecutive,
            },
        )


async def _suppress_policy_collisions(
    conn: asyncpg.Connection,
    schema: str,
    successes: list[_FireSuccess],
    actor_policies: Mapping[str, ActorFirePolicy],
    actor_configs: Mapping[str, _ActorConfig],
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

    Each capped actor's effective cap is RESOLVED here, not taken from
    the policy map alone: a non-NULL operator-stored
    ``actor_config.max_pending`` (carried by *actor_configs*, from the
    ac-rows SELECT the tick already runs) is authoritative over the
    registry literal the policy map carries — it can tighten a declared
    literal or cap an actor declared without one, including the stored-0
    emergency drain — while a NULL stored value leaves the literal
    standing and neither means uncapped, exactly as today.  The singleton
    flag stays registry-only: it is not a stored actor_config field.
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
        if plan.actor in blocking:
            continue
        policy = actor_policies.get(plan.actor)
        ac = actor_configs.get(plan.actor)
        resolved = _resolve_max_pending(
            ac.max_pending if ac is not None else None,
            policy.max_pending if policy is not None else None,
        )
        if resolved is not None:
            capped[plan.actor] = resolved
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


_DETAIL_KEY_RE = re.compile(r"^Key \((?P<cols>[^)]*)\)=\((?P<vals>.*)\) already exists\.$")
"""Postgres' unique-violation detail line: the index's columns and the
colliding values.  ``jobs_singleton_uniq`` is keyed on ``(actor)`` and
``jobs_pkey`` on ``(id)`` (the initial migration), so the values name the
colliding actor or the collided job id — the one fact per-plan
attribution needs without re-inserting anything."""

_ATTRIBUTABLE_CONSTRAINTS: Final[frozenset[str]] = frozenset({"jobs_pkey", "jobs_singleton_uniq"})
"""The only constraints whose violations per-plan attribution may trust.

The detail regex accepts any ``(cols)=(vals) already exists.`` shape an
index can produce, but only these two are TaskQ's own.  An operator-added
non-partial unique index on ``(actor)`` raises the same ``Key
(actor)=(x)`` detail under a different constraint name; see
:func:`_attribute_violation`."""


def _attributable_violation(exc: Exception) -> UniqueViolationError | None:
    """The ``UniqueViolationError`` *exc* carries, raw or converted.

    The enqueue paths convert a server-side ``jobs_singleton_uniq``
    violation into a typed refusal (:class:`SingletonCollisionError`)
    raised ``from`` the driver's error — the caller-facing contract for
    client enqueues.  For the tick's per-plan attribution that wrapper is
    opaque: the constraint name and the ``Key (cols)=(vals)`` detail the
    attribution parses live on the wrapped violation, and the wrapper's
    own text (``BackpressureError: actor=…, pending=0, max_pending=None``)
    names neither — a strike recorded from it loses the committed
    outcome's identity.  Walking the explicit cause chain (``__cause__``
    only — the conversion's documented intent, never ``__context__``,
    which can name an unrelated exception merely being handled) lets
    attribution and the strike text follow the violation that COMMITTED,
    not the conversion.  Bounded: a pathological chain must not loop the
    tick, and an unconverted violation answers at depth zero.
    """
    current: BaseException | None = exc
    for _ in range(8):
        if isinstance(current, UniqueViolationError):
            return current
        if current is None:
            return None
        current = current.__cause__
    return None


def _attribute_violation(
    pending: list[_FireSuccess],
    exc: Exception,
) -> tuple[UniqueViolationError, list[_FireSuccess], list[_FireSuccess]] | None:
    """Map a batched-INSERT unique violation to the plan(s) that caused it.

    Returns ``(violation, offenders, survivors)`` — the unwrapped
    violation the strike text and span status are recorded from, the
    plan(s) it convicts, and the plans cleared to retry — or ``None``
    when the error cannot be attributed SAFELY — any other exception
    type, a violation of a constraint TaskQ does not own, an unparsable
    or truncated detail line, or a value naming no pending plan.  The
    caller isolates per plan on ``None`` rather than guessing: striking
    the wrong schedule is the auto-disable trap this whole path exists
    to avoid.

    Every attribution is verified against the pending plans before it is
    trusted: an ``id`` value must be a job id some pending plan actually
    mints, and an ``actor`` value must name a plan whose args carry the
    ``singleton`` stamp — the partial index covers only stamped rows, so
    an unstamped plan for the same actor cannot be the violator.  A value
    that verifies against nothing pending means the detail is not telling
    us which of OUR rows collided (PG truncates long detail values;
    indexes can be added by an operator) — unattributable, on purpose.
    """
    violation = _attributable_violation(exc)
    if violation is None:
        return None
    # Why: gate on the constraint NAME, not just the detail's column list.
    # Today only jobs_pkey (id) and jobs_singleton_uniq (actor, partial)
    # can produce the detail shapes parsed below — but an operator-added
    # non-partial unique index on (actor) raises the same "Key
    # (actor)=(x) already exists." detail under its own name, and
    # attributing from the detail alone would strike a singleton-stamped
    # plan of that actor when the violator was an unstamped row the
    # operator's index (not TaskQ's) rejected — a wrong strike toward
    # auto-disable.  A None/unknown constraint name falls back too: only
    # the two names TaskQ ships are attributable.
    if violation.constraint_name not in _ATTRIBUTABLE_CONSTRAINTS:
        return None
    match = _DETAIL_KEY_RE.match(violation.detail or "")
    if match is None:
        return None
    cols = match.group("cols")
    value = match.group("vals")
    if cols == "id":
        try:
            collided: UUID = UUID(value)
        except ValueError:
            return None
        offenders = [
            plan for plan in pending if any(args.id == collided for args in plan.enqueue_args)
        ]
    elif cols == "actor":
        offenders = [
            plan
            for plan in pending
            if plan.actor == value
            and any(args.metadata.get("singleton") is True for args in plan.enqueue_args)
        ]
    else:
        return None
    if not offenders:
        return None
    struck_ids = {plan.schedule_id for plan in offenders}
    return violation, offenders, [plan for plan in pending if plan.schedule_id not in struck_ids]


def _strike_plans(
    plans: list[_FireSuccess],
    exc: Exception,
    failures: list[_FireFailure],
    telemetry: list[_BufferedFailureTelemetry],
    worker_id: UUID,
    settings: WorkerSettings,
) -> None:
    """Convert write-failed plans into per-schedule failures, appended to
    *failures*, and buffer their telemetry in *telemetry* for the
    end-of-tick emission.

    The failure RECORD must exist at strike time — the tick's failures
    UPDATE binds it — but the span, auto-disable event and metric delta
    must not be exported here: the strikes only persist if that UPDATE
    executes AND the caller's transaction commits, and a TRANSIENT error
    from any later statement of the tick rolls them all back (the
    emission site in :func:`tick_cron` is the single exporter).
    """
    for plan in plans:
        current_span = trace.get_current_span()
        current_ctx = current_span.get_span_context()
        links = [trace.Link(current_ctx)] if current_ctx.is_valid else None
        failure = _compute_fire_failure(plan.row, exc, settings)
        failures.append(failure)
        telemetry.append(
            _BufferedFailureTelemetry(
                failure=failure,
                exc=exc,
                links=links,
                worker_id=worker_id,
            )
        )


async def _enqueue_planned_fires(
    conn: asyncpg.Connection,
    backend: Backend,
    plans: list[_FireSuccess],
    failures: list[_FireFailure],
    telemetry: list[_BufferedFailureTelemetry],
    worker_id: UUID,
    settings: WorkerSettings,
) -> list[_FireSuccess]:
    """Enqueue the planned fires and return the plans whose jobs landed.

    The whole batch is ONE ``INSERT ... SELECT`` statement, so Postgres
    aborts the entire statement when a single row violates a constraint —
    and a statement error poisons the surrounding transaction (every later
    statement fails with SQLSTATE 25P02 until rollback).  Both halves of
    that sentence are what the pre-batching per-row enqueue never had to
    face, and what this helper's shape contains:

    * The enqueue runs inside a SAVEPOINT (asyncpg's nested
      ``conn.transaction()`` on the caller's already-open transaction): a
      failed batch rolls back to the savepoint, leaving the caller's
      transaction alive and the tick's remaining bookkeeping — the
      survivors' advance, the suppression UPDATE, the strikes — committable.
      Without it, the failure UPDATE below the old inline except-branch
      raised ``InFailedSQLTransactionError`` itself: no strike ever
      persisted, while the span/metric telemetry still claimed every
      schedule failed (and the leader's backstop guard counted the
      non-transient abort toward killing the worker).
    * A unique violation is attributed from the error itself
      (:func:`_attribute_violation`): the colliding plan(s) take one
      strike each and the SURVIVORS retry as a batch.  The preflight is
      advisory — a client enqueue committing between the preflight SELECT
      and this INSERT (READ COMMITTED: the INSERT takes a fresh snapshot)
      is a race the tick lost for that one actor, not a defect of every
      schedule in the batch.  Each retry strikes at least one plan, so
      the loop is bounded by the batch size.
    * Transient PG errors (:data:`TRANSIENT_PG_ERRORS` — statement
      timeout, connection drop, server shutdown) re-raise without
      recording a single failure: the caller's transaction rolls back and
      the leader's transient handling retries the tick.  A strike is a
      statement about the SCHEDULE's health; PG weather must not write
      one, however many schedules were in flight.
    * Any other failure is not attributable from the error alone, so each
      plan retries in its own savepoint: the plans that individually fail
      take their own strike with their own exception; the plans that
      individually succeed land.  This is the fallback for shapes like a
      check violation or a NUL that escaped to the server — per-plan cost
      is paid only on the failure path.
    """
    pending = list(plans)
    landed: list[_FireSuccess] = []
    while pending:
        batch_args = [args for plan in pending for args in plan.enqueue_args]
        try:
            async with conn.transaction():
                await backend.enqueue_batch(
                    batch_args,
                    connection=conn,
                    # Pre-admitted by _suppress_policy_collisions, which
                    # trims every plan to the remaining capacity: re-checking
                    # here would turn a concurrent client enqueue borrowing the
                    # last slot into a whole-tick abort, striking schedules
                    # whose only defect is a busy actor (the auto-disable trap
                    # the preflight exists to prevent). Accepted converse cost:
                    # a client commit landing between the preflight SELECT and
                    # this INSERT overshoots the cap by that commit, bounded by
                    # one tick's kept plans — liveness over strictness, stated.
                    # That same commit can violate jobs_singleton_uniq — the
                    # attribution + survivor-retry below is the backstop for
                    # exactly that window.
                    enforce_max_pending=False,
                )
            landed.extend(pending)
            pending = []
        except TRANSIENT_PG_ERRORS:
            raise
        except Exception as exc:
            attributed = _attribute_violation(pending, exc)
            if attributed is not None:
                # Strike with the unwrapped violation, not the raised
                # wrapper: a converted refusal's own text (the typed
                # SingletonCollisionError) names no constraint, so the
                # recorded strike would not identify the committed
                # outcome — the racer's row must carry
                # jobs_singleton_uniq itself, exactly as an unconverted
                # jobs_pkey collision does.
                violation, offenders, survivors = attributed
                _strike_plans(offenders, violation, failures, telemetry, worker_id, settings)
                pending = survivors
            else:
                for plan in pending:
                    try:
                        async with conn.transaction():
                            await backend.enqueue_batch(
                                plan.enqueue_args,
                                connection=conn,
                                enforce_max_pending=False,
                            )
                        landed.append(plan)
                    except TRANSIENT_PG_ERRORS:
                        raise
                    except Exception as plan_exc:
                        _strike_plans([plan], plan_exc, failures, telemetry, worker_id, settings)
                pending = []
    return landed


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
    the PG server clock in the same statement as the due read: the
    due-check (``next_fire_at <= statement_timestamp()``) is server-side,
    so every croniter seed must come from the same domain — and from the
    same instant, so a due row's ``next_fire_at`` never exceeds the seed.
    Seeding from the leader's Python clock shifts every recomputed fire by
    the app↔DB skew and can recompute ``next_fire_at`` into the server's
    past (a fire loop).

    *actor_policies* carries the worker's ``actor_registry`` singleton /
    ``max_pending`` flags (``None`` — the default, and every pre-plumbing
    caller — stamps nothing and enforces nothing, exactly the previous
    behavior).  With flags present, planned fires carry the same stamps a
    client enqueue would get, and a fire blocked by an active singleton
    job or a full pending cap is SUPPRESSED: dropped from the batch,
    ``next_fire_at`` advanced, neither a fire nor a failure (suppressed
    slots are absent from the return count).  The ``max_pending`` cap is
    resolved per actor against the operator-stored ``actor_config`` row
    the tick already reads — a non-NULL stored value is authoritative
    over the registry literal, the client path's own rule — so a stored
    cap (including the stored-0 emergency drain) bounds the tick exactly
    like the same literal cap.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("limit", limit)

    tick_started = time.monotonic()
    lock_name = schema_lock_name("cron", schema)
    # One statement for the try-lock, the planning clock and the due read
    # (pg-boss folds its cron lock into the read the same way): the leader
    # ticks once a second and is idle almost always, so the idle tick's
    # cost is the round-trip count. The lock sits in a MATERIALIZED CTE
    # so it is taken exactly once and before the read; the LATERAL read is
    # gated on the verdict, so a contended tick reads nothing and returns
    # one row with got = false. LEFT JOIN keeps that one row (and the
    # idle tick's) when the read yields nothing.
    #
    # statement_timestamp() (STABLE) — not clock_timestamp() (VOLATILE) —
    # for the due bound: a volatile comparison cannot be a btree index
    # condition, so cron_schedules_next_fire_idx (partial on enabled,
    # keyed on next_fire_at) would degrade from an Index Cond that stops
    # at the boundary to a post-scan filter walk of every enabled entry,
    # per tick, every second. Measured at 10k enabled schedules (PG 18,
    # EXPLAIN ANALYZE): clock_timestamp() walks all 10,000 entries
    # (1.04 ms); statement_timestamp() is an Index Cond scan (2 buffers,
    # 0.005 ms). The same instant is the planning clock (server_now):
    # every croniter seed and the due bound come from one server-side
    # reading, so a due row is never "in the future" of its own seed.
    tick_rows: list[asyncpg.Record] = await conn.fetch(
        CRON_TICK_SQL_TEMPLATE.replace("{schema}", schema),
        lock_name,
        limit,
    )
    head = tick_rows[0]
    lock_acquired: bool = bool(head["got"])
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

    server_now: datetime = head["server_now"]
    # The idle tick's one row carries the verdict and the clock with NULL
    # schedule columns; a due row always has an id.
    rows = [row for row in tick_rows if row["id"] is not None]
    if not rows:
        return 0

    # One round trip for every distinct actor in the batch.  A missing actor
    # is not an error here — the planning loop turns each affected schedule
    # into a per-schedule failure with the same message the per-row lookup
    # raised before batching.
    actors: list[str] = sorted({str(row["actor"]) for row in rows})
    ac_rows: list[asyncpg.Record] = await conn.fetch(
        f"SELECT actor, queue, max_attempts, retry_kind, max_pending "
        f'FROM "{schema}".actor_config '
        f"WHERE actor = ANY($1::text[])",
        actors,
    )
    actor_configs: dict[str, _ActorConfig] = {
        str(ac_row["actor"]): _ActorConfig(
            queue=ac_row["queue"],
            max_attempts=ac_row["max_attempts"],
            retry_kind=ac_row["retry_kind"],
            max_pending=ac_row["max_pending"],
        )
        for ac_row in ac_rows
    }

    successes: list[_FireSuccess] = []
    failures: list[_FireFailure] = []
    # Failure telemetry (spans, auto-disable events, metric deltas) is
    # buffered here and exported ONLY after every statement of the tick
    # has executed — see the emission section at the end of this function.
    failure_telemetry: list[_BufferedFailureTelemetry] = []

    for row in rows:
        current_span = trace.get_current_span()
        current_ctx = current_span.get_span_context()
        links = [trace.Link(current_ctx)] if current_ctx.is_valid else None

        with safe_start_span(
            "cron fire",
            kind=SpanKind.PRODUCER,
            attributes={
                "cron_schedule_name": row["actor"],
                "taskq.worker_id": str(worker_id),
                # Why: per-schedule attribution lives here and on the log
                # lines, not on the consecutive-failures metric's label --
                # span cardinality is free (see obs/_otel.py).
                "taskq.cron_schedule_id": str(row["id"]),
            },
            links=links,
            new_root=True,
        ):
            try:
                successes.append(
                    await _plan_fire(
                        row,
                        server_now,
                        settings,
                        actor_configs,
                        actor_policies,
                        tick_started=tick_started,
                    )
                )
            except Exception as exc:
                # Why buffered, not marked on the span above: that span
                # records the planning ATTEMPT and closes UNSET; the failure
                # claim (ERROR status, auto-disable event) belongs to the
                # emission span, which opens only once the transaction has
                # committed — a rollback takes this failure with it.
                failure = _compute_fire_failure(row, exc, settings)
                failures.append(failure)
                failure_telemetry.append(
                    _BufferedFailureTelemetry(
                        failure=failure,
                        exc=exc,
                        links=links,
                        worker_id=worker_id,
                    )
                )

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
            conn, schema, successes, actor_policies, actor_configs
        )

    if successes:
        successes = await _enqueue_planned_fires(
            conn, backend, successes, failures, failure_telemetry, worker_id, settings
        )

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

    disabled_count_after: int | None = None
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
            # SQL phase, deliberately: the count must be read INSIDE this
            # transaction (it includes the tick's own uncommitted disables).
            # Only the gauge export is deferred to the emission section
            # below, with the rest of the strike telemetry.
            disabled_count_after = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema}".cron_schedules WHERE enabled = false'
            )

    # The failure level every actor with a failing schedule now carries,
    # read INSIDE this transaction so it includes the tick's own
    # uncommitted strikes and resets, and read over the whole table rather
    # than scoped to this batch's actors: a batch covers only DUE
    # schedules, so an actor whose failing schedule was disabled or
    # deleted between ticks never re-enters a batch, and a batch-scoped
    # read would strand its reported level at its last value forever.
    # One aggregate over the operator-sized cron_schedules table — and
    # skipped when telemetry is off, the reconcile it feeds being its
    # only consumer.
    failure_totals: dict[str, int] = (
        await _actor_failure_totals(conn, schema) if otel_enabled() else {}
    )

    # ── Telemetry emission — gated on the caller's COMMIT ─────────────
    #
    # Why: every claim below describes a row this tick wrote — a strike,
    # an auto-disable, a published message, a failure-count reset — and
    # those rows persist only if the caller's transaction commits. Emitted
    # inline, a failed COMMIT would leave operators reading an
    # auto-disable for a schedule the database still has enabled, and a
    # failure count for a strike the database says never happened, with no
    # way to reconstruct the trail from the row. So the whole emission is
    # a closure armed on the transaction's commit (see _emit_on_commit);
    # a rollback delivers nothing and the tick reports nothing.

    def _emit() -> None:
        for plan in successes:
            if plan.prev_consecutive > 0:
                # Why actor, not schedule_id: the metric's dimension is the
                # registered actor set (bounded by the shipped code); the
                # schedule id rides the log line below and the cron-fire span.
                # The delta is this schedule's OWN prior count, so the actor's
                # balance lands on the sum of its other schedules' counts.
                record_cron_failure(plan.actor, -plan.prev_consecutive)
            log.info(
                "cron fired",
                kind="cron_fire",
                actor=plan.actor,
                worker_id=str(worker_id),
                schedule_id=str(plan.schedule_id),
                next_fire_at=plan.next_fire_at.isoformat(),
            )
            record_published_message(plan.actor, plan.queue)

        for entry in failure_telemetry:
            # Span shape matches the strike-time export (PRODUCER kind, the
            # link captured at failure time, the cron_schedule_name /
            # worker_id / cron_schedule_id attributes) — only the emission
            # TIME moved.
            with safe_start_span(
                "cron fire",
                kind=SpanKind.PRODUCER,
                attributes={
                    "cron_schedule_name": entry.failure.row["actor"],
                    "taskq.worker_id": str(entry.worker_id),
                    # Why: same per-schedule attribution as the planning-loop
                    # span above -- the metric lost this label on purpose.
                    "taskq.cron_schedule_id": str(entry.failure.schedule_id),
                },
                links=entry.links,
                new_root=True,
            ) as span:
                _mark_failure_span(span, entry.failure, entry.exc)
            failure = entry.failure
            log.error(
                "cron schedule auto-disabled" if failure.auto_disable else "cron fire failed",
                kind="cron_fire",
                actor=failure.row["actor"],
                worker_id=str(entry.worker_id),
                schedule_id=str(failure.schedule_id),
                consecutive_failures=failure.consecutive,
                error=failure.error_text,
            )
            # Why actor: the schedule id stays on this log line and the
            # cron-fire span, not on the metric (see the cardinality note in
            # obs/_otel.py).
            record_cron_failure(failure.row["actor"], 1)

        # Last, so it settles the series on what the database holds
        # whatever the per-event deltas above added: the deltas describe
        # what THIS tick did, the reconciliation describes what every
        # process has left behind.
        reconcile_cron_failures(failure_totals)

        if disabled_count_after is not None:
            update_disabled_schedules_count(disabled_count_after)

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

    await _emit_on_commit(conn, _emit, schema=schema)
    return len(successes)


async def _actor_failure_totals(conn: asyncpg.Connection, schema: str) -> dict[str, int]:
    """The database's own summed ``consecutive_failures`` per actor, for
    every actor with at least one failing schedule.

    Whole-table, not scoped to the tick's batch: the batch covers only
    DUE schedules, so an actor whose failing schedule was disabled or
    deleted with nothing left due never appears in a batch again, and a
    batch-scoped read would leave its reported level stranded at its last
    value.  ``cron_schedules`` is an operator-sized configuration table —
    bounded by the schedules an operator creates, never by backlog depth —
    so one aggregate over it is a constant-cost read on a path that runs
    once a second.  The reconcile this feeds reads an actor's ABSENCE
    here as a true zero (no failing schedule anywhere), which is what
    lets a delete or a re-enable performed by any process self-correct.
    """
    # One row, one JSON column: the aggregate is per-actor but the result
    # is a single value, so the tick spends one round trip and no
    # row-decoding pass on a path that runs every second.
    row: asyncpg.Record | None = await conn.fetchrow(
        f"SELECT COALESCE(jsonb_object_agg(actor, total), '{{}}'::jsonb) AS totals FROM ("
        f"SELECT actor, SUM(consecutive_failures) AS total "
        f'FROM "{schema}".cron_schedules '
        f"WHERE consecutive_failures > 0 "
        f"GROUP BY actor"
        f") per_actor"
    )
    if row is None:
        return {}
    aggregated = loads(str(row["totals"]))
    if not isinstance(aggregated, dict):
        return {}
    return {str(actor): int(total) for actor, total in aggregated.items()}  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]  # Why: the JSON decoder is untyped; the aggregate's shape is fixed by the SELECT above.


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
    *,
    tick_started: float,
) -> _FireSuccess:
    """Plan one due schedule's fire: resolve the fire time (miss handling),
    payload and enqueue args, and the next ``next_fire_at`` — all in memory,
    inside the caller's per-schedule span.

    *server_now* is the PG server clock read inside the caller's tick
    transaction (``clock_timestamp()``) — the single domain for the
    catch-up cutoff and the beyond-window recompute, matching the
    server-side due-check that selected this row.

    *tick_started* is the tick's ``time.monotonic()`` origin; the payload
    factory's deadline is clamped against what is left of the leader's
    whole-tick budget from it (see :func:`_factory_deadline`).  When no
    funded wait remains, a factory-backed schedule fails immediately with
    :class:`_TickBudgetExhaustedError` — the factory is never called — so
    a batch of hung factories cannot sum past the whole-tick deadline.
    Static-payload schedules pay no factory wait and plan regardless.

    *actor_policies* stamps the planned args with the actor's singleton /
    ``max_pending`` flags exactly the way the client enqueue path stamps
    its own (``client/_args.py``); ``None`` stamps nothing.  The
    ``max_pending`` stamp is the stored-over-literal RESOLUTION against
    the actor's ``actor_config`` row (the client path passes its
    capacity-cache-resolved value, not the raw literal), so the args
    carry the same effective cap a client enqueue's would.
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
    # ActorRef and passes its capacity-cache-RESOLVED max_pending (a
    # non-NULL stored actor_config value over the ref literal); the tick
    # resolves the same rule from its own ac-rows read, so a cron fire's
    # args never carry a stale literal.  Without them the
    # jobs_singleton_uniq partial index (keyed on the flag) never covers a
    # cron fire and no cap applies.  "cron_schedule_id" is provenance: the
    # twin-coverage walk scopes delivered instants to the schedule that
    # enqueued them — identity_key cannot serve that scope (it defaults to
    # NULL and is a user-facing dedup handle shared with on-demand jobs).
    stamped_metadata: dict[str, object] = {"cron_schedule_id": str(row["id"])}
    if policy is not None and policy.singleton:
        stamped_metadata["singleton"] = True
    stamped_max_pending: int | None = _resolve_max_pending(
        ac.max_pending,
        policy.max_pending if policy is not None else None,
    )

    identity_key_raw: object = row["identity_key"]
    schedule_identity_key: IdentityKey | None = (
        IdentityKey(str(identity_key_raw)) if identity_key_raw is not None else None
    )

    payload_budget = _factory_deadline(settings, time.monotonic() - tick_started)
    if row["payload_factory"] is not None and payload_budget is None:
        raise _TickBudgetExhaustedError(
            "cron tick budget exhausted before payload factory "
            f"{row['payload_factory']!r} could run"
        )
    payload = await resolve_payload(row, timeout_s=payload_budget)

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
