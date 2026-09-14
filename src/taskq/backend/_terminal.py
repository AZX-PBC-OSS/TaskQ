"""Terminal-write operations for PostgresBackend.

All ``mark_*`` methods, ``write_cancel_escalation``, ``write_attempt``,
and their shared helpers (attempt inserts, event inserts, owner lookup)
live here as module-level functions taking explicit
``(conn, sql: SqlTemplates, ...)`` or ``(pool, sql: SqlTemplates, ...)``
parameters.  :class:`~taskq.backend.postgres.PostgresBackend` methods are
thin wrappers that acquire the appropriate pool and delegate.

One statement per terminal write
===============================

Each ``mark_*`` write is ONE data-modifying-CTE statement
(``sql.mark_*`` in ``_sql_templates.py``): the fenced ``jobs`` UPDATE,
the ``job_attempts`` INSERT, and the ``job_events`` INSERT execute inside
a single statement instead of three awaited round trips inside one
transaction.  E2E profiling of the dispatch loop
(benchmarks/e2e_dispatch.py, 2000-job drain against local compose
Postgres) measured the three-statement form at ~1.4 ms per job — ~89% of
dispatch wall-clock and ~10x the terminal UPDATE's own 0.14 ms — with the
round trips, not the statements' work, dominating.  Fusing them, plus
dropping the pool path's now-redundant explicit BEGIN/COMMIT (a single
SQL statement — CTEs included — is atomic by itself, so the transaction
context manager's two round trips bought nothing the statement doesn't
already provide; the LOOP-scope ``mark_succeeded_with_conn`` path still
runs inside the actor's transaction as before), removes four round trips
per job with NO change to commit semantics: statement failure still
aborts the write whole, still classifies through
``_TERMINAL_WRITE_INFRA_EXCEPTIONS``, and still leaves the job
``running`` for lease-sweep reclaim (at-least-once).

Invariants preserved verbatim from the three-statement form:

* The UPDATE stays the single arbiter: its fencing WHERE (``status =
  'running' AND locked_by_worker = $2`` — now one epoch deeper with the
  attempt conjunct ``AND attempt = $k``, the handler's dispatch-time
  job-row attempt snapshot threaded from every call site: a stale
  attempt's write after a same-worker reclaim/redispatch no-ops exactly
  like a different worker's late write, Oban's ``ack_query`` contract)
  decides everything, and an empty ``upd`` CTE makes the INSERT CTEs
  insert nothing and the final ``SELECT`` return no row — the exact
  ``rec is None`` / ``WorkerOwnershipMismatch`` / ``False`` contract,
  without a second read.
* ``job_attempts.worker_id`` resolves through the holder-CTE idiom
  (``FOR KEY SHARE`` probe, LEFT-scan semantics via scalar subquery): a
  present worker row records the id, an already-deleted one records
  NULL, mirroring the column's ON DELETE SET NULL — never an FK
  violation (the constraint-violation tear-down risk documented on
  ``_sql.py``'s INSERT_ATTEMPT_SQL).
* Every timestamp is database-written (``clock_timestamp()``; the
  retry/snooze arms' ``now_ts``); ``duration_ms`` is computed in the
  statement from the same started/finished pair Python used to receive
  and re-multiply — but server-side, with exact numeric arithmetic
  instead of Python's float path.  Values can differ from the old
  Python computation by 1ms on exactly-whole-millisecond boundaries
  (where the float product drifted just below the integer); the
  server-side values are strictly more accurate.  ``trunc()`` keeps
  the same truncation toward zero (a bare ``::int`` cast rounds to
  nearest).
* Per-arm arbitration of the two- and three-arm variants
  (``mark_retry``, ``mark_snoozed``, ``mark_retry_after_*``): each arm
  chains its own attempt/event CTEs with that arm's outcome, error
  fields, and detail jsonb, so the ``outcome_branch`` tri-state the
  Python returns is computed by the same UPDATE predicates as before.
* Event ``detail`` is built server-side with ``jsonb_build_object``;
  ``jsonb_strip_nulls`` drops the keys Python conditionally omitted
  (``error_class`` when absent, ``worker_id`` for NULL holders).  The
  stored jsonb is key-order-normalized either way, so readers parsing
  the detail see the identical object.

Bounded pool checkout
=====================

Every pool-bearing function here acquires with ``pool.acquire(timeout=…)``
(keyword-only ``acquire_timeout``, default
:data:`DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S`; the PostgresBackend
wrappers thread ``dispatcher_command_timeout``). An exhausted or wedged
pool therefore fails the individual write inside a bound instead of
queueing it forever: the ``TimeoutError`` is the designed terminal-write
infra failure (see ``worker/_handlers.py``), never a job outcome — the
row stays ``running`` and lease expiry reclaims it. This is what keeps a
cancel storm of concurrent consumer ``mark_cancelled`` writes from
wedging both the writers AND any loop that shares the pool (the
heartbeat spiral pinned by
``tests/test_rt_locks_terminal_write_pool_starvation.py``).

Deliberately NOT done here (the evaluated alternative): coalescing
outcomes across jobs into one multi-row flusher.  Completion is the
job's final act — the consumer ``await``s (under ``asyncio.shield``)
its terminal write before reporting the outcome, so a returned
``mark_succeeded`` means the row is durably terminal, and the at-least-
once window (crash → lease sweep → re-run) is exercised only when the
write itself fails.  A non-awaited flusher would widen that window to
every successful job (a crash inside the flush window re-runs already-
succeeded actors) and reorder ``on_success`` hooks, terminal Redis
publishes, and progress-buffer bookkeeping that all assume post-commit
ordering; an awaited flusher adds queueing latency without removing the
round trips this merge already collapsed.  The CTE fusion captures the
measured win with zero semantic movement, so the coalescing design was
rejected rather than shipped behind a setting.
"""

from datetime import timedelta
from typing import TYPE_CHECKING, Final, Literal
from uuid import UUID

import structlog

# Why: private import — the pre-serialized result path holds bytes, not a
# dict, so the byte-level scan is the only way to run dumps_jsonb_str's NUL
# guard without a second serialization. Same package, behavior pinned by test.
from taskq._json import (
    NUL_JSONB_ERROR,
    _encoded_has_nul,  # pyright: ignore[reportPrivateUsage]  # Why: byte-level NUL scan for the pre-serialized bytes path — see the comment above.
    decode_result_bytes,
    dumps_jsonb_str,
    sanitize_surrogates,
)
from taskq.backend._protocol import (
    AttemptRow,
    ConnLike,
    DenialReason,
    ErrorInfo,
    JobId,
    JobRow,
    SnoozeOutcome,
    validate_denial_reason,
    validate_snooze_outcome,
)
from taskq.backend._records import (
    _job_row_from_record,
    jsonb_param,
    parse_rowcount,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.constants import MAX_RESULT_BYTES
from taskq.exceptions import (
    ResultTooLarge,
    UnencodableValue,
    WorkerOwnershipMismatch,
)
from taskq.obs import (
    get_logger,
    log_cancel_phase_change,
    log_state_change,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_insert_cancel_request_event",
    "_insert_state_change_event",
    "_mark_abandoned",
    "_mark_cancelled",
    "_mark_failed_or_retry",
    "_mark_retry",
    "_mark_retry_after",
    "_mark_snoozed",
    "_mark_succeeded",
    "_mark_succeeded_on_conn",
    "_select_owner",
    "_write_attempt",
    "_write_cancel_escalation",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: Fallback bound (seconds) for a terminal write's pool checkout when the
#: caller does not thread one in. Every terminal write is ONE fused
#: statement (the ~0.14 ms UPDATE the module docstring's e2e profile
#: measured), so a checkout that cannot resolve in a quarter second is a
#: wedged or closing pool, not a busy one — and the write's correct
#: outcome there is the DESIGNED failure: the acquire's ``TimeoutError``
#: surfaces as terminal-write infra (``_TERMINAL_WRITE_INFRA_EXCEPTIONS``
#: in worker/_handlers.py explicitly anticipates "timeout acquiring a
#: pool connection"), the job row stays ``running``, and lock-lease
#: expiry reclaims it at-least-once. Production callers
#: (:class:`~taskq.backend.postgres.PostgresBackend`) thread
#: ``dispatcher_command_timeout`` — the operator knob the repo's other
#: bounded acquires use — so this default binds only direct module-level
#: callers, which otherwise had NO bound at all.
DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S: Final[float] = 0.25


# ── Attempt-epoch + derived-text bind helpers ──────────────────────────


def _progress_jsonb_escaped(value: dict[str, object] | None) -> str | None:
    """``jsonb_param`` for ACTOR-DERIVED progress state, with the cold-path
    surrogate escape.

    The progress state reaching a terminal write is the coalesced buffer
    the actor filled, not caller input, so a value no UTF-8 encoder
    accepts (a lone surrogate published below the publish guard's
    wiring) is escaped here instead of refused: the write lands with the
    defect visible (:func:`~taskq._json.sanitize_surrogates`), never
    stranding the job ``running`` in the crash-reclaim loop. Mirrors the
    in-memory twin's ``_merge_progress`` (testing/_terminal.py) exactly —
    the escape runs only on the cold path the serialization already
    rejected, and a structural refusal (over-deep nesting) still stands,
    because escaping cannot repair it. NUL stays refused (the ValueError
    the twin raises for the same input) — the NUL family's split.
    """
    if value is None:
        return None
    try:
        return dumps_jsonb_str(value)
    except UnencodableValue as exc:
        try:
            return dumps_jsonb_str(sanitize_surrogates(value))
        except RecursionError:
            # from None: the walk's stack exhaustion is an artifact of the
            # repair attempt, not the refusal's cause — the original
            # UnencodableValue is the truthful failure (twin-mirrored).
            raise exc from None


def _error_text_escaped(value: str | None) -> str | None:
    """Escape unencodable codepoints in DERIVED error text before the bind.

    The message and traceback are derived from an uncontrolled exception
    the actor raised; rejecting them would strand the very job the text
    describes, and binding them raw raises asyncpg ``DataError`` (a
    ``PostgresError`` subclass) that the terminal-write classification
    misreads as transient infra — the job loops through reclaim against
    the same unencodable text forever. The escaped form keeps the write
    valid and the defect diagnosable: the stored text shows exactly
    where the unencodable codepoint was. ``str.encode("utf-8",
    "backslashreplace")`` is the identity on any string a UTF-8 encoder
    accepts, so clean text binds byte-identically. The in-memory twin
    stores this text verbatim (no encode on that tier) — pinned as the
    intended mirror split in tests/test_rt_payload_surrogate_guards.py.
    """
    return sanitize_surrogates(value)


# ── Shared helpers ─────────────────────────────────────────────────────


async def _insert_state_change_event(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    from_state: str,
    to_state: str,
    error_class: str | None = None,
    worker_id: UUID | None = None,
    extra_detail: dict[str, object] | None = None,
) -> None:
    """INSERT a job_events row with kind='state_change'."""
    detail: dict[str, object] = {
        "from_state": from_state,
        "to_state": to_state,
    }
    if error_class is not None:
        detail["error_class"] = error_class
    if worker_id is not None:
        detail["worker_id"] = str(worker_id)
    if extra_detail is not None:
        detail.update(extra_detail)
    await conn.execute(
        sql.insert_event,
        job_id,
        "state_change",
        jsonb_param(detail),
    )


async def _insert_cancel_request_event(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    reason: str | None,
) -> None:
    """INSERT a job_events row with kind='cancel_request'."""
    detail: dict[str, object] = {}
    if reason is not None:
        detail["reason"] = reason
    await conn.execute(
        sql.insert_event,
        job_id,
        "cancel_request",
        jsonb_param(detail),
    )


async def _select_owner(conn: ConnLike, sql: SqlTemplates, job_id: JobId) -> UUID | None:
    """Fetch ``locked_by_worker`` for a job to populate
    :class:`WorkerOwnershipMismatch.actual`.  Returns ``None`` if
    the row does not exist.
    """
    row = await conn.fetchrow(sql.select_owner, job_id)
    if row is None:
        return None
    return row["locked_by_worker"]


# ── mark_succeeded ─────────────────────────────────────────────────────


async def _mark_succeeded_on_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    fallback_result_ttl: timedelta | None = None,
    max_result_bytes: int = MAX_RESULT_BYTES,
    *,
    result_bytes: bytes | None = None,
    attempt: int | None = None,
) -> bool:
    """Terminal success write: ONE statement (UPDATE + attempt + event).

    The fused ``sql.mark_succeeded`` keeps the pool and LOOP-scope
    transactional paths byte-identical in semantics — see the module
    docstring.  ``None`` here still means the fencing UPDATE matched no
    row (wrong worker, missing job, already moved): nothing was written.

    *attempt* is the attempt-identity epoch — the handler's dispatch-time
    job-row attempt snapshot. The fence is one epoch deeper than the
    worker fence: ``attempt = $8`` must match the row's current attempt,
    so a stale handler's write (a same-worker reclaim/redispatch moved
    the row to a later attempt) no-ops exactly like a different worker's
    late write — Oban's ``ack_query`` contract. ``attempt=None`` — a
    caller that cannot present the epoch — binds NULL, which never
    satisfies the equality: a write that cannot prove which attempt it
    terminates must not terminate any attempt.

    ``result_bytes`` carries the caller's own orjson encoding of *result*
    (the worker consumer serializes exactly once and passes the bytes);
    the dict form serializes here.  The two are mutually exclusive.
    """
    if result is not None and result_bytes is not None:
        raise ValueError(
            "result and result_bytes are mutually exclusive; pass the actor's "
            "result dict (serialized here) or its taskq._json.dumps bytes "
            "(reused as-is), not both"
        )
    serialized_result: str | None
    result_size: int | None
    if result_bytes is not None:
        # Why: the caller (the worker consumer) serialized this exact result
        # once (orjson, the same options ``dumps_jsonb_str`` uses) — reuse
        # the bytes instead of dumping a second time and re-encoding the
        # bound str a third time just to measure result_size_bytes.
        # len(result_bytes) IS the stored byte length: the bound value is
        # result_bytes.decode() and asyncpg encodes text parameters back to
        # the identical utf-8 bytes.  The NUL guard runs on the same bytes
        # via the byte-level scan (_encoded_has_nul), so the ValueError
        # still fires for NUL results — same defect, same boundary, same
        # message as the dict path below.
        if not result_bytes:
            # Empty bytes are never valid orjson output (dumps always emits
            # at least "null"/"{}"); decoded they bind as '' which jsonb
            # rejects with a PostgresError — a permanent data defect the
            # terminal-write classification would read as transient infra
            # failure, stranding the job until the lease sweep reclaims it
            # into the same write forever.  Same ValueError class the
            # in-memory/testing mirrors raise for the same input.
            raise ValueError(
                "result_bytes must be non-empty orjson output (taskq._json.dumps); "
                "pass result for the dict form or omit both for a NULL result"
            )
        if _encoded_has_nul(result_bytes):
            raise ValueError(NUL_JSONB_ERROR)
        # Why parse-and-discard: validation only — the bound value is the
        # decoded str below. decode_result_bytes carries the rejection
        # rationale (the transient-infra misclassification its ValueError
        # prevents) and proves the decode underneath cannot fail.
        decode_result_bytes(result_bytes)
        serialized_result = result_bytes.decode("utf-8")
        result_size = len(result_bytes)
    else:
        serialized_result = jsonb_param(result)
        result_size = (
            len(serialized_result.encode("utf-8")) if serialized_result is not None else None
        )
    if result_size is not None and result_size > max_result_bytes:
        raise ResultTooLarge(f"result size {result_size} bytes exceeds {max_result_bytes} byte cap")
    rec = await conn.fetchrow(
        sql.mark_succeeded,
        job_id,
        worker_id,
        serialized_result,
        result_size,
        progress_seq,
        _progress_jsonb_escaped(progress_state),
        fallback_result_ttl,
        attempt,
    )
    if rec is None:
        return False

    log_state_change(
        logger,
        from_state="running",
        to_state="succeeded",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=rec["attempt"],
    )
    return True


async def _mark_succeeded(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    fallback_result_ttl: timedelta | None = None,
    max_result_bytes: int = MAX_RESULT_BYTES,
    *,
    result_bytes: bytes | None = None,
    attempt: int | None = None,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> bool:
    # No explicit transaction: the fused statement is atomic by itself (a
    # single SQL statement — CTEs included — either completes entirely or
    # not at all), and dropping the BEGIN/COMMIT pair removes two of the
    # four remaining per-job round trips.  It also shortens the fencing
    # row lock's hold from UPDATE..COMMIT to the statement's own duration.
    # The LOOP-scope path (_mark_succeeded_on_conn via
    # mark_succeeded_with_conn) still runs inside the actor's transaction
    # exactly as before.
    async with pool.acquire(timeout=acquire_timeout) as conn:
        return await _mark_succeeded_on_conn(
            conn,
            sql,
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            fallback_result_ttl,
            max_result_bytes,
            result_bytes=result_bytes,
            attempt=attempt,
        )


# ── mark_failed_or_retry ──────────────────────────────────────────────


async def _mark_failed_or_retry(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    retry_delay: timedelta | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    attempt: int | None = None,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> JobRow:
    if retry_delay is None:
        return await _mark_failed(
            pool,
            sql,
            job_id,
            worker_id,
            error_info,
            progress_seq,
            progress_state,
            attempt=attempt,
            acquire_timeout=acquire_timeout,
        )
    return await _mark_retry(
        pool,
        sql,
        job_id,
        worker_id,
        error_info,
        retry_delay,
        progress_seq,
        progress_state,
        attempt=attempt,
        acquire_timeout=acquire_timeout,
    )


async def _mark_failed(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    progress_seq: int,
    progress_state: dict[str, object] | None,
    *,
    attempt: int | None = None,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> JobRow:
    # No explicit transaction — see _mark_succeeded.  The rare
    # WorkerOwnershipMismatch diagnostic (_select_owner) runs after the
    # no-op statement on the same connection: both the fused statement and
    # the owner read are single-statement implicit transactions, and the
    # write could not have modified the row when it matched nothing.
    # The error text is escaped at the bind (_error_text_escaped): the
    # message/traceback are derived from an uncontrolled exception, and a
    # lone surrogate in them must fail the job with the defect visible,
    # not strand it running through the reclaim loop on asyncpg's
    # DataError-as-infra misclassification.
    async with pool.acquire(timeout=acquire_timeout) as conn:
        rec = await conn.fetchrow(
            sql.mark_failed,
            job_id,
            worker_id,
            error_info.error_class,
            _error_text_escaped(error_info.error_message),
            _error_text_escaped(error_info.error_traceback),
            progress_seq,
            _progress_jsonb_escaped(progress_state),
            attempt,
        )
        if rec is None:
            actual = await _select_owner(conn, sql, job_id)
            raise WorkerOwnershipMismatch(job_id, worker_id, actual)

        row = _job_row_from_record(rec)

    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=row.attempt,
    )
    return row


async def _mark_retry(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    retry_delay: timedelta,
    progress_seq: int,
    progress_state: dict[str, object] | None,
    *,
    attempt: int | None = None,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> JobRow:
    branch: str
    async with pool.acquire(timeout=acquire_timeout) as conn:
        # Error text escaped at the bind for the same reason as
        # _mark_failed; the retry_delay is floored at
        # MIN_DEFERRAL_INTERVAL inside the statement (params CTE's
        # GREATEST) — the template-side twin of the in-memory
        # _mark_failed_or_retry floor.
        rec = await conn.fetchrow(
            sql.mark_retry,
            job_id,
            worker_id,
            retry_delay,
            error_info.error_class,
            _error_text_escaped(error_info.error_message),
            _error_text_escaped(error_info.error_traceback),
            progress_seq,
            _progress_jsonb_escaped(progress_state),
            attempt,
        )
        if rec is None:
            actual = await _select_owner(conn, sql, job_id)
            raise WorkerOwnershipMismatch(job_id, worker_id, actual)

        branch = rec["outcome_branch"]
        row = _job_row_from_record(rec)
        # The attempt row (duration_ms computed from the winning arm's
        # own timestamps) and the state_change event are written by the
        # fused statement's per-arm CTEs — see _sql_templates.mark_retry.

    if branch == "retried":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=row.attempt,
        )
        return row
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=row.attempt,
        reason="schedule_to_close",
    )
    return row


# ── mark_cancelled ─────────────────────────────────────────────────────


async def _mark_cancelled(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    attempt: int | None = None,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> bool:
    async with pool.acquire(timeout=acquire_timeout) as conn:
        rec = await conn.fetchrow(
            sql.mark_cancelled,
            job_id,
            worker_id,
            progress_seq,
            _progress_jsonb_escaped(progress_state),
            attempt,
        )
        if rec is None:
            return False

    log_state_change(
        logger,
        from_state="running",
        to_state="cancelled",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=rec["attempt"],
    )
    return True


# ── write_cancel_escalation ────────────────────────────────────────────


async def _write_cancel_escalation(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    phase: Literal[2],
    *,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> bool:
    if phase != 2:
        raise ValueError(
            "write_cancel_escalation only accepts phase=2; use write_cancel_request for phase=1"
        )

    async with pool.acquire(timeout=acquire_timeout) as conn:
        async with conn.transaction():
            tag = await conn.execute(sql.cancel_escalation, job_id, worker_id)
            if parse_rowcount(tag) != 1:
                return False
            await _insert_state_change_event(
                conn,
                sql,
                job_id,
                "running",
                "running",
                worker_id=worker_id,
                extra_detail={
                    "cancel_phase_from": 1,
                    "cancel_phase_to": 2,
                },
            )

    log_cancel_phase_change(
        logger,
        from_phase=1,
        to_phase=2,
        job_id=str(job_id),
        worker_id=str(worker_id),
    )
    return True


# ── mark_abandoned ─────────────────────────────────────────────────────


async def _mark_abandoned(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> bool:
    async with pool.acquire(timeout=acquire_timeout) as conn:
        rec = await conn.fetchrow(
            sql.mark_abandoned,
            job_id,
            progress_seq,
            jsonb_param(progress_state),
        )
        if rec is None:
            return False

        locked_by_worker: UUID | None = rec["locked_by_worker"]
        # The statement's NULL-lease defense-in-depth arm (see
        # mark_abandoned's comment in _sql_templates.py): the applied row
        # still carries its NULL lock (the abandon clears neither the
        # holder nor the lease), so a NULL here means that arm is the one
        # that fired. The row was unholdable by construction — warn on the
        # shape so a fleet that keeps producing it (a rogue direct-SQL
        # writer, a restored backup with NULLed leases) is visible instead
        # of silently absorbing corruptions one abandon at a time; the
        # stranded-jobs detector's per-shape event convention.
        null_lease_arm: bool = rec["lock_expires_at"] is None

    if null_lease_arm:
        logger.warning(
            "abandoned-null-lock-running-row",
            kind="null_lock_running_row_abandoned",
            job_id=str(job_id),
            worker_id=str(locked_by_worker) if locked_by_worker is not None else None,
        )
    log_state_change(
        logger,
        from_state="running",
        to_state="abandoned",
        job_id=str(job_id),
        worker_id=str(locked_by_worker) if locked_by_worker is not None else None,
        attempt=rec["attempt"],
    )
    return True


# ── mark_snoozed ───────────────────────────────────────────────────────


async def _mark_snoozed(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    metadata_update: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    outcome: SnoozeOutcome = "snoozed",
    *,
    attempt: int | None = None,
    denial_reason: DenialReason = "capacity",
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> Literal["scheduled", "failed", "failed:MaxAttemptsExceeded", "noop"]:
    # The statement's arms key on exactly the three SnoozeOutcome values;
    # PG cannot reject an unknown bind value inside the statement itself,
    # so this boundary owns the check (the in-memory twin raises the
    # identical error) — before the pool is even touched, so an illegal
    # outcome raises loudly whatever the job's state instead of firing
    # no arm and stranding the row 'running'. denial_reason gets the
    # same boundary check for the same reason: an illegal reason must
    # not silently fall into one arm's budget semantics.
    validate_snooze_outcome(outcome)
    validate_denial_reason(denial_reason)
    branch: str
    async with pool.acquire(timeout=acquire_timeout) as conn:
        rec = await conn.fetchrow(
            sql.mark_snoozed,
            job_id,
            worker_id,
            delay,
            jsonb_param(metadata_update),
            progress_seq,
            _progress_jsonb_escaped(progress_state),
            outcome,
            attempt,
            denial_reason,
        )
        if rec is None:
            return "noop"

        branch = rec["outcome_branch"]
        # A non-terminal snooze/denial writes no attempt/event rows and no
        # timestamps of its own — it increments the outcome-keyed counter
        # on the row (see _sql_templates.mark_snoozed).  The terminal
        # max_attempts arm writes its attempt row and state_change event
        # exactly like every other terminal transition.

    if branch == "snoozed":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=rec["attempt"],
        )
        return "scheduled"
    if branch == "max_attempts_failed":
        log_state_change(
            logger,
            from_state="running",
            to_state="failed",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=rec["attempt"],
            cause="max_attempts",
        )
        return "failed:MaxAttemptsExceeded"
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=rec["attempt"],
    )
    return "failed"


# ── mark_retry_after ───────────────────────────────────────────────────


async def _mark_retry_after(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    consume_budget: bool = True,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    attempt: int | None = None,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
    branch: str
    async with pool.acquire(timeout=acquire_timeout) as conn:
        sql_stmt = (
            sql.mark_retry_after_consume_true
            if consume_budget
            else sql.mark_retry_after_consume_false
        )
        rec = await conn.fetchrow(
            sql_stmt,
            job_id,
            worker_id,
            delay,
            progress_seq,
            _progress_jsonb_escaped(progress_state),
            attempt,
        )
        if rec is None:
            return "noop"

        branch = rec["outcome_branch"]
        # Why row_attempt, not attempt: the caller's ``attempt`` hint (the
        # SQL bind above) is a different value from the row's own attempt
        # the logs must report — a same-named local would obscure the
        # parameter (pyright reportRedeclaration) and invite a future edit
        # to bind the wrong one.
        row_attempt: int = rec["attempt"]
        # The attempt row and state_change event are written by the
        # fused statement's per-arm CTEs (attempt outcome/error fields
        # and the snoozed arms' now_ts-based duration included) — see
        # _sql_templates.mark_retry_after_consume_*.

    if branch == "snoozed":
        log_state_change(
            logger,
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
            worker_id=str(worker_id),
            attempt=row_attempt,
            cause="retry_after",
        )
        return "scheduled"
    log_state_change(
        logger,
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
        worker_id=str(worker_id),
        attempt=row_attempt,
        cause="retry_after",
    )
    if branch == "max_attempts_failed":
        return "failed:MaxAttemptsExceeded"
    return "failed:DeadlineExceeded"


# ── write_attempt ──────────────────────────────────────────────────────


async def _write_attempt(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    attempt: AttemptRow,
    *,
    acquire_timeout: float = DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S,
) -> None:
    async with pool.acquire(timeout=acquire_timeout) as conn:
        async with conn.transaction():
            await conn.execute(
                sql.insert_attempt_explicit,
                attempt.job_id,
                attempt.attempt,
                attempt.started_at,
                attempt.finished_at,
                attempt.outcome,
                attempt.error_class,
                attempt.error_message,
                attempt.error_traceback,
                attempt.duration_ms,
                attempt.worker_id,
                jsonb_param(attempt.metadata),
            )
