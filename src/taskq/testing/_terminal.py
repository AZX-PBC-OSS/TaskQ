"""Terminal-write operations for InMemoryBackend.

All ``mark_*`` methods and ``write_attempt`` live here as module-level
functions taking ``self: InMemoryBackend`` as the first parameter,
following the :mod:`taskq.testing._runner` pattern.
"""

from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import structlog

# Why: private import, the pre-serialized result path holds bytes, not a
# dict, so the byte-level scan is the only way to run dumps_jsonb_str's NUL
# guard without a second serialization (same justification as
# backend/_terminal.py, mirrored here so both backends fail identically).
from taskq._json import (
    NUL_JSONB_ERROR,
    _encoded_has_nul,  # pyright: ignore[reportPrivateUsage]
    decode_result_bytes,
    dumps_jsonb_str,
    loads,
    sanitize_surrogates,
)
from taskq._json import dumps as _json_dumps
from taskq.backend._protocol import (
    AttemptRow,
    CancelPhase,
    DenialReason,
    ErrorInfo,
    JobId,
    JobRow,
    SnoozeOutcome,
    validate_denial_reason,
    validate_snooze_outcome,
)

# Why: the twin's deadline arms must stamp the exact failure text the SQL
# arms stamp (the differential corpus pins the two sides equal), so the
# message is read from the shared constant the templates interpolate, never
# restated here. The constants live in the driver-free _sql_fragments module
# (no backend imports): this file is part of the driver-free testing surface.
from taskq.backend._sql_fragments import (
    DEADLINE_EXCEEDED_MESSAGE,
    DEADLINE_RETRY_EXCEEDED_MESSAGE,
)
from taskq.constants import (
    CANCEL_ORIGIN_ABANDONED,
    CANCEL_ORIGIN_COOPERATIVE,
    CANCEL_ORIGIN_FORCED,
    CANCEL_ORIGIN_UNREQUESTED,
    MIN_DEFERRAL_INTERVAL,
)
from taskq.exceptions import (
    ResultTooLarge,
    UnencodableValue,
    WorkerOwnershipMismatch,
)
from taskq.obs import record_job_abandoned
from taskq.testing._reads import _read_copy

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_mark_abandoned",
    "_mark_cancelled",
    "_mark_failed_or_retry",
    "_mark_interrupted",
    "_mark_retry_after",
    "_mark_snoozed",
    "_mark_succeeded",
    "_mark_succeeded_with_conn",
    "_merge_progress",
    "_retry_job",
    "_write_attempt",
    "_write_cancel_escalation",
]

logger: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.testing.in_memory")


def _merge_progress(
    current: dict[str, object],
    update: dict[str, object] | None,
) -> dict[str, object]:
    """Mirror PG ``COALESCE(progress_state,'{}') || new`` for terminal writes.

    The merge result is round-tripped through the same guarded
    serialization PG binds (both sides of the jsonb concat are
    ``dumps_jsonb_str`` output), so values whose encoding differs from
    the Python object (NaN/Infinity → null, UUID → string, tuple →
    array) read back exactly as PG reads them, and a NUL in the update
    raises the same ``ValueError`` PG's bind raises instead of being
    stored; the round-trip is idempotent for already-stored JSON-native
    state.

    The progress state reaching a terminal write is ACTOR-DERIVED, the
    coalesced buffer the actor filled, not caller input, so a value no
    UTF-8 encoder accepts (a lone surrogate published through a
    settings-less context, below the publish guard's wiring) is escaped
    here instead of refused: the write lands with the defect visible
    (:func:`~taskq._json.sanitize_surrogates`), never stranding the job
    ``running`` in the reclaim loop. The escape runs only on the cold
    path the round-trip already rejected, and a structural refusal
    (over-deep nesting) still stands, escaping cannot repair it.
    """
    if update is not None:
        return _round_trip_progress_state(current | update)
    return _round_trip_progress_state(current)


def _round_trip_progress_state(state: dict[str, object]) -> dict[str, object]:
    try:
        return loads(dumps_jsonb_str(state))
    except UnencodableValue as exc:
        try:
            return loads(dumps_jsonb_str(sanitize_surrogates(state)))
        except RecursionError:
            # from None: the walk's stack exhaustion is an artifact of the
            # repair attempt, not the refusal's cause, the original
            # UnencodableValue (with orjson's own reason chained) is the
            # truthful failure.
            raise exc from None


def _fenced(
    row: JobRow,
    worker_id: UUID,
    attempt: int | None,
    claim_epoch: int | None,
) -> bool:
    """True when *row* fails the terminal-write fence: the SQL arms'
    ``status = 'running' AND locked_by_worker = ... AND attempt = ...
    AND claim_epoch = ...`` conjuncts (the ``JOB_FENCE_SQL`` fragment in
    backend/_sql_fragments.py, the bound spelling's ``JOB_FENCE_BOUND_SQL``
    single-row form).

    One helper, not a hand-restated predicate per method: the fence is the
    invariant every terminal write leans on, a conjunct edited in one twin
    method but not its siblings would let a stale handler's write land
    exactly where the SQL fence refuses it. ``attempt is None`` or
    ``claim_epoch is None`` (a caller that cannot present the epochs) never
    matches, mirroring PG's NULL binds never satisfying the equality. The
    claim epoch rides beside the attempt because at the attempt ceiling,
    where the displayed counter stops advancing, the non-saturating claim
    epoch is what keeps a stale handler's write off the re-dispatched
    attempt.
    """
    return (
        row.status != "running"
        or row.locked_by_worker != worker_id
        or row.attempt != attempt
        or row.claim_epoch != claim_epoch
    )


async def _mark_succeeded(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    fallback_result_ttl: timedelta | None = None,
    *,
    result_bytes: bytes | None = None,
    attempt: int | None = None,
    claim_epoch: int | None = None,
) -> bool:
    # Caller-input validation precedes the state fence, matching the PG
    # terminal's order (its guards run before the fencing UPDATE): a
    # misuse or a permanently-unstorable value raises loudly whatever the
    # job's state, and only a value PG would accept reaches the fence ,
    # where a missing or mismatched job still returns False, exactly as
    # PG's fencing UPDATE matching no row returns False.
    if result is not None and result_bytes is not None:
        raise ValueError(
            "result and result_bytes are mutually exclusive; pass the actor's "
            "result dict (serialized here) or its taskq._json.dumps bytes "
            "(reused as-is), not both"
        )
    # Both result forms normalize to the same observable state as PG: the
    # stored result never reaches storage by reference (PG serializes into
    # jsonb at write time) and result_size_bytes is the exact byte length
    # of what PG would store.  The bytes form, what the worker consumer
    # passes, reuses the caller's serialization as-is (dict via a decode
    # round-trip, the same orjson bytes PG would bind); the dict form
    # serializes exactly once here and stores the round-trip of those
    # same bytes, so values whose orjson encoding differs from the Python
    # object (NaN/Infinity → null, UUID → string, tuple → array) read
    # back exactly as PG's jsonb column reads them back.
    stored_result: dict[str, object] | None
    result_size_bytes: int | None
    if result_bytes is not None:
        if not result_bytes:
            # Mirror the PG backend: empty bytes are never valid orjson
            # output and would bind as '' (invalid jsonb), raise the same
            # ValueError here so the testing backend is observable-equivalent.
            raise ValueError(
                "result_bytes must be non-empty orjson output (taskq._json.dumps); "
                "pass result for the dict form or omit both for a NULL result"
            )
        if _encoded_has_nul(result_bytes):
            raise ValueError(NUL_JSONB_ERROR)
        stored_result = decode_result_bytes(result_bytes)
        result_size_bytes = len(result_bytes)
    elif result is not None:
        data = _json_dumps(result)
        if _encoded_has_nul(data):
            raise ValueError(NUL_JSONB_ERROR)
        stored_result = loads(data)
        result_size_bytes = len(data)
    else:
        stored_result = None
        result_size_bytes = None
    max_result_bytes = self._result_max_bytes
    if result_size_bytes is not None and result_size_bytes > max_result_bytes:
        raise ResultTooLarge(
            f"result size {result_size_bytes} bytes exceeds {max_result_bytes} byte cap"
        )

    row = self._jobs.get(job_id)
    # The fence folds the missing row into the same check (_fenced's
    # docstring carries the conjuncts' contract, the attempt-epoch
    # mirror included).
    if row is None or _fenced(row, worker_id, attempt, claim_epoch):
        return False
    now = self._clock.now()
    # Mirror the PG COALESCE: stored (operator-owned) result_ttl applied at
    # completion; then the worker-supplied fallback (the @actor literal),
    # also at completion; then the enqueue-time value.
    new_result_expires_at = row.result_expires_at
    actor_cfg = self._actor_configs_meta.get(row.actor)
    if actor_cfg is not None and actor_cfg.result_ttl is not None:
        new_result_expires_at = now + timedelta(seconds=actor_cfg.result_ttl)
    elif fallback_result_ttl is not None:
        new_result_expires_at = now + fallback_result_ttl
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status="succeeded",
        finished_at=now,
        # The terminal success write clears the lock holder and lease exactly
        # like PG's mark_succeeded SET clause (_sql_templates.py: locked_by_worker
        # = NULL, lock_expires_at = NULL): a succeeded row must not keep
        # matching every locked_by_worker-scoped reader.
        locked_by_worker=None,
        lock_expires_at=None,
        result=stored_result,
        result_size_bytes=result_size_bytes,
        result_expires_at=new_result_expires_at,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="succeeded",
        error_class=None,
        error_message=None,
        error_traceback=None,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="succeeded",
        now=now,
        worker_id=worker_id,
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="succeeded",
        job_id=str(job_id),
    )
    return True


async def _mark_succeeded_with_conn(
    self: "InMemoryBackend",
    conn: object,
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    fallback_result_ttl: timedelta | None = None,
    *,
    result_bytes: bytes | None = None,
    attempt: int | None = None,
    claim_epoch: int | None = None,
) -> bool:
    return await _mark_succeeded(
        self,
        job_id,
        worker_id,
        result,
        progress_seq,
        progress_state,
        fallback_result_ttl,
        result_bytes=result_bytes,
        attempt=attempt,
        claim_epoch=claim_epoch,
    )


async def _mark_failed_or_retry(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    retry_delay: timedelta | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    attempt: int | None = None,
    claim_epoch: int | None = None,
) -> JobRow:
    row = self._jobs.get(job_id)
    if row is None:
        # PG's fencing UPDATE matches nothing for a job id that was never
        # stored, and the pool wrapper raises WorkerOwnershipMismatch with
        # actual=None (backend/_terminal.py: _select_owner reads no row) ,
        # the same typed error the wrong-worker and terminal-row cases
        # raise, never a KeyError.
        raise WorkerOwnershipMismatch(job_id, worker_id, None)

    # The attempt-epoch conjunct mirrors the PG fence's
    # ``AND j.attempt = (SELECT attempt FROM params)``: a mismatched
    # epoch raises the same WorkerOwnershipMismatch the PG rowcount-0
    # path raises (safe_mark_failed_or_retry converts it to the None the
    # handlers treat as a no-op), and ``attempt=None``, a caller that
    # cannot present the epoch, never matches, exactly as PG's NULL
    # bind never satisfies the equality.
    if row.status != "running":
        raise WorkerOwnershipMismatch(job_id, worker_id, row.locked_by_worker)

    if (
        row.locked_by_worker != worker_id
        or row.attempt != attempt
        or row.claim_epoch != claim_epoch
    ):
        raise WorkerOwnershipMismatch(job_id, worker_id, row.locked_by_worker)

    if retry_delay is not None:
        # The cancel fence (the SQL retried and deadline_failed arms'
        # `cancel_phase = 0` conjunct, the sibling deferral arms'
        # semantics): an operator cancel in flight WINS over the failure
        # retry. Both arms of the retry below reset or overwrite state a
        # cancel in flight owns (the retried arm resets the cancel
        # columns; the deadline arm stamps DeadlineExceeded), so a
        # phase-carrying row must never reach either: the reset would
        # launder the operator's request mid-flight and the job would
        # run again. The fenced-out row stays 'running' carrying its
        # phase, the caller reads back the same WorkerOwnershipMismatch
        # a wrong-worker retry raises (the handler's no-op), and the
        # worker's cancel ladder terminalises it. On a clean row the
        # check is trivially false and the retry semantics are
        # unchanged. The TERMINAL fail path below is deliberately NOT
        # fenced: it writes no re-pend and keeps the cancel columns as
        # the audit trail, the doctrine every terminal cancel path
        # carries.
        if row.cancel_phase != CancelPhase.NONE:
            raise WorkerOwnershipMismatch(job_id, worker_id, row.locked_by_worker)

        now = self._clock.now()
        # The failure-retry arm's deferral floor, the same bound
        # mark_snoozed and the non-consuming retry-after arm apply, and the
        # same bound PG's mark_retry template pins in its params CTE
        # (``GREATEST($3::interval, MIN_DEFERRAL_INTERVAL) AS
        # effective_delay``, _sql_templates.py): a zero/now delay would
        # park the job at the head of the dispatch order and make it
        # instantly re-claimable, one claim/refund round trip per cycle
        # monopolising a worker slot. The floored delay is the SINGLE
        # effective delay: the status branch and the deadline comparison
        # below both use it, the conservative direction, mirroring the SQL
        # arm's GREATEST. (The decision layer floors too, retry.py's
        # RetryClassifier, the same two-layer defense-in-depth shape the
        # deferral family ships.)
        effective_delay = max(retry_delay, MIN_DEFERRAL_INTERVAL)
        new_scheduled = now + effective_delay
        # Mirror the PG mark_retry deadline CTE: the backend's own clock is
        # the single arbiter, a retry that would land past
        # schedule_to_close fails with DeadlineExceeded instead.
        if row.schedule_to_close is not None and new_scheduled > row.schedule_to_close:
            merged_progress = _merge_progress(row.progress_state, progress_state)
            updated = replace(
                row,
                status="failed",
                finished_at=now,
                locked_by_worker=None,
                lock_expires_at=None,
                last_heartbeat_at=None,
                error_class="DeadlineExceeded",
                error_message=DEADLINE_RETRY_EXCEEDED_MESSAGE,
                progress_seq=max(row.progress_seq, progress_seq),
                progress_state=merged_progress,
            )
            self._jobs[job_id] = updated
            self._append_attempt(
                job_id=job_id,
                attempt=row.attempt,
                started_at=row.started_at,
                now=now,
                outcome="failed",
                error_class="DeadlineExceeded",
                error_message=DEADLINE_RETRY_EXCEEDED_MESSAGE,
                error_traceback=None,
                worker_id=worker_id,
            )
            self._append_state_change_event(
                job_id=job_id,
                from_state="running",
                to_state="failed",
                now=now,
                error_class="DeadlineExceeded",
                worker_id=worker_id,
            )
            logger.debug(
                "state-change",
                kind="state_change",
                from_state="running",
                to_state="failed",
                job_id=str(job_id),
            )
            return _read_copy(updated)

        retry_status: Literal["scheduled", "pending"] = (
            "scheduled" if effective_delay > timedelta(0) else "pending"
        )
        merged_progress = _merge_progress(row.progress_state, progress_state)
        updated = replace(
            row,
            status=retry_status,
            scheduled_at=new_scheduled,
            finished_at=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            error_class=error_info.error_class,
            error_message=error_info.error_message,
            error_traceback=error_info.error_traceback,
            cancel_phase=CancelPhase.NONE,
            cancel_requested_at=None,
            # A failure retry returns the row to the pending pool, so it
            # routes by the actor's current assignment from here on.
            assignment_routed=True,
            progress_seq=max(row.progress_seq, progress_seq),
            progress_state=merged_progress,
        )
        self._jobs[job_id] = updated
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class=error_info.error_class,
            error_message=error_info.error_message,
            error_traceback=error_info.error_traceback,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="scheduled",
            now=now,
            error_class=error_info.error_class,
            worker_id=worker_id,
        )
        logger.debug(
            "state-change",
            kind="state_change",
            from_state="running",
            to_state="scheduled",
            job_id=str(job_id),
        )
        return _read_copy(updated)

    now = self._clock.now()
    merged_progress = _merge_progress(row.progress_state, progress_state)
    updated = replace(
        row,
        status="failed",
        finished_at=now,
        locked_by_worker=None,
        lock_expires_at=None,
        error_class=error_info.error_class,
        error_message=error_info.error_message,
        error_traceback=error_info.error_traceback,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    self._jobs[job_id] = updated
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="failed",
        error_class=error_info.error_class,
        error_message=error_info.error_message,
        error_traceback=error_info.error_traceback,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="failed",
        now=now,
        error_class=error_info.error_class,
        worker_id=worker_id,
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="failed",
        job_id=str(job_id),
    )
    return _read_copy(updated)


async def _mark_cancelled(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    attempt: int | None = None,
    claim_epoch: int | None = None,
) -> bool:
    row = self._jobs.get(job_id)
    if row is None:
        return False
    # Attempt-epoch fence, one predicate shared with the sibling writes
    # (_fenced's docstring).
    if _fenced(row, worker_id, attempt, claim_epoch):
        return False

    now = self._clock.now()
    merged_progress = _merge_progress(row.progress_state, progress_state)
    # Twin of the PG template's CASE: an actor stopped while still only
    # asked (phase 1, a request on the row) stopped cooperatively; one
    # interrupted at phase 2 was forced; and a row carrying neither
    # evidence (phase 0, no request) was cancelled by the worker's
    # runtime itself, a sibling crash or an actor self cancel, so the
    # cooperative marker would forge a request that never existed. One
    # local feeds the row, the attempt and the event so the three writes
    # can never disagree (upd.error_class in the SQL).
    origin = (
        CANCEL_ORIGIN_FORCED
        if row.cancel_phase == CancelPhase.FORCED
        else CANCEL_ORIGIN_COOPERATIVE
        if row.cancel_requested_at is not None
        else CANCEL_ORIGIN_UNREQUESTED
    )
    self._jobs[job_id] = replace(
        row,
        status="cancelled",
        finished_at=now,
        locked_by_worker=None,
        lock_expires_at=None,
        error_class=origin,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="cancelled",
        error_class=origin,
        error_message=None,
        error_traceback=None,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="cancelled",
        now=now,
        error_class=origin,
        worker_id=worker_id,
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="cancelled",
        job_id=str(job_id),
    )
    return True


async def _write_cancel_escalation(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    phase: Literal[2],
) -> bool:

    if phase != 2:
        raise ValueError(
            "write_cancel_escalation only accepts phase=2; "
            "phase=1 is written by write_cancel_request"
        )

    row = self._jobs.get(job_id)
    if row is None:
        return False
    if row.status != "running" or row.locked_by_worker != worker_id:
        return False
    if row.cancel_phase != CancelPhase.COOPERATIVE:
        return False

    now = self._clock.now()
    self._jobs[job_id] = replace(row, cancel_phase=CancelPhase.FORCED)
    # worker_id rides the escalation event's detail exactly as PG's
    # _write_cancel_escalation does (backend/_terminal.py passes
    # worker_id into _insert_state_change_event): the running→running
    # phase-change event names the holder that escalated.
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="running",
        now=now,
        worker_id=worker_id,
        cancel_phase_from=CancelPhase.COOPERATIVE,
        cancel_phase_to=CancelPhase.FORCED,
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="running",
        job_id=str(job_id),
        cancel_phase=CancelPhase.FORCED,
    )
    return True


async def _mark_abandoned(
    self: "InMemoryBackend",
    job_id: JobId,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:

    row = self._jobs.get(job_id)
    if row is None:
        return False
    if row.status != "running" or row.cancel_phase != CancelPhase.FORCED:
        return False

    now = self._clock.now()
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status="abandoned",
        finished_at=now,
        # Twin of the PG template: the abandon stamps its own origin, so
        # the row reads "taken away after the graces", distinct from a
        # cooperative cancel, see _sql_templates.mark_abandoned.
        error_class=CANCEL_ORIGIN_ABANDONED,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="cancelled",
        error_class=CANCEL_ORIGIN_ABANDONED,
        error_message=None,
        error_traceback=None,
        worker_id=row.locked_by_worker,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="abandoned",
        now=now,
        error_class=CANCEL_ORIGIN_ABANDONED,
        worker_id=row.locked_by_worker,
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="abandoned",
        job_id=str(job_id),
    )
    record_job_abandoned(row.actor)
    return True


async def _retry_job(self: "InMemoryBackend", job_id: JobId) -> bool:
    row = self._jobs.get(job_id)
    # An operator re-run is "run this again", so every state a job can
    # come to rest in is a valid source, including 'succeeded' (the
    # replay path after a bad deploy) and 'abandoned' (a deploy
    # interrupted the job; it did not fail). 'running' is the one
    # exclusion, and it is a correctness constraint rather than a
    # policy choice: re-pending a row while an attempt is live races
    # that attempt's terminal write and the job can execute twice
    # concurrently. 'pending'/'scheduled' are excluded because the job
    # is already queued, there is nothing to put back, and re-pending
    # would discard its place in the dispatch order.
    if row is None or row.status in ("running", "pending", "scheduled"):
        return False
    # Monotonic attempt with the ceiling raised just enough to open
    # the budget gates, mirroring the PG statement's
    # LEAST(GREATEST(max_attempts, attempt + 1), 32767): the attempt
    # counter never resets across retries. A re-run climbs to fresh
    # attempt numbers, the twin's dispatch claim stamps attempt + 1 ,
    # so no attempt-row writer can revisit a spent epoch's key. At the
    # smallint bound the ceiling cannot rise further and the retry
    # is refused, the row stays terminal, rather than re-pending
    # a job the next claim could only overflow.
    raised_ceiling = min(max(row.max_attempts, row.attempt + 1), 32767)
    if raised_ceiling <= row.attempt:
        return False
    now = self._clock.now()
    self._jobs[job_id] = replace(
        row,
        status="pending",
        max_attempts=raised_ceiling,
        # An operator hand-back routes by the actor's current
        # assignment, not by the label the row was first placed
        # under, including for a row terminalized before it was
        # ever claimed.
        assignment_routed=True,
        cancel_phase=CancelPhase.NONE,
        # The whole cancel trail goes with the spent epoch, mirroring
        # the PG SET clause's cancel_requested_at = NULL: the TERMINAL
        # writes deliberately keep the cancel columns as the audit
        # trail of why the job ended, and a re-run must not inherit
        # that trail, the next attempt's cancel protocol starts at
        # phase 0 with no request stamp.
        cancel_requested_at=None,
        error_class=None,
        error_message=None,
        error_traceback=None,
        scheduled_at=now,
        # Twin of the PG SET clause's CASE: an already-elapsed
        # schedule_to_close is a spent epoch's artifact, the twin's own
        # dispatch claim (_dispatch.py) admits a row only when its
        # deadline is NULL or strictly in the future, so re-pending with
        # the stale deadline intact hands back a row no claim can ever
        # reach and the next deadline-sweep tick silently re-fails it.
        # Only an elapsed deadline is cleared; a still-future one is the
        # operator's original budget intent and survives the retry.
        schedule_to_close=(
            None
            if row.schedule_to_close is not None and row.schedule_to_close <= now
            else row.schedule_to_close
        ),
        finished_at=None,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
    )
    # Batch-status reconciliation, the twin of the PG statement's
    # reopened CTE (_sql_templates.py retry_job): a re-pended member
    # makes a terminal batch row's claim a lie, and every batch-status
    # writer guards on 'active', so the reopen happens here, in the
    # same store mutation as the re-pend. metadata.batch_id marks
    # membership only (the finalizer is never stamped), and the guard
    # on the terminal statuses keeps it idempotent.
    raw_bid = row.metadata.get("batch_id")
    if raw_bid is not None:
        batch_row = self._batches.get(UUID(str(raw_bid)))
        if batch_row is not None and batch_row.status in ("complete", "aborted"):
            self._batches[UUID(str(raw_bid))] = replace(
                batch_row, status="active", completed_at=None
            )
    for event in self._wake_subscribers:
        event.set()
    return True


async def _mark_snoozed(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    *,
    metadata_update: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    outcome: SnoozeOutcome = "snoozed",
    attempt: int | None = None,
    claim_epoch: int | None = None,
    denial_reason: DenialReason = "capacity",
) -> Literal["scheduled", "failed", "noop"]:
    # Caller-input validation precedes the state fence, matching the PG
    # terminal's order (its guard runs before the fencing UPDATE): an
    # illegal outcome raises loudly whatever the job's state, the same
    # ValueError, from the same shared validator, the PG boundary
    # raises, never silently degrading to the "noop" a fenced-out
    # write would return. denial_reason keeps the same boundary check
    # even though no arm branches on it, a caller naming a reason the
    # protocol does not define is a coding error the API must refuse
    # rather than silently accept.
    validate_snooze_outcome(outcome)
    validate_denial_reason(denial_reason)
    row = self._jobs.get(job_id)
    # The fence folds the missing row into the same check; a fenced-out
    # epoch returns "noop" through the same machinery as the worker fence.
    if row is None or _fenced(row, worker_id, attempt, claim_epoch):
        return "noop"

    now = self._clock.now()
    # The non-consuming deferral floor: a zero/now delay would park the
    # job 'pending' at the head of the dispatch order and make it
    # instantly re-claimable, one claim/refund round trip per cycle
    # monopolising a worker slot. A positive delay is required to prevent
    # this thrashing. The floored delay is the SINGLE effective delay: the
    # deadline comparison below uses it too, the conservative direction,
    # mirroring the SQL arms' GREATEST().
    effective_delay = max(delay, MIN_DEFERRAL_INTERVAL)
    new_scheduled_at = now + effective_delay

    # Two arms only, deadline → snooze here against the fused SQL's
    # snoozed → deadline: observably equivalent, since the SQL snooze
    # arm's own deadline guard excludes every past-deadline row before
    # the deadline arm sees it. The deadline is a deferral's ONLY
    # terminal exit for a clean row, nothing about admission decides a
    # job's outcome.
    if row.schedule_to_close is not None and new_scheduled_at > row.schedule_to_close:
        deadline_merged_progress = _merge_progress(row.progress_state, progress_state)
        # Cancel-first arbitration inside the deadline arm, the twin of
        # the SQL deadline_cancelled arm (and of _SWEEP_1_SQL's CASE
        # ordering): operator intent outranks the deadline, so a
        # phase-carrying row whose schedule_to_close lapsed at deferral
        # time terminalises 'cancelled', never 'failed:DeadlineExceeded'
        # (which would fire DeadlineExceeded hooks and error reports on
        # a cancel in flight). The write mirrors _mark_cancelled: the
        # cancel-origin marker on the row and attempt, the cancel
        # columns preserved as the audit trail, no deadline stamp. The
        # row is already terminal, so the caller reads back "noop", the
        # same contract the fence below returns (the deferral did not
        # land; the operator's cancel decided the row).
        if row.cancel_phase != CancelPhase.NONE:
            origin = (
                CANCEL_ORIGIN_FORCED
                if row.cancel_phase == CancelPhase.FORCED
                else CANCEL_ORIGIN_COOPERATIVE
            )
            self._jobs[job_id] = replace(
                row,
                status="cancelled",
                finished_at=now,
                error_class=origin,
                locked_by_worker=None,
                lock_expires_at=None,
                last_heartbeat_at=None,
                progress_seq=max(row.progress_seq, progress_seq),
                progress_state=deadline_merged_progress,
            )
            self._append_attempt(
                job_id=job_id,
                attempt=row.attempt,
                started_at=row.started_at,
                now=now,
                outcome="cancelled",
                error_class=origin,
                error_message=None,
                error_traceback=None,
                worker_id=worker_id,
            )
            self._append_state_change_event(
                job_id=job_id,
                from_state="running",
                to_state="cancelled",
                now=now,
                error_class=origin,
                worker_id=worker_id,
            )
            logger.debug(
                "state-change",
                kind="state_change",
                from_state="running",
                to_state="cancelled",
                job_id=str(job_id),
            )
            return "noop"
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="DeadlineExceeded",
            error_message=DEADLINE_EXCEEDED_MESSAGE,
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            # The denial that ran the job out of road still happened to
            # it, and with no per-occurrence rows the aggregate is its
            # only record, counting it here makes the terminal row show
            # the deadline was reached while the job was starving for
            # admission.  An actor-requested deferral is NOT counted:
            # snooze_count tallies deferrals the job actually took, and
            # this one was rejected outright (mirrors the SQL arm).
            rate_limit_blocked_count=row.rate_limit_blocked_count
            + (1 if outcome in ("reservation_denied", "rate_limit_denied") else 0),
            progress_seq=max(row.progress_seq, progress_seq),
            progress_state=deadline_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="DeadlineExceeded",
            error_message=DEADLINE_EXCEEDED_MESSAGE,
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="DeadlineExceeded",
            worker_id=worker_id,
            # The terminal event names which starvation ended a
            # perpetually-denied job, the twin of the SQL deadline arm's
            # conditional denial_reason detail (absent for a plain
            # snooze past the deadline, exactly as the SQL arm's
            # conditional build omits the key PG-side).
            **(
                {"denial_reason": denial_reason}
                if outcome in ("reservation_denied", "rate_limit_denied")
                else {}
            ),
        )
        logger.debug(
            "state-change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=str(job_id),
        )
        return "failed"

    # There is deliberately no budget gate here, mirroring the SQL
    # statement's arms: a deferral's only terminal exit is the deadline
    # arm above.  Being refused a slot is not an execution, so it cannot
    # exhaust a budget only executions spend.
    #
    # The cancel fence (the SQL snoozed arm's `cancel_phase = 0` conjunct,
    # mark_interrupted's semantics): an operator cancel in flight WINS
    # over the deferral. A phase-carrying row must never be rescheduled
    # the reset below would launder the operator's request mid-flight (the
    # bulk-cancel drain would then report the same id from two arms). The
    # fenced-out row stays 'running' carrying its phase, the caller reads
    # back "noop", and the worker's cancel ladder terminalises it. On a
    # clean row the check is trivially false and the deferral semantics
    # are unchanged.
    if row.cancel_phase != CancelPhase.NONE:
        return "noop"

    # PG's snooze arm binds metadata_update through jsonb_param (the
    # NUL-guarded serialization) and merges it server-side
    # (j.metadata || update), so the update's values read back as PG's
    # jsonb round-trip reads them, round-trip it here through the same
    # serialization before the merge; row.metadata is already the
    # round-trip enqueue stored.
    new_metadata = (
        row.metadata
        if metadata_update is None
        else {**row.metadata, **loads(dumps_jsonb_str(metadata_update))}
    )
    snooze_status: Literal["scheduled", "pending"] = (
        "scheduled" if new_scheduled_at > now else "pending"
    )
    merged_progress = _merge_progress(row.progress_state, progress_state)
    # A non-terminal snooze/denial writes no attempt/event rows and never
    # touches max_attempts (the ceiling is a bound, not a counter), and
    # REFUNDS the claim's attempt increment (floored at 0): dispatch
    # stamped attempt+1 to claim the row, and no actor ran, so the gap
    # `max_attempts - attempt` is returned to exactly what it was before
    # the claim.  This holds for every deferral shape, an actor-requested
    # snooze and an admission denial alike, so downstream-429 snoozing
    # is unbounded and never walks the column, and the retry budget
    # measures only real executions, never how saturated a bucket
    # happened to be while the job waited.  The outcome-keyed counters on
    # the row are the deferral's whole durable record, mirroring the SQL
    # arms' CASE increments.
    self._jobs[job_id] = replace(
        row,
        status=snooze_status,
        scheduled_at=new_scheduled_at,
        finished_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        last_heartbeat_at=None,
        attempt=max(row.attempt - 1, 0),
        snooze_count=row.snooze_count + (1 if outcome == "snoozed" else 0),
        rate_limit_blocked_count=row.rate_limit_blocked_count
        + (1 if outcome in ("reservation_denied", "rate_limit_denied") else 0),
        metadata=new_metadata,
        cancel_phase=CancelPhase.NONE,
        cancel_requested_at=None,
        # A deferral returns the row to the pending pool, so it routes by
        # the actor's current assignment from here on.
        assignment_routed=True,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="scheduled",
        job_id=str(job_id),
    )
    return "scheduled"


async def _mark_retry_after(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    *,
    consume_budget: bool = True,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    attempt: int | None = None,
    claim_epoch: int | None = None,
) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
    row = self._jobs.get(job_id)
    # Attempt-epoch fence, one predicate shared with the sibling writes
    # (_fenced's docstring); a fenced-out epoch returns "noop".
    if row is None or _fenced(row, worker_id, attempt, claim_epoch):
        return "noop"

    now = self._clock.now()
    # The deferral floor applies only to the NON-consuming arm: a
    # consuming RetryAfter is a real execution choosing to retry
    # immediately, bounded by the budget it spends, so it keeps the raw
    # delay (0 → pending at now).  The non-consuming arm carries
    # mark_snoozed's floor in full, the same single effective delay
    # feeds the deadline comparison below, the conservative direction,
    # mirroring the SQL arms' GREATEST().
    effective_delay = delay if consume_budget else max(delay, MIN_DEFERRAL_INTERVAL)
    new_scheduled_at = now + effective_delay

    if row.schedule_to_close is not None and new_scheduled_at > row.schedule_to_close:
        deadline_merged_progress = _merge_progress(row.progress_state, progress_state)
        # Cancel-first arbitration inside the deadline arm, the twin of
        # the SQL deadline_cancelled arm (and of _SWEEP_1_SQL's CASE
        # ordering): operator intent outranks the deadline AND the
        # budget, so a phase-carrying row whose schedule_to_close lapsed
        # at deferral time terminalises 'cancelled', never
        # 'failed:DeadlineExceeded'/'failed:MaxAttemptsExceeded' (which
        # would fire deadline/exhaustion hooks and error reports on a
        # cancel in flight). The write mirrors _mark_cancelled: the
        # cancel-origin marker on the row and attempt, the cancel
        # columns preserved as the audit trail, no deadline/budget
        # stamp. The row is already terminal, so the caller reads back
        # "noop", the same contract the fence below returns (the
        # deferral did not land; the operator's cancel decided the row).
        if row.cancel_phase != CancelPhase.NONE:
            origin = (
                CANCEL_ORIGIN_FORCED
                if row.cancel_phase == CancelPhase.FORCED
                else CANCEL_ORIGIN_COOPERATIVE
            )
            self._jobs[job_id] = replace(
                row,
                status="cancelled",
                finished_at=now,
                error_class=origin,
                locked_by_worker=None,
                lock_expires_at=None,
                last_heartbeat_at=None,
                progress_seq=max(row.progress_seq, progress_seq),
                progress_state=deadline_merged_progress,
            )
            self._append_attempt(
                job_id=job_id,
                attempt=row.attempt,
                started_at=row.started_at,
                now=now,
                outcome="cancelled",
                error_class=origin,
                error_message=None,
                error_traceback=None,
                worker_id=worker_id,
            )
            self._append_state_change_event(
                job_id=job_id,
                from_state="running",
                to_state="cancelled",
                now=now,
                error_class=origin,
                worker_id=worker_id,
            )
            logger.debug(
                "state-change",
                kind="state_change",
                from_state="running",
                to_state="cancelled",
                job_id=str(job_id),
                worker_id=worker_id,
                attempt=row.attempt,
                cause="retry_after",
            )
            return "noop"
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="DeadlineExceeded",
            error_message=DEADLINE_EXCEEDED_MESSAGE,
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            progress_seq=max(row.progress_seq, progress_seq),
            progress_state=deadline_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="DeadlineExceeded",
            error_message=DEADLINE_EXCEEDED_MESSAGE,
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="DeadlineExceeded",
            worker_id=worker_id,
        )
        logger.debug(
            "state-change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=str(job_id),
            worker_id=worker_id,
            attempt=row.attempt,
            cause="retry_after",
        )
        return "failed:DeadlineExceeded"

    # Arm order here is deadline → max_attempts → snooze; the fused SQL
    # checks snoozed → max_attempts → deadline with NOT EXISTS chaining.
    # The orders are observably equivalent for the same reason as
    # _mark_snoozed above: a past-deadline row reaches the deadline arm
    # under either order, and every other row's arm choice depends only
    # on its own predicate.
    #
    # The exhaustion arm is enum-complete over retry_kind (any
    # non-indefinite kind fails at budget, a transient-only predicate
    # left non_retryable matching no arm) and exists ONLY on the
    # consuming path: a consuming RetryAfter IS a real execution, so the
    # budget it spends is real.  A non-consuming RetryAfter is an
    # actor-requested deferral, the deadline check above has already
    # returned past-deadline rows, and everything else reschedules with
    # the attempt refunded below, so the budget never degrades no matter
    # how long downstream is unready.
    if consume_budget and row.attempt >= row.max_attempts and row.retry_kind != "indefinite":
        maxatt_merged_progress = _merge_progress(row.progress_state, progress_state)
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="MaxAttemptsExceeded",
            error_message="retry budget exhausted",
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            attempt=row.attempt,
            progress_seq=max(row.progress_seq, progress_seq),
            progress_state=maxatt_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="MaxAttemptsExceeded",
            error_message="retry budget exhausted",
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="MaxAttemptsExceeded",
            worker_id=worker_id,
        )
        logger.debug(
            "state-change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=str(job_id),
            worker_id=worker_id,
            attempt=row.attempt,
            cause="retry_after",
        )
        return "failed:MaxAttemptsExceeded"

    # A non-consuming RetryAfter is an actor-requested deferral, not an
    # execution: it writes no attempt/event rows, never touches
    # max_attempts, REFUNDS the claim's attempt increment (floored at 0,
    # so honouring downstream 429s never degrades the budget), and
    # counts itself on the row's snooze counter.  A consuming one IS a
    # real execution: its attempt increment stands and it keeps writing
    # its rows below.
    #
    # The cancel fence (the SQL snoozed arms' `cancel_phase = 0`
    # conjunct, mark_interrupted's semantics): an operator cancel in
    # flight WINS over the deferral. A phase-carrying row must never be
    # rescheduled; the reset below would launder the operator's request
    # mid-flight. The fenced-out row stays 'running' carrying its phase,
    # the caller reads back "noop", and the worker's cancel ladder
    # terminalises it. (A phase-carrying row whose deadline has ALREADY
    # lapsed never reaches this fence: the deadline arm's cancel-first
    # split above terminalised it 'cancelled' first.) On a clean row the
    # check is trivially false and the budget semantics are unchanged.
    if row.cancel_phase != CancelPhase.NONE:
        return "noop"
    new_attempt = row.attempt if consume_budget else max(row.attempt - 1, 0)
    retry_status: Literal["scheduled", "pending"] = (
        "scheduled" if new_scheduled_at > now else "pending"
    )
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status=retry_status,
        scheduled_at=new_scheduled_at,
        finished_at=None,
        attempt=new_attempt,
        snooze_count=row.snooze_count if consume_budget else row.snooze_count + 1,
        locked_by_worker=None,
        lock_expires_at=None,
        last_heartbeat_at=None,
        cancel_phase=CancelPhase.NONE,
        cancel_requested_at=None,
        # A deferral returns the row to the pending pool, so it routes by
        # the actor's current assignment from here on.
        assignment_routed=True,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    if consume_budget:
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="snoozed",
            error_class="RetryAfter",
            error_message=None,
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="scheduled",
            now=now,
            worker_id=worker_id,
        )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state="scheduled",
        job_id=str(job_id),
        worker_id=worker_id,
        attempt=new_attempt,
        cause="retry_after",
    )
    return "scheduled"


async def _mark_interrupted(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    *,
    attempt: int,
    hold: timedelta,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    claim_epoch: int | None = None,
) -> Literal["pending", "scheduled", "failed:DeadlineExceeded", "noop"]:
    row = self._jobs.get(job_id)
    # The fence mirrors the SQL arm conjunct-for-conjunct: the shared
    # status/owner/attempt-epoch predicate (_fenced), then this arm's
    # extra cancel conjunct (cancel_phase = 0: an operator cancel in
    # flight wins and reads back as "noop", since a row with a cancel
    # timestamp set is cancelled, never re-available).
    if (
        row is None
        or _fenced(row, worker_id, attempt, claim_epoch)
        or row.cancel_phase != CancelPhase.NONE
    ):
        return "noop"

    now = self._clock.now()
    # The hold floor mirrors the params CTE's CASE: a positive hold is
    # floored at the non-consuming deferral floor; a zero hold stays zero
    # so the release lands pending immediately (the row is genuinely free
    # and the actor is gone, the floor exists to stop deferral loops and
    # an interruption is not one).
    effective_hold = max(hold, MIN_DEFERRAL_INTERVAL) if hold > timedelta(0) else timedelta(0)
    new_scheduled_at = now + effective_hold

    # The deadline arm mirrors the SQL's deadline_failed CTE (checked first
    # here exactly as _mark_snoozed's twin orders it, the orders are
    # observably equivalent because the release arm's deadline guard and
    # this arm's NOT EXISTS chain partition the rows identically).
    if row.schedule_to_close is not None and new_scheduled_at > row.schedule_to_close:
        deadline_merged_progress = _merge_progress(row.progress_state, progress_state)
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="DeadlineExceeded",
            error_message=DEADLINE_EXCEEDED_MESSAGE,
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            progress_seq=max(row.progress_seq, progress_seq),
            progress_state=deadline_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="DeadlineExceeded",
            error_message=DEADLINE_EXCEEDED_MESSAGE,
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="DeadlineExceeded",
            worker_id=worker_id,
        )
        logger.debug(
            "state-change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=str(job_id),
        )
        return "failed:DeadlineExceeded"

    # The release arm. The claim's attempt increment is NOT refunded: the
    # attempt did start executing, so its increment stands; refunding it
    # would re-create the exact epoch the interrupted (zombie) handler
    # still holds, and the zombie's later terminal write would land on
    # the re-dispatched attempt; the snooze/unavailable arms
    # keep their refund because nothing executed there). No attempt row
    # is written (an interruption is not an execution outcome); one
    # state_change event with reason 'interrupted' records the transition
    # and interrupt_count carries the aggregate on the row. The release
    # is a re-pend, so the row routes by the actor's current assignment
    # from here on (mirrors the SQL released arm's assignment_routed).
    released_status: Literal["scheduled", "pending"] = (
        "scheduled" if effective_hold > timedelta(0) else "pending"
    )
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status=released_status,
        scheduled_at=new_scheduled_at,
        finished_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        last_heartbeat_at=None,
        cancel_phase=CancelPhase.NONE,
        cancel_requested_at=None,
        interrupt_count=row.interrupt_count + 1,
        assignment_routed=True,
        progress_seq=max(row.progress_seq, progress_seq),
        progress_state=merged_progress,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state=released_status,
        now=now,
        reason="interrupted",
        worker_id=worker_id,
        hold_seconds=effective_hold.total_seconds(),
    )
    logger.debug(
        "state-change",
        kind="state_change",
        from_state="running",
        to_state=released_status,
        job_id=str(job_id),
        reason="interrupted",
    )
    return released_status


async def _write_attempt(self: "InMemoryBackend", attempt: AttemptRow) -> None:
    # PG serialises the attempt row at INSERT time (metadata through
    # dumps_jsonb_str, the same NUL-guarded serialization jsonb_param
    # binds) and reads it back through loads, so a caller-held AttemptRow
    # can never reach storage by reference and the stored metadata holds
    # PG's jsonb read-back values.
    rows = self._attempts.setdefault(attempt.job_id, [])
    # Mirrors the ON CONFLICT (job_id, attempt) DO NOTHING guard every PG
    # job_attempts insert carries: the claim's ceiling clamp can repeat an
    # attempt number, and PG keeps the first record of it, the twin must
    # not accumulate a duplicate PG refused to store.
    if any(row.attempt == attempt.attempt for row in rows):
        return
    rows.append(replace(attempt, metadata=loads(dumps_jsonb_str(attempt.metadata))))
