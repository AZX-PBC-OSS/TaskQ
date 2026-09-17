"""Terminal exception handlers for the consumer.

The six exception-to-terminal-state routers (timeout, snooze, retry_after,
reservation denied, generic exception) live here.  Each handler maps a
raised exception to the appropriate backend terminal write, span event,
and structured log entry.

Failure-logging contract.  The names below are the event strings as
emitted — alert on them verbatim:

- ``state-change`` — INFO, lifecycle transitions.
- ``job_timeout`` / ``job_exception`` — WARNING, every attempt (retryable
  or terminal), with full error_class/error_message/error_traceback.
- ``job-failed`` — ERROR, exactly once per dead job, emitted by all five
  handlers after the terminal write persists.
- ``terminal-write-retry`` — WARNING, one per retried attempt of a
  terminal write that hit an infra error (attempt number, wait, cause).
- ``terminal-write-failed`` — ERROR, infra write failure after the retry
  budget is spent.

Note the spelling split: this module emits ``job-failed`` hyphenated (the
prevailing convention elsewhere in the tree) but ``job_timeout`` /
``job_exception`` in snake_case.  That inconsistency is real and is left
alone deliberately — event names are an observable contract that
operators' alert rules already match on, and the project-wide naming
convention (kebab vs snake vs OTel dotted namespaces) is an open
decision.  Renaming them belongs to that decision, not to a docstring
fix; until then this list tells the truth about what is emitted.

:func:`_dispatch_exception` consolidates the exception dispatch logic
shared between ``consume_one_job`` and ``_consume_transactional``.
"""

import asyncio
import time
import traceback
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Final, Literal
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace

from taskq._json import sanitize_nul_str
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import (
    Backend,
    ErrorInfo,
    JobRow,
)
from taskq.backend._protocol import (
    DenialReason as BackendDenialReason,
)
from taskq.backend._protocol import (
    SnoozeOutcome as BackendSnoozeOutcome,
)
from taskq.exceptions import (
    ReservationUnavailable,
    RetryAfter,
    Snooze,
)
from taskq.obs import (
    ErrorReporter,
    ExceptionText,
    invoke_error_reporter,
    log_state_change,
    record_attempt_failure,
    record_job_timeout,
    record_reservation_denial,
    render_exception,
)
from taskq.retry import (
    ActorConfigLike,
    JobRetryState,
    Retry,
    apply_jitter,
    decide_after_failure,
    invoke_on_retry_exhausted,
    safe_mark_failed_or_retry,
)
from taskq.settings import WorkerSettings

if TYPE_CHECKING:
    import redis.asyncio as redis_async

    from taskq.progress._buffer import _ProgressBuffer

type AttemptOutcome = Literal[
    "succeeded",
    "failed",
    "cancelled",
    "scheduled",
    "noop",
]

__all__ = [
    "AttemptOutcome",
    "_AttemptFencedOut",
    "_TerminalWriteFailed",
    "_disown_job",
    "_dispatch_exception",
    "_handle_generic_exception",
    "_handle_reservation_class_denied",
    "_handle_retry_after",
    "_handle_snooze",
    "_handle_timeout",
    "_log_terminal_write_failed",
    "_terminal_write_with_retry",
]

# Infra failures during the terminal-write itself (DB connection drop,
# timeout acquiring a pool connection, socket errors, pool/conn
# lifecycle refusals) — as opposed to the actor's own exception, which
# is what decide_after_failure/error_info describe. These must NOT be
# treated as "the actor failed with this exception": doing so would
# overwrite the real error_info and re-run the retry decision against
# the wrong exception type. See `_log_terminal_write_failed`.
#
# asyncpg.InterfaceError is the pool/conn lifecycle family the dispatch
# release path and POOL_INFRA_EXCEPTIONS already classify as
# infrastructure: a bounded pool close (worker teardown, credential
# rotation drain) terminates or releases the slot connection underneath
# an in-flight dispatch, and the fallback terminal write then hits a
# closed pool or a released proxy — teardown infrastructure, never a
# job outcome; letting it escape paints a job failure that never
# happened (drain mode counts it as exit 3).
_TERMINAL_WRITE_INFRA_EXCEPTIONS: tuple[type[BaseException], ...] = (
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    OSError,
    TimeoutError,
)


_TERMINAL_WRITE_ATTEMPTS: Final[int] = 4
"""How many times one terminal write is attempted before it is reported
as failed. Four: enough to ride out a connection reset, a pool-acquire
timeout or a failover of a few hundred milliseconds. The heartbeat keeps
renewing the lease for the whole window (the job is disowned only after
the budget is spent), so the lock lease cannot lapse mid-retry."""

_TERMINAL_WRITE_BACKOFF: Final[tuple[timedelta, ...]] = (
    timedelta(milliseconds=50),
    timedelta(milliseconds=200),
    timedelta(milliseconds=800),
)
"""The wait before the second, third and fourth attempt. Geometric and
just over a second in total: a blip that has not cleared by then is an
outage the lease-reclaim path owns, and every attempt past it converts
into an at-least-once re-run of work that already finished, so the
window is worth a little more than the quarter-second a bare connection
reset needs."""

_TERMINAL_WRITE_BUDGET: Final[timedelta] = timedelta(seconds=5)
"""Wall time, from the first attempt, within which a retry may still be
started. Attempts alone do not bound the window: against a black-holed
Postgres each attempt costs a full statement timeout, and counting to
four would hold the consumer slot for four of them. A retry whose wait
would end past the budget is not made. One statement timeout at the
default settings — a write still failing after that long is an outage,
not a blip."""

_TERMINAL_WRITE_JITTER: Final[float] = 0.25
"""Spread on each backoff wait, so every consumer slot that hit the same
blip does not re-issue its write on the same tick."""


async def _terminal_write_with_retry[T](
    write: Callable[[], Awaitable[T]],
    *,
    log: structlog.stdlib.BoundLogger,
    job: JobRow,
    write_name: str,
    monotonic: Callable[[], float] = time.monotonic,
) -> T:
    """Run one pool-path terminal write, retrying the infra-failure family.

    *write* builds the backend call afresh per attempt (a coroutine can be
    awaited once). Each attempt runs under ``shield_with_retrieval`` so an
    external cancel never strands an in-flight statement, and only
    :data:`_TERMINAL_WRITE_INFRA_EXCEPTIONS` is retried — a fence outcome
    (``False``, ``"noop"``, ``None``) is the backend's answer and returns
    as-is, and any other exception is a defect that stays loud on the
    first raise. The budget is both :data:`_TERMINAL_WRITE_ATTEMPTS` and
    :data:`_TERMINAL_WRITE_BUDGET` of wall time (read from *monotonic*),
    whichever is spent first. The final infra failure propagates
    unchanged, so every caller's existing ``terminal-write-failed``
    handling is the terminal outcome of an exhausted budget.

    Not for ``*_with_conn`` writes: those run on the job's own transaction
    connection, and an infra error there has already aborted the
    transaction — re-issuing the statement on it cannot land.
    """
    started = monotonic()
    budget_s = _TERMINAL_WRITE_BUDGET.total_seconds()
    for attempt in range(1, _TERMINAL_WRITE_ATTEMPTS + 1):
        try:
            return await shield_with_retrieval(write())
        except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
            if attempt >= _TERMINAL_WRITE_ATTEMPTS:
                raise
            wait = apply_jitter(_TERMINAL_WRITE_BACKOFF[attempt - 1], _TERMINAL_WRITE_JITTER)
            elapsed = monotonic() - started
            if elapsed + wait.total_seconds() > budget_s:
                log.warning(
                    "terminal-write-retry-budget-exhausted",
                    kind="terminal_write_retry_budget_exhausted",
                    job_id=str(job.id),
                    actor=job.actor,
                    write=write_name,
                    attempt=attempt,
                    elapsed_ms=int(elapsed * 1000),
                    budget_ms=int(budget_s * 1000),
                    infra_error_class=type(infra_exc).__name__,
                    infra_error_message=str(infra_exc),
                )
                raise
            log.warning(
                "terminal-write-retry",
                kind="terminal_write_retry",
                job_id=str(job.id),
                actor=job.actor,
                write=write_name,
                attempt=attempt,
                max_attempts=_TERMINAL_WRITE_ATTEMPTS,
                retry_in_ms=int(wait.total_seconds() * 1000),
                infra_error_class=type(infra_exc).__name__,
                infra_error_message=str(infra_exc),
            )
            await asyncio.sleep(wait.total_seconds())
    raise AssertionError("unreachable: the attempt loop returns or raises")


class _TerminalWriteFailed(BaseException):
    """Control-flow sentinel: a success-path terminal write failed with an infra error.

    Raised by the consumer's success paths (``mark_succeeded``,
    ``mark_succeeded_with_conn``) when the DB write raises an infra
    exception.  Extends :class:`BaseException` (not :class:`Exception`) so
    it propagates past the generic ``except Exception`` dispatch clauses
    without being re-dispatched into ``_handle_generic_exception`` — which
    would misclassify the infra error as the actor's failure.

    The actor already succeeded; the job stays ``running`` and is reclaimed
    via lock-lease expiry.
    """

    def __init__(self, infra_exc: BaseException) -> None:
        self.infra_exc = infra_exc
        super().__init__("terminal write failed")


class _AttemptFencedOut(BaseException):
    """Control-flow sentinel: the attempt's terminal write matched no row.

    Raised inside the transactional success path when
    ``mark_succeeded_with_conn`` returns ``False`` — the fencing predicate
    (``id`` + ``status='running'`` + ``locked_by_worker`` + attempt epoch)
    matched nothing, so the row moved underneath this attempt (a lease
    reclaim re-pended it and a later attempt owns it now). Raised, not
    returned, because the actor's writes share the open transaction: the
    raise is what rolls them back instead of committing side effects for
    an attempt the system never recorded as terminated.

    Extends :class:`BaseException` (not :class:`Exception`) so it
    propagates past the generic ``except Exception`` dispatch clauses
    without being re-dispatched into ``_handle_generic_exception`` — a
    fenced-out write is not the actor's failure, and routing it there
    would issue a second, also-fenced terminal write and misreport the
    attempt. The consumer's transactional wrapper catches it by name and
    reports ``"noop"`` (the outcome batch policy and the dispatch metrics
    already treat as "nothing was this dispatch's to move").
    """


def _log_terminal_write_failed(
    log: structlog.stdlib.BoundLogger,
    job: JobRow,
    job_exc: BaseException | None,
    infra_exc: BaseException,
) -> None:
    """Log a terminal-write infra failure without mutating job state.

    The job row is left in ``running`` — none of the write's attempts
    landed (see :func:`_terminal_write_with_retry`) — so lock-lease
    expiry and the crash sweep reclaim it for retry. This is intentionally
    NOT re-dispatched into
    ``_handle_generic_exception`` (that would classify the *infra*
    exception as the actor's failure, discarding the real one and
    re-running the retry decision against the wrong exception type).

    When *job_exc* is ``None`` (success-path), ``actor_succeeded=True`` is
    logged to distinguish it from the error-path case where the actor
    itself raised.
    """
    log.error(
        "terminal-write-failed",
        kind="terminal-write-failed",
        job_id=str(job.id),
        actor=job.actor,
        actor_succeeded=job_exc is None,
        job_error_class=type(job_exc).__name__ if job_exc is not None else None,
        job_error_message=str(job_exc) if job_exc is not None else None,
        job_error_traceback=_format_exc(job_exc) if job_exc is not None else None,
        infra_error_class=type(infra_exc).__name__,
        infra_error_message=str(infra_exc),
        infra_error_traceback=_format_exc(infra_exc),
    )


def _disown_job(disowned_jobs: "set[UUID] | None", job: JobRow) -> None:
    """Record that this worker is done with *job* but could not move its row.

    Called wherever a terminal write's retry budget is spent and the row
    is left ``running`` under this worker's lock. The heartbeat excludes
    the recorded ids from lease renewal (see ``WorkerDeps.disowned_jobs``),
    which is what turns "lock-lease expiry reclaims it" from a promise
    into the actual recovery. ``None`` is a caller with no worker deps —
    a direct test invocation — and nothing to record into.
    """
    if disowned_jobs is not None:
        disowned_jobs.add(job.id)


def _format_exc(exc: BaseException) -> str:
    """Format *exc*'s traceback from the explicit exception object.

    ``traceback.format_exc()`` formats the *ambient* active exception and
    silently degrades to ``'NoneType: None\\n'`` when the handler runs
    outside an ``except`` block (direct calls in tests; any future
    refactored call site that defers handling). Formatting the explicit
    parameter matches :func:`_log_terminal_write_failed` and is immune to
    call-site structure.
    """
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def _log_job_failed(
    log: structlog.stdlib.BoundLogger,
    job: JobRow,
    *,
    cause: str,
    error_class: str,
    error_message: str | None = None,
    error_traceback: str | None = None,
    **context: object,
) -> None:
    """Emit ``job-failed`` — the single ERROR event for a dead job.

    Alerting contract: exactly one ``job-failed`` per terminal
    (non-retryable) failure, emitted by all five handlers *after* the
    terminal write persists — never on the retry path, never on infra
    write failure (that is ``terminal-write-failed``), never on ownership
    mismatch (our write did not land; the job is not dead by our hand).

    ``cause`` is the terminal classification (``Fail.error_class`` or the
    backend tri-state cause); ``error_class`` is the concrete exception
    class. ``None``-valued optionals are omitted from the event, not
    logged as null, so synthesized-cause sites keep their exact field set.
    """
    fields: dict[str, object] = {
        "job_id": str(job.id),
        "actor": job.actor,
        "attempt": job.attempt,
        "cause": cause,
        "error_class": error_class,
    }
    if error_message is not None:
        fields["error_message"] = error_message
    if error_traceback is not None:
        fields["error_traceback"] = error_traceback
    fields.update(context)
    log.error("job-failed", **fields)


async def _post_write_row(backend: Backend, job: JobRow) -> JobRow:
    """Re-read the job row a terminal hook is handed, post-write.

    The snooze-family terminal writes return a tri-state string (unlike
    ``mark_failed_or_retry``, which returns the written row), so the
    post-write world a hook inspects — status, error_class, the standing
    attempt — is re-read here. A re-read that fails with an infra error
    or finds no row degrades to the dispatch-time row: the terminal
    write already landed, and misreporting it as a terminal-write
    failure — or dropping the hooks entirely — would be worse than
    handing the hooks the stale snapshot.
    """
    reread_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.worker.hooks")
    try:
        updated = await backend.get(job.id)
    except _TERMINAL_WRITE_INFRA_EXCEPTIONS as exc:
        reread_log.warning(
            "terminal-hook-row-reread-failed",
            kind="terminal_hook_row_reread_failed",
            job_id=str(job.id),
            error_class=type(exc).__name__,
        )
        return job
    if updated is None:
        reread_log.warning(
            "terminal-hook-row-reread-missing",
            kind="terminal_hook_row_reread_missing",
            job_id=str(job.id),
        )
        return job
    return updated


async def _report_terminal_failure(
    *,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    job: JobRow,
    updated_row: JobRow,
    exc: BaseException,
    error_info: ErrorInfo,
    log_message: str,
    log_traceback: str,
    cause: str,
    retryable: bool,
    actor_config: ActorConfigLike,
    error_reporter: ErrorReporter | None,
) -> None:
    """Announce a failure the terminal write just made final — once, and
    the same way however the job got there.

    The span event, the ``running -> failed`` state change, the
    ``job-failed`` ERROR line, the ``on_retry_exhausted`` hook and the
    :class:`ErrorReporter` are the whole set of terminal-failure signals;
    both the decision's own Fail and a Retry the row's
    ``schedule_to_close`` deadline refused at the write (``cause`` is
    then ``DeadlineExceeded``) go through here, so no terminal failure is
    silent to the alerting contract or to the reporter.
    """
    span.add_event(
        "lifecycle.failed",
        attributes={
            "from_state": "running",
            "to_state": "failed",
            "error_class": cause if cause == "DeadlineExceeded" else error_info.error_class,
        },
    )
    log_state_change(
        log,
        from_state="running",
        to_state="failed",
        cause=type(exc).__name__,
        retryable=retryable,
    )
    _log_job_failed(
        log,
        job,
        cause=cause,
        error_class=error_info.error_class,
        error_message=log_message,
        error_traceback=log_traceback,
    )
    await invoke_on_retry_exhausted(
        actor_config.on_retry_exhausted,
        updated_row,
        exc,
        actor_config.on_retry_exhausted_timeout,
        log=log,
    )
    await invoke_error_reporter(error_reporter, updated_row, exc, log=log)


async def _handle_timeout(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    exc: TimeoutError,
    actor_config: ActorConfigLike,
    max_retry_backoff: timedelta,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    error_reporter: ErrorReporter | None = None,
    text: ExceptionText | None = None,
) -> AttemptOutcome:
    """Route a ``start_to_close`` timeout to its retry decision and terminal write.

    *text* is the rendering the ``attempt.N`` span already made of *exc* when
    the exception escaped the span; rendered here when the caller has none.
    """
    if text is None:
        text = render_exception(exc)
    # The message and traceback are derived from an uncontrolled exception:
    # rejecting them (the ErrorInfo guard's job for caller-supplied text)
    # would strand the very job the text describes — the terminal write
    # must land with the defect visible as an escape sequence.
    raw_message = str(exc)
    error_info = ErrorInfo(
        error_class=type(exc).__name__,
        error_message=sanitize_nul_str(raw_message or "start_to_close"),
        error_traceback=sanitize_nul_str(text.raw_stacktrace),
    )
    # The log channel leaves the trust boundary and carries the scrubbed text;
    # the fallback name is a constant, not exception text, so it needs neither.
    log_message = text.message.nul_escaped() if raw_message else "start_to_close"
    log_traceback = text.stacktrace.nul_escaped()
    log.warning(
        "job_timeout",
        job_id=str(job.id),
        actor=job.actor,
        attempt=job.attempt,
        error_class=error_info.error_class,
        error_message=log_message,
        error_traceback=log_traceback,
    )
    record_job_timeout(job.actor, kind="start_to_close")
    job_state = JobRetryState(
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        retry_kind=job.retry_kind,
        schedule_to_close=job.schedule_to_close,
        start_to_close=job.start_to_close,
    )
    decision = decide_after_failure(
        actor_config,
        exc,
        job_state,
        max_retry_backoff=max_retry_backoff,
    )
    record_attempt_failure(job.actor, error_info.error_class, retryable=isinstance(decision, Retry))
    if isinstance(decision, Retry):
        updated_row = await _terminal_write_with_retry(
            lambda: safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                decision.retry_delay,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
                attempt=job.attempt,
            ),
            log=log,
            job=job,
            write_name="mark_failed_or_retry",
        )
        if updated_row is None:
            # Fenced out: the row moved underneath this attempt (a reclaim
            # race), so nothing was scheduled — announce nothing. The live
            # attempt's own write records the real transition.
            log.debug(
                "consume-timeout-noop",
                from_state="running",
                to_state="noop",
                cause=type(exc).__name__,
            )
            return "noop"
        if updated_row.status == "failed":
            # The write's deadline arm refused the retry: the row's
            # schedule_to_close lies before the next dispatch, so the
            # backend landed it failed with DeadlineExceeded — a terminal
            # failure, reported exactly like a Fail decision.
            record_job_timeout(job.actor, kind="schedule_to_close")
            await _report_terminal_failure(
                span=span,
                log=log,
                job=job,
                updated_row=updated_row,
                exc=exc,
                error_info=error_info,
                log_message=log_message,
                log_traceback=log_traceback,
                cause="DeadlineExceeded",
                retryable=True,
                actor_config=actor_config,
                error_reporter=error_reporter,
            )
            return "failed"
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "error_class": error_info.error_class,
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause=type(exc).__name__,
        )
        return "scheduled"
    else:
        updated_row = await _terminal_write_with_retry(
            lambda: safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                None,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
                attempt=job.attempt,
            ),
            log=log,
            job=job,
            write_name="mark_failed_or_retry",
        )
        if updated_row is None:
            # Fenced out — see the retry branch above.
            log.debug(
                "consume-timeout-noop",
                from_state="running",
                to_state="noop",
                cause=type(exc).__name__,
            )
            return "noop"
        await _report_terminal_failure(
            span=span,
            log=log,
            job=job,
            updated_row=updated_row,
            exc=exc,
            error_info=error_info,
            log_message=log_message,
            log_traceback=log_traceback,
            cause=decision.error_class,
            retryable=decision.retryable,
            actor_config=actor_config,
            error_reporter=error_reporter,
        )
        return "failed"


async def _handle_snooze(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    s: Snooze,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    actor_config: ActorConfigLike,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    error_reporter: ErrorReporter | None = None,
) -> AttemptOutcome:
    # The row's snooze_count column is the deferral counter — the
    # backend's snooze arm increments it; no metadata mirror is merged
    # here (one source of truth).
    tri = await _terminal_write_with_retry(
        lambda: backend.mark_snoozed(
            job.id,
            worker_id,
            s.delay,
            progress_seq=progress_seq,
            progress_state=progress_state,
            attempt=job.attempt,
        ),
        log=log,
        job=job,
        write_name="mark_snoozed",
    )
    if tri == "scheduled":
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "delay_seconds": s.delay.total_seconds(),
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause="Snooze",
            delay_seconds=s.delay.total_seconds(),
        )
        return "scheduled"
    elif tri == "failed":
        record_job_timeout(job.actor, kind="schedule_to_close")
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": "DeadlineExceeded",
            },
        )
        hook_row = await _post_write_row(backend, job)
        # The snooze_count log field reads the row counter the terminal
        # write left behind: the rejected deferral never landed, so the
        # column still counts only the snoozes that did.
        _log_job_failed(
            log,
            job,
            cause="DeadlineExceeded",
            error_class="DeadlineExceeded",
            snooze_count=hook_row.snooze_count,
        )
        log_state_change(
            log,
            from_state="running",
            to_state="failed",
            cause="DeadlineExceeded",
        )
        await invoke_on_retry_exhausted(
            actor_config.on_retry_exhausted,
            hook_row,
            TimeoutError("DeadlineExceeded"),
            actor_config.on_retry_exhausted_timeout,
            log=log,
        )
        await invoke_error_reporter(
            error_reporter,
            hook_row,
            TimeoutError("DeadlineExceeded"),
            log=log,
        )
        return "failed"
    else:
        log.debug(
            "consume-snooze-noop",
            from_state="running",
            to_state="noop",
            cause="Snooze",
        )
        return "noop"


async def _handle_retry_after(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    r: RetryAfter,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    actor_config: ActorConfigLike,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    error_reporter: ErrorReporter | None = None,
) -> AttemptOutcome:
    tri = await _terminal_write_with_retry(
        lambda: backend.mark_retry_after(
            job.id,
            worker_id,
            r.delay,
            consume_budget=r.consume_budget,
            progress_seq=progress_seq,
            progress_state=progress_state,
            attempt=job.attempt,
        ),
        log=log,
        job=job,
        write_name="mark_retry_after",
    )
    if tri == "scheduled":
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "delay_seconds": r.delay.total_seconds(),
                "consume_budget": r.consume_budget,
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause="RetryAfter",
            delay_seconds=r.delay.total_seconds(),
            consume_budget=r.consume_budget,
        )
        return "scheduled"
    elif tri in ("failed:DeadlineExceeded", "failed:MaxAttemptsExceeded"):
        cause = tri.split(":")[1]
        if cause == "DeadlineExceeded":
            record_job_timeout(job.actor, kind="schedule_to_close")
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": cause,
            },
        )
        hook_row = await _post_write_row(backend, job)
        _log_job_failed(
            log,
            job,
            cause=cause,
            error_class=cause,
            consume_budget=r.consume_budget,
        )
        log_state_change(
            log,
            from_state="running",
            to_state="failed",
            cause=cause,
            consume_budget=r.consume_budget,
        )
        exc = (
            TimeoutError("DeadlineExceeded")
            if cause == "DeadlineExceeded"
            else RuntimeError("MaxAttemptsExceeded")
        )
        await invoke_on_retry_exhausted(
            actor_config.on_retry_exhausted,
            hook_row,
            exc,
            actor_config.on_retry_exhausted_timeout,
            log=log,
        )
        await invoke_error_reporter(error_reporter, hook_row, exc, log=log)
        return "failed"
    else:
        log.debug(
            "consume-retry-after-noop",
            from_state="running",
            to_state="noop",
            cause="RetryAfter",
        )
        return "noop"


async def _handle_reservation_class_denied(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    e: ReservationUnavailable,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    actor_config: ActorConfigLike,
    *,
    awaiting_prefix: str,
    outcome: BackendSnoozeOutcome,
    debug_event: str,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    error_reporter: ErrorReporter | None = None,
    denial_reason: BackendDenialReason = "capacity",
) -> AttemptOutcome:
    # Every denial a worker fields is counted before anything else: the
    # denial itself is the operational signal (a saturated bucket), and
    # the outcome of the snooze write below must never be able to lose
    # it. Labeled by source only — bucket names are caller-derived and
    # unbounded, so they are not a dimension (see obs/_otel.py).
    record_reservation_denial(e.bucket_name, e.source)
    # The raw retry_after is a synchroniser under mass denial: every job
    # denied in the same round carries the same hint (identical token
    # deficit for same-round rate-limit denials, one lease horizon for
    # slot denials), so the herd re-attempts in lockstep and each cycle
    # costs claim + acquire + snooze per job. Spread it with the actor's
    # retry jitter — the same knob and formula the failure backoff uses —
    # before handing it to the snooze arm, whose MIN_DEFERRAL_INTERVAL
    # floor still applies downstream. Timing-only: retry_after is
    # advisory, never an admission or budget decision.
    retry_after = apply_jitter(e.retry_after, actor_config.retry.jitter)
    tri = await _terminal_write_with_retry(
        lambda: backend.mark_snoozed(
            job.id,
            worker_id,
            retry_after,
            metadata_update={"awaiting": f"{awaiting_prefix}{e.bucket_name}"},
            outcome=outcome,
            progress_seq=progress_seq,
            progress_state=progress_state,
            attempt=job.attempt,
            denial_reason=denial_reason,
        ),
        log=log,
        job=job,
        write_name="mark_snoozed",
    )
    if tri == "scheduled":
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "bucket_name": e.bucket_name,
                "delay_seconds": retry_after.total_seconds(),
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause="ReservationUnavailable",
            bucket_name=e.bucket_name,
            delay_seconds=retry_after.total_seconds(),
        )
        return "scheduled"
    elif tri == "failed":
        record_job_timeout(job.actor, kind="schedule_to_close")
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": "DeadlineExceeded",
            },
        )
        hook_row = await _post_write_row(backend, job)
        _log_job_failed(
            log,
            job,
            cause="DeadlineExceeded",
            error_class="DeadlineExceeded",
            bucket_name=e.bucket_name,
        )
        log_state_change(
            log,
            from_state="running",
            to_state="failed",
            cause="DeadlineExceeded",
            bucket_name=e.bucket_name,
        )
        await invoke_on_retry_exhausted(
            actor_config.on_retry_exhausted,
            hook_row,
            TimeoutError("DeadlineExceeded"),
            actor_config.on_retry_exhausted_timeout,
            log=log,
        )
        await invoke_error_reporter(
            error_reporter,
            hook_row,
            TimeoutError("DeadlineExceeded"),
            log=log,
        )
        return "failed"
    else:
        log.debug(
            debug_event,
            from_state="running",
            to_state="noop",
            cause="ReservationUnavailable",
        )
    return "noop"


async def _handle_generic_exception(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    e: Exception,
    actor_config: ActorConfigLike,
    max_retry_backoff: timedelta,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    error_reporter: ErrorReporter | None = None,
    text: ExceptionText | None = None,
) -> AttemptOutcome:
    """Route an actor exception to its retry decision and terminal write.

    *text* is the rendering the ``attempt.N`` span already made of *e* when
    the exception escaped the span; rendered here when the caller has none.
    """
    if text is None:
        text = render_exception(e)
    # The message and traceback are derived from an uncontrolled exception:
    # rejecting them (the ErrorInfo guard's job for caller-supplied text)
    # would strand the very job the text describes — the terminal write
    # must land with the defect visible as an escape sequence.
    error_info = ErrorInfo(
        error_class=type(e).__name__,
        error_message=sanitize_nul_str(str(e)),
        error_traceback=sanitize_nul_str(text.raw_stacktrace),
    )
    # The log channel leaves the trust boundary and carries the scrubbed text.
    log_message = text.message.nul_escaped()
    log_traceback = text.stacktrace.nul_escaped()
    log.warning(
        "job_exception",
        job_id=str(job.id),
        actor=job.actor,
        attempt=job.attempt,
        error_class=error_info.error_class,
        error_message=log_message,
        error_traceback=log_traceback,
    )
    job_state = JobRetryState(
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        retry_kind=job.retry_kind,
        schedule_to_close=job.schedule_to_close,
        start_to_close=job.start_to_close,
    )
    decision = decide_after_failure(actor_config, e, job_state, max_retry_backoff=max_retry_backoff)
    record_attempt_failure(job.actor, error_info.error_class, retryable=isinstance(decision, Retry))
    if isinstance(decision, Retry):
        updated_row = await _terminal_write_with_retry(
            lambda: safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                decision.retry_delay,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
                attempt=job.attempt,
            ),
            log=log,
            job=job,
            write_name="mark_failed_or_retry",
        )
        if updated_row is None:
            # Fenced out: the row moved underneath this attempt (a reclaim
            # race), so nothing was scheduled — announce nothing. The live
            # attempt's own write records the real transition.
            log.debug(
                "consume-exception-noop",
                from_state="running",
                to_state="noop",
                cause=type(e).__name__,
            )
            return "noop"
        if updated_row.status == "failed":
            # The write's deadline arm refused the retry — see
            # _handle_timeout's retry branch.
            record_job_timeout(job.actor, kind="schedule_to_close")
            await _report_terminal_failure(
                span=span,
                log=log,
                job=job,
                updated_row=updated_row,
                exc=e,
                error_info=error_info,
                log_message=log_message,
                log_traceback=log_traceback,
                cause="DeadlineExceeded",
                retryable=True,
                actor_config=actor_config,
                error_reporter=error_reporter,
            )
            return "failed"
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "error_class": type(e).__name__,
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause=type(e).__name__,
        )
        return "scheduled"
    else:
        updated_row = await _terminal_write_with_retry(
            lambda: safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                None,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
                attempt=job.attempt,
            ),
            log=log,
            job=job,
            write_name="mark_failed_or_retry",
        )
        if updated_row is None:
            # Fenced out — see the retry branch above.
            log.debug(
                "consume-exception-noop",
                from_state="running",
                to_state="noop",
                cause=type(e).__name__,
            )
            return "noop"
        await _report_terminal_failure(
            span=span,
            log=log,
            job=job,
            updated_row=updated_row,
            exc=e,
            error_info=error_info,
            log_message=log_message,
            log_traceback=log_traceback,
            cause=decision.error_class,
            retryable=decision.retryable,
            actor_config=actor_config,
            error_reporter=error_reporter,
        )
        return "failed"


async def _dispatch_exception(
    exc: BaseException,
    *,
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    actor_config: ActorConfigLike,
    max_retry_backoff: timedelta,
    consumer_span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None",
    worker_pool: "asyncpg.Pool | None",
    settings: WorkerSettings | None,
    redis_client: "redis_async.Redis | None",
    pre_handler: Callable[[], None] | None = None,
    error_reporter: ErrorReporter | None = None,
    text: ExceptionText | None = None,
    disowned_jobs: "set[UUID] | None" = None,
) -> AttemptOutcome:
    """Route *exc* to the appropriate terminal handler via ``_run_terminal_path``.

    Consolidates the 6 exception handler blocks that were duplicated between
    ``consume_one_job`` and ``_consume_transactional``.  When *pre_handler*
    is provided (transactional path), it is called before each handler to
    discard the sub-enqueue buffer.

    *error_reporter* is forwarded to each handler so it can invoke
    :func:`~taskq.obs.invoke_error_reporter` alongside
    :func:`~taskq.retry.invoke_on_retry_exhausted` when a job reaches a
    terminal failure state.

    *text* is the rendering the ``attempt.N`` span already made of *exc*
    when the exception escaped it (the autonomous path); the handlers that
    report a traceback reuse it rather than rendering a second time. The
    transactional path catches inside the span and passes none, and only
    those handlers render — a snooze never pays for a traceback.

    *disowned_jobs* is the worker's disowned set, handed to
    ``_run_terminal_path`` for the exhausted-write path.
    """
    from taskq.worker._consumer import _run_terminal_path

    if pre_handler is not None:
        pre_handler()

    if isinstance(exc, TimeoutError):
        return await _run_terminal_path(
            job=job,
            worker_id=worker_id,
            progress_buffers=progress_buffers,
            worker_pool=worker_pool,
            settings=settings,
            redis_client=redis_client,
            disowned_jobs=disowned_jobs,
            job_log=log,
            handler=_handle_timeout,
            handler_args=(
                backend,
                job,
                worker_id,
                exc,
                actor_config,
                max_retry_backoff,
                consumer_span,
                log,
            ),
            handler_kwargs={"error_reporter": error_reporter, "text": text},
            status="failed",
            terminal=True,
            outcome="failed",
            job_exc=exc,
        )

    if isinstance(exc, Snooze):
        return await _run_terminal_path(
            job=job,
            worker_id=worker_id,
            progress_buffers=progress_buffers,
            worker_pool=worker_pool,
            settings=settings,
            redis_client=redis_client,
            disowned_jobs=disowned_jobs,
            job_log=log,
            handler=_handle_snooze,
            handler_args=(backend, job, worker_id, exc, consumer_span, log, actor_config),
            handler_kwargs={"error_reporter": error_reporter},
            status="scheduled",
            terminal=False,
            outcome="scheduled",
            job_exc=exc,
        )

    if isinstance(exc, RetryAfter):
        return await _run_terminal_path(
            job=job,
            worker_id=worker_id,
            progress_buffers=progress_buffers,
            worker_pool=worker_pool,
            settings=settings,
            redis_client=redis_client,
            disowned_jobs=disowned_jobs,
            job_log=log,
            handler=_handle_retry_after,
            handler_args=(backend, job, worker_id, exc, consumer_span, log, actor_config),
            handler_kwargs={"error_reporter": error_reporter},
            status="scheduled",
            terminal=False,
            outcome="scheduled",
            job_exc=exc,
        )

    if isinstance(exc, ReservationUnavailable):
        return await _run_terminal_path(
            job=job,
            worker_id=worker_id,
            progress_buffers=progress_buffers,
            worker_pool=worker_pool,
            settings=settings,
            redis_client=redis_client,
            disowned_jobs=disowned_jobs,
            job_log=log,
            handler=_handle_reservation_class_denied,
            handler_args=(backend, job, worker_id, exc, consumer_span, log, actor_config),
            handler_kwargs={
                "awaiting_prefix": "reservation:",
                "outcome": "reservation_denied",
                "debug_event": "consume-reservation-denied-noop",
                "error_reporter": error_reporter,
            },
            status="scheduled",
            terminal=False,
            outcome="scheduled",
            job_exc=exc,
        )

    return await _run_terminal_path(
        job=job,
        worker_id=worker_id,
        progress_buffers=progress_buffers,
        worker_pool=worker_pool,
        settings=settings,
        redis_client=redis_client,
        disowned_jobs=disowned_jobs,
        job_log=log,
        handler=_handle_generic_exception,
        handler_args=(
            backend,
            job,
            worker_id,
            exc,
            actor_config,
            max_retry_backoff,
            consumer_span,
            log,
        ),
        handler_kwargs={"error_reporter": error_reporter, "text": text},
        status="failed",
        terminal=True,
        outcome="failed",
        job_exc=exc,
    )
