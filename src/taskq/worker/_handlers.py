"""Terminal exception handlers for the consumer.

The six exception-to-terminal-state routers (timeout, snooze, retry_after,
reservation denied, generic exception) live here.  Each handler maps a
raised exception to the appropriate backend terminal write, span event,
and structured log entry.

:func:`_dispatch_exception` consolidates the exception dispatch logic
shared between ``consume_one_job`` and ``_consume_transactional``.
"""

import asyncio
import traceback
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace

from taskq.backend._protocol import (
    AttemptOutcome as BackendAttemptOutcome,
)
from taskq.backend._protocol import (
    Backend,
    ErrorInfo,
    JobRow,
)
from taskq.backend.clock import Clock
from taskq.exceptions import (
    ReservationUnavailable,
    RetryAfter,
    Snooze,
)
from taskq.obs import ErrorReporter, invoke_error_reporter, log_state_change
from taskq.retry import (
    ActorConfigLike,
    JobRetryState,
    Retry,
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
]

__all__ = [
    "_TerminalWriteFailed",
    "_dispatch_exception",
    "_handle_generic_exception",
    "_handle_reservation_class_denied",
    "_handle_retry_after",
    "_handle_snooze",
    "_handle_timeout",
    "_log_terminal_write_failed",
]

# Infra failures during the terminal-write itself (DB connection drop,
# timeout acquiring a pool connection, socket errors) — as opposed to the
# actor's own exception, which is what decide_after_failure/error_info
# describe. These must NOT be treated as "the actor failed with this
# exception": doing so would overwrite the real error_info and re-run the
# retry decision against the wrong exception type. See
# `_log_terminal_write_failed`.
_TERMINAL_WRITE_INFRA_EXCEPTIONS: tuple[type[BaseException], ...] = (
    asyncpg.PostgresError,
    OSError,
    TimeoutError,
)


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


def _log_terminal_write_failed(
    log: structlog.stdlib.BoundLogger,
    job: JobRow,
    job_exc: BaseException | None,
    infra_exc: BaseException,
) -> None:
    """Log a terminal-write infra failure without mutating job state.

    The job row is left in ``running`` — no mark_failed/mark_succeeded
    write happened — so lock-lease expiry and the crash sweep reclaim it
    for retry. This is intentionally NOT re-dispatched into
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
        job_id=job.id,
        actor=job.actor,
        actor_succeeded=job_exc is None,
        job_error_class=type(job_exc).__name__ if job_exc is not None else None,
        job_error_message=str(job_exc) if job_exc is not None else None,
        infra_error_class=type(infra_exc).__name__,
        infra_error_message=str(infra_exc),
    )


async def _handle_timeout(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    actor_config: ActorConfigLike,
    clock: Clock,
    max_retry_backoff: timedelta,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    error_reporter: ErrorReporter | None = None,
) -> None:
    error_info = ErrorInfo(
        error_class="TimeoutError",
        error_message="start_to_close",
        error_traceback=None,
    )
    job_state = JobRetryState(
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        retry_kind=job.retry_kind,
        schedule_to_close=job.schedule_to_close,
        start_to_close=job.start_to_close,
    )
    decision = decide_after_failure(
        actor_config,
        TimeoutError("start_to_close"),
        job_state,
        clock.now(),
        max_retry_backoff=max_retry_backoff,
    )
    if isinstance(decision, Retry):
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "error_class": "TimeoutError",
            },
        )
        await asyncio.shield(
            safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                decision.next_scheduled_at,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
            )
        )
    else:
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": "TimeoutError",
            },
        )
        updated_row = await asyncio.shield(
            safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                None,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
            )
        )
        if updated_row is not None:
            await invoke_on_retry_exhausted(
                actor_config.on_retry_exhausted,
                updated_row,
                TimeoutError("start_to_close"),
                actor_config.on_retry_exhausted_timeout,
                log=log,
            )
            await invoke_error_reporter(
                error_reporter,
                updated_row,
                TimeoutError("start_to_close"),
                log=log,
            )


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
) -> None:
    raw_count = (job.metadata or {}).get("snooze_count", 0)
    current_snooze_count: int = int(raw_count) if isinstance(raw_count, (int, str)) else 0
    tri = await asyncio.shield(
        backend.mark_snoozed(
            job.id,
            worker_id,
            s.delay,
            metadata_update={"snooze_count": current_snooze_count + 1},
            progress_seq=progress_seq,
            progress_state=progress_state,
        )
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
    elif tri == "failed":
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": "DeadlineExceeded",
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="failed",
            cause="DeadlineExceeded",
        )
        await invoke_on_retry_exhausted(
            actor_config.on_retry_exhausted,
            job,
            TimeoutError("DeadlineExceeded"),
            actor_config.on_retry_exhausted_timeout,
            log=log,
        )
        await invoke_error_reporter(
            error_reporter,
            job,
            TimeoutError("DeadlineExceeded"),
            log=log,
        )
    else:
        log.debug(
            "consume-snooze-noop",
            from_state="running",
            to_state="noop",
            cause="Snooze",
        )


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
) -> None:
    tri = await asyncio.shield(
        backend.mark_retry_after(
            job.id,
            worker_id,
            r.delay,
            consume_budget=r.consume_budget,
            progress_seq=progress_seq,
            progress_state=progress_state,
        )
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
    elif tri in ("failed:DeadlineExceeded", "failed:MaxAttemptsExceeded"):
        cause = tri.split(":")[1]
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": cause,
            },
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
            job,
            exc,
            actor_config.on_retry_exhausted_timeout,
            log=log,
        )
        await invoke_error_reporter(error_reporter, job, exc, log=log)
    else:
        log.debug(
            "consume-retry-after-noop",
            from_state="running",
            to_state="noop",
            cause="RetryAfter",
        )


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
    outcome: BackendAttemptOutcome,
    debug_event: str,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    error_reporter: ErrorReporter | None = None,
) -> None:
    tri = await asyncio.shield(
        backend.mark_snoozed(
            job.id,
            worker_id,
            e.retry_after,
            metadata_update={"awaiting": f"{awaiting_prefix}{e.bucket_name}"},
            outcome=outcome,
            progress_seq=progress_seq,
            progress_state=progress_state,
        )
    )
    if tri == "scheduled":
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "bucket_name": e.bucket_name,
                "delay_seconds": e.retry_after.total_seconds(),
            },
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause="ReservationUnavailable",
            bucket_name=e.bucket_name,
            delay_seconds=e.retry_after.total_seconds(),
        )
    elif tri == "failed":
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": "DeadlineExceeded",
            },
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
            job,
            TimeoutError("DeadlineExceeded"),
            actor_config.on_retry_exhausted_timeout,
            log=log,
        )
        await invoke_error_reporter(
            error_reporter,
            job,
            TimeoutError("DeadlineExceeded"),
            log=log,
        )
    else:
        log.debug(
            debug_event,
            from_state="running",
            to_state="noop",
            cause="ReservationUnavailable",
        )


async def _handle_generic_exception(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    e: Exception,
    actor_config: ActorConfigLike,
    clock: Clock,
    max_retry_backoff: timedelta,
    span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    *,
    error_reporter: ErrorReporter | None = None,
) -> None:
    error_info = ErrorInfo(
        error_class=type(e).__name__,
        error_message=str(e),
        error_traceback=traceback.format_exc(),
    )
    log.error(
        "job_exception",
        job_id=str(job.id),
        actor=job.actor,
        attempt=job.attempt,
        error_class=error_info.error_class,
        error_message=error_info.error_message,
        error_traceback=error_info.error_traceback,
    )
    job_state = JobRetryState(
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        retry_kind=job.retry_kind,
        schedule_to_close=job.schedule_to_close,
        start_to_close=job.start_to_close,
    )
    decision = decide_after_failure(
        actor_config, e, job_state, clock.now(), max_retry_backoff=max_retry_backoff
    )
    if isinstance(decision, Retry):
        span.add_event(
            "lifecycle.scheduled",
            attributes={
                "from_state": "running",
                "to_state": "scheduled",
                "error_class": type(e).__name__,
            },
        )
        await asyncio.shield(
            safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                decision.next_scheduled_at,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
            )
        )
        log_state_change(
            log,
            from_state="running",
            to_state="scheduled",
            cause=type(e).__name__,
        )
    else:
        span.add_event(
            "lifecycle.failed",
            attributes={
                "from_state": "running",
                "to_state": "failed",
                "error_class": (
                    "DeadlineExceeded"
                    if decision.error_class == "DeadlineExceeded"
                    else type(e).__name__
                ),
            },
        )
        updated_row = await asyncio.shield(
            safe_mark_failed_or_retry(
                backend,
                job.id,
                worker_id,
                error_info,
                None,
                progress_seq=progress_seq,
                progress_state=progress_state,
                log=log,
            )
        )
        log_state_change(
            log,
            from_state="running",
            to_state="failed",
            cause=type(e).__name__,
            retryable=decision.retryable,
        )
        if updated_row is not None:
            await invoke_on_retry_exhausted(
                actor_config.on_retry_exhausted,
                updated_row,
                e,
                actor_config.on_retry_exhausted_timeout,
                log=log,
            )
            await invoke_error_reporter(error_reporter, updated_row, e, log=log)


async def _dispatch_exception(
    exc: BaseException,
    *,
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    actor_config: ActorConfigLike,
    clock: Clock,
    max_retry_backoff: timedelta,
    consumer_span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None",
    worker_pool: "asyncpg.Pool | None",
    settings: WorkerSettings | None,
    redis_client: "redis_async.Redis | None",
    pre_handler: Callable[[], None] | None = None,
    error_reporter: ErrorReporter | None = None,
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
            handler=_handle_timeout,
            handler_args=(
                backend,
                job,
                worker_id,
                actor_config,
                clock,
                max_retry_backoff,
                consumer_span,
                log,
            ),
            handler_kwargs={"error_reporter": error_reporter},
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
        handler=_handle_generic_exception,
        handler_args=(
            backend,
            job,
            worker_id,
            exc,
            actor_config,
            clock,
            max_retry_backoff,
            consumer_span,
            log,
        ),
        handler_kwargs={"error_reporter": error_reporter},
        status="failed",
        terminal=True,
        outcome="failed",
        job_exc=exc,
    )
