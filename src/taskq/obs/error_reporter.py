"""Vendor-neutral error reporting Protocol for terminal job failures.

The library emits OpenTelemetry spans/metrics/logs for all observability, but
some error-routing workflows (e.g. DLQ forwarding to Sentry, custom alerting)
need a hook that runs when a job reaches a terminal failure state.  Users
implement the :class:`ErrorReporter` Protocol and register it as a DI provider
— the library never imports vendor SDKs.

See :mod:`taskq.obs` for the full observability surface.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import structlog

from taskq.obs._otel import record_error_reporter_failure

if TYPE_CHECKING:
    from taskq.backend._protocol import JobRow

__all__ = [
    "ErrorReporter",
    "ErrorReporterType",
    "NullErrorReporter",
    "invoke_error_reporter",
]

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.obs.error_reporter")


@runtime_checkable
class ErrorReporter(Protocol):
    """Vendor-neutral hook for routing terminal job failures to external systems.

    Implementations capture the error and job row, then forward to a
    vendor-specific backend (Sentry, Datadog, a DLQ, etc.).  The library
    calls :meth:`report` when a job reaches a terminal failure state —
    either because retries were exhausted or because the error was
    non-retryable.

    The call is wrapped in a try/except by :func:`invoke_error_reporter`;
    a failing reporter never crashes the worker.  Reporter failures are
    counted on the ``taskq.error_reporter.failures`` counter with a
    ``reporter_type`` attribute.

    Register an :class:`ErrorReporter` instance as a DI provider
    (``registry.register_value(ErrorReporter, Scope.PROCESS, my_reporter)``)
    or pass it directly to the worker bootstrap.
    """

    async def report(self, job: JobRow, exception: BaseException) -> None: ...


class NullErrorReporter:
    """Default no-op :class:`ErrorReporter` — silently drops all reports.

    Used when no vendor-specific error routing is configured.  Instances
    are stateless and safe to share.
    """

    async def report(self, job: JobRow, exception: BaseException) -> None:
        return None


type ErrorReporterType = ErrorReporter
"""Structural type alias for DI registration and parameter annotations.

Use :class:`ErrorReporter` directly in most contexts; this alias is provided
for ``register_value(ErrorReporterType, ...)`` calls where a distinct type
object is needed for the registry key.
"""


async def invoke_error_reporter(
    reporter: ErrorReporter | None,
    job: JobRow,
    exception: BaseException,
    timeout: float = 3.0,  # noqa: ASYNC109  Why: parameter name matches the on_retry_exhausted/invoke_on_success convention; asyncio.wait_for requires a timeout value, not asyncio.timeout() context manager
    *,
    log: structlog.stdlib.BoundLogger | None = None,
) -> None:
    """Invoke *reporter*.report(), swallowing and counting failures.

    A ``None`` reporter is treated as a no-op (equivalent to
    :class:`NullErrorReporter`).  Exceptions from ``report()`` are caught,
    logged at WARNING, and counted on the ``taskq.error_reporter.failures``
    counter — they never propagate to the caller.

    This mirrors the defensive pattern of
    :func:`~taskq.retry.invoke_on_retry_exhausted`: a user-supplied hook
    must never crash the worker's terminal-write path.
    """
    if reporter is None:
        return

    logger: structlog.stdlib.BoundLogger = log if log is not None else _log
    reporter_type = type(reporter).__name__

    try:
        await asyncio.wait_for(reporter.report(job, exception), timeout=timeout)
    except TimeoutError:
        logger.warning(
            "error-reporter-timeout",
            job_id=job.id,
            actor=job.actor,
            reporter_type=reporter_type,
            timeout_seconds=timeout,
        )
    except Exception as exc:
        logger.warning(
            "error-reporter-failed",
            job_id=job.id,
            actor=job.actor,
            reporter_type=reporter_type,
            error=repr(exc),
        )
        record_error_reporter_failure(reporter_type)
