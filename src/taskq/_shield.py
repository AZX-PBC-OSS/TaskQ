"""``asyncio.shield`` that retrieves a detached inner outcome on cancellation.

Leaf module by design (mirrors ``taskq._close``): imports only asyncio and
``taskq.obs``, so both the worker and backend packages can depend on it
without a layering inversion.

Why this exists: ``asyncio.shield`` leaves the inner awaitable running
detached when the OUTER await is cancelled — the cancellation is delivered
to the waiter, not to the inner task. If a second ``CancelledError`` lands
while that detached inner is still running (a double cancel: shutdown
racing a job cancellation, a force-cancel escalating over a cooperative
one), its eventual failure is retrieved by nobody — asyncio reports
"Task exception was never retrieved" and the infra error is lost.
:func:`shield_with_retrieval` generalizes the
``_retrieve_detached_outcome`` pattern from ``taskq.worker._consumer``:
on the cancellation path it attaches a done-callback that retrieves the
detached outcome and logs the inner exception, so the signal stays visible
in the logs.

The non-cancelled path is byte-for-byte ``asyncio.shield`` semantics: the
inner result is returned, and an inner exception re-raises to the awaiting
caller (the retrieval callback is only attached once cancellation has
already been seen, so the caller's own handling of a propagated inner
exception is never double-logged).
"""

import asyncio
from collections.abc import Awaitable

from taskq.obs import get_logger

__all__ = ["shield_with_retrieval"]

logger = get_logger(__name__)


def _log_detached_failure(task: "asyncio.Task[object]") -> None:
    """Retrieve a detached shield task's outcome, logging any failure.

    Nobody awaits the detached task afterwards, so its outcome must be
    retrieved here or asyncio reports "Task exception was never
    retrieved" — but unlike the minimal consumer-side variant this also
    LOGS the exception: the failure is a real infra signal, not noise.
    ``task.exception()`` raises ``CancelledError`` when the task ended
    cancelled — the only outcome suppressed here (asyncio already exempts
    cancelled tasks from the retrieval warning, and there is no error to
    surface).
    """
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    if exc is not None:
        logger.warning(
            "shield-detached-task-failed",
            error_type=type(exc).__name__,
            error=repr(exc),
            exc_info=exc,
        )


async def shield_with_retrieval[T](aw: Awaitable[T]) -> T:
    """Await *aw* under ``asyncio.shield``, retrieving the inner outcome if detached.

    Behaves exactly like ``await asyncio.shield(aw)``: the inner awaitable
    keeps running when the outer await is cancelled, and its result or
    exception is delivered to the caller on the normal path. If the outer
    await IS cancelled, the still-running inner task is left detached with
    a done-callback that retrieves (and logs) its eventual outcome — see
    the module docstring for the double-cancel race this closes.
    """
    # Why an explicit task: the handle is needed on the cancellation path
    # to attach the retrieval callback (plain shield would wrap the
    # coroutine in an anonymous future with no reachable handle).
    task: asyncio.Task[T] = asyncio.ensure_future(aw)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        task.add_done_callback(_log_detached_failure)
        raise
