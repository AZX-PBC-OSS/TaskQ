"""``asyncio.shield`` that retrieves a detached inner outcome on cancellation.

Leaf module by design (mirrors ``taskq._close``): imports only asyncio and
``taskq.obs``, so both the worker and backend packages can depend on it
without a layering inversion.

Why this exists: ``asyncio.shield`` leaves the inner awaitable running
detached when the OUTER await is cancelled, the cancellation is delivered
to the waiter, not to the inner task. If a second ``CancelledError`` lands
while that detached inner is still running (a double cancel: shutdown
racing a job cancellation, a force-cancel escalating over a cooperative
one), its eventual failure is retrieved by nobody, asyncio reports
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

:func:`shield_until_complete` is the other half of the same decision. A
write that is the row's only remaining writer must not be detached at all:
the teardown that closes its pool is ordered after the caller, so this one
absorbs the cancellation, finishes the write, and re-raises afterwards.
"""

import asyncio
from collections.abc import Awaitable

from taskq.obs import get_logger

__all__ = ["shield_until_complete", "shield_with_retrieval"]

logger = get_logger(__name__)


def _log_detached_failure(task: "asyncio.Task[object]") -> None:
    """Retrieve a detached shield task's outcome, logging any failure.

    Nobody awaits the detached task afterwards, so its outcome must be
    retrieved here or asyncio reports "Task exception was never
    retrieved", but unlike the minimal consumer-side variant this also
    LOGS the exception: the failure is a real infra signal, not noise.
    ``task.exception()`` raises ``CancelledError`` when the task ended
    cancelled, the only outcome suppressed here (asyncio already exempts
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


async def shield_until_complete[T](aw: Awaitable[T]) -> T:
    """Await *aw* to completion, holding a cancellation until it finishes.

    ``shield_with_retrieval`` detaches: a cancellation returns control to
    the caller at once and leaves the inner operation running with nobody
    left to wait for it. That is right for a write whose owner is finished
    either way. It is wrong for an operation whose caller is the row's only
    writer *and* whose connection is closed by a teardown ordered after that
    caller: the teardown closes the pool mid-write, and the write that was
    supposed to settle the row fails against it.

    Here the cancellation is absorbed instead of detaching. The inner
    operation is awaited to completion and the cancellation is re-raised
    only afterwards, so whoever owns the caller (a ``TaskGroup`` join that
    completes before the deps teardown closes the pools) also owns the whole
    operation.

    The wait is bounded by the operation's own timeouts, not by this helper:
    finishing means finishing. A failure the inner operation raises after the
    cancellation is retrieved and logged (the caller is already unwinding and
    cannot receive it) rather than left for asyncio to report as never
    retrieved, or re-raised as an exception the cancelled caller never asked
    for. The non-cancelled path is plain ``asyncio.shield`` semantics: the
    result is returned and an inner exception re-raises to the caller.
    """
    task: asyncio.Task[T] = asyncio.ensure_future(aw)
    cancellation: asyncio.CancelledError | None = None
    while True:
        # Why wait() and not shield(): wait never surfaces the operation's own
        # outcome, so a failure it raises after the cancellation cannot escape
        # in place of the cancellation the caller's task already owes. It also
        # leaves the operation running when this await is cancelled, which is
        # what gives the loop below another pass to wait on.
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as exc:
            cancellation = exc
            continue
        break
    if cancellation is None:
        return task.result()
    if not task.cancelled():
        _log_detached_failure(task)
    raise cancellation


async def shield_with_retrieval[T](aw: Awaitable[T]) -> T:
    """Await *aw* under ``asyncio.shield``, retrieving the inner outcome if detached.

    Behaves exactly like ``await asyncio.shield(aw)``: the inner awaitable
    keeps running when the outer await is cancelled, and its result or
    exception is delivered to the caller on the normal path. If the outer
    await IS cancelled, the still-running inner task is left detached with
    a done-callback that retrieves (and logs) its eventual outcome, see
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
