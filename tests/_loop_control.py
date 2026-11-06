"""The one lifecycle owner for a test's long-running loop tasks.

Any test that starts a registered long-running loop (a sweep loop, a
leader's run, the wake/cron/watchdog/election/prune loops, the heartbeat
and notify loops) starts it through :func:`managed_loop`. The supervisor
owns the task: it runs the test body, stops the loop through its
shutdown event, waits a bounded grace, cancels only as a fallback, and
preserves the body's primary exception over any cleanup failure. The
grace must sit above the dispatcher command timeout (5s by default) so
a loaded release finishes inside it; the default is 30s.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

GRACE_SECONDS = 30.0


@dataclass
class ManagedLoop:
    """The supervised loop's task and its shutdown event."""

    task: asyncio.Task[object]
    shutdown: asyncio.Event


@contextlib.asynccontextmanager  # pyright: ignore[reportDeprecated]  # Why: the stdlib cm factory is the test-utility shape the suite's other async CMs use.
async def managed_loop(
    loop_fn: Callable[[asyncio.Event], Awaitable[object]],
    *,
    grace: float = GRACE_SECONDS,
    drain: Callable[[], Awaitable[object]] | None = None,
    name: str | None = None,
) -> AsyncIterator[ManagedLoop]:
    """Start ``loop_fn(shutdown)`` as a task and stop it on exit.

    On exit the shutdown event is set and the loop gets *grace* seconds
    to finish its current tick and exit on its own. Only if the grace
    expires is the task cancelled, and then a hard short bound waits for
    the cancellation to land: a loop that swallows it is reported as a
    survivor (the caller's drain, or the guard's, contains it). The
    body's primary exception (including a TimeoutError the loop task
    raised itself) wins over any cleanup failure: a cleanup error is
    surfaced only when the body raised nothing. The supervisor does not
    retrieve a detached shielded release; the caller-supplied *drain*
    (for a pool the test owns) runs after the stop and is bounded and
    suppressed when the body failed, so the primary error is not masked.
    """
    shutdown = asyncio.Event()
    task = asyncio.create_task(loop_fn(shutdown), name=name)
    body_exc: BaseException | None = None
    try:
        yield ManagedLoop(task=task, shutdown=shutdown)
    except BaseException as exc:
        body_exc = exc
        raise
    finally:
        shutdown.set()
        done, _pending_after_grace = await asyncio.wait({task}, timeout=grace)
        if done:
            if not task.cancelled():
                # The loop ended on its own; its exception (a TimeoutError
                # it raised itself included) is surfaced unless the body's
                # error takes precedence.
                try:
                    task.result()
                except BaseException:
                    if body_exc is None:
                        raise
        else:
            # The grace expired: cancel, then a hard short bound. A loop
            # that swallows the cancellation is reported as a survivor;
            # the guard's drain contains it.
            task.cancel()
            _, still = await asyncio.wait({task}, timeout=2.0)
            if still and body_exc is None:
                raise RuntimeError(
                    f"the supervised loop {task.get_name()!r} did not finish "
                    "within its grace and ignored its cancellation"
                )
        if drain is not None:
            if body_exc is None:
                await asyncio.wait_for(drain(), timeout=grace)
            else:
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(drain(), timeout=grace)
