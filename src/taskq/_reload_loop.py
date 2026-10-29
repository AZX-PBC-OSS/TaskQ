"""The rebuild loop behind a :class:`~taskq.auth.ReloadSchedule`.

Used by the consumers that rebuild provider-backed pools but have no
worker coordinator of their own: ``taskq ui serve`` and
:class:`taskq.TaskQ`. The worker keeps its own loop
(``taskq.worker._bootstrap._reload_coordinator_loop``), which has to
interleave with shutdown phases and DI refreshes; the trigger and
coalescing semantics here are the same as its, so an operator's SIGHUP
means one thing everywhere.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from taskq._reaper import reap_cancelled_child
from taskq.auth import ReloadSchedule
from taskq.obs import get_logger

logger = get_logger(__name__)

__all__ = ["run_reload_schedule"]


async def run_reload_schedule(
    schedule: ReloadSchedule,
    rebuild: Callable[[], Awaitable[None]],
    *,
    trigger: asyncio.Event,
    role: str,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Call *rebuild* on every *trigger* and on the schedule's interval, until cancelled.

    *trigger* is the on-demand path (a SIGHUP handler, a programmatic
    request). It is cleared **before** each rebuild and never after, so a
    request arriving mid-rebuild - whether that rebuild succeeds or fails -
    is honoured by exactly one follow-up; N requests during one rebuild
    coalesce into one. The interval is re-read from *schedule* on every
    wait, so a lease that comes back shorter after a rebuild tightens the
    cadence from the next wait on.

    A rebuild that raises is logged (``credentials-reload-failed``) and the
    loop continues: the resources it would have replaced are still live,
    and the next tick or trigger retries. *sleep* is the clock seam - tests
    drive the loop with a fake one.

    Ends only by cancellation (the owner's shutdown).
    """

    async def _wait(seconds: float) -> None:
        await sleep(seconds)

    while True:
        interval = schedule.interval
        waiters: list[asyncio.Task[Any]] = [asyncio.create_task(trigger.wait())]
        if interval is not None:
            waiters.append(asyncio.create_task(_wait(interval)))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            # THE TEARDOWN'S OWN ORDER (the leaked-_wait cure, found by the
            # leaked-task guard's own teeth): CANCEL ALL the pending
            # waiters first, THEN reap each. The previous shape cancelled
            # and reaped one waiter per lap — the reaper's own concurrent
            # cancellation (this loop's shutdown cut arriving mid-reap)
            # re-raised out of the FIRST reap and orphaned the remaining
            # waiters: a cancelled-not-awaited trigger.wait and a STILL-
            # RUNNING _wait, leaking the parked sleeper into later tests
            # (the leaked-task guard's conviction, 3 errors on the fast
            # tier). The reaper's law is untouched: our OWN cancellation
            # still propagates — after the bounded reap completes, never
            # swallowed, only not allowed to orphan the children.
            pending = [t for t in waiters if not t.done()]
            for task in pending:
                task.cancel()
            own: asyncio.CancelledError | None = None
            for task in pending:
                try:
                    await reap_cancelled_child(task)
                except asyncio.CancelledError as exc:
                    # the reaper re-raised OUR cancel (me.cancelling()):
                    # noted — the remaining children are reaped first.
                    own = exc
            if own is not None:
                raise own

        cause = "trigger" if trigger.is_set() else "schedule"
        trigger.clear()

        started = time.perf_counter()
        try:
            await rebuild()
        except Exception as exc:
            logger.error(
                "credentials-reload-failed",
                role=role,
                cause=cause,
                error_type=type(exc).__name__,
                error=str(exc),
                duration_ms=round((time.perf_counter() - started) * 1000.0, 1),
                note="the resources this would have replaced are still serving; "
                "the next tick or SIGHUP retries",
            )
            continue
        logger.info(
            "credentials-reloaded",
            role=role,
            cause=cause,
            duration_ms=round((time.perf_counter() - started) * 1000.0, 1),
            next_interval=schedule.interval,
        )
