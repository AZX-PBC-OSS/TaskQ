"""The lifecycle owner for the converted drivers' long-running loops.

Tests that start a registered long-running loop (the sweep family) start
it through :func:`managed_loop`. The supervisor owns the task: it runs
the test body, stops the loop through its shutdown event, waits a
bounded grace, cancels only as a fallback, and preserves the body's
primary exception over any cleanup failure. The grace must sit above
the dispatcher command timeout (5s by default) so a loaded release
finishes inside it; the default is 30s. This module's drivers are the
converted ones, not every long-running loop in the suite; the conftest
leak guard is the containment for the rest.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextvars import Context
from dataclasses import dataclass

GRACE_SECONDS = 30.0


@dataclass
class ManagedLoop:
    """The supervised loop's task and its shutdown event."""

    task: asyncio.Task[object]
    shutdown: asyncio.Event


def install_parent_link_factory(loop: asyncio.AbstractEventLoop) -> None:
    """Install, once per loop, a task factory that records each task's
    creator on ``task._tq_parent``. The conftest leak guard's drain walks
    these links to scope child discovery to the call-owned tree; a task
    created outside a running task (a sync ``run_until_complete`` path)
    records no parent."""
    if getattr(loop, "_tq_parent_link_installed", False):
        return

    def _parent_link_factory(
        loop: asyncio.AbstractEventLoop,
        coro: asyncio.Future[object] | Awaitable[object],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[object]:
        task = asyncio.Task(coro, loop=loop, name=name, context=context)  # type: ignore[arg-type]
        parent: asyncio.Task[object] | None
        try:
            parent = asyncio.current_task()
        except RuntimeError:
            parent = None
        task._tq_parent = parent  # type: ignore[attr-defined]  # Why: Task exposes no parent link; the drain's ownership needs the creator chain and this is the one place every task creation passes through.
        return task

    loop.set_task_factory(_parent_link_factory)
    loop._tq_parent_link_installed = True  # type: ignore[attr-defined]  # Why: the idempotence marker lives on the loop; the module loops are plain asyncio loops.


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
    surfaced only when the body raised nothing. The caller-supplied
    *drain* (for a pool the test owns) runs on every stop path, before a
    survivor is reported: the fallback is the path that detaches a
    shielded release, so the drain must not be skipped there. The
    supervisor does not itself retrieve a detached shielded release; the
    drain does. It is bounded, and suppressed when the body failed so
    the primary error is not masked.
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
        survivor = False
        loop_exc: BaseException | None = None
        done, _pending_after_grace = await asyncio.wait({task}, timeout=grace)
        if done:
            if not task.cancelled():
                # The loop ended on its own; its exception (a TimeoutError
                # it raised itself included) is surfaced unless the body's
                # error takes precedence. Captured so the caller's drain
                # still runs below.
                try:
                    task.result()
                except BaseException as exc:
                    loop_exc = exc
        else:
            # The grace expired: cancel, then a hard short bound. A loop
            # that swallows the cancellation is reported as a survivor;
            # the guard's drain contains it.
            task.cancel()
            _, still = await asyncio.wait({task}, timeout=2.0)
            survivor = bool(still)
        # The caller's drain runs on every path: the fallback is the path
        # that detached a shielded release, and a loop error must not
        # skip it either.
        if drain is not None:
            if body_exc is None and loop_exc is None:
                await asyncio.wait_for(drain(), timeout=grace)
            else:
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(drain(), timeout=grace)
        if body_exc is None and loop_exc is not None:
            raise loop_exc
        if survivor and body_exc is None:
            raise RuntimeError(
                f"the supervised loop {task.get_name()!r} did not finish "
                "within its grace and ignored its cancellation"
            )
