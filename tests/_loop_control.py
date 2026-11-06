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
from contextvars import Context, ContextVar
from dataclasses import dataclass

GRACE_SECONDS = 30.0

#: The task that registered a done callback, carried into the callback's
#: execution so a task the callback creates descends from it. Callbacks
#: run without a current task; without this the factory would record no
#: parent for a callback-spawned child.
_callback_owner: ContextVar[asyncio.Task[object] | None] = ContextVar(  # type: ignore[misc]  # Why: the contextvar's default is None; the declared value type is the owning Task.
    "tq_callback_owner", default=None
)


@dataclass
class ManagedLoop:
    """The supervised loop's task and its shutdown event."""

    task: asyncio.Task[object]
    shutdown: asyncio.Event


class _ParentLinkTask(asyncio.Task[object]):
    """A Task that records its creator and propagates callback ownership.

    ``_tq_parent`` is the task that created this one, or the task whose
    done callback created it. The conftest leak guard's drain walks the
    links to scope child discovery to the call-owned tree."""

    def __init__(
        self,
        coro: asyncio.Future[object] | Awaitable[object],
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        name: str | None = None,
        context: Context | None = None,
    ) -> None:
        super().__init__(coro, loop=loop, name=name, context=context)
        owner: asyncio.Task[object] | None
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            owner = None
        if owner is None:
            owner = _callback_owner.get()
        self._tq_parent = owner  # type: ignore[attr-defined]  # Why: Task exposes no parent link; the drain's ownership needs the creator chain and this is the one place every task creation passes through.

    def add_done_callback(
        self,
        callback: Callable[[asyncio.Task[object]], object],
        *,
        context: Context | None = None,
    ) -> None:
        try:
            registering: asyncio.Task[object] | None = asyncio.current_task()
        except RuntimeError:
            registering = None
        if registering is None:
            registering = _callback_owner.get()
        if registering is None:
            # No owning task anywhere (the loop's own machinery): the
            # callback runs unwrapped.
            super().add_done_callback(callback, context=context)
            return

        def _owned(fut: asyncio.Task[object]) -> None:
            token = _callback_owner.set(registering)
            try:
                callback(fut)
            finally:
                _callback_owner.reset(token)

        super().add_done_callback(_owned, context=context)


def install_parent_link_factory(loop: asyncio.AbstractEventLoop) -> None:
    """Install, once per loop, the parent-link task factory. The conftest
    leak guard's drain walks the links to scope child discovery to the
    call-owned tree; a task created outside a running task and outside a
    call-owned callback records no parent."""
    if getattr(loop, "_tq_parent_link_installed", False):
        return
    loop.set_task_factory(_parent_link_factory)
    loop._tq_parent_link_installed = True  # type: ignore[attr-defined]  # Why: the idempotence marker lives on the loop; the module loops are plain asyncio loops.


def _parent_link_factory(
    loop: asyncio.AbstractEventLoop,
    coro: asyncio.Future[object] | Awaitable[object],
    *,
    name: str | None = None,
    context: Context | None = None,
) -> _ParentLinkTask:
    """The task factory the supervisor list's loops run under: every task
    gets the parent link (see ``_ParentLinkTask``)."""
    return _ParentLinkTask(coro, loop=loop, name=name, context=context)


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
