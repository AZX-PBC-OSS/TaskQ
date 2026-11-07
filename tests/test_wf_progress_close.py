"""The progress emitter's close owns its cadence flush.

``ProgressEmitter.aclose`` waits a bounded second for the in-flight
cadence flush and then writes the buffer once more itself. The pre-cure
code awaited ``asyncio.shield(self._flush_task)``: on the timeout,
``wait_for`` cancelled only the shield wrapper, the emitter-owned
cadence task stayed live (parked in pool acquisition), and it could
complete after the attempt's finalization and write outside the
attempt. The cure removes the shield: ``wait_for``'s timeout and the
caller's cancellation now cancel and reap the cadence task itself (a
task awaiting a task delivers its cancellation into the inner task and
does not resume until the inner task has finished, so the flush task is
always done when ``aclose`` returns or propagates), and the final
``_flush_once`` is the only writer after the in-flight wait (the bound
covers each wait; the cancellation's completion may add unwind time
past it). The pins
use the real emitter with a fail-closed acquire fake, so no database is
involved.
"""

from __future__ import annotations

import asyncio
import contextlib

from taskq._ids import new_uuid
from taskq.workflows._progress import ProgressEmitter


class _BlockedAcquire:
    """A fail-closed acquire: parks until released; a release instead of
    a cancellation means the flush under test escaped its close, and the
    context fails closed (it never yields a connection)."""

    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        self._started = started
        self._release = release

    async def __aenter__(self) -> object:
        self._started.set()
        await self._release.wait()
        raise AssertionError("the acquire was released instead of cancelled")

    async def __aexit__(self, *exc: object) -> None:
        return None


class _BlockedPool:
    """The pool whose acquisition is the flush's blocker."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def acquire(self) -> _BlockedAcquire:
        return _BlockedAcquire(self.started, self.release)


def _emitter(pool: _BlockedPool) -> ProgressEmitter:
    return ProgressEmitter(
        pool,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        flow_id=new_uuid(),  # type: ignore[arg-type]
        node_id=new_uuid(),  # type: ignore[arg-type]
        cadence_s=0,
    )


async def _reap(emitter: ProgressEmitter, pool: _BlockedPool) -> None:
    """The pins' own containment: release the blocked acquire and reap
    the cadence task whatever the emitter's close did, so a red run
    leaks nothing onto the loop."""
    pool.release.set()
    task = emitter._flush_task
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        with contextlib.suppress(asyncio.CancelledError, AssertionError):
            await task


async def test_aclose_timeout_cancels_and_reaps_the_inflight_flush() -> None:
    """The in-flight wait's timeout must cancel and reap the emitter's
    cadence task, not shield-detach it: when ``aclose`` returns after
    its bounded wait (the cancellation's completion may add unwind time
    past the bound), the flush task is done (cancelled), its outcome is
    consumed, and no write ever landed without a connection. The final
    ``_flush_once`` stays the last writer; the bounded best-effort
    behavior (the timeout, the lost-flush accounting) is unchanged."""
    pool = _BlockedPool()
    emitter = _emitter(pool)
    try:
        await emitter.emit(25, "in flight", None)
        flush_task = emitter._flush_task
        assert flush_task is not None, "the cadence flush was not armed"
        await asyncio.wait_for(pool.started.wait(), timeout=5.0)
        started = asyncio.get_running_loop().time()
        await emitter.aclose()
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed >= 0.9, "the in-flight close timeout did not fire"
        assert flush_task.done(), "close returned with its cadence flush still live"
        assert flush_task.cancelled(), (
            "the in-flight flush was detached from the close instead of cancelled"
        )
        assert emitter.appended == 0, "no write may land without a connection"
        with contextlib.suppress(asyncio.CancelledError):
            flush_task.exception()  # retrieved: no unretrieved-task diagnostic
    finally:
        await _reap(emitter, pool)


async def test_aclose_caller_cancellation_cancels_and_reaps_the_inflight_flush() -> None:
    """The caller's cancellation during the bounded wait must not
    shield-detach the cadence task either: the cancellation propagates
    into the flush task, ``aclose`` re-raises it, and no detached writer
    outlives the close."""
    pool = _BlockedPool()
    emitter = _emitter(pool)
    try:
        await emitter.emit(25, "in flight", None)
        flush_task = emitter._flush_task
        assert flush_task is not None, "the cadence flush was not armed"
        await asyncio.wait_for(pool.started.wait(), timeout=5.0)
        closer = asyncio.create_task(emitter.aclose())
        await asyncio.sleep(0.1)  # the close is parked in its bounded wait
        closer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await closer
        assert flush_task.done(), (
            "the close left its cadence flush live across the caller's cancellation"
        )
        assert flush_task.cancelled(), (
            "the caller's cancellation detached the flush task instead of cancelling it"
        )
        assert emitter.appended == 0, "no write may land without a connection"
    finally:
        await _reap(emitter, pool)
