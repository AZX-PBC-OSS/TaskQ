"""Reap the loop tasks an abandoned SSE stream leaves behind, in-call.

Client-side port of ``_reap_stream_teardown_tasks`` from
``tests/web_progress/test_integration.py``, which proved the mechanism
against ``httpx.ASGITransport``; this copy serves e2e tests that stream
over a real socket (the same httpx generator chain, a real connection
instead of a buffered one).

Breaking out of ``resp.aiter_lines()`` abandons the suspended httpx
generator chain (aiter_lines -> aiter_text -> aiter_bytes -> aiter_raw ->
the response stream); CPython finalizes each abandoned generator through
the loop's asyncgen hook, which mints an ``async_generator_athrow`` task
per generator, one wave per loop turn, tail waves minting only after the
consuming frame dies. The tasks cannot be held by reference, so the reap
is a set diff against *baseline* (everything pending when the interaction
began) driven to quiescence: ``gc.collect()`` forces each wave's
finalization deterministically, and every outcome is retrieved - a
finalizer that died with a real exception is a finding, never a silent
give-up. Finalizer tasks are AWAITED, never cancelled: cancelling one
aborts the unwind mid-way and the cascade re-mints a wave later (measured
in the web_progress module). The sse-starlette ``_shutdown_watcher`` is
cancelled by name; a subprocess server's watcher lives on the subprocess
loop and cannot appear here, but the branch keeps this helper safe for
in-process ASGI streams too.

The reap must run while the test's consuming frame is already dead: a
frame still holding the abandoned chain keeps it reachable, and a
reachable generator is never finalized, so the cascade would mint its
tail waves after the reap gave up.
"""

import asyncio
import gc

__all__ = ["_reap_stream_teardown_tasks"]


async def _reap_stream_teardown_tasks(
    baseline: frozenset[asyncio.Task[object]],
    *,
    reap_timeout: float = 5.0,
) -> None:
    """Reap every task the stream interaction minted, INSIDE the call phase.

    See the module docstring for the two task shapes (the dropped
    sse-starlette shutdown watcher and the abandoned-``aiter_lines``
    finalization cascade) and why neither can be awaited by reference.
    Bounded by *reap_timeout*; loud (AssertionError) on survivors, so a
    stream teardown that does not reach quiescence fails the test instead
    of leaking into the next one.
    """
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    deadline = loop.time() + reap_timeout

    def _is_shutdown_watcher(task: asyncio.Task[object]) -> bool:
        return getattr(task.get_coro(), "__name__", None) == "_shutdown_watcher"

    async def _reap_one_wave(wave: list[asyncio.Task[object]]) -> None:
        """Await one cascade wave to completion, outcomes retrieved.

        Lives one frame below the driver loop ON PURPOSE: every task
        reference this function holds (including the completed finalizer
        tasks, whose coroutine pins the generator it just closed) dies
        when this frame returns, so the driver's next ``gc.collect()``
        sees the chain's NEXT generator actually garbage.
        """
        # The shutdown watcher parks forever; it only finishes cancelled.
        # The finalizer tasks are awaited, never cancelled (see module docstring).
        for task in wave:
            if _is_shutdown_watcher(task):
                task.cancel()
        done, pending = await asyncio.wait(wave, timeout=max(deadline - loop.time(), 0.001))
        crashes: list[str] = []
        for task in done:
            if task.cancelled():
                continue  # the watcher's expected cancellation delivery
            exc = task.exception()
            if exc is not None:
                crashes.append(f"  - task {task.get_name()!r} died with: {exc!r}")
        if pending or crashes:
            lines = [
                f"SSE stream teardown left {len(pending)} task(s) pending after "
                f"{reap_timeout:.0f}s - the module loop is not clean at the end of "
                "the stream interaction. Live tasks:"
            ]
            lines.extend(f"  - task {t.get_name()!r} still pending: {t!r}" for t in pending)
            lines.extend(crashes)
            raise AssertionError("\n".join(lines))

    while True:
        # Release the PREVIOUS wave's task references first: a completed
        # finalizer task still pins the generator it closed, and a
        # generator still reachable cannot be finalized, so a collect run
        # under it mints nothing and the loop would report a quiescence
        # one wave short of the real cascade.
        wave: list[asyncio.Task[object]] = []
        # Deterministic asyncgen finalization for this wave: without it the
        # abandoned generators are finalized whenever the next collection
        # happens, which must be inside the call, not at some later test's
        # gc pause.
        gc.collect()
        # One loop turn BEFORE the quiescence snapshot: the loop's
        # asyncgen finalizer hook schedules the athrow task with
        # ``call_soon_threadsafe(create_task, agen.aclose())`` - the task
        # is only CREATED on the next turn. A synchronous
        # collect-then-snapshot reports quiescence while the final
        # finalization's task is still pending creation, and the reap
        # returns leaving exactly that straggler for pytest teardown.
        await asyncio.sleep(0)
        wave = [
            task
            for task in asyncio.all_tasks(loop) - baseline
            if not task.done() and task is not current
        ]
        if not wave:
            return
        await _reap_one_wave(wave)
