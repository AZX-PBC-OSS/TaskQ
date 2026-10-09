"""THE REAPER (the silent-swallow class's one legal home).

``reap_cancelled_child`` awaits a JUST-CANCELLED child task: the
``task.cancel()`` + ``await task`` reaper shape needs the await's
CancelledError contained — but a BARE
``contextlib.suppress(asyncio.CancelledError)`` also contains the
REAPER'S OWN concurrent cancellation (the shutdown's cancel dies inside
the suppress and the reaper runs on — the F-DEMO-2 conviction). The
helper suppresses only THE CHILD'S cancel: the uncancel accounting
distinguishes the two (the child's cancel never touched this task's
count; the reaper's own did) and re-raises what belongs to the reaper.
"""

from __future__ import annotations

import asyncio

__all__ = ["reap_cancelled_child"]


async def reap_cancelled_child(task: asyncio.Future[object]) -> None:
    """Await a JUST-CANCELLED child task, suppressing only THE CHILD'S
    OWN cancellation. A concurrent cancellation of THIS task (the
    shutdown's own cut) propagates — never swallowed."""
    try:
        await task
    except asyncio.CancelledError:
        me = asyncio.current_task()
        if me is not None and me.cancelling():
            raise
