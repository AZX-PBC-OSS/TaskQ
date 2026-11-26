"""One-cancellation-only delivery to an in-flight attempt's asyncio task.

The worker has four cancellers - the cancel ladder's phase-2 escalation, the
shutdown orchestrator's FORCING phase, ``isolate_self``, and the abandon
drain - and any two of them can reach the same registered attempt within a
few milliseconds of each other. This module owns the rule they share, kept in
its own module because ``taskq.worker.cancel`` imports ``ShutdownPhase`` from
``taskq.worker.shutdown``: a helper the orchestrator also needs cannot live in
either without a cycle.
"""

from __future__ import annotations

import asyncio
from typing import Any

__all__ = ["deliver_first_cancel"]


def deliver_first_cancel(task: asyncio.Task[Any]) -> None:
    """Deliver a cancellation to an in-flight attempt, first delivery only.

    A second ``task.cancel()`` is not a no-op. A task already cancelling is
    unwinding through the ``CancelledError`` handler that owns its terminal
    write (``mark_cancelled`` or ``mark_interrupted``); a second cancel
    interrupts that write, so the consumer's handler disowns the job and
    deregisters the entry, and the row is left ``running`` under a worker that
    is about to exit ``rc == 0`` - the cancel-racing-SIGTERM strand. Whoever
    cancels first therefore owns the attempt, and every later canceller
    delivers nothing.

    ``cancelling() == 0`` is the guard rather than ``done()`` alone because a
    consumer that has taken its cancellation is still running: it is awaiting
    its terminal write, which is exactly what a second cancel would destroy.
    """
    if not task.done() and task.cancelling() == 0:
        task.cancel()
