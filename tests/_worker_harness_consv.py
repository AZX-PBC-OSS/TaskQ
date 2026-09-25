"""Private test-only worker entry point for the conservation chaos tests.

Invoked via ``sys.executable -m tests._worker_harness_consv`` by
``tests/test_rt_conservation_chaos.py``. Loads ``WorkerSettings`` from the
environment, runs the production ``_main`` bootstrap with the module's
actors, and honors one fault-injection switch:

``TASKQ_CONSV_KILL_ON_TERMINAL=1``
    Patch ``PostgresBackend.mark_succeeded`` so the process dies by SIGKILL
    the instant the terminal DB write commits - before the terminal
    state-change publish and the result fanout can run. What the outside
    world observes afterwards is exactly what a SIGKILL in that window
    leaves behind: the committed row, the attempt ledger, the event trail,
    and no fanout.

``TASKQ_CONSV_KILL_BEFORE_TERMINAL=1``
    The other side of the same window: the process dies by SIGKILL BEFORE
    the terminal write is attempted, so the row is left running, locked,
    with a live lease - the uncommitted side of the terminal-write kill.
    Recovery is the lease-lapse reclaim, the requeue, and a second attempt.

Both switches are the two deterministic brackets of one kill window: a kill
that lands anywhere inside the terminal write's commit leaves the row
committed (the first switch's shape) or uncommitted (the second's), and the
conservation pins assert BOTH shapes conserve.

The ``grace_*`` actors also write one body-run ledger row per execution into
``{schema}.consv_body_runs`` (the calling test creates the table from the
same TASKQ_PG_DSN / TASKQ_SCHEMA_NAME the harness runs with), so a test can
count executions exactly: the double-run and the dropped-run are the two
failures the graceful-shutdown timeline has to be proven against.

Without a switch the harness is a plain worker (the ``tests._worker_harness``
shape plus the actors the chaos tests dispatch).
"""

# ruff: noqa: S608  # Why: the schema comes from the worker's own validated TASKQ_SCHEMA_NAME env; every value is $-bound.

import asyncio
import os
import signal
import sys
import time
from datetime import timedelta
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend.postgres import PostgresBackend
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker.run import _main

_QUEUE = "consv_q"

#: The grace actors' retry policy: a 1s flat reclaim curve with zero jitter.
#: The leader sweep's re-pend reschedules a reclaimed job out by the row's
#: OWN stamped curve (base * 2**(attempt-1), jittered, floored at 1s); with
#: base 1s and jitter 0 the delay is exactly 1.0s at attempt 1, so the pins'
#: pickup bound carries a deterministic reclaim-backoff term instead of a
#: jitter band.
_GRACE_RETRY = RetryPolicy(max_attempts=5, base=timedelta(seconds=1), jitter=0.0)


class ConsvPayload(BaseModel):
    calls: int = 6
    hold: float = 0.0


@actor(name="consv_progress", queue=_QUEUE)
async def consv_progress(
    payload: ConsvPayload, ctx: JobContext[ConsvPayload]
) -> dict[str, str | int]:
    for step in range(payload.calls):
        await ctx.progress(step=step)
    return {"marker": "landed", "calls": payload.calls}


@actor(name="consv_slow", queue=_QUEUE)
async def consv_slow(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> None:
    _ = payload
    await asyncio.sleep(30.0)
    _ = ctx


async def _harness_body_run(ctx: JobContext[ConsvPayload]) -> None:
    """Write the body-run ledger row: one per (job_id, attempt) execution.

    The ledger DSN and schema are the worker's own env (the spawn helper
    sets TASKQ_PG_DSN / TASKQ_SCHEMA_NAME); the calling test creates the
    table before the worker boots.
    """
    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        await conn.execute(
            f'INSERT INTO "{os.environ["TASKQ_SCHEMA_NAME"]}".consv_body_runs '
            "(job_id, attempt, run_token) VALUES ($1, $2, $3)",
            ctx.job_id,
            ctx.attempt,
            new_uuid(),
        )
    finally:
        await conn.close()


@actor(name="consv_grace_tail", queue=_QUEUE, retry=_GRACE_RETRY)
async def consv_grace_tail(
    payload: ConsvPayload, ctx: JobContext[ConsvPayload]
) -> dict[str, str | int]:
    """Bounded stepped work (~1.5s): a job that finishes WITHIN the grace."""
    await _harness_body_run(ctx)
    for step in range(6):
        await ctx.progress(step=step)
        await asyncio.sleep(0.25)
    return {"marker": "landed", "calls": payload.calls}


@actor(name="consv_grace_holder", queue=_QUEUE, retry=_GRACE_RETRY)
async def consv_grace_holder(
    payload: ConsvPayload, ctx: JobContext[ConsvPayload]
) -> dict[str, str]:
    """Hold for ``payload.hold`` on attempt 1, briefly on any later attempt.

    The attempt-aware tail is what lets a pin's successor re-run the job to
    a terminal state inside the test's time budget without the payload
    carrying per-attempt state.
    """
    await _harness_body_run(ctx)
    hold = payload.hold if ctx.attempt == 1 else 0.2
    await asyncio.sleep(hold)
    _ = ctx
    return {"marker": "landed"}


@actor(name="consv_grace_defier", queue=_QUEUE, retry=_GRACE_RETRY)
async def consv_grace_defier(
    payload: ConsvPayload, ctx: JobContext[ConsvPayload]
) -> dict[str, str]:
    """Hold past every grace by DEFYING cancellation.

    Catches CancelledError and keeps holding: the stand-in for work that
    cannot observe cancellation (a blocked C call, a thread join), the
    exact case the shutdown watchdog's deadline trip exists to end. On any
    later attempt (the successor's re-run) it completes promptly.
    """
    await _harness_body_run(ctx)
    hold = payload.hold if ctx.attempt == 1 else 0.2
    held_until = time.monotonic() + hold
    while time.monotonic() < held_until:
        try:
            await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            continue
    return {"marker": "landed"}


_HARNESS_GRACE_REGISTRY: dict[str, Any] = {
    "consv_grace_tail": consv_grace_tail,
    "consv_grace_holder": consv_grace_holder,
    "consv_grace_defier": consv_grace_defier,
}


_original_mark_succeeded = PostgresBackend.mark_succeeded
_original_mark_succeeded_with_conn = PostgresBackend.mark_succeeded_with_conn


async def _mark_succeeded_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    row = await _original_mark_succeeded(self, *args, **kwargs)
    # The terminal write has committed (mark_succeeded is the single
    # statement that lands status + result + attempt row + event). Die the
    # way a SIGKILL does: no cleanup, no publish, no flush drain.
    os.kill(os.getpid(), signal.SIGKILL)
    return row  # pragma: no cover - the process is gone before this returns


async def _mark_succeeded_with_conn_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    row = await _original_mark_succeeded_with_conn(self, *args, **kwargs)
    os.kill(os.getpid(), signal.SIGKILL)
    return row  # pragma: no cover - the process is gone before this returns


async def _mark_succeeded_with_conn_sigkill_first(self: Any, *args: Any, **kwargs: Any) -> Any:
    # The uncommitted side of the window: die BEFORE the terminal write is
    # attempted. The row stays running, locked, its lease live; recovery is
    # the lease-lapse reclaim and a second attempt.
    os.kill(os.getpid(), signal.SIGKILL)
    return await _original_mark_succeeded_with_conn(self, *args, **kwargs)  # pragma: no cover


if os.environ.get("TASKQ_CONSV_KILL_ON_TERMINAL") == "1":
    # The consumer's success path rides the pooled-with-conn variant; the
    # plain variant is patched too so any caller dies the same way.
    PostgresBackend.mark_succeeded = _mark_succeeded_then_sigkill  # type: ignore[assignment]
    PostgresBackend.mark_succeeded_with_conn = _mark_succeeded_with_conn_then_sigkill  # type: ignore[assignment]
elif os.environ.get("TASKQ_CONSV_KILL_BEFORE_TERMINAL") == "1":
    PostgresBackend.mark_succeeded = _mark_succeeded_with_conn_sigkill_first  # type: ignore[assignment]
    PostgresBackend.mark_succeeded_with_conn = _mark_succeeded_with_conn_sigkill_first  # type: ignore[assignment]

if __name__ == "__main__":
    settings = WorkerSettings.load()
    registry: dict[str, Any] = {
        "consv_progress": consv_progress,
        "consv_slow": consv_slow,
        **_HARNESS_GRACE_REGISTRY,
    }
    with asyncio.Runner() as runner:
        sys.exit(runner.run(_main(settings, actor_registry=registry)))
