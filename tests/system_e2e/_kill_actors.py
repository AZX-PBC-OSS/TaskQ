"""The kill9 chaos campaign's actors: the module BOTH sides of a kill import.

The test process imports it for the ActorRef handles; the SIGKILL harness
(``tests.system_e2e._kill_entry``) imports the same registry, so the worker
that dies mid-phase and the client that reconciles the aftermath serve the
exact same actor definitions - the registry-drift guarantee the system tier
gets from :mod:`tests.system_e2e.actors`, kept in a separate module so the
campaign's deterministic-kill shapes cannot drift into the graceful scenarios'
registry.

Every workload is deterministic: simulated work via ``asyncio.sleep``, one
effects row per recorded phase, and an ATTEMPT-AWARE tail (a killed attempt
1 sleeps; the reclaim's re-run finishes promptly) so a crash's recovery lands
inside the test's derived budget instead of re-paying attempt 1's full span.
The retry curve is the fastest legal ladder (1s floor, zero jitter), which
makes the reclaim re-pend delay exactly deterministic: base * 2**(attempt-1)
= 1.0s at attempt 1 (see actors.py's ``_FAST_RETRY`` note).
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq import ActorRef, JobContext, RetryPolicy, actor

# ruff: noqa: S608  # Why: the schema identifier is operator-side (a fixture-validated name) and every value is $-bound; only the f-string interpolation of the schema name is flagged.

_QUEUE = "system_e2e"

#: The fastest legal retry ladder: 1s is MIN_DEFERRAL_INTERVAL, jitter 0 makes
#: the reclaim re-pend delay exactly 1.0s at attempt 1. Budget 5 so a crash's
#: re-run has real headroom without a retry-exhausted terminal mid-test.
_KILL9_RETRY = RetryPolicy(
    kind="transient",
    max_attempts=5,
    base=timedelta(seconds=1),
    jitter=0.0,
)


#: Single-attempt policy for the exhausted-retry kill shape: the killed
#: attempt IS the budget, so the reclaim's crash arm (not a re-run) owns
#: the row - the "running -> crashed when exhausted" disposition under a
#: real SIGKILL.
_KILL9_ONE_LIFE = RetryPolicy(
    kind="transient", max_attempts=1, base=timedelta(seconds=1), jitter=0.0
)


class Kill9Payload(BaseModel):
    """Payload for the kill9 workloads; the tail is attempt-aware."""

    sleep: float = 0.0
    beats: int = 0


async def _record(kind: str, actor_name: str, job_id: object, attempt: int) -> None:
    """Append one effects row to ``{schema}.sys_effects`` (per-process conn).

    The DSN and schema come from the worker's own environment, the same
    ``TASKQ_*`` variables the production bootstrap reads - the actor surface
    stays ``(payload, ctx)`` by contract.
    """
    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        await conn.execute(
            f'INSERT INTO "{os.environ["TASKQ_SCHEMA_NAME"]}".sys_effects '
            "(job_id, attempt, actor, kind) VALUES ($1, $2, $3, $4)",
            job_id,
            attempt,
            actor_name,
            kind,
        )
    finally:
        await conn.close()


@actor(name="kill9_slow", queue=_QUEUE, retry=_KILL9_RETRY)
async def kill9_slow(payload: Kill9Payload, ctx: JobContext[Kill9Payload]) -> dict[str, float]:
    """The effect-on-completion-only workload: a killed attempt writes
    NOTHING (the re-run's row is the exactly-once evidence)."""
    await asyncio.sleep(payload.sleep if ctx.attempt == 1 else 0.2)
    await _record("done", "kill9_slow", ctx.job_id, ctx.attempt)
    return {"sleep": payload.sleep}


@actor(name="kill9_starting", queue=_QUEUE, retry=_KILL9_RETRY)
async def kill9_starting(payload: Kill9Payload, ctx: JobContext[Kill9Payload]) -> dict[str, float]:
    """The observable-body workload: records ``start`` the moment the body
    begins (the external kill's readiness signal), then holds."""
    await _record("start", "kill9_starting", ctx.job_id, ctx.attempt)
    await asyncio.sleep(payload.sleep if ctx.attempt == 1 else 0.2)
    await _record("done", "kill9_starting", ctx.job_id, ctx.attempt)
    return {"sleep": payload.sleep}


@actor(name="kill9_progress", queue=_QUEUE, retry=_KILL9_RETRY)
async def kill9_progress(payload: Kill9Payload, ctx: JobContext[Kill9Payload]) -> dict[str, int]:
    """The fanout workload: one publish per beat, the durable seq advances
    even when the broker drops them (the progress-loss accounting surface)."""
    for step in range(payload.beats):
        await ctx.progress(step=step)
        await asyncio.sleep(0.15)
    await _record("done", "kill9_progress", ctx.job_id, ctx.attempt)
    return {"beats": payload.beats}


@actor(name="kill9_one_life", queue=_QUEUE, retry=_KILL9_ONE_LIFE)
async def kill9_one_life(payload: Kill9Payload, ctx: JobContext[Kill9Payload]) -> dict[str, float]:
    """The single-attempt workload: the body records ``start`` and holds -
    a kill mid-body spends the WHOLE retry budget, so the reclaim's crash
    arm (not a re-run) owns the row."""
    await _record("start", "kill9_one_life", ctx.job_id, ctx.attempt)
    await asyncio.sleep(payload.sleep)
    await _record("done", "kill9_one_life", ctx.job_id, ctx.attempt)
    return {"sleep": payload.sleep}


ACTORS: dict[str, ActorRef[Any, Any]] = {
    "kill9_slow": kill9_slow,
    "kill9_starting": kill9_starting,
    "kill9_progress": kill9_progress,
    "kill9_one_life": kill9_one_life,
}
