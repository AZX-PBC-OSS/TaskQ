"""The system tier's actor module: the one module BOTH sides import.

The test process imports it for the ActorRef handles and payload models
(``tests.system_e2e.actors``); every worker subprocess imports the same
module by the same absolute path (the spawn inherits the repo root as
``cwd``, so ``-m tests.system_e2e._worker_entry`` resolves it), which is
what guarantees the registry the worker serves is the registry the client
enqueues against - no name drift between generations of the fleet.

Imports are limited to stdlib, pydantic, asyncpg and the taskq public API
so both environments load it cleanly. Every workload is deterministic:
simulated work via ``asyncio.sleep``, failure injection only via payload
flags and fixed attempt counters.

The effects ledger: each body appends ``(job_id, attempt, actor, kind)``
rows to ``{schema}.sys_effects``. That table is the body-run evidence the
system invariants reconcile against (never two runs per attempt for
non-idempotent actors, never a run with no claim row behind it). It is
created per module schema by the tier conftest, NOT by TaskQ migrations.
"""

# ruff: noqa: S608  # Why: the schema identifier is operator-side (a fixture-validated name) and every value is $-bound; only the f-string interpolation of the schema name is flagged.

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq import ActorRef, JobContext, RetryPolicy, actor

_QUEUE = "system_e2e"

#: Retry ladder used wherever a scenario needs retries to land inside the
#: scenario's own wall-clock window. 1s is MIN_DEFERRAL_INTERVAL (the floor
#: the failure-retry decision enforces), so this is the fastest legal ladder.
_FAST_RETRY = RetryPolicy(
    kind="transient",
    max_attempts=5,
    base=timedelta(seconds=1),
    jitter=0.0,
)

#: Single-attempt policy for actors whose scenario wants NO retry ladder:
#: the first failure is terminal.
_NO_RETRY = RetryPolicy(kind="transient", max_attempts=1)


class SysPayload(BaseModel):
    """Payload for the plain workloads; ``sleep`` scales the body."""

    sleep: float = 0.0
    beats: int = 0


class FlakyPayload(SysPayload):
    """Fails while ``ctx.attempt <= fail_until_attempt``, then succeeds."""

    fail_until_attempt: int = 2


# ── The effects ledger ───────────────────────────────────────────────────


_effect_conn: asyncpg.Connection | None = None
_effect_lock: asyncio.Lock | None = None


async def _record(kind: str, actor_name: str, job_id: object, attempt: int) -> None:
    """Append one effects row, on a per-process cached connection.

    The DSN and schema come from the worker's own environment (the same
    ``TASKQ_*`` variables the production bootstrap reads), so the actor
    surface stays ``(payload, ctx)`` by contract.
    """
    global _effect_conn, _effect_lock
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    if _effect_lock is None:
        _effect_lock = asyncio.Lock()
    async with _effect_lock:
        if _effect_conn is None or _effect_conn.is_closed():
            _effect_conn = await asyncpg.connect(dsn)
        await _effect_conn.execute(
            f'INSERT INTO "{schema}".sys_effects (job_id, attempt, actor, kind) '
            "VALUES ($1, $2, $3, $4)",
            job_id,
            attempt,
            actor_name,
            kind,
        )


def _record_sync(kind: str, actor_name: str, job_id: object, attempt: int) -> None:
    """Sync-side record: a private loop on the calling (worker) thread."""
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]

    async def _go() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                f'INSERT INTO "{schema}".sys_effects (job_id, attempt, actor, kind) '
                "VALUES ($1, $2, $3, $4)",
                job_id,
                attempt,
                actor_name,
                kind,
            )
        finally:
            await conn.close()

    asyncio.run(_go())


# ── Async actors ─────────────────────────────────────────────────────────


@actor(name="sys_fast", queue=_QUEUE)
async def sys_fast(payload: SysPayload, ctx: JobContext[SysPayload]) -> dict[str, int]:
    await asyncio.sleep(payload.sleep)
    await _record("done", "sys_fast", ctx.job_id, ctx.attempt)
    return {"beats": payload.beats}


@actor(name="sys_slow", queue=_QUEUE, retry=_FAST_RETRY)
async def sys_slow(payload: SysPayload, ctx: JobContext[SysPayload]) -> dict[str, float]:
    """The deploy/leader-loss workload: long, and the effect lands only on
    completion, so a killed attempt writes nothing (the re-run's row is the
    exactly-once evidence). The retry budget is real headroom, not
    decoration: a reclaim after a crash re-runs the job under a NEW attempt
    only while the budget lasts - exhausted, the row terminalises."""
    await asyncio.sleep(payload.sleep)
    await _record("done", "sys_slow", ctx.job_id, ctx.attempt)
    return {"sleep": payload.sleep}


@actor(name="sys_progress", queue=_QUEUE)
async def sys_progress(payload: SysPayload, ctx: JobContext[SysPayload]) -> dict[str, int]:
    for step in range(payload.beats):
        await ctx.progress(step=step)
        await asyncio.sleep(0.15)
    await _record("done", "sys_progress", ctx.job_id, ctx.attempt)
    return {"beats": payload.beats}


@actor(name="sys_flaky", queue=_QUEUE, retry=_FAST_RETRY)
async def sys_flaky(payload: FlakyPayload, ctx: JobContext[FlakyPayload]) -> dict[str, int]:
    await _record("run", "sys_flaky", ctx.job_id, ctx.attempt)
    if ctx.attempt <= payload.fail_until_attempt:
        raise RuntimeError(f"flaky attempt {ctx.attempt}")
    await _record("done", "sys_flaky", ctx.job_id, ctx.attempt)
    return {"succeeded_at": ctx.attempt}


@actor(name="sys_always_fails", queue=_QUEUE, retry=_FAST_RETRY)
async def sys_always_fails(payload: SysPayload, ctx: JobContext[SysPayload]) -> None:
    await _record("run", "sys_always_fails", ctx.job_id, ctx.attempt)
    raise RuntimeError("always fails")


@actor(name="sys_hang", queue=_QUEUE, retry=_NO_RETRY)
async def sys_hang(payload: SysPayload, ctx: JobContext[SysPayload]) -> None:
    """The deadline workload: records the start, then never returns. The
    reaper (in-process start_to_close or the leader's sweep) owns the row."""
    await _record("start", "sys_hang", ctx.job_id, ctx.attempt)
    await asyncio.sleep(600.0)
    _ = ctx


@actor(name="sys_panic", queue=_QUEUE, retry=_NO_RETRY)
async def sys_panic(payload: SysPayload, ctx: JobContext[SysPayload]) -> None:
    await _record("run", "sys_panic", ctx.job_id, ctx.attempt)
    raise RuntimeError("panic")


# ── Sync actors ──────────────────────────────────────────────────────────


@actor(name="sys_sync_ok", queue=_QUEUE)
def sys_sync_ok(payload: SysPayload, ctx: JobContext[SysPayload]) -> dict[str, str]:
    _record_sync("done", "sys_sync_ok", ctx.job_id, ctx.attempt)
    return {"kind": "sync"}


@actor(name="sys_sysexit", queue=_QUEUE, retry=_NO_RETRY)
def sys_sysexit(payload: SysPayload, ctx: JobContext[SysPayload]) -> None:
    """The SystemExit workload (the #459 family): a sync body that raises a
    BaseException. The worker must survive it and the job must terminalise."""
    _record_sync("run", "sys_sysexit", ctx.job_id, ctx.attempt)
    raise SystemExit(3)


# ── The registry the worker subprocess serves ────────────────────────────

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "sys_fast": sys_fast,
    "sys_slow": sys_slow,
    "sys_progress": sys_progress,
    "sys_flaky": sys_flaky,
    "sys_always_fails": sys_always_fails,
    "sys_hang": sys_hang,
    "sys_panic": sys_panic,
    "sys_sync_ok": sys_sync_ok,
    "sys_sysexit": sys_sysexit,
}
