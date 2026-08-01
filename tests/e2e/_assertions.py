"""E2E polling-with-deadline assertion helpers.

``taskq.testing.assertions`` is not usable client-side: ``wait_for`` awaits an
in-process ``asyncio.Event`` and ``wait_for_job_status`` requires an in-process
``Backend`` — neither exists across the container boundary. The e2e test
process is a pure client, so these helpers poll externally observable state
(``JobHandle`` reads and PG rows) with wall-clock deadlines instead.

"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Sequence
from uuid import UUID

import asyncpg
from pydantic import BaseModel

from taskq.client import JobHandle
from taskq.exceptions import JobFailed, ResultUnavailable

__all__ = [
    "fetch_batch_status",
    "fetch_effects",
    "fetch_job_rows",
    "poll_until",
    "wait_all",
    "wait_all_ignoring_failures",
    "wait_for_effects",
    "wait_for_handle_status",
    "wait_for_worker_ready",
]

_EFFECTS_COLUMNS = "seq, at, actor, job_id, attempt, kind, detail"
_JOBS_COLUMNS = (
    "id, actor, queue, status, attempt, max_attempts, created_at, scheduled_at, "
    "started_at, finished_at, metadata, payload"
)


async def poll_until(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout: float,  # noqa: ASYNC109  # Why: deadline budget consumed internally by the polling loop; asyncio.timeout() doesn't fit a poll-and-report-elapsed pattern.
    interval: float = 0.25,
    description: str = "condition",
) -> None:
    """Poll *predicate* until it returns ``True`` or *timeout* seconds elapse.

    The predicate runs once per iteration; on expiry raises :class:`TimeoutError`
    carrying *description* and the elapsed time. Sleeps only between probes,
    never past the deadline.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    deadline = start + timeout
    while True:
        if await predicate():
            return
        now = loop.time()
        remaining = deadline - now
        if remaining <= 0:
            elapsed = now - start
            msg = (
                f"timed out after {elapsed:.2f}s (budget {timeout:.2f}s) waiting for {description}"
            )
            raise TimeoutError(msg)
        await asyncio.sleep(min(interval, remaining))


async def wait_for_handle_status[R: BaseModel | None](
    handle: JobHandle[R],
    status: str,
    *,
    timeout: float = 60.0,  # noqa: ASYNC109  # Why: forwarded to poll_until as a polling deadline, not an asyncio.timeout-style wrapper.
) -> None:
    """Poll ``handle.status()`` until it equals *status* (e.g. ``running``
    before a cancel, ``failed`` after a permanent failure)."""

    async def _matches() -> bool:
        return (await handle.status()) == status

    await poll_until(
        _matches,
        timeout=timeout,
        description=f"job {handle.job_id} to reach status {status!r}",
    )


async def wait_all[R: BaseModel | None](
    handles: Sequence[JobHandle[R]],
    *,
    timeout: float,  # noqa: ASYNC109  # Why: forwarded to handle.wait as a per-handle polling deadline, not an asyncio.timeout-style wrapper.
) -> list[R]:
    """Wait for every handle in parallel via :class:`asyncio.TaskGroup`.

    Returns each handle's ``wait`` result in input order.  Any exception
    (``TimeoutError``, :class:`JobFailed`, :class:`ResultUnavailable`)
    propagates inside the ``ExceptionGroup`` raised by the ``TaskGroup`` on
    exit — use this when every handle is expected to reach ``succeeded``.
    """
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(handle.wait(timeout=timeout)) for handle in handles]
    return [task.result() for task in tasks]


async def wait_all_ignoring_failures[R: BaseModel | None](
    handles: Sequence[JobHandle[R]],
    *,
    timeout: float,  # noqa: ASYNC109  # Why: forwarded to handle.wait as a per-handle deadline; individual failures/timeouts are swallowed, not wrapped in asyncio.timeout().
) -> None:
    """Wait for every handle in parallel, swallowing per-handle failures.

    Each ``handle.wait`` is awaited inside a :class:`asyncio.TaskGroup`;
    ``TimeoutError``, :class:`JobFailed` and :class:`ResultUnavailable` raised
    by an individual handle are swallowed so one non-succeeding handle cannot
    abort the wait.  Use this when some handles are not expected to reach
    ``succeeded`` within *timeout* — e.g. a finalizer that snoozes via
    ``wait_for_batch`` until its children finish, or jobs cancelled/failed by
    a batch-abort policy.  Terminal state is verified afterward via DB polling
    (``fetch_job_rows`` / ``wait_for_effects``) rather than ``wait``'s return.
    """

    async def _wait_or_swallow(handle: JobHandle[R]) -> None:
        with contextlib.suppress(TimeoutError, JobFailed, ResultUnavailable):
            await handle.wait(timeout=timeout)

    async with asyncio.TaskGroup() as tg:
        for handle in handles:
            tg.create_task(_wait_or_swallow(handle))


async def fetch_effects(
    pool: asyncpg.Pool,
    schema: str,
    run_id: str,
    *,
    kind: str | None = None,
) -> list[asyncpg.Record]:
    """One-shot read of ``{schema}.e2e_effects`` rows correlated to *run_id*
    (stored in ``detail->>'run_id'`` by the actors), ordered by ``seq``.

    For final assertions after a wait; use :func:`wait_for_effects` to poll.
    """
    if kind is None:
        return await pool.fetch(
            f"""
            SELECT {_EFFECTS_COLUMNS} FROM "{schema}".e2e_effects
            WHERE detail->>'run_id' = $1
            ORDER BY seq
            """,
            run_id,
        )
    return await pool.fetch(
        f"""
        SELECT {_EFFECTS_COLUMNS} FROM "{schema}".e2e_effects
        WHERE detail->>'run_id' = $1 AND kind = $2
        ORDER BY seq
        """,
        run_id,
        kind,
    )


async def fetch_job_rows(
    pool: asyncpg.Pool,
    schema: str,
    job_ids: Sequence[UUID],
) -> list[asyncpg.Record]:
    """One-shot read of ``{schema}.jobs`` rows by id, ordered by ``created_at``.

    For terminal-state assertions after a wait (e.g. ``max_attempts`` as a
    snooze discriminator — ``mark_snoozed`` is the only healthy-lifecycle
    writer that bumps it) — pair with ``JobHandle.wait`` so no polling is
    needed.
    """
    return await pool.fetch(
        f"""
        SELECT {_JOBS_COLUMNS} FROM "{schema}".jobs
        WHERE id = ANY($1::uuid[])
        ORDER BY created_at
        """,
        job_ids,
    )


async def fetch_batch_status(
    pool: asyncpg.Pool,
    schema: str,
    batch_id: UUID,
) -> str | None:
    """One-shot read of the ``status`` column from ``{schema}.batches`` for *batch_id*."""
    return await pool.fetchval(
        f'SELECT status FROM "{schema}".batches WHERE id = $1',
        batch_id,
    )


async def wait_for_effects(
    pool: asyncpg.Pool,
    schema: str,
    run_id: str,
    *,
    kind: str | None = None,
    min_count: int = 1,
    timeout: float = 60.0,  # noqa: ASYNC109  # Why: forwarded to poll_until as a polling deadline, not an asyncio.timeout-style wrapper.
) -> list[asyncpg.Record]:
    """Poll ``{schema}.e2e_effects`` until at least *min_count* rows exist for
    *run_id* (optionally filtered by *kind*), then return them ordered by ``seq``."""
    rows: list[asyncpg.Record] = []

    async def _enough() -> bool:
        nonlocal rows
        rows = await fetch_effects(pool, schema, run_id, kind=kind)
        return len(rows) >= min_count

    await poll_until(
        _enough,
        timeout=timeout,
        description=(
            f">= {min_count} e2e_effects row(s) "
            f"(run_id={run_id!r}, kind={kind!r}) in {schema}.e2e_effects"
        ),
    )
    return rows


async def wait_for_worker_ready(
    pool: asyncpg.Pool,
    schema: str,
    *,
    timeout: float = 30.0,  # noqa: ASYNC109  # Why: forwarded to poll_until as a polling deadline, not an asyncio.timeout-style wrapper.
) -> None:
    """Readiness gate: poll ``{schema}.workers`` until a row shows a fresh
    POST-REGISTER heartbeat (``last_seen_at > started_at``, within the last
    10s). Requiring a heartbeat after registration means a worker that
    crashes mid-bootstrap (after ``register_worker`` but before its first
    tick — e.g. an actor-config failure) never satisfies the gate, so the
    fixture dumps the container logs with the actual traceback instead of
    letting the test proceed against a dead worker."""

    async def _fresh_heartbeat() -> bool:
        return (
            await pool.fetchval(
                f"""
                SELECT 1 FROM "{schema}".workers
                WHERE last_seen_at > now() - interval '10 seconds'
                  AND last_seen_at > started_at
                LIMIT 1
                """
            )
            is not None
        )

    await poll_until(
        _fresh_heartbeat,
        timeout=timeout,
        description=f"fresh post-register worker heartbeat in {schema}.workers",
    )
