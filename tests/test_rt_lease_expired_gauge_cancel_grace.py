"""Evidence pins for two code-level observability defects surfaced while
auditing the docs -- both real, both OUTSIDE the docs-only fix bound of
the issue that prompted this audit, and recorded here as an explicit
broken-window report rather than folded into that issue's fix.

The running-lease-expired gauge must not alarm on rows the reclaim
sweep is deliberately still waiting out. The reclaim sweep gives a
cancelling row (`cancel_phase >= 1`) a grace window before it reclaims
an expired lease -- cancel grace + cleanup grace + 60 seconds
(`_sweeps.py`'s lease_arm predicate), so an actor finishing its
cooperative cancel does not get its in-flight work double-run. The
`TaskQRunningLeaseExpired` alert fires on
`taskq_jobs_running_lease_expired > 0` sustained for 5 minutes with no
carve-out for that same grace window, so a row correctly waiting out
its cancel grace reads as a stuck reclaim to the alert -- a false
positive that pages an operator for behavior working exactly as
designed.

This also proves the sampler's caches go stale, not empty, when a
sampling round fails: the except branch only logs and never resets the
per-gauge caches, so a dead sampler reports the last good number
forever instead of a missing/zero sample -- indistinguishable from a
healthy, quiet fleet.

Both pins currently assert the DEFECT'S observed behavior (the gauge
over-counts; the cache freezes) so they read RED-for-the-desired-state
today; whichever future fix addresses the alert's cancel_phase
carve-out and the sampler's failure-path cache reset should flip these
assertions to the corrected behavior rather than delete them.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker import _leader_sweeps
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _backlog_detection_loop
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration


async def _seed_running_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    locked_by_worker: UUID | None,
    lock_expires_at: datetime | None,
    cancel_phase: int = 0,
) -> UUID:
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
        "retry_kind, status, priority, scheduled_at, locked_by_worker, "
        "lock_expires_at, cancel_phase) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 1, 'non_retryable', "
        "'running', 0, $2, $3, $4, $5)",
        job_id,
        datetime.now(UTC),
        locked_by_worker,
        lock_expires_at,
        cancel_phase,
    )
    return job_id


def _settings(schema: str) -> WorkerSettings:
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}, validate=False
    )
    settings.schema_name = schema
    settings.queue_depth_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    return settings


class _PoolStub:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[object, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
        yield self._conn


def _pg_ctx(conn: asyncpg.Connection, *, schema: str) -> SweepContext:
    settings = _settings(schema)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_PoolStub(conn),  # pyright: ignore[reportArgumentType]  # Why: the sampler's real statement runs on the fixture's real connection.
        heartbeat_pool=_PoolStub(conn),  # pyright: ignore[reportArgumentType]
        worker_pool=_PoolStub(conn),  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
    )
    return SweepContext(
        deps=deps,
        backend=cast("Backend", object()),
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )


async def _drive_one_sample(ctx: SweepContext) -> int | None:
    observed: list[int] = []
    original = _leader_sweeps.update_running_lease_expired_cache

    def _spy(count: int) -> None:
        observed.append(count)

    _leader_sweeps.update_running_lease_expired_cache = _spy  # type: ignore[assignment]  # Why: test-only instrumentation of the module's imported name, same pattern as the existing backlog-loop coverage.
    shutdown = asyncio.Event()
    task = asyncio.create_task(_backlog_detection_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if observed:
                break
            await asyncio.sleep(0.01)
    finally:
        _leader_sweeps.update_running_lease_expired_cache = original  # type: ignore[assignment]  # Why: restoring the spied module attribute.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return observed[0] if observed else None


async def test_gauge_counts_a_cancelling_row_still_inside_its_reclaim_grace(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A row with an expired lease but `cancel_phase >= 1` is exactly the
    shape the reclaim sweep deliberately leaves alone during its grace
    window -- the sweep's own predicate
    (`cancel_phase = 0 OR lock_expires_at < now - cancel_grace - cleanup_grace - 60s`)
    proves it. The gauge's predicate is `status='running' AND
    lock_expires_at < now` with no such carve-out, so it counts this row
    as a stuck reclaim while the reclaim sweep is working exactly as
    designed.
    """
    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    worker = new_uuid()
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=5),
        cancel_phase=1,
    )

    ctx = _pg_ctx(clean_pg_conn, schema=schema)
    expired_lease = await _drive_one_sample(ctx)

    assert expired_lease == 1, (
        "a cancelling row within its reclaim grace window is counted by the "
        "gauge as an expired lease with no carve-out -- this is the current, "
        "documented defect: the TaskQRunningLeaseExpired alert has no "
        "cancel_phase exemption, so it can fire on a row the reclaim sweep is "
        "deliberately still waiting to reclaim, not a stuck fleet"
    )


async def test_gauge_freezes_at_last_good_value_when_sampling_fails(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Drive one healthy sample (gauge reads 1), then break the pool so the
    next round's statements fail. The except branch only logs -- it never
    resets `_running_lease_expired_count` (or its by-status/oldest-due
    siblings) -- so the gauge keeps reporting the stale value 1 forever
    instead of dropping to a missing/zero sample an operator could tell
    apart from a healthy fleet.
    """
    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    worker = new_uuid()
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=60),
    )

    class _FailingPool:
        def __init__(self, real_conn: object) -> None:
            self._real_conn = real_conn
            self.calls = 0

        @asynccontextmanager
        async def acquire(
            self,
            *,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
        ) -> AsyncGenerator[object, None]:
            self.calls += 1
            if self.calls == 1:
                yield self._real_conn
            else:
                raise ConnectionError("pool exhausted")

    pool = _FailingPool(clean_pg_conn)
    settings = _settings(schema)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: fails from the second acquire onward to exercise the except branch.
        heartbeat_pool=pool,  # pyright: ignore[reportArgumentType]
        worker_pool=pool,  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
    )
    ctx = SweepContext(
        deps=deps,
        backend=cast("Backend", object()),
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )

    observed: list[int] = []
    original = _leader_sweeps.update_running_lease_expired_cache

    def _spy(count: int) -> None:
        observed.append(count)

    _leader_sweeps.update_running_lease_expired_cache = _spy  # type: ignore[assignment]  # Why: test-only instrumentation of the module's imported name.
    shutdown = asyncio.Event()
    task = asyncio.create_task(_backlog_detection_loop(ctx, shutdown))
    try:
        # Wait for the first (successful) sample.
        for _ in range(400):
            if observed:
                break
            await asyncio.sleep(0.01)
        assert observed and observed[0] == 1, "setup: first sample must succeed and read 1"

        # Delete the job so a healthy re-sample (if one ran) would read 0,
        # then wait through several more tick intervals while the pool is
        # failing every subsequent acquire.
        await clean_pg_conn.execute(f'DELETE FROM "{schema}".jobs')
        await asyncio.sleep(settings.queue_depth_interval * 6)
    finally:
        _leader_sweeps.update_running_lease_expired_cache = original  # type: ignore[assignment]  # Why: restoring the spied module attribute.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert pool.calls >= 2, "setup: the sampler must have attempted a second round"
    assert observed[-1] == 1, (
        f"the gauge cache should freeze at its last good value (1) while sampling "
        f"fails, but observed {observed!r} -- if it dropped to 0 the except branch "
        "reset the cache, contradicting the current code path that only logs"
    )
