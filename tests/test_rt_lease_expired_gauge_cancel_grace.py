"""Evidence pins for the running-lease-expired gauge's alert truth.

The running-lease-expired gauge must not alarm on rows the reclaim
sweep is deliberately still waiting out. The reclaim sweep gives a
cancelling row (`cancel_phase >= 1`) a grace window before it reclaims
an expired lease -- cancel grace + cleanup grace + 60 seconds
(`_sweeps.py`'s lease_arm predicate), so an actor finishing its
cooperative cancel does not get its in-flight work double-run. The
gauge's predicate now carves those rows out
(`status='running' AND lock_expires_at < now AND cancel_phase = 0`),
so `TaskQRunningLeaseExpired` pages only on genuinely stuck rows: a
cancel that never completes pages elsewhere: TaskQAbandonedJobs when
its worker is alive to escalate through the phases, TaskQHeartbeatMisses
when it died mid-cancel, and the reclaim sweep honors the row to
`cancelled` either way.

These pins were filed RED-for-the-desired-state (the gauge
over-counted; the cache froze); the cancel_phase carve-out flipped the
first to the corrected behavior -- a cancelling row inside its grace
window reads 0 while a genuinely stuck row in the same sample still
reads 1 -- rather than deleting it.

The second pin documents the fleet-wide arm's accepted failure
semantics: when the whole tick cannot acquire its connection, the
fleet-wide caches keep their last values (never reset to a fake zero --
a missing sample reads identically to a healthy fleet, a false zero
reads as recovery) and the failure counts on
`taskq.maintenance_leader.sweep_timeouts` under the backlog_detection
sweep_name, which is what keeps the degradation observable.
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


@pytest.mark.parametrize("cancel_phase", [1, 2])
async def test_gauge_excludes_cancelling_rows_but_counts_genuinely_stuck_ones(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    cancel_phase: int,
) -> None:
    """A row with an expired lease but `cancel_phase >= 1` is exactly the
    shape the reclaim sweep deliberately leaves alone during its grace
    window -- the sweep's own predicate
    (`cancel_phase = 0 OR lock_expires_at < now - cancel_grace - cleanup_grace - 60s`)
    proves it, so the gauge must not count it: an expired lease mid-cancel
    is the cancellation protocol working, not a stuck reclaim. A
    genuinely-stuck row (cancel_phase = 0, lease past) in the SAME sample
    must still count, so the carve-out cannot hide the real zombies --
    and so this pin can never read 0 vacuously (a broken gauge reads 0 on
    both rows, an uncarved one reads 2). Both phases are covered:
    cooperative (1) and forced (2) sit under the same grace ladder.
    """
    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    worker = new_uuid()
    # The cancelling row inside its reclaim grace window...
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=5),
        cancel_phase=cancel_phase,
    )
    # ...beside a genuinely stuck row the gauge exists to count.
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=60),
        cancel_phase=0,
    )

    ctx = _pg_ctx(clean_pg_conn, schema=schema)
    expired_lease = await _drive_one_sample(ctx)

    assert expired_lease == 1, (
        f"a cancelling row (cancel_phase={cancel_phase}) within its reclaim "
        "grace window must not read as an expired lease — the "
        "TaskQRunningLeaseExpired alert would page on reclaim working "
        "exactly as designed — while the genuinely stuck row beside it "
        f"must still count; the gauge read {expired_lease!r}, expected 1"
    )


@pytest.mark.parametrize("cancel_phase", [1, 2])
async def test_gauge_excludes_a_lone_cancelling_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    cancel_phase: int,
) -> None:
    """The lone-row shape the original evidence pin caught red: nothing in
    the fleet but a cancel in flight whose lease expired five seconds ago.
    Both cooperative (1) and forced (2) phases are carved out: the
    reclaim sweep's grace ladder applies to either, and a cancel that
    never completes pages elsewhere (TaskQAbandonedJobs when its worker
    is alive to escalate, TaskQHeartbeatMisses when it died mid-cancel,
    reclaim honors the row to `cancelled` either way), not on this gauge.
    """
    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    worker = new_uuid()
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=5),
        cancel_phase=cancel_phase,
    )

    ctx = _pg_ctx(clean_pg_conn, schema=schema)
    expired_lease = await _drive_one_sample(ctx)

    assert expired_lease == 0, (
        f"a lone cancelling row (cancel_phase={cancel_phase}) inside its "
        "reclaim grace window read as an expired lease — the gauge's "
        "cancel_phase carve-out is gone and TaskQRunningLeaseExpired fires "
        "on the cancellation protocol working as designed"
    )


async def test_gauge_freezes_at_last_good_value_when_sampling_fails(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Drive one healthy sample (gauge reads 1), then break the pool so the
    next round's statements fail. The fleet-wide arm never resets
    `_running_lease_expired_count` (or its by-status/oldest-due siblings)
    on a failed tick: the caches keep their last values rather than drop
    to a zero an operator would read as recovery, and the failure counts
    on `taskq.maintenance_leader.sweep_timeouts` under the
    backlog_detection sweep_name, which is what keeps a held fleet-wide
    value observable instead of silent (see TaskQSweepTimeouts).
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
        await clean_pg_conn.execute(f'DELETE FROM "{schema}".jobs')  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
        await asyncio.sleep(settings.queue_depth_interval * 6)
    finally:
        _leader_sweeps.update_running_lease_expired_cache = original  # type: ignore[assignment]  # Why: restoring the spied module attribute.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert pool.calls >= 2, "setup: the sampler must have attempted a second round"
    assert observed[-1] == 1, (
        f"the gauge cache should keep its last good value (1) while sampling "
        f"fails — a held value is a missing sample the sweep-timeouts counter "
        f"names; observed {observed!r} — if it dropped to 0 the except branch "
        "wrote a fake zero that reads as recovery"
    )
