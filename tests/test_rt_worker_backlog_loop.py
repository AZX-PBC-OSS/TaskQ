"""Adversarial pins for the unconditional backlog-detection loop.

The loop's reason to exist (the #102/#105 lesson): a detector hosted
behind the leadership gate emits nothing under exactly the failure it
exists to expose. The pins here attack that property at the LOOP level —
leadership held by nobody must still feed both gauges — plus the
oldest-due age's real computation, the per-status split, and the
deliberate difference from queue depth on demotion (the backlog caches
are every-worker authority and must SURVIVE demotion).
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
from taskq.worker.leader import MaintenanceLeader

_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"


# ── Doubles ──────────────────────────────────────────────────────────────


class _ConnStub:
    """asyncpg.Connection stand-in with scriptable fetch/fetchval."""

    def __init__(
        self,
        *,
        fetch_rows: list[dict[str, object]] | None = None,
        fetchval_result: object = None,
    ) -> None:
        self._fetch_rows = fetch_rows if fetch_rows is not None else []
        self._fetchval_result = fetchval_result
        self.fetch_calls: list[str] = []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append(sql)
        return self._fetch_rows

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetch_calls.append(sql)
        return self._fetchval_result

    async def execute(self, sql: str, *args: object) -> str:
        return "OK"

    def is_closed(self) -> bool:
        return False


class _PoolStub:
    """Pool stand-in yielding one fixed conn (or a real conn for PG tests)."""

    def __init__(self, conn: object) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[object, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
        yield self._conn


def _settings(**env_overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": _PG_DSN}
    data.update(env_overrides)
    return WorkerSettings.load_from_dict(data, validate=False)


def _deps(*, dispatcher_pool: object, is_leader: bool) -> WorkerDeps:
    deps = WorkerDeps(
        settings=_settings(),
        dispatcher_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]  # Why: pool stand-in satisfying the acquire() surface the loop uses; same seam as the leader-sweeps coverage tests.
        heartbeat_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]
        worker_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
    )
    if is_leader:
        deps.is_leader.set()
    return deps


def _ctx(*, dispatcher_pool: object, is_leader: bool) -> SweepContext:
    return SweepContext(
        deps=_deps(dispatcher_pool=dispatcher_pool, is_leader=is_leader),
        backend=cast("Backend", object()),
        # A clock the backlog loop never consults: its arbiter is the
        # server clock inside the sampled statements.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )


async def _drive_one_sample(
    ctx: SweepContext,
) -> tuple[dict[str, int] | None, float | None]:
    """Run the loop until both gauges are fed once, then stop it.

    Spies on the obs cache-update functions via the sweeps module's own
    imported names (the established instrumentation seam) and returns the
    first (by-status, oldest-due) pair observed.
    """
    observed: list[tuple[dict[str, int], float]] = []
    original_by_status = _leader_sweeps.update_jobs_by_status_cache
    original_oldest = _leader_sweeps.update_oldest_due_age_cache

    def _spy_by_status(data: dict[str, int]) -> None:
        observed.append((dict(data), observed_oldest[0]))

    observed_oldest: list[float] = [0.0]

    def _spy_oldest(age: float) -> None:
        observed_oldest[0] = age
        if observed:
            observed[-1] = (observed[-1][0], age)

    _leader_sweeps.update_jobs_by_status_cache = _spy_by_status  # type: ignore[assignment]  # Why: test-only instrumentation of the module's imported names, same pattern as the stranded-jobs spies in the coverage tests.
    _leader_sweeps.update_oldest_due_age_cache = _spy_oldest  # type: ignore[assignment]  # Why: see above.
    shutdown = asyncio.Event()
    task = asyncio.create_task(_backlog_detection_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if observed:
                break
            await asyncio.sleep(0.01)
    finally:
        _leader_sweeps.update_jobs_by_status_cache = original_by_status  # type: ignore[assignment]  # Why: restoring the spied module attribute.
        _leader_sweeps.update_oldest_due_age_cache = original_oldest  # type: ignore[assignment]  # Why: see above.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if not observed:
        return None, None
    return observed[0]


# ── (a) UNCONDITIONAL: leadership held by nobody still feeds the gauges ──


async def test_backlog_gauges_fed_with_leadership_held_by_nobody() -> None:
    """THE #102 loop-level pin: ``is_leader`` is NEVER set on the deps —
    the exact condition of the second-schema lock loss — and the backlog
    detectors must still report. A leader gate reintroduced here is the
    detector hosted behind the failure it detects."""
    conn = _ConnStub(
        fetch_rows=[{"status": "scheduled", "count": 12}, {"status": "pending", "count": 3}],
        fetchval_result=87.5,
    )
    ctx = _ctx(dispatcher_pool=_PoolStub(conn), is_leader=False)

    by_status, oldest_due = await _drive_one_sample(ctx)

    assert by_status is not None, (
        "with leadership held by nobody the backlog sampler never fed the "
        "jobs-by-status gauge — the detector is hosted behind the failure it "
        "exists to expose"
    )
    assert by_status == {"scheduled": 12, "pending": 3}
    assert oldest_due == 87.5


async def test_backlog_none_due_reports_zero_age() -> None:
    """MIN(scheduled_at) over an empty eligible set is NULL server-side;
    the gauge must express 'nothing is due' as 0.0, not as a missing
    sample (a missing sample reads identically to a dead sampler)."""
    conn = _ConnStub(fetch_rows=[], fetchval_result=None)
    ctx = _ctx(dispatcher_pool=_PoolStub(conn), is_leader=False)

    by_status, oldest_due = await _drive_one_sample(ctx)

    assert by_status == {}
    assert oldest_due == 0.0


# ── (d) demotion: backlog authority SURVIVES, leader-scoped gauges clear ──


async def test_demotion_keeps_backlog_gauges_and_clears_leader_scoped() -> None:
    """The deliberate difference from queue depth: queue depth,
    reservation slots and stranded jobs are leader-only samplers, so
    demotion must clear them (no authority over numbers it stopped
    sampling); the backlog gauges are every-worker samplers, so demotion
    must NOT clear them — clearing would mute the detectors under the
    exact leadership failure they exist to expose."""
    import taskq.obs._otel as otel_mod
    from taskq.obs import (
        update_oldest_due_age_cache,
        update_queue_depth_cache,
        update_reservation_slots_cache,
        update_stranded_jobs_cache,
    )

    update_queue_depth_cache({"default": 4})
    update_reservation_slots_cache({"gpu": 2})
    update_stranded_jobs_cache({"orphan": 7})
    otel_mod.update_jobs_by_status_cache({"scheduled": 9})  # pyright: ignore[reportPrivateUsage]  # Why: the cache-update seams are the loop's own inputs; the public re-export covers the backlog pair being asserted.
    update_oldest_due_age_cache(42.0)
    try:
        deps = _deps(dispatcher_pool=_PoolStub(_ConnStub()), is_leader=True)
        leader = MaintenanceLeader(
            deps,
            new_uuid(),
            cast("Backend", object()),
            # A clock the demotion path never consults.
            clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        )
        await leader._close_leader_owned_conns()  # pyright: ignore[reportPrivateUsage]  # Why: driving the demotion path directly is the point of the test.

        assert deps.is_leader.is_set() is False, "demotion must clear is_leader"
        assert not _queue_depth_cache(), "queue depth must lose authority on demotion"
        assert not _reservation_cache(), "reservation slots must lose authority on demotion"
        assert not _stranded_cache(), "stranded jobs must lose authority on demotion"
        assert otel_mod._jobs_by_status_cache == {"scheduled": 9}, (  # pyright: ignore[reportPrivateUsage]  # Why: same singleton-cache read the observable callback performs.
            "the backlog gauges are every-worker samplers — demotion must keep them alive"
        )
        assert otel_mod._oldest_due_age_seconds == 42.0  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    finally:
        # Restore the process-wide sampler caches this test populated.
        update_queue_depth_cache({})
        update_reservation_slots_cache({})
        update_stranded_jobs_cache({})
        otel_mod.update_jobs_by_status_cache({})
        update_oldest_due_age_cache(0.0)


def _queue_depth_cache() -> dict[str, int]:
    import taskq.obs._otel as otel_mod

    return dict(otel_mod._queue_depth_cache)  # pyright: ignore[reportPrivateUsage]  # Why: reading the singleton cache the demotion path clears.


def _reservation_cache() -> dict[str, int]:
    import taskq.obs._otel as otel_mod

    return dict(otel_mod._reservation_slots_cache)  # pyright: ignore[reportPrivateUsage]  # Why: see above.


def _stranded_cache() -> dict[str, int]:
    import taskq.obs._otel as otel_mod

    return dict(otel_mod._stranded_jobs_cache)  # pyright: ignore[reportPrivateUsage]  # Why: see above.


# ── (b)/(c) the real statements against a real schema ────────────────────


async def _seed_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str,
    scheduled_at: datetime,
) -> UUID:
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
        "retry_kind, status, priority, scheduled_at) "
        "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 1, 'non_retryable', "
        "$2, 0, $3)",
        job_id,
        status,
        scheduled_at,
    )
    return job_id


def _pg_ctx(
    conn: asyncpg.Connection,
    *,
    schema: str,
    is_leader: bool,
) -> SweepContext:
    """SweepContext for the real-schema tests: the sampler's two statements
    run on the fixture's real connection, against the fixture's schema."""
    settings = _settings()
    settings.schema_name = schema
    settings.queue_depth_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_PoolStub(conn),  # pyright: ignore[reportArgumentType]  # Why: the sampler's real statements run on the fixture's real connection.
        heartbeat_pool=_PoolStub(conn),  # pyright: ignore[reportArgumentType]
        worker_pool=_PoolStub(conn),  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
    )
    if is_leader:
        deps.is_leader.set()
    return SweepContext(
        deps=deps,
        backend=cast("Backend", object()),
        # A clock the loop never consults; its arbiter is the server clock.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )


@pytest.mark.integration
async def test_oldest_due_age_and_by_status_against_real_schema(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The loop's two statements against a seeded schema: the oldest-due
    age is the seconds since the oldest DUE scheduled job became due
    (EXTRACT(EPOCH ...) over the due set only — future-scheduled and
    already-pending rows must not hold it back), and the by-status cache
    carries EVERY status present, not a merged count."""
    schema = module_pg_schema.schema_name

    now = datetime.now(UTC)
    await _seed_job(
        clean_pg_conn, schema, status="scheduled", scheduled_at=now - timedelta(seconds=30)
    )
    await _seed_job(
        clean_pg_conn, schema, status="scheduled", scheduled_at=now + timedelta(hours=1)
    )
    await _seed_job(clean_pg_conn, schema, status="pending", scheduled_at=now)

    ctx = _pg_ctx(clean_pg_conn, schema=module_pg_schema.schema_name, is_leader=False)

    by_status, oldest_due = await _drive_one_sample(ctx)

    assert by_status is not None, "the sampler never ran against the real schema"
    assert by_status == {"scheduled": 2, "pending": 1}, (
        "the by-status cache must carry every status present — a merged or "
        f"partial split is the #102 invisibility in a new shape: {by_status}"
    )
    assert oldest_due is not None and 25.0 <= oldest_due <= 45.0, (
        f"the oldest due job was seeded 30 s in the past; got {oldest_due!r} — "
        "the age must be the seconds since the OLDEST DUE job, ignoring "
        "future-scheduled rows"
    )


@pytest.mark.integration
async def test_oldest_due_age_zero_when_nothing_due(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Nothing due (only future-scheduled rows): the gauge must read 0.0 —
    the healthy promotion state — not the future rows' negative age and
    not a missing sample."""
    schema = module_pg_schema.schema_name

    now = datetime.now(UTC)
    await _seed_job(
        clean_pg_conn, schema, status="scheduled", scheduled_at=now + timedelta(hours=1)
    )

    ctx = _pg_ctx(clean_pg_conn, schema=module_pg_schema.schema_name, is_leader=False)

    _by_status, oldest_due = await _drive_one_sample(ctx)

    assert oldest_due == 0.0, f"nothing is due; the gauge must read 0.0, got {oldest_due!r}"
