"""Adversarial pins for the unconditional backlog-detection loop.

The loop's reason to exist: a detector hosted behind the leadership gate
emits nothing under exactly the failure it exists to expose. The pins here
attack that property at the LOOP level - leadership held by nobody must
still feed both gauges - plus the oldest-due age's real computation, the
per-status split, and the deliberate difference from queue depth on
demotion (the backlog caches are every-worker authority and must SURVIVE
demotion).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.obs import StrandedReason
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


class _ActorBacklogFailingConn(_ConnStub):
    """Fleet reads answer; the per-actor backlog read raises.

    That read - a GROUP BY over the whole pending population - is the
    widest-shaped statement in the tick and the first to hit the
    statement timeout under the incident load it exists to expose.
    """

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        if "group by actor, queue" in " ".join(sql.lower().split()):
            raise asyncpg.exceptions.QueryCanceledError(
                "canceling statement due to statement timeout"
            )
        return await super().fetch(sql, *args)


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
) -> tuple[dict[str, int] | None, float | None, int | None]:
    """Run the loop until the gauges are fed once, then stop it.

    Spies on the obs cache-update functions via the sweeps module's own
    imported names (the established instrumentation seam) and returns the
    first (by-status, oldest-due, running-lease-expired) triple observed.
    """
    observed: list[tuple[dict[str, int], float, int]] = []
    original_by_status = _leader_sweeps.update_jobs_by_status_cache
    original_oldest = _leader_sweeps.update_oldest_due_age_cache
    original_expired = _leader_sweeps.update_running_lease_expired_cache

    def _spy_by_status(data: dict[str, int]) -> None:
        observed.append((dict(data), observed_oldest[0], observed_expired[0]))

    observed_oldest: list[float] = [0.0]
    observed_expired: list[int] = [0]

    def _spy_oldest(age: float) -> None:
        observed_oldest[0] = age
        if observed:
            observed[-1] = (observed[-1][0], age, observed[-1][2])

    def _spy_expired(count: int) -> None:
        observed_expired[0] = count
        if observed:
            observed[-1] = (observed[-1][0], observed[-1][1], count)

    _leader_sweeps.update_jobs_by_status_cache = _spy_by_status  # type: ignore[assignment]  # Why: test-only instrumentation of the module's imported names, same pattern as the stranded-jobs spies in the coverage tests.
    _leader_sweeps.update_oldest_due_age_cache = _spy_oldest  # type: ignore[assignment]  # Why: see above.
    _leader_sweeps.update_running_lease_expired_cache = _spy_expired  # type: ignore[assignment]  # Why: see above.
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
        _leader_sweeps.update_running_lease_expired_cache = original_expired  # type: ignore[assignment]  # Why: see above.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if not observed:
        return None, None, None
    return observed[0]


# ── (a) UNCONDITIONAL: leadership held by nobody still feeds the gauges ──


async def test_backlog_gauges_fed_with_leadership_held_by_nobody() -> None:
    """THE loop-level pin for hosting nothing behind the gate: ``is_leader``
    is NEVER set on the deps - the exact condition of the second-schema
    lock loss - and the backlog detectors must still report. A leader gate
    reintroduced here is the detector hosted behind the failure it
    detects."""
    conn = _ConnStub(
        fetch_rows=[{"status": "scheduled", "count": 12}, {"status": "pending", "count": 3}],
        fetchval_result=87.5,
    )
    ctx = _ctx(dispatcher_pool=_PoolStub(conn), is_leader=False)

    by_status, oldest_due, expired_lease = await _drive_one_sample(ctx)

    assert by_status is not None, (
        "with leadership held by nobody the backlog sampler never fed the "
        "jobs-by-status gauge - the detector is hosted behind the failure it "
        "exists to expose"
    )
    assert by_status == {"scheduled": 12, "pending": 3}
    assert oldest_due == 87.5
    assert expired_lease == 87, (
        "the running-lease-expired gauge must be fed from the same "
        "unconditional sample - a zombie-running detector that only reports "
        "under a leader is hosted behind the leadership failure that mutes it"
    )


async def test_backlog_none_due_reports_zero_age() -> None:
    """MIN(scheduled_at) over an empty eligible set is NULL server-side;
    the gauge must express 'nothing is due' as 0.0, not as a missing
    sample (a missing sample reads identically to a dead sampler)."""
    conn = _ConnStub(fetch_rows=[], fetchval_result=None)
    ctx = _ctx(dispatcher_pool=_PoolStub(conn), is_leader=False)

    by_status, oldest_due, expired_lease = await _drive_one_sample(ctx)

    assert by_status == {}
    assert oldest_due == 0.0
    assert expired_lease == 0, (
        "a NULL aggregate (nothing running / no lease rows) must read as 0 "
        "on the running-lease-expired gauge, not as a missing sample - a "
        "missing sample reads identically to a dead sampler"
    )


# ── (e) isolation: a failed per-actor read must not starve the fleet samples ──


async def _drive_one_sample_observing_every_update(ctx: SweepContext) -> dict[str, object]:
    """Run the loop until one tick has fired every gauge update it owes,
    then stop it.

    Same instrumentation seam as ``_drive_one_sample`` (the module's own
    imported update names), but records all six updates a tick must make:
    a raise escaping mid-tick shows up here as the updates that never
    happened, not merely as a wrong value.
    """
    fed: dict[str, object] = {}
    original_by_status = _leader_sweeps.update_jobs_by_status_cache
    original_scheduled = _leader_sweeps.update_scheduled_count_cache
    original_oldest = _leader_sweeps.update_oldest_due_age_cache
    original_expired = _leader_sweeps.update_running_lease_expired_cache
    original_actor_depth = _leader_sweeps.update_actor_backlog_cache
    original_actor_age = _leader_sweeps.update_actor_oldest_pending_age_cache

    def _spy_by_status(data: dict[str, int]) -> None:
        fed["by_status"] = dict(data)

    def _spy_scheduled(count: int) -> None:
        fed["scheduled_count"] = count

    def _spy_oldest(age: float) -> None:
        fed["oldest_due_age"] = age

    def _spy_expired(count: int) -> None:
        fed["running_lease_expired"] = count

    def _spy_actor_depth(data: dict[tuple[str, str], int]) -> None:
        fed["actor_backlog"] = dict(data)

    def _spy_actor_age(data: dict[tuple[str, str], float]) -> None:
        fed["actor_oldest_pending_age"] = dict(data)

    _leader_sweeps.update_jobs_by_status_cache = _spy_by_status  # type: ignore[assignment]  # Why: test-only instrumentation of the module's imported names, same seam as _drive_one_sample's spies.
    _leader_sweeps.update_scheduled_count_cache = _spy_scheduled  # type: ignore[assignment]  # Why: see above.
    _leader_sweeps.update_oldest_due_age_cache = _spy_oldest  # type: ignore[assignment]  # Why: see above.
    _leader_sweeps.update_running_lease_expired_cache = _spy_expired  # type: ignore[assignment]  # Why: see above.
    _leader_sweeps.update_actor_backlog_cache = _spy_actor_depth  # type: ignore[assignment]  # Why: see above.
    _leader_sweeps.update_actor_oldest_pending_age_cache = _spy_actor_age  # type: ignore[assignment]  # Why: see above.
    shutdown = asyncio.Event()
    task = asyncio.create_task(_backlog_detection_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if len(fed) >= 6:
                break
            await asyncio.sleep(0.01)
    finally:
        _leader_sweeps.update_jobs_by_status_cache = original_by_status  # type: ignore[assignment]  # Why: restoring the spied module attribute.
        _leader_sweeps.update_scheduled_count_cache = original_scheduled  # type: ignore[assignment]  # Why: see above.
        _leader_sweeps.update_oldest_due_age_cache = original_oldest  # type: ignore[assignment]  # Why: see above.
        _leader_sweeps.update_running_lease_expired_cache = original_expired  # type: ignore[assignment]  # Why: see above.
        _leader_sweeps.update_actor_backlog_cache = original_actor_depth  # type: ignore[assignment]  # Why: see above.
        _leader_sweeps.update_actor_oldest_pending_age_cache = original_actor_age  # type: ignore[assignment]  # Why: see above.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return fed


async def test_actor_backlog_fetch_failure_still_feeds_the_fleet_gauges() -> None:
    """The per-actor read is isolated from the fleet-wide samples.

    Its GROUP BY over the whole pending population is the widest-shaped
    statement in the tick and the first to hit the statement timeout
    under the incident load it exists to expose. If that raise escaped
    before the fleet-wide updates, the scheduled-count and oldest-due
    gauges would freeze at their last values, the backlog-growing
    alert's `count > count offset` comparison would go false, and the
    loop would stay alive reporting stale health - a failure that looks
    like a success. The failure itself surfaces on the actor sampler's
    own log path, and the per-actor caches are rebuilt from the empty
    snapshot rather than frozen at readings the worker can no longer
    see - the series go absent for the interval, never a stale claim.
    """
    import structlog.testing

    conn = _ActorBacklogFailingConn(
        fetch_rows=[{"status": "scheduled", "count": 12}, {"status": "pending", "count": 3}],
        fetchval_result=87.5,
    )
    ctx = _ctx(dispatcher_pool=_PoolStub(conn), is_leader=False)

    with structlog.testing.capture_logs() as captured:
        fed = await _drive_one_sample_observing_every_update(ctx)

    assert fed.get("by_status") == {"scheduled": 12, "pending": 3}, (
        "a failed per-actor read must not cost the tick its jobs-by-status "
        f"sample - the updates that fired: {sorted(fed)}"
    )
    assert fed.get("scheduled_count") == 12, (
        "the scheduled-count operand of the backlog-growing alert must still "
        "update - frozen, its `count > count offset` comparison goes false "
        "while the loop stays alive"
    )
    assert fed.get("oldest_due_age") == 87.5, (
        "the oldest-due-age operand must still update beside it"
    )
    assert fed.get("running_lease_expired") == 87, (
        "the zombie-running sample must survive the per-actor read's failure"
    )
    assert fed.get("actor_backlog") == {} and fed.get("actor_oldest_pending_age") == {}, (
        "on a failed read the per-actor caches are rebuilt from the empty "
        "snapshot - the series go absent for the interval rather than freeze "
        f"at readings the worker can no longer see; got {fed!r}"
    )
    assert any(e.get("event") == "actor-backlog-sampling-failed" for e in captured), (
        "the failed read must surface on the actor sampler's own log path - "
        "a degraded tick is reported, never silent"
    )
    assert not any(e.get("event") == "backlog-detection-sampling-failed" for e in captured), (
        "the per-actor failure must be contained by its own isolation - the "
        "fleet-wide detector's failure handler firing means the raise escaped"
    )


# ── (d) demotion: backlog authority SURVIVES, leader-scoped gauges clear ──


async def test_demotion_keeps_backlog_gauges_and_clears_leader_scoped() -> None:
    """The deliberate difference from queue depth: queue depth,
    reservation slots and stranded jobs are leader-only samplers, so
    demotion must clear them (no authority over numbers it stopped
    sampling); the backlog gauges - jobs-by-status, the scheduled-count
    twin, oldest due age, expired leases and the per-(actor, queue) pair -
    are every-worker samplers, so demotion must NOT clear them - clearing
    would mute the detectors under the exact leadership failure they
    exist to expose."""
    import taskq.obs._otel as otel_mod
    from taskq.obs import (
        update_actor_backlog_cache,
        update_actor_oldest_pending_age_cache,
        update_oldest_due_age_cache,
        update_queue_depth_cache,
        update_reservation_slots_cache,
        update_running_lease_expired_cache,
        update_scheduled_count_cache,
        update_stranded_jobs_cache,
    )

    update_queue_depth_cache({"default": 4})
    update_reservation_slots_cache({"gpu": 2})
    update_stranded_jobs_cache({("orphan", "no_actor_config"): 7})
    otel_mod.update_jobs_by_status_cache({"scheduled": 9})  # pyright: ignore[reportPrivateUsage]  # Why: the cache-update seams are the loop's own inputs; the public re-export covers the backlog pair being asserted.
    update_scheduled_count_cache(9)
    update_oldest_due_age_cache(42.0)
    update_running_lease_expired_cache(5)
    update_actor_backlog_cache({("emails", "default"): 3})
    update_actor_oldest_pending_age_cache({("emails", "default"): 12.5})
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
            "the backlog gauges are every-worker samplers - demotion must keep them alive"
        )
        assert otel_mod._scheduled_count == 9, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "the scheduled-count twin is fed by the same every-worker "
            "sampler tick as jobs-by-status - demotion must keep it alive too"
        )
        assert otel_mod._oldest_due_age_seconds == 42.0  # pyright: ignore[reportPrivateUsage]  # Why: see above.
        assert otel_mod._running_lease_expired_count == 5, (  # pyright: ignore[reportPrivateUsage]  # Why: see above - the zombie-running detector is an every-worker sampler with the rest of the backlog family.
            "the running-lease-expired gauge is an every-worker sampler - "
            "demotion must keep it alive like its backlog siblings"
        )
        assert otel_mod._actor_backlog_cache == {("emails", "default"): 3}, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "the per-actor backlog depth is sampled by the same every-worker "
            "loop - demotion must keep it alive"
        )
        assert otel_mod._actor_oldest_pending_age_cache == {("emails", "default"): 12.5}, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "the per-actor oldest-pending age is sampled by the same "
            "every-worker loop - demotion must keep it alive"
        )
    finally:
        # Restore the process-wide sampler caches this test populated.
        update_queue_depth_cache({})
        update_reservation_slots_cache({})
        update_stranded_jobs_cache({})
        otel_mod.update_jobs_by_status_cache({})
        update_scheduled_count_cache(0)
        update_oldest_due_age_cache(0.0)
        update_running_lease_expired_cache(0)
        update_actor_backlog_cache({})
        update_actor_oldest_pending_age_cache({})


def _queue_depth_cache() -> dict[str, int]:
    import taskq.obs._otel as otel_mod

    return dict(otel_mod._queue_depth_cache)  # pyright: ignore[reportPrivateUsage]  # Why: reading the singleton cache the demotion path clears.


def _reservation_cache() -> dict[str, int]:
    import taskq.obs._otel as otel_mod

    return dict(otel_mod._reservation_slots_cache)  # pyright: ignore[reportPrivateUsage]  # Why: see above.


def _stranded_cache() -> dict[tuple[str, StrandedReason], int]:
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


async def _seed_running_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    locked_by_worker: UUID | None,
    lock_expires_at: datetime | None,
    actor: str = "test_actor",
    started_at: datetime | None = None,
    cancel_phase: int = 0,
) -> UUID:
    """Seed one running row with the lock columns the zombie predicate reads
    (and the actor / started_at the running-age sampler groups on, and the
    cancel_phase the zombie predicate's carve-out reads)."""
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
        "retry_kind, status, priority, scheduled_at, locked_by_worker, "
        "lock_expires_at, started_at, cancel_phase) "
        "VALUES ($1, $5, 'default', '{}'::jsonb, 1, 'non_retryable', "
        "'running', 0, $2, $3, $4, $6, $7)",
        job_id,
        datetime.now(UTC),
        locked_by_worker,
        lock_expires_at,
        actor,
        started_at,
        cancel_phase,
    )
    return job_id


async def _drive_one_running_sample(
    ctx: SweepContext,
) -> tuple[dict[str, int] | None, dict[str, float] | None]:
    """Run the loop until the per-actor running gauges (count, oldest age)
    are fed once; both come from one statement, so one tick feeds both."""
    counts: list[dict[str, int]] = []
    ages: list[dict[str, float]] = []
    original_count = _leader_sweeps.update_jobs_running_cache
    original_age = _leader_sweeps.update_actor_oldest_running_age_cache

    def _spy_count(data: Mapping[str, int]) -> None:
        counts.append(dict(data))

    def _spy_age(data: Mapping[str, float]) -> None:
        ages.append(dict(data))

    _leader_sweeps.update_jobs_running_cache = _spy_count  # type: ignore[assignment]  # Why: test-only instrumentation of the module's imported name, the file's established seam.
    _leader_sweeps.update_actor_oldest_running_age_cache = _spy_age  # type: ignore[assignment]  # Why: see above.
    shutdown = asyncio.Event()
    task = asyncio.create_task(_backlog_detection_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if counts and ages:
                break
            await asyncio.sleep(0.01)
    finally:
        _leader_sweeps.update_jobs_running_cache = original_count  # type: ignore[assignment]  # Why: restoring the spied module attribute.
        _leader_sweeps.update_actor_oldest_running_age_cache = original_age  # type: ignore[assignment]  # Why: see above.
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return (counts[0] if counts else None), (ages[0] if ages else None)


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
    (EXTRACT(EPOCH ...) over the due set only - future-scheduled and
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

    by_status, oldest_due, _expired_lease = await _drive_one_sample(ctx)

    assert by_status is not None, "the sampler never ran against the real schema"
    assert by_status == {"scheduled": 2, "pending": 1}, (
        "the by-status cache must carry every status present - a merged or "
        f"partial split is the promotion-stall invisibility in a new shape: {by_status}"
    )
    assert oldest_due is not None and 25.0 <= oldest_due <= 45.0, (
        f"the oldest due job was seeded 30 s in the past; got {oldest_due!r} - "
        "the age must be the seconds since the OLDEST DUE job, ignoring "
        "future-scheduled rows"
    )


@pytest.mark.integration
async def test_oldest_due_age_zero_when_nothing_due(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Nothing due (only future-scheduled rows): the gauge must read 0.0 -
    the healthy promotion state - not the future rows' negative age and
    not a missing sample."""
    schema = module_pg_schema.schema_name

    now = datetime.now(UTC)
    await _seed_job(
        clean_pg_conn, schema, status="scheduled", scheduled_at=now + timedelta(hours=1)
    )

    ctx = _pg_ctx(clean_pg_conn, schema=module_pg_schema.schema_name, is_leader=False)

    _by_status, oldest_due, _expired_lease = await _drive_one_sample(ctx)

    assert oldest_due == 0.0, f"nothing is due; the gauge must read 0.0, got {oldest_due!r}"


@pytest.mark.integration
async def test_running_lease_expired_counts_only_expired_running_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The zombie-running shape is visible: the expired-lease count covers
    exactly the running rows whose lease is past AND that carry no cancel
    in flight, not future leases, not pending rows, not running rows that
    hold no lease, and not cancelling rows (the reclaim sweep deliberately
    waits out the cancel grace ladder for those, so an expired lease
    mid-cancel is the protocol working, never a zombie the alert should
    page on).

    A healthy fleet drives this gauge to zero (the reclaim sweep reclaims
    expired leases within a tick or two of expiry), so a SUSTAINED
    non-zero reading - the alert this gauge feeds - means reclaim is not
    draining: work is claimed and stuck while health probes stay green.
    """
    schema = module_pg_schema.schema_name

    now = datetime.now(UTC)
    worker = new_uuid()
    # One zombie: running, lease 60 s in the past.
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=60),
    )
    # Not zombies: a running row with a live future lease…
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now + timedelta(seconds=60),
    )
    # …a running row holding no lease at all (NULL never satisfies the
    # bound)…
    await _seed_running_job(clean_pg_conn, schema, locked_by_worker=None, lock_expires_at=None)
    # …a running row PAST its lease but mid-cancel (cancel_phase = 1: the
    # reclaim sweep's grace ladder owns this row, so the carve-out, not the
    # lease bound, must exclude it)…
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now - timedelta(seconds=60),
        cancel_phase=1,
    )
    # …and a pending row whose lock columns are set (a raced write) -
    # status, not the columns, gates the zombie shape.
    await _seed_job(clean_pg_conn, schema, status="pending", scheduled_at=now)

    ctx = _pg_ctx(clean_pg_conn, schema=module_pg_schema.schema_name, is_leader=False)

    _by_status, _oldest_due, expired_lease = await _drive_one_sample(ctx)

    assert expired_lease == 1, (
        f"exactly one running row carries a past lease with no cancel in "
        f"flight; the gauge read {expired_lease!r} - the zombie-running "
        "predicate is status='running' AND lock_expires_at < now AND "
        "cancel_phase = 0, nothing broader and nothing narrower"
    )


async def test_running_jobs_are_counted_per_actor(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """taskq.jobs.running is an exact per-actor count of the running
    population - the capacity view beside the per-process active_jobs
    gauge: which actors hold the fleet's slots while pending work waits.
    Pending rows are not in it, and an actor with nothing running is
    absent rather than reported as 0."""
    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    worker = new_uuid()
    for _ in range(2):
        await _seed_running_job(
            clean_pg_conn,
            schema,
            locked_by_worker=worker,
            lock_expires_at=now + timedelta(seconds=60),
            actor="resize_image",
        )
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now + timedelta(seconds=60),
        actor="send_email",
    )
    await _seed_job(clean_pg_conn, schema, status="pending", scheduled_at=now)

    ctx = _pg_ctx(clean_pg_conn, schema=schema, is_leader=False)
    running, _ages = await _drive_one_running_sample(ctx)

    assert running == {"resize_image": 2, "send_email": 1}


async def test_oldest_running_age_is_per_actor_from_started_at(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """taskq.jobs.oldest_running_age_seconds is the age of each actor's
    oldest running attempt, measured from started_at by the server clock -
    the series that shows an attempt outliving what the actor normally
    takes when nothing (no start_to_close) will end it. An actor with a
    fresh attempt reads near 0; an actor whose oldest attempt started 90 s
    ago reads about 90 even though it also has a fresh one."""
    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    worker = new_uuid()
    for started in (now - timedelta(seconds=90), now):
        await _seed_running_job(
            clean_pg_conn,
            schema,
            locked_by_worker=worker,
            lock_expires_at=now + timedelta(seconds=60),
            actor="resize_image",
            started_at=started,
        )
    await _seed_running_job(
        clean_pg_conn,
        schema,
        locked_by_worker=worker,
        lock_expires_at=now + timedelta(seconds=60),
        actor="send_email",
        started_at=now,
    )

    ctx = _pg_ctx(clean_pg_conn, schema=schema, is_leader=False)
    running, ages = await _drive_one_running_sample(ctx)

    assert running == {"resize_image": 2, "send_email": 1}
    assert ages is not None and set(ages) == {"resize_image", "send_email"}
    assert 85.0 <= ages["resize_image"] <= 100.0, ages
    assert 0.0 <= ages["send_email"] <= 10.0, ages
