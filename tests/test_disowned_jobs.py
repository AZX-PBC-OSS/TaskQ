"""A worker stops renewing the lease of a job it could not record an outcome for.

When every attempt of a terminal write fails with an infrastructure error
the row is left ``running`` and the documented recovery is lock-lease
expiry. That recovery only works if this worker's heartbeat stops
extending the row's lease: the renewal is keyed by ``locked_by_worker``,
so without a per-worker disowned set the row would be renewed for as long
as the process lived. These tests pin the disowned set at both ends -
the consumer records it, the heartbeat honours and prunes it, and the
producer clears it when the fleet hands the row back to this worker.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import Backend, EnqueueArgs, ErrorInfo, JobId, JobRow
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.assertions import wait_for
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job, create_worker
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.run import producer_loop
from tests.conftest import _FakePool

_START = datetime(2026, 1, 1, tzinfo=UTC)
_ACTOR = "disowning_actor"


# ── Consumer side: a failed terminal write disowns the row ─────────────


class _DeadWriteBackend(InMemoryBackend):
    """Every terminal write fails with an infra error - the DB is gone for
    the duration of the dispatch, so no retry attempt can land."""

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
    ) -> JobRow:
        raise OSError("connection reset by peer")

    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        raise OSError("connection reset by peer")


class _ConsumerDeps:
    """The WorkerDeps fields consume_one_job reads, plus the disowned set."""

    def __init__(self) -> None:
        self.progress_buffers: dict[UUID, Any] = {}
        self.worker_pool: asyncpg.Pool | None = None
        self.settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        self.redis_client: Any | None = None
        self.pending_publish_tasks: set[asyncio.Task[None]] = set()
        self.disowned_jobs: set[UUID] = set()


async def _running_job(backend: InMemoryBackend) -> tuple[JobRow, UUID]:
    backend.register_actor_config(actor=_ACTOR)
    args = EnqueueArgs(
        id=new_job_id(),
        actor=_ACTOR,
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind="non_retryable",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only; the runner's own dispatch uses the same worker id.
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1
    return dispatched[0], worker_id


async def _failing_actor(job_row: object, ctx: object) -> object:
    raise ValueError("actor failed")


async def _succeeding_actor(job_row: object, ctx: object) -> object:
    return {"ok": True}


@pytest.mark.parametrize("actor", [_failing_actor, _succeeding_actor], ids=["failure", "success"])
async def test_exhausted_terminal_write_disowns_the_job(actor: object) -> None:
    """After the write budget is spent on either terminal path the row is
    still running and this worker has recorded it as disowned - the
    heartbeat's cue to stop renewing it."""
    backend = _DeadWriteBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()

    outcome = await consume_one_job(
        backend,
        job,
        worker_id,
        deps=cast(WorkerDeps, deps),
        run_actor=actor,  # type: ignore[arg-type]  # Why: the test actors take (job_row, ctx) positionally, the consumer's run_actor contract.
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=FakeClock(start=_START),
    )

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None and row.status == "running"
    assert job.id in deps.disowned_jobs, (
        "the terminal write failed and the row is still running, yet the job "
        "was not disowned - the heartbeat will keep renewing its lease and the "
        "sweep can never reclaim it while this worker lives"
    )


class _TxConn:
    """asyncpg.Connection stand-in for the transactional path."""

    class _Transaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *args: object) -> None:
            return None

    def transaction(self) -> _TxConn._Transaction:
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _DeadTxWriteBackend(_DeadWriteBackend):
    supports_transactional_simulation = True

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        raise OSError("connection reset by peer")


async def test_exhausted_transactional_success_write_disowns_the_job() -> None:
    """The transactional success write runs on the job's own connection
    and is not retried (its transaction is already aborted), so a single
    infra failure there is the exhausted case: the row stays running and
    the job is disowned."""
    backend = _DeadTxWriteBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()

    outcome = await consume_one_job(
        backend,
        job,
        worker_id,
        deps=cast(WorkerDeps, deps),
        run_actor=_succeeding_actor,
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=FakeClock(start=_START),
        transaction_conn=cast(Any, _TxConn()),
    )

    assert outcome == "failed"
    assert job.id in deps.disowned_jobs


async def test_exhausted_cancel_write_disowns_the_job() -> None:
    """An operator cancel whose terminal write fails leaves the row running
    with no owner left to move it: it is disowned, and the cancellation
    still propagates."""
    backend = _DeadWriteBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()
    actor_entered = asyncio.Event()

    async def blocking_actor(_job: object, ctx: Any) -> object:
        actor_entered.set()
        await ctx.cancel_event.wait()

    async def _dead_mark_cancelled(*args: object, **kwargs: object) -> bool:
        raise OSError("connection reset by peer")

    backend.mark_cancelled = _dead_mark_cancelled  # type: ignore[method-assign]  # Why: force the shielded cancel write onto the infra-failure path.
    active = ActiveJobRegistry()
    task = asyncio.create_task(
        consume_one_job(
            backend,
            job,
            worker_id,
            deps=cast(WorkerDeps, deps),
            run_actor=blocking_actor,
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
            active_jobs=active,
        )
    )
    await asyncio.wait_for(actor_entered.wait(), timeout=5.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert job.id in deps.disowned_jobs


async def test_landed_terminal_write_does_not_disown() -> None:
    """The disowned set is for rows this worker could not move; a landed
    write leaves it untouched (the row is terminal, nothing to renew)."""
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend)
    deps = _ConsumerDeps()

    outcome = await consume_one_job(
        backend,
        job,
        worker_id,
        deps=cast(WorkerDeps, deps),
        run_actor=_failing_actor,
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=FakeClock(start=_START),
    )

    assert outcome == "failed"
    assert deps.disowned_jobs == set()


# ── Heartbeat side: disowned rows are excluded from renewal and pruned ──


class _RecordingConn:
    """asyncpg connection stand-in recording every statement with its
    bound parameters; ``fetch`` answers the disowned-prune probe with the
    ids the test declares still held."""

    def __init__(self, *, still_held: list[UUID]) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self._still_held = still_held

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 1"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((sql, args))
        return [{"id": job_id} for job_id in self._still_held]

    def transaction(self) -> _NoopTransaction:
        return _NoopTransaction()


class _NoopTransaction:
    """Explicit-API transaction stand-in (the heartbeat tick drives the
    transaction explicitly since the fix round's command budget)."""

    def __init__(self) -> None:
        self.started = False
        self.committed = False
        self.rolled_back = False

    async def start(self) -> None:
        self.started = True

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class _AcquiredConn:
    def __init__(self, conn: _RecordingConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RecordingConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        return None


class _RecordingPool:
    def __init__(self, conn: _RecordingConn) -> None:
        self.conn = conn

    def acquire(self, *, timeout: float | None = None) -> _AcquiredConn:
        return _AcquiredConn(self.conn)


def _heartbeat_deps(conn: _RecordingConn, *, disowned: set[UUID]) -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            # 3.0 + the tiny command timeout satisfies the cascade
            # floor: 4 * (0.5 + 2 * 0.1) = 2.8 <= 3.0.
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_LOCK_LEASE": "3.0",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: not used by the heartbeat; a stand-in satisfies the field type.
        heartbeat_pool=_RecordingPool(conn),  # type: ignore[arg-type]  # Why: the recording pool is a drop-in for asyncpg.Pool here.
        worker_pool=_FakePool(),  # type: ignore[arg-type]  # Why: as above.
        notify_conn=None,
        leader_conn=None,
    )
    deps.disowned_jobs.update(disowned)
    return deps


async def _one_tick(deps: WorkerDeps, worker_id: UUID) -> None:
    import taskq.worker.heartbeat as hb_mod

    shutdown = asyncio.Event()
    tick_done = asyncio.Event()
    prev_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]  # Why: the tick-complete hook the heartbeat unit tests synchronise on.

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]  # Why: as above.
    try:
        task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
        await wait_for(tick_done, timeout=5.0)
        shutdown.set()
        await task
    finally:
        hb_mod._tick_duration.record = prev_record  # type: ignore[method-assign,reportPrivateUsage]  # Why: as above.


def _lease_renewals(conn: _RecordingConn) -> list[tuple[str, tuple[object, ...]]]:
    return [(sql, args) for sql, args in conn.execute_calls if "lock_expires_at" in sql]


async def test_heartbeat_excludes_disowned_jobs_from_lease_renewal() -> None:
    """The jobs-lock renewal binds the disowned ids and excludes them in
    its predicate, so a disowned row's lease lapses on schedule."""
    disowned = new_uuid()
    conn = _RecordingConn(still_held=[disowned])
    deps = _heartbeat_deps(conn, disowned={disowned})

    await _one_tick(deps, new_uuid())

    renewals = _lease_renewals(conn)
    assert len(renewals) == 1
    sql, args = renewals[0]
    # $4 is the renewal threshold the loop binds since the gated
    # renewal: (worker_id, lease, disowned, threshold).
    assert len(args) == 4 and list(cast(list[UUID], args[2])) == [disowned], (
        f"the lease renewal ran with {args!r}: the disowned ids are not bound, so "
        "the statement still renews every row this worker holds"
    )
    assert "$3::uuid[]" in sql
    # The gated statement, with the threshold compared server-side.
    assert "lock_expires_at <= clock_timestamp() + $4::interval" in sql


async def test_heartbeat_excludes_disowned_jobs_from_reservation_lease_renewal() -> None:
    """The reservation-slot renewal follows the same exclusion: a slot the
    consumer released for a disowned job must not be re-leased by proxy."""
    disowned = new_uuid()
    conn = _RecordingConn(still_held=[disowned])
    deps = _heartbeat_deps(conn, disowned={disowned})

    await _one_tick(deps, new_uuid())

    slot_renewals = [(sql, args) for sql, args in conn.execute_calls if "reservation_slots" in sql]
    assert len(slot_renewals) == 1
    _sql, args = slot_renewals[0]
    assert len(args) == 3 and list(cast(list[UUID], args[2])) == [disowned]


async def test_heartbeat_prunes_disowned_jobs_the_fleet_has_reclaimed() -> None:
    """Once the row no longer belongs to this worker (the sweep re-pended
    it, or another worker claimed it) its id leaves the set; an id whose
    row is still ours-and-running stays until it is."""
    reclaimed, still_ours = new_uuid(), new_uuid()
    conn = _RecordingConn(still_held=[still_ours])
    deps = _heartbeat_deps(conn, disowned={reclaimed, still_ours})
    worker_id = new_uuid()

    await _one_tick(deps, worker_id)

    assert deps.disowned_jobs == {still_ours}
    assert len(conn.fetch_calls) == 1
    probe_sql, probe_args = conn.fetch_calls[0]
    assert set(cast(list[UUID], probe_args[0])) == {reclaimed, still_ours}
    assert probe_args[1] == worker_id
    assert "locked_by_worker" in probe_sql and "'running'" in probe_sql


async def test_heartbeat_skips_the_prune_probe_when_nothing_is_disowned() -> None:
    """The common tick carries no disowned rows and must not pay a probe."""
    conn = _RecordingConn(still_held=[])
    deps = _heartbeat_deps(conn, disowned=set())

    await _one_tick(deps, new_uuid())

    assert conn.fetch_calls == []
    _sql, args = _lease_renewals(conn)[0]
    assert list(cast(list[UUID], args[2])) == []


# ── The backend's own renewal methods carry the same exclusion ──────────


async def _two_running_jobs(backend: Backend) -> tuple[UUID, list[UUID]]:
    """Two running rows locked to one worker on *backend*, whichever twin
    (``actor_a`` is seeded on both by the fixture)."""
    worker_id = new_uuid()
    ids: list[UUID] = []
    for _ in range(2):
        args = EnqueueArgs(
            id=new_job_id(),
            actor="actor_a",
            queue="default",
            payload={},
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=None,
        )
        await backend.enqueue(args)
        ids.append(args.id)
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=2, lock_lease=timedelta(seconds=60)
    )
    assert {j.id for j in dispatched} == set(ids), "fixture broken: claim"
    return worker_id, ids


async def _leases(backend: Backend, ids: list[UUID]) -> dict[UUID, datetime]:
    leases: dict[UUID, datetime] = {}
    for job_id in ids:
        row = await backend.get(job_id)
        assert row is not None and row.lock_expires_at is not None, "fixture broken: not running"
        leases[job_id] = row.lock_expires_at
    return leases


@pytest.mark.integration
async def test_backend_heartbeat_jobs_skips_the_disowned_rows(backend_pair: Backend) -> None:
    """The renewal the backend exposes is the one the worker's heartbeat
    issues: a disowned id is excluded from it on both twins, so a caller
    of the protocol cannot renew a lease the worker has given up."""
    worker_id, (disowned, sibling) = await _two_running_jobs(backend_pair)
    before = await _leases(backend_pair, [disowned, sibling])

    renewed = await backend_pair.heartbeat_jobs(
        worker_id, timedelta(seconds=120), disowned=[disowned]
    )

    assert renewed == 1
    after = await _leases(backend_pair, [disowned, sibling])
    assert after[disowned] == before[disowned]
    assert after[sibling] > before[sibling]


@pytest.mark.integration
async def test_backend_heartbeat_jobs_with_nothing_disowned_renews_every_row(
    backend_pair: Backend,
) -> None:
    worker_id, ids = await _two_running_jobs(backend_pair)
    assert await backend_pair.heartbeat_jobs(worker_id, timedelta(seconds=120)) == 2
    assert await backend_pair.heartbeat_jobs(worker_id, timedelta(seconds=120), disowned=[]) == 2
    assert await backend_pair.heartbeat_jobs(worker_id, timedelta(seconds=120), disowned=ids) == 0


# ── Producer side: a row handed back to this worker is owned again ──────


class _ClaimingBackend:
    def __init__(self, jobs: list[JobRow]) -> None:
        self._jobs = jobs

    async def dispatch_batch(
        self, *, worker_id: object, queues: object, limit: int, lock_lease: object
    ) -> list[JobRow]:
        jobs, self._jobs = self._jobs[:limit], self._jobs[limit:]
        return jobs


async def test_producer_reowns_a_disowned_job_it_claims_again() -> None:
    """The sweep re-pends a disowned row and this worker can be the one to
    claim it back; the claim makes it a live job of ours again, so its id
    must leave the disowned set before a heartbeat could skip renewing the
    new attempt's lease."""
    job = make_job_row(status="pending")
    disowned: set[UUID] = {job.id}
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=5.0,
        notify_poll_interval=5.0,
        max_concurrency=1,
    )
    deps = SimpleNamespace(
        settings=settings,
        liveness=SimpleNamespace(tick=lambda *a, **k: None, forget=lambda *a, **k: None),
        disowned_jobs=disowned,
        # The producer's availability subtracts active jobs; this
        # test's single claimed job is never registered.
        active_jobs=SimpleNamespace(count=lambda: 0),
    )
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    task = asyncio.create_task(
        producer_loop(
            deps,  # type: ignore[arg-type]  # Why: the established producer-loop unit pattern - a namespace with the fields the loop reads.
            local_queue,
            shutdown_event,
            stop_event,
            backend=cast(Backend, _ClaimingBackend([job])),
            worker_id=new_uuid(),
        )
    )
    try:
        claimed = await asyncio.wait_for(local_queue.get(), timeout=2.0)
        assert claimed.id == job.id
        assert disowned == set()
    finally:
        shutdown_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ── End to end against Postgres: the lease lapses and the sweep reclaims ─


@pytest.mark.integration
async def test_disowned_row_lease_lapses_and_the_sweep_reclaims_it(
    clean_jobs_app: JobsApp,
) -> None:
    """With the worker alive and beating, a disowned row's lease is not
    renewed while a sibling row's is; once the lease lapses the reclaim
    sweep hands the disowned row back to the fleet and leaves the sibling
    running. The next tick then prunes the reclaimed id from the set."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    lease_end = datetime.now(UTC) + timedelta(seconds=1)
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        disowned_id = await create_running_job(conn, schema, worker_id, lock_expires_at=lease_end)
        sibling_id = await create_running_job(conn, schema, worker_id, lock_expires_at=lease_end)
    deps.disowned_jobs.add(disowned_id)

    await _one_tick(deps, worker_id)

    async def _lease(job_id: UUID) -> datetime:
        async with deps.worker_pool.acquire() as conn:
            value = await conn.fetchval(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is the fixture's validated identifier.
                job_id,
            )
        assert isinstance(value, datetime)
        return value

    assert await _lease(disowned_id) == lease_end, (
        "the heartbeat renewed the disowned row's lease - the sweep can never "
        "reclaim it while this worker lives"
    )
    assert await _lease(sibling_id) > lease_end
    assert deps.disowned_jobs == {disowned_id}

    await asyncio.sleep(1.2)
    reclaimed = await backend.reclaim_expired_locks(
        timedelta(seconds=deps.settings.cancellation_grace_period),
        timedelta(seconds=deps.settings.cleanup_grace_period),
    )
    assert reclaimed == 1

    async with deps.worker_pool.acquire() as conn:
        statuses = {
            row["id"]: row["status"]
            for row in await conn.fetch(
                f'SELECT id, status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: as above.
                [disowned_id, sibling_id],
            )
        }
    assert statuses[disowned_id] == "pending"
    assert statuses[sibling_id] == "running"

    await _one_tick(deps, worker_id)
    assert deps.disowned_jobs == set()

    # The common tick binds an empty disowned array against live Postgres
    # and still renews everything this worker holds.
    sibling_lease = await _lease(sibling_id)
    await _one_tick(deps, worker_id)
    assert await _lease(sibling_id) > sibling_lease


# ── The consumer loop's own release: an unregistered actor's row ────────


async def test_failed_actor_not_found_release_disowns_the_job() -> None:
    """A row whose actor this worker does not know is released with a
    snooze write; when that write fails the row is still locked to this
    worker with nothing left to move it, so it is disowned like any other
    exhausted terminal write."""
    from datetime import datetime as _dt
    from unittest.mock import Mock

    from taskq.backend.clock import Clock
    from taskq.worker.run import di_consumer_loop

    clock = FakeClock(_dt(2025, 1, 1, tzinfo=UTC))
    process_scope = SimpleNamespace(get=lambda t: clock if t is Clock else None)
    shutdown_event = asyncio.Event()

    class _DeadSnoozeBackend:
        async def mark_snoozed(
            self,
            job_id: JobId,
            worker_id: object,
            delay: object,
            *,
            metadata_update: dict[str, object] | None = None,
            attempt: int | None = None,
        ) -> str:
            shutdown_event.set()
            raise OSError("connection reset by peer")

    job = make_job_row(status="pending")
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    await local_queue.put(job)
    disowned: set[UUID] = set()

    await asyncio.wait_for(
        di_consumer_loop(
            SimpleNamespace(
                producer_stop_event=asyncio.Event(),
                disowned_jobs=disowned,
                active_jobs=ActiveJobRegistry(),
                drain_failures=0,
            ),  # type: ignore[arg-type]  # Why: the fields the loop reads on this path; the signature still requires the full WorkerDeps.
            local_queue,
            shutdown_event,
            backend=cast(Backend, _DeadSnoozeBackend()),  # type: ignore[arg-type]  # Why: structural stand-in satisfying the one call the loop makes.
            worker_id=new_uuid(),
            registry=cast(Any, SimpleNamespace()),
            process_scope=cast(Any, process_scope),
            thread_scope=cast(Any, SimpleNamespace()),
            loop_scope=cast(Any, SimpleNamespace()),
            actor_registry={},
            enqueuer=cast(Any, Mock()),
        ),
        timeout=2.0,
    )

    assert disowned == {job.id}


# ── The dispatch path's own failure handler: a pre-actor failure ────────


async def test_exhausted_pre_actor_failure_write_disowns_the_job() -> None:
    """A job that fails before its actor runs (a payload the actor's model
    rejects) is terminalised by the dispatch path's own handler; when that
    write's budget is spent the row is disowned there too."""
    from pydantic import BaseModel, ConfigDict

    from taskq._di.registry import ProviderRegistry
    from taskq._di.scope import Scope
    from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
    from taskq.actor import ActorRef
    from taskq.backend._protocol import EnqueueArgs
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.worker.dispatch import dispatch_one_job

    class _Strict(BaseModel):
        model_config = ConfigDict(extra="forbid")

    async def _never_runs(payload: _Strict, ctx: object) -> None:
        raise AssertionError("the actor must not run on a rejected payload")

    backend = _DeadWriteBackend(clock=FakeClock(start=_START))
    backend.register_actor_config(actor=_ACTOR)
    args = EnqueueArgs(
        id=new_job_id(),
        actor=_ACTOR,
        queue="default",
        payload={"unexpected": 1},
        max_attempts=1,
        retry_kind="non_retryable",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only; the runner's own dispatch uses the same worker id.
    (job,) = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )

    registry = ProviderRegistry()
    registry.validate()
    containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, containers)
    process_scope, thread_scope, loop_scope = (
        ProcessScope(resolver=resolver),
        ThreadScope(resolver=resolver),
        LoopScope(resolver=resolver),
    )
    containers.update(
        {Scope.PROCESS: process_scope, Scope.THREAD: thread_scope, Scope.LOOP: loop_scope}
    )
    deps = _ConsumerDeps()
    deps_view = SimpleNamespace(
        active_jobs=ActiveJobRegistry(),
        slot_pool=None,
        slot_pool_connection_init=None,
        settings=deps.settings,
        worker_pool=None,
        redis_client=None,
        progress_buffers=deps.progress_buffers,
        disowned_jobs=deps.disowned_jobs,
    )
    deps_view.settings.worker_group = "default"
    actor_ref: ActorRef[_Strict, None] = ActorRef(
        name=_ACTOR,
        queue="default",
        fn=_never_runs,
        wants_ctx=True,
        dependencies={},
        payload_type=_Strict,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; never read on the failure path.
        retry=RetryPolicy(jitter=0.0),
        result_ttl=None,
    )

    outcome = await dispatch_one_job(
        backend=backend,
        deps=cast(WorkerDeps, deps_view),
        job=job,
        worker_id=worker_id,
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: pyright cannot widen the generic parameters; the runtime shape is the one dispatch reads.
        actor_config=actor_ref.config,
        clock=FakeClock(start=_START),
        active_jobs=deps_view.active_jobs,
        enqueuer=SubJobEnqueuer(backend=backend, loop_scope_resolved=None, worker_pool=None),
    )

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None and row.status == "running"
    assert job.id in deps.disowned_jobs
