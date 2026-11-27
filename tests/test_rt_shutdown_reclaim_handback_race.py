"""Regression pins for same-worker shutdown re-claim stranding rows on rc=0 exit.

When a worker is shutting down (SIGTERM / ``orchestrate_shutdown``):

1. Consumer 0 holds job ``J`` at ``(attempt=N, claim_epoch=E_N)`` in
   ``deps.active_jobs`` and commits ``mark_interrupted(..., hold=0)``, moving
   ``J`` back to ``status='pending'`` while ``active_jobs.deregister`` has not
   yet completed in ``consume_one_job``'s ``finally`` block.
2. Meanwhile, ``producer_loop`` has an in-flight ``dispatch_batch`` round
   (started before ``producer_stop_event.set()``) that commits in Postgres and
   re-claims ``J`` at ``(attempt=N+1, claim_epoch=E_{N+1})`` under the same
   ``worker_id``.
3. ``producer_loop`` observes ``producer_stop_event.is_set()`` and runs its
   exit hand-back ``drain_local_queue_to_pending(deps, worker_id)``.
4. ``drain_local_queue_to_pending`` must exclude the claim, not the row: the
   exclusion is scoped to ``(job_id, claim_epoch)`` pairs
   (``deps.active_jobs.held_claims()``), so the stale entry for
   ``claim_epoch=E_N`` does not shield the brand-new unstarted claim
   ``claim_epoch=E_{N+1}``. A bare ``job_id`` exclusion (``AND id <>
   ALL($2::uuid[])``) lets it through: the hand-back succeeds with rowcount 0
   (no error, no release-fallback log), Consumer 0 finishes deregistering, and
   the worker exits ``rc=0`` leaving ``J`` ``status='running'`` locked by the
   dead worker.

``isolate_self`` binds the same claim-scoped exclusion over held and queued
claims, and ``producer_loop`` sends a round that commits after the draining
stop straight to its exit hand-back instead of parking it in ``local_queue``,
so a post-stop claim is never left in a map nothing clears.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._di import ProviderRegistry, Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.clock import Clock, SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import get_logger
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.cancel import ActiveJobRegistry, _ActiveJob
from taskq.worker.run import di_consumer_loop, producer_loop
from taskq.worker.shutdown import drain_local_queue_to_pending, orchestrate_shutdown

pytestmark = pytest.mark.integration

_log = get_logger(__name__)


class _SlowPayload(BaseModel):
    sleep: float = 6.0


def _immediate() -> datetime:
    return datetime.now(UTC) - timedelta(seconds=1)


def _dummy_ctx(job: JobRow, worker_id: UUID, backend: Any) -> JobContext[BaseModel]:
    return JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        claim_epoch=job.claim_epoch,
        snooze_count=job.snooze_count,
        worker_id=worker_id,
        payload=_SlowPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=_log,
    )


async def test_drain_local_queue_repends_newer_claim_epoch_while_prior_attempt_still_registered(
    clean_jobs_app: JobsApp,
) -> None:
    """A stale active-jobs entry for claim_epoch=1 must not shield an unstarted
    re-claim of the same job at claim_epoch=2 from ``drain_local_queue_to_pending``."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = JobId(new_uuid())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"sleep": 6.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    # Attempt 1 is claimed and registered in active_jobs.
    first_claim = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(first_claim) == 1
    job_attempt_1 = first_claim[0]
    assert job_attempt_1.attempt == 1
    assert job_attempt_1.claim_epoch == 1

    holder_task: asyncio.Task[object] = asyncio.create_task(asyncio.sleep(60))
    try:
        entry_1 = await deps.active_jobs.register(
            job_attempt_1.id,
            holder_task,
            _dummy_ctx(job_attempt_1, worker_id, backend),
        )

        # Attempt 1 commits mark_interrupted(hold=0), returning the row to pending,
        # while entry_1 has not yet reached active_jobs.deregister().
        interrupted = await backend.mark_interrupted(
            job_attempt_1.id,
            worker_id,
            attempt=job_attempt_1.attempt,
            claim_epoch=job_attempt_1.claim_epoch,
            hold=timedelta(0),
        )
        assert interrupted == "pending"

        # An in-flight dispatch_batch on the same worker re-claims the row at
        # attempt=2, claim_epoch=2 while entry_1 (claim_epoch=1) is still registered.
        second_claim = await backend.dispatch_batch(
            worker_id, ["default"], 1, timedelta(seconds=60)
        )
        assert len(second_claim) == 1
        job_attempt_2 = second_claim[0]
        assert job_attempt_2.id == job_id
        assert job_attempt_2.attempt == 2
        assert job_attempt_2.claim_epoch == 2
        assert job_attempt_2.interrupt_count == 1

        # Producer exit hand-back runs while entry_1 (claim_epoch=1) is still in
        # active_jobs. It must re-pend the unstarted claim_epoch=2 row rather than
        # excluding it on bare job_id.
        handed_back = await drain_local_queue_to_pending(deps, worker_id)
        await deps.active_jobs.deregister(job_id, entry_1)

        row = await backend.get(job_id)
        assert row is not None
        assert handed_back == 1, (
            "drain_local_queue_to_pending excluded the unstarted claim_epoch=2 row "
            f"because stale claim_epoch=1 was still registered under the same job_id "
            f"(handed_back={handed_back}, row.status={row.status!r}, "
            f"row.locked_by_worker={row.locked_by_worker}, row.attempt={row.attempt}, "
            f"row.claim_epoch={row.claim_epoch})"
        )
        assert row.status == "pending"
        assert row.locked_by_worker is None
        assert row.attempt == 1
        assert row.interrupt_count == 1
    finally:
        holder_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await holder_task


class _GatedReclaimBackend:
    """Thin backend proxy that forces an in-flight ``dispatch_batch`` to land
    immediately after ``mark_interrupted`` commits, reproducing the exact
    producer-vs-FORCING race from ``test_rolling_release_fleet``."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.second_batch_in_flight = asyncio.Event()
        self.allow_second_batch_commit = asyncio.Event()
        self.second_batch_claimed = asyncio.Event()
        self._dispatch_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def dispatch_batch(
        self,
        worker_id: UUID,
        queues: list[str],
        limit: int,
        lock_lease: timedelta,
    ) -> list[JobRow]:
        self._dispatch_calls += 1
        if self._dispatch_calls == 2:
            self.second_batch_in_flight.set()
            await self.allow_second_batch_commit.wait()
            rows = await self._inner.dispatch_batch(worker_id, queues, limit, lock_lease)
            if rows:
                self.second_batch_claimed.set()
            return rows
        return await self._inner.dispatch_batch(worker_id, queues, limit, lock_lease)

    async def mark_interrupted(self, *args: Any, **kwargs: Any) -> Any:
        result = await self._inner.mark_interrupted(*args, **kwargs)
        # Release the in-flight dispatch_batch right after mark_interrupted commits
        # in Postgres, before consume_one_job unwinds to active_jobs.deregister().
        self.allow_second_batch_commit.set()
        return result


class _GatedDeregisterRegistry(ActiveJobRegistry):
    """Holds ``deregister`` until the caller signals that producer_loop's exit
    hand-back has completed, making the commit-to-deregister window deterministic."""

    def __init__(self) -> None:
        super().__init__()
        self.allow_deregister = asyncio.Event()

    async def deregister(self, job_id: JobId, entry: _ActiveJob) -> None:
        await self.allow_deregister.wait()
        await super().deregister(job_id, entry)


async def test_shutdown_reclaim_by_in_flight_producer_does_not_strand_row_on_rc0_exit(
    clean_jobs_app: JobsApp,
) -> None:
    """End-to-end reproduction of the rc=0 rolling-release release miss:

    An in-flight ``producer_loop`` ``dispatch_batch`` re-claims a job right after
    ``FORCING`` interrupts attempt 1 (``mark_interrupted`` committed) and before
    attempt 1 finishes ``active_jobs.deregister()``. When ``orchestrate_shutdown``
    returns 0 and all loops exit cleanly, the database row must not remain
    ``status='running'`` locked by the departed worker.
    """
    deps = clean_jobs_app.deps
    real_backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    # Use max_concurrency=2 so slot 1 runs the job while producer_loop has
    # available=1 and enters its second (in-flight) dispatch_batch round.
    deps.settings.max_concurrency = 2
    deps.settings.queues = ["default"]
    deps.settings.poll_interval = 0.05
    deps.settings.notify_enabled = False
    deps.settings.cancellation_grace_period = 1.0
    deps.settings.cleanup_grace_period = 1.0

    gated_registry = _GatedDeregisterRegistry()
    deps.active_jobs = gated_registry
    gated_backend = _GatedReclaimBackend(real_backend)

    actor_started = asyncio.Event()

    @actor(name="test_actor", queue="default")
    async def slow_actor(payload: _SlowPayload, ctx: JobContext[_SlowPayload]) -> dict[str, float]:
        del ctx
        actor_started.set()
        await asyncio.sleep(payload.sleep)
        return {"sleep": payload.sleep}

    job_id = JobId(new_uuid())
    await real_backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"sleep": 6.0},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    di_registry = ProviderRegistry()
    di_registry.register_value(Clock, Scope.PROCESS, SystemClock())
    scope_containers: dict[Scope, ProcessScope | ThreadScope | LoopScope] = {}
    resolver = make_resolver(di_registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    await process_scope.bootstrap(di_registry, deps.settings)
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    await thread_scope.bootstrap(di_registry, process_scope)
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope
    await loop_scope.bootstrap(di_registry, process_scope, thread_scope)

    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=loop_scope.resolved_cache(),
        worker_pool=deps.worker_pool,
        backend=real_backend,
    )

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=deps.settings.max_concurrency)
    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    slot_freed_event = asyncio.Event()

    producer_task = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            deps.producer_stop_event,
            backend=gated_backend,
            worker_id=worker_id,
            slot_freed_event=slot_freed_event,
        )
    )
    consumer_task = asyncio.create_task(
        di_consumer_loop(
            deps,
            local_queue,
            shutdown_event,
            backend=gated_backend,
            worker_id=worker_id,
            registry=di_registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_registry={"test_actor": slow_actor},
            enqueuer=enqueuer,
            slot_freed_event=slot_freed_event,
        )
    )

    shutdown_task: asyncio.Task[int] | None = None
    try:
        # Wait until attempt 1 is running in slow_actor AND producer_loop has
        # entered its second dispatch_batch call (in flight).
        await asyncio.wait_for(actor_started.wait(), timeout=5.0)
        await asyncio.wait_for(gated_backend.second_batch_in_flight.wait(), timeout=5.0)

        # Back-to-back SIGTERM #1 + SIGTERM #2 (escalate_event skips CANCELLING
        # grace straight to FORCING, exactly as test_rolling_release_fleet does).
        escalate_event.set()
        shutdown_task = asyncio.create_task(
            orchestrate_shutdown(
                deps,
                deps.settings,
                worker_id,
                shutdown_event,
                escalate_event,
                backend=gated_backend,
            )
        )

        # Wait until the in-flight producer re-claims the interrupted job and
        # completes its exit hand-back (producer_loop finishes), then release
        # attempt 1's deregister so the consumer and orchestrator finish cleanly.
        await asyncio.wait_for(gated_backend.second_batch_claimed.wait(), timeout=5.0)
        await asyncio.wait_for(producer_task, timeout=5.0)
        gated_registry.allow_deregister.set()

        rc = await asyncio.wait_for(shutdown_task, timeout=5.0)
        assert rc == 0

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(consumer_task, timeout=5.0)

        row = await real_backend.get(job_id)
        assert row is not None
        assert row.status == "pending" and row.locked_by_worker is None, (
            "worker exited rc=0 while leaving the re-claimed row running and locked "
            f"(status={row.status!r}, locked_by_worker={row.locked_by_worker}, "
            f"attempt={row.attempt}, claim_epoch={row.claim_epoch}, "
            f"interrupt_count={row.interrupt_count})"
        )
        assert row.attempt == 1
        assert row.interrupt_count == 1
    finally:
        gated_backend.allow_second_batch_commit.set()
        gated_registry.allow_deregister.set()
        shutdown_event.set()
        deps.producer_stop_event.set()
        tasks: list[asyncio.Task[Any]] = [producer_task, consumer_task]
        if shutdown_task is not None:
            tasks.append(shutdown_task)
        for task in tasks:
            if not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await loop_scope.shutdown()
        await thread_scope.shutdown()
        await process_scope.shutdown()


class _GatedFirstDispatch:
    """Backend proxy that parks the producer's first claim round until released."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.in_flight = asyncio.Event()
        self.allow_commit = asyncio.Event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def dispatch_batch(
        self,
        worker_id: UUID,
        queues: list[str],
        limit: int,
        lock_lease: timedelta,
    ) -> list[JobRow]:
        self.in_flight.set()
        await self.allow_commit.wait()
        return await self._inner.dispatch_batch(worker_id, queues, limit, lock_lease)


async def _enqueue_one(backend: Any) -> JobId:
    job_id = JobId(new_uuid())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )
    return job_id


async def test_claim_committing_at_the_draining_stop_is_handed_back_not_queued(
    clean_jobs_app: JobsApp,
) -> None:
    """A round that commits after the draining stop lands goes straight to the
    exit hand-back.

    Every take loop exits on ``producer_stop_event``, so a row parked in
    ``local_queue`` at that point is never dispatched - and it would sit in
    the queued map, which nothing clears once the consumers stop and which
    the isolate pass excludes. The producer must keep the claim (it is the
    only pass left that can release it) and hand it back, so the row is
    re-pended rather than left locked to a worker on its way out.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    deps.settings.notify_enabled = False
    deps.settings.max_concurrency = 2

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    job_id = await _enqueue_one(backend)
    # A sweep can hand a previously-disowned row back to this same worker.
    # The producer must re-own it before the post-stop break, or the drain's
    # deliberate disowned exclusion would shield the fresh claim forever.
    deps.disowned_jobs.add(job_id)
    gated = _GatedFirstDispatch(backend)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=2)
    shutdown_event = asyncio.Event()

    producer = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            deps.producer_stop_event,
            backend=gated,
            worker_id=worker_id,
        )
    )
    try:
        await asyncio.wait_for(gated.in_flight.wait(), timeout=5.0)
        # SIGTERM's draining entry lands while the round is in flight.
        deps.producer_stop_event.set()
        gated.allow_commit.set()
        await asyncio.wait_for(producer, timeout=5.0)

        assert job_id not in deps.disowned_jobs, (
            "the fresh claim must re-own the row before the post-stop break; "
            "otherwise the exit hand-back's disowned exclusion strands it"
        )
        assert deps.active_jobs.queued_ids() == [], (
            "the post-stop claim was parked in the queued map: no consumer "
            "exits to take it, the map never clears, and the isolate pass "
            "excludes it from its own re-pend"
        )
        assert local_queue.qsize() == 0, "the post-stop claim must not be queued"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "pending" and row.locked_by_worker is None, (
            f"the exit hand-back must re-pend the post-stop claim, got "
            f"(status={row.status!r}, locked_by_worker={row.locked_by_worker})"
        )
    finally:
        gated.allow_commit.set()
        if not producer.done():
            producer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await producer


async def test_claim_committing_at_a_bare_shutdown_stop_is_still_queued(
    clean_jobs_app: JobsApp,
) -> None:
    """The post-stop skip is gated on ``producer_stop_event`` alone.

    A bare ``shutdown_event`` exit (an external stop with no orchestration)
    runs no exit hand-back, and the consumer loops may still execute a queued
    row on their final turn. Skipping the enqueue there would strand the
    claim with neither a consumer execution nor a hand-back, so the row must
    reach ``local_queue`` as it always has.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    deps.settings.notify_enabled = False
    deps.settings.max_concurrency = 2

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    job_id = await _enqueue_one(backend)
    gated = _GatedFirstDispatch(backend)
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=2)
    shutdown_event = asyncio.Event()

    producer = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            deps.producer_stop_event,
            backend=gated,
            worker_id=worker_id,
        )
    )
    try:
        await asyncio.wait_for(gated.in_flight.wait(), timeout=5.0)
        shutdown_event.set()
        gated.allow_commit.set()
        await asyncio.wait_for(producer, timeout=5.0)

        assert local_queue.qsize() == 1, (
            "a bare shutdown_event exit must still enqueue the claim: the "
            "consumer loops may execute it on their final turn"
        )
        assert deps.active_jobs.queued_ids() == [job_id], (
            "the queued claim must stay covered by the queued map so no "
            "hand-back pass re-pends a row a local consumer may still run"
        )
    finally:
        gated.allow_commit.set()
        if not producer.done():
            producer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await producer
        # Leave the row for the fleet: the queued map is not the drain's
        # exclusion, so one pass re-pends the never-started claim.
        handed_back = await drain_local_queue_to_pending(deps, worker_id)
        assert handed_back == 1


@pytest.mark.parametrize(
    "newer_registration_first",
    [False, True],
    ids=["newer-intent-standing", "newer-registration-standing"],
)
async def test_stale_registration_cannot_displace_newer_claim_coverage(
    clean_jobs_app: JobsApp,
    newer_registration_first: bool,
) -> None:
    """A delayed stale registration must not unfence the newer claim.

    A take can be delayed before registration while its lease lapses and the
    same worker re-claims the row. Whether the newer take is still an intent
    or has already registered, installing the stale context must leave the
    newer ``(job_id, claim_epoch)`` covered. Otherwise the shutdown drain
    re-pends a row whose newer consumer is about to execute or is executing.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    job_id = await _enqueue_one(backend)
    first = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(first) == 1 and first[0].claim_epoch == 1
    stale_token = deps.active_jobs.mark_claimed(job_id, first[0].claim_epoch)

    # The older take is still pre-registration when its lease lapses. Sweep 1
    # reclaims it, and this worker's producer legitimately claims epoch 2.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = clock_timestamp() - '  # noqa: S608  # Why: the fixture-owned schema is validated; the job id is $-bound.
            "interval '1 second' WHERE id = $1",
            job_id,
        )
    assert await backend.reclaim_expired_locks(timedelta(0), timedelta(0)) == 1
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - '  # noqa: S608  # Why: the fixture-owned schema is validated; the job id is $-bound.
            "interval '1 second' WHERE id = $1",
            job_id,
        )
    second = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(second) == 1 and second[0].claim_epoch == 2
    live_token = deps.active_jobs.mark_claimed(job_id, second[0].claim_epoch)

    stale_task: asyncio.Task[object] = asyncio.create_task(asyncio.sleep(60))
    live_task: asyncio.Task[object] = asyncio.create_task(asyncio.sleep(60))
    stale_entry: _ActiveJob | None = None
    live_entry: _ActiveJob | None = None
    try:
        if newer_registration_first:
            live_entry = await deps.active_jobs.register(
                job_id,
                live_task,
                _dummy_ctx(second[0], worker_id, backend),
            )
        stale_entry = await deps.active_jobs.register(
            job_id,
            stale_task,
            _dummy_ctx(first[0], worker_id, backend),
        )
        # With newer_registration_first=False the live claim deliberately
        # remains an intent throughout the drain. With it True, the live
        # registration must survive the stale call above. No body runs.
        handed_back = await drain_local_queue_to_pending(deps, worker_id)
        row = await backend.get(job_id)
        assert row is not None
        assert handed_back == 0 and (
            row.status,
            row.locked_by_worker,
            row.claim_epoch,
        ) == ("running", worker_id, 2), (
            "the stale registration displaced the newer claim's coverage and "
            "the drain re-pended a live take: "
            f"handed_back={handed_back}, status={row.status!r}, "
            f"locked_by_worker={row.locked_by_worker}, claim_epoch={row.claim_epoch}"
        )
    finally:
        if stale_entry is not None:
            await deps.active_jobs.deregister(job_id, stale_entry)
        if live_entry is not None:
            await deps.active_jobs.deregister(job_id, live_entry)
        deps.active_jobs.resolve_claim(job_id, stale_token)
        deps.active_jobs.resolve_claim(job_id, live_token)
        for task in (stale_task, live_task):
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


async def test_stale_take_cannot_displace_newer_claim_intent(
    clean_jobs_app: JobsApp,
) -> None:
    """A delayed stale take must not unfence the newer claim's intent.

    A queue ``get()`` can complete before the consumer's waiter's cleanup
    yields, so two consumers can record their takes out of queue order. If the
    older row's lease was reclaimed and queued again, the newer take can install
    epoch 2 before the older take records epoch 1. The older token must not
    replace that newer coverage: the drain would otherwise re-pend epoch 2 while
    its consumer is still in the take-to-register window.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    job_id = await _enqueue_one(backend)
    first = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(first) == 1 and first[0].claim_epoch == 1
    deps.active_jobs.mark_enqueued(job_id, first[0].claim_epoch)

    # The older local copy was taken, but its consumer has not reached
    # mark_claimed yet. Its lease lapses and this worker claims the row again.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = clock_timestamp() - '  # noqa: S608  # Why: the fixture-owned schema is validated; the job id is $-bound.
            "interval '1 second' WHERE id = $1",
            job_id,
        )
    assert await backend.reclaim_expired_locks(timedelta(0), timedelta(0)) == 1
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - '  # noqa: S608  # Why: the fixture-owned schema is validated; the job id is $-bound.
            "interval '1 second' WHERE id = $1",
            job_id,
        )
    second = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(second) == 1 and second[0].claim_epoch == 2
    deps.active_jobs.mark_enqueued(job_id, second[0].claim_epoch)

    # The newer consumer records its take first. The older consumer then
    # resumes from its post-get cleanup and records the stale local copy.
    live_token = deps.active_jobs.mark_claimed(job_id, second[0].claim_epoch)
    stale_token = deps.active_jobs.mark_claimed(job_id, first[0].claim_epoch)
    try:
        handed_back = await drain_local_queue_to_pending(deps, worker_id)
        row = await backend.get(job_id)
        assert row is not None
        assert handed_back == 0 and (
            row.status,
            row.locked_by_worker,
            row.claim_epoch,
        ) == ("running", worker_id, 2), (
            "the stale take displaced the newer claim's only intent and the "
            "drain re-pended a live take: "
            f"handed_back={handed_back}, status={row.status!r}, "
            f"locked_by_worker={row.locked_by_worker}, claim_epoch={row.claim_epoch}"
        )
    finally:
        deps.active_jobs.resolve_claim(job_id, stale_token)
        deps.active_jobs.resolve_claim(job_id, live_token)


async def test_drain_combines_claim_and_disowned_exclusions(
    clean_jobs_app: JobsApp,
) -> None:
    """Adding epoch binds must not shift the disowned exclusion onto them.

    With a held claim, the disowned array moves from the third SQL parameter
    to the fourth. Drive that exact shape through Postgres with an unheld
    control row: the control must re-pend, while the exact held claim and the
    disowned started attempt both stay running and charged.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    held_id, disowned_id, control_id = [await _enqueue_one(backend) for _ in range(3)]
    claims = await backend.dispatch_batch(worker_id, ["default"], 3, timedelta(seconds=60))
    assert {job.id for job in claims} == {held_id, disowned_id, control_id}
    by_id = {job.id: job for job in claims}
    holder_task: asyncio.Task[object] = asyncio.create_task(asyncio.sleep(60))
    entry: _ActiveJob | None = None
    try:
        entry = await deps.active_jobs.register(
            held_id,
            holder_task,
            _dummy_ctx(by_id[held_id], worker_id, backend),
        )
        deps.disowned_jobs.add(disowned_id)

        handed_back = await drain_local_queue_to_pending(deps, worker_id)

        assert handed_back == 1, (
            "only the unheld, non-disowned control is the drain's population; "
            f"got {handed_back} re-pended rows"
        )
        held_row = await backend.get(held_id)
        disowned_row = await backend.get(disowned_id)
        control_row = await backend.get(control_id)
        assert held_row is not None and disowned_row is not None and control_row is not None
        assert (held_row.status, held_row.locked_by_worker, held_row.attempt) == (
            "running",
            worker_id,
            1,
        ), "the exact held (job_id, claim_epoch) claim must stay owned and charged"
        assert (disowned_row.status, disowned_row.locked_by_worker, disowned_row.attempt) == (
            "running",
            worker_id,
            1,
        ), "the disowned row's started attempt stays charged for the lease sweep"
        assert (control_row.status, control_row.locked_by_worker, control_row.attempt) == (
            "pending",
            None,
            0,
        ), "the unheld control proves the UPDATE ran and refunded exactly its own claim"
    finally:
        if entry is not None:
            await deps.active_jobs.deregister(held_id, entry)
        holder_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await holder_task


@pytest.mark.parametrize(
    ("held", "disowned", "expected_repended"),
    [
        (0, 0, 3),
        (1, 0, 2),
        (0, 1, 2),
        (1, 1, 1),
    ],
    ids=["neither", "held-only", "disowned-only", "held-and-disowned"],
)
async def test_drain_repends_exactly_the_rows_its_exclusions_do_not_name(
    clean_jobs_app: JobsApp,
    held: int,
    disowned: int,
    expected_repended: int,
) -> None:
    """Every reachable exclusion shape re-pends exactly the rows it spares.

    ``drain_local_queue_to_pending`` appends its two optional conjuncts in
    sequence: the held-claim pair (``$2``/``$3``) only when the registry is
    non-empty, and the disowned array numbered from the running
    ``len(params)``. Four renderings are reachable - neither conjunct, held
    only, disowned only, both - and each must agree with its bound arguments.

    The disowned array is the operand this matrix exists for. A bound
    argument count that disagrees with the statement raises
    ``asyncpg.InterfaceError``, a member of ``TRANSIENT_PG_ERRORS``, so the
    helper logs the failure at error level (``statement_contract_error=True``)
    and returns 0: "the statement never ran" and "nothing to hand back" are
    indistinguishable from the row state alone.
    The unheld, non-disowned control row proves the statement ran and
    refunded exactly its own claim, while the excluded rows keep their
    claim.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    ids = [await _enqueue_one(backend) for _ in range(3)]
    claims = await backend.dispatch_batch(worker_id, ["default"], 3, timedelta(seconds=60))
    assert {job.id for job in claims} == set(ids)
    by_id = {job.id: job for job in claims}

    holder_task: asyncio.Task[object] = asyncio.create_task(asyncio.sleep(60))
    entries: list[_ActiveJob] = []
    try:
        for job_id in ids[:held]:
            entries.append(
                await deps.active_jobs.register(
                    job_id,
                    holder_task,
                    _dummy_ctx(by_id[job_id], worker_id, backend),
                )
            )
        for job_id in ids[held : held + disowned]:
            deps.disowned_jobs.add(job_id)

        handed_back = await drain_local_queue_to_pending(deps, worker_id)

        assert handed_back == expected_repended, (
            f"held={held} disowned={disowned} re-pended {handed_back} rows, expected "
            f"{expected_repended}: a bound argument that disagrees with the statement "
            "raises InterfaceError, which this helper swallows into a 0 rowcount"
        )
        for job_id in ids:
            row = await backend.get(job_id)
            assert row is not None
            should_be_repended = ids.index(job_id) >= held + disowned
            assert (row.status == "pending") is should_be_repended, (
                f"job {job_id} ended status={row.status!r}, "
                f"locked_by_worker={row.locked_by_worker!r}"
            )
    finally:
        for entry, job_id in zip(entries, ids, strict=False):
            await deps.active_jobs.deregister(job_id, entry)
        holder_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await holder_task
