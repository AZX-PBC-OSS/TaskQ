"""Concurrent consumer slots must not share one LOOP-scope connection or one
unkeyed SubJobEnqueuer.

The documented BYO-connection pattern registers an ``asyncpg.Connection`` at
``Scope.LOOP``. ``dispatch_one_job`` resolves that one object and hands it to
every slot, and ``SubJobEnqueuer`` (constructed once per loop) carries unkeyed
per-job buffer state shared by every slot. With ``max_concurrency > 1`` — the
default is 8 — two jobs are in flight on the same worker at once, and:

- the later job's ``conn.transaction()`` nests as a SAVEPOINT inside the
  earlier job's transaction, so the earlier job's ROLLBACK silently discards
  work the later job already reported as committed (its terminal write and
  its transactional sub-enqueues), while the job row briefly read
  ``succeeded`` — a failure that looks like a success;
- a failing job's ``discard_buffer()`` clears the shared enqueuer buffer
  wholesale, deleting a sibling slot's not-yet-flushed sub-jobs.

Both tests drive the production dispatch path (``dispatch_one_job`` →
``consume_one_job`` → ``_consume_transactional``) with two concurrent slots
and assert the durable outcome a per-slot design must guarantee. The first
needs real Postgres — the behaviour originates in asyncpg's ``_top_xact``
handling and Postgres savepoint semantics.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
from taskq._di.solver import solve_dependencies
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, JobFilter
from taskq.backend.clock import SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.pg import seed_actors
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.dispatch import dispatch_one_job

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

_SLOT_POOL_SIZE = 3
"""Two consumer slots plus the readiness reserve — the shape bootstrap
sizes (max_concurrency + 1) for a two-slot worker."""


async def _open_slot_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=dsn, min_size=_SLOT_POOL_SIZE, max_size=_SLOT_POOL_SIZE)


class _SlotPayload(BaseModel):
    role: str

    model_config = ConfigDict(extra="forbid")


class _SubPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


async def _sub_actor(payload: _SubPayload) -> None: ...


def _make_actor_ref(fn: Any, *, name: str, payload_type: type[BaseModel]) -> Any:
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=True,
        dependencies={},
        payload_type=payload_type,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter not exercised by these tests
        retry=RetryPolicy(),
        result_ttl=None,
    )


class _ScopeStack:
    """PROCESS/THREAD/LOOP DI scopes wired like production bootstrap."""

    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> "_ScopeStack":
        self.registry.validate()
        scope_containers: dict[Scope, Any] = {}

        def _resolver(func: object) -> Any:
            async def _resolve() -> dict[str, object]:
                return await solve_dependencies(
                    func=func,
                    registry=self.registry,
                    scope_containers=scope_containers,
                )

            return _resolve()

        self.process_scope = ProcessScope(resolver=_resolver)
        self.thread_scope = ThreadScope(resolver=_resolver)
        self.loop_scope = LoopScope(resolver=_resolver)
        scope_containers[Scope.PROCESS] = self.process_scope
        scope_containers[Scope.THREAD] = self.thread_scope
        scope_containers[Scope.LOOP] = self.loop_scope

        settings = WorkerSettings.load_from_dict(
            {
                "PG_DSN": "postgres://u:p@localhost:5432/db",
                "LOCK_LEASE": 60,
                "HEARTBEAT_INTERVAL": 10,
            },
        )
        await self.process_scope.bootstrap(self.registry, settings)
        await self.thread_scope.bootstrap(self.registry, self.process_scope)
        await self.loop_scope.bootstrap(self.registry, self.process_scope, self.thread_scope)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


async def test_sibling_failure_does_not_roll_back_a_committed_slot(
    clean_jobs_app: JobsApp,
    module_pg_schema: Any,
) -> None:
    """Two slots on one worker, one LOOP-scope connection: slot B finishes and
    reports success while slot A is still inside its transaction; A then fails.
    B's terminal write and its transactional sub-enqueue must be durable — A's
    rollback must not reach work B already committed."""
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    # The dispatch capacity gate inner-joins actor_config — the actor needs a
    # stored row before its jobs can be claimed.
    seed_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await seed_actors(seed_conn, module_pg_schema.schema_name, actors=["slot_actor"])
    finally:
        await seed_conn.close()

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    slot_pool = await _open_slot_pool(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        sub_ref = _make_actor_ref(_sub_actor, name="sub_actor", payload_type=_SubPayload)
        committed_sub_jobs: list[Any] = []

        async def slot_actor(
            payload: _SlotPayload, ctx: JobContext[_SlotPayload]
        ) -> dict[str, object]:
            if payload.role == "fail":
                # Signal that the actor body is running — i.e. this slot's
                # transaction is open — then stay inside it long enough
                # for the sibling slot to finish.
                a_in_transaction.set()
                await asyncio.sleep(1.5)
                raise RuntimeError("slot A actor raised")
            handle = await ctx.jobs.enqueue(sub_ref, _SubPayload())
            committed_sub_jobs.append(handle.job_id)
            return {"ok": True}

        a_in_transaction = asyncio.Event()
        actor_ref = _make_actor_ref(slot_actor, name="slot_actor", payload_type=_SlotPayload)

        # The per-slot path wiring: bootstrap opens the dedicated pool
        # when a LOOP-scope connection is registered and
        # max_concurrency > 1; this test drives two concurrent slots, so
        # the pool must be on deps for dispatch to acquire from.
        deps.slot_pool = slot_pool

        async with _ScopeStack(registry) as scopes:
            # Guard the wiring: without the LOOP-scope connection in the
            # resolved cache the dispatch drops to the autonomous path and
            # the test would pass without exercising the transactional seam.
            assert scopes.loop_scope.resolved_cache().get(asyncpg.Connection) is loop_conn
            # And without the slot pool, dispatch would resolve the ONE
            # registered connection for every slot — the defect itself.
            assert deps.slot_pool is slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            for role in ("fail", "ok"):
                await backend.enqueue(
                    EnqueueArgs(
                        id=new_uuid(),
                        actor="slot_actor",
                        queue="default",
                        payload={"role": role},
                        max_attempts=3,
                        retry_kind="transient",
                        scheduled_at=None,
                    )
                )
            claimed = await backend.dispatch_batch(
                _WORKER_ID, ["default"], 2, timedelta(seconds=120)
            )
            assert len(claimed) == 2
            row_a = next(r for r in claimed if r.payload["role"] == "fail")
            row_b = next(r for r in claimed if r.payload["role"] == "ok")

            def _dispatch(row: Any) -> Any:
                return dispatch_one_job(
                    backend=backend,
                    deps=deps,
                    job=row,
                    worker_id=_WORKER_ID,
                    registry=registry,
                    process_scope=scopes.process_scope,
                    thread_scope=scopes.thread_scope,
                    loop_scope=scopes.loop_scope,
                    actor_ref=actor_ref,
                    actor_config=StubActorConfig(retry=RetryPolicy()),
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                    enqueuer=enqueuer,
                )

            task_a = asyncio.create_task(_dispatch(row_a))
            # Slot B starts only once A's transaction is open on the shared
            # connection — the interleaving max_concurrency > 1 produces.
            await asyncio.wait_for(a_in_transaction.wait(), timeout=10)
            outcome_b = await _dispatch(row_b)
            outcome_a = await task_a

            assert outcome_b == "succeeded"
            assert outcome_a in ("scheduled", "failed")

            row_b_after = await backend.get(row_b.id)
            assert row_b_after is not None
            assert row_b_after.status == "succeeded", (
                "slot B reported success but its terminal write was rolled back "
                f"by slot A's failure (status is {row_b_after.status!r})"
            )

            assert len(committed_sub_jobs) == 1
            sub_row = await backend.get(committed_sub_jobs[0])
            assert sub_row is not None, (
                "slot B's transactional sub-enqueue was discarded by slot A's "
                "rollback even though B committed"
            )
    finally:
        await slot_pool.close()
        await loop_conn.close()


async def test_sibling_failure_does_not_discard_pending_sub_jobs(
    pg_dsn: str,
) -> None:
    """A failing slot calls ``discard_buffer()`` on the loop-shared
    ``SubJobEnqueuer``; a sibling slot's buffered-but-not-yet-flushed
    sub-jobs must survive. Uses InMemoryBackend (whose sub-enqueue path
    buffers for transactional simulation) with a real LOOP-scope connection
    so the transactional consume path is taken."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    loop_conn = await asyncpg.connect(pg_dsn)
    slot_pool = await _open_slot_pool(pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        sub_ref = _make_actor_ref(_sub_actor, name="sub_actor", payload_type=_SubPayload)
        buffered_sub_jobs: list[Any] = []

        async def slot_actor(
            payload: _SlotPayload, ctx: JobContext[_SlotPayload]
        ) -> dict[str, object]:
            if payload.role == "fail":
                raise RuntimeError("slot A actor raised")
            handle = await ctx.jobs.enqueue(sub_ref, _SubPayload())
            buffered_sub_jobs.append(handle.job_id)
            sibling_buffered.set()
            # Stay inside the transaction until the sibling slot's failure
            # (and its discard_buffer) has fully played out.
            await asyncio.wait_for(sibling_failed.wait(), timeout=10)
            return {"ok": True}

        sibling_buffered = asyncio.Event()
        sibling_failed = asyncio.Event()
        actor_ref = _make_actor_ref(slot_actor, name="slot_actor", payload_type=_SlotPayload)

        class _Deps:
            def __init__(self) -> None:
                self.active_jobs = ActiveJobRegistry()
                self.worker_pool: asyncpg.Pool | None = None
                self.slot_pool: asyncpg.Pool | None = None
                self.settings = WorkerSettings.load_from_dict(
                    {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
                )
                self.settings.worker_group = "default"
                self.redis_client: Any | None = None
                self.progress_buffers: dict[Any, Any] = {}

        deps = _Deps()
        deps.slot_pool = slot_pool

        async with _ScopeStack(registry) as scopes:
            assert scopes.loop_scope.resolved_cache().get(asyncpg.Connection) is loop_conn
            assert deps.slot_pool is slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=None,
                backend=backend,
            )

            for role in ("ok", "fail"):
                await backend.enqueue(
                    EnqueueArgs(
                        id=new_uuid(),
                        actor="slot_actor",
                        queue="default",
                        payload={"role": role},
                        max_attempts=3,
                        retry_kind="transient",
                        scheduled_at=None,
                    )
                )
            claimed = await backend.dispatch_batch(
                _WORKER_ID, ["default"], 2, timedelta(seconds=120)
            )
            assert len(claimed) == 2
            row_a = next(r for r in claimed if r.payload["role"] == "fail")
            row_b = next(r for r in claimed if r.payload["role"] == "ok")

            def _dispatch(row: Any) -> Any:
                return dispatch_one_job(
                    backend=backend,
                    deps=deps,  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps stub, mirrors tests/test_dispatch_one_job.py
                    job=row,
                    worker_id=_WORKER_ID,
                    registry=registry,
                    process_scope=scopes.process_scope,
                    thread_scope=scopes.thread_scope,
                    loop_scope=scopes.loop_scope,
                    actor_ref=actor_ref,
                    actor_config=StubActorConfig(retry=RetryPolicy()),
                    clock=FakeClock(_NOW),
                    active_jobs=deps.active_jobs,
                    enqueuer=enqueuer,
                )

            # Slot B runs first so its transaction is the top one: slot A's
            # nested rollback cannot touch it, isolating the enqueuer-buffer
            # defect from the connection-nesting one covered above.
            task_b = asyncio.create_task(_dispatch(row_b))
            await asyncio.wait_for(sibling_buffered.wait(), timeout=10)
            # Precondition: B's sub-job is observably still buffered —
            # the enqueue returned a handle but nothing is stored yet.
            # Without this, an implementation that never buffers (and
            # writes sub-jobs immediately instead) passes the final
            # assertion vacuously.
            assert await backend.get(buffered_sub_jobs[0]) is None, (
                "slot B's sub-job must be pending in the transactional "
                "buffer before slot A's failure lands, not already stored"
            )
            outcome_a = await _dispatch(row_a)
            sibling_failed.set()
            outcome_b = await task_b

            assert outcome_a in ("scheduled", "failed")
            assert outcome_b == "succeeded"

            assert len(buffered_sub_jobs) == 1
            sub_row = await backend.get(buffered_sub_jobs[0])
            assert sub_row is not None, (
                "slot A's discard_buffer() cleared the shared enqueuer buffer, "
                "silently dropping slot B's pending sub-job while B succeeded"
            )
    finally:
        await slot_pool.close()
        await loop_conn.close()


async def test_snoozing_slot_does_not_re_enqueue_a_siblings_committed_sub_jobs(
    clean_jobs_app: JobsApp,
    module_pg_schema: Any,
) -> None:
    """The production half of the per-job enqueuer binding, on real PG.

    ``_pending_buffer`` only ever populates on the in-memory backend; on
    Postgres the shared state that leaks across slots is the
    transactional sub-enqueue tracking (``_loop_enqueue_args``): a job
    that snoozes drains and re-enqueues every tracked sub-job — with a
    shared enqueuer that includes a SIBLING's already-committed ones,
    duplicate work (or an id-collision failure mislabelled as the
    snoozing job's own). With per-job binding, a snoozing slot drains
    only its own (empty) list.

    Deterministic by ordering, not timing: slot B runs to completion
    (its sub-job committed) before slot A snoozes.
    """
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    seed_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await seed_actors(seed_conn, module_pg_schema.schema_name, actors=["slot_actor"])
    finally:
        await seed_conn.close()

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    slot_pool = await _open_slot_pool(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        sub_ref = _make_actor_ref(_sub_actor, name="sub_actor", payload_type=_SubPayload)
        committed_sub_jobs: list[Any] = []

        async def slot_actor(
            payload: _SlotPayload, ctx: JobContext[_SlotPayload]
        ) -> dict[str, object]:
            if payload.role == "snooze":
                raise Snooze(timedelta(seconds=1))
            handle = await ctx.jobs.enqueue(sub_ref, _SubPayload())
            committed_sub_jobs.append(handle.job_id)
            return {"ok": True}

        actor_ref = _make_actor_ref(slot_actor, name="slot_actor", payload_type=_SlotPayload)
        deps.slot_pool = slot_pool

        async with _ScopeStack(registry) as scopes:
            assert scopes.loop_scope.resolved_cache().get(asyncpg.Connection) is loop_conn
            assert deps.slot_pool is slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            for role in ("snooze", "sub"):
                await backend.enqueue(
                    EnqueueArgs(
                        id=new_uuid(),
                        actor="slot_actor",
                        queue="default",
                        payload={"role": role},
                        max_attempts=3,
                        retry_kind="transient",
                        scheduled_at=None,
                    )
                )
            claimed = await backend.dispatch_batch(
                _WORKER_ID, ["default"], 2, timedelta(seconds=120)
            )
            assert len(claimed) == 2
            row_a = next(r for r in claimed if r.payload["role"] == "snooze")
            row_b = next(r for r in claimed if r.payload["role"] == "sub")

            def _dispatch(row: Any) -> Any:
                return dispatch_one_job(
                    backend=backend,
                    deps=deps,
                    job=row,
                    worker_id=_WORKER_ID,
                    registry=registry,
                    process_scope=scopes.process_scope,
                    thread_scope=scopes.thread_scope,
                    loop_scope=scopes.loop_scope,
                    actor_ref=actor_ref,
                    actor_config=StubActorConfig(retry=RetryPolicy()),
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                    enqueuer=enqueuer,
                )

            # Slot B completes first — its transactional sub-job is
            # committed — then slot A snoozes and its drain must not
            # touch B's work.
            outcome_b = await _dispatch(row_b)
            outcome_a = await _dispatch(row_a)

            assert outcome_b == "succeeded"
            assert outcome_a == "scheduled", (
                "the snoozing slot's own outcome was hijacked by a "
                f"re-enqueue failure from a sibling's committed sub-jobs "
                f"(outcome is {outcome_a!r})"
            )

            assert len(committed_sub_jobs) == 1
            rows = await backend.list_jobs(
                JobFilter(actor="sub_actor", limit=10)  # type: ignore[arg-type]  # Why: status filter left open — the assertion counts every row for the actor, whatever its state.
            )
            assert len(rows) == 1, (
                "a sibling slot's snooze re-enqueued already-committed "
                f"transactional sub-jobs: found {len(rows)} sub_actor rows, "
                "expected exactly the one slot B committed"
            )
    finally:
        await slot_pool.close()
        await loop_conn.close()


async def test_cancelled_slot_never_leaks_a_live_transaction_connection(
    clean_jobs_app: JobsApp,
    module_pg_schema: Any,
) -> None:
    """External cancellation detaches the job's transaction task (the
    consumer shields an in-flight commit, so the task outlives the
    dispatch call). The dispatch unwind must not return that connection
    to the pool while the detached task still holds its transaction
    open: asyncpg's release-reset would roll back under the live
    transaction and hand the connection to a sibling mid-flight. A
    connection still inside its transaction at release time is
    terminated instead — the work is discarded, the row is never
    `succeeded`, and no sibling ever acquires a transaction-tainted
    connection.
    """
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    seed_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await seed_actors(seed_conn, module_pg_schema.schema_name, actors=["slot_actor"])
    finally:
        await seed_conn.close()

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    slot_pool = await _open_slot_pool(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        in_transaction = asyncio.Event()
        release_actor = asyncio.Event()

        async def slot_actor(
            payload: _SlotPayload, ctx: JobContext[_SlotPayload]
        ) -> dict[str, object]:
            in_transaction.set()
            await release_actor.wait()
            return {"ok": True}

        actor_ref = _make_actor_ref(slot_actor, name="slot_actor", payload_type=_SlotPayload)
        deps.slot_pool = slot_pool

        async with _ScopeStack(registry) as scopes:
            assert deps.slot_pool is slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await backend.enqueue(
                EnqueueArgs(
                    id=new_uuid(),
                    actor="slot_actor",
                    queue="default",
                    payload={"role": "hang"},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
            )
            claimed = await backend.dispatch_batch(
                _WORKER_ID, ["default"], 1, timedelta(seconds=120)
            )
            assert len(claimed) == 1

            def _dispatch(row: Any) -> Any:
                return dispatch_one_job(
                    backend=backend,
                    deps=deps,
                    job=row,
                    worker_id=_WORKER_ID,
                    registry=registry,
                    process_scope=scopes.process_scope,
                    thread_scope=scopes.thread_scope,
                    loop_scope=scopes.loop_scope,
                    actor_ref=actor_ref,
                    actor_config=StubActorConfig(retry=RetryPolicy()),
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                    enqueuer=enqueuer,
                )

            task = asyncio.create_task(_dispatch(claimed[0]))
            await asyncio.wait_for(in_transaction.wait(), timeout=10)

            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            # No connection the pool can hand out may carry a live
            # transaction: the detached task's connection was terminated,
            # and every acquire here opens (or reuses) a clean one.
            reacquired = await asyncio.wait_for(
                asyncio.gather(*(slot_pool.acquire() for _ in range(_SLOT_POOL_SIZE))),
                timeout=10,
            )
            try:
                for conn in reacquired:
                    assert not conn.is_in_transaction(), (
                        "a sibling acquired a connection still carrying the "
                        "cancelled slot's live transaction"
                    )
            finally:
                for conn in reacquired:
                    await slot_pool.release(conn)

            row = await backend.get(claimed[0].id)
            assert row is not None
            assert row.status != "succeeded", (
                "the cancelled slot's detached transaction landed a "
                "terminal write after cancellation — a false success"
            )

            # Let the detached task unwind on its terminated connection
            # (its terminal write fails; the outcome is retrieved by the
            # consumer's done-callback) so no task outlives the test.
            release_actor.set()
            await asyncio.sleep(0.2)
    finally:
        await slot_pool.close()
        await loop_conn.close()
