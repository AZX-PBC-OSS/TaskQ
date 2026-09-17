"""Red-team attacks on two worker surfaces.

Surface A — the per-slot transactional connections: the
dispatch path acquires one slot-pool connection per job and shadows the
LOOP-registered ``asyncpg.Connection`` for the actor invocation through
``LoopScopeSlotView`` (``taskq/_di/scopes.py``), so concurrent slots can
never interleave operations on one connection (the LoopScopeSlotView
contract). The DIRECT-injection half of that claim is pinned green in
``tests/test_loop_conn_per_slot.py``; these attacks go after the seams
that pin does not cover:

- a NESTED LOOP-scoped resolution — a TRANSIENT factory whose own
  parameter injects the connection — must resolve the slot's instance
  (the ``LoopScopeSlotView`` docstring's own claim, pinned nowhere: the
  re-bound TRANSIENT resolver is the only path that carries the
  shadowed scope-containers map);
- a LOOP-scoped helper factory (the "database pools, HTTP clients" DI
  shape) bakes the ONE registered connection into a bootstrap-resolved
  singleton every concurrent slot's actor receives — the no-sharing
  rule extends to "any object derived from a shared connection (an
  enqueuer, a helper)";
- pool exhaustion under slots: every connection checked out plus one
  more acquire must be a bounded, typed failure, not an
  unbounded park;
- teardown while a slot holds its connection: the deps exit-stack unwind
  must stay bounded (``close_pool_bounded``'s graceful window then
  terminate) and must never manufacture a false ``succeeded`` row;
- the heartbeat/notify pools' independence from the transactional roles
  under exhaustion (the I-02 rule: no cross-role reuse).

Surface B — the DI scopes first-use change: ``_main`` calls the scope
bootstraps bare (``worker/_bootstrap.py``) and
``ScopeContainer.get_or_create`` awaits user-registered async factories
with no bound (``taskq/_di/scopes.py`` — the class sweep's Tier-2 item).
The bounded-bootstrap work covered every ``WorkerConnections`` factory open
(``tests/test_deps_bootstrap_bounded.py``); the DI registry's own
first-use awaits were NOT touched. The attack drives the REAL ``_main``
bootstrap with a black-holed user factory at the real seam, with a
test-side budget so a red fails in bounded time.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import timedelta
from typing import Annotated, Any

import asyncpg
import pytest
import structlog
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
from taskq._di.solver import solve_dependencies
from taskq._ids import new_base62, new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.obs import get_logger
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.health import unique_health_sock_path
from taskq.testing.pg import seed_actors
from taskq.worker._bootstrap import _maybe_open_slot_pool, _slot_pool_factory
from taskq.worker.dispatch import SlotPoolAcquireError, dispatch_one_job
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.run import _main

pytestmark = pytest.mark.integration

_WORKER_ID = new_uuid()

# The production-shaped hand-built slot pool (the _ScopeStack-driven
# tests): two consumer slots plus the readiness reserve — bootstrap's own
# sizing (max_concurrency + 1) for a two-slot worker.
_SLOT_POOL_SIZE = 3

# ── Budgets ─────────────────────────────────────────────────────────────
#
# Every budget below is sized so the production bound it watches fires
# well inside it: a budget firing (budget.expired()) is the RED result —
# production never bounded the operation on its own.

# Surface B: reload_factory_timeout shrunk so a production-side bound on
# the DI factory's first-use would fire at ~3s, far inside the budget.
_PROD_BOUND_SECS = "3"
_TEST_BUDGET_SECS = 12.0

# Surface A, exhaustion: the dispatch slot acquire waits
# dispatcher_command_timeout (settings floor is 1.0s) before raising the
# typed SlotPoolAcquireError.
_ACQUIRE_TIMEOUT_SECS = 1.0
_ACQUIRE_BUDGET_SECS = 8.0

# Surface A, teardown: the slot pool's close waits CLOSE_TIMEOUT_SECS
# (5.0, taskq._close) for in-flight connections before terminating them.
_TEARDOWN_BUDGET_SECS = 12.0


# ── Shared harness (mirrors tests/test_loop_conn_per_slot.py) ──────────


class _SlotPayload(BaseModel):
    role: str

    model_config = ConfigDict(extra="forbid")


def _make_actor_ref(
    fn: Any,
    *,
    name: str,
    dependencies: dict[str, type[object]] | None = None,
) -> Any:
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=True,
        dependencies=dependencies if dependencies is not None else {},
        payload_type=_SlotPayload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter not exercised by these tests
        retry=RetryPolicy(),
        result_ttl=None,
    )


class _ScopeStack:
    """PROCESS/THREAD/LOOP DI scopes wired like production bootstrap."""

    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> _ScopeStack:
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


async def _open_slot_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=dsn, min_size=_SLOT_POOL_SIZE, max_size=_SLOT_POOL_SIZE)


async def _seed_slot_actor(module_pg_schema: ModulePgSchema) -> None:
    """Register the slot_actor config row — the dispatch capacity gate
    inner-joins actor_config, so no stored row means no claimable job."""
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await seed_actors(conn, module_pg_schema.schema_name, actors=["slot_actor"])
    finally:
        await conn.close()


async def _claim_roles(backend: Any, roles: tuple[str, ...]) -> list[Any]:
    for role in roles:
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
        _WORKER_ID, ["default"], len(roles), timedelta(seconds=120)
    )
    assert len(claimed) == len(roles)
    return claimed


# ── Surface A, attack 1: the nested LOOP-scoped resolution ──────────────


class _TransientHelper:
    """Per-invocation helper whose factory injects the LOOP-scoped
    connection — the nested-resolution shape the slot view claims to
    shadow."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self.conn = conn


async def _transient_helper_factory(
    conn: Annotated[asyncpg.Connection, Scope.LOOP],
) -> _TransientHelper:
    return _TransientHelper(conn)


async def test_nested_transient_dependency_resolves_the_slot_connection(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A TRANSIENT-scoped factory whose own parameter injects the
    LOOP-scoped connection must resolve the SLOT's connection — the
    ``LoopScopeSlotView`` docstring claims "every nested LOOP-scoped
    resolution its dependencies trigger through the same scope-containers
    map" resolves the slot's instance, and the only path that carries the
    shadowed map into a factory's own parameters is build_actor_scope's
    re-bound TRANSIENT resolver. Nothing pins it: the existing per-slot
    pin covers direct actor-parameter injection only.

    Two concurrent slots drive the claim end-to-end: each slot's helper
    holds the connection that slot's own transaction runs on (in-transaction
    at actor entry, identical to the slot's directly-injected connection),
    the failing slot's write THROUGH THE HELPER joins its slot's
    transaction (rolled back with it), and the healthy sibling succeeds
    while the failing slot's transaction is open.
    """
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    await _seed_slot_actor(module_pg_schema)
    marker_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await marker_conn.execute("CREATE TABLE IF NOT EXISTS tq_rt_nested_marker (role text)")
        await marker_conn.execute("TRUNCATE tq_rt_nested_marker")
    finally:
        await marker_conn.close()

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    slot_pool = await _open_slot_pool(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)
        registry.register_factory(_TransientHelper, Scope.TRANSIENT, _transient_helper_factory)

        helpers_seen: dict[str, _TransientHelper] = {}
        direct_conns: dict[str, Any] = {}
        in_tx_at_entry: dict[str, bool] = {}
        a_in_tx = asyncio.Event()
        b_done = asyncio.Event()

        async def slot_actor(
            payload: _SlotPayload,
            ctx: JobContext[_SlotPayload],
            conn: asyncpg.Connection,
            helper: _TransientHelper,
        ) -> dict[str, object]:
            direct_conns[payload.role] = conn
            helpers_seen[payload.role] = helper
            # Taken at actor entry, before any sibling can interleave: the
            # helper's connection must already be inside this slot's open
            # transaction — the per-slot LOOP-scope semantics.
            in_tx_at_entry[payload.role] = bool(helper.conn.is_in_transaction())
            if payload.role == "fail":
                # A bare write through the helper's connection: whichever
                # connection this is, the write must live inside THIS slot's
                # transaction, so this slot's failure rolls it back.
                await helper.conn.execute("INSERT INTO tq_rt_nested_marker VALUES ('fail')")
                a_in_tx.set()
                await b_done.wait()
                raise RuntimeError("slot A actor raised")
            await a_in_tx.wait()
            await helper.conn.fetchval("SELECT 1")
            return {"ok": True}

        actor_ref = _make_actor_ref(
            slot_actor,
            name="slot_actor",
            dependencies={"conn": asyncpg.Connection, "helper": _TransientHelper},
        )
        deps.slot_pool = slot_pool

        async with _ScopeStack(registry) as scopes:
            # Guard the wiring: the LOOP-scope cache still holds the ONE
            # registered connection, and dispatch acquires from the slot
            # pool — the per-slot path is active.
            assert scopes.loop_scope.resolved_cache().get(asyncpg.Connection) is loop_conn
            assert deps.slot_pool is slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            claimed = await _claim_roles(backend, ("fail", "probe"))
            row_a = next(r for r in claimed if r.payload["role"] == "fail")
            row_b = next(r for r in claimed if r.payload["role"] == "probe")

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

            # Slot B's whole attempt runs while slot A's transaction is
            # open — event-driven on both ends, no timed window.
            task_a = asyncio.create_task(_dispatch(row_a))
            await asyncio.wait_for(a_in_tx.wait(), timeout=10)
            outcome_b = await _dispatch(row_b)
            b_done.set()
            outcome_a = await task_a

            # Per-slot identity through the NESTED resolution: neither
            # slot's helper received the ONE registered connection...
            assert helpers_seen["fail"].conn is not loop_conn, (
                "the failing slot's TRANSIENT factory resolved the registered "
                "LOOP-scope connection for its nested dependency — the shadowed "
                "scope-containers map did not reach the factory's own parameters"
            )
            assert helpers_seen["probe"].conn is not loop_conn, (
                "the probing slot's TRANSIENT factory resolved the registered "
                "LOOP-scope connection for its nested dependency — the shadowed "
                "scope-containers map did not reach the factory's own parameters"
            )
            # ...and the two slots did not receive each other's.
            assert helpers_seen["fail"].conn is not helpers_seen["probe"].conn, (
                "two concurrent transactional slots resolved the same connection "
                "for their nested dependency"
            )
            # The nested resolution and the direct injection agree: the
            # helper's connection IS the slot's transaction connection.
            assert helpers_seen["fail"].conn is direct_conns["fail"], (
                "the failing slot's nested resolution and its direct injection "
                "disagree about which connection this slot owns"
            )
            assert helpers_seen["probe"].conn is direct_conns["probe"], (
                "the probing slot's nested resolution and its direct injection "
                "disagree about which connection this slot owns"
            )
            assert in_tx_at_entry["fail"] is True, (
                "the failing slot's helper connection was not inside its slot's "
                "open transaction at actor entry"
            )
            assert in_tx_at_entry["probe"] is True, (
                "the probing slot's helper connection was not inside its slot's "
                "open transaction at actor entry"
            )

            assert outcome_b == "succeeded", (
                "the probing slot's healthy actor failed (outcome "
                f"{outcome_b!r}) — the nested resolution handed it a "
                "connection it could not use"
            )
            assert outcome_a in ("scheduled", "failed")

            # The failing slot's write THROUGH THE HELPER rolled back with
            # its slot's transaction — transactional consume must be
            # transactional for the actor's work through its dependencies.
            probe = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                marker_count = await probe.fetchval("SELECT count(*) FROM tq_rt_nested_marker")
            finally:
                await probe.close()
            assert marker_count == 0, (
                "the failing slot's helper write escaped its slot's "
                f"transaction ({marker_count} marker rows survive) — the "
                "nested dependency's connection was not the slot's"
            )

            row_b_after = await backend.get(row_b.id)
            assert row_b_after is not None
            assert row_b_after.status == "succeeded"
    finally:
        await slot_pool.close()
        await loop_conn.close()


# ── Surface A, attack 2: the LOOP-scoped helper singleton ───────────────


class _LoopHelper:
    """Loop-lifetime helper — the "database pools, HTTP clients" DI shape
    the scope bootstraps exist to resolve, holding the connection its
    factory injected."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self.conn = conn


def _loop_helper_factory(conn: Annotated[asyncpg.Connection, Scope.LOOP]) -> _LoopHelper:
    return _LoopHelper(conn)


async def test_loop_scoped_helper_does_not_carry_the_registered_connection_into_actors(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """ATTACK — the no-sharing rule's helper clause against the per-slot
    view's headline claim.

    A LOOP-scoped factory whose parameter injects the LOOP-registered
    connection is resolved ONCE at ``LoopScope.bootstrap`` (through the
    real scope containers, before any dispatch exists), so the singleton
    it produces bakes in whichever connection the factory saw then. The
    per-slot fix's own contract (the ``LoopScopeSlotView`` docstring)
    is that "a LOOP-registered connection never reaches two
    concurrent slots' actors", and the no-sharing rule extends
    to "any object derived from a shared connection (an enqueuer, a
    helper) is shared the same way" — a helper holding the ONE registered
    connection is exactly that object. The actor here depends ONLY on the
    helper (the realistic user shape: "my repository handles the DB"), on
    a worker whose per-slot path is ACTIVE.
    """
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    await _seed_slot_actor(module_pg_schema)
    marker_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await marker_conn.execute("CREATE TABLE IF NOT EXISTS tq_rt_loop_helper_marker (role text)")
        await marker_conn.execute("TRUNCATE tq_rt_loop_helper_marker")
    finally:
        await marker_conn.close()

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    slot_pool = await _open_slot_pool(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)
        registry.register_factory(_LoopHelper, Scope.LOOP, _loop_helper_factory)

        helpers_seen: dict[str, _LoopHelper] = {}
        a_in_tx = asyncio.Event()
        b_done = asyncio.Event()

        async def slot_actor(
            payload: _SlotPayload,
            ctx: JobContext[_SlotPayload],
            helper: _LoopHelper,
        ) -> dict[str, object]:
            helpers_seen[payload.role] = helper
            if payload.role == "fail":
                # The helper's write must join THIS slot's transaction:
                # the helper's connection must be the slot's.
                await helper.conn.execute("INSERT INTO tq_rt_loop_helper_marker VALUES ('fail')")
                a_in_tx.set()
                await b_done.wait()
                raise RuntimeError("slot A actor raised")
            await a_in_tx.wait()
            await helper.conn.fetchval("SELECT 1")
            return {"ok": True}

        actor_ref = _make_actor_ref(
            slot_actor,
            name="slot_actor",
            dependencies={"helper": _LoopHelper},
        )
        # The per-slot path wiring: bootstrap opens the dedicated pool on
        # exactly this shape (LOOP-scope connection registered,
        # max_concurrency > 1) — the premise of the attack.
        deps.slot_pool = slot_pool

        async with _ScopeStack(registry) as scopes:
            assert scopes.loop_scope.resolved_cache().get(asyncpg.Connection) is loop_conn
            assert deps.slot_pool is slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            claimed = await _claim_roles(backend, ("fail", "probe"))
            row_a = next(r for r in claimed if r.payload["role"] == "fail")
            row_b = next(r for r in claimed if r.payload["role"] == "probe")

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
            await asyncio.wait_for(a_in_tx.wait(), timeout=10)
            outcome_b = await _dispatch(row_b)
            b_done.set()
            outcome_a = await task_a

            # The attack: neither concurrent slot's actor may receive the
            # ONE registered connection through its LOOP-scoped helper.
            assert helpers_seen["fail"].conn is not loop_conn, (
                "the LOOP-scoped helper factory resolved the ONE registered "
                "LOOP-scope connection at bootstrap and handed it to a "
                "concurrent slot's actor through the shared singleton — the "
                "per-slot view's own contract says a LOOP-registered "
                "connection never reaches two concurrent slots' actors "
                "(the LoopScopeSlotView contract), and the no-sharing rule extends to "
                "any object derived from a shared connection ('an enqueuer, "
                "a helper')"
            )
            assert helpers_seen["probe"].conn is not loop_conn, (
                "the probing slot's actor received the ONE registered "
                "LOOP-scope connection through its LOOP-scoped helper — the "
                "shared-connection defect one level removed from direct "
                "injection"
            )
            assert helpers_seen["fail"].conn is not helpers_seen["probe"].conn, (
                "two concurrent slots' actors resolved the same connection "
                "through their LOOP-scoped helper"
            )

            assert outcome_b == "succeeded"
            assert outcome_a in ("scheduled", "failed")

            # The failing slot's write THROUGH THE HELPER must have joined
            # its slot's transaction and rolled back with it.
            probe = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                marker_count = await probe.fetchval("SELECT count(*) FROM tq_rt_loop_helper_marker")
            finally:
                await probe.close()
            assert marker_count == 0, (
                "the failing slot's helper write escaped its slot's "
                f"transaction ({marker_count} marker rows survive): "
                "transactional consume was not transactional for the actor's "
                "work through its LOOP-scoped helper — the helper's writes "
                "ran autonomously on the registered connection"
            )

            row_b_after = await backend.get(row_b.id)
            assert row_b_after is not None
            assert row_b_after.status == "succeeded"
    finally:
        await slot_pool.close()
        await loop_conn.close()


# ── Surface A, attack 3: bounded exhaustion under slots ─────────────────


async def test_slot_pool_exhaustion_is_a_bounded_typed_failure(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Every slot-pool connection checked out, then one more dispatch
    acquire — the bounded-acquire discipline. The acquire must time out into
    the typed :class:`SlotPoolAcquireError` within
    ``dispatcher_command_timeout`` (never an unbounded park), the job row
    must stay claimed/running (infrastructure, not a job outcome —
    lock-lease expiry reclaims it), and the failure must name the bound.

    The pool is opened through the REAL bootstrap seam
    (``_maybe_open_slot_pool`` — production's own sizing and warm open),
    and every connection of the pool's own max size is held, so the
    bounded wait is exercised against a genuinely exhausted asyncpg pool
    — the existing pin drives this path with a fake that raises
    instantly and cannot detect a dropped ``timeout=``.
    """
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    await _seed_slot_actor(module_pg_schema)

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        # Why the two mutations: the dispatch acquire reads its timeout
        # from deps.settings at dispatch time (worker/dispatch.py) and the
        # slot pool sizes itself from max_concurrency at open time
        # (worker/_bootstrap.py) — a two-slot worker gives the smallest
        # production-shaped pool (2 slots + 1 readiness reserve).
        deps.settings.max_concurrency = 2
        deps.settings.dispatcher_command_timeout = _ACQUIRE_TIMEOUT_SECS

        async with _ScopeStack(registry) as scopes:
            opened = await _maybe_open_slot_pool(
                scopes.loop_scope,
                deps.settings,
                deps,
                factory=_slot_pool_factory(deps.settings, None),
                pg_credential_provider=None,
                caller_supplied_pg_pools=False,
                log=get_logger(__name__),
            )
            assert opened is True, "the per-slot path must activate on this shape"
            assert deps.slot_pool is not None
            pool = deps.slot_pool

            # Every connection the pool can hand out is held by the test —
            # both consumer slots plus the readiness reserve.
            held: list[Any] = []
            try:
                for _ in range(pool.get_max_size()):
                    held.append(await pool.acquire())

                enqueuer = SubJobEnqueuer(
                    loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                    worker_pool=deps.worker_pool,
                    backend=backend,
                )
                actor_ref = _make_actor_ref(_noop_slot_actor, name="slot_actor", dependencies={})

                claimed = await _claim_roles(backend, ("blocked",))

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

                t0 = time.monotonic()
                budget = asyncio.timeout(_ACQUIRE_BUDGET_SECS)
                try:
                    async with budget:
                        await _dispatch(claimed[0])
                    pytest.fail("the dispatch must not run while every slot connection is held")
                except SlotPoolAcquireError as exc:
                    elapsed = time.monotonic() - t0
                    # Bounded: it waited the configured acquire timeout (not
                    # an instant failure, not a park) and named the bound.
                    assert elapsed >= _ACQUIRE_TIMEOUT_SECS * 0.9, (
                        f"the acquire failed after only {elapsed:.2f}s — it "
                        "never waited the configured bound (a different "
                        "failure than exhaustion)"
                    )
                    assert elapsed < _ACQUIRE_BUDGET_SECS
                    assert f"within {_ACQUIRE_TIMEOUT_SECS}s" in str(exc), (
                        f"the typed failure must name the acquire bound; got: {exc!r}"
                    )
                except TimeoutError:
                    pytest.fail(
                        "the dispatch slot acquire parks forever on an "
                        "exhausted pool: still suspended "
                        f"{_ACQUIRE_BUDGET_SECS:.0f}s in with "
                        f"dispatcher_command_timeout={_ACQUIRE_TIMEOUT_SECS}s "
                        "configured. The acquire must be bounded "
                        "(SlotPoolAcquireError), not an unbounded park."
                    )

                # Infrastructure, not a job outcome: the row stays
                # claimed/running and recovers by lock-lease expiry.
                row_after = await backend.get(claimed[0].id)
                assert row_after is not None
                assert row_after.status == "running", (
                    "an acquire failure is infrastructure — the job must stay "
                    f"claimed for lock-lease reclaim (status {row_after.status!r})"
                )
            finally:
                for conn in held:
                    await pool.release(conn)
    finally:
        await loop_conn.close()


async def _noop_slot_actor(
    payload: _SlotPayload, ctx: JobContext[_SlotPayload]
) -> dict[str, object]:
    return {"ok": True}


# ── Surface A, attack 4: teardown while a slot holds its connection ─────


async def test_teardown_mid_slot_is_bounded_and_preserves_the_job_outcome(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Shutdown while a slot holds its connection mid-transaction: the
    deps exit-stack unwind (the same callbacks ``open_worker_deps`` runs
    at shutdown) must stay bounded — ``close_pool_bounded`` gives the
    in-flight slot a graceful window (CLOSE_TIMEOUT_SECS) then terminates
    the held connection — and the terminated dispatch must return a
    JOB OUTCOME, never a false ``succeeded`` row and never an unhandled
    exception: the row stays running and lock-lease expiry reclaims it,
    the same loud, retryable outcome a rotation-terminate produces.

    RED finding this attack caught on the release candidate: the
    terminated dispatch ESCAPES ``dispatch_one_job`` with
    ``InterfaceError('pool is closed')`` — the generic handler's fallback
    terminal write goes through the worker pool that the SAME unwind
    closed moments later, and ``_TERMINAL_WRITE_INFRA_EXCEPTIONS``
    (PostgresError, OSError, TimeoutError) omits asyncpg's pool/conn
    lifecycle errors (``asyncpg.InterfaceError``) that the dispatch
    release path and ``POOL_INFRA_EXCEPTIONS`` already classify as
    infrastructure. The consumer loop counts a raising dispatch as an
    unhandled error — in drain mode, exit code 3 ("some jobs failed")
    for pure teardown infrastructure: a job failure that never happened.
    The same shape is reachable mid-run via a credential-rotation drain
    (bounded old-pool close underneath an in-flight dispatch).
    """
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps

    await _seed_slot_actor(module_pg_schema)

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        deps.settings.max_concurrency = 2

        in_tx = asyncio.Event()
        release_actor = asyncio.Event()

        async def hanging_slot_actor(
            payload: _SlotPayload,
            ctx: JobContext[_SlotPayload],
        ) -> dict[str, object]:
            in_tx.set()
            await release_actor.wait()
            return {"ok": True}

        actor_ref = _make_actor_ref(hanging_slot_actor, name="slot_actor", dependencies={})

        async with _ScopeStack(registry) as scopes:
            opened = await _maybe_open_slot_pool(
                scopes.loop_scope,
                deps.settings,
                deps,
                factory=_slot_pool_factory(deps.settings, None),
                pg_credential_provider=None,
                caller_supplied_pg_pools=False,
                log=get_logger(__name__),
            )
            assert opened is True
            assert deps.slot_pool is not None
            pool = deps.slot_pool

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=scopes.loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            claimed = await _claim_roles(backend, ("hang",))

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
            await asyncio.wait_for(in_tx.wait(), timeout=10)

            # The slot holds its connection inside an open transaction.
            # Run the production teardown seam: the same exit-stack unwind
            # the worker performs at shutdown.
            exit_stack = deps._exit_stack  # pyright: ignore[reportPrivateUsage]  # Why: the deps exit stack IS the teardown seam under attack — the callbacks _maybe_open_slot_pool pushed (the slot pool's bounded close) are what this test unwinds; no public accessor exists for it.
            assert exit_stack is not None, (
                "the deps exit stack must be live inside open_worker_deps"
            )
            t0 = time.monotonic()
            budget = asyncio.timeout(_TEARDOWN_BUDGET_SECS)
            try:
                async with budget:
                    await exit_stack.aclose()
            except TimeoutError:
                pytest.fail(
                    "the deps teardown parks forever while a slot holds its "
                    f"connection: still suspended {_TEARDOWN_BUDGET_SECS:.0f}s "
                    "in. close_pool_bounded must bound the slot pool's close "
                    "(graceful window then terminate), so shutdown can never "
                    "wedge on an in-flight dispatch."
                )
            teardown_elapsed = time.monotonic() - t0

            assert teardown_elapsed < _TEARDOWN_BUDGET_SECS
            # The graceful window: the close WAITED for the in-flight slot
            # (up to CLOSE_TIMEOUT_SECS) before terminating it — teardown
            # happens after in-flight work's window, not instead of it.
            assert teardown_elapsed >= 4.0, (
                f"teardown returned after only {teardown_elapsed:.2f}s — the "
                "slot pool's close never gave the in-flight dispatch its "
                "graceful window before terminating the connection"
            )
            assert pool.is_closing(), "the slot pool must be closed after teardown"

            # Let the parked dispatch unwind on its terminated connection.
            release_actor.set()
            try:
                outcome = await asyncio.wait_for(task, timeout=15.0)
            except Exception as exc:
                pytest.fail(
                    "the terminated dispatch escaped dispatch_one_job with an "
                    f"unhandled {type(exc).__name__} ({exc!r}) — the teardown "
                    "underneath an in-flight dispatch must be infrastructure, "
                    "never a job outcome. The generic handler's fallback "
                    "terminal write hit the worker pool the same unwind had "
                    "closed, and _TERMINAL_WRITE_INFRA_EXCEPTIONS omits "
                    "asyncpg's pool/conn lifecycle errors "
                    "(asyncpg.InterfaceError — 'pool is closed') that the "
                    "dispatch release path and POOL_INFRA_EXCEPTIONS already "
                    "classify as infra; the consumer loop counts a raising "
                    "dispatch as an unhandled error (drain mode: exit 3, "
                    "'some jobs failed', for pure teardown infrastructure)."
                )

            assert outcome != "succeeded", (
                "the terminated connection's dispatch manufactured a false "
                f"success (outcome {outcome!r})"
            )
            row = await backend.get(claimed[0].id)
            assert row is not None
            assert row.status != "succeeded", (
                "the terminated slot's job reported succeeded — a false "
                "success row from teardown (status "
                f"{row.status!r}); it must stay running for lock-lease reclaim"
            )
    finally:
        await loop_conn.close()


# ── Surface A, attack 5: heartbeat/notify independence (I-02) ───────────


async def test_heartbeat_and_notify_survive_transactional_role_exhaustion(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """With every transactional-role connection checked out — the slot
    pool, the dispatcher pool, and the worker pool, each at its own max
    size — the heartbeat tick and the notify connection must both stay
    healthy: the heartbeat runs on its OWN pool and notify on its OWN
    dedicated connection (the I-02 rule: no cross-role reuse — a
    transaction-carrying connection is never shared with a liveness or
    wake role). A heartbeat that "simplified" onto any transactional role
    would time out exactly here, stranding lock leases while every slot
    is busy — the misattribution shape I-02 records.
    """
    deps = clean_jobs_app.deps
    assert deps.notify_conn is not None

    loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        registry = ProviderRegistry()
        registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        deps.settings.max_concurrency = 2

        async with _ScopeStack(registry) as scopes:
            opened = await _maybe_open_slot_pool(
                scopes.loop_scope,
                deps.settings,
                deps,
                factory=_slot_pool_factory(deps.settings, None),
                pg_credential_provider=None,
                caller_supplied_pg_pools=False,
                log=get_logger(__name__),
            )
            assert opened is True
            assert deps.slot_pool is not None

            # Exhaust every transactional role: both PG role pools plus
            # the per-slot transaction pool, every connection they have.
            held: list[Any] = []
            try:
                for pool in (deps.dispatcher_pool, deps.worker_pool, deps.slot_pool):
                    for _ in range(pool.get_max_size()):
                        held.append(await pool.acquire())

                shutdown = asyncio.Event()
                hb_task = asyncio.create_task(heartbeat_loop(deps, _WORKER_ID, shutdown))
                try:
                    with structlog.testing.capture_logs() as logs:
                        deadline = time.monotonic() + 6.0
                        while time.monotonic() < deadline:
                            if any(e.get("event") == "heartbeat-tick-success" for e in logs):
                                break
                            if hb_task.done():
                                break
                            await asyncio.sleep(0.05)

                        assert any(e.get("event") == "heartbeat-tick-success" for e in logs), (
                            "no heartbeat tick completed while every "
                            "transactional-role connection was held — the "
                            "heartbeat must run on its own pool (I-02: no "
                            "cross-role reuse), or lock leases strand the "
                            "moment every slot is busy"
                        )
                        assert not any(e.get("event") == "heartbeat-miss" for e in logs), (
                            "the heartbeat missed under transactional-role "
                            "exhaustion — it borrowed (or waited on) a "
                            "transactional role's connection instead of its "
                            "own pool"
                        )
                        assert deps.heartbeat_failures == 0

                        notify_val = await asyncio.wait_for(
                            deps.notify_conn.fetchval("SELECT 1"), timeout=3.0
                        )
                        assert notify_val == 1, (
                            "the notify connection must stay responsive while "
                            "every transactional role is exhausted — wake "
                            "delivery is a dedicated role (I-02)"
                        )
                finally:
                    shutdown.set()
                    with contextlib.suppress(BaseException):
                        await asyncio.wait_for(hb_task, timeout=5.0)
            finally:
                for conn in held:
                    with contextlib.suppress(Exception):
                        await conn.close()
    finally:
        await loop_conn.close()


# ── Surface B: the DI scope bootstraps' first-use bound ─────────────────


class _HangingDbPool:
    """The realistic hang shape: a user-registered DI provider (the
    "database pools, HTTP clients" class the scope bootstraps exist to
    resolve) whose factory accepts the call and never returns — a
    black-holed credential endpoint is the canonical case."""


async def _prepare_schema(pg_dsn: str, schema: str) -> None:
    """Drop and recreate *schema*, apply migrations — the _main bootstrap
    refuses to boot on a schema with pending migrations."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


@pytest.mark.parametrize(
    "hang_scope",
    [Scope.PROCESS, Scope.LOOP],
    ids=["process-scope", "loop-scope"],
)
async def test_worker_bootstrap_di_factory_first_use_is_bounded(
    pg_dsn: str,
    hang_scope: Scope,
) -> None:
    """ATTACK — the unbounded-first-use hazard at the DI registry's own first-use seam.

    A user-registered DI async factory that accepts the call and never
    returns must fail the REAL worker bootstrap (``_main`` → the scope
    bootstraps → ``ScopeContainer.get_or_create``'s bare await) within
    the configured bound. Every ``WorkerConnections`` factory open got
    that treatment earlier (``reload_factory_timeout`` —
    ``tests/test_deps_bootstrap_bounded.py``); the DI registry's own
    factories run in the same pre-watchdog window (the loop keeps
    scheduling, so the lag watchdog never trips; the stale-tick
    detectors arm only after bootstrap) and were left bare. A red here
    is a LIVE release finding: the boot parks forever, undetected.
    """
    schema = f"trt_{new_base62()}".lower()
    await _prepare_schema(pg_dsn, schema)

    factory_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the factory hangs

    async def black_hole_factory() -> _HangingDbPool:
        factory_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    registry = ProviderRegistry()
    registry.register_factory(_HangingDbPool, hang_scope, black_hole_factory)

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            # _main starts a real HealthServer — never the shared default path.
            "health_socket_path": unique_health_sock_path("rt_di_first_use"),
            "reload_factory_timeout": _PROD_BOUND_SECS,
            "notify_listener_setup_timeout": _PROD_BOUND_SECS,
        }
    )

    try:
        budget = asyncio.timeout(_TEST_BUDGET_SECS)
        try:
            async with budget:
                await _main(settings, _registry=registry)
        except TimeoutError:
            assert factory_entered.is_set(), (
                "the boot never reached the DI factory call within the "
                "budget — a harness or container problem, not the finding"
            )
            if budget.expired():
                pytest.fail(
                    f"worker bootstrap parks forever in the {hang_scope.name}-scope "
                    "DI factory's first-use await: still suspended "
                    f"{_TEST_BUDGET_SECS:.0f}s in with "
                    f"reload_factory_timeout={_PROD_BOUND_SECS}s configured. "
                    "ScopeContainer.get_or_create awaits user-registered async "
                    "factories with no bound (taskq/_di/scopes.py) and _main "
                    "calls the scope bootstraps bare (worker/_bootstrap.py) — "
                    "the same pre-watchdog window every WorkerConnections "
                    "factory open was bounded for earlier. A "
                    "black-holed DI factory (the documented 'database pools, "
                    "HTTP clients' shape) wedges worker startup undetected."
                )
            return  # production converted the hang to its own typed bound failure
        pytest.fail("the bootstrap must not complete while the DI factory hangs")
    finally:
        await _drop_schema(pg_dsn, schema)
