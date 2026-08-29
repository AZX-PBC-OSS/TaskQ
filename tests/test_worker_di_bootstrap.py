"""Unit and integration tests for worker DI bootstrap wiring
(ProviderRegistry, ProcessScope, ThreadScope, LoopScope,
validate-before-bootstrap, scope teardown LIFO, Clock auto-registration).

Covers:
  - Bootstrap sequence happy path
  - WorkerSettings registered at PROCESS scope
  - validate() is called after all registrations, before scope.bootstrap()
  - Validate-time MissingProvider raises before TaskGroup starts
  - Validate-time DependencyCycle raises before TaskGroup starts
  - Validate-time ScopeViolation raises before TaskGroup starts
  - Scope teardown LIFO on shutdown
  - pre-registered Clock survives bootstrap
  - fresh registry auto-registers SystemClock
  - integration — worker bootstrap auto-registers SystemClock
  - integration — pre-registered FakeClock survives bootstrap
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.actor import ActorRef, actor
from taskq.backend._protocol import Backend, CancelPhase, JobRow
from taskq.backend.clock import Clock, SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import DependencyCycle, MissingProvider, ScopeViolation
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload
from taskq.testing.clock import FakeClock
from taskq.testing.health import unique_health_sock_path
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _main
from tests.conftest import _FakePool

# ── Helpers ────────────────────────────────────────────────────────


class _LoopDep:
    pass


class _ProcessDep:
    pass


class _Unregistered:
    pass


class _CycleA:
    pass


class _CycleB:
    pass


class _LoopToTransient:
    pass


def _settings(redis_url: str | None = None, **overrides: object) -> WorkerSettings:
    config: dict[str, object] = {
        "PG_DSN": "postgres://u:p@localhost:5432/db",
        "LOCK_LEASE": 60,
        "HEARTBEAT_INTERVAL": 10,
        # _main starts a real HealthServer — never the shared default path.
        "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("worker_di_bootstrap"),
    }
    if redis_url is not None:
        config["TASKQ_REDIS_URL"] = redis_url
    for key, value in overrides.items():
        config[key] = value
    return WorkerSettings.load_from_dict(config)


def _make_scopes_and_bootstrap(
    registry: ProviderRegistry,
) -> tuple[ProcessScope, ThreadScope, LoopScope]:
    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    return process_scope, thread_scope, loop_scope


async def _bootstrap_scopes(
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
) -> None:
    settings = _settings()
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)


def _backend_methods_stub() -> Backend:
    class _Methods:
        async def mark_succeeded(
            self,
            job_id: object,
            worker_id: object,
            result: object,
            fallback_result_ttl: object = None,
        ) -> bool:
            return True

        async def mark_succeeded_with_conn(
            self,
            conn: object,
            job_id: object,
            worker_id: object,
            result: object,
            fallback_result_ttl: object = None,
        ) -> bool:
            return True

        async def mark_cancelled(self, job_id: object, worker_id: object) -> bool:
            return True

        async def write_cancel_escalation(
            self, job_id: object, worker_id: object, phase: object
        ) -> bool:
            return True

        async def mark_abandoned(
            self, job_id: object, progress_seq: object = 0, progress_state: object = None
        ) -> bool:
            return True

    raw = create_autospec(_Methods, instance=True)
    return raw  # type: ignore[return-value]


def _stub_deps(settings: WorkerSettings) -> WorkerDeps:
    pool = _FakePool()
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


def _fake_install_with_holder(
    loop: asyncio.AbstractEventLoop,
    shutdown_event: asyncio.Event,
    holder: list[asyncio.Task[int]],
) -> None:
    shutdown_event.set()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_result(0)
    holder.append(fut)  # type: ignore[arg-type]


def _integration_settings(pg_dsn: str, *, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            # _main starts a real HealthServer — never the shared default path.
            "health_socket_path": unique_health_sock_path("worker_di_bootstrap"),
        },
    )


async def _run_main_with_mocked_deps(
    settings: WorkerSettings,
    *,
    _registry: ProviderRegistry | None = None,
    actor_registry: dict[str, ActorRef[Any, Any]] | None = None,
) -> int:
    fake_backend = _backend_methods_stub()
    worker_id_val = new_uuid()

    async def _fake_register(pool: object, s: WorkerSettings) -> object:
        return worker_id_val

    def _fake_install(
        loop: asyncio.AbstractEventLoop,
        deps: WorkerDeps,
        wid: object,
        sh_ev: asyncio.Event,
        esc_ev: asyncio.Event,
        backend: Backend,
        holder: list[asyncio.Task[int]],
    ) -> None:
        _fake_install_with_holder(loop, sh_ev, holder)

    async def _fake_all(*args: object, **kwargs: object) -> None:
        pass

    with (
        patch("taskq.worker._bootstrap.PostgresBackend", return_value=fake_backend),
        patch("taskq.worker._bootstrap.open_worker_deps") as mock_open,
        patch("taskq.worker.run.register_worker", side_effect=_fake_register),
        patch("taskq.worker._bootstrap.install_signal_handlers", side_effect=_fake_install),
        patch("taskq.worker._bootstrap.heartbeat_loop", side_effect=_fake_all),
        patch("taskq.worker._bootstrap.notify_listener_loop", side_effect=_fake_all),
        patch("taskq.worker._bootstrap.MaintenanceLeader") as mock_leader_cls,
        patch("taskq.worker.run.producer_loop", side_effect=_fake_all),
        patch("taskq.worker.run.consumer_loop_stub", side_effect=_fake_all),
        patch("taskq.worker.run.di_consumer_loop", side_effect=_fake_all),
        patch("taskq.worker.run.deregister_worker", new_callable=AsyncMock),
    ):
        mock_leader_instance = MagicMock()
        mock_leader_instance.run.side_effect = _fake_all
        mock_leader_cls.return_value = mock_leader_instance

        deps = _stub_deps(settings)
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = AsyncMock(return_value=None)

        return await _main(settings, actor_registry=actor_registry, _registry=_registry)


# ── Bootstrap sequence happy path ───────────────────────────────


async def test_bootstrap_happy_path() -> None:
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_factory(_ProcessDep, Scope.PROCESS, lambda: _ProcessDep())
    registry.register_factory(_LoopDep, Scope.LOOP, lambda: _LoopDep())
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    assert process_scope.get(WorkerSettings) is settings
    assert isinstance(process_scope.get(_ProcessDep), _ProcessDep)
    assert isinstance(loop_scope.get(_LoopDep), _LoopDep)
    assert thread_scope.get(_ProcessDep) is None

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── WorkerSettings registered at PROCESS scope ────────────────────


async def test_worker_settings_registered_at_process() -> None:
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.validate()

    entry = registry.get(WorkerSettings)
    assert entry.scope == Scope.PROCESS
    assert entry.kind == "value"
    assert entry.impl is settings


# ── validate() is called after all registrations, before bootstrap ─


async def test_validate_called_once_after_registrations() -> None:
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.validate()

    with pytest.raises(RuntimeError, match="sealed"):
        registry.register_value(_ProcessDep, Scope.PROCESS, _ProcessDep())


# ── Validate-time MissingProvider raises before TaskGroup starts ──


async def test_missing_provider_raises_before_taskgroup() -> None:
    @actor(name="needs_unregistered")
    async def needs_unregistered(
        payload: BaseModel,
        ctx: JobContext[BaseModel],
        dep: _Unregistered,
    ) -> None: ...

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    actors_list: list[ActorRef[Any, Any]] = [needs_unregistered]

    with pytest.raises(MissingProvider):
        registry.validate(actors=actors_list)


# ── Validate-time DependencyCycle raises before TaskGroup starts ──


async def test_dependency_cycle_raises_before_taskgroup() -> None:
    registry = ProviderRegistry()

    async def make_a(dep_b: _CycleB) -> _CycleA:
        return _CycleA()

    async def make_b(dep_a: _CycleA) -> _CycleB:
        return _CycleB()

    registry.register_factory(_CycleA, Scope.LOOP, make_a)
    registry.register_factory(_CycleB, Scope.LOOP, make_b)

    with pytest.raises(DependencyCycle):
        registry.validate()


# ── Validate-time ScopeViolation raises before TaskGroup starts ──


async def test_scope_violation_raises_before_taskgroup() -> None:
    registry = ProviderRegistry()

    class _TransientDep:
        pass

    async def make_loop_dep(trans: _TransientDep) -> _LoopToTransient:
        return _LoopToTransient()

    registry.register_factory(_TransientDep, Scope.TRANSIENT, lambda: _TransientDep())
    registry.register_factory(_LoopToTransient, Scope.LOOP, make_loop_dep)

    with pytest.raises(ScopeViolation):
        registry.validate()


# ── Scope teardown LIFO on shutdown ──────────────────────────────


async def test_scope_teardown_lifo() -> None:
    teardown_order: list[str] = []

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    async def make_process() -> AsyncIterator[_ProcessDep]:
        yield _ProcessDep()
        teardown_order.append("process")

    async def make_loop() -> AsyncIterator[_LoopDep]:
        yield _LoopDep()
        teardown_order.append("loop")

    registry.register_factory(_ProcessDep, Scope.PROCESS, make_process)
    registry.register_factory(_LoopDep, Scope.LOOP, make_loop)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    async with contextlib.AsyncExitStack() as stack:
        stack.push_async_callback(process_scope.shutdown)
        stack.push_async_callback(thread_scope.shutdown)
        stack.push_async_callback(loop_scope.shutdown)

    assert teardown_order == ["loop", "process"]


# ── Scope teardown runs via AsyncExitStack even on exception ──────


async def test_scope_teardown_on_exception() -> None:
    teardown_order: list[str] = []

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    async def make_loop() -> AsyncIterator[_LoopDep]:
        yield _LoopDep()
        teardown_order.append("loop")

    registry.register_factory(_LoopDep, Scope.LOOP, make_loop)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    with pytest.raises(RuntimeError, match="boom"):
        async with contextlib.AsyncExitStack() as stack:
            stack.push_async_callback(process_scope.shutdown)
            stack.push_async_callback(thread_scope.shutdown)
            stack.push_async_callback(loop_scope.shutdown)
            raise RuntimeError("boom")

    assert teardown_order == ["loop"]


# ── pre-registered Clock survives bootstrap ─────────────


async def test_pre_registered_clock_survives_bootstrap() -> None:
    """pre-registered Clock survives bootstrap."""
    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)

    assert registry.has_provider(Clock) is True

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    clock_entry = registry.get(Clock)
    assert clock_entry.impl is fake_clock
    assert isinstance(clock_entry.impl, FakeClock)
    assert not isinstance(clock_entry.impl, SystemClock)


# ── fresh registry auto-registers SystemClock ────────


async def test_fresh_registry_auto_registers_system_clock() -> None:
    """has_provider(Clock) False on fresh registry; guard registers SystemClock."""
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    assert registry.has_provider(Clock) is False

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    assert registry.has_provider(Clock) is True
    clock_entry = registry.get(Clock)
    assert isinstance(clock_entry.impl, SystemClock)
    assert clock_entry.scope == Scope.PROCESS


# ── bootstrap registers the Redis rate-limit pool provider ────────


async def test_bootstrap_registers_redis_pool_provider() -> None:
    """_main registers a redis.asyncio.Redis provider at LOOP scope.

    Regression: worker/dispatch.py resolves the rate-limit Redis client
    from DI; bootstrap must register it alongside RateLimitRegistry or
    Redis-backed rate limits fail at dispatch time. Registration is
    conditional on redis_url being configured because LoopScope.bootstrap
    eagerly resolves LOOP providers and get_redis_pool raises when
    redis_url is None (the no-Redis boot path is covered by the Clock
    bootstrap tests above, which run with redis_url unset).
    """
    import redis.asyncio as redis_async

    from taskq.ratelimit._provider import get_redis_pool

    registry = ProviderRegistry()
    settings = _settings(redis_url="redis://localhost:6379/0")
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    assert registry.has_provider(redis_async.Redis) is False

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    assert registry.has_provider(redis_async.Redis) is True
    redis_entry = registry.get(redis_async.Redis)
    assert redis_entry.scope == Scope.LOOP
    assert redis_entry.kind == "factory"
    assert redis_entry.impl is get_redis_pool


# ── fail fast when a Redis-backend rate limit lacks TASKQ_REDIS_URL ──


async def test_bootstrap_fails_fast_on_redis_rate_limit_without_redis_url() -> None:
    """_main raises at bootstrap when a backend="redis" rate limit is
    registered but redis_url is None.

    Regression: without the startup check, the misconfiguration surfaced
    per-dispatch as a confusing RuntimeError from get_redis_pool — after
    the job had already burned retries. Bootstrap must fail fast and name
    the offending limiter(s). The scan is scoped to actors this worker
    actually serves, so the actor declaring the limit must be registered.
    """
    from taskq.ratelimit.registry import registry as rl_registry
    from taskq.ratelimit.token_bucket import TokenBucket

    rl_registry.register(
        TokenBucket(
            name="redis_bucket_requires_url",
            capacity=10.0,
            refill_per_second=1.0,
            backend="redis",
        )
    )

    @actor(rate_limits=["redis_bucket_requires_url"])
    async def served_actor(payload: EmptyPayload) -> None:
        pass

    with pytest.raises(RuntimeError, match="redis_bucket_requires_url"):
        await _run_main_with_mocked_deps(
            _settings(),
            actor_registry={served_actor.name: served_actor},
        )


async def test_bootstrap_allows_redis_rate_limit_with_user_redis_provider() -> None:
    """A user-supplied redis.asyncio.Redis DI provider satisfies the guard.

    Regression: user provider precedence is the documented pre-guard path
    (``register_redis_pool`` skips registration when a provider exists),
    so checking only ``settings.redis_url`` crash-loops those deployments
    at boot. A registered Redis provider must count as "redis available".
    """
    import fakeredis.aioredis
    import redis.asyncio as redis_async

    from taskq.ratelimit.registry import registry as rl_registry
    from taskq.ratelimit.token_bucket import TokenBucket

    rl_registry.register(
        TokenBucket(
            name="redis_bucket_user_provider",
            capacity=10.0,
            refill_per_second=1.0,
            backend="redis",
        )
    )

    @actor(rate_limits=["redis_bucket_user_provider"])
    async def served_actor(payload: EmptyPayload) -> None:
        pass

    registry = ProviderRegistry()
    # fakeredis: a real async redis client (in-memory), not a hand-rolled double.
    registry.register_value(redis_async.Redis, Scope.LOOP, fakeredis.aioredis.FakeRedis())

    result = await _run_main_with_mocked_deps(
        _settings(),
        _registry=registry,
        actor_registry={served_actor.name: served_actor},
    )
    assert result == 0


async def test_bootstrap_fails_fast_on_keyed_redis_rate_limit_without_redis() -> None:
    """KeyedRateLimitRef(backend="redis") with no Redis configured fails fast.

    Regression: keyed refs materialize in the registry only at first
    acquire, so scanning ``rl_registry.rate_limits`` never sees them — a
    worker serving an actor with a keyed redis ref failed per-dispatch,
    which is the exact failure class the guard exists to close. The scan
    must walk served actors' keyed refs too.
    """
    from taskq.ratelimit.refs import KeyedRateLimitRef

    class _TenantPayload(BaseModel):
        tenant: str = "t"

    @actor(
        rate_limits=[
            KeyedRateLimitRef.typed(
                _TenantPayload,
                base_name="keyed-tenant-budget",
                key_fn=lambda p: p.tenant,
                capacity=5.0,
                refill_per_second=1.0,
            )
        ]
    )
    async def keyed_actor(payload: EmptyPayload) -> None:
        pass

    with pytest.raises(RuntimeError, match="keyed-tenant-budget"):
        await _run_main_with_mocked_deps(
            _settings(),
            actor_registry={keyed_actor.name: keyed_actor},
        )


async def test_bootstrap_ignores_redis_rate_limit_of_unserved_actor() -> None:
    """A redis-backed limit no served actor references must not brick the worker.

    Regression: the rate-limit registry is process-global — a shared actor
    package can register a redis-backed limit for an actor THIS worker
    does not serve. Scanning the whole registry at bootstrap crash-loops
    an unrelated worker. The guard is scoped to served actors.
    """
    from taskq.ratelimit.registry import registry as rl_registry
    from taskq.ratelimit.token_bucket import TokenBucket

    rl_registry.register(
        TokenBucket(
            name="redis_bucket_unserved",
            capacity=10.0,
            refill_per_second=1.0,
            backend="redis",
        )
    )

    result = await _run_main_with_mocked_deps(_settings())
    assert result == 0


async def test_bootstrap_clear_error_when_redis_extra_missing() -> None:
    """TASKQ_REDIS_URL set + served redis limits + [redis] extra missing
    raises an actionable bootstrap error naming the extra and the limits.

    Regression: this configuration previously surfaced as a bare
    ``MissingProvider`` at DI validate, with no hint that the fix is
    installing the extra.
    """
    from taskq.ratelimit.registry import registry as rl_registry
    from taskq.ratelimit.token_bucket import TokenBucket

    rl_registry.register(
        TokenBucket(
            name="redis_bucket_needs_extra",
            capacity=10.0,
            refill_per_second=1.0,
            backend="redis",
        )
    )

    @actor(rate_limits=["redis_bucket_needs_extra"])
    async def served_actor(payload: EmptyPayload) -> None:
        pass

    with (
        patch("taskq.worker._bootstrap._redis_extra_installed", lambda: False),
        pytest.raises(RuntimeError, match=r"taskq\[redis\]"),
    ):
        await _run_main_with_mocked_deps(
            _settings(redis_url="redis://localhost:6379/0"),
            actor_registry={served_actor.name: served_actor},
        )


async def test_bootstrap_redis_url_without_extra_and_no_redis_limits_boots() -> None:
    """TASKQ_REDIS_URL set with the extra missing but no served redis-backed
    limits is harmless: the worker boots (register_redis_pool silently skips
    and nothing requires the provider)."""
    with patch("taskq.worker._bootstrap._redis_extra_installed", lambda: False):
        result = await _run_main_with_mocked_deps(_settings(redis_url="redis://localhost:6379/0"))
    assert result == 0


# ── _served_redis_rate_limits uses the injected registry, not the singleton ──


async def test_served_redis_rate_limits_uses_injected_registry() -> None:
    """_served_redis_rate_limits scans the injected registry, not the module singleton.

    Regression (PR #39 / #42): ``_served_redis_rate_limits`` read the
    module-level ``rl_registry`` singleton instead of the resolved
    registry from ``_resolve_rl_registry``. With a custom
    ``RateLimitRegistry``:

    - **False negative:** custom registry has Redis-backed limits,
      singleton doesn't → startup check finds nothing → worker crashes
      per-dispatch with MissingProvider.
    - **False positive:** singleton has Redis-backed limits from another
      registration, custom registry doesn't → spurious RuntimeError at
      startup.

    This test exercises both directions directly against the function.
    """
    from taskq.ratelimit.registry import (
        RateLimitRegistry,
    )
    from taskq.ratelimit.registry import (
        registry as singleton_registry,
    )
    from taskq.ratelimit.token_bucket import TokenBucket
    from taskq.worker._bootstrap import _served_redis_rate_limits

    # ── False negative: custom registry has a Redis-backed limit ──
    custom = RateLimitRegistry()
    custom.register(
        TokenBucket(
            name="di_custom_redis_bucket_26",
            capacity=10.0,
            refill_per_second=1.0,
            backend="redis",
        )
    )

    @actor(rate_limits=["di_custom_redis_bucket_26"])
    async def served_by_custom(payload: EmptyPayload) -> None:
        pass

    # The custom registry has the limit → detected
    result = _served_redis_rate_limits({served_by_custom.name: served_by_custom}, custom)
    assert result == ["di_custom_redis_bucket_26"]

    # The singleton does NOT have this limit → not detected when scanning
    # the singleton (this was the bug: the old code always used the singleton)
    assert "di_custom_redis_bucket_26" not in singleton_registry.rate_limits
    result_singleton = _served_redis_rate_limits(
        {served_by_custom.name: served_by_custom}, singleton_registry
    )
    assert result_singleton == []

    # ── False positive: singleton has a Redis-backed limit the custom doesn't ──
    singleton_registry.register(
        TokenBucket(
            name="singleton_only_redis_bucket_26",
            capacity=10.0,
            refill_per_second=1.0,
            backend="redis",
        )
    )

    @actor(rate_limits=["singleton_only_redis_bucket_26"])
    async def served_by_singleton(payload: EmptyPayload) -> None:
        pass

    # Scanning the custom registry must NOT see the singleton's limit
    result_custom = _served_redis_rate_limits(
        {served_by_singleton.name: served_by_singleton}, custom
    )
    assert result_custom == []


async def test_bootstrap_rejects_actor_registry_key_name_mismatch() -> None:
    """An actor_registry entry whose key differs from its ActorRef's name
    fails fast at bootstrap, naming the mismatch.

    Regression: a mismapped entry (e.g. ``{"quick_result": <ref named
    "enrich_order">}``) previously surfaced deep in ``sync_actor_config``
    as a raw ``ON CONFLICT DO UPDATE command cannot affect row a second
    time`` CardinalityViolation — the batch UPSERT hits the same actor row
    twice when two refs share a ``.name``. The key-equals-name check also
    makes duplicate names impossible (same name means same key, so the
    dict itself dedupes at construction).
    """

    @actor(name="enrich_order", queue="e2e")
    async def mismapped(payload: EmptyPayload) -> None:
        pass

    with pytest.raises((RuntimeError, ValueError), match=r"quick_result.*enrich_order"):
        await _run_main_with_mocked_deps(
            _settings(),
            actor_registry={"quick_result": mismapped},
        )


# ── caller-supplied registry auto-registers WorkerSettings ────────


async def test_caller_registry_auto_registers_worker_settings() -> None:
    """_main registers WorkerSettings at PROCESS scope even when the caller
    supplied ``_registry`` without it.

    Pins the worker_main docstring contract ("WorkerSettings and Clock are
    registered automatically if not already present") — required by
    providers with a WorkerSettings dep edge (e.g. the Redis rate-limit
    pool factory) when di_registry carries only user providers, as in the
    e2e worker container topology.
    """
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_factory(_ProcessDep, Scope.PROCESS, lambda: _ProcessDep())

    assert registry.has_provider(WorkerSettings) is False

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    assert registry.has_provider(WorkerSettings) is True
    settings_entry = registry.get(WorkerSettings)
    assert settings_entry.scope == Scope.PROCESS
    assert settings_entry.impl is settings


# ── Integration tests ─────────────────────────────────────────────────


# ── integration — worker bootstrap auto-registers SystemClock ─


@pytest.mark.integration
async def test_integration_worker_bootstrap_auto_registers_system_clock(
    pg_dsn: str,
) -> None:
    """after bootstrap, resolved Clock is SystemClock with UTC now()."""

    from taskq.migrate import apply_pending

    schema = f"twdb_{new_base62()}".lower()
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

    settings = _integration_settings(pg_dsn, schema=schema)

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    assert registry.has_provider(Clock) is True
    clock_entry = registry.get(Clock)
    assert isinstance(clock_entry.impl, SystemClock)
    assert clock_entry.scope == Scope.PROCESS

    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    registry.validate()
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    resolved = process_scope.get(Clock)
    assert resolved is not None
    assert isinstance(resolved, SystemClock)
    now_val = resolved.now()
    assert isinstance(now_val, datetime)
    assert now_val.tzinfo is not None
    delta = abs((now_val - datetime.now(UTC)).total_seconds())
    assert delta < 2.0

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── integration — pre-registered FakeClock survives bootstrap ─


@pytest.mark.integration
async def test_integration_pre_registered_fake_clock_survives_bootstrap(
    pg_dsn: str,
) -> None:
    """pre-registered FakeClock at PROCESS scope survives _main bootstrap."""

    from taskq.migrate import apply_pending

    schema = f"twdb_{new_base62()}".lower()
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

    settings = _integration_settings(pg_dsn, schema=schema)
    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    clock_entry = registry.get(Clock)
    assert clock_entry.impl is fake_clock
    assert not isinstance(clock_entry.impl, SystemClock)

    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    registry.validate()
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    resolved = process_scope.get(Clock)
    assert resolved is fake_clock
    assert not isinstance(resolved, SystemClock)

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── di_consumer_loop uses ProcessScope-cached Clock ────


async def test_di_consumer_loop_uses_process_scope_clock() -> None:
    """di_consumer_loop resolves Clock from ProcessScope, not SystemClock()."""
    from taskq.worker.run import di_consumer_loop

    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    captured_clock: Clock | None = None
    dispatch_event = asyncio.Event()

    async def _fake_dispatch(*args: object, **kwargs: object) -> None:
        nonlocal captured_clock
        captured_clock = kwargs.get("clock")  # type: ignore[assignment] # Why: kwargs.get() returns object | None; captured_clock is Clock | None — the assertion below verifies the runtime type.
        dispatch_event.set()

    shutdown_event = asyncio.Event()

    @actor(name="test_actor_scope_override_11")
    async def _test_actor(payload: BaseModel, ctx: JobContext[BaseModel]) -> None: ...

    job = JobRow(
        id=new_job_id(),
        actor=_test_actor.name,
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=CancelPhase.NONE,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    await local_queue.put(job)

    deps = _stub_deps(settings)
    backend = _backend_methods_stub()

    with patch("taskq.worker.run.dispatch_one_job", side_effect=_fake_dispatch):
        loop_task = asyncio.create_task(
            di_consumer_loop(
                deps,
                local_queue,
                shutdown_event,
                backend=backend,
                worker_id=new_uuid(),
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_registry={_test_actor.name: _test_actor},
                enqueuer=SubJobEnqueuer(
                    loop_scope_resolved=None,
                    worker_pool=None,
                    backend=backend,
                ),
            )
        )
        await asyncio.wait_for(dispatch_event.wait(), timeout=2.0)
        shutdown_event.set()
        await asyncio.wait_for(loop_task, timeout=2.0)

    assert captured_clock is fake_clock

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── MissingProvider when ProcessScope has no Clock ──────


async def test_di_consumer_loop_raises_missing_provider_no_clock() -> None:
    """di_consumer_loop raises MissingProvider when ProcessScope has no cached Clock."""
    from taskq.worker.run import di_consumer_loop

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    deps = _stub_deps(settings)
    backend = _backend_methods_stub()

    with pytest.raises(MissingProvider, match="Clock"):
        await di_consumer_loop(
            deps,
            local_queue,
            shutdown_event,
            backend=backend,
            worker_id=new_uuid(),
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_registry={},
            enqueuer=SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            ),
        )

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Unknown actor releases the claimed job instead of stranding it ──


async def test_di_consumer_loop_releases_job_for_unknown_actor() -> None:
    """A dispatched job whose actor is absent from actor_registry is
    released via mark_snoozed (short delay) rather than left 'running'
    until lock-lease expiry."""
    from taskq.worker.run import di_consumer_loop

    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    backend = _backend_methods_stub()
    released = asyncio.Event()
    snooze_calls: list[tuple[object, object, object, dict[str, object] | None]] = []

    async def _spy_mark_snoozed(
        job_id: object,
        worker_id: object,
        delay: object,
        *,
        metadata_update: dict[str, object] | None = None,
        **_kwargs: object,
    ) -> str:
        snooze_calls.append((job_id, worker_id, delay, metadata_update))
        released.set()
        return "scheduled"

    backend.mark_snoozed = _spy_mark_snoozed  # type: ignore[attr-defined] # Why: stub backend is a plain object; spy attribute injection is the established pattern in this file.

    job = JobRow(
        id=new_job_id(),
        actor="actor-not-in-registry",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=CancelPhase.NONE,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    await local_queue.put(job)

    deps = _stub_deps(settings)
    shutdown_event = asyncio.Event()

    loop_task = asyncio.create_task(
        di_consumer_loop(
            deps,
            local_queue,
            shutdown_event,
            backend=backend,
            worker_id=new_uuid(),
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_registry={},
            enqueuer=SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            ),
        )
    )
    await asyncio.wait_for(released.wait(), timeout=2.0)
    shutdown_event.set()
    await asyncio.wait_for(loop_task, timeout=2.0)

    assert len(snooze_calls) == 1
    released_job_id, _wid, delay, metadata_update = snooze_calls[0]
    assert released_job_id == job.id
    assert delay == timedelta(seconds=10)
    assert metadata_update == {"released_reason": "actor-not-found"}

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Watchdog wiring through the real _main bootstrap path ────────


async def test_bootstrap_with_watchdog_disabled_does_not_spawn_or_fail() -> None:
    """TASKQ_WATCHDOG_ENABLED=false must boot cleanly: the stale-tick loop
    is simply not spawned. An early-return loop with no shutdown in
    progress would trip detector 3 — the master kill-switch is the one
    path that must never fail."""
    result = await _run_main_with_mocked_deps(_settings(TASKQ_WATCHDOG_ENABLED="false"))
    assert result == 0
