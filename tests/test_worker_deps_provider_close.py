"""The worker's exit stack releases the credential providers its factories declare.

``open_worker_deps`` closes every pool and client built through a
credential-provider factory; the provider itself - the Entra ID providers
hold a lazily created credential session - stayed open after shutdown. These
tests pin the wiring: closed ONCE (one provider may serve both the PG and
the Redis role), AFTER every resource built through it, never for a factory
that declares no provider, and for the per-slot pool's provider only when
the role factories do not already track it.

Docker-free: hand-rolled fakes wired through the REAL ``open_worker_deps``
and ``_maybe_open_slot_pool``, mirroring ``tests/test_worker_deps_teardown.py``'s
fake conventions.

No ``pytestmark`` - must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any, Self

import asyncpg

from taskq.auth import (  # pyright: ignore[reportPrivateUsage]  # Why: the declared attribute is internal by design; the pin needs the same stamping path the make_*_factory helpers use.
    PgCredential,
    _declare_provider,
)
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import _maybe_open_slot_pool
from taskq.worker.deps import open_worker_deps


def _make_settings(max_concurrency: int = 8) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_PG_DSN_DIRECT": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_PG_DSN_POOLED": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_NOTIFY_ENABLED": "false",
        }
    )


class _FakeProvider:
    """Duck-typed credential provider recording aclose() into an event list."""

    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.aclose_calls = 0
        self._events = events

    async def get_pg_credential(self) -> PgCredential:
        return PgCredential(password="fake")

    async def get_redis_credential(self) -> Any:  # pragma: no cover - unused here
        raise AssertionError("this test never fetches a Redis credential")

    async def aclose(self) -> None:
        self.aclose_calls += 1
        self._events.append(f"provider:{self.name}")


class _FakePool:
    """Fake asyncpg.Pool recording close() into an event list."""

    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self._events = events

    async def close(self) -> None:
        if self.closed or self.terminated:
            return
        self.closed = True
        self._events.append(f"pool:{self.name}")

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


class _FakeConn:
    """Fake asyncpg.Connection recording close() into an event list."""

    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.closed = False
        self._events = events
        self.executed: list[str] = []

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._events.append(f"conn:{self.name}")

    def terminate(self) -> None:
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed


class _FakeRedisClient:
    """Fake Redis client recording aclose() into an event list."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def aclose(self) -> None:
        self._events.append("redis")


async def test_provider_closes_once_after_every_resource_built_through_it() -> None:
    """One provider behind the pg AND the redis role closes exactly once, and
    only after the pool and the Redis client that authenticate through it."""
    events: list[str] = []
    provider = _FakeProvider("shared", events)
    dispatcher_pool = _FakePool("dispatcher", events)
    heartbeat_pool = _FakePool("heartbeat", events)
    worker_pool = _FakePool("worker", events)

    async def dispatcher_factory() -> asyncpg.Pool:
        return dispatcher_pool  # type: ignore[return-value]

    async def redis_factory() -> Any:
        return _FakeRedisClient(events)

    _declare_provider(dispatcher_factory, provider)  # pyright: ignore[reportArgumentType]
    _declare_provider(redis_factory, provider)  # pyright: ignore[reportArgumentType]

    conns = WorkerConnections(
        heartbeat_pool=heartbeat_pool,  # type: ignore[arg-type]
        worker_pool=worker_pool,  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify", events),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader", events),  # type: ignore[arg-type]
        dispatcher_pool_factory=dispatcher_factory,  # type: ignore[arg-type]
        redis_client_factory=redis_factory,  # type: ignore[arg-type]
    )

    async with open_worker_deps(_make_settings(), connections=conns) as deps:
        assert deps._credential_providers == (provider,)

    assert provider.aclose_calls == 1, (
        "one provider serving both roles must be closed exactly once, not once per factory"
    )
    # Caller-owned pools/conns are left alone by teardown (ownership rule);
    # the TaskQ-owned dispatcher pool and Redis client, both built through
    # the shared provider, close BEFORE it.
    assert events.index("provider:shared") > events.index("pool:dispatcher")
    assert events.index("provider:shared") > events.index("redis"), (
        "the provider must close after the Redis client built through it"
    )


async def test_a_factory_that_declares_no_provider_is_left_alone() -> None:
    """A factory built some other way declares no provider: nothing is
    closed behind the caller's back and teardown completes unchanged."""
    events: list[str] = []
    pool = _FakePool("dispatcher", events)

    async def dispatcher_factory() -> asyncpg.Pool:
        return pool  # type: ignore[return-value]

    conns = WorkerConnections(
        heartbeat_pool=_FakePool("heartbeat", events),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker", events),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify", events),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader", events),  # type: ignore[arg-type]
        dispatcher_pool_factory=dispatcher_factory,  # type: ignore[arg-type]
    )

    async with open_worker_deps(_make_settings(), connections=conns) as deps:
        assert deps._credential_providers == ()

    assert not any(event.startswith("provider:") for event in events)
    assert pool.closed


async def test_slot_pool_provider_closes_after_the_slot_pool() -> None:
    """A provider reachable ONLY through the per-slot pool closes after the
    slot pool, on the same incremental stack that owns the pool's teardown."""
    events: list[str] = []
    provider = _FakeProvider("slot", events)
    slot_pool = _FakePool("slot", events)

    async def slot_factory() -> asyncpg.Pool:
        return slot_pool  # type: ignore[return-value]

    _declare_provider(slot_factory, provider)  # pyright: ignore[reportArgumentType]

    # A registered connection that cannot answer the session-state queries:
    # _registered_session_state degrades to {} and the factory is used as
    # passed - the fake above, whose declared provider is what this test pins.
    async def unreadable(_query: str) -> object:
        raise RuntimeError("unreadable session")

    stack = AsyncExitStack()
    await stack.__aenter__()
    deps = SimpleNamespace(
        slot_pool=None,
        slot_pool_factory=None,
        _exit_stack=stack,
        _credential_providers=(),
    )
    loop_scope = SimpleNamespace(
        resolved_cache=lambda: {asyncpg.Connection: SimpleNamespace(fetchval=unreadable)}
    )

    try:
        opened = await _maybe_open_slot_pool(
            loop_scope,
            _make_settings(max_concurrency=4),
            deps,
            factory=slot_factory,
            pg_credential_provider=provider,  # type: ignore[arg-type]
            caller_supplied_pg_pools=False,
            log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None),
        )
        assert opened, "the per-slot path must activate at max_concurrency > 1"
        assert provider.aclose_calls == 0, "the provider closes at unwind, not at open"
        assert slot_pool.closed is False, "the slot pool is still open"
    finally:
        await stack.aclose()

    assert provider.aclose_calls == 1
    assert events.index("provider:slot") > events.index("pool:slot"), (
        "the credential that built the slot pool must outlive the pool's close"
    )


async def test_slot_pool_provider_already_tracked_by_the_role_factories_is_not_closed_twice() -> (
    None
):
    """The documented worker path builds every role through ONE provider:
    open_worker_deps already closes it after the role pools, so the slot
    pool's registration must not add a second close."""
    events: list[str] = []
    provider = _FakeProvider("shared", events)
    slot_pool = _FakePool("slot", events)

    async def slot_factory() -> asyncpg.Pool:
        return slot_pool  # type: ignore[return-value]

    _declare_provider(slot_factory, provider)  # pyright: ignore[reportArgumentType]

    async def unreadable(_query: str) -> object:
        raise RuntimeError("unreadable session")

    stack = AsyncExitStack()
    await stack.__aenter__()
    deps = SimpleNamespace(
        slot_pool=None,
        slot_pool_factory=None,
        _exit_stack=stack,
        # What open_worker_deps recorded: the role factories declare the
        # same provider object.
        _credential_providers=(provider,),
    )
    loop_scope = SimpleNamespace(
        resolved_cache=lambda: {asyncpg.Connection: SimpleNamespace(fetchval=unreadable)}
    )

    try:
        opened = await _maybe_open_slot_pool(
            loop_scope,
            _make_settings(max_concurrency=4),
            deps,
            factory=slot_factory,
            pg_credential_provider=provider,  # type: ignore[arg-type]
            caller_supplied_pg_pools=False,
            log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None),
        )
        assert opened
    finally:
        await stack.aclose()

    assert provider.aclose_calls == 0, (
        "the provider is tracked on the base stack (closed after the role "
        "pools); a second registration here would close it BEFORE those "
        "pools, while connections that authenticate through it still open"
    )
    assert slot_pool.closed, "the slot pool's own teardown guard still runs"


def test_no_event_loop_dependencies_in_the_provider_close_path() -> None:
    """close_provider_bounded is pushed as a callback, not awaited at open:
    a worker whose providers never close (nothing declares aclose) boots
    exactly as before. Control test - the collection is a no-op here."""
    events: list[str] = []
    conns = WorkerConnections(
        heartbeat_pool=_FakePool("heartbeat", events),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker", events),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify", events),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader", events),  # type: ignore[arg-type]
        dispatcher_pool=_FakePool("dispatcher", events),  # type: ignore[arg-type]
    )

    async def scenario() -> None:
        async with open_worker_deps(_make_settings(), connections=conns) as deps:
            assert deps._credential_providers == ()

    asyncio.run(scenario())
