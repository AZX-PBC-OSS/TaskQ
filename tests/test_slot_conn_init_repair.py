"""Unit tests for the slot-connection init-hook repair path.

Covers the injected-pool shape: a slot pool the worker did not build
leaves ``deps.slot_pool_connection_init`` unset, so dispatch's repair
path (:func:`taskq.worker.dispatch._ensure_registered_init_on_slot_conn`)
applies the registration's declared init hook to each acquired
connection exactly once per physical connection. asyncpg 0.31.0's
``PoolConnectionProxy`` is ``__slots__``-sealed (``('_con', '_holder')``),
so the exactly-once record must live off the connection object, a
setattr marker on the proxy raises ``AttributeError`` and kills every
dispatch on an injected pool.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
import pytest

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_uuid
from taskq.connections import with_connection_init
from taskq.worker import dispatch as dispatch_mod
from taskq.worker.dispatch import _ensure_registered_init_on_slot_conn


class _PhysicalConn:
    """Stand-in for the physical asyncpg.Connection behind a pool proxy.

    Only what the repair path touches: the hook contract (any callable is
    accepted) and ``terminate()`` on the failure path.
    """

    def __init__(self) -> None:
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True


class _PoolConnProxy:
    """Mirrors asyncpg 0.31.0's PoolConnectionProxy: __slots__-sealed."""

    __slots__ = ("_con", "_holder")

    def __init__(self, con: _PhysicalConn) -> None:
        self._con = con
        self._holder = None

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._con, attr)


class _Deps:
    def __init__(self) -> None:
        self.slot_pool_connection_init = None


def _registry_with_hook(
    applied_to: list[object],
) -> tuple[ProviderRegistry, Callable[[Any], Awaitable[None]]]:
    async def hook(conn: Any) -> None:
        applied_to.append(conn)

    registry = ProviderRegistry()
    registry.register_factory(
        asyncpg.Connection, Scope.LOOP, with_connection_init(lambda: None, hook)
    )
    return registry, hook


async def test_injected_pool_proxy_carries_the_hook_without_setattr() -> None:
    """The repair path applies the declared hook to a slot connection from
    an injected pool. The proxy is __slots__-sealed like asyncpg 0.31.0's
    PoolConnectionProxy, so the exactly-once marker cannot be a setattr on
    it: a marker there raises AttributeError and kills the dispatch."""
    applied_to: list[object] = []
    registry, _hook = _registry_with_hook(applied_to)
    physical = _PhysicalConn()
    proxy = _PoolConnProxy(physical)
    deps = _Deps()

    await _ensure_registered_init_on_slot_conn(
        proxy,  # type: ignore[arg-type]  # Why: stand-in for the ConnLike runtime alias
        deps=deps,  # type: ignore[arg-type]
        registry=registry,
        acquire_timeout=5.0,
        job_id=new_uuid(),
    )

    # The hook's contract is asyncpg's own init=: it receives the acquired
    # slot connection (the proxy forwards attribute access to the physical
    # connection behind it).
    assert applied_to == [proxy]


async def test_repair_path_applies_the_hook_once_per_physical_connection() -> None:
    """Reacquiring the same physical connection through a new proxy must
    not re-run the hook; a different physical connection must get it."""
    applied_to: list[object] = []
    registry, _hook = _registry_with_hook(applied_to)
    deps = _Deps()
    job_id = new_uuid()

    physical_a = _PhysicalConn()
    physical_b = _PhysicalConn()
    proxy_a1 = _PoolConnProxy(physical_a)
    proxy_b1 = _PoolConnProxy(physical_b)
    for proxy in (proxy_a1, _PoolConnProxy(physical_a), proxy_b1):
        await _ensure_registered_init_on_slot_conn(
            proxy,  # type: ignore[arg-type]
            deps=deps,  # type: ignore[arg-type]
            registry=registry,
            acquire_timeout=5.0,
            job_id=job_id,
        )

    # Once per PHYSICAL connection: two proxies wrapping physical_a ran
    # the hook once (the first one), physical_b's first acquire ran once.
    assert applied_to == [proxy_a1, proxy_b1]


async def test_repair_path_terminates_on_hook_failure() -> None:
    """A hook failure is connect-time infrastructure: the connection is
    terminated (never released back half-configured) and
    SlotPoolAcquireError is raised."""
    applied_to: list[object] = []

    async def failing_hook(conn: Any) -> None:
        raise RuntimeError("codec registration exploded")

    registry = ProviderRegistry()
    registry.register_factory(
        asyncpg.Connection, Scope.LOOP, with_connection_init(lambda: None, failing_hook)
    )
    physical = _PhysicalConn()
    proxy = _PoolConnProxy(physical)
    deps = _Deps()

    with pytest.raises(dispatch_mod.SlotPoolAcquireError):
        await _ensure_registered_init_on_slot_conn(
            proxy,  # type: ignore[arg-type]
            deps=deps,  # type: ignore[arg-type]
            registry=registry,
            acquire_timeout=5.0,
            job_id=new_uuid(),
        )

    assert applied_to == []
    assert physical.terminated is True


async def test_repair_path_survives_a_hook_exception_with_a_hostile_str() -> None:
    """The hook is registration-supplied code, so its exception's __str__
    can raise too. The handler converts the failure into
    SlotPoolAcquireError; an unguarded str() in the log or the detail
    f-string would raise a fresh TypeError out of the handler and escape
    the acquire classification entirely."""
    applied_to: list[object] = []

    class _UnprintableStr(Exception):  # noqa: N818  # Why: the name IS the mutation probe, matching the pin file's actor-shaped exception class.
        def __str__(self) -> str:
            raise TypeError("__str__ is a lie")

    async def hostile_hook(conn: Any) -> None:
        applied_to.append(conn)
        raise _UnprintableStr()

    registry = ProviderRegistry()
    registry.register_factory(
        asyncpg.Connection, Scope.LOOP, with_connection_init(lambda: None, hostile_hook)
    )
    physical = _PhysicalConn()
    proxy = _PoolConnProxy(physical)
    deps = _Deps()

    with pytest.raises(dispatch_mod.SlotPoolAcquireError) as exc_info:
        await _ensure_registered_init_on_slot_conn(
            proxy,  # type: ignore[arg-type]
            deps=deps,  # type: ignore[arg-type]
            registry=registry,
            acquire_timeout=5.0,
            job_id=new_uuid(),
        )

    assert physical.terminated is True
    assert "<exception str() failed>" in str(exc_info.value)
