"""`TaskQ` rotates a factory-built pool on its ReloadSchedule.

The client counterpart of the worker's reload coordinator: a pool built
through a credential provider that issues a username-bearing pair is
pinned to that pair, so the client rebuilds it - at ``reload_interval``
when given, otherwise at half the lease TTL the provider granted - as a
background task owned by the client. A token provider's pool has no lease
and is never rebuilt automatically.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import structlog.testing

from taskq import TaskQ
from taskq.auth import PgCredential, make_pg_pool_factory
from taskq.testing.assertions import wait_for


class _LeaseProvider:
    def __init__(self, lease_duration: float | None, *, username: bool = True) -> None:
        self.calls = 0
        self.lease_duration = lease_duration
        self.username = username

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(
            password=f"pw-{self.calls}",
            username=f"v-{self.calls}" if self.username else None,
            lease_duration=self.lease_duration,
        )


class _FakePool:
    def __init__(self) -> None:
        self.closed = asyncio.Event()

    async def close(self) -> None:
        self.closed.set()

    def terminate(self) -> None:
        self.closed.set()


def _pools() -> tuple[list[_FakePool], AsyncMock]:
    built: list[_FakePool] = []

    async def _create_pool(**_: Any) -> _FakePool:
        pool = _FakePool()
        built.append(pool)
        return pool

    return built, AsyncMock(side_effect=_create_pool)


async def test_pg_provider_pool_is_rebuilt_at_half_the_granted_lease() -> None:
    """The pg_provider sugar: with no reload_interval the pool is rebuilt
    at half the lease TTL, the provider is asked once per build, and the
    backend follows the swap."""
    provider = _LeaseProvider(lease_duration=0.2)
    built, create_pool = _pools()
    with patch("asyncpg.create_pool", new=create_pool), structlog.testing.capture_logs() as logs:
        tq = TaskQ(dsn="postgresql://u:p@h/db", pg_provider=provider, schema="taskq")
        await tq.open()
        try:
            assert provider.calls == 1
            await wait_for(built[0].closed, timeout=5.0)
            assert provider.calls >= 2
            assert tq._pool is built[-1]  # pyright: ignore[reportPrivateUsage]  # Why: the swap is the observable; the public surface routes every query through it.
            assert tq._deps is not None and tq._deps.worker_pool is built[-1]  # pyright: ignore[reportPrivateUsage]  # Why: same.
        finally:
            await tq.close()
    armed = [e for e in logs if e["event"] == "client-credential-rotation-armed"]
    assert len(armed) == 1
    assert armed[0]["reload_interval"] == pytest.approx(0.1)
    assert armed[0]["derived_from_lease"] is True
    # close() cancelled the rotation: nothing rebuilds after it.
    calls_at_close = provider.calls
    await asyncio.sleep(0.25)
    assert provider.calls == calls_at_close
    assert built[-1].closed.is_set()


async def test_an_opaque_pool_factory_rotates_on_the_schedule_it_declares() -> None:
    """A factory built with make_pg_pool_factory and handed in as
    pool_factory carries its own schedule; the client adopts it."""
    provider = _LeaseProvider(lease_duration=0.2)
    factory = make_pg_pool_factory("postgresql://u:p@h/db", provider)
    built, create_pool = _pools()
    with patch("asyncpg.create_pool", new=create_pool):
        async with TaskQ(pool_factory=factory, schema="taskq"):
            await wait_for(built[0].closed, timeout=5.0)
            assert provider.calls >= 2


async def test_explicit_reload_interval_overrides_the_lease() -> None:
    provider = _LeaseProvider(lease_duration=3600)
    built, create_pool = _pools()
    with patch("asyncpg.create_pool", new=create_pool), structlog.testing.capture_logs() as logs:
        async with TaskQ(
            dsn="postgresql://u:p@h/db", pg_provider=provider, schema="taskq", reload_interval=0.05
        ):
            await wait_for(built[0].closed, timeout=5.0)
    armed = [e for e in logs if e["event"] == "client-credential-rotation-armed"]
    assert armed[0]["reload_interval"] == 0.05
    assert armed[0]["derived_from_lease"] is False


async def test_a_token_provider_is_never_rebuilt_automatically() -> None:
    """A token credential (no username, no lease) refreshes per physical
    connection; no rotation task is started and the provider is asked only
    at open."""
    provider = _LeaseProvider(lease_duration=None, username=False)
    with patch("asyncpg.create_pool", new=AsyncMock(return_value=MagicMock())):
        async with TaskQ(dsn="postgresql://u:p@h/db", pg_provider=provider, schema="taskq") as tq:
            assert tq._reload_task is None  # pyright: ignore[reportPrivateUsage]  # Why: the absence of the task is the contract.
            await asyncio.sleep(0.05)
            assert provider.calls == 1


async def test_scheduled_and_explicit_reloads_are_serialized() -> None:
    """A scheduled rotation and an explicit reload_credentials() racing
    would each build a pool and one would close the other's fresh one;
    they take turns instead, and every pool but the live one ends closed."""
    provider = _LeaseProvider(lease_duration=0.2)
    built, create_pool = _pools()
    with patch("asyncpg.create_pool", new=create_pool):
        async with TaskQ(dsn="postgresql://u:p@h/db", pg_provider=provider, schema="taskq") as tq:
            await asyncio.gather(tq.reload_credentials(), tq.reload_credentials())
            await wait_for(built[0].closed, timeout=5.0)
            live = cast(object, tq._pool)  # pyright: ignore[reportPrivateUsage]  # Why: the live pool is the observable; cast because _FakePool has no overlap with asyncpg.Pool.
            assert isinstance(live, _FakePool)
            for pool in built:
                if pool is not live:
                    await wait_for(pool.closed, timeout=5.0)
            assert not live.closed.is_set()


def test_reload_interval_requires_a_factory() -> None:
    with pytest.raises(ValueError, match="reload_interval"):
        TaskQ(dsn="postgresql://u:p@h/db", schema="taskq", reload_interval=60)
    with pytest.raises(ValueError, match="reload_interval"):
        TaskQ(pool=object(), schema="taskq", reload_interval=60)  # type: ignore[arg-type]  # Why: any object stands in for a caller-owned pool at construction.


def test_reload_interval_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        TaskQ(
            dsn="postgresql://u:p@h/db",
            pg_provider=_LeaseProvider(None),
            schema="taskq",
            reload_interval=0,
        )
