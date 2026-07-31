"""Integration tests for injectable RateLimitRegistry at worker bootstrap.

Covers: actor-declared primitive instances collected+registered by _main and
acquirable at dispatch; DI value provider wins over the singleton end-to-end;
backwards-compat regression (import-time .register() on the singleton + no
rate_limit_registry argument → singleton resolved, worker starts).
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Coroutine
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._di import ProviderRegistry, Scope
from taskq._ids import new_base62, new_uuid
from taskq.actor import actor
from taskq.backend.clock import SystemClock
from taskq.migrate import apply_pending
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as singleton
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.conftest import unique_health_sock_path

pytestmark = pytest.mark.integration


class _Payload(BaseModel):
    x: int = 1


async def _prepare_schema(pg_dsn: str, schema: str) -> None:
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


async def _cleanup_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "health_socket_path": unique_health_sock_path("rl_injection"),
        }
    )


async def _run_and_cancel(
    coro_factory: Callable[[], Coroutine[Any, Any, int]], *, sleep: float = 1.5
) -> None:
    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await coro_factory()

    task = asyncio.create_task(_runner())
    await asyncio.sleep(sleep)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    else:
        await task  # propagate bootstrap failures


@pytest.fixture()
async def schema(pg_dsn: str) -> AsyncGenerator[str]:
    label = f"twri_{new_base62()}".lower()
    await _prepare_schema(pg_dsn, label)
    yield label
    await _cleanup_schema(pg_dsn, label)


async def test_actor_declared_instances_collected_and_acquirable(pg_dsn: str, schema: str) -> None:
    """@actor(rate_limits=[TokenBucket(...)]) is registered by the _main
    collection pass (validate would fail otherwise) and acquirable post-boot."""
    bucket = TokenBucket(name="coll_bucket", capacity=5, refill_per_second=1.0, backend="memory")

    @actor(name="rl_inst_actor", queue="default", rate_limits=[bucket])
    async def rl_inst_actor(payload: _Payload) -> None:
        pass

    own = RateLimitRegistry()
    settings = _settings(pg_dsn, schema)

    await _run_and_cancel(
        lambda: _main(
            settings, actor_registry={"rl_inst_actor": rl_inst_actor}, rate_limit_registry=own
        )
    )

    # Collection pass registered the instance into the OWNED registry...
    assert own.rate_limits["coll_bucket"] is bucket
    # ...and the worker never touched the singleton.
    assert "coll_bucket" not in singleton.rate_limits
    # ...and the declaration resolves at acquisition time (dispatch path).
    acquired = await own.acquire_for_actor(
        rate_limits=rl_inst_actor.rate_limits,
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        clock=SystemClock(),
    )
    assert len(acquired) == 1


async def test_di_value_provider_wins_over_singleton_end_to_end(pg_dsn: str, schema: str) -> None:
    """A name-ref actor validates against the DI-provided registry, not the
    singleton (which has no ``di_bucket`` registration) — proving rule 2."""
    own = RateLimitRegistry()
    own.register(TokenBucket(name="di_bucket", capacity=5, refill_per_second=1.0, backend="memory"))

    @actor(name="rl_di_actor", queue="default", rate_limits=["di_bucket"])
    async def rl_di_actor(payload: _Payload) -> None:
        pass

    di = ProviderRegistry()
    di.register_value(RateLimitRegistry, Scope.LOOP, own)
    settings = _settings(pg_dsn, schema)

    # Would raise MissingProvider at validate() if the singleton were used.
    await _run_and_cancel(
        lambda: _main(settings, actor_registry={"rl_di_actor": rl_di_actor}, _registry=di)
    )


async def test_singleton_default_backwards_compatible(pg_dsn: str, schema: str) -> None:
    """Regression: import-time .register() on the singleton + _main WITHOUT
    rate_limit_registry → singleton resolved, worker starts cleanly."""
    singleton.register(
        TokenBucket(name="bc_bucket", capacity=5, refill_per_second=1.0, backend="memory")
    )

    @actor(name="rl_bc_actor", queue="default", rate_limits=["bc_bucket"])
    async def rl_bc_actor(payload: _Payload) -> None:
        pass

    settings = _settings(pg_dsn, schema)

    await _run_and_cancel(lambda: _main(settings, actor_registry={"rl_bc_actor": rl_bc_actor}))
