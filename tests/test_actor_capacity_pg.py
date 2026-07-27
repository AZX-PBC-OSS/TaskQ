"""Integration tests (real Postgres) for operator-owned ``max_pending``.

The unit tier (test_actor_capacity_cache.py) pins resolution and cache
mechanics against InMemoryBackend; this tier proves the real wiring:
``sync_actor_config`` seeds the row, ``set_actor_config_capacity`` is the
operator's out-of-band write (exactly what ``taskq actor-config set``
issues), and a ``JobsClient`` over a real ``PostgresBackend`` enforces
the stored value — with no worker restart and no client recreation.

Mirrors test_dispatch_pg.py::test_live_capacity_change_takes_effect_without_restart,
which proves the same property for ``max_concurrent`` on the dispatch side.
"""

import asyncio

import pytest
from pydantic import BaseModel

from taskq.actor import actor
from taskq.client import JobsClient
from taskq.exceptions import MaxPendingExceededError
from taskq.testing.fixtures import JobsApp
from taskq.worker.actor_config import ActorConfig
from taskq.worker.actor_config_ops import set_actor_config_capacity
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


class _Payload(BaseModel):
    value: int


@actor(name="live_mp_capped", max_pending=100)
async def _live_mp_capped(payload: _Payload) -> None: ...


@actor(name="live_mp_uncapped")
async def _live_mp_uncapped(payload: _Payload) -> None: ...


@actor(name="live_mp_revert", max_pending=2)
async def _live_mp_revert(payload: _Payload) -> None: ...


def _configs(*configs: ActorConfig) -> list[ActorConfig]:
    return list(configs)


async def _sync(jobs_app: JobsApp, configs: list[ActorConfig]) -> None:
    async with jobs_app.deps.dispatcher_pool.acquire() as conn:
        await sync_actor_config(
            conn,  # type: ignore[arg-type] # Why: PoolConnectionProxy is a transparent proxy delegating to the real Connection; asyncpg's public API accepts it interchangeably
            configs,
            force=False,
            schema=jobs_app.deps.settings.schema_name,
        )


async def _set_max_pending(jobs_app: JobsApp, actor_name: str, value: int | None) -> None:
    async with jobs_app.deps.dispatcher_pool.acquire() as conn:
        await set_actor_config_capacity(
            conn,  # type: ignore[arg-type] # Why: see _sync
            actor_name,
            max_pending=value,
            schema=jobs_app.deps.settings.schema_name,
        )


async def test_live_max_pending_change_takes_effect_without_restart(
    jobs_app: JobsApp,
) -> None:
    """Stored max_pending=2 beats the literal 100 on the very next enqueue
    after the operator's out-of-band UPDATE — no sync re-run, no restart,
    no client recreation (ttl=0 disables snapshot reuse)."""
    await _sync(
        jobs_app,
        _configs(
            ActorConfig(
                actor="live_mp_capped", max_concurrent=None, max_pending=100, queue="default"
            )
        ),
    )
    client = JobsClient(jobs_app.backend, capacity_cache_ttl=0.0)

    await client.enqueue(_live_mp_capped, _Payload(value=1))

    await _set_max_pending(jobs_app, "live_mp_capped", 2)

    await client.enqueue(_live_mp_capped, _Payload(value=2))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(_live_mp_capped, _Payload(value=3))


async def test_live_max_pending_change_visible_after_invalidation(
    jobs_app: JobsApp,
) -> None:
    """Default-TTL client: the operator's change is picked up after
    explicit invalidation — the mechanism standing in for TTL expiry."""
    await _sync(
        jobs_app,
        _configs(
            ActorConfig(
                actor="live_mp_capped", max_concurrent=None, max_pending=100, queue="default"
            )
        ),
    )
    client = JobsClient(jobs_app.backend)

    await client.enqueue(_live_mp_capped, _Payload(value=1))
    await _set_max_pending(jobs_app, "live_mp_capped", 2)
    client.invalidate_actor_capacity_cache()

    await client.enqueue(_live_mp_capped, _Payload(value=2))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(_live_mp_capped, _Payload(value=3))


async def test_operator_can_cap_actor_with_no_literal_pg(
    jobs_app: JobsApp,
) -> None:
    """An actor declared without max_pending is capped live: the stored
    row starts NULL (seeded from the None literal), the operator sets 1,
    and the next enqueue beyond the cap raises."""
    await _sync(
        jobs_app,
        _configs(ActorConfig(actor="live_mp_uncapped", max_concurrent=None, queue="default")),
    )
    client = JobsClient(jobs_app.backend, capacity_cache_ttl=0.0)

    # NULL stored value → no limit, matching the missing literal.
    await client.enqueue(_live_mp_uncapped, _Payload(value=1))
    await client.enqueue(_live_mp_uncapped, _Payload(value=2))

    await _set_max_pending(jobs_app, "live_mp_uncapped", 3)

    await client.enqueue(_live_mp_uncapped, _Payload(value=3))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(_live_mp_uncapped, _Payload(value=4))


async def test_cleared_override_reverts_to_literal_pg(
    jobs_app: JobsApp,
) -> None:
    """`set --clear-max-pending` (stored NULL) reverts enforcement to the
    @actor literal — it does NOT make the actor unlimited."""
    await _sync(
        jobs_app,
        _configs(
            ActorConfig(actor="live_mp_revert", max_concurrent=None, max_pending=2, queue="default")
        ),
    )
    client = JobsClient(jobs_app.backend, capacity_cache_ttl=0.0)

    # Operator raises the cap to 5: four enqueues fit.
    await _set_max_pending(jobs_app, "live_mp_revert", 5)
    for i in range(4):
        await client.enqueue(_live_mp_revert, _Payload(value=i))

    # Operator clears the override: literal 2 is authoritative again and
    # the queue (4 deep) is over it.
    await _set_max_pending(jobs_app, "live_mp_revert", None)
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(_live_mp_revert, _Payload(value=99))


async def test_two_clients_enforce_the_same_stored_limit_pg(
    jobs_app: JobsApp,
) -> None:
    """Multi-process agreement: two independent clients (two 'processes')
    read the same actor_config row — a job enqueued through one counts
    against the other's enforcement."""
    await _sync(
        jobs_app,
        _configs(
            ActorConfig(actor="live_mp_capped", max_concurrent=None, max_pending=2, queue="default")
        ),
    )
    client_a = JobsClient(jobs_app.backend, capacity_cache_ttl=0.0)
    client_b = JobsClient(jobs_app.backend, capacity_cache_ttl=0.0)

    await client_a.enqueue(_live_mp_capped, _Payload(value=1))
    await client_b.enqueue(_live_mp_capped, _Payload(value=2))
    with pytest.raises(MaxPendingExceededError):
        await client_b.enqueue(_live_mp_capped, _Payload(value=3))


async def test_concurrent_set_writers_on_disjoint_fields_both_land(
    jobs_app: JobsApp,
) -> None:
    """CASE-WHEN conditional update audit: two concurrent UPDATEs of the
    same row touching DISJOINT fields must not lose either write. Postgres
    serializes them on the row lock and the second CASE re-reads the
    first's committed values, so both changes compose."""
    await _sync(
        jobs_app,
        _configs(
            ActorConfig(
                actor="live_mp_capped",
                max_concurrent=1,
                max_pending=1,
                result_ttl=1.0,
                queue="default",
            )
        ),
    )
    schema = jobs_app.deps.settings.schema_name

    async def _set(field: str, value: object) -> None:
        async with jobs_app.deps.dispatcher_pool.acquire() as conn:
            kwargs: dict[str, object] = {field: value}
            await set_actor_config_capacity(
                conn,  # type: ignore[arg-type] # Why: see _sync
                "live_mp_capped",
                schema=schema,
                **kwargs,  # type: ignore[arg-type] # Why: kwargs narrowed by the caller pairs below
            )

    await asyncio.gather(
        _set("max_concurrent", 10),
        _set("max_pending", 20),
        _set("result_ttl", 30.0),
    )

    async with jobs_app.deps.dispatcher_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT max_concurrent, max_pending, result_ttl FROM "{schema}".actor_config '
            "WHERE actor = $1",
            "live_mp_capped",
        )
    assert row is not None
    assert (row["max_concurrent"], row["max_pending"], row["result_ttl"]) == (10, 20, 30.0), (
        f"concurrent disjoint-field updates lost a write: {dict(row)}"
    )
