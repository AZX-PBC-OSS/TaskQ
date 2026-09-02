"""``unique_for`` + ``identity_key`` must be single-flight, not best-effort.

The column comment ("Used for serialization and unique-for") and the
client docs both promise deduplication. The implementation was a preflight
SELECT followed by an unguarded INSERT: two dispatchers enqueuing the same
(actor, identity_key) concurrently both saw an empty preflight and both
inserted, so two jobs with the same identity ran at once -- the exact
thing the feature exists to prevent.

Real concurrency against real Postgres. A sequential simulation would pass
before and after the fix and prove nothing.
"""

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

from taskq._ids import new_base62
from taskq.actor import actor
from taskq.client import JobHandle, JobsClient

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object

pytestmark = pytest.mark.integration

_RACERS = 100


class _Payload(BaseModel):
    value: int = 1


@actor(
    name="_single_flight_actor",
    unique_for=timedelta(minutes=15),
    unique_states=("pending", "scheduled", "running"),
)
async def _single_flight_actor(payload: _Payload) -> None:
    pass


@actor(name="_single_flight_short_window_actor", unique_for=timedelta(milliseconds=1))
async def _single_flight_short_window_actor(payload: _Payload) -> None:
    pass


@actor(name="_no_unique_for_actor")
async def _no_unique_for_actor(payload: _Payload) -> None:
    pass


async def _warm_pool(deps: WorkerDeps) -> None:
    """Open every pooled connection before racing.

    asyncpg pools start at min_size=1 and open the rest lazily. A cold pool
    therefore serialises the first callers behind connection setup: the
    first enqueue commits and releases its connection before the second
    even has one, so the second's preflight sees the committed row and the
    race never happens. Production pools are warm. Warming here is what
    makes the concurrency real rather than nominal -- and is why this bug
    survived a 100-way concurrent test that only ever saw a cold pool.
    """
    pool = deps.worker_pool
    held = [await pool.acquire() for _ in range(pool.get_max_size())]
    for conn in held:
        await pool.release(conn)


async def _count(deps: WorkerDeps, actor_name: str, identity: str) -> int:
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        count: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND identity_key = $2',  # noqa: S608  # Why: schema is fixture-derived and validated; values are $1/$2-bound.
            actor_name,
            identity,
        )
    return count


async def test_concurrent_enqueues_create_exactly_one_job(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """The single-flight guarantee, under a genuine race."""
    deps, backend = clean_jobs_app
    await _warm_pool(deps)
    client = JobsClient(backend)
    identity = f"single-flight-{new_base62()}"

    async def _enqueue() -> JobHandle[None]:
        return await client.enqueue(_single_flight_actor, _Payload(value=1), identity_key=identity)

    results = await asyncio.gather(*[_enqueue() for _ in range(_RACERS)])

    count = await _count(deps, "_single_flight_actor", identity)
    assert count == 1, (
        f"{_RACERS} concurrent enqueues of the same (actor, identity_key) "
        f"created {count} jobs; unique_for promises one"
    )
    assert len({r.job_id for r in results}) == 1, (
        "every caller must be handed the one surviving job"
    )


async def test_the_window_still_expires(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Serializing the check must not turn unique_for into a permanent lock.

    ``unique_for`` is a WINDOW: once it elapses, a new job is created even
    though the previous one is still active. This is why the guarantee
    cannot be a partial unique index on (actor, identity_key) -- an index
    predicate must be IMMUTABLE and so cannot reference clock_timestamp().
    """
    deps, backend = clean_jobs_app
    client = JobsClient(backend)
    identity = f"expiring-{new_base62()}"

    first = await client.enqueue(
        _single_flight_short_window_actor, _Payload(), identity_key=identity
    )
    await asyncio.sleep(0.05)
    second = await client.enqueue(
        _single_flight_short_window_actor, _Payload(), identity_key=identity
    )

    assert first.job_id != second.job_id, "an elapsed window must admit a new job"
    assert await _count(deps, "_single_flight_short_window_actor", identity) == 2


async def test_identity_key_without_unique_for_is_not_deduplicated(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """identity_key alone is a serialization/fairness cohort, not a lock.

    Actors with no ``unique_for`` legitimately keep many active jobs under
    one identity_key (that is what jobs_identity_active_idx indexes, and it
    is deliberately NOT unique). The second reason the guarantee cannot be
    a unique index: it would reject every one of these.
    """
    deps, backend = clean_jobs_app
    await _warm_pool(deps)
    client = JobsClient(backend)
    identity = f"cohort-{new_base62()}"

    handles = await asyncio.gather(
        *[
            client.enqueue(_no_unique_for_actor, _Payload(value=i), identity_key=identity)
            for i in range(4)
        ]
    )

    assert len({h.job_id for h in handles}) == 4
    assert await _count(deps, "_no_unique_for_actor", identity) == 4


async def test_distinct_identities_are_not_serialized_into_one_job(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Different identity keys under the same actor stay independent."""
    deps, backend = clean_jobs_app
    await _warm_pool(deps)
    client = JobsClient(backend)
    prefix = new_base62()

    handles = await asyncio.gather(
        *[
            client.enqueue(_single_flight_actor, _Payload(value=i), identity_key=f"{prefix}-{i}")
            for i in range(8)
        ]
    )

    assert len({h.job_id for h in handles}) == 8
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        total: int = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: as above.
            "_single_flight_actor",
        )
    assert total == 8
