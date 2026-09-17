"""A cross-actor idempotency hit is refused, never silently resolved.

Uniqueness is ``(idempotency_scope, idempotency_key)`` — schema-wide, not
per actor, and the jobs guide tells callers to namespace keys per actor.
When they do not, ``enqueue(send_receipt, key="order-1")`` after
``enqueue(refund, key="order-1")`` used to return the REFUND's handle with
``was_existing=True`` and an INFO-level dedup log: a handle whose
``.result()`` is another actor's, indistinguishable to the caller from a
successful dedup of its own job. A uniqueness contract that includes the
actor cannot hand back a different actor's job; TaskQ's index cannot
include the actor
without a migration, so the dedup hit is checked instead and a mismatch
raises :class:`IdempotencyKeyActorMismatchError` naming both actors and the
existing job. A same-actor hit still returns the existing row.
"""

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs, JobFilter
from taskq.exceptions import IdempotencyKeyActorMismatchError

pytestmark = pytest.mark.integration


def _args(actor: str, key: str) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=None,
        idempotency_key=key,
    )


async def test_same_actor_hit_returns_the_existing_row(backend_pair: Backend) -> None:
    key = f"k-{new_uuid()}"
    first = await backend_pair.enqueue(_args("actor_a", key))
    again = await backend_pair.enqueue(_args("actor_a", key))
    assert again.id == first.id


async def test_cross_actor_hit_raises_naming_both_actors(backend_pair: Backend) -> None:
    key = f"k-{new_uuid()}"
    existing = await backend_pair.enqueue(_args("actor_a", key))

    with pytest.raises(IdempotencyKeyActorMismatchError) as excinfo:
        await backend_pair.enqueue(_args("actor_b", key))

    err = excinfo.value
    assert err.actor == "actor_b"
    assert err.existing_actor == "actor_a"
    assert err.existing_job_id == existing.id
    assert err.idempotency_key == key
    assert "actor_a" in str(err) and "actor_b" in str(err) and key in str(err)


async def test_cross_actor_hit_in_a_batch_refuses_the_whole_batch(
    backend_pair: Backend,
) -> None:
    """The batch tier admits nothing on a mismatch: the refusal is
    all-or-nothing like the singleton collision, so a caller can fix the
    key and resubmit the batch without duplicating its other items."""
    key = f"k-{new_uuid()}"
    await backend_pair.enqueue(_args("actor_a", key))
    fresh = _args("actor_b", f"{key}-fresh")

    with pytest.raises(IdempotencyKeyActorMismatchError):
        await backend_pair.enqueue_batch([fresh, _args("actor_b", key)])

    rows = await backend_pair.list_jobs(JobFilter(actor="actor_b", limit=100))
    assert all(r.id != fresh.id for r in rows), "the refused batch must leave no rows behind"


async def test_same_actor_hit_in_a_batch_still_dedupes(backend_pair: Backend) -> None:
    key = f"k-{new_uuid()}"
    first = await backend_pair.enqueue(_args("actor_a", key))
    rows = await backend_pair.enqueue_batch([_args("actor_a", key)])
    assert [r.id for r in rows] == [first.id]


async def test_cross_actor_hit_on_a_bare_caller_connection_admits_nothing(
    backend_pair: Backend,
) -> None:
    """On a caller-supplied connection with no open transaction the batch
    INSERT would otherwise autocommit before the hit is seen; the batch
    tier opens its own scope so the refusal still withdraws every row."""
    from taskq.backend.postgres import PostgresBackend

    if not isinstance(backend_pair, PostgresBackend):
        pytest.skip("the caller-connection path is Postgres-only")
    key = f"k-{new_uuid()}"
    await backend_pair.enqueue(_args("actor_a", key))
    fresh = _args("actor_b", f"{key}-fresh")

    pool = backend_pair._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: the bare-connection path is reached only through a caller-supplied conn.
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        assert not conn.is_in_transaction()
        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend_pair.enqueue_batch([fresh, _args("actor_b", key)], connection=conn)
        assert not conn.is_in_transaction(), "the refusal must leave the connection as it found it"

    rows = await backend_pair.list_jobs(JobFilter(actor="actor_b", limit=100))
    assert all(r.id != fresh.id for r in rows), "the refused batch must leave no rows behind"


async def test_cross_actor_hit_on_the_fast_tier_raises_the_mismatch_error(
    backend_pair: Backend,
) -> None:
    """The COPY tier has no arbiter — any duplicate pair aborts the whole
    batch — but a pair held by ANOTHER actor is the same misuse the single
    and batch tiers refuse with the typed mismatch, not a same-actor
    duplicate, and is classified the same way on both backends."""
    key = f"k-{new_uuid()}"
    existing = await backend_pair.enqueue(_args("actor_a", key))
    fresh = _args("actor_b", f"{key}-fresh")

    with pytest.raises(IdempotencyKeyActorMismatchError) as excinfo:
        await backend_pair.enqueue_batch_fast([fresh, _args("actor_b", key)])

    err = excinfo.value
    assert err.actor == "actor_b"
    assert err.existing_actor == "actor_a"
    assert err.existing_job_id == existing.id
    assert err.idempotency_key == key
    rows = await backend_pair.list_jobs(JobFilter(actor="actor_b", limit=100))
    assert all(r.id != fresh.id for r in rows), "the refused batch must leave no rows behind"


async def test_cross_actor_pair_inside_one_fast_batch_names_the_two_actors(
    backend_pair: Backend,
) -> None:
    """Two items of one COPY batch sharing a pair across actors: the abort
    lands on the second item, so it is the incoming actor and the first
    item's actor the existing one; no row persisted, so no existing id."""
    key = f"k-{new_uuid()}"

    with pytest.raises(IdempotencyKeyActorMismatchError) as excinfo:
        await backend_pair.enqueue_batch_fast(
            [_args("actor_a", key), _args("actor_b", key), _args("actor_a", key)]
        )

    err = excinfo.value
    assert (err.actor, err.existing_actor) == ("actor_b", "actor_a")
    assert err.existing_job_id is None
    assert err.idempotency_key == key
    for actor in ("actor_a", "actor_b"):
        rows = await backend_pair.list_jobs(JobFilter(actor=actor, limit=100))
        assert rows == [], "the refused batch must leave no rows behind"


async def test_same_actor_duplicate_on_the_fast_tier_stays_the_duplicate_error(
    backend_pair: Backend,
) -> None:
    from taskq.exceptions import DuplicateIdempotencyKeyError

    key = f"k-{new_uuid()}"
    await backend_pair.enqueue(_args("actor_a", key))

    with pytest.raises(DuplicateIdempotencyKeyError):
        await backend_pair.enqueue_batch_fast([_args("actor_a", key)])
