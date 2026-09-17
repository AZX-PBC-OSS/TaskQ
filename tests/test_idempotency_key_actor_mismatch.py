"""A cross-actor idempotency hit is refused, never silently resolved.

Uniqueness is ``(idempotency_scope, idempotency_key)`` — schema-wide, not
per actor, and the jobs guide tells callers to namespace keys per actor.
When they do not, ``enqueue(send_receipt, key="order-1")`` after
``enqueue(refund, key="order-1")`` used to return the REFUND's handle with
``was_existing=True`` and an INFO-level dedup log: a handle whose
``.result()`` is another actor's, indistinguishable to the caller from a
successful dedup of its own job. River folds the job kind into its unique
key and Oban's default unique fields include the worker, so neither can
hand back a different worker's job; TaskQ's index cannot include the actor
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
