# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team pin: the idempotency ON CONFLICT path with NO advisory-lock hit.

The unique_for preflight serializes same-(actor, identity_key) enqueues with
a transaction-scoped advisory lock keyed on
``taskq:unique_for:{schema}:{actor}:{identity_key}`` - pinned already by
``tests/test_postgres_unique_for_single_flight.py`` (100 racers).  But the
idempotency arbiter is keyed on ``(idempotency_scope, idempotency_key)``
with NO actor in it, so two enqueues from DIFFERENT actors sharing one
(scope, key) never touch the same advisory lock: there is no serialization
before the INSERT.  What arbitrates instead is the composite partial unique
index plus ON CONFLICT DO NOTHING's wait semantics - an INSERT that hits an
in-flight conflicting insertion BLOCKS until that transaction resolves, so
the loser's follow-up ``enqueue_select_by_key`` SELECT runs only after the
winner committed, and must find the winner's row.

``tests/test_postgres_idempotency_scope.py`` pins the concurrent
same-(scope,key) race only for ONE actor (the default ``test_actor``), so
the cross-actor, no-lock-hit window is unpinned.  Green pin here: exactly
one row, and the loser's follow-up SELECT finds the winner's committed
row - which, being another actor's, it refuses with
``IdempotencyKeyActorMismatchError`` naming that row (a cross-actor hit is
not a dedup of the loser's job; see ``tests/test_idempotency_key_actor_
mismatch.py``) rather than being handed it as its own.

The singleton path is not re-tested: ``tests/test_singleton.py``'s layer-2
test already pins the concurrent INSERT race (preflight miss → unique
violation → SingletonCollisionError) and the partial index
``jobs_singleton_uniq`` carries the status window, so terminal singletons
do not brick the actor.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, JobRow
from taskq.exceptions import IdempotencyKeyActorMismatchError
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

_QUEUE = "default"
_BOUNDED_WAIT_SECS = 15.0
_BLOCK_SETTLE_SECS = 0.25


def _args(actor: str, *, scope: str, key: IdempotencyKey) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=_QUEUE,
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        idempotency_scope=scope,
        idempotency_key=key,
    )


async def _teardown(stack: Any, pg_dsn: str, schema: str) -> None:
    await stack.aclose()
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


async def test_cross_actor_same_scope_key_concurrent_enqueue_resolves_via_index_wait(
    pg_dsn: str,
) -> None:
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    scope = f"tqr-scope-{new_base62()}"
    key = IdempotencyKey(f"tqr-key-{new_base62()}")
    actor_l = "tqr_xident_left"
    actor_r = "tqr_xident_right"
    try:
        conn_l = await deps.worker_pool.acquire()
        conn_r = await deps.worker_pool.acquire()
        task_r: asyncio.Task[JobRow] | None = None
        try:
            tx_l = conn_l.transaction()
            await tx_l.start()
            try:
                row_l: JobRow = await backend.enqueue_with_conn(
                    conn_l, _args(actor_l, scope=scope, key=key)
                )

                async def _racer() -> JobRow:
                    async with conn_r.transaction():
                        return await backend.enqueue_with_conn(
                            conn_r, _args(actor_r, scope=scope, key=key)
                        )

                task_r = asyncio.create_task(_racer(), name="tqr-cross-actor-racer")
                await asyncio.sleep(_BLOCK_SETTLE_SECS)
                await tx_l.commit()
            except BaseException:
                await tx_l.rollback()
                if task_r is not None and not task_r.done():
                    task_r.cancel()
                raise
            assert task_r is not None
            with pytest.raises(IdempotencyKeyActorMismatchError) as refusal:
                await asyncio.wait_for(task_r, timeout=_BOUNDED_WAIT_SECS)
        finally:
            await deps.worker_pool.release(conn_l)
            await deps.worker_pool.release(conn_r)

        assert refusal.value.existing_job_id == row_l.id, (
            f"CONTRACT: two concurrent enqueues sharing (idempotency_scope, "
            f"idempotency_key) resolve to ONE job - the composite partial unique "
            f"index plus ON CONFLICT DO NOTHING's wait on the in-flight twin "
            f"arbitrate, and the loser's enqueue_select_by_key SELECT must find the "
            f"winner's committed row. No advisory lock serializes them here (the "
            f"unique_for lock key includes the actor, and these are two different "
            f"actors). Got winner {row_l.id} vs the racer's refusal naming "
            f"{refusal.value.existing_job_id}."
        )
        assert refusal.value.existing_actor == actor_l and refusal.value.actor == actor_r, (
            "the refusal names the surviving row's actor and the racer's own"
        )
        async with deps.worker_pool.acquire() as conn:
            val: Any = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE idempotency_scope = $1 AND idempotency_key = $2",
                scope,
                key,
            )
            assert isinstance(val, int)
            assert val == 1, (
                f"CONTRACT: exactly one row may exist per (idempotency_scope, "
                f"idempotency_key); the no-advisory-lock path double-inserted ({val} "
                f"rows) - the ON CONFLICT arbiter did not wait out the in-flight twin."
            )
            row_count: Any = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1',
                row_l.id,
            )
            assert isinstance(row_count, int)
            assert row_count == 1
    finally:
        await _teardown(stack, pg_dsn, schema)
