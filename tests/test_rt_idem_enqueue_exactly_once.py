# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""RED-TEAM pins: the idempotency key is ONE outcome no matter how the world fails.

Scenario 1 - enqueue-with-key across every failure point between client and
row. The documented dedup channel for a caller's retry is the idempotency key
(``taskq/connections.py``: "idempotency keys remain the dedup channel for the
retry the caller chooses"). The retry is a no-op returning the SAME job id:

* a fresh client call (new job id, same key) after the first row COMMITTED
  dedupes to the committed row,
* the pool-retry wrapper's own re-run (identical args, identical id) dedupes
  to the committed row - the arbiter catches the pair before the pkey can,
* the InMemory twin agrees with Postgres on both shapes.

Scenario 2 - one error shape per collision class, the detection path
invisible to the client. A singleton collision caught by the Layer-1
preflight SELECT and one caught by the Layer-2 ``jobs_singleton_uniq``
UniqueViolationError catch (the race path) raise the SAME exception class -
``detection_path`` exists only in the log stream, never on the error - so a
caller's ``except`` branch cannot branch on how the collision was detected.
Same property for a cross-actor idempotency hit: the sequential path (the
holder row visible before the INSERT) and the raced path (the arbiter's
follow-up SELECT after the loser's DO NOTHING) raise the identical
``IdempotencyKeyActorMismatchError`` with identical field values.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey
from taskq.exceptions import IdempotencyKeyActorMismatchError, SingletonCollisionError
from taskq.testing.fixtures import _open_pg_backend
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

_BOUNDED_WAIT_SECS = 15.0
_BLOCK_SETTLE_SECS = 0.25


async def _fresh_backend(pg_dsn: str) -> tuple[Any, Any, Any, str]:
    """A backend on a fresh schema: ``(stack, deps, backend, schema)``.

    The caller awaits ``stack.aclose()`` and then tears the schema down
    through :func:`_drop_schema` in its ``finally``.
    """
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    return stack, deps, backend, schema


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


# ── Scenario 1: the retry is a no-op returning the SAME job id ───────────


async def test_client_retry_new_id_same_key_returns_same_job_id(pg_dsn: str) -> None:
    """A client retry after an ambiguous first outcome (the row committed but
    the ack was lost) issues a FRESH job id with the SAME idempotency key.
    The committed row must come back: same id, no second row."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    key = IdempotencyKey(f"rt-key-{new_base62()}")
    try:
        first = make_enqueue_args(idempotency_key=str(key))
        retry = make_enqueue_args(idempotency_key=str(key))
        assert first.id != retry.id, "a fresh client call issues a fresh job id"

        row1 = await backend.enqueue(first)
        row2 = await backend.enqueue(retry)

        assert row2.id == row1.id, (
            f"CONTRACT: the retry with the same idempotency key is a no-op "
            f"returning the SAME job id; got {row1.id} then {row2.id} - the "
            f"second call created a duplicate side effect."
        )
        async with deps.worker_pool.acquire() as conn:
            val: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE idempotency_scope = $1 AND idempotency_key = $2",
                "",
                str(key),
            )
        assert val == 1, (
            f"CONTRACT: exactly one row may exist per (scope, key); the retry "
            f"inserted a second ({val} rows)."
        )
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


async def test_pool_retry_rerun_with_identical_args_dedupes(pg_dsn: str) -> None:
    """The pool-retry wrapper re-runs the op with the IDENTICAL args (same
    job id, same key) when the first attempt's write was never acknowledged.
    The arbiter must catch the pair: the re-run returns the committed row
    instead of raising a raw jobs_pkey violation."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    key = IdempotencyKey(f"rt-key-{new_base62()}")
    try:
        args = make_enqueue_args(idempotency_key=str(key))
        row1 = await backend.enqueue(args)
        # The identical re-run: the shape _with_fresh_connection_retry's
        # second attempt executes after an unacknowledged write.
        row2 = await backend.enqueue(args)

        assert row2.id == row1.id, (
            f"CONTRACT: the wrapper's identical-args re-run dedupes to the "
            f"committed row; got {row1.id} then {row2.id}."
        )
        async with deps.worker_pool.acquire() as conn:
            val: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
        assert val == 1, f"the identical re-run duplicated the row ({val} rows)"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


async def test_twin_client_retry_same_key_returns_same_job_id() -> None:
    """Twin differential, scenario 1: the InMemory mirror dedupes both retry
    shapes exactly like Postgres."""
    from taskq.testing._runner import register_actor_config
    from taskq.testing.clock import FakeClock

    start = datetime(2026, 1, 1, tzinfo=UTC)
    backend = InMemoryBackend(clock=FakeClock(start=start))
    register_actor_config(backend, actor="test_actor")
    past = start - timedelta(seconds=1)

    key = "rt-twin-key"
    row1 = await backend.enqueue(make_enqueue_args(idempotency_key=key, scheduled_at=past))
    row2 = await backend.enqueue(make_enqueue_args(idempotency_key=key, scheduled_at=past))
    assert row2.id == row1.id, "twin: the same-key retry must return the same job id"

    row3 = await backend.enqueue(make_enqueue_args(idempotency_key=key, scheduled_at=past))
    assert row3.id == row1.id, "twin: the identical-args re-run must dedupe to the same row"
    assert len(backend._jobs) == 1, (  # pyright: ignore[reportAttributeAccessIssue]  # Why: the twin differential reads the store the same way tests/test_in_memory_idempotency.py does.
        f"twin: exactly one row may exist; got {len(backend._jobs)}"
    )


# ── Scenario 2: one error shape per collision class ──────────────────────


def _singleton_args(actor: str) -> EnqueueArgs:
    return make_enqueue_args(actor=actor, metadata={"singleton": True})


async def test_singleton_collision_same_error_shape_both_detection_paths(pg_dsn: str) -> None:
    """Layer-1 preflight and Layer-2 unique-violation catch raise the SAME
    exception class with no detection_path attribute: a caller's except
    branch cannot tell how the collision was detected."""
    stack, _deps, backend, schema = await _fresh_backend(pg_dsn)
    actor = "rt_singleton_shape"
    try:
        row1 = await backend.enqueue(_singleton_args(actor))

        # Layer 1: the preflight SELECT sees the live singleton row.
        with pytest.raises(SingletonCollisionError) as layer1:
            await backend.enqueue(_singleton_args(actor))

        # Layer 2: blind the preflight, two concurrent enqueues race at the
        # INSERT, the loser's jobs_singleton_uniq violation is caught by the
        # UniqueViolationError arm (the seam tests/test_singleton.py's
        # layer-2 pin uses). A second actor keeps this race independent of
        # the Layer-1 row above.
        actor_r = "rt_singleton_shape_racer"
        blinded = (
            f'SELECT id, schedule_to_close FROM "{schema}".jobs WHERE actor = $1 AND FALSE LIMIT 1'
        )
        backend._sql = dataclasses.replace(backend._sql, singleton_preflight=blinded)  # pyright: ignore[reportAttributeAccessIssue]  # Why: the same seam test_singleton.py patches.

        caught: list[SingletonCollisionError] = []

        async def _racer() -> None:
            try:
                await backend.enqueue(_singleton_args(actor_r))
            except SingletonCollisionError as exc:
                caught.append(exc)

        await asyncio.gather(_racer(), _racer())
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)

    assert layer1.value.actor == actor
    assert layer1.value.blocking_job_id == row1.id
    assert len(caught) == 1, (
        f"CONTRACT: with the preflight blinded exactly one of the two racers "
        f"succeeds and exactly one hits the Layer-2 catch; got {len(caught)} refusals."
    )
    hit = caught[0]
    assert type(hit) is type(layer1.value) is SingletonCollisionError, (
        "CONTRACT: Layer-1 preflight and Layer-2 unique-violation catch raise "
        "the same error class; a caller branching on the error type would "
        "branch on the detection path."
    )
    assert not hasattr(hit, "detection_path") and not hasattr(layer1.value, "detection_path"), (
        "CONTRACT: detection_path is a log field, never an error attribute; an "
        "error carrying it lets the caller branch on the detection path."
    )
    assert hit.blocking_job_id is None and hit.retry_after is None, (
        "the Layer-2 race catch fetched no blocking row: the documented None "
        "fields (SingletonCollisionError's docstring)"
    )


async def test_cross_actor_mismatch_same_shape_stored_row_vs_arbiter_race(pg_dsn: str) -> None:
    """A cross-actor idempotency hit caught with the holder row visible
    before the INSERT (the sequential path) and one caught after the arbiter's
    DO NOTHING (the raced path) raise the identical error with identical
    field values."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    scope = f"rt-scope-{new_base62()}"
    key = IdempotencyKey(f"rt-xact-{new_base62()}")
    actor_a, actor_b = "rt_xshape_left", "rt_xshape_right"
    try:
        row_a = await backend.enqueue(
            make_enqueue_args(actor=actor_a, idempotency_key=str(key), idempotency_scope=scope)
        )
        with pytest.raises(IdempotencyKeyActorMismatchError) as sequential:
            await backend.enqueue(
                make_enqueue_args(actor=actor_b, idempotency_key=str(key), idempotency_scope=scope)
            )

        # The raced path: a fresh pair, holder A's INSERT uncommitted while
        # B's enqueue blocks on the arbiter and resolves after the commit -
        # the follow-up SELECT then refuses exactly like the sequential path.
        scope2 = f"rt-scope2-{new_base62()}"
        key2 = IdempotencyKey(f"rt-xact2-{new_base62()}")
        conn_l = await deps.worker_pool.acquire()
        conn_r = await deps.worker_pool.acquire()
        task_r: asyncio.Task[object] | None = None
        tx_l = conn_l.transaction()
        await tx_l.start()
        try:
            row2_a = await backend.enqueue_with_conn(
                conn_l,
                make_enqueue_args(
                    actor=actor_a, idempotency_key=str(key2), idempotency_scope=scope2
                ),
            )

            async def _racer() -> object:
                async with conn_r.transaction():
                    return await backend.enqueue_with_conn(
                        conn_r,
                        make_enqueue_args(
                            actor=actor_b, idempotency_key=str(key2), idempotency_scope=scope2
                        ),
                    )

            task_r = asyncio.create_task(_racer(), name="rt-xshape-racer")
            await asyncio.sleep(_BLOCK_SETTLE_SECS)
            await tx_l.commit()
        except BaseException:
            await tx_l.rollback()
            if task_r is not None and not task_r.done():
                task_r.cancel()
            await deps.worker_pool.release(conn_l)
            await deps.worker_pool.release(conn_r)
            raise
        with pytest.raises(IdempotencyKeyActorMismatchError) as raced:
            await asyncio.wait_for(task_r, timeout=_BOUNDED_WAIT_SECS)
        await deps.worker_pool.release(conn_l)
        await deps.worker_pool.release(conn_r)
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)

    assert type(raced.value) is type(sequential.value) is IdempotencyKeyActorMismatchError
    assert sequential.value.actor == raced.value.actor == actor_b
    assert sequential.value.existing_actor == raced.value.existing_actor == actor_a
    assert sequential.value.existing_job_id == row_a.id
    assert raced.value.existing_job_id == row2_a.id, (
        "CONTRACT: the raced catch names the winner's committed row, the same "
        "field the sequential preflight-visible path names - the error shape "
        "does not depend on which detection path caught the collision."
    )
