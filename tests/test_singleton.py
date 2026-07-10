"""Unit-tier and integration-tier tests for singleton enforcement.

Covers:
- Enqueue singleton actor twice; second raises SingletonCollisionError.
- Catch SingletonCollisionError as BackpressureError.
- Singleton + mark succeeded + re-enqueue returns fresh row.
- Singleton with schedule_to_close → retry_after.
- Non-singleton actor enqueued twice → both succeed.
- Singleton with identity; different site_id still blocked.
- through Integration-tier PG enforcement tests.
- Negative path integration tests.
- Chaos test — connection failure mid-pre-flight.
- Hypothesis property test against live PG backend.
"""

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal
from unittest.mock import AsyncMock

import asyncpg
import pytest
from hypothesis import HealthCheck, given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey
from taskq.exceptions import BackpressureError, SingletonCollisionError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object

_START = datetime(2025, 1, 1, tzinfo=UTC)

# ── Helpers ────────────────────────────────────────────────────────────


def _make_backend() -> tuple[FakeClock, InMemoryBackend]:
    clock = FakeClock(_START)
    backend = InMemoryBackend(clock)
    return clock, backend


def _singleton_args(
    actor: str = "test_actor",
    queue: str = "default",
    *,
    schedule_to_close: datetime | None = None,
    identity_key: str | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"singleton": True},
        schedule_to_close=schedule_to_close,
        identity_key=IdentityKey(identity_key) if identity_key is not None else None,
    )


def _non_singleton_args(
    actor: str = "test_actor",
    queue: str = "default",
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )


# ── singleton collision ─────────────────────────────────────────


async def test_singleton_collision_second_enqueue_raises() -> None:
    """Enqueue a singleton actor twice; second raises SingletonCollisionError."""
    _clock, backend = _make_backend()

    first = await backend.enqueue(_singleton_args())
    assert first.status == "pending"

    with pytest.raises(SingletonCollisionError) as exc_info:
        await backend.enqueue(_singleton_args())

    exc = exc_info.value
    assert exc.actor == "test_actor"
    assert exc.blocking_job_id == first.id


# ── catch as BackpressureError ──────────────────────────────────


async def test_singleton_collision_caught_as_backpressure_error() -> None:
    """SingletonCollisionError caught as BackpressureError."""
    _clock, backend = _make_backend()

    await backend.enqueue(_singleton_args())

    with pytest.raises(BackpressureError) as exc_info:
        await backend.enqueue(_singleton_args())

    exc = exc_info.value
    assert isinstance(exc, SingletonCollisionError)
    assert exc.actor == "test_actor"


# ── terminal singleton does not block ───────────────────────────


async def test_terminal_singleton_allows_re_enqueue() -> None:
    """Singleton actor: mark succeeded, enqueue again; fresh row."""
    _clock, backend = _make_backend()

    first = await backend.enqueue(_singleton_args())
    first_id = first.id

    backend._jobs[first_id] = replace(first, status="succeeded")

    second = await backend.enqueue(_singleton_args())
    assert second.id != first_id
    assert second.status == "pending"
    assert second.metadata.get("singleton") is True


# ── schedule_to_close → retry_after ────────────────────────────


async def test_singleton_schedule_to_close_provides_retry_after() -> None:
    """Singleton with schedule_to_close; collision retry_after ≈ 5 min."""
    clock, backend = _make_backend()

    close_at = clock.now() + timedelta(minutes=5)

    await backend.enqueue(_singleton_args(schedule_to_close=close_at))

    with pytest.raises(SingletonCollisionError) as exc_info:
        await backend.enqueue(_singleton_args())

    exc = exc_info.value
    assert exc.retry_after is not None
    assert abs(exc.retry_after.total_seconds() - 300) < 1


# ── non-singleton double enqueue → both succeed ────────────────


async def test_non_singleton_double_enqueue_both_succeed() -> None:
    """Non-singleton actor twice; both succeed, two pending rows."""
    _clock, backend = _make_backend()

    first = await backend.enqueue(_non_singleton_args())
    second = await backend.enqueue(_non_singleton_args())

    assert first.status == "pending"
    assert second.status == "pending"
    assert first.id != second.id

    pending = [
        r for r in backend._jobs.values() if r.actor == "test_actor" and r.status == "pending"
    ]
    assert len(pending) == 2


# ── identity does not scope singleton ──────────────────────────


async def test_singleton_identity_does_not_scope() -> None:
    """Singleton with identity; different site_id still blocked."""
    _clock, backend = _make_backend()

    await backend.enqueue(_singleton_args(identity_key="site_a"))

    with pytest.raises(SingletonCollisionError) as exc_info:
        await backend.enqueue(_singleton_args(identity_key="site_b"))

    exc = exc_info.value
    assert exc.actor == "test_actor"


# ═══════════════════════════════════════════════════════════════════════
# Integration-tier tests (require testcontainers Postgres)
# ═══════════════════════════════════════════════════════════════════════


def _pg_singleton_args(
    actor: str = "test_actor",
    queue: str = "default",
    *,
    schedule_to_close: datetime | None = None,
    idempotency_key: str | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC),
        metadata={"singleton": True},
        schedule_to_close=schedule_to_close,
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
    )


def _pg_non_singleton_args(
    actor: str = "test_actor",
    queue: str = "default",
    *,
    idempotency_key: str | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC),
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
    )


# ── partial unique index enforcement (direct INSERT) ────────────


@pytest.mark.integration
async def test_direct_insert_duplicate_singleton_constraint(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Direct INSERT two singleton rows for same actor; second raises UniqueViolationError."""
    deps, _backend = clean_jobs_app
    schema = deps.settings.schema_name
    actor = "ti1_actor"

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO \"{schema}\".jobs
            (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, metadata)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8::jsonb)""",
            new_uuid(),
            actor,
            "default",
            "{}",
            3,
            "transient",
            datetime.now(UTC),
            '{"singleton": true}',
        )

    async with deps.worker_pool.acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError) as exc_info:
            await conn.execute(
                f"""INSERT INTO \"{schema}\".jobs
                (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, metadata)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8::jsonb)""",
                new_uuid(),
                actor,
                "default",
                "{}",
                3,
                "transient",
                datetime.now(UTC),
                '{"singleton": true}',
            )

    assert exc_info.value.constraint_name == "jobs_singleton_uniq"

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── Layer 1 pre-flight catch (via backend) ──────────────────────


@pytest.mark.integration
async def test_singleton_enqueue_collision_layer1(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Enqueue singleton via backend; enqueue again; Layer 1 raises SingletonCollisionError."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "ti2_actor"
    row1 = await backend.enqueue(_pg_singleton_args(actor=actor))

    with pytest.raises(SingletonCollisionError) as exc_info:
        await backend.enqueue(_pg_singleton_args(actor=actor))

    exc = exc_info.value
    assert exc.actor == actor
    assert exc.blocking_job_id == row1.id

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', actor)
    assert count == 1

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── Layer 2 UniqueViolation race catch ──────────────────────────


@pytest.mark.integration
async def test_singleton_concurrent_race_layer2(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Monkeypatch pre-flight to None; concurrent enqueues race at INSERT;
    exactly one succeeds, one raises SingletonCollisionError with blocking_job_id=None."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "ti3_actor"

    from dataclasses import replace

    monkeypatch.setattr(
        backend,
        "_sql",
        replace(
            backend._sql,  # type: ignore[reportPrivateUsage]
            singleton_preflight=f'SELECT id, schedule_to_close FROM "{schema}".jobs WHERE actor = $1 AND FALSE LIMIT 1',
        ),
    )

    async def _enqueue() -> bool:
        try:
            await backend.enqueue(_pg_singleton_args(actor=actor))
            return True
        except SingletonCollisionError as exc:
            assert exc.blocking_job_id is None
            assert exc.actor == actor
            return False

    results = await asyncio.gather(_enqueue(), _enqueue())
    assert results.count(True) == 1
    assert results.count(False) == 1

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── terminal singleton does not block ──────────────────────────


@pytest.mark.integration
async def test_singleton_terminal_allows_re_enqueue_pg(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Singleton actor: mark succeeded, enqueue again; second succeeds, count=2."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "ti4_actor"
    row1 = await backend.enqueue(_pg_singleton_args(actor=actor))

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
            row1.id,
        )

    row2 = await backend.enqueue(_pg_singleton_args(actor=actor))
    assert row2.id != row1.id
    assert row2.status == "pending"

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', actor)
    assert count == 2

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── repeated collisions return same blocking_job_id ─────────────


@pytest.mark.integration
async def test_singleton_multiple_collisions_same_blocking_id(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Enqueue singleton; 5 more enqueues all raise collision with same blocking_job_id."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "ti5_actor"
    row1 = await backend.enqueue(_pg_singleton_args(actor=actor))

    for _ in range(5):
        with pytest.raises(SingletonCollisionError) as exc_info:
            await backend.enqueue(_pg_singleton_args(actor=actor))
        assert exc_info.value.blocking_job_id == row1.id

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', actor)
    assert count == 1

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── singleton blocks before max_concurrent ──────────────────────


@pytest.mark.integration
async def test_singleton_blocks_before_max_concurrent_dispatch(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Enqueue singleton; dispatch to running; enqueue again raises collision at enqueue."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "ti6_actor"
    row1 = await backend.enqueue(_pg_singleton_args(actor=actor))

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"""UPDATE \"{schema}\".jobs
            SET status = 'running', started_at = now(),
                lock_expires_at = now() + interval '60 seconds'
            WHERE id = $1""",
            row1.id,
        )

    with pytest.raises(SingletonCollisionError) as exc_info:
        await backend.enqueue(_pg_singleton_args(actor=actor))

    exc = exc_info.value
    assert exc.actor == actor
    assert exc.blocking_job_id == row1.id

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── singleton index is per-actor ────────────────────────────────


@pytest.mark.integration
async def test_singleton_different_actors_both_succeed(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Two singleton actors A and B; both enqueue successfully."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor_a = "ti7_actor_a"
    actor_b = "ti7_actor_b"
    row_a = await backend.enqueue(_pg_singleton_args(actor=actor_a))
    row_b = await backend.enqueue(_pg_singleton_args(actor=actor_b))

    assert row_a.status == "pending"
    assert row_b.status == "pending"

    async with deps.worker_pool.acquire() as conn:
        count_a = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', actor_a
        )
        count_b = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', actor_b
        )
    assert count_a == 1
    assert count_b == 1

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor IN ($1, $2)', actor_a, actor_b)


# ── idempotency dedup is not a singleton collision ──────────────


@pytest.mark.integration
async def test_idempotency_dedup_not_singleton_collision(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Non-singleton actor with idempotency_key; second enqueue dedup, no collision."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "tn3_actor"
    key = "tn3_key"
    args1 = _pg_non_singleton_args(actor=actor, idempotency_key=key)
    row1 = await backend.enqueue(args1)

    args2 = _pg_non_singleton_args(actor=actor, idempotency_key=key)
    row2 = await backend.enqueue(args2)

    assert row1.id == row2.id

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── transaction rollback does not leak singleton rows ───────────


@pytest.mark.integration
async def test_singleton_rollback_does_not_leak(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Direct INSERT inside rolled-back transaction; row does not persist;
    re-enqueue via backend succeeds."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor = "tn4_actor"
    job_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(
                    f"""INSERT INTO \"{schema}\".jobs
                    (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, metadata)
                    VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8::jsonb)""",
                    job_id,
                    actor,
                    "default",
                    "{}",
                    3,
                    "transient",
                    datetime.now(UTC),
                    '{"singleton": true}',
                )
                raise asyncpg.TransactionRollbackError()
        except asyncpg.TransactionRollbackError:
            pass

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', actor)
    assert count == 0

    row = await backend.enqueue(_pg_singleton_args(actor=actor))
    assert row.status == "pending"

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)


# ── connection failure mid-pre-flight ───────────────────────────


@pytest.mark.integration
async def test_singleton_connection_failure_mid_preflight(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kill PG connection mid-pre-flight; assert PostgresConnectionError raised."""
    deps, backend = clean_jobs_app

    actor = "tc1_actor"

    class _MockPool:
        @asynccontextmanager
        async def acquire(self):
            mock_conn = AsyncMock()

            @asynccontextmanager
            async def _mock_transaction():
                yield

            mock_conn.transaction = _mock_transaction
            mock_conn.fetchrow.side_effect = asyncpg.PostgresConnectionError("connection killed")
            yield mock_conn

    monkeypatch.setattr(backend, "_worker_pool", _MockPool())

    with pytest.raises(asyncpg.PostgresConnectionError):
        await backend.enqueue(_pg_singleton_args(actor=actor))

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{deps.settings.schema_name}".jobs WHERE actor = $1', actor
        )
    assert count == 0


# ═══════════════════════════════════════════════════════════════════════
# Hypothesis property test (integration tier)
# ═══════════════════════════════════════════════════════════════════════


type _SingletonOp = (
    tuple[Literal["enqueue"], int]
    | tuple[Literal["dispatch"]]
    | tuple[Literal["mark_succeeded"]]
    | tuple[Literal["mark_failed"]]
    | tuple[Literal["mark_cancelled"]]
)

_enqueue_op_strategy: st.SearchStrategy[tuple[Literal["enqueue"], int]] = st.tuples(
    st.just("enqueue"),
    st.integers(min_value=0, max_value=999),
)

_status_ops_strategy: st.SearchStrategy[
    tuple[Literal["mark_succeeded"]]
    | tuple[Literal["mark_failed"]]
    | tuple[Literal["mark_cancelled"]]
] = st.one_of(
    st.just(("mark_succeeded",)),
    st.just(("mark_failed",)),
    st.just(("mark_cancelled",)),
)

_operation_strategy: st.SearchStrategy[_SingletonOp] = st.one_of(
    _enqueue_op_strategy,
    st.just(("dispatch",)),
    _status_ops_strategy,
)

_singleton_operation_sequence = st.lists(_operation_strategy, min_size=1, max_size=20)


@hyp_settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    ops=_singleton_operation_sequence,
    example_idx=st.integers(min_value=0, max_value=999),
)
@pytest.mark.integration
async def test_property_singleton_invariant_pg(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
    ops: list[_SingletonOp],
    example_idx: int,
) -> None:
    """Hypothesis property test: for randomised enqueue/dispatch/terminal
    sequences, the singleton invariant holds — at most one active job per actor."""
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name
    actor = f"prop_{example_idx}"
    worker_id = new_uuid()

    async def _check_invariant() -> int:
        async with deps.worker_pool.acquire() as conn:
            return await conn.fetchval(
                f"""SELECT count(*) FROM \"{schema}\".jobs
                WHERE actor = $1 AND status IN ('pending', 'scheduled', 'running')
                AND metadata @> '{{"singleton": true}}'::jsonb""",
                actor,
            )

    try:
        for op in ops:
            op_name = op[0]

            if op_name == "enqueue":
                with suppress(SingletonCollisionError):
                    await backend.enqueue(
                        EnqueueArgs(
                            id=new_job_id(),
                            actor=actor,
                            queue="default",
                            payload={},
                            max_attempts=3,
                            retry_kind="transient",
                            scheduled_at=datetime.now(UTC),
                            metadata={"singleton": True},
                        )
                    )

            elif op_name == "dispatch":
                async with deps.worker_pool.acquire() as conn:
                    await conn.execute(
                        f"""UPDATE \"{schema}\".jobs
                        SET status = 'running',
                            started_at = now(),
                            lock_expires_at = now() + interval '300 seconds',
                            locked_by_worker = $2
                        WHERE id = (
                            SELECT id FROM \"{schema}\".jobs
                            WHERE actor = $1 AND status = 'pending'
                            ORDER BY priority DESC, scheduled_at ASC, id ASC
                            LIMIT 1
                        )""",
                        actor,
                        worker_id,
                    )

            elif op_name in ("mark_succeeded", "mark_failed", "mark_cancelled"):
                new_status = op_name.split("_", 1)[1]
                async with deps.worker_pool.acquire() as conn:
                    await conn.execute(
                        f"""UPDATE \"{schema}\".jobs
                        SET status = $3, finished_at = now()
                        WHERE id = (
                            SELECT id FROM \"{schema}\".jobs
                            WHERE actor = $1 AND status = 'running'
                              AND locked_by_worker = $2
                            LIMIT 1
                        )""",
                        actor,
                        worker_id,
                        new_status,
                    )

            active = await _check_invariant()
            assert active <= 1, f"singleton invariant violated: {active} active jobs for {actor}"

    finally:
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor)
