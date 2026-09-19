"""Unit tests for enqueue_batch_streaming."""

from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._args import build_enqueue_args
from taskq.client._jobs import JobsClient
from taskq.exceptions import BatchMaxPendingExceededError
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


@actor(name="batch_streaming_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend=backend, clock=FakeClock(start=_START))


def _make_item(value: int = 0) -> EnqueueItem:
    return EnqueueItem(actor_ref=_test_actor, payload=_Payload(value=value))


def _items_gen(count: int) -> Iterable[EnqueueItem]:
    return (_make_item(i) for i in range(count))


class TestStreamingBasic:
    async def test_large_iterable(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue_batch_streaming(_items_gen(2500), chunk_size=1000)

        assert handle.size == 2500
        assert len(handle.job_handles) == 2500
        assert isinstance(handle.batch_id, UUID)

        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.metadata.get("batch_id") == str(handle.batch_id)

    async def test_small_iterable(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue_batch_streaming(_items_gen(10))

        assert handle.size == 10
        assert len(handle.job_handles) == 10

    async def test_all_jobs_share_one_batch_id(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue_batch_streaming(_items_gen(2500), chunk_size=1000)

        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.metadata.get("batch_id") == str(handle.batch_id)


class TestStreamingWithPolicy:
    async def test_with_failure_policy(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue_batch_streaming(
            _items_gen(1500),
            chunk_size=1000,
            failure_policy=AbortBatchAfter(3),
        )

        batch_row = backend._batches.get(handle.batch_id)
        assert batch_row is not None
        assert batch_row.failure_threshold == 3
        assert batch_row.expected_size == 1500
        assert batch_row.status == "active"

    async def test_with_finalizer(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        finalizer = EnqueueItem(
            actor_ref=_test_actor,
            payload=_Payload(value=-1),
        )

        handle = await client.enqueue_batch_streaming(
            _items_gen(10),
            finalizer=finalizer,
        )

        assert handle.finalizer_handle is not None
        assert handle.size == 10
        assert len(handle.job_handles) == 11

        fin_row = await backend.get(handle.finalizer_handle.job_id)
        assert fin_row is not None
        assert "batch_id" not in fin_row.metadata

    async def test_streaming_with_policy_and_finalizer(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        finalizer = EnqueueItem(
            actor_ref=_test_actor,
            payload=_Payload(value=-1),
        )

        handle = await client.enqueue_batch_streaming(
            _items_gen(20),
            chunk_size=10,
            failure_policy=AbortBatchAfter(5),
            finalizer=finalizer,
        )

        # Batch row must have both finalizer_job_id and failure_threshold set.
        batch_row = backend._batches.get(handle.batch_id)
        assert batch_row is not None
        assert batch_row.failure_threshold == 5
        assert batch_row.finalizer_job_id is not None
        assert batch_row.expected_size == 20

        # Finalizer job must NOT have batch_id metadata.
        assert handle.finalizer_handle is not None
        fin_row = await backend.get(handle.finalizer_handle.job_id)
        assert fin_row is not None
        assert "batch_id" not in fin_row.metadata


class TestStreamingValidation:
    async def test_empty_iterable_raises(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        with pytest.raises(ValueError, match="empty"):
            await client.enqueue_batch_streaming(iter([]))

    async def test_invalid_chunk_size_zero_raises(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        with pytest.raises(ValueError, match="chunk_size"):
            await client.enqueue_batch_streaming(_items_gen(5), chunk_size=0)

    async def test_invalid_chunk_size_too_large_raises(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)

        with pytest.raises(ValueError, match="chunk_size"):
            await client.enqueue_batch_streaming(_items_gen(5), chunk_size=1001)


# ── No-connection path - per-chunk commits, typed refusal ──────────────


@actor(name="batch_streaming_partition_healthy")
async def _partition_healthy(_payload: _Payload) -> None:
    pass


@actor(name="batch_streaming_partition_capped", max_pending=1)
async def _partition_capped(_payload: _Payload) -> None:
    pass


async def test_no_conn_chunk_failure_commits_prefix_and_raises_typed_error() -> None:
    """PIN: with no caller connection each chunk is its own pool
    transaction, so a cap refusal on chunk N leaves chunks 1..N-1 (plus
    the refusing chunk's within-cap actors, under the per-actor
    partition) durably committed, and the call raises the typed batch
    error with STREAM-GLOBAL item indices - everything a caller needs to
    retry only the refused items instead of duplicating the prefix."""
    backend = _make_backend()
    client = _make_client(backend)
    # Fill the capped actor's single slot so its first stream item is
    # refused.
    await backend.enqueue(build_enqueue_args(_partition_capped, _Payload(value=99), max_pending=1))

    def _stream() -> Iterable[EnqueueItem]:
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=0))
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=1))
        yield EnqueueItem(actor_ref=_partition_capped, payload=_Payload(value=2))
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=3))
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=4))

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await client.enqueue_batch_streaming(_stream(), chunk_size=2)

    err = exc_info.value
    # Stream-global: index 2 is the capped item's position in the caller's
    # stream, not its chunk-local position (0) in the failing chunk.
    assert err.refused_indices == {_partition_capped.name: [2]}
    # The committed prefix: chunk 1's two healthy items plus the failing
    # chunk's one healthy item.
    assert err.admitted_count == 3
    healthy_stored = sum(
        1 for row in backend._jobs.values() if row.actor == _partition_healthy.name
    )
    capped_stored = sum(1 for row in backend._jobs.values() if row.actor == _partition_capped.name)
    assert healthy_stored == 3
    # The capped actor admitted nothing past its single stored slot.
    assert capped_stored == 1


@pytest.mark.integration
async def test_no_conn_chunk_failure_commits_prefix_on_pg(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The same pin against real Postgres: the no-connection path commits
    each chunk in its OWN pool transaction, so the committed prefix
    survives the failing chunk's refusal - the durability claim the
    in-memory mirror cannot prove by itself."""
    from taskq.client._jobs import JobsClient as _JobsClient

    from .test_rt_cron_harness import count_jobs, cron_settings, pool_backend

    schema = module_pg_schema.schema_name
    backend = pool_backend(cron_settings(schema), module_pg_pool)
    client = _JobsClient(backend)
    # Fill the capped actor's single slot.
    await backend.enqueue(build_enqueue_args(_partition_capped, _Payload(value=99), max_pending=1))

    def _stream() -> Iterable[EnqueueItem]:
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=0))
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=1))
        yield EnqueueItem(actor_ref=_partition_capped, payload=_Payload(value=2))
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=3))
        yield EnqueueItem(actor_ref=_partition_healthy, payload=_Payload(value=4))

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await client.enqueue_batch_streaming(_stream(), chunk_size=2)

    err = exc_info.value
    assert err.refused_indices == {_partition_capped.name: [2]}
    assert err.admitted_count == 3
    # Chunks 1..N-1 (and the failing chunk's within-cap actor) are
    # durably committed in their own transactions.
    assert await count_jobs(clean_pg_conn, schema, _partition_healthy.name) == 3
    assert await count_jobs(clean_pg_conn, schema, _partition_capped.name) == 1
