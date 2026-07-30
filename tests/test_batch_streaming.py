"""Unit tests for enqueue_batch_streaming."""

from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.testing.clock import FakeClock
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
