"""Unit tests for modified enqueue_batch with failure_policy and finalizer."""

from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel

from taskq import actor
from taskq.batch import BatchHandle, EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._handle import JobHandle
from taskq.client._jobs import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


class _FinalizerPayload(BaseModel):
    batch_id_str: str = ""


@actor(name="batch_policy_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


@actor(name="batch_finalizer_actor")
async def _finalizer_actor(_payload: _FinalizerPayload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend=backend, clock=FakeClock(start=_START))


def _make_item(value: int = 0) -> EnqueueItem:
    return EnqueueItem(actor_ref=_test_actor, payload=_Payload(value=value))


def _make_finalizer(batch_id: UUID) -> EnqueueItem:
    return EnqueueItem(
        actor_ref=_finalizer_actor,
        payload=_FinalizerPayload(batch_id_str=str(batch_id)),
    )


class TestBatchRowCreation:
    async def test_creates_batch_row_when_policy_set(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(5)]

        handle = await client.enqueue_batch(items, failure_policy=AbortBatchAfter(3))

        batch_row = backend._batches.get(handle.batch_id)
        assert batch_row is not None
        assert batch_row.failure_threshold == 3
        assert batch_row.expected_size == 5
        assert batch_row.status == "active"

    async def test_no_batch_row_when_policy_none(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(5)]

        handle = await client.enqueue_batch(items)

        assert handle.batch_id not in backend._batches

    async def test_originating_actor_is_none_for_direct_client(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(items, failure_policy=AbortBatchAfter(2))

        batch_row = backend._batches.get(handle.batch_id)
        assert batch_row is not None
        assert batch_row.originating_actor is None


class TestFinalizerEnqueue:
    async def test_finalizer_not_stamped_with_batch_id(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(
            items,
            finalizer=_make_finalizer(UUID(int=0)),
        )

        assert handle.finalizer_handle is not None
        fin_row = await backend.get(handle.finalizer_handle.job_id)
        assert fin_row is not None
        assert "batch_id" not in fin_row.metadata

    async def test_finalizer_handle_separate_from_children(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(4)]

        handle = await client.enqueue_batch(
            items,
            finalizer=_make_finalizer(UUID(int=0)),
        )

        assert handle.finalizer_handle is not None
        assert isinstance(handle.finalizer_handle, JobHandle)
        assert handle.size == 4
        assert len(handle.job_handles) == 5
        assert handle.job_handles[-1] is handle.finalizer_handle

    async def test_finalizer_job_id_on_batch_row(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(
            items,
            failure_policy=AbortBatchAfter(2),
            finalizer=_make_finalizer(UUID(int=0)),
        )

        batch_row = backend._batches.get(handle.batch_id)
        assert batch_row is not None
        assert batch_row.finalizer_job_id is not None
        assert handle.finalizer_handle is not None
        assert batch_row.finalizer_job_id == handle.finalizer_handle.job_id


class TestChildBatchIdMetadata:
    async def test_child_jobs_have_batch_id_in_metadata(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(
            items,
            failure_policy=AbortBatchAfter(2),
            finalizer=_make_finalizer(UUID(int=0)),
        )

        for i in range(3):
            h = handle.job_handles[i]
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.metadata.get("batch_id") == str(handle.batch_id)

    async def test_child_jobs_have_batch_id_without_policy_or_finalizer(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(items)

        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.metadata.get("batch_id") == str(handle.batch_id)


class TestBatchHandleWithFinalizer:
    async def test_handle_is_valid(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(
            items,
            finalizer=_make_finalizer(UUID(int=0)),
        )

        assert isinstance(handle, BatchHandle)
        assert handle.size == 3
        assert handle.finalizer_handle is not None
        assert handle.finalizer_handle.job_id is not None

    async def test_finalizer_without_policy_creates_batch_row(self) -> None:
        backend = _make_backend()
        client = _make_client(backend)
        items = [_make_item(i) for i in range(3)]

        handle = await client.enqueue_batch(
            items,
            finalizer=_make_finalizer(UUID(int=0)),
        )

        # C3: finalizer-only batches create a batch row (failure_threshold=None)
        # for list_batches discoverability and finalizer_job_id auto-exclusion.
        batch_row = backend._batches.get(handle.batch_id)
        assert batch_row is not None
        assert batch_row.failure_threshold is None
        assert batch_row.finalizer_job_id is not None
        assert batch_row.expected_size == 3
