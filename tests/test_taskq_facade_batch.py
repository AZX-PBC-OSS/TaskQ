"""Unit tests for TaskQ facade batch forwarding (review MEDIUM-2).

Uses a mock backend to verify the facade threads failure_policy and
finalizer through to JobsClient, without requiring a live Postgres.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

from pydantic import BaseModel

from taskq import actor
from taskq.backend._protocol import BatchFilter
from taskq.batch import BatchCompletionStatus, BatchHandle, BatchSummary, EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.client._taskq import TaskQ

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


class _FinalizerPayload(BaseModel):
    batch_id_str: str = ""


@actor(name="facade_batch_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


@actor(name="facade_batch_finalizer_actor")
async def _finalizer_actor(_payload: _FinalizerPayload) -> None:
    pass


def _make_item(value: int = 0) -> EnqueueItem:
    return EnqueueItem(actor_ref=_test_actor, payload=_Payload(value=value))


def _make_finalizer(batch_id: UUID) -> EnqueueItem:
    return EnqueueItem(
        actor_ref=_finalizer_actor,
        payload=_FinalizerPayload(batch_id_str=str(batch_id)),
    )


def _make_mock_taskq() -> tuple[TaskQ, AsyncMock]:
    """Build a TaskQ with a mocked JobsClient to capture forwarded args."""
    mock_jobs = MagicMock(spec=JobsClient)
    mock_jobs.enqueue_batch = AsyncMock()
    mock_jobs.enqueue_batch_streaming = AsyncMock()
    mock_jobs.list_batches = AsyncMock()
    mock_jobs.close = AsyncMock()

    tq = object.__new__(TaskQ)
    tq._client = mock_jobs  # pyright: ignore[reportAttributeAccessIssue]
    return tq, mock_jobs


class TestFacadeEnqueueBatchForwarding:
    async def test_facade_enqueue_batch_with_failure_policy(self) -> None:
        tq, mock_jobs = _make_mock_taskq()
        items = [_make_item(i) for i in range(5)]
        policy = AbortBatchAfter(3)
        expected_handle = BatchHandle(batch_id=uuid4(), job_handles=[], size=5)
        mock_jobs.enqueue_batch.return_value = expected_handle

        result = await tq.enqueue_batch(items, failure_policy=policy)

        mock_jobs.enqueue_batch.assert_awaited_once()
        call_kwargs = mock_jobs.enqueue_batch.call_args
        assert call_kwargs.kwargs.get("failure_policy") is policy
        assert result is expected_handle

    async def test_facade_enqueue_batch_with_finalizer(self) -> None:
        tq, mock_jobs = _make_mock_taskq()
        items = [_make_item(i) for i in range(3)]
        finalizer = _make_finalizer(uuid4())
        expected_handle = BatchHandle(batch_id=uuid4(), job_handles=[], size=3)
        mock_jobs.enqueue_batch.return_value = expected_handle

        result = await tq.enqueue_batch(items, finalizer=finalizer)

        mock_jobs.enqueue_batch.assert_awaited_once()
        call_kwargs = mock_jobs.enqueue_batch.call_args
        assert call_kwargs.kwargs.get("finalizer") is finalizer
        assert result is expected_handle

    async def test_facade_enqueue_batch_with_both(self) -> None:
        tq, mock_jobs = _make_mock_taskq()
        items = [_make_item(i) for i in range(3)]
        policy = AbortBatchAfter(2)
        finalizer = _make_finalizer(uuid4())
        expected_handle = BatchHandle(
            batch_id=uuid4(),
            job_handles=[],
            size=3,
            finalizer_handle=MagicMock(),
        )
        mock_jobs.enqueue_batch.return_value = expected_handle

        result = await tq.enqueue_batch(items, failure_policy=policy, finalizer=finalizer)

        mock_jobs.enqueue_batch.assert_awaited_once()
        call_kwargs = mock_jobs.enqueue_batch.call_args
        assert call_kwargs.kwargs.get("failure_policy") is policy
        assert call_kwargs.kwargs.get("finalizer") is finalizer
        assert result is expected_handle


class TestFacadeEnqueueBatchStreaming:
    async def test_facade_enqueue_batch_streaming(self) -> None:
        tq, mock_jobs = _make_mock_taskq()

        def gen() -> Any:
            yield _make_item(0)
            yield _make_item(1)

        expected_handle = BatchHandle(batch_id=uuid4(), job_handles=[], size=2)
        mock_jobs.enqueue_batch_streaming.return_value = expected_handle

        result = await tq.enqueue_batch_streaming(gen(), chunk_size=500)

        mock_jobs.enqueue_batch_streaming.assert_awaited_once()
        call_kwargs = mock_jobs.enqueue_batch_streaming.call_args
        assert call_kwargs.kwargs.get("chunk_size") == 500
        assert result is expected_handle


class TestFacadeListBatches:
    async def test_facade_list_batches(self) -> None:
        tq, mock_jobs = _make_mock_taskq()
        bid = uuid4()
        mock_jobs.list_batches.return_value = [
            BatchSummary(
                batch_id=bid,
                queue="default",
                status="active",
                expected_size=5,
                consecutive_failures=0,
                failure_threshold=3,
                finalizer_job_id=None,
                originating_actor=None,
                created_at=_START,
                completed_at=None,
                completion=BatchCompletionStatus(
                    total=5, pending=3, succeeded=2, failed=0, cancelled=0, crashed=0, abandoned=0
                ),
            )
        ]

        filt = BatchFilter(queue="default")
        results = await tq.list_batches(filt)

        mock_jobs.list_batches.assert_awaited_once_with(filt)
        assert len(results) == 1
        assert results[0].batch_id == bid
