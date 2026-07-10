"""Tests for JobHandle public API.

Covers:
- JobHandle.wait polls until terminal (using in-memory backend,
  enqueue + run_until_drained, then wait).
- JobHandle.wait(timeout=0.1) on a job that never terminates raises
  asyncio.TimeoutError.
- JobHandle.attempts returns the attempt rows in order.
- JobHandle.progress_stream raises NotImplementedError referencing Redis pub/sub bridge.
- JobHandle.cancel round-trips through JobsClient.cancel and returns
  a CancelResult.
- Construct with backend only: wait() works, read-back methods raise RuntimeError.
- Construct with client only: _backend is filled from client.backend.
- Construct with neither raises ValueError.
- status/refresh/attempts/cancel raise RuntimeError without client.
"""

import asyncio
import contextlib
from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter

from taskq.backend._protocol import EnqueueArgs
from taskq.client import CancelResult, JobHandle, JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_RA: TypeAdapter[None] = TypeAdapter(type(None))


def _make_client() -> tuple[InMemoryBackend, JobsClient]:
    backend = InMemoryBackend(clock=FakeClock(_START))
    client = JobsClient(backend)
    return backend, client


async def _enqueue(
    backend: InMemoryBackend, client: JobsClient, args: EnqueueArgs
) -> JobHandle[None]:
    """Enqueue via the backend and return a typed JobHandle."""
    row = await backend.enqueue(args)
    return JobHandle(client=client, row=row, result_adapter=_RA, was_existing=False)


# ── wait polls until terminal ──────────────────────────────────────────


class TestWait:
    """JobHandle.wait polls until the job reaches a terminal status."""

    async def test_wait_returns_when_terminal(self) -> None:
        """Enqueue a job, dispatch via run_until_drained, then call wait
        and assert it returns the final JobRow.
        """
        backend, client = _make_client()

        # Register a stub that succeeds immediately (returns None to match JobHandle[None])
        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        # The job is pending; run_until_drained will dispatch and complete it
        await backend.run_until_drained()

        # wait should return immediately since the job is already terminal
        await handle.wait(timeout=2.0)
        row = await handle.refresh()
        assert row.status in ("succeeded", "failed", "cancelled", "abandoned")

    async def test_wait_timeout_raises(self) -> None:
        """wait(timeout=0.1) on a job that never terminates raises
        asyncio.TimeoutError.
        """
        backend, client = _make_client()

        # Register a stub that sleeps forever
        async def _hang(payload: object, ctx: object) -> None:
            await asyncio.sleep(100)

        backend.register_stub("test_actor", _hang)

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        # Start dispatch in background
        async def _drain() -> None:
            await backend.run_until_drained()

        drain_task = asyncio.create_task(_drain())

        # Give the dispatch a moment to start
        await asyncio.sleep(0.05)

        with pytest.raises(asyncio.TimeoutError):
            await handle.wait(timeout=0.1)

        drain_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await drain_task

    async def test_wait_no_timeout_on_terminal_job(self) -> None:
        """wait with no timeout on an already-terminal job returns
        immediately.
        """
        backend, client = _make_client()

        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)
        await backend.run_until_drained()

        await handle.wait()
        row = await handle.refresh()
        assert row.status == "succeeded"


# ── attempts ───────────────────────────────────────────────────────────


class TestAttempts:
    """JobHandle.attempts returns the attempt rows in order."""

    async def test_attempts_returns_attempt_rows(self) -> None:
        backend, client = _make_client()

        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)
        await backend.run_until_drained()

        attempts = await handle.attempts()
        assert len(attempts) >= 1
        assert attempts[0].job_id == handle.job_id
        # Attempt rows should be ordered by attempt number
        for i, att in enumerate(attempts):
            assert att.attempt == i + 1


# ── progress_stream ────────────────────────────────────────────────────


class TestProgressStream:
    """JobHandle.progress_stream raises NotImplementedError referencing Redis pub/sub bridge."""

    async def test_progress_stream_raises_not_implemented(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        with pytest.raises(NotImplementedError, match="in-memory backend"):
            async for _ in handle.progress_stream():
                pass


# ── cancel ─────────────────────────────────────────────────────────────


class TestCancel:
    """JobHandle.cancel round-trips through JobsClient.cancel and returns
    a CancelResult.
    """

    async def test_cancel_returns_cancel_result(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        result = await handle.cancel(reason="test cancel")

        assert isinstance(result, CancelResult)
        assert result.job_id == handle.job_id
        assert result.cancellation_initiated is True
        assert result.previous_status == "pending"
        assert result.new_status == "cancelled"

    async def test_cancel_with_no_reason(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        result = await handle.cancel()

        assert isinstance(result, CancelResult)
        assert result.cancellation_initiated is True


# ── status ─────────────────────────────────────────────────────────────


class TestStatus:
    """JobHandle.status returns the current (live) status."""

    async def test_status_is_pending_after_enqueue(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        status = await handle.status()
        assert status == "pending"

    async def test_status_changes_after_run(self) -> None:
        backend, client = _make_client()

        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)
        await backend.run_until_drained()

        status = await handle.status()
        assert status == "succeeded"


# ── wait raises KeyError when job disappears ───────────────────────────


class TestWaitDeletedJob:
    """wait() raises KeyError when the job row is deleted mid-poll,
    consistent with status() which raises KeyError on a None get.
    """

    async def test_wait_raises_key_error_on_deleted_job(self) -> None:
        """If the job row is deleted (prune / out-of-band tooling),
        wait() should raise KeyError(job_id) rather than silently
        looping until timeout.
        """
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        # Simulate out-of-band deletion (prune path)
        del backend._jobs[handle.job_id]  # type: ignore[reportPrivateUsage] # Why: test-only simulation of a deleted row; InMemoryBackend has no public delete method

        with pytest.raises(KeyError, match=str(handle.job_id)):
            await handle.wait(timeout=2.0)


# ── properties ─────────────────────────────────────────────────────────


class TestProperties:
    """JobHandle properties expose the JobRow fields."""

    async def test_job_id_property(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        assert handle.job_id == args.id

    async def test_actor_name_property(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(actor="my_actor", scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        assert handle.actor_name == "my_actor"

    async def test_queue_property(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(queue="priority", scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        assert handle.queue == "priority"


# ── was_existing ────────────────────────────────────────────────────────


class TestWasExisting:
    """was_existing field on JobHandle."""

    async def test_job_handle_default_was_existing_false(self) -> None:
        """Handle constructed without was_existing has was_existing == False."""
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(client=client, row=row, result_adapter=_RA, was_existing=False)

        assert handle.was_existing is False

    async def test_job_handle_was_existing_true_round_trips(self) -> None:
        """Explicit True is preserved."""
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(client=client, row=row, result_adapter=_RA, was_existing=True)

        assert handle.was_existing is True

    async def test_jobs_client_get_returns_was_existing_false(self) -> None:
        """JobsClient.get always returns a handle with was_existing == False."""
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        await backend.enqueue(args)
        handle = await client.get(args.id, result_adapter=_RA)

        assert handle is not None
        assert handle.was_existing is False


# ── Construct with backend only ──────────────────────────────────────


class TestBackendOnlyConstruction:
    """JobHandle(client=None, backend=...) supports wait() but not
    read-back methods ().
    """

    async def test_backend_only_stores_backend(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        assert handle._backend is backend

    async def test_backend_only_wait_resolves(self) -> None:
        backend, _client = _make_client()

        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        await backend.run_until_drained()
        await handle.wait(timeout=2.0)

    async def test_backend_only_status_raises_runtime_error(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        with pytest.raises(RuntimeError, match=r"JobHandle\.status\(\) requires a JobsClient"):
            await handle.status()

    async def test_backend_only_refresh_raises_runtime_error(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        with pytest.raises(RuntimeError, match=r"JobHandle\.refresh\(\) requires a JobsClient"):
            await handle.refresh()

    async def test_backend_only_attempts_raises_runtime_error(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        with pytest.raises(RuntimeError, match=r"JobHandle\.attempts\(\) requires a JobsClient"):
            await handle.attempts()

    async def test_backend_only_cancel_raises_runtime_error(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        with pytest.raises(RuntimeError, match=r"JobHandle\.cancel\(\) requires a JobsClient"):
            await handle.cancel()

    async def test_runtime_error_message_contains_guidance(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, backend=backend)

        with pytest.raises(RuntimeError, match=r"ctx\.jobs\.enqueue"):
            await handle.status()


# ── Construct with client only (back-compat) ──────────────────────────


class TestClientOnlyConstruction:
    """JobHandle(client=..., backend=None) fills _backend from
    client.backend — existing behavior preserved.
    """

    async def test_client_only_fills_backend(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(row=row, result_adapter=_RA, was_existing=False, client=client)

        assert handle._backend is backend

    async def test_client_only_wait_works(self) -> None:
        backend, client = _make_client()

        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)
        await backend.run_until_drained()

        await handle.wait(timeout=2.0)
        row = await handle.refresh()
        assert row.status == "succeeded"


# ── Construct with neither raises ValueError ──────────────────────────


class TestNeitherClientNorBackend:
    """JobHandle(row=..., result_adapter=..., was_existing=...) with no
    client or backend raises ValueError.
    """

    async def test_raises_value_error(self) -> None:
        backend, _client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        with pytest.raises(ValueError, match="at least one of client= or backend="):
            JobHandle(row=row, result_adapter=_RA, was_existing=False)
