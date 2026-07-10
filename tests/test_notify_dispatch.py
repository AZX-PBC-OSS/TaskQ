"""Integration tests for Postgres LISTEN/NOTIFY triggered dispatch (,).

Uses InMemoryBackend (which mirrors the wake subscriber pattern) to test
the end-to-end wake-up flow: enqueue → wake → dispatch, sweep → wake →
dispatch, poll fallback, eager re-check, and notify-disabled behavior.

Real Postgres trigger tests live in test_notify_dispatch_pg.py.
"""

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock
from uuid import uuid4

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args, make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_inmem_backend(clock: FakeClock | None = None) -> InMemoryBackend:
    if clock is None:
        clock = FakeClock(_START)
    return InMemoryBackend(clock=clock)


def _enqueue_args(
    payload: dict[str, object] | None = None,
    *,
    scheduled_at: datetime | None = None,
    queue: str = "default",
    actor: str = "test_actor",
) -> EnqueueArgs:
    return make_enqueue_args(
        payload=payload or {},
        scheduled_at=scheduled_at or _START,
        queue=queue,
        actor=actor,
    )


# ── Enqueue wakes subscriber ──────────────────────────────────────


class TestEnqueueWakesSubscriber:
    async def test_enqueue_sets_wake_event(self) -> None:
        """Enqueuing a pending job sets wake subscriber events."""
        backend = _make_inmem_backend()

        async with backend.subscribe_wake() as event:
            assert not event.is_set()
            await backend.enqueue(_enqueue_args())
            assert event.is_set()

    async def test_enqueue_wakes_multiple_subscribers(self) -> None:
        """Enqueue sets events for all registered wake subscribers."""
        backend = _make_inmem_backend()

        async with (
            backend.subscribe_wake() as event_a,
            backend.subscribe_wake() as event_b,
        ):
            await backend.enqueue(_enqueue_args())
            assert event_a.is_set()
            assert event_b.is_set()

    async def test_enqueue_does_not_wake_after_unsubscribe(self) -> None:
        """After unsubscribe_wake, enqueue does not set the old event."""
        backend = _make_inmem_backend()

        async with backend.subscribe_wake() as event_a:
            pass  # unsubscribe on exit

        assert event_a not in backend._wake_subscribers  # type: ignore[reportPrivateUsage]

        # A second subscriber should still receive notifications
        async with backend.subscribe_wake() as event_b:
            await backend.enqueue(_enqueue_args())
            assert event_b.is_set()
            assert not event_a.is_set()

    async def test_enqueue_scheduled_job_does_not_wake(self) -> None:
        """Enqueuing a scheduled (future) job should still set wake
        event — the InMemoryBackend fires on any enqueue regardless of status.
        This mirrors the PG behavior where pg_notify is fired on INSERT
        even when the trigger only fires on status='pending'.
        """
        backend = _make_inmem_backend()
        future = datetime(2026, 1, 1, tzinfo=UTC)

        async with backend.subscribe_wake() as event:
            args = _enqueue_args(scheduled_at=future)
            await backend.enqueue(args)
            # InMemoryBackend fires on all enqueues — matching PG behavior
            # where application-side pg_notify fires unconditionally.
            assert event.is_set()


# ── scheduled_to_pending sweep wakes subscriber ───────────────────


class TestSweepWakesSubscriber:
    async def test_scheduled_to_pending_wakes_subscriber(self) -> None:
        """When scheduled_to_pending promotes a job, wake subscribers are set."""
        backend = _make_inmem_backend()
        clock = backend._clock  # type: ignore[reportPrivateUsage]

        # Enqueue a job scheduled 5 minutes in the future
        future = _START + timedelta(minutes=5)
        await backend.enqueue(_enqueue_args(scheduled_at=future))

        # Verify it was enqueued as 'scheduled'
        # Advance clock past the scheduled time so sweep promotes it
        clock.advance(timedelta(minutes=10))  # pyright: ignore[reportAttributeAccessIssue] # Why: backend._clock is a FakeClock at runtime; Clock protocol does not expose advance().

        async with backend.subscribe_wake() as event:
            count = await backend.scheduled_to_pending(clock.now())
            assert count == 1
            assert event.is_set()

    async def test_scheduled_to_pending_no_rows_no_wake(self) -> None:
        """When scheduled_to_pending promotes zero jobs, no wake is fired."""
        backend = _make_inmem_backend()
        clock = backend._clock  # type: ignore[reportPrivateUsage]

        async with backend.subscribe_wake() as event:
            count = await backend.scheduled_to_pending(clock.now())
            assert count == 0
            assert not event.is_set()

    async def test_reclaim_expired_locks_wakes_subscriber(self) -> None:
        """When reclaim_expired_locks reclaims a job to pending, wake is fired."""
        backend = _make_inmem_backend()
        clock = backend._clock  # type: ignore[reportPrivateUsage]
        worker_id = uuid4()

        # Manually craft a running job with an expired lock
        job_id = new_job_id()
        expired_row = JobRow(
            id=job_id,
            actor="test_actor",
            queue="default",
            identity_key=None,
            fairness_key=None,
            tags=(),
            payload={},
            payload_schema_ver=1,
            status="running",
            priority=0,
            attempt=0,
            max_attempts=3,
            retry_kind="transient",
            schedule_to_close=None,
            start_to_close=None,
            heartbeat_timeout=None,
            created_at=_START,
            scheduled_at=_START,
            started_at=_START - timedelta(minutes=5),
            finished_at=None,
            last_heartbeat_at=None,
            locked_by_worker=worker_id,
            lock_expires_at=_START - timedelta(seconds=1),
            cancel_requested_at=None,
            cancel_phase=0,  # type: ignore[arg-type]
            error_class=None,
            error_message=None,
            error_traceback=None,
            progress_state={},
            progress_seq=0,
            result=None,
            result_size_bytes=None,
            result_expires_at=None,
            idempotency_key=None,
            trace_id=None,
            span_id=None,
            metadata={},
        )
        backend._jobs[job_id] = expired_row  # type: ignore[reportPrivateUsage]

        async with backend.subscribe_wake() as event:
            count = await backend.reclaim_expired_locks(
                clock.now(),
                cancel_grace=timedelta(seconds=30),
                cleanup_grace=timedelta(seconds=30),
            )
            assert count == 1
            assert event.is_set()

    async def test_reclaim_expired_locks_no_pending_no_wake(self) -> None:
        """When reclaim_expired_locks transitions to crashed (not pending),
        no wake is needed because the job is terminal."""
        backend = _make_inmem_backend()
        clock = backend._clock  # type: ignore[reportPrivateUsage]
        worker_id = uuid4()

        # Running job with retries left but non-retryable → will go to crashed
        # condition: row.attempt < row.max_attempts AND row.retry_kind != "non_retryable"
        # attempt=0 < max_attempts=1 is True, but non_retryable → False → crashed
        job_id = new_job_id()
        expired_row = JobRow(
            id=job_id,
            actor="test_actor",
            queue="default",
            identity_key=None,
            fairness_key=None,
            tags=(),
            payload={},
            payload_schema_ver=1,
            status="running",
            priority=0,
            attempt=0,
            max_attempts=1,
            retry_kind="non_retryable",  # type: ignore[arg-type]
            schedule_to_close=None,
            start_to_close=None,
            heartbeat_timeout=None,
            created_at=_START,
            scheduled_at=_START,
            started_at=_START - timedelta(minutes=5),
            finished_at=None,
            last_heartbeat_at=None,
            locked_by_worker=worker_id,
            lock_expires_at=_START - timedelta(seconds=1),
            cancel_requested_at=None,
            cancel_phase=0,  # type: ignore[arg-type]
            error_class=None,
            error_message=None,
            error_traceback=None,
            progress_state={},
            progress_seq=0,
            result=None,
            result_size_bytes=None,
            result_expires_at=None,
            idempotency_key=None,
            trace_id=None,
            span_id=None,
            metadata={},
        )
        backend._jobs[job_id] = expired_row  # type: ignore[reportPrivateUsage]

        async with backend.subscribe_wake() as event:
            count = await backend.reclaim_expired_locks(
                clock.now(),
                cancel_grace=timedelta(seconds=30),
                cleanup_grace=timedelta(seconds=30),
            )
            assert count == 1
            # Goes to crashed, not pending — no wake needed
            assert not event.is_set()
            assert backend._jobs[job_id].status == "crashed"  # type: ignore[reportPrivateUsage]


# ── Poll fallback works ───────────────────────────────────────────


class TestPollFallback:
    async def test_producer_sleeps_on_poll_interval_when_no_wake(self) -> None:
        """When no wake event fires and no jobs are available, the producer
        sleeps for poll_interval seconds."""
        from unittest.mock import AsyncMock, Mock

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
                "TASKQ_SCHEMA_NAME": "taskq_test",
                "TASKQ_POLL_INTERVAL": "0.1",
                "TASKQ_NOTIFY_ENABLED": "false",
                "TASKQ_NOTIFY_POLL_INTERVAL": "0.5",
            }
        )

        deps = Mock()
        deps.settings = settings
        deps.worker_pool = Mock()
        deps.dispatcher_pool = Mock()
        deps.notify_conn = Mock()

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=deps,
            clock=mock_clock,
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        )

        from taskq.worker.run import producer_loop

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
        shutdown = asyncio.Event()
        stop_event = asyncio.Event()

        # Mock dispatch_batch to return empty
        backend.dispatch_batch = AsyncMock(return_value=[])  # type: ignore[method-assign]

        async def run_and_stop() -> None:
            await asyncio.sleep(0.15)
            stop_event.set()

        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown,
                stop_event,
                backend=backend,
                worker_id=uuid4(),
            )
        )
        await asyncio.wait_for(run_and_stop(), timeout=2.0)
        await asyncio.wait_for(task, timeout=2.0)

        # dispatch_batch should have been called
        assert backend.dispatch_batch.call_count >= 1  # type: ignore[reportOptionalMemberAccess]

    async def test_producer_uses_notify_poll_interval_when_enabled(self) -> None:
        """When notify_enabled=True, the producer uses notify_poll_interval
        as the fallback poll cadence."""
        from unittest.mock import AsyncMock, Mock

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
                "TASKQ_SCHEMA_NAME": "taskq_test",
                "TASKQ_POLL_INTERVAL": "0.05",
                "TASKQ_NOTIFY_ENABLED": "true",
                "TASKQ_NOTIFY_POLL_INTERVAL": "0.5",
            }
        )

        deps = Mock()
        deps.settings = settings
        deps.worker_pool = Mock()
        deps.dispatcher_pool = Mock()
        deps.notify_conn = Mock()

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=deps,
            clock=mock_clock,
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        )

        from taskq.worker.run import producer_loop

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
        shutdown = asyncio.Event()
        stop_event = asyncio.Event()

        backend.dispatch_batch = AsyncMock(return_value=[])  # type: ignore[method-assign]

        async def run_and_stop() -> None:
            await asyncio.sleep(0.15)
            stop_event.set()

        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown,
                stop_event,
                backend=backend,
                worker_id=uuid4(),
            )
        )
        await asyncio.wait_for(run_and_stop(), timeout=2.0)
        await asyncio.wait_for(task, timeout=2.0)

        assert backend.dispatch_batch.call_count >= 1  # type: ignore[reportOptionalMemberAccess]


# ── Eager re-check optimization ────────────────────────────────────


class TestEagerRecheck:
    async def test_eager_recheck_after_empty_dispatch(self) -> None:
        """After dispatch_batch returns 0 jobs but a consumer freed a slot
        during dispatch, the producer re-checks before sleeping."""
        from unittest.mock import Mock

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
                "TASKQ_SCHEMA_NAME": "taskq_test",
                "TASKQ_POLL_INTERVAL": "0.5",
                "TASKQ_NOTIFY_ENABLED": "false",
            }
        )

        deps = Mock()
        deps.settings = settings
        deps.worker_pool = Mock()
        deps.dispatcher_pool = Mock()
        deps.notify_conn = Mock()

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=deps,
            clock=mock_clock,
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        )

        from taskq.worker.run import producer_loop

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=2)
        shutdown = asyncio.Event()
        stop_event = asyncio.Event()

        # First call returns 0, eager re-check returns 1 job.
        # Subsequent calls return 0 (correct — the eager re-check is a one-off).
        call_count = 0
        jobs_remaining = 1

        async def mock_dispatch(*args: object, **kwargs: object) -> list[JobRow]:
            nonlocal call_count, jobs_remaining
            call_count += 1
            if jobs_remaining > 0:
                jobs_remaining -= 1
                return [make_job_row(status="pending")]
            return []

        backend.dispatch_batch = mock_dispatch  # type: ignore[method-assign]

        async def run_and_stop() -> None:
            for _ in range(50):
                if call_count >= 2:
                    break
                await asyncio.sleep(0.02)
            stop_event.set()

        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown,
                stop_event,
                backend=backend,
                worker_id=uuid4(),
            )
        )
        await asyncio.wait_for(run_and_stop(), timeout=2.0)
        await asyncio.wait_for(task, timeout=2.0)

        assert call_count >= 2
        assert local_queue.qsize() == 1

    async def test_eager_recheck_no_loop_when_queue_full(self) -> None:
        """When the queue is full, the eager re-check does not cause
        an infinite loop — it falls through to sleep."""
        from unittest.mock import Mock

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
                "TASKQ_SCHEMA_NAME": "taskq_test",
                "TASKQ_POLL_INTERVAL": "0.05",
                "TASKQ_NOTIFY_ENABLED": "false",
            }
        )

        deps = Mock()
        deps.settings = settings
        deps.worker_pool = Mock()
        deps.dispatcher_pool = Mock()
        deps.notify_conn = Mock()

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=deps,
            clock=mock_clock,
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        )

        from taskq.worker.run import producer_loop

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
        # Fill the queue
        await local_queue.put(make_job_row(status="pending"))

        shutdown = asyncio.Event()
        stop_event = asyncio.Event()

        call_count = 0

        async def mock_dispatch(*args: object, **kwargs: object) -> list[JobRow]:
            nonlocal call_count
            call_count += 1
            return []

        backend.dispatch_batch = mock_dispatch  # type: ignore[method-assign]

        async def run_and_stop() -> None:
            await asyncio.sleep(0.15)
            stop_event.set()

        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown,
                stop_event,
                backend=backend,
                worker_id=uuid4(),
            )
        )
        await asyncio.wait_for(run_and_stop(), timeout=2.0)
        await asyncio.wait_for(task, timeout=2.0)

        # When queue is full, dispatch is NOT called
        assert call_count == 0


# ── Notify-disabled behavior ───────────────────────────────────────


class TestNotifyDisabled:
    async def test_notify_disabled_no_wake_subscribe(self) -> None:
        """When notify_enabled=False, the producer loop does NOT call
        subscribe_wake, using poll_interval exclusively."""
        from unittest.mock import AsyncMock, Mock

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
                "TASKQ_SCHEMA_NAME": "taskq_test",
                "TASKQ_POLL_INTERVAL": "0.05",
                "TASKQ_NOTIFY_ENABLED": "false",
            }
        )

        deps = Mock()
        deps.settings = settings
        deps.worker_pool = Mock()
        deps.dispatcher_pool = Mock()
        deps.notify_conn = Mock()

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=deps,
            clock=mock_clock,
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        )

        # Patch subscribe_wake to track calls
        subscribe_called = False

        def track_subscribe() -> asyncio.Event:
            nonlocal subscribe_called
            subscribe_called = True
            return asyncio.Event()

        backend.subscribe_wake = track_subscribe  # type: ignore[method-assign]
        backend.dispatch_batch = AsyncMock(return_value=[])  # type: ignore[method-assign]

        from taskq.worker.run import producer_loop

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
        shutdown = asyncio.Event()
        stop_event = asyncio.Event()

        async def run_and_stop() -> None:
            await asyncio.sleep(0.15)
            stop_event.set()

        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown,
                stop_event,
                backend=backend,
                worker_id=uuid4(),
            )
        )
        await asyncio.wait_for(run_and_stop(), timeout=2.0)
        await asyncio.wait_for(task, timeout=2.0)

        # subscribe_wake should NOT have been called
        assert not subscribe_called


# ── Wake event lifecycle ───────────────────────────────────────────


class TestWakeEventLifecycle:
    async def test_producer_clears_wake_event_after_dispatch(self) -> None:
        """After dispatch_batch finds jobs, the producer clears the wake
        event so the next NOTIFY re-sets it."""
        from unittest.mock import Mock

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
                "TASKQ_SCHEMA_NAME": "taskq_test",
                "TASKQ_POLL_INTERVAL": "0.5",
                "TASKQ_NOTIFY_ENABLED": "true",
            }
        )

        deps = Mock()
        deps.settings = settings
        deps.worker_pool = Mock()
        deps.dispatcher_pool = Mock()
        deps.notify_conn = Mock()

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=deps,
            clock=mock_clock,
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=30),
        )

        from taskq.worker.run import producer_loop

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=8)
        shutdown = asyncio.Event()
        stop_event = asyncio.Event()

        jobs_remaining = 1

        async def mock_dispatch(*args: object, **kwargs: object) -> list[JobRow]:
            nonlocal jobs_remaining
            if jobs_remaining > 0:
                jobs_remaining -= 1
                return [make_job_row(status="pending")]
            return []

        backend.dispatch_batch = mock_dispatch  # type: ignore[method-assign]

        async def run_and_stop() -> None:
            await asyncio.sleep(0.05)
            stop_event.set()

        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown,
                stop_event,
                backend=backend,
                worker_id=uuid4(),
            )
        )
        await asyncio.wait_for(run_and_stop(), timeout=2.0)
        await asyncio.wait_for(task, timeout=2.0)

        # Job was dispatched to the local queue
        assert local_queue.qsize() == 1


# ── PostgresBackend subscribe_wake with mock ──────────────────────────────


@pytest.fixture
def mock_deps() -> tuple[Mock, PostgresBackend]:
    from unittest.mock import Mock

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://localhost:5432/taskq",
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_NOTIFY_ENABLED": "true",
            "TASKQ_NOTIFY_POLL_INTERVAL": "0.5",
        }
    )

    deps = Mock()
    deps.settings = settings
    deps.worker_pool = Mock()
    deps.dispatcher_pool = Mock()
    deps.notify_conn = Mock()

    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0

    backend = PostgresBackend(
        deps=deps,
        clock=mock_clock,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )

    return deps, backend


async def test_producer_uses_notify_poll_with_wake_subscribe(
    mock_deps: tuple[Mock, PostgresBackend],
) -> None:
    """With notify_enabled=True, the producer subscribes to wake events."""
    from unittest.mock import AsyncMock

    from taskq.worker.run import producer_loop

    deps, backend = mock_deps
    backend.dispatch_batch = AsyncMock(return_value=[])  # type: ignore[method-assign]

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)
    shutdown = asyncio.Event()
    stop_event = asyncio.Event()

    subscribe_called = False

    def tracking_subscribe() -> contextlib.AbstractAsyncContextManager[asyncio.Event]:
        class _Ctx:
            async def __aenter__(self) -> asyncio.Event:
                nonlocal subscribe_called
                subscribe_called = True
                return asyncio.Event()

            async def __aexit__(self, *exc: object) -> None:
                pass

        return _Ctx()

    backend.subscribe_wake = tracking_subscribe  # type: ignore[method-assign]

    async def run_and_stop() -> None:
        await asyncio.sleep(0.05)
        stop_event.set()

    task = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown,
            stop_event,
            backend=backend,
            worker_id=uuid4(),
        )
    )
    await asyncio.wait_for(run_and_stop(), timeout=2.0)
    await asyncio.wait_for(task, timeout=2.0)

    assert subscribe_called, "subscribe_wake should be called when notify_enabled=True"
