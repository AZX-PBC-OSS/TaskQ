"""Integration and unit tests for the :class:`~taskq.client.TaskQ` top-level client.

Covers lifecycle (open/close/context-manager), constructor validation, and all
public job operations (enqueue, get, list, cancel, stream) against a real
Postgres backend.

Test plan IDs map to the spec in the task description:
- Lifecycle: open/close patterns, guard clauses, pool-ownership semantics.
- Enqueue: JobHandle shape, idempotency, scheduled_at status.
- Get: hit and miss.
- List: queue / status / actor filters.
- Cancel: pending job and unknown id.
- Stream: NotImplementedError stub.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel, TypeAdapter

from taskq import TaskQ, actor
from taskq._ids import new_base62, new_job_id
from taskq.backend._protocol import Backend, JobFilter, JobId, JobRow
from taskq.client._handle import JobHandle
from taskq.client._taskq import JobEvent, _stream_pg, _stream_redis
from taskq.migrate import apply_pending
from taskq.types import CancelResult

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = f"ttc_{new_base62()}".lower()

# ---------------------------------------------------------------------------
# Shared test actor
# ---------------------------------------------------------------------------


class _Payload(BaseModel):
    value: int = 1


@actor(name="tq_client_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


# Result adapter for void actors
_RA: TypeAdapter[None] = TypeAdapter(type(None))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _migrate(dsn: str, schema: str = _SCHEMA_LABEL) -> None:
    """Drop the test schema and apply all migrations.

    pg_conn fixture drops the schema but does NOT recreate it — migrations
    must be applied before TaskQ can use the schema.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# TestLifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    """TaskQ lifecycle: open/close patterns and guard clauses."""

    async def test_async_with_opens_and_closes_cleanly(self, pg_dsn: str) -> None:
        """async with TaskQ(dsn=...) opens cleanly and closes without error."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            # If we get here the pool is open and the client is live.
            assert tq is not None

    async def test_explicit_open_close(self, pg_dsn: str) -> None:
        """await tq.open() + await tq.close() works (FastAPI lifespan pattern)."""
        await _migrate(pg_dsn)
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.open()
        try:
            handle = await tq.enqueue(_test_actor, _Payload(value=7))
            assert isinstance(handle, JobHandle)
        finally:
            await tq.close()

    async def test_open_twice_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling open() on an already-open TaskQ raises RuntimeError."""
        await _migrate(pg_dsn)
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.open()
        try:
            with pytest.raises(RuntimeError, match="already open"):
                await tq.open()
        finally:
            await tq.close()

    async def test_job_method_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling enqueue before open() raises RuntimeError referencing tq.open()."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.enqueue(_test_actor, _Payload())

    async def test_get_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling get() before open() raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.get(new_job_id(), result_adapter=_RA)

    async def test_list_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling list() before open() raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.list(JobFilter())

    async def test_cancel_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling cancel() before open() raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.cancel(new_job_id())

    async def test_close_when_already_closed_is_noop(self, pg_dsn: str) -> None:
        """close() on a closed (never opened) TaskQ is a no-op — does not raise."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.close()  # must not raise

    async def test_close_twice_is_noop(self, pg_dsn: str) -> None:
        """Calling close() twice does not raise."""
        await _migrate(pg_dsn)
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.open()
        await tq.close()
        await tq.close()  # second close is a no-op

    def test_no_dsn_no_pool_raises_value_error(self) -> None:
        """TaskQ() with neither dsn nor pool raises ValueError at construction."""
        with pytest.raises(ValueError, match=r"dsn.*pool|pool.*dsn"):
            TaskQ()

    def test_both_dsn_and_pool_raises_value_error(self, pg_dsn: str) -> None:
        """TaskQ(dsn=..., pool=...) raises ValueError — they are mutually exclusive."""
        # We only need a pool object for the constructor check; we never open it.
        import unittest.mock as mock

        fake_pool = mock.MagicMock(spec=asyncpg.Pool)
        with pytest.raises(ValueError, match=r"dsn.*pool|pool.*dsn"):
            TaskQ(dsn=pg_dsn, pool=fake_pool)

    def test_both_redis_url_and_redis_client_raises_value_error(self, pg_dsn: str) -> None:
        """TaskQ(redis_url=..., redis_client=...) raises ValueError —
        they are mutually exclusive.
        """
        fake_redis = MagicMock()
        with pytest.raises(ValueError, match=r"redis_url.*redis_client|redis_client.*redis_url"):
            TaskQ(dsn=pg_dsn, redis_url="redis://localhost:6379/0", redis_client=fake_redis)

    def test_poll_timeout_stored(self, pg_dsn: str) -> None:
        """TaskQ(poll_timeout=5.0) stores the value as _poll_timeout."""
        tq = TaskQ(dsn=pg_dsn, poll_timeout=5.0)
        assert tq._poll_timeout == 5.0

    def test_poll_timeout_default(self, pg_dsn: str) -> None:
        """TaskQ() without poll_timeout defaults _poll_timeout to 30.0."""
        tq = TaskQ(dsn=pg_dsn)
        assert tq._poll_timeout == 30.0

    def test_dsn_none_when_pool_supplied(self) -> None:
        """When TaskQ is constructed with pool= (no dsn), _dsn remains None."""
        fake_pool = MagicMock(spec=asyncpg.Pool)
        tq = TaskQ(pool=fake_pool)
        assert tq._dsn is None

    async def test_caller_owned_pool_not_closed_by_taskq(self, pg_dsn: str) -> None:
        """When TaskQ is constructed with a caller-owned pool, close() does not
        close that pool — it remains usable after TaskQ.close().
        """
        await _migrate(pg_dsn)
        # Open a pool that the caller owns.
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            async with TaskQ(pool=pool, schema=_SCHEMA_LABEL):
                pass  # opens and closes TaskQ but must NOT close the pool

            # Pool should still be usable after TaskQ closed.
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
            assert result == 1
        finally:
            await pool.close()


# ---------------------------------------------------------------------------
# TestEnqueue
# ---------------------------------------------------------------------------


class TestEnqueue:
    """TaskQ.enqueue public-behaviour tests."""

    async def test_enqueue_returns_job_handle(self, pg_dsn: str) -> None:
        """enqueue returns a JobHandle with a valid UUID job_id."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=1))

        assert isinstance(handle, JobHandle)
        assert isinstance(handle.job_id, UUID)

    async def test_enqueue_fresh_was_existing_false(self, pg_dsn: str) -> None:
        """A fresh enqueue has was_existing == False."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=2))

        assert handle.was_existing is False

    async def test_enqueue_idempotency_key_dedup_same_job_id(self, pg_dsn: str) -> None:
        """Two enqueues with the same idempotency_key return the same job_id."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle1 = await tq.enqueue(
                _test_actor, _Payload(value=1), idempotency_key="tq-client-idem-1"
            )
            handle2 = await tq.enqueue(
                _test_actor, _Payload(value=9), idempotency_key="tq-client-idem-1"
            )

        assert handle1.job_id == handle2.job_id

    async def test_enqueue_idempotency_key_second_was_existing_true(self, pg_dsn: str) -> None:
        """The second enqueue with the same idempotency_key returns was_existing == True."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            await tq.enqueue(_test_actor, _Payload(value=1), idempotency_key="tq-client-idem-2")
            handle2 = await tq.enqueue(
                _test_actor, _Payload(value=99), idempotency_key="tq-client-idem-2"
            )

        assert handle2.was_existing is True

    async def test_enqueue_without_scheduled_at_status_is_pending(self, pg_dsn: str) -> None:
        """Enqueueing without scheduled_at results in a job with status='pending'."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=3))
            fetched = await tq.get(handle.job_id, result_adapter=_RA)

        assert fetched is not None
        assert fetched._row.status == "pending"

    async def test_enqueue_with_future_scheduled_at_stored_at_correct_time(
        self, pg_dsn: str
    ) -> None:
        """Enqueueing with a future scheduled_at stores the correct timestamp.

        The PG backend always inserts with status='pending' (no trigger flips
        it to 'scheduled' at insert time — that transition is done by the
        scheduled-to-pending sweep at dispatch). What we can assert is that the
        stored scheduled_at matches the value we supplied.
        """
        await _migrate(pg_dsn)
        future = datetime.now(UTC) + timedelta(hours=1)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=4), scheduled_at=future)
            fetched = await tq.get(handle.job_id, result_adapter=_RA)

        assert fetched is not None
        # scheduled_at is stored faithfully (within sub-second PG rounding).
        stored_at = fetched._row.scheduled_at
        assert stored_at is not None
        diff = abs((stored_at - future).total_seconds())
        assert diff < 1.0


# ---------------------------------------------------------------------------
# TestGet
# ---------------------------------------------------------------------------


class TestGet:
    """TaskQ.get public-behaviour tests."""

    async def test_get_existing_job_returns_handle(self, pg_dsn: str) -> None:
        """get(job_id) returns a JobHandle for an existing job."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            enqueued = await tq.enqueue(_test_actor, _Payload(value=10))
            found = await tq.get(enqueued.job_id, result_adapter=_RA)

        assert found is not None
        assert isinstance(found, JobHandle)
        assert found.job_id == enqueued.job_id

    async def test_get_unknown_id_returns_none(self, pg_dsn: str) -> None:
        """get(unknown_id) returns None — does not raise."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            result = await tq.get(new_job_id(), result_adapter=_RA)

        assert result is None


# ---------------------------------------------------------------------------
# TestList
# ---------------------------------------------------------------------------


class TestList:
    """TaskQ.list filtering tests."""

    async def test_list_by_queue_returns_enqueued_job(self, pg_dsn: str) -> None:
        """list(JobFilter(queue='default')) returns a page containing the enqueued job."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=20))
            page = await tq.list(JobFilter(queue="default"))

        assert any(j.id == handle.job_id for j in page.jobs)

    async def test_list_by_status_pending_filters_correctly(self, pg_dsn: str) -> None:
        """list(JobFilter(status='pending')) returns only pending jobs."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=21))
            page = await tq.list(JobFilter(status="pending"))

        assert any(j.id == handle.job_id for j in page.jobs)
        assert all(j.status == "pending" for j in page.jobs)

    async def test_list_by_nonexistent_actor_returns_empty_page(self, pg_dsn: str) -> None:
        """list(JobFilter(actor='no_such_actor')) returns an empty page."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            # Ensure at least one job exists so the filter is meaningful.
            await tq.enqueue(_test_actor, _Payload(value=22))
            page = await tq.list(JobFilter(actor="no_such_actor"))

        assert page.jobs == []


# ---------------------------------------------------------------------------
# TestCancel
# ---------------------------------------------------------------------------


class TestCancel:
    """TaskQ.cancel public-behaviour tests."""

    async def test_cancel_pending_job_returns_cancel_result(self, pg_dsn: str) -> None:
        """cancel(job_id) on a pending job returns CancelResult with
        cancellation_initiated=True and previous_status='pending'.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=30))
            result = await tq.cancel(handle.job_id)

        assert isinstance(result, CancelResult)
        assert result.cancellation_initiated is True
        assert result.previous_status == "pending"
        assert result.job_id == handle.job_id

    async def test_cancel_unknown_id_raises_key_error(self, pg_dsn: str) -> None:
        """cancel(unknown_id) raises KeyError."""
        await _migrate(pg_dsn)
        missing_id: JobId = new_job_id()
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            with pytest.raises(KeyError):
                await tq.cancel(missing_id)


# ---------------------------------------------------------------------------
# TestSchedules
# ---------------------------------------------------------------------------


class TestSchedules:
    """TaskQ.update_schedule / TaskQ.delete_schedule delegation tests."""

    async def test_update_schedule_delegates_and_returns_updated_record(self, pg_dsn: str) -> None:
        """update_schedule() delegates to JobsClient.update_schedule and the
        returned ScheduleRecord reflects the requested change.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            created = await tq.create_schedule(
                _test_actor,
                "0 * * * *",
                name="tq-client-update-schedule",
                enabled=True,
            )

            updated = await tq.update_schedule(created.schedule_id, enabled=False)

            assert updated.id == created.schedule_id
            assert updated.enabled is False

            listed = await tq.list_schedules()

        matching = [s for s in listed if s.id == created.schedule_id]
        assert len(matching) == 1
        assert matching[0].enabled is False

    async def test_delete_schedule_delegates_and_removes_schedule(self, pg_dsn: str) -> None:
        """delete_schedule() delegates to JobsClient.delete_schedule; the
        schedule no longer appears in list_schedules() afterwards.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            created = await tq.create_schedule(
                _test_actor,
                "0 * * * *",
                name="tq-client-delete-schedule",
                enabled=True,
            )

            await tq.delete_schedule(created.schedule_id)

            listed = await tq.list_schedules()

        assert all(s.id != created.schedule_id for s in listed)

    async def test_delete_schedule_is_idempotent(self, pg_dsn: str) -> None:
        """delete_schedule() on an already-deleted schedule does not raise
        (delegation is idempotent per JobsClient contract).
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            created = await tq.create_schedule(
                _test_actor,
                "0 * * * *",
                name="tq-client-delete-schedule-twice",
                enabled=True,
            )
            await tq.delete_schedule(created.schedule_id)
            await tq.delete_schedule(created.schedule_id)  # must not raise


# ---------------------------------------------------------------------------
# TestStream
# ---------------------------------------------------------------------------


class TestStream:
    """TaskQ.stream behaviour tests."""

    async def test_stream_on_terminal_job_yields_one_event(self, pg_dsn: str) -> None:
        """stream() on a job that is already terminal yields exactly one
        JobEvent with terminal=True and returns.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=40))
            await tq.cancel(handle.job_id)
            events: list[JobEvent] = []
            async for event in tq.stream(handle.job_id):
                events.append(event)

        assert len(events) == 1
        assert events[0].terminal is True
        assert events[0].status == "cancelled"

    async def test_stream_on_nonexistent_job_raises_key_error(self, pg_dsn: str) -> None:
        """stream() on a non-existent job_id raises KeyError."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            with pytest.raises(KeyError):
                async for _ in tq.stream(new_job_id()):
                    pass

    async def test_stream_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """stream() called outside async with block raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            async for _ in tq.stream(new_job_id()):
                pass


# ---------------------------------------------------------------------------
# TestStreamPgInternals — direct exercise of the PG LISTEN/NOTIFY transport
# ---------------------------------------------------------------------------


async def _set_job_status(pool: asyncpg.Pool, schema: str, job_id: UUID, status: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET status = $1 WHERE id = $2',  # noqa: S608 — schema is a worker-scoped constant, not user input; values are $N-bound
            status,
            job_id,
        )


async def _delete_job(pool: asyncpg.Pool, schema: str, job_id: UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f'DELETE FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 — schema is a worker-scoped constant, not user input; values are $N-bound
            job_id,
        )


class TestStreamPgInternals:
    """Direct unit tests for :func:`taskq.client._taskq._stream_pg`.

    Bypasses ``TaskQ.stream()`` / a live worker to drive multi-event
    transitions deterministically. Uses a small ``poll_timeout`` so the
    fallback poll loop advances quickly without depending on real
    LISTEN/NOTIFY timing.
    """

    async def test_stream_pg_yields_on_change_and_returns_on_terminal(self, pg_dsn: str) -> None:
        """_stream_pg yields an event for each detected status change and
        returns once a terminal status is observed.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=100))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            events: list[JobEvent] = []

            async def _consume() -> None:
                async for evt in _stream_pg(
                    pg_dsn,
                    _SCHEMA_LABEL,
                    handle.job_id,
                    client,
                    0.05,
                    last_seq=-1,
                    last_status=None,
                ):
                    events.append(evt)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "running")
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "succeeded")
            await asyncio.wait_for(consumer, timeout=5)

        statuses = [e.status for e in events]
        assert "running" in statuses
        assert statuses[-1] == "succeeded"
        assert events[-1].terminal is True

    async def test_stream_pg_wakes_on_real_notify(self, pg_dsn: str) -> None:
        """A real ``NOTIFY`` on the wake channel wakes the LISTEN loop
        (exercises the ``_on_notify`` callback registered via ``add_listener``).
        """
        from taskq.constants import wake_channel

        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=103))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            events: list[JobEvent] = []

            async def _consume() -> None:
                async for evt in _stream_pg(
                    pg_dsn,
                    _SCHEMA_LABEL,
                    handle.job_id,
                    client,
                    30.0,  # long timeout — only a real NOTIFY should wake this loop
                    last_seq=-1,
                    last_status=None,
                ):
                    events.append(evt)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)  # let the LISTEN registration land
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "succeeded")
            async with pool.acquire() as conn:
                await conn.execute(f"NOTIFY \"{wake_channel(_SCHEMA_LABEL)}\", 'x'")
            await asyncio.wait_for(consumer, timeout=5)

        assert events[-1].status == "succeeded"
        assert events[-1].terminal is True

    async def test_stream_via_taskq_multiple_pg_events(self, pg_dsn: str) -> None:
        """TaskQ.stream() (PG transport, no redis_client) yields more than one
        JobEvent across successive status transitions before terminating —
        exercises the ``_stream_pg`` delegation branch in ``TaskQ.stream()``.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL, poll_timeout=0.05) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=104))
            pool = tq._pool
            assert pool is not None

            events: list[JobEvent] = []

            async def _consume() -> None:
                async for evt in tq.stream(handle.job_id):
                    events.append(evt)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "running")
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "succeeded")
            await asyncio.wait_for(consumer, timeout=5)

        assert len(events) >= 2
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"

    async def test_stream_pg_raises_key_error_when_job_disappears(self, pg_dsn: str) -> None:
        """_stream_pg raises KeyError if the job row disappears mid-stream."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=101))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            async def _consume() -> None:
                async for _ in _stream_pg(
                    pg_dsn,
                    _SCHEMA_LABEL,
                    handle.job_id,
                    client,
                    0.05,
                    last_seq=-1,
                    last_status=None,
                ):
                    pass

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            await _delete_job(pool, _SCHEMA_LABEL, handle.job_id)

            with pytest.raises(KeyError):
                await asyncio.wait_for(consumer, timeout=5)

    async def test_stream_pg_falls_back_to_polling_after_listen_connection_loss(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the LISTEN wait raises OSError (simulating a killed connection),
        _stream_pg logs a warning and falls back to a plain ``asyncio.sleep``
        poll loop, still detecting the eventual terminal transition.
        """
        import taskq.client._taskq as taskq_mod

        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=102))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            real_wait_for = asyncio.wait_for
            call_count = 0

            async def _fake_wait_for(aw: object, timeout: float | None = None) -> object:  # noqa: ASYNC109 — mirrors asyncio.wait_for's signature to monkeypatch it in a test
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    close = getattr(aw, "close", None)
                    if callable(close):
                        close()
                    raise OSError("simulated LISTEN connection loss")
                return await real_wait_for(aw, timeout)  # type: ignore[arg-type]

            monkeypatch.setattr(taskq_mod.asyncio, "wait_for", _fake_wait_for)

            events: list[JobEvent] = []

            async def _consume() -> None:
                async for evt in _stream_pg(
                    pg_dsn,
                    _SCHEMA_LABEL,
                    handle.job_id,
                    client,
                    0.05,
                    last_seq=-1,
                    last_status=None,
                ):
                    events.append(evt)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            assert call_count >= 1  # the injected OSError has fired
            # A non-terminal transition inside the fallback poll loop exercises
            # the loop-back path (as opposed to returning immediately).
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "running")
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "succeeded")
            await asyncio.wait_for(consumer, timeout=5)

        assert "running" in [e.status for e in events]
        assert events[-1].status == "succeeded"
        assert events[-1].terminal is True

    async def test_stream_pg_fallback_raises_key_error_when_job_disappears(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the job row disappears while the stream is in the post-OSError
        fallback poll loop, KeyError is still raised (fallback loop's own
        disappearance check).
        """
        import taskq.client._taskq as taskq_mod

        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=105))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            real_wait_for = asyncio.wait_for
            call_count = 0

            async def _fake_wait_for(aw: object, timeout: float | None = None) -> object:  # noqa: ASYNC109 — mirrors asyncio.wait_for's signature to monkeypatch it in a test
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    close = getattr(aw, "close", None)
                    if callable(close):
                        close()
                    raise OSError("simulated LISTEN connection loss")
                return await real_wait_for(aw, timeout)  # type: ignore[arg-type]

            monkeypatch.setattr(taskq_mod.asyncio, "wait_for", _fake_wait_for)

            async def _consume() -> None:
                async for _ in _stream_pg(
                    pg_dsn,
                    _SCHEMA_LABEL,
                    handle.job_id,
                    client,
                    0.05,
                    last_seq=-1,
                    last_status=None,
                ):
                    pass

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            assert call_count >= 1  # now in the fallback poll loop
            await _delete_job(pool, _SCHEMA_LABEL, handle.job_id)

            with pytest.raises(KeyError):
                await asyncio.wait_for(consumer, timeout=5)


# ---------------------------------------------------------------------------
# TestStreamRedisInternals — direct unit tests for _stream_redis
# ---------------------------------------------------------------------------


def _stub_backend_sequence(rows: list[JobRow | None]) -> Backend:
    """Build a stub Backend where ``get`` returns successive values from *rows*."""
    remaining = list(rows)
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:
        if remaining:
            return remaining.pop(0)
        return None

    backend.get = _get
    return backend


class TestStreamRedisInternals:
    """Direct unit tests for :func:`taskq.client._taskq._stream_redis`."""

    async def test_stream_redis_refetch_raises_key_error_when_job_disappears(
        self, pg_dsn: str
    ) -> None:
        """_refetch() raises KeyError when the job row disappears between the
        initial fetch and a later redis-triggered refetch.
        """
        from taskq.client._jobs import JobsClient
        from taskq.progress._events import ProgressEvent
        from taskq.settings import TaskQSettings

        job_id = new_job_id()
        backend = _stub_backend_sequence([None])
        settings_schema = _SCHEMA_LABEL

        settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": settings_schema})
        client = JobsClient(backend, settings=settings)

        pubsub = AsyncMock()
        pubsub.subscribe = AsyncMock()
        pubsub.unsubscribe = AsyncMock()
        pubsub.aclose = AsyncMock()

        progress_event = ProgressEvent(
            kind="progress",
            job_id=job_id,
            actor="test_actor",
            ts=datetime.now(UTC),
            seq=1,
            status="running",
        )
        raw_data = progress_event.model_dump_json(exclude_none=True).encode("utf-8")

        message_calls = 0

        async def _get_message(
            *,
            ignore_subscribe_messages: bool = True,
            timeout: float = 0,  # noqa: ASYNC109 — mirrors redis-py's get_message signature to monkeypatch it in a test
        ) -> dict[str, object] | None:
            nonlocal message_calls
            message_calls += 1
            if message_calls == 1:
                return {"type": "message", "data": raw_data}
            return None

        pubsub.get_message = _get_message
        redis_client = MagicMock(spec=["pubsub"])
        redis_client.pubsub.return_value = pubsub

        with pytest.raises(KeyError):
            async for _ in _stream_redis(
                redis_client,
                settings_schema,
                job_id,
                client,
                30.0,
                last_seq=-1,
                last_status=None,
            ):
                pass
