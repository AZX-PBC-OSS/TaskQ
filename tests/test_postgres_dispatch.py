"""Unit-tier tests for PostgresBackend.dispatch_batch.

anchor: Tests use fake asyncpg pool/connection objects;
PG-container integration tests for the full CTE round-trip are in a separate
integration module.
"""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend

_GRACE = timedelta(seconds=30)
_FIXED_UUID = UUID("12345678-1234-5678-1234-567812345678")

_FakeFetch = Callable[..., list[dict[str, Any]]]

# ── Default record fields for _job_row_from_record decoding ────────────


def _default_record() -> dict[str, Any]:
    """Return a dict with all keys that ``_job_row_from_record`` accesses."""
    return {
        "id": new_uuid(),
        "actor": "test_actor",
        "queue": "default",
        "identity_key": None,
        "fairness_key": "default",
        "payload": '{"msg": "hello"}',
        "payload_schema_ver": 1,
        "status": "pending",
        "priority": 0,
        "attempt": 0,
        "max_attempts": 5,
        "retry_kind": "transient",
        "schedule_to_close": None,
        "start_to_close": None,
        "heartbeat_timeout": None,
        "created_at": datetime(2026, 5, 4, tzinfo=UTC),
        "scheduled_at": datetime(2026, 5, 4, tzinfo=UTC),
        "started_at": None,
        "finished_at": None,
        "last_heartbeat_at": None,
        "locked_by_worker": None,
        "lock_expires_at": None,
        "cancel_requested_at": None,
        "cancel_phase": 0,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "progress_state": "{}",
        "progress_seq": 0,
        "result": None,
        "result_size_bytes": None,
        "result_expires_at": None,
        "idempotency_key": None,
        "trace_id": None,
        "span_id": None,
        "metadata": "{}",
        "tags": [],
    }


# ── Fake pool / connection ─────────────────────────────────────────────


class _MarkingFakePool:
    """Fake pool that tracks whether ``acquire()`` was called.

    ``marker`` is a shared list that gets appended when ``acquire()`` fires.
    """

    def __init__(self, marker: list[str], fetch_fn: _FakeFetch | None = None) -> None:
        self._marker = marker
        self._fetch_fn = fetch_fn

    def acquire(self) -> "_MarkingFakeConn":
        self._marker.append("acquire")
        return _MarkingFakeConn(self._fetch_fn)

    async def __aenter__(self) -> "_MarkingFakePool":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class _MarkingFakeConn:
    """Fake connection that tracks fetch calls and params."""

    def __init__(self, fetch_fn: _FakeFetch | None = None) -> None:
        self._fetch_fn = fetch_fn
        self._call_idx = 0

    async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
        self._fetch_sql = sql
        self._fetch_args = args
        if self._call_idx == 0:
            self._call_idx += 1
            return []
        self._call_idx += 1
        if self._fetch_fn is not None:
            return self._fetch_fn()
        return []

    async def execute(self, sql: str, *args: object) -> str:
        return "UPDATE 0"

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()

    async def __aenter__(self) -> "_MarkingFakeConn":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class _FakeTransaction:
    """Fake asyncpg transaction context manager."""

    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


# ── Build PostgresBackend with fake deps ────────────────────────────────


def _make_backend(
    *,
    dispatcher_marker: list[str] | None = None,
    fetch_fn: _FakeFetch | None = None,
    schema_name: str = "taskq_test",
) -> PostgresBackend:
    """Construct a PostgresBackend with fake deps for unit testing."""
    mock_deps = Mock()
    mock_deps.settings.schema_name = schema_name
    mock_deps.worker_pool = Mock()
    _dp = _MarkingFakePool(dispatcher_marker, fetch_fn) if dispatcher_marker is not None else Mock()
    mock_deps.dispatcher_pool = _dp

    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0

    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


class _CaptureFakePool:
    """Fake pool whose connection records fetch() parameters."""

    def __init__(self, marker: list[str], captured: list[tuple[object, ...]]) -> None:
        self._marker = marker
        self._captured = captured

    def acquire(self) -> "_CaptureFakeConn":
        self._marker.append("acquire")
        return _CaptureFakeConn(self._captured)

    async def __aenter__(self) -> "_CaptureFakePool":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class _CaptureFakeConn:
    """Fake connection that records fetch() parameters."""

    def __init__(self, captured: list[tuple[object, ...]]) -> None:
        self._captured = captured

    async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
        self._captured.append(args)
        return []

    async def execute(self, sql: str, *args: object) -> str:
        return "UPDATE 0"

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()

    async def __aenter__(self) -> "_CaptureFakeConn":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


def _make_capture_backend(
    marker: list[str],
    captured: list[tuple[object, ...]],
) -> PostgresBackend:
    """Construct a PostgresBackend with a capture fake pool."""
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.settings.dispatch_oversample = 2
    mock_deps.worker_pool = Mock()
    mock_deps.dispatcher_pool = _CaptureFakePool(marker, captured)
    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


# ── Tests ──────────────────────────────────────────────────────────────


class TestDispatchBatchAcquire:
    """dispatch_batch calls acquire() exactly once per invocation."""

    def test_acquire_called_once(self) -> None:
        marker: list[str] = []
        backend = _make_backend(dispatcher_marker=marker)
        records: list[JobRow] = asyncio.run(
            backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE),
        )
        assert marker == ["acquire"]
        assert isinstance(records, list)

    def test_two_calls_acquire_twice(self) -> None:
        marker: list[str] = []
        backend = _make_backend(dispatcher_marker=marker)
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 5, _GRACE))
        assert marker == ["acquire", "acquire"]


class TestDispatchBatchHelperParams:
    """dispatch_batch calls the helper with the rendered SQL and correct params."""

    def test_helper_receives_rendered_sql(self) -> None:
        marker: list[str] = []
        backend = _make_backend(dispatcher_marker=marker, fetch_fn=lambda: [])
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert '"taskq_test".jobs' in backend._sql.dispatch_strict_fifo

    def test_helper_receives_params_in_order(self) -> None:
        marker: list[str] = []
        captured: list[tuple[object, ...]] = []

        backend = _make_capture_backend(marker, captured)
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["high", "low"], 10, _GRACE))

        # First fetch: queue modes query (1 arg); second: dispatch CTE (5 args: queues, limit_n, worker_id, lock_lease, oversample)
        dispatch_args = captured[-1]
        assert len(dispatch_args) == 5
        assert dispatch_args[0] == ["high", "low"]
        assert dispatch_args[1] == 10
        assert dispatch_args[2] == _FIXED_UUID
        assert dispatch_args[3] == _GRACE
        assert dispatch_args[4] == 2  # default oversample


class TestDispatchBatchDecoding:
    """Records returned by the helper are decoded via _job_row_from_record."""

    def test_returns_list_of_jobrow(self) -> None:
        marker: list[str] = []

        def _fetch_two() -> list[dict[str, Any]]:
            r1 = _default_record()
            r2 = _default_record()
            return [r1, r2]

        backend = _make_backend(dispatcher_marker=marker, fetch_fn=_fetch_two)
        result = asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert len(result) == 2
        assert all(isinstance(row, JobRow) for row in result)
        assert isinstance(result[0].id, UUID)

    def test_representative_field_matches(self) -> None:
        marker: list[str] = []
        job_id = new_uuid()

        def _fetch_one() -> list[dict[str, Any]]:
            rec = _default_record()
            rec["id"] = job_id
            return [rec]

        backend = _make_backend(dispatcher_marker=marker, fetch_fn=_fetch_one)
        result = asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert len(result) == 1
        assert result[0].id == JobId(job_id)

    def test_empty_result_returns_empty_list(self) -> None:
        marker: list[str] = []

        def _fetch_none() -> list[dict[str, Any]]:
            return []

        backend = _make_backend(dispatcher_marker=marker, fetch_fn=_fetch_none)
        result = asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert result == []


class TestDispatchBatchPoolSelection:
    """The dispatcher pool is used, not any other pool."""

    def test_dispatcher_pool_used_for_acquire(self) -> None:
        notify_marker: list[str] = []
        dispatch_marker: list[str] = []

        _dp = _MarkingFakePool(dispatch_marker)
        _np = _MarkingFakePool(notify_marker)

        mock_deps = Mock()
        mock_deps.settings.schema_name = "taskq_test"
        mock_deps.worker_pool = Mock()
        mock_deps.dispatcher_pool = _dp

        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=mock_deps,
            clock=mock_clock,
            cancellation_grace_period=_GRACE,
            cleanup_grace_period=_GRACE,
        )
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert dispatch_marker == ["acquire"]
        assert notify_marker == []


class TestDispatchBatchEmptyQueues:
    """Empty queues list: implementation passes through (not short-circuits)."""

    def test_empty_queues_passes_through(self) -> None:
        marker: list[str] = []
        captured: list[tuple[object, ...]] = []

        backend = _make_capture_backend(marker, captured)
        result = asyncio.run(backend.dispatch_batch(_FIXED_UUID, [], 10, _GRACE))
        assert result == []
        assert marker == ["acquire"]
        assert len(captured) >= 1
        dispatch_args = captured[-1]
        assert dispatch_args[0] == []


class TestDispatchBatchCancellation:
    """Cancellation during dispatch propagates CancelledError; connection released."""

    def test_cancellation_propagates_cancelled_error(self) -> None:
        marker: list[str] = []

        class _SlowFakePool:
            def __init__(self, marker: list[str]) -> None:
                self._marker = marker

            def acquire(self) -> "_SlowFakeConn":
                self._marker.append("acquire")
                return _SlowFakeConn()

            async def __aenter__(self) -> "_SlowFakePool":
                return self

            async def __aexit__(self, *args: object) -> None:
                pass

        class _SlowFakeConn:
            async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
                await asyncio.sleep(10.0)
                return []

            async def execute(self, sql: str, *args: object) -> str:
                return "UPDATE 0"

            def transaction(self) -> "_FakeTransaction":
                return _FakeTransaction()

            async def __aenter__(self) -> "_SlowFakeConn":
                return self

            async def __aexit__(self, *args: object) -> None:
                marker.append("conn_exit")

        mock_deps = Mock()
        mock_deps.settings.schema_name = "taskq_test"
        mock_deps.worker_pool = Mock()
        mock_deps.dispatcher_pool = _SlowFakePool(marker)
        mock_clock = Mock(spec=Clock)
        mock_clock.now.return_value = NotImplemented
        mock_clock.monotonic.return_value = 0.0

        backend = PostgresBackend(
            deps=mock_deps,
            clock=mock_clock,
            cancellation_grace_period=_GRACE,
            cleanup_grace_period=_GRACE,
        )

        async def _run_and_cancel() -> None:
            task = asyncio.create_task(
                backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE),
            )
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(_run_and_cancel())
        # Connection was released by the async with block
        assert marker[0] == "acquire"
        assert "conn_exit" in marker
