"""Unit-tier tests for PostgresBackend.dispatch_batch.

anchor: Tests use fake asyncpg pool/connection objects;
PG-container integration tests for the full CTE round-trip are in a separate
integration module.
"""

import asyncio
import math
from collections.abc import Callable
from datetime import timedelta
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import pytest

from taskq.backend._protocol import JobRow
from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosPool

_GRACE = timedelta(seconds=30)
_FIXED_UUID = UUID("12345678-1234-5678-1234-567812345678")
# Pinned dispatcher_command_timeout so the acquire-timeout forwarding is
# observable through the fake pools.
_ACQUIRE_TIMEOUT = 1.5

_FakeFetch = Callable[..., list[dict[str, Any]]]

# ── Default record fields for _job_row_from_record decoding ────────────


# ── Fake pool / connection ─────────────────────────────────────────────


class _MarkingFakePool:
    """Fake pool that tracks whether ``acquire()`` was called.

    ``marker`` is a shared list that gets appended when ``acquire()`` fires.
    ``acquire_timeouts`` records the ``timeout=`` each call was given.
    Mirrors asyncpg's acquire contract: keyword-only ``timeout`` bound, and
    the method returns the connection context (asyncpg's own ``acquire`` is
    also a sync method returning an async context manager).
    """

    def __init__(self, marker: list[str], fetch_fn: _FakeFetch | None = None) -> None:
        self._marker = marker
        self._fetch_fn = fetch_fn
        self.acquire_timeouts: list[float | None] = []

    def acquire(self, *, timeout: float | None = None) -> "_MarkingFakeConn":
        # Recorded, never discarded: a fake that swallows ``timeout=``
        # makes the bounded-wait invariant untestable through it.
        self._marker.append("acquire")
        self.acquire_timeouts.append(timeout)
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


def _backend_with_dispatcher_pool(
    pool: object, *, command_timeout: float = _ACQUIRE_TIMEOUT
) -> PostgresBackend:
    """Construct a PostgresBackend whose dispatcher_pool is *pool*.

    Shared seam for every fake-pool test in this module: pins
    ``dispatcher_command_timeout`` to *command_timeout* (default
    ``_ACQUIRE_TIMEOUT``) so the acquire-timeout forwarding through
    ``dispatch_batch`` is observable and variable.
    """
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.settings.dispatch_oversample = 2
    mock_deps.settings.claim_cursor_reset_seconds = 0.0
    mock_deps.settings.dispatcher_command_timeout = command_timeout
    mock_deps.worker_pool = Mock()
    mock_deps.dispatcher_pool = pool

    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0

    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


def _make_backend_with_pool(
    pool: object, *, command_timeout: float = _ACQUIRE_TIMEOUT
) -> PostgresBackend:
    """Construct a PostgresBackend with *pool* as the dispatcher pool."""
    return _backend_with_dispatcher_pool(pool, command_timeout=command_timeout)


def _make_backend(
    *,
    dispatcher_marker: list[str],
    fetch_fn: _FakeFetch | None = None,
) -> PostgresBackend:
    """Construct a PostgresBackend with a marking fake dispatcher pool."""
    return _backend_with_dispatcher_pool(_MarkingFakePool(dispatcher_marker, fetch_fn))


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

    def test_acquire_bounded_by_command_timeout(self) -> None:
        """The pool checkout is bounded: dispatcher_command_timeout is
        forwarded as acquire(timeout=...) so an exhausted pool cannot park
        the producer forever (contract added by 61cd656)."""
        marker: list[str] = []
        pool = _MarkingFakePool(marker)
        backend = _backend_with_dispatcher_pool(pool)
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert marker == ["acquire"]
        assert pool.acquire_timeouts == [_ACQUIRE_TIMEOUT]


class TestDispatchBatchDecoding:
    """Records returned by the helper are decoded via _job_row_from_record.

    NOTE (the mock-subtraction cure): the fake-record decode arms are
    deleted — tests/test_dispatch_pg.py::test_full_cte_round_trip owns the
    decode against the REAL claim's row (every field, status, attempt,
    locks, timestamps), and the empty page's shape rides its backlog arms.
    The param-order arm died with them (the real round trip's own claim is
    the statement the params feed).
    """


class TestDispatchBatchPoolSelection:
    """The dispatcher pool is used, not any other pool."""

    def test_dispatcher_pool_used_for_acquire(self) -> None:
        notify_marker: list[str] = []
        dispatch_marker: list[str] = []

        _dp = _MarkingFakePool(dispatch_marker)
        _np = _MarkingFakePool(notify_marker)

        backend = _backend_with_dispatcher_pool(_dp)
        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
        assert dispatch_marker == ["acquire"]
        assert notify_marker == []


class TestDispatchBatchEmptyQueues:
    """Empty queues list: implementation passes through (not short-circuits).

    NOTE (the mock-subtraction cure): the fake's bound-arg pin is deleted;
    the pass-through BEHAVIOR (an empty queue list completes, returns an
    empty batch, still one pool checkout) converted to the real fixture in
    tests/test_dispatch_pg.py::test_empty_queue_list_round_trips_empty.
    """


class TestDispatchBatchCancellation:
    """Cancellation during dispatch propagates CancelledError; connection released."""

    def test_cancellation_propagates_cancelled_error(self) -> None:
        marker: list[str] = []

        class _SlowFakePool:
            def __init__(self, marker: list[str]) -> None:
                self._marker = marker

            def acquire(self, *, timeout: float | None = None) -> "_SlowFakeConn":
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

        backend = _backend_with_dispatcher_pool(_SlowFakePool(marker))

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


# ── The producer's acquire is bounded ──────────────────────────────────
#
# `_dispatch_batch` used a bare `dispatcher_pool.acquire()`. Every
# statement inside its transaction is bounded by the dispatcher pool's
# `command_timeout`, so the checkout was the one unbounded wait left on
# the producer's path: with the pool exhausted (leader loops, health
# ping, shutdown and bootstrap share it) the producer parks forever and
# only the stale-loop watchdog force-exiting the process ends it.
#
# These use ChaosPool's `acquire_delay` to model an exhausted pool.
# Before that knob existed, ChaosPool accepted `timeout=` and discarded
# it, so no test could observe an acquire bound at all.


class TestDispatchBatchAcquireIsBounded:
    """dispatch_batch passes a timeout to acquire(), and it fires."""

    def test_acquire_receives_the_configured_command_timeout(self) -> None:
        """The bound is `settings.dispatcher_command_timeout`, not a literal.

        Kills the mutant that drops `timeout=` (None recorded) and the one
        that hard-codes a value.
        """
        marker: list[str] = []
        pool = _MarkingFakePool(marker)
        backend = _make_backend_with_pool(pool)

        asyncio.run(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))

        assert pool.acquire_timeouts == [_ACQUIRE_TIMEOUT]

    def test_exhausted_pool_raises_instead_of_hanging(self) -> None:
        """Against a pool that never yields a connection, dispatch_batch
        completes with TimeoutError instead of blocking forever.

        The discriminator is deliberately NOT "a TimeoutError was raised".
        An unbounded acquire hanging inside an outer `wait_for` guard also
        raises TimeoutError, so `pytest.raises(TimeoutError)` around a
        guarded call passes either way - it cannot fail, which is the
        exact defect this file's harness had. Instead `asyncio.wait`
        reports the task as *still pending*, which only an unbounded
        acquire can produce, and that is asserted directly.
        """
        bound = 0.05
        chaos_conn = ChaosConnection(object(), fail_on_call=0)
        pool = ChaosPool(chaos_conn, acquire_delay=math.inf)
        backend = _make_backend_with_pool(pool, command_timeout=bound)

        async def _run() -> None:
            task = asyncio.create_task(backend.dispatch_batch(_FIXED_UUID, ["default"], 10, _GRACE))
            # asyncio.wait never raises on expiry - it reports what is
            # still pending, keeping "the bound fired" and "nothing
            # bounded it" distinguishable.
            _done, pending = await asyncio.wait({task}, timeout=bound * 20)
            if pending:
                task.cancel()
                pytest.fail(
                    f"dispatch_batch did not bound its pool acquire: still running "
                    f"{bound * 20}s into a {bound}s timeout against an exhausted pool"
                )
            assert isinstance(task.exception(), TimeoutError), (
                f"expected the acquire bound to surface as TimeoutError, got {task.exception()!r}"
            )

        asyncio.run(_run())
