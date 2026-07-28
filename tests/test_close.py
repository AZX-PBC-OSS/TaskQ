"""Unit pins for the :mod:`taskq._close` bounded-close contract.

Pins the promises the helpers' docstrings make but the suite previously
never asserted: the helpers never raise EXCEPT ``asyncio.CancelledError``
(which must still propagate so outer cancellation unwinds promptly — a
refactor to ``except BaseException`` would otherwise pass the whole suite);
a hung conn close is terminated after the bound; and ``mid_run`` selects the
structlog event family (``conn-close-*`` vs ``conn-teardown-close-*``) so a
mid-run close failure stays distinguishable from final-teardown noise in log
alerts (review C10). Docker-free, hand-rolled fakes; no ``pytestmark`` so
the file runs under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio

import pytest
import structlog.testing

from taskq._close import close_conn_bounded, close_pool_bounded, close_redis_bounded

# ── Test helpers ───────────────────────────────────────────────────────
# Minimal fakes mirror the hang-gate / terminate-tracking conventions of
# tests/test_worker_deps_teardown.py (itself mirroring
# tests/test_reload_credentials.py). tests/conftest.py's _FakePool/_FakeConn
# are no-op stubs without close()/terminate()/aclose() tracking, so they
# cannot be reused here.


class _FakePool:
    """Minimal asyncpg.Pool fake: close() that can raise; terminate tracking.

    Real-semantics error path (asyncpg 0.31 ``Pool.close()``): on ANY close
    error the pool calls ``self.terminate()`` and sets ``self._closed = True``
    in the ``finally`` before re-raising — so a raising close still leaves
    the pool terminated AND closed. ``terminate_calls`` counts only EXTERNAL
    terminate() invocations, so tests can still pin "the helper never
    terminates on the error path".
    """

    def __init__(self) -> None:
        self.closed = False
        self.terminated = False
        self.terminate_calls = 0
        self.close_calls = 0
        self.close_error: BaseException | None = None

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            # Mirrors asyncpg Pool.close(): except -> terminate(); finally ->
            # _closed = True; then re-raise.
            self.terminated = True
            self.closed = True
            raise self.close_error
        self.closed = True

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.terminated = True
        self.closed = True


class _FakeConn:
    """Minimal asyncpg.Connection fake: hang-gated close(), terminate tracking."""

    def __init__(self) -> None:
        self.closed = False
        self.terminated = False
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.close_error: BaseException | None = None

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        if self.close_error is not None:
            # Mirrors asyncpg 0.31 Connection.close(): on ANY close error it
            # calls self._abort() before re-raising, so is_closed() is True
            # afterwards — a raising close still leaves the conn closed.
            self.closed = True
            raise self.close_error
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()  # aborts any in-flight close() wait


class _FakeRedisClient:
    """Minimal Redis fake: aclose() that can raise."""

    def __init__(self) -> None:
        self.aclose_calls = 0
        self.aclose_error: BaseException | None = None

    async def aclose(self) -> None:
        self.aclose_calls += 1
        if self.aclose_error is not None:
            raise self.aclose_error


# ── CancelledError propagation pins ────────────────────────────────────


async def test_close_pool_bounded_propagates_cancelled_error() -> None:
    """A pool close() raising CancelledError propagates out of
    close_pool_bounded — the never-raise contract covers ``Exception`` only,
    so outer cancellation is never swallowed."""
    pool = _FakePool()
    pool.close_error = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await close_pool_bounded(pool, "pool", 0.05)


async def test_close_conn_bounded_propagates_cancelled_error() -> None:
    """Same CancelledError propagation pin for close_conn_bounded."""
    conn = _FakeConn()
    conn.close_error = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await close_conn_bounded(conn, "x", 0.05)


async def test_close_redis_bounded_propagates_cancelled_error() -> None:
    """Same CancelledError propagation pin for close_redis_bounded."""
    client = _FakeRedisClient()
    client.aclose_error = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await close_redis_bounded(client, "redis", 0.05)


# ── mid_run event-family pins ──────────────────────────────────────────


async def test_close_conn_bounded_mid_run_timeout_logs_conn_close_family() -> None:
    """A hung mid-run conn close is terminated after the bound and logs the
    ``conn-close-*`` family — NOT ``conn-teardown-close-*`` — so a conn so
    dead that even close() hung while the worker is alive stays
    distinguishable in log alerts."""
    conn = _FakeConn()
    conn.close_wait.clear()  # close() blocks forever from now on

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await close_conn_bounded(conn, "x", 0.05, mid_run=True)

    assert conn.terminated is True
    assert conn.close_calls == 1
    timeout_events = [e for e in captured if e.get("event") == "conn-close-timeout-terminating"]
    assert len(timeout_events) == 1, f"expected 1 mid-run timeout event, got {captured!r}"
    event = timeout_events[0]
    assert event.get("close_timeout") == 0.05
    assert event.get("label") == "x"
    teardown_events = [
        e for e in captured if str(e.get("event", "")).startswith("conn-teardown-close-")
    ]
    assert teardown_events == [], f"teardown family must not fire mid-run: {captured!r}"


async def test_close_conn_bounded_teardown_timeout_logs_teardown_family() -> None:
    """A hung conn close on the default (final-teardown) path is terminated
    after the bound and logs the ``conn-teardown-close-*`` family — NOT the
    mid-run ``conn-close-*`` family — so final-teardown noise never
    masquerades as a mid-run close failure in log alerts."""
    conn = _FakeConn()
    conn.close_wait.clear()  # close() blocks forever from now on

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await close_conn_bounded(conn, "x", 0.05)  # mid_run defaults to False

    assert conn.terminated is True
    assert conn.close_calls == 1
    timeout_events = [
        e for e in captured if e.get("event") == "conn-teardown-close-timeout-terminating"
    ]
    assert len(timeout_events) == 1, f"expected 1 teardown timeout event, got {captured!r}"
    event = timeout_events[0]
    assert event.get("close_timeout") == 0.05
    assert event.get("label") == "x"
    mid_run_events = [e for e in captured if str(e.get("event", "")).startswith("conn-close-")]
    assert mid_run_events == [], f"mid-run family must not fire on teardown: {captured!r}"


async def test_close_conn_bounded_mid_run_error_logs_conn_close_error() -> None:
    """A mid-run conn close() that raises is logged as ``conn-close-error``
    and swallowed (never-raise); the HELPER does not terminate — termination
    is the timeout path only. The conn IS still closed: real asyncpg aborts
    the conn before re-raising a close error, which the fake mirrors."""
    conn = _FakeConn()
    boom = RuntimeError("simulated PG close failure")
    conn.close_error = boom

    with structlog.testing.capture_logs() as captured:
        await close_conn_bounded(conn, "x", 0.05, mid_run=True)  # must not raise

    assert conn.close_calls == 1
    assert conn.terminated is False  # helper never terminates on the error path
    assert conn.closed is True  # real asyncpg Connection.close() aborts, then re-raises
    error_events = [e for e in captured if e.get("event") == "conn-close-error"]
    assert len(error_events) == 1, f"expected 1 mid-run error event, got {captured!r}"
    event = error_events[0]
    assert event.get("error") == repr(boom)
    assert event.get("label") == "x"


async def test_close_conn_bounded_teardown_error_logs_teardown_family() -> None:
    """The same raising close() on the default (final-teardown) path logs
    ``conn-teardown-close-error`` instead — the two families never mix."""
    conn = _FakeConn()
    boom = RuntimeError("simulated PG close failure")
    conn.close_error = boom

    with structlog.testing.capture_logs() as captured:
        await close_conn_bounded(conn, "x", 0.05)  # mid_run defaults to False

    assert conn.close_calls == 1
    assert conn.terminated is False  # helper never terminates on the error path
    assert conn.closed is True  # real asyncpg Connection.close() aborts, then re-raises
    error_events = [e for e in captured if e.get("event") == "conn-teardown-close-error"]
    assert len(error_events) == 1, f"expected 1 teardown error event, got {captured!r}"
    event = error_events[0]
    assert event.get("error") == repr(boom)
    assert event.get("label") == "x"


# ── Teardown error-event pins: pool / redis ────────────────────────────


async def test_close_pool_bounded_error_logs_pool_teardown_close_error() -> None:
    """A pool close() that raises is logged as ``pool-teardown-close-error``
    and swallowed (never-raise); the HELPER does not terminate — termination
    by the helper is the timeout path only. The pool IS still terminated and
    closed: real asyncpg Pool.close() self-terminates and marks the pool
    closed before re-raising a close error, which the fake mirrors."""
    pool = _FakePool()
    boom = RuntimeError("simulated PG close failure")
    pool.close_error = boom

    with structlog.testing.capture_logs() as captured:
        await close_pool_bounded(pool, "pool", 0.05)  # must not raise

    assert pool.close_calls == 1
    assert pool.terminate_calls == 0  # helper never terminates on the error path
    assert pool.terminated is True  # real Pool.close() self-terminates on close error
    assert pool.closed is True  # ... and sets _closed=True in the finally
    error_events = [e for e in captured if e.get("event") == "pool-teardown-close-error"]
    assert len(error_events) == 1, f"expected 1 pool error event, got {captured!r}"
    event = error_events[0]
    assert event.get("error") == repr(boom)
    assert event.get("pool") == "pool"


async def test_close_redis_bounded_error_logs_redis_teardown_close_error() -> None:
    """A redis aclose() that raises is logged as
    ``redis-teardown-close-error`` and swallowed (never-raise), so teardown
    keeps unwinding. The event carries ``label=`` identifying which client
    errored, matching the pool/conn siblings (review N7)."""
    client = _FakeRedisClient()
    boom = RuntimeError("simulated Redis close failure")
    client.aclose_error = boom

    with structlog.testing.capture_logs() as captured:
        await close_redis_bounded(client, "redis", 0.05)  # must not raise

    assert client.aclose_calls == 1
    error_events = [e for e in captured if e.get("event") == "redis-teardown-close-error"]
    assert len(error_events) == 1, f"expected 1 redis error event, got {captured!r}"
    event = error_events[0]
    assert event.get("error") == repr(boom)
    assert event.get("label") == "redis"
