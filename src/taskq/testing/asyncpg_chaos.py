"""Chaos testing helpers for asyncpg connections.

Provides :class:`ChaosConnection` and :class:`ChaosPool` for simulating
mid-transaction failures in integration tests.  The wrapper raises
:class:`ChaosException` on the configured call number, allowing tests
to verify that transaction rollback works correctly when a failure
occurs between SQL statements inside a transaction.

Contract:

- :class:`ChaosException` is raised on the Nth query call (execute,
  fetchrow, fetch, fetchval).  Query calls are counted in execution
  order regardless of method name.
- Does **not** swallow ``CancelledError`` — it propagates naturally
  from the wrapped connection.
- :class:`ChaosPool` honours ``acquire(timeout=...)`` against its
  ``acquire_delay``, raising ``TimeoutError`` on expiry exactly as an
  exhausted ``asyncpg.Pool`` does.
- Defers all other semantics (transaction management, type codecs,
  connection lifecycle) to the wrapped connection.
- The ``transaction()`` method delegates to the real connection so
  asyncpg's transaction management (commit / rollback) works correctly
  when ``ChaosException`` is raised inside a transaction block.
"""

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object

__all__ = ["ChaosConnection", "ChaosException", "ChaosPool"]


class ChaosException(Exception):  # noqa: N818  # Why: test utility — not a public API exception; naming matches the "Chaos" prefix convention for test helpers
    """Raised by :class:`ChaosConnection` on the configured call number.

    Carries the call number for debugging.  Does **not** represent a real
    PG error — it simulates a failure between SQL statements inside a
    transaction, causing the transaction to roll back.
    """

    def __init__(self, call_number: int) -> None:
        self.call_number = call_number
        super().__init__(f"ChaosException: simulated failure on call {call_number}")


class ChaosConnection:
    """Async wrapper around an asyncpg ``Connection`` that raises
    :class:`ChaosException` on the configured call number.

    *fail_on_call* counts query methods (``execute``, ``fetchrow``,
    ``fetch``, ``fetchval``) in execution order.  When the counter
    reaches *fail_on_call*, ``ChaosException`` is raised **before**
    the query is sent to PG, simulating a mid-transaction failure.

    Set *fail_with* to raise a different exception type (e.g.
    :class:`asyncpg.PostgresConnectionError` or
    :class:`asyncpg.QueryCanceledError`) instead of the default
    :class:`ChaosException`.

    ``transaction()`` delegates to the real connection so that asyncpg's
    transaction context manager can roll back the real transaction when
    ``ChaosException`` propagates through ``async with conn.transaction():``.
    """

    def __init__(
        self,
        conn: _Conn,
        fail_on_call: int,
        fail_with: type[BaseException] = ChaosException,
    ) -> None:
        self._conn: _Conn = conn
        self._fail_on_call = fail_on_call
        self._fail_with = fail_with
        self._call_count = 0

    def _check(self) -> None:
        self._call_count += 1
        if self._call_count == self._fail_on_call:
            if self._fail_with is ChaosException:
                raise ChaosException(self._call_count)
            raise self._fail_with(
                f"{self._fail_with.__name__}: chaos failure on call {self._call_count}"
            )

    async def execute(self, query: str, *args: object, timeout: float | None = None) -> str:
        self._check()
        return await self._conn.execute(  # type: ignore[union-attr]  # Why: _conn is typed as `object` at runtime; asyncpg is TYPE_CHECKING-only to avoid transitive imports
            query, *args, timeout=timeout
        )

    async def fetchrow(
        self, query: str, *args: object, timeout: float | None = None
    ) -> object | None:
        self._check()
        return await self._conn.fetchrow(  # type: ignore[union-attr]  # Why: _conn is typed as `object` at runtime; asyncpg is TYPE_CHECKING-only to avoid transitive imports
            query, *args, timeout=timeout
        )

    async def fetch(self, query: str, *args: object, timeout: float | None = None) -> list[object]:
        self._check()
        return await self._conn.fetch(  # type: ignore[union-attr]  # Why: _conn is typed as `object` at runtime; asyncpg is TYPE_CHECKING-only to avoid transitive imports
            query, *args, timeout=timeout
        )

    async def fetchval(
        self,
        query: str,
        *args: object,
        column: int = 0,
        timeout: float | None = None,
    ) -> object | None:
        self._check()
        return await self._conn.fetchval(  # type: ignore[union-attr]  # Why: _conn is typed as `object` at runtime; asyncpg is TYPE_CHECKING-only to avoid transitive imports
            query, *args, column=column, timeout=timeout
        )

    def transaction(self, **kwargs: object) -> object:
        return self._conn.transaction(**kwargs)  # type: ignore[union-attr]  # Why: _conn is typed as `object` at runtime; asyncpg is TYPE_CHECKING-only to avoid transitive imports

    async def close(self) -> None:
        await self._conn.close()  # type: ignore[union-attr]  # Why: _conn is typed as `object` at runtime; asyncpg is TYPE_CHECKING-only to avoid transitive imports


class _ChaosAcquireCtx:
    """Async context manager yielded by :class:`ChaosPool.acquire`.

    Reproduces ``asyncpg.Pool.acquire``'s wait semantics: *acquire_delay*
    models the time the pool spends without a connection to hand over, and
    *timeout* bounds the caller's willingness to wait for one.

    Matched against asyncpg 0.31.0 ``pool.py`` rather than assumed:
    ``Pool._acquire`` runs ``await compat.wait_for(_acquire_impl(),
    timeout=timeout)`` when *timeout* is not None and bare
    ``await _acquire_impl()`` when it is, and ``compat.wait_for`` is
    ``asyncio.wait_for`` on Python 3.12+.  So this double calls the same
    ``asyncio.wait_for`` and lets the same ``TimeoutError`` propagate,
    from the same place: ``PoolAcquireContext.__aenter__``, which is
    where the real ``async with pool.acquire(timeout=...)`` surfaces it.
    ``timeout=None`` waits forever in both.

    The delay deliberately spans the whole acquire, because asyncpg's
    timeout does too: ``_acquire_impl`` covers both the wait on the
    pool's free-holder queue *and* ``ch.acquire()``'s connection setup,
    so a slow ``setup``/``init`` hook is inside the bound, not outside it.
    """

    def __init__(
        self, conn: ChaosConnection, acquire_delay: float | None, timeout: float | None
    ) -> None:
        self._conn = conn
        self._acquire_delay = acquire_delay
        self._timeout = timeout

    async def __aenter__(self) -> ChaosConnection:
        if self._acquire_delay is not None:
            # timeout=None means "wait forever", exactly as asyncpg does —
            # so acquire_delay=math.inf with no timeout models the wedged
            # pool that an unbounded acquire() hangs on indefinitely.
            await asyncio.wait_for(asyncio.sleep(self._acquire_delay), timeout=self._timeout)
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        # Why no release: the wrapped connection is owned by the test that
        # built it (which closes it), not by this pool. A real
        # PoolAcquireContext returns the holder to the pool's queue here;
        # a single-connection double has no queue to return it to.
        pass


class ChaosPool:
    """Pool-like object that yields a :class:`ChaosConnection` from
    ``acquire()``.

    Used to inject a ``ChaosConnection`` into backend methods that acquire
    connections from ``self._worker_pool``.  Temporarily replace
    ``backend._worker_pool`` with a ``ChaosPool`` to test mid-transaction
    failures.

    *acquire_delay* simulates a pool with no free connection: ``acquire()``
    waits that many seconds before yielding.  Combined with the caller's
    ``timeout=``, this is what makes the *bounded-wait* invariant testable
    — a call site that forgets ``timeout=`` hangs here exactly as it would
    against a wedged Postgres, and one that passes it raises
    ``TimeoutError``.  Default ``None`` means a connection is immediately
    available, so ``acquire()`` returns without waiting and ``timeout=``
    can never fire — matching a real pool that is not exhausted, and
    leaving the mid-transaction-failure tests unaffected.

    ``timeout`` was previously accepted and silently discarded, which made
    it impossible for any test built on this pool to observe an acquire
    bound at all.

    Deliberate divergences from ``asyncpg.Pool``, all inherent to a
    single-connection double and none of them silent: there is no holder
    queue (so no contention between concurrent acquirers), no
    ``release()``/reset cycle, no ``closing``/uninitialised state (a real
    pool raises ``InterfaceError`` from ``acquire()`` for those *before*
    it waits, regardless of *timeout*), and no bare-``await`` acquire
    form.  Tests needing any of those want a real pool.
    """

    def __init__(self, chaos_conn: ChaosConnection, *, acquire_delay: float | None = None) -> None:
        self._conn = chaos_conn
        self._acquire_delay = acquire_delay

    def acquire(self, *, timeout: float | None = None) -> _ChaosAcquireCtx:
        # ``timeout`` mirrors asyncpg.Pool.acquire and is honoured against
        # ``acquire_delay``. ASYNC109 is suppressed file-wide via
        # per-file-ignores in pyproject.toml.
        return _ChaosAcquireCtx(self._conn, self._acquire_delay, timeout)
