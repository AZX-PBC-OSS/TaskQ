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
- Defers all other semantics (transaction management, type codecs,
  connection lifecycle) to the wrapped connection.
- The ``transaction()`` method delegates to the real connection so
  asyncpg's transaction management (commit / rollback) works correctly
  when ``ChaosException`` is raised inside a transaction block.
"""

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
    """Async context manager yielded by :class:`ChaosPool.acquire`."""

    def __init__(self, conn: ChaosConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> ChaosConnection:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class ChaosPool:
    """Pool-like object that yields a :class:`ChaosConnection` from
    ``acquire()``.

    Used to inject a ``ChaosConnection`` into backend methods that acquire
    connections from ``self._worker_pool``.  Temporarily replace
    ``backend._worker_pool`` with a ``ChaosPool`` to test mid-transaction
    failures.
    """

    def __init__(self, chaos_conn: ChaosConnection) -> None:
        self._conn = chaos_conn

    def acquire(self, *, timeout: float | None = None) -> _ChaosAcquireCtx:
        # ``timeout`` mirrors asyncpg.Pool.acquire so ChaosPool can substitute
        # for a real pool in heartbeat tests that pass timeout=. ASYNC109 is
        # suppressed file-wide via per-file-ignores in pyproject.toml.
        return _ChaosAcquireCtx(self._conn)
