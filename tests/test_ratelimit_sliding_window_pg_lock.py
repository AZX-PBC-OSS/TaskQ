"""Bounded-wait contract for the log-style PG sliding-window bucket lock.

``_acquire_pg_log`` serialises its per-bucket DELETE / count+INSERT behind
a transaction-scoped advisory lock keyed ``taskq:{schema}:sw:{name}``. This
module pins three properties:

1. The lock WAIT is bounded: a racer that cannot acquire the lock within
   its budget gets the limiter's DENIAL outcome (``allowed=False`` with a
   retry hint) — never an unbounded block, never an admission, never a
   raw driver error. Unlike the enqueue path's max_pending lock (which
   raises a typed backpressure error), the limiter's denial channel is a
   return value: the dispatch layer already converts ``allowed=False``
   into a snooze and re-promotion, so a lock-timeout denial is shed load,
   not a failure.
2. Contended racers RETRY: once the holder releases inside the budget,
   the acquire proceeds and the window is enforced exactly.
3. The window stays EXACT under concurrency — the lock serialises the
   racers, and bounding the wait never loosens the limit.

The unit tests drive ``_acquire_pg_log`` through a fake pool that models
contention deterministically; the integration tests pin the same contract
against real Postgres (advisory locks are server state, so the contention
behavior can only be fully observed there).
"""

import asyncio
import time
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_pg import _acquire_pg_log
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema

_HOLD_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"


def _fake_settings() -> WorkerSettings:
    """Settings for the fake-pool unit tests — no connection is ever made."""
    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "taskq_fake"},
    )


def _settings(module_pg_schema: ModulePgSchema) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": module_pg_schema.schema_name},
    )


def _sw(name: str, *, limit: int = 5) -> SlidingWindow:
    return SlidingWindow(
        name=name,
        limit=limit,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )


# ── Unit: bounded wait, fake pool (no PG) ────────────────────────────────


class _ContendedFakeConn:
    """ConnLike stand-in modelling deterministic advisory-lock contention.

    ``pg_try_advisory_xact_lock`` fails until *success_after* calls have
    been made (``None`` = never), and a BLOCKING ``pg_advisory_xact_lock``
    (the pre-fix statement shape) raises the raw 55P03 so a regression
    back to the unbounded blocking acquire is loud instead of silently
    green — same guard as the enqueue lock's fake connection.
    """

    def __init__(self, *, success_after: int | None = None) -> None:
        self.try_lock_calls = 0
        self.success_after = success_after
        self.executed_sql: list[str] = []
        self.fetched_rows: list[str] = []

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return self.success_after is not None and self.try_lock_calls > self.success_after
        raise AssertionError(f"unexpected fetchval on the log acquire path: {sql}")

    async def execute(self, sql: str, *params: object) -> str:
        self.executed_sql.append(sql)
        if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
            raise asyncpg.LockNotAvailableError("simulated lock contention")
        return "OK"

    async def fetchrow(self, sql: str, *params: object) -> object:
        self.fetched_rows.append(sql)
        if "INSERT INTO" in sql:
            return {"1": 1}
        if "count(*)" in sql:
            return {"count": 1}
        return None

    def transaction(self) -> "_NullTxn":
        return _NullTxn()


class _NullTxn:
    async def __aenter__(self) -> "_NullTxn":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeAcquireCtx:
    def __init__(self, conn: _ContendedFakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _ContendedFakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePgPool:
    def __init__(self, conn: _ContendedFakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self._conn)


class TestSlidingWindowLockBoundedWaitUnit:
    """The bounded-wait contract, driven through a fake pool."""

    async def test_lock_timeout_returns_denial_never_admission(self) -> None:
        """A racer that never acquires the lock inside its budget gets the
        limiter's denial outcome — allowed=False with a retry hint — and
        the window-mutating statements never ran: fail closed, a racer
        that could not check the window never over-admits."""
        sw = _sw("sw_lock_unit")
        conn = _ContendedFakeConn(success_after=None)
        request_id = new_uuid()
        start = time.monotonic()
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            request_id,
            lock_timeout_ms=100.0,
        )
        elapsed = time.monotonic() - start
        assert decision.allowed is False
        assert decision.remaining == 0.0
        assert decision.retry_after is not None
        assert decision.retry_after > timedelta(0)
        assert decision.backend == "postgres"
        assert decision.bucket_name == "sw_lock_unit"
        assert decision.request_id == str(request_id)
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        # Fail closed: no admission slot was written or evicted.
        assert not any("INSERT INTO" in s for s in conn.fetched_rows)
        assert not any("DELETE FROM" in s for s in conn.executed_sql)

    async def test_lock_acquired_after_contention_enforces_window(self) -> None:
        """Contended racers RETRY: once the holder releases inside the
        budget, the acquire proceeds through the locked window sequence."""
        sw = _sw("sw_lock_unit2")
        conn = _ContendedFakeConn(success_after=2)
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            new_uuid(),
            lock_timeout_ms=1000.0,
        )
        assert decision.allowed is True
        assert decision.retry_after == timedelta(0)
        assert conn.try_lock_calls == 3  # two contended polls, then the acquire
        assert any("DELETE FROM" in s for s in conn.executed_sql)
        assert any("INSERT INTO" in s for s in conn.fetched_rows)

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound (the pre-fix
        behavior), matching the enqueue lock's ``lock_timeout`` GUC
        convention — pinned by contract, not by waiting forever: the loop
        must keep polling past the point where a bounded racer would have
        given up."""
        sw = _sw("sw_lock_unit3")
        # Contention clears on the 5th poll — well past a 20 ms budget, so
        # only an unbounded loop can reach the admission.
        conn = _ContendedFakeConn(success_after=4)
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            new_uuid(),
            lock_timeout_ms=0.0,
        )
        assert decision.allowed is True
        assert conn.try_lock_calls == 5


# ── Integration: real Postgres ───────────────────────────────────────────


class TestSlidingWindowLockBoundedWait:
    """The advisory-lock WAIT is bounded under real Postgres contention."""

    pytestmark = pytest.mark.integration

    async def test_lock_wait_bounded_under_real_held_lock(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """A racer facing a long-held lock gets the denial outcome inside
        its budget instead of blocking until the holder finishes. On the
        pre-fix blocking acquire the unbounded block trips the 3 s
        asyncio.timeout and TimeoutError escapes — the RED."""
        schema = module_pg_schema.schema_name
        settings = _settings(module_pg_schema)
        name = f"sw_lock_{new_base62()}"
        sw = _sw(name, limit=10)
        lock_key = f"taskq:{schema}:sw:{name}"
        request_id = new_uuid()

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(_HOLD_LOCK_SQL, lock_key)
                start = time.monotonic()
                # Nested, not comma-joined: asyncio.timeout is an async CM.
                async with asyncio.timeout(3.0):
                    decision = await _acquire_pg_log(
                        sw,
                        module_pg_pool,
                        settings,
                        request_id,
                        lock_timeout_ms=250.0,
                    )
                elapsed = time.monotonic() - start

            assert decision.allowed is False
            assert decision.remaining == 0.0
            assert decision.retry_after is not None
            assert decision.retry_after > timedelta(0)
            assert decision.request_id == str(request_id)
            assert elapsed < 3.0, f"budget was 250 ms but the wait took {elapsed:.3f}s"

            # Fail closed: the timed-out racer admitted nothing.
            stored = await module_pg_pool.fetchval(
                f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                f"WHERE bucket_name = $1",
                name,
            )
            assert stored == 0

            # Bounded retry clears: once the holder's transaction ends, a
            # fresh acquire takes the lock and admits normally.
            cleared = await _acquire_pg_log(
                sw,
                module_pg_pool,
                settings,
                new_uuid(),
                lock_timeout_ms=250.0,
            )
            assert cleared.allowed is True
            stored_after = await module_pg_pool.fetchval(
                f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                f"WHERE bucket_name = $1",
                name,
            )
            assert stored_after == 1
        finally:
            await holder.close()


class TestSlidingWindowExactUnderConcurrency:
    """Pin: the per-bucket lock keeps the window EXACT under concurrency —
    bounding the wait never loosens the limit."""

    pytestmark = pytest.mark.integration

    async def test_exact_window_under_concurrent_racers(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """limit=5 on a fresh bucket, 20 concurrent racers: exactly 5
        admitted, the stored in-window count exactly 5, every denial
        carrying a retry hint. The default budget covers 20 serialized
        racers, so denials here are window denials — the lock never
        becomes the bottleneck the bound has to shed."""
        schema = module_pg_schema.schema_name
        settings = _settings(module_pg_schema)
        name = f"sw_lock_exact_{new_base62()}"
        sw = _sw(name, limit=5)

        decisions = await asyncio.gather(
            *[sw.acquire(pg_pool=module_pg_pool, settings=settings) for _ in range(20)]
        )

        allowed_count = sum(1 for d in decisions if d.allowed)
        denied = [d for d in decisions if not d.allowed]
        assert allowed_count == 5, (
            f"exactly limit=5 of 20 concurrent acquires must be admitted, got {allowed_count}"
        )
        assert len(denied) == 15
        assert all(d.retry_after is not None and d.retry_after > timedelta(0) for d in denied)
        assert all(d.remaining == 0.0 for d in denied)

        stored = await module_pg_pool.fetchval(
            f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            name,
        )
        assert stored == 5, f"the in-window entry count must never exceed limit=5, got {stored}"
