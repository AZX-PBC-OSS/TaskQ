"""Bounded-wait contract for the PG row locks on ``rate_limit_buckets``.

The token-bucket and GCRA PG acquires serialise on the bucket row with a
blocking ``SELECT … FOR UPDATE`` — with ``rate_limit_pg_fallback_enabled``
defaulting on, a Redis outage funnels ALL admission through these row
locks, exactly when the fleet is already degraded. An unbounded wait
there means one black-holed holder (dead TCP, no FIN — the server reaps
it only via keepalives) stalls that bucket's admission until then: a
silent hang, the worst failure this project ships.

The log-style sliding window in the SAME package is already bounded
(``_acquire_pg_log``: advisory two-tier, fail-closed denial on budget
exhaustion — see ``tests/test_ratelimit_sliding_window_pg_lock.py``).
This module pins the same contract for the two row-lock paths:

1. The row-lock WAIT is bounded: a racer whose server-side
   ``lock_timeout`` fires gets the limiter's DENIAL outcome —
   ``allowed=False`` with a retry hint of one more budget — never an
   unbounded block, never an admission, never a raw driver error.
2. Fail closed: the timed-out racer's upsert never runs — a racer that
   could not read the bucket state can never spend or admit tokens.
3. ``lock_timeout_ms <= 0`` waits indefinitely — the ``lock_timeout``
   GUC convention shared with migrate.py and ``taskq._advisory``.
4. The client-side ``asyncio.wait_for`` backstop bounds the network
   black hole the server-side timeout cannot see.

Mechanics mirrored from ``taskq._advisory``'s contended tier:
``set_config('lock_timeout', ..., true)`` (``SET LOCAL`` semantics, so
the bound dies with the acquire's own transaction — no restore needed,
unlike the enqueue helper whose caller keeps using the transaction), a
savepoint around the lock-taking statements so a 55P03 leaves the
transaction committable, and the backstop outside both.

The unit tests drive the acquires through fake pools that model the
stuck row lock deterministically; the integration tests pin the same
contract against real Postgres (row locks are server state).
"""

import asyncio
import time
from datetime import timedelta

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_base62
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.ratelimit._sliding_window_pg import _acquire_pg_gcra
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema


def _fake_settings() -> WorkerSettings:
    """Settings for the fake-pool unit tests — no connection is ever made."""
    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "taskq_fake"},
    )


def _tb(name: str) -> TokenBucket:
    return TokenBucket(name=name, capacity=10.0, refill_per_second=1.0, backend="postgres")


def _sw(name: str) -> SlidingWindow:
    return SlidingWindow(
        name=name, limit=4, window=timedelta(seconds=1), backend="postgres", style="gcra"
    )


# ── Unit: bounded row-lock wait, fake pool (no PG) ──────────────────────


class _NullSavepoint:
    """async with conn.transaction() stand-in: counts opens, no-ops."""

    def __init__(self, conn: "_RowLockFakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_NullSavepoint":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _RowLockFakeConn:
    """ConnLike stand-in modelling a stuck ``FOR UPDATE`` row lock.

    The bucket-row SELECT raises the raw 55P03 (*select_times_out*: the
    server-side ``lock_timeout`` fired while the row was held by a
    black-holed peer) or returns *select_row* (*granted*). The preseed
    and upsert statements are recorded so the tests can pin the fail
    closed shape; ``set_config`` calls record the GUC budget.
    """

    def __init__(
        self,
        *,
        select_times_out: bool = True,
        select_row: dict[str, object] | None = None,
    ) -> None:
        self.select_times_out = select_times_out
        self.select_row = select_row
        self.savepoint_opens = 0
        self.executed_sql: list[str] = []
        self.fetched_rows: list[str] = []
        self.set_config_values: list[str | None] = []

    def transaction(self) -> _NullSavepoint:
        self.savepoint_opens += 1
        return _NullSavepoint(self)

    async def execute(self, sql: str, *params: object) -> str:
        self.executed_sql.append(sql)
        if "set_config" in sql:
            self.set_config_values.append(str(params[0]) if params else None)
            return "OK"
        return "OK"

    async def fetchrow(self, sql: str, *params: object) -> object:
        if "FOR UPDATE" in sql:
            self.fetched_rows.append(sql)
            if self.select_times_out:
                raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
            assert self.select_row is not None
            return self.select_row
        # The GCRA upsert's RETURNING fetch: any non-None means the row
        # was written.
        self.fetched_rows.append(sql)
        return {"written": 1}


class _BlackHoleRowLockConn(_RowLockFakeConn):
    """Row-lock SELECT that NEVER returns (network black hole: the server
    is unreachable, the statement never completes) — only the
    client-side wait_for backstop can bound this."""

    def __init__(self, select_row: dict[str, object] | None = None) -> None:
        super().__init__(select_times_out=False, select_row=select_row)
        self._gate = asyncio.Event()

    async def fetchrow(self, sql: str, *params: object) -> object:
        if "FOR UPDATE" in sql:
            self.fetched_rows.append(sql)
            await self._gate.wait()
            assert self.select_row is not None
            return self.select_row
        return await super().fetchrow(sql, *params)


class _FakeAcquireCtx:
    def __init__(self, conn: _RowLockFakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RowLockFakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePgPool:
    def __init__(self, conn: _RowLockFakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self._conn)


_TB_ROW: dict[str, object] = {"state": {"tokens": 5.0, "ts": 1000.0}, "now_s": 1000.0}
_GCRA_ROW: dict[str, object] = {"kind": "gcra", "state": {"tat": 1000.0}, "now_s": 1000.0}


class TestTokenBucketRowLockBoundedWaitUnit:
    async def test_lock_timeout_returns_denial_never_admission(self) -> None:
        """A token-bucket acquire whose server-side lock_timeout fires
        gets the limiter's denial outcome — allowed=False with a retry
        hint of exactly one more budget — and the state-mutating upsert
        never ran: fail closed, a racer that could not read the bucket
        never spends or admits tokens."""
        tb = _tb("tb_row_lock_unit")
        conn = _RowLockFakeConn(select_times_out=True)
        start = time.monotonic()
        decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a fake pool cannot satisfy.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=100.0,
        )
        elapsed = time.monotonic() - start
        assert decision.allowed is False
        assert decision.remaining == 0.0
        assert decision.retry_after == timedelta(milliseconds=100.0), (
            "the denial's retry hint is exactly one more budget"
        )
        assert decision.backend == "postgres"
        assert decision.bucket_name == "tb_row_lock_unit"
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        # The GUC budget was set for this transaction.
        assert conn.set_config_values == ["100ms"]
        # One savepoint from the acquire's OWN transaction wrapper, one
        # wrapping the lock-taking statements.
        assert conn.savepoint_opens == 2
        # Fail closed: no upsert — the preseed (DO NOTHING) may have run
        # inside the rolled-back savepoint, but the admission write
        # (DO UPDATE) never executed.
        assert not any("DO UPDATE" in s for s in conn.executed_sql), (
            "a timed-out racer wrote bucket state — the denial must be "
            "fail closed, never an admission."
        )

    async def test_lock_timeout_logs_ratelimit_warning_event(self) -> None:
        """The ``ratelimit-lock-timeout`` log event carries the bucket and
        the expired budget — the same signal the log-style path emits,
        so an operator reads one event name for one condition."""
        tb = _tb("tb_row_lock_unit_log")
        conn = _RowLockFakeConn(select_times_out=True)
        with structlog.testing.capture_logs() as logs:
            decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a fake pool cannot satisfy.
                1.0,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                lock_timeout_ms=250.0,
            )
        assert decision.allowed is False
        entries = [e for e in logs if e.get("event") == "ratelimit-lock-timeout"]
        assert len(entries) == 1, f"expected exactly one timeout event, got {logs!r}"
        assert entries[0].get("bucket_name") == "tb_row_lock_unit_log"
        assert entries[0].get("backend") == "postgres"
        assert entries[0].get("lock_timeout_ms") == 250.0

    async def test_granted_row_lock_proceeds_with_exact_arithmetic(self) -> None:
        """Contended racer granted the row lock inside the budget reads
        the bucket and proceeds: the bound changes nothing about the
        admission arithmetic (5 tokens stored, 1 consumed, 4 remain)."""
        tb = _tb("tb_row_lock_unit_granted")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_TB_ROW)
        decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a fake pool cannot satisfy.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=1000.0,
        )
        assert decision.allowed is True
        assert decision.remaining == 4.0
        assert decision.retry_after == timedelta(0)
        assert conn.set_config_values == ["1000ms"]
        assert conn.savepoint_opens == 2
        assert any("DO UPDATE" in s for s in conn.executed_sql)

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound (the pre-fix
        behavior), matching the ``lock_timeout`` GUC convention — pinned
        by contract, not by waiting forever: the granted fake proves the
        indefinite mode takes the plain blocking read with no GUC
        statements and no savepoint of its own."""
        tb = _tb("tb_row_lock_unit3")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_TB_ROW)
        decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a fake pool cannot satisfy.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=0.0,
        )
        assert decision.allowed is True
        # Only the acquire's OWN transaction wrapper opened — indefinite
        # mode adds no savepoint of its own.
        assert conn.savepoint_opens == 1, "indefinite mode must not open a savepoint"
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"

    async def test_client_backstop_bounds_black_holed_row_lock(self) -> None:
        """A network black hole (the row-lock SELECT never returns) is
        bounded by the client-side wait_for backstop at budget + slack —
        the server-side lock_timeout cannot fire if the server is
        unreachable, so this layer is the only bound left, and the
        outcome is still the fail-closed denial."""
        tb = _tb("tb_row_lock_unit_blackhole")
        conn = _BlackHoleRowLockConn(select_row=_TB_ROW)
        start = time.monotonic()
        decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a fake pool cannot satisfy.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=100.0,
        )
        elapsed = time.monotonic() - start
        assert decision.allowed is False
        assert decision.remaining == 0.0
        assert decision.retry_after == timedelta(milliseconds=100.0)
        # The backstop fires at budget + slack: strictly after the budget,
        # well before any plausible unbounded hang.
        assert elapsed >= 0.5, f"the backstop must outlast the 100ms budget, took {elapsed:.3f}s"
        assert elapsed < 2.0, f"the backstop must bound the black hole, took {elapsed:.3f}s"
        # Fail closed: no admission write.
        assert not any("DO UPDATE" in s for s in conn.executed_sql)


class TestGcraRowLockBoundedWaitUnit:
    async def test_lock_timeout_returns_denial_never_admission(self) -> None:
        """A GCRA acquire whose server-side lock_timeout fires gets the
        limiter's denial outcome with a retry hint of one more budget,
        and the TAT-mutating upsert never ran: a racer that could not
        read the TAT can never advance it."""
        sw = _sw("gcra_row_lock_unit")
        conn = _RowLockFakeConn(select_times_out=True)
        start = time.monotonic()
        decision = await _acquire_pg_gcra(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=100.0,
        )
        elapsed = time.monotonic() - start
        assert decision.allowed is False
        assert decision.remaining == 0.0
        assert decision.retry_after == timedelta(milliseconds=100.0), (
            "the denial's retry hint is exactly one more budget"
        )
        assert decision.backend == "postgres"
        assert decision.bucket_name == "gcra_row_lock_unit"
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        assert conn.set_config_values == ["100ms"]
        assert conn.savepoint_opens == 2
        # Fail closed: the TAT-mutating upsert (a fetchrow — RETURNING)
        # never ran; the preseed (DO NOTHING) may have executed inside
        # the rolled-back savepoint, but no admission write happened on
        # either recorder.
        assert not any("DO UPDATE" in s for s in conn.executed_sql)
        assert not any("DO UPDATE" in s for s in conn.fetched_rows), (
            "a timed-out racer advanced the TAT — the denial must be "
            "fail closed, never an admission."
        )

    async def test_lock_timeout_logs_ratelimit_warning_event(self) -> None:
        """The same ``ratelimit-lock-timeout`` warning, same keys, as the
        token-bucket and log-style paths — one event name for one
        condition across every bounded limiter lock."""
        sw = _sw("gcra_row_lock_unit_log")
        conn = _RowLockFakeConn(select_times_out=True)
        with structlog.testing.capture_logs() as logs:
            decision = await _acquire_pg_gcra(
                sw,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                lock_timeout_ms=250.0,
            )
        assert decision.allowed is False
        entries = [e for e in logs if e.get("event") == "ratelimit-lock-timeout"]
        assert len(entries) == 1, f"expected exactly one timeout event, got {logs!r}"
        assert entries[0].get("bucket_name") == "gcra_row_lock_unit_log"
        assert entries[0].get("backend") == "postgres"
        assert entries[0].get("lock_timeout_ms") == 250.0

    async def test_granted_row_lock_proceeds_with_exact_arithmetic(self) -> None:
        """A contended racer granted inside the budget proceeds through
        the unchanged TAT arithmetic: first acquire on tat=now is allowed
        (allow_at is one window minus one emission in the past)."""
        sw = _sw("gcra_row_lock_unit_granted")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_GCRA_ROW)
        decision = await _acquire_pg_gcra(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=1000.0,
        )
        assert decision.allowed is True
        assert decision.retry_after == timedelta(0)
        assert conn.set_config_values == ["1000ms"]
        assert conn.savepoint_opens == 2
        # The GCRA upsert is a fetchrow (RETURNING), not an execute.
        assert any("DO UPDATE" in s for s in conn.fetched_rows)

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound — the GUC
        convention, pinned by contract: the granted fake proves the
        indefinite mode takes the plain blocking read with no GUC
        statements and no savepoint of its own."""
        sw = _sw("gcra_row_lock_unit3")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_GCRA_ROW)
        decision = await _acquire_pg_gcra(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=0.0,
        )
        assert decision.allowed is True
        assert conn.savepoint_opens == 1, "indefinite mode must not open a savepoint"
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"

    async def test_client_backstop_bounds_black_holed_row_lock(self) -> None:
        """The client-side backstop bounds the GCRA path's network black
        hole the same way — budget + slack, then the fail-closed
        denial."""
        sw = _sw("gcra_row_lock_unit_blackhole")
        conn = _BlackHoleRowLockConn(select_row=_GCRA_ROW)
        start = time.monotonic()
        decision = await _acquire_pg_gcra(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=100.0,
        )
        elapsed = time.monotonic() - start
        assert decision.allowed is False
        assert decision.remaining == 0.0
        assert decision.retry_after == timedelta(milliseconds=100.0)
        assert elapsed >= 0.5, f"the backstop must outlast the 100ms budget, took {elapsed:.3f}s"
        assert elapsed < 2.0, f"the backstop must bound the black hole, took {elapsed:.3f}s"
        # Fail closed: no admission write on either recorder.
        assert not any("DO UPDATE" in s for s in conn.executed_sql)
        assert not any("DO UPDATE" in s for s in conn.fetched_rows)


# ── Integration: real Postgres ──────────────────────────────────────────


class TestRowLockBoundedWaitPg:
    """The row-lock WAIT is bounded under real Postgres contention."""

    pytestmark = pytest.mark.integration

    async def test_token_bucket_row_lock_wait_bounded(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """A token-bucket acquire facing a long-held bucket row gets the
        denial outcome inside its budget instead of blocking until the
        holder finishes, and admits nothing."""
        schema = module_pg_schema.schema_name
        settings = WorkerSettings.load_from_dict(
            {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
        )
        name = f"tb_row_lock_{new_base62()}"
        tb = _tb(name)

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(
                    f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema is fixture-derived; values are $1-bound
                    "VALUES ($1, 'token_bucket', "
                    "jsonb_build_object('tokens', 5.0::float8, "
                    "'ts', EXTRACT(EPOCH FROM clock_timestamp())), clock_timestamp())",
                    name,
                )
                await holder.execute(
                    f'SELECT bucket_name FROM "{schema}".rate_limit_buckets '  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                    "WHERE bucket_name = $1 FOR UPDATE",
                    name,
                )
                start = time.monotonic()
                # An unbounded acquire would block past the 3 s deadline
                # and TimeoutError would escape.
                async with asyncio.timeout(3.0):
                    decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a held peer lock cannot satisfy.
                        1.0, module_pg_pool, settings, lock_timeout_ms=250.0
                    )
                elapsed = time.monotonic() - start

            assert decision.allowed is False
            assert decision.remaining == 0.0
            assert decision.retry_after == timedelta(milliseconds=250.0)
            assert 0.2 <= elapsed < 3.0, f"budget was 250 ms but the wait took {elapsed:.3f}s"

            # Fail closed: the timed-out racer consumed nothing — the
            # holder's seeded 5 tokens are intact once it commits.
        finally:
            await holder.close()

        state = await tb.peek(pg_pool=module_pg_pool, settings=settings)
        assert state.tokens_remaining == 5.0

        # Bounded retry clears: once the holder's transaction ended, a
        # fresh acquire takes the row and admits normally.
        cleared = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
            1.0, module_pg_pool, settings, lock_timeout_ms=250.0
        )
        assert cleared.allowed is True
        assert cleared.remaining == 4.0

    async def test_gcra_row_lock_wait_bounded(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """A GCRA acquire facing a long-held bucket row gets the denial
        outcome inside its budget, advances no TAT, and admits normally
        once the holder's transaction ends."""
        schema = module_pg_schema.schema_name
        settings = WorkerSettings.load_from_dict(
            {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
        )
        name = f"gcra_row_lock_{new_base62()}"
        sw = _sw(name)

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(
                    f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema is fixture-derived; values are $1-bound
                    "VALUES ($1, 'gcra', "
                    "jsonb_build_object('tat', EXTRACT(EPOCH FROM clock_timestamp())), "
                    "clock_timestamp())",
                    name,
                )
                await holder.execute(
                    f'SELECT bucket_name FROM "{schema}".rate_limit_buckets '  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                    "WHERE bucket_name = $1 FOR UPDATE",
                    name,
                )
                async with asyncio.timeout(3.0):
                    decision = await _acquire_pg_gcra(
                        sw, module_pg_pool, settings, lock_timeout_ms=250.0
                    )

            assert decision.allowed is False
            assert decision.remaining == 0.0
            assert decision.retry_after == timedelta(milliseconds=250.0)
        finally:
            await holder.close()

        cleared = await _acquire_pg_gcra(sw, module_pg_pool, settings, lock_timeout_ms=250.0)
        assert cleared.allowed is True
        assert cleared.retry_after == timedelta(0)
