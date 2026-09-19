"""Bounded-wait contract for the PG row locks on ``rate_limit_buckets``.

The token-bucket and GCRA PG acquires serialise on the bucket row with a
blocking ``SELECT … FOR UPDATE`` - with ``rate_limit_pg_fallback_enabled``
defaulting on, a Redis outage funnels ALL admission through these row
locks, exactly when the fleet is already degraded. An unbounded wait
there means one black-holed holder (dead TCP, no FIN - the server reaps
it only via keepalives) stalls that bucket's admission until then: a
silent hang, the worst failure this project ships.

The log-style sliding window in the SAME package is already bounded
(``_acquire_pg_log``: advisory two-tier, fail-closed denial on budget
exhaustion - see ``tests/test_ratelimit_sliding_window_pg_lock.py``).
This module pins the same contract for the two row-lock paths:

1. The row-lock WAIT is bounded: a racer whose server-side
   ``lock_timeout`` fires gets the limiter's DENIAL outcome -
   ``allowed=False`` with a retry hint of one more budget - never an
   unbounded block, never an admission, never a raw driver error.
2. Fail closed: the timed-out racer's upsert never runs - a racer that
   could not read the bucket state can never spend or admit tokens.
3. ``lock_timeout_ms <= 0`` waits indefinitely - the ``lock_timeout``
   GUC convention shared with migrate.py and ``taskq._advisory``.
4. The client-side ``asyncio.wait_for`` backstop bounds the network
   black hole the server-side timeout cannot see.

Mechanics mirrored from ``taskq._advisory``'s contended tier:
``set_config('lock_timeout', ..., true)`` (``SET LOCAL`` semantics, so
the bound dies with the acquire's own transaction - no restore needed,
unlike the enqueue helper whose caller keeps using the transaction), a
savepoint around the lock-taking statements so a 55P03 leaves the
transaction committable, and the backstop outside both.

The unit tests drive the acquires through fake pools that model the
stuck row lock deterministically; the integration tests pin the same
contract against real Postgres (row locks are server state).
"""

import asyncio
import inspect
import time
from collections.abc import Callable
from datetime import timedelta

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_base62
from taskq.connections import lock_budget_command_timeout_secs
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.ratelimit._lock_budget import (
    resolve_sliding_window_lock_timeout_ms,
    resolve_token_bucket_lock_timeout_ms,
)
from taskq.ratelimit._sliding_window_pg import (
    DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS,
    _acquire_pg_gcra,
)
from taskq.ratelimit.token_bucket import DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import (
    _admission_lock_budget_pairs,  # pyright: ignore[reportPrivateUsage]  # Why: the pair-list seam is the exact input open_worker_deps derives the dispatcher pool's bound from - pinning it pins the reconciliation's wiring, not a reimplementation of it.
    open_worker_deps,
)


def _fake_settings() -> WorkerSettings:
    """Settings for the fake-pool unit tests - no connection is ever made."""
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
    """ConnLike stand-in modelling a stuck row lock on the bucket row.

    The fused acquires (token bucket and GCRA) take the bucket
    row's lock in the ``ON CONFLICT (bucket_name) DO UPDATE`` arm, so
    THAT fetchrow is where the bounded wait lands: it raises the raw
    55P03 (*select_times_out*: the server-side ``lock_timeout`` fired
    while the row was held by a black-holed peer) or returns the
    decision row (*granted*). The refund's ``SELECT … FOR UPDATE``
    keeps its own shape (unchanged by the fusion, the refund is the
    cold rollback path). ``set_config`` calls record the GUC budget;
    ``fused_results`` records the fused acquires that RETURNED, a
    timeout records nothing, the fail-closed observable.
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
        self.fused_results: list[dict[str, object]] = []

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
        if "ON CONFLICT (bucket_name) DO UPDATE" in sql:
            self.fetched_rows.append(sql)
            if self.select_times_out:
                raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
            if "tokens_after" in sql:
                # token-bucket fused acquire: the final token count and
                # the decision bit the statement carried home.
                row = {"tokens_after": 4.0, "granted": True}
            else:
                # GCRA fused acquire: the advanced TAT and the statement
                # clock the arithmetic used.
                row = {"new_tat": 1000.25, "now_s": 1000.0}
            self.fused_results.append(row)
            return row
        if "FOR UPDATE" in sql:
            self.fetched_rows.append(sql)
            if self.select_times_out:
                raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
            assert self.select_row is not None
            return self.select_row
        self.fetched_rows.append(sql)
        return {"written": 1}


class _BlackHoleRowLockConn(_RowLockFakeConn):
    """Row-lock statement that NEVER returns (network black hole: the
    server is unreachable, the statement never completes), only the
    client-side wait_for backstop can bound this."""

    def __init__(self, select_row: dict[str, object] | None = None) -> None:
        super().__init__(select_times_out=False, select_row=select_row)
        self._gate = asyncio.Event()

    async def fetchrow(self, sql: str, *params: object) -> object:
        if "ON CONFLICT (bucket_name) DO UPDATE" in sql or "FOR UPDATE" in sql:
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
        gets the limiter's denial outcome - allowed=False with a retry
        hint of exactly one more budget - and the state-mutating upsert
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
        # Only the acquire's OWN transaction wrapper opened: the fused
        # statement needs no savepoint of its own (a refusal aborts the
        # transaction outright and the bound dies with it).
        assert conn.savepoint_opens == 1
        # Fail closed: the fused acquire never RETURNED, a statement that
        # raised spent and admitted nothing (statement-level atomicity).
        assert conn.fused_results == [], (
            "a timed-out racer wrote bucket state - the denial must be "
            "fail closed, never an admission."
        )

    async def test_lock_timeout_logs_ratelimit_warning_event(self) -> None:
        """The ``ratelimit-lock-timeout`` log event carries the bucket and
        the expired budget - the same signal the log-style path emits,
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
        assert conn.savepoint_opens == 1
        assert len(conn.fused_results) == 1, "the fused acquire returned the decision row"
        assert conn.fused_results[0]["granted"] is True

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound (the pre-fix
        behavior), matching the ``lock_timeout`` GUC convention - pinned
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
        # Indefinite mode opens NO transaction at all: the fused
        # statement is one autocommit round trip, its row lock ending
        # with the statement.
        assert conn.savepoint_opens == 0, (
            "indefinite mode must not open a transaction, the pre-fused "
            "shape needed one to span preseed + read + upsert; the fused "
            "statement does not"
        )
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"

    async def test_client_backstop_bounds_black_holed_row_lock(self) -> None:
        """A network black hole (the row-lock SELECT never returns) is
        bounded by the client-side wait_for backstop at budget + slack -
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
        # Fail closed: the fused acquire never RETURNED.
        assert conn.fused_results == []


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
        assert conn.savepoint_opens == 1
        # Fail closed: the fused upsert never RETURNED, a statement that
        # raised advanced no TAT and admitted nothing.
        assert conn.fused_results == [], (
            "a timed-out racer advanced the TAT - the denial must be "
            "fail closed, never an admission."
        )

    async def test_lock_timeout_logs_ratelimit_warning_event(self) -> None:
        """The same ``ratelimit-lock-timeout`` warning, same keys, as the
        token-bucket and log-style paths - one event name for one
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
        assert conn.savepoint_opens == 1
        assert len(conn.fused_results) == 1, "the fused upsert returned the advanced TAT"

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound - the GUC
        convention, pinned by contract: the granted fake proves the
        indefinite mode is ONE autocommit statement with no GUC
        statements and no transaction of its own."""
        sw = _sw("gcra_row_lock_unit3")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_GCRA_ROW)
        decision = await _acquire_pg_gcra(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=0.0,
        )
        assert decision.allowed is True
        assert conn.savepoint_opens == 0, (
            "indefinite mode must not open a transaction, the pre-fused "
            "shape needed one to span preseed + read + upsert; the fused "
            "statement does not"
        )
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"

    async def test_client_backstop_bounds_black_holed_row_lock(self) -> None:
        """The client-side backstop bounds the GCRA path's network black
        hole the same way - budget + slack, then the fail-closed
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
        # Fail closed: the fused upsert never RETURNED.
        assert conn.fused_results == []


# ── Unit: bounded refund row-lock wait, fake pool (no PG) ───────────────


class TestTokenBucketRefundRowLockBoundedWaitUnit:
    """The token-bucket PG refund's ``FOR UPDATE`` wait is bounded by the
    same discipline as the acquire - with the opposite exhaustion
    semantics: a refund that could not take the row lock RAISES, because
    ``_refund_pg`` returns ``None`` on success and a silent no-op return
    on budget exhaustion would make a lost refund look like a completed
    one (tokens stay spent - for a fixed-quota bucket, permanently)."""

    async def test_lock_timeout_raises_never_silent_noop(self) -> None:
        """A refund whose server-side lock_timeout fires raises the
        driver error - never returns ``None`` as though the refund
        happened - and the state-mutating UPDATE never ran."""
        tb = _tb("tb_refund_row_lock_unit")
        conn = _RowLockFakeConn(select_times_out=True)
        start = time.monotonic()
        with pytest.raises(asyncpg.LockNotAvailableError):
            await tb._refund_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the refund is the unit under test; the public surface wraps it in machinery a fake pool cannot satisfy.
                1.0,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                lock_timeout_ms=100.0,
            )
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        # The GUC budget was set for this transaction.
        assert conn.set_config_values == ["100ms"]
        # One savepoint from the refund's OWN transaction wrapper, one
        # wrapping the lock-taking read.
        assert conn.savepoint_opens == 2
        # Never a silent success: the refund UPDATE never executed.
        assert not any("UPDATE" in s for s in conn.executed_sql), (
            "a timed-out refund wrote bucket state or returned quietly - a "
            "refund failure must never look like a success"
        )

    async def test_lock_timeout_logs_ratelimit_warning_event(self) -> None:
        """The refund's lock-timeout emits the same ``ratelimit-lock-timeout``
        event as the acquire paths - one event name for one condition -
        with ``phase="refund"`` marking the different consequence (a lost
        refund, not a denied admission)."""
        tb = _tb("tb_refund_row_lock_unit_log")
        conn = _RowLockFakeConn(select_times_out=True)
        with (
            structlog.testing.capture_logs() as logs,
            pytest.raises(asyncpg.LockNotAvailableError),
        ):
            await tb._refund_pg(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
                1.0,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                lock_timeout_ms=250.0,
            )
        entries = [e for e in logs if e.get("event") == "ratelimit-lock-timeout"]
        assert len(entries) == 1, f"expected exactly one timeout event, got {logs!r}"
        assert entries[0].get("bucket_name") == "tb_refund_row_lock_unit_log"
        assert entries[0].get("backend") == "postgres"
        assert entries[0].get("lock_timeout_ms") == 250.0
        assert entries[0].get("phase") == "refund"

    async def test_granted_row_lock_refunds_with_exact_arithmetic(self) -> None:
        """A refund granted the row lock inside the budget proceeds through
        the unchanged arithmetic: elapsed refill applied, then +1 capped
        at capacity (5 stored + 1 refunded = 6)."""
        tb = _tb("tb_refund_row_lock_unit_granted")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_TB_ROW)
        await tb._refund_pg(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=1000.0,
        )
        assert conn.set_config_values == ["1000ms"]
        assert conn.savepoint_opens == 2
        assert any("UPDATE" in s for s in conn.executed_sql), (
            "the granted refund must write the refunded state"
        )

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound (the pre-bound
        behavior), matching the ``lock_timeout`` GUC convention - pinned
        by contract: the granted fake proves the indefinite mode takes
        the plain blocking read with no GUC statements and no savepoint
        of its own."""
        tb = _tb("tb_refund_row_lock_unit3")
        conn = _RowLockFakeConn(select_times_out=False, select_row=_TB_ROW)
        await tb._refund_pg(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            lock_timeout_ms=0.0,
        )
        # Only the refund's OWN transaction wrapper opened - indefinite
        # mode adds no savepoint of its own.
        assert conn.savepoint_opens == 1, "indefinite mode must not open a savepoint"
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"
        assert any("UPDATE" in s for s in conn.executed_sql)

    async def test_client_backstop_bounds_black_holed_row_lock(self) -> None:
        """A network black hole (the refund's row-lock SELECT never
        returns) is bounded by the client-side wait_for backstop at
        budget + slack, and the outcome is still a RAISE - the refund
        never completes silently."""
        tb = _tb("tb_refund_row_lock_unit_blackhole")
        conn = _BlackHoleRowLockConn(select_row=_TB_ROW)
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            await tb._refund_pg(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
                1.0,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                lock_timeout_ms=100.0,
            )
        elapsed = time.monotonic() - start
        # The backstop fires at budget + slack: strictly after the budget,
        # well before any plausible unbounded hang.
        assert elapsed >= 0.5, f"the backstop must outlast the 100ms budget, took {elapsed:.3f}s"
        assert elapsed < 2.0, f"the backstop must bound the black hole, took {elapsed:.3f}s"
        assert not any("UPDATE" in s for s in conn.executed_sql), (
            "a backstopped refund must not write state - it raised instead"
        )


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

            # Fail closed: the timed-out racer consumed nothing - the
            # holder's seeded 5 tokens are intact once it commits.
        finally:
            await holder.close()

        state = await tb.peek(pg_pool=module_pg_pool, settings=settings)
        assert state.tokens_remaining == 5.0

        # Elapsed-zero the cleared window: the holder's INSERT stamped the
        # bucket's ts at seed time, and the pinned elapsed-accrual contract
        # (test_ratelimit_token_bucket_pg.py::test_pg_burst_throttle_refill)
        # makes every real acquire fold refill since that stamp into
        # remaining - the exact 5.0 -> 4.0 arithmetic below is only
        # assertable with the window rewound to the acquire's own read
        # (the same _rewind_bucket_ts discipline the token-bucket PG
        # suite uses for time travel).
        async with module_pg_pool.acquire() as _rewind_conn:
            await _rewind_conn.execute(
                f'UPDATE "{module_pg_schema.schema_name}".rate_limit_buckets '  # noqa: S608  # Why: schema is fixture-derived; values are $-bound.
                f"SET state = jsonb_set(state, '{{ts}}', "
                f"to_jsonb(EXTRACT(EPOCH FROM clock_timestamp()))) "
                f"WHERE bucket_name = $1",
                name,
            )

        # Bounded retry clears: once the holder's transaction ended, a
        # fresh acquire takes the row and admits normally.
        cleared = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
            1.0, module_pg_pool, settings, lock_timeout_ms=250.0
        )
        assert cleared.allowed is True
        # abs tolerance, constraint named: the rewind and the acquire are
        # two separate round trips, so real wall-clock time (a few ms at
        # 1 token/s refill) accrues between them - the pinned elapsed
        # accrual contract makes that mandatory. The tolerance covers only
        # that inter-statement gap; the pre-rewind shape failed at 4.26
        # (the whole 250 ms lock budget's accrual), an order past it.
        assert cleared.remaining == pytest.approx(4.0, abs=0.1)

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


# ── Operator surface: the row-lock budgets are WorkerSettings knobs ──────
#
# The budgets above are correct but frozen: DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS
# and DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS are module constants reachable only
# through private keyword arguments that no production caller passes. Every
# dispatch arm in sliding_window.py and token_bucket.py calls the PG acquire and
# refund with the settings object but no budget, so the constant is the only
# source.
#
# That matters because these budgets bound admission, not a background sweep.
# With rate_limit_pg_fallback_enabled on, a Redis outage funnels the whole
# fleet's admission through these row locks; budget exhaustion is a fail-closed
# DENIAL that sheds load, and on the refund side it raises. An operator riding
# out a slow-holder incident needs to widen the budget, and one riding out a
# latency incident needs to narrow it, and today neither is possible without a
# code change and a redeploy.
#
# The knobs are spelled the way the enqueue path's budgets are spelled (see
# tests/test_enqueue_lock_budget_settings.py): a WorkerSettings field under the
# TASKQ_ env prefix, defaulting to today's constant so wiring the surface cannot
# change the shipped ceiling, and read at the PG use site -- where a real
# settings object is already in hand and already consulted for schema_name.


#: Operator budgets distinct from each other and from the 5 s default, so a
#: wiring that falls back to the constant, or cross-wires the two knobs onto
#: one parameter, fails instead of passing by coincidence.
_OPERATOR_TOKEN_BUCKET_BUDGET_MS = 150.0
_OPERATOR_SLIDING_WINDOW_BUDGET_MS = 250.0


def _operator_settings(**overrides: object) -> WorkerSettings:
    """Fake-pool settings carrying operator-set lock budgets.

    ``load_from_dict`` is hermetic (no dotfiles, no process env) and silently
    ignores unknown keys, so a missing field loads clean and the value simply
    never arrives -- which is the failure these tests catch.
    """
    base: dict[str, object] = {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "taskq_fake"}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


class TestRowLockBudgetsAreOperatorSettings:
    """The PG rate-limiter lock budgets are WorkerSettings fields, and the
    value an operator sets is the value the row-lock wait actually uses."""

    def test_lock_budgets_are_worker_settings_fields(self) -> None:
        """Both rate-limiter budgets exist as WorkerSettings fields, the
        operator surface for the PG admission row locks."""
        # WorkerSettings is a dotenvmodel DotEnvConfig, not a pydantic
        # BaseModel: get_fields() is its introspection seam, mapping
        # name -> (type, FieldInfo).
        fields = WorkerSettings.get_fields()
        missing = [
            name
            for name in ("token_bucket_lock_timeout_ms", "sliding_window_lock_timeout_ms")
            if name not in fields
        ]
        assert not missing, (
            f"WorkerSettings has no {missing} -- the PG rate-limiter row-lock "
            "wait budgets are hard-coded module constants "
            "(DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS in taskq/ratelimit/"
            "token_bucket.py, DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS in "
            "taskq/ratelimit/_sliding_window_pg.py, 5 s each) that no "
            "production caller overrides, so an operator cannot tune them. "
            "The enqueue path's three budgets already have this surface; "
            "the admission path does not."
        )

    def test_lock_budgets_load_from_the_taskq_env_prefix(self) -> None:
        """Both budgets round-trip through their TASKQ_* env keys, so the
        env var an operator sets during an incident is the value the
        settings object carries."""
        s = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://u:p@h/d",
                "TASKQ_TOKEN_BUCKET_LOCK_TIMEOUT_MS": f"{_OPERATOR_TOKEN_BUCKET_BUDGET_MS:g}",
                "TASKQ_SLIDING_WINDOW_LOCK_TIMEOUT_MS": f"{_OPERATOR_SLIDING_WINDOW_BUDGET_MS:g}",
            },
        )
        expectations = (
            (
                "token_bucket_lock_timeout_ms",
                "TASKQ_TOKEN_BUCKET_LOCK_TIMEOUT_MS",
                _OPERATOR_TOKEN_BUCKET_BUDGET_MS,
            ),
            (
                "sliding_window_lock_timeout_ms",
                "TASKQ_SLIDING_WINDOW_LOCK_TIMEOUT_MS",
                _OPERATOR_SLIDING_WINDOW_BUDGET_MS,
            ),
        )
        for name, env_var, expected in expectations:
            loaded = getattr(s, name, None)
            assert loaded == expected, (
                f"{env_var}={expected:g} did not reach WorkerSettings.{name} "
                f"(got {loaded!r}): the field does not exist, so the env var "
                "an operator sets to retune an admission lock budget is "
                "silently ignored."
            )

    def test_lock_budget_settings_default_to_the_shipped_constants(self) -> None:
        """The knobs' defaults are today's constants, so wiring the settings
        surface cannot silently change the shipped ceiling for a deployment
        that never sets the env var."""
        defaults = (
            ("token_bucket_lock_timeout_ms", DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS),
            ("sliding_window_lock_timeout_ms", DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS),
        )
        for name, constant in defaults:
            entry = WorkerSettings.get_fields().get(name)
            assert entry is not None, (
                f"WorkerSettings has no {name!r} field -- the {constant:g} ms "
                "bounded-wait budget is a hard-coded constant with no "
                "operator surface."
            )
            _type, info = entry
            assert info.default == constant, (
                f"WorkerSettings.{name} defaults to {info.default!r}, not the "
                f"shipped {constant:g} ms constant -- wiring the knob must "
                "preserve today's ceiling or every deployment that does not "
                "set the env var changes behavior on upgrade."
            )

    @pytest.mark.parametrize(
        ("resolve", "field"),
        [
            (resolve_token_bucket_lock_timeout_ms, "token_bucket_lock_timeout_ms"),
            (resolve_sliding_window_lock_timeout_ms, "sliding_window_lock_timeout_ms"),
        ],
        ids=["token_bucket", "sliding_window"],
    )
    def test_settings_double_lacking_the_knob_fails_loud(
        self,
        resolve: Callable[[float | None, WorkerSettings | None, float], float],
        field: str,
    ) -> None:
        """A settings object missing the budget field raises AttributeError
        at the resolution seam rather than silently falling back to the
        shipped constant: a stale or mis-spelled settings double must
        surface as a loud failure, not as the operator's knob being
        ignored while the wait pretends all is well."""

        class _PreKnobSettings:
            """A settings double from before the knobs existed: it carries
            schema_name (the other field the PG paths read) but not the
            lock-budget fields."""

            schema_name = "taskq_fake"

        with pytest.raises(AttributeError, match=field):
            resolve(
                None,
                _PreKnobSettings(),  # type: ignore[arg-type]  # Why: the double deliberately lacks the field - its absence is the behaviour under test.
                DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS,
            )

    async def test_operator_budget_governs_the_token_bucket_acquire_wait(self) -> None:
        """A token-bucket PG acquire called the way production calls it,
        with no explicit budget, bounds its row-lock wait at the operator's
        configured budget: the server-side lock_timeout GUC carries it and
        the denial's retry hint reports it."""
        tb = _tb("tb_row_lock_operator_budget")
        conn = _RowLockFakeConn(select_times_out=True)
        settings = _operator_settings(token_bucket_lock_timeout_ms=_OPERATOR_TOKEN_BUCKET_BUDGET_MS)
        decision = await tb._acquire_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the acquire is the unit under test; the public surface wraps it in Redis-fallback machinery a fake pool cannot satisfy.
            1.0,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            settings,
            # No lock_timeout_ms override: this is the exact call shape
            # TokenBucket.acquire's postgres arm uses in production.
        )
        assert decision.allowed is False
        assert conn.set_config_values == [f"{round(_OPERATOR_TOKEN_BUCKET_BUDGET_MS)}ms"], (
            f"the server-side lock_timeout GUC was set to "
            f"{conn.set_config_values!r}, not the operator's "
            f"{_OPERATOR_TOKEN_BUCKET_BUDGET_MS:g} ms budget -- the wait real "
            "Postgres enforces is still the frozen module default, because "
            "the acquire never reads a budget off the settings object it was "
            "handed."
        )
        assert decision.retry_after == timedelta(milliseconds=_OPERATOR_TOKEN_BUCKET_BUDGET_MS), (
            "the denial's retry hint must be one more of the OPERATOR's "
            "budget: a hint derived from the frozen default tells callers to "
            "back off for a window the limiter no longer waits."
        )

    async def test_operator_budget_governs_the_gcra_acquire_wait(self) -> None:
        """The GCRA PG acquire honors the sliding-window budget from
        settings under the same no-override call shape, so both admission
        styles share one operator knob rather than one being tunable and
        the other frozen."""
        sw = _sw("gcra_row_lock_operator_budget")
        conn = _RowLockFakeConn(select_times_out=True)
        settings = _operator_settings(
            sliding_window_lock_timeout_ms=_OPERATOR_SLIDING_WINDOW_BUDGET_MS
        )
        decision = await _acquire_pg_gcra(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            settings,
            # No lock_timeout_ms override: the shape sliding_window.py's
            # postgres/gcra dispatch arm uses.
        )
        assert decision.allowed is False
        assert conn.set_config_values == [f"{round(_OPERATOR_SLIDING_WINDOW_BUDGET_MS)}ms"], (
            f"the server-side lock_timeout GUC was set to "
            f"{conn.set_config_values!r}, not the operator's "
            f"{_OPERATOR_SLIDING_WINDOW_BUDGET_MS:g} ms budget."
        )
        assert decision.retry_after == timedelta(milliseconds=_OPERATOR_SLIDING_WINDOW_BUDGET_MS), (
            "the denial's retry hint must be one more of the OPERATOR's budget"
        )

    async def test_operator_budget_governs_the_token_bucket_refund_wait(self) -> None:
        """The refund's row-lock wait honors the same operator budget as
        the acquire.

        The refund is the arm most easily left behind by a partial fix: it
        is a separate method with its own frozen default, and it is the arm
        where exhaustion RAISES rather than denying. A refund left on the
        frozen budget while the acquire is tuned down means spent tokens
        sit unreturned for a window the operator thought they had shortened,
        and for a fixed-quota bucket an unreturned token is gone for good.
        """
        tb = _tb("tb_refund_row_lock_operator_budget")
        conn = _RowLockFakeConn(select_times_out=True)
        settings = _operator_settings(token_bucket_lock_timeout_ms=_OPERATOR_TOKEN_BUCKET_BUDGET_MS)
        with pytest.raises(asyncpg.LockNotAvailableError):
            await tb._refund_pg(  # pyright: ignore[reportPrivateUsage]  # Why: the refund is the unit under test; the public surface wraps it in machinery a fake pool cannot satisfy.
                1.0,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                settings,
                # No lock_timeout_ms override: the shape TokenBucket.refund's
                # postgres arm uses in production.
            )
        assert conn.set_config_values == [f"{round(_OPERATOR_TOKEN_BUCKET_BUDGET_MS)}ms"], (
            f"the refund's server-side lock_timeout GUC was set to "
            f"{conn.set_config_values!r}, not the operator's "
            f"{_OPERATOR_TOKEN_BUCKET_BUDGET_MS:g} ms budget -- the refund arm "
            "still reads the frozen module default even once the acquire arm "
            "is plumbed."
        )


class TestAdmissionLockBudgetReDerivesTheDispatcherPoolBound:
    """A widened admission lock budget re-derives the dispatcher pool's own
    client-side ``command_timeout`` upward, so the pool's per-statement
    timer can never truncate the operator's wider server-side budget.

    ``dispatcher_pool`` is the pool the admission-path acquires actually
    run on in production (``taskq.worker.deps.open_worker_deps`` builds it,
    and ``_leader_sweeps.py`` / the rate-limit registry run their PG
    acquires against exactly this pool). The acquires wrap the contended
    tier in ``asyncio.wait_for(..., timeout=lock_timeout_ms / 1000 +
    slack)`` - but that ``wait_for`` is layered OUTSIDE asyncpg's own
    per-statement ``command_timeout`` enforcement, which fires
    independently on the connection itself. Left unreconciled, a budget
    widened past the pool's bound is silently truncated by it: the
    connection's timer fires before the operator's wider server-side
    ``lock_timeout`` can produce the fail-closed denial with its retry
    hint.

    The reconciliation these tests pin: ``open_worker_deps`` derives the
    dispatcher pool's bound through
    ``taskq.connections.lock_budget_command_timeout_secs`` from the
    ``(configured, shipped-default)`` pairs that
    ``_admission_lock_budget_pairs`` reads off the two admission budget
    fields, with ``settings.dispatcher_command_timeout`` as the floor -
    the same machinery the client pool applies to the enqueue budgets
    (``taskq.client._taskq._ENQUEUE_LOCK_BUDGET_FIELDS``). At the shipped
    defaults the derived bound IS the configured value, so a deployment
    that sets nothing keeps byte-identical behavior; a budget widened
    past its default raises the bound to ``budget / 0.8``, so the
    server-side ``lock_timeout`` refusal always fires a fifth of the
    bound ahead of the pool's client-side timer. The leader/notify
    dedicated connections keep the CONFIGURED value: no admission
    acquire runs on them.

    There is deliberately NO settings-level cross-field validator here:
    refusing a valid configuration would be strictly weaker than
    honouring it. (This class previously pinned the pre-reconciliation
    defect - a widened budget silently accepted and then truncated at
    run time; its negative form is ungreenable now that the derivation
    exists, and its own failure message instructed this rewrite.)
    """

    def test_shipped_defaults_keep_the_pool_bound_byte_identical(self) -> None:
        """A deployment that sets nothing gets exactly the pre-knob pool
        bound: the pairs carry each budget's shipped constant as BOTH the
        configured and default value, and the derivation returns the
        configured floor untouched."""
        settings = _operator_settings()
        pairs = _admission_lock_budget_pairs(settings)
        assert pairs == [
            (DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS, DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS),
            (DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS, DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS),
        ], (
            "the derivation input drifted from the shipped constants - the "
            "pairs must carry (configured, shipped default) per admission "
            "budget, read off the settings fields, so a widened budget is "
            "detected against its own default"
        )
        derived = lock_budget_command_timeout_secs(
            pairs, floor_secs=settings.dispatcher_command_timeout
        )
        assert derived == settings.dispatcher_command_timeout == 5.0, (
            "at the shipped defaults the derived dispatcher pool bound must "
            "BE the configured dispatcher_command_timeout - the "
            "reconciliation exists to deliver WIDENED budgets and must not "
            "move the bound for a deployment that sets nothing"
        )

    @pytest.mark.parametrize(
        ("field", "widened_ms"),
        [
            ("token_bucket_lock_timeout_ms", 8000.0),
            ("sliding_window_lock_timeout_ms", 9000.0),
        ],
        ids=["token_bucket", "sliding_window"],
    )
    def test_widened_admission_budget_re_derives_a_bound_that_cannot_truncate_it(
        self, field: str, widened_ms: float
    ) -> None:
        """A budget widened past its 5000ms shipped default re-derives the
        pool bound to ``budget / 0.8``: the server-side ``lock_timeout``
        fires at exactly the 0.8 share of the bound, so the pool's
        client-side timer always loses the race and the denial (with its
        retry hint) is what the caller sees. The two budgets use distinct
        widened values so a cross-wiring onto one parameter fails instead
        of passing by coincidence."""
        settings = _operator_settings(**{field: f"{widened_ms:g}"})
        derived = lock_budget_command_timeout_secs(
            _admission_lock_budget_pairs(settings),
            floor_secs=settings.dispatcher_command_timeout,
        )
        expected_secs = widened_ms / 1000.0 / 0.8
        assert derived == expected_secs, (
            f"a widened {field}={widened_ms:g}ms must re-derive the "
            f"dispatcher pool's command_timeout to {expected_secs:g}s "
            f"(budget / 0.8); got {derived:g}s - left at the "
            f"{settings.dispatcher_command_timeout:g}s floor, the pool's own "
            "client-side timer would fire first and silently truncate the "
            "operator's wider server-side budget"
        )
        assert derived * 1000.0 * 0.8 == widened_ms, (
            "the widened budget occupies exactly its share of the derived "
            "bound - the server-side refusal fires a fifth of the bound "
            "ahead of the pool's client-side timer"
        )
        assert derived > settings.dispatcher_command_timeout

    def test_two_widened_budgets_raise_the_bound_to_the_wider_one(self) -> None:
        """With both budgets widened, the bound follows the WIDER budget -
        the narrower one then fits inside the same bound unclamped."""
        settings = _operator_settings(
            token_bucket_lock_timeout_ms="8000",
            sliding_window_lock_timeout_ms="12000",
        )
        derived = lock_budget_command_timeout_secs(
            _admission_lock_budget_pairs(settings),
            floor_secs=settings.dispatcher_command_timeout,
        )
        assert derived == 12000.0 / 1000.0 / 0.8 == 15.0

    def test_narrowed_or_unbounded_budgets_never_lower_the_pool_bound(self) -> None:
        """A narrowed budget, ``0``, or a negative value (both spell an
        unbounded server-side wait under the ``lock_timeout`` GUC
        convention) never moves the bound off the configured floor: the
        floor already delivers a narrowed budget, and an unbounded wait
        cannot fit inside any finite bound - dropping the pool's
        per-query bound for it would remove the black-hole guard every
        other statement relies on."""
        for value in ("1000", "0", "-1"):
            settings = _operator_settings(
                token_bucket_lock_timeout_ms=value,
                sliding_window_lock_timeout_ms=value,
            )
            derived = lock_budget_command_timeout_secs(
                _admission_lock_budget_pairs(settings),
                floor_secs=settings.dispatcher_command_timeout,
            )
            assert derived == settings.dispatcher_command_timeout, (
                f"budgets of {value}ms must leave the pool bound at the "
                f"configured floor; got {derived:g}s"
            )

    def test_open_worker_deps_applies_the_derived_bound_to_the_dispatcher_pool(
        self,
    ) -> None:
        """The derivation is wired, not dead code: ``open_worker_deps``
        feeds the derived value into the dispatcher pool factory's
        ``command_timeout``. A refactor that stopped applying it (the
        original defect's shape - machinery present, admission fields
        never wired in) fails here even though the derivation's own unit
        tests still pass."""
        src = inspect.getsource(open_worker_deps)
        assert "lock_budget_command_timeout_secs(" in src, (
            "open_worker_deps no longer derives any pool bound from the lock budgets"
        )
        assert "_admission_lock_budget_pairs(settings)" in src, (
            "open_worker_deps no longer derives the dispatcher pool bound "
            "from the admission lock budgets - a widened "
            "token_bucket_lock_timeout_ms / sliding_window_lock_timeout_ms "
            "is silently truncated by the pool's own command_timeout again"
        )
        assert "command_timeout=dispatcher_pool_command_timeout" in src, (
            "the dispatcher pool factory no longer receives the derived "
            "bound as its command_timeout"
        )
