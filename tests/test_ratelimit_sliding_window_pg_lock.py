"""Bounded-wait contract for the log-style PG sliding-window bucket lock.

``_acquire_pg_log`` serialises its per-bucket window work, one fused
prune + admission-insert + count statement, behind a
transaction-scoped advisory lock keyed ``taskq:{schema}:sw:{name}``. This
module pins three properties:

1. The lock WAIT is bounded: a racer that cannot acquire the lock within
   its budget gets the limiter's DENIAL outcome (``allowed=False`` with a
   retry hint) - never an unbounded block, never an admission, never a
   raw driver error. Unlike the enqueue path's max_pending lock (which
   raises a typed backpressure error), the limiter's denial channel is a
   return value: the dispatch layer already converts ``allowed=False``
   into a snooze and re-promotion, so a lock-timeout denial is shed load,
   not a failure.
2. Contended racers QUEUE SERVER-SIDE: once the holder releases inside the
   budget, the acquire proceeds and the window is enforced exactly.
3. The window stays EXACT under concurrency - the lock serialises the
   racers, and bounding the wait never loosens the limit.

The acquire is TWO-TIER (``acquire_advisory_xact_lock_bounded``, imported
from ``taskq._advisory`` - the one helper shared with the enqueue
branch's bounded locks, so every bounded lock wait has the same
mechanics): one ``pg_try_advisory_xact_lock`` statement when
uncontended, a savepoint-scoped server-side bounded blocking acquire
(``set_config('lock_timeout', ..., true)`` + ``pg_advisory_xact_lock`` +
restore) when contended, and a client-side ``asyncio.wait_for`` backstop
for the network black hole.

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
import structlog.testing

from taskq._ids import new_base62, new_uuid
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_pg import _acquire_pg_log
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema

_HOLD_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"


def _fake_settings() -> WorkerSettings:
    """Settings for the fake-pool unit tests - no connection is ever made."""
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


class _NullSavepoint:
    """async with conn.transaction() stand-in: counts opens, no-ops."""

    def __init__(self, conn: "_ContendedFakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_NullSavepoint":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _ContendedFakeConn:
    """ConnLike stand-in modelling deterministic advisory-lock contention.

    Two-tier shapes: the fast-path ``pg_try_advisory_xact_lock`` returns
    *try_lock_result* (False models a holder owning the bucket lock at
    that instant); the contended tier's BLOCKING ``pg_advisory_xact_lock``
    inside the savepoint raises the raw 55P03 (*blocking_times_out*: the
    server-side ``lock_timeout`` fired) or succeeds (*granted*: the
    server-side queue handed the lock over once the holder released).
    ``set_config``/``current_setting`` round trips are recorded so the
    tests can pin the GUC save/restore shape.
    """

    def __init__(
        self,
        *,
        try_lock_result: bool = False,
        blocking_times_out: bool = True,
    ) -> None:
        self.try_lock_result = try_lock_result
        self.blocking_times_out = blocking_times_out
        self.try_lock_calls = 0
        self.blocking_lock_calls = 0
        self.savepoint_opens = 0
        self.executed_sql: list[str] = []
        self.fetched_rows: list[str] = []
        self.set_config_values: list[str | None] = []

    def transaction(self) -> _NullSavepoint:
        self.savepoint_opens += 1
        return _NullSavepoint(self)

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return self.try_lock_result
        if "current_setting" in sql:
            return "0"
        raise AssertionError(f"unexpected fetchval on the log acquire path: {sql}")

    async def execute(self, sql: str, *params: object) -> str:
        self.executed_sql.append(sql)
        if "set_config" in sql:
            self.set_config_values.append(str(params[0]) if params else None)
            return "OK"
        if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
            self.blocking_lock_calls += 1
            if self.blocking_times_out:
                raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
            return "OK"
        return "OK"

    async def fetchrow(self, sql: str, *params: object) -> object:
        self.fetched_rows.append(sql)
        if "WITH pruned AS" in sql:
            # The fused log-style acquire: one row carrying the whole
            # decision (insert landed, pre-insert in-window count, and
            # the denial hint's inputs, unused on the allowed path).
            return {
                "inserted": True,
                "count_in_window": 1,
                "oldest_ts": None,
                "server_now": None,
            }
        return None


class _BlackHoleFakeConn(_ContendedFakeConn):
    """Contended tier whose blocking acquire NEVER returns (network black
    hole: the server is unreachable, the statement never completes) - only
    the client-side wait_for backstop can bound this."""

    def __init__(self) -> None:
        super().__init__(blocking_times_out=False)
        self._gate = asyncio.Event()

    async def execute(self, sql: str, *params: object) -> str:
        if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
            self.blocking_lock_calls += 1
            await self._gate.wait()
            return "OK"
        return await super().execute(sql, *params)


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
        """A racer whose server-side lock_timeout fires gets the limiter's
        denial outcome - allowed=False with a retry hint of exactly one
        more budget - and the window-mutating statements never ran: fail
        closed, a racer that could not check the window never
        over-admits."""
        sw = _sw("sw_lock_unit")
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
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
        assert decision.retry_after == timedelta(milliseconds=100.0), (
            "the denial's retry hint is exactly one more budget"
        )
        assert decision.backend == "postgres"
        assert decision.bucket_name == "sw_lock_unit"
        assert decision.request_id == str(request_id)
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        # Two-tier shape: one try-lock, then the savepoint tier. Only the
        # SET ran - the timeout raised before any restore, and the
        # savepoint ROLLBACK undoes the set itself.
        assert conn.try_lock_calls == 1
        # One savepoint from the acquire's OWN transaction wrapper, one
        # from the contended tier inside it.
        assert conn.savepoint_opens == 2
        assert conn.blocking_lock_calls == 1
        assert conn.set_config_values == ["100ms"]
        # Fail closed: no admission slot was written or evicted: the
        # fused statement (prune + admission insert in one) never ran.
        assert not any("WITH pruned AS" in s for s in conn.fetched_rows)

    async def test_lock_timeout_logs_ratelimit_warning_event(self) -> None:
        """The ``ratelimit-lock-timeout`` log event carries the bucket and
        the expired budget - the signal that separates a contended or sick
        bucket (or holder) from a merely busy one."""
        sw = _sw("sw_lock_unit_log")
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
        with structlog.testing.capture_logs() as logs:
            decision = await _acquire_pg_log(
                sw,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                new_uuid(),
                lock_timeout_ms=250.0,
            )
        assert decision.allowed is False
        entries = [e for e in logs if e.get("event") == "ratelimit-lock-timeout"]
        assert len(entries) == 1, f"expected exactly one timeout event, got {logs!r}"
        assert entries[0].get("bucket_name") == "sw_lock_unit_log"
        assert entries[0].get("backend") == "postgres"
        assert entries[0].get("lock_timeout_ms") == 250.0

    async def test_settings_lock_timeout_is_honored_by_default_call_shape(self) -> None:
        """The settings object is the only budget wire under the
        production call shape: ``SlidingWindow.acquire`` calls
        ``_acquire_pg_log(self, pg_pool, settings, request_id)`` with NO
        ``lock_timeout_ms`` kwarg, so the operator-configured budget on
        ``WorkerSettings.sliding_window_lock_timeout_ms`` must govern the
        server-side ``lock_timeout`` GUC and the denial's retry hint -
        not the 5000 ms module default
        (``DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS``), which applies only
        when no settings object is in hand.
        """
        sw = _sw("sw_lock_settings_budget")
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
        settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": "postgresql://u:p@h/d",
                "schema_name": "taskq_fake",
                # A short budget far from the 5000 ms module default, so a
                # wiring that ignores the knob fails the assertions below
                # instead of passing by coincidence.
                "sliding_window_lock_timeout_ms": 150.0,
            },
        )
        start = time.monotonic()
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            settings,
            new_uuid(),
            # No lock_timeout_ms override: this is the exact call shape
            # SlidingWindow.acquire uses in production.
        )
        elapsed = time.monotonic() - start
        assert decision.allowed is False
        assert decision.retry_after == timedelta(milliseconds=150.0), (
            "the operator-configured 150ms budget on settings should have "
            "governed the wait, not the 5000ms module default"
        )
        assert conn.set_config_values == ["150ms"], (
            "the server-side lock_timeout GUC should reflect the "
            "settings-provided budget, not DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS"
        )
        assert elapsed < 2.0, (
            f"budget should have been 150ms (from settings) but the wait "
            f"took {elapsed:.3f}s, consistent with the 5000ms module default "
            f"still governing"
        )

    async def test_lock_acquired_after_contention_enforces_window(self) -> None:
        """Contended racers queue server-side: once the holder releases
        inside the budget, the blocking acquire is granted and the acquire
        proceeds through the locked window sequence."""
        sw = _sw("sw_lock_unit2")
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=False)
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            new_uuid(),
            lock_timeout_ms=1000.0,
        )
        assert decision.allowed is True
        assert decision.retry_after == timedelta(0)
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 1
        # One savepoint from the acquire's OWN transaction wrapper, one
        # from the contended tier inside it.
        assert conn.savepoint_opens == 2
        # Granted path: the budget was set, then restored to the prior
        # value BEFORE the savepoint RELEASE.
        assert conn.set_config_values == ["1000ms", "0"]
        # The fused statement is the whole locked critical section: the
        # prune DELETE and the admission INSERT both live in it.
        assert any("DELETE FROM" in s and "WITH pruned AS" in s for s in conn.fetched_rows)
        assert any("INSERT INTO" in s and "WITH pruned AS" in s for s in conn.fetched_rows)

    async def test_fast_path_uncontended_is_single_try_lock_statement(self) -> None:
        """Happy-path round-trip parity: an uncontended acquire issues
        exactly ONE advisory-lock statement (the try-lock) and never
        touches the savepoint/GUC machinery - identical to the pre-bounded
        statement count, so the bound costs the common case nothing."""
        sw = _sw("sw_lock_unit_fast")
        conn = _ContendedFakeConn(try_lock_result=True)
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            new_uuid(),
            lock_timeout_ms=5000.0,
        )
        assert decision.allowed is True
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 0
        # Only the acquire's OWN transaction wrapper opened - the
        # contended tier never ran.
        assert conn.savepoint_opens == 1
        assert conn.set_config_values == []
        # The fused statement (prune + admission insert in one) is the
        # only work statement under the lock.
        assert any("INSERT INTO" in s and "WITH pruned AS" in s for s in conn.fetched_rows)

    async def test_lock_timeout_budget_zero_waits_indefinitely(self) -> None:
        """``lock_timeout_ms <= 0`` disables the bound (the pre-fix
        behavior), matching the enqueue lock's ``lock_timeout`` GUC
        convention - pinned by contract, not by waiting forever: the
        fast-path try-lock never succeeds on this fake, so only an
        UNBOUNDED server-side wait reaches the admission, and the
        indefinite mode must take it with no savepoint and no GUC
        statements."""
        sw = _sw("sw_lock_unit3")
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=False)
        async with asyncio.timeout(5.0):
            decision = await _acquire_pg_log(
                sw,
                _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
                _fake_settings(),
                new_uuid(),
                lock_timeout_ms=0.0,
            )
        assert decision.allowed is True
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 1
        # Only the acquire's OWN transaction wrapper opened - indefinite
        # mode adds no savepoint of its own.
        assert conn.savepoint_opens == 1, "indefinite mode must not open a savepoint"
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"

    async def test_client_backstop_bounds_black_holed_blocking_acquire(self) -> None:
        """A network black hole (the blocking-acquire statement never
        returns) is bounded by the client-side wait_for backstop at
        budget + slack - the server-side lock_timeout cannot fire if the
        server is unreachable, so this layer is the only bound left, and
        the outcome is still the fail-closed denial."""
        sw = _sw("sw_lock_unit_blackhole")
        conn = _BlackHoleFakeConn()
        start = time.monotonic()
        decision = await _acquire_pg_log(
            sw,
            _FakePgPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool stand-in
            _fake_settings(),
            new_uuid(),
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
        # Fail closed: no admission slot was written or evicted: the
        # fused statement (prune + admission insert in one) never ran.
        assert not any("WITH pruned AS" in s for s in conn.fetched_rows)


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
        its budget instead of blocking until the holder finishes - the
        server-side lock_timeout fires at the budget, precisely."""
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
                # An unbounded acquire would trip the 3 s deadline and
                # TimeoutError would escape.
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
            assert decision.retry_after == timedelta(milliseconds=250.0)
            assert decision.request_id == str(request_id)
            # The server-side timeout is precise: ~budget, not the 3 s wall
            # bound and not instant.
            assert 0.2 <= elapsed < 3.0, f"budget was 250 ms but the wait took {elapsed:.3f}s"

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

    async def test_refund_after_lock_timeout_denial_is_noop(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """Refunding a lock-timeout denial must be a no-op: nothing was
        admitted (fail closed), so the refund's request_id-keyed DELETE
        matches no row and the bucket's state is untouched - the released
        slot a caller might expect never existed."""
        schema = module_pg_schema.schema_name
        settings = _settings(module_pg_schema)
        name = f"sw_lock_refund_{new_base62()}"
        sw = _sw(name, limit=10)
        lock_key = f"taskq:{schema}:sw:{name}"

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(_HOLD_LOCK_SQL, lock_key)
                async with asyncio.timeout(3.0):
                    denied = await _acquire_pg_log(
                        sw,
                        module_pg_pool,
                        settings,
                        new_uuid(),
                        lock_timeout_ms=250.0,
                    )
            assert denied.allowed is False

            # The release path refunds the decision it was handed; for a
            # lock-timeout denial there is nothing to remove.
            await sw.refund(denied, pg_pool=module_pg_pool, settings=settings)

            stored = await module_pg_pool.fetchval(
                f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                f"WHERE bucket_name = $1",
                name,
            )
            assert stored == 0

            # And the bucket still works: the next acquire admits normally.
            allowed = await _acquire_pg_log(
                sw, module_pg_pool, settings, new_uuid(), lock_timeout_ms=250.0
            )
            assert allowed.allowed is True
        finally:
            await holder.close()

    async def test_contended_acquire_restores_lock_timeout_guc(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """GUC non-leakage: a contended acquire sets ``lock_timeout`` inside
        a savepoint and MUST restore the prior value before releasing the
        savepoint (``SET LOCAL`` effects persist through RELEASE), so a
        LATER lock acquire in the same transaction does not inherit the
        stale bound - pinned with a real PG transaction doing two acquires
        of the helper directly (``_acquire_pg_log`` owns its whole
        transaction, so the helper is the level at which two acquires
        share one): the second acquire waits LONGER than the first
        acquire's whole budget and must still succeed."""
        from taskq._advisory import acquire_advisory_xact_lock_bounded

        schema = module_pg_schema.schema_name
        key_a = f"taskq:{schema}:sw:guc_a_{new_base62()}"
        key_b = f"taskq:{schema}:sw:guc_b_{new_base62()}"
        first_budget_ms = 200.0

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:

            async def _hold_briefly(key: str, hold_s: float, held: asyncio.Event) -> None:
                async with holder.transaction():
                    await holder.execute(_HOLD_LOCK_SQL, key)
                    held.set()
                    await asyncio.sleep(hold_s)
                # Transaction exit releases the advisory lock.

            async with module_pg_pool.acquire() as conn, conn.transaction():
                # First acquire: contended (holder signaled before the
                # racer's first try-lock), granted at ~50 ms inside the
                # 200 ms budget.
                held_a = asyncio.Event()
                hold_a = asyncio.create_task(_hold_briefly(key_a, 0.05, held_a))
                await held_a.wait()
                assert await acquire_advisory_xact_lock_bounded(
                    conn, key_a, timeout_ms=first_budget_ms
                )
                await hold_a
                # The GUC reads back as the session default inside the
                # SAME transaction (restore happened before RELEASE).
                current = await conn.fetchval("SELECT current_setting('lock_timeout')")
                assert current == "0", f"stale lock_timeout leaked: {current!r}"

                # Second acquire in the SAME transaction, on a key held
                # for 300 ms > first_budget_ms: with the stale 200 ms
                # bound this would raise 55P03 at ~200 ms; restored, it
                # succeeds at ~300 ms.
                held_b = asyncio.Event()
                hold_b = asyncio.create_task(_hold_briefly(key_b, 0.30, held_b))
                await held_b.wait()
                async with asyncio.timeout(5.0):
                    assert await acquire_advisory_xact_lock_bounded(conn, key_b, timeout_ms=5000.0)
                await hold_b
        finally:
            await holder.close()


class TestSlidingWindowExactUnderConcurrency:
    """Pin: the per-bucket lock keeps the window EXACT under concurrency -
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
        racers (the server-side queue drains one racer per holder critical
        section), so denials here are window denials - the lock never
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
