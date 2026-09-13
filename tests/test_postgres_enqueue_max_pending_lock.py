"""Bounded-wait contract for the capped-actor max_pending advisory lock.

Single enqueues against a capped actor serialize the count-then-insert per
(schema, actor) behind a transaction-scoped advisory lock. This module pins
two properties:

1. The cap is EXACT under concurrency (the lock serializes the racers).
2. The lock WAIT is bounded: a racer that cannot acquire the lock within
   its budget gets the typed backpressure error -- never a raw asyncpg
   error, and never an unbounded block.

The acquire is TWO-TIER (``taskq._advisory.acquire_advisory_xact_lock_bounded``):
an uncontended racer takes the fast path (one ``pg_try_advisory_xact_lock``
statement, identical round-trip count to the pre-bound era), while a
contended racer falls back to a server-side bounded blocking acquire --
``SAVEPOINT`` + ``set_config('lock_timeout', ..., true)`` +
``pg_advisory_xact_lock`` + restore -- so Postgres' lock scheduler queues
the waiters and hands the lock off at the rate the holders release it,
instead of a client-side poll loop sleeping between attempts. A
client-side ``asyncio.wait_for`` backstop bounds the network-black-hole
case (a server that never answers) that the server-side timeout cannot.

The unit tests drive :func:`_enqueue_on_conn` through a fake connection
that models contention deterministically; the integration tests pin the
same contract against real Postgres (advisory locks are server state, so
the exactness and contention behavior can only be fully observed there).
"""

import asyncio
import dataclasses
import time
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
import structlog.testing

import taskq.backend._enqueue as _enqueue_mod
from taskq.backend._enqueue import _enqueue, _enqueue_on_conn
from taskq.backend._sql_templates import render
from taskq.backend.clock import SystemClock
from taskq.exceptions import (
    BackpressureError,
    MaxPendingExceededError,
    MaxPendingLockTimeoutError,
)
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.jobs import make_enqueue_args, make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)

_CAPPED_ACTOR = "test_actor"
_CAP = 5

_HOLD_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"


def _capped_args() -> object:
    """EnqueueArgs carrying the cap literal (the single path's own input)."""
    return dataclasses.replace(make_enqueue_args(actor=_CAPPED_ACTOR), max_pending=_CAP)


def _inserted_record() -> dict[str, object]:
    """A RETURNING * shaped record for the fake conn's INSERT arm."""
    return asdict(make_job_row(status="pending", actor=_CAPPED_ACTOR))


class _NullSavepoint:
    """async with conn.transaction() stand-in: counts opens, no-ops."""

    def __init__(self, conn: "_ContendedFakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_NullSavepoint":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _ContendedFakeConn:
    """ConnLike stand-in for the BYO-transaction capped-enqueue path.

    Models a caller-owned transaction (``is_in_transaction() -> True``, so
    ``_enqueue_on_conn`` takes the no-wrap branch unchanged) and the
    TWO-TIER acquire's statement shapes:

    - fast path: ``pg_try_advisory_xact_lock`` returns *try_lock_result*
      (False models a holder owning the lock at that instant);
    - contended tier: the blocking ``pg_advisory_xact_lock`` inside the
      savepoint either raises the raw 55P03 (*blocking_times_out*: the
      server-side ``lock_timeout`` fired) or succeeds (*granted*: the
      server-side queue handed the lock over once the holder released);
    - ``set_config``/``current_setting`` round trips are recorded so the
      tests can pin the exact GUC save/restore shape (and its absence in
      the indefinite and fast-path modes).
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
        self.set_config_values: list[str | None] = []
        self._release_gate: asyncio.Event | None = None

    def is_in_transaction(self) -> bool:
        return True

    def transaction(self) -> _NullSavepoint:
        self.savepoint_opens += 1
        return _NullSavepoint(self)

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return self.try_lock_result
        if "current_setting" in sql:
            # The GUC save: the pre-acquire value on a session that never
            # set one (PG's default lock_timeout is 0 = off).
            return "0"
        # enqueue_max_pending_count — reached only after the lock is held.
        return 0

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

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object]:
        return _inserted_record()


class _BlackHoleFakeConn(_ContendedFakeConn):
    """Contended tier whose blocking acquire NEVER returns (network black
    hole: the server is unreachable, the statement never completes) — only
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


def _spy_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, str]]:
    """Capture record_backpressure_error calls made by the enqueue module."""
    recorded: list[tuple[str, str]] = []

    def _record(actor: str, *, kind: str = "max_pending") -> None:
        recorded.append((actor, kind))

    monkeypatch.setattr(_enqueue_mod, "record_backpressure_error", _record)
    return recorded


# ── Unit: bounded wait, fake conn (no PG) ────────────────────────────────


class TestMaxPendingLockBoundedWaitUnit:
    """The bounded-wait contract, driven through a fake connection."""

    async def test_lock_timeout_raises_typed_backpressure_within_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A contended racer whose server-side lock_timeout fires gets the
        typed backpressure error, never the raw driver error, with the
        two-tier statement shape: one try-lock, then the savepoint tier
        setting the budget and restoring the prior GUC."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
        args = _capped_args()
        start = time.monotonic()
        with pytest.raises(BackpressureError) as exc_info:
            await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                max_pending_lock_timeout_ms=100.0,
            )
        elapsed = time.monotonic() - start
        assert isinstance(exc_info.value, MaxPendingLockTimeoutError)
        assert exc_info.value.timeout_ms == 100.0
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        assert recorded == [(_CAPPED_ACTOR, "max_pending_lock_timeout")]
        # Two-tier shape: the fast path tried once, then the savepoint tier.
        assert conn.try_lock_calls == 1
        assert conn.savepoint_opens == 1
        assert conn.blocking_lock_calls == 1
        # Only the SET ran: the timeout raised before the restore, and the
        # savepoint ROLLBACK undoes the GUC set itself (a restore statement
        # after the failed acquire would hit "current transaction is
        # aborted") — the restore-before-RELEASE is the SUCCESS path's job.
        assert conn.set_config_values == ["100ms"]

    async def test_lock_timeout_logs_warning_event_with_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ``max-pending-lock-timeout`` log event carries the actor and
        the expired budget — the observability for an exhaustion that must
        NOT be silently conflated with a cap rejection."""
        _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
        with (
            structlog.testing.capture_logs() as logs,
            pytest.raises(MaxPendingLockTimeoutError),
        ):
            await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                _capped_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                max_pending_lock_timeout_ms=250.0,
            )
        entries = [e for e in logs if e.get("event") == "max-pending-lock-timeout"]
        assert len(entries) == 1, f"expected exactly one timeout event, got {logs!r}"
        assert entries[0].get("actor") == _CAPPED_ACTOR
        assert entries[0].get("lock_timeout_ms") == 250.0

    async def test_lock_acquired_after_contention_clears_within_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Contended racers queue server-side: once the holder releases
        inside the budget, the blocking acquire is granted and the enqueue
        proceeds to completion — the pre-poll drain rate restored."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=False)
        args = _capped_args()
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
            max_pending_lock_timeout_ms=1000.0,
        )
        assert row.actor == _CAPPED_ACTOR
        # Fast path failed once; the savepoint tier acquired once.
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 1
        assert conn.savepoint_opens == 1
        assert conn.set_config_values == ["1000ms", "0"]
        assert recorded == []

    async def test_fast_path_uncontended_is_single_try_lock_statement(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path round-trip parity: an uncontended capped enqueue
        issues exactly ONE advisory-lock statement (the try-lock) and never
        touches the savepoint/GUC machinery — identical to the pre-bound
        statement count, so the redesign costs the common case nothing."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=True)
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            _capped_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
            max_pending_lock_timeout_ms=5000.0,
        )
        assert row.actor == _CAPPED_ACTOR
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 0
        assert conn.savepoint_opens == 0
        assert conn.set_config_values == []
        assert recorded == []

    async def test_lock_timeout_budget_zero_waits_indefinitely(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``timeout_ms <= 0`` disables the bound (the pre-fix behavior),
        matching the ``lock_timeout`` GUC convention used by migrate.py: a
        plain blocking acquire with NO savepoint and NO GUC statements —
        only an unbounded server-side wait reaches the admission."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=False)
        args = _capped_args()
        # The wall bound proves the fast path could not have admitted (the
        # try-lock never succeeds on this fake); only the unbounded
        # blocking acquire can.
        async with asyncio.timeout(5.0):
            row = await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                max_pending_lock_timeout_ms=0.0,
            )
        assert row.actor == _CAPPED_ACTOR
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 1
        assert conn.savepoint_opens == 0, "indefinite mode must not open a savepoint"
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"
        assert recorded == []

    async def test_client_backstop_bounds_black_holed_blocking_acquire(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A network black hole (the blocking-acquire statement never
        returns) is bounded by the client-side wait_for backstop at
        budget + slack — the server-side lock_timeout cannot fire if the
        server is unreachable, so this layer is the only bound left."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _BlackHoleFakeConn()
        start = time.monotonic()
        with pytest.raises(MaxPendingLockTimeoutError) as exc_info:
            await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                _capped_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                max_pending_lock_timeout_ms=100.0,
            )
        elapsed = time.monotonic() - start
        assert exc_info.value.timeout_ms == 100.0
        # The backstop fires at budget + slack: strictly after the budget,
        # well before any plausible unbounded hang.
        assert elapsed >= 0.5, f"the backstop must outlast the 100ms budget, took {elapsed:.3f}s"
        assert elapsed < 2.0, f"the backstop must bound the black hole, took {elapsed:.3f}s"
        assert recorded == [(_CAPPED_ACTOR, "max_pending_lock_timeout")]

    async def test_uncapped_enqueue_takes_no_advisory_lock(self) -> None:
        """max_pending=None (the default) never touches the advisory lock —
        the bounded-wait machinery is scoped to capped actors only."""
        conn = _ContendedFakeConn(try_lock_result=True)
        args = make_enqueue_args(actor=_CAPPED_ACTOR)
        assert args.max_pending is None
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            args,  # type: ignore[arg-type]  # Why: make_enqueue_args returns the EnqueueArgs type
        )
        assert row.actor == _CAPPED_ACTOR
        assert conn.try_lock_calls == 0
        assert conn.blocking_lock_calls == 0
        assert not any("pg_advisory" in s for s in conn.executed_sql)


# ── Integration: real Postgres ───────────────────────────────────────────


class _CountingConn:
    """Delegating ConnLike that records every public statement's SQL, so
    integration tests can pin the two-tier acquire's statement shape on a
    real connection (fast path = one try-lock; contended tier = the
    set_config pair inside the savepoint)."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self.sql_log: list[str] = []

    def is_in_transaction(self) -> bool:
        return self._conn.is_in_transaction()

    def transaction(self) -> Any:
        return self._conn.transaction()

    async def fetchval(self, sql: str, *params: object) -> object:
        self.sql_log.append(sql)
        return await self._conn.fetchval(sql, *params)

    async def fetchrow(self, sql: str, *params: object) -> object:
        self.sql_log.append(sql)
        return await self._conn.fetchrow(sql, *params)

    async def execute(self, sql: str, *params: object) -> str:
        self.sql_log.append(sql)
        return await self._conn.execute(sql, *params)


class TestMaxPendingCapExactness:
    """Pin: the per-(schema, actor) advisory lock makes the single-path cap
    EXACT under concurrency."""

    pytestmark = pytest.mark.integration

    async def test_cap_exact_under_concurrent_enqueue(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        sql = render(schema)
        racers = [_capped_args() for _ in range(50)]
        results = await asyncio.gather(
            *[_enqueue(deps.worker_pool, sql, schema, SystemClock(), args) for args in racers],
            return_exceptions=True,
        )
        admitted = [r for r in results if not isinstance(r, BaseException)]
        rejected = [
            r for r in results if isinstance(r, BaseException) and isinstance(r, BackpressureError)
        ]
        other_errors = [
            r
            for r in results
            if isinstance(r, BaseException) and not isinstance(r, BackpressureError)
        ]
        assert other_errors == []
        assert len(admitted) == _CAP
        assert len(rejected) == len(racers) - _CAP
        # The rejected errors are cap rejections, not lock timeouts: the
        # default budget comfortably covers 50 serialized racers (the
        # server-side queue drains one racer per holder critical section).
        assert all(
            isinstance(r, MaxPendingExceededError) for r in results if isinstance(r, BaseException)
        )
        stored = await deps.worker_pool.fetchval(f'SELECT count(*) FROM "{schema}".jobs')  # noqa: S608  # Why: human-readable row-count check, not a SQL query; ruff's SQL-injection heuristic false-positives on f-string interpolation.
        assert stored == _CAP


class TestMaxPendingLockBoundedWait:
    """The advisory-lock WAIT is bounded under real Postgres contention."""

    pytestmark = pytest.mark.integration

    async def test_lock_wait_bounded_under_real_contention(
        self, clean_jobs_app: JobsApp, module_pg_schema: object
    ) -> None:
        """A racer facing a long-held lock fails with the typed backpressure
        error inside its budget instead of blocking until the holder
        finishes — the server-side lock_timeout fires at the budget and the
        savepoint rollback leaves the pool connection's transaction usable
        for its own rollback."""
        pg_schema: ModulePgSchema = module_pg_schema  # type: ignore[assignment]  # Why: fixture is typed ModulePgSchema; object keeps the test signature loose like test_backend_enqueue_with_conn
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        sql = render(schema)
        lock_key = f"taskq:max_pending:{schema}:{_CAPPED_ACTOR}"
        holder = await asyncpg.connect(pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(_HOLD_LOCK_SQL, lock_key)
                start = time.monotonic()
                with pytest.raises(BackpressureError) as exc_info:
                    # Nested, not comma-joined: asyncio.timeout is an async
                    # CM. An unbounded acquire would trip the 3 s deadline
                    # and TimeoutError would escape pytest.raises.
                    async with asyncio.timeout(3.0):
                        await _enqueue(
                            deps.worker_pool,
                            sql,
                            schema,
                            SystemClock(),
                            _capped_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                            max_pending_lock_timeout_ms=250.0,
                        )
                elapsed = time.monotonic() - start
            assert isinstance(exc_info.value, MaxPendingLockTimeoutError)
            assert not isinstance(exc_info.value, MaxPendingExceededError), (
                "the cap was not reached — the rejection must be the lock budget"
            )
            assert exc_info.value.timeout_ms == 250.0
            # The server-side timeout is precise: ~budget, not the 3 s wall
            # bound and not instant.
            assert 0.2 <= elapsed < 3.0, f"budget was 250 ms but the wait took {elapsed:.3f}s"
            # The timed-out racer's transaction rolled back: nothing stored.
            stored = await deps.worker_pool.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: human-readable row-count check, not a SQL query
            )
            assert stored == 0
        finally:
            await holder.close()

    async def test_contended_acquire_restores_lock_timeout_guc(
        self, clean_jobs_app: JobsApp, module_pg_schema: object
    ) -> None:
        """GUC non-leakage: a contended acquire sets ``lock_timeout`` inside
        a savepoint and MUST restore the prior value before releasing the
        savepoint (``SET LOCAL`` effects persist through RELEASE), so a
        LATER lock acquire in the same transaction does not inherit the
        stale bound — pinned with a second acquire that waits LONGER than
        the first acquire's whole budget and must still succeed."""
        pg_schema: ModulePgSchema = module_pg_schema  # type: ignore[assignment]  # Why: fixture is typed ModulePgSchema; object keeps the test signature loose like test_postgres_enqueue_max_pending_lock
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        sql = render(schema)
        actor_a = "guc_actor_a"
        actor_b = "guc_actor_b"
        key_a = f"taskq:max_pending:{schema}:{actor_a}"
        key_b = f"taskq:max_pending:{schema}:{actor_b}"
        first_budget_ms = 200.0
        holder = await asyncpg.connect(pg_schema.pg_dsn)
        try:

            async def _hold_briefly(key: str, hold_s: float, held: asyncio.Event) -> None:
                async with holder.transaction():
                    await holder.execute(_HOLD_LOCK_SQL, key)
                    held.set()
                    await asyncio.sleep(hold_s)
                # Transaction exit releases the advisory lock.

            async with (
                deps.worker_pool.acquire() as pool_conn,
                pool_conn.transaction(),
            ):
                counting = _CountingConn(pool_conn)
                # First acquire: contended (holder signaled before the
                # racer's first try-lock), granted at ~50 ms inside the
                # 200 ms budget.
                held_a = asyncio.Event()
                hold_a = asyncio.create_task(_hold_briefly(key_a, 0.05, held_a))
                await held_a.wait()
                args_a = dataclasses.replace(make_enqueue_args(actor=actor_a), max_pending=_CAP)
                row_a = await _enqueue_on_conn(
                    counting,  # type: ignore[arg-type]  # Why: delegating wrapper is a ConnLike stand-in
                    sql,
                    schema,
                    SystemClock(),
                    args_a,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                    max_pending_lock_timeout_ms=first_budget_ms,
                )
                await hold_a
                assert row_a.actor == actor_a
                # The contended tier ran: budget set, prior value restored.
                set_calls = [s for s in counting.sql_log if "set_config" in s]
                assert len(set_calls) == 2, (
                    f"contended acquire must set then restore lock_timeout, saw {counting.sql_log!r}"
                )
                # The GUC reads back as the session default inside the
                # SAME transaction (restore happened before RELEASE).
                current = await pool_conn.fetchval("SELECT current_setting('lock_timeout')")
                assert current == "0", f"stale lock_timeout leaked: {current!r}"

                # Second acquire in the SAME transaction, on a key held
                # for 300 ms > first_budget_ms: with the stale 200 ms
                # bound this would raise 55P03 at ~200 ms; restored, it
                # succeeds at ~300 ms.
                held_b = asyncio.Event()
                hold_b = asyncio.create_task(_hold_briefly(key_b, 0.30, held_b))
                await held_b.wait()
                args_b = dataclasses.replace(make_enqueue_args(actor=actor_b), max_pending=_CAP)
                async with asyncio.timeout(5.0):
                    row_b = await _enqueue_on_conn(
                        counting,  # type: ignore[arg-type]  # Why: delegating wrapper is a ConnLike stand-in
                        sql,
                        schema,
                        SystemClock(),
                        args_b,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                        max_pending_lock_timeout_ms=5000.0,
                    )
                await hold_b
                assert row_b.actor == actor_b
        finally:
            await holder.close()

    async def test_happy_path_statement_count_parity(self, clean_jobs_app: JobsApp) -> None:
        """Happy-path round-trip parity on real Postgres: an uncontended
        capped enqueue issues exactly ONE advisory-lock statement (the
        try-lock fast path) — no savepoint, no GUC round trips — the same
        count as before the bounded-wait redesign."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        sql = render(schema)
        async with (
            deps.worker_pool.acquire() as pool_conn,
            pool_conn.transaction(),
        ):
            counting = _CountingConn(pool_conn)
            row = await _enqueue_on_conn(
                counting,  # type: ignore[arg-type]  # Why: delegating wrapper is a ConnLike stand-in
                sql,
                schema,
                SystemClock(),
                _capped_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                max_pending_lock_timeout_ms=5000.0,
            )
        assert row.actor == _CAPPED_ACTOR
        # "advisory", not "pg_advisory": the fast-path statement is
        # pg_try_advisory_xact_lock, which does not contain the substring
        # "pg_advisory".
        lock_stmts = [s for s in counting.sql_log if "advisory" in s]
        assert lock_stmts == [
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))"
        ], (  # Why: static expected literal, not interpolated
            f"the uncontended path must issue exactly the one try-lock, saw {counting.sql_log!r}"
        )
        assert not any("set_config" in s for s in counting.sql_log)
        assert not any("SAVEPOINT" in s for s in counting.sql_log)
