"""Bounded-wait contract for the capped-actor max_pending advisory lock.

Single enqueues against a capped actor serialize the count-then-insert per
(schema, actor) behind a transaction-scoped advisory lock. This module pins
two properties:

1. The cap is EXACT under concurrency (the lock serializes the racers).
2. The lock WAIT is bounded: a racer that cannot acquire the lock within
   its budget gets the typed backpressure error -- never a raw asyncpg
   error, and never an unbounded block.

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

import asyncpg
import pytest

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


class _ContendedFakeConn:
    """ConnLike stand-in for the BYO-transaction capped-enqueue path.

    Models a caller-owned transaction (``is_in_transaction() -> True``, so
    ``_enqueue_on_conn`` takes the no-wrap branch unchanged). Contention is
    deterministic: ``pg_try_advisory_xact_lock`` fails until
    *success_after* calls have been made (``None`` = never), and a
    BLOCKING ``pg_advisory_xact_lock`` (the pre-fix statement shape)
    surfaces the raw 55P03 so a regression back to the unbounded blocking
    acquire is loud instead of silently green.
    """

    def __init__(self, *, success_after: int | None = None) -> None:
        self.try_lock_calls = 0
        self.success_after = success_after
        self.executed_sql: list[str] = []

    def is_in_transaction(self) -> bool:
        return True

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return self.success_after is not None and self.try_lock_calls > self.success_after
        # enqueue_max_pending_count — reached only after the lock is held.
        return 0

    async def execute(self, sql: str, *params: object) -> str:
        self.executed_sql.append(sql)
        if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
            raise asyncpg.LockNotAvailableError("simulated lock contention")
        return "OK"

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object]:
        return _inserted_record()


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
        """A racer that never acquires the lock inside its budget gets the
        typed backpressure error, never the raw driver error."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(success_after=None)
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
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        assert recorded == [(_CAPPED_ACTOR, "max_pending_lock_timeout")]

    async def test_lock_acquired_after_contention_clears_within_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Contended racers RETRY: once the holder releases inside the
        budget, the enqueue proceeds to completion."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(success_after=2)
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
        assert conn.try_lock_calls == 3  # two contended polls, then the acquire
        assert recorded == []

    async def test_uncapped_enqueue_takes_no_advisory_lock(self) -> None:
        """max_pending=None (the default) never touches the advisory lock —
        the bounded-wait machinery is scoped to capped actors only."""
        conn = _ContendedFakeConn(success_after=None)
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
        assert not any("pg_advisory" in s for s in conn.executed_sql)


# ── Integration: real Postgres ───────────────────────────────────────────


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
        # default budget comfortably covers 50 serialized racers.
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
        finishes."""
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
                    # CM. On the pre-fix code the unbounded block trips the
                    # 3s deadline and TimeoutError escapes pytest.raises --
                    # the RED below.
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
            assert elapsed < 3.0, f"budget was 250 ms but the wait took {elapsed:.3f}s"
            # The timed-out racer's transaction rolled back: nothing stored.
            stored = await deps.worker_pool.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: human-readable row-count check, not a SQL query
            )
            assert stored == 0
        finally:
            await holder.close()
