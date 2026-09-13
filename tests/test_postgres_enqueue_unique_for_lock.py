"""Bounded-wait contract for the unique_for single-flight advisory lock.

A ``unique_for`` + ``identity_key`` single enqueue serializes its
preflight-then-insert behind a transaction-scoped advisory lock keyed
``taskq:unique_for:<schema>:<actor>:<identity_key>``. This module pins
three properties:

1. The lock WAIT is bounded: a racer that cannot acquire the lock within
   its budget gets the typed :class:`UniqueForLockTimeoutError` -- never a
   raw asyncpg error, and never an unbounded block.
2. Exhaustion is NOT backpressure: the error sits outside the
   ``BackpressureError`` family and bumps no ``taskq.backpressure.errors``
   counter -- the caller's correct response is to retry the same enqueue,
   not to shed load.
3. The wait's correct outcome survives: once a racer acquires the lock
   after contention, the preflight either returns the winner's row (the
   dedup return the wait was for) or inserts a fresh row.

The unit tests drive :func:`_enqueue_on_conn` through a fake connection
that models contention deterministically; the integration tests pin the
same contract against real Postgres (advisory locks are server state, so
the bounded-wait behavior can only be fully observed there).
"""

import asyncio
import dataclasses
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

import taskq.backend._enqueue as _enqueue_mod
from taskq.backend._enqueue import _enqueue, _enqueue_on_conn
from taskq.backend._sql_templates import render
from taskq.backend.clock import SystemClock
from taskq.exceptions import (
    BackpressureError,
    UniqueForLockTimeoutError,
)
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.jobs import make_enqueue_args, make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)

_UNIQUE_FOR_ACTOR = "test_actor"
_IDENTITY = "acct-148"
_UNIQUE_FOR = timedelta(minutes=15)

_HOLD_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"


def _unique_for_args() -> object:
    """EnqueueArgs carrying the unique_for window + identity (the single
    path's own input for the lock)."""
    return dataclasses.replace(
        make_enqueue_args(actor=_UNIQUE_FOR_ACTOR, identity_key=_IDENTITY),
        unique_for=_UNIQUE_FOR,
    )


def _inserted_record() -> dict[str, object]:
    """A RETURNING * shaped record for the fake conn's INSERT arm."""
    return asdict(make_job_row(status="pending", actor=_UNIQUE_FOR_ACTOR, identity_key=_IDENTITY))


class _ContendedFakeConn:
    """ConnLike stand-in for the unique_for single-enqueue path.

    Models a caller-owned transaction (``is_in_transaction() -> True``).
    Contention is deterministic: ``pg_try_advisory_xact_lock`` fails until
    *success_after* calls have been made (``None`` = never), the preflight
    returns *preflight_rec* (``None`` = miss), and a BLOCKING
    ``pg_advisory_xact_lock`` (the pre-fix statement shape) surfaces the
    raw 55P03 so a regression back to the unbounded blocking acquire is
    loud instead of silently green.
    """

    def __init__(
        self,
        *,
        success_after: int | None = None,
        preflight_rec: dict[str, object] | None = None,
    ) -> None:
        self.try_lock_calls = 0
        self.success_after = success_after
        self.preflight_rec = preflight_rec
        self.executed_sql: list[str] = []

    def is_in_transaction(self) -> bool:
        return True

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return self.success_after is not None and self.try_lock_calls > self.success_after
        raise AssertionError(f"unexpected fetchval: {sql}")

    async def execute(self, sql: str, *params: object) -> str:
        self.executed_sql.append(sql)
        if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
            raise asyncpg.LockNotAvailableError("simulated lock contention")
        return "OK"

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object] | None:
        if "identity_key = $2" in sql:
            # enqueue_unique_for_preflight — reached only after the lock is held.
            return self.preflight_rec
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


class TestUniqueForLockBoundedWaitUnit:
    """The bounded-wait contract, driven through a fake connection."""

    async def test_lock_timeout_raises_typed_error_within_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A racer that never acquires the lock inside its budget gets the
        typed unique_for error -- not a raw driver error, and not a
        BackpressureError (exhaustion is a dedup-outcome unknown, not a
        capacity signal)."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(success_after=None)
        args = _unique_for_args()
        start = time.monotonic()
        with pytest.raises(UniqueForLockTimeoutError) as exc_info:
            await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                unique_for_lock_timeout_ms=100.0,
            )
        elapsed = time.monotonic() - start
        assert not isinstance(exc_info.value, BackpressureError)
        assert exc_info.value.identity_key == _IDENTITY
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        # Identity-key contention is not a capacity signal: the backpressure
        # counter must stay untouched.
        assert recorded == []

    async def test_lock_acquired_after_contention_proceeds_to_insert(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Contended racers RETRY: once the holder releases inside the
        budget, the enqueue proceeds to a fresh INSERT (preflight miss)."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(success_after=2)
        args = _unique_for_args()
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
            unique_for_lock_timeout_ms=1000.0,
        )
        assert row.actor == _UNIQUE_FOR_ACTOR
        assert conn.try_lock_calls == 3  # two contended polls, then the acquire
        assert recorded == []

    async def test_dedup_return_after_contention_is_preserved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The wait's correct outcome: a racer that acquires after the
        holder committed gets the holder's row back as the dedup return,
        not a second insert."""
        recorded = _spy_backpressure(monkeypatch)
        winner = _inserted_record()
        conn = _ContendedFakeConn(success_after=1, preflight_rec=winner)
        args = _unique_for_args()
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
            unique_for_lock_timeout_ms=1000.0,
        )
        assert str(row.id) == str(winner["id"])
        # The INSERT never ran: only the preflight fetchrow was issued.
        assert not any("INSERT" in s for s in conn.executed_sql)
        assert recorded == []

    async def test_no_unique_for_takes_no_advisory_lock(self) -> None:
        """unique_for=None (or identity_key=None) never touches the
        advisory lock — the bounded-wait machinery is scoped to the
        single-flight path only."""
        conn = _ContendedFakeConn(success_after=None)
        args = make_enqueue_args(actor=_UNIQUE_FOR_ACTOR, identity_key=_IDENTITY)
        assert args.unique_for is None
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            args,  # type: ignore[arg-type]  # Why: make_enqueue_args returns the EnqueueArgs type
        )
        assert row.actor == _UNIQUE_FOR_ACTOR
        assert conn.try_lock_calls == 0
        assert not any("pg_advisory" in s for s in conn.executed_sql)


# ── Integration: real Postgres ───────────────────────────────────────────


class TestUniqueForLockBoundedWait:
    """The advisory-lock WAIT is bounded under real Postgres contention."""

    pytestmark = pytest.mark.integration

    async def test_lock_wait_bounded_under_real_contention(
        self, clean_jobs_app: JobsApp, module_pg_schema: object
    ) -> None:
        """A racer facing a long-held lock fails with the typed error
        inside its budget instead of blocking until the holder finishes
        (the pre-fix behavior: an unbounded queue behind the holder)."""
        pg_schema: ModulePgSchema = module_pg_schema  # type: ignore[assignment]  # Why: fixture is typed ModulePgSchema; object keeps the test signature loose like test_postgres_enqueue_max_pending_lock
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        sql = render(schema)
        lock_key = f"taskq:unique_for:{schema}:{_UNIQUE_FOR_ACTOR}:{_IDENTITY}"
        holder = await asyncpg.connect(pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(_HOLD_LOCK_SQL, lock_key)
                start = time.monotonic()
                with pytest.raises(UniqueForLockTimeoutError) as exc_info:
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
                            _unique_for_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                            unique_for_lock_timeout_ms=250.0,
                        )
                elapsed = time.monotonic() - start
            assert not isinstance(exc_info.value, BackpressureError)
            assert exc_info.value.identity_key == _IDENTITY
            assert elapsed < 3.0, f"budget was 250 ms but the wait took {elapsed:.3f}s"
            # The timed-out racer's transaction rolled back: nothing stored.
            stored = await deps.worker_pool.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: human-readable row-count check, not a SQL query
            )
            assert stored == 0
        finally:
            await holder.close()

    async def test_racer_acquires_after_holder_releases(
        self, clean_jobs_app: JobsApp, module_pg_schema: object
    ) -> None:
        """A racer polling inside its budget proceeds as soon as the holder
        releases -- the preflight misses (the holder inserted nothing) and
        the racer's own INSERT lands."""
        pg_schema: ModulePgSchema = module_pg_schema  # type: ignore[assignment]  # Why: fixture is typed ModulePgSchema; object keeps the test signature loose like test_postgres_enqueue_max_pending_lock
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        sql = render(schema)
        lock_key = f"taskq:unique_for:{schema}:{_UNIQUE_FOR_ACTOR}:{_IDENTITY}"
        holder = await asyncpg.connect(pg_schema.pg_dsn)
        try:
            holder_tx = holder.transaction()
            await holder_tx.start()
            await holder.execute(_HOLD_LOCK_SQL, lock_key)
            racer = asyncio.create_task(
                _enqueue(
                    deps.worker_pool,
                    sql,
                    schema,
                    SystemClock(),
                    _unique_for_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                    unique_for_lock_timeout_ms=5000.0,
                )
            )
            # Let the racer take at least one contended poll, then release.
            await asyncio.sleep(0.2)
            await holder_tx.commit()
            row = await asyncio.wait_for(racer, timeout=10.0)
            assert row.actor == _UNIQUE_FOR_ACTOR
            stored = await deps.worker_pool.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: human-readable row-count check, not a SQL query
            )
            assert stored == 1
        finally:
            await holder.close()
