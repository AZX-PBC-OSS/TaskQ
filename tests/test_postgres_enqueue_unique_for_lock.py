"""Bounded-wait contract for the unique_for single-flight advisory lock.

A ``unique_for`` + ``identity_key`` single enqueue serializes its
preflight-then-insert behind a transaction-scoped advisory lock keyed
``taskq:unique_for:<schema>:<actor>:<identity_key>``. This module pins
three properties:

1. The lock WAIT is bounded: a racer that cannot acquire the lock within
   its budget gets the typed :class:`UniqueForLockTimeoutError` -- never a
   raw asyncpg error, and never an unbounded block.
2. Exhaustion is NOT backpressure: the error sits outside the
   ``BackpressureError`` family (the caller's correct response is to
   retry the same enqueue, not to shed load) -- but the refusal is
   COUNTED on ``taskq.backpressure.errors`` under its own bounded kind
   (``unique_for_lock_timeout``), beside the warning log: a typed
   refusal an operator can only see by reading logs is invisible at
   3am, and the ``kind`` label keeps identity contention off the
   capacity kinds an operator's capacity alerting keys on.
3. The wait's correct outcome survives: once a racer acquires the lock
   after contention, the preflight either returns the winner's row (the
   dedup return the wait was for) or inserts a fresh row.

The acquire is TWO-TIER (``taskq._advisory.acquire_advisory_xact_lock_bounded``,
shared with the max_pending lock): an uncontended racer takes the fast path
(one ``pg_try_advisory_xact_lock`` statement), while a contended racer
falls back to a server-side bounded blocking acquire inside a savepoint
(``set_config('lock_timeout', ..., true)`` + ``pg_advisory_xact_lock`` +
restore) so Postgres' lock scheduler hands the lock off at the rate
holders release it, with a client-side ``asyncio.wait_for`` backstop for
the network-black-hole case.

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
import structlog.testing

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
    """A RETURNING * shaped record for the fake conn's INSERT arm.

    ``JobRow``'s retry-curve fields are typed ``timedelta`` (application
    domain); the ``jobs`` row stores them as ``_seconds`` float columns
    (see migration 01.00.12_03) and ``_job_row_from_record`` reads them by
    that column name - the one field family ``asdict`` cannot shape
    correctly for a RETURNING-* stand-in, so it is patched here.
    """
    row = asdict(make_job_row(status="pending", actor=_UNIQUE_FOR_ACTOR, identity_key=_IDENTITY))
    row["retry_base_seconds"] = row.pop("retry_base").total_seconds()
    row["retry_cap_seconds"] = row.pop("retry_cap").total_seconds()
    return row


class _NullSavepoint:
    """async with conn.transaction() stand-in: counts opens, no-ops."""

    async def __aenter__(self) -> "_NullSavepoint":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _ContendedFakeConn:
    """ConnLike stand-in for the unique_for single-enqueue path.

    Models a caller-owned transaction (``is_in_transaction() -> True``) and
    the TWO-TIER acquire's statement shapes: the fast-path
    ``pg_try_advisory_xact_lock`` returns *try_lock_result* (False models a
    holder owning the lock at that instant); the contended tier's BLOCKING
    ``pg_advisory_xact_lock`` inside the savepoint raises the raw 55P03
    (*blocking_times_out*: the server-side ``lock_timeout`` fired) or
    succeeds (*granted*: the server-side queue handed the lock over once
    the holder released). ``set_config``/``current_setting`` round trips
    are recorded so the tests can pin the GUC save/restore shape.
    """

    def __init__(
        self,
        *,
        try_lock_result: bool = False,
        blocking_times_out: bool = True,
        preflight_rec: dict[str, object] | None = None,
    ) -> None:
        self.try_lock_result = try_lock_result
        self.blocking_times_out = blocking_times_out
        self.preflight_rec = preflight_rec
        self.try_lock_calls = 0
        self.blocking_lock_calls = 0
        self.savepoint_opens = 0
        self.executed_sql: list[str] = []
        self.fetchrow_sql: list[str] = []
        self.set_config_values: list[str | None] = []

    def is_in_transaction(self) -> bool:
        return True

    def transaction(self) -> _NullSavepoint:
        self.savepoint_opens += 1
        return _NullSavepoint()

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return self.try_lock_result
        if "current_setting" in sql:
            return "0"
        raise AssertionError(f"unexpected fetchval: {sql}")

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

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object] | None:
        self.fetchrow_sql.append(sql)
        if "identity_key = $2" in sql:
            # enqueue_unique_for_preflight - reached only after the lock is held.
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
        """A racer whose server-side lock_timeout fires gets the typed
        unique_for error -- not a raw driver error, and not a
        BackpressureError (exhaustion is a dedup-outcome unknown, not a
        capacity signal) -- with the two-tier statement shape."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
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
        assert exc_info.value.timeout_ms == 100.0
        assert elapsed < 2.0, f"budget was 100 ms but the wait took {elapsed:.3f}s"
        # Two-tier shape: one try-lock, then the savepoint tier with the
        # GUC set to the budget. Only the SET ran - the timeout raised
        # before any restore, and the savepoint ROLLBACK undoes the set
        # itself (verified PG savepoint/GUC semantics); the
        # restore-before-RELEASE belongs to the success path.
        assert conn.try_lock_calls == 1
        assert conn.savepoint_opens == 1
        assert conn.blocking_lock_calls == 1
        assert conn.set_config_values == ["100ms"]
        # The refusal is counted under its own bounded kind, beside the
        # log line - never under a capacity kind, and never uncounted
        # (a log-only refusal is invisible to an operator's alerting).
        assert recorded == [(_UNIQUE_FOR_ACTOR, "unique_for_lock_timeout")]

    async def test_lock_timeout_logs_warning_event_with_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ``unique-for-lock-timeout`` log event carries the identity
        and the expired budget - the per-occurrence observability channel
        for an exhaustion whose RATE rides the backpressure counter."""
        _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)
        with (
            structlog.testing.capture_logs() as logs,
            pytest.raises(UniqueForLockTimeoutError),
        ):
            await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                _unique_for_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                unique_for_lock_timeout_ms=250.0,
            )
        entries = [e for e in logs if e.get("event") == "unique-for-lock-timeout"]
        assert len(entries) == 1, f"expected exactly one timeout event, got {logs!r}"
        assert entries[0].get("actor") == _UNIQUE_FOR_ACTOR
        assert entries[0].get("identity_key") == _IDENTITY
        assert entries[0].get("lock_timeout_ms") == 250.0

    async def test_lock_acquired_after_contention_proceeds_to_insert(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Contended racers queue server-side: once the holder releases
        inside the budget, the blocking acquire is granted and the enqueue
        proceeds to a fresh INSERT (preflight miss)."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=False)
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
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 1
        assert conn.savepoint_opens == 1
        assert conn.set_config_values == ["1000ms", "0"]
        assert recorded == []

    async def test_dedup_return_after_contention_is_preserved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The wait's correct outcome: a racer that acquires after the
        holder committed gets the holder's row back as the dedup return,
        not a second insert."""
        recorded = _spy_backpressure(monkeypatch)
        winner = _inserted_record()
        conn = _ContendedFakeConn(
            try_lock_result=False, blocking_times_out=False, preflight_rec=winner
        )
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
        # The INSERT never ran: the only fetchrow issued was the preflight.
        assert len(conn.fetchrow_sql) == 1
        assert "identity_key = $2" in conn.fetchrow_sql[0]
        assert recorded == []

    async def test_lock_timeout_budget_zero_waits_indefinitely(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``timeout_ms <= 0`` disables the bound (the pre-fix behavior),
        matching the ``lock_timeout`` GUC convention used by migrate.py: a
        plain blocking acquire with NO savepoint and NO GUC statements -
        only an unbounded server-side wait reaches the dedup answer."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=False)
        args = _unique_for_args()
        async with asyncio.timeout(5.0):
            row = await _enqueue_on_conn(
                conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
                render("taskq"),
                "taskq",
                FakeClock(_START),
                args,  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
                unique_for_lock_timeout_ms=0.0,
            )
        assert row.actor == _UNIQUE_FOR_ACTOR
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 1
        assert conn.savepoint_opens == 0, "indefinite mode must not open a savepoint"
        assert conn.set_config_values == [], "indefinite mode must not touch the GUC"
        assert recorded == []

    async def test_fast_path_uncontended_is_single_try_lock_statement(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path round-trip parity: an uncontended unique_for enqueue
        issues exactly ONE advisory-lock statement (the try-lock) and never
        touches the savepoint/GUC machinery."""
        recorded = _spy_backpressure(monkeypatch)
        conn = _ContendedFakeConn(try_lock_result=True)
        row = await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            _unique_for_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
            unique_for_lock_timeout_ms=5000.0,
        )
        assert row.actor == _UNIQUE_FOR_ACTOR
        assert conn.try_lock_calls == 1
        assert conn.blocking_lock_calls == 0
        assert conn.savepoint_opens == 0
        assert conn.set_config_values == []
        assert recorded == []

    async def test_no_unique_for_takes_no_advisory_lock(self) -> None:
        """unique_for=None (or identity_key=None) never touches the
        advisory lock - the bounded-wait machinery is scoped to the
        single-flight path only."""
        conn = _ContendedFakeConn(try_lock_result=True)
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
        assert conn.blocking_lock_calls == 0
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
                    # CM. An unbounded acquire would trip the 3 s deadline
                    # and TimeoutError would escape pytest.raises.
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

    async def test_racer_acquires_after_holder_releases(
        self, clean_jobs_app: JobsApp, module_pg_schema: object
    ) -> None:
        """A racer queued server-side inside its budget proceeds as soon as
        the holder releases -- the preflight misses (the holder inserted
        nothing) and the racer's own INSERT lands."""
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
            # Let the racer reach the contended tier (queued server-side),
            # then release.
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
