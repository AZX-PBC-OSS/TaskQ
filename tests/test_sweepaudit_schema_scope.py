"""Schema-scope audit pins: database-scoped resources must be schema-qualified.

The advisory-lock class defect this file pins: Postgres advisory locks live
in a per-DATABASE namespace, so any lock key built without the schema is
shared by every schema (and every deployment) in one database. The
leader/cron/prune/archive locks were qualified via
``taskq.constants.schema_lock_name``; this file pins the two lock surfaces
that audit found still unqualified:

* the migration advisory lock (``taskq.migrate``) — a fixed bigint key
  shared by ALL schemas, so deployment B's bounded startup wait
  SystemExits while deployment A runs a long migration, even when B's
  schema has nothing to migrate;
* the PG log-window rate-limit acquire's per-bucket advisory lock
  (``taskq.ratelimit._sliding_window_pg``) — keyed on the bare bucket
  name, so two schemas sharing one database serialize on one lock while
  operating on different ``"{schema}".rate_limit_window_entries`` tables;
* the keyed-reservation materialization path's silent ``"taskq"`` schema
  fallback when a PG pool is supplied without settings
  (``taskq.ratelimit.registry``) — a direct library caller would write
  slot rows into the wrong schema.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid

pytestmark = pytest.mark.integration


class _RecordingConn:
    """Minimal asyncpg-Connection stand-in recording every execute.

    Same recording pattern as ``tests/test_migrate_hooks.py``'s
    ``_FakeConn``: the lock-key pin asserts on the statements the real
    connection would receive, not on a reimplementation of the lock.
    """

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((sql, args))
        return "OK"


# ── Migration advisory lock ────────────────────────────────────────────


async def test_migration_lock_key_is_schema_qualified() -> None:
    """The lock must be a hashtextextended string key carrying the schema.

    Pre-fix this was ``pg_advisory_lock($1)`` with a fixed bigint — one
    lock for every schema in the database.
    """
    conn = _RecordingConn()
    async with migrate_mod.migration_advisory_lock(conn, schema="sweepaudit_q"):
        pass

    lock_calls = [(sql, args) for sql, args in conn.executed if "pg_advisory_lock(" in sql]
    assert lock_calls, f"no advisory-lock acquire recorded: {conn.executed}"
    lock_sql, lock_args = lock_calls[0]
    assert "hashtextextended" in lock_sql, (
        f"lock acquire must hash a string key (schema-qualified), got: {lock_sql}"
    )
    assert lock_args == ("taskq:migrate:sweepaudit_q",), (
        f"lock key must be schema-qualified, got: {lock_args!r}"
    )

    unlock_calls = [(sql, args) for sql, args in conn.executed if "pg_advisory_unlock" in sql]
    assert unlock_calls, "no advisory-lock release recorded"
    unlock_sql, unlock_args = unlock_calls[0]
    assert "hashtextextended" in unlock_sql
    assert unlock_args == ("taskq:migrate:sweepaudit_q",), (
        f"unlock key must match the acquire key, got: {unlock_args!r}"
    )


async def test_migration_lock_cross_schema_isolation(pg_dsn: str) -> None:
    """Holding deployment A's migration lock must not block deployment B.

    Two schemas in one database are two deployments: a long migration in
    schema A (index builds run for minutes) must not consume schema B's
    bounded startup wait — B's lock is a different key, so B proceeds even
    while A holds its own migration lock. Pre-fix both schemas shared one
    bigint key and B SystemExited at the wait bound.
    """
    schema_a = "sweepaudit_holder_a"  # Name-only: the lock precedes any table access.
    schema_b = "sweepaudit_isolated_b"
    holder = await asyncpg.connect(pg_dsn)
    try:
        async with migrate_mod.migration_advisory_lock(holder, schema=schema_a):
            applied = await asyncio.wait_for(
                migrate_mod.apply_pending_locked(pg_dsn, schema=schema_b, lock_timeout=5.0),
                timeout=120.0,
            )
            assert applied, "schema B should have applied its migrations unblocked"
    finally:
        await holder.close()

    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE')
    finally:
        await cleanup.close()


async def test_migration_lock_same_schema_still_serializes(pg_dsn: str) -> None:
    """Qualifying the key must not break same-schema serialization.

    The lock exists so two replicas migrating the SAME schema cannot race
    a virgin schema's bare CREATE TABLE. A second migrator for the same
    schema must still fail fast at the wait bound, not run concurrently.
    """
    schema = "sweepaudit_same_schema"
    holder = await asyncpg.connect(pg_dsn)
    try:
        async with migrate_mod.migration_advisory_lock(holder, schema=schema):
            with pytest.raises(SystemExit) as excinfo:
                await migrate_mod.apply_pending_locked(pg_dsn, schema=schema, lock_timeout=1.0)
            msg = str(excinfo.value)
            assert "another process is applying migrations" in msg
            assert "migration failed, aborting startup" not in msg
    finally:
        await holder.close()


# ── PG log-window rate-limit advisory lock ─────────────────────────────


class _NullTx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> None:
        return None


class _RecordingPgConn:
    """Stand-in asyncpg connection recording statements for the lock-key pin."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _NullTx:
        return _NullTx()

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((sql, args))
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> dict[str, int]:
        self.executed.append((sql, args))
        if "count(*)" in sql:
            return {"count": 3}
        return {"inserted": 1}


class _RecordingPool:
    """Stand-in asyncpg Pool yielding the recording connection."""

    def __init__(self, conn: _RecordingPgConn) -> None:
        self._conn = conn

    def acquire(self) -> _PoolAcquire:
        return _PoolAcquire(self._conn)


class _PoolAcquire:
    def __init__(self, conn: _RecordingPgConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RecordingPgConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> None:
        return None


async def test_pg_log_window_advisory_lock_key_is_schema_qualified() -> None:
    """The per-bucket advisory lock must key on schema + bucket name.

    Advisory locks are database-scoped: keying on the bare bucket name made
    two schemas in one database serialize on one lock while operating on
    different ``"{schema}".rate_limit_window_entries`` tables — cross-
    deployment lock contention with zero correctness benefit.
    """
    from taskq.ratelimit._sliding_window_pg import (  # pyright: ignore[reportPrivateUsage]  # Why: pinning the exact production lock key is the point; redefining it here would let the pin drift.
        _acquire_pg_log,
    )
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": "sweepaudit_rl",
        },
        validate=False,
    )
    bucket = SlidingWindow(
        name="shared_bucket",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    conn = _RecordingPgConn()
    pool = _RecordingPool(conn)

    decision = await _acquire_pg_log(  # type: ignore[arg-type]  # Why: recording stand-ins for asyncpg types.
        bucket, pool, settings, new_uuid()
    )
    assert decision.backend == "postgres"

    lock_calls = [(sql, args) for sql, args in conn.executed if "pg_advisory_xact_lock" in sql]
    assert lock_calls, f"no advisory-lock acquire recorded: {conn.executed}"
    key = lock_calls[0][1][0]
    assert isinstance(key, str)
    assert "sweepaudit_rl" in key, (
        f"advisory lock key must carry the configured schema, got {key!r}"
    )
    assert "shared_bucket" in key, f"lock key must name the bucket, got {key!r}"


# ── Keyed-reservation materialization schema source ────────────────────


class _TypedPayload(BaseModel):
    session_id: str


async def test_keyed_reservation_with_pool_but_no_settings_raises() -> None:
    """Materializing a keyed reservation against PG needs the schema source.

    A direct library caller that supplies ``pg_pool`` but no ``settings``
    used to fall back to the ``ConcurrencyReservation`` default schema
    (``"taskq"``) — a silent wrong-schema write: the reservation's slot
    rows land in whatever ``"taskq".reservation_slots`` happens to exist
    (or the write fails noisily against a schema the caller never
    configured). The in-memory path (``pg_pool=None``) stays settings-free
    because its slot table is process-local and schema is irrelevant.
    """
    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.ratelimit.registry import RateLimitRegistry

    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="sweepaudit_session",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )
    registry = RateLimitRegistry()
    conn = _RecordingPgConn()
    pool = _RecordingPool(conn)

    with pytest.raises(RuntimeError, match="settings"):
        await registry._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: the settings-schema contract is pinned at its only enforcement point.
            ref,
            payload=_TypedPayload(session_id="s1"),
            pg_pool=pool,  # type: ignore[arg-type]  # Why: recording stand-in for asyncpg.Pool.
            settings=None,
        )
    # Nothing was registered or seeded against a default schema.
    assert registry.reservations == {}
    assert not conn.executed, f"no SQL may run before the settings guard: {conn.executed}"
