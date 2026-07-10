"""Coverage for ``taskq.backend._enqueue`` error and edge-case paths.

Exercises branches not covered by the PG integration tests, using a fake
asyncpg connection so no database is required:

- ``_enqueue_on_conn``: ``unique_for`` preflight dedup, singleton
  preflight collision, ``max_pending`` exceeded, singleton
  ``UniqueViolationError`` catch, and ``result_ttl`` → ``result_expires_at``.
- ``_enqueue_batch``: empty ``args_list`` raises ``ValueError``.
- ``_enqueue_batch_fast``: empty ``args_list`` raises ``ValueError`` and
  ``schedule_to_close_interval`` resolution.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id
from taskq.backend._enqueue import (
    _enqueue_batch,
    _enqueue_batch_fast,
    _enqueue_on_conn,
)
from taskq.backend._protocol import EnqueueArgs, IdentityKey, JobRow
from taskq.backend._sql_templates import render as render_sql
from taskq.exceptions import MaxPendingExceededError, SingletonCollisionError
from taskq.testing.clock import FakeClock

_SCHEMA_LABEL = "taskq"
_SQL = render_sql(_SCHEMA_LABEL)
_NOW = datetime(2025, 1, 1, tzinfo=UTC)


# ── Fake asyncpg Record / Connection ─────────────────────────────────────


def _full_record(*, job_id: UUID | None = None) -> dict[str, object]:
    """A dict with every field ``_job_row_from_record`` reads."""
    jid = job_id or new_job_id()
    return {
        "id": jid,
        "actor": "test_actor",
        "queue": "default",
        "identity_key": None,
        "fairness_key": None,
        "payload": "{}",
        "payload_schema_ver": 1,
        "status": "pending",
        "priority": 0,
        "attempt": 0,
        "max_attempts": 3,
        "retry_kind": "transient",
        "schedule_to_close": None,
        "start_to_close": None,
        "heartbeat_timeout": None,
        "created_at": _NOW,
        "scheduled_at": _NOW,
        "started_at": None,
        "finished_at": None,
        "last_heartbeat_at": None,
        "locked_by_worker": None,
        "lock_expires_at": None,
        "cancel_requested_at": None,
        "cancel_phase": 0,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "progress_state": "{}",
        "progress_seq": 0,
        "result": None,
        "result_size_bytes": None,
        "result_expires_at": None,
        "idempotency_key": None,
        "trace_id": None,
        "span_id": None,
        "metadata": "{}",
        "tags": [],
    }


class _Record:
    """Duck-typed asyncpg.Record — supports ``rec[key]`` and ``key in rec``."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data


class _FakeTx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeEnqueueConn:
    """asyncpg.Connection stand-in that routes calls by SQL substring."""

    def __init__(
        self,
        *,
        fetchrow_map: dict[str, object | _Record | None] | None = None,
        fetchval_map: dict[str, object] | None = None,
        fetch_map: dict[str, list[_Record]] | None = None,
        insert_exc: BaseException | None = None,
        copy_result: str = "COPY 1",
    ) -> None:
        self._fetchrow_map = fetchrow_map or {}
        self._fetchval_map = fetchval_map or {}
        self._fetch_map = fetch_map or {}
        self._insert_exc = insert_exc
        self._copy_result = copy_result
        self.execute_calls: list[str] = []

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        for pattern, result in self._fetchrow_map.items():
            if pattern in sql:
                return result
        return None

    async def fetchval(self, sql: str, *args: object) -> object:
        for pattern, result in self._fetchval_map.items():
            if pattern in sql:
                return result
        return 0

    async def fetch(self, sql: str, *args: object) -> list[_Record]:
        for pattern, result in self._fetch_map.items():
            if pattern in sql:
                return result
        return []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append(sql)
        return "OK"

    def transaction(self) -> _FakeTx:
        return _FakeTx()

    async def copy_records_to_table(
        self, table: str, *, records: list[object], columns: list[str], schema_name: str
    ) -> str:
        return self._copy_result


def _make_args(
    *,
    unique_for: timedelta | None = None,
    identity_key: str | None = None,
    singleton: bool = False,
    max_pending: int | None = None,
    result_ttl: timedelta | None = None,
    schedule_to_close_interval: timedelta | None = None,
    idempotency_key: str | None = None,
    scheduled_at: datetime | None = None,
) -> EnqueueArgs:
    metadata: dict[str, object] = {}
    if singleton:
        metadata["singleton"] = True
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=scheduled_at or _NOW,
        priority=0,
        schedule_to_close=None,
        idempotency_key=idempotency_key,
        identity_key=IdentityKey(identity_key) if identity_key is not None else None,
        unique_for=unique_for,
        unique_states=("pending", "scheduled", "running"),
        max_pending=max_pending,
        result_ttl=result_ttl,
        schedule_to_close_interval=schedule_to_close_interval,
        metadata=metadata,
        tags=(),
    )


# ── _enqueue_on_conn: unique_for preflight dedup ─────────────────────────


async def test_unique_for_preflight_returns_existing_row() -> None:
    """When ``unique_for`` + ``identity_key`` are set and a matching row
    exists, the existing row is returned without inserting."""
    existing_id = new_job_id()
    conn = _FakeEnqueueConn(
        fetchrow_map={"identity_key = $2": _Record(_full_record(job_id=existing_id))}
    )
    args = _make_args(unique_for=timedelta(minutes=5), identity_key="dedup-key")
    clock = FakeClock(_NOW)

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert isinstance(row, JobRow)
    assert row.id == existing_id


# ── _enqueue_on_conn: singleton preflight collision ──────────────────────


async def test_singleton_preflight_raises_collision() -> None:
    """When ``metadata['singleton']`` is True and a blocking row exists,
    ``SingletonCollisionError`` is raised with a retry_after when the
    blocking job has a future ``schedule_to_close``."""
    blocking_id = new_job_id()
    future = _NOW + timedelta(minutes=10)
    conn = _FakeEnqueueConn(
        fetchrow_map={
            "schedule_to_close FROM": _Record({"id": blocking_id, "schedule_to_close": future})
        }
    )
    args = _make_args(singleton=True)
    clock = FakeClock(_NOW)

    with pytest.raises(SingletonCollisionError) as exc_info:
        await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    err = exc_info.value
    assert err.blocking_job_id == blocking_id
    assert err.retry_after is not None
    assert err.retry_after > timedelta(seconds=0)


async def test_singleton_preflight_no_retry_after_when_no_deadline() -> None:
    """A singleton collision with no ``schedule_to_close`` yields
    ``retry_after=None``."""
    conn = _FakeEnqueueConn(
        fetchrow_map={
            "schedule_to_close FROM": _Record({"id": new_job_id(), "schedule_to_close": None})
        }
    )
    args = _make_args(singleton=True)
    clock = FakeClock(_NOW)

    with pytest.raises(SingletonCollisionError) as exc_info:
        await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert exc_info.value.retry_after is None


# ── _enqueue_on_conn: max_pending exceeded ───────────────────────────────


async def test_max_pending_exceeded_raises() -> None:
    """When the pending count reaches ``max_pending``, a
    ``MaxPendingExceededError`` is raised."""
    conn = _FakeEnqueueConn(fetchval_map={"count": 10})
    args = _make_args(max_pending=10)
    clock = FakeClock(_NOW)

    with pytest.raises(MaxPendingExceededError) as exc_info:
        await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert exc_info.value.max_pending == 10
    assert exc_info.value.current_count == 10


# ── _enqueue_on_conn: singleton UniqueViolationError catch ───────────────


async def test_singleton_unique_violation_raises_collision() -> None:
    """A ``UniqueViolationError`` on the singleton constraint is caught and
    re-raised as ``SingletonCollisionError``."""
    exc = asyncpg.UniqueViolationError()
    exc.constraint_name = "jobs_singleton_uniq"  # type: ignore[attr-defined]  # Why: asyncpg sets constraint_name at runtime; assigning for test setup.

    class _InsertFailsConn(_FakeEnqueueConn):
        async def fetchrow(self, sql: str, *args: object) -> object | None:
            if "INSERT" in sql.upper():
                raise exc
            return await super().fetchrow(sql, *args)

    conn = _InsertFailsConn()
    args = _make_args()
    clock = FakeClock(_NOW)

    with pytest.raises(SingletonCollisionError):
        await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)


# ── _enqueue_on_conn: result_ttl sets result_expires_at ──────────────────


async def test_result_ttl_path_succeeds_and_notifies() -> None:
    """When ``result_ttl`` is set, the INSERT succeeds, the row is returned,
    and a pg_notify is issued for the new row."""
    rec = _Record(_full_record())
    conn = _FakeEnqueueConn(fetchrow_map={"RETURNING": rec, "INSERT": rec})
    args = _make_args(result_ttl=timedelta(hours=1))
    clock = FakeClock(_NOW)

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert isinstance(row, JobRow)
    # pg_notify was issued (enqueue_notify SQL).
    assert any("pg_notify" in sql for sql in conn.execute_calls)


# ── _enqueue_on_conn: idempotency-key ON CONFLICT dedup ──────────────────


async def test_idempotency_key_conflict_returns_existing_row() -> None:
    """When the INSERT returns no row (ON CONFLICT), the follow-up SELECT by
    idempotency_key returns the existing row."""
    existing_id = new_job_id()
    existing_rec = _Record(_full_record(job_id=existing_id))
    conn = _FakeEnqueueConn(
        fetchrow_map={"idempotency_key = $1": existing_rec},
        # INSERT RETURNING returns None (conflict) — default fetchrow returns None.
    )
    args = _make_args(idempotency_key="idem-1")
    clock = FakeClock(_NOW)

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert isinstance(row, JobRow)
    assert row.id == existing_id
    # No pg_notify for a deduplicated (not-new) row.
    assert not any("pg_notify" in sql for sql in conn.execute_calls)


# ── _enqueue_batch: empty args_list ──────────────────────────────────────


async def test_enqueue_batch_empty_raises_value_error() -> None:
    """An empty ``args_list`` raises ``ValueError`` before any SQL runs."""
    pool = _FakePool(_FakeEnqueueConn())
    clock = FakeClock(_NOW)
    with pytest.raises(ValueError, match="must not be empty"):
        await _enqueue_batch(pool, _SQL, _SCHEMA_LABEL, clock, [])


# ── _enqueue_batch_fast: empty args_list ─────────────────────────────────


async def test_enqueue_batch_fast_empty_raises_value_error() -> None:
    """An empty ``args_list`` raises ``ValueError`` before any COPY runs."""
    pool = _FakePool(_FakeEnqueueConn())
    clock = FakeClock(_NOW)
    with pytest.raises(ValueError, match="must not be empty"):
        await _enqueue_batch_fast(pool, _SQL, _SCHEMA_LABEL, clock, [])


# ── _enqueue_batch_fast: schedule_to_close_interval + result_ttl ────────


async def test_enqueue_batch_fast_schedule_interval_and_result_ttl() -> None:
    """``schedule_to_close_interval`` and ``result_ttl`` are resolved into
    the COPY record tuple; the COPY returns a row count."""
    conn = _FakeEnqueueConn(copy_result="COPY 2")
    pool = _FakePool(conn)
    clock = FakeClock(_NOW)
    args = _make_args(
        schedule_to_close_interval=timedelta(hours=1),
        result_ttl=timedelta(hours=2),
        scheduled_at=_NOW + timedelta(minutes=5),
    )

    count = await _enqueue_batch_fast(pool, _SQL, _SCHEMA_LABEL, clock, [args])

    assert count == 2
    # pg_notify issued after COPY.
    assert any("pg_notify" in sql for sql in conn.execute_calls)


# ── _enqueue_batch_fast: scheduled vs pending status ─────────────────────


async def test_enqueue_batch_fast_immediate_job_is_pending() -> None:
    """A job with ``scheduled_at <= now`` is marked ``pending`` (not
    ``scheduled``) in the COPY record."""
    conn = _FakeEnqueueConn(copy_result="COPY 1")
    pool = _FakePool(conn)
    clock = FakeClock(_NOW)
    args = _make_args(scheduled_at=_NOW)  # immediate

    count = await _enqueue_batch_fast(pool, _SQL, _SCHEMA_LABEL, clock, [args])
    assert count == 1


# ── Fake pool ────────────────────────────────────────────────────────────


class _FakePool:
    """Minimal asyncpg.Pool stand-in yielding a fixed connection."""

    def __init__(self, conn: _FakeEnqueueConn) -> None:
        self._conn = conn

    def acquire(self) -> "_PoolCtx":
        return _PoolCtx(self._conn)


class _PoolCtx:
    def __init__(self, conn: _FakeEnqueueConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeEnqueueConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass
