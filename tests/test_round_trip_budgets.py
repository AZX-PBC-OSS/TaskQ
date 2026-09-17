"""Round-trip budgets of the hot paths, pinned on a statement-recording fake.

asyncpg sends ``BEGIN`` and ``COMMIT`` (and, nested, ``SAVEPOINT`` /
``RELEASE``) as their own round trips, so ``conn.transaction()`` around a
single atomic statement triples its cost. The fake below records every
statement AND every transaction boundary in order, so each pin below is
the exact wire conversation a path spends. Peers run the equivalent
statements on the pool in autocommit (River's ``JobGetAvailable``,
pgqueuer's dequeue, pg-boss's fetch); Oban alone wraps its fetch.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from taskq._ids import new_job_id, new_uuid
from taskq.backend._dispatch import QueueModeCache, _dispatch_batch
from taskq.backend._enqueue import _enqueue, _enqueue_with_conn
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend._sql_templates import render as render_sql
from taskq.testing.clock import FakeClock

_SCHEMA = "taskq"
_SQL = render_sql(_SCHEMA)
_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_QUEUE = "default"
_LEASE = timedelta(seconds=30)

_CLAIM_MARKER = "WITH RECURSIVE params AS ("
_PROBE_MARKER = "ac.queue = ANY($1::text[])"


class _Record:
    """Duck-typed asyncpg.Record."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def keys(self) -> list[str]:
        return list(self._data)


def _job_record(*, job_id: UUID | None = None, actor: str = "test_actor") -> _Record:
    """Every column ``_job_row_from_record`` reads."""
    return _Record(
        {
            "id": job_id or new_job_id(),
            "actor": actor,
            "queue": _QUEUE,
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
            "idempotency_scope": "",
            "trace_id": None,
            "span_id": None,
            "metadata": "{}",
            "tags": [],
            "snooze_count": 0,
            "rate_limit_blocked_count": 0,
            "interrupt_count": 0,
            "retry_base_seconds": 5.0,
            "retry_cap_seconds": 3600.0,
            "retry_backoff": "exponential",
            "retry_jitter": 0.2,
            "assignment_routed": False,
        }
    )


class _Tx:
    """Records the wire statements asyncpg's Transaction issues: BEGIN /
    COMMIT at depth 0, SAVEPOINT / RELEASE when nested."""

    def __init__(self, conn: "_RecordingConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        self._conn.wire.append("BEGIN" if self._conn.depth == 0 else "SAVEPOINT")
        self._conn.depth += 1

    async def __aexit__(self, exc_type: object, *args: object) -> None:
        self._conn.depth -= 1
        if exc_type is not None:
            self._conn.wire.append("ROLLBACK" if self._conn.depth == 0 else "ROLLBACK TO SAVEPOINT")
        else:
            self._conn.wire.append("COMMIT" if self._conn.depth == 0 else "RELEASE")


class _RecordingConn:
    """Every statement and transaction boundary, in wire order.

    *responders* maps a SQL substring to the rows a ``fetch`` of that
    statement returns (``fetchrow`` returns the first, ``fetchval`` its
    first column); unmatched statements return nothing.
    """

    def __init__(self, responders: dict[str, list[_Record]] | None = None) -> None:
        self.wire: list[str] = []
        self.depth = 0
        self._responders = responders or {}

    def _rows(self, sql: str) -> list[_Record]:
        for marker, rows in self._responders.items():
            if marker in sql:
                return rows
        return []

    async def fetch(self, sql: str, *args: object) -> list[_Record]:
        self.wire.append(sql)
        return self._rows(sql)

    async def fetchrow(self, sql: str, *args: object) -> _Record | None:
        self.wire.append(sql)
        rows = self._rows(sql)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *args: object) -> object:
        self.wire.append(sql)
        rows = self._rows(sql)
        return rows[0][rows[0].keys()[0]] if rows else None

    async def execute(self, sql: str, *args: object) -> str:
        self.wire.append(sql)
        return "OK"

    def transaction(self, *args: object, **kwargs: object) -> _Tx:
        return _Tx(self)

    def is_in_transaction(self) -> bool:
        return self.depth > 0


class _RecordingPool:
    def __init__(self, conn: _RecordingConn) -> None:
        self.conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_RecordingConn]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's signature.
        yield self.conn


def _shape(wire: list[str]) -> list[str]:
    """The conversation as recognisable tokens."""
    out: list[str] = []
    for sql in wire:
        if sql in {"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "ROLLBACK TO SAVEPOINT"}:
            out.append(sql)
        elif _CLAIM_MARKER in sql:
            out.append("claim")
        elif _PROBE_MARKER in sql:
            out.append("probe")
        else:
            out.append(sql.split("\n", 1)[0][:40])
    return out


# ── dispatch ──────────────────────────────────────────────────────────────


async def _dispatch(conn: _RecordingConn) -> Any:
    cache = QueueModeCache()
    cache.store({_QUEUE: "strict_fifo"})
    return await _dispatch_batch(
        _RecordingPool(conn),  # type: ignore[arg-type]  # Why: duck-typed recording pool.
        _SQL,
        2,
        5.0,
        _SCHEMA,
        new_uuid(),
        [_QUEUE],
        10,
        _LEASE,
        queue_mode_cache=cache,
    )


async def test_a_dispatch_round_that_claims_is_one_statement() -> None:
    """The claim is one atomic UPDATE … RETURNING whose row locks end with
    the statement; wrapping it in a transaction only adds BEGIN and
    COMMIT round trips around it."""
    conn = _RecordingConn({_CLAIM_MARKER: [_job_record()]})
    rows = await _dispatch(conn)
    assert len(rows) == 1
    assert _shape(conn.wire) == ["claim"]


async def test_an_empty_dispatch_round_is_claim_then_probe() -> None:
    """Nothing claimable: the round spends the claim and one LIMIT-1
    probe, holds no locks between them, and opens no transaction."""
    conn = _RecordingConn()
    rows = await _dispatch(conn)
    assert rows == []
    assert _shape(conn.wire) == ["claim", "probe"]


# ── enqueue ───────────────────────────────────────────────────────────────


def _enqueue_args(**overrides: Any) -> EnqueueArgs:
    base: dict[str, Any] = {
        "id": new_job_id(),
        "actor": "test_actor",
        "queue": _QUEUE,
        "payload": {},
        "max_attempts": 3,
        "retry_kind": "transient",
        "scheduled_at": None,
    }
    base.update(overrides)
    return EnqueueArgs(**base)


async def _enqueue_on_pool(conn: _RecordingConn, args: EnqueueArgs) -> JobRow:
    return await _enqueue(
        _RecordingPool(conn),  # type: ignore[arg-type]  # Why: duck-typed recording pool.
        _SQL,
        _SCHEMA,
        FakeClock(_NOW),
        args,
    )


async def test_a_plain_enqueue_is_one_statement() -> None:
    """INSERT … RETURNING, nothing else: no transaction (the INSERT is
    atomic on its own and the arm takes no lock) and no app-side notify
    (the row trigger is the wake source)."""
    args = _enqueue_args()
    conn = _RecordingConn({"INSERT INTO": [_job_record(job_id=args.id)]})
    row = await _enqueue_on_pool(conn, args)
    assert row.id == args.id
    assert _shape(conn.wire) == ['INSERT INTO "taskq".jobs']


async def test_a_keyed_pool_enqueue_bounds_its_wait_without_a_savepoint_or_restore() -> None:
    """The bounded idempotency wait needs a transaction for SET LOCAL to
    span the INSERT — and nothing more when the transaction is the
    enqueue's own: a refusal aborts it outright, and the transaction-local
    bound dies with it, so the savepoint and the read-then-restore of the
    caller's lock_timeout that a caller-owned transaction needs are pure
    cost here."""
    args = _enqueue_args(idempotency_key="k-1")
    conn = _RecordingConn({"INSERT INTO": [_job_record(job_id=args.id)]})
    await _enqueue_on_pool(conn, args)
    assert _shape(conn.wire) == [
        "BEGIN",
        "SELECT set_config('lock_timeout', $1, tr",
        'INSERT INTO "taskq".jobs',
        "COMMIT",
    ]


async def test_a_keyed_enqueue_in_a_callers_transaction_keeps_the_restore_discipline() -> None:
    """A caller-owned transaction outlives the enqueue: the bound is set
    inside a savepoint and the caller's prior lock_timeout is read and
    restored before RELEASE, so nothing leaks onto the caller's later
    statements."""
    args = _enqueue_args(idempotency_key="k-1")
    conn = _RecordingConn(
        {
            "INSERT INTO": [_job_record(job_id=args.id)],
            "current_setting": [_Record({"current_setting": "0"})],
        }
    )
    async with conn.transaction():
        await _enqueue_with_conn(conn, _SQL, _SCHEMA, FakeClock(_NOW), args)  # type: ignore[arg-type]  # Why: duck-typed recording connection.
    assert _shape(conn.wire) == [
        "BEGIN",
        "SAVEPOINT",
        "SELECT current_setting('lock_timeout')",
        "SELECT set_config('lock_timeout', $1, tr",
        'INSERT INTO "taskq".jobs',
        "SELECT set_config('lock_timeout', $1, tr",
        "RELEASE",
        "COMMIT",
    ]
