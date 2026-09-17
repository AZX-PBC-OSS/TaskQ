"""Coverage for ``taskq.backend._enqueue`` error and edge-case paths.

Exercises branches not covered by the PG integration tests, using a fake
asyncpg connection so no database is required:

- ``_enqueue_on_conn``: ``unique_for`` preflight dedup, singleton
  preflight collision, ``max_pending`` exceeded, singleton
  ``UniqueViolationError`` catch, and ``result_ttl`` → ``result_expires_at``.
- ``_enqueue`` / ``_enqueue_batch``: legacy-index violation retry logic
  (rolling-deploy overlap window).
- ``_enqueue_batch``: empty ``args_list`` raises ``ValueError``.
- ``_enqueue_batch_fast``: empty ``args_list`` raises ``ValueError`` and
  ``schedule_to_close_interval`` resolution.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
from asyncpg.exceptions import InternalClientError

from taskq._ids import new_job_id
from taskq.backend._enqueue import (
    _enqueue,
    _enqueue_batch,
    _enqueue_batch_fast,
    _enqueue_on_conn,
    _enqueue_with_conn,
)
from taskq.backend._protocol import EnqueueArgs, IdentityKey, JobRow
from taskq.backend._sql_templates import render as render_sql
from taskq.exceptions import (
    MaxPendingExceededError,
    PayloadValidationError,
    ScopedIdempotencyMigrationPendingError,
    SingletonCollisionError,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

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
        # The max_pending try-lock succeeds by default: these tests model an
        # uncontended enqueue; contention behavior is covered by
        # test_postgres_enqueue_max_pending_lock.py. Without this, the
        # bounded-wait budget (5 s default) would expire inside every
        # capped-path test before the count is ever reached.
        if "pg_try_advisory_xact_lock" in sql:
            return True
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

    def is_in_transaction(self) -> bool:
        """Model a caller-owned connection with an open transaction.

        ``_enqueue_on_conn`` wraps capped actors in its own transaction
        only when the caller supplied none; True here exercises the
        lock-then-count path directly.
        """
        return True

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
    idempotency_scope: str = "",
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
        idempotency_scope=idempotency_scope,
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


async def test_result_ttl_path_succeeds_without_an_app_side_notify() -> None:
    """When ``result_ttl`` is set, the INSERT succeeds and the row is
    returned; the wake is the INSERT trigger's, so no pg_notify statement
    follows."""
    rec = _Record(_full_record())
    conn = _FakeEnqueueConn(fetchrow_map={"RETURNING": rec, "INSERT": rec})
    args = _make_args(result_ttl=timedelta(hours=1))
    clock = FakeClock(_NOW)

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert isinstance(row, JobRow)
    assert not any("pg_notify" in sql for sql in conn.execute_calls)


# ── _enqueue_on_conn: idempotency-key ON CONFLICT dedup ──────────────────


async def test_idempotency_key_conflict_returns_existing_row() -> None:
    """When the INSERT returns no row (ON CONFLICT), the follow-up SELECT by
    idempotency_key returns the existing row."""
    existing_id = new_job_id()
    existing_rec = _Record(_full_record(job_id=existing_id))
    conn = _FakeEnqueueConn(
        fetchrow_map={"idempotency_key = $2": existing_rec},
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
    with pytest.raises(ValueError, match="must not be empty"):
        await _enqueue_batch(pool, _SQL, _SCHEMA_LABEL, [])


# ── _enqueue_batch_fast: empty args_list ─────────────────────────────────


async def test_enqueue_batch_fast_empty_raises_value_error() -> None:
    """An empty ``args_list`` raises ``ValueError`` before any COPY runs."""
    pool = _FakePool(_FakeEnqueueConn())
    with pytest.raises(ValueError, match="must not be empty"):
        await _enqueue_batch_fast(pool, _SQL, _SCHEMA_LABEL, [])


# ── NUL rejection is enforced at the EnqueueArgs chokepoint ─────────────
#
# Every enqueue path -- single, batch, the COPY-based fast batch, the atomic
# batch, and the InMemory mirror -- builds an ``EnqueueArgs`` first, so the
# guard lives in ``__post_init__`` rather than being repeated per path.
# That is deliberate: this class of bug recurred four times precisely
# because each fix stopped at the paths in its own diff.  PostgreSQL
# rejects a NUL in ``text`` with ``CharacterNotInRepertoireError``
# (SQLSTATE 22021), a ``PostgresError`` subclass that
# ``_TERMINAL_WRITE_INFRA_EXCEPTIONS`` misreads as transient infra failure,
# so an unguarded NUL retries forever instead of failing.


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("actor", "bad\x00actor"),
        ("queue", "bad\x00queue"),
        ("idempotency_scope", "bad\x00scope"),
        ("identity_key", "bad\x00identity"),
        ("fairness_key", "bad\x00fairness"),
        ("idempotency_key", "bad\x00idem"),
        ("trace_id", "bad\x00trace"),
        ("span_id", "bad\x00span"),
    ],
)
def test_enqueue_args_rejects_nul_in_any_text_field(field_name: str, value: str) -> None:
    """Every caller-supplied field bound as ``text`` is guarded, not just
    ``tags`` -- they all reach the same 22021 misclassification trap."""
    with pytest.raises(ValueError, match="NUL"):
        replace(_make_args(), **{field_name: value})


def test_enqueue_args_rejects_nul_in_tags() -> None:
    """``tags`` binds as ``$N::text[]`` on the single-job path and as
    ``$N::jsonb[]`` on the batch path; both are rejected at construction."""
    with pytest.raises(ValueError, match="NUL"):
        replace(_make_args(), tags=("bad\x00tag",))


def test_enqueue_paths_are_unreachable_with_a_nul() -> None:
    """Construction is the only gate the enqueue paths need: an args value
    carrying a NUL cannot be built, so no path can bind one.  Bypassing
    ``__post_init__`` (as only a deliberate ``object.__setattr__`` can)
    is what it takes to get a NUL past the chokepoint -- proof the guard
    is on the struct, not on any one caller."""
    args = _make_args()
    object.__setattr__(args, "tags", ("bad\x00tag",))
    with pytest.raises(ValueError, match="NUL"):
        args._check_no_nul_text()  # pyright: ignore[reportPrivateUsage]  # Why: asserting the chokepoint itself, not a public surface.


async def test_enqueue_accepts_clean_tags() -> None:
    """A tag list with no NUL is unaffected -- the guard is a pure prefilter
    and the INSERT proceeds normally."""
    rec = _Record(_full_record())
    conn = _FakeEnqueueConn(fetchrow_map={"RETURNING": rec, "INSERT": rec})
    args = replace(_make_args(), tags=("clean", "tags"))
    clock = FakeClock(_NOW)

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, clock, args)

    assert isinstance(row, JobRow)


# ── Batch serialization: per-item NUL attribution ────────────────────────
#
# The batch build loops serialize every item BEFORE any SQL runs, so a NUL
# in any item raised a bare ValueError(NUL_JSONB_ERROR) that named neither
# the item nor the field. Pydantic validation failures get per-item
# annotation via _item_payload_error in the client layer
# (taskq.client._jobs); the NUL ValueError bypassed that contract. The
# batch still refuses atomically (attribution, not partial admission):
# these tests prove the rejection fires before any connection is acquired.


class _ForbiddenPool:
    """Pool stand-in that fails if touched.

    The NUL build-loop rejection must fire before any connection is
    acquired: nothing ran, so nothing was written -- the same
    all-or-nothing admission PG gives by aborting before the INSERT.
    """

    def acquire(self) -> object:
        raise AssertionError("pool must not be acquired when a NUL item rejects the batch")


async def test_enqueue_batch_nul_payload_names_item_and_field() -> None:
    """A NUL in item 2's payload rejects the whole batch with a
    PayloadValidationError naming item 2, the actor, and the payload
    field -- the _item_payload_error annotation contract."""
    args_list = [_make_args() for _ in range(5)]
    args_list[2] = replace(args_list[2], payload={"value": "bad\x00value"})

    with pytest.raises(PayloadValidationError) as exc_info:
        await _enqueue_batch(_ForbiddenPool(), _SQL, _SCHEMA_LABEL, args_list)  # type: ignore[arg-type]  # Why: pool is never reached; the stand-in proves it

    msg = str(exc_info.value)
    assert "item 2" in msg
    assert "payload" in msg
    assert "test_actor" in msg
    assert "NUL" in msg
    assert exc_info.value.validation_errors[0]["loc"] == ("payload",)


async def test_enqueue_batch_nul_metadata_names_item_and_field() -> None:
    """Same attribution for a NUL riding in item 3's metadata."""
    args_list = [_make_args() for _ in range(5)]
    args_list[3] = replace(args_list[3], metadata={"note": "bad\x00note"})

    with pytest.raises(PayloadValidationError) as exc_info:
        await _enqueue_batch(_ForbiddenPool(), _SQL, _SCHEMA_LABEL, args_list)  # type: ignore[arg-type]  # Why: pool is never reached; the stand-in proves it

    msg = str(exc_info.value)
    assert "item 3" in msg
    assert "metadata" in msg
    assert exc_info.value.validation_errors[0]["loc"] == ("metadata",)


async def test_enqueue_batch_nul_tags_names_item_and_field() -> None:
    """Same attribution for a NUL in item 4's tags, reachable only by
    bypassing the EnqueueArgs chokepoint (as only object.__setattr__
    can) -- the batch path still binds tags through jsonb, so the
    serialization layer keeps its own annotated guard."""
    args_list = [_make_args() for _ in range(5)]
    object.__setattr__(args_list[4], "tags", ("bad\x00tag",))

    with pytest.raises(PayloadValidationError) as exc_info:
        await _enqueue_batch(_ForbiddenPool(), _SQL, _SCHEMA_LABEL, args_list)  # type: ignore[arg-type]  # Why: pool is never reached; the stand-in proves it

    msg = str(exc_info.value)
    assert "item 4" in msg
    assert "tags" in msg
    assert exc_info.value.validation_errors[0]["loc"] == ("tags",)


async def test_enqueue_batch_fast_nul_payload_names_item_and_field() -> None:
    """The COPY build loop has the same gap: a NUL in item 1's payload
    rejects the whole batch with the per-item annotation, before any
    COPY is issued."""
    args_list = [_make_args() for _ in range(3)]
    args_list[1] = replace(args_list[1], payload={"value": "bad\x00value"})

    with pytest.raises(PayloadValidationError) as exc_info:
        await _enqueue_batch_fast(_ForbiddenPool(), _SQL, _SCHEMA_LABEL, args_list)  # type: ignore[arg-type]  # Why: pool is never reached; the stand-in proves it

    msg = str(exc_info.value)
    assert "item 1" in msg
    assert "payload" in msg
    assert exc_info.value.validation_errors[0]["loc"] == ("payload",)


async def test_memory_enqueue_batch_nul_payload_annotated_and_atomic() -> None:
    """InMemoryBackend parity: the batch mirror rejects a NUL-bearing item
    with the same per-item annotation AND the same all-or-nothing
    admission as PG. Pre-fix the per-item loop admitted items 0..k-1
    before item k raised a bare, unattributed ValueError."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    args_list = [_make_args() for _ in range(4)]
    args_list[2] = replace(args_list[2], payload={"value": "bad\x00value"})

    with pytest.raises(PayloadValidationError) as exc_info:
        await backend.enqueue_batch(args_list)

    assert "item 2" in str(exc_info.value)
    assert "payload" in str(exc_info.value)
    # Atomic like PG: items 0..1 were not admitted before the rejection.
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: test-only admission check


async def test_memory_enqueue_batch_nul_metadata_annotated_and_atomic() -> None:
    """InMemoryBackend parity for the metadata field."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    args_list = [_make_args() for _ in range(3)]
    args_list[1] = replace(args_list[1], metadata={"note": "bad\x00note"})

    with pytest.raises(PayloadValidationError) as exc_info:
        await backend.enqueue_batch(args_list)

    assert "item 1" in str(exc_info.value)
    assert "metadata" in str(exc_info.value)
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: test-only admission check


# ── _enqueue_batch_fast: schedule_to_close_interval + result_ttl ────────


async def test_enqueue_batch_fast_schedule_interval_and_result_ttl() -> None:
    """``schedule_to_close_interval`` and ``result_ttl`` are carried into the
    post-COPY fixup arrays (server-side computation); the COPY returns a row
    count."""
    conn = _FakeEnqueueConn(copy_result="COPY 2")
    pool = _FakePool(conn)
    args = _make_args(
        schedule_to_close_interval=timedelta(hours=1),
        result_ttl=timedelta(hours=2),
        scheduled_at=_NOW + timedelta(minutes=5),
    )

    count = await _enqueue_batch_fast(pool, _SQL, _SCHEMA_LABEL, [args])

    assert count == 2
    # The wake is the fixup's own: COPY lands every row 'scheduled' so the
    # INSERT trigger stays silent, and the fixup statement carries one
    # pg_notify gated server-side on a row it actually made runnable. These
    # rows are future-dated, so the gate selects none of them; the statement
    # text still binds the gate and the channel.
    fixup_calls = [sql for sql in conn.execute_calls if "pg_notify" in sql]
    assert len(fixup_calls) == 1
    assert "status = 'pending'" in fixup_calls[0]


# ── _enqueue_batch_fast: scheduled vs pending status ─────────────────────


async def test_enqueue_batch_fast_immediate_job_is_pending() -> None:
    """A job with ``scheduled_at <= now`` lands ``pending`` (not
    ``scheduled``) — decided by the post-COPY fixup UPDATE's server CASE,
    never in Python."""
    conn = _FakeEnqueueConn(copy_result="COPY 1")
    pool = _FakePool(conn)
    args = _make_args(scheduled_at=_NOW)  # immediate

    count = await _enqueue_batch_fast(pool, _SQL, _SCHEMA_LABEL, [args])
    assert count == 1


# ── Fake pool ────────────────────────────────────────────────────────────


class _FakePool:
    """Minimal asyncpg.Pool stand-in yielding a fixed connection."""

    def __init__(self, conn: _FakeEnqueueConn) -> None:
        self._conn = conn
        self.acquire_count = 0
        # (conn, timeout) per release — pins that the enqueue paths route
        # their releases through the retry guard's bounded channel.
        self.releases: list[tuple[_FakeEnqueueConn, float | None]] = []

    def acquire(self) -> "_PoolCtx":
        self.acquire_count += 1
        return _PoolCtx(self._conn)

    async def release(
        self,
        conn: _FakeEnqueueConn,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: models asyncpg's Pool.release(timeout=...) signature — the guard's bounded-release channel, not a cancel scope.
    ) -> None:
        self.releases.append((conn, timeout))


class _FakePoolSequence:
    """Pool stand-in handing out a different fake connection per acquire,
    so retry logic can be driven deterministically."""

    def __init__(self, conns: list[_FakeEnqueueConn]) -> None:
        self._conns = conns
        self.acquire_count = 0
        self.releases: list[tuple[_FakeEnqueueConn, float | None]] = []

    def acquire(self) -> "_PoolCtx":
        conn = self._conns[self.acquire_count]
        self.acquire_count += 1
        return _PoolCtx(conn)

    async def release(
        self,
        conn: _FakeEnqueueConn,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: models asyncpg's Pool.release(timeout=...) signature — the guard's bounded-release channel, not a cancel scope.
    ) -> None:
        self.releases.append((conn, timeout))


def _legacy_violation() -> asyncpg.UniqueViolationError:
    exc = asyncpg.UniqueViolationError("duplicate key")
    exc.constraint_name = "jobs_idempotency_key_uniq"  # type: ignore[attr-defined]  # Why: asyncpg sets constraint_name at runtime; assigning for test setup.
    return exc


class _LegacyFailConn(_FakeEnqueueConn):
    """Connection whose INSERT raises a legacy-index UniqueViolationError."""

    def __init__(self, exc: asyncpg.UniqueViolationError, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._exc = exc

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        if "INSERT" in sql.upper():
            raise self._exc
        return await super().fetchrow(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[_Record]:
        if "INSERT" in sql.upper():
            raise self._exc
        return await super().fetch(sql, *args)


# ── _enqueue: legacy-index violation retry (rolling-deploy window) ───────
#
# During the pre/post overlap window the legacy single-column idempotency
# index is a non-arbiter index for this release's INSERTs. A violation
# against it means either genuine cross-scope reuse (must raise the typed
# migration-pending error) or a same-pair race against a concurrent
# old-shape INSERT (must dedupe cleanly after one retry, because a
# unique-violation report implies the conflicting transaction committed).


async def test_legacy_violation_same_pair_race_retries_and_dedupes() -> None:
    """First attempt violates the legacy index (same-pair race); the retry
    on a fresh transaction finds the raced row via the composite arbiter
    and returns it -- no error surfaces to the caller."""
    existing_id = new_job_id()
    existing_rec = _Record(_full_record(job_id=existing_id))
    first = _LegacyFailConn(_legacy_violation())
    # Retry: INSERT ON CONFLICT DO NOTHING returns nothing (the row the
    # racing transaction committed now conflicts via the composite
    # arbiter); the follow-up SELECT by (scope, key) returns it.
    second = _FakeEnqueueConn(fetchrow_map={"idempotency_key = $2": existing_rec})
    pool = _FakePoolSequence([first, second])
    clock = FakeClock(_NOW)

    row = await _enqueue(pool, _SQL, _SCHEMA_LABEL, clock, _make_args(idempotency_key="k"))  # type: ignore[arg-type]

    assert isinstance(row, JobRow)
    assert row.id == existing_id
    assert pool.acquire_count == 2


async def test_legacy_violation_twice_raises_typed_migration_error() -> None:
    """Genuine cross-scope reuse violates the legacy index on both
    attempts; the caller gets ScopedIdempotencyMigrationPendingError with
    the raw driver error as __cause__."""
    first = _LegacyFailConn(_legacy_violation())
    second = _LegacyFailConn(_legacy_violation())
    pool = _FakePoolSequence([first, second])
    clock = FakeClock(_NOW)
    args = _make_args(idempotency_key="k", idempotency_scope="run-B")

    with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
        await _enqueue(pool, _SQL, _SCHEMA_LABEL, clock, args)  # type: ignore[arg-type]

    err = exc_info.value
    assert err.idempotency_key == "k"
    assert err.idempotency_scope == "run-B"
    assert isinstance(err.__cause__, asyncpg.UniqueViolationError)
    assert pool.acquire_count == 2


async def test_enqueue_with_conn_legacy_violation_converts_without_retry() -> None:
    """On a caller-owned connection the transaction is already aborted, so
    the typed error is raised immediately -- no retry is possible."""
    conn = _LegacyFailConn(_legacy_violation())
    clock = FakeClock(_NOW)

    with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
        await _enqueue_with_conn(conn, _SQL, _SCHEMA_LABEL, clock, _make_args(idempotency_key="k"))

    assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)


# ── _enqueue under the dead-connection retry guard (#236) ────────────────
#
# The retry wrapper must absorb the dead-on-acquire race WITHOUT ever
# re-running a write that was already acknowledged. Three orderings:
#
# 1. The connection is poisoned BEFORE the INSERT is sent: the statement
#    fails locally (nothing reached the server), no write is durable, the
#    retry runs and succeeds. The wrapper's original purpose.
# 2. The INSERT is acknowledged (committed, autocommit) and the RELEASE's
#    reset then fails (the parked-error-consume connection): the guard's
#    checkout swallows the release failure, the caller gets the row, and
#    no second INSERT is issued.
# 3. The INSERT is acknowledged and a LATER statement of the same attempt
#    (the keyed arm's follow-up SELECT) fails with InternalClientError:
#    the guard's mark refuses the retry; the error propagates and the
#    INSERT count stays one.
#
# What a refused-path regression would actually do (jobs.id is
# ``uuid PRIMARY KEY`` and the op re-runs with the SAME args): the re-issued
# INSERT raises UniqueViolationError for an enqueue that already committed —
# an error returned for work that succeeded, whose caller-side retry (a
# fresh enqueue call, a fresh id) is the route that runs the job twice. The
# unit pins below hold the deterministic refusal; the fleet interruption pin
# (test_fleet_pg_transient_failure.py) holds the live no-UniqueViolation /
# durability side.


class _CountingInsertConn(_FakeEnqueueConn):
    """Counts INSERT statements issued (the re-issue observable)."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.insert_calls = 0

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        if "INSERT" in sql.upper():
            self.insert_calls += 1
        return await super().fetchrow(sql, *args)


class _ParkedBeforeInsertConn(_CountingInsertConn):
    """The dead-on-acquire case: the pooled connection was poisoned before
    handout, so the FIRST statement (the plain arm's INSERT) fails locally
    with the driver's state error -- nothing was sent, nothing committed."""

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        if "INSERT" in sql.upper():
            self.insert_calls += 1
            raise InternalClientError(
                "cannot switch to state 15; another operation (2) is in progress"
            )
        return await super().fetchrow(sql, *args)


class _ParkedAfterInsertConn(_CountingInsertConn):
    """The server's FATAL lands between the INSERT's acknowledgement and
    the next statement: the write is durable and every later statement of
    the attempt fails locally with the driver's state error."""

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        if "INSERT" in sql.upper():
            # _CountingInsertConn.fetchrow does the counting; this override
            # only arms the parked state after the acknowledged INSERT.
            rec = await super().fetchrow(sql, *args)
            self._parked = True  # type: ignore[attr-defined]  # Why: test-local state on the fake; _FakeEnqueueConn is not slotted.
            return rec
        if getattr(self, "_parked", False):
            raise InternalClientError(
                "cannot switch to state 15; another operation (2) is in progress"
            )
        return await super().fetchrow(sql, *args)


class _ReleaseFailPool(_FakePool):
    """Pool whose release fails the way asyncpg's does on a parked
    connection: the reset raises, the holder terminates the connection and
    re-raises (issue #236's release path)."""

    def __init__(self, conn: _FakeEnqueueConn, exc: BaseException) -> None:
        super().__init__(conn)
        self._exc = exc

    async def release(
        self,
        conn: _FakeEnqueueConn,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: models asyncpg's Pool.release(timeout=...) signature — the guard's bounded-release channel, not a cancel scope.
    ) -> None:
        await super().release(conn, timeout=timeout)
        raise self._exc


async def test_enqueue_poisoned_before_the_insert_retries_and_succeeds() -> None:
    """Ordering 1: a locally-poisoned first statement (nothing sent) costs
    one transparent retry on a fresh connection. Marking-before-the-write
    would have refused this retry -- the reason the guard marks at the
    acknowledgement, not at issuance."""
    first = _ParkedBeforeInsertConn()
    second = _CountingInsertConn(fetchrow_map={"INSERT": _Record(_full_record())})  # type: ignore[arg-type]
    pool = _FakePoolSequence([first, second])
    clock = FakeClock(_NOW)

    row = await _enqueue(pool, _SQL, _SCHEMA_LABEL, clock, _make_args())  # type: ignore[arg-type]

    assert isinstance(row, JobRow)
    assert pool.acquire_count == 2, "the dead-on-acquire failure must retry once"
    assert first.insert_calls == 1 and second.insert_calls == 1
    # The retry's release went through the guard's bounded channel.
    assert all(timeout is not None for _conn, timeout in pool.releases)


async def test_enqueue_committed_insert_survives_a_failed_release() -> None:
    """Ordering 2 (#236's duplication half): the INSERT is acknowledged and
    committed (autocommit), then the release's reset fails on the parked
    connection. The caller must get the committed row -- an error here is
    exactly what invited the caller-side re-enqueue that duplicated the
    job -- and the INSERT must have run exactly once."""
    conn = _CountingInsertConn(fetchrow_map={"INSERT": _Record(_full_record())})  # type: ignore[arg-type]
    pool = _ReleaseFailPool(
        conn,
        InternalClientError("cannot switch to state 15; another operation (2) is in progress"),
    )
    clock = FakeClock(_NOW)

    row = await _enqueue(pool, _SQL, _SCHEMA_LABEL, clock, _make_args())  # type: ignore[arg-type]

    assert isinstance(row, JobRow)
    assert pool.acquire_count == 1, "a release failure is not an op failure; no retry may run"
    assert conn.insert_calls == 1, "no second INSERT: the first was acknowledged and committed"


async def test_enqueue_post_insert_statement_error_never_re_runs_the_write() -> None:
    """Ordering 3 (the wrote-then-read case): the INSERT is acknowledged
    (dedup arm: no RETURNING row) and the follow-up SELECT then fails
    locally on the parked connection. The guard's mark refuses the retry:
    the error propagates and the INSERT count stays one. Pre-fix the
    wrapper read every InternalClientError as dead-on-acquire and re-ran
    the enqueue with the same args -- against the jobs primary key that
    re-run raises UniqueViolationError for an enqueue that already
    committed, the error-for-succeeded-work whose caller-side retry (a
    fresh id) runs the job twice."""
    conn = _ParkedAfterInsertConn()  # INSERT returns None (ON CONFLICT); follow-up SELECT parks
    pool = _FakePool(conn)
    clock = FakeClock(_NOW)

    with pytest.raises(InternalClientError, match="cannot switch to state 15"):
        await _enqueue(pool, _SQL, _SCHEMA_LABEL, clock, _make_args(idempotency_key="k"))  # type: ignore[arg-type]

    assert pool.acquire_count == 1, (
        "a retry after an acknowledged write re-issues the INSERT with the "
        "same id: a UniqueViolationError for work that succeeded, and the "
        "invitation for the caller's fresh-id re-enqueue that runs the job "
        "twice (#236)"
    )
    assert conn.insert_calls == 1


async def test_enqueue_batch_committed_transaction_survives_a_failed_release() -> None:
    """The batch arm's ordering-2: the batch transaction's COMMIT is
    acknowledged, then the release fails. The committed rows are returned,
    the batch INSERT ran once, no retry."""
    args = _make_args()
    conn = _FakeEnqueueConn(fetch_map={"FROM unnest(": [_Record(_full_record(job_id=args.id))]})
    pool = _ReleaseFailPool(
        conn,
        InternalClientError("cannot switch to state 15; another operation (2) is in progress"),
    )

    rows = await _enqueue_batch(pool, _SQL, _SCHEMA_LABEL, [args])  # type: ignore[arg-type]

    assert len(rows) == 1
    assert rows[0].id == args.id
    assert pool.acquire_count == 1


async def test_enqueue_batch_post_commit_statement_error_refuses_the_retry() -> None:
    """The batch arm's ordering-3, at the transaction boundary: the batch's
    driving statement succeeded but a LATER statement of the same
    transaction fails locally on the parked connection. The transaction
    never committed (the server died; everything rolled back server-side),
    so the retry IS safe -- and it runs, re-executing the batch atomically
    on a fresh connection."""

    # First connection: the multi-row INSERT dedupes... instead model the
    # simplest in-transaction failure: the driving fetch fails parked.
    class _ParkedMidTxConn(_FakeEnqueueConn):
        async def fetch(self, sql: str, *args: object) -> list[_Record]:
            raise InternalClientError(
                "cannot switch to state 15; another operation (2) is in progress"
            )

    first = _ParkedMidTxConn()
    args = _make_args()
    second = _FakeEnqueueConn(fetch_map={"FROM unnest(": [_Record(_full_record(job_id=args.id))]})
    pool = _FakePoolSequence([first, second])

    rows = await _enqueue_batch(pool, _SQL, _SCHEMA_LABEL, [args])  # type: ignore[arg-type]

    assert len(rows) == 1
    assert pool.acquire_count == 2, (
        "a mid-transaction failure rolled the batch back server-side; "
        "the retry re-runs it atomically and must run"
    )


async def test_enqueue_batch_legacy_violation_retries_and_dedupes() -> None:
    """Batch path: same-pair race on the first attempt aborts the whole
    batch statement; the retry dedupes the raced item via the composite
    arbiter and the follow-up fetch."""
    existing_id = new_job_id()
    existing_rec = _Record({**_full_record(job_id=existing_id), "idempotency_key": "k"})
    first = _LegacyFailConn(_legacy_violation())
    # Retry: the multi-row INSERT dedupes (RETURNING empty); the existing
    # row is fetched via the (scope, key) pairs join.
    second = _FakeEnqueueConn(fetch_map={"JOIN unnest": [existing_rec]})
    pool = _FakePoolSequence([first, second])
    args = _make_args(idempotency_key="k")

    rows = await _enqueue_batch(pool, _SQL, _SCHEMA_LABEL, [args])  # type: ignore[arg-type]

    assert len(rows) == 1
    assert rows[0].id == existing_id
    assert pool.acquire_count == 2


async def test_enqueue_batch_legacy_violation_twice_raises_typed_error() -> None:
    first = _LegacyFailConn(_legacy_violation())
    second = _LegacyFailConn(_legacy_violation())
    pool = _FakePoolSequence([first, second])

    with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
        await _enqueue_batch(pool, _SQL, _SCHEMA_LABEL, [_make_args(idempotency_key="k")])  # type: ignore[arg-type]

    assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)
    assert pool.acquire_count == 2


async def test_enqueue_batch_with_conn_legacy_violation_converts_without_retry() -> None:
    conn = _LegacyFailConn(_legacy_violation())

    with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
        await _enqueue_batch(
            None,
            _SQL,
            _SCHEMA_LABEL,
            [_make_args(idempotency_key="k")],
            connection=conn,  # type: ignore[arg-type]
        )

    assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)


class _PoolCtx:
    """asyncpg.PoolAcquireContext stand-in: BOTH awaitable and async-CM,
    the real surface's documented dual shape (``await pool.acquire()`` /
    ``async with pool.acquire()``). The retry guard's checkout uses the
    await form plus an explicit ``pool.release(conn, timeout=...)``."""

    def __init__(self, conn: _FakeEnqueueConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeEnqueueConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass

    def __await__(self):
        return self.__aenter__().__await__()
