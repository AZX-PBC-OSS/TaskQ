"""The singleton INSERT's unique-violation catch on a caller-owned connection.

The singleton arm is the one enqueue statement whose documented failure
mode is a typed, catchable refusal: a concurrent singleton insert wins
the ``jobs_singleton_uniq`` race and this INSERT loses it. The raw
``UniqueViolationError`` is a STATEMENT error — on a caller-owned
transaction it aborts the whole transaction, and converting it to
``SingletonCollisionError`` does not undo that, so the same typed
refusal left the caller's transaction dead on the violation path but
alive on the preflight path. The PG-backed pin for that contract is
``tests/test_rt_pools_enqueue_caller_conn.py``; these are the unit pins
for the SHAPE that delivers it: the violation is caught at a savepoint,
rolled back to it, and the typed error propagates with the transaction
depth unchanged.

The connection stand-in tracks savepoint depth and records whether each
entered scope exited by ROLLBACK (an exception was converted inside it)
or RELEASE — the same discipline as the bounded advisory acquire
(``taskq._advisory``) and the serialization pins in
``tests/test_unique_for_caller_conn_serialization.py``, verified
without a database.
"""

from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id
from taskq.backend._enqueue import _enqueue_with_conn
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend._sql_templates import render as render_sql
from taskq.exceptions import SingletonCollisionError
from taskq.testing.clock import FakeClock

_SCHEMA_LABEL = "taskq"
_SQL = render_sql(_SCHEMA_LABEL)
_NOW = datetime(2025, 1, 1, tzinfo=UTC)


# ── Record / connection stand-ins ─────────────────────────────────────


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
        "retry_base_seconds": 5.0,
        "retry_cap_seconds": 3600.0,
        "retry_backoff": "exponential",
        "retry_jitter": 0.2,
    }


class _Record:
    """Duck-typed asyncpg.Record — supports ``rec[key]``."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]


class _SavepointTx:
    """asyncpg.Transaction stand-in: bumps the connection's depth on enter
    and records the exit kind — rollback when an exception propagated
    through the scope (the savepoint conversion path), release otherwise."""

    def __init__(self, conn: "_ConnStandin") -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        self._conn.tx_depth += 1
        self._conn.events.append("savepoint-enter")

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._conn.tx_depth -= 1
        exited = "savepoint-rollback" if exc_type is not None else "savepoint-release"
        self._conn.events.append(exited)


class _ConnStandin:
    """ConnLike stand-in routing calls by SQL substring and recording the
    savepoint shape around each statement.

    ``caller_tx_open`` models the two caller shapes the savepoint must
    distinguish: a caller-owned OPEN transaction (True — the caller owns
    the scope, and a nested ``transaction()`` is a SAVEPOINT of it) and a
    bare autocommit connection (False — a ``transaction()`` opens a real
    short transaction that must be gone by the time the typed refusal
    raises).
    """

    def __init__(
        self,
        *,
        preflight_row: _Record | None = None,
        insert_rec: _Record | None = None,
        insert_exc: BaseException | None = None,
        caller_tx_open: bool = False,
    ) -> None:
        self._preflight_row = preflight_row
        self._insert_rec = insert_rec
        self._insert_exc = insert_exc
        self._caller_tx_open = caller_tx_open
        self.tx_depth = 0
        #: Ordered scope/statement events, e.g. "savepoint-enter".
        self.events: list[str] = []
        #: Statement kind -> standin-counted transaction depth when it ran.
        self.statement_depths: dict[str, int] = {}

    def is_in_transaction(self) -> bool:
        return self._caller_tx_open or self.tx_depth > 0

    def transaction(self) -> _SavepointTx:
        return _SavepointTx(self)

    def _note(self, kind: str) -> None:
        self.events.append(f"stmt:{kind}")
        self.statement_depths[kind] = self.tx_depth

    async def fetchval(self, sql: str, *args: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self._note("advisory_try_lock")
            return True
        self._note("fetchval")
        return 0

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        if "schedule_to_close FROM" in sql:
            self._note("singleton_preflight")
            return self._preflight_row
        if "RETURNING" in sql:
            self._note("insert")
            if self._insert_exc is not None:
                raise self._insert_exc
            return self._insert_rec
        return None

    async def execute(self, sql: str, *args: object) -> str:
        self._note("pg_notify" if "pg_notify" in sql else "execute")
        return "OK"


def _singleton_violation() -> asyncpg.UniqueViolationError:
    """The raw statement error the racing INSERT loses with."""
    exc = asyncpg.UniqueViolationError(
        'duplicate key value violates unique constraint "jobs_singleton_uniq"'
    )
    exc.constraint_name = "jobs_singleton_uniq"  # type: ignore[attr-defined]  # Why: asyncpg derives constraint_name at runtime from server diagnostics; the stand-in path never runs a server, so the classification input is assigned directly (same pattern as tests/test_enqueue_coverage.py).
    return exc


def _make_args(*, singleton: bool = False) -> EnqueueArgs:
    metadata: dict[str, object] = {"singleton": True} if singleton else {}
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_NOW,
        metadata=metadata,
        tags=(),
    )


# ── the violation path: caught at a savepoint, converted, scope intact ──


async def test_singleton_violation_on_caller_tx_rolls_back_to_savepoint() -> None:
    """The losing INSERT's UniqueViolationError must be rolled back to a
    savepoint and converted to the typed refusal with the caller's
    transaction exactly as TaskQ found it — parity with the preflight
    path, whose plain Python raise already keeps it usable."""
    conn = _ConnStandin(
        preflight_row=None,  # the race: the blocker is invisible to the preflight
        insert_exc=_singleton_violation(),
        caller_tx_open=True,
    )

    with pytest.raises(SingletonCollisionError) as exc_info:
        await _enqueue_with_conn(
            conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), _make_args(singleton=True)
        )

    err = exc_info.value
    assert err.actor == "test_actor"
    assert err.blocking_job_id is None, (
        "the Layer 2 catch has no preflight row (documented semantics)"
    )
    assert err.retry_after is None, "the Layer 2 catch carries no schedule_to_close to hint from"
    assert conn.events == [
        "stmt:singleton_preflight",
        "savepoint-enter",
        "stmt:insert",
        "savepoint-rollback",
    ], (
        f"the violation must be caught inside a savepoint and rolled back to it; events={conn.events}"
    )
    assert conn.statement_depths["insert"] == 1, "the INSERT must run INSIDE the savepoint"
    assert conn.tx_depth == 0, (
        "the savepoint must be closed by the rollback — the caller's transaction "
        "depth is unchanged, so the scope TaskQ hands back is the one it was given"
    )


async def test_singleton_violation_on_bare_conn_ends_not_in_transaction() -> None:
    """On a bare connection the savepoint tier opens a real short
    transaction; the rollback must return the connection to bare — a
    dangling transaction would pin the caller's next use."""
    conn = _ConnStandin(
        preflight_row=None,
        insert_exc=_singleton_violation(),
        caller_tx_open=False,
    )

    with pytest.raises(SingletonCollisionError):
        await _enqueue_with_conn(
            conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), _make_args(singleton=True)
        )

    assert conn.tx_depth == 0
    assert conn.is_in_transaction() is False, (
        "the short transaction the savepoint tier opened on the bare connection "
        "must be rolled back fully, not left dangling"
    )
    assert conn.events[-1] == "savepoint-rollback"


# ── the preflight path: untouched, no savepoint, attributed blocker ────


async def test_singleton_preflight_refusal_opens_no_savepoint_and_names_blocker() -> None:
    """The preflight arm is a plain Python raise after a SELECT — no
    statement error, so it needs (and must not pay for) a savepoint. Its
    refusal names the blocking row and, when the blocker has no
    schedule_to_close (the stranded-blocker shape), carries
    ``retry_after=None``."""
    blocker_id = new_job_id()
    conn = _ConnStandin(
        preflight_row=_Record({"id": blocker_id, "schedule_to_close": None}),
    )

    with pytest.raises(SingletonCollisionError) as exc_info:
        await _enqueue_with_conn(
            conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), _make_args(singleton=True)
        )

    err = exc_info.value
    assert err.blocking_job_id == blocker_id
    assert err.retry_after is None
    assert conn.events == ["stmt:singleton_preflight"], (
        f"the preflight refusal must run no INSERT and open no savepoint; events={conn.events}"
    )
    assert conn.tx_depth == 0


# ── the savepoint stays scoped to the singleton arm ────────────────────


async def test_plain_enqueue_insert_runs_without_savepoint() -> None:
    """A non-singleton enqueue keeps the zero-extra-round-trip INSERT path:
    its violation outcomes are raw or migration-window errors, not
    catch-and-continue refusals, so it must not pay for a savepoint."""
    conn = _ConnStandin(insert_rec=_Record(_full_record()))

    row = await _enqueue_with_conn(conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), _make_args())

    assert isinstance(row, JobRow)
    assert conn.events == ["stmt:insert", "stmt:pg_notify"], (
        f"a plain enqueue must open no savepoint; events={conn.events}"
    )
    assert conn.statement_depths["insert"] == 0


async def test_singleton_insert_success_releases_savepoint_and_notifies_outside_it() -> None:
    """The savepoint bounds exactly the INSERT: on success it RELEASEs
    before the notify, so the wrap never widens into the rest of the
    enqueue."""
    conn = _ConnStandin(insert_rec=_Record(_full_record()))

    row = await _enqueue_with_conn(
        conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), _make_args(singleton=True)
    )

    assert isinstance(row, JobRow)
    assert conn.events == [
        "stmt:singleton_preflight",
        "savepoint-enter",
        "stmt:insert",
        "savepoint-release",
        "stmt:pg_notify",
    ], f"events={conn.events}"
    assert conn.statement_depths["singleton_preflight"] == 0, (
        "the preflight is a read outside the savepoint"
    )
    assert conn.statement_depths["insert"] == 1, "the INSERT runs inside the savepoint"
    assert conn.statement_depths["pg_notify"] == 0, "the savepoint closes before the notify"
    assert conn.tx_depth == 0
