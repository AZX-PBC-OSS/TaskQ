"""The unique_for single-flight wrap on a caller-supplied connection.

``_enqueue_on_conn`` opens its own transaction when the caller-supplied
connection carries none and the enqueue needs transaction-scoped
serialization — a capped actor's count-then-insert, and the unique_for
check-then-insert. A transaction-scoped advisory lock only serializes
statements that share one transaction: on a bare autocommit connection
every statement is its own transaction, so without the wrap the lock
releases at statement end and the preflight races (the measured "100
concurrent enqueues produced 6 rows" case; the PG-backed race pin is
``tests/test_postgres_unique_for_single_flight.py``). A caller who
already holds a transaction keeps the existing semantics: the lock then
spans that caller's transaction, and the enqueue path opens nothing.

These are unit pins: the wrap itself is the observable, recorded by a
connection stand-in that notes which statements ran with a transaction
open, so the serialization precondition is verified without a database.
Transaction-scoped serialization is the standard approach for deduplication
and concurrency caps: the check and insert must share one advisory-lock
scope to prevent concurrent dispatchers from racing. This is the guarantee
TaskQ provides.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from taskq._ids import new_job_id
from taskq.backend._enqueue import _enqueue_on_conn
from taskq.backend._protocol import EnqueueArgs, IdentityKey, JobRow
from taskq.backend._sql_templates import render as render_sql
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
    }


class _Record:
    """Duck-typed asyncpg.Record — supports ``rec[key]``."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]


class _Transaction:
    """asyncpg.Transaction stand-in tracking open depth on its connection."""

    def __init__(self, conn: "_ConnStandin") -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        self._conn.tx_depth += 1

    async def __aexit__(self, *args: object) -> None:
        self._conn.tx_depth -= 1


class _ConnStandin:
    """ConnLike stand-in that routes calls by SQL substring and records,
    per statement, whether an open transaction surrounded it.

    ``caller_tx_open`` models the two caller shapes the wrap must
    distinguish: a bare autocommit connection (False — every statement
    its own transaction unless the enqueue path opens one) and a
    caller-owned OPEN transaction (True — the caller owns the scope).
    """

    def __init__(
        self,
        *,
        fetchrow_map: dict[str, _Record | None] | None = None,
        fetchval_map: dict[str, object] | None = None,
        caller_tx_open: bool = False,
    ) -> None:
        self._fetchrow_map = fetchrow_map or {}
        self._fetchval_map = fetchval_map or {}
        self._caller_tx_open = caller_tx_open
        self.tx_depth = 0
        #: (statement kind, a transaction was open when it ran)
        self.statements: list[tuple[str, bool]] = []

    def is_in_transaction(self) -> bool:
        return self._caller_tx_open or self.tx_depth > 0

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    def _record(self, kind: str) -> None:
        self.statements.append((kind, self.is_in_transaction()))

    async def fetchval(self, sql: str, *args: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self._record("advisory_try_lock")
            return True
        kind = "max_pending_count" if "count(*)" in sql else "fetchval"
        self._record(kind)
        for pattern, result in self._fetchval_map.items():
            if pattern in sql:
                return result
        return 0

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        if "identity_key = $2" in sql:
            kind = "unique_for_preflight"
        elif "RETURNING" in sql:
            kind = "insert"
        elif "idempotency_key = $2" in sql:
            kind = "select_by_key"
        elif "schedule_to_close FROM" in sql:
            kind = "singleton_preflight"
        else:
            kind = "fetchrow"
        self._record(kind)
        for pattern, result in self._fetchrow_map.items():
            if pattern in sql:
                return result
        return None

    async def execute(self, sql: str, *args: object) -> str:
        self._record("pg_notify" if "pg_notify" in sql else "execute")
        return "OK"


def _make_args(
    *,
    unique_for: timedelta | None = None,
    identity_key: str | None = None,
    max_pending: int | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_NOW,
        identity_key=IdentityKey(identity_key) if identity_key is not None else None,
        unique_for=unique_for,
        unique_states=("pending", "scheduled", "running"),
        max_pending=max_pending,
        metadata={},
        tags=(),
    )


def _kinds_in_tx(conn: _ConnStandin) -> set[str]:
    return {kind for kind, in_tx in conn.statements if in_tx}


# ── unique_for on a bare caller connection ────────────────────────────


async def test_unique_for_on_bare_caller_conn_runs_lock_preflight_and_insert_in_one_transaction() -> (
    None
):
    """The unique_for arm needs the same wrap the capped-actor arm has.

    The advisory lock is transaction-scoped: on a bare caller connection
    without a wrap it releases at statement end, before the INSERT, and
    two dispatchers enqueuing the same (actor, identity_key) both see an
    empty preflight and both insert. The wrap makes the lock, the
    preflight, and the INSERT share one transaction.
    """
    args = _make_args(unique_for=timedelta(minutes=15), identity_key="account:1")
    # A faithful INSERT ... RETURNING *: the row it hands back is the one
    # this call inserted, so its id is args.id.
    conn = _ConnStandin(fetchrow_map={"RETURNING": _Record(_full_record(job_id=args.id))})

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), args)

    assert isinstance(row, JobRow)
    assert row.id == args.id, "fixture: the preflight found nothing, so this inserted"
    kinds_in_tx = _kinds_in_tx(conn)
    assert {"advisory_try_lock", "unique_for_preflight", "insert", "pg_notify"} <= kinds_in_tx, (
        f"the single-flight critical section must run inside one transaction; "
        f"statements={conn.statements}"
    )
    assert all(in_tx for _, in_tx in conn.statements), (
        f"no statement may run outside the wrap on a bare connection; statements={conn.statements}"
    )


async def test_unique_for_dedup_hit_on_bare_caller_conn_returns_inside_the_wrap() -> None:
    """The wrap covers the dedup return too, not only the INSERT arm.

    The lock must still be held when the existing row is handed back (it
    releases at the wrap's commit), so a same-identity racer that queued
    behind this call cannot insert in between.
    """
    existing_id = new_job_id()
    conn = _ConnStandin(
        fetchrow_map={"identity_key = $2": _Record(_full_record(job_id=existing_id))}
    )
    args = _make_args(unique_for=timedelta(minutes=15), identity_key="account:2")

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), args)

    assert row.id == existing_id, "fixture: the preflight hit must dedup"
    kinds = {kind for kind, _ in conn.statements}
    assert "insert" not in kinds, "a dedup return must not INSERT"
    assert all(in_tx for _, in_tx in conn.statements), (
        f"the dedup return must run inside the wrap; statements={conn.statements}"
    )


# ── the wrap stays scoped to what needs it ────────────────────────────


async def test_caller_owned_open_transaction_is_not_re_wrapped() -> None:
    """A caller who already holds a transaction owns the scope.

    The lock then spans that caller's transaction — the documented
    semantics for this shape — and the enqueue path must not open a
    second one (asyncpg would nest it as a savepoint of the caller's).
    """
    conn = _ConnStandin(
        fetchrow_map={"RETURNING": _Record(_full_record())},
        caller_tx_open=True,
    )
    args = _make_args(unique_for=timedelta(minutes=15), identity_key="account:3")

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), args)

    assert isinstance(row, JobRow)
    assert conn.tx_depth == 0, "the enqueue path must not open a transaction the caller owns"
    assert _kinds_in_tx(conn) >= {"advisory_try_lock", "unique_for_preflight", "insert"}


async def test_plain_enqueue_on_bare_caller_conn_opens_no_transaction() -> None:
    """An enqueue with no transaction-scoped serialization need (no
    unique_for, no cap) must not pay for a wrap it does not need."""
    conn = _ConnStandin(fetchrow_map={"RETURNING": _Record(_full_record())})
    args = _make_args()

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), args)

    assert isinstance(row, JobRow)
    assert conn.tx_depth == 0, "a plain enqueue opens no transaction on a bare connection"
    assert conn.statements, "fixture: the enqueue ran"


async def test_capped_actor_on_bare_caller_conn_runs_count_and_insert_in_one_transaction() -> None:
    """The capped-actor arm of the same wrap condition, pinned at unit
    level: the count-then-insert must share one transaction or two
    concurrent enqueues both see room and both insert, overshooting the
    operator's cap."""
    conn = _ConnStandin(
        fetchrow_map={"RETURNING": _Record(_full_record())},
        fetchval_map={"count(*)": 0},
    )
    args = _make_args(max_pending=10)

    row = await _enqueue_on_conn(conn, _SQL, _SCHEMA_LABEL, FakeClock(_NOW), args)

    assert isinstance(row, JobRow)
    assert _kinds_in_tx(conn) >= {"advisory_try_lock", "max_pending_count", "insert"}
    assert all(in_tx for _, in_tx in conn.statements), (
        f"the count-then-insert must run inside one transaction; statements={conn.statements}"
    )
