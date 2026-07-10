"""Unit tests for isolate_self — pure-Python, no PG required."""

import asyncio

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import isolate_self
from tests.conftest import _FakePool

# ── Test helpers ─────────────────────────────────────────────────────────


class FakeConn:
    """Lightweight asyncpg.Connection stand-in for isolate_self tests."""

    def __init__(
        self,
        *,
        fetch_rows: list[dict[str, object]] | None = None,
        fail_execute_with: BaseException | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_calls = 0
        self._fetch_rows = fetch_rows or []
        self._fail_execute_with = fail_execute_with
        self._execute_count = 0

    async def execute(self, sql: str, *args: object) -> str:
        self._execute_count += 1
        if self._fail_execute_with is not None:
            raise self._fail_execute_with
        self.execute_calls.append((sql, args))
        pieces = sql.rsplit(" ", 1)
        return f"{pieces[0]} 1"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return list(self._fetch_rows)

    async def close(self) -> None:
        return

    def transaction(self) -> "_FakeTransaction":
        self.transaction_calls += 1
        return _FakeTransaction()


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


def _worker_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": pg_dsn}
    for key, value in overrides.items():
        if not key.startswith("TASKQ_"):
            data[f"TASKQ_{key}"] = value
        else:
            data[key] = value
    return WorkerSettings.load_from_dict(data)


def _make_deps(
    *,
    lock_lease: float = 60.0,
    heartbeat_interval: float = 10.0,
) -> WorkerDeps:
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        LOCK_LEASE=str(lock_lease),
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    return deps


# ── Test: isolate_self opens a fresh asyncpg.connect ─────────────────────


async def test_isolate_self_opens_fresh_connect() -> None:
    """isolate_self opens a fresh asyncpg.connect() — NOT the heartbeat pool."""

    connect_calls: list[tuple[str, float]] = []

    async def fake_connect(dsn: str, *, timeout: float) -> FakeConn:
        connect_calls.append((dsn, timeout))
        return FakeConn()

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign] # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)
        assert len(connect_calls) == 1
        assert connect_calls[0][1] == 5.0
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: shutdown.set() called even when connect fails ──────────────────


async def test_isolate_self_shutdown_even_on_connect_failure() -> None:
    """isolate_self calls shutdown.set() even when asyncpg.connect() raises."""

    async def fake_connect(dsn: str, *, timeout: float) -> FakeConn:
        raise OSError("connection refused")

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)
        assert shutdown.is_set()
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: isolate_self writes one AttemptRow per running job ─────────────


async def test_isolate_self_writes_attempt_row_per_job() -> None:
    """isolate_self INSERTs one AttemptRow per running job with correct fields."""

    job_rows: list[dict[str, object]] = [
        {
            "id": new_uuid(),
            "attempt": 1,
            "started_at": "2025-01-01T00:00:00Z",
            "max_attempts": 3,
            "retry_kind": "transient",
        },
        {
            "id": new_uuid(),
            "attempt": 2,
            "started_at": "2025-01-01T00:00:01Z",
            "max_attempts": 2,
            "retry_kind": "non_retryable",
        },
    ]

    conn = FakeConn(fetch_rows=job_rows)

    async def fake_connect(dsn: str, *, timeout: float) -> FakeConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        worker_id = new_uuid()
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, worker_id, shutdown)

        assert shutdown.is_set()
        insert_calls = [
            (sql, args)
            for sql, args in conn.execute_calls
            if "INSERT" in sql.upper().split(None, 1)[0]
        ]
        assert len(insert_calls) == 2

        for i, (_sql, args) in enumerate(insert_calls):
            assert args[0] == job_rows[i]["id"]
            assert args[1] == job_rows[i]["attempt"]
            assert args[3] == "crashed"
            assert args[4] == "HeartbeatLost"
            assert args[8] == worker_id
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: isolate_self honours CASE shape ──────────────────────────


async def test_isolate_self_honours_fr12_case_shape() -> None:
    """isolate_self sends the exact disposition UPDATE SQL."""
    runner = None

    class StubConn(FakeConn):
        async def execute(self, sql: str, *args: object) -> str:
            nonlocal runner
            if "SET status = CASE" in sql:
                runner = sql
            return "UPDATE 1"

        async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
            return [
                {
                    "id": new_uuid(),
                    "attempt": 0,
                    "started_at": "2025-01-01T00:00:00Z",
                    "max_attempts": 3,
                    "retry_kind": "transient",
                }
            ]

    conn = StubConn()

    async def fake_connect(dsn: str, *, timeout: float) -> StubConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)
        assert runner is not None
        assert "attempt < max_attempts AND retry_kind != 'non_retryable'" in runner
        assert "now() + interval '5 seconds'" in runner
        assert "NOT (attempt < max_attempts AND retry_kind != 'non_retryable')" in runner
        assert "WHERE id = $1" in runner
        assert "locked_by_worker = $2" in runner
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: asyncio.shield on terminal writes ───────────────────────────────


async def test_isolate_self_shields_terminal_writes() -> None:
    """Terminal writes in isolate_self survive task cancellation via asyncio.shield.

    Verifies that the transaction block (fetch + update + insert) passes
    through asyncio.shield, so mid-flight cancellation cannot strand
    PG in an inconsistent state.
    """
    shield_calls: list[object] = []
    _real_shield = asyncio.shield

    async def _tracking_shield(coro: object) -> object:
        shield_calls.append(coro)
        return await _real_shield(coro)  # type: ignore[arg-type] # Why: coro wraps _inner; passthrough to real shield.

    import taskq.worker.heartbeat as hb_mod

    hb_mod.asyncio.shield = _tracking_shield  # type: ignore[method-assign]
    try:
        conn = FakeConn(
            fetch_rows=[
                {
                    "id": new_uuid(),
                    "attempt": 0,
                    "started_at": "2025-01-01T00:00:00Z",
                    "max_attempts": 3,
                    "retry_kind": "transient",
                }
            ]
        )

        async def fake_connect(dsn: str, *, timeout: float) -> FakeConn:
            return conn

        import asyncpg as apg

        orig_pg_connect = apg.connect
        apg.connect = fake_connect  # type: ignore[method-assign]
        try:
            deps = _make_deps()
            shutdown = asyncio.Event()
            await isolate_self(deps, new_uuid(), shutdown)
            assert len(shield_calls) == 1
            assert shutdown.is_set()
        finally:
            apg.connect = orig_pg_connect  # type: ignore[method-assign]
    finally:
        hb_mod.asyncio.shield = _real_shield  # type: ignore[method-assign]
