"""Unit tests for apply_pending phase-ordering guard and truncation
interplay, using a fabricated migration set (monkeypatched ``discover``)
and a fake connection -- no real database needed.

The integration scenarios against the real 01.00.03 migration set live in
tests/test_idempotency_scope_migrations.py::TestPhaseOrderingGuard; these
tests cover the branches that set cannot reach (a post-only version, and
``target``/``max_steps`` truncation interacting with the guard). The tail
section pins the bounded teardown in apply_pending_locked's ``finally``
(dead-PG unlock-execute and owned-conn-close hangs).
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq.migrate import Migration


class _FakeTx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeMigrateConn:
    """asyncpg connection stand-in for apply_pending.

    Reports schema_migrations as existing, serves the given applied keys,
    and records executed SQL so tests can assert what actually ran.
    """

    def __init__(self, applied: set[str]) -> None:
        self._applied = applied
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args: object) -> bool:
        return True  # schema_migrations table exists

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        return [{"version": key, "checksum": ""} for key in sorted(self._applied)]

    def transaction(self) -> _FakeTx:
        return _FakeTx()

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append(sql)
        return "OK"


def _make_migration(version: str, phase: str) -> Migration:
    return Migration(
        version=version,
        phase=phase,  # type: ignore[arg-type]  # Why: test fixture; Phase is Literal["pre", "post"].
        description=f"{version} {phase}",
        filename=f"{version}_{phase}_fabricated.sql",
        sql_template="SELECT 1;",
    )


def _patch_discover(monkeypatch: Any, migrations: list[Migration]) -> None:
    # discover() sorts by (version, pre-before-post); apply_pending does
    # not re-sort, so the patched list must already be in that order.
    ordered = sorted(migrations, key=lambda m: (m.version, 0 if m.phase == "pre" else 1))
    monkeypatch.setattr(migrate_mod, "discover", lambda: ordered)


# ── post-only version (no pre sibling) ──────────────────────────


async def test_post_only_version_is_allowed(monkeypatch: Any) -> None:
    """A post migration whose version has NO pre-phase counterpart is not
    guarded -- the ordering rule only binds pre/post pairs."""
    _patch_discover(
        monkeypatch,
        [
            _make_migration("01.00.00_01", "pre"),
            _make_migration("02.00.00_01", "post"),
        ],
    )
    conn = _FakeMigrateConn(applied={"01.00.00_01:pre"})

    applied = await migrate_mod.apply_pending(conn, schema="taskq")  # type: ignore[arg-type]

    assert [m.key for m in applied] == ["02.00.00_01:post"]


# ── truncation interplay ─────────────────────────────────────────


def _two_pair_migrations() -> list[Migration]:
    return [
        _make_migration("01.00.00_01", "pre"),
        _make_migration("01.00.03_01", "pre"),
        _make_migration("01.00.03_01", "post"),
        _make_migration("02.00.00_01", "pre"),
        _make_migration("02.00.00_01", "post"),
    ]


async def test_max_steps_truncation_does_not_trip_guard(monkeypatch: Any) -> None:
    """--phase post --max-steps 1 with two pending posts: the run applies
    only the first post (whose pre is applied); the guard must not refuse
    over the LATER post (whose pre is not applied) that truncation cut."""
    _patch_discover(monkeypatch, _two_pair_migrations())
    conn = _FakeMigrateConn(applied={"01.00.00_01:pre", "01.00.03_01:pre"})

    applied = await migrate_mod.apply_pending(
        conn,
        schema="taskq",
        phase="post",
        max_steps=1,  # type: ignore[arg-type]
    )

    assert [m.key for m in applied] == ["01.00.03_01:post"]


async def test_target_truncation_does_not_trip_guard(monkeypatch: Any) -> None:
    """--phase post --target <first-post-version>: same reasoning as
    max_steps -- the target is inclusive and cuts the unguarded tail."""
    _patch_discover(monkeypatch, _two_pair_migrations())
    conn = _FakeMigrateConn(applied={"01.00.00_01:pre", "01.00.03_01:pre"})

    applied = await migrate_mod.apply_pending(
        conn,
        schema="taskq",
        phase="post",
        target="01.00.03_01",  # type: ignore[arg-type]
    )

    assert [m.key for m in applied] == ["01.00.03_01:post"]


async def test_guard_still_refuses_unguarded_post_within_truncation(
    monkeypatch: Any,
) -> None:
    """Truncation must not become a way to sneak an unguarded post past the
    guard: targeting the second post directly still refuses."""
    _patch_discover(monkeypatch, _two_pair_migrations())
    conn = _FakeMigrateConn(applied={"01.00.00_01:pre", "01.00.03_01:pre"})

    with pytest.raises(ValueError, match="cannot be applied before its pre-phase"):
        await migrate_mod.apply_pending(
            conn,
            schema="taskq",
            phase="post",
            max_steps=2,  # type: ignore[arg-type]
        )


async def test_guard_passes_when_pre_applied_earlier_in_same_run(
    monkeypatch: Any,
) -> None:
    """A plain run over a fresh schema applies pre then post of the same
    version in one call -- the guard sees the pre as eligible."""
    _patch_discover(monkeypatch, _two_pair_migrations())
    conn = _FakeMigrateConn(applied=set())

    applied = await migrate_mod.apply_pending(conn, schema="taskq")  # type: ignore[arg-type]

    keys = [m.key for m in applied]
    assert keys.index("01.00.03_01:pre") < keys.index("01.00.03_01:post")
    assert keys.index("02.00.00_01:pre") < keys.index("02.00.00_01:post")
    # Every migration executed inside its own transaction, in order.
    assert conn.executed.count("SELECT 1;") == 5


# ── apply_pending: failure tagging for self-diagnosis ───────────────────


class _FailOnMarkerConn(_FakeMigrateConn):
    """_FakeMigrateConn whose execute raises when the SQL contains a marker.

    Lets a test fail ONE specific migration mid-apply while every other
    migration executes normally.
    """

    def __init__(self, applied: set[str], fail_marker: str) -> None:
        super().__init__(applied)
        self._fail_marker = fail_marker

    async def execute(self, sql: str, *args: object) -> str:
        if self._fail_marker in sql:
            raise RuntimeError(f"synthetic apply failure: {self._fail_marker}")
        return await super().execute(sql, *args)


def _failing_migration(
    version: str, phase: str, marker: str, *, use_transaction: bool
) -> Migration:
    return Migration(
        version=version,
        phase=phase,  # type: ignore[arg-type]  # Why: test fixture; Phase is Literal["pre", "post"].
        description="fabricated failing",
        filename=f"{version}_{phase}_fabricated.sql",
        sql_template=f"SELECT '{marker}';",
        use_transaction=use_transaction,
    )


async def test_apply_pending_tags_exception_with_failing_transactional_migration(
    monkeypatch: Any,
) -> None:
    """The self-diagnosis can only name the right file if the failure
    carries WHICH migration failed — the first-unrecorded-in-discover-order
    heuristic is wrong under ``--phase``. apply_pending must tag the raised
    exception with the failing migration and re-raise the SAME object (the
    type pins in test_migrate_no_transaction.py forbid wrapping)."""
    failing = _failing_migration("02.00.00_01", "pre", "tx-fail-marker", use_transaction=True)
    _patch_discover(monkeypatch, [_make_migration("01.00.00_01", "pre"), failing])
    conn = _FailOnMarkerConn(applied={"01.00.00_01:pre"}, fail_marker="tx-fail-marker")

    with pytest.raises(RuntimeError, match="synthetic apply failure") as excinfo:
        await migrate_mod.apply_pending(conn, schema="taskq")  # type: ignore[arg-type]

    assert getattr(excinfo.value, "taskq_failed_migration", None) is failing


async def test_apply_pending_tags_exception_from_no_transaction_statement(
    monkeypatch: Any,
) -> None:
    """The no-transaction statement loop gets the same tagging: a mid-file
    statement failure is attributed to its own migration, not to whatever
    sorts first in discover() order."""
    failing = _failing_migration("02.00.00_01", "pre", "nt-fail-marker", use_transaction=False)
    _patch_discover(monkeypatch, [_make_migration("01.00.00_01", "pre"), failing])
    conn = _FailOnMarkerConn(applied={"01.00.00_01:pre"}, fail_marker="nt-fail-marker")

    with pytest.raises(RuntimeError, match="synthetic apply failure") as excinfo:
        await migrate_mod.apply_pending(conn, schema="taskq")  # type: ignore[arg-type]

    assert getattr(excinfo.value, "taskq_failed_migration", None) is failing


# ── phase-ordering guard: failure-report attribution ────────────────────


async def test_phase_guard_failure_report_names_offending_migration(
    monkeypatch: Any,
) -> None:
    """``migrate up --phase post`` on a stale schema trips the ordering
    guard BEFORE the per-migration loop, so its ValueError carries no
    ``taskq_failed_migration`` tag and the diagnosis fell back to the
    first-unrecorded heuristic — the report named the pre_initial FILE
    while the headline named the post VIOLATION. The guard knows the
    offending migration; it must tag it (the loop's mechanism) so the
    report is self-consistent."""
    _patch_discover(monkeypatch, _two_pair_migrations())
    conn = _FakeMigrateConn(applied=set())  # stale schema: nothing applied yet

    with pytest.raises(ValueError, match="cannot be applied before its pre-phase") as excinfo:
        await migrate_mod.apply_pending(
            conn,
            schema="taskq",
            phase="post",  # type: ignore[arg-type]
        )

    d = await migrate_mod.diagnose_apply_failure(  # type: ignore[arg-type]  # Why: _FakeMigrateConn stands in for asyncpg.Connection; the diagnosis reads only fetchval/fetch.
        conn, "taskq", excinfo.value
    )

    assert d.failed_filename == "01.00.03_01_post_fabricated.sql", (
        "the report must name the offending post migration, not the first "
        "unapplied pre in discover() order"
    )
    lines = migrate_mod.render_apply_failure_lines(d)
    assert "01.00.03_01_post_fabricated.sql" in lines[0]
    assert "01.00.00_01_pre_fabricated.sql" not in "\n".join(lines), (
        "the misattributed first-unapplied file must not appear anywhere in the report"
    )


# ── apply_pending_locked: bounded finally teardown (dead PG) ────────────


class _HangCloseMigrateConn(_FakeMigrateConn):
    """_FakeMigrateConn whose close() wedges forever (dead PG).

    Mirrors the _FakeConn hang gate in tests/test_cli_migrate.py:
    close_wait is never set, so close() blocks until cancelled;
    terminate() is the only way out and unblocks any in-flight close().
    """

    def __init__(self, applied: set[str]) -> None:
        super().__init__(applied)
        self.close_calls = 0
        self.close_wait = asyncio.Event()  # never set: close() blocks forever
        self.closed = False
        self.terminated = False

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()


class _HangUnlockMigrateConn(_FakeMigrateConn):
    """_FakeMigrateConn whose pg_advisory_unlock execute wedges forever
    (dead PG); every other SQL and close() complete normally.
    """

    def __init__(self, applied: set[str]) -> None:
        super().__init__(applied)
        self.unlock_wait = asyncio.Event()  # never set: unlock blocks forever
        self.closed = False

    async def execute(self, sql: str, *args: object) -> str:
        if "pg_advisory_unlock" in sql:
            await self.unlock_wait.wait()
        return await super().execute(sql, *args)

    async def close(self) -> None:
        self.closed = True


def _make_conn_factory(
    fake: _FakeMigrateConn,
) -> Callable[[], Awaitable[asyncpg.Connection]]:
    async def factory() -> asyncpg.Connection:
        return fake  # type: ignore[return-value]  # Why: test fake; asyncpg.Connection is a C-extension type that cannot be subclassed.

    return factory


async def test_apply_pending_locked_bounds_hung_conn_close(monkeypatch: Any) -> None:
    """A dead PG can wedge conn.close() forever; the owned-conn close in
    apply_pending_locked's finally must be bounded and terminate the conn
    on timeout, so a dead PG at startup cannot wedge CLI/UI startup before
    the lifespan exit stack exists."""
    _patch_discover(monkeypatch, [_make_migration("01.00.00_01", "pre")])
    conn = _HangCloseMigrateConn(applied=set())
    # Shrink seam: CLOSE_TIMEOUT_SECS is read from migrate_mod's module
    # globals at call time, and the outer timeout below is what makes the
    # RED hang fail fast.
    monkeypatch.setattr(migrate_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    # Why the outer timeout: pre-fix apply_pending_locked awaited c.close()
    # unbounded in its finally, so the RED state hangs forever instead of
    # failing fast. Mirrors the CLI bounded-close tests.
    async with asyncio.timeout(5):
        applied = await migrate_mod.apply_pending_locked(
            schema="taskq",
            conn_factory=_make_conn_factory(conn),
        )

    assert [m.key for m in applied] == ["01.00.00_01:pre"]
    assert conn.terminated is True
    assert conn.close_calls == 1


async def test_apply_pending_locked_bounds_hung_unlock_execute(monkeypatch: Any) -> None:
    """A dead PG can wedge the advisory-unlock execute forever; it must be
    bounded so the finally still reaches the owned-conn close (itself
    bounded) instead of hanging before the lifespan exit stack exists."""
    _patch_discover(monkeypatch, [_make_migration("01.00.00_01", "pre")])
    conn = _HangUnlockMigrateConn(applied=set())
    # See the sibling test for the shrink-seam rationale.
    monkeypatch.setattr(migrate_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    # Why the outer timeout: pre-fix the unlock execute was awaited
    # unbounded in the finally, so the RED state hangs forever instead of
    # failing fast.
    async with asyncio.timeout(5):
        applied = await migrate_mod.apply_pending_locked(
            schema="taskq",
            conn_factory=_make_conn_factory(conn),
        )

    assert [m.key for m in applied] == ["01.00.00_01:pre"]
    assert conn.closed is True
