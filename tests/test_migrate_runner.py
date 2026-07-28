"""Unit tests for apply_pending phase-ordering guard and truncation
interplay, using a fabricated migration set (monkeypatched ``discover``)
and a fake connection -- no real database needed.

The integration scenarios against the real 01.00.03 migration set live in
tests/test_idempotency_scope_migrations.py::TestPhaseOrderingGuard; these
tests cover the branches that set cannot reach (a post-only version, and
``target``/``max_steps`` truncation interacting with the guard).
"""

from typing import Any

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
