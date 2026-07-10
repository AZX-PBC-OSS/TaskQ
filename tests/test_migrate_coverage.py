"""Coverage tests for taskq.migrate: list_applied edge cases and the
apply_pending / apply_pending_locked early-stop and drift-logging paths.

The bundled migration set currently contains exactly one migration, so
the phase/target/max_steps early-stop branches are exercised against a
schema whose `discover()` is monkeypatched to return synthetic
migrations layered on top of the real bootstrap migration. Every test
uses its own schema name (``new_base62()``-suffixed) to avoid colliding
with other test modules/agents sharing the same PG instance.
"""

from __future__ import annotations

import asyncpg
import pytest
import structlog.testing

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.migrate import Migration

pytestmark = pytest.mark.integration


def _fake_migration(version: str, phase: str, sql: str) -> Migration:
    return Migration(
        version=version,
        phase=phase,  # type: ignore[arg-type] # Why: test fixture; Phase is Literal["pre", "post"].
        description="synthetic",
        filename=f"{version}_{phase}_synthetic.sql",
        sql_template=sql,
    )


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# ── list_applied ────────────────────────────────────────────────────────


async def test_list_applied_returns_empty_set_for_nonexistent_schema(pg_dsn: str) -> None:
    schema = f"mig_cov_absent_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        result = await migrate_mod.list_applied(conn, schema)
        assert result == set()
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_list_applied_matches_applied_migrations(pg_dsn: str) -> None:
    schema = f"mig_cov_applied_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        applied = await migrate_mod.apply_pending(conn, schema=schema)
        assert applied, "expected at least the bundled migration to apply"

        result = await migrate_mod.list_applied(conn, schema)
        assert result == {m.key for m in applied}
        assert result == {m.key for m in migrate_mod.discover()}
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_list_applied_matches_multiple_synthetic_migrations(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"mig_cov_multi_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        bootstrap = migrate_mod.discover()
        extra = [
            _fake_migration("50.00.00_01", "pre", 'CREATE TABLE "{schema}".fake_t1 (id int);'),
            _fake_migration("50.00.00_02", "post", 'CREATE TABLE "{schema}".fake_t2 (id int);'),
        ]
        all_migrations = bootstrap + extra
        monkeypatch.setattr(migrate_mod, "discover", lambda: all_migrations)

        applied = await migrate_mod.apply_pending(conn, schema=schema)
        assert {m.key for m in applied} == {m.key for m in all_migrations}

        result = await migrate_mod.list_applied(conn, schema)
        assert result == {m.key for m in all_migrations}
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_list_applied_rejects_invalid_schema_name(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        with pytest.raises(ValueError, match="invalid schema name"):
            await migrate_mod.list_applied(conn, "bad; drop table")
    finally:
        await conn.close()


# ── apply_pending: checksum drift ───────────────────────────────────────


async def test_apply_pending_logs_checksum_drift_without_raising(pg_dsn: str) -> None:
    """Checksum drift (stored != current) is logged as a warning, not fatal."""
    schema = f"mig_cov_drift_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        first = await migrate_mod.apply_pending(conn, schema=schema)
        assert first

        drifted_key = first[0].key
        await conn.execute(
            f'UPDATE "{schema}".schema_migrations SET checksum = $1 WHERE version = $2',  # noqa: S608 # Why: schema is a test-generated identifier, not user input; asyncpg has no parameter binding for identifiers.
            "0" * 64,
            drifted_key,
        )

        with structlog.testing.capture_logs() as captured:
            second = await migrate_mod.apply_pending(conn, schema=schema)

        assert second == [], "already-applied migration must not be re-applied"
        drift_events = [e for e in captured if e.get("event") == "migration-checksum-drift"]
        assert len(drift_events) == 1
        assert drift_events[0]["key"] == drifted_key
        assert drift_events[0]["stored_checksum"] == "0" * 64
        assert drift_events[0]["log_level"] == "warning"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── apply_pending: phase filter ──────────────────────────────────────────


async def test_apply_pending_phase_filter_applies_only_matching_phase(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"mig_cov_phase_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        bootstrap = await migrate_mod.apply_pending(conn, schema=schema)
        assert bootstrap

        real = migrate_mod.discover()
        pre_extra = _fake_migration(
            "51.00.00_01", "pre", 'CREATE TABLE "{schema}".phase_pre (id int);'
        )
        post_extra = _fake_migration(
            "51.00.00_02", "post", 'CREATE TABLE "{schema}".phase_post (id int);'
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, pre_extra, post_extra])

        pre_applied = await migrate_mod.apply_pending(conn, schema=schema, phase="pre")
        assert [m.key for m in pre_applied] == [pre_extra.key]

        post_applied = await migrate_mod.apply_pending(conn, schema=schema, phase="post")
        assert [m.key for m in post_applied] == [post_extra.key]

        final = await migrate_mod.apply_pending(conn, schema=schema)
        assert final == [], "everything already applied"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── apply_pending: target stop ───────────────────────────────────────────


async def test_apply_pending_target_stops_after_matching_version(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"mig_cov_target_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        bootstrap = await migrate_mod.apply_pending(conn, schema=schema)
        assert bootstrap

        real = migrate_mod.discover()
        m2 = _fake_migration("52.00.00_01", "pre", 'CREATE TABLE "{schema}".target_t2 (id int);')
        m3 = _fake_migration("52.00.00_02", "post", 'CREATE TABLE "{schema}".target_t3 (id int);')
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m2, m3])

        applied = await migrate_mod.apply_pending(conn, schema=schema, target=m2.version)
        assert [m.key for m in applied] == [m2.key], "must stop after target, leaving m3 pending"

        applied_after = await migrate_mod.list_applied(conn, schema)
        assert m3.key not in applied_after

        rest = await migrate_mod.apply_pending(conn, schema=schema)
        assert [m.key for m in rest] == [m3.key]
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── apply_pending: max_steps stop ────────────────────────────────────────


async def test_apply_pending_max_steps_stops_after_n_applies(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"mig_cov_maxsteps_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        bootstrap = await migrate_mod.apply_pending(conn, schema=schema)
        assert bootstrap

        real = migrate_mod.discover()
        m2 = _fake_migration("53.00.00_01", "pre", 'CREATE TABLE "{schema}".steps_t2 (id int);')
        m3 = _fake_migration("53.00.00_02", "post", 'CREATE TABLE "{schema}".steps_t3 (id int);')
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m2, m3])

        applied = await migrate_mod.apply_pending(conn, schema=schema, max_steps=1)
        assert [m.key for m in applied] == [m2.key]

        applied_keys = await migrate_mod.list_applied(conn, schema)
        assert m3.key not in applied_keys, "max_steps=1 must leave the second migration pending"

        rest = await migrate_mod.apply_pending(conn, schema=schema)
        assert [m.key for m in rest] == [m3.key]
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_apply_pending_max_steps_zero_still_applies_one(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Surprising boundary: the ``max_steps`` break check runs *after* a
    migration is appended to ``applied_now``, so ``max_steps=0`` does NOT
    prevent the first pending migration from being applied — it merely
    stops immediately after applying exactly one (same as ``max_steps=1``).
    """
    schema = f"mig_cov_maxsteps0_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        bootstrap = await migrate_mod.apply_pending(conn, schema=schema)
        assert bootstrap

        real = migrate_mod.discover()
        m2 = _fake_migration("54.00.00_01", "pre", 'CREATE TABLE "{schema}".zero_t2 (id int);')
        m3 = _fake_migration("54.00.00_02", "post", 'CREATE TABLE "{schema}".zero_t3 (id int);')
        monkeypatch.setattr(migrate_mod, "discover", lambda: [*real, m2, m3])

        applied = await migrate_mod.apply_pending(conn, schema=schema, max_steps=0)
        assert [m.key for m in applied] == [m2.key], (
            "max_steps=0 still applies exactly one migration due to the post-append break check"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── apply_pending_locked ──────────────────────────────────────────────────


async def test_apply_pending_locked_happy_path_then_noop(pg_dsn: str) -> None:
    schema = f"mig_cov_locked_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
    finally:
        await conn.close()

    try:
        with structlog.testing.capture_logs() as captured:
            first = await migrate_mod.apply_pending_locked(pg_dsn, schema=schema)
        assert first, "expected the bundled migration to apply"
        assert any(e.get("event") == "applied migrations before startup" for e in captured)

        with structlog.testing.capture_logs() as captured_second:
            second = await migrate_mod.apply_pending_locked(pg_dsn, schema=schema)
        assert second == []
        assert any(e.get("event") == "no pending migrations" for e in captured_second)
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()


async def test_apply_pending_locked_wraps_failure_in_system_exit(pg_dsn: str) -> None:
    """An invalid schema name makes apply_pending's internal validation raise
    ValueError; apply_pending_locked must wrap it in SystemExit and still
    release the advisory lock / close the connection (finally-block cleanup).
    """
    with pytest.raises(SystemExit, match="migration failed, aborting startup"):
        await migrate_mod.apply_pending_locked(pg_dsn, schema="bad;schema")

    # The advisory lock must have been released in the finally-block: a
    # fresh apply_pending_locked call against a valid schema should not hang.
    schema = f"mig_cov_lockrelease_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
    finally:
        await conn.close()
    try:
        applied = await migrate_mod.apply_pending_locked(pg_dsn, schema=schema)
        assert applied
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()
