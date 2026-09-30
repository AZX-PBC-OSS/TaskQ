"""The accepted-checksums contract, behaviorally: a migration's published
history is legitimate provenance.

Some migration files exist in more than one published version — the bytes
were amended after first shipping, and databases that applied the earlier
bytes carry that version's checksum in their ledger permanently. No
single file can match two ledgers. The BEHAVIOR under contract:

- a database whose ledger holds ANY published version's checksum upgrades
  with zero drift refusals, through the same public surfaces every
  deployment uses (``apply_pending`` / ``taskq migrate status``);
- a ledger checksum NOTHING published produced still refuses — the guard
  keeps its teeth;
- a migration with no published history behaves exactly as before.

Every assertion here runs through public API only — the drift verdict
arrives the way an operator sees it: apply succeeding or raising, and
``migrate status`` reporting. The anchor checksums are LITERals from a
live production ledger (schema ``taskq``), reproduced from the bundled
published bytes by an independent path (file bytes → render → hash), never
through the guard's own internals.
"""

import hashlib
import inspect
import os
import subprocess
from importlib import resources
from pathlib import Path

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq.settings import TaskQSettings
from taskq.testing.assertions import plain_cli_output

#: The ledger checksums a live production database recorded for the two
#: amended files (schema ``taskq``), supplied by the operator during the
#: incident.
PROD_CHECKSUMS: dict[str, str] = {
    "01.00.13_03_pre_jobs_batch_open_members_index.sql": (
        "f5c0539aa774f6adf7611848393ebfdcd8a39672650c545444e0b547e1cf3090"
    ),
    "01.00.14_01_pre_wake_channel_schema_tag.sql": (
        "cee5f313042b0cf1d9698a158627e08d9487dc255a4a6e4da0e530b8d1044a55"
    ),
}

#: A checksum NOTHING published produced — the tamper arm's anchor.
UNKNOWN_CHECKSUM = "d" * 64

RUNNER_ENV = {"TASKQ_PG_DSN", "TASKQ_SCHEMA_NAME"}


def _migrate_status(settings: TaskQSettings) -> subprocess.CompletedProcess[str]:
    """The REAL operator surface: the taskq CLI as its own process — no
    event-loop collision with the test's loop, and the same command an
    operator types."""
    env = dict(os.environ)
    env["TASKQ_PG_DSN"] = str(settings.pg_dsn)
    env["TASKQ_SCHEMA_NAME"] = settings.schema_name
    return subprocess.run(  # Why: fixed argv, no shell — the operator's own command.
        ["uv", "run", "--no-sync", "taskq", "migrate", "status"],  # noqa: S607  # Why: fixed argv, no shell — the operator command.
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )


def _published_versions(filename: str) -> list[bytes]:
    """Every published version of a migration file: the bundled file plus
    the file's bundled historical variants — raw bytes, the package's own
    public data."""
    package = resources.files("taskq.migrations")
    versions = [package.joinpath(filename).read_bytes()]
    variants_dir = package.joinpath("_variants").joinpath(Path(filename).stem)
    if variants_dir.is_dir():
        for entry in variants_dir.iterdir():
            if entry.is_file() and entry.name.endswith(".sql"):
                versions.append(entry.read_bytes())
    return versions


def _ledger_checksums(filename: str, schema: str) -> set[str]:
    """The checksums a database with schema ``schema`` could legitimately
    hold for ``filename`` — computed HERE from the published bytes by an
    independent path (bytes → render → hash), never via the guard's
    internals."""
    template_text = [v.decode("utf-8-sig") for v in _published_versions(filename)]
    return {
        hashlib.sha256(migrate_mod.render(t, schema).encode("utf-8")).hexdigest()
        for t in template_text
    }


async def _point_ledger_at_a_published_version(
    pg_conn: asyncpg.Connection, settings: TaskQSettings, filename: str
) -> str:
    """Record, in this database's ledger, the checksum of a NON-current
    published version — the prod shape: the database applied the bytes of
    an older vintage. Returns the version digest written."""
    discovered = {m.filename: m for m in migrate_mod.discover()}
    assert filename in discovered, f"{filename} is not a bundled migration"
    migration = discovered[filename]
    key = migration.key
    current = migration.checksum(settings.schema_name)
    historical = _ledger_checksums(filename, settings.schema_name) - {current}
    assert historical, f"{filename} has no published history to point at"
    digest = sorted(historical)[0]
    updated = await pg_conn.execute(  # Why: schema is a fixture-provided identifier.
        f'UPDATE "{settings.schema_name}".schema_migrations SET checksum = $1 WHERE version = $2',  # noqa: S608  # Why: schema is a fixture-provided identifier.
        digest,
        key,
    )
    assert updated == "UPDATE 1", (
        f"the tamper must hit exactly the ledger row {key!r} — a zero-row update proves nothing"
    )
    # Read-back canary: UPDATE 1 proves a row matched the WHERE, not that the
    # stored value is now the digest (a row already holding it updates as a
    # no-op). Query the ledger back — the tamper's contact with reality.
    stored = await pg_conn.fetchval(
        f'SELECT checksum FROM "{settings.schema_name}".schema_migrations WHERE version = $1',  # noqa: S608  # Why: schema is a fixture-provided identifier.
        key,
    )
    assert stored == digest, f"the ledger row {key!r} must hold the tampered digest"
    return digest


@pytest.fixture
async def _applied_schema(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)


class TestPublishedHistoryIsAccepted:
    """The behavioral contract: every published vintage upgrades cleanly,
    through the operator's surfaces."""

    @pytest.mark.parametrize("filename", sorted(PROD_CHECKSUMS))
    async def test_a_ledger_holding_an_older_vintage_upgrades_cleanly(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
        filename: str,
    ) -> None:
        await _point_ledger_at_a_published_version(pg_conn, settings, filename)

        # The operator surface first: migrate status reports NO drift for a
        # database holding a published version's checksum.
        result = _migrate_status(settings)
        assert result.returncode == 0, result.stdout + result.stderr
        # The verdict must be about THIS database's ledger: the schema line
        # names it, and a checked row proves a POPULATED ledger was read (a
        # subprocess pointed elsewhere would also print no drift — over an
        # empty ledger, vacuously).
        assert f"schema: {settings.schema_name}" in plain_cli_output(result.stdout), result.stdout
        assert "[✔]" in plain_cli_output(result.stdout), result.stdout
        assert "drift" not in plain_cli_output(result.stdout).lower(), (
            f"migrate status must not report drift for a published vintage: {result.stdout}"
        )

        # Then the apply: zero refusals, nothing pending, and the public
        # drift report empty.
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert applied == [], "an up-to-date database has nothing pending"
        drifts = await migrate_mod.checksum_drifts(pg_conn, schema=settings.schema_name)
        assert drifts == {}, f"published history must not drift: {drifts}"

    @pytest.mark.parametrize("filename", sorted(PROD_CHECKSUMS))
    def test_the_live_production_checksum_is_a_published_version(self, filename: str) -> None:
        """The anchor: the checksum a live production ledger recorded is
        among the published versions, rendered under the schema the
        recording database used. If this fails, the published bytes no
        longer describe what shipped."""
        prod = PROD_CHECKSUMS[filename]
        rendered = _ledger_checksums(filename, "taskq")
        assert prod in rendered, (
            f"the published versions of {filename} render to "
            f"{sorted(h[:12] for h in rendered)}, not the recorded "
            f"{prod[:12]}"
        )


class TestTheGuardKeepsItsTeeth:
    """Accepting published history must not mean accepting everything."""

    async def test_an_unknown_checksum_still_refuses(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await pg_conn.execute(  # Why: schema is a fixture-provided identifier.
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
            " SET checksum = $1 WHERE version = '01.00.13_03:pre'",
            UNKNOWN_CHECKSUM,
        )
        with pytest.raises(migrate_mod.ChecksumDriftError):
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    async def test_migrate_status_reports_the_unknown_checksum_as_drift(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await pg_conn.execute(  # Why: schema is a fixture-provided identifier.
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
            " SET checksum = $1 WHERE version = '01.00.13_03:pre'",
            UNKNOWN_CHECKSUM,
        )
        result = _migrate_status(settings)
        assert "drift" in plain_cli_output(result.stdout).lower(), (
            f"migrate status must surface the unknown checksum as drift: {result.stdout}"
        )
        # The subprocess saw THIS database's ledger, not some other schema's:
        # the tampered row's key and the distinctive d-digest must both be
        # named in the drift section (nothing published produces d*12).
        assert f"schema: {settings.schema_name}" in plain_cli_output(result.stdout), result.stdout
        assert UNKNOWN_CHECKSUM[:12] in plain_cli_output(result.stdout), result.stdout
        assert "01.00.13_03:pre" in plain_cli_output(result.stdout), result.stdout

    async def test_a_migration_without_published_history_behaves_as_before(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        """01.00.00_01 has one published version (the bundled file). Its
        ledger row matching the file = no drift; anything else = drift.
        The historical-shape FileNotFoundError class must not exist."""
        await pg_conn.execute(  # Why: schema is a fixture-provided identifier.
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
            " SET checksum = $1 WHERE version = '01.00.00_01:pre'",
            UNKNOWN_CHECKSUM,
        )
        with pytest.raises(migrate_mod.ChecksumDriftError):
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)


def test_the_historical_file_not_found_error_is_dead() -> None:
    """The docstring promise above, pinned: an earlier shape of this
    mechanism raised FileNotFoundError (for a missing variant directory or
    file); the shipped shape degrades a missing variant dir to "no
    variants" and fails closed at the drift COMPARISON instead, never as
    an OSError. No function of the migration runner may raise
    FileNotFoundError as its own failure mode, and a stem with no variant
    directory answers empty."""
    members = [getattr(migrate_mod, name) for name in dir(migrate_mod)]
    assert not any(
        inspect.isfunction(fn)
        and getattr(fn, "__module__", None) == migrate_mod.__name__
        and "FileNotFoundError" in inspect.getsource(fn)
        for fn in members
    ), "a migration-runner function still raises FileNotFoundError as its failure mode"
    # The degrade path itself: no variant directory, empty answer, no raise.
    assert migrate_mod._variant_templates("01.00.00_01_pre_initial.sql") == []  # pyright: ignore[reportPrivateUsage]  # Why: the death pin exercises the private lookup directly.
