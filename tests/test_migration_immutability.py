"""The shipped-migration immutability contract.

A migration file's bytes are part of its contract: the runner records a
SHA-256 of the file's rendered SQL — every byte, comments included — in
each database's ledger at apply time, and refuses every future upgrade on
a mismatch. Two enforcement layers carry that contract:

1. ``test_migrations_released_frozen.py`` pins every file a RELEASE has
   shipped (``tests/data/released_migrations.sha256``) — released bytes
   never change, full stop.
2. THIS family pins the files that shipped only to SOURCE pins (never in
   a release, so the manifest does not list them): the three files the
   Sep 27 storm rewrote post-ship and this fix restored byte-exact to
   their introduction-commit blobs (30983de1, aca69ff6, e6b8a933).

3. The upgrade arm reproduces the PROD ledger's real shape — a database
   that applied the released ``01.00.05_01`` rewrite AND the original
   ``01.00.19_01``/``01.00.13_03``/``01.00.14_01`` — and proves it
   upgrades to head with zero drift refusals, with the #464 fence-probe
   rebuild arriving as the ``01.00.21_02`` carrier.
"""

import hashlib
from pathlib import Path

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.settings import TaskQSettings

ORIGINALS_DIR = Path(__file__).parent / "migration_originals"

#: Restored post-ship-edit files that shipped ONLY to source pins. Their
#: bytes are pinned here against a future edit; the released files are the
#: manifest guard's jurisdiction.
RESTORED_SOURCE_PINNED_FILES: list[str] = [
    "01.00.13_03_pre_jobs_batch_open_members_index.sql",
    "01.00.14_01_pre_wake_channel_schema_tag.sql",
    "01.00.19_01_pre_fence_probe_index.sql",
]


def _original_bytes(filename: str) -> bytes:
    return (ORIGINALS_DIR / filename).read_bytes()


def _original_checksum(filename: str, schema: str) -> str:
    """The ledger checksum the file rendered to as originally shipped —
    the same input the apply path hashes (``Migration.checksum``)."""
    rendered = _original_bytes(filename).decode("utf-8").format(schema=schema)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


@pytest.fixture
async def _applied_schema(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """The pair container starts empty; every test builds the schema the
    way every deployment does — the runner applies the bundled set."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)


class TestShippedFilesAreImmutable:
    """The canary: a shipped file's bytes never change after it ships."""

    @pytest.mark.parametrize("filename", RESTORED_SOURCE_PINNED_FILES)
    def test_file_matches_its_shipped_bytes(self, filename: str) -> None:
        bundled = (Path(migrate_mod.__file__).parent / "migrations" / filename).read_bytes()
        assert bundled == _original_bytes(filename), (
            f"{filename} was edited after shipping. Shipped migration files "
            "are immutable: the ledger checksum every existing database "
            "recorded is over these bytes, and the drift guard will refuse "
            "to run on any database that applied the original. Restore the "
            "file byte-exact and ship the change as a NEW migration instead."
        )


@pytest.mark.integration
class TestUpgradeFromTheProdLedger:
    """The prod repro, at its REAL shape: the released 01.00.05_01 rewrite
    AND the originals of the three restored files, in one ledger."""

    async def _point_ledger_at_what_prod_ran(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Source-pin databases recorded the ORIGINALS' checksums for the
        three restored files (the released 01.00.05_01 needs no tampering:
        the tree carries the released bytes and a fresh apply already
        recorded them)."""
        for filename in RESTORED_SOURCE_PINNED_FILES:
            version = filename.removesuffix(".sql")
            original = _original_checksum(filename, settings.schema_name)
            await pg_conn.execute(
                f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
                " SET checksum = $1 WHERE version = $2",
                original,
                version,
            )

    async def test_apply_pending_does_not_refuse(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await self._point_ledger_at_what_prod_ran(pg_conn, settings)

        # The prod failure was exactly this call raising: the guard reads
        # every applied checksum against the bundled file and refuses all
        # of it on a mismatch. On the pre-fix tree the rewritten files'
        # checksums no longer match the originals' — this raise is the
        # deploy blocker, reproduced.
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert applied == [], "an up-to-date database has nothing pending"

        drifts = await migrate_mod.checksum_drifts(pg_conn, schema=settings.schema_name)
        assert drifts == {}, (
            f"after the restore, every ledger checksum matches its file: got {drifts}"
        )

    async def test_the_fence_probe_rebuild_carrier_applied(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        """The 01.00.19_01 rewrite was carrying the #464 fix; the restore
        must not lose it. The carrier's ledger row exists and its effect —
        the two-key fence-probe index — is in the schema."""
        rows = await pg_conn.fetch(
            f'SELECT version FROM "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
        )
        versions = {r["version"] for r in rows}
        assert "01.00.21_02:pre" in versions, "the fence-probe rebuild carrier applied"

        indexdef = await pg_conn.fetchval(
            """
            SELECT indexdef FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_locked_by_worker_running_idx'
            """,
            settings.schema_name,
        )
        assert indexdef is not None and "(locked_by_worker, id)" in indexdef


@pytest.mark.integration
class TestBatchesCheckConstraintsEnforce:
    """The released 01.00.05_01's CHECK constraints are live law."""

    async def test_negative_expected_size_is_refused(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        with pytest.raises(asyncpg.CheckViolationError):
            await pg_conn.execute(
                f'INSERT INTO "{settings.schema_name}".batches '  # noqa: S608  # Why: schema is a fixture-provided identifier.
                f"(id, queue, expected_size) "
                f"VALUES ('{new_uuid()}', 'carry-check', -1)"
            )

    async def test_null_failure_threshold_is_accepted(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await pg_conn.execute(
            f'INSERT INTO "{settings.schema_name}".batches '  # noqa: S608  # Why: schema is a fixture-provided identifier.
            f"(id, queue, failure_threshold) "
            f"VALUES ('{new_uuid()}', 'carry-check', NULL)"
        )

    async def test_zero_failure_threshold_is_refused(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        with pytest.raises(asyncpg.CheckViolationError):
            await pg_conn.execute(
                f'INSERT INTO "{settings.schema_name}".batches '  # noqa: S608  # Why: schema is a fixture-provided identifier.
                f"(id, queue, failure_threshold) "
                f"VALUES ('{new_uuid()}', 'carry-check', 0)"
            )
