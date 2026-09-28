"""The shipped-migration immutability contract.

A migration file's bytes are part of its contract: the runner records a
SHA-256 of the file's rendered SQL at apply time and refuses every future
apply on any mismatch (``_detect_checksum_drifts`` — fail-closed, and the
refusal is per-design: a drifted file means the schema was built from
different SQL than the tree carries). Every byte of the file is hashed —
``render`` is a pure ``{schema}`` substitution, so comments count too.

This family pins that contract from both ends:

1. The canary: the four files that WERE rewritten after shipping (found by
   the post-incident sweep — ``01.00.05_01`` by 28c665bc, ``01.00.13_03``
   by e2963a5e, ``01.00.14_01`` by 9de360e0, ``01.00.19_01`` by 4e534cb7)
   are byte-identical to the versions that originally shipped. Any future
   edit to a shipped file fails here, at PR time, instead of failing a
   customer's deploy at migration time.

2. The upgrade path (the class every CI tier missed — tests always started
   from a fresh database): a database whose ledger holds the ORIGINALS'
   checksums must upgrade to head with zero drift refusals, and the
   improvements the rewrites were carrying must arrive as NEW migrations
   (``01.00.21_02``'s pg_index-conditioned rebuild, ``01.00.21_03``'s
   batches CHECK constraints).

The originals live verbatim in ``tests/migration_originals/`` — copied
from each file's introduction commit (f75762b5, 30983de1, aca69ff6,
e6b8a933), the bytes those databases actually ran.
"""

import hashlib
from pathlib import Path

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.settings import TaskQSettings

ORIGINALS_DIR = Path(__file__).parent / "migration_originals"

SHIPPED_FILES: list[str] = [
    "01.00.05_01_pre_batches.sql",
    "01.00.13_03_pre_jobs_batch_open_members_index.sql",
    "01.00.14_01_pre_wake_channel_schema_tag.sql",
    "01.00.19_01_pre_fence_probe_index.sql",
]


def _original_bytes(filename: str) -> bytes:
    return (ORIGINALS_DIR / filename).read_bytes()


def _original_checksum(filename: str, schema: str) -> str:
    """The ledger checksum the ORIGINAL file rendered to — the same input
    the apply path hashes (``Migration.checksum``: sha256 of the
    schema-rendered template)."""
    rendered = _original_bytes(filename).decode("utf-8").format(schema=schema)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


@pytest.fixture
async def _applied_schema(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """The pair container starts empty; every test builds the schema the
    way every deployment does — the runner applies the bundled set."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)


class TestShippedFilesAreImmutable:
    """The canary: a shipped file's bytes never change after it ships."""

    @pytest.mark.parametrize("filename", SHIPPED_FILES)
    def test_file_matches_its_shipped_bytes(self, filename: str) -> None:
        bundled = (Path(migrate_mod.__file__).parent / "migrations" / filename).read_bytes()
        assert bundled == _original_bytes(filename), (
            f"{filename} was edited after shipping. Shipped migration files "
            "are immutable: the ledger checksum every existing database "
            "recorded is over these bytes, and the drift guard will refuse "
            "to run on any database that applied the original. Restore the "
            "file byte-exact and ship the change as a NEW migration instead."
        )


class TestUpgradeFromOriginalChecksums:
    """The prod repro: a database that applied the originals must upgrade
    to head without a drift refusal, and the improvements must arrive as
    the new carrier migrations."""

    async def _point_ledger_at_the_originals(
        self, pg_conn: asyncpg.Connection, schema: str
    ) -> None:
        """Simulate a database that applied the originally-shipped files:
        the ledger's checksums for the four rewritten files are overwritten
        with the originals' rendered checksums."""
        for filename in SHIPPED_FILES:
            version = filename.removesuffix(".sql")
            original = _original_checksum(filename, schema)
            await pg_conn.execute(
                f'UPDATE "{schema}".schema_migrations SET checksum = $1'  # noqa: S608  # Why: schema is a fixture-provided identifier.
                " WHERE version = $2",
                original,
                version,
            )

    async def test_apply_pending_does_not_refuse(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await self._point_ledger_at_the_originals(pg_conn, settings.schema_name)

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

    async def test_the_improvements_arrive_as_carriers(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        """The rewrites were carrying real fixes; the restore must not lose
        them. The carriers' ledger rows exist and their effects are in the
        schema."""
        rows = await pg_conn.fetch(
            f'SELECT version FROM "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
        )
        versions = {r["version"] for r in rows}
        assert "01.00.21_02:pre" in versions, "the fence-probe rebuild carrier applied"
        assert "01.00.21_03:pre" in versions, "the batches CHECK carrier applied"

        # 01.00.21_02's effect: the canonical fence-probe index is the
        # two-key form (the restored 01.00.19_01 lands it on a fresh
        # database; the carrier spares it — the spare path pinned in
        # test_migrations.py's condition family).
        indexdef = await pg_conn.fetchval(
            """
            SELECT indexdef FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_locked_by_worker_running_idx'
            """,
            settings.schema_name,
        )
        assert indexdef is not None and "(locked_by_worker, id)" in indexdef

        # 01.00.21_03's effect: the three CHECK constraints exist, validated.
        constraints = await pg_conn.fetch(
            """
            SELECT c.conname, c.convalidated
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1 AND t.relname = 'batches'
              AND c.conname LIKE 'batches_%_check'
            """,
            settings.schema_name,
        )
        names = {r["conname"] for r in constraints}
        assert {
            "batches_expected_size_check",
            "batches_consecutive_failures_check",
            "batches_failure_threshold_check",
        } <= names, f"the three CHECKs must exist, got {names}"
        assert all(r["convalidated"] for r in constraints), (
            "every CHECK must have passed VALIDATE — a NOT VALID constraint "
            "left behind means the migration failed fail-closed"
        )


class TestBatchesCheckConstraintsEnforce:
    """The carrier's constraints are live law, not decoration."""

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
