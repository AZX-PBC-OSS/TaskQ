"""The accepted-checksums contract: a migration's published history is
legitimate provenance.

Some migration files exist in more than one published version — the bytes
were amended after first shipping, and databases that applied the earlier
bytes carry that version's checksum in their ledger permanently. No
single file can match two ledgers, so the drift check renders every
bundled published variant with the CHECKING database's own schema and
accepts a stored checksum any of them produces. A checksum no bundled
version produces is still drift — the schema was built from SQL this
package cannot account for — and the refusal keeps its teeth.

The anchor pins are LITERAL: the two checksums below are the ones a live
production ledger recorded (schema ``taskq``), reproduced here byte-for-
byte from the bundled variant templates. If these pins fail, the variants
no longer describe what actually shipped — fix the variants, not the pin.
"""

import hashlib
from pathlib import Path

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq.settings import TaskQSettings

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

#: A checksum NOTHING in the package's history produced — the tamper arm's
#: anchor.
UNKNOWN_CHECKSUM = "d" * 64


def _variant_checksums(filename: str, schema: str) -> set[str]:
    """Render each bundled variant of ``filename`` with ``schema`` and hash
    — computed HERE from the bundled bytes, independently of the guard's
    own helper, so the acceptance proof is not self-fulfilling."""
    variants_dir = (
        Path(migrate_mod.__file__).parent / "migrations" / "_variants" / Path(filename).stem
    )
    return {
        hashlib.sha256(
            migrate_mod.render(p.read_text(encoding="utf-8-sig"), schema).encode("utf-8")
        ).hexdigest()
        for p in variants_dir.glob("*.sql")
    }


@pytest.fixture
async def _applied_schema(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)


class TestVariantsReproducePublishedHistory:
    """The bundled variant templates render to the checksums real ledgers
    hold."""

    @pytest.mark.parametrize("filename", sorted(PROD_CHECKSUMS))
    def test_variant_renders_to_the_prod_checksum(self, filename: str) -> None:
        prod = PROD_CHECKSUMS[filename]
        rendered = _variant_checksums(filename, "taskq")
        assert prod in rendered, (
            f"the bundled variants of {filename} render to "
            f"{sorted(h[:12] for h in rendered)}, not the published "
            f"{prod[:12]} — the variants no longer describe what shipped"
        )


class TestTheProdShapeUpgrades:
    """A database holding a published variant's checksum — the prod shape —
    upgrades with zero drift refusals. The tamper values are the variants'
    checksums rendered under THIS database's own schema (an independent
    path: file bytes → render → hash), which is exactly what a real
    deployment's ledger holds for its applied vintage."""

    async def _point_ledger_at_the_variant(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        for filename in PROD_CHECKSUMS:
            version = filename.removesuffix(".sql")
            variant = _variant_checksums(filename, settings.schema_name)
            assert len(variant) == 1, f"one published variant for {filename}"
            await pg_conn.execute(
                f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
                " SET checksum = $1 WHERE version = $2",
                next(iter(variant)),
                version,
            )

    async def test_apply_pending_does_not_refuse(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await self._point_ledger_at_the_variant(pg_conn, settings)
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert applied == [], "an up-to-date database has nothing pending"
        drifts = await migrate_mod.checksum_drifts(pg_conn, schema=settings.schema_name)
        assert drifts == {}, f"the published-history ledger must not drift: {drifts}"


class TestTheGuardKeepsItsTeeth:
    """Accepting published history must not mean accepting everything."""

    async def test_an_unknown_checksum_still_refuses(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        _applied_schema: None,
    ) -> None:
        await pg_conn.execute(
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
            " SET checksum = $1 WHERE version = '01.00.13_03:pre'",
            UNKNOWN_CHECKSUM,
        )
        with pytest.raises(migrate_mod.ChecksumDriftError):
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    async def test_a_migration_without_variants_has_no_variant_dir_crash(
        self, settings: TaskQSettings
    ) -> None:
        """The file that CRASHED every drift check before this fix: a
        migration with no published variants must contribute zero accepted
        variants, not a FileNotFoundError."""
        plain = {m.filename: m for m in migrate_mod.discover()}["01.00.00_01_pre_initial.sql"]
        accepted = migrate_mod._accepted_checksums(plain, settings.schema_name)
        assert len(accepted) == 1, "no variants: only the current file's checksum"
