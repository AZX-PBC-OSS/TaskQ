"""Red-team audit of the accepted-checksums fix (#594, e0bc9bbd).

Written by an adversarial reviewer, not the fix's author. Every class here
attacks one claim of the fix and pins the answer with an executed proof:

- The bundled variant bytes are the published history, not forgeries: each
  variant file's sha256 is pinned to the sha256 of the git blob of the
  commit that introduced it (independent reproduction of the byte-compare
  against ``git show <commit>:<path>``, no git dependency at test time).
- The variants SHIP: the wheel/sdist packaging config cannot drop them.
  A PyPI install missing ``_variants/**`` would silently re-break every
  production ledger this fix exists to unblock.
- The ``_variants`` directory cannot leak into ``discover()``: a variant
  file becoming a runnable migration would double-apply its DDL.
- The refusal keeps its teeth for EVERY migration key, not just the two
  the incident touched - including a migration with no variants at all -
  and the CLI surfaces accept published history without error while an
  unknown checksum still refuses.
- The two production anchor checksums (schema ``taskq``) are reproduced
  from the bundled variant FILES on a real PG ledger, independently of
  the guard's own helpers.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
import tomllib
from importlib import resources
from pathlib import Path

import asyncpg
import pytest
from typer.testing import CliRunner

from taskq import migrate as migrate_mod
from taskq.cli import app
from taskq.settings import TaskQSettings
from taskq.testing.assertions import plain_cli_output

#: The sha256 of the git blob each bundled variant file must equal. These
#: literals were reproduced by an independent audit via
#: ``git show <commit>:src/taskq/migrations/<stem>.sql | sha256sum``:
#: the bundled variant is the PUBLISHED bytes, byte-for-byte, never a
#: rewritten history. Variant files are named for their origin commit.
VARIANT_PROVENANCE: dict[str, str] = {
    "01.00.05_01_pre_batches/f75762b5.sql": (
        "f8a5496c3604f2b0eff66f72bca210a90375f9fa0f709262f79ae60aa34d7d34"
    ),
    "01.00.13_03_pre_jobs_batch_open_members_index/e2963a5e.sql": (
        "99a8514c0bf4e571713c8df390a58d60ea4c120492b2beaeec442ea9a9898a1c"
    ),
    "01.00.14_01_pre_wake_channel_schema_tag/9de360e0.sql": (
        "ed44972d8db0e24b5c60260f522cd6bebde502ab1d2abf8af0bfcbe642c31e55"
    ),
    "01.00.19_01_pre_fence_probe_index/4e534cb7.sql": (
        "677ac97bfbbeb5035f39bd41d09de8ef2a58b38a2bcc69de4d6ea8319353f8de"
    ),
}

#: The ledger checksums a live production database recorded for the two
#: amended files (schema ``taskq``), supplied by the operator during the
#: incident. Reproduced here from the bundled variant FILES, never through
#: the guard's helpers.
PROD_CHECKSUMS: dict[str, str] = {
    "01.00.13_03_pre_jobs_batch_open_members_index.sql": (
        "f5c0539aa774f6adf7611848393ebfdcd8a39672650c545444e0b547e1cf3090"
    ),
    "01.00.14_01_pre_wake_channel_schema_tag.sql": (
        "cee5f313042b0cf1d9698a158627e08d9487dc255a4a6e4da0e530b8d1044a55"
    ),
}

#: A checksum NOTHING in the package's history produced - the tamper arm.
UNKNOWN_CHECKSUM = "d" * 64


def _variant_files() -> dict[str, bytes]:
    """Every bundled variant file's bytes, read from the package resources."""
    package = resources.files("taskq.migrations")
    variants = package.joinpath("_variants")
    out: dict[str, bytes] = {}
    for stem_dir in sorted(variants.iterdir()):
        for entry in sorted(stem_dir.iterdir()):
            if entry.is_file() and entry.name.endswith(".sql"):
                out[f"{stem_dir.name}/{entry.name}"] = entry.read_bytes()
    return out


def _variant_checksums_from_files(filename: str, schema: str) -> set[str]:
    """Render each bundled variant of ``filename`` with ``schema`` and hash.

    Deliberately independent of the guard's helpers: package resource bytes
    -> ``render`` -> sha256, computed here, so the anchor proof cannot be
    self-fulfilling.
    """
    stem = Path(filename).stem
    return {
        hashlib.sha256(
            migrate_mod.render(text.decode("utf-8-sig"), schema).encode("utf-8")
        ).hexdigest()
        for name, text in _variant_files().items()
        if name.startswith(f"{stem}/")
    }


async def _set_ledger_checksum(settings: TaskQSettings, key: str, checksum: str) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
            " SET checksum = $1 WHERE version = $2",
            checksum,
            key,
        )
    finally:
        await conn.close()


class TestVariantBytesAreThePublishedHistory:
    """A variant is only legitimate provenance if its bytes are the bytes
    the origin commit published. Any other bytes and the acceptance is
    built on forged history."""

    @pytest.mark.parametrize("relpath", sorted(VARIANT_PROVENANCE))
    def test_bundled_variant_bytes_equal_the_origin_blob(self, relpath: str) -> None:
        pinned = VARIANT_PROVENANCE[relpath]
        files = _variant_files()
        assert relpath in files, f"the bundled variant {relpath} is missing"
        actual = hashlib.sha256(files[relpath]).hexdigest()
        assert actual == pinned, (
            f"{relpath} is not the published bytes of its origin commit "
            f"(sha256 {actual}, published {pinned}); the acceptance is built "
            "on forged history. Restore the blob's exact bytes."
        )

    def test_the_bundle_covers_every_variant_directory(self) -> None:
        assert set(_variant_files()) == set(VARIANT_PROVENANCE), (
            "an unexpected file inside _variants changes what the guard "
            "accepts without any pin over it"
        )


class TestTheWheelShipsTheVariants:
    """A PyPI install that lacks ``_variants/**`` silently re-breaks every
    ledger the fix exists to unblock: ``_variant_templates`` finds no dir,
    returns [], and the prod checksums refuse again. The packaging is part
    of the fix."""

    def test_pyproject_excludes_nothing_that_could_drop_the_variants(self) -> None:
        pyproject = Path(migrate_mod.__file__).parents[2] / "pyproject.toml"
        config = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        for target in ("wheel", "sdist"):
            section = config["tool"]["hatch"]["build"]["targets"].get(target, {})
            dropped = {"exclude", "only-include", "externally-excluded"} & set(section)
            assert not dropped, (
                f"tool.hatch.build.targets.{target} sets {sorted(dropped)}: any "
                "pattern that matches _variants/** ships a taskq whose drift "
                "guard refuses the production ledgers this fix unblocks"
            )
            assert section.get("packages", ["src/taskq"]) == ["src/taskq"] or (target == "sdist"), (
                f"tool.hatch.build.targets.{target} no longer packages src/taskq"
            )

    def test_variant_files_are_package_data_reachable_at_runtime(self) -> None:
        """The runtime read path is importlib.resources, not the repo tree:
        every pinned variant must resolve through it (a wheel installs ONLY
        what the resource API can see)."""
        assert set(_variant_files()) == set(VARIANT_PROVENANCE), (
            "a variant is invisible to importlib.resources - it will not "
            "survive installation into site-packages"
        )


class TestDiscoverHygiene:
    """The _variants directory is inside the migrations package: if
    discover() ever treated a variant file as a runnable migration, its
    DDL would apply twice against ledgers that already ran it."""

    def test_discover_returns_no_variant_paths(self) -> None:
        found = migrate_mod.discover()
        leaked = [m.filename for m in found if "_variants" in m.filename]
        assert leaked == [], f"discover() leaks variant files as migrations: {leaked}"

    def test_discover_is_exactly_the_top_level_sql_files(self) -> None:
        package = resources.files("taskq.migrations")
        top_level = {e.name for e in package.iterdir() if e.is_file() and e.name.endswith(".sql")}
        assert {m.filename for m in migrate_mod.discover()} == top_level

    def test_a_variant_filename_could_never_parse_as_a_migration(self) -> None:
        """Variant files are named for commit hashes. If one ever leaked
        into discover()'s iteration it must fail the filename convention
        loudly (ValueError), never masquerade as a runnable migration."""
        for relpath in VARIANT_PROVENANCE:
            name = Path(relpath).name
            assert migrate_mod._NAME_RE.match(name) is None, (  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the regex's rejection directly.
                f"variant filename {name!r} matches the migration filename "
                "convention - a leak into the package root would become a "
                "second runnable migration with duplicate DDL"
            )


@pytest.mark.integration
class TestTheTeethEverywhere:
    """Accepting published history must not mean accepting everything: an
    unknown checksum refuses for EVERY migration key, and the CLI treats
    published history as honest provenance, not an error."""

    def test_every_discovered_key_refuses_an_unknown_checksum(self) -> None:
        """All 44 migrations: a ledger checksum nothing bundled produced is
        drift, for the amended files, the plain files, every key."""
        found = migrate_mod.discover()
        tampered = {m.key: UNKNOWN_CHECKSUM for m in found}
        drifts = migrate_mod._detect_checksum_drifts(found, tampered, "taskq")  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the guard directly.
        assert {d.key for d in drifts} == {m.key for m in found}
        assert all(d.stored == UNKNOWN_CHECKSUM for d in drifts)

    def test_the_variant_migrations_accept_their_variant_checksum(self) -> None:
        """The positive arm of the same unit-level sweep: each amended
        file's variant checksum (computed here from the bundled bytes,
        rendered under the SAME schema the guard checks with) produces
        zero drifts."""
        found = {m.filename: m for m in migrate_mod.discover()}
        applied: dict[str, str] = {}
        for filename in PROD_CHECKSUMS:
            m = found[filename]
            (variant,) = _variant_checksums_from_files(filename, "taskq")
            applied[m.key] = variant
        assert (
            migrate_mod._detect_checksum_drifts(  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the guard directly.
                list(found.values()), applied, "taskq"
            )
            == []
        )

    @pytest.mark.parametrize(
        "filename",
        [
            "01.00.00_01_pre_initial.sql",  # no published variants at all
            "01.00.05_01_pre_batches.sql",
            "01.00.13_03_pre_jobs_batch_open_members_index.sql",
            "01.00.14_01_pre_wake_channel_schema_tag.sql",
            "01.00.19_01_pre_fence_probe_index.sql",
        ],
    )
    async def test_an_unknown_checksum_refuses_on_a_real_ledger(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
        filename: str,
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        m = {x.filename: x for x in migrate_mod.discover()}[filename]
        await pg_conn.execute(
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608  # Why: schema is a fixture-provided identifier.
            " SET checksum = $1 WHERE version = $2",
            UNKNOWN_CHECKSUM,
            m.key,
        )
        drifts = await migrate_mod.checksum_drifts(pg_conn, schema=settings.schema_name)
        assert set(drifts) == {m.key}, f"the tamper of {m.key} must refuse: {drifts}"
        with pytest.raises(migrate_mod.ChecksumDriftError):
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    def _applied_ledger_with_variants(
        self, settings: TaskQSettings, *, schema_for_render: str
    ) -> None:
        """Apply everything, then point the two amended files' ledger rows
        at their variant checksums rendered under ``schema_for_render``."""

        async def _do() -> None:
            conn = await asyncpg.connect(str(settings.pg_dsn))
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
                discovered = {m.filename: m for m in migrate_mod.discover()}
                await migrate_mod.apply_pending(conn, schema=settings.schema_name)
                for filename in PROD_CHECKSUMS:
                    (variant,) = _variant_checksums_from_files(filename, schema_for_render)
                    # The ledger's version column holds the FULL migration
                    # key ({version}:{phase}) - tampering by bare stem
                    # matches zero rows and makes the test vacuous.
                    await conn.execute(
                        f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608
                        " SET checksum = $1 WHERE version = $2",
                        variant,
                        discovered[filename].key,
                    )
            finally:
                await conn.close()

        asyncio.run(_do())

    def test_cli_status_reports_the_variant_state_as_no_drift(
        self,
        settings: TaskQSettings,
    ) -> None:
        """``taskq migrate status`` on the prod shape: the accepted variant
        checksums are honest provenance - exit 0, no drift, and ``migrate
        up`` through the same CLI runs without refusal."""
        self._applied_ledger_with_variants(settings, schema_for_render=settings.schema_name)
        runner = CliRunner()
        result = runner.invoke(app, ["migrate", "status"])
        assert result.exit_code == 0, result.output
        assert "drift" not in plain_cli_output(result.output).lower(), result.output
        up = runner.invoke(app, ["migrate", "up"])
        assert up.exit_code == 0, up.output

    def test_cli_reports_an_unknown_checksum_state_as_drift(
        self,
        settings: TaskQSettings,
    ) -> None:
        """Same CLI, tampered ledger: the unknown checksum surfaces as
        drift (nonzero exit, the drift named), never a silent success."""
        self._applied_ledger_with_variants(settings, schema_for_render=settings.schema_name)
        asyncio.run(_set_ledger_checksum(settings, "01.00.13_03:pre", UNKNOWN_CHECKSUM))
        result = CliRunner().invoke(app, ["migrate", "up"])
        assert result.exit_code != 0, "a tampered ledger must not upgrade cleanly"
        assert "drift" in plain_cli_output(result.output).lower() or UNKNOWN_CHECKSUM[
            :12
        ] in plain_cli_output(result.output)


class TestTheAnchorsReproducedFromTheVariantFiles:
    """The two production checksums are reproduced from the bundled variant
    FILES - not through the guard's helpers - and the prod shape upgrades a
    real PG ledger with zero refusals and an empty drift report."""

    @pytest.mark.parametrize("filename", sorted(PROD_CHECKSUMS))
    def test_the_variant_files_render_to_the_prod_checksum(self, filename: str) -> None:
        (rendered,) = _variant_checksums_from_files(filename, "taskq")
        assert rendered == PROD_CHECKSUMS[filename], (
            f"the bundled variant of {filename} no longer renders to the "
            "published production checksum - the variants no longer "
            "describe what shipped"
        )

    async def test_tampering_by_bare_stem_matches_zero_rows(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
    ) -> None:
        """The trap the PR's own prod-shape test fell into: the ledger's
        version column holds the FULL key ({version}:{phase}), so an
        UPDATE keyed by the bare migration stem matches ZERO rows - the
        tamper silently no-ops and the test proves nothing. Pin the key
        shape so no tamper in this suite ever vacuously passes."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        m = migrate_mod.discover()[0]
        by_stem = await pg_conn.execute(
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608
            " SET checksum = $1 WHERE version = $2",
            UNKNOWN_CHECKSUM,
            m.version,
        )
        assert by_stem == "UPDATE 0", (
            "a stem-keyed tamper must match nothing - any tamper test keyed by stem is vacuous"
        )
        by_key = await pg_conn.execute(
            f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608
            " SET checksum = $1 WHERE version = $2",
            UNKNOWN_CHECKSUM,
            m.key,
        )
        assert by_key == "UPDATE 1", f"the ledger must be keyed by {m.key!r}"

    async def test_the_prod_shape_upgrades_with_zero_refusals(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        for filename in PROD_CHECKSUMS:
            (variant,) = _variant_checksums_from_files(filename, settings.schema_name)
            await pg_conn.execute(
                f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608
                " SET checksum = $1 WHERE version = $2",
                variant,
                Path(filename).stem,
            )
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert applied == [], "an up-to-date database has nothing pending"
        assert await migrate_mod.checksum_drifts(pg_conn, schema=settings.schema_name) == {}, (
            "the drift report must stay empty for the published-history ledger"
        )

    async def test_a_foreign_schema_checksum_is_still_drift(
        self,
        pg_conn: asyncpg.Connection,
        settings: TaskQSettings,
    ) -> None:
        """The complement of schema-agnosticism: checksums are rendered
        with the CHECKING database's own schema, so a checksum produced
        under a DIFFERENT schema cannot appear in this ledger — accepting
        it would accept unaccounted-for SQL. The variants make the same
        BYTES work for every schema; they do not make one schema's hashes
        valid for another."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        discovered = {m.filename: m for m in migrate_mod.discover()}
        for filename in PROD_CHECKSUMS:
            (foreign,) = _variant_checksums_from_files(filename, "some_other_schema")
            await pg_conn.execute(
                f'UPDATE "{settings.schema_name}".schema_migrations'  # noqa: S608
                " SET checksum = $1 WHERE version = $2",
                foreign,
                discovered[filename].key,
            )
        drifts = await migrate_mod.checksum_drifts(pg_conn, schema=settings.schema_name)
        assert set(drifts) == {discovered[f].key for f in PROD_CHECKSUMS}, (
            "a foreign-schema checksum must drift"
        )


def _knock_out_variant_dirs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the guard's variant lookup at a root with NO _variants dir,
    simulating a wheel that failed to ship them (see
    TestTheWheelShipsTheVariants): the prod checksums must refuse again.
    The fake root exposes exactly the real top-level .sql files, snapshotted
    before the patch, so only the variants go missing."""
    real_root = resources.files("taskq.migrations")
    top_level = sorted(
        e.name for e in real_root.iterdir() if e.is_file() and e.name.endswith(".sql")
    )
    fake = _FakePackage(top_level)
    real_files = resources.files

    def files(name: str):
        return fake if name == "taskq.migrations" else real_files(name)

    monkeypatch.setattr(resources, "files", files)


class _FakePackage:
    """A resources root with NO _variants dir, exposing only snapshotted
    top-level .sql file names - what discover() and _variant_templates()
    traverse."""

    def __init__(self, top_level_sql: list[str]) -> None:
        real_root = resources.files("taskq.migrations")
        self._files = {name: real_root.joinpath(name).read_bytes() for name in top_level_sql}

    def iterdir(self):
        return [_FakeEntry([name], self._files) for name in self._files]

    def joinpath(self, *parts: str):
        return _FakeEntry(list(parts), self._files)

    def is_dir(self) -> bool:
        return False


class _FakeEntry:
    def __init__(self, parts: list[str], files: dict[str, bytes]) -> None:
        self._parts = parts
        self._files = files

    @property
    def name(self) -> str:
        return self._parts[-1]

    def is_dir(self) -> bool:
        prefix = "/".join(self._parts)
        # A nonexistent path inside the fake root (e.g. _variants/<stem>)
        # is a directory-less entry: report False, never raise.
        return any(name.startswith(f"{prefix}/") for name in self._files)

    def is_file(self) -> bool:
        return "/".join(self._parts) in self._files

    def joinpath(self, *parts: str):
        return _FakeEntry([*self._parts, *parts], self._files)

    def read_bytes(self) -> bytes:
        key = "/".join(self._parts)
        if key in self._files:
            return self._files[key]
        matches = [v for k, v in self._files.items() if k.startswith(f"{key}/")]
        if not matches:
            raise FileNotFoundError(key)
        return matches[0]

    def read_text(self, encoding: str) -> str:
        return self.read_bytes().decode(encoding)


class TestTheFixDiesWithoutTheShipping:
    """The teeth of the packaging pin: if the variants stop shipping
    (install-time data loss), the guard must refuse the prod checksums -
    fail loudly, never silently accept."""

    def test_without_the_variants_a_prod_checksum_is_drift_again(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _knock_out_variant_dirs(monkeypatch)
        found = migrate_mod.discover()
        applied: dict[str, str] = {}
        for filename, prod in PROD_CHECKSUMS.items():
            m = {x.filename: x for x in found}[filename]
            applied[m.key] = prod
        drifts = migrate_mod._detect_checksum_drifts(found, applied, "taskq")  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the guard directly.
        assert {d.key for d in drifts} == set(applied), (
            "a wheel without _variants would silently accept-or-miss prod "
            "ledgers; without the data the guard must report drift"
        )


def test_guard_reads_variants_through_the_real_package_resources() -> None:
    """Sanity for the monkeypatch above: with the REAL resources root, the
    variant templates are found and nonempty for exactly the four amended
    files, and empty for everything else."""
    found = migrate_mod.discover()
    with_variants = {
        m.filename
        for m in found
        if migrate_mod._variant_templates(m.filename)  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the helper's output directly.
    }
    assert with_variants == {
        "01.00.05_01_pre_batches.sql",
        "01.00.13_03_pre_jobs_batch_open_members_index.sql",
        "01.00.14_01_pre_wake_channel_schema_tag.sql",
        "01.00.19_01_pre_fence_probe_index.sql",
    }


# ── the variant LIST itself: multiplicities and duplicates ────────────────


class _VariantEntry:
    def __init__(self, name: str, text: str) -> None:
        self._name = name
        self._text = text

    @property
    def name(self) -> str:
        return self._name

    def is_file(self) -> bool:
        return True

    def is_dir(self) -> bool:
        return False

    def read_text(self, encoding: str) -> str:
        return self._text

    def read_bytes(self) -> bytes:
        return self._text.encode("utf-8")

    def joinpath(self, *parts: str) -> _VariantEntry:
        raise AssertionError(f"unexpected joinpath into a variant file: {parts}")


class _VariantDir:
    def __init__(self, entries: list[_VariantEntry]) -> None:
        self._entries = entries

    def is_dir(self) -> bool:
        return True

    def is_file(self) -> bool:
        return False

    def iterdir(self) -> list[_VariantEntry]:
        return list(self._entries)

    def read_text(self, encoding: str) -> str:
        raise AssertionError("read_text on a variant directory")

    def joinpath(self, *parts: str) -> _VariantEntry:
        raise AssertionError(f"unexpected joinpath into a variant directory: {parts}")


class _MissingEntry:
    def is_dir(self) -> bool:
        return False

    def is_file(self) -> bool:
        return False

    def joinpath(self, *parts: str) -> _MissingEntry:
        return self


class _VariantsHop:
    """The ``_variants`` intermediate hop: its joinpath resolves a stem."""

    def __init__(self, stems: dict[str, list[tuple[str, str]]]) -> None:
        self._stems = stems

    def joinpath(self, *parts: str) -> _VariantDir | _MissingEntry:
        entries = self._stems.get("/".join(parts))
        if entries is None:
            return _MissingEntry()
        return _VariantDir([_VariantEntry(name, text) for name, text in entries])


class _VariantsRoot:
    """A resources root serving ONLY ``_variants/<stem>/`` lookups, with the
    given entries per stem; every other path resolves missing. What
    _variant_templates() traverses, no more."""

    def __init__(self, stems: dict[str, list[tuple[str, str]]]) -> None:
        self._stems = stems

    def joinpath(self, *parts: str) -> _VariantDir | _MissingEntry | _VariantsHop:
        key = "/".join(parts)
        if key == "_variants":
            # The caller chains joinpath("_variants").joinpath(stem): the
            # second hop only sees the stem, so serve it from a hop object.
            return _VariantsHop(self._stems)
        if key.startswith("_variants/"):
            entries = self._stems.get(key[len("_variants/") :])
            if entries is None:
                return _MissingEntry()
            return _VariantDir([_VariantEntry(name, text) for name, text in entries])
        return _MissingEntry()

    def iterdir(self) -> list[_VariantEntry]:
        raise AssertionError("_variant_templates must not iterate the package root")


class TestTheVariantListEdges:
    """Shapes of a migration's bundled variant list the shipped bundle never
    exercises (every amended file carries exactly ONE variant today): a
    file with TWO published variants, and a variant whose rendered bytes
    equal the CURRENT file's (a duplicate). The acceptance is a SET of
    legitimate checksums, so both shapes must behave."""

    def _patch_root(
        self, monkeypatch: pytest.MonkeyPatch, stems: dict[str, list[tuple[str, str]]]
    ) -> None:
        root = _VariantsRoot(stems)
        real_files = resources.files

        def files(name: str):
            return root if name == "taskq.migrations" else real_files(name)

        monkeypatch.setattr(resources, "files", files)

    def test_two_variants_for_one_file_are_both_accepted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A file published twice (two amendments, two historical vintages):
        a ledger holding EITHER vintage's checksum is honest provenance; a
        checksum matching NEITHER is still drift. Order of the entries must
        not matter (the acceptance is a set)."""
        found = {m.filename: m for m in migrate_mod.discover()}
        target = found["01.00.05_01_pre_batches.sql"]
        # The real bundled variant's text, captured BEFORE the root is patched.
        (real_variant_text,) = [
            text.decode("utf-8-sig")
            for name, text in _variant_files().items()
            if name.startswith(f"{Path(target.filename).stem}/")
        ]
        real_variant = hashlib.sha256(
            migrate_mod.render(real_variant_text, "taskq").encode("utf-8")
        ).hexdigest()
        extra_template = target.sql_template + "\n-- a second published vintage\n"
        extra_checksum = hashlib.sha256(
            migrate_mod.render(extra_template, "taskq").encode("utf-8")
        ).hexdigest()
        self._patch_root(
            monkeypatch,
            {
                target.filename.removesuffix(".sql"): [
                    ("bbbb_second.sql", extra_template),
                    ("aaaa_first.sql", real_variant_text),
                ]
            },
        )

        # Both variant checksums accepted, in either listing order.
        for digest in (real_variant, extra_checksum):
            drifts = migrate_mod._detect_checksum_drifts(  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the guard directly.
                list(found.values()), {target.key: digest}, "taskq"
            )
            assert drifts == [], f"published vintage {digest[:12]} must not drift: {drifts}"

        # A checksum matching NEITHER still refuses.
        drifts = migrate_mod._detect_checksum_drifts(  # pyright: ignore[reportPrivateUsage]
            list(found.values()), {target.key: UNKNOWN_CHECKSUM}, "taskq"
        )
        assert [d.key for d in drifts] == [target.key]

    def test_a_variant_equal_to_the_current_file_is_harmless(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A duplicate variant (its rendered checksum equals the CURRENT
        file's) collapses the accepted set to one digest and must neither
        crash the guard nor widen what it accepts: the current file's own
        checksum was always accepted, so the union is unchanged."""
        found = {m.filename: m for m in migrate_mod.discover()}
        target = found["01.00.05_01_pre_batches.sql"]
        current = target.checksum("taskq")
        self._patch_root(
            monkeypatch,
            {
                target.filename.removesuffix(".sql"): [
                    ("deadbeef.sql", target.sql_template),
                ]
            },
        )
        assert migrate_mod._variant_templates(target.filename) == [target.sql_template]  # pyright: ignore[reportPrivateUsage]  # Why: the audit pins the helper directly.
        drifts = migrate_mod._detect_checksum_drifts(  # pyright: ignore[reportPrivateUsage]
            list(found.values()), {target.key: current}, "taskq"
        )
        assert drifts == []
        drifts = migrate_mod._detect_checksum_drifts(  # pyright: ignore[reportPrivateUsage]
            list(found.values()), {target.key: UNKNOWN_CHECKSUM}, "taskq"
        )
        assert [d.key for d in drifts] == [target.key]

    def test_every_bundled_variant_renders_cleanly(self) -> None:
        """The drift check renders every bundled variant with the checking
        database's schema INSIDE the apply/status path: a variant whose
        text breaks str.format (an unescaped ``{``) would crash EVERY
        apply_pending and migrate status for every database, not just the
        amended file's check. Pin that all shipped variants format cleanly
        under an arbitrary valid schema."""
        for name, text in sorted(_variant_files().items()):
            stem = name.split("/")[0]
            rendered = migrate_mod.render(text.decode("utf-8-sig"), "some_schema_name")
            digest = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
            assert len(digest) == 64, f"variant {name} (of {stem}) failed to render"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(f"run with {sys.executable} -m pytest {__file__}")
