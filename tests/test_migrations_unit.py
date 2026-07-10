"""Unit tests for the migration runner's pure functions (no PG required).

Covers ``discover``, ``render``, ``Migration`` dataclass behavior, and
``apply_pending``/``list_applied`` error paths that don't need a live
database connection.
"""

from __future__ import annotations

import pytest

from taskq import migrate
from taskq.migrate import Migration


def test_discover_returns_sorted_migrations() -> None:
    migrations = migrate.discover()
    assert migrations, "expected at least one bundled migration"
    # pre phase sorts before post for the same version
    sort_keys = [(m.version, 0 if m.phase == "pre" else 1) for m in migrations]
    assert sort_keys == sorted(sort_keys), "migrations must be sorted by version then phase"


def test_discover_all_filenames_match_convention() -> None:
    for m in migrate.discover():
        assert m.phase in ("pre", "post")
        assert m.description
        assert m.filename.endswith(".sql")


def test_migration_key_format() -> None:
    m = migrate.discover()[0]
    assert m.key == f"{m.version}:{m.phase}"


def test_migration_checksum_is_stable_for_same_schema() -> None:
    m = migrate.discover()[0]
    assert m.checksum("taskq") == m.checksum("taskq")


def test_migration_checksum_differs_for_different_schema() -> None:
    m = migrate.discover()[0]
    if "{schema}" in m.sql_template:
        assert m.checksum("taskq") != m.checksum("other")
    else:
        # If the template has no {schema} placeholder, checksums are equal
        assert m.checksum("taskq") == m.checksum("other")


def test_migration_render_substitutes_schema() -> None:
    m = migrate.discover()[0]
    rendered = m.render("myschema")
    assert "{schema}" not in rendered
    assert "myschema" in rendered


def test_render_substitutes_schema_placeholder() -> None:
    rendered = migrate.render('CREATE SCHEMA "{schema}";', "taskq")
    assert rendered == 'CREATE SCHEMA "taskq";'


def test_render_doubles_curly_braces_to_literals() -> None:
    """SQL files escape literal curly braces by doubling them."""
    rendered = migrate.render("SELECT '{{not a placeholder}}';", "taskq")
    assert rendered == "SELECT '{not a placeholder}';"


def test_render_rejects_invalid_schema_name() -> None:
    with pytest.raises(ValueError, match="invalid schema name"):
        migrate.render('CREATE SCHEMA "{schema}";', "invalid-schema!")


def test_render_rejects_schema_with_semicolon() -> None:
    with pytest.raises(ValueError, match="invalid schema name"):
        migrate.render('CREATE SCHEMA "{schema}";', "taskq; DROP SCHEMA")


def test_render_rejects_empty_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema name"):
        migrate.render('CREATE SCHEMA "{schema}";', "")


def test_render_accepts_underscore_schema() -> None:
    rendered = migrate.render('CREATE SCHEMA "{schema}";', "my_schema")
    assert rendered == 'CREATE SCHEMA "my_schema";'


def test_list_applied_rejects_invalid_schema() -> None:
    """list_applied validates schema before touching the DB."""
    with pytest.raises(ValueError, match="invalid schema name"):
        import asyncio

        asyncio.run(migrate.list_applied(object(), "invalid;schema"))  # type: ignore[arg-type]


def test_apply_pending_rejects_invalid_schema() -> None:
    """apply_pending validates schema before touching the DB."""
    with pytest.raises(ValueError, match="invalid schema name"):
        import asyncio

        asyncio.run(migrate.apply_pending(object(), schema="bad schema!"))  # type: ignore[arg-type]


def test_migration_dataclass_is_frozen() -> None:
    from dataclasses import FrozenInstanceError

    m = migrate.discover()[0]
    with pytest.raises(FrozenInstanceError):
        m.version = "99.99.99_99"  # type: ignore[misc]


def test_migration_render_uses_render_function() -> None:
    """Migration.render delegates to the module-level render function."""
    m = Migration(
        version="01.00.00_01",
        phase="pre",
        description="test",
        filename="test.sql",
        sql_template='CREATE SCHEMA "{schema}";',
    )
    assert m.render("taskq") == 'CREATE SCHEMA "taskq";'


def test_discover_rejects_invalid_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    """discover() raises ValueError for filenames that don't match convention."""

    class FakeEntry:
        def __init__(self, name: str, content: str = "-- empty\n") -> None:
            self.name = name
            self._content = content

        def is_file(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8") -> str:
            return self._content

    class FakePackage:
        def iterdir(self):
            return [FakeEntry("bad_name.sql")]

    monkeypatch.setattr(migrate.resources, "files", lambda _pkg: FakePackage())
    with pytest.raises(ValueError, match="does not match convention"):
        migrate.discover()


def test_discover_skips_non_sql_files(monkeypatch: pytest.MonkeyPatch) -> None:
    """discover() ignores non-.sql files and directories."""

    class FakeEntry:
        def __init__(self, name: str, is_file: bool = True) -> None:
            self.name = name
            self._is_file = is_file

        def is_file(self) -> bool:
            return self._is_file

        def read_text(self, encoding: str = "utf-8") -> str:
            return "-- empty\n"

    class FakePackage:
        def iterdir(self):
            return [
                FakeEntry("__init__.py"),
                FakeEntry("__pycache__", is_file=False),
            ]

    monkeypatch.setattr(migrate.resources, "files", lambda _pkg: FakePackage())
    assert migrate.discover() == []
