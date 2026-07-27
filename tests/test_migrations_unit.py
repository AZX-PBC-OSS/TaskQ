"""Unit tests for the migration runner's pure functions (no PG required).

Covers ``discover``, ``render``, ``Migration`` dataclass behavior,
``split_statements``, and ``apply_pending``/``list_applied`` error paths
that don't need a live database connection.
"""

from __future__ import annotations

import pytest

from taskq import migrate
from taskq.migrate import (  # pyright: ignore[reportPrivateUsage]  # Why: unit-testing the guard directly; the end-to-end path is pinned separately in test_migrate_no_transaction.py.
    Migration,
    _reject_transaction_control,
)


def _fake_package(files: dict[str, str]) -> type:
    """Build a ``importlib.resources.files()`` stand-in for monkeypatching.

    Keys are filenames, values are file contents. Contents are stored as
    UTF-8 bytes and decoded with the requested encoding so tests exercise
    ``discover()``'s real decode path (e.g. ``utf-8-sig`` BOM handling).
    """

    class FakeEntry:
        def __init__(self, name: str, content: str) -> None:
            self.name = name
            self._content_bytes = content.encode("utf-8")

        def is_file(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8") -> str:
            return self._content_bytes.decode(encoding)

    class FakePackage:
        def iterdir(self):
            return [FakeEntry(name, content) for name, content in files.items()]

    return FakePackage


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


# ── use_transaction / no-transaction directive ─────────────────────────────


def _discover_with_content(monkeypatch: pytest.MonkeyPatch, content: str) -> Migration:
    files = {"90.00.00_01_post_directive.sql": content}
    monkeypatch.setattr(migrate.resources, "files", lambda _pkg: _fake_package(files)())
    (m,) = migrate.discover()
    return m


def test_migration_use_transaction_defaults_to_true() -> None:
    """Existing call sites construct Migration without the field: default True."""
    m = Migration(
        version="01.00.00_01",
        phase="pre",
        description="test",
        filename="test.sql",
        sql_template="SELECT 1;",
    )
    assert m.use_transaction is True


def test_bundled_migrations_are_all_transactional() -> None:
    """No bundled migration uses the no-transaction directive yet.

    Guards against accidentally retrofitting the mechanism onto existing
    migrations, which the framework requires to stay transactional.
    """
    assert migrate.discover(), "expected bundled migrations"
    assert all(m.use_transaction for m in migrate.discover())


def test_discover_marks_directive_in_leading_comment_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _discover_with_content(
        monkeypatch,
        "-- Rebuild an index without locking writes.\n"
        "-- taskq:no-transaction\n"
        "-- The literal {schema} token is substituted at apply time.\n"
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS t_idx ON "{schema}".t (id);\n',
    )
    assert m.use_transaction is False


def test_discover_defaults_transactional_without_directive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _discover_with_content(
        monkeypatch,
        '-- Just a normal migration.\nCREATE TABLE "{schema}".t (id int);\n',
    )
    assert m.use_transaction is True


def test_discover_accepts_directive_whitespace_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _discover_with_content(
        monkeypatch,
        "   --taskq:no-transaction   \nSELECT 1;\n",
    )
    assert m.use_transaction is False


def test_discover_ignores_directive_after_first_sql_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The directive only counts in the leading comment block — a stray
    ``-- taskq:no-transaction`` later in the file must not flip semantics."""
    m = _discover_with_content(
        monkeypatch,
        'CREATE TABLE "{schema}".t (id int);\n-- taskq:no-transaction\n',
    )
    assert m.use_transaction is True


def test_discover_ignores_directive_in_later_comment_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _discover_with_content(
        monkeypatch,
        "SELECT 1;\n-- a mid-file comment block\n-- taskq:no-transaction\nSELECT 2;\n",
    )
    assert m.use_transaction is True


def test_discover_honors_directive_in_bom_prefixed_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A UTF-8 BOM (Windows editors) must not silently disable the directive."""
    m = _discover_with_content(
        monkeypatch,
        "\ufeff-- taskq:no-transaction\nSELECT 1;\n",
    )
    assert m.use_transaction is False


def test_directive_changes_checksum(monkeypatch: pytest.MonkeyPatch) -> None:
    """The directive lives inside the SQL template, so toggling it changes
    the checksum — drift detection catches an edited-in-place directive."""
    sql = "-- taskq:no-transaction\nSELECT 1;\n"
    with_directive = Migration(
        version="90.00.00_01",
        phase="post",
        description="d",
        filename="f.sql",
        sql_template=sql,
    )
    without_directive = Migration(
        version="90.00.00_01",
        phase="post",
        description="d",
        filename="f.sql",
        sql_template="SELECT 1;\n",
    )
    assert with_directive.checksum("taskq") != without_directive.checksum("taskq")


# ── split_statements ────────────────────────────────────────────────────────


def test_split_statements_empty_input() -> None:
    assert migrate.split_statements("") == []
    assert migrate.split_statements("   \n  ") == []


def test_split_statements_single_statement_without_semicolon() -> None:
    assert migrate.split_statements("SELECT 1") == ["SELECT 1"]


def test_split_statements_two_statements() -> None:
    assert migrate.split_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]


def test_split_statements_ignores_semicolon_in_string_literal() -> None:
    assert migrate.split_statements("INSERT INTO t VALUES ('a;b'); SELECT 1;") == [
        "INSERT INTO t VALUES ('a;b')",
        "SELECT 1",
    ]


def test_split_statements_handles_doubled_quote_escape() -> None:
    assert migrate.split_statements("SELECT 'it''s;';") == ["SELECT 'it''s;'"]


def test_split_statements_handles_e_string_backslash_escape() -> None:
    # In E'...' strings a backslash escapes the next char, so the first '
    # here does NOT terminate the string.
    assert migrate.split_statements("SELECT E'a\\';b';") == ["SELECT E'a\\';b'"]


def test_split_statements_ignores_semicolon_in_quoted_identifier() -> None:
    assert migrate.split_statements('SELECT 1 AS "we;ird";') == ['SELECT 1 AS "we;ird"']


def test_split_statements_keeps_leading_comment_attached() -> None:
    assert migrate.split_statements("-- a;b\nSELECT 1;") == ["-- a;b\nSELECT 1"]


def test_split_statements_ignores_semicolons_in_block_comment() -> None:
    assert migrate.split_statements("/* one ; two ; */ SELECT 1; SELECT 2;") == [
        "/* one ; two ; */ SELECT 1",
        "SELECT 2",
    ]


def test_split_statements_handles_nested_block_comments() -> None:
    sql = "/* outer /* inner ; */ still outer ; */ SELECT 1;"
    assert migrate.split_statements(sql) == ["/* outer /* inner ; */ still outer ; */ SELECT 1"]


def test_split_statements_ignores_semicolons_in_dollar_quoted_body() -> None:
    sql = (
        "CREATE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "    RAISE NOTICE 'x;y';\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n"
        "SELECT 1;"
    )
    assert migrate.split_statements(sql) == [
        "CREATE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "    RAISE NOTICE 'x;y';\n"
        "END;\n"
        "$$ LANGUAGE plpgsql",
        "SELECT 1",
    ]


def test_split_statements_handles_tagged_dollar_quotes() -> None:
    sql = "DO $body$\nBEGIN\n    PERFORM 1;\nEND\n$body$;\nSELECT 1;"
    assert migrate.split_statements(sql) == [
        "DO $body$\nBEGIN\n    PERFORM 1;\nEND\n$body$",
        "SELECT 1",
    ]


def test_split_statements_handles_dollar_quote_inside_other_tag() -> None:
    # A different tag inside a dollar-quoted body must not close it.
    sql = "SELECT $outer$ a $inner$ b $inner$ c $outer$; SELECT 1;"
    assert migrate.split_statements(sql) == [
        "SELECT $outer$ a $inner$ b $inner$ c $outer$",
        "SELECT 1",
    ]


def test_split_statements_drops_comment_only_chunks() -> None:
    assert migrate.split_statements("-- only a comment;\n") == []
    assert migrate.split_statements("SELECT 1; -- trailing\n; SELECT 2;") == [
        "SELECT 1",
        "SELECT 2",
    ]


def test_split_statements_strips_trailing_semicolon_and_whitespace() -> None:
    assert migrate.split_statements("\n SELECT 1 ;\n") == ["SELECT 1"]


def test_split_statements_handles_empty_dollar_quoted_string() -> None:
    assert migrate.split_statements("SELECT $$$$;") == ["SELECT $$$$"]


def test_split_statements_handles_unicode_dollar_tag() -> None:
    # Postgres dollar tags follow identifier rules and may be non-ASCII.
    assert migrate.split_statements("SELECT $é$a;b$é$; SELECT 2;") == [
        "SELECT $é$a;b$é$",
        "SELECT 2",
    ]


def test_split_statements_cr_only_line_ending_ends_line_comment() -> None:
    """Postgres ends ``--`` comments at ``\\r`` too; otherwise the comment
    swallows the rest of a CR-only file and statements are silently dropped."""
    assert migrate.split_statements("SELECT 1; -- c\rSELECT 2;") == [
        "SELECT 1",
        "-- c\rSELECT 2",
    ]


def test_split_statements_e_string_right_after_statement_boundary() -> None:
    assert migrate.split_statements("SELECT 1;E'\\'';") == ["SELECT 1", "E'\\''"]


# ── transaction-control guard for non-transactional migrations ──────────────


def _nt_migration() -> Migration:
    return Migration(
        version="90.00.00_01",
        phase="post",
        description="d",
        filename="f.sql",
        sql_template="-- taskq:no-transaction\nSELECT 1;\n",
        use_transaction=False,
    )


def test_reject_transaction_control_accepts_plain_statements() -> None:
    _reject_transaction_control(_nt_migration(), ["SELECT 1", "CREATE TABLE t (id int)"])


@pytest.mark.parametrize(
    "keyword", ["BEGIN", "begin", "COMMIT", "ROLLBACK", "END", "ABORT", "START"]
)
def test_reject_transaction_control_rejects_keywords(keyword: str) -> None:
    with pytest.raises(ValueError, match="transaction-control"):
        _reject_transaction_control(_nt_migration(), [f"{keyword} WORK"])


def test_reject_transaction_control_rejects_after_leading_comments() -> None:
    """split_statements keeps leading comments attached; the guard must see
    past them or a ``-- comment\\nBEGIN`` would slip through."""
    with pytest.raises(ValueError, match="transaction-control"):
        _reject_transaction_control(_nt_migration(), ["-- explain\n/* plus */\nBEGIN"])


def test_reject_transaction_control_message_names_migration_and_keyword() -> None:
    with pytest.raises(ValueError, match=r"f\.sql.*'COMMIT'"):
        _reject_transaction_control(_nt_migration(), ["COMMIT"])
