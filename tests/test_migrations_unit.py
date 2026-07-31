"""Unit tests for the migration runner's pure functions (no PG required).

Covers ``discover``, ``render``, ``Migration`` dataclass behavior,
``split_statements``, and ``apply_pending``/``list_applied`` error paths
that don't need a live database connection.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import AsyncMock

import pytest
import structlog.testing

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


def test_list_invalid_indexes_rejects_invalid_schema() -> None:
    """list_invalid_indexes validates schema before touching the DB."""
    with pytest.raises(ValueError, match="invalid schema name"):
        import asyncio

        asyncio.run(migrate.list_invalid_indexes(object(), "invalid;schema"))  # type: ignore[arg-type]


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


def test_discover_accepts_directive_with_trailing_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real-world directives carry a reason after the token
    (``-- taskq:no-transaction  needed for CIC``); silently ignoring that
    form defeats the opt-out — the migration runs transactional anyway."""
    m = _discover_with_content(
        monkeypatch,
        "-- taskq:no-transaction  needed for CIC\nSELECT 1;\n",
    )
    assert m.use_transaction is False


def test_discover_accepts_directive_mixed_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _discover_with_content(
        monkeypatch,
        "-- TaskQ:No-Transaction\nSELECT 1;\n",
    )
    assert m.use_transaction is False


def test_discover_rejects_directive_lookalike_and_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``-- taskq:no-transactional`` must NOT match (the token boundary stops
    prefix drift), but it looks like a directive attempt — warn so the author
    notices instead of silently running transactional."""
    with structlog.testing.capture_logs() as captured:
        m = _discover_with_content(
            monkeypatch,
            "-- taskq:no-transactional\nSELECT 1;\n",
        )
    assert m.use_transaction is True
    warnings = [e for e in captured if e.get("event") == "migration-directive-unrecognized"]
    assert warnings, "expected an unrecognized-directive warning"
    assert warnings[0].get("filename") == "90.00.00_01_post_directive.sql"
    assert "taskq:no-transactional" in str(warnings[0].get("line"))


def test_discover_warns_on_directive_typo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with structlog.testing.capture_logs() as captured:
        m = _discover_with_content(
            monkeypatch,
            "-- taskq:no-transction\nSELECT 1;\n",
        )
    assert m.use_transaction is True
    assert any(e.get("event") == "migration-directive-unrecognized" for e in captured)


def test_discover_exact_directive_logs_no_unrecognized_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with structlog.testing.capture_logs() as captured:
        m = _discover_with_content(
            monkeypatch,
            "-- taskq:no-transaction\nSELECT 1;\n",
        )
    assert m.use_transaction is False
    assert not any(e.get("event") == "migration-directive-unrecognized" for e in captured)


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


def test_split_statements_dollar_sign_inside_identifier() -> None:
    """``a$b$c`` is a legal Postgres identifier; ``$b$`` must not be read as
    a dollar-quote opener here — a tag cannot immediately follow an
    identifier character (same rule as the E'...' detection)."""
    assert migrate.split_statements("SELECT a$b$c; SELECT 2;") == [
        "SELECT a$b$c",
        "SELECT 2",
    ]


def test_split_statements_dollar_quote_after_punctuation_still_parses() -> None:
    """The identifier-char gate must not block real dollar quotes: after
    whitespace, a comma, or an open paren there is no identifier char before
    the ``$``, so the tag still opens a quoted body."""
    assert migrate.split_statements("SELECT 1, $tag$x$tag$ FROM t; SELECT 2;") == [
        "SELECT 1, $tag$x$tag$ FROM t",
        "SELECT 2",
    ]
    assert migrate.split_statements("SELECT f($tag$x$tag$); SELECT 2;") == [
        "SELECT f($tag$x$tag$)",
        "SELECT 2",
    ]


def test_split_statements_unicode_escape_string_regression() -> None:
    """Regression pins for U&'...' literals: backslash is NOT an escape
    outside E'...', and '' still doubles."""
    assert migrate.split_statements("SELECT U&'d\\0061t\\+000061'; SELECT 2;") == [
        "SELECT U&'d\\0061t\\+000061'",
        "SELECT 2",
    ]
    assert migrate.split_statements("SELECT U&'x''y'; SELECT 2;") == [
        "SELECT U&'x''y'",
        "SELECT 2",
    ]


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


@pytest.mark.parametrize(
    "statement",
    [
        "SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp1",
        "SET LOCAL statement_timeout = 0",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "set local statement_timeout = 0",  # lowercase variant
    ],
)
def test_reject_transaction_control_rejects_transaction_scoped_statements(
    statement: str,
) -> None:
    """``SAVEPOINT``/``RELEASE`` would fail loudly at execution outside a
    transaction, but the guard's value is rejecting the file BEFORE anything
    runs. ``SET LOCAL``/``SET TRANSACTION`` are worse: outside a transaction
    they are SILENT no-ops (server WARNING only), so the author believes e.g.
    the statement timeout was disabled for a long build when it was not."""
    with pytest.raises(ValueError, match="transaction-control"):
        _reject_transaction_control(_nt_migration(), [statement])


def test_reject_transaction_control_rejects_set_local_after_leading_comments() -> None:
    with pytest.raises(ValueError, match="transaction-control"):
        _reject_transaction_control(
            _nt_migration(), ["-- tune for the long build\nSET LOCAL statement_timeout = 0"]
        )


def test_reject_transaction_control_message_names_set_local_keyword() -> None:
    with pytest.raises(ValueError, match=r"f\.sql.*'SET LOCAL'"):
        _reject_transaction_control(_nt_migration(), ["SET LOCAL statement_timeout = 0"])


def test_reject_transaction_control_accepts_session_set_and_checkpoint() -> None:
    # Deliberate allowlist: plain SET / SET SESSION is session-scoped — it
    # behaves identically inside and outside a transaction, so opting out
    # changes nothing about it and it is not deceptive. CHECKPOINT is a
    # cluster-level maintenance statement with no transaction semantics at
    # all. Neither pretends the runner is managing a transaction, so the
    # guard stays out of the way.
    _reject_transaction_control(
        _nt_migration(),
        ["SET work_mem = '1GB'", "SET SESSION work_mem = '1GB'", "CHECKPOINT"],
    )


@pytest.mark.parametrize(
    "statement",
    [
        "SET /* x */ LOCAL statement_timeout = 0",
        "SET -- x\nLOCAL statement_timeout = 0",
        "SET/**/LOCAL statement_timeout = 0",
        "/* a /* b */ c */ SET LOCAL statement_timeout = 0",
    ],
)
def test_reject_transaction_control_rejects_comment_trivia_bypasses(statement: str) -> None:
    """Comments are valid trivia here because Postgres treats them as
    whitespace between keywords — ``SET /* x */ LOCAL`` is the same statement
    to the server as ``SET LOCAL`` (and /* */ comments NEST), so the guard
    must skip them too or these forms slip past as silent no-ops."""
    with pytest.raises(ValueError, match="transaction-control"):
        _reject_transaction_control(_nt_migration(), [statement])


def test_reject_transaction_control_rejects_begin_after_nested_comment() -> None:
    """A nested /* ... /* ... */ ... */ block comment before BEGIN must not
    hide it — Postgres sees straight through to the keyword."""
    with pytest.raises(ValueError, match="transaction-control"):
        _reject_transaction_control(_nt_migration(), ["/* a /* b */ c */ BEGIN"])


def test_reject_transaction_control_message_names_set_local_through_comments() -> None:
    """The rejection message names the keyword even when comments intervene."""
    with pytest.raises(ValueError, match=r"f\.sql.*'SET LOCAL'"):
        _reject_transaction_control(_nt_migration(), ["SET /* tune */ LOCAL statement_timeout = 0"])


def test_reject_transaction_control_allows_session_set_through_comments() -> None:
    """Comments between SET and a session-scoped keyword keep it ALLOWED:
    the keyword is what decides, not the trivia around it."""
    _reject_transaction_control(
        _nt_migration(),
        ["SET /* tune */ work_mem = '1GB'", "SET -- tune\nSESSION work_mem = '1GB'"],
    )


# ── apply-failure diagnosis rendering ────────────────────────────────────────

# The startup action line differs from the CLI's: a worker/startup apply
# failure is retried by restarting the process, not by re-running the CLI.
_STARTUP_ACTION_LINE = (
    "Action: restart is safe — migrations are idempotent and self-heal on retry; "
    "if the failure repeats, run `taskq migrate up` and report the output."
)


def _tx_diagnosis() -> migrate.ApplyFailureDiagnosis:
    return migrate.ApplyFailureDiagnosis(
        headline="deadlock detected",
        failed_filename="01.00.00_01_pre_failing.sql",
        use_transaction=True,
        invalid_indexes=(),
        schema="taskq",
    )


def _nt_diagnosis() -> migrate.ApplyFailureDiagnosis:
    return migrate.ApplyFailureDiagnosis(
        headline="canceling statement due to statement timeout",
        failed_filename="01.00.02_01_post_concurrent_idx.sql",
        use_transaction=False,
        invalid_indexes=("jobs_queue_idx",),
        schema="taskq",
    )


def _generic_diagnosis() -> migrate.ApplyFailureDiagnosis:
    return migrate.ApplyFailureDiagnosis(
        headline="boom",
        failed_filename=None,
        use_transaction=None,
        invalid_indexes=(),
        schema="taskq",
    )


# Expected line lists mirror tests/test_cli_migrate.py verbatim so CLI
# byte-identity is pinned at the renderer level.
_TX_DEFAULT_LINES = [
    "migration 01.00.00_01_pre_failing.sql failed: deadlock detected",
    "It ran in a transaction and rolled back: nothing from the migration was applied.",
    "Action: fix the error and re-run `taskq migrate up`.",
]
_NT_DEFAULT_LINES = [
    "migration 01.00.02_01_post_concurrent_idx.sql failed: canceling statement due to statement timeout",
    "It ran WITHOUT a transaction (-- taskq:no-transaction): statements "
    "before the failure remain applied, and the migration was NOT recorded "
    "in the ledger.",
    'INVALID index(es) in schema "taskq": jobs_queue_idx — an interrupted '
    "CREATE INDEX CONCURRENTLY left them behind.",
    "Action: re-run `taskq migrate up` — the migration is idempotent and "
    "drops/rebuilds the debris itself.",
]
_GENERIC_DEFAULT_LINES = [
    "migration failed: boom",
    "Action: fix the error and re-run `taskq migrate up` — already-applied migrations are skipped.",
]


@pytest.mark.parametrize(
    ("make", "startup", "expected"),
    [
        pytest.param(_tx_diagnosis, False, _TX_DEFAULT_LINES, id="transactional-default"),
        pytest.param(
            _tx_diagnosis,
            True,
            [*_TX_DEFAULT_LINES[:-1], _STARTUP_ACTION_LINE],
            id="transactional-startup",
        ),
        pytest.param(_nt_diagnosis, False, _NT_DEFAULT_LINES, id="no-transaction-default"),
        pytest.param(
            _nt_diagnosis,
            True,
            [*_NT_DEFAULT_LINES[:-1], _STARTUP_ACTION_LINE],
            id="no-transaction-startup",
        ),
        pytest.param(_generic_diagnosis, False, _GENERIC_DEFAULT_LINES, id="generic-default"),
        pytest.param(
            _generic_diagnosis,
            True,
            [*_GENERIC_DEFAULT_LINES[:-1], _STARTUP_ACTION_LINE],
            id="generic-startup",
        ),
    ],
)
def test_render_apply_failure_lines(
    make: Callable[[], migrate.ApplyFailureDiagnosis],
    startup: bool,
    expected: list[str],
) -> None:
    """The renderer reproduces the CLI's three report variants verbatim and
    swaps ONLY the action line for startup (worker/UI) failures."""
    assert migrate.render_apply_failure_lines(make(), startup=startup) == expected


def test_render_apply_failure_lines_omits_invalid_line_without_debris() -> None:
    """A no-transaction failure with no INVALID-index debris omits the
    INVALID line (mirrors the CLI's conditional)."""
    d = migrate.ApplyFailureDiagnosis(
        headline="connection was closed mid-build",
        failed_filename="01.00.02_01_post_concurrent_idx.sql",
        use_transaction=False,
        invalid_indexes=(),
        schema="taskq",
    )
    lines = migrate.render_apply_failure_lines(d)
    assert len(lines) == 3
    assert not any("INVALID" in line for line in lines)


def test_apply_failure_diagnosis_dataclass_is_frozen() -> None:
    from dataclasses import FrozenInstanceError

    d = _generic_diagnosis()
    with pytest.raises(FrozenInstanceError):
        d.headline = "changed"  # type: ignore[misc]


def test_apply_failure_diagnosis_rejects_filename_without_use_transaction() -> None:
    """The renderer branches on use_transaction whenever failed_filename is
    set, so the pair must be consistent: a filename with use_transaction=None
    would silently render the no-transaction wording. Fail fast instead."""
    with pytest.raises(ValueError, match="use_transaction"):
        migrate.ApplyFailureDiagnosis(
            headline="boom",
            failed_filename="01.00.00_01_pre_failing.sql",
            use_transaction=None,
            invalid_indexes=(),
            schema="taskq",
        )


# ── apply-failure diagnosis attribution ─────────────────────────────────────


def _phase_scenario_migrations() -> tuple[Migration, Migration]:
    """discover() order for the --phase misattribution scenario: an
    earlier-version pending :post sorts BEFORE a later-version :pre (the
    sort key is version first), so under ``migrate up --phase pre`` — where
    only the :pre applies and fails — the first-unrecorded heuristic would
    blame the wrong file."""
    earlier_pending_post = Migration(
        version="01.00.00_01",
        phase="post",
        description="d",
        filename="01.00.00_01_post_pending.sql",
        sql_template="SELECT 1;",
    )
    later_failing_pre = Migration(
        version="01.00.02_01",
        phase="pre",
        description="d",
        filename="01.00.02_01_pre_failing.sql",
        sql_template="SELECT 1;",
    )
    return earlier_pending_post, later_failing_pre


async def test_diagnose_apply_failure_prefers_tagged_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under --phase the first unrecorded migration in discover() order is
    NOT necessarily the one that failed. apply_pending tags the exception
    with the failing migration; the diagnosis must trust the tag over the
    heuristic."""
    earlier_pending_post, later_failing_pre = _phase_scenario_migrations()
    monkeypatch.setattr(migrate, "discover", lambda: [earlier_pending_post, later_failing_pre])
    monkeypatch.setattr(migrate, "list_applied", AsyncMock(return_value=set()))
    exc = RuntimeError("boom")
    exc.__dict__["taskq_failed_migration"] = later_failing_pre

    d = await migrate.diagnose_apply_failure(object(), "taskq", exc)  # type: ignore[arg-type]  # Why: the tagged path never touches the conn (transactional tag), so a stand-in suffices.

    assert d.failed_filename == "01.00.02_01_pre_failing.sql"
    assert d.use_transaction is True


async def test_diagnose_apply_failure_untagged_falls_back_to_first_unrecorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An untagged exception (raised outside apply_pending's per-migration
    loop — e.g. the ledger ensure) keeps the original heuristic: first
    unrecorded in discover() order."""
    earlier_pending_post, later_failing_pre = _phase_scenario_migrations()
    monkeypatch.setattr(migrate, "discover", lambda: [earlier_pending_post, later_failing_pre])
    monkeypatch.setattr(migrate, "list_applied", AsyncMock(return_value=set()))

    d = await migrate.diagnose_apply_failure(object(), "taskq", RuntimeError("boom"))  # type: ignore[arg-type]  # Why: list_applied is monkeypatched, so the conn stand-in is never used.

    assert d.failed_filename == "01.00.00_01_post_pending.sql"
    assert d.use_transaction is True


async def test_diagnose_apply_failure_tagged_no_transaction_gathers_invalid_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tagged path must still gather INVALID-index debris when the
    tagged migration opted out of the transaction wrapper — an interrupted
    CREATE INDEX CONCURRENTLY is exactly the failure the tag exists for."""
    tagged = Migration(
        version="01.00.02_01",
        phase="post",
        description="d",
        filename="01.00.02_01_post_concurrent_idx.sql",
        sql_template="SELECT 1;",
        use_transaction=False,
    )
    monkeypatch.setattr(migrate, "list_invalid_indexes", AsyncMock(return_value=["jobs_queue_idx"]))
    exc = RuntimeError("boom")
    exc.__dict__["taskq_failed_migration"] = tagged

    d = await migrate.diagnose_apply_failure(object(), "taskq", exc)  # type: ignore[arg-type]  # Why: list_invalid_indexes is monkeypatched, so the conn stand-in is never used.

    assert d.failed_filename == "01.00.02_01_post_concurrent_idx.sql"
    assert d.use_transaction is False
    assert d.invalid_indexes == ("jobs_queue_idx",)


async def test_diagnose_apply_failure_headline_falls_back_to_type_name() -> None:
    """An exception whose first str() line is empty/whitespace must not
    render ``migration failed: `` with an empty headline — use the
    exception's type name instead."""
    d = await migrate.diagnose_apply_failure(  # type: ignore[arg-type]  # Why: the generic path suppresses every read, so a conn stand-in is fine.
        object(), "taskq", RuntimeError("   \nDETAIL: something")
    )
    assert d.headline == "RuntimeError"
