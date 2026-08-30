"""Migration runner.

Forward-only by design. The runner:

1. Discovers ``*.sql`` files under :mod:`taskq.migrations` in lexicographic
   order — the naming convention is ``{ver}_{nn}_{pre|post}_{description}.sql``.
2. Substitutes the ``{schema}`` placeholder with the configured schema name.
3. Applies migrations not already recorded in ``{schema}.schema_migrations``,
   recording a SHA-256 checksum of the rendered SQL after each successful apply.

There is no ``down`` operation. To revert, restore from a database backup.

Non-transactional migrations
----------------------------

By default each migration file runs inside its own transaction, so a failure
rolls the whole file back. Postgres forbids some statements inside a
transaction block — ``CREATE INDEX CONCURRENTLY``, ``DROP INDEX
CONCURRENTLY``, several ``ALTER TYPE``/``VACUUM`` forms — which makes them
inexpressible under the default wrapper. A migration can opt out by placing
the header directive ``-- taskq:no-transaction`` in its leading comment
block (``--`` line comments only, before the first SQL token); the runner
then executes the file statement by statement with no wrapping transaction
(Alembic's autocommit-block semantics). Two rules keep that safe:

* The migration **must be idempotent and re-runnable** — nothing rolls back,
  so a mid-file failure leaves earlier statements in place and the migration
  is re-executed on the next run. The ledger records completion only after
  every statement succeeds.
* An interrupted ``CREATE INDEX CONCURRENTLY`` leaves an ``INVALID`` index
  behind; the standard remedy is drop-and-rebuild, written into the
  migration itself::

      -- taskq:no-transaction
      -- NOT redundant with IF NOT EXISTS below: an interrupted CREATE INDEX
      -- CONCURRENTLY leaves an INVALID index that IF NOT EXISTS alone would
      -- silently skip rebuilding, so drop the debris first.
      DROP INDEX CONCURRENTLY IF EXISTS "{schema}".jobs_foo_idx;
      CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_foo_idx ON "{schema}".jobs (foo);

Transaction-control statements (``BEGIN``/``COMMIT``/``ROLLBACK``/...) are
rejected in non-transactional files — they would silently re-open a
transaction and, on failure, poison the caller's connection. Statement
splitting assumes ``standard_conforming_strings=on`` (the Postgres default
since 9.1).

The ledger column ``schema_migrations.use_transaction`` records how each
migration ran (``false`` = outside a transaction) so operators can see which
migrations were/are safe to run online; the runner adds the column on first
use, so pre-upgrade ledgers need no dedicated migration.
"""

import asyncio
import contextlib
import hashlib
import re
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from importlib import resources
from typing import Literal, TypeAlias

import asyncpg
import structlog

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)

__all__ = [
    "ApplyFailureDiagnosis",
    "Migration",
    "Phase",
    "apply_pending",
    "apply_pending_locked",
    "diagnose_apply_failure",
    "discover",
    "list_applied",
    "list_invalid_indexes",
    "render",
    "render_apply_failure_lines",
    "split_statements",
]

logger = structlog.get_logger("taskq.migrate")

Phase: TypeAlias = Literal["pre", "post"]  # noqa: UP040  # Why: typer's CliRunner does not support PEP 695 type aliases; traditional TypeAlias form is required for CLI testability.

_NAME_RE = re.compile(
    r"^(?P<ver>\d{2}\.\d{2}\.\d{2})_(?P<seq>\d{2})_(?P<phase>pre|post)_(?P<desc>[a-z0-9_]+)\.sql$"
)

# Prefix match (not fullmatch): a trailing note after the token is the common
# real-world form ("-- taskq:no-transaction — CIC cannot run inside a
# transaction"). Case-insensitive like SQL itself. \b stops prefix drift, so
# "-- taskq:no-transactional" does NOT match — but see the near-miss warning
# in _uses_transaction, which keeps such typos from failing silently.
_NO_TRANSACTION_DIRECTIVE_RE = re.compile(r"--\s*taskq:no-transaction\b", re.IGNORECASE)

_DOLLAR_TAG_RE = re.compile(
    r"\$[^\W\d][\w]*\$|\$\$"
)  # tags follow identifier rules (Unicode-aware, no digits first)


def _uses_transaction(sql_template: str, filename: str) -> bool:
    """Parse the ``-- taskq:no-transaction`` header directive.

    The directive is only honored in the file's *leading comment block* —
    the blank lines and ``--`` comments before the first SQL token — so a
    stray mention later in the file cannot silently flip a migration's
    semantics.

    A ``--`` line that mentions ``taskq:`` without matching the directive
    (a typo, or drift like ``taskq:no-transactional``) is nearly always an
    attempted opt-out that will silently run transactional — the worst
    outcome for a lock-duration-motivated migration. Warn so the author
    notices; the warning names the file because discovery parses many.
    """
    for line in sql_template.splitlines():
        stripped = line.strip()
        if stripped == "" or stripped.startswith("--"):
            if _NO_TRANSACTION_DIRECTIVE_RE.match(stripped):
                return False
            if "taskq:" in stripped.lower():
                logger.warning(
                    "migration-directive-unrecognized",
                    filename=filename,
                    line=stripped,
                )
            continue
        break
    return True


@dataclass(frozen=True, slots=True)
class Migration:
    """A single SQL migration file."""

    version: str
    """``{ver}_{nn}``, e.g. ``01.00.00_01``."""

    phase: Phase
    description: str
    filename: str
    sql_template: str

    use_transaction: bool = True
    """When False (``-- taskq:no-transaction`` header directive), apply the
    file statement by statement with no wrapping transaction. Postgres
    requires this for ``CREATE INDEX CONCURRENTLY`` and friends — but nothing
    rolls back on failure, so such migrations must be idempotent."""

    @property
    def key(self) -> str:
        """Identity stored in ``schema_migrations.version``: ``{version}:{phase}``."""
        return f"{self.version}:{self.phase}"

    def render(self, schema: str) -> str:
        return render(self.sql_template, schema)

    def checksum(self, schema: str) -> str:
        return hashlib.sha256(self.render(schema).encode("utf-8")).hexdigest()


def discover() -> list[Migration]:
    """Return all bundled migrations sorted by version, then ``pre`` before ``post``."""
    found: list[Migration] = []
    package = resources.files("taskq.migrations")
    for entry in package.iterdir():
        if not entry.is_file() or not entry.name.endswith(".sql"):
            continue
        match = _NAME_RE.match(entry.name)
        if match is None:
            raise ValueError(f"migration filename does not match convention: {entry.name!r}")
        version = f"{match.group('ver')}_{match.group('seq')}"
        phase: Phase = match.group("phase")  # type: ignore[assignment]  # Why: regex group "phase" is constrained to "pre|post" by _NAME_RE; re.match guarantees the value matches the Literal["pre", "post"] alias but str cannot be narrowed to it statically.
        sql_template = entry.read_text(
            encoding="utf-8-sig"
        )  # utf-8-sig: a BOM (Windows editors) must not silently disable the header directive
        found.append(
            Migration(
                version=version,
                phase=phase,
                description=match.group("desc"),
                filename=entry.name,
                sql_template=sql_template,
                use_transaction=_uses_transaction(sql_template, entry.name),
            )
        )
    found.sort(key=lambda m: (m.version, 0 if m.phase == "pre" else 1))
    return found


def render(template: str, schema: str) -> str:
    """Substitute ``{schema}`` in a SQL template.

    SQL files escape literal curly braces by doubling them (``{{`` → ``{``)
    because :func:`str.format` is the substitution engine.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    return template.format(schema=schema)


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements, without their ``;``.

    Non-transactional migrations are executed statement by statement: a
    multi-statement string sent through Postgres' simple query protocol runs
    as ONE implicit transaction, which would defeat the point (``CREATE
    INDEX CONCURRENTLY`` would still be "inside a transaction block").

    Splitting understands single-quoted strings (including ``E'...'``
    backslash escapes and ``''`` doubling), ``"..."``-quoted identifiers,
    ``--`` line comments, nested ``/* ... */`` block comments, and
    dollar-quoted bodies (``$$...$$`` / ``$tag$...$tag$``). Leading comments
    stay attached to the statement that follows them; comment-only chunks
    are dropped. Unterminated constructs yield one trailing chunk, leaving
    the syntax error to Postgres — same as executing the file whole.
    """
    statements: list[str] = []
    buf: list[str] = []
    has_content = False  # any non-comment, non-whitespace char in the chunk
    i = 0
    n = len(sql)
    state = "normal"
    backslash_escapes = False  # inside E'...' strings only
    block_depth = 0
    dollar_tag = ""

    def flush() -> None:
        nonlocal buf, has_content
        chunk = "".join(buf).strip()
        if has_content:
            statements.append(chunk)
        buf = []
        has_content = False

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if state == "normal":
            if ch == "'":
                # E'...' (E directly before the quote, not part of a longer
                # identifier) uses backslash escapes; plain '...' does not
                # (standard_conforming_strings=on).
                backslash_escapes = buf[-1:] in (["e"], ["E"]) and (
                    len(buf) < 2 or not (buf[-2].isalnum() or buf[-2] in "_$")
                )
                state = "squote"
                has_content = True
                buf.append(ch)
                i += 1
            elif ch == '"':
                state = "dquote"
                has_content = True
                buf.append(ch)
                i += 1
            elif ch == "-" and nxt == "-":
                state = "line_comment"
                buf.append(ch)
                buf.append(nxt)
                i += 2
            elif ch == "/" and nxt == "*":
                state = "block_comment"
                block_depth = 1
                buf.append(ch)
                buf.append(nxt)
                i += 2
            elif (
                ch == "$"
                # A dollar-quote tag cannot immediately follow an identifier
                # char — ``a$b$c`` is a legal identifier, not a quoted body
                # (same rule as the E'...' detection above). buf holds
                # everything since the last flush; empty buf (right after a
                # ``;``) means no previous char, so the branch stays allowed.
                # Why: isalnum() approximates PG's identifier-char rule (any
                # char >= 0x80 counts as an identifier char there) — close
                # enough for a splitter, and consistent with the E'...' gate.
                and not (buf and (buf[-1].isalnum() or buf[-1] in "_$"))
                and (m := _DOLLAR_TAG_RE.match(sql, i)) is not None
            ):
                state = "dollar"
                dollar_tag = m.group(0)
                has_content = True
                buf.append(dollar_tag)
                i = m.end()
            elif ch == ";":
                flush()
                i += 1
            else:
                if not ch.isspace():
                    has_content = True
                buf.append(ch)
                i += 1
        elif state == "squote":
            buf.append(ch)
            if backslash_escapes and ch == "\\" and i + 1 < n:
                buf.append(sql[i + 1])
                i += 2
            elif ch == "'":
                if nxt == "'":  # '' escape inside the string
                    buf.append(nxt)
                    i += 2
                else:
                    state = "normal"
                    i += 1
            else:
                i += 1
        elif state == "dquote":
            buf.append(ch)
            if ch == '"':
                if nxt == '"':  # "" escape inside the identifier
                    buf.append(nxt)
                    i += 2
                else:
                    state = "normal"
                    i += 1
            else:
                i += 1
        elif state == "line_comment":
            buf.append(ch)
            if ch == "\n" or ch == "\r":  # Postgres ends -- comments at CR too (CR-only files)
                state = "normal"
            i += 1
        elif state == "block_comment":
            if ch == "/" and nxt == "*":
                block_depth += 1
                buf.append(ch)
                buf.append(nxt)
                i += 2
            elif ch == "*" and nxt == "/":
                block_depth -= 1
                buf.append(ch)
                buf.append(nxt)
                i += 2
                if block_depth == 0:
                    state = "normal"
            else:
                buf.append(ch)
                i += 1
        else:  # dollar-quoted body: verbatim until the matching closing tag
            if ch == "$" and sql.startswith(dollar_tag, i):
                buf.append(dollar_tag)
                i += len(dollar_tag)
                state = "normal"
            else:
                buf.append(ch)
                i += 1

    flush()
    return statements


def _skip_sql_trivia(sql: str, i: int) -> int:
    """Advance past whitespace and comments (``--`` line, nested ``/* */``).

    Comments are valid trivia here because Postgres treats them as
    whitespace when scanning keywords — ``SET /* x */ LOCAL`` is the same
    statement to the server as ``SET LOCAL``, and block comments NEST, which
    no single regex can skip. The guard must therefore skip exactly what the
    server skips, or comment-wrapped forms slide past as silent no-ops.
    """
    n = len(sql)
    while i < n:
        if sql[i].isspace():
            i += 1
        elif sql.startswith("--", i):
            # Postgres ends -- comments at CR too (CR-only files).
            end = i + 2
            while end < n and sql[end] not in "\r\n":
                end += 1
            i = end
        elif sql.startswith("/*", i):
            depth = 1
            i += 2
            while i < n and depth > 0:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            # An unterminated block comment consumes the rest of the
            # statement; the word read after it finds nothing and the guard
            # defers to Postgres' own syntax error — same as split_statements.
        else:
            break
    return i


def _read_sql_word(sql: str, i: int) -> tuple[str, int]:
    """Read one keyword-shaped word (identifier chars) starting at ``i``,
    lowercased, plus the index just past it."""
    start = i
    n = len(sql)
    while i < n and (sql[i].isalnum() or sql[i] in "_$"):
        i += 1
    return sql[start:i].lower(), i


_TXN_CONTROL_WORDS = frozenset(
    {"begin", "commit", "rollback", "end", "abort", "start", "savepoint", "release"}
)


def _transaction_control_word(statement: str) -> str | None:
    """Uppercase transaction-control keyword the statement opens with
    (``'SET LOCAL'`` / ``'SET TRANSACTION'`` for the two-word forms), or
    ``None`` when the statement is allowed.

    Beyond BEGIN/COMMIT and friends this covers SAVEPOINT/RELEASE (only
    valid inside a transaction) and SET LOCAL / SET TRANSACTION (silent
    no-ops outside one — the author believes a setting applied when it did
    not). Plain SET / SET SESSION stays allowed: it is session-scoped and
    behaves identically either way.
    """
    i = _skip_sql_trivia(statement, 0)
    word, i = _read_sql_word(statement, i)
    if word in _TXN_CONTROL_WORDS:
        return word.upper()
    if word == "set":
        second, _ = _read_sql_word(statement, _skip_sql_trivia(statement, i))
        if second in ("local", "transaction"):
            return f"SET {second.upper()}"
    return None


def _reject_transaction_control(migration: Migration, statements: list[str]) -> None:
    """Forbid transaction-control statements in a non-transactional migration.

    ``BEGIN``/``COMMIT`` and friends would silently re-open an explicit
    transaction (defeating ``CREATE INDEX CONCURRENTLY``) and, on failure,
    leave the caller's connection in an aborted transaction. ``SET LOCAL`` /
    ``SET TRANSACTION`` are rejected for the opposite reason: outside a
    transaction they are SILENT no-ops (server WARNING only), so an author
    could believe e.g. ``statement_timeout`` was disabled for a long index
    build when it was not. ``SAVEPOINT``/``RELEASE`` would fail loudly at
    execution time, but the guard's value is rejecting the whole file before
    any statement executes — a rejected file applies nothing.
    """
    for statement in statements:
        word = _transaction_control_word(statement)
        if word is not None:
            raise ValueError(
                f"migration {migration.filename!r} is marked no-transaction but contains "
                f"transaction-control statement {word!r}; remove it — "
                "the runner manages transactions"
            )


async def list_applied(conn: asyncpg.Connection, schema: str) -> set[str]:
    """Return ``{version}:{phase}`` keys recorded in ``schema_migrations``.

    Returns an empty set if the schema or table does not yet exist — a
    fresh database is the common case on first ``migrate up``.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'schema_migrations'
        )
        """,
        schema,
    )
    if not exists:
        return set()
    rows = await conn.fetch(f'SELECT version, checksum FROM "{schema}".schema_migrations')
    applied_keys: set[str] = set()
    for r in rows:
        applied_keys.add(r["version"])
    return applied_keys


async def list_invalid_indexes(conn: asyncpg.Connection, schema: str) -> list[str]:
    """Return the names of INVALID indexes in ``schema``, sorted by name.

    An interrupted ``CREATE INDEX CONCURRENTLY`` leaves an INVALID index
    behind: the query planner ignores it, but writers still maintain it, so
    it is pure overhead (and blocks the re-run's ``IF NOT EXISTS``). The CLI
    surfaces these in its failure report so users never have to query the
    catalogs by hand.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND NOT i.indisvalid
        ORDER BY c.relname
        """,
        schema,
    )
    return [r["relname"] for r in rows]


async def apply_pending(
    conn: asyncpg.Connection,
    *,
    schema: str,
    phase: Phase | None = None,
    target: str | None = None,
    max_steps: int | None = None,
) -> list[Migration]:
    """Apply pending migrations.

    Each migration runs in its own transaction so a failure in one file does
    not leave a half-applied schema — unless the file carries the
    ``-- taskq:no-transaction`` header directive (:attr:`Migration.use_transaction`),
    in which case it is executed statement by statement with no wrapping
    transaction. That unlocks ``CREATE INDEX CONCURRENTLY`` and friends, but
    nothing rolls back on failure: prior statements stay applied, the
    migration is NOT recorded in the ledger, and it will be re-executed on
    the next run — so non-transactional migrations must be idempotent.

    :param phase: restrict to ``pre`` or ``post`` migrations only.
    :param target: stop after applying this version (inclusive).
    :param max_steps: stop after this many applies.
    :returns: migrations that were applied (in order).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")

    exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'schema_migrations'
        )
        """,
        schema,
    )
    if exists:
        applied_rows = await conn.fetch(
            f'SELECT version, checksum FROM "{schema}".schema_migrations'
        )
        applied_keys: set[str] = {r["version"] for r in applied_rows}
        applied_checksums: dict[str, str] = {r["version"]: r["checksum"] for r in applied_rows}
    else:
        applied_keys = set()
        applied_checksums: dict[str, str] = {}

    all_migrations = discover()

    for m in all_migrations:
        if m.key in applied_checksums:
            stored = applied_checksums[m.key]
            current = m.checksum(schema)
            if stored != current:
                logger.warning(
                    "migration-checksum-drift",
                    key=m.key,
                    stored_checksum=stored,
                    current_checksum=current,
                )

    pending = [m for m in all_migrations if m.key not in applied_keys]
    if phase is not None:
        pending = [m for m in pending if m.phase == phase]

    # Truncate to the prefix this run will actually apply (target is
    # inclusive; max_steps caps the count) BEFORE validating phase ordering,
    # so the guard below reasons about exactly this run's apply list and
    # cannot refuse over a migration the truncation would have skipped.
    effective: list[Migration] = []
    for migration in pending:
        effective.append(migration)
        if target is not None and migration.version == target:
            break
        if max_steps is not None and len(effective) >= max_steps:
            break

    # A post-phase migration is only eligible once its same-version pre-phase
    # counterpart is applied (or will be applied earlier in this same run).
    # Post-phase migrations remove the structures pre-phase migrations add —
    # e.g. 01.00.03_01:post drops the old idempotency index the rolling-deploy
    # overlap window depends on. Applying one first (e.g. `migrate up --phase
    # post` against a schema whose pre phase hasn't run) both strands the pre
    # phase's protections permanently (the post is already recorded) and can
    # break not-yet-upgraded workers immediately. Refuse loudly instead.
    pre_versions: set[str] = {m.version for m in all_migrations if m.phase == "pre"}
    eligible_keys = set(applied_keys)
    for m in effective:
        if m.phase == "post" and m.version in pre_versions:
            pre_key = f"{m.version}:pre"
            if pre_key not in eligible_keys:
                raise ValueError(
                    f"migration {m.key} cannot be applied before its pre-phase "
                    f"counterpart {pre_key}. Run `taskq migrate up --phase pre` "
                    f"(or a plain `taskq migrate up`) first."
                )
        eligible_keys.add(m.key)

    # The ledger must be able to record how each migration runs. An existing
    # ledger is upgraded once, up front, outside any migration transaction
    # (pre-upgrade ledgers lack the column); a fresh install has no ledger
    # until the initial migration creates one mid-loop, so the ensure runs
    # lazily on the first record instead.
    ledger_ready = False
    if exists and pending:
        await _ensure_ledger_use_transaction_column(conn, schema)
        ledger_ready = True

    applied_now: list[Migration] = []
    for migration in effective:
        try:
            if migration.use_transaction:
                async with conn.transaction():
                    await conn.execute(migration.render(schema))
                    if not ledger_ready:
                        await _ensure_ledger_use_transaction_column(conn, schema)
                        ledger_ready = True
                    await _record_applied(conn, schema, migration)
            else:
                # No wrapping transaction: each statement commits on its own, so
                # the ledger is written only after every statement succeeds. A
                # failure leaves earlier statements in place and nothing recorded;
                # idempotency makes the re-run safe (see module docstring).
                logger.warning(
                    "migration-no-transaction",
                    key=migration.key,
                    filename=migration.filename,
                )
                statements = split_statements(migration.render(schema))
                _reject_transaction_control(migration, statements)
                for statement in statements:
                    await conn.execute(statement)
                if not ledger_ready:
                    await _ensure_ledger_use_transaction_column(conn, schema)
                    ledger_ready = True
                await _record_applied(conn, schema, migration)
        except Exception as exc:
            # Why tag and re-raise: diagnose_apply_failure's first-unrecorded
            # heuristic misattributes the failure under --phase (an
            # earlier-version pending migration of the OTHER phase sorts
            # first in discover() order). The exception object itself is the
            # reliable channel to the diagnosis — tagging, not wrapping, so
            # the caller-visible exception type is unchanged.
            exc.__dict__["taskq_failed_migration"] = migration
            raise
        applied_now.append(migration)
    return applied_now


async def _ensure_ledger_use_transaction_column(conn: asyncpg.Connection, schema: str) -> None:
    """Add the ledger's ``use_transaction`` column if it is missing.

    The ledger is runner bookkeeping (like Rails' ``schema_migrations`` or
    Alembic's ``alembic_version``), so the runner owns its shape and upgrades
    it in place — no dedicated migration file is needed for ledgers created
    by older TaskQ versions. Pre-existing rows backfill to ``true``: every
    migration applied before this column existed ran inside a transaction.
    """
    await conn.execute(
        f'ALTER TABLE "{schema}".schema_migrations '
        "ADD COLUMN IF NOT EXISTS use_transaction boolean NOT NULL DEFAULT true"
    )


async def _record_applied(conn: asyncpg.Connection, schema: str, migration: Migration) -> None:
    """Record a successfully applied migration in ``schema_migrations``."""
    await conn.execute(
        f'INSERT INTO "{schema}".schema_migrations (version, checksum, use_transaction) '
        "VALUES ($1, $2, $3)",
        migration.key,
        migration.checksum(schema),
        migration.use_transaction,
    )


_MIGRATION_LOCK_KEY: int = 1_234_567

#: Bound on how long to WAIT for another process's migration lock.
#:
#: `pg_advisory_lock` is a blocking acquire with no client-side bound, so a
#: replica arriving while another holds the lock mid-DDL waited indefinitely
#: with no log line -- long enough to blow past a container platform's startup
#: probe, get killed, restart, and block again. This bounds the WAIT only; it
#: is reset before the migrations themselves run so a legitimately long DDL
#: step (index builds, ADD CONSTRAINT ... CHECK) is never killed halfway.
DEFAULT_MIGRATION_LOCK_TIMEOUT: float = 120.0


# ── apply-failure self-diagnosis ──────────────────────────────────────────────

# Why this lives here and not in the CLI: both failure surfaces (``migrate
# up`` and worker/UI startup via ``apply_pending_locked``) need the same
# report, and the gathering logic reads ``discover``/``list_applied``/
# ``list_invalid_indexes`` from this module — a helper module would
# circular-import them.

# Why a separate action line for startup: a worker/startup apply failure is
# retried by restarting the process (migrations are idempotent and the
# runner self-heals on retry), not by re-running the CLI — the CLI guidance
# would send operators down the wrong path. Escalate to the CLI only when
# the restart loop does not heal.
_STARTUP_ACTION_LINE = (
    "Action: restart is safe — migrations are idempotent and self-heal on retry; "
    "if the failure repeats, run `taskq migrate up` and report the output."
)


@dataclass(frozen=True, slots=True)
class ApplyFailureDiagnosis:
    """Self-diagnosis of a failed migration apply, gathered on the still-open
    connection by :func:`diagnose_apply_failure` and rendered by
    :func:`render_apply_failure_lines`."""

    headline: str
    """First line of the original error (asyncpg messages can be multiline)."""

    failed_filename: str | None
    """Filename of the migration that failed — taken from the exception's
    ``taskq_failed_migration`` tag when :func:`apply_pending` attached one,
    else the first unrecorded migration in ``discover()`` order (a heuristic
    that misattributes under ``--phase``); ``None`` when it could not be
    determined."""

    use_transaction: bool | None
    """``Migration.use_transaction`` of the failed migration; ``None`` when
    the failed migration could not be determined."""

    invalid_indexes: tuple[str, ...]
    """INVALID indexes in the schema — debris of an interrupted ``CREATE
    INDEX CONCURRENTLY``; only gathered for no-transaction failures."""

    # Why carried here: the INVALID-index line names the schema and the
    # renderer must stay pure (no re-querying), so the diagnosis owns every
    # value the report needs.
    schema: str
    """Schema the report names in the INVALID-index line."""

    def __post_init__(self) -> None:
        # Why: the renderer branches on use_transaction whenever
        # failed_filename is set, so the pair must stay consistent — a
        # filename with use_transaction=None would silently render the
        # no-transaction wording for a failure whose nature is unknown.
        if self.failed_filename is not None and self.use_transaction is None:
            raise ValueError(
                "ApplyFailureDiagnosis: use_transaction is required when failed_filename is set"
            )


def _exception_headline(exc: Exception) -> str:
    """First line of ``str(exc)`` — or the type name when that line is empty
    or whitespace, so a report never opens with ``migration failed: `` and a
    blank headline. asyncpg messages can be multiline (DETAIL/HINT lines);
    the report keeps the first line only."""
    message = str(exc)
    first_line = message.splitlines()[0] if message else ""
    return first_line if first_line.strip() else type(exc).__name__


async def diagnose_apply_failure(
    conn: asyncpg.Connection, schema: str, exc: Exception
) -> ApplyFailureDiagnosis:
    """Gather a self-diagnosis of a failed apply on the still-open conn.

    Both apply paths leave the connection reusable (a transactional failure
    rolls back; the no-transaction path never opened one), so the same conn
    can report what failed and what state the schema is in. Diagnosis must
    NEVER mask the original error: every read is individually suppressed,
    and whatever could not be gathered degrades to the generic report
    (``failed_filename=None``).
    """
    headline = _exception_headline(exc)

    failed: Migration | None = None
    # apply_pending tags the exception with the migration that failed before
    # re-raising — trust the tag over any heuristic. The isinstance guard
    # keeps an exotic/mocked attribute from steering the report.
    tagged = getattr(exc, "taskq_failed_migration", None)
    if isinstance(tagged, Migration):
        failed = tagged
    else:
        applied: set[str] | None = None
        with contextlib.suppress(Exception):
            applied = await list_applied(conn, schema)
        if applied is not None:
            # Fallback for exceptions from non-loop paths (e.g. the ledger
            # ensure): apply_pending applies in discover() order and stops
            # at the first failure, so the first unrecorded migration is the
            # one that failed. Best-effort only — this misattributes under
            # --phase, which is exactly why the loop tags.
            with contextlib.suppress(Exception):
                failed = next((m for m in discover() if m.key not in applied), None)

    invalid: list[str] = []
    if failed is not None and not failed.use_transaction:
        with contextlib.suppress(Exception):
            invalid = await list_invalid_indexes(conn, schema)

    return ApplyFailureDiagnosis(
        headline=headline,
        failed_filename=failed.filename if failed is not None else None,
        use_transaction=failed.use_transaction if failed is not None else None,
        invalid_indexes=tuple(invalid),
        schema=schema,
    )


def render_apply_failure_lines(d: ApplyFailureDiagnosis, *, startup: bool = False) -> list[str]:
    """Render a diagnosis as report lines (CLI stderr / startup SystemExit).

    ``startup=False`` reproduces the ``migrate up`` CLI report verbatim;
    ``startup=True`` swaps only the action line for the restart-safe
    variant (:data:`_STARTUP_ACTION_LINE`).
    """
    if startup:
        action = _STARTUP_ACTION_LINE
    elif d.failed_filename is None:
        action = (
            "Action: fix the error and re-run `taskq migrate up` — already-applied "
            "migrations are skipped."
        )
    elif d.use_transaction:
        action = "Action: fix the error and re-run `taskq migrate up`."
    else:
        action = (
            "Action: re-run `taskq migrate up` — the migration is idempotent and "
            "drops/rebuilds the debris itself."
        )

    if d.failed_filename is None:
        return [f"migration failed: {d.headline}", action]
    if d.use_transaction:
        return [
            f"migration {d.failed_filename} failed: {d.headline}",
            "It ran in a transaction and rolled back: nothing from the migration was applied.",
            action,
        ]
    lines = [
        f"migration {d.failed_filename} failed: {d.headline}",
        "It ran WITHOUT a transaction (-- taskq:no-transaction): statements "
        "before the failure remain applied, and the migration was NOT recorded "
        "in the ledger.",
    ]
    if d.invalid_indexes:
        names = ", ".join(d.invalid_indexes)
        lines.append(
            f'INVALID index(es) in schema "{d.schema}": {names} — an interrupted '
            "CREATE INDEX CONCURRENTLY left them behind."
        )
    lines.append(action)
    return lines


@contextlib.asynccontextmanager
async def migration_advisory_lock(
    conn: asyncpg.Connection, lock_timeout: float = DEFAULT_MIGRATION_LOCK_TIMEOUT
) -> AsyncGenerator[None]:
    """Hold the migration advisory lock on *conn*, with a bounded wait.

    Extracted so the CLI and :func:`apply_pending_locked` serialize on the SAME
    lock without duplicating the acquire/reset/release protocol. The CLI cannot
    simply delegate to ``apply_pending_locked``: it owns the connection so it
    can run ``_report_up_failure`` diagnostics on it after a failure, and
    ``apply_pending_locked`` converts failures to ``SystemExit`` before that
    could run.

    ``lock_timeout`` bounds only the WAIT, via Postgres' ``lock_timeout`` GUC
    (which governs advisory-lock acquisition). It is reset to unlimited before
    the body runs, so a long DDL step is never killed midway. ``0`` waits
    indefinitely, the pre-existing behaviour.

    Raises :class:`SystemExit` on contention rather than blocking until the
    container platform kills the process.
    """
    if lock_timeout > 0:
        # Milliseconds; applies to the advisory-lock acquire below.
        await conn.execute(f"SET lock_timeout = {int(lock_timeout * 1000)}")
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_KEY)
    except asyncpg.LockNotAvailableError as exc:
        msg = (
            f"could not acquire the migration advisory lock within {lock_timeout}s: "
            "another process is applying migrations (or is wedged holding the lock). "
            "Migrations must run once, from a single pre-deploy job or init "
            "container -- not from every replica."
        )
        raise SystemExit(msg) from exc
    finally:
        # Reset before the DDL so a legitimately long migration step is not
        # killed by the wait bound.
        if lock_timeout > 0:
            with contextlib.suppress(Exception):
                await conn.execute("SET lock_timeout = 0")
    try:
        yield
    finally:
        # Why bounded: contextlib.suppress catches errors but cannot stop a
        # call that never returns; a dead PG wedges the unlock indefinitely.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(
                conn.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY),
                timeout=CLOSE_TIMEOUT_SECS,
            )


async def apply_pending_locked(
    dsn: str | None = None,
    *,
    schema: str,
    phase: Phase | None = None,
    target: str | None = None,
    max_steps: int | None = None,
    conn: asyncpg.Connection | None = None,
    conn_factory: Callable[[], Awaitable[asyncpg.Connection]] | None = None,
    lock_timeout: float = DEFAULT_MIGRATION_LOCK_TIMEOUT,
) -> list[Migration]:
    """Apply pending migrations under a session-level advisory lock.

    Acquires ``pg_advisory_lock`` to prevent concurrent startup races,
    applies pending migrations, and releases the lock.

    ``lock_timeout`` bounds only the **wait** for the lock, via Postgres'
    ``lock_timeout`` GUC, which applies to advisory-lock acquisition. It is
    reset to unlimited before the migrations run, so a long DDL step is never
    interrupted midway. Pass ``0`` to wait indefinitely (the old behaviour).
    Losing the race raises :class:`SystemExit` naming the contention, rather
    than hanging until the platform kills the container.

    Connection sources (mutually exclusive):
    * ``conn`` — pre-constructed, caller-owned; NOT closed here.
    * ``conn_factory`` — zero-arg async factory; closed in ``finally``.
    * ``dsn`` — ``asyncpg.connect(dsn)``; closed in ``finally``.

    Raises :class:`SystemExit` on failure so the calling process aborts
    cleanly.  This is the recommended entry point for CLI ``--migrate``
    and admin sidecar ``TASKQ_MIGRATE_ON_START`` paths.
    """
    if conn is not None and conn_factory is not None:
        raise ValueError("apply_pending_locked: provide 'conn' or 'conn_factory', not both")
    if dsn is not None and (conn is not None or conn_factory is not None):
        raise ValueError(
            "apply_pending_locked: 'dsn' is mutually exclusive with 'conn' and 'conn_factory'"
        )
    if conn is None and conn_factory is None and dsn is None:
        raise ValueError("apply_pending_locked: provide 'dsn', 'conn', or 'conn_factory'")

    owns_conn = conn is None  # factory/DSN → we close; caller-owned → we don't
    c: asyncpg.Connection | None = None
    try:
        if conn is not None:
            c = conn
        elif conn_factory is not None:
            c = await conn_factory()
        else:
            assert dsn is not None  # guarded by validation above
            c = await asyncpg.connect(dsn)
        async with migration_advisory_lock(c, lock_timeout):
            applied = await apply_pending(
                c, schema=schema, phase=phase, target=target, max_steps=max_steps
            )
        if applied:
            logger.info("migrations-applied-before-startup", count=len(applied))
        else:
            logger.info("no-pending-migrations")
        return applied
    except SystemExit:
        # Lock-contention exit above is already precise; do not re-wrap it as
        # "migration failed", which would misreport a queueing problem as a
        # broken migration.
        raise
    except Exception as exc:
        if c is None:
            # The conn was never acquired (conn_factory/asyncpg.connect
            # raised): there is nothing to diagnose on — keep the generic
            # wrap.
            raise SystemExit(f"migration failed, aborting startup: {exc}") from exc
        diagnosis: ApplyFailureDiagnosis | None = None
        with contextlib.suppress(Exception):
            # Best-effort: the conn is still open (both apply paths leave it
            # reusable), so report which migration failed and what state the
            # schema is in instead of escaping a raw error. Diagnosis must
            # never mask the original error — any surprise falls back to the
            # generic wrap below.
            diagnosis = await diagnose_apply_failure(c, schema, exc)
        if diagnosis is None:
            raise SystemExit(f"migration failed, aborting startup: {exc}") from exc
        # Why one " — "-joined line: startup logs are grepped, not read as
        # paragraphs, and the prefix stays stable for existing alerting
        # rules pinned on it.
        raise SystemExit(
            "migration failed, aborting startup: "
            + " — ".join(render_apply_failure_lines(diagnosis, startup=True))
        ) from exc
    finally:
        if c is not None and owns_conn:
            # The advisory unlock now belongs to migration_advisory_lock's own
            # finally (bounded there for the same reason), so only the owned
            # connection close remains here. Unlocking again would fire
            # pg_advisory_unlock on a lock this session no longer holds, which
            # Postgres answers with a WARNING and a false return.
            #
            # Why bounded: contextlib.suppress catches errors but cannot stop a
            # call that never returns, and this finally runs before any lifespan
            # exit stack exists, so an unbounded close would wedge CLI/UI
            # startup forever. close_conn_bounded terminates on timeout.
            await close_conn_bounded(c, "migrate", CLOSE_TIMEOUT_SECS)
