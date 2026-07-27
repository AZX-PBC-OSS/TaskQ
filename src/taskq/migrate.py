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
from collections.abc import Awaitable, Callable
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
    "Migration",
    "Phase",
    "apply_pending",
    "apply_pending_locked",
    "discover",
    "list_applied",
    "render",
    "split_statements",
]

logger = structlog.get_logger("taskq.migrate")

Phase: TypeAlias = Literal["pre", "post"]  # noqa: UP040  # Why: typer's CliRunner does not support PEP 695 type aliases; traditional TypeAlias form is required for CLI testability.

_NAME_RE = re.compile(
    r"^(?P<ver>\d{2}\.\d{2}\.\d{2})_(?P<seq>\d{2})_(?P<phase>pre|post)_(?P<desc>[a-z0-9_]+)\.sql$"
)

_NO_TRANSACTION_DIRECTIVE_RE = re.compile(r"--\s*taskq:no-transaction")

# First keyword of a split statement, skipping any leading line/block
# comments the splitter left attached. Used to reject transaction control in
# non-transactional migrations.
_TXN_CONTROL_RE = re.compile(
    r"\A(?:(?:--[^\n]*(?:\n|\r|$))|(?:/\*.*?\*/)|\s)*"
    r"(begin|commit|rollback|end|abort|start)\b",
    re.IGNORECASE | re.DOTALL,
)

_DOLLAR_TAG_RE = re.compile(
    r"\$[^\W\d][\w]*\$|\$\$"
)  # tags follow identifier rules (Unicode-aware, no digits first)


def _uses_transaction(sql_template: str) -> bool:
    """Parse the ``-- taskq:no-transaction`` header directive.

    The directive is only honored in the file's *leading comment block* —
    the blank lines and ``--`` comments before the first SQL token — so a
    stray mention later in the file cannot silently flip a migration's
    semantics.
    """
    for line in sql_template.splitlines():
        stripped = line.strip()
        if stripped == "" or stripped.startswith("--"):
            if _NO_TRANSACTION_DIRECTIVE_RE.fullmatch(stripped):
                return False
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
                use_transaction=_uses_transaction(sql_template),
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
            elif ch == "$" and (m := _DOLLAR_TAG_RE.match(sql, i)) is not None:
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


def _reject_transaction_control(migration: Migration, statements: list[str]) -> None:
    """Forbid transaction-control statements in a non-transactional migration.

    ``BEGIN``/``COMMIT`` and friends would silently re-open an explicit
    transaction (defeating ``CREATE INDEX CONCURRENTLY``) and, on failure,
    leave the caller's connection in an aborted transaction. Checked before
    any statement executes, so a rejected file applies nothing.
    """
    for statement in statements:
        match = _TXN_CONTROL_RE.match(statement)
        if match is not None:
            raise ValueError(
                f"migration {migration.filename!r} is marked no-transaction but contains "
                f"transaction-control statement {match.group(1).upper()!r}; remove it — "
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


async def apply_pending_locked(
    dsn: str | None = None,
    *,
    schema: str,
    phase: Phase | None = None,
    target: str | None = None,
    max_steps: int | None = None,
    conn: asyncpg.Connection | None = None,
    conn_factory: Callable[[], Awaitable[asyncpg.Connection]] | None = None,
) -> list[Migration]:
    """Apply pending migrations under a session-level advisory lock.

    Acquires ``pg_advisory_lock`` to prevent concurrent startup races,
    applies pending migrations, and releases the lock.

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
        await c.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_KEY)
        applied = await apply_pending(
            c, schema=schema, phase=phase, target=target, max_steps=max_steps
        )
        if applied:
            logger.info("applied migrations before startup", count=len(applied))
        else:
            logger.info("no pending migrations")
        return applied
    except Exception as exc:
        raise SystemExit(f"migration failed, aborting startup: {exc}") from exc
    finally:
        if c is not None:
            # Why the bounds: contextlib.suppress(Exception) catches errors
            # but cannot stop a call that never returns — a dead PG wedges
            # the unlock execute / conn close indefinitely, and this finally
            # runs before the lifespan exit stack exists, so an unbounded
            # teardown here would wedge CLI/UI startup forever. The unlock
            # is bounded by wait_for (+suppress); the owned close goes
            # through close_conn_bounded, which terminates the conn on
            # timeout — worst case 2 x CLOSE_TIMEOUT_SECS instead of forever.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    c.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY),
                    timeout=CLOSE_TIMEOUT_SECS,
                )
            if owns_conn:
                await close_conn_bounded(c, "migrate", CLOSE_TIMEOUT_SECS)
