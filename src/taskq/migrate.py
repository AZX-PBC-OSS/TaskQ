"""Migration runner.

Forward-only by design. The runner:

1. Discovers ``*.sql`` files under :mod:`taskq.migrations` in lexicographic
   order — the naming convention is ``{ver}_{nn}_{pre|post}_{description}.sql``.
2. Substitutes the ``{schema}`` placeholder with the configured schema name.
3. Applies migrations not already recorded in ``{schema}.schema_migrations``,
   recording a SHA-256 checksum of the rendered SQL after each successful apply.

There is no ``down`` operation. To revert, restore from a database backup.
"""

import contextlib
import hashlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from importlib import resources
from typing import Literal, TypeAlias

import asyncpg
import structlog

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
]

logger = structlog.get_logger("taskq.migrate")

Phase: TypeAlias = Literal["pre", "post"]  # noqa: UP040  # Why: typer's CliRunner does not support PEP 695 type aliases; traditional TypeAlias form is required for CLI testability.

_NAME_RE = re.compile(
    r"^(?P<ver>\d{2}\.\d{2}\.\d{2})_(?P<seq>\d{2})_(?P<phase>pre|post)_(?P<desc>[a-z0-9_]+)\.sql$"
)


@dataclass(frozen=True, slots=True)
class Migration:
    """A single SQL migration file."""

    version: str
    """``{ver}_{nn}``, e.g. ``01.00.00_01``."""

    phase: Phase
    description: str
    filename: str
    sql_template: str

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
        found.append(
            Migration(
                version=version,
                phase=phase,
                description=match.group("desc"),
                filename=entry.name,
                sql_template=entry.read_text(encoding="utf-8"),
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
    not leave a half-applied schema.

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

    applied_now: list[Migration] = []
    for migration in pending:
        async with conn.transaction():
            await conn.execute(migration.render(schema))
            await conn.execute(
                f'INSERT INTO "{schema}".schema_migrations (version, checksum) VALUES ($1, $2)',
                migration.key,
                migration.checksum(schema),
            )
        applied_now.append(migration)
        if target is not None and migration.version == target:
            break
        if max_steps is not None and len(applied_now) >= max_steps:
            break
    return applied_now


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
            with contextlib.suppress(Exception):
                await c.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_KEY)
            if owns_conn:
                with contextlib.suppress(Exception):
                    await c.close()
