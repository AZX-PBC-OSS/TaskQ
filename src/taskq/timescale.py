"""Optional TimescaleDB hypertable support for retention and metrics.

Opt-in via ``TASKQ_TIMESCALEDB_HYPERTABLES=true``. When the flag is false
(the default) this module's setup path is a pure no-op gate: it issues
ZERO statements against the server and the library behaves exactly as it
does on vanilla Postgres, which remains fully supported.

Why a setup-time DDL path and not a migration
---------------------------------------------

The migration ledger (``taskq.migrate``) is checksummed and each file
applies exactly once per schema. A hypertable conversion cannot live
there:

* Opt-in happens at an arbitrary point in a database's life. A migration
  that already applied never re-runs, so an operator who flips the flag
  on a schema migrated without it could never convert the tables.
* The ledger's checksum is fail-closed: the same file must apply
  identically on vanilla Postgres and TimescaleDB. A file whose body
  silently no-ops on vanilla servers would hide exactly the
  misconfiguration this feature refuses to hide.

So the conversion is idempotent DDL run by :func:`enable_hypertables`
from the ``taskq migrate up`` deploy step (inside the migration advisory
lock, after pending migrations apply). It re-runs on every deploy and
converges the schema to the flag's current value.

What converts, and what the SQL guarantees
------------------------------------------

Three tables, each partitioned on its own time column:

* ``job_events`` on ``occurred_at``: the metrics/audit log, the highest
  write volume and the table chunk pruning helps most. Its primary key
  becomes ``UNIQUE (id, occurred_at)`` (a hypertable requires the
  partition column in every unique constraint; nothing references
  ``job_events.id``). The foreign key to ``jobs(id) ON DELETE CASCADE``
  is kept: foreign keys FROM a hypertable to a regular table are
  supported.
* ``jobs_archive`` on ``finished_at``: its primary key on ``id`` alone
  cannot survive (same hypertable rule), so it becomes
  ``UNIQUE (id, finished_at)``. The re-archive guarantee the old primary
  key enforced by accident of uniqueness ("a job id is archived at most
  once") is enforced instead by an explicit ``NOT EXISTS`` guard in the
  archive write itself (:mod:`taskq.worker._leader_shared`), which is
  what keeps the prune's ghost semantics identical in both modes.
* ``job_attempts_archive`` on ``started_at``, primary key widened to
  ``UNIQUE (job_id, attempt, started_at)`` for the same reason.

The one structural loss is deliberate and loud: ``job_attempts_archive``'s
foreign key to ``jobs_archive(id) ON DELETE CASCADE`` must drop, because
no table (regular or hypertable) may reference a hypertable. Chunk drops
replace the cascade: both tables convert with retention policies derived
from the same ``archive_retention_period`` setting, so an archived job's
attempts drop on the same clock instead of through the parent's DELETE.
The alignment is by chunk, not by row: an attempt whose ``started_at``
precedes its job's ``finished_at`` can outlive the parent's chunk by up
to one chunk interval.

Retention policies and the sweeps compose
-----------------------------------------

``add_retention_policy`` is registered per table from the existing
settings (``archive_retention_period`` for the two archive tables,
``event_retention_period`` for ``job_events`` when it is not the 0
disable sentinel; a 0 event retention means keep everything, which a
hypertable expresses by having no policy). The row-level sweeps keep
running unchanged: the event TTL sweep and the archive expiry sweep
still delete eligible rows inside young chunks, and the expiry sweep
remains the only mechanism that honors ``expire_at`` exactly (a chunk
drops on the newest ``finished_at`` it contains, so a row can pass its
``expire_at`` while its chunk is still present). No runtime code branches
on whether hypertables are enabled.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

import asyncpg
import structlog

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)

if TYPE_CHECKING:
    from taskq.settings import WorkerSettings

__all__ = [
    "HypertableReport",
    "TimescaleCapability",
    "TimescaleDBUnavailableError",
    "enable_hypertables",
    "probe_timescale_capability",
]

logger = structlog.get_logger("taskq.timescale")

#: The extension's name, checked against ``pg_available_extensions`` /
#: ``pg_extension`` / ``shared_preload_libraries``.
_TIMESCALE_EXTENSION_NAME = "timescaledb"

# Chunk sizing: retention / 4, clamped. A retention window should span a
# handful of chunks (chunk drops replace the row-level sweeps' long-window
# DELETEs, so chunks must be small enough that a drop is bounded work) but
# not so small that chunk catalog overhead dominates. /4 keeps 4-13 chunks
# alive at any instant for windows inside the clamp; the clamp bounds both
# ends (a 1-hour retention still gets a 1-day chunk, a 1-year retention
# does not get thousands of 6-hour chunks).
_MIN_CHUNK_INTERVAL = timedelta(days=1)
_MAX_CHUNK_INTERVAL = timedelta(days=30)


class TimescaleDBUnavailableError(Exception):
    """The ``TASKQ_TIMESCALEDB_HYPERTABLES`` setting is true but the server
    cannot honor it.

    Never a silent degrade: the setup path refuses loudly and names what is
    missing. ``reason`` distinguishes the three failure shapes:

    * the extension is not offered by the server (absent from
      ``pg_available_extensions``),
    * the extension could not be created (the connecting role lacks the
      privilege; pre-create it with an administrative role),
    * the extension is installed but TimescaleDB is absent from
      ``shared_preload_libraries``.

    Unset ``TASKQ_TIMESCALEDB_HYPERTABLES`` (or leave it false) to run on
    this server without hypertables: vanilla Postgres remains fully
    supported, no other behavior changes.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class TimescaleCapability:
    """What the connected server offers, probed at setup."""

    available: bool
    """``timescaledb`` appears in ``pg_available_extensions``."""

    installed: bool
    """``timescaledb`` appears in ``pg_extension``."""

    preloaded: bool
    """``timescaledb`` appears in ``shared_preload_libraries``."""


@dataclass(frozen=True, slots=True)
class HypertableReport:
    """What one :func:`enable_hypertables` run did, for logs and tests."""

    converted: tuple[str, ...]
    """Table names converted to hypertables by this run (already-hypertable
    tables are skipped, so this is empty on a re-run)."""

    retention_policies: tuple[str, ...]
    """``table:interval`` strings for every retention policy this run
    registered (or re-registered after a setting change)."""


async def probe_timescale_capability(conn: asyncpg.Connection) -> TimescaleCapability:
    """Probe the connected server for TimescaleDB support.

    Three independent facts: offered by the server, created in this
    database, and loaded into ``shared_preload_libraries`` (TimescaleDB
    needs the preload; without it ``create_hypertable`` fails at call time
    with a message that names nothing an operator can act on, so the probe
    reports it instead).
    """
    available = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = $1)",
        _TIMESCALE_EXTENSION_NAME,
    )
    installed = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)",
        _TIMESCALE_EXTENSION_NAME,
    )
    preload = await conn.fetchval("SELECT current_setting('shared_preload_libraries', true)")
    preloaded = preload is not None and _TIMESCALE_EXTENSION_NAME in preload.split(",")
    return TimescaleCapability(
        available=bool(available),
        installed=bool(installed),
        preloaded=preloaded,
    )


def _chunk_interval(retention: timedelta) -> timedelta:
    """Retention / 4, clamped to :data:`_MIN_CHUNK_INTERVAL` /
    :data:`_MAX_CHUNK_INTERVAL` (see the sizing rationale above)."""
    quarter = retention / 4
    if quarter < _MIN_CHUNK_INTERVAL:
        return _MIN_CHUNK_INTERVAL
    if quarter > _MAX_CHUNK_INTERVAL:
        return _MAX_CHUNK_INTERVAL
    return quarter


def _add_unique_constraint_sql(schema: str, table: str, name: str, columns: str) -> str:
    """Idempotent ``ADD CONSTRAINT ... UNIQUE``: no transaction wrapper, so
    every statement of the conversion is independently re-runnable, and
    Postgres has no ``ADD CONSTRAINT IF NOT EXISTS``."""
    return (
        # Why: schema is _IDENT_RE-validated in enable_hypertables; the table,
        # constraint and column names are module-owned constants, never input.
        f"DO $$ BEGIN "  # noqa: S608
        f"IF NOT EXISTS (SELECT 1 FROM pg_constraint c "
        f"JOIN pg_class t ON t.oid = c.conrelid "
        f"JOIN pg_namespace n ON n.oid = t.relnamespace "
        f"WHERE c.conname = '{name}' AND n.nspname = '{schema}' AND t.relname = '{table}') "
        f'THEN ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT {name} UNIQUE ({columns}); '
        f"END IF; END $$"
    )


async def enable_hypertables(
    conn: asyncpg.Connection,
    *,
    schema: str,
    settings: WorkerSettings,
) -> HypertableReport:
    """Convert the retention tables to hypertables when the flag is on.

    Called from the ``taskq migrate up`` deploy step (inside the migration
    advisory lock) after pending migrations apply. With
    ``TASKQ_TIMESCALEDB_HYPERTABLES`` false this returns immediately having
    issued ZERO statements: vanilla deployments see no probe, no DDL, no
    extension query, nothing.

    With the flag true the path is loud in both directions: it refuses
    with :class:`TimescaleDBUnavailableError` on a server that cannot
    support the feature, and it converges the schema to the current
    settings when it can (idempotent DDL, re-run on every deploy: chunk
    intervals re-asserted from the current retention settings, retention
    policies re-registered, already-hypertable tables skipped).

    ``settings`` is the :class:`~taskq.settings.WorkerSettings` model
    because the retention intervals the policies derive from
    (``archive_retention_period``, ``event_retention_period``) are
    worker-scoped fields; ``taskq migrate up`` loads the worker model for
    this step so the deploy env and the workers' env cannot disagree.

    :param schema: the TaskQ schema (validated like every identifier).
    :param settings: retention settings are read off this model; the
        ``timescaledb_hypertables`` flag must already be true (callers
        gate on it, and this function asserts it).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    if not settings.timescaledb_hypertables:
        # The flag gate is the ZERO-SQL contract: a vanilla deployment
        # never reaches the probe below.
        return HypertableReport(converted=(), retention_policies=())

    capability = await probe_timescale_capability(conn)
    if not capability.installed:
        if not capability.available:
            raise TimescaleDBUnavailableError(
                "TASKQ_TIMESCALEDB_HYPERTABLES=true requires the TimescaleDB "
                "extension, but this server does not offer it: there is no "
                "'timescaledb' row in pg_available_extensions. On Azure Database "
                "for PostgreSQL Flexible Server, allow the extension (add "
                "'timescaledb' to the azure.extensions server parameter) and "
                "create it with an administrative role before opting in. To run "
                "this server as-is, unset TASKQ_TIMESCALEDB_HYPERTABLES: vanilla "
                "Postgres is fully supported and this flag off is the default."
            )
        try:
            await conn.execute(f"CREATE EXTENSION IF NOT EXISTS {_TIMESCALE_EXTENSION_NAME}")
        except asyncpg.InsufficientPrivilegeError as exc:
            raise TimescaleDBUnavailableError(
                "TASKQ_TIMESCALEDB_HYPERTABLES=true requires the TimescaleDB "
                "extension: the server offers it (pg_available_extensions) but "
                "the connecting role lacks the privilege to create it. Create "
                "the extension once with an administrative role "
                f"('CREATE EXTENSION {_TIMESCALE_EXTENSION_NAME};') and re-run "
                "`taskq migrate up`. To run this server as-is, unset "
                "TASKQ_TIMESCALEDB_HYPERTABLES."
            ) from exc
        capability = await probe_timescale_capability(conn)
        if not capability.installed:
            raise TimescaleDBUnavailableError(
                "TASKQ_TIMESCALEDB_HYPERTABLES=true: CREATE EXTENSION did not "
                "error but 'timescaledb' is still absent from pg_extension; "
                "refusing to guess what happened. Check the server logs."
            )
    if not capability.preloaded:
        raise TimescaleDBUnavailableError(
            "TASKQ_TIMESCALEDB_HYPERTABLES=true: the timescaledb extension is "
            "installed but TimescaleDB is not in shared_preload_libraries, so "
            "hypertable DDL cannot run. Set shared_preload_libraries to include "
            "'timescaledb' in the server parameters (on Azure Database for "
            "PostgreSQL Flexible Server this is set when the extension is "
            "enabled; a restart applies it). To run this server as-is, unset "
            "TASKQ_TIMESCALEDB_HYPERTABLES."
        )

    converted: list[str] = []
    await _convert_job_events(conn, schema, settings, converted)
    await _convert_archive_tables(conn, schema, settings, converted)

    policies = await _register_retention_policies(conn, schema, settings)
    report = HypertableReport(converted=tuple(converted), retention_policies=policies)
    if converted:
        logger.info(
            "hypertables-enabled",
            schema=schema,
            converted=list(converted),
            retention_policies=list(policies),
        )
    return report


async def _to_hypertable(
    conn: asyncpg.Connection,
    schema: str,
    table: str,
    partition_column: str,
    chunk_interval: timedelta,
) -> bool:
    """Convert one table if it is not already a hypertable.

    Returns True when this call did the conversion. The table's existing
    indexes (including partial and GIN) carry over as chunk indexes; the
    caller must already have replaced any unique constraint that omits the
    partition column.
    """
    is_hypertable = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM _timescaledb_catalog.hypertable
            WHERE schema_name = $1 AND table_name = $2
        )
        """,
        schema,
        table,
    )
    if is_hypertable:
        await conn.execute(
            "SELECT set_chunk_time_interval($1::regclass, $2::interval)",
            f'"{schema}"."{table}"',
            chunk_interval,
        )
        return False
    await conn.execute(
        "SELECT create_hypertable($1::regclass, $2::text, "
        "chunk_time_interval => $3::interval, if_not_exists => TRUE, "
        "migrate_data => TRUE)",
        f'"{schema}"."{table}"',
        partition_column,
        chunk_interval,
    )
    return True


async def _convert_job_events(
    conn: asyncpg.Connection,
    schema: str,
    settings: WorkerSettings,
    converted: list[str],
) -> None:
    """``job_events`` on ``occurred_at``: PK widened to include the
    partition column, the ``jobs`` FK kept (hypertable-to-regular-table
    foreign keys are supported)."""
    await conn.execute(
        f'ALTER TABLE "{schema}".job_events DROP CONSTRAINT IF EXISTS job_events_pkey'
    )
    await conn.execute(
        _add_unique_constraint_sql(
            schema, "job_events", "job_events_id_occurred_at_uniq", "id, occurred_at"
        )
    )
    did = await _to_hypertable(
        conn,
        schema,
        "job_events",
        "occurred_at",
        _chunk_interval(settings.event_retention_period),
    )
    if did:
        converted.append("job_events")


async def _convert_archive_tables(
    conn: asyncpg.Connection,
    schema: str,
    settings: WorkerSettings,
    converted: list[str],
) -> None:
    """``jobs_archive`` on ``finished_at`` and ``job_attempts_archive`` on
    ``started_at``.

    The FK from ``job_attempts_archive`` to ``jobs_archive(id)`` drops
    first (no table may reference a hypertable); chunk retention replaces
    the cascade, see the module docstring. ``jobs_archive``'s PK on ``id``
    widens to ``UNIQUE (id, finished_at)``; the re-archive guarantee the
    bare PK enforced is re-implemented as the archive write's explicit
    ``NOT EXISTS`` guard, so the prune's ghost semantics do not change.
    """
    chunk = _chunk_interval(settings.archive_retention_period)
    await conn.execute(
        f'ALTER TABLE "{schema}".job_attempts_archive '
        "DROP CONSTRAINT IF EXISTS job_attempts_archive_job_id_fkey"
    )
    await conn.execute(
        f'ALTER TABLE "{schema}".jobs_archive DROP CONSTRAINT IF EXISTS jobs_archive_pkey'
    )
    await conn.execute(
        _add_unique_constraint_sql(
            schema,
            "jobs_archive",
            "jobs_archive_id_finished_at_uniq",
            "id, finished_at",
        )
    )
    await conn.execute(
        f'ALTER TABLE "{schema}".job_attempts_archive '
        "DROP CONSTRAINT IF EXISTS job_attempts_archive_pkey"
    )
    await conn.execute(
        _add_unique_constraint_sql(
            schema,
            "job_attempts_archive",
            "job_attempts_archive_job_attempt_started_at_uniq",
            "job_id, attempt, started_at",
        )
    )
    if await _to_hypertable(conn, schema, "jobs_archive", "finished_at", chunk):
        converted.append("jobs_archive")
    if await _to_hypertable(conn, schema, "job_attempts_archive", "started_at", chunk):
        converted.append("job_attempts_archive")


async def _register_retention_policies(
    conn: asyncpg.Connection,
    schema: str,
    settings: WorkerSettings,
) -> tuple[str, ...]:
    """Re-register ``add_retention_policy`` per table from the retention
    settings.

    Remove-then-add (not ``if_not_exists`` alone) so a changed
    ``archive_retention_period`` / ``event_retention_period`` is honored on
    the next deploy instead of leaving the first-registered interval in
    force. ``event_retention_period = timedelta(0)`` is that setting's
    disable sentinel: keep every event, expressed as NO policy, so the
    sweep family's zero-means-off polarity carries over.
    """
    registered: list[str] = []
    archive_retention = settings.archive_retention_period
    for table in ("jobs_archive", "job_attempts_archive"):
        await conn.execute(
            "SELECT remove_retention_policy($1::regclass, if_exists => TRUE)",
            f'"{schema}"."{table}"',
        )
        await conn.execute(
            "SELECT add_retention_policy($1::regclass, $2::interval, if_not_exists => TRUE)",
            f'"{schema}"."{table}"',
            archive_retention,
        )
        registered.append(f"{table}:{_format_interval(archive_retention)}")
    if settings.event_retention_period > timedelta(0):
        event_retention = settings.event_retention_period
        await conn.execute(
            "SELECT remove_retention_policy($1::regclass, if_exists => TRUE)",
            f'"{schema}"."job_events"',
        )
        await conn.execute(
            "SELECT add_retention_policy($1::regclass, $2::interval, if_not_exists => TRUE)",
            f'"{schema}"."job_events"',
            event_retention,
        )
        registered.append(f"job_events:{_format_interval(event_retention)}")
    return tuple(registered)


def _format_interval(td: timedelta) -> str:
    """Human form for the report only ('30 days', '7 days'), never SQL."""
    seconds = int(td.total_seconds())
    days, rem = divmod(seconds, 86400)
    if days and not rem:
        return f"{days} days"
    hours, rem = divmod(rem, 3600)
    if hours and not rem:
        return f"{hours} hours"
    return f"{seconds} seconds"
