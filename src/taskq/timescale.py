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

Columnstore (compression) is adopted for the archive tables only
----------------------------------------------------------------

After each archive conversion the deploy step arms the columnstore: the
segmentby/orderby settings are set per table and a compression policy is
registered at one chunk interval (a chunk compresses once it has stopped
receiving rows). ``jobs_archive`` segments by ``(actor, queue)`` ordered
by ``finished_at DESC`` — the admin archive tab's newest-first read, and
low-cardinality filter columns, never the per-row-unique ``id`` (a
unique segmentby key collapses every compressed batch to one row, the
docs' documented anti-pattern); ``job_attempts_archive`` segments by
``job_id`` ordered by ``started_at DESC``. ``job_events`` deliberately
stays rowstore: measured (``benchmarks/results/timescale-compression.json``
via ``benchmarks/timescale_compression.py``), the events table's writes
dominate and its reads are windowed — there was nothing to gain.

One server prerequisite is probed and WARNED about loudly:
``timescaledb.max_tuples_decompressed_per_dml_transaction`` defaults to
100000, and at that default the archive-expiry sweep hard-errors on
compressed chunks with ``ConfigurationLimitExceededError`` (measured:
a single 10k-row expiry batch decompressed 356633 tuples). Raise it
(or set 0 = unlimited) server-wide before relying on the columnstore;
the report and the log name the setting when the probe sees the default.

Disabling: the mirror conversion
--------------------------------

:func:`disable_hypertables` is the inverse operation, for an operator
who wants the vanilla schema back (flip the flag off first — the same
mirror gate enable applies). It removes the registered policies, then
per hypertable copies every row into a byte-exact vanilla table and
swaps: the vanilla shape is never re-typed by hand, it is cloned from a
scratch schema the bundled migrations themselves build (applied fresh
into ``{schema}__vanilla`` and moved table-by-table into place after the
hypertable drops), so the restored pkeys, indexes, foreign keys, and
column defaults are the migrations' own output — byte-equal to a schema
that never converted. The behaviors the conversion traded away come back
with the shape: the bare primary keys reject duplicates, the
``job_attempts_archive → jobs_archive`` foreign key cascades again, and
the event id sequence continues from the restored maximum.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import asyncpg
import structlog

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)
from taskq.migrate import apply_pending

if TYPE_CHECKING:
    from taskq.backend._protocol import ConnLike
    from taskq.settings import WorkerSettings

__all__ = [
    "HypertableReport",
    "TimescaleCapability",
    "TimescaleDBUnavailableError",
    "disable_hypertables",
    "enable_hypertables",
    "probe_timescale_capability",
    "retention_policy_floor",
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

# The decompression budget the extension ships as its default. At this
# budget the archive-expiry sweep's first batch on compressed chunks
# hard-errors (measured on the 2.30.1 image: one 10k-row expiry batch
# decompressed 356633 tuples -> ConfigurationLimitExceededError), so a
# deployment adopting the columnstore must raise the GUC — the probe
# below turns the default into a loud warning, never a silent trap.
_DECOMPRESSION_GUC = "timescaledb.max_tuples_decompressed_per_dml_transaction"
_DEFAULT_DECOMPRESSION_BUDGET = 100_000

# Columnstore adoption per archive table, measured by
# benchmarks/timescale_compression.py (6.18x storage reduction on the
# 400k-row archive corpus). segmentby is deliberately never the
# per-row-unique id: a unique segmentby key makes every compressed batch
# one row and collapses the ratio (the docs' documented anti-pattern).
# orderby serves each table's real newest-first read.
_COMPRESSION_SETTINGS: dict[str, tuple[str, str]] = {
    "jobs_archive": ("actor, queue", "finished_at DESC"),
    "job_attempts_archive": ("job_id", "started_at DESC"),
}


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
    """What one :func:`enable_hypertables` run did, for logs and tests.

    :meth:`disable_hypertables` returns the same dataclass with the
    mirrored reading documented on it.
    """

    converted: tuple[str, ...] = ()
    """Table names converted to hypertables by this run (already-hypertable
    tables are skipped, so this is empty on a re-run)."""

    retention_policies: tuple[str, ...] = ()
    """``table:interval`` strings for every retention policy this run
    registered (or re-registered after a setting change)."""

    compression_policies: tuple[str, ...] = ()
    """``table:interval`` strings for every compression policy this run
    registered (or re-registered; ``compress_after`` is the interval).
    The two archive tables only — ``job_events`` stays rowstore."""

    decompression_guc_warning: str | None = None
    """Loud warning text when the server's
    ``timescaledb.max_tuples_decompressed_per_dml_transaction`` is at or
    under its 100000 default: the archive-expiry sweep hard-errors on
    compressed chunks at that budget (measured
    ``ConfigurationLimitExceededError``). None when the budget is raised
    (or unlimited) or the probe could not read it."""


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
    policies re-registered, already-hypertable tables skipped). A
    re-asserted interval shapes future chunks only: existing chunks keep
    the interval they were created with.

    The two archive tables also adopt the columnstore: per-table
    segmentby/orderby settings plus a compression policy registered at
    one chunk interval (a chunk compresses once it has stopped receiving
    rows). ``job_events`` stays rowstore — measured, nothing to gain.
    A compression policy is remove-then-add registered like its
    retention sibling, so a settings change takes effect on the next
    deploy; a segmentby/orderby CHANGE with compressed chunks already on
    disk fails loudly (decompress first — the policy can only re-shape
    what is still rowstore). Because compression puts chunks in the
    expiry sweep's DML path, this function probes
    ``timescaledb.max_tuples_decompressed_per_dml_transaction`` and —
    when it is at or under its 100000 default — logs a loud WARNING and
    returns it in the report's ``decompression_guc_warning``: at that
    budget the archive-expiry sweep hard-errors on compressed chunks
    (measured ``ConfigurationLimitExceededError``). Raise the GUC (or
    set 0 = unlimited) server-wide before relying on the columnstore.

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
    compression = await _register_compression_policies(conn, schema, settings)
    guc_warning = await _probe_decompression_budget(conn)
    report = HypertableReport(
        converted=tuple(converted),
        retention_policies=policies,
        compression_policies=compression,
        decompression_guc_warning=guc_warning,
    )
    if converted:
        logger.info(
            "hypertables-enabled",
            schema=schema,
            converted=list(converted),
            retention_policies=list(policies),
            compression_policies=list(compression),
        )
    if guc_warning is not None:
        logger.warning("hypertable-decompression-budget-at-default", schema=schema)
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
        # Convergence, not a rewrite: this re-asserts the interval from the
        # current settings and returns. The interval shapes FUTURE chunks
        # only - existing chunks keep the interval they were created with.
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
    foreign keys are supported).

    Lock windows to size a deploy by: the pkey drop takes ACCESS
    EXCLUSIVE (catalog-only, fast), and the unique add below takes ACCESS
    EXCLUSIVE and full-scans the table to validate the uniqueness before
    ``create_hypertable`` runs. On a populated table that scan is part of
    the deploy window, not a footnote to it.
    """
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

    Lock windows and one honest gap, same shape as
    :func:`_convert_job_events`: the FK drop and the two pkey drops take
    ACCESS EXCLUSIVE (catalog-only), and each unique add takes ACCESS
    EXCLUSIVE and full-scans its table to validate - size a deploy window
    for those scans plus the ``migrate_data`` copy together. The gap: the
    statements run one per transaction on the deploy connection's
    autocommit, and the migration advisory lock serializes migrators only
    (workers never take it), so between a pkey drop and its unique add the
    table carries no unique constraint on its id columns while workers
    keep running. A duplicate insert landing in that window makes every
    subsequent deploy fail on the unique add until the duplicates are
    removed by hand; the deploy wants a maintenance window with no worker
    archiving, or an operator who accepts the two-statement window.
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

    Two boundaries worth stating where the registration happens:

    * The FIRST registration arms the mid-life backlog: every chunk older
      than the retention interval is drop-eligible immediately, so the
      first background policy run drops the whole aged tail in one sweep
      (potentially GBs of IO) where vanilla mode would have expired it row
      by row, in bounded batch deletes.
    * On ``job_events`` the policy is a THIRD deleter the fail-visible gap
      signal does not cover: migration 01.00.20_02's watermark contract
      (``job_events_prune_state.pruned_through_id``, advanced by the two
      sweep deleters in the same statement that deletes) is not kept here -
      a chunk drop removes event ids WITHOUT advancing the watermark, so a
      consumer cursor below the dropped ids reads as safe and loses them
      silently. No watermark advance exists on this path; keep
      ``watch_reclaims`` cursors strictly inside ``event_retention_period``
      on hypertables.
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


async def _register_compression_policies(
    conn: asyncpg.Connection,
    schema: str,
    settings: WorkerSettings,
) -> tuple[str, ...]:
    """Arm the columnstore on the two archive tables; ``job_events``
    deliberately stays rowstore (measured: nothing to gain — its writes
    dominate and its reads are windowed; see the module docstring).

    Per table, mirroring :func:`_register_retention_policies`'s
    remove-then-add convergence: the old policy is removed first (either
    removal API the server offers — the new-style columnstore name is a
    procedure on some versions, the legacy compression name a function —
    so both are probed and at least one lands), then the segmentby/
    orderby settings are set (new-style columnstore options, legacy
    compress aliases as the fallback — the house probe pattern; the
    2.30.1 image accepts the new settings and errors on the new policy
    function), then the compression policy is registered at one chunk
    interval — a chunk compresses once it has stopped receiving rows.
    Remove-then-add means a changed ``archive_retention_period`` moves
    the compress_after on the next deploy. A segmentby/orderby CHANGE
    with compressed chunks already on disk is ACCEPTED by the server —
    with a NOTICE ("updated compression settings will only apply to
    future compressions; existing compressed chunks will not be
    recompressed", measured on 2.30.1), not an error: the new shape is
    the future chunks' shape and the compressed ones keep their old
    segmentation until decompressed and recompressed by hand. The
    deploy-side convergence is therefore silent by design; the docs (and
    the compression_settings view) name the gap.
    """
    compress_after = _chunk_interval(settings.archive_retention_period)
    registered: list[str] = []
    for table, (segmentby, orderby) in _COMPRESSION_SETTINGS.items():
        target = f'"{schema}"."{table}"'
        # Remove-then-add (the retention discipline): either removal API
        # landing is enough; a failed removal surfaces loudly at the add.
        for remover in (
            "CALL remove_columnstore_policy($1::regclass, if_exists => TRUE)",
            "SELECT remove_compression_policy($1::regclass, if_exists => TRUE)",
        ):
            try:
                await conn.execute(remover, target)
                break
            except Exception:  # noqa: S112  # Why: the house probe pattern across extension versions; a real failure resurfaces at the add below.
                continue
        # The settings probe mirrors benchmarks/timescale_compression.py's
        # measured setup: columnstore-style options first, legacy
        # compress aliases as the fallback. The option VALUES are
        # module-owned constants (never input), like every identifier here.
        columnstore = (
            "timescaledb.enable_columnstore = true, "
            f"timescaledb.segmentby = '{segmentby}', "
            f"timescaledb.orderby = '{orderby}'"
        )
        legacy = (
            "timescaledb.compress, "
            f"timescaledb.compress_segmentby = '{segmentby}', "
            f"timescaledb.compress_orderby = '{orderby}'"
        )
        last_error: Exception | None = None
        for style in (columnstore, legacy):
            try:
                await conn.execute(f"ALTER TABLE {target} SET ({style})")
                last_error = None
                break
            except (
                Exception
            ) as exc:  # Why: probe across extension versions; both failing raises below.
                last_error = exc
        if last_error is not None:
            raise RuntimeError(
                f"could not set columnstore settings on {schema}.{table}: "
                "neither the columnstore nor the legacy compression options "
                "were accepted by this server"
            ) from last_error
        # The policy: try the new name first, fall back to the legacy one
        # (the 2.30.1 image errors on the new name — measured — and the
        # try/fallback mirrors the house probe pattern).
        for adder in (
            "SELECT add_columnstore_policy($1::regclass, $2::interval)",
            "SELECT add_compression_policy($1::regclass, $2::interval)",
        ):
            try:
                await conn.execute(adder, target, compress_after)
                break
            except (
                Exception
            ) as exc:  # Why: probe across extension versions; both failing raises below.
                last_error = exc
        else:
            raise RuntimeError(
                f"could not register the compression policy on "
                f"{schema}.{table}: neither add_columnstore_policy nor "
                "add_compression_policy was accepted by this server"
            ) from last_error
        registered.append(f"{table}:{_format_interval(compress_after)}")
    return tuple(registered)


async def _probe_decompression_budget(conn: asyncpg.Connection) -> str | None:
    """The loud GUC-prerequisite warning, or None when the server is fine.

    Reads ``timescaledb.max_tuples_decompressed_per_dml_transaction`` off
    the connected server; at or under the 100000 default the archive-
    expiry sweep hard-errors on compressed chunks (measured
    ``ConfigurationLimitExceededError`` — one 10k-row batch decompressed
    356633 tuples), so the default is never allowed to pass silently:
    the warning names the setting, the number, and the fix, and rides
    both the report and the log. A probe failure (the GUC missing from
    an unexpected server, a permission gap) fails OPEN to None — the
    sweep's own hard error, if it comes, is the loud backstop.
    """
    try:
        raw = await conn.fetchval("SELECT current_setting($1, true)", _DECOMPRESSION_GUC)
    except Exception as exc:  # Why: fail-open — the expiry sweep's own hard error is the backstop, and no probe failure may break a deploy.
        logger.debug("decompression-budget-probe-failed", error=repr(exc))
        return None
    if raw is None:
        return None
    try:
        budget = int(raw)
    except (TypeError, ValueError):
        return None
    if budget == 0 or budget > _DEFAULT_DECOMPRESSION_BUDGET:
        # 0 is the unlimited setting; anything above the default clears it.
        return None
    warning = (
        f"timescaledb.max_tuples_decompressed_per_dml_transaction is {raw} (at or under "
        f"the {_DEFAULT_DECOMPRESSION_BUDGET} default): the archive-expiry sweep will "
        "hard-error on compressed chunks with ConfigurationLimitExceededError once the "
        "columnstore policy ages chunks in. Raise the budget server-wide before "
        "relying on compression, e.g. ALTER SYSTEM SET "
        "timescaledb.max_tuples_decompressed_per_dml_transaction = '0' (unlimited) and "
        "reload, or set it on the workers' sessions; see docs/guides/timescaledb.md."
    )
    logger.warning("hypertable-decompression-budget-at-default", budget=raw)
    return warning


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


# One round trip answering both probe questions: is *schema.table* a
# hypertable with a registered retention policy (the information views'
# join), and if so what is the policy's own horizon.  The policy's
# ``drop_after`` is read out of the registered job's ``config`` — the
# EXACT interval the policy drops on, not a re-derivation from TaskQ's
# settings, so a policy registered by any release with any interval is
# honored as registered.  ``timescaledb_information.dimensions`` pins the
# hypertable's time column to the caller's *partition_col*: the floor's
# clock IS the partition column's, and a caller passing a column the
# hypertable is not partitioned on gets None (fail-open) rather than a
# floor drawn on the wrong clock.  Every value is $-bound — nothing here
# is interpolated.  On vanilla Postgres the views do not exist and the
# statement raises UndefinedTable: caught below, the fail-open.
_RETENTION_POLICY_FLOOR_PROBE_SQL = """\
SELECT MIN(((j.config)::text::jsonb ->> 'drop_after')::interval) AS drop_after,
       MIN(statement_timestamp()) AS db_now
FROM timescaledb_information.hypertables h
JOIN timescaledb_information.jobs j
  ON j.hypertable_schema = h.hypertable_schema
 AND j.hypertable_name = h.hypertable_name
WHERE h.hypertable_schema = $1
  AND h.hypertable_name = $2
  AND j.proc_name = 'policy_retention'
  AND EXISTS (
      SELECT 1 FROM timescaledb_information.dimensions d
      WHERE d.hypertable_schema = $1
        AND d.hypertable_name = $2
        AND d.column_name = $3
        AND d.dimension_type = 'Time'
  )
-- Aggregates (MIN), not LIMIT 1 without ORDER BY: the single-row answer is
-- deterministic no matter the catalog's physical order, and the no-policy
-- case still returns exactly one row of NULLs (the caller's None)."""


async def retention_policy_floor(
    conn: ConnLike,
    schema: str,
    table: str,
    partition_col: str,
    now: datetime | None = None,
) -> datetime | None:
    """The armed retention policy's own horizon for one hypertable, or
    None when nothing owns the aged end.

    Answers ONE catalog question per sweep run (never per batch): is
    *schema.table* a hypertable with a ``policy_retention`` job registered
    against it, and if so, when does that policy's ownership of the aged
    end begin?  The answer is ``now - drop_after``, where *drop_after* is
    parsed out of the registered policy's own config
    (``timescaledb_information.jobs.config``) — the policy's EXACT
    horizon, not a re-derivation from TaskQ's settings, so the floor and
    the policy can never disagree about where the boundary sits.

    *partition_col* is the table's partition column (``occurred_at`` for
    ``job_events``, ``finished_at`` for ``jobs_archive``): the policy
    drops chunks on that column's age, and the probe verifies through
    ``timescaledb_information.dimensions`` that the hypertable really is
    partitioned on it — a mismatch returns None (the caller's floor
    semantics would be drawn on the wrong clock).

    Returns None — and the caller's sweep then runs full-range, byte-
    identical to vanilla behavior — when ANY of:

    * the table is not a hypertable (no row in
      ``timescaledb_information.hypertables``),
    * the hypertable carries no ``policy_retention`` job (nothing owns
      the aged end; the sweep must not skip a single row),
    * the hypertable is partitioned on a column other than
      *partition_col*,
    * the probe fails for ANY reason — vanilla Postgres above all (the
      ``timescaledb_information`` views do not exist there and the
      statement raises on first execution), but also permission gaps,
      extension upgrades, config shapes the cast cannot parse.

    A probe failure must never break a sweep: the failure is logged at
    debug and the sweep keeps today's exact behavior.  *now* overrides
    the clock the floor is anchored to (test seam); by default the probe
    query's own ``statement_timestamp()`` (read in the same round trip)
    anchors it to the database's clock domain, the domain every retention
    predicate in the sweeps runs in.

    The floor is a BOUNDARY, not a deletion order: rows NEWER than ``now -
    drop_after`` (inside the window) the calling sweep keeps deleting row-
    exactly; rows OLDER than it the registered policy owns — silently, at
    chunk granularity, without advancing any watermark (see
    ``tests/test_timescale_retention_interplay.py``'s pinned boundary).
    A row older than the floor but living in a young chunk is dropped by
    the policy when the chunk itself ages past the boundary, at most one
    chunk interval after the row crosses the floor — chunk granularity,
    not row precision, is the trade.
    """
    try:
        rows = await conn.fetch(_RETENTION_POLICY_FLOOR_PROBE_SQL, schema, table, partition_col)
    except Exception as exc:  # Why: ANY probe failure must fail open to None — on vanilla Postgres this is the UndefinedTable the views' absence raises, and no probe error may ever break a sweep.
        logger.debug(
            "retention_policy_floor_probe_failed",
            schema=schema,
            table=table,
            error=repr(exc),
        )
        return None
    if not rows or rows[0]["drop_after"] is None:
        return None
    drop_after: timedelta = rows[0]["drop_after"]
    floor_now: datetime = now if now is not None else rows[0]["db_now"]
    return floor_now - drop_after


def _vanilla_staging_schema(schema: str) -> str:
    """The scratch schema the vanilla clone is built in.

    Derived from the TaskQ schema's own name (never user input beyond it)
    and guarded loudly: Postgres truncates identifiers at 63 bytes, and a
    silently truncated staging name would not match the references built
    from it.
    """
    staging = f"{schema}__vanilla"
    if len(staging) > 63 or not _IDENT_RE.match(staging):
        raise ValueError(
            f"cannot derive the vanilla staging schema name from {schema!r}: "
            f"{staging!r} is not a legal untruncated identifier"
        )
    return staging


async def _is_hypertable(conn: asyncpg.Connection, schema: str, table: str) -> bool:
    """The same catalog probe :func:`_to_hypertable` gates on."""
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM _timescaledb_catalog.hypertable
                WHERE schema_name = $1 AND table_name = $2
            )
            """,
            schema,
            table,
        )
    )


def _add_foreign_key_sql(
    schema: str, table: str, name: str, column: str, ref_table: str, ref_column: str
) -> str:
    """Idempotent ``ADD CONSTRAINT ... FOREIGN KEY ... ON DELETE CASCADE``,
    the exact shape the initial migration gives the vanilla tables (the
    constraint name is the ``{table}_{column}_fkey`` a fresh migration
    mints), scoped to schema+table so another schema's same-named
    constraint cannot make the guard skip. No transaction wrapper, like
    :func:`_add_unique_constraint_sql`."""
    return (
        f"DO $$ BEGIN "  # noqa: S608
        f"IF NOT EXISTS (SELECT 1 FROM pg_constraint c "
        f"JOIN pg_class t ON t.oid = c.conrelid "
        f"JOIN pg_namespace n ON n.oid = t.relnamespace "
        f"WHERE c.conname = '{name}' AND n.nspname = '{schema}' AND t.relname = '{table}') "
        f'THEN ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT {name} '
        f'FOREIGN KEY ({column}) REFERENCES "{schema}"."{ref_table}" ({ref_column}) '
        f"ON DELETE CASCADE; "
        f"END IF; END $$"
    )


async def _remove_registered_policies(
    conn: asyncpg.Connection,
    schema: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Remove every registered policy (retention on all three tables,
    compression on the two archives), idempotently, BEFORE any table
    surgery; a policy left behind would dangle or re-fire mid-swap.

    Returns ``(retention, compression)`` report entries for the policies
    that WERE registered, read from the jobs' own configs before removal
    (the floor discipline: the registered interval, not a re-derivation).
    The pre-read fails open — a report degradation, never a blocked
    removal — but the removals and the convergence check below are loud:
    nothing of either kind may survive this function.
    """
    try:
        jobs = await conn.fetch(
            """
            SELECT hypertable_name, proc_name,
                   COALESCE((config)::text::jsonb ->> 'drop_after',
                            (config)::text::jsonb ->> 'compress_after') AS horizon
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = $1
              AND proc_name IN ('policy_retention', 'policy_compression', 'policy_columnstore')
            """,
            schema,
        )
    except Exception as exc:  # Why: fail-open on the REPORT read only — vanilla Postgres and permission gaps degrade the report, never block the removal.
        logger.debug("disable_policy_read_failed", schema=schema, error=repr(exc))
        jobs = []
    # Removal is hypertable-scoped: ``if_exists`` guards a missing POLICY,
    # not a plain TABLE (a second disable run finds plain tables here and
    # must skip them — remove_retention_policy on one errors loudly). The
    # trash name is checked too: a crash between the rename-first swap's
    # trash rename and its drop leaves the retired hypertable registered
    # under ``{table}__hypertable_trash``, and a policy that outlived the
    # crashed run's removal must not survive into the converging re-run.
    for table in ("jobs_archive", "job_attempts_archive", "job_events"):
        for name in (table, f"{table}{_TRASH_SUFFIX}"):
            if not await _is_hypertable(conn, schema, name):
                continue
            await conn.execute(
                "SELECT remove_retention_policy($1::regclass, if_exists => TRUE)",
                f'"{schema}"."{name}"',
            )
    for table in ("jobs_archive", "job_attempts_archive"):
        for name in (table, f"{table}{_TRASH_SUFFIX}"):
            if not await _is_hypertable(conn, schema, name):
                continue
            target = f'"{schema}"."{name}"'
            # Either removal API landing is enough: the new-style columnstore
            # name is a procedure on some versions, the legacy name a function
            # (probed, the house pattern).
            for remover in (
                "CALL remove_columnstore_policy($1::regclass, if_exists => TRUE)",
                "SELECT remove_compression_policy($1::regclass, if_exists => TRUE)",
            ):
                try:
                    await conn.execute(remover, target)
                    break
                except Exception:  # noqa: S112  # Why: probe across extension versions; a surviving policy fails the loud check below.
                    continue
    remaining = await conn.fetch(
        """
        SELECT hypertable_name, proc_name FROM timescaledb_information.jobs
        WHERE hypertable_schema = $1
          AND proc_name IN ('policy_retention', 'policy_compression', 'policy_columnstore')
        """,
        schema,
    )
    if remaining:
        raise RuntimeError(
            f"could not remove the registered policies for schema {schema!r}: "
            f"{[(r['proc_name'], r['hypertable_name']) for r in remaining]} survived "
            "the removal APIs; refusing to swap tables under a live policy"
        )
    retention = tuple(
        f"{r['hypertable_name']}:{r['horizon']}"
        for r in jobs
        if r["proc_name"] == "policy_retention"
    )
    compression = tuple(
        sorted(
            {
                r["hypertable_name"]
                for r in jobs
                if r["proc_name"] in ("policy_compression", "policy_columnstore")
            }
        )
    )
    return retention, compression


# The per-table vanilla behavior restores that run UNCONDITIONALLY and
# idempotently after (or without) a swap, so a disable that crashed
# mid-swap converges on the re-run. Each guards its own work: the enum
# retype only when the column still points at the staging schema's type,
# the sequence re-anchor only to GREATEST(existing state, max(id)) —
# never backward past a production sequence's own position.

#: The trash name the rename-first swap gives the retired hypertable
#: (``ALTER TABLE ... RENAME TO``): metadata-only and instant, and the
#: moment the rows exist in TWO places. The trash survives — twin-verified
#: — until the swap's last statement, so no crash order strands rows in a
#: table whose only copy is about to be destroyed.
_TRASH_SUFFIX = "__hypertable_trash"

#: The per-table natural key deciding whether an orphan copy's rows are
#: all accounted for in the live table (aliased ``o`` = orphan, ``l`` =
#: live). A full twin match is the ONLY license the disable path has to
#: drop a table holding rows.
_TWIN_MATCH: dict[str, str] = {
    "jobs_archive": "l.id = o.id",
    "job_events": "l.id = o.id",
    "job_attempts_archive": "l.job_id = o.job_id AND l.attempt = o.attempt",
}

#: The vanilla primary key the restored table will enforce, whose columns
#: the twin guard keys on. The hypertable's WIDENED uniqueness admits rows
#: vanilla cannot hold (same id, different partition-column value), so the
#: swap refuses loudly — before any name moves — when the source carries
#: duplicates the restored table could never accept.
_VANILLA_KEY: dict[str, tuple[str, ...]] = {
    "jobs_archive": ("id",),
    "job_events": ("id",),
    "job_attempts_archive": ("job_id", "attempt"),
}

#: The staging foreign keys that travel with a table moved out of (or into)
#: the staging schema. They reference the staging schema by name and must
#: drop before the staging schema itself can drop cleanly; the real ones
#: come back against this schema's tables in the behavior restore.
_TRAVELED_FOREIGN_KEYS: dict[str, tuple[str, ...]] = {
    "job_events": ("job_events_job_id_fkey",),
    "job_attempts_archive": ("job_attempts_archive_job_id_fkey",),
}


async def _drop_traveled_foreign_keys(conn: asyncpg.Connection, schema: str, table: str) -> None:
    """Drop the staging foreign keys a table carried across a schema move
    (idempotent; ``jobs_archive`` references nothing and has none)."""
    for name in _TRAVELED_FOREIGN_KEYS.get(table, ()):
        await conn.execute(f'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT IF EXISTS {name}')


async def _table_exists(conn: asyncpg.Connection, schema: str, table: str) -> bool:
    """The qualified relation resolves in the catalogs (tables only in
    practice: every caller passes module-owned names)."""
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{table}"'))


async def _restore_vanilla_behaviors(conn: asyncpg.Connection, schema: str) -> None:
    """The pieces the swap's moved-in table cannot carry by itself: the
    ``status`` enum re-pointed at THIS schema's ``job_status`` (the moved
    table arrives typing it with the staging schema's twin), the event id
    sequence re-anchored past the restored maximum (the moved-in sequence
    is the staging one — fresh, never incremented), and the two foreign
    keys (LIKE/move machinery and the drop both leave them off; nothing
    may reference the staging schema that is about to be dropped)."""
    # jobs_archive.status: the moved table's type must be THIS schema's
    # enum (identical labels, created from the same migration), or the
    # staging drop would take the column's type with it.
    await _retype_archive_status_if_needed(conn, schema)
    # job_events.id: re-anchor past the restored maximum — forward-only,
    # so a healthy production sequence never regresses. One honest limit
    # (pinned in _anchor_event_id_sequence): on an EMPTY restored table
    # the anchor re-issues the anchor value itself, and a production
    # sequence can sit past the restored max(id) — ids below it that
    # retention already deleted. The next issued id can reuse one
    # ``pruned_through_id`` already claims gone: a caught-up
    # ``watch_reclaims`` consumer (cursor at or above the watermark) will
    # not see that one event; a consumer below the watermark fails
    # visibly as designed. One id, once, at the boundary — the price of
    # anchoring forward-only against a table that cannot say which ids
    # below the sequence's own position were already issued and reaped.
    await _anchor_event_id_sequence(conn, schema, floor_table="job_events")
    # The two foreign keys, byte-exact vanilla names, idempotent DO
    # blocks (the constraint-name guards are scoped to this schema and
    # table — another schema's same-named constraint must not skip it).
    await conn.execute(
        _add_foreign_key_sql(schema, "job_events", "job_events_job_id_fkey", "job_id", "jobs", "id")
    )
    await conn.execute(
        _add_foreign_key_sql(
            schema,
            "job_attempts_archive",
            "job_attempts_archive_job_id_fkey",
            "job_id",
            "jobs_archive",
            "id",
        )
    )


async def _retype_archive_status_if_needed(conn: asyncpg.Connection, schema: str) -> None:
    """``jobs_archive.status`` re-pointed at THIS schema's ``job_status`` —
    idempotent (a no-op when the column already types it, or the table is
    absent).

    This must run BEFORE any ``DROP SCHEMA "{staging}" CASCADE`` that a
    disable's crash convergence precedes: a crash between the moved-in
    table's arrival and its retype leaves the live ``jobs_archive`` typing
    its ``status`` column with the STAGING schema's enum, and the type
    dependency lets a staging-schema CASCADE drop the live table — with
    any rows on it — from under a healthy fleet.
    """
    udt_schema = await conn.fetchval(
        """
        SELECT udt_schema FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs_archive' AND column_name = 'status'
        """,
        schema,
    )
    if udt_schema is not None and udt_schema != schema:
        await conn.execute(
            f'ALTER TABLE "{schema}".jobs_archive '
            f'ALTER COLUMN status TYPE "{schema}".job_status '
            f'USING status::text::"{schema}".job_status'
        )


async def _anchor_event_id_sequence(
    conn: asyncpg.Connection, schema: str, *, floor_table: str
) -> None:
    """Re-anchor ``job_events``' id sequence past the rows in *floor_table*.

    GREATEST of the sequence's own state and the floor table's ``max(id)`` —
    forward-only, never backward past a production sequence's own position —
    with the anchor's ``is_called`` flag derived from the floor table's
    emptiness. The swap calls it BEFORE the rows return (the moved-in table's
    sequence is the staging twin's — fresh, starting at 1, exactly where the
    trash's ids start: a worker's insert in that window would collide) and
    the behavior restore calls it again after (idempotent, forward-only).

    One honest limit, pinned here and in the docs: on an EMPTY floor table
    the re-anchor re-issues the anchor value itself (``is_called`` false),
    and a production sequence can sit past the restored ``max(id)`` — ids
    below it that retention already deleted. The next issued id can
    therefore reuse one ``job_events_prune_state.pruned_through_id`` already
    claims gone; a caught-up ``watch_reclaims`` consumer (cursor at or above
    the watermark) will not see that one event. See
    :func:`_restore_vanilla_behaviors` for the full statement of the trade.
    """
    if not await conn.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", f'"{schema}".job_events_id_seq'
    ):
        return
    await conn.execute(
        # Why noqa S608: schema is _IDENT_RE-validated in disable_hypertables;
        # the sequence, floor-table and column names are module-owned
        # constants, never input.
        f"""SELECT setval(
            '{schema}.job_events_id_seq',
            GREATEST(
                (SELECT COALESCE(last_value, 1) FROM pg_sequences
                 WHERE schemaname = '{schema}' AND sequencename = 'job_events_id_seq'),
                (SELECT COALESCE(max(id), 1) FROM "{schema}"."{floor_table}")
            ),
            EXISTS (SELECT 1 FROM "{schema}"."{floor_table}")
        )"""  # noqa: S608
    )


async def _refuse_vanilla_key_duplicates(
    conn: asyncpg.Connection, schema: str, table: str, relation: str
) -> None:
    """Loud refusal when *relation* holds rows whose vanilla key
    (:data:`_VANILLA_KEY`) collides — rows the hypertable's widened
    uniqueness admitted and the restored table's primary key cannot.

    Runs BEFORE the trash rename (before any name moves): the widened
    constraint is the disable's one unrecoverable mismatch — no ordering
    fixes it, only choosing which of the colliding rows to keep, and that
    choice is the operator's, never a silent dedupe.
    """
    key = ", ".join(_VANILLA_KEY[table])
    dupes = await conn.fetchval(
        f'SELECT count(*) FROM (SELECT {key} FROM "{schema}"."{relation}" '  # noqa: S608
        f"GROUP BY {key} HAVING count(*) > 1) d"
    )
    if dupes:
        raise RuntimeError(
            f"{relation} holds {dupes} vanilla-key collision(s) the hypertable's "
            f"widened uniqueness admitted (same {key}, different partition-column "
            f"value) and the restored vanilla table's primary key cannot hold; "
            f"resolve the duplicate {table} rows (keep the ones you want) before "
            f"disabling — the swap refuses to choose for you"
        )


async def _absorb_orphan_rows(
    conn: asyncpg.Connection, schema: str, table: str, orphan: str
) -> None:
    """Move every *orphan*-table row the live ``{schema}.{table}`` is
    missing into it, verify EVERY orphan row then has a live twin (the
    per-table natural key — :data:`_TWIN_MATCH`), and only then drop the
    orphan.

    The disable path's ONE drop rule: never destroy a table containing
    rows whose live twin is missing. The insert is twin-guarded, so a
    re-run converges instead of duplicating; the post-insert re-check is
    the proof that turns the drop into a no-loss statement.
    """
    # The live table may have moved in moments ago still typing jobs_archive's
    # status with the staging schema's enum; the orphan's rows carry THIS
    # schema's type. The retype is idempotent and must precede the insert.
    if table == "jobs_archive":
        await _retype_archive_status_if_needed(conn, schema)
    if not await _is_hypertable(conn, schema, table):
        # A plain live table cannot hold what the widened uniqueness did:
        # internal key collisions in the orphan are the operator's choice,
        # never a silent twin-skip dedupe.
        await _refuse_vanilla_key_duplicates(conn, schema, table, orphan)
    twin = _TWIN_MATCH[table]
    missing_sql = (
        f'SELECT count(*) FROM "{schema}"."{orphan}" o '  # noqa: S608
        f'WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{table}" l WHERE {twin})'
    )
    if await conn.fetchval(missing_sql):
        await conn.execute(
            f'INSERT INTO "{schema}"."{table}" '  # noqa: S608
            f'SELECT o.* FROM "{schema}"."{orphan}" o '
            f'WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{table}" l WHERE {twin})'
        )
    still_missing = await conn.fetchval(missing_sql)
    if still_missing:
        raise RuntimeError(
            f'crash recovery is short: {still_missing} row(s) of "{schema}"."{orphan}" '
            f'have no twin in "{schema}"."{table}"; refusing to drop the orphan copy'
        )
    await conn.execute(f'DROP TABLE "{schema}"."{orphan}"')


async def _converge_crashed_swaps(
    conn: asyncpg.Connection, schema: str, staging: str
) -> tuple[str, ...]:
    """Complete any earlier run's crashed per-table swap — BEFORE the
    staging schema is dropped and rebuilt, and before any ``DROP SCHEMA
    CASCADE`` can touch a dependency a crash left dangling.

    The crash states, per table (live = ``{schema}.{table}``):

    * **live is the hypertable** (crash during/after the verified copy,
      before the trash rename): the swap below re-copies from scratch; a
      leftover copy is absorbed twin-first.
    * **live missing, trash present** (crash between the trash rename and
      the move-in): the trash renames BACK — the rename is metadata-only,
      nothing was lost — and the normal swap below redoes the move.
    * **live present and plain, trash or restore heap present** (crash
      between the move-in and the trash drop): the stranded rows are
      absorbed twin-first; a live table already holding every row just
      gets its orphan copies verified away and dropped.
    * **live missing, no trash, restore heap present** (a crash of the
      pre-rename-first ordering's DROP-before-SET window): the staging
      table moves in and the heap is absorbed twin-first; with no staging
      table left, the heap IS the recovery — it renames into place (rows
      preserved; the shape is degraded, logged loudly).

    Every branch is count/twin-verified; nothing holding a row whose live
    twin is missing is ever dropped. Returns the tables a stranded state
    was found (and finished) for.
    """
    # First, always: sever the staging enum dependency a crash between the
    # move-in and the retype leaves on the live jobs_archive (the reason
    # this runs before the CASCADE, not after).
    await _retype_archive_status_if_needed(conn, schema)
    converged: list[str] = []
    for table in ("jobs_archive", "job_attempts_archive", "job_events"):
        trash = f"{table}{_TRASH_SUFFIX}"
        heap = f"{table}__restore"
        if await _table_exists(conn, schema, table):
            if await _is_hypertable(conn, schema, table):
                for orphan in (trash, heap):
                    if await _table_exists(conn, schema, orphan):
                        await _absorb_orphan_rows(conn, schema, table, orphan)
                continue
            absorbed = False
            for orphan in (trash, heap):
                if await _table_exists(conn, schema, orphan):
                    await _absorb_orphan_rows(conn, schema, table, orphan)
                    absorbed = True
            if absorbed:
                converged.append(table)
            continue
        # The live table is missing: a crashed swap holds the rows.
        if await _table_exists(conn, schema, trash):
            # Crash between the trash rename and the move-in: the rename
            # is metadata-only, so renaming back restores the exact
            # pre-rename state; the normal swap below redoes the move.
            await conn.execute(f'ALTER TABLE "{schema}"."{trash}" RENAME TO "{table}"')
            converged.append(table)
            continue
        if await _table_exists(conn, schema, heap):
            # No trash: the pre-rename-first ordering died between its DROP
            # and its SET SCHEMA. The vanilla shape is still in the staging
            # schema when the crash left it there — move it in, drop the
            # staging foreign keys that traveled nowhere yet, absorb the
            # heap. Without a staging table the heap IS the recovery: it
            # renames into place (rows preserved, shape degraded loudly).
            if await _table_exists(conn, schema, f"{staging}.{table}"):
                await conn.execute(f'ALTER TABLE "{staging}"."{table}" SET SCHEMA "{schema}"')
                await _drop_traveled_foreign_keys(conn, schema, table)
                await _absorb_orphan_rows(conn, schema, table, heap)
            else:
                await conn.execute(f'ALTER TABLE "{schema}"."{heap}" RENAME TO "{table}"')
                logger.warning(
                    "hypertable-disable-recovery-degraded",
                    schema=schema,
                    table=table,
                    detail=(
                        "no staging table survived the crash; the restore heap moved into "
                        "place as-is. Every row is preserved but the shape is the hypertable's "
                        "copy, not the migrations': re-run the disable after rebuilding "
                        "the vanilla shape by hand if byte-exact shape matters."
                    ),
                )
            converged.append(table)
    return tuple(converged)


async def _free_vanilla_names(conn: asyncpg.Connection, schema: str, trash: str) -> None:
    """Strip the trash's constraints and standalone indexes, and rename its
    owned sequence aside.

    The trash rename moves the hypertable's rows but leaves its pg_class
    names in place — and indexes and sequences share one namespace per
    schema, so the migration-built table cannot move in while the trash
    still holds the vanilla names (constraint names do not collide: they
    are per-relation). The trash is the retired copy — its ROWS are what
    the swap still needs, never its shape — so its constraints and indexes
    strip (a plain DROP, which hypertables propagate to their chunks
    safely; RENAMING them does not: a chunk's derived constraint name
    truncates into collision at 63 bytes), the owned sequence renames
    aside, and the vanilla names return with the table that owns them for
    real. Not-null constraints stay: they are per-relation, and the strip
    has no need to touch them.
    """
    constraints = await conn.fetch(
        """
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2 AND con.contype <> 'n'
        """,
        schema,
        trash,
    )
    for row in constraints:
        await conn.execute(
            # Why noqa S608: schema is _IDENT_RE-validated; the trash name is
            # module-owned and the constraint names are read from the
            # catalogs of the swap this module itself built, never input.
            f'ALTER TABLE "{schema}"."{trash}" DROP CONSTRAINT IF EXISTS "{row["conname"]}"'
        )
    indexes = await conn.fetch(
        """
        SELECT i.relname AS indexname
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2
          AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid)
        """,
        schema,
        trash,
    )
    for row in indexes:
        await conn.execute(f'DROP INDEX IF EXISTS "{schema}"."{row["indexname"]}"')
    sequences = await conn.fetch(
        """
        SELECT s.relname AS seqname
        FROM pg_class s
        JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass
        JOIN pg_class t ON t.oid = d.refobjid AND d.refclassid = 'pg_class'::regclass
        JOIN pg_namespace n ON n.oid = s.relnamespace
        WHERE d.deptype = 'a' AND s.relkind = 'S'
          AND n.nspname = $1 AND t.relname = $2
        """,
        schema,
        trash,
    )
    for row in sequences:
        await conn.execute(
            f'ALTER SEQUENCE "{schema}"."{row["seqname"]}" '
            f'RENAME TO "{row["seqname"]}{_TRASH_SUFFIX}"'
        )


async def _restore_vanilla_table(
    conn: asyncpg.Connection,
    schema: str,
    staging: str,
    table: str,
    post_move: tuple[str, ...] = (),
) -> bool:
    """Swap one hypertable back to its vanilla self. True when swapped.

    RENAME-FIRST, so no order of death loses rows. The vanilla shape is
    never re-typed: the staging schema holds a fresh application of the
    bundled migrations, and the swap moves the migrations' OWN table into
    place (``SET SCHEMA`` carries its indexes, constraints, defaults, and
    owned sequence across). The stages:

    1. The rows are copied into a bare restore heap and the copy is
       count-verified — this exercises the FULL read path (compressed
       chunks decompress) before any name moves; a copy that cannot be
       read back is a loud refusal while the hypertable is still intact.
    2. The hypertable RENAMES to ``{table}__hypertable_trash`` —
       metadata-only, instant. From this moment the rows exist in TWO
       places (trash and heap) and the vanilla name is free; no crash
       order can strand the table missing anymore.
    3. The migration-built table moves into the freed name, *post_move*
       runs, and the event id sequence re-anchors past the trash's
       ``max(id)`` BEFORE the rows return (the moved-in sequence is the
       staging twin's — fresh, starting at 1, exactly where the trash's
       ids start; a worker's insert in the window would collide).
    4. The rows return FROM THE TRASH — the superset: a row committed
       between the count-verify and the rename landed in the trash, and
       the twin-guarded insert catches it too — twin-verified against the
       trash AND the heap (every row must have a live twin), and only
       then are both copies dropped. A crash anywhere in stages 2-4
       leaves every row in at least two places, and the next disable's
       crash convergence (:func:`_converge_crashed_swaps`) finishes the
       move with the same guarantees.
    """
    if not await _is_hypertable(conn, schema, table):
        return False
    trash = f"{table}{_TRASH_SUFFIX}"
    restore_heap = f"{table}__restore"
    source_count = await conn.fetchval(
        # Why noqa S608: schema is _IDENT_RE-validated; the table and copy
        # names are module-owned constants, never input.
        f'SELECT count(*) FROM "{schema}"."{table}"'  # noqa: S608
    )
    await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{restore_heap}"')
    await conn.execute(
        # Why noqa S608: module-owned identifiers only, as above.
        f'CREATE TABLE "{schema}"."{restore_heap}" AS SELECT * FROM "{schema}"."{table}"'  # noqa: S608
    )
    staged_count = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}"."{restore_heap}"'  # noqa: S608
    )
    if staged_count != source_count:
        raise RuntimeError(
            f"the {table} row copy is short: staged {staged_count} of "
            f"{source_count} rows; refusing to touch the hypertable"
        )
    # The widened-uniqueness pre-flight, before any name moves.
    await _refuse_vanilla_key_duplicates(conn, schema, table, restore_heap)
    # The trash rename: metadata-only, instant, and the point of no return
    # that is not one — the rows now live in two places (trash + heap) and
    # the vanilla name is free. The trash's index/sequence names are freed
    # aside too (renames only), or the move-in below collides with them.
    await conn.execute(f'ALTER TABLE "{schema}"."{table}" RENAME TO "{trash}"')
    await _free_vanilla_names(conn, schema, trash)
    await conn.execute(f'ALTER TABLE "{staging}"."{table}" SET SCHEMA "{schema}"')
    for statement in post_move:
        await conn.execute(statement)
    if table == "job_events":
        await _anchor_event_id_sequence(conn, schema, floor_table=trash)
    twin = _TWIN_MATCH[table]
    await conn.execute(
        f'INSERT INTO "{schema}"."{table}" '  # noqa: S608  # Why: module-owned identifiers only.
        f'SELECT o.* FROM "{schema}"."{trash}" o '
        f'WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{table}" l WHERE {twin})'
    )
    live_count = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}"."{table}"'  # noqa: S608
    )
    if live_count < source_count:
        raise RuntimeError(
            f"the {table} row restore is short: {live_count} against the "
            f"{source_count} the swap verified; the trash keeps every row"
        )
    for orphan in (trash, restore_heap):
        await _absorb_orphan_rows(conn, schema, table, orphan)
    return True


async def disable_hypertables(
    conn: asyncpg.Connection,
    *,
    schema: str,
    settings: WorkerSettings,
) -> HypertableReport:
    """The mirror of :func:`enable_hypertables`: every hypertable back to
    its plain vanilla self, every row preserved, every traded-away
    behavior restored.

    Runs inside the caller's transaction/lock context exactly like
    :func:`enable_hypertables` (the ``taskq migrate up`` deploy step owns
    the migration advisory lock) and is idempotent: already-plain tables
    are skipped, already-absent policies remove as no-ops, and a second
    full run converges to the same report. The gate mirrors enable's
    zero-SQL contract inverted: with ``settings.timescaledb_hypertables``
    still true this returns immediately having issued ZERO statements —
    the operator flips the flag off first, then disables; a run against
    a server without the extension is equally a no-op (vanilla servers
    have nothing to disable).

    The mechanics per hypertable: the vanilla shape is cloned from a
    scratch schema (``{schema}__vanilla``) that the bundled migrations
    themselves build fresh — never re-typed by hand, so it cannot drift
    from the migrations — the rows are copied out and count-verified, the
    hypertable RENAMES to ``{table}__hypertable_trash`` (metadata-only:
    from that moment the rows exist in two places and the vanilla name is
    free), the migration-built table moves into place (``SET SCHEMA``,
    carrying its indexes, constraints, defaults, and owned sequence), the
    rows return from the trash twin-verified (a row committed mid-swap is
    caught too), and only then are the trash and the restore heap dropped
    — never while a row of theirs lacks a live twin. The vanilla
    behaviors (the bare primary keys, the ``job_attempts_archive``
    foreign key and its cascade, the event id sequence's position) are
    restored unconditionally and idempotently afterward. The in-memory
    twins need nothing: vanilla semantics are the default — there is
    nothing to disable but the schema.

    Crash safety is structural, not procedural: a crashed run leaves every
    row in at least two places (the trash and the heap, or the trash and
    the live table), and the NEXT disable's first act — before any ``DROP
    SCHEMA CASCADE``, the one statement that could ever destroy a
    dependency a crash left dangling — is
    :func:`_converge_crashed_swaps`, which finishes every crashed table's
    move under the same twin/count verification. The staging schema is
    dropped on the success path; a crashed run leaves it behind and the
    re-run cleans it up only after the convergence has finished with it.

    The report is the mirror reading of :class:`HypertableReport`:
    ``converted`` lists the tables returned to plain by this run,
    ``retention_policies`` the ``table:interval`` strings of the
    retention policies REMOVED (intervals read from the registered jobs'
    own configs before removal), ``compression_policies`` the tables
    whose compression policies were removed, and
    ``decompression_guc_warning`` is always None (nothing was adopted).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    if settings.timescaledb_hypertables:
        # The flag gate, mirrored: the operator flips the flag off
        # FIRST. A zero-statement no-op, like enable's flag-off path.
        return HypertableReport()

    capability = await probe_timescale_capability(conn)
    if not capability.installed:
        # A vanilla server: nothing to disable, nothing to issue.
        return HypertableReport()
    if not capability.preloaded:
        raise TimescaleDBUnavailableError(
            "disabling hypertables requires TimescaleDB to be loadable: the "
            "extension is installed but absent from shared_preload_libraries, "
            "so the hypertable DDL this disable must run cannot execute. "
            "Restore the preload (and restart) before disabling."
        )

    retention_removed, compression_removed = await _remove_registered_policies(conn, schema)

    staging = _vanilla_staging_schema(schema)
    # Crash convergence FIRST — before any DROP SCHEMA CASCADE (the one
    # statement that could destroy a dependency a crash left dangling, the
    # staging enum type a half-moved jobs_archive still types its status
    # with) and before the migrations rebuild the staging schema: every
    # crashed table's stranded rows are twin-verified into their live
    # table here, or the trash renames back and the swap below redoes it.
    converged = await _converge_crashed_swaps(conn, schema, staging)
    # A crashed earlier run's leftovers go last; the convergence above
    # guarantees nothing of value depends on the staging schema (its
    # tables are the migrations' own rowless output — the rows always
    # live in the trash/heap/live tables of THIS schema). The migrations
    # rebuild the staging schema from zero below.
    await conn.execute(f'DROP SCHEMA IF EXISTS "{staging}" CASCADE')
    await apply_pending(conn, schema=staging)
    # The staged tables' foreign keys point at the STAGING schema's own
    # jobs / jobs_archive; dropped here so the moved-in tables carry no
    # reference into a schema that is about to be dropped. The real ones
    # come back against this schema's tables in the behavior restore.
    await conn.execute(
        f'ALTER TABLE "{staging}".job_events DROP CONSTRAINT IF EXISTS job_events_job_id_fkey'
    )
    await conn.execute(
        f'ALTER TABLE "{staging}".job_attempts_archive '
        "DROP CONSTRAINT IF EXISTS job_attempts_archive_job_id_fkey"
    )
    restored: list[str] = []
    for table, post_move in (
        (
            "jobs_archive",
            (
                f'ALTER TABLE "{schema}".jobs_archive '
                f'ALTER COLUMN status TYPE "{schema}".job_status '
                f'USING status::text::"{schema}".job_status',
            ),
        ),
        ("job_attempts_archive", ()),
        ("job_events", ()),
    ):
        if await _restore_vanilla_table(conn, schema, staging, table, post_move):
            restored.append(table)
    # Unconditional and idempotent: converges a crashed mid-swap run too.
    await _restore_vanilla_behaviors(conn, schema)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{staging}" CASCADE')

    report = HypertableReport(
        converted=tuple(dict.fromkeys((*restored, *converged))),
        retention_policies=retention_removed,
        compression_policies=compression_removed,
    )
    if restored or converged:
        logger.info(
            "hypertables-disabled",
            schema=schema,
            restored=list(restored),
            crash_recovered=list(converged),
            retention_policies=list(retention_removed),
            compression_policies=list(compression_removed),
        )
    return report
