"""End-to-end check that bundled migrations apply cleanly to a real PG18.

If these pass, the schema in :mod:`taskq.migrations` is loadable, the
runner records every file, and a second ``apply_pending`` call is a
no-op (idempotency).
"""

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.backend._sql_templates import COPY_FROM_COLUMNS
from taskq.settings import TaskQSettings

pytestmark = pytest.mark.integration


async def test_discover_finds_initial_migration() -> None:
    migrations = migrate_mod.discover()
    assert migrations, "expected at least one bundled migration"
    first = migrations[0]
    assert first.version == "01.00.00_01"
    assert first.phase == "pre"
    assert first.description == "initial"


async def test_apply_pending_creates_schema(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    assert len(applied) == len(migrate_mod.discover())

    rows = await pg_conn.fetch(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
        ORDER BY table_name
        """,
        settings.schema_name,
    )
    table_names = {r["table_name"] for r in rows}
    assert {
        "actor_config",
        "cron_schedules",
        "job_attempts",
        "job_attempts_archive",
        "job_events",
        "jobs",
        "jobs_archive",
        "maintenance_leader",
        "rate_limit_buckets",
        "reservation_slots",
        "schema_migrations",
        "workers",
    } <= table_names


async def test_apply_pending_is_idempotent(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    first = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    assert first, "expected initial run to apply migrations"

    second = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    assert second == [], "second apply should be a no-op"


async def test_dispatch_index_exists(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Spot-check the most performance-critical index from"""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    row = await pg_conn.fetchrow(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'jobs_dispatch_idx'
        """,
        settings.schema_name,
    )
    assert row is not None
    assert "queue" in row["indexdef"]
    assert "priority" in row["indexdef"]
    assert "scheduled_at" in row["indexdef"]
    assert "status = 'pending'" in row["indexdef"]


# ── Archive tables () ──────────────────────────────────


async def test_copy_from_columns_match_jobs_table_exactly(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """COPY_FROM_COLUMNS is the single source of truth for both the
    enqueue_batch_fast COPY tuple and the archive sweep's explicit column
    lists. If a future migration adds a column to `jobs` without updating
    it, the archive sweep would silently stop archiving that column (data
    loss on prune) and the COPY path would fail or misalign. Lock the
    tuple to the actual table definition: exact set equality, no missing,
    no extras."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs'
        """,
        settings.schema_name,
    )
    actual = {r["column_name"] for r in rows}
    declared = set(COPY_FROM_COLUMNS)
    assert declared == actual, (
        f"COPY_FROM_COLUMNS drifted from jobs table: "
        f"missing from tuple: {sorted(actual - declared)}, "
        f"not in table: {sorted(declared - actual)}"
    )


async def test_copy_enqueue_columns_are_copy_from_minus_server_stamped(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """COPY_ENQUEUE_COLUMNS (the enqueue COPY path) is COPY_FROM_COLUMNS
    minus exactly the columns the post-COPY fixup UPDATE stamps server-side
    or a DDL default covers.  Every omitted column must be safe to omit
    from COPY: nullable or carrying a DDL default - a NOT NULL column
    without a default would make the COPY insert fail outright.  Order is
    positional (the record tuples are built by hand), so it must be a
    order-preserving subsequence of COPY_FROM_COLUMNS.  ``status`` is
    written explicitly, as a status the INSERT trigger's gate ignores."""
    from taskq.backend._sql_templates import COPY_ENQUEUE_COLUMNS, COPY_ENQUEUE_STATUS

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    assert "status" in COPY_ENQUEUE_COLUMNS
    assert COPY_ENQUEUE_STATUS != "pending"
    omitted = {
        "created_at",
        "scheduled_at",
        "schedule_to_close",
        "result_expires_at",
        "snooze_count",
        "rate_limit_blocked_count",
        "interrupt_count",
        "claim_epoch",
        # 01.00.24's workflow columns: vanilla enqueues never set them (the
        # DDL defaults apply); the engine's own statements write them.
        # parent_id is NOT in this omission set: the LIB-2 fan-out ledger's
        # trailing member rides the COPY (the enqueue contextvar stamps
        # current_parent_id(); the record builder carries it last and the
        # ARITY PIN (test_enqueue_copy_record_arity_matches_columns) holds
        # the builder-vs-columns coherence from the records' side).
        "deps_pending",
        "map_index",
        "step_key",
        "code_version",
        # 01.00.26/01.00.30's loop-budget columns: the same law as the
        # workflow columns (vanilla enqueues never set them, the DDL
        # defaults apply). 01.00.26 joined COPY_FROM_COLUMNS for the archive
        # CTE's mirror parity WITHOUT joining this set -- the desync made
        # every enqueue_batch_fast COPY die in asyncpg's copy_in with
        # "IndexError: tuple index out of range" (42 columns, 39-wide
        # records). The record-arity pin below is the other half of the
        # fence: it reads the BUILDER, this set reads the omission law.
        "budget_deadline",
        "budget_paused",
        "budget_remaining_ms",
    }
    assert set(COPY_ENQUEUE_COLUMNS) == set(COPY_FROM_COLUMNS) - omitted
    assert list(COPY_ENQUEUE_COLUMNS) == [c for c in COPY_FROM_COLUMNS if c not in omitted]

    rows = await pg_conn.fetch(
        """
        SELECT column_name, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs'
        """,
        settings.schema_name,
    )
    by_col = {r["column_name"]: r for r in rows}
    for col in sorted(omitted):
        rec = by_col[col]
        assert rec["is_nullable"] == "YES" or rec["column_default"] is not None, (
            f"column {col!r} is omitted from COPY_ENQUEUE_COLUMNS but is NOT NULL "
            f"without a DDL default - the enqueue COPY path cannot omit it"
        )


async def test_enqueue_copy_record_arity_matches_columns(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """THE ARITY PIN (the COPY record builder vs the column list it feeds).

    The record tuples in ``_enqueue_batch_fast``'s build loop are built BY
    HAND, positionally, against ``copy_enqueue_columns`` -- nothing in the
    type system ties their lengths together. The drift is not theoretical:
    01.00.26 added budget_deadline/budget_paused/budget_remaining_ms to
    COPY_FROM_COLUMNS (hence to COPY_ENQUEUE_COLUMNS) without touching the
    builder, and EVERY enqueue_batch_fast COPY died in asyncpg's
    protocol.pyx copy_in with a bare "IndexError: tuple index out of
    range" -- a 39-wide record against 42 columns. The failure names
    nothing useful; the mirror-divergence pins reported it as an
    InMemory-vs-PG contract break. This pin fails at the seam instead:
    drive one real enqueue through the real builder and demand the
    record's width equals the column list's, with both named on failure.
    """
    from unittest.mock import patch

    from pydantic import BaseModel

    from taskq import EnqueueItem, TaskQ
    from taskq.actor import actor
    from taskq.migrate import apply_pending

    class _Payload(BaseModel):
        value: int

    schema = f"{settings.schema_name}_copy_arity"
    await apply_pending(pg_conn, schema=schema)

    captured: list[tuple[tuple[str, ...], tuple[object, ...]]] = []
    orig = asyncpg.Connection.copy_records_to_table

    async def _probe(
        self: asyncpg.Connection,
        table: str,
        *,
        records: list[tuple[object, ...]],
        columns: tuple[str, ...],
        schema_name: str | None,
        **kw: object,
    ) -> str:
        captured.append((tuple(columns), tuple(records[0])))
        result = await orig(
            self,
            table,
            records=records,
            columns=columns,
            schema_name=schema_name,
            **kw,  # pyright: ignore[reportUnknownArgumentType]  # Why: the probe mirrors asyncpg's own signature; the pass-through is the real call.
        )
        return result  # pyright: ignore[reportReturnType]  # Why: asyncpg's COPY returns a COPY-status str; the stub's declared str matches the real call's shape.

    @actor(name="copy_arity_probe_actor")
    async def _probe_actor(payload: _Payload) -> None: ...

    async with TaskQ(dsn=str(settings.pg_dsn), schema=schema) as tq:
        with patch.object(asyncpg.Connection, "copy_records_to_table", _probe):
            await tq.enqueue_batch_fast(
                [EnqueueItem(actor_ref=_probe_actor, payload=_Payload(value=1))]
            )

    assert captured, "the COPY probe saw no call - the builder's path changed"
    for columns, record in captured:
        assert len(record) == len(columns), (
            f"COPY record arity desynced from copy_enqueue_columns: "
            f"record has {len(record)} elements, the column list has {len(columns)} "
            f"({columns[len(record) :]} unwritten). asyncpg's copy_in fails this shape "
            f"with a bare IndexError from protocol.pyx. Extend the record builder in "
            f"_enqueue_batch_fast OR add the columns to _COPY_ENQUEUE_OMITTED - "
            f"whichever the column's writer contract says."
        )


async def test_jobs_archive_columns_match_jobs_plus_archive_fields(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    jobs_cols = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )
    archive_cols = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs_archive'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )

    jobs_col_map = {r["column_name"]: r for r in jobs_cols}
    archive_col_map = {r["column_name"]: r for r in archive_cols}

    for name in jobs_col_map:
        assert name in archive_col_map, f"jobs column {name!r} missing from jobs_archive"

    assert "archived_at" in archive_col_map
    assert archive_col_map["archived_at"]["is_nullable"] == "NO"
    assert "expire_at" in archive_col_map
    assert archive_col_map["expire_at"]["is_nullable"] == "NO"


async def test_job_attempts_archive_columns_match_job_attempts(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    attempts_cols = await pg_conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'job_attempts'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )
    archive_cols = await pg_conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'job_attempts_archive'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )

    attempts_names = {r["column_name"] for r in attempts_cols}
    archive_names = {r["column_name"] for r in archive_cols}
    assert attempts_names == archive_names, (
        f"job_attempts columns {attempts_names - archive_names} missing from "
        f"job_attempts_archive; extra: {archive_names - attempts_names}"
    )

    fk_rows = await pg_conn.fetch(
        """
        SELECT tc.constraint_name, kcu.column_name,
               ccu.table_name AS ref_table
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = 'job_attempts_archive'
            AND tc.constraint_type = 'FOREIGN KEY'
        """,
        settings.schema_name,
    )
    assert any(
        r["column_name"] == "job_id" and r["ref_table"] == "jobs_archive" for r in fk_rows
    ), "job_attempts_archive.job_id must reference jobs_archive"

    fk_delete_rows = await pg_conn.fetch(
        """
        SELECT rc.delete_rule
        FROM information_schema.referential_constraints rc
        JOIN information_schema.table_constraints tc
            ON rc.constraint_name = tc.constraint_name
            AND rc.constraint_schema = tc.constraint_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = 'job_attempts_archive'
            AND tc.constraint_type = 'FOREIGN KEY'
        """,
        settings.schema_name,
    )
    assert any(r["delete_rule"] == "CASCADE" for r in fk_delete_rows), (
        "job_attempts_archive FK must use ON DELETE CASCADE"
    )


async def test_archive_indexes_exist(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    index_names = await pg_conn.fetch(
        """
        SELECT indexname FROM pg_indexes
        WHERE schemaname = $1
            AND indexname IN (
                'jobs_archive_expire_at_idx',
                'jobs_archive_finished_at_idx',
                'job_attempts_archive_job_id_idx'
            )
        """,
        settings.schema_name,
    )
    found = {r["indexname"] for r in index_names}
    assert {
        "jobs_archive_expire_at_idx",
        "jobs_archive_finished_at_idx",
        "job_attempts_archive_job_id_idx",
    } <= found


async def test_archive_table_comments_exist(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    for table in ("jobs_archive", "job_attempts_archive"):
        row = await pg_conn.fetchrow(
            """
            SELECT obj_description(
                ($1 || '.' || $2)::regclass, 'pg_class'
            ) AS comment
            """,
            settings.schema_name,
            table,
        )
        assert row is not None and row["comment"] is not None, f"{table} missing table comment"


async def test_cron_schedules_has_consecutive_failures_column(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1
            AND table_name = 'cron_schedules'
            AND column_name = 'consecutive_failures'
        """,
        settings.schema_name,
    )
    assert len(rows) == 1, "consecutive_failures column missing from cron_schedules"
    col = rows[0]
    assert col["data_type"] == "integer"
    assert col["is_nullable"] == "NO"
    assert col["column_default"] == "0"


async def test_cron_schedules_has_disabled_by_column(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The ``01.00.19_02_pre_cron_disabled_by`` migration adds a nullable
    ``disabled_by text`` column to ``cron_schedules``: the ownership model's
    who-disabled-it marker ('auto' = the cron loop's failure-count
    auto-disable, 'operator' = a deliberate disable, NULL = enabled or
    disabled before ownership was tracked)."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1
            AND table_name = 'cron_schedules'
            AND column_name = 'disabled_by'
        """,
        settings.schema_name,
    )
    assert len(rows) == 1, "disabled_by column missing from cron_schedules"
    col = rows[0]
    assert col["data_type"] == "text"
    assert col["is_nullable"] == "YES"
    assert col["column_default"] is None

    # The allowed values are enforced: NULL (enabled / pre-ownership) passes,
    # the two ownership markers pass, anything else is rejected.
    await pg_conn.execute(
        f'INSERT INTO "{settings.schema_name}".cron_schedules'  # noqa: S608  # Why: schema is a fixture-provided identifier.
        "(id, actor, cron_expr, next_fire_at, enabled, disabled_by) "
        "VALUES ($1, $2, '0 * * * *', statement_timestamp(), false, 'operator')",
        new_uuid(),
        "disabled_by_check_probe",
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await pg_conn.execute(
            f'INSERT INTO "{settings.schema_name}".cron_schedules'  # noqa: S608
            "(id, actor, cron_expr, next_fire_at, enabled, disabled_by) "
            "VALUES ($1, $2, '0 * * * *', statement_timestamp(), false, 'nobody')",
            new_uuid(),
            "disabled_by_check_probe_bad",
        )


async def test_queues_has_max_concurrent_column(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The ``01.00.04_01_pre_queue_concurrency`` migration adds a nullable
    ``max_concurrent int`` column to the ``queues`` table."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1
            AND table_name = 'queues'
            AND column_name = 'max_concurrent'
        """,
        settings.schema_name,
    )
    assert len(rows) == 1, "max_concurrent column missing from queues"
    col = rows[0]
    assert col["data_type"] == "integer"
    assert col["is_nullable"] == "YES"


# ── The fence-probe index migration's OPS NOTE escape hatch ────────────


async def _reset_fence_probe_to_legacy_form(
    pg_conn: asyncpg.Connection, schema: str, version: str = "01.00.21_02"
) -> None:
    """Drag the fence-probe index back to a pre-migration state: the carrier
    migration's ledger row deleted and the LEGACY one-key form owning the
    canonical name, as on a pre-upgrade database. The condition under test
    ships in ``01.00.21_02`` — the restored ``01.00.19_01`` is immutable and
    keeps its originally-shipped (weaker) condition."""
    await pg_conn.execute(f'DROP INDEX IF EXISTS "{schema}".jobs_locked_by_worker_running_idx')
    await pg_conn.execute(f'DROP INDEX IF EXISTS "{schema}".jobs_locked_by_worker_running_idx_old')
    await pg_conn.execute(f'DROP INDEX IF EXISTS "{schema}".jobs_locked_by_worker_running_idx_new')
    await pg_conn.execute(
        f'CREATE INDEX jobs_locked_by_worker_running_idx ON "{schema}".jobs'  # Why: schema is a fixture-provided identifier.
        " (locked_by_worker) WHERE status = 'running'"
    )
    await pg_conn.execute(
        f"DELETE FROM \"{schema}\".schema_migrations WHERE version = '{version}:pre'"  # noqa: S608  # Why: schema is a fixture-provided identifier.
    )


async def test_fence_probe_migration_replaces_legacy_index(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The plain path: with the legacy one-key form owning the canonical
    name (the pre-upgrade state), re-applying the ``01.00.21_02`` carrier
    drops it and lands the two-key form under the canonical name, valid.
    The restored ``01.00.19_01`` is immutable and keeps its originally
    shipped condition; the pg_index-conditioned rebuild ships in the
    carrier."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    await _reset_fence_probe_to_legacy_form(pg_conn, settings.schema_name)

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    row = await pg_conn.fetchrow(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert row is not None, "canonical index missing after the plain path"
    assert "(locked_by_worker, id)" in row["indexdef"], (
        "the plain path must land the two-key form under the canonical name"
    )
    leftover = await pg_conn.fetchval(
        """
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = $1 AND indexname LIKE 'jobs_locked_by_worker_running_idx_%'
        """,
        settings.schema_name,
    )
    assert leftover == 0, "the plain path must leave no swap leftovers"


async def test_fence_probe_migration_survives_the_documented_swap(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The OPS NOTE's escape hatch, end to end: pre-build concurrently, swap
    by name (legacy form parked under ``..._old``, two-key form installed as
    canonical), then apply the migration. The migration's drop must NOT
    undo the swap: the canonical two-key form survives, valid, and the
    parked legacy form is cleaned up."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    await _reset_fence_probe_to_legacy_form(pg_conn, settings.schema_name)

    # The recipe, exactly as the migration's OPS NOTE documents it. (The
    # index name is deliberately NOT schema-qualified: CREATE INDEX takes
    # the table's schema, and a qualified name is a syntax error.)
    await pg_conn.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS"  # Why: schema is a fixture-provided identifier.
        f" jobs_locked_by_worker_running_idx_new"
        f' ON "{settings.schema_name}".jobs (locked_by_worker, id)'
        f" WHERE status = 'running'"
    )
    async with pg_conn.transaction():
        await pg_conn.execute(
            f'ALTER INDEX "{settings.schema_name}".jobs_locked_by_worker_running_idx'
            " RENAME TO jobs_locked_by_worker_running_idx_old"
        )
        await pg_conn.execute(
            f'ALTER INDEX "{settings.schema_name}".jobs_locked_by_worker_running_idx_new'
            " RENAME TO jobs_locked_by_worker_running_idx"
        )

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    row = await pg_conn.fetchrow(
        """
        SELECT i.indisvalid, i.indisready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert row is not None, "the swap's canonical index must survive the migration"
    assert row["indisvalid"] and row["indisready"], (
        "the surviving canonical index must be valid and ready"
    )
    indexdef = await pg_conn.fetchval(
        """
        SELECT indexdef FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert "(locked_by_worker, id)" in indexdef, (
        "the surviving canonical index must be the two-key form the operator built"
    )
    leftover = await pg_conn.fetchval(
        """
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = $1 AND indexname LIKE 'jobs_locked_by_worker_running_idx_%'
        """,
        settings.schema_name,
    )
    assert leftover == 0, "the parked legacy form must be cleaned up"


async def test_fence_probe_migration_rebuilds_an_include_id_canonical(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """An operator who built the canonical name as
    ``(locked_by_worker) INCLUDE (id)`` — a plausible misreading of the
    recipe — is NOT holding the two-key form: an INCLUDE column is payload,
    never an Index Cond, so the fence probe still walks the whole running
    set. ``pg_attribute`` lists INCLUDE columns, so a definition check that
    reads attributes cannot tell the two forms apart — which is why the
    restored ``01.00.19_01`` (immutable, original condition) spares this
    form and the ``01.00.21_02`` carrier's pg_index-conditioned drop must
    fire on it: after the migration, ``id`` must be a KEY
    column of the canonical index, valid."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    await _reset_fence_probe_to_legacy_form(pg_conn, settings.schema_name)

    # The operator's mistake: the INCLUDE form under the canonical name.
    await pg_conn.execute(f'DROP INDEX "{settings.schema_name}".jobs_locked_by_worker_running_idx')
    await pg_conn.execute(
        f"CREATE INDEX jobs_locked_by_worker_running_idx"
        f' ON "{settings.schema_name}".jobs (locked_by_worker) INCLUDE (id)'
        f" WHERE status = 'running'"
    )

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    indexdef = await pg_conn.fetchval(
        """
        SELECT indexdef FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert indexdef is not None, "canonical index missing after the migration"
    assert "(locked_by_worker, id)" in indexdef, (
        "an INCLUDE(id) form is not the two-key form: id must end up a KEY "
        f"column, got {indexdef!r}"
    )


async def test_fence_probe_migration_rebuilds_an_invalid_canonical_debris(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """An interrupted direct ``CREATE INDEX CONCURRENTLY`` under the canonical
    name leaves INVALID debris that the trailing ``IF NOT EXISTS`` alone
    would silently keep (the runner's own drop-the-debris discipline). The
    conditional drop must not spare it: after the migration the canonical
    index must be the two-key form, valid."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    await _reset_fence_probe_to_legacy_form(pg_conn, settings.schema_name)

    # Reproduce the debris honestly: an uncommitted write transaction on
    # another connection parks CIC's phase-2 ShareLock wait (an empty table's
    # read-only snapshot never engages it), statement_timeout cancels the
    # build mid-flight, and PG keeps the INVALID index. The legacy form is
    # dropped first: the failing build targets the canonical name itself, the
    # way an operator running the recipe against the canonical name directly
    # would.
    await pg_conn.execute(f'DROP INDEX "{settings.schema_name}".jobs_locked_by_worker_running_idx')
    blocker = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await blocker.execute("BEGIN")
        await blocker.execute(
            f'DELETE FROM "{settings.schema_name}".jobs WHERE false'  # noqa: S608  # Why: schema is a fixture-provided identifier.
        )
        await pg_conn.execute("SET statement_timeout = '2s'")
        with pytest.raises(asyncpg.exceptions.QueryCanceledError):
            await pg_conn.execute(
                f"CREATE INDEX CONCURRENTLY jobs_locked_by_worker_running_idx"
                f' ON "{settings.schema_name}".jobs (locked_by_worker, id)'
                f" WHERE status = 'running'"
            )
    finally:
        await blocker.execute("ROLLBACK")
        await blocker.close()

    invalid = await pg_conn.fetchval(
        """
        SELECT NOT i.indisvalid
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert invalid is True, "setup precondition: the interrupted CIC must have left INVALID debris"

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    row = await pg_conn.fetchrow(
        """
        SELECT i.indisvalid, i.indisready
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert row is not None, "canonical index missing after the migration"
    assert row["indisvalid"] and row["indisready"], (
        "INVALID debris must be rebuilt, not silently kept behind IF NOT EXISTS"
    )
    indexdef = await pg_conn.fetchval(
        """
        SELECT indexdef FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'jobs_locked_by_worker_running_idx'
        """,
        settings.schema_name,
    )
    assert indexdef is not None and "(locked_by_worker, id)" in indexdef, (
        "the rebuilt canonical index must be the two-key form"
    )
