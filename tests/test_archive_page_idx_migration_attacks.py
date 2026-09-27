"""Migration 01.00.21_01 under attack: the archive tab's page-keyset
index (``jobs_archive_page_idx``, ``(finished_at DESC NULLS LAST,
id DESC)``).

The index is a plain (non-concurrent) ``CREATE INDEX`` — the 01.00.19_01
/ 01.00.20_01 lock discipline, documented in the migration's own OPS
NOTE. The attacks here are the ones a live fleet actually runs into:

* **idempotence** — the file's statement re-executed on an
  already-migrated schema must be a true no-op: same index, still valid,
  no duplicate under the canonical name (the IF NOT EXISTS contract the
  documented CONCURRENTLY pre-build escape hatch leans on);
* **checksum honesty** — the ledger checksum recorded at apply time must
  be the bundled file's own render (``checksum_drifts`` empty), so an
  operator diffing a migrated schema against the repo sees no drift;
* **the fleet path** — a schema migrated BEFORE this migration existed
  (ledger complete through 01.00.20_03, index absent) must pick up
  exactly this one migration and land the index;
* **the hypertable path** — an operator who converts ``jobs_archive`` to
  a hypertable (``TASKQ_TIMESCALEDB_HYPERTABLES=true``) AFTER migrating:
  ``create_hypertable(migrate_data => TRUE)`` must not choke on the new
  index, the index must carry to every chunk (chunks are where the
  archive tab's pages actually read), the page walk stays row-exact
  through the conversion, and the disable mirror must restore the index
  it rebuilds from the bundled migrations.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

pytest.importorskip("fastapi")  # Why: this module attacks the admin page's builders; the extras legs without fastapi must skip, not error at collection.

from taskq import migrate as migrate_mod  # noqa: E402  # Why: importorskip must precede the optional-import chain.
from taskq._ids import new_base62
from taskq.constants import _IDENT_RE  # pyright: ignore[reportPrivateUsage]
from taskq.settings import WorkerSettings  # pyright: ignore[reportPrivateUsage]
from taskq.timescale import (  # pyright: ignore[reportPrivateUsage]
    disable_hypertables,
    enable_hypertables,
)
from taskq.web.admin._constants import _PAGE_SIZE  # pyright: ignore[reportPrivateUsage]  # noqa: E402  # Why: after importorskip.
from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]  # noqa: E402  # Why: the admin module's own builders are the queries under attack; a hand-copied SQL shape would drift from the real page.
    _ARCHIVE_COLS,
    _SORTABLE_ARCHIVE,
    _build_paginated_sql,
    _build_where,
    _cursor_field,
)

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifiers (built from new_base62, validated by the migration runner's _IDENT_RE) or renders the admin builders' own SQL; all values are $n-bound.

pytestmark = pytest.mark.integration

_MIGRATION_FILENAME = "01.00.21_01_pre_archive_page_keyset_index.sql"
_INDEX = "jobs_archive_page_idx"
_TERMINAL = sorted({"succeeded", "failed", "cancelled", "crashed", "abandoned"})


# ── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
async def migrated_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, torn down at module end."""
    schema = f"mig_attack_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _index_defs(
    conn: asyncpg.Connection, schema: str, table: str
) -> list[tuple[str, str, bool]]:
    """(name, pg_get_indexdef, indisvalid) for every index on *table*."""
    rows = await conn.fetch(
        """
        SELECT c.relname AS name, pg_get_indexdef(c.oid) AS def, x.indisvalid AS valid
        FROM pg_index x
        JOIN pg_class c ON c.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2
        """,
        schema,
        table,
    )
    return [(r["name"], r["def"], r["valid"]) for r in rows]


# ── idempotence + checksum honesty ───────────────────────────────────────


async def test_migration_file_rerun_is_a_true_noop(pg_dsn: str, migrated_schema: str) -> None:
    """The migration's statement, executed again on the migrated schema:
    no error, no duplicate index, still valid, definition unchanged — the
    IF NOT EXISTS contract the documented CONCURRENTLY pre-build escape
    hatch relies on (an operator who pre-built must be able to re-run)."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        before = await _index_defs(conn, migrated_schema, "jobs_archive")
        matching = [(n, d, v) for n, d, v in before if n == _INDEX]
        assert len(matching) == 1, f"the canonical name is not unique pre-rerun: {before}"
        assert matching[0][2], "the page index is not valid pre-rerun"

        migration = next(m for m in migrate_mod.discover() if m.filename == _MIGRATION_FILENAME)
        await conn.execute(migration.render(migrated_schema))

        after = await _index_defs(conn, migrated_schema, "jobs_archive")
        after_matching = [(n, d, v) for n, d, v in after if n == _INDEX]
        assert after_matching == matching, (
            f"the rerun changed the index: {matching} -> {after_matching}"
        )
    finally:
        await conn.close()


async def test_ledger_checksum_is_the_bundled_files_own_render(
    pg_dsn: str, migrated_schema: str
) -> None:
    """Checksum honesty on a fresh apply: the ledger row for 01.00.21_01
    records the bundled file's own render — ``checksum_drifts`` must come
    back empty, so an operator diffing the migrated schema sees no drift."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        drifts = await migrate_mod.checksum_drifts(conn, schema=migrated_schema)
        assert drifts == {}, (
            f"checksum drift on a fresh apply — the ledger does not match the "
            f"bundled files: {drifts}"
        )
        applied = await conn.fetch(
            f'SELECT checksum FROM "{migrated_schema}".schema_migrations WHERE version = $1',
            "01.00.21_01:pre",
        )
        assert len(applied) == 1, "01.00.21_01 is not in the ledger after apply"
        migration = next(m for m in migrate_mod.discover() if m.filename == _MIGRATION_FILENAME)
        assert applied[0]["checksum"] == migration.checksum(migrated_schema), (
            "the ledger checksum is not the bundled file's render"
        )
    finally:
        await conn.close()


async def test_fleet_path_apply_picks_up_only_the_new_migration(pg_dsn: str) -> None:
    """A dev DB migrated before 01.00.21_01 existed: ledger complete
    through 01.00.20_03, page index absent. apply_pending must apply
    exactly this migration, create the index, and leave the ledger
    drift-free — the upgrade every existing deployment runs."""
    schema = f"mig_fleet_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)

        # Roll the schema back to the pre-01.00.21_01 fleet state: drop
        # the index and its ledger row (the dev DB that predates the file).
        await conn.execute(f'DROP INDEX IF EXISTS "{schema}"."{_INDEX}"')
        await conn.execute(
            f'DELETE FROM "{schema}".schema_migrations WHERE version = $1',
            "01.00.21_01:pre",
        )
        pre = await _index_defs(conn, schema, "jobs_archive")
        assert not any(name == _INDEX for name, _, _ in pre), "the index survived the rollback"

        report = await migrate_mod.apply_pending(conn, schema=schema)
        applied = [m.key for m in report]
        assert applied == ["01.00.21_01:pre"], (
            f"the fleet apply must pick up exactly the new migration: {applied}"
        )
        post = await _index_defs(conn, schema, "jobs_archive")
        defs = [d for name, d, _ in post if name == _INDEX]
        assert len(defs) == 1, f"the fleet apply did not land exactly one page index: {post}"
        assert "(finished_at DESC NULLS LAST, id DESC)" in defs[0], defs[0]
        drifts = await migrate_mod.checksum_drifts(conn, schema=schema)
        assert drifts == {}, f"drift after the fleet apply: {drifts}"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── the hypertable path ──────────────────────────────────────────────────

_TIMESCALE_IMAGE_DEFAULT = "timescale/timescaledb:2.30.1-pg18"


@pytest.fixture(scope="module")
def timescale_dsn() -> Any:
    """One timescaledb container per module; skips without Docker (the
    hypertable suite's own fixture shape)."""
    from taskq.testing._shared_containers import skip_test_without_docker

    skip_test_without_docker()
    import os

    from testcontainers.community.postgres import PostgresContainer

    image = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT
    with PostgresContainer(
        image=image, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels={}) as container:
        yield container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


def _ts_settings(dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
        },
        validate=False,
    )


async def _seed_archive_rows(
    conn: asyncpg.Connection, schema: str, fins: list[datetime | None]
) -> None:
    """Adversarial archive population: valued rows, a wide tie, a NULL tail."""
    base = datetime.now(UTC) - timedelta(days=10)
    for i, fin in enumerate(fins):
        await conn.execute(
            f'INSERT INTO "{schema}".jobs_archive '
            "(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
            "created_at, scheduled_at, started_at, finished_at, archived_at, expire_at) "
            f"VALUES (md5('ht' || $1::text)::uuid, 'ht_actor', 'ht_q', "
            f"'{{\"v\": 1}}'::jsonb, 'succeeded', 1, 3, 'transient', "
            "$2::timestamptz - interval '1 day', $2::timestamptz - interval '1 day', "
            "COALESCE($3::timestamptz, $2::timestamptz - interval '12 hours'), "
            "$3::timestamptz, $2::timestamptz, $2::timestamptz + interval '365 days')",
            str(i),
            base,
            fin,
        )


async def _walk_and_reference(conn: asyncpg.Connection, schema: str) -> None:
    """One full forward page walk must equal the bare reference order."""
    walked: list[Any] = []
    cursor: tuple[str, str] | None = None
    while True:
        # The walk rebuilds the statement each page (the same builder,
        # fresh cursor).
        where, params = _build_where(_TERMINAL, None, None, None, None, None, None, None)
        page_sql, page_args = _build_paginated_sql(
            schema,
            "jobs_archive",
            _ARCHIVE_COLS,
            dict(_SORTABLE_ARCHIVE),
            where,
            list(params),
            cursor[0] if cursor else None,
            cursor[1] if cursor else None,
            "next",
            "finished_at",
            "desc",
        )
        rows = await conn.fetch(page_sql, *page_args)
        if not rows:
            break
        shown = rows[:_PAGE_SIZE]
        walked.extend(r["id"] for r in shown)
        last = shown[-1]
        cursor = (_cursor_field(last["finished_at"]), str(last["id"]))
    reference = await conn.fetch(
        f'SELECT id FROM "{schema}".jobs_archive ORDER BY finished_at DESC NULLS LAST, id DESC'
    )
    assert walked == [r["id"] for r in reference], (
        "the archive page walk diverged from the reference order — the "
        "conversion dropped, duplicated, or reordered rows"
    )


async def test_hypertable_conversion_refuses_the_archives_null_tail(
    timescale_dsn: str,
) -> None:
    """FINDING (pre-existing, pinned for attribution): enable_hypertables
    CANNOT convert an archive that carries NULL finished_at rows —
    create_hypertable's partition column must be NOT NULL, and
    migrate_data => TRUE dies on the NULL rows with NotNullViolationError.

    The archive's own data model produces exactly those rows (archived-
    unfinished: the NULL tail the admin page's second UNION branch and
    _cursor_field's empty-string rendering exist to serve, and the scan
    campaign's own seed carries 2% of them). So ANY deployment with an
    archived-unfinished row is locked out of the columnstore with a bare
    driver error — the opt-in guide's flag does not mention it.

    Attribution leg: the failure is the NULL DATA, not this branch's
    index — the identical conversion with jobs_archive_page_idx DROPPED
    fails the same way. The index-removal question this module exists to
    ask (does the conversion's index handling choke on the new index?)
    is answered by the carry test below, on a NULL-free archive.
    """
    schema = f"ts_nulltail_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        base = datetime.now(UTC) - timedelta(days=10)
        fins = [base - timedelta(days=i % 7, seconds=i) for i in range(20)]
        fins += [None] * 5  # the archived-unfinished rows
        await _seed_archive_rows(conn, schema, fins)

        with pytest.raises(asyncpg.NotNullViolationError):
            await enable_hypertables(
                conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
            )

        # Attribution: drop the new index, retry — same refusal.
        await conn.execute(f'DROP INDEX IF EXISTS "{schema}"."{_INDEX}"')
        with pytest.raises(asyncpg.NotNullViolationError):
            await enable_hypertables(
                conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_hypertable_conversion_carries_the_page_index(timescale_dsn: str) -> None:
    """On a NULL-free archive (the population the conversion CAN take),
    the page index must survive the columnstore round trip:
    create_hypertable(migrate_data) must not choke on the new index, every
    CHUNK must carry it (chunks are where the tab's pages read), the page
    walk stays row-exact through the conversion, and the disable mirror's
    migration-rebuilt vanilla table has the index back."""
    schema = f"ts_page_idx_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)

        # A small adversarial population BEFORE conversion: valued rows
        # and a wide tie spanning a page boundary — migrate_data must
        # carry them through the conversion byte-exact.
        base = datetime.now(UTC) - timedelta(days=10)
        fins = [base - timedelta(days=i % 7, seconds=i) for i in range(3 * _PAGE_SIZE)]
        fins += [base] * 7  # a tie spanning a page boundary
        await _seed_archive_rows(conn, schema, fins)

        # The conversion runs with the page index already on the table.
        report = await enable_hypertables(
            conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
        )
        assert "jobs_archive" in report.converted, report.converted

        # The index must exist on the PARENT and on every chunk.
        parent = await _index_defs(conn, schema, "jobs_archive")
        assert any(
            name == _INDEX and "(finished_at DESC NULLS LAST, id DESC)" in d and valid
            for name, d, valid in parent
        ), f"the parent hypertable lost the page index: {parent}"
        chunks = await conn.fetch(
            """
            SELECT chunk_schema, chunk_name
            FROM timescaledb_information.chunks
            WHERE hypertable_schema = $1 AND hypertable_name = 'jobs_archive'
            """,
            schema,
        )
        assert chunks, "the conversion produced no chunks to inspect"
        for c in chunks:
            chunk_indexes = await conn.fetch(
                """
                SELECT c.relname AS name, pg_get_indexdef(c.oid) AS def
                FROM pg_index x
                JOIN pg_class c ON c.oid = x.indexrelid
                WHERE x.indrelid = format('%I.%I', $1::text, $2::text)::regclass
                """,
                c["chunk_schema"],
                c["chunk_name"],
            )
            assert any(
                _INDEX in r["name"] and "finished_at DESC NULLS LAST" in r["def"]
                for r in chunk_indexes
            ), (
                f"chunk {c['chunk_name']} does not carry the page index: "
                f"{[r['name'] for r in chunk_indexes]}"
            )

        # The page walk stays row-exact through the conversion.
        await _walk_and_reference(conn, schema)

        # The disable mirror rebuilds the vanilla shape from the bundled
        # migrations — the page index must be in that rebuild.
        await disable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        vanilla = await _index_defs(conn, schema, "jobs_archive")
        assert any(
            name == _INDEX and "(finished_at DESC NULLS LAST, id DESC)" in d and valid
            for name, d, valid in vanilla
        ), f"the disable mirror's vanilla table lost the page index: {vanilla}"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
