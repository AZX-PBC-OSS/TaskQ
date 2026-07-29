"""Integration tests for the idempotency_scope migration
(01.00.03_01_pre_idempotency_scope.sql /
01.00.03_01_post_idempotency_scope_drop_old_index.sql).

Covers:
- Migration correctness: after applying both phases against a fresh DB,
  idempotency_scope column exists NOT NULL DEFAULT '', the old single-column
  unique index is gone, the new composite unique index exists, and two rows
  with the default scope ('') and the same idempotency_key raise
  UniqueViolationError — proving the sentinel-not-NULL composite index
  enforces global dedupe for unscoped keys at the DB level.
- Migration upgrade path: apply migrations up through 01.00.01, insert a
  job row with idempotency_key (no idempotency_scope column yet), then
  apply the rest and assert the existing row has idempotency_scope = ''
  and a second insert with the same key still conflicts.
- Rolling-deploy overlap window: after applying ONLY the `pre` phase (the
  state every worker's schema is in the moment the migration lands, before
  every worker is confirmed running the new code), pre-this-release code's
  exact `ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL`
  SQL shape must still execute successfully against the schema — this is
  the assertion that proves the pre/post split actually avoids the outage
  a single combined migration would have caused. See the "PHASE
  OBLIGATIONS" header comment in the pre migration file for the full
  rationale.
- Application enqueue path during the pre-only window: drives the real
  ``PostgresBackend.enqueue`` / ``enqueue_batch`` (not raw SQL) against a
  pre-phase-only schema. Unscoped and same-scope usage must keep working
  exactly as before; cross-scope usage of a repeated key must raise
  :class:`~taskq.exceptions.ScopedIdempotencyMigrationPendingError` — a
  clear, typed, documented error — rather than crash on a raw
  ``asyncpg.UniqueViolationError``. See that exception's docstring for why
  the library raises instead of silently falling back to a different
  scope's row.
"""

import asyncio
from datetime import UTC, datetime

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.exceptions import ScopedIdempotencyMigrationPendingError
from taskq.settings import TaskQSettings
from taskq.testing.fixtures import _open_pg_backend_on_schema
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration


async def _insert_job_raw(
    conn: asyncpg.Connection,
    schema: str,
    *,
    idempotency_key: str | None = None,
    idempotency_scope: str | None = None,
) -> None:
    """Insert a minimal job row directly via asyncpg.

    If *idempotency_scope* is None, the column is omitted so PG applies
    the column DEFAULT ('').  If the idempotency_scope column does not
    exist yet (pre-migration), it is always omitted.
    """
    if idempotency_scope is not None:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, "
            f"idempotency_scope, idempotency_key) "
            f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)",
            new_uuid(),
            "direct_actor",
            "default",
            "{}",
            3,
            "transient",
            datetime.now(UTC),
            idempotency_scope,
            idempotency_key,
        )
    else:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, "
            f"idempotency_key) "
            f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)",
            new_uuid(),
            "direct_actor",
            "default",
            "{}",
            3,
            "transient",
            datetime.now(UTC),
            idempotency_key,
        )


# ── Migration correctness: fresh schema ────────────────────────


class TestMigrationCorrectness:
    """After applying all migrations, the schema has the correct
    idempotency_scope column and composite unique index."""

    async def test_idempotency_scope_column_exists_not_null_default_empty(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        rows = await pg_conn.fetch(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = $1
                AND table_name = 'jobs'
                AND column_name = 'idempotency_scope'
            """,
            settings.schema_name,
        )
        assert len(rows) == 1, "idempotency_scope column missing from jobs"
        col = rows[0]
        assert col["data_type"] == "text"
        assert col["is_nullable"] == "NO"
        assert col["column_default"] == "''::text"

    async def test_old_single_column_index_gone(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is None, "old single-column unique index should have been dropped"

    async def test_new_composite_index_exists(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        row = await pg_conn.fetchrow(
            """
            SELECT indexdef FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_scope_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is not None, "composite unique index should exist"
        definition: str = row["indexdef"]
        assert "idempotency_scope" in definition
        assert "idempotency_key" in definition
        assert "UNIQUE" in definition
        assert "idempotency_key IS NOT NULL" in definition

    async def test_default_scope_same_key_raises_unique_violation(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Two rows inserted with the DEFAULT scope ('') and the same
        idempotency_key must raise UniqueViolationError — the critical
        regression check for the NULL-distinctness trap."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        key = "migration-correctness-default-scope"
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

    async def test_different_scopes_same_key_allowed(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Two rows with different idempotency_scope values and the same
        idempotency_key should both succeed."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        key = "migration-correctness-diff-scopes"
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-A"
        )
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-B"
        )

    async def test_null_idempotency_key_allows_duplicates(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """NULL idempotency_key rows should not conflict regardless of scope."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=None)
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=None)


# ── Migration upgrade path: pre-existing data ──────────────────


class TestMigrationUpgradePath:
    """Apply migrations up through 01.00.01 (before idempotency_scope),
    insert a job row with idempotency_key, then apply remaining migrations
    (including 01.00.03) and verify the existing row is backfilled with
    idempotency_scope = '' and the composite unique index still enforces
    dedupe for the pre-existing row.
    """

    async def test_pre_existing_row_gets_default_scope(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        # 1. Apply migrations up through 01.00.01_01 (before idempotency_scope)
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, target="01.00.01_01")

        # 2. Insert a job row with idempotency_key set (no idempotency_scope column yet)
        key = "upgrade-path-pre-existing"
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

        # 3. Apply the remaining migrations (01.00.02_01 + 01.00.03_01 pre + post)
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert len(applied) >= 1
        applied_versions = {m.version for m in applied}
        assert "01.00.03_01" in applied_versions

        # 4. Assert the existing row now has idempotency_scope = ''
        row = await pg_conn.fetchrow(
            f'SELECT idempotency_scope FROM "{settings.schema_name}".jobs '
            f"WHERE idempotency_key = $1",
            key,
        )
        assert row is not None
        assert row["idempotency_scope"] == ""

    async def test_pre_existing_row_still_dedupes_after_upgrade(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        # 1. Apply up through 01.00.01_01
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, target="01.00.01_01")

        # 2. Insert a pre-existing row with idempotency_key
        key = "upgrade-path-dedup"
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

        # 3. Apply the new migration
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        # 4. Inserting a second row with the same key and no explicit scope
        #    (defaults to '') should raise UniqueViolationError
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

    async def test_old_index_gone_after_upgrade(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        # 1. Apply up through 01.00.01_01
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, target="01.00.01_01")

        # 2. Verify old index exists before the new migration
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is not None, "old index should exist before 01.00.03"

        # 3. Apply the new migration
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        # 4. Verify old index is gone
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is None, "old index should be gone after 01.00.03"

        # 5. Verify new composite index exists
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_scope_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is not None, "composite index should exist after 01.00.03"


# ── Rolling-deploy overlap window: pre phase only ──────────────
#
# The moment `taskq migrate up --phase pre` runs, EVERY worker's schema is
# in the "pre applied, post not yet applied" state — including workers
# still running the code that shipped before this feature. This class
# proves that state is safe for that old code, which is the entire
# point of splitting the migration into pre + post phases instead of
# shipping a single migration that both adds the column and drops the old
# index.


async def _insert_job_old_shape(
    conn: asyncpg.Connection,
    schema: str,
    *,
    idempotency_key: str | None,
) -> None:
    """Issue the EXACT INSERT statement pre-this-release code used:
    no idempotency_scope column reference at all (that code doesn't know
    the column exists), and — critically — the single-column
    `ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL`
    target, which only resolves against `jobs_idempotency_key_uniq`.
    """
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, idempotency_key) "
        f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8) "
        f"ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING",
        new_uuid(),
        "direct_actor",
        "default",
        "{}",
        3,
        "transient",
        datetime.now(UTC),
        idempotency_key,
    )


class TestPrePhaseOverlapWindow:
    """After `taskq migrate up --phase pre` only (the state during a
    rolling deploy, before the post phase drops the old index), both
    indexes coexist and pre-this-release code's exact SQL shape keeps
    working unmodified."""

    async def test_old_shape_on_conflict_still_resolves_after_pre_only(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """This is the assertion that proves the overlap window is safe:
        pre-this-release code's `ON CONFLICT (idempotency_key)` — which
        can only resolve against a unique index whose column set is
        EXACTLY `(idempotency_key)` — must still find a matching index
        and succeed, not raise
        "there is no unique or exclusion constraint matching the
        ON CONFLICT specification"."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        key = "pre-phase-overlap-old-shape"
        # First insert: new row. Must not raise.
        await _insert_job_old_shape(pg_conn, settings.schema_name, idempotency_key=key)
        # Second insert: same key, old code's ON CONFLICT DO NOTHING path.
        # Must still resolve against jobs_idempotency_key_uniq and no-op,
        # not fail with "no unique or exclusion constraint matching".
        await _insert_job_old_shape(pg_conn, settings.schema_name, idempotency_key=key)

        rows = await pg_conn.fetch(
            f'SELECT id FROM "{settings.schema_name}".jobs WHERE idempotency_key = $1',
            key,
        )
        assert len(rows) == 1, "ON CONFLICT DO NOTHING must have deduped, not inserted twice"

    async def test_both_indexes_coexist_after_pre_only(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        old_index = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert old_index is not None, "old index must still exist after pre-only"

        new_index = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_scope_key_uniq'
            """,
            settings.schema_name,
        )
        assert new_index is not None, "composite index must already exist after pre-only"

    async def test_scoped_dedupe_is_inert_until_post_applied(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """While only the pre phase is applied, the OLD index still
        enforces "idempotency_key unique across ALL scopes" — strictly
        stronger than the new composite constraint — so the same key in
        different scopes still collides. The new scoped-dedupe behavior
        only activates once the post phase drops the old index."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        key = "pre-phase-scoped-dedupe-inert"
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-A"
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_job_raw(
                pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-B"
            )

    async def test_scoped_dedupe_activates_after_post_applied(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Once the post phase drops the old index, the same key in
        different scopes both succeed -- the feature's actual payoff."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        key = "post-phase-scoped-dedupe-active"
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-A"
        )

        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="post")

        # Now the same key in a different scope must succeed -- no longer
        # blocked by the (now-dropped) single-column index.
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-B"
        )


# ── Application enqueue path during the pre-only window ────────
#
# TestPrePhaseOverlapWindow above proves the raw-SQL shape stays safe.
# This class drives the real PostgresBackend.enqueue()/enqueue_batch()
# (this release's actual application code, not raw SQL) against a
# pre-phase-only schema -- the gap a HIGH finding fell through in an
# earlier review round: unscoped and same-scope usage must be
# unaffected, but cross-scope usage of a repeated key hits the
# still-present old global index and must raise a clear, typed error
# rather than crash on a raw asyncpg.UniqueViolationError.


class TestApplicationEnqueuePathDuringPreOnlyWindow:
    async def test_unscoped_enqueue_dedupes_safely(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-unscoped-pre-only"
            row1 = await backend.enqueue(make_enqueue_args(idempotency_key=key))
            row2 = await backend.enqueue(
                make_enqueue_args(idempotency_key=key, payload={"second": True})
            )
            assert row1.id == row2.id
        finally:
            await stack.aclose()

    async def test_same_scope_enqueue_dedupes_safely(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-same-scope-pre-only"
            row1 = await backend.enqueue(
                make_enqueue_args(idempotency_key=key, idempotency_scope="run-A")
            )
            row2 = await backend.enqueue(
                make_enqueue_args(
                    idempotency_key=key, idempotency_scope="run-A", payload={"second": True}
                )
            )
            assert row1.id == row2.id
        finally:
            await stack.aclose()

    async def test_cross_scope_enqueue_raises_typed_error_not_raw_driver_error(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """This is the assertion that closes the test-coverage gap: a
        repeated key under a DIFFERENT scope during the pre-only window
        must raise ScopedIdempotencyMigrationPendingError -- a documented,
        catchable, legible error -- not a raw asyncpg.UniqueViolationError
        that crashes the caller."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-cross-scope-pre-only"
            await backend.enqueue(make_enqueue_args(idempotency_key=key, idempotency_scope="run-A"))
            with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
                await backend.enqueue(
                    make_enqueue_args(idempotency_key=key, idempotency_scope="run-B")
                )
            assert exc_info.value.idempotency_key == key
            assert exc_info.value.idempotency_scope == "run-B"
            # The raw asyncpg error must be chained, not swallowed.
            assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)
        finally:
            await stack.aclose()

    async def test_scoped_first_then_unscoped_raises_typed_error(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """The reverse direction of the cross-scope case: a key first
        written under a non-default scope, then reused by an UNSCOPED
        call, hits the legacy global index identically -- the trigger is
        the key existing under a different scope, not the caller passing
        idempotency_scope. (Before the message fix, the raised error said
        "idempotency_scope was used" while printing idempotency_scope=''
        on the same line; this test also pins the corrected message.)"""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-scoped-then-unscoped-pre-only"
            await backend.enqueue(make_enqueue_args(idempotency_key=key, idempotency_scope="run-A"))
            with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
                await backend.enqueue(make_enqueue_args(idempotency_key=key))
            assert exc_info.value.idempotency_key == key
            assert exc_info.value.idempotency_scope == ""
            assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)
            message = str(exc_info.value)
            # The message must report the call's actual (empty) scope and
            # must not claim the caller "used" idempotency_scope.
            assert "idempotency_scope=''" in message
            assert "idempotency_scope was used" not in message
            assert key in message
            # It must name the corrective action for the on-call operator.
            assert "taskq migrate up --phase post" in message
        finally:
            await stack.aclose()

    async def test_enqueue_batch_fast_cross_scope_raises_typed_error(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """The COPY path translates the legacy-index violation too: during
        the pre-only window, a batch-fast item whose bare key already
        exists under a different scope must surface
        ScopedIdempotencyMigrationPendingError -- the same typed error as
        every other enqueue path -- not a raw asyncpg.UniqueViolationError.
        The all-or-nothing contract is unchanged: no rows are written."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-batch-fast-cross-scope-pre-only"
            await backend.enqueue(make_enqueue_args(idempotency_key=key, idempotency_scope="run-A"))
            with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
                await backend.enqueue_batch_fast(
                    [
                        make_enqueue_args(idempotency_key="app-path-batch-fast-fresh-key"),
                        make_enqueue_args(idempotency_key=key, idempotency_scope="run-B"),
                    ]
                )
            assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)
            # Nothing from the aborted COPY persisted.
            remaining = await pg_conn.fetchval(
                f'SELECT count(*) FROM "{settings.schema_name}".jobs WHERE idempotency_key = $1',
                "app-path-batch-fast-fresh-key",
            )
            assert remaining == 0
        finally:
            await stack.aclose()

    async def test_enqueue_batch_cross_scope_collision_raises_typed_error(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """A single cross-scope collision inside enqueue_batch aborts the
        whole batch statement (Postgres gives no cheaper per-item
        attribution) -- must still surface as the typed error, not a raw
        driver exception."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-batch-cross-scope-pre-only"
            await backend.enqueue(make_enqueue_args(idempotency_key=key, idempotency_scope="run-A"))
            with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
                await backend.enqueue_batch(
                    [
                        make_enqueue_args(idempotency_key="app-path-batch-fresh-key"),
                        make_enqueue_args(idempotency_key=key, idempotency_scope="run-B"),
                    ]
                )
            assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)
        finally:
            await stack.aclose()

    async def test_enqueue_batch_same_scope_dedupes_safely(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """A same-scope repeated idempotency_key inside a batch must
        dedupe cleanly against the composite index even while the old
        global index still exists -- the legacy index must not fire."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-batch-same-scope-pre-only"
            rows = await backend.enqueue_batch(
                [
                    make_enqueue_args(idempotency_key=key, idempotency_scope="run-A"),
                    make_enqueue_args(
                        idempotency_key=key,
                        idempotency_scope="run-A",
                        payload={"second": True},
                    ),
                ]
            )
            assert len(rows) == 2
            assert rows[0].id == rows[1].id
        finally:
            await stack.aclose()

    async def test_enqueue_batch_unscoped_dedupes_safely(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """An unscoped repeated idempotency_key inside a batch must
        dedupe cleanly against the composite index even while the old
        global index still exists -- the legacy index must not fire."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-batch-unscoped-pre-only"
            rows = await backend.enqueue_batch(
                [
                    make_enqueue_args(idempotency_key=key),
                    make_enqueue_args(idempotency_key=key, payload={"second": True}),
                ]
            )
            assert len(rows) == 2
            assert rows[0].id == rows[1].id
        finally:
            await stack.aclose()

    async def test_enqueue_batch_cross_scope_collision_within_batch_raises_typed_error(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """A cross-scope collision entirely within one batch (no
        pre-existing row) must still surface as the typed migration-pending
        error, not a raw driver exception."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-batch-cross-scope-within-batch-pre-only"
            with pytest.raises(ScopedIdempotencyMigrationPendingError) as exc_info:
                await backend.enqueue_batch(
                    [
                        make_enqueue_args(idempotency_key=key, idempotency_scope="run-A"),
                        make_enqueue_args(idempotency_key=key, idempotency_scope="run-B"),
                    ]
                )
            assert isinstance(exc_info.value.__cause__, asyncpg.UniqueViolationError)
        finally:
            await stack.aclose()

    async def test_cross_scope_enqueue_succeeds_after_post_applied(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """The same cross-scope pattern that raised above must succeed
        once the post migration has run -- proving the typed error is
        purely a transitional-window signal, not a permanent limitation."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "app-path-cross-scope-post-applied"
            row1 = await backend.enqueue(
                make_enqueue_args(idempotency_key=key, idempotency_scope="run-A")
            )

            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="post")

            row2 = await backend.enqueue(
                make_enqueue_args(idempotency_key=key, idempotency_scope="run-B")
            )
            assert row1.id != row2.id
        finally:
            await stack.aclose()


# ── Concurrent rolling-deploy overlap: old + new code hammered ──
#
# TestPrePhaseOverlapWindow and TestApplicationEnqueuePathDuringPreOnlyWindow
# are sequential. This class simulates the actual deploy: old-code writers
# (the exact pre-release INSERT shape, on raw connections standing in for
# not-yet-upgraded workers) and new-code writers (PostgresBackend.enqueue)
# firing concurrently against the same pre-only schema, then again after
# the post phase. Asserts the window's three invariants: no duplicate job,
# no lost job, and ScopedIdempotencyMigrationPendingError exactly on
# cross-scope reuse and nowhere else.


async def _insert_job_old_shape_returning_status(
    conn: asyncpg.Connection,
    schema: str,
    *,
    idempotency_key: str | None,
) -> str:
    """Like _insert_job_old_shape but returns the command status
    (``INSERT 0 1`` = row won the race, ``INSERT 0 0`` = deduped)."""
    return await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, idempotency_key) "
        f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8) "
        f"ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING",
        new_uuid(),
        "direct_actor",
        "default",
        "{}",
        3,
        "transient",
        datetime.now(UTC),
        idempotency_key,
    )


async def _count_jobs_by_key(
    conn: asyncpg.Connection, schema: str, key: str, scope: str | None = None
) -> int:
    if scope is None:
        return int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE idempotency_key = $1', key
            )
        )
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            f"WHERE idempotency_scope = $1 AND idempotency_key = $2",
            scope,
            key,
        )
    )


class TestConcurrentOverlapWindow:
    """Old-code and new-code writers racing on a pre-only schema."""

    async def test_concurrent_old_and_new_unscoped_same_key_exactly_one_job(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """The core no-outage invariant under concurrency: old-code
        INSERTs and new-code enqueues with the SAME unscoped key,
        interleaved arbitrarily, must produce exactly one row.

        New-code callers must NEVER see an error for this same-pair race:
        if the legacy non-arbiter index reports the in-flight conflict to
        the new-code INSERT, the backend retries once on a fresh
        transaction and dedupes via the composite arbiter (see
        _LegacyIdempotencyKeyConflictError). Old-code callers, unfixable
        retroactively, may see a raw UniqueViolationError from the
        composite index in the same race -- a documented transitional
        hazard of the window (see the RESIDUAL RISK note in the pre
        migration)."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        old_pool = await asyncpg.create_pool(str(settings.pg_dsn), min_size=4, max_size=8)
        try:
            key = "overlap-concurrent-unscoped"

            async def old_write() -> str | asyncpg.UniqueViolationError:
                try:
                    async with old_pool.acquire() as conn:
                        return await _insert_job_old_shape_returning_status(
                            conn, settings.schema_name, idempotency_key=key
                        )
                except asyncpg.UniqueViolationError as exc:
                    return exc

            async def new_write() -> str:
                row = await backend.enqueue(make_enqueue_args(idempotency_key=key))
                return str(row.id)

            results = await asyncio.gather(
                *(old_write() for _ in range(6)),
                *(new_write() for _ in range(6)),
            )
            # Exactly one row survives; no job duplicated, none lost.
            assert await _count_jobs_by_key(pg_conn, settings.schema_name, key) == 1

            old_results = results[:6]
            new_results = results[6:]
            # New code: every call succeeded and returned the surviving row.
            assert len(set(new_results)) == 1
            # Old code: deduped, inserted, or (transitionally) crashed on
            # the composite non-arbiter index -- at most one actual insert.
            statuses = [r for r in old_results if isinstance(r, str)]
            assert statuses.count("INSERT 0 1") <= 1
            for r in old_results:
                if isinstance(r, asyncpg.UniqueViolationError):
                    assert r.constraint_name == "jobs_idempotency_scope_key_uniq"
        finally:
            await old_pool.close()
            await stack.aclose()

    async def test_concurrent_new_code_same_scope_same_key_exactly_one_job(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Same scope + same key hammered concurrently through the real
        backend during the window: exactly one job, every caller gets its
        id, no ScopedIdempotencyMigrationPendingError (same scope never
        trips the legacy index)."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "overlap-concurrent-same-scope"
            scope = "run-A"
            rows = await asyncio.gather(
                *(
                    backend.enqueue(make_enqueue_args(idempotency_key=key, idempotency_scope=scope))
                    for _ in range(8)
                )
            )
            assert len({str(r.id) for r in rows}) == 1
            assert await _count_jobs_by_key(pg_conn, settings.schema_name, key, scope) == 1
        finally:
            await stack.aclose()

    async def test_concurrent_cross_scope_exactly_one_survives_rest_raise_typed(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Same key, two different scopes, hammered concurrently during
        the window: the legacy index serializes the race -- exactly one
        scope's row survives, every losing enqueue raises
        ScopedIdempotencyMigrationPendingError (never a raw driver error,
        never a silent wrong-scope return), and every enqueue for the
        surviving scope dedupes onto the one row."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "overlap-concurrent-cross-scope"

            async def scoped_write(scope: str) -> JobRow | ScopedIdempotencyMigrationPendingError:
                try:
                    return await backend.enqueue(
                        make_enqueue_args(idempotency_key=key, idempotency_scope=scope)
                    )
                except ScopedIdempotencyMigrationPendingError as exc:
                    return exc

            results = await asyncio.gather(
                *(scoped_write("run-A") for _ in range(5)),
                *(scoped_write("run-B") for _ in range(5)),
            )
            rows = [r for r in results if isinstance(r, JobRow)]
            errors = [r for r in results if isinstance(r, ScopedIdempotencyMigrationPendingError)]

            # Exactly one row total across both scopes; no job lost, none duplicated.
            assert await _count_jobs_by_key(pg_conn, settings.schema_name, key) == 1
            # Every non-raising call returned the same surviving row.
            assert len({str(r.id) for r in rows}) == 1 if rows else True
            assert rows, "at least the winning scope's first enqueue must succeed"
            # Every failure is the typed migration-pending error -- and all
            # of them name the LOSING scope (the one whose insert lost the race).
            assert errors, "cross-scope race during pre-only window must raise"
            for exc in errors:
                assert isinstance(exc, ScopedIdempotencyMigrationPendingError)
                assert isinstance(exc.__cause__, asyncpg.UniqueViolationError)
            losing_scopes = {e.idempotency_scope for e in errors}
            assert len(losing_scopes) == 1
            surviving_scope = next(iter({"run-A", "run-B"} - losing_scopes))
            assert all(r.idempotency_scope == surviving_scope for r in rows)
        finally:
            await stack.aclose()

    async def test_losing_scope_enqueue_succeeds_once_post_applied(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """End-to-end window exit: after the race above, applying the post
        phase lets the previously-losing scope enqueue the same key --
        the window's error was transitional, not a lost job."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre")

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            key = "overlap-window-exit"
            row_a = await backend.enqueue(
                make_enqueue_args(idempotency_key=key, idempotency_scope="run-A")
            )
            with pytest.raises(ScopedIdempotencyMigrationPendingError):
                await backend.enqueue(
                    make_enqueue_args(idempotency_key=key, idempotency_scope="run-B")
                )

            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="post")

            row_b = await backend.enqueue(
                make_enqueue_args(idempotency_key=key, idempotency_scope="run-B")
            )
            assert row_a.id != row_b.id
            assert await _count_jobs_by_key(pg_conn, settings.schema_name, key) == 2
        finally:
            await stack.aclose()


# ── Post-phase: pre-release code is now hard-broken ────────────
#
# The post migration's header documents this as the reason the post phase
# must wait for a fully-upgraded fleet. These tests lock the claim in
# executable form: after the post phase, the old INSERT shape fails with
# SQLSTATE 42P10 -- for keyed AND unkeyed inserts alike, because Postgres
# resolves the ON CONFLICT arbiter statically at plan time.


class TestPostPhaseOldCodeFails:
    async def test_old_shape_keyed_insert_fails_after_post(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        with pytest.raises(asyncpg.InvalidColumnReferenceError) as exc_info:
            await _insert_job_old_shape(pg_conn, settings.schema_name, idempotency_key="k1")
        assert exc_info.value.sqlstate == "42P10"

    async def test_old_shape_null_key_insert_also_fails_after_post(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Even a NULL-key old-shape insert fails: the ON CONFLICT clause
        is in the statement regardless of row values and its arbiter index
        is gone. This is why the post phase is gated on full fleet
        upgrade, not just on 'no keyed enqueues in flight'."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        with pytest.raises(asyncpg.InvalidColumnReferenceError) as exc_info:
            await _insert_job_old_shape(pg_conn, settings.schema_name, idempotency_key=None)
        assert exc_info.value.sqlstate == "42P10"

    async def test_new_code_unaffected_after_post(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        stack, _deps, backend = await _open_pg_backend_on_schema(
            str(settings.pg_dsn), settings.schema_name
        )
        try:
            row = await backend.enqueue(make_enqueue_args(idempotency_key="post-new-code"))
            assert row.id is not None
        finally:
            await stack.aclose()


# ── Phase-ordering guard ───────────────────────────────────────
#
# 01.00.03_01:post is the first post-phase migration this project ships,
# so the runner's phase-ordering semantics are this feature's
# responsibility. Applying a post migration before its same-version pre
# counterpart would (a) drop the old idempotency index out from under
# not-yet-upgraded workers immediately and (b) record the post as applied
# so a later plain `migrate up` reports 'no pending migrations' with the
# overlap protection never having existed. The runner refuses.


class TestPhaseOrderingGuard:
    async def test_post_refused_before_pre_on_existing_schema(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """An existing deployment (migrated through 01.00.01) whose
        operator runs `--phase post` first must get a loud error, and the
        old index must survive untouched."""
        schema = settings.schema_name
        await migrate_mod.apply_pending(pg_conn, schema=schema, target="01.00.01_01")

        with pytest.raises(ValueError, match="cannot be applied before its pre-phase"):
            await migrate_mod.apply_pending(pg_conn, schema=schema, phase="post")

        # Nothing recorded, nothing dropped.
        applied = await migrate_mod.list_applied(pg_conn, schema)
        assert "01.00.03_01:post" not in applied
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            schema,
        )
        assert row is not None, "guard must have prevented the index drop"

        # The documented sequence still works afterwards.
        await migrate_mod.apply_pending(pg_conn, schema=schema, phase="pre")
        await migrate_mod.apply_pending(pg_conn, schema=schema, phase="post")
        applied = await migrate_mod.list_applied(pg_conn, schema)
        assert "01.00.03_01:pre" in applied
        assert "01.00.03_01:post" in applied

    async def test_post_refused_on_fresh_schema(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        with pytest.raises(ValueError, match="cannot be applied before its pre-phase"):
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="post")

    async def test_plain_up_applies_pre_before_post_in_one_run(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """The guard must not reject the normal single-run path: a plain
        `migrate up` on a fresh schema applies 01.00.03_01:pre and
        01.00.03_01:post in that order."""
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        keys = [m.key for m in applied]
        assert keys.index("01.00.03_01:pre") < keys.index("01.00.03_01:post")


# ── Re-run idempotency ─────────────────────────────────────────


class TestMigrationReRunIdempotency:
    async def test_apply_pending_is_a_noop_when_nothing_pending(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name) == []
        assert (
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="pre") == []
        )
        assert (
            await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, phase="post")
            == []
        )

    async def test_rendered_sql_is_reentrant_against_lost_migration_record(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Every statement in both phase files must tolerate re-execution
        (IF NOT EXISTS / IF EXISTS guards) -- this is what makes a failed
        apply safe to retry and a manually-repaired schema_migrations
        table non-fatal."""
        schema = settings.schema_name
        await migrate_mod.apply_pending(pg_conn, schema=schema)

        for migration in migrate_mod.discover():
            if not migration.version.startswith("01.00.03"):
                continue
            # Re-executing the fully-applied SQL verbatim must not raise.
            await pg_conn.execute(migration.render(schema))
