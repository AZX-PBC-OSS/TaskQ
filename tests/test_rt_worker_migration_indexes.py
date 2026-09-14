"""Predicate pins for migration 01.00.06_01's three audit indexes.

The migration header documents each index's partial WHERE clause as
load-bearing: the active-status predicates keep terminal rows out of the
bulk-cancel key ranges (and out of index maintenance), and the
``worker_id IS NOT NULL`` predicate follows the RI-trigger convention
that lets the parameterized probe use the index. Existence and validity
are already pinned in tests/test_index_audit.py; what nothing pins is
that the indexes actually carry the DOCUMENTED predicates and key
columns — an index silently created unpartial, or keyed differently,
would pass the existence pin and serve none of the audited plans.

The pin is normalization-independent: each migration index's
``pg_get_indexdef`` (minus the name) is compared against a reference
index created in the same schema from the header's documented
definition, so planner-level text normalization cannot make the pin
brittle.
"""

from __future__ import annotations

from typing import cast

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62

pytestmark = pytest.mark.integration

#: (migration index name, table, documented definition body) — the body
#: is exactly what the migration header documents for each index, minus
#: the name the CREATE statement carries.
_AUDIT_INDEXES: tuple[tuple[str, str, str], ...] = (
    (
        "jobs_queue_active_idx",
        "jobs",
        "ON \"{schema}\".jobs (queue, id) WHERE status IN ('pending', 'scheduled')",
    ),
    (
        "jobs_actor_active_id_idx",
        "jobs",
        "ON \"{schema}\".jobs (actor, id) WHERE status IN ('pending', 'scheduled')",
    ),
    (
        "job_attempts_worker_id_idx",
        "job_attempts",
        'ON "{schema}".job_attempts (worker_id) WHERE worker_id IS NOT NULL',
    ),
)


async def _indexdef(conn: asyncpg.Connection, schema: str, index: str) -> str | None:
    row = await conn.fetchrow(
        """
        SELECT pg_get_indexdef(ix.indexrelid) AS ddl,
               (ix.indpred IS NOT NULL) AS partial,
               ix.indisvalid AS valid
        FROM pg_class i
        JOIN pg_index ix ON ix.indexrelid = i.oid
        JOIN pg_namespace n ON n.oid = i.relnamespace
        WHERE n.nspname = $1 AND i.relname = $2
        """,
        schema,
        index,
    )
    if row is None:
        return None
    assert row["partial"] is True, (
        f"{index} is not a PARTIAL index — the documented predicate is "
        "load-bearing (terminal rows must stay out of the key range)"
    )
    assert row["valid"] is True, f"{index} exists but is INVALID"
    # Strip the name/schema prefix so the comparison is about the body:
    # key columns, order, and the WHERE clause.
    return cast("str", row["ddl"]).split(" ON ", 1)[1]


async def test_audit_indexes_carry_the_documented_predicates_and_keys(
    pg_dsn: str,
) -> None:
    """Each migration index's definition (key columns + order + partial
    predicate) must equal the header-documented definition, compared
    against a reference index created from that documentation in the same
    schema."""
    schema = f"idx_pred_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)

        for name, table, documented in _AUDIT_INDEXES:
            ref_name = f"{name}_ref_pin"
            await conn.execute(
                f'CREATE INDEX "{ref_name}" '  # Why: schema and index names are this module's own throwaway identifiers; the body is a module constant.
                + documented.format(schema=schema)
            )
            try:
                migrated = await _indexdef(conn, schema, name)
                reference = await _indexdef(conn, schema, ref_name)
            finally:
                await conn.execute(f'DROP INDEX "{schema}"."{ref_name}"')

            assert migrated is not None, f"{name} missing after apply_pending"
            assert reference is not None, f"reference {ref_name} missing (test bug)"
            assert migrated == reference, (
                f"{name} does not match its documented definition "
                f"(key columns or predicate):\n  migrated: {migrated}\n  "
                f"documented: {reference}"
            )
            assert table in migrated
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_audit_indexes_present_after_full_migration_run(pg_dsn: str) -> None:
    """Companion to the predicate pin: a FRESH schema's full migration run
    (every previous migration, then 01.00.06_01, in ledger order) leaves
    all three indexes present, partial and valid — the apply-order path
    the pinned ledger test does not spell out."""
    schema = f"idx_full_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        applied = await migrate_mod.apply_pending(conn, schema=schema)
        applied_keys = [m.key for m in applied]
        assert "01.00.06_01:pre" in applied_keys, (
            "the audit migration must be part of a fresh schema's pending set; "
            f"applied: {applied_keys}"
        )
        # Every previous migration applied before it (ledger order is the
        # runner's contract; this pins the file's position in it).
        position = applied_keys.index("01.00.06_01:pre")
        assert position == len(applied_keys) - 1, (
            "01.00.06_01 must apply LAST on a fresh schema (it depends on the "
            "tables every earlier migration creates)"
        )
        for name, _table, _documented in _AUDIT_INDEXES:
            ddl = await _indexdef(conn, schema, name)
            assert ddl is not None, f"{name} missing after the full run"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
