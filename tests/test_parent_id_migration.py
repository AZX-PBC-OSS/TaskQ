"""Migration pins for the ``jobs.parent_id`` column (LIB-2, issue #670).

The fan-out ledger's exact-count half stands on this column: children
carry the parent's job id, the pending-children aggregate reads it
through one partial index. These pins hold WITHOUT a database (the
file-content contract); the applied-to-PG behavior is pinned by the
integration legs in tests/test_children_count_parity.py.
"""

from __future__ import annotations

from pathlib import Path

from taskq.backend._sql_templates import COPY_FROM_COLUMNS

MIGRATION_DIR = Path("src/taskq/migrations")
MIGRATION_NAME = "01.00.23_01_pre_jobs_parent_id.sql"
# The index is SPLIT from the columns file (the single-lock-class
# law, the consolidation): ALTERs and CREATE INDEX are different
# lock classes and may not share a file.
INDEX_MIGRATION_NAME = "01.00.23_07_pre_jobs_parent_pending_idx.sql"


def _migration_sql() -> str:
    return (MIGRATION_DIR / MIGRATION_NAME).read_text()


def test_migration_file_exists_at_the_next_version() -> None:
    """The regime: {major.minor.patch}_{nn}_{pre|post}_{description}.sql, next in line."""
    assert (MIGRATION_DIR / MIGRATION_NAME).is_file()


def test_migration_adds_parent_id_to_jobs_and_archive() -> None:
    """Both sides of the mirrored pair get the column (the archive copy flows
    from COPY_FROM_COLUMNS; the column must exist on both tables)."""
    sql = _migration_sql()
    assert 'ALTER TABLE "{schema}".jobs ADD COLUMN parent_id uuid' in sql
    assert 'ALTER TABLE "{schema}".jobs_archive ADD COLUMN parent_id uuid' in sql


def test_migration_adds_no_foreign_key() -> None:
    """The binding constraint: parent_id is a PLAIN indexed column.

    An FK would (1) take a key-share lock on the parent row per child
    insert, (2) block a retention purge of a parent while children pend
    (or cascade-delete them), (3) fight TimescaleDB's self-referencing
    restrictions. Dangling parent_id (parent purged, children pending)
    is a defined, harmless state: the count never joins to the parent.
    """
    sql = _migration_sql().upper()
    assert "REFERENCES" not in sql


def test_migration_index_serves_the_pending_children_count() -> None:
    """The partial index repeats the count's quals verbatim (the 01.00.12_06
    doctrine: a partial index is only a candidate when the planner can
    prove its predicate from the query's own quals). The index lives in its
    OWN file since the consolidation (the single-lock-class law: ALTERs and
    CREATE INDEX are different lock classes, one file, one class)."""
    index_sql = (MIGRATION_DIR / INDEX_MIGRATION_NAME).read_text()
    assert "jobs_parent_pending_idx" in index_sql
    assert "WHERE status IN ('pending', 'scheduled') AND parent_id IS NOT NULL" in index_sql


def test_migration_creates_index_non_concurrently() -> None:
    """The runner wraps each file in a transaction; CREATE INDEX CONCURRENTLY
    cannot run inside one (the 01.00.12_06 ops note). Plain CREATE INDEX
    statements only, with the by-hand CONCURRENTLY guidance in the header
    comment."""
    index_sql = (MIGRATION_DIR / INDEX_MIGRATION_NAME).read_text()
    columns_sql = _migration_sql()
    statements = "\n".join(
        line for line in index_sql.splitlines() if not line.strip().startswith("--")
    )
    assert "CONCURRENTLY" not in statements
    assert (
        "maintenance window" in columns_sql or "maintenance window" in index_sql
    )  # the ops guidance is in one of the split files' headers


def test_copy_from_columns_carries_parent_id() -> None:
    """THE TRAILING LAW: parent_id is COPY_FROM_COLUMNS' LAST member.

    The list is the single source the archive CSVs and the enqueue COPY
    both derive from, and the record builder writes parent_id LAST (the
    ARITY pin's coherence — the batch record's trailing member matches
    the list's trailing member). The position DIED in the consolidation
    (the budget trio appended past it) and was RESTORED (93b255ff's
    resolution, carried here): the trio sits BEFORE parent_id, the
    archive-mirror parity intact. This pin holds the position — both
    omission-set comments cite it as the trailing law's enforcement;
    membership alone would let the trio's next append strand the
    builder's 40-wide record against a 39-column list again."""
    assert "parent_id" in COPY_FROM_COLUMNS
    assert COPY_FROM_COLUMNS[-1] == "parent_id"
