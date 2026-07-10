"""Unit tests for the NOTIFY trigger SQL migration.

Verifies the trigger SQL template contains the expected DDL statements
and that it renders correctly with schema name substitution.
"""

from importlib import resources


def _load_migration_sql(filename: str) -> str:
    package = resources.files("taskq.migrations")
    path = package / filename
    return path.read_text()


_MIGRATION_FILE = "01.00.00_01_pre_initial.sql"


def test_trigger_migration_file_exists() -> None:
    """The migration file for the NOTIFY trigger exists and is readable."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert len(sql) > 0


def test_trigger_sql_contains_create_function() -> None:
    """Migration contains CREATE OR REPLACE FUNCTION for notify_job_insert."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert 'CREATE OR REPLACE FUNCTION "{schema}".notify_job_insert()' in sql


def test_trigger_sql_contains_create_trigger() -> None:
    """Migration contains CREATE TRIGGER for tr_notify_job_insert."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert "CREATE TRIGGER tr_notify_job_insert" in sql


def test_trigger_sql_contains_pg_notify() -> None:
    """Migration contains pg_notify call with wake channel prefix."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert "pg_notify(" in sql
    assert "taskq_wake_" in sql


def test_trigger_sql_contains_when_clause() -> None:
    """Trigger has WHEN (NEW.status = 'pending') to filter non-pending INSERTs."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert "WHEN (NEW.status = 'pending')" in sql


def test_trigger_sql_renders_with_schema_substitution() -> None:
    """The SQL template renders correctly when {schema} is substituted."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    rendered = sql.replace("{schema}", "myapp")
    assert "myapp" in rendered
    assert '"myapp".notify_job_insert()' in rendered
    assert 'AFTER INSERT ON "myapp".jobs' in rendered
    assert "{schema}" not in rendered


def test_trigger_uses_tg_table_schema() -> None:
    """pg_notify uses TG_TABLE_SCHEMA for dynamic channel naming."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert "TG_TABLE_SCHEMA" in sql
    assert "taskq_wake_' || TG_TABLE_SCHEMA" in sql


def test_trigger_is_after_insert_on_jobs() -> None:
    """Trigger is AFTER INSERT ON jobs table."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert 'AFTER INSERT ON "{schema}".jobs' in sql


def test_trigger_is_for_each_row() -> None:
    """Trigger is FOR EACH ROW."""
    sql = _load_migration_sql(_MIGRATION_FILE)
    assert "FOR EACH ROW" in sql
