"""Unit tests for DSN host extraction (no PG required)."""

from taskq._dsn import dsn_host


def test_dsn_host_extracts_hostname() -> None:
    assert dsn_host("postgresql://user:pass@db.test.invalid:5432/mydb") == "db.test.invalid"


def test_dsn_host_returns_unknown_on_garbage() -> None:
    assert dsn_host("not-a-dsn") == "unknown"
