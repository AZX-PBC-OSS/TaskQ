"""Unit tests for DSN host extraction (no PG required)."""

from taskq._dsn import dsn_host


def test_dsn_host_extracts_hostname() -> None:
    assert dsn_host("postgresql://user:pass@db.example.com:5432/mydb") == "db.example.com"


def test_dsn_host_returns_unknown_on_garbage() -> None:
    assert dsn_host("not-a-dsn") == "unknown"
