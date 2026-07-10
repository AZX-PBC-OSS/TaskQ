"""Unit tests for shared SQL helpers and taskq.constants (no PG required)."""

import pytest

from taskq.backend._sql import parse_rowcount
from taskq.constants import WAKE_CHANNEL_FMT, wake_channel


def test_parse_rowcount_update() -> None:
    assert parse_rowcount("UPDATE 7") == 7


def test_parse_rowcount_insert() -> None:
    assert parse_rowcount("INSERT 0 1") == 1


def test_parse_rowcount_delete_zero() -> None:
    assert parse_rowcount("DELETE 0") == 0


# ── wake_channel happy-path formatting ────────────────────────


@pytest.mark.parametrize(
    "schema",
    ["taskq", "myschema", "_private", "schema123", "a1b2c3"],
)
def test_wake_channel_valid_schema(schema: str) -> None:
    """wake_channel returns correctly formatted name for valid schemas."""
    result = wake_channel(schema)
    assert result == f"taskq_wake_{schema}"
    assert result == WAKE_CHANNEL_FMT.format(schema=schema)


# ── WAKE_CHANNEL_FMT constant value ──────────────────────────


def test_wake_channel_fmt_constant() -> None:
    """WAKE_CHANNEL_FMT has the exact expected value."""
    assert WAKE_CHANNEL_FMT == "taskq_wake_{schema}"


# ── wake_channel rejects invalid schema identifiers ───────────


@pytest.mark.parametrize(
    "invalid_schema",
    [
        "",  # empty string
        "1bad",  # leading digit
        "bad-name",  # hyphen
        "bad name",  # space
        "bad.name",  # dot
        "bad!name",  # special char
        "bad@name",  # at-sign
    ],
)
def test_wake_channel_invalid_schema_raises(invalid_schema: str) -> None:
    """wake_channel raises ValueError for invalid schema identifiers."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel(invalid_schema)
