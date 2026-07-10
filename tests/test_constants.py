"""Unit tests for taskq.constants."""

import pytest

from taskq.constants import CRON_LOCK_NAME, WAKE_CHANNEL_FMT, wake_channel

# ── wake_channel happy-path formatting ───────────────────────────


def test_wake_channel_simple_schema() -> None:
    """wake_channel returns the formatted channel name for a valid schema."""
    assert wake_channel("public") == "taskq_wake_public"


def test_wake_channel_underscore_schema() -> None:
    """wake_channel accepts schemas starting with underscore."""
    assert wake_channel("_private") == "taskq_wake__private"


def test_wake_channel_with_digits() -> None:
    """wake_channel accepts schemas containing digits after the first char."""
    assert wake_channel("schema_v2") == "taskq_wake_schema_v2"


def test_wake_channel_uses_fmt_constant() -> None:
    """wake_channel output matches WAKE_CHANNEL_FMT.format(schema=...)."""
    schema = "taskq"
    assert wake_channel(schema) == WAKE_CHANNEL_FMT.format(schema=schema)


# ── wake_channel validation rejects invalid schemas ───────────────


def test_wake_channel_rejects_empty() -> None:
    """wake_channel raises ValueError on empty string."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel("")


def test_wake_channel_rejects_starts_with_digit() -> None:
    """wake_channel raises ValueError when schema starts with a digit."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel("1schema")


def test_wake_channel_rejects_hyphen() -> None:
    """wake_channel raises ValueError when schema contains hyphens."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel("my-schema")


def test_wake_channel_rejects_space() -> None:
    """wake_channel raises ValueError when schema contains spaces."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel("my schema")


def test_wake_channel_rejects_dotted() -> None:
    """wake_channel raises ValueError when schema contains dots."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel("my.schema")


def test_wake_channel_error_includes_value() -> None:
    """ValueError message includes the offending schema name."""
    with pytest.raises(ValueError, match="nope!") as exc_info:
        wake_channel("nope!")
    assert "nope!" in str(exc_info.value)


# ── WAKE_CHANNEL_FMT constant value ────────────────────────────


def test_wake_channel_fmt_value() -> None:
    """WAKE_CHANNEL_FMT is the expected template string."""
    assert WAKE_CHANNEL_FMT == "taskq_wake_{schema}"


# ── CRON_LOCK_NAME constant ──────────────────────────────────────────────


def test_cron_lock_name_value() -> None:
    """CRON_LOCK_NAME is the advisory lock name for cron scheduler."""
    assert CRON_LOCK_NAME == "taskq:cron"
