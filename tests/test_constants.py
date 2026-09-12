"""Unit tests for taskq.constants."""

import pytest

from taskq.constants import WAKE_CHANNEL_FMT, schema_lock_name, wake_channel

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


# ── schema_lock_name: the advisory-lock naming convention ────────────────
#
# The unqualified per-purpose constants (CRON_LOCK_NAME,
# MAINTENANCE_LEADER_LOCK_NAME) were replaced by schema_lock_name: advisory
# locks live in a per-database namespace, so a bare taskq:{purpose} was
# shared by every schema in the database and two schemas serialized (or one
# silently starved the other of leadership). The pin below keeps asserting
# the convention itself — every advisory lock TaskQ takes is named
# taskq:{purpose}:{schema} — for each purpose that takes one.


def test_schema_lock_name_cron() -> None:
    """The cron tick lock is schema-qualified: taskq:cron:<schema>."""
    assert schema_lock_name("cron", "x") == "taskq:cron:x"


def test_schema_lock_name_maintenance_leader() -> None:
    """The maintenance-leader election lock is schema-qualified."""
    assert schema_lock_name("maintenance_leader", "x") == "taskq:maintenance_leader:x"


def test_schema_lock_name_prune() -> None:
    """The prune sweep lock is schema-qualified."""
    assert schema_lock_name("prune", "x") == "taskq:prune:x"


def test_schema_lock_name_archive_expiry() -> None:
    """The archive-expiry sweep lock is schema-qualified."""
    assert schema_lock_name("archive_expiry", "x") == "taskq:archive_expiry:x"


def test_schema_lock_name_qualifies_per_schema() -> None:
    """Two schemas must not share a lock: the property the unqualified
    constants violated."""
    assert schema_lock_name("cron", "s1") != schema_lock_name("cron", "s2")
