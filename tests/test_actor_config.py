"""Tests for the ActorConfig carrier dataclass."""

from dataclasses import FrozenInstanceError

import pytest

from taskq.worker.actor_config import ActorConfig

# ── Construction ───────────────────────────────────────────────────────────


def test_actor_config_construction_no_metadata() -> None:
    """ActorConfig constructs with required fields and default metadata."""
    cfg = ActorConfig(actor="my_actor", max_concurrent=5, queue="default")
    assert cfg.actor == "my_actor"
    assert cfg.max_concurrent == 5
    assert cfg.queue == "default"
    assert cfg.metadata == {}


def test_actor_config_construction_with_metadata() -> None:
    """ActorConfig constructs with an explicit metadata dict."""
    meta: dict[str, object] = {"owner": "team-a", "priority": "high"}
    cfg = ActorConfig(actor="my_actor", max_concurrent=None, queue="critical", metadata=meta)
    assert cfg.actor == "my_actor"
    assert cfg.max_concurrent is None
    assert cfg.queue == "critical"
    assert cfg.metadata == {"owner": "team-a", "priority": "high"}


# ── Frozen dataclass invariant ─────────────────────────────────────────────


def test_actor_config_is_frozen() -> None:
    """Assignment to any field raises FrozenInstanceError."""
    cfg = ActorConfig(actor="my_actor", max_concurrent=5, queue="default")
    with pytest.raises(FrozenInstanceError):
        cfg.actor = "other"  # type: ignore[misc] # Why: pyright reports this as unreachable; we verify the runtime guard.


def test_actor_config_is_frozen_metadata() -> None:
    """Assignment to metadata field raises FrozenInstanceError (the dict ref itself is immutable)."""
    cfg = ActorConfig(actor="my_actor", max_concurrent=5, queue="default")
    with pytest.raises(FrozenInstanceError):
        cfg.metadata = {"new": "dict"}  # type: ignore[misc] # Why: pyright reports this as unreachable; we verify the runtime guard.


# ── Metadata-default isolation ─────────────────────────────────────────────


def test_metadata_default_fresh_per_instance() -> None:
    """Each instance gets its own empty dict — no shared-mutable-default bug."""
    a = ActorConfig(actor="a", max_concurrent=1, queue="q")
    b = ActorConfig(actor="b", max_concurrent=2, queue="q")
    a.metadata["tag"] = "x"
    assert "tag" not in b.metadata
    assert b.metadata == {}


# ── max_pending field ──────────────────────────────────────────────────────


def test_actor_config_max_pending_int_round_trips() -> None:
    """ActorConfig(max_pending=10,...) round-trips the field value."""
    cfg = ActorConfig(actor="a", max_concurrent=None, queue="q", max_pending=10)
    assert cfg.max_pending == 10


def test_actor_config_max_pending_default_is_none() -> None:
    """ActorConfig without max_pending defaults to None."""
    cfg = ActorConfig(actor="a", max_concurrent=None, queue="q")
    assert cfg.max_pending is None
