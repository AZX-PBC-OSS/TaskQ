"""Load-time hardening for worker-identity settings (PR #51 doctrine).

Settings values that would crash — or hit opaque database errors — at
worker registration must fail at settings load time with a clean error
instead. Covers ``workgroup_instance`` (UUID), ``worker_label`` (NUL-free
text), and ``queues`` items (the canonical queue-name charset).
"""

import pytest
from dotenvmodel import ValidationError

from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
_UUID7 = "0197e4d1-4d2b-7a3c-8e9f-1a2b3c4d5e6f"


def _load(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings from a dict with sensible defaults.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── workgroup_instance: UUID at load time ───────────────────────────────


def test_workgroup_instance_non_uuid_rejected_at_load() -> None:
    """worker/run.py calls ``UUID(workgroup_instance)`` at registration; a
    non-UUID value must fail at load time with a clean error, not as a raw
    ValueError during worker registration."""
    with pytest.raises(ValidationError, match="workgroup_instance must be a valid UUID"):
        _load(TASKQ_WORKGROUP_INSTANCE="not-a-uuid")


def test_workgroup_instance_valid_uuid_loads() -> None:
    s = _load(TASKQ_WORKGROUP_INSTANCE=_UUID7)
    assert s.workgroup_instance == _UUID7


def test_workgroup_instance_default_is_none_and_loads() -> None:
    s = _load()
    assert s.workgroup_instance is None


# ── worker_label: NUL-free text ─────────────────────────────────────────


def test_worker_label_nul_rejected_at_load() -> None:
    """The label is bound directly as a text parameter in the registration
    INSERT; a NUL must fail at load time, not as an opaque asyncpg 22021
    at worker startup."""
    with pytest.raises(ValidationError, match="worker_label contains a NUL"):
        _load(TASKQ_WORKER_LABEL="bad\x00label")


def test_worker_label_valid_loads() -> None:
    s = _load(TASKQ_WORKER_LABEL="worker-eu-1")
    assert s.worker_label == "worker-eu-1"


# ── queues: canonical queue-name charset per item ───────────────────────


def test_queues_item_bad_charset_rejected_at_load() -> None:
    with pytest.raises(ValidationError, match=r"queues\[1\] must be a valid queue name"):
        _load(TASKQ_QUEUES="default,has space")


def test_queues_item_nul_rejected_at_load() -> None:
    """A NUL is outside the queue-name charset, so the same per-item rule
    rejects it — before it can reach the registration INSERT's text[]
    parameter as an opaque asyncpg 22021."""
    with pytest.raises(ValidationError, match=r"queues\[1\] must be a valid queue name"):
        _load(TASKQ_QUEUES="default,ba\x00d")


def test_queues_valid_names_load() -> None:
    s = _load(TASKQ_QUEUES="default,high_priority,q-1.2")
    assert s.queues == ["default", "high_priority", "q-1.2"]
