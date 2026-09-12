"""Adversarial pin: the reduced-batch degraded signal must not depend on
OTel export being enabled.

``maintenance_health`` reports "sweep batch size degraded" so orchestrators
surfacing the health body see a latched reduced-tier worker WITHOUT a
Prometheus/OTel pipeline — that independence is the whole point of the
health view. The emitter the production sweep path drives with the
effective tier is ``record_sweep_batch_size`` (called from
``PostgresBackend._run_bounded_sweep``). If that emitter no-ops under
``TASKQ_OTEL_ENABLED=False``, the batch-size cache is never populated,
``maintenance_health`` can report "stalled" but never "batch size
degraded" — half the degraded signal silently dies exactly when the
operator opted out of OTel.
"""

from __future__ import annotations

import pytest

import taskq.obs._otel as otel_mod
from taskq.settings import WorkerSettings
from taskq.worker.health import maintenance_health

_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _PG_DSN})


def test_batch_size_degraded_is_reported_when_otel_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production emitter path, with OTel off: a sweep reporting the
    latched reduced tier must still make ``maintenance_health`` degraded.

    Mirrors the production call order — ``record_sweep_success`` is what
    the leader loops call on a completed sweep, ``record_sweep_batch_size``
    is what ``_run_bounded_sweep`` calls with the effective tier. Success
    is fresh (the "stalled" branch stays quiet), so the degraded reason
    must come from the batch-size branch.
    """
    monkeypatch.setattr(otel_mod, "_otel_enabled", False)
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})
    monkeypatch.setattr(otel_mod, "_sweep_batch_size_cache", {})
    settings = _settings()

    otel_mod.record_sweep_success("expired_locks")
    # 25 = the latched reduced tier (event_writer_batch_size // divisor).
    otel_mod.record_sweep_batch_size("expired_locks", settings.event_writer_batch_size // 4)

    view = maintenance_health(settings)

    assert view["degraded"] is True, (
        "a sweep latched to the reduced batch tier must report degraded in "
        "the health body even with OTel export disabled — the health view "
        "is the orchestrator's Prometheus-free signal"
    )
    assert (
        f"sweep=expired_locks batch size degraded to {settings.event_writer_batch_size // 4}"
        in view["reasons"]
    ), f"expected the batch-size reason, got {view['reasons']!r}"


def test_full_batch_size_not_degraded_when_otel_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same production emitter path at the FULL tier stays clean with
    OTel off — the degradation signal is the reduced tier alone, not the
    mere presence of a batch-size sample."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", False)
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})
    monkeypatch.setattr(otel_mod, "_sweep_batch_size_cache", {})
    settings = _settings()

    otel_mod.record_sweep_success("expired_locks")
    otel_mod.record_sweep_batch_size("expired_locks", settings.event_writer_batch_size)

    view = maintenance_health(settings)

    assert view["degraded"] is False
    assert view["reasons"] == []
