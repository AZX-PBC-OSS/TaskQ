"""Adversarial pins for ``maintenance_health`` and its authority model.

The degraded view reads the obs layer's module-level success/batch-size
caches — THIS process's stamps for the sweeps ITS leader loops ran. The
attacks here: the staleness boundary (three whole intervals, strictly
greater), the reduced-tier threshold reading the PASSED settings (not a
constant), the emitter↔reader pairing (``record_sweep_success`` mutates
the cache in place; the reader must see that), and the demotion path —
a demoted process keeps exporting frozen success stamps it no longer has
authority over, which is both a permanently-degraded health body and a
permanently-firing promotion-stalled alert from every ex-leader pod.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.obs import (
    _otel,  # pyright: ignore[reportPrivateUsage]  # Why: the caches under test are module-level singletons; monkeypatching them in place is the only seam the helper has (same access as tests/test_health_maintenance_degraded.py).
)
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker.deps import WorkerDeps
from taskq.worker.health import maintenance_health
from taskq.worker.leader import MaintenanceLeader

_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"


def _settings(**env_overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": _PG_DSN}
    data.update(env_overrides)
    return WorkerSettings.load_from_dict(data, validate=False)


def _patch_caches(
    monkeypatch: pytest.MonkeyPatch,
    success: dict[str, float],
    batch: dict[str, int],
) -> None:
    # Replace, never mutate: the real dicts are process-wide singletons.
    monkeypatch.setattr(_otel, "_sweep_success_cache", success)
    monkeypatch.setattr(_otel, "_sweep_batch_size_cache", batch)


# ── The staleness boundary: three WHOLE intervals, strictly greater ──────


def test_staleness_just_under_three_intervals_stays_healthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """2.9x sweep_interval of staleness is jitter/slow, not a stall —
    three whole intervals without a completion is the threshold, and the
    check is strictly-greater so exactly-three is still 'suspicion', not
    'stalled'. A large sweep interval (100 s) gives the boundary a 10 s
    test-execution margin, so the pin is deterministic without an
    injectable clock."""
    import time

    settings = _settings(TASKQ_SWEEP_INTERVAL="100")
    lagging = time.time() - 2.9 * settings.sweep_interval
    _patch_caches(monkeypatch, {"scheduled_to_pending": lagging}, {})

    view = maintenance_health(settings)

    assert view["degraded"] is False
    assert view["reasons"] == []


def test_staleness_just_over_three_intervals_degrades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """3.1x sweep_interval: three whole missed intervals — a stall, not a
    slow sweep — with the sweep named in the reason."""
    import time

    settings = _settings(TASKQ_SWEEP_INTERVAL="100")
    stalled = time.time() - 3.1 * settings.sweep_interval
    _patch_caches(monkeypatch, {"scheduled_to_pending": stalled}, {})

    view = maintenance_health(settings)

    assert view["degraded"] is True
    assert any(r.startswith("sweep=scheduled_to_pending stalled ") for r in view["reasons"]), (
        f"expected a stalled reason naming the sweep, got {view['reasons']!r}"
    )


# ── The reduced-tier threshold reads the PASSED settings ─────────────────


def test_reduced_tier_threshold_follows_the_configured_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator who raises ``event_writer_batch_size`` to 250 moves the
    reduced-tier threshold WITH it: a sweep reporting 249 is the reduced
    tier (249 < 250). A hardcoded threshold (the default 100) would call
    249 healthy — exactly the misreport this view exists to prevent."""
    import time

    settings = _settings(TASKQ_EVENT_WRITER_BATCH_SIZE="250")
    assert settings.event_writer_batch_size == 250
    _patch_caches(
        monkeypatch,
        {"scheduled_to_pending": time.time()},
        {"scheduled_to_pending": 249},
    )

    view = maintenance_health(settings)

    assert view["degraded"] is True, (
        "batch size 249 against a configured 250 is the reduced tier — the "
        "threshold must follow the setting, not a constant"
    )
    assert "sweep=scheduled_to_pending batch size degraded to 249" in view["reasons"]


def test_batch_size_equal_to_configured_is_not_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exactly the configured size is the full tier: the comparison is
    strictly-less, so equal must stay healthy."""
    import time

    settings = _settings(TASKQ_EVENT_WRITER_BATCH_SIZE="250")
    _patch_caches(
        monkeypatch,
        {"scheduled_to_pending": time.time()},
        {"scheduled_to_pending": 250},
    )

    view = maintenance_health(settings)

    assert view["degraded"] is False
    assert view["reasons"] == []


# ── Emitter↔reader pairing: in-place mutation must be visible ────────────


def test_reader_sees_in_place_success_stamps(monkeypatch: pytest.MonkeyPatch) -> None:
    """``record_sweep_success`` mutates the cache dict IN PLACE — the
    reader must observe that mutation. Guards against a refactor that
    rebinds the reader to a copy taken at import time (the stamps would
    silently stop reaching the health view and the staleness gauge)."""
    import time

    monkeypatch.setattr(_otel, "_sweep_success_cache", {})
    monkeypatch.setattr(_otel, "_sweep_batch_size_cache", {})
    settings = _settings(TASKQ_SWEEP_INTERVAL="1")

    # The emitter writes through the CURRENT module cache, in place.
    _otel.record_sweep_success("scheduled_to_pending")
    _otel._sweep_success_cache["scheduled_to_pending"] = (  # pyright: ignore[reportPrivateUsage]  # Why: ageing the just-written stamp in place, exactly as elapsed time would.
        time.time() - 10 * settings.sweep_interval
    )

    view = maintenance_health(settings)

    assert view["degraded"] is True, (
        "the health view did not see the emitter's in-place stamp — the "
        "reader and the writer are on different cache objects"
    )


# ── Demotion must release sweep-health authority ─────────────────────────


async def test_demotion_clears_sweep_health_stamps() -> None:
    """A demoted process stops running the leader sweeps, so its success
    stamps and batch sizes are numbers it no longer has any authority
    over — the same rationale the demotion path already applies to queue
    depth. Keeping them has two failure modes: this pod's health body
    reads ``degraded`` forever after an ordinary failover, and its frozen
    ``sweep_last_success_seconds`` series fires the promotion-stalled
    alert forever while the NEW leader promotes fine."""
    import time

    from taskq.obs import update_queue_depth_cache

    # Populate every leader-sampled cache the demotion path owns.
    _otel._sweep_success_cache["scheduled_to_pending"] = time.time() - 9999.0  # pyright: ignore[reportPrivateUsage]  # Why: populating the process singleton the demotion path must clear; restored in the finally below.
    _otel._sweep_batch_size_cache["scheduled_to_pending"] = 25  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    update_queue_depth_cache({"default": 3})
    try:
        deps = WorkerDeps(
            settings=_settings(),
            dispatcher_pool=None,  # pyright: ignore[reportArgumentType]  # Why: demotion touches no pool — the leader-owned conns are None.
            heartbeat_pool=None,  # pyright: ignore[reportArgumentType]
            worker_pool=None,  # pyright: ignore[reportArgumentType]
            notify_conn=None,
            leader_conn=None,
        )
        deps.is_leader.set()
        leader = MaintenanceLeader(
            deps,
            new_uuid(),
            cast("Backend", object()),
            clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        )

        await leader._close_leader_owned_conns()  # pyright: ignore[reportPrivateUsage]  # Why: driving the demotion path directly is the point of the test.

        assert deps.is_leader.is_set() is False
        assert not _otel._queue_depth_cache, "queue depth already clears on demotion"  # pyright: ignore[reportPrivateUsage]  # Why: reading the singleton the demotion path clears.
        assert _otel._sweep_success_cache == {}, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "a demoted process keeps frozen sweep-success stamps it no longer "
            "has authority over — its health body reads degraded forever and "
            "its frozen last-success series pages promotion-stalled forever"
        )
        assert _otel._sweep_batch_size_cache == {}  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    finally:
        _otel._sweep_success_cache.clear()  # pyright: ignore[reportPrivateUsage]  # Why: restoring the process singleton this test populated.
        _otel._sweep_batch_size_cache.clear()  # pyright: ignore[reportPrivateUsage]  # Why: see above.
        update_queue_depth_cache({})


async def test_demoted_process_health_is_not_degraded() -> None:
    """The operator-facing consequence: after demotion the health body
    must report the informational not-leader state, not a permanent
    'sweep stalled' degraded view — the stamp the demoted process kept
    was never its own to keep."""
    import time

    _otel._sweep_success_cache["cron"] = time.time() - 9999.0  # pyright: ignore[reportPrivateUsage]  # Why: a frozen stamp from this process's leadership tenure; restored in the finally below.
    try:
        deps = WorkerDeps(
            settings=_settings(),
            dispatcher_pool=None,  # pyright: ignore[reportArgumentType]  # Why: demotion touches no pool.
            heartbeat_pool=None,  # pyright: ignore[reportArgumentType]
            worker_pool=None,  # pyright: ignore[reportArgumentType]
            notify_conn=None,
            leader_conn=None,
        )
        deps.is_leader.set()
        leader = MaintenanceLeader(
            deps,
            new_uuid(),
            cast("Backend", object()),
            clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        )
        settings = _settings()

        await leader._close_leader_owned_conns()  # pyright: ignore[reportPrivateUsage]  # Why: driving the demotion path directly is the point of the test.
        view = maintenance_health(settings)

        assert view["degraded"] is False, (
            "a demoted process reports degraded maintenance forever from a "
            f"frozen stamp: {view['reasons']!r}"
        )
        assert view["reasons"] == ["no sweep has completed yet"]
    finally:
        _otel._sweep_success_cache.clear()  # pyright: ignore[reportPrivateUsage]  # Why: restoring the process singleton this test populated.


# ── A leader process that IS stalled stays degraded (the pin that the
# demotion fix must not weaken) ──────────────────────────────────────────


def test_stalled_leader_process_is_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A process whose OWN stamps went stale (it holds leadership, or
    nothing cleared them because it never demoted) stays degraded — the
    demotion authority fix must only release stamps on actual demotion,
    never mute a genuinely stalled process's view."""
    import time

    settings = _settings(TASKQ_SWEEP_INTERVAL="1")
    stalled = time.time() - 5 * settings.sweep_interval
    _patch_caches(monkeypatch, {"scheduled_to_pending": stalled}, {})

    view = maintenance_health(settings)

    assert view["degraded"] is True
