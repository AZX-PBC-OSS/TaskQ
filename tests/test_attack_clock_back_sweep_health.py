"""Attack pin: the clock jumps BACKWARDS while the worker runs (an NTP
correction, a VM live-migration, a hypervisor pause-replay) and the
sweep-stall health bound must survive it.

The bound: ``maintenance_health`` reports ``degraded`` when a leader sweep
has not completed for three whole ``sweep_interval``s ("sweep=X stalled
Ns"). Anchoring that elapsed-time comparison on the WALL clock
(``time.time()`` stamps and a ``time.time()`` read) lets a backward step
defeat it exactly: after the jump, ``time() - last_success`` is NEGATIVE
for the length of the jump (up to an hour), so a sweep that stopped
completing before the jump reads fresh on /ready for the entire catch-up
- the one monitoring plane an operator has for a dead leader sweep, muted
precisely while the clock event that usually accompanies the incident
(NTP correction, live-migration) is happening.

The mechanism fix: ``record_sweep_success`` stamps a MONOTONIC ledger
beside the wall one (the wall ledger keeps feeding the exported
``sweep_last_success_seconds`` gauge, whose contract is an absolute Unix
timestamp), and ``maintenance_health`` reads the monotonic ledger, which
cannot step backwards.

The tests reproduce the jump deterministically: the wall domain is
monkeypatched an hour into the past while the monotonic domain keeps
running, and the stall is simulated by ageing the ledger stamp exactly as
elapsed time would (the established pattern in
tests/test_rt_worker_health_view.py).
"""

from __future__ import annotations

import time

import pytest

import taskq.obs._otel as otel_mod
from taskq.settings import WorkerSettings
from taskq.worker.health import maintenance_health

_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"

_JUMP_SECONDS = 3600.0  # the NTP correction / live-migration step


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _PG_DSN, "TASKQ_SWEEP_INTERVAL": "10"})


@pytest.fixture
def fresh_ledgers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Swap both sweep-success ledgers for empty dicts, restored on teardown.

    The caches are process-wide singletons (the thread-safety pins in
    tests/test_rt_sweep_health_cache_thread_safety.py own that contract);
    replacement, never mutation, keeps the leak out of other tests.
    """
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})
    monkeypatch.setattr(otel_mod, "_sweep_success_monotonic_cache", {})
    monkeypatch.setattr(otel_mod, "_sweep_batch_size_cache", {})


class _BackwardWall:
    """A wall clock standing ``_JUMP_SECONDS`` in the past, monotonic intact.

    ``record_sweep_success`` resolves ``time`` through the ``_otel``
    module namespace, so stubbing ``otel_mod.time`` moves ONLY the wall
    domain the emitter stamps; ``time.monotonic()`` inside the stub is
    the real event-loop clock.
    """

    def __init__(self, wall_now: float) -> None:
        self._wall_now = wall_now

    def time(self) -> float:
        return self._wall_now

    def monotonic(self) -> float:
        return time.monotonic()


def test_backward_wall_jump_cannot_hide_a_stalled_sweep(
    monkeypatch: pytest.MonkeyPatch,
    fresh_ledgers: None,
) -> None:
    """THE attack: a sweep succeeds, the wall jumps back an hour, and the
    sweep then stops completing. The stalled view must still fire.

    On the wall-anchored implementation the health read computed
    ``time() - stamp``; with ``time()`` an hour behind the stamp the
    staleness is negative for the whole catch-up and the view reads
    healthy while the sweep is dead. The monotonic ledger's stamp keeps
    ageing through the jump, so the three-interval bound holds.
    """
    settings = _settings()

    # The last successful sweep, stamped while the wall was still honest.
    # A stalled sweep stamps nothing further: the process's wall then
    # steps back one hour (the clock event), and the stall continues.
    otel_mod.record_sweep_success("scheduled_to_pending")
    monkeypatch.setattr(otel_mod, "time", _BackwardWall(time.time() - _JUMP_SECONDS), raising=True)
    # The stall: age the monotonic stamp exactly as ten intervals of
    # elapsed time would, with the wall standing an hour in the past.
    otel_mod._sweep_success_monotonic_cache["scheduled_to_pending"] = (  # pyright: ignore[reportPrivateUsage]  # Why: ageing the just-written stamp, exactly as elapsed time would (the established pattern in test_rt_worker_health_view.py).
        time.monotonic() - 4 * settings.sweep_interval
    )

    view = maintenance_health(settings)

    assert view["degraded"] is True, (
        "a backward wall step muted the stalled-sweep view: the health "
        f"body reported {view['reasons']!r} for a sweep stalled four "
        "intervals - the staleness bound must be anchored on the "
        "monotonic domain, which cannot step backwards"
    )
    assert any(r.startswith("sweep=scheduled_to_pending stalled ") for r in view["reasons"]), (
        f"expected a stalled reason naming the sweep, got {view['reasons']!r}"
    )


def test_wall_jump_alone_does_not_false_degrade(
    monkeypatch: pytest.MonkeyPatch,
    fresh_ledgers: None,
) -> None:
    """The inverse direction: the jump alone must not manufacture a stall.

    A sweep completing right through the jump (its monotonic stamp fresh)
    stays healthy; the fix is a clock-domain correction, not a new way to
    page on an NTP correction that changed nothing.
    """
    settings = _settings()
    monkeypatch.setattr(otel_mod, "time", _BackwardWall(time.time() - _JUMP_SECONDS), raising=True)
    otel_mod.record_sweep_success("scheduled_to_pending")

    view = maintenance_health(settings)

    assert view["degraded"] is False, (
        f"a fresh monotonic stamp read degraded after a wall jump: {view['reasons']!r}"
    )
    assert view["reasons"] == []


def test_stall_visible_before_wall_catches_up(
    monkeypatch: pytest.MonkeyPatch,
    fresh_ledgers: None,
) -> None:
    """The catch-up window is the attack's payload: with the wall an hour
    behind, EVERY staleness reading on the wall ledger is negative until
    real time catches up. The monotonic view degrades the moment the
    real stall crosses the threshold, not an hour later."""
    settings = _settings()
    otel_mod.record_sweep_success("deadline_exceeded")
    monkeypatch.setattr(otel_mod, "time", _BackwardWall(time.time() - _JUMP_SECONDS), raising=True)

    # Under the threshold (one interval): healthy.
    otel_mod._sweep_success_monotonic_cache["deadline_exceeded"] = (  # pyright: ignore[reportPrivateUsage]  # Why: ageing the stamp as elapsed time would.
        time.monotonic() - 1 * settings.sweep_interval
    )
    assert maintenance_health(settings)["degraded"] is False

    # Across the threshold (four intervals): degraded, wall still behind.
    otel_mod._sweep_success_monotonic_cache["deadline_exceeded"] = (  # pyright: ignore[reportPrivateUsage]  # Why: same ageing pattern, past the threshold.
        time.monotonic() - 4 * settings.sweep_interval
    )
    view = maintenance_health(settings)
    assert view["degraded"] is True
    assert any(r.startswith("sweep=deadline_exceeded stalled ") for r in view["reasons"]), (
        f"expected a stalled reason naming the sweep, got {view['reasons']!r}"
    )


def test_both_ledgers_stamp_together(fresh_ledgers: None) -> None:
    """One emitter call stamps BOTH domains. A wall ledger that moved
    without its monotonic twin (or vice versa) desynchronises the gauge
    from the health view: the gauge would page promotion-stalled on a
    stamp the health view never saw, or the health view would read a
    stall the gauge's series cannot show."""
    otel_mod.record_sweep_success("expired_locks")

    assert set(otel_mod._sweep_success_cache) == {"expired_locks"}
    assert set(otel_mod._sweep_success_monotonic_cache) == {"expired_locks"}
    # The wall ledger keeps its absolute-timestamp contract (the exported
    # gauge's unit is a Unix timestamp); the monotonic ledger's stamp is
    # an event-loop-relative float, orders of magnitude below it.
    assert otel_mod._sweep_success_cache["expired_locks"] > 1_500_000_000.0
    assert otel_mod._sweep_success_monotonic_cache["expired_locks"] < 1_500_000_000.0


def test_clear_sweep_health_caches_releases_both_ledgers(fresh_ledgers: None) -> None:
    """Demotion releases authority over BOTH ledgers: a demoted process
    whose monotonic ledger survived the clear reads its own pre-demotion
    freshness on /ready forever (the exact frozen-stamp failure the
    demotion fix removed for the wall ledger)."""
    otel_mod.record_sweep_success("cron")

    otel_mod.clear_sweep_health_caches()

    assert otel_mod._sweep_success_cache == {}
    assert otel_mod._sweep_success_monotonic_cache == {}
