"""The claim cursor: per-worker, per-queue memory of the last
successfully-claimed job id, the worker-side half of the MVCC-horizon
hygiene pair (the gauges are the predictor, this is the partial cure).

The failure mode (brandur.org/postgres-queues, re-run by PlanetScale's
2026 "Keeping a Postgres queue healthy"): a long or overlapping
transaction pins the MVCC horizon, VACUUM cannot reclaim, and the claim
query's B-tree scans degenerate through dead tuples - measured 15x
lock-time degradation, identical under SKIP LOCKED ("lifts the floor,
not the ceiling"). The cursor's claim predicate adds an id lower bound
(last successfully-claimed id, UUIDv7, time-ordered) so the seek lands
on live tuples instead of walking the dead zone the claims themselves
left behind.

Pinned here (fast tier, injected clock - no PG):

1. **the bound**: ``bound()`` returns the id advanced last for that queue;
   ``advance()`` keeps the maximum (a round's rows arrive in claim order,
   the cursor is the high-water mark).
2. **the jitter reset**: the bound cannot outlive the reset window - the
   ONE knob (``reset_seconds``), expired bounds are forgotten and the
   next advance re-arms a fresh jittered window. This is the pin that
   reds if the jitter is broken: a cursor that never expires is a
   selection predicate that never heals.
3. **per-queue isolation** (the store): each queue's bound is its own
   last-claimed id; advancing one queue never moves another queue's
   bound.
4. **the disable**: ``reset_seconds=0`` is the documented OFF switch -
   the cursor is inert (never bounds, never stores).
5. **the jitter bounds**: the effective window is the knob scaled by the
   injected jitter factor, so a fleet's resets desynchronize; the pin
   draws the factor extremes through and pins the resulting expiry to
   the [0.5x, 1.5x] envelope.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from taskq.backend._claim_cursor import CLAIM_CURSOR_RESET_SECONDS, ClaimCursor

if TYPE_CHECKING:
    from uuid import UUID


class _ManualClock:
    """Injected monotonic clock: tests move time explicitly."""

    def __init__(self) -> None:
        self.now: float = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _u(seed: int) -> UUID:
    import uuid

    return uuid.UUID(int=seed)


# ── the bound ──────────────────────────────────────────────────────────


def test_bound_returns_the_last_advanced_id() -> None:
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    assert cursor.bound("default") is None, (
        "a queue that has never claimed must produce no bound - an "
        "invented bound would exclude rows before the worker has any "
        "claim history to stand on"
    )

    first = _u(1)
    cursor.advance("default", first)
    assert cursor.bound("default") == first

    second = _u(2)
    cursor.advance("default", second)
    assert cursor.bound("default") == second, (
        "the bound is the high-water mark of successfully-claimed ids: "
        "after claiming id 2 the bound is id 2, not id 1"
    )


def test_advance_keeps_the_maximum_not_the_last_write() -> None:
    """A round's claimed rows are advanced per row; the cursor must end at
    the MAX claimed id, so an out-of-order advance cannot drag the bound
    backwards (a backwards bound re-admits already-claimed regions and
    silently widens every later scan)."""
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("default", _u(10))
    cursor.advance("default", _u(5))
    assert cursor.bound("default") == _u(10), (
        "advance(5) after advance(10) must not lower the bound: the "
        "cursor is a high-water mark, and a lower id arriving later is "
        "a skew insert, not a time reversal"
    )


# ── the jitter reset ───────────────────────────────────────────────────


def test_bound_cannot_outlive_the_reset_window() -> None:
    """THE JITTER PIN - the mutation target. The bound is forgotten once
    the reset window (the only knob) has elapsed, so a cursor stranded by
    any divergence between claim order and id order heals within one
    window. A cursor that never expires (the jitter broken) reds this."""
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("default", _u(1))
    clock.advance(59.9)
    assert cursor.bound("default") == _u(1), "the bound holds inside the window"

    clock.advance(0.2)  # 60.1s since the advance, window was 60s
    assert cursor.bound("default") is None, (
        "the bound must be FORGOTTEN once the reset window elapses - a "
        "cursor that outlives its window strands lower-id rows forever, "
        "turning the bounded-stranding trade into an unbounded one"
    )


def test_the_next_advance_rearms_a_fresh_window() -> None:
    """After a reset the cursor is empty until the next successful claim;
    that claim re-arms a full window (the reset is periodic, not one-shot)."""
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("default", _u(1))
    clock.advance(61.0)
    assert cursor.bound("default") is None  # first reset

    cursor.advance("default", _u(2))
    clock.advance(30.0)
    assert cursor.bound("default") == _u(2), (
        "the advance after a reset must re-arm the window: a reset that "
        "permanently disabled the cursor would silently turn the feature "
        "off after 60s"
    )
    clock.advance(31.0)
    assert cursor.bound("default") is None  # second reset, on schedule


def test_reset_is_periodic_not_extensible_by_claims() -> None:
    """Continuous claiming must not keep pushing the reset back: the window
    is armed at the advance that CREATED the entry, and expires on schedule
    no matter how many claims landed inside it. (A sliding window would let
    a hot queue's cursor live forever - the exact never-heals state pin 2
    forbids.)"""
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("default", _u(1))
    for i in range(2, 40):
        clock.advance(2.0)  # 78 seconds of continuous claiming in total
        cursor.advance("default", _u(i))
        if clock.now >= 60.0:
            assert cursor.bound("default") is None, (
                f"the cursor survived {clock.now:.0f}s of continuous claims - "
                "the reset window must expire on schedule, claims inside it "
                "may not extend it"
            )
            return
    pytest.fail("the reset never fired across 78s of continuous claiming")


# ── per-queue isolation (the store) ────────────────────────────────────


def test_advancing_one_queue_never_moves_another_queues_bound() -> None:
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("billing", _u(100))
    cursor.advance("email", _u(7))

    assert cursor.bound("email") == _u(7)
    assert cursor.bound("billing") == _u(100), (
        "the memory is per queue: billing's high-water mark must not become email's bound"
    )

    cursor.advance("billing", _u(200))
    assert cursor.bound("email") == _u(7), (
        "a later claim on billing must not move email's bound - queue "
        "cursors are independent stores"
    )


def test_queues_reset_independently() -> None:
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("billing", _u(100))
    clock.advance(30.0)
    cursor.advance("email", _u(7))
    clock.advance(31.0)  # billing's window (60s) elapsed; email's (30s) has not

    assert cursor.bound("billing") is None
    assert cursor.bound("email") == _u(7), (
        "the reset window is per queue from that queue's own first "
        "advance: email's fresh window must not be cut short by "
        "billing's older one"
    )


# ── the disable ────────────────────────────────────────────────────────


def test_reset_seconds_zero_disables_the_cursor() -> None:
    """``reset_seconds=0`` is the documented OFF switch: the cursor neither
    stores nor bounds, the claim path runs the naive shape. The A/B
    harness and opting-out operators both ride this."""
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=0.0, clock=clock, jitter=lambda: 1.0)

    cursor.advance("default", _u(1))
    clock.advance(0.01)
    assert cursor.bound("default") is None, (
        "a disabled cursor must never bound the claim query - the off "
        "switch must be total, not a very long window"
    )


# ── the jitter bounds ──────────────────────────────────────────────────


@pytest.mark.parametrize("factor,expected_expiry", [(0.5, 30.0), (1.0, 60.0), (1.5, 90.0)])
def test_the_effective_window_is_the_knob_times_the_jitter_factor(
    factor: float, expected_expiry: float
) -> None:
    """The reset interval is the knob SCALED by the jitter draw, so a
    fleet's resets do not synchronize (a fleet resetting in the same tick
    would all run the naive shape in the same round - the synchronized
    cost spike the jitter exists to spread). The envelope is [0.5x, 1.5x]."""
    clock = _ManualClock()
    cursor = ClaimCursor(reset_seconds=60.0, clock=clock, jitter=lambda: factor)

    cursor.advance("default", _u(1))
    clock.advance(expected_expiry - 0.1)
    assert cursor.bound("default") == _u(1), (
        f"jitter factor {factor} must arm a {expected_expiry}s window: the "
        "bound held 0.1s short of it"
    )
    clock.advance(0.2)
    assert cursor.bound("default") is None, (
        f"jitter factor {factor} must arm a {expected_expiry}s window: the "
        "bound expired just past it"
    )


def test_the_shipped_default_reset_is_the_documented_constant() -> None:
    """The MECHANISM's cadence is the documented 60s constant (the class
    default; the stranding bound the runbook quotes is this number). The
    SETTINGS default is 0 - the opt-in posture: the A/B harness measured
    the cursor's full-path win as plan-dependent on this codebase's
    multi-surface claim CTE, so the bounded-stranding trade ships OFF
    (see the settings field's description and the perf-evidence record);
    the mechanism stays at brandur's cadence for the fleets that opt in."""
    assert CLAIM_CURSOR_RESET_SECONDS == 60.0
    clock = _ManualClock()
    cursor = ClaimCursor(clock=clock, jitter=lambda: 1.0)
    cursor.advance("default", _u(1))
    clock.advance(59.9)
    assert cursor.bound("default") == _u(1), (
        "the class default must be the documented 60s constant: the bound held through 59.9s"
    )


def test_the_settings_default_is_the_opt_in_posture() -> None:
    """DEFAULTS row: the cursor is OPT-IN (TASKQ_CLAIM_CURSOR_RESET_SECONDS
    defaults 0 = disabled). The predictor (the degradation-ratio gauges)
    is default-on; the cure waits for evidence on the adopting fleet."""
    import os

    from taskq.settings import WorkerSettings

    saved = {k: v for k, v in os.environ.items() if k.startswith("TASKQ_")}
    try:
        os.environ.pop("TASKQ_CLAIM_CURSOR_RESET_SECONDS", None)
        settings = WorkerSettings.load(read_dotfiles=False)
        default = settings.claim_cursor_reset_seconds
    finally:
        os.environ.update(saved)
    assert default == 0.0, (
        "the settings default must be the opt-in posture (0 = disabled): "
        "the A/B harness measured the cursor's full-path win as "
        "plan-dependent, and a default-on selection predicate that buys "
        "no robust end-to-end win is a pure stranding risk"
    )
    assert default != CLAIM_CURSOR_RESET_SECONDS, (
        "the SETTINGS default (off) and the MECHANISM cadence (60s) are "
        "deliberately different numbers: one is a posture decision, the "
        "other the documented reset cadence of an enabled cursor"
    )
