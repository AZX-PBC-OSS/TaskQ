"""Exact-value assertions on the in-memory sliding-window arithmetic.

The GCRA ``peek()`` computation and the two ``acquire()`` denial branches
ran under test with nothing pinning the numbers they produce — the existing
suite asserted ``is not None`` and loose inequalities, which survive an
arithmetic sign flip or a branch-selection flip.  Every assertion here is
an exact equality against a constant computed by hand from ``limit`` and
``window``, so a change to the operator or the branch guard changes the
number and fails.

Constants for the GCRA fixtures (limit=4, window=1000 ms):
    emission_interval = window / limit = 250 ms
    delay_tolerance   = window         = 1000 ms
A burst of 4 acquires at t=0 leaves ``tat = 1000`` and 4 log entries at 0.
"""

from datetime import UTC, datetime, timedelta

from taskq.ratelimit import SlidingWindow
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)
_WINDOW = timedelta(milliseconds=1000)
_LIMIT = 4
_EMISSION = timedelta(milliseconds=250)


def _gcra(name: str, *, limit: int = _LIMIT, window: timedelta = _WINDOW) -> SlidingWindow:
    return SlidingWindow(
        name=name,
        limit=limit,
        window=window,
        backend="memory",
        style="gcra",
    )


async def _saturate(sw: SlidingWindow, clock: FakeClock, n: int) -> None:
    for i in range(n):
        decision = await sw.acquire(clock=clock)
        assert decision.allowed is True, f"acquire {i} unexpectedly denied"


# ── GCRA acquire: the two denial branches yield different numbers ─────


async def test_gcra_denied_by_emission_interval_retry_after_is_exact() -> None:
    """Denied while ``now < allow_at``: retry_after is exactly ``allow_at - now``.

    Why exact: the emission-interval branch and the count branch are both
    reachable at saturation and both return a positive retry_after, so only
    the exact value distinguishes which branch ran.
    """
    sw = _gcra("gcra_emission")
    clock = FakeClock(_START)
    await _saturate(sw, clock, _LIMIT)

    denied = await sw.acquire(clock=clock)

    assert denied.allowed is False
    assert denied.remaining == 0.0
    # allow_at = (tat=1000 + 250) - 1000 = 250; now = 0.
    assert denied.retry_after == _EMISSION


async def test_gcra_denied_by_count_at_allow_at_boundary_retry_after_is_exact() -> None:
    """At ``now == allow_at`` the count branch runs: ``oldest + window - now``.

    Why the boundary instant: the branch guard is ``now >= allow_at``.  Any
    later instant satisfies ``>`` as well, so only ``now == allow_at`` pins
    the ``>=`` rather than merely the branch body.
    """
    sw = _gcra("gcra_count")
    clock = FakeClock(_START)
    await _saturate(sw, clock, _LIMIT)

    # tat is 1000 ms, so allow_at is 250 ms; land exactly on it.
    clock.advance(_EMISSION)
    denied = await sw.acquire(clock=clock)

    assert denied.allowed is False
    assert denied.remaining == 0.0
    # oldest log entry is t=0: 0 + 1000 - 250 = 750 ms.
    assert denied.retry_after == timedelta(milliseconds=750)


# ── GCRA peek: exhausted state ───────────────────────────────────────


async def test_gcra_peek_exhausted_returns_exact_state() -> None:
    """Every field of the exhausted GCRA snapshot, by exact value."""
    sw = _gcra("gcra_peek_exhausted")
    clock = FakeClock(_START)
    await _saturate(sw, clock, _LIMIT)
    clock.advance(_EMISSION)

    state = await sw.peek(clock=clock)

    assert state.bucket_name == "gcra_peek_exhausted"
    assert state.backend == "memory"
    assert state.style == "gcra"
    assert state.limit == _LIMIT
    assert state.window == _WINDOW
    assert state.is_exhausted is True
    assert state.remaining == 0.0
    # log_count == limit, so the count branch wins: 0 + 1000 - 250 = 750 ms.
    assert state.retry_after == timedelta(milliseconds=750)


async def test_gcra_peek_at_window_boundary_treats_entries_as_expired() -> None:
    """An entry exactly ``window`` old is outside the window, not inside.

    Peeking at t=1000 after a burst at t=0 puts ``cutoff`` exactly on the
    logged timestamps.  The window is half-open, so the bucket reads as fully
    replenished; counting the boundary entry instead would report exhausted.
    """
    sw = _gcra("gcra_peek_boundary")
    clock = FakeClock(_START)
    await _saturate(sw, clock, _LIMIT)
    clock.advance(_WINDOW)

    state = await sw.peek(clock=clock)

    assert state.is_exhausted is False
    assert state.remaining == float(_LIMIT)
    assert state.retry_after is None


async def test_gcra_peek_partial_reports_exact_remaining_and_no_retry_after() -> None:
    """Below saturation, ``remaining`` is bounded by the per-window count.

    limit=10, window=1000 ms → emission_interval = 100 ms.  Three acquires at
    t=0 leave ``tat = 300``; peeking at t=500 clamps ``tat`` to ``now``, so the
    TAT term alone would say 10.  The log term (10 - 3) is the binding one.
    """
    sw = _gcra("gcra_peek_partial", limit=10)
    clock = FakeClock(_START)
    await _saturate(sw, clock, 3)
    clock.advance(timedelta(milliseconds=500))

    state = await sw.peek(clock=clock)

    assert state.is_exhausted is False
    assert state.remaining == 7.0
    assert state.retry_after is None
    assert state.limit == 10
    assert state.window == _WINDOW


# ── Log-style refund removes the named entry, not a bystander ────────


async def test_log_refund_releases_only_the_named_request() -> None:
    """``refund(b)`` frees b's slot; a and c keep theirs.

    Observable via admission arithmetic: with a at t=0, b at t=10 s and c at
    t=20 s against limit=3, the entry that survives as *oldest* decides the
    next denial's retry_after.  Refunding b leaves a as oldest → 0 + 60 - 20
    = 40 s.  Releasing the wrong entry (a) would leave b as oldest → 50 s.
    """
    window = timedelta(seconds=60)
    sw = SlidingWindow(
        name="log_refund_identity",
        limit=3,
        window=window,
        backend="memory",
        style="log",
    )
    clock = FakeClock(_START)

    await sw.acquire(clock=clock)  # a @ t=0
    clock.advance(timedelta(seconds=10))
    decision_b = await sw.acquire(clock=clock)  # b @ t=10
    clock.advance(timedelta(seconds=10))
    await sw.acquire(clock=clock)  # c @ t=20

    await sw.refund(decision_b, clock=clock)

    # The freed slot is real: a fourth admission fits.
    refilled = await sw.acquire(clock=clock)  # d @ t=20
    assert refilled.allowed is True
    assert refilled.remaining == 0.0

    denied = await sw.acquire(clock=clock)
    assert denied.allowed is False
    assert denied.retry_after == timedelta(seconds=40)
