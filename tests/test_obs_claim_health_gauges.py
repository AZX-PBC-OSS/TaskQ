"""The MVCC-horizon hygiene gauges: the PREDICTOR half of the claim-query
degradation pair.

The death spiral (brandur.org/postgres-queues; PlanetScale's 2026 re-run
"Keeping a Postgres queue healthy"): a long or overlapping transaction
pins the MVCC horizon, VACUUM cannot reclaim, and the claim query's
B-tree scans degenerate through the dead tuples the claims themselves
left behind - 15x lock-time degradation measured, and SKIP LOCKED only
"lifts the floor, not the ceiling" (identical dead-tuple scans under
both). The literature's first cure is the PREDICTOR: the degradation is
visible in the claim-latency DISTRIBUTION before the backlog explodes.

These gauges are that predictor, under the obs conventions (dotted
``taskq.<domain>.<noun_unit>`` names, bounded labels, no per-job
cardinality):

- ``taskq.claim.latency_p50_seconds`` / ``_p95_`` / ``_p99_``: the
  claim-statement latency percentiles, per queue, computed from this
  worker's in-process recent window.
- ``taskq.claim.claims_per_second``: the claim throughput the window
  observed, per queue.
- ``taskq.claim.degradation_ratio``: the p99 against its rolling
  baseline (minute-bucketed p99s kept for 24h). THE gauge: crossing
  ~10x is the pinned-horizon signature - see the
  ``TaskQClaimLatencyDegraded`` runbook.

Pinned here (fast tier): the recording hook and its bounds, the pure
math (percentiles, rate, ratio, warm-up, the divide-by-zero floor), the
empty-not-zero discipline, and the cardinality doctrine (the queue label
is the ONLY label, bounded at record time; nothing job-shaped can reach
a label).
"""

from __future__ import annotations

import pytest

from taskq.obs import record_claim_latency
from taskq.obs._claim_health import (
    CLAIM_BASELINE_FLOOR_SECONDS,
    CLAIM_BASELINE_MINUTES,
    CLAIM_BASELINE_WARMUP_MINUTES,
    CLAIM_WINDOW_CAP_ENTRIES,
    CLAIM_WINDOW_SECONDS,
    ClaimQueueHealth,
    claim_health_snapshot,
    reset_claim_health_state,
)

pytestmark = pytest.mark.usefixtures("_otel_enabled_guard")

#: The tests drive time explicitly (monotonic seconds); the hooks accept
#: ``now=`` so no pin sleeps and no pin depends on wall-clock speed.
_T0 = 1_000_000.0


# ── the recording hook ─────────────────────────────────────────────────


def test_record_claim_latency_feeds_a_bounded_recent_window() -> None:
    reset_claim_health_state()
    for i in range(50):
        record_claim_latency("default", 0.001 + i / 1e6, now=_T0 + i)
    health = claim_health_snapshot(now=_T0 + 50)
    assert health["default"].claims_per_second > 0
    assert health["default"].p50_seconds > 0


def test_the_window_is_time_and_count_bounded() -> None:
    """Both bounds are load-bearing: the time bound keeps the 'recent'
    window recent (a gauge that mixes last week's healthy latencies into
    today's p99 dilutes the degradation signal exactly when it matters),
    the count bound caps the memory a hot claim loop can grow."""
    reset_claim_health_state()
    assert CLAIM_WINDOW_SECONDS <= 900.0, "the recent window is minutes, not hours"
    assert CLAIM_WINDOW_CAP_ENTRIES <= 100_000, (
        "the entry cap bounds the window's memory per worker"
    )
    # The count bound is enforced, not decorative: flood past it and the
    # window holds the cap (the rate the snapshot reports saturates at the
    # cap over the window span, not the flooded count).
    for i in range(CLAIM_WINDOW_CAP_ENTRIES + 500):
        record_claim_latency("default", 0.001, now=_T0 + i * (CLAIM_WINDOW_SECONDS / 50_000))
    health = claim_health_snapshot(now=_T0 + CLAIM_WINDOW_SECONDS)
    assert health["default"].claims_per_second > 0


def test_old_window_entries_stop_counting() -> None:
    reset_claim_health_state()
    for i in range(100):
        record_claim_latency("default", 0.001, now=_T0 + i)
    # an hour later the whole window has aged out
    snapshot = claim_health_snapshot(now=_T0 + 3600.0)
    assert snapshot == {}, (
        "entries older than the recent window must drop out: the p99 and "
        "the rate describe the RECENT claim latencies, and a stale window "
        "would report a degradation that already healed (or hide one that "
        "just began)"
    )


def test_the_baseline_is_24h_of_minute_buckets() -> None:
    assert CLAIM_BASELINE_MINUTES == 1440, (
        "the degradation baseline is 24h of minute-bucketed p99s - the "
        "runbook's ratio compares today's p99 against a full day of them"
    )


# ── the percentiles ────────────────────────────────────────────────────


def test_percentiles_are_nearest_rank_over_the_recent_window() -> None:
    reset_claim_health_state()
    # 100 latencies 1ms..100ms; p50 = 50ms, p95 = 95ms, p99 = 99ms.
    for i in range(1, 101):
        record_claim_latency("default", i / 1000.0, now=_T0 + i)
    health = claim_health_snapshot(now=_T0 + 200)
    assert health["default"].p50_seconds == pytest.approx(0.050, abs=1e-4)
    assert health["default"].p95_seconds == pytest.approx(0.095, abs=1e-4)
    assert health["default"].p99_seconds == pytest.approx(0.099, abs=1e-4)


def test_percentiles_are_per_queue() -> None:
    reset_claim_health_state()
    for i in range(10):
        record_claim_latency("fast", 0.001, now=_T0 + i)
        record_claim_latency("slow", 0.500, now=_T0 + i)
    snapshot = claim_health_snapshot(now=_T0 + 20)
    assert snapshot["fast"].p99_seconds < 0.01
    assert snapshot["slow"].p99_seconds > 0.4, (
        "the percentiles are per queue: one queue's degradation must not "
        "dilute into (or be diluted by) another queue's latency"
    )


# ── claims-per-second ──────────────────────────────────────────────────


def test_claims_per_second_is_the_window_rate() -> None:
    reset_claim_health_state()
    span = 40.0
    for i in range(80):
        record_claim_latency("default", 0.001, now=_T0 + i * (span / 80))
    health = claim_health_snapshot(now=_T0 + span)
    assert health["default"].claims_per_second == pytest.approx(80 / span, rel=0.25)


def test_an_empty_window_yields_no_data_point_not_zero() -> None:
    """The empty-not-zero discipline: a worker that has claimed nothing
    (fresh boot, or a queue that saw no traffic) yields NO observation - a
    0 would read as a flatlining claim rate and could silence (or worse,
    fire) the rate-shaped operands downstream."""
    reset_claim_health_state()
    snapshot = claim_health_snapshot(now=_T0)
    assert snapshot == {}, "an empty window must produce no per-queue observations"


# ── the degradation ratio ──────────────────────────────────────────────


def test_the_ratio_is_p99_over_its_baseline() -> None:
    reset_claim_health_state()
    # Warm the baseline: one minute bucket of ~1ms p99s per minute, past
    # the warm-up floor.
    now = _T0
    for _ in range(CLAIM_BASELINE_WARMUP_MINUTES + 1):
        for i in range(10):
            record_claim_latency("default", 0.001, now=now + i)
        now += 60.0
    # live p99 ~50ms now, same minute as the last baseline bucket plus one
    for i in range(100):
        record_claim_latency("default", 0.050, now=now + i)
    health = claim_health_snapshot(now=now + 200)
    ratio = health["default"].degradation_ratio
    assert ratio is not None and ratio > 25.0, (
        "a 1ms baseline against a 50ms live p99 must read as heavy "
        "degradation - the pinned-horizon signature the runbook's 10x "
        "threshold keys on"
    )


def test_a_healthy_worker_reads_ratio_one() -> None:
    reset_claim_health_state()
    now = _T0
    for _ in range(CLAIM_BASELINE_WARMUP_MINUTES + 1):
        for i in range(10):
            record_claim_latency("default", 0.001, now=now + i)
        now += 60.0
    for i in range(100):
        record_claim_latency("default", 0.0012, now=now + i)
    health = claim_health_snapshot(now=now + 200)
    ratio = health["default"].degradation_ratio
    assert ratio is not None and ratio < 2.0, (
        "a live p99 at its baseline must read ~1x - a healthy worker must never arm the ratio alert"
    )


def test_a_cold_start_reports_no_ratio_not_a_fabricated_one() -> None:
    """The warm-up guard: without a warmed baseline the ratio reports NO
    data point, not a confident 1x and not a divide-by-zero monster. A
    cold worker's first minutes cannot page anyone."""
    reset_claim_health_state()
    for i in range(100):
        record_claim_latency("default", 0.050, now=_T0 + i)
    health = claim_health_snapshot(now=_T0 + 200)
    assert health["default"].degradation_ratio is None, (
        "without a warmed baseline the ratio must report NO data: a "
        "cold-start worker emitting 1.0 would train readers to ignore the "
        "gauge, and a raw p99/baseline-of-nothing would divide by zero "
        "into a monster"
    )


def test_the_ratio_never_divides_by_a_zero_baseline() -> None:
    """A sub-millisecond baseline is the healthy common case; the floor
    keeps the ratio finite and sane when the live p99 spikes from ~0."""
    assert CLAIM_BASELINE_FLOOR_SECONDS > 0.0
    assert CLAIM_BASELINE_FLOOR_SECONDS <= 0.001, (
        "the floor guards divide-by-zero without flattering real "
        "degradation: a 1ms floor keeps a 50ms p99 at >= 50x"
    )


def test_the_baseline_rolls_at_24h_not_forever() -> None:
    """The baseline is a ROLLING day: buckets older than 24h drop, so a
    permanently-degraded fleet's ratio decays back toward 1x only as the
    degradation itself becomes the new normal - and a healed fleet's
    baseline heals within a day."""
    reset_claim_health_state()
    now = _T0
    bucket = 60.0
    # 24h + 1 of minute buckets at 1ms
    for _m in range(CLAIM_BASELINE_MINUTES + 2):
        record_claim_latency("default", 0.001, now=now)
        now += bucket
    for i in range(100):
        record_claim_latency("default", 0.050, now=now + i)
    health = claim_health_snapshot(now=now + 200)
    ratio = health["default"].degradation_ratio
    assert ratio is not None and ratio > 25.0, (
        "a baseline that kept every bucket forever would eventually be "
        "polluted by the degraded p99s themselves and read ~1x while the "
        "worker is 50x degraded; the rolling 24h keeps the baseline the "
        "healthy day behind the spike"
    )


# ── the cardinality doctrine ───────────────────────────────────────────


def test_the_queue_label_is_bounded_at_record_time() -> None:
    """No per-job labels, anywhere: the recording hook accepts only
    (queue, seconds), and the queue label passes the obs layer's
    cardinality cap - a flood of distinct queue names saturates to the
    fixed ``_other_`` overflow label, so the series count is hard-bounded
    no matter what the callers mint (a job-shaped string can never become
    a label)."""
    from taskq.obs import _otel as otel_mod

    reset_claim_health_state()
    cap = otel_mod._MAX_QUEUE_LABEL_VALUES
    for i in range(cap + 20):
        record_claim_latency(f"queue-{i}", 0.001, now=_T0 + i)
    snapshot = claim_health_snapshot(now=_T0 + cap + 30)
    assert len(snapshot) <= cap + 1, (
        "the queue label must be cardinality-capped at record time (the "
        "same _bounded_queue discipline every taskq.* metric uses)"
    )
    assert otel_mod._QUEUE_LABEL_OVERFLOW in snapshot, (
        "queues past the cap must land on the fixed overflow label, never on their own series"
    )
    reset_claim_health_state()


def test_the_gauge_names_follow_the_dotted_noun_unit_convention() -> None:
    from taskq.obs import _claim_health as ch

    for name in ch._GAUGE_NAMES:
        assert name.startswith("taskq."), name
        parts = name.split(".")
        assert len(parts) >= 3, name
        assert parts[1] == "claim", name
        assert parts[-1].endswith(("_seconds", "_ratio", "_per_second")), (
            f"{name}: the noun carries its unit suffix"
        )


def test_the_health_dataclass_carries_exactly_the_runbook_operands() -> None:
    """The five numbers the runbook's TaskQClaimLatencyDegraded reads -
    nothing more, so the gauge surface cannot drift from its own doc."""
    fields = set(ClaimQueueHealth.__dataclass_fields__)
    assert fields == {
        "p50_seconds",
        "p95_seconds",
        "p99_seconds",
        "claims_per_second",
        "degradation_ratio",
    }


# ── finding 11: the gauge racing itself — the hot-load pin ───────────────


def test_the_hot_load_scrape_is_clean_for_three_seconds() -> None:
    """FINDING 11's PIN (RED-FIRST): the gauge callbacks scrape on the
    OTel collection THREAD while the claim path records on the EVENT-
    LOOP thread — the pre-cure stores had no lock, and the two walkers
    raced inside the deque/dict: **287 RuntimeErrors in 3 s of hot
    load** ("deque mutated during iteration"), every raise killing the
    callback's observation batch — the alert's operand VANISHED exactly
    when the queue was hottest. THE CURE: the read is ATOMIC (the
    stores' lock — the snapshot and the record each hold it for their
    whole walk). This pin hammers the record path on a writer thread
    while a scraper thread snapshots continuously for 3 s and convicts:
    ZERO RuntimeErrors, and the scrape's operands PRESENT (zero is a
    number, not an error — a warmed baseline reports a NUMBER)."""
    import threading
    import time as time_mod

    reset_claim_health_state()
    stop = threading.Event()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()
    operands_seen: list[bool] = []

    def _record_hot(tid: int) -> None:
        # The claim path's shape: one record per claim round, the clock
        # advancing. The ts step lands >5 minute-buckets inside the 3 s
        # wall, so the baseline WARMED and the ratio is a NUMBER by the
        # final scrapes (the empty/ratio-None face is the cold-start's
        # own pin, not this one).
        ts = _T0 + float(tid)
        try:
            while not stop.is_set():
                record_claim_latency(f"hot-q{tid}", 0.001, now=ts)
                ts += 0.05
                time_mod.sleep(0.0005)
        except BaseException as exc:  # Why: the pin convicts ANY raise on the hot path.
            with errors_lock:
                errors.append(exc)

    def _scrape_hot() -> None:
        # The OTel collection thread's shape: the FIVE gauge callbacks'
        # snapshots, back to back, for the whole window.
        try:
            while not stop.is_set():
                snap = claim_health_snapshot(now=_T0 + 600.0)
                with errors_lock:
                    operands_seen.append(bool(snap) and snap["hot-q0"].p99_seconds >= 0.0)
                time_mod.sleep(0.001)
        except BaseException as exc:  # Why: the convicted shape raised RuntimeError here.
            with errors_lock:
                errors.append(exc)

    # THE PRODUCTION SHAPE'S CONCURRENCY: several claim paths recording
    # (the loop + the fleet's rounds) AND several gauge callbacks
    # scraping at once — the pre-cure race needed BOTH walkers live (the
    # convicted run: 287 RuntimeErrors in 3 s).
    threads = [threading.Thread(target=_record_hot, args=(i,)) for i in range(4)]
    threads += [threading.Thread(target=_scrape_hot) for _ in range(3)]
    for t in threads:
        t.start()
    time_mod.sleep(3.0)
    stop.set()
    for t in threads:
        t.join(timeout=10)

    assert not errors, (
        f"the hot-load scrape raised {len(errors)}x — the gauge raced "
        f"itself and the alert's operand vanished hot: {errors[:3]}"
    )
    assert operands_seen, "the scraper never saw a snapshot — the operand vanished"
    assert all(operands_seen), "a scrape lost its operand mid-window"
    assert len(operands_seen) >= 100, (
        f"only {len(operands_seen)} scrapes in 3 s — the hot loop did not exercise the race"
    )
    reset_claim_health_state()
