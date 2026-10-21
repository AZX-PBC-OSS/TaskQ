"""The claim-query health gauges: the MVCC-horizon degradation PREDICTOR.

The death spiral (brandur.org/postgres-queues; PlanetScale's 2026
re-run): a long or overlapping transaction pins the MVCC horizon, VACUUM
cannot reclaim, and the claim query's B-tree scans degenerate through
dead tuples - 15x lock-time degradation measured, identical under SKIP
LOCKED. The literature's first cure is the PREDICTOR: the degradation
shows in the claim-latency distribution BEFORE the backlog explodes.

What this module computes, per queue, from THIS worker's own claim
latencies (each worker is the sole authority over its own series - the
same demotion-independence the backlog detectors claim for themselves,
and unlike the leader's SQL samplers there is no query here to fail, so
no sampler-timeout surface either):

- the recent window's p50/p95/p99 (``taskq.claim.latency_p{50,95,99}_seconds``);
- the window's claim rate (``taskq.claim.claims_per_second``);
- THE gauge: ``taskq.claim.degradation_ratio`` - the live p99 against a
  rolling 24h baseline of minute-bucketed p99s. Crossing ~10x is the
  pinned-horizon signature; the ``TaskQClaimLatencyDegraded`` alert and
  its runbook key on it.

Architecture: the claim path calls :func:`record_claim_latency` (one
append per claim round, bounded on both axes); the Prometheus/OTel
observable-gauge callbacks call :func:`claim_health_snapshot` at SCRAPE
time (a pull, not a sampler loop - always fresh, nothing to go stale,
nothing to gate on leadership). The cardinality doctrine holds: the
queue label is the only label, bounded at record time; nothing
job-shaped reaches a label.

All windows are bounded and the empty-not-zero discipline holds: a
worker that has claimed nothing yields NO observations, and a
cold-start worker yields no RATIO (never a fabricated 1.0).
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Final

from opentelemetry.metrics import Observation

from taskq.obs._otel import bounded_queue_label, get_meter, otel_enabled

__all__ = [
    "CLAIM_BASELINE_FLOOR_SECONDS",
    "CLAIM_BASELINE_MINUTES",
    "CLAIM_BASELINE_WARMUP_MINUTES",
    "CLAIM_WINDOW_CAP_ENTRIES",
    "CLAIM_WINDOW_SECONDS",
    "ClaimQueueHealth",
    "claim_health_snapshot",
    "record_claim_latency",
    "reset_claim_health_state",
]

CLAIM_WINDOW_SECONDS: Final[float] = 600.0
"""The recent window: ten minutes of this worker's claim latencies.

The percentiles and the rate describe the RECENT claim path only; a
window that kept everything forever would dilute today's degradation
into last week's healthy latencies exactly when the signal matters."""

CLAIM_WINDOW_CAP_ENTRIES: Final[int] = 20_000
"""Absolute entry cap on the recent window (memory, not statistics).

At the notify-wake ceiling (~20 claim rounds/s) this is ~16 minutes of
hot traffic, comfortably above the 10-minute time bound; a faster loop
trades window coverage for bounded memory."""

CLAIM_BASELINE_MINUTES: Final[int] = 1440
"""The degradation baseline: 24h of minute-bucketed p99s (rolling)."""

CLAIM_BASELINE_WARMUP_MINUTES: Final[int] = 5
"""Baseline buckets required before the ratio reports at all.

A cold-start worker has no "before" to degrade from; a ratio computed
against one thin bucket is noise. Below the floor the ratio reports NO
data point - never a fabricated 1.0 readers learn to ignore."""

CLAIM_BASELINE_FLOOR_SECONDS: Final[float] = 1e-4
"""Divide-by-zero guard: the baseline median never divides below this.

0.1ms keeps healthy sub-millisecond claims' ratios finite without
flattering real degradation (a 50ms p99 over a floored baseline still
reads >= 50x)."""


@dataclass(frozen=True, slots=True)
class ClaimQueueHealth:
    """One queue's claim-health view - exactly the runbook's operands."""

    p50_seconds: float
    p95_seconds: float
    p99_seconds: float
    claims_per_second: float
    #: Live p99 / rolling 24h baseline p99; ``None`` before the baseline
    #: has warmed (never a fabricated 1.0).
    degradation_ratio: float | None


# ── the bounded stores ─────────────────────────────────────────────────

# THE STORES' LOCK (finding 11's cure — the gauge racing itself): the
# record path runs on the EVENT-LOOP thread; the observable-gauge
# callbacks run on the OTel SDK's COLLECTION thread. Both walk
# ``_window`` (a deque — cross-thread mutation during iteration raises
# ``RuntimeError: deque mutated during iteration``) and both roll/read
# ``_baseline`` (the same race, deque and dict). The convicted shape:
# 287 RuntimeErrors in 3 s of hot load — every raise kills the
# callback's observation batch, so the alert's operand (the degradation
# ratio, the p99s) VANISHED exactly when the queue was hottest. The read
# is ATOMIC: every store touch holds ``_store_lock``; the percentiles'
# math runs on the COPY taken under it. Zero is a NUMBER, not an error —
# a raced read never trades a number for a raise.
_store_lock = threading.Lock()

# (monotonic_ts, queue, seconds), oldest first; trimmed by time and count.
_window: deque[tuple[float, str, float]] = deque()
# Minute bucket -> latencies of the bucket currently being filled; on the
# bucket roll its p99 freezes into _baseline. Single current-bucket buffer
# keeps the baseline's memory at O(buckets), never O(entries).
_current_bucket: dict[tuple[int, str], list[float]] = {}
_baseline: dict[str, deque[tuple[int, float]]] = {}
# The record-time clock; tests inject a monotonic value instead of sleeping.
_clock: Callable[[], float] = time.monotonic


def reset_claim_health_state() -> None:
    """Drop every recorded latency (test isolation)."""
    global _clock
    with _store_lock:
        _window.clear()
        _current_bucket.clear()
        _baseline.clear()
    _clock = time.monotonic


def record_claim_latency(queue: str, seconds: float, *, now: float | None = None) -> None:
    """Record ONE claim statement's latency (the wiring site is the
    dispatch helper, success or failure - a failed claim round is still a
    latency the queue paid).

    The queue label is bounded HERE, at record time - the same
    ``_bounded_queue`` discipline every ``taskq.*`` metric uses; a
    payload-shaped string can never become a label.
    """
    if not otel_enabled():
        return
    ts = now if now is not None else _clock()
    q = bounded_queue_label(queue)
    # The store lock (the record path's O(1) critical section — the
    # collection thread's snapshot holds it only for its own copy).
    with _store_lock:
        _window.append((ts, q, seconds))
        # The count cap trims oldest-first; the time trim happens at read.
        while len(_window) > CLAIM_WINDOW_CAP_ENTRIES:
            _window.popleft()
        # Baseline bucket fill: minute buckets keyed on the recording clock.
        bucket = int(ts // 60.0)
        lat = _current_bucket.get((bucket, q))
        if lat is None:
            _roll_baseline_buckets(bucket)
            lat = _current_bucket.setdefault((bucket, q), [])
        lat.append(seconds)


def _roll_baseline_buckets(up_to_bucket: int) -> None:
    """Freeze every completed minute bucket's p99 into the baseline.

    Called whenever a record lands in a NEW bucket: every bucket strictly
    before it is complete. Buckets older than 24h drop (rolling), and a
    bucket the worker recorded NOTHING in never existed - gaps read as
    absent, not zero.
    """
    for (bucket, _q), lat in list(_current_bucket.items()):
        if bucket < up_to_bucket:
            _freeze_bucket(_q, bucket, lat)
            _current_bucket.pop((bucket, _q), None)
    # Rolling 24h: drop buckets that fell out of the day.
    cutoff = up_to_bucket - CLAIM_BASELINE_MINUTES
    for buckets in _baseline.values():
        while buckets and buckets[0][0] < cutoff:
            buckets.popleft()


def _freeze_bucket(q: str, bucket: int, lat: list[float]) -> None:
    buckets = _baseline.setdefault(q, deque())
    buckets.append((bucket, _percentile(lat, 99.0)))
    while len(buckets) > CLAIM_BASELINE_MINUTES:
        buckets.popleft()


def _percentile(values: Iterable[float], pct: float) -> float:
    """Nearest-rank percentile: the value at rank ceil(N * pct/100).

    (The dispatch benchmark's ``int(N * pct/100)`` truncation answers
    data[idx] one past the rank for these 100-value windows; the
    ceil-minus-one form is the textbook nearest-rank.)
    """
    data = sorted(values)
    if not data:
        return 0.0
    idx = max(0, math.ceil(len(data) * pct / 100.0) - 1)
    return data[min(idx, len(data) - 1)]


# ── the snapshot ───────────────────────────────────────────────────────


def claim_health_snapshot(*, now: float | None = None) -> dict[str, ClaimQueueHealth]:
    """Per-queue claim health over the recent window, at *now*.

    Pure read: trims nothing the next record wouldn't, yields NOTHING for
    a queue with no window entries (the empty-not-zero discipline - a
    scrape of an idle worker produces no data points, never zeros).

    THE ATOMIC READ (finding 11's cure): the window walk, the baseline
    roll, and the baseline read all hold ``_store_lock`` — the OTel
    collection thread's scrape can no longer interleave with the event
    loop's records inside a deque/dict iteration (the 287-RuntimeErrors-
    in-3s race, dead). The math on the copied latencies runs outside the
    lock.
    """
    ts = now if now is not None else _clock()
    cutoff = ts - CLAIM_WINDOW_SECONDS
    by_queue: dict[str, list[float]] = {}
    oldest: dict[str, float] = {}
    newest: dict[str, float] = {}
    with _store_lock:
        for entry_ts, q, seconds in _window:
            if entry_ts < cutoff:
                continue
            by_queue.setdefault(q, []).append(seconds)
            if q not in oldest or entry_ts < oldest[q]:
                oldest[q] = entry_ts
            if q not in newest or entry_ts > newest[q]:
                newest[q] = entry_ts

        # Freeze any baseline buckets the read's clock has completed (a
        # quiet worker's last bucket rolls on the read, not the next
        # write) — the roll MUTATES the baseline stores, so it holds the
        # same lock the walkers hold.
        _roll_baseline_buckets(int(ts // 60.0))

        ratio_inputs: dict[str, float | None] = {}
        for q, lat in by_queue.items():
            p99 = _percentile(lat, 99.0)
            ratio_inputs[q] = _degradation_ratio(q, p99, ts)

    out: dict[str, ClaimQueueHealth] = {}
    for q, lat in by_queue.items():
        p50 = _percentile(lat, 50.0)
        p95 = _percentile(lat, 95.0)
        p99 = _percentile(lat, 99.0)
        span = max(newest[q] - oldest[q], 1.0)
        out[q] = ClaimQueueHealth(
            p50_seconds=p50,
            p95_seconds=p95,
            p99_seconds=p99,
            claims_per_second=len(lat) / span,
            degradation_ratio=ratio_inputs[q],
        )
    return out


def _degradation_ratio(q: str, live_p99: float, ts: float) -> float | None:
    """Live p99 / rolling-24h baseline p99, ``None`` before the baseline
    has the warm-up floor of buckets."""
    buckets = _baseline.get(q)
    if not buckets or len(buckets) < CLAIM_BASELINE_WARMUP_MINUTES:
        return None
    baseline_values = sorted(p for _, p in buckets)
    mid = len(baseline_values) // 2
    if len(baseline_values) % 2:
        median = baseline_values[mid]
    else:
        median = (baseline_values[mid - 1] + baseline_values[mid]) / 2.0
    return live_p99 / max(median, CLAIM_BASELINE_FLOOR_SECONDS)


# ── the gauges ─────────────────────────────────────────────────────────

_GAUGE_NAMES: Final[tuple[str, ...]] = (
    "taskq.claim.latency_p50_seconds",
    "taskq.claim.latency_p95_seconds",
    "taskq.claim.latency_p99_seconds",
    "taskq.claim.claims_per_second",
    "taskq.claim.degradation_ratio",
)


def _observe_p50(options: object) -> Iterable[Observation]:
    for q, h in claim_health_snapshot().items():
        yield Observation(h.p50_seconds, {"queue": q})


def _observe_p95(options: object) -> Iterable[Observation]:
    for q, h in claim_health_snapshot().items():
        yield Observation(h.p95_seconds, {"queue": q})


def _observe_p99(options: object) -> Iterable[Observation]:
    for q, h in claim_health_snapshot().items():
        yield Observation(h.p99_seconds, {"queue": q})


def _observe_rate(options: object) -> Iterable[Observation]:
    for q, h in claim_health_snapshot().items():
        yield Observation(h.claims_per_second, {"queue": q})


def _observe_ratio(options: object) -> Iterable[Observation]:
    for q, h in claim_health_snapshot().items():
        # Warm-up guard: None yields NO data point, never a fabricated 1.0.
        if h.degradation_ratio is not None:
            yield Observation(h.degradation_ratio, {"queue": q})


get_meter().create_observable_gauge(
    name="taskq.claim.latency_p50_seconds",
    description=(
        "This worker's claim-statement latency p50 over the recent "
        "window, per queue. The claim path's own distribution view - the "
        "degradation shows here before the backlog explodes."
    ),
    unit="s",
    callbacks=[_observe_p50],
)
get_meter().create_observable_gauge(
    name="taskq.claim.latency_p95_seconds",
    description=(
        "This worker's claim-statement latency p95 over the recent "
        "window, per queue. taskq.dispatch.duration's histogram carries "
        "the same signal as a histogram; this gauge exists so the "
        "degradation-ratio operands are directly scrapeable without "
        "histogram_quantile."
    ),
    unit="s",
    callbacks=[_observe_p95],
)
get_meter().create_observable_gauge(
    name="taskq.claim.latency_p99_seconds",
    description=(
        "This worker's claim-statement latency p99 over the recent "
        "window, per queue - the numerator of the degradation ratio."
    ),
    unit="s",
    callbacks=[_observe_p99],
)
get_meter().create_observable_gauge(
    name="taskq.claim.claims_per_second",
    description=(
        "This worker's claim throughput over the recent window, per "
        "queue. Read beside the degradation ratio: a rising ratio with a "
        "collapsing rate is a claim path about to stall (the death "
        "spiral's last visible stage); a rising ratio with a steady rate "
        "is headroom still being spent."
    ),
    unit="1",
    callbacks=[_observe_rate],
)
get_meter().create_observable_gauge(
    name="taskq.claim.degradation_ratio",
    description=(
        "The live claim-latency p99 against its rolling 24h baseline "
        "(minute-bucketed p99s), per queue. ~10x is the pinned-MVCC-"
        "horizon signature: a long transaction is holding VACUUM off and "
        "the claim query's scan is degenerating through dead tuples. "
        "Runbook: TaskQClaimLatencyDegraded. Reports no data until the "
        "baseline has warmed (5 minutes), never a fabricated 1.0."
    ),
    unit="1",
    callbacks=[_observe_ratio],
)
