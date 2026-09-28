"""Metrics review: are the seconds-scaled histogram buckets HONEST?

The commit under review replaced the OTel SDK's unit-agnostic default
boundaries (0..10000) with seconds-scaled ``explicit_bucket_boundaries_advisory``
on the six histograms whose documented reads are percentiles. The claim under
attack: each bucket scale lets the alert-relevant quantiles interpolate
honestly - a 300 ms dispatch lands in a bucket that bounds the p99 error, a
45 s queue wait is covered, the event-loop lag's sub-millisecond range has
usable resolution instead of collapsing every healthy beat into one bucket.

How this suite attacks that claim - against the SERVED exposition, never the
SDK objects:

1. A subprocess wires the real bridge (``PrometheusMetricReader`` →
   ``CollectorRegistry`` → ``generate_latest``, the same path
   ``/jobs/health/metrics`` serves) and records SYNTHETIC distributions with
   known order statistics through the real public emitters
   (``record_dispatch_duration`` & co.), then dumps both the scrape text and
   the raw samples.
2. The test computes each distribution's true p50/p95/p99 (nearest-rank) and
   the quantile a Prometheus server computes from the served cumulative
   buckets (the documented linear-interpolation algorithm, reimplemented
   faithfully - including the ``le="0"`` first-bucket and last-finite-bucket
   edges), and bounds the interpolation error by the width of the bucket the
   rank falls in. That bound is the honest promise a bucket histogram can
   make; a regression to the default 0..10000 boundaries (the defect this
   commit fixed, where a healthy ~13 ms dispatch quantiled to ~4.95 s) blows
   it by orders of magnitude and fails here.
3. The alert-side reads are pinned on the correct side of their thresholds:
   a healthy dispatch fleet's served p99 stays under ``TaskQDispatchLatencyHigh``'s
   50 ms line, a healthy lock's served p99 stays over ``TaskQLockExpiringSoon``'s
   30 s line - and the one shape where a HEALTHY configuration quantiles
   UNDER that 30 s line (the threshold sitting on the ``le="30"`` bucket
   edge) is pinned and documented instead of hidden.

The quantiles are APPROXIMATE by construction - that is the contract being
audited: the error must be bounded by the containing bucket's width (small
where the alerts read), not zero, and the shipped docs must say so.
"""

from __future__ import annotations

import json
import math
import subprocess
import sys
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")

from tests._prom_review import Exposition, parse_exposition, probe_env

pytestmark = [pytest.mark.integration, pytest.mark.otel]


# ── the served-side quantile algorithm (Prometheus's own) ───────────


def _parse_le(raw: str) -> float:
    """The bridge renders ``le`` as Go-format floats ("5e-06", "0.0025",
    "0", "+Inf") - float() parses every one of them."""
    return float(raw)


def served_histogram_quantile(q: float, buckets: list[tuple[float, float]]) -> float:
    """Prometheus's ``histogram_quantile`` over cumulative (le, count) pairs.

    Faithful reimplementation of the documented algorithm (linear
    interpolation inside the bucket containing the rank; the ``le <= 0``
    first-bucket and last-finite-bucket edges included), so the error
    bounds below are computed the way the alerts compute them. The
    promtool-evaluated marginal cases in
    ``test_prometheus_metrics_review.py`` cross-check this implementation
    against promtool itself.
    """
    if len(buckets) < 2:
        return math.nan
    cumulative = sorted(buckets, key=lambda b: b[0])
    total = cumulative[-1][1]
    rank = q * total
    # First finite bucket whose cumulative count reaches the rank.
    b = len(cumulative) - 1
    for i in range(len(cumulative) - 1):
        if cumulative[i][1] >= rank:
            b = i
            break
    if b == len(cumulative) - 1:
        # The rank falls beyond the last finite boundary: Prometheus
        # returns that boundary (no interpolation into +Inf).
        return cumulative[-2][0]
    if b == 0 and cumulative[0][0] <= 0:
        return cumulative[0][0]
    lower = cumulative[b - 1][0] if b > 0 else 0.0
    lower_count = cumulative[b - 1][1] if b > 0 else 0.0
    upper = cumulative[b][0]
    count = cumulative[b][1] - lower_count
    if count <= 0:
        return upper
    return lower + (upper - lower) * (rank - lower_count) / count


def true_quantile(q: float, samples: list[float]) -> float:
    """Nearest-rank empirical quantile of the raw synthetic samples."""
    ordered = sorted(samples)
    rank = math.ceil(q * len(ordered))
    return ordered[max(rank - 1, 0)]


def served_buckets(
    exp: Any, base: str, labels: dict[str, str] | None = None
) -> list[tuple[float, float]]:
    """The cumulative (le, count) pairs a Prometheus server ingests for
    *base*, filtered to the given label set (the single-series histograms
    pass None; the queue-labeled ones pass their label)."""
    out: list[tuple[float, float]] = []
    for s in exp.series(f"{base}_bucket"):
        if labels and any(s.labels.get(k) != v for k, v in labels.items()):
            continue
        if "le" not in s.labels:
            continue
        out.append((_parse_le(s.labels["le"]), s.value))
    out.sort()
    return out


# ── the synthetic-distribution probe ────────────────────────────────

_PROBE = '''
"""Records known synthetic distributions through the REAL emitters and dumps
the served exposition plus the raw samples (as JSON) for the honesty math."""

import json
import math
import os
import random
import sys

PROBE_DIR = os.environ["PROBE_DIR"]
sys.path.insert(0, PROBE_DIR)

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import CollectorRegistry, generate_latest

REGISTRY = CollectorRegistry()
READER = PrometheusMetricReader(registry=REGISTRY)
metrics.set_meter_provider(MeterProvider(metric_readers=[READER]))

from taskq.obs import (  # noqa: E402
    record_dispatch_duration,
    record_lock_expires_in_seconds,
    record_pool_acquire_duration,
    record_process_duration,
    record_queue_wait,
)
from taskq.obs import _otel as otel_mod  # noqa: E402
from taskq.worker import _watchdog as watchdog  # noqa: E402

otel_mod.set_otel_enabled(True)

rng = random.Random(20260927)


def lognormal(median: float, sigma: float, n: int) -> list[float]:
    return [median * math.exp(rng.gauss(0.0, sigma)) for _ in range(n)]


distributions: dict[str, list[float]] = {}

# 1. dispatch: a healthy sub-50ms fleet (median ~12ms) with a 1% tail - and
#    ONE dispatch at exactly 300ms: does the bucket scale interpolate a p99
#    around it honestly?
distributions["dispatch"] = (
    lognormal(0.012, 0.35, 3000) + [rng.uniform(0.15, 0.9) for _ in range(29)] + [0.3]
)
for v in distributions["dispatch"]:
    record_dispatch_duration("probe_queue", v)

# 2. pool acquire: healthy sub-5ms waits, 2% pool-exhausted multi-second
#    waits (the shape the histogram exists to keep OUT of the SQL read).
distributions["pool_acquire"] = lognormal(0.0004, 0.5, 1960) + [
    rng.uniform(2.0, 8.0) for _ in range(40)
]
for v in distributions["pool_acquire"]:
    record_pool_acquire_duration("probe_queue", v)

# 3. process duration: jobs 0.1s..45s with start_to_close timeouts at 60s.
distributions["process"] = lognormal(1.5, 1.0, 1495) + [60.0] * 5
for i, v in enumerate(distributions["process"]):
    record_process_duration(
        f"actor_{i % 7}", "probe_queue", v, outcome="succeeded"
    )

# 4. queue wait: sub-second healthy dispatches plus 2% stragglers at ~45s
#    (a 1% tail would put the p99 rank exactly at the healthy/tail
#    boundary; 2% puts it inside the straggler mass).
distributions["queue_wait"] = lognormal(0.02, 0.8, 1960) + [
    rng.uniform(44.0, 46.0) for _ in range(40)
]
for i, v in enumerate(distributions["queue_wait"]):
    record_queue_wait(f"actor_{i % 5}", "probe_queue", v)

# 5. lock remaining TTL: the healthy default read (lock_lease 60s minus a
#    ~10s heartbeat gap).
distributions["lock_expires"] = [rng.uniform(49.0, 50.0) for _ in range(500)]
for v in distributions["lock_expires"]:
    record_lock_expires_in_seconds("w1", v)

# 6. event-loop lag: healthy microsecond beats plus one landed 2s stall.
distributions["loop_lag"] = [
    max(1e-6, 20e-6 * math.exp(rng.gauss(0.0, 0.6))) for _ in range(2999)
] + [2.0]
for v in distributions["loop_lag"]:
    watchdog._event_loop_lag.record(v)  # pyright: ignore[reportPrivateUsage]

# 7. (The TaskQLockExpiringSoon cliff shape is NOT recorded here: the lock
#    histogram is single-series and label-less, so cliff samples would mix
#    into #5's distribution. The cliff is proven on the served bucket scale
#    in test_lock_expiring_soon_cliff_shape_quantiles_below_its_threshold
#    and by promtool on isolated series in the review suite.)

text = generate_latest(REGISTRY).decode()
with open(os.environ["PROBE_SCRAPE_PATH"], "w") as fh:
    fh.write(text)
with open(os.environ["PROBE_SAMPLES_PATH"], "w") as fh:
    json.dump(distributions, fh)
print("QUANTILE_PROBE_OK", flush=True)
'''

_FAMILIES: dict[str, tuple[str, dict[str, str] | None]] = {
    # base name -> (label filter, None = no labels / single series)
    "dispatch": ("taskq_dispatch_duration_seconds", {"queue": "probe_queue"}),
    "pool_acquire": ("taskq_dispatch_pool_acquire_duration_seconds", {"queue": "probe_queue"}),
    "process": ("messaging_process_duration_seconds", None),  # summed over actor/outcome below
    "queue_wait": ("taskq_jobs_queue_wait_seconds", None),
    "lock_expires": ("taskq_lock_expires_in_seconds", None),
    "loop_lag": ("taskq_worker_event_loop_lag_seconds", None),
}

#: The src-declared boundary scales (the claim under attack), pinned here so
#: a drift in the served buckets - or a regression to the SDK defaults -
#: fails with the exact expected scale in the message.
_DECLARED_BOUNDS: dict[str, tuple[float, ...]] = {
    "dispatch": (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    "pool_acquire": (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    "process": (
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
        600.0,
    ),
    "queue_wait": (
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
        600.0,
        1200.0,
        3600.0,
    ),
    "lock_expires": (0, 5, 10, 15, 20, 30, 45, 60),
    "loop_lag": (
        5e-06,
        1e-05,
        5e-05,
        1e-04,
        5e-04,
        1e-03,
        5e-03,
        1e-02,
        5e-02,
        1e-01,
        5e-01,
        1.0,
        5.0,
        30.0,
    ),
}


@pytest.fixture(scope="module")
def honesty(tmp_path_factory: Any) -> tuple[Exposition, dict[str, list[float]]]:
    workdir = tmp_path_factory.mktemp("prom_quantile_honesty")
    script = workdir / "probe_quantile.py"
    script.write_text(_PROBE)
    scrape_path = workdir / "quantile_scrape.txt"
    samples_path = workdir / "quantile_samples.json"
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        env=probe_env(
            PROBE_DIR=str(workdir),
            PROBE_SCRAPE_PATH=str(scrape_path),
            PROBE_SAMPLES_PATH=str(samples_path),
        ),
    )
    assert result.returncode == 0, (
        f"quantile probe failed:\nstdout={result.stdout[-3000:]}\nstderr={result.stderr[-3000:]}"
    )
    return (
        parse_exposition(scrape_path.read_text()),
        dict(json.loads(samples_path.read_text()).items()),
    )


def _served_series(exp: Any, key: str) -> list[tuple[float, float]]:
    """The served cumulative buckets for one distribution. For the
    multi-series histograms the distributions are summed the way a
    Prometheus ``histogram_quantile`` over ``sum by (le)`` would: the
    synthetic samples share the aggregate behavior being audited."""
    base, labels = _FAMILIES[key]
    if key in ("process", "queue_wait"):
        # Multi-series histograms (actor/outcome labels): sum every series'
        # counts per le, the way a fleet-wide ``histogram_quantile`` over
        # ``sum by (le)`` read does.
        merged: dict[float, float] = {}
        for s in exp.series(f"{base}_bucket"):
            if "le" not in s.labels:
                continue
            le = _parse_le(s.labels["le"])
            merged[le] = merged.get(le, 0.0) + s.value
        return sorted(merged.items())
    return served_buckets(exp, base, labels)


def _assert_honest(
    exp: Any,
    samples: list[float],
    key: str,
    quantiles: tuple[float, ...],
) -> dict[float, float]:
    """The core honesty bound: for each quantile, the served value must sit
    within the width of the bucket containing the rank (the promise a
    bucket histogram can make), and the served bucket set must be exactly
    the src-declared scale. Returns the measured errors for the report."""
    served_series = _served_series(exp, key)
    finite = [le for le, _ in served_series if math.isfinite(le)]
    expected = list(_DECLARED_BOUNDS[key])
    assert finite == sorted(expected), (
        f"{key}: served boundaries {finite} are not the declared seconds-scaled "
        f"scale {sorted(expected)} - the buckets drifted or regressed to the "
        "SDK defaults"
    )
    assert served_series[-1][0] == math.inf and served_series[-1][1] == float(len(samples)), (
        f"{key}: the +Inf bucket must carry every sample: {served_series[-1]}"
    )
    errors: dict[float, float] = {}
    # A sample's containing bucket: the first boundary >= the truth, paired
    # with the one below it (the le=0 first bucket's floor is 0).
    edges = [0.0 if finite[0] > 0 else -math.inf, *finite]
    for q in quantiles:
        truth = true_quantile(q, samples)
        containing = None
        for lo_val, hi_val in zip(edges, finite, strict=False):
            if lo_val <= truth <= hi_val:
                containing = (lo_val, hi_val)
                break
        assert containing is not None, (
            f"{key}: true p{q * 100:g}={truth} outside the declared scale"
        )
        served = served_histogram_quantile(q, served_series)
        error = abs(served - truth)
        width = containing[1] - max(containing[0], 0.0)
        assert error <= width, (
            f"{key}: served p{q * 100:g}={served:.6g} vs true {truth:.6g} - the "
            f"interpolation error {error:.6g} exceeds the containing bucket's "
            f"width {width:.6g}: the buckets are not honest for this read"
        )
        errors[q] = error
    return errors


class TestBucketScalesAreHonest:
    """Each of the six histograms, against the distribution it will
    actually see."""

    def test_dispatch_300ms_tail_interpolates_honestly(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        exp, samples = honesty
        errors = _assert_honest(exp, samples["dispatch"], "dispatch", (0.5, 0.95, 0.99))
        # The 300ms dispatch itself: with 1% tail mass the p99 rank sits in
        # the (0.025, 0.05] bucket - sub-50ms, where the alert reads.
        served_p99 = served_histogram_quantile(0.99, _served_series(exp, "dispatch"))
        assert served_p99 < 0.05, (
            f"a healthy dispatch fleet (3000 sub-50ms + 1% tail) served p99 "
            f"{served_p99:.4g} - TaskQDispatchLatencyHigh (>50ms) would page on health"
        )
        # The 300ms sample must NOT vanish into an all-in-one-bucket top:
        # it is distinguishable from the healthy mass (the le=0.25/0.5/1.0
        # buckets carry it alone).
        series = _served_series(exp, "dispatch")
        by_le = dict(series)
        assert by_le[0.05] == 3000.0 and by_le[1.0] == 3030.0, (
            "the tail mass must land past the le=0.05 boundary, resolvable "
            f"per bucket: {[(le, c) for le, c in series]}"
        )
        assert errors[0.99] <= 0.025

    def test_pool_acquire_exhaustion_tail_is_resolvable(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        exp, samples = honesty
        _assert_honest(exp, samples["pool_acquire"], "pool_acquire", (0.5, 0.95, 0.99))
        # p99 falls in the exhausted tail: it must be served ABOVE the
        # healthy sub-5ms mass (the histogram's whole point).
        served_p99 = served_histogram_quantile(0.99, _served_series(exp, "pool_acquire"))
        assert served_p99 > 1.0, (
            f"pool-exhaustion p99 served as {served_p99:.4g} - the 2s-8s waits "
            "must dominate the tail, not collapse into a top bucket"
        )

    def test_process_duration_percentiles_across_the_whole_range(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        exp, samples = honesty
        _assert_honest(exp, samples["process"], "process", (0.5, 0.95, 0.99))

    def test_queue_wait_45s_straggler_is_covered(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        exp, samples = honesty
        errors = _assert_honest(exp, samples["queue_wait"], "queue_wait", (0.5, 0.99))
        served_p99 = served_histogram_quantile(0.99, _served_series(exp, "queue_wait"))
        truth_p99 = true_quantile(0.99, samples["queue_wait"])
        # The ~45s stragglers: covered by the 30s/60s pair - the served p99
        # stays inside their bucket (error bounded by its 30s width) and on
        # the STARVED side of the healthy sub-second mass.
        assert served_p99 > 30.0, (
            f"45s queue-wait stragglers served p99 {served_p99:.4g} - the p99 "
            "read the playbook prescribes must see the stragglers"
        )
        assert abs(served_p99 - truth_p99) <= 30.0
        assert errors[0.5] <= 0.05  # healthy mass keeps sub-50ms resolution

    def test_lock_expires_healthy_read_stays_alert_silent(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        exp, samples = honesty
        errors = _assert_honest(exp, samples["lock_expires"], "lock_expires", (0.5, 0.99))
        served_p99 = served_histogram_quantile(0.99, _served_series(exp, "lock_expires"))
        # Healthy defaults (lease 60s, heartbeat 10s -> remaining ~50s): the
        # served p99 must stay ABOVE TaskQLockExpiringSoon's 30s line.
        assert served_p99 > 30.0, (
            f"healthy lock remaining-TTL served p99 {served_p99:.4g} - "
            "TaskQLockExpiringSoon would page on the default configuration"
        )
        # ...but the honest error is the (45, 60] bucket's 15s width - a
        # ~50s truth served as ~59.85s. Documented as approximate (the docs
        # table pins this scale); asserted here so it can never silently
        # widen.
        assert errors[0.99] <= 15.0
        truth_p99 = true_quantile(0.99, samples["lock_expires"])
        assert abs(served_p99 - truth_p99) <= 15.0

    def test_event_loop_lag_sub_ms_resolution_is_not_all_in_one_bucket(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        exp, samples = honesty
        errors = _assert_honest(exp, samples["loop_lag"], "loop_lag", (0.5, 0.99))
        series = _served_series(exp, "loop_lag")
        sub_ms = [(le, c) for le, c in series if math.isfinite(le) and le <= 0.001]
        # Strictly-increasing cumulative counts across the sub-millisecond
        # boundaries: healthy microsecond beats are spread over buckets,
        # not collapsed into one.
        distinct_masses = len({c for _, c in sub_ms})
        assert distinct_masses >= 4, (
            f"the sub-ms range collapsed: cumulative sub-ms bucket counts "
            f"{sub_ms} - a healthy ~20us beat quantiles into one bucket and "
            "the 'rising p99' read is dead"
        )
        served_p99 = served_histogram_quantile(0.99, series)
        # Healthy beats ~20us: p99 stays under 1ms (error bounded by the
        # containing sub-ms bucket), AND the one 2s stall is in a different
        # bucket than the healthy mass (le=5 bucket).
        assert served_p99 < 0.001, (
            f"healthy event-loop beats served p99 {served_p99:.4g} - the "
            "sub-ms resolution claim is false"
        )
        by_le = dict(series)
        # The 2s stall lands in the (1, 5] bucket - a different bucket than
        # every healthy beat (all 2999 under 1ms): the scale separates a
        # healthy microsecond fleet from a multi-second stall.
        assert by_le[0.001] == 2999.0 and by_le[1.0] == 2999.0 and by_le[5.0] == 3000.0, (
            "the 2s stall sample must land above the 1ms boundary, distinct "
            f"from the healthy mass: {[(le, c) for le, c in series]}"
        )
        assert errors[0.5] <= 4e-05  # the (1e-5, 5e-5] bucket's width

    def test_lock_expiring_soon_cliff_shape_quantiles_below_its_threshold(
        self, honesty: tuple[Exposition, dict[str, list[float]]]
    ) -> None:
        """THE FOUND CLIFF (documented, not hidden): a HEALTHY
        minimal-valid lease configuration - heartbeat_interval 5s with
        lock_lease 33s (the cascade floor for that cadence) - reads
        remaining TTL ~28s, every renewal's sample lands in the le="30"
        bucket, and the served p99 quantiles to 20 + 10 * 0.99 = 29.9 < 30:
        TaskQLockExpiringSoon pages on that healthy fleet. The threshold
        sits exactly on a bucket edge, so a boundary-straddling healthy
        cadence quantiles just BELOW it. The promtool marginal case in
        test_prometheus_metrics_review.py proves the firing on isolated
        series; this test pins the served bucket scale's part in it."""
        exp, _samples = honesty
        # The served scale, from the LIVE bridge scrape of the real
        # instrument: the le="30" edge is the cliff's landing bucket.
        finite = [le for le, _ in _served_series(exp, "lock_expires") if math.isfinite(le)]
        assert 20.0 in finite and 30.0 in finite and 45.0 in finite, finite
        # All 500 cliff samples land in le="30" (28s < 30): the served p99
        # on that bucket occupancy = 20 + 10 * 0.99 = 29.9 < 30.
        cliff_cumulative = [(le, 0.0 if le < 30 else 500.0) for le in finite if le > 0]
        served = served_histogram_quantile(0.99, cliff_cumulative)
        assert served < 30.0, (
            "the cliff shape must quantile below the 30s threshold - this "
            "assertion documents the false-page rather than hiding it"
        )
