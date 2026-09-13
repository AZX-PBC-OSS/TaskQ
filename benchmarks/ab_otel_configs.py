"""SPIKE A — OTel SDK per-configuration cost at realistic TaskQ cardinality.

Questions (from the perf-rust-hotspots spike brief):
  1. What is INSIDE the measured 15.8 µs/job span tax and ~1.35 µs metric
     add()?  cProfile + pyinstrument at 10k spans: BatchSpanProcessor queue
     overhead, attribute-dict handling, Context machinery.
  2. Is there a cheaper configuration that changes per-job cost?
       - span processors: BatchSpanProcessor vs SimpleSpanProcessor
       - samplers: default ParentBased(ALWAYS_ON) vs ALWAYS_OFF vs
         TraceIdRatioBased(0.1)
       - queue/latency tuning on BatchSpanProcessor
       - metric readers: InMemory (cumulative) vs DELTA temporality vs
         PeriodicExportingMetricReader @ 60 s / 5 s / 0.5 s vs
         PrometheusMetricReader (the pull-based contrib/prometheus path)
  3. What does the export thread cost the hot path while it runs?

Isolation rules:
  - No global provider is set for traces (each TracerProvider instance is
    used directly via provider.get_tracer), so configurations cannot leak.
  - Metric instruments are rebuilt from each candidate provider; the global
    meter provider is never replaced (avoids _Proxy rebinding side effects).
  - Same 9-key attribute dict shape dispatch.py builds per job, fresh dict
    per call (production style), 200 pre-existing series on the metric side.

Usage:
    python benchmarks/ab_otel_configs.py            # table + JSON in results/
    python benchmarks/ab_otel_configs.py --json     # machine-readable only
"""

from __future__ import annotations

import argparse
import cProfile
import io
import json
import pstats
import statistics
import sys
import time
import uuid
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, str(_BENCH_DIR.parent))  # for `import taskq`

from opentelemetry.exporter.prometheus import PrometheusMetricReader  # noqa: E402
from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics._internal.instrument import (  # noqa: E402
    Counter as _SdkCounter,
)
from opentelemetry.sdk.metrics._internal.instrument import (  # noqa: E402
    Histogram as _SdkHistogram,
)
from opentelemetry.sdk.metrics._internal.instrument import (  # noqa: E402
    UpDownCounter as _SdkUpDownCounter,
)
from opentelemetry.sdk.metrics.export import (  # noqa: E402
    AggregationTemporality,
    InMemoryMetricReader,
    MetricExporter,
    MetricExportResult,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import (  # noqa: E402
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.sampling import (  # noqa: E402
    ALWAYS_OFF,
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402

N_SPANS = 10_000
N_METRICS = 10_000
ROUNDS = 5

# Same shape/values as dispatch.py:205-215 builds per job.
_JOB_ID = uuid.uuid4()  # noqa: TID251  # Why: throwaway attribute payload for timing A/Bs; PK locality is not the variable under test.
_JOB_ID_STR = str(_JOB_ID)


def _fresh_consumer_attrs() -> dict[str, str | int]:
    return {
        "messaging.system": "taskq",
        "messaging.destination.name": "default",
        "messaging.operation.type": "process",
        "messaging.message.id": _JOB_ID_STR,
        "messaging.consumer.group.name": "workers",
        "taskq.actor": "actor_0",
        "taskq.attempt": 1,
        "taskq.identity_key": "",
        "taskq.batch_id": "",
    }


class _DiscardExporter(SpanExporter):
    def export(self, spans):  # type: ignore[no-untyped-def]
        return SpanExportResult.SUCCESS


class _NullMetricExporter(MetricExporter):
    """Stand-in exporter for readers whose storage is the reader itself."""

    def export(self, metrics_data, timeout_millis=10000, **kwargs):  # type: ignore[no-untyped-def]
        return MetricExportResult.SUCCESS

    def shutdown(self, timeout_millis=30000, **kwargs):  # type: ignore[no-untyped-def]
        return True

    def force_flush(self, timeout_millis=10000, **kwargs):  # type: ignore[no-untyped-def]
        return True


class _SinkReader(MetricReader):
    """Minimal concrete MetricReader: receives, retains nothing."""

    def __init__(self, preferred_temporality=None):  # type: ignore[no-untyped-def]
        super().__init__(preferred_temporality=preferred_temporality)

    def _receive_metrics(self, metrics, timeout_millis=None):  # type: ignore[no-untyped-def]
        return None

    def shutdown(self, timeout_millis=None):  # type: ignore[no-untyped-def]
        return True


# ── span configurations ───────────────────────────────────────────────


def _tracer_for(sampler=None, processor=None, **bsp_kwargs):  # type: ignore[no-untyped-def]
    provider = TracerProvider(sampler=sampler) if sampler is not None else TracerProvider()
    if processor is None:
        processor = BatchSpanProcessor(_DiscardExporter(), **bsp_kwargs)
    provider.add_span_processor(processor)
    return provider.get_tracer("taskq", "0.0.0"), provider


def _span_loop(tracer) -> float:  # type: ignore[no-untyped-def]
    """One pass of N_SPANS job-shaped spans; returns µs/span."""
    start_as_current = tracer.start_as_current_span
    for _ in range(N_SPANS):
        with start_as_current(
            "process actor_0",
            context=None,
            kind=SpanKind.CONSUMER,
            attributes=_fresh_consumer_attrs(),
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            span.set_status(StatusCode.OK)
    return 0.0


def measure_span_config(name: str, sampler=None, processor=None, **bsp_kwargs):  # type: ignore[no-untyped-def]
    tracer, provider = _tracer_for(sampler=sampler, processor=processor, **bsp_kwargs)
    _span_loop(tracer)  # warmup (JIT-ish caches, thread startup for BSP)
    per_ops = []
    for _ in range(ROUNDS):
        t0 = time.perf_counter_ns()
        _span_loop(tracer)
        per_ops.append((time.perf_counter_ns() - t0) / N_SPANS / 1000.0)  # µs
    note = bsp_kwargs or {}
    row = {
        "name": name,
        "us_per_span_median": round(statistics.median(per_ops), 3),
        "us_per_span_min": round(min(per_ops), 3),
        "us_per_span_max": round(max(per_ops), 3),
        "note": json.dumps(note) if note else "",
    }
    provider.shutdown()
    return row


def profile_batch_span_processor() -> dict[str, str]:
    """cProfile + pyinstrument the default-BSP config at 10k spans."""
    tracer, provider = _tracer_for()  # default: BatchSpanProcessor
    prof = cProfile.Profile()
    prof.enable()
    _span_loop(tracer)
    prof.disable()
    provider.shutdown()

    out: dict[str, str] = {}
    for key, order in (("cumulative", "cumulative"), ("internal", "tottime")):
        s = io.StringIO()
        st = pstats.Stats(prof, stream=s)
        st.sort_stats(order)
        st.print_stats(28)
        out[f"cprofile_{key}"] = s.getvalue()
    try:
        from pyinstrument import Profiler

        tracer2, provider2 = _tracer_for()
        p = Profiler()
        p.start()
        _span_loop(tracer2)
        p.stop()
        provider2.shutdown()
        out["pyinstrument"] = p.output_text(unicode=False, color=False)
    except ImportError:
        out["pyinstrument"] = "(pyinstrument not installed)"
    return out


# ── metric configurations ─────────────────────────────────────────────

_ACTORS = [f"actor_{i}" for i in range(10)]
_QUEUES = [f"queue_{i}" for i in range(5)]
_OUTCOMES = ("succeeded", "failed", "cancelled", "abandoned")


def _prefill_series(counter, histogram) -> None:
    """Create 200 counter + 50 histogram series (same as ab_otel_hotspots)."""
    for a in _ACTORS:
        for q in _QUEUES:
            for o in _OUTCOMES:
                counter.add(1, {"actor": a, "queue": q, "outcome": o})
            histogram.record(0.001, {"actor": a, "queue": q})


def _metric_loop(counter, histogram) -> None:
    for i in range(N_METRICS):
        a = _ACTORS[i % 10]
        q = _QUEUES[i % 5]
        counter.add(1, {"actor": a, "queue": q, "outcome": "succeeded"})
        histogram.record(0.001, {"actor": a, "queue": q})


def measure_metric_config(name: str, reader) -> dict[str, object]:  # type: ignore[no-untyped-def]
    """Time 10k emit pairs (fresh dicts, production style) under *reader*."""
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("taskq", "0.0.0")
    counter = meter.create_counter("messaging.client.consumed.messages")
    histogram = meter.create_histogram("messaging.process.duration")
    _prefill_series(counter, histogram)

    per_ops = []
    for _ in range(ROUNDS):
        t0 = time.perf_counter_ns()
        _metric_loop(counter, histogram)
        per_ops.append((time.perf_counter_ns() - t0) / N_METRICS / 1000.0)  # µs
    row: dict[str, object] = {
        "name": name,
        "us_per_pair_median": round(statistics.median(per_ops), 3),
        "us_per_pair_min": round(min(per_ops), 3),
        "us_per_pair_max": round(max(per_ops), 3),
    }
    provider.shutdown()
    return row


def measure_pemr_contention(interval_ms: int) -> dict[str, object]:
    """add() cost WHILE the export thread wakes every *interval_ms*.

    Reports mean/p95/pair-max across one long pass — export-thread wakeups
    (collection lock) show up in the tail, not the median.
    """
    reader = PeriodicExportingMetricReader(
        _NullMetricExporter(), export_interval_millis=interval_ms
    )
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("taskq", "0.0.0")
    counter = meter.create_counter("messaging.client.consumed.messages")
    histogram = meter.create_histogram("messaging.process.duration")
    _prefill_series(counter, histogram)

    samples: list[float] = []
    for i in range(N_METRICS):
        a = _ACTORS[i % 10]
        q = _QUEUES[i % 5]
        t0 = time.perf_counter_ns()
        counter.add(1, {"actor": a, "queue": q, "outcome": "succeeded"})
        histogram.record(0.001, {"actor": a, "queue": q})
        samples.append((time.perf_counter_ns() - t0) / 1000.0)
    samples.sort()
    p95 = samples[int(len(samples) * 0.95)]
    row: dict[str, object] = {
        "name": f"PeriodicExportingMetricReader @{interval_ms}ms (export-thread contention)",
        "us_per_pair_median": round(statistics.median(samples), 3),
        "us_per_pair_p95": round(p95, 3),
        "us_per_pair_max": round(samples[-1], 3),
    }
    provider.shutdown()
    return row


# ── driver ────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="JSON output only")
    args = parser.parse_args()

    out: dict[str, object] = {
        "schema": 1,
        "bench": "ab_otel_configs",
        "python_version": sys.version.split()[0],
        "n_spans": N_SPANS,
        "n_metric_pairs": N_METRICS,
        "spans": [],
        "metrics": [],
        "profiles": {},
    }

    # -- spans ----------------------------------------------------------
    span_rows = [
        measure_span_config("BatchSpanProcessor (default queue=2048, delay=5s)"),
        measure_span_config(
            "BatchSpanProcessor tuned (queue=65536, delay=500ms)",
            max_queue_size=65536,
            schedule_delay_millis=500,
        ),
        measure_span_config("SimpleSpanProcessor (no queue thread)"),
        measure_span_config("BatchSpanProcessor + ALWAYS_OFF sampler", sampler=ALWAYS_OFF),
        measure_span_config(
            "BatchSpanProcessor + TraceIdRatioBased(0.1)",
            sampler=TraceIdRatioBased(0.1),
        ),
        measure_span_config(
            "BatchSpanProcessor + ParentBased(TraceIdRatioBased(0.1))",
            sampler=ParentBased(TraceIdRatioBased(0.1)),
        ),
    ]
    out["spans"] = span_rows

    # -- metrics --------------------------------------------------------
    prom_reader_kwargs = {}
    import inspect

    if "registry" in inspect.signature(PrometheusMetricReader.__init__).parameters:
        from prometheus_client import CollectorRegistry

        prom_reader_kwargs["registry"] = CollectorRegistry()

    metric_rows = [
        measure_metric_config(
            "InMemoryMetricReader (cumulative) [prior baseline config]",
            InMemoryMetricReader(),
        ),
        measure_metric_config(
            "MetricReader cumulative (raw sink, no snapshot helper)",
            _SinkReader(),
        ),
        measure_metric_config(
            "MetricReader DELTA temporality",
            _SinkReader(
                preferred_temporality={
                    _SdkCounter: AggregationTemporality.DELTA,
                    _SdkUpDownCounter: AggregationTemporality.DELTA,
                    _SdkHistogram: AggregationTemporality.DELTA,
                }
            ),
        ),
        measure_metric_config(
            "PeriodicExportingMetricReader @60s (default interval)",
            PeriodicExportingMetricReader(_NullMetricExporter(), export_interval_millis=60000),
        ),
        measure_metric_config(
            "PeriodicExportingMetricReader @5s",
            PeriodicExportingMetricReader(_NullMetricExporter(), export_interval_millis=5000),
        ),
        measure_metric_config(
            "PrometheusMetricReader (pull, cumulative) — contrib/prometheus path",
            PrometheusMetricReader("taskq", **prom_reader_kwargs),
        ),
        measure_pemr_contention(500),
    ]
    out["metrics"] = metric_rows

    # -- profiles -------------------------------------------------------
    out["profiles"] = profile_batch_span_processor()

    if not args.json:
        print("\n== span configurations (µs/span, 10k spans, 1 span/job) ==")
        for r in span_rows:  # type: ignore[union-attr]
            print(f"  {r['us_per_span_median']:9.3f}  {r['name']}")
        print("\n== metric configurations (µs per consumed+process pair, 10k pairs) ==")
        for r in metric_rows:  # type: ignore[union-attr]
            tail = (
                f"  p95={r['us_per_pair_p95']} max={r['us_per_pair_max']}"
                if "us_per_pair_p95" in r
                else f"  min={r['us_per_pair_min']} max={r['us_per_pair_max']}"
            )
            print(f"  {r['us_per_pair_median']:9.3f}  {r['name']}{tail}")

    _RESULTS_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    path = _RESULTS_DIR / f"otel_configs-{ts}.json"
    path.write_text(json.dumps(out, indent=2))
    if not args.json:
        print(f"\nwrote {path}")
        print("profile artifacts: cProfile top-28 + pyinstrument are in the JSON")


if __name__ == "__main__":
    main()
