"""Observability-cost A/B benches: get_tracer root cause, span/metric tax, attr churn.

Jobs 1/2/3/5 of the observability-cost hunt. Measured here:

  1. ``get_tracer()`` — per-call ``importlib.metadata.version("taskq-py")``
     (obs/_otel.py:77-86); cached-vs-uncached delta; whether the OTel API
     memoizes ``trace.get_tracer`` (it does not — evidence below).
  2. Span bookkeeping per job with NO SDK configured (proxy tracer) vs
     ``_otel_enabled=False`` vs stripped, plus the
     ``trace.get_current_span`` contextvar read the structlog processor pays.
  3. ``record_consumed_message`` + ``record_process_duration`` per job:
     disabled early-return vs no-provider ``_ProxyMeter`` vs real SDK at
     realistic cardinality (10 actors x 5 queues x 4 outcomes).
  5. Attribute-dict churn: fresh-dict vs interned-dict share of the
     emission tax.

Run order matters and is enforced: the no-provider phase runs BEFORE any
provider is installed, because the OTel API forbids unsetting providers.

Usage:
    python benchmarks/ab_otel_hotspots.py          # table + JSON in results/
    python benchmarks/ab_otel_hotspots.py --json   # machine-readable only
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import statistics
import sys
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, _BENCH_DIR)

import bench_hotspots as bh  # noqa: E402  # Why: house A/B harness resolved via the sys.path.insert above.
from opentelemetry import trace  # noqa: E402
from opentelemetry.metrics import set_meter_provider  # noqa: E402
from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402
from opentelemetry.sdk.trace import SpanProcessor as SdkSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402

import taskq.obs._otel as otel_mod  # noqa: E402

# ── shared fixtures ────────────────────────────────────────────────────

_CACHED_VERSION: str = importlib.metadata.version("taskq-py")
_ACTORS = [f"actor_{i}" for i in range(10)]
_QUEUES = [f"queue_{i}" for i in range(5)]
_OUTCOMES = ("succeeded", "failed", "cancelled", "abandoned")

# Same shape/values as dispatch.py:205-215 builds per job.
_JOB_ID = uuid.uuid4()  # noqa: TID251  # Why: throwaway attribute payload for timing A/Bs; PK locality is not the variable under test.
_CONSUMER_ATTRS: dict[str, str | int] = {
    "messaging.system": "taskq",
    "messaging.destination.name": "default",
    "messaging.operation.type": "process",
    "messaging.message.id": str(_JOB_ID),
    "messaging.consumer.group.name": "workers",
    "taskq.actor": "actor_0",
    "taskq.attempt": 1,
    "taskq.identity_key": "",
    "taskq.batch_id": "",
}


def solo_ns(fn: Callable[[], object], batch: int, batches: int = 7) -> float:
    """Median ns/op for one callable, interleaved against nothing."""
    for _ in range(50):
        fn()
    times = bh.time_callable(fn, batch, batches)
    return statistics.median(times)


# ── Job 1: get_tracer root cause ───────────────────────────────────────


def _patch_version() -> None:
    otel_mod._version = lambda: _CACHED_VERSION  # type: ignore[assignment]


def _unpatch_version() -> None:
    def _real() -> str:
        return importlib.metadata.version("taskq-py")

    otel_mod._version = _real  # type: ignore[assignment]


def bench_get_tracer() -> list[tuple[str, float, str]]:
    """Decompose get_tracer(): importlib.metadata share vs ProxyTracer construction."""
    rows: list[tuple[str, float, str]] = []

    # Frequency evidence: importlib.metadata.version runs once per get_tracer().
    calls = 0
    real_version = importlib.metadata.version

    def counting_version(name: str) -> str:  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return real_version(name)

    importlib.metadata.version = counting_version  # type: ignore[assignment]
    try:
        otel_mod.get_tracer()
        otel_mod.get_tracer()
        otel_mod.get_tracer()
        with otel_mod.safe_start_span("probe"):
            pass
        per_tracer, per_span = calls - 1, 1  # last call came from safe_start_span
    finally:
        importlib.metadata.version = real_version  # type: ignore[assignment]
    assert per_tracer == 3 and per_span == 1, (per_tracer, per_span)

    # Does the OTel API memoize? ProxyTracerProvider.get_tracer builds a new
    # ProxyTracer per call (opentelemetry/trace/__init__.py, ProxyTracerProvider).
    t1 = trace.get_tracer(otel_mod.INSTRUMENTATION_NAME, _CACHED_VERSION)
    t2 = trace.get_tracer(otel_mod.INSTRUMENTATION_NAME, _CACHED_VERSION)
    api_memoizes = t1 is t2

    batch = 20
    rows.append(
        (
            "importlib.metadata.version('taskq-py') alone",
            solo_ns(lambda: importlib.metadata.version("taskq-py"), batch),
            "the uncached cost get_tracer pays per call (_otel.py:79)",
        )
    )
    rows.append(
        (
            "get_tracer() as shipped (uncached _version)",
            solo_ns(otel_mod.get_tracer, batch),
            "obs/_otel.py:84-86 — calls _version() every time",
        )
    )

    _patch_version()
    try:
        memoized = solo_ns(otel_mod.get_tracer, batch)
    finally:
        _unpatch_version()
    rows.append(
        (
            "get_tracer() with memoized _version",
            memoized,
            "delta vs shipped = the importlib.metadata share",
        )
    )

    tracer_singleton = otel_mod.get_tracer()
    rows.append(
        (
            "prebound tracer singleton return",
            solo_ns(lambda: tracer_singleton, batch),
            "floor: module-level tracer, zero per-call work",
        )
    )
    rows.append(
        (
            "trace.get_tracer(name, version) fresh (memoized version)",
            solo_ns(
                lambda: trace.get_tracer(otel_mod.INSTRUMENTATION_NAME, _CACHED_VERSION),
                batch,
            ),
            "API-level cost: ProxyTracer construction per call",
        )
    )

    print(f"  otel API memoizes trace.get_tracer: {api_memoizes}")
    print("  importlib.metadata calls per get_tracer: 1, per safe_start_span: 1")
    return rows


# ── Job 2: span tax ────────────────────────────────────────────────────


def bench_span_tax_no_provider() -> list[tuple[str, float, str]]:
    """Per-job span bookkeeping with no SDK: disabled / proxy / stripped."""
    rows: list[tuple[str, float, str]] = []
    batch = 50

    def _span_disabled() -> None:
        with otel_mod.safe_start_span(
            "process actor_0", kind=SpanKind.CONSUMER, attributes=_CONSUMER_ATTRS
        ) as span:
            span.set_status(StatusCode.OK)

    def _span_proxy() -> None:
        with otel_mod.safe_start_span(
            "process actor_0", kind=SpanKind.CONSUMER, attributes=_CONSUMER_ATTRS
        ) as span:
            span.set_status(StatusCode.OK)

    def _stripped_equality() -> None:
        with nullcontext():
            pass

    # disabled: the _otel_enabled=False early return (obs/_otel.py:146-148)
    otel_mod._otel_enabled = False
    try:
        rows.append(
            (
                "safe_start_span + end, _otel_enabled=False",
                solo_ns(_span_disabled, batch),
                "the production OFF path: NonRecordingSpan yield",
            )
        )
    finally:
        otel_mod._otel_enabled = True

    # enabled with NO SDK provider: ProxyTracer path, shipped uncached _version
    rows.append(
        (
            "safe_start_span + end, no SDK (proxy tracer, shipped)",
            solo_ns(_span_proxy, batch),
            "the E2E finding: get_tracer() -> importlib per dispatch",
        )
    )

    _patch_version()
    try:
        rows.append(
            (
                "safe_start_span + end, no SDK (proxy, memoized _version)",
                solo_ns(_span_proxy, batch),
                "delta vs shipped = importlib share of the span tax",
            )
        )
    finally:
        _unpatch_version()

    rows.append(
        (
            "stripped baseline (nullcontext, no span machinery)",
            solo_ns(_stripped_equality, batch),
            "A/B floor for the span tax",
        )
    )

    # The structlog processor's contextvar read, no provider (obs/_structlog.py:48)
    rows.append(
        (
            "trace.get_current_span().get_span_context() (no provider)",
            solo_ns(lambda: trace.get_current_span().get_span_context(), 500),
            "per LOG LINE, _otel_span_processor — is_valid=False path",
        )
    )

    # attribute dict dispatch builds per job (dispatch.py:205-215)
    job_id = str(_JOB_ID)

    def _build_consumer_attrs() -> dict[str, str | int]:
        return {
            "messaging.system": "taskq",
            "messaging.destination.name": "default",
            "messaging.operation.type": "process",
            "messaging.message.id": job_id,
            "messaging.consumer.group.name": "workers",
            "taskq.actor": "actor_0",
            "taskq.attempt": 1,
            "taskq.identity_key": "",
            "taskq.batch_id": "",
        }

    rows.append(
        (
            "consumer_attrs 9-key dict build (dispatch.py:205-215)",
            solo_ns(_build_consumer_attrs, 500),
            "unconditional — runs even with _otel_enabled=False",
        )
    )
    return rows


# ── Job 3 + 5: metric tax and attr-dict churn ──────────────────────────


def _emit_pair(actor: str, queue: str) -> None:
    otel_mod.record_consumed_message(actor, queue, outcome="succeeded")
    otel_mod.record_process_duration(actor, queue, 0.001)


def bench_metric_tax_no_provider() -> list[tuple[str, float, str]]:
    """record_consumed_message + record_process_duration per job, no SDK."""
    rows: list[tuple[str, float, str]] = []
    batch = 200

    def _disabled() -> None:
        otel_mod.record_consumed_message("actor_0", "queue_0", outcome="succeeded")
        otel_mod.record_process_duration("actor_0", "queue_0", 0.001)

    otel_mod._otel_enabled = False
    try:
        rows.append(
            (
                "consumed+process pair, _otel_enabled=False",
                solo_ns(_disabled, batch),
                "early return at obs/_otel.py:345-346, 364-365",
            )
        )
    finally:
        otel_mod._otel_enabled = True

    # ProxyMeter path: instruments were created at import with no provider,
    # so _consumed_messages/_process_duration are _Proxy* no-ops.
    from opentelemetry.metrics import _internal as metrics_internal

    assert otel_mod._consumed_messages.__class__.__name__ == "_ProxyCounter"
    assert otel_mod._process_duration.__class__.__name__ == "_ProxyHistogram"
    del metrics_internal

    rows.append(
        (
            "consumed+process pair, no SDK (proxy instruments)",
            solo_ns(lambda: _emit_pair("actor_0", "queue_0"), batch),
            "fresh attr dicts per call, as production builds them",
        )
    )

    def _attrs3() -> dict[str, str]:
        return {"actor": "actor_0", "queue": "queue_0", "outcome": "succeeded"}

    def _attrs2() -> dict[str, str]:
        return {"actor": "actor_0", "queue": "queue_0"}

    rows.append(
        (
            "attr dict build, 3-key (consumed)",
            solo_ns(_attrs3, 500),
            "job 5: fresh dict per add()",
        )
    )
    rows.append(
        (
            "attr dict build, 2-key (process duration)",
            solo_ns(_attrs2, 500),
            "job 5",
        )
    )
    return rows


def _install_real_sdk() -> tuple[object, object]:  # type: ignore[no-untyped-def]
    class _DiscardProcessor(SdkSpanProcessor):
        def on_start(self, span, parent_context=None) -> None:  # type: ignore[no-untyped-def]
            pass

        def on_end(self, span) -> None:  # type: ignore[no-untyped-def]
            pass

        def shutdown(self) -> bool:
            return True

        def force_flush(self, timeout_millis=None) -> bool:  # type: ignore[no-untyped-def]
            return True

    trace_provider = TracerProvider()
    trace_provider.add_span_processor(_DiscardProcessor())
    trace.set_tracer_provider(trace_provider)

    reader = InMemoryMetricReader()
    metrics_provider = MeterProvider(metric_readers=[reader])
    set_meter_provider(metrics_provider)
    # Existing _Proxy instruments rebind in place via on_meter_set, so the
    # module singletons in obs/_otel.py are now real SDK instruments.
    return reader, metrics_provider


def _count_series(reader: object, name: str) -> int:  # type: ignore[no-untyped-def]
    data = reader.get_metrics_data()  # type: ignore[attr-defined]
    if data is None:
        return 0
    return sum(
        1
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for _p in m.data.data_points
    )


def bench_metric_tax_real_sdk(reader: object) -> list[tuple[str, float, str]]:  # type: ignore[no-untyped-def]
    """Same emission pair against the real SDK at realistic cardinality."""
    rows: list[tuple[str, float, str]] = []
    batch = 200

    # Prefill: 10 actors x 5 queues x 4 outcomes = 200 counter series,
    # 10 x 5 = 50 histogram series.
    for a in _ACTORS:
        for q in _QUEUES:
            for o in _OUTCOMES:
                otel_mod.record_consumed_message(a, q, outcome=o)
            otel_mod.record_process_duration(a, q, 0.001)
    n_consumed = _count_series(reader, "messaging.client.consumed.messages")
    n_process = _count_series(reader, "messaging.process.duration")
    assert n_consumed == 200, n_consumed
    assert n_process == 50, n_process
    reader.get_metrics_data()  # reset delta/cumulative baseline before timing

    rows.append(
        (
            "consumed+process pair, real SDK @200 series",
            solo_ns(lambda: _emit_pair("actor_0", "queue_0"), batch),
            "hot series: aggregation hit path, fresh dicts",
        )
    )

    # Interned-dict variant (job 5): prebuilt per (actor, queue, outcome).
    interned: dict[tuple[str, str, str], dict[str, str]] = {}

    def _emit_pair_interned(actor: str, queue: str) -> None:
        key3 = (actor, queue, "succeeded")
        d3 = interned.get(key3)
        if d3 is None:
            d3 = {"actor": actor, "queue": queue, "outcome": "succeeded"}
            interned[key3] = d3
        otel_mod._consumed_messages.add(1, d3)
        key2 = (actor, queue)
        d2 = interned.get(key2)  # type: ignore[assignment]
        if d2 is None:
            d2 = {"actor": actor, "queue": queue}  # type: ignore[assignment]
            interned[key2] = d2  # type: ignore[assignment]
        otel_mod._process_duration.record(0.001, d2)

    rows.append(
        (
            "consumed+process pair, real SDK, interned dicts",
            solo_ns(lambda: _emit_pair_interned("actor_0", "queue_0"), batch),
            "job 5: bounded-enum dict cache win estimate",
        )
    )

    # Cold series: a NEW label value (what a fresh queue costs per add).
    cold_i = 0

    def _emit_cold() -> None:
        nonlocal cold_i
        cold_i += 1
        otel_mod.record_consumed_message("actor_0", f"cold-queue-{cold_i}", outcome="succeeded")

    rows.append(
        (
            "single add(), real SDK, BRAND-NEW queue label",
            solo_ns(_emit_cold, 50),
            "series-creation cost — see ab_metric_labels.py for growth",
        )
    )

    # Correctness: steady-state sums land in the reader.
    _emit_pair("actor_0", "queue_0")
    data = reader.get_metrics_data()  # type: ignore[attr-defined]
    sums = {
        tuple(sorted(dict(p.attributes or {}).items())): p.value
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "messaging.client.consumed.messages"
        for p in m.data.data_points
    }
    hit = sums.get((("actor", "actor_0"), ("outcome", "succeeded"), ("queue", "queue_0")))
    assert hit is not None and hit > 0, "steady-state add() did not aggregate"

    # Scrub the cold-series pollution so the cardinality file starts clean.
    reader.get_metrics_data()
    return rows


def bench_span_tax_real_sdk() -> list[tuple[str, float, str]]:  # type: ignore[no-untyped-def]
    """safe_start_span full path against the real SDK (discard processor)."""
    rows: list[tuple[str, float, str]] = []

    def _span_real() -> None:
        with otel_mod.safe_start_span(
            "process actor_0", kind=SpanKind.CONSUMER, attributes=_CONSUMER_ATTRS
        ) as span:
            span.set_status(StatusCode.OK)

    rows.append(
        (
            "safe_start_span + end, real SDK (discard exporter)",
            solo_ns(_span_real, 50),
            "still pays the uncached _version() inside get_tracer()",
        )
    )

    _patch_version()
    try:
        rows.append(
            (
                "safe_start_span + end, real SDK, memoized _version",
                solo_ns(_span_real, 50),
                "the SDK's intrinsic span create/context/end cost",
            )
        )
    finally:
        _unpatch_version()

    # structlog processor read WITH a valid current span (2 format() calls).
    tracer = trace.get_tracer("bench", "0")
    with tracer.start_span("ambient"):
        rows.append(
            (
                "get_current_span().get_span_context(), valid ctx",
                solo_ns(lambda: trace.get_current_span().get_span_context(), 500),
                "per LOG LINE on jobs with a recording span",
            )
        )
    return rows


# ── driver ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="JSON output only")
    args = parser.parse_args()

    out: dict[str, object] = {
        "schema": 1,
        "bench": "ab_otel_hotspots",
        "python_version": sys.version.split()[0],
        "sections": {},
    }

    print("== phase 1: no provider installed (proxy instruments) ==")
    rows1 = bench_get_tracer()
    rows2 = bench_span_tax_no_provider()
    rows3 = bench_metric_tax_no_provider()

    print("== phase 2: real SDK (opentelemetry-sdk 1.44, discard/noop exporters) ==")
    reader, _provider = _install_real_sdk()
    rows4 = bench_metric_tax_real_sdk(reader)
    rows5 = bench_span_tax_real_sdk()

    out["sections"] = {
        "no_provider": rows1 + rows2 + rows3,
        "real_sdk": rows4 + rows5,
    }

    if not args.json:
        for section, rows in (("no_provider", rows1 + rows2 + rows3), ("real_sdk", rows4 + rows5)):
            print(f"\n-- {section} --")
            for name, ns, note in rows:  # type: ignore[misc]
                print(f"  {ns / 1000:12.3f} µs/op  {name}  ({note})")

    _RESULTS_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    path = _RESULTS_DIR / f"obs_hotspots-{ts}.json"
    path.write_text(json.dumps(out, indent=2))
    if not args.json:
        print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
