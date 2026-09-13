"""Metric-cardinality probe: can enqueue-side strings create unbounded series?

Job 4 of the observability-cost hunt. The pinned contract
(tests/test_rt_worker_metric_cardinality.py + tests/test_obs_metric_cardinality.py)
covers the maintenance emitters, which take closed enums only. This probe
exercises the JOB-SIDE emitters, whose ``queue``/``actor`` dimension values
originate from enqueue calls:

    obs/_otel.py:303   record_published_message(actor, queue)   <- enqueue
    obs/_otel.py:321   record_dispatch_duration(queue)          <- dispatch
    obs/_otel.py:347   record_consumed_message(actor, queue, outcome)
    obs/_otel.py:366   record_process_duration(actor, queue)
    obs/_otel.py:536   _observe_queue_depth -> Observation(depth, {"queue": queue})

Queue names are charset-validated at enqueue (backend/_protocol.py:270-272:
``[A-Za-z0-9_][A-Za-z0-9_.-]*``) but have NO length cap and NO registry:
the name is user-supplied per enqueue call. src/taskq configures no OTel
Views (grep: only tests/tasking create providers), and the SDK applies no
per-label cardinality limit, so every distinct queue name mints a new
series on four instruments, per worker process.

Usage:
    python benchmarks/ab_metric_labels.py            # table + JSON in results/
    python benchmarks/ab_metric_labels.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, _BENCH_DIR)

from opentelemetry.metrics import (  # noqa: E402  # Why: the sys.path.insert above resolves sibling bench modules first.
    CallbackOptions,
    set_meter_provider,
)
from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402

import taskq.obs._otel as otel_mod  # noqa: E402
from taskq.backend._protocol import _validate_queue_name  # noqa: E402

_ACTORS = [f"actor_{i}" for i in range(10)]
_OUTCOMES = ("succeeded", "failed", "cancelled", "abandoned")


def _series(reader: InMemoryMetricReader, name: str) -> list[dict]:
    data = reader.get_metrics_data()
    assert data is not None
    return [
        dict(p.attributes or {})
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for p in m.data.data_points
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="JSON output only")
    args = parser.parse_args()

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    set_meter_provider(provider)
    otel_mod._otel_enabled = True

    # ── static evidence: enqueue-side queue names are registry-free ──
    long_name = "q" * 500
    _validate_queue_name(long_name)  # no length cap -> passes
    verdict_charset = "charset-checked, NO length cap, NO registry (user-supplied per enqueue)"

    out: dict[str, object] = {
        "schema": 1,
        "bench": "ab_metric_labels",
        "queue_dimension_verdict": verdict_charset,
        "views_configured_in_src": False,
        "scenarios": [],
    }

    # ── scenario A: bounded fleet (10 actors x 5 queues) ──────────────
    n_jobs = 0
    t0 = time.perf_counter()
    for _ in range(10):
        for a in _ACTORS:
            for q in (f"queue_{i}" for i in range(5)):
                for o in _OUTCOMES:
                    otel_mod.record_consumed_message(a, q, outcome=o)
                otel_mod.record_process_duration(a, q, 0.001)
                n_jobs += 1
    bounded_s = time.perf_counter() - t0
    series_a_consumed = _series(reader, "messaging.client.consumed.messages")
    series_a_process = _series(reader, "messaging.process.duration")
    assert len(series_a_consumed) == 10 * 5 * 4, len(series_a_consumed)
    assert len(series_a_process) == 10 * 5, len(series_a_process)
    print(
        f"A bounded:   {n_jobs} jobs -> {len(series_a_consumed)} consumed series "
        f"(+{len(series_a_process)} histogram), {bounded_s * 1000:.0f} ms"
    )

    # ── scenario B: busy/malicious tenant, one queue per user id ─────
    n_bound_users = 5000
    n_jobs_b = 0
    t0 = time.perf_counter()
    for user in range(n_bound_users):
        q = f"user-{user}"  # passes _validate_queue_name, like queue="user-<id>"
        for a in _ACTORS:
            otel_mod.record_consumed_message(a, q, outcome="succeeded")
            otel_mod.record_process_duration(a, q, 0.001)
            n_jobs_b += 1
    unbounded_s = time.perf_counter() - t0
    series_b = _series(reader, "messaging.client.consumed.messages")
    series_b_process = _series(reader, "messaging.process.duration")
    assert len(series_b) == 200 + n_bound_users * 10, len(series_b)
    assert len(series_b_process) == 50 + n_bound_users * 10, len(series_b_process)

    # Scrape cost growth: the same collection now walks 50k data points.
    t0 = time.perf_counter()
    _series(reader, "messaging.client.consumed.messages")
    _series(reader, "messaging.process.duration")
    scrape_s = time.perf_counter() - t0
    print(
        f"B unbounded: {n_jobs_b} jobs over {n_bound_users} queues -> "
        f"{len(series_b)} consumed series (+{len(series_b_process)} histogram), "
        f"{unbounded_s * 1000:.0f} ms emit; full scrape {scrape_s * 1000:.0f} ms"
    )

    # ── scenario C: the queue-depth gauge callback (leader, 15s cadence) ──
    otel_mod.update_queue_depth_cache({f"user-{u}": u for u in range(n_bound_users)})
    t0 = time.perf_counter()
    observations = list(otel_mod._observe_queue_depth(CallbackOptions()))
    gauge_s = time.perf_counter() - t0
    print(
        f"C gauge:     queue-depth callback yields {len(observations)} observations "
        f"per 15s scrape, {gauge_s * 1000:.1f} ms per scrape"
    )

    out["scenarios"] = [
        {
            "name": "bounded 10 actors x 5 queues",
            "jobs": n_jobs,
            "consumed_series": len(series_a_consumed),
            "process_series": len(series_a_process),
            "emit_s": round(bounded_s, 3),
        },
        {
            "name": f"one queue per user id x{n_bound_users} users",
            "jobs": n_jobs_b,
            "consumed_series": len(series_b),
            "process_series": len(series_b_process),
            "emit_s": round(unbounded_s, 3),
            "scrape_s": round(scrape_s, 3),
        },
        {
            "name": "queue-depth gauge callback",
            "observations_per_scrape": len(observations),
            "scrape_s": round(gauge_s, 4),
        },
    ]

    if not args.json:
        print(f"\nverdict: queue dimension is {verdict_charset}")
        print(
            "series growth: consumed.messages = actors x queues x outcomes; "
            "process.duration = actors x queues; no View caps it in src/"
        )

    _RESULTS_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    path = _RESULTS_DIR / f"metric_labels-{ts}.json"
    path.write_text(json.dumps(out, indent=2))
    if not args.json:
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
