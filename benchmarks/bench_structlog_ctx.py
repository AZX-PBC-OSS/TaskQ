"""structlog contextvars / span-context injection cost per log line.

Job 6 of the observability-cost hunt. Measures, on the production
processor chain from ``setup_logging`` (obs/_structlog.py:166-187):

  - ``merge_contextvars`` per line (worker_id is bound once at bootstrap,
    _bootstrap.py:1003 — typically 1 live contextvar)
  - ``_otel_span_processor`` per line: the
    ``trace.get_current_span().get_span_context()`` read, invalid (no span)
    and valid (recording span) variants
  - the full chain render per line via a bound logger (I/O excluded:
    root handler stream pointed at devnull)
  - ``bind_job_context`` per job (obs/_structlog.py:257-289) — the per-job
    BoundLogger allocation

Materiality is reported at 1k lines/sec: per-line µs x 1000 = ms of CPU
per second.

Usage:
    python benchmarks/bench_structlog_ctx.py          # table + JSON in results/
    python benchmarks/bench_structlog_ctx.py --json
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
import uuid
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_RESULTS_DIR = _BENCH_DIR / "results"
sys.path.insert(0, _BENCH_DIR)

import bench_hotspots as bh  # noqa: E402  # Why: house A/B harness resolved via the sys.path.insert above.
import structlog  # noqa: E402

from taskq.obs._structlog import _otel_span_processor, bind_job_context, setup_logging  # noqa: E402


def solo_ns(fn, batch: int, batches: int = 7) -> float:  # type: ignore[no-untyped-def]
    for _ in range(50):
        fn()
    times = bh.time_callable(fn, batch, batches)
    return statistics.median(times)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="JSON output only")
    args = parser.parse_args()

    setup_logging(level="INFO", log_format="json")

    # Exclude terminal/file I/O: keep the full processor chain + renderer,
    # replace only the stream. Matches how dispatch measures are normally run.
    devnull = open("/dev/null", "w")  # noqa: SIM115
    for h in logging.root.handlers:
        if isinstance(h, logging.StreamHandler):
            h.stream = devnull

    log = structlog.get_logger("taskq.bench")
    bound = bind_job_context(
        log,
        job_id=uuid.uuid4(),  # noqa: TID251  # Why: throwaway payload id; PK locality is not the variable under test.
        actor="actor_0",
        queue="queue_0",
        attempt=1,
        identity_key=None,
        trace_id="",
    )

    # merge_contextvars solo: worker_id bound at bootstrap (1 live var).
    structlog.contextvars.bind_contextvars(worker_id="w-1234")
    event_dict: dict[str, object] = {"event": "job-start", "kind": "state_change"}

    def _merge() -> dict[str, object]:
        d = dict(event_dict)
        return structlog.contextvars.merge_contextvars(None, "info", d)

    # _otel_span_processor solo, no span (is_valid=False path).
    def _otel_proc_invalid() -> dict[str, object]:
        d = dict(event_dict)
        return _otel_span_processor(None, "info", d)

    rows: list[tuple[str, float, str]] = []
    rows.append(
        (
            "merge_contextvars (1 var bound)",
            solo_ns(_merge, 500),
            "first shared processor, every line",
        )
    )
    rows.append(
        (
            "_otel_span_processor, no span (invalid ctx)",
            solo_ns(_otel_proc_invalid, 500),
            "obs/_structlog.py:48-49 short-circuit",
        )
    )
    rows.append(
        (
            "bind_job_context (per job)",
            solo_ns(
                lambda: bind_job_context(
                    log,
                    job_id=uuid.uuid4(),  # noqa: TID251  # Why: throwaway payload id; PK locality is not the variable under test.
                    actor="actor_0",
                    queue="queue_0",
                    attempt=1,
                    identity_key="idem-key",
                    trace_id="0" * 32,
                    span_id="0" * 16,
                    batch_id="b-1",
                ),
                500,
            ),
            "obs/_structlog.py:257-289 — one per dispatch + one per handler",
        )
    )

    # Full chain per line, bound logger (the per-job allocation reused).
    def _line_bound() -> None:
        bound.info("job-start", kind="job_start")

    rows.append(
        (
            "full chain render, bound job logger",
            solo_ns(_line_bound, 100),
            "all shared processors + wrap_for_formatter + ProcessorFormatter JSON",
        )
    )

    # Full chain with a valid OTel span active (real SDK provider).
    from opentelemetry import trace
    from opentelemetry.sdk.trace import SpanProcessor as SdkSpanProcessor
    from opentelemetry.sdk.trace import TracerProvider

    class _DiscardProcessor(SdkSpanProcessor):
        def on_start(self, span, parent_context=None) -> None:  # type: ignore[no-untyped-def]
            pass

        def on_end(self, span) -> None:  # type: ignore[no-untyped-def]
            pass

        def shutdown(self) -> bool:
            return True

        def force_flush(self, timeout_millis=None) -> bool:  # type: ignore[no-untyped-def]
            return True

    provider = TracerProvider()
    provider.add_span_processor(_DiscardProcessor())
    trace.set_tracer_provider(provider)

    tracer = trace.get_tracer("bench", "0")
    with tracer.start_as_current_span("process actor_0"):
        rows.append(
            (
                "_otel_span_processor, recording span (valid ctx)",
                solo_ns(_otel_proc_invalid, 500),
                "two format() calls: trace_id 032x + span_id 016x",
            )
        )
        rows.append(
            (
                "full chain render, bound job logger, span active",
                solo_ns(_line_bound, 100),
                "the per-line cost for jobs with a recording span",
            )
        )

    out = {
        "schema": 1,
        "bench": "bench_structlog_ctx",
        "rows": [{"name": n, "ns": ns, "note": note} for n, ns, note in rows],
        "at_1k_lines_per_sec_ms_cpu": {
            n: round(ns / 1000 * 1000 / 1000, 2)  # µs * 1000 lines = ms
            for n, ns, _ in rows
            if n.startswith("full chain")
        },
    }

    if not args.json:
        for name, ns, note in rows:
            print(f"  {ns / 1000:12.3f} µs/op  {name}  ({note})")
        print("\nmateriality @1k lines/sec (ms CPU per second):")
        for n, ms in out["at_1k_lines_per_sec_ms_cpu"].items():  # type: ignore[union-attr]
            print(f"  {ms:8.2f} ms/s   {n}")

    _RESULTS_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    path = _RESULTS_DIR / f"structlog_ctx-{ts}.json"
    path.write_text(json.dumps(out, indent=2))
    if not args.json:
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
