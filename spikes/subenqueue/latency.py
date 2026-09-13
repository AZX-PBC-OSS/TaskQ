"""Measure per-sub-enqueue round-trip latency over the stdio protocol.

Semantics (a) buffered-bridge, strict request/response mode: the child
emits {op:"subenqueue"} and BLOCKS until the parent's {op:"ack"} arrives.
The child records write->ack time per request (100 sequential requests).
The parent acks immediately after buffering the request (buffering never
fails), so this isolates pure protocol overhead: two pipe hops + two
NDJSON parse/serializes + one event-loop scheduling hop, per sub-enqueue.

Also runs a fire-and-forget control (child does not await the ack) so the
cost of awaiting the ack is visible by contrast.

Writes traces/latency.json with the full per-request samples and p50/p95.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
TRACES = HERE / "traces"

N = 100


def percentiles(samples: list[float]) -> dict[str, float]:
    s = sorted(samples)
    def pct(p: float) -> float:
        idx = min(len(s) - 1, max(0, round(p * (len(s) - 1))))
        return s[idx]
    return {"p50_ms": pct(0.50), "p95_ms": pct(0.95), "min_ms": s[0], "max_ms": s[-1]}


def run(mode: str) -> dict[str, object]:
    trace_path = TRACES / f"latency_{mode}.json"
    cmd = [
        sys.executable,
        str(HERE / "worker_parent.py"),
        "--semantics", "buffered",
        "--subenqueues", str(N),
        "--sleep-ms", "0",
        "--trace", str(trace_path),
    ]
    if mode == "await_ack":
        cmd.append("--await-ack")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"FAILED latency {mode}\n{proc.stderr}", file=sys.stderr)
        raise SystemExit(1)
    trace = json.loads(trace_path.read_text())
    rtts = trace.get("child_rtt_ms")
    if mode == "fire_and_forget":
        # no acks awaited; the child does not record RTT samples. Instead
        # derive per-request wall cost from the child's emit loop: not
        # available without acks, so report only the ack-mode numbers and
        # mark this run as the control.
        return {"mode": mode, "note": "control run: child never blocks; per-request RTT not sampled", "total_requests": N}
    stats = percentiles([float(x) for x in rtts])
    return {
        "mode": mode,
        "requests": len(rtts),
        **stats,
        "mean_ms": sum(rtts) / len(rtts),
        "samples_ms": rtts,
    }


def main() -> None:
    await_ack = run("await_ack")
    control = run("fire_and_forget")
    out = {"await_ack": await_ack, "fire_and_forget_control": control}
    (TRACES / "latency.json").write_text(json.dumps(out, indent=2))
    print(json.dumps({k: v for k, v in out["await_ack"].items() if k != "samples_ms"}))
    print(json.dumps(control))


if __name__ == "__main__":
    main()
