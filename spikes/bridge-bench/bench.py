#!/usr/bin/env python3
"""TaskQ spike: Python asyncio <-> Node child-process actor bridge microbenchmark.

Measures four execution modes for running a TypeScript/Node actor from the
Python worker, plus cancellation latency of the child process:

  inproc   baseline: an async Python function called directly (the floor).
  cold     `node worker.js oneshot` spawned per job (fresh process each job).
  warm1    one persistent `node worker.js server` child, strictly sequential
           jobs, NDJSON over stdio.
  warm4    pool of 4 persistent children, 20 concurrent in-flight jobs
           (pool saturation scenario). Reports per-job wall time
           (dispatch->response) and pure in-pool wait (dispatch until a child
           is free) separately.
  warmt1   optional variant: one persistent child in `threads` mode, i.e. a
           worker_threads Worker spawned per job inside the child.

Cancellation:
  * SIGKILL the process group of a persistent child (10 trials): signal -> reaped.
  * SIGTERM grace: does node exit on SIGTERM while blocked on stdin read?
    Escalate to SIGKILL after 5s if it does not.

Payloads: ~100 B, ~10 KB, ~1 MB JSON objects `{"n": 42, "data": "<ascii>"}`.
200 measured jobs per mode per size after 20 warmup jobs.

Output: markdown table on stdout, raw per-job samples in results.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import os
import platform
import random
import shutil
import signal
import string
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORKER = HERE / "worker.js"

SIZES = {"100B": 100, "10KB": 10_000, "1MB": 1_000_000}
STREAM_LIMIT = 64 * 1024 * 1024  # asyncio stream limit; must exceed 1 MB lines


def make_payload(target_bytes: int, rng: random.Random) -> bytes:
    """JSON line `{"n":42,"data":"<ascii>"}` of ~target_bytes, NDJSON-safe."""
    alphabet = string.ascii_letters + string.digits
    overhead = len(json.dumps({"n": 42, "data": ""}, separators=(",", ":"))) + 1
    data = "".join(rng.choices(alphabet, k=max(1, target_bytes - overhead)))
    return (json.dumps({"n": 42, "data": data}, separators=(",", ":")) + "\n").encode("ascii")


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def summarize(lat_ms: list[float]) -> dict:
    s = sorted(lat_ms)
    return {
        "n": len(s),
        "p50_ms": round(percentile(s, 50), 3),
        "p95_ms": round(percentile(s, 95), 3),
        "p99_ms": round(percentile(s, 99), 3),
        "min_ms": round(s[0], 3),
        "max_ms": round(s[-1], 3),
        "mean_ms": round(sum(s) / len(s), 3),
    }


# ---------------------------------------------------------------- handlers --

async def handle_inproc(payload: bytes) -> dict:
    """Mirror of worker.js handle(): parse payload, derive sum, echo back."""
    obj = json.loads(payload)
    return {"ok": True, "sum": obj["n"] + len(obj["data"]), "echo": obj}


def validate(resp: dict, payload: bytes) -> None:
    obj = json.loads(payload)
    assert resp.get("ok") is True, f"bad response: {resp!r}"
    assert resp["sum"] == obj["n"] + len(obj["data"]), "wrong sum"
    assert resp["echo"] == obj, "echo mismatch"


# ------------------------------------------------------------ child helpers --

async def spawn_runtime(node: str, mode: str, new_session: bool = False) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        node, str(WORKER), mode,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        limit=STREAM_LIMIT,
        start_new_session=new_session,
    )


async def call(proc: asyncio.subprocess.Process, payload: bytes) -> dict:
    proc.stdin.write(payload)
    await proc.stdin.drain()
    line = await proc.stdout.readline()
    if not line:
        raise RuntimeError("runtime closed stdout")
    return json.loads(line)


async def stop(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


# ----------------------------------------------------------------- benches --

async def bench_inproc(payload: bytes, jobs: int, warmup: int) -> tuple[list[float], float]:
    for _ in range(warmup):
        await handle_inproc(payload)
    lat: list[float] = []
    t_start = time.perf_counter()
    for _ in range(jobs):
        t0 = time.perf_counter()
        await handle_inproc(payload)
        lat.append((time.perf_counter() - t0) * 1000.0)
    return lat, time.perf_counter() - t_start


async def bench_cold(payload: bytes, jobs: int, warmup: int, node: str) -> tuple[list[float], float]:
    for _ in range(warmup):
        p = await asyncio.create_subprocess_exec(
            node, str(WORKER), "oneshot",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        out, _ = await p.communicate(payload)
        assert p.returncode == 0
        validate(json.loads(out), payload)

    lat: list[float] = []
    last_out = b""
    t_start = time.perf_counter()
    for _ in range(jobs):
        t0 = time.perf_counter()
        p = await asyncio.create_subprocess_exec(
            node, str(WORKER), "oneshot",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        last_out, _ = await p.communicate(payload)
        lat.append((time.perf_counter() - t0) * 1000.0)
        assert p.returncode == 0
    total = time.perf_counter() - t_start
    validate(json.loads(last_out), payload)
    return lat, total


async def bench_warm_seq(payload: bytes, jobs: int, warmup: int, node: str, mode: str) -> tuple[list[float], float]:
    proc = await spawn_runtime(node, mode)
    try:
        for _ in range(warmup):
            validate(await call(proc, payload), payload)
        lat: list[float] = []
        t_start = time.perf_counter()
        for _ in range(jobs):
            t0 = time.perf_counter()
            await call(proc, payload)
            lat.append((time.perf_counter() - t0) * 1000.0)
        total = time.perf_counter() - t_start
    finally:
        await stop(proc)
    return lat, total


async def bench_warm_pool(payload: bytes, jobs: int, warmup: int, node: str,
                          pool_size: int, concurrency: int) -> tuple[list[float], list[float], float]:
    procs = [await spawn_runtime(node, "server") for _ in range(pool_size)]
    try:
        free: asyncio.Queue = asyncio.Queue()
        for p in procs:
            free.put_nowait(p)

        for _ in range(warmup):  # warm children sequentially
            child = await free.get()
            validate(await call(child, payload), payload)
            free.put_nowait(child)

        sem = asyncio.Semaphore(concurrency)
        wall: list[float] = []
        wait: list[float] = []

        async def job(i: int) -> None:
            async with sem:  # at most `concurrency` jobs in flight
                t0 = time.perf_counter()          # dispatch
                child = await free.get()          # in-pool queue wait
                t_acq = time.perf_counter()
                resp = await call(child, payload)
                t1 = time.perf_counter()
                free.put_nowait(child)
                if i == 0:
                    validate(resp, payload)
                wall.append((t1 - t0) * 1000.0)
                wait.append((t_acq - t0) * 1000.0)

        t_start = time.perf_counter()
        await asyncio.gather(*(job(i) for i in range(jobs)))
        total = time.perf_counter() - t_start
    finally:
        for p in procs:
            await stop(p)
    return wall, wait, total


async def bench_cancel(node: str, trials: int) -> dict:
    # --- SIGKILL the whole process group, measure signal -> reaped ---
    reap_ms: list[float] = []
    for _ in range(trials):
        proc = await spawn_runtime(node, "server", new_session=True)
        await asyncio.sleep(0.05)  # let node reach its stdin read loop
        pgid = os.getpgid(proc.pid)
        t0 = time.perf_counter()
        os.killpg(pgid, signal.SIGKILL)
        await proc.wait()
        reap_ms.append((time.perf_counter() - t0) * 1000.0)

    # --- SIGTERM grace: does node exit while blocked on stdin read? ---
    sigterm: list[dict] = []
    for _ in range(trials):
        proc = await spawn_runtime(node, "server", new_session=True)
        await asyncio.sleep(0.05)
        t0 = time.perf_counter()
        proc.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
            sigterm.append({
                "exited_on_sigterm": True,
                "sigterm_to_exit_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                "escalated_to_sigkill": False,
                "returncode": proc.returncode,
            })
        except asyncio.TimeoutError:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            await proc.wait()
            sigterm.append({
                "exited_on_sigterm": False,
                "sigterm_to_exit_ms": None,
                "escalated_to_sigkill": True,
                "returncode": proc.returncode,
            })
    return {"sigkill_group_reap_ms": reap_ms, "sigterm_trials": sigterm}


# -------------------------------------------------------------------- main --

def md_table(rows: list[tuple], header: list[str]) -> str:
    out = ["| " + " | ".join(header) + " |",
           "|" + "|".join(["---"] * len(header)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)


async def run(args: argparse.Namespace) -> dict:
    rng = random.Random(0xC0FFEE)
    node = shutil.which(args.node) or args.node
    sizes = [s.strip() for s in args.sizes.split(",") if s.strip()] or list(SIZES)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()] or \
        ["inproc", "cold", "warm1", "warm4", "warmt1"]

    payloads = {name: make_payload(SIZES[name], rng) for name in sizes}
    for name, p in payloads.items():
        print(f"# payload {name}: {len(p)} bytes", file=sys.stderr)

    results: dict = {
        "meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node": subprocess.run([node, "--version"], capture_output=True, text=True).stdout.strip(),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpus": multiprocessing.cpu_count(),
            "jobs": args.jobs,
            "warmup": args.warmup,
            "pool_size": 4,
            "warm4_concurrency": 20,
            "payload_bytes": {name: len(p) for name, p in payloads.items()},
        },
        "modes": {},
        "cancel": None,
    }

    for mode in modes:
        results["modes"][mode] = {}
        for size in sizes:
            payload = payloads[size]
            print(f"# running {mode} / {size} ...", file=sys.stderr)
            entry: dict = {}
            if mode == "inproc":
                lat, total = await bench_inproc(payload, args.jobs, args.warmup)
            elif mode == "cold":
                lat, total = await bench_cold(payload, args.jobs, args.warmup, node)
            elif mode == "warm1":
                lat, total = await bench_warm_seq(payload, args.jobs, args.warmup, node, "server")
            elif mode == "warmt1":
                lat, total = await bench_warm_seq(payload, args.jobs, args.warmup, node, "threads")
            elif mode == "warm4":
                lat, wait, total = await bench_warm_pool(
                    payload, args.jobs, args.warmup, node, pool_size=4, concurrency=20)
                w = summarize(wait)
                entry["in_pool_wait_ms"] = w
                entry["in_pool_wait_samples_ms"] = [round(x, 3) for x in sorted(wait)]
            else:
                raise SystemExit(f"unknown mode: {mode}")
            entry["latency_ms"] = summarize(lat)
            entry["jobs_per_sec"] = round(args.jobs / total, 1) if total > 0 else float("inf")
            entry["latency_samples_ms"] = [round(x, 3) for x in sorted(lat)]
            results["modes"][mode][size] = entry

    if not args.skip_cancel:
        print("# running cancellation trials ...", file=sys.stderr)
        results["cancel"] = await bench_cancel(node, args.trials)

    return results


def render_markdown(results: dict) -> str:
    lines = ["### Latency by mode and payload size", ""]
    rows = []
    for mode, by_size in results["modes"].items():
        for size, entry in by_size.items():
            s = entry["latency_ms"]
            rows.append((mode, size, s["p50_ms"], s["p95_ms"], s["p99_ms"], entry["jobs_per_sec"]))
    lines.append(md_table(rows, ["mode", "size", "p50 (ms)", "p95 (ms)", "p99 (ms)", "jobs/sec"]))

    if "warm4" in results["modes"]:
        lines += ["", "### warm4 in-pool wait (dispatch until a child is free)", ""]
        rows = []
        for size, entry in results["modes"]["warm4"].items():
            w = entry["in_pool_wait_ms"]
            rows.append((size, w["p50_ms"], w["p95_ms"], w["p99_ms"], w["max_ms"]))
        lines.append(md_table(rows, ["size", "wait p50 (ms)", "wait p95 (ms)", "wait p99 (ms)", "wait max (ms)"]))

    c = results.get("cancel")
    if c:
        k = c["sigkill_group_reap_ms"]
        lines += ["", "### Cancellation", ""]
        rows = [("SIGKILL process group -> reaped", len(k), round(sum(k) / len(k), 3),
                 round(min(k), 3), round(max(k), 3))]
        ex = [t for t in c["sigterm_trials"] if t["exited_on_sigterm"]]
        escal = [t for t in c["sigterm_trials"] if not t["exited_on_sigterm"]]
        if ex:
            ts = [t["sigterm_to_exit_ms"] for t in ex]
            rows.append(("SIGTERM (blocked on stdin read) -> exit", len(ex),
                         round(sum(ts) / len(ts), 3), round(min(ts), 3), round(max(ts), 3)))
        lines.append(md_table(rows, ["signal", "trials", "mean (ms)", "min (ms)", "max (ms)"]))
        lines += ["", f"SIGTERM escalated to SIGKILL in {len(escal)}/{c['sigterm_trials']} trials."
                  if escal else f"SIGTERM sufficed in all {len(ex)} trials (node exits on default "
                                f"SIGTERM disposition even while blocked on stdin).", ""]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jobs", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--sizes", type=str, default=",".join(SIZES))
    ap.add_argument("--modes", type=str, default=",".join(["inproc", "cold", "warm1", "warm4", "warmt1"]))
    ap.add_argument("--trials", type=int, default=10, help="cancellation trials")
    ap.add_argument("--node", type=str, default="node")
    ap.add_argument("--skip-cancel", action="store_true")
    ap.add_argument("--out", type=Path, default=HERE / "results.json")
    args = ap.parse_args()

    results = asyncio.run(run(args))
    args.out.write_text(json.dumps(results, indent=1))
    print(render_markdown(results))
    print(f"\n# raw results written to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
