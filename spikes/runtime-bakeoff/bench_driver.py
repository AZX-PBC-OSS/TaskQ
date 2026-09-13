#!/usr/bin/env python3
"""Runtime bake-off driver: benchmarks Node / Bun / Deno as persistent NDJSON actors.

Subcommands: warm (RTT + memory), mux (concurrency), startup (cold spawn),
kill (SIGKILL/SIGTERM/EOF), sandbox (capability probes), versions.
Writes JSON results into results/.
"""
from __future__ import annotations

import asyncio
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
RESULTS = BASE / "results"
RESULTS.mkdir(exist_ok=True)

# One actor implementation (runtime.ts / runtime.js), five execution configs.
TS = "runtime.ts"
JS = "runtime.js"
CONFIGS = {
    "node-js": ["node", JS],
    "bun-js": ["bun", JS],
    "deno-js": ["deno", "run", JS],
    "node-ts-native": ["node", TS],  # Node >=23.6 strips types by default
    "node-ts-tsx": ["node", "--import", "tsx", TS],
    "bun-ts": ["bun", TS],
    "deno-ts": ["deno", "run", TS],
}
NPROC = os.cpu_count() or 1
PAYLOAD = "x" * 100
WARMUP = 20
RTT_JOBS = 200
MEM_JOBS = 1000
MUX_CONC = 20
MUX_SLEEP_MS = 50
MUX_WAVES = 10
STARTUP_TRIALS = 200


def percentile(vals: list[float], p: float) -> float:
    vals = sorted(vals)
    if not vals:
        return float("nan")
    k = (len(vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(vals) - 1)
    if f == c:
        return vals[f]
    return vals[f] + (vals[c] - vals[f]) * (k - f)


def summarize(vals_ms: list[float]) -> dict:
    return {
        "n": len(vals_ms),
        "p50_ms": round(percentile(vals_ms, 50), 4),
        "p95_ms": round(percentile(vals_ms, 95), 4),
        "p99_ms": round(percentile(vals_ms, 99), 4),
        "min_ms": round(min(vals_ms), 4),
        "max_ms": round(max(vals_ms), 4),
        "mean_ms": round(statistics.fmean(vals_ms), 4),
    }


def rss_kb(pid: int) -> int | None:
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        return int(out.split()[0]) if out else None
    except Exception:
        return None


class Client:
    """Async NDJSON client: request/response by id, concurrent-safe."""

    def __init__(self, cmd: list[str]):
        self.cmd = cmd
        self.proc: asyncio.subprocess.Process | None = None
        self.pending: dict[int, asyncio.Future] = {}
        self.next_id = 1
        self.ready: asyncio.Future | None = None
        self.reader_task: asyncio.Task | None = None

    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
            cwd=BASE,
        )
        self.ready = asyncio.get_running_loop().create_future()
        self.reader_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self.proc and self.proc.stdout
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("op") == "ready":
                if self.ready and not self.ready.done():
                    self.ready.set_result(True)
                continue
            fut = self.pending.pop(msg.get("id"), None)
            if fut and not fut.done():
                fut.set_result(msg)

    async def wait_ready(self, timeout: float = 30.0) -> None:
        assert self.ready is not None
        await asyncio.wait_for(self.ready, timeout)

    async def request(self, obj: dict) -> dict:
        assert self.proc and self.proc.stdin
        rid = self.next_id
        self.next_id += 1
        obj = {"id": rid, **obj}
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        self.proc.stdin.write((json.dumps(obj) + "\n").encode())
        await self.proc.stdin.drain()
        return await fut

    def kill(self) -> None:
        if self.proc and self.proc.returncode is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass

    async def close(self) -> None:
        self.kill()
        if self.proc:
            await self.proc.wait()
        if self.reader_task:
            self.reader_task.cancel()


# ---------------------------------------------------------------- warm RTT + memory
async def bench_warm(name: str, cmd: list[str]) -> dict:
    client = Client(cmd)
    await client.start()
    await client.wait_ready()

    req = {"op": "ping", "payload": PAYLOAD}
    for _ in range(WARMUP):
        await client.request(req)

    assert client.proc
    pid = client.proc.pid
    idle_rss_kb = rss_kb(pid)

    rtt_ms: list[float] = []
    for _ in range(RTT_JOBS):
        t0 = time.perf_counter_ns()
        await client.request(req)
        rtt_ms.append((time.perf_counter_ns() - t0) / 1e6)

    jobs_sec = RTT_JOBS / (sum(rtt_ms) / 1000.0)

    # memory: keep pushing jobs, sample RSS every 100 -> max
    rss_samples = []
    for i in range(MEM_JOBS):
        await client.request(req)
        if (i + 1) % 100 == 0:
            kb = rss_kb(pid)
            if kb:
                rss_samples.append(kb)
    post_rss_kb = rss_kb(pid)

    await client.close()
    return {
        "config": name,
        "cmd": cmd,
        "rtt": summarize(rtt_ms),
        "jobs_per_sec": round(jobs_sec, 1),
        "idle_rss_kb": idle_rss_kb,
        "post_1000_jobs_rss_kb": post_rss_kb,
        "max_rss_kb_during_1000": max(rss_samples) if rss_samples else None,
    }


# ---------------------------------------------------------------- multiplexed
async def bench_mux_one(name: str, cmd: list[str], n_procs: int,
                        conc: int) -> dict:
    clients = []
    for _ in range(n_procs):
        c = Client(cmd)
        await c.start()
        await c.wait_ready()
        clients.append(c)

    req = {"op": "sleep", "ms": MUX_SLEEP_MS}
    job_ms: list[float] = []

    for _wave in range(MUX_WAVES):
        async def one(c: Client) -> None:
            t0 = time.perf_counter_ns()
            await c.request(req)
            job_ms.append((time.perf_counter_ns() - t0) / 1e6)

        # spread conc requests across the n_procs clients
        jobs = [one(clients[i % n_procs]) for i in range(conc)]
        await asyncio.gather(*jobs)

    for c in clients:
        await c.close()
    return {
        "config": name,
        "cmd": cmd,
        "variant": f"{n_procs} process(es) x {conc // n_procs} concurrent",
        "sleep_ms": MUX_SLEEP_MS,
        "job_wall_time": summarize(job_ms),
    }


# ---------------------------------------------------------------- startup
async def bench_startup(name: str, cmd: list[str]) -> dict:
    t_ready_ms: list[float] = []
    t_first_ms: list[float] = []
    req = {"op": "ping", "payload": PAYLOAD}

    for _ in range(STARTUP_TRIALS):
        t0 = time.perf_counter_ns()
        client = Client(cmd)
        await client.start()
        await client.wait_ready()
        t_ready_ms.append((time.perf_counter_ns() - t0) / 1e6)
        await client.request(req)
        t_first_ms.append((time.perf_counter_ns() - t0) / 1e6)
        await client.close()

    return {
        "config": name,
        "cmd": cmd,
        "trials": STARTUP_TRIALS,
        "spawn_to_ready": summarize(t_ready_ms),
        "spawn_to_first_rtt": summarize(t_first_ms),
    }


# ---------------------------------------------------------------- kill behavior
async def bench_kill(name: str, cmd: list[str]) -> dict:
    res: dict = {"config": name, "cmd": cmd}

    # SIGKILL the process group -> time-to-reap
    reap_ms: list[float] = []
    for _ in range(20):
        client = Client(cmd)
        await client.start()
        await client.wait_ready()
        assert client.proc
        t0 = time.perf_counter_ns()
        client.kill()
        await client.proc.wait()
        reap_ms.append((time.perf_counter_ns() - t0) / 1e6)
        res["sigkill_exit_status"] = client.proc.returncode
        await client.close()
    res["sigkill_time_to_reap"] = summarize(reap_ms)

    # SIGTERM while blocked on stdin (default disposition)
    term: list[dict] = []
    for _ in range(10):
        client = Client(cmd)
        await client.start()
        await client.wait_ready()
        assert client.proc
        try:
            os.killpg(os.getpgid(client.proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(client.proc.wait(), timeout=3.0)
            exited = True
        except asyncio.TimeoutError:
            exited = False
            client.kill()
            await client.proc.wait()
        term.append({"exited": exited, "status": client.proc.returncode})
        await client.close()
    res["sigterm_while_blocked_on_stdin"] = {
        "trials": len(term),
        "all_exited": all(t["exited"] for t in term),
        "statuses": sorted({str(t["status"]) for t in term}),
    }

    # stdin EOF (graceful path: parent closes pipe)
    eof: list[dict] = []
    for _ in range(5):
        client = Client(cmd)
        await client.start()
        await client.wait_ready()
        assert client.proc and client.proc.stdin
        client.proc.stdin.write_eof()
        try:
            await asyncio.wait_for(client.proc.wait(), timeout=3.0)
            exited = True
        except asyncio.TimeoutError:
            exited = False
            client.kill()
            await client.proc.wait()
        eof.append({"exited": exited, "status": client.proc.returncode})
        await client.close()
    res["stdin_eof"] = {
        "trials": len(eof),
        "all_exited": all(t["exited"] for t in eof),
        "statuses": sorted({str(t["status"]) for t in eof}),
    }
    return res


# ---------------------------------------------------------------- sandbox probes
async def bench_sandbox() -> dict:
    import http.server
    import threading

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    probe_file = "/tmp/taskq-sandbox-probe.txt"

    trials = {
        "node --permission": ["node", "--permission", "sandbox_probe.js"],
        "node --permission +fs-grants": [
            "node", "--permission",
            f"--allow-fs-read={BASE}", "--allow-fs-write=/tmp",
            "sandbox_probe.js",
        ],
        "node --permission +net-grant": [
            "node", "--permission", f"--allow-net=127.0.0.1:{port}",
            "sandbox_probe.js",
        ],
        "deno (no flags)": ["deno", "run", "sandbox_probe.js"],
        "deno +net+fs grants": [
            "deno", "run", f"--allow-net=127.0.0.1:{port}",
            "--allow-write=/tmp", "sandbox_probe.js",
        ],
        "bun (no flags)": ["bun", "sandbox_probe.js"],
        "bun --smol": ["bun", "--smol", "sandbox_probe.js"],
    }
    out = {}
    for label, cmd in trials.items():
        cmd = [*cmd, str(port), probe_file]
        try:
            p = await asyncio.create_subprocess_exec(
                *cmd, cwd=BASE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                so, se = await asyncio.wait_for(p.communicate(), timeout=15)
            except asyncio.TimeoutError:
                p.kill()
                out[label] = {"error": "timeout"}
                continue
            line = so.decode().strip().splitlines()
            parsed = json.loads(line[-1]) if line else None
            out[label] = {"report": parsed, "stderr": se.decode().strip()[:200]}
        except Exception as e:  # spawn failure is itself a result
            out[label] = {"error": repr(e)[:200]}
    srv.shutdown()
    return out


# ---------------------------------------------------------------- versions
def bench_versions() -> dict:
    def first_line(cmd: list[str]) -> str:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return (r.stdout or r.stderr).strip().splitlines()[0]

    return {
        "node": first_line(["node", "--version"]),
        "bun": first_line(["bun", "--version"]),
        "deno": first_line(["deno", "--version"]).split()[1],
        "deno_full": first_line(["deno", "--version"]),
        "python": sys.version.split()[0],
        "platform": f"{platform.mac_ver()[0]} {platform.machine()}",
        "cpu_count": NPROC,
    }


def save(fname: str, data: dict) -> None:
    path = RESULTS / fname
    path.write_text(json.dumps(data, indent=2) + "\n")
    print(f"wrote {path}")


async def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "versions"):
        save("00_versions.json", bench_versions())
    if which in ("all", "warm"):
        out = {}
        for name, cmd in CONFIGS.items():
            print(f"[warm] {name}", flush=True)
            out[name] = await bench_warm(name, cmd)
        save("10_warm_rtt_memory.json", out)
    if which in ("all", "mux"):
        out = {}
        for name in ("node-ts-native", "bun-ts", "deno-ts"):
            cmd = CONFIGS[name]
            print(f"[mux 1x{MUX_CONC}] {name}", flush=True)
            out[f"{name} 1x{MUX_CONC}"] = await bench_mux_one(
                name, cmd, 1, MUX_CONC)
            print(f"[mux 4x{MUX_CONC // 4}] {name}", flush=True)
            out[f"{name} 4x{MUX_CONC // 4}"] = await bench_mux_one(
                name, cmd, 4, MUX_CONC)
        save("20_mux.json", out)
    if which in ("all", "startup"):
        out = {}
        for name, cmd in CONFIGS.items():
            print(f"[startup] {name}", flush=True)
            out[name] = await bench_startup(name, cmd)
        save("30_startup.json", out)
    if which in ("all", "kill"):
        out = {}
        for name in ("node-ts-native", "bun-ts", "deno-ts"):
            print(f"[kill] {name}", flush=True)
            out[name] = await bench_kill(name, CONFIGS[name])
        save("40_kill.json", out)
    if which in ("all", "sandbox"):
        print("[sandbox]", flush=True)
        save("50_sandbox.json", await bench_sandbox())


if __name__ == "__main__":
    asyncio.run(main())
