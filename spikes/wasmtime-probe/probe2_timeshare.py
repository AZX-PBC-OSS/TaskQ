"""Probe 2: does a long-running WASM actor starve the host asyncio loop?

Run in three isolated modes (one process per mode, chosen via argv):
  baseline — no wasm thread; pure asyncio watchdog.
  noepoch  — background thread spins a non-yielding wasm loop forever,
             epoch interruption DISABLED (uncancellable daemon thread).
  epoch    — background thread runs the same loop sliced by epoch deadlines
             (ticker advances the engine epoch every 10ms; worker catches the
             interrupt trap, resets the deadline, and resumes).

Main thread: asyncio watchdog task sleeps 10ms per beat and records
(actual_wakeup - scheduled_wakeup) lag. 2s window, then report p50/p95/max.

Note: wasmtime-py enters wasm via ctypes CDLL, which releases the GIL during
wasm execution — this probe measures whether that alone keeps the loop
responsive, and what epoch slicing adds (cancellable time slices).
"""

import asyncio
import statistics
import sys
import threading
import time
from pathlib import Path

import wasmtime

HERE = Path(__file__).parent
WAT = (HERE / "wat" / "spin.wat").read_text()
WINDOW_S = 2.0
BEAT_S = 0.010
TICK_MS = 10


async def watchdog(window_s: float) -> list[float]:
    lags: list[float] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + window_s
    while loop.time() < deadline:
        next_beat = loop.time() + BEAT_S
        await asyncio.sleep(BEAT_S)
        lags.append(max(0.0, loop.time() - next_beat) * 1000)
    return lags


def pct(sorted_ms: list[float], q: float) -> float:
    return sorted_ms[min(len(sorted_ms) - 1, int(q * len(sorted_ms)))]


def run(mode: str) -> None:
    stop = threading.Event()
    stats: dict = {}

    if mode == "baseline":
        pass
    elif mode == "noepoch":
        engine = wasmtime.Engine()  # epoch_interruption defaults to False
        module = wasmtime.Module(engine, WAT)
        store = wasmtime.Store(engine)
        instance = wasmtime.Instance(store, module, [])
        spin = instance.exports(store)["spin"]

        def spin_forever() -> None:  # daemon: cannot be cancelled, ever
            while not stop.is_set():
                spin(store)  # infinite loop inside

        th = threading.Thread(target=spin_forever, daemon=True)
        th.start()
        stats["threads"] = [th]
    elif mode == "epoch":
        cfg = wasmtime.Config()
        cfg.epoch_interruption = True
        engine = wasmtime.Engine(cfg)
        module = wasmtime.Module(engine, WAT)
        store = wasmtime.Store(engine)
        instance = wasmtime.Instance(store, module, [])
        exports = instance.exports(store)
        spin, read = exports["spin"], exports["read"]
        store.set_epoch_deadline(1)
        box = {"slices": 0, "counter": 0, "exited": False}
        threads: list[threading.Thread] = []

        def ticker() -> None:
            while not stop.is_set():
                time.sleep(TICK_MS / 1000)
                engine.increment_epoch()

        def worker() -> None:
            while not stop.is_set():
                try:
                    spin(store)
                except wasmtime.Trap:
                    box["slices"] += 1
                    if stop.is_set():
                        break
                    store.set_epoch_deadline(1)  # budget: 1 tick = ~10ms
                    continue
            # Exit path: grant a huge budget (no ticker increments can
            # consume 1M ticks) and verify the global persisted across
            # all the interrupt/resume cycles.
            store.set_epoch_deadline(1_000_000)
            box["counter"] = read(store)
            box["exited"] = True

        threads.append(threading.Thread(target=ticker, daemon=True))
        threads.append(threading.Thread(target=worker, daemon=True))
        for t in threads:
            t.start()
        stats["threads"] = threads
        stats["box"] = box
    else:
        raise SystemExit(f"unknown mode {mode}")

    lags = asyncio.run(watchdog(WINDOW_S))
    stop.set()
    if mode == "epoch":
        # Final kick: the worker may be waiting on the next ticker tick;
        # advancing the epoch guarantees its current/next slice traps so it
        # can observe `stop` and exit.
        engine.increment_epoch()
    if "threads" in stats:
        time.sleep(TICK_MS / 1000 * 2)
        for t in stats["threads"]:
            t.join(timeout=1.0)
        joined = all(not t.is_alive() for t in stats["threads"])
    else:
        joined = True  # no worker threads in baseline

    ms = sorted(lags)
    print(f"mode={mode} beats={len(ms)}")
    print(f"  loop_lag_ms p50={statistics.median(ms):.3f} p95={pct(ms, 0.95):.3f} "
          f"max={ms[-1]:.3f} mean={statistics.mean(ms):.3f}")
    print(f"  worker_joinable_after_stop={joined}")

    if mode == "epoch":
        box = stats["box"]
        print(f"  epoch_slices={box['slices']} counter_across_slices={box['counter']} "
              f"worker_exited={box['exited']}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "baseline")
