"""Probe 1: epoch-interruption cancellation of a non-yielding WASM busy loop.

Hand-written WAT module spins forever on a background thread. Main thread
calls engine.increment_epoch() after 100ms. We measure the wall-clock time
from the increment to the trap surfacing in the worker thread (10 trials).
"""

import statistics
import threading
import time
from pathlib import Path

import wasmtime

HERE = Path(__file__).parent
WAT = (HERE / "wat" / "spin.wat").read_text()

DELAY_S = 0.100
TRIALS = 10


def main() -> None:
    cfg = wasmtime.Config()
    cfg.epoch_interruption = True
    engine = wasmtime.Engine(cfg)
    module = wasmtime.Module(engine, WAT)

    results: list[float] = []
    trap_desc: str = ""
    counter_after: int = 0

    for trial in range(TRIALS):
        store = wasmtime.Store(engine)
        store.set_epoch_deadline(1)  # trap once epoch advances past current
        instance = wasmtime.Instance(store, module, [])
        spin = instance.exports(store)["spin"]
        read = instance.exports(store)["read"]

        t_exc = None
        err = None

        def worker() -> None:
            nonlocal t_exc, err
            try:
                spin(store)
                t_exc = time.perf_counter()  # should never happen
            except BaseException as e:  # noqa: BLE001
                t_exc = time.perf_counter()
                err = e

        t0 = time.perf_counter()
        th = threading.Thread(target=worker, daemon=True)
        th.start()
        time.sleep(DELAY_S)
        t_inc = time.perf_counter()
        engine.increment_epoch()
        th.join(timeout=5.0)
        assert t_exc is not None, "worker never surfaced the trap"
        latency = t_exc - t_inc
        results.append(latency)
        # Any wasm entry while past-deadline re-traps; push the deadline
        # past the current epoch so we can inspect the store again.
        store.set_epoch_deadline(1)
        counter_after = read(store)
        if trial == 0:
            trap_desc = f"{type(err).__name__}: {err}"

    ms = sorted(r * 1000 for r in results)
    print(f"delay_before_increment_ms={DELAY_S * 1000:.0f}")
    print(f"trap={trap_desc}")
    print(f"counter_when_trapped={counter_after}")
    print(f"cancel_latency_ms: " + ", ".join(f"{v:.3f}" for v in ms))
    print(f"cancel_latency_p50={statistics.median(ms):.3f}ms")
    print(f"cancel_latency_p95={(ms[8] + (ms[9] if len(ms) > 9 else ms[8])) / 2:.3f}ms")
    print(f"cancel_latency_min={ms[0]:.3f}ms max={ms[-1]:.3f}ms")


if __name__ == "__main__":
    main()
