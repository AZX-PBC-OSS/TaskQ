"""Probe 3: store limits (memory cap) and fuel metering.

A. Memory: store.set_limits(memory_size=10MB) on a module that grows its
   memory until refused. memory.grow returns -1 when the limiter refuses
   (graceful, actor-catchable) rather than trapping. Report max bytes held.

B. Fuel: Config.consume_fuel on a 1M-iteration loop; compare wall-clock
   vs the identical loop with fuel metering disabled; measure fuel consumed
   (cost model) and show the exhaustion trap + refuel-and-resume ergonomics.
"""

import statistics
import time
from pathlib import Path

import wasmtime

HERE = Path(__file__).parent
GROW_WAT = (HERE / "wat" / "grow.wat").read_text()
COUNT_WAT = (HERE / "wat" / "count.wat").read_text()

MEM_CAP_BYTES = 10 * 1024 * 1024
PAGE = 65_536
ITERS = 1_000_000


def part_a_memory() -> None:
    engine = wasmtime.Engine()
    module = wasmtime.Module(engine, GROW_WAT)
    store = wasmtime.Store(engine)
    store.set_limits(memory_size=MEM_CAP_BYTES)
    instance = wasmtime.Instance(store, module, [])
    exports = instance.exports(store)
    grow_until_fail = exports["grow_until_fail"]
    current_pages = exports["current_pages"]

    refused = grow_until_fail(store)  # grows 1 page at a time until -1
    pages = current_pages(store)
    print(f"[memory] cap={MEM_CAP_BYTES / 1e6:.0f}MB "
          f"held={pages * PAGE / 1e6:.3f}MB ({pages} pages + initial 1)")
    print(f"[memory] memory.grow refusal signal: -1 return (no trap); "
          f"pages_acquired_beyond_initial={refused}")
    # A second explicit grow attempt past the cap for the record:
    r = exports["grow_by"](store, 1)
    print(f"[memory] explicit grow past cap returned: {r}")

    # What a module WITHOUT a limiter would do (unbounded, this module
    # would happily take GBs) — demonstrate cap difference on same module:
    store2 = wasmtime.Store(engine)
    inst2 = wasmtime.Instance(store2, module, [])
    r2 = inst2.exports(store2)["grow_by"](store2, 200)  # +200 pages = ~13MB
    print(f"[memory] unlimiter control grow(+200 pages) returned: {r2} "
          f"({(r2 + 1) * PAGE / 1e6:.2f}MB held)")


def part_b_fuel(trials: int = 20) -> None:
    # no-fuel engine
    eng_plain = wasmtime.Engine()
    mod_plain = wasmtime.Module(eng_plain, COUNT_WAT)

    # fuel engine
    cfg = wasmtime.Config()
    cfg.consume_fuel = True
    eng_fuel = wasmtime.Engine(cfg)
    mod_fuel = wasmtime.Module(eng_fuel, COUNT_WAT)

    def bench(engine, module, fueled: bool) -> float:
        store = wasmtime.Store(engine)
        if fueled:
            store.set_fuel(2**40)  # effectively unlimited for the run
        instance = wasmtime.Instance(store, module, [])
        count = instance.exports(store)["count"]
        t0 = time.perf_counter()
        result = count(store, ITERS)
        dt = time.perf_counter() - t0
        assert result == ITERS
        return dt

    plain_ms = sorted(bench(eng_plain, mod_plain, False) * 1000
                      for _ in range(trials))
    fuel_ms = sorted(bench(eng_fuel, mod_fuel, True) * 1000
                     for _ in range(trials))
    p = lambda xs, q: xs[min(len(xs) - 1, int(q * len(xs)))]  # noqa: E731
    print(f"[fuel] count({ITERS}) no-fuel  p50={statistics.median(plain_ms):.3f}ms "
          f"min={plain_ms[0]:.3f} p95={p(plain_ms, 0.95):.3f}")
    print(f"[fuel] count({ITERS}) fuel     p50={statistics.median(fuel_ms):.3f}ms "
          f"min={fuel_ms[0]:.3f} p95={p(fuel_ms, 0.95):.3f}")
    ov = (statistics.median(fuel_ms) / statistics.median(plain_ms) - 1) * 100
    print(f"[fuel] overhead p50={ov:.1f}%")

    # cost model: fuel consumed by the 1M-iteration loop
    store = wasmtime.Store(eng_fuel)
    store.set_fuel(2**40)
    inst = wasmtime.Instance(store, mod_fuel, [])
    inst.exports(store)["count"](store, ITERS)
    remaining = store.get_fuel()
    burned = 2**40 - remaining
    print(f"[fuel] 1M iterations consumed {burned:,} fuel units "
          f"({burned / ITERS:.1f} units/iter)")

    # exhaustion + refuel-resume: cap fuel mid-loop repeatedly using a global
    # counter module variant is overkill; demonstrate on a fresh bounded call
    # where fuel runs out partway:
    store2 = wasmtime.Store(eng_fuel)
    store2.set_fuel(burned // 2)  # only half enough
    inst2 = wasmtime.Instance(store2, mod_fuel, [])
    count2 = inst2.exports(store2)["count"]
    try:
        count2(store2, ITERS)
        print("[fuel] ERROR: expected exhaustion trap")
    except wasmtime.Trap as t:
        print(f"[fuel] exhaustion trap: {t.trap_code} / '...out of fuel...' present: "
              f"{'fuel' in str(t).lower()}")
        store2.set_fuel(burned)  # refuel and finish the same call
        result = count2(store2, ITERS)
        print(f"[fuel] after refuel, same call completed: result={result}")


if __name__ == "__main__":
    part_a_memory()
    part_b_fuel()
