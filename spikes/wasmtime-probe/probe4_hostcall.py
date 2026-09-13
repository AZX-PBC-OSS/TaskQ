"""Probe 4: wasm -> host call round-trip overhead.

A wasm module imports host function `db` (simulating a DB-ish call) and an
exported wrapper calls it. Measure the full wasm->host->wasm round trip
(includes the ctypes->Python callback into wasmtime-py) over 2,000 calls,
plus baselines to decompose the cost:
  - direct Python call of the same host logic
  - python -> wasm export call with no host import (entry/exit overhead)
  - wasm -> wasm internal call loop (pure wasm dispatch, for contrast)
"""

import statistics
import time
from pathlib import Path

import wasmtime

HERE = Path(__file__).parent
CALLS = 2_000

HOSTCALL_WAT = (HERE / "wat" / "hostcall.wat").read_text()
# internal wasm->wasm: exported func calls an internal func 1000x per outer call
INTERNAL_WAT = """(module
  (func $inner (param i64) (result i64) (i64.add (local.get 0) (i64.const 1)))
  (func (export "outer") (param i64) (result i64)
    (local $i i64) (local $acc i64)
    (loop $l
      (local.set $acc (call $inner (local.get $acc)))
      (local.set $i (i64.add (local.get $i) (i64.const 1)))
      (br_if $l (i64.lt_u (local.get $i) (i64.const 1000))))
    (local.get $acc)))"""


def host_db(x: int) -> int:
    return x + 1


def p50(values_ms: list[float]) -> float:
    return statistics.median(sorted(values_ms))


def main() -> None:
    engine = wasmtime.Engine()
    linker = wasmtime.Linker(engine)
    linker.define_func("host", "db",
                       wasmtime.FuncType([wasmtime.ValType.i64()],
                                         [wasmtime.ValType.i64()]),
                       host_db)

    store = wasmtime.Store(engine)
    instance = linker.instantiate(store, wasmtime.Module(engine, HOSTCALL_WAT))
    call_db = instance.exports(store)["call_db"]

    # warmup
    for i in range(200):
        assert call_db(store, i) == i + 1

    # wasm -> host -> wasm round trip
    rt_ms = []
    for i in range(CALLS):
        t0 = time.perf_counter()
        r = call_db(store, i)
        rt_ms.append((time.perf_counter() - t0) * 1000)
        assert r == i + 1

    # baseline: python -> wasm export with no host import
    plain_mod = wasmtime.Module(engine, """(module
      (func (export "id") (param i64) (result i64) (local.get 0)))""")
    inst2 = wasmtime.Instance(store, plain_mod, [])
    ident = inst2.exports(store)["id"]
    for i in range(200):
        assert ident(store, i) == i
    py_wasm_ms = []
    for i in range(CALLS):
        t0 = time.perf_counter()
        ident(store, i)
        py_wasm_ms.append((time.perf_counter() - t0) * 1000)

    # baseline: direct python call of same logic
    py_ms = []
    for i in range(CALLS):
        t0 = time.perf_counter()
        host_db(i)
        py_ms.append((time.perf_counter() - t0) * 1000)

    # wasm->wasm internal calls: 1000 calls per outer invocation
    inst3 = wasmtime.Instance(store, wasmtime.Module(engine, INTERNAL_WAT), [])
    outer = inst3.exports(store)["outer"]
    assert outer(store, 0) == 1000
    t0 = time.perf_counter()
    outer(store, 0)
    wasm_wasm_ms = (time.perf_counter() - t0) * 1000 / 1000

    print(f"n={CALLS} calls each")
    print(f"wasm->host->wasm RTT    p50={p50(rt_ms):.4f}ms "
          f"p95={sorted(rt_ms)[int(0.95 * CALLS)]:.4f}ms "
          f"mean={statistics.mean(rt_ms):.4f}ms")
    print(f"python->wasm entry RTT  p50={p50(py_wasm_ms):.4f}ms")
    print(f"python->python call     p50={p50(py_ms):.5f}ms")
    print(f"wasm->wasm internal call p50={wasm_wasm_ms:.6f}ms (1M-iter free)")
    print(f"reference: warm subprocess bridge RTT ~= 0.065ms")


if __name__ == "__main__":
    main()
