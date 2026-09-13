# Spike: WASM actors via wasmtime-py (epoch interruption)

Feasibility probe for a secondary actor tier in TaskQ: **in-process WASM
actors executed by wasmtime-py, cancellable mid-busy-loop via epoch
interruption** — the pitch being sandboxed actors with no subprocess and no
bridge protocol. Everything below is measured on real execution.

- Machine: macOS arm64 (Apple Silicon), Python 3.13.15 (spike venv), `wasmtime==48.0.0`
- wasmtime docs corroborating semantics: [Config::epoch_interruption (docs.rs, wasmtime 48.0.2)](https://docs.rs/wasmtime/latest/wasmtime/struct.Config.html#method.epoch_interruption)
- Reproduce: `uv venv .venv-spike && uv pip install -r requirements.txt && .venv-spike/bin/python probeN_*.py`

## 1. Busy-loop cancellation (epochs)

Hand-written WAT infinite loop (below) runs on a background thread; main
thread calls `engine.increment_epoch()` after 100 ms; measured time from
increment to trap surfacing in the worker thread. 10 trials, fresh `Store`
per trial.

```wat
;; wat/spin.wat
(module
  (global $g (mut i64) (i64.const 0))
  (func (export "spin")
    (loop $l
      (global.set $g (i64.add (global.get $g) (i64.const 1)))
      (br $l)))
  (func (export "read") (result i64) (global.get $g)))
```

| metric (probe1_cancel.py) | value |
|---|---|
| interrupts a **non-yielding loop**? | **yes** — loop is genuinely preempted from outside (counter showed 136M–194M iterations at trap time across runs) |
| surfaced error | `wasmtime.Trap` — `wasm trap: interrupt` (`TrapCode.INTERRUPT`) |
| cancel latency p50 (10 trials) | **0.043–0.053 ms** across runs |
| cancel latency p95 | 0.148–0.217 ms |
| min / max observed | 0.038 ms / 0.247 ms |

Sample run: `0.038, 0.038, 0.041, 0.042, 0.043, 0.043, 0.053, 0.056, 0.067, 0.229 ms`

Resumption after the trap works: reset the store deadline
(`store.set_epoch_deadline(1)` — relative to current epoch) and re-enter any
export; guest state (globals) persists. Gotcha found while probing: the
deadline check compares against the *current* epoch, so any wasm entry while
past-deadline re-traps immediately — reset the deadline before touching the
store again.

wasmtime's own docs (Config::epoch_interruption): the deadline check "cannot
be avoided by WebAssembly code. It is safe to use epoch deadlines to limit
the execution time of untrusted code." One documented hole: bulk-memory ops
(`memory.copy`) check the epoch only once at operation start, so max
unpreemptible slice ≈ max(epoch interval, one full-memory copy) — bound
memory via a limiter to bound this.

## 2. Time-sharing vs the host asyncio loop

Watchdog task sleeps 10 ms per beat in the main thread's asyncio loop and
records wakeup lag, while a background thread runs the same infinite WASM
loop. Three isolated process runs (probe2_timeshare.py), 2 s window:

| mode | loop lag p50 | p95 | max | worker joinable after stop? |
|---|---|---|---|---|
| baseline (no wasm) | 0.908 ms | 1.093 | 1.698 | n/a |
| busy wasm loop, **no interruption** | 0.873 ms | 1.085 | 2.726 | **no** (uncancellable daemon leak) |
| busy wasm loop, **epoch budget 10 ms slices** | 0.665 ms | 1.081 | 1.749 | **yes** |

Epoch-slicing details: a ticker thread advances the engine epoch every 10 ms;
the worker catches the interrupt trap, resets the deadline, and resumes.
179 slices in 2 s (~10.9 ms/slice), and the guest's global counter read
**4,517,348,402** after 179 interrupt/resume cycles — state fully preserved
across preemptions; catch-and-resume is a viable cooperative scheduler.

**Honest finding that contradicts the pitch's premise:** the asyncio loop is
*not* starved even without epoch interruption. wasmtime-py enters wasm via
ctypes `CDLL` (wasmtime/_ffi.py:38), which **releases the GIL** during wasm
execution — the host loop keeps running (sub-ms lag) while the actor burns a
core. The real difference epochs make is bounded *control*: without them the
runaway actor thread can never be stopped or reclaimed (join times out
forever); with them it is cancellable in <0.05 ms and sliceable into a
CPU budget. Epochs would be *load-bearing* only if actors ran on the loop
thread itself (single-threaded host), which we do not recommend anyway.

## 3. Limits: memory cap and fuel

**Memory limiter** (`Store.set_limits(memory_size=10MB)`, wat/grow.wat):
`memory.grow` past the cap returns `-1` (spec's graceful refusal —
actor-catchable, no trap). The capped module held 160 pages = 10.486 MB and
could not grow further; an unlimited control grew freely. So: hard,
per-actor memory caps work out of the box.

**Fuel metering** (`Config.consume_fuel`) on a 1M-iteration bounded loop
(wat/count.wat), 20 trials each:

| config | p50 wall time |
|---|---|
| no fuel | 0.273 ms |
| fuel enabled (plenty granted) | 0.528 ms |
| **overhead** | **~93–96% (≈2× slower)** |

1M iterations consumed 8,000,002 fuel units (≈8 units/iter — usable cost
model). Exhaustion surfaces as `TrapCode.OUT_OF_FUEL`; refueling
(`store.set_fuel`) and re-calling completes the work.

Ergonomics comparison (mirrors wasmtime's docs, which report fuel can be
2–3× slower): **epochs** are near-zero-overhead, wall-clock-based,
non-deterministic, ideal for cancellation/timeouts of untrusted code;
**fuel** is deterministic (same input → same trap point), good for
billing/quota fairness, but costs ~2× execution and requires pre-allocating
a budget. For TaskQ's cancellation need, epochs win; fuel is a niche
add-on for accounting.

## 4. Host-call round trip

wat/hostcall.wat — wasm export calls an imported host function
(`host.db: i64 -> i64`) and returns the result; measured over 2,000 calls
(probe4_hostcall.py):

| path | p50 |
|---|---|
| **wasm → host → wasm (round trip)** | **0.0165 ms** (p95 0.019 ms) |
| python → wasm entry/exit (no host import) | 0.0106 ms |
| python → python call (same logic) | 0.00004 ms |
| wasm → wasm internal call | ~0.00002 ms |
| **reference: warm subprocess bridge RTT** | **~0.065 ms** |

In-process WASM host-call RTT is **~4× lower** than the warm subprocess
bridge (saves ~48 µs/call). The bridge cost is dominated by IPC framing;
the WASM cost is dominated by the ctypes marshal at the python→wasm
boundary. Note the improvement is only meaningful for *chatty* actors
(hundreds of host calls per task); for an actor doing 1–2 host calls per
job, 48 µs is noise next to queue bookkeeping.

## 5. Maturity: TypeScript → WASM components (2025–2026)

- **jco** (Bytecode Alliance) is alive and current: v1.25.2 on npm,
  released within weeks of this writing; scaffolds/builds **TypeScript
  components from WIT worlds** (`jco scaffold` from `builtin:wasi-command`
  / `wasi-reactor` / `wasi-proxy`, WASI 0.2/0.3). Source: [jco GitHub README](https://github.com/bytecodealliance/jco), [npm @bytecodealliance/jco](https://www.npmjs.com/package/@bytecodealliance/jco).
- The build path is **not** AOT compilation of JS to wasm: it embeds
  **StarlingMonkey (SpiderMonkey) inside the component**, ~8 MB embedding
  per component — "Because JavaScript cannot be compiled ahead-of-time to
  raw WebAssembly instructions, ComponentizeJS embeds a JavaScript engine
  inside each component." Sources: [ComponentizeJS](https://github.com/bytecodealliance/ComponentizeJS), [wasmCloud TS guide](https://wasmcloud.com/docs/wash/developer-guide/language-support/typescript/).
- Real-framework evidence: the Nov 12, 2025 wasmCloud community call demoed
  **Hono (web framework) running as a component via jco-stl** — npm-dep
  apps can build today, though the jco-stl "standard library" is young
  ("first stable release"). Source: [wasmCloud community meeting](https://wasmcloud.com/community/2025-11-12-community-meeting/).

**Verdict:** TS actors *can* compile to components today, but each ships a
full SpiderMonkey — startup and memory footprint land back in
subprocess-bridge territory (heavy warm runtime), not the lightweight
in-process-compute niche. The niche is real for actors written in
Rust/Go/C/C++/TinyGo compiled to core wasm. Also unresolved for JS
components: a JS `Atomics.wait`-style blocking call inside SpiderMonkey is
the same class of uninterruptible blocking Extism hit; a busy JS *loop*
should still be epoch-preemptible (it is a wasm loop), but that needs its
own probe before trusting with untrusted TS.

## Verdict for TaskQ

**Technically viable — as a deliberately scoped tier, not a bridge
replacement.** All four mechanism claims verified: (1) epoch cancellation of
non-yielding loops is real, fast (p50 <0.05 ms), and safe against malicious
guests; (2) 10 ms epoch time-slicing works with full state preservation;
(3) per-actor 10 MB memory caps enforce gracefully (`memory.grow` → `-1`);
(4) host-call RTT is 0.0165 ms vs 0.065 ms subprocess — a real 4× win, but
worth ~48 µs/call.

The complexity premium buys the most for actors that are:
- **computationally intensive** (the tier exists for these; I/O-bound actors
  gain nothing — they're mostly parked in host calls either way),
- **short-lived / bounded** (sub-second; epoch deadline as a hard timeout),
- **untrusted or third-party** (shared-nothing sandbox + memory cap +
  interrupt that can't be dodged),
- and written in a **compiled language** (Rust/Go/C) — not TS.

Keep the **warm Node subprocess bridge as the primary path** for general
actors (full Node/npm ecosystem, blocking-friendly, ~0.065 ms RTT is fine
for I/O actors). The WASM tier earns its place as a *plugin/compute* tier:
pure functions over small inputs, hard CPU/memory caps, untrusted authors.

Two operational notes if pursued: pin the host GIL story (ctypes releases
the GIL during wasm exec, so wasm actors run genuinely concurrently with
the asyncio loop — but keep them off the loop thread); and remember epochs
never interrupt a *blocking host call* — host calls need their own timeouts
(same caveat wasmtime documents, same class of issue that sank Extism's
cancellation story).

## Files

- `probe1_cancel.py` — epoch cancellation latency (10 trials)
- `probe2_timeshare.py` — asyncio watchdog lag, baseline/noepoch/epoch modes
- `probe3_limits.py` — memory limiter + fuel overhead/exhaustion
- `probe4_hostcall.py` — wasm↔host round-trip benchmarks
- `wat/spin.wat`, `wat/count.wat`, `wat/grow.wat`, `wat/hostcall.wat` — WAT sources
- `requirements.txt` — `wasmtime==48.0.0`
