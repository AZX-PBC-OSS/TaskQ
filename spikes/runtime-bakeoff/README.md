# Spike: Runtime Bake-off — Node vs Bun vs Deno as TaskQ's TS actor runtime

TaskQ will host TypeScript actors in a **persistent warm runtime process** speaking
NDJSON over stdio, spawned and supervised by the Python worker. This spike measures
the three candidate runtimes executing the **same actor implementation**
(`runtime.ts` / its plain-JS twin `runtime.js`) under identical protocol, driven by a
Python asyncio supervisor (`bench_driver.py`) — the same shape as the real worker.

Environment (2026-09-12, macOS 26.6.2 arm64, 16 cores, Python 3.14.7):

| Runtime | Version |
|---|---|
| Node   | v25.8.1 |
| Bun    | 1.3.3   |
| Deno   | 2.9.6 (installed via `npm install -g deno`) |

Configs benchmarked (one actor file each, differences minimal):

| Config | Command | What it isolates |
|---|---|---|
| `node-js`        | `node runtime.js` | Node, zero TS machinery |
| `bun-js`         | `bun runtime.js`  | Bun, zero TS machinery |
| `deno-js`        | `deno run runtime.js` | Deno, zero TS machinery |
| `node-ts-native` | `node runtime.ts` | Node 25 native type stripping (no flags) |
| `node-ts-tsx`    | `node --import tsx runtime.ts` | Node + tsx loader |
| `bun-ts`         | `bun runtime.ts`  | Bun native TS |
| `deno-ts`        | `deno run runtime.ts` | Deno native TS |

## Results matrix

### 1. Warm RTT — persistent process, sequential NDJSON req/resp (100 B payload), 200 jobs after 20 warmup

| Config | p50 (ms) | p95 (ms) | p99 (ms) | jobs/sec |
|---|---|---|---|---|
| node-js        | 0.072 | 0.201 | 0.286 | 11,281 |
| bun-js         | 0.073 | 0.135 | 0.328 | 11,634 |
| deno-js        | 0.089 | 0.163 | 0.306 |  9,843 |
| **node-ts-native** | 0.072 | 0.122 | 0.240 | **12,364** |
| node-ts-tsx    | 0.077 | 0.127 | 0.199 | 12,018 |
| **bun-ts**     | 0.073 | 0.131 | 0.301 | 11,846 |
| deno-ts        | 0.079 | 0.144 | 0.308 | 11,048 |

**Tie.** All three hold ~0.07–0.09 ms p50 at 10–12k jobs/sec; the spread is within
run-to-run noise (an earlier noisier run showed the same ordering differences
shuffled). Warm RTT is not a differentiator.

### 2. Multiplexed concurrency — async I/O-bound jobs (50 ms sleep), wall time per job

| Config | Variant | p50 (ms) | p95 (ms) | overhead vs 50 ms |
|---|---|---|---|---|
| node-ts-native | 1 process × 20 conc | 50.8 | 51.4 | +0.8 ms |
| node-ts-native | 4 processes × 5 conc | 50.7 | 51.5 | +0.8 ms |
| bun-ts         | 1 process × 20 conc | 51.2 | 53.4 | +1.2 ms |
| bun-ts         | 4 processes × 5 conc | 51.3 | 52.6 | +1.3 ms |
| deno-ts        | 1 process × 20 conc | 51.8 | 52.9 | +1.8 ms |
| deno-ts        | 4 processes × 5 conc | 51.8 | 53.2 | +1.8 ms |

All three multiplex cleanly: 20 in-flight sleeps over **one** process complete in
~50–53 ms (p95 ≤ 53.4 ms) — no serialization, no head-of-line blocking. 4 × 5
processes performs the same, so a single runtime process per actor group suffices.

### 3. Startup to first response — cold spawn → handshake → first RTT, 200 trials

| Config | spawn→ready p50/p95 (ms) | spawn→first RTT p50/p95 (ms) | TS cost vs own JS |
|---|---|---|---|
| node-js        | 31.2 / 35.5 | 31.6 / 36.0 | — |
| bun-js         | 24.2 / 26.8 | 26.1 / 28.8 | — |
| deno-js        | 26.8 / 30.4 | 27.4 / 31.2 | — |
| node-ts-native | 56.9 / 62.3 | 57.5 / 63.3 | **+26 ms** |
| node-ts-tsx    | 56.4 / 63.3 | 57.3 / 64.6 | +25 ms |
| bun-ts         | **23.7 / 27.6** | **25.7 / 29.5** | ~0 |
| deno-ts        | 27.3 / 30.7 | 27.9 / 31.9 | +0.5 ms |

The reputation is real: **Bun and Deno cold-start with TS 2.1–2.4× faster than
Node** (25.7 ms / 27.9 ms vs 57.5 ms p50). Node's native type stripping costs
~26 ms; notably `--import tsx` costs **nothing extra over native** on Node 25
(56.4 vs 56.9 ms) — tsx is only needed pre-23.6-style, not for startup time.

### 4. Memory (RSS via `ps`, persistent process)

| Config | Idle (after warmup) | Max during 1000 jobs |
|---|---|---|
| node-js        | 50.7 MB | 55.8 MB |
| bun-js         | 35.0 MB | 59.8 MB |
| deno-js        | 33.2 MB | 48.1 MB |
| node-ts-native | 74.5 MB | **78.4 MB** |
| node-ts-tsx    | 61.3 MB | 64.7 MB |
| bun-ts         | 34.2 MB | 59.8 MB |
| deno-ts        | **33.3 MB** | **48.9 MB** |

Deno is leanest and most stable (33 → 49 MB). Bun is lean at idle (34 MB) but grows
+25 MB under load (JIT warmup). Node with TS idles at 2.2× Bun/Deno (75 MB native,
61 MB via tsx). For a warm pool of N actor processes this is the largest practical
gap between Node and the others.

### 5. Kill behavior (cancel ladder)

| Probe | node-ts-native | bun-ts | deno-ts |
|---|---|---|---|
| SIGKILL process group → time-to-reap p50 (20 trials) | 1.9 ms | **1.2 ms** | 1.5 ms |
| SIGKILL exit status | -9 (killed) | -9 | -9 |
| SIGTERM while blocked on stdin (10 trials) | exits, status -15, none hang | exits, -15, none hang | exits, -15, none hang |
| stdin EOF (parent closes pipe, 5 trials) | clean exit 0 | clean exit 0 | clean exit 0 |

All three: default SIGTERM disposition terminates immediately (no hang, no cleanup
needed at the Python side beyond a wait), SIGKILL reaps in ≤2 ms, and stdin EOF
produces a clean exit 0 — so the escalate-SIGTERM-then-SIGKILL cancel ladder works
uniformly, and EOF is a viable graceful shutdown signal.

### 6. TypeScript execution story

| Runtime | Native TS? | Measured cost |
|---|---|---|
| Bun  | Yes, zero-config | **Free**: warm RTT identical to JS; startup identical to JS (23.7 vs 24.2 ms) |
| Deno | Yes, zero-config | ~Free: +0.5 ms startup vs JS; warm RTT within noise |
| Node | Yes by default in 25 (type stripping) | **+26 ms startup** vs node-js; warm RTT free; tsx adds nothing more |

Node + tsx is no longer the path — Node 25 runs `.ts` natively and tsx measured
identical (startup and RTT) to native stripping. The real TS cost on Node is the
strip/compile step itself at every cold spawn (~26 ms), which Bun and Deno avoid
at spawn and only pay lazily.

### 7. Cancellation-visible APIs (qualitative)

All three expose the same cooperative-cancel primitives: global
`AbortController`/`AbortSignal` (web standard), plus `AbortSignal.timeout()` and
`AbortSignal.any()`. For an I/O-bound actor the cooperative story is parity.
Process-level cancellation is covered in §5 and is also parity. No runtime offers
a meaningful advantage here; TaskQ's cancel ladder can be runtime-agnostic.

### 8. Sandboxing flags (capability model — probed empirically, `sandbox_probe.js`)

| Runtime | Deny by default? | Probed behavior |
|---|---|---|
| **Deno** | **Yes — full capability model** | No flags: fs read/write `NotCapable`, net `NotCapable`, child spawn denied (needs `--allow-env` for PATH lookup). stdio always allowed. Grants are granular: `--allow-net=127.0.0.1:PORT`, `--allow-write=/tmp` restored exactly those. |
| Node `--permission` | Yes — experimental but working | fs read/write `ERR_ACCESS_DENIED`, **net denied** (`--allow-net` exists, emits experimental warning), child process denied. Selective grants work (`--allow-fs-write=/tmp` → fsWrite ok, other ops still denied). **stdio unaffected** in all cases. |
| Bun | No | fs, net, childProcess all allowed; no permission model. `--smol` is GC/memory tuning only, not security. |

Deno is the only runtime where an actor is sandboxed by default; Node 25's
`--permission` is a credible opt-in (net portion experimental); Bun requires
OS-level confinement.

## Verdict

**Recommendation: Bun as the default runtime for TaskQ's TS actor bridge.**

Weights, in the order that mattered:

1. **Warm RTT** — tie across all three (~0.07 ms p50, 11–12k jobs/s); the bridge is
   I/O-dominated, so this metric only rules runtimes *out* below ~10× worse. None.
2. **TS ergonomics** — Bun and Deno run TS with zero config at zero cold-start cost;
   Node pays ~26 ms per cold spawn (native stripping; tsx no longer helps). For a
   supervisor that respawns crashed actors into a warm pool, Bun's 24 ms vs Node's
   57 ms spawn-to-first-response is the single biggest win (2.4×).
3. **Kill behavior** — parity, and it's good everywhere: SIGTERM exits via default
   disposition (nothing hangs), SIGKILL reaps in ~1.2–1.9 ms, stdin EOF exits 0.
   Bun is nominally fastest to reap; not decisive.
4. **Memory** — Bun idles at 34 MB vs Node's 61–75 MB; Deno 33 MB and flattest
   under load (49 vs Bun's 60 MB after 1000 jobs). Per-100-actor-pool, Bun/Deno
   save ~3–4 GB vs Node.
5. **Sandbox capability** — the one axis Bun loses outright: no permission model.
   Deno wins it; Node 25 `--permission` is workable; Bun needs OS confinement.

**Runner-up: Deno** — statistically tied with Bun on RTT, leanest memory, best
capability model, only ~2 ms slower startup; its npm-wrapper install and marginally
slower warm path are minor. **Pick Deno instead of Bun if actors must be
sandboxed in-process** (deny-by-default with granular grants, stdio unaffected).

**Node** remains a safe fallback (identical warm RTT, best-trodden runtime,
`--permission` exists) but costs 2.4× startup and ~2× idle memory for the bridge —
worth it only if adding a second vendored runtime is unacceptable.

Run everything: `./run_all.sh` — raw data in `results/*.json`.
