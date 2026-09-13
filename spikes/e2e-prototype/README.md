# Spike: integrated e2e bridge prototype (Python worker ⇄ warm Node runtime)

The vertical slice proving the foreign-actor bridge design works together:
Python owns **all durability** (pull, lock, retry, cancel phases, terminal
writes); a warm Node "actor host" executes TypeScript actors and speaks
**NDJSON over stdio**. Built against taskq's `InMemoryBackend` — no Postgres,
no changes to `src/` or `tests/`.

Everything below was executed for real (`node v25.8.1`, Python 3.13.15,
macOS arm64). Reproduce:

```sh
uv sync                                              # repo venv
cd spikes/e2e-prototype && npm install               # zod + tsx
uv run python spikes/e2e-prototype/run_scenarios.py  # runs all 8 scenarios, writes traces/
```

## Files

| file | role |
|---|---|
| `runtime.js` | warm Node actor host: handshake manifest → request loop → streamed events; advisory cancel; clean exit on stdin close |
| `actors/*.ts` | the 3 foreign actors (Zod schemas, executed via `tsx`) + a deliberately drifted variant |
| `miniworker.py` | asyncio mini-worker: handshake validation, merged registry, dispatch loop, sub-enqueue buffer, cancels, fail-fast |
| `native_actor.py` | a real `@actor`-decorated Python handler (registry entry kind 1) |
| `run_scenarios.py` | scenario driver; one JSON trace per scenario in `traces/` |

## Architecture

```
┌──────────────────────── Python mini-worker (owns ALL durability) ────────────────┐
│                                                                                  │
│  dispatch loop (50ms tick)        cancel-poll loop (50ms)     heartbeat (500ms)  │
│  backend.dispatch_batch(...)  →   poll_cancel_flags(...)      heartbeat_jobs()   │
│   ├─ NativeEntry → call ActorRef  │ {op:"cancel"} advisory ──┐                  │
│   ├─ ForeignEntry → {op:"run"} ───┼──────────────────────────┼──► Node runtime  │
│   └─ not hosted   → mark_snoozed(10s)                          │        (warm)    │
│                                        ◄── {op:"log"} {op:"progress"} ──┐        │
│  registry (two entry kinds)            ◄── {op:"subenqueue"} (streamed) │        │
│  ┌ NativeEntry: real ActorRef ─────────────────────────────────────────┐│        │
│  ├ ForeignEntry: manifest entry ⊕ pre-registered schema row            ││        │
│  │   payload → codegen'd Pydantic model ; result → TypeAdapter         ││        │
│  └ quarantined: manifest hash ≠ registry hash → refuse dispatch        ││        │
│                                                                        ▼│        │
│  SubJobBuffer (mirror of SubJobEnqueuer)                                │        │
│   buffer streamed requests → flush AFTER mark_succeeded                 │        │
│                            → discard on failure / crash                 │        │
│                                                                        │        │
│  terminal writes: mark_succeeded / mark_failed_or_retry / mark_cancelled         │
│  fail-fast: runtime stdout EOF → all in-flight jobs failed-or-retryable          │
│             (escalated job → WorkerCancelled; others → WorkerCrashed, pending)   │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                   │ NDJSON over stdio (spawn: node --import tsx/esm
                                   ▼  runtime.js ./actors/index.ts)
┌──────────────────────────── runtime.js (no durability) ──────────────────────────┐
│ boot:  {op:"manifest", runtime_version, actors:[{name, queue,                    │
│          payload_schema: z.toJSONSchema(...), result_schema,                     │
│          retry:{max_attempts}, rate_limits:[...]}]}                              │
│ serve: {op:"run", job_id, attempt, payload} → handler(payload, ctx)              │
│        ctx: cancelled() · log() · progress() · subenqueue(jobs)                  │
│        {op:"cancel", job_id} → per-job advisory flag (polled by the actor)       │
│ end:   {op:"done", job_id, ok:{result}} | {op:"done", job_id, err:{errtype,      │
│          message, backtrace, retryable}} | {op:"done", job_id, cancelled:true}   │
│ stdin close → exit 0                                                             │
└──────────────────────────────────────────────────────────────────────────────────┘
```

Handshake discipline: the manifest's payload/result schemas are canonicalized
(`json.dumps(sort_keys, compact)`) and sha256-hashed against the pre-registered
registry rows (the read model of what the schema-pipeline codegen step would
store). Runtime hosts an unknown actor → startup refuses. Hash mismatch →
loud `schema_drift_detected`, that actor quarantined, dispatch refuses it.

## Scenario results (measured, from `traces/*.json`)

| # | scenario | outcome | key measurements |
|---|---|---|---|
| 1 | happy path: typed Python enqueue → TS `etl_small` → typed result | ✅ succeeded, attempt 1 | enqueue 0.23 ms; run 204–207 ms (5×40 ms stages); 5 progress events (seq 5, percent 100); result re-validated via `TypeAdapter[EtlSmallResult]`; JSONB result 62 B |
| 2 | fanout success | ✅ exactly 5 sub-jobs on parent success | subenqueue buffered at run start, flushed **after** `mark_succeeded`; all 5 children succeeded with `metadata.parent_job_id` set |
| 3 | fanout crash (kill mid-hold, after requests streamed, before done) | ✅ 0 sub-jobs durable | buffer discarded (5 discarded, 0 enqueued); parent fail-fast requeued: **kill→requeued 2.3 ms** (internal recovery 0.22 ms), status `pending`, `WorkerCrashed`, attempt 1 |
| 4 | cooperative cancel (`spinny`, 10 s run) | ✅ clean `cancelled` | cancel→terminal **187 ms** (poll 50 ms + ≤100 ms actor chunk + RTT); actor observed `ctx.cancelled()`, returned `{observed_cancel: true}` after 8.7 M iterations; `cancel_phase` stays 1 |
| 5 | forced cancel (`spinny --no-cooperate`, 30 s run) | ✅ `failed` / `WorkerCancelled` | advisory ignored (no-cooperate pins the event loop — the cancel line is never even read); **2.16 s cancel→terminal** (2 s grace + ~0.16 s poll/escalation); `write_cancel_escalation` phase 2; **SIGKILL→reaped 2.4 ms** |
| 6 | crash recovery, 3 in-flight jobs | ✅ all 3 requeued | **fail-fast: kill→all-requeued 2.4 ms** (EOF→3 terminal writes; internal 0.17 ms). Fallback (`reclaim_expired_locks` sweep): sweep execution itself 0.05 ms for 3 rows, but sweep-only recovery costs ≤ lock lease (30 s) + sweep poll interval + 5 s re-dispatch delay — fail-fast is ~4 orders of magnitude faster |
| 7 | schema drift (`actors/drifted.ts`: `label` required + `rows` upper bound removed) | ✅ loud, contained | registry sha256 `52fa39616275…` vs manifest `2820e9975017…` → `schema_drift_detected` at startup, actor quarantined; job never executed — snoozed 10 s (released for honest workers) |
| 8 | mixed fleet: native + foreign + not-hosted | ✅ all three handled | `native_wordcount` (real `ActorRef`) succeeded in-process; `etl_small` succeeded on the runtime; `foreign_elsewhere` (registered, hosted elsewhere) snoozed-and-released **exactly +10.0 s**, twice |

## Top frictions & lessons

1. **Sub-enqueue buffering must live Python-side, and the runtime must stream
   requests as they happen.** My first runtime buffered node-side and emitted
   the buffer back-to-back with `done` — which made the crash window
   (scenario 3) literally unhittable and the buffer un-discardable. The
   buffered-bridge model is: `ctx.subenqueue()` streams each request
   immediately; the worker's `SubJobBuffer` (a `SubJobEnqueuer` mirror) holds
   them, flushes after `mark_succeeded`, discards on failure/crash. Scenario 3
   is the proof: 5 requests delivered, 0 durable.
2. **A CPU-bound foreign actor can pin the runtime's event loop, and that
   changes what "cancel" means.** Cooperative cancel only works because
   `spinny` yields between ~100 ms busy chunks; `--no-cooperate` never yields,
   so the `{op:"cancel"}` line sits unread in the pipe — advisory cancel is
   structurally unenforceable for such actors. Forced cancel must be a
   process-group SIGKILL (`start_new_session=True`, kill `node` **and** tsx),
   and it kills **every** job that runtime hosts. A one-big-shared-runtime
   design has fleet-wide cancel blast radius; the real integration wants
   runtime-per-actor-class (or small pools).
3. **No DI story for foreign actors.** Native handlers get a full
   `JobContext` (payload model, `SubJobEnqueuer`, structlog logger, DI-resolved
   deps). The runtime `ctx` is hand-rolled (`cancelled/log/progress/
   subenqueue`) with serialized primitives over the wire. The mini-worker
   also had to fake the native path's context construction with a module-level
   backend/clock wart — the real worker's `WorkerDeps` wiring covers this, but
   any *shared* bridge code will need real DI, not globals.
4. **Rate-limit config mapping is unresolved.** The manifest carries
   serialized rate-limit configs (e.g. `token_bucket {capacity, refill_per_sec}`),
   and the mini-worker just records them. The real registration flow must map
   them to Python `TokenBucket`/`SlidingWindow` objects (or reject unknown
   kinds) and enforce them before dispatch — none of that exists yet.
5. **The schema registry is hand-mirrored.** Expected JSON Schemas were pasted
   from a boot run of the runtime; drift detection then works (scenario 7),
   but that's a test double for the codegen pipeline. The real flow needs the
   pipeline to publish canonical schemas + hashes as the source of truth, and
   the handshake check to be the *consumer*, not the producer.
6. **No runtime respawn, no shutdown drain.** After a kill, fail-fast requeues
   everything and dispatch halts (`run_forever` returns); a real worker needs
   supervised respawn with backoff. Symmetrically, closing stdin on a live
   runtime exits(0) immediately, dropping in-flight runs — fine for teardown
   (the parent requeues), but a drain phase ("finish in-flight, take no new
   work") is needed for graceful shutdown parity with native actors.
7. **Retry backoff was faked.** Retries requeue at `timedelta(0)` (immediate
   pending) instead of the real `RetryPolicy` exponential backoff; the
   fail-fast timings above are therefore best-case requeue, not
   time-to-*re-execute*.

## What the mini-worker had to fake (integration checklist)

- `actor_config` table → `backend.register_actor_config(...)` per actor
- codegen'd Pydantic models / registry table → hand-mirrored `FOREIGN_SCHEMAS`
- one warm runtime hosting all foreign actors (no pool sizing/fairness)
- runtime respawn after fail-fast (dispatch halts instead)
- retry backoff computation (fixed 0 s)
- runtime manifest `retry`/`rate_limits` recorded but not enforced
- `WorkerCancelled` error class chosen by the prototype (the sweep path's
  canonical lock-loss class is `WorkerCrashed`; forced-cancel classification
  needs to be pinned in the real worker's cancel controller)
