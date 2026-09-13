# TS Build/Dev Pipeline Spike

Answers: *how does TS actor code get from the author's editor to a warm Node
runtime the Python worker spawns (NDJSON over stdio)* — in dev (watch/reload),
in prod (bundled artifact), and how actor discovery + schema artifacts flow
(Zod schema in the actor definition is the source of truth for Python codegen).

Everything here was executed for real (macOS arm64, Node v25.8.1, bun 1.3.3,
zod 4.6.4, esbuild 0.25.12, tsx 4.23.13, TS 5.9.3). Reproduce with:

```bash
cd spikes/build-pipeline && npm install
node runtime.ts                          # boot: manifest NDJSON frame + ready marker
TASKQ_DUMP_MANIFEST=1 node runtime.ts    # + smoke handler run, writes manifest.json
npm run typecheck                        # tsc --noEmit over the authoring shape
node dev.mjs                             # tsx watch edit->restart->manifest loop
WATCH_CMD=node WATCH_ARGS="--watch runtime.ts" node dev.mjs   # zero-tooling variant
node bundle.mjs                          # esbuild bundle + bun --compile, timed
node bench.mjs                           # cold-start: 100 trials x 5 variants, p50/p95
uv run python codegen.py                 # manifest -> JSON Schema -> pydantic models
uv run python settings_sketch.py         # TOML config validation sketch
```

## 1. Authoring shape

`actors.ts` is what an author writes. One helper, fully typed end to end:

```ts
export const sendWelcomeEmail = defineActor({
  name: "send-welcome-email",
  queue: "emails",
  retry: { maxAttempts: 5, delay: 2 },
  maxConcurrent: 4,
  schema: {
    payload: z.object({ userId: z.uuid(), email: z.email(), locale: z.enum(["en", "de", "fr"]).default("en") }),
    result:  z.object({ messageId: z.string(), acceptedAt: z.string() }),
  },
  async handler(payload, ctx) {           // payload/result fully inferred from the Zod schemas
    return { messageId: `msg_${payload.userId.slice(0, 8)}`, acceptedAt: new Date().toISOString() };
  },
});

// Discovery: the runtime imports this array.
export const actors = [sendWelcomeEmail, resizeImage];
```

`defineActor` (~50 lines in `actors.ts`) freezes the manifest at definition
time: `z.toJSONSchema(schema, { io: "input" | "output" })` per direction (input
keeps defaults optional for consumers, output marks them required on results),
queue/retry/maxConcurrent metadata, and a `duplicate actor name` guard at
boot. Handler typing flows from the Zod schemas — no separate TS interface.

## 2. Dev loop

`dev.mjs` spawns a watcher around `runtime.ts` (`TASKQ_LINGER=1` so the child
stays warm like the real stdio runtime), then appends a temporary
`dev-probe-N` actor to `actors.ts` per trial and asserts the restarted
runtime's manifest frame contains it — restart *and* manifest pickup are
proven, not assumed. `actors.ts` is restored afterwards.

| watcher | boot (spawn → ready) | reload (write → ready, n=3) | tooling |
|---|---|---|---|
| `node --watch runtime.ts` | **121ms** | min 308 / median 308 / max 314ms | **zero** |
| `npx tsx watch runtime.ts` | 486–533ms | min 222 / median 302 / max 320ms | tsx dep |

### Node-native TS findings (v25.8.1)

- **`node runtime.ts` just works — zero flags, zero stderr noise.** Type
  stripping is default-on (the `--experimental-strip-types` flag is accepted
  as a no-op default; the help text lists a `--no-strip-types` negation).
  With stripping off, `.ts` fails immediately with `ERR_UNKNOWN_FILE_EXTENSION`.
- **Erasable-syntax-only subset**: relative imports must carry explicit `.ts`
  extensions (tsc: `allowImportingTsExtensions` + `moduleResolution: bundler`).
  Enums/namespaces/parameter properties are rejected
  (`ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX`, strip-only mode); the spike's
  `tsconfig.json` sets `erasableSyntaxOnly` so the editor enforces the
  runtime-safe subset at authoring time. `--experimental-transform-types`
  rescues enums if ever needed.
- `package.json` `"type": "module"` governs `.ts` files as ESM, as for `.js`.
- Type stripping costs ~2.2× vs a prebuilt bundle at cold start (§3) — fine
  for dev, wrong for prod.

**Recommended dev flow: `node --watch runtime.ts`.** No bundler, no tsx, no
watch dep — Node 25 runs the actors' TS natively, restarts in ~310ms with the
new manifest, and the edit→typecheck→run loop is identical to prod's runtime
entry file. (`tsx watch` buys nothing measurable here; its reload is the same
within noise and it adds boot overhead + a dependency.)

## 3. Prod artifact

`bundle.mjs` inlines runtime + actors + zod into one ESM file
(`--platform=node` keeps `node:*` external implicitly), then builds the
single-executable alternative. Both artifacts boot-verified with identical
manifests.

| artifact | build time | size | cold start → ready (100 trials) |
|---|---|---|---|
| `node runtime.ts` (native strip, unbundled) | — | — | p50 105.7ms · p95 131.2ms |
| `node dist/runtime.bundle.mjs` (esbuild) | **~30ms** (52ms first run) | **739 KiB** | p50 47.7ms · p95 53.4ms |
| `./dist/runtime-bun` (`bun --compile`) | 117ms | 57.7 MiB | **p50 34.9ms · p95 38.3ms** |
| `bun runtime.ts` (bun native TS) | — | — | p50 38.9ms · p95 48.6ms |
| `npx tsx runtime.ts` (no watch) | — | — | p50 424.7ms · p95 518.9ms |

Method: spawn → first `__TASKQ_READY__` frame on stdout (the first RTT a
Python worker would see), sequential spawns, warm FS cache, min/p50/p95/max
computed over 100 trials each (`bench.mjs`).

**Recommended prod flow: esbuild bundle, run with plain `node`.** 739 KiB
self-contained artifact (no `node_modules` ships), 30ms deterministic build
that slots into any CI, p50 47.7ms cold start — 2.2× faster than on-the-fly
stripping and within 13ms p50 of the bun-compiled executable, which costs
57.7 MiB per artifact and pins runtime updates to bun releases. If cold-start
p50 ~13ms matters more than artifact size (huge fleets, high idle_exit churn),
`bun --compile` is the fallback; never ship `tsx`-executed runtimes (8.9× the
bundle's p50).

## 4. Discovery / manifest / codegen

Discovery = the runtime imports `actors.ts` and maps `actors[].manifest` —
metadata + frozen JSON Schemas from `defineActor`. Boot prints one NDJSON
frame (`__TASKQ_MANIFEST__ {…}`) then `__TASKQ_READY__`; a boot-time duplicate
actor-name check refuses to start rather than silently shadowing.

`TASKQ_DUMP_MANIFEST=1` additionally runs one handler through its Zod schemas
(smoke, verified) and writes `manifest.json`. `codegen.py` then proves
actor-definition-as-source-of-truth end to end:

```
manifest.json (2 actors) → aggregate JSON Schema ($defs: SendWelcomeEmailPayload/
SendWelcomeEmailResult/ResizeImagePayload/ResizeImageResult)
  → uvx --from datamodel-code-generator datamodel-codegen
      --input-file-type jsonschema --output-model-type pydantic_v2.BaseModel
      --strict-nullable            # pinned: spike-schema-pipeline proved
                                   # default codegen widens defaulted fields
                                   # to `X | None` (13/34 → 32/34 with it)
  → models_generated.py: UUID, EmailStr, Enum classes, conint/constr,
    extra='forbid' on results — real pydantic v2, round-trip validated
```

Findings: `z.email()` → `format: email` → pydantic `EmailStr`, which **needs
`email-validator` at import time** in the consuming venv (real dependency
implication of the source-of-truth chain); `z.uuid()` maps to `uuid.UUID`
(coerces from str, verified). Worker-side `models_generated.py` is build
output, never hand-edited — CI should regenerate and diff.

## 5. Config sketch (worker side)

`taskq.worker.example.toml` — the `taskq worker --config` surface, with env
overrides (`TASKQ_` prefix) documented per field:

- `[runtime]`: `command` / `args` / `cwd` (dev: `npx tsx watch` or `node --watch`; prod: the bundle path or bun exe), `protocol = 1` (worker aborts on boot-frame mismatch), `boot_timeout` (spawn→ready budget; fail-closed, default 10s vs measured p95 53ms bundled).
- `[[runtime.pools]]`: `name`, `queues[]`, `max_pool_size` (processes, ≥ each actor's maxConcurrent), `idle_exit_timeout` (reap idle warm processes; `0` = worker lifetime — cheap at 35–50ms cold starts).

`settings_sketch.py` mirrors `WorkerSettings` conventions (bounded `Field`s
with `TASKQ_*` env names, "Why:"-commented validators, `load()` classmethod)
in pydantic v2 over `tomllib`, and executes the invariants: valid example TOML
parses; queue claimed by two pools / blank command / empty queues all
**REJECTED** at load (fail closed). Production would port this onto the real
dotenvmodel `DotEnvConfig` style of `src/taskq/settings.py`.

## What `taskq dev` vs `taskq worker` would own

| | `taskq dev` | `taskq worker` |
|---|---|---|
| runtime spawn | `node --watch <entry.ts>` (zero tooling) | entrypoint from config (bundled `node dist/…` or bun exe) |
| manifest | re-read on every restart; overlay over PG `actor_config` | asserted once at boot (`boot_timeout`), then source of truth for dispatch |
| schema artifacts | regen pydantic/TS clients on manifest change (watch) | never; bundle is built upstream (CI/pre-deploy) |
| pools | single pool, idle_exit generous, no reaping | per-queue pools, `max_pool_size`, `idle_exit_timeout` reaping |
| hot reload | watch child restart (~310ms) + in-flight jobs drained/failed | N/A — restarts are supervisor territory |

## Verdicts

1. **Dev: `node --watch` over the entry `.ts` — no build step, no tsx.**
   Node 25's native type stripping (default-on, warning-free, explicit-`.ts`
   imports, erasable-syntax subset enforced by tsconfig) makes watcher
   tooling redundant for this shape.
2. **Prod: esbuild → single ESM file, run with plain `node`.** 30ms builds,
   739 KiB, p50 47.7ms / p95 53.4ms cold start; bun `--compile` only if
   ~13ms p50 justifies 57.7 MiB and a bun runtime pin.
3. **`z.toJSONSchema(io: "input"/"output")` at defineActor time** makes the
   Zod schema a genuine single source of truth: manifest, envelope validation
   and pydantic codegen all derive from it; pin `--strict-nullable` in codegen.
4. **Boot protocol is cheap and assertable**: manifest frame + ready marker in
   ≤106ms worst-case measured (unbundled), ≤53ms bundled — a 10s
   `boot_timeout` default is generous by three orders of magnitude.
