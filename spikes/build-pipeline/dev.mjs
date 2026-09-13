/**
 * Dev-loop spike: `taskq dev` stand-in.
 *
 * Spawns `tsx watch runtime.ts` (TASKQ_LINGER=1 so the warm runtime stays up,
 * like the real NDJSON stdio runtime would), then proves the edit->restart->
 * new-manifest loop and measures:
 *   - watcher boot latency (spawn -> first __TASKQ_READY__)
 *   - reload latency (actors.ts write -> next __TASKQ_READY__), N trials
 *
 * Manifest pickup is proven for real: each trial appends a temporary
 * `devProbe<N>` actor to actors.ts and asserts the restarted runtime's
 * manifest frame contains it. actors.ts is restored afterwards.
 */
import { spawn } from "node:child_process";
import { readFileSync, writeFileSync, appendFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

const TRIALS = 3;
const ACTORS_PATH = new URL("./actors.ts", import.meta.url);
const PROBE_SNIPPET = (n) => `

// >>> dev-probe ${n} (auto-removed by dev.mjs)
actors.push(
  defineActor({
    name: "dev-probe-${n}",
    queue: "dev",
    schema: { payload: z.object({ n: z.int() }), result: z.object({ n: z.int() }) },
    handler: async ({ n }) => ({ n }),
  }),
);
`;

const original = readFileSync(ACTORS_PATH, "utf8");
let restored = false;
const restore = () => {
  if (restored) return;
  restored = true;
  writeFileSync(ACTORS_PATH, original);
};
process.on("exit", restore);
process.on("SIGINT", () => process.exit(130));

// Watcher is parameterizable so the spike can compare `tsx watch` against
// Node's built-in `node --watch` (zero tooling). Default: tsx.
const WATCH_CMD = process.env.WATCH_CMD ?? "npx";
const WATCH_ARGS = (process.env.WATCH_ARGS ?? "tsx watch runtime.ts").split(" ");

const child = spawn(WATCH_CMD, WATCH_ARGS, {
  cwd: new URL(".", import.meta.url),
  env: { ...process.env, TASKQ_LINGER: "1" },
  stdio: ["ignore", "pipe", "pipe"],
});

let stdoutBuf = "";
const manifestFrames = [];
let readyResolve = null;
let readyAt = 0;

child.stdout.on("data", (chunk) => {
  stdoutBuf += chunk;
  let idx;
  while ((idx = stdoutBuf.indexOf("\n")) !== -1) {
    const line = stdoutBuf.slice(0, idx);
    stdoutBuf = stdoutBuf.slice(idx + 1);
    if (line.startsWith("__TASKQ_MANIFEST__ ")) {
      manifestFrames.push(JSON.parse(line.slice("__TASKQ_MANIFEST__ ".length)));
    } else if (line.startsWith("__TASKQ_READY__")) {
      readyAt = performance.now();
      readyResolve?.();
      readyResolve = null;
    }
  }
});
child.stderr.on("data", (c) => process.stderr.write(`[tsx] ${c}`));

const p = (ms) => `${ms.toFixed(0)}ms`;

try {
  // -- Phase 1: watcher boot ------------------------------------------------
  const t0 = performance.now();
  const bootPromise = new Promise((r) => (readyResolve = r));
  await Promise.race([bootPromise, sleep(30_000).then(() => { throw new Error("boot timeout"); })]);
  const bootMs = readyAt - t0;
  console.log(`watcher boot (spawn -> ready):        ${p(bootMs)}`);

  // -- Phase 2: edit -> restart -> new manifest -----------------------------
  const reloads = [];
  for (let n = 1; n <= TRIALS; n++) {
    appendFileSync(ACTORS_PATH, PROBE_SNIPPET(n));
    const writeAt = performance.now();
    const before = manifestFrames.length;
    await Promise.race([
      new Promise((r) => (readyResolve = r)),
      sleep(30_000).then(() => { throw new Error(`reload ${n} timeout`); }),
    ]);
    const ms = readyAt - writeAt;
    const manifest = manifestFrames[manifestFrames.length - 1];
    const pickedUp = manifest.actors.some((a) => a.name === `dev-probe-${n}`);
    if (!pickedUp) throw new Error(`reload ${n}: manifest did not pick up dev-probe-${n}`);
    reloads.push(ms);
    console.log(`reload ${n} (write -> ready, new manifest): ${p(ms)}  [dev-probe-${n} in manifest: ${pickedUp}]`);
  }

  const sorted = [...reloads].sort((a, b) => a - b);
  console.log(`\nreload latency (n=${TRIALS}): min ${p(sorted[0])}  median ${p(sorted[Math.floor(TRIALS / 2)])}  max ${p(sorted[sorted.length - 1])}`);
  console.log(`actor count in final manifest: ${manifestFrames[manifestFrames.length - 1].actors.length} (2 original + ${TRIALS} probes seen across restarts)`);
} finally {
  child.kill("SIGTERM");
  await sleep(200);
  child.kill("SIGKILL");
  restore();
}
