/**
 * Cold-start benchmark: spawn -> runtime prints __TASKQ_READY__ (the first
 * RTT a Python worker would see before it can offer a job over NDJSON stdio).
 *
 * Variants: plain node on .ts (native type stripping), esbuild bundle,
 * bun-compiled single executable, bun-native .ts, and tsx (no watch).
 * 100 trials each; reports min/p50/p95/max. Sequential spawns, warm FS cache.
 */
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const TRIALS = Number(process.env.TRIALS ?? 100);
const URL_HERE = new URL(".", import.meta.url);

const VARIANTS = [
  { label: "node runtime.ts (native TS strip)", cmd: "node", args: ["runtime.ts"] },
  { label: "node dist/runtime.bundle.mjs (esbuild)", cmd: "node", args: ["dist/runtime.bundle.mjs"] },
  { label: "./dist/runtime-bun (bun --compile)", cmd: "./dist/runtime-bun", args: [] },
  { label: "bun runtime.ts (bun native TS)", cmd: "bun", args: ["runtime.ts"] },
  { label: "npx tsx runtime.ts (tsx, no watch)", cmd: "npx", args: ["tsx", "runtime.ts"] },
];

function coldStartOnce(cmd, args) {
  return new Promise((resolve, reject) => {
    const t0 = performance.now();
    const child = spawn(cmd, args, {
      cwd: URL_HERE,
      env: { ...process.env, TASKQ_LINGER: "" }, // exit after ready: clean benching
      stdio: ["ignore", "pipe", "ignore"],
    });
    let settled = false;
    const done = (ms) => {
      if (settled) return;
      settled = true;
      child.removeAllListeners();
      child.stdout.destroy();
      resolve(ms);
    };
    child.stdout.on("data", (chunk) => {
      if (chunk.includes("__TASKQ_READY__")) done(performance.now() - t0);
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (!settled) reject(new Error(`${cmd} exited ${code} before ready`));
    });
    child.on("close", () => done(performance.now() - t0));
  });
}

function pct(sorted, p) {
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}

const results = [];
for (const v of VARIANTS) {
  const times = [];
  for (let i = 0; i < TRIALS; i++) {
    times.push(await coldStartOnce(v.cmd, v.args));
    await sleep(2);
  }
  times.sort((a, b) => a - b);
  results.push({
    label: v.label,
    min: times[0],
    p50: pct(times, 50),
    p95: pct(times, 95),
    max: times[times.length - 1],
  });
  const f = (ms) => `${ms.toFixed(1)}ms`;
  console.log(`${v.label.padEnd(45)} n=${TRIALS}  min ${f(times[0])}  p50 ${f(pct(times, 50))}  p95 ${f(pct(times, 95))}  max ${f(times[times.length - 1])}`);
}

// Quick winner summary for the README.
const byP50 = [...results].sort((a, b) => a.p50 - b.p50);
console.log(`\nfastest p50: ${byP50[0].label} (${byP50[0].p50.toFixed(1)}ms)`);
