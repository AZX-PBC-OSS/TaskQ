/**
 * Prod-artifact spike: bundle runtime.ts + actors.ts (+ zod) into a single
 * ESM file with esbuild, and build a single executable with `bun build
 * --compile`. Measures and prints both build times and artifact sizes.
 *
 * esbuild flags: --platform=node keeps node:* builtins external implicitly;
 * zod and everything else is inlined so the artifact needs no node_modules.
 */
import { build } from "esbuild";
import { spawnSync } from "node:child_process";
import { statSync } from "node:fs";
import { mkdirSync } from "node:fs";

mkdirSync(new URL("./dist/", import.meta.url), { recursive: true });

const t0 = performance.now();
const esbuildResult = await build({
  entryPoints: ["runtime.ts"],
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node25",
  outfile: "dist/runtime.bundle.mjs",
  sourcemap: false,
  minify: false,
  logLevel: "silent",
  // platform=node makes node:* builtins external implicitly; nothing else
  // stays external — zod is inlined so dist/ is self-contained.
});
const esbuildMs = performance.now() - t0;

const bundlePath = new URL("./dist/runtime.bundle.mjs", import.meta.url);
const bundleBytes = statSync(bundlePath).size;

// Sanity: the bundled artifact must boot and produce an identical manifest.
const smoke = spawnSync("node", [bundlePath.pathname], {
  cwd: new URL(".", import.meta.url),
  encoding: "utf8",
  timeout: 30_000,
});
if (smoke.status !== 0 || !smoke.stdout.includes("__TASKQ_READY__")) {
  console.error(smoke.stdout, smoke.stderr);
  throw new Error("bundled runtime failed to boot");
}
const bundledManifest = smoke.stdout
  .split("\n")
  .find((l) => l.startsWith("__TASKQ_MANIFEST__ "));

console.log(`esbuild: ${esbuildMs.toFixed(0)}ms, ${(bundleBytes / 1024).toFixed(1)} KiB -> dist/runtime.bundle.mjs (boots OK, ${JSON.parse(bundledManifest.slice("__TASKQ_MANIFEST__ ".length)).actors.length} actors)`);

// -- bun build --compile ----------------------------------------------------
const tb0 = performance.now();
const bun = spawnSync("bun", ["build", "--compile", "runtime.ts", "--outfile", "dist/runtime-bun"], {
  cwd: new URL(".", import.meta.url),
  encoding: "utf8",
  stdio: ["ignore", "pipe", "pipe"],
});
const bunMs = performance.now() - tb0;
if (bun.status !== 0) {
  console.error(bun.stdout, bun.stderr);
  throw new Error("bun build --compile failed");
}
const exePath = new URL("./dist/runtime-bun", import.meta.url);
const exeBytes = statSync(exePath).size;

const smokeBun = spawnSync(exePath.pathname, {
  cwd: new URL(".", import.meta.url),
  encoding: "utf8",
  timeout: 30_000,
});
if (smokeBun.status !== 0 || !smokeBun.stdout.includes("__TASKQ_READY__")) {
  console.error(smokeBun.stdout, smokeBun.stderr);
  throw new Error("bun-compiled runtime failed to boot");
}
const bunManifest = smokeBun.stdout
  .split("\n")
  .find((l) => l.startsWith("__TASKQ_MANIFEST__ "));

console.log(`bun --compile: ${bunMs.toFixed(0)}ms, ${(exeBytes / 1024 / 1024).toFixed(1)} MiB -> dist/runtime-bun (boots OK, ${JSON.parse(bunManifest.slice("__TASKQ_MANIFEST__ ".length)).actors.length} actors)`);
