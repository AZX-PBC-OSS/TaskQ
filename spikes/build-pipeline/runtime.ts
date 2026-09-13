/**
 * TS actor runtime — the process the Python worker spawns over stdio (NDJSON).
 *
 * Boot sequence (what a Python worker would see, in order, on the child's stdout):
 *   1. one NDJSON frame:  __TASKQ_MANIFEST__ { ...manifest... }
 *   2. ready marker:      __TASKQ_READY__
 *
 * Discovery = importing the actors module. Everything on the manifest is
 * derived from `defineActor` arguments + `z.toJSONSchema` at definition time.
 *
 * Env knobs used by the spike:
 *   TASKQ_DUMP_MANIFEST=1  write manifest.json (pretty) and exit (codegen input)
 *   TASKQ_LINGER=1         stay alive after ready (dev-loop realism)
 */
import { actors, sendWelcomeEmail } from "./actors.ts";

type ActorManifest = (typeof actors)[number]["manifest"];

interface RuntimeManifest {
  protocol: number;
  runtime: { node: string; pid: number; entrypoint: string };
  actors: ActorManifest[];
}

function buildManifest(): RuntimeManifest {
  const names = new Set<string>();
  for (const actor of actors) {
    if (names.has(actor.manifest.name)) {
      throw new Error(`duplicate actor name: ${actor.manifest.name}`);
    }
    names.add(actor.manifest.name);
  }
  return {
    protocol: 1,
    runtime: {
      node: process.version,
      pid: process.pid,
      entrypoint: process.argv[1] ?? "unknown",
    },
    actors: actors.map((a) => a.manifest),
  };
}

const manifest = buildManifest();
const manifestLine = JSON.stringify(manifest);

process.stdout.write(`__TASKQ_MANIFEST__ ${manifestLine}\n`);
process.stdout.write(`__TASKQ_READY__\n`);

if (process.env.TASKQ_DUMP_MANIFEST === "1") {
  // Smoke: run one handler through the Zod schema to prove the wiring is real.
  const parsed = sendWelcomeEmail.schema.payload.parse({
    userId: "7d3f9c2a-1b4e-4c8d-9a0f-2e5d6b8a1c3e",
    email: "author@example.com",
  });
  const result = await sendWelcomeEmail.handler(parsed, {
    attempt: 1,
    jobId: "00000000-0000-4000-8000-000000000000",
    queue: sendWelcomeEmail.manifest.queue,
  });
  sendWelcomeEmail.schema.result.parse(result);

  const { writeFileSync } = await import("node:fs");
  writeFileSync(
    new URL("./manifest.json", import.meta.url),
    JSON.stringify(manifest, null, 2),
  );
  process.stdout.write(`__TASKQ_SMOKE_OK__ handler returned ${JSON.stringify(result)}\n`);
} else if (process.env.TASKQ_LINGER === "1") {
  // Warm-runtime realism: hold the process open for worker traffic.
  process.stdin.resume();
  process.stdin.on("data", (chunk) => {
    // Real protocol loop goes here; the spike only echoes frames back.
    process.stdout.write(`__TASKQ_ECHO__ ${JSON.stringify(String(chunk).trim())}\n`);
  });
}
