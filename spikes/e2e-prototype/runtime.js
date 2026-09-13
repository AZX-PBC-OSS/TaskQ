// TaskQ foreign-language actor runtime — integrated e2e prototype.
//
// A warm Node "actor host": at boot it imports the actor module (TypeScript,
// executed via tsx), performs a HANDSHAKE with the parent (sends one manifest
// line describing every actor it hosts, schemas extracted from Zod via
// z.toJSONSchema), then serves a request loop over NDJSON on stdio.
//
// Protocol (one JSON object per line):
//   runtime → parent, boot:  {op:"manifest", runtime_version, actors:[...]}
//   parent → runtime:        {op:"run",    job_id, attempt, payload}
//   parent → runtime:        {op:"cancel", job_id}          (advisory)
//   runtime → parent:        {op:"log",       job_id, level, message}
//   runtime → parent:        {op:"progress",  job_id, step, percent}
//   runtime → parent:        {op:"subenqueue", job_id, jobs:[{actor, payload}]}
//   runtime → parent:        {op:"done", job_id, ok:{result}}
//                                 | {op:"done", job_id, err:{errtype, message, backtrace, retryable}}
//                                 | {op:"done", job_id, cancelled:true, result}
//
// Sub-enqueue requests are STREAMED to the parent as the actor makes them;
// the parent buffers them and only flushes to durable storage when the run
// finishes successfully — on failure or crash the parent discards the
// buffer. The parent owns all durability.
//
// Exits cleanly (0) when stdin closes. A crash/SIGKILL of this process is
// how the parent detects runtime death (EOF on stdout).

import { createInterface } from "node:readline";
import process from "node:process";
import { pathToFileURL } from "node:url";
import { z } from "zod";

const RUNTIME_VERSION = `node-${process.versions.node}`;
const ACTORS_MODULE = process.argv[2] ?? "./actors/index.ts";

function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

// ── Actor registration ────────────────────────────────────────────────

const registry = new Map(); // name -> {name, queue, payload, result, retry, rate_limits, handler}

export function defineActor(def) {
  if (registry.has(def.name)) {
    throw new Error(`actor ${def.name} registered twice`);
  }
  registry.set(def.name, def);
}

function manifest() {
  return {
    op: "manifest",
    runtime_version: RUNTIME_VERSION,
    actors: [...registry.values()].map((a) => ({
      name: a.name,
      queue: a.queue,
      payload_schema: z.toJSONSchema(a.payload),
      result_schema: z.toJSONSchema(a.result),
      retry: { max_attempts: a.retry?.max_attempts ?? 3 },
      rate_limits: a.rate_limits ?? [],
    })),
  };
}

// ── Per-run state ─────────────────────────────────────────────────────

const runs = new Map(); // job_id -> {cancelled: boolean}

// ── Run execution ─────────────────────────────────────────────────────

async function execute(actor, jobId, attempt, payload) {
  const state = { cancelled: false };
  runs.set(jobId, state);

  const ctx = {
    jobId,
    attempt,
    cancelled: () => state.cancelled,
    log: (message, fields = {}) =>
      emit({ op: "log", job_id: jobId, level: "info", message, ...fields }),
    progress: (step, percent) =>
      emit({ op: "progress", job_id: jobId, step, percent }),
    subenqueue: (jobs) => {
      if (!Array.isArray(jobs)) throw new Error("ctx.subenqueue expects an array");
      // Stream each request to the parent immediately — the PARENT buffers
      // it (flush on parent success, discard on failure). Node keeps no
      // durability; a request already delivered but unflushed when the
      // runtime dies is discarded by the parent's buffer.
      emit({ op: "subenqueue", job_id: jobId, jobs });
    },
  };

  try {
    // Defense in depth: validate the payload against the Zod schema before
    // running. The Python worker validates first (against the codegen'd
    // Pydantic model); this side re-validates so a registry/schema drift
    // can never execute a handler against garbage.
    const parsed = actor.payload.safeParse(payload);
    if (!parsed.success) {
      emit({
        op: "done",
        job_id: jobId,
        err: {
          errtype: "PayloadValidationError",
          message: JSON.stringify(parsed.error.issues),
          backtrace: null,
          retryable: false,
        },
      });
      return;
    }

    const result = await actor.handler(parsed.data, ctx);

    // Cooperative-cancel classification: if a cancel was requested and the
    // handler returned early, the run is "cancelled" — the parent marks the
    // job cancelled rather than succeeded.
    if (state.cancelled) {
      emit({ op: "done", job_id: jobId, cancelled: true, result: result ?? null });
      return;
    }
    emit({ op: "done", job_id: jobId, ok: { result: result ?? null } });
  } catch (err) {
    emit({
      op: "done",
      job_id: jobId,
      err: {
        errtype: err?.name ?? "Error",
        message: String(err?.message ?? err),
        backtrace: err?.stack ?? null,
        retryable: err?.retryable ?? true,
      },
    });
  } finally {
    runs.delete(jobId);
  }
}

// ── Request loop ──────────────────────────────────────────────────────

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

rl.on("line", (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    emit({ op: "protocol_error", message: "unparseable line from parent" });
    return;
  }
  if (msg.op === "run") {
    const actor = registry.get(msg.actor);
    if (!actor) {
      emit({
        op: "done",
        job_id: msg.job_id,
        err: {
          errtype: "ActorNotHosted",
          message: `runtime does not host actor ${msg.actor}`,
          backtrace: null,
          retryable: false,
        },
      });
      return;
    }
    execute(actor, msg.job_id, msg.attempt, msg.payload);
  } else if (msg.op === "cancel") {
    const state = runs.get(msg.job_id);
    if (state) state.cancelled = true; // advisory only
  } else {
    emit({ op: "protocol_error", message: `unknown op ${msg.op}` });
  }
});

rl.on("close", () => {
  process.exit(0);
});

// ── Boot: import actors, then handshake ───────────────────────────────

const mod = await import(pathToFileURL(ACTORS_MODULE).href);
if (typeof mod.registerAll === "function") mod.registerAll(defineActor);

emit(manifest());
