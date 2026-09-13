// Single NDJSON actor implementation, run identically under node / bun / deno.
// Protocol: one JSON object per line on stdin, one JSON object per line on stdout.
//   in:  {"id": <n>, "op": "ping"|"sleep"|"mem", "ms"?: <int>}
//   out: {"id": <n>, "ok": true}
// On boot it emits a handshake line: {"op": "ready"}
import { createInterface } from "node:readline";
import { writeSync } from "node:fs";

type Req = { id: number; op: string; ms?: number };

const emit = (obj: unknown): void => {
  writeSync(1, JSON.stringify(obj) + "\n");
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function handle(req: Req): Promise<void> {
  if (req.op === "sleep") {
    await sleep(req.ms ?? 0);
  }
  emit({ id: req.id, ok: true });
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line: string) => {
  if (line.length === 0) return;
  let req: Req;
  try {
    req = JSON.parse(line) as Req;
  } catch {
    return;
  }
  if (req.op === "mem") {
    emit({ id: req.id, ok: true, rss: process.memoryUsage().rss });
    return;
  }
  void handle(req);
});

emit({ op: "ready" });
