// Plain-JS twin of runtime.ts — identical protocol/logic, zero type annotations.
// Used to measure the pure-JS baseline (warm RTT with no TS machinery at all).
import { createInterface } from "node:readline";
import { writeSync } from "node:fs";

const emit = (obj) => {
  writeSync(1, JSON.stringify(obj) + "\n");
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function handle(req) {
  if (req.op === "sleep") {
    await sleep(req.ms ?? 0);
  }
  emit({ id: req.id, ok: true });
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  if (line.length === 0) return;
  let req;
  try {
    req = JSON.parse(line);
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
