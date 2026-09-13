#!/usr/bin/env node
"use strict";
// Actor runtime for the TaskQ bridge microbenchmark.
//
// Modes (argv[2]):
//   oneshot  read one JSON request from stdin (to EOF), write one JSON line to
//            stdout, exit. Simulates a cold-started actor process.
//   server   persistent NDJSON loop: one newline-delimited JSON request per
//            line of stdin, one NDJSON response per request on stdout.
//   threads  like `server`, but each request is handled in a freshly spawned
//            worker_threads Worker (optional variant).
//
// Response: {ok: true, sum: <derived from payload to force a full parse>,
//            echo: <the payload object back>}.

const fs = require("fs");

// A large writeSync(1, ...) can return short on a non-blocking pipe (this
// happens once a worker_threads Worker has been spawned), so loop until the
// whole buffer is out, parking ~1ms on EAGAIN to let the reader drain.
const parkBuf = new Int32Array(new SharedArrayBuffer(4));
function emit(str) {
  const buf = Buffer.from(str, "utf8");
  let off = 0;
  while (off < buf.length) {
    try {
      off += fs.writeSync(1, buf, off, buf.length - off);
    } catch (e) {
      if (e.code === "EAGAIN") {
        Atomics.wait(parkBuf, 0, 0, 1);
        continue;
      }
      throw e;
    }
  }
  fs.writeSync(1, "\n");
}

function handle(payload) {
  // n + len(data) forces JSON.parse to materialize the whole payload.
  const sum = payload.n + payload.data.length;
  return { ok: true, sum: sum, echo: payload };
}

const emitLine = (obj) => emit(JSON.stringify(obj));

const mode = process.argv[2] || "server";

if (mode === "oneshot") {
  const raw = fs.readFileSync(0, "utf8");
  emitLine(handle(JSON.parse(raw)));
  process.exit(0);
} else if (mode === "server" || mode === "threads") {
  const useThreads = mode === "threads";
  const workerSrc = [
    'const { parentPort } = require("worker_threads");',
    'parentPort.on("message", (payload) => {',
    "  const sum = payload.n + payload.data.length;",
    '  parentPort.postMessage({ ok: true, sum: sum, echo: payload });',
    "});",
  ].join("\n");

  let useWorkers;
  if (useThreads) {
    const { Worker } = require("worker_threads");
    useWorkers = function spawnWorker(req) {
      const w = new Worker(workerSrc, { eval: true });
      w.on("message", (res) => {
        w.terminate();
        emitLine(res);
      });
      w.postMessage(req);
    };
  }

  let buf = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    buf += chunk;
    let i;
    while ((i = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, i);
      buf = buf.slice(i + 1);
      if (line.length === 0) continue;
      const req = JSON.parse(line);
      if (useThreads) {
        useWorkers(req);
      } else {
        emitLine(handle(req));
      }
    }
  });
  process.stdin.resume();
} else {
  console.error("unknown mode: " + mode + " (use oneshot|server|threads)");
  process.exit(2);
}
