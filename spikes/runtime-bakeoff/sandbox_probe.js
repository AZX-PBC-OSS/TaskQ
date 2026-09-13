// Sandbox capability probe: attempts fs read, fs write, net fetch (loopback),
// and child spawn, then reports what was allowed/denied as one JSON line.
// Args: [port of driver-run loopback http server] [fs target path]
// (argv, not env: Deno gates env access behind --allow-env)
// stdio working at all is itself part of the result (probe emits this line).
import { readFileSync, writeFileSync } from "node:fs";

const results = { stdio: "ok" };
const [port, file] = process.argv.slice(2);
const fsTarget = file || "/tmp/taskq-sandbox-probe.txt";

try {
  readFileSync("/etc/hosts", "utf8");
  results.fsRead = "ok";
} catch (e) {
  results.fsRead = "denied: " + (e.code || e.name || String(e)).slice(0, 60);
}

try {
  writeFileSync(fsTarget, "probe");
  results.fsWrite = "ok";
} catch (e) {
  results.fsWrite = "denied: " + (e.code || e.name || String(e)).slice(0, 60);
}

if (port) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/`, {
      signal: AbortSignal.timeout(3000),
    });
    await res.text();
    results.net = "ok:" + res.status;
  } catch (e) {
    results.net =
      "denied: " + (e.message || e.code || e.name || String(e)).slice(0, 60);
  }
}

try {
  const { execFileSync } = await import("node:child_process");
  execFileSync("true");
  results.childProcess = "ok";
} catch (e) {
  results.childProcess =
    "denied: " + (e.message || e.code || String(e)).slice(0, 60);
}

process.stdout.write(JSON.stringify(results) + "\n");
