"""Simulated foreign-runtime actor (NDJSON over stdio).

Faithful stand-in for a TS actor executing in a Node child process: the
child speaks newline-delimited JSON on stdin/stdout and has NO access to
the worker's backend or DB transaction.

Protocol (child receives on stdin):
    {"op": "run", "job_id": "...", "payload": {...}}

Protocol (child emits on stdout):
    {"op": "subenqueue", "seq": i, "actor": "spike_sub_actor", "payload": {...}}
    {"op": "done", "ok": true}
    {"op": "done", "ok": false, "error": "..."}

With --await-ack the child waits for the parent's
    {"op": "ack", "seq": i}
before emitting the next sub-enqueue request, which turns the protocol
into strict request/response and lets us measure round-trip latency.

Behavior scripting:
    --subenqueues N   emit N sub-enqueue requests (default 5)
    --sleep-ms M      sleep M ms between requests (default 20)
    --crash-after K   os._exit(1) after K sub-enqueues (K=0: crash first)
    --fail            after all N sub-enqueues, report ok:false instead

Per-request round-trip timings (await-ack mode) are written to stderr as
a JSON object at completion.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time


def _emit(msg: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", default=None, help="informational only; real job_id arrives on the run message")
    parser.add_argument("--subenqueues", type=int, default=5)
    parser.add_argument("--sleep-ms", type=int, default=20)
    parser.add_argument("--crash-after", type=int, default=None)
    parser.add_argument("--fail", action="store_true")
    parser.add_argument("--await-ack", action="store_true")
    parser.add_argument("--late-enqueue-after-done", action="store_true")
    parser.add_argument("--actor", default="spike_sub_actor")
    args = parser.parse_args()

    rtts_ms: list[float] = []
    acked: set[int] = set()

    def _read_ack(seq: int) -> None:
        line = sys.stdin.readline()
        if not line:
            os._exit(1)
        msg = json.loads(line)
        if msg.get("op") == "ack" and msg.get("seq") == seq:
            acked.add(seq)

    emit_idx = 0
    for i in range(args.subenqueues):
        if args.crash_after is not None and i >= args.crash_after:
            # Hard crash before emitting request i: K requests were sent,
            # then die like a segfault/OOM — no "done" message, ever.
            os._exit(1)
        payload = {"n": i}
        if args.await_ack:
            t0 = time.perf_counter()
        _emit({
            "op": "subenqueue",
            "seq": i,
            "actor": args.actor,
            "payload": payload,
        })
        emit_idx = i + 1
        if args.await_ack:
            _read_ack(i)
            rtts_ms.append((time.perf_counter() - t0) * 1000.0)
        if args.sleep_ms > 0 and i < args.subenqueues - 1:
            time.sleep(args.sleep_ms / 1000.0)

    if args.fail:
        _emit({"op": "done", "ok": False, "error": "ChildFailure: scripted failure"})
    else:
        _emit({"op": "done", "ok": True})
        if args.late_enqueue_after_done:
            # PROTOCOL VIOLATION DEMO: a sub-enqueue sent after success.
            # In fire-and-forget style the write lands in the pipe buffer
            # but the parent has stopped reading; it is silently dropped
            # and reaches no backend. In await-ack style this would
            # deadlock instead (the parent never acks after done).
            time.sleep(0.05)
            _emit({
                "op": "subenqueue",
                "seq": args.subenqueues,
                "actor": args.actor,
                "payload": {"n": args.subenqueues, "late": True},
            })

    if args.await_ack:
        sys.stderr.write(json.dumps({"rtt_ms": rtts_ms}) + "\n")
        sys.stderr.flush()


if __name__ == "__main__":
    main()
