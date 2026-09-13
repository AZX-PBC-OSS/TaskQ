"""Asyncio worker-parent bridge — implements both candidate sub-enqueue semantics.

The parent owns the InMemoryBackend + JobsClient (the "DB"), dispatches a
fake parent job, runs worker_child.py as a subprocess over NDJSON stdio,
and honors the child's sub-enqueue requests per the selected semantics:

  buffered  — every {op:"subenqueue"} request is buffered locally as an
              EnqueueArgs (mirroring SubJobEnqueuer's _pending_buffer);
              nothing touches the backend until the child reports
              {ok:true}, at which point the parent applies the terminal
              write (mark_succeeded) and THEN flushes the buffer, exactly
              like the consumer's flush_buffer-after-commit ordering.
              On child failure/crash the buffer is discarded.

  direct    — every {op:"subenqueue"} request is executed immediately
              against the backend via JobsClient.enqueue (at-least-once,
              no atomicity). The ack reports the enqueue outcome.

Usage:
  uv run python worker_parent.py --semantics buffered --crash-after 3 \
      --trace traces/crash3_buffered.json

Protocol (parent -> child):
  {"op": "run", "job_id": "...", "payload": {...}}
  {"op": "ack", "seq": i, "ok": true|false, "error": "..."}

The parent ALWAYS acks every subenqueue request; awaiting the ack is the
child's choice (--await-ack).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from taskq import actor
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobFilter
from taskq.backend.clock import SystemClock
from taskq.client._args import build_enqueue_args
from taskq.client._jobs import JobsClient
from taskq.testing.in_memory import InMemoryBackend

CHILD = str(Path(__file__).with_name("worker_child.py"))
INTERACTION_TIMEOUT_S = 30.0
LOCK_LEASE = timedelta(minutes=5)


class _ParentPayload(BaseModel):
    n: int = 0


class _SubPayload(BaseModel):
    n: int = 0


@actor(name="spike_parent_actor")
async def _parent_actor(_payload: _ParentPayload) -> None: ...


@actor(name="spike_sub_actor")
async def _sub_actor(_payload: _SubPayload) -> None: ...


_SUB_ACTOR: dict[str, Any] = {"spike_sub_actor": _sub_actor}


def _dump_row(row: Any) -> dict[str, Any]:
    d = asdict(row)
    return {
        "id": str(d["id"]),
        "actor": d["actor"],
        "queue": d["queue"],
        "status": d["status"],
        "payload": d["payload"],
        "metadata": d["metadata"],
        "created_at": str(d["created_at"]),
    }


class ParentBridge:
    def __init__(self, semantics: str) -> None:
        self.semantics = semantics
        self.backend = InMemoryBackend(clock=SystemClock())
        self.client = JobsClient(backend=self.backend, clock=SystemClock())
        self.worker_id = uuid4()
        self.buffered: list[EnqueueArgs] = []
        self.subenqueues_received = 0
        self.subenqueues_flushed = 0
        self.subenqueues_discarded = 0
        self.ack_errors: list[str] = []

    async def dispatch_parent_job(self) -> Any:
        handle = await self.client.enqueue(_parent_actor, _ParentPayload())
        rows = await self.backend.dispatch_batch(
            self.worker_id, [_parent_actor.queue], limit=1, lock_lease=LOCK_LEASE
        )
        if not rows or rows[0].id != handle.job_id:
            raise RuntimeError(f"dispatch did not return parent job {handle.job_id}: {rows}")
        return handle.job_id

    async def handle_subenqueue(self, msg: dict[str, Any]) -> dict[str, object]:
        self.subenqueues_received += 1
        seq = msg["seq"]
        ref = _SUB_ACTOR[msg["actor"]]
        payload = ref.payload_type(**msg["payload"])
        if self.semantics == "buffered":
            args = build_enqueue_args(ref, payload, metadata={"parent_bridge": "buffered"})
            self.buffered.append(args)
            return {"op": "ack", "seq": seq, "ok": True}
        try:
            await self.client.enqueue(ref, payload)
        except Exception as exc:  # at-least-once: report, never undo
            self.ack_errors.append(f"seq={seq}: {exc}")
            return {"op": "ack", "seq": seq, "ok": False, "error": str(exc)}
        return {"op": "ack", "seq": seq, "ok": True}

    async def aflush_buffered(self) -> list[str]:
        errors: list[str] = []
        pending, self.buffered = self.buffered, []
        for args in pending:
            try:
                await self.backend.enqueue(args)
            except Exception as exc:
                errors.append(f"{args.id}: {exc}")
            else:
                self.subenqueues_flushed += 1
        self.subenqueues_discarded = 0
        return errors

    def discard_buffered(self) -> None:
        self.subenqueues_discarded = len(self.buffered)
        self.buffered.clear()


async def run(
    semantics: str,
    child_args: list[str],
    trace_path: Path | None,
    parent_payload_n: int = 0,
) -> dict[str, Any]:
    bridge = ParentBridge(semantics)
    parent_job_id = await bridge.dispatch_parent_job()

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        CHILD,
        *child_args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdin is not None and proc.stdout is not None

    proc.stdin.write(
        json.dumps({
            "op": "run",
            "job_id": str(parent_job_id),
            "payload": {"n": parent_payload_n},
        }).encode() + b"\n"
    )
    await proc.stdin.drain()

    done_msg: dict[str, Any] | None = None
    timed_out = False
    try:
        while True:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=INTERACTION_TIMEOUT_S)
            if not line:
                break  # EOF — child died
            msg = json.loads(line)
            op = msg.get("op")
            if op == "subenqueue":
                ack = await bridge.handle_subenqueue(msg)
                proc.stdin.write(json.dumps(ack).encode() + b"\n")
                await proc.stdin.drain()
            elif op == "done":
                done_msg = msg
                break
    except TimeoutError:
        timed_out = True
        proc.kill()

    proc.stdin.close()
    try:
        await proc.stdin.wait_closed()
    except (BrokenPipeError, ConnectionResetError):
        pass
    await proc.wait()
    stderr_data = (await proc.stderr.read()).decode() if proc.stderr else ""

    child_ok = (
        done_msg is not None
        and done_msg.get("ok") is True
        and proc.returncode == 0
        and not timed_out
    )

    if child_ok:
        # Buffered-bridge ordering, mirroring the consumer: terminal write
        # first, then flush of buffered sub-enqueues.
        await bridge.backend.mark_succeeded(parent_job_id, bridge.worker_id, {"ok": True})
        if semantics == "buffered":
            await bridge.aflush_buffered()
        terminal = "succeeded"
    else:
        error = (done_msg or {}).get("error") or f"child exited code={proc.returncode}"
        await bridge.backend.mark_failed_or_retry(
            parent_job_id,
            bridge.worker_id,
            ErrorInfo(
                error_class="ChildFailure" if done_msg else "ChildCrash",
                error_message=str(error),
                error_traceback=None,
            ),
            None,
        )
        if semantics == "buffered":
            bridge.discard_buffered()
        terminal = "failed"

    rows = await bridge.backend.list_jobs(JobFilter())
    trace: dict[str, Any] = {
        "semantics": semantics,
        "child_args": child_args,
        "child_returncode": proc.returncode,
        "child_done_msg": done_msg,
        "timed_out": timed_out,
        "subenqueues_received": bridge.subenqueues_received,
        "subenqueues_flushed_to_backend": bridge.subenqueues_flushed,
        "subenqueues_discarded": bridge.subenqueues_discarded,
        "ack_errors": bridge.ack_errors,
        "parent_terminal_status": terminal,
        "backend_jobs": [_dump_row(r) for r in rows],
        "backend_job_count": len(rows),
    }
    if "rtt_ms" in stderr_data:
        trace["child_rtt_ms"] = json.loads(stderr_data.strip().splitlines()[-1])["rtt_ms"]

    if trace_path is not None:
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        trace_path.write_text(json.dumps(trace, indent=2, default=str))
    return trace


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--semantics", choices=["buffered", "direct"], required=True)
    parser.add_argument("--subenqueues", type=int, default=5)
    parser.add_argument("--sleep-ms", type=int, default=20)
    parser.add_argument("--crash-after", type=int, default=None)
    parser.add_argument("--fail", action="store_true")
    parser.add_argument("--await-ack", action="store_true")
    parser.add_argument("--late-enqueue", action="store_true")
    parser.add_argument("--trace", default=None)
    args = parser.parse_args()

    child_args: list[str] = [
        "--subenqueues", str(args.subenqueues),
        "--sleep-ms", str(args.sleep_ms),
    ]
    if args.crash_after is not None:
        child_args += ["--crash-after", str(args.crash_after)]
    if args.fail:
        child_args += ["--fail"]
    if args.await_ack:
        child_args += ["--await-ack"]
    if args.late_enqueue:
        child_args += ["--late-enqueue-after-done"]

    trace = asyncio.run(
        run(args.semantics, child_args, Path(args.trace) if args.trace else None)
    )
    print(json.dumps({
        "semantics": trace["semantics"],
        "child_returncode": trace["child_returncode"],
        "parent_terminal_status": trace["parent_terminal_status"],
        "backend_job_count": trace["backend_job_count"],
        "subjobs_in_backend": sum(
            1 for j in trace["backend_jobs"] if j["actor"] == "spike_sub_actor"
        ),
        "subenqueues_received": trace["subenqueues_received"],
    }))


if __name__ == "__main__":
    main()
