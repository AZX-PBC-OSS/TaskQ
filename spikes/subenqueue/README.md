# Spike: sub-enqueue bridge semantics for foreign-runtime actors (TS-in-Node)

Simulates a TS actor executing in a Node child process over NDJSON stdio.
The child cannot join the worker's DB transaction, so `ctx.jobs`-style
transactional fan-out must be rebuilt at the bridge. Two candidate
semantics were implemented and run for real against taskq's
`InMemoryBackend` (the same backend that simulates transaction rollback
for in-process `SubJobEnqueuer`):

- **(a) BUFFERED-BRIDGE** — the worker buffers every
  `{op:"subenqueue"}` request locally (mirroring `SubJobEnqueuer._pending_buffer`),
  acks it immediately, and only flushes the buffered `EnqueueArgs` to the
  backend after the child reports `{ok:true}` (terminal write first, then
  flush — the same ordering as the consumer's `flush_buffer()` after
  commit). Any failure/crash path discards the buffer.
- **(b) DIRECT** — every sub-enqueue request is executed immediately via
  `JobsClient.enqueue` on receipt (at-least-once; no undo on failure).

## Files

| file | role |
|---|---|
| `worker_child.py` | Simulated foreign runtime: NDJSON stdio, emits N sub-enqueue requests, `--crash-after K` (`os._exit(1)`, no `done`), `--fail`, `--await-ack` (request/response mode) |
| `worker_parent.py` | Asyncio bridge owning `InMemoryBackend` + `JobsClient`; dispatches a real parent job, runs the child, honors sub-enqueues per `--semantics {buffered,direct}`, applies the terminal write, dumps backend state |
| `scenarios.py` | Runs the full matrix, writes one trace per combo to `traces/` |
| `latency.py` | 100 sequential await-ack sub-enqueues; p50/p95 of child-measured round trips |
| `traces/*.json` | Actual `backend.list_jobs()` dumps per run |

Reproduce: `uv run python spikes/subenqueue/scenarios.py && uv run python spikes/subenqueue/latency.py`

## Results: scenario × semantics → resulting backend state

The parent job went through the real backend lifecycle each run
(enqueue → `dispatch_batch` → terminal write); "sub-jobs" counts rows with
`actor=spike_sub_actor`. Source traces: `traces/summary.json`.

| scenario (child behavior) | (a) BUFFERED-BRIDGE backend state | (b) DIRECT backend state |
|---|---|---|
| **success** — 5 sub-enqueues, `{ok:true}` | 5 sub-jobs `pending` (n=0..4), parent `succeeded` | 5 sub-jobs `pending` (n=0..4), parent `succeeded` |
| **crash K=3** — 3 sub-enqueues, `exit(1)` mid-run | **0** sub-jobs, parent `failed` | **3** sub-jobs `pending` (n=0..2), parent `failed` |
| **crash K=0** — `exit(1)` before any request | **0** sub-jobs, parent `failed` | **0** sub-jobs, parent `failed` |
| **failure after 5** — 5 sub-enqueues, then `{ok:false}` | **0** sub-jobs, parent `failed` | **5** sub-jobs `pending` (n=0..4), parent `failed` |

The two semantics are indistinguishable on the success path and diverge
on exactly the paths where the actor didn't succeed.

## Atomicity story

**(a) BUFFERED-BRIDGE — all-or-nothing per parent attempt.**
Requests are buffered in worker memory and only become durable after the
child's success report, in the order terminal-write → flush. Equivalent
to `SubJobEnqueuer`'s in-memory transactional simulation
(`_pending_buffer` / `flush_buffer` / `discard_buffer`): a crash, an
`{ok:false}`, or a timeout discards the buffer, so the backend is never
polluted by a failed parent. The atomicity window moved, not vanished:
between "child said ok" and "flush complete" the sub-jobs exist only in
worker RAM — a worker crash in that window loses them (same exposure as
the existing in-memory simulation; a real Postgres design can shrink it
by flushing inside the parent job's commit transaction, restoring exact
atomicity). Caveat inherited from `flush_buffer`: if the terminal write
succeeds but a flush item fails, the parent is `succeeded` with lost
sub-jobs (surfaced as `SubEnqueueError`).

**(b) DIRECT — at-least-once, no atomicity.**
Each request is a separate autonomous commit the moment it arrives. A
crash after 3 of 5 leaves 3 durable orphan sub-jobs whose parent will
never succeed — no rollback exists, and a parent retry would re-run the
child and re-enqueue (duplicates unless idempotency keys are used end to
end). This is the "partial fan-out" failure mode the Python
`SubJobEnqueuer` was built specifically to prevent.

## Measured per-sub-enqueue overhead (semantics (a), await-ack mode)

100 sequential sub-enqueues, child blocks on the parent's ack each time;
samples are child-measured write→ack round trips over local pipes
(`traces/latency.json`, `traces/latency_await_ack.json`; three runs:

| run | p50 | p95 | mean |
|---|---|---|---|
| 1 | 0.047 ms | 0.090 ms | 0.051 ms |
| 2 | 0.046 ms | 0.097 ms | 0.053 ms |
| 3 | 0.045 ms | 0.079 ms | 0.048 ms |

≈ **45–50 µs p50, ~0.1 ms p95** per sub-enqueue round trip. That is the
cost of an *awaiting* `ctx.jobs.enqueue` in (a): 2 pipe hops, 2
NDJSON parse/serializes, 1 event-loop scheduling hop each way. At TaskQ
realistic fan-out (tens of sub-jobs per job) this is noise (<5 ms per
job); it is 2–3 orders of magnitude below a single Postgres round trip.
In fire-and-forget style the child pays no RTT at all (control run in
`traces/latency_fire_forget.json`), which pushes toward the API design
below.

## Recommendation: (a) BUFFERED-BRIDGE

- It is the only semantics that preserves TaskQ's core fan-out contract —
  "sub-jobs exist iff the parent succeeds" — across a process boundary
  that cannot share a transaction. The spike shows (b) leaking durable
  orphans on every failure path (3/5 on crash, 5/5 on reported failure).
- It reuses the proven `SubJobEnqueuer` buffer/flush/discard shape and its
  consumer ordering, so the bridge is a transport adapter, not a new
  semantic.
- Orphan cleanup under (b) needs idempotency keys + reconciliation sweeps
  — new machinery with new failure modes. Under (a) the failure mode
  (buffer lost on worker crash mid-flush) is at-least-nothing rather than
  at-least-something, which matches the documented in-memory-simulation
  exposure TaskQ already accepts for Python actors.
- The RTT cost is only paid if the child-side API awaits acks; see below
  — it doesn't have to.

## Protocol flag: sub-enqueue arriving AFTER the child reported success

Demonstrated: `traces/late_enqueue_after_done_buffered.json` (child emits
a 6th request 50 ms after `{ok:true}`; run ends with 5 sub-jobs, parent
`succeeded`, child exit 0). The parent stops reading stdout after
`done`, so the late request lands in a pipe nobody reads and **silently
vanishes — never acked, never enqueued, no error anywhere**. Under an
await-ack child it would be worse: a deadlock (the parent never acks
after `done`). The protocol MUST therefore forbid it, not merely drop
it: after sending `done` the child must not emit any further `subenqueue`
(parent treats a post-`done` request as a protocol violation and fails
the job), and the runtime should close its enqueue surface on completion.

## Child-side API implication: await the ack, or fire-and-forget?

The parent acks every request either way; the choice is the child's:

- **Fire-and-forget** (recommended default for `ctx.jobs.enqueue` in the
  TS runtime): the enqueue call returns immediately with a locally
  synthesized handle (exactly what `SubJobEnqueuer` returns while
  buffering — a synthetic `JobRow`, `was_existing=False`). Zero RTT, and
  semantics stay all-or-nothing because durability is decided by the
  parent's flush, not by the child's write. Matches the existing Python
  API shape where the buffered path never awaits the backend.
- **Await-ack** must NOT be the default: it adds ~50 µs p50 per call, and
  acks mean different things per semantics — in (a) "ack" = "buffered"
  (no durability statement whatsoever), in (b) "ack" = "durable". An
  await-ack API invites the TS actor to treat ack-as-durable and write
  code that diverges between semantics. If a future ack is wanted, it
  should be opt-in and explicitly documented as backpressure, not
  durability.
