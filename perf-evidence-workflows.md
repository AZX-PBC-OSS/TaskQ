# The workflow engine's bands (T04) — the fan-out tx, the join fire, the sweep

The three G11/G12 benches, measured on the landed branch through the REAL
engine (`taskq.workflows.engine` / `._fork` / `._sweep`) against the test
suite's own Postgres 18 testcontainer. The bands are SET from these
measurements and pinned in the same files that measured them
(`tests/test_wf_perf_bands.py`, `tests/test_wf_fork_pins.py`) — a
regression past a band fails CI and must argue the bound in review.

## Method

- Every measurement drives the engine's own transactions on the migrated
  schema (no mocks): the fork through `finalize_node`'s tx1, the fire
  through tx2 / the sweep arm.
- Wall clock via `time.perf_counter_ns` around the engine call (the
  client-visible latency an operator's step observes).
- Raw outputs: `.measurements/fanout-1000-tx-band.json`,
  `.measurements/join-fire-latency.json`.

## The 1000-child fan-out transaction (G12)

1000 child INSERTs + 1000 edge rows + the join row + the fenced terminal
mark, ONE transaction (`finalize_node` tx1, chunked parallel-array inserts
at 500 rows/statement — never a second transaction, which would break the
fork atomicity rule):

- **measured: 51.8 ms** → band **≤ 500 ms** (generous headroom by design —
  the gate trips on a cost-CLASS regression — a per-row round-trip loop at
  this width would pay 2000 round trips, the fanout proof's cut #4 crime
  measured at p50 477 ms / p95 1.73 s @ ~340 live joins in the per-join
  round-trip shape).

## The join-fire latency

Last-parent-finalize → the joined row dispatchable (`deps_pending = 0` +
one `wf_join_fire` row + the outbox rows), measured on the finalize call
(tx1 + tx2 — the fire is inside tx2):

- **measured: 5.2 ms** → band **≤ 50 ms**.

## The sweep arm (the set-based re-derive)

The shipped arm is ONE batched statement (lock the join-wait children
`FOR UPDATE SKIP LOCKED` → count un-terminal parents from the edge ledger
→ reconcile the cache → block the orphans) + the set-based fire arm. The
prototype's per-join round-trip variant measured p50 477 ms / p95 1.73 s @
~340 live joins vs the set-based 14.9 ms @ 200 (P1's evidence, `60.4k
nodes / 30k edges`, the scale curve 200 → 14.9 ms, 1000 → 23.8 ms, 5000 →
39.3 ms; the unscoped monster 83.7 ms WITH SEQ SCANS at 75k rows). The
scope pin convicts the seq-scan shape; the crash matrix + the storm +
duplicate-finalize numbers are P1 FINAL's imported evidence
(`/tmp/opencode/proto1/spike1/evidence/`).

## CI minutes (DoD-4)

The bands carry `load_sensitive` (the serial perf lane); measured cost
~7 s per run warm; no new lane added.
