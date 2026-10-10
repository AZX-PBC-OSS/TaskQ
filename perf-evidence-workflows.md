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

- **measured: 50.8–159.7 ms across the nine captured runs** (the
  artifact's newest run: 57.7 ms; `.measurements/runs/fanout-1000-tx-
  band-*.json` carries every run beside the rolled artifact) → band
  **≤ 500 ms** (generous headroom by design — the gate trips on a
  cost-CLASS regression — a per-row round-trip loop at this width would
  pay 2000 round trips: the fanout proof's cut #4 crime, the per-join
  round-trip shape the P1 sweep-cost curve convicts in the sweep-arm
  section below. An earlier revision of this file cited that crime at a
  per-join p50/p95 figure for which NO capture exists anywhere in the
  evidence tree (the P1 summary's real numbers are the sweep curve
  below); the dead figure is DELETED — provenance or silence).

## The join-fire latency

Last-parent-finalize → the joined row dispatchable (`deps_pending = 0` +
one `wf_join_fire` row + the outbox rows), measured on the finalize call
(tx1 + tx2 — the fire is inside tx2):

- **measured: 4.3–42.3 ms across the nine captured runs** (the
  artifact's newest run: 4.3 ms; the runs beside the rolled
  artifact) → band **≤ 50 ms**.

## The sweep arm (the set-based re-derive)

The shipped arm is ONE batched statement (lock the join-wait children
`FOR UPDATE SKIP LOCKED` → count un-terminal parents from the edge ledger
→ reconcile the cache → block the orphans) + the set-based fire arm. The
convicted alternative — the per-join round-trip shape (the fanout proof's
cut #4 crime) — has its MEASURED conviction in P1's evidence, `60.4k
nodes / 30k edges`, the sweep-cost scale curve 200 → 14.9 ms, 1000 →
23.8 ms, 5000 → 39.3 ms, and the unscoped monster 83.7 ms WITH SEQ SCANS
at 75k rows (the CITED-IMPORT artifact:
`.measurements/runs/sweep-cost-scale-curve-*.json` — the P1 spike's
summary imported at the consolidation; the spike's session evidence
directory is gone, and the artifact SAYS SO — the imported record is
the provenance, never a re-claimed fresh measurement). An earlier
revision of this file and of the sweep's source
comments cited the per-join variant at a p50/p95 figure for which no
capture with those numbers exists in any evidence tree
the repo names; the dead figure is DELETED (provenance or silence). The scope
pin convicts the seq-scan shape; the crash matrix + the storm +
duplicate-finalize numbers are P1 FINAL's imported evidence.

## CI minutes (DoD-4)

The bands carry `load_sensitive` (the serial perf lane); no new lane
added. (An earlier revision claimed "measured cost ~7 s per run warm" —
no capture backs the figure; deleted — provenance or silence. Each
band pin's own runtime in CI is the record.)

## The hold→resume latency band (T10, G11c — the third workflow band)

The deliver CAS (validate → the `'held' → 'delivered'` CAS → the node's
resume write — ONE transaction, two statements) measured at the pin
shape (one held node, one run, the local PG lane):

| metric | value | source |
| --- | --- | --- |
| hold→resume (the deliver's commit → the row claimable) | see `.measurements/t10-hold-resume-band.json` (`hold_to_resume_ms`) | `tests/test_wf_hitl_pins.py::test_hold_to_resume_latency_band` |
| the PINNED band | **≤ 50 ms** (the assert fails past 500 ms — the CI headroom; the band is the p99-far-bound, not the p50) | the same pin |

The band's rationale: the deliver is two statements in one tx (the row
CAS + the node's held-representation clear) — single-digit
milliseconds at the pin shape; 50 ms gives the fleet-lane headroom
(the load-sensitive marker applies — the pin runs in the default lane,
the band is wide enough to be deterministic-by-construction).

## The workflow progress bands (T21)

The two-channel persistence's bands, re-proven on the BUILT code
(`.measurements/t21-numbers.json`; the pins:
`tests/test_wf_progress_persistence.py`, `tests/test_wf_progress_emission.py`,
`tests/test_wf_progress_faces.py`):

| band | value | source |
| --- | --- | --- |
| the STATE channel's row count under 10,000 emissions | **2 rows** (nodes × channels — CONSTANT; the convicted every-emission-a-row variant: 2,000 rows for 2,000 emissions, kept red) | `test_chatty_body_coalesce_cadence_and_constant_rows` + the numbers run |
| the occurrence counter under 10,000 emissions | **== 10,000 exactly** (the coalescing is honest) | the same |
| the ring's drop accounting | **appended == retained + dropped** exactly (the bound honored; the counter on the record) | `test_stream_ring_bound_and_drop_counter_honest` |
| the read-side aggregate's fn time (200 children, mid-flight-capable) | **0.009 ms** (the numbers run's own measurement of the shipped shape; the read writes nothing) | the numbers run |
| the finalize's blindness to emission (structural) | the terminal-mark statement touches NO progress table (the zero-finalize-changes probe); a 10k-emission node finalizes normally | `test_zero_finalize_changes_probe` |
| the crash window | freshness-only loss: the display's STATE ledger-derived at every sample; the terminal heals | `test_crash_window_freshness_only_loss_then_the_terminal_heals` |
## The click→panel latency band (T11, the admin workflow page)

The node-detail panel's data path (the route's full read through the
ASGI transport — the server half of the click→panel budget), measured
on the local PG lane (the band's set-then-pin discipline — G11's
method):

| metric | value | source |
| --- | --- | --- |
| the panel route's read (20 samples) | avg 1.205 ms · p95 3.238 ms | `.measurements/t11-latency-band.json` |
| the PINNED band | **≤ 50 ms** (p95, the assert's headroom ×15) | `tests/web_admin/test_workflows_page.py::test_panel_latency_band` |

## The vendored mermaid.min.js (T11 — the air-gap argument)

The same argument alpine/htmx/lucide carry: an air-gapped ops
deployment must not lose the graph to a CDN. The shipped file IS
mermaid v11.12.2's IIFE build, pinned:

| fact | value |
| --- | --- |
| raw | 2,754,895 B (the ticket's recorded size, byte-exact) |
| gzipped | 792,766 B ≈ 774 KB (the admin's `GZipStaticOnly` middleware serves /static/* gzipped) |
| no bundler / no React / no CDN | the file is a self-contained IIFE; the grep-able pin |
