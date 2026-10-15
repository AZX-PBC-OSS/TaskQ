# Workflows schema — the §16.4 forever-pins' measurements (T03)

The measured-tax migration's residuals, re-measured on the landed branch.
The pins live in `tests/test_wf_schema_migration.py` (structural + row-width
+ partial-index) and `tests/test_wf_perf_bands.py` (the noise bands); every
pin reds first (the red drills are in the same files, recorded to
`.measurements/`), and the bounds are pinned forever: any PR taxing the
vanilla path beyond them fails CI, and a PR that must tax it argues the
bound in review.

## Method

- Engine: the test suite's exact `postgres:18` testcontainer image and
  server tuning (`fsync=off`, `synchronous_commit=off`,
  `full_page_writes=off`, `max_connections=1000`).
- Row width: BOTH table shapes built from the LIVE bundled migrations (the
  control = every migration before the `01.00.23` round; the extended =
  the full set), seeded identically via the same INSERT builder, 10,000
  rows each, `VACUUM ANALYZE` before measuring.
  `pg_relation_size(jobs)` / live row count = heap bytes/row;
  `avg(pg_column_size(j))` = avg tuple.
- Partial index: the workflow partial indexes cloned onto a 100k-row clone
  table (`LIKE ... INCLUDING ALL` — the migration's exact index shapes),
  a full same-column control index built beside them, sizes via
  `pg_relation_size`.
- Enqueue band: best-of-3-rounds p50 over 50 backend enqueues each (the
  house benchmark's noise-robust statistic — `perf-evidence-dispatch.md`'s
  doctrine: the best round's p50 is the gate; p99 recorded, never gated).
- Dispatch band: `dispatch_batch` (the strict-FIFO production entry) at a
  1,000-row pending backlog, 10 actors, limit 50, oversample 2; best-round
  p50 over 3×20 samples; the backlog re-pended between rounds.

## Row width (pin 1) — the measured residuals

| shape | avg tuple | heap bytes/row |
|---|---|---|
| vanilla (pre-`01.00.23`) | 232.0 B | 241.7 B |
| extended (+5 wf columns, NULL/0) | 234.0 B | 249.0 B |
| **Δ** | **+2.0 B** | **+7.4 B** |

Bound: **≤ 8 B** on both (the ticket's §16.4 law; the prototype measured
+3.0 B / 0.0 B — this checkout's column set re-measures at +2.0 B /
+7.4 B, inside the bound; re-measured, not assumed).

RED DRILL (recorded, `.measurements/pin1-red-pad-drift.json`): the
misaligned-padding fixture (the five columns + a wide text column with a
per-row value) must blow the bound — the drill asserts the drift exceeds
8 B, proving the pin can fail.

## Partial-index exemption (pin 2)

| partial index (the clone's copy) | partial | full (same column, no WHERE) | ratio |
|---|---|---|---|
| join-wait walk (`deps_pending > 0`) | 8,192 B | 3,178,496 B | **0.26%** |
| children walk (`parent_id IS NOT NULL`) | 8,192 B | 3,178,496 B | **0.26%** |

Bound: **≤ 1%** — the measured truth (the prototype's 0.5% / 194×) holds.
RED DRILL: the FULL control index is the convicted fixture — its ratio IS
the shape the pin exists to flag.

## Import law (pin 3)

`import taskq` leaves `taskq.workflows` ABSENT from `sys.modules` (a fresh
interpreter probe + the AST harness's module-scope invariant on
`taskq/__init__.py`). RED DRILL: the mutated `__init__` (a module-level
`import taskq.workflows` prepended) is convicted by the same parser.

## Enqueue-latency noise band (pin 4, `load_sensitive`)

- shipped path: **p50 311 µs** / p99 550 µs (best round), budget 25 ms.
- RED DRILL (recorded): a fixture hook touching the five new columns on
  the hot path (an extra round-trip read per enqueue) moves the band
  (p50 311 → 514 µs) — the pin can fail; a cost-class regression trips it.

## Dispatch-claim noise band (pin 5, `load_sensitive`)

- WITH the `AND deps_pending = 0` exclusion clause present at every
  claimable-row site: **p50 5.9 ms** / p99 9.1 ms (best round), budget
  50 ms — the clause is a semantic no-op for vanilla rows and the pin
  proves it stays that way.
- RED DRILL (recorded): the fixture plan regression (index plans disabled
  → the Seq-Scan monster class) blows the band — the pin can fail.

## CI minutes (DoD-4)

The two `load_sensitive` pins run in the serial perf lane; measured cost
~7 s per run (containers warm) — negligible against the existing
`tests/perf` lane's budget; no new lane added.
