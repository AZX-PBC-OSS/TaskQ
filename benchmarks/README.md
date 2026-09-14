# Benchmark toolkit — ops manual

Ongoing tooling around the A/B harness in `bench_hotspots.py` (which measures
"current code" vs a "proposed variant" in interleaved batches, with
correctness assertions). This manual covers monitoring, maintenance, and
regression testing; see the harness docstrings for how individual benches work.

| File | Role |
|---|---|
| `bench_hotspots.py` | The A/B harness itself (19 benches + stall probes + cProfile mode) |
| `stress_dispatch.py` | ~20s per-job CPU-path stress (no Postgres), prints jobs/sec |
| `gil_sample.py` | In-process GIL sampler + flamegraph (py-spy fallback for macOS) |
| `run_bench.py` | CLI runner: structured JSON, baselines, regression gate |
| `profile_hotspot.py` | Profile one named bench under cProfile / pyinstrument / gil-sampler |
| `run_matrix.sh` | Run the suite across Python 3.12/3.13/3.14 via uv |
| `results/` | Run history + baselines + profile artifacts (mostly gitignored) |

## Quick spot-check

```sh
make bench                          # full suite, human table
python benchmarks/run_bench.py --only di,jsonb   # subset
make bench-stall                    # + event-loop stall probes
```

Every run writes `results/run-<iso-ts>-<shortsha>.json` (gitignored history).

## Save a baseline

```sh
make bench-save    # or: python benchmarks/run_bench.py --save-baseline
```

Writes `results/baseline.json` (canonical, tracked) and
`results/baseline-<pyver>-<shortsha>.json` (untracked copy). Re-save after
landing an intentional perf change; commit the refreshed `baseline.json` in
the same PR.

## CI regression gate

```sh
make bench-check    # exit 0 clean, 1 regression, 2 missing baseline
```

Compares the current run vs `results/baseline.json` per bench name and prints
`bench / baseline / current / delta / verdict`. The rule, by design:

- Both sides are **medians of interleaved batches** (the harness's
  interleaving cancels thermal/frequency drift within a run), so the gate
  compares the **ratio of medians**.
- A bench is a **REGRESSION** only when it is slower by **both**
  `> --threshold` (default 1.20×, i.e. 20%) *and* **more than
  `--min-abs-delta`** (default 100 ns/op). Requiring both conditions keeps
  sub-microsecond benches from flip-flopping the gate on timer jitter.
- `IMPROVED` is symmetric (faster by both conditions); `NEW`/`MISSING` benches
  are reported but do not fail the gate.
- A correctness mismatch (`correct=False` in either run) is `MISMATCH` and
  **fails the gate** — a variant that stopped being output-identical is worse
  than a slow one.

CI snippet (after committing `baseline.json`):

```yaml
- run: uv run --no-sync python benchmarks/run_bench.py --check
  # exit 1 fails the job; exit 2 means baseline.json is missing — commit it
```

## Cross-version matrix

```sh
make bench-matrix    # 3.12 + 3.13 + 3.14; needs uv (fetches interpreters)
BENCH_MATRIX_VERSIONS="3.12 3.14" bash benchmarks/run_matrix.sh   # subset
```

Creates one uv venv per version under `.bench-matrix/` (gitignored), installs
taskq editable, runs `run_bench.py --json` in each, collects the JSONs into
`results/matrix-<ts>/`, and prints a bench × version summary of A-side medians.
Fail-soft: an unavailable interpreter skips that version.

## Profiling spot-checks

```sh
make bench-profile                            # cProfile the DI bench (default hotspot)
python benchmarks/profile_hotspot.py jsonb    # named bench: jsonb|decode|cron|rows|retry|di
python benchmarks/profile_hotspot.py cron --engine pyinstrument
python benchmarks/profile_hotspot.py --engine gil-sampler
```

`cprofile` is stdlib; `pyinstrument` falls back to `cprofile` with a warning
if not installed; `gil-sampler` runs the ~20s dispatch stress under
`gil_sample.py` and prints top GIL-holding leaf frames. Artifacts
(`profile-*.txt`, `gil_dispatch-*.raw/.svg`) land in `results/`.

**py-spy on macOS needs root** (`task_for_pid`), which is usually unavailable —
that is why `gil_sample.py` exists: it samples `sys._current_frames()` from a
sampler thread around the same stress workload, producing the same
GIL-holding population py-spy `--gil` would report. On Linux, `py-spy` works
without root: `uv run py-spy record -o results/flip.svg --gil -- python
benchmarks/stress_dispatch.py`.

## Adding a new A/B bench

Write it in `bench_hotspots.py` following the house rules, then register it in
**both** `bench_hotspots.main()`'s dispatch and `run_bench.py`'s
`BENCH_GROUPS` so the runner and gate see it:

1. **Interleaved batches, always.** Measure A and B in alternating batches
   (`ab_bench()` does this) so thermal/frequency drift cancels; never run all
   of A then all of B.
2. **Correctness assertion required.** A bench that cannot prove B is
   output-identical to A must not report timings (`correct=False` fails the
   regression gate). Write the assertion before trusting any number.
3. **Honest variants.** B must implement the real proposal, not a stub that
   skips work A must do. Say in `note=` what B actually changes; the note
   travels into the JSON and the gate table.
4. **Batch sizing.** Pick `batch` so one batch is ~0.1–10 ms of work:
   too small and timer resolution dominates; too large and the interleaving
   stops canceling drift. Use `batch=1` (plus `batches`) for naturally
   batched work like 1000-row decodes; 200–500 for ns/µs-scale primitives.
   Keep `batches` at 5–7 — the median needs odd counts.

## Results file schema

Written by `run_bench.py` to `results/run-*.json` and `results/baseline*.json`:

```jsonc
{
  "schema": 1,
  "timestamp": "2026-09-12T10:15:30.123456+00:00",  // UTC, ISO-8601
  "git_sha": "e89ba7a…",                             // "" when not a git repo
  "git_dirty": false,                                // true if uncommitted changes
  "python_version": "3.13.7",
  "platform": "macOS-15.6.1-arm64-arm-64bit",
  "cpu_brand": "Apple M4 Pro",
  "results": [                                       // one entry per A/B bench
    {
      "name": "jsonb_str[small] A=cur B=bytes",
      "a_ns": 3210.5,        // A-side median ns/op (the "current code" side)
      "b_ns": 987.6,         // B-side median ns/op (the variant)
      "speedup": 3.25,       // a_ns / b_ns
      "correct": true,       // B provably output-identical to A
      "note": ""             // harness annotation, travels to all tables
    }
  ],
  "stall_probes": [          // present only for --stall runs
    {"name": "cron burst 10k schedules (1 full burst)",
     "max_stall_ms": 3.2, "mean_stall_ms": 1.1, "wall_s": 2.01, "ticks": 1900}
  ]
}
```

The regression gate compares `results[].a_ns` per `name` (see the rule above);
`stall_probes` are recorded for trend-spotting, not gated.
