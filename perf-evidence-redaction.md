# `redact_payload` call cost — measured evidence for the retired 10µs CI gate

`taskq.obs.redact_payload` (`src/taskq/obs/_structlog.py`) is the sanctioned
way payload-derived data reaches a log line: it returns the first 16 hex
characters of the SHA-256 digest of the JSON-serialized payload, so a log
event carries a stable fingerprint of a payload and never its content.

## Why there is no timing gate in the unit suite

`tests/test_obs_logging.py` carried a gate asserting the call's **average**
time stayed under 10µs. It was a lottery, not a gate: it failed at
**26.53µs** on a loaded CI runner — runner noise, not a code change — and a
gate that random-fails trains people to ignore it. An average over bare
back-to-back `perf_counter_ns` samples has no statistic that survives a
co-tenant: a single scheduler preemption between the two clock reads
injects milliseconds into a mean whose budget is microseconds.

The behavior redaction exists to provide — correctness and no leakage — is
pinned by the unit tests (`test_redact_payload_handles_realistic_payloads`,
`test_log_line_with_redacted_payload_hash_leaks_no_payload_fragments`, and
the deterministic-shape pins next to them). The cost class is recorded
here instead, where noise can be stated honestly instead of failing a PR.

## Method

- `redact_payload` on the production-shape payload of the unit test
  (nesting, lists, unicode, nulls, booleans, numbers — the same dict, so
  the serialization cost is the realistic one).
- 10,000 timed calls after a 1,000-call warm-up, `time.perf_counter_ns`
  around the bare call; median, p95 and min reported.
- 5 repetitions of the whole procedure in one process; every repetition
  reported (no best-of cherry-picking).
- Engine: CPython 3.13, Linux x86_64 container, otherwise idle. The
  machine is *not* a loaded GitHub runner — treat the numbers as a class
  (single-digit µs), not a bound.

## Results

| repetition | median µs | p95 µs | min µs |
|---|---|---|---|
| 1 | 1.58 | 1.92 | 1.24 |
| 2 | 1.63 | 1.83 | 1.36 |
| 3 | 1.39 | 1.52 | 0.95 |
| 4 | 1.68 | 2.00 | 1.25 |
| 5 | 1.58 | 1.72 | 1.29 |

## Verdict

- The call is a `dumps` + one SHA-256 block: single-digit µs on the
  realistic payload, ~1.6µs at the median. There is nothing to regress
  silently: the cost is one serialization, and a change that made it
  O(payload × n) or I/O-bound would move it by orders of magnitude.
- If a budget ever needs re-enforcing, gate a **median** or **min-of-N**
  with an order of magnitude of headroom (the file's
  `test_bind_job_context_performance_bounded` is the pattern: median,
  20× headroom, slow-marked), never a bare average — and only at a
  surface an operator can observe, not an internal helper's micro-budget.

## Appendix: `_scrub_text` on a realistic traceback — measured evidence for the retired 1 ms CI gate

`tests/test_obs_exception_redaction.py` carried a gate asserting a 27-frame
traceback scrubs in under 1 ms. It failed at **2.2 ms** on a shared CI runner
(single preemption between two clock reads on an operation whose clean cost is
~20 µs) — the same lottery the section above documents, at the same ratio of
noise to budget. The gate is retired; the behavioral contract it protected (a
credential-free traceback survives the scrub verbatim) is pinned as
`test_scrubbing_a_realistic_traceback_preserves_the_diagnostics`, and the
scan's linearity on the dotted shapes a traceback reaches the scrub with stays
pinned structurally by `test_jwt_scan_stays_linear_on_long_word_runs`.

### Method

- The realistic field shape: `"Traceback (most recent call last):\n"` plus 27
  `'  File "taskq/worker.py", line 1, in run\n'` frames plus
  `"RuntimeError: deadline exceeded"` (dots from the `.py` paths, so the JWT
  prefilter fires — the trigger-present case, not a free pass).
- Single timed call after import warm-up, `time.perf_counter` around the bare
  call; for the scale linearity check, the same shape at 40× the frames
  (1080), one timed call.
- Engine: CPython 3.13, Linux x86_64 container, otherwise idle. Treat the
  numbers as a class (tens of µs per traceback, linear in frames), not a bound.

### Results

| shape | frames | text size | time |
|---|---|---|---|
| realistic traceback | 27 | ~1.4 KB | ~20–25 µs |
| 40× traceback | 1080 | ~57 KB | ~0.86 ms |

Scaling 40× the frames moved the cost ~36× — linear (the per-frame scan is
O(text)), no super-linear term at traceback magnitudes. A quadratic term would
have shown ~1600×.

### Verdict

- The scrub of a rendered traceback is microsecond-scale at realistic frame
  counts and linear in frames. Nothing to regress silently: a change that made
  it super-linear would show up in the structural JWT-linearity pin first
  (that pin's shapes are the scan's worst case), and a change that made it
  I/O-bound or copying-bound would move the absolute cost by orders of
  magnitude, visible in any benchmark run rather than a PR lottery.
