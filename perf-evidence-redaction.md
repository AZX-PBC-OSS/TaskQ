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
