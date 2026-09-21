# tors adoption — the NUL scan's before/after, measured

`taskq._json._encoded_has_nul` guards `MAX_RESULT_BYTES` (65536) on every
terminal write: it classifies `\\u0000` byte runs on already-serialized
orjson output, where a live escape (a real NUL, rejected with
`NUL_JSONB_ERROR`) and the literal six-character text are
byte-ambiguous until the backslash run before each match is counted. The
pure-Python loop that ran before adoption was measured linear
(`perf-evidence` history: 175.8 → 1438.4 µs for 1000 → 8000
escaped-literal matches, ~1.7 ms worst case at the bound, on the event
loop).

TaskQ now adopts `tors` (0.10.1, maturin/Rust) as a core dependency —
first-party, published by the same org (AZX PBC) as TaskQ. The scan is
`tors.contains_unescaped` — the same escape-parity scan in Rust over two
zero-copy `PyBytes` borrows with the GIL released — imported directly:
one code path, no probe, no fallback. No TaskQ API changed shape; the
import surface is untouched.

## Parity (proven by execution)

`tests/test_tors_nul_parity.py` differential-pins the scan against an
independent oracle on the adversarial shapes (mid / terminal / offset-0 /
dense / escaped-literal / backslash runs k=1..5 / multibyte boundaries)
plus a 500-example hypothesis sweep over backslash-heavy bytes. The
pre-existing battery — `test_nul_scan_scaling`, the
input-validation-hardening suite, the redaction suites, the NUL-guard
suites — passes UNCHANGED:

| battery (160 tests) | new parity tests |
|---------------------|------------------|
| 160 passed          | 25 passed        |

## Method

- `benchmarks/ab_tors_nul_scan.py`: both implementations timed in ONE
  process, arms interleaved round-by-round (thermal drift cancels),
  min-of-5 rounds × 20 scans, 1 warm-up round excluded (load only ever
  ADDS time, so the least-contaminated round is the closest estimate of
  the scan's own cost).
- Every timed payload's verdict equality (`pure == dispatch`) is asserted
  by the script, so a speedup on a different answer cannot masquerade as
  a win.
- Shapes: the escaped-literal full-walk corpus (the scaling pin's worst
  shape) at the pin's sizes (1000 / 8000 units), at the
  `MAX_RESULT_BYTES` boundary (9362 units = 65536 bytes), at 3 MB; the
  dense live-escape shape (first-match early return, the common
  rejection case); a clean 64 KB memchr miss; and two realistic result
  payloads — the 40k-row shape of `perf-evidence-bulk-cancel.md` and a
  3 MB single-result text payload.
- Engine: CPython 3.13.15, Linux x86_64 (AMD Ryzen AI MAX+ 395),
  otherwise idle. Treat the numbers as a class, not a bound.

## Results — the pure-Python baseline (the "before")

| payload                  | bytes     | pure µs | dispatch µs | ratio  | verdicts |
|--------------------------|-----------|---------|-------------|--------|----------|
| escaped_literal_1000     | 7,002     | 160.5   | 159.9       | 1.00x  | match    |
| escaped_literal_8000     | 56,002    | 1315.0  | 1312.1      | 1.00x  | match    |
| escaped_literal_64k_bound| 65,536    | 1541.8  | 1547.2      | 1.00x  | match    |
| escaped_literal_3mb      | 3,010,002 | 73408.1 | 73795.2     | 0.99x  | match    |
| dense_live_8000          | 48,002    | 0.1     | 0.1         | 0.89x  | match    |
| clean_64k                | 65,526    | 24.6    | 24.4        | 1.01x  | match    |
| realistic_rows_40k       | 1,897,790 | 723.9   | 721.1       | 1.00x  | match    |
| realistic_3mb_text       | 3,000,011 | 1125.9  | 1133.6      | 0.99x  | match    |

(Consistent with the scaling pin's recorded 175.8 → 1438.4 µs — same
class, different machine.)

## Results — tors 0.10.1 (the "after", the shipped scan)

| payload                  | bytes     | pure µs | dispatch µs | ratio  | verdicts |
|--------------------------|-----------|---------|-------------|--------|----------|
| escaped_literal_1000     | 7,002     | 158.1   | 4.9         | 32.17x | match    |
| escaped_literal_8000     | 56,002    | 1370.3  | 40.9        | 33.50x | match    |
| escaped_literal_64k_bound| 65,536    | 1579.4  | 47.0        | 33.60x | match    |
| escaped_literal_3mb      | 3,010,002 | 74370.3 | 2118.1      | 35.11x | match    |
| dense_live_8000          | 48,002    | 0.1     | 0.1         | 1.20x  | match    |
| clean_64k                | 65,526    | 24.5    | 1.0         | 25.65x | match    |
| realistic_rows_40k       | 1,897,790 | 731.9   | 24.2        | 30.20x | match    |
| realistic_3mb_text       | 3,000,011 | 1109.6  | 37.0        | 29.95x | match    |

## Verdict

- The scan WINS measurably on every shape that walks the payload:
  **30–36x** on the escaped-literal worst shape at every size (the
  ~1.5 ms worst case at the 65536-byte bound drops to ~47 µs), 26x on
  the clean miss, 30x on the realistic 40k-row and 3 MB results. The
  dense live-escape shape is a first-match early return for both paths
  (~0.1 µs either way): the one shape where there is nothing to win,
  which is exactly why `tests/test_tors_nul_perf.py` gates the
  escaped-literal corpus and the realistic payload, gated at ≥5x
  (a small fraction of the measured 30–36x, so runner noise cannot flip
  the verdict while a hardware change cannot strand the pin). The pure
  baseline those ratios are measured against is spelled in the perf gate
  and the A/B script themselves — the scan serves one path, so the
  baseline lives where it is timed.
- The redaction layer was evaluated and NOT adopted, on measurement and
  on scope: `sanitize_nul_str`'s single-pair `str.replace` is ~6x faster
  than the nearest 1:1 tors primitive (`tors.replace_many`, 19.9 µs vs
  125.9 µs on a 20 KB traceback shape); `redact_payload` is `dumps` +
  one SHA-256 block (~58 µs on a 2000-row payload) and tors ships no
  sha256 primitive, so no 1:1 mapping exists; the exception-text scrub's
  DETAIL/credential masks are regex contracts whose observables a tors
  scrubber (`scrub_pii` is a different redaction semantics) would
  change — out of scope by the adoption's parity rule.
- `tors>=0.10.1` is a core dependency, first-party like dotenvmodel
  (published by the same org, AZX PBC), so the lock's 24-hour quarantine
  is opted out for it per the dotenvmodel precedent; every other dep
  still honors the cooldown.
