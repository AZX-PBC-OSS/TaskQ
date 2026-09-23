# Issue 419 OOM verdict: dead on current main

Status: VERDICT REACHED (2026-09-23, two bounded local runs against
`origin/main` at `c7277f78`, plus a CI-history sweep). No fix branch was
owed; nothing in this note required a code change.

## The original symptom

The issue-419 memory-hunt agent (session of 2026-09-21, residue in
`/tmp/opencode/oom-419`, last activity 02:17) reported the coverage leg's
test process vanishing silently at roughly 98% suite completion: no
failed tests printed, no summary line, process gone. The working
hypotheses were the retry-contract warning loop and the hypothesis twin
pair. That session ended without a verdict.

## What changed on main since the symptom

Four merges plausibly cover the mechanism, and each is independently
capable of retiring the observed death:

- The budgeted-pool family was fixed (#427 plus the post-tx repair).
- The drain-cost pins moved regimes (#473).
- The coverage leg now runs `-n 2` (#451), halving the traced tree's
  resident footprint per worker (2 cores per worker instead of 1).
- The always-firing multi-HTTP-stack config warning was retired (#475,
  `f2be6043`), removing the warning loop suspect outright.

## Evidence 1: CI history has no silent death in the last day

Every `CI` run on `main` in the trailing window is `success` (the two
`cancelled` entries are concurrency supersessions; the
`action_required` rows are gated release-please workflows). The latest
main run's coverage job (run `35806894622`, job `107009708715`) ends
with a full summary and the coverage table, not an abrupt cutoff:

```
TOTAL                                          24266    834   6862    510    95%
========= 10269 passed, 7 skipped, 122 warnings in 1684.87s (0:28:04) ==========
```

## Evidence 2: two local runs, RSS-sampled, both complete

Environment: `origin/main` at `c7277f78`, fresh worktree,
`uv sync --locked --all-extras --group dev`, 32-core host, 120 GB RAM.
Command per run (the protocol's coverage-shaped fast tier):

```
uv run --no-sync pytest -m "not integration and not slow and not load_sensitive" --cov -n 2 -q
```

A background sampler read every process in the pytest tree every 5 s
(`/proc/*/stat` field 24, summed over the tree) and recorded each
minute's peak in KB. Logs: `rss-run1.log`, `rss-run2.log` beside this
note's working tree.

Run 1:

```
run1 minute=0 peak_rss_kb=1187840
run1 minute=1 peak_rss_kb=1188536
run1 minute=2 peak_rss_kb=1695444
run1 minute=3 peak_rss_kb=1328168 RUN_ENDED
7661 passed, 2 skipped, 117 warnings in 305.46s (0:05:05)
```

Run 2:

```
run2 minute=0 peak_rss_kb=1159876
run2 minute=1 peak_rss_kb=1247720
run2 minute=2 peak_rss_kb=1176156
run2 minute=3 peak_rss_kb=1297448
run2 minute=4 peak_rss_kb=1289280 RUN_ENDED
7661 passed, 2 skipped, 117 warnings in 284.89s (0:04:44)
```

Both runs printed the full `short test summary info` block and the
coverage `TOTAL` table (88% locally because the `integration` mark is
excluded from this filter; CI's leg, which keeps integration, holds
95%). Peak tree RSS never exceeded 1.66 GB, well under the 3 GB bound.
The pre-fix symptom (a silent vanish near the end, no summary) did not
reproduce; minute 3-4, where the predecessor's run died at ~98%, is
where these runs print their summary and exit cleanly.

## Verdict

The issue-419 OOM is DEAD on current main. Both verdict runs completed
with bounded RSS, and the day's CI coverage legs end in summaries. The
fix candidates above (#427, #473, #451, #475) are the likely retired
mechanisms; no single one is pinned as THE cause, and with the symptom
unreproducible on main, pinning it further has no remaining value.

## What was NOT done

- No dump analysis of the predecessor's residue was needed; the local
  runs never died.
- No fix branch was cut (`fix/419-oom-verdict` was owed only on a red
  or a vanish; there was neither).
- The two hypothesis twins and the retry-contract warning loop were
  not independently exonerated; #475 removed the warning loop and both
  runs exercise the twins green under the tracer. The mechanism-level
  attribution above is by merge history, not by repro.
