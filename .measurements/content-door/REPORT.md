# THE CONTENT-DOOR LANE'S REPORT

The conviction, now its own cure: **the verifier never read capture
content — live captures whose CONTENT records failure (a header saying
rc=1) greened under the head-stamp verdict.**

## The mechanism

`scripts/verify_evidence_heads.py` gains THE CONTENT DOOR: every
EFFECTIVE claim's capture (the fresh live claim; the fresh chain
terminal — the door applies where the chain lands, per the ported
estate-cures cure) has its own verdict PARSED and checked AGAINST the
claim. The claim's verdict is the registry's own: a fresh live claim IS
the stem's green — so the mismatch test is one-sided by construction:
the content records failure ⇒ the claim lies. The conviction names THE
CONTENT-VERDICT MISMATCH, the file, and the line.

The check is CHEAP by construction: the capture's stamp (first 3 lines)
+ the summary TAIL (last 12 lines) are scanned — never a full parse.
The runtime regression is measured below.

The door is HONEST about formats — the estate's ACTUAL families:

| family | verdict shape | parse |
|---|---|---|
| the run-scoped stamped .txt (the batteries, the gates) | `rc=N` in the stamp | `rc≠0` ⇒ failure |
| the pytest tail | `N failed`, `N passed`, `FAILED tests/…` short-summaries | `N≥1 failed` or a FAILED line ⇒ failure |
| pyright | `0 errors, 0 warnings` | the green shape |
| the JSON band payloads | measurements, no verdict line | JSON loads ⇒ green |
| no shape at all (the staged slot) | — | the TYPED WARNING (`unparsable-capture`, stem + file named) — never a silent pass |

## Red-first receipts

* `red-pins-pre-fix-*.txt` — 12 pins RED against the pre-fix verifier:
  the three conviction shapes (the rc=1 stamp, the forged rc=0 stamp,
  the suppressed-count FAILED line) each greened silently (rc=0); the
  warning edges were silent; the chain-walk pins (the ported cure's)
  red. The honest-family controls passed pre-fix and stay green
  post-fix (17/17 pins green at the landed head).
* `live-estate-verdict-*.txt` — the content-doored verifier against
  the COMMITTED tip estate: 22 convictions (below).

## The live-estate verdict (THE FINDING)

The pre-door verifier's before-receipt at the tip 0e2f5cc09: **rc=0 in
0.245 s** — the whole estate greenstamped. The doored verifier's
verdict on the SAME estate: **rc=1, 22 convictions**, all real:

* THE CONTENT-VERDICT MISMATCH ×2 — the conviction class's real
  instances, exactly the estate-cures report's names:
  - `wf-battery-rv4-20261011T043000Z.txt:1` — the capture's own stamp
    says `rc=1` (`1 failed, 603 passed` in its tail) while it stood as
    the wf-battery-rv4 stem's live green.
  - `deslop-pin-packs-20261011T042947Z.txt:1` — same shape (`1 failed,
    75 passed`).
* The ported chain cure un-hid 20 more rot shapes the "the link must
  resolve" law greenstamped: stale TERMINAL claims
  (battery-final-wf-attacks, battery-wf-suite, fixround-*, the
  wf-battery-* family, stealth-mutant-shipped-drill, estate-slice,
  attack-rounds, wf-perf-bands), DEAD mid-walk links
  (docs-lane-rv2-13-cure-captures, rv2-pins-final and kin,
  mkdocs-strict-docslane), and TWO CYCLING chains (fasttier-final,
  fasttier-final-7a58…).

## The repair (honest, no masking)

* The toolchain proofs, the bands leg (32 passed; the 9 band claims
  auto-written by the writers), wf-bands-rv4, and the scoped-coverage
  gate (92.97% vs the 90.0 floor) re-stamped at the source-final head.
* THE DRAIN: the five registry slots (wf-battery-rv4, deslop-pin-packs,
  fast-tier-n8, evidence-heads-verify, rv2-13-green) staged with honest
  no-verdict declarations (the typed warning — the door's designed
  honesty, not a lie), committed, and FILLED IN PLACE by the green
  runs: the wf battery **612 passed, 0 failed, rc=0** (rv2-13 GREEN
  in-run against the committed staged estate), the de-slopped packs
  **65 passed, rc=0**, the fast tier at -n 8 (**the fast tier's own
  capture is the fill**), then the verifier's and rv2-13's green
  receipts at the filled head. No escape hatch: every intermediate
  state is committed, warned or green, never silently passed.
* The twenty chain stems re-pointed at the fresh family captures that
  genuinely re-prove their kinds (the battery family → the fresh
  wf-battery-rv4 capture; the fasttier family → the fresh fast-tier
  capture; wf-perf-bands → the fresh bands capture;
  mkdocs-strict-docslane → the fresh mkdocs capture).

## The runtime (before / after, the real estate)

| verifier | estate | rc | wall |
|---|---|---|---|
| before (the tip's head-stamp law) | the committed tip estate | 0 | 0.245 s |
| after (the content door) | the same convicting estate | 1 | 0.295 s |
| after (the content door) | the repaired estate at the final head | 0 | 0.344 / 0.347 / 0.378 s, warnings=0 |

The door's cost: ~+0.1 s over ~41 stems + the chain terminals (~61
stamp+tail reads, 1352 registry entries walked) — the verifier stays
fast (the law's own bar).

## The proofs (at the final head)

* ruff check + ruff format --check: clean (1707 files already
  formatted).
* pyright on the script and the pins: 0 errors / 0 warnings / 0
  informations.
* The pins ×2: tests/test_evidence_content_door_pins.py (8) +
  tests/test_wf_evidence_chain_pins.py (9, the ported chain cure's) —
  17/17 green, run twice (5.07 s / 4.91 s).
* rv2-13 GREEN against the committed final estate (1 passed).
* The verifier rc=0, ZERO warnings, against the repaired estate.
* THE FAST TIER at -n 8: **13516 passed / 9 skipped / 1 xfailed /
  0 failed, rc=0** — rv2-13 green IN-RUN under the content door, on the
  contract PG (postgres:18-alpine + the shared pair's tuning).

## The environment finding (the fast tier's two reds, receipted)

The first two fast-tier attempts red on exactly two tests —
`test_cron_due_tick_is_index_bounded_without_sort` (the plan pin: PG
15's planner picks a plain Sort; the pin's Presorted-Key requirement is
PG 18's planner — the shared pair's contract image is
`postgres:18-alpine`, and the lane's first container was `postgres:15`)
and `test_pin_4_enqueue_latency_band` (the untuned container's fsync /
checkpoint defaults blew the band). Both reds are receipted
(`fast-tier-red-attempt-*-pg*.txt`), both green on the contract shape;
the lane's source diff touches no `src/` code — the environment,
honestly recorded, never masked.

The branch: `feat/taskqflow-content-door` @ 5d4104e92 (branched off the
feat/taskqflow tip 0e2f5cc09; the chain-resolution cure PORTED onto the
current tree — the choice, since the current tip is where the estate's
lanes land).
