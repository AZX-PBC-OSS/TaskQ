# PINMAP — the rv6-estate pin pack (the content door's own holes)

File: `tests/test_evidence_door_rv6_pins.py` (drop-in at the lane's
`tests/`; sha256
`dceb474193337d2a92921ac42a9847038c70f4739d257ba80dc969c7ec5c6970`;
ruff-clean and pyright-clean against the repo configs — see RECEIPTS.md's
Environment leg for the venv/PYTHONPATH seam).
Tree pinned at `3f69eaa325d523866de8e4de2e2490e483fe3384` (the
content-door lane's head — `scripts/verify_evidence_heads.py` there is
the cured verifier under pin).
Fixtures: NONE beyond pytest's `tmp_path` (the pack is static — synthetic
estates in scratch dirs, the verifier as a subprocess; the rv2-13 pin's
own method). No markers: the pins are sync and container-free.

Convention for the "red verbatim" column: the pin's OWN failure
signature's first lines in the unmarked run (`red-run-raw.txt`) — the
assertion line and the first `E`-line carrying the conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-D1 (the scan window: a mid-file red verdict outside the 3+12 edge window greens) | `test_d1_the_door_convicts_a_red_verdict_anywhere_in_the_capture` | `assert "d1-stem" in output and "the head-stamp law holds" not in output, (`<br>`E   AssertionError: F-D1: the red verdict mid-capture was never read — the door greens a capture whose own content records failure because the verdict sits outside the first-3/last-12 edge window. …` | xfail-strict |
| F-D2 (the JSON launder: `*.json` + `{"rc": 1, "failed": 3}` greens) | `test_d2_a_json_capture_verdict_is_never_laundered` | `assert "d2-stem" in output and "the head-stamp law holds" not in output, (`<br>`E   AssertionError: F-D2: the JSON capture's own verdict fields (rc=1, failed=3) were never read — a valid-JSON body greens whatever it records. …` | xfail-strict |
| F-D3 (the empty registry greens vacuously) | `test_d3_the_empty_registry_is_a_loud_refusal` | `assert "rc=0" not in output, (`<br>`E   AssertionError: F-D3: the empty registry GREENS — zero claims is zero verdicts, and the estate passes vacuously. …` | xfail-strict |
| F-D4 (a capture dated 2030 with a fresh head_sha greens) | `test_d4_a_future_dated_capture_is_refused` | `assert "d4-stem" in output and "the head-stamp law holds" not in output, (`<br>`E   AssertionError: F-D4: the capture dated 2030 greens — the registry's captured_at field (and the filename's embedded timestamp) is never parsed. …` | xfail-strict |
| F-D5 (GITHUB_EVENT_PATH forges the verified head) | `test_d5_the_verified_head_is_the_repos_own` | `assert "the head-stamp law holds" not in output, (`<br>`E   AssertionError: F-D5: the verifier judged the estate against the FORGED head ffffffffffff… (the env payload's say-so) and GREENED — the repo's own HEAD was never consulted. …` | xfail-strict |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv6-d1** — the door convicts a red verdict ANYWHERE in the capture:
  the estate's fresh claim over a capture whose line-24-of-44 carries
  `1 failed, 31 passed` (stamp `rc=0` at line 1) exits nonzero NAMING
  `d1-stem`. Both cure directions satisfy it: the full-content verdict
  scan, or the bounded window redefined by verdict-SHAPED lines (the
  window's membership decided by the verdict regexes, never by line
  counts from the edges).
* **rv6-d2** — a `*.json` capture carrying failing verdict fields
  (`rc`/`failed`) is convicted (nonzero exit naming `d2-stem`). Both
  cure directions satisfy it: the door reads the JSON family's verdict
  fields, OR the measurements family is declared exempt BY NAME — this
  capture's name (`pytest-summary-…`) is deliberately outside the band
  family's shape, so the named exemption still judges it on content.
* **rv6-d3** — `claims: []` exits nonzero with a refusal that NAMES the
  empty registry (the missing-registry law's own shape extended to the
  empty one — an estate with no claims is unverifiable, never
  vacuously green).
* **rv6-d4** — a future-dated capture (registry `captured_at` 2030 AND
  the filename's embedded `20300101T000000Z`) exits nonzero naming
  `d4-stem`. The cure may parse either timestamp seat (the registry
  field is always present — `record_claim` writes it; the filename when
  it embeds one); a clock-skew tolerance satisfies the pin (2030 is
  years past any).
* **rv6-d5** — with `GITHUB_EVENT_PATH` pointing at a hand-written
  payload naming a sha that exists nowhere in the repo, the estate
  never greens: env ignored ⇒ the forged-sha claim is stale at the
  repo's HEAD (the off-by-one rule's own red — the sha resolves to no
  ancestor); env cross-checked ⇒ the unresolvable sha is refused. The
  pin asserts only "the law holds" never prints.

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-D1's conviction word** — the pin asserts the stem is NAMED with a
   nonzero exit, deliberately NOT the current "CONTENT-VERDICT MISMATCH"
   vocabulary: a cure that redefines the scan window may rephrase the
   message. If the maintainers want the vocabulary itself pinned (the
   door's convictions greppable), tighten the assertion.
2. **F-D2's warning alternative** — a cure that exempts the band family
   by name AND degrades a foreign JSON to the typed WARNING (rc=0) does
   NOT satisfy the pin (the launder stays half-open for verdict-carrying
   JSON outside the family). Flagged: if the maintainers rule the
   warning IS the honest outcome there, rewrite the pin to accept
   rc=0-with-the-warning-naming-the-file.
3. **F-D4's tolerance seat** — the pin convicts a YEARS-future stamp; it
   takes no position on the tolerance's size (minutes of clock skew vs
   the capture's own run window) or on whether the filename timestamp or
   the registry field is the parsed seat (both are staged future; either
   read convicts).
4. **F-D5's CI carve-out** — the pin tolerates the legitimate CI need
   (the PR merge-ref checkout: the payload's `pull_request.head.sha` IS
   the branch's head there) only under a cross-check against the repo's
   own history (the payload sha must resolve as an ancestor/constituent
   of the checkout). "Env ignored entirely" also satisfies the pin and
   moves the CI seam's cost to the workflow file (a `--head` flag or a
   recorded-merge-ref convention) — flagged, not judged.

## Unreproducible findings

NONE — all five findings reproduced live at
`3f69eaa325d523866de8e4de2e2490e483fe3384` before pinning (the pre-pin
repro script + output are in `repro/`; every leg rc=0). No probe
artifacts of my own needed correction.
