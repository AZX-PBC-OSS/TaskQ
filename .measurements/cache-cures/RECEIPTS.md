# THE CACHE-CURES LANE — the receipts

Branch `feat/taskqflow-cache-cures` off `feat/taskqflow` @ `0e2f5cc096727`.
The red receipts were captured BEFORE the cures, on the base head, against
the lane's own PG (:5810). The cure commit is the source landing; this
directory's repro scripts are the red receipts' replayable evidence.

## CURE 1 — the route-source wedge (P8f, the merge blocker)

- **Mechanism**: `cache=True` on a typed-route source; run 1 stores; run 2
  hits — and the hit path never rebuilds the route fork (the arm dispatch
  reads the element's RUNTIME type; the cached envelope carries only
  encoded dicts — the fork cannot be rebuilt from the envelope by
  construction; the map face rebuilds, the route face mechanically cannot).
- **Red** (`route_wedge_repro.py`): run 1 succeeded with the full fork
  (source + 2 arms + join + collect); run 2 WEDGED — `running` forever,
  the `collect` row pending, the source reading `succeeded` wearing the
  `wf_cache_hit` receipt, ZERO route children, ZERO join rows.
- **Green**: the wiring refuses (both spellings — `route()` and
  `map_source`'s dict form) + the validator's `E17-cache-route-source`
  re-proof. The wedge's own law holds in the pin: at the first hit the
  fork spawns identically — because the hit can never happen.

## CURE 2 — the helper-closure hole (P2c)

- **Mechanism**: the address covers the body's OWN source only — a
  helper's redeploy is a byte-identical address; the pre-deploy truth
  serves until TTL.
- **Cure (docs, stated exactly)**: the `_cache.py` module docstring's
  scope law + the guide's determinism section: the operator's discipline
  is bumping the body's own source when the closure changes. A
  transitive-closure hash was considered and NOT built (the cost /
  false-positive design not justified) — the recorded-not-built row is in
  the guide.

## CURE 3 — the ctx.input cross-tenant leak (P2d)

- **Mechanism**: the address covers the wired args only; `ctx.input` never
  enters it.
- **Red** (`ctx_input_leak_repro.py`): tenant B's run terminal-SUCCEEDED
  with tenant A's answer (`tenant-A:fixed-name`), the body never ran, the
  receipt naming A's run.
- **Green**: the static source-scan refusal at the wiring (one walk shared
  with the validator's `E18-cache-ctx-input`); the wired-param version
  (the guide's teaching example) caches per value — pinned live.

## AMENDMENT A — the source-node input blindness (the import probe's #4)

- **Red** (`source_feed_stale_hit_repro.py`, the 640→60 serve): the
  640-row feed stored; the 60-row feed was served **640 rows**,
  terminal-success, the body never ran.
- **Green**: the E18 refusal's face covers it (a source node whose only
  run-varying input is the flow input has no sound address); the
  docstrings + the guide state WHICH input counts for WHICH node kind.

## AMENDMENT B — the unverifiable-source silence (the import probe's #3)

- **Red** (`unverifiable_source_silent_repro.py`): `cache=True` accepted
  at wiring; both runs succeeded; the body ran EVERY run; zero cache rows;
  `code_version` NULL — the only trace two log lines.
- **Green**: the precondition refuses at the wiring (deterministic — the
  claim path's hash reads the same source through the same `own_source`
  seam, no false-positive risk) + the validator's `E20
  -cache-unverifiable-source` re-proof + the guide's stated precondition.

## CURE 4 — the strengthened pins

- **(a) the input-sensitivity pin**: two DIFFERENT wired inputs through
  one cached body — two addresses, both bodies pay once, each hit serves
  its own result, no cross-serve (`test_two_different_inputs_two_addresses_no_cross_serve`).
- **(b) the CAS-freshness pin**: first-winner-KEEPS (a store against a
  FRESH row loses — the last-writer-wins shape is dead) distinguished from
  the expired-corpse RE-FILL (wins); the two-armed race pinned — exactly
  ONE concurrent winner (the loser's `DO UPDATE` re-checks the freshness
  leg against the row the winner just made fresh)
  (`test_cas_freshness_first_winner_keeps_expired_corpse_refills`).

## CURE 5 — the poison-pill door (the eviction face)

- `taskq wf cache evict` — by `--address-prefix` / `--flow` / `--step` /
  `--all`; **DRY-RUN default** (what WOULD go; nothing touched), `--yes`
  deletes; a bare evict refuses by name.
- `taskq wf cache list` — the cache rows (address, fresh/EXPIRED,
  producing run, store/expiry instants) AND the hits' `wf_cache_hit`
  receipts.
- The docs' operations paragraph: the guide's "Operating the cache" section.

## CURE 6 — the smaller

- **(a) the hit-finalize fault's one seat**: the finalize moved to the
  claim tail OUTSIDE the body boundary. Red (`hit_finalize_fault_repro.py`):
  an `InterfaceError` blip inside the hit's finalize was ABSORBED by the
  body ladder — the node terminal-FAILED on a `max_attempts=1` step whose
  body never ran; the run killed `UnabsorbedNodeFailure`. Green: the blip
  ESCAPES the claim tail (the machinery seat, the reclaim, the ladder
  unburned, no error record).
- **(b) the Exit no-op**: `cache=True` on an Exit-returning body refused
  at the wiring (`E19-cache-exit-body`; unions and list elements included).
- **(c) the API-reference hazards**: the `step` row + the E17–E20 rule rows.

## The proofs (at the cure head)

See `.measurements/runs/` — the re-recorded claims (lint-format,
pyright-full 0/0/0, typeprobe-gate 39×2, mkdocs-strict, wf-scoped-coverage
90.97% vs the 90.0 floor, deslop-pin-packs, wf-bands-rv4, wf-battery-rv4 ×2
at -n 8, fast-tier-n8) + the existing 11 cache pins green ×2 alongside the
cures' 18 pins (`test_wf_cache_cures_pins.py` + `test_wf_step_cache_pins.py`
= 29 passed ×2).
