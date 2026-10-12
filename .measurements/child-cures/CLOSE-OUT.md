# THE CHILD-FLOW CURES — CLOSE-OUT (the lane's evidence, never verdict)

Branch `feat/taskqflow-child-cures` off the child line's head `f8bfc5033`.
PG :5812 (`taskq-child-cures-pg`), destroyed at close. Source-final head
**822772b410c6**; the estate commit `7cb8d52fe`; the receipt commit /
final head **ff0d900b20a7**.

## Per cure — mechanism, red, green

### CURE 1 — THE ASYNCIO.GATHER DRAGON (grand-merge blocker)
* Mechanism: the per-attempt spawn cursor's read+write was two
  statements; K concurrent `spawn_flow` calls (the natural
  `asyncio.gather` fan-out) all read the same slot, adopted the same
  run key, and the arbiter handed every caller THE ONE child's handle —
  tenant-0's verdict K times, zero errors. The cure: the slot claim is
  ONE atomic `UPDATE … RETURNING` row-CAS
  (`_SPAWN_SLOT_CLAIM_SQL`); the run-key adoption was already a CAS
  (`ON CONFLICT DO NOTHING`), the honest shared face for the same-key
  case.
* Red (`.measurements/child-cures/rv5-child-red-first.txt`, C1): 5
  handles, **1 distinct child**, verdicts
  `['found:d1-0' × 5]`, 1 child root (the law: 5).
* Green: `test_gather_spawns_mint_k_distinct_children_never_tenant_zero_k_times`
  — 5 distinct roots, each carrying its own input, receipts = the roots;
  `test_the_same_key_concurrent_spawn_adopts_one_child_all_callers_agree`;
  `test_input_fidelity_two_input_shapes_each_child_carries_its_own`
  (two input SHAPES through one gather; each child's rows carry only
  its own shape's fields).

### CURE 2 — THE RECURSION BOMB + THE RECURSIVE CASCade (grand-merge blocker)
* Mechanism: (a) no depth bound, no cycle check — a self-spawning flow
  minted a generation per sweep tick. The cure: the spawn walks the
  ancestor chain from the rows' `parent_flow_id` links capped at
  `DEFAULT_SPAWN_MAX_DEPTH` (32; `max_depth=` the author's raise), the
  cycle detection folded into the same walk; the typed
  `SpawnDepthExceededError` (`FanInBoundExceededError`'s sibling),
  deterministic on the ladder. (b) the cancel cascade was
  Python-recursive; the cure: the WORKLIST walk (`cancel_cascade` +
  `_cancel_one_run`), both surfaces (the runner method and the
  id-addressed verb) share it.
* Red: the bomb's generations grew past the drive (pre-cure
  `max_ticks` exhaustion, unbounded census); the 1200-deep cascade →
  **RecursionError at the operator's face, 216 runs left non-terminal**.
* Green: the bomb census ≤ 32 with `SpawnDepthExceededError` on the
  record; the self-cycle walk fires the same bound; the 1200-deep
  cascade cancels clean — **0 runs left non-terminal, no error**.

### CURE 3 — THE RETENTION-RESPAWN FAIL-CLOSE
* Mechanism: the purge took the child's rows AND the arbiter's row; the
  parent's resume re-minted a fresh child (silent double execution, the
  parent SUCCEEDED). The cure: the spawn's dangling-receipt read
  (`_DANGLING_RECEIPT_SQL`) raises the typed `ChildFlowLostError`
  (deterministic on the ladder) — never a respawn.
* Red: **the child body ran AGAIN after the purge** (1 fresh child
  root; the parent's face `succeeded`).
* Green: no child re-minted (census 0); the parent FAILED with
  `ChildFlowLostError` on the node row.

### CURE 4 — THE ORPHAN WINDOW
* Mechanism: the cascade walked the receipts alone (the crash-unsafe
  direction); the rows' crash-safe `parent_flow_id` was never read. The
  cure: `CHILD_FLOWS_OF_RUN_SQL_TEMPLATE` — the rows' law FIRST, the
  receipts the accelerator.
* Red: the crash-orphan child **still `running` after the parent's
  cancel** (escaped forever).
* Green: the orphan child terminal-`cancelled` by the rows' direction.

### CURE 5 — THE SMALLER FACES
* (a) dev-drives parity: the tick now runs `sweep_expired_signals` — a
  `timeout_s` wait in a dev drive fires `Expired`
  (red: `max_ticks`, the hold still `held`). Green:
  `test_a_dev_drive_runs_the_expiry_arm_timeout_s_fires_expired`.
* (b) the admin is not child-blind: `RunView.child_flows` /
  `parent_flow_id` read from the rows; the run page renders the "Flow
  family" section (both directions).
* (c) the child-join resolve: the typed refusal
  (`_child_join_refusal`) teaches where the resolution actually lives
  (the child's terminal); the hold SURVIVES
  (red: `resolve: delivered` — the bounce).

## The proof
* ruff check/format: clean (`runs/lint-format-childcures.txt`).
* pyright FULL (src+tests): 0 errors, 0 warnings (`runs/pyright-full-childcures.txt`).
* the type gate: 41 markers × pyright 1.1.414 + ty 0.0.85, all red on
  the declared rules (`runs/typeprobe-gate-childcures.txt`).
* the wf battery ×2 at -n 8: 582+1 (rv2-13 red truthfully pre-estate) ×2
  at 822772b4; **583 passed ×2 at the committed estate**
  (`child-cures/wf-battery-committed-x2.txt`).
* the existing child pins: 10 passed (unregressed); the cures' pins +
  the child pins together: **21 passed ×2**.
* the fast tier at -n 8: 9673 passed, 2 failed (rv2-13 + the redlog
  guard, both the pre-estate state; both green at the committed tree).
* the wf-scoped coverage gate: 92.72% vs floor 90 — green.
* the attack rounds ×2: 640 passed + the expected rv2-13; the estate
  slice ×3: 41 ×3; the bands: 13 passed.
* rv2-13: green on the committed tree; the verifier rc=0
  (`runs/evidence-heads-verify-childcures.txt`); mkdocs --strict rc=0.
