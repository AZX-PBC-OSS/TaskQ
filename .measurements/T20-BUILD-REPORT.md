# T20 BUILD REPORT — the streaming source + the chain, landed

**Branch**: `feat/taskqflow-t20` (rebased over the phase-3 fixer's
landing `ca3f5ba8`; no merges, no PRs). **Spike**: `/tmp/opencode/t20-proto/`
(`PROOF.md` — the PROVEN verdict this build implements). **PG**: the
T20 builder's own `taskq-t20-pg` on :5704 (the capture law: every run →
a timestamped file in `.measurements/`, this report's numbers point at
THE FILE).

## The pin map (commit → the fence it pins)

| commit | subject | the fence / the pin |
|---|---|---|
| `9a0711a5` | fix(workflows): the maintenance leg's failed arm gates on live UNRESOLVED work | THE CORE FIX (the spike's live finding, §6b): the failed arm gates on `has_unresolved` — running/crashed/abandoned OR pending/scheduled NOT blocked-stamped. `tests/test_wf_t20_maintain_liveness.py`: the 149-siblings scenario (RED on the shipped leg — captured) + the wedge-cure-stands regression (the H1 blocked-stamp exclusion is load-bearing). The H1 drill's mutation string follows the arm to its new shape. |
| `528de99f` | feat(workflows): the EMIT TX | THE ONE NEW PRIMITIVE (§6a): `WorkflowSql.{emit_children,emit_edges,emit_cursor}`; `_emit.emit_batch` (+ `EmitFencedError`, `EMIT_CURSOR_KEY`, `EmitChild`); `ctx.emit_batch` + `ctx.cursor` on the runner's StepContext. `tests/test_wf_t20_emit_pins.py`: the kill-storm at every statement window (REAL `pg_terminate_backend`, zero re-emitted / zero lost / cursor == last committed page), the between-pages kill, the zombie fence, the map_index discipline (the REFUTED-CLAIM: two rows, two keys, two ledger claims; the duplicate key aborts the tx on the unique index). |
| `e673ca11` | test(workflows): the FENCE PROBE | The SOURCE-TERMINAL + PREMATURE-TERMINAL fences (the spike's 30-sweep probe): 30 hard passes with NO worker — the root NEVER terminalizes, firable=0, zero join-wait rows; the resume completes and the root terminals ONLY after everything drained. The mutation drill (the liveness gate → `true`) reproduces the premature terminal live, then restores the honest state. |
| `f0ebddc5` | feat(workflows): the ROUTER | The typed-outcome Chain/Step/Route surface (§4's author shape): the DECLARATION-time totality refusal (naming the dropped member; the non-step route/start flips), the RUNTIME `RouterNotTotal` loud refusal (the row's `error_class`; the run derives 'failed' over a chain that died mid-route), the route through the certified fork (one child, no join; map_index + trace forward), `chain_source()` (the declaration-time key-collision refusal), the chain bodies under their own keys (D1). |
| `8fc300a8` | docs(workflows): §10 | THE MIGRATION PATH: the ad-hoc actor child-dispatch → the emit + the fork (pattern-by-pattern table) + THE LINEAGE LAW (flow_id/trace_id/map_index/parent_id on every row; the migration buys the lineage + the completion semantics, pays the declared-edges discipline; a child outside the fork is a foreign row no derivation counts). |
| `9171d373` | docs: the §10 fence tagged no-exec | The docs-example convention (the fragment references the author's own bodies). |
| `09af0ebb` | perf(workflows): the T20 bands | The bands re-measured ON THE BUILT CODE (below). |

## The reds (observed, captured — BUILD-PROTOCOL §2)

1. **The 149-stranded-chains red** — the shipped failed arm flipped the
   root 'failed' with 149 pending siblings live: the dispatch fence then
   strands them forever. File: `.measurements/t20-maintain-liveness-red-*.txt`
   (the assertion's observed value: `root_status_with_149_live_siblings:
   'failed'`). The convicted variants also land in `.measurements/t20-pin-reds.json`.
2. **The emit-tx missing surface red** — the pins' collection ImportError
   against the pre-emit tree: `.measurements/t20-emit-red-*.txt`.
3. **The premature-terminal mutation red** — the liveness gate mutated
   out finalizes the root 'succeeded' mid-stream (4 pending chains under
   it), observed live in the drill pin.
4. **The router's two-door reds** — the declaration refusal's
   NOT-REFUSED assertion, the runtime door's NOT-REFUSED assertion, and
   the runner-level silent-drop conviction (the row must NAME
   'RouterNotTotal'), all in the router pins' redlog.

## The numbers (the built code; `.measurements/t20-streaming-bands.json`)

| workload | p50 | p95 | max |
|---|---|---|---|
| emit tx per page (40 starts + edges + cursor, ONE tx) | 3.17 ms | 10.36 ms | 10.36 ms |
| dispatch band @ 200-chain backlog | 7.50 ms | 9.09 ms | 19.48 ms |
| dispatch band @ 200-plain (baseline) | 7.36 ms | 9.08 ms | 9.48 ms |
| dispatch band @ 800-row mixed | 7.23 ms | 8.53 ms | 9.70 ms |
| chain-step fork tx (the routed finalize) | 0.83 ms | 1.40 ms | 2.54 ms |

Band ratio (chain/plain) at p50: **1.02×** (the spike: 1.19×). The
emit-tx band 7–14 ms/page: held with headroom. Evidence:
`perf-evidence-workflows-streaming.md`.

## The gates

- `ruff check` + `ruff format --check` on the touched tree: **clean**
  (the repo's pre-existing lint debt — `tests/test_wf_hitl_pins.py`,
  `tests/test_wf_loop_pins.py`, the `.measurements/**` probe scripts —
  is another lane's, unchanged; see the unspecifications).
- `pyright src/taskq` + the T20 pins + the script: **0 errors**
  (`.measurements/t20-*-gate-*.txt`).
- The full estate slice + the attack tests: **172 passed**
  (`.measurements/t20-full-battery-*.txt`).
- The T20 suite × 3 consecutive clean rounds: **15 passed × 3**
  (`.measurements/t20-stability-3rounds-*.txt`).
- The import law (AST harness): green (inside
  `tests/test_wf_schema_migration.py`'s run, in the battery).

## The unspecifications

1. **The emit backpressure (DH9, the ticket's decision 7) is NOT in this
   build** — and was not in the spike's enumerated engine changes: the
   PROOF's §8 list owns the three landed items; the max-in-flight bound
   + the pager's block is a follow-up (the emitted-children count vs the
   declared bound needs a decision about WHERE the bound is declared —
   the chain? the run? the actor's `max_pending`? — which no spike
   evidence pins). The band evidence bounds the exposure (the emit tx
   emits per page, bounded by the pager's own page size) — the dragon's
   fence is owed its own ticket if the operator's scale demands it.
2. **The `maybe` policy's AbsorbingPolicy vocabulary vs the chain's
   fail_closed starts**: the emit's edges are `fail_closed` by design (a
   mid-stream source death is the honest fail-closed parent). A streaming
   shape with an ABSORBING source-parent policy has no evidence — not
   built, not promised.
3. **The runner's claim writes `claim_epoch = 0`** (the shipped
   `_NODE_CLAIM_SQL_TEMPLATE`'s constant) — the emit fence consumes it
   as-is. A future multi-claim epoch discipline on the runner side would
   flow through the emit fence unchanged (it reads the row's fence
   columns), but the interaction is unpinned.
4. **The chain surface's `actor`/`queue` are Chain-level, not
   step-level** — the spike's shape; a per-step queue override is
   unpinned ergonomics, deferred.
5. **The pre-existing lint debt**: `make lint` on the base branch is NOT
   clean (49 + others across `tests/test_wf_hitl_pins.py`,
   `test_wf_loop_pins.py`, and the `.measurements/**` captured probe
   scripts). Unchanged here — the captured corpora are append-only and
   the hitl/loop pins are the phase-3 fixer's lane.
