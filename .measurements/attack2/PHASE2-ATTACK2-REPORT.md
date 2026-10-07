# PHASE-2 ATTACK REPORT (round 2) — T06 / T07 / T08 / T18

Attacker: the phase-2 attack subagent, fresh eyes, on feat/taskqflow @ 49a90a1d.
All runs captured in this directory (grep THESE files, not the scroll).
Fix nothing — reds + this report are the deliverables.
Attack container (postgres:18.6 @ :5691) created for the EXPLAIN probes and
destroyed after (cleaned up).

## VERDICT: CERTIFICATION REFUSED

Four red gates stand on the shipped tree (2 engine BLOCKER-class, 1 red
pinned test, 1 inert always-on assertion). Evidence per finding below.

## Baselines (the pins' own claims, re-run by the attacker)

- baseline-pins-run1.txt: T06/T07-scenario/T18 pin files 14/14 green (under
  -n 4, concurrent with the certifier's own full-suite run).
- baseline-status-run1.txt: T08 status pins + totality + property 11/11 green.
- baseline-fanin-bound-run1.txt: the T07 bound pins 4/4 green.
- t08-costgate-run1.txt: the rollup cost-gate pin green AT ITS OWN SHAPE
  (see H3: the shape is the finding).

## REDS (attack tests, this round)

1. t06-attacks-run1.txt — test_attack_t06_crash_window_fail_closed_root_and_peers
   (H1) and test_attack_t06_mixed_policy_node_the_collect_join_hangs (H2).
2. t08-promtool-run3.txt — test_every_alert_rule_fires_on_real_names_and_labels
   (H3's red gate: TaskQWorkflowBlockedStuck references
   taskq_wf_progress_nodes_total, which the real scrapes never emit).
3. probe-pin6-endstate2.txt — g7_check executed against pin 6's own end
   state REDS ("the status cache has arrived") while pin 6 passes (H4).

Greens kept as certification evidence: maybe-does-not-cascade, cascade
racing a finalize (6 interleaves), two cascades with overlapping peer
sets, the fire→read expiry boundary, the vanilla over-hold probe, the
timeout arm racing the ladder's last retry (the fence loses cleanly).

## FINDINGS BY SEVERITY

### H1 (BLOCKER-class, T06/T08): the crash-window fail_closed run wedges the flow root at 'running' forever
- tx1 commits the child's terminal FAILURE, the cascade (tx2) dies → the
  sweep's blocked_required heal stamps the join — but NOTHING fails the
  flow root. The only root writers are the direct cascade's flow_failed
  arm and WORKFLOW_ROOT_MAINTAIN_SQL — and the maintenance leg's
  `has_live` counts the blocked join row (status 'pending', which nothing
  can ever terminalize: dispatch excludes deps>0; no decrement, no cancel
  arm). The root rows report 'running' forever: the pruner's liveness
  guard then holds the run's rows forever (unbounded retention), every
  flow-status leg sees the flow alive, and the maintenance leg re-scans
  the dead run every sweep pass.
- The rows themselves reconstruct 'failed' — the shipped G7 check (were
  it wired, see H4) reds on this shape; it is exactly pin 6's own end
  state, masked there only because the peers keep running (row 1 wins).
- Fix shape (for the fixer): the maintenance leg must read blocked-with-
  reason stamps as RESOLVED (not live), or the heal must terminalize the
  blocked row; has_live's predicate is the one-line defect.

### H2 (BLOCKER-class, T06): the mixed-policy node leaves a hanging, claimable, never-fired collect join
- Node X → joinA (fail_closed) AND joinB (collect); X terminal-fails.
  The cascade flips the flow IN THE SAME TX as the collect leg's
  decrement → DECREMENT_ABSORBED_SQL's flow_alive guard refuses → joinB
  never fires, never blocks-with-reason. The sweep's recount then
  RECONCILES joinB's counter to 0 (its failed_required is 0 — the edge
  says collect), leaving: pending, deps_pending=0, blocking_reason='join',
  fires=0 — a claimable join row that never fired, on a failed flow.
  "The record never shows a hanging join" is false for the mixed shape.
- Compounded by the rollup's `absorbed` predicate (EXISTS any absorbing
  outgoing edge): X is marked absorbed, so T08's derivation can NEVER say
  'failed' for this failed-closed run — it derives 'blocked' (reconstruct
  verified in the capture). The envelope lies about which policy ran —
  the exact lie T07's C forbids.

### H3 (HIGH, T08): the rollup's index cannot serve the rollup — and the promtool gate REDS on the shipped tree
- 01.00.25_01 builds jobs_wf_flow_nodes_idx on ((metadata->>'flow_id'),
  status) — TEXT. The shipped reads compare ((metadata->>'flow_id')::uuid
  = $1::uuid) — the uuid CAST between expression and comparison breaks
  the index match. Measured on the attacker's own container
  (explain-hot-statements-attack2.txt, rollup-fleet-explain.txt): at a
  21k-row fleet table the grouped rollup, the per-node read AND the
  gauge's fleet sampler all Seq Scan jobs; at 221k rows + ANALYZE the
  rollup reads ONE 500-node run in p50 15.2 ms, linear in the FLEET
  table, not the run. The same cast sits in WORKFLOW_ROOT_MAINTAIN_SQL's
  per_flow join and _QUERY_WF_PROGRESS_SQL_TEMPLATE's root join — the
  maintenance leg + gauge are fleet-wide scans per sweep pass/metrics
  tick. The docs ("every read is index-driven", insights.md 484; the
  pin's band argument "never the fleet table") are false at fleet shapes;
  the cost-gate pin's own shape (60k rows where the measured flow IS the
  whole table) manufactures its green.
- The pinned promtool test REDS (t08-promtool-run3.txt):
  TaskQWorkflowBlockedStuck references taskq_wf_progress_nodes_total,
  "which the real scrapes never emitted" — the observable gauge emits no
  series until the maintenance leader's sampler feeds the cache, and the
  review module's probes are worker scrapes. A red gate stands on the
  branch (§7b: this alone refuses the phase).

### H4 (HIGH, test estate): G7's "always-on" registration is INERT — the fixture never runs
- tests/conftest.py's collection hook adds
  pytest.mark.usefixtures("wf_g7_status_truth") via item.add_marker AFTER
  the items' fixture closures are computed — no wf pin file ever runs the
  check. Proven two ways: pin 6 and test_t08_reconstruction both end in
  shapes where the root row contradicts the rows (root 'running', rows
  'failed'/'cancelled') and pass their teardowns; the SAME end state run
  through g7_check directly reds (probe-pin6-endstate2.txt). The teeth
  pin passes because it calls g7_check itself — it proves the check's
  body, never the wiring. The drift H1/H2 produce is exactly the drift
  G7 exists to catch continuously.

### M1 (T18, medium): the doc's rolling-deploy tolerance for the result-expiry arm does not exist
- maintenance-sweeps.md §7: "the result-expiry arm tolerates the missing
  workflow tables the same way the workflow sweep arms do (the per-tick
  tolerance)". Code: sweep_expired_results runs _SWEEP_RESULT_TTL_SQL
  (the wf_edge guard hard-coded) via _run_single_statement_sweep with NO
  tolerance — on a pre-workflow schema the sweep ERRORS per tick (the
  hold direction holds only by accident of the failure, the named
  mechanism is fiction). Only prune_terminal_jobs got the try/except.
- Related asymmetry: the per-actor prune arm composes the same guarded
  predicate (_ARCHIVE_CANDIDATE_ACTOR_SQL) with NO tolerance — a
  pre-workflow schema with actor_overrides configured kills the leader.
  Also note: once the fallback fires, candidate_sql is reassigned
  unguarded for the REST of the call — a migration landing mid-prune
  leaves the remainder of that prune unguarded.

### M2 (T07/T08, medium): the gremlins surface was never extended for the phase-2 guards
- [tool.pytest-gremlins] targets engine.py/_sweep.py/_sql.py only. The
  new guards live in _sql_finalize.py (FAIL_CLOSED_CASCADE_SQL's block/
  peers/flow arms, DECREMENT_ABSORBED_SQL's policy filter, the fan-in
  cap's three arms), _sql_sweep.py (blocked_required), backend/_sweeps.py
  (the expiry guard), _leader_shared.py (the candidate guard). Some have
  hand-rolled drills in the pin files; the mutation GATE cannot see any
  of them. (Also recorded: t06-pin-reds.txt's RED1 is a SyntaxError from
  the drill script, not a semantic red — weak evidence where the commit
  claims a conviction.)

### L1 (low): the guide's refit numbers match no artifact
- workflows.md: "the refit reads ~0.6 µs/edge + ~2.8 ms base". T07's
  original artifact: 0.811/2.576; the recert round's re-measure (HEAD):
  1.684/2.705 (t07-refit-recompute.txt — the endpoint fit recomputes
  EXACTLY from the recorded points, the outlier is named, the
  base-dominated criterion genuinely holds: base 2.7 ms vs the 1000-edge
  marginal term ~1.7 ms). The derivation is honest; the guide's numbers
  are stale/unsourced. Cosmetic but the "re-derived from the artifact"
  claim is part of the honest-criterion discipline.

### L2 (low): the new doors' vocabularies are bare str
- FailureInfo.policy: str (default "collect") — "the envelope must not
  lie" door does not type-restrict collect|maybe (the type probes red
  everything else on both checkers: type-probe-pyright.txt,
  type-probe-ty.txt — all MUST_ERROR markers red on pyright 1.1.414 +
  ty 0.0.85). NodeView.blocking_reason: str|None likewise. The
  _resolve_failed_parent ledger walk carries 5 pyright: ignore[s] with
  Why-comments (lawful) around an Any-typed wire walk.

## THE VACUOUS AUDIT (every new pin, guard flipped in the head)

Vacuous or hollow:
1. test_t08_query_count_one_read_per_status_surface — counts the TEST'S
   OWN three calls through a passthrough; no admin surface or gauge is
   driven; flipping any implementation guard fails nothing. The "the pin
   counts the statements the surfaces issue" claim is fiction.
2. test_t08_cardinality_never_per_node_labels — feeds only (workflow,
   state) keys and asserts the emitted label set; never ATTEMPTS a
   per-node key, so "the registration must REJECT a per-node label
   series" is untested (half the pin's stated claim).
3. test_t06_pin1's in-test mutation drill — re-asserts the stamp the
   SHIPPED cascade already wrote two lines earlier; cannot fail (theater;
   the recorded reds file carries the real evidence).
4. H4's inert G7 wiring — the always-on assertion on every wf pin file.

Non-vacuous (biting; certified by trace + the greens): T06 pins 2-7, the
T07 scenario pins + the bound pins, T08's derivation/precedence matrix +
the property trio + the totality table + the reconstruction pin + the
teeth pin (body-level), T18 pins 1-4 (pin 1's live unguarded comparator
is the estate's best drill shape).

## HYGIENE

- The dirty files (pyproject.toml + 9 measurement JSONs) were the
  cancelled builder's uncommitted mid-flight work; commit 49a90a1d landed
  them at 14:52 DURING this attack (the S608 rows + the recert round's
  re-measured artifacts) — preserved and accounted, nothing lost.
- CONCURRENT SESSIONS: the certifier's full-suite run (PID 655126) is
  writing .measurements/phase2-full-verify-215209.txt in this worktree
  WHILE this attack ran, and re-wrote edge-scale-curve.json mid-attack
  (this report's numbers cite the post-write values; the fit recomputes
  consistently against both rounds). One worktree, two writers — the
  measurements' provenance during this window is not single-writer.
- Gremlins/docs: see M2 + L1. Docs content for 06/07/18 is real (not
  stubs); T08's guide lives in insights.md + observability/runbooks —
  real, but see H3 (its index claims are false).
- Cleanup: the attacker's PG container (attack2-pg @ :5691) destroyed;
  scratch schemas died with it.
