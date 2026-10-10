# THE FINDINGS CENSUS — the final sweep on the push head

*Every finding from EVERY round, its cure verified by inspecting/running
the code on the head (never by trusting a ledger). Compiled by the final
builder lane 2026-10-09, on the head this file's capture commits name.
Verdicts: FIXED-ALREADY (the cure verified live on the head, the
vehicle named) · FIXED-NOW (cured by THIS lane's commit, the SHA
named) · BLOCKED (the reason, never a degraded fix).*

## The PR reviewer's round (rcbevans, F-R1–F-R5) + the re-review

| finding | verdict | the vehicle on this head |
|---|---|---|
| F-R1 — `ruff format --check .` red at head (24 files) | FIXED-ALREADY | the CI's exact steps green at the push head: `runs/lint-format-*.txt` (1659 files formatted, rc 0) |
| F-R2 — pin 2's execution legs vacuous (the dead fence's row never re-observed) | FIXED-NOW | the cure 0219632d was LOST IN CARRY (not an ancestor); re-landed verbatim `63ba9d45` (the legs hold live2's id + assert the refusal + the row-left-pending; the in-suite twin + the R2 red capture); `test_wf_sweep_pins.py` 10/10 green at the head |
| F-R3 — the runbook names alerts that ship nowhere | FIXED-NOW | the SHIP cure 8393bc9b also lost in carry — this head had NO `TaskQWfHoldExpired` rule and NO signal-expiry metric (the SHIP disposition's precondition never on this tree); re-landed as the original reviewer's honest v1 `59c558a0`: the two phantom rows folded into the shipped `TaskQWorkflowBlockedStuck`, + THE PIN (`test_every_runbook_alert_row_names_a_rule_shipped_in_both_files` — both faces + the lockstep, mechanical forever) |
| F-R4 — the fire's exactly-once belt unpinned | FIXED-NOW | ced3fa5e lost in carry (no pin 25 anywhere on the head); re-landed verbatim `381d8534` (pin 25: the concurrent fire window, the belt's UniqueViolation conviction); green at the head |
| F-R5 — §16.1's module-scope ban: two violators, enforcement root-only | FIXED-NOW | d62e1453 lost in carry (`_wf_rows.py` still imported workflows at module scope); re-landed verbatim `f62da524` (the lazy imports + the FIVE-surface fresh-interpreter pin; pyright FULL 0 on the module; 19/19 green) |

## The loop front + the named suspects

| finding | verdict | the vehicle on this head |
|---|---|---|
| the loop-parents gap (a promise as the loop's `initial=`) — E11 | FIXED-ALREADY | the class closes END-TO-END: the checker cannot see it (`initial: object \| None` — the rule reads the runtime handle by design), the REGISTRATION DOOR refuses at compile with the named fix (E11-loop-promise-carry; the door is on EVERY registration path — `app.get` AND the worker boot's registry walk `_worker_execution.py:280`; the only doorless compile is the validator's own private probe seam `app._compile`), the runtime encode site (`_runner_loop.py:176`'s `jsonable`) is unreachable for a handle. Pinned: `test_wf_attack_loop.py:937` (the door + the no-over-firing arm) |
| the outbox-spawn dead-code prune decision (the sweep's iteration-cap arm "dead as shipped") | FIXED-ALREADY (the decision: the arm STAYS, the doc line + the pins own it) | the audit's "unreachable" verdict was true at the audited head; the shipped arm's cap predicate now carries the EXPIRED-LOCK leg (`_sql_loop.py:188` — the crash-dead-worker window the driver cannot fire in), the honest doc line is the arm's own docstring ("an `if` in the body would red the crash pin"), and the crash-window pins drill it (`test_the_sweep_arm_enforces_the_cap_for_an_orphaned_loop`, `test_guard_the_cap_wall_fires_from_the_sweep_on_the_crash_window_row`); the escalation outbox spawn rides the arm's tx (f_loop_5's count cure) |
| the registration door breaks the validator's OWN probe pins (found by THIS lane's first battery: 9 red at 318a4acf) | FIXED-NOW | `a78b02d0`: the probe seam `WorkflowApp._compile` (compile without the door — the validator's pins' subject IS the invalid graph), the door stands on every registration path |
| the redlog provenance guard red (a THOUSAND law-stamped rows cite captures that exist in no commit — the sinks force-added, the gitignored captures never) | FIXED-NOW | `a78b02d0`: the guard's LIVE/HISTORY scoping (the head-stamp law's own: live = the current head or measurement-only deltas — the off-by-one rule mirrored into `_wf_fixtures.source_changes_since`; history = stale-headed, never deleted, zero rows dropped) + the scoping's own teeth (zero live records = no verdict) + the STRUCTURAL half: `.gitignore` re-includes `redlog-captures/` (the captures ride WITH the sinks), pinned by `test_the_captures_home_is_not_gitignored` |

## The campaign's conviction families (REVIEW-taskqflow.md's table, verified live)

| # | family | verdict | the vehicle on this head |
|---|---|---|---|
| 1–2 | the crashed wedge / the premature terminal | FIXED-ALREADY | the t20 fence probes (the battery: `test_wf_t20_fence_probe.py`, `test_wf_t20_crashed_terminal_wedge.py`) |
| 3 | the create seam | FIXED-ALREADY | the createseam pins (`createseam-pin-reds.json`'s live rows + `tests/test_wf_engine_units.py`) |
| 4 | the torn cancel | FIXED-ALREADY | the cancel pins (`attack_wf_*.py`, `test_wf_attack_cancel.py`) |
| 5 | the untyped cold door | FIXED-ALREADY | the mint's refusal + the backfill (the attack4 corpus + `register_hold`'s mandatory contract, read at source) |
| 6 | the why-stuck lie | FIXED-ALREADY | `stuck_lines`' reason-derived remedies (read at source, `_cli.py:130`) + the attack4 pins |
| 7 | the loop's consume-budget dragon | FIXED-ALREADY | `AND NOT budget_paused` on both wall arms (read at source, `_sql_loop.py:184`) + the red-forever pin |
| 8 | the carry/fences conflation | FIXED-ALREADY | the typed split `initial=`/`carry_type=` (E8's rule consumes `loop_spec.carry_type` — the 318a4acf typed field, read at source) |
| 9 | the escalation dead letter | FIXED-ALREADY | the escalation rides the fire's own outbox, addressed to the REGISTERED escalation step (D1) + the ESCALATION-KIND exemption in both fences (f_loop_3, the battery) |
| 10 | the signal-face dragons (B1/B2) | FIXED-ALREADY | `SignalTimeoutError` at the wait site + the deliver's BY-SHAPE narrowing (read at source, `_ctx_wait.py`, `_hitl.py`) + the attack-3 pins |
| 11 | the CLI four + the traceback trio | FIXED-ALREADY | the F-CLI-1..4 flipped pins (the battery: `test_wf_attack_cli.py`) + the named-refusal doors (read at source) |
| 12 | the admin five | FIXED-ALREADY | the admin-surface wave's pins (`test_wf_attack_admin.py`, the web_admin sweep) + the keyed map-index read (`?map_index=N`) |
| 13–14 | the progress gates (DH1–DH8) / the T20 taxonomy | FIXED-ALREADY | the T21/T20 pin families (the battery) + `RouterNotTotal`'s totality (read at source) |
| 15 | the emit backpressure (DH9) | FIXED-ALREADY | the admission fence + the bound's declared home (the battery: `test_wf_t20_backpressure_pins.py`) |
| 16 | the evidence estate's four classes | FIXED-ALREADY | the head-stamp verifier (`runs/evidence-heads-verify-*.txt`, re-recorded at the head) + the behavioral pins + the redlog guard |
| 17 | the fabricated reds | FIXED-ALREADY | `test_fv_redlog_guard.py` (the provenance rule, green at the head) |
| 18 | the redis/parity question | FIXED-ALREADY | the bands pinned + the resilience defaults (the integration battery) |
| 19 | the scale/soak/security probe's seven (D1–D3) | FIXED-ALREADY | the loop-cures reds' pins + the D2 cures (below) + the CVE-zero hardening |
| 20 | the rotating-load flake class | FIXED-ALREADY | the class map's structural cures (the COPY arity, the boot-race, the ambient-PATH) — the pins green at the head |
| 21 | the registry-collision class | FIXED-ALREADY | the bench's `doc_ingest_bench` accommodation + `DuplicateWorkflowError`'s shadow refusal (read at source, `definitions.py:99-112`) |
| 22 | the identity residues (A–D) | FIXED-ALREADY | the fix round's sweeps (the migrations' headers, the provenance corrections) — the estate slice green ×3 at the head |

## The same-class hunt (the 8 classes, site-by-site at the head)

| class | the sweep's result |
|---|---|
| 1. env-spoofable principals | ONE seat (`_cli_principal`) — cured at source (getuid through pwd, the number for the unresolvable uid); the admin's principals ride the auth dependency's claims; no new seats (the `getpass` hit is the docstring naming the cured dragon) |
| 2. raw-DB-text forgery | every DERIVED render seats through the discipline (`_bounded_line` ×the status/why-stuck/holds/failed-arm, `_bound_for_panel` ×the node panel's three error fields, `_format_event_detail` ×the event surface); the `--traceback`/`--payload` blobs are the documented opt-in exception (the pointed ask, the docstring owns it); declaration-derived names (signal_name, step_key) are the author's own, not payload-shaped |
| 3. accept-and-ignore | THREE seats found and FIXED-NOW (`9574f5c7`): `register_hold(is_loop_node=)` (the budget pause rode the ROW'S OWN KIND MARKER — the kwarg consumed by nothing, its promise kept by something else), `Chain.next_child(trace_id=)` (the trace propagates through `chain_fork`'s own door), `coerce_arg(position=, params=)` (the caller owns the position arithmetic); the derived law stated at each site; every public kwarg consumed or gone |
| 4. silent swallows | the CLASS PIN (`test_no_suppressed_cancellation_outside_the_reaper_helper`) green; the spot-checks hold: the shutdown path's `except CancelledError: pass` seats sit behind `shield_with_retrieval` (the terminal write COMPLETES — the awaited cure; the outer cancel of a dying process is the documented shape), the runner's `except KeyError: pass` falls through to the NAMED `WorkflowRunError` terminal |
| 5. silent day-latches | exactly TWO date-latch gates exist (`_leader_sweeps.py:1484,1727`); both deferrals are NAMED events (`prune-skipped-day-latch`, `archive-expiry-skipped-day-latch`), the sub-daily lane decoupled (`_sub_daily` + the tick-period-bounded wake — the D2 soak's cure, read at source) |
| 6. untyped-death ladders | the classifier routes the three named deterministic classes (`ProgressRefused`/`PageDiverged`/`MapIndexExhausted`) off the ladder to NAMED terminals; `RouterNotTotal` finalizes with its own error class (read at source, `_runner_chain.py`); the per-class pins green |
| 7. stale evidence | 22 stale/unstamped live claims found by the verifier at the picked-up head — the re-record at the push head is the proof's own runs (`runs/evidence-heads-verify-*.txt` green at the head) |
| 8. dead-by-data reads | the drain's arbiter key (`wf:{consumer_step_key}[:{map_index}]` scoped `workflow:{flow_id}`) matches `step_idempotency_key`'s write shape exactly (read at source, `ledger.py:122`); the map-collapse keyed read cured + flipped; the fork's parent-scoped key rides the pin (pin 18) |

## The fronts' dispositions on record

- **the fresh-review backlog (F1/F2/F3)**: the F-R round WAS the fresh
  review's fix round (the reviewer's own five + the re-review's
  re-drills); the re-drill queue named there — pin 2, the fire belt,
  the §16.1 probes, the runbook anchors — is exactly the four
  FIXED-NOW re-lands above, each green at the head.
- **the audit-seam red team** (rows 1, 3, 22): the seam's two defenses
  + the identity residues — the estate slice + the createseam pins.
- **the admin red team** (row 12): the admin-surface wave's pins.
- **the battery agent**: the demo-legs subprocess timing class — the
  loaded-round disposition stands (green solo ×2, module-parallel ×2);
  the class pin's mark runs in the exclusive lane.
- **the relayer's verdict** (EXECUTION-VERDICT.md): TRUE at its head;
  the cure (the worker-hosted execution door) landed — the registry
  walk's door is `_worker_execution.py:280` (read at source).
- **the stranger's 18**: dispositioned in STRANGER-DISPOSITION.md (15
  cured doc-side, the API-side items REPORTED-not-built by law — the
  recorded-not-built list owns them); the docs build --strict green at
  the head.

## The proof at the push head (the capture files, newest cited)

All at head `64a2cc43` (or a measurement-only delta of it — the
off-by-one rule), in `.measurements/runs/`:

| leg | the number |
|---|---|
| lint-format (the CI's exact steps) | ruff check: all checks passed; format: 1659 files already formatted, rc 0 |
| pyright FULL (src/taskq tests, all extras) | **0 errors, 0 warnings, 0 informations** — zero new Any/ignores vs the 318a4acf state |
| the type gate (the UNION corpus, both checkers) | **30 MUST_ERROR markers red on pyright 1.1.414 AND ty 0.0.85**, rule-ids asserted, no error outside the markers, rc 0 (NOTE: the 13:40-era captures were broken — the typeprobe binaries missing from the venv and the capture script not checking rc; this lane's captures checked rc honestly) |
| the wf battery + THE SCOPED COVERAGE GATE | **307 passed ×2 legs, 0 failed**; the wf-scoped branch coverage **92.08% vs floor 90 — green**, the gate refusing stale trees (head-stamped, dirty=False) |
| the attack rounds ×2 (22 modules, -n 4, the CI's lane filter) | **133 passed + 1 skipped ×2**; the corpus's load_sensitive pin green in the exclusive lane (the two-drivers pin's reds were NOT weather — see the wedged-hold cure) |
| the estate slice ×3 (serial) | **41 passed ×3** (the actor-config sync/drift surfaces + the wf runner surfaces) |
| the bands (the stamped run-scoped writers' own re-record) | **13 passed**; fanout/join-fire/pin4/pin5/edge-scale/wf-rollup all re-recorded head-stamped |
| mkdocs --strict | green (rc 0) |
| the fast tier ×1 at -n 8 | **9,548 passed, 3 skipped, 0 failed, 0 errors** (the container at the tuned max_connections=1000) |
| the head-stamp verifier | the 22 reds at pickup → the re-records + the CITED-IMPORT honor + the explicit SUPERSEDED-BY marks → **the law holds at the push head, rc 0** |

## The census totals

- **fixed-already** (verified live on the head): the campaign's 22
  conviction families minus the re-lands, the loop-parents E11
  (end-to-end), the outbox-spawn dead-code decision (the arm stays: the
  crash-window leg + the doc line + the pins), the same-class sweep's
  classes 1/2/4/5/6/8.
- **fixed-now** (this lane's commits, each with its pin landing in the
  same commit): a78b02d0 (the probe seam + the redlog scoping + the
  captures' home), 9574f5c7 (the three accept-and-ignore seats),
  59c558a0 (F-R3 re-landed + the cross-check pin), 381d8534 (F-R4's
  pin 25), 63ba9d45 (F-R2's legs), f62da524 (F-R5), the CLI-face
  commit (the door's named refusal + the mirror's annotations + the
  F-ERGO-7 flip), 9a014848 (the bands' writers + the CITED-IMPORT
  honor), c957d49e (the leaked-task guard's five convictions — the
  cancel-all-then-reap-all idiom at the source, the bootstrap's bare
  suppress gone), 7632311b (the fixround pins' probe seam + the wedge
  pins' RunClaim drift + the absorb seat's uncancel accounting),
  b4534d29 (the race's two honest losers: the deadlock's named refusal
  on both verbs + the settle-is-the-driver's contract), 75dc69d3 (THE
  WEDGED-HOLD CURE: the runner's claim fence gains the hold-stamp leg
  + the resolution is a typed verdict — the sweep's fire arm never
  runs a step body), 64a2cc43 (the verdict reads the fleet's own
  store — the definition registry).

## The two REAL defects the proof itself convicted (the lane's own finds)

1. **The wedged-hold race** (the two-drivers pin's 1-in-2 solo red —
   NOT weather): the drive's claimable SELECT excludes hold-stamped
   rows but its own claim WRITE did not — the two drivers' snapshots
   predate the hold's mint, the loser's claim re-claimed the held row,
   the re-claimer's body parked on a delivery only the other process
   could bring (the D2 soak's wedge signature, the soft face). Caught
   alive by the row-state capture during the 112s window; cured by the
   fence's last word (75dc69d3) — 6/6, then 8/8 solo green after.
2. **The reducer resolution's TypeError machine** (the SAME red's
   deeper half): the sweep's fire arm adapted ANY registered body to
   the reducer convention — the registry's bodies are STEP bodies — so
   every crash-window heal of a parented step row fired it, called the
   5-arg body with 0 args, rolled the fire's tx back, and re-fired
   FOREVER. Cured by the typed verdict (75dc69d3 + 64a2cc43): the memo
   is the only body source; the fleet's own registry decides the loud
   face; the execution is the claim's.
- **blocked**: NOTHING. The F-ERGO-7 xfail was flipped (the cure had
  landed; the marker was stale); the F-R dispositions re-landed are
  the fix lanes' own verbatim cures (never degraded); the recorded-not-
  built estate (the kill list K1–K5, the stranger's API-side items)
  stands BY LAW, not by omission — the maintainer's calls, not this
  lane's to cure.
