# THE WORK LEDGER

Every known gap, defect, and demand — tracked, ranked, and worked to done.
Nothing on this ledger may be closed without a red-proven pin and a green
run. Nothing may be removed; closed items move to the DONE section with
their receipts.

Legend: `[ ]` open · `[~]` in flight (branch/PR named) · `[x]` done (receipt) · IN BUILD — a distinct state on an open item: a fresh builder agent is working it on a local branch (not yet pushed, not yet reviewed); the marker stays `[ ]` until the branch or PR is named (`[~]`).

---

## A. THE CONSUMERS' GAP MATRIX (who counts, what blocks them)

| consumer | counts | blocked on | status |
|---|---|---|---|
| TAStack | yes — deepest (36+ actors, reservations, tiers) | capacity + retention LANDED (proven by their own re-review's live run). STILL BLOCKING: `priority` on wf enqueues (a shipped-surface regression); the boot refusal without `TASKQ_QUEUES_STRICT` (subset-subscribing workgroups can't set it); code-owned capacity sync (the damper); `Snooze` passthrough; `start_to_close`; wf otel spans; `max_pending` (on trial as C16 — build-or-prove). THEIR VERDICT: hold the pin at 66b8f369 until these land. | [~] §B1, §B2; blockers IN BUILD §C |
| cennan | yes — 30 actors, 50k-deep backlogs | capacity fields (§B1); retention (§B2); priority; fairness_key; start_to_close; retry hooks; transactional run creation | [~] §B1, §B2; rest §C |
| warden | yes — hand-rolled non-durable DAG engine (pipeline/) | capacity fields; retention; result_ttl; terminal hooks on steps; cron-triggered flows | [~] §B1, §B2; rest §C |
| OperationsDashboard | moderate — sync actors + 11 crons | nothing hard; SSE "holds" topic budget note; the unique_for/hooks family if flows land | [x] doc note §B3 |
| aacrtool | yes — 8-stage DAG, keyed rate limits | the 961-commit pin jump FIRST; then keyed `rate_limits` on steps | [ ] §C |
| assay + 5 XP forks | mid — campaign rungs, fan-in | nothing blocking; `max_concurrent` on steps when converting | [~] §B1 |
| event-intel | mid — sweep join is flow-shaped | nothing blocking; `max_concurrent` on steps | [~] §B1 |
| SustainabilityAI | no — Prefect/dramatiq own orchestration | n/a | [x] not-a-candidate |

## B. THE CURE — branch `feat/wf-retention-and-actor-caps` — MERGED as #693 (`f71763df1`, in origin/main). The `[~]` markers below scope ONLY the C15/C16 follow-on gaps named in §C; the surfaces themselves landed.

- [~] B1. The capacity surface: `@app.actor()`/`step()` gain
      `max_concurrent`, `reservations=[ConcurrencyReservation]`,
      `result_ttl`; the handle's terms flow into wired nodes (the explicit
      step param overrides); the projection carries them into `actor_config`
      (the seed-only law; the operator owns the row after —
      `ActorsClient.set_capacity`); the one-cap-per-actor law
      (`WorkflowActorCapacityConflictError`); the reservations reach the
      bootstrap's ensure-slots pass; the engine's claim arithmetic needed
      ZERO changes (it already enforces per-actor caps on wf rows).
      CORRECTED (the consumer's re-review): this block listed
      `max_pending` among the gained params — FALSE, it never shipped;
      the assertion behind its absence is on trial as C16. The
      seed-only law above stands as a GAP, not a decision: the
      code-owned sync is C15, in build.
- [~] B2. The retention arms: five bounded-batch sweep arms
      (`wf_edge`, `wf_step_ledger`, `wf_join_fire`, `wf_signals`,
      `wf_node_progress`), flow-terminal-scoped, one batch per pass,
      five `TASKQ_WORKFLOW_*_RETENTION_PERIOD` settings, default OFF
      (zero-means-off — no surprise deletes on upgrade), one index
      migration for the arms' scan targets.
- [~] B3. The docs: the capacity surface, the retention knobs, and the
      two consumer traps — the SSE "holds" topic budget arithmetic
      (OperationsDashboard's under-count) and the queue-default strand
      trap.
- [~] B4. The queue-default trap's loud door: the projected wf cohorts'
      queues ride the boot-time consumed-queues assertion — an
      unconsumed queue fails the boot loudly ONLY under
      `TASKQ_QUEUES_STRICT`; the default arm is a per-child warning.
      CORRECTED (the consumer's re-review caught the overclaim):
      subset-subscribing fleets structurally cannot set the env var, so
      the loud door is closed to exactly the fleets that need it — the
      app-declared face is C14, in build.

## C. THE NEXT TIER (the consumers' ranked needs, sequenced after B)

THE WORKING RANK (the consumer re-review's order — build top-down; no
 omission lands without a red-proven impossibility pin behind it):
C1 → C14 → C15 → C16 → C4 → C3 → C10 → C13(the click) → the rest.

- [ ] C14. The queue-trap's app-declared strict face: `strict_queues`
      declared on the App (code, not env) so subset-subscribing fleets —
      workgroup children each consuming a cohort subset — get the boot
      REFUSAL without fleet-wide `TASKQ_QUEUES_STRICT`. The env stays
      (strict if either is set); the remedy text names both faces.
      IN BUILD.
- [ ] C15. Opt-in code-owned capacity sync: `sync_actor_config`'s UPSERT
      omits the capacity columns from `SET` (the operator owns the row
      after first boot) — so a flow actor's `max_concurrent` change
      deploys NOTHING after the seed. Add the per-actor opt-in: for
      code-owned actors the UPSERT carries the capacity columns; the
      default unchanged; the reservation face unaffected (it never
      rides `actor_config`). IN BUILD.
- [ ] C16. `max_pending` on the wf surface — BUILD OR PROVE. The H-A6
      verdict (the engine's inserts bypass the enqueue admission; a
      claim-side gate wedges) is an ASSERTION until it is a pin. Either
      build the claim-side throttle (pending per cohort >= max → the
      claim delays; a fan-out-wider-than-max run must complete —
      pinned) or produce the impossibility pin (a minimal repro where
      any claim-side gate deadlocks a join). No adjectives. IN BUILD.
- [ ] C10. The workflows package emits ZERO otel telemetry (no spans, no
      metrics — the vanilla actors' observability doesn't extend to the
      runs/steps; the consumers run otel and their flows would be
      invisible). Hypothesized in the meta-audit, confirmed by grep.
- [ ] C11. No run-level deadline — a hung run lives forever (the step
      ladder bounds attempts; nothing bounds the run's lifetime).
- [ ] C12. The CLI's flow surface lacks a retry verb (the UI carries
      rerun; the operator's CLI doesn't).
- [ ] C13. The 0.3.0 cut: the release PR (#36) waits — the consumers pin
      revs because there is no tag to pin. The click is the release.
- [ ] C1. `priority` on wf run/step enqueues (cennan's interactive-vs-backlog;
      TAStack's BACKLOG stamping). RANK 1 — the re-review calls it a
      regression on a shipped surface: 23 `priority=` matches downstream
      (17 in src, the rest harness/vendored), and a
      flow step lands at the column default, so an interactive step
      cannot jump a 100k-deep sweep backlog. IN BUILD.
- [ ] C2. `fairness_key` on wf enqueues (cennan's multi-tenant round_robin —
      without it a workflow is a starvation vector on shared queues).
- [ ] C3. `start_to_close` per-actor backstop on the wf surface.
- [ ] C4. The retry-hooks tier: `retry_classifier`, `non_retryable_exceptions`,
      `Snooze` passthrough — or the explicit vanilla-actor interop door.
- [ ] C5. Terminal hooks on steps (`on_success`/`on_retry_exhausted`) —
      warden's exactly-once settlement discipline.
- [ ] C6. `unique_for`/`identity_key` on steps (OperationsDashboard's
      double-click protection).
- [ ] C7. Transactional run creation (`connection=`) — cennan's run-row +
      fan-out atomicity.
- [ ] C8. Cron-triggered flows (a schedule face) — warden's ticks.
- [ ] C9. RouteArm-level capacity declarations (the arms' overridden cohorts
      project NULL today).

## D. THE ADMIN UI — FULL TASKQFLOW SUPPORT

- [~] D1. Viewing runs: the workflow detail page (exists — the SSE live
      machinery, the node panel, the resolve form).
- [ ] D2. RERUNNING: the run-level and node-level rerun affordances riding
      the engine's own retry verbs, CSRF + confirm + the audit row.
- [ ] D3. The GRAPH VIEW: the compiled flow's DAG visualized on the run
      page with the live node statuses.
- [ ] D4. The NODE I/O: each node's declared inputs and its captured
      outputs visible per run (the capture/redact policies applying).

## E. THE PROVING GROUND

- [x] E1. The example app: examples/flows_demo — the run_demo orchestrator
      (migrate → seed → drive → HOLD → resolve → terminal → the retention
      arms live), the webapp sidecar (the admin + the Resolve form), the
      README's observe-list, and the smoke test (5 pins, green; the
      projection asserted directly). Independently convicted the two
      capacity landmines mid-flight.
- [~] E2. The full test coverage of every new surface — the capacity pins
      (19) + the retention pins (5) + the demo smoke (5) green; the UI
      routes' pins land with the UI agent.

## F. THE RED-TEAM ROUNDS — ROUND 1 COMPLETE, CONVICTIONS IN §H

- [x] F1. The capacity wiring: reviewed (14 repros). 2 CRITICAL + 5 HIGH
      + 4 MEDIUM convicted — see §H.
- [x] F2. The retention arms: reviewed (live PG repros at production
      scale). 1 CRITICAL + 2 HIGH + 3 MEDIUM convicted — see §H. NOTE:
      the orphan-root defect (the meta-audit's conviction) landed FIXED
      mid-round — the two-leg predicate across all five arms, the pin
      red-proven; the round then verified the fix live and convicted
      what follows in §H-B.
- [x] F3. The UI: reviewed (live execution against the migrated schema).
      3 HIGH + 5 MEDIUM convicted — see §H.
- [x] F4. The performance: verified — the dispatch SQL untouched (zero
      diff on src/taskq/backend/), the boot bounded (the compile
      doubled: H-D1), the arms' delete work batch-bounded BUT the
      zero-doomed tick is O(N) per table per tick (H-D2), the claim's
      capped path vanilla-bounded, the run page's payload bounded.

## H. ROUND-1 CONVICTIONS — THE FIX QUEUE (each fixed by a fresh agent, re-reviewed by another)

### H-A. The capacity wiring — ALL FIXED (receipts = the pins; the fix
### agent's full per-item report is in the round-1 fix wave's session log)
- [x] H-A1 CRITICAL: wf reservations are collected, registered, slotted —
      and NEVER ACQUIRED. The flow gate's acquire set = rate_limits +
      queue-cap only; the rows never carry reservations; the strict cap
      enforces nothing while the docs direct users to it. FIX: stamp the
      step's reservations onto the flow rows (or resolve from the
      projected cohort at claim time) and acquire through the vanilla
      composition (denial -> mark_snoozed, budget-free) before the body.
      DONE: the rows carry the reservation NAMES beside ``rate_limits``
      (``_row_metadata(reservations=…)`` — the static node inserts, the
      map's forked children inherit the source node's terms, the route
      arms' own declaration rides its rows; ``workflows/_types.py``,
      ``_fork.py``, ``api/_runner.py``); the flow gate reads
      ``metadata.reservations`` with the rate_limits' own five-point ride
      — the same jsonb-boundary walk, the same fail-closed arms (unknown
      name / malformed metadata → the row parks budget-free, LOUD), the
      same acquire composition (the denial-retry included), the release
      riding the flow-gate release path; the denial's row stamp names the
      reservation (``_flow_denial_snooze_metadata`` — released_bucket).
      RECEIPTS: ``test_the_reservation_stamp_rides_the_static_and_fork_rows``,
      ``test_two_flow_rows_sharing_a_reservation_run_one_body_at_a_time``
      (the slots=1 reservation: two concurrent claims → one body at a
      time, the denial → the named snooze → the re-claim after the
      release; the demo's overlap-guard tripwire silent),
      ``test_an_unknown_reservation_name_parks_fail_closed``,
      ``test_malformed_reservation_metadata_parks_fail_closed``
      (tests/test_wf_capacity_enforcement_pins.py).
- [x] H-A2 CRITICAL: one reservation shared vanilla+wf crashes the boot
      on non-default schemas (the vanilla pass registers the raw
      declaration, the wf pass the rebind; _same_config compares
      schema). FIX: ONE rebind doctrine — the vanilla pass rebinds too.
      DONE: ``worker/_bootstrap.py``'s collection pass registers
      ``res_entry.rebind_schema(settings.schema_name)`` — the same
      rebind at the same phase. RECEIPT:
      ``test_shared_reservation_boots_clean_on_a_non_default_schema``
      (tests/test_wf_capacity_surface.py).
- [x] H-A3 HIGH: the one-cap law compares ConcurrencyReservation
      INSTANCES (no __eq__) — equal configs = false conflicts
      (skip-loudly within a workflow; boot-refusal across apps). FIX:
      compare only the row-carried fields; the reservation identity is
      the collect's name-keyed law. DONE: ``_CapacityTerms`` carries the
      ROW-CARRIED fields only (max_concurrent, result_ttl — H-A6 took
      max_pending off the surface); both refusal sites name the
      DIFFERING FIELDS (``_capacity_conflict_fields``). RECEIPTS:
      ``test_equal_terms_different_reservation_instances_do_not_conflict``,
      ``test_differing_row_fields_conflict_names_the_fields``.
- [x] H-A4 HIGH: route arms / map items / chains silently DROP a capped
      handle's cap — the guard fails open by rename, no refusal, no
      warning. FIX: the arms carry their own capacity fields, and a
      capped handle whose terms would be dropped by an actor override
      is REFUSED at the wiring. DONE: ``RouteArm`` gains the
      max_concurrent/reservations/result_ttl passthrough (the arm's own
      declaration rides its cohort's projection + its rows' stamps + the
      collect); the wiring refuses the uncovered override
      (``_uncovered_handle_terms`` — route arms; ``chain_source`` for
      the chain's own placement, which has no passthrough); the
      same-actor map body's terms FLOW into the node's unset slots
      (never dropped, never refused). RECEIPTS:
      ``test_route_arm_actor_override_refuses_a_capped_body``,
      ``test_route_arm_declares_its_own_terms``,
      ``test_map_queue_only_override_is_not_refused``,
      ``test_chain_step_capped_body_refused``.
- [x] H-A5 HIGH: result_ttl is a dead knob (wf rows never get
      result_expires_at) AND skips validation. FIX: the wf finalize
      writes it from the cohort's setting, or the knob comes OFF the
      surface — never a dead knob. DONE: (a) — the finalize's
      ``TERMINAL_MARK_SQL`` stamps ``result_expires_at`` on the SUCCEEDED
      terminal rows from the row's OWN cohort's stored
      ``actor_config.result_ttl`` (the vanilla mark_succeeded's lateral
      equivalent, priced from clock_timestamp()); a FAILED terminal
      stamps no expiry (the vanilla mark_failed's parity); a cohort
      with no TTL writes NULL. RECEIPT:
      ``test_terminal_node_writes_result_expires_at_from_the_cohort_ttl``
      (tests/test_wf_capacity_enforcement_pins.py).
- [x] H-A6 HIGH: max_pending is a dead knob (the claim SQL never reads
      it; the engine's inserts bypass the enqueue admission). FIX: wire
      the enforcement or remove the declaration — never a dead knob.
      DONE: (a) — REMOVED from the wf surface (the step() param, the
      decorator's param, NodeDecl, the handle, the carrier, the terms
      law; the projection seeds the estate's column UNSET). The claim
      side CANNOT enforce a backlog bound (claiming is the only drain —
      a claim-side gate would wedge the backlog forever), and the
      fork/emit inserts cannot ride the enqueue admission mid-finalize:
      removal is the honest scope. RECEIPT:
      ``test_max_pending_is_off_the_wf_surface``. NB: the docs
      (docs/guides/workflows.md's capacity table's ``max_pending`` row +
      the validation note) still promise the knob — the DOCS OWNERS must
      strike it (the fix agent's territory excludes docs/**).
      CORRECTION (2026-10-30): the impossibility rationale above is an
      ASSERTION, not a pin — on trial as C16 (build-or-prove). This
      entry's DONE marking covers the surface removal, not the
      impossibility proof.
- [x] H-A7 HIGH: the within-workflow capacity conflict SKIPS the
      workflow — its rows strand forever and the queue-trap door goes
      blind (the severity inversion: the common shape is the silent
      one). FIX: a capacity conflict is NEVER skippable — refuse the
      boot like the across-apps arm. DONE: the projection re-raises
      ``WorkflowActorCapacityConflictError`` outside the isolation arm;
      the OTHER failure classes (a raising build fn, a validation
      refusal, the queue conflict) keep the skip-loudly arm. RECEIPTS:
      ``test_within_workflow_capacity_conflict_refuses_the_boot``,
      ``test_a_raising_build_fn_still_skips_loudly``.
- [x] H-A8 MEDIUM: the queue-consumed refusal is unconditional — the
      heterogeneous fleet (per-queue worker specializations importing
      the shared apps module) cannot boot. FIX: gate on queues_strict or
      scope the refusal to the worker's OWN cohorts. DONE: the gate
      reads ``settings.queues_strict`` (the vanilla twin's own opt-in
      law): NON-strict → the aggregated
      ``workflow-cohorts-on-unconsumed-queues`` warning (the remedy
      named) and the boot proceeds; strict → the refusal, its message
      naming the TASKQ_QUEUES_STRICT gate. RECEIPTS:
      ``test_non_strict_foreign_cohort_warns_not_refuses``,
      ``test_unconsumed_workflow_queue_refuses_the_boot`` (now strict).
- [x] H-A9 MEDIUM: the collect's dedup compares schema (rebound away)
      but ignores keyed/lock_lease. FIX: dedup on the identity that
      survives the rebind. DONE: the collect's equality is
      ``(name, slots, lease, keyed, lock_lease)`` — schema EXCLUDED
      (the rebind owns it); the ValueError names the differing fields.
      RECEIPTS: ``test_collect_dedup_ignores_schema_but_heeds_keyed_and_lock_lease``,
      ``test_collect_dedup_refuses_differing_keyed_and_lock_lease``,
      ``test_collect_dedup_refuses_differing_lock_lease``.
- [x] H-A10 MEDIUM: KeyedReservationRef in reservations= is silently
      dropped. FIX: the loud door at the wiring (rate_limits' own
      pattern). DONE: ``reservation_names()`` (the rate_limit_names
      twin) refuses every non-ConcurrencyReservation/non-str entry at
      step() AND at the ``@app.actor`` declaration site. RECEIPTS:
      ``test_keyed_reservation_ref_refused_at_the_wiring``,
      ``test_keyed_reservation_ref_refused_at_the_decorator``.
- [x] H-A11 MEDIUM (PRE-EXISTING on main): the loop escalation's cohort
      is unprojected — the escalation rows park forever under the
      DEFAULT on_exhausted policy. FIX: the projection walks the
      escalation cohorts. DONE: ``_workflow_cohorts`` adds the
      registered definition's own (actor, queue) when an
      ``on_exhausted="escalate"`` loop registers its escalation body.
      RECEIPT: ``test_escalation_cohort_projects``.
- [x] H-A12 NIT: actor_config() is a dead carrier (tests only) with a
      divergent name scheme. FIX: align it to the real projection's
      name scheme (or absorb it into the projection). DONE: the carrier
      speaks the projection's ONE vocabulary — the ActorConfig field
      set (actor by the handle's declared name, queue, the terms, the
      ``workflow_actor`` metadata mark), the workflow_name parameter
      and the ActorRef-shaped max_attempts/retry_kind fields GONE.
      RECEIPT: ``test_actor_carrier_matches_the_projection_vocabulary``.

#### H-A ROUND-2 — the red team's re-convictions (each fixed pin-first:
#### the pin failed against the convicted tree, then the fix, then green)
- [x] C1 CRITICAL: every FAIL-CLOSED park DISOWNED the row instead of
      snoozing — ``worker/run.py``'s ``assert _flow_gate.denied is not
      None`` fired AssertionError inside the caller's broad except →
      ``_disown_job`` → the heartbeat excluded the row → the reclaim
      handed it back → the re-claim re-denied: the infinite churn at
      lease cadence, the denial stamps never landing (all five
      fail-closed arms return ``denied=None`` + ``dependency_failure``
      set). FIX: the assert is GONE; the metadata helper takes
      ``denied | None`` + the gate's new ``parked_kind`` (the arm's own
      name) and stamps BOTH arms — a real denial keeps
      ``flow-rate-limit-denied`` + ``released_bucket``; a fail-closed
      park stamps ``{"released_reason": "flow-rate-limit-parked",
      "parked": <the arm's kind>}`` (the kinds: the metadata-malformed
      pair, the unknown-bucket pair, the store outage) — and the snooze
      runs for both. DONE: ``worker/run.py`` (``_FlowSlotGate.parked_kind``,
      ``_flow_denial_snooze_metadata``, the caller's arm split re-placed
      the denial-first with no assert). RECEIPTS:
      ``test_a_renamed_reservation_park_snoozes_and_stamps_never_disowns``
      (RED pre-fix: THE CHURN — the row disowned, no snooze),
      ``test_a_malformed_metadata_park_snoozes_and_stamps_never_disowns``
      (tests/test_wf_capacity_enforcement_pins.py — both drive
      ``di_consumer_loop``'s own intercept with a recording backend).
- [x] C2 HIGH: the docs' blessed explicit-registry pattern (construct a
      ``RateLimitRegistry``, register the RAW import-time reservation,
      pass ``rate_limit_registry=`` to ``worker_main``) CRASHED the boot
      on a non-default schema: the wf collect's rebind registered
      ``schema=settings.schema_name`` against the pre-registered raw
      instance → ``_same_config`` schema mismatch → ValueError. FIX: ONE
      doctrine — the rebind at the REGISTER step: BOTH passes (the
      vanilla ~:1745 and the wf ~:2175) register the settings-bound
      copy, and the registry's register step REBINDS a pre-registered
      raw instance to the incoming copy at whichever pass encounters it
      (``_same_config`` now compares identical SCHEMAS — C3). DONE:
      ``ratelimit/registry.py`` (``_register_reservation_unchecked``'s
      schema-rebind arm + ``registry-reservation-schema-rebound``),
      ``worker/_bootstrap.py`` (the doctrine comment). RECEIPT:
      ``test_the_explicit_registry_door_boots_clean_on_the_docs_pattern``
      (tests/test_wf_capacity_surface.py — the docs' exact pattern, both
      passes, ``schema=acme``: clean boot, the resolved entry
      settings-bound, the raw declaration never mutated). DOC-DRIFT
      REPORTED (docs/** is out of the fix territory):
      ``docs/guides/rate-limiting.md:1374``'s
      ``schema=_tq_schema,  # Must match TASKQ_SCHEMA_NAME`` — the
      declaration's schema param does NOT need to match (the
      registration rebind owns the live binding; it matters only for
      registry-less direct ``acquire()`` use).
- [x] C3 MEDIUM: the registry's ``_same_config`` ignored
      ``lock_lease``/``keyed`` (and compared the rebind-owned schema):
      two registrations differing only in ``lock_lease`` were the
      idempotent no-op — the second config silently lost. FIX: the
      duplicate-name law compares ``(name, slots, lease, keyed,
      lock_lease)`` — schema EXCLUDED (the rebind owns it); a true
      config difference raises the ValueError with the differing
      FIELDS NAMED (``_reservation_config_diff`` — the address-only
      ``__slots__`` repr was never a diagnosis); an identical
      re-register stays idempotent. RECEIPT:
      ``test_the_registry_duplicate_name_law_heeds_lock_lease``
      (tests/test_wf_capacity_surface.py — RED pre-fix: no refusal).
- [x] C4 MEDIUM: the plain-``str`` reservations had NO boot warning
      (the rate-limits collect warns its unknowns, the reservations'
      collect ignored its own). FIX:
      ``collect_workflow_reservations`` gathers plain-str names beside
      the instances (the tuple return ``(reservations, unknown_names)``
      — the rate-limits collect's own shape), and the bootstrap warns
      ``workflow-reservation-name-unregistered`` (the rate-limit twin's
      warning shape: the names + the fail-closed remedy) for str names
      NO registry entry backs (a vanilla actor's registration
      included). The row still stamps the name — the claim-time
      fail-closed park remains the enforcer. DONE:
      ``workflows/_worker_execution.py`` (the collect's second slot),
      ``worker/_bootstrap.py`` (``_warn_unregistered_workflow_reservations``).
      RECEIPTS: ``test_the_collect_gathers_plain_str_reservation_names``,
      ``test_the_bootstrap_warns_unbacked_workflow_reservation_names``
      (tests/test_wf_capacity_surface.py — RED pre-fix: the str name
      silently dropped).
- [x] C5 MEDIUM: the route-arm inheritance ASYMMETRY — the bare arm's
      rows inherited the source's ``reservations`` but NOT its
      ``rate_limits`` (``api/_runner.py``'s ``_route_fork``): the
      source's rate caps silently dropped at the stamp. FIX:
      SYMMETRIZED — the bare arm (no own rate_limits, no actor
      override) inherits the source's ``rate_limits`` alongside the
      reservations; an arm declaring its own keeps its OWN (no silent
      merge). RECEIPT:
      ``test_the_route_arm_stamp_symmetry_inherits_the_source_terms``
      (tests/test_wf_capacity_enforcement_pins.py — a real route run;
      RED pre-fix: the bare arm's child carried no ``rate_limits``).
- [x] C6 MEDIUM: the escalation cohort was a DEAD KNOB —
      ``WorkflowDef.actor``/``queue`` had defaults nothing could set;
      the escalation rows landed on ("workflow", "default") uncapped,
      undocumented. FIX: the ``@app.workflow`` decorator gains optional
      ``actor=``/``queue=`` (the placement face for the definition's
      OWN rows — the escalation consumer rows via D1 AND the loop
      node's fallback placement), guarded at the declaration site
      (non-empty str or None), joining the SAME one-queue law the node
      cohorts keep (the projection's conflict conviction), defaulted to
      the CURRENT values ("workflow"/"default"; the loop's fallback
      stays ("wf", "default") — no behavior change for non-adopters);
      the projection reads the WorkflowDef's placement. DONE:
      ``api/_app.py`` (the decorator + the compile's
      ``def_actor``/``def_queue``), ``api/_graph.py`` (the recorder
      carries the declared placement to the wiring verbs),
      ``api/_loop.py`` (the loop's fallback placement),
      ``definitions.py`` (the fields already there — now reachable).
      RECEIPTS: ``test_the_workflow_placement_declares_the_escalation_cohort``
      (the ("wf-x", "heavy") cohort projects, the definition + the
      loop's rows + the escalation bindings all stamp it),
      ``test_the_workflow_placement_defaults_stay``,
      ``test_the_workflow_placement_joins_the_one_queue_law`` (the
      split placement skips the workflow loudly, both queues named),
      ``test_the_workflow_placement_refuses_non_string_values``
      (tests/test_wf_capacity_surface.py — RED pre-fix: the
      decorator refused the kwargs themselves).

### H-B. The retention arms
- [x] H-B1 CRITICAL: the zero-doomed steady-state pass is a full-table
      walk with 2 jobs-probes per row — at 1-3M rows it exceeds the 5s
      command_timeout and the arms PERMANENTLY stop pruning at exactly
      the scale they exist for (proven: TimeoutError, forever, plus a
      7M-buffer walk every 30s and the serial leader starved). FIX: the
      flows-side doomed selection — select the terminal-aged roots (a
      tiny jobs-side set via jobs_finished_at_idx) and delete each
      flow's rows via the flow_id indexes; the rootless leg rides a
      slower periodic pass.
      DONE (the two-pass law): the flows-side pass selects the
      TERMINAL-AGED ROOTS first (`_sql_sweep.py` EDGE/JOIN_FIRE/
      STEP_LEDGER/SIGNALS/NODE_PROGRESS_RETENTION_SQL — `step_key=
      '__flow__'` + terminal + `finished_at <= statement_timestamp() -
      $2` via jobs_finished_at_idx, LIMIT $1) and deletes each selected
      flow's rows through the flow_id indexes, row-LIMITed to the batch;
      the ROOTLESS leg is five separate batch-bounded PK-ordered walk
      statements (`*_ROOTLESS_RETENTION_SQL` — the LIMIT bounds the SCAN)
      on a coarser cadence (same spec alternating: the leader wiring runs
      each arm's rootless pass every 8th call — `_WF_ROOTLESS_PASS_EVERY`
      in `_leader_sweeps.py`). PINS: the original 6 stay green (the
      orphan pin's semantics preserved — `_run_arms` drives both passes);
      NEW `test_scale_pin_loaded_pass_drain_and_zero_doomed_steady_state`
      (20k edge rows across 400 aged flows + 20 live controls: the loaded
      pass ≤ batch and < dispatcher_command_timeout, the drain empties
      the aged flows, the live rows ALL survive, the zero-doomed
      flows-side pass, the rootless walk, and the full five-arm battery
      all < the timeout). MEASURED (EXPLAIN ANALYZE, 50k seed —
      .measurements/runs/wf-retention-doomed-selection-hb1-*.json): the
      loaded pass 21.4ms (the doomed selection is a BitmapOr over
      jobs_finished_at_idx → 1000 roots; the delete rides wf_edge_flow_idx
      + the PK; ModifyTable rows=10000=batch), the zero-doomed steady pass
      3.4ms (selection O(1000 roots), wf-side probes find 0), the rootless
      walk 4.3ms (Limit→Index Scan wf_edge_pkey — the batch-bounded scan;
      with debris it deletes the 500 orphan rows), node_progress 7.6ms
      (jobs_wf_flow_nodes_idx → the PK probe). The floor is
      O(terminal flows + batch) — H-D1/H-D2's cure rides the same shape.
- [x] H-B2 HIGH: the drain batch borrows event_writer_batch_size (100)
      — a 10M backlog = ~35 days per table. FIX: the prune family's own
      batch knob (prune_batch_size + the SweepBatchSizer circuit
      breaker) or a dedicated retention batch setting.
      DONE: the five arms' leader closures take
      `batch_size=ctx.deps.settings.prune_batch_size` (the prune family's
      own knob; the SweepBatchSizer breaker remains the cron lane's —
      these arms' passes are index-served + batch-bounded and the tick
      tolerates QueryCanceledError per spec). PIN:
      `test_arms_batch_derives_from_the_prune_family_knob` — the AST read
      of the five closures: the batch keyword names prune_batch_size,
      event_writer_batch_size appears in none, and each closure wires its
      arm's rootless pass; the settings pin asserts the knob is positive
      at the prune family's default.
- [x] H-B3 HIGH: the EXISTS legs never check step_key='__flow__' — a
      row pointing at a live NON-root job is unprunable (a THIRD orphan
      class, proven); the node_progress rows on no-flow_id nodes too;
      the docs' claim is aspiration, not SQL. FIX: the root check gains
      the step_key leg (or the arms' contract is restated honestly +
      the named counter for the unmatchable class).
      DONE: the two-pass shape states the ROOT IDENTITY once per leg —
      the flows-side selection requires `step_key='__flow__'` (only true
      roots are selected); the rootless legs prune when the pointee is
      NOT a flow root at all (absent OR step_key != '__flow__' — one
      NOT-EXISTS per arm; node_progress's variant: the node gone, the
      node WITHOUT the flow_id stamp, or the pointee not a root — one
      NOT-EXISTS). PINS: `test_non_root_pointee_rows_prune_and_live_
      root_rows_survive` (the non-root-pointee rows prune via the
      rootless pass; the live-root rows survive; the terminal-aged
      root's rows stay the flows-side pass's own — the two passes'
      division of labor pinned) and `test_node_progress_orphan_legs_are_
      honest` (the gone node, the no-flow_id node, the non-root-pointee
      node each prune; the live-root node's row survives). THE DOCS'
      RESTATEMENT IS OUTSTANDING: docs/guides/workflows.md is DIRTY (the
      docs agent's §retention section still claims the row-clock age
      law + the EXISTS-leg shape + `event_writer_batch_size`) — flagged
      to the docs agent in this session's report, not edited (their
      territory, mid-flight).
- [x] H-B4 MEDIUM: the edge arm's finished_at-NULL hole — a terminal
      root without the stamp is unprunable forever, silently. FIX: the
      arm includes NULL-stamped terminal roots past the period (age
      from the row's own clock), or a named gauge fires.
      DONE: every arm's doomed-flow selection carries the NULL-stamp leg
      (`finished_at IS NULL OR finished_at <= statement_timestamp() -
      $2` — aged by nothing → included), and each flows-side arm answers
      a `null_stamped` scalar; the arm function fires the NAMED warning
      `wf_retention_null_finished_at` when it deletes under a NULL stamp
      (`_sweep.py::_warn_null_stamp` — the stamping-bug tripwire).
      PIN: `test_null_finished_at_terminal_root_prunes_with_warning` —
      the NULL-stamped terminal root's edge prunes and the warning
      fires.
- [x] H-B5 MEDIUM: the migration builds three full-table plain indexes
      at boot (30-90s SHARE locks, crash-looping pods on the 120s
      advisory wait) — missing the estate's own by-hand-CONCURRENTLY
      convention (01.00.23_01's documented cure). FIX: the
      no-transaction + CONCURRENTLY preamble or the ops-note convention.
      DONE: 01.00.35_01 rewritten to the runner's own no-transaction
      template (`-- taskq:no-transaction` in the leading block;
      DROP INDEX CONCURRENTLY IF EXISTS → CREATE INDEX CONCURRENTLY IF
      NOT EXISTS per index — the interrupted-build INVALID-index cure
      the runner docstring mandates; the by-hand note STAYS for the
      too-large deployments, and the 01.00.06_01 deadlock caveat is
      stated honestly in the header with its re-run remedy). PINS (new
      file tests/test_wf_retention_migration_pins.py): the directive is
      in the leading block and the runner ACCEPTS the file
      (`Migration.use_transaction is False`); every statement body is
      the CONCURRENTLY shape (no plain CREATE INDEX); the module
      schema's own ledger records `use_transaction = false` (the real
      runner's acceptance receipt — the fixture applies the bundled set
      through apply_pending); the file RE-RUNS clean on the live schema
      (idempotent, the no-transaction contract) with both indexes VALID.
- [x] H-B6 MEDIUM: wf_step_ledger_flow_idx is REDUNDANT (the claim
      arbiter's leading column serves the probe; measured 1.39ms vs
      2.25ms without) — pure write amplification on the hottest table.
      FIX: drop it.
      DONE: the migration ships exactly TWO indexes (wf_edge_flow_idx +
      wf_join_fire_flow_idx — the tables with no flow_id-led index);
      the step ledger's flow probe rides the claim arbiter's leading
      column (the flows-side statement's comment states it). PIN:
      `test_migration_ships_exactly_the_two_flow_indexes` (wf_step_
      ledger_flow_idx absent, the ops note present) +
      `test_flow_index_serves_the_flow_probe` (the per-flow delete leg
      plans against the shipped indexes).
      NB (REVERSED BY THE INTEGRATION PASS): the no-transaction
      CONCURRENTLY shape above DEADLOCKED the 8-applier boot race —
      01.00.06_01's own analysis, re-proven (the winner's CIC waits on
      the parked advisory waiters' virtual transactions, the deadlock
      detector fails the apply, 7 of 8 appliers died in turn, INVALID
      debris each time). The file is now the 01.00.06_01 transactional
      plain shape (use_transaction=true, no CONCURRENTLY statement); the
      pins flipped with it; see the DONE section's integration pass.
- [x] H-B7 LOW/NIT: the UndefinedTableError tolerance starves the
      guard's reset during long pre-migration windows; three clock
      vocabularies in one family.
      DONE (the clock half — the assigned fix): the five arms tell ONE
      clock — statement_timestamp(), the prune family's STABLE
      selection-bound face (the two-clock law test_index_audit.py
      documents) — all ten statements carry it, no clock_timestamp()
      survives in any. PIN: `test_the_five_arms_tell_one_clock` (with
      the documented exemption: the EDGE rootless statement carries no
      clock at all — wf_edge has no timestamp column; the pointee's
      absence IS the age proof). The UndefinedTableError-tolerance half
      is NOT fixed (the pre-migration windows' guard-reset starvation —
      a different cure, left for a follow-up).

#### ROUND-2 (the red team's five convictions on the arms)

- [x] R1 MEDIUM-HIGH: the rootless walk never advances at a live PK
      front — `ORDER BY pk LIMIT batch` re-visits the SAME first batch
      when its probes all answer "live root" (proven: a 3,000-row live
      front, batch 1,000, five passes, deleted=0, 50 orphans beyond the
      front stranded forever).
      DONE (the persisted cursor — the estate's own watermark
      precedent, `job_events_prune_state` 01.00.20_02): migration
      `01.00.36_01_pre_wf_rootless_sweep_cursors.sql` adds
      `wf_rootless_sweep_state` (one row per arm, the arm's last
      VISITED PK as text in `cursor_a`/`cursor_b`, NULL = from the
      start, seeded for the six arms); every `*_ROOTLESS_RETENTION_SQL`
      statement now SEEKS STRICTLY PAST its arm's row (a COALESCE to
      the below-everything sentinel — the nil uuid / seq 0 / the empty
      channel — deliberately NOT an `OR cursor IS NULL` leg, which
      would cost the walk its PK index), and ADVANCES the row to the
      last VISITED key in the DELETE's own statement/transaction (the
      watermark's own law: never lag what is already gone, never claim
      what is not — a crashed pass rolls the cursor back with its
      deletes). VISITED, not deleted: a live front's rows ARE the
      advance. An exhausted pass REWINDS the cursor to NULL (the
      wraparound: a row whose root died after the walk passed is
      re-probed on the next sweep). The state row survives restarts
      (a table row) and a concurrent same-arm pass serializes on its
      FOR UPDATE. PINS (red→green, the exact story, parametrized over
      all six walks):
      `test_rootless_walk_advances_past_a_live_pk_front` — 3,000-row
      live front + 50 orphans above it, batch 1,000, five passes; RED
      (pre-cure): `per_pass == [0, 0, 0, 0, 0]` — five passes, zero
      deletes, the orphans stranded (the conviction reproduced); GREEN:
      `[0, 0, 0, 50, 0]` — the orphans DELETED by pass 4 (≥ 2), the
      front's 3,000 rows ALL survive, the cursor is set after pass 1,
      MOVES pass over pass, IS the front's last key after pass 3 (the
      visited cost bounded: exactly the batch), and REWINDS to NULL on
      the exhausted pass 5. MEASURED (EXPLAIN ANALYZE, 50k-row seed —
      .measurements/runs/wf-rootless-cursor-plans-r1-*.json): the walk
      plans `Limit (rows=1000) → Index Scan (rows=1000)` — the row-wise
      seek rides the PK index, no sort node (the index's own order),
      0.18-0.20 ms/pass fresh AND advanced; the state row's own read is
      `LockRows → Index Scan rows=1`. (At a 3k-row table the planner
      correctly prefers Bitmap+Sort — cheaper below its crossover; the
      visit is still only the rows above the cursor.)
- [x] R2 MEDIUM: the NULL-stamp leg was degenerate BOTH ways —
      [busy] `ORDER BY finished_at NULLS LAST` put the NULL-stamped
      terminal roots past the LIMIT: never selected, and the tripwire
      (keyed on DELETION) never fired, silently forever; [idle] a
      seconds-old flow's ENTIRE history deleted under a 30-day period
      because its stamp never landed ("aged by nothing").
      DONE (the CONSERVATIVE shape): the NULL-stamped terminal roots
      are SKIPPED — NEVER doomed (`finished_at <= statement_timestamp()
      - $2` only; no history is removed that has no honest age) — and
      the flows-side statements OBSERVE the class: a `null_observed`
      count over the same jobs_finished_at_idx face (the NULLs sit
      contiguous per status — an index range probe, not a scan), which
      is NOT the selection and cannot starve behind the batch; the arm
      function warns, rate-limited to the leader's warn cadence
      (`_NULL_STAMP_REWARN_SECS` = 3600 s per table, the stranded-actor
      re-warn precedent, module-level and monkeypatchable like
      `_PRUNE_RETRY_BACKOFF_INITIAL_SECS`). PINS (the round-1 pin (i)
      is REPLACED — its law was the convicted shape):
      `test_null_stamp_idle_shape_rows_survive_and_warning_fires` (RED:
      "the NULL-stamped terminal root's edge DELETED under a 0:30:00
      period (deleted 1) — the delete was eager"; GREEN: deleted=0, the
      rows SURVIVE, `wf_retention_null_finished_at` fires) +
      `test_null_stamp_busy_shape_warning_not_starved` (5 aged stamped
      roots fill the batch of 3; RED: "the tripwire starved behind the
      batch ... the warning never fired: []"; GREEN: the NULL root's
      rows survive AND the warning fires — the observation is not the
      selection).
      THE DOCS' NEEDED EDIT (docs are not this lane's — REPORTED):
      docs/guides/workflows.md §retention, the flows-side bullet's
      NULL-stamp sentence (~line 824: "A NULL-stamped terminal root is
      INCLUDED (aged by nothing ...)" + "warning when it deletes under
      a NULL stamp") states the CONVICTED shape — it must read: SKIPPED,
      never doomed (no history is removed that has no honest age), and
      the arm's rate-limited `wf_retention_null_finished_at` warning
      fires when a pass OBSERVES one (fix the stamp or backfill it —
      the rows stay until they carry an honest age). The same section's
      rootless bullet may name the persisted cursor (01.00.36_01) —
      optional.
- [x] R3 MEDIUM: the SIXTH table — wf_node_stream was in neither the
      arms nor testing/pg.py's truncate list: a dead node's ring
      (nodes x 64 rows) survives forever, unbounded across nodes.
      DONE (the FOLD — the documented call): the node_progress arm's
      statements gained the stream's deletion. (a) testing/pg.py's
      FK-less truncate list += `wf_node_stream` (+ the new
      `wf_rootless_sweep_state` — mutable test state, the
      job_events_prune_state precedent's reset contract); (b) the ARM:
      NODE_PROGRESS_RETENTION_SQL deletes the doomed flows' stream rows
      in the SAME statement (the stream rows carry `flow_id` directly —
      wf_node_stream_flow_seq_idx, 01.00.29_01, serves it — off the
      SAME doomed-flow selection, its own `LIMIT $1`), and
      NODE_PROGRESS_ROOTLESS_RETENTION_SQL runs the stream's orphan
      walk (the node GONE, or the carried flow link naming a NON-root,
      aged by `emitted_at`, its own batch bound and its own arm
      cursor). ONE PASS, TWO BATCH-BOUNDED DELETES/WALKS — the family's
      one-batch-per-pass law's documented allowance (each data-modifying
      CTE carries its own LIMIT $1; the pass's cost stays O(terminal
      flows + 2x batch)); a LIVE node's ring is never touched (the ring
      trim owns the live side). No sixth spec, no new settings field —
      the stream rides the node_progress arm's period knob and batch
      knob. PINS: `test_truncate_list_names_every_wf_table` (RED:
      "wf_node_stream [is] not in the truncate list"; GREEN: every wf
      table the schema ships is listed) +
      `test_truncate_schema_leaves_zero_rows_in_all_wf_tables` (RED:
      "wf_node_stream kept 1 row(s) past the reset"; GREEN: zero rows
      in ALL nine wf tables after the reset) + the dead-node/live-ring
      legs pinned inside
      `test_rootless_walk_advances_past_a_live_pk_front[wf_node_stream]`
      (the dead node's 50 ring rows delete under the arm; the live
      node's 3,000-row ring survives) — the existing 13 arms pins stay
      green UNCHANGED (the fold adds zero deletions when no stream rows
      are seeded; the arm's return is the pass's TOTAL — progress +
      stream — so the metric stays honest).
- [x] R4 LOW-MEDIUM: the five rootless gates ran in LOCKSTEP (every 8th
      call, all five) — the tick-8 spike: 5 walks + 5 flows-side passes
      in ONE tick.
      DONE: the per-arm phase offset — `_wf_rootless_gate(arm, phase)`,
      the gate fires when `turns % 8 == phase % 8`, the five
      construction sites pass the phases 0-4 in spec order (one line
      each); the spike is spread across the ticks. PINS:
      `test_rootless_gates_fire_phase_offset_never_two_in_one_tick` —
      the behavioral read drives the five gates over 16 consecutive
      ticks (RED pre-cure: the factory took no phase — the wiring pin's
      phases read []; GREEN: each arm's rootless pass fires EXACTLY
      twice, on its own phase's turns, and no tick carries two) + the
      AST read of the loop's five construction sites (the phases are
      [0, 1, 2, 3, 4], in spec order).
- [x] R5 LOW: the migration's lock threshold was UNMEASURED — the ops
      note said "TOO-LARGE deployments build by hand" with no number to
      threshold on.
      DONE (no code change): 01.00.35_01's OPS NOTE (locks) carries
      the MEASURED figure — the boot's SHARE lock BLOCKS ALL WRITES
      (inserts/deletes; reads proceed) for the build's span, measured
      ~1.0 s @ 3M rows on warm NVMe (the round-2's data; re-measured on
      this box at 0.6 s warm-cache / 1.8 s cold, PG 18, the 266 MB
      wf_edge shape — a cloud network volume is a multiple of any local
      number: measure a staging restore and state your own multiple) —
      and the by-hand threshold's shape: beyond ~1M rows on cloud
      volumes, build by hand (the CONCURRENTLY maintenance-window form)
      instead of at boot. PIN:
      `test_migration_ops_note_carries_the_measured_lock_threshold`
      (RED: the tokens absent; GREEN: "measured", "3M", "1.0", "SHARE
      lock", "BLOCKS ALL WRITES", "cloud", "~1M", "maintenance window"
      all present in the ops-note section).

### H-C. The admin UI
- [x] H-C1 HIGH: POST /nodes/__flow__/retry re-arms the RUN'S ROOT —
      fixed: the CAS carries the verb-level root guard
      (`_sql_runner.RETRY_NODE_CAS_SQL_TEMPLATE` gains
      `AND step_key <> '__flow__'` — one seat; the admin route, the
      CLI's `flows retry`, and the compiled face all ride it); the
      root-key POST lands the not-applied banner + its own audited
      refusal row; the law is stated in both verb docstrings.
      PINS (red→green): test_wf_hc_cures_pins.py —
      test_retry_verb_refuses_the_run_root_key_and_moves_nothing (red:
      granted=1 + the root flipped/verdict erased; green: 0 + the row
      byte-identical) + test_retry_route_answers_the_not_applied_
      banner_for_the_root_key (red: a clean 303; green: the banner +
      the audit row).
- [x] H-C2 HIGH: the I/O panel's RESULT face renders token-head secrets
      RAW — fixed: `_wf_actions._redact_io_doc` runs the FULL chain
      (mask_credentials → the token-head pass) on BOTH faces, over
      VALUES and KEY NAMES both (a `{"sk-…": …}` key masks); the
      result face's text + truncated flag both describe the
      POST-redaction bytes. PIN: test_the_node_io_faces_mask_the_
      canary_shapes_symmetrically (red: the bare sk-… value + the key
      name rode raw on the result face, the key name on the input
      face; green: no canary byte on either face, `***` where the
      masks live).
- [x] H-C3 HIGH: the cancel/retry TOCTOU — fixed: the verb's root
      status read takes `FOR UPDATE` (`_runner_exit.retry_workflow_node`)
      — the cancel serializes behind the retry's commit (its cascade
      flips the re-armed node), the refusal sees a landed cancel.
      PIN: test_the_retry_serializes_behind_a_concurrent_cancel_no_
      pending_under_cancelled — the TRUE two-connection interleaving
      (the read, the cancel's commit inside a bounded window, the CAS):
      red = the node PENDING under the cancelled root (the wedge);
      green = the node cancelled. Stable ×3.
- [x] H-C4 MEDIUM: exit-SKIPPED branches never re-run while the confirm
      copy promises "the run resumes" — fixed: the census leg (the
      engine's own re-arm statement `RETRY_SKIP_REARM_SQL_TEMPLATE`:
      `result ? 'skipped'` rows flip pending + the envelope CLEARED,
      inside the grant's transaction; registered in the bounded-writes
      census); the confirm copy names the re-arm; the audit detail
      carries skips_rearmed. PIN: test_the_run_rerun_re_arms_the_exit_
      skipped_rows (red: the envelope outlived the rerun; green:
      pending + envelope gone + the root running).
- [x] H-C5 MEDIUM: a mid-composition refusal leaves the run
      HALF-RERUNNED while the banner implies nothing moved — fixed: the
      composition tracks applied/refused; the refusal lands the
      PARTIAL banner ("N of M re-opened before the refusal",
      `rerun-partial:<applied>:<total>` in the closed vocabulary) and
      its own audit row with the counts. PIN: test_a_mid_composition_
      refusal_names_the_partial_state (monkeypatched verb raising on
      the 2nd of 3 keys; red: the clean refused banner; green: the
      counts in the banner AND the audit row).
- [x] H-C6 MEDIUM: the capture read hides the REAL failed attempt's
      capture once a later attempt succeeds — fixed: the read prefers
      the LATEST FAILED row WITH a capture
      (`_NODE_IO_CAPTURE_FAILED_SQL`), falling back to the latest
      terminal row; the io view labels it (`capture_attempt` /
      `capture_status`), the JS renders the label. PIN:
      test_the_capture_read_surfaces_the_failed_attempt_labeled
      (failed@1-with-capture + the manual resume + succeeded@2; red:
      capture None; green: attempt 1's capture, labeled).
- [x] H-C7 MEDIUM: the retention arms amputate the surfaces that READ
      flow history — fixed (THE UI SIDE ONLY, per the lane): the panel
      distinguishes RETENTION-PRUNED from policy-suppressed (a failed
      node with an attempt on the clock, an EMPTY step history, and a
      terminal root = the pruned arm: `capture_pruned`, the honest
      copy "the flow history was pruned by retention"; the suppression
      lie is dead); the rerun's closure blindness (the pruned wf_edge
      read) is stated in the route docstring. PIN:
      test_the_panel_names_retention_pruned_history_not_policy_
      suppression (the ledger rows deleted by hand — the arms' own SQL
      is another lane's; red: capture_suppressed=true; green: the
      pruned arm).
- [x] H-C8 MEDIUM-LOW: the compile's key law stops at the dot — fixed:
      the wiring key refuses /, ?, #, %, both quotes, control chars,
      and edge whitespace at the dot law's own site (`api/_graph.py`),
      the remedy named; the mermaid label writer escapes `\`,
      `"`, `\r`, `\n` (`_wf_rows._mermaid_label`) — byte-stable,
      touchless for clean keys. PINS: test_the_wiring_key_refuses_the_
      affordance_hostile_charset (the refusal per char class + the dot
      law standing) + test_the_mermaid_label_writer_escapes_quotes_
      and_newlines (the escaped label + byte-stability). NOTE (found
      beyond the list): a hostile ROW's key also rides the mermaid
      node-ID and edge lines (outside the convicted label face) — a
      row with a raw newline there still parse-errors the render;
      the JS's defined render-failure state is the degrade, the
      compile law stops new keys.
- [x] H-C9 NIT: fixed — the not-applied copy drops the impossible
      attempt-ceiling cause (+ names the root key); the SSE snapshot
      carries the Error column's class (`run_state_json`) and the JS
      patches it + adds/removes the Retry affordance LIVE (the form
      built from the stamped CSRF; the behavioral Node pin:
      test_the_retry_affordance_adds_and_removes_live_under_node —
      teeth-proven against the HEAD JS); the truncated flag computes
      POST-redaction (pinned with the canary's bulk band);
      a refused rerun writes its audit row (both routes, the
      audited-refusal law); the panel's children render is capped
      (`_PANEL_CHILDREN_CAP` + `count(*) OVER ()`'s total + the
      "N more" arm, pinned with the cap monkeypatched to 5).

#### H-C ROUND-2 — THE RED-TEAM'S SECOND PASS (each fixed + pinned by
#### the round-2 fix agent; pins in
#### tests/web_admin/test_wf_round2_cures_pins.py, red-proven: 11 of
#### the 14 pins red against the reverted tree, the other 3 being unit
#### pins of NEW functions whose pre-fix state lacked them)
- [x] R2-1 MEDIUM: the retry faces lack the deadlock NAME — the
      cancel/resolve CLI verbs catch
      (DeadlockDetectedError, SerializationError) → the named refusal,
      but the retry CLI caught only WorkflowRunError (a laggard
      writer's deadlock = the rich-traceback face) and the admin
      retry route caught NOTHING (= the 500 wall). FIXED: both faces
      catch the SAME pair — the CLI echoes the named race refusal
      (exit 1, the re-run remedy, `from None`); the node-retry route
      AND the run-rerun composition land the audited refusal row
      (`_audit_rerun_refusal`, `DEADLOCK_REFUSAL` on the record) + the
      new `rerun-raced` banner (`RERUN_RACED_BANNER`; the copy names
      the race and the reload remedy; the composition's race with
      nothing granted shows the raced banner — "the run is cancelled"
      would lie). PINS: test_the_retry_cli_answers_the_deadlock_
      with_the_named_refusal (monkeypatched verb raising
      DeadlockDetectedError → exit 1 + the copy, the raw error's
      traceback face absent), test_the_retry_route_answers_the_
      deadlock_banner_not_a_500 (red: the 500 wall; green: the 303 +
      `error=rerun-raced` + the banner copy renders + the audit row
      names the deadlock), test_the_run_rerun_route_catches_the_
      deadlock_pair_too.
- [x] R2-2 MEDIUM: the capture face runs no display-time mask — the
      input/result faces re-run `_redact_io_doc` at display, the
      capture face trusted the write (a pre-chain-release write's raw
      canary rendered raw). FIXED: `_node_io_view`'s capture block
      runs the SAME chain (`mask_credentials` → `_mask_hold_token`,
      values and key names) before the view builds; the H-C6 labels
      survive (the chain masks strings, not the attempt's identity).
      PIN: test_the_capture_face_masks_the_raw_canary_at_display
      (a hand-seeded ledger row carrying the raw canary — the
      pre-chain write's shape — renders masked, `***`, no canary byte;
      capture_attempt still 1).
- [x] R2-3 MEDIUM: the unbounded pre-truncation walk — the result
      face walked/dumped the ENTIRE result doc before the 10k display
      bound (a multi-MB row = a multi-MB walk per drill-down). FIXED:
      `_bounded_io_projection` (the structural pre-cap) — depth (32),
      node (2,048), and character (8,000) budgets applied BEFORE the
      redaction walk; the FIRST cut ends the walk (every further node
      is the marker — the per-item-marker ballooning is itself
      measured dead); the served text stays POST-redaction (the
      honesty law: the chain runs on the projected doc; a secret cut
      by the trim is absent, a secret in the kept band is masked);
      the named size arm rides the face (`walk_note`: "the result
      exceeded the panel's walk budget — showing the first N
      characters…") and the JS renders it. PINS:
      test_the_walk_projection_bounds_a_multi_mb_doc (a 2MB doc and a
      50k-DEEP doc — the node/char census at budget, no
      RecursionError; the under-budget doc untouched, no note),
      test_the_result_face_answers_bounded_with_the_named_arm (a 2MB
      synthetic row: the response < 100KB, the walk_note + the
      at-site marker present, the canary in the kept band masked),
      test_the_page_js_renders_the_walk_budget_arm.
- [x] R2-4 MEDIUM-NIT: the skip-rearm's predicate was key-existence
      (`result ? 'skipped'`) — a foreign writer's `{"skipped": false}`
      matched and had its result DELETED. FIXED: the engine's own
      SKIPPED_RESULT containment — `result @> '{"skipped": true}'::
      jsonb` (RETRY_SKIP_REARM_SQL_TEMPLATE); the census entry's
      scoping substring moved with it (the round-2 registration
      duty). PIN: test_the_skip_rearm_predicate_is_the_true_
      skipped_true_shape ({"skipped": false} and {"skipped": "no"}
      rows NOT re-armed, their envelopes INTACT; the true envelope
      `{"skipped": true, "exit_from": …}` re-armed pending + cleared;
      the audit's skips_rearmed == 1).
- [x] R2-5 NIT batch: (a) the dead `ExitOps.retry_node` (zero
      PRODUCTION callers, stale docstring) — the DELETE arm was
      rejected on the finding that TWO test callers exist
      (tests/test_wf_phase3_cure_pins.py, outside this lane), so the
      method DELEGATES to `retry_workflow_node` through the public
      seam (the ONE implementation; the compiled face gains the
      H-C3 FOR-UPDATE serialization, the H-C4 skip re-arm, and the
      cancelled-run refusal it lacked) — PIN:
      test_the_compiled_retry_face_delegates_to_the_verb (the spy
      call-through on the patched seam + the bool mapping of the row
      count). (b) the masked-key collisions — two keys masking to the
      same `***` overwrote (the second replaced the first); the walk
      disambiguates `***[2]`, `***[3]`, … until unique (every entry
      survives, no raw key byte rides) — PIN:
      test_masked_key_collisions_keep_every_entry. (c) the map-key
      retry's row count — the verb returned a grant BIT while a map
      key's grant re-arms EVERY failed row of the key; now it returns
      the ROW count (its audit row carries `rows_reopened`), the
      run-rerun composition tracks keys_applied AND rows_reopened,
      the audit row carries both (`applied`/`total` keys,
      `rows` — H-C5's shape preserved), and the partial banner copy
      says ROWS-WHERE-HONEST (the key shape
      `rerun-partial:<rows>:<keys>:<total>`; rows == keys → the plain
      "N of M", they diverge → "N node rows re-opened across M of K
      failed keys") — PINS: test_the_verb_returns_the_true_row_count_
      for_a_map_key (2 seeded failed rows of one key → 2 + the audit
      row's rows_reopened == 2), test_the_rerun_audit_row_gains_the_
      rows_count_and_the_banner_says_rows (red: the key-only audit +
      the plain copy; green: rows == 2 on the row + "2 node rows
      re-opened across 1 of 3 failed keys"),
      test_the_partial_banner_copy_says_rows_only_where_honest (both
      copy arms). (d) the cookie-loss 403 loop — the CSRF-mismatch
      refusal named no remedy; both 403 doors (mismatch,
      missing-cookie) now name it ("the page's token is stale —
      reload the page and submit again") — PIN:
      test_the_csrf_mismatch_refusal_names_the_reload (the mismatch
      403's detail carries the phrase; the missing-cookie arm too).
- [x] R2-6 THE BOUNDED-WRITES CENSUS: the round-2 write-set change
      (the R2-4 predicate) is registered in
      tests/test_sweepaudit_bounded_writes.py's own convention — the
      RETRY_SKIP_REARM_SQL_TEMPLATE entry's scoping substring is the
      new containment predicate with the updated justification; the
      census tests green in the gate run (no new module-level write
      statement landed in this wave — the projection is a read, the
      delegate deletes a write path).

### H-D. The performance follow-ups
- [x] H-D1 MEDIUM: the collect recompiles every workflow per boot (the
      compile+validate doubled: 92us/node, 918ms @ 10k nodes). FIX: the
      collect reads the recorded compiled graphs (the projection's
      record_compiled already wrote them).
      DONE (the integration pass): ``collect_workflow_reservations``
      reads ``get_compiled_workflow(name)`` (the ``_compiled`` recording
      the projection's ``get``/``record_compiled`` already wrote) — the
      sibling ``collect_workflow_rate_limits``' own shape; a workflow
      NOT in the recording (the projection's skip-loudly arm skipped it)
      hits the KeyError → the same skip-loudly log, the build fn NEVER
      re-invoked to resurrect it; the boot's own order (the projection
      :2102 → the collect :2135) guarantees the recording. The six
      cold-collect pins gained the projection call first (the boot's
      order; the file's own established pattern — the H-A3/H-A4 collect
      pins already projected). PIN (red→green):
      ``test_the_collect_reads_the_recorded_graphs_not_a_recompile`` —
      a build-fn call counter: red = 1 → 2 (the collect re-invoked the
      fn); green = the count never moves AND the declared reservations
      still return.
- [ ] H-D2 MEDIUM: the zero-doomed tick is O(table) per arm per tick —
      fine at the cited scales, unbounded at 1000x. FIX (with H-B1): the
      flows-side doomed selection flips the floor to
      O(terminal flows + batch).

#### ROUND-3 — the red team's six residues (each fixed pin-first by the
#### round-3 fix agent; the behavioral pins red-proven against the
#### convicted shapes, then green; the new web pins live in
#### tests/web_admin/test_wf_round3_cures_pins.py — NOT yet in the
#### fixed gate command, reported)

- [x] R3-1 RANK 1 (the doc-truth contradiction): the retention section's
      flows-side bullet (docs/guides/workflows.md ~:824) stated the
      CONVICTED INCLUDE law — "A NULL-stamped terminal root is INCLUDED
      (aged by nothing…) … the warning fires when it deletes under a
      NULL stamp" — while the landed code (_sweep.py's
      ``_warn_null_stamp`` + the statements' ``null_observed`` face,
      settings.py's own rows) is the SKIP law: NULL-stamped terminal
      roots are NEVER doomed, and the rate-limited
      ``wf_retention_null_finished_at`` warning fires when a pass
      OBSERVES one (hourly re-warn cadence per table), never alongside
      a deletion. FIXED: the bullet rewritten to the landed law,
      mirroring settings.py's own rows' text (SKIPPED — never doomed:
      no history is removed that has no honest age; the rate-limited
      warning fires on the OBSERVATION — fix the stamp or backfill it,
      the rows stay until they carry an honest age). Verified against
      the code before writing (_sweep.py:527-558 +
      ``_NULL_STAMP_REWARN_SECS`` = 3600 s; the selection keeps
      ``finished_at`` non-NULL). This closes the H-B ROUND-2 R2 "DOCS'
      NEEDED EDIT — REPORTED" debt. VERIFIED: ``mkdocs build --strict``
      clean ("Documentation built in 16.10 seconds").
- [x] R3-2 LOW-MED (the sibling collects' divergence): the
      reservations collect walked the LIVE apps' workflow names, the
      rate-limit collect walked ``sorted(_compiled)`` — the
      process-global record with NO eviction: a workflow deleted from
      its app in-process left its graph strongly referenced forever,
      its buckets kept registering, and a surviving workflow
      re-declaring the same bucket name with a different config raised
      the registry's conflict at the collect's unguarded register —
      THE GHOST REFUSED THE BOOT. FIXED (ONE base for the siblings):
      ``_iter_live_recorded_workflows`` — the LIVE apps' names over the
      RECORDED cache (H-D1's no-recompile law kept), the rate-limit
      collect converged to the reservations collect's own base; the
      orphaned graphs' buckets stop registering (the ghosts die); a
      skipped workflow skips LOUDLY in BOTH collects (the rate-limit
      face gains the reservations' own
      ``workflow-*-collect-skipped`` arm). Caller check first: the
      bootstrap's rate-limit block consumes ``(registered, unknown)``
      for its log + warning only — no base expectation. ALSO (the
      ValueError's render): the collect re-raises the registry's
      duplicate-config refusal with the RESOLVED bucket name + the
      differing config FIELDS (``_rate_limit_config_diff`` — the
      reservation face's ``_reservation_config_diff`` shape mirrored),
      never the raw ``__slots__`` reprs, cause chained. PINS (red
      against the convicted walk, then green):
      ``test_the_dropped_workflows_ghost_stops_registering_the_boot_
      stays_clean`` (RED: ``ValueError: rate-limit name already
      registered with a different config: 'ghost-bucket',
      existing=<TokenBucket object at 0x…>`` — the ghost refusing the
      boot; GREEN: the live config-9 bucket registered, the ghost's
      capacity-5 and slots-1 terms in neither collect's return) +
      ``test_the_rate_limit_conflict_refusal_names_the_buckets_not_
      the_reprs`` (RED: the repr dump; GREEN: ``clash-bucket`` +
      ``capacity`` named, ``object at 0x`` absent).
- [x] R3-3 LOW (the park stamp's stale released_bucket): a row denied
      on bucket X then fail-closed-parked carried BOTH stamps — the
      snooze statement's metadata merge is jsonb concatenation, the
      park's merge overwrote ``released_reason`` but not
      ``released_bucket``: the trail read "parked" beside a bucket
      that was not the park's truth. FIXED: the park shape clears
      ``released_bucket`` (None — jsonb null overwrites) in
      ``_flow_denial_snooze_metadata``. PIN:
      ``test_the_park_stamp_clears_the_stale_denied_bucket`` — the
      deny → park story at the helper (the merge's left side is the
      denial's own stamp, ``dict | dict`` is the statement's jsonb ||
      in Python); RED: the merged row still carried
      ``'released_bucket': 'guard-x'``; GREEN:
      ``released_reason=flow-rate-limit-parked``,
      ``released_bucket is None``, ``parked=flow-reservation-unknown-
      bucket``.
- [x] R3-4 NIT (the walk-note's unit lie): the size budget counted
      string VALUES only — keys and JSON structure rode FREE; the
      key-heavy doc served ~46k for a claimed 1,023 and the panel's
      own truncation arm fired on top of the walk's (the
      ``_bounded_io_projection`` comment's "the panel's truncation arm
      then never fires" false in that shape). FIXED: the budget counts
      the SERVED size — every node a flat syntax frame (quotes, colon,
      comma, braces) + its content's length (strings, keys, the
      scalars' exact json render); a cut reserve lands every cut's
      census AT/UNDER the budget (the round-2 pin's tight
      ``chars <= _WALK_CHAR_BUDGET`` stays green); the note states the
      served truth ("showing the first N characters of the SERVED doc
      (keys, structure, and values counted…)" — the number IS the
      accounting); the comment corrected (the panel's cap is the
      BACKSTOP for escape inflation, never the first bound). PIN:
      ``test_the_walk_budget_counts_the_served_size_key_heavy_doc``
      (1,500 keys; RED: "the served text ran 17552 characters for a
      claimed 1023"; GREEN: served ≤ the accounting, < the panel's
      10k cap, the note's number matches).
- [x] R3-5 NIT (the dict budget-out overwrite): the dict walk's
      budget-out wrote ``out[_WALK_PRUNED_MARKER] = _mark()`` — a doc
      whose OWN entry's key equals the marker string was OVERWRITTEN.
      FIXED: the collision-safe arm — the marker key disambiguates
      with an ordinal suffix until unique (``…not shown…[2]``, the
      masked-key collision law's own shape), the legit entry survives
      beside the marker. PIN:
      ``test_the_prune_marker_never_overwrites_a_legit_entry`` (RED:
      the legit entry replaced by the marker's own text; GREEN: the
      legit entry + exactly two marker keys, the suffix present).
- [x] R3-6 NIT (the logout CSRF's 403 lacks the reload remedy): the
      logout door's token refusal (auth/_session.py) was "the 403
      loop's other carrier" — it named the mismatch and no cure, while
      the action doors' CSRF refusals all carry the remedy. FIXED: the
      same phrase the doors carry ("— the page's token is stale —
      reload the page and submit again"). PIN:
      ``test_the_logout_csrf_403_names_the_reload_remedy`` (the OIDC
      mount, a bad token's 403; RED: "the 403 names no remedy —
      'logout CSRF token missing or mismatched'"; GREEN: the phrase in
      the detail).

      THE ROUND-3 GATES (all green):
      ``ruff check src/taskq tests/test_wf_capacity_surface.py`` —
      "All checks passed!"; ``ruff format --check …`` — "250 files
      already formatted"; ``pyright src/taskq tests`` — "0 errors, 0
      warnings, 0 informations"; the pytest gate — "103 passed in
      18.62s"; ``mkdocs build --strict`` — "Documentation built in
      16.10 seconds". BEYOND THE GATE: the round-3 web pins file +
      the step-rate-limit pins + the execution-seam + the retention
      arms — "51 passed"; the cli/api-surface/flows-demo/enforcement/
      sweepaudit files — "45 passed".

      REPORTED (cross-file, not this agent's files): (a) the
      registry's OWN duplicate-config message still renders raw
      ``__slots__`` reprs for every OTHER registration path (the
      bootstrap's vanilla pass, keyed materialisation) — only the
      workflow collect re-renders it now; a ``__repr__`` or a
      field-diff render in ratelimit/registry.py would cure the class.
      (b) The symmetric stale-stamp case (a row PARKED then denied
      keeps the old ``parked`` kind beside
      ``flow-rate-limit-denied``) — left as-is, the residue scoped the
      park's one truth only. (c) The fixed gate command should grow
      tests/web_admin/test_wf_round3_cures_pins.py (the round-3 web
      pins run only in ad-hoc sweeps today).

## G. THE STANDING DEBTS (the audit's lanes, unchanged)

- [ ] G1. The estate machinery's cure (subtree-scoped claims; the dance
      documented; the fill tax dead).
- [ ] G2. The client/transport P1s (the redis legs' budgets, the
      hostile-envelope starvation, the SSE wire schema, the wedge-parking).
- [ ] G3. The worker-core P1s (the producer-loop swallow seat, the PG-outage
      fleet kill, the orphaned dispatch children, the uncancel loop).
- [ ] G4. The web/admin P1s (the run page's base_path 404s, the bearer
      auth's unattributed trail).
- [ ] G5. The types/API P1s (the dead settings, the unreachable exceptions,
      the test-double detection in production paths, the blanket ignores).
- [ ] G6. The data-model P1s (the map-sibling collapse, the 'awaited'
      reaper, the uuid::text joins, the unindexed sweeps, the hold_epoch
      race, the EXIT_SKIP/RETRY states).
- [ ] G7. The test-estate P1s (the rv2-13 replacement's subprocess burn —
      done with the pin; the registry's divergent faces; the residue).

## I. THE CONSUMER'S RE-REVIEW (2026-10-30) — THE STATUS BOARD'S LAW

TAStack re-ran the proving ground end-to-end (scratch Postgres 18.6,
isolated worktree) and convicted the ledger itself in two places. The
receipts (their run, exit 0): the typed HITL hold; 6 bodies / 0 overlaps
under max_concurrent=1 + the reservation; the pdf_gadget slot released;
the terminal flow swept at a 1µs retention period (the period is
their harness's figure, as reported) — wf_edge 15, wf_step_ledger 13,
wf_node_progress 35 — while the LIVE run's rows stayed untouched. The
capacity surface and the retention arms are REAL in execution.

Their verdict: HOLD THE PIN at 66b8f369. Adopted as the governing
verdict. The convictions, all adopted:

1. `priority` absent — a regression on a shipped surface, not a gap (C1).
2. The queue trap half-closed — the refusal is strict-gated; a
   subset-subscribing fleet cannot set the env var (C14). §B4's
   "fails the boot loudly" was an overclaim; corrected above.
3. Seed-only capacity breaks the code-deploys doctrine for dampers
   (C15); the reservation face keeps their doctrine, the damper loses it.
4. No `Snooze` passthrough (429s burn attempt budget — C4), no
   `start_to_close` (C3), zero flow telemetry (C10), no release tag (C13).

THE STANDING RULE: a documented omission is a claim like any other. It
ships only with a red-proven impossibility pin behind it. "Deliberate",
"deferred", "accepted" — none of these is a proof. If it is buildable,
it is built; if it is genuinely impossible, the pin says so. THIS
LEDGER IS THE CONSUMERS' STATUS BOARD: every claim in it is checked
against source before it ships, and the consumers' re-reviews outrank
its verdicts.

## DONE (with receipts)

- [x] The resolve/cancel deadlock — the lock order unified; convicted by
      the race pin at -n 8; re-proven green (fa76681d8).
- [x] The hypertable time bomb — the seed instant pins its age
      (d05601a82).
- [x] The type gate's hermetic config (c5bf99a43).
- [x] The security revs: aiohttp/certifi/filelock/otel (4b4e4caef);
      authlib/msgpack (31a60848c) — pip-audit clean.
- [x] The release-notes census deleted; the head-stamp law in CI, main-push
      only, .github excluded from the staleness diff (#690, #692).
- [x] The load_sensitive discipline on the three weather bands.
- [x] THE INTEGRATION PASS (the three fix waves' cross-cutting residues —
      no fix agent owned these; NOTHING COMMITTED, all in the worktree):
      + **The CONCURRENTLY boot-race deadlock (H-C report finding #2, the
        new red)** — RED: test_boot_race_8_appliers_fresh_schema lost 7 of
        8 appliers, each winner SystemExit-ing on 01.00.35_01
        ("deadlock detected", INVALID wf_edge_flow_idx debris; the PG
        DETAIL: the winner's CIC waits ``ShareLock on virtual
        transaction`` of a replica parked on the migration advisory
        lock, which waits on the winner's advisory lock). DIAGNOSED: the
        runner's lock DOES span the CONCURRENTLY window (the
        ``async with migration_advisory_lock`` wraps the whole
        ``apply_pending``; the reset-to-0 covers the no-transaction
        files) — the appliers don't bypass it; the cycle is STRUCTURAL:
        a session parked on ANY lock queue is a CIC blocker-scan target
        (proven with an unrelated-advisory-lock probe: CIC blocks behind
        a foreign-tag waiter; idle sessions and idle-in-transaction
        readers do not block it). FIX: the MIGRATION's shape — 01.00.35_01
        rewritten to 01.00.06_01's own transactional plain-CREATE-INDEX
        precedent (use_transaction=true; DROP INDEX IF EXISTS + CREATE
        INDEX IF NOT EXISTS; the by-hand CONCURRENTLY note stays for
        too-large deployments; the deadlock caveat now states the
        PROVEN law, not the accepted-cost guess). The pins flipped with
        it (test_wf_retention_migration_pins.py: the transactional shape,
        no CONCURRENTLY outside comments, the ledger's
        use_transaction=true; H-B6's two-index pins unchanged). The
        standing ``test_bundled_migrations_are_all_transactional`` is
        green again. GREEN ×3: "4 passed" (the four boot-race tests) ×3
        + the full attack file's other families green
        (tests/test_attack_lifecycle_edges.py + test_migrations_unit.py +
        the pins: 115 passed).
      + **The docs' residue** — (a) workflows.md's capacity table: the
        ``max_pending`` row STRUCK, the honest removal stated (the
        engine's inserts bypass the enqueue admission; a claim-side gate
        would wedge the backlog), the validation note's ``max_pending``
        arm dropped; upgrading.md's three ``max_pending`` mentions are
        the VANILLA surface's own (verified true, untouched) — its wf
        queue-trap section DID drift against H-A8 (claimed the
        unconditional refusal): rewritten to the landed gated law
        (strict refuses, non-strict warns), and workflows.md's same
        paragraph with it. (b) admin-ui.md: both SSE sections enumerate
        FIVE topics from sse.py's own ``_valid_topics`` (``holds`` added,
        the HITL broadcast face described; the state feeds' consumer
        claim scoped honestly); the deregister redirect's notice shape
        corrected to the code's ``/actors?notice=deregistered`` (the
        fixed-key message; the ``+{actor}`` shape never existed); the
        queue-detail's status allowed-values corrected to the code's
        eight (``_ALLOWED_STATUSES``); the nav list gains
        Insights/Workflows (read from _base.html). (c)
        api-reference/workflows.md: the ``step()`` row carries the landed
        signature (progress_schema/rate_limits/max_concurrent/
        reservations/result_ttl + the capacity terms' semantics + the
        max_pending absence); the RouteArm face gains the H-A4 capacity
        passthrough; ``@app.actor``'s landed signature joined. (d)
        cli.py's flows-retry not-applied copy: the impossible
        attempt-ceiling cause dropped, the real three named (live,
        unknown, the run's own root) + the root-key law with the cure —
        the admin banner's vocabulary (H-C9's copy honesty). (e) the
        H-B3-flagged retention restatement LANDED (workflows.md's five
        arms section: the two-pass law, the ROOT's finished_at as the
        age clock, the NULL-stamp leg + the named warning, the rootless
        walk's cadence, ``prune_batch_size`` replacing the borrowed
        event_writer_batch_size). ``mkdocs build --strict``: "Documentation
        built in 16.63 seconds".
      + **The lint residue** — (a) B905 at _worker_execution.py:178:
        already strict=True (and the ruff select carries no B rules —
        the finding was belt-and-braces); (b) _graph.py:234's REAL
        finding: the handle-term conflict message rendered raw
        ``<ConcurrencyReservation object at 0x…>`` dumps (the class has
        no __repr__) — now renders the resolved BUCKET NAMES (the same
        identity the comparison keys on); pinned red→green
        (test_handle_term_conflict_message_names_reservation_buckets_
        not_instances: red = the pointer dump; green = both bucket names,
        "object at 0x" absent); (c) format drift in
        tests/test_wf_flows_demo.py (two H-A6 asserts) — reformatted.
        ``ruff check .``: "All checks passed!"; ``ruff format --check .``:
        "1841 files already formatted".
      + **The outbox/step-cache borrowed batch (j2)** — the two
        derived-data retention arms (``wf_outbox_retention_call``,
        ``wf_step_cache_retention_call``) took the five arms' H-B2 fix
        mechanically: ``batch_size=ctx.deps.settings.prune_batch_size``
        (the settings' default needed NO reconsideration — the knob is
        the deletion-sweep family's own, both docstrings already promised
        one bounded batch per pass). PIN (red→green):
        ``test_outbox_and_step_cache_arms_derive_from_the_prune_family_
        knob`` — the AST mirror of (j): red = knob_calls 0; green = both
        closures name the knob, neither borrows. The full retention
        arms file: 13 passed.
      + **H-D1 landed** (see §H-D) — the collect reads the recording;
        the pyright residue it surfaced fixed with it (the H-A6 pin's
        deliberate removed-param calls got the annotated ignore + Why;
        the H-A4 wave's dead ``_image_arm`` helper deleted).
      + **The full gates** — ``pyright src/taskq tests``: "0 errors,
        0 warnings, 0 informations"; ``tests/typeprobe/_gate.py``: "the
        type gate holds: every MUST_ERROR marker (46) reds on pyright
        1.1.414 AND ty 0.0.85 with its DECLARED rule-ids, and no error
        falls outside the markers"; the targeted suite
        (retention arms, capacity surface, enforcement pins, api surface
        pins, flows demo, web_admin workflows page, status pins, schema
        migration, wf CLI pins): "148 passed in 50.81s"; the FULL
        attack-lifecycle file (all four families, the 1M-row legs
        included): "11 passed in 29.50s". The CLI copy gained its own
        pin (test_retry_not_applied_copy_drops_the_impossible_cause_
        and_names_the_root_law — red-proofed by H-C9's law: the unknown
        node and the root key both answer the honest copy, "attempt
        ceiling" absent, zero rows moved, zero audit rows).
