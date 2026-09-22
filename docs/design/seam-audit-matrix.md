# The seam-audit matrix

The systematic version of the red-team. Issues #457-461 all lived at SHARED
SEAMS: call sites where a neighbor caller or an interleaved state that no test
constructed turned a locally-correct contract into a production loss. This
document enumerates those seams, the callers that share them, the failures
that can interleave, the invariant that must hold regardless, and the pin
that holds it. A new seam or caller lands WITH a row here; a row without a
pin is a standing invitation to the next #457.

Method: for each seam, grep every caller; for each caller, the failures that
can interleave (exception mid-call, concurrent exit, restart overlap, clock
skew, resource outage); for each (seam, invariant), grep the tests. Pins are
marked EXISTS (a named test holds it, green on `cf15fb29`), MISSING (no pin),
or ADDED (a pin this audit wrote, green on `cf15fb29`, mutation-verified).
Rows the five live regressions own are marked with their owner branch.

The audit ran on main at `cf15fb29`; the five fix branches
(`fix/457-abandon-drain-cut`, `fix/458-claim-loss-attempt-charge`,
`fix/459-sync-actor-systemexit`, `fix/460-rolling-deploy-cron-recovery`,
`fix/461-registry-attempt-scoped-deregister`) own the live fixes and their
repros. This audit fixes no source.

## 1. Registries (in-process state shared across loops)

### 1.1 `ActiveJobRegistry` mutation keys (`worker/cancel.py`)

Shared by: `_consumer.py` (register, the unconditional-finally deregister),
`worker/run.py` (stub consumer register/deregister; producer
`mark_enqueued`/`mark_claimed`/`resolve_claim`), `heartbeat.py`
(`held_ids`/`queued_ids` for the reconcile), `shutdown.py`
(`held_ids` for the hand-back), `cancel.py` (the drain's deregister).

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| `deregister(job_id)` on bare id, stale attempt exiting after a same-worker re-claim | attempt A's finally runs after attempt B overwrote the key | the popped entry must be the CALLER's; a live attempt's registration survives a stale exit; `held_ids()` still names the row | (repro: `f7256661`, `tests/test_cancel_registry.py` on the fix branch) | MISSING on main - owner `fix/461` |
| `resolve_claim(job_id)` / `mark_claimed(job_id)` on bare id, same re-claim overlap | A's `resolve_claim` lands inside B's take-to-register window; the intent map (the only fence over that window) loses the id before B's `register` covers it | every window of the claim-to-register chain is fenced by exactly one map; a stale attempt's exit cannot unfence a live attempt's row against the hand-back and reconcile passes | none | MISSING - unowned, same fix shape as #461 (identity-scoped hand-back) |
| `register` absorbing the intent, the queued-to-intent transition | take lands between the drain's snapshot and its UPDATE | `mark_claimed` moves coverage queued to intents with no gap; `queued_ids()` and `held_ids()` partition the two windows | `test_cancel_registry.py::test_mark_enqueued_parks_the_id_until_the_take`, `::test_mark_enqueued_is_idempotent_per_take_cycle` | EXISTS |
| snapshot atomicity of `all()` | register/deregister concurrent with an iterate | `all()` returns an independent copy; no await inside; concurrent register/deregister leaves the map consistent | `test_cancel_registry.py::test_all_returns_snapshot`, `::test_concurrent_register_deregister_atomicity` | EXISTS |

### 1.2 `WorkerDeps.disowned_jobs`

Shared by: `_consumer.py`/`_handlers.py` (`_disown_job` on unrecordable
outcomes), `heartbeat.py` (renewal exclusion, the still-held prune, the
lost-claims reconcile), `run.py` (re-own on re-claim), the sweeps (the
reclaim the disown hands the row to).

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| disown on exhausted terminal write; the prune probe | the fleet reclaims the row between the disown and the prune | a disowned id no longer held by this worker leaves the set; a still-held one stays | `test_disowned_jobs.py::test_heartbeat_prunes_disowned_jobs_the_fleet_has_reclaimed` | EXISTS |
| re-own on re-claim | the sweep re-pended a disowned row and this worker claimed it back | `disowned_jobs.discard` at the claim, renewal resumes | `test_disowned_jobs.py::test_producer_reowns_a_disowned_job_it_claims_again` | EXISTS |
| disowned row's lease lapse | heartbeat stopped renewing, worker alive | the sweep reclaims it; no stuck running row | `test_disowned_jobs.py::test_disowned_row_lease_lapses_and_the_sweep_reclaims_it` | EXISTS |
| reconcile probe exclusions and grace | any of the three maps holds the row; a row younger than its lease is an in-flight handoff | held + queued + disowned are ALL excluded from the lost-claims probe; anything else is disowned under a one-lease grace on `started_at` | `test_heartbeat.py::test_heartbeat_tick_reconcile_excludes_held_queued_disowned`, `::test_heartbeat_tick_disowns_the_lost_claim` | EXISTS |
| claim-loss reconcile's outcome recording | a round committed its claim and lost the response (#402 shape) | the disown must release without charging (see 4.2) | `test_rt_402_claim_loss_release_raise.py` covers the release-raise; the attempt charge is #458's gap | MISSING on main - owner `fix/458` |

## 2. Pool and connection ownership

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| dead-on-acquire retry (`connections._with_fresh_connection_retry`), callers: enqueue, bulk-cancel | pooled conn poisoned between release and reuse; `InternalClientError` after a durable write | retry once only when nothing is durable (`mark_wrote` after the ack); a post-write `InternalClientError` propagates (no re-issue, no double-run invitation) | `test_connections.py` (dead-on-acquire absorbed; post-write refused) | EXISTS |
| bounded release (`_bounded_checkout`) | reset hits a silently-dead server | release carries its own timeout, never raises; the op's result stands | `test_backend_bounded_checkout.py`, `test_connections.py` | EXISTS |
| dedicated vs pooled roles (`WorkerConnections`) | caller passes concrete AND factory | rejected eagerly; caller-owned resources never closed by TaskQ | `test_connections.py`, `test_worker_init_coverage.py` | EXISTS |
| per-slot pool hook inheritance (`with_connection_init`) | slot pool opens a second connection family above max_concurrency 1 | a declared init hook reaches every slot connection; a failed hook closes the conn and propagates | `test_connections.py`, `test_worker_bootstrap.py` | EXISTS |
| enqueue lock budget vs command timeout (`bounded_lock_budget_ms`) | budget >= the per-query bound | the clamp keeps the typed server-side refusal reachable | `test_enqueue_coverage.py`, `test_connections.py` | EXISTS |

## 3. The heartbeat tick and its budget

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| one command budget for the whole tick sequence | brownout: statements each just under the per-statement bound, the third times out | failed tick bounded by acquire + ONE budget; the rollback and the close SHARE the remainder | `test_heartbeat.py::test_the_tick_command_budget_cuts_a_brownout_tick`, `::test_an_ordinary_statement_failure_rolls_back_within_the_budget` | EXISTS |
| teardown never displaces the original exception | rollback timeout / close bound during unwinding | the tick's original exception re-raises bare | `test_heartbeat_chaos.py` | EXISTS |
| renewal threshold vs the cascade floor | F+1 failed cycles plus the last good beat's tail | a live lease never lapses before the isolate decision; the gate never skips a row into lapse | `test_heartbeat.py::test_gated_renewal_never_lets_a_live_lease_lapse`, `::test_naive_half_lease_threshold_lapses_before_isolation_at_defaults` | EXISTS |
| failed-tick ledger, both arms | a persistent non-transient fault | one threshold, reset only by a fully good tick; the hook's carve-out counts exactly once | `test_heartbeat.py::test_unexpected_failure_counts_toward_isolate`, `::test_hook_increments_counter_exactly_once` | EXISTS |
| post-tx drain always follows run_in_tx | the tick raised after queueing phase-3 entries | the drain runs in the finally even on a failed or rolled-back tick | `test_heartbeat_post_tx_contract.py::test_run_post_tx_runs_even_when_the_tick_transaction_fails` | EXISTS |
| post-tx drain on an acquire-failed tick | the pool never handed out a connection; the deque holds earlier ticks' entries | the drain still runs, under a full budget (none was consumed); a failing drain logs, never displaces the acquire failure | `test_seam_audit_pins.py::test_an_acquire_failed_tick_still_drains_the_pending_abandons`, `::test_a_failing_post_tx_drain_does_not_displace_the_tick_failure` | ADDED |
| deferred post-tx under an exhausted budget | the budget ran out mid-tick | the drain defers with the deque intact, never drops | `test_heartbeat.py::test_post_tx_is_deferred_when_the_budget_is_exhausted` | EXISTS |

## 4. The cancel ladder (`CancelController`)

Shared by: `heartbeat_loop` (the only caller of run_in_tx/run_post_tx), the
consumer (the ABANDON_PENDING guard, the CancelledError routing),
`mark_abandoned` (the backend write), the shutdown orchestrator (its own
separate phases).

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| the drain's re-queue after a raised write; the budget cut detaching the write under the shield | the abandon DETACHES, commits server-side, and the except arm re-queues it; the next tick's re-issued abandon reads False (row no longer running), the not-applied arm re-arms at FORCED without delivering the cancellation; the poll's `status = 'running'` filter then reads NONE forever | a budget cut that detaches the drain's write STILL delivers the cancellation: the re-queue path may not bypass the delivery the applied arm owns | none on main (repro rides the fix branch) | MISSING on main - owner `fix/457` |
| not-applied abandon leaves the entry registered and re-armed | `mark_abandoned`'s `cancel_phase = 2` guard misses | phase back to FORCED, a later tick re-issues; no deregister on the False path | `test_rt_cancelwatch_abandon_drain.py::test_abandon_write_failure_does_not_strand_job_at_abandon_pending` | EXISTS |
| the abandon's worker fence | a reclaim moved the row to another holder | the abandon is issued only for a row the worker's own poll returned | `test_rt_cancelwatch_cross_worker_abandon.py`, `::test_phase3_abandon_not_issued_for_job_absent_from_own_poll` | EXISTS |
| same-tick fast path vs heartbeat rollback | both deadlines met in one tick, then the tx rolls back | the cancellation is delivered by the drain AFTER the abandon is durable, first delivery only; a rollback leaves the entry registered and re-armed | `test_rt_cancel_same_tick_abandon_rollback.py` | EXISTS |
| phase-2 re-issue after a rolled-back escalation write | local FORCED, PG still COOPERATIVE | the escalation is re-issued the next tick; `cancel_phase = 1` guard keeps it idempotent | `test_heartbeat_post_tx_contract.py::test_rolled_back_phase_2_write_is_reissued_on_the_next_tick` | EXISTS |
| escalation vs terminal write (no re-issue storm) | the consumer's terminal write lands mid-ladder | the escalation applies at most once; the terminal row spares the ladder | `test_rt_cancelwatch_poll_fencing.py::test_escalation_then_terminal_write_no_reissue_storm`, `::test_stale_cancel_request_on_terminal_row_is_inert` | EXISTS |
| the drain's detector-2 liveness renewal | a bulk cancel drains many entries inside one tick | one liveness tick per entry, with the heartbeat loop's own name and period; a healthy drain never reads as a dead loop | `test_seam_audit_pins.py::test_the_abandon_drain_renews_detector2_liveness_per_entry` | ADDED |
| cancel-origin stamping (OPERATOR vs SHUTDOWN) | the row is the final arbiter | the poll's observation stamps OPERATOR over a SHUTDOWN stamp; the shutdown phases never walk the operator ladder | `test_shutdown_orchestrator.py`, `test_rt_diff_cancel.py` | EXISTS |

## 5. Drains and sweeps

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| shutdown hand-back exclusion core (`drain_local_queue_to_pending`), callers: the DRAINING pass, the producer's exit pass | a claim round commits after the DRAINING pass | held ids excluded; the producer's exit pass is the last write; idempotent across the two passes | `test_shutdown_drain.py::test_drain_excludes_jobs_a_consumer_is_already_executing`, `::test_drain_hands_back_at_most_once_across_repeated_calls`, `test_rt_execloop_ignores_producer_stop.py` | EXISTS |
| the hand-back's cancel fence | an operator cancel lands between the operator's write and the drain | a row with `cancel_phase != 0` is NEVER re-pended by the deploy path; it stays with the ladder | `test_seam_audit_pins.py::test_drain_handback_keeps_the_cancel_fence_in_both_statement_shapes` | ADDED |
| the hand-back's attempt refund | a claim that never reached an actor | the refund (floored, exactly-once across the two passes); a never-ran job cannot reach a terminal state | `test_shutdown_handback_retry_budget.py::test_a_job_handed_back_by_shutdown_keeps_its_full_retry_budget`, `::test_a_job_that_never_ran_cannot_reach_a_terminal_state` | EXISTS |
| the DRAINING both-done turn | the consumer's get TAKES a row as the drain begins | the stale local copy is not executed; the hand-back owns the row | `test_rt_execloop_ignores_producer_stop.py`, `test_rt_execloop_consumer_both_done_drop.py` | EXISTS |
| the isolate join window | a claim lands during isolate_self's join | a live claim is not re-pended; an unmarked residual is handed back, not double-run | `test_rt_isolate_join_window_claim.py` | EXISTS |
| sweep 1 reclaim (budget predicate, cancel-first CASE) | lease lapses with and without retry budget; cancel in flight | operator intent outranks budget; the crashed arm self-describes (`WorkerCrashed`); the re-pend reschedules and wakes | `test_rt_sweeps_ladder.py`, `test_rt_sweeps_parity.py`, `test_sweep_crashed_row_error_fields.py` | EXISTS |
| isolate template parity with sweep 1 | either statement's SET clause drifts | branch-for-branch SET equivalence (shared fragments, one source) | `test_heartbeat.py::test_isolate_self_sweep1_row_state_identical`, `test_leader_property.py` | EXISTS |
| reclaim event feed | the re-pend or the crashed arm writes | every reclaim rides the pollable outbox channel so feed consumers settle | `test_isolate_reclaim_feed.py`, `test_watch_reclaims.py` | EXISTS |
| deadline sweep (heartbeat arm) | a slow-but-live worker near the deadline | the phantom-cancel window: a failed tick retries promptly so the recovery beat lands inside the 2x floor | `test_heartbeat.py::test_one_fast_transient_failure_is_one_miss_retried_promptly` | EXISTS |
| archive / retention sweeps | retention bounds vs live readers | bounded batches, event TTL carve-out for reclaim events | `test_attack_rt_retention_integration.py`, `test_sweepaudit_bounded_writes.py` | EXISTS |
| sweep liveness under long batches | a sweep batch outlasts the staleness budget | `_drain_bounded` renews the detector stamp mid-sweep | `test_drain_liveness.py`, `test_rt_worker_sweep_telemetry.py` | EXISTS |

## 6. State machines

### 6.1 Attempt lifecycle

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| the claim-time attempt increment vs the reconcile's disown (#458) | a round commits its claim, loses the response, the reconcile disowns, the lease lapses, sweep 1's budget predicate sees the charged attempt | a claim that never reached an actor buys nothing: the reconcile refunds the increment while leaving the row for sweep 1; no phantom `job_attempts` row, no never-executed `crashed` | none on main (repro rides `fix/458`) | MISSING on main - owner `fix/458` |
| the attempt ceiling | `attempt = LEAST(attempt + 1, 32767)` at claim | the ceiling holds through the full cycle | `test_attempt_ceiling_full_cycle_pg.py` | EXISTS |
| per-attempt BaseException capture | an actor raises a non-Exception BaseException | truthful attempt outcome, the worker survives; KeyboardInterrupt propagates | `test_attempt_baseexception_capture.py` | EXISTS |
| SystemExit crossing a TASK boundary (#459) | a sync actor's `asyncio.to_thread` task (or the tx path's actor task) ends with SystemExit; `Task.__step` re-raises the pair past the loop's handler | the task boundaries wrap the actor's SystemExit in a carrier the dispatcher unwraps: the attempt records `SystemExit`, the loop survives | none on main (repro rides `fix/459`) | MISSING on main - owner `fix/459` |
| SystemExit on the plain async path | the actor is awaited inside the consumer's own task | the boundary `except BaseException` captures it: truthful `failed` row, loop alive | `test_seam_audit_pins.py::test_async_actor_system_exit_is_an_attempt_outcome_not_worker_death` | ADDED |
| fencing on terminal writes | a reclaim moved the row mid-attempt | fenced writes lose loudly; the loser never relabels the new holder's work | `test_rt_terminal_write_fencing.py`, `test_typed_outcomes_attacks.py` | EXISTS |

### 6.2 Cancel phases

Covered in section 4. The phase enum's PG constraint (`0..2`) and the
in-process-only ABANDON_PENDING sentinel are pinned through the ladder tests
above.

### 6.3 Cron ownership

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| boot revert vs operator intent | a code-owned schedule disabled by whom | an `auto` disable of a code-owned schedule reverts; an `operator` disable never does | `test_cron_ownership_model.py` | EXISTS |
| the mixed-version rolling deploy's unmarked disable (#460) | an old pod's failure UPDATE writes `enabled=false` before `disabled_by` existed: NULL marker, auto-disable fingerprint | a NULL-disabled row WITH the fingerprint (failures at threshold, `last_fire_error` set) reverts like an `auto` row; without the fingerprint it stays (operator intent) | none on main (fix rides `fix/460` with the backfill migration) | MISSING on main - owner `fix/460` |
| the fire's singleton race | two workers fire the same schedule | only the racer strikes; the singleton guard decides in one statement | `test_rt_cron_singleton_parity.py`, `test_cron_loop.py::test_singleton_race_between_preflight_and_insert_strikes_only_the_racer` | EXISTS |
| disable vs fire ordering | the disable write races an in-flight fire | the disable-fire race pins the resolution | `test_rt_cron_disable_fire_race.py` | EXISTS |
| the catch-up window | missed fires across restarts | within the window, catch up; beyond it, skip (never stampede) | `test_cron_loop.py::test_cron_fire_miss_within_catch_up_window_not_skipped`, `::test_cron_fire_miss_beyond_catch_up_window_skipped` | EXISTS |
| the disable's strike ledger | transient vs attributable failures | transient failures raise without striking; attribution is per plan; a cancelled savepoint writes no strikes | `test_cron_loop.py::test_transient_enqueue_failure_raises_without_striking_schedules`, `::test_cancelled_error_mid_savepoint_rolls_back_and_writes_no_strikes` | EXISTS |

## 7. Thread and sync boundaries

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| the sync actor's executor thread vs cancel | `task.cancel()` cannot reach the thread | the consumer parks on the detached thread bounded; the row resolves through the fence | `test_timeout_path_exit_hold.py`, `test_rt_execloop_tx_actor_cancel_detach.py`, `_handlers.py`'s park arm | EXISTS |
| the transactional actor task | the actor runs in a separate task so the tx conn is not nested | the task boundary routes exceptions through the same dispatcher; a cancelled wait discards the buffer | `test_rt_execloop_tx_actor_cancel_detach.py` | EXISTS (SystemExit AT this boundary is #459's gap, see 6.1) |
| `shield_with_retrieval` double-cancel | a second cancel lands while the inner write is detached | the detached outcome is retrieved and logged, never "exception was never retrieved"; a late duplicate is absorbed | `test_shield_with_retrieval.py` | EXISTS |
| watchdog detectors (loop lag, shutdown deadline) | a stalled loop or an over-budget shutdown | force-exit fires with the stack dump; the trip survives raising log sinks | `test_worker_watchdog.py` | EXISTS |

## 8. Redis fallback decision points

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| the fallback's rejection absorption (`ratelimit/_redis_utils`) | READONLY (replica promotion), OOM rejections | the fallback absorbs the rejection and composes through PG; retry budget is not burned by a Redis outage | `test_rt_depfail_ratelimit_acquire.py::test_redis_outage_fallback_composition_runs_actor_via_pg`, `::test_redis_outage_acquire_does_not_burn_retry_budget` | EXISTS |
| slot lease fencing | the redispatched attempt re-acquires a lapsed slot | a release is fenced to the exact lease: it frees nothing rather than stealing the live holder's slot | `test_ratelimit_reservation_fencing.py`, `test_ratelimit_reservation_chaos.py` | EXISTS |
| progress publish fallback | Redis down mid-progress | the PG path carries the buffer; the seq total order holds | `test_progress_redis.py`, `test_progress_seq_total_order.py` | EXISTS |

## 9. Audit and telemetry emission paths

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| admin audit trail (`web/admin/_audit.py`) | the mutation commits and the audit row does not (or vice versa) | the same-transaction shape for admin-owned SQL; warn-mode degradation only post-commit | `tests/web_admin/test_admin_audit_trail.py` | EXISTS |
| cancel phase-transition counter | new attribute pairs | cardinality bounded at 4 timeseries; the pairs stay the documented ones | `test_otel_integration.py`, `test_prometheus_metrics.py` (structure) | EXISTS (weak: the pair SET is documented, not pinned exhaustively) |
| heartbeat gauges and histograms | failed ticks, late beats | the miss counter, the consecutive-failures gauge, and the lock-TTL sample keep their meanings under the gated renewal | `test_heartbeat.py::test_lock_ttl_sample_is_the_lease_minus_the_gap_between_renewals`, `test_attack_heartbeat_ledger.py` | EXISTS |

## 10. Clock anchors

| seam / caller | interleaving | invariant | pin | status |
|---|---|---|---|---|
| the cancel ladder's elapsed (`cancel_observed_at`) | NTP corrections move the wall clock | elapsed is measured on the monotonic loop clock, never `time.time()` | the field contract is documented and the shutdown orchestrator's W-2 guard pins the None arm (`test_shutdown_orchestrator.py`) | EXISTS (partial: no pin constructs a wall-clock jump against the ladder) |
| lease and sweep deadlines | worker clock skew | the comparisons are server-side (`clock_timestamp()`); the same clock that stamped the lease judges it | `test_heartbeat.py` (the gated statement's server-side threshold), `test_rt_sweeps_*` | EXISTS |
| harness seed aging | timing-sensitive choreography | seeds anchor to the PG clock instead of wall-clock sleeps | `tests/test_rt_458_*.py` (fix branch), the sweep pins' seeding discipline | EXISTS |

## Tallies and what remains

- Seam families enumerated: 10 (registries, pool/conn ownership, heartbeat
  tick, cancel ladder, drains and sweeps, state machines, thread/sync
  boundaries, Redis fallback, audit/telemetry, clock anchors); 43 audited
  (seam, invariant) rows in the tables above.
- EXISTS: 30. MISSING: 8. ADDED: 5.
- The MISSING rows split two ways:
  - Owned by the live fix branches (5): #457 (the drain's re-queue
    bypassing the cancellation delivery), #458 (the reconcile's unrefunded
    claim-time attempt charge), #459 (SystemExit across a task boundary),
    #460 (the rolling deploy's NULL-disabled cron row), #461 (the bare-id
    deregister; repro committed red on `cf15fb29`).
  - Unowned (1 honest remainder): the `resolve_claim`/`mark_claimed` bare-id
    intent mutation (section 1.1). Same class as #461, reachable through the
    same re-claim overlap: a stale attempt's `resolve_claim` inside the live
    attempt's take-to-register window unfences the row against every
    hand-back and reconcile pass. A pin written today is red on main, so it
    belongs with the #461 fix's shape: make the intent hand-back
    identity-scoped the way `deregister` becomes, then pin the intent twin
    of `test_stale_attempt_exit_evicts_live_attempt` next to it in
    `tests/test_cancel_registry.py`.
- The five ADDED pins live in `tests/test_seam_audit_pins.py`. Each was
  mutation-verified: the fence pin fails when the drain's
  `cancel_phase = 0` conjunct is dropped, the acquire-failure pins fail when
  the drain is gated on tick success or when the drain's error is allowed to
  displace the tick's, the liveness pin fails when the drain skips its
  per-entry `tick`, and the SystemExit pin fails when the escape reaches the
  consumer's task boundary uncaught. All green on `cf15fb29`; no sleeps, no
  real PG required.
- Honest limits: the matrix's EXISTS verdicts are grep-and-read verdicts
  over the named test files, not re-runs of every cited suite on this
  commit. The cited names were verified to exist with the asserted shape;
  running the full 818-file suite on every row was out of scope for this
  pass. The two weak EXISTS rows (the phase-transition counter's pair set,
  the ladder's monotonic anchor) are marked as such rather than padded.
