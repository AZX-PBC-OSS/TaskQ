# Docs audit — a fresh-dev pass at the recurring-refresh face

Written by the fresh-dev probe that built `examples/gaugefleet/` from the
guides ALONE (library `src/` never read). Every item below was a friction
actually hit while building; line numbers are this repo's docs at the branch
head. Evidence for each is in this directory (the drill captures).

## Frictions

1. **The "durable wait" is three mechanisms, and no page names the family.**
   A recurring-refresh integrator looks for a single "wait durably, then
   resume" primitive. The docs' answer is real and good —
   `raise Snooze(delay)` (self-continuation), a `scheduled_at` successor
   enqueue (`docs/guides/ops.md:1028-1035`, "The job queue is your timer"), and
   cron (`docs/guides/cron.md`) — but §7 of ops.md frames these under
   rate-limit handling, and the "three come back later signals" list
   (`docs/guides/ops.md:987`) is where I landed only after reading the whole
   guide. A short "durable waits" section (the three mechanisms, a chooser
   table like §5's dedup one) would have saved the detour.

2. **A `singleton=True` actor cannot self-schedule its own successor — not
   warned where singletons are documented.** The running tick IS an active job
   of the actor, and "Active means `pending`/`scheduled`/`running`"
   (`docs/guides/actors.md:584`) means the successor's enqueue deterministically
   collides with itself (preflight step 3, `docs/guides/jobs-clients.md:322`).
   I derived this only from the evaluation-order section; the singleton section
   (`docs/guides/actors.md:574-596`) should carry the warning plus the two
   escapes (app-level guard for chains, or cron's suppress-not-fail path,
   `docs/guides/cron.md:571-576`).

3. **Leader failover latency after a worker death is documented in the wrong
   place for the question I had.** `docs/guides/jobs-clients.md:54` promises
   the leader promotes `scheduled_at` jobs every ~1 s — but after a worker is
   SIGKILLed, takeover waits out the dead leader's lease
   (`TASKQ_LEADER_LEASE`, documented only in
   `docs/guides/configuration.md:174`). My first drill run had a scheduled
   successor sit unpromoted past its whole 30 s window; the harness now waits
   for the `maintenance_leader` row to name a live worker. A cross-reference
   from the promotion claim to the leader-lease knob (and a "leader failover"
   paragraph in `docs/guides/maintenance-sweeps.md`) closes the gap.
   Measured: with `TASKQ_LEADER_LEASE=15`, takeover + promotion landed within
   the drill's waits (see `overlap.txt`, `timer.txt`); at the 40 s default the
   same window timed out.

4. **`TASKQ_LOCK_LEASE` is one invariant in the quick start, three at boot.**
   The quick start pins the failed-beat cascade formula
   (`docs/getting-started/quick-start.md:98`). Boot validation adds two more
   the guides don't list together: `cancellation_grace_period +
   cleanup_grace_period < lock_lease` and `watchdog_loop_lag_budget +
   heartbeat_interval < lock_lease` (both surfaced only as
   `MultipleValidationErrors` at worker boot). A single "everything that must
   fit inside the lock lease" table in `docs/guides/workers.md` saves a boot
   cycle per discovery.

5. **`taskq job show` does not print `tags`.** The operator-facing field list
   (`docs/guides/cli.md:667-722`) omits the one column that groups a recurring
   run's ticks; my admin-view capture had to read tags through the client
   list/SQL instead. (Tags are documented as filterable everywhere else —
   e.g. `docs/guides/jobs-clients.md:1345+` — so the omission reads as an
   oversight, not a decision.)

6. **No "display identity for a recurring run" recipe.** TaskQ's operator
   identity surface is job id / actor / tags / idempotency key / metadata
   (there is no per-run step or map concept, and none is needed). What's
   missing is the one-paragraph convention: tag every tick with a run tag,
   put the cycle number in a per-tick tag and the run namespace in the
   idempotency key, and the admin UI's filters + `cancel_where` become the
   operator's run view (that convention is what `examples/gaugefleet/`
   implements). `docs/guides/ops.md` §5 or `docs/guides/cron.md` is its home.

7. **Cross-run key reuse dedups silently against `succeeded` rows.** Re-running
   my harness with stable keys hit `enqueue_deduplicated` (a WARNING in the
   worker log) and returned success — the second run no-op'd. The docs warn
   keys are status-blind (`docs/guides/ops.md:723`) and the discipline is
   right; the gap is observability: a dedup hit is easy to miss in a busy log.
   (The docs' `idempotency_scope` suggestion — encode the window in a scope —
   is the fix I shipped: a per-run namespace in every key.)

8. **Live progress on the no-Redis transport may carry only first and last
   events for a fast job.** Documented as coalescing ("Postgres retains only
   the most recent snapshot", `docs/guides/progress.md:59-77`, and the
   Redis-optional note at `docs/guides/progress.md:11-22`), and the durable
   snapshot (`progress_state`/`progress_seq`) is the receipts' real carrier —
   but a reader wiring `progress_stream()` for a sub-second job should be told
   flatly that intermediate phases may never appear live. (My capture: 2 live
   events for a cycle whose durable snapshot ended at `progress_seq=6`.)

## What the docs got right (kept honest)

- Pattern A's cursor chain (`docs/guides/ops.md:559-635`) built the example's
  cursor + fan-out exactly as documented — including the fresh-key-dimension
  rule for self-continuation successors (`docs/guides/ops.md:730`), which
  prevents a forked chain by construction.
- The scheduled re-enqueue pattern (`docs/guides/ops.md:1028`) is exactly the
  durable timer the drills pinned, including its crash-safety argument.
- The dedup chooser table (`docs/guides/ops.md:710-731`) is what pointed the
  overlap guard at the app-level running-run guard rather than `singleton`.
- `SingletonCollisionError`'s fields and semantics
  (`docs/api-reference/exceptions.md:35`) matched the drill's refusal exactly.
- The cron docs' suppressed-not-failed classification
  (`docs/guides/cron.md:571-576`) is precise and quotable; the example ships no
  cron schedule (the chained timer is the periodic trigger), so the cron
  suppression path itself is NOT exercised by these drills.

## Scope note

The drills run against a real Postgres + real worker subprocesses at the
branch head; every PASS line in `*.txt` is a measured claim, and the run's
stdout (63 PASS / 0 FAIL) is in `run-all.log`.
