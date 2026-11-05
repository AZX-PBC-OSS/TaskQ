# The friction map

What the LedgerWatch demo shows and what to watch when building a
similar adoption. The app runs strictly on `docs/` + `examples/` +
docstrings (the library's `src/` is never read), and every workaround
lives in the app - an adopting team files friction; it never patches
the framework.

## What the demo shows

- The full adoption shape: ingest → enrich (a fan-out over a flaky
  external API) → branch by risk → human approval for HIGH-risk
  transactions (the `Expired` arm escalates to a compliance queue) →
  auto-clear for the rest → a typed completion report, plus a
  scheduled reconciliation pass on a durable timer.
- The adverse world as data: ~15% transient failures, ~5% hangs, 50-300ms
  jitter, SIGKILL drills mid-fan-out and mid-approval, re-delivered
  feed batches (the profile lives in `config.py`).
- Diagnosis from rows alone: every drill's question is answered from
  `jobs`, `wf_step_ledger`, `wf_signals`, `wf_node_stream` - the
  runbook's order holds.
- The machinery's clean faces: the run-key arbiter lands
  duplicate-feed re-deliveries on the first run's typed claim (zero
  double-effects); the typed `Expired` member forces the fail-close arm
  at the checker; the map fan-out collects 1000 children with the
  journal and the ledger agreeing per transaction; the validators'
  E16 discriminator law catches a real mispick risk at build; the
  one-actor-one-queue law refuses a mis-wired cohort loudly at boot.

## Where the app pays friction tax (watch these)

- **The map's join needs an identity pass-through before a route**:
  `route(enriched, arms)` over a `map_source`'s derived join is refused
  at build ("a route attaches to a wired source node"). The app spells
  an identity STEP between the map and the route (`app.py`,
  `branch_body`); the constraint lives only in the error message.
- **One review run per HIGH transaction**: the hold identity carries
  the map index (pinned in `tests/test_wf_holdmap_cures_pins.py`), so
  concurrent per-item holds on one node key are addressable and
  individually resolvable; the app still chooses one review run per
  transaction - every hold gets its own node, the run id is the
  operator's address, and the review run is idempotent by the run key
  under the feed's re-delivery.
- **The item ladder honors the stamped `max_attempts=`** (pinned in
  `tests/test_wf_holdmap_cures_pins.py`); the app also keeps its own
  in-body retry policy as the cheap in-attempt absorber.
- **A substep on a hot fan-out path costs throughput**: the substep
  releases the pool connection across the body (pinned in
  `tests/test_wf_holdmap_cures_pins.py`), but this app's journal
  receipt is still cheaper than a ledger row, so the enrichment call
  stays un-ledgered.
- **A page's route fan is safe to ~150 classified elements solo**;
  under multi-page concurrency the feed pages its deliveries one at a
  time (each page's run reaches terminal before the next page fires).
  The zero-loss invariant is per document across the pages.
- **Tuning the fleet**: a warn budget below the check interval fails
  validation at boot (pinned in
  `tests/test_wf_holdmap_cures_pins.py`); the demo's values
  (`tests/support.py`'s FLEET_TUNING) keep the invariants
  `warn > check` and `budget > warn`.

## Doc gaps an adopter feels

- The workflow node's retry backoff curve is undocumented (the vanilla
  actor's is in `docs/guides/retries.md`); timing budgets come from
  observation, not the docs.
- The hold's status vocabulary (`held` / `delivered` / `abandoned`) has
  no doc table; the guides say "pending" where the rows say `held`.
- There is no durable-timer verb: the durable timer is a HOLD whose
  DB-clock deadline is the tick, plus author work - arm the schedule
  once via `ctx.substep`, enforce it on every wake, match the `Expired`
  member (`app.py`'s reconciliation flow is the recipe).
- The `wf_signals` / `wf_step_ledger` / `wf_node_stream` tables are
  operator-visible but doc-invisible; the guides' SQL examples name
  `jobs` but never the wf_* tables.
- The operator's display read is taught from a private module path
  (`taskq.workflows._progress_read`); re-home it public or re-export it.
