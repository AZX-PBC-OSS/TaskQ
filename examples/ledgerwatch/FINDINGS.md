# LedgerWatch adoption — THE FINDING LEDGER

The adopting team's friction list: every misleading doc, every error
that didn't teach, every capability gap, every API that fought — with
the evidence. This is the deliverable; the greens are earned, the reds
are reported.

The team adopted STRICTLY from `docs/` + `examples/` + docstrings: the
library's `src/` was never read (the readiness test). Every finding
below was hit in flight, then worked around IN THE APP (an adopting team
files friction; it never patches the framework).

---

## F1 — the typed route cannot attach to a map's join (a missing verb shape)

- **What fought**: `route(enriched, arms)` over a `map_source`'s derived
  join is REFUSED at build: "route's source 'src.join' declares no body
  — a route attaches to a wired source node". The natural
  enrich-then-branch pipeline — ingest → `map_source(enrich)` →
  `route(branch_by_risk)` — cannot be spelled directly.
- **The workaround** (`app.py:215`, `branch_body`): an identity
  pass-through STEP between the map and the route — the honest spelling
  of "the route's source is the enriched collect", but boilerplate the
  author must know to write.
- **The doc gap**: `docs/api-reference/workflows.md` (the `route` row)
  does not state the attach constraint; it is stated only in the error
  message. Teach it at the `route` verb's entry.
- **Severity**: minor (the workaround is clean; the error teaches well).

## F2 — concurrent HITL holds from map siblings collide (the promise's edge)

- **What fought**: a hold's identity is
  `(workflow_id, node_key, signal_name, hold_epoch)` — map SIBLINGS of
  one step share `node_key` and hold under the same epoch, so two
  HIGH-risk transactions holding CONCURRENTLY in one map collide on ONE
  hold row. Observed BOTH faces:
  - in-process (`FlowRunner.drive`): one hold row; one answer wakes one
    sibling (the ledger shows it `succeeded` on the resume); the other
    sibling stays `running` FOREVER — the run wedges (the probe's
    `drive` returned `max_ticks`; `src.join` pending; `report`
    `deps_pending=1` forever).
  - worker mode: the second sibling's hold INSERT raises
    `UniqueViolationError` (`wf_signals_hold_identity_uniq`) INTO the
    ladder; the retry then consumes the OTHER sibling's delivered
    answer.
- **The broken promise**: `HitlClient.list`'s docstring — "a run with
  MULTIPLE concurrent holds lists ALL of them distinctly" — holds for
  holds on DISTINCT nodes; map siblings are one node key, and the
  promise's edge is undocumented.
- **The product's cure** (`app.py:230`, `queue_for_review` + the
  `txn_review` workflow): ONE review run per HIGH transaction — every
  hold gets its own node; the run id is the operator's address; the
  review run is idempotent by the run key under the feed's re-delivery.
- **The framework cure owed**: either the hold identity gains the map
  index (concurrent per-item holds — the HITL-per-item shape any
  moderation/approval pipeline needs), or the docs state the
  "one hold per (node, epoch)" law LOUDLY and the route/map docs teach
  the per-item-run cure.
- **Severity**: MAJOR — the natural per-item HITL shape wedges a run
  silently. The wedged sibling's record (a `running` row on a live-looking
  flow) is the worst failure mode an operator can be handed.

## F3 — the map item's ladder ignores the wired `max_attempts`

- **What fought**: `map_source(source, body, max_attempts=7)` stamps the
  child rows with `max_attempts=7`, but the item ladder terminal-failed
  after attempt 3 (the flake drill's first run: a child dead at attempt
  3 of 6 — budget remaining). The ISOLATED always-fail probe: a plain
  `step(max_attempts=7)` ladders 7 times (the ledger's 7 attempt rows);
  a map item wired 7 ladders 3.
- **The workaround** (`app.py:181`, `enrich_item`): the client's OWN
  retry policy in the body (each retry journalled as its own call) —
  realistic for external calls, but the framework's knob is a lie on
  the row.
- **Severity**: MAJOR — at a 15%+5% adverse rate, the effective 3-budget
  kills ~0.8% of items per attempt window; a batch's run FAILS on items
  that "never die of flakiness" only because the product re-implemented
  the ladder.

## F4 — the substep's ledger claim serializes concurrent map children

- **What fought**: wrapping the enrichment call in
  `ctx.substep("enrich:{txn_id}", …)` (the exactly-once replay face the
  docs teach for "pre-wait side effects… replay cheap") throttled a
  300-child fan-out EIGHT-fold: max in-flight 32 → 4; wall clock 10.1s →
  27.1s (the A/B measured on the same estate, same profile).
- **The workaround** (`app.py:181`): the call is NOT substep-ledgered —
  the enrichment tolerates the at-least-once boundary (the verdict is a
  pure function of the transaction); the journal (the app's own table)
  provides the receipt instead.
- **Severity**: moderate — the tax is real and silent; a substep on a
  hot path is the natural first move and it quietly eats the fan-out.

## F5 — the workflow node's retry backoff curve is undocumented

- **What fought**: the vanilla actor's retry machinery is richly
  documented (`docs/guides/retries.md`: the algorithms, the jitter, the
  caps); the workflow node's ladder (what a re-pended attempt waits,
  what the observed sub-second doubling was) has NO doc an adopter can
  plan a drill's wall clock against. The drills' timing budgets were
  set by OBSERVATION, not by the docs.
- **Severity**: minor-to-moderate (planning blindness, not a correctness
  issue).

## F6 — the watchdog's warn-budget below the check interval warning-storms

- **What fought**: tuning the fleet tight
  (`WATCHDOG_CHECK_INTERVAL=0.5`, `LOOP_LAG_WARN_BUDGET=0.4`,
  `LOOP_LAG_BUDGET=1.0`) passes the cross-field validation (budget >
  check; warn < budget) and then warning-STORMS: the warn budget below
  the check interval warns on every healthy beat, and EACH warn dumps
  every live asyncio task — the dump itself blocks the loop, a
  self-sustaining storm. Observed: 295 warn+dump pairs, the worker's
  heartbeat starved past the fleet's readiness timeout (the log's event
  census: 891 warnings, 29 infos, the boot lines the last INFO events).
- **The cure on our side**: the warn budget ABOVE the check cadence
  (`check=1.0, warn=1.5, budget=2.0`, `support.py`'s FLEET_TUNING).
- **The framework cure owed**: validate `warn_budget >= check_interval`
  (the same lattice that already catches `budget <= check`), or rate-
  limit the task dumps.
- **Severity**: moderate — a mis-tuned (but VALIDATED) config starves a
  worker silently; the operator sees a warning STORM, which is at least
  loud.

## F7 — the display read face is taught from a private module path

- **What fought**: the progress guide teaches
  `from taskq.workflows._progress_read import keyed_display, run_display`
  — the run display (the operator's data contract) lives under a
  private underscore path. The app imports it as documented
  (`display.py:16`).
- **Severity**: minor (it works; the path says "private" while the docs
  bless it — re-home it public or re-export it).

## F8 — the hold's status vocabulary is undocumented

- **What fought**: the drills read the hold rows (the rows-alone law);
  the waiting hold's status is `held`, a resolved hold's is `delivered`,
  an expired one's `abandoned` — none of the three spellings is
  documented (the guides say "pending" for the wait and "abandoned" for
  the expiry; the ROWS say `held`). The drills' first assertions red on
  the vocabulary before the rows taught it.
- **Severity**: minor (a doc table of the signal row's states).

## F9 — there is no durable-timer composite; it is authored from the hold's expiry

- **What fought**: the brief's "durable timer (`wait_until`)" does not
  exist as a verb; the durable timer is a HOLD whose DB-clock deadline
  is the tick (well-documented as the HITL deadline face) PLUS author
  work: arm the schedule once via `ctx.substep` (a resume replays it —
  no drift), ENFORCE the schedule on every wake (an early nudge waits
  again — the body's while loop, `app.py:352`), match the `Expired`
  member (the normal face — the timer's tick IS the expiry).
- **Severity**: minor (the primitives are right and documented; the
  composite deserves a named recipe — this guide is one).

## F10 — the estate's row-level tables are operator-visible but doc-invisible

- **What fought**: the reconciliation pass and the drills read
  `wf_signals` / `wf_step_ledger` / `wf_node_stream` by name (the
  rows-alone law: the operator's SQL is legitimate); the TABLE NAMES are
  discoverable only from the schema or the pin inventory, not from the
  guides. A global hold inventory (the recon pass's `pending_holds`) has
  NO sanctioned read at all (`HitlClient.list` is per-run).
- **Severity**: minor (the tables are stable; the guides' SQL examples
  already name `jobs` — the wf_* tables deserve the same).

---

## The earned greens (what the machinery delivered without a fight)

- The RUN-KEY ARBITER: the duplicate-feed drill's re-deliveries (mid-flight
  and post-terminal) land on the FIRST run's typed claim — zero double-effects,
  proven per drill.
- The TYPED EXPIRY (T26's amendment): the `Expired` member forces the
  fail-close arm at the checker; the escalation face is a VALUE the body matches.
- The two-transaction finalize + the sweep arms: the SIGKILL drills' resume
  proofs — children carried past attempt 1 by the reclaim, the join fired
  exactly once per drill, the flow terminalized truthful.
- The BOOT REFUSALS: the one-actor-one-queue law refused a mis-wired cohort
  loudly at boot (the error named the cure) — the drift never reached a row.
- The MAP FAN-OUT: 1000 children forked, dispatched and collected with the
  journal and the ledger AGREEING per transaction (one applied verdict each).
- The validators: E16's discriminator law caught a real mispick risk at
  build; the zero-false-positive budget held on every clean graph.
- The rows-alone diagnosis: every drill's question answered from the rows
  (`jobs`, `wf_step_ledger`, `wf_signals`, `wf_node_stream`) — the runbook's
  order held.
