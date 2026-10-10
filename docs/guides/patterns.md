# The pattern catalog

The workflow patterns in this catalog are industry-common nouns — the
shapes orchestration systems of every flavor express, whatever they call
them locally. This page maps each pattern to TaskQflow's mechanism and
to the runnable example that shows it. The honest comparative claim:
**these are the patterns the ecosystem's orchestrators express; here is
one mechanism per pattern** — and where the mechanism is ONE row
carrying five of them at once, that is said plainly, because it is the
design's best idea.

## The headline: one row, five mechanisms

| Pattern | The common shape | TaskQflow's mechanism | Where to see it |
|---|---|---|---|
| **signal-and-wait** | a running procedure pauses until a typed value arrives from outside | `ctx.wait_signal((Approval, ...), timeout_s=...)` — the wait's outcome is the closed union `Payload \| Expired` | [Agent fleets](agent-fleet.md) §the minimal gate |
| **suspend-resume** | a node suspends the whole run and a separate door resumes it by id | the HOLD: a `wf_signals` row; `HitlClient.resolve(hold_id, decision)` — one CAS, idempotent (the second resolve is the defined no-op) | the same fence, verified end to end |
| **deferrable-open-slot** | the suspended node frees its executor; nothing parks a thread waiting | the held node's `locked_by_worker` is NULL — the worker moved on; resume consumes NO retry attempt (awaited ≠ failed) | the hold pins: `tests/test_wf_hitl_pins.py` |
| **checkpoint-interrupt** | a body interrupts mid-flight; on resume the state is re-derived, not re-run from nothing | the hold row + the step ledger: the replayed body re-executes from the top and the ledger's memo returns the SAME results (side effects run once across attempts; delivered answers are consumed in epoch order) | `tests/test_wf_hitl_pins.py` (the replay pins), the checkpoint-replay probe |
| **poll-until-true** | a wait with a deadline: check, sleep, re-check — converge by knock or by poll | the same hold row carries the deadline: the expiry sweep owns the timeout arm, the NOTIFY broadcast is the knock, the row is the poll's truth | the broadcast pins: `tests/test_wf_hitl_pubsub_pins.py` |

Five patterns that the ecosystem builds as five separate mechanisms
(a signal queue, a suspend primitive, a deferral scheduler, a
checkpoint store, a sensor loop) reduce here to **one row shape** —
CAS'd on delivery, idempotent, slot-releasing, deadline-bearing. The
compile sees them too: a declared gate (`GateDecl`) renders the hold as
a graph node, and an eternal wait is a build error, not a production
surprise.

## The other patterns

| Pattern | The common shape | TaskQflow's mechanism | Where to see it |
|---|---|---|---|
| **map-reduce** | fan N items out at runtime cardinality, reduce when all settle | `map_source(source, body)` — forks at the source's finalize; the derived `<key>.join` collects `list[R]` and fires EXACTLY ONCE (the transactional outbox) | [Event pipelines](event-pipeline.md) §fan-out with a join; the dynamic-map probe (7 items → 7 child rows, the join once) |
| **router / dead-letter-route** | classify each record, send each outcome somewhere TOTAL — including failure | the chain's `Route`: typed enum outcomes, `DONE`, the dead-letter arm; an outcome with no arm is the loud `RouterNotTotal` at COMPILE | [Event pipelines](event-pipeline.md) §the typed route (verified fence: record `b` dead-letters, nothing drops) |
| **retry-ladder** | bounded attempts with backoff, per unit of work | per-node `max_attempts` + the PER-ITEM ISOLATION: a map child re-runs alone (siblings and succeeded items never re-run); infra fault never burns the ladder | [Task stacks](task-stacks.md) §retries across steps; the reschedule probe |
| **saga / compensation** | multi-write flow with an undo walk on failure | NOT a typed surface yet — the honest state: the step ledger records every terminal outcome a compensator needs, and the cancel cascade already walks REVERSE edges; compensation steps are hand-rolled today. The typed verb (`on_failure="compensate"`) is recorded backlog, not shipped API | [Agent fleets](agent-fleet.md) §where the limits are |
| **deterministic-replay recovery** | recover by re-executing the orchestrator code deterministically | DIVERGENT BY DESIGN — there is no orchestrator process to replay: recovery RE-DERIVES the status from rows alone. Bodies may use `random`, wall-clock, threads — no determinism contract | the rows-only reconstruction pins; the compile's byte-stability probe |
| **the trigger-idempotent run** | the same logical trigger twice starts one run | `create_flow(run_key=...)` / `run(..., key=...)`: the typed `RunClaim` — `created` / `existing-running` / `existing-terminal` (the refused-to-reuse verdict, stated loudly) | [Task stacks](task-stacks.md) §the two-claims law (verified fence: same key twice → one run; the terminal replay answers 409-shaped) |
| **the exactly-once sink** | downstream side effects land once despite crashes and retries | the transactional outbox trio: the UNIQUE join fire, the outbox drained with idempotent consumer inserts, the step-ledger memo | the outbox pins: `tests/test_wf_outbox_retention_pins.py`; the exactly-once probe (two redundant sweeps moved nothing) |
| **cursor-paged streaming** | consume an unbounded feed page by page; a crash re-emits exactly the lost page | `ctx.emit_batch(children, cursor=...)` — the children + the edges + the cursor commit as ONE transaction; the resume continues from the last COMMITTED page | [Event pipelines](event-pipeline.md) §the cursor |
| **the stuck-running ghost** | a run sits `running` forever after a process dies | STRUCTURALLY IMPOSSIBLE here, and that is a property, not a feature request: the status is DERIVED from rows on the DATABASE clock (a +1h-skewed app clock cannot fire or unfire a budget — the skew pin); a lease expiry re-claims the row. There is no status column to go stale and no in-process state to orphan | [Task stacks](task-stacks.md) law 2; the skewed-clock pin |
| **human-in-the-loop gate** | a pipeline pauses for an approval, minutes or days | the same hold row — the budget PAUSES while held (a human's thinking time is not the run's spend); the faces: the admin's Resolve form, the per-user SSE board, `HitlClient`, the CLI's `flows resolve` | [Agent fleets](agent-fleet.md) (the whole worked pattern) |
| **backpressured fan-out** | a fast producer must not outpace its workers without bound | the declared `max_in_flight=` per workflow: the admission fence (in-tx advisory lock + bounded poll), the wide-page refusal, the ladder-owned stall. Observability degrades FIRST (coalesced emissions, the drop on the record); correctness never | [Event pipelines](event-pipeline.md) §backpressure |

## What is deliberately NOT here

- **Event-time windows** (tumbling/sliding aggregation over event
  clocks). One clock domain — the database's — is a design position,
  not an oversight: deadlines, budgets, and leases evaluate IN-DB, which
  is what kills the stuck-running ghost. The loop + `gather` + the
  read-side `aggregate=` recipe covers the real cases; a stream
  processor composes beside.
- **The asset graph** (versioned materialized datasets with IO
  managers). Data moves as typed payloads riding job rows; the lineage
  is `wf_edge`, the backfill safety is the run key. No catalog, no
  partitioned materializations — a documented trade.
- **Arbitrary mid-graph `goto`.** Dynamic control flow is the loop's
  typed `Done`/`Refine` union — an unchecked cycle is the failure mode
  this architecture declines.

## The map's own evidence

The correspondence above is not asserted from the armchair — the
internal campaign's pattern probes ran each EXPRESSED claim against a
live database (the hold's slot release, the replay's once-only memo,
the dynamic map's cardinality, the outbox's idempotence, the run key's
arbiter) and the captures ship with the source under
`.measurements/patterns/`. The user-facing claim stays the modest one:
**one mechanism per pattern, one row for five of them, and the limits
named in the same breath as the wins.**
