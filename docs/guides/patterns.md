# The pattern catalog — the feature-parity map

The adopter's question is "does it have what I'm leaving?" — this page
answers it per feature, honestly: **the pattern (as the ecosystem's
orchestrators name it) → the face here → the runnable example**. Where
the honest answer is "not built", that is said in the same row, with the
current answer named. The landing faces are pinned by tests that can
fail; every claim links its guide section and its example.

## The headline: one hold row, five mechanisms

Five patterns that the ecosystem builds as FIVE separate mechanisms —
the Temporal signal, the Argo suspend, the Airflow deferrable, the
Airflow sensor, the LangGraph interrupt — reduce here to **one row
shape**: CAS'd on delivery, idempotent, slot-releasing, deadline-bearing.

| Pattern | As the systems name it | The face here | Where to see it |
|---|---|---|---|
| **signal-and-wait** | Temporal-style signals — typed, mid-run delivery | `ctx.wait_signal((Approval, ...), timeout_s=...)`; the outcome is the closed union `Payload \| Expired`; delivery is ONE CAS by hold id, idempotent (the second resolve is the defined no-op) | [Agent fleets](agent-fleet.md) §the minimal gate (verified fence); the hold pins `tests/test_wf_hitl_pins.py` |
| **suspend-resume** | Argo's suspend — the run parks, a separate door resumes it | the HOLD: a `wf_signals` row; `HitlClient.resolve(hold_id, decision)` validates the payload against the gate's declared models BY SHAPE | the same fence; the deliver CAS pins |
| **deferrable-open-slot** | Airflow-style deferrable — the slot is never parked | the held node's `locked_by_worker` is NULL — the worker moved on; resume consumes NO retry attempt (awaited ≠ failed) | the hold pins; the deferral probe (slot RELEASED while held) |
| **poll-until-true** | Airflow-style sensor — check until poke-true, with a deadline | the same row carries the deadline: the expiry sweep owns the timeout arm, the `pg_notify` broadcast is the knock, the row is the poll's truth | the broadcast pins `tests/test_wf_hitl_pubsub_pins.py` |
| **checkpoint-interrupt** | LangGraph-style interrupt + resume | the body replays from the top on wake; the step ledger's memo returns the SAME results (side effects once across attempts); delivered answers consumed in epoch order | the hold pins' replay tests; the checkpoint-replay probe (memo executed once across two attempts) |

A declared gate (`GateDecl`) makes the hold compile-visible: it renders
as a graph node, the admin's Resolve form knows the payload's shape, and
an eternal wait is the W1 validate WARNING at build (declare the
deadline explicitly), not a production surprise.

## The rest of the parity table

| Pattern | As the systems name it | The face here | Where to see it |
|---|---|---|---|
| **map-reduce** | Prefect-style dynamic map (runtime fan-out cardinality) | `map_source(source, body)` — forks at the source's finalize; the derived `<key>.join` collects `list[R]` and fires EXACTLY ONCE (the transactional outbox) | [Event pipelines](event-pipeline.md) §fan-out with a join; `examples/workflows.py`; the dynamic-map probe (7 items → 7 children, the join once) |
| **typed graph wiring** | Dagster-style op/job split — the unit typed, the placement separate | `@app.workflow` + `step(...)`: the promise arguments ARE the edges; `actor=`/`queue=` place them; the E-rules refuse the wrong shapes at COMPILE (both pinned checkers) | [Task stacks](task-stacks.md) §the wiring; the type-probe gate (38 markers red on both checkers) |
| **router / dead-letter-route** | the content router with a dead-letter arm | the chain's `Route`: typed enum outcomes, `DONE`, the dead-letter arm; an outcome with no arm is the loud `RouterNotTotal` at compile | [Event pipelines](event-pipeline.md) §the typed route (verified fence: record `b` dead-letters, nothing drops) |
| **retry-ladder** | Prefect-style retries — per unit, not per flow | per-node `max_attempts` + the PER-ITEM ISOLATION: a map child re-runs alone (siblings and succeeded items never re-run); infra fault never burns the ladder (the reschedule rule) | [Task stacks](task-stacks.md) §retries; the reschedule probe (attempt 2 replayed the memo, the operator never re-answered) |
| **saga / compensation** | Temporal's saga samples — the undo walk on failure | **NOT BUILT as a typed surface** — the honest state: the step ledger records every terminal outcome a compensator needs, and the cancel cascade already walks REVERSE edges; compensation steps are hand-rolled today. The typed verb is recorded backlog, not shipped API | [Agent fleets](agent-fleet.md) §where the limits are |
| **deterministic-replay recovery** | Temporal's deterministic replay | DIVERGENT BY DESIGN — no orchestrator process exists to replay: recovery RE-DERIVES the status from rows alone, and bodies may use `random`, wall-clock, threads (no determinism contract, no SDK sandbox) | the rows-only reconstruction pins; the compile's byte-stability probe |
| **the trigger-idempotent run** | Temporal's cron workflow (one run per slot) | `create_flow(run_key=...)` / `run(..., key=...)`: the typed `RunClaim` — `created` / `existing-running` / `existing-terminal` (the refused-to-reuse verdict, stated loudly) | [Task stacks](task-stacks.md) §the two-claims law (verified fence); the run-key pins `tests/test_wf_attack_runkey.py` |
| **the exactly-once sink** | Flink/Kafka's exactly-once semantics | the transactional outbox trio: the UNIQUE join fire, the outbox drained with idempotent consumer inserts, the step-ledger memo | the outbox pins `tests/test_wf_outbox_retention_pins.py`; the exactly-once probe (two redundant sweeps moved nothing) |
| **cursor-paged streaming** | the Kafka consumer's committed offset | `ctx.emit_batch(children, cursor=...)` — children + edges + the cursor commit as ONE transaction; a crash re-emits exactly the lost page | [Event pipelines](event-pipeline.md) §the cursor; the emit pins `tests/test_wf_attack_emit.py` |
| **the stuck-running ghost** | Prefect's stuck-`running` class (the row that never resolves) | STRUCTURALLY IMPOSSIBLE here — a property, not a feature request: the status is DERIVED from rows on the DATABASE clock (a +1h-skewed app clock cannot fire or unfire a budget — the skew pin); lease expiry re-claims | [Task stacks](task-stacks.md) law 2; the skewed-clock pin |
| **human-in-the-loop gate** | the LangGraph interrupt behind a UI | the same hold row — the budget PAUSES while held (a human's thinking time is not the run's spend); the faces: the admin's Resolve form, the per-user SSE board, `HitlClient`, the CLI's `flows resolve` | [Agent fleets](agent-fleet.md) (the whole worked pattern); `examples/deep_research.py` |
| **backpressured fan-out** | the credit-based backpressure (Flink) | the declared `max_in_flight=` per workflow: the admission fence (in-tx advisory lock + bounded poll), the wide-page refusal, the ladder-owned stall. Observability degrades FIRST (coalesced emissions, the drop on the record); correctness never | [Event pipelines](event-pipeline.md) §backpressure; the backpressure pins (the unbounded variant's captured red) |
| **windowed aggregation** | Flink/Kafka's event-time windows | **DECLINED, deliberately**: one clock domain (the database's) — the loop + `gather` + the read-side `aggregate=` recipe covers the real cases; a stream processor composes beside | [Event pipelines](event-pipeline.md) §the windows |
| **the asset graph** | Dagster's versioned, materialized assets | DIVERGENT: data moves as typed payloads riding job rows; the lineage is `wf_edge`, the backfill safety is the run key; no catalog, no partitioned materializations — a documented trade | [Task stacks](task-stacks.md) §where the limits are |
| **subflows / cross-flow triggers** | Prefect-style subflows, Airflow's dataset-triggered DAGs | **NOT TYPED yet**: `FlowRunner.create_flow` inside a body works and is idempotent via the run key, but the child edge is invisible to `validate()` and the graph view — recorded backlog (S) | [Task stacks](task-stacks.md) §where the limits are |
| **named query handlers** | Temporal's queries (typed read handlers) | **NOT BUILT**: the rows answer everything raw (`flows status`, the run explorer, `HitlClient`); no registered typed query surface — recorded backlog (S) | [Task stacks](task-stacks.md) §the operator's day |

## The evidence discipline

The parity claims above are not asserted from the armchair — the
internal campaign's pattern probes ran each EXPRESSED claim against a
live database (the hold's slot release, the replay's once-only memo,
the dynamic map's cardinality, the outbox's idempotence, the run key's
arbiter) and the captures ship with the source under
`.measurements/patterns/`. The claims stay modest: **one mechanism per
pattern, one row for five of them, and the limits named in the same
breath as the wins.**
