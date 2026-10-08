# THE ECOSYSTEM PATTERN MAP — TaskQflow P3 vs the workflow/dataflow ecosystem

Mapper: the ecosystem-pattern miner (P3 red team). READ-ONLY on /home/rich/src/*;
the built surface measured is `/tmp/opencode/wt-taskqflow-p3` (branch
`feat/taskqflow-p3`, head `4eb6a0e8`). All probes ran against the mapper's OWN
Postgres :5701 (container `taskq-ecopatterns-pg`, schema `ecoprobe`), captured
under `.measurements/patterns/`.

## THE FLEET'S OTHER WORKFLOW ENGINES (what's actually in use)

Scanned every `pyproject.toml` / `requirements*` / `uv.lock` under /home/rich/src:

| repo | engine | signal |
| --- | --- | --- |
| `cbre-pfc` | **Prefect 3.6 + prefect-redis + Dramatiq[redis]** | production dep, two-tier (orchestrator + raw queue) |
| `SustainabilityAI` | **Prefect 3.6 + prefect-redis + Dramatiq[redis]** | production dep; a source comment complains a row sat `running` forever "transitively via Prefect" — the exact failure class the durable ledger's rows-only reconstruction kills |
| cennan, harvest-forecast-py, VisyX | tenacity (retry budgets only) | no orchestrator |

No Temporal, Dagster, Airflow, or Argo anywhere in the fleet. **The nearest
competitor TaskQflow displaces is Prefect-3 + Dramatiq, and the fleet has
already lived the lost-orchestrator pain Prefect charges for.** The catalog
below therefore weights Prefect's run semantics and Flink/Temporal's
correctness patterns most heavily; the TaskQflow app-usage comparison
belongs to the other miner.

## THE CATALOG (24 signature patterns, four verdicts)

Verdicts: **EXPRESSED** (probe-proven on :5701) · **AWKWARD** (expressible-
but-awkward, the paper cut) · **MISSING** (missing-with-demand, sized
honestly) · **DIVERGENT** (divergent-by-architecture, the trade documented).

| # | system | signature pattern | verdict | TaskQflow's mechanism |
| --- | --- | --- | --- | --- |
| 1 | Temporal | deterministic replay (re-derive state on recovery) | EXPRESSED | No orchestrator process exists to replay — the graph IS rows. `reconstruct_workflow_status` re-derives the workflow status from rows ALONE (`wf_step_ledger` attempted terminals + node-row error jsonb for never-granted), no status cache consulted. **Probe 1**: mid-run + terminal reconstruction; compile determinism verified byte-stable (`mermaid()` × 2 identical). The ledger-vs-replay divergence costs less than folklore says — see the trades. |
| 2 | Temporal | signals (typed, mid-run delivery) | EXPRESSED | `wf_signals` rows + `TypedGate` + `HitlClient.resolve`'s deliver CAS (exactly one resume; second deliver = typed no-op). **Probe 2**: hold → `list()` → resolve → resume → decoded result; double-deliver = `no-op`. |
| 3 | Temporal | queries (named typed read handlers) | AWKWARD | Raw rows answer everything (`workflow_nodes`, `HitlClient.list/get` — probe 2's enumeration face), but there is no NAMED, typed query-handler registry like Temporal's `@workflow.query`; every read is the caller hand-rolling SQL or hitting the client surface. → also MISSING list #5. |
| 4 | Temporal | saga / compensation | MISSING | Nothing in `src/taskq/workflows/` mentions compensation/undo (grep: zero hits). Ecosystem demand is proven (Temporal's saga samples, Airflow's TriggerRuleStory, every payment-pipeline migration). The ledger fit is EXCELLENT — see the ranked list. **M**. |
| 5 | Temporal | the cron workflow (one run per slot) | EXPRESSED | The estate's cron scheduler (`worker/cron_loop.py`, per-property cron since 01.00.01) lands on the run-key arbiter: `create_flow(run_key=…)` twice → the SAME flow id, `created=False` (the founding-incident pin). **Probe 6**: identical ids across two creates. |
| 6 | Temporal | continue-as-new (bounded history) | AWKWARD | The loop's fresh-jobs-per-iteration with `(flow, loop, iter, step)` keys is the same "don't grow one closure's history" instinct (probe 7: 4 iterations = 4 per-key ledger rows), but iteration rows still accumulate on ONE flow id — there is no first-class epoch handoff (close the flow, open a successor with the carry). The retention sweep prunes eventually; the gap is bound-per-flow. **S** if promoted. |
| 7 | Prefect | dynamic map (runtime fan-out cardinality) | EXPRESSED | `map_source` forks at the source's FINALIZE — cardinality is a run-time fact. **Probe 4**: a body returning 7 items at runtime → 7 child rows + 7 per-item ledger rows (map_index identity), join result `[0,10,…,60]`. **Papercut found en route** (see below). |
| 8 | Prefect | reschedule / transactional run semantics | EXPRESSED | Lease reclaim + the ledger: infra fault ≠ body failure (reclaim never burns the ladder); a transient body failure repends with backoff and the attempt replays. **Probe 3** proves the retry face end-to-end: attempt 1 failed after the answer, attempt 2 replayed the memo (memo fn executed ONCE) and the delivered answer (1 delivered signal — the operator never re-answered). |
| 9 | Prefect | subflows / flow-of-flows | MISSING | No `child_flow` verb, no subflow row linkage; an author composes by calling `FlowRunner.create_flow` inside a body — legal, untyped, invisible to `validate()` and the Mermaid face. → ranked list #3. **S**. |
| 10 | Dagster | the op/job split (pure unit vs placement) | EXPRESSED | `@app.actor` body = the op; `WorkflowActor.actor_config()` projects into the estate's `ActorConfig` carrier — one actor population, visible to config-sync, drift guards, `TASKQ_QUEUES_STRICT`. **Probe 1**: projection `{actor: one_step_a.prepared, queue, max_attempts, retry_kind}` read back from the compiled node's body. |
| 11 | Dagster | the asset graph (versioned, materialized IO) | DIVERGENT | TaskQflow moves data as typed payloads riding job rows and edges — there are no versioned materialized assets and no IO manager. What the asset graph BUYS (declarative lineage, backfills, freshness) TaskQflow partially re-derives (`wf_edge` lineage, run-key idempotency ≈ backfill safety); what it COSTS a migrating Dagster user: no asset catalog, no partitioned materializations. Documented trade, not a gap to paper over with a table. |
| 12 | Dagster | run-status sensors | MISSING | The leader sweeps are the PATTERN'S ENGINE but they are internal re-derivation arms; there is no user-facing "watch runs matching a filter → run my handler". The fleet's Prefect repos get this from Prefect automations today. → ranked list #2. **M**. |
| 13 | Airflow | sensors (poll until poke-true) | EXPRESSED | The hold IS a sensor with a deadline: the wait site registers the hold row, the expiry sweep owns the timeout arm, the consumer converges by poll or knock. **Probe 2**'s signal row + deadline + deliver is the poke-true path. |
| 14 | Airflow | deferrable operators (free the slot, resume on trigger) | EXPRESSED | The held representation is exactly the deferral: node `pending` + deadline, **`locked_by_worker` NULL** (the slot is RELEASED — probe 2 measured it), resume consumes NO ladder attempt (resume-not-retry; the awaited≠failed ledger law). |
| 15 | Airflow | dataset-triggered DAGs | MISSING | Cross-workflow triggering has no typed surface (same gap as #9 — merged in the ranked list). **S**. |
| 16 | Argo | template steps/dags (declarative, compiled, stable) | EXPRESSED | The compile: same module → byte-identical graph; `validate()` (E1–E6 + W1, one-pass report) and `mermaid()` are pure functions of it. **Probe 1** re-compiles twice and diffs. Argo has nothing like the E-rule validator. |
| 17 | Argo | the suspend node | EXPRESSED | The hold gate (compile-VISIBLE: `GateDecl` renders `[(hold)]` Mermaid nodes; W1 convicts the eternal wait at compile). **Probe 2** runs it. |
| 18 | Kafka/Flink | windowed aggregation (event-time windows) | MISSING | `gather` is structure/count-bounded; the loop's `budget_s` is a wall-clock TOTAL, not tumbling/sliding windows. Honest verdict: LOW fit with the ledger's commit-ordered domain — ranked last, recommendation NOT to build (see list #4). **L**. |
| 19 | Kafka/Flink | watermark / event-time semantics | DIVERGENT | TaskQflow's clock domain is PG's clock, full stop: deadlines, budgets, lease expiry all evaluate IN-DB (the T19 SkewedClock pin proves the budget fires on DB time under +1h app skew). See the trades. |
| 20 | Kafka/Flink | the exactly-once sink | EXPRESSED | The transactional outbox trio: `wf_join_fire` (UNIQUE join_job_id — at most one fire per join, ever) + `wf_outbox` drained with idempotent consumer inserts + the step idempotency ledger. **Probe 5**: after a terminal flow, hammering `sweep_join_rederive` + `drain_outbox` twice more moved NOTHING (fire rows 2→2, consumer rows 1→1, status `complete`). |
| 21 | Kafka/Flink | backpressure-by-credit | AWKWARD | Credits ≙ per-queue concurrency limits + `FOR UPDATE SKIP LOCKED` batched claims + the sweeps' bounded drain. The paper cut: a map forks ALL children up front (10k items = 10k rows) and throttling is queue-global — there is no per-map in-flight cap or chunked fork. **S**. |
| 22 | LangGraph | interrupt / checkpoint | EXPRESSED | The hold rows + node rows ARE the checkpoints; `interrupt()` ≙ `_NodeHeld` (the runner catches it, no terminal, no ledger failure, the slot releases). **Probes 2+3**. |
| 23 | LangGraph | thread replay (resume from checkpoint) | EXPRESSED | The re-execution doctrine + `ctx.step`'s ledgered memo (pre-wait side effects replay CHEAP) + the answer queue (delivered holds consumed in epoch order by a per-attempt cursor). **Probe 3** is exactly the LangGraph "replay from checkpoint with the interrupt's resume value" — memo executed once across two attempts, answer replayed. |
| 24 | LangGraph | state channels / reducers (per-key merge) | AWKWARD | The carry (frozen-at-spawn, advanced exactly once per iteration — probe 7: `[1,2,3,4]`, no double-apply) is a SINGLE-channel reducer; multi-key state channels with per-key merge fns have no first-class shape (the NAIVE-MEMO guidance pushes it to author-owned one-TX memos). **M** if promoted; the loop carry covers the common case. |

## THE PROBES (the EXPRESSED evidence)

All seven probes live in **`.measurements/patterns/probes/probe_ecosystem.py`**
(standalone async script; runs against `postgres://…@localhost:5701/taskq`,
schema `ecoprobe`, via the worktree's own venv: `.venv/bin/python
.measurements/patterns/probes/probe_ecosystem.py`).

Captured evidence:

| file | contents |
| --- | --- |
| `.measurements/patterns/probe-run-ecosystem.txt` | the full captured run: **7/7 PASS**, one line per pattern claim with its measured detail |
| `.measurements/patterns/probe-run-ecosystem.jsonl` | the same run as machine-readable records (probe / pattern / verdict / detail) |

| probe | stands for | claim proven |
| --- | --- | --- |
| `probe_01_rows_replay` | Temporal replay, Dagster op/job, Argo templates | rows-only reconstruction `complete`; input rides the root row; Mermaid byte-stable ×2; `actor_config` projection correct |
| `probe_02_signal_hold` | Temporal signals, Argo suspend, Airflow deferrable/sensor | slot RELEASED while held (`locked_by_worker` NULL, `pending` + deadline); 1 hold listed; deliver → `delivered`; re-deliver → `no-op`; decoded result |
| `probe_03_checkpoint_replay` | LangGraph checkpoint replay, Prefect reschedule | memo fn executed **once** across two attempts; **one** delivered signal (the operator never re-answered); attempt 2 returned the replayed answer; terminal |
| `probe_04_dynamic_map` | Prefect dynamic map | runtime 7-item fork → 7 child rows + 7 per-item ledger rows; flat join result |
| `probe_05_exactly_once_outbox` | Flink exactly-once sink | two redundant sweep+drain passes over a terminal flow: fire rows 2→2, outbox/consumer rows unchanged, status `complete` |
| `probe_06_run_key_arbiter` | Temporal cron workflow | two `create_flow(run_key=X)` → identical flow id (the arbiter, never a second run) |
| `probe_07_loop_carry` | Temporal continue-as-new adjacency, LangGraph reducers | carry advanced exactly once per iteration (`[1,2,3,4]`), 4 iteration ledger rows, `Done` payload decoded |

**A DEFECT OBSERVED EN ROUTE (not fixed here — the fixer's lane).** Probe 4's
first wiring consumed the map's join promise in a downstream `step`
(`step(_map_tail, mapped)`) and tripped the runner's own assertion:

```
AssertionError: the parent 'src.join' has no result yet — the arg resolution
ran before the edge's terminal (a dispatch bug)
```

The pin corpus never exercises that shape (it uses `build(mapped)` or
consumes the SOURCE promise). Every ecosystem mapper would write the
downstream-of-map shape first — this is exactly the AWKWARD class: either a
real dispatch-ordering bug or a sanctioned-shape gap, and the compile should
refuse or handle it either way. Repro is in the probe file's history; wire
`build(step(_map_tail, map_source(src, body)))` to see it.

## THE RANKED MISSING LIST (demand-proven, honestly sized, fit-vs-ledger)

1. **Saga / compensation** (Temporal #4). Demand: every ordering/payment/
   multi-write pipeline; the fleet's Prefect repos hand-roll try/except +
   cleanup steps today. Fit with the ledger: **excellent** — the ledger
   already records every terminal outcome a compensator needs to decide
   WHAT to undo; compensation steps are ordinary nodes; the cascade
   machinery (T06) already walks reverse edges for cancel. COST: **M** —
   a type surface (`on_failure="compensate"` edge values + a
   `compensates=` wiring arg mapping forward steps to undo bodies), a
   runner pass that enqueues undo nodes along the reverse edge ledger,
   pins. NO new table. This is the backlog's head.
2. **Run-status sensors** (Dagster #12, Prefect automations). Demand: the
   fleet runs Prefect partly FOR this (automations on run failure). Fit:
   the leader-sweep registry is the engine — a sensor is another sweep arm
   over `jobs`/`wf_*` filters. COST: **M** — one sweep arm + a
   sensor-definition table (filter + handler + cooldown) + a typed handler
   registry (D1 discipline) + dedupe state so a sensor fires once per
   transition, not per tick.
3. **Cross-flow triggering / subflows** (Prefect #9, Airflow #15). Demand:
   flow-of-flows is the default scaling move in both systems. Fit: the
   outbox already delivers typed consumer jobs; a `wf.child_flow(...)`
   verb lowers to an outbox consumer body that calls
   `FlowRunner.create_flow(run_key="child:<parent>:<key>")` — the arbiter
   (probe 6) makes the trigger idempotent for free, and the run_key
   convention gives the lineage. COST: **S** — a verb + the consumer-body
   sugar + a validate rule (E8?) + the Mermaid face. Cheapest high-value
   item on the list.
4. **Windowed aggregation** (Flink #18). Demand: proven by the streaming
   ecosystem, but TaskQflow's niche is durable orchestration + agent
   loops, where count/structure-bounded gathers and the loop cover the
   real cases. Fit: LOW — windows need event-time, and the architecture
   deliberately has one clock domain. COST: **L** (window-state table, a
   timer arm, an event-time surface that fights the DB-clock doctrine).
   Recommendation: DON'T build; document the loop+gather recipe instead.
5. **Named typed query handlers** (Temporal #3). Demand: moderate — every
   dashboard wants "ask the run a question". The rows already answer raw
   queries; the gap is the typed, nameable surface. COST: **S** — a
   registry keyed like the signal catalog + a read path over the existing
   row queries.

## THE TRADES (divergent-by-architecture, documented)

**The ledger vs deterministic replay.** Temporal recovers by RE-EXECUTING
the orchestrator code deterministically; TaskQflow has no orchestrator
process — the workflow IS the row graph, and recovery is RE-DERIVATION from
rows (`reconstruct_workflow_status`; probe 1 proves it end-to-end, including
the crash-window rule where a pending row's ledger terminal wins). What the
divergence COSTS a migrating user: no arbitrary code runs "between" steps at
recovery time (a Temporal user's orchestrator-side helpers must become
steps or sweeps); the wiring must be statically spellable (dynamic control
flow lives in the loop's Done/Refine union, not in imperative branching).
What it BUYS: no replay-compatibility constraint AT ALL — bodies may be
non-deterministic (probe 3's body calls `datetime`-free but side-effecting
code freely; Temporal forbids that in the orchestrator), no SDK sandbox, no
versioning of workflow HISTORY — the rows version themselves per attempt —
and the status surface is one SQL query away for any BI tool. The other
miner's LangGraph comparison should note: TaskQflow's replay is
*reconstruction*, LangGraph's is *re-execution from the checkpoint state* —
TaskQflow sits on the cheaper, more auditable side of that line, and the
answer queue (probe 3) is the row-level answer to LangGraph's resume-with-
interrupt-value.

**The DB clock vs event time.** Every timer in TaskQflow — hold deadlines,
loop budgets, lease expiry — evaluates against PG's clock inside the
statement that owns the transition (the SkewedClock pin: a +1h-skewed app
clock cannot fire or unfire a budget; probe-adjacent evidence: t19 pins #2/#9
already captured). What it COSTS: event-time semantics for out-of-order
data don't exist — you cannot ask "what did the window know at event time
T"; watermark-style lateness tolerance is inexpressible without a new clock
domain. What it BUYS: ONE totally-ordered, crash-safe clock domain where
timers are rows with unique constraints — the consume-budget dragon is
convicted by a mutation drill because the budget's truth is a row arm
(`AND NOT budget_paused`), not a distributed timer; and the fleet's lived
Prefect pain ("a row `running` forever") is structurally impossible while
the derivation re-derives from rows.

**Assets vs payloads** (Dagster #11). Data ownership lives in the job rows'
typed payloads, not in versioned materialized assets. Costs Dagster users
the asset catalog, freshness curves, partitioned backfills. Buys: no
double-bookkeeping — the payload that ran IS the record, redaction chains
and capture policies apply uniformly, and cross-flow data never dies in a
Python closure (cut #7 — the input rides the root row, probe 1 asserts it).

## COUNTS

**24 patterns mapped: 13 EXPRESSED (all probe-proven, 7/7 probes green),
4 AWKWARD (paper cuts), 5 MISSING (sized: M, M, S, L, S), 2 DIVERGENT
(trades documented).**

## TOP FIVE FINDINGS

1. **The replay claim is true and cheap.** Rows-only reconstruction with no
   status cache passes end-to-end (probe 1) — the ledger-vs-replay
   divergence is a real architecture difference that costs migrating
   Temporal users their orchestrator-side code, and buys them
   non-deterministic bodies plus a SQL-queryable run state.
2. **The hold row is five ecosystem patterns at once** (Temporal signal,
   Argo suspend, Airflow deferrable, Airflow sensor, LangGraph interrupt)
   behind ONE CAS'd, idempotent, slot-releasing mechanism (probes 2+3).
   No competitor unifies these five into one row shape.
3. **Saga/compensation is the head of the backlog**: the ledger already
   holds every fact a compensator needs, cancel already walks reverse
   edges — the cost is a type surface + a runner pass, never a table (M).
4. **The map's join has an unguarded seam**: consuming a map's join promise
   in a downstream step trips the runner's own assert (`the parent
   'src.join' has no result yet`) — the first shape every Prefect user
   would wire. Either a dispatch bug or a missing E-rule; the pin corpus
   never covered it.
5. **The DB clock is load-bearing, not incidental**: every timer is an
   in-DB row arm, pinned under +1h clock skew — which is exactly why
   event-time/watermark semantics (Flink's crown jewels) are out of scope
   by architecture, and why the honest verdict on windows is "don't build".

## THE STRATEGIC VERDICT

TaskQflow P3 stands, against the whole ecosystem, as a **ledger-native
orchestrator that has already absorbed the ecosystem's correctness
patterns** — replay-equivalence (as re-derivation), exactly-once joins (as
the outbox trio), deferrable/suspend/signal/interrupt (as one hold-row
mechanism), checkpoint replay (as the memo ledger + answer queue), dynamic
map, the run-key arbiter, and a compile-time validator stricter than
anything in Temporal, Airflow, or Argo — and it has done so with a smaller
concept count than any of them (rows, sweeps, two type faces). Where it
genuinely trails the ecosystem is in the OPERATOR's patterns, not the
engine's: no compensation, no user-facing sensors, no cross-flow trigger —
all three are S/M-sized rows-and-sweeps work that fits the architecture
without bending it, and all three have live demand in the fleet's own
Prefect installs. The two structural divergences (replay→re-derivation,
event-time→DB-clock) are defensible and should be sold as features, not
apologized for — with one caveat: the map-join downstream seam shows the
static-spelling doctrine still has unguarded edges where a dynamic-map
user's first instinct breaks, and that class of seam is where the
migrating Prefect user will live.
