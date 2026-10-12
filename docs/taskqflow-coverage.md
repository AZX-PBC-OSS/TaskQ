# TaskQflow — the shipped-capability coverage matrix

*What ships, where it is taught, where it is proven, and whom it serves.
One row per shipped capability; the gaps are named, not papered over
(§Gaps). This matrix is the docs lane's inventory — the consumer guides
(`docs/guides/agent-fleet.md`, `docs/guides/event-pipeline.md`,
`docs/guides/task-stacks.md`) and the pattern catalog
(`docs/guides/patterns.md`) were written FROM it.*

Legend — **example**: the runnable file that demonstrates the capability
(the example IS the verification; each is exercised by a named test
file, run twice for the docs lane's capture). **guide**: the reference
section that teaches it. **pins**: the test family that can fail. **use
case**: the consumer workload the capability carries.

## The matrix

| Capability (the shipped surface) | Example file | Guide section | Pin family | Consumer use-case |
|---|---|---|---|---|
| The DAG wiring — `@app.workflow` + `step(...)` + `build(...)`; the promise arguments ARE the edges; `validate()`'s E-rules at compile | `examples/workflows.py` (`doc_ingest`), `examples/deep_research.py` | [Workflows](guides/workflows.md) §the typed API; [API reference](api-reference/workflows.md) | `tests/test_wf_api_surface_pins.py`, `tests/typeprobe/wf_api_negative_types.py` (the wrong-shape wiring reds on both checkers) | Task stacks: a multi-step pipeline typed end to end |
| `map_source` / fan-in — runtime-cardinality fork, the derived `<key>.join`, the exactly-once join (the transactional outbox) | `examples/workflows.py` (`map_source(ingested, enrich_item)`), `docs/examples/doc-ingest.md` (the fence) | [Workflows](guides/workflows.md) §fan-out/fan-in; [Patterns](guides/patterns.md) §map-reduce | `tests/test_wf_fork_pins.py`, `tests/test_wf_fanin_bound_pins.py`, the join-fire pins in `tests/test_wf_outbox_retention_pins.py` | Event pipeline: fan a batch of records across workers, join when all settle |
| `gather` — the multi-parent barrier; `on_failure="fail_closed"` / `"maybe"` | `examples/workflows.py` (`barrier = gather([...])`) | [Workflows](guides/workflows.md) §fan-out/fan-in | `tests/test_wf_fanin_bound_pins.py`, `tests/test_wf_propagation_pins.py` | Event pipeline: the AND-join over sibling passes |
| The typed route (the chain surface) — `Chain`/`Step`/`Route`, the enum outcomes, `DONE`, the totality fence (`RouterNotTotal`), `chain_source`/`chain_start`/`chain_fork`, one trace per record | `examples/workflows.py` (`SCREEN_CHAIN`, `doc_screen_router`) | [Workflows](guides/workflows.md) §the chain; [Patterns](guides/patterns.md) §dead-letter-route | `tests/test_wf_attack_emit.py` (the router's record identity), the router pins in the t20 pack (`.measurements/t20-pin-reds.json`) | Event pipeline: per-element dispatch — the enrich arm, the dead-letter arm, no silent drop |
| The loop + the budget walls — `loop(...)`, `Done`/`Refine`, `max_iterations`, `budget_s` (the DB-clock wall), `on_exhausted="escalate"` (the REGISTERED escalation step) | `examples/deep_research.py` (`research_iteration`), `examples/workflows.py` (`review_iteration`) | [Workflows](guides/workflows.md) §the loop; [Agent fleet](guides/agent-fleet.md) §the budget walls | `tests/test_wf_loop_pins.py`, `tests/test_wf_attack_loop.py` (+ the consume-budget dragon kept red forever, `test_consume_budget_dragon_red_forever`) | Agent fleet: the deep-research loop bounded by wall, wallet-equivalent, and iteration count |
| The fail-close — the wait's outcome is the closed union `Payload \| Expired`; the expiry is a VALUE (a body that wants the failure raises `SignalTimeoutError` itself) | `examples/deep_research.py` (the `case Expired():` arm) | [Workflows](guides/workflows.md) §holds; [Agent fleet](guides/agent-fleet.md) §the fail-close | `tests/test_wf_hitl_pins.py`, `tests/test_wf_loop_pins.py` | Agent fleet: nobody-watching is a named result, never a hang |
| The hold row + the HITL gate — `ctx.wait_signal`, `GateDecl` (compile-visible), `HitlClient.list/get/resolve` (the deliver CAS, the defined no-op), the typed payload door (validate BY SHAPE) | `examples/deep_research.py`, `docs/guides/verify_guide.py` (blocks A2–A5) | [Workflows](guides/workflows.md) §holds; [Migration](guides/migrating-graph-checkpoints.md) §the worked port | `tests/test_wf_hitl_pins.py` (the t10 pack incl. the folded pin: `reason is None` after delivery) | Agent fleet: the 120-second approval gate; task stacks: the human gate mid-pipeline |
| The broadcast + the listener — `pg_notify` on `taskq_wf_hold_created` (transactional: a rolled-back tx is silent), `HitlListener` (backfill first, tail second), the typed event union `HoldCreated/HoldResolved/HoldExpired/Backfilled` | `examples/deep_research_web.py` | [Workflows](guides/workflows.md) §the broadcast; [Agent fleet](guides/agent-fleet.md) §the approval board | `tests/test_wf_hitl_pubsub_pins.py` | Agent fleet: one listener per process, every watcher served |
| The SSE faces — the admin's ops feed (`/sse/holds`, operator-gated), the embedding app's per-user stream (the run-scoped fan-out + the keepalive), the progress SSE (`/api/flow/{id}/progress/stream`, seq-cursor replay + the named partial) | `examples/deep_research_web.py`, `examples/admin_app.py` | [Progress](guides/progress.md) §HTTP SSE; [Agent fleet](guides/agent-fleet.md) §the board | `tests/test_wf_hitl_web_demo.py`, `tests/test_wf_progress_faces.py`, `tests/web_admin/` | Agent fleet: the per-user approval board; ops: the schema-wide hold feed |
| Dependency injection (the deps seam) — `WorkflowApp(deps=...)`; a body declares a dep as a parameter TYPE, the runner binds one instance at the app's door | `examples/workflows.py` (`EnrichClient`) | [Dependency injection](guides/dependency-injection.md); [Workflows](guides/workflows.md) §deps | `tests/test_wf_deps_pins.py` | Task stacks: the queue client or DB pool the bodies need, mockable at the seam |
| Rate limiting (the workflow path) — a flow row's claim honors the queue's `max_concurrent` cap (the same reservation the vanilla actors honor); the deny snoozes budget-free; the held flow RELEASES the queue slot while it waits on a human | `examples/workflows.py` (the queues' wiring) | [Rate limiting](guides/rate-limiting.md); [Workflows](guides/workflows.md) §the worker execution | `tests/test_wf_flow_rate_limit_pins.py` (the deny, the cadence, the fail-closed dependency arm, the door's slot law) | Event pipeline: a bursty source held to the downstream's real capacity |
| Progress — the two channels (STATE upsert + STREAM ring, ONE seq space), the coalescing emitter, the drop accounting, `read_map_aggregate` (the declared `aggregate=` read) | `examples/actors/progress.py`, `examples/workflows.py` | [Progress](guides/progress.md) §Workflow node progress | `tests/test_wf_progress_persistence.py`, `tests/test_wf_progress_emission.py`, `tests/test_wf_progress_faces.py` | Event pipeline: the window's progress projection read back by the dashboard |
| Streaming fan-out — `chain_source`'s paged generator, `ctx.emit_batch([...], cursor=...)`: the emit TX commits children + edges + the cursor checkpoint as ONE statement group; the resume continues from the last COMMITTED page (the crash re-emits exactly the lost page) | `examples/workflows.py` (`_screen_source_body`) | [Workflows](guides/workflows.md) §the chain/streaming; [Event pipeline](guides/event-pipeline.md) §the cursor | `tests/test_wf_attack_emit.py` (F-EMIT-1: a diverging re-page dies loudly), the emit-tx pins (`.measurements/t20-pin-reds.json`) | Event pipeline: replay an unbounded feed page by page, crash-safe |
| Backpressure — the declared `max_in_flight=` bound, the admission fence (in-tx advisory lock + bounded poll), the wide-page refusal, the ladder-owned stall | `examples/workflows.py` (the queues' wiring) | [Workflows](guides/workflows.md) §backpressure | the backpressure pins (the captured red: the unbounded variant blows the in-flight count) | Event pipeline: a fast source cannot outpace its workers without bound; observability degrades first, never correctness |
| Retries — the ladder (per-node `max_attempts`), the per-item isolation (a map child re-runs ALONE; siblings and succeeded items never re-run), infra-fault ≠ body-fault (reclaim never burns the ladder) | `examples/workflows.py` (`doc-doomed`, `ctx.attempt < 2`) | [Retries](guides/retries.md); [Patterns](guides/patterns.md) §retry-ladder | `tests/test_wf_runner_pins.py`, the reschedule probe (`.measurements/patterns/probe-run-ecosystem.txt`, probe 3) | Task stacks: step ladders that don't cascade; event pipeline: one bad record retries alone |
| Idempotency — the run keys (`create_flow(run_key=…)`, the packaged `run(..., key=…)`): the typed `RunClaim` (`created` / `existing-running` / `existing-terminal` — the refused-to-reuse verdict); the enqueue `idempotency_key` upsert (unique within its scope). THE TWO-CLAIMS LAW: the run-key arbiter dedups CONCURRENT triggers (two callers racing one key get ONE run); the enqueue key dedups TEMPORAL re-submissions (a resubmit after terminal never doubles the work) | `examples/workflows.py` (`trigger_run`), `docs/guides/verify_guide.py` (block B3) | [Workflows](guides/workflows.md) §the run-key arbiter; [Task stacks](guides/task-stacks.md) §the two-claims law | `tests/test_wf_attack_runkey.py`, `tests/test_idempotency_key_bounds.py`, `tests/test_idempotency_scope_migrations.py` | Agent fleet: one run per (user, topic) slot; task stacks: safe re-submission |
| Resumability — kill storms + re-derivation: every workflow row IS a `jobs` row; lease reclaim re-forks mid-fork kills; the status re-derives from ROWS ALONE (`reconstruct_workflow_status`); the replayed body reads the step ledger's memo (side effects cheap); delivered holds consumed in epoch order (the answer queue) | `examples/deep_research.py` (the deterministic replay), `examples/workflows.py` (`drive_loop`) | [Workflows](guides/workflows.md) §the join-wait representation + §the worker execution; [Patterns](guides/patterns.md) §checkpoint-interrupt | `tests/test_wf_finalize_pins.py`, `tests/attack_wf_engine_windows.py`, probe 3 (the checkpoint-replay probe) | Agent fleet: the kill-storm resume (SIGTERM deploy mid-approval loses nothing) |
| The admin surface — the run explorer (`/taskq/workflows/{run_id}`: the graph view, the taken paths, the map hexagon, the node panel with `?map_index=N` addressing), the Resolve form (the same typed door), the ops SSE | `examples/admin_app.py`, `examples/workflows.py` (property 3) | [Admin UI](guides/admin-ui.md) | `tests/web_admin/`, `tests/test_wf_attack_admin.py` | The operator: the run's whole story from rows alone |
| The CLI/operator surface — `taskq job show/cancel/retry/cancel-where/events`, `taskq queues ...`, `taskq doctor`, `taskq insights`; the named refusals (exit 1, never a traceback); the workflow CLI (`taskq.workflows._cli`'s holds/resolve faces) | `examples/client_script.py` | [CLI](guides/cli.md); [Operational Insights](guides/insights.md) | `tests/test_wf_cli_pins.py`, `tests/test_wf_attack_cli.py` | The operator's day: diagnose from the row's REASON |
| The runbook/alerts — the shipped alert rows (queue depth by age, the scheduled-plateau stall, sweep timeouts, lease expiry, rate-limit dependency outage, progress publish failures, cron slots, failed/retry rates, abandoned jobs) each with confirm-SQL + remediate + under-provisioned-vs-stalled | the rules files (`src/taskq/contrib/prometheus/rules.yaml`, the k8s PrometheusRule twin) | [Alert Runbooks](guides/runbooks.md) | the alert rules' own tests under `tests/` (the rules' shape) | The on-call: the kill-storm page, the starved-queue page |
| The packaged run — `taskq.workflows.run(flow, pool, schema, input=…, key=…)`: create + drive + the decoded result in one call | `docs/api-reference/workflows.md` (the thirty-second tour) | [API reference](api-reference/workflows.md) | `tests/test_wf_runner_pins.py` | Task stacks: the small-script face of a workflow |
| Cancellation — the one-tx cancel (the cascade's signal leg on the CALLER'S connection), `cancel_workflow_run`, `taskq job cancel-where` | `examples/workflows.py` (the demo's cancel affordances) | [Cancellation](guides/cancellation.md) | `tests/test_wf_attack_cancel.py` (the torn-cancel pins), the cancel matrix | The operator: stop a fleet of runs without zombies |
| Observability — structlog faces, the OTLP validation lane, the redaction chain (REDACT-BEFORE-PERSIST; the hold context passes the chain at the row read) | `examples/otel_setup.py` | [Observability](guides/observability.md) | `tests/otel_validation/`, the redact pins in `tests/test_wf_hitl_pins.py` | Every use-case: the record must not re-create the leak |

## The gaps, NAMED (honest, each with its disposition)

| Gap | The honest state | Disposition home |
|---|---|---|
| Saga / compensation (the typed surface) | The LEDGER fit is proven (every terminal outcome a compensator needs is recorded; the cancel cascade already walks reverse edges) — but there is no `on_failure="compensate"` wiring verb, no undo-node pass. Authors hand-roll compensation steps today. | Backlog head — a design-first ticket path exists; the reverse-edge walk is the engine's own precedent. See [Patterns](guides/patterns.md) §saga. |
| Named typed query handlers | The rows answer everything raw; there is no registered, typed "ask the run a question" surface. | Recorded-not-built (S). |
| Subflows / cross-flow triggering (the typed verb) | Authors compose by calling `create_flow` inside a body — legal, untyped, invisible to `validate()`. The run-key arbiter makes the manual form idempotent for free. | Recorded-not-built (S). |
| Windowed / event-time aggregation | Deliberately declined: one clock domain (the DB clock). The loop + `gather` + the progress projection cover the real cases; tumbling/sliding windows fight the architecture. | The refusal is a design position — see [Patterns](guides/patterns.md) §map-reduce; the recipe is the documented answer. |
| Multi-key state channels (per-key reducers) | The loop carry is a single-channel reducer (frozen at spawn, advanced exactly once per iteration). | The known gap — the carry covers the common case. |
| Token-level LLM streaming INTO a hold's UI | The engine's two channels + the SSE faces are shipped; the token stream is the embedding app's job (the per-user SSE fan-out). | The documented boundary — see [Agent fleet](guides/agent-fleet.md) §the board. |
| The stuck-running class | NOT a gap — structurally impossible while the status derivation re-derives from rows on the DB clock. The claim is pinned, not promised. | [Patterns](guides/patterns.md) §stuck-running. |

## § The ecosystem parity matrix — the honest competitor comparison

*Researched October 2026 from the competitors' current documentation,
blogs and changelogs (Temporal's Replay 2026 announcements, DBOS's
monthly "what's new" notes, Hatchet's v1 architecture docs, Restate's
foundations docs, Prefect 3.x docs, Dagster's 2026 declarative-automation
material, Airflow 3.x release notes, Windmill/Trigger.dev/Inngest product
docs). The states per cell: **HAVE** (shipped, the same face),
**DIFFERENT-SHAPE** (the need is served — the shape is named),
**LACK** (the gap is real — named, with its cost). The OSS law holds:
the ecosystem products are WELCOME in the frame; this section is the
feature-parity question's home, not a war poster. Where a competitor's
doc says something better than we ship, the cell says so.*

### The ten, in one line each

| Competitor | The model (one line) | The standout faces, late 2026 | The Postgres question |
|---|---|---|---|
| **Temporal** | the external orchestrator + deterministic-replay workers — the durability gold standard | Worker Versioning GA (pinned / auto-upgrade, upgrade-on-Continue-as-New preview), replay testing as a deploy gate, Standalone Activities (job processing without a workflow), Workflow Streams (durable token streaming, preview), task-queue priority & fairness, serverless workers, multi-region replication | EXTRA INFRA — its own server cluster AND its own database (Postgres is one of several persistence options) |
| **DBOS** | an embedded library checkpointing workflows + steps to any Postgres — the closest category | transactional steps (the checkpoint rides the app's own tx — exactly-once for DB-local effects), durable queues with 10× partitioned performance, durable streams, cron, HITL, Conductor (the closed-source ops plane) | THE DB YOU ALREADY RUN — the same category claim we make |
| **Hatchet** | the Postgres-backed task queue + DAG + durable tasks; engine + API server + gRPC workers | durable tasks (a checkpointed event log), DAGs, durable sleep/event waits (the pause-resume HITL frame for agents), cron + one-off schedules, concurrency/rate limits/priority, worker labels + affinity, dashboard + alerting + one-click replay, ~10k tasks/s load-tested, SDKs ×4 | PG IS THE SOURCE OF TRUTH, but the ENGINE is a separate service to run (RabbitMQ optional at high throughput) |
| **Restate** | a single self-contained Rust binary — journaling model, no external database | journals every step and replays on recovery, Virtual Objects (keyed durable state, single-writer), workflows with durable promises (the shared-handler signal face), awakeables (webhooks/HITL), durable timers "seconds or years", flow control (hierarchical concurrency budgets), sagas documented, SDKs ×6 | NONE — its own embedded storage; Postgres is neither required nor usable |
| **Prefect 3.x** | Python flows/tasks with TRANSACTIONAL orchestration | every task runs in a transaction; the caching layer is built ON the transaction records (`cache_key_fn` + `cache_expiration` — the cross-run cache, a different shape); dynamic `.map`; reschedule-on-SIGTERM (crash recovery without a flow-run restart); pause/suspend with `wait_for_input` (the UI-form HITL); events + automations | EXTRA INFRA — its own API server + its own metadata database (Postgres among the options) |
| **Dagster** | the asset graph — a different frame (data's lineage, not work's) | software-defined assets, declarative automation (2026 — desired-state policy per asset), sensors/schedules/backfills, the runs/event-log observability bar; as of 2026 announced as part of Prefect | EXTRA INFRA — Postgres storage + the daemon + code-location servers: the heaviest footprint of the ten |
| **Airflow 3.x** | the incumbent's DAG scheduler | DAG versioning (the most-requested feature, landed), event-driven scheduling (AIP-82 asset watchers), human-or-agent-in-the-loop tasks (3.1), deferrable operators, dynamic task mapping, edge/remote execution | EXTRA INFRA — scheduler + triggerer + workers + a metadata DB: heavyweight, and second-scale latency |
| **Windmill** | scripts (20+ languages) → flows on a Postgres queue with Rust workers — the other one-dependency candidate | approval steps (pause for user interaction), schedules, webhooks, git-based versioning, full audit + logs, the apps builder; per-step persistence with step-level replay — NOT checkpoint-and-replay in the Temporal sense | PG + ONE BINARY — the closest infra footprint to ours |
| **Trigger.dev v4** | TS-first durable tasks on managed (or self-hosted, in-your-stack) compute | queues defined upfront with concurrency limits, waitpoints (HITL for hours–weeks), checkpointing/freezing during waits, atomic versioning, realtime streaming, LLM observability | NOT YOURS — the platform's own storage (ClickHouse for logs); self-hosting drags in several services |
| **Inngest** | event-driven durable functions, step-memoized over HTTP | `step.run` memoized per step, `step.waitForEvent` (the HITL face, timeout-bearing), `step.invoke` (sub-agents), durable Realtime streaming + AgentKit, runs pinned to immutable deployments (hot-edit by step-ID hash) | NOT YOURS — their platform's own state stores |

### The face matrix

`✓` HAVE · `~` DIFFERENT-SHAPE · `–` LACK. (The shape notes follow the table.)

| The face | TaskQflow | Temporal | DBOS | Hatchet | Restate | Prefect 3 | Dagster | Airflow 3 | Windmill | Trigger.dev | Inngest |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Durable workflow (step-level recovery) | ~ | ✓ | ✓ | ✓ | ✓ | ~ | ~ | ~ | ~ | ✓ | ✓ |
| HITL (pause for a human, typed) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | – | ✓ | ✓ | ✓ | ✓ |
| Durable timers | ~ | ✓ | ✓ | ✓ | ✓ | – | – | ~ | ✓ | ✓ | ✓ |
| Child / sub-workflow (typed verb) | – | ✓ | ✓ | ✓ | ✓ | ✓ | – | ✓ | ✓ | ✓ | ✓ |
| Cron / schedule | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Queue / actor (concurrency, rate, priority) | ~ | ~ | ✓ | ✓ | ✓ | ~ | ~ | ~ | ✓ | ✓ | ✓ |
| Observability (runs UI, alerts) | ~ | ✓ | ~ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| The Postgres question | ✓ | – | ✓ | ~ | – | – | – | – | ✓ | – | – |
| Cross-run step cache | ✓ | – | ~ | ~ | ~ | ✓ | ~ | – | ~ | ~ | ~ |
| Versioning / deploy safety | ~ | ✓ | ~ | ✓ | ✓ | ~ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Durable streaming (tokens → UI) | ~ | ✓ | ✓ | – | – | – | – | – | – | ✓ | ✓ |
| Saga / compensation (typed verb) | – | ~ | ~ | – | ~ | – | – | – | – | – | ~ |

### The shapes, named (per face)

* **Durable workflow.** TaskQflow's shape: ROWS-ONLY RE-DERIVATION — every
  workflow row IS a `jobs` row; recovery re-derives the status from rows
  alone; there is no orchestrator process to replay and NO determinism
  contract (non-deterministic bodies are legal). Temporal/DBOS/Hatchet/
  Restate/Trigger.dev/Inngest checkpoint-and-REPLAY (deterministic or
  step-memoized); Prefect's shape is the transactional record; Dagster's
  the materialization; Airflow's the per-task re-run; Windmill's the
  per-step persistence with step-level replay. The honest trade: our
  bodies cannot use awaits-anywhere imperative control flow (the wiring
  must be statically spellable) — theirs cannot run a non-deterministic
  body without a sandbox and a versioning discipline.
* **HITL.** The hold row is the headline (five mechanisms in one row,
  the typed fail-close union `Payload | Expired`, the deliver CAS, the
  transactional broadcast + backfilling listener, the SSE faces,
  redact-before-persist). Temporal's signals+updates, Prefect's
  pause/suspend-with-input, Airflow 3.1's HITL tasks, Inngest's
  `waitForEvent`, Trigger.dev's waitpoints, Restate's durable promises,
  Hatchet's pause/resume, Windmill's approval steps — all HAVE the face;
  none pairs it with a checker-forced fail-close arm or a redaction law
  at persist. The boundary stands: token-level streaming INTO the hold's
  UI is theirs (below).
* **Durable timers.** The DB clock INSIDE the statement owning the
  transition — one totally-ordered crash-safe clock domain; hold
  deadlines, loop budgets, lease expiry. DIFFERENT-SHAPE twice: the
  precision is at-least (the sweep cadence is in the worst case), and
  there is no bare `ctx.sleep(30d)` step verb — a plain sleep is spelled
  as a hold-with-deadline today. Prefect/Dagster LACK a durable sleep
  (a paused run parks, it does not schedule its own wake at the DB).
* **Child / sub-workflow.** THE GAP. Temporal's child workflows (with
  version inheritance), DBOS child workflows, Hatchet child tasks,
  Restate's service calls, Inngest `step.invoke`, Trigger.dev
  `trigger()`, Prefect subflows, Airflow `TriggerDag`, Windmill
  subflows — every serious competitor has the TYPED verb. Here:
  `create_flow` inside a body works and is idempotent via the run key,
  but the child edge is invisible to `validate()` and the graph view.
  BLOCKER for the orchestration-migration segment.
* **Cron.** HAVE — and the composition is pinned: the cron entry fires
  the slot key AS the run key (a double-fired slot is ONE run — the
  deploy matrix's cell-7). Temporal's Schedules, Hatchet's cron +
  one-off schedules, everyone's cron: the face is table stakes; ours
  composes it with the arbiter, which several competitors hand-roll as
  a documented caveat.
* **Queue / actor.** The base queue (actors, leases, the ladder,
  retention, rate limits, `max_concurrent`, per-queue routing) is REAL.
  DIFFERENT-SHAPE: Temporal's task-queue priority & fairness (2026),
  Hatchet's priorities + worker labels/affinity, Trigger.dev's
  concurrency keys, Inngest's multi-tenant fairness — the weighted /
  priority / affinity dimension is LACK here. Windmill and DBOS share
  our shape.
* **Observability.** The run explorer (the graph view, the taken paths,
  the map hexagon), the progress SSE with cursor replay, the insights,
  the alert runbooks, the CLI's named refusals, OTLP. DIFFERENT-SHAPE
  vs the UI-mature competitors: Temporal's Worker Status UI + OpenMetrics,
  Dagster's asset lineage, Hatchet's dashboard + one-click replay,
  Trigger.dev's LLM spans. The gap is maturity and breadth, not the
  absence of a plane.
* **The Postgres question.** THE WIN COLUMN. TaskQflow, DBOS and Windmill
  run on THE database you already have. Hatchet adds one service (the
  engine). Everything else demands its own server, database, or
  platform. For a shop that already runs Postgres and resists a fleet,
  this column decides the shortlist.
* **Cross-run step cache.** HAVE and ahead-of-shape: the Nix-style
  recursive hash address (the body's code canon RECURSED with the
  resolved input), position-independent across runs, success-only, the
  receipt on the node row. Prefect's `cache_key_fn` + expiration is the
  only close relative (caller-supplied keys, no code-version
  sensitivity). Temporal/DBOS/Restate answer the need with the checkpoint
  model itself (within-run only) or hand-rolled idempotency keys.
* **Versioning / deploy safety.** DIFFERENT-SHAPE, and the difference is
  the point: per-attempt `code_version` RECORDS (the cache's address is
  code-sensitive by construction), the capability fence (a vanilla pod
  never claims a flow row), the deploy matrix (PG 15–18 × 6 cells).
  Temporal's Worker Versioning + replay testing exists to manage the
  determinism contract — a problem the rows-only shape does not have.
  The honest residue: a run is not PINNED to the definition digest that
  produced it — a changed body may execute for an in-flight run (legal
  here, unversioned from the run's perspective; see the ranked list).
* **Durable streaming.** DIFFERENT-SHAPE: the two channels (STATE +
  STREAM, one seq space), the coalescing emitter, the seq-cursor SSE
  replay — row-level progress is served. Token-level LLM streams into a
  UI are Temporal's Workflow Streams (2026), Inngest's Realtime,
  Trigger.dev's realtime: LACK here, documented as the embedding app's
  boundary.
* **Saga / compensation.** LACK the typed verb; the substrate is proven
  (the ledger records every terminal a compensator needs; the cancel
  cascade walks reverse edges). Restate documents the saga pattern,
  Temporal ships the samples, DBOS the examples — the verb is the gap,
  not the data.

## § Verdict — where we win

1. **The all-in-one face.** Queue + cron + workflow + HITL + rate limits
   + progress + admin explorer + CLI + alert runbooks in ONE Python
   dependency whose entire infra requirement is the Postgres instance
   already running. The competitors' equivalents: Temporal = server
   fleet + database + workers; Hatchet = engine + Postgres (+ optional
   RabbitMQ); Prefect/Dagster/Airflow = their own server + database +
   schedulers; Restate = its own binary; Trigger.dev/Inngest = their
   platform. Only DBOS matches the footprint — and its OSS observability
   plane is a UI behind Conductor, not a run explorer + holds board +
   runbooks in the library.
2. **The typed wiring is unique in the field.** Build-time totality —
   the checker probes AND the runtime validator, the typed payload codec
   at every boundary, `RouterNotTotal`, E1–E16 — no competitor refuses a
   wrong graph at COMPILE the way this does (the rest validate at run
   time, at replay time, or not at all). For the agentic segment the
   error moves from production to the IDE.
3. **The record discipline.** The envelope must not lie: fail-close
   unions, absorbed-failure records, redact-before-persist, the drop
   accounting, the stuck-running class structurally impossible (the
   status re-derives from rows on the DB clock). Nobody else's record
   makes those promises.
4. **The cross-run cache's address.** Code-version-sensitive,
   position-independent, success-only, receipted — the only competitor
   shape (Prefect's) requires the caller to design the key and lacks the
   code sensitivity.
5. **The pin discipline.** Every invariant on this page is a test that
   can fail, red-first, plus the deploy matrix across four Postgres
   versions — an evidence bar the ecosystem's marketing pages do not
   meet.

## § Verdict — where we're behind (the honest list)

1. **Deterministic-replay expressiveness.** We do not replay arbitrary
   code; recovery re-derives from rows, and the wiring must be
   statically spellable. A month-long imperative workflow with awaits
   anywhere (the Temporal/DBOS/Restate authoring model) is NOT writable
   here. This is the single most common "why not Temporal" objection and
   it is correct.
2. **Multi-language workers.** Python only. Temporal ships 8+ SDKs,
   Restate 6, DBOS 5, Hatchet 4, Inngest 3, Trigger.dev TS-first. A
   polyglot org cannot adopt us as its orchestration layer at all.
3. **Throughput and scale numbers.** Hatchet load-tests ~10k tasks/s and
   publishes its bands; Temporal scales horizontally by design; Restate
   publishes latency for a 10-step workflow. Our published bands are
   per-statement (the emit tx, the join fire, the fan-out) — there is no
   fleet-scale throughput number to compare against a vendor page.
4. **The UI's maturity.** The explorer and the holds board exist and are
   pinned; the competitors have years of UI investment (run search,
   saved views, metric dashboards, user management, LLM spans). Adopters
   judge the demo, and the demo is judged against mature dashboards.
5. **Snapshot-per-run isolation.** A run is not pinned to the definition
   digest that produced it (see the ranked list) — the deploy fences are
   proven, the pin-your-run-to-its-code contract is not.
6. **Event-driven triggers.** Cron + the run key are shipped; an event
   topic triggering a flow (Inngest's core, Airflow's AIP-82, Hatchet's
   events) is not. Event-shaped architectures start their shortlist
   elsewhere.
7. **Ecosystem gravity.** Connectors, AI-SDK plugins (Pydantic AI,
   Vercel AI, LangChain extensions), academies, Discord-scale communities,
   and the 2026 consolidation (Dagster into Prefect). This is not a
   feature gap — it is the funnel we do not have, and no matrix row
   closes it.

## § What the gaps cost (the adoption segments)

| Segment | The blocking gap | Where they go instead |
|---|---|---|
| Polyglot engineering orgs | Python-only workers (BLOCKER) | Temporal, DBOS, Hatchet, Restate |
| Imperative long-lived workflows (months, awaits anywhere) | the static-wiring shape (BLOCKER) | Temporal, Restate, DBOS |
| Agent platforms wanting streaming approval UIs | token-level durable streams (PAIN) | Inngest, Trigger.dev, Temporal |
| Multi-tenant SaaS (fairness / priority / affinity) | the weighted dispatch dimension (PAIN) | Temporal, Hatchet, Inngest |
| Fintech / multi-step side effects | the saga verb (PAIN until landed) | Restate, Temporal samples |
| Event-driven architectures | event triggers (PAIN) | Inngest, Hatchet, Airflow 3 |
| Data-platform teams (assets, backfills, connectors) | NOT OUR SEGMENT — the asset frame was declined by design | Dagster, Prefect, Airflow |
| **The segment we win** | **a Python shop already running Postgres, today assembling Celery + APScheduler + a hand-rolled admin, being told the cure is Temporal's fleet** | **us — if the gap list below stays short** |

## § The ranked gap list (top 10 by adoption impact — the ticket seeds)

1. **The typed sub-flow verb.** Every serious competitor has it; its
   absence blocks the orchestration-migration segment outright. Sketch:
   `subflow(child_flow, input, key=...)` compiles to a fork-of-one child
   whose finalize enqueues the child's `create_flow` with the parent's
   run id stamped on the child root's `metadata.wf_parent`; the outbox
   (already exactly-once) delivers the child's terminal back to a
   promise the parent's wiring consumes — the join machinery is
   unchanged; `validate()` gains the E-rule (a sub-flow promise is
   build-complete only through `build`); the run-key arbiter makes the
   spawn idempotent for free; the cancel cascade extends one hop
   (parent→child) because it already walks edges; the explorer renders
   the child node with a drill-down link. The spec's tickets 23/24 are
   the seeds.
2. **The saga / compensation verb.** The ledger already holds every
   terminal outcome and the cascade already walks reverse edges. Sketch:
   `step(fn, ..., compensate=undo_fn)` registers the compensator at
   compile (E-rule: the compensator's input type = the step's result
   type); on flow failure the cascade invokes compensators for SUCCEEDED
   nodes in reverse topological order as fresh jobs (idempotent via the
   step ledger); a failing compensator is RECORDED, never cascaded; the
   explorer draws the undo walk.
3. **Multi-language workers (start: TypeScript).** The machinery is SQL
   — the claim, the fences, the ledger are language-neutral; the
   definition registry is the hard part. Sketch: a non-Python body is
   registered as `(language, module, qualname)` with the same
   code-version stamp; a TS worker imports the same definitions and
   speaks the claim/finalize statements (asyncpg ↔ pg/porservice);
   the capability fence extends `metadata.workflow_execution` to a
   `languages` array. Seed with the ten most-asked faces (claim,
   finalize, emit, hold-resume), not the full API.
4. **Token-level durable streaming.** The two channels already share one
   seq space. Sketch: `ctx.stream(chunk)` rides the STREAM ring with a
   `kind='token'` class through the coalescing emitter; the progress SSE
   gains a token topic (the cursor-replay face already handles partials);
   the hold's context carries the stream pointer so the approval board
   renders the live transcript next to the Resolve form; retention
   prunes with the row.
5. **Priority / fairness on the dispatch band.** Temporal shipped it
   (2026), Hatchet and Inngest have it. Sketch: a `priority` column with
   a weighted round-robin `ORDER BY` inside the existing claim
   statement (the advisory-lock fence unchanged); per-queue or
   per-tenant weights in `workgroups`; the fairness invariant pinned the
   way the bands are (a low-priority starved-fleet probe).
6. **Worker labels / affinity routing.** Hatchet's labels, Temporal's
   queue-per-version. Sketch: `@app.actor(..., labels={"gpu":"a100"})`;
   the workers row stamps its labels at boot (the same projection as the
   capability fence); the claim's candidate WHERE matches labels against
   the row's queue+label pair; `TASKQ_QUEUES_STRICT` refuses the
   unservable label combination at boot (the drift-guard precedent).
7. **Typed query handlers.** Recorded-not-built (S) in the gaps table
   above — promoted. Sketch: `@app.query("progress", ProgressModel)` on
   the workflow; the registry resolves it like a body; a query reads
   rows through the runner's decode path and returns the validated
   model; the CLI (`taskq flows query`) and the explorer's node panel
   call it; read-only by construction (no row writes — the E-rule).
8. **Parallel holds inside one iteration.** The spec's LATER row — an
   agent loop that awaits two approvals concurrently is unwritable
   (sequential waits only). Sketch: a hold spawned as a CHILD ROW of the
   loop node (the fork machinery, fan-in 1); the budget pause aggregates
   the children's open holds (the `budget_paused` arm counts the open
   set); epoch identity is per child hold; the answer-queue cursor moves
   per (signal, epoch) pair. This REPLACES the proven hold
   representation — design-first, behind a flag, the pin matrix re-derived.
9. **Query-shaped bulk DLQ replay.** The spec's exclusivity: workflows
   inherit the vanilla surface when it grows one. Sketch:
   `taskq job retry-where <predicate> --preview` renders the matching
   rows first (the preview IS the dry run), then re-dispatches through
   the vanilla CAS in bounded batches; workflow rows ride the same
   re-pend (the reclaim arm), the ladder continues, the ledger records
   the operator.
10. **The definition-digest pin (snapshot-per-run isolation).** The
    honest completion of the versioning contract: a run's create stamps
    the compiled graph's digest on the root; a worker executing a node
    whose stamped digest ≠ its local compile snoozes budget-free
    (`released_reason='definition-mismatch'`) — the stranded-jobs
    detector surfaces the fleet that cannot run the run. Old runs
    finish on the definitions that produced them (Temporal's Pinned
    semantics), new runs get the new digest — and the rows-only shape
    still needs no replay test, because there is no replay.

## The fill state

Every row's example file runs under a named test family; the three
consumer guides and the pattern catalog walk the matrix's rows against
those files. The matrix's gaps are the four recorded-not-built items +
the two documented boundaries — none is silent. The ecosystem sections
above are RESEARCH (dated October 2026, from the competitors' own
current docs), not promises: each cell's state is checkable against the
cited faces, and the ranked list is ticket seeds, not a roadmap.
