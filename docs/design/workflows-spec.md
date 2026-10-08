# THE TYPED-WORKFLOW LAYER — the spec, the built truth (T14)

*This is the workflow layer's SPEC as SHIPPED: every mechanism described
here is LANDED and pinned; every number cites a file in this repository.
It opens with the KILL LIST (the can't-lie document — what was refused,
with the evidence), states the resolved decisions, and closes with the
adoption pitch and the open LATER rows. Companion pages: the engine
internals live in [the workflows guide](../guides/workflows.md); the
authoring surface in [the API reference](../api-reference/workflows.md);
the worked example in [doc-ingest](../examples/doc-ingest.md).*

---

## 0. THE KILL LIST (read this first)

What was proposed, attacked with evidence, and REFUSED:

1. **The `split`/`Router` parallel-API proposal — DEAD.** The three
   checkers agree it cannot be typed: pyright false-reds it, ty
   false-greens it (`Promise[Unknown]`), mypy collapses `Promise[Never]`
   (the probe corpus, T01's gate; the errata's #3). The ONE API is the
   wiring verbs (`step`/`gather`/`map_source`/`loop`/`build`) — a second
   router vocabulary would have shipped a type hole with a nice name.
2. **The full DAG engine (a general graph executor) — DECLINED.** The
   layer's shape is fan-out/fan-in over a counter and a ledger — NOT a
   general DAG runtime. Every mechanism is a row shape + one statement;
   a general engine would need its own scheduler, its own state machine,
   its own observability plane — the second system the architecture
   refuses to be.
3. **Content-addressed rerun (§12's K3) — LATER, re-raised for the
   maintainer's re-ruling.** The original deferral's premise ("no
   canonical-hash call site") is VOID: T03's `code_version` column
   (the per-attempt record, `taskq/workflows/_version.py`) CREATED the
   call site via tors' canonical `content_hash` — the adoption map's row
   is now ADOPT. What remains is ARCHITECTURAL SEQUENCING (the rerun
   machinery touches the fork path, the ledger, the emit cursor — the
   newest surfaces sequence first). `cache_key` stays dead: the
   `code_version` is a RECORD, never a cache.
4. **The refusal list** — the shapes the layer will not express, by
   design: event-time/windowed aggregation (one clock domain is the
   architecture's own law — see §5), arbitrary code between steps at
   recovery (the replay is re-derivation, §6), parallel holds inside one
   iteration (a hold is a NODE state; the LATER row, §8), a
   workflow-specific bulk DLQ replay (the vanilla redispatch owns
   redispatch; §8).

---

## 1. The packaging law

* The extra: `taskq[flows]` — the operator-facing opt-in marker.
* THE IMPORT LAW: `import taskq` never imports the workflows package;
  the only sanctioned entry is `import taskq.workflows` by a caller
  that opted in. A worker that never imported the definitions cannot
  resolve any body — the boot stamps `workflow_execution: false` (the
  capability is DATA on the workers row), and the dispatch fence never
  hands it a workflow row.
* The decorator law: bodies are plain typed async functions; the graph
  is a SYNC, PURE build function (`@app.workflow("name")`) — compile-
  time dataflow spelling, compiled fresh each time (`app.get(name)`),
  registered once per name (D1: the definition registry is the ONLY
  body source at run time).

## 2. The schema decision: columns on jobs

Workflow identity lives as COLUMNS on `jobs` (01.00.24_01): `parent_id`,
`deps_pending`, `map_index`, `step_key`, `code_version` (+ the loop
budget trio, 01.00.27/31). The measured tax (the SKEPTIC §IV probe): **+3.0
B tuple / 0.0 B heap on vanilla rows; 194× the partial-index exemption;
+104.9 B paid by workflow rows only.** The `wf_nodes` split-table variant
is the documented ESCAPE HATCH (the hot path's ticket if the tax ever
bites), never the shipped shape — the counter-as-cache law needs the
counter ON the row the dispatch claim already touches.

## 3. The engine rules (the ones that decide correctness)

1. **The two-transaction finalize** (P1 FINAL; erratum 6): tx1 = the
   result write + the TERMINAL-MARK FENCE (`status='running' AND
   locked_by_worker=… AND attempt=… AND claim_epoch=…` — the attempt is
   the fencing token, H8) + the ledger's terminal + the fork's
   children/edges/join; tx2 (rowcount-gated) = the guarded decrement +
   the `join_fire` insert + the outbox. Exactly-once = the fence + the
   `join_fire` PK + the status-predicate transition.
2. **COUNTER-AS-CACHE / LEDGER-AS-TRUTH**: `deps_pending` is the cache;
   the `wf_edge` ledger is the only decrement authority; the sweep's
   recount is the healer. The NESTED shape's rule:
   `remaining = join_target − committed decrements`, never child-row
   presence (the fanout proof: children don't exist yet at the nested
   join's check — pins 17/18).
3. **The eighth rule**: derived values are written only by the statement
   that derives them (the advance-and-cap write, the decrement, the
   terminal — each has exactly one writer; the docs' claim that
   something else writes a derived value is a lie the pin inventory
   convicts).
4. **FORK ATOMICITY** (pin 19): the terminal UPDATE + the fork's INSERTs
   in ONE transaction; a kill at any statement boundary rolls the whole
   tx; the fork-debt reconcile is a REPAIR, never the design.
5. **THE OUTBOX DRAIN IS OWNED** (pin 20): idempotent consumer inserts
   (`ON CONFLICT` on the step key); the flag flips in the insert's tx.
6. **RESUME-NOT-RETRY**: a hold's resume consumes NO ladder attempt
   (erratum 15's cure); the ladder burns for BODY failures only — an
   infra fault routes to the reclaim (the escape-point contract).
7. **THE ABSORBED-FAILURE CLAUSE precedes the `failed` row** (the §17.5
   derivation): a failed node absorbed by its edge's declared policy
   derives through its PARENT'S outcome — the workflow succeeds WITH the
   failure report (the report is the point, not a consolation).

## 4. The idempotency contract

Two composite arbiters on one live index shape: the STEP key
(`workflow:{flow_id}` × the step key — the ledger row IS the claim, the
PK blocks double-recording) and the RUN key (`workflow-run:<flow name>`
× the caller's key — a conflict RETURNS the existing run, never a second
silent run; the founding incident's cure). THE CRON COMPOSITION (G3):
the cron entry fires the slot key AS the run key — same slot twice →
ONE run (the deploy matrix's cell-7 pin proves it at fleet scale; the
deep-research march kicks the run from a REAL schedule's fire).

## 5. The type story's division of labor

* LOCAL truth: pyright 1.1.414 + ty 0.0.85 (the probe CI gate, T01 —
  both must be RED on each seeded type defect; mypy's evidence is
  recorded, not gated).
* GLOBAL truth: `compiled.validate()` — the E1–E7/W1 rule matrix
  (acyclicity, fan-in bounds, the total router, the residual-union
  closure, the gate declarations) runs BEFORE any row is written; a
  graph with errors does not run.
* THE CYCLE-HONESTY BOX: the static checkers cannot see a cycle through
  a variable — `validate()` owns acyclicity unconditionally (erratum
  2: "pyright catches cycles" was FALSIFIED by probe).
* The typed wait is the TUPLE FORM (`ctx.wait_signal((Model,), ...)`);
  the deliver side validates BY SHAPE at the runtime boundary (a wrong
  payload is the typed `refused`, the hold SURVIVES, the refusal is
  audited).

## 6. Terminal semantics + the replay trade

The body's annotation decides (the `Done`/`Refine` union, the `Exit`
sentinel, the typed partial); the finalize CAS is the shape guard. THE
STATUS IS DERIVED (§17.5): the run's verdict reconstructs from the ROWS
— never one column's cache.

**THE DIVERGENCE TRADES, AS FEATURES** (the adoption pitch, not
apologies):

1. **Replay = ROWS-ONLY RE-DERIVATION.** There is no orchestrator
   process to replay — the graph IS rows; recovery re-derives from rows
   alone (probe 1: mid-run + terminal reconstruction, Mermaid
   byte-stable). *Costs* a migrating Temporal user: no arbitrary code
   between steps at recovery; the wiring must be statically spellable.
   *Buys:* **non-deterministic bodies are LEGAL** (no determinism
   contract, no SDK sandbox, no history versioning — the rows version
   themselves per attempt), and run state is one SQL query away for any
   BI tool.
2. **THE DB-CLOCK TIMERS.** Every timer (hold deadlines, loop budgets,
   lease expiry) evaluates against PG's clock INSIDE the statement
   owning the transition (the SkewedClock pin: a +1h-skewed app clock
   cannot fire or unfire a budget). *Costs:* event-time semantics
   (inexpressible — §0's refusal 4). *Buys:* ONE totally-ordered
   crash-safe clock domain; the fleet's lived "a row `running` forever"
   pain is structurally impossible while the derivation re-derives.
3. **THE HOLD ROW — the headline.** Five ecosystem mechanisms behind
   ONE CAS'd, idempotent, slot-releasing row: the Temporal signal, the
   Argo suspend, the Airflow deferrable, the Airflow sensor, the
   LangGraph interrupt (probes 2+3: the slot RELEASES
   (`locked_by_worker` NULL), the resume consumes no ladder attempt,
   the memo replay is cheap, a re-delivered hold is a no-op). **No
   competitor unifies these five into one row shape.**
4. **Assets vs payloads** (Dagster's trade): the payload that ran IS the
   record — no double-bookkeeping, no asset catalog; redaction + the
   failure IO-capture apply uniformly (the G1 law: the record must not
   re-create the "looked healthy, was wrong" state; REDACT-BEFORE-
   PERSIST).

## 7. The deploy matrix (§22's commitment, LANDED)

The rolling-deploy contract is PROVEN at the system tier
(`tests/system_e2e/test_wf_deploy_matrix.py`, PG 15.19/16.15/17.11/18.6 ×
the cells, 6/6 green per version — `.measurements/p5/matrix-pg*-final.txt`):

| Cell | The contract | The pin |
|---|---|---|
| WORKER DEPLOY | a SIGKILLed node re-pends (lock_expired); the join counter never moved; a surviving pod finishes; the per-attempt `code_version` rides BOTH claims | cell-1 |
| SCHEMA MIGRATION MID-FLIGHT | the ACCESS-EXCLUSIVE deploy window: the ladder heals through it; no corruption | cell-2 |
| ROLLBACK | the vanilla pod meets a v2 fleet: the fence never hands it a workflow row; no crash loop | cell-3 |
| CONFIG DRIFT | the unserved queue: LIVE-blocked, the reason NAMED, the `TaskQQueueUnserved` join | cell-5 |
| CRON × WORKFLOW | the double-fired slot: ONE run (G3) | cell-7 |
| SIGTERM CLEAN-DRAIN | mid-iteration: fenced → requeued → resumed; the memo replay never re-ran an applied body | cell-8 |

The heartbeat budget is the operator's own knob above the deploy window
(the march fleet boots `TASKQ_MAX_HEARTBEAT_FAILURES=10` + the
co-invariant lock lease — the settings validator's cascade floor).

## 8. The open LATER rows (recorded, not settled)

* **Holds as child rows** (parallel holds inside one iteration): NOT a
  feature add — a REPLACEMENT of the proven hold representation; the
  fence matrix, the epoch identity and the T10/T19 pins re-derive.
  Sequenced after the core hold machinery's production experience.
* **Query-shaped bulk DLQ replay** (`retry_where` + preview): the
  workflow tables RIDE the vanilla redispatch ownership (the §22.6
  exclusivity); a workflow-specific bulk replay FORKS the model. When
  the vanilla surface grows one, the workflows inherit it.
* **First-class cronflow machinery**: the composition ALREADY EXISTS
  through the run-key law (§4) — a duplicate would violate the
  don't-pay law.
* **SAGA/compensation, run-status sensors, cross-flow triggers /
  subflows**: promoted to their own tickets (22/23/24) — the ledger
  already holds every terminal a compensator needs; the cascade already
  walks reverse edges; the outbox + the run-key arbiter make the
  child-flow trigger idempotent for free.
* **The uniform join-input read** (the two reader code paths for one
  join concept): the ergonomics fold-in candidate.

## 9. The evidence appendix (the numbers' home)

| Claim | The file |
|---|---|
| The deploy matrix's cells (PG 15/16/17/18 × 6, the runs ×3 where pinned) | `.measurements/p5/matrix-pg*-final.txt`, `.measurements/p5/matrix-run5.txt` |
| The marches (the cron kickoff → the map → the multi-HITL loop → the timeout face → the kill storm → the explorer; the streaming emit; the doc-ingest fan-in; the operator's walk incl. the UPGRADE PATH: the prior-release schema → the full chain on populated data → the workflows live) | `.measurements/p5/march-run{1,2,3}.txt`, `.measurements/p5/operator-full-run{1,2}.txt`, `.measurements/p5/march-timeline.json` |
| The fan-in curve (200 → 3.0 ms, 1000 → 3.3 ms, 5000 → 11.1 ms; the refit ~1.7 µs/edge + ~2.7 ms base; the 5 ms-class comparison struck) | `.measurements/edge-scale-curve.json`, the workflows guide §T07 |
| The 1000-child fan-out tx band, the join-fire latency | `.measurements/fanout-1000-tx-band.json`, `.measurements/join-fire-latency.json` |
| The hold/resume band (the resume consumes no attempt) | `.measurements/t10-hold-resume-band.json` |
| The streaming bands (the emit cursor) | `.measurements/t20-streaming-bands.json` |
| The migration-chain proof (every bundled migration, stepwise, onto populated data) | `.measurements/p5/cure-all-final.txt`, `tests/test_migrations_populated.py` |
| The stranger test (a fresh agent, docs-only: the demo + the API explanation + the 18 stumbles, dispositioned) | `.measurements/p5/stranger-report.md`, `.measurements/p5/STRANGER-DISPOSITION.md` |
| The engine's pin inventory (the tests that can fail) | the workflows guide's foot; `tests/test_wf_*.py` |

## 10. The abstraction contract (the wire-shim boundary)

This doc is a SHIPPED artifact: every inline example lives in the
ABSTRACT `doc_ingest` domain (ingest → summarize / extract_entities /
classify → the `ReviewDecision` approval hold → the barrier join →
publish) — self-explanatory to a stranger, zero case-study content.
The boundary is MEASURED, not aspirational: the repo-wide gate
(`tests/test_doc_ingest_example.py`'s forbidden-string list — the
case-study's own domain vocabulary, spelled in full only in that
gate's list — must report ZERO hits over `docs/`, `examples/`,
`src/taskq/workflows/`, `tests/`; this doc names none of them, which
is why this sentence describes the gate rather than quoting it).
The case studies that motivated the layer live OUTSIDE this repository;
only their SHAPE (the founding incident, the demand rows of §8) crossed
the wire-shim into the spec.
