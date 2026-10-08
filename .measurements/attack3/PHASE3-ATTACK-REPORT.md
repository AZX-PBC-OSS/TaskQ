# THE PHASE-3 ATTACK REPORT (the red team vs feat/taskqflow-p3, 8 commits off b4c0013d)

Attacker: the phase-3 subagent (fresh). Surfaces: T09's flow API
(validate/TypedGate/Mermaid/runner), T19's loop machinery, T10's HITL.
Method: local-first (the attacker's own warm PG on :5697 — :5696 was
squatted by a leftover `taskqflow-p2fix-ts` container; deviation noted),
every run captured here, red probes in `tests/attack3-*.py` +
`tests/typeprobe/attack3_wf_negative_types.py`. NOTHING was fixed; the
worktree's src/ is untouched.

## THE VERDICT: CERTIFICATION REFUSED

Two blockers + four highs on the new surface. The core/phase-2 layers
held under attack (the fences, the outbox's exactly-once drain, the
arbiter keys) — every red below is a PHASE-3 defect, and three of them
break the phase-2 certified laws' pins (exactly-once audit,
REDACT-BEFORE-PERSIST, STRANDED-FLOW).

## THE FINDINGS (by severity; each red is captured + reproducible)

### BLOCKERS

**B1 — the timeout face is DEAD: an expired hold re-holds forever, the
flow never terminates.** `SignalTimeoutError`/`SignalAbandonedError`
exist as classes and docstrings; NEITHER has a raise site. The shipped
`wait_signal` (runner.ts:210-263) checks only `status='held'` before
re-holding: after `sweep_expired_signals` marks the row `abandoned`, the
body re-executes, finds no held row, and MINTS A NEW HOLD with a new
epoch and a NEW deadline. Hold → expire → re-hold → ∞. The pin that
should own this (`test_signal_timeout_fires_on_db_clock`, pin 13)
asserts only the abandonment — its docstring claims "the wait site
raises the typed timeout face", which the test never exercises and
which is false. `GateDecl.on_timeout` is read by NOTHING.
Observed (A3-H1): the flow `running`, signals `[{abandoned, epoch 1},
{held, epoch 2}]` after the sweep + re-drive. RED:
`tests/attack3-hitl.py::test_a3_expired_hold_reholds_forever_no_timeout_face`.

**B2 — the union wait mis-narrows to the FIRST model (the smuggle that
type-checks).** `_coerce_signal` iterates the declared models IN ORDER
and returns the first that validates. A lenient first member swallows a
strict second member's delivery: gate `(Lenient, Strict)`, the operator
delivers `{"decision": …}` (Strict's shape) — the body receives
`Lenient(a='default', b='default')` — the WRONG type AND the payload's
data silently dropped. The typed door's core promise (the payload
re-validates into the declared model) is broken at the answer queue,
`HitlClient.resolve`, and `deliver_payload` alike — there is NO runtime
boundary anywhere on the deliver path (see H2). RED:
`test_a3_union_wait_narrows_to_the_first_model`.

### HIGHS

**H1 — `on_exhausted="escalate"` is dead end-to-end (three breaks).**
(a) The DRIVER's exhaustion (the advance guard's refusal — the common
live-worker path, and the body-failure path) never enqueues: only the
SWEEP's arm writes the outbox row (A3-L1, RED: zero `loop.escalation`
rows after a driver-path exhaustion). (b) `spec.on_exhausted` is read
by NOTHING — `fail` behaves identically to `escalate`; the sweep writes
the escalation for BOTH (A3-L2, RED). (c) Even the sweep's row is a
DEAD LETTER: its hardcoded binding `{"actor": "loop_escalation"}` names
an actor no author registers (LoopSpec has no escalation-actor field);
`drain_outbox` inserts a consumer job for it whose body never resolves
(`_resolve_body` → WorkflowRunError).

**H2 — the deliver path has NO runtime payload boundary.** The
docstrings promise "the smuggled `Approval.model_construct(verdict=42)`
is refused at the boundary with the named error, the hold SURVIVES".
`HitlClient.resolve` delivers ANY dict; the CAS consumes the hold
(delivered, cursor advanced), and the body's `_coerce_signal`
ValidationError then ladders the node — the operator's mistake kills
the flow, the hold does NOT survive, the ladder burns. Observed (A3-H5):
`resolve` returned `delivered` for a payload that satisfies no declared
model. RED.

**H3 — the hold context leaks: REDACT-BEFORE-PERSIST does NOT extend
to T10.** `wait_signal(reason=, tool=, args=…)` persists the context
RAW into `wf_signals.payload` (a second statement AFTER the hold
insert's tx — its own crash window too); `HitlClient` redacts only at
READ time and only if the constructor was handed a hook — and nothing
in the estate wires the workflow's own redact hook into the client.
Observed (A3-H3): the canary `sk-canary-a3-4f9d` in `args` reached
`client.list()` verbatim on the default surface.

**H4 — the resolve audit is NOT exactly-once.** `HitlClient.resolve`
writes the audit row + fires the `resolved` knock in the GUARD tx (the
row still `'held'`), and the deliver CAS runs in a LATER tx. Two
concurrent resolves BOTH pass the `'held'` guard → TWO `hitl.resolve`
audit rows + two knocks for ONE resolution (the result surface itself
is correct: one `delivered`, one `no-op`). Deterministic under
`asyncio.gather` (A3-H2, RED: `2 == 1` audit rows, op-a + op-b). The
guard tx must own (or CAS-check) the transition for the audit's
exactly-once to hold.

**H5 — a body-controlled `ConnectionError` wedges the loop forever.**
`_is_infra_fault` treats ANY `ConnectionError` raised by the BODY as
reclaim-eligible: re-pend (0.05 s), no ladder burn, no wall, no bound —
the ledger accretes `crashed` rows unboundedly and the flow stays
`running` forever (the exact STRANDED-FLOW shape the named-exhaustion
law exists to prevent — resurrected through the classifier). Observed
(A3-L3): 20 body runs, 20 crashed rows, flow `running` after a bounded
drive. RED.

### MEDIUMS

- **M1 — `drive(until="held")` is blind to the W1-sanctioned ETERNAL
  hold** (`timeout_s=None`): `_any_held` counts only `scheduled_at >
  now()`; an eternal hold never sets a future `scheduled_at`. Observed
  (A3-H7): `max_ticks` on a genuinely-held node; the wedge costs the
  full tick budget (~100 s at defaults). The held check should read the
  hold marker (`metadata ? 'hold'`), not the racy `scheduled_at` proxy.
- **M2 — the E3 rule is DEAD CODE**: `_rule_edgeless_join`'s
  `diagnostics.append` sits after the `continue` inside the `if` —
  unreachable. Its only pin tests the VERB (`gather([])`), not the
  rule. The shape the rule exists to convict (an injected edge-less
  join node — the compiled graph is public, mutable data) validates
  CLEAN (A3-V1, RED).
- **M3 — the cross-graph promise smuggle**: `step`/`gather` never check
  a promise's graph is the active recorder (`map_source` DOES). A
  foreign promise wired under a colliding key builds a silently WRONG
  edge to the victim app's own same-named node — clean at build AND at
  validate (A3-V2, RED: zero diagnostics).
- **M4 — E5's totality claim overreaches**: an UNANNOTATED (or
  duck-typed) consumer param consumes any producer unseen (A3-V5, RED).
- **M5 — the unknown-queue seam, confirmed from the build side**: an
  actor projected onto a nonexistent queue validates clean at build;
  the only door left is the worker-boot fail-fast the report itself
  admits is partial (#7) (A3-V4, RED).

### LOWS / NOTES

- The hold's `reason/tool/args` context lands in a SECOND transaction
  after the hold insert commits — a crash window leaves a context-less
  hold (liveness only; the operator sees a bare pointer).
- `runner.result()` returns untyped `Any` decoded jsonb — the read side
  of the typed door has no face at all.
- `SignalChannel` redact on `payload_schema` is skipped (schema refs are
  low-risk, but they DO name the gate's models — note, not a leak).
- `_runner.py` is 1,431 lines — within the repo's own god-module bar
  (the architecture review tolerates larger single-concern modules), but
  the loop driver is a clean seam if the next phase touches it.
- Tamper-evidence of the audit row: `record_admin_action` is a plain
  INSERT (no chain, no hash) — the phase-2 admin surface's contract; the
  read-back is plain. Flagged as a QUESTION for the phase-2 owner, not a
  phase-3 finding.

## THE VACUOUS AUDIT (the new pins, flipped in the head; "cannot" = vacuous)

| pin / guard | flip thought experiment | verdict |
| --- | --- | --- |
| E1 acyclicity | the DFS walks real `parents` edges; a forged forward reference cycles | **REAL** (unconditional ownership confirmed) |
| E2 residual | the consumed-set derivation is live; the pin's orphan reds | **REAL** |
| E3 edgeless-join | flip = delete the rule — no assertion changes (dead code) | **VACUOUS** (M2; the pin tests the verb, not the rule) |
| E4 unannotated | `body_hints` + the missing-return check is live | **REAL** |
| E5 consumer-compat | live for model-vs-model; blind to duck params (M4) | **REAL but partial** |
| E6 fan-in bound | the T07 constant compared live | **REAL** |
| W1 eternal-wait | emits a WARNING — refusal never fires; correct per doctrine | **REAL** |
| T19 mutation drill (`AND NOT budget_paused`) | the captured red (t19-mutation-drill-red.txt) + restored green exist; the paused-row pin has teeth | **REAL** |
| T19 sweep cap arm | the sweep's `iteration >= max_iterations` predicate is UNREACHABLE in production: the advance guard refuses at `i+1 >= max`, so the metadata's iteration tops out at `max-1` FOREVER — the pin hand-crafts `iteration: 3, max_iterations: 3` | **VACUOUS-in-production** (the arm's trigger state is unconstructible by the shipped code) |
| T10 pin 12 (the knock's shape) | the "pin" assigns `_ = HOLD_CHANNEL` and never listens — the payload shape is pinned by NOTHING shipped | **VACUOUS** (the actual knock is fine — A3-H4 proves pointer-only green — but the pin pins nothing) |
| T10 pin 13 (the timeout face) | the docstring claims the typed face; the assertion stops at `abandoned` | **VACUOUS claim, real assertion** (B1: the claimed face never fires) |
| T10 pin 1/2 (the late-deliver refusal, the double-send no-op) | the CAS predicate + the result-level no-op are live | **REAL** (but see H4: the audit leg is not covered) |
| T19×T10 composition pin | 6 checks, all live (pause, forced-past invisibility, resume, completion, zero burns) | **REAL** |
| T17's four contract probes (the strict-xfail flip) | the capture (t17-contract-first-run.txt: 4 xfailed, pre-API tree) + the cure commit ships the same probes marker-less and green; each probe exercises the API (bar_walk, cuts 1/7/4, the type story) | **THE FLIP IS REAL** |
| The type gate | run by the attacker: 6 shipped markers red on pyright 1.1.414 AND ty 0.0.85; the attacker's 4 NEW markers red on both (attack3_wf_negative_types.py) | **REAL** |

## THE CONFIRMED GREENS (the probes that held)

- **Mermaid byte-stability**: two compiles + reversed wiring order →
  byte-identical (A3-V3).
- **The knock is pointer-only**: hold_id + run_id + event, never the
  payload (A3-H4). (The attacker's first "dead knock" was its own
  listener on the wrong DATABASE — `pg_dsn` is a per-module database,
  `tq_db_<hash>`; pg_notify is per-database. Beware, next attacker.)
- **The epoch identity on a retried hold-site**: while a hold stands,
  the re-execution re-raises the SAME hold id/epoch (idempotent re-hold)
  — matches the docs' v1 semantics (A3-H8).
- **The answer-queue cursor through a body failure**: the retry replays
  EXACTLY the queue head — the operator never re-answers, no second
  hold, the wrong-answer replay dragon does not loose (A3-L4).
- **The cap wall bounds spawns exactly** (4 = max_iterations) and the
  sweep's shadow on terminal rows neither spawns nor resurrects
  (A3-L6, A3-L5).

## THE ANSWERS TO THE BRIEF'S QUESTIONS, STRAIGHT

- **validate catches ALL?** No. Cycles: yes. Two fan-in policies on one
  key: UNSPEAKABLE via the verb (one `on_failure` per node — the case
  cannot be constructed, note the expressiveness gap). The redact hook's
  wider return: caught by NOBODY (no rule, no runtime check). The
  nonexistent queue: not at build (M5).
- **The budget arithmetic under concurrency**: the sweep-vs-advance race
  resolves idempotently (both walls guard `status='running'`); the cap
  guard's atomic statement is real — but the sweep's cap predicate can
  never fire (vacuous, above).
- **The ladder in a mixed run**: infra → never burns (reclaim); body →
  burns ONCE, immediately terminal (documented v1 semantics: "burns its
  typed failure"); never twice; the exhausted state is idempotent.
- **The escalation rides the outbox exactly-once?** It does not ride at
  all (H1).
- **The double deliver**: one wins at the result level; the audit/knock
  surface doubles (H4).
- **The epoch on a retried hold-site**: same epoch while held (pinned by
  A3-H8; new epoch only past the queue's end — per the docs).

## THE CAPTURES (this directory)

- `validate-run1.txt` — 4 red (M2/M3/M4/M5) + 1 green (mermaid)
- `loop-run1.txt` — 3 red (H1a/H1b/H5) + 3 green
- `hitl-run1.txt` — 6 red (B1/B2/H2/H3/H4/M1) + 2 green
- `typegate-attack3-pyright.txt` / `typegate-attack3-ty.txt` — the 4 new
  MUST_ERROR markers red on both pinned checkers
- `redlog-scope.txt` — the hygiene finding below

## THE HYGIENE

- **The builder's finding #9 is CONFIRMED and WORSE than stated**:
  `RedLog.flush()` uses `write_text` (rewrite) on 5 tracked sinks
  (`pin-reds.json`, `ledger-pin-reds.json`, `t10-pin-reds.json`,
  `t19-pin-reds.json`, `t06-propagation-reds.json`), and a PARTIAL run
  rewrites the file with ONLY that subset's entries — the attacker's
  runs replaced t10-pin-reds.json's pin1 record with pin7's and flipped
  the band json's measured number (11.276 vs the committed 12.842). The
  append-only measurement corpus is not just rewritten-per-run, it is
  SUBSET-DEPENDENT: any non-full run silently falsifies the recorded
  evidence. Fix (the fixer's, not mine): append or key-per-run, and
  stop tracking self-rewriting measurements.
- **The docs are real**: both workflow docs in the mkdocs nav with
  substance (653 lines); the guides' contract language matches the
  shipped behavior EXCEPT the two lines this report convicts (the
  timeout face §"the timers"; the escalation enqueue §9).
- **The strays**: the worktree carried pre-existing modifications
  (`t10-hold-resume-band.json`, the two pin-reds files, an untracked
  `final-verify.txt`) — the builder's own last run's churn, present
  before the attack.

## THE DEVIATION NOTE

The brief's :5696 was occupied by a leftover foreign container
(`taskqflow-p2fix-ts`, a TS dev server's PG). The attacker's own warm PG
ran on **:5697** (`taskqflow-attack3-pg`, postgres:18) and is destroyed
after this report — nothing else touched it.
