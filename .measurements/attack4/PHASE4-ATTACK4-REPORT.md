# THE PHASE-4 ATTACK REPORT (ATTACK4)

The attacker: the stranger's adversarial twin, alone, on
`feat/taskqflow-p4` @ `f1f08a38` (the range `fd0a06b8..HEAD`, 13
commits). The dev loop was LOCAL (the attacker's own PG on :5711, its
cluster under `/home/rich/.attack4-pg`, destroyed after the run — the
/tmp tmpfs was full, so the cluster lived on /home). Every run was
captured to a timestamped file in THIS directory; every number below
grep-audits against one of them.

## THE DELIVERABLES

| file | what it is |
| --- | --- |
| `tests/attack4-admin-chaos.py` | the admin page under fire: hostile Last-Event-IDs, the kill/reconnect, two browsers, the resolve-mid-render rows==DOM race, the XSS probes, the audit completeness, the 200-child-map latency |
| `tests/attack4-cli-races.py` | the CLI under fire: resolve×cancel as real subprocesses, the typed door's refusals, the why-stuck truth, the exit-code contract, the torn-cancel probe |
| `tests/attack4-nothing-stuck-probes.py` | the attacker's own shapes: the self-cancelling body mid-emit, the resolve after terminal, the unregistered-gate cold process, the DB-restart rounds |
| `tests/attack4-coverage-closers.py` | the split owner's residual lines CLOSED (20 tests — attack4 adds tests, never source) |
| `tests/typeprobe/attack4_new_surfaces_negative_types.py` | the negative type-probes for the admin/CLI surfaces, red on BOTH pinned checkers (pyright 1.1.414 + ty 0.0.85) |

## THE VERDICT UP FRONT

**THE CERTIFICATION IS REFUSED.** The engine's concurrency core held
under everything the attacker threw at it — but two HIGH findings are
data-loss class (a torn cancel that destroys a held operator's decision;
a silent hole in the typed door), and the demo's own compose stack
cannot perform the demo's central resolve. The fixes are small; the
certification waits for them.

---

## THE FINDINGS, BY SEVERITY

### HIGH

**F-P4-TORN-CANCEL — the cancel cascade's signal leg rides a SECOND
connection outside the caller's transaction.**
`cancel_workflow_run` (`api/_runner_exit.py:229`) opens ONE
`conn.transaction()`, flips the root, cancels the nodes — then calls
`cancel_run_signals`, which acquires a DIFFERENT pool connection and
updates the `wf_signals` rows in AUTOCOMMIT. The module docstring claims
"the same one-tx cancel"; the structure is two connections, one
transaction. The probe (`test_the_torn_cancel_the_signal_leg_rides_a_second_connection`)
forces the outer tx to roll back after the signal leg ran (the audit
writer raises) and observes:

```
THE TORN CANCEL, OBSERVED: the signal leg ran on its own connection and
SURVIVED the outer rollback (signal='cancelled', root='running') — a
held operator's decision is gone on a run that never cancelled
```

A held human decision is DESTROYED by a cancel that never happened. The
run keeps running; the body's wait site sees the abandoned hold; the
audit trail says a cancel that the run's own row denies. Cure shape:
the signal leg takes the CALLER'S connection (the statement is one
UPDATE), or the whole cascade moves inside the caller's tx.
**RED: stable across 3 runs** (`attack4-cli-races-run9`, the joint
captures, the restart capture).

**F-P4-UNTYPED-COLD-DOOR — the typed door has a silent hole in the
upgrade world.** The cold-process fallback in `_boundary_refusal`
(`api/_hitl.py:514-532`): when the hold row carries NO
`payload_schema`, the boundary returns `None` — "no declared models on
the row — the legacy hold's shape" — and the delivery proceeds UNTYPED.
The probe erases the row's `payload_schema` (the migration/upgrade
world), resolves from a FRESH PROCESS that never ran the wait site (the
model catalog cannot witness it), and delivers
`{"totally": "undeclared", "verdict": {"nested": True}}`:

```
[attack4 cold-gate] rc=0 out='RESULT: delivered | None'
```

The node wakes with a payload NOTHING declared — the body's downstream
read of `decision.verdict` gets a dict where it typed a str. The WARM
control (`test_a_hold_with_payload_schema_in_the_warm_process_still_refuses_garbage`)
holds: the same garbage is refused when the row's schema or the
process's catalog can witness it. The door's teeth therefore depend on
which process asks — a hole the deliver-no-drop law does not name.
**RED: stable across 3 runs.**

**F-P4-DEMO-DEAD-RESOLVE — the compose stack cannot perform the demo's
central resolve, three ways.** Observed on the COLD compose run
(`attack4-demo-*.txt`, the stack rebuilt from this tree first — see the
next finding):

1. the APP service (where the trigger's 202 URL lands,
   `/taskq/workflows/{id}`) renders the Resolve form but its POST is
   refused: `{"detail":"Actions are disabled on this deployment
   (TASKQ_ADMIN_ACTIONS_ENABLED=false)"}` — the env var is set on the
   `admin` sidecar only (`docker-compose.yml` line 130), never on
   `app`;
2. the SIDECAR (actions enabled) answers the resolve with the typed
   door's 501 — `examples/admin_app.py:72` mounts the router WITHOUT
   `workflow_app=...` — "no workflow definitions mounted … the resolve
   refuses to deliver untyped";
3. the prefixes disagree: the trigger's URL says `/taskq/…`, the
   sidecar serves `/admin/…`.

The demo's demonstrable property #2 ("the Resolve form (on the run
page) delivers the typed `ReviewDecision`") is UNTRUE on the shipped
stack. The run completes only when the resolve rides the container's
own `HitlClient` (captured: `resolve: delivered` → the root
`succeeded`, 17/17 nodes, the derived status `complete`).
`docker compose down` after the capture.

### MEDIUM

**F-P4-STALE-IMAGE — the compose cold start silently serves a stale
app.** `image: taskq-example:dev` is a worktree-GLOBAL tag; compose
reuses the image when present and does not rebuild. The first cold
`docker compose up -d` served an image built at 00:33 from ANOTHER
tree: the trigger 404ed (`{"detail":"Not Found"}`), the openapi showed
ZERO workflow routes, and the image's `examples/app.py` contains no
`doc_ingest` at all (`grep -c` = 0, captured). Only
`docker compose up -d --build` cured it. A multi-checkout operator (or
any second clone) gets yesterday's demo with today's README. Cure
shape: a tree-scoped image tag, or `pull_policy: build`/`--build` in
the README's command.

**F-P4-CLI-KEYERROR-TRACEBACK — the CLI's named-refusal contract breaks
on the stale-app overlap.** `flows resolve <id> <json> --app appmod:app`
against an app that does not declare the run's workflow (the stale-deploy
world — exactly the resolve×cancel overlap's shape): `gate_models_for`
→ `app.get(workflow)` raises an uncaught `KeyError` → the operator sees
a RICH TRACEBACK + exit 1 (`attack4-cli-races-run1`/the race repro):
`KeyError: "workflow 'attack4_hold_flow' is not declared on this app"`.
The ADMIN's twin (`_validate_through_gates`) catches `KeyError` and
continues; the CLI's `_validate_through_gate` (`cli.py:4185`) does not.
The output contract ("the named error, never a traceback") is broken.

**F-P4-WHYSTUCK-LADDER-LIE — the status read drops the attempt
counters.** `_flows_status` (`cli.py:4075-4091`) builds `FlowNodeRow`
without `attempt`/`max_attempts` (the dataclass defaults: 0 and 3). A
node that failed on an EXHAUSTED ladder (observed: attempt=1,
max_attempts=1) reports "ladder headroom 3 attempt(s) left" — the lie
is on EVERY failed row whose ladder is not the default. Captured in
`attack4-cli-races-run5`'s output; pinned xfail(strict)
(`test_the_why_stuck_arm_names_the_TRUE_blocker`).

**F-P4-WHYSTUCK-FALSE-REMEDY — a blocked-after-failed-parent row reads
as a join-wait with a false remedy.** The same run: `downstream`'s
parent finalized as a FAILURE, the child can never run, yet
`flows status` says:

```
downstream: JOIN-WAIT — waiting on 1 upstream result(s)
     remedy: none — the join fires when its parents finalize
```

The join will NEVER fire (the parent finalized; the row's
`blocking_reason` was never stamped — the seeded shape shows
`deps_pending=1` with NO `blocking_reason` after 15 ticks). `stuck_lines`
(`_cli.py:125`) checks `is_join_wait` BEFORE the blocked-reason arm and
never reconciles the deps counter against the parents' terminal states.
The operator's remedy line is a false promise. Pinned xfail(strict)
in the same test.

### LOW

**F-P4-CRON-202 — the slot replay's status-code drift.**
`examples/app.py:342`'s comment promises "answers 200 with the EXISTING
run"; the code always returns 202 (`attack4-demo-cron-*.txt`: the same
slot twice → the SAME run_id, both `[202]`). The IDEMPOTENCY held
(the capture: same slot → same id; a new slot → a new id); the comment
drifts.

**F-P4-SSE-DRY — the run stream is a second SSE dialect outside the
estate's protections.** The DRY walk: `admin/sse.py` (the jobs' event
stream) carries the per-topic connection semaphore
(`admin_max_sse_connections`), the OTel connect/close/reject counters,
the reconnect backoff, and the shared `_SSE_HEADERS`/`_KEEPALIVE_INTERVAL`
constants. The NEW run stream (`_wf_actions._stream_generator`) repeats
the keepalive/headers INLINE (`_STREAM_KEEPALIVE_S = 30.0`, the same
headers dict re-written), carries NO connection bound and NO OTel
counters, and the frame conventions differ (raw `id:`/`event:` strings
vs `progress_stream_generator`'s dict-mapped frames). The seq-cursor
law itself is applied correctly but COPIED per generator (T11's law,
three handwritten implementations). Not a defect today (the poll-driven
stream holds one connection per browser and the pool is bounded) — but
the next SSE surface will copy the unprotected shape. The poll-vs-LISTEN
split is a declared design; the unbounded-connections + the duplicated
constants are the DRY debt.

**F-P4-PREEXISTING-FLAKE — the web_admin degrade test is
order-fragile (not attack-caused).**
`test_the_pages_degrade_when_the_workflow_tables_are_absent` reds in a
SERIAL joint run of the BUILDER'S OWN files with NO attack files
present (captured: `1 failed, 268 passed`), and greens solo. The attack
suite shares no fixtures with it.

---

## THE HELD LANES (attacked, no finding)

* **THE XSS SURFACE — HELD.** A node's `error_message` carrying
  `<script>alert(1)</script>`, `<img src=x onerror=…>`, and a literal
  `</script>` closer: the run page renders no raw markup (autoescape
  on — `_factory.py:945`), the `wf-boot` JSON survives its
  `<script id="wf-boot">` element UNBROKEN (the tojson filter breaks
  the closer — the parse succeeding is the proof), the panel API
  carries the error as a JSON STRING (`application/json`), and
  `workflows.js` writes every dynamic line via `textContent` (the only
  `innerHTML` sites are the vendored mermaid's SVG + the two
  deliberate clears — pinned). The hold's `reason` carrying markup
  renders escaped-and-readable (`&lt;b&gt;…`). The existing conventions
  held.
* **THE SSE REPLAY CONTRACT — HELD under chaos.** Hostile cursors
  (`999999`, `-5`, `not-a-number`, `1e9`) → the next frame is the FULL
  snapshot, `seq >= 1`, no 500; the KILLED socket mid-stream → the
  server survives (a later read 200s) and the reconnect replays the
  identical state; TWO browsers on one run → each consumer's own seq is
  monotonic, both see the same transition order, and the DOUBLE resolve
  (two operators) lands exactly-once (`delivered` + `no-op`).
* **THE ROWS==DOM CHECK — HELD under a resolve landing mid-render**
  (10 renders racing one resolve): every page's boot JSON is internally
  consistent — the §17.5 derivation over its OWN node states equals its
  status, whichever side of the race it caught.
* **THE PANEL LATENCY — the pinned 50 ms band holds with ~38× headroom
  at map scale, measured MYSELF** on a 200-child map (a real run, the
  fork's 200 children): the node panel's p95 ≈ **1.3 ms**; the run
  page's median ≈ 40 ms region (both captured in
  `attack4-panel-latency-200map.json` — my own file, written by the
  test).
* **THE AUDIT TRAIL'S COMPLETENESS — HELD.** Resolve + cancel each
  land their `admin_audit` row (attributed: `hitl.resolve`,
  `workflow.cancel`, principal named), the page renders both, and the
  no-op cancel writes nothing.
* **THE CLI'S EXIT-CODE CONTRACT — HELD** in the empty/error/permission
  states: a bad UUID → exit 1 + "invalid run id"; an unknown run →
  exit 1 + the remedy; an empty schema's `flows list` → exit 0 + the
  honest zero; an unknown hold → exit 1 + the named error; a terminal
  run's cancel → exit 0 + the no-op line + ZERO audit rows; the
  malformed payloads → the NAMED pydantic refusal (the field named)
  with the hold SURVIVING every time.
* **THE RESOLVE×CANCEL RACE — the outcomes honest** (with the typed
  door intact): the resolve refused ("hold … is 'cancelled'") + exit 1,
  the cancel succeeded + exit 0, the rows and the outputs agree, no
  traceback.
* **THE NOTHING-STUCK SHAPES — HELD**: a body that cancels ITS OWN run
  mid-emit lands in a named terminal with no running child left
  behind; a resolve arriving after the flow went terminal is a defined
  typed outcome with no late wake (no pending child on a succeeded
  root); the 3-round DB-restart probe (immediate-stop mid-round, the
  WAL recovery, reconnect, re-drive) lands EVERY node terminal — zero
  jobs lost across the restarts (`attack4-restart-capture-*.txt`).
* **THE COVERAGE RESIDUALS — CLOSED by the attack's own tests**
  (the p4 report's `p4-coverage-split4.txt` lists re-verified still-missing
  on the certified head BEFORE the closers — the builder's `5ea18be3`
  "coverage closers" did not close them):

  | module | the report | after attack4 | the joint capture |
  | --- | --- | --- | --- |
  | `api/_hitl.py` | 68% | **92%** | `attack4-cov-joint-CLOSED-*.txt` |
  | `api/_loop.py` | 80% | **100%** | " |
  | `api/_ctx.py` | 81% | **94%** | " |
  | `api/_ctx_wait.py` | 86% | **99%** | " |
  | `engine.py` | 87% | **99%** | " |

  The NOT-closed, named: `_ctx.py:161-163` (the jsonb-as-TEXT decode
  arm — asyncpg 1.x hands jsonb decoded on every connection the test
  can construct; the arm guards a world the driver cannot produce — a
  test would pin a lie); `_hitl.py:514-532`'s remainder + `557` (the
  cold-fit walk's inner arms — the cold SUBPROCESS probe refuses via
  the catalog-free arm; the residual arms need a second DELCARED model
  per hold, a shape the current gate compile cannot mint); the
  partial-branch arcs (`583->596`, `636->622`, `259->278`, `346->353`)
  that re-enter lines already covered — a test can only re-run the same
  shape; `_runner.py`'s forked-child arms — the t20-fence probe's lane,
  a duplicate pin would assert nothing new.
* **THE TYPE PROBES — red on BOTH checkers** (the negative corpus for
  the new admin/CLI surfaces: `FlowNodeRow`'s typo field,
  `format_flow_status`'s kwarg lie, `format_flow_list`'s non-sequence,
  the parse boundary's wrong index, `GateDecl`'s unknown kwarg, the
  UUID boundary): pyright `reportCallIssue`/`reportArgumentType`/
  `reportIndexIssue`, ty `unknown-argument`/`invalid-argument-type` —
  the captures `attack4-pyright-final-*.txt` + `attack4-ty-final-*.txt`.
  The attack's own files: pyright 0 errors, ruff clean.
* **THE GOD-FILE SMELL — CLEAN on the new modules**: `_cli.py` 279,
  `admin/workflows.py` 270, `_wf_actions.py` 418, `_wf_rows.py` 300 —
  all under the estate's concern-boundary sizes; the split owner's
  `_hitl.py` (856) and the pre-existing `_runner.py` (943) are the
  largest of the family but predate/grew with the phase and are
  single-concern. No new god file.
* **THE GREP GATE — HELD, 0 hits** (the attacker's own run of the
  abstraction contract over `docs/`, `examples/`, `src/taskq/workflows/`:
  zero case-study strings; `attack4-grep-gate-hits.txt`).

---

## THE CERTIFICATION

**REFUSED** — pending three cures, all small:

1. **the torn cancel** (F-P4-TORN-CANCEL): the signal leg takes the
   caller's connection — the red test is already the fixer's target;
2. **the untyped cold door** (F-P4-UNTYPED-COLD-DOOR): a schema-less
   hold must refuse the delivery LOUDLY (the legacy arm fits nothing);
3. **the demo's dead resolve** (F-P4-DEMO-DEAD-RESOLVE): the compose
   `app` service gets `TASKQ_ADMIN_ACTIONS_ENABLED: "true"` and the
   sidecar mounts `workflow_app=...` (or the demo's README block is
   rewritten to the stack's real surface), plus the stale-image tag
   scoping (F-P4-STALE-IMAGE).

The CLI's traceback (F-P4-CLI-KEYERROR-TRACEBACK) and the two why-stuck
honesty defects (F-P4-WHYSTUCK-*) ride the same fix wave. Everything
else the brief attacked — the SSE chaos, the XSS surface, the
concurrency core, the exit-code contract, the restart rounds, the
coverage floor — is CERTIFIED HELD.

— the phase-4 attacker, alone; the PG on :5711 destroyed after the run.
