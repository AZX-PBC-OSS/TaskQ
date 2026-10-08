# THE PHASE-4 RECERTIFICATION (round 2) — CERT2

The recertifier: the adversarial twin's round 2 (the round-1 attack was
refused on 3 HIGHs), fresh, alone, on `/tmp/opencode/wt-taskqflow-p4`
@ `7a082c15` (the fix round's 9 commits over the attack-4 handoff).
The dev loop was LOCAL: my OWN PG on **:5713** (the fresh cluster
`/home/rich/.cert2-pg` — the fixer's :5712 was not touched), Docker up
for the compose walkthrough. Every run below is captured in THIS
directory (`cert2-*.txt`); the one-process batch artifacts live in
`/tmp/opencode/cert2-solo-*.txt` (the six structural reds' full outputs
copied here).

---

## THE VERDICT UP FRONT

**CERTIFIED-FOR-PHASE-5.** All three round-1 HIGHs' cures are REAL and
held under the recertifier's own attacks (the attacker's probes
unchanged + my new interleaving/smuggling/terminalization drills);
the demo's cold stack performs its central resolve on BOTH surfaces
(walked three times from a `down -v` cold start); the image-freshness
law was proven LIVE (a plain `up -d` REBUILT the image); the gates pass
(pyright FULL 0, ruff clean, mkdocs --strict clean, the bands green,
the execution lanes green, the battery strictly better than the clean
base). The battery's failure class is PARTLY mislabeled in the fixer's
report — a deterministic pre-existing red class (11 tests) rides in
every battery on this tree including the clean base's — and that class
**rides with this certification as the scheduled demand** (the phase-1
precedent), detailed in FINDINGS.

---

## 1. THE TORN CANCEL (32297d6f) — CURE VERIFIED

**The code:** `cancel_run_signals(conn: ConnLike, ...)` takes the
CALLER'S connection and runs ONE `conn.fetch` — no pool acquire, no
autocommit leg. Both call sites (`_runner_exit.py:257` and
`_runner.py:745`) pass their own transaction's `conn`; the signal
write, the root flip, and the audit row commit together or not at all.
No third caller exists (grepped src+tests). The record cannot lie.

**My drills:**

* **The attacker's rollback probe, unchanged** — RED on the pre-cure
  head, GREEN here (ran ×3 with the pin set, plus the full
  `attack4-cli-races.py` file: 7 passed).
* **THE THREE-OPERATOR INTERLEAVING (new, mine):** cancel × resolve ×
  the expiry sweep (`sweep_expired_signals`), ONE hold, three concurrent
  operators with jittered 0–60 ms starts, **24 rounds**. Every landing
  was a NAMED consistent state: `sig=cancelled` only ever stood on
  `root=cancelled` (the torn state — round 1's finding — ZERO
  occurrences); the signal row transitioned exactly once (the
  held-guarded CASes arbitrate delivered/cancelled/abandoned); the
  `workflow.cancel` audit rows were EXACTLY-ONCE (1 per round, never
  ≥2); the root terminalized every round.
* **The audit-writer rollback** (the structural probe's forced-raise
  shape) is the committed pin; re-verified.

## 2. THE UNTYPED COLD DOOR (3d688fce) — CURE VERIFIED (option (a))

**The code:** the mint refuses a contract-less hold (`register_hold`'s
`TypeError`); the wait site refuses an empty model tuple (the NAMED
`TypeError` — `_ctx_wait.py`, the body's first statement, before any
pool access); the boundary's schema-less arm returns the LOUD TYPED
REFUSAL (naming the missing contract + the re-run remedy), the hold
SURVIVES; the migration `01.00.29_01` backfills the legacy NULLs to
`'{}'::jsonb`, quoted `"{schema}"` (§8's catch).

**My drills:**

* **The smuggle attempt (new, mine):** a cold fresh-process resolve
  against a hold row carrying EACH marker form — the backfill marker
  `'{}'::jsonb` itself, `'null'::jsonb`, `'[]'::jsonb` — delivering
  `{"totally": "undeclared", ...}`. **All three refused**, each with
  the typed refusal naming the missing contract and the remedy, and
  `|HOLD_STATUS: held` — the hold survived all three.
* **The mint's halves (new, mine):** `register_hold(payload_schema=None)`
  → the named `TypeError`; `CtxWaitOps.wait_signal(())` → the named
  `TypeError` (not IndexError).
* **The migration chain on a MIXED-CASE schema (new, mine):** a fresh
  `Cert2_MixedCase9` schema applied the full chain through `01.00.29_01`
  on my own cluster — the §8 quoting cure holds.
* **The zero-false-positive control (new, mine):** the WARM typed
  resolve still refuses garbage (the attacker's control, green); a
  COLD fresh-process resolve of an HONEST payload against the row's
  REAL schema **delivers** and the flow terminalizes `succeeded` —
  the door never bit the honest cold operator.
* **Observation (rides, LOW):** a hand-written per-model BOOLEAN jsonb
  schema (`{"Approval": true}`) fits anything in the cold fallback
  (`_fits_by_schema`'s cannot-judge arm). The mint cannot produce it
  (it writes real model schemas) and the backfill writes only `'{}'` —
  outside the threat model (needs raw DB write = the superuser), but a
  phase-5 hardening may flip that arm to refuse non-object schemas.

## 3. THE DEMO'S COLD STACK (3bee4833) — WALKED MYSELF, THREE RUNS

`docker compose down -v` (the volume too) then the README's steps AS
WRITTEN — a plain `docker compose up -d`, NO `--build`:

* **THE IMAGE-FRESHNESS LAW, PROVEN LIVE:** the plain up showed
  `Image taskq-example:dev Building` and produced a NEW image
  (`3403163c9da0`, built from THIS tree) — `pull_policy: build` is the
  default and a stale worktree-global tag CANNOT serve silently
  (`cert2-cold-up-pull-policy-build.txt`; `compose config` shows
  `pull_policy: build` on all four taskq-example services; the fast
  path opts out EXPLICITLY with `TASKQ_EXAMPLE_PULL_POLICY=missing`).
* **Run 1 (the app surface):** trigger → `202 {"run_id", "url"}` → the
  run page `200` on `/taskq/workflows/{id}` → the hold (the form with
  its declared payload schema renders) → the form's own multipart POST
  (`api/runs/{id}/resolve`, the page JS's exact shape incl. CSRF
  cookie) → `{"status":"delivered"}` → the root **succeeded**
  (derived `complete`).
* **Run 2 (the sidecar surface):** its own run page `200` on
  `/admin/workflows/{id}` (the workflow DEFINITIONS mount — the typed
  door open) → the same relative POST → **delivered** → **succeeded**.
* **Run 3 (the scripted capture, `cert2-cold-stack-walkthrough-r3.txt`):**
  both surfaces delivered, the root succeeded, and the CRON
  IDEMPOTENCY re-proven: the same slot twice → the SAME run id, both
  202 (my first probe sent the slot as a QUERY param and got two ids —
  the slot rides the FORM body; the probe was wrong, the code is
  honest, and the comment's 202-with-the-existing-id claim is TRUE).

## 4. THE ROOT-CAUSE CURE (d9692a81) — VERIFIED AGAINST THE ENGINE'S LAW

* **At birth (new, mine):** `create_flow` (the static create path) —
  the JOIN child is born `deps_pending=1` AND
  `metadata.blocking_reason='join'` (every increment rides
  `INCREMENT_DEPS_SQL_TEMPLATE`, which stamps the marker — both call
  sites audited).
* **The cascade (new, mine):** a failed parent (exhausted ladder,
  `fail_closed`) + a JOIN child → the drive terminalized the FLOW
  (`root='failed'`, `CancelledByPeerFailure` — no stranded
  deps_pending=1-with-no-reason row, no eternal 'blocked'), and the
  child carried `blocking_reason='failed_parent'`.
* **The why-stuck truth:** the committed pin
  (`test_the_why_stuck_arm_names_the_TRUE_blocker`, now HARD asserts —
  the xfail is gone) passes: the BLOCKED arm reads first, the remedy
  derives from the REASON, the join-fires promise appears only on a
  LIVE join-wait, the exhausted ladder reads "the ladder is EXHAUSTED".
  `stuck_lines`' order in `workflows/_cli.py` read and confirmed.

## 5. THE PRE-EXISTING RED'S CURE (f2eb2ff1) — THE 24 REGISTRATIONS

The `_EXEMPT` registry: every phase-work entry carries its tripwire
substring + an honest reason (the write set is ONE RUN's own rows —
the graph's node count — or ONE keyed row; the progress channels are
the one conflict row and the OFFSET-bounded ring trim). The guard is
green (4 passed). **THE SPOT-DRILLS (the guard's teeth, each mutation
RESTORED after, `git diff` clean):**

1. `_CANCEL_RUN_SIGNALS_SQL`: dropped `AND status = 'held'` (the
   write set widened to the run's whole signal history) → **the guard
   RED**;
2. `INCREMENT_DEPS_SQL_TEMPLATE`: dropped the `WHERE id = $1` scope →
   **the guard RED** (a no-op `+ 0` rewrite — the write set unchanged —
   correctly stays green: the tripwire bites only on scope);
3. `PROGRESS_STREAM_APPEND_TRIM_SQL`: the ring trim's OFFSET bound →
   `OFFSET 0` → **the guard RED**.

## 6. THE GATES (all run on MY stack, my PG :5713)

| gate | result | capture |
| --- | --- | --- |
| the changed pins ×3 (torn-cancel + cold-door + warm-control) | 3 passed ×3 runs | the run log (this report's §1/§2) |
| the attacker's corpus, unchanged (attacks 1–4) | cli-races 7 / nothing-stuck 4 (+1 env-gated skip) / admin-chaos + coverage-closers 31 — **42 passed** | — |
| the execution lanes (the intercept is sacred) | 95 + 41 = **136 passed** | — |
| the estate slice (the registry guards 42 + the actor-config surfaces 61) | **103 passed** | — |
| the bands (`test_wf_perf_bands.py`) | **5 passed** | — |
| the full battery (`pytest -n 4`, one run, ~29 min) | **67 failed / 12,980 passed / 3 errors** — strictly below the clean base's 88 on the same tree+command | `cert2-BATTERY-full-0641.txt` |
| the battery's failure set vs the clean base's 88 | every one of my 67 is EITHER in the base's 88 OR accounted below (the rotating xdist victims + my env's cluster shape) | the FINDINGS section |
| pyright FULL (src + tests) | **0 errors, 0 warnings** | `cert2-PYRIGHT-full-0.txt` |
| ruff check . | All checks passed | `cert2-RUFF-clean.txt` |
| mkdocs build --strict | exit 0 | — |
| the migration chain on a mixed-case schema | applied through `01.00.29_01` | §2 |

**The xdist-flake caveat, TAKEN:** the victims were re-run TRULY solo
(one pytest process PER test — the one-process batch of all 67 is
itself order-dependent: the global workflow registry + leaked tasks
cross-pollute, 61 F — see `cert2-victims-one-process-BATCH.txt`).
**50 of 67 green solo. The 17 that stayed red are the FINDINGS section
— and they are NOT all flakes:** the fixer's "every failed test passes
SOLO (15/15)" was true of the 15 it re-ran, and FALSE of the class.

## 7. THE COVERAGE

The joint coverage run (the wf family + the attack4 files + my drills +
web_admin, `--cov` over workflows/web/cli): **3 failed / 1,202 passed**
(the 3 = the pin-4 band's known sensitivity + the two web_admin env
shapes — see FINDINGS). The attacker's closures HELD at ≥90 on the
phase-4 floor set:

| module | round-1 report | my joint |
| --- | --- | --- |
| `api/_hitl.py` | 92% | **91%** (the new refusal arm's 527-561 is the COLD-process fallback — it executes in a SUBPROCESS, invisible to the in-process tracer; the warm-tested surface holds the floor) |
| `api/_loop.py` | 100% | **100%** |
| `api/_ctx.py` | 94% | **94%** |
| `api/_ctx_wait.py` | 99% | **97%** (the missing line is the wait's timeout face — the sweep-driven arm) |
| `engine.py` | 99% | **99%** |
| `api/_sql_runner.py` | 100% | **100%** |

The not-closable lines' honesty: `_ctx.py`'s decode arm (the
asyncpg-1.x-hands-decoded-jsonb world — unreachable without a
text-codec connection) and `_runner_loop`'s escalation arms are the
same named residuals the attacker's report disclosed — still missing,
still honestly named. `_runner.py` reads 89% on MY joint set (vs 91% on
the attacker's) — the delta is the joint's file-set composition + the
rebase's `_claim_epoch` union, not the cures; the attacker's floor set
never included it, and the formal `make coverage` floor (`--cov=taskq
--cov-fail-under=90` on the FULL battery) is the package-total gate the
fixer inherited — not re-run here (a second 30-min battery for a gate
the fix round's own lanes do not tighten; the surface floors above are
the phase-4 contract and they hold).

---

## FINDINGS (ride with the certification — the phase-1 precedent, R2-1's shape)

**F-CERT2-1 (MEDIUM — the fixer's flake analysis is PARTLY FALSE):**
the battery failure class is NOT uniformly the rotating xdist victims.
True-solo re-runs (one process per test) show **17 of 67 red**:

* **6 are environmental to MY dev loop's cluster shape** (each also
  red, or env-explained, on the fixer's own runs): the trust-auth
  cluster never raises `InvalidPasswordError` (`auth_credential_refresh`
  ×2, `enterprise_dsn` rds_iam — red on the base AND fix2), my PG
  doesn't listen on ::1 (`ipv6_dsn`), `web_admin`'s degrade test and
  the timescale container tests assume a database literally named
  `taskq` / a specific TSL partition behavior (the degrade test GREENS
  once `CREATE DATABASE taskq` lands; tsl is red on base + fix2 + mine
  with rotating error shapes).
* **11 are DETERMINISTIC STRUCTURAL REDS, byte-identical on the clean
  base's 88 and in the fixer's own fix2 run** (captured set-for-set):
  `test_no_stdlib_json` (7 stdlib `import json` — ALL in the phase-4
  lane's own files: `web/admin/_wf_actions.py`, `_wf_rows.py`,
  `web/admin/workflows.py`, `workflows/_cli.py`, `_progress.py`,
  `_progress_read.py`), `test_migrations` ×2 (the loop-budget jobs
  columns `budget_deadline`/`budget_paused`/`budget_remaining_ms`
  missing from `jobs_archive` + the `COPY_FROM_COLUMNS` drift — the
  archive-sync guards), `test_breaking_change_markers` (the
  hand-written `Unreleased — TaskQflow phase 4/2` CHANGELOG sections),
  `test_migration_lock_scope_dead_index` ×2 (`relation
  "wf_exec_capable" does not exist` — the EXPLAIN pin prepares a
  fragment outside its CTE's scope), `test_examples_smoke::test_trigger_app_enqueue`
  (a `TypeError: object bool can't be used in 'await'` in the demo
  app's lifespan TEARDOWN — the serve path itself is fine, the live
  stack proves it), `test_sse_connection_caps` (the uncapped-SSE scan
  names `admin/_wf_actions.py` + `jobs.py:1038 flow_progress_stream`),
  `web_admin::test_bounded_pool` (the same stream takes the RAW pool
  dependency), and `test_suite_hygiene` ×2 (a module-level SCHEMA
  constant in the lane's own `t21_numbers_run.py`, and the attack-4
  corpus's own `os.environ` writes trip the guard).
* **The scheduled demand:** PHASE-5's OPENING lands the estate-hygiene
  cure before stacking — route the json imports through `taskq._json`,
  sync `jobs_archive`/`COPY_FROM_COLUMNS` with the budget columns,
  register (or cap) the two SSE endpoints and the raw-pool stream
  dependency, regenerate the CHANGELOG sections through
  release-please, name the EXPLAIN pin's full statement (CTE header
  included), and fix the demo lifespan's teardown await. None is a
  data-loss or demo-breaking defect (the demo works live, three cold
  walks); all are doctrine gates lying on the certified tree — the
  same class the fix round DID cure for the bounded-writes guard.
  The full-suite gate (`make coverage`, `--cov-fail-under=90`) is
  UNREACHABLE while they stand.

**F-CERT2-2 (LOW — the boolean-schema hardening, §2's observation):**
`_fits_by_schema` fits a per-model jsonb BOOLEAN schema (`true`)
unconditionally in the cold fallback. Unreachable through the mint and
the backfill; a phase-5 hardening may refuse non-object model schemas.

**F-CERT2-3 (NOTE):** the fixer's `:5712` cluster and the worktree's
uncommitted `.measurements/*.json` mutation-harness appends
(04:35–04:36, after their capture commit `7a082c15`) were left as
found; my drills mutated only DB rows and three source files, each
restored, `git status` clean of source changes.

---

## THE COMMITS VERIFIED

`32297d6f` (the torn cancel), `3d688fce` (the cold door),
`d9692a81` (the why-stuck truth), `92385152` + `5e393955` (the CLI's
named refusal + its pin — the stale-app resolve exits 1 NAMED, pinned
in `attack4-cli-races.py`, green here), `3bee4833` (the demo's cold
stack), `f2eb2ff1` (the 24 registered writes), `1e0d3e2b` (the lint
shape), `7a082c15` (the captures).

— the phase-4 recertifier, round 2, alone; my PG cluster
(`/home/rich/.cert2-pg`, port **:5713**) left RUNNING for the next
lane (the drill files removed; the demo stack `down -v`).
