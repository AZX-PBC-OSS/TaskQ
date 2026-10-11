# The sync-friction cures lane — the ten-item audit, verified at 0e2f5cc09 and cured on feat/taskqflow-syncfriction

Audited tip: `0e2f5cc09` (feat/taskqflow). The probe's audit lives at
`.measurements/probe-sync-FRICTION-AUDIT.md` in wt-probe-sync; this lane
verified each item AT THE TIP before curing — four of the ten do not hold
there (the probe read a stale tree or hypothesized without running), six
hold and are cured. Live receipts were captured against a dedicated PG 17
(:5808, destroyed at close).

## THE BRANCH'S FINAL SHAPE (the head-stamp law's own two-step, times three)

- `351e1863f` — R, the source-final head: the cures + the pins (red-first
  pins green here) + this report.
- `3af85243c` — M1: every content stem's live claim re-recorded at R
  (lint-format, pyright-full 0/0/0, typeprobe-gate, mkdocs-strict,
  deslop-pin-packs, wf-bands-rv4 + the five perf-band stems, wf-rollup-band,
  wf-scoped-coverage 93% green, wf-battery-rv4 — whose single red is
  rv2-13's pin, truthful at R: the estate's cure is M1 landing).
- `d9e4a3cc9` — M2: the fast-tier-n8 leg-1 capture at M1 (rv2-13 still
  circularly red at M1's committed estate — M2 is its cure — plus one
  hypertables differential flake under -n 8 load, solo-green re-verified)
  + the two receipts (rv2-13-green, evidence-heads-verify rc=0 at M1).
- `4e6d65705` — M3: the capture FILES ride with the registry (the
  .gitignore'd runs/* force-added — the index had named captures the
  commits never carried, the GHOST arms the verifier convicts). At M3:
  rv2-13's pin GREEN on the committed tree, the verifier rc=0.
- `14e61d6df` — M4: THE FAST TIER's ZERO-FAILURES capture at M3
  (13429 passed / 7 skipped / 1 xfailed / 0 failed, rc=0 at -n 8 with the
  docs tier in) — the lane's loaded bar.

## REFUTED with evidence

### Item 5 — "the workflows/run machinery is invisible to a docs-only reader"

Stale snapshot. At the tip:

- `docs/examples/doc-ingest.md` **exists** (the probe: "no docs/examples/
  directory") — `examples/workflows.py:2`'s reference resolves.
- `docs/guides/workflows.md` is in the nav (`mkdocs.yml:105`) and teaches
  `step_key` (lines 323, 532, 1723) and `map_index` (153, 274, 1088,
  1253-1258, 1317, 1387, 1723 — including "THE REFUTED-CLAIM DISCIPLINE:
  map_index is load-bearing" and the run explorer's `?map_index=N`
  addressing via `docs/taskqflow-coverage.md:37`).
- `docs/api-reference/workflows.md` renders (`mkdocs.yml:136`), and
  `api-reference/cli.md:15-17` cross-links the workflow guide.
- `mkdocs build --strict` passes with zero broken links.

No cure needed; nothing to fix at this tip.

### Item 2 — "the shipped example teaches a deadlock" (as stated)

The claim's headline does not survive a RUN. Live E2E of the example's
exact flow (real worker, real PG, `examples.actors.batch.batch_counter` →
3 × `counter` → `batch_finalizer` stamped `metadata={"batch_id": ...}`):

- the finalizer row lands **unstamped** — `{"role": "finalizer"}` only —
  and **succeeds on attempt 1**, receipt:
  `batch-complete ... succeeded=3, failed=0, total=3`.

Why: `batch_id` is a **reserved metadata key** —
`client/_args.py::build_enqueue_args` pops it from every caller-supplied
metadata dict ("Security boundary: a single-job enqueue cannot
self-assert membership in a victim batch"), and the plain `enqueue()` the
example uses crosses that boundary. The stamped finalizer is unreachable
through the public API.

The deadlock IS real where the API boundary is absent: the raw-SQL repro
(the finalizer row hand-inserted with `metadata.batch_id`, status
`running`) shows `wait_for_batch` counting 4 members and Snoozing forever
past 6 s with all 3 children succeeded — captured in
`/tmp/opencode/syncfric-mod/repro_deadlock.py` (ARM A) vs the unstamped
arm completing (`succeeded=3 total=3 pending=0`). The docs' law holds; it
is the invariant the API strip enforces.

## CONFIRMED and cured

### Item 1 — PARTIAL: the streaming docs' atomic flip is stated, the table rows are not

The probe's "the streaming section never repeats this" is stale —
`jobs-clients.md`'s streaming section states the delegation to
`enqueue_batch_atomic` right after its parameter table, and the source
agrees (`client/_jobs.py`: `if has_batch_extras and connection is None:`
→ the atomic path). But the `finalizer` and `failure_policy` rows of the
streaming section's OWN table — the rows a reader actually consults when
adding them — carried no warning. Cured: both rows now name the atomic
flip and what it undoes (the eager handoff / incremental commits).

### Item 2 — the adjacent defect: a reserved key taught as if it worked

The example taught `metadata.batch_id` on the finalizer — a key that
**silently never lands on the row**. Cured:

- `examples/actors/batch.py`: the finalizer enqueue carries
  `metadata={"role": "finalizer"}` with a Why-comment pointing at the
  reserved-key strip and the docs' law; the batch link rides the payload.
- `jobs-clients.md`: the `enqueue()` metadata row names the reserved-key
  strip (`batch_id`, `cron_schedule_id` — stripped, never rejected); the
  "Finalizer is NOT stamped with batch_id" section adds the mechanical-
  enforcement sentence and links it.
- Red-first pin: `tests/test_examples_batch_example.py` (asserts the
  finalizer's metadata carries no `batch_id` at the example's call site) —
  RED at the tip, GREEN here.

### Item 3 — `jobs.status` is the `taskq.job_status` enum

Confirmed: `migrations/01.00.00_01_pre_initial.sql:55` (CREATE TYPE) and
:79 (the column); zero docs mention the cast (verified by grep). Cured:
the "Writing your own status SQL?" note at `BatchHandle.status()` —
`WHERE status::text = 'succeeded'` with the exact
`operator does not exist: taskq.job_status = text` failure quoted.

### Item 4 — `taskq worker --queues a b c` errors "No such command 'b'"

Confirmed live (`No such command 'review'.` under the rich panel). The
parser declares repeat-the-flag (`list[str]` on one `--queues` name; the
help states it; `cli.md:311-325` teaches it; accepting space-separated
form would need a catch-all positional that silently swallows typos —
refused). Cured in the error face: `_WorkerCommandGroup` on
`worker_app` overrides `resolve_command` — a bare token now errors
"unexpected argument 'b' — `taskq worker` takes no subcommands. Queue
names go on the repeatable --queues flag, once per queue: --queues=ingest
--queues=review (or TASKQ_QUEUES=ingest,review)." Pins (red at tip →
green): `tests/test_syncfriction_pins.py::TestQueuesFlagTeachesRepeatForm`
— the error must name `--queues` and "repeat", must not say "No such
command", and the repeat form must still parse to `["a","b","c"]`.

### Item 6 — PARTIAL: until-idle taught in workers.md, absent from the quick-start

`workers.md:912-946` is a full "Until-idle mode" section (the probe's
"eight lines" undersells it); the quick-start's `worker_main` block
(line 205-212) never mentioned it. Cured: a quick-start paragraph after
the in-process worker block — `until_idle=True`, the 0/3/4 exit codes,
and the workers.md#until-idle-mode cross-link.

### Item 7 — the default-DSN footgun

Confirmed live: a worker without `TASKQ_PG_DSN` on a machine with a
foreign server on 5432 crashes `InvalidPasswordError: password
authentication failed for user "taskq"` — teaching "wrong password",
not "wrong server". The boot log prints `host_direct` (no port, no DSN).
Cured at both faces:

- the error face (small and typed, per the lane's license): the worker
  command's `except InvalidPasswordError` prints the resolved
  password-masked DSN (`_redacted_dsn`: `postgresql://taskq:***@localhost
  :5432/taskq`) and names `TASKQ_PG_DSN` and its default.
- the quick-start: a DSN-footgun callout at the env paragraph.

Pin (red at tip → green):
`tests/test_syncfriction_pins.py::TestWorkerDsnErrorTeachesDsn`.

### Item 8 — fast jobs' progress coalesces; the docs' example over-implies

Confirmed live: a 30-item actor calling `ctx.progress()` per item
(sub-100 ms total) left exactly ONE snapshot on the row
(`progress_seq=32`, `step=30`, `percent=100` — the terminal state; zero
intermediate durable events) and the consumer's stream captured ~1 event.
The row truth (`settings.py:2158`: `progress_coalesce_interval` default
0.5 s) and the design ("only the latest value for each field") are fine —
the guide's sleeping-transcode example implied one event per call
without saying it rests on the sleeps. Cured: the progress.md note —
per-item progress on sub-second jobs coalesces to ~one snapshot per
flush; push step counters, not per-item events, for a dense stream; a
thin stream on a fast job is the design, not a failure.

### Item 9 — TaskQ / JobsClient parity undocumented

Confirmed by runtime `dir()` diff. `TaskQ` implements every `JobsClient`
method except `invalidate_actor_capacity_cache()` (a raw-client knob) and
adds `open`/`close`, `stream`, `retry_job`, `watch_reclaims`,
`reload_credentials`, `actors` — 18 shared methods. Neither face of the
docs named the diff (the facade section said only "the job operations
plus tq.actors"). Cured: the parity paragraph in the `TaskQ()` section of
jobs-clients.md — the one exception and the six extras, named.

### Item 10 — no CLI schedule run-now

Confirmed: the CLI has NO schedule command family at all (no list, no
get, no run-now — `taskq --help` shows `dev/doctor/insights/migrate/
worker/health/ui/workgroup/actor-config/queues/queue/job/flows`); run-now
exists only as the admin's `POST /schedules/{id}/run` (ops.py:444) and
its button. A lone `run-now` without a schedule family would be the
half-feature the lane forbids (nothing on the CLI could even discover a
schedule id). Cured as the docs note: cron.md's run-now section carries
the "Run-now is a UI/admin face; the CLI has no schedule commands" callout.

## Gates (all green at the branch's final head 14e61d6df)

- ruff check + ruff format --check: clean.
- pyright FULL (`pyright src/taskq tests`): 0 errors, 0 warnings,
  0 informations.
- the type gate (`tests/typeprobe/_gate.py`): holds — every MUST_ERROR
  marker (39) reds on pyright 1.1.414 AND ty 0.0.85, no error outside
  the markers.
- `mkdocs build --strict`: clean.
- `TASKQ_DOCS_EXAMPLES=1 pytest -m docs_examples`: 96 passed.
- the affected suites (`test_cli_worker/job/job_ops/actor_config/actor_
  deregister/queues/migrate/doctor/health/ui/workgroup/ref_resolution_
  errors/credential_provider`, `test_examples_{wiring,smoke}`,
  the pins): 284 passed, 0 failed.
- the cured batch example's E2E: `batch-complete` receipt (3/0/3),
  finalizer unstamped, worker exits rc=0.
- the estate's head-stamp law: the verifier rc=0 at the final head;
  rv2-13's pin greens on the committed tree.
- the fast tier at -n 8 (with the docs tier): 13429 passed / 7 skipped /
  1 xfailed / 0 failed, rc=0.

Incidental (out of lane, NOT chased): an until-idle worker that never
observes a settled drain can outlive its own clean shutdown by ~30 s
(shutdown-phase EXITED at T, process killed at T+30 — an exit-tail thread
join, otel-flush-shaped). Left to a future lane.
