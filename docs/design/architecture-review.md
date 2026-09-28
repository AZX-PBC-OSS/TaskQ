# The architecture audit: structure verdict, touch counts, consolidations

The architecture review of the wave's output (main at `e50e4263`): is the
codebase clean, DRY, extensible, maintainable at the STRUCTURE level — what
did the wave's velocity paper over? Evidence per finding; every fix in this
pass is behavior-preserving with its test family green three consecutive
runs (`feat/audit-architecture`).

## 1. The dependency graph

Built from the module-level imports across `src/taskq`'s nine packages.
The intended layering:

```
constants / _ids / _json / _shield / exceptions   (leaf infra)
        ├── obs                (metrics + logging; imports leaf only)
        ├── backend            (protocol, statemachine, SQL carriers)
        │      ▲        ▲
        │      │        └────────── client, ratelimit, progress
        ├── worker             (composition: consumes backend/obs/ratelimit/progress)
        └── web                (composition: admin UI + health over backend/worker health)
cli                              (composition root: imports everything)
testing                          (in-memory Backend + fixtures)
```

**Verdict: the layering is real and mostly held.** No import cycles among
the packages (verified by walking every module-level `from taskq.*`
import): `obs` imports leaf infra only — the observability layer imports
NOTHING from its consumers. `backend` never imports `worker`, `web`, or
`client`. `ratelimit` stays inside itself plus `backend._protocol`. The
repo has no import-linter config; its equivalent is `tests/_import_discipline.py`,
an AST-based invariant checker (module-level-import assertions, e.g. "the
admin package must not couple to `taskq.worker` at import time") consumed by
`tests/test_web_health.py` and the DI-scope tests — enforced where a runtime
check cannot express the invariant.

### The two violations found

1. **`progress/_flush.py` imports `taskq.worker._transient.UnexpectedLoopErrorGuard`
   and `taskq.worker._watchdog.LoopLiveness` at module level** — a low-level
   package (progress pub/sub) reaching UP into the worker's PRIVATE modules.
   Not a cycle (neither watchdog module imports progress), but an inversion:
   importing `taskq.progress._flush` drags worker subsystems into
   `sys.modules`. Both primitives are consumer-agnostic loop-hygiene
   machinery that history parked in `worker/`. Fix shape (recommended, not
   executed — it moves two classes whose consumers are these EIGHT modules,
   seven worker + one progress, each verified by grep against the current
   tree): promote `LoopLiveness` and `UnexpectedLoopErrorGuard` to a neutral
   `taskq/_loophealth.py`; the consumers to re-point (or re-export) are
   `worker/deps.py` (LoopLiveness field type + default),
   `worker/health.py` (LoopLiveness probe), `worker/notify.py`
   (LoopLiveness tick), `worker/heartbeat.py` (UnexpectedLoopErrorGuard),
   `worker/leader.py` (guard, 8 uses), `worker/_leader_sweeps.py` (guard,
   6 uses), `worker/run.py` (guard), and the inverted `progress/_flush.py`
   itself (both classes); `worker/_transient.py`/`worker/_watchdog.py`
   keep the definitions and re-export from the new home for one release.
2. **`web/health.py` imports `taskq.worker.health` at module level.**
   Tolerable, not accidental: web is a HIGHER layer than worker, and the
   imported names (`compute_health`, `build_ready_body`, `_check_live`) are
   the worker-health domain itself, with `WorkerDeps` typed only under
   `TYPE_CHECKING`. The admin package's own import-time prohibition on
   `taskq.worker` (the AST discipline) still holds — `web/admin/ops.py`'s
   reach into `worker.cron_loop` is function-level (lazy), the shape the
   discipline sanctions for a genuinely optional dependency.

### The god modules

| module | lines | defs | verdict |
|---|---|---|---|
| `obs/_otel.py` | 3,032 | 116 | **Real seams; split recommended, not executed.** Five named sections (worker capacity, cron slots-behind, maintenance-sweep health, backlog detection, per-actor attribution) around the instrument definitions. All consumers import from `taskq.obs`, so a split into `obs/_otel_*` sections re-exports transparently. Not executed here: module-level instrument creation is init-order sensitive, and the mechanical value is below the risk in this pass. |
| `worker/_bootstrap.py` | 2,972 | — | Composition root by role (the worker's whole startup story); its length is the accumulation of startup steps, not tangled concerns. |
| `backend/_dispatch_sql.py` | 1,764 | 1 top-level def | **Not a god module.** One rendered SQL template (data, not logic): no seam to split — splitting it would be line-count theater. |
| `backend/_sweeps.py` | 2,392 | 8 | One concern (the sweep statements' SQL + batching carriers); seams are real and fine. |
| `cli.py` | 3,518 | many | The largest true god module: doctor, queues, jobs, schedules, actor-config, workgroup. Each subcommand family is a candidate extraction (`cli/` package); recorded as the next structural debt, not executed. |

## 2. The extensibility test

How many files must you touch to add one unit of extension?

### (a) A new backend (non-PG) — **2 composition roots + 1 package; no registry**

The seam is real and PROVEN in-tree: `testing/in_memory.py`'s
`InMemoryBackend` implements the same `Backend` protocol (2,656-line
`backend/_protocol.py`, `BACKEND_PROTOCOL_VERSION` pinned by contract
tests). But construction is hard-coded in exactly two composition roots:
`worker/_bootstrap.py` (the worker's `PostgresBackend(...)`) and
`client/_taskq.py` (the client's). Touch count: **3** (new module +
2 edits). A DI-registered backend factory would drop it to 1, but the DI
engine currently resolves `Clock`, not the backend; registering a
backend-constructing factory is an API change, not a behavior-preserving
refactor, so recorded as the design recommendation. The maintenance sweeps
already degrade correctly on a non-PG backend (the `hasattr` gates — now
explicit per-sweep in the spec table, see below).

### (b) A new admin page — **2 files (module + template), zero framework edits**

`web/admin/_factory.py::_discover_and_register` pkgutil-scans sibling
submodules and calls their `register()` — "pages add a `register()`
function to their own submodule, they never edit this file." Verified: ten
pages, none named in the factory. The one hidden coupling: the nav bar in
`templates/_base.html` hand-lists the page links — a new page is discoverable
but invisible until the nav is edited (touch count **3** with the template,
**2** without nav). Deriving the nav from the router's routes would be
behavior-preserving only if the derived order matched the curated order;
recorded as the candidate fix, not executed.

### (c) A new leader sweep — **was: copy a ~70-line block with 5 name-sites; now: append one spec row** (FIXED this pass)

`worker/_leader_sweeps.py::_sweep_loop` had EIGHT hand-unrolled
copy-paste blocks — expired_locks, deadline_exceeded, leaked_slots,
expired_results, job_events_retention, keyed_row_reclaim, stale_workers,
stale_batches — each repeating the same try/except(NotImplementedError?/
transient-set + extras)/finally(metrics)/bounded-drain skeleton with the
sweep's name stamped in FIVE places (metric duration, metric rows, success
record, warn event, warn kind), plus a debug event, gates, and drain
discipline. The drift the copies invited was real: the stale-batches
block's hand-rolled transient tuple predated the shared
`TRANSIENT_PG_ERRORS` set and missed `QueryCanceledError` (documented in
its own comment as a past production bug).

**The fix (behavior-preserving):** `_SweepSpec` — a frozen dataclass whose
fields are exactly what differed between the blocks (name, call, warn
event/kind, `hasattr` gates, period gate, extra tolerance exceptions,
warn-once unimplemented arm, drain discipline, debug hook, on-rows log) —
and ONE runner (`run_sweep`) implementing the skeleton and the sample
discipline (duration always; rows and the success stamp only when the
awaited call returned). Tick order, gates, log events, metric names, and
per-sweep tolerances are carried verbatim; the keyed-eviction blocks stay
deliberately OUTSIDE the leader gate and the backstop guard, exactly as
before. Adding a sweep is now: a `call` closure + one `_SweepSpec` entry
(name stamped once), plus the backend method. Touch count inside
`_leader_sweeps.py`: from ~70 lines / 5 name-sites to ~10 lines / 1 name-site.

Proof: the sweep family (22 files: the wiring, coverage, bounded, parity,
breaker, ladder, drain-domain, timeout-metrics, retention, and validation
suites) green 3× — 368 passed per run — plus ruff/pyright clean. The
verbatim claim is additionally executed, not just asserted:
`tests/test_audit_sweep_registry_differential.py` runs the VENDORED
pre-refactor module (extracted verbatim from `origin/main` by AST) and the
spec-driven runner through the same scripted fault matrix — per sweep:
deadline-family and plain-transient failures, the pre-migration
`UndefinedColumnError`/`UndefinedTableError` tolerances (on the sweep that
has them AND on the siblings that must propagate them), the
`NotImplementedError` warn-once arms, the `timedelta(0)` period gates, the
`hasattr` backend gates, and the drain discipline — and asserts the two
event streams (call order, `record_sweep_*`, metric emissions, warn/debug/
err events, drains) are EQUAL; the harness's sensitivity to a real
tolerance mutation is itself pinned.

### (d) A new metric — **1 file (+ call sites); nothing else**

`obs/_otel.py` defines the instrument and its `record_*` function; the
call site imports it. Prometheus needs NO per-metric edit —
`contrib/prometheus` wires a `PrometheusMetricReader` bridge over the OTel
provider, so every instrument reaches the scrape automatically (verified:
no metric-name list exists in contrib). Verdict: good extensibility, no
fix needed.

## 3. The DRY render layer

The premise named three operator surfaces rendering verdicts/markers/
thresholds. The audit's findings, against the actual code:

- **Already single-sourced (the wave's earlier DRY pass got these):**
  `INSIGHTS_WINDOWS` (the closed window set) and all six `fetch_*`
  statements live once in `taskq/insights.py`; the dashboard's
  `/insights` route imports them and writes no SQL of its own.
  `TERMINAL_STATUSES` for the SQL UNION arms imports the state machine's
  set, not a copy. The insights surface has a pinned
  `INSIGHTS_CONTRACT_VERSION` with a contract test.
- **No `taskq insights` CLI command exists** — the mission's "CLI
  insights" surface is the doctor command (`taskq doctor`), whose finding
  strings (NEVER DISPATCHES / STALE / INCOHERENT) live only in `cli.py`.
  A future CLI insights command would be the third consumer of
  `taskq.insights`; nothing blocks it.
- **Fixed here — the human-duration cascade, three hand-maintained
  copies:** `cli.py::_format_age`, `web/admin/insights.py::`
  `_humanize_seconds`, and `timescale.py::_format_interval` each carried
  their own `divmod` cascade over 86 400/3 600/60. New
  `taskq/_humantime.py` owns the split (`split_seconds`) and the two
  compact formatters (`humanize_wait` — the dashboard's rounding,
  `<1s`-floored, minutes-and-up contract; `humanize_age` — the CLI's
  truncating, no-space column contract); `_format_interval` composes the
  shared split with its whole-unit plural wording. One formatter per
  surface, NOT one mode-flag function on purpose: each surface's output
  is a pinned presentation contract, and tests now pin the exact strings
  (`tests/test_humantime.py`) so a change to one surface cannot silently
  move another's.
- **Fixed here — the admin pages' status sets:** `web/admin/_constants.py`
  hand-copied the terminal/active status string sets; the code's own
  comments (in `insights.py`) had already named that copy as "a second
  hand-maintained copy" to avoid. The sets now derive from
  `taskq.backend.statemachine.TERMINAL_STATUSES`/`ACTIVE_STATUSES`
  (identity-pinned by `test_admin_status_sets_derive_from_the_state_machine`),
  so a status added to the machine follows into every admin filter
  without a second edit.
- **Deliberately NOT merged:** `STATS_WINDOWS` (actors page: 1h/24h/7d/
  30d + all-time default) vs `INSIGHTS_WINDOWS` (insights page: 1h/6h/
  24h/7d, all-time deliberately absent). Two pages' closed query-param
  sets are two contracts; unifying them would change what each URL
  accepts — behavior, not duplication.

## 4. The naming/coherence census

- **Job statuses**: `JobStatus` (a Literal) is the single source; raw
  strings appear only where SQL requires literals, validated against the
  closed set at parse boundaries. After this pass the admin package's
  sets derive, not copy.
- **`schema` vs `schema_name`**: two names, two ROLES — `settings.schema_name`
  is the configured value, `schema=` is the bind-style kwarg every
  statement takes. The split is uniform across backend/client/cli; not an
  alias bug.
- **`worker_id`**: UUID in `SweepContext`, stringified once at each log
  emit (`worker_id=str(ctx.worker_id)`) — consistent. No camelCase
  aliases (`workerId`) anywhere in `src/`.
- **Window-selector sets**: `STATS_WINDOWS`/`INSIGHTS_WINDOWS` — same
  CONCEPT, different closed sets per page (above); names already say so.
- The remaining alias worth watching: `ConnLike` (backend protocol) vs
  `asyncpg.Connection` at direct-use sites — the protocol alias is the
  intended seam and is used consistently.

## 5. The verdict

**Clean:** yes, with two named violations (progress→worker-privates being
the real one) and no cycles. **DRY:** the render layer's known gap was
real but narrower than feared — the window set and SQL were already
single-sourced; the human-duration cascade and the admin status sets were
not, and are now. **Extensible:** admin pages (discovery) and metrics
(bridge) are genuinely add-in-one-place; backends are protocol-clean with
hard-coded construction; leader sweeps WERE the worst edits-in-N-places
coupling and are now a table. **Maintainable:** the god-module audit
clears the SQL carriers as data, flags `cli.py` and `obs/_otel.py` as the
next structural debt, with the split shapes recorded above.

## Gates

- `ruff check` / `ruff format --check`: clean across `src` + `tests`.
- `pyright`: 0 errors, 0 warnings (whole repo).
- Families, 3 consecutive runs each, all green:
  - the sweep family (22 files): **368 passed × 3**
  - the admin family (`tests/web_admin/` + security-fixes): **826 passed × 3**
  - the CLI/import-discipline family: **79 passed × 3**
- The full `-m "not integration"` tier passes serially and in isolated
  re-runs; under `-n 4` in this sandbox the container-backed tests flake
  on Docker contention (failure sets vary run to run and every failure
  passes in isolation) — environmental, not code.
