# The architecture notes: structure, extensibility, duplication

The structural facts a maintainer needs: the package layering and the
import discipline that holds it, what each extension point costs to
touch, and where duplication is deliberate.

## 1. The dependency graph

Built from the module-level imports across `src/taskq`'s nine packages.
The layering:

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

No import cycles among the packages: `obs` imports leaf infra only -
the observability layer imports nothing from its consumers. `backend`
never imports `worker`, `web`, or `client`. `ratelimit` stays inside
itself plus `backend._protocol`.

The repo has no import-linter config; its equivalent is
`tests/_import_discipline.py`, an AST-based invariant checker
(module-level-import assertions, e.g. "the admin package must not
couple to `taskq.worker` at import time") consumed by
`tests/test_web_health.py` and the DI-scope tests - enforced where a
runtime check cannot express the invariant.

Two known, deliberate inversions:

1. `progress/_flush.py` imports
   `taskq.worker._transient.UnexpectedLoopErrorGuard` and
   `taskq.worker._watchdog.LoopLiveness` at module level - a low-level
   package reaching up into the worker's private modules. Not a cycle
   (neither watchdog module imports progress), but an inversion:
   importing `taskq.progress._flush` drags worker subsystems into
   `sys.modules`. Both primitives are consumer-agnostic loop-hygiene
   machinery; promoting them to a neutral `taskq/_loophealth.py` is the
   recorded fix shape (consumers: `worker/deps.py`, `worker/health.py`,
   `worker/notify.py`, `worker/heartbeat.py`, `worker/leader.py`,
   `worker/_leader_sweeps.py`, `worker/run.py`, and `progress/_flush.py`
   itself).
2. `web/health.py` imports `taskq.worker.health` at module level.
   Tolerable, not accidental: web is a higher layer than worker, and the
   imported names (`compute_health`, `build_ready_body`, `_check_live`) are
   the worker-health domain itself, with `WorkerDeps` typed only under
   `TYPE_CHECKING`. The admin package's own import-time prohibition on
   `taskq.worker` still holds - `web/admin/ops.py`'s reach into
   `worker.cron_loop` is function-level (lazy), the shape the discipline
   sanctions for a genuinely optional dependency.

### Module sizes

| module | lines | shape |
|---|---|---|
| `obs/_otel.py` | 3,032 | Five named sections (worker capacity, cron slots-behind, maintenance-sweep health, backlog detection, per-actor attribution) around the instrument definitions. All consumers import from `taskq.obs`, so a split into `obs/_otel_*` sections re-exports transparently; module-level instrument creation is init-order sensitive. |
| `worker/_bootstrap.py` | 2,972 | Composition root by role (the worker's whole startup sequence); its length is the accumulation of startup steps, not tangled concerns. |
| `backend/_dispatch_sql.py` | 1,764 | One rendered SQL template (data, not logic): no seam to split. |
| `backend/_sweeps.py` | 2,392 | One concern (the sweep statements' SQL + batching carriers). |
| `cli.py` | 3,518 | The largest true god module: doctor, queues, jobs, schedules, actor-config, workgroup. Each subcommand family is a candidate extraction (`cli/` package) - the recorded next structural debt. |

## 2. The extensibility test

How many files must you touch to add one unit of extension?

### (a) A new backend (non-PG) - 2 composition roots + 1 package; no registry

The seam is real and proven in-tree: `testing/in_memory.py`'s
`InMemoryBackend` implements the same `Backend` protocol
(`BACKEND_PROTOCOL_VERSION` pinned by contract tests). Construction is
hard-coded in exactly two composition roots: `worker/_bootstrap.py`
(the worker's `PostgresBackend(...)`) and `client/_taskq.py` (the
client's). Touch count: **3** (new module + 2 edits). A DI-registered
backend factory would drop it to 1, but the DI engine currently
resolves `Clock`, not the backend - an API change, not a
behavior-preserving refactor. The maintenance sweeps degrade correctly
on a non-PG backend (the `hasattr` gates).

### (b) A new admin page - 2 files (module + template), zero framework edits

`web/admin/_factory.py::_discover_and_register` pkgutil-scans sibling
submodules and calls their `register()` - pages add a `register()`
function to their own submodule, they never edit the factory. The one
hidden coupling: the nav bar in `templates/_base.html` hand-lists the
page links - a new page is discoverable but invisible until the nav is
edited (touch count **3** with the template, **2** without nav).
Deriving the nav from the router's routes would be
behavior-preserving only if the derived order matched the curated
order.

### (c) A new leader sweep - one spec row

`worker/_leader_sweeps.py` carries `_SweepSpec`, a frozen dataclass
whose fields are what differed between the sweeps (name, call, warn
event/kind, `hasattr` gates, period gate, extra tolerance exceptions,
warn-once unimplemented arm, drain discipline, debug hook, on-rows
log) - and ONE runner (`run_sweep`) implementing the skeleton and the
sample discipline (duration always; rows and the success stamp only
when the awaited call returned). Adding a sweep is: a `call` closure +
one `_SweepSpec` entry (name stamped once), plus the backend method.
`tests/test_audit_sweep_registry_differential.py` runs a vendored
pre-refactor module and the spec-driven runner through the same
scripted fault matrix and asserts the two event streams are equal.
The keyed-eviction blocks stay deliberately outside the leader gate
and the backstop guard.

### (d) A new metric - 1 file (+ call sites); nothing else

`obs/_otel.py` defines the instrument and its `record_*` function; the
call site imports it. Prometheus needs no per-metric edit -
`contrib/prometheus` wires a `PrometheusMetricReader` bridge over the
OTel provider, so every instrument reaches the scrape automatically
(no metric-name list exists in contrib).

## 3. The render layer's single sources

- Single-sourced: `INSIGHTS_WINDOWS` and all six `fetch_*` statements
  live once in `taskq/insights.py`; the dashboard's `/insights` route
  imports them and writes no SQL of its own. `TERMINAL_STATUSES` for
  the SQL UNION arms imports the state machine's set, not a copy. The
  insights surface has a pinned `INSIGHTS_CONTRACT_VERSION` with a
  contract test.
- No `taskq insights` CLI command exists - the CLI's surface is the
  doctor command (`taskq doctor`), whose finding strings live only in
  `cli.py`. A future CLI insights command would be the third consumer
  of `taskq.insights`; nothing blocks it.
- The human-duration cascade is single-sourced in `taskq/_humantime.py`
  (`split_seconds`, plus `humanize_wait` - the dashboard's rounding,
  `<1s`-floored, minutes-and-up contract; `humanize_age` - the CLI's
  truncating, no-space column contract); `timescale.py` composes the
  shared split with its whole-unit plural wording. One formatter per
  surface, NOT one mode-flag function on purpose: each surface's output
  is a pinned presentation contract (`tests/test_humantime.py` pins the
  exact strings), so a change to one surface cannot silently move
  another's.
- The admin pages' status sets derive from
  `taskq.backend.statemachine.TERMINAL_STATUSES`/`ACTIVE_STATUSES`
  (identity-pinned by
  `test_admin_status_sets_derive_from_the_state_machine`), so a status
  added to the machine follows into every admin filter without a
  second edit.
- Deliberately NOT merged: `STATS_WINDOWS` (actors page: 1h/24h/7d/
  30d + all-time default) vs `INSIGHTS_WINDOWS` (insights page: 1h/6h/
  24h/7d, all-time deliberately absent). Two pages' closed query-param
  sets are two contracts; unifying them would change what each URL
  accepts - behavior, not duplication.

## 4. The naming conventions

- **Job statuses**: `JobStatus` (a Literal) is the single source; raw
  strings appear only where SQL requires literals, validated against the
  closed set at parse boundaries.
- **`schema` vs `schema_name`**: two names, two roles -
  `settings.schema_name` is the configured value, `schema=` is the
  bind-style kwarg every statement takes. The split is uniform across
  backend/client/cli.
- **`worker_id`**: UUID in `SweepContext`, stringified once at each log
  emit (`worker_id=str(ctx.worker_id)`). No camelCase aliases
  (`workerId`) anywhere in `src/`.
- **Window-selector sets**: `STATS_WINDOWS`/`INSIGHTS_WINDOWS` - same
  concept, different closed sets per page; the names say so.
- `ConnLike` (backend protocol) vs `asyncpg.Connection` at direct-use
  sites - the protocol alias is the intended seam and is used
  consistently.
