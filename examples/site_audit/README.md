# The site-audit pipeline — fan out, branch by type, fan back in

A fleet audit, wired as ONE workflow: a plan lists N sites, each site's
audit fans out as its own job, branches on the site's TYPE (sensor
stations and solar panels take different arms), each arm's dispatch
cadence is capped by its own rate-limit bucket, and the typed results
fan back in to one aggregate report. A site whose audit fails its retry
ladder lands in the join's TYPED failure record — the run succeeds with
the failure named, never a silent swallow. A site flagged by its
calibration reading gets a sub-investigation: a child flow spawned from
inside the audit arm that joins its verdict back into the site's result.

The machinery, by face:

| face | where | the verb / surface |
| --- | --- | --- |
| the fan-out (N entities) | `pipeline.py` `build_app` | `route(planned, arms)` — one child per element, forked at the source's finalize |
| the typed branch (per-type work) | `pipeline.py` `audit_sensor` / `audit_panel` | the arms are keyed by the item union's MEMBERS; each arm body declares its own member |
| the per-branch rate limits | `pipeline.py` `SENSOR_BUCKET` / `PANEL_BUCKET` | `RouteArm(..., rate_limits=(bucket,))` — Postgres-backed buckets |
| the fan-in (typed aggregate) | `pipeline.py` `write_report` | the route's derived join fires once; the survivors' typed sum decodes at the consumer |
| the typed failure arm | `drills/partial_failure.py` | `route(..., on_failure="collect")` — the failure fans in as the typed record, the survivors' join still fires |
| the child flow (join back) | `pipeline.py` `audit_sensor` | `FlowRunner.create_flow(..., run_key=...)` inside the body — idempotent via the run key |
| the progress push | `drills/scale.py` | `ctx.progress(...)` in the arms; `ProgressListener` on the run |
| the operator's view | `drills/scale.py` | `taskq flows list` / `taskq flows status` — mid-flight and at terminal |

## Run the happy path

```bash
export TASKQ_PG_DSN=postgresql://postgres:taskq@localhost:5432/taskq
export TASKQ_SCHEMA_NAME=taskq
uv run python -m examples.site_audit.run_audit
```

The last line is the typed success receipt:

```
AUDIT COMPLETE  planned=9  sensor=5  panel=4  investigations=1  failed=[]  wall=…
```

## The drills (each face, proved on a live database)

Every drill provisions its OWN schema (`<TASKQ_SCHEMA_NAME>_probe_<slug>`)
— fresh migrations, no cross-drill state — and prints its receipt plus
the assertion verdict. The rate-limit drill additionally spawns a worker
process (the buckets are honored at the fleet's dispatch claim, which the
in-process driver does not traverse), and the resume drill sets
`TASKQ_AUDIT_DELAY_S=1.5` (the arms' per-site latency — the knob that
holds the fan-out open while the kill window lands).

```bash
export TASKQ_AUDIT_DELAY_S=1.5   # the resume drill needs mid-flight work to kill
uv run python -m examples.site_audit.drills.partial_failure  # one entity fails; the fleet completes; the failure lands typed
uv run python -m examples.site_audit.drills.rate_limit       # the observed dispatch cadence respects the declared buckets
uv run python -m examples.site_audit.drills.scale            # N=200: fan-out, fan-in, the progress push, the admin view
uv run python -m examples.site_audit.drills.resume           # SIGKILL mid-fan-out; restart; the completed never re-run
uv run python -m examples.site_audit.drills.type_route       # each type's arm ran and ONLY its arm
```

## The shape, in one paragraph

`plan_audit` returns the plan (a list of the item union). `route` forks
one child per element and dispatches each element by WHAT IT IS: the
union member's type keys the arm, a non-total route is a build refusal,
and an element with no arm dies loudly — routing nothing is never a
success. Each arm's body sees its element AS its declared model. The
arms' `rate_limits` stamp each child row with its branch's buckets; the
claim path honors them (a denial snoozes the row budget-free and bumps
the denial counter). The derived join fires EXACTLY ONCE when the last
child terminalizes, carrying the survivors' typed sum — with
`on_failure="collect"`, a child that exhausted its ladder fans in as a
typed failure record on the join row instead, and the flow still
terminalizes. `write_report` decodes the sum, derives the failed
residual from the plan (the envelope is the truth, never a hardcoded
`[]`), and the run's result read returns the DECODED report.
