# Multi-tenant workflow demo

Two tenants' runs of ONE workflow graph, ONE keyed rate limit
(`tenant-api`, one token per tenant, refilling at 0.5/s), ONE
`round_robin` queue — driven through the REAL fleet door (the
production dispatch claim, the workflow intercept's rate-limit gate,
the fleet execution door).

## What it shows

1. **Per-tenant keyed buckets on the child rows.** The keyed ref is
   declared on the `map_source`; each item child's row carries its own
   concrete `tenant-api:<tenant>` bucket in its admission-terms
   metadata (resolved from the item's payload at the fork).
2. **The per-key acquire/deny trace.** A tenant's saturated bucket
   snoozes that tenant's item budget-free while the other tenant's
   items claim through their own bucket — one tenant's saturation never
   gates the other. The round_robin queue interleaves the two runs'
   fairness cohorts (each run is born with its own `flow_id` cohort).
   **THE TRACE ORDER IS CLAIM ORDER**: each round prints its claimed
   set, the rotation's interleave is what the round headers show, and
   each DENY line carries both the denial hint and the APPLIED deferral
   (read back off the row — the backend's own deferral floor applies).
3. **Both tenants' completion.** Both runs terminalize `succeeded`,
   the results read back through the typed door.

## Prerequisites

A reachable Postgres 18 (Redis is not required — the demo's keyed
buckets use the memory backend):

```sh
docker run -d --name taskq-demo-pg -e POSTGRES_USER=taskq \
  -e POSTGRES_PASSWORD=taskq -e POSTGRES_DB=taskq -p 5432:5432 pg18-or-later
```

## Run

```sh
TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
TASKQ_ENVIRONMENT=dev \
    uv run python -m examples.tenant_fairness_demo.run_demo
```

The schema (`tenant_demo` by default, override with `TASKQ_SCHEMA_NAME`)
is left in place for inspection.
