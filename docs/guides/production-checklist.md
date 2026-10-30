# The production checklist

One page for the deploy day: the landmines that only fire under a fleet,
the fences you must respect at wiring, the tuning contract the batteries
measured, and the migration law — each rule cross-linked to the page that
teaches it. Nothing here is new law; this page COLLECTS.

## Connections: the DSN split and the PgBouncer wall

- [ ] **`TASKQ_PG_DSN` points at Postgres DIRECTLY** — never at a
      transaction-mode PgBouncer. Session-scoped machinery
      (`LISTEN/NOTIFY`, `pg_try_advisory_lock`) silently breaks under
      transaction pooling: the leader's advisory lock drops when the
      session is released, the listeners go deaf. The worker carries
      **up to four pools plus dedicated connections** — the dispatcher,
      heartbeat and per-slot pools are DIRECT by contract, only the
      worker_pool may traverse PgBouncer. Split DSNs per side:
      `TASKQ_PG_DSN` (direct) + `TASKQ_PG_DSN_POOLED` (pooled); both
      fall back to the direct one if unset.
      → [deployment.md — the production checklist](deployment.md#production-checklist), [workers.md — PgBouncer compatibility](workers.md#pgbouncer-compatibility)
- [ ] **Size `max_connections` for the POOL ARITHMETIC, not the pod
      count.** Idle floor: 5 sessions per worker (7 as leader); the
      per-slot path adds `max_concurrency + 1` direct connections per
      worker and a credential-rotation window peaks at
      `2 × (max_concurrency + 1)`. Ten workers at
      `max_concurrency=16`: ~340 connections steady, ~510 on the
      per-slot path. Your application's own pools ride on top.
      → [ops.md — Sizing](ops.md#4-sizing-workers-and-postgres-connections), [deployment.md — the two DSNs](deployment.md#worker-deployment)
- [ ] **Fork-safety landmine (the listeners):** a LISTEN connection is
      session-scoped and travels badly — one pool slot for the stream's
      life, never a direct connection handed across a process fork, and
      the run-scoped backfill bounded. The SSE/progress listeners are
      built to this shape; an app-side listener must be too.
      → [progress.md — the push transport's landmines](progress.md#the-push-transport-the-sse-streams-primary-leg), [agent-fleet.md — the approval listener](agent-fleet.md)

## The capability fences: what the boot refuses, what the fleet refuses

- [ ] **One actor, one queue.** `actor_config` is keyed by actor; a
      cohort split across two queues is refused at boot
      (`workflow-projection-skipped`). The wiring REPORTS the derived
      join's conflict at validate time (the W6 warning names the actor,
      both queues, and the distinct-actor remedy) — fix it at the wiring
      (`RouteArm(actor=..., queue=...)`), never at 3am.
      → [api-reference/workflows.md — W6](../api-reference/workflows.md), [workflows.md — the placement law](workflows.md)
- [ ] **The boot projection covers the WHOLE cohort surface** — the
      wired nodes, the map attachments, the chains, and the typed
      route's arms' effective placements. If a queue your bodies stamp
      receives no `actor_config` row, no worker listens: the rows
      strand (the strict-queue worker never claims them). A boot that
      strands silently is a defect — report it, don't provision around
      it.
      → [docshelf.md — F2](docshelf.md)
- [ ] **`TASKQ_QUEUES_STRICT` fail-fast** — a worker projected onto a
      queue its app cannot see refuses to boot loudly. Keep it on in
      production — declare it in code on the app
      (`WorkflowApp(queues_strict=True)`) or via the env; the faces
      OR-combine and the env can only add strictness, never silence a
      declared contract.
      → [workers.md](workers.md), [configuration.md](configuration.md)
- [ ] **Versioning contract (the stamp read):** a workflow definition
      carries a content stamp; a redeploy that renames or removes a
      node strands in-flight runs at that node (they zombie) —
      `flows status --app` compares the run's node inventory against
      the CURRENT registry and NAMES the stranded nodes. Read the stamp
      before you deploy; never rename a live node's key.
      → [workflows.md §6 — the versioning contract](workflows.md), [upgrading.md](upgrading.md)

## The tuning contract (the batteries' measured numbers)

- [ ] **Postgres** (the load batteries' own profile):
      `max_connections=1000` (the fleet arithmetic above),
      `fsync=off` + `synchronous_commit=off` + `full_page_writes=off`
      are load-test-only settings — in production leave WAL durability
      ON and instead size `max_wal_size` (the batteries' 4GB keeps the
      checkpoint cadence off the latency bands at the measured fan-out)
      and `checkpoint_timeout` (3600s in the batteries). The contract:
      the durably-queued job must survive a crash — the queue's
      at-least-once delivery is only as durable as the WAL.
      → [scaling.md](scaling.md), [ops.md — Sizing](ops.md#4-sizing-workers-and-postgres-connections)
- [ ] **The lease and the reclaims:** a lapsed lease is reclaimed at
      ~90 s worst case; the reclaim runs the body again (at-least-once —
      the terminal write's CAS fences the lapped attempt's terminal).
      Tune `TASKQ_LEASE_SECONDS`/heartbeat above your bodies' worst
      real runtime, not above your hopes.
      → [ops.md — the stuck-job row](ops.md), [workers.md — heartbeat and liveness](workers.md#heartbeat-and-liveness)
- [ ] **Workflow fan-in bound: 1000 parents per join.** A map past it
      partitions by document (the honest partition) or takes the
      `child_driven=True` escape (the join counts its terminal children
      from the edge ledger). A chunk-level fan of ~2300 into one join is
      refused at build — `FanInBoundExceededError` is the wall's name.
      → [workflows.md — the fan-in bound](workflows.md), [docshelf.md — F6](docshelf.md)
- [ ] **The sweep cadence:** the leader's sweeps (the signal expiry, the
      timer due-drain, the join re-derive) run on one cadence — the
      timer's due-drain DRAINS until empty per tick (batch-bounded per
      iteration), so a 10K-timer backlog costs one tick, not
      `N × 30s`. Keep the cadence at its default unless the bands move.
      → [workflows.md §4b — the timers](workflows.md), [maintenance-sweeps.md](maintenance-sweeps.md)

## The migration law

- [ ] **Forward-only, ledger-identified.** Every migration carries a
      ledger identity `(phase, number)`; the loader REFUSES a duplicate
      identity loudly (a two-lane merge shipping the same number fails
      closed at apply — renumber the later landing, the reads decide by
      the file's committed timestamp). There is no down migration: to
      revert, restore from backup.
      → [deployment.md — migrations under a running fleet](deployment.md#migrations-under-a-running-fleet-what-rides-what-must-wait)
- [ ] **`apply_pending_locked` before the workers claim** — the
      migrate → workers → clients discipline; the runner's 30s
      `ddl_lock_timeout` gives up cleanly when a queued `ACCESS
      EXCLUSIVE` parks the fleet's queries (find the holder, re-run).
      DDL against live tables mid-deploy surfaces as a brief
      `InvalidCachedStatementError` — transient by design, the pool
      re-prepares next tick.
      → [deployment.md](deployment.md#migrations-under-a-running-fleet-what-rides-what-must-wait), [ops.md](ops.md)
- [ ] **The workflow migrations are ADDITIVE** — no `post_` phase ships
      for v1; a workflow DDL file declares its lock class in its
      header (the obligations the loader verifies).
      → [workflows.md — the schema's own law](workflows.md)

## The durability story (what you can promise your users)

- The queue delivers **at-least-once**; the terminal write's CAS and
  the ledger's arbiters make the EFFECTS exactly-once per attempt
  budget — the idempotency discipline is the author's (a stable
  business key per fan-out chunk).
  → [jobs-clients.md — idempotency](jobs-clients.md), [ops.md §5](ops.md)
- A workflow run's state is the ROWS — a crash anywhere (worker SIGKILL,
  deploy, failover) resumes from the ledger: succeeded substeps REPLAY
  free, held waits re-park, the cursor never rewinds (GREATEST).
  → [workflows.md — the resume law](workflows.md), [docshelf.md — the crash drills](docshelf.md)
- A hold is a row: the worker's slot is RELEASED while the body waits on
  a human or a deadline — one worker can carry thousands of parked
  flows. The hold identity is
  `(workflow, node_key, signal, COALESCE(map_index, -1), epoch)` —
  per-item holds in a map are distinct rows.
  → [workflows.md §4 — holds and timers](workflows.md), [docshelf.md — F5](docshelf.md)
