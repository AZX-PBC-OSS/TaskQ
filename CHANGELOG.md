# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased — TaskQflow phase 4]

### Added

* **Admin (T11): the workflow-run explorer** — the runs list + the run
  page (`/taskq/workflows/{id}`): the rows-alone Mermaid graph (the
  collapsed map = one hexagon + the done/total counter), the node drill
  panel (the attempts, the trace id, the captured error, one upstream
  hop, the attempt ledger), the holds & decisions panel (the waiting-on
  state + the declared payload schema + the prefilled example), the
  audit trail (every operator action is a ROW), and the SSE
  revisioned-snapshot feed (the seq-cursor + `Last-Event-ID` replay —
  the reconnect never loses state). The page sets `suppress_refresh`
  (the meta refresh is the killer); the no-JS surface is the
  server-rendered snapshot, stated on the page. The nodes are keyboard
  operable (tab + Enter). The vendored `mermaid.min.js` v11.12.2
  (2,754,895 B / 774 KB gzipped) joins alpine/htmx/lucide in
  `/static` — no bundler, no React, no CDN.
* **The typed actions** — the admin's `resolve`/`deliver` validate
  through the MOUNTED WorkflowApp's bound gates (`create_router(
  workflow_app=...)`); without definitions the doors answer 501 — no
  untyped deliver surface ships.
* **CLI (T12): `taskq flows`** — list/status/holds/signal/resolve/
  cancel/retry: one question, one command; the analysis lives in the
  pure module (`workflows/_cli.py`, the `_doctor` seam — the shared
  §17.5 derivation + the why-stuck arm); the typed door validates the
  shell's JSON against the bound gates (the named pydantic refusal; the
  hold survives); the write verbs ride the engine's audit rows with the
  `cli:<user>` principal; the read verbs are read-only (pinned).
* **Examples (T13): the doc-ingest pipeline** — the nine shapes
  (heterogeneous fork, batch collect with the partial failures, the
  typed HITL hold inside the budget-capped loop, the worked duality of
  the edge policies, the union routing with `assert_never`, the typed
  terminal verdict, the cron-slot run key) in ONE flow, shipped
  abstract, executed in CI (the fast-tier smoke + the Mermaid golden +
  the examples lane: the join fires exactly once across 3 rounds; the
  cron slot's idempotency live). The grep gate: zero case-study strings
  in the shipped tree.
* **Demo (T16): the doc_ingest graph LIVE** — `examples/workflows.py` +
  the app's trigger route (the F3 envelope) + the mounted WorkflowApp +
  the in-process drive loop; the admin's run explorer renders the
  demo's runs live.
* **The context contract** — the body's ctx carries the runtime info:
  the flow name, the queue, the claim timestamp, the loop's
  budget_remaining_ms, the consumed hold's epoch (the observability
  primitive for body authors; the pins are surface-driven).
* **loop(gates=...)** — the mid-loop hold's gate is declared at wiring
  (the typed door's compile visibility).

### Fixed

* **The corrupted terminal (the case study's own incident class):** a
  node downstream of a map-join consumer dispatched before its parent
  existed — the gather fired on an EMPTY collect and the run terminalized
  SUCCEEDED with nothing done. The full static insert: every non-item
  node exists before the run is live; the fork ADOPTS the pre-inserted
  join; the drain's spawn is join-wait (deps 1 + the edge-ledger row) so
  a consumer's claim follows the join's RESULT write; the spawn key is
  the static key (the arbiter unifies the births — no double rows).
* **The held-drive livelock:** `drive(until="held")` read the hold
  through `scheduled_at > now()` — NULL for a legal no-deadline hold —
  and spun max_ticks (5000 polls). The hold MARKER is the question.
* **The held-drive starvation:** the hold check ran before the tick —
  one held node starved the run's other claimable work. The held answer
  is the quiescent one.
* **The coercion completeness:** a list/union/generic param walks the
  TypeAdapter (the collect's dicts reached the body raw and the union's
  `match` fell to the never-arm).
* **The gather's packer** delivers the FLAT shape its contract promises
  (a gather over list-parents flattens one level; the map join packs
  the items as-is).
* **The capture policy lands:** the workflow's declared
  `capture=` now reaches the failing node's capture jsonb (it was
  inert); a refused capture writes NULL, never `{}`; the truncation
  loop no longer crashes on its own marker's int.

## [Unreleased — TaskQflow phase 2]

### Added

* **Workflows (T06):** the failed-parent propagation — a parent's TERMINAL
  failure resolves the joins counting it by the edge's declared
  `failure_policy`: `fail_closed` (the default) blocks the join (the record
  names the failed parent), peer-cancels the running siblings
  (`CancelledByPeerFailure` + `metadata.peer_cancel`) and fails the flow;
  `collect` fans the failure in as a typed `FailureInfo` item (the estate's
  `ErrorInfo` envelope, the full attempt history) and fires the join with
  the partial result. A skip fans in with zero ledger rows.
* **Workflows (T07):** the declared maximum fan-in per join (1000) refused
  at validate with the child-driven alternative named; the child-driven
  escape (`JoinSpec(child_driven=True)`) counts terminal children from the
  edge ledger; the `maybe` edge policy (absorbed + SURFACED); the scale
  curve pinned across the boundary (the refit + the 100k-edge plan assert).
* **Workflows (T08):** the status + progress rollup — the §17.5 derivation
  table (the absorbed-failure clause stated first), the rows-only
  reconstruction (the two-source rule: the ledger for the attempted
  terminals, the node row's error jsonb for the never-granted ones), the
  G7 always-on reported==reconstructed assertion in every workflow
  integration test, the `taskq.wf_progress_nodes_total{workflow, state}`
  gauge (the declared-workflow dimension, the `_other_` collapse, sampled
  by the maintenance leader on the admin surface), the
  `TaskQWorkflowBlockedStuck` alert in both rule files, the runbook row,
  the insights SQL recipes, the hypothesis totality property, the
  (state, event) totality table (64 cells, zero undefined), one-run-one-
  trace ×10 concurrent, the index-driven rollup cost gate
  (`jobs_wf_flow_nodes_idx`, migration 01.00.25_01).

## [0.2.2](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.1...v0.2.2) (2026-07-22)


### Continuous Integration

* local self-contained publish workflow with attestations off ([#15](https://github.com/AZX-PBC-OSS/TaskQ/issues/15)) ([07fcfce](https://github.com/AZX-PBC-OSS/TaskQ/commit/07fcfced7d4cf8de26859d24b9282ba16a0a25f8))

## [0.2.1](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.0...v0.2.1) (2026-07-22)


### Continuous Integration

* fix reusable-workflow publish — conditional attestations, manual republish dispatch ([#12](https://github.com/AZX-PBC-OSS/TaskQ/issues/12)) ([8798fdd](https://github.com/AZX-PBC-OSS/TaskQ/commit/8797fdd8ab7605b15879c0121055726aa465d26d))

## [0.2.0](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.1.0...v0.2.0) (2026-07-22)


### Features

* managed-identity connections, credential hot-reload, BYO pools ([df9d7c3](https://github.com/AZX-PBC-OSS/TaskQ/commit/df9d7c35ad00f267a6cffc0460a0a0a2cd0ec922))
* managed-identity connections, credential hot-reload, BYO pools ([a754fd7](https://github.com/AZX-PBC-OSS/TaskQ/commit/a754fd730e21f939b2ad4e6e1acd0ebca78c1eb5))


### Bug Fixes

* handle ENOTSOCK in stale socket cleanup, add session backstop fixture ([dc87254](https://github.com/AZX-PBC-OSS/TaskQ/commit/dc8725424fe1c788234d51e9d73dc38b0b92facf))
* log traceback on generic job exceptions ([63d18ca](https://github.com/AZX-PBC-OSS/TaskQ/commit/63d18caafffbcfc9b7fd739cde16a8b2f083b70f))
* PR review correctness fixes, reload hardening, isolated test infra ([5cb6483](https://github.com/AZX-PBC-OSS/TaskQ/commit/5cb64837a28a514c4f2c13ee520af6aa2c5681c8))
* stop swallowing exceptions in worker exception handlers ([4ff0065](https://github.com/AZX-PBC-OSS/TaskQ/commit/4ff0065f242a45a34ee27f9325640114124c0540))
* stop swallowing exceptions in worker exception handlers ([fc7786b](https://github.com/AZX-PBC-OSS/TaskQ/commit/fc7786b3ba6565f6cb4c17879dbf05f71689120d))
* stringify job ids ([8dbf369](https://github.com/AZX-PBC-OSS/TaskQ/commit/8dbf369353fd649997dcf65013b927cd9b263396))


### Documentation

* improve examples, add real-world actors, deployment/troubleshooting/tutorial guides ([1d02b34](https://github.com/AZX-PBC-OSS/TaskQ/commit/1d02b34743014a1edf5574f961853a766761e637))


### Continuous Integration

* add release-please for automated release PRs, tags, and PyPI publish ([#10](https://github.com/AZX-PBC-OSS/TaskQ/issues/10)) ([ab86d7d](https://github.com/AZX-PBC-OSS/TaskQ/commit/ab86d7d371faf550aad8fdceb5f95b9d5da37b48))
* only deploy docs on push to main, not on PRs ([afaffc7](https://github.com/AZX-PBC-OSS/TaskQ/commit/afaffc79c7690db6c2947f61a0e71cf7778bc3d9))

## 0.1.0 - 2026-07-08

### Added

- **Core Job System**
  - `@actor` decorator with typed `ActorRef` references
  - `TaskQ` facade for enqueueing and managing jobs
  - `JobsClient` for job queries, cancellation, and inspection
  - `JobHandle` for awaiting individual job results
  - Batch enqueue with `wait_for_batch` and `BatchHandle`

- **Worker System**
  - Multi-queue worker with configurable concurrency
  - Leader election for singleton job dispatch
  - Graceful shutdown with drain semantics
  - Heartbeat-based lease management
  - Workgroup orchestration for multi-replica deployments

- **Rate Limiting**
  - Sliding window (GCRA) algorithm
  - Token bucket algorithm
  - Composable rate limit groups
  - PostgreSQL and Redis backends

- **Scheduling**
  - Cron-based recurring schedules via `cron()`
  - Delayed job execution

- **Reliability**
  - Configurable retry policies with exponential backoff
  - Job cancellation with phase tracking
  - Idempotency keys and identity-based deduplication
  - Max pending and backpressure controls

- **Observability**
  - Vendor-neutral OpenTelemetry integration
  - Structured logging via structlog
  - Prometheus metrics exporter (optional extra)

- **Admin UI**
  - FastAPI-based web dashboard with htmx
  - Real-time SSE updates
  - Job inspection, queue management, worker monitoring

- **Progress Tracking**
  - Progress event streaming
  - Optional Redis fanout for real-time updates

- **Dependency Injection**
  - Scoped DI container with provider registry
  - Singleton and request scopes

- **Developer Experience**
  - `taskq` CLI (Typer) for migrations, health checks, admin UI, and workgroup management
  - Forward-only SQL migration runner
  - `taskq.testing` module with in-memory backend, fixtures, and assertions
  - Full type safety with py.typed marker

### Changed

- N/A (initial release)

### Security

- No known security issues
