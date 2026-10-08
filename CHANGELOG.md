# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
