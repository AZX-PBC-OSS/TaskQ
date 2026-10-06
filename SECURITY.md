# Security Policy

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security vulnerability in TaskQ, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please use [GitHub Security Advisories](https://github.com/AZX-PBC-OSS/TaskQ/security/advisories/new) to report vulnerabilities privately.

## Response Timeline

- **Acknowledgment:** Within 48 hours
- **Initial Assessment:** Within 5 business days
- **Fix or Mitigation:** Depends on severity, typically within 30 days for high-severity issues

## Scope

This policy covers the TaskQ Python package and its CI/CD pipeline. Vulnerabilities in third-party dependencies should be reported to their respective maintainers.

## Network listeners

TaskQ's worker-side health transports are unauthenticated by design and are
meant for the pod network: the health Unix socket
(`TASKQ_HEALTH_SOCKET_PATH` — the field fallback is `/tmp/taskq_health.sock`,
while a worker booted with no explicit path binds the per-process default
`/tmp/taskq_health_<pid>.sock` — plus the
optional TCP listener `TASKQ_HEALTH_PORT`) serves three process gauges, and
the worker's Prometheus scrape (`TASKQ_METRICS_PORT` with the
`[prometheus]` extra) serves metric series naming actors, queues, and
exception classes. Neither carries payloads or credentials, but neither
authenticates its clients: bind them to a loopback or pod-network interface
and do not expose them to untrusted networks.

The admin UI's health and metrics endpoints are a different surface: they are
served through the FastAPI application and are token-gated — in a non-dev
environment the server refuses to boot unless `TASKQ_HEALTH_TOKEN` is set
(or `TASKQ_HEALTH_REQUIRE_TOKEN=false` explicitly opts out).

## Disclosure

We follow coordinated disclosure. Once a fix is released, we will publish a GitHub Security Advisory with credit to the reporter (unless they prefer to remain anonymous).
