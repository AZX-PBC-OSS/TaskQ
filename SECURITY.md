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

TaskQ's optional TCP listeners are unauthenticated by design and are meant
for the pod network: the health socket (`TASKQ_HEALTH_SOCKET`, plus the
optional health port) serves three process gauges, and the worker's
Prometheus scrape (`TASKQ_METRICS_PORT` with the `[prometheus]` extra)
serves metric series naming actors, queues, and exception classes. Neither
carries payloads or credentials, but neither authenticates its clients:
bind them to a loopback or pod-network interface and do not expose them
to untrusted networks.

## Disclosure

We follow coordinated disclosure. Once a fix is released, we will publish a GitHub Security Advisory with credit to the reporter (unless they prefer to remain anonymous).
