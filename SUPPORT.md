# Support

How to get help with TaskQ — and what kind of help to expect.

> **Stability reminder:** TaskQ is pre-1.0 and follows SemVer 0.x conventions,
> so pin an exact or narrow version range in production until 1.0. See the
> [README](README.md).

## Questions

Ask on [GitHub Discussions](https://github.com/AZX-PBC-OSS/TaskQ/discussions).
Questions are answered on a best-effort basis by the maintainers and
community — there is no response-time guarantee.

For usage questions, check the
[documentation](https://AZX-PBC-OSS.github.io/TaskQ/) first: the
[guides](docs/guides/) cover configuration, deployment, operations, and
troubleshooting runbooks.

## Bug reports and feature requests

Open a [GitHub Issue](https://github.com/AZX-PBC-OSS/TaskQ/issues). Two
templates are available:

- **Bug reports** use the *Bug Report* template (labelled `bug`). Include a
  minimal reproduction, the full error output, and your `taskq-py`, Python,
  and OS versions.
- **Feature requests** use the *Feature Request* template (labelled
  `enhancement`).

Documentation fixes and clarifications are welcome too — open an issue with
the `documentation` label, or a pull request (see
[CONTRIBUTING.md](CONTRIBUTING.md)).

**Do not use issues for security vulnerabilities.** See below.

## Security vulnerabilities

Report vulnerabilities **privately** through
[GitHub Security Advisories](https://github.com/AZX-PBC-OSS/TaskQ/security/advisories/new)
— never as a public issue. [SECURITY.md](SECURITY.md) describes the full
policy, including acknowledgment and fix timelines and the coordinated
disclosure process.

## Enterprise / commercial support

There is no commercial or enterprise support offering attached to this
repository, and none of the documentation promises response-time SLAs. TaskQ
is maintained by [AZX, PBC.](https://github.com/AZX-PBC-OSS) and supported on
a community, best-effort basis through the channels above.

## What is supported

Fully supported means exercised by the project's CI matrix:

- **Python**: 3.12, 3.13, and 3.14.
- **PostgreSQL**: 15–18. 15 is the minimum supported major; older majors may
  work but are untested and unsupported. A direct connection is required —
  PgBouncer in transaction-pooling mode will not work (advisory locks and
  `LISTEN/NOTIFY`). See
  [Installation: prerequisites](docs/getting-started/installation.md#prerequisites).
- **Optional extras** ([pyproject.toml](pyproject.toml) is the source of
  truth): `redis`, `fastapi`, `otel`, `prometheus`, `oidc`, `saml`, `aad`,
  `aws`, `vault`, `reload`, and `bench`.

Everything else — other Python versions, older Postgres majors, other
operating systems, and deployments outside the configurations above — is
best-effort: it may work, but bug reports against unsupported configurations
may be closed as `wontfix`.
