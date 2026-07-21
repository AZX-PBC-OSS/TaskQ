# Credential Providers

Vendor-neutral credential provider Protocols (`PgCredentialProvider`,
`RedisCredentialProvider`), the `PgCredential` / `RedisCredential` carriers,
the `make_*` factory builders, and `enrich_pg_dsn`. Provider-specific
implementations live in the [`taskq[aad]`](aad.md), [`taskq[aws]`](aws.md),
and [`taskq[vault]`](vault.md) extras. See the
[Managed Identities guide](../guides/managed-identities.md) for usage.

::: taskq.auth
