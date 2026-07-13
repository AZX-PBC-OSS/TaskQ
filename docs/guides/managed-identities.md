# Managed Identities & Bring-Your-Own Connections

TaskQ ships DSN-first: the worker, client, admin UI, and migration helpers
all accept a `TASKQ_PG_DSN` / `TASKQ_REDIS_URL` and construct `asyncpg`
pools and `redis.asyncio` clients internally. That model breaks down when
your deployment authenticates with **rotating credentials** — managed
identities (Azure Entra ID, AWS IAM), dynamic secret managers (HashiCorp
Vault), or short-lived OAuth tokens — all of which issue credentials that
expire, so a DSN baked at process start goes stale mid-run.

This guide documents the **connection hook points** TaskQ exposes so you
can either:

1. hand TaskQ a **pre-constructed** pool / connection / Redis client that
   you own and close yourself, or
2. hand TaskQ a **zero-arg async factory** that TaskQ invokes at the
   right point in its lifecycle to build that resource — letting you fetch
   a fresh credential at construction time.

For rotating-credential deployments, TaskQ provides a vendor-neutral
**credential provider** abstraction (`taskq.auth`) with provider-specific
implementations available as extras:

| Extra | Module | Providers |
| --- | --- | --- |
| `taskq[aad]` | `taskq.aad` | Microsoft Entra ID (Azure AD) — PG + Redis |
| `taskq[aws]` | `taskq.aws` | AWS IAM RDS — PG |
| `taskq[vault]` | `taskq.vault` | HashiCorp Vault database secrets engine — PG |
| _(none needed)_ | `taskq.auth` | Base interfaces + factory builders — implement your own provider |

---

## Why hook points (not just "pass a DSN")

| Problem with DSN-only | What hooks give you |
| --- | --- |
| Token expires → pool's cached connections reject auth mid-run. | Factory is called when TaskQ builds the pool; you fetch a fresh token then. |
| `asyncpg.create_pool` binds the password at connect time. | You own pool construction → use asyncpg's `setup`/`server_settings`, or rebuild the pool on token rotation. |
| Azure Redis requires a `CredentialProvider` returning `(username, token)` per reconnect. | You own the `redis.asyncio.Redis` client → pass a `CredentialProvider`. |
| You already run an app-wide pool (FastAPI lifespan) and want to share it. | Pass the pool directly — TaskQ will **not** close a caller-owned resource. |
| Migrations / `TaskQ.stream()` open their own `asyncpg.connect(dsn)`. | `migrate` and the client accept a `conn` / `conn_factory` so LISTEN/migrate work without a DSN. |

### Ownership rule (read this carefully)

> **Pre-constructed** objects (`pool=`, `redis_client=`, `notify_conn=`,
> …) are **caller-owned**. TaskQ never closes them. You close them in your
> own lifespan/finally.
>
> **Factory-produced** objects are **TaskQ-owned**. TaskQ closes them on
> teardown via its `AsyncExitStack`.

---

## Hook point inventory

| Site | Pre-constructed | Factory | Notes |
| --- | --- | --- | --- |
| Worker — dispatcher pool | `WorkerConnections.dispatcher_pool` | `dispatcher_pool_factory` | `pg_dsn_direct` role |
| Worker — heartbeat pool | `WorkerConnections.heartbeat_pool` | `heartbeat_pool_factory` | `command_timeout=2s` is your responsibility when overriding |
| Worker — worker pool | `WorkerConnections.worker_pool` | `worker_pool_factory` | `pg_dsn_pooled` role |
| Worker — notify conn | `WorkerConnections.notify_conn` | `notify_conn_factory` | LISTEN is still issued by TaskQ |
| Worker — leader conn | `WorkerConnections.leader_conn` | `leader_conn_factory` | Advisory-lock conn |
| Worker — Redis | `WorkerConnections.redis_client` | `redis_client_factory` | |
| Client — main pool | `TaskQ(pool=...)` ✓ existing | — | |
| Client — Redis | `TaskQ(redis_client=...)` ✓ existing | — | |
| Client — stream LISTEN conn | `TaskQ(listen_conn=...)` | `TaskQ(pg_conn_factory=...)` | Replaces the DSN-only LISTEN transport |
| Migrate — apply/list | `conn=...` | `conn_factory=...` | `apply_pending_locked(conn_factory=...)` |
| Admin UI | `create_router(pg_pool=..., redis_client=...)` ✓ existing | — | The `ui serve` CLI builds from DSN; for AAD, run the admin UI in-process from your app lifespan and pass a pool. |

---

## The credential provider abstraction

`taskq.auth` provides two async Protocols and reusable factory builders.
Any provider implementing the Protocols gets all the factory builders for
free — no third-party dependencies required.

### Protocols

```python
from taskq.auth import PgCredential, PgCredentialProvider, RedisCredential, RedisCredentialProvider

# Postgres — return a password (token) and optionally a fresh username
class PgCredentialProvider(Protocol):
    async def get_pg_credential(self) -> PgCredential: ...

# Redis — return (username, password)
class RedisCredentialProvider(Protocol):
    async def get_redis_credential(self) -> RedisCredential: ...
```

`PgCredential` carries a `password` (always required) and an optional
`username` — token providers (AAD, AWS IAM) set only the password; dynamic
username providers (Vault) set both. `enrich_pg_dsn` handles either case.

### Factory builders

```python
from taskq.auth import make_pg_pool_factory, make_dedicated_conn_factory, make_redis_client_factory

# Any PgCredentialProvider → PoolFactory / ConnFactory
pool_factory = make_pg_pool_factory(dsn, provider, max_size=8, command_timeout=2)
conn_factory = make_dedicated_conn_factory(dsn, provider)

# Any RedisCredentialProvider → RedisFactory
redis_factory = make_redis_client_factory(url, provider)
```

The factories are zero-arg async callables matching the `PoolFactory` /
`ConnFactory` / `RedisFactory` type aliases in `taskq.connections`. Each
invocation fetches a fresh credential, enriches the DSN, and calls
`asyncpg.create_pool` / `connect` / `redis.from_url`. Redis reconnects
re-fetch automatically via the redis-py `CredentialProvider` adapter.

### Token refresh for long-lived pools

`asyncpg.create_pool` evaluates the password once — the token is baked
into the DSN string at pool construction time and reused for every
internal connection open, even when `max_inactive_connection_lifetime`
churns idle connections. For workloads where the credential expires
faster than the process lifetime, **send SIGHUP to the worker process**:

```bash
kill -HUP <worker-pid>
```

SIGHUP triggers `reload_credentials`, which hot-swaps every factory-backed
pool, dedicated connection, and Redis client with freshly-built
replacements (each factory call fetches a fresh credential). Old resources
are drained in the background with a bounded timeout (default 5 s) —
in-flight queries are given time to finish before the old pool is
force-closed. The dispatcher/consumer/heartbeat loops are never blocked or
paused during a reload — the worker keeps processing jobs throughout.

Each resource reloads independently: if one factory call fails (e.g. a
transient credential-fetch error), that resource simply keeps its current
pool/connection and everything else still reloads. Check the
`credentials-reloaded` log line's `failed` field after a SIGHUP — a
non-empty list means a partial reload; send SIGHUP again to retry the
resources that didn't rotate.

`leader_conn` reload is indirect: closing it triggers the existing leader
watchdog's reopen-and-re-acquire path (the same path a real connection
drop takes), which also rebuilds the leader's other dedicated connections
(the monitor and cron loops' connections) through the same credential
source — so a single SIGHUP rotates every leader-owned connection, not
just `leader_conn` itself. This happens within one `heartbeat_interval`
tick, not instantly.

For AWS IAM RDS (15-minute tokens), schedule a SIGHUP every ~12 minutes:

```bash
# crontab or k8s CronJob
kill -HUP $(pidof taskq-worker)
```

For Azure Redis, refresh is also automatic: `redis-py` calls the
`CredentialProvider` on every reconnect, so a single factory-built client
rotates tokens for free between SIGHUPs.

Caller-owned resources (passed as concrete `pool=` / `redis_client=` /
`notify_conn=`) are **not** swapped by SIGHUP — the caller owns their
lifecycle. Only factory-backed resources are hot-reloaded.

`reload_credentials` can also be called programmatically:

```python
from taskq.worker.deps import reload_credentials

await reload_credentials(deps, drain_timeout=10.0)
```

---

## Provider extras

### Azure Entra ID (AAD) — `taskq[aad]`

```bash
pip install 'taskq-py[aad]'
```

```python
from azure.identity.aio import DefaultAzureCredential
from taskq.auth import make_pg_pool_factory, make_dedicated_conn_factory, make_redis_client_factory
from taskq.aad import EntraIdProvider

cred = DefaultAzureCredential()
provider = EntraIdProvider(cred, redis_username="<managed-identity-object-id>")

WorkerConnections(
    dispatcher_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
    ),
    heartbeat_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct, provider,
        max_size=settings.heartbeat_pool_size, command_timeout=2,
    ),
    worker_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_pooled, provider, max_size=settings.worker_pool_size,
    ),
    notify_conn_factory=make_dedicated_conn_factory(settings.pg_dsn_direct, provider),
    leader_conn_factory=make_dedicated_conn_factory(settings.pg_dsn_direct, provider),
    redis_client_factory=make_redis_client_factory(settings.redis_url, provider),
)
```

`EntraIdProvider` implements **both** Protocols — pass one instance to PG
and Redis factories. For PG-only or Redis-only, use `EntraIdPgProvider` /
`EntraIdRedisProvider` individually.

**Scopes**: `https://ossrdbms-aad.database.windows.net/.default` (PG),
`https://redis.azure.com/.default` (Redis).

**Prerequisites**: enable Entra authentication on Azure DB for Postgres
and Azure Cache for Redis; grant the managed identity the appropriate
roles. `sslmode=require` is enforced automatically.

### AWS IAM RDS — `taskq[aws]`

```bash
pip install 'taskq-py[aws]'
```

```python
from taskq.auth import make_pg_pool_factory, make_dedicated_conn_factory
from taskq.aws import RdsIamProvider

provider = RdsIamProvider(settings.pg_dsn_direct, region="us-east-1")

WorkerConnections(
    dispatcher_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
    ),
    # ... heartbeat, worker, notify, leader similarly
)
```

AWS IAM RDS auth tokens are valid for **15 minutes**. `boto3`'s
`generate_db_auth_token` is local SigV4 signing (no network I/O), so it's
safe to call from an async context. Recreate pools on a schedule shorter
than 15 minutes for long-lived workers.

**Prerequisites**: enable IAM database authentication on the RDS instance;
create an IAM-mapped DB user (`GRANT rds_iam TO myuser`); grant
`rds-db:connect` to the IAM principal. The DSN's `user` must be the
IAM-mapped DB user.

### HashiCorp Vault — `taskq[vault]`

```bash
pip install 'taskq-py[vault]'
```

```python
import hvac
from taskq.auth import make_pg_pool_factory
from taskq.vault import VaultDynamicDbProvider

client = hvac.Client(url="https://vault.example", token="...")
provider = VaultDynamicDbProvider(client, role="taskq-readonly")

WorkerConnections(
    dispatcher_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
    ),
    # ...
)
```

Vault's database secrets engine issues a **fresh username + password** on
each `generate_credentials` call, with a configurable lease TTL. Unlike
token providers, `PgCredential.username` is always set — the DSN's user
is overridden. `hvac` is synchronous; the provider offloads
`generate_credentials` to a thread via `asyncio.to_thread`.

**Prerequisites**: enable the database secrets engine; configure a
connection and role pointing at your Postgres. The DSN's host/port/dbname
must point at the Postgres Vault provisions creds for.

---

## Other patterns (no extra needed)

These don't warrant a dedicated extra — implement a
`PgCredentialProvider` or pass a pre-constructed pool directly.

### GCP Cloud SQL IAM

Use the official `google-cloud-sql-connector` — it handles token refresh
and mTLS automatically, so you don't need a credential provider. Build a
pool factory that uses the connector's async connection helper:

```python
import asyncio
import asyncpg
from google.cloud.sql.connector import Connector, IPTypes

connector = Connector()

async def pg_pool_factory() -> asyncpg.Pool:
    # The connector's async connect helper handles IAM token refresh + mTLS.
    # Offload the sync pool creation to a thread to avoid blocking.
    def _build() -> asyncpg.Pool:
        async def getconn() -> asyncpg.Connection:
            return await connector.connect_async(
                "project:region:instance",
                "pg8000",
                user="my-mi@project.iam",
                db="taskq",
                enable_iam_auth=True,
            )
        return asyncpg.create_pool(getconn)
    return await asyncio.to_thread(_build)

WorkerConnections(dispatcher_pool_factory=pg_pool_factory)
```

### mTLS / client certificates

Pass an `ssl.SSLContext` via a factory — no credential provider needed:

```python
import ssl

sslctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
sslctx.load_cert_chain("client.crt", "client.key")

async def pg_pool_factory() -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=settings.pg_dsn_direct, ssl=sslctx, ...)

WorkerConnections(dispatcher_pool_factory=pg_pool_factory)
```

### Secrets-manager password rotation (AWS Secrets Manager, Doppler, etc.)

Implement a `PgCredentialProvider` that fetches the current password from
your secrets manager on each call:

```python
from taskq.auth import PgCredential, PgCredentialProvider

class SecretsManagerProvider:
    def __init__(self, client, secret_id: str) -> None:
        self._client = client
        self._secret_id = secret_id

    async def get_pg_credential(self) -> PgCredential:
        import asyncio, json
        def _fetch() -> str:
            resp = self._client.get_secret_value(SecretId=self._secret_id)
            return json.loads(resp["SecretString"])["password"]
        password = await asyncio.to_thread(_fetch)
        return PgCredential(password=password)
```

### Custom OAuth / token endpoint

```python
from taskq.auth import PgCredential, PgCredentialProvider
import httpx

class OAuthTokenProvider:
    def __init__(self, token_url: str, client_id: str, client_secret: str) -> None:
        ...

    async def get_pg_credential(self) -> PgCredential:
        async with httpx.AsyncClient() as client:
            resp = await client.post(self._token_url, data={...})
            token = resp.json()["access_token"]
        return PgCredential(password=token)
```

---

## Worker: `WorkerConnections`

`open_worker_deps` and `worker_main` accept an optional
`connections: WorkerConnections` dataclass. Any field left `None` falls
back to the existing DSN construction, so the change is purely additive.

```python
from taskq import WorkerConnections
from taskq.worker.run import worker_main

worker_main(settings, actor_registry=ACTORS, connections=WorkerConnections(
    dispatcher_pool_factory=my_factory,
    # fields left None → TaskQ builds them from DSNs as before.
))
```

Mixing a pre-constructed pool **and** a factory for the same role raises
`ValueError` at startup — pick one.

### `PoolFactory` / `ConnFactory` / `RedisFactory` signatures

```python
type PoolFactory  = Callable[[], Awaitable[asyncpg.Pool]]
type ConnFactory  = Callable[[], Awaitable[asyncpg.Connection]]
type RedisFactory = Callable[[], Awaitable[redis.asyncio.Redis]]
```

All three are **zero-arg async callables** — closures that capture
whatever they need (DSN, sizing, credentials). Exported from `taskq`
top-level.

---

## Client: `TaskQ`

`TaskQ` accepts `pool=` and `redis_client=` (caller-owned). Two additions
close the remaining DSN-only gaps for the LISTEN/NOTIFY transport in
`stream()`:

```python
from taskq.auth import make_dedicated_conn_factory

tq = TaskQ(
    pool=app_state.pg_pool,          # caller-owned
    redis_client=app_state.redis,    # caller-owned
    # LISTEN transport for tq.stream() without a DSN:
    pg_conn_factory=make_dedicated_conn_factory(settings.pg_dsn_direct, provider),  # OR
    listen_conn=app_state.listen_conn,  # pre-constructed, caller-owned
)
```

---

## Migrate

```python
from taskq.migrate import apply_pending_locked

await apply_pending_locked(conn_factory=lambda: build_conn(token), schema="taskq")
```

`conn` (caller-owned) and `conn_factory` (TaskQ-owned) are mutually
exclusive; either replaces the `dsn` parameter.

---

## FastAPI lifespan example

```python
from contextlib import asynccontextmanager
from azure.identity.aio import DefaultAzureCredential
from taskq import TaskQ
from taskq.aad import EntraIdProvider
from taskq.auth import make_pg_pool_factory, make_redis_client_factory

cred = DefaultAzureCredential()
provider = EntraIdProvider(cred)

@asynccontextmanager
async def lifespan(app):
    # Factory-build a caller-owned pool (fresh AAD token at construction)
    # and a caller-owned Redis client (auto-rotating tokens on reconnect).
    pool_factory = make_pg_pool_factory(settings.pg_dsn_direct, provider, max_size=5)
    pg_pool = await pool_factory()
    redis_factory = make_redis_client_factory(settings.redis_url, provider)
    redis_client = await redis_factory()
    try:
        app.state.tq = TaskQ(
            pool=pg_pool,              # caller-owned
            redis_client=redis_client, # caller-owned
        )
        await app.state.tq.open()
        yield
    finally:
        await app.state.tq.close()
        await pg_pool.close()
        await redis_client.aclose()
```

See `examples/fastapi_app/aad.py` for a runnable end-to-end scaffold.
