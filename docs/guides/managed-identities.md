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

If you run the stock `taskq` console script (including under the
workgroup supervisor), you do **not** need a custom entrypoint to reach
any of this: point `TASKQ_PG_CREDENTIAL_PROVIDER` /
`TASKQ_REDIS_CREDENTIAL_PROVIDER` at your provider and TaskQ builds every
factory for you — see [From the CLI](#from-the-cli-no-custom-entrypoint).
Both are ordinary TaskQ settings, so they can equally live in a `.env`
file alongside `TASKQ_PG_DSN`; the matching CLI flag, when given, wins.

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
| Token expires → **new** connections fail auth. Established sessions survive token expiry; it is pool growth / reconnects that get rejected. | Factory is called when TaskQ builds the pool; you fetch a fresh token then. |
| A password passed as a fixed string is reused for every connection the pool opens later. | The factory builders pass `password=` as an **async callable**, which asyncpg awaits once per physical connection - pool growth and idle-recycle replacements each authenticate with a freshly fetched credential. |
| Azure Redis requires a `CredentialProvider` returning `(username, token)` per reconnect. | You own the `redis.asyncio.Redis` client → pass a `CredentialProvider`. |
| You already run an app-wide pool (FastAPI lifespan) and want to share it. | Pass the pool directly — TaskQ will **not** close a caller-owned resource. |
| Migrations / `TaskQ.stream()` open their own `asyncpg.connect(dsn)`. | `apply_pending_locked` and the client accept a `conn` / `conn_factory` so LISTEN/migrate work without a DSN. |

### Ownership rule (read this carefully)

> **Pre-constructed** objects (`pool=`, `redis_client=`, `notify_conn=`,
> …) are **caller-owned**. TaskQ never closes them. You close them in your
> own lifespan/finally.
>
> **Factory-produced** objects are **TaskQ-owned**. TaskQ closes them on
> teardown via its `AsyncExitStack`.

Three consequences worth knowing:

* A **caller-owned `notify_conn`** that drops leaves TaskQ nothing to
  rebuild through — the NOTIFY listener disables itself (logged as
  `notify-listener-disabled`) and the worker falls back to poll-based
  dispatch instead of crashing.
* A **caller-owned `leader_conn`** with no `leader_conn_factory` **and**
  no `pg_dsn_direct` is a **startup `ValueError`** — a dropped leader
  connection could never be rebuilt, so TaskQ fails fast instead of
  silently never recovering leadership.
* **TaskQ-owned** dedicated connections (DSN- or factory-built
  `notify_conn` / `leader_conn`) get TCP keepalive applied automatically.

---

## Hook point inventory

| Site | Pre-constructed | Factory | Notes |
| --- | --- | --- | --- |
| Worker — dispatcher pool | `WorkerConnections.dispatcher_pool` | `dispatcher_pool_factory` | `pg_dsn_direct` role |
| Worker — heartbeat pool | `WorkerConnections.heartbeat_pool` | `heartbeat_pool_factory` | `command_timeout=2s` is your responsibility when overriding |
| Worker — worker pool | `WorkerConnections.worker_pool` | `worker_pool_factory` | `pg_dsn_pooled` role |
| Worker — notify conn | `WorkerConnections.notify_conn` | `notify_conn_factory` | LISTEN is issued by TaskQ; a dropped conn is rebuilt through the same factory |
| Worker — leader conn | `WorkerConnections.leader_conn` | `leader_conn_factory` | Advisory-lock conn |
| Worker — Redis | `WorkerConnections.redis_client` | `redis_client_factory` | |
| Client — main pool | `TaskQ(pool=...)` (caller-owned) | `TaskQ(pool_factory=...)` or `TaskQ(dsn=..., pg_provider=...)` | TaskQ-owned; rotate with `await tq.reload_credentials()` |
| Client — Redis | `TaskQ(redis_client=...)` ✓ existing | — | |
| Client — stream LISTEN conn | `TaskQ(listen_conn=...)` | `TaskQ(pg_conn_factory=...)` | Replaces the DSN-only LISTEN transport |
| Migrate — locked apply | `apply_pending_locked(conn=...)` | `apply_pending_locked(conn_factory=...)` | `list_applied` / `apply_pending` take an open conn only — no factory |
| Admin UI | `create_router(pg_pool=..., redis_client=...)` ✓ existing | — | Or `taskq ui serve --pg-credential-provider` / `--redis-credential-provider` |
| CLI — `taskq worker` | — | `--pg-credential-provider` / `--redis-credential-provider` (env: `TASKQ_PG_CREDENTIAL_PROVIDER` / `TASKQ_REDIS_CREDENTIAL_PROVIDER`) | Builds **every** worker role: all three pools, `notify_conn`, `leader_conn`, Redis |
| CLI — `taskq workgroup start` | — | same env vars | Children are `taskq worker` subprocesses and inherit the environment |
| CLI — `taskq migrate up` / `status` | — | `--pg-credential-provider` (env: `TASKQ_PG_CREDENTIAL_PROVIDER`) | One-shot connection through the provider |

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
from taskq import make_pg_pool_factory, make_dedicated_conn_factory, make_redis_client_factory
# (also importable from taskq.auth)

# Any PgCredentialProvider → PoolFactory / ConnFactory
pool_factory = make_pg_pool_factory(dsn, provider, max_size=8, command_timeout=5.0)
conn_factory = make_dedicated_conn_factory(dsn, provider)

# Any RedisCredentialProvider → RedisFactory
redis_factory = make_redis_client_factory(url, provider)
```

The factories are zero-arg async callables matching the `PoolFactory` /
`ConnFactory` / `RedisFactory` type aliases in `taskq.connections`. Each
invocation fetches a fresh credential and calls `asyncpg.create_pool` /
`asyncpg.connect` / `redis.from_url`. Redis reconnects re-fetch
automatically via the redis-py `CredentialProvider` adapter.

**How the credential reaches asyncpg**: the PG factories pass it as
`password=` (always) and `user=` (when the credential carries one)
**keyword arguments**. Keyword arguments take precedence over both DSN
userinfo and DSN query parameters in asyncpg's resolver, so a stale
credential baked into the DSN can never shadow the fresh one — and the
token never appears in the DSN string.

`enrich_pg_dsn(dsn, credential)` (also exported from `taskq` top-level)
is the string-helper variant for callers that need a self-contained DSN:
the credential is written into the DSN **userinfo** (percent-encoded),
replacing any existing userinfo password — and replacing the user only
when `credential.username` is set (Vault dynamic creds). Never put the
credential in the query string instead: asyncpg applies userinfo *before*
query parameters, so a `password=` query param is silently ignored
whenever the DSN already carries userinfo (`enrich_pg_dsn` drops stale
`user=` / `password=` query params for the same reason).

**sslmode**: both the factories and `enrich_pg_dsn` add
`sslmode=require` **only when the DSN has no explicit sslmode** — an
explicit `verify-ca` / `verify-full` is preserved (never downgraded) and
is recommended where your server presents a verifiable certificate.
`require` encrypts the connection but does **not** verify the server
certificate.

## From the CLI (no custom entrypoint)

`taskq worker`, `taskq workgroup start`, `taskq ui serve` and
`taskq migrate` resolve a credential provider from a `module:attr`
reference — the same syntax `--actors` uses:

```bash
export TASKQ_PG_CREDENTIAL_PROVIDER=myapp.auth:make_provider
export TASKQ_REDIS_CREDENTIAL_PROVIDER=myapp.auth:make_provider   # optional
taskq worker --actors myapp.actors:registry
```

```python
# myapp/auth.py
from azure.identity.aio import DefaultAzureCredential
from taskq.aad import EntraIdProvider


def make_provider() -> EntraIdProvider:
    return EntraIdProvider(DefaultAzureCredential(), redis_username="<mi-object-id>")
```

The reference may point at any of:

* a **provider instance** — `myapp.auth:PROVIDER`
* a **zero-arg factory** returning one — `myapp.auth:make_provider`
* the **provider class**, when its constructor takes no required
  arguments — `myapp.auth:MyProvider`

Equivalent CLI flags exist for one-off runs:
`--pg-credential-provider` / `--redis-credential-provider`.

**What the worker builds.** Every Postgres role — the dispatcher,
heartbeat and worker pools, the `notify_conn` LISTEN connection and the
`leader_conn` advisory-lock connection — plus the Redis client is built
through your provider, sized and timed out exactly as the DSN path sizes
them (`TASKQ_DISPATCHER_POOL_SIZE`, `TASKQ_HEARTBEAT_COMMAND_TIMEOUT`,
`TASKQ_POOL_MAX_INACTIVE_LIFETIME`, `dispatcher_command_timeout` on the
dedicated connections). Because *every* role is factory-backed, SIGHUP and
`TASKQ_RELOAD_INTERVAL` rotate all of them — a role left on the DSN
fallback would be silently un-rotatable, since `reload_credentials`
skips roles that have no factory.

The DSN is still read: it supplies host, port, database and (for
username-stable providers) the user. The credential itself is passed to
asyncpg as keyword arguments and never appears in the DSN string.

**Workgroups.** The supervisor spawns `taskq worker` subprocesses and
does not scrub the environment, so exporting the env vars before
`taskq workgroup start` configures every child. Send SIGHUP to the
children (`pkill -HUP -f 'taskq worker'`), not to the supervisor.

**Failures are fatal, never silent.** A reference that does not import,
names a missing attribute, resolves to something without an async
`get_pg_credential()` / `get_redis_credential()`, or names a Redis
provider with no `TASKQ_REDIS_URL` set, exits non-zero at startup. There
is no fallback to the DSN password: a worker that quietly authenticated
with a static credential would look healthy until the first reconnect
after deploy.

**Other commands.**

```bash
# admin UI — pool and (with --migrate) the migration connection
taskq ui serve --pg-credential-provider myapp.auth:make_provider

# migrations — the one-shot connection
taskq migrate up --pg-credential-provider myapp.auth:make_provider
```

`taskq ui serve` has no reload path, and does not need one: its pool
re-authenticates per physical connection through the same `password=`
callable described below.

**Embedding.** The builder behind the worker option is public — use it
when you have a custom entrypoint and want the same full wiring:

```python
from taskq.auth import build_worker_connections

connections = build_worker_connections(
    settings, pg_provider=provider, redis_provider=provider
)
worker_main(settings, actor_registry=ACTORS, connections=connections)
```

---

### Token refresh for long-lived pools

**Token refresh is automatic - no external rotation schedule is
required.** The factory builders pass the token to asyncpg as a
`password=` **async callable** (never baked into the DSN string), and
asyncpg invokes and awaits it once per *physical* connection: the
connections opened at pool creation, those opened later by pool growth,
and the replacements opened after `max_inactive_connection_lifetime`
recycles an idle connection. Every new connection therefore
authenticates with a freshly fetched credential.

This matters because Postgres authenticates at connect time only. A
credential resolved once and reused as a fixed string keeps working on
already-open sessions while **new** connections fail auth - so a pool
built that way is healthy at deploy and then, roughly one token lifetime
later, degrades as idle connections are recycled. That was TaskQ's
behaviour before this changed; it no longer is.

Two things per-connection refresh cannot do, for which the
**credential hot-reload** below is still the answer:

* **A changed username.** asyncpg resolves `user=` once per pool and
  accepts a callable only for `password=`. Providers that issue a fresh
  username alongside each password (HashiCorp Vault dynamic database
  credentials) need a pool rebuild; the password callable raises a
  `RuntimeError` naming this rather than pairing a fresh password with
  the stale username.
* **Forcing a full pool rebuild** - e.g. to drop sessions opened under a
  revoked credential, or after a DSN/endpoint change.

All four triggers run the same `reload_credentials` path:

| Trigger | How | When to use |
| --- | --- | --- |
| `TASKQ_RELOAD_INTERVAL` (seconds, unset by default) | `TASKQ_RELOAD_INTERVAL=720 taskq worker --actors …` | **Recommended.** Periodic reload with no external signal — the only option on Windows (no SIGHUP) and the hands-off option everywhere else. |
| SIGHUP | `pkill -HUP -f 'taskq worker'` | Unix on-demand rotation (cron, k8s CronJob, config-change hooks). |
| `deps.request_reload()` | programmatic, from an embedder holding `WorkerDeps` | In-process trigger (e.g. your own secrets-watch callback). Equivalent to SIGHUP. |
| `reload_credentials(deps, ...)` | direct async call | Lower-level (e.g. tests); returns `(reloaded, failed)`. |

A reload only rotates **factory-backed** resources. On the console
script that means the provider must be configured
(`TASKQ_PG_CREDENTIAL_PROVIDER`, see [From the CLI](#from-the-cli-no-custom-entrypoint));
on a pure DSN worker a reload reconnects `notify_conn` / `leader_conn`
with the same static DSN credential and rotates nothing else. Confirm
with the `credentials-reloaded` log line — its `resources` list names
what actually rotated.

SIGHUP delivery patterns (the console script is `taskq` — there is no
`taskq-worker` process name):

```bash
# cron / k8s CronJob / operator script
pkill -HUP -f 'taskq worker'

# systemd unit
ExecReload=/bin/kill -HUP $MAINPID

# container where `taskq worker` is PID 1 (exec-form entrypoint)
kill -HUP 1
```

What a reload does:

* Every **factory-backed** pool, dedicated connection, and Redis client
  is rebuilt by re-invoking its factory (each factory call fetches a
  fresh credential). Each factory call is bounded by
  `TASKQ_RELOAD_FACTORY_TIMEOUT` (default 30 s) so a hung token endpoint
  cannot wedge the reload.
* The swap is atomic: the old pool stops serving new acquisitions
  immediately, so new work starts on the new pool. DI-injected
  `db: asyncpg.Pool` actors also resolve the new pool (the LOOP-scope
  cache is refreshed after a successful worker-pool reload), and
  progress flushing follows the swap on its next tick.
* The old pool is closed in the background with a **bounded drain**
  (`drain_timeout`, default 5 s): in-flight actors holding the old pool
  get that long to finish. On timeout the old pool is **terminated**;
  an actor that outlives the drain sees its next `acquire()` fail and
  the job retries — landing on the new pool.
* A SIGHUP arriving **mid-reload** (success or failure) is honored with
  exactly one follow-up reload; N signals during one reload coalesce
  into one follow-up, not N. Reloads are skipped while shutdown is in
  progress.
* Job processing continues throughout — the dispatcher/consumer/
  heartbeat loops are not stopped for a reload.

Each resource reloads independently: if one factory call fails (e.g. a
transient credential-fetch error), that resource simply keeps its current
pool/connection and everything else still reloads. Check the
`credentials-reloaded` log line's `failed` field after a SIGHUP — a
non-empty list means a partial reload; trigger another reload to retry
the resources that didn't rotate.

`leader_conn` reload is indirect: closing it triggers the existing leader
watchdog's reopen-and-re-acquire path (the same path a real connection
drop takes), which also rebuilds the leader's other dedicated connections
(the monitor and cron loops' connections) through the same credential
source — so a single reload rotates every leader-owned connection, not
just `leader_conn` itself. This happens within one `heartbeat_interval`
tick, not instantly.

For AWS IAM RDS (15-minute tokens), rotate every ~12 minutes:

```bash
# hands-off (recommended)
export TASKQ_PG_CREDENTIAL_PROVIDER=myapp.auth:make_provider
TASKQ_RELOAD_INTERVAL=720 taskq worker --actors myapp.actors:registry

# or via cron / k8s CronJob
pkill -HUP -f 'taskq worker'
```

For Azure Redis, refresh is also automatic: `redis-py` calls the
`CredentialProvider` on every reconnect, so a single factory-built client
rotates tokens for free between reloads.

Caller-owned resources (passed as concrete `pool=` / `redis_client=` /
`notify_conn=`) are **not** swapped by a reload — the caller owns their
lifecycle. Only factory-backed resources are hot-reloaded.

`reload_credentials` can also be called programmatically (also
re-exported from `taskq.worker`):

```python
from taskq.worker.deps import reload_credentials

await reload_credentials(deps, drain_timeout=10.0, factory_timeout=30.0)
```

---

## Provider extras

### Azure Entra ID (AAD) — `taskq[aad]`

```bash
pip install 'taskq-py[aad]'
```

The extra includes `azure-identity` **and `aiohttp`** (required by the
`azure.identity.aio` async credentials).

```python
from azure.identity.aio import DefaultAzureCredential
from taskq import make_pg_pool_factory, make_dedicated_conn_factory, make_redis_client_factory
from taskq.aad import EntraIdProvider

cred = DefaultAzureCredential()
provider = EntraIdProvider(cred, redis_username="<managed-identity-object-id>")

WorkerConnections(
    dispatcher_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct,
        provider,
        max_size=settings.dispatcher_pool_size,
    ),
    heartbeat_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct,
        provider,
        max_size=settings.heartbeat_pool_size,
        command_timeout=settings.heartbeat_command_timeout,
    ),
    worker_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_pooled,
        provider,
        max_size=settings.worker_pool_size,
    ),
    notify_conn_factory=make_dedicated_conn_factory(settings.pg_dsn_direct, provider),
    leader_conn_factory=make_dedicated_conn_factory(settings.pg_dsn_direct, provider),
    redis_client_factory=make_redis_client_factory(settings.redis_url, provider),
)
```

`EntraIdProvider` implements **both** Protocols — pass one instance to PG
and Redis factories. For PG-only or Redis-only, use `EntraIdPgProvider` /
`EntraIdRedisProvider` individually.

The providers accept either an **async** credential
(`azure.identity.aio`, as above) or a **sync** `azure.identity`
credential — sync credentials perform blocking HTTP, so their
`get_token` is offloaded to a thread and never stalls the event loop.
The credential you pass is **caller-owned** (close it in your lifespan).
Pass `credential=None` (the default) and the provider lazily creates
**one** `DefaultAzureCredential` and reuses it for its lifetime.

**Scopes**: `https://ossrdbms-aad.database.windows.net/.default` (PG),
`https://redis.azure.com/.default` (Redis).

**Prerequisites**: enable Entra authentication on Azure DB for Postgres
and Azure Cache for Redis; grant the managed identity the appropriate
roles. The factories add `sslmode=require` when the DSN has no explicit
sslmode (see *sslmode* under Factory builders) — Azure requires TLS.

### AWS IAM RDS — `taskq[aws]`

```bash
pip install 'taskq-py[aws]'
```

```python
from taskq import make_pg_pool_factory, make_dedicated_conn_factory
from taskq.aws import RdsIamProvider

provider = RdsIamProvider(settings.pg_dsn_direct, region="us-east-1")

WorkerConnections(
    dispatcher_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct,
        provider,
        max_size=settings.dispatcher_pool_size,
    ),
    # ... heartbeat, worker, notify, leader similarly
)
```

AWS IAM RDS auth tokens are valid for **15 minutes**.
`generate_db_auth_token` itself is local SigV4 signing, but resolving the
ambient AWS credential chain can block on STS/IMDS HTTPS refreshes — so
the provider offloads the boto call to a thread rather than stalling the
event loop. Pass `region=None` (the default) to let botocore fall back
to the ambient client region. Reload on a schedule shorter than 15
minutes for long-lived workers (e.g. `TASKQ_RELOAD_INTERVAL=720`).

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
from taskq import make_pg_pool_factory
from taskq.vault import VaultDynamicDbProvider

client = hvac.Client(url="https://vault.example", token="...")
provider = VaultDynamicDbProvider(client, role="taskq-readonly")

WorkerConnections(
    dispatcher_pool_factory=make_pg_pool_factory(
        settings.pg_dsn_direct,
        provider,
        max_size=settings.dispatcher_pool_size,
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
pool factory on the connector's async API (`create_async_connector` +
`connect_async` with the `"asyncpg"` driver and `enable_iam_auth=True`),
and pass it to `asyncpg.create_pool` via the **`connect=` keyword**. All
of these calls are already async, so the factory awaits them directly —
no `asyncio.to_thread` offload:

```python
from contextlib import asynccontextmanager

import asyncpg
from google.cloud.sql.connector import create_async_connector


def make_cloudsql_pool_factory(connector, instance: str, user: str, db: str):
    """Zero-arg async pool factory backed by the Cloud SQL connector."""

    async def pg_pool_factory() -> asyncpg.Pool:
        async def getconn() -> asyncpg.Connection:
            return await connector.connect_async(
                instance,  # "project:region:instance"
                "asyncpg",  # driver — asyncpg, not pg8000
                user=user,  # IAM principal, e.g. "my-mi@project.iam"
                db=db,
                enable_iam_auth=True,
            )

        return await asyncpg.create_pool(connect=getconn)  # connect= keyword

    return pg_pool_factory


@asynccontextmanager
async def lifespan(app):
    connector = await create_async_connector()
    try:
        connections = WorkerConnections(
            dispatcher_pool_factory=make_cloudsql_pool_factory(
                connector, "project:region:instance", "my-mi@project.iam", "taskq"
            ),
            # ... heartbeat, worker, notify, leader similarly
        )
        # hand connections to worker_main / open_worker_deps
        yield
    finally:
        await connector.close_async()  # caller-owned: you close the connector
```

### mTLS / client certificates

Pass an `ssl.SSLContext` via a factory — no credential provider needed:

```python
import ssl

sslctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
sslctx.load_cert_chain("client.crt", "client.key")


async def pg_pool_factory() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=settings.pg_dsn_direct,
        ssl=sslctx,
        min_size=1,
        max_size=4,
    )


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
    def __init__(self, token_url: str, client_id: str, client_secret: str) -> None: ...

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

worker_main(
    settings,
    actor_registry=ACTORS,
    connections=WorkerConnections(
        dispatcher_pool_factory=my_factory,
        # fields left None → TaskQ builds them from DSNs as before.
    ),
)
```

Mixing a pre-constructed pool **and** a factory for the same role raises
`ValueError` at startup — pick one.

### `worker_main_async` — when you must own the loop

`worker_main` drives its own `asyncio.Runner`. If your actors are closures
over dependencies you have to build first — an `asyncpg.Pool` is **loop-bound**,
so it must be created on the loop the worker will run on — await
`worker_main_async` from your own coroutine instead. Same parameters, same
exit codes; `worker_main` is the thin sync wrapper over it.

```python
import asyncio, asyncpg
from taskq.worker import worker_main_async

async def main() -> int:
    pool = await asyncpg.create_pool(dsn)      # this loop
    registry = build_actors(pool)              # actors close over it
    try:
        return await worker_main_async(settings, actor_registry=registry)
    finally:
        await pool.close()                     # yours to close

raise SystemExit(asyncio.run(main()))
```

### `build_worker_connections` with an explicit DSN

`build_worker_connections(settings, pg_provider=…)` reads its endpoints from
`TASKQ_PG_DSN`. If your application already knows where TaskQ's tables live,
pass that in rather than restating it in a second config system:

```python
connections = build_worker_connections(
    worker_settings,
    pg_provider=provider,
    pg_dsn=my_settings.taskq_pg_dsn,      # or pg_dsn_direct=/pg_dsn_pooled= for a pgbouncer split
    redis_url=my_settings.taskq_redis_url,
)
```

Only the **endpoint** is overridden — every pool size and timeout still comes
from `WorkerSettings`. That is the point: hand-building one
`make_pg_pool_factory` and passing it to all three pool roles silently gives
each role a full `max_size` pool (`5 x pool_max + 3` per replica) instead of
`dispatcher_pool_size` / `heartbeat_pool_size` / `worker_pool_size`.

You only need this when you have a custom entrypoint. Running the stock
console script, set `TASKQ_PG_CREDENTIAL_PROVIDER` instead
([From the CLI](#from-the-cli-no-custom-entrypoint)); with a custom
entrypoint, `taskq.auth.build_worker_connections(settings,
pg_provider=…, redis_provider=…)` builds the same complete set of
factories in one call.

### `PoolFactory` / `ConnFactory` / `RedisFactory` signatures

```python
type PoolFactory = Callable[[], Awaitable[asyncpg.Pool]]
type ConnFactory = Callable[[], Awaitable[asyncpg.Connection]]
type RedisFactory = Callable[[], Awaitable[redis.asyncio.Redis]]
```

All three are **zero-arg async callables** — closures that capture
whatever they need (DSN, sizing, credentials). Exported from `taskq`
top-level.

---

## Client: `TaskQ`

### Rotating credentials on the client

`TaskQ` takes a `pool_factory=` — the same
[`PoolFactory`](#poolfactory--connfactory--redisfactory-signatures) the worker
takes per role. TaskQ invokes it at `open()` and **owns** the result, and
`await tq.reload_credentials()` re-invokes it to swap the pool in place: the
backend behind `enqueue`/`get`/`list`/`cancel`, the `tq.actors` client and the
`stream()` LISTEN fallback all move to the new pool, and the old one is closed
with a bounded drain. Nothing needs to reach into the client's internals, and
no restart is required when a token expires.

```python
from taskq import TaskQ
from taskq.aad import EntraIdProvider

provider = EntraIdProvider()

# Sugar: dsn + provider. Exactly make_pg_pool_factory(dsn, provider,
# min_size=min_pool_size, max_size=max_pool_size) under the hood.
tq = TaskQ(dsn=dsn, pg_provider=provider, max_pool_size=5)
await tq.open()

# ...anywhere: a scheduled task, a signal handler, your issuer's callback.
await tq.reload_credentials()
```

Use `pool_factory=` directly when you need the factory's other hooks
(`init=`, `server_settings=`, `command_timeout=`):

```python
from taskq import make_pg_pool_factory

tq = TaskQ(pool_factory=make_pg_pool_factory(dsn, provider, max_size=5, init=register_vector))
```

`dsn=`, `pool=` and `pool_factory=` are mutually exclusive, and `pg_provider=`
requires `dsn=`. Ownership follows the rule above: a pool TaskQ built (from a
DSN or a factory) is closed by `tq.close()`; one you passed as `pool=` never
is — and `reload_credentials()` refuses to rotate it, because rotating means
closing.

Note that `reload_credentials()` is not needed for ordinary token refresh:
`make_pg_pool_factory` passes `password=` to asyncpg as a callable, so every
*new physical connection* already authenticates with a freshly fetched
credential. Reload is how you drop sessions opened under a **revoked**
credential, and the only way to pick up a **changed username** (asyncpg
resolves `user=` once per pool).

### LISTEN transport

`TaskQ` accepts `pool=` and `redis_client=` (caller-owned). Two additions
close the remaining DSN-only gaps for the LISTEN/NOTIFY transport in
`stream()`:

```python
from taskq import make_dedicated_conn_factory

tq = TaskQ(
    pool=app_state.pg_pool,  # caller-owned
    redis_client=app_state.redis,  # caller-owned
    # LISTEN transport for tq.stream() without a DSN:
    pg_conn_factory=make_dedicated_conn_factory(settings.pg_dsn_direct, provider),  # OR
    listen_conn=app_state.listen_conn,  # pre-constructed, caller-owned
)
```

Without one of Redis / `pg_conn_factory` / `listen_conn`, `tq.stream()`
raises a documented `RuntimeError` in pool-only mode.

---

## `ensure_sslmode_require`

Every factory in `taskq.auth` applies this before connecting. Call it yourself
on the DSN paths they do not cover — a raw `asyncpg.connect`, a migration
connection, a DSN handed to another library:

```python
from taskq import ensure_sslmode_require

conn = await asyncpg.connect(ensure_sslmode_require(dsn), password=token)
```

It adds `sslmode=require` **only when no sslmode is set**. An explicit mode is
never overridden — `verify-ca` / `verify-full` are not downgraded (`require`
skips certificate verification, which would expose the very token you are
injecting), and `sslmode=disable` stays disabled, which is how a test container
or a Unix-socket deployment opts out.

---

## Migrate

```python
from taskq.migrate import apply_pending_locked

await apply_pending_locked(conn_factory=lambda: build_conn(token), schema="taskq")
```

`conn` (caller-owned) and `conn_factory` (TaskQ-owned) are mutually
exclusive; either replaces the `dsn` parameter.

From the CLI, `taskq migrate up --pg-credential-provider
myapp.auth:make_provider` (also `migrate status`, and `ui serve
--migrate`) builds that `conn_factory` for you.

---

## FastAPI lifespan example

```python
from contextlib import asynccontextmanager
from azure.identity.aio import DefaultAzureCredential
from taskq import TaskQ, make_pg_pool_factory, make_redis_client_factory
from taskq.aad import EntraIdProvider


@asynccontextmanager
async def lifespan(app):
    # The credential is caller-owned — async with (or aclose() in a
    # finally) so its aiohttp session is closed on shutdown.
    async with DefaultAzureCredential() as cred:
        provider = EntraIdProvider(cred)
        # Factory-build a caller-owned pool (fresh AAD token at construction)
        # and a caller-owned Redis client (auto-rotating tokens on reconnect).
        pool_factory = make_pg_pool_factory(settings.pg_dsn_direct, provider, max_size=5)
        pg_pool = await pool_factory()
        redis_factory = make_redis_client_factory(settings.redis_url, provider)
        redis_client = await redis_factory()
        try:
            app.state.tq = TaskQ(
                pool=pg_pool,  # caller-owned
                redis_client=redis_client,  # caller-owned
            )
            await app.state.tq.open()
            yield
        finally:
            await app.state.tq.close()
            await pg_pool.close()
            await redis_client.aclose()
```

See `examples/fastapi_app/aad.py` for a runnable end-to-end scaffold.
