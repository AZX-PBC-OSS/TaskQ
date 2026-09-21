# SSO / SAML

The TaskQ admin UI supports optional single sign-on (SSO) via **OIDC** (primary,
for Microsoft Entra ID and any OIDC-compliant provider) and **SAML** (for legacy
IdPs), behind a shared abstraction. Both are purely additive: the default
(`TASKQ_SSO_BACKEND=none`) remains the unauthenticated / bring-your-own-auth
behavior described in [admin-ui.md](admin-ui.md).

---

## Which one should I use?

| | OIDC (`taskq[oidc]`) | SAML (`taskq[saml]`) |
|---|---|---|
| **Native dependencies** | None (pure Python) | None on common platforms; see [SAML container requirements](#saml-container-requirements) |
| **Setup complexity** | Lower | Slightly higher (IdP metadata exchange; no container changes needed on common platforms) |
| **Entra support** | App registrations (recommended) | Enterprise applications (SAML gallery/non-gallery) |
| **When to use** | Default for new integrations | Only if an IdP or compliance requirement mandates SAML |

**Default to OIDC.** It requires no system packages, no container changes, and
works against any OIDC provider. Reserve SAML for IdPs that genuinely require it.

---

## Quick start: OIDC with `taskq ui serve`

```sh
pip install 'taskq[fastapi]' 'taskq[oidc]'

export TASKQ_SSO_BACKEND=oidc
export TASKQ_OIDC_ISSUER='https://login.microsoftonline.com/{tenant}/v2.0'
export TASKQ_OIDC_CLIENT_ID='your-client-id'
export TASKQ_OIDC_CLIENT_SECRET='your-client-secret'
export TASKQ_OIDC_REDIRECT_URI='https://admin.example.com/admin/callback'
export TASKQ_OIDC_SESSION_SECRET='$(python -c "import secrets; print(secrets.token_urlsafe(32))")'

taskq ui serve
```

The CLI reads `TASKQ_SSO_BACKEND` and the matching `TASKQ_OIDC_*` settings,
builds the auth bundle, and mounts the `/login`, `/callback`, and `/logout`
routes alongside the admin router at `/admin`. When `TASKQ_SSO_BACKEND=oidc`
(or `saml`), a non-`None` `auth_dependency` is passed to `create_router`, which
also satisfies the `admin_ui_require_auth` fail-closed check.

`TASKQ_SSO_BACKEND=none` (the default) preserves today's unauthenticated /
BYO-auth behavior unchanged.

### Cookie security in local dev

`secure_cookie` is derived from `TASKQ_ENVIRONMENT`: set
`TASKQ_ENVIRONMENT=dev` (or `development`) to use non-secure cookies over
local `http://localhost`. In any other environment, cookies are `Secure`
(HTTPS only).

---

## OIDC configuration

### `OIDCAuthConfig`

| Field | Type | Default | Env var |
|---|---|---|---|
| `issuer` | `str` | n/a | `TASKQ_OIDC_ISSUER` |
| `client_id` | `str` | n/a | `TASKQ_OIDC_CLIENT_ID` |
| `client_secret` | `str` | n/a | `TASKQ_OIDC_CLIENT_SECRET` |
| `redirect_uri` | `str` | n/a | `TASKQ_OIDC_REDIRECT_URI` |
| `session_secret` | `str` | n/a | `TASKQ_OIDC_SESSION_SECRET` |
| `session_max_age_seconds` | `int` | `28800` (8h) | `TASKQ_OIDC_SESSION_MAX_AGE_SECONDS` |
| `scope` | `str` | `openid profile email` | `TASKQ_OIDC_SCOPE` |
| `group_claim` | `str \| None` | `None` | `TASKQ_OIDC_GROUP_CLAIM` |
| `allowed_groups` | `frozenset[str]` | `frozenset()` | `TASKQ_OIDC_ALLOWED_GROUPS` (comma-separated) |
| `group_resolver` | `Callable \| None` | `None` | _(programmatic only)_ |

`session_secret` should be at least 32 bytes of random data. Rotating it
invalidates every outstanding session at once, since there is no session store to flush.

### Login flow

1. **`/login`**: generates a PKCE `code_verifier` + `state`, stores both in a
   short-lived signed cookie (separate from the session cookie), and redirects
   to the IdP authorization endpoint.
2. **`/callback`**: validates `state`, exchanges the code for tokens,
   validates the ID token (issuer, audience, signature via JWKS), extracts
   claims into `IdentityClaims`, sets the session cookie, and redirects to the
   admin UI root.
3. **`/logout`** (POST), requires a CSRF token bound to the live session
   (the admin UI's Sign out control posts it), clears the session cookie,
   and redirects to the admin root. `GET /logout` is refused (405), so a
   forced top-level navigation from any page cannot clear an admin session
   (see [Logging out](#logging-out)).

On any error during `/callback` (token exchange failure, JWKS fetch timeout,
invalid ID token), the user is redirected with a generic
`?error=authentication+failed`; **never** raw exception text. The full
exception is logged server-side.

### Discovery and JWKS caching

`/login` needs the issuer's discovery document and `/callback` needs the
discovery document plus the JWKS. Both are cached in memory per issuer with
a 300 s TTL (bounded to the most recently used 16 issuers), so a flood of
unauthenticated `/login` requests costs at most one outbound fetch per
issuer per TTL window instead of one per request. A refresh that fails
invalidates the issuer's cache entry, the next request starts from a fresh
fetch rather than serving a document the IdP would not refresh, and a
rotated IdP signing key is picked up when the entry expires, at most one
TTL window later. The cache is process-local, like every store in the SSO
layer; replicas fetch independently.

### Group overage (Entra-specific)

Once a user belongs to more than ~200 groups, Entra omits the `groups` claim
from the ID token and emits a `_claim_names`/`hasgroups` marker instead. The
optional `group_resolver` callable handles this: it receives an
`OIDCTokenContext` (ID token claims + access token) and returns a
`frozenset[str]` of groups, typically by calling Microsoft Graph
`/me/memberOf`. To use it, add `Group.Read.All` to `TASKQ_OIDC_SCOPE`.

A reference Graph-API resolver (using `httpx2`) ships as a documented example;
it is **not** a hard dependency of `taskq[oidc]`:

```python
import httpx2
from taskq.web.admin.auth import OIDCAuthConfig, OIDCTokenContext, create_oidc_auth


async def graph_group_resolver(ctx: OIDCTokenContext) -> frozenset[str]:
    if ctx.access_token is None:
        return frozenset()
    headers = {"Authorization": f"Bearer {ctx.access_token}"}
    groups: set[str] = set()
    url = "https://graph.microsoft.com/v1.0/me/memberOf?$select=id"
    while url:
        resp = await httpx2.AsyncClient().get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for g in data.get("value", []):
            if g.get("@odata.type", "").endswith("group"):
                groups.add(g["id"])
        url = data.get("@odata.nextLink")
    return frozenset(groups)


config = OIDCAuthConfig(
    issuer="https://login.microsoftonline.com/{tenant}/v2.0",
    client_id="...",
    client_secret="...",
    redirect_uri="https://admin.example.com/admin/callback",
    session_secret="...",
    scope="openid profile email Group.Read.All",
    group_claim="groups",
    allowed_groups=frozenset({"admin-group-object-id"}),
    group_resolver=graph_group_resolver,
)
bundle = create_oidc_auth(config, base_path="/admin")
```

---

## SAML configuration

### `SAMLAuthConfig`

| Field | Type | Default | Env var |
|---|---|---|---|
| `entity_id` | `str` | n/a | `TASKQ_SAML_ENTITY_ID` |
| `acs_url` | `str` | n/a | `TASKQ_SAML_ACS_URL` |
| `idp_entity_id` | `str` | n/a | `TASKQ_SAML_IDP_ENTITY_ID` |
| `idp_sso_url` | `str` | n/a | `TASKQ_SAML_IDP_SSO_URL` |
| `idp_x509_cert` | `str` (PEM) | n/a | `TASKQ_SAML_IDP_X509_CERT` |
| `sp_x509_cert` | `str \| None` | `None` | `TASKQ_SAML_SP_X509_CERT` |
| `sp_private_key` | `str \| None` | `None` | `TASKQ_SAML_SP_PRIVATE_KEY` |
| `session_secret` | `str` | n/a | `TASKQ_SAML_SESSION_SECRET` |
| `session_max_age_seconds` | `int` | `28800` | _(same as OIDC)_ |
| `group_attribute` | `str \| None` | `None` | `TASKQ_SAML_GROUP_ATTRIBUTE` |
| `allowed_groups` | `frozenset[str]` | `frozenset()` | `TASKQ_SAML_ALLOWED_GROUPS` (comma-separated) |
| `allow_cookieless_fallback` | `bool` | `false` | `TASKQ_SAML_ALLOW_COOKIELESS_FALLBACK` |

### Routes

- **`/login`**: builds a SAML `AuthnRequest` and redirects to the IdP SSO URL.
- **`/callback`** (POST, the ACS endpoint): validates the signed SAML
  response, requires the AuthnRequest correlation cookie set by `/login`
  (see [Session handling](#session-handling); a cookie-less callback is
  refused unless `allow_cookieless_fallback` is on), extracts the NameID +
  attributes into `IdentityClaims`, sets the session cookie, and redirects
  to the admin root.
- **`/metadata`** (GET): returns SP metadata XML for IdP configuration.
- **`/logout`** (POST), requires the session-bound CSRF token; clears the
  session cookie (see [Logging out](#logging-out)).

v1 supports SP-initiated flow only (the user hits `/login` first). IdP-initiated
SSO is a non-goal for v1.

### SAML container requirements

`python3-saml` depends on the `xmlsec` Python package, which historically
bound to the system `libxmlsec1` C library at both build and runtime. As
currently pinned, this is no longer the case on common platforms: `xmlsec`
ships prebuilt `manylinux`/`musllinux` wheels (Linux x86_64/aarch64, both
glibc and musl) as well as macOS and Windows wheels, each bundling its
native dependencies internally, confirmed via `ldd` against the installed
extension module, which links only against base glibc (`libc`, `libm`,
`libpthread`, `librt`), nothing `libxmlsec1`/`libxml2`/`libssl`-related.
**No system package installation is required** to install or run
`taskq[saml]` on any of these platforms: a plain `uv add "taskq[saml]"`
(or `pip install`) is sufficient, no Dockerfile changes needed.

The one case that still needs system build dependencies is an **unsupported
platform/architecture with no matching prebuilt wheel** (e.g. a niche or very
new architecture), where the resolver would fall back to building `xmlsec`
from source. If that happens, you'll need:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
        libxml2-dev libxmlsec1-dev libxmlsec1-openssl pkg-config build-essential \
    && rm -rf /var/lib/apt/lists/*
```

Check which case applies to your deployment target by running
`uv add "taskq[saml]"` (or `pip install taskq[saml]`) directly; if it
resolves a wheel (no compilation step in the install output), no system
packages are needed.

---

## Authorization model

### Default: authentication-only (IdP-side assignment)

When no group field is configured (`group_claim=None` for OIDC,
`group_attribute=None` for SAML), any user who completes the login flow is
authorized. **This is the recommended configuration for Entra** regardless of
protocol: enable **"User assignment required"** on the enterprise application
and assign the specific users/groups who should have admin access. Entra then
refuses to issue a token/assertion to anyone not assigned, so the app never sees
a login attempt from an unauthorized user, no group-claim parsing is needed, and
the group-overage edge case never comes up.

### Optional: group/role allowlist

When configured, the auth dependency additionally checks `IdentityClaims.groups`
against `allowed_groups`. Empty intersection = 401 (redirect to `/login` for
browser navigation). Use this for app-side, in-repo-configurable control instead
of (or in addition to) IdP-side assignment, or with IdPs that lack an
app-assignment concept.

**Fail-closed:** if `allowed_groups` is non-empty but group membership cannot be
determined (the claim/attribute is absent and no `group_resolver` is configured),
the login is rejected; no session cookie is issued. The user is never silently
authorized.

---

## Entra ID app-registration walkthrough

### OIDC (app registration)

1. In the Entra portal, go to **App registrations** → **New registration**.
2. Set the **Redirect URI** to `https://admin.example.com/admin/callback`
   (Web platform).
3. Note the **Application (client) ID** and **Directory (tenant) ID**.
4. Under **Certificates & secrets**, create a client secret →
   `TASKQ_OIDC_CLIENT_SECRET`.
5. Set `TASKQ_OIDC_ISSUER` to
   `https://login.microsoftonline.com/{tenant_id}/v2.0`.
6. *(Optional)* Under **Token configuration**, add the `groups` claim to the ID
   token, then set `TASKQ_OIDC_GROUP_CLAIM=groups` and
   `TASKQ_OIDC_ALLOWED_GROUPS` to the allowed group object IDs.
7. *(Optional, overage fallback)* Add **Microsoft Graph** →
   **Group.Read.All** (delegated) permission and set
   `TASKQ_OIDC_SCOPE=openid profile email Group.Read.All` with a
   `group_resolver`.

### SAML (enterprise application)

1. In the Entra portal, go to **Enterprise applications** → **New application**
   → **Create your own application** → "Non-gallery" SAML app.
2. Under **Single sign-on** → **SAML**, set:
   - **Identifier (Entity ID)** = `TASKQ_SAML_ENTITY_ID`
   - **Reply URL (ACS URL)** = `TASKQ_SAML_ACS_URL`
3. Download the IdP **certificate** → `TASKQ_SAML_IDP_X509_CERT` (PEM).
4. Set `TASKQ_SAML_IDP_ENTITY_ID` and `TASKQ_SAML_IDP_SSO_URL` from the
   Entra-provided metadata.
5. Visit `https://admin.example.com/admin/metadata` to fetch SP metadata for
   Entra's "Upload metadata file" option (or enter the values manually).
6. *(Optional)* Under **User attributes & claims**, add a group claim
   (attribute name `http://schemas.microsoft.com/ws/2008/06/identity/claims/groups`)
   → set `TASKQ_SAML_GROUP_ATTRIBUTE` to that name and
   `TASKQ_SAML_ALLOWED_GROUPS` to the allowed group object IDs.
7. Enable **User assignment required** and assign the users/groups who should
   have admin access.

---

## Mounting into an existing FastAPI app

Instead of `taskq ui serve`, embed the admin router and SSO router together:

```python
from contextlib import asynccontextmanager
import asyncpg
from fastapi import FastAPI
from taskq.settings import TaskQSettings
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth import OIDCAuthConfig, create_oidc_auth


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = TaskQSettings.load()
    pool = await asyncpg.create_pool(str(settings.pg_dsn))

    oidc = settings.oidc
    sso_config = OIDCAuthConfig(
        issuer=oidc.issuer,
        client_id=oidc.client_id,
        # Settings secret fields are SecretStr (masked in any repr or error);
        # unwrap at the boundary where the runtime auth config needs the
        # plain string.
        client_secret=oidc.client_secret.get_secret_value(),
        redirect_uri=oidc.redirect_uri,
        session_secret=oidc.session_secret.get_secret_value(),
        group_claim=oidc.group_claim,
        allowed_groups=oidc.allowed_groups_set,
    )
    sso_bundle = create_oidc_auth(sso_config, base_path="/admin")

    bundle = create_router(
        pool,
        schema=settings.schema_name,
        auth_dependency=sso_bundle.dependency,
        base_path="/admin",
    )
    setup_admin_state(app, bundle)
    app.include_router(sso_bundle.router, prefix="/admin")
    app.include_router(bundle.router, prefix="/admin")
    yield
    await pool.close()


app = FastAPI(lifespan=lifespan)
```

The SAML equivalent uses `SAMLAuthConfig` and `create_saml_auth`; the wiring is
identical. SSO sub-configs are separate `DotEnvConfig` classes with their own
`env_prefix` (`TASKQ_OIDC_*`, `TASKQ_SAML_*`), accessed via lazy properties:
`settings.oidc.issuer`, `settings.saml.entity_id`, etc. The env var names are
unchanged from the flat-field layout.

---

## Session handling

Sessions are stateless signed cookies (`itsdangerous.URLSafeTimedSerializer`),
no Redis/DB dependency. The cookie payload stores only `subject`, `email`, and
`groups` (as a sorted list). Cookie flags: `HttpOnly`, `Secure` (configurable),
`SameSite=Lax`.

The SAML backend also sets a second, short-lived cookie (`taskq_saml_request`,
5 minutes) that correlates the ACS callback with the AuthnRequest that started
the login. A hosted IdP POSTs its assertion from a different site, and a
browser withholds a `SameSite=Lax` cookie from a cross-site POST, so this one
is marked `SameSite=None` when `secure_cookie` is on and is scoped to the
`/callback` path alone. The session cookie's policy is untouched.

The OIDC backend sets the analogous short-lived cookie (`taskq_oidc_state`,
5 minutes) carrying the `state`, the PKCE `code_verifier`, and the `nonce` in
one signed record. It is scoped to the callback route the same way, it is
consumed by exactly one route, so it is offered on exactly one.

The correlation cookie is the binding: the callback accepts an assertion only
when its `InResponseTo` matches the request ID the signed cookie carries
(python3-saml enforces the same comparison inside `process_response`), and the
cookie is single-use: cleared on every callback outcome, and its request ID
recorded as answered so a re-supplied captured copy cannot buy a second
assertion on the process that answered it, with a 300 s TTL. Because it is
signed with `session_secret`, **any replica sharing the secret can verify
it**: multi-replica deployments and `uvicorn --workers N` need no sticky
sessions, and a login whose callback lands on a different process than the
one that issued it completes normally.

One caveat survives that: the consumed-assertion replay record (and the
answered-request record) are per process. A party who captured a complete ACS
POST (cookie plus response body) can mint one additional session per
sibling process within the 300 s cookie window, and the records are
capped/evictable under a flood of valid assertions. Over HTTPS that capture
implies a compromised client, a MITM, or a TLS break, each of which already
yields session theft directly; closing it fully needs a replay record in a
store every replica shares, which is tracked separately.

### The cookie-less fallback (opt-in, default off)

A hosted IdP's ACS POST is a genuine cross-site POST, and some browsers
withhold even a `SameSite=None` cookie from it (third-party cookie blocking,
privacy modes). On a default deployment such a callback is refused, so the user
sees the standard login error and can retry from the same browser, which
re-issues a fresh correlation cookie.

Deployments that must serve cookie-blocking browsers can opt in:

```sh
export TASKQ_SAML_ALLOW_COOKIELESS_FALLBACK=true
```

The callback then also accepts an assertion with no usable cookie when, after
full signature and timestamp validation, its `InResponseTo` names an
AuthnRequest the receiving process issued in the last 5 minutes and has not
yet spent.

**The tradeoff, stated directly:** nothing ties that response to the browser
posting it. A party who starts a login, authenticates at the IdP as
themselves, and captures the signed response without posting it to the ACS
can have a cookie-less victim's browser POST it within the 5-minute window;
the victim receives a session cookie for that party's NameID. The replay
cache only blocks the *second* presentation of the same assertion. If your
threat model cannot carry that, leave the flag off.

Two operational consequences of opting in:

- The pending-request record is **per process**. A cookie-less callback must
  land on the same process that issued the login: put the SSO routes behind
  sticky sessions, or expect an occasional retry when the browser withholds
  the cookie *and* the callback lands on a different replica than the login.
  This is the only configuration that needs sticky sessions: the cookie path
  above works on any replica.
- The record is capped (10,000 entries, soonest-to-expire eviction) and
  `/login` is unauthenticated, so a flood of login starts can evict a real
  pending ID and force that one cookie-less login to retry. The cookie path
  is immune to eviction pressure.

### Why OIDC does not have this fallback

The OIDC callback is a top-level GET redirect, which browsers allow
`SameSite=Lax` cookies on: the state cookie (carrying `state`, the PKCE
`code_verifier`, and the `nonce` in one signed cookie) arrives where the SAML
correlation cookie does not, because the ACS POST is not a safe-method
navigation. PKCE also makes a cookie-less OIDC callback structurally unable
to complete: without the `code_verifier` from the cookie there is no token
exchange at all, and the nonce binds the ID token to the login that started
it. OIDC therefore took the stateless-no-fallback tradeoff: a browser that
loses the state cookie cannot log in, and has neither the
cross-replica dependency nor the cookie-less acceptance shape.

The auth dependency re-checks the group allowlist on every request, so changing
`allowed_groups` takes effect immediately for existing sessions (a user whose
group no longer intersects the allowlist gets 401 on the next request). Rotating
`session_secret` invalidates all sessions at once.

**Long-lived SSE streams** are the one place a per-request check is not
enough, so both streaming endpoints (the per-job progress stream and the
admin `/sse/{topic}` channel) re-run the same session verification while the
stream is open: before every streamed event and at every keepalive tick. A
session invalidated mid-stream -- a rotated `session_secret`, a session that
ages past `max_age_seconds`, or an `allowed_groups` change that excludes the
identity -- ends the live stream within one keepalive interval (at most 60
seconds, whatever `sse_heartbeat_interval` is set to); the browser's
`EventSource` reconnects, is refused with 401, and the user logs in again.
This re-check reads the same signed cookie the per-request check reads, so it
covers exactly what that check covers; a stateless cookie session has no
server-side revocation list to consult. Streams built with taskq's own
`create_auth_dependency` or `token_auth` get the re-check automatically; a
host supplying its own `auth_dependency` can pass a `session_verifier`
(`Callable[[Request], Awaitable[bool]]`) to `create_router` and is warned at
startup when it does not.

---

### Logging out

Logout is a POST, not a GET. Both backends refuse `GET /logout` (405), so a
forced top-level navigation, a link, an image, a redirect from any page ,
cannot clear an admin session, and the POST must carry a CSRF token derived
from the live session cookie (an HMAC of the cookie value under
`session_secret`). The admin UI's **Sign out** control is a small POST form
that the server renders with that token in a hidden field whenever an SSO
session is live, and the token ends where the session ends: a fresh login
mints a fresh value, and a logged-out or expired session has no valid token
at all. A cross-site attacker has neither the HttpOnly session cookie the
token is derived from nor `session_secret`, so a forged form POST cannot
carry a valid token either; the `SameSite=Lax` session cookie would not ride
a cross-site POST in the first place.

Integrations that logged out by fetching `GET /admin/logout` must switch to
an authenticated `POST /admin/logout` with the token. There is no endpoint
that ends a session without one.

## Machine-token auth (`token_auth`)

For endpoints that serve machine-to-machine traffic (Prometheus scrapers,
kubelet probes, CI scripts) an interactive OIDC/SAML redirect isn't practical.
`token_auth` provides a lightweight bearer-token dependency with no extra
dependencies:

```python
from taskq.web.admin.auth import token_auth

health_dependency = token_auth("your-secret-token")

# Use as auth_dependency on specific routes or the whole router:
app.include_router(
    create_router(pool, auth_dependency=health_dependency, base_path="/admin"),
    prefix="/admin",
)
```

The dependency uses `hmac.compare_digest` for timing-safe comparison and
raises `HTTPException(401)` on missing or mismatched tokens. Passing an empty
string to `token_auth()` raises `ValueError`; an empty token is never
accepted.

When using `taskq ui serve`, set `TASKQ_HEALTH_TOKEN` instead of wiring
`token_auth` manually; the CLI applies it to health and metrics routes
automatically (see [admin-ui.md](admin-ui.md#protecting-health-endpoints-with-a-bearer-token)).
