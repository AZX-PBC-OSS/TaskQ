# SSO / SAML

The TaskQ admin UI supports optional single sign-on (SSO) via **OIDC** (primary,
for Microsoft Entra ID and any OIDC-compliant provider) and **SAML** (for legacy
IdPs), behind a shared abstraction. Both are purely additive — the default
(`TASKQ_SSO_BACKEND=none`) remains the unauthenticated / bring-your-own-auth
behavior described in [admin-ui.md](admin-ui.md).

---

## Which one should I use?

| | OIDC (`taskq[oidc]`) | SAML (`taskq[saml]`) |
|---|---|---|
| **Native dependencies** | None (pure Python) | None on common platforms — see [SAML container requirements](#saml-container-requirements) |
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
| `issuer` | `str` | — | `TASKQ_OIDC_ISSUER` |
| `client_id` | `str` | — | `TASKQ_OIDC_CLIENT_ID` |
| `client_secret` | `str` | — | `TASKQ_OIDC_CLIENT_SECRET` |
| `redirect_uri` | `str` | — | `TASKQ_OIDC_REDIRECT_URI` |
| `session_secret` | `str` | — | `TASKQ_OIDC_SESSION_SECRET` |
| `session_max_age_seconds` | `int` | `28800` (8h) | `TASKQ_OIDC_SESSION_MAX_AGE_SECONDS` |
| `scope` | `str` | `openid profile email` | `TASKQ_OIDC_SCOPE` |
| `group_claim` | `str \| None` | `None` | `TASKQ_OIDC_GROUP_CLAIM` |
| `allowed_groups` | `frozenset[str]` | `frozenset()` | `TASKQ_OIDC_ALLOWED_GROUPS` (comma-separated) |
| `group_resolver` | `Callable \| None` | `None` | _(programmatic only)_ |

`session_secret` should be at least 32 bytes of random data. Rotating it
invalidates every outstanding session at once — no session store to flush.

### Login flow

1. **`/login`** — generates a PKCE `code_verifier` + `state`, stores both in a
   short-lived signed cookie (separate from the session cookie), and redirects
   to the IdP authorization endpoint.
2. **`/callback`** — validates `state`, exchanges the code for tokens,
   validates the ID token (issuer, audience, signature via JWKS), extracts
   claims into `IdentityClaims`, sets the session cookie, and redirects to the
   admin UI root.
3. **`/logout`** — clears the session cookie and redirects to the admin root.

On any error during `/callback` (token exchange failure, JWKS fetch timeout,
invalid ID token), the user is redirected with a generic
`?error=authentication+failed` — **never** raw exception text. The full
exception is logged server-side.

### Group overage (Entra-specific)

Once a user belongs to more than ~200 groups, Entra omits the `groups` claim
from the ID token and emits a `_claim_names`/`hasgroups` marker instead. The
optional `group_resolver` callable handles this: it receives an
`OIDCTokenContext` (ID token claims + access token) and returns a
`frozenset[str]` of groups, typically by calling Microsoft Graph
`/me/memberOf`. To use it, add `Group.Read.All` to `TASKQ_OIDC_SCOPE`.

A reference Graph-API resolver (using `httpx2`) ships as a documented example —
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
| `entity_id` | `str` | — | `TASKQ_SAML_ENTITY_ID` |
| `acs_url` | `str` | — | `TASKQ_SAML_ACS_URL` |
| `idp_entity_id` | `str` | — | `TASKQ_SAML_IDP_ENTITY_ID` |
| `idp_sso_url` | `str` | — | `TASKQ_SAML_IDP_SSO_URL` |
| `idp_x509_cert` | `str` (PEM) | — | `TASKQ_SAML_IDP_X509_CERT` |
| `sp_x509_cert` | `str \| None` | `None` | `TASKQ_SAML_SP_X509_CERT` |
| `sp_private_key` | `str \| None` | `None` | `TASKQ_SAML_SP_PRIVATE_KEY` |
| `session_secret` | `str` | — | `TASKQ_SAML_SESSION_SECRET` |
| `session_max_age_seconds` | `int` | `28800` | _(same as OIDC)_ |
| `group_attribute` | `str \| None` | `None` | `TASKQ_SAML_GROUP_ATTRIBUTE` |
| `allowed_groups` | `frozenset[str]` | `frozenset()` | `TASKQ_SAML_ALLOWED_GROUPS` (comma-separated) |

### Routes

- **`/login`** — builds a SAML `AuthnRequest` and redirects to the IdP SSO URL.
- **`/callback`** (POST, the ACS endpoint) — validates the signed SAML
  response, extracts the NameID + attributes into `IdentityClaims`, sets the
  session cookie, and redirects to the admin root.
- **`/metadata`** (GET) — returns SP metadata XML for IdP configuration.
- **`/logout`** — clears the session cookie.

v1 supports SP-initiated flow only (the user hits `/login` first). IdP-initiated
SSO is a non-goal for v1.

### SAML container requirements

`python3-saml` depends on the `xmlsec` Python package, which historically
bound to the system `libxmlsec1` C library at both build and runtime. As
currently pinned, this is no longer the case on common platforms: `xmlsec`
ships prebuilt `manylinux`/`musllinux` wheels (Linux x86_64/aarch64, both
glibc and musl) as well as macOS and Windows wheels, each bundling its
native dependencies internally — confirmed via `ldd` against the installed
extension module, which links only against base glibc (`libc`, `libm`,
`libpthread`, `librt`), nothing `libxmlsec1`/`libxml2`/`libssl`-related.
**No system package installation is required** to install or run
`taskq[saml]` on any of these platforms — a plain `uv add "taskq[saml]"`
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
`uv add "taskq[saml]"` (or `pip install taskq[saml]`) directly — if it
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
refuses to issue a token/assertion to anyone not assigned — the app never sees
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
the login is rejected — no session cookie is issued. The user is never silently
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
        client_secret=oidc.client_secret,
        redirect_uri=oidc.redirect_uri,
        session_secret=oidc.session_secret,
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

The SAML equivalent uses `SAMLAuthConfig` and `create_saml_auth` — the wiring is
identical. SSO sub-configs are separate `DotEnvConfig` classes with their own
`env_prefix` (`TASKQ_OIDC_*`, `TASKQ_SAML_*`), accessed via lazy properties:
`settings.oidc.issuer`, `settings.saml.entity_id`, etc. The env var names are
unchanged from the flat-field layout.

---

## Session handling

Sessions are stateless signed cookies (`itsdangerous.URLSafeTimedSerializer`) —
no Redis/DB dependency. The cookie payload stores only `subject`, `email`, and
`groups` (as a sorted list). Cookie flags: `HttpOnly`, `Secure` (configurable),
`SameSite=Lax`.

The auth dependency re-checks the group allowlist on every request, so changing
`allowed_groups` takes effect immediately for existing sessions (a user whose
group no longer intersects the allowlist gets 401 on the next request). Rotating
`session_secret` invalidates all sessions at once.

---

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
string to `token_auth()` raises `ValueError` — an empty token is never
accepted.

When using `taskq ui serve`, set `TASKQ_HEALTH_TOKEN` instead of wiring
`token_auth` manually — the CLI applies it to health and metrics routes
automatically (see [admin-ui.md](admin-ui.md#protecting-health-endpoints-with-a-bearer-token)).
