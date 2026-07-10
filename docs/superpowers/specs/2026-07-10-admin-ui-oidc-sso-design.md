# Admin UI SSO Support (OIDC + SAML) — Design

**Date:** 2026-07-10
**Status:** Draft, pending review

## 1. Goal

Ship an optional, out-of-the-box way to protect the TaskQ admin UI with SSO
— primarily targeting Microsoft Entra ID via OIDC, but with a pluggable
SAML backend behind the same abstraction — without changing anything about
the existing unauthenticated/BYO-auth default.

## 2. Non-goals

- **Changing the default posture.** `auth_dependency=None` remains the
  default for both standalone `taskq ui serve` and the mounted-router case.
  This feature is purely additive — an alternative value to pass for
  `auth_dependency`, not a required migration.
- **Token/session refresh beyond re-login.** Session cookies expire and the
  user re-authenticates through the normal login flow; no refresh-token or
  IdP-session persistence in v1 (OIDC or SAML).
- **IdP-initiated SAML SSO.** v1 supports SP-initiated flow only (user hits
  `/login` first) for both backends — no unsolicited-assertion handling.
  Straightforward to add later if a client's IdP requires it.

## 3. Architecture: shared abstraction, two backends

Both SSO protocols reduce to the same shape for TaskQ's purposes: redirect
to an IdP, come back with a signed credential, extract an identity + group
membership, establish a session, gate `auth_dependency` on that session.
That shape is factored into one shared contract so switching backends is a
config choice, not a code change, and so both get the same session
handling, CSRF reuse, and group/role-allowlist logic for free.

```
src/taskq/web/admin/auth/
    __init__.py     # re-exports: AuthBundle, IdentityClaims,
                    #   create_oidc_auth, create_saml_auth
    _session.py     # shared: signed-cookie session (itsdangerous),
                    #   group/role allowlist check — protocol-agnostic
    oidc.py         # create_oidc_auth(config: OIDCAuthConfig) -> AuthBundle
    saml.py         # create_saml_auth(config: SAMLAuthConfig) -> AuthBundle
```

Structuring this as a package with `oidc.py` as the first implementation
and `__init__.py` re-exporting the public surface means `saml.py` slots in
later (§3.2) without restructuring anything — the package shape already
accommodates it, which is what makes "SAML as a future extra" a credible
commitment rather than a hand-wave.

```python
@dataclass(frozen=True)
class AuthBundle:
    router: APIRouter               # login/callback/logout (+ SAML metadata,
                                     # see §3.3) — mount at the admin
                                     # router's base_path
    dependency: Callable[..., Any]  # pass to create_router(auth_dependency=...)


@dataclass(frozen=True)
class IdentityClaims:
    """Normalized identity, regardless of which protocol produced it."""
    subject: str
    email: str | None
    groups: frozenset[str]          # empty if the backend has no group
                                     # data or none was requested
    raw: Mapping[str, Any]          # original ID token claims / SAML
                                     # attribute statement, for custom checks
```

Both `create_oidc_auth()` and `create_saml_auth()` return an `AuthBundle`.
Wiring at the call site is backend-agnostic:

```python
bundle = create_oidc_auth(oidc_config) if settings.sso_backend == "oidc" \
    else create_saml_auth(saml_config)

app.include_router(bundle.router, prefix="/admin")
admin = create_router(pg_pool, auth_dependency=bundle.dependency, base_path="/admin")
```

`_session.py` owns: signed-cookie issuance/verification (itsdangerous
`URLSafeTimedSerializer`), the group/role-allowlist check against
`IdentityClaims.groups`, and the shared `HTTPException(401)` /
redirect-to-`/login` behavior — both backends call into this rather than
reimplementing it, so session security and the authorization model (§5) are
identical no matter which protocol is in play.

### 3.1 OIDC backend

New optional extra `taskq[oidc]`:

```toml
oidc = [
    "authlib>=1.3.0",      # OAuth2/OIDC client, discovery, token validation
    "itsdangerous>=2.2.0", # signed session cookies (shared with SAML backend)
]
```

Vendor-neutral: works against any OIDC-compliant provider (Entra, Okta,
Auth0, Google) — no Microsoft-specific SDK (no `msal`), consistent with
TaskQ's existing "vendor-neutral Protocol, no vendor SDK" pattern
(`ErrorReporter`, OTel).

`OIDCAuthConfig` (Pydantic model, `DotEnvConfig` pattern):

| Field | Type | Default | Notes |
|---|---|---|---|
| `issuer` | `str` | — | OIDC discovery issuer, e.g. `https://login.microsoftonline.com/{tenant_id}/v2.0` |
| `client_id` | `str` | — | |
| `client_secret` | `str` | — | |
| `redirect_uri` | `str` | — | Must match the app registration's configured redirect URI |
| `session_secret` | `str` | — | Signing key for the session cookie; rotate to invalidate all sessions |
| `session_max_age_seconds` | `int` | `28800` (8h) | |
| `group_claim` | `str \| None` | `None` | ID token claim to read into `IdentityClaims.groups` (e.g. `"groups"`, `"roles"`). `None` = authorization is authentication-only (see §5). |
| `allowed_groups` | `frozenset[str]` | `frozenset()` | Allowlist checked against `group_claim`'s value when set. |
| `group_resolver` | `Callable[[OIDCTokenContext], Awaitable[frozenset[str]]] \| None` | `None` | Optional fallback to resolve group membership out-of-band (e.g. Microsoft Graph `/me/memberOf`) when the ID token can't carry the full claim (see §5.2). |

```python
@dataclass(frozen=True)
class OIDCTokenContext:
    """Passed to group_resolver — the ID token claims alone aren't enough
    for the Entra Graph-API overage fallback, which needs the access token
    to call /me/memberOf."""
    id_token_claims: dict[str, object]
    access_token: str | None
```

Login flow: `/login` redirects to the IdP's authorization endpoint with a
`state` + PKCE `code_verifier` (stored in a short-lived, separate signed
cookie, not the session cookie) → `/callback` validates `state`, exchanges
the code, validates the ID token (issuer, audience, signature via authlib's
JWKS handling), calls `group_resolver(OIDCTokenContext(...))` if configured
— the full token response (including the access token) is in scope at this
point in `/callback` — populates `IdentityClaims`, hands off to
`_session.py` to set the session cookie.

**IdP failure handling:** token exchange failure, JWKS fetch timeout, or an
unreachable IdP during `/callback` redirect to a generic error
(`?error=authentication+failed`), never raw exception text in the URL —
same pattern as the existing cron `payload_factory` error-reflection fix.
The full exception is logged server-side (`exc_info=True`), not shown to
the browser.

### 3.2 SAML backend

New optional extra `taskq[saml]`:

```toml
saml = [
    "python3-saml>=1.16.0", # OneLogin SP toolkit; requires system xmlsec1
                            # libraries — see §3.3 for container requirements
    "itsdangerous>=2.2.0",  # shared session cookie module
]
```

`SAMLAuthConfig` (Pydantic model):

| Field | Type | Default | Notes |
|---|---|---|---|
| `entity_id` | `str` | — | SP entity ID |
| `acs_url` | `str` | — | Assertion Consumer Service URL (the `/callback` route) |
| `idp_entity_id` | `str` | — | |
| `idp_sso_url` | `str` | — | IdP's SSO redirect/POST endpoint |
| `idp_x509_cert` | `str` | — | IdP's signing certificate (PEM), for assertion signature validation |
| `sp_x509_cert` / `sp_private_key` | `str \| None` | `None` | Only needed if signing SP requests or requiring encrypted assertions |
| `session_secret` / `session_max_age_seconds` | — | same as OIDC | shared `_session.py` fields |
| `group_attribute` | `str \| None` | `None` | SAML attribute-statement name to read into `IdentityClaims.groups` (Entra's default group-claim attribute is `http://schemas.microsoft.com/ws/2008/06/identity/claims/groups`) |
| `allowed_groups` | `frozenset[str]` | `frozenset()` | Same semantics as OIDC's |

Login flow: `/login` builds and redirects to a SAML `AuthnRequest`.
`/callback` (the ACS endpoint) validates the signed `Response`/assertion via
`python3-saml`, extracts `NameID` + attribute statements into
`IdentityClaims`, hands off to the same `_session.py` session-issuance path
used by OIDC. A `/metadata` route is also exposed (SP metadata XML), since
most SAML IdPs (including Entra's SAML gallery/non-gallery app setup) want
to consume it during IdP-side app configuration.

### 3.3 SAML container/runtime requirements

**Correction (post-implementation):** this section originally assumed
`python3-saml`/`xmlsec` required system `libxmlsec1` at build and runtime,
based on that package's older behavior. Verified against the actual pinned
version during implementation — `xmlsec` ships prebuilt `manylinux`/
`musllinux` wheels (Linux x86_64/aarch64, glibc and musl) plus macOS/Windows
wheels, each bundling native dependencies internally. Confirmed via `ldd`
against the installed extension module: it links only against base glibc
(`libc`, `libm`, `libpthread`, `librt`) — nothing `libxmlsec1`/`libxml2`/
`libssl`-related. **No system package installation is required** on any of
these platforms; `uv add "taskq[saml]"` is sufficient. System build
dependencies (`libxml2-dev`, `libxmlsec1-dev`, `libxmlsec1-openssl`,
`pkg-config`, a C compiler) are only needed if no prebuilt wheel exists for
an unusual target platform/architecture, in which case the resolver falls
back to building from source. See `docs/guides/sso.md`'s "SAML container
requirements" section for the current, verified guidance and Dockerfile
snippet for that fallback case.

Choosing `taskq[oidc]` alone still requires none of this at all — a
deployment that doesn't need SAML has zero native-dependency burden either
way — but the SAML side turned out to be far lighter in practice than
initially assumed here. This changes the OIDC-vs-SAML tradeoff discussion:
the deciding factor for new integrations is no longer "SAML means container
changes" (it usually doesn't) but simply IdP/compliance requirements and
OIDC's overall simpler setup (fewer moving parts, no certificate exchange).

## 4. Session handling (shared, `_session.py`)

Stateless, signed cookie (itsdangerous) — no Redis/DB dependency for
session storage, consistent with TaskQ treating Redis as optional
everywhere else. Cookie payload: `subject`, `email`, and `groups` from
`IdentityClaims` (nothing else — no raw tokens/assertions, no PII beyond
what's needed for the allowlist check). Cookie flags: `httponly`, `secure`
(configurable off only for local http dev), `samesite="lax"`.

**Operational note for docs (not a code change):** `session_secret` should
be at least 32 bytes of random data. itsdangerous rejects any cookie signed
with a different key than the one currently configured, so rotating
`session_secret` is a clean, complete way to invalidate every outstanding
session at once (e.g. after a suspected compromise) — no session store to
flush, just change the key.

The dependency built into `AuthBundle.dependency` reads the session cookie,
verifies signature/expiry, re-checks the group allowlist (in case
`allowed_groups` changed since the cookie was issued), and raises
`HTTPException(401)` (redirecting to `/login` for browser navigations) if
invalid. Identical for both backends — it operates on `IdentityClaims` and
the session cookie, not on OIDC/SAML specifics.

CSRF: the existing `_CsrfRoute`/`validate_csrf` machinery already used by
admin write-routes is reused for `/callback` and `/logout`; `/login` and
`/metadata` don't need it (no state-changing side effect).

## 5. Authorization model (shared, protocol-agnostic)

### 5.1 Default: authentication-only (IdP-side assignment as the real gate)

When no group/role field is configured (`group_claim=None` for OIDC,
`group_attribute=None` for SAML), any user who completes the login flow
successfully is authorized. This is the recommended configuration for
Entra regardless of protocol: turn on **"User assignment required"** on the
enterprise application and assign the specific users/groups who should have
admin access. Entra then refuses to issue a token/assertion to anyone not
assigned — the app never sees a login attempt from an unauthorized user,
no group-claim parsing is needed, and the group-overage edge case (§5.2)
never comes up. This is Microsoft's own recommended pattern for "gate
access to this specific app" as opposed to "check org-wide group
membership," and it applies the same way whether the app is registered as
an OIDC or SAML app in Entra.

### 5.2 Optional: group/role allowlist

When configured, the dependency additionally checks `IdentityClaims.groups`
against `allowed_groups`. Exists for: teams that want app-side,
in-repo-configurable control instead of (or in addition to) IdP-side
assignment; IdPs without an equivalent app-assignment concept; or role-based
gating (e.g. `admin` vs. `viewer`, reserved for a future read-only admin
mode — out of scope here but the check is generic enough to support it
later without a redesign).

**Entra group-overage caveat (OIDC-specific):** once a user belongs to more
than ~200 groups, Entra omits the `groups` claim from the ID token entirely
and emits a `_claim_names`/`hasgroups` marker instead, requiring a
Microsoft Graph `/me/memberOf` call to resolve membership. Handled via the
optional `group_resolver` callable on `OIDCAuthConfig` — invoked when the
expected claim is absent/overage-marked, result used in place of the token
claim. A reference implementation using `httpx` against Graph ships as a
documented example, not a hard dependency of `taskq[oidc]`. SAML assertions
don't have this limitation (Entra's SAML group attribute can be configured
to emit group *object IDs* without the same 200-group ceiling, or filtered
server-side via Entra's group-claim configuration), so no equivalent
resolver is needed for the SAML backend.

## 6. Standalone vs. mounted wiring

- **Standalone (`taskq ui serve`):** the CLI's app-factory reads
  `TASKQ_SSO_BACKEND` (`none` / `oidc` / `saml`, default `none`) plus the
  matching `TASKQ_OIDC_*` / `TASKQ_SAML_*` settings, builds the
  corresponding `AuthBundle`, and mounts both the auth router and the admin
  router (with `auth_dependency=bundle.dependency`) at the same
  `base_path`. `TASKQ_SSO_BACKEND=none` is the default and preserves
  today's unauthenticated/BYO-auth behavior unchanged.
- **Mounted into an existing app:** the embedding app calls
  `create_oidc_auth(config)` or `create_saml_auth(config)` itself, includes
  `bundle.router` alongside the admin router from
  `create_router(..., auth_dependency=bundle.dependency)` — same pattern
  already documented for custom `auth_dependency` today, just with a
  ready-made implementation instead of hand-rolling one.
- **Interaction with `admin_ui_require_auth` (fail-closed default):**
  `admin_ui_require_auth=True` raises `RuntimeError` in non-dev
  environments when `auth_dependency is None`. When `TASKQ_SSO_BACKEND` is
  `oidc` or `saml`, the CLI passes `bundle.dependency` (non-`None`) as
  `auth_dependency`, so this check passes without any special-casing — the
  OIDC/SAML dependency satisfies `admin_ui_require_auth` the same way a
  hand-rolled one would. No `RuntimeError` is raised when SSO is enabled.

## 7. Docs

New `docs/guides/sso.md`, covering both backends under one guide (shared
concepts: assignment-first authorization, session cookie behavior) with
protocol-specific subsections:
- Entra app-registration walkthrough for **both** OIDC and SAML app types:
  redirect URI / ACS URL, client secret / IdP certificate, enabling **User
  assignment required**, optional App Roles / group-claim configuration,
  optional `Group.Read.All` Graph permission for the OIDC overage
  `group_resolver`.
- Config reference for `OIDCAuthConfig` / `SAMLAuthConfig` and their
  `TASKQ_OIDC_*` / `TASKQ_SAML_*` env vars.
- SAML container-requirements callout (§3.3) with the Dockerfile snippet,
  positioned prominently so it's not discovered via a build failure.
- Explicit "which one should I use" guidance: OIDC by default (simpler,
  zero native deps); SAML only if a specific IdP or compliance requirement
  mandates it.
- Explicitly restates that `auth_dependency=None` / `TASKQ_SSO_BACKEND=none`
  (unauthenticated, BYO-auth via reverse proxy) remains fully supported and
  unchanged — this feature doesn't obsolete that path.
- Note in `docs/guides/admin-ui.md`'s security section pointing to the new
  guide.

## 8. Testing plan

**Shared (`_session.py`):** cookie issuance/verification round trip,
expired/tampered cookie rejected, group-allowlist check against
`IdentityClaims` (protocol-agnostic — tested once, not duplicated per
backend).

**OIDC**, against a mocked OIDC provider (fake discovery doc + JWKS + token
endpoint via `respx`/`pytest-httpx` — no real Entra dependency in CI):
- Full login → callback → session → authorized request round trip.
- `state`/PKCE mismatch rejected.
- `group_claim` set: user's claim value in `allowed_groups` → pass; not in
  it → 401.
- Group-overage marker present, `group_resolver` invoked, result used.
- No `group_resolver` configured + overage marker + group check configured
  → fails closed (401), not silently authorized.
- Default config (`group_claim=None`) → any authenticated user passes.

**SAML**, against a fixture IdP (test signing cert/key pair, hand-built
signed `Response` XML fixtures — no real Entra dependency in CI):
- Full login → ACS callback → session → authorized request round trip.
- Unsigned / tampered assertion rejected.
- `/metadata` returns valid SP metadata XML.
- `group_attribute` set: attribute value in `allowed_groups` → pass; not in
  it → 401.
- Default config (`group_attribute=None`) → any authenticated user passes.

**Both:** logout clears the session cookie; switching `TASKQ_SSO_BACKEND`
requires no code change, only config (smoke-tested via the CLI app-factory
test).

## 9. Open questions for implementation-planning stage

None outstanding — proceed to `writing-plans`. Implementation should
sequence the shared `_session.py` + OIDC backend first (simpler, no native
deps, unblocks the Entra/OIDC use case immediately), then the SAML backend
as a follow-on slice once the shared contract is proven out.
