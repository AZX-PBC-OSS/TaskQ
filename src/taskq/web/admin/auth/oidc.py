"""OIDC SSO backend (vendor-neutral; primary target: Microsoft Entra ID).

Uses :mod:`authlib` for the OAuth2 authorization-code + PKCE flow and
:mod:`joserfc` (bundled with authlib) for ID-token signature/claims validation.
``authlib`` is imported lazily inside :func:`create_oidc_auth` so importing
:mod:`taskq.web.admin.auth` never crashes when the ``[oidc]`` extra is absent.
"""

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from taskq.web.admin.auth._session import (
    AuthBundle,
    IdentityClaims,
    SessionManager,
    create_auth_dependency,
    require_logout_csrf,
    warn_if_no_group_allowlist,
)

__all__ = [
    "OIDCAuthConfig",
    "OIDCTokenContext",
    "create_oidc_auth",
]

logger = structlog.get_logger("taskq.web.admin.auth.oidc")

_STATE_COOKIE_NAME: str = "taskq_oidc_state"
_STATE_MAX_AGE: int = 300
_DISCOVERY_TTL_SECONDS: int = 300
_DISCOVERY_CACHE_MAX_ENTRIES: int = 16


@dataclass
class _IssuerMetadata:
    """One issuer's cached discovery document, plus its JWKS once fetched.

    ``jwks`` starts as ``None``: ``/login`` needs only the discovery document,
    so a login that never reaches the callback leaves the key set unfetched
    rather than paying for it speculatively.
    """

    discovery: dict[str, Any]
    jwks: dict[str, Any] | None
    expires_at: float


class _OIDCMetadataCache:
    """Process-local TTL cache of discovery documents and JWKS, keyed per issuer.

    Every unauthenticated ``/login`` used to perform a live outbound fetch,
    and every ``/callback`` a discovery fetch plus a JWKS fetch, so a flood
    of login attempts was a flood of outbound requests to the IdP. One cache
    entry per issuer bounds that to one fetch per TTL window: the second
    ``/login`` and the ``/callback`` after it read from memory.

    Bounded like every store in the SSO layer (entries evict
    soonest-to-expire), and invalidated on fetch failure: a refresh that
    raises evicts the issuer's entry, so the next request starts from a
    fresh fetch instead of serving a document the IdP would not refresh.
    Within the TTL window a rotated signing key is picked up on expiry;
    the window is one TTL wide, not one login wide.

    Process-local by design: a sibling process or replica keeps its own
    cache, which only changes how many outbound fetches a deployment makes,
    never what a callback accepts -- signature validation runs on every ID
    token regardless of where its JWKS came from.
    """

    def __init__(
        self,
        *,
        ttl_seconds: int = _DISCOVERY_TTL_SECONDS,
        max_entries: int = _DISCOVERY_CACHE_MAX_ENTRIES,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: dict[str, _IssuerMetadata] = {}

    async def get_discovery(
        self, issuer: str, fetch: Callable[[], Awaitable[dict[str, Any]]]
    ) -> dict[str, Any]:
        entry = self._fresh_entry(issuer)
        if entry is not None:
            return entry.discovery
        try:
            discovery = await fetch()
        except Exception:
            self._entries.pop(issuer, None)
            raise
        self._store(
            issuer,
            _IssuerMetadata(
                discovery=discovery, jwks=None, expires_at=time.monotonic() + self._ttl_seconds
            ),
        )
        return discovery

    async def get_jwks(
        self, issuer: str, fetch: Callable[[], Awaitable[dict[str, Any]]]
    ) -> dict[str, Any]:
        entry = self._fresh_entry(issuer)
        if entry is not None and entry.jwks is not None:
            return entry.jwks
        try:
            jwks = await fetch()
        except Exception:
            self._entries.pop(issuer, None)
            raise
        if entry is not None:
            # Fresh discovery, JWKS not yet fetched this window: fill it in
            # without resetting the entry's expiry.
            entry.jwks = jwks
        return jwks

    def _fresh_entry(self, issuer: str) -> _IssuerMetadata | None:
        entry = self._entries.get(issuer)
        if entry is None:
            return None
        if entry.expires_at <= time.monotonic():
            del self._entries[issuer]
            return None
        return entry

    def _store(self, issuer: str, entry: _IssuerMetadata) -> None:
        if len(self._entries) >= self._max_entries:
            soonest = min(self._entries, key=lambda key: self._entries[key].expires_at)
            del self._entries[soonest]
        self._entries[issuer] = entry


@dataclass(frozen=True)
class OIDCTokenContext:
    """Passed to ``group_resolver`` — the ID token claims alone are not enough
    for the Entra Graph-API overage fallback, which needs the access token to
    call ``/me/memberOf``.
    """

    id_token_claims: dict[str, object]
    access_token: str | None


class OIDCAuthConfig(BaseModel):
    """Configuration for the OIDC SSO backend."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    issuer: str = Field(
        description="OIDC discovery issuer URL, e.g. "
        "https://login.microsoftonline.com/{tenant}/v2.0",
    )
    client_id: str = Field(description="OAuth2 client ID registered at the IdP.")
    client_secret: str = Field(description="OAuth2 client secret.")
    redirect_uri: str = Field(
        description="Must match the app registration's configured redirect URI.",
    )
    session_secret: str = Field(
        description="Signing key for session cookies; rotate to invalidate all sessions.",
    )
    session_max_age_seconds: int = Field(default=28800, description="Session lifetime (s).")
    secure_cookie: bool = Field(
        default=True,
        description="Set False only for local http dev.",
    )
    scope: str = Field(
        default="openid profile email",
        description="OIDC scopes requested. Add 'Group.Read.All' for the Entra "
        "overage group_resolver (Graph API /me/memberOf).",
    )
    group_claim: str | None = Field(
        default=None,
        description="ID token claim name for groups (e.g. 'groups', 'roles'). "
        "None = authentication-only authorization.",
    )
    allowed_groups: frozenset[str] = Field(
        default_factory=frozenset,
        description="Allowlist checked against the group claim when set.",
    )
    group_resolver: Callable[[OIDCTokenContext], Awaitable[frozenset[str]]] | None = Field(
        default=None,
        description="Optional fallback to resolve group membership out-of-band "
        "(e.g. Graph /me/memberOf) when the ID token cannot carry the claim.",
    )


def _state_serializer(secret: str) -> Any:
    from itsdangerous import URLSafeTimedSerializer

    return URLSafeTimedSerializer(secret, salt="taskq-oidc-state")


def _issue_state_cookie(
    response: Response,
    secret: str,
    state: str,
    code_verifier: str,
    nonce: str,
    *,
    secure: bool,
    path: str,
) -> None:
    response.set_cookie(
        _STATE_COOKIE_NAME,
        str(_state_serializer(secret).dumps({"state": state, "cv": code_verifier, "nonce": nonce})),
        max_age=_STATE_MAX_AGE,
        httponly=True,
        secure=secure,
        samesite="lax",
        path=path,
    )


def _read_state_cookie(cookie: str, secret: str) -> dict[str, str] | None:
    from itsdangerous import BadSignature, SignatureExpired

    try:
        payload = _state_serializer(secret).loads(cookie, max_age=_STATE_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    state = payload.get("state")
    cv = payload.get("cv")
    nonce = payload.get("nonce")
    if not isinstance(state, str) or not isinstance(cv, str):
        return None
    # The nonce must be a non-empty string: an absent or empty one leaves the
    # ID token unbound to this browser, which is the one thing the callback
    # must never accept.
    if not isinstance(nonce, str) or not nonce:
        return None
    return {"state": state, "cv": cv, "nonce": nonce}


def _clear_state_cookie(response: Response, secure: bool, path: str) -> None:
    # The same scoped Path the issue set: a delete on a different path clears
    # nothing, so the cookie would keep riding every callback request.
    response.delete_cookie(
        _STATE_COOKIE_NAME, httponly=True, secure=secure, samesite="lax", path=path
    )


def _error_redirect(base_path: str) -> RedirectResponse:
    return RedirectResponse(url=f"{base_path}?error=authentication+failed", status_code=302)


def _extract_groups(claim_value: object) -> frozenset[str]:
    if isinstance(claim_value, str):
        return frozenset({claim_value})
    if isinstance(claim_value, list):
        return frozenset(str(g) for g in claim_value)
    return frozenset()


def create_oidc_auth(config: OIDCAuthConfig, *, base_path: str = "") -> AuthBundle:
    """Build an OIDC :class:`AuthBundle` (login/callback/logout router + dependency)."""
    try:
        import httpx2 as httpx
        from authlib.integrations.httpx_client import AsyncOAuth2Client
        from authlib.oidc.core import CodeIDToken
        from joserfc import jwt
        from joserfc.jwk import KeySet
    except ImportError as exc:  # pragma: no cover - exercised only without extra
        raise ImportError(
            "authlib is required for the OIDC backend. Install it with: pip install 'taskq[oidc]'"
        ) from exc

    warn_if_no_group_allowlist("oidc", config.allowed_groups)

    session_manager = SessionManager(
        secret=config.session_secret,
        max_age_seconds=config.session_max_age_seconds,
        secure_cookie=config.secure_cookie,
        cookie_path=base_path or "/",
    )
    login_path = f"{base_path}/login"
    # Scoped to the callback route alone, like the SAML correlation cookie is
    # scoped to the ACS path: the state cookie carries the PKCE verifier and
    # the nonce, is consumed by exactly one route, and should be offered on
    # exactly one.
    callback_path = f"{base_path}/callback"
    metadata_cache = _OIDCMetadataCache()
    router = APIRouter(tags=["sso-oidc"])

    async def _fetch_discovery() -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as http:
            return dict(
                (await http.get(f"{config.issuer}/.well-known/openid-configuration")).json()
            )

    @router.get("/login")
    async def login(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        try:
            import secrets

            code_verifier = secrets.token_urlsafe(48)
            state = secrets.token_urlsafe(32)
            nonce = secrets.token_urlsafe(32)
            meta = await metadata_cache.get_discovery(config.issuer, _fetch_discovery)
            async with AsyncOAuth2Client(  # pyright: ignore[reportGeneralTypeIssues]  # Why: authlib ships no stubs; AsyncOAuth2Client subclasses httpx2.AsyncClient but pyright cannot see __aenter__/__aexit__ across the untyped MRO.
                client_id=config.client_id,
                scope=config.scope,
                redirect_uri=config.redirect_uri,
                code_challenge_method="S256",
            ) as client:
                auth_url, _ = client.create_authorization_url(
                    meta["authorization_endpoint"],
                    state=state,
                    code_verifier=code_verifier,
                    nonce=nonce,
                )
            response = RedirectResponse(url=auth_url, status_code=302)
            # state + PKCE bind the authorization code to this browser; the
            # nonce binds the ID token — the credential the session is minted
            # from — so it rides the same signed cookie the callback compares
            # the ID token's nonce claim against.
            _issue_state_cookie(
                response,
                config.session_secret,
                state,
                code_verifier,
                nonce,
                secure=config.secure_cookie,
                path=callback_path,
            )
            return response
        except Exception:
            logger.exception("oidc-login-error")
            return _error_redirect(base_path)

    @router.get("/callback")
    async def callback(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        try:
            import hmac

            state_cookie = request.cookies.get(_STATE_COOKIE_NAME)
            if not state_cookie:
                raise ValueError("missing state cookie")
            state_data = _read_state_cookie(state_cookie, config.session_secret)
            if state_data is None:
                raise ValueError("invalid state cookie")
            expected_state = state_data["state"]
            code_verifier = state_data["cv"]
            nonce = state_data["nonce"]

            query_state = request.query_params.get("state")
            if not query_state or not hmac.compare_digest(query_state, expected_state):
                raise ValueError("state mismatch")
            if request.query_params.get("error"):
                raise ValueError("IdP returned error")
            code = request.query_params.get("code")
            if not code:
                raise ValueError("missing authorization code")

            meta = await metadata_cache.get_discovery(config.issuer, _fetch_discovery)

            async def _fetch_jwks() -> dict[str, Any]:
                async with httpx.AsyncClient(timeout=10.0) as http:
                    return dict((await http.get(meta["jwks_uri"])).json())

            jwks = await metadata_cache.get_jwks(config.issuer, _fetch_jwks)

            async with AsyncOAuth2Client(  # pyright: ignore[reportGeneralTypeIssues]  # Why: authlib ships no stubs; see login() above.
                client_id=config.client_id,
                client_secret=config.client_secret,
                scope=config.scope,
                redirect_uri=config.redirect_uri,
                code_challenge_method="S256",
            ) as client:
                token = await client.fetch_token(
                    meta["token_endpoint"],
                    authorization_response=str(request.url),
                    state=expected_state,
                    code_verifier=code_verifier,
                    redirect_uri=config.redirect_uri,
                )

            id_token_str = token.get("id_token")
            if not isinstance(id_token_str, str) or not id_token_str:
                raise ValueError("missing id_token")

            key_set = KeySet.import_key_set(jwks)  # pyright: ignore[reportArgumentType]  # Why: joserfc's KeySetSerialization is structurally the JSON dict the JWKS endpoint returned; the response is not typed as such.
            token_obj = jwt.decode(id_token_str, key_set)
            options: dict[str, dict[str, object]] = {
                "iss": {"essential": True, "value": config.issuer},
                "aud": {"essential": True, "value": config.client_id},
            }
            claims_obj = CodeIDToken(
                token_obj.claims,
                token_obj.header,
                options=options,
                # The nonce param is what arms authlib's ID-token nonce check:
                # the token must carry this login's nonce or validation fails,
                # so an ID token minted for any other flow cannot mint a session.
                params={"client_id": config.client_id, "nonce": nonce},
            )
            claims_obj.validate()
            id_claims: dict[str, object] = dict(claims_obj)

            subject = id_claims.get("sub")
            if not isinstance(subject, str) or not subject:
                raise ValueError("missing subject")
            email_raw = id_claims.get("email")
            email = str(email_raw) if isinstance(email_raw, str) else None

            groups: frozenset[str] = frozenset()
            if config.group_claim is not None:
                groups = _extract_groups(id_claims.get(config.group_claim))
            if not groups and config.group_resolver is not None:
                access_token = token.get("access_token")
                at = str(access_token) if isinstance(access_token, str) else None
                groups = await config.group_resolver(
                    OIDCTokenContext(id_token_claims=id_claims, access_token=at)
                )

            if config.allowed_groups and not groups:
                raise ValueError("no group membership for allowlist")

            identity = IdentityClaims(
                subject=subject,
                email=email,
                groups=groups,
                raw=id_claims,
            )
            response = RedirectResponse(url=base_path or "/", status_code=302)
            session_manager.set_session_cookie(response, identity)
            _clear_state_cookie(response, config.secure_cookie, callback_path)
            return response
        except Exception:
            logger.exception("oidc-callback-error")
            resp = _error_redirect(base_path)
            _clear_state_cookie(resp, config.secure_cookie, callback_path)
            return resp

    @router.post("/logout")
    async def logout(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        # POST + a CSRF token bound to the live session: a forced top-level
        # navigation is a GET and can no longer clear an admin session, and a
        # cross-site form POST carries neither the session cookie it must
        # derive the token from (HttpOnly, SameSite=Lax) nor the secret.
        await require_logout_csrf(request, session_manager)
        response = RedirectResponse(url=base_path or "/", status_code=302)
        session_manager.clear_session_cookie(response)
        return response

    dependency = create_auth_dependency(
        session_manager,
        config.allowed_groups,
        login_path=login_path,
    )
    return AuthBundle(router=router, dependency=dependency)
