"""OIDC SSO backend (vendor-neutral; primary target: Microsoft Entra ID).

Uses :mod:`authlib` for the OAuth2 authorization-code + PKCE flow and
:mod:`joserfc` (bundled with authlib) for ID-token signature/claims validation.
``authlib`` is imported lazily inside :func:`create_oidc_auth` so importing
:mod:`taskq.web.admin.auth` never crashes when the ``[oidc]`` extra is absent.
"""

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
)

__all__ = [
    "OIDCAuthConfig",
    "OIDCTokenContext",
    "create_oidc_auth",
]

logger = structlog.get_logger("taskq.web.admin.auth.oidc")

_STATE_COOKIE_NAME: str = "taskq_oidc_state"
_STATE_MAX_AGE: int = 300


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
    response: Response, secret: str, state: str, code_verifier: str, *, secure: bool
) -> None:
    response.set_cookie(
        _STATE_COOKIE_NAME,
        str(_state_serializer(secret).dumps({"state": state, "cv": code_verifier})),
        max_age=_STATE_MAX_AGE,
        httponly=True,
        secure=secure,
        samesite="lax",
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
    if not isinstance(state, str) or not isinstance(cv, str):
        return None
    return {"state": state, "cv": cv}


def _clear_state_cookie(response: Response, secure: bool) -> None:
    response.delete_cookie(_STATE_COOKIE_NAME, httponly=True, secure=secure, samesite="lax")


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

    session_manager = SessionManager(
        secret=config.session_secret,
        max_age_seconds=config.session_max_age_seconds,
        secure_cookie=config.secure_cookie,
    )
    login_path = f"{base_path}/login"
    router = APIRouter(tags=["sso-oidc"])

    @router.get("/login")
    async def login(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        try:
            import secrets

            code_verifier = secrets.token_urlsafe(48)
            state = secrets.token_urlsafe(32)
            async with httpx.AsyncClient(timeout=10.0) as http:
                meta = (await http.get(f"{config.issuer}/.well-known/openid-configuration")).json()
            async with AsyncOAuth2Client(  # pyright: ignore[reportGeneralTypeIssues]  # Why: authlib ships no stubs; AsyncOAuth2Client subclasses httpx.AsyncClient but pyright cannot see __aenter__/__aexit__ across the untyped MRO.
                client_id=config.client_id,
                scope=config.scope,
                redirect_uri=config.redirect_uri,
                code_challenge_method="S256",
            ) as client:
                auth_url, _ = client.create_authorization_url(
                    meta["authorization_endpoint"],
                    state=state,
                    code_verifier=code_verifier,
                )
            response = RedirectResponse(url=auth_url, status_code=302)
            _issue_state_cookie(
                response, config.session_secret, state, code_verifier, secure=config.secure_cookie
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

            query_state = request.query_params.get("state")
            if not query_state or not hmac.compare_digest(query_state, expected_state):
                raise ValueError("state mismatch")
            if request.query_params.get("error"):
                raise ValueError("IdP returned error")
            code = request.query_params.get("code")
            if not code:
                raise ValueError("missing authorization code")

            async with httpx.AsyncClient(timeout=10.0) as http:
                meta = (await http.get(f"{config.issuer}/.well-known/openid-configuration")).json()
                jwks = (await http.get(meta["jwks_uri"])).json()

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

            key_set = KeySet.import_key_set(jwks)
            token_obj = jwt.decode(id_token_str, key_set)
            options: dict[str, dict[str, object]] = {
                "iss": {"essential": True, "value": config.issuer},
                "aud": {"essential": True, "value": config.client_id},
            }
            claims_obj = CodeIDToken(
                token_obj.claims,
                token_obj.header,
                options=options,
                params={"client_id": config.client_id},
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
            _clear_state_cookie(response, config.secure_cookie)
            return response
        except Exception:
            logger.exception("oidc-callback-error")
            resp = _error_redirect(base_path)
            _clear_state_cookie(resp, config.secure_cookie)
            return resp

    @router.get("/logout")
    async def logout() -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        response = RedirectResponse(url=base_path or "/", status_code=302)
        session_manager.clear_session_cookie(response)
        return response

    dependency = create_auth_dependency(
        session_manager,
        config.allowed_groups,
        login_path=login_path,
    )
    return AuthBundle(router=router, dependency=dependency)
