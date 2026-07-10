"""Protocol-agnostic SSO session core: signed cookies, identity, auth dependency.

This module owns the parts shared by both the OIDC and SAML backends:

* :class:`IdentityClaims` — normalized identity regardless of which protocol
  produced it.
* :class:`AuthBundle` — the router + dependency pair returned by each backend
  factory.
* :class:`SessionManager` — stateless signed-cookie session via
  ``itsdangerous.URLSafeTimedSerializer`` (no Redis/DB dependency).
* :func:`create_auth_dependency` — the FastAPI dependency that gates every
  admin route on a valid session cookie and the optional group allowlist.

Importing this module does **not** require ``itsdangerous``; the dependency is
imported lazily inside :class:`SessionManager` so that ``AuthBundle`` and
``IdentityClaims`` (pure-stdlib dataclasses) remain importable without any SSO
extra installed.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import Response

__all__ = [
    "SESSION_COOKIE_NAME",
    "AuthBundle",
    "IdentityClaims",
    "SessionManager",
    "create_auth_dependency",
]
SESSION_COOKIE_NAME: str = "taskq_session"


@dataclass(frozen=True)
class IdentityClaims:
    """Normalized identity, regardless of which protocol produced it."""

    subject: str
    email: str | None
    groups: frozenset[str]
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class AuthBundle:
    """Returned by each SSO backend factory.

    ``router`` carries the login/callback/logout (and SAML metadata) routes —
    mount it at the admin router's ``base_path``.  ``dependency`` is the
    FastAPI dependency to pass to ``create_router(auth_dependency=...)``.
    """

    router: Any
    dependency: Callable[..., Any]


@dataclass
class SessionManager:
    """Stateless signed-cookie session backed by itsdangerous.

    Cookie payload stores only ``subject``, ``email``, and ``groups`` (as a
    sorted list) — no raw tokens/assertions, no PII beyond what the allowlist
    check needs.  Cookie flags: ``httponly``, ``secure`` (configurable for
    local http dev), ``samesite="lax"``.
    """

    secret: str
    max_age_seconds: int = 28800
    secure_cookie: bool = True
    cookie_name: str = SESSION_COOKIE_NAME
    _serializer: Any = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.secret:
            raise ValueError("session_secret must be a non-empty string")
        try:
            from itsdangerous import URLSafeTimedSerializer
        except ImportError as exc:  # pragma: no cover - exercised only without extra
            raise ImportError(
                "itsdangerous is required for SSO sessions. "
                "Install it with: pip install 'taskq[oidc]' or 'taskq[saml]'"
            ) from exc
        object.__setattr__(
            self, "_serializer", URLSafeTimedSerializer(self.secret, salt="taskq-session")
        )

    def create_session_cookie(self, claims: IdentityClaims) -> str:
        """Sign and return the cookie value for *claims*."""
        payload: dict[str, Any] = {
            "sub": claims.subject,
            "email": claims.email,
            "groups": sorted(claims.groups),
        }
        return str(self._serializer.dumps(payload))

    def verify_session_cookie(self, cookie: str) -> IdentityClaims | None:
        """Verify signature + expiry; return claims or ``None`` on any failure."""
        from itsdangerous import BadSignature, SignatureExpired

        try:
            payload = self._serializer.loads(cookie, max_age=self.max_age_seconds)
        except (BadSignature, SignatureExpired):
            return None
        if not isinstance(payload, dict):
            return None
        subject = payload.get("sub")
        if not isinstance(subject, str):
            return None
        email = payload.get("email")
        if email is not None and not isinstance(email, str):
            email = None
        raw_groups = payload.get("groups", [])
        if not isinstance(raw_groups, list):
            raw_groups = []
        groups = frozenset(str(g) for g in raw_groups)
        return IdentityClaims(
            subject=subject,
            email=email,
            groups=groups,
            raw=dict(payload),
        )

    def set_session_cookie(self, response: Response, claims: IdentityClaims) -> None:
        """Set the session cookie on *response* with the configured flags."""
        response.set_cookie(
            self.cookie_name,
            self.create_session_cookie(claims),
            max_age=self.max_age_seconds,
            httponly=True,
            secure=self.secure_cookie,
            samesite="lax",
        )

    def clear_session_cookie(self, response: Response) -> None:
        """Clear (expire) the session cookie on *response*."""
        response.delete_cookie(
            self.cookie_name,
            httponly=True,
            secure=self.secure_cookie,
            samesite="lax",
        )


def _accepts_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept


def _unauthorized(request: Request, login_path: str) -> None:
    """Raise 401 (API) or redirect to *login_path* (browser navigation)."""
    if _accepts_html(request):
        raise HTTPException(
            status_code=302,
            headers={"location": login_path},
        )
    raise HTTPException(status_code=401, detail="not authenticated")


def create_auth_dependency(
    session_manager: SessionManager,
    allowed_groups: frozenset[str] = frozenset(),
    *,
    login_path: str = "/login",
) -> Callable[..., Any]:
    """Build a FastAPI dependency that gates routes on a valid SSO session.

    Reads the session cookie, verifies signature + expiry, re-checks the group
    allowlist (in case ``allowed_groups`` changed since the cookie was issued),
    and raises ``HTTPException`` — redirecting to *login_path* for browser
    navigation (``Accept: text/html``) or 401 for API clients — if invalid.
    """

    async def _dependency(request: Request) -> IdentityClaims:
        cookie = request.cookies.get(session_manager.cookie_name)
        if cookie is None:
            _unauthorized(request, login_path)
        claims = session_manager.verify_session_cookie(cookie) if cookie else None
        if claims is None:
            _unauthorized(request, login_path)
        if allowed_groups and claims is not None and allowed_groups.isdisjoint(claims.groups):
            _unauthorized(request, login_path)
        assert (
            claims is not None
        )  # Why: narrowed by the guards above; satisfies pyright strict return type.
        return claims

    return _dependency
