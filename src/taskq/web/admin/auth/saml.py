"""SAML SSO backend (OneLogin python3-saml toolkit; for legacy IdPs).

``python3-saml`` binds to the system ``libxmlsec1`` C library.  The import is
guarded so :mod:`taskq.web.admin.auth` never crashes when the ``[saml]`` extra
is absent; a clear :class:`ImportError` with install instructions is raised
when :func:`create_saml_auth` is called without the extra.
"""

import time
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
    warn_if_no_group_allowlist,
)

__all__ = [
    "SAMLAuthConfig",
    "create_saml_auth",
]

logger = structlog.get_logger("taskq.web.admin.auth.saml")

_REQUEST_COOKIE_NAME: str = "taskq_saml_request"
_REQUEST_MAX_AGE: int = 300
_REPLAY_CACHE_MAX_ENTRIES: int = 10_000
_REPLAY_FALLBACK_TTL_SECONDS: int = 3600


class SAMLAuthConfig(BaseModel):
    """Configuration for the SAML SSO backend."""

    model_config = ConfigDict(frozen=True)

    entity_id: str = Field(description="SP entity ID.")
    acs_url: str = Field(description="Assertion Consumer Service URL (the /callback route).")
    idp_entity_id: str = Field(description="IdP entity ID.")
    idp_sso_url: str = Field(description="IdP SSO redirect/POST endpoint.")
    idp_x509_cert: str = Field(description="IdP signing certificate (PEM).")
    sp_x509_cert: str | None = Field(
        default=None, description="SP cert (signed requests / encrypted assertions)."
    )
    sp_private_key: str | None = Field(default=None, description="SP private key (PEM).")
    session_secret: str = Field(description="Signing key for session cookies.")
    session_max_age_seconds: int = Field(default=28800, description="Session lifetime (s).")
    secure_cookie: bool = Field(default=True, description="Set False only for local http dev.")
    group_attribute: str | None = Field(
        default=None,
        description="SAML attribute-statement name to read into IdentityClaims.groups.",
    )
    allowed_groups: frozenset[str] = Field(
        default_factory=frozenset,
        description="Allowlist checked against the group attribute when set.",
    )


def _build_settings(config: SAMLAuthConfig) -> dict[str, Any]:
    """Build the python3-saml settings dict from the Pydantic config."""
    sp: dict[str, Any] = {
        "entityId": config.entity_id,
        "assertionConsumerService": {
            "url": config.acs_url,
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
        },
    }
    if config.sp_x509_cert:
        sp["x509cert"] = config.sp_x509_cert
    if config.sp_private_key:
        sp["privateKey"] = config.sp_private_key
    return {
        "strict": True,
        "debug": False,
        "sp": sp,
        "idp": {
            "entityId": config.idp_entity_id,
            "singleSignOnService": {
                "url": config.idp_sso_url,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "x509cert": config.idp_x509_cert,
        },
    }


def _request_data(request: Request, post_data: dict[str, str] | None = None) -> dict[str, Any]:
    """Build the request_data dict python3-saml expects from a FastAPI Request."""
    host = request.url.hostname or "localhost"
    port = request.url.port
    if port and port not in (80, 443):
        host = f"{host}:{port}"
    data: dict[str, Any] = {
        "https": "on" if request.url.scheme == "https" else "off",
        "http_host": host,
        "script_name": request.url.path,
    }
    if post_data is not None:
        data["post_data"] = post_data
    return data


def _error_redirect(base_path: str) -> RedirectResponse:
    return RedirectResponse(url=f"{base_path}?error=authentication+failed", status_code=302)


def _request_serializer(secret: str) -> Any:
    from itsdangerous import URLSafeTimedSerializer

    return URLSafeTimedSerializer(secret, salt="taskq-saml-request")


def _acs_path(base_path: str) -> str:
    return f"{base_path}/callback"


def _issue_request_cookie(
    response: Response, secret: str, request_id: str, *, base_path: str, secure: bool
) -> None:
    # The IdP POSTs the assertion back from ITS OWN origin, so a hosted IdP
    # (a different registrable site than the admin UI) makes this a genuine
    # cross-site POST -- a cookie with any SameSite attribute other than
    # "none" is withheld by the browser on that request, which is exactly
    # the outage this cookie exists to prevent. Real browsers reject a
    # SameSite=None cookie outright unless it also carries Secure, so
    # ``secure`` mirrors ``config.secure_cookie`` the same way the session
    # cookie's does -- production runs behind TLS (the default) and gets a
    # spec-compliant Secure+None cookie; the local-http escape hatch that
    # already applies to the session cookie applies here too, rather than
    # this cookie inventing a stricter rule the session cookie does not
    # follow. The blast radius of SameSite=None is bounded three ways the
    # standing session cookie is not: the cookie's own value is a
    # short-lived, single-use, server-signed correlation token (not a
    # credential -- possessing it authenticates nothing by itself, only the
    # signed IdP assertion does that); its `path` is scoped to the ACS
    # endpoint alone, so no other route ever sees it; and its `max_age`
    # matches the AuthnRequest's own validity window, so it cannot be
    # replayed after that window closes.
    response.set_cookie(
        _REQUEST_COOKIE_NAME,
        str(_request_serializer(secret).dumps({"request_id": request_id})),
        max_age=_REQUEST_MAX_AGE,
        path=_acs_path(base_path),
        httponly=True,
        secure=secure,
        samesite="none",
    )


def _read_request_cookie(cookie: str, secret: str) -> str | None:
    from itsdangerous import BadSignature, SignatureExpired

    try:
        payload = _request_serializer(secret).loads(cookie, max_age=_REQUEST_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(payload, dict):
        return None
    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not request_id:
        return None
    return request_id


def _clear_request_cookie(response: Response, base_path: str, *, secure: bool) -> None:
    response.delete_cookie(
        _REQUEST_COOKIE_NAME,
        path=_acs_path(base_path),
        httponly=True,
        secure=secure,
        samesite="none",
    )


class _PendingRequestStore:
    """Process-local record of AuthnRequest IDs this process's ``/login`` issued.

    The cookie is the primary binding (it also survives a multi-process
    deployment, since it round-trips through the browser rather than this
    process's memory), but a hosted IdP's ACS POST is a genuine cross-site
    request: even a ``SameSite=None`` cookie can arrive missing (an
    intermediary that strips unrecognised cookie attributes, a browser
    extension, third-party-cookie blocking a misconfigured deployment
    triggers) with no way for this process to tell "stripped" apart from
    "attacker POST with no cookie at all." This store is that fallback, not
    a replacement for the cookie: `/login` records the ID it minted here
    *in addition to* setting the cookie, and `/callback` consults the store
    only when the cookie itself is absent or unreadable. A request_id not
    recorded here is refused exactly like a missing cookie -- there is no
    permissive branch, only a second place the same binding can be found.
    Mirrors :class:`_AssertionReplayCache`'s process-local scope and
    eviction shape: entries expire with the AuthnRequest's own short
    lifetime, and the store is capped, evicting the soonest-to-expire
    entry, so a login flood cannot grow it without bound.
    """

    def __init__(self) -> None:
        self._expiry_by_request_id: dict[str, float] = {}

    def record(self, request_id: str, *, now: float) -> None:
        self._prune_expired(now)
        if len(self._expiry_by_request_id) >= _REPLAY_CACHE_MAX_ENTRIES:
            soonest = min(self._expiry_by_request_id, key=self._expiry_by_request_id.__getitem__)
            del self._expiry_by_request_id[soonest]
        self._expiry_by_request_id[request_id] = now + _REQUEST_MAX_AGE

    def consume(self, request_id: str, *, now: float) -> bool:
        """True and forget the entry if `request_id` is a live pending login."""
        self._prune_expired(now)
        expiry = self._expiry_by_request_id.pop(request_id, None)
        return expiry is not None and expiry > now

    def _prune_expired(self, now: float) -> None:
        expired = [rid for rid, expiry in self._expiry_by_request_id.items() if expiry <= now]
        for request_id in expired:
            del self._expiry_by_request_id[request_id]


class _AssertionReplayCache:
    """Process-local record of consumed assertion IDs, one instance per bundle.

    A SAML assertion is single-use: once one has minted a session, a second
    presentation of the same ID is a replay. The record lives in this process
    only — a multi-process deployment runs one cache per process, so a replay
    routed to a sibling process is not caught here; the InResponseTo binding
    (every accepted assertion must answer this browser's own AuthnRequest) is
    the check that does not depend on process locality. Entries expire with
    their assertion's NotOnOrAfter — past that window the assertion is already
    rejected on its timestamps, so pruning its ID loses nothing — and the
    cache is capped, evicting the soonest-to-expire entry, so it can never
    grow without bound.
    """

    def __init__(self) -> None:
        self._expiry_by_assertion_id: dict[str, float] = {}

    def consume(self, assertion_id: str, not_on_or_after: float | None, *, now: float) -> None:
        """Record an assertion ID as consumed; a second consume of the same ID raises."""
        self._prune_expired(now)
        if assertion_id in self._expiry_by_assertion_id:
            raise ValueError("SAML assertion replayed")
        expiry = (
            not_on_or_after if not_on_or_after is not None else now + _REPLAY_FALLBACK_TTL_SECONDS
        )
        if len(self._expiry_by_assertion_id) >= _REPLAY_CACHE_MAX_ENTRIES:
            soonest = min(
                self._expiry_by_assertion_id, key=self._expiry_by_assertion_id.__getitem__
            )
            del self._expiry_by_assertion_id[soonest]
        self._expiry_by_assertion_id[assertion_id] = expiry

    def _prune_expired(self, now: float) -> None:
        expired = [aid for aid, expiry in self._expiry_by_assertion_id.items() if expiry <= now]
        for assertion_id in expired:
            del self._expiry_by_assertion_id[assertion_id]


def create_saml_auth(config: SAMLAuthConfig, *, base_path: str = "") -> AuthBundle:
    """Build a SAML :class:`AuthBundle` (login/callback/metadata/logout + dependency)."""
    try:
        from onelogin.saml2.auth import OneLogin_Saml2_Auth
        from onelogin.saml2.settings import OneLogin_Saml2_Settings
    except ImportError as exc:  # pragma: no cover - exercised only without extra
        raise ImportError(
            "python3-saml is required for the SAML backend and binds to the system "
            "libxmlsec1 library. Install it with: pip install 'taskq[saml]' "
            "(see docs/guides/sso.md for container requirements)"
        ) from exc

    warn_if_no_group_allowlist("saml", config.allowed_groups)

    session_manager = SessionManager(
        secret=config.session_secret,
        max_age_seconds=config.session_max_age_seconds,
        secure_cookie=config.secure_cookie,
        cookie_path=base_path or "/",
    )
    login_path = f"{base_path}/login"
    settings_dict = _build_settings(config)
    replay_cache = _AssertionReplayCache()
    pending_requests = _PendingRequestStore()
    router = APIRouter(tags=["sso-saml"])

    @router.get("/login")
    async def login(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        try:
            auth = OneLogin_Saml2_Auth(_request_data(request), settings_dict)
            sso_url = auth.login(return_to=base_path or "/")
            request_id = auth.get_last_request_id()
            if not isinstance(request_id, str) or not request_id:
                raise ValueError("AuthnRequest has no ID")
            response = RedirectResponse(url=sso_url, status_code=302)
            # The callback accepts only an assertion answering the AuthnRequest
            # this login minted; the ID travels to the browser in a signed
            # cookie because the ACS POST returns through the browser, not
            # through this process. Also recorded server-side: a hosted IdP's
            # ACS POST is a genuine cross-site request, and even a
            # SameSite=None cookie can arrive stripped (an intermediary,
            # extension, or misconfigured third-party-cookie block) — the
            # store is the fallback binding /callback falls back to, never a
            # looser check than the cookie provided.
            pending_requests.record(request_id, now=time.time())
            _issue_request_cookie(
                response,
                config.session_secret,
                request_id,
                base_path=base_path,
                secure=config.secure_cookie,
            )
            return response
        except Exception:
            logger.exception("saml-login-error")
            return _error_redirect(base_path)

    @router.get("/metadata")
    async def metadata(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        settings = OneLogin_Saml2_Settings(settings_dict, sp_validation_only=True)
        xml = settings.get_sp_metadata()
        return Response(content=xml, media_type="application/xml")

    @router.post("/callback")
    async def callback(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        try:
            # The ACS endpoint answers this browser's own AuthnRequest and
            # nothing else. The cookie is the primary binding and, when
            # present, is handed to process_response so python3-saml itself
            # refuses a response answering a different request_id. A hosted
            # IdP's ACS POST is a genuine cross-site request though, and even
            # a SameSite=None cookie can arrive stripped — when it does, the
            # server-side pending-request store is the fallback: the
            # response is still signature-validated with no request_id
            # pinned, and only THEN is its InResponseTo checked against a
            # request this process's own /login actually issued. Either path
            # ends at the same equality check below, so a response answering
            # no live AuthnRequest is refused regardless of which binding
            # was available.
            request_cookie = request.cookies.get(_REQUEST_COOKIE_NAME)
            cookie_request_id = (
                _read_request_cookie(request_cookie, config.session_secret)
                if request_cookie
                else None
            )

            form = await request.form()
            post_data: dict[str, str] = {}
            for key, value in form.multi_items():
                if isinstance(value, str):
                    post_data[key] = value
            auth = OneLogin_Saml2_Auth(_request_data(request, post_data=post_data), settings_dict)
            auth.process_response(request_id=cookie_request_id)
            if auth.get_errors():
                raise ValueError(auth.get_last_error_reason() or "SAML response validation failed")
            if not auth.is_authenticated():
                raise ValueError("not authenticated")

            # python3-saml compares InResponseTo against request_id only when
            # request_id is non-None, so with no cookie this reduces to
            # signature validation with no binding check yet -- the
            # equality/store check below is what refuses an IdP-initiated or
            # otherwise unbound response in that case.
            in_response_to = auth.get_last_response_in_response_to()
            if cookie_request_id is not None:
                if in_response_to != cookie_request_id:
                    raise ValueError("SAML response does not answer this browser's AuthnRequest")
            else:
                if not isinstance(in_response_to, str) or not in_response_to:
                    raise ValueError("no pending SAML AuthnRequest for this browser")
                if not pending_requests.consume(in_response_to, now=time.time()):
                    raise ValueError("SAML response does not answer a pending AuthnRequest")

            # An assertion whose InResponseTo is absent (or otherwise valid but
            # captured) can be re-POSTed while its window is live; only a
            # consumed-ID record refuses the second presentation.
            assertion_id = auth.get_last_assertion_id()
            if not isinstance(assertion_id, str) or not assertion_id:
                raise ValueError("SAML response carries no assertion ID")
            replay_cache.consume(
                assertion_id, auth.get_last_assertion_not_on_or_after(), now=time.time()
            )

            nameid = auth.get_nameid()
            if not isinstance(nameid, str) or not nameid:
                raise ValueError("missing NameID")

            attributes = auth.get_attributes()
            raw: dict[str, Any] = {
                "attributes": attributes,
                "nameid": nameid,
                "nameid_format": auth.get_nameid_format(),
            }

            groups: frozenset[str] = frozenset()
            if config.group_attribute is not None:
                raw_groups = attributes.get(config.group_attribute, [])
                if isinstance(raw_groups, list):
                    groups = frozenset(str(g) for g in raw_groups)
                elif isinstance(raw_groups, str):
                    groups = frozenset({raw_groups})

            if config.allowed_groups and not groups:
                raise ValueError("no group membership for allowlist")

            email: str | None = None
            email_attr = attributes.get(
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress"
            )
            if isinstance(email_attr, list) and email_attr:
                email = str(email_attr[0])
            elif isinstance(email_attr, str):
                email = email_attr

            identity = IdentityClaims(
                subject=nameid,
                email=email,
                groups=groups,
                raw=raw,
            )
            response = RedirectResponse(url=base_path or "/", status_code=302)
            session_manager.set_session_cookie(response, identity)
            # The AuthnRequest ID is single-use: drop it whether the callback
            # succeeded or failed, so a second POST must begin a new login.
            _clear_request_cookie(response, base_path, secure=config.secure_cookie)
            return response
        except Exception:
            logger.exception("saml-callback-error")
            resp = _error_redirect(base_path)
            _clear_request_cookie(resp, base_path, secure=config.secure_cookie)
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
