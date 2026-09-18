"""SAML SSO backend (OneLogin python3-saml toolkit; for legacy IdPs).

``python3-saml`` binds to the system ``libxmlsec1`` C library.  The import is
guarded so :mod:`taskq.web.admin.auth` never crashes when the ``[saml]`` extra
is absent; a clear :class:`ImportError` with install instructions is raised
when :func:`create_saml_auth` is called without the extra.
"""

import time
from typing import Any, Literal

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
    "SAMLAuthConfig",
    "create_saml_auth",
]

logger = structlog.get_logger("taskq.web.admin.auth.saml")

_REQUEST_COOKIE_NAME: str = "taskq_saml_request"
_REQUEST_MAX_AGE: int = 300
_REPLAY_CACHE_MAX_ENTRIES: int = 10_000
_REPLAY_FALLBACK_TTL_SECONDS: int = 3600
_PENDING_REQUEST_MAX_ENTRIES: int = 10_000
_ANSWERED_REQUEST_MAX_ENTRIES: int = 10_000


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
    allow_cookieless_fallback: bool = Field(
        default=False,
        description="Opt in to accepting an ACS callback whose correlation "
        "cookie is missing, when its validated InResponseTo names an "
        "AuthnRequest this process issued and has not spent. Needed only for "
        "browsers that block the cross-site correlation cookie; nothing ties "
        "the response to the browser posting it, so a captured signed "
        "response can be planted on a cookie-less victim (login CSRF) -- the "
        "operator opts in and accepts that tradeoff. Default off.",
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


def _request_cookie_samesite(secure: bool) -> Literal["lax", "none"]:
    """SameSite attribute for the AuthnRequest correlation cookie.

    A hosted IdP lives on a different registrable domain, so its ACS POST is
    cross-site and a browser withholds a cookie marked ``SameSite=Lax`` from
    it. ``None`` is what makes the correlation cookie arrive, and it is
    confined to this one short-lived, ACS-path-scoped, HttpOnly cookie: the
    session cookie is the standing credential for the whole admin mount and
    keeps its non-cross-site policy.

    ``SameSite=None`` without ``Secure`` is rejected outright by browsers, so
    a plain-http dev deployment keeps ``Lax`` -- there is no cross-site IdP to
    serve in that configuration anyway.
    """
    return "none" if secure else "lax"


def _issue_request_cookie(
    response: Response, secret: str, request_id: str, *, secure: bool, path: str
) -> None:
    response.set_cookie(
        _REQUEST_COOKIE_NAME,
        str(_request_serializer(secret).dumps({"request_id": request_id})),
        max_age=_REQUEST_MAX_AGE,
        httponly=True,
        secure=secure,
        samesite=_request_cookie_samesite(secure),
        path=path,
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


def _clear_request_cookie(response: Response, secure: bool, path: str) -> None:
    response.delete_cookie(
        _REQUEST_COOKIE_NAME,
        httponly=True,
        secure=secure,
        samesite=_request_cookie_samesite(secure),
        path=path,
    )


class _ExpiringIdSet:
    """Process-local set of SAML correlation IDs, each with its own expiry.

    Every SAML ID gate needs the same store: a bounded set of IDs that ages
    out on its own. Entries past their expiry are dropped on every touch (an
    ID past its window is already refused on the assertion's own timestamps,
    so pruning it loses nothing), and the set is capped, evicting the
    soonest-to-expire entry, so a flood of IDs cannot grow it without bound.

    Process-local: a multi-process deployment runs one set per process, so a
    presentation routed to a sibling process is not seen here. That is why
    these gates sit alongside, not instead of, the signature and timestamp
    validation that holds in every process.
    """

    def __init__(self, max_entries: int) -> None:
        self._max_entries = max_entries
        self._expiry_by_id: dict[str, float] = {}

    def add(self, identifier: str, expiry: float, *, now: float) -> None:
        self._prune_expired(now)
        if len(self._expiry_by_id) >= self._max_entries:
            soonest = min(self._expiry_by_id, key=self._expiry_by_id.__getitem__)
            del self._expiry_by_id[soonest]
        self._expiry_by_id[identifier] = expiry

    def contains(self, identifier: str, *, now: float) -> bool:
        self._prune_expired(now)
        return identifier in self._expiry_by_id

    def discard(self, identifier: str) -> None:
        self._expiry_by_id.pop(identifier, None)

    def _prune_expired(self, now: float) -> None:
        expired = [key for key, expiry in self._expiry_by_id.items() if expiry <= now]
        for key in expired:
            del self._expiry_by_id[key]


class _AssertionReplayCache:
    """Record of consumed assertion IDs, one instance per bundle.

    A SAML assertion is single-use: once one has minted a session, a second
    presentation of the same ID is a replay. Entries expire with their
    assertion's NotOnOrAfter.

    Process-local, like every store in this module: a second presentation of
    the same response to a *sibling* process or replica is not seen here.
    On the cookie-less fallback path that replay is still refused by the
    pending-set spend; on the cookie path the browser's copy of the
    single-use cookie is cleared on first use, so what remains exposed is a
    network-level party who captured both the cookie and the response
    body re-POSTing them to a sibling within the cookie's 300 s TTL.
    Closing that needs a replay record in a store every replica shares
    (Postgres/Redis) -- a deliberate follow-up, not something this
    stateless auth layer can grow on its own.
    """

    def __init__(self) -> None:
        self._consumed = _ExpiringIdSet(_REPLAY_CACHE_MAX_ENTRIES)

    def consume(self, assertion_id: str, not_on_or_after: float | None, *, now: float) -> None:
        """Record an assertion ID as consumed; a second consume of the same ID raises."""
        if self._consumed.contains(assertion_id, now=now):
            raise ValueError("SAML assertion replayed")
        expiry = (
            not_on_or_after if not_on_or_after is not None else now + _REPLAY_FALLBACK_TTL_SECONDS
        )
        self._consumed.add(assertion_id, expiry, now=now)


class _PendingAuthnRequests:
    """Record of AuthnRequest IDs this process issued and has not yet answered.

    The correlation cookie binds an accepted assertion to the very browser
    that started the login, is verifiable by every process sharing
    ``session_secret``, and is the only binding the default policy needs.
    It cannot serve one deployment shape on its own: the cookie rides a
    cross-site POST from a hosted IdP, and a browser may withhold it
    however the cookie is marked (third-party cookie blocking, a privacy
    mode, a redirect chain that drops it).

    ``allow_cookieless_fallback`` opts a deployment into the weaker second
    binding for exactly that shape: an assertion arriving with no usable
    cookie is accepted only if, after full signature and timestamp
    validation, its InResponseTo names an AuthnRequest this process issued
    and has not yet spent. That keeps the property the cookie gate was
    protecting -- no assertion answering a login this deployment never
    started can mint a session, so a captured or IdP-initiated response is
    still refused -- while losing the narrower binding to one browser: the
    posting browser need not be the one that started the login, which is
    the login-CSRF tradeoff the flag's documentation states directly. On that
    fallback path the spend is a real gate: the ID is consumed by the first
    assertion that answers it, so the window is a single login attempt wide.
    The cookie path does not consult this set for admission -- the login may
    have been issued by a sibling process -- so the ID's single-use property
    there is enforced by :class:`_AnsweredAuthnRequests` instead.
    """

    def __init__(self) -> None:
        self._issued = _ExpiringIdSet(_PENDING_REQUEST_MAX_ENTRIES)

    def issue(self, request_id: str, *, now: float) -> None:
        self._issued.add(request_id, now + _REQUEST_MAX_AGE, now=now)

    def spend(self, request_id: str, *, now: float) -> bool:
        """Consume *request_id*; False when this process never issued it (or it aged out)."""
        if not self._issued.contains(request_id, now=now):
            return False
        self._issued.discard(request_id)
        return True

    def discard(self, request_id: str) -> None:
        """Drop *request_id* without requiring that this process issued it.

        The cookie-valid callback path drops the ID best-effort: the login
        may have been issued by a sibling process, so the ID's absence here
        is not an error on that path -- the signed cookie is the binding.
        Dropping is bookkeeping, not a gate; admission on that path and the
        ID's single-use property are enforced elsewhere (the answered-request
        record below).
        """
        self._issued.discard(request_id)


class _AnsweredAuthnRequests:
    """Record of AuthnRequest IDs an accepted assertion has already answered.

    The server-side half of the AuthnRequest ID's single-use property on the
    cookie path. The browser's half -- clearing the correlation cookie on
    every callback outcome -- binds the honest browser; a party that
    captured the POST holds a copy of the cookie that clearing cannot reach,
    and on the issuing process the pending-set drop is deliberately not a
    gate. This record refuses a second DISTINCT assertion answering the same
    request ID -- the shape the replay cache cannot refuse, because the
    replaying party's second response carries a fresh assertion ID.

    Entries live for the correlation cookie's own window (``_REQUEST_MAX_AGE``):
    after that the cookie can no longer authenticate a presentation on the
    cookie path, and the fallback path's pending-set spend has aged out too,
    so an answered-ID record has nothing left to refuse.

    Process-local like every store in this module, and capped/evictable like
    every other (eviction requires a flood of *accepted* logins, since only
    an accepted presentation writes here) -- a sibling process or an
    eviction under flood pressure is not covered. That needs a replay
    record in a store every replica shares, which is tracked separately as
    a follow-up.
    """

    def __init__(self) -> None:
        self._answered = _ExpiringIdSet(_ANSWERED_REQUEST_MAX_ENTRIES)

    def already_answered(self, request_id: str, *, now: float) -> bool:
        return self._answered.contains(request_id, now=now)

    def record(self, request_id: str, *, now: float) -> None:
        """Mark *request_id* as answered; a later presentation of it is refused."""
        self._answered.add(request_id, now + _REQUEST_MAX_AGE, now=now)


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
    # Scoped to the ACS route alone: the correlation cookie is marked for
    # cross-site delivery, so every path it is offered on is a path a
    # third-party page can cause it to be sent to. It is needed on exactly
    # one.
    callback_path = f"{base_path}/callback"
    settings_dict = _build_settings(config)
    replay_cache = _AssertionReplayCache()
    pending_requests = _PendingAuthnRequests()
    answered_requests = _AnsweredAuthnRequests()
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
            # The callback accepts only an assertion answering an AuthnRequest
            # this login minted. The ID is recorded twice: in a signed cookie,
            # which binds the answer to this very browser and is verifiable by
            # every process sharing session_secret, and in the pending set,
            # which backs the opt-in cookie-less fallback (the only path that
            # still needs it, since the set is process-local).
            pending_requests.issue(request_id, now=time.time())
            _issue_request_cookie(
                response,
                config.session_secret,
                request_id,
                secure=config.secure_cookie,
                path=callback_path,
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
            # Every accepted assertion answers an AuthnRequest this deployment
            # issued, and which gate enforces that depends on what the browser
            # brought. With a usable correlation cookie, the cookie IS the
            # binding: python3-saml itself compares the response's
            # InResponseTo against the cookie's request_id inside
            # process_response, and the cookie is signed with session_secret
            # (verifiable by every replica sharing it), 300 s old at most,
            # and single-use twice over -- cleared below for the honest
            # browser, and answered-recorded below so a re-supplied captured
            # copy cannot buy a second assertion. Without one, the validated
            # InResponseTo is looked up in the pending set -- but only when
            # the deployment opted into that fallback, because nothing about
            # it ties the response to the browser posting it.
            request_cookie = request.cookies.get(_REQUEST_COOKIE_NAME)
            request_id = (
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
            auth.process_response(request_id=request_id)
            if auth.get_errors():
                raise ValueError(auth.get_last_error_reason() or "SAML response validation failed")
            if not auth.is_authenticated():
                raise ValueError("not authenticated")

            # python3-saml compares InResponseTo only when the response
            # carries one, so an IdP-initiated response (no InResponseTo)
            # would pass process_response unanswered to any AuthnRequest;
            # only this check refuses that shape.
            in_response_to = auth.get_last_response_in_response_to()
            if not isinstance(in_response_to, str) or not in_response_to:
                raise ValueError("SAML response answers no AuthnRequest")
            if request_id is not None:
                # Cookie path: the signed, 300 s cookie is the binding. The
                # pending set is process-local, so requiring a successful
                # spend here would reject any callback served by a sibling
                # replica or worker process that never saw the login (#239)
                # -- and an unauthenticated /login flood evicting the ID
                # would break a legitimate login the same way. The ID is
                # dropped best-effort instead, and its single-use property
                # on this path is enforced right here by the answered-request
                # record: a second DISTINCT assertion answering an
                # already-answered ID -- the captured cookie re-supplied, a
                # fresh assertion ID so the replay cache cannot refuse it --
                # is refused server-side, not only by the browser's cookie
                # having been cleared. The replay cache below still refuses
                # a re-presentation of the same assertion.
                if in_response_to != request_id:
                    raise ValueError("SAML response does not answer this browser's AuthnRequest")
                if answered_requests.already_answered(in_response_to, now=time.time()):
                    raise ValueError("SAML response answers an already-answered AuthnRequest")
                pending_requests.discard(in_response_to)
            elif config.allow_cookieless_fallback:
                # Opt-in fallback: the validated InResponseTo must name an
                # AuthnRequest this process issued and has not spent -- the
                # spend IS the single-use gate on this path. Nothing binds
                # the response to the browser posting it (login CSRF, #240)
                # -- the flag's documentation says so, and the default is
                # off.
                if not pending_requests.spend(in_response_to, now=time.time()):
                    raise ValueError("SAML response answers no pending AuthnRequest")
            else:
                raise ValueError(
                    "SAML callback refused: no usable taskq_saml_request "
                    "correlation cookie and the cookie-less fallback is "
                    "disabled. The browser must send the correlation cookie "
                    "(started by /login on this browser, SameSite=None + "
                    "Secure); operators serving browsers that block it can "
                    "opt in with allow_cookieless_fallback "
                    "(TASKQ_SAML_ALLOW_COOKIELESS_FALLBACK=true) and accept "
                    "the login-CSRF tradeoff documented in docs/guides/sso.md"
                )
            # Whichever gate admitted it, the AuthnRequest ID is now
            # answered: recorded so a second distinct assertion answering it
            # is refused even when the posting party re-supplies a captured
            # correlation cookie (the browser's own copy is cleared below).
            # On the fallback path the pending-set spend already refuses a
            # second fallback presentation; the record additionally closes
            # the cross-path replay -- fallback acceptance, then a cookie-
            # path replay of a fresh assertion with the captured cookie.
            answered_requests.record(in_response_to, now=time.time())

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
            # That is the browser's half; the answered-request record above
            # is the server-side half against a re-supplied captured cookie.
            _clear_request_cookie(response, config.secure_cookie, callback_path)
            return response
        except Exception:
            logger.exception("saml-callback-error")
            resp = _error_redirect(base_path)
            _clear_request_cookie(resp, config.secure_cookie, callback_path)
            return resp

    @router.post("/logout")
    async def logout(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        # Same contract as the OIDC backend (shared require_logout_csrf): a
        # forced top-level navigation is a GET and can no longer clear an
        # admin session, and a cross-site form POST carries neither the
        # session cookie it must derive the token from nor the secret.
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
