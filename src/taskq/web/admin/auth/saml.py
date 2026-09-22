"""SAML SSO backend (OneLogin python3-saml toolkit; for legacy IdPs).

``python3-saml`` binds to the system ``libxmlsec1`` C library.  The import is
guarded so :mod:`taskq.web.admin.auth` never crashes when the ``[saml]`` extra
is absent; a clear :class:`ImportError` with install instructions is raised
when :func:`create_saml_auth` is called without the extra.
"""

import time
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import asyncpg
import structlog
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex for the schema name interpolated into the store's SQL, exactly as migrate.py does.
)
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
# Row kinds in the shared ``saml_replay_store`` table (one table, one (kind,
# id) key space, two gates).
_ASSERTION_REPLAY_KIND: str = "assertion_replay"
_ANSWERED_REQUEST_KIND: str = "answered_request"


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
    """Bounded in-process set of SAML correlation IDs, each with its own expiry.

    Backs exactly one structure now: the pending AuthnRequest map (see
    :class:`_PendingAuthnRequests`), whose process-locality is deliberate.
    The other two ID gates, assertion replay and answered requests, moved to
    the shared store (:class:`_PostgresSamlReplayStore`) that every replica
    reads and writes; their former in-process classes are gone.

    Entries past their expiry are dropped on every touch (an ID past its
    window is already refused on the assertion's own timestamps, so pruning
    it loses nothing), and the set is capped, evicting the
    soonest-to-expire entry, so a flood of IDs cannot grow it without bound.
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


class _SamlReplayStore(Protocol):
    """The two shared ID gates: assertion replay + answered AuthnRequests.

    ``consume`` is the replay gate: an atomic first-wins claim of an
    assertion ID whose second presentation raises. ``already_answered`` /
    ``record_answered`` are the read and write halves of the
    answered-AuthnRequest gate. Both carry their own expiry; rows past
    theirs neither block nor are required (the assertion's own timestamp
    validation, and the correlation cookie's signature window, refuse what
    an expired record no longer can).
    """

    async def consume(self, assertion_id: str, expires_at: float, *, now: float) -> None:
        """Claim *assertion_id* until *expires_at*; a second claim raises."""
        ...

    async def already_answered(self, request_id: str, *, now: float) -> bool:
        """True when *request_id* has an unexpired answered-record."""
        ...

    async def record_answered(self, request_id: str, ttl: int, *, now: float) -> None:
        """Record *request_id* as answered for *ttl* seconds from *now*."""
        ...


class _InProcessSamlReplayStore:
    """Process-local fallback for the two shared gates.

    Used only when the request's app carries no admin pool (``app.state``
    without both ``pg_pool`` and ``schema``): an embedder mounting this
    router on a bare FastAPI app of its own, and the test suite's bare
    fixtures. The shipped wiring (``taskq ui serve``, through
    ``setup_admin_state``) always sets both keys, so every deployment that
    serves SAML through the admin app runs :class:`_PostgresSamlReplayStore`
    instead, and this class keeps exactly the limits its predecessors
    documented: one set of records per process, capped and evictable, a
    sibling replica seeing nothing here.
    """

    def __init__(self) -> None:
        self._consumed = _ExpiringIdSet(_REPLAY_CACHE_MAX_ENTRIES)
        self._answered = _ExpiringIdSet(_ANSWERED_REQUEST_MAX_ENTRIES)

    async def consume(self, assertion_id: str, expires_at: float, *, now: float) -> None:
        """Record an assertion ID as consumed; a second consume of the same ID raises."""
        if self._consumed.contains(assertion_id, now=now):
            raise ValueError("SAML assertion replayed")
        self._consumed.add(assertion_id, expires_at, now=now)

    async def already_answered(self, request_id: str, *, now: float) -> bool:
        return self._answered.contains(request_id, now=now)

    async def record_answered(self, request_id: str, ttl: int, *, now: float) -> None:
        self._answered.add(request_id, now + ttl, now=now)


class _PostgresSamlReplayStore:
    """The two ID gates in Postgres: a store every replica shares.

    Closes the cross-replica replay the process-local records could not see:
    a captured, correctly-signed response re-POSTed to a SIBLING process now
    hits the same rows the accepting process wrote. The pool and schema come
    from the admin app's per-request state (``app.state.pg_pool`` /
    ``app.state.schema``, the same keys every admin route resolves
    dependencies from), so a credential rotation that swaps the pool is
    picked up on the next request and nothing here outlives a request.

    Atomicity: the consume and the answered-record write are one statement,
    ``INSERT ... ON CONFLICT (kind, id) DO UPDATE ... WHERE <row expired>
    RETURNING 1``. A live row for the same key returns no row (first-wins:
    the loser of the race is a replay), an expired row is reclaimed in place,
    and a fresh key inserts. Two replicas claiming the same assertion ID
    concurrently resolve to exactly one winner at the primary key, whatever
    the interleaving.

    Expiry uses the APPLICATION clock end to end: the caller passes ``now``
    and the expiry instants are application-clock values (NotOnOrAfter read
    off the assertion, TTLs counted from ``time.time()``), so both sides of
    every comparison share one clock and an application/database skew cannot
    stretch or shrink a TTL.

    Cost: only an ACCEPTED login reaches these statements (signature and
    timestamp validation run first), each claim also sweeps expired rows,
    and rows live at most one NotOnOrAfter window, so the table stays at
    roughly the accepted logins of one window and the sweep's scan of it is
    cheap. A DB outage fails closed: the exception is the callback's own
    error redirect, no session is minted.
    """

    # First-wins claim. The WHERE on the conflict branch reclaims an expired
    # row instead of being blocked by it; a live row returns no row.
    _CLAIM_SQL = (
        'INSERT INTO "{schema}".saml_replay_store (kind, id, expires_at) '
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (kind, id) DO UPDATE SET expires_at = EXCLUDED.expires_at "
        "WHERE saml_replay_store.expires_at <= $4 "
        "RETURNING 1"
    )
    # Expiry-predicated read: a record past its window answers nothing.
    _HOLDS_SQL = (
        'SELECT 1 FROM "{schema}".saml_replay_store WHERE kind = $1 AND id = $2 AND expires_at > $3'
    )
    # Opportunistic sweep: rows no claim ever touches again must not linger.
    _PURGE_SQL = 'DELETE FROM "{schema}".saml_replay_store WHERE expires_at <= $1'

    def __init__(self, pool: asyncpg.Pool, schema: str) -> None:
        # The schema is interpolated into SQL (asyncpg cannot bind
        # identifiers); validate rather than trust app.state. The admin
        # factory already validated it, this repeats at the last writer.
        if not _IDENT_RE.match(schema):
            raise ValueError(f"invalid schema identifier: {schema!r}")
        self._pool = pool
        self._schema = schema

    @staticmethod
    def _ts(seconds: float) -> datetime:
        """Float epoch seconds to the aware UTC datetime asyncpg binds to timestamptz."""
        return datetime.fromtimestamp(seconds, tz=UTC)

    async def _claim(self, kind: str, identifier: str, expires_at: float, *, now: float) -> bool:
        """Atomic first-wins insert; False when a live row already holds the key."""
        async with self._pool.acquire() as conn:
            await conn.execute(self._PURGE_SQL.format(schema=self._schema), self._ts(now))
            claimed = await conn.fetchrow(
                self._CLAIM_SQL.format(schema=self._schema),
                kind,
                identifier,
                self._ts(expires_at),
                self._ts(now),
            )
        return claimed is not None

    async def consume(self, assertion_id: str, expires_at: float, *, now: float) -> None:
        if not await self._claim(_ASSERTION_REPLAY_KIND, assertion_id, expires_at, now=now):
            raise ValueError("SAML assertion replayed")

    async def already_answered(self, request_id: str, *, now: float) -> bool:
        async with self._pool.acquire() as conn:
            answered = await conn.fetchval(
                self._HOLDS_SQL.format(schema=self._schema),
                _ANSWERED_REQUEST_KIND,
                request_id,
                self._ts(now),
            )
        return answered is not None

    async def record_answered(self, request_id: str, ttl: int, *, now: float) -> None:
        await self._claim(_ANSWERED_REQUEST_KIND, request_id, now + ttl, now=now)


def _replay_store_for(request: Request, fallback: _SamlReplayStore) -> _SamlReplayStore:
    """The shared Postgres store when the app carries the admin pool, else *fallback*.

    The admin app always populates ``app.state.pg_pool`` and
    ``app.state.schema`` (``setup_admin_state``) before its first request, so
    every SAML callback served through it shares one store. Bare apps --
    embedders and the test fixtures -- carry neither key and keep the
    bundle's in-process store.
    """
    pool = getattr(request.app.state, "pg_pool", None)
    schema = getattr(request.app.state, "schema", None)
    if pool is not None and isinstance(schema, str):
        return _PostgresSamlReplayStore(pool, schema)
    return fallback


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
    there is enforced by the shared answered-request store instead.

    Deliberately STILL in-process when the replay and answered gates moved to
    Postgres, for three reasons. First, locality here fails CLOSED: the set
    is consulted only on the opt-in cookie-less fallback, where a missing ID
    REFUSES, so a sibling process that never saw the /login rejects more,
    never less -- no replay window opens (the cookie path, the default, never
    reads this set at all; its ID comes back inside the same browser's
    correlation cookie). Second, /login is unauthenticated: moving this map
    into the shared database would convert unauthenticated /login spam into
    unbounded database writes against the very store the real gates need
    cheap, while the bounded set caps the flood in process. Third, the
    flood-eviction behavior is pinned by
    test_pending_set_flood_eviction_cannot_break_the_cookie_bound_login. The
    cost is a documented deployment limit, not a replay hazard: a
    cookie-blocked browser behind a multi-replica deployment needs the
    callback served by the same process that issued the login (sticky
    sessions), because the fallback is a weaker, opt-in binding whose
    remaining gate is exactly "one process, one live spend".
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
    # The replay + answered gates are shared through Postgres on every app
    # that carries the admin pool (resolved per request, see
    # _replay_store_for); this bundle-local instance is the fallback for bare
    # apps without one. The pending map below is in-process by design.
    in_process_replay_store = _InProcessSamlReplayStore()
    pending_requests = _PendingAuthnRequests()
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
            # Shared when the app carries the admin pool (every replica then
            # reads and writes the same rows), the bundle-local in-process
            # store otherwise.
            replay_store = _replay_store_for(request, in_process_replay_store)

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
                # replica or worker process that never saw the login
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
                if await replay_store.already_answered(in_response_to, now=time.time()):
                    raise ValueError("SAML response answers an already-answered AuthnRequest")
                pending_requests.discard(in_response_to)
            elif config.allow_cookieless_fallback:
                # Opt-in fallback: the validated InResponseTo must name an
                # AuthnRequest this process issued and has not spent -- the
                # spend IS the single-use gate on this path. Nothing binds
                # the response to the browser posting it (login CSRF)
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
            await replay_store.record_answered(
                in_response_to, ttl=_REQUEST_MAX_AGE, now=time.time()
            )

            # An assertion whose InResponseTo is absent (or otherwise valid but
            # captured) can be re-POSTed while its window is live; only a
            # consumed-ID record refuses the second presentation.
            assertion_id = auth.get_last_assertion_id()
            if not isinstance(assertion_id, str) or not assertion_id:
                raise ValueError("SAML response carries no assertion ID")
            now = time.time()
            not_on_or_after = auth.get_last_assertion_not_on_or_after()
            assertion_expires_at = (
                not_on_or_after
                if not_on_or_after is not None
                else now + _REPLAY_FALLBACK_TTL_SECONDS
            )
            await replay_store.consume(assertion_id, assertion_expires_at, now=now)

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
