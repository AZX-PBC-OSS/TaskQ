"""SAML SSO backend (OneLogin python3-saml toolkit; for legacy IdPs).

``python3-saml`` binds to the system ``libxmlsec1`` C library.  The import is
guarded so :mod:`taskq.web.admin.auth` never crashes when the ``[saml]`` extra
is absent; a clear :class:`ImportError` with install instructions is raised
when :func:`create_saml_auth` is called without the extra.
"""

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
    "SAMLAuthConfig",
    "create_saml_auth",
]

logger = structlog.get_logger("taskq.web.admin.auth.saml")


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

    session_manager = SessionManager(
        secret=config.session_secret,
        max_age_seconds=config.session_max_age_seconds,
        secure_cookie=config.secure_cookie,
    )
    login_path = f"{base_path}/login"
    settings_dict = _build_settings(config)
    router = APIRouter(tags=["sso-saml"])

    @router.get("/login")
    async def login(request: Request) -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        try:
            auth = OneLogin_Saml2_Auth(_request_data(request), settings_dict)
            sso_url = auth.login(return_to=base_path or "/")
            return RedirectResponse(url=sso_url, status_code=302)
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
            form = await request.form()
            post_data: dict[str, str] = {}
            for key, value in form.multi_items():
                if isinstance(value, str):
                    post_data[key] = value
            auth = OneLogin_Saml2_Auth(_request_data(request, post_data=post_data), settings_dict)
            auth.process_response()
            if auth.get_errors():
                raise ValueError(auth.get_last_error_reason() or "SAML response validation failed")
            if not auth.is_authenticated():
                raise ValueError("not authenticated")

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
            return response
        except Exception:
            logger.exception("saml-callback-error")
            return _error_redirect(base_path)

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
