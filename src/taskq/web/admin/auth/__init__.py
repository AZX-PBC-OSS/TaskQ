"""SSO auth provider system for the TaskQ admin UI.

Public surface:

* :class:`AuthBundle`, :class:`IdentityClaims` — shared, protocol-agnostic.
* :func:`create_oidc_auth`, :class:`OIDCAuthConfig`, :class:`OIDCTokenContext`
  — OIDC backend (``taskq[oidc]``).
* :func:`create_saml_auth`, :class:`SAMLAuthConfig` — SAML backend
  (``taskq[saml]``).

Importing ``oidc.py``/``saml.py`` never requires ``authlib`` or
``python3-saml`` — those heavy deps are imported lazily *inside*
:func:`create_oidc_auth`/:func:`create_saml_auth` themselves, each raising a
clear :class:`ImportError` with install instructions the first time they're
actually called without the matching extra installed. So this package is
always importable, and so are ``create_oidc_auth``/``create_saml_auth`` as
plain re-exports below — the ImportError (if any) surfaces on first call, not
on import.
"""

from taskq.web.admin.auth._session import (
    AuthBundle,
    IdentityClaims,
    SessionManager,
    create_auth_dependency,
)
from taskq.web.admin.auth.oidc import OIDCAuthConfig, OIDCTokenContext, create_oidc_auth
from taskq.web.admin.auth.saml import SAMLAuthConfig, create_saml_auth
from taskq.web.admin.auth.token import token_auth

__all__ = [
    "AuthBundle",
    "IdentityClaims",
    "OIDCAuthConfig",
    "OIDCTokenContext",
    "SAMLAuthConfig",
    "SessionManager",
    "create_auth_dependency",
    "create_oidc_auth",
    "create_saml_auth",
    "token_auth",
]
