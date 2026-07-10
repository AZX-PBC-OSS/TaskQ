"""Bearer-token auth dependency for machine-to-machine access.

Suitable for Prometheus scrapers, kubelet probes, CI scripts, and other
non-interactive clients where an OIDC/SAML login flow is impractical.
"""

import hmac
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

__all__ = ["token_auth"]

_bearer_scheme = HTTPBearer(auto_error=False)


def token_auth(expected_token: str) -> Callable[..., Any]:
    """Build a FastAPI dependency that validates a bearer token.

    Raises :class:`ValueError` if *expected_token* is empty — an empty token
    would accept any request, which is a fail-open misconfiguration.
    """
    if not expected_token:
        raise ValueError("expected_token must be a non-empty string")

    async def _dependency(
        credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
    ) -> str:
        if credentials is None or not hmac.compare_digest(credentials.credentials, expected_token):
            raise HTTPException(status_code=401, detail="invalid or missing bearer token")
        return "authenticated"

    return _dependency
