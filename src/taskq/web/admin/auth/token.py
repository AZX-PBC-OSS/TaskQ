"""Bearer-token auth dependency for machine-to-machine access.

Suitable for Prometheus scrapers, kubelet probes, CI scripts, and other
non-interactive clients where an OIDC/SAML login flow is impractical.
"""

import hmac
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

__all__ = ["token_auth"]

_bearer_scheme = HTTPBearer(auto_error=False)


def token_auth(expected_token: str) -> Callable[..., Any]:
    """Build a FastAPI dependency that validates a bearer token.

    Raises :class:`ValueError` if *expected_token* is empty, an empty token
    would accept any request, which is a fail-open misconfiguration.

    The returned callable carries a ``session_verifier`` attribute -- the
    re-check long-lived SSE streams re-invoke (#316) -- so a router built with
    this dependency re-validates the live request's bearer token at every
    keepalive tick and before every streamed event.
    """
    if not expected_token:
        raise ValueError("expected_token must be a non-empty string")

    async def _dependency(
        credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
    ) -> str:
        if credentials is None or not hmac.compare_digest(credentials.credentials, expected_token):
            raise HTTPException(status_code=401, detail="invalid or missing bearer token")
        return "authenticated"

    async def _verify(request: Request) -> bool:
        # The same check the dependency runs, reading the request the stream
        # arrived on: scheme and token must both match. No round trip, so a
        # per-tick re-check costs one header parse and one compare.
        authorization = request.headers.get("Authorization", "")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            return False
        return hmac.compare_digest(token.strip(), expected_token)

    cast_to_any: Any = _dependency
    cast_to_any.session_verifier = _verify
    return _dependency
