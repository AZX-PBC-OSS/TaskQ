"""Unit pins for the OIDC discovery/JWKS TTL cache.

The cache lives behind :func:`create_oidc_auth`; these tests exercise the
class directly with stub fetchers so TTL expiry, bounded eviction, and the
invalidation-on-fetch-failure rule are pinned without a mocked IdP. The
end-to-end pin (one discovery fetch across login + callback) lives in
``tests/test_sso_oidc.py``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

pytest.importorskip("fastapi")

from taskq.web.admin.auth.oidc import (
    _OIDCMetadataCache,  # pyright: ignore[reportPrivateUsage]  # Why: the cache is the behaviour under test and is not re-exported.
)
from tests._ns_patch import module_ns_proxy


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


class _FetchCounter:
    """Stub fetcher returning a per-call payload and counting invocations."""

    def __init__(self, payloads: list[dict[str, Any]] | None = None) -> None:
        self.calls = 0
        self.payloads = payloads or [{"n": 0}]
        self.fail_next = False

    async def __call__(self) -> dict[str, Any]:
        self.calls += 1
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("idp unreachable")
        return {**self.payloads[min(self.calls - 1, len(self.payloads) - 1)], "call": self.calls}


def test_discovery_is_fetched_once_and_reused_within_the_ttl() -> None:
    cache = _OIDCMetadataCache()
    fetch = _FetchCounter([{"authorization_endpoint": "a"}, {"authorization_endpoint": "b"}])

    first = _run(cache.get_discovery("https://idp.invalid", fetch))
    second = _run(cache.get_discovery("https://idp.invalid", fetch))

    assert first == {"authorization_endpoint": "a", "call": 1}
    assert second == first, "second read within the TTL refetched the discovery document"
    assert fetch.calls == 1


def test_cache_is_keyed_per_issuer() -> None:
    cache = _OIDCMetadataCache()
    fetch = _FetchCounter([{"issuer": "one"}, {"issuer": "two"}])

    a = _run(cache.get_discovery("https://a.invalid", fetch))
    b = _run(cache.get_discovery("https://b.invalid", fetch))

    assert a["issuer"] == "one"
    assert b["issuer"] == "two"
    assert fetch.calls == 2


def test_stale_entries_are_refetched(monkeypatch: pytest.MonkeyPatch) -> None:
    import taskq.web.admin.auth.oidc as oidc_module

    cache = _OIDCMetadataCache(ttl_seconds=300)
    fetch = _FetchCounter([{"fresh": True}, {"rotated": True}])
    now = {"t": 1000.0}
    # Patch where the name is LOOKED UP - the oidc module's own ``time``
    # binding - not through to the global time module (tests/_ns_patch.py).
    monkeypatch.setattr(oidc_module, "time", module_ns_proxy(time, monotonic=lambda: now["t"]))

    assert _run(cache.get_discovery("https://idp.invalid", fetch))["fresh"] is True
    now["t"] += 301.0  # past the TTL window
    assert _run(cache.get_discovery("https://idp.invalid", fetch))["rotated"] is True
    assert fetch.calls == 2


def test_a_failed_jwks_refresh_invalidates_even_a_fresh_discovery_entry() -> None:
    """A refresh failure must not leave the entry cached as if healthy: the
    next read refetches from the IdP instead of serving a discovery document
    whose key-set refresh just failed."""
    cache = _OIDCMetadataCache(ttl_seconds=300)
    discovery_fetch = _FetchCounter([{"first": True}, {"second": True}])
    jwks_fetch = _FetchCounter([{"keys": [1]}])
    issuer = "https://idp.invalid"

    _run(cache.get_discovery(issuer, discovery_fetch))

    jwks_fetch.fail_next = True
    with pytest.raises(RuntimeError, match="idp unreachable"):
        _run(cache.get_jwks(issuer, jwks_fetch))

    assert _run(cache.get_discovery(issuer, discovery_fetch))["second"] is True
    assert discovery_fetch.calls == 2


def test_a_failed_discovery_refresh_on_a_stale_entry_leaves_nothing_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale entry whose refresh fails is gone: the next read starts from a
    fresh fetch and serves the NEW document, never the stale one."""
    import taskq.web.admin.auth.oidc as oidc_module

    cache = _OIDCMetadataCache(ttl_seconds=300)
    fetch = _FetchCounter([{"stale": True}, {"rotated": True}])
    issuer = "https://idp.invalid"
    now = {"t": 1000.0}
    # Patch where the name is LOOKED UP - the oidc module's own ``time``
    # binding - not through to the global time module (tests/_ns_patch.py).
    monkeypatch.setattr(oidc_module, "time", module_ns_proxy(time, monotonic=lambda: now["t"]))

    assert _run(cache.get_discovery(issuer, fetch))["stale"] is True
    now["t"] += 301.0
    fetch.fail_next = True
    with pytest.raises(RuntimeError, match="idp unreachable"):
        _run(cache.get_discovery(issuer, fetch))
    assert _run(cache.get_discovery(issuer, fetch))["rotated"] is True
    assert fetch.calls == 3


def test_the_cache_is_bounded_by_max_entries() -> None:
    """A flood of distinct issuers cannot grow the cache without bound."""
    cache = _OIDCMetadataCache(max_entries=2)
    fetch = _FetchCounter()

    _run(cache.get_discovery("https://a.invalid", fetch))
    _run(cache.get_discovery("https://b.invalid", fetch))
    _run(cache.get_discovery("https://c.invalid", fetch))

    assert len(cache._entries) == 2  # pyright: ignore[reportPrivateUsage]  # Why: the bound is the behaviour under test.


def test_jwks_is_cached_alongside_its_discovery_document() -> None:
    cache = _OIDCMetadataCache()
    discovery_fetch = _FetchCounter([{"jwks_uri": "https://idp.invalid/jwks"}])
    jwks_fetch = _FetchCounter([{"keys": [1]}, {"keys": [1, 2]}])
    issuer = "https://idp.invalid"

    _run(cache.get_discovery(issuer, discovery_fetch))
    first = _run(cache.get_jwks(issuer, jwks_fetch))
    second = _run(cache.get_jwks(issuer, jwks_fetch))

    assert first == second
    assert jwks_fetch.calls == 1, "JWKS was refetched within the same cache window"
    assert discovery_fetch.calls == 1
