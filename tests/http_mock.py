"""respx, aimed at every httpcore stack installed, not just the deprecated one.

``taskq[oidc]`` installs TWO httpx stacks and one production code path uses both.
``src/taskq/web/admin/auth/oidc.py`` does ``import httpx2 as httpx`` for the
discovery and JWKS fetches, while authlib's ``AsyncOAuth2Client`` subclasses
whichever stack authlib's own version binds to (1.7.x hard-imports ``httpx``;
1.8 added ``integrations/httpx_client/_compat.py``, which prefers ``httpx2``
whenever it is importable).

Out of the box respx patches ``httpcore`` only, so it sees the httpx half of that
flow and silently misses the httpx2 half. The previous workaround was an autouse
fixture in tests/test_sso_oidc.py doing ``monkeypatch.setattr(httpx2,
"AsyncClient", httpx.AsyncClient)``. It made the mocks apply, but every OIDC test
then ran against a client class production never constructs: timeouts, redirect
handling, TLS verification, proxy handling and exception types were all exercised
on the wrong stack, and an httpx2-only regression could not be caught here.

respx has a documented extension point for exactly this - ``Mocker`` subclasses
register themselves by name and ``respx.mock(using=...)`` selects one. This
module registers a single mocker whose targets are httpcore's connection classes
AND httpcore2's, so ONE router with ONE route table covers both stacks at once.
That matters because the OIDC callback needs both mocked inside a single test.

Two details the bare "add httpcore2 targets" recipe omits:

* ``HTTPCoreMocker.from_*_httpx_response`` hard-codes ``httpcore.Response``.
  Each core only accepts its own response type, so the class has to follow the
  connection instance that is asking; :func:`_core_of` picks it.
* Interception happens at the connection pool, below the client, so anything
  built on a non-httpcore transport is untouched - ``AsyncClient(transport=
  ASGITransport(app))`` and starlette's ``TestClient`` keep driving their apps
  in-process rather than being swallowed by this router.

Every intercepted request is recorded with the stack that issued it, which is
what lets a test assert it exercised httpx2 rather than trusting that it did::

    with mock_http() as router:
        route = router.get(url).mock(return_value=httpx.Response(200))
        ...
        assert stacks_for(url) == {"httpx2"}

Not taking ``pytest-httpx2`` as a dependency: it registers an httpcore2-only
mocker, which would leave authlib's httpx call unmocked in the same test, and it
is a 1.0.0 release with a handful of commits. The mechanism it uses is the thirty
lines below.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType
from typing import Any, ClassVar

import httpcore
import httpx
import respx
from respx.mocks import HTTPCoreMocker
from respx.patterns import parse_url

__all__ = [
    "MultiCoreMocker",
    "StackCall",
    "installed_stacks",
    "mock_http",
    "stack_calls",
    "stacks_for",
]

_CORES: dict[str, ModuleType] = {"httpcore": httpcore}
try:  # pragma: no cover - depends on installed extras
    import httpcore2

    _CORES["httpcore2"] = httpcore2
except ImportError:  # pragma: no cover - httpx2 always ships httpcore2
    pass

# httpcore is what httpx talks to; httpcore2 is what httpx2 talks to. Assertions
# read better in terms of the client library a caller actually imports.
_CLIENT_STACK: Mapping[str, str] = {"httpcore": "httpx", "httpcore2": "httpx2"}


def installed_stacks() -> frozenset[str]:
    """Names of the httpx-compatible client stacks this mocker covers."""
    return frozenset(_CLIENT_STACK[core] for core in _CORES)


def _core_of(target: object) -> str:
    """Which core package a patched connection/pool instance belongs to."""
    return type(target).__module__.split(".", 1)[0]


@dataclass(frozen=True)
class StackCall:
    """One intercepted request and the client stack that issued it."""

    method: str
    url: str
    stack: str


_CALLS: list[StackCall] = []


class MultiCoreMocker(HTTPCoreMocker):
    """respx mocker patching every installed httpcore, not only the first.

    Registered under the name ``httpcore-multi``; select it with
    ``respx.mock(using="httpcore-multi")`` or, preferably, :func:`mock_http`.
    """

    name: ClassVar[str] = "httpcore-multi"
    targets: ClassVar[list[str]] = [
        target.replace("httpcore.", f"{core}.", 1)
        for core in _CORES
        for target in HTTPCoreMocker.targets
    ]

    @classmethod
    def from_sync_httpx_response(
        cls, httpx_response: httpx.Response, target: object, **kwargs: Any
    ) -> Any:
        """Build the reply with the core module of the connection that asked.

        An ``httpcore2`` pool rejects an ``httpcore.Response`` and vice versa,
        so the type follows the target rather than being fixed at import time.
        """
        core_name = _core_of(target)
        request: Any = kwargs["request"]
        method: Any = request.method
        _CALLS.append(
            StackCall(
                method=method.decode("ascii") if isinstance(method, bytes) else str(method),
                url=str(
                    parse_url(
                        (
                            request.url.scheme,
                            request.url.host,
                            request.url.port,
                            request.url.target,
                        )
                    )
                ),
                stack=_CLIENT_STACK[core_name],
            )
        )
        return _CORES[core_name].Response(
            status=httpx_response.status_code,
            headers=httpx_response.headers.raw,
            content=httpx_response.stream,
            extensions=httpx_response.extensions,
        )

    @classmethod
    async def from_async_httpx_response(
        cls, httpx_response: httpx.Response, target: object, **kwargs: Any
    ) -> Any:
        return cls.from_sync_httpx_response(httpx_response, target, **kwargs)


def stack_calls() -> tuple[StackCall, ...]:
    """Every request intercepted since the current :func:`mock_http` started."""
    return tuple(_CALLS)


def stacks_for(url: str, method: str = "GET") -> set[str]:
    """Client stacks that issued *method* *url* while the mock was active.

    Query strings are ignored so callers need not spell out every parameter.
    An empty set means the request never happened.
    """
    wanted = url.split("?", 1)[0]
    return {
        call.stack
        for call in _CALLS
        if call.method.upper() == method.upper() and call.url.split("?", 1)[0] == wanted
    }


@contextmanager
def mock_http(*, assert_all_called: bool = False) -> Generator[respx.MockRouter]:
    """A respx router that intercepts httpx and httpx2 through one route table."""
    _CALLS.clear()
    with respx.mock(using=MultiCoreMocker.name, assert_all_called=assert_all_called) as router:
        yield router
