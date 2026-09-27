"""The partition campaign's weather engine: REAL network weather via toxiproxy.

The tiers below this one simulate dependency states (``CLIENT PAUSE``, a
killed container, a stubbed exception). A network partition is none of
those: packets are ACCEPTED by the OS and never delivered, or delivered
late, and every client budget - asyncpg's connect/command timeouts,
redis-py's socket timeouts, the SSE bridge's broker-read deadline - is
exercised against the actual wire, not a stub's idea of it.

Topology: one toxiproxy server container on the host network (a Linux
CI box shares the daemon's network namespace, so the proxy's API and
every proxy listener are ordinary localhost ports, and the upstreams are
the shared containers' PUBLISHED host ports - the same endpoints the
test process itself uses). Workers and clients that must take the
weather connect to a proxy listener; everything else keeps the direct
endpoint, which is how one worker can be partitioned while its sibling
and the client stay healthy - a per-worker cut is impossible to express
with a container-level outage (``killable_redis_container`` et al. cut
EVERYBODY).

Weather vocabulary (toxiproxy has no "blackhole" type; ``timeout`` with
``timeout: 0`` IS the blackhole - data is accepted and never released,
the connection stays ESTABLISHED, and toxics added mid-flight apply to
existing connections, which a container restart cannot do):

* :meth:`PartitionProxy.blackhole` - packets accepted, never delivered,
  both directions, existing connections included: the true partition.
* :meth:`PartitionProxy.hold_then_close` - data held for ``hold_ms``
  then the connection closed: the SYN-ok/data-dropped shape with a
  hard end, which turns a silent hang into an honest connection error.
* :meth:`PartitionProxy.latency` - added latency both directions, the
  slowdown that must trip every connection budget honestly.

House rules honored here: the container carries ``creator_labels()`` (a
crashed run's leftovers stay sweepable once the owner pids die - Ryuk is
disabled process-wide by the shared-container machinery), every weather
call records the toxics it added so the test's ``finally`` can clear
exactly those (no residue across tests sharing the server), and no
helper ever sleeps: weather is applied and removed by API call, the
tests own every deadline.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import urllib.error
import urllib.request
from typing import Any, cast
from urllib.parse import urlparse, urlunparse

from taskq.testing._shared_containers import (
    creator_labels,  # pyright: ignore[reportPrivateUsage]  # Why: the ownership-label contract is the shared machinery's; a second copy would drift.
)

_TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0"

#: The toxiproxy API's own timeout: a control-plane call that outlives this
#: is a wedged server, fail loud rather than strand the test's finally.
_API_TIMEOUT_S = 10.0


def free_port() -> int:
    """A free TCP port on localhost, allocated the port-0 way.

    Deliberately racy (the port is free only until the proxy binds it):
    callers allocate and bind within the same fixture setup, on a machine
    where the only port contention is the run's own containers.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def dsn_host_port(dsn: str) -> tuple[str, int]:
    """The host and PUBLISHED port of a postgres DSN - the endpoint the
    test process itself reaches the dependency on, which (host networking)
    is exactly the upstream endpoint the toxiproxy server can reach."""
    parsed = urlparse(dsn)
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"dsn has no host:port: {dsn!r}")
    return parsed.hostname, parsed.port


def proxied_dsn(pg_dsn: str, proxy: PartitionProxy) -> str:
    """The module's DSN re-pointed at a proxy listener: same credentials,
    same database path, the host:port swapped for the listener. The proxy
    forwards to the shared container's published port, so the wire the
    worker speaks is byte-identical - only the weather differs."""
    parsed = urlparse(pg_dsn)
    userinfo = f"{parsed.username}:{parsed.password}@" if parsed.username else ""
    return urlunparse(
        parsed._replace(  # Why: namedtuple._replace is the sanctioned API, the same shape _harness.scoped_dsn uses.
            netloc=f"{userinfo}{proxy.url_host}:{proxy.port}"
        )
    )


def _api_call(base_url: str, path: str, method: str, body: object | None = None) -> Any:
    """One synchronous toxiproxy API round trip, JSON in and out.

    Raises with the response BODY on an HTTP error - toxiproxy's error
    bodies (``{"error": "invalid toxic type"}``) are the only way to debug
    a mis-shaped toxic, and urllib's bare status line hides them.
    """
    data = json.dumps(body).encode() if body is not None else None
    # S310's audit is satisfied by construction: the URL is this module's
    # own loopback base_url (handed out by start_toxiproxy) plus a
    # validated path - no untrusted part, no scheme but http can appear.
    request = urllib.request.Request(  # noqa: S310
        f"{base_url}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        raw = urllib.request.urlopen(request, timeout=_API_TIMEOUT_S).read().decode()  # noqa: S310
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"toxiproxy API {method} {path} failed: {exc.read().decode()}") from exc
    return json.loads(raw) if raw else None


class PartitionProxy:
    """One named toxiproxy listener: an address clients dial and an
    upstream they are partitioned from. The toxic ledger is per-proxy so
    two proxies on one server (a cut worker and a healthy sibling) never
    clear each other's weather."""

    def __init__(self, base_url: str, name: str, host: str, port: int) -> None:
        self.base_url = base_url
        self.name = name
        self.host = host
        self.port = port
        self._toxics: list[str] = []
        self._lock = asyncio.Lock()

    @property
    def url_host(self) -> str:
        """The host component clients dial. The proxy binds loopback."""
        return "127.0.0.1"

    def _spawn(self, path: str, method: str, body: object | None = None) -> None:
        _api_call(self.base_url, path, method, body)

    def _add_toxic(
        self, name: str, toxic_type: str, stream: str, attributes: dict[str, int]
    ) -> None:
        self._spawn(
            f"/proxies/{self.name}/toxics",
            "POST",
            {
                "name": name,
                "type": toxic_type,
                "stream": stream,
                "toxicity": 1.0,
                "attributes": attributes,
            },
        )
        self._toxics.append(name)

    def _add_pair(self, stem: str, toxic_type: str, attributes: dict[str, int]) -> None:
        """One toxic per direction: toxiproxy applies a toxic to ONE link
        direction only, and weather that only swallows server->client
        bytes is a different (kinder) fault than a full cut."""
        self._add_toxic(f"{stem}_up", toxic_type, "upstream", attributes)
        self._add_toxic(f"{stem}_down", toxic_type, "downstream", attributes)

    async def blackhole(self) -> None:
        """The partition: packets accepted, never delivered, both
        directions, existing connections included. The connection stays
        established - a client that has no read/command budget waits
        forever, which is exactly the weather the budgets must survive."""
        async with self._lock:
            await asyncio.to_thread(self._add_pair, "blackhole", "timeout", {"timeout": 0})

    async def hold_then_close(self, hold_ms: int) -> None:
        """SYN ok, data dropped: data is held ``hold_ms`` then the
        connection is closed. The connect succeeds and every byte the
        client sends vanishes until the close turns the silent hang into
        an honest ``ConnectionDoesNotExistError``."""
        async with self._lock:
            await asyncio.to_thread(self._add_pair, "hold_close", "timeout", {"timeout": hold_ms})

    async def latency(self, latency_ms: int) -> None:
        """Added latency both directions - the slowdown every connection
        budget (connect, command, socket) must trip on honestly."""
        async with self._lock:
            await asyncio.to_thread(self._add_pair, "latency", "latency", {"latency": latency_ms})

    async def clear(self) -> None:
        """Remove exactly the toxics this proxy holds - the recovery arm
        of every scenario, and the finally-scoped residue cleanup."""
        async with self._lock:
            for name in list(self._toxics):
                await asyncio.to_thread(
                    self._spawn,
                    f"/proxies/{self.name}/toxics/{name}",
                    "DELETE",
                )
                self._toxics.remove(name)

    def clear_sync(self) -> None:
        """The synchronous form of :meth:`clear`: the session-scoped
        fixture's teardown runs outside any event loop, and a residue
        sweep must never be skipped because of that. Never raises - a
        wedged server cannot mask the teardown's own failure."""
        for name in list(self._toxics):
            with contextlib.suppress(Exception):
                self._spawn(f"/proxies/{self.name}/toxics/{name}", "DELETE")
            self._toxics.remove(name)


class Toxiproxy:
    """The running toxiproxy server: creates per-worker/per-role proxies
    on demand, each forwarding to a real dependency's published port."""

    def __init__(self, base_url: str, container: object) -> None:
        self.base_url = base_url
        self._container = container
        self._proxies: list[PartitionProxy] = []

    def create_proxy_sync(
        self, name: str, upstream_host: str, upstream_port: int
    ) -> PartitionProxy:
        """Create one listener synchronously (fixture/test setup runs on
        the event loop; the API round trip is a local HTTP call)."""
        listen_port = free_port()
        _api_call(
            self.base_url,
            "/proxies",
            "POST",
            {
                "name": name,
                "listen": f"127.0.0.1:{listen_port}",
                "upstream": f"{upstream_host}:{upstream_port}",
            },
        )
        proxy = PartitionProxy(self.base_url, name, "127.0.0.1", listen_port)
        self._proxies.append(proxy)
        return proxy

    async def clear_all(self) -> None:
        """Sweep every proxy this server created - the belt-and-braces
        cleanup a test's own finally backs up."""
        for proxy in self._proxies:
            await proxy.clear()

    def clear_all_sync(self) -> None:
        """The synchronous form of :meth:`clear_all` - the session
        fixture's teardown. Sweeps every proxy's toxic ledger so weather
        a test's own ``finally`` could not reach (a mid-``finally`` crash
        of its own, a failure between two of its clears) cannot outlive
        the test that armed it. Never raises."""
        for proxy in self._proxies:
            proxy.clear_sync()

    def stop(self) -> None:
        """Stop the server container. Never raises: a teardown failure
        must not mask the test's own failure."""
        with contextlib.suppress(Exception):
            self.clear_all_sync()
            self._proxies.clear()
            cast("Any", self._container).stop()


def start_toxiproxy() -> Toxiproxy:
    """Start one toxiproxy server on the host network and return its
    controller. Host networking: the API and every proxy listener are
    loopback ports of the machine the test process and the worker
    subprocesses already run on, and the upstreams are the shared
    containers' published host ports - no port publishing, no network
    attachment, nothing to tear down but the container itself."""
    from testcontainers.core.container import DockerContainer

    api_port = free_port()
    container = (
        DockerContainer(_TOXIPROXY_IMAGE)
        .with_command(f"-host 127.0.0.1 -port {api_port}")
        .with_kwargs(network_mode="host", labels=creator_labels())
    )
    container.start()
    return Toxiproxy(f"http://127.0.0.1:{api_port}", container)
