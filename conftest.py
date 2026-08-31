"""Repository-root pytest configuration.

Holds the e2e-tier collection gate and the outbound-network guard. Both MUST
live at the root: conftest files are registered as their directories are
visited, so a gate in tests/conftest.py registers too late to reliably stop the
tier from being collected on every invocation shape, and ``pytest_configure`` /
``pytest_terminal_summary`` only fire for a root-level plugin.
"""

import functools
import importlib.util
import ipaddress
import socket
from collections.abc import Iterator
from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help=(
            "Collect the containerized e2e tier (tests/e2e). Off by default; "
            "requires the e2e dependency group (uv sync --group e2e) and Docker."
        ),
    )


def _is_e2e_path(path: Path) -> bool:
    return path.name == "e2e" and path.parent.name == "tests"


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Keep the e2e tier out of directory recursion unless ``--e2e`` is passed.

    A command-line ``-m`` REPLACES the addopts marker expression instead of
    combining with it, so a marker-only gate (``-m "not e2e"`` in addopts)
    silently opened the tier to every ``-m "not integration"`` /
    ``-m "not redis"`` run. Ignoring the directory at collection is
    independent of ``-m``.
    """
    if config.getoption("--e2e"):
        return None
    if _is_e2e_path(collection_path):
        return True
    return None


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Explicit-arg backstop for the e2e gate.

    ``pytest_ignore_collect`` is not consulted for paths passed explicitly
    on the command line (``pytest tests/e2e``), so an explicit-arg run
    without ``--e2e`` still collects the tier. Drop those items here.
    """
    if config.getoption("--e2e"):
        return
    deselected = [
        item
        for item in items
        if item.path.parent.name == "e2e" and item.path.parent.parent.name == "tests"
    ]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = [item for item in items if item not in deselected]


# ── Outbound-network guard ──────────────────────────────────────────────
# Every lane in this repo is meant to stay on this machine. The integration,
# redis and e2e tiers reach Postgres and Dragonfly through testcontainers, which
# publishes them on loopback; the unit tier reaches nothing at all. Nothing here
# is supposed to call a real third-party service. Until this guard existed
# nothing enforced that, so a mock that stopped matching failed OPEN: the
# request left the box and whatever answered was treated as the fixture.
#
# The guard is suite-wide, with NO marker allowlist. That is a decision about
# TaskQ's actual lanes rather than a copy of another repo's: `integration` and
# `redis` mean testcontainers, `e2e` means containers plus an in-process client,
# and all three reach their infrastructure on addresses the guard already
# permits. Exempting them would buy nothing and would leave the tiers with the
# most machinery — and so the most room for a mock to stop matching — as the
# only unguarded ones.
#
# This lives in the root conftest rather than tests/conftest.py because
# `pytest_configure`, `pytest_terminal_summary` and the xdist node hooks only
# fire for a root-level plugin, and the guard's parts belong together.

_BLOCKED_ATTEMPTS: list[tuple[str, str]] = []


class OutboundNetworkBlockedError(RuntimeError):
    """A test tried to open a connection to the public internet."""


def _is_local_literal(host: str) -> bool:
    """Whether an address literal stays on this machine or its private networks."""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    # Multicast is NOT `is_global == False`: 224.0.0.251 (mDNS), 239.255.255.250
    # (SSDP) and ff02::fb all report is_global True while never leaving the
    # segment. Blocking them would break local service discovery.
    return not ip.is_global or ip.is_multicast


@functools.lru_cache(maxsize=512)
def _resolves_only_locally(host: str) -> bool:
    """Whether every address *host* resolves to is local.

    Cached because this runs on EVERY connect: a client that retries asks the
    same question hundreds of times in one test, and each miss is a real DNS
    round trip.

    A name that cannot be resolved counts as local: it cannot reach anything, so
    it fails on its own with a clearer error than this guard would give.
    """
    try:
        resolved = socket.getaddrinfo(host, None)
    except OSError:
        return True
    return all(_is_local_literal(info[4][0]) for info in resolved if isinstance(info[4][0], str))


def _is_local_target(address: object) -> bool:
    """Whether *address* is on this machine or one of its private networks.

    The rule is "not globally routable", NOT "loopback". Docker is the reason: a
    container reached directly sits on a bridge address (172.17/16 by default),
    a user-defined compose network lands anywhere in 172.16/12,
    ``host.docker.internal`` resolves to a private address, and a service on the
    LAN is 10/8 or 192.168/16. A loopback-only rule blocks every one of those
    and breaks testcontainers, compose services and CI service containers, which
    is the opposite of the point: this exists to stop tests reaching the
    INTERNET, not to stop them reaching their own infrastructure.

    Hostnames are resolved before classifying, because ``socket.connect``
    accepts them and a compose service is usually reached by name (``db``,
    ``postgres``).

    Unix sockets (a plain path, so ``str`` rather than ``tuple``) and unfamiliar
    address families are local by the same reasoning. That is what lets the
    testcontainers fixtures talk to /var/run/docker.sock and what lets
    HealthServer bind its AF_UNIX socket.
    """
    if not isinstance(address, tuple) or not address:
        return True
    host = address[0]
    if not isinstance(host, str) or not host:
        return True
    return _is_local_literal(host) or _resolves_only_locally(host)


def _blocked_message(nodeid: str, address: object, via: str) -> str:
    """The failure text a developer sees, naming cause and remedy."""
    return (
        f"Blocked outbound connection to {address!r} from {nodeid} (via {via}).\n"
        "TaskQ's test lanes are hermetic: everything real runs on this machine "
        "(testcontainers Postgres/Dragonfly, local stub servers, unix sockets), "
        "and nothing calls a live third-party service.\n"
        "Most likely cause: an HTTP mock stopped matching, so the call fell "
        "through to the real endpoint. See `pytest_configure` in this file for "
        "the httpx/httpx2 case that makes respx silently stop intercepting.\n"
        "Second most likely cause: a test fake points at a routable domain "
        "(example.com, login.microsoftonline.com), so a mock miss reaches a real "
        "server instead of failing. Point fakes at an unroutable name "
        "(RFC 2606 .invalid).\n"
        "If the call is genuinely deliberate, do not re-mark the test to dodge "
        "this: TaskQ has no lane that is allowed to leave the machine, so make "
        "that case explicitly by adding an allowance here with a reason."
    )


@pytest.fixture(autouse=True)
def _no_outbound_network(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail any test that opens a connection to the public internet.

    Worked example of why this is structural rather than per-test hygiene.
    ``taskq[oidc]`` installs BOTH ``httpx`` and ``httpx2``:
    ``src/taskq/web/admin/auth/oidc.py`` does ``import httpx2 as httpx`` for the
    discovery and JWKS fetches, while authlib's ``AsyncOAuth2Client`` subclasses
    ``httpx.AsyncClient`` for the token exchange. ``respx`` patches ``httpx``
    only, so ``tests/test_sso_oidc.py`` has to bridge the gap by monkeypatching
    ``httpx2.AsyncClient`` to ``httpx.AsyncClient``. Drop, rename or narrow that
    bridge and respx stops intercepting, with no error of its own.

    In a sibling repo the identical shape (authlib >= 1.8 prefers ``httpx2``
    whenever it is importable) sent unit-lane traffic to the real Microsoft
    Entra endpoint for months while the suite stayed green, because the fake
    issuer was a real routable domain and the live error response satisfied the
    test's assertion.

    So: block at the socket. That is independent of whichever mocking library a
    test happens to use, and it fails at the call site instead of becoming a
    confusing assertion about a response nobody expected to be real.

    Everything not globally routable stays open. See `_is_local_target`.

    The originals are captured HERE rather than at import: another fixture may
    legitimately have patched ``socket.connect`` for this test (chaos injection,
    a stub transport), and restoring the module-level original on the way out
    would silently discard that patch instead of the one this fixture installed.
    """
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def _guard(self: socket.socket, address: object, *args: object) -> object:
        if _is_local_target(address):
            return real_connect(self, address, *args)  # type: ignore[arg-type]
        _BLOCKED_ATTEMPTS.append((request.node.nodeid, repr(address)))
        raise OutboundNetworkBlockedError(_blocked_message(request.node.nodeid, address, "connect"))

    def _guard_ex(self: socket.socket, address: object, *args: object) -> object:
        if _is_local_target(address):
            return real_connect_ex(self, address, *args)  # type: ignore[arg-type]
        _BLOCKED_ATTEMPTS.append((request.node.nodeid, repr(address)))
        raise OutboundNetworkBlockedError(
            _blocked_message(request.node.nodeid, address, "connect_ex")
        )

    socket.socket.connect = _guard  # type: ignore[method-assign]
    socket.socket.connect_ex = _guard_ex  # type: ignore[method-assign]
    try:
        yield
    finally:
        socket.socket.connect = real_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = real_connect_ex  # type: ignore[method-assign]


def _installed_http_stacks() -> list[str]:
    """Which of the two mutually-invisible httpx stacks are importable."""
    return [name for name in ("httpx", "httpx2") if importlib.util.find_spec(name) is not None]


def pytest_configure(config: pytest.Config) -> None:
    """Warn at session start when more than one HTTP stack is installed.

    Two importable stacks is the precondition for a mock that silently stops
    applying: respx patches ``httpx``'s transport and cannot see ``httpx2`` at
    all, so any code path that reaches for ``httpx2`` runs unmocked while the
    test still reads as mocked. It is a warning and not a failure because
    ``taskq[oidc]`` legitimately needs both — authlib is on ``httpx`` and
    ``taskq.web.admin.auth.oidc`` is on ``httpx2``. The point is that the
    condition is visible at session start rather than discovered later through a
    live API call.
    """
    stacks = _installed_http_stacks()
    if len(stacks) > 1:
        config.issue_config_time_warning(
            pytest.PytestConfigWarning(
                f"Multiple HTTP stacks installed ({', '.join(stacks)}). respx patches "
                "httpx only and cannot intercept httpx2, so a test can look mocked while "
                "calling out for real. tests/test_sso_oidc.py bridges the two with a "
                "monkeypatch; tests/test_suite_hygiene.py pins that the bridge is still "
                "needed and still present. The outbound-network guard in conftest.py is "
                "the backstop."
            ),
            stacklevel=2,
        )


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Hand this xdist worker's blocked attempts back to the controller.

    ``_BLOCKED_ATTEMPTS`` is per-process, so under ``-n`` every block happens in
    a worker and the controller's own list stays empty. ``workeroutput`` is the
    sanctioned channel back; it is absent on the controller and in serial runs,
    where the list is already the right one.
    """
    output = getattr(session.config, "workeroutput", None)
    if output is not None:
        output["taskq_blocked_attempts"] = _BLOCKED_ATTEMPTS


def pytest_testnodedown(node: object, error: object) -> None:
    """Collect a finished xdist worker's blocked attempts on the controller."""
    del error
    forwarded = getattr(node, "workeroutput", {}).get("taskq_blocked_attempts") or []
    _BLOCKED_ATTEMPTS.extend((str(nodeid), str(address)) for nodeid, address in forwarded)


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    """List every connection the guard blocked this session.

    Collects the scattered per-test failures into one place, including the ones
    that happened inside xdist workers (see `pytest_sessionfinish`).
    """
    if not _BLOCKED_ATTEMPTS:
        return
    terminalreporter.section("outbound connections blocked", red=True)
    for nodeid, address in _BLOCKED_ATTEMPTS:
        terminalreporter.line(f"  {nodeid} -> {address}")
