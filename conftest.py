"""Repository-root pytest configuration.

Holds the e2e-tier collection gate and the outbound-network guard. Both MUST
live at the root: conftest files are registered as their directories are
visited, so a gate in tests/conftest.py registers too late to reliably stop the
tier from being collected on every invocation shape, and ``pytest_configure`` /
``pytest_terminal_summary`` only fire for a root-level plugin.
"""

import faulthandler
import functools
import ipaddress
import os
import socket
import sys
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
    parser.addoption(
        "--otel-validation",
        action="store_true",
        default=False,
        help=(
            "Collect the OTLP validation lane (tests/otel_validation). Off by "
            "default; requires Docker and a collector container image."
        ),
    )


#: Directory names of the opt-in containerized tiers, each gated the way the
#: e2e tier is (collection ignored unless the tier's flag is passed).
_OPT_IN_TIERS: tuple[tuple[str, str], ...] = (
    ("e2e", "--e2e"),
    ("otel_validation", "--otel-validation"),
)


def _opt_in_tier_dir(path: Path) -> str | None:
    """The flag that gates *path* when it IS a tier directory, else None."""
    for dirname, flag in _OPT_IN_TIERS:
        if path.name == dirname and path.parent.name == "tests":
            return flag
    return None


def _opt_in_tier_file(path: Path) -> str | None:
    """The flag that gates *path* when it is a file inside a tier, else None."""
    for dirname, flag in _OPT_IN_TIERS:
        if path.parent.name == dirname and path.parent.parent.name == "tests":
            return flag
    return None


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Keep the opt-in container tiers out of directory recursion unless their
    flag is passed.

    A command-line ``-m`` REPLACES the addopts marker expression instead of
    combining with it, so a marker-only gate (``-m "not e2e"`` in addopts)
    silently opened the tier to every ``-m "not integration"`` /
    ``-m "not redis"`` run. Ignoring the directory at collection is
    independent of ``-m``.
    """
    # The directory only, never the files inside it: an explicitly passed
    # tier directory (``pytest tests/e2e``) must reach the backstop below,
    # which REPORTS what it strips through pytest_deselected — ignoring the
    # files at collection would hide the stripped tier from the summary.
    tier_flag = _opt_in_tier_dir(collection_path)
    if tier_flag is None:
        return None
    if config.getoption(tier_flag.lstrip("-").replace("-", "_")):
        return None
    return True


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Explicit-arg backstop for the opt-in tier gates.

    ``pytest_ignore_collect`` is not consulted for paths passed explicitly
    on the command line (``pytest tests/e2e``), so an explicit-arg run
    without the tier's flag still collects it. Drop those items here.
    """
    gated_flags = {
        flag for _dir, flag in _OPT_IN_TIERS if config.getoption(flag.lstrip("-").replace("-", "_"))
    }
    deselected = [
        item
        for item in items
        if (tier_flag := _opt_in_tier_file(item.path)) is not None and tier_flag not in gated_flags
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

#: Force-exits intercepted in this process (see ``pytest_configure``). Each
#: entry is ``(exit_code, first-caller-frame)`` for one ``os._exit`` the test
#: process did not perform.
_WATCHDOG_FORCE_EXITS: list[tuple[str, str]] = []


class _ForceExitIntercepted(SystemExit):
    """Raised in place of an intercepted ``os._exit``.

    The caller's contract decides what survives: the watchdog's daemon
    poll thread dies (its loop is the wedged thing), a loop task ends with
    the exception for its awaiter to see, and the dump already went to
    fd 2 either way. The pytest process — the thing that must report the
    result — always survives.
    """


def _intercepted_force_exit(code: int) -> None:
    """Stand in for ``os._exit`` inside the pytest process.

    The worker's watchdog force-exits the process on a terminal trip
    (``taskq.worker._watchdog``: ``os._exit(EXIT_WATCHDOG)`` with the
    faulthandler dump written to stderr just before). In-process, that
    process is the pytest worker itself: a watchdog trip in one test's
    worker killed the whole xdist worker mid-suite — xdist prints
    ``node down: Not properly terminated``, replaces the worker, the
    replacement re-runs collection under the same co-tenancy, and the leg
    drags to its job cap with the trip's own dump lost to pytest's
    captured-output buffer, which a hard exit never flushes. Three such
    worker deaths in one evening (the 3.12 leg of three separate CI runs,
    three different suite positions) burned the 30-minute cap three times
    with zero diagnosis.

    The interception keeps the diagnosis and drops the process death: the
    dump goes straight to fd 2 (pytest's fd-level capture cannot swallow
    it, and a hard exit would have discarded it), the trip is recorded for
    ``pytest_terminal_summary``, and ``SystemExit`` propagates in the
    calling thread instead. Production semantics are untouched — the
    watchdog still force-exits real workers; tests that pin the exit path
    patch ``os._exit`` themselves and simply layer over this one.
    """
    frame = sys._getframe(1)
    site = f"{frame.f_code.co_filename}:{frame.f_lineno} ({frame.f_code.co_name})"
    _WATCHDOG_FORCE_EXITS.append((str(code), site))
    os.write(
        2,
        f"\n=== pytest intercepted a force-exit (code={code}) at {site} - "
        "the test process would have died here ===\n".encode(),
    )
    faulthandler.dump_traceback(fd=2, all_threads=True)
    raise _ForceExitIntercepted(code)


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
        "through to the real endpoint. Every HTTP mock in this suite goes "
        "through tests/http_mock.py, whose router targets every installed "
        "httpcore (httpx's and httpx2's); tests/test_suite_hygiene.py pins "
        "that no test bypasses it.\n"
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
    discovery and JWKS fetches, while authlib's ``AsyncOAuth2Client`` uses
    whichever stack its version binds to. There was a stretch when
    ``tests/test_sso_oidc.py`` bridged the stacks by monkeypatching
    ``httpx2.AsyncClient`` to ``httpx.AsyncClient`` so stock respx (which
    patches ``httpcore`` only) would intercept both halves; drop, rename or
    narrow that bridge and respx stopped intercepting, with no error of its
    own. The bridge is gone - ``tests/http_mock.py`` aims respx at every
    installed core instead - but the guard stays: a mock miss of any shape
    must fail at the call site, not leave the machine.

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


def pytest_configure(config: pytest.Config) -> None:
    """Intercept ``os._exit`` in the pytest process (see ``_intercepted_force_exit``).

    A config-time warning about multiple installed HTTP stacks used to live
    here too: stock respx patches ``httpcore`` only, so with ``taskq[oidc]``'s
    two stacks (``httpx`` for the dev group, ``httpx2`` for the OIDC extra)
    an ``httpx2`` call could read as mocked while reaching out for real. That
    condition is defeated at the mechanism, so the warning is retired rather
    than fires forever: ``tests/http_mock.py`` registers a respx mocker whose
    targets include every installed core (``httpcore`` AND ``httpcore2``)
    behind one route table, and ``tests/test_suite_hygiene.py`` pins both the
    entry point (no bare ``respx.mock``, no httpx/httpx2 client-class bridge)
    and the interception itself (a request on each installed stack must reach
    the mock, or the suite fails). The outbound-network guard in this file is
    the backstop for anything the pins cannot see. Retiring the warning does
    not weaken any of that; a warning that fired on every run of every leg
    trained the reader to dismiss it.
    """
    os._exit = _intercepted_force_exit  # type: ignore[assignment]  # Why: the seam is deliberate (see _intercepted_force_exit); mypy/pyright see a module builtin reassigned.


@pytest.fixture
def intercepted_force_exits() -> list[tuple[str, str]]:
    """The force-exits this process intercepted so far (see pytest_configure)."""
    return _WATCHDOG_FORCE_EXITS


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
        output["taskq_force_exits"] = _WATCHDOG_FORCE_EXITS


def pytest_testnodedown(node: object, error: object) -> None:
    """Collect a finished xdist worker's blocked attempts on the controller."""
    del error
    forwarded = getattr(node, "workeroutput", {}).get("taskq_blocked_attempts") or []
    _BLOCKED_ATTEMPTS.extend((str(nodeid), str(address)) for nodeid, address in forwarded)
    exits = getattr(node, "workeroutput", {}).get("taskq_force_exits") or []
    _WATCHDOG_FORCE_EXITS.extend((str(code), str(site)) for code, site in exits)


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
    if _WATCHDOG_FORCE_EXITS:
        terminalreporter.section("intercepted force-exits", red=True)
        for code, site in _WATCHDOG_FORCE_EXITS:
            terminalreporter.line(f"  code={code} at {site}")
