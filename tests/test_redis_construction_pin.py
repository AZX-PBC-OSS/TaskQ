"""The single-point-of-construction pin for TaskQ's redis clients.

Born from the redis exposure audit: every redis client TaskQ builds must
come from ``taskq._redis_client.build_redis_client`` (the one place the
resilience kwargs — health checks, TCP keepalive, retry — are applied).
A bare ``redis...from_url(...)`` anywhere else silently skips them, so it
is pinned out by AST, not by review.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src" / "taskq"

# The four production construction sites the audit enumerated. Every one of
# them must go through build_redis_client.
_CONSTRUCTION_SITES = (
    "worker/deps.py",
    "ratelimit/_provider.py",
    "client/_jobs.py",
    "cli.py",
)


def _async_from_url_bindings(tree: ast.Module) -> set[str]:
    """The local names bound to ``redis.asyncio.from_url`` in *tree*.

    Resolves both import shapes: ``import redis.asyncio [as alias]`` (the
    module attribute is then ``from_url``) and
    ``from redis.asyncio import from_url [as alias]``.
    """
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "redis.asyncio":
                    names.add(alias.asname or "redis")
        elif isinstance(node, ast.ImportFrom) and node.module == "redis.asyncio":
            for alias in node.names:
                if alias.name == "from_url":
                    names.add(alias.asname or "from_url")
    return names


def _from_url_call_sites() -> list[tuple[Path, int]]:
    """Every ``redis.asyncio.from_url(...)`` call in src/taskq, as (path, lineno).

    Matches the module-level ``from_url`` function of the ASYNC redis
    namespace (however aliased) — the bare-construction shape the audit
    enumerated. The credential-provider factory's
    ``redis_async.Redis.from_url(...)`` classmethod call is the documented
    caller-owned path (see docs/guides/managed-identities.md) and the
    sync ``redis_sync.from_url`` scaffolding in ``testing/`` is out of
    scope, so neither is flagged.
    """
    sites: list[tuple[Path, int]] = []
    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text())
        bindings = _async_from_url_bindings(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "from_url"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in bindings
            ):
                sites.append((path, node.lineno))
    return sites


def test_no_from_url_outside_build_redis_client() -> None:
    offenders = [
        f"{path.relative_to(_SRC)}:{lineno}"
        for path, lineno in _from_url_call_sites()
        if path.name != "_redis_client.py"
    ]
    assert offenders == [], (
        "redis clients must be built through taskq._redis_client."
        "build_redis_client (single point of construction for the "
        "resilience kwargs); these modules call from_url directly: "
        f"{offenders}"
    )


@pytest.mark.parametrize("module", _CONSTRUCTION_SITES)
def test_construction_sites_use_build_redis_client(module: str) -> None:
    tree = ast.parse((_SRC / module).read_text())
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "build_redis_client"
    }
    assert "build_redis_client" in calls, (
        f"{module} builds a redis client without build_redis_client"
    )
