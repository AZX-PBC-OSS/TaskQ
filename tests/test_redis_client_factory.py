"""Pins for ``build_redis_client`` — the kwargs TaskQ builds its own clients with.

Born from the redis exposure audit: every client TaskQ builds internally
carries the resilience kwargs (health checks, TCP keepalive, a real Retry
with ``retry_on_error``) and deliberately NO ``socket_timeout`` (see the
builder's docstring for why a global socket timeout would change pubsub
blocking-read semantics). The no-bare-``from_url`` construction pin lives
in ``tests/test_redis_construction_pin.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from redis.asyncio import Connection as RedisAsyncConnection
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from taskq._forkguard import guarded_redis_connection_class
from taskq._redis_client import build_redis_client

_SRC = Path(__file__).resolve().parents[1] / "src" / "taskq"


def test_build_redis_client_sets_resilience_kwargs() -> None:
    client = build_redis_client("redis://localhost:6379/0")

    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["health_check_interval"] == 30
    assert kwargs["socket_keepalive"] is True

    retry = kwargs["retry"]
    assert isinstance(retry, Retry)
    assert retry.get_retries() == 3
    # `_backoff` is private but the only accessor redis-py exposes; pin the
    # class so a silent swap to NoBackoff (retry = immediate re-fire into a
    # dead broker) fails loudly.
    assert isinstance(retry._backoff, ExponentialBackoff)

    assert set(kwargs["retry_on_error"]) == {RedisConnectionError, RedisTimeoutError}


def test_build_redis_client_keeps_fork_guard_connection_class() -> None:
    client = build_redis_client("redis://localhost:6379/0")

    assert client.connection_pool.connection_class is guarded_redis_connection_class()
    assert isinstance(client.connection_pool.connection_class(), RedisAsyncConnection)


def test_build_redis_client_decode_responses_passthrough() -> None:
    default_client = build_redis_client("redis://localhost:6379/0")
    decoded_client = build_redis_client("redis://localhost:6379/0", decode_responses=True)

    assert default_client.connection_pool.connection_kwargs["decode_responses"] is False
    assert decoded_client.connection_pool.connection_kwargs["decode_responses"] is True


def test_build_redis_client_pins_socket_timeout() -> None:
    # A bound-pin, not an absence: redis-py 8.x's asyncio Connection already
    # defaults socket_timeout to 5s (redis._defaults.DEFAULT_SOCKET_TIMEOUT),
    # and the fail-closed surfaces' bounded wall depends on it — the rate
    # limiter's with_pg_fallback wraps its redis call in NO app-level
    # wait_for, so against a black-holed broker the socket timeout is the
    # only bound per read. Pinning the value makes that bound structural
    # instead of inherited (redis-py's asyncio default was None for years
    # pre-8.0). Behavior is byte-identical today; pubsub blocking reads are
    # unaffected either way (parse_response hands math.inf to read_response
    # when block=True, overriding the socket timeout explicitly).
    kwargs = build_redis_client("redis://localhost:6379/0").connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == 5.0


def test_build_redis_client_returns_fresh_retry_per_call() -> None:
    # Retry/backoff objects carry per-attempt state; two clients must never
    # share one Retry instance.
    first = build_redis_client("redis://localhost:6379/0").connection_pool.connection_kwargs[
        "retry"
    ]
    second = build_redis_client("redis://localhost:6379/0").connection_pool.connection_kwargs[
        "retry"
    ]
    assert first is not second


@pytest.mark.parametrize(
    "module",
    (
        "worker/deps.py",
        "ratelimit/_provider.py",
        "client/_jobs.py",
        "cli.py",
        "_redis_client.py",
    ),
)
def test_import_discipline_redis_extras_are_optional(module: str) -> None:
    # The [redis] extra is optional: no module may import redis at module
    # scope. build_redis_client defers the import like every other site.
    # Module top-level only, skipping `if TYPE_CHECKING:` guards (stubs are
    # free) and function bodies (a def-local import IS the call-time
    # discipline this pin enforces).
    import ast

    def _module_level_imports(tree: ast.Module) -> list[ast.stmt]:
        found: list[ast.stmt] = []

        def _walk(stmts: list[ast.stmt]) -> None:
            for stmt in stmts:
                if isinstance(stmt, (ast.Import, ast.ImportFrom)):
                    found.append(stmt)
                elif isinstance(stmt, ast.If):
                    is_type_checking = (
                        isinstance(stmt.test, ast.Name) and stmt.test.id == "TYPE_CHECKING"
                    ) or (
                        isinstance(stmt.test, ast.Attribute) and stmt.test.attr == "TYPE_CHECKING"
                    )
                    if not is_type_checking:
                        _walk(stmt.body)
                        _walk(stmt.orelse)
                elif isinstance(stmt, ast.Try):
                    _walk(stmt.body)
                    _walk(stmt.handlers)
                    _walk(stmt.orelse)
                    _walk(stmt.finalbody)

        _walk(tree.body)
        return found

    tree = ast.parse((_SRC / module).read_text())
    for node in _module_level_imports(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] != "redis" for alias in node.names), (
                f"{module} imports redis at module scope; the [redis] extra "
                "is optional and the import must stay call-time"
            )
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            assert node.module.split(".")[0] != "redis", (
                f"{module} imports from redis at module scope; the [redis] "
                "extra is optional and the import must stay call-time"
            )
