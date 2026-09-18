"""Every request handler's pool checkout is bounded.

A handler that awaited ``pool.acquire()`` with no bound hung for as long as
the pool had no connection to give, and every request behind it hung the
same way. Handlers check out through :class:`taskq.web._pool.BoundedPool`,
which answers 503 after ``TASKQ_ADMIN_ACQUIRE_TIMEOUT``; the inventory
guard here keeps a handler from taking a raw pool again.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from typing import Any

import pytest
import structlog.testing

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from taskq.web._pool import BoundedPool
from taskq.web.admin import create_router, setup_admin_state

pytestmark = [pytest.mark.fastapi]
_WEB = Path(__file__).resolve().parents[2] / "src" / "taskq" / "web"

# Modules whose functions are request handlers or run inside one. The LISTEN
# generator bounds its own checkout (it lives for the stream, not the
# request) and the pool module is the bound itself.
_HANDLER_MODULES = sorted(
    path
    for path in [*(_WEB / "admin").glob("*.py"), _WEB / "progress.py"]
    if path.name not in {"_listen.py", "__init__.py"}
)


def _annotation(node: ast.expr | None) -> str:
    return ast.unparse(node) if node is not None else ""


def _own_calls(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.Call]:
    """Calls in *fn*'s own body, not in the functions nested inside it."""
    calls: list[ast.Call] = []
    stack: list[ast.AST] = list(fn.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Call):
            calls.append(node)
        stack.extend(ast.iter_child_nodes(node))
    return calls


def _unbounded_checkouts(path: Path) -> list[str]:
    """Every pool ``acquire(...)`` whose receiver is not typed BoundedPool.

    A receiver counts as a pool when its name or annotation says so; the
    SSE semaphores' ``acquire`` is a different thing.
    """
    tree = ast.parse(path.read_text())
    offenders: list[str] = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        params = {a.arg: _annotation(a.annotation) for a in fn.args.args + fn.args.kwonlyargs}
        for call in _own_calls(fn):
            if not (isinstance(call.func, ast.Attribute) and call.func.attr == "acquire"):
                continue
            receiver = call.func.value
            name = ast.unparse(receiver)
            annotation = params.get(name, "")
            if annotation == "BoundedPool":
                continue
            if "pool" not in name.lower() and "Pool" not in annotation:
                continue
            if path.name == "_pool.py" and name == "self.pool":
                # BoundedPool.acquire's own checkout of the raw pool: the one
                # place the bound is applied.
                continue
            offenders.append(f"{path.relative_to(_WEB).as_posix()}:{call.lineno} {name}.acquire")
    return offenders


def test_no_handler_checks_out_a_raw_pool() -> None:
    offenders = [o for path in _HANDLER_MODULES for o in _unbounded_checkouts(path)]
    assert offenders == [], (
        "these checkouts are not bounded by BoundedPool; a wedged pool would hang the "
        f"request and every request behind it: {offenders}"
    )


def test_no_handler_takes_the_raw_pool_dependency() -> None:
    """``Depends(get_pg_pool)`` on a handler parameter hands it the raw pool;
    only the stream resolvers (which take a Request) may use it."""
    offenders: list[str] = []
    for path in _HANDLER_MODULES:
        if path.name == "_factory.py":
            continue
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for arg, default in zip(
                reversed(fn.args.args), reversed(fn.args.defaults), strict=False
            ):
                if "get_pg_pool" in ast.unparse(default):
                    offenders.append(f"{path.name}:{fn.lineno} {fn.name}({arg.arg})")
    assert offenders == [], offenders


# ── The bound itself ─────────────────────────────────────────────────────


class _AcquireCtx:
    def __init__(self, pool: _Pool) -> None:
        self._pool = pool

    async def __aenter__(self) -> Any:
        if self._pool.hang:
            raise TimeoutError
        return self._pool.conn

    async def __aexit__(self, *_: object) -> None:
        return None


class _Conn:
    async def fetch(self, *_: object) -> list[Any]:
        return []

    async def fetchval(self, *_: object) -> Any:
        return None

    async def fetchrow(self, *_: object) -> Any:
        return None

    async def execute(self, *_: object) -> str:
        return "OK"


class _Pool:
    """A pool that either hands out a connection or times out the checkout."""

    def __init__(self, *, hang: bool) -> None:
        self.hang = hang
        self.conn = _Conn()
        self.timeouts: list[float | None] = []

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        self.timeouts.append(timeout)
        return _AcquireCtx(self)

    def get_size(self) -> int:
        return 4

    def get_idle_size(self) -> int:
        return 0


async def test_a_checkout_that_never_arrives_is_a_503_with_retry_after() -> None:
    pool = _Pool(hang=True)
    bounded = BoundedPool(pool, acquire_timeout=0.25, role="test")  # pyright: ignore[reportArgumentType]  # Why: duck-typed pool.
    with structlog.testing.capture_logs() as logs, pytest.raises(HTTPException) as info:
        async with bounded.acquire():
            pytest.fail("no connection can be handed out")
    assert info.value.status_code == 503
    assert info.value.headers is not None and info.value.headers["Retry-After"] == "2"
    assert "TASKQ_ADMIN_ACQUIRE_TIMEOUT" in str(info.value.detail)
    assert pool.timeouts == [0.25]
    entry = next(e for e in logs if e["event"] == "pool-acquire-timeout")
    assert entry["role"] == "test"
    assert entry["pool_size"] == 4 and entry["pool_idle"] == 0


async def test_a_timeout_inside_the_checkout_is_the_handlers_own() -> None:
    """A TimeoutError raised by the handler's work while it holds the
    connection propagates as itself, never as a 503 blaming the checkout."""
    bounded = BoundedPool(_Pool(hang=False), acquire_timeout=1.0, role="test")  # pyright: ignore[reportArgumentType]  # Why: duck-typed pool.
    with pytest.raises(TimeoutError):
        async with bounded.acquire():
            await asyncio.wait_for(asyncio.sleep(1), timeout=0.01)


def test_queue_overview_answers_503_when_the_pool_is_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One route end to end: a checkout that times out is a 503 with a
    clear body, and the timeout it waited is the admin setting."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACQUIRE_TIMEOUT", "0.5")
    pool = _Pool(hang=True)
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: duck-typed pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/queues")

    assert response.status_code == 503, response.text
    assert response.headers["retry-after"] == "2"
    assert "no database connection became available" in response.text
    assert 0.5 in pool.timeouts
