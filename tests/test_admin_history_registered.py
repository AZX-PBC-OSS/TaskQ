"""The /history admin panel is registered on the router.

The history module shipped as ``_history.py`` - a leading underscore that
``_factory._discover_and_register`` skips - so the fully-implemented panel
was never attached to any admin router. The rename to ``history.py`` puts it
on the auto-discovery path; these tests pin that ``create_router`` actually
registers its routes, so a future rename back to an underscore-prefixed
module fails here instead of silently unregistering the panel. The page pin
drives the served panel over the module's real pool (the
tests/test_web_admin_integration.py pattern), so the 200 is the route's own
read answering the real schema.
"""

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state

pytestmark = [pytest.mark.fastapi]


def _admin_app(pool: asyncpg.Pool, schema: str) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


def test_history_routes_registered_via_discovery(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """create_router auto-discovers history.py: /history and /api/history/stats exist."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(module_pg_pool, schema=module_pg_schema.schema_name, base_path="")
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]
    assert "/history" in route_paths
    assert "/api/history/stats" in route_paths


async def test_history_page_returns_200(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /history over the real pool returns 200 HTML."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    app = _admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/history")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
