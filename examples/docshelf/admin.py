"""DocShelf's admin sidecar builder — the operator's view over the rows.

The TaskQ admin router mounted under ``/taskq``: the runs list, the run
explorer (the Mermaid graph, the node panels, the SSE live transport),
the progress stream. The tests capture the pushed event stream and the
operator faces through this app (httpx's ASGI transport — no sockets).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

import asyncpg
from fastapi import FastAPI

from examples.docshelf.app import SHELF_APP
from taskq import TaskQ
from taskq.web.admin import create_router, setup_admin_state


def build_admin_app(dsn: str, schema: str, *, auth: bool | None = None) -> FastAPI:
    """The sidecar app: the admin router over DocShelf's schema, with the
    workflow definitions mounted (the typed resolve door delivers).

    *auth* forces the fail-closed auth posture on/off (the drill's
    local capture runs with it OFF — the admin's own startup check
    demands an auth_dependency in non-dev environments otherwise)."""
    import os

    if auth is not None:
        os.environ["TASKQ_ADMIN_UI_REQUIRE_AUTH"] = "false" if not auth else "true"
        os.environ["TASKQ_PROGRESS_REQUIRE_AUTH"] = "false" if not auth else "true"

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        async with AsyncExitStack() as stack:
            pg_pool = await stack.enter_async_context(
                asyncpg.create_pool(dsn, min_size=1, max_size=4),
            )
            assert pg_pool is not None
            tq = await stack.enter_async_context(
                TaskQ(pool=pg_pool, schema=schema),
            )
            bundle = create_router(
                pg_pool,
                schema=schema,
                redis_client=None,
                auth_dependency=None,
                base_path="/admin",
                backend=tq.backend,
                workflow_app=SHELF_APP,
            )
            setup_admin_state(application, bundle)
            application.include_router(bundle.router, prefix="/admin")
            yield

    return FastAPI(lifespan=lifespan)


def create_app() -> FastAPI:
    """The uvicorn factory face: ``uvicorn examples.docshelf.admin:create_app
    --factory``. The env carries the deployment's own settings; the
    fail-closed auth posture is the deployment's decision (the drill's
    local capture passes ``auth=False`` explicitly)."""
    import os

    return build_admin_app(
        os.environ["TASKQ_PG_DSN"],
        os.environ.get("TASKQ_SCHEMA_NAME", "taskq"),
    )
