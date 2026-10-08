"""Admin sidecar FastAPI app - mounts the TaskQ admin router at ``/admin``.

Demonstrates the "separate process" deployment shape: the admin UI is
completely decoupled from the user's trigger routes.  No trigger UI
routes, no ``examples.app`` dependency — but the workflow DEFINITIONS
module (``examples.workflows``) mounts so the typed resolve door
delivers: the door validates every payload against the declared gate
models (``workflow_app=...``), and without a mounted WorkflowApp the
resolve endpoints answer ``501`` (the typed door refuses to deliver
untyped — a resolve that only answers 403/501 is a demo that cannot
perform its own central action).  All configuration is loaded through
:meth:`TaskQSettings.load`; no raw ``os.environ`` access.

Set ``TASKQ_MIGRATE_ON_START=true`` to apply pending migrations before
the first request.  The process exits if migrations fail.

Run with: ``uv run uvicorn examples.admin_app:app --host 0.0.0.0 --port 8001``
"""

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

import asyncpg
from fastapi import FastAPI

from examples.workflows import wf_app
from taskq import TaskQ
from taskq.migrate import apply_pending_locked
from taskq.settings import TaskQSettings
from taskq.web.admin import create_router, setup_admin_state


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
    settings = TaskQSettings.load()

    if settings.migrate_on_start:
        await apply_pending_locked(str(settings.pg_dsn), schema=settings.schema_name)

    async with AsyncExitStack() as stack:
        pg_pool = await stack.enter_async_context(
            asyncpg.create_pool(str(settings.pg_dsn), min_size=1, max_size=4),  # type: ignore[arg-type]  # Why: asyncpg.create_pool returns AsyncContextManager[Pool | None]; enter_async_context expects AsyncContextManager[T]; pyright cannot resolve the generic across the conditional pool-return.
        )
        assert pg_pool is not None, "asyncpg.create_pool returned None"

        redis_client: object | None = None
        if settings.redis_url is not None:
            import redis.asyncio as aioredis

            redis_client = await stack.enter_async_context(
                aioredis.from_url(str(settings.redis_url)),  # type: ignore[arg-type]  # Why: aioredis.from_url returns Redis which is an async context manager; pyright cannot resolve the generic across the object | None erasure boundary.
            )

        schema = settings.schema_name

        # The sidecar owns a TaskQ client so the admin router gets a real
        # Backend: without one, the backend-mediated mutation buttons
        # (job cancel, job retry, schedule run-now) render but every one
        # answers 503 "Backend not configured". The client is constructed
        # against the SAME pool (caller-owned; close() will not close it),
        # so the sidecar adds no second connection pool.
        tq = await stack.enter_async_context(
            TaskQ(
                pool=pg_pool,
                schema=schema,
                redis_client=redis_client,
            ),
        )

        bundle = create_router(
            pg_pool,
            schema=schema,
            redis_client=redis_client,
            auth_dependency=None,
            base_path="/admin",
            backend=tq.backend,
            # THE TYPED DOOR'S WITNESS (attack-4 F-P4-DEMO-DEAD-RESOLVE's
            # cure): the demo's workflow definitions mount so the run
            # page's Resolve form DELIVERS the typed ReviewDecision.
            # Without it the resolve endpoints answer 501 — a sidecar
            # that cannot resolve is a demo that cannot perform its own
            # central action.
            workflow_app=wf_app,
        )
        setup_admin_state(application, bundle)
        application.include_router(bundle.router, prefix="/admin")

        yield


app = FastAPI(lifespan=lifespan)
