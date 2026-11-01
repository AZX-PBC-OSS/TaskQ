"""The flows demo's admin sidecar — ``admin_app.py``'s shape, wired for
the workflow demo: the admin UI (with the run explorer + the typed
Resolve form) mounted over the SAME schema the in-process demo drove,
plus a background drive loop so runs continue past holds and timers
while the UI is open.

Run (the same env ``run_demo`` used — its schema persists the run's
rows for inspection):

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_ENVIRONMENT=dev \
    TASKQ_QUEUES=flows-src,flows-review,flows-timer,flows-pub \
        uv run uvicorn examples.flows_demo.webapp:app --port 8003

Then open ``http://localhost:8003/admin/`` — README.md's WHAT-TO-OBSERVE
list names the pages. All configuration loads through
:meth:`TaskQSettings.load`; the demo's workflow definitions mount
(``workflow_app=flows_app``) so the run page's Resolve form DELIVERS the
typed ``Approval`` — without a mounted WorkflowApp the resolve
endpoints answer ``501``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

import asyncpg
import structlog
from fastapi import FastAPI

from examples.flows_demo.app import configure, flows_app
from taskq import TaskQ
from taskq.migrate import apply_pending_locked
from taskq.settings import TaskQSettings
from taskq.web.admin import create_router, setup_admin_state

# THE LOCAL-HTTP DEV CASE (the settings' own carve-out for
# TASKQ_ADMIN_UI_SECURE_COOKIES): this demo serves plain
# http://localhost:8003, where the browser rejects a Secure cookie — the
# admin CSRF cookie never lands and every action POST (the Resolve flow)
# answers 403. The demo's own process defaults the flag off, before the
# settings load; an explicit operator setting still wins (setdefault,
# never a stomp).
os.environ.setdefault("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")

#: The drive loop's logger (one name, the demo's own).
log = structlog.get_logger("examples.flows_demo.webapp")

#: The background drive loop's cadence (the demo's own; a worker
#: deployment runs the fleet door instead).
_DRIVE_TICK_S = 0.5


async def _drive_pending(pool: asyncpg.Pool, schema: str) -> None:
    """One drive pass over THIS app's running flows (the demo's
    in-process driver; a worker process would drive the same rows). A
    run's failure is THAT run's story: the pass continues to the runs
    behind it."""
    from taskq.workflows import FlowRunner

    names = flows_app.workflow_names()
    rows = await pool.fetch(
        f'SELECT id FROM "{schema}".jobs '  # noqa: S608  # Why: the schema is the settings-validated identifier; the names are this module's own list.
        "WHERE step_key = '__flow__' AND status = 'running' "
        "AND metadata->>'workflow' = ANY($1) ORDER BY id LIMIT 5",
        names,
    )
    for row in rows:
        runner = FlowRunner(flows_app.get("flows_demo"), pool, schema)
        try:
            await runner.drive(row["id"], until="held", max_ticks=200)
        except asyncio.CancelledError:
            raise  # the cancel is never swallowed (the shutdown discipline)
        except Exception:
            log.exception("flows-demo-drive-run-failed", run_id=str(row["id"]))


async def _drive_loop(pool: asyncpg.Pool, schema: str) -> None:
    """The demo's drive loop: every 0.5 s, one pass over the pending
    runs. Cancelled with the app's shutdown (the cancel AWAITS the
    loop — a consumed cancellation is never re-delivered)."""
    log.info("flows-demo-drive-loop-started", schema=schema)
    while True:
        try:
            await _drive_pending(pool, schema)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The loop must survive a bad tick; the rows are the truth
            # and the next pass re-drives — but it says so LOUDLY.
            log.exception("flows-demo-drive-pass-failed")
        await asyncio.sleep(_DRIVE_TICK_S)


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

        # The demo's schema-bound pieces (the reservation's slot rows
        # live in THIS schema) + the capacity terms' actor_config rows.
        gadget = configure(settings.schema_name)
        await gadget.ensure_slots(pg_pool)

        schema = settings.schema_name

        # The admin router's Backend (the sidecar owns a TaskQ client —
        # admin_app.py's own reasoning: without one the backend-mediated
        # buttons render but answer 503).
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
            # THE TYPED DOOR'S WITNESS: the demo's workflow definitions
            # mount so the run page's Resolve form DELIVERS the typed
            # Approval (a sidecar that cannot resolve is a demo that
            # cannot perform its own central action).
            workflow_app=flows_app,
        )
        setup_admin_state(application, bundle)
        application.include_router(bundle.router, prefix="/admin")

        # THE DRIVE LOOP: the demo's runs continue past holds + timers
        # while the UI is open.
        loop_task = asyncio.create_task(_drive_loop(pg_pool, schema))
        try:
            yield
        finally:
            loop_task.cancel()
            # THE AWAITED CANCEL (the shutdown discipline's shape): the
            # loop's own cancellation is consumed by ITS raise, this
            # await only observes the exit.
            with contextlib.suppress(asyncio.CancelledError):
                await loop_task


app = FastAPI(lifespan=lifespan)
