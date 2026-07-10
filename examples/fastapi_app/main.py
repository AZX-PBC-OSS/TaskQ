"""FastAPI example app — enqueue, stream, and cancel jobs via the public TaskQ surface.

Demonstrates :meth:`TaskQ.stream` for real-time SSE fanout of
:class:`~taskq.JobEvent` objects.  Uses only the public
``taskq`` API — no private submodule imports.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from examples.fastapi_app.routes import router
from taskq import TaskQ
from taskq.settings import TaskQSettings


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
    settings = TaskQSettings.load()
    kwargs: dict[str, object] = {"dsn": str(settings.pg_dsn), "schema": settings.schema_name}
    if settings.redis_url is not None:
        kwargs["redis_url"] = str(settings.redis_url)

    tq = TaskQ(**kwargs)
    await tq.open()
    application.state.tq = tq
    try:
        yield
    finally:
        await tq.close()


app = FastAPI(lifespan=lifespan)
app.include_router(router)
