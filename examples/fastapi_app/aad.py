"""Azure managed-identity (AAD) deployment scaffold for TaskQ.

This example shows the complete wiring for a web app + worker that
authenticates to Azure Database for PostgreSQL and Azure Cache for Redis
using Microsoft Entra ID managed identities via the ``taskq[aad]`` extra.

Prerequisites
-------------

1. Install the extras::

       pip install 'taskq-py[aad,redis,fastapi]'

2. Enable Microsoft Entra authentication on your Azure DB for Postgres
   and Azure Cache for Redis instances (see docs/guides/managed-identities.md).

3. Assign the managed identity to your app's compute (App Service,
   Container Apps, AKS, etc.) and grant it the appropriate roles.

4. Set environment variables::

       TASKQ_PG_DSN=postgresql://my-mi@my-pg.postgres.database.azure.com:5432/taskq
       TASKQ_REDIS_URL=rediss://my-cache.redis.cache.windows.net:6380
       # No password in the DSN — the AAD token is injected at connect time.

Run the worker::

    python -m examples.fastapi_app.aad worker

Run the web app::

    python -m examples.fastapi_app.aad serve
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

# FastAPI / uvicorn are optional — guard the import so this module is
# import-safe for the worker-only path.
try:
    import uvicorn
except ImportError:  # pragma: no cover
    uvicorn = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from fastapi import FastAPI

from taskq import WorkerConnections
from taskq.aad import EntraIdProvider
from taskq.auth import (
    make_dedicated_conn_factory,
    make_pg_pool_factory,
    make_redis_client_factory,
)
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main

# In a real app these would be your decorated actors.
ACTORS: dict[str, Any] = {}


def build_connections(settings: WorkerSettings, provider: EntraIdProvider) -> WorkerConnections:
    """Build WorkerConnections with AAD factories for every PG/Redis role."""
    return WorkerConnections(
        dispatcher_pool_factory=make_pg_pool_factory(
            str(settings.pg_dsn_direct),
            provider,
            max_size=settings.dispatcher_pool_size,
        ),
        heartbeat_pool_factory=make_pg_pool_factory(
            str(settings.pg_dsn_direct),
            provider,
            max_size=settings.heartbeat_pool_size,
            command_timeout=2,
        ),
        worker_pool_factory=make_pg_pool_factory(
            str(settings.pg_dsn_pooled),
            provider,
            max_size=settings.worker_pool_size,
        ),
        notify_conn_factory=make_dedicated_conn_factory(str(settings.pg_dsn_direct), provider),
        leader_conn_factory=make_dedicated_conn_factory(str(settings.pg_dsn_direct), provider),
        redis_client_factory=make_redis_client_factory(
            str(settings.redis_url) if settings.redis_url else None,
            provider,
        ),
    )


def main() -> None:
    """Entry point: run the worker or the web app based on argv[1]."""
    import sys

    settings = WorkerSettings.load()
    mode = sys.argv[1] if len(sys.argv) > 1 else "worker"

    if mode == "worker":
        from azure.identity.aio import DefaultAzureCredential

        # DefaultAzureCredential is an async context manager
        # (azure.identity.aio); worker_main() drives its own asyncio.Runner,
        # so the credential is entered/exited manually around it rather than
        # with `async with`.
        cred = DefaultAzureCredential()
        asyncio.run(cred.__aenter__())
        try:
            provider = EntraIdProvider(cred)
            worker_main(
                settings,
                actor_registry=ACTORS,
                connections=build_connections(settings, provider),
            )
        finally:
            asyncio.run(cred.__aexit__(None, None, None))
    elif mode == "serve":
        try:
            import importlib

            importlib.import_module("fastapi")
        except ImportError as exc:
            raise SystemExit(
                "FastAPI is not installed. pip install 'taskq-py[fastapi]'"
            ) from exc
        asyncio.run(_serve(settings))
    else:  # pragma: no cover
        raise SystemExit(f"unknown mode {mode!r}; use 'worker' or 'serve'")


async def _serve(settings: WorkerSettings) -> None:
    """Run the FastAPI web app with an AAD-authenticated TaskQ client."""
    from azure.identity.aio import DefaultAzureCredential

    from taskq import TaskQ
    from taskq.auth import make_pg_pool_factory

    async with DefaultAzureCredential() as cred:
        # One provider instance for both PG and Redis (EntraIdProvider implements
        # both Protocols). The client uses a factory-built pool (fresh AAD
        # token at construction) and a factory-built Redis client (auto-
        # rotating tokens on reconnect).
        provider = EntraIdProvider(cred)
        pool_factory = make_pg_pool_factory(
            str(settings.pg_dsn_direct), provider, min_size=1, max_size=5
        )
        pool = await pool_factory()
        # Factory-build a caller-owned Redis client (auto-rotating tokens).
        redis_client = None
        if settings.redis_url:
            redis_factory = make_redis_client_factory(str(settings.redis_url), provider)
            redis_client = await redis_factory()
        try:
            tq = TaskQ(
                pool=pool,  # caller-owned
                redis_client=redis_client,  # caller-owned, None if no Redis
                schema=settings.schema_name,
            )
            await tq.open()
            try:
                app = _build_app(tq)
                config = uvicorn.Config(  # type: ignore[union-attr]
                    app, host="0.0.0.0", port=8000  # noqa: S104  # demo
                )
                server = uvicorn.Server(config)  # type: ignore[union-attr]
                await server.serve()
            finally:
                await tq.close()
        finally:
            await pool.close()
            if redis_client is not None:
                await redis_client.aclose()


def _build_app(tq: Any) -> FastAPI:
    """Build a minimal FastAPI app with TaskQ in lifespan."""
    from fastapi import FastAPI

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        app.state.tq = tq
        yield

    app = FastAPI(lifespan=lifespan)

    @app.post("/enqueue")
    async def enqueue() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]  # Why: registered as a route via the decorator, not called by name.
        # In a real app you'd enqueue a typed actor here.
        return {"status": "ok"}

    return app


if __name__ == "__main__":
    main()
