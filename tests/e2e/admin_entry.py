"""Admin UI subprocess entrypoint for the admin e2e test.

Spawned ONLY by ``tests/e2e/test_admin_ui.py`` as a host subprocess
(``sys.executable tests/e2e/admin_entry.py``); never imported by pytest.
Mirrors ``taskq.cli._ui_serve`` (cli.py:412-594) reduced to the minimal
read-path wiring: one asyncpg pool, the admin router mounted at ``/admin``
in bearer-token auth mode, uvicorn on 127.0.0.1.

Configuration arrives via env vars:

- ``TASKQ_PG_DSN`` / ``TASKQ_SCHEMA_NAME``: host DSN and migrated schema of
  the test module's e2e Postgres (the same values the e2e client uses).
- ``TASKQ_E2E_ADMIN_TOKEN``: bearer token required on every admin route.
- ``TASKQ_E2E_ADMIN_PORT``: bind port chosen by the spawning test.

Auth choice: ``auth_dependency=token_auth(token)`` is passed explicitly, so
``create_router``'s non-dev fail-closed RuntimeError (``_factory.py:340-349``,
``TASKQ_ADMIN_UI_REQUIRE_AUTH``) never fires, and every admin route requires
``Authorization: Bearer <token>`` (``auth/token.py:19-34``). Redis is omitted
(``redis_client=None``): the UI degrades to polling mode, which the asserted
read endpoints do not depend on. Migrations are NOT run here: the e2e
conftest migrates the module schema before any test starts.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
import uvicorn
from fastapi import FastAPI

from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth import token_auth


def main() -> None:
    """Build the app from env vars and serve until SIGTERM/SIGINT."""
    pg_dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    token = os.environ["TASKQ_E2E_ADMIN_TOKEN"]
    port = int(os.environ["TASKQ_E2E_ADMIN_PORT"])

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
        assert pool is not None  # asyncpg returns None only for record_class paths
        try:
            bundle = create_router(
                pool,
                schema=schema,
                auth_dependency=token_auth(token),
                base_path="/admin",
            )
            setup_admin_state(app, bundle)
            app.include_router(bundle.router, prefix="/admin")
            yield
        finally:
            await pool.close()

    app = FastAPI(lifespan=lifespan)
    uvicorn.run(app, host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
