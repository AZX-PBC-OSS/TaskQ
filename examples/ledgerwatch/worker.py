"""LedgerWatch's worker entrypoint (the fleet's process).

The workflow definitions ride this process (the import of
:mod:`examples.ledgerwatch.app` is the registration — the boot projection marks
this worker workflow-capable and populates the definition registry the
claimed rows' bodies resolve from). The registry carries one vanilla
actor too: the worker's ``--actors`` flag needs a real ActorRef
surface, and the fleet's heartbeat probe is the honest minimal one.

Run: ``python -m examples.ledgerwatch.worker`` with the TASKQ_* env.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

# THE REGISTRATION IMPORT: the app's definitions + the deps binding
# (the D1 registry the fleet-claimed execution door resolves bodies from).
import asyncpg
from pydantic import BaseModel

from examples.ledgerwatch.app import (
    app,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the import IS the registration (D1 + the boot projection).
)
from examples.ledgerwatch.store import bootstrap_conn
from taskq import ActorRef, actor
from taskq.migrate import apply_pending_locked
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main


class WorkerPing(BaseModel):
    """The registry face's payload (the fleet's liveness probe)."""

    note: str = "pong"


@actor
async def lw_worker_ping(payload: WorkerPing) -> WorkerPing:
    """The vanilla-actor face the worker registry boots with."""
    return payload


ACTORS: dict[str, ActorRef[Any, Any]] = {"lw_worker_ping": lw_worker_ping}


if __name__ == "__main__":
    settings = WorkerSettings.load()

    async def _boot() -> None:
        await apply_pending_locked(
            str(settings.resolved_pg_dsn_direct), schema=settings.schema_name
        )
        # The app's own tables (the pool the bodies use is opened lazily
        # inside the worker's run loop — this boot connection is throwaway).
        conn = await asyncpg.connect(str(settings.resolved_pg_dsn_direct))
        try:
            await bootstrap_conn(conn, settings.schema_name)
        finally:
            await conn.close()

    asyncio.run(_boot())
    sys.exit(worker_main(settings, actor_registry=ACTORS))
