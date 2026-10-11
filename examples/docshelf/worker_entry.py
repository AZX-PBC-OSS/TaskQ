"""DocShelf's worker entrypoint — the registry the ``taskq worker`` CLI
loads (``--actors examples.docshelf.worker_entry:ACTORS``).

THE IMPORT IS THE REGISTRATION: importing :mod:`examples.docshelf.app`
compiles the workflow definitions into THIS process (the D1 registry the
fleet's execution door resolves bodies from) and projects the workflows'
actor cohorts into ``actor_config`` (the boot projection) — a worker
that did not import the app can never claim a flow row, by design.
"""

from __future__ import annotations

import asyncio
from typing import Any

import examples.docshelf.app  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the import IS the registration (D1 + the cohorts' projection).
from examples.docshelf.app import docshelf_nightly_tick
from examples.docshelf.provision import provision_cohorts
from taskq import ActorRef

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "docshelf-nightly-tick": docshelf_nightly_tick,
}


def _provision() -> None:
    """The deploy's cohort registration (create-only; a best-effort at
    worker boot — the worker's DSN is the deployment's own)."""
    import os

    dsn = os.environ.get("TASKQ_PG_DSN")
    if not dsn:
        return

    async def _run() -> None:
        import asyncpg

        from examples.docshelf.config import DOCSHELF_QUEUES

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        assert pool is not None
        schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
        try:
            # The dispatch needs the worker's OWN queues' cohorts — the
            # vanilla workers' refs register themselves; the route arms'
            # cohorts (and any queue the projection missed) provision here.
            for actor_name in DOCSHELF_QUEUES:
                await pool.execute(
                    f'INSERT INTO "{schema}".actor_config '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
                    "(actor, queue, metadata, max_attempts, retry_kind) "
                    "VALUES ($1, $1, '{\"workflow_actor\": true}'::jsonb, 3, 'transient') "
                    "ON CONFLICT (actor) DO NOTHING",
                    actor_name,
                )
            await provision_cohorts(pool, schema)
        finally:
            await pool.close()

    asyncio.run(_run())


_provision()
