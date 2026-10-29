"""The operator's faces: the display API, the admin app, the listeners.

An operator watches three ways, all over the same rows:

1. THE DISPLAY API — ``run_display`` / ``keyed_display`` (the
   ledger-derived states with the progress inside).
2. THE ADMIN APP — the full UI mounted into a host FastAPI app
   (``/admin/workflows/{run_id}`` is the run explorer).
3. THE LISTENERS — the typed progress stream (and the holds' stream on
   the same machinery), direct async iteration.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from taskq.workflows import ProgressListener

# The display read is documented at ``taskq.workflows.run_display`` (the
# progress guide's verified fence); the module lives under the private
# underscore path — filed as adoption friction, used as documented.
from taskq.workflows._progress_read import keyed_display, run_display


async def operator_view(pool: asyncpg.Pool, runner_wsql: Any, flow_id: str) -> dict[str, Any]:
    """The ledger-derived run display, keyed BY NAME (``step_key`` +
    ``map_index`` — the consumer's render, no out-of-band join)."""
    display = await run_display(pool, runner_wsql, flow_id)
    return keyed_display(display)


async def stream_progress(
    pool: asyncpg.Pool,
    schema: str,
    flow_id: str,
    events: list[Any],
    *,
    stop_after: int | None = None,
) -> None:
    """Capture the typed progress stream into ``events`` (the drills'
    push-face receipt). Runs until cancelled — the caller schedules it
    as a task and cancels on completion; ``stop_after`` bounds a
    test's capture for a run that streams a KNOWN number of updates."""
    listener = ProgressListener(pool, schema, flow_id=flow_id)
    async with listener:
        seen = 0
        async for event in listener.updates():
            events.append(event)
            seen += 1
            if stop_after is not None and seen >= stop_after:
                break


def admin_app(pool: asyncpg.Pool, schema: str, base_path: str = "/admin") -> Any:
    """The admin UI mounted into a host FastAPI app (the operator's
    full face: the run explorer at ``{base_path}/workflows/{run_id}``).
    Requires ``TASKQ_ENVIRONMENT=dev`` (or an auth_dependency) — the
    admin fails closed otherwise."""
    import os

    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    os.environ.setdefault("TASKQ_ENVIRONMENT", "dev")

    bundle = create_router(
        pool, schema=schema, redis_client=None, auth_dependency=None, base_path=base_path
    )
    host = FastAPI()
    setup_admin_state(host, bundle)
    host.include_router(bundle.router, prefix=base_path)
    return host
