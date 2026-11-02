"""Scenario 7 — the degraded faces.

Three ways a deployment can be wrong, and the three clean refusals:
nothing hangs silently, nothing crashes a worker, and no page anywhere
answers a 500.

- The UNINSTALLED SCHEMA: the run is refused with the typed error naming
  the schema and the remedy, and the admin's runs page renders its
  notice (a 200 with the notice, never a 500).
- The STALE DEFINITION: the worker's definitions dropped a step the
  database's rows still carry. The row parks loudly (budget-free, the
  reason stamped on it) and the run finishes the moment the definitions
  are whole again.
- The MISCONFIGURED QUEUE: a worker pointed at a workflow whose queue
  the worker does not consume refuses to boot, naming the queue and the
  remedy.

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_REDIS_URL=redis://localhost:6379/0 \
    uv run python -m examples.scenario_grounds.s7_degraded_faces
"""

from __future__ import annotations

import asyncio
import os

from examples.scenario_grounds._common import (
    ev,
    fresh_schema,
    open_pool,
    pg_dsn,
    say,
    scratch_dir,
    start_worker,
    wait_for_worker_ready,
    wait_until,
)

SCHEMA = "grounds_s7"
BARE_SCHEMA = "grounds_s7_bare"
V1_REF = "examples.scenario_grounds.defs_stale_v1:ACTORS"
V2_REF = "examples.scenario_grounds.defs_stale_v2:ACTORS"
QUEUE = "grounds-stale"


async def main() -> None:
    # THE PAGE FACE'S OWN CONTRACT, FORCED: the admin's UI mounts
    # without an auth dependency only under the dev label — an ambient
    # production label (a CI lane's env, a deploy artifact) would crash
    # the page face at create_router. The scenario OVERRIDES the label
    # (never a setdefault), and the page face is dev-only by design:
    # production mounts the sidecar with its real auth dependency.
    os.environ["TASKQ_ENVIRONMENT"] = "development"
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    # The bare schema: created, deliberately not migrated.
    from asyncpg import connect

    conn = await connect(dsn)
    await conn.execute(
        f'DROP SCHEMA IF EXISTS "{BARE_SCHEMA}" CASCADE'
    )  # Why: the schema is this module's own constant identifier.
    await conn.execute(f'CREATE SCHEMA "{BARE_SCHEMA}"')  # Why: same — the module's own constant.
    await conn.close()

    await the_uninstalled_schema(dsn)
    await the_stale_definition(dsn)
    await the_misconfigured_queue(dsn)

    say("")
    say("Three wrong deployments, three clean refusals: the typed error,")
    say("the notice on the page, the parked row, the boot that refuses —")
    say("and not one 500 anywhere.")


async def the_uninstalled_schema(dsn: str) -> None:
    """Face one: nothing is migrated. The run refuses with the typed
    error; the page renders its notice."""
    from examples.scenario_grounds.defs_stale_v1 import app as stale_app
    from taskq.exceptions import SchemaNotMigratedError
    from taskq.workflows import run

    say("")
    say("FACE 1 — the uninstalled schema. A run pointed at a schema with")
    say("no migrations applied:")
    pool = await open_pool(dsn)
    try:
        await run(stale_app.get("ground_stale"), pool, BARE_SCHEMA, input=None)
        raise RuntimeError("the run on the bare schema did not refuse")
    except SchemaNotMigratedError as exc:
        say(f"  the run refuses cleanly: {type(exc).__name__}: {exc}")
        ev(check="schema_refusal", refused=True, error_class=type(exc).__name__, message=str(exc))

    status, page = await admin_runs_page(pool, BARE_SCHEMA)
    notice_shown = "not installed" in page.lower() or "migrate" in page.lower()
    say(f"  the admin's runs page: HTTP {status}, the notice shown: {notice_shown}")
    ev(check="page_notice", status=status, notice_shown=notice_shown)
    await pool.close()


async def admin_runs_page(pool: object, schema: str) -> tuple[int, str]:
    """Render the admin's runs page against the given schema (the same
    router the sidecar mounts), and return the status and the HTML."""
    import httpx
    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType]  # Why: the admin router takes the asyncpg pool; this is the same object the scenario opened.
        schema=schema,
        auth_dependency=None,
        base_path="",
    )
    page_app = FastAPI()
    setup_admin_state(page_app, bundle)
    page_app.include_router(bundle.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=page_app), base_url="http://scenario"
    ) as client:
        response = await client.get("/workflows")
        return response.status_code, response.text


async def the_stale_definition(dsn: str) -> None:
    """Face two: the worker's definitions dropped a step the database's
    rows still carry. The row parks loudly; the definitions' return
    finishes the run."""
    from examples.scenario_grounds.defs_stale_v1 import app as v1_app
    from taskq.workflows import FlowRunner

    say("")
    say("FACE 2 — the stale definition. The run's rows say 'prep' then")
    say("'finish'; the worker's definitions say the prep body is gone:")
    pool = await open_pool(dsn)
    runner = FlowRunner(v1_app.get("ground_stale"), pool, SCHEMA)
    claim = await runner.create_flow(input={"subject": "the order"})

    stale_worker = await start_worker(
        schema=SCHEMA,
        actors_ref=V2_REF,
        queues=[QUEUE],
        log_path=scratch_dir("s7") / "worker-stale.log",
        extra_env={"TASKQ_SWEEP_INTERVAL": "1"},
    )
    await wait_for_worker_ready(pool, SCHEMA, stale_worker)

    async def parked() -> str | None:
        row = await pool.fetchrow(
            f"SELECT status, metadata->>'released_reason' AS reason "  # noqa: S608  # Why: the schema is this module's own constant identifier.
            f'FROM "{SCHEMA}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'prep'",
            str(claim.flow_id),
        )
        if row is not None and row["reason"] == "workflow-body-unresolvable":
            return str(row["status"])
        return None

    parked_state = await wait_until(
        parked, timeout=45, what="the row to park with its reason stamped"
    )
    say(f"  the prep row parks: status {parked_state!r}, the reason stamped on the row.")
    say("  (no failure, no attempt spent — the row waits for a worker that can resolve it)")
    ev(check="stale_row_parked", status=str(parked_state), reason="workflow-body-unresolvable")

    await stale_worker.stop()
    cured_worker = await start_worker(
        schema=SCHEMA,
        actors_ref=V1_REF,
        queues=[QUEUE],
        log_path=scratch_dir("s7") / "worker-cured.log",
        extra_env={"TASKQ_SWEEP_INTERVAL": "1"},
    )
    await wait_for_worker_ready(pool, SCHEMA, cured_worker)
    say("The definitions are whole again (the worker now carries them):")

    from examples.scenario_grounds._common import wait_all_runs_terminal

    census = await wait_all_runs_terminal(
        pool, SCHEMA, [str(claim.flow_id)], worker=cured_worker, timeout=120
    )
    root = await pool.fetchval(
        f'SELECT status FROM "{SCHEMA}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is this module's own constant identifier; the id is bound.
        str(claim.flow_id),
    )
    result = await runner.result(claim.flow_id)
    say(f"  the run finishes: {census.succeeded}/{census.total} succeeded, root {root}.")
    say(f"  the result: {result}")
    ev(check="stale_run_healed", root=str(root), succeeded=census.succeeded)

    await cured_worker.stop()
    await pool.close()


async def the_misconfigured_queue(dsn: str) -> None:
    """Face three: the worker's consumed set misses the workflow's
    queue. Under the strict gate the boot refuses, naming the queue."""
    say("")
    say("FACE 3 — the misconfigured queue. A worker that never consumes")
    say(f"the '{QUEUE}' queue, pointed at the workflow that lives there:")
    worker = await start_worker(
        schema=SCHEMA,
        actors_ref=V1_REF,
        queues=["default"],
        log_path=scratch_dir("s7") / "worker-strict.log",
        until_idle=True,
        extra_env={"TASKQ_QUEUES_STRICT": "1", "TASKQ_SWEEP_INTERVAL": "1"},
    )
    code = await worker.wait_for_exit(timeout=60)
    text = " ".join(worker.log_text().split())
    refusal = "workflow actor cohorts are projected onto queues" in text
    names_queue = "grounds-stale" in text and "does not consume" in text
    say(f"  the boot refuses: exit code {code}.")
    say(f"  the refusal names the queue: {names_queue}.")
    say("  the row's own fate without the gate: pending forever, no error, no log.")
    ev(check="queue_refusal", exit_code=code, refused=refusal, names_queue=names_queue)
    if code == 0:
        raise RuntimeError("the misconfigured worker booted anyway")


if __name__ == "__main__":
    asyncio.run(main())
