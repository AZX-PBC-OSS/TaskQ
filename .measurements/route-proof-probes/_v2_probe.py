"""V2's spot-verify probe: the stamper's per-body hash on the @app.actor
path — TWO different actor bodies must stamp DIFFERENT code_version
values (the wrapper's unwrap: the hash reads the INNER function), and
each stamp must equal the body's own compute_code_version."""

import asyncio

import asyncpg
from pydantic import BaseModel

from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp
from taskq.workflows._version import compute_code_version

DSN = "postgresql://postgres:taskq@localhost:5768/taskq"
SCHEMA = "v2probe"


class Ingest(BaseModel):
    n: int = 1


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    from taskq.migrate import apply_pending

    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await apply_pending(conn, schema=SCHEMA)

    app = WorkflowApp()

    @app.actor(name="alpha_body", queue="v2q")
    async def alpha(ctx: StepContext, params: Ingest) -> Ingest:
        return params  # body A

    @app.actor(name="beta_body", queue="v2q")
    async def beta(ctx: StepContext, params: Ingest) -> Ingest:
        return Ingest(n=params.n + 1)  # body B — DIFFERENT source

    wf_app = WorkflowApp()

    # wire with the actor handles as steps
    from taskq.workflows import build, step

    @wf_app.workflow("v2probe2")
    def wf2() -> Promise[Ingest]:
        a = step(alpha, Ingest(n=1), key="alpha")
        b = step(beta, a, key="beta")
        return build(b)

    runner = FlowRunner(wf_app.get("v2probe2"), None, SCHEMA) if False else None
    pool = await asyncpg.create_pool(DSN)
    runner = FlowRunner(wf_app.get("v2probe2"), pool, SCHEMA)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id, until="terminal")
    print("drive:", outcome)

    rows = await conn.fetch(
        f'SELECT step_key, code_version, status FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY created_at",
        flow_id,
    )
    stamps = {}
    for r in rows:
        print(r["step_key"], r["status"], r["code_version"])
        stamps[r["step_key"]] = r["code_version"]

    # the bodies' own hashes (the inner functions, through the same seam)
    from taskq.workflows.api._hints import inner_fn, own_source

    expected = {}
    for key, fn in (("alpha", alpha), ("beta", beta)):
        target = inner_fn(fn)
        source = own_source(target)
        expected[key] = compute_code_version(
            getattr(target, "__module__", ""),
            getattr(target, "__qualname__", "") or "",
            source,
        )
        print(f"expected {key}:", expected[key])

    assert stamps["alpha"] and stamps["beta"], f"UNSTAMPED: {stamps}"
    assert stamps["alpha"] != stamps["beta"], "PER-BODY HASH DEAD: both nodes the same constant"
    assert stamps["alpha"] == expected["alpha"], (stamps["alpha"], expected["alpha"])
    assert stamps["beta"] == expected["beta"], (stamps["beta"], expected["beta"])
    print("V2 VERIFIED: per-body stamps, both equal each body's own hash")

    await pool.close()
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()


asyncio.run(main())
