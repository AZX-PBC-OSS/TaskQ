"""THE UNVERIFIABLE-SOURCE SILENCE — the live repro (AMENDMENT (B)'s red).

An unverifiable body source (exec'd — the cache cannot hash what it
cannot read) = ``step-cache-unaddressable`` = SILENTLY uncached: the
wiring accepts cache=True, every run succeeds, every run re-executes the
body, and the ONLY trace is a log line the operator never reads. The
operator learns the face from a MISSING HIT.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, run, step

DSN = "postgresql://postgres:taskq@localhost:5810/taskq"
SCHEMA = "unverifiable_repro"


class Report(BaseModel):
    ref: str


CALLS = {"ghost": 0}

_NS: dict[str, object] = {"Report": Report, "CALLS": CALLS, "StepContext": StepContext}
exec(
    """
async def ghost_body(ctx: StepContext, d: Report) -> Report:
    CALLS['ghost'] += 1
    return Report(ref=d.ref)
""",
    _NS,
)
ghost_body = _NS["ghost_body"]

app = WorkflowApp()


@app.workflow("ghost_flow")
def flow() -> Promise[object]:
    return build(step(ghost_body, Report(ref="r1"), key="ghost", cache=True))  # type: ignore[arg-type]


async def main() -> None:
    conn0 = await asyncpg.connect(DSN)
    await conn0.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn0.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)
    compiled = app.get("ghost_flow")

    for i, key in enumerate(("ghost:1", "ghost:2"), 1):
        r = await run(compiled, pool, SCHEMA, input=None, key=key)
        print(f"=== RUN {i} ({key}) ===")
        print("status:", await _status(pool, str(r.flow_id)))
        print(
            "result:",
            await pool.fetchval(
                f'SELECT result->\'value\'->>\'ref\' FROM "{SCHEMA}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ghost'",
                r.flow_id,
            ),
        )
    print("calls:", CALLS, "(cache=True accepted at wiring; the body ran EVERY run)")
    rows = await pool.fetch(f'SELECT content_address FROM "{SCHEMA}".wf_step_cache')
    print("cache rows:", len(rows), "(the store never saw an address)")
    meta = await pool.fetchval(
        f'SELECT code_version FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ghost'",
        r.flow_id,
    )
    print("code_version stamp:", meta, "(the hash could not read the source)")
    await pool.close()


async def _status(pool: asyncpg.Pool, flow_id: str) -> str:
    return await pool.fetchval(f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1', flow_id)


asyncio.run(main())
