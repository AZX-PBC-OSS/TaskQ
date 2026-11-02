"""Scenario 4 — the reservation guard.

A rendering step declares a concurrency reservation: two seats, shared
with a competing plain actor. The plain actor takes both seats first
and holds them while it works. The step's turn comes — and finds no
seat free.

What the rows must show: the step WAITS (its row parks with the
denial's own stamp on it — the reason and the denied bucket, read off
the row; no failure, no attempt spent), the seats show who held them,
and the step goes again when the denial's retry hint says the seats may
be free — the hint is derived from the seats' own lease expiries, so
the reservation's short lease keeps the wait short.

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    TASKQ_REDIS_URL=redis://localhost:6379/0 \
    uv run python -m examples.scenario_grounds.s4_reservation_guard
"""

from __future__ import annotations

import asyncio

import asyncpg

from examples.scenario_grounds._common import (
    ev,
    flow_root_status,
    fresh_schema,
    open_pool,
    pg_dsn,
    say,
    scratch_dir,
    start_worker,
    wait_all_runs_terminal,
    wait_for_worker_ready,
    wait_until,
)
from examples.scenario_grounds.defs_reserved import app as reserved_app

SCHEMA = "grounds_s4"
ACTORS_REF = "examples.scenario_grounds.defs_reserved:ACTORS"
RESERVATION = "grounds_scene_slots"


async def main() -> None:
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    pool = await open_pool(dsn)

    from taskq import TaskQ
    from taskq.workflows import FlowRunner

    tq = TaskQ(pool=pool, schema=SCHEMA)
    await tq.open()
    try:
        worker = await start_worker(
            schema=SCHEMA,
            actors_ref=ACTORS_REF,
            queues=["grounds-scenes", "grounds-holder"],
            log_path=scratch_dir("s4") / "worker.log",
            max_concurrency=6,
            extra_env={"TASKQ_SWEEP_INTERVAL": "1"},
        )
        await wait_for_worker_ready(pool, SCHEMA, worker)

        say("Two stages share two rendering seats. The plain actor takes both first:")
        holder_handles = []
        for n in range(2):
            handle = await tq.enqueue(
                _holder_ref(), {"note": f"holder-{n}"}, queue="grounds-holder"
            )
            holder_handles.append(handle)

        async def both_seats_taken() -> bool:
            return await held_seats(pool) == 2

        await wait_until(both_seats_taken, timeout=30, what="the holder jobs to take both seats")
        holders_running = await holder_states(pool)
        say(f"  the holders took both seats: {holders_running}")

        say("Now the workflow's step arrives and wants a seat:")
        runner = FlowRunner(reserved_app.get("ground_reserved"), pool, SCHEMA)
        claim = await runner.create_flow(input={"scene": "docking-bay"})
        flow_id = str(claim.flow_id)

        # While the seats are held, the step waits. Whatever face the
        # wait takes — the claim holding the row back, or a claim that
        # was denied and parked — the row must NOT fail and must NOT
        # spend its attempt. The denial's stamp and the denied bucket
        # are read off the row itself, and the seats' occupancy is the
        # reservation table's own read — the evidence is the rows'
        # testimony, never literals the scenario made up.
        during, held_during = await step_state_while(pool, flow_id)
        say(f"  the step waits: {during}")
        say(f"  the seats, read off the reservation table while it waited: {held_during} held")
        ev(
            check="step_waits",
            status=during["status"],
            attempt=during["attempt"],
            released_reason=during["released_reason"],
            released_bucket=during["released_bucket"],
        )
        ev(check="never_failed_during_hold", error_class=during["error_class"])
        ev(
            check="seats_held_by_holder",
            held=held_during,
            bucket=during["released_bucket"] or RESERVATION,
        )

        say(
            "The holders stand up; the seats free; the step goes again at the"
            " denial's retry hint (the parked row's snooze rides the hint the"
            " denial derived from the seats' own lease expiries) and finishes:"
        )
        await wait_all_runs_terminal(pool, SCHEMA, [flow_id], worker=worker, timeout=240)
        final = await step_state(pool, flow_id)
        seats_after = await held_seats(pool)
        root = await flow_root_status(pool, SCHEMA, flow_id)
        say(f"  the step finished: {final}")
        say(f"  seats held after: {seats_after}")
        say(f"  the run's verdict: {root}")
        ev(check="step_proceeds", status=final["status"], attempt=final["attempt"])
        ev(check="seats_released", held=seats_after)
        ev(check="flow_completed", root=root, step=final["status"])

        await worker.stop()
        await pool.close()
        say("")
        say("The guard worked: no failure, no burned attempt — the step waited")
        say("out the denial's retry hint and finished when it went again.")
    finally:
        await tq.close()


def _holder_ref() -> object:
    from examples.scenario_grounds.defs_reserved import scene_holder

    return scene_holder


async def held_seats(pool: asyncpg.Pool) -> int:
    """How many of the reservation's seats are currently occupied."""
    count = await pool.fetchval(
        f'SELECT count(*) FROM "{SCHEMA}".reservation_slots '  # noqa: S608  # Why: the interpolated value is the scenario's own constant schema identifier; the bucket name is bound.
        "WHERE bucket_name = $1 AND job_id IS NOT NULL",
        RESERVATION,
    )
    return int(count)


async def holder_states(pool: asyncpg.Pool) -> dict[str, int]:
    rows = await pool.fetch(
        f'SELECT status, count(*) AS n FROM "{SCHEMA}".jobs '  # noqa: S608  # Why: the interpolated value is the scenario's own constant schema identifier.
        "WHERE actor = 'scene_holder' GROUP BY status",
    )
    return {str(r["status"]): int(r["n"]) for r in rows}


async def step_state(pool: asyncpg.Pool, flow_id: str) -> dict[str, object]:
    """The render step's row: status, attempt, and the deferral's stamp."""
    row = await pool.fetchrow(
        f"""
        SELECT status, attempt, error_class,
               metadata->>'released_reason' AS released_reason,
               metadata->>'released_bucket' AS released_bucket
        FROM "{SCHEMA}".jobs
        WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'render'
        """,  # noqa: S608  # Why: the schema is the scenario's own constant identifier; the id is bound.
        flow_id,
    )
    if row is None:
        return {"status": "missing", "attempt": -1, "error_class": None}
    return {
        "status": str(row["status"]),
        "attempt": int(row["attempt"]),
        "error_class": row["error_class"],
        "released_reason": row["released_reason"],
        "released_bucket": row["released_bucket"],
    }


async def step_state_while(pool: asyncpg.Pool, flow_id: str) -> tuple[dict[str, object], int]:
    """Sample the step's row and the seats' occupancy TOGETHER while the
    seats are held; the last sample taken during the hold is the wait's
    face (the sample after the seats free is not the wait's)."""
    import time

    deadline = time.monotonic() + 60
    last: dict[str, object] = {"status": "unobserved", "attempt": -1, "error_class": None}
    last_held = 0
    while time.monotonic() < deadline:
        held = await held_seats(pool)
        state = await step_state(pool, flow_id)
        if held < 2:
            break
        last, last_held = state, held
        await asyncio.sleep(0.2)
    return last, last_held


if __name__ == "__main__":
    asyncio.run(main())
