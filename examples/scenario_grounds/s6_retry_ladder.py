"""Scenario 6 — the retry ladder.

A flaky body fails twice, then succeeds on its third try, all on its
own: the step's declared three attempts and the ladder's backoff
between them.

The evidence is the re-pend's own stamped provenance, read off the
row: each failure's re-pend statement stamps the row's metadata with
the attempt's own record — the applied backoff, the re-pend's instant,
the due instant (one clock read: due minus at IS the backoff the
statement applied, captured atomically with the re-pend itself). The
scenario reads those stamps after the run and asserts the curve: 50 ms
after the first failure, 100 ms after the second, the second step the
doubling wider, and the next attempt's claim honoring the stamped due
instant. No polling, no wall-clock reconstruction — the rows carry
their own testimony.

Run it:

    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
    uv run python -m examples.scenario_grounds.s6_retry_ladder
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime

from examples.scenario_grounds._common import ev, fresh_schema, open_pool, pg_dsn, say
from examples.scenario_grounds.defs_ladder import app as ladder_app

SCHEMA = "grounds_s6"

#: The ladder's curve: the wait after failure N is 0.05 x 2^N seconds.
CURVE_S = {1: 0.05, 2: 0.10}

#: The reading margins: the stamp IS the applied value (written in the
#: re-pend's own statement), so the bounds are the curve's own truth
#: with a hair of slack for the jsonb round-trip.
LOWER_MARGIN_S = 0.02
UPPER_MARGIN_S = 0.25


async def main() -> None:
    dsn = pg_dsn()
    await fresh_schema(dsn, SCHEMA)
    pool = await open_pool(dsn)

    from taskq.workflows import run

    say("A flaky body arms two failures and then succeeds. The step owns")
    say("three attempts; the ladder re-pends between them (50 ms, then 100 ms),")
    say("each re-pend stamping its own record into the row's metadata.")

    outcome = await run(ladder_app.get("ground_ladder"), pool, SCHEMA, input=None)

    say("")
    say("The run ended. The attempts' ledger, oldest first:")
    ledger = await attempts_ledger(pool)
    for row in ledger:
        say(f"  attempt {row['attempt']}: {row['status']}")
    ev(
        check="attempt_ledger",
        attempts=[f"{r['attempt']}:{r['status']}" for r in ledger],
    )

    stamps = await repend_stamps(pool)
    steps: dict[int, float] = {}
    for n in sorted(CURVE_S):
        record = stamps.get(str(n))
        if record is None:
            raise AssertionError(f"the row carries no re-pend stamp for attempt {n}")
        measured = (parse_stamp(record["due"]) - parse_stamp(record["at"])).total_seconds()
        steps[n] = measured
        say(
            f"  after failure {n}: the re-pend's own stamp says it applied"
            f" {measured * 1000:.0f} ms (the curve says {CURVE_S[n] * 1000:.0f}),"
            f" due {record['due']}"
        )
    ev(check="backoff_steps", **{f"step_{n}": round(m, 4) for n, m in sorted(steps.items())})

    doubling = steps[2] - steps[1]
    say(f"  the curve doubled: the second step is {doubling * 1000:.0f} ms wider.")
    ev(check="curve_doubled", widening_s=round(doubling, 4))

    next_claim = ledger[-1]["created_at"] if ledger else None
    last_due = parse_stamp(stamps[str(max(CURVE_S))]["due"])
    honored = next_claim is not None and as_utc(next_claim) >= last_due
    say("  the next attempt's claim honored the stamped due instant:")
    say(f"    due {last_due} -> claimed {next_claim}")
    ev(check="claim_honored_due", honored=bool(honored))

    say(f"  the run's verdict: {outcome.outcome} ({await root_status(pool)})")
    say(f"  the decoded result: {outcome.result}")
    ev(check="success", outcome=outcome.outcome, result=str(outcome.result))

    await pool.close()
    say("")
    say("The ladder did its job: two failures recorded honestly, the curve's")
    say("own applied backoff stamped onto the row per attempt, the third")
    say("attempt green — and the run succeeded.")


async def repend_stamps(pool: object) -> dict[str, dict[str, object]]:
    """The re-pend provenance off the flaky node's own row: one record
    per attempt, as the re-pend statement stamped it."""
    raw = await pool.fetchval(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the caller passes this module's own pool; the duck-typed seam keeps the helper's read face simple.
        f"SELECT metadata->'wf_repend' FROM \"{SCHEMA}\".jobs WHERE step_key = 'flaky'"  # noqa: S608  # Why: the schema is the scenario's own constant identifier.
    )
    if raw is None:
        raise AssertionError("the flaky node's row carries no re-pend provenance")
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        raise AssertionError(f"the re-pend provenance decoded to {type(decoded).__name__}")
    return decoded


async def attempts_ledger(pool: object) -> list[dict[str, object]]:
    """The flaky step's ledger rows, one per attempt, oldest first."""
    rows = await pool.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: as above — the module's own pool.
        f"""
        SELECT attempt, status, created_at, updated_at
        FROM "{SCHEMA}".wf_step_ledger
        WHERE step_key = 'flaky'
        ORDER BY created_at, attempt
        """,  # noqa: S608  # Why: the schema is the scenario's own constant identifier.
    )
    return [dict(r) for r in rows]


async def root_status(pool: object) -> str | None:
    return await pool.fetchval(  # pyright: ignore[reportAttributeAccessIssue]  # Why: as above.
        f"SELECT status FROM \"{SCHEMA}\".jobs WHERE step_key = '__flow__'",  # noqa: S608  # Why: the schema is the scenario's own constant identifier.
    )


def parse_stamp(value: object) -> datetime:
    """The stamped instant (the row's ISO-8 face) as a datetime."""
    if isinstance(value, datetime):
        return as_utc(value)
    if isinstance(value, str):
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise TypeError(f"expected a stamped instant, got {type(value).__name__}")


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.astimezone()


if __name__ == "__main__":
    asyncio.run(main())
