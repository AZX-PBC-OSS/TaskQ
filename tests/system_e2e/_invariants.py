"""The system invariants: the counter every scenario is asserted against.

These are SYSTEM invariants, not per-call pins. They run over the whole
tagged population, across BOTH the live table and the archive, and they
are asserted continuously in the sense that matters for a lifecycle
scenario: at every settle point (after every chaos window, before every
teardown) the counter must balance.

- Conservation: every enqueued job reaches exactly one terminal outcome.
  One row per job across jobs + jobs_archive (zero is a dropped job,
  two is a resurrected one); archived rows terminal; terminal rows whole
  (finished_at set); no running row with a lapsed lease and no live holder
  (limbo); every non-terminal non-running row claimable (scheduled_at
  set); and the attempt ledger reconciles, where reconcile means exactly
  what the terminal-write and release contracts define: the attempt a job
  TERMINALISED on always has its ledger row when that attempt was
  claimed (attempt >= 1; the terminal write inserts it, a terminal row
  without it is a half write), no ledger row sits above the attempt
  counter (a claim the counter never charged), and two honest shapes
  write no row: an INTERRUPTED attempt (the shutdown release arm,
  mark_interrupted - "an interruption is not an execution outcome") and
  a never-claimed job terminalised directly at attempt 0 (the bulk
  cancel's pending/scheduled arm). A counter above the ledger-row count
  is the interruption's honest signature, not a lost claim. The
  body-run evidence is the effects ledger's orphan check: every body run
  has the attempt row of the attempt that executed it.

- Exactly-once effects: reconciled per scenario against the effects
  ledger the actor bodies write.

- Progress-loss accounting: every consumed seq is durable (the PG
  progress_seq poll-state surface) even when the broker dropped the
  publish; subscriber-visible seqs are always <= the durable seq.

- Audit truthfulness: for every live tagged job, a terminal status comes
  with a state_change event naming that status, and a cancelled status
  comes with the operator's request columns still armed (an audit row
  exists iff the mutation happened, in both directions).

The conservation predicate is the pattern of the conservation chaos
suite's counter (tests/test_rt_conservation_chaos.py), rebuilt here as the
system tier's shared invariant so every lifecycle scenario asserts the
same balance and none can silently drop a clause.
"""

# ruff: noqa: S608  # Why: every query's schema identifier is validated by the settings/backend boundary and every value is $-bound; only the f-string interpolation of the schema name is flagged.

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

import asyncpg

from taskq.backend.statemachine import TERMINAL_STATUSES

_TERMINAL = tuple(TERMINAL_STATUSES)


async def conservation_violations(conn: asyncpg.Connection, schema: str, tag: str) -> list[str]:
    """The conservation counter, as a list of named violations."""
    rows = await conn.fetch(
        f"""
        WITH pop AS (
            SELECT id, status::text AS status, lock_expires_at, attempt,
                   finished_at, scheduled_at, 'jobs'::text AS src
            FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]
            UNION ALL
            SELECT id, status::text AS status, lock_expires_at, attempt,
                   finished_at, scheduled_at, 'archive'::text AS src
            FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]
        )
        SELECT id,
               count(*)::int AS copies,
               (count(*) FILTER (WHERE src = 'jobs'))::int AS in_jobs,
               (count(*) FILTER (WHERE src = 'archive'))::int AS in_archive,
               max(status::text) AS status,
               max(lock_expires_at) AS lock_expires_at,
               max(attempt)::int AS attempt,
               max(finished_at) AS finished_at,
               max(scheduled_at) AS scheduled_at
        FROM pop GROUP BY id
        """,
        tag,
    )
    violations: list[str] = []
    now = datetime.now(UTC)
    for row in rows:
        jid = row["id"]
        if row["copies"] != 1:
            violations.append(
                f"job {jid}: {row['copies']} rows exist (jobs={row['in_jobs']}, "
                f"archive={row['in_archive']}) - dropped or resurrected"
            )
            continue
        if row["in_archive"] and row["status"] not in _TERMINAL:
            violations.append(
                f"job {jid}: archived while status={row['status']} - the archiver moved a live row"
            )
        if row["status"] in _TERMINAL and row["finished_at"] is None:
            violations.append(f"job {jid}: terminal with finished_at NULL - half state")
        if (
            row["status"] == "running"
            and row["lock_expires_at"] is not None
            and row["lock_expires_at"] < now
        ):
            violations.append(f"job {jid}: running with a lapsed lease and no live holder - limbo")
        if (
            row["status"] not in _TERMINAL
            and row["status"] != "running"
            and row["scheduled_at"] is None
        ):
            violations.append(
                f"job {jid}: status={row['status']} with scheduled_at NULL - "
                "awaiting-reclaim rows must be claimable"
            )
        if row["in_jobs"]:
            ledger = await conn.fetch(
                f'SELECT attempt, outcome FROM "{schema}".job_attempts WHERE job_id = $1 '
                "ORDER BY attempt",
                jid,
            )
        else:
            ledger = await conn.fetch(
                f'SELECT attempt, outcome FROM "{schema}".job_attempts_archive '
                "WHERE job_id = $1 ORDER BY attempt",
                jid,
            )
        if (
            row["attempt"] >= 1
            and row["status"] in _TERMINAL
            and not any(int(r["attempt"]) == int(row["attempt"]) for r in ledger)
        ):
            violations.append(
                f"job {jid}: terminal at attempt {row['attempt']} with no ledger row "
                f"for that attempt (ledger: {[dict(r) for r in ledger]}) - the "
                "terminal write's own attempt row is missing"
            )
        if any(int(r["attempt"]) > int(row["attempt"]) for r in ledger):
            violations.append(
                f"job {jid}: ledger rows above the attempt counter "
                f"{row['attempt']} (ledger: {[dict(r) for r in ledger]}) - a claim "
                "the counter never charged"
            )
    return violations


async def audit_violations(conn: asyncpg.Connection, schema: str, tag: str) -> list[str]:
    """Audit truthfulness for the tagged LIVE population.

    The trail is bidirectional: a terminal status must carry the
    state_change event that names it (a terminal row with no event is a
    mutation nobody wrote down), and a state_change to 'cancelled' must
    belong to a row that IS cancelled (an audit row for a mutation that
    did not happen is a lie in the other direction). Archived rows are out
    of scope here: pruning deletes the event rows with the job (FK
    cascade), which is why the conservation counter, not the event trail,
    is the archive's witness.
    """
    rows = await conn.fetch(
        f"""
        SELECT j.id, j.status::text AS status,
               (SELECT count(*)::int FROM "{schema}".job_events e
                WHERE e.job_id = j.id AND e.kind = 'state_change'
                  AND e.detail->>'to_state' = j.status::text) AS matching_events
        FROM "{schema}".jobs j
        WHERE j.tags @> ARRAY[$1::text] AND j.status::text = ANY($2::text[])
        """,
        tag,
        list(_TERMINAL),
    )
    violations: list[str] = []
    for row in rows:
        if row["matching_events"] < 1:
            violations.append(
                f"job {row['id']}: terminal status {row['status']} with no "
                "state_change event naming it - a mutation with no audit row"
            )
    ghosts = await conn.fetchval(
        f"""
        SELECT count(*)::int FROM "{schema}".job_events e
        JOIN "{schema}".jobs j ON j.id = e.job_id
        WHERE j.tags @> ARRAY[$1::text] AND e.kind = 'state_change'
          AND e.detail->>'to_state' = 'cancelled' AND j.status <> 'cancelled'
        """,
        tag,
    )
    if ghosts:
        violations.append(
            f"{ghosts} state_change('cancelled') events on jobs that are not "
            "cancelled - an audit row for a mutation that did not happen"
        )
    return violations


async def settle_terminal(
    conn: asyncpg.Connection, schema: str, tag: str, cap_secs: float
) -> dict[str, int]:
    """Wait until every tagged job is terminal (in the live table or gone
    to the archive), then return the live status counts. The cap is the
    "no permanently-stuck state" bound: a population that cannot settle
    fails the scenario by construction."""
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        row = await conn.fetchrow(
            f'SELECT count(*)::int AS n FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status::text != ALL($2::text[])",
            tag,
            list(_TERMINAL),
        )
        assert row is not None
        if row["n"] == 0:
            counts = await conn.fetch(
                "SELECT status::text AS status, count(*)::int AS n "
                f'FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] GROUP BY status',
                tag,
            )
            return {r["status"]: r["n"] for r in counts}
        await asyncio.sleep(0.25)
    raise AssertionError(
        f"NOT SETTLED within {cap_secs}s: tagged jobs never reached terminal - "
        "a dropped or livelocked population"
    )


async def effect_ledger_violations(conn: asyncpg.Connection, schema: str, tag: str) -> list[str]:
    """Exactly-once effects, reconciled against the attempt ledger.

    One effects row per (job_id, attempt, actor, kind): a re-run is a
    NEW attempt, never a second run of the same one, and no effects row
    exists without the matching claim row (a body ran where nothing was
    dispatched to it) - with ONE documented exception, the same shape
    the ledger paragraph above records: an INTERRUPTED attempt (the
    shutdown release arm, ``mark_interrupted``) writes no claim row, so
    a body that recorded effects and was then interrupted by a drain
    has effects whose attempt the ledger intentionally lacks. Each such
    release bumps the row's ``interrupt_count`` (and writes one
    ``interrupted`` state_change event), so the reconciliation allows a
    job at most ``interrupt_count`` no-claim attempts and flags anything
    beyond it: a body run with neither a claim row behind it nor
    interruption evidence to cover it is still an orphan. Runs against
    the tagged population only; joins live and archived attempt ledgers.
    """
    doubles = await conn.fetchval(
        f"""
        SELECT count(*)::int FROM (
            SELECT e.job_id, e.attempt, e.actor, e.kind
            FROM "{schema}".sys_effects e
            WHERE e.job_id IN (
                SELECT id FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]
                UNION ALL
                SELECT id FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]
            )
            GROUP BY e.job_id, e.attempt, e.actor, e.kind HAVING count(*) > 1
        ) d
        """,
        tag,
    )
    orphans = await conn.fetchval(
        f"""
        WITH tagged AS (
            SELECT id, interrupt_count FROM "{schema}".jobs
            WHERE tags @> ARRAY[$1::text]
            UNION ALL
            SELECT id, interrupt_count FROM "{schema}".jobs_archive
            WHERE tags @> ARRAY[$1::text]
        ),
        no_claim AS (
            SELECT e.job_id, e.attempt
            FROM "{schema}".sys_effects e
            WHERE e.job_id IN (SELECT id FROM tagged)
            AND NOT EXISTS (
                SELECT 1 FROM "{schema}".job_attempts a
                WHERE a.job_id = e.job_id AND a.attempt = e.attempt
            )
            AND NOT EXISTS (
                SELECT 1 FROM "{schema}".job_attempts_archive a
                WHERE a.job_id = e.job_id AND a.attempt = e.attempt
            )
            GROUP BY e.job_id, e.attempt
        )
        SELECT count(*)::int FROM (
            SELECT n.job_id
            FROM no_claim n JOIN tagged t ON t.id = n.job_id
            GROUP BY n.job_id, t.interrupt_count
            HAVING count(*) > t.interrupt_count
        ) o
        """,
        tag,
    )
    violations: list[str] = []
    if doubles:
        violations.append(
            f"{doubles} (job, attempt, actor) effects rows ran the body twice - "
            "a double-run against one attempt"
        )
    if orphans:
        detail = await conn.fetch(
            f"""
            WITH tagged AS (
                SELECT id, interrupt_count FROM "{schema}".jobs
                WHERE tags @> ARRAY[$1::text]
                UNION ALL
                SELECT id, interrupt_count FROM "{schema}".jobs_archive
                WHERE tags @> ARRAY[$1::text]
            ),
            no_claim AS (
                SELECT e.job_id, e.attempt
                FROM "{schema}".sys_effects e
                WHERE e.job_id IN (SELECT id FROM tagged)
                AND NOT EXISTS (
                    SELECT 1 FROM "{schema}".job_attempts a
                    WHERE a.job_id = e.job_id AND a.attempt = e.attempt
                )
                AND NOT EXISTS (
                    SELECT 1 FROM "{schema}".job_attempts_archive a
                    WHERE a.job_id = e.job_id AND a.attempt = e.attempt
                )
                GROUP BY e.job_id, e.attempt
            )
            SELECT n.job_id, n.attempt, t.interrupt_count,
                   j.status::text AS live_status, j.attempt AS live_attempt
            FROM no_claim n
            JOIN tagged t ON t.id = n.job_id
            LEFT JOIN "{schema}".jobs j ON j.id = n.job_id
            ORDER BY n.job_id, n.attempt
            """,
            tag,
        )
        violations.append(
            f"{len(detail)} attempt(s) ran a body with neither a claim row behind them "
            f"nor interruption evidence to cover it - a body run with no dispatch: "
            f"{[dict(r) for r in detail[:10]]}"
        )
    return violations


async def delete_tagged(conn: asyncpg.Connection, schema: str, tag: str) -> None:
    """Teardown: remove the tagged population from both tables.

    Events and attempts cascade from jobs. Order matters: the archive
    first (no FK between the tables, but the delete is bounded the same
    way), then the live rows.
    """
    await conn.execute(f'DELETE FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]', tag)
    await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)


async def assert_balanced(conn: asyncpg.Connection, schema: str, tag: str) -> dict[str, int]:
    """Run the shared invariants over the tagged population and return the
    live status counts. Every scenario ends here; a violation names its
    own defect in the failure message."""
    counts = await settle_terminal(conn, schema, tag, cap_secs=120.0)
    violations = await conservation_violations(conn, schema, tag)
    violations += await audit_violations(conn, schema, tag)
    assert not violations, "the system invariants do not balance after settle:\n" + "\n".join(
        violations
    )
    return counts


async def assert_effects_balance(conn: asyncpg.Connection, schema: str, tag: str) -> None:
    """The effects-ledger half of the balance (scenarios whose actors record)."""
    violations = await effect_ledger_violations(conn, schema, tag)
    assert not violations, "the effects ledger does not reconcile:\n" + "\n".join(violations)
