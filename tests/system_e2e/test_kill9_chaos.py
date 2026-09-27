"""The kill9 SIGKILL campaign, part 1: the worker subprocess dies mid-phase.

The graceful tiers prove recovery under cooperative failure (SIGTERM
ladders, drains, interrupts). Nothing here is cooperative: the worker
process dies by real ``kill -9`` at deterministic phase windows, each
seam driven by the harness's fault-injection switch
(``tests.system_e2e._kill_entry``), so the kill lands inside the named
window by construction, not by wall-clock luck:

* mid-claim (``after_claim``): the claim statement has committed, the
  body has not started - the row is running under a LIVE lease with no
  execution behind it;
* mid-body (external, on observed readiness): the body recorded its
  ``start`` effect and is holding - a real execution is in flight;
* mid-commit, uncommitted side (``before_terminal``): the body's effects
  are durable and the terminal write has NOT been attempted;
* mid-commit, committed side (``after_terminal``): the fused terminal
  statement has landed status + result + attempt row + event, and
  nothing after it (publish, fanout, next heartbeat) has run - the kill
  between the ledger write and the next heartbeat;
* mid-heartbeat (``after_heartbeat``): a beat that actually extended an
  in-flight job's lease has just committed - the kill lands inside the
  visibility window at its widest (the lease is at its FRESHEST, so the
  reclaim must wait out the full lapse, not inherit a stale one).

After every death the OS reaps the process (``returncode == -9`` is
asserted: a graceful exit would not prove the kill landed) and the
assertions are the same system invariants every scenario in the tier
runs, over the tagged population:

* NO STUCK LEASE: within the reclaim budget (leader-lease lapse + wake
  jitter + sweep tick + the re-pend's own backoff, each term derived
  from the settings the workers run with) the row must leave ``running``
  - reclaimed into the re-pend with its ``crashed`` attempt row;
* EXACTLY ONCE-OR-VISIBLE: every attempt runs at most once (the effects
  ledger's per-(job, attempt) uniqueness), the job reaches exactly one
  terminal outcome across jobs + jobs_archive, and the attempt ledger
  reconciles (a crash attempt gets its ``crashed`` row from the reclaim
  sweep, a terminal attempt its row from the terminal write);
* NO ORPHANED SIDE EFFECTS: every effects row has the claim-row evidence
  of the attempt that executed it (no body ran where nothing was
  dispatched);
* the terminal fence: on the committed side the replacement worker must
  NOT re-run the job (no second attempt, no second effects row);
* the replacement drains: a trailing enqueue runs to success on it.
"""

# ruff: noqa: S608  # Why: every query's schema identifier is the fixture-validated module schema; every value is $-bound.

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple

import pytest

from taskq.testing.health import unique_health_sock_path
from tests.system_e2e._harness import (
    _BASE_ENV,  # pyright: ignore[reportPrivateUsage]  # Why: the fleet env is the tier's shared constant; the kill harness spawns with the SAME knobs the graceful scenarios prove.
    WorkerProc,
    reap,
    wait_worker_ready,
)
from tests.system_e2e._invariants import (
    assert_effects_balance,
    conservation_violations,
    delete_tagged,
    settle_terminal,
)
from tests.system_e2e._kill_actors import (
    Kill9Payload,
    kill9_one_life,
    kill9_slow,
    kill9_starting,
)
from tests.system_e2e.actors import RollPayload, sys_keyed

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [
    pytest.mark.integration,
    pytest.mark.system,
    pytest.mark.timeout(300),
]

_TAG = "kill9"

# ── The timing knobs the workers run with (mirror _harness._BASE_ENV) ───
_LEASE_S = 8.0
_SWEEP_INTERVAL_S = 1.0
_HEARTBEAT_INTERVAL_S = 0.5
_POLL_INTERVAL_S = 0.05

# The campaign shortens the leader lease from the 40s default: after a
# kill, the reclaim runs on the LEADER sweep, and the dead worker's lease
# must lapse before a replacement can win leadership. 6.0 >= 4 beats (the
# resolved floor max(lease, 4 * heartbeat_interval) = max(6, 2) = 6).
_LEADER_LEASE_S = 6.0
_LEADER_WAKE_JITTER_S = 0.25  # settings.leader_wake_jitter default

# The reclaim re-pend's own deferral: the kill9 actors stamp base=1s,
# jitter=0, so attempt 1's crash re-pend waits exactly 1.0s.
_RECLAIM_BACKOFF_S = 1.0

# Measured-phase budget, every term from the settings above: leadership
# handover (lease lapse + wake jitter) + the reclaim sweep's tick + the
# re-pend backoff + the dispatch poll + the attempt-2 body (the 0.2s
# attempt-aware tail) + the terminal write, plus a margin for co-tenant
# load. Every bound here is a CAP a poll asserts against, never a sleep.
_RECLAIM_OBSERVE_CAP_S = (
    _LEADER_LEASE_S + _LEADER_WAKE_JITTER_S + 2 * _SWEEP_INTERVAL_S + _RECLAIM_BACKOFF_S + 15.0
)
_SETTLE_CAP_S = _RECLAIM_OBSERVE_CAP_S + _POLL_INTERVAL_S + 30.0

_KILL_LANDS_CAP_S = 30.0  # worker boot + the enqueue's dispatch are sub-second each


def _spawn_kill_worker(
    pg_dsn: str, schema: str, *, seam: str | None = None, tag: str = "kill9"
) -> WorkerProc:
    """One kill-harness worker subprocess: the tier's fleet env, the
    campaign's shortened leader lease, and (optionally) the kill seam."""
    sock_path = _health_sock(tag)
    env = {**os.environ, **_BASE_ENV}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEALTH_SOCKET_PATH": sock_path,
            "TASKQ_LEADER_LEASE": str(_LEADER_LEASE_S),
        }
    )
    if seam is not None:
        env["TASKQ_KILL9_SEAM"] = seam
    proc: subprocess.Popen[bytes] = (
        subprocess.Popen(  # Why: fixed argv, no shell, this interpreter, project-owned module.
            [sys.executable, "-m", "tests.system_e2e._kill_entry"],
            env=env,
            cwd=os.environ.get("TASKQ_REPO_ROOT", os.getcwd()),
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
    )
    return WorkerProc(proc=proc, sock_path=sock_path)


def _health_sock(tag: str) -> str:
    return unique_health_sock_path(f"syse2e-{tag}")


async def _wait_killed(worker: WorkerProc, cap_s: float, phase: str) -> int:
    """Wait for the OS to reap the killed process; the -9 returncode is the
    proof the SIGKILL landed (a graceful exit would not)."""
    deadline = time.monotonic() + cap_s
    while time.monotonic() < deadline:
        rc = worker.poll()
        if rc is not None:
            assert rc == -9, f"the {phase} worker exited rc={rc} before the SIGKILL landed"
            return rc
        await asyncio.sleep(0.05)
    raise AssertionError(f"the {phase} kill did not land within {cap_s}s (worker still alive)")


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: object) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f"SELECT status::text AS status, attempt AS attempt, "
        f"lock_expires_at AS lock_expires_at, finished_at AS finished_at, "
        f'scheduled_at AS scheduled_at FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )


async def _wait_effect(
    conn: asyncpg.Connection, schema: str, job_id: object, kind: str, attempt: int
) -> None:
    """Block until the effects row is observable - the readiness signal the
    external kill timing derives from, never a blind sleep."""
    deadline = time.monotonic() + _KILL_LANDS_CAP_S
    while time.monotonic() < deadline:
        n = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects '
            "WHERE job_id = $1 AND kind = $2 AND attempt = $3",
            job_id,
            kind,
            attempt,
        )
        if n:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"effect {kind!r}@{attempt} never observed for job {job_id}")


async def _status_counts_across(conn: asyncpg.Connection, schema: str, tag: str) -> dict[str, int]:
    """Terminal outcome counts across BOTH tables (settle's live-only
    counts cannot see the archive's share)."""
    rows = await conn.fetch(
        f"""
        SELECT status::text AS status, count(*)::int AS n FROM (
            SELECT status FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]
            UNION ALL
            SELECT status FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]
        ) s GROUP BY status
        """,
        tag,
    )
    return {r["status"]: r["n"] for r in rows}


async def _assert_no_stuck_lease(conn: asyncpg.Connection, schema: str, job_id: object) -> None:
    """From death to reclaim: the row must LEAVE ``running`` inside the
    reclaim budget (the lease lapses, the leader sweep re-pends). A row
    still running at the cap is a stuck lease - the visibility timeout did
    not recover the kill."""
    deadline = time.monotonic() + _RECLAIM_OBSERVE_CAP_S
    while time.monotonic() < deadline:
        row = await _job_row(conn, schema, job_id)
        if row is None or row["status"] != "running":
            return
        await asyncio.sleep(0.25)
    row = await _job_row(conn, schema, job_id)
    raise AssertionError(
        f"STUCK LEASE: job {job_id} still status={row and row['status']} after "
        f"{_RECLAIM_OBSERVE_CAP_S}s (leader lease {_LEADER_LEASE_S}s + sweep "
        f"{_SWEEP_INTERVAL_S}s + backoff {_RECLAIM_BACKOFF_S}s + margin) - the "
        "visibility timeout did not recover the kill"
    )


async def _assert_attempts(
    conn: asyncpg.Connection, schema: str, job_id: object, expected: list[tuple[int, str]]
) -> None:
    """The attempt ledger, read across BOTH ledgers: a terminal row the
    archiver has already pruned carries its attempts in
    ``job_attempts_archive``, and the assertion must see the archived
    shape too (conservation reads it the same way)."""
    rows = await conn.fetch(
        f"""
        SELECT attempt, outcome::text AS outcome FROM (
            SELECT attempt, outcome FROM "{schema}".job_attempts WHERE job_id = $1
            UNION ALL
            SELECT attempt, outcome FROM "{schema}".job_attempts_archive WHERE job_id = $1
        ) a ORDER BY attempt
        """,
        job_id,
    )
    got = [(int(r["attempt"]), r["outcome"]) for r in rows]
    assert got == expected, (
        f"job {job_id}: attempt ledger {got} != {expected} - the crash/terminal "
        "attempt rows do not reconcile with the kill's phase"
    )


class PhaseResult(NamedTuple):
    """What one phase's choreography observed."""

    job_id: object
    row_after_death: dict[str, Any]
    counts: dict[str, int]
    death_wall: datetime


async def _run_phase(
    conn: asyncpg.Connection,
    schema: str,
    pg_dsn: str,
    client: TaskQ,
    *,
    seam: str | None,
    actor: Any,
    payload: Kill9Payload | None = None,
    kill_after_effect: tuple[str, int] | None = None,
) -> PhaseResult:
    """One phase's choreography: spawn the kill-worker (seam or observed-
    effect kill), prove the death was a SIGKILL, snapshot the row, spawn
    the replacement, prove the reclaim, settle, and return the
    observations. Full cleanup is the CALLER's finally (the worker handles
    are returned, not owned here)."""
    killer = _spawn_kill_worker(pg_dsn, schema, seam=seam, tag="kill9-a")
    replacement: WorkerProc | None = None
    try:
        wait_worker_ready(killer)
        handle = await client.enqueue(actor, payload or Kill9Payload(), tags=[_TAG])
        job_id = handle.job_id

        if kill_after_effect is not None:
            # The external mid-body kill: the timing derives from the
            # observed effect row, not a sleep.
            kind, attempt = kill_after_effect
            await _wait_effect(conn, schema, job_id, kind, attempt)
            killer.proc.kill()  # Why: the kill IS the experiment.

        await _wait_killed(killer, _KILL_LANDS_CAP_S, phase=seam or "mid-body")
        death_wall = datetime.now(UTC)

        row = await _job_row(conn, schema, job_id)
        assert row is not None, f"job {job_id} vanished from the jobs table"
        row_after_death = dict(row)

        # The replacement: identical env, no seam. It inherits the reclaim
        # (leadership handover), the re-pend, and the drain.
        replacement = _spawn_kill_worker(pg_dsn, schema, seam=None, tag="kill9-b")
        wait_worker_ready(replacement)
        # The replacement drains NEW work too, not just the reclaim.
        await client.enqueue(kill9_slow, Kill9Payload(sleep=0.2), tags=[_TAG])

        await _assert_no_stuck_lease(conn, schema, job_id)

        await settle_terminal(conn, schema, _TAG, cap_secs=_SETTLE_CAP_S)
        violations = await conservation_violations(conn, schema, _TAG)
        assert not violations, (
            f"the invariants do not balance after the {seam or 'mid-body'} kill:\n"
            + "\n".join(violations)
        )
        await assert_effects_balance(conn, schema, _TAG)
        counts = await _status_counts_across(conn, schema, _TAG)
        return PhaseResult(
            job_id=job_id, row_after_death=row_after_death, counts=counts, death_wall=death_wall
        )
    finally:
        reap(killer)
        if replacement is not None:
            reap(replacement)
        # The TAG's cleanup belongs to the CALLER's finally: the phase's
        # return value feeds the caller's ledger assertions, which must
        # run against a population that still exists.


# ── The phases ──────────────────────────────────────────────────────────


@pytest.mark.load_sensitive
async def test_sigkill_mid_claim_leaves_a_reclaimable_claim_not_a_stuck_lease(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """Kill the instant the claim commits: no body ran, the row is running
    under a live lease. Recovery is the lease-lapse reclaim (a ``crashed``
    attempt row + the re-pend), the re-run lands under a NEW attempt, and
    the effects ledger proves the killed claim never executed a body."""
    schema = module_pg_schema.schema_name
    try:
        result = await _run_phase(
            sys_ledger,
            schema,
            pg_dsn,
            sys_client,
            seam="after_claim",
            actor=kill9_slow,
            payload=Kill9Payload(sleep=2.0),
        )
        row = result.row_after_death
        assert row["status"] == "running", (
            f"the claim was not observed committed at the kill: {row}"
        )
        assert row["attempt"] == 1, f"the kill did not land on the first claim: {row}"
        assert row["lock_expires_at"] is not None and row["lock_expires_at"] > result.death_wall, (
            f"the claim's lease was not live at the kill (death {result.death_wall}): {row}"
        )
        job_id = result.job_id
        # The killed claim ran NO body: no effects row exists for attempt 1.
        n = await sys_ledger.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects WHERE job_id = $1 AND attempt = 1',
            job_id,
        )
        assert n == 0, (
            f"the killed claim's body ran {n} effect(s) - a body where nothing was dispatched"
        )
        await _assert_attempts(sys_ledger, schema, job_id, [(1, "crashed"), (2, "succeeded")])
        counts = result.counts
        assert counts.get("succeeded", 0) >= 2, (
            f"the re-run or the trailing job never completed: {counts}"
        )
        assert set(counts) <= {"succeeded"}, f"the kill manufactured outcomes: {counts}"
    finally:
        await delete_tagged(sys_ledger, schema, _TAG)


@pytest.mark.load_sensitive
async def test_sigkill_mid_body_interrupts_the_run_and_the_reclaim_reruns_once(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """Kill mid-body (on the observed ``start`` effect): the execution is
    real, so the reclaim must record the attempt as ``crashed`` and re-run
    the body ONCE - never zero times (dropped) and never twice against one
    attempt (the exactly-once-or-visible contract)."""
    schema = module_pg_schema.schema_name
    try:
        result = await _run_phase(
            sys_ledger,
            schema,
            pg_dsn,
            sys_client,
            seam=None,
            actor=kill9_starting,
            payload=Kill9Payload(sleep=20.0),
            kill_after_effect=("start", 1),
        )
        row = result.row_after_death
        assert row["status"] == "running", f"the body was not in flight at the kill: {row}"
        job_id = result.job_id
        await _assert_attempts(sys_ledger, schema, job_id, [(1, "crashed"), (2, "succeeded")])
        counts = result.counts
        assert counts.get("succeeded", 0) >= 2, (
            f"the re-run or the trailing job never completed: {counts}"
        )
        # Exactly-once: attempt 2 ran exactly one body.
        n2 = await sys_ledger.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects '
            "WHERE job_id = $1 AND attempt = 2 AND kind = 'done'",
            job_id,
        )
        assert n2 == 1, f"attempt 2 ran the body {n2} times - the re-run is not exactly-once"
    finally:
        await delete_tagged(sys_ledger, schema, _TAG)


@pytest.mark.load_sensitive
async def test_sigkill_before_the_terminal_write_leaves_effects_durable_and_row_reclaimable(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The uncommitted side of the terminal-write window: the body's
    effects are durable, the terminal write is LOST with the process. The
    row stays running under a live lease (nothing committed), the reclaim
    records attempt 1 as crashed, and the re-run's terminal is the only
    terminal - the lost write must not surface as a dropped job."""
    schema = module_pg_schema.schema_name
    try:
        result = await _run_phase(
            sys_ledger,
            schema,
            pg_dsn,
            sys_client,
            seam="before_terminal",
            actor=kill9_starting,
            payload=Kill9Payload(sleep=0.2),
        )
        row = result.row_after_death
        assert row["status"] == "running", (
            f"the uncommitted side must leave the row running at the kill: {row}"
        )
        assert row["finished_at"] is None, f"a finished_at exists with no terminal write: {row}"
        job_id = result.job_id
        # The body's effects ARE durable (they committed before the kill).
        n = await sys_ledger.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects '
            "WHERE job_id = $1 AND attempt = 1 AND kind = 'done'",
            job_id,
        )
        assert n == 1, f"the body's durable effects did not survive the kill: {n}"
        await _assert_attempts(sys_ledger, schema, job_id, [(1, "crashed"), (2, "succeeded")])
        counts = result.counts
        assert counts.get("succeeded", 0) >= 2, (
            f"the re-run or the trailing job never completed: {counts}"
        )
    finally:
        await delete_tagged(sys_ledger, schema, _TAG)


@pytest.mark.load_sensitive
async def test_sigkill_after_the_terminal_write_commits_never_reruns_the_job(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The committed side of the terminal-write window: the fused terminal
    statement has landed (status + result + attempt row + event) and the
    process dies before anything after it. The row must be terminal AND
    whole at the kill, and the replacement worker must NOT re-run it: no
    crash row, no second attempt, no second effects row (the terminal
    fence under a kill between the ledger write and the next heartbeat)."""
    schema = module_pg_schema.schema_name
    try:
        result = await _run_phase(
            sys_ledger,
            schema,
            pg_dsn,
            sys_client,
            seam="after_terminal",
            actor=kill9_starting,
            payload=Kill9Payload(sleep=0.2),
        )
        row = result.row_after_death
        assert row["status"] == "succeeded", (
            f"the terminal write had committed; the row must be succeeded at the kill: {row}"
        )
        assert row["finished_at"] is not None, f"terminal but not whole at the kill: {row}"
        job_id = result.job_id
        await _assert_attempts(sys_ledger, schema, job_id, [(1, "succeeded")])
        counts = result.counts
        assert counts.get("succeeded", 0) == 2, (
            f"expected exactly the committed job + the trailing job: {counts}"
        )
        n = await sys_ledger.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects WHERE job_id = $1',
            job_id,
        )
        assert n == 2, f"the committed job's body ran again after the kill: {n} effects rows"
    finally:
        await delete_tagged(sys_ledger, schema, _TAG)


@pytest.mark.load_sensitive
async def test_sigkill_while_holding_a_keyed_slot_frees_it_within_the_lease(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The no-orphaned-refund shape: a worker SIGKILLed while holding the
    one keyed-reservation slot of a tenant. The dead holder cannot release,
    so the slot's own lease is the recovery clock: after the lapse, the
    leaked-slot sweep must free it and the blocked second job (same tenant,
    slots=1) must run to success - never wait forever behind a dead holder,
    never run twice against one attempt."""
    schema = module_pg_schema.schema_name
    killer = _spawn_kill_worker(pg_dsn, schema, seam=None, tag="kill9-a")
    replacement: WorkerProc | None = None
    try:
        wait_worker_ready(killer)
        # One slot per tenant. The holder is enqueued FIRST and observed
        # RUNNING (slot held) before the second job exists, so the roles
        # are not a claim race: the blocked job can only be denied behind
        # a live holder (its denial snooze prices the earliest lease's
        # expiry - the crash bound this test kills into).
        holder = await sys_client.enqueue(
            sys_keyed, RollPayload(tenant="kill9-tenant", sleep=20.0), tags=[_TAG]
        )
        deadline = time.monotonic() + _KILL_LANDS_CAP_S
        holder_row: asyncpg.Record | None = None
        while time.monotonic() < deadline:
            holder_row = await _job_row(sys_ledger, schema, holder.job_id)
            if holder_row is not None and holder_row["status"] == "running":
                break
            await asyncio.sleep(0.02)
        assert holder_row is not None and holder_row["status"] == "running", (
            f"the holder never took the slot (status: {holder_row and holder_row['status']})"
        )
        blocked = await sys_client.enqueue(
            sys_keyed, RollPayload(tenant="kill9-tenant", sleep=0.2), tags=[_TAG]
        )
        blocked_row = await _job_row(sys_ledger, schema, blocked.job_id)
        assert blocked_row is not None and blocked_row["status"] not in (
            "succeeded",
            "failed",
            "crashed",
            "cancelled",
        ), f"the blocked job ran despite the held slot - the cap did not hold: {blocked_row}"

        killer.proc.kill()  # Why: the kill IS the experiment.
        await _wait_killed(killer, _KILL_LANDS_CAP_S, phase="mid-slot-hold")

        replacement = _spawn_kill_worker(pg_dsn, schema, seam=None, tag="kill9-b")
        wait_worker_ready(replacement)
        await settle_terminal(conn=sys_ledger, schema=schema, tag=_TAG, cap_secs=_SETTLE_CAP_S)
        violations = await conservation_violations(sys_ledger, schema, _TAG)
        assert not violations, (
            "the invariants do not balance after the slot-hold kill:\n" + "\n".join(violations)
        )
        await assert_effects_balance(sys_ledger, schema, _TAG)
        counts = await _status_counts_across(sys_ledger, schema, _TAG)
        assert counts.get("succeeded", 0) == 2, (
            f"the blocked job never ran after the dead holder's lease lapsed: {counts}"
        )
        # The dead holder's slot did not orphan its lease forever: the
        # blocked job's own execution proves the slot was recovered.
        blocked_after = await _job_row(sys_ledger, schema, blocked.job_id)
        assert blocked_after is not None and blocked_after["attempt"] >= 1, (
            f"the blocked job never claimed after the slot recovered: {blocked_after}"
        )
    finally:
        reap(killer)
        if replacement is not None:
            reap(replacement)
        await delete_tagged(sys_ledger, schema, _TAG)


@pytest.mark.load_sensitive
async def test_sigkill_mid_heartbeat_with_a_fresh_lease_still_reclaims_within_the_window(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """Kill the instant a real heartbeat commits: the lease is at its
    FRESHEST (extended to the full span moments before death), so the
    reclaim inherits the widest visibility window. The pinned property:
    even the freshest possible lease recovers within the derived budget -
    the row leaves running inside lease + sweep + backoff, and the re-run
    completes exactly once."""
    schema = module_pg_schema.schema_name
    try:
        result = await _run_phase(
            sys_ledger,
            schema,
            pg_dsn,
            sys_client,
            seam="after_heartbeat",
            actor=kill9_slow,
            payload=Kill9Payload(sleep=20.0),
        )
        row = result.row_after_death
        assert row["status"] == "running", f"the job was not in flight at the heartbeat kill: {row}"
        assert row["lock_expires_at"] is not None, "no lease at the heartbeat kill"
        # The beat had JUST committed: the lease must be near its full span
        # (within one heartbeat interval of the kill's observation).
        remaining = (row["lock_expires_at"] - result.death_wall).total_seconds()
        assert remaining >= _LEASE_S - _HEARTBEAT_INTERVAL_S - 2.0, (
            f"the lease had {remaining:.2f}s left at the kill - the beat that "
            f"committed did not extend it to the full {_LEASE_S}s span"
        )
        job_id = result.job_id
        await _assert_attempts(sys_ledger, schema, job_id, [(1, "crashed"), (2, "succeeded")])
        counts = result.counts
        assert counts.get("succeeded", 0) >= 2, (
            f"the re-run or the trailing job never completed: {counts}"
        )
    finally:
        await delete_tagged(sys_ledger, schema, _TAG)


@pytest.mark.load_sensitive
async def test_sigkill_mid_body_on_the_last_attempt_terminalises_crashed_not_stuck(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The budget edge: the kill lands mid-body on a job whose retry
    budget the kill SPENDS (max_attempts=1). The reclaim must own the row
    through its crash arm - terminal 'crashed', whole (finished_at and
    the crash attempt row stamped, the worker-crashed error class on the
    row), never re-run (the budget is gone), never left running with a
    lapsed lease (the no-stuck-lease clock still applies to a
    dying-with-no-budget row)."""
    schema = module_pg_schema.schema_name
    try:
        result = await _run_phase(
            sys_ledger,
            schema,
            pg_dsn,
            sys_client,
            seam=None,
            actor=kill9_one_life,
            payload=Kill9Payload(sleep=20.0),
            kill_after_effect=("start", 1),
        )
        row = result.row_after_death
        assert row["status"] == "running", f"the body was not in flight at the kill: {row}"
        job_id = result.job_id
        # The crash arm terminalised the row: the attempt ledger carries
        # the crashed row, and no attempt 2 exists (the budget was spent
        # by the kill).
        await _assert_attempts(sys_ledger, schema, job_id, [(1, "crashed")])
        counts = result.counts
        assert counts.get("crashed", 0) == 1, (
            f"the spent-budget kill did not terminalise as crashed: {counts}"
        )
        assert counts.get("succeeded", 0) >= 1, (
            f"the replacement's trailing job never completed: {counts}"
        )
        # The terminal row is WHOLE, live or archived by the time the
        # settle returned: finished_at and the crash arm's error class.
        live = await _job_row(sys_ledger, schema, job_id)
        archived = await sys_ledger.fetchrow(
            f"SELECT finished_at AS finished_at, error_class::text AS err "
            f'FROM "{schema}".jobs_archive WHERE id = $1',
            job_id,
        )
        if live is not None:
            assert live["finished_at"] is not None, (
                f"a crashed terminal row without finished_at is a half state: {live}"
            )
            err = await sys_ledger.fetchval(
                f'SELECT error_class::text FROM "{schema}".jobs WHERE id = $1', job_id
            )
        else:
            assert archived is not None and archived["finished_at"] is not None, (
                f"the crashed row vanished from both tables: {archived}"
            )
            err = archived["err"]
        assert err, f"the crashed row carries no error_class: {err}"
    finally:
        await delete_tagged(sys_ledger, schema, _TAG)
