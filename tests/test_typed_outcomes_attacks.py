# ruff: noqa: S608, S603  # Why: schema names are fixed test identifiers, every value is $-bound; the subprocess runs THIS repo's own test suite against a scratch copy of THIS repo.
"""ATK-351: attacks on the typed outcome branches, the denial_reason
contract, and the shared fence fragments.

Every assertion is OBSERVED BEHAVIOR on a public surface: the ValueError a
backend boundary raises, the row a caller reads back through
``backend.get`` / the event rows the backend persists, and the return
values the backend methods report. SQL text is read or mutated only to
ARRANGE the attack - the loud-failure design is exactly the behavior "a
mutated arm label cannot flow through the read boundary silently, and a
mutated label never changes what the row became".

Surfaces:

1. Arm enumeration: every outcome_branch label the fused terminal
   statements emit is a member of the closed ``SqlOutcomeBranch`` set, and
   each statement emits exactly its documented subset (mark_retry never
   'snoozed'; mark_interrupted never 'max_attempts_failed'). Observed
   through ``parse_outcome_branch`` - the read boundary every backend row
   passes - accepting the documented set and refusing anything else.

2. Fuzz: an arm label mutated inside a rendered statement must raise
   ValueError at the backend boundary (the disclosed loud-failure design)
   AND leave the row exactly as the honest arm left it - the mutation
   touched only the returned label, so the row is never half-written.

3. denial_reason contract: a live (non-terminal) denial writes NO event
   row at all; a denial whose schedule_to_close lapses dies through the
   terminal deadline arm with the event detail naming the caller's reason;
   a plain snooze's deadline event carries no denial_reason key; an
   illegal outcome or reason raises ValueError on the boundary before any
   write (never half-written).

4. Fence-fragment misfold drill: a fence conjunct misfolded in a scratch
   copy of the tree must be caught by the standing nets (differential
   fencing scenarios / bind-arity / terminal-write pins). A guard that
   cannot fire is a red finding.
"""

import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, get_args
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    EnqueueArgs,
    ErrorInfo,
    JobRow,
    SqlOutcomeBranch,
    parse_outcome_branch,
)
from taskq.backend._sql_templates import render
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.assertions import assert_transition_sequence
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_ARM_RE = re.compile(r"'([a-z_]+)'::text AS outcome_branch")

#: The documented per-statement arm subsets: exactly the outcomes each
#: fused terminal statement can report to its caller.
_DOCUMENTED_ARMS: dict[str, set[str]] = {
    "mark_retry": {"retried", "deadline_failed"},
    "mark_snoozed": {"snoozed", "cancelled", "failed"},
    "mark_retry_after_consume_true": {
        "snoozed",
        "cancelled",
        "max_attempts_failed",
        "deadline_failed",
    },
    "mark_retry_after_consume_false": {"snoozed", "cancelled", "deadline_failed"},
    "mark_interrupted": {"released", "deadline_failed"},
}


def test_atk_arm_enumeration_is_the_closed_set() -> None:
    """Every arm label the fused statements emit parses at the read
    boundary, each statement emits exactly its documented subset, and the
    boundary refuses a mutated label loudly."""
    sql = render("taskq_atk_enum")
    seen: set[str] = set()
    for field, documented in _DOCUMENTED_ARMS.items():
        arms = set(_ARM_RE.findall(getattr(sql, field)))
        assert arms == documented, (
            f"{field} emits {sorted(arms)}, the documented subset is {sorted(documented)}"
        )
        seen |= arms
    # Every emitted label is inside the closed union the read boundary serves.
    assert seen <= set(get_args(SqlOutcomeBranch.__value__))
    for arm in seen:
        parse_outcome_branch(arm)  # must not raise
    # The boundary is loud about anything outside the union.
    with pytest.raises(ValueError, match="unknown outcome_branch"):
        parse_outcome_branch("retired")  # a one-letter drift from 'retried'
    with pytest.raises(ValueError, match="unknown outcome_branch"):
        parse_outcome_branch("Release")  # case drift


# ── PG plumbing (the same duck-typed-deps shape the differential harness uses) ─


class _DepsShim:
    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool


async def _setup_pg(pg_dsn: str, schema: str) -> PostgresBackend:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) '
            "VALUES ('test_actor', 'default') ON CONFLICT (actor) DO NOTHING"
        )
    finally:
        await conn.close()
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema}, validate=False
    )
    return PostgresBackend(
        _DepsShim(settings, pool),  # type: ignore[arg-type]
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=0.5),
        cleanup_grace_period=timedelta(seconds=0.5),
    )


async def _dispatched_job(
    backend: PostgresBackend, schema: str, worker_id: UUID, *, stc_s: float | None = None
) -> tuple[Any, int, int]:
    """One enqueued job claimed by this worker; returns (job_id, attempt, claim_epoch)."""
    async with backend._deps.dispatcher_pool.acquire() as conn:  # type: ignore[union-attr]
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            worker_id,
            "atk-host",
            7,
            ["default"],
        )
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            payload_schema_ver=1,
            priority=0,
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            schedule_to_close=(
                None if stc_s is None else datetime.now(UTC) + timedelta(seconds=stc_s)
            ),
        )
    )
    rows = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert rows and rows[0].id == job_id
    # The claim view the handler must present on every later write: the
    # fence binds BOTH the attempt epoch and the claim epoch (JobRow
    # .claim_epoch, bumped +1 by every dispatch claim), and a caller that
    # cannot present them no-ops by design (the cannot-prove-it doctrine).
    return rows[0].id, rows[0].attempt, rows[0].claim_epoch


def _mutate_label(sql_text: str, honest: str) -> str:
    mutated, n = re.subn(
        rf"'{honest}'::text AS outcome_branch",
        "'zzz_mutated'::text AS outcome_branch",
        sql_text,
    )
    assert n >= 1, f"the honest arm label {honest!r} is not in the rendered statement"
    return mutated


@dataclass(frozen=True)
class _FuzzTarget:
    statement: str  # the SqlTemplates field the method executes
    method: str  # the Backend public method the caller uses
    honest_arm: str  # the arm label the arranged scenario exercises
    kwargs: dict[str, Any]  # the call's keyword arguments (besides job/worker/attempt)


_FUZZ_TARGETS = [
    _FuzzTarget(
        "mark_retry",
        "mark_failed_or_retry",
        "retried",
        kwargs={"retry_delay": timedelta(seconds=10)},
    ),
    _FuzzTarget(
        "mark_snoozed",
        "mark_snoozed",
        "snoozed",
        kwargs={"delay": timedelta(seconds=10)},
    ),
    _FuzzTarget(
        "mark_retry_after_consume_true",
        "mark_retry_after",
        "snoozed",
        kwargs={"delay": timedelta(seconds=10), "consume_budget": True},
    ),
    _FuzzTarget(
        "mark_retry_after_consume_false",
        "mark_retry_after",
        "snoozed",
        kwargs={"delay": timedelta(seconds=10), "consume_budget": False},
    ),
    _FuzzTarget(
        "mark_interrupted",
        "mark_interrupted",
        "released",
        kwargs={"hold": timedelta(seconds=0)},
    ),
]


@pytest.mark.parametrize("target", _FUZZ_TARGETS, ids=lambda t: t.statement)
@pytest.mark.asyncio
async def test_atk_mutated_arm_label_raises_and_never_rewrites_the_row(
    target: _FuzzTarget, pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """Drive the honest arm once (records what the caller hears and what
    the row became), then drive the SAME arm on a fresh job with its
    returned label mutated: the caller hears ValueError - never a silent
    mislabel - and the row reads back EXACTLY the fate the honest run
    recorded. The mutation changed the label, never the row."""
    schema = module_pg_schema.schema_name
    backend = await _setup_pg(pg_dsn, schema)
    worker_id = new_uuid()

    async def arrange() -> tuple[Any, int, int]:
        return await _dispatched_job(backend, schema, worker_id)

    async def write(job_id: Any, attempt: int, claim_epoch: int) -> tuple[Any, JobRow | None]:
        call = getattr(backend, target.method)
        if target.method == "mark_failed_or_retry":
            result = await call(
                job_id,
                worker_id,
                ErrorInfo(error_class="RuntimeError", error_message="boom", error_traceback=None),
                attempt=attempt,
                claim_epoch=claim_epoch,
                **target.kwargs,
            )
        else:
            result = await call(
                job_id,
                worker_id,
                attempt=attempt,
                claim_epoch=claim_epoch,
                **target.kwargs,
            )
        return result, await backend.get(job_id)

    async def drive() -> tuple[Any, JobRow | None, Any]:
        job_id, attempt, claim_epoch = await arrange()
        result, row = await write(job_id, attempt, claim_epoch)
        return result, row, job_id

    _honest_return, honest_row, _honest_job = await drive()
    assert honest_row is not None
    honest_status = honest_row.status

    original = backend._sql
    backend._sql = replace(
        original,
        **{target.statement: _mutate_label(getattr(original, target.statement), target.honest_arm)},
    )
    mutated_job_id, mutated_attempt, _mutated_epoch = await arrange()
    try:
        with pytest.raises(ValueError, match="unknown outcome_branch"):
            await write(mutated_job_id, mutated_attempt, _mutated_epoch)
    finally:
        backend._sql = original
    mutated_job = mutated_job_id

    # The mutated run's own row, read back through the backend's own API.
    mutated_row = await backend.get(mutated_job)
    assert mutated_row is not None
    assert mutated_row.status == honest_status, (
        f"{target.statement}: a mutated returned label changed the row's fate "
        f"({honest_status!r} -> {mutated_row.status!r}): a half-written row"
    )
    # A non-terminal arm never leaves terminal timestamps behind.
    if honest_status in ("scheduled", "pending"):
        assert mutated_row.finished_at is None


# ── denial_reason contract ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_atk_denial_reason_contract(pg_dsn: str, module_pg_schema: ModulePgSchema) -> None:
    """The disclosed discriminator is terminality: a live denial writes no
    event row; the terminal deadline arm's event names the caller's reason
    (both reasons); a plain snooze through the same terminal arm carries no
    denial_reason key."""
    schema = module_pg_schema.schema_name
    backend = await _setup_pg(pg_dsn, schema)
    worker_id = new_uuid()

    # Cell 1: a live (non-terminal) denial - no state_change event at all.
    job_id, attempt, claim_epoch = await _dispatched_job(backend, schema, worker_id, stc_s=60.0)
    verdict = await backend.mark_snoozed(
        job_id,
        worker_id,
        timedelta(seconds=10),
        outcome="rate_limit_denied",
        attempt=attempt,
        claim_epoch=claim_epoch,
        denial_reason="capacity",
    )
    assert verdict == "scheduled"
    events = await backend.get_events(job_id)
    assert not [e for e in events if e.kind == "state_change"], (
        "a live denial wrote a state_change event: the non-terminal deferral "
        "must write no event row"
    )

    # Cell 2: the denial whose schedule_to_close lapses dies through the
    # terminal deadline arm, and the event detail names the reason.
    job_id, attempt, claim_epoch = await _dispatched_job(backend, schema, worker_id, stc_s=5.0)
    verdict = await backend.mark_snoozed(
        job_id,
        worker_id,
        timedelta(seconds=10),
        outcome="rate_limit_denied",
        attempt=attempt,
        claim_epoch=claim_epoch,
        denial_reason="unavailable",
    )
    assert verdict == "failed"
    deadline_event = next(e for e in await backend.get_events(job_id) if e.kind == "state_change")
    assert deadline_event.detail.get("denial_reason") == "unavailable"

    # Cell 2b: the same terminal arm on a saturation denial names 'capacity'.
    job_id, attempt, claim_epoch = await _dispatched_job(backend, schema, worker_id, stc_s=5.0)
    verdict = await backend.mark_snoozed(
        job_id,
        worker_id,
        timedelta(seconds=10),
        outcome="reservation_denied",
        attempt=attempt,
        claim_epoch=claim_epoch,
        denial_reason="capacity",
    )
    assert verdict == "failed"
    deadline_event = next(e for e in await backend.get_events(job_id) if e.kind == "state_change")
    assert deadline_event.detail.get("denial_reason") == "capacity"

    # Cell 3: a PLAIN snooze through the same terminal arm - no
    # denial_reason key, the detail shape is unchanged.
    job_id, attempt, claim_epoch = await _dispatched_job(backend, schema, worker_id, stc_s=5.0)
    verdict = await backend.mark_snoozed(
        job_id,
        worker_id,
        timedelta(seconds=10),
        outcome="snoozed",
        attempt=attempt,
        claim_epoch=claim_epoch,
    )
    assert verdict == "failed"
    deadline_event = next(e for e in await backend.get_events(job_id) if e.kind == "state_change")
    assert "denial_reason" not in deadline_event.detail


@pytest.mark.asyncio
async def test_atk_illegal_outcome_and_reason_raise_before_any_write(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """An outcome or denial_reason outside the closed unions raises
    ValueError on the boundary before any write, and the row reads back
    still-running and untouched (never half-written)."""
    schema = module_pg_schema.schema_name
    backend = await _setup_pg(pg_dsn, schema)
    worker_id = new_uuid()
    job_id, attempt, _claim_epoch = await _dispatched_job(backend, schema, worker_id)

    with pytest.raises(ValueError):
        await backend.mark_snoozed(
            job_id, worker_id, timedelta(seconds=10), outcome="succeeded", attempt=attempt
        )
    with pytest.raises(ValueError):
        await backend.mark_snoozed(
            job_id,
            worker_id,
            timedelta(seconds=10),
            outcome="snoozed",
            attempt=attempt,
            denial_reason="saturation",
        )
    row = await backend.get(job_id)
    assert row is not None and row.status == "running", (
        "a rejected outcome/reason must not have touched the row"
    )


# ── the stale-epoch behavior pin the bound spelling lacked ─────────────────


async def _redispatch_same_worker(
    backend: PostgresBackend, schema: str, job_id: Any, worker_id: UUID, expect_attempt: int
) -> tuple[int, int]:
    """Reclaim the row (its lease expires) and re-claim it on the SAME
    worker; returns the row's new (attempt, claim_epoch) claim view."""
    from taskq.backend._sweeps import sweep_expired_locks

    async with backend._deps.dispatcher_pool.acquire() as conn:  # type: ignore[union-attr]
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = '
            f"statement_timestamp() - interval '1 seconds' WHERE id = $1",
            job_id,
        )
        await sweep_expired_locks(conn, timedelta(0), timedelta(0), schema=schema)
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = '
            f"statement_timestamp() - interval '1 seconds' WHERE id = $1",
            job_id,
        )
    await backend.scheduled_to_pending()
    rows = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert rows and rows[0].id == job_id and rows[0].attempt == expect_attempt
    # The claim epoch advanced: every claim stamps an epoch no earlier
    # claim could ever stamp (01.00.18_02_pre_claim_epoch.sql).
    assert rows[0].claim_epoch > 0
    return rows[0].attempt, rows[0].claim_epoch


@pytest.mark.asyncio
async def test_atk_stale_epoch_write_never_applies_to_the_live_attempt(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """The fence's attempt-epoch conjunct is BEHAVIOR, on both spellings: a
    stale attempt's write presented to a row re-dispatched at a later epoch
    on the SAME worker must no-op - never terminalise, never defer, never
    strand - and the LIVE attempt's own write must still apply (the fence
    refused the epoch, not the write)."""
    schema = module_pg_schema.schema_name
    backend = await _setup_pg(pg_dsn, schema)
    worker_id = new_uuid()

    # ── the bound spelling (mark_succeeded / mark_failed / mark_cancelled) ──
    job_id, attempt, stale_epoch = await _dispatched_job(backend, schema, worker_id)
    assert attempt == 1
    live_attempt, live_epoch = await _redispatch_same_worker(
        backend, schema, job_id, worker_id, expect_attempt=2
    )
    assert live_epoch != stale_epoch, "the re-claim must stamp a fresh claim epoch"

    # The stale attempt-1 handler's writes arrive late, presenting the FULL
    # stale claim view (its own attempt AND its own claim epoch): none may
    # apply.
    assert (
        await backend.mark_succeeded(
            job_id, worker_id, {"stale": True}, attempt=1, claim_epoch=stale_epoch
        )
        is False
    )
    assert (
        await backend.mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=stale_epoch) is False
    )
    with pytest.raises(WorkerOwnershipMismatch):
        await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            ErrorInfo(error_class="RuntimeError", error_message="stale", error_traceback=None),
            timedelta(seconds=10),
            attempt=1,
            claim_epoch=stale_epoch,
        )
    row = await backend.get(job_id)
    assert row is not None and row.status == "running" and row.attempt == 2, (
        f"a stale attempt-1 write moved the live attempt-2 row: "
        f"status={row.status if row else None}"
    )

    # The fence is a CONJUNCTION: a write that gets ANY conjunct wrong must
    # not land. The hybrid view here (the row's CURRENT claim epoch paired
    # with the STALE attempt) is what isolates the attempt-epoch conjunct -
    # the full-stale-view writes above are already refused by the newer
    # claim epoch, so a misfolded attempt conjunct would sail through them.
    assert (
        await backend.mark_succeeded(
            job_id, worker_id, {"hybrid": True}, attempt=1, claim_epoch=live_epoch
        )
        is False
    )
    row = await backend.get(job_id)
    assert row is not None and row.status == "running" and row.attempt == 2

    # The LIVE attempt's own write, presenting ITS OWN claim view, still
    # applies: the fence refused the stale epochs, not the write.
    assert (
        await backend.mark_succeeded(
            job_id, worker_id, {"ok": True}, attempt=live_attempt, claim_epoch=live_epoch
        )
        is True
    )
    row = await backend.get(job_id)
    assert row is not None and row.status == "succeeded"
    # The honest outcome's full event trail: the lock-expiry reclaim hands
    # the row back (running → pending), the re-dispatch writes no event
    # row, and the LIVE write terminalises ONCE (running → succeeded) -
    # the stale writes contributed nothing to the feed.
    assert_transition_sequence(
        await backend.get_events(job_id),
        [
            ("running", "pending"),
            ("running", "succeeded"),
        ],
    )

    # ── the aliased spelling (the multi-arm arbiters) ──
    job_id, attempt, stale_epoch = await _dispatched_job(backend, schema, worker_id)
    live_attempt, live_epoch = await _redispatch_same_worker(
        backend, schema, job_id, worker_id, expect_attempt=2
    )
    verdict = await backend.mark_snoozed(
        job_id, worker_id, timedelta(seconds=30), attempt=1, claim_epoch=stale_epoch
    )
    assert verdict == "noop", f"a stale attempt-1 deferral moved the live row: {verdict!r}"
    # The same hybrid isolation on the aliased spelling: the stale attempt
    # paired with the CURRENT claim epoch must still be refused, by the
    # attempt-epoch conjunct alone.
    verdict = await backend.mark_snoozed(
        job_id, worker_id, timedelta(seconds=30), attempt=1, claim_epoch=live_epoch
    )
    assert verdict == "noop", f"a hybrid stale-attempt deferral moved the live row: {verdict!r}"
    # The fenced-out deferrals took the no-op path: the row's fate is
    # unchanged and the live claim view still applies afterwards.
    row = await backend.get(job_id)
    assert row is not None and row.status == "running" and row.attempt == 2
    assert (
        await backend.mark_succeeded(
            job_id, worker_id, {"ok": True}, attempt=live_attempt, claim_epoch=live_epoch
        )
        is True
    )
    row = await backend.get(job_id)
    assert row is not None and row.status == "succeeded"


# ── fence-fragment misfold drill ───────────────────────────────────────────


_MISFOLDS: dict[str, tuple[str, str]] = {
    # The aliased spelling loses its attempt-epoch conjunct: a stale
    # attempt's write would land where the fence refuses it.
    "aliased_attempt": (
        "AND j.attempt = (SELECT attempt FROM params)",
        "AND j.attempt >= 0",  # a tautology: the conjunct is gone
    ),
    # The bound spelling loses its attempt-epoch conjunct. The pair edits
    # the SOURCE TEXT (the fragment value is spelled across two adjacent
    # string literals in the module), so the old string is the literal
    # substring the module actually contains.
    "bound_attempt": (
        "AND attempt = ${attempt_bind}",
        "",  # the conjunct is gone
    ),
    # The bound spelling's worker conjunct drifts to a bind position no
    # caller populates with the worker id.
    "bound_worker_bind": (
        "locked_by_worker = $2",
        "locked_by_worker = $9",
    ),
    # The SHARP one: an ARITY-PRESERVING semantic misfold on the bound
    # spelling - the epoch conjunct becomes a tautology over every later
    # attempt. Only the stale-epoch behavior pin can catch this.
    "bound_attempt_semantic": (
        "AND attempt = ${attempt_bind}",
        "AND attempt >= ${attempt_bind}",
    ),
}

_NET_TESTS = (
    "tests/test_terminal_sql_bind_arity.py",
    "tests/test_rt_diff_terminal.py",
    "tests/test_postgres_terminal_writes.py",
    "tests/test_typed_outcomes_attacks.py::test_atk_stale_epoch_write_never_applies_to_the_live_attempt",
)


def _run_nets_in_scratch(
    repo: Path, scratch: Path, tests: tuple[str, ...]
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(scratch / "src")}
    return subprocess.run(
        [sys.executable, "-m", "pytest", *tests, "-q", "-p", "no:cacheprovider", "--no-header"],
        cwd=scratch,
        env=env,
        capture_output=True,
        text=True,
        timeout=1200,
    )


def _copy_scratch(repo: Path, tmp_path: Path) -> Path:
    scratch = tmp_path / "scratch"
    shutil.copytree(
        repo,
        scratch,
        ignore=shutil.ignore_patterns(
            ".venv", ".git", "__pycache__", "*.pyc", ".pytest_cache", ".ruff_cache"
        ),
        dirs_exist_ok=True,
    )
    return scratch


@pytest.mark.parametrize("misfold", list(_MISFOLDS))
def test_atk_fence_misfold_is_caught_by_the_standing_nets(misfold: str, tmp_path: Path) -> None:
    """Copy the tree to a scratch dir, misfold ONE fence fragment, run the
    standing nets there. The drill's control (the sibling test) proves the
    same harness runs green unmutated, so any failure below is the misfold
    firing a net. A misfold NO net catches is a red finding."""
    repo = Path(__file__).resolve().parent.parent
    scratch = _copy_scratch(repo, tmp_path)
    old, new = _MISFOLDS[misfold]
    assert old != new, f"misfold {misfold} is a no-op edit"
    fragments = scratch / "src" / "taskq" / "backend" / "_sql_fragments.py"
    text = fragments.read_text()
    assert old in text, f"misfold {misfold}: the honest fragment text is not in the module"
    fragments.write_text(text.replace(old, new))

    proc = _run_nets_in_scratch(repo, scratch, _NET_TESTS)
    assert proc.returncode != 0, (
        f"misfold {misfold!r} sailed through every standing net "
        f"({', '.join(_NET_TESTS)}): a guard that cannot fire\n"
        f"--- tail ---\n{proc.stdout[-2000:]}"
    )


def test_atk_fence_drill_control_unmutated_copy_passes(tmp_path: Path) -> None:
    """The control for the drill: the same scratch-copy harness with NO
    misfold runs the standing nets green, so the failures above are
    attributable to the misfold alone."""
    repo = Path(__file__).resolve().parent.parent
    scratch = _copy_scratch(repo, tmp_path)
    proc = _run_nets_in_scratch(repo, scratch, _NET_TESTS)
    assert proc.returncode == 0, proc.stdout[-2000:]
