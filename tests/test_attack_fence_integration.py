# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this test's own fixture-validated schema identifier; every value is $-bound.

"""ATTACK tests for the fence doctrine AS ONE SYSTEM on the terminal-write
surface (integration attack, hunt/fence-integration).

Prior hunts fixed the pieces on this surface: the claim-epoch fence on all
of a fused statement's arms, the ``cancel_phase`` fence on the deferral
and failure-retry families, the truthful cancel-origin CASE (phase 2
forced / request-evidence cooperative / neither unrequested), the closed
``outcome_branch`` union with its loud post-terminalisation drift raise,
and the ``denial_reason`` the terminal deadline arm persists. This module
attacks the SEAMS — the same statements carrying every fix at once:

1. full-matrix differential: statement x cancel phase x request evidence x
   deadline state, on PG and on the in-memory twin, per cell asserting the
   arm that fired, the origin reported on all three read surfaces
   (``list_jobs``/``get_attempts``/``get_events``), the epoch/cancel
   columns' end state, and the counters — with twin and PG required to
   agree on every cell AND both required to match the doctrine oracle;
2. the triple race: cancel stamp + epoch bump (re-dispatch) + terminal
   write in flight simultaneously, looped with alternating head starts,
   asserting no phantom cancel survives a fenced write, no stale-epoch
   write terminalises a live row, one cancel event, and an audit trail
   whose attempts history matches the events;
3. the deny-then-terminal path: denial churn that mints no rows, then the
   terminal deadline arm with ``denial_reason`` persisted, exact counters,
   and the typed branch raise AFTER terminalisation when the arm label is
   mutated (the disclosed contract, re-verified post-integration);
4. the misuse battery: stale epoch, armed cancel, retry after cancel,
   cancel after terminal — each through public surfaces, each ending in
   exactly one legal state with a truthful origin.

A failure anywhere below is a RED.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace as dc_replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest

from taskq.backend._protocol import ErrorInfo, JobFilter, JobId
from taskq.constants import (
    CANCEL_ORIGIN_COOPERATIVE,
    CANCEL_ORIGIN_FORCED,
    CANCEL_ORIGIN_PENDING,
    CANCEL_ORIGIN_UNREQUESTED,
)
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.pg import setup_running_job

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp
    from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_BOOM = ErrorInfo(error_class="BoomError", error_message="m", error_traceback=None)
_DE = "DeadlineExceeded"
_FUTURE = timedelta(seconds=60)
_DELAY = timedelta(seconds=30)
_RETRY_DELAY = timedelta(seconds=1)

# The matrix axes.
PHASES = (0, 1, 2)
EVIDENCE = ("with_request", "no_request")
DEADLINES = ("none", "future", "past")
STATEMENTS = (
    "mark_retry",
    "mark_failed",
    "mark_snoozed",
    "mark_denied",
    "mark_retry_after_true",
    "mark_retry_after_false",
    "mark_interrupted",
    "mark_cancelled",
)


# ── Doctrine oracle ─────────────────────────────────────────────────────


def _origin_for_deferral_deadline_cancel(phase: int) -> str:
    """The deferral deadline_cancelled arms' CASE: phase 2 forced, else
    cooperative (a phase-carrying row is operator-driven by construction)."""
    return CANCEL_ORIGIN_FORCED if phase == 2 else CANCEL_ORIGIN_COOPERATIVE


def oracle(stmt: str, phase: int, evidence: str, deadline: str) -> dict[str, Any]:
    """The doctrine's expected observable cell.

    Keys: ``ret`` (projected return value, or the string ``"mismatch"`` for
    the WorkerOwnershipMismatch raise), ``status``, ``origin``,
    ``phase_kept`` (the cancel columns survive as the audit trail),
    ``attempt`` (the row's end attempt), ``attempts``/``events`` (the NEW
    rows the write minted, projected), and the three counter deltas.
    """
    forced = CANCEL_ORIGIN_FORCED
    coop = CANCEL_ORIGIN_COOPERATIVE
    unreq = CANCEL_ORIGIN_UNREQUESTED
    request = evidence == "with_request"
    running: dict[str, Any] = {
        "status": "running",
        "origin": None,
        "phase_kept": True,
        "attempt": 1,
        "attempts": [],
        "events": [],
        "snooze": 0,
        "denial": 0,
        "interrupt": 0,
    }

    if stmt == "mark_cancelled":
        origin = forced if phase == 2 else (coop if request else unreq)
        return {
            "ret": True,
            "status": "cancelled",
            "origin": origin,
            "phase_kept": True,
            "attempt": 1,
            "attempts": [("cancelled", origin)],
            "events": [("state_change", "cancelled", origin, None)],
            "snooze": 0,
            "denial": 0,
            "interrupt": 0,
        }

    if stmt == "mark_failed":
        # The actor's own failure is an execution outcome: no cancel fence,
        # the terminal row keeps the cancel columns as its audit trail.
        return {
            "ret": ("failed", "BoomError"),
            "status": "failed",
            "origin": "BoomError",
            "phase_kept": True,
            "attempt": 1,
            "attempts": [("failed", "BoomError")],
            "events": [("state_change", "failed", "BoomError", None)],
            "snooze": 0,
            "denial": 0,
            "interrupt": 0,
        }

    if stmt == "mark_retry":
        if phase != 0:
            # The cancel fence: a phase-carrying row matches NO arm, the
            # write no-ops through WorkerOwnershipMismatch, the row stays
            # running carrying its phase.
            return {"ret": "mismatch", **running}
        if deadline == "past":
            return {
                "ret": ("failed", _DE),
                "status": "failed",
                "origin": _DE,
                # A TERMINAL arm keeps the cancel columns: they are the
                # audit trail of why the row ended.
                "phase_kept": True,
                "attempt": 1,
                "attempts": [("failed", _DE)],
                "events": [("state_change", "failed", _DE, None)],
                "snooze": 0,
                "denial": 0,
                "interrupt": 0,
            }
        return {
            "ret": ("scheduled", "BoomError"),
            "status": "scheduled",
            "origin": "BoomError",
            "phase_kept": False,
            "attempt": 1,
            "attempts": [("failed", "BoomError")],
            "events": [("state_change", "scheduled", "BoomError", None)],
            "snooze": 0,
            "denial": 0,
            "interrupt": 0,
        }

    if stmt in ("mark_snoozed", "mark_denied"):
        denial_outcome = stmt == "mark_denied"
        if phase != 0 and deadline == "past":
            # Cancel-first arbitration in the deadline arm: operator intent
            # outranks the lapsed deadline, the row terminalises cancelled
            # and the caller reads back noop.
            origin = _origin_for_deferral_deadline_cancel(phase)
            return {
                "ret": "noop",
                "status": "cancelled",
                "origin": origin,
                "phase_kept": True,
                "attempt": 1,
                "attempts": [("cancelled", origin)],
                "events": [("state_change", "cancelled", origin, None)],
                "snooze": 0,
                "denial": 0,
                "interrupt": 0,
            }
        if phase != 0:
            # The cancel fence: the deferral never lands.
            return {"ret": "noop", **running}
        if deadline == "past":
            return {
                "ret": "failed",
                "status": "failed",
                "origin": _DE,
                # Terminal arm: the cancel columns survive.
                "phase_kept": True,
                "attempt": 1,
                "attempts": [("failed", _DE)],
                "events": [("state_change", "failed", _DE, "capacity" if denial_outcome else None)],
                "snooze": 0,
                "denial": 1 if denial_outcome else 0,
                "interrupt": 0,
            }
        # A landing deferral refunds the claim's attempt increment, mints
        # no rows, and bumps only its own outcome-keyed counter.
        return {
            "ret": "scheduled",
            "status": "scheduled",
            "origin": None,
            "phase_kept": False,
            "attempt": 0,
            "attempts": [],
            "events": [],
            "snooze": 0 if denial_outcome else 1,
            "denial": 1 if denial_outcome else 0,
            "interrupt": 0,
        }

    if stmt in ("mark_retry_after_true", "mark_retry_after_false"):
        consume = stmt.endswith("_true")
        if phase != 0 and deadline == "past":
            origin = _origin_for_deferral_deadline_cancel(phase)
            return {
                "ret": "noop",
                "status": "cancelled",
                "origin": origin,
                "phase_kept": True,
                "attempt": 1,
                "attempts": [("cancelled", origin)],
                "events": [("state_change", "cancelled", origin, None)],
                "snooze": 0,
                "denial": 0,
                "interrupt": 0,
            }
        if phase != 0:
            return {"ret": "noop", **running}
        if deadline == "past":
            return {
                "ret": "failed:DeadlineExceeded",
                "status": "failed",
                "origin": _DE,
                # Terminal arm: the cancel columns survive.
                "phase_kept": True,
                "attempt": 1,
                "attempts": [("failed", _DE)],
                "events": [("state_change", "failed", _DE, None)],
                "snooze": 0,
                "denial": 0,
                "interrupt": 0,
            }
        if consume:
            # A consuming RetryAfter IS an execution: the attempt stands
            # and writes its snoozed rows.
            return {
                "ret": "scheduled",
                "status": "scheduled",
                "origin": None,
                "phase_kept": False,
                "attempt": 1,
                "attempts": [("snoozed", "RetryAfter")],
                "events": [("state_change", "scheduled", None, None)],
                "snooze": 0,
                "denial": 0,
                "interrupt": 0,
            }
        return {
            "ret": "scheduled",
            "status": "scheduled",
            "origin": None,
            "phase_kept": False,
            "attempt": 0,
            "attempts": [],
            "events": [],
            "snooze": 1,
            "denial": 0,
            "interrupt": 0,
        }

    if stmt == "mark_interrupted":
        if phase != 0:
            # The fence BOTH arms carry: an operator cancel in flight wins,
            # even past a lapsed deadline (mark_retry/mark_snoozed route
            # the past-deadline shape to a cancelled arm; the interrupt
            # has no cancelled arm of its own, the cancel ladder owns it).
            return {"ret": "noop", **running}
        if deadline == "past":
            return {
                "ret": "failed:DeadlineExceeded",
                "status": "failed",
                "origin": _DE,
                # Terminal arm: the cancel columns survive.
                "phase_kept": True,
                "attempt": 1,
                "attempts": [("failed", _DE)],
                "events": [("state_change", "failed", _DE, None)],
                "snooze": 0,
                "denial": 0,
                "interrupt": 0,
            }
        # hold=0 releases the row PENDING immediately; the attempt did
        # start, so no refund and no attempt row — the interrupt counter
        # and one reason='interrupted' event carry it.
        return {
            "ret": "pending",
            "status": "pending",
            "origin": None,
            "phase_kept": False,
            "attempt": 1,
            "attempts": [],
            "events": [("state_change", "pending", None, "interrupted")],
            "snooze": 0,
            "denial": 0,
            "interrupt": 1,
        }

    raise AssertionError(f"unknown statement {stmt!r}")


# ── Observation through the public read surfaces ────────────────────────


async def _list_row(backend: Any, job_id: JobId) -> Any:
    rows = await backend.list_jobs(JobFilter())
    found = [r for r in rows if r.id == job_id]
    assert len(found) == 1, f"list_jobs lost job {job_id}"
    return found[0]


def _project_row(row: Any) -> tuple[Any, ...]:
    return (
        row.status,
        row.error_class,
        row.attempt,
        row.claim_epoch,
        row.cancel_phase,
        row.cancel_requested_at is not None,
        row.locked_by_worker is None,
        row.snooze_count,
        row.rate_limit_blocked_count,
        row.interrupt_count,
    )


def _project_attempts(rows: list[Any]) -> list[tuple[str, str | None]]:
    return [(a.outcome, a.error_class) for a in rows]


def _project_events(rows: list[Any]) -> list[tuple[str, str | None, str | None, str | None]]:
    return [
        (
            e.kind,
            e.detail.get("to_state"),
            e.detail.get("error_class"),
            e.detail.get("denial_reason") or e.detail.get("reason"),
        )
        for e in rows
    ]


# ── Cell drivers: seed one claimed row per cell, run the write ──────────


def _deadline_arg(deadline: str) -> datetime | None:
    if deadline == "none":
        return None
    if deadline == "future":
        return _START + _FUTURE
    return _START - timedelta(seconds=10)


async def _seed_memory_cell(
    backend: "InMemoryBackend", phase: int, evidence: str, deadline: str
) -> JobId:
    from taskq._ids import new_job_id
    from taskq.backend import EnqueueArgs
    from taskq.backend._protocol import CancelPhase

    if "itl_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]
        backend.register_actor_config(actor="itl_actor")
    # A unique queue per cell: earlier cells' released rows re-queue
    # 'pending' and would otherwise be the claim this cell's dispatch
    # returns. A re-queued row routes by the actor's assignment, never
    # back into this label, so one seed per queue claims exactly its own.
    queue = f"itl-{new_job_id()}"
    args = EnqueueArgs(
        id=new_job_id(),
        actor="itl_actor",
        queue=queue,
        payload={"k": "v"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        schedule_to_close=None,
    )
    await backend.enqueue(args)
    wid = backend._worker_id  # type: ignore[reportPrivateUsage]
    dispatched = await backend.dispatch_batch(wid, [queue], limit=1, lock_lease=_FUTURE)
    assert len(dispatched) == 1 and dispatched[0].id == args.id
    row = backend._jobs[args.id]  # type: ignore[reportPrivateUsage]
    backend._jobs[args.id] = dc_replace(  # type: ignore[reportPrivateUsage]
        row,
        cancel_phase=CancelPhase(phase),
        cancel_requested_at=_START if evidence == "with_request" else None,
        schedule_to_close=_deadline_arg(deadline),
    )
    return args.id


async def _seed_pg_cell(
    app: "JobsApp", phase: int, evidence: str, deadline: str
) -> tuple[UUID, UUID]:
    deps = app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        return await setup_running_job(
            conn,
            schema,
            attempt=1,
            cancel_phase=phase,
            cancel_requested_at=datetime.now(UTC) if evidence == "with_request" else None,
            schedule_to_close=(
                datetime.now(UTC) + timedelta(seconds=90)
                if deadline == "future"
                else (datetime.now(UTC) - timedelta(seconds=10) if deadline == "past" else None)
            ),
        )


async def _run_statement(backend: Any, stmt: str, job_id: JobId, wid: Any) -> Any:
    """Run one matrix statement through the public backend surface and
    return the projected result (or ``"mismatch"`` for the typed raise)."""
    kws: dict[str, Any] = {"attempt": 1, "claim_epoch": 1}
    if stmt == "mark_retry":
        try:
            row = await backend.mark_failed_or_retry(job_id, wid, _BOOM, _RETRY_DELAY, **kws)
        except WorkerOwnershipMismatch:
            return "mismatch"
        return (row.status, row.error_class)
    if stmt == "mark_failed":
        try:
            row = await backend.mark_failed_or_retry(job_id, wid, _BOOM, None, **kws)
        except WorkerOwnershipMismatch:
            return "mismatch"
        return (row.status, row.error_class)
    if stmt == "mark_snoozed":
        return await backend.mark_snoozed(
            job_id, wid, _DELAY, outcome="snoozed", denial_reason="capacity", **kws
        )
    if stmt == "mark_denied":
        return await backend.mark_snoozed(
            job_id,
            wid,
            _DELAY,
            outcome="reservation_denied",
            denial_reason="capacity",
            **kws,
        )
    if stmt == "mark_retry_after_true":
        return await backend.mark_retry_after(job_id, wid, _RETRY_DELAY, consume_budget=True, **kws)
    if stmt == "mark_retry_after_false":
        return await backend.mark_retry_after(job_id, wid, _DELAY, consume_budget=False, **kws)
    if stmt == "mark_interrupted":
        return await backend.mark_interrupted(job_id, wid, hold=timedelta(0), **kws)
    if stmt == "mark_cancelled":
        return await backend.mark_cancelled(job_id, wid, **kws)
    raise AssertionError(f"unknown statement {stmt!r}")


async def _observe_cell(backend: Any, job_id: JobId) -> dict[str, Any]:
    row = await _list_row(backend, job_id)
    attempts = _project_attempts(await backend.get_attempts(job_id))
    events = _project_events(await backend.get_events(job_id))
    return {
        "row": _project_row(row),
        "attempts": attempts,
        "events": events,
    }


# ── Attack 1: the full-matrix differential ──────────────────────────────


def _expected_cell(
    stmt: str, phase: int, evidence: str, deadline: str, *, seed_events: int, seed_attempts: int
) -> dict[str, Any]:
    exp = oracle(stmt, phase, evidence, deadline)
    # The event projection is ABSOLUTE (every event of the job): the seed
    # event(s) first (PG's seeded running row carries one pending→running
    # state_change; the twin's dispatched row carries none), then the
    # deltas this cell's write minted.
    events: list[tuple[str, str | None, str | None, str | None]] = []
    for _ in range(seed_events):
        events.append(("state_change", "running", None, None))
    events.extend(exp["events"])
    attempts: list[tuple[str, str | None]] = list(exp["attempts"])
    del seed_attempts  # neither seeding helper mints attempt rows
    return {
        "ret": exp["ret"],
        "row": (
            exp["status"],
            exp["origin"],
            exp["attempt"],
            1,  # claim_epoch: terminal/deferral writes never bump it
            phase,
            evidence == "with_request" if exp["phase_kept"] else False,
            exp["status"] != "running",
            exp["snooze"],
            exp["denial"],
            exp["interrupt"],
        ),
        "attempts": attempts,
        "events": events,
    }


class TestFullMatrixDifferential:
    """Every (statement x phase x evidence x deadline) cell: the arm that
    fired, the origin on all three read surfaces, the epoch/cancel end
    state, the counters — and twin == PG == doctrine on every cell."""

    @pytest.mark.parametrize("stmt", STATEMENTS)
    async def test_pg_matches_the_doctrine(self, clean_jobs_app: "JobsApp", stmt: str) -> None:
        backend = clean_jobs_app.backend
        checked = 0
        for phase in PHASES:
            for evidence in EVIDENCE:
                for deadline in DEADLINES:
                    worker_id, job_id = await _seed_pg_cell(
                        app=clean_jobs_app, phase=phase, evidence=evidence, deadline=deadline
                    )
                    before = await _observe_cell(backend, job_id)
                    ret = await _run_statement(backend, stmt, job_id, worker_id)
                    after = await _observe_cell(backend, job_id)
                    exp = _expected_cell(
                        stmt,
                        phase,
                        evidence,
                        deadline,
                        seed_events=len(before["events"]),
                        seed_attempts=len(before["attempts"]),
                    )
                    assert ret == exp["ret"], (
                        f"RED [{stmt} phase={phase} {evidence} deadline={deadline}]: "
                        f"return {ret!r} != {exp['ret']!r}"
                    )
                    assert after["row"] == exp["row"], (
                        f"RED [{stmt} phase={phase} {evidence} deadline={deadline}]: row "
                        f"{after['row']} != {exp['row']}"
                    )
                    assert after["attempts"] == exp["attempts"], (
                        f"RED [{stmt} phase={phase} {evidence} deadline={deadline}]: attempts "
                        f"{after['attempts']} != {exp['attempts']}"
                    )
                    assert after["events"] == exp["events"], (
                        f"RED [{stmt} phase={phase} {evidence} deadline={deadline}]: events "
                        f"{after['events']} != {exp['events']}"
                    )
                    checked += 1
        assert checked == len(PHASES) * len(EVIDENCE) * len(DEADLINES)

    @pytest.mark.parametrize("stmt", STATEMENTS)
    async def test_twin_matches_the_doctrine(self, stmt: str) -> None:
        from taskq.testing.clock import FakeClock
        from taskq.testing.in_memory import InMemoryBackend

        backend = InMemoryBackend(clock=FakeClock(_START))
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]
        checked = 0
        for phase in PHASES:
            for evidence in EVIDENCE:
                for deadline in DEADLINES:
                    job_id = await _seed_memory_cell(
                        backend, phase=phase, evidence=evidence, deadline=deadline
                    )
                    before = await _observe_cell(backend, job_id)
                    ret = await _run_statement(backend, stmt, job_id, wid)
                    after = await _observe_cell(backend, job_id)
                    exp = _expected_cell(
                        stmt,
                        phase,
                        evidence,
                        deadline,
                        seed_events=len(before["events"]),
                        seed_attempts=len(before["attempts"]),
                    )
                    assert ret == exp["ret"], (
                        f"RED[twin] [{stmt} phase={phase} {evidence} deadline={deadline}]: "
                        f"return {ret!r} != {exp['ret']!r}"
                    )
                    assert after["row"] == exp["row"], (
                        f"RED[twin] [{stmt} phase={phase} {evidence} deadline={deadline}]: row "
                        f"{after['row']} != {exp['row']}"
                    )
                    assert after["attempts"] == exp["attempts"], (
                        f"RED[twin] [{stmt} phase={phase} {evidence} deadline={deadline}]: "
                        f"attempts {after['attempts']} != {exp['attempts']}"
                    )
                    assert after["events"] == exp["events"], (
                        f"RED[twin] [{stmt} phase={phase} {evidence} deadline={deadline}]: "
                        f"events {after['events']} != {exp['events']}"
                    )
                    checked += 1
        assert checked == len(PHASES) * len(EVIDENCE) * len(DEADLINES)


# ── Attack 2: the triple race ───────────────────────────────────────────


class TestTripleRace:
    """cancel stamp + epoch bump (re-dispatch) + terminal write, in flight
    simultaneously, looped with alternating head starts."""

    async def test_triple_race_never_launder_or_stale_terminalise(
        self, clean_jobs_app: "JobsApp"
    ) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        # The three writer orders rotate so each writer really gets the
        # head start across the loop.
        orders = (
            ("stamp", "bump", "retry"),
            ("retry", "stamp", "bump"),
            ("bump", "retry", "stamp"),
        )

        for i in range(60):
            async with deps.worker_pool.acquire() as conn:
                worker_id, job_id = await setup_running_job(conn, schema, attempt=1)

            async def _bump(job_id: JobId = job_id) -> None:
                async with deps.worker_pool.acquire() as c:
                    await c.execute(
                        f'UPDATE "{schema}".jobs SET attempt = 2, claim_epoch = 2 WHERE id = $1',
                        job_id,
                    )

            async def _stamp(job_id: JobId = job_id, i: int = i) -> None:
                stamp_ok = await backend.write_cancel_request(job_id, "triple-race")
                if not stamp_ok:
                    # A False is legal ONLY when the row is already terminal
                    # (the cancel had nothing left to do). A non-terminal row
                    # that refused the stamp ATE the operator's request: no
                    # phase on the row, no event, no later ladder pass will
                    # ever see it — the cancel vanished.
                    diag = await _list_row(backend, job_id)
                    if diag.status in ("pending", "scheduled", "running"):
                        raise AssertionError(
                            f"RED iteration {i}: write_cancel_request returned False on a "
                            f"non-terminal row (status={diag.status} phase={diag.cancel_phase} "
                            f"attempt={diag.attempt} epoch={diag.claim_epoch}) — the "
                            "operator's cancel request vanished: no stamp, no event, and "
                            "the cancel ladder never sees this job"
                        )

            async def _retry(job_id: JobId = job_id, worker_id: UUID = worker_id) -> Any:
                try:
                    return await backend.mark_failed_or_retry(
                        job_id,
                        worker_id,
                        _BOOM,
                        _RETRY_DELAY,
                        attempt=1,
                        claim_epoch=1,
                    )
                except WorkerOwnershipMismatch:
                    return "mismatch"

            fns: dict[str, Callable[[], Awaitable[Any]]] = {
                "bump": _bump,  # type: ignore[dict-item]  # Why: the default-arg bindings only widen the parameter space, the no-arg call is the contract.
                "stamp": _stamp,
                "retry": _retry,
            }
            first, second, third = orders[i % 3]
            # The first writer takes the head start; the other two race.
            await fns[first]()
            await asyncio.gather(fns[second](), fns[third]())

            row = await _list_row(backend, job_id)
            attempts = await backend.get_attempts(job_id)
            events = await backend.get_events(job_id)
            terminal_events = [
                e
                for e in events
                if e.detail.get("to_state") in ("failed", "cancelled", "succeeded", "abandoned")
            ]
            requeue_events = [e for e in events if e.detail.get("to_state") == "scheduled"]
            cancel_events = [e for e in events if e.kind == "cancel_request"]

            # Exactly one cancel event: the stamp wrote it once, no arm
            # minted a second one.
            assert len(cancel_events) == 1, (
                f"RED iteration {i}: {len(cancel_events)} cancel_request events"
            )
            # The audit trail is coherent: an attempt row exists iff the
            # mark_retry arm actually fired.
            if requeue_events:
                assert len(attempts) == 1, (
                    f"RED iteration {i}: re-queued row with {len(attempts)} attempt rows"
                )
            else:
                assert not attempts, (
                    f"RED iteration {i}: attempt row {attempts} with no re-queue event"
                )
            assert len(terminal_events) <= 1, (
                f"RED iteration {i}: {len(terminal_events)} terminal events"
            )
            # No phantom cancel, no laundering: a row that left running
            # behind the retry must NOT carry the operator's cancel
            # columns, and a phase-carrying row must still be running.
            if row.status in ("scheduled", "pending"):
                assert row.cancel_phase == 0 and row.cancel_requested_at is None, (
                    f"RED iteration {i}: the retry laundered an in-flight cancel "
                    f"(status={row.status}, phase={row.cancel_phase})"
                )
                assert not terminal_events
            elif row.status == "running":
                # The fence held (or the stamp lost): the request survives.
                assert row.cancel_phase in (1, 2), (
                    f"RED iteration {i}: running row lost the cancel stamp"
                )
                assert not requeue_events and not attempts, (
                    f"RED iteration {i}: fenced-out retry wrote rows"
                )
            if row.status == "failed":
                # mark_retry's deadline arm is this race's only 'failed'
                # writer, and it runs at the row's own claimed epoch: a
                # 'failed' at the bumped epoch is the stale write landing
                # on a live re-dispatch.
                assert row.attempt == 1, (
                    f"RED iteration {i}: failed row at attempt {row.attempt} — a "
                    "stale-epoch write terminalised a live row"
                )
            if row.status == "cancelled" and row.attempt == 2:
                # Pre-ladder, the ONLY writer that can cancel is the fused
                # pending/scheduled arm (the re-queued row's direct cancel,
                # at the epoch the row really carries); its origin is
                # never-ran. Anything else at the bumped epoch is a stale
                # write's product.
                assert row.error_class == CANCEL_ORIGIN_PENDING, (
                    f"RED iteration {i}: cancelled row at attempt 2 with origin "
                    f"{row.error_class!r} — a stale write terminalised a live row"
                )
            # Close the ladder: a surviving phase-carrying row is
            # terminalised by the cancel write at the CURRENT epoch.
            if row.status == "running":
                assert (
                    await backend.mark_cancelled(job_id, worker_id, attempt=2, claim_epoch=2)
                    is True
                )
                row = await _list_row(backend, job_id)
                assert row.status == "cancelled"
                assert row.error_class == CANCEL_ORIGIN_COOPERATIVE

    async def test_triple_race_all_serial_orders_end_legal(self) -> None:
        """The twin cannot interleave inside a method, so run all six
        serial orders of the three writers — each must end in exactly one
        legal state with a truthful origin."""
        import itertools

        from taskq.backend._protocol import CancelPhase
        from taskq.testing.clock import FakeClock
        from taskq.testing.in_memory import InMemoryBackend

        for order in itertools.permutations(("stamp", "bump", "retry"), 3):
            backend = InMemoryBackend(clock=FakeClock(_START))
            if "itl_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]
                backend.register_actor_config(actor="itl_actor")
            from taskq._ids import new_job_id
            from taskq.backend import EnqueueArgs

            args = EnqueueArgs(
                id=new_job_id(),
                actor="itl_actor",
                queue="default",
                payload={"k": "v"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
                schedule_to_close=None,
            )
            await backend.enqueue(args)
            wid = backend._worker_id  # type: ignore[reportPrivateUsage]
            await backend.dispatch_batch(wid, ["default"], limit=1, lock_lease=_FUTURE)

            async def _stamp(args_id: JobId = args.id, backend: Any = backend) -> None:
                assert await backend.write_cancel_request(args_id, "serial") is True

            async def _bump(args_id: JobId = args.id, backend: Any = backend) -> None:
                row = backend._jobs[args_id]  # type: ignore[reportPrivateUsage]
                backend._jobs[args_id] = dc_replace(  # type: ignore[reportPrivateUsage]
                    row, attempt=2, claim_epoch=2
                )

            async def _retry(
                args_id: JobId = args.id, backend: Any = backend, wid: Any = wid
            ) -> Any:
                try:
                    return await backend.mark_failed_or_retry(
                        args_id, wid, _BOOM, _RETRY_DELAY, attempt=1, claim_epoch=1
                    )
                except WorkerOwnershipMismatch:
                    return "mismatch"

            fns = {"stamp": _stamp, "bump": _bump, "retry": _retry}
            for step in order:
                await fns[step]()

            row = await backend.get(args.id)
            assert row is not None
            if row.status in ("scheduled", "pending"):
                assert row.cancel_phase == CancelPhase.NONE and row.cancel_requested_at is None, (
                    f"RED order {order}: re-queued row carries a laundered cancel"
                )
            elif row.status == "running":
                assert row.cancel_phase == CancelPhase.COOPERATIVE, (
                    f"RED order {order}: running row lost the stamp"
                )
            elif row.status == "cancelled":
                assert row.error_class in (CANCEL_ORIGIN_COOPERATIVE, CANCEL_ORIGIN_PENDING)
            else:
                pytest.fail(f"RED order {order}: illegal end status {row.status}")


# ── Attack 3: deny-then-terminal ────────────────────────────────────────


class TestDenyThenTerminal:
    """Denial churn mints no rows; the terminal deadline arm carries the
    denial_reason and exact counters; a mutated arm label raises AFTER
    terminalisation with the row coherent."""

    async def test_denial_churn_then_deadline_terminal(self, clean_jobs_app: "JobsApp") -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            # setup_running_job inserts the worker row itself (its holder
            # id), so no separate create_worker here.
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                attempt=1,
                schedule_to_close=datetime.now(UTC) + timedelta(seconds=120),
            )

        epoch = 1
        for round_no in range(1, 6):
            ret = await backend.mark_snoozed(
                job_id,
                worker_id,
                _DELAY,
                outcome="reservation_denied",
                denial_reason="capacity",
                attempt=1,
                claim_epoch=epoch,
            )
            assert ret == "scheduled", f"RED round {round_no}: churn denial refused: {ret}"
            row = await _list_row(backend, job_id)
            # No rows: a denial is admission control, not an execution.
            assert await backend.get_attempts(job_id) == [], f"RED round {round_no}"
            assert (
                len([e for e in await backend.get_events(job_id) if e.kind == "state_change"]) == 1
            ), f"RED round {round_no}: the churn minted events"
            assert row.rate_limit_blocked_count == round_no, (
                f"RED round {round_no}: counter {row.rate_limit_blocked_count}"
            )
            assert row.snooze_count == 0, f"RED round {round_no}: the denial spent a snooze"
            assert row.attempt == 0, f"RED round {round_no}: the refund did not land"
            # Re-claim for the next round: the refund made the row due at
            # attempt 0; the next claim bumps attempt and epoch together.
            async with deps.worker_pool.acquire() as c:
                await c.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'pending', scheduled_at = "
                    f"clock_timestamp() - interval '1 second' WHERE id = $1",
                    job_id,
                )
            dispatched = await backend.dispatch_batch(
                worker_id, ["default"], limit=1, lock_lease=_FUTURE
            )
            assert len(dispatched) == 1, f"RED round {round_no}: re-claim failed"
            epoch += 1
            assert dispatched[0].claim_epoch == epoch

        # The terminal deadline arm: one more denial past schedule_to_close.
        async with deps.worker_pool.acquire() as c:
            await c.execute(
                f'UPDATE "{schema}".jobs SET schedule_to_close = '
                f"clock_timestamp() - interval '10 seconds' WHERE id = $1",
                job_id,
            )
        ret = await backend.mark_snoozed(
            job_id,
            worker_id,
            _DELAY,
            outcome="reservation_denied",
            denial_reason="capacity",
            attempt=1,
            claim_epoch=epoch,
        )
        assert ret == "failed", f"RED terminal: {ret}"
        row = await _list_row(backend, job_id)
        assert row.status == "failed"
        assert row.error_class == _DE
        assert row.rate_limit_blocked_count == 6, (
            f"RED terminal: the final denial vanished ({row.rate_limit_blocked_count})"
        )
        assert row.snooze_count == 0
        attempts = await backend.get_attempts(job_id)
        assert [(a.outcome, a.error_class, a.error_message) for a in attempts] == [
            ("failed", _DE, "schedule_to_close reached before next dispatch")
        ], f"RED terminal: attempt history {attempts}"
        events = await backend.get_events(job_id)
        terminal = [
            e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "failed"
        ]
        assert len(terminal) == 1
        assert terminal[0].detail.get("denial_reason") == "capacity", (
            f"RED terminal: denial_reason not persisted on the event: {terminal[0].detail}"
        )

    async def test_mutated_deadline_arm_label_raises_after_terminalisation(
        self, clean_jobs_app: "JobsApp", monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The disclosed drift contract, post-integration: the typed
        branch union is closed, an arm label renamed out of step with its
        reader raises AFTER the statement terminalised the row, and the
        row itself is coherent (the drift is in the report, not the state)."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                schedule_to_close=datetime.now(UTC) - timedelta(seconds=10),
            )

        sql = backend._sql
        drifted = sql.mark_snoozed.replace(
            "'failed'::text AS outcome_branch", "'denial_deadline'::text AS outcome_branch", 1
        )
        assert drifted != sql.mark_snoozed
        monkeypatch.setattr(backend, "_sql", dc_replace(sql, mark_snoozed=drifted))

        with pytest.raises(ValueError, match="denial_deadline"):
            await backend.mark_snoozed(
                job_id,
                worker_id,
                _DELAY,
                outcome="reservation_denied",
                denial_reason="capacity",
                attempt=1,
                claim_epoch=1,
            )
        monkeypatch.undo()

        # The row terminalised BEFORE the raise and is coherent on all
        # three read surfaces.
        row = await _list_row(backend, job_id)
        assert row.status == "failed"
        assert row.error_class == _DE
        assert row.rate_limit_blocked_count == 1 and row.snooze_count == 0
        attempts = await backend.get_attempts(job_id)
        assert [(a.outcome, a.error_class) for a in attempts] == [("failed", _DE)]
        events = await backend.get_events(job_id)
        terminal = [
            e for e in events if e.kind == "state_change" and e.detail.get("to_state") == "failed"
        ]
        assert len(terminal) == 1
        assert terminal[0].detail.get("denial_reason") == "capacity"


# ── Attack 4: the misuse battery ────────────────────────────────────────


class TestMisuseBattery:
    """Stale epoch + armed cancel, retry after cancel, cancel after
    terminal — each through public surfaces, each ending in exactly one
    legal state with a truthful origin. Run on PG and on the twin."""

    async def test_pg_battery(self, clean_jobs_app: "JobsApp") -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        # (a) stale epoch + armed cancel: the re-dispatch bumped the row,
        # the stale handler's write must not land, and the ladder's own
        # cancel at the CURRENT epoch terminalises truthfully.
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn, schema, attempt=1, cancel_phase=1, cancel_requested_at=datetime.now(UTC)
            )
            await conn.execute(
                f'UPDATE "{schema}".jobs SET attempt = 2, claim_epoch = 2 WHERE id = $1',
                job_id,
            )
        assert (
            await backend.mark_succeeded(job_id, worker_id, {"ok": 1}, attempt=1, claim_epoch=1)
            is False
        ), "RED (a): the stale-epoch write terminalised a live row"
        assert await backend.mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=1) is False, (
            "RED (a): the stale-epoch cancel landed"
        )
        assert await backend.mark_cancelled(job_id, worker_id, attempt=2, claim_epoch=2) is True
        row = await _list_row(backend, job_id)
        assert row.status == "cancelled" and row.error_class == CANCEL_ORIGIN_COOPERATIVE
        assert row.cancel_phase == 1 and row.cancel_requested_at is not None, (
            "RED (a): the terminal write did not keep the cancel audit trail"
        )

        # (b) retry after cancel: the fence refuses, the ladder closes.
        async with deps.worker_pool.acquire() as conn:
            worker_b, job_b = await setup_running_job(
                conn, schema, attempt=1, cancel_phase=1, cancel_requested_at=datetime.now(UTC)
            )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(
                job_b, worker_b, _BOOM, _RETRY_DELAY, attempt=1, claim_epoch=1
            )
        row = await _list_row(backend, job_b)
        assert row.status == "running" and row.cancel_phase == 1
        assert await backend.mark_cancelled(job_b, worker_b, attempt=1, claim_epoch=1) is True
        row = await _list_row(backend, job_b)
        assert row.status == "cancelled" and row.error_class == CANCEL_ORIGIN_COOPERATIVE

        # (c) cancel after terminal: a succeeded row is not cancellable,
        # exactly one terminal event survives.
        async with deps.worker_pool.acquire() as conn:
            worker_c, job_c = await setup_running_job(conn, schema)
        assert (
            await backend.mark_succeeded(job_c, worker_c, {"ok": 1}, attempt=1, claim_epoch=1)
            is True
        )
        assert await backend.mark_cancelled(job_c, worker_c, attempt=1, claim_epoch=1) is False, (
            "RED (c): a terminal row was cancelled"
        )
        row = await _list_row(backend, job_c)
        assert row.status == "succeeded"
        events = await backend.get_events(job_c)
        terminal = [
            e
            for e in events
            if e.detail.get("to_state") in ("succeeded", "failed", "cancelled", "abandoned")
        ]
        assert len(terminal) == 1 and terminal[0].detail.get("to_state") == "succeeded"
        attempts = await backend.get_attempts(job_c)
        assert len(attempts) == 1 and attempts[0].outcome == "succeeded"

        # (d) cancel after terminal, deferral shape: the interrupted arm
        # refuses a terminal row the same way.
        async with deps.worker_pool.acquire() as conn:
            worker_d, job_d = await setup_running_job(conn, schema)
        assert (
            await backend.mark_failed_or_retry(
                job_d, worker_d, _BOOM, None, attempt=1, claim_epoch=1
            )
            is not None
        )
        assert (
            await backend.mark_interrupted(
                job_d, worker_d, attempt=1, hold=timedelta(0), claim_epoch=1
            )
            == "noop"
        ), "RED (d): a failed row was released back to the queue"
        row = await _list_row(backend, job_d)
        assert row.status == "failed" and row.error_class == "BoomError"

    async def test_twin_battery(self) -> None:
        from taskq.backend._protocol import CancelPhase
        from taskq.testing.clock import FakeClock
        from taskq.testing.in_memory import InMemoryBackend

        backend = InMemoryBackend(clock=FakeClock(_START))
        if "itl_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]
            backend.register_actor_config(actor="itl_actor")
        from taskq._ids import new_job_id
        from taskq.backend import EnqueueArgs

        async def _claimed_job() -> JobId:
            args = EnqueueArgs(
                id=new_job_id(),
                actor="itl_actor",
                queue="default",
                payload={"k": "v"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
                schedule_to_close=None,
            )
            await backend.enqueue(args)
            await backend.dispatch_batch(
                backend._worker_id,
                ["default"],
                limit=1,
                lock_lease=_FUTURE,  # type: ignore[reportPrivateUsage]
            )
            return args.id

        def _arm(job_id: JobId, phase: CancelPhase) -> None:
            row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]
            backend._jobs[job_id] = dc_replace(  # type: ignore[reportPrivateUsage]
                row, cancel_phase=phase, cancel_requested_at=_START
            )

        def _bump(job_id: JobId) -> None:
            row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]
            backend._jobs[job_id] = dc_replace(row, attempt=2, claim_epoch=2)  # type: ignore[reportPrivateUsage]

        wid = backend._worker_id  # type: ignore[reportPrivateUsage]

        # (a) stale epoch + armed cancel.
        job_a = await _claimed_job()
        _arm(job_a, CancelPhase.COOPERATIVE)
        _bump(job_a)
        assert (
            await backend.mark_succeeded(job_a, wid, {"ok": 1}, attempt=1, claim_epoch=1) is False
        )
        assert await backend.mark_cancelled(job_a, wid, attempt=1, claim_epoch=1) is False
        assert await backend.mark_cancelled(job_a, wid, attempt=2, claim_epoch=2) is True
        row = await backend.get(job_a)
        assert row is not None
        assert row.status == "cancelled" and row.error_class == CANCEL_ORIGIN_COOPERATIVE

        # (b) retry after cancel.
        job_b = await _claimed_job()
        _arm(job_b, CancelPhase.COOPERATIVE)
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(
                job_b, wid, _BOOM, _RETRY_DELAY, attempt=1, claim_epoch=1
            )
        assert await backend.mark_cancelled(job_b, wid, attempt=1, claim_epoch=1) is True
        row = await backend.get(job_b)
        assert row is not None
        assert row.status == "cancelled" and row.error_class == CANCEL_ORIGIN_COOPERATIVE

        # (c) cancel after terminal.
        job_c = await _claimed_job()
        assert await backend.mark_succeeded(job_c, wid, {"ok": 1}, attempt=1, claim_epoch=1) is True
        assert await backend.mark_cancelled(job_c, wid, attempt=1, claim_epoch=1) is False
        row = await backend.get(job_c)
        assert row is not None
        assert row.status == "succeeded"
        events = await backend.get_events(job_c)
        terminal = [
            e
            for e in events
            if e.detail.get("to_state") in ("succeeded", "failed", "cancelled", "abandoned")
        ]
        assert len(terminal) == 1 and terminal[0].detail.get("to_state") == "succeeded"

        # (d) interrupt after terminal.
        job_d = await _claimed_job()
        assert (
            await backend.mark_failed_or_retry(job_d, wid, _BOOM, None, attempt=1, claim_epoch=1)
            is not None
        )
        assert (
            await backend.mark_interrupted(job_d, wid, attempt=1, hold=timedelta(0), claim_epoch=1)
            == "noop"
        )
        row = await backend.get(job_d)
        assert row is not None
        assert row.status == "failed" and row.error_class == "BoomError"
