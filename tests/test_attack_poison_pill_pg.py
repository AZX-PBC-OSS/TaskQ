"""ATTACK (real PG): the poison pill against the fused terminal statement
and the real reclaim sweep.

The twin tier (tests/test_attack_poison_pill.py) pins the lifecycle on
the in-memory backend. This file re-runs the same poison against real
Postgres, because two things only exist there:

* the terminal write is ONE fused data-modifying-CTE statement (the
  fenced ``jobs`` UPDATE, the ``job_attempts`` INSERT and the
  ``job_events`` INSERT in a single statement), so a connection death
  mid-commit aborts the write WHOLE - no attempt row, no event, the row
  stays ``running``. The split-brain (event lands, row write does not)
  is unreachable through the backend's own writers, and the pin proves
  the exhausted write leaves zero partial residue behind;
* the reclaim is the real ``sweep_expired_locks`` batch statement, the
  one production runs, with its own ``job_attempts`` batched INSERT and
  ``WorkerCrashed`` arm.

The poison rides the backend boundary: a delegating proxy whose
``mark_succeeded``/``mark_failed_or_retry`` raise ``OSError`` (the
connection-dies-mid-commit family, a member of the consumer's
``_TERMINAL_WRITE_INFRA_EXCEPTIONS``) for the one poisoned id, for as
long as the row lives. Everything else delegates untouched.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, _create_worker
from taskq.worker._consumer import consume_one_job

pytestmark = pytest.mark.integration

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only the fixture's own schema identifier; every value is $-bound.

_CANCEL_GRACE = timedelta(0)
_CLEANUP_GRACE = timedelta(0)
_LEASE = timedelta(seconds=30)
_NOW = datetime(2026, 1, 1, tzinfo=UTC)


class _PoisonProxy:
    """Delegate-everything proxy that kills exactly the poisoned rows'
    terminal writes, on every call, with the infra-family error."""

    def __init__(self, inner: PostgresBackend, poisoned: set[JobId]) -> None:
        self._inner = inner
        self._poisoned = poisoned
        self.terminal_write_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def mark_succeeded(self, job_id: JobId, *args: Any, **kwargs: Any) -> bool:
        if job_id in self._poisoned:
            self.terminal_write_calls += 1
            raise OSError("simulated: connection died mid-commit")
        return await self._inner.mark_succeeded(job_id, *args, **kwargs)

    async def mark_failed_or_retry(self, job_id: JobId, *args: Any, **kwargs: Any) -> Any:
        if job_id in self._poisoned:
            self.terminal_write_calls += 1
            raise OSError("simulated: connection died mid-commit")
        return await self._inner.mark_failed_or_retry(job_id, *args, **kwargs)


async def _seed_poison_job(
    app: JobsApp,
    *,
    max_attempts: int,
    actor: str,
) -> tuple[JobId, UUID]:
    """One pending job on a registered actor, plus the worker row the
    attempt ledger's holder resolution probes."""
    deps = app.deps
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{deps.settings.schema_name}".actor_config (actor, queue) '
            "VALUES ($1, 'default') ON CONFLICT (actor) DO NOTHING",
            actor,
        )
        await _create_worker(conn, deps.settings.schema_name, worker_id)
    job_id = new_job_id()
    await app.backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=max_attempts,
            retry_kind="transient",
            # The reclaim re-pend delay rides this curve, floored at the
            # 1 s MIN_DEFERRAL_INTERVAL; the test backdates scheduled_at
            # directly instead of waiting it out.
            retry_base=timedelta(milliseconds=1),
            retry_backoff="fixed",
            retry_jitter=0.0,
            scheduled_at=None,
        )
    )
    return job_id, worker_id


async def _consume_poisoned(
    app: JobsApp,
    poison: _PoisonProxy,
    job_id: JobId,
    worker_id: UUID,
    runs: list[JobId],
) -> str:
    dispatched = await app.backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=10, lock_lease=_LEASE
    )
    assert [row.id for row in dispatched] == [job_id], (
        f"the poisoned row must be the row in hand, dispatched {[str(r.id) for r in dispatched]}"
    )
    job = dispatched[0]

    async def run_actor(job_row: object, _ctx: object) -> object:
        runs.append(job_id)
        return {"ok": True}

    outcome = await consume_one_job(
        poison,  # pyright: ignore[reportArgumentType]  # Why: the proxy delegates the whole protocol; the cast-free boundary is the attack's injection seam.
        job,
        worker_id,
        run_actor=run_actor,  # pyright: ignore[reportArgumentType]  # Why: the stub takes (job_row, ctx) positionally, the established consumer-test shape.
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=FakeClock(
            start=_NOW
        ),  # Why: the autonomous path reads no clock without rate limits; the real row timestamps are PG's.
    )
    return str(outcome)


async def _status(app: JobsApp, job_id: JobId) -> tuple[str, str | None]:
    deps = app.deps
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class FROM "{deps.settings.schema_name}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None
    return str(row["status"]), row["error_class"]


async def _expire_lease(app: JobsApp, job_id: JobId) -> None:
    """The worker died mid-poison: nothing renews the lease. Backdate it
    the way the real-PG harness does (direct SQL, the sibling tests'
    convention)."""
    deps = app.deps
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{deps.settings.schema_name}".jobs '
            "SET lock_expires_at = clock_timestamp() - interval '120 seconds' "
            "WHERE id = $1",
            job_id,
        )


async def _promote_past_repend_wait(app: JobsApp, job_id: JobId) -> None:
    """Clear the re-pend delay the sweep just stamped (production waits
    the row's own backoff curve out; the harness backdates the stamp)."""
    deps = app.deps
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{deps.settings.schema_name}".jobs '
            "SET scheduled_at = clock_timestamp() - interval '5 seconds' "
            "WHERE id = $1",
            job_id,
        )


async def _ledger(app: JobsApp, job_id: JobId) -> list[tuple[int, str, str | None]]:
    deps = app.deps
    async with deps.worker_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT attempt, outcome, error_class FROM "{deps.settings.schema_name}"'
            ".job_attempts WHERE job_id = $1 ORDER BY attempt",
            job_id,
        )
    return [(int(r["attempt"]), str(r["outcome"]), r["error_class"]) for r in rows]


async def test_pg_poison_walks_the_row_to_crashed_through_the_real_sweep(
    clean_jobs_app: JobsApp,
) -> None:
    """The full poison lifecycle on real PG: two poisoned cycles (each
    attempt's fused terminal write dies under the consumer's bounded
    retry), the real reclaim sweep between them, the row terminal at
    ``crashed`` with a whole attempt ledger and zero partial residue."""
    app = clean_jobs_app
    schema = app.deps.settings.schema_name
    actor = f"poison_pg_{new_base62(6)}".lower()
    max_attempts = 2
    job_id, worker_a = await _seed_poison_job(app, max_attempts=max_attempts, actor=actor)
    poison = _PoisonProxy(app.backend, {job_id})
    runs: list[JobId] = []

    # ── cycle 1: the fused success write dies, the row waits running ──
    outcome1 = await _consume_poisoned(app, poison, job_id, worker_a, runs)
    assert outcome1 == "failed", (
        f"the consumer must report the attempt honestly when the terminal write "
        f"dies, got {outcome1!r}"
    )
    assert await _status(app, job_id) == ("running", None), (
        "the exhausted write leaves the row running: no partial residue from the "
        "fused statement (no attempt row, no event, no error stamp), the reclaim "
        "owns it from here"
    )
    assert poison.terminal_write_calls == 4, (
        f"the terminal-write retry ceiling is 4 per attempt, got {poison.terminal_write_calls}"
    )
    assert await _ledger(app, job_id) == [], (
        "the fused statement is atomic: a dead write leaves NO attempt row behind"
    )

    # ── the split-brain forge, before the first sweep: an event row
    # claiming success while the row says running. The sweep must read
    # the ROW.
    async with app.deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, kind, detail) '
            "VALUES ($1, 'state_change', "
            '\'{"from_state": "running", "to_state": "succeeded"}\'::jsonb)',
            job_id,
        )

    # ── the worker is dead: the lease expires, the real sweep reclaims ──
    await _expire_lease(app, job_id)
    async with app.deps.worker_pool.acquire() as conn:
        reclaimed = await PostgresBackend.sweep_expired_locks(
            conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
        )
    assert reclaimed == 1, (
        "the running row with the spent lease must be reclaimed even though an "
        "event row claims it succeeded - the row is the recovery's truth source"
    )
    status_after_reclaim, _ = await _status(app, job_id)
    assert status_after_reclaim == "pending", (
        "with attempt budget left, the sweep hands the row back to the fleet"
    )
    first_ledger = await _ledger(app, job_id)
    assert first_ledger == [(1, "crashed", "WorkerCrashed")], (
        f"the sweep records the burned attempt exactly once as "
        f"crashed/WorkerCrashed, got {first_ledger}"
    )

    # ── cycle 2: a different worker re-claims the re-pended row ──
    await _promote_past_repend_wait(app, job_id)
    worker_b = new_uuid()
    async with app.deps.worker_pool.acquire() as conn:
        await _create_worker(conn, schema, worker_b)
    outcome2 = await _consume_poisoned(app, poison, job_id, worker_b, runs)
    assert outcome2 == "failed"
    await _expire_lease(app, job_id)
    async with app.deps.worker_pool.acquire() as conn:
        reclaimed = await PostgresBackend.sweep_expired_locks(
            conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
        )
    assert reclaimed == 1

    # ── the terminal: crashed, truthful, ledger whole ──
    status, error_class = await _status(app, job_id)
    assert status == "crashed", (
        f"at the attempt ceiling the sweep terminalises crashed, got {status!r}"
    )
    assert error_class == "WorkerCrashed"
    assert len(runs) == 2, (
        f"each attempt executes the body exactly once ({max_attempts} total), got {len(runs)}"
    )
    assert poison.terminal_write_calls == max_attempts * 4, (
        f"the per-attempt retry ceiling held across both cycles "
        f"({max_attempts * 4}), got {poison.terminal_write_calls}"
    )
    ledger = await _ledger(app, job_id)
    assert [(a, o) for a, o, _ in ledger] == [(1, "crashed"), (2, "crashed")], (
        f"the ledger must record every burned attempt exactly once, got {ledger}"
    )
    assert all(ec == "WorkerCrashed" for _, _, ec in ledger)
    async with app.deps.worker_pool.acquire() as conn:
        stored = await conn.fetchval(
            f'SELECT result FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert stored is None, "an unpersisted success must never be claimed: the row carries no result"
