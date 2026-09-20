"""Claim-epoch fencing: the terminal-write fence survives the attempt ceiling.

The terminal writes are fenced on ``(status='running', locked_by_worker,
attempt)`` and the claim stamps the displayed counter with a saturating
``LEAST(attempt + 1, 32767)``. At the ceiling the value stops advancing, so
a reclaim plus a redispatch to the same worker gives the stale execution and
the live one the SAME fence: the stale ``mark_succeeded`` wins and the live
one reads ``WorkerOwnershipMismatch``, a failed job recorded as succeeded
(reachable with ``retry_kind='indefinite'``, about 9.1 hours of tight
retrying at the 1 s deferral floor).

The fix fences every terminal/ownership write on a SECOND, non-saturating
column, ``claim_epoch``: the dispatch claim bumps it by exactly 1 per claim
(bigint), the reclaim sweeps that clear locks leave it, and every write
that fences on ``attempt`` also fences on ``claim_epoch`` equalling the
epoch of the writer's own claim view. The displayed counter keeps its
saturating, budget-display semantics unchanged.

The pins here are behavior-level and drive the REAL backends:

* the issue's exact reproduction at the ceiling on testcontainers PG, plus
  the attempt-5 control and the strict-increase pin;
* every fenced arm (all nine ``mark_*`` methods, every arm class the
  templates carry) parametrized over ``backend_pair`` - the stale write is
  refused and the live write lands with its own outcome, on PG and on the
  in-memory twin alike.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this test's own fixture-validated schema identifier; every value is $-bound.

import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import ErrorInfo, JobId, JobRow
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.pg import create_worker
from taskq.worker._leader_shared import prune_terminal_jobs

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _PGConn = asyncpg.Connection[asyncpg.Record] | PoolConnectionProxy[asyncpg.Record]
else:
    type _PGConn = object  # pyright: ignore[reportInvalidTypeForm]  # Why: asyncpg classes are not subscriptable at runtime

pytestmark = pytest.mark.integration

#: The domain ceiling of the ``jobs.attempt`` smallint column.
_CEILING = 32767
#: A past timestamp: a row scheduled here is due on any backend clock.
_PAST = datetime(2025, 1, 1, tzinfo=UTC)
#: A lock lease the reclaim sweep can age out in one tick.
_TICK_LEASE = timedelta(microseconds=1)
_ERROR_B = ErrorInfo(error_class="BoomError", error_message="live b", error_traceback=None)
_ERROR_A = ErrorInfo(error_class="StaleA", error_message="stale a", error_traceback=None)


# ── Part A: the issue's reproduction, on real PG ─────────────────────────


async def _park(
    conn: _PGConn,
    schema: str,
    actor: str,
    *,
    attempt: int,
    retry_kind: str,
) -> UUID:
    """Seed one claimable pending job at *attempt*.

    ``retry_kind='indefinite'`` is the production route to the ceiling: its
    budget is the deadline, not ``max_attempts``. ``retry_base_seconds=0``
    keeps the reclaim sweep's reschedule delay at zero so a reclaimed row is
    immediately claimable again.
    """
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, scheduled_at, attempt, "
        " max_attempts, retry_kind, retry_base_seconds, retry_cap_seconds) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 'pending', "
        "        clock_timestamp() - interval '5 minutes', $3, 3, $4, 0, 0)",
        job_id,
        actor,
        attempt,
        retry_kind,
    )
    return job_id


async def _register(conn: _PGConn, schema: str, actor: str, worker_id: UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO NOTHING",
        actor,
        "default",
    )
    await create_worker(conn, schema, worker_id)


async def _expire_lease(conn: _PGConn, schema: str, job_id: JobId) -> None:
    """Age one claimed row's lock past expiry so the reclaim sweep owns it."""
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET lock_expires_at = clock_timestamp() - interval '1 second' "
        "WHERE id = $1",
        job_id,
    )


async def _row(conn: _PGConn, schema: str, job_id: JobId) -> dict[str, object]:
    rec = await conn.fetchrow(
        f'SELECT status, attempt, claim_epoch, error_class, result FROM "{schema}".jobs '
        "WHERE id = $1",
        job_id,
    )
    assert rec is not None
    return dict(rec)


class TestCeilingReproduction:
    """The issue's repro chain, driven through the real claim and sweep SQL."""

    async def test_ceiling_stale_mark_succeeded_is_refused_live_b_lands(
        self, jobs_app: JobsApp
    ) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        actor = f"ceiling_repro_{new_base62(6)}".lower()
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await _register(conn, schema, actor, worker_id)
            job_id = JobId(
                await _park(conn, schema, actor, attempt=_CEILING - 1, retry_kind="indefinite")
            )

            # Claim A: attempt clamps to the ceiling, the epoch advances.
            a = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=["default"],
                limit=10,
                lock_lease=timedelta(seconds=30),
            )
            assert [r.id for r in a] == [job_id]
            assert a[0].attempt == _CEILING
            assert a[0].claim_epoch == 1

            # Reclaim: the lease expires, the sweep hands the row back.
            await _expire_lease(conn, schema, job_id)
            assert (
                await PostgresBackend.sweep_expired_locks(
                    conn, timedelta(0), timedelta(0), schema=schema
                )
                == 1
            )
            # The sweep's re-pend floors its reschedule at
            # MIN_DEFERRAL_INTERVAL; wait it out on the server clock.
            await asyncio.sleep(1.1)

            # Claim B: the SAME attempt number (the clamp), a NEW epoch.
            b = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=["default"],
                limit=10,
                lock_lease=timedelta(seconds=30),
            )
            assert [r.id for r in b] == [job_id]
            assert b[0].attempt == _CEILING, "the clamp must reuse the attempt number"
            assert b[0].claim_epoch == 2, "the epoch must never reuse a claim's identity"

            # Stale A's terminal write carries the ceiling attempt and the
            # stale epoch: refused, exactly like a different worker's late
            # write. (This is the write that USED to win and record the
            # failed job as succeeded.)
            landed = await backend.mark_succeeded(
                job_id, worker_id, {"from": "STALE_A"}, attempt=_CEILING, claim_epoch=1
            )
            assert landed is False, (
                "the stale claim's success write must be fenced out at the "
                "ceiling: attempt alone can no longer distinguish the two "
                "executions, the epoch does"
            )

            # Live B's terminal write lands with B's outcome.
            row = await backend.mark_failed_or_retry(
                job_id, worker_id, _ERROR_B, None, attempt=_CEILING, claim_epoch=2
            )
            assert row.status == "failed"
            assert row.error_class == "BoomError"

            final = await _row(conn, schema, job_id)
        assert final["status"] == "failed"
        assert final["error_class"] == "BoomError"
        assert final["result"] is None, "no stale result may ride the row"
        assert final["claim_epoch"] == 2

    async def test_control_attempt_five_behaviour_is_unchanged(self, jobs_app: JobsApp) -> None:
        """The pre-epoch control from the repro, unchanged by the fix: away
        from the ceiling the attempt number already advanced, so the stale
        write was fenced before and must still be fenced."""
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        actor = f"ceiling_ctl_{new_base62(6)}".lower()
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await _register(conn, schema, actor, worker_id)
            job_id = JobId(await _park(conn, schema, actor, attempt=5, retry_kind="indefinite"))

            a = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=["default"],
                limit=10,
                lock_lease=timedelta(seconds=30),
            )
            assert [r.id for r in a] == [job_id]
            assert a[0].attempt == 6
            assert a[0].claim_epoch == 1

            await _expire_lease(conn, schema, job_id)
            assert (
                await PostgresBackend.sweep_expired_locks(
                    conn, timedelta(0), timedelta(0), schema=schema
                )
                == 1
            )
            # The sweep's re-pend floors its reschedule at
            # MIN_DEFERRAL_INTERVAL; wait it out on the server clock.
            await asyncio.sleep(1.1)

            b = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=["default"],
                limit=10,
                lock_lease=timedelta(seconds=30),
            )
            assert [r.id for r in b] == [job_id]
            assert b[0].attempt == 7, "away from the ceiling the counter advances"
            assert b[0].claim_epoch == 2

            # Stale A: stale attempt AND stale epoch, refused either way.
            assert (
                await backend.mark_succeeded(
                    job_id, worker_id, {"from": "STALE_A"}, attempt=6, claim_epoch=1
                )
                is False
            )
            # The stale attempt number alone is still a fence: a writer
            # presenting a wrong attempt against the live epoch lands
            # nothing.
            assert (
                await backend.mark_succeeded(
                    job_id, worker_id, {"from": "STALE_A"}, attempt=6, claim_epoch=2
                )
                is False
            )
            row = await backend.mark_failed_or_retry(
                job_id, worker_id, _ERROR_B, None, attempt=7, claim_epoch=2
            )
            assert row.status == "failed"
            assert row.error_class == "BoomError"

            final = await _row(conn, schema, job_id)
        assert final["status"] == "failed"
        assert final["error_class"] == "BoomError"

    async def test_epoch_strictly_increases_across_claims_at_the_ceiling(
        self, jobs_app: JobsApp
    ) -> None:
        """N reclaim/redispatch cycles at the ceiling: attempt is clamped
        every time, the epoch never repeats. A monotone epoch is what makes
        every fence comparison unambiguous."""
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        actor = f"ceiling_mono_{new_base62(6)}".lower()
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await _register(conn, schema, actor, worker_id)
            job_id = JobId(
                await _park(conn, schema, actor, attempt=_CEILING - 1, retry_kind="indefinite")
            )

            seen_epochs: list[int] = []
            for _ in range(4):
                claimed = await backend.dispatch_batch(
                    worker_id=worker_id,
                    queues=["default"],
                    limit=10,
                    lock_lease=timedelta(seconds=30),
                )
                assert [r.id for r in claimed] == [job_id]
                assert claimed[0].attempt == _CEILING, "the clamp holds across cycles"
                seen_epochs.append(claimed[0].claim_epoch)

                await _expire_lease(conn, schema, job_id)
                assert (
                    await PostgresBackend.sweep_expired_locks(
                        conn, timedelta(0), timedelta(0), schema=schema
                    )
                    == 1
                )
                # The sweep's re-pend floors its reschedule at
                # MIN_DEFERRAL_INTERVAL; wait it out on the server clock.
                await asyncio.sleep(1.1)

            assert seen_epochs == [1, 2, 3, 4], (
                f"the epoch must strictly increase across claims at the ceiling; saw {seen_epochs}"
            )


# ── Part B: every fenced arm, on both backends ───────────────────────────


async def _expire_and_reclaim(backend: Backend) -> None:
    """Age every lease past expiry on the backend's own clock, then run the
    reclaim sweep itself (never a row mutation), and clear the sweep's
    MIN_DEFERRAL_INTERVAL reschedule floor so the re-pended row is due."""
    if isinstance(backend, InMemoryBackend):
        from taskq.testing._runner import advance_clock_to

        def travel() -> None:
            # Time-travel a day past the current twin time: past the
            # microsecond lease and past the sweep's reschedule floor,
            # the twin's own documented clock surface.
            advance_clock_to(backend, backend._clock.now() + timedelta(days=1))  # pyright: ignore[reportPrivateUsage]  # Why: the twin-tier pin owns the clock handle; the runner's advance_clock_to is the documented surface.

        travel()
        reclaimed = await backend.reclaim_expired_locks(timedelta(0), timedelta(0))
        travel()
    else:
        await asyncio.sleep(0.05)
        reclaimed = await backend.reclaim_expired_locks(timedelta(0), timedelta(0))
        # The sweep's re-pend floors its reschedule at
        # MIN_DEFERRAL_INTERVAL; wait it out on the server clock.
        await asyncio.sleep(1.1)
    assert reclaimed == 1, "the reclaim sweep must hand the expired row back"


async def _seed_and_claim(backend: Backend) -> tuple[JobId, UUID, tuple[int, int], tuple[int, int]]:
    """Enqueue, claim, then reclaim + re-claim: the stale/live epoch shape.

    Returns ``(job_id, worker_id, stale_pair, live_pair)``: the stale pair
    is the FIRST claim's (attempt, epoch), the live pair the second
    claim's. Both claims go through the backend's real dispatch, and the
    reclaim through its real sweep, so the epoch's advance is the shipped
    semantics, not a simulated mutation.
    """
    actor = f"epoch_pin_{new_base62(6)}".lower()
    if isinstance(backend, InMemoryBackend):
        backend.register_actor_config(actor=actor)
        worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: the twin's dispatch stamps this id; the pins hold the same handle the workers do.
    else:
        pool = backend._dispatcher_pool  # type: ignore[reportPrivateUsage]  # Why: the PG branch needs an actor_config row and a workers row; the pins hold the backend's own pool handle.
        assert pool is not None
        pg_schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]
        worker_id = new_uuid()
        async with pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{pg_schema}".actor_config (actor, queue) '
                "VALUES ($1, 'default') ON CONFLICT (actor) DO NOTHING",
                actor,
            )
            await conn.execute(
                f'INSERT INTO "{pg_schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, $2, $3, $4)",
                worker_id,
                "test-host",
                12345,
                ["default"],
            )

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="indefinite",
            scheduled_at=_PAST,
            # A zero retry curve keeps the reclaim's reschedule delay at
            # zero: the reclaimed row is claimable again immediately.
            retry_base=timedelta(0),
            retry_cap=timedelta(0),
            retry_jitter=0.0,
        )
    )

    first = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=_TICK_LEASE
    )
    assert len(first) == 1
    stale = (first[0].attempt, first[0].claim_epoch)

    await _expire_and_reclaim(backend)

    second = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=_TICK_LEASE
    )
    assert len(second) == 1 and second[0].id == job_id
    live = (second[0].attempt, second[0].claim_epoch)
    assert live[1] == stale[1] + 1, "the epoch must advance by exactly one per claim"
    return job_id, worker_id, stale, live


# (name, driver(backend, job_id, worker_id, attempt, epoch), expected
# refusal observable, expected live observable) - one entry per fenced
# method/arm class. A method missing here is a fence gap.
_FENCED_ARMS = (
    (
        "mark_succeeded",
        lambda b, j, w, a, e: b.mark_succeeded(j, w, {"from": "LIVE_B"}, attempt=a, claim_epoch=e),
        False,
        True,
    ),
    (
        "mark_cancelled",
        lambda b, j, w, a, e: b.mark_cancelled(j, w, attempt=a, claim_epoch=e),
        False,
        True,
    ),
    (
        "mark_snoozed",
        lambda b, j, w, a, e: b.mark_snoozed(j, w, timedelta(seconds=30), attempt=a, claim_epoch=e),
        "noop",
        "scheduled",
    ),
    (
        "mark_retry_after_consume_true",
        lambda b, j, w, a, e: b.mark_retry_after(
            j, w, timedelta(seconds=30), consume_budget=True, attempt=a, claim_epoch=e
        ),
        "noop",
        "scheduled",
    ),
    (
        "mark_retry_after_consume_false",
        lambda b, j, w, a, e: b.mark_retry_after(
            j, w, timedelta(seconds=30), consume_budget=False, attempt=a, claim_epoch=e
        ),
        "noop",
        "scheduled",
    ),
    (
        "mark_failed_or_retry_retry_arm",
        lambda b, j, w, a, e: b.mark_failed_or_retry(
            j, w, _ERROR_A, timedelta(seconds=30), attempt=a, claim_epoch=e
        ),
        "raise",
        "scheduled",
    ),
    (
        "mark_failed_or_retry_fail_arm",
        lambda b, j, w, a, e: b.mark_failed_or_retry(
            j, w, _ERROR_A, None, attempt=a, claim_epoch=e
        ),
        "raise",
        "failed",
    ),
    (
        "mark_interrupted",
        lambda b, j, w, a, e: b.mark_interrupted(j, w, attempt=a, hold=timedelta(0), claim_epoch=e),
        "noop",
        "pending",
    ),
)


@pytest.mark.parametrize(
    ("arm", "driver", "refused", "live_outcome"),
    _FENCED_ARMS,
    ids=[arm[0] for arm in _FENCED_ARMS],
)
async def test_every_fenced_arm_refuses_the_stale_epoch_and_lands_the_live_one(
    backend_pair: Backend,
    arm: str,
    driver: "Callable[[Backend, JobId, UUID, int, int], Coroutine[Any, Any, object]]",
    refused: object,
    live_outcome: object,
) -> None:
    """Each fenced arm refuses the stale claim's write and lands the live
    claim's own outcome, on both backends.

    A stale epoch (a reclaim + redispatch happened after this writer's
    claim) must no-op through the same machinery the worker fence uses;
    the live epoch lands the write with its own outcome. Away from the
    ceiling the attempt number advances alongside the epoch, so the stale
    writer here presents its true-at-the-time attempt and only the epoch
    distinguishes it from the live claim's write.
    """
    backend = backend_pair
    job_id, worker_id, stale, live = await _seed_and_claim(backend)

    if refused == "raise":
        with pytest.raises(WorkerOwnershipMismatch):
            await driver(backend, job_id, worker_id, stale[0], stale[1])
    else:
        observed = await driver(backend, job_id, worker_id, stale[0], stale[1])
        assert observed == refused, (
            f"{arm}: the stale claim's write must be fenced out (got {observed!r})"
        )

    observed_live = await driver(backend, job_id, worker_id, live[0], live[1])
    if isinstance(observed_live, JobRow):
        assert observed_live.status == live_outcome, (
            f"{arm}: the live claim's write must land with its own outcome"
        )
    else:
        assert observed_live == live_outcome, (
            f"{arm}: the live claim's write must land (got {observed_live!r})"
        )


# ── Part C: the archive carries the true epoch ───────────────────────────


async def test_archived_row_keeps_the_live_row_claim_epoch(jobs_app: JobsApp) -> None:
    """A twice-claimed job that goes terminal and then through the real
    archive CTE lands in jobs_archive with the epoch its last claim
    stamped, the value the live row read at archival.

    The archive INSERT names its columns through COPY_FROM_COLUMNS; the
    claim_epoch entry there is what keeps the archived identity true. A
    row archived without it would read the default 0 forever, an epoch
    no claim can ever have stamped, and the column's contract (bumped by
    exactly 1 per claim) would be false for every archived row.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    job_id, worker_id, _stale, live = await _seed_and_claim(backend)

    row = await backend.get(job_id)
    assert row is not None
    assert row.claim_epoch == live[1]

    assert (
        await backend.mark_succeeded(
            job_id, worker_id, {"done": True}, attempt=live[0], claim_epoch=live[1]
        )
        is True
    )

    async with deps.worker_pool.acquire() as conn:
        result = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": timedelta(0)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=schema,
        )
    assert result.archived == 1

    async with deps.worker_pool.acquire() as conn:
        archived = await conn.fetchrow(
            f'SELECT claim_epoch FROM "{schema}".jobs_archive WHERE id = $1', job_id
        )
    assert archived is not None
    assert archived["claim_epoch"] == live[1] == 2
