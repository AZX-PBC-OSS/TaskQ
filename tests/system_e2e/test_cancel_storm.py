"""Lifecycle 4: cancel storms - cancels racing retries racing archive pruning.

Three operators act on one fleet at once: a canceller cancels whatever is
running or pending, a retrier re-pends whatever has come to rest, and the
archiver prunes terminal rows to the archive the whole time. Every pair
of those races on the same rows.

System invariants: the conservation counter balances across BOTH tables
(every enqueued job is exactly one terminal row somewhere, never dropped
by a prune that raced a retry, never resurrected by a retry that raced a
prune); the attempt ledger reconciles; every cancelled row still carries
the operator's request columns (the cancel audit trail survives the
honouring); and the composition actually fired (rows archived, retries
taken, cancels honoured - never a vacuous storm).
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.worker._leader_shared import prune_terminal_jobs
from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, assert_effects_balance, delete_tagged
from tests.system_e2e.actors import (
    FlakyPayload,
    SysPayload,
    sys_fast,
    sys_flaky,
    sys_slow,
)

if TYPE_CHECKING:
    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s4"
# The storm's fleet shape, which every premise budget below derives from:
# 6 flaky jobs (the retry premise's own cohort), 4 slow, 6 fast.
_FLAKY_COHORT = 6
_SLOW_COHORT = 4
_FAST_COHORT = 6

# The storm's budgets are DERIVED, never bare: the storm soaks until the
# vacuousness premises are OBSERVED in the ledger (a retry fired, a
# cancel honoured, the archiver moved a row), and the deadline for that
# observation derives from the scenario's own retry math under the
# co-tenancy band this file's history measured on loaded -n 2 runners
# (74c5931b's premise-wait fix):
#
# * one ladder cycle = the flaky cohort's 1s deferral floor
#   (MIN_DEFERRAL_INTERVAL, the fastest legal ladder) + the worker's
#   50ms claim poll + the premise loop's 250ms observation cadence;
# * taken twice for headroom (a deferral -> re-claim -> observation
#   round trip, twice);
# * times the 20x co-tenancy stretch (the stall band these runners
#   produce between a seed and its observation), plus the original 8s
#   soak as the floor  =>  60.0s.
# It bounds FAILURE only - a premise that never lands is exactly what
# the assertions after the storm red - and the conservation invariants
# hold for any duration the storm runs.
_LADDER_FLOOR_SECS = 1.0  # the flaky ladder's MIN_DEFERRAL_INTERVAL floor
_CLAIM_POLL_SECS = 0.05  # the worker's TASKQ_POLL_INTERVAL (the harness default)
_PREMISE_POLL_SECS = 0.25  # the premise loop's own observation cadence
_LADDER_CYCLE_SECS = _LADDER_FLOOR_SECS + _CLAIM_POLL_SECS + _PREMISE_POLL_SECS
_PREMISE_HEADROOM = 2
_COTENANCY_STRETCH = 20
_SOAK_FLOOR_SECS = 8.0
_STORM_DEADLINE_SECS = (
    _LADDER_CYCLE_SECS * _PREMISE_HEADROOM * _COTENANCY_STRETCH + _SOAK_FLOOR_SECS
)  # 1.3 * 2 * 20 + 8 == 60.0

# The storm worker's slot count, derived the same way: the harness
# default (4) lets the SLOW cohort monopolise every slot for its whole
# (stretched) sleep, queueing the flaky cohort's re-claims behind it.
# On a co-tenancy-stalled runner the deferral windows then expire
# unclaimed and the canceller strips them - the measured CI red: 60s of
# storm with jobs archived and cancels honoured while max(attempt)
# never left 1. The storm's worker gets the slow cohort plus the flaky
# cohort as headroom, so a re-claim never queues behind a sleep it does
# not depend on.
_STORM_WORKER_SLOTS = _SLOW_COHORT + _FLAKY_COHORT

# The retrier's per-tick cohort: a cohort of the fleet's own size, not a
# bare count. Its fuel is the non-cancelled terminal set (see the
# retrier's docstring), dominated by the fast cohort - a cohort-wide
# LIMIT re-pends the whole fuel set each 0.5s tick, while still losing
# some rows to the archiver's 0.9s tick - the archived premise stays
# live.
_RETRIER_TICK_ROWS = _FAST_COHORT


@pytest.mark.timeout(300)
async def test_cancel_storm_racing_retries_racing_archive_prune_conserves(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    worker: WorkerProc | None = None
    try:
        worker = spawn_worker(
            pg_dsn,
            schema,
            tag="s4",
            extra_env={"TASKQ_MAX_CONCURRENCY": str(_STORM_WORKER_SLOTS)},
        )
        wait_worker_ready(worker)

        # The population: retries in flight the whole window (each flaky
        # job fails its first two attempts on the 1s floor ladder), long
        # jobs to cancel, fast jobs to feed the archiver.
        for _ in range(6):
            await sys_client.enqueue(sys_flaky, FlakyPayload(fail_until_attempt=2), tags=[_TAG])
        for _ in range(4):
            await sys_client.enqueue(sys_slow, SysPayload(sleep=6.0), tags=[_TAG])
        for _ in range(6):
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
        enqueued = 16

        stop = asyncio.Event()

        async def canceller() -> int:
            """Operator 1: cancels TARGETED jobs, two unfinished rows per
            tick (an operator storm singles jobs out; a fleet-wide cancel
            every tick would swamp the retry ladder and the race would
            never fire). Mixes the direct arm (pending/scheduled) and the
            cooperative arm (running) through the same client surface."""
            requested = 0
            while not stop.is_set():
                rows = await conn.fetch(
                    f'SELECT id FROM "{schema}".jobs '
                    "WHERE tags @> ARRAY[$1::text] AND status IN "
                    "('pending', 'scheduled', 'running') "
                    "ORDER BY random() LIMIT 2",
                    _TAG,
                )
                for row in rows:
                    with contextlib.suppress(Exception):
                        await sys_client.cancel(row["id"], reason="storm")
                        requested += 1
                await asyncio.sleep(0.7)
            return requested

        async def retrier() -> int:
            """Operator 2: re-pend whatever has come to rest, racing the
            archiver that is moving those same rows away. Owns its own
            connection: concurrent tasks must never share the ledger
            connection (asyncpg forbids concurrent use of one connection).

            The re-pend set EXCLUDES 'cancelled': the cancel audit trail is
            pinned truthful in both directions (a cancelled row must carry
            the operator's request columns, and a state_change('cancelled')
            event must belong to a row that IS cancelled), so a re-pended
            cancel would make the trail lie - a mutation the ledger says
            happened standing on a row that un-happened it. The fuel the
            retry premise needs is the rest of the terminal set: a
            re-pended row's re-claim stamps its attempt up, which is
            exactly the cancel/retry race the premises observe."""
            rconn = await asyncpg.connect(pg_dsn)
            retried = 0
            # The non-cancelled terminal statuses (see the docstring): the
            # re-pend fuel, derived from the state machine's own terminal
            # set rather than spelled bare.
            rependable = sorted(TERMINAL_STATUSES - {"cancelled"})
            try:
                while not stop.is_set():
                    rows = await rconn.fetch(
                        # status::text: jobs.status is the job_status ENUM and
                        # `enum = ANY($2::text[])` is an operator error - the
                        # retrier task crashed silently on its very first tick
                        # in every environment this shape ever ran in (the
                        # task's exception is swallowed until GC), leaving the
                        # storm's retry fuel to the flaky ladder alone - which
                        # a loaded -n 2 runner's canceller strips faster than
                        # a starved worker re-claims: the measured
                        # "no job ever retried" red. The ::text cast puts the
                        # comparison on the text side, where the array
                        # already is.
                        f'SELECT id FROM "{schema}".jobs '
                        "WHERE tags @> ARRAY[$1::text] AND status::text = ANY($2::text[]) "
                        f"ORDER BY random() LIMIT {_RETRIER_TICK_ROWS}",
                        _TAG,
                        rependable,
                    )
                    for row in rows:
                        with contextlib.suppress(Exception):
                            if await sys_client.retry_job(row["id"]):
                                retried += 1
                    await asyncio.sleep(0.5)
            finally:
                await rconn.close()
            return retried

        async def archiver() -> None:
            """Operator 3: the leader's prune, zero retention on succeeded,
            running the whole window against the same rows. Owns its own
            connection (see the retrier)."""
            aconn = await asyncpg.connect(pg_dsn)
            try:
                while not stop.is_set():
                    await prune_terminal_jobs(
                        aconn,
                        retention_per_status={
                            "succeeded": timedelta(0),
                            **{
                                s: timedelta(days=3650)
                                for s in TERMINAL_STATUSES
                                if s != "succeeded"
                            },
                        },
                        archive_retention=timedelta(days=3650),
                        batch_size=2,
                        schema=schema,
                    )
                    # The tick is a SCENARIO KNOB, not a product constant, and
                    # it must be slower than the retrier's 0.5s: the retrier
                    # has to beat the archiver to a terminal row often enough
                    # for a retry to fire (the scenario's premise, here by
                    # construction rather than by runner weather - a faster
                    # tick let the archiver consume every terminal row before
                    # the retrier ever saw one on loaded runners). The
                    # archiver still archives plenty (its cohort is the 6
                    # sys_fast jobs) and the storm deadline bounds failure.
                    await asyncio.sleep(0.9)
            finally:
                await aconn.close()

        storm = [asyncio.create_task(canceller()), asyncio.create_task(retrier())]
        prune_task = asyncio.create_task(archiver())
        # The premise poll owns ITS OWN connection: the ledger connection
        # is the canceller task's while the storm runs (concurrent tasks
        # must never share an asyncpg connection - the retrier's and the
        # archiver's comments up top are this module's own rule).
        pconn = await asyncpg.connect(pg_dsn)
        try:
            try:
                async with asyncio.timeout(_STORM_DEADLINE_SECS):
                    while True:
                        # The vacuousness premises, read from the ledger the
                        # assertions below re-read: the race is provably fired in
                        # every direction before the storm stops.
                        premises = await pconn.fetchrow(
                            "SELECT ("
                            f'SELECT max(attempt)::int FROM "{schema}".jobs '
                            "WHERE tags @> ARRAY[$1::text]) AS max_live_attempt, ("
                            f'SELECT max(attempt)::int FROM "{schema}".jobs_archive '
                            "WHERE tags @> ARRAY[$1::text]) AS max_archived_attempt, ("
                            f'SELECT count(*)::int FROM "{schema}".jobs '
                            "WHERE tags @> ARRAY[$1::text] AND status = 'cancelled'"
                            ") + ("
                            f'SELECT count(*)::int FROM "{schema}".jobs_archive '
                            "WHERE tags @> ARRAY[$1::text] AND status = 'cancelled'"
                            ") AS cancelled, ("
                            f'SELECT count(*)::int FROM "{schema}".jobs_archive '
                            "WHERE tags @> ARRAY[$1::text]) AS archived",
                            _TAG,
                        )
                        assert premises is not None
                        if (
                            max(
                                premises["max_live_attempt"] or 0,
                                premises["max_archived_attempt"] or 0,
                            )
                            >= 2
                            and premises["cancelled"] >= 1
                            and premises["archived"] > 0
                        ):
                            break
                        await asyncio.sleep(0.25)
            except TimeoutError:
                # The premises never landed in the deadline: the
                # assertions below red with the vacuous-race message -
                # but the storm's tasks still get their stop, exactly the
                # normal teardown path. The TimeoutError must NOT
                # propagate up past stop.set(): it would skip the
                # cancel/gather and leak every operator task.
                pass
        finally:
            await pconn.close()
        stop.set()
        await prune_terminal_jobs(
            conn,
            retention_per_status={
                "succeeded": timedelta(0),
                **{s: timedelta(days=3650) for s in TERMINAL_STATUSES if s != "succeeded"},
            },
            archive_retention=timedelta(days=3650),
            batch_size=2,
            schema=schema,
        )
        for task in storm:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*storm, return_exceptions=True)
        prune_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await prune_task

        # Cancel audit truthfulness, per the path that produced each
        # cancelled row: the COOPERATIVE path (running jobs) stamps the
        # operator's request columns and keeps them on the terminal row;
        # the DIRECT path (pending/scheduled) stamps the origin marker in
        # error_class ('CancelledBeforeStart') instead - the columns name
        # a request that was made, the marker names the outcome, and a
        # cancelled row with NEITHER is a mutation nobody wrote down.
        # (The state_change event's existence is audit_violations' half.)
        cancel_rows = await conn.fetch(
            "SELECT status::text AS status, error_class AS error_class, "
            "cancel_requested_at AS requested_at FROM ("
            f'SELECT status, error_class, cancel_requested_at FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = 'cancelled' "
            "UNION ALL "
            f'SELECT status, error_class, cancel_requested_at FROM "{schema}".jobs_archive '
            "WHERE tags @> ARRAY[$1::text] AND status = 'cancelled'"
            ") c",
            _TAG,
        )
        cancelled = len(cancel_rows)
        unmarked = [
            dict(r)
            for r in cancel_rows
            if r["requested_at"] is None
            and r["error_class"]
            not in (
                "CancelledBeforeStart",
                "CancelledCooperatively",
            )
        ]
        assert not unmarked, (
            f"{len(unmarked)} of {cancelled} cancelled rows carry neither the "
            f"operator's request columns nor an origin marker: {unmarked}"
        )

        # Conservation across BOTH tables: enqueued == live + archived,
        # every row terminal (assert_balanced runs the counter and the
        # audit predicate over the whole tagged population).
        counts = await assert_balanced(conn, schema, _TAG)
        archived_total = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]',
            _TAG,
        )
        assert sum(counts.values()) + archived_total == enqueued, (
            f"jobs-in {enqueued} != terminal-out {sum(counts.values()) + archived_total} "
            f"(live {counts}, archived {archived_total}) - a prune/retry race "
            "dropped or duplicated a row"
        )

        # The composition fired in every direction, else the pin is vacuous.
        print(f"[sys-s4] live counts: {counts}, archived: {archived_total}")
        assert archived_total > 0, "the archiver never moved a row"
        max_live_attempt = await conn.fetchval(
            f'SELECT max(attempt)::int FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]',
            _TAG,
        )
        max_archived_attempt = await conn.fetchval(
            f'SELECT max(attempt)::int FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]',
            _TAG,
        )
        attempts_seen = max((max_live_attempt or 0), (max_archived_attempt or 0))
        assert attempts_seen >= 2, "no job ever retried: the cancel/retry race never fired"
        assert cancelled >= 1, "the canceller never honoured a request"
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)
