# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Clock-domain pins for the worker's DECISION inputs.

TaskQ writes its deadlines with the PG server clock (``clock_timestamp()`` /
``statement_timestamp()``): lease expiry, cancel observations, scheduled_at
deferrals. A worker process whose own wall clock skews from PG's (container
drift, VM pause, NTP step) must never have that skew change a DECISION that
those deadlines govern. The failure shapes, one per skew direction:

* Python BEHIND PG (S = python - server < 0): a remaining-lease margin
  computed client-side reads LARGE, the renewal gate skips, the lease
  lapses while the body runs, and the reclaim sweep hands the row back -
  the reclaim-vs-live-body double-run.
* Python AHEAD of PG (S > 0): a remaining-lease margin computed
  client-side reads SMALL, rows renew eagerly (a non-HOT update storm),
  and any client-side "is the deadline past" check fires early - mass
  false lapses.

These pins hold the two decisions the heartbeat and cancel paths make
against a Python clock offset by a workday-hostile +/-120 s, the same
magnitude test_clock_domain_isolation.py uses for the enqueue path:

* ``test_renewal_gate_judged_in_the_lease_domain_under_python_clock_skew`` -
  the heartbeat's threshold-gated renewal compares
  ``lock_expires_at <= clock_timestamp() + threshold`` INSIDE the
  statement: the same clock that stamps the lease judges it. The seeds
  land each row in the band where a client-side gate (Python now +/- the
  skew, compared against the fetched ``lock_expires_at``) decides the
  OPPOSITE of the server, and the server verdict must win both ways.
  After the renewal the real reclaim sweep must leave the live body
  running: no Python skew can start the double-run.
* ``test_cancel_grace_measured_from_local_observation_not_the_row_stamp`` -
  the cancel ladder's graces measure THIS process's observation of the
  cancel flag (``loop.time()``, a monotonic anchor), never the row's
  ``cancel_requested_at`` wall stamp (a PG-domain instant an hour in the
  past here) and never a wall clock the skew can move. The escalation
  must wait out the full grace from the local observation at every skew.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend._sql import build_heartbeat_sql
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker
from taskq.testing.settings import make_integration_settings
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps, open_worker_deps
from tests._clock_skew import SkewedClock

pytestmark = pytest.mark.integration

#: The skew magnitude under test: two minutes, the VM-step / container-drift
#: order the ops guidance treats as survivable-but-real. Both directions are
#: run; the two decisions under pin fail in OPPOSITE directions under them.
_SKEWS = (timedelta(seconds=-120), timedelta(seconds=0), timedelta(seconds=120))

#: The heartbeat renewal gate's threshold, 30 s: wide enough that the
#: +/-120 s seeds below land deep inside the disagreement bands.
_THRESHOLD = timedelta(seconds=30)

#: The lease a renewal stamps. Long enough that the renewed row is nowhere
#: near expiry when the reclaim sweep runs right after.
_LEASE = timedelta(seconds=8)

#: The reclaim sweep's graces for the live-body assertion: zero, the
#: sibling files' convention, the cancel carve-out then contributes only
#: its flat 60 s margin and the row carries no cancel flag anyway.
_GRACE = timedelta(seconds=0)


class _Payload(BaseModel):
    """Minimal payload for the cancel pin's JobContext."""


def _make_ctx(job_id: UUID, worker_id: UUID) -> JobContext[BaseModel]:
    """A minimal JobContext bound to *job_id*, the cancel-hook idiom."""
    return JobContext(
        job_id=job_id,
        actor="test",
        queue="default",
        attempt=1,
        claim_epoch=0,
        worker_id=worker_id,
        payload=_Payload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=None,
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor="test",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


async def _pg_now(conn: asyncpg.Connection) -> Any:
    """One server-clock read; every domain claim below is stated against it."""
    return await conn.fetchval("SELECT clock_timestamp()")


async def _read_lease_with_clock(
    conn: asyncpg.Connection, schema: str, job_id: UUID
) -> tuple[Any, Any]:
    """The row's lease and the server clock, read in ONE statement.

    The same single-statement discipline the heartbeat integration file
    uses: the assertion compares two PG-domain values, so the test
    process's own clock (and any real drift this host carries) cannot
    corrupt the verdict.
    """
    row = await conn.fetchrow(
        f'SELECT clock_timestamp() AS pg_now, lock_expires_at FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert row is not None, f"job row {job_id} vanished"
    return row["pg_now"], row["lock_expires_at"]


# ── Pin 1: the renewal gate is judged in the lease's own domain ──────────


@pytest.mark.parametrize("skew", _SKEWS, ids=lambda s: f"skew={int(s.total_seconds()):+d}s")
async def test_renewal_gate_judged_in_the_lease_domain_under_python_clock_skew(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    skew: timedelta,
) -> None:
    """The gated renewal's threshold comparison is SERVER-side.

    For each skew, one row is seeded in the band where a client-side
    margin (a Python clock offset by *skew*, compared against the fetched
    ``lock_expires_at``) decides the OPPOSITE of the server gate, the
    heartbeat's own statement (built by ``build_heartbeat_sql`` with the
    threshold bound as an INTERVAL, never a Python timestamp) runs, and
    the server's verdict must govern:

    * skew -120 s (Python behind): remaining 10 s. Server: 10 <= 30,
      renew. Client-side view: 130 > 30, skip - the skip is the dangerous
      verdict, the lease would lapse and the reclaim would race a live
      body. The row must RENEW.
    * skew +120 s (Python ahead): remaining 140 s. Server: 140 > 30,
      skip. Client-side view: 20 <= 30, renew - the eager verdict, a
      non-HOT update storm. The row must stay UNRENEWED.
    * skew 0: the control, remaining 10 s, renew, same as -120 s.

    After the renewed case the REAL reclaim sweep runs against the live
    row: it must reclaim nothing. A Python clock skew alone can never
    start the reclaim-vs-live-body double-run, because the sweep judges
    ``lock_expires_at < statement_timestamp()`` and the renewal wrote
    that stamp in the same PG domain.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)

    # -120 s and 0 share the "server must renew" seed; +120 s gets the
    # "server must skip" seed. Both seeds are ABSOLUTE instants read off
    # the server clock, so the seeding itself never crosses domains.
    server_renews = skew <= timedelta(0)
    remaining = timedelta(seconds=10) if server_renews else timedelta(seconds=140)

    pg_now = await _pg_now(clean_pg_conn)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=pg_now + remaining,
        with_events=False,
    )

    _, jobs_lock_sql, _ = build_heartbeat_sql(schema, renewal_threshold=_THRESHOLD)
    # The mechanism, stated in the rendered text: the threshold is an
    # INTERVAL bound against the server clock inside the statement, the
    # write is stamped by the same clock, and no Python timestamp is
    # bound anywhere. This shape IS the contract - a gate that compares
    # against a bound absolute instant (a Python now) flips every
    # verdict above the moment the host's clocks diverge.
    assert "lock_expires_at = clock_timestamp() + $2" in jobs_lock_sql
    assert "lock_expires_at <= clock_timestamp() + $4::interval" in jobs_lock_sql
    assert "::timestamptz" not in jobs_lock_sql and "$5" not in jobs_lock_sql
    # The heartbeat loop's exact call shape: (worker_id, lease, disowned,
    # threshold). No Python timestamp crosses this boundary - the pin
    # fails if one ever does (see the mutation note in the module
    # docstring).
    await clean_pg_conn.execute(
        jobs_lock_sql,
        worker_id,
        _LEASE,
        [],
        _THRESHOLD,
    )

    pg_now_after, lease_after = await _read_lease_with_clock(clean_pg_conn, schema, job_id)

    if server_renews:
        remaining_after = lease_after - pg_now_after
        # Single-statement comparison, PG domain both sides: the renewed
        # lease sits ~_LEASE in the future of the clock read beside it.
        assert remaining_after > timedelta(0), (
            f"skew {skew}: the gate skipped a row the server judges under "
            f"the threshold (remaining {remaining}); a client-side margin "
            "has moved into the renewal decision - the lease would lapse "
            "under a behind-clock worker and the reclaim would race its "
            "live body."
        )
        assert remaining_after <= _LEASE, (
            f"skew {skew}: the renewal stamped {remaining_after} of lease, "
            f"not ~{_LEASE}; the statement's stamp is not the server clock."
        )
        # The double-run cannot start: the sweep judges the row's lease
        # in the same domain the renewal just re-stamped, whatever the
        # worker's own clock says.
        reclaimed = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn, _GRACE, _GRACE, schema=schema
        )
        status = await clean_pg_conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert reclaimed == 0 and status == "running", (
            f"skew {skew}: the reclaim sweep took a live, just-renewed body "
            f"(reclaimed {reclaimed} rows, status {status!r}) - the "
            "double-run shape a skewed worker must never produce."
        )
    else:
        assert lease_after == pg_now + remaining, (
            f"skew {skew}: the gate renewed a row the server judges well "
            f"above the threshold (remaining {remaining}); the eager "
            "verdict is the Python-ahead direction - a client-side margin "
            "has moved into the renewal decision."
        )


# ── Pin 2: the cancel ladder's graces anchor to the local observation ────


@pytest.mark.parametrize("skew", _SKEWS, ids=lambda s: f"skew={int(s.total_seconds()):+d}s")
async def test_cancel_grace_measured_from_local_observation_not_the_row_stamp(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    skew: timedelta,
) -> None:
    """The escalation waits out the grace from the LOCAL observation.

    Tick 1 observes the flag (phase 1): the hour-old ``cancel_requested_at``
    wall stamp must not advance the ladder - no escalation, the PG row
    still reads cancel_phase 1. The pin then back-dates the LOCAL
    observation stamp past the cancel grace and ticks again: the
    escalation fires (the PG row reads cancel_phase 2). The boundary is
    the local observation's age, at every skew; a regression that
    measures the grace from the row's PG wall stamp escalates on tick 1
    and fails this pin at every skew, and a regression that swaps the
    loop clock for a skewable wall clock moves the boundary by the skew.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)

    pg_now = await _pg_now(clean_pg_conn)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        cancel_phase=1,
        cancel_requested_at=pg_now - timedelta(hours=1),
        with_events=False,
    )

    settings = make_integration_settings(
        module_pg_schema.pg_dsn,
        SCHEMA_NAME=schema,
        CANCELLATION_GRACE_PERIOD="1",
        CLEANUP_GRACE_PERIOD="1",
        TERMINATION_GRACE_PERIOD="30",
    )
    async with AsyncExitStack() as stack:
        deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
        backend = PostgresBackend(
            deps,
            SkewedClock(SystemClock(), skew),
            timedelta(seconds=1),
            timedelta(seconds=1),
        )
        controller = make_cancel_controller(deps, worker_id, backend)

        task = asyncio.get_running_loop().create_task(asyncio.sleep(3600))
        ctx = _make_ctx(job_id, worker_id)
        await deps.active_jobs.register(job_id, task, ctx)

        try:
            # Tick 1: observation only. The row's wall stamp is an hour
            # old; the ladder's clocks did not start until NOW.
            await controller.run_in_tx(clean_pg_conn)
            active = deps.active_jobs.get(job_id)
            assert active is not None
            assert active.cancel_phase == CancelPhase.COOPERATIVE, (
                f"skew {skew}: tick 1 landed past phase 1 "
                f"({active.cancel_phase}); the ladder measured its grace "
                "from the row's PG wall stamp (or a wall clock) instead of "
                "the local observation - a cancel fires hours early, way "
                "past its bound."
            )
            phase = await clean_pg_conn.fetchval(
                f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert phase == 1, (
                f"skew {skew}: tick 1 wrote the escalation to PG despite "
                "the grace not having elapsed from the local observation."
            )

            # Tick 2: the local grace HAS elapsed (back-date the
            # observation past the 1 s cancel grace, under the 2 s
            # cancel+cleanup sum so the tick escalates without queuing
            # the abandon).
            active.cancel_observed_at = asyncio.get_running_loop().time() - 1.5
            await controller.run_in_tx(clean_pg_conn)
            assert active.cancel_phase >= CancelPhase.FORCED, (
                f"skew {skew}: the escalation did not fire after the local "
                "grace elapsed - the ladder is anchored to something other "
                "than the observation stamp (a monotonic anchor lost)."
            )
            phase = await clean_pg_conn.fetchval(
                f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert phase == 2, (
                f"skew {skew}: the local ladder escalated but the PG row "
                f"still reads cancel_phase {phase}; the escalation write "
                "did not land."
            )
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
