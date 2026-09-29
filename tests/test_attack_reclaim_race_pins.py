# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.
"""Red-team race pins for the reclaim machinery (Area 9: reclaim & recovery).

The predicate and parity pins elsewhere in the suite run the reclaim
sweep against a quiescent database. Every test here runs it against a
CONCURRENT writer, the races the leader loop meets in production:

* two leaders sweeping the same expired backlog concurrently (a rolling
  deploy before the lock names converge) - ledger conservation: every
  job exactly one attempt row, one event, one reclaim;
* the sweep racing a LIVE holder's heartbeat renewal - a committed
  renewal must win over a reclaim (the row stays running on its future
  lease, however the two statements interleave), and a renewal the sweep
  beat must match nothing (the re-pend cleared the hold the renewal's
  WHERE reads);
* the sweep racing a terminal write in flight - the row is skipped
  while the write holds the lock, and the terminalised row is never
  reclaimed (no phantom crash attempt row, no reclaim event);
* the leader's sweep racing a partitioned worker's ``isolate_self`` -
  exactly one of the two reclaim arbiters wins each row, and the
  loser's guarded UPDATE reports the loss instead of double-writing the
  ledger;
* the budget arithmetic at its boundaries (``attempt`` at/below
  ``max_attempts``, the degenerate zero-base curve's monopolisation
  floor, the operator ceiling clamping a row stamped with a 10-year
  cap);
* the audit columns (the attempt row's ``due_at`` is the claim-time
  ``scheduled_at`` read pre-reschedule; a dangling holder records a
  NULL ``worker_id`` instead of raising).

All pins pass against the current implementation; they exist so a
rewrite of ``_SWEEP_1_SQL`` / the renewal statements cannot quietly
resurrect the races they close.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.constants import DEFAULT_MAX_RETRY_BACKOFF, MIN_DEFERRAL_INTERVAL
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_worker
from taskq.worker.heartbeat import (
    _ISOLATE_JOB_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: the pin replays the isolate arbiter's guarded UPDATE verbatim; a copy would drift from the statement the worker actually runs.
)

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=0)


async def _seed_running(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    worker_id: UUID | None,
    *,
    max_attempts: int = 3,
    attempt: int = 1,
    retry_kind: str = "transient",
    lease_expires_in: timedelta = timedelta(seconds=-10),
    cancel_phase: int = 0,
    started_seconds_ago: float = 30.0,
    retry_base_seconds: float = 5.0,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs ('
        "    id, actor, queue, payload, max_attempts, retry_kind,"
        "    status, priority, attempt, scheduled_at,"
        "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
        "    cancel_phase, cancel_requested_at, retry_base_seconds"
        ") VALUES ("
        "    $1, 'test_actor', 'default', '{\"key\": \"value\"}'::jsonb,"
        "    $2::smallint, $3::text,"
        "    'running', 0, $4::smallint, clock_timestamp(),"
        "    $5::uuid, clock_timestamp() + $6::interval,"
        "    clock_timestamp() - ($7::double precision * interval '1 second'),"
        "    clock_timestamp() - ($7::double precision * interval '1 second'),"
        "    $8::smallint,"
        "    CASE WHEN $8::smallint = 0 THEN NULL ELSE clock_timestamp() END,"
        f"    $9::float8"
        ")",
        job_id,
        max_attempts,
        retry_kind,
        attempt,
        worker_id,
        lease_expires_in,
        started_seconds_ago,
        cancel_phase,
        retry_base_seconds,
    )


async def _job(conn: asyncpg.Connection, schema: str, job_id: UUID) -> asyncpg.Record:
    row = await conn.fetchrow(f'SELECT * FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    return row


# ── Attack 1: two leaders double-reclaim ─────────────────────────────


class TestDoubleReclaimRace:
    async def test_two_leaders_sweeping_concurrently_never_double_reclaim(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """Two concurrent sweepers (rolling deploy, lock-name convergence)
        over the same expired backlog: every job exactly one attempt row,
        one event, one reclaim; both connections drain to zero; nothing
        raises."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        job_ids = [new_uuid() for _ in range(60)]
        for jid in job_ids:
            await _seed_running(clean_pg_conn, schema, jid, worker_id)

        conn_b = await asyncpg.connect(pg_dsn)

        async def drain(conn: asyncpg.Connection) -> int:
            total = 0
            for _ in range(500):
                n = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)
                total += n
                if n == 0:
                    break
                await asyncio.sleep(0)
            return total

        totals = await asyncio.gather(
            drain(clean_pg_conn),
            drain(conn_b),
            return_exceptions=True,
        )
        await conn_b.close()

        for exc in totals:
            assert not isinstance(exc, BaseException), f"a leader sweep raised: {exc!r}"

        attempts = await clean_pg_conn.fetch(
            f'SELECT job_id, count(*) AS n FROM "{schema}".job_attempts GROUP BY job_id'
        )
        dupes = [(r["job_id"], r["n"]) for r in attempts if r["n"] != 1]
        assert not dupes, f"double-reclaimed jobs carry duplicated attempt rows: {dupes[:5]}"

        events = await clean_pg_conn.fetch(
            f'SELECT job_id, count(*) AS n FROM "{schema}".job_events GROUP BY job_id'
        )
        dupes = [(r["job_id"], r["n"]) for r in events if r["n"] != 1]
        assert not dupes, f"double-reclaimed jobs carry duplicated events: {dupes[:5]}"

        assert sum(t for t in totals if not isinstance(t, BaseException)) == len(job_ids)
        still_running = await clean_pg_conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'running'"
        )
        assert still_running == 0


# ── Attack 2: live renewal vs reclaim (the fence interplay) ──────────


class TestLiveRenewalVsReclaim:
    async def test_renewal_that_wins_the_row_lock_never_loses_the_job(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """A holder renewing its lease concurrently with a sweep: whenever
        the renewal commits first, the row must stay running with a FUTURE
        lease; whenever the sweep wins, the renewal must match nothing and
        the row must be handed back. Never both, never neither."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        job_id = new_uuid()
        await _seed_running(
            clean_pg_conn,
            schema,
            job_id,
            worker_id,
            lease_expires_in=timedelta(seconds=-0.05),
        )

        conn_b = await asyncpg.connect(pg_dsn)
        renew_sql = (
            f'UPDATE "{schema}".jobs '
            "SET last_heartbeat_at = clock_timestamp(), "
            "    lock_expires_at = clock_timestamp() + interval '60 seconds' "
            "WHERE id = $1 AND locked_by_worker = $2 AND status = 'running'"
        )
        for _ in range(50):
            # Alternate the two orders, renewal-first every iteration: a
            # renewal that committed before the sweep's statement started
            # must be seen by the sweep's snapshot (a future lease), and
            # the sweep must leave the row alone.
            renewed = await conn_b.execute(renew_sql, job_id, worker_id)
            n = await PostgresBackend.sweep_expired_locks(
                clean_pg_conn, _GRACE, _GRACE, schema=schema
            )
            if renewed.endswith("1"):
                if n != 0:
                    pytest.fail(
                        "the sweep re-pended a row whose lease a committed "
                        "renewal had pushed into the future"
                    )
                row = await _job(clean_pg_conn, schema, job_id)
                assert row["status"] == "running"
                assert row["lock_expires_at"] > datetime.now(UTC), (
                    "a committed renewal left the row running with a past lease"
                )
            await asyncio.sleep(0.001)
        await conn_b.close()

    async def test_sweep_wins_then_renewal_matches_nothing(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """The mirror order: the sweep commits the re-pend while a renewal
        is mid-flight; the renewal must not resurrect the hold, and the
        stale holder's terminal write must be fenced out."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        job_id = new_uuid()
        await _seed_running(
            clean_pg_conn,
            schema,
            job_id,
            worker_id,
            lease_expires_in=timedelta(seconds=-0.05),
        )

        conn_b = await asyncpg.connect(pg_dsn)
        tx = conn_b.transaction()
        await tx.start()
        # The renewal grabs the row lock first.
        await conn_b.execute(
            f'UPDATE "{schema}".jobs '
            "SET lock_expires_at = clock_timestamp() + interval '60 seconds' "
            "WHERE id = $1 AND locked_by_worker = $2 AND status = 'running'",
            job_id,
            worker_id,
        )
        # The sweep cannot take the locked row.
        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 0, "the sweep took a row the renewal held locked"
        await tx.commit()

        # The renewal committed a FUTURE lease; the sweep must now see a
        # future lease and leave the row alone.
        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 0, (
            "the sweep re-pended a row whose lease was renewed into the "
            "future before the sweep's statement started"
        )
        row = await _job(clean_pg_conn, schema, job_id)
        assert row["status"] == "running"
        await conn_b.close()

    async def test_in_flight_terminal_write_blocks_then_wins_over_the_sweep(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """A terminal write in flight (the row locked, uncommitted) when
        the lease is already past: the sweep must SKIP the row, and once
        the terminal write commits, the sweep must never reclaim (let
        alone crash) the succeeded job. The stale holder's fence never
        even has to fire."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        job_id = new_uuid()
        await _seed_running(
            clean_pg_conn,
            schema,
            job_id,
            worker_id,
            lease_expires_in=timedelta(seconds=-0.05),
        )

        conn_b = await asyncpg.connect(pg_dsn)
        tx = conn_b.transaction()
        await tx.start()
        # The consumer's mark_succeeded in flight: row locked, uncommitted.
        await conn_b.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
            "finished_at = clock_timestamp(), locked_by_worker = NULL, "
            "lock_expires_at = NULL WHERE id = $1 AND locked_by_worker = $2 "
            "AND status = 'running'",
            job_id,
            worker_id,
        )
        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 0, "the sweep took a row a terminal write held locked"
        await tx.commit()

        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 0, "the sweep reclaimed a terminalised job"
        row = await _job(clean_pg_conn, schema, job_id)
        assert row["status"] == "succeeded"
        assert row["finished_at"] is not None
        attempts = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1', job_id
        )
        assert attempts == 0, "a skipped-then-terminalised job must carry no crash attempt row"
        assert events == 0, "a skipped-then-terminalised job must carry no reclaim event"
        await conn_b.close()

    async def test_sweep_and_isolate_race_keeps_one_arbiter_per_row(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        pg_dsn: str,
    ) -> None:
        """Two reclaim arbiters on the same rows - the leader's sweep and a
        partitioned worker's isolate_self: every row exactly one arbiter's
        transition, one attempt row, one event, and the isolate's own
        accounting reports the rows it lost as lost_race (selected =
        pending + crashed + cancelled + lost_race)."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        job_ids = [new_uuid() for _ in range(20)]
        for jid in job_ids:
            await _seed_running(clean_pg_conn, schema, jid, worker_id)

        # The isolate's arbiter, replayed per row on a second connection,
        # concurrent with the leader's sweep - the two reclaim writers.
        isolate_sql = _ISOLATE_JOB_SQL_TEMPLATE.format(schema=schema)
        conn_b = await asyncpg.connect(pg_dsn)

        async def run_isolate() -> tuple[int, int]:
            won = 0
            lost = 0
            for jid in job_ids:
                updated = await conn_b.fetchrow(isolate_sql, jid, worker_id, 86400.0)
                if updated is None:
                    lost += 1
                else:
                    won += 1
                await asyncio.sleep(0)
            return won, lost

        async def run_sweeps() -> int:
            total = 0
            for _ in range(500):
                n = await PostgresBackend.sweep_expired_locks(
                    clean_pg_conn, _GRACE, _GRACE, schema=schema
                )
                total += n
                if n == 0:
                    break
                await asyncio.sleep(0)
            return total

        results = await asyncio.gather(run_isolate(), run_sweeps(), return_exceptions=True)
        for r in results:
            assert not isinstance(r, BaseException), f"a reclaim arbiter raised: {r!r}"
        isolate_won, isolate_lost = cast("tuple[int, int]", results[0])
        swept_total = cast("int", results[1])
        await conn_b.close()

        assert isolate_won + isolate_lost == len(job_ids), (
            "the isolate's arithmetic must account for every row"
        )
        assert swept_total == isolate_lost, (
            f"each row must be reclaimed exactly once: the isolate won "
            f"{isolate_won}, lost {isolate_lost}, but the sweep transitioned {swept_total}"
        )

        dupes = await clean_pg_conn.fetch(
            f'SELECT job_id, count(*) AS n FROM "{schema}".job_attempts '
            "GROUP BY job_id HAVING count(*) > 1"
        )
        assert not dupes, f"duplicated attempt rows across the two arbiters: {dupes[:5]}"
        dupes = await clean_pg_conn.fetch(
            f'SELECT job_id, count(*) AS n FROM "{schema}".job_events '
            "GROUP BY job_id HAVING count(*) > 1"
        )
        assert not dupes, f"duplicated events across the two arbiters: {dupes[:5]}"


# ── Attack 3: budget arithmetic at exact boundaries ──────────────────


class TestBudgetBoundaries:
    async def test_attempt_at_max_is_crashed_below_is_pending(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        at_max = new_uuid()
        under = new_uuid()
        await _seed_running(clean_pg_conn, schema, at_max, worker_id, max_attempts=2, attempt=2)
        await _seed_running(clean_pg_conn, schema, under, worker_id, max_attempts=2, attempt=1)

        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 2
        assert (await _job(clean_pg_conn, schema, at_max))["status"] == "crashed"
        assert (await _job(clean_pg_conn, schema, under))["status"] == "pending"

    async def test_degenerate_zero_base_row_reschedules_past_now(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A zero-base curve (a degenerate row) must draw the
        monopolisation floor, never re-pend at or before now."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        jid = new_uuid()
        await _seed_running(clean_pg_conn, schema, jid, worker_id, retry_base_seconds=0.0)

        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 1
        row = await _job(clean_pg_conn, schema, jid)
        assert row["status"] == "pending"
        delta = row["scheduled_at"] - datetime.now(UTC)
        assert delta >= MIN_DEFERRAL_INTERVAL - timedelta(milliseconds=50), (
            f"a zero-base degenerate row re-pended at +{delta}: the claim/"
            "reclaim loop with no period the floor exists to prevent"
        )

    async def test_cap_above_max_retry_backoff_is_clamped(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The effective cap is min(row cap, max_retry_backoff); a row
        stamped with a 10-year cap draws under the operator ceiling."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        jid = new_uuid()
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".jobs ('
            "    id, actor, queue, payload, max_attempts, retry_kind,"
            "    status, priority, attempt, scheduled_at,"
            "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
            "    cancel_phase, retry_base_seconds, retry_cap_seconds"
            ") VALUES ("
            "    $1, 'test_actor', 'default', '{}'::jsonb,"
            "    3::smallint, 'transient',"
            "    'running', 0, 1::smallint, clock_timestamp(),"
            "    $2::uuid, clock_timestamp() - interval '10 seconds',"
            "    clock_timestamp() - interval '30 seconds',"
            "    clock_timestamp() - interval '30 seconds',"
            "    0::smallint, 3600.0::float8, 315360000.0::float8"
            ")",
            jid,
            worker_id,
        )
        n = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _GRACE,
            _GRACE,
            schema=schema,
            max_retry_backoff=timedelta(hours=24),
        )
        assert n == 1
        row = await _job(clean_pg_conn, schema, jid)
        delta = row["scheduled_at"] - datetime.now(UTC)
        assert delta <= DEFAULT_MAX_RETRY_BACKOFF + timedelta(seconds=5), (
            f"a row capped above the operator ceiling re-pended at +{delta}"
        )


# ── Attack 4: audit columns ──────────────────────────────────────────


class TestReclaimAudit:
    async def test_due_at_is_the_claim_time_scheduled_at(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The attempt row's due_at is the claim-time scheduled_at, read
        before the re-pend arm rescheduled the row."""
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await create_worker(clean_pg_conn, schema, worker_id)
        jid = new_uuid()
        claim_scheduled = datetime.now(UTC) - timedelta(seconds=5)
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".jobs ('
            "    id, actor, queue, payload, max_attempts, retry_kind,"
            "    status, priority, attempt, scheduled_at,"
            "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
            "    cancel_phase"
            ") VALUES ("
            "    $1, 'test_actor', 'default', '{}'::jsonb,"
            "    3::smallint, 'transient',"
            "    'running', 0, 1::smallint, $3,"
            "    $2::uuid, clock_timestamp() - interval '10 seconds',"
            "    clock_timestamp() - interval '30 seconds',"
            "    clock_timestamp() - interval '30 seconds',"
            "    0::smallint"
            ")",
            jid,
            worker_id,
            claim_scheduled,
        )

        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 1
        att = await clean_pg_conn.fetchrow(
            f'SELECT due_at FROM "{schema}".job_attempts WHERE job_id = $1', jid
        )
        assert att is not None and att["due_at"] is not None
        assert abs((att["due_at"] - claim_scheduled).total_seconds()) < 5.0, (
            f"due_at {att['due_at']} is not the claim-time scheduled_at {claim_scheduled}"
        )

    async def test_dangling_holder_records_null_worker_id_not_crash(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A reclaim whose holder's workers row is gone (the stale-worker
        window beat the lease) records NULL worker_id instead of raising."""
        schema = module_pg_schema.schema_name
        ghost = new_uuid()  # never inserted into workers
        jid = new_uuid()
        await _seed_running(clean_pg_conn, schema, jid, ghost)

        n = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
        assert n == 1, "the dangling-holder row must still be reclaimed"
        att = await clean_pg_conn.fetchrow(
            f'SELECT worker_id FROM "{schema}".job_attempts WHERE job_id = $1', jid
        )
        assert att is not None
        assert att["worker_id"] is None
