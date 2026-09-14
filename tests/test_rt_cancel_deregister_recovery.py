"""Recovery and TOCTOU-gap attacks on the bounded force-deregistration.

``deregister_actor(force=True)`` drains the actor's pending/scheduled
backlog as bounded committed batches and only then finalizes (disable
schedules, delete ``actor_config``, count, purge).  Two properties no
pinned test exercises:

* **Mid-drain failure recovery** — a non-deadlock statement failure on
  batch 2: the call RAISES (no partial-success result), batch 1's
  cancels and events stay committed, batch 2 rolls back in full, the
  ``actor_config`` row SURVIVES (finalize never ran), and a re-run
  cancels exactly the remainder (EPQ re-selection skips the already
  cancelled), deletes the config, and writes exactly-once events per
  job across both runs.
* **The check→drain gap** — a job claimed ``pending→running`` (the
  dispatch claim shape) on a real second connection between the
  running-check and the drain: it stays ``running`` — not cancelled,
  no events — while the rest of the actor drains and the config is
  deleted.  This is the TOCTOU the docstring documents verbatim; the
  test pins it so a rewrite cannot silently "fix" it into a
  running-job cancellation the safety check explicitly refuses to do.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import deregister_actor
from taskq.backend._sql_templates import render
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

pytestmark = pytest.mark.integration


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    actor: str,
    status: str = "pending",
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        f"SELECT id, $2, 'default', '{{}}'::jsonb, $3::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        actor,
        status,
    )


async def _actor_config_exists(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
) -> bool:
    return bool(
        await conn.fetchval(
            f'SELECT 1 FROM "{schema}".actor_config WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
            actor,
        )
    )


class _InstrumentedConn:
    """Delegates to a real connection with two injection points.

    * ``fail_event_insert_at`` — raise before the Nth batched event
      INSERT (N counts the deregister drain's one INSERT per batch), a
      statement-level failure arriving after that batch's driving
      UPDATE already mutated its rows inside the open transaction.
    * ``gate_first_driving_fetch`` — hold the FIRST driving cancel
      fetch open (after the running-check has executed, before any
      cancel UPDATE runs), which is exactly the check→drain window the
      TOCTOU docstring describes.
    """

    def __init__(
        self,
        conn: Any,
        *,
        fail_event_insert_at: int | None = None,
        gate_first_driving_fetch: bool = False,
    ) -> None:
        self._conn = conn
        self._fail_at = fail_event_insert_at
        self._gate_driving = gate_first_driving_fetch
        self.event_inserts = 0
        self.gated = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def transaction(self, **kwargs: object) -> Any:
        outer = self

        @asynccontextmanager
        async def _tx() -> AsyncGenerator[None]:
            async with outer._conn.transaction(**kwargs):
                yield

        return _tx()

    async def fetch(self, sql: str, *args: object) -> Any:
        return await self._conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> Any:
        # The driving cancel statement is the one whose row aggregates
        # carry prev_status — gated here, before any cancel UPDATE runs.
        if self._gate_driving and not self.gated and "prev_status" in sql:
            self.gated = True
            self.entered.set()
            await self.release.wait()
        return await self._conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> Any:
        return await self._conn.fetchval(sql, *args)

    async def execute(self, sql: str, *args: object) -> Any:
        upper = sql.lstrip().upper()
        if upper.startswith("INSERT INTO") and ".JOB_EVENTS" in upper:
            self.event_inserts += 1
            if self._fail_at is not None and self.event_inserts == self._fail_at:
                raise RuntimeError("injected statement failure")
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


async def test_mid_drain_failure_leaves_batch1_committed_config_intact_and_rerun_completes(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A statement error on batch 2 of a 250-job force deregistration:
    batch 1's 100 cancels committed, actor_config NOT deleted (finalize
    never ran), the error propagates; a re-run cancels the remaining 150
    exactly once and deletes the config."""
    schema = module_pg_schema.schema_name
    render(schema)
    actor = "rt_recover_actor"
    job_ids = [new_uuid() for _ in range(250)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, actor=actor)
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor=actor, max_concurrent=5, queue="default")],
        schema=schema,
    )
    batch1, rest = set(sorted(job_ids)[:100]), set(sorted(job_ids)[100:])

    # Batch 1's drain writes event INSERT #1; the failure lands on
    # batch 2's (#2), after batch 2's driving UPDATE already ran.
    conn = _InstrumentedConn(clean_pg_conn, fail_event_insert_at=2)
    with pytest.raises(RuntimeError, match="injected statement failure"):
        await deregister_actor(conn, actor, force=True, schema=schema, batch_size=100)

    # Batch 1: committed, with its events.
    b1 = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        list(batch1),
    )
    assert {r["status"] for r in b1} == {"cancelled"}, "batch 1 must stay committed"
    b1_events: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        list(batch1),
    )
    assert b1_events == 100, "exactly one state_change per batch-1 job"

    # Batch 2+3: fully rolled back — still pending, no event fragments.
    rest_rows = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        list(rest),
    )
    assert {r["status"] for r in rest_rows} == {"pending"}
    rest_events: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        list(rest),
    )
    assert rest_events == 0

    # The finalize never ran: the config row survives the failed drain.
    assert await _actor_config_exists(clean_pg_conn, schema, actor), (
        "a mid-drain failure must not delete actor_config — the actor would be "
        "deregistered with 150 jobs still pending and uncancelled"
    )

    # Re-run: drains the remainder, deletes the config, exactly-once
    # events per job across both runs.
    result = await deregister_actor(clean_pg_conn, actor, force=True, schema=schema, batch_size=100)
    assert result.jobs_cancelled == 150, "the re-run must cancel exactly the remainder"
    assert result.actor_config_deleted is True
    assert not await _actor_config_exists(clean_pg_conn, schema, actor)

    statuses = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert {r["status"] for r in statuses} == {"cancelled"}

    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "GROUP BY job_id, kind"
    )
    assert {r["kind"] for r in events} == {"state_change"}
    assert all(r["n"] == 1 for r in events), (
        "across the failed run and its re-run, every job must carry exactly one "
        "state_change — the EPQ re-selection must cancel nothing twice"
    )
    assert len(events) == 250


async def test_job_claimed_between_running_check_and_drain_stays_running(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The check→drain gap: a job claimed running after the running-check
    returned zero is skipped by the drain's status predicates — it stays
    running with no events while its 4 peers drain and the config is
    deleted (the documented TOCTOU, not a silent running-job cancel)."""
    schema = module_pg_schema.schema_name
    render(schema)
    actor = "rt_gap_actor"
    job_ids = [new_uuid() for _ in range(5)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, actor=actor)
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor=actor, max_concurrent=5, queue="default")],
        schema=schema,
    )
    claim_target = sorted(job_ids)[2]

    conn = _InstrumentedConn(clean_pg_conn, gate_first_driving_fetch=True)
    task = asyncio.create_task(
        deregister_actor(conn, actor, force=True, schema=schema, batch_size=100)
    )
    await conn.entered.wait()

    # The running-check has already returned zero; a dispatcher claims
    # one of the five pending jobs inside the window before the drain's
    # first cancel UPDATE executes.
    claimer = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        claim_tag = await claimer.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
            "SET status = 'running', locked_by_worker = $1, started_at = clock_timestamp() "
            "WHERE id = $2 AND status = 'pending'",
            new_uuid(),
            claim_target,
        )
        assert claim_tag == "UPDATE 1", "the claim must take a still-pending row"
    finally:
        await claimer.close()
    conn.release.set()
    result = await asyncio.wait_for(task, timeout=30)

    assert result.jobs_cancelled == 4, "the drain skips the claimed row, not the batch"
    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining == 4, "the claimed job is running, not terminal"
    assert result.schedules_disabled == 0
    assert result.queue_purged is False

    claimed = await clean_pg_conn.fetchrow(
        f"SELECT status::text AS status, finished_at, cancel_phase "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = $1',
        claim_target,
    )
    assert claimed is not None
    assert claimed["status"] == "running", (
        "a job claimed between the running-check and the drain must stay running — "
        "cancelling it would be the running-job cancellation the check exists to refuse"
    )
    assert claimed["finished_at"] is None
    assert claimed["cancel_phase"] == 0

    claimed_events: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        claim_target,
    )
    assert claimed_events == 0, "no events may describe a job the drain never touched"

    peers = await clean_pg_conn.fetch(
        f'SELECT id, status::text AS status FROM "{schema}".jobs WHERE id <> $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        claim_target,
    )
    assert {r["status"] for r in peers} == {"cancelled"}
    events = await clean_pg_conn.fetch(
        f'SELECT job_id, detail FROM "{schema}".job_events',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert len(events) == 4
    for row in events:
        assert parse_detail(row["detail"]) == {
            "from_state": "pending",
            "to_state": "cancelled",
            "reason": "actor_deregistered",
        }
    assert not await _actor_config_exists(clean_pg_conn, schema, actor)
