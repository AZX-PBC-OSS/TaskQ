"""Differential parity pins: the read seams' archive-tier fallback.

The #314 class of liability: the twin's read seams used to answer from
the hot tier only, while production's reads fall back to the archive
tier on a hot miss. A test written against the twin saw a DIFFERENT
world than production for the same scenario: an archived id answered
``None`` (twin) where production answered ``archived=True``, and an
archived job's attempt history answered ``[]`` where production answers
the moved history. A documented divergence is still a divergence: tests
written against the twin give false confidence, so the twin now mirrors
production and these pins hold it there.

The shape is ``test_batch_cap_refusals_parity``'s: drive BOTH
implementations through the identical scenario with matching seeded
state and assert equivalent observable outcomes. The PG half runs the
real read functions (``taskq.backend._reads``) over a fake pool/conn
modeling the seeded world (the repo's test-double convention, the
context-manager half of ``acquire()``); the twin half seeds a real
``InMemoryBackend`` and reaches the archive state the way production
reaches it, through the prune simulation, never by hand-writing archive
rows. Reverting the twin's fallback fails the twin half of a pin while
the PG half stays green, and vice versa: that asymmetry is the proof
the pin tests both, not either.

The events cascade's PG half is a server FK behavior, so it runs
integration-gated against a real schema (``TestArchiveEventsCascade``);
its twin half runs always.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import AttemptRow, EnqueueArgs, JobId, JobRow
from taskq.backend._reads import _get as _pg_get
from taskq.backend._reads import _get_attempts as _pg_get_attempts
from taskq.backend._sql_templates import render as render_sql
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._leader_shared import prune_terminal_jobs

from .test_enqueue_coverage import _full_record, _Record

_SCHEMA_LABEL = "taskq"
_SQL = render_sql(_SCHEMA_LABEL)
_START = datetime(2025, 1, 1, tzinfo=UTC)


# ── PG test doubles ────────────────────────────────────────────────────


class _FakeReadPool:
    """Stand-in pool modeling only the context-manager half of
    ``acquire()`` (the documented test-double shape ``_bounded_checkout``
    supports); every checkout yields the one fake conn."""

    def __init__(self, conn: _FakeReadConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeReadPool:
        return self

    async def __aenter__(self) -> _FakeReadConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeReadConn:
    """asyncpg.Connection stand-in routing ``fetchrow``/``fetch`` by SQL
    substring. Probes are logged so a pin can assert WHICH tier a read
    consulted, not only what it answered."""

    def __init__(
        self,
        *,
        fetchrow_map: dict[str, object | _Record | None] | None = None,
        fetch_map: dict[str, list[_Record]] | None = None,
    ) -> None:
        self._fetchrow_map = fetchrow_map or {}
        self._fetch_map = fetch_map or {}
        self.probes: list[str] = []

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        self.probes.append(sql)
        for pattern, result in self._fetchrow_map.items():
            if pattern in sql:
                return result
        return None

    async def fetch(self, sql: str, *args: object) -> list[_Record]:
        self.probes.append(sql)
        for pattern, result in self._fetch_map.items():
            if pattern in sql:
                return result
        return []


def _archived_record(job_id: JobId) -> _Record:
    """A jobs_archive-shaped record for a terminal succeeded job (every
    column ``_job_row_from_record`` reads)."""
    return _Record(
        {
            **_full_record(job_id=job_id),
            "status": "succeeded",
            "finished_at": _START - timedelta(days=31),
        }
    )


def _attempt_records(job_id: JobId) -> list[_Record]:
    started = datetime(2025, 5, 31, tzinfo=UTC)
    return [
        _Record(
            {
                "job_id": job_id,
                "attempt": 1,
                "started_at": started,
                "finished_at": started,
                "outcome": "succeeded",
                "error_class": None,
                "error_message": None,
                "error_traceback": None,
                "duration_ms": 1000,
                "worker_id": new_uuid(),
                "metadata": {},
            }
        )
    ]


# ── Twin scenario seeding ──────────────────────────────────────────────


def _enqueue_args() -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"v": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )


def _memory_backend() -> InMemoryBackend:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    backend.register_actor_config(actor="test_actor")
    return backend


async def _seed_archived_job(
    backend: InMemoryBackend,
    *,
    with_attempts: bool = False,
) -> JobId:
    """One terminal succeeded job past any retention, moved to the
    archive by the prune simulation, production's route to the archive
    tier, never a hand-written archive row. The row reaches its terminal
    status through the real claim and terminal-write seams (a direct
    status rewrite would bypass the state machine and accrue no
    narration, the exact events the cascade pin's scenario is about);
    finished_at is then backdated past the retention cutoff the way the
    archive pins backdate theirs (the scenario targets the prune, not
    the clock)."""
    enqueued = await backend.enqueue(_enqueue_args())
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]
    claimed = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert [j.id for j in claimed] == [enqueued.id], (
        "the scenario needs the seeded job claimed, its narration accrues "
        "through the real transitions"
    )
    marked = await backend.mark_succeeded(
        enqueued.id,
        worker_id,
        {"v": 1},
        attempt=claimed[0].attempt,
        claim_epoch=claimed[0].claim_epoch,
    )
    assert marked is True, "the scenario needs the claimed job terminal"
    ago_31d = _START - timedelta(days=31)
    row = backend._jobs[enqueued.id]  # pyright: ignore[reportPrivateUsage]
    backend._jobs[enqueued.id] = replace(row, finished_at=ago_31d)  # pyright: ignore[reportPrivateUsage]
    if with_attempts:
        # The consumer writes the attempt row through the real seam
        # (mark_succeeded writes none), then the pin backdates it with
        # the job's own finished_at.
        await backend.write_attempt(
            AttemptRow(
                job_id=enqueued.id,
                attempt=claimed[0].attempt,
                started_at=_START,
                finished_at=_START,
                outcome="succeeded",
                error_class=None,
                error_message=None,
                error_traceback=None,
                duration_ms=1000,
                worker_id=worker_id,
                metadata={},
            )
        )
        attempts = await backend.get_attempts(enqueued.id)
        backend._attempts[enqueued.id] = [  # pyright: ignore[reportPrivateUsage]
            replace(attempts[0], started_at=ago_31d, finished_at=ago_31d)
        ]
    result = backend.archive_terminal_jobs(
        retention=timedelta(days=30),
        archive_retention=timedelta(days=365),
    )
    assert result.archived == 1, (
        "the prune simulation archived nothing; the scenario requires the "
        "seeded job to have left the hot tier"
    )
    return enqueued.id


def _row_facts(row: JobRow | None) -> tuple[bool, str, bool]:
    """The observable facts a pin asserts, twin and PG alike: answered
    at all, terminal status, and which tier the answer came from."""
    assert row is not None
    return (True, row.status, row.archived)


# ── get: the #314 fallback, both backends ──────────────────────────────


class TestGetArchiveFallbackParity:
    async def test_archived_id_answers_on_both(self) -> None:
        """The #314 scenario, both backends: an id whose row the prune
        moved to the archive answers with its terminal status and
        ``archived=True`` instead of reading as missing."""
        job_id = new_job_id()
        pg_conn = _FakeReadConn(
            fetchrow_map={
                # hot tier: the prune deleted the row
                'FROM "taskq".jobs WHERE': None,
                # archive tier: the prune's row
                'FROM "taskq".jobs_archive': _archived_record(job_id),
            }
        )

        pg_row = await _pg_get(_FakeReadPool(pg_conn), _SQL, job_id)
        mem_backend = _memory_backend()
        mem_id = await _seed_archived_job(mem_backend)
        mem_row = await mem_backend.get(mem_id)

        assert _row_facts(pg_row) == _row_facts(mem_row) == (True, "succeeded", True)

    async def test_archived_id_reads_the_archive_tier_not_the_hot_one(self) -> None:
        """The fallback is a real second probe, not a cached miss: the PG
        read consults the hot tier first and the archive tier on the
        miss, in that order, the jobs-then-archive shape the CLI applies."""
        job_id = new_job_id()
        pg_conn = _FakeReadConn(
            fetchrow_map={
                'FROM "taskq".jobs WHERE': None,
                'FROM "taskq".jobs_archive': _archived_record(job_id),
            }
        )
        await _pg_get(_FakeReadPool(pg_conn), _SQL, job_id)

        assert len(pg_conn.probes) == 2
        assert 'FROM "taskq".jobs WHERE' in pg_conn.probes[0]
        assert 'FROM "taskq".jobs_archive' in pg_conn.probes[1]

        # The twin's observable half of the same shape: the answer comes
        # from the archive storage, so get_archived finds the same id.
        mem_backend = _memory_backend()
        mem_id = await _seed_archived_job(mem_backend)
        assert await mem_backend.get_archived(mem_id) is not None
        mem_row = await mem_backend.get(mem_id)
        assert mem_row is not None
        assert mem_row.archived is True

    async def test_hot_hit_is_not_marked_archived_on_both(self) -> None:
        """The marker tracks the tier, not age: a hot row reads False on
        both backends."""
        pg_conn = _FakeReadConn(fetchrow_map={'FROM "taskq".jobs WHERE': _Record(_full_record())})

        pg_row = await _pg_get(_FakeReadPool(pg_conn), _SQL, JobId(_full_record()["id"]))
        mem_backend = _memory_backend()
        enqueued = await mem_backend.enqueue(_enqueue_args())
        mem_row = await mem_backend.get(enqueued.id)

        assert _row_facts(pg_row) == _row_facts(mem_row) == (True, "pending", False)

    async def test_never_existed_id_answers_none_on_both(self) -> None:
        """The fallback must not turn 'no such job' into a finding: an id
        in neither tier is ``None`` on both backends."""
        pg_conn = _FakeReadConn(
            fetchrow_map={
                'FROM "taskq".jobs WHERE': None,
                'FROM "taskq".jobs_archive': None,
            }
        )

        missing = new_job_id()
        pg_row = await _pg_get(_FakeReadPool(pg_conn), _SQL, missing)
        mem_backend = _memory_backend()
        mem_row = await mem_backend.get(missing)

        assert pg_row is None
        assert mem_row is None
        assert len(pg_conn.probes) == 2, "both tiers were probed before answering None"


# ── get_attempts: the moved history, both backends ────────────────────


class TestGetAttemptsArchiveFallbackParity:
    async def test_archived_history_answers_on_both(self) -> None:
        """An archived job's attempt history answers from the archive
        tier on both backends: the row claims attempt=1, the history must
        corroborate it, not claim nothing ran."""
        job_id = new_job_id()
        pg_conn = _FakeReadConn(
            fetchrow_map={'FROM "taskq".job_attempts WHERE': None},
            fetch_map={'FROM "taskq".job_attempts_archive': _attempt_records(job_id)},
        )

        pg_attempts = await _pg_get_attempts(_FakeReadPool(pg_conn), _SQL, job_id)
        mem_backend = _memory_backend()
        mem_id = await _seed_archived_job(mem_backend, with_attempts=True)
        mem_attempts = await mem_backend.get_attempts(mem_id)

        assert [(a.attempt, a.outcome) for a in pg_attempts] == [(1, "succeeded")]
        assert [(a.attempt, a.outcome) for a in mem_attempts] == [(1, "succeeded")]

    async def test_hot_history_answers_without_touching_the_archive_on_both(self) -> None:
        """A live job's history answers from the hot tier on both
        backends; the archive tier is not consulted, and changes no live
        answer."""
        job_id = new_job_id()
        hot_conn = _FakeReadConn(
            fetch_map={'FROM "taskq".job_attempts WHERE': _attempt_records(job_id)},
        )

        pg_attempts = await _pg_get_attempts(_FakeReadPool(hot_conn), _SQL, job_id)
        assert [(a.attempt, a.outcome) for a in pg_attempts] == [(1, "succeeded")]
        assert hot_conn.probes and "archive" not in hot_conn.probes[0]

        mem_backend = _memory_backend()
        enqueued = await mem_backend.enqueue(_enqueue_args())
        mem_backend._attempts[enqueued.id] = [  # pyright: ignore[reportPrivateUsage]
            AttemptRow(
                job_id=enqueued.id,
                attempt=1,
                started_at=_START,
                finished_at=_START,
                outcome="succeeded",
                error_class=None,
                error_message=None,
                error_traceback=None,
                duration_ms=1000,
                worker_id=None,
                metadata={},
            )
        ]
        mem_attempts = await mem_backend.get_attempts(enqueued.id)
        assert [(a.attempt, a.outcome) for a in mem_attempts] == [(1, "succeeded")]


# ── the prune's event cascade ──────────────────────────────────────────


class TestArchiveEventsCascade:
    async def test_archived_jobs_events_do_not_answer_on_the_twin(self) -> None:
        """Production's prune deletes the archived jobs' job_events rows
        (the job_id FK is ON DELETE CASCADE and no events archive
        exists), so ``get_events`` for an archived id answers empty on
        PG. The twin's prune simulation must leave the same world: a job
        whose narration outlived it on the twin only would make every
        twin test pass over a PG answer it never models. The PG half of
        this pin is the integration test below.
        """
        mem_backend = _memory_backend()
        archived = await _seed_archived_job(mem_backend)

        events = await mem_backend.get_events(archived)
        assert events == [], (
            "the archived job's events survived the prune on the twin; "
            "production's cascade deletes them, the twin over-answers"
        )

    async def test_live_jobs_events_still_answer_on_the_twin(self) -> None:
        """The cascade is the prune's, not a blanket event wipe: a job
        the prune kept keeps its own narration. The kept job's event is
        written through the real cancel seam (a pending cancel emits the
        state_change row), then the prune runs beside it."""
        mem_backend = _memory_backend()
        kept = await mem_backend.enqueue(_enqueue_args())
        cancelled = await mem_backend.write_cancel_request(kept.id, "parity pin")
        assert cancelled is True, "the scenario needs one kept job holding an event"

        await _seed_archived_job(mem_backend)
        events = await mem_backend.get_events(kept.id)
        assert len(events) >= 1, "the kept job's own narration must survive the prune"


@pytest.mark.integration
class TestArchiveEventsCascadeIntegration:
    """The cascade half of the events pin, against a real schema: the
    prune's DELETE FROM jobs must leave ``get_events`` empty for the
    archived id, the world the twin half above is held to."""

    async def test_archived_jobs_events_do_not_answer_on_pg(
        self,
        clean_jobs_app: JobsApp,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        _deps, backend = clean_jobs_app
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            job_id = await _seed_terminal_job(conn, module_pg_schema.schema_name)
            await _seed_event(conn, module_pg_schema.schema_name, job_id)
            await _prune_into_archive(conn, module_pg_schema.schema_name, job_id)
        finally:
            await conn.close()

        events = await backend.get_events(job_id)
        assert events == [], (
            "the archived job's events survived the prune on PG; the "
            "cascade the twin's prune simulation mirrors is gone"
        )


# ── PG integration seeding (the #314 file's scenario shape) ───────────


async def _seed_terminal_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str = "succeeded",
    job_id: JobId | None = None,
) -> JobId:
    """One terminal job already past any retention, ready for the prune."""
    jid = job_id if job_id is not None else new_job_id()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            finished_at, metadata, payload_schema_ver, attempt
        ) VALUES (
            $1, 'test_actor', 'default', '{{"v": 1}}'::jsonb, 3, 'transient',
            $2::{schema}.job_status, 0, $3, $4,
            $5, '{{}}'::jsonb, 1, 0
        )""",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        jid,
        status,
        now,
        now + timedelta(hours=1),
        now - timedelta(minutes=5),
    )
    return jid


async def _seed_event(
    conn: asyncpg.Connection,
    schema: str,
    job_id: JobId,
) -> None:
    """One narration row for *job_id*, the shape production's cascade
    deletes with the prune's DELETE FROM jobs."""
    await conn.execute(
        f"""INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
        VALUES ($1, $2, 'state_change',
        '{{"from_state": "pending", "to_state": "pending"}}'::jsonb)""",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        job_id,
        datetime.now(UTC),
    )


async def _prune_into_archive(
    conn: asyncpg.Connection,
    schema: str,
    job_id: JobId,
) -> None:
    """A real prune: the issue's table state is 'the row the prune left
    behind', so the test produces it the way production does."""
    from taskq.backend.statemachine import TERMINAL_STATUSES

    result = await prune_terminal_jobs(
        conn,
        retention_per_status={status: timedelta(0) for status in TERMINAL_STATUSES},
        archive_retention=timedelta(days=365),
        schema=schema,
    )
    assert result.archived >= 1, (
        f"the prune archived nothing ({result!r}); the scenario requires the "
        "seeded job to have been moved to jobs_archive"
    )
    remaining = await conn.fetchval(
        f"SELECT count(*) FROM {schema}.jobs WHERE id = $1",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        job_id,
    )
    assert remaining == 0, "the seeded job must have left the hot table"
