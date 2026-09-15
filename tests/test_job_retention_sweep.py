# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Red-team pins for age-based ``job_events`` retention.

``job_events`` has 16 INSERT sites and no DELETE anywhere in the package;
its only exit is the ``ON DELETE CASCADE`` when the leader prune removes a
terminal parent job. Two consequences, both confirmed:

* Events of a job that never terminates (the snooze loop re-nulls
  ``finished_at`` every cycle, so the terminality-keyed prune can never
  match it) live forever.
* Even for terminal jobs, event volume is bounded by job retention
  (30-90 days), not by anything proportional to what operators read.

The attached design (``docs/design/sql-hotpath-followups.md`` §2) settles
the shape: a leader-gated, batched ``DELETE`` by ``occurred_at`` —
``sweep_expired_events()`` in ``taskq/backend/_sweeps.py``, mirroring
``sweep_expired_results`` — driven by a worker-configurable
``event_retention_period`` / ``event_retention_batch_size`` under the
``TASKQ_`` prefix.

One carve-out is a hard constraint, not a preference: the
``kind='state_change' AND detail->>'reason' = 'lock_expired'`` slice of
``job_events`` is the crash-reclaim outbox that ``poll_reclaim_events``
and ``TaskQ.watch_reclaims()`` consume under a trailing-watermark
protocol. An age sweep that deletes that slice races the watermark and
silently corrupts crash reclamation — the failure this project's
bounded-writes rule was written about. The sweep must leave it alone.

These tests target the seam the design names. They fail today because no
such sweep or setting exists.
"""

import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_OLD = datetime.now(UTC) - timedelta(days=60)
_YOUNG = datetime.now(UTC) - timedelta(days=1)

# Settings-only assertions need a syntactically valid DSN and never connect.
_DUMMY_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


_SweepFn = Callable[..., Coroutine[Any, Any, int]]


def _event_retention_sweep() -> _SweepFn:
    """The designed retention seam, or a descriptive failure.

    The design attached to the RCA names ``sweep_expired_events`` in
    ``taskq/backend/_sweeps.py``. If the fix lands the capability under a
    different name, update this driver — the assertions below are the
    contract, not the spelling.
    """
    from taskq.backend import _sweeps

    sweep = getattr(_sweeps, "sweep_expired_events", None)
    if sweep is None:
        pytest.fail(
            "no age-based job_events retention sweep exists. job_events has "
            "16 INSERT sites and zero DELETEs; its only exit is the ON "
            "DELETE CASCADE from a terminal parent, so events of a "
            "never-terminal job live forever. The attached design "
            "(docs/design/sql-hotpath-followups.md §2) names the seam: "
            "sweep_expired_events() in taskq/backend/_sweeps.py, a bounded "
            "batched DELETE by occurred_at, leader-gated per sweep tick."
        )
    return sweep


def test_event_retention_is_worker_configurable() -> None:
    """The retention window and batch size are worker settings under the
    ``TASKQ_`` prefix — the operator directive is a *configurable* sweep,
    not a hard-coded one."""
    missing = [
        name
        for name in ("event_retention_period", "event_retention_batch_size")
        # WorkerSettings is a dotenvmodel DotEnvConfig, not a pydantic
        # BaseModel: get_fields() is its introspection seam, mapping
        # name -> (type, FieldInfo).
        if name not in WorkerSettings.get_fields()
    ]
    assert not missing, (
        f"WorkerSettings has no {missing} — the retention sweep must be "
        "configurable via DotEnvConfig under the TASKQ_ prefix (the design "
        "settles TASKQ_EVENT_RETENTION_PERIOD, default ~7 days, 0 disables; "
        "and a batch size bounded like the other sweeps). "
        "grep -rn 'event_retention' src/taskq/settings.py finds nothing."
    )


def test_event_retention_defaults_to_seven_days_out_of_the_box() -> None:
    """An operator who sets nothing gets a seven-day ``job_events`` window.

    The default is the product decision, not an implementation detail: it
    is what bounds event growth on every deployment that never reads the
    setting's documentation. Seven days is deliberately far shorter than
    the 30-90 day job-retention window, because events are narration
    (the durable forensic record is jobs/job_attempts and their archives),
    and because the pre-existing cascade-only regime could never reclaim
    the events of a job that never terminates at all.

    A silent drift of this default — to something long enough that the
    table still grows without practical bound, or short enough to erase a
    week of operator-visible timeline — changes shipped behaviour for
    every deployment at once, so it is pinned by value rather than by
    reference to the constant.
    """
    settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DUMMY_DSN}, validate=False)
    assert settings.event_retention_period == timedelta(days=7), (
        "the shipped job_events retention default is "
        f"{settings.event_retention_period!r}, not 7 days. The default is what "
        "bounds job_events on every deployment that never touches the "
        "setting; changing it changes behaviour for all of them."
    )


def test_zero_event_retention_disables_the_sweep_rather_than_deleting_everything() -> None:
    """``TASKQ_EVENT_RETENTION_PERIOD=0`` turns the sweep off.

    Zero reads two opposite ways for an age-bounded DELETE: "keep nothing
    older than now" (delete every event in the table) or "no window
    configured" (do not sweep). For a deletion loop the safe reading of a
    misconfiguration is off, and that is the one this project ships — the
    inverse of the prune family's zero-means-archive-immediately, so the
    inversion is worth pinning explicitly.

    The two halves belong together: the settings layer accepts zero and
    carries it as the disable sentinel, while the sweep function itself
    refuses it, because at the function boundary zero has no safe meaning
    and a caller passing it through is a wiring bug that would otherwise
    empty the table.
    """
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": _DUMMY_DSN, "TASKQ_EVENT_RETENTION_PERIOD": "0"},
        validate=False,
    )
    assert settings.event_retention_period == timedelta(0), (
        "TASKQ_EVENT_RETENTION_PERIOD=0 must load as timedelta(0), the documented disable sentinel"
    )

    sweep = _event_retention_sweep()
    with pytest.raises(ValueError, match="positive"):
        # A conn is never reached: the guard must fire on the argument.
        asyncio.run(sweep(cast("Any", None), schema="taskq", retention=timedelta(0), batch_size=10))


def test_negative_event_retention_is_rejected_at_settings_load() -> None:
    """A negative retention window is an operator typo with no coherent
    meaning, and it is rejected where the operator can still see it — at
    settings load, on the worker's own boot path — rather than surfacing
    later as an inexplicable sweep error on a leader tick."""
    with pytest.raises(Exception, match=r"(?i)negative|greater|positive|invalid"):
        WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": _DUMMY_DSN, "TASKQ_EVENT_RETENTION_PERIOD": "-1d"},
            validate=False,
        )


async def _seed_events(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    *,
    occurred_at: datetime,
    count: int,
    kind: str = "state_change",
    detail: str = "{}",
) -> None:
    for _ in range(count):
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail) '
            "VALUES ($1, $2, $3, $4::jsonb)",
            job_id,
            occurred_at,
            kind,
            detail,
        )


async def _event_count(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    return await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',
        job_id,
    )


async def test_event_retention_reclaims_old_events_of_live_jobs_in_bounded_batches(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Old events are reclaimed even while their parent job is still live
    (non-terminal — the case the cascade can never reach), young events are
    kept, and one call deletes at most one bounded batch."""
    schema = module_pg_schema.schema_name
    sweep = _event_retention_sweep()

    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        with_events=False,
    )

    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_OLD, count=5)
    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_YOUNG, count=1)

    first_pass: int = await sweep(
        clean_pg_conn,
        schema=schema,
        retention=timedelta(days=30),
        batch_size=2,
    )
    assert first_pass <= 2, (
        f"one retention call deleted {first_pass} rows with batch_size=2 — "
        "the sweep must delete in bounded batches (one short transaction "
        "per batch), not drain the whole backlog in one statement."
    )
    assert first_pass > 0

    drained = first_pass
    while drained:
        drained = await sweep(
            clean_pg_conn,
            schema=schema,
            retention=timedelta(days=30),
            batch_size=2,
        )

    remaining = await _event_count(clean_pg_conn, schema, job_id)
    assert remaining == 1, (
        f"expected only the young event to survive, found {remaining} rows. "
        "The age sweep must reclaim every event older than the retention "
        "window — including those of a job that is still 'running', whose "
        "rows the terminality-keyed prune can never reach."
    )


async def test_event_retention_preserves_the_reclaim_outbox_slice(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``kind='state_change' AND reason='lock_expired'`` events are the
    crash-reclaim outbox (``poll_reclaim_events`` /
    ``TaskQ.watch_reclaims()``). However old they are, the age sweep must
    not delete them — racing the trailing watermark silently corrupts
    crash reclamation."""
    schema = module_pg_schema.schema_name
    sweep = _event_retention_sweep()

    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        with_events=False,
    )

    await _seed_events(
        clean_pg_conn,
        schema,
        job_id,
        occurred_at=_OLD,
        count=2,
        kind="state_change",
        detail='{"reason": "lock_expired"}',
    )
    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_OLD, count=3)

    # Drain fully — several bounded passes.
    while await sweep(
        clean_pg_conn,
        schema=schema,
        retention=timedelta(days=30),
        batch_size=100,
    ):
        pass

    remaining = await _event_count(clean_pg_conn, schema, job_id)
    assert remaining == 2, (
        f"expected the 2 reclaim-outbox events to survive, found {remaining} "
        "rows. The lock_expired slice of job_events is machine-read by the "
        "crash-reclaim outbox under a trailing-watermark protocol; an age "
        "sweep must carve it out explicitly, whatever happens to every "
        "other kind."
    )


# ── churn stays diagnosable after the event timeline is reclaimed ──────


async def test_attempt_and_retry_counters_survive_event_retention(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Event retention reclaims the timeline; it must not reclaim the
    counters on the job row that make churn diagnosable.

    A job cycling through retries and admission denials is exactly the
    job whose event history ages out first, because it produces the most
    events. If the age sweep can reach the row's own accounting —
    ``attempt``, ``max_attempts``, ``snooze_count``,
    ``rate_limit_blocked_count`` — then the loudest symptom of a
    misconfigured queue disappears precisely on the jobs that exhibit it
    most, and the operator is left with a live job and no way to tell a
    job on its first attempt from one that has been churning for a week.
    """
    schema = module_pg_schema.schema_name
    sweep = _event_retention_sweep()

    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        attempt=4,
        max_attempts=9,
        with_events=False,
    )
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET snooze_count = 7, rate_limit_blocked_count = 22 WHERE id = $1',
        job_id,
    )
    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_OLD, count=6)

    while await sweep(
        clean_pg_conn,
        schema=schema,
        retention=timedelta(days=30),
        batch_size=2,
    ):
        pass

    assert await _event_count(clean_pg_conn, schema, job_id) == 0, (
        "test premise: the churning job's aged events must actually be reclaimed, "
        "or this test proves nothing about what survives the sweep"
    )
    row = await clean_pg_conn.fetchrow(
        f"SELECT status, attempt, max_attempts, snooze_count, rate_limit_blocked_count "
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert row is not None, (
        "event retention deleted the job row itself — a live running job vanished "
        "from the jobs table because its events aged out"
    )
    assert (
        row["status"],
        row["attempt"],
        row["max_attempts"],
        row["snooze_count"],
        row["rate_limit_blocked_count"],
    ) == ("running", 4, 9, 7, 22), (
        "the job row's churn accounting changed when its event timeline was "
        f"reclaimed; row is now {dict(row)!r}. Attempt, retry budget, snooze and "
        "aggregated denial counts are the only remaining evidence of churn once "
        "the per-event rows are gone, and they live on the row for exactly that "
        "reason"
    )


async def test_event_retention_touches_no_attempt_audit_rows(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``job_attempts`` is a separate audit surface with its own lifetime.

    The two tables are both per-job history, and a retention sweep that
    reached across would silently take the attempt audit with it. Attempt
    rows are what answers "how did this job fail the last six times" long
    after the event stream has been trimmed, so an operator investigating
    churn on an old job needs them to outlive the event window.
    """
    schema = module_pg_schema.schema_name
    sweep = _event_retention_sweep()

    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        attempt=3,
        with_events=False,
    )
    for attempt_no in (1, 2):
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".job_attempts '
            "(job_id, attempt, worker_id, started_at, finished_at, outcome, error_class) "
            "VALUES ($1, $2, $3, $4, $4, 'failed', 'ValueError')",
            job_id,
            attempt_no,
            worker_id,
            _OLD,
        )
    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_OLD, count=4)

    while await sweep(
        clean_pg_conn,
        schema=schema,
        retention=timedelta(days=30),
        batch_size=2,
    ):
        pass

    surviving_attempts = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
        job_id,
    )
    assert int(surviving_attempts) == 2, (
        f"the event-retention sweep left {surviving_attempts} of 2 attempt audit "
        "rows. job_attempts is a distinct surface with its own retention; an "
        "event sweep that reaches it destroys the per-attempt failure history "
        "an operator reads when diagnosing churn, and does so invisibly"
    )
