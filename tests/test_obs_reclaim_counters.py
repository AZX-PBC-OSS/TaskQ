"""Tests for the counter ``taskq.jobs.reclaimed``.

Covers the per-actor, per-disposition counter increments from both
backends:
- Unit (in-memory): reclaimed rows across two distinct actors and all
  three dispositions (repended / crashed / cancelled).
- Integration (PG): the same corpus against testcontainers Postgres,
  exercising the ``j.actor`` column added to ``_SWEEP_1_SQL``'s
  ``RETURNING`` and the post-transaction aggregation the sweep records.

Also pins the disposition map's totality, the property the adversarial
review round established is critical: an unmapped post-update status
must not kill the reclaim sweep (it is the fleet's crash-recovery path),
and map/CASE drift must fail CI loudly instead of surfacing as a
dashboard anomaly.

The counter is the per-actor crash split #230 asks the expired-locks
sweep to carry; ``taskq.maintenance_leader.sweep_rows{sweep_name}``
stays the sweep-name total and is pinned separately
(test_rt_worker_sweep_telemetry.py).
"""

from collections import Counter
from dataclasses import replace as dc_replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.otel import counter_data_points

_START = datetime(2026, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)
# The cancel carve-out admits a cancel-in-flight job only once the lock
# has been expired past cancel_grace + cleanup_grace + a flat 60 s, the
# same constant test_rt_sweeps_parity.py derives.
_CANCEL_LOCK_EXPIRED_AGO = timedelta(seconds=130)
_PLAIN_LOCK_EXPIRED_AGO = timedelta(seconds=10)
_STARTED_AGO = timedelta(seconds=30)

# (actor, disposition) pairs the seeded corpus produces: two actors, one
# re-pended row and one crashed row each, plus one cancelled row on the
# first actor (budget exhausted with a cancel in flight: the honest
# terminal label, not a crash).
_EXPECTED: Counter[tuple[str, str]] = Counter(
    {
        ("actor_alpha", "repended"): 1,
        ("actor_alpha", "crashed"): 1,
        ("actor_alpha", "cancelled"): 1,
        ("actor_beta", "repended"): 1,
        ("actor_beta", "crashed"): 1,
    }
)

# (actor, max_attempts, cancel_phase): attempts remain -> re-pended;
# exhausted + no cancel -> crashed; exhausted + cancel in flight ->
# cancelled.
_CORPUS: list[tuple[str, int, int]] = [
    ("actor_alpha", 3, 0),
    ("actor_beta", 3, 0),
    ("actor_alpha", 1, 0),
    ("actor_beta", 1, 0),
    ("actor_alpha", 1, 1),
]


@pytest.fixture
def metric_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the reclaimed-jobs counter.

    Replaces ``taskq.obs._reclaimed_jobs`` with a fresh counter backed by
    ``InMemoryMetricReader``. monkeypatch auto-restores the original.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())

    monkeypatch.setattr(
        otel_mod,
        "_reclaimed_jobs",
        new_meter.create_counter(
            "taskq.jobs.reclaimed",
            description=(
                "Running jobs reclaimed by the expired-locks sweep (the holder broke "
                "its liveness promise - lease expiry or heartbeat timeout), labeled "
                "by actor and disposition: repended (attempts remained, the row went "
                "back to pending on its retry curve), crashed (budget exhausted, "
                "terminal), cancelled (a cancel request was in-flight when the "
                "holder died - the honest terminal label)."
            ),
            unit="1",
        ),
    )

    return reader


def _assert_reclaimed_points(reader: InMemoryMetricReader) -> None:
    """The counter carries exactly the seeded (actor, disposition) pairs."""
    dps = counter_data_points(reader, "taskq.jobs.reclaimed")
    observed: Counter[tuple[str, str]] = Counter()
    for dp in dps:
        attrs = dict(dp.attributes or {})
        observed[(str(attrs["actor"]), str(attrs["disposition"]))] += dp.value
    assert observed == _EXPECTED, (
        f"expected per-(actor, disposition) counts {_EXPECTED}, observed {observed}"
    )
    # The disposition label set is the code-fixed enum, never the raw row
    # status of some future branch: every observed value is one of the
    # three literals the map defines.
    assert {d for _, d in observed} <= {"repended", "crashed", "cancelled"}


# ── In-memory twin ───────────────────────────────────────────────────────


def _make_memory_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


async def _seed_memory_row(
    backend: InMemoryBackend,
    holder: UUID,
    actor: str,
    *,
    max_attempts: int,
    cancel_phase: int,
) -> None:
    """Seed one running row whose lease expired, in the twin's established
    seeding shape (test_rt_sweeps_parity.py)."""
    args = make_enqueue_args(actor=actor, scheduled_at=_START, max_attempts=max_attempts)
    row = await backend.enqueue(args)
    expired_ago = _CANCEL_LOCK_EXPIRED_AGO if cancel_phase else _PLAIN_LOCK_EXPIRED_AGO
    backend._jobs[args.id] = dc_replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access to set up the running-row fixture, the established seeding pattern (test_rt_sweeps_parity.py).
        row,
        status="running",
        attempt=1,
        locked_by_worker=holder,
        lock_expires_at=_START - expired_ago,
        started_at=_START - _STARTED_AGO,
        cancel_phase=CancelPhase(cancel_phase),
        cancel_requested_at=_START if cancel_phase else None,
    )


async def test_in_memory_reclaim_increments_per_actor_and_disposition(
    metric_reader: InMemoryMetricReader,
) -> None:
    """Reclaimed rows across two actors and all three dispositions; the
    twin records one aggregated increment per (actor, disposition) pair."""
    backend = _make_memory_backend()
    holder = new_uuid()
    for actor, max_attempts, cancel_phase in _CORPUS:
        await _seed_memory_row(
            backend, holder, actor, max_attempts=max_attempts, cancel_phase=cancel_phase
        )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 5

    _assert_reclaimed_points(metric_reader)


async def test_in_memory_reclaim_no_rows_no_counter(
    metric_reader: InMemoryMetricReader,
) -> None:
    """When nothing is reclaimed, the counter has no data points."""
    backend = _make_memory_backend()

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 0

    dps = counter_data_points(metric_reader, "taskq.jobs.reclaimed")
    assert len(dps) == 0


# ── Postgres sweep ───────────────────────────────────────────────────────


@pytest.mark.integration
async def test_pg_reclaim_increments_per_actor_and_disposition(
    metric_reader: InMemoryMetricReader,
    pg_dsn: str,
) -> None:
    """The PG sweep records the identical (actor, disposition) split,
    exercising the ``j.actor`` column the RETURNING now carries and the
    post-transaction aggregation over it."""
    import asyncpg

    from taskq.migrate import apply_pending

    schema = f"torc_{new_base62()}".lower()

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        holder = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, rendered only after apply_pending created it.
            "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
            holder,
        )

        ids = [new_uuid() for _ in _CORPUS]
        expires_ago = [
            (_CANCEL_LOCK_EXPIRED_AGO if phase else _PLAIN_LOCK_EXPIRED_AGO).total_seconds()
            for _, _, phase in _CORPUS
        ]
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, rendered only after apply_pending created it.
            "(id, actor, queue, payload, status, max_attempts, retry_kind, attempt, "
            " scheduled_at, locked_by_worker, lock_expires_at, started_at, cancel_phase, "
            " cancel_requested_at) "
            "SELECT t.id, t.actor, 'default', '{}'::jsonb, 'running', "
            "t.max_attempts, 'transient', 1, clock_timestamp(), $2, "
            "clock_timestamp() - (t.expired_ago * interval '1 second'), "
            "clock_timestamp() - interval '30 seconds', t.phase, "
            "CASE WHEN t.phase = 0 THEN NULL ELSE clock_timestamp() END "
            "FROM unnest($1::uuid[], $3::text[], $4::int[], $5::int[], $6::float8[]) "
            "    AS t(id, actor, max_attempts, phase, expired_ago)",
            ids,
            holder,
            [a for a, _, _ in _CORPUS],
            [m for _, m, _ in _CORPUS],
            [p for _, _, p in _CORPUS],
            expires_ago,
        )

        count = await PostgresBackend.sweep_expired_locks(
            conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _GRACE,
            _GRACE,
            schema=schema,
        )
        assert count == 5
    finally:
        await conn.close()

    _assert_reclaimed_points(metric_reader)


# ── Map totality: the sweep must survive drift, and CI must see it ───────
#
# The adversarial review round drove an unmapped post-update status through
# the REAL sweep: a bare ``_RECLAIM_DISPOSITIONS[status]`` raised KeyError
# inside the transaction -> the whole reclaim batch rolled back (no
# job_attempts, no job_events, no wake NOTIFY, no metric) -> the exception
# escaped the NotImplementedError/TRANSIENT_PG_ERRORS guards into
# UnexpectedLoopErrorGuard -> kill-streak after 5 consecutive ticks ->
# crash recovery dead fleet-wide. The twin died the same way, mid-loop,
# leaving a half-drained corpus. The map is total today; these pins keep
# the invariant enforced instead of incidental.


def test_disposition_map_is_exhaustive_over_the_sweep_case_branches() -> None:
    """The map's key set equals the reachable post-update statuses of
    ``_SWEEP_1_SQL``'s status CASE: exactly, in both directions.

    The robustness tests below keep the sweep ALIVE when this pin is
    violated; this test makes the violation itself fail CI loudly, with
    the fix named in the failure message, instead of surfacing as an
    ``unknown`` series on a dashboard. (An ELSE arm counts as a branch:
    the crashed literal is written by the CASE's ELSE, not a THEN.)"""
    import re

    from taskq.backend._sweeps import _RECLAIM_DISPOSITIONS, _SWEEP_1_SQL

    block = re.search(r"SET status = CASE(.*?)END,", _SWEEP_1_SQL, re.DOTALL)
    assert block is not None, (
        "the status CASE block moved in _SWEEP_1_SQL: update this pin's "
        "extraction so it keeps tying _RECLAIM_DISPOSITIONS to the CASE branches"
    )
    case_statuses = set(re.findall(r"'(\w+)'::\"\{schema\}\"\.job_status", block.group(1)))
    assert case_statuses, (
        "no job_status literals found inside the status CASE: the pin's "
        "extraction and the SQL drifted apart"
    )
    assert case_statuses == set(_RECLAIM_DISPOSITIONS), (
        "_RECLAIM_DISPOSITIONS and the sweep CASE's reachable statuses "
        f"drifted: the CASE writes {sorted(case_statuses)}, the map knows "
        f"{sorted(_RECLAIM_DISPOSITIONS)}. add the new status to the map (or "
        "drop the stale entry): an unmapped status costs the sweep its "
        'per-actor split and counts as disposition="unknown" until fixed'
    )


class _FabricatedTx:
    """The sweep transaction's recorder: COMMIT on a clean exit, ROLLBACK
    when the body raised, the verdict the real connection would get."""

    def __init__(self, conn: "_FabricatedReturningConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FabricatedReturningConn":
        return self._conn

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self._conn.tx_outcome = "ROLLBACK" if exc_type is not None else "COMMIT"
        return False


class _FabricatedReturningConn:
    """ConnLike whose sweep fetch returns fabricated RETURNING rows.

    The adversarial review shape: everything else answers like Postgres
    would (the statement_timeout probe, set_config, the batched INSERTs,
    the wake NOTIFY), so the sweep runs its REAL Python path against rows
    the database never wrote: the only way to reach an unmapped
    post-update status while the CASE still writes the mapped three.
    """

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.tx_outcome: str | None = None
        self.executed_sql: list[str] = []

    def transaction(self) -> _FabricatedTx:
        return _FabricatedTx(self)

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        if "current_setting" in sql:
            return [{"current_setting": "0"}]
        return self._rows

    async def execute(self, sql: str, *args: object) -> str:
        self.executed_sql.append(sql)
        return "OK 0"


def _fabricated_row(status: str, actor: str = "actor_alpha") -> dict[str, object]:
    """One sweep-1 RETURNING row exactly as the sweep's row loop reads it."""
    now = datetime(2026, 9, 17, tzinfo=UTC)
    return {
        "id": new_uuid(),
        "status": status,
        "attempt": 1,
        "started_at": now - _STARTED_AGO,
        "actor": actor,
        "locked_by_worker": new_uuid(),
        "reclaim_reason": "lock_expired",
        "now_ts": now,
    }


async def test_pg_reclaim_survives_an_unmapped_returning_status(
    metric_reader: InMemoryMetricReader,
) -> None:
    """A RETURNING row carrying an unmapped post-update status (the
    #238-evolved or migration-era CASE) must not kill the sweep: the
    disposition lookup is total, so the batch completes: attempts, events,
    wake NOTIFY, metric.  The unknown counts as its own series instead
    of a rolled-back transaction escaping into the leader's kill-streak."""
    from taskq.backend._sweeps import sweep_expired_locks

    # One mapped row, one fabricated-unmapped row ('scheduled', a status
    # no current CASE branch writes, standing in for any future one).
    conn = _FabricatedReturningConn([_fabricated_row("pending"), _fabricated_row("scheduled")])

    count = await sweep_expired_locks(
        conn,  # pyright: ignore[reportArgumentType]  # Why: test duck-type connection, the review shape drives the sweep's real Python path with fabricated RETURNING rows.
        _GRACE,
        _GRACE,
        schema="taskq",
    )

    # The sweep completed and its writes escaped the transaction.
    assert count == 2
    assert conn.tx_outcome == "COMMIT", (
        "an unmapped status must not roll the reclaim batch back: the "
        "batch is the fleet's crash-recovery path"
    )
    assert any("pg_notify" in sql for sql in conn.executed_sql), (
        "the wake NOTIFY must fire: consumers of the crash-reclaim outbox "
        "channel lose their low-latency wakeup when the batch rolls back"
    )
    assert any("job_attempts" in sql for sql in conn.executed_sql)
    # The metric counts BOTH rows: the mapped one under its real
    # disposition, the unmapped one under the explicit unknown label.
    # Totals preserved, drift visible on the dashboard.
    observed: Counter[tuple[str, str]] = Counter()
    for dp in counter_data_points(metric_reader, "taskq.jobs.reclaimed"):
        attrs = dict(dp.attributes or {})
        observed[(str(attrs["actor"]), str(attrs["disposition"]))] += dp.value
    assert observed == Counter({("actor_alpha", "repended"): 1, ("actor_alpha", "unknown"): 1})


async def test_twin_reclaim_survives_an_unmapped_status(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The in-memory twin must survive the same drift: a KeyError
    mid-loop left a half-drained corpus (some rows transitioned, the rest
    still running, nothing counted) and no exception path to clean it up.
    Patching the map at its SOURCE module is the drift simulation: the
    twin resolves dispositions through the shared
    ``backend._sweeps._reclaim_disposition`` helper, which reads the
    map where it is defined, so however a consumer binds it the drifted
    map reaches the lookup. Every seeded row must still transition and
    count."""
    import taskq.backend._sweeps as sweeps_mod

    drifted = {k: v for k, v in sweeps_mod._RECLAIM_DISPOSITIONS.items() if k != "pending"}
    monkeypatch.setattr(sweeps_mod, "_RECLAIM_DISPOSITIONS", drifted)

    backend = _make_memory_backend()
    holder = new_uuid()
    for _ in range(3):
        await _seed_memory_row(backend, holder, "actor_alpha", max_attempts=3, cancel_phase=0)

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    # The whole corpus drained: no half-drained rows left behind.
    assert count == 3
    statuses = {row.status for row in backend._jobs.values()}  # pyright: ignore[reportPrivateUsage]  # Why: test-only read of the twin's corpus, the established seeding/inspection pattern (test_rt_sweeps_parity.py).
    assert statuses == {"pending"}, (
        f"every row must transition; corpus left half-drained: {statuses}"
    )
    # And the count landed, under the explicit unknown label, since the
    # drifted map no longer knows 'pending'.
    observed: Counter[tuple[str, str]] = Counter()
    for dp in counter_data_points(metric_reader, "taskq.jobs.reclaimed"):
        attrs = dict(dp.attributes or {})
        observed[(str(attrs["actor"]), str(attrs["disposition"]))] += dp.value
    assert observed == Counter({("actor_alpha", "unknown"): 3})
