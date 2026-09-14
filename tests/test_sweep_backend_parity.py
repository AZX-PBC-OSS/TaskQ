"""Sweep 3's row cap must be honoured identically by both backends.

``sweep_scheduled_to_pending`` gains a ``batch_size`` bound on Postgres (a
LIMITed snapshot CTE: one call transitions at most ``batch_size`` rows in
one short transaction) and the in-memory twin gains the same keyword so the
two backends keep processing the same amount of work per call.  The Backend
protocol signature stays unchanged — an extra defaulted keyword-only
parameter is structurally compatible — but parity is a behaviour, not a
signature: a backend that accepts the keyword and ignores it (or clamps it
differently) silently diverges, and every cross-backend assertion built on
the cap then passes on one backend while meaning nothing on the other.

Both backends are therefore driven here against the same corpus and the
same cap, asserting the two properties the cap exists for:

* one call promotes exactly ``batch_size`` eligible rows and leaves the
  remainder eligible (bounded per call);
* repeated calls drain to completion with exactly one ``state_change``
  event per promoted row (complete across calls — a cap that loses work or
  events is a regression, not a fix).

PG is seeded via one ``INSERT ... SELECT FROM unnest`` round trip (never
row-by-row: a row-by-row seed would itself be the defect under test); the
in-memory backend is seeded through its own ``enqueue`` with a future
``scheduled_at`` and the clock advanced past it — the established pattern
of the in-memory sweep tests.

The file also unit-pins ``SweepBatchSizer``, the state machine that picks
the Postgres side's effective cap when the caller does not pass one
explicitly: the two-tier degradation the parity bound degrades through
when the database keeps aborting full-size batches.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.backend._sweeps import SweepBatchSizer
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

# Corpus and cap: 25 eligible rows against a cap of 10 leaves a visible
# 15-row remainder, so a backend that sweeps the whole backlog in one call
# (no cap) and one that promotes fewer than the cap (a clamp bug) are both
# caught by the same first-call assertion.
_ELIGIBLE = 25
_CAP = 10

# Generous ceiling for the drain loops: any correct cap retires 25 rows in
# at most ceil(25/10) + 1 further calls, so a broken cap fails the
# assertion instead of hanging the suite.
_DRAIN_CALL_CEILING = _ELIGIBLE // _CAP + 5

# FakeClock start and the instant every seeded job becomes due: enqueueing
# with a future scheduled_at leaves rows 'scheduled', and advancing the
# clock to _DUE makes all of them eligible in one deterministic step.
_START = datetime(2025, 1, 1, tzinfo=UTC)
_DUE = _START + timedelta(hours=1)


def _make_memory_backend() -> InMemoryBackend:
    """Construct an InMemoryBackend with a FakeClock at the standard start."""
    return InMemoryBackend(clock=FakeClock(_START))


async def _seed_scheduled_pg(
    conn: asyncpg.Connection,
    schema: str,
    count: int,
) -> list[UUID]:
    """Seed *count* due scheduled jobs on Postgres in ONE round trip."""
    job_ids = [new_uuid() for _ in range(count)]
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
    )
    return job_ids


async def _seed_scheduled_memory(backend: InMemoryBackend, count: int) -> list[JobId]:
    """Seed *count* due scheduled jobs on the in-memory backend.

    Enqueue carries a FUTURE scheduled_at (a past one would land the row
    directly in 'pending', never exercising the sweep), then the clock is
    advanced past every row's scheduled_at in one step.
    """
    job_ids: list[JobId] = []
    for _ in range(count):
        args = make_enqueue_args(scheduled_at=_DUE)
        await backend.enqueue(args)
        job_ids.append(args.id)
    backend.advance_clock_to(_DUE)
    return job_ids


def _memory_status_count(backend: InMemoryBackend, status: str) -> int:
    """Count the in-memory backend's job rows currently in *status*."""
    return sum(
        1
        for row in backend._jobs.values()  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access; the public read APIs aggregate over ACTIVE_STATUSES and cannot express a single-status count.
        if row.status == status
    )


async def test_both_backends_promote_at_most_batch_size_per_call(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """One capped call promotes exactly the cap on BOTH backends.

    The two backends must honour the same bound per call: the leader's
    sweep budget (and the ``job_events`` INSERT-to-COMMIT span the
    visibility margin bounds) is spent per call, so a backend that ignores
    the cap re-introduces the unbounded transaction on its side of the
    parity line even while the other side is fixed.
    """
    schema = module_pg_schema.schema_name
    await _seed_scheduled_pg(clean_pg_conn, schema, _ELIGIBLE)

    memory = _make_memory_backend()
    await _seed_scheduled_memory(memory, _ELIGIBLE)
    assert _memory_status_count(memory, "scheduled") == _ELIGIBLE, (
        "the in-memory seed must leave every row eligible before the first call"
    )

    pg_count = await PostgresBackend.sweep_scheduled_to_pending(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        batch_size=_CAP,
    )
    mem_count = await memory.scheduled_to_pending(batch_size=_CAP)

    assert pg_count == _CAP, (
        f"one Postgres sweep call promoted {pg_count} of {_ELIGIBLE} eligible rows "
        f"with a cap of {_CAP} — the batch bound is not honoured"
    )
    assert mem_count == _CAP, (
        f"one in-memory sweep call promoted {mem_count} of {_ELIGIBLE} eligible rows "
        f"with a cap of {_CAP} — the twin does not honour the same bound the "
        "Postgres sweep honours, so the backends have diverged"
    )

    pg_remaining = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'scheduled'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert pg_remaining == _ELIGIBLE - _CAP, (
        "rows beyond the cap must stay 'scheduled' on Postgres for the next "
        f"committed batch; expected {_ELIGIBLE - _CAP}, got {pg_remaining}"
    )
    assert _memory_status_count(memory, "scheduled") == _ELIGIBLE - _CAP, (
        "rows beyond the cap must stay 'scheduled' in memory for the next call; "
        f"expected {_ELIGIBLE - _CAP}, got {_memory_status_count(memory, 'scheduled')}"
    )


async def test_capped_calls_drain_with_one_event_per_promoted_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Repeated capped calls drain fully, one event per promoted row, on BOTH
    backends.

    The companion to the cap assertion: a bound that leaves rows behind
    forever is a stall, and a drain that loses the per-row ``state_change``
    event breaks the audit trail (and, on Postgres, the reclaim-event
    watermark's per-row distinctness).  Both backends must complete.
    """
    schema = module_pg_schema.schema_name
    pg_job_ids = await _seed_scheduled_pg(clean_pg_conn, schema, _ELIGIBLE)

    memory = _make_memory_backend()
    mem_job_ids = await _seed_scheduled_memory(memory, _ELIGIBLE)

    # ── Postgres drain ──────────────────────────────────────────────
    pg_total = 0
    for _ in range(_DRAIN_CALL_CEILING):
        n = await PostgresBackend.sweep_scheduled_to_pending(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
            batch_size=_CAP,
        )
        assert n <= _CAP, f"a capped Postgres call promoted {n} rows, cap was {_CAP}"
        if n == 0:
            break
        pg_total += n
    assert pg_total == _ELIGIBLE, (
        f"repeated capped Postgres calls promoted {pg_total} of {_ELIGIBLE} rows"
    )

    # ── In-memory drain ─────────────────────────────────────────────
    mem_total = 0
    for _ in range(_DRAIN_CALL_CEILING):
        n = await memory.scheduled_to_pending(batch_size=_CAP)
        assert n <= _CAP, f"a capped in-memory call promoted {n} rows, cap was {_CAP}"
        if n == 0:
            break
        mem_total += n
    assert mem_total == _ELIGIBLE, (
        f"repeated capped in-memory calls promoted {mem_total} of {_ELIGIBLE} rows"
    )

    assert _memory_status_count(memory, "scheduled") == 0, (
        "the in-memory drain must leave no row 'scheduled'"
    )

    # ── One state_change event per promoted row, on both ────────────
    pg_events = await clean_pg_conn.fetch(
        f'SELECT job_id, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier.
        "WHERE kind = 'state_change' GROUP BY job_id",
    )
    assert {row["job_id"] for row in pg_events} == set(pg_job_ids), (
        "every promoted Postgres job must carry at least one state_change event"
    )
    assert all(row["n"] == 1 for row in pg_events), (
        "exactly one state_change event per promoted Postgres job — a batched "
        "write must not drop or duplicate the per-row audit trail"
    )

    for job_id in mem_job_ids:
        events = await memory.get_events(job_id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 1, (
            f"job {job_id} carries {len(state_changes)} state_change events in "
            "memory — exactly one per promoted row is the parity contract"
        )
        assert state_changes[0].detail["to_state"] == "pending"
        assert state_changes[0].detail["from_state"] == "scheduled"


# ── SweepBatchSizer ────────────────────────────────────────────────────
# Unit pins for the two-tier degradation the Postgres side of the parity
# pair falls back to.  A fake clock drives the rolling window so the
# latch semantics are deterministic, not sleep-flaky.


class _FakeClock:
    """Monotonic fake: starts at 0.0, advances only when told to."""

    def __init__(self) -> None:
        self.now_value: float = 0.0

    def __call__(self) -> float:
        return self.now_value

    def advance(self, secs: float) -> None:
        self.now_value += secs


class TestSweepBatchSizer:
    def test_default_tier_before_any_failure(self) -> None:
        sizer = SweepBatchSizer(100, 4, 3, 600.0)
        assert sizer.effective_size() == 100

    def test_latches_after_threshold_consecutive_failures(self) -> None:
        clock = _FakeClock()
        sizer = SweepBatchSizer(100, 4, 3, 600.0, now=clock)
        sizer.on_timeout()
        clock.advance(1.0)
        sizer.on_timeout()
        clock.advance(1.0)
        assert sizer.effective_size() == 100, "two failures of three must not latch"
        sizer.on_timeout()
        assert sizer.effective_size() == 25, "third consecutive failure must latch the reduced tier"

    def test_success_resets_the_consecutive_count_before_latching(self) -> None:
        clock = _FakeClock()
        sizer = SweepBatchSizer(100, 4, 3, 600.0, now=clock)
        sizer.on_timeout()
        clock.advance(1.0)
        sizer.on_timeout()
        sizer.on_success()
        clock.advance(1.0)
        sizer.on_timeout()
        sizer.on_timeout()
        assert sizer.effective_size() == 100, (
            "a success between failures resets the consecutive count — only "
            "failures since the last success count toward the threshold"
        )

    def test_failures_outside_the_rolling_window_expire(self) -> None:
        clock = _FakeClock()
        sizer = SweepBatchSizer(100, 4, 3, 600.0, now=clock)
        sizer.on_timeout()
        clock.advance(601.0)  # the first failure is now outside the window
        sizer.on_timeout()
        sizer.on_timeout()
        assert sizer.effective_size() == 100, (
            "a failure older than the window must not count toward the latch"
        )

    def test_the_latch_is_one_way(self) -> None:
        clock = _FakeClock()
        sizer = SweepBatchSizer(100, 4, 3, 600.0, now=clock)
        for _ in range(3):
            sizer.on_timeout()
        assert sizer.effective_size() == 25
        sizer.on_success()
        assert sizer.effective_size() == 25, (
            "on_success resets the consecutive count but must never unlatch — "
            "a database that needed smaller bites once will need them again"
        )
        for _ in range(3):
            sizer.on_timeout()
        assert sizer.effective_size() == 25, "the reduced tier is for the object's lifetime"

    def test_reduced_tier_floors_at_one(self) -> None:
        sizer = SweepBatchSizer(3, 4, 3, 600.0)
        for _ in range(3):
            sizer.on_timeout()
        assert sizer.effective_size() == 1, "max(1, default // divisor) must never reach zero"

    def test_constructor_accepts_the_minimum_valid_configuration(self) -> None:
        """The floor values of the constructor's ``<`` guards must WORK.

        The guards reject ``divisor < 2`` and ``failure_threshold < 1``, so
        ``divisor=2`` and ``failure_threshold=1`` are legal configurations
        the settings layer also accepts (``ge=2`` / ``ge=1``).  An off-by-one
        in either guard (``<= 2``) would pass every rejection test while
        breaking a legal deployment's first sweep.  At the floor the reduced
        tier must still divide (100 // 2 == 50) and the latch must engage on
        the first timeout."""
        sizer = SweepBatchSizer(100, 2, 1, 600.0)
        assert sizer.effective_size() == 100
        sizer.on_timeout()
        assert sizer.effective_size() == 50, (
            "threshold=1 latches on the first timeout, and the floor divisor "
            "of 2 must still produce a reduced tier half the default"
        )
