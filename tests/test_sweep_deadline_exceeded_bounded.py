"""Sweep 2 (deadline exceeded) must be bounded in statements AND in rows.

``sweep_deadline_exceeded`` (``src/taskq/backend/_sweeps.py``) sweeps at
most ``batch_size`` overdue ``pending``/``scheduled`` rows per call, in
one short transaction: a LIMIT-ed ``FOR UPDATE SKIP LOCKED`` driving
UPDATE, one batched ``job_attempts`` INSERT, one batched ``job_events``
INSERT, and an aggregate (not per-row) OTel metric emission — with a
server-side ``statement_timeout`` over the whole batch.  Repeated calls
drain the eligible backlog one committed batch at a time.

Why boundedness is a correctness property, not just a speed one
----------------------------------------------------------------
``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY`` is 2 seconds, and its
docstring states the trailing-watermark guarantee behind
``poll_reclaim_events`` is **conditional**: it holds only while "no
``job_events`` writer takes longer than this margin between its INSERT and
its COMMIT", and it names "an abnormally large batch inserted in one
transaction" as a known way to violate it. The consequence it spells out is
not an error --

    "the consequence is a **silently missed event**: a lower-``id`` row can
    commit after the cursor has already advanced past its position, with no
    error raised anywhere"

This sweep is exactly the writer that guarantee assumes away, so both of
its boundedness properties are load-bearing:

1. **Statements bounded** -- the per-row awaited round trips collapse to a
   constant number of statements per sweep call (a batched ``unnest`` over
   the swept ids), and the metric emission leaves the transaction
   (``record_deadline_exceeded_swept`` takes a ``count``, so it
   aggregates naturally).
2. **Rows bounded** -- the driving CTE honours a row cap, so one call's
   transaction duration is bounded by a constant rather than by the
   backlog.  Draining the rest is the caller's loop, one committed batch
   at a time.

The statement assertions count awaited round trips rather than wall-clock
seconds: the count is constant in N after bounding, in any environment,
whereas the seconds are RTT-dependent and would be flaky.

Layer 2 pins the observable behaviour the bounding must not change. Sweep 2
writes genuinely **per-row-distinct** values -- ``started_at``, ``attempt``,
``duration_ms``, and critically ``prev_status``, which is honestly two-valued
because the driving CTE filters ``status IN ('pending','scheduled')`` and
carries the pre-UPDATE value out as ``snap.prev_status``. A careless rewrite
that batches the event INSERT over a bare ``uuid[]`` and hardcodes a single
``from_state`` would be silently wrong for half the corpus, so the Layer 2
tests seed BOTH source statuses and assert each row's event maps back to the
status that row actually had.
"""

from __future__ import annotations

import inspect
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq._json import loads
from taskq.backend._sweeps import _SWEEP_2_SQL, sweep_deadline_exceeded
from taskq.backend.postgres import PostgresBackend
from taskq.constants import RECLAIM_EVENT_VISIBILITY_DELAY
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

# Large enough that a per-row loop is unmistakable in the statement count
# (2N + 1 = 121 vs. a small constant), small enough to stay a fast test.
_SWEPT = 60

# Row cap used to prove one call's transaction is bounded by a constant rather
# than by the backlog. Deliberately smaller than _SWEPT so an unbounded sweep
# overshoots it visibly.
_CAP = 10


def _detail(raw: object) -> dict[str, Any]:
    """Decode a ``job_events.detail`` value.

    asyncpg returns ``jsonb`` as ``str`` unless a codec is registered; the
    existing sweep tests decode defensively the same way.
    """
    if isinstance(raw, str):
        decoded: dict[str, Any] = loads(raw)
        return decoded
    assert isinstance(raw, dict)
    return raw


class _CountingConn:
    """Delegates to a real connection, counting awaited round trips by target.

    The statement count IS the property under test: correctness never differed
    between the per-row loop and a batched write, only the number of awaited
    round trips taken inside the transaction while the swept rows' locks are
    held -- and it is that hold duration, measured against
    ``RECLAIM_EVENT_VISIBILITY_DELAY``, that turns a slow sweep into a silently
    missed reclaim event.

    Counting by target table rather than by exact SQL text pins the invariant
    (a bounded number of statements per sweep, not one per row) rather than the
    spelling of the fix: the single-row form and a batched ``unnest`` form both
    count as one statement each, so any correct batching passes and any
    reintroduced loop -- spelled with ``enumerate``, a comprehension of awaits,
    or a helper -- still fails.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.attempt_inserts = 0
        self.event_inserts = 0
        self.other_executes = 0

    def _tally(self, sql: str) -> None:
        head = sql.lstrip().upper()
        if head.startswith("INSERT INTO") or head.startswith("WITH"):
            if ".job_attempts" in sql:
                self.attempt_inserts += 1
                return
            if ".job_events" in sql:
                self.event_inserts += 1
                return
        self.other_executes += 1

    @property
    def total_writes(self) -> int:
        return self.attempt_inserts + self.event_inserts + self.other_executes

    async def execute(self, sql: str, *args: object) -> str:
        self._tally(sql)
        result: str = await self._conn.execute(sql, *args)
        return result

    async def executemany(self, sql: str, args: object) -> Any:
        self._tally(sql)
        return await self._conn.executemany(sql, args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


async def _seed(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str,
    count: int,
    overdue: bool = True,
) -> list[Any]:
    """Seed *count* jobs in *status* with a past (or future) schedule_to_close.

    One ``INSERT ... SELECT FROM unnest($1::uuid[])`` -- never row-by-row: a
    row-by-row seed would itself be the defect under test, and at _SWEPT rows it
    would dominate the test's runtime.

    ``started_at`` is left NULL (these jobs were never dispatched), which is the
    real shape of a sweep-2 victim and the reason ``_SWEEP_2_ATTEMPTS_BATCH_SQL``
    carries ``COALESCE($3, clock_timestamp())``.
    """
    job_ids = [new_uuid() for _ in range(count)]
    direction = "-" if overdue else "+"
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        " priority, scheduled_at, schedule_to_close) "
        f"SELECT id, 'test_actor', 'default', '{{}}'::jsonb, '{status}', 3, "  # Why: status is a test-local literal, not user input.
        "'transient', 0, clock_timestamp() - interval '60 seconds', "
        f"clock_timestamp() {direction} interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
    )
    return job_ids


# ══════════════════════════════════════════════════════════════════════
# LAYER 1 — the boundedness contract these tests enforce.
# ══════════════════════════════════════════════════════════════════════


async def test_sweep_issues_a_bounded_number_of_statements(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: sweeping N jobs must not take 2N round trips.

    Drives the real ``sweep_deadline_exceeded`` and counts awaited round trips,
    rather than grepping the source for a ``for rec in rows:`` loop: a
    reintroduced loop spelled any other way is the same regression and a regex
    would not see it.

    A bounded call collapses the per-row ``job_attempts`` and
    ``job_events`` INSERTs to a constant number of statements per sweep
    call, inside the transaction holding the swept rows' locks.
    """
    schema = module_pg_schema.schema_name
    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT)

    counting = _CountingConn(clean_pg_conn)
    count = await PostgresBackend.sweep_deadline_exceeded(
        counting,  # type: ignore[arg-type]  # Why: duck-typed connection; only execute/fetch/transaction are used.
        schema=schema,
    )

    assert count == _SWEPT, f"all {_SWEPT} overdue jobs must be swept"

    # A batched fix needs at most a handful of statements regardless of N. The
    # bound is deliberately generous (it must not dictate the spelling of the
    # fix) while still being far below 2N.
    assert counting.attempt_inserts <= 2, (
        f"expected a bounded number of job_attempts INSERTs for {_SWEPT} swept "
        f"jobs, got {counting.attempt_inserts} — the per-row loop at "
        "_sweeps.py:419 is still there, taking one awaited round trip per row "
        "inside the transaction holding every swept row's lock"
    )
    assert counting.event_inserts <= 2, (
        f"expected a bounded number of job_events INSERTs for {_SWEPT} swept "
        f"jobs, got {counting.event_inserts} — the per-row loop at "
        "_sweeps.py:438 is still there. job_events is the table whose "
        "INSERT-to-COMMIT span RECLAIM_EVENT_VISIBILITY_DELAY "
        f"({RECLAIM_EVENT_VISIBILITY_DELAY.total_seconds():.0f}s) bounds; "
        "exceeding it silently drops reclaim events with no error raised"
    )
    assert counting.total_writes <= 6, (
        f"total awaited write statements for {_SWEPT} swept jobs must be "
        f"constant, got {counting.total_writes} (≈2N today)"
    )


async def test_statement_count_does_not_grow_with_the_backlog(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: round trips must be independent of row count.

    The scale-invariance form of the assertion above, and the one that
    most directly states the contract: doubling the backlog must not
    double the number of statements held inside one transaction.
    Comparing two runs rather than asserting one absolute bound makes
    this test indifferent to whatever constant overhead the batching
    settles on.
    """
    schema = module_pg_schema.schema_name

    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT)
    small = _CountingConn(clean_pg_conn)
    await PostgresBackend.sweep_deadline_exceeded(
        small,  # type: ignore[arg-type]  # Why: duck-typed connection.
        schema=schema,
    )

    # Second, larger backlog in the same schema.
    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT * 2)
    large = _CountingConn(clean_pg_conn)
    await PostgresBackend.sweep_deadline_exceeded(
        large,  # type: ignore[arg-type]  # Why: duck-typed connection.
        schema=schema,
    )

    assert large.total_writes == small.total_writes, (
        f"sweeping {_SWEPT * 2} rows took {large.total_writes} statements vs "
        f"{small.total_writes} for {_SWEPT} rows — transaction hold time scales "
        "with the backlog, which is precisely the trigger "
        "RECLAIM_EVENT_VISIBILITY_DELAY's docstring names ('an abnormally "
        "large batch inserted in one transaction')"
    )


def test_sweep_2_sql_carries_a_row_cap() -> None:
    """LAYER 1: ``_SWEEP_2_SQL``'s CTE must be LIMITed.

    Reading the SQL text is legitimate here (unlike the loop, which must be
    observed behaviourally) because the LIMIT is a property of the
    statement itself: without one, the ``FOR UPDATE SKIP LOCKED`` snapshot locks
    the entire matching backlog in a single transaction no matter how the Python
    around it is written.
    """
    assert "LIMIT" in _SWEEP_2_SQL.upper(), (
        "_SWEEP_2_SQL (src/taskq/backend/_sweeps.py:140) has no LIMIT: its CTE "
        "takes FOR UPDATE SKIP LOCKED over every matching row, so one call's "
        "transaction duration and lock-hold set are proportional to the backlog"
    )


async def test_one_call_touches_at_most_the_cap(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: a row cap must be honoured per call.

    ``sweep_deadline_exceeded`` exposes a cap and honours it, so that one
    call's transaction is bounded by a constant.

    The cap's parameter name is not pinned: the test accepts ``batch_size``
    or ``limit``, whichever spelling the implementation adopts.
    """
    schema = module_pg_schema.schema_name
    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT)

    params = inspect.signature(sweep_deadline_exceeded).parameters
    cap_kw = next((name for name in ("batch_size", "limit") if name in params), None)
    assert cap_kw is not None, (
        "sweep_deadline_exceeded exposes no row cap (expected a 'batch_size' or "
        "'limit' keyword): one call still sweeps the entire backlog in a single "
        "transaction, so its lock-hold and its job_events INSERT-to-COMMIT span "
        "are unbounded"
    )

    swept = await sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        **{cap_kw: _CAP},
    )

    assert swept <= _CAP, f"one call swept {swept} rows despite a cap of {_CAP}"
    remaining = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier.
        "WHERE status IN ('pending', 'scheduled')"
    )
    assert remaining == _SWEPT - swept, (
        "rows beyond the cap must be left untouched for the caller's next "
        f"committed batch; expected {_SWEPT - swept} still eligible, got {remaining}"
    )


async def test_capped_calls_drain_the_whole_backlog(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: repeated capped calls must make full progress.

    A cap is only safe if draining still terminates and still sweeps every
    eligible row: each call commits its own batch, so a backlog is retired over
    several short transactions instead of one long one. This is the property
    that makes bounding the sweep a correctness fix rather than a throughput
    regression.
    """
    schema = module_pg_schema.schema_name
    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT)

    params = inspect.signature(sweep_deadline_exceeded).parameters
    cap_kw = next((name for name in ("batch_size", "limit") if name in params), None)
    assert cap_kw is not None, "sweep_deadline_exceeded exposes no row cap"

    total = 0
    # Bound the drain loop so a broken cap fails the assertion rather than
    # hanging the suite.
    for _ in range(_SWEPT // _CAP + 5):
        swept = await sweep_deadline_exceeded(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
            **{cap_kw: _CAP},
        )
        if swept == 0:
            break
        assert swept <= _CAP, f"a capped call swept {swept} rows, cap was {_CAP}"
        total += swept

    assert total == _SWEPT, f"drain loop swept {total} of {_SWEPT} eligible rows"
    failed = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'failed'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert failed == _SWEPT, "every eligible row must end 'failed' after the drain"
    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert events == _SWEPT, "one event per swept row must survive batching"


# ══════════════════════════════════════════════════════════════════════
# LAYER 2 — CORRECTNESS PINNING. The bounded sweep rewrites SQL that
# mutates job state, so every observable effect of that SQL is nailed
# down here.
# ══════════════════════════════════════════════════════════════════════


async def test_pins_terminal_job_state_of_every_swept_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    Every swept job lands ``status='failed'`` with
    ``error_class='DeadlineExceeded'``, the exact ``error_message``
    literal from ``_SWEEP_2_SQL``, and a stamped ``finished_at``.
    ``finished_at`` must be per-row ``clock_timestamp()``, never
    transaction-start ``now()``: the module docstring of ``_sweeps.py``
    is explicit that inside a long-held sweep transaction the two
    disagree, and that this column must agree with
    ``job_events.occurred_at``.
    """
    schema = module_pg_schema.schema_name
    pending_ids = await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT // 2)
    scheduled_ids = await _seed(clean_pg_conn, schema, status="scheduled", count=_SWEPT // 2)
    expected = set(pending_ids) | set(scheduled_ids)

    count = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert count == len(expected)

    rows = await clean_pg_conn.fetch(
        f"SELECT id, status, error_class, error_message, finished_at, started_at "  # noqa: S608  # Why: schema is a test-fixture identifier.
        f'FROM "{schema}".jobs'
    )
    assert {r["id"] for r in rows} == expected

    for row in rows:
        assert row["status"] == "failed", f"job {row['id']} ended {row['status']!r}"
        assert row["error_class"] == "DeadlineExceeded"
        assert row["error_message"] == "schedule_to_close reached before next dispatch"
        assert row["finished_at"] is not None, "finished_at must be stamped"
        # Never dispatched: the sweep must not invent a started_at on the job.
        assert row["started_at"] is None

    # Per-row clock_timestamp(), not transaction-start now(): a batch written
    # under now() would collapse to a single identical value.
    distinct_finished = await clean_pg_conn.fetchval(
        f'SELECT count(DISTINCT finished_at) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert distinct_finished > 1, (
        "jobs.finished_at collapsed to a single value across the batch — that is "
        "transaction-start now(), not the per-row clock_timestamp() the sweep "
        "module docstring requires"
    )


async def test_pins_one_job_attempts_row_per_swept_job(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    Exactly one ``job_attempts`` row per swept job, with the shape
    ``_SWEEP_2_ATTEMPTS_BATCH_SQL`` (:211-215) writes today: ``outcome='failed'``,
    ``error_class='DeadlineExceeded'``, NULL traceback, NULL ``worker_id`` (the
    job was never dispatched, so there is no lock holder), and a non-NULL
    ``started_at`` supplied by the ``COALESCE($3, clock_timestamp())``.

    ``attempt`` must round-trip per row, not be flattened to one value: the
    ``(job_id, attempt)`` primary key means a rewrite that unnests attempts in
    the wrong column order would either key-collide or mis-attribute silently.
    """
    schema = module_pg_schema.schema_name
    job_ids = await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT)
    # Give the corpus genuinely distinct per-row attempt values so a rewrite
    # that broadcasts a single scalar is caught.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET attempt = (abs(hashtext(id::text)) % 3)'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    expected_attempt = {
        r["id"]: r["attempt"]
        for r in await clean_pg_conn.fetch(f'SELECT id, attempt FROM "{schema}".jobs')  # noqa: S608  # Why: schema is a test-fixture identifier.
    }

    count = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert count == _SWEPT

    attempts = await clean_pg_conn.fetch(f'SELECT * FROM "{schema}".job_attempts')  # noqa: S608  # Why: schema is a test-fixture identifier.
    assert len(attempts) == _SWEPT, "exactly one job_attempts row per swept job"
    assert {a["job_id"] for a in attempts} == set(job_ids)

    for att in attempts:
        assert att["outcome"] == "failed"
        assert att["error_class"] == "DeadlineExceeded"
        assert att["error_message"] == "schedule_to_close reached before next dispatch"
        assert att["error_traceback"] is None
        assert att["worker_id"] is None, "never dispatched: no lock holder to record"
        assert att["started_at"] is not None, "COALESCE($3, clock_timestamp()) must fire"
        assert att["finished_at"] is not None
        assert att["attempt"] == expected_attempt[att["job_id"]], (
            f"job {att['job_id']} recorded attempt {att['attempt']}, expected "
            f"{expected_attempt[att['job_id']]} — per-row attempt was not "
            "carried through"
        )

    # started_at was NULL on every job, so every attempt row's started_at came
    # from its own clock_timestamp() call; a single broadcast value would mean
    # the per-row evaluation was lost.
    distinct_started = await clean_pg_conn.fetchval(
        f'SELECT count(DISTINCT started_at) FROM "{schema}".job_attempts'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert distinct_started > 1, (
        "job_attempts.started_at collapsed to one value — the COALESCE's "
        "clock_timestamp() is no longer evaluated per row"
    )


async def test_pins_event_from_state_matches_each_rows_actual_prior_status(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    **This is the invariant a careless multi-column-unnest rewrite would
    break.** ``_SWEEP_2_SQL``'s CTE filters ``status IN ('pending','scheduled')``
    and carries the pre-UPDATE value out as ``snap.prev_status``, so
    ``detail.from_state`` is genuinely two-valued across one batch. A fix that
    batches the event INSERT over a bare ``uuid[]`` and hardcodes a single
    ``from_state`` -- or that unnests ids and statuses as independently-ordered
    arrays -- would be silently wrong for whichever half it guessed against.

    Both source statuses are seeded, interleaved by id, and every event is
    asserted against the status *that row actually had*, not against an
    aggregate.
    """
    schema = module_pg_schema.schema_name
    pending_ids = await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT // 2)
    scheduled_ids = await _seed(clean_pg_conn, schema, status="scheduled", count=_SWEPT // 2)
    expected_from = dict.fromkeys(pending_ids, "pending")
    expected_from.update(dict.fromkeys(scheduled_ids, "scheduled"))

    count = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert count == len(expected_from)

    events = await clean_pg_conn.fetch(
        f'SELECT id, job_id, kind, detail, occurred_at FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier.
        "ORDER BY id"
    )
    assert len(events) == len(expected_from), "exactly one job_events row per swept job"
    assert {e["job_id"] for e in events} == set(expected_from)

    seen_from: set[str] = set()
    for ev in events:
        assert ev["kind"] == "state_change"
        detail = _detail(ev["detail"])
        assert detail["to_state"] == "failed"
        assert detail["error_class"] == "DeadlineExceeded"
        want = expected_from[ev["job_id"]]
        assert detail["from_state"] == want, (
            f"job {ev['job_id']} was {want!r} before the sweep but its event "
            f"records from_state={detail['from_state']!r} — per-row prev_status "
            "was lost"
        )
        seen_from.add(detail["from_state"])

    assert seen_from == {"pending", "scheduled"}, (
        "the corpus must exercise BOTH source statuses for this assertion to "
        f"mean anything; saw {sorted(seen_from)}"
    )


async def test_pins_event_occurred_at_is_per_row_and_co_monotonic_with_id(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    ``job_events.occurred_at`` (``clock_timestamp()``) and ``job_events.id``
    (bigserial) must stay co-monotonic: whichever row was inserted first has
    both the lower id and the earlier ``occurred_at``. This is the assumption
    ``RECLAIM_EVENT_VISIBILITY_DELAY``'s docstring rests the trailing-watermark
    guarantee on, and ``poll_reclaim_events`` orders by it. A batched rewrite
    that stamps the whole batch with transaction-start ``now()`` -- or that
    inserts in an order other than the one that assigns ids -- breaks reclaim
    delivery silently.

    Two assertions, both necessary:
      - ``occurred_at`` is non-decreasing when ordered by ``id`` (no inversions);
      - ``occurred_at`` values are DISTINCT per row, which is what proves the
        per-row ``clock_timestamp()`` survived rather than collapsing to one
        transaction-wide value.
    """
    schema = module_pg_schema.schema_name
    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT)

    await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )

    rows = await clean_pg_conn.fetch(
        f'SELECT id, occurred_at FROM "{schema}".job_events ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert len(rows) == _SWEPT

    inversions = [
        (rows[i - 1]["id"], rows[i]["id"])
        for i in range(1, len(rows))
        if rows[i]["occurred_at"] < rows[i - 1]["occurred_at"]
    ]
    assert not inversions, (
        f"job_events.occurred_at is not co-monotonic with id: {inversions[:3]} — "
        "poll_reclaim_events' trailing watermark orders on this and would "
        "silently skip the inverted rows"
    )

    distinct = len({r["occurred_at"] for r in rows})
    assert distinct > 1, (
        f"all {len(rows)} job_events rows share one occurred_at — that is "
        "transaction-start now(), not the per-row clock_timestamp() "
        "INSERT_EVENT_SQL specifies"
    )


async def test_pins_non_matching_rows_are_untouched(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    Rows outside the predicate must be left exactly as they were: a job whose
    ``schedule_to_close`` is still in the future, a job with no
    ``schedule_to_close`` at all, and a ``running`` job past its deadline (sweep
    2 only targets ``pending``/``scheduled``; the running case belongs to
    sweep 1). None of them may gain a ``job_attempts`` or ``job_events`` row.
    """
    schema = module_pg_schema.schema_name

    doomed = await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT // 2)
    future = await _seed(clean_pg_conn, schema, status="pending", count=5, overdue=False)
    future_scheduled = await _seed(
        clean_pg_conn, schema, status="scheduled", count=5, overdue=False
    )

    # No schedule_to_close at all.
    no_deadline = [new_uuid() for _ in range(3)]
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() FROM unnest($1::uuid[]) AS t(id)",
        no_deadline,
    )

    # Running, past deadline: sweep 1's territory, not sweep 2's.
    running = [new_uuid()]
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        " scheduled_at, schedule_to_close, started_at, lock_expires_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'running', 3, 'transient', "
        "clock_timestamp(), clock_timestamp() - interval '30 seconds', "
        "clock_timestamp(), clock_timestamp() + interval '60 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        running,
    )

    untouched = set(future) | set(future_scheduled) | set(no_deadline) | set(running)
    before = {
        r["id"]: (r["status"], r["error_class"], r["error_message"], r["finished_at"])
        for r in await clean_pg_conn.fetch(
            f"SELECT id, status, error_class, error_message, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
            list(untouched),
        )
    }

    count = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert count == len(doomed), (
        f"only the {len(doomed)} overdue pending/scheduled jobs are eligible; swept {count}"
    )

    after = {
        r["id"]: (r["status"], r["error_class"], r["error_message"], r["finished_at"])
        for r in await clean_pg_conn.fetch(
            f"SELECT id, status, error_class, error_message, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
            list(untouched),
        )
    }
    assert after == before, "rows outside the predicate must be byte-for-byte unchanged"

    side_effects = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier.
        list(untouched),
    )
    assert side_effects == 0, "ineligible rows must gain no job_events row"
    attempt_side_effects = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier.
        list(untouched),
    )
    assert attempt_side_effects == 0, "ineligible rows must gain no job_attempts row"


async def test_pins_idempotency_of_a_second_sweep(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    Re-running the sweep over an already-swept corpus sweeps nothing extra and
    writes no further rows: the swept jobs are now ``failed``, outside the CTE's
    ``status IN ('pending','scheduled')`` predicate. This matters doubly after
    batching, because ``job_attempts`` is keyed ``(job_id, attempt)`` — a
    rewrite that re-selected an already-swept row would raise a unique violation
    inside the leader's sweep loop, where a constraint error is deliberately
    non-transient and would tear the leader down.
    """
    schema = module_pg_schema.schema_name
    await _seed(clean_pg_conn, schema, status="pending", count=_SWEPT // 2)
    await _seed(clean_pg_conn, schema, status="scheduled", count=_SWEPT // 2)

    first = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert first == _SWEPT

    events_after_first = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    attempts_after_first = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )

    second = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert second == 0, f"a second sweep must find nothing eligible, swept {second}"

    assert (
        await clean_pg_conn.fetchval(f'SELECT count(*) FROM "{schema}".job_events')  # noqa: S608  # Why: schema is a test-fixture identifier.
    ) == events_after_first, "a no-op sweep must write no job_events rows"
    assert (
        await clean_pg_conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts')  # noqa: S608  # Why: schema is a test-fixture identifier.
    ) == attempts_after_first, "a no-op sweep must write no job_attempts rows"
    assert (
        await clean_pg_conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'failed'"  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
    ) == _SWEPT


async def test_pins_return_value_equals_rows_actually_swept(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    The return value is the count the leader logs and metrics
    (``worker/_leader_sweeps.py``:145-165), so it must equal the number of rows
    whose state actually changed — not the number of candidates scanned, and not
    a cap. An empty backlog returns 0.
    """
    schema = module_pg_schema.schema_name

    assert (
        await PostgresBackend.sweep_deadline_exceeded(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
        )
        == 0
    ), "an empty backlog must return 0"

    await _seed(clean_pg_conn, schema, status="pending", count=7)
    await _seed(clean_pg_conn, schema, status="scheduled", count=4)
    await _seed(clean_pg_conn, schema, status="pending", count=3, overdue=False)

    count = await PostgresBackend.sweep_deadline_exceeded(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
    )
    assert count == 11, f"7 pending + 4 scheduled overdue rows are eligible, got {count}"

    changed = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'failed'"  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert changed == count, "the return value must equal the rows actually transitioned"


async def test_pins_rejection_of_an_invalid_schema_identifier(
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded sweep must preserve.

    The ``_IDENT_RE`` guard at ``_sweeps.py``:391 runs before any SQL is
    formatted. A batching rewrite that moves the ``.format`` calls around must
    not let an unvalidated schema reach interpolation.
    """
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await sweep_deadline_exceeded(
            None,  # type: ignore[arg-type]  # Why: the guard raises before the conn is touched.
            schema="bad-schema; DROP TABLE jobs",
        )
