"""Red-team attacks on the singleton DST-``allof`` deferral claim (real PG).

The singleton parity change deliberately deviates from the client enqueue
path in one place: a cron fire whose ``dst_strategy='allof'`` slot falls
inside a DST overlap (a repeated local hour) enqueues only ONE job — the
second occurrence of the repeated hour is deferred, with the planning
code claiming the schedule's own ``next_fire_at`` machinery delivers it
later: "next_fire_at lands on the first occurrence, and the repeated hour
fires on its own later tick once the first goes terminal."

That claim is the attack surface.  ``compute_next_fire_after`` walks
NAIVE local time, and the second occurrence of a repeated hour is
naive-IDENTICAL to the first, so the next cron match after the first
occurrence is next YEAR — the deferral is honest only if the first
occurrence's own tick advances ``next_fire_at`` to the second
occurrence's instant, and nothing computes that instant.  Measured
(real croniter): from 2026-11-01T05:30Z (local 01:30 EDT) the yearly
``30 1 1 11 *`` computes ``[2027-11-01 01:30-04:00]`` — the 06:30Z
second occurrence is unreachable from any later seed.

The fall-back construction (America/New_York 2026-11-01, local 01:30 at
both 05:30Z and 06:30Z — the same construction the time-semantics file
uses for the non-singleton pin) is driven at real PG across the tick
sequence a real leader would run: the pre-overlap tick (real wall clock,
a within-window catch-up seed), then the first- and second-occurrence
ticks with the due-check bound pinned to the overlap via
:class:`PinnedDueConn` — the due bound is the one statement in the tick
whose clock a test cannot otherwise reach, because the wall clock is
months away from the overlap and a past seed makes the planning compute
the overlap PAIR instead of landing inside it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    count_jobs,
    cron_settings,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_now,
)

pytestmark = pytest.mark.integration

_SINGLETON_ACTOR = "rt_parity_dst_singleton"
_OPEN_ACTOR = "rt_parity_dst_open"
_CAPPED_ACTOR = "rt_parity_dst_capped"

# America/New_York falls back on 2026-11-01 (02:00 EDT → 01:00 EST), so the
# local 01:30 of the yearly expression below occurs at BOTH 05:30Z (fold 0)
# and 06:30Z (fold 1).  2027-11-01 is NOT an overlap date (fall-back 2027 is
# Nov 7), so the next yearly match after the overlap is 2027-11-01 05:30Z.
_OVERLAP_YEARLY = "30 1 1 11 *"
_OVERLAP_TZ = "America/New_York"
_OVERLAP_FIRST_UTC = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
_OVERLAP_SECOND_UTC = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
_NEXT_YEAR_UTC = datetime(2027, 11, 1, 5, 30, tzinfo=UTC)

# The minutely fold-traversal drive below uses the same 2026-11-01 fall-back
# but every minute: fold-0 runs 05:00-05:59 UTC, fold-1 06:00-06:59 UTC, and
# the first slot past the repeated range (02:00 local) is 07:00 UTC.
_MINUTELY = "* * * * *"
_FOLD0_SEED_UTC = datetime(2026, 11, 1, 4, 58, tzinfo=UTC)  # 00:58 fold-0
_LAST_FOLD0_TICK_UTC = datetime(2026, 11, 1, 5, 59, tzinfo=UTC)  # fires 01:59 fold-0
_AFTER_RANGE_UTC = datetime(2026, 11, 1, 7, 0, tzinfo=UTC)  # 02:00 local
_FOLD1_TICKS_UTC = [
    datetime(2026, 11, 1, 6, 0, tzinfo=UTC),
    datetime(2026, 11, 1, 6, 1, tzinfo=UTC),
    datetime(2026, 11, 1, 6, 2, tzinfo=UTC),
]

_SINGLETON_POLICIES = {_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)}


class PinnedDueConn:
    """Delegates to a real connection except the tick's due-check bound.

    The tick's driving SELECT is the only statement whose
    ``statement_timestamp()`` decides WHICH rows are due; rewriting that
    one bound to a pinned instant simulates the wall clock reaching the
    2026-11-01 overlap, which a test otherwise cannot (the real clock is
    months away, and seeding a PAST ``next_fire_at`` makes the planning
    compute the overlap PAIR — the pre-overlap tick shape — instead of a
    fire landing inside the overlap).  Every other statement — the
    advisory-lock probe, the planning clock read, the policy preflights,
    the batched enqueue and the schedule UPDATEs — runs against real PG
    with real values on this same connection.
    """

    def __init__(self, conn: asyncpg.Connection, due_as_of: datetime) -> None:
        self._conn = conn
        self._due_as_of = due_as_of

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        if "next_fire_at <= statement_timestamp()" in sql:
            sql = sql.replace(
                "statement_timestamp()",
                f"'{self._due_as_of.isoformat()}'::timestamptz",
            )
        return await self._conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> asyncpg.Record | None:
        return await self._conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> object | None:
        return await self._conn.fetchval(sql, *args)

    async def execute(self, sql: str, *args: object) -> object:
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


async def _tick(
    conn: asyncpg.Connection,
    settings: WorkerSettings,
    schema: str,
    policies: Mapping[str, ActorFirePolicy],
    *,
    due_as_of: datetime | None = None,
) -> int:
    """One tick in its own transaction; *due_as_of* pins the due bound."""
    tick_conn: object = PinnedDueConn(conn, due_as_of) if due_as_of is not None else conn
    async with conn.transaction():
        return await tick_cron(
            tick_conn,  # type: ignore[arg-type]  # Why: either the real connection or the due-bound-pinning wrapper whose awaited protocol methods are all typed above.
            settings,
            make_backend(settings),
            schema,
            new_uuid(),
            actor_policies=policies,
        )


async def _settle_active_jobs(conn: asyncpg.Connection, schema: str, actor: str) -> None:
    """Take every active job for *actor* terminal — the real lifecycle event
    (a finished run) that frees a singleton actor between ticks."""
    await conn.execute(
        f'UPDATE "{schema}".jobs SET status = \'succeeded\'::"{schema}".job_status '  # noqa: S608  # Why: schema is a test-fixture identifier; the actor is $-bound.
        "WHERE actor = $1 AND status IN ('pending', 'scheduled', 'running')",
        actor,
    )


async def _seed_overlap_schedule(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    name: str,
    next_fire_at: datetime,
    identity_key: str,
) -> UUID:
    return await seed_schedule(
        conn,
        schema,
        actor=actor,
        name=name,
        cron_expr=_OVERLAP_YEARLY,
        timezone=_OVERLAP_TZ,
        dst_strategy="allof",
        next_fire_at=next_fire_at,
        identity_key=identity_key,
    )


async def _seed_active_singleton_job(conn: asyncpg.Connection, schema: str) -> UUID:
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, metadata) "
        f"VALUES ($1, $2, 'rt_queue', '{{}}'::jsonb, 5, 'transient', "
        f"'running'::\"{schema}\".job_status, '{{\"singleton\": true}}'::jsonb)",
        job_id,
        _SINGLETON_ACTOR,
    )
    return job_id


async def _count_schedule_jobs(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    schedule_id: UUID,
) -> int:
    """How many jobs the cron loop has enqueued FOR *schedule_id* — scoped
    by the ``cron_schedule_id`` provenance stamp, the same scope the
    coverage walk uses."""
    return await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the fixture identifier.
        "WHERE actor = $1 AND metadata->>'cron_schedule_id' = $2",
        actor,
        str(schedule_id),
    )


async def _seed_scheduled_twin(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    scheduled_at: datetime,
    schedule_id: UUID,
) -> None:
    """A pre-queued fold-1 twin job at *scheduled_at* — the delivery a
    fold-0-pass tick leaves behind for the next slot's later occurrence.
    Stamped with the schedule's provenance exactly the way ``_plan_fire``
    stamps the twins it enqueues, so the coverage walk recognises them as
    the schedule's own."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at, metadata) "
        f"VALUES ($1, $2, 'rt_queue', '{{}}'::jsonb, 5, 'transient', "
        f"'scheduled'::\"{schema}\".job_status, $3, "
        "jsonb_build_object('cron_schedule_id', $4::text))",
        new_uuid(),
        actor,
        scheduled_at,
        str(schedule_id),
    )


class TestSingletonDstOverlap:
    """The deferral claim, end to end: both occurrences of the repeated hour
    must reach the queue for a singleton actor — sequentially, not
    concurrently (the deviation's own terms)."""

    async def test_pre_overlap_tick_fires_catchup_only_and_defers_second(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The tick BEFORE the overlap (the deviation's stated shape): one
        catch-up fire, no second-occurrence enqueue, ``next_fire_at`` on the
        first occurrence, and the deferral logged naming the deferred
        instant."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="dst-pre-overlap",
            next_fire_at=due_slot,
            identity_key="dst-pre-overlap",
        )

        with structlog.testing.capture_logs() as captured:
            fired = await _tick(clean_pg_conn, settings, schema, _SINGLETON_POLICIES)

        assert fired == 1, "the catch-up slot fires; only the second occurrence is deferred"
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "a singleton pre-overlap fire must enqueue exactly one job — the deferred "
            "second occurrence may not sit active next to it"
        )
        has_flag: bool = await clean_pg_conn.fetchval(
            f"SELECT metadata @> '{{\"singleton\": true}}'::jsonb "  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the escaped jsonb literal.
            f'FROM "{schema}".jobs WHERE actor = $1',
            _SINGLETON_ACTOR,
        )
        assert has_flag is True, "the catch-up fire carries the singleton stamp"
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_FIRST_UTC, (
            "the deferred schedule advances to the FIRST occurrence — the instant its "
            "own later tick must fire"
        )
        assert row["last_fired_at"] is not None

        deferred = [e for e in captured if e["event"] == "singleton-dst-overlap-second-deferred"]
        assert len(deferred) == 1
        deferred_at = datetime.fromisoformat(deferred[0]["deferred_at"])
        assert deferred_at == _OVERLAP_SECOND_UTC, (
            "the deferral log must name the second occurrence's own instant — the "
            "promise the later ticks have to keep"
        )

    async def test_second_occurrence_fires_on_its_own_later_tick(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """THE attack. Across the full tick sequence — pre-overlap catch-up,
        the first occurrence's tick (unblocked), the second occurrence's
        tick (unblocked) — a singleton actor must end up with a job for BOTH
        occurrences of the repeated hour. Today the first occurrence's tick
        advances ``next_fire_at`` straight to next year, so the second
        occurrence never fires: the deferral promise is broken and the
        singleton actor silently loses a fire the ``allof`` strategy owes it
        (a behavior change against the non-singleton path, which pre-schedules
        the second occurrence in the same position)."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="dst-chain",
            next_fire_at=due_slot,
            identity_key="dst-chain",
        )

        # Tick 1 — pre-overlap, real wall clock: the catch-up fire, deferral.
        fired = await _tick(clean_pg_conn, settings, schema, _SINGLETON_POLICIES)
        assert fired == 1
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        await _settle_active_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR)

        # Tick 2 — the wall clock reaches the first occurrence (05:30Z); the
        # catch-up job is terminal, so the actor is free to fire it.
        second = await _tick(
            clean_pg_conn,
            settings,
            schema,
            _SINGLETON_POLICIES,
            due_as_of=_OVERLAP_FIRST_UTC + timedelta(minutes=1),
        )
        assert second == 1, "the first occurrence's own tick must fire it"
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 2
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_SECOND_UTC, (
            f"the first occurrence's tick advanced next_fire_at to {row['next_fire_at']} "
            "(next year) — the second occurrence of the repeated hour is unreachable "
            "from there, so the deferral promise is broken and that occurrence is "
            "silently lost for singleton actors"
        )
        await _settle_active_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR)

        # Tick 3 — the wall clock reaches the second occurrence (06:30Z); the
        # first occurrence's job is terminal, so the actor is free to fire it.
        third = await _tick(
            clean_pg_conn,
            settings,
            schema,
            _SINGLETON_POLICIES,
            due_as_of=_OVERLAP_SECOND_UTC + timedelta(minutes=1),
        )
        assert third == 1, (
            "the second occurrence never fired — a singleton actor on an 'allof' "
            "schedule loses the repeated hour's second occurrence entirely"
        )
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 3, (
            "catch-up fire + first occurrence + second occurrence: three jobs across "
            "the sequence, none concurrent"
        )
        final = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert final["next_fire_at"] == _NEXT_YEAR_UTC, (
            "after the second occurrence the schedule resumes its normal yearly cadence"
        )
        assert final["consecutive_failures"] == 0
        assert final["enabled"] is True

    async def test_suppressed_first_occurrence_tick_defers_to_second_occurrence(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The suppressed variant of the same claim: with the singleton
        blocker ACTIVE across the whole overlap, every tick suppresses (no
        strike, no stamp), and the suppressed first-occurrence tick advances
        to the second occurrence's instant — so the second-occurrence slot is
        skipped by SUPPRESSION (an honest, observable skip) rather than
        silently bypassed by a year-jumping advance."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await _seed_active_singleton_job(clean_pg_conn, schema)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="dst-suppressed",
            next_fire_at=due_slot,
            identity_key="dst-suppressed",
        )

        # Tick 1 — pre-overlap, blocked: suppressed, advanced to the first
        # occurrence (sequential catch-up without firing).
        first = await _tick(clean_pg_conn, settings, schema, _SINGLETON_POLICIES)
        assert first == 0
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_FIRST_UTC
        assert row["consecutive_failures"] == 0

        # Tick 2 — the first occurrence's tick, still blocked.
        second = await _tick(
            clean_pg_conn,
            settings,
            schema,
            _SINGLETON_POLICIES,
            due_as_of=_OVERLAP_FIRST_UTC + timedelta(minutes=1),
        )
        assert second == 0
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_SECOND_UTC, (
            f"a suppressed first-occurrence tick advanced next_fire_at to "
            f"{row['next_fire_at']} (next year) — the second-occurrence slot is never "
            "processed, so its skip is invisible instead of an honest suppression"
        )
        assert row["consecutive_failures"] == 0, "suppression is not a strike"
        assert row["last_fired_at"] is None

        # Tick 3 — the second occurrence's tick, still blocked: suppressed,
        # advances past the overlap, still no strike.
        third = await _tick(
            clean_pg_conn,
            settings,
            schema,
            _SINGLETON_POLICIES,
            due_as_of=_OVERLAP_SECOND_UTC + timedelta(minutes=1),
        )
        assert third == 0
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _NEXT_YEAR_UTC, (
            "the second-occurrence slot was never processed — with the correct "
            "deferral it is due here, suppresses against the still-active blocker, "
            "and advances past the overlap"
        )
        assert row["consecutive_failures"] == 0
        assert row["enabled"] is True
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "the blocker is the only job — no occurrence fired while it was active"
        )


class TestCappedDstOverlap:
    """The ``max_pending`` cap meets the DST ``allof`` pair: one plan can
    carry TWO enqueue args (the occurrence firing now plus the second
    occurrence of the repeated hour, future-scheduled) — a shape the client
    path can never produce, since a second sequential enqueue at the cap
    raises ``MaxPendingExceededError`` instead of landing both."""

    async def test_overlap_pair_beyond_capacity_is_trimmed_and_deferred(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A ``max_pending=1`` actor on the overlap schedule: the
        pre-overlap plan's pair (catch-up occurrence + second occurrence
        scheduled at 06:30Z) exceeds the cap by itself. The tick must
        enqueue only what fits (one job) and defer the dropped occurrence
        to its own instant via ``next_fire_at`` — delivered later once
        capacity frees, never past the cap, never silently lost."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _CAPPED_ACTOR)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_CAPPED_ACTOR,
            name="dst-capped",
            next_fire_at=due_slot,
            identity_key="dst-capped",
        )
        policies = {_CAPPED_ACTOR: ActorFirePolicy(max_pending=1)}

        with structlog.testing.capture_logs() as captured:
            first = await _tick(clean_pg_conn, settings, schema, policies)

        assert first == 1
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 1, (
            "the overlap pair alone exceeds max_pending=1 — enqueueing both puts two "
            "not-yet-running jobs past the cap the client path enforces"
        )
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_SECOND_UTC, (
            f"the dropped occurrence must be deferred to its own instant; got "
            f"{row['next_fire_at']} — an advance to the first occurrence loses the "
            "dropped second occurrence (its slot is never revisited)"
        )
        deferred = [e for e in captured if e["event"] == "max-pending-dst-overlap-deferred"]
        assert len(deferred) == 1
        assert datetime.fromisoformat(deferred[0]["deferred_at"]) == _OVERLAP_SECOND_UTC

        # Capacity frees and the wall clock reaches the deferred occurrence:
        # it fires at its own instant.
        await _settle_active_jobs(clean_pg_conn, schema, _CAPPED_ACTOR)
        second = await _tick(
            clean_pg_conn,
            settings,
            schema,
            policies,
            due_as_of=_OVERLAP_SECOND_UTC + timedelta(minutes=1),
        )
        assert second == 1, "the deferred occurrence must fire once capacity frees"
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 2
        final = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert final["next_fire_at"] == _NEXT_YEAR_UTC

    async def test_overlap_pair_within_capacity_lands_intact(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The capacity boundary: a ``max_pending=2`` actor's pair fits, so
        nothing is trimmed — both occurrences land (one pending, one
        scheduled at 06:30Z) and the schedule advances to the first
        occurrence, whose own tick then suppresses at the now-full cap
        instead of firing a third pending job."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _CAPPED_ACTOR)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_CAPPED_ACTOR,
            name="dst-capped-two",
            next_fire_at=due_slot,
            identity_key="dst-capped-two",
        )
        policies = {_CAPPED_ACTOR: ActorFirePolicy(max_pending=2)}

        first = await _tick(clean_pg_conn, settings, schema, policies)
        assert first == 1
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 2, (
            "capacity 2 admits the whole pair — no trim may drop an occurrence that fits"
        )
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_FIRST_UTC
        scheduled_at: datetime | None = await clean_pg_conn.fetchval(
            f'SELECT scheduled_at FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the actor is $-bound.
            "WHERE actor = $1 AND status = 'scheduled'::\"{}\".job_status".format(schema),
            _CAPPED_ACTOR,
        )
        assert scheduled_at == _OVERLAP_SECOND_UTC

        # The first occurrence's own tick: the pair occupies the full cap,
        # so the slot suppresses (an honest, observable skip).
        second = await _tick(
            clean_pg_conn,
            settings,
            schema,
            policies,
            due_as_of=_OVERLAP_FIRST_UTC + timedelta(minutes=1),
        )
        assert second == 0, "the pair occupies the cap — the first occurrence's slot suppresses"
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 2
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 0, "capacity backpressure is not a strike"
        assert row["next_fire_at"] == _NEXT_YEAR_UTC


class TestOpenActorDstControl:
    """The non-singleton control for the same tick sequence: both occurrences
    via the pre-scheduled enqueue (the path the singleton deviation deferred),
    so the parity bar is explicit — same fires delivered, different
    mechanism."""

    async def test_open_actor_first_occurrence_tick_advances_to_next_year(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A flagless-policy actor on the same overlap schedule: the
        pre-overlap tick pre-schedules the second occurrence; the first
        occurrence's own tick fires it and advances to next year (its second
        occurrence is already in the queue, so nothing is lost). This is the
        shape the singleton path must match in OUTCOME (both occurrences
        delivered) — pinned here so the singleton chain above can be judged
        against it."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="dst-open",
            next_fire_at=due_slot,
            identity_key="dst-open",
        )
        policies = {_OPEN_ACTOR: ActorFirePolicy()}

        first = await _tick(clean_pg_conn, settings, schema, policies)
        assert first == 1
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 2, (
            "catch-up fire (pending) + second occurrence (pre-scheduled)"
        )
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_FIRST_UTC

        second = await _tick(
            clean_pg_conn,
            settings,
            schema,
            policies,
            due_as_of=_OVERLAP_FIRST_UTC + timedelta(minutes=1),
        )
        assert second == 1, "the first occurrence's own tick fires it"
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 3
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _NEXT_YEAR_UTC, (
            "the open actor's second occurrence is already pre-scheduled, so its "
            "first-occurrence tick advances a full year — the singleton path's "
            "one-cadence advance is what differs, not the outcome"
        )
        scheduled_at: datetime | None = await clean_pg_conn.fetchval(
            f'SELECT scheduled_at FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the actor is $-bound.
            "WHERE actor = $1 AND status = 'scheduled'::\"{}\".job_status".format(schema),
            _OPEN_ACTOR,
        )
        assert scheduled_at == _OVERLAP_SECOND_UTC, (
            "the pre-scheduled second occurrence carries its own instant, verbatim"
        )

        third = await _tick(
            clean_pg_conn,
            settings,
            schema,
            policies,
            due_as_of=_OVERLAP_SECOND_UTC + timedelta(minutes=1),
        )
        assert third == 0, "nothing is due at the second occurrence — its job is queued"


class TestNonSingletonAllofOutageInsideOverlap:
    """The outage scenario for the general (non-singleton) path: a leader
    outage spanning the fold-0 instant leaves ``next_fire_at`` ON the fold-0
    occurrence with no pre-scheduled twin — the tick that would have created
    it never ran.  The post-outage tick fires the fold-0 occurrence and must
    advance to the fold-1 occurrence, not a year: ``allof`` owes the later
    occurrence of a repeated hour."""

    async def test_outage_seed_on_first_occurrence_fires_both_across_two_ticks(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        schedule_id = await _seed_overlap_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="outage-fold0",
            next_fire_at=_OVERLAP_FIRST_UTC,
            identity_key="outage-fold0",
        )
        no_policies: Mapping[str, ActorFirePolicy] = {}

        # The post-outage tick: the wall clock has reached the fold-0
        # occurrence, and the schedule is due exactly there.
        first = await _tick(
            clean_pg_conn,
            settings,
            schema,
            no_policies,
            due_as_of=_OVERLAP_FIRST_UTC + timedelta(minutes=1),
        )
        assert first == 1, "the fold-0 occurrence fires on the post-outage tick"
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _OVERLAP_SECOND_UTC, (
            f"the post-outage tick advanced next_fire_at to {row['next_fire_at']} "
            "— under allof the fold-1 occurrence is still owed and must be the "
            "next fire, or it is silently lost for a year"
        )
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 1

        # The wall clock reaches the fold-1 occurrence: it fires on its own
        # tick (nothing pre-scheduled it — this tick IS its delivery).
        second = await _tick(
            clean_pg_conn,
            settings,
            schema,
            no_policies,
            due_as_of=_OVERLAP_SECOND_UTC + timedelta(minutes=1),
        )
        assert second == 1, "the fold-1 occurrence must fire on its own tick"
        final = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert final["next_fire_at"] == _NEXT_YEAR_UTC, (
            "from the fold-1 occurrence both members of the pair are consumed; "
            f"the next slot is {_NEXT_YEAR_UTC}, got {final['next_fire_at']}"
        )
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 2, (
            "both occurrences of the repeated hour delivered, one job each"
        )


class TestSteadyStateMinutelyFoldTraversal:
    """FINDING (red until fixed): the fold-1 routing branch breaks the
    steady-state fold traversal it was meant to complete.

    A minutely ``allof`` schedule crossing the fall-back fold owes every
    slot twice — once per pass.  The fold-1 pass is delivered by the twin
    chain: during the fold-0 pass each tick pre-schedules a twin job for
    the NEXT slot's fold-1 occurrence (the pair answer's second member), so
    by the time the last fold-0 slot fires, every fold-1 slot already has a
    queued job.  The schedule's next fire after the last fold-0 slot is
    therefore the slot PAST the repeated range (02:00 local = 07:00 UTC).
    ``_skip_already_delivered_overlap_twins`` exists precisely to advance
    past such covered instants — but at the time it only engaged when the
    computation's answer equalled the row's OWN twin.

    The newest branch answers the fold-1 pass's FIRST match once the fold-0
    pass is spent — for a minutely schedule that is 01:00 fold-1 (06:00
    UTC), which was never the current slot's twin, so the skip never
    engaged.  The fix rewrote the skip as a coverage-prefix walk: any plan
    landing inside a repeated range advances past the instants the actor's
    own queued jobs already cover.  Measured on this exact drive while
    red: the
    06:00 UTC tick fires the schedule for a slot whose twin job is already
    queued (double delivery), the walk from the fold-1 seed then answers
    with a pair whose first member is IN THE PAST, and from there the
    schedule sits due every tick, re-firing already-played fold-0 slots and
    double-scheduling every twin (duplicate jobs at 06:01, 06:02, ...).
    """

    async def test_minutely_schedule_crosses_the_fold_without_double_firing(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="steady-fold-minutely",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_FOLD0_SEED_UTC,
            identity_key="steady-fold-minutely",
        )
        no_policies: Mapping[str, ActorFirePolicy] = {}

        # The fold-0 pass: 04:58..05:59 UTC fires slots 00:58..01:59 fold-0.
        # From 04:59 on, each tick also pre-schedules the next slot's fold-1
        # twin, so by the end all 60 fold-1 slots are twin-covered.
        due = _FOLD0_SEED_UTC
        fired_total = 0
        while due <= _LAST_FOLD0_TICK_UTC:
            fired_total += await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
            due += timedelta(minutes=1)
        assert fired_total == 62, (
            f"the fold-0 pass fires every slot 00:58..01:59 exactly once, got {fired_total}"
        )
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 122, (
            "62 fold-0 fires + 60 pre-scheduled fold-1 twins — every owed slot "
            "through the repeated hour now has exactly one job"
        )

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] == _AFTER_RANGE_UTC, (
            f"the last fold-0 slot is spent and every fold-1 slot is twin-covered, "
            f"so the schedule owes nothing until 02:00 local ({_AFTER_RANGE_UTC}); "
            f"got {row['next_fire_at']}"
        )

        # The fold-1 pass ticks: the twins deliver those slots, the schedule
        # must NOT fire them a second time.
        for due in _FOLD1_TICKS_UTC:
            fired = await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
            assert fired == 0, (
                f"the {due.isoformat()} tick must not fire the schedule — every "
                "fold-1 slot is already delivered by its pre-scheduled twin"
            )
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 122, (
            "the fold-1 pass adds no jobs — re-firing a twin-covered slot is double delivery"
        )
        duplicates = await clean_pg_conn.fetch(
            f'SELECT scheduled_at, count(*) AS n FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the fixture identifier.
            "WHERE actor = $1 "
            "  AND scheduled_at >= '2026-11-01 04:00+00'::timestamptz "
            "  AND scheduled_at <  '2026-11-01 08:00+00'::timestamptz "
            "GROUP BY scheduled_at HAVING count(*) > 1",
            _OPEN_ACTOR,
        )
        assert duplicates == [], (
            "no instant inside the repeated hour may hold two jobs — duplicates "
            f"mean a twin-covered slot was scheduled again: {duplicates}"
        )


class TestPartialTwinCoverageFoldHandoff:
    """The partial-coverage pin the round-4 triage deferred to the fix:
    some fold-1 slots twin-covered, some not — the handoff must deliver
    the uncovered remainder exactly once and never re-deliver a covered
    instant, whichever mechanism carries each slot.

    The partial state is seeded directly: the schedule sits mid fold-1
    pass at 01:00 fold-1 with pre-queued twin jobs covering 01:05..01:59
    — the state a mid-pass leader outage with a beyond-catch-up-window
    gap leaves behind (the harness cannot produce that gap naturally:
    its real clock precedes the November overlap, so no slot is ever
    beyond the window relative to the server clock the tick reads).  The
    assertions are invariant-strength and ordering-agnostic: every owed
    occurrence lands exactly once, no instant holds two jobs, and
    ``next_fire_at`` is strictly monotone across the whole traversal.
    """

    async def test_uncovered_remainder_fires_and_covered_slots_never_refire(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="partial-fold",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_FOLD1_TICKS_UTC[0],
            identity_key="partial-fold",
        )
        covered_start_utc = datetime(2026, 11, 1, 6, 5, tzinfo=UTC)  # 01:05 fold-1
        covered_utc = [
            covered_start_utc + timedelta(minutes=i)
            for i in range(55)  # 01:05..01:59
        ]
        for instant in covered_utc:
            await _seed_scheduled_twin(clean_pg_conn, schema, _OPEN_ACTOR, instant, schedule_id)
        no_policies: Mapping[str, ActorFirePolicy] = {}

        fired_total = 0
        prev_next: datetime = _FOLD1_TICKS_UTC[0]
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        for minute in range(60):
            due = _FOLD1_TICKS_UTC[0] + timedelta(minutes=minute)
            fired = await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
            row = await schedule_row(clean_pg_conn, schema, schedule_id)
            assert row["next_fire_at"] >= prev_next, (
                f"next_fire_at must never move backwards across the fold-1 traversal: "
                f"{prev_next.isoformat()} -> {row['next_fire_at'].isoformat()} at tick {due.isoformat()}"
            )
            assert row["next_fire_at"] > due, (
                f"after the {due.isoformat()} tick the schedule sits due in the past "
                f"(next_fire_at={row['next_fire_at']}) — it would re-fire an "
                "already-delivered slot on every later tick"
            )
            prev_next = row["next_fire_at"]
            if due < covered_start_utc:
                assert fired == 1, (
                    f"the {due.isoformat()} tick owes the uncovered fold-1 occurrence — "
                    "an owed occurrence may never be silently skipped"
                )
                fired_total += 1
            else:
                assert fired == 0, (
                    f"the {due.isoformat()} tick fired a twin-covered instant — "
                    "re-delivering a covered occurrence is double delivery"
                )
        assert row["next_fire_at"] == _AFTER_RANGE_UTC, (
            "the covered prefix is twin-delivered and the uncovered remainder spent, "
            f"so the schedule owes nothing until {_AFTER_RANGE_UTC}; got {row['next_fire_at']}"
        )
        assert fired_total == 5, "the uncovered remainder is 01:00..01:04 fold-1, five fires"
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 60, (
            "5 schedule-delivered occurrences + 55 pre-queued twins — every fold-1 "
            "occurrence of the repeated range delivered exactly once"
        )
        duplicates = await clean_pg_conn.fetch(
            f'SELECT scheduled_at, count(*) AS n FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the fixture identifier.
            "WHERE actor = $1 "
            "  AND scheduled_at >= '2026-11-01 04:00+00'::timestamptz "
            "  AND scheduled_at <  '2026-11-01 08:00+00'::timestamptz "
            "GROUP BY scheduled_at HAVING count(*) > 1",
            _OPEN_ACTOR,
        )
        assert duplicates == [], (
            f"no instant inside the repeated range may hold two jobs: {duplicates}"
        )


class TestSingletonMinutelyFoldTraversal:
    """FINDING (red until fixed): the singleton twin-override loses the
    fold-1 pass of a multi-match repeated hour.

    Singletons are excluded from the twin chain (``_plan_fire`` enqueues
    no second job for them — two singleton-flagged jobs cannot sit active
    at once without tripping ``jobs_singleton_uniq``), so every fold-1
    occurrence must be delivered by the schedule's own later ticks.  The
    fold-1 routing branch answers exactly that: from the spent last
    fold-0 slot it returns the fold-1 pass's first match (01:00 fold-1 =
    06:00 UTC).  But the singleton ``elif`` then in ``_plan_fire`` (since
    deleted — the computation now owns the fold handoff) overrode
    ``next_fire`` with the fired slot's OWN twin (01:59 fold-1 = 06:59
    UTC) whenever the fired slot was any fold-0 occurrence of a repeated
    wall time.  Its premise — "the next slot the walk finds is a full
    cadence away (for a yearly schedule, a year)" — held only for
    single-match hours: on a minutely schedule the walk had already found
    the fold-1 pass, and the override discarded it.

    Measured on this exact drive while red: after the last
    fold-0 tick the schedule lands on 06:59 UTC, the 06:00..06:58 ticks
    fire nothing, and 59 of the 60 fold-1 occurrences are silently lost —
    only 01:59 fold-1 ever fires.  Under-delivery, the mirror of the open
    actor's over-delivery pinned in the class above.
    """

    async def test_singleton_minutely_schedule_fires_every_fold_occurrence_sequentially(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="singleton-fold-minutely",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_FOLD0_SEED_UTC,
            identity_key="singleton-fold-minutely",
        )

        # Every minute from the seed through 02:00 local owes exactly one
        # occurrence: the fold-0 pass, the fold-1 pass, and the slot past
        # the range are contiguous minutely slots.  Singleton delivery is
        # sequential, so each tick's job is settled before the next — the
        # real lifecycle event that frees the actor.
        due = _FOLD0_SEED_UTC
        fired_total = 0
        while due <= _AFTER_RANGE_UTC:
            await _settle_active_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR)
            fired = await _tick(clean_pg_conn, settings, schema, _SINGLETON_POLICIES, due_as_of=due)
            assert fired == 1, (
                f"the {due.isoformat()} tick owes exactly one occurrence — an owed "
                "occurrence may never be silently skipped"
            )
            row = await schedule_row(clean_pg_conn, schema, schedule_id)
            assert row["next_fire_at"] > due, (
                f"after the {due.isoformat()} tick the schedule sits due in the past "
                f"(next_fire_at={row['next_fire_at']}) — it would re-fire an "
                "already-played slot on every later tick"
            )
            if due == _LAST_FOLD0_TICK_UTC:
                assert row["next_fire_at"] == _FOLD1_TICKS_UTC[0], (
                    "the last fold-0 slot is spent; the next owed occurrence is the "
                    f"fold-1 pass's first match ({_FOLD1_TICKS_UTC[0]}) — singletons "
                    f"have no twin chain to deliver it — got {row['next_fire_at']}"
                )
            fired_total += fired
            due += timedelta(minutes=1)

        assert fired_total == 123, (
            "62 fold-0 + 60 fold-1 + 1 past-the-range occurrence, each exactly once"
        )
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 123


class TestTwinCoverageIsPerSchedule:
    """FINDING (coverage round 5, fixed): the coverage walk counted ANY
    job at an in-range instant as delivered — including another
    schedule's.

    ``_skip_already_delivered_overlap_twins`` scoped its coverage query
    to the ACTOR (``j.actor = a.actor``), not to the schedule whose plan
    it was adjusting — yet twins are per-schedule by construction: each
    schedule's fold-0 ticks pre-schedule THEIR OWN next-slot twins.
    Two schedules on one actor are independent everywhere else in the
    system — both fire the same instant and both jobs coexist (no
    dedup) — so each owes its own delivery of every occurrence.

    The composition that exposed the granularity: schedule A crosses
    the fold normally and holds a twin at every fold-1 slot; schedule
    B, same actor and same expression, is seeded on the LAST fold-0
    slot — it has no twins and owes the entire fold-1 pass.  Measured
    on this exact drive while red: at the last fold-0 tick B's
    ``next_fire_at`` jumped from 06:00 straight to 07:00 UTC — the walk
    took A's twins as B's coverage — the whole fold-1 pass fired
    nothing for B, and B delivered 2 jobs where it owed 62: 60
    occurrences silently lost, stolen by a neighbour's twin chain.

    The fix: every cron-fired job (fire and twin alike) is stamped with
    ``metadata['cron_schedule_id']`` and the walk's coverage query joins
    on that stamp.  ``identity_key`` could not serve as the scope — it
    defaults to NULL and is a user-facing dedup handle shared with
    on-demand jobs, which are not the schedule's delivery.
    """

    async def test_one_schedules_twins_never_cover_another_schedules_owed_pass(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        id_a = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="fold-granularity-a",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_FOLD0_SEED_UTC,
            identity_key="fold-granularity-a",
        )
        id_b = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="fold-granularity-b",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            # B's first slot is the LAST fold-0 slot: no earlier tick, so
            # no twin chain — B owes the whole fold-1 pass itself.
            next_fire_at=_LAST_FOLD0_TICK_UTC,
            identity_key="fold-granularity-b",
        )

        async def b_jobs() -> int:
            return await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the fixture identifier.
                "WHERE actor = $1 AND identity_key = 'fold-granularity-b'",
                _OPEN_ACTOR,
            )

        no_policies: Mapping[str, ActorFirePolicy] = {}
        due = _FOLD0_SEED_UTC
        prev_b = 0
        while due <= _AFTER_RANGE_UTC:
            await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
            row_b = await schedule_row(clean_pg_conn, schema, id_b)
            if due >= _LAST_FOLD0_TICK_UTC:
                current_b = await b_jobs()
                assert current_b == prev_b + 1, (
                    f"the {due.isoformat()} tick owes schedule B exactly one "
                    "occurrence — B has no twins of its own, so an owed "
                    "occurrence may never be silently skipped"
                )
                prev_b = current_b
                assert row_b["next_fire_at"] > due, (
                    f"after the {due.isoformat()} tick B sits due in the past "
                    f"(next_fire_at={row_b['next_fire_at']})"
                )
            if due == _LAST_FOLD0_TICK_UTC:
                assert row_b["next_fire_at"] == _FOLD1_TICKS_UTC[0], (
                    "B just fired the last fold-0 slot and holds NO twins — its "
                    "next owed occurrence is the fold-1 pass's first match "
                    f"({_FOLD1_TICKS_UTC[0]}); only A's twin chain is in the "
                    f"range, and A's coverage is not B's — got "
                    f"{row_b['next_fire_at']}"
                )
            due += timedelta(minutes=1)

        assert prev_b == 62, (
            "B owes 62 occurrences from its seed: the last fold-0 slot, the "
            "60-slot fold-1 pass, and the slot past the range"
        )
        # A is unaffected: its own full twin chain carries its fold-1 pass.
        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the fixture identifier.
                "WHERE actor = $1 AND identity_key = 'fold-granularity-a'",
                _OPEN_ACTOR,
            )
            == 123
        )
        row_a = await schedule_row(clean_pg_conn, schema, id_a)
        assert row_a["next_fire_at"] == datetime(2026, 11, 1, 7, 1, tzinfo=UTC)

    async def test_default_null_identity_key_schedules_are_independent_too(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The theft test above uses NAMED identity keys; the default is
        NULL (the ``create_schedule`` signature defaults
        ``identity_key=None``, and multiple schedules per actor are a
        documented feature).  This is the discriminating case: a
        regression that scoped the coverage walk on ``identity_key IS NOT
        DISTINCT FROM`` would pass the named-key test (the keys differ)
        while still stealing here (NULL matches NULL).  Only the
        ``cron_schedule_id`` provenance stamp keeps default-configured
        schedules independent — this test is what pins the stamp's
        existence, not just the per-actor scope's absence.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        id_a = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="fold-null-a",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_FOLD0_SEED_UTC,
        )
        id_b = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="fold-null-b",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_LAST_FOLD0_TICK_UTC,
        )

        no_policies: Mapping[str, ActorFirePolicy] = {}
        due = _FOLD0_SEED_UTC
        prev_b = 0
        while due <= _AFTER_RANGE_UTC:
            await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
            if due >= _LAST_FOLD0_TICK_UTC:
                current_b = await _count_schedule_jobs(clean_pg_conn, schema, _OPEN_ACTOR, id_b)
                assert current_b == prev_b + 1, (
                    f"the {due.isoformat()} tick owes default-configured schedule "
                    "B exactly one occurrence — A's NULL-identity twins are not "
                    "B's coverage"
                )
                prev_b = current_b
            if due == _LAST_FOLD0_TICK_UTC:
                row_b = await schedule_row(clean_pg_conn, schema, id_b)
                assert row_b["next_fire_at"] == _FOLD1_TICKS_UTC[0], (
                    "B holds no twins of its own; its next owed occurrence is "
                    f"{_FOLD1_TICKS_UTC[0]} — got {row_b['next_fire_at']}"
                )
            due += timedelta(minutes=1)

        assert prev_b == 62
        assert await _count_schedule_jobs(clean_pg_conn, schema, _OPEN_ACTOR, id_a) == 123

    async def test_ondemand_jobs_at_inrange_instants_are_not_coverage(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Only jobs THIS schedule fired count as its coverage.  On-demand
        jobs — same actor, scheduled at instants inside the repeated
        range, carrying no cron provenance — are not the schedule's
        delivery: the schedule owes its occurrences regardless of what
        else the actor has queued.  This pins the producer-class half of
        the contract against a future widening of the walk's scope (e.g.
        a "legacy compatibility" clause matching unstamped jobs): under
        the pre-fix actor-wide scope these 60 on-demand jobs would steal
        the schedule's entire fold-1 pass.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _OPEN_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_OPEN_ACTOR,
            name="fold-ondemand-neighbour",
            cron_expr=_MINUTELY,
            timezone=_OVERLAP_TZ,
            dst_strategy="allof",
            next_fire_at=_LAST_FOLD0_TICK_UTC,
        )
        # 60 on-demand jobs, one per fold-1 instant, unstamped, each with
        # its own business identity key — everything the old actor-wide
        # query counted as delivered.
        for i in range(60):
            await clean_pg_conn.execute(
                f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
                "(id, actor, queue, payload, max_attempts, retry_kind, status, "
                "scheduled_at, identity_key, metadata) "
                f"VALUES ($1, $2, 'rt_queue', '{{}}'::jsonb, 5, 'transient', "
                f"'scheduled'::\"{schema}\".job_status, $3, $4, '{{}}'::jsonb)",
                new_uuid(),
                _OPEN_ACTOR,
                _FOLD1_TICKS_UTC[0] + timedelta(minutes=i),
                f"ondemand-{i}",
            )

        no_policies: Mapping[str, ActorFirePolicy] = {}
        due = _LAST_FOLD0_TICK_UTC
        prev = 0
        while due <= _AFTER_RANGE_UTC:
            await _tick(clean_pg_conn, settings, schema, no_policies, due_as_of=due)
            current = await _count_schedule_jobs(clean_pg_conn, schema, _OPEN_ACTOR, schedule_id)
            assert current == prev + 1, (
                f"the {due.isoformat()} tick owes the schedule exactly one "
                "occurrence — on-demand jobs at the same instants are not its "
                "delivery"
            )
            prev = current
            if due == _LAST_FOLD0_TICK_UTC:
                row = await schedule_row(clean_pg_conn, schema, schedule_id)
                assert row["next_fire_at"] == _FOLD1_TICKS_UTC[0], (
                    "the schedule holds no twins; its next owed occurrence is "
                    f"{_FOLD1_TICKS_UTC[0]} — got {row['next_fire_at']}"
                )
            due += timedelta(minutes=1)

        assert prev == 62, (
            "the schedule owes 62 occurrences from its seed — every one must "
            "fire despite 60 on-demand jobs sitting at the fold-1 instants"
        )
        assert await count_jobs(clean_pg_conn, schema, _OPEN_ACTOR) == 62 + 60, (
            "the on-demand jobs are untouched: the schedule's fires coexist "
            "with them, no dedup, no displacement"
        )
