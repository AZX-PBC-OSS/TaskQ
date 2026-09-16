# ruff: noqa: S608  # Why: every interpolated identifier is the module fixture's generated schema name, validated by _IDENT_RE inside build_leader_lease_sql; all values are $-bound.
"""Contract tests for the maintenance-leader lease statements.

Leadership failover is exactly three statements — elect, renew, resign —
so their predicates are pinned against a real database rather than
trusted from reading.  The properties pinned here:

* An unclaimed role is always winnable.  Whether the role is claimable
  is a property of the row alone (absent, or both of its liveness
  signals lapsed) — never of anything a session can hold.  A candidate
  that dies between taking the courtesy advisory lock and writing the
  row, and a leader that resigns while its own session outlives the
  delete, both leave the lock held with no row behind it; the fleet must
  still elect, on a horizon TaskQ controls, with no privilege beyond
  writing its own tables.
* A live holder is never displaced: an unexpired ``expires_at`` the
  holder chose keeps the row, and so does a ``last_seen_at`` still being
  pinged on a row written without a lease (a holder from a release that
  predates the column names only four columns in its upsert).
* A lapsed holder is always displaceable: once the lease has passed and
  the ping has stopped, any pod takes the row, and concurrent takers get
  exactly one winner — the loser's conflict update re-evaluates against
  the winner's fresh row and matches nothing.
* Renew and resign are fenced on the holder's term
  (``worker_id``, ``elected_at``): a deposed holder's late statements
  never touch a successor's row, and a lease already lapsed at the
  server cannot be renewed — the previous holder re-elects through the
  same statement every peer runs, with no advantage.

The loop-level behaviour built on these statements (renewal cadence,
trust-window step-down, resign-on-shutdown handover) is pinned in
tests/test_leader_integration.py; the two-schema election shape in
tests/test_rt_worker_election_schema.py.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_uuid
from taskq.constants import schema_lock_name
from taskq.testing.fixtures import ModulePgSchema, _create_worker
from taskq.worker.leader import build_leader_lease_sql

pytestmark = pytest.mark.integration

# Generous horizons: these tests pin WHICH rows the predicates accept, not
# how time passes — the lapsed states are arranged by backdating, never by
# sleeping.
_LEASE_SECS = 3600.0
_STALE_AFTER_SECS = 3600.0


@pytest_asyncio.fixture
async def lease_conn(module_pg_schema: ModulePgSchema) -> AsyncIterator[asyncpg.Connection]:
    """A raw connection over a maintenance_leader table proven empty.

    The module schema is shared across this file's tests, so the row is
    deleted up front and the emptiness asserted — a leftover row turning
    an INSERT into an UPDATE would let every assertion here pass for the
    wrong reason.
    """
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DELETE FROM "{schema}".maintenance_leader')
        leftover = await conn.fetchval(f'SELECT count(*) FROM "{schema}".maintenance_leader')
        assert leftover == 0, f"test isolation broken: {leftover} leader rows at setup"
        yield conn
    finally:
        await conn.close()


async def _elect(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    *,
    lease_secs: float = _LEASE_SECS,
    stale_after_secs: float = _STALE_AFTER_SECS,
) -> datetime | None:
    """Run the production elect statement; the term's ``elected_at`` when won.

    Zero rows back (a ``None`` here) is the ordinary follower state: the
    recorded holder's signals are both still live.
    """
    elect_sql, _, _ = build_leader_lease_sql(schema)
    return await conn.fetchval(elect_sql, worker_id, lease_secs, stale_after_secs)


async def _renew(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    elected_at: datetime,
    *,
    lease_secs: float = _LEASE_SECS,
) -> datetime | None:
    """Run the production renew statement; the new ``expires_at`` when held."""
    _, renew_sql, _ = build_leader_lease_sql(schema)
    return await conn.fetchval(renew_sql, worker_id, elected_at, lease_secs)


async def _resign(
    conn: asyncpg.Connection, schema: str, worker_id: UUID, elected_at: datetime
) -> str:
    """Run the production resign statement; the command tag (``DELETE n``)."""
    _, _, resign_sql = build_leader_lease_sql(schema)
    return await conn.execute(resign_sql, worker_id, elected_at)


async def _leader_row(conn: asyncpg.Connection, schema: str) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f"SELECT worker_id, elected_at, last_seen_at, expires_at "
        f'FROM "{schema}".maintenance_leader WHERE singleton = true'
    )


async def _backdate_holder(
    conn: asyncpg.Connection, schema: str, *, stale_after_secs: float
) -> None:
    """Age the recorded holder past both takeover horizons in one write.

    Backdating rather than waiting: the predicates read the server's clock,
    so a row written an hour ago is exactly the state a silent holder
    reaches an hour later, without the test paying the hour.
    """
    await conn.execute(
        f'UPDATE "{schema}".maintenance_leader SET '
        f"expires_at = clock_timestamp() - interval '1 hour', "
        f"last_seen_at = clock_timestamp() - interval '1 hour' - make_interval(secs => $1) "
        f"WHERE singleton = true",
        stale_after_secs,
    )


async def test_unclaimed_role_is_winnable_while_another_session_holds_the_courtesy_lock(
    module_pg_schema: ModulePgSchema,
    lease_conn: asyncpg.Connection,
) -> None:
    """The empty-row path must never consult anything a session can hold.

    The deadlock this pins: the row is ABSENT (a resigning leader's delete
    landed, or a candidate died between its lock attempt and its election
    write) while the courtesy advisory lock stays held by a session the
    fleet cannot reap — a partitioned peer, or simply a departed leader
    whose connection outlives its row.  If claimability consulted the
    lock, every pod would observe it held and no pod could elect until
    the server's connection reaping fired (stock keepalives: hours).  The
    row being absent is all the evidence the role is free.
    """
    schema = module_pg_schema.schema_name
    candidate = new_uuid()
    await _create_worker(lease_conn, schema, candidate)

    # A session the fleet does not control holds the schema's courtesy lock
    # — the shape a dead-without-FIN candidate or a slow-to-exit resigning
    # leader leaves behind.
    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        held = await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            schema_lock_name("maintenance_leader", schema),
        )
        assert held is True, "test setup: the courtesy lock must be held by the stranger"

        elected_at = await _elect(lease_conn, schema, candidate)
        assert elected_at is not None, (
            "the role is unclaimed — the row is absent — so the elect must land no "
            "matter what any session holds; gating the insert on the courtesy lock "
            "leaves the whole fleet unelectable until the server reaps a session it "
            "may never reap"
        )
        row = await _leader_row(lease_conn, schema)
        assert row is not None and row["worker_id"] == candidate
        fresh = await lease_conn.fetchval(
            f'SELECT expires_at > clock_timestamp() FROM "{schema}".maintenance_leader '
            f"WHERE singleton = true"
        )
        assert fresh is True, "a won election stamps the lease the holder chose"
    finally:
        await holder.close()


async def test_live_lease_is_not_takeable(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """A holder whose lease has not passed keeps the role; a taker gets nothing."""
    schema = module_pg_schema.schema_name
    holder, taker = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, holder)
    await _create_worker(lease_conn, schema, taker)

    elected_at = await _elect(lease_conn, schema, holder)
    assert elected_at is not None, "test setup: the unclaimed role must be winnable"
    before = await _leader_row(lease_conn, schema)
    assert before is not None

    took = await _elect(lease_conn, schema, taker)
    assert took is None, "a live lease must never be taken over — that is two leaders"

    after = await _leader_row(lease_conn, schema)
    assert after is not None
    assert after["worker_id"] == holder, "the failed takeover must not move the row"
    assert after["elected_at"] == before["elected_at"], (
        "the failed takeover must not rewrite the holder's term"
    )
    assert after["expires_at"] == before["expires_at"], (
        "the failed takeover must not touch the holder's expiry"
    )


async def test_lapsed_lease_is_takeable(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """Once the lease has passed and the ping stopped, any pod takes the row."""
    schema = module_pg_schema.schema_name
    holder, taker = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, holder)
    await _create_worker(lease_conn, schema, taker)

    elected_at = await _elect(lease_conn, schema, holder)
    assert elected_at is not None
    await _backdate_holder(lease_conn, schema, stale_after_secs=_STALE_AFTER_SECS)

    took = await _elect(lease_conn, schema, taker)
    assert took is not None, "a holder whose lease and ping both stopped must lose the row"
    assert took != elected_at, "the takeover begins a new term"

    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == taker
    fresh = await lease_conn.fetchval(
        f'SELECT expires_at > clock_timestamp() FROM "{schema}".maintenance_leader '
        f"WHERE singleton = true"
    )
    assert fresh is True, "the taker's own lease starts with the takeover"


async def test_pre_lease_row_is_held_by_its_ping_and_takeable_once_the_ping_stops(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """A row carrying no lease (``expires_at IS NULL``) is judged on its ping alone.

    Pods from a release without the lease column write four columns, so
    the ping is the only liveness signal such a row can have: while it is
    fresh the holder is alive and must keep the role, and once it stops
    the role is recoverable on the staleness slack.
    """
    schema = module_pg_schema.schema_name
    pre_lease, taker = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, pre_lease)
    await _create_worker(lease_conn, schema, taker)

    # The previous release's upsert shape: four columns, no expires_at.
    await lease_conn.execute(
        f'INSERT INTO "{schema}".maintenance_leader (singleton, worker_id, elected_at, last_seen_at) '
        f"VALUES (true, $1, clock_timestamp(), clock_timestamp())",
        pre_lease,
    )

    took = await _elect(lease_conn, schema, taker)
    assert took is None, (
        "a pinging pre-lease holder must keep the role — judging the row on its "
        "(absent) lease alone would depose a live pod mid-roll"
    )
    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == pre_lease

    await lease_conn.execute(
        f'UPDATE "{schema}".maintenance_leader SET '
        f"last_seen_at = clock_timestamp() - interval '1 hour' - make_interval(secs => $1) "
        f"WHERE singleton = true",
        _STALE_AFTER_SECS,
    )
    took = await _elect(lease_conn, schema, taker)
    assert took is not None, "once the ping stops, the role is recoverable on the slack"
    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == taker
    assert row["expires_at"] is not None, "the takeover stamps the taker's own lease"


async def test_renew_is_fenced_on_the_term(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """Renew lands only for the recorded holder presenting its own term."""
    schema = module_pg_schema.schema_name
    holder, other = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, holder)
    await _create_worker(lease_conn, schema, other)

    elected_at = await _elect(lease_conn, schema, holder)
    assert elected_at is not None
    before = await _leader_row(lease_conn, schema)
    assert before is not None

    renewed = await _renew(lease_conn, schema, other, elected_at)
    assert renewed is None, "another worker's id must not renew this term"
    renewed = await _renew(lease_conn, schema, holder, datetime(2000, 1, 1, tzinfo=UTC))
    assert renewed is None, "a term token that is not the row's must not renew it"
    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["expires_at"] == before["expires_at"], (
        "a refused renewal must not move the expiry"
    )

    renewed = await _renew(lease_conn, schema, holder, elected_at)
    assert renewed is not None, "the holder presenting its own term renews"
    assert renewed > before["expires_at"], "a renewal extends the lease"


async def test_renew_does_not_revive_a_lapsed_lease(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """A lease already past at the server refuses renewal even to its holder.

    The holder must re-elect through the same statement every peer runs,
    so being the previous holder confers no advantage — and a pod that
    wakes from a long suspension cannot quietly extend a role a peer may
    already be taking.
    """
    schema = module_pg_schema.schema_name
    holder = new_uuid()
    await _create_worker(lease_conn, schema, holder)

    elected_at = await _elect(lease_conn, schema, holder)
    assert elected_at is not None
    await _backdate_holder(lease_conn, schema, stale_after_secs=_STALE_AFTER_SECS)

    renewed = await _renew(lease_conn, schema, holder, elected_at)
    assert renewed is None, "a lapsed lease is not renewable — the holder must re-elect"


async def test_resign_is_fenced_on_the_term(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """Resign deletes only the resignment's own term.

    A resign issued late — after a takeover — must not delete the
    successor's row, or a graceful shutdown would hand the fleet a fresh
    no-leader window.
    """
    schema = module_pg_schema.schema_name
    holder, other = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, holder)
    await _create_worker(lease_conn, schema, other)

    elected_at = await _elect(lease_conn, schema, holder)
    assert elected_at is not None

    tag = await _resign(lease_conn, schema, other, elected_at)
    assert tag == "DELETE 0", "another worker's id must not delete this term"
    tag = await _resign(lease_conn, schema, holder, datetime(2000, 1, 1, tzinfo=UTC))
    assert tag == "DELETE 0", "a stale term token must not delete the successor's row"
    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == holder, (
        "the fenced-out resigns must leave the row standing"
    )

    tag = await _resign(lease_conn, schema, holder, elected_at)
    assert tag == "DELETE 1", "the holder presenting its own term resigns"
    assert await _leader_row(lease_conn, schema) is None


async def test_concurrent_takers_on_an_unclaimed_row_get_exactly_one_winner(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """Two pods electing at once over an empty row: exactly one row comes back."""
    schema = module_pg_schema.schema_name
    first, second = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, first)
    await _create_worker(lease_conn, schema, second)

    conn_b = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        won_a, won_b = await asyncio.gather(
            _elect(lease_conn, schema, first),
            _elect(conn_b, schema, second),
        )
    finally:
        await conn_b.close()

    winners = [w for w in (won_a, won_b) if w is not None]
    assert len(winners) == 1, (
        f"exactly one taker may win an unclaimed row; both returned a term: {won_a=}, {won_b=}"
    )
    row = await _leader_row(lease_conn, schema)
    assert row is not None
    expected = first if won_a is not None else second
    assert row["worker_id"] == expected, "the row names the taker that won"
    count = await lease_conn.fetchval(f'SELECT count(*) FROM "{schema}".maintenance_leader')
    assert count == 1, "the role is a singleton — two rows is two leaders"


async def test_concurrent_takers_on_a_lapsed_row_get_exactly_one_winner(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """Two pods racing a lapsed row: the loser's conflict update re-evaluates
    against the winner's fresh lease and matches nothing — exactly one wins."""
    schema = module_pg_schema.schema_name
    holder, first, second = new_uuid(), new_uuid(), new_uuid()
    for worker_id in (holder, first, second):
        await _create_worker(lease_conn, schema, worker_id)

    assert await _elect(lease_conn, schema, holder) is not None
    await _backdate_holder(lease_conn, schema, stale_after_secs=_STALE_AFTER_SECS)

    conn_b = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        won_a, won_b = await asyncio.gather(
            _elect(lease_conn, schema, first),
            _elect(conn_b, schema, second),
        )
    finally:
        await conn_b.close()

    winners = [w for w in (won_a, won_b) if w is not None]
    assert len(winners) == 1, (
        "exactly one taker may win a lapsed row — the conflict predicate must be "
        f"re-checked against the winner's fresh expiry: {won_a=}, {won_b=}"
    )
    row = await _leader_row(lease_conn, schema)
    assert row is not None
    expected = first if won_a is not None else second
    assert row["worker_id"] == expected
    # The loser's re-evaluation must not have rewritten the winner's term.
    assert row["elected_at"] == winners[0]


async def test_the_recorded_holder_can_re_elect_its_own_live_row(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """The holder may take its own live row again, starting a fresh term.

    A leader that stepped down without its lease lapsing — a dropped
    leader_conn on a credential reload, a renewal that spent the trust
    window — re-elects through this arm instead of waiting its own lease
    out. It cannot create a second leader: while the row names this worker
    with an unexpired lease, no peer could have taken it.
    """
    schema = module_pg_schema.schema_name
    holder, other = new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, holder)
    await _create_worker(lease_conn, schema, other)

    first_term = await _elect(lease_conn, schema, holder)
    assert first_term is not None

    second_term = await _elect(lease_conn, schema, holder)
    assert second_term is not None, (
        "the recorded holder re-electing its own live row must win — "
        "the alternative is a leadership gap the width of the lease on "
        "every credential reload"
    )
    assert second_term > first_term, "a re-elect starts a fresh term"

    # ...and the row still admits no peer while the fresh term lives.
    took = await _elect(lease_conn, schema, other)
    assert took is None
    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == holder


async def test_a_holder_can_extend_its_own_row_repeatedly(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """The steady state: one holder renews every heartbeat indefinitely, and
    the row keeps naming it with a rolling expiry — no re-election churn."""
    schema = module_pg_schema.schema_name
    holder = new_uuid()
    await _create_worker(lease_conn, schema, holder)

    elected_at = await _elect(lease_conn, schema, holder)
    assert elected_at is not None
    expiry = (await _leader_row(lease_conn, schema))["expires_at"]  # type: ignore[union-attr]  # Why: the row exists — the elect just returned its term.

    for _ in range(3):
        renewed = await _renew(lease_conn, schema, holder, elected_at)
        assert renewed is not None, "a live term must keep renewing"
        assert renewed > expiry, "each renewal moves the expiry forward"
        expiry = renewed

    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == holder
    assert row["elected_at"] == elected_at, "renewals extend the term, never replace it"


async def test_an_old_release_pod_taking_over_the_row_does_not_open_a_stale_expiry_window(
    module_pg_schema: ModulePgSchema, lease_conn: asyncpg.Connection
) -> None:
    """D9's rolling-upgrade split-brain hazard: an old-release pod's four-column
    upsert leaves ``expires_at`` at its PREVIOUS holder's stamped value rather
    than clearing it. If a new pod judged claimability on ``expires_at`` alone,
    once that leftover instant passed a new pod would take over from an
    old-release pod that is still alive and still pinging ``last_seen_at`` —
    the exact split-brain window this design exists to close.

    This mirrors what a real old pod does: its upsert (``INSERT ... (singleton,
    worker_id, elected_at, last_seen_at) ... ON CONFLICT DO UPDATE SET`` naming
    only those four columns, unconditionally once it holds the courtesy lock)
    never mentions ``expires_at``, so the column is carried forward untouched
    across the overwrite.
    """
    schema = module_pg_schema.schema_name
    new_release_leader, old_release_leader, successor = new_uuid(), new_uuid(), new_uuid()
    await _create_worker(lease_conn, schema, new_release_leader)
    await _create_worker(lease_conn, schema, old_release_leader)
    await _create_worker(lease_conn, schema, successor)

    # A new-release pod wins the role and stamps a lease.
    elected_at = await _elect(lease_conn, schema, new_release_leader)
    assert elected_at is not None
    stale_expiry = (await _leader_row(lease_conn, schema))["expires_at"]  # type: ignore[union-attr]

    # The new-release leader is deposed by an old-release pod's unguarded,
    # unconditional four-column upsert (the pre-lease shape: no WHERE clause,
    # no expires_at in its SET list). This is the production old-release
    # statement, reproduced verbatim rather than invoked, since the fix
    # deleted that code path from this release.
    await lease_conn.execute(
        f'INSERT INTO "{schema}".maintenance_leader (singleton, worker_id, elected_at, last_seen_at) '
        f"VALUES (true, $1, clock_timestamp(), clock_timestamp()) "
        f"ON CONFLICT (singleton) DO UPDATE SET "
        f"worker_id = EXCLUDED.worker_id, "
        f"elected_at = EXCLUDED.elected_at, "
        f"last_seen_at = EXCLUDED.last_seen_at",
        old_release_leader,
    )
    row = await _leader_row(lease_conn, schema)
    assert row is not None
    assert row["worker_id"] == old_release_leader, "test setup: the old pod now holds the row"
    assert row["expires_at"] == stale_expiry, (
        "test setup: the old pod's upsert must carry the previous holder's "
        "expires_at forward untouched -- that is the hazard being pinned"
    )

    # Let the leftover expires_at instant pass, while the old-release pod
    # keeps pinging last_seen_at (it is alive; only its election protocol is
    # stale). The old pod's own upsert did this to itself unconditionally
    # once it won the courtesy lock, so backdating just fast-forwards past
    # the borrowed expiry without touching the ping.
    await lease_conn.execute(
        f'UPDATE "{schema}".maintenance_leader SET '
        f"expires_at = clock_timestamp() - interval '1 second' "
        f'WHERE singleton = true',
    )

    took = await _elect(lease_conn, schema, successor)
    assert took is None, (
        "a new pod must not take over once the borrowed expires_at lapses while "
        "the old-release holder's last_seen_at is still fresh -- doing so would "
        "depose a LIVE leader the instant the stale expiry passed, which is "
        "exactly the split-brain window D9 requires closed"
    )
    row = await _leader_row(lease_conn, schema)
    assert row is not None and row["worker_id"] == old_release_leader
