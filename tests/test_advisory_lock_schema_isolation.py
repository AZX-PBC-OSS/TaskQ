"""Schema-qualified advisory locks: two schemas in one database never serialize.

Advisory locks live in a per-DATABASE namespace, so the unqualified names
this module's history shipped (``taskq:maintenance_leader``, ``taskq:prune``,
``taskq:archive_expiry``, ``taskq:cron``) were shared by every schema in the
database: two schemas' workers then contested one election lock, and the
perpetual loser never ran its sweeps while its dispatch (not leader-gated)
kept flowing - a fleet that reports healthy while scheduled work stops
moving. ``schema_lock_name`` qualifies each lock with its schema.

The tests here pin the property at the PG level - two schemas in one
database both win their own lock; within one schema the lock still
serializes - and pin the production sources against a reversion to the
unqualified literals.
"""

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from taskq.constants import schema_lock_name
from taskq.migrate import apply_pending
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import seed_actors

#: The worker sources that must carry no unqualified lock-name literal.
#: Kept as repo-relative paths resolved from this file so the pins work
#: regardless of the pytest rootdir.
_SOURCE_FILES: tuple[str, ...] = (
    "src/taskq/worker/leader.py",
    "src/taskq/worker/_leader_sweeps.py",
    "src/taskq/worker/_leader_shared.py",
)

#: Exact-string pins for the killed unqualified literals. Each pin includes
#: the literal's own quotes so only the exact reversion matches - a
#: schema-qualified name (``taskq:prune:{schema}``) can never trip one.
_UNQUALIFIED_LITERALS: tuple[str, ...] = (
    'taskq:maintenance_leader"',
    '"taskq:prune"',
    '"taskq:archive_expiry"',
)


@pytest_asyncio.fixture(scope="module")
async def module_pg_schema_b(
    module_pg_schema: ModulePgSchema,
) -> AsyncIterator[ModulePgSchema]:
    """A SECOND migrated schema in the SAME database as ``module_pg_schema``.

    Derives its name from the primary fixture's name (hex suffix swapped for
    ``_b``), so it stays inside the same identifier budget and can never
    collide with the primary. The advisory-lock statements under test never
    touch these tables - the schemas are migrated so the two-schema topology
    under test is real, not just two name strings.
    """
    schema_name = module_pg_schema.schema_name[:-2] + "_b"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await apply_pending(conn, schema=schema_name)
        await seed_actors(conn, schema_name)
    finally:
        await conn.close()

    yield ModulePgSchema(schema_name=schema_name, pg_dsn=module_pg_schema.pg_dsn)

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def _two_conns(
    pg_dsn: str,
) -> AsyncGenerator[tuple[asyncpg.Connection, asyncpg.Connection], None]:
    """Two independent connections on one database.

    Both close in teardown even after a failed assertion: advisory session
    locks release on close, so a leaked holder would poison later tests in
    this module.
    """
    conn_a = await asyncpg.connect(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn)
    try:
        yield conn_a, conn_b
    finally:
        for conn in (conn_a, conn_b):
            if not conn.is_closed():
                await conn.close()


async def _try_lock(conn: asyncpg.Connection, lock_name: str) -> bool:
    """Take the session advisory lock exactly as the election code does."""
    got: bool = await conn.fetchval(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
    )
    return got


@pytest.mark.integration
async def test_maintenance_leader_locks_independent_across_schemas(
    module_pg_schema: ModulePgSchema,
    module_pg_schema_b: ModulePgSchema,
) -> None:
    """THE cross-schema regression: two schemas in one database must BOTH
    win their own maintenance-leader election lock.

    Under the unqualified name both schemas contended one lock and the
    perpetual loser never ran its sweeps - healthy-looking fleet, stalled
    scheduled work.
    """
    lock_s1 = schema_lock_name("maintenance_leader", module_pg_schema.schema_name)
    lock_s2 = schema_lock_name("maintenance_leader", module_pg_schema_b.schema_name)
    assert lock_s1 != lock_s2, "schema_lock_name must qualify the lock with the schema"

    async with _two_conns(module_pg_schema.pg_dsn) as (conn_a, conn_b):
        got_a = await _try_lock(conn_a, lock_s1)
        got_b = await _try_lock(conn_b, lock_s2)
        assert got_a is True, "schema 1 must win its own schema-qualified election lock"
        assert got_b is True, (
            "schema 2 lost the election lock to schema 1 in the same database - "
            "the cross-schema serialization regression"
        )


@pytest.mark.integration
async def test_maintenance_leader_lock_still_serializes_within_one_schema(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Qualifying the lock must not weaken it: within ONE schema a second
    connection still loses, and the winner's unlock re-arms it."""
    lock = schema_lock_name("maintenance_leader", module_pg_schema.schema_name)

    async with _two_conns(module_pg_schema.pg_dsn) as (conn_a, conn_b):
        got_a = await _try_lock(conn_a, lock)
        got_b = await _try_lock(conn_b, lock)
        assert got_a is True
        assert got_b is False, "the schema-qualified lock must still serialize one schema"

        await conn_a.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock)
        got_b_retry = await _try_lock(conn_b, lock)
        assert got_b_retry is True, "unlock on the qualified name must re-arm the lock"


@pytest.mark.integration
async def test_cron_locks_independent_across_schemas(
    module_pg_schema: ModulePgSchema,
    module_pg_schema_b: ModulePgSchema,
) -> None:
    """The same two-schema property for the cron lock name: one schema's
    cron tick lock must not mute another schema's cron."""
    lock_s1 = schema_lock_name("cron", module_pg_schema.schema_name)
    lock_s2 = schema_lock_name("cron", module_pg_schema_b.schema_name)
    assert lock_s1 != lock_s2

    async with _two_conns(module_pg_schema.pg_dsn) as (conn_a, conn_b):
        got_a = await _try_lock(conn_a, lock_s1)
        got_b = await _try_lock(conn_b, lock_s2)
        assert got_a is True
        assert got_b is True, (
            "schema 2 lost the cron lock to schema 1 in the same database - "
            "the cross-schema serialization regression"
        )


def test_no_unqualified_lock_name_literals_remain() -> None:
    """Source pin: the unqualified lock-name literals must not come back.

    Exact-string pins (each literal including its quotes) so any reversion
    to the unqualified constants fails this test instead of silently
    reintroducing cross-schema serialization. Runs in the unit tier too -
    it reads files, no PG needed.
    """
    repo_root = Path(__file__).resolve().parents[1]
    for rel in _SOURCE_FILES:
        text = (repo_root / rel).read_text(encoding="utf-8")
        for pin in _UNQUALIFIED_LITERALS:
            assert pin not in text, (
                f"{rel} reintroduces the unqualified lock literal {pin}; "
                "advisory locks must be schema-qualified via schema_lock_name"
            )


# ── Key-space isolation: purpose locks vs keyed (limiter) locks ─────────
#
# The advisory key space mixes two conventions (migrate.py's
# migration_lock_name docstring documents both):
#
# * PURPOSE locks, one per schema per purpose, schema LAST:
#   ``taskq:{purpose}:{schema}`` (maintenance_leader, prune,
#   archive_expiry, cron, migrate);
# * KEYED locks, one per resource within a schema, schema FIRST:
#   ``taskq:{schema}:sw:{name}`` (the PG sliding-window limiter's
#   advisory lock, mirroring the bucket's Redis key), its ``sw_gcra`` and
#   ``rl:tb`` siblings, and the enqueue unique-for key.
#
# Both feed ``hashtextextended`` in the SAME per-database lock namespace,
# so a string collision between the families would make one schema's
# rate-limiter bucket serialize against another schema's maintenance
# sweep. It cannot happen: a purpose lock has exactly three ``:``-split
# segments and a valid schema name never contains ``:`` (the identifier
# regex), while every keyed lock carries at least one literal segment
# (``sw``, ``rl``, ``unique_for``) beyond ``taskq:``. Hash collisions
# between DISTINCT strings are a different, documented-benign class (a
# little needless serialization, never correctness), so the predicate
# below is string identity over the exact keys production builds.


_PURPOSES: tuple[str, ...] = (
    "maintenance_leader",
    "prune",
    "archive_expiry",
    "cron",
    "migrate",
)

_KEYED_KEY_FORMATS: tuple[str, ...] = (
    # The PG limiter's advisory key (ratelimit/_sliding_window_pg.py).
    "taskq:{schema}:sw:{name}",
    "taskq:{schema}:sw_gcra:{name}",
    "taskq:{schema}:rl:tb:{name}",
    # The enqueue unique-for key (backend/_enqueue.py), keyed-family too.
    "taskq:unique_for:{schema}:{actor}:{identity}",
)

_BUCKET_NAMES: tuple[str, ...] = (
    "default",
    "a",
    "",  # the degenerate bucket name still adds a trailing segment
    "vendor:per_min",  # a name may carry colons; more segments, never fewer
    "{braced}",  # the Redis forms brace the bucket name
    "b" * 63,  # edge length
)

_SCHEMA_CORPUS: tuple[str, ...] = (
    "a",  # minimum shape
    "taskq",
    # A schema named after a purpose or a key literal: the sharpest edge
    # the segment-count argument has to survive.
    "prune",
    "cron",
    "migrate",
    "maintenance_leader",
    "archive_expiry",
    "sw",
    "sw_gcra",
    "rl",
    "tb",
    "unique_for",
    "tq_abc123",  # the fixture-style typical name
    "a" + "0" * 62,  # PG_MAX_IDENTIFIER_BYTES: 63 chars
)

_LIMITER_SOURCE = "src/taskq/ratelimit/_sliding_window_pg.py"


def _purpose_lock_keys(schema: str) -> set[str]:
    """The purpose locks production builds for one schema."""
    return {schema_lock_name(purpose, schema) for purpose in _PURPOSES}


def _keyed_lock_keys(schema: str) -> set[str]:
    """The keyed locks production builds for one schema, every format."""
    return {
        fmt.format(schema=schema, name=name, actor="actor_x", identity="id_y")
        for fmt in _KEYED_KEY_FORMATS
        for name in _BUCKET_NAMES
    }


def test_corpus_is_valid_distinct_schema_names() -> None:
    """The predicate must run over VALID schema names, including the edge
    lengths, or the invariant is pinned against a corpus production can
    never pass in. Also asserts the corpus's own distinctness."""
    from taskq.constants import (
        _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical identifier grammar is the predicate's validity bound.
    )

    assert len(set(_SCHEMA_CORPUS)) == len(_SCHEMA_CORPUS)
    for schema in _SCHEMA_CORPUS:
        assert _IDENT_RE.match(schema), f"corpus member {schema!r} must be a valid schema"
    assert len(_SCHEMA_CORPUS[0]) == 1 and len(_SCHEMA_CORPUS[-1]) == 63, (
        "the corpus must span the identifier lengths, 1 to 63 bytes"
    )


def test_purpose_locks_and_keyed_locks_never_collide_for_any_schema() -> None:
    """THE invariant: for every valid schema name, no purpose lock (any
    purpose, any schema) equals any keyed lock (any format, any bucket
    name, any schema) - the two families share one hashtextextended
    namespace and one collision would serialize a rate limiter against a
    maintenance sweep."""
    for purpose_schema in _SCHEMA_CORPUS:
        for keyed_schema in _SCHEMA_CORPUS:
            overlap = _purpose_lock_keys(purpose_schema) & _keyed_lock_keys(keyed_schema)
            assert not overlap, (
                f"advisory key-space collision: purpose locks for schema "
                f"{purpose_schema!r} equal keyed locks for schema "
                f"{keyed_schema!r}: {sorted(overlap)}"
            )


def test_purpose_locks_stay_distinct_across_the_whole_corpus() -> None:
    """The cross-schema guarantee this file pins with two fixture schemas,
    extended over every valid schema length: two DISTINCT schemas never
    share a purpose lock. This is the property a schema name containing
    ``:`` would break - which is exactly why the identifier grammar
    forbids it."""
    for purpose in _PURPOSES:
        keys = [schema_lock_name(purpose, schema) for schema in _SCHEMA_CORPUS]
        assert len(set(keys)) == len(keys), (
            f"two corpus schemas share the {purpose!r} lock; the "
            "identifier grammar must keep the qualifying segment colon-free"
        )


def test_the_keyed_lock_format_stays_pinned_at_its_source() -> None:
    """Source pin: the PG limiter's advisory key format. If the format
    moves, this fails and the collision predicate above must be re-run
    against the new spelling rather than silently stale."""
    repo_root = Path(__file__).resolve().parents[1]
    text = (repo_root / _LIMITER_SOURCE).read_text(encoding="utf-8")
    assert 'lock_key = f"taskq:{schema}:sw:{self._name}"' in text, (
        f"{_LIMITER_SOURCE} changed the PG limiter's advisory key format; "
        "re-run the purpose-vs-keyed collision predicate against the new "
        "format before updating this pin"
    )
