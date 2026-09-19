"""NOTIFY channel names fit Postgres's identifier limit for every legal schema.

``LISTEN`` takes the channel as an identifier, which Postgres silently
truncates to NAMEDATALEN-1 (63) bytes; ``pg_notify`` takes it as text and
raises ``22023 channel name too long`` past the same bound. A channel built
by interpolating the schema name therefore has a cliff: the per-worker
cancel channel (13 + schema + 1 + 36-char uuid) overflowed at a 14-char
schema - every cancel's NOTIFY statement then errored, was swallowed as a
warning, and cancel latency degraded to the heartbeat poll - and the wake
channel overflowed at 53, breaking every enqueue. ``schema_name`` admits up
to 63 characters.

Every channel is now derived from a fixed-width schema tag (a hash prefix),
so its length no longer depends on the schema at all; the settings loader
refuses a schema whose channels would not fit; and the wake trigger
computes the identical tag in SQL, pinned end to end below.
"""

import asyncio
from contextlib import AsyncExitStack
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import (
    PG_MAX_IDENTIFIER_BYTES,
    cron_commit_gate_channel,
    events_channel,
    progress_channel,
    progress_global_channel,
    schema_channel_tag,
    wake_channel,
    worker_channel,
)
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_running_job, create_worker
from taskq.testing.settings import make_integration_settings

_IDENT_FIRST = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
_IDENT_REST = _IDENT_FIRST + "0123456789"

#: Every legal ``schema_name``: identifier charset, 1..63 chars.
_schemas = st.builds(
    lambda head, tail: head + tail,
    st.text(_IDENT_FIRST, min_size=1, max_size=1),
    st.text(_IDENT_REST, min_size=0, max_size=62),
)

#: The widest ``str(uuid)`` form (36 chars) and a 40-char schema - well past
#: the 13-char cliff the per-worker channel used to have.
_WIDEST_UUID = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
_LONG_SCHEMA = "tq_forty_character_schema_name_xxxxxxxxx"
assert len(_LONG_SCHEMA) == 40
_GRACE = timedelta(seconds=30)


def _every_channel(schema: str) -> list[str]:
    return [
        wake_channel(schema),
        events_channel(schema),
        worker_channel(schema, str(_WIDEST_UUID)),
        progress_channel(schema, _WIDEST_UUID),
        progress_global_channel(schema),
        cron_commit_gate_channel(schema),
    ]


@settings(max_examples=300)
@given(schema=_schemas)
def test_every_channel_fits_the_identifier_limit_for_any_legal_schema(schema: str) -> None:
    for channel in _every_channel(schema):
        assert len(channel.encode()) <= PG_MAX_IDENTIFIER_BYTES, (
            f"{channel!r} is {len(channel.encode())} bytes for schema {schema!r}; "
            f"LISTEN would silently truncate it to {PG_MAX_IDENTIFIER_BYTES}"
        )


@settings(max_examples=300)
@given(a=_schemas, b=_schemas)
def test_distinct_schemas_derive_distinct_channels(a: str, b: str) -> None:
    if a == b:
        return
    assert schema_channel_tag(a) != schema_channel_tag(b)
    assert set(_every_channel(a)).isdisjoint(_every_channel(b))


def test_channel_tag_is_stable_and_case_sensitive() -> None:
    """The tag is a pure function of the schema text - quoted identifiers are
    case-sensitive, so ``Taskq`` and ``taskq`` are different schemas and must
    not share channels."""
    assert schema_channel_tag("taskq") == schema_channel_tag("taskq")
    assert schema_channel_tag("taskq") != schema_channel_tag("Taskq")


def test_settings_accept_the_longest_legal_schema() -> None:
    """A 63-char schema loads: its channels fit, so nothing refuses it."""
    loaded = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@localhost:5432/db", "schema_name": "s" * 63}
    )
    assert loaded.schema_name == "s" * 63


# ── End to end: the trigger and the app derive the same channel ──────────


async def _migrated(pg_dsn: str, schema: str) -> WorkerSettings:
    from taskq.migrate import apply_pending

    settings_ = make_integration_settings(pg_dsn, schema_name=schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return settings_


@pytest.mark.integration
async def test_enqueue_wake_arrives_under_a_long_schema(pg_dsn: str) -> None:
    """The wake trigger (SQL) and ``wake_channel`` (Python) derive the same
    name for a 40-char schema, so a pending INSERT wakes a listener."""
    await _migrated(pg_dsn, _LONG_SCHEMA)
    woken = asyncio.Event()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.add_listener(
            wake_channel(_LONG_SCHEMA),
            lambda _c, _pid, _ch, _payload: woken.set(),
        )
        await conn.execute(
            f'INSERT INTO "{_LONG_SCHEMA}".jobs '  # noqa: S608  # Why: schema is a test constant matching _IDENT_RE.
            "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
            "VALUES ($1, 'a', 'default', '{}'::jsonb, 'pending', 3, 'transient', clock_timestamp())",
            new_uuid(),
        )
        async with asyncio.timeout(5.0):
            await woken.wait()
    finally:
        await conn.close()


@pytest.mark.integration
async def test_cancel_notify_reaches_the_worker_channel_under_a_long_schema(
    pg_dsn: str,
) -> None:
    """A cancel under a 40-char schema lands on the per-worker channel - the
    fast path that used to error on the channel length and fall back to the
    heartbeat poll."""
    from taskq.worker.deps import open_worker_deps

    settings_ = await _migrated(pg_dsn, _LONG_SCHEMA)
    async with AsyncExitStack() as stack:
        deps = await stack.enter_async_context(open_worker_deps(settings_))
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, _LONG_SCHEMA, worker_id)
            job_id = await create_running_job(conn, _LONG_SCHEMA, worker_id)

        received: list[str] = []
        arrived = asyncio.Event()

        def _on_notify(_c: object, _pid: int, _ch: str, payload: str) -> None:
            received.append(payload)
            arrived.set()

        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                worker_channel(_LONG_SCHEMA, str(worker_id)),
                _on_notify,  # pyright: ignore[reportArgumentType]  # Why: asyncpg stubs over-narrow the callback type - same pattern as worker/notify.py
            )
            backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
            assert await backend.write_cancel_request(job_id, "test cancel") is True
            async with asyncio.timeout(5.0):
                await arrived.wait()
        finally:
            await listen_conn.close()
        assert str(job_id) in received[0]
