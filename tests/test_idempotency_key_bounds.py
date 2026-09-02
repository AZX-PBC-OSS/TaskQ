"""The idempotency-key bound maps to the btree index limit, not to 256.

``idempotency_key``/``idempotency_scope`` were capped at 256 characters by a
literal duplicated across ``client/_args.py`` and ``client/_jobs.py``, with no
comment in either. The column is plain ``text``; what actually bounds it is the
composite unique index ``jobs_idempotency_scope_key_uniq``, whose btree entries
cannot exceed ``BTREE_MAX_ITEM_BYTES`` — Postgres raises ``index row size N
exceeds btree version 4 maximum 2704`` at INSERT. That limit counts encoded
bytes, so the cap is expressed in UTF-8 bytes.
"""

import secrets
from typing import TYPE_CHECKING

import pytest

from taskq.client._jobs import JobsClient

if TYPE_CHECKING:
    import asyncpg
from taskq.constants import (
    BTREE_MAX_ITEM_BYTES,
    IDEMPOTENCY_KEY_BYTES_CEILING,
    MAX_IDEMPOTENCY_KEY_BYTES,
)
from taskq.settings import TaskQSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from tests.test_jobs_client import (  # type: ignore[import-untyped]
    _START,
    _singleton_actor,
    _SingletonPayload,
)

_INDEX_TUPLE_OVERHEAD = 16
"""Measured index-tuple overhead around the two values, in bytes.

Postgres reports ``index row size 2720`` for a 1352+1352-byte pair, so the
entry carries exactly 16 bytes of its own (the IndexTupleData header, the
varlena headers and MAXALIGN padding).
"""


def _client(settings: TaskQSettings | None = None) -> JobsClient:
    return JobsClient(InMemoryBackend(clock=FakeClock(_START)), settings=settings)


def test_ceiling_cannot_overflow_a_btree_entry() -> None:
    """No configuration of the setting can produce a raw Postgres index error:
    scope and key both at the ceiling still fit one btree entry."""
    assert 2 * IDEMPOTENCY_KEY_BYTES_CEILING + _INDEX_TUPLE_OVERHEAD < BTREE_MAX_ITEM_BYTES
    assert MAX_IDEMPOTENCY_KEY_BYTES <= IDEMPOTENCY_KEY_BYTES_CEILING


async def test_a_key_longer_than_256_is_accepted() -> None:
    """The cap that made a consumer sha512-hash its vendor cursors is gone."""
    handle = await _client().enqueue(
        _singleton_actor, _SingletonPayload(value=1), idempotency_key="x" * 900
    )
    assert handle.job_id is not None


async def test_over_the_cap_is_a_clean_value_error_naming_the_setting() -> None:
    too_long = "x" * (MAX_IDEMPOTENCY_KEY_BYTES + 1)
    with pytest.raises(ValueError, match="TASKQ_IDEMPOTENCY_KEY_MAX_BYTES"):
        await _client().enqueue(
            _singleton_actor, _SingletonPayload(value=1), idempotency_key=too_long
        )


async def test_the_cap_counts_utf8_bytes_not_characters() -> None:
    """A multibyte key that fits the cap in *characters* but not in bytes is
    rejected — the btree limit counts bytes."""
    key = "中" * (MAX_IDEMPOTENCY_KEY_BYTES // 3 + 1)  # 3 bytes per char
    assert len(key) < MAX_IDEMPOTENCY_KEY_BYTES
    with pytest.raises(ValueError, match="UTF-8 bytes"):
        await _client().enqueue(_singleton_actor, _SingletonPayload(value=1), idempotency_key=key)


async def test_the_cap_is_configurable() -> None:
    settings = TaskQSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq",
            "TASKQ_IDEMPOTENCY_KEY_MAX_BYTES": str(IDEMPOTENCY_KEY_BYTES_CEILING),
        }
    )
    key = "x" * IDEMPOTENCY_KEY_BYTES_CEILING
    handle = await _client(settings).enqueue(
        _singleton_actor, _SingletonPayload(value=1), idempotency_key=key
    )
    assert handle.job_id is not None


async def test_scope_shares_the_same_bound() -> None:
    handle = await _client().enqueue(
        _singleton_actor,
        _SingletonPayload(value=1),
        idempotency_key="k",
        idempotency_scope="s" * 900,
    )
    assert handle.job_id is not None
    with pytest.raises(ValueError, match="TASKQ_IDEMPOTENCY_KEY_MAX_BYTES"):
        await _client().enqueue(
            _singleton_actor,
            _SingletonPayload(value=1),
            idempotency_key="k",
            idempotency_scope="s" * (MAX_IDEMPOTENCY_KEY_BYTES + 1),
        )


# ── Against a real Postgres ─────────────────────────────────────────────
#
# The unit tests above pin the arithmetic; these pin the premise — that the
# btree entry size is the actual constraint and that the ceiling sits under
# it. Marked integration: they need a migrated schema.


async def _insert_raw(
    conn: "asyncpg.Connection",
    schema: str,
    *,
    idempotency_key: str,
    idempotency_scope: str,
) -> None:
    from datetime import UTC, datetime

    from taskq._ids import new_uuid

    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, "
        f"idempotency_scope, idempotency_key) "
        f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)",
        new_uuid(),
        "direct_actor",
        "default",
        "{}",
        3,
        "transient",
        datetime.now(UTC),
        idempotency_scope,
        idempotency_key,
    )


def _incompressible(n: int) -> str:
    """*n* characters a btree index tuple cannot shrink.

    Index tuples are PGLZ-compressed, so ``"k" * 8000`` inserts happily —
    measured, not assumed. Only incompressible values probe the real limit,
    and that is what a caller's opaque vendor cursor or hash digest is.
    """
    return secrets.token_urlsafe(n * 2)[:n]


@pytest.mark.integration
async def test_pg_accepts_scope_and_key_both_at_the_ceiling(
    pg_conn: "asyncpg.Connection", settings: TaskQSettings
) -> None:
    from taskq import migrate as migrate_mod

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    await _insert_raw(
        pg_conn,
        settings.schema_name,
        idempotency_key=_incompressible(IDEMPOTENCY_KEY_BYTES_CEILING),
        idempotency_scope=_incompressible(IDEMPOTENCY_KEY_BYTES_CEILING),
    )


@pytest.mark.integration
async def test_pg_btree_limit_is_the_real_constraint(
    pg_conn: "asyncpg.Connection", settings: TaskQSettings
) -> None:
    """Past the btree entry size Postgres itself rejects the INSERT — the
    error the client-side cap exists so that no caller ever sees it.

    Measured: 1352 incompressible bytes of scope and key make a 2720-byte
    index row against the 2704 maximum, so the tuple carries 16 bytes of its
    own and the largest safe symmetric value is (2704 - 16) / 2 = 1344 —
    which is why IDEMPOTENCY_KEY_BYTES_CEILING is 1300.
    """
    import asyncpg

    from taskq import migrate as migrate_mod

    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    over = (BTREE_MAX_ITEM_BYTES - _INDEX_TUPLE_OVERHEAD) // 2 + 8
    with pytest.raises(asyncpg.PostgresError, match="btree version 4 maximum 2704"):
        await _insert_raw(
            pg_conn,
            settings.schema_name,
            idempotency_key=_incompressible(over),
            idempotency_scope=_incompressible(over),
        )
