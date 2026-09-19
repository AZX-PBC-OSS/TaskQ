# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on the admin jobs list's absolute time filters (PG tier).

Hunt scope: ``src/taskq/web/admin/jobs.py`` - ``/jobs`` and ``/jobs/count``
input validation for ``time_from``/``time_to``.

Papered defect
--------------
REAL DEFECT - the absolute time filter cannot bind, so every request that
uses it is a 500 (jobs.py:154-160)::

    if time_from:
        clauses.append(f"created_at >= ${idx}::timestamptz")
        params.append(time_from)
    if time_to:
        clauses.append(f"created_at <= ${idx}::timestamptz")
        params.append(time_to)

``time_from``/``time_to`` are raw query STRINGS bound against
``$n::timestamptz``. asyncpg's timestamptz encoder accepts only
``datetime.date``/``datetime.datetime`` instances - a ``str`` argument is
rejected client-side with
``DataError: invalid input for query argument … expected a
datetime.date or datetime.datetime instance`` (verified against a real
PostgreSQL 18 server with this repo's pinned asyncpg). The exception is
unhandled in ``jobs_list``/``jobs_count``, so:

* a WELL-FORMED window (``2025-01-01T00:00:00+00:00``) → HTTP 500 - the
  shipped feature is unusable, not merely mis-validated;
* a GARBAGE window (``yesterday``) → HTTP 500 too, where the admin
  family's own convention (history.py:178-184, queues.py cursor
  validation) is a clean 400 for unparseable timestamps.

Desired observables pinned below (all RED today):
* ``test_jobs_list_absolute_time_filter_binds`` - a well-formed window
  returns 200 with the in-window row visible and the out-of-window row
  excluded (the route must parse the strings to datetimes, or otherwise
  bind them).
* ``test_jobs_list_garbage_time_filter_is_400`` - a malformed window is a
  400 input error, never a 500.
* ``test_jobs_count_absolute_time_filter_binds`` - the count endpoint
  shares ``_build_where``, so it must bind the same way (200, and the
  count reflects the window).
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI

from taskq._ids import new_base62, new_uuid
from taskq.migrate import apply_pending
from taskq.web.admin import create_router, setup_admin_state

pytestmark = [pytest.mark.fastapi]
pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _admin_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed implicitly by the runner; pyright does not track fixture usage.
    """Dev environment so the admin factory's fail-closed auth check does not
    raise (the gate itself is pinned by test_web_router_factories_fail_closed.py)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")


async def _migrated_schema(pg_dsn: str) -> str:
    """Fresh random schema with migrations applied; dropped by each test."""
    schema = f"twb_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return schema


def _mount_admin(pool: asyncpg.Pool, schema: str) -> httpx.AsyncClient:
    """Admin UI mounted on a real pool over the given schema.

    ``raise_app_exceptions=False`` so an unhandled route exception is
    observable as the HTTP 500 the deployment would serve (Starlette's
    ServerErrorMiddleware answer) instead of exploding out of the
    transport - the RED state must be an asserted status, not a traceback.
    """
    bundle = create_router(pool, schema=schema, redis_client=None)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    return httpx.AsyncClient(transport=transport, base_url="http://rtweb.local")


async def _insert_job(pool: asyncpg.Pool, schema: str, *, actor: str) -> UUID:
    """Seed one pending job; returns its id (UUIDv7, time-ordered)."""
    job_id = new_uuid()
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at) "
            "VALUES ($1, $2, 'default', '{}'::jsonb, 3, 'transient', 'pending', "
            "clock_timestamp())",
            job_id,
            actor,
        )
    return job_id


async def _drop_schema(pool: asyncpg.Pool, schema: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# ── 1. A well-formed absolute window must bind ────────────────────────────


async def test_jobs_list_absolute_time_filter_binds(pg_dsn: str) -> None:
    """A well-formed time_from/time_to pair must return 200 with the window
    applied - not a 500 before a single row is read.

    CONTRACT: /jobs with a well-formed absolute window must bind and return
    200, the in-window row visible, the out-of-window row excluded. The
    route parses ISO strings (the family's own convention: history.py:178-184
    parses cursor timestamps with a 400 on garbage) or otherwise binds
    datetimes - never hands the raw query STRING to a ``$n::timestamptz``
    parameter.

    CURRENT VIOLATION: jobs.py:154-160 appends the raw strings;
    asyncpg rejects a str for a timestamptz parameter with
    ``DataError … expected a datetime.date or datetime.datetime instance``
    (verified against PG 18 + this repo's pinned asyncpg), unhandled in
    jobs_list → HTTP 500 for EVERY absolute-window request.
    """
    async with asyncio.timeout(60):
        schema = await _migrated_schema(pg_dsn)
        pool = await asyncpg.create_pool(pg_dsn)
        try:
            async with _mount_admin(pool, schema) as client:
                inside_id = await _insert_job(pool, schema, actor="rt_web_time_filter_in")
                resp = await client.get(
                    "/jobs",
                    params={
                        "time_from": "2020-01-01T00:00:00+00:00",
                        "time_to": "2035-01-01T00:00:00+00:00",
                    },
                )
                assert resp.status_code == 200, (
                    "CONTRACT: /jobs with a well-formed absolute time window must "
                    "return 200 with the window applied - time_from/time_to are "
                    "shipped filters of the page. CURRENT VIOLATION: jobs.py:154-160 "
                    "binds the raw query STRING against $n::timestamptz and asyncpg "
                    "rejects a str for timestamptz ('expected a datetime.date or "
                    "datetime.datetime instance' - verified against PG 18), so the "
                    f"unhandled DataError turns every window request into a "
                    f"{resp.status_code}."
                )
                assert str(inside_id)[:8] in resp.text, (
                    "CONTRACT: a job created now falls inside [2020, 2035] and must "
                    "be visible once the filter binds."
                )

                past = await client.get(
                    "/jobs",
                    params={
                        "time_from": "2019-01-01T00:00:00+00:00",
                        "time_to": "2019-06-01T00:00:00+00:00",
                    },
                )
                assert past.status_code == 200, (
                    "CONTRACT: an empty-but-valid past window must also bind (200)."
                )
                assert str(inside_id)[:8] not in past.text, (
                    "CONTRACT: the created-now job is outside a 2019 window and must "
                    "be excluded once the filter binds - a 'fixed' route that 200s "
                    "but ignores the window is a silent lie, not a fix."
                )
        finally:
            await _drop_schema(pool, schema)
            await pool.close()


# ── 2. A garbage window is a 400, never a 500 ─────────────────────────────


async def test_jobs_list_garbage_time_filter_is_400(pg_dsn: str) -> None:
    """A malformed time_from must be a clean 400 input error.

    CONTRACT: an unparseable timestamp in a caller-supplied filter is a 400
    - the admin family's own convention (history.py:178-184 returns 400 for
    a bad cursor timestamp; queues.py validates cursor_at the same way).

    CURRENT VIOLATION: the garbage string reaches the asyncpg bind, whose
    DataError is unhandled in jobs_list → opaque 500.
    """
    async with asyncio.timeout(60):
        schema = await _migrated_schema(pg_dsn)
        pool = await asyncpg.create_pool(pg_dsn)
        try:
            async with _mount_admin(pool, schema) as client:
                resp = await client.get(
                    "/jobs",
                    params={
                        "time_from": "yesterday-ish",
                        "time_to": "2035-01-01T00:00:00+00:00",
                    },
                )
                assert resp.status_code == 400, (
                    "CONTRACT: a malformed absolute time window is a 400 input error "
                    "(the admin family's convention - history.py:178-184 400s on a "
                    "bad cursor timestamp). CURRENT VIOLATION: the raw string "
                    "reaches the $n::timestamptz bind and the resulting asyncpg "
                    f"DataError is unhandled in jobs_list - got {resp.status_code} "
                    f"{resp.text[:200]!r}"
                )
        finally:
            await _drop_schema(pool, schema)
            await pool.close()


# ── 3. /jobs/count shares the binding path ────────────────────────────────


async def test_jobs_count_absolute_time_filter_binds(pg_dsn: str) -> None:
    """The count endpoint shares ``_build_where``, so a well-formed window
    must bind there too (200 + a count that reflects the window).

    CONTRACT: /jobs/count with a well-formed window returns 200 and the
    count reflects the window (0 for a past window, 1 for a covering
    window).

    CURRENT VIOLATION: same str→timestamptz bind through the shared
    ``_build_where`` (jobs.py:535-542) → unhandled DataError → 500.
    """
    async with asyncio.timeout(60):
        schema = await _migrated_schema(pg_dsn)
        pool = await asyncpg.create_pool(pg_dsn)
        try:
            await _insert_job(pool, schema, actor="rt_web_time_count")
            async with _mount_admin(pool, schema) as client:
                covering = await client.get(
                    "/jobs/count",
                    params={
                        "time_from": "2020-01-01T00:00:00+00:00",
                        "time_to": "2035-01-01T00:00:00+00:00",
                    },
                )
                assert covering.status_code == 200, (
                    "CONTRACT: /jobs/count with a well-formed window must bind and "
                    "return 200. CURRENT VIOLATION: jobs_count reuses _build_where "
                    "(jobs.py:535-542) whose raw-string ::timestamptz bind raises "
                    f"asyncpg DataError unhandled - got {covering.status_code}."
                )
                assert covering.json()["count"] == 1, (
                    "CONTRACT: the covering window counts the one seeded job."
                )

                past = await client.get(
                    "/jobs/count",
                    params={
                        "time_from": "2019-01-01T00:00:00+00:00",
                        "time_to": "2019-06-01T00:00:00+00:00",
                    },
                )
                assert past.status_code == 200, "the past window must bind too (200)."
                assert past.json()["count"] == 0, (
                    "CONTRACT: a 2019 window excludes the created-now job - the "
                    "count must reflect the window, not ignore it."
                )
        finally:
            await _drop_schema(pool, schema)
            await pool.close()
