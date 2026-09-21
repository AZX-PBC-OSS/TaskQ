"""Attack: the admin jobs list's pagination/filter edges, against real PG.

The list page's contract for hostile query strings is documented on
:func:`taskq.web.admin.jobs._cursor_values`: "A malformed cursor -- hand-edited
URL, stale bookmark ... returns None so the caller falls back to the unpaged
first page rather than surfacing a driver error". Every junk parameter the
page accepts must therefore land on a 4xx (the family's clean 400) or a
rendered first page -- never a 500.

Also pins the row-vanishing race: rows swept/pruned between two page turns of
a live poll sequence must not 500 or re-serve a stale page, and the duration
cell's edge states (zero-length span, running row, archived row).
"""

import re
from datetime import UTC, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI
from pydantic import BaseModel

from taskq.actor import actor
from taskq.client import JobsClient
from taskq.web.admin import create_router, setup_admin_state

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    Pool = object
    WorkerDeps = PostgresBackend = object

pytestmark = [pytest.mark.fastapi]


class _Payload(BaseModel):
    value: int = 1


@actor(name="_atk_list_edges_actor")
async def _atk_list_edges_actor(payload: _Payload) -> None:
    pass


def _admin_client(
    deps: WorkerDeps,
    backend: PostgresBackend,
) -> httpx.AsyncClient:
    """The admin app over the module's REAL pool, 500s surfaced as statuses.

    ``raise_app_exceptions=False`` so an unhandled route exception is
    observable as the HTTP 500 the deployment would serve, an asserted
    status rather than a traceback out of the transport.
    """
    bundle = create_router(
        deps.worker_pool,  # pyright: ignore[reportArgumentType]  # Why: the fixture's pool is a real asyncpg pool.
        schema=deps.settings.schema_name,
        backend=backend,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)  # type: ignore[arg-type]  # Why: ASGITransport takes the ASGI app; FastAPI satisfies it, pyright's protocol view disagrees.
    return httpx.AsyncClient(transport=transport, base_url="http://atkweb.local")


_JUNK: list[tuple[str, str]] = [
    ("cursor_at", "not-a-timestamp"),
    ("cursor_at", "99999999999999"),  # parses as an int, out of int4 range
    ("cursor_at", "-99999999999999"),
    ("cursor_at", "2025-01-01T00:00:00+99:00"),  # bogus offset
    ("cursor_id", "not-a-uuid"),
    ("cursor_id", "99999999999999"),
    ("cursor_dir", "sideways"),
    ("sort", "old_sort_not_a_column"),
    ("order", "sideways"),
    ("status", "bogus_status"),
    ("time_range", "13mo"),
    ("time_from", "garbage"),
    ("time_to", "garbage"),
    ("actor", "a\x00b"),  # NUL: asyncpg 22021 unless guarded
    ("queue", "\x00"),
    ("search", "\x00"),
]


@pytest.mark.parametrize("param,value", _JUNK)
async def test_junk_jobs_list_params_are_never_a_500(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
    param: str,
    value: str,
) -> None:
    """Every junk query parameter lands on 4xx or a rendered first page.

    The documented malformed-cursor contract names hand-edited URLs and
    stale bookmarks: an operator-facing page must answer them, not 500.
    """
    deps, backend = clean_jobs_app
    client = _admin_client(deps, backend)
    response = await client.get("/jobs", params={param: value})
    assert response.status_code < 500, (
        f"/jobs?{param}={value!r} -> {response.status_code}: the page 500'd "
        "on junk input instead of the documented 4xx/first-page fallback"
    )


@pytest.mark.parametrize("param,value", _JUNK)
async def test_junk_jobs_count_params_are_never_a_500(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
    param: str,
    value: str,
) -> None:
    """The poll endpoint the live refresh drives takes the same junk battery:
    a polling refresh must never surface a driver error either."""
    deps, backend = clean_jobs_app
    client = _admin_client(deps, backend)
    response = await client.get("/jobs/count", params={param: value})
    assert response.status_code < 500, f"/jobs/count?{param}={value!r} -> {response.status_code}"


async def test_out_of_range_int_cursor_returns_the_first_page(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """An int cursor value that parses in Python but is out of the column's
    int4 range is a MALFORMED cursor per the documented contract: the page
    falls back to the unpaged first page (200), it does not surface the
    driver's DataError as a 500.

    The seam: ``SortColumn.parse`` for the "int" kind succeeds on any Python
    int, the ``::int`` bind then raises ``asyncpg.DataError`` -- not a
    ``ValueError`` -- so the malformed-cursor guard in ``_cursor_values``
    never saw it.
    """
    deps, backend = clean_jobs_app
    client = _admin_client(deps, backend)
    any_uuid = "00000000-0000-0000-0000-00000000000f"
    response = await client.get(  # pyright: ignore[reportUnknownVariableType]
        "/jobs",
        params={
            "tab": "live",
            "sort": "attempt",
            "order": "asc",
            "cursor_at": "99999999999999",
            "cursor_id": any_uuid,
        },
    )
    assert response.status_code == 200, (
        f"out-of-range int cursor 500'd the list page: {response.status_code}"
    )


async def test_rows_vanishing_between_page_turns_do_not_500_or_replay_the_page(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """A poll sequence whose rows are swept mid-walk (prune/retention racing
    the admin's read) turns pages without a 500 and never re-serves rows the
    cursor already passed.

    55 rows (one over the 50-row page), page 1 walked, then every row the
    cursor has NOT yet reached is deleted (the sweep racing the read) - the
    next page must come back 200 and empty, not 500 and not the deleted
    rows.
    """
    from datetime import datetime as dt

    deps, backend = clean_jobs_app
    client = _admin_client(deps, backend)

    handle_ids: list[str] = []
    job_client = JobsClient(backend)
    for _ in range(55):
        handle = await job_client.enqueue(_atk_list_edges_actor, _Payload())
        handle_ids.append(str(handle.job_id))
    schema = deps.settings.schema_name
    base = dt(2026, 1, 1, tzinfo=UTC)
    async with deps.worker_pool.acquire() as conn:
        for offset, job_id in enumerate(handle_ids):
            await conn.execute(
                f'UPDATE "{schema}".jobs SET created_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is fixture-derived and validated; values are $1/$2-bound.
                job_id,
                base + timedelta(minutes=offset),
            )

    page1 = await client.get("/jobs", params={"sort": "created_at", "order": "asc"})
    assert page1.status_code == 200

    next_url = _next_href(page1.text)
    assert next_url is not None, "55 rows over a 50-row page must offer a next page"
    # The sweep/prune lands: every row past the cursor is deleted before the
    # page turn.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'DELETE FROM "{schema}".jobs WHERE created_at > $1',  # noqa: S608  # Why: as above.
            base + timedelta(minutes=49),
        )
    page2 = await client.get(next_url)
    assert page2.status_code == 200, f"the page turn over vanishing rows 500'd: {page2.status_code}"


def _admin_env() -> Any:
    """The admin app's Jinja2 environment, filters installed."""
    from jinja2 import Environment, PackageLoader

    from taskq.web.admin._factory import (  # pyright: ignore[reportPrivateUsage]  # Why: the exact filters the factory installs; reusing them keeps the render honest.
        _db_now,
        _iso_attr,
        _time_ago,
    )

    env = Environment(autoescape=True, loader=PackageLoader("taskq.web", "templates"))
    env.filters["time_ago"] = _time_ago
    env.filters["iso_attr"] = _iso_attr
    env.globals["base_path"] = ""
    env.globals["sso_logout_token"] = lambda: None
    _ = _db_now  # Why: imported for symmetry with the factory's env setup.
    return env


def _next_href(html: str) -> str | None:
    """The Next link's href from a rendered jobs page, or None.

    The pagination footer renders the enabled Next control as an anchor
    whose text ends in ``Next``; the disabled one is a ``<span>``.
    """
    for anchor in re.findall(r"<a href=\"([^\"]+)\"[^>]*>\s*Next\b", html):
        return anchor
    return None


# ── duration / span rendering edge states ───────────────────────────────


def test_duration_cell_edge_states() -> None:
    """The Duration cell's three shapes render without error and with the
    right emphasis: a settled zero-length span (started_at == finished_at)
    reads as 0ms, a running row renders the server-computed elapsed span,
    and an archived row with neither value renders the dash."""
    env = _admin_env()

    def _cell(job: dict[str, Any]) -> str:
        template = env.get_template("_partials/job_table.html")
        html = template.render(
            jobs=[job],
            tab="live",
            base_path="",
            realtime_mode="polling",
            mode_label="polling mode",
        )
        return html

    zero: dict[str, Any] = {
        "id": "j-zero",
        "actor": "a",
        "queue": "default",
        "status": "succeeded",
        "created_at": "2025-01-01T00:00:00+00:00",
        "scheduled_at": None,
        "started_at": "2025-01-01T00:00:01+00:00",
        "finished_at": "2025-01-01T00:00:01+00:00",
        "duration_ms": 0.0,
        "attempt": 1,
        "max_attempts": 3,
        "retry_kind": "transient",
        "priority": 0,
        "identity_key": None,
        "fairness_key": None,
        "locked_by_worker": None,
        "lock_expires_at": None,
        "lease_expired": False,
        "cancel_requested_at": None,
        "progress_state": None,
        "error_message": None,
        "tags": [],
    }
    html = _cell(zero)
    assert "0ms" in html, "a zero-length settled span must render 0ms, not a dash"

    running = {
        **zero,
        "id": "j-running",
        "status": "running",
        "finished_at": None,
        "duration_ms": None,
        "running_for_ms": 5000.0,
    }
    html = _cell(running)
    assert "5.0s" in html, "a running row renders the server-computed elapsed span"
    assert "still running" in html, "the elapsed span is marked as live, not settled"

    archived = {
        **zero,
        "id": "j-archived",
        "status": "succeeded",
        "started_at": None,
        "finished_at": None,
        "duration_ms": None,
    }
    html = _cell(archived)
    assert "still running" not in html, "an archived row never renders the live span"
