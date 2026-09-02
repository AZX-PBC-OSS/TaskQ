"""The admin UI's relative-time filter must not answer from the app clock.

Every timestamp the admin UI renders was written by the database clock.
Subtracting them from ``datetime.now()`` in the admin process makes the
displayed age wrong by exactly the app-to-database skew -- NTP drift, a
paused VM, a container clock, WSL2's documented stepping -- and the
question it answers ("how stale is this row?") is asked during an
incident, when a wrong answer costs the most.

Skew direction (see tests/_clock_skew.py): an app clock AHEAD of the
server inflates ages, so a row written a moment ago reads as minutes old.
The assertions here are on the rendered HTML of a real admin request.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from pydantic import BaseModel

import taskq.web.admin._factory as factory
from taskq.actor import actor
from taskq.client import JobsClient
from taskq.web.admin import create_router, setup_admin_state

pytestmark = pytest.mark.integration

_SKEW = timedelta(minutes=5)


class _Payload(BaseModel):
    value: int = 1


@actor(name="_time_ago_actor")
async def _time_ago_actor(payload: _Payload) -> None:
    pass


class _AheadOfServer(datetime):
    """A process wall clock running ``_SKEW`` ahead of the database."""

    @classmethod
    def now(cls, tz: object = None) -> datetime:  # type: ignore[override]  # Why: test double narrowing datetime.now; only the tz-aware call shape is exercised.
        return datetime.now(UTC) + _SKEW


@pytest.fixture(autouse=True)
def _dev_environment(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner.
    """Same rationale as tests/test_web_admin_integration.py: this file
    tests rendering, not the auth gate, so it builds an unauthenticated
    router under a dev environment label."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")


@pytest.fixture(autouse=True)
def _force_clock_offset_remeasure() -> None:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner.
    """Expire the process-wide offset cache before each test.

    The offset is cached for 30 s in production, which is right there and
    wrong here: without this, whichever of these tests runs first decides
    the offset for the other and the skew would never be re-measured.
    """
    factory._db_clock_offset.expires_at = 0.0  # pyright: ignore[reportPrivateUsage]  # Why: expiring a cache is the documented way to force the next read to re-probe.


@pytest.fixture
def _app_clock_ahead_of_server(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: requested by name as a test parameter; pyright does not track fixture usage.
    monkeypatch.setattr(factory, "datetime", _AheadOfServer)


@pytest_asyncio.fixture
async def admin_client(
    clean_jobs_app: tuple[object, object],
) -> AsyncIterator[httpx.AsyncClient]:
    """An admin app served over ASGI, on the same pool the job is seeded in."""
    deps, backend = clean_jobs_app
    schema: str = deps.settings.schema_name  # pyright: ignore[reportAttributeAccessIssue]  # Why: WorkerDeps is only imported for typing in this suite's convention; the field exists at runtime.
    client = JobsClient(backend)  # pyright: ignore[reportArgumentType]  # Why: as above.
    await client.enqueue(_time_ago_actor, _Payload())

    app = FastAPI()
    bundle = create_router(deps.worker_pool, schema=schema, base_path="/admin")  # pyright: ignore[reportAttributeAccessIssue]  # Why: as above.
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http_client:
        yield http_client


async def test_rendered_age_ignores_app_clock_skew(
    admin_client: httpx.AsyncClient,
    _app_clock_ahead_of_server: None,
) -> None:
    """A job created a moment ago must render as "now" on a host whose
    clock runs five minutes ahead of the database."""
    response = await admin_client.get("/admin/jobs")
    assert response.status_code == 200
    body = response.text

    assert "_time_ago_actor" in body, "the seeded job must be on the page"
    assert "5 minutes ago" not in body, (
        f"a just-created job rendered as five minutes old because the age was "
        f"measured against an app clock {_SKEW} ahead of the database"
    )
    assert "now" in body


async def test_rendered_age_is_correct_without_skew(
    admin_client: httpx.AsyncClient,
) -> None:
    """The unskewed case keeps rendering a fresh row as "now"."""
    response = await admin_client.get("/admin/jobs")
    assert response.status_code == 200
    assert "5 minutes ago" not in response.text
    assert "now" in response.text
