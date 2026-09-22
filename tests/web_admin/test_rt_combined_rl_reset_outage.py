"""Cross-PR attack: a redis outage rejection on the admin rate-limit reset.

#421 taught the redis rate-limit primitives to absorb ``ReadOnlyError``
and ``OutOfMemoryError`` (besides ConnectionError/TimeoutError) by
falling back to Postgres -- on the ACQUIRE path. The admin reset route
(#429) reaches the SAME redis-backed primitive through a path with no
fallback and no handling: ``TokenBucket._reset_redis`` issues a raw
``redis.delete``, and the route catches only ``TimeoutError`` and
``KeyError``. A replica promoted mid-flight or a maxmemory breach --
exactly the outage classes #421 names -- therefore escapes the route as
an unhandled 500, while the route's own documented contract for a store
that cannot serve is a 503 with Retry-After ("the reset's round trip
must time out into a 503, not park the request" -- the timeout pin one
door down in test_ops_mutations.py covers the dead-store shape only).

The refusal must also stay audit-clean (#429's own contract: a refused
mutation writes no admin_audit row), so the 503 answers with the row
unwritten.
"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
redis_mod = pytest.importorskip("redis")

from taskq.ratelimit.registry import registry as rl_registry  # noqa: E402
from taskq.ratelimit.token_bucket import TokenBucket  # noqa: E402
from taskq.web.admin import create_router, setup_admin_state  # noqa: E402

from . import StubConnection  # noqa: E402

pytestmark = [pytest.mark.fastapi]


class _OutageRedis:
    """Redis client double whose writes fail with the named outage class."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.delete_calls: list[str] = []

    async def delete(self, key: str) -> int:
        self.delete_calls.append(key)
        raise self._exc


class _RecordingConnection(StubConnection):
    """StubConnection that records every statement bound through it."""

    def __init__(self) -> None:
        super().__init__()
        self.executed: list[str] = []

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append(query)
        return ""


class _AuditWatchPool:
    """Pool double handing out the recording connection.

    The route's audit row reaches the database through BoundedPool, which
    yields the raw connection from :meth:`acquire`, so statements issued
    by ``record_admin_action_safe`` surface in ``conn.executed``."""

    def __init__(self, conn: StubConnection) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> Any:
        return self

    async def __aenter__(self) -> StubConnection:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass

    def get_size(self) -> int:
        return 1

    def get_idle_size(self) -> int:
        return 0


def _make_app(pool: Any, **kwargs: Any) -> TestClient:
    bundle = create_router(pool, **kwargs)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)  # type: ignore[arg-type]


def _get_csrf_token(client: TestClient) -> str:
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token") or ""


@pytest.mark.parametrize(
    "outage",
    [
        redis_mod.exceptions.OutOfMemoryError(
            "OOM command not allowed when used memory > 'maxmemory'"
        ),
        redis_mod.exceptions.ReadOnlyError("You can't write against a read only replica"),
    ],
    ids=["oom", "readonly"],
)
def test_rate_limit_reset_redis_outage_is_a_503_not_a_500(
    monkeypatch: pytest.MonkeyPatch, outage: BaseException
) -> None:
    """A redis outage rejection on the reset answers the route's own 503 /
    Retry-After shape, never a raw 500: the same outage classes #421
    absorbs on the acquire path must not fall out of the operator path
    unhandled."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="redis")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})
    outage_redis = _OutageRedis(outage)
    conn = _RecordingConnection()
    watch = _AuditWatchPool(conn)
    client = _make_app(watch, redis_client=outage_redis)
    token = _get_csrf_token(client)
    resp = client.post(
        "/rate-limits/api%3Aglobal/reset", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 503  # pyright: ignore[reportUnknownMemberType]
    assert resp.headers["retry-after"] == "2"  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.parametrize(
    "outage",
    [
        redis_mod.exceptions.OutOfMemoryError(
            "OOM command not allowed when used memory > 'maxmemory'"
        ),
        redis_mod.exceptions.ReadOnlyError("You can't write against a read only replica"),
    ],
    ids=["oom", "readonly"],
)
def test_rate_limit_reset_redis_outage_writes_no_audit_row(
    monkeypatch: pytest.MonkeyPatch, outage: BaseException
) -> None:
    """The outage refusal is a refused mutation (#429's own contract): no
    admin_audit row may attribute a reset that did not happen."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="redis")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})
    outage_redis = _OutageRedis(outage)
    conn = _RecordingConnection()
    watch = _AuditWatchPool(conn)
    client = _make_app(watch, redis_client=outage_redis)
    token = _get_csrf_token(client)
    client.post(
        "/rate-limits/api%3Aglobal/reset", data={"csrf_token": token}, follow_redirects=False
    )
    assert not any("admin_audit" in sql for sql in conn.executed)


def test_no_audit_row_pin_is_non_vacuous(monkeypatch: pytest.MonkeyPatch) -> None:
    """Control for the no-audit pins above: a reset that SUCCEEDS does pass
    an admin_audit INSERT through the same watched pool, so the refusal
    pins cannot pass vacuously on a watch that records nothing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="memory")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})
    conn = _RecordingConnection()
    watch = _AuditWatchPool(conn)
    client = _make_app(watch)
    token = _get_csrf_token(client)
    resp = client.post(
        "/rate-limits/api%3Aglobal/reset", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert any("admin_audit" in sql for sql in conn.executed)
