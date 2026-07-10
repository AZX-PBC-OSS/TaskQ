"""Dependency-injection actors — LOOP-scope and TRANSIENT-scope providers.

These actors demonstrate TaskQ's DI system:

- ``FakeHttpClient`` is registered at ``Scope.LOOP`` via an async generator
  factory, so one shared instance is created per worker event loop and torn
  down on worker shutdown.
- ``FakeDb`` is registered at ``Scope.TRANSIENT`` via a plain factory, so
  each actor invocation receives a fresh instance.
- ``fetch_actor`` injects both the client and context, returns a typed result,
  and writes a structured log line via ``ctx.log``.
- ``db_lookup_actor`` injects only the DB session (no ctx) to show the
  minimal DI-only signature.

``build_registry()`` is called by ``worker.py`` and passed to
``worker_main(di_registry=...)`` so the worker knows how to construct these
providers before any job is dispatched.
"""

from collections.abc import AsyncGenerator
from datetime import timedelta

from pydantic import BaseModel

from taskq import JobContext, actor
from taskq.di import ProviderRegistry, Scope


class FakeHttpClient:
    """Toy HTTP client — simulates a real async HTTP session."""

    def __init__(self) -> None:
        self._closed = False

    async def get(self, url: str) -> dict[str, object]:
        return {"url": url, "status": 200, "body": f"<html>content of {url}</html>"}

    async def aclose(self) -> None:
        self._closed = True


class FakeDb:
    """Toy DB session — simulates a per-request database connection."""

    def query(self, sql: str, *params: object) -> list[dict[str, object]]:
        return [{"id": 1, "sql": sql, "params": list(params), "result": "ok"}]


async def _http_client_factory() -> AsyncGenerator[FakeHttpClient, None]:
    client = FakeHttpClient()
    yield client
    await client.aclose()


def _db_factory() -> FakeDb:
    return FakeDb()


class FetchPayload(BaseModel):
    url: str = "https://example.com"


class FetchResult(BaseModel):
    status: int
    body: str


class DbLookupPayload(BaseModel):
    item_id: int = 1


@actor(name="fetch", queue="examples", result_ttl=timedelta(minutes=5))
async def fetch_actor(
    payload: FetchPayload,
    ctx: JobContext[FetchPayload],
    *,
    http: FakeHttpClient,
) -> FetchResult:
    """Fetches a URL via injected HTTP client — demonstrates LOOP-scope DI and result_ttl."""
    data = await http.get(payload.url)
    ctx.log.info("fetch_complete", url=payload.url, status=data["status"])
    return FetchResult(status=int(data["status"]), body=str(data["body"]))


@actor(name="db_lookup", queue="examples")
async def db_lookup_actor(
    payload: DbLookupPayload,
    *,
    db: FakeDb,
) -> None:
    """Queries the fake DB — demonstrates TRANSIENT-scope DI (fresh session per job)."""
    rows = db.query("SELECT * FROM items WHERE id = $1", payload.item_id)
    _ = rows


def build_registry() -> ProviderRegistry:
    """Build and return a configured DI registry for the example worker.

    Called by worker.py and passed to worker_main(di_registry=...).
    Do NOT call validate() here — the worker does that during bootstrap.
    """
    registry = ProviderRegistry()
    registry.register_factory(FakeHttpClient, Scope.LOOP, _http_client_factory)
    registry.register_factory(FakeDb, Scope.TRANSIENT, _db_factory)
    return registry
