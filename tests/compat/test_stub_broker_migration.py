"""stub_broker compatibility test.

Exercises an actor that fetches data via HTTP and writes to Neo4j using
``memory_jobs`` and ``actor_runner`` with stub HTTP / Neo4j collaborators.
Validates that the test surface is equivalent to a legacy ``stub_broker``
pattern.

The test uses the direct-call form of ``actor_runner`` (no enqueue /
drain loop) for simplicity. A TODO references the full
enqueue-then-drain form for a follow-up if needed.

No ``EventLoopThread`` or ``stub_worker.join()`` is required; both are
replaced by ``await actor_runner(...)``.
"""

from pydantic import BaseModel

from taskq.testing.fixtures import ActorRunnerCallable
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.job_context import JobContext

# ── Inline Pydantic models ─────────────────────────────────────────────


class ThirdPartyUpdatePayload(BaseModel):
    """Minimal payload model for a third-party property update."""

    portfolio_id: str
    property_id: str


# ── Stub collaborators ────────────────────────────────────────────────


class StubHttpClient:
    """Stub HTTP client that records calls and returns canned responses."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def get(self, url: str) -> dict[str, object]:
        self.calls.append(("GET", url))
        return {"status": "ok", "data": {"property_name": "123 Main St"}}

    async def post(self, url: str, body: dict[str, object]) -> dict[str, object]:
        self.calls.append(("POST", url))
        return {"status": "ok"}


class StubNeo4jSession:
    """Stub Neo4j session that records queries and returns empty results."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    async def run(self, query: str, **params: object) -> list[dict[str, object]]:
        self.queries.append(query)
        return []


# ── Actor definition ──────────────────────────────────────────────────


async def update_property(
    payload: object,
    ctx: JobContext[BaseModel],
) -> dict[str, str]:
    """Actor that fetches property data via HTTP and writes to Neo4j.

    Uses the injected ``http_client`` and ``neo4j`` deps from ctx.deps.
    """
    assert ctx.deps is not None
    http: StubHttpClient = ctx.deps["http_client"]  # type: ignore[assignment]
    neo4j: StubNeo4jSession = ctx.deps["neo4j"]  # type: ignore[assignment]

    pl = (
        payload
        if isinstance(payload, ThirdPartyUpdatePayload)
        else ThirdPartyUpdatePayload.model_validate(payload)
    )

    # Fetch property data
    await http.get(f"/properties/{pl.property_id}")

    # Write to Neo4j
    await neo4j.run(
        "MERGE (p:Property {id: $id}) SET p.updated = true",
        id=pl.property_id,
    )

    return {"status": "succeeded"}


# ── Test ───────────────────────────────────────────────────────────────


async def test_update_property_happy_path(
    memory_jobs: InMemoryBackend,
    actor_runner: ActorRunnerCallable,
) -> None:
    """stub_broker drop-in compatibility.

    Verifies that the actor completes successfully via the direct-call
    form of ``actor_runner``, with stub HTTP and Neo4j collaborators
    injected via ``**deps``. No ``EventLoopThread`` or
    ``stub_worker.join()`` — replaced by ``await actor_runner(...)``.
    """
    http = StubHttpClient()
    neo4j = StubNeo4jSession()

    result = await actor_runner(
        update_property,
        ThirdPartyUpdatePayload(portfolio_id="p1", property_id="prop42"),
        backend=memory_jobs,
        http_client=http,
        neo4j=neo4j,
    )

    # Actor returned success
    assert result == {"status": "succeeded"}

    # Stub collaborators were called
    assert len(http.calls) == 1
    assert http.calls[0] == ("GET", "/properties/prop42")
    assert len(neo4j.queries) == 1

    # TODO: Add the full enqueue-then-drain form:
    # 1. Register the actor stub on memory_jobs
    # 2. Enqueue the job
    # 3. Call memory_jobs.run_until_drained()
    # 4. Assert via memory_jobs.get(job_id) that status == "succeeded"
    # and attempt count == 1.
    # This validates the full dispatch loop; the direct-call form here
    # validates the actor body in isolation.
