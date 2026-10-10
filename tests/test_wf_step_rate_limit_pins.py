"""THE PER-STEP TOKEN BUCKET ON THE AUTHORING SURFACE (the consumer-face
lane's CURE 2 — the verified gap): the workflow authoring face had NO
rate limit at all — ``taskq.actor``'s ``rate_limits=[TokenBucket(...)]``
is vanilla-only, ``WorkflowApp.actor()``/``step()``/``RouteArm`` carried
nothing, and "5 concurrent" (the queue cap) is not "50/min" under the
LLM fleet's varying latency.

THE CURE'S SHAPE (ONE mechanism, the authoring face added):

* the authoring surface: ``step(body, rate_limits=[TokenBucket(...)])``,
  ``RouteArm(body=..., rate_limits=[...])`` (the typed route's per-arm
  buckets), ``map_source(src, item, rate_limits=[...])`` (the plain
  map's children's buckets) — the VANILLA union's shapes; a keyed ref
  names no concrete bucket (the fork stamps names) and is REFUSED at
  the wiring site;
* the fork stamps the buckets' NAMES onto the child rows' metadata (the
  static step's row too) — the ROW carries its own admission terms;
* the claim path honors them: the intercept's rate-limit gate acquires
  the row's buckets through the SAME registry + the SAME denial path
  the queue-concurrency fence rides (``_acquire_for_actor_with_denial_retry``
  — the AND-composition, the observable deny, the never-bursts cadence);
* the boot collects: the workflow-declared bucket instances register
  into the worker's ``RateLimitRegistry`` (the vanilla actors' own
  collection pass's sibling); a ``str`` name nothing registers is the
  WARNING (W2's own register — probably a typo, never a refusal);
  at claim an unknown name is the FAIL-CLOSED arm (the row parks
  budget-free, LOUD — never a silent pass-through, the admitted-never-
  limited lie).

Red-first: the pins ran RED at the pre-cure head; the reds are captured
in ``.measurements/cons-cure2-reds.json``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq.backend._protocol import JobId, JobRow
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _flow_rate_limit_gate
from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)
from taskq.workflows.api._graph import rate_limit_names  # the fork's own read
from tests._wf_fixtures import RedLog
from tests.conftest import unique_health_sock_path

pytestmark = pytest.mark.integration


@pytest.fixture
def cons2_redlog() -> RedLog:
    """The red sink for the consumer-face lane's CURE 2 pins."""
    log = RedLog("cons-cure2-reds.json")
    return log


_START = datetime(2025, 1, 1, tzinfo=UTC)
CAPABLE_WORKER = UUID(int=7)


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Image(BaseModel):
    v: int


class Audio(BaseModel):
    v: int


async def _bucket_registry(
    name: str, capacity: float, refill: float, clock: Clock
) -> RateLimitRegistry:
    """A registry holding ONE memory-backend token bucket (the injected
    clock drives it — the deterministic cadence, no store)."""
    registry = RateLimitRegistry()
    registry.register(TokenBucket(name, capacity, refill, backend="memory"))
    return registry


def _bucket_job(bucket_names: list[str] | None) -> JobRow:
    metadata: dict[str, object] = {"flow_id": str(JobId(UUID(int=1)))}
    if bucket_names is not None:
        metadata["rate_limits"] = bucket_names
    return JobRow(
        id=JobId(UUID(int=2)),
        actor="wf",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_START,
        scheduled_at=_START,
        metadata=metadata,
        trace_id=None,
    )


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("cons2_step_rate_limit_pins"),
        }
    )


def _deps(real_pool: asyncpg.Pool, schema: str) -> WorkerDeps:
    return WorkerDeps(
        settings=_settings(schema),
        dispatcher_pool=real_pool,
        heartbeat_pool=real_pool,
        worker_pool=real_pool,
        notify_conn=None,
        leader_conn=None,
    )


JOB_LOG = structlog.get_logger("cons2_step_rate_limit_pins")


def _loads(raw: Any) -> Any:
    """jsonb on a raw connection rides as str — parse before indexing."""
    import json

    return json.loads(raw) if isinstance(raw, str) else raw


# ── THE AUTHORING FACE ───────────────────────────────────────────────────


def test_step_carries_its_rate_limits(cons2_redlog: RedLog) -> None:
    """``step(body, rate_limits=[TokenBucket(...)])`` — the compiled node
    carries the DECLARATION (the authoring face's existence)."""
    app = WorkflowApp()
    bucket = TokenBucket("cons2-step-bucket", 5, 1.0)

    @app.workflow("cons2_step_rl")
    def w() -> Promise[object]:
        return build(step(_ocr_body, Ingest(doc_id="d1"), key="ocr", rate_limits=[bucket]))

    node = app.get("cons2_step_rl").nodes["ocr"]
    if not hasattr(node, "rate_limits") or not node.rate_limits:
        cons2_redlog.red(
            "cons2-step-authoring-face",
            "step() accepts no rate_limits= — the authoring surface carries "
            "nothing (actor.py's face is vanilla-only)",
            {"compiled_fields": sorted(vars(node))},
        )
    assert node.rate_limits == (bucket,)


def test_route_arms_carry_their_own_rate_limits(cons2_redlog: RedLog) -> None:
    """THE PER-ARM BUCKETS (R4's placement sibling): the gpu arm's
    children ride the gpu bucket, the io arm's the io bucket — the ROWS
    name their arm's admission terms."""
    app = WorkflowApp()
    gpu_bucket = TokenBucket("cons2-gpu", 1, 0.1)
    io_bucket = TokenBucket("cons2-io", 2, 0.2)

    async def src(ctx: StepContext, params: Ingest) -> list[Image | Audio]:
        return [Image(v=1)]

    async def arm_image(ctx: StepContext, item: Image) -> Report:
        return Report(ref="img")

    async def arm_audio(ctx: StepContext, item: Audio) -> Report:
        return Report(ref="aud")

    @app.workflow("cons2_route_rl")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src")
        return build(
            route(
                source,
                {
                    Image: RouteArm(body=arm_image, rate_limits=(gpu_bucket,)),
                    Audio: RouteArm(body=arm_audio, rate_limits=(io_bucket,)),
                },
            )
        )

    compiled = app.get("cons2_route_rl")
    arms = compiled.nodes["src"].map_arms or {}
    from taskq.workflows.chain import type_tag

    image_key, audio_key = type_tag(Image), type_tag(Audio)
    if not (arms[image_key].rate_limits == (gpu_bucket,)):
        cons2_redlog.red(
            "cons2-route-arm-buckets",
            "RouteArm accepts no rate_limits= — the per-arm admission terms "
            "cannot be declared",
            {"arm_fields": sorted(arms[image_key].__dataclass_fields__)},
        )
    assert arms[image_key].rate_limits == (gpu_bucket,)
    assert arms[audio_key].rate_limits == (io_bucket,)


def test_map_source_children_carry_the_map_rate_limits(cons2_redlog: RedLog) -> None:
    """``map_source(src, item, rate_limits=[...])`` — the map's CHILDREN
    ride the declared buckets (the source node carries them for the
    fork)."""
    app = WorkflowApp()
    bucket = TokenBucket("cons2-map-bucket", 3, 0.3)

    @app.workflow("cons2_map_rl")
    def w() -> Promise[object]:
        source = step(_src_body, Ingest(doc_id="d1"), key="src")
        items = map_source(source, _item_body, rate_limits=[bucket])
        return build(items)

    node = app.get("cons2_map_rl").nodes["src"]
    if not getattr(node, "map_rate_limits", None):
        cons2_redlog.red(
            "cons2-map-children-buckets",
            "map_source accepts no rate_limits= — the children's admission "
            "terms cannot be declared",
            {},
        )
    assert node.map_rate_limits == (bucket,)


def test_a_keyed_ref_names_no_concrete_bucket_and_is_refused() -> None:
    """THE LOUD DOOR: a keyed ref's concrete bucket materializes per
    payload — the fork stamps NAMES onto rows, so a keyed ref on a
    workflow step is the refused shape (named at the wiring site, before
    any row exists)."""
    class Tenant(BaseModel):
        tenant_id: str

    app = WorkflowApp()
    keyed = KeyedRateLimitRef.typed(
        Tenant, base_name="cons2-tenant", key_fn=lambda p: p.tenant_id, capacity=1,
        refill_per_second=1.0,
    )

    @app.workflow("cons2_keyed_refusal")
    def w() -> Promise[object]:
        return build(step(_ocr_body, Ingest(doc_id="d1"), key="ocr", rate_limits=[keyed]))

    with pytest.raises(Exception, match="keyed|concrete|per-payload"):
        app.get("cons2_keyed_refusal")


def test_the_forks_name_read_is_the_instances_names() -> None:
    """The fork's stamp source: instances → their ``.name``, plain strs
    through (the vanilla union's own resolution, read once)."""
    assert rate_limit_names((TokenBucket("b", 1, 0), "named")) == ("b", "named")
    assert rate_limit_names(()) == ()


# ── THE FORK STAMPS THE ROWS ─────────────────────────────────────────────


async def test_the_fork_stamps_the_buckets_onto_the_child_rows(
    cons2_redlog: RedLog, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE FORK'S STAMP: a map flow's tick forks the children — each
    child row's metadata carries the map's declared bucket NAMES (the
    row its own admission terms; the claim path reads exactly this)."""
    bucket = TokenBucket("cons2-fork-stamp", 3, 0.3)
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[int]:
        return [0, 1]

    async def item(ctx: StepContext, value: int) -> dict[str, object]:
        return {"risk": value}

    @app.workflow("cons2_fork_stamp")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src")
        return build(map_source(source, item, rate_limits=[bucket]))

    runner = FlowRunner(app.get("cons2_fork_stamp"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)

    rows = await wf_pool.fetch(
        f"""SELECT step_key, map_index, metadata FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND step_key = 'src.item' ORDER BY map_index""",
        flow_id,
    )
    assert len(rows) == 2, "the fork created the two children"
    stamped = [
        (_loads(r["metadata"]) if isinstance(r["metadata"], str) else r["metadata"]).get("rate_limits")
        for r in rows
    ]
    if stamped != [["cons2-fork-stamp"], ["cons2-fork-stamp"]]:
        cons2_redlog.red(
            "cons2-fork-stamp",
            "the fork's child rows carry no rate_limits names — the claim "
            "path cannot honor what the rows do not say",
            {"stamped": stamped},
        )
    assert stamped == [["cons2-fork-stamp"], ["cons2-fork-stamp"]]


async def test_a_static_step_row_carries_its_own_bucket_names(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A plain step's row is the STATIC insert — its declared buckets'
    names ride its metadata the same way (one carrier, both faces)."""
    bucket = TokenBucket("cons2-static-stamp", 1, 0)
    app = WorkflowApp()

    @app.workflow("cons2_static_stamp")
    def w() -> Promise[object]:
        return build(step(_ocr_body, Ingest(doc_id="d1"), key="ocr", rate_limits=[bucket]))

    runner = FlowRunner(app.get("cons2_static_stamp"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    raw = await wf_pool.fetchval(
        f"""SELECT metadata FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'ocr'""",
        flow_id,
    )
    assert raw is not None
    meta = _loads(raw) if isinstance(raw, str) else raw
    assert meta.get("rate_limits") == ["cons2-static-stamp"]


# ── THE BOOT'S COLLECTION ────────────────────────────────────────────────


def test_the_boot_collection_registers_the_instances_and_names_the_unknowns(
    cons2_redlog: RedLog,
) -> None:
    """THE BOOT'S COLLECT (the vanilla actors' collection pass's
    sibling): every workflow-declared bucket instance registers into the
    resolved registry (idempotent, _same_config's law); a str name
    nothing registers is REPORTED (the warning's input — W2's register),
    never silently swallowed."""
    from taskq.workflows import _worker_execution

    if not hasattr(_worker_execution, "collect_workflow_rate_limits"):
        cons2_redlog.red(
            "cons2-boot-collection",
            "the boot's workflow rate-limit collection pass does not exist — "
            "the authoring face's buckets would never register",
            {},
        )
        pytest.fail("collect_workflow_rate_limits is absent — the red is the pin")

    bucket = TokenBucket("cons2-collected", 5, 1.0)
    app = WorkflowApp()

    @app.workflow("cons2_collect")
    def w() -> Promise[object]:
        source = step(_src_body, Ingest(doc_id="d1"), key="named", rate_limits=[bucket, "cons2-foreign"])
        mapped = map_source(source, _item_body, rate_limits=[TokenBucket("cons2-map-col", 1, 0)])
        return build(mapped)

    compiled = app.get("cons2_collect")
    assert compiled is not None
    registry = RateLimitRegistry()
    registered, unknown = _worker_execution.collect_workflow_rate_limits(registry)
    assert "cons2-collected" in registered and "cons2-map-col" in registered
    assert registry.has_rate_limit("cons2-collected")
    assert unknown == ["cons2-foreign"], "the unregistered str name is REPORTED"


# ── THE CLAIM PATH HONORS THEM ───────────────────────────────────────────


async def test_the_gate_acquires_the_rows_buckets_and_the_deny_is_observable(
    cons2_redlog: RedLog, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ACQUIRE + THE OBSERVABLE DENY: the row's metadata names the
    bucket; the gate drains a token per acquire; the burst past the
    capacity DENIES — the denial names the bucket, the denials counter
    bumps (the same observable the queue-concurrency fence keeps)."""
    clock = FakeClock(_START)
    registry = await _bucket_registry("cons2-gate-bucket", 2, 0.0, clock)
    job = _bucket_job(["cons2-gate-bucket"])

    denials: list[str] = []
    with patch("taskq.worker.run.record_ratelimit_denial", side_effect=denials.append):
        first = await _flow_rate_limit_gate(
            registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
        )
        second = await _flow_rate_limit_gate(
            registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
        )
        third = await _flow_rate_limit_gate(
            registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
        )
    assert first.denied is None and second.denied is None, "capacity 2 admits two"
    if third.denied is None:
        cons2_redlog.red(
            "cons2-claim-path-honors",
            "the gate ignores the row's rate_limits metadata — the third "
            "acquire admitted past the bucket (never-bursts broken)",
            {},
        )
    assert third.denied is not None, "the burst past the bucket DENIES (never a burst)"
    assert third.denied.bucket_name == "cons2-gate-bucket"
    assert denials == ["postgres"]


async def test_the_cadence_the_bucket_rate_holds_under_the_burst(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE CADENCE (the never-bursts pin's pattern): capacity 1, refill
    1/s — acquire, deny, TIME, acquire. The third admission happens ONLY
    after the refill: the bucket's rate is the admission's clock, the
    latency variance cannot buy a burst."""
    clock = FakeClock(_START)
    registry = await _bucket_registry("cons2-cadence-bucket", 1, 1.0, clock)
    job = _bucket_job(["cons2-cadence-bucket"])

    first = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    second = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert first.denied is None and second.denied is not None

    clock.advance(timedelta(seconds=1.0))  # ONE token refills — exactly one more admission
    third = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert third.denied is None, "the refilled token admits exactly one"

    fourth = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert fourth.denied is not None, "the rate holds: no token, no admission"


async def test_an_unknown_bucket_name_is_the_fail_closed_arm(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE UNKNOWN NAME (the refusal/warning face's claim-time teeth): a
    row naming a bucket NO registry entry backs parks budget-free — the
    FAIL-CLOSED arm (the dependency arm's shape), LOUD, never a silent
    pass-through (the admitted-never-limited lie), never a crash."""
    clock = FakeClock(_START)
    registry = RateLimitRegistry()
    gate = await _flow_rate_limit_gate(
        registry,
        clock,
        _bucket_job(["cons2-no-such-bucket"]),
        CAPABLE_WORKER,
        _deps(wf_pool, wf_schema),
        job_log=JOB_LOG,
    )
    assert gate.dependency_failure is not None, "the unknown name fails CLOSED"
    assert gate.denied is None and gate.acquired == []


async def test_the_no_registry_fast_path_still_passes_through(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The no-caps fleet's law HOLDS (the vanilla shape): no registry —
    the gate passes everything through; a job with NO rate_limits
    metadata rides the cap-only path unchanged (byte-identity for the
    queue-concurrency fence's own pins)."""
    clock = FakeClock(_START)
    bare = await _flow_rate_limit_gate(
        None, clock, _bucket_job(["cons2-never-registered"]), CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert bare.acquired == [] and bare.denied is None
    plain = await _flow_rate_limit_gate(
        RateLimitRegistry(), clock, _bucket_job(None), CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert plain.acquired == [] and plain.denied is None


# ── the pin flows' bodies (module-level, D1-registered) ──────────────────


async def _ocr_body(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _src_body(ctx: StepContext, params: Ingest) -> list[int]:
    return [0, 1]


async def _item_body(ctx: StepContext, value: int) -> dict[str, object]:
    return {"risk": value}
