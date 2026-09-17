"""Worker startup warning for served actors on queues this worker does not consume.

The failure: an actor registered ``@actor(queue="cron")`` with a worker
consuming only ``["default"]`` never dispatches. Enqueue succeeds, cron
keeps firing, and the jobs sit pending forever because the dispatch CTE
unnests ``$1::text[]`` — the worker's own ``settings.queues`` — and
claims only jobs whose queue matches; nothing fails and nothing logs.
Decoration-time validation checks queue NAME FORMAT only, so bootstrap,
where a process holds both every served actor's declared queue and its
own consumed queues, is the only fix surface.

Red-team rework notes pinned by this file:

- ONE aggregated event per boot, not one per actor: workgroup children
  each import the full actor registry while consuming a queue subset,
  so per-actor warnings would storm exactly the healthy heterogeneous
  fleets the issue blesses. The affected actor→queue mapping rides the
  ``actors`` field (the shape documented in docs/guides/workers.md, from
  the parallel agent's committed variant), with the distinct unconsumed
  queue names in ``queues`` and the worker's subscription in
  ``worker_queues``.
- Empty ``settings.queues`` is a different, unambiguous failure — a
  worker that dispatches nothing — with its own single event.
- A wiring pin: helper-level tests stay green when the production call
  in ``_main`` is deleted, so the call itself is asserted structurally.

Two cases are ported from the parallel agent's superseded
``test_worker_queue_subscription_warnings.py`` (its remaining cases
duplicated the controls above): the all-consumed multi-queue worker,
and the two-unconsumed-queues mapping that discriminates per-actor
filtering across multiple offending queues.

Pure-Python unit tests — no PG required. Warnings are asserted as
actually emitted (event, level, fields) via ``structlog.testing.
capture_logs``, mirroring ``tests/test_migrate_on_start_worker.py``;
the emitter is imported through the ``taskq.worker.run`` re-export seam,
matching ``tests/test_worker_startup_warnings.py``.
"""

import logging
from pathlib import Path

import asyncpg
import pytest
import structlog.testing
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_base62
from taskq.actor import ActorRef, actor
from taskq.migrate import apply_pending
from taskq.obs import setup_logging
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import _open_pg_backend_on_schema
from taskq.testing.health import unique_health_sock_path
from taskq.testing.jobs import make_enqueue_args
from taskq.worker.run import (
    _emit_unconsumed_queue_startup_warnings,
    _startup_log,
    worker_main_async,
)

_AGGREGATE_EVENT = "actors-on-unconsumed-queues"
_EMPTY_QUEUES_EVENT = "worker-consumes-no-queues"


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_settings(*, queues_csv: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
            "TASKQ_QUEUES": queues_csv,
        },
    )


def _make_actor_ref(*, name: str, queue: str) -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue=queue,
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


def test_unconsumed_actors_aggregate_into_one_warning() -> None:
    """Actor on a queue the worker does not consume → exactly ONE warning
    carrying the actor→queue mapping and the worker's queues. Two
    consumed queues must not multiply the warning."""
    settings = _make_settings(queues_csv="default,batch")
    registry = {"nightly": _make_actor_ref(name="nightly", queue="cron")}

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    matches = [e for e in logs if e["event"] == _AGGREGATE_EVENT]
    assert len(matches) == 1, f"exactly one aggregated warning expected: {logs}"
    entry = matches[0]
    assert entry["log_level"] == "warning"
    assert entry["actors"] == {"nightly": "cron"}
    assert entry["queues"] == ["cron"]
    assert entry["worker_queues"] == ["default", "batch"]


def test_actor_on_consumed_queue_emits_no_warning() -> None:
    """Control: an actor whose declared queue IS consumed must produce no
    warning, or every correctly configured worker gets log noise."""
    settings = _make_settings(queues_csv="default")
    registry = {"alpha": _make_actor_ref(name="alpha", queue="default")}

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    assert [e for e in logs if e["event"] == _AGGREGATE_EVENT] == [], (
        f"no warning may fire when the actor's queue is consumed: {[e['event'] for e in logs]}"
    )


def test_empty_actor_registry_emits_no_warning() -> None:
    """No served actors → nothing to compare; the aggregate stays silent
    (the empty-queues event is a different condition)."""
    settings = _make_settings(queues_csv="default")
    registry: dict[str, ActorRef[_Payload, _Result]] = {}

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    assert [e for e in logs if e["event"] == _AGGREGATE_EVENT] == []


def test_two_actors_sharing_an_unconsumed_queue_aggregate_into_one_warning() -> None:
    """One event per boot, not one per actor or per queue occurrence: two
    actors sharing the same unconsumed queue produce a single warning
    whose fields carry both names."""
    settings = _make_settings(queues_csv="default")
    registry = {
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
        "digest": _make_actor_ref(name="digest", queue="cron"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    matches = [e for e in logs if e["event"] == _AGGREGATE_EVENT]
    assert len(matches) == 1, f"one aggregated warning expected, got {len(matches)}: {logs}"
    assert matches[0]["actors"] == {"digest": "cron", "nightly": "cron"}
    assert matches[0]["queues"] == ["cron"]


def test_mixed_registry_names_only_the_unconsumed_actors() -> None:
    """Subset discrimination: with one actor's queue consumed and another's
    not, exactly one warning fires and its fields name only the
    unconsumed actor — the case a degenerate "if any actor is
    unconsumed, warn for every actor" implementation fails (alpha would
    appear in the event)."""
    settings = _make_settings(queues_csv="default")
    registry = {
        "alpha": _make_actor_ref(name="alpha", queue="default"),
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    matches = [e for e in logs if e["event"] == _AGGREGATE_EVENT]
    assert len(matches) == 1, f"only the unconsumed actor may be warned about: {logs}"
    assert matches[0]["actors"] == {"nightly": "cron"}
    assert matches[0]["queues"] == ["cron"]
    assert not any(
        "alpha" in (e.get("actors") or {}) for e in logs if e["log_level"] == "warning"
    ), "zero warnings may mention the consumed actor"


def test_multi_queue_worker_consuming_all_actor_queues_stays_silent() -> None:
    """Ported from the parallel agent's superseded suite: a worker
    consuming several queues with actors targeting each of them is the
    legitimate split-queue topology on ONE worker — per-actor silence
    must hold, not just for the everything-on-default shape."""
    settings = _make_settings(queues_csv="default,cron")
    registry = {
        "inline": _make_actor_ref(name="inline", queue="default"),
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    assert [e for e in logs if e["event"] == _AGGREGATE_EVENT] == [], (
        f"a worker consuming every actor's queue must stay silent: {logs}"
    )


def test_mixed_registry_two_unconsumed_queues_maps_each_actor_to_its_queue() -> None:
    """Ported from the parallel agent's superseded suite: one consumed
    actor plus two unconsumed actors on DIFFERENT queues — the case that
    discriminates the actor→queue mapping itself, which two parallel
    name/queue lists cannot express (who is on which queue?). The mapping
    is emitted sorted by actor name so the event is byte-stable across
    boots."""
    settings = _make_settings(queues_csv="default")
    registry = {
        "alpha": _make_actor_ref(name="alpha", queue="default"),
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
        "mailer": _make_actor_ref(name="mailer", queue="email"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    matches = [e for e in logs if e["event"] == _AGGREGATE_EVENT]
    assert len(matches) == 1, f"one aggregated warning expected: {logs}"
    entry = matches[0]
    assert entry["actors"] == {"mailer": "email", "nightly": "cron"}
    assert list(entry["actors"].items()) == [("mailer", "email"), ("nightly", "cron")], (
        "the mapping must be sorted by actor name for stable event content"
    )
    assert entry["queues"] == ["cron", "email"]
    assert entry["worker_queues"] == ["default"]


def test_aggregate_warning_carries_the_fleet_caveat_note() -> None:
    """The note field is the only carrier of the heterogeneous-fleet
    legitimacy the issue demands — without it the warning reads as a hard
    error and operators of intentionally split fleets learn to filter it.
    Dropping the note must fail this test."""
    settings = _make_settings(queues_csv="default")
    registry = {"nightly": _make_actor_ref(name="nightly", queue="cron")}

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    entry = next(e for e in logs if e["event"] == _AGGREGATE_EVENT)
    note = entry.get("note")
    assert isinstance(note, str) and note, "the aggregate warning must carry a note"
    assert "another worker in the fleet" in note, "the note must bless the split-fleet case"
    assert "TASKQ_QUEUES" in note, "the note must name the remedy"


def test_empty_queues_with_actors_warns_worker_consumes_nothing() -> None:
    """TASKQ_QUEUES="" parses to queues == [] — a worker that provably
    dispatches nothing, a certain misconfiguration rather than the
    ambiguous fleet case: exactly one distinct event, and no per-actor
    aggregate whose note claims "another worker may legitimately consume
    the queue" — wrong text for a worker that consumes nothing at all."""
    settings = _make_settings(queues_csv="")
    registry = {
        "alpha": _make_actor_ref(name="alpha", queue="default"),
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    empty_matches = [e for e in logs if e["event"] == _EMPTY_QUEUES_EVENT]
    assert len(empty_matches) == 1, f"exactly one {_EMPTY_QUEUES_EVENT} warning expected: {logs}"
    assert empty_matches[0]["log_level"] == "warning"
    assert empty_matches[0]["worker_queues"] == [], "the event must carry the empty subscription"
    empty_note = empty_matches[0].get("note")
    assert isinstance(empty_note, str) and "TASKQ_QUEUES" in empty_note, (
        "the empty-queues note must name the setting and the remedy"
    )
    assert [e for e in logs if e["event"] == _AGGREGATE_EVENT] == [], (
        "the per-actor aggregate must not fire when the worker consumes nothing"
    )


def test_empty_queues_with_empty_registry_still_warns_once() -> None:
    """A worker consuming nothing is broken even with no served actors —
    the empty-queues warning does not depend on the registry."""
    settings = _make_settings(queues_csv="")
    registry: dict[str, ActorRef[_Payload, _Result]] = {}

    with structlog.testing.capture_logs() as logs:
        _emit_unconsumed_queue_startup_warnings(settings, registry, _startup_log)

    assert len([e for e in logs if e["event"] == _EMPTY_QUEUES_EVENT]) == 1


def test_main_wires_the_warning_after_worker_id_and_before_sync() -> None:
    """Structural wiring pin: ``_main`` must call the emitter exactly once,
    inside an ``actor_registry is not None`` guard, after
    ``bind_contextvars`` (worker_id correlation with the workers-table
    row) and before ``sync_actor_config`` (whose drift raise or
    pool-acquire stall would swallow a warning placed after it).

    Red-team motivation: deleting the production call leaves every
    helper-level unit test green — they invoke the helper directly — so
    the call itself needs its own guard. AST-based, not source-text
    matching, so it is robust to reformatting (the
    ``test_sweep_loop_acquire_calls_pass_timeout_ast`` house pattern).
    """
    import ast

    import taskq.worker._bootstrap as bootstrap_mod

    source = ast.parse(Path(bootstrap_mod.__file__).read_text())

    main_fn = next(
        (
            node
            for node in ast.walk(source)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_main"
        ),
        None,
    )
    assert main_fn is not None, "_main must exist in taskq.worker._bootstrap"

    def _call_name(call: ast.Call) -> str | None:
        func = call.func
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return None

    emit_calls: list[ast.Call] = []
    bind_calls: list[ast.Call] = []
    sync_calls: list[ast.Call] = []
    for node in ast.walk(main_fn):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if name == "_emit_unconsumed_queue_startup_warnings":
            emit_calls.append(node)
        elif name == "bind_contextvars":
            bind_calls.append(node)
        elif name == "sync_actor_config":
            sync_calls.append(node)

    assert len(emit_calls) == 1, (
        "expected exactly one _emit_unconsumed_queue_startup_warnings call "
        f"in _main, found {len(emit_calls)} at lines {[c.lineno for c in emit_calls]}"
    )
    emit = emit_calls[0]
    assert bind_calls, "_main must call bind_contextvars"
    assert sync_calls, "_main must call sync_actor_config"

    guard_holds_emit = any(
        any(call is emit for call in ast.walk(node) if isinstance(call, ast.Call))
        for node in ast.walk(main_fn)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "actor_registry"
        and len(node.test.ops) == 1
        and isinstance(node.test.ops[0], ast.IsNot)
    )
    assert guard_holds_emit, (
        "the _emit_unconsumed_queue_startup_warnings call must live inside "
        "an `if actor_registry is not None:` guard in _main"
    )

    assert emit.lineno > min(b.lineno for b in bind_calls), (
        "the emit call must come AFTER bind_contextvars so the warning "
        "carries the worker_id correlation"
    )
    assert emit.lineno < min(s.lineno for s in sync_calls), (
        "the emit call must come BEFORE sync_actor_config, whose drift "
        "raise or pool-acquire stall would swallow the warning"
    )


# ── The warning never becomes a refusal ──────────────────────────────


@pytest.mark.integration
async def test_worker_with_actors_on_unconsumed_queues_still_boots_and_works(
    pg_dsn: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A worker whose registry contains an actor on a queue it does not
    consume must still start and must still run the work it CAN run.

    Queue coverage is a fleet-wide fact no single worker can decide: a
    sibling worker elsewhere may well consume that queue, and heterogeneous
    fleets that split queues across workers are the normal deployment. If
    this condition refused the boot, scaling a fleet out by adding a worker
    that serves a queue subset would take that worker down instead, and the
    operator would see a crash loop rather than the queue coverage warning.
    The behaviour asserted here is: boot succeeds, the warning is emitted,
    and a job on the consumed queue actually reaches a terminal success.
    """
    schema = f"twuq_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    ran: list[str] = []

    class _Value(BaseModel):
        marker: str

    @actor(name="served_actor", queue="default")
    async def served_actor(payload: _Value) -> None:
        ran.append(payload.marker)

    @actor(name="unserved_actor", queue="reports")
    async def unserved_actor(payload: _Value) -> None:  # pragma: no cover - never dispatched
        ran.append("unserved")

    registry = {"served_actor": served_actor, "unserved_actor": unserved_actor}

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "queues": "default",
            "health_socket_path": unique_health_sock_path("unconsumed_queue"),
        }
    )

    stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema_name=schema)
    try:
        await backend.enqueue(
            make_enqueue_args(
                actor="served_actor",
                queue="default",
                payload={"marker": "ran"},
            )
        )
    finally:
        await stack.aclose()

    # Observe the warning where an operator observes it: the stdlib logging
    # stream. ``setup_logging`` is the production configurator and is
    # idempotent, so this only guarantees the routing is in place rather
    # than depending on some earlier test in the session having set it up.
    setup_logging(level="INFO", log_format="json")
    with caplog.at_level(logging.WARNING):
        code = await worker_main_async(
            settings,
            actor_registry=registry,
            cron_registry=[],
            until_idle=True,
            idle_settle_window=0.3,
            idle_poll_interval=0.1,
            idle_max_runtime=45.0,
        )

    assert code == 0, "a worker that can serve its own queues must not refuse to start"
    assert ran == ["ran"], (
        "the worker must still execute jobs on the queue it does consume; "
        f"handler invocations: {ran}"
    )

    coverage_records = [
        record for record in caplog.records if _AGGREGATE_EVENT in record.getMessage()
    ]
    assert len(coverage_records) == 1, (
        "the unconsumed queue must produce exactly one loud boot warning, "
        f"got {len(coverage_records)}"
    )
    record = coverage_records[0]
    assert record.levelno == logging.WARNING, (
        "the coverage gap is diagnosable-but-workable: a warning, never an error"
    )
    message = record.getMessage()
    assert "unserved_actor" in message and "reports" in message, (
        f"the warning must name the actor and its queue so the operator can act: {message}"
    )

    verify_conn = await asyncpg.connect(pg_dsn)
    try:
        statuses = await verify_conn.fetch(
            f'SELECT actor, status FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-generated identifier, not user input.
        )
    finally:
        await verify_conn.close()

    assert [(row["actor"], row["status"]) for row in statuses] == [("served_actor", "succeeded")], (
        "the job on the consumed queue must reach a terminal success"
    )


# ── The queue that routes is the stored assignment ───────────────────


@pytest.mark.integration
@pytest.mark.parametrize("stored_queue", ["default", "retired_tier"])
async def test_boot_queue_coverage_discriminates_on_the_stored_assignment(
    pg_dsn: str,
    caplog: pytest.LogCaptureFixture,
    stored_queue: str,
) -> None:
    """Boot's queue-coverage signal must tell apart an actor whose stored
    assignment this worker consumes from one whose stored assignment it does
    not — the literal is the same in both cases.

    The stored assignment is the operator-owned one: it is what an actor move
    rewrites, what the cron leader's fires follow, and what routes every
    re-pended row. A worker holding a literal that disagrees with it is the
    normal, blessed state during the rolling deploy that ships the matching
    code, so a signal that fires on literal-vs-stored disagreement alone says
    nothing about coverage: it fires just as loudly on the healthy deploy
    window as on the actor whose routed work nothing can claim.

    Both parameters here run the identical registry and subscription and
    differ only in the stored assignment, so a coverage signal that cannot
    separate them is not a coverage signal. Whether *any* worker consumes the
    queue stays a fleet-wide question this process cannot answer — hence a
    warning and never a refusal, which the pin also asserts.
    """
    consumed = stored_queue == "default"
    schema = f"twsa_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    ran: list[str] = []

    class _Value(BaseModel):
        marker: str

    @actor(name="moved_actor", queue="default")
    async def moved_actor(payload: _Value) -> None:
        ran.append(payload.marker)

    registry = {"moved_actor": moved_actor}

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "queues": "default",
            "health_socket_path": unique_health_sock_path("stored_assignment"),
        }
    )

    # The stored assignment is the only thing that varies between the two
    # parameters. The code literal names "default" either way, so nothing
    # about the registry or the subscription distinguishes them.
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608  # Why: schema is a test-generated identifier, not user input.
            "moved_actor",
            stored_queue,
        )
    finally:
        await conn.close()

    stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema_name=schema)
    try:
        await backend.enqueue(
            make_enqueue_args(
                actor="moved_actor",
                queue="default",
                payload={"marker": "ran"},
            )
        )
    finally:
        await stack.aclose()

    setup_logging(level="INFO", log_format="json")
    with caplog.at_level(logging.WARNING):
        code = await worker_main_async(
            settings,
            actor_registry=registry,
            cron_registry=[],
            until_idle=True,
            idle_settle_window=0.3,
            idle_poll_interval=0.1,
            idle_max_runtime=45.0,
        )

    assert code == 0, "queue coverage is a warning, never a refusal"
    assert ran == ["ran"], (
        f"the worker must still run the work it can claim; handler invocations: {ran}"
    )

    warnings = [
        record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING
    ]
    coverage_warnings = [
        message
        for message in warnings
        if _AGGREGATE_EVENT in message or _EMPTY_QUEUES_EVENT in message
    ]

    if consumed:
        assert coverage_warnings == [], (
            "this worker consumes the actor's stored assignment, so its routed "
            "work is claimable here and no coverage warning belongs at boot: "
            f"{coverage_warnings}"
        )
        return

    assert coverage_warnings, (
        "boot emitted no queue-coverage warning although this actor's stored "
        f"assignment is {stored_queue!r}, a queue this worker does not consume "
        "— so every re-pended row and every cron fire for it routes somewhere "
        "this process cannot claim. The coverage check reads the "
        "@actor(queue=...) literal ('default', which this worker does consume) "
        "instead of the stored assignment, so the one state where the actor's "
        "routed work provably cannot be claimed here is the state it stays "
        f"silent for. Warnings emitted: {warnings}"
    )
    assert any(
        "moved_actor" in message and stored_queue in message for message in coverage_warnings
    ), (
        "the coverage warning must name the actor and the queue its work is "
        f"routed to, so an operator can act on it directly: {coverage_warnings}"
    )
