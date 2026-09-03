"""Worker startup warning for served actors on queues this worker does not consume.

Issue #90: an actor registered ``@actor(queue="cron")`` with a worker
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

from pathlib import Path

import structlog.testing
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker.run import _emit_unconsumed_queue_startup_warnings, _startup_log

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
