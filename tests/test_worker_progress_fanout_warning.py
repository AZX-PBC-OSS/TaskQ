"""Worker startup warning for progress publishes that cannot fan out.

Issue #341's consumer-side degradation chain is loud: the SSE endpoint
serves 503 ``redis_not_configured`` (web/progress.py), clients fall back
to 500 ms Postgres polling (client/_handle.py), and the ready-check
reports ``redis_configured`` (worker/health.py). The one silent link was
the worker side: ``context.py``'s per-call publish block keys on the
resolved Redis client, so with ``WorkerDeps.redis_client`` ``None`` the
publish is skipped quietly - the documented contract - and nothing at
boot names the resulting fanout gap. This pin holds that last link:
exactly ONE ``progress-fanout-unconfigured`` startup warning when the
worker's progress publishes will not fan out in real time, none when
they will.

Durable progress state still rides the Postgres flush, so this is a
latency degradation, not data loss; the warning surfaces the condition
at the only place that knows it before the first subscriber stares at a
polling stream.

Pure-Python unit tests - no PG required. Warnings are asserted as
actually emitted (event, level, fields) via ``structlog.testing.
capture_logs``, mirroring ``tests/test_worker_unconsumed_queue_warning.py``.
The wiring of the call inside ``_main`` is pinned structurally (AST),
for the same reason as there: deleting the production call leaves every
helper-level unit test green. The pin goes one step further and binds
the ARGUMENT: the call must pass ``deps.redis_client``, the exact
object the publish block tests - a revert to any other predicate (the
rate-limit gate's registry check included) changes the call shape and
fails here.
"""

import ast
from pathlib import Path

import structlog.testing

from taskq.worker._bootstrap import _emit_progress_fanout_unconfigured_warning

_EVENT = "progress-fanout-unconfigured"


def test_unresolved_client_warns_exactly_once() -> None:
    """``WorkerDeps.redis_client`` is ``None`` (no TASKQ_REDIS_URL, no
    caller-owned client, no factory) -> exactly ONE
    ``progress-fanout-unconfigured`` warning carrying a remedy that names
    a fix which actually wires the publish client."""
    with structlog.testing.capture_logs() as logs:
        _emit_progress_fanout_unconfigured_warning(None)

    matches = [e for e in logs if e["event"] == _EVENT]
    assert len(matches) == 1, f"exactly one warning expected: {logs}"
    entry = matches[0]
    assert entry["log_level"] == "warning"
    remedy = entry.get("remedy", "")
    assert "TASKQ_REDIS_URL" in remedy, "the remedy must name the env fix"
    assert "WorkerConnections" in remedy, (
        "the remedy must name the caller-owned client/factory fix: those "
        "wire the publish client with no URL set"
    )
    assert "DI provider" not in remedy, (
        "the remedy must not recommend the DI provider: a registered "
        "redis.asyncio.Redis provider serves the rate limiters but never "
        "reaches WorkerDeps.redis_client, so following it would leave the "
        "fanout off and the warning re-armed"
    )


def test_resolved_client_does_not_warn() -> None:
    """Any resolved client suppresses the warning, whatever its source:
    TASKQ_REDIS_URL, a caller-owned ``WorkerConnections.redis_client``,
    or a ``redis_client_factory`` all land in the same
    ``WorkerDeps.redis_client`` slot the publish block tests. The
    caller-owned shapes set NO url and register NO provider, so a
    predicate that re-checks the gate's (url-or-DI) conditions would
    warn on a working fanout."""
    with structlog.testing.capture_logs() as logs:
        _emit_progress_fanout_unconfigured_warning(object())  # any resolved client

    assert not [e for e in logs if e["event"] == _EVENT], logs


def test_main_wires_the_progress_fanout_warning() -> None:
    """Structural wiring pin: ``_main`` must call the emitter exactly once,
    with the resolved client.

    Red-team motivation: deleting the production call leaves every
    helper-level unit test green - they invoke the helper directly - so
    the call itself needs its own guard. AST-based, not source-text
    matching, so it is robust to reformatting (the
    ``test_worker_unconsumed_queue_warning.py`` house pattern). The
    keyword binding is part of the pin: the predicate must be
    ``deps.redis_client``, the exact object context.py's publish block
    tests. The rate-limit gate's registry-based predicate is NOT
    equivalent (a DI-only provider serves limiters while the fanout
    stays off; a caller-owned client fans out with no URL and no
    provider), so reverting to it must fail this pin.
    """
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

    emit_calls = [
        node
        for node in ast.walk(main_fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_emit_progress_fanout_unconfigured_warning"
    ]
    assert len(emit_calls) == 1, (
        "expected exactly one _emit_progress_fanout_unconfigured_warning "
        f"call in _main, found {len(emit_calls)} at lines {[c.lineno for c in emit_calls]}"
    )

    call = emit_calls[0]
    keywords = {kw.arg: kw.value for kw in call.keywords if kw.arg is not None}
    arg = keywords.get("redis_client")
    assert arg is not None, (
        "the call must pass the resolved client as redis_client=; a "
        "predicate that cannot see WorkerDeps.redis_client mis-warns in "
        "both directions (DI-only deployments, caller-owned clients)"
    )
    assert isinstance(arg, ast.Attribute) and arg.attr == "redis_client", (
        "the predicate must be deps.redis_client, the object the publish block tests"
    )
    assert isinstance(arg.value, ast.Name) and arg.value.id == "deps", (
        "the client must come from the WorkerDeps instance, not a local or a settings attribute"
    )
