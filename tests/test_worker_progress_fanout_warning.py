"""Worker startup warning for progress publishes that cannot fan out.

Issue #341's consumer-side degradation chain is loud: the SSE endpoint
serves 503 ``redis_not_configured`` (web/progress.py), clients fall back
to 500 ms Postgres polling (client/_handle.py), and the ready-check
reports ``redis_configured`` (worker/health.py). The one silent link was
the worker side: ``context.py``'s per-call publish block keys on a
resolved Redis client, so with no ``TASKQ_REDIS_URL`` and no registered
``redis.asyncio.Redis`` provider the publish is skipped quietly - the
documented contract - and nothing at boot names the resulting fanout
gap. This pin holds that last link: exactly ONE
``progress-fanout-unconfigured`` startup warning when the worker's
progress publishes will not fan out in real time, none when they will.

Durable progress state still rides the Postgres flush, so this is a
latency degradation, not data loss; the warning surfaces the condition
at the only place that knows it before the first subscriber stares at a
polling stream.

Pure-Python unit tests - no PG required. Warnings are asserted as
actually emitted (event, level, fields) via ``structlog.testing.
capture_logs``, mirroring ``tests/test_worker_unconsumed_queue_warning.py``.
The wiring of the call inside ``_main`` is pinned structurally (AST),
for the same reason as there: deleting the production call leaves every
helper-level unit test green.
"""

import ast
from pathlib import Path

import pytest
import structlog.testing

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import _emit_progress_fanout_unconfigured_warning

_EVENT = "progress-fanout-unconfigured"


def _make_settings(*, redis_url: str | None) -> WorkerSettings:
    config: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
    }
    if redis_url is not None:
        config["TASKQ_REDIS_URL"] = redis_url
    return WorkerSettings.load_from_dict(config)


def test_unconfigured_redis_warns_exactly_once() -> None:
    """No TASKQ_REDIS_URL and no Redis DI provider → exactly ONE
    ``progress-fanout-unconfigured`` warning carrying a remedy."""
    settings = _make_settings(redis_url=None)

    with structlog.testing.capture_logs() as logs:
        _emit_progress_fanout_unconfigured_warning(settings, ProviderRegistry())

    matches = [e for e in logs if e["event"] == _EVENT]
    assert len(matches) == 1, f"exactly one warning expected: {logs}"
    entry = matches[0]
    assert entry["log_level"] == "warning"
    assert "remedy" in entry, "the warning must name the fix: TASKQ_REDIS_URL or a DI provider"


def test_redis_url_set_does_not_warn() -> None:
    """A set TASKQ_REDIS_URL means the publish block resolves a client and
    fans out; warning here would be a false positive."""
    settings = _make_settings(redis_url="redis://localhost:6379/0")

    with structlog.testing.capture_logs() as logs:
        _emit_progress_fanout_unconfigured_warning(settings, ProviderRegistry())

    assert not [e for e in logs if e["event"] == _EVENT], logs


def test_redis_di_provider_satisfies_without_url() -> None:
    """A user-supplied ``redis.asyncio.Redis`` provider is the documented
    alternative to TASKQ_REDIS_URL (the same predicate the rate-limit
    gate honours), so it must suppress the warning too."""
    redis_async = pytest.importorskip("redis.asyncio")
    settings = _make_settings(redis_url=None)
    registry = ProviderRegistry()
    registry.register_value(redis_async.Redis, Scope.PROCESS, redis_async.Redis())

    with structlog.testing.capture_logs() as logs:
        _emit_progress_fanout_unconfigured_warning(settings, registry)

    assert not [e for e in logs if e["event"] == _EVENT], logs


def test_main_wires_the_progress_fanout_warning() -> None:
    """Structural wiring pin: ``_main`` must call the emitter exactly once.

    Red-team motivation: deleting the production call leaves every
    helper-level unit test green - they invoke the helper directly - so
    the call itself needs its own guard. AST-based, not source-text
    matching, so it is robust to reformatting (the
    ``test_worker_unconsumed_queue_warning.py`` house pattern).
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
