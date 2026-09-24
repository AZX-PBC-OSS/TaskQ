"""The admin portal's no-Redis-client startup warning (the #424 pattern, admin side).

When TaskQ's settings have Redis configured but the admin app's state
carries no client (``create_router`` called without ``redis_client``), the
portal renders in polling mode and the progress stream answers 503
``redis_not_configured``: live SSE progress is silently absent while the
dashboard keeps working. This pin holds the one link that names that
wiring gap at startup, exactly ONE ``admin-ui-no-redis-client`` warning.

The warning fires ONLY on the gap. A deployment with no TaskQ Redis
configured at all is the legitimate polling mode: the badge says so
accurately, nothing is misconfigured, and no warning fires. The predicate
is therefore ``settings.redis_url is not None and redis_client is None`` -
both halves, pinned structurally (AST) the way
``tests/test_worker_progress_fanout_warning.py`` pins its call: deleting
the production call leaves helper-level assertions vacuous, and dropping
either half of the predicate mis-warns in one direction (a legitimate
no-Redis deployment spammed) or the other (the gap silenced).

Pure-Python unit tests, no PG required; warnings are asserted as actually
emitted (event, level, fields) via ``structlog.testing.capture_logs``.
"""

import ast
from pathlib import Path

import pytest
import structlog.testing

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin import create_router

from . import _StubPool

_EVENT = "admin-ui-no-redis-client"

_DETAIL = (
    "the admin portal has no Redis client: live SSE progress is "
    "unavailable, the dashboard falls back to polling"
)


def test_wiring_gap_warns_exactly_once(
    stub_pool: _StubPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis configured in settings (TASKQ_REDIS_URL set) + no client in
    the app state -> exactly ONE ``admin-ui-no-redis-client`` warning
    carrying the degradation statement and a remedy that names the wiring
    that closes the gap."""
    monkeypatch.setenv("TASKQ_REDIS_URL", "redis://localhost:6379/0")

    with structlog.testing.capture_logs() as logs:
        create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool, the test_factory pattern.

    matches = [e for e in logs if e["event"] == _EVENT]
    assert len(matches) == 1, f"exactly one warning expected: {logs}"
    entry = matches[0]
    assert entry["log_level"] == "warning"
    assert _DETAIL in entry.get("detail", ""), (
        "the warning must carry the degradation statement the operator greps for"
    )
    remedy = entry.get("remedy", "")
    assert "redis_client" in remedy, (
        "the remedy must name the missing wiring: the client this router was created without"
    )
    assert "TASKQ_REDIS_URL" in remedy, (
        "the remedy must state the settings side is already configured, so "
        "the operator does not go looking for a missing env var"
    )


def test_no_redis_configured_is_legitimate_polling_and_does_not_warn(
    stub_pool: _StubPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No TASKQ_REDIS_URL and no client is the documented polling mode:
    the badge reads ``polling mode`` accurately and NO warning fires. A
    warning here would be log noise on every legitimate deployment."""
    monkeypatch.delenv("TASKQ_REDIS_URL", raising=False)

    with structlog.testing.capture_logs() as logs:
        create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.

    assert not [e for e in logs if e["event"] == _EVENT], (
        f"the legitimate polling mode must stay silent: {[e for e in logs if e['event'] == _EVENT]}"
    )


def test_wired_client_does_not_warn(stub_pool: _StubPool, monkeypatch: pytest.MonkeyPatch) -> None:
    """A router created WITH the client - the gap closed - is silent,
    whether Redis is configured via the URL or the client arrived
    caller-owned with no URL at all: the predicate is the app state, not
    the settings."""
    monkeypatch.setenv("TASKQ_REDIS_URL", "redis://localhost:6379/0")

    with structlog.testing.capture_logs() as logs:
        create_router(stub_pool, redis_client=object())

    assert not [e for e in logs if e["event"] == _EVENT], logs


def test_create_router_wires_the_no_redis_warning_behind_the_exact_predicate() -> None:
    """Structural wiring pin: ``create_router`` must contain exactly one
    ``admin-ui-no-redis-client`` warning call, guarded by BOTH halves of
    the predicate - the settings expecting Redis AND the app state
    lacking the client.

    Red-team motivation: deleting the call leaves every helper-level
    behavioral test green (they drive create_router, not the call site
    shape), and dropping either predicate half mis-warns in a direction
    the behavioral tests above pin separately - so the guard shape is
    part of the pin. AST-based, not source-text matching, so it is robust
    to reformatting (the ``test_worker_progress_fanout_warning.py`` house
    pattern)."""
    import taskq.web.admin._factory as factory_mod

    source = ast.parse(Path(factory_mod.__file__).read_text())

    create_router_fn = next(
        (
            node
            for node in ast.walk(source)
            if isinstance(node, ast.FunctionDef) and node.name == "create_router"
        ),
        None,
    )
    assert create_router_fn is not None, "create_router must exist in taskq.web.admin._factory"

    warn_calls = [
        node
        for node in ast.walk(create_router_fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "warning"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == _EVENT
    ]
    assert len(warn_calls) == 1, (
        f"expected exactly one {_EVENT} warning call in create_router, "
        f"found {len(warn_calls)} at lines {[c.lineno for c in warn_calls]}"
    )

    # The call must sit behind an If whose test is the two-half predicate:
    # settings.redis_url is not None  AND  redis_client is None.
    # (ast.walk includes body and orelse; the only If around this call is
    # its guard.)
    guard = next(
        (
            node
            for node in ast.walk(create_router_fn)
            if isinstance(node, ast.If) and warn_calls[0] in set(ast.walk(node))
        ),
        None,
    )
    assert guard is not None, f"the {_EVENT} warning must be guarded by an If (the predicate)"

    test = guard.test
    assert (
        isinstance(test, ast.BoolOp) and isinstance(test.op, ast.And) and len(test.values) == 2
    ), (
        "the predicate must be the conjunction of both halves: the settings "
        "expecting Redis AND the app state lacking the client - dropping "
        "either half mis-warns (a legitimate no-Redis deployment spammed, "
        "or the gap silenced)"
    )

    def _is_settings_redis_url(cond: ast.expr) -> bool:
        return (
            isinstance(cond, ast.Compare)
            and isinstance(cond.left, ast.Attribute)
            and cond.left.attr == "redis_url"
            and isinstance(cond.left.value, ast.Name)
            and cond.left.value.id == "settings"
        )

    def _is_client_none(cond: ast.expr) -> bool:
        return (
            isinstance(cond, ast.Compare)
            and isinstance(cond.left, ast.Name)
            and cond.left.id == "redis_client"
        )

    assert (_is_settings_redis_url(test.values[0]) and _is_client_none(test.values[1])) or (
        _is_client_none(test.values[0]) and _is_settings_redis_url(test.values[1])
    ), (
        "the guard must test settings.redis_url is not None AND redis_client "
        "is None; any other predicate (the client alone, the URL alone, a "
        "truthiness check) mis-warns in one direction or the other"
    )
