"""Unit tests for _emit_sub_enqueue_startup_warnings.

Pure-Python tests — no PG required. The helper is a synchronous function
that reads the LoopScope resolved cache and WorkerSettings (DSNs,
max_concurrency) to decide which (if any) startup warnings to emit.

Covers unit behaviour and negative / edge-case paths.

These tests assert on *which warning path is taken* (autonomous-fallback,
dsn-mismatch, shared-across-slots, or none; dsn-mismatch and
shared-across-slots can fire together) via the warning event name only —
never on log message format or field names, which are implementation
details that change independently of behaviour.
"""

import asyncpg
from pydantic import BaseModel, TypeAdapter

from taskq._di.scopes import LoopScope
from taskq.actor import ActorRef
from taskq.settings import WorkerSettings
from taskq.testing.spy import WarningSpy
from taskq.worker.run import _emit_sub_enqueue_startup_warnings


def _stub_resolver(func: object) -> object:
    return None


async def _make_loop_scope(
    resolved: dict[type, object] | None = None,
) -> LoopScope:
    scope = LoopScope(resolver=_stub_resolver)
    if resolved is not None:
        scope._cache.update(resolved)  # pyright: ignore[reportPrivateUsage] # Why: test helper populates the cache directly to avoid full DI bootstrap; the helper under test only reads resolved_cache()
    return scope


def _make_settings(
    *,
    pg_dsn_pooled: str | None = None,
    pg_dsn_direct: str | None = None,
    max_concurrency: int | None = None,
) -> WorkerSettings:
    base: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
    }
    if pg_dsn_pooled is not None:
        base["TASKQ_PG_DSN_POOLED"] = pg_dsn_pooled
    if pg_dsn_direct is not None:
        base["TASKQ_PG_DSN_DIRECT"] = pg_dsn_direct
    if max_concurrency is not None:
        base["TASKQ_MAX_CONCURRENCY"] = str(max_concurrency)
    return WorkerSettings.load_from_dict(base)


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(*, name: str = "actor") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=__import__("taskq.retry", fromlist=["RetryPolicy"]).RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


class _StubConn:
    pass


class _EventSpy:
    """Records each warning() event name — WarningSpy counts calls only."""

    def __init__(self) -> None:
        self.events: list[str] = []

    def warning(self, event: str, *_args: object, **_kwargs: object) -> None:
        self.events.append(event)


async def test_no_loop_conn_emits_autonomous_fallback_warning() -> None:
    """startup: no LOOP-scope Connection → one warning emitted."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings()
    actor_registry = {
        "alpha": _make_actor_ref(name="alpha"),
        "beta": _make_actor_ref(name="beta"),
    }
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_loop_conn_with_dsn_mismatch_emits_warning() -> None:
    """LOOP-scope conn present + DSNs differ → one warning emitted."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    # Why: max_concurrency pinned to 1 so ONLY the dsn-mismatch path can
    # fire — the shared-across-slots check is not DSN-gated and would add
    # a second warning at the default concurrency (8).
    settings = _make_settings(
        pg_dsn_pooled="postgresql://user:pass@pgbouncer:6432/taskq",
        pg_dsn_direct="postgresql://user:pass@pg-primary:5432/taskq",
        max_concurrency=1,
    )
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_loop_conn_with_matching_dsns_emits_no_warning() -> None:
    """No warning when LOOP-scope conn is registered and DSNs are equal."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    # Why: max_concurrency pinned to 1 to isolate the DSN check — at the
    # default (8) the shared-across-slots warning correctly fires too.
    settings = _make_settings(max_concurrency=1)
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 0


async def test_no_loop_conn_with_mismatched_dsns_emits_only_one_warning() -> None:
    """When no LOOP-scope conn, the DSN-mismatch warning must NOT also fire."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings(
        pg_dsn_pooled="postgresql://user:pass@pgbouncer:6432/taskq",
        pg_dsn_direct="postgresql://user:pass@pg-primary:5432/taskq",
    )
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_empty_actor_registry_still_emits_autonomous_fallback_warning() -> None:
    """With no actors registered, the warning still fires."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings()
    actor_registry: dict[str, ActorRef[_Payload, _Result]] = {}
    spy = WarningSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_loop_conn_max_concurrency_one_emits_no_shared_slots_warning() -> None:
    """LOOP-scope conn + max_concurrency=1 → shared-across-slots must not fire."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=1)
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = _EventSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert "loop_scope_conn_shared_across_slots" not in spy.events


async def test_loop_conn_max_concurrency_above_one_emits_shared_slots_warning() -> None:
    """LOOP-scope conn + max_concurrency>1 → every slot shares one connection."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(max_concurrency=4)
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = _EventSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert "loop_scope_conn_shared_across_slots" in spy.events


async def test_no_loop_conn_max_concurrency_above_one_no_shared_slots_warning() -> None:
    """No LOOP-scope conn → autonomous-fallback fires; shared warning must not."""
    loop_scope = await _make_loop_scope()
    settings = _make_settings(max_concurrency=4)
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = _EventSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert "loop_scope_conn_shared_across_slots" not in spy.events
    assert "sub_enqueue_autonomous_fallback" in spy.events


async def test_loop_conn_dsn_mismatch_and_max_concurrency_above_one_emits_both() -> None:
    """Checks 2 and 3 are not mutually exclusive — both warnings fire."""
    loop_scope = await _make_loop_scope(resolved={asyncpg.Connection: _StubConn()})
    settings = _make_settings(
        pg_dsn_pooled="postgresql://user:pass@pgbouncer:6432/taskq",
        pg_dsn_direct="postgresql://user:pass@pg-primary:5432/taskq",
        max_concurrency=4,
    )
    actor_registry = {"alpha": _make_actor_ref(name="alpha")}
    spy = _EventSpy()

    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)

    assert "loop_scope_conn_dsn_mismatch" in spy.events
    assert "loop_scope_conn_shared_across_slots" in spy.events
