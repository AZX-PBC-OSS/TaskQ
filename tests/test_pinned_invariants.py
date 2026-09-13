"""Standing guards for behaviour that is correct today but otherwise undefended.

Every test in this module PASSES at the time it was written. That is the
point: each pins a property that no existing test asserts, and that sits in
the blast radius of a *named* piece of upcoming work. A failure here is not a
flaky test to relax — it is the upcoming change having silently altered a
contract someone relied on.

Each test docstring states WHAT is pinned and WHICH pending change could
break it. A pinning test whose purpose is not stated is one a future agent
deletes.

Scope of this file (and what is deliberately NOT here):

* The ``max_pending`` exactness-under-concurrency property is already pinned
  by ``tests/test_postgres_enqueue_max_pending_lock.py::
  TestCapExactnessPg::test_cap_exact_under_concurrent_enqueue``. Not
  duplicated here.
* Per-route ``admin_actions_enabled`` behaviour is already pinned by
  ``tests/test_admin_security_fixes.py`` and ``tests/test_web_admin_actors.py``;
  the fail-closed *auth* gate on router factories is pinned by
  ``tests/test_web_router_factories_fail_closed.py``. What neither covers is
  the *surface*: this file walks the built admin router so a NEW mutating
  route fails on arrival rather than shipping ungated.
"""

# ruff: noqa: S608 Why: schema names come from new_base62()-generated test
# fixtures and are validated by the migration runner's _IDENT_RE; asyncpg has
# no parameter binding for identifiers. Same suppression as
# tests/test_watch_reclaims.py and tests/test_ratelimit_reservation_pg.py.

import ast
import asyncio
import dataclasses
import inspect
import textwrap
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, EventRow
from taskq.backend.postgres import PostgresBackend
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_pg import _acquire_pg_log
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps
else:  # pragma: no cover - runtime alias only, mirrors test_postgres_unique_for.py
    WorkerDeps = object

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=30)

#: Poll with a zero trailing watermark so the pin observes the *slice*
#: (kind + reason predicate) rather than racing the 2 s production margin.
#: The margin itself is documented and separately exercised by
#: tests/test_watch_reclaims.py; this file pins what rows the slice selects.
_NO_DELAY = timedelta(0)


# ── PIN 1 — the crash-reclaim outbox slice ──────────────────────────


async def test_crash_reclaim_event_reaches_poll_reclaim_events(
    clean_jobs_app: JobsApp,
) -> None:
    """PIN: a crash-reclaim written by Sweep 1 is readable end-to-end through
    ``PostgresBackend.poll_reclaim_events`` — the exact query that drives
    ``TaskQ.watch_reclaims()``.

    WHY IT MATTERS: ``poll_reclaim_events``
    (``src/taskq/backend/_sql_templates.py``) reads a single narrow slice of
    ``job_events`` — ``kind = 'state_change' AND (detail->>'reason') =
    'lock_expired'`` — and that table is the *only* transport for reclaim
    notifications. The crash-reclaim outbox is the subsystem
    ``.shipwright/initiatives/I-01/rca.md`` was written about.

    UPCOMING CHANGE THIS PROTECTS AGAINST: age-based ``job_events``
    retention. A retention sweep that deletes by age with no carve-out for
    the reclaim slice (or that trims before a consumer's trailing watermark
    has cleared) silently drops crash notifications: jobs get reclaimed,
    nobody is told, and nothing fails loudly. This test asserts the row is
    both written by the sweep AND selected by the reclaim query, with the
    ``kind``/``reason`` predicate values spelled out so a change to either
    the writer's or the reader's spelling breaks here.
    """
    deps: WorkerDeps = clean_jobs_app.deps
    backend: PostgresBackend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    baseline = await backend.poll_reclaim_events(0, visibility_delay=_NO_DELAY)
    after_id = max((e.event_id for e in baseline), default=0)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
            max_attempts=1,
            retry_kind="transient",
            attempt=1,
        )

    async with deps.worker_pool.acquire() as conn:
        reclaimed = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)
    assert reclaimed >= 1, "Sweep 1 reclaimed nothing — the fixture job was not eligible"

    events = await backend.poll_reclaim_events(after_id, visibility_delay=_NO_DELAY)
    mine = [e for e in events if str(e.job_id) == str(job_id)]

    assert len(mine) == 1, (
        "the crash-reclaim event did not reach poll_reclaim_events — "
        "watch_reclaims() would deliver nothing for this job. "
        f"got events for job ids {[str(e.job_id) for e in events]}"
    )
    event = mine[0]
    # Spelled out, not derived: these two literals ARE the outbox contract.
    assert event.kind == "state_change"
    assert event.detail.get("reason") == "lock_expired"


async def test_reclaim_slice_is_selected_by_kind_and_reason_only(
    clean_jobs_app: JobsApp,
) -> None:
    """PIN: ``poll_reclaim_events`` selects ONLY the ``lock_expired``
    ``state_change`` slice — an unrelated ``job_events`` row of the same
    ``kind`` but a different ``reason`` is not delivered, and its presence
    does not suppress the real reclaim behind it.

    WHY IT MATTERS: the reclaim watermark (``id > $1``) advances past every
    row the query returns. If the predicate widened, ``watch_reclaims()``
    consumers would see phantom reclaims; if it narrowed, real ones vanish.
    The watermark protocol means both failure modes are silent.

    UPCOMING CHANGE THIS PROTECTS AGAINST: ``job_events`` retention work
    that adds columns, a partial index, or a retention-aware predicate to
    this query. Any rewrite of the WHERE clause must keep this slice exact.
    """
    deps: WorkerDeps = clean_jobs_app.deps
    backend: PostgresBackend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    baseline = await backend.poll_reclaim_events(0, visibility_delay=_NO_DELAY)
    after_id = max((e.event_id for e in baseline), default=0)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        decoy_job = await create_running_job(conn, schema, worker_id)
        # Same kind, different reason — must NOT be in the reclaim slice.
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, kind, detail) '
            "VALUES ($1, 'state_change', $2::jsonb)",
            decoy_job,
            '{"reason": "cancelled"}',
        )
        real_job = await create_running_job(
            conn,
            schema,
            worker_id,
            lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
            max_attempts=1,
            retry_kind="transient",
            attempt=1,
        )

    async with deps.worker_pool.acquire() as conn:
        await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)

    events = await backend.poll_reclaim_events(after_id, visibility_delay=_NO_DELAY)
    job_ids = {str(e.job_id) for e in events}

    assert str(real_job) in job_ids, "the real crash-reclaim was not delivered"
    assert str(decoy_job) not in job_ids, (
        "a non-lock_expired state_change leaked into the reclaim slice — "
        "watch_reclaims() consumers would see a phantom reclaim"
    )
    assert all(e.detail.get("reason") == "lock_expired" for e in events)


async def test_watch_reclaims_delivers_a_crash_reclaim_end_to_end(
    module_pg_schema: ModulePgSchema,
) -> None:
    """PIN: the public ``TaskQ.watch_reclaims()`` generator — not just the
    SQL beneath it — yields a real crash-reclaim.

    WHY IT MATTERS: the two pins above prove the query. This proves the
    wiring: settings plumbing, the visibility-delay default, the poll loop,
    and the ``EventRow`` shape a consumer actually observes. A refactor can
    keep the SQL correct and still break the caller-visible path.

    UPCOMING CHANGE THIS PROTECTS AGAINST: the same ``job_events`` retention
    work, plus any reshaping of the watch transport it prompts. Promptness of
    the NOTIFY path is pinned separately in tests/test_watch_reclaims.py;
    what is pinned here is *delivery at all* through the public generator.
    """
    from taskq.client._taskq import TaskQ

    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    tq = TaskQ(
        dsn=module_pg_schema.pg_dsn,
        schema=schema,
        poll_timeout=0.25,
        reclaim_event_visibility_delay=_NO_DELAY,
    )
    await tq.open()
    try:
        pool = tq._pool  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access, reusing the client's own pool for fixture setup (same technique as tests/test_watch_reclaims.py).
        assert pool is not None
        async with pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
                max_attempts=1,
                retry_kind="transient",
                attempt=1,
            )
            await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)

        got = await asyncio.wait_for(_first_reclaim_for(tq, job_id), timeout=30.0)
    finally:
        await tq.close()

    assert str(got.job_id) == str(job_id)
    assert got.kind == "state_change"
    assert got.detail.get("reason") == "lock_expired"


async def _first_reclaim_for(tq: Any, job_id: Any) -> EventRow:
    """Drain ``watch_reclaims()`` until the event for *job_id* arrives."""
    agen = tq.watch_reclaims(after_id=0)
    try:
        async for event in agen:
            if str(event.job_id) == str(job_id):
                return event
    finally:
        await agen.aclose()
    raise AssertionError("watch_reclaims() ended without delivering the reclaim")


# ── PIN 2 — rate_limit_window_entries self-trims ────────────────────


async def test_window_entries_table_is_bounded_by_window_not_history(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """PIN: ``rate_limit_window_entries`` self-trims on every acquire, so its
    row count is bounded by (window width x arrival rate) — NOT by cumulative
    history.

    WHY IT MATTERS: ``_acquire_pg_log``
    (``src/taskq/ratelimit/_sliding_window_pg.py``) issues a DELETE of rows
    older than the window immediately BEFORE its INSERT, and the refund path
    deletes the specific entry by ``request_id``. This makes the table the
    one rate-limit table that needs no external retention sweep. That
    property is load-bearing and completely implicit: nothing today asserts
    it, and the DELETE looks like an optimisation a reader could "hoist out
    of the hot path" into a periodic sweep.

    UPCOMING CHANGE THIS PROTECTS AGAINST: retention/GC work across the
    schema. If the inline prune is moved to a sweep — or reordered after the
    INSERT, or made conditional — this table starts growing with total
    arrivals and the admission count itself goes wrong (the count subquery
    is window-scoped, but an unbounded table makes it a full-history scan).
    Here: three windows' worth of acquires at a limit of 3, with the table
    checked after each window. The count never exceeds one window's worth,
    even though many more entries were inserted overall.
    """
    schema = module_pg_schema.schema_name
    bucket = f"sw_pin_{new_base62()}"
    limit = 3
    window = timedelta(milliseconds=250)

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )
    sw = SlidingWindow(name=bucket, limit=limit, window=window, backend="postgres")

    # Why the table is seeded with stale rows rather than waiting for a real
    # window to elapse: an earlier version of this pin drove three windows
    # back to back and asserted the count after each. That races wall-clock
    # time — whether the prior window has aged out depends on how long the
    # round trips took — and it was observed flaking between 1 and 3
    # failures across runs. A pin that fails intermittently is worse than no
    # pin: it trains readers to re-run rather than read. The invariant is
    # "entries older than the window are gone after an acquire", so the test
    # states exactly that, with the staleness constructed rather than waited
    # for.
    stale_cutoff = datetime.now(UTC) - (window * 10)
    async with module_pg_pool.acquire() as conn:
        for _ in range(limit * 3):
            await conn.execute(
                f'INSERT INTO "{schema}".rate_limit_window_entries '
                "(bucket_name, ts, request_id) VALUES ($1, $2, $3)",
                bucket,
                stale_cutoff,
                new_uuid(),
            )

    seeded = await _window_entry_count(module_pg_pool, schema, bucket)
    assert seeded == limit * 3, (
        f"fixture seeding failed: expected {limit * 3} stale rows, found {seeded}"
    )

    total_inserted = 0
    for _ in range(3):
        allowed_this_window = 0
        for _ in range(limit):
            decision = await _acquire_pg_log(sw, module_pg_pool, settings, request_id=new_uuid())
            if decision.allowed:
                allowed_this_window += 1
        total_inserted += allowed_this_window

        rows = await _window_entry_count(module_pg_pool, schema, bucket)
        assert rows <= limit, (
            "rate_limit_window_entries grew past one window's worth "
            f"({rows} rows > limit {limit}) — the inline DELETE-before-INSERT "
            "prune in _acquire_pg_log is no longer trimming. This table has no "
            "external retention sweep; it must self-trim."
        )

        # Let the window age out so the next batch exercises the prune.
        await asyncio.sleep(window.total_seconds() + 0.05)

    assert total_inserted == limit * 3, "fixture did not actually insert across 3 windows"

    final = await _window_entry_count(module_pg_pool, schema, bucket)
    assert final <= limit, (
        f"{total_inserted} entries were inserted over 3 windows but "
        f"{final} rows remain — row count must track the window, not history"
    )


async def _window_entry_count(pool: asyncpg.Pool, schema: str, bucket: str) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".rate_limit_window_entries WHERE bucket_name = $1',
            bucket,
        )
    return int(value)


# ── PIN 4 — the admin mutating-route surface stays gated ────────────


def test_every_mutating_admin_route_is_gated_by_actions_and_csrf() -> None:
    """PIN (surface sweep): EVERY state-changing route under
    ``taskq.web.admin`` checks ``settings.admin_actions_enabled`` and depends
    on ``validate_csrf``. A NEW mutating route fails here on arrival.

    WHY IT MATTERS: ``admin_actions_enabled`` defaults to ``False``
    (``src/taskq/settings.py``) precisely so that mounting the admin UI never
    by itself exposes retry/cancel/run-now/deregister. That default is only
    as good as its weakest route. ``tests/test_admin_security_fixes.py`` and
    ``tests/test_web_admin_actors.py`` pin individual routes behaviourally;
    ``tests/test_web_router_factories_fail_closed.py`` pins the *auth* gate
    on the factories. Nothing pins the mutating-route surface — the seventh
    POST route can ship ungated and every existing test still passes.

    UPCOMING CHANGE THIS PROTECTS AGAINST: any admin-UI work that adds a
    mutating route (bulk cancel, requeue, schedule edit, rate-limit reset
    variants). This is a static check over the handler sources — it needs no
    PG, no running app, and it cannot be satisfied by a route that merely
    *looks* gated, because it matches on the attribute access
    ``admin_actions_enabled`` and the ``validate_csrf`` dependency by name.

    WHAT TO DO IF THIS FAILS on a route you added: add both gates. If you
    believe a mutating admin route should run without the opt-in, that is a
    security decision — take it to review, and expect to be asked why the
    sibling pattern does not fit.
    """
    register_fns = _admin_register_functions()
    assert register_fns, "no taskq.web.admin register() functions found — sweep is blind"

    ungated: list[str] = []
    uncsrfed: list[str] = []
    seen: list[str] = []

    for module_name, fn in register_fns:
        source = textwrap.dedent(inspect.getsource(fn))
        tree = ast.parse(source)
        for handler, methods in _mutating_handlers(tree):
            qualname = f"{module_name}.{handler.name} [{'/'.join(sorted(methods))}]"
            seen.append(qualname)
            body = ast.dump(handler)
            # Either fail-safe gate counts. `rate_limit_reset` deliberately
            # uses its own `admin_ui_allow_rate_limit_reset` flag (also
            # defaulting to False) rather than the shared one, so the
            # invariant is "a mutating route consults SOME opt-in flag that
            # defaults off", not "every route reads one specific name".
            # Narrowing this to the shared flag would report a correctly
            # gated route as ungated — a false positive that trains readers
            # to ignore the guard.
            if not any(
                gate in body
                for gate in ("admin_actions_enabled", "admin_ui_allow_rate_limit_reset")
            ):
                ungated.append(qualname)
            if "validate_csrf" not in body:
                uncsrfed.append(qualname)

    assert seen, (
        "the sweep found no mutating admin routes at all — the walk is broken, "
        "not the codebase (taskq.web.admin.ops alone defines several POST routes)"
    )
    assert not ungated, (
        "mutating admin route(s) do not check settings.admin_actions_enabled "
        f"(default False): {ungated}"
    )
    assert not uncsrfed, f"mutating admin route(s) do not depend on validate_csrf: {uncsrfed}"


_MUTATING_METHODS = frozenset({"post", "put", "patch", "delete"})


def _admin_register_functions() -> list[tuple[str, Any]]:
    """Every ``register(router)`` under ``taskq.web.admin`` — the only place
    admin routes are declared."""
    import pkgutil
    from importlib import import_module

    import taskq.web.admin

    found: list[tuple[str, Any]] = []
    for info in pkgutil.walk_packages(taskq.web.admin.__path__, prefix="taskq.web.admin."):
        module = import_module(info.name)
        register = getattr(module, "register", None)
        if register is not None and inspect.isfunction(register):
            found.append((info.name, register))
    return found


def _mutating_handlers(tree: ast.AST) -> list[tuple[ast.AsyncFunctionDef, set[str]]]:
    """Nested handler defs decorated with a mutating ``@router.<method>``."""
    out: list[tuple[ast.AsyncFunctionDef, set[str]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        methods = {
            deco.func.attr
            for deco in node.decorator_list
            if isinstance(deco, ast.Call)
            and isinstance(deco.func, ast.Attribute)
            and deco.func.attr in _MUTATING_METHODS
        }
        if methods:
            out.append((node, methods))
    return out


# ── PIN 5 — unique_for does not dedupe onto terminal jobs ───────────


async def test_unique_for_does_not_dedupe_onto_a_succeeded_job(
    clean_jobs_app: JobsApp,
) -> None:
    """PIN: with the default ``unique_states``, a ``unique_for`` enqueue that
    lands after the prior job SUCCEEDED creates a NEW job — even inside the
    uniqueness window.

    WHY IT MATTERS: ``EnqueueArgs.unique_states`` defaults to
    ``("pending", "scheduled", "running")``
    (``src/taskq/backend/_protocol.py``) — terminal states are deliberately
    excluded, and ``src/taskq/actor.py`` warns callers about overriding it.
    This is what makes ``unique_for`` a *single-flight* guard (one in flight
    at a time) rather than an idempotency key (one ever). Periodic work that
    re-enqueues the same identity every minute depends on it: fold terminal
    states in and the second run is silently swallowed for the whole window,
    with a job handle that looks successful.

    UPCOMING CHANGE THIS PROTECTS AGAINST: work on the idempotency-key dedup
    path, which (unlike this one) carries NO status predicate. The temptation
    when unifying the two dedup paths is to give them one shared predicate.
    If that unification pulls terminal states into the ``unique_for`` path,
    this test is the thing that says no.
    """
    deps: WorkerDeps = clean_jobs_app.deps
    backend: PostgresBackend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    identity = f"pin:unique-for-terminal:{new_base62()}"
    unique_for = timedelta(minutes=15)

    def _args() -> EnqueueArgs:
        return EnqueueArgs(
            id=new_job_id(),
            actor="_pin_unique_for_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=None,
            identity_key=identity,
            unique_for=unique_for,
            # unique_states deliberately NOT passed: the pin is on the
            # DEFAULT. Spelling it out here would make this test survive a
            # widened default and pin only the mechanism, not the contract.
        )

    first_args = _args()
    first = await backend.enqueue(first_args)
    assert first.id == first_args.id, "fixture: the first enqueue should be a fresh insert"

    # Sanity half of the pin: while still non-terminal, unique_for DOES dedupe.
    dup_args = _args()
    dup = await backend.enqueue(dup_args)
    assert dup.id == first.id, (
        "unique_for stopped deduping a still-pending job — the single-flight "
        "guard itself is broken, not just its terminal-state boundary"
    )

    # Drive the first job to a terminal state via the real write path.
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'running', "
            "locked_by_worker = $2, lock_expires_at = clock_timestamp() + interval '60 s', "
            "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp() "
            "WHERE id = $1",
            first.id,
            worker_id,
        )
    assert await backend.mark_succeeded(first.id, worker_id, {"ok": True})

    async with deps.worker_pool.acquire() as conn:
        status = await conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', first.id)
    assert status == "succeeded", f"fixture: expected terminal 'succeeded', got {status!r}"

    # THE PIN: still well inside the 15-minute unique_for window, but the
    # only prior job is terminal — so this must be a NEW job.
    third_args = _args()
    third = await backend.enqueue(third_args)

    assert third.id != first.id, (
        "unique_for deduped onto a SUCCEEDED job. The default unique_states "
        "excludes terminal states on purpose: unique_for is a single-flight "
        "guard, not an idempotency key. Periodic re-enqueues of the same "
        "identity are now silently swallowed for the whole window."
    )
    assert third.id == third_args.id, "the new enqueue should be a fresh insert"

    async with deps.worker_pool.acquire() as conn:
        rows = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE identity_key = $1',
            identity,
        )
    assert int(rows) == 2, f"expected exactly 2 rows for the identity, got {rows}"


def test_unique_states_default_excludes_terminal_states() -> None:
    """PIN: the ``unique_states`` DEFAULT literal is exactly the three
    non-terminal states.

    WHY IT MATTERS: the behavioural pin above proves the runtime consequence;
    this pins the declaration itself, so a change to the default is a failure
    at the definition site with no PG required. The set is spelled out rather
    than derived — the whole point is that adding ``succeeded``/``failed``/
    ``cancelled`` must be a conscious, reviewed edit.

    UPCOMING CHANGE THIS PROTECTS AGAINST: the idempotency-key dedup work
    (that path has no status predicate); a shared predicate between the two
    dedup paths would most naturally land as a widened default here.
    """
    # Why dataclasses.fields rather than the class attribute: EnqueueArgs is a
    # slotted dataclass, so `EnqueueArgs.unique_states` is the slot descriptor,
    # not the default value. Reading the descriptor would make this pin pass
    # vacuously against any default.
    field = next(f for f in dataclasses.fields(EnqueueArgs) if f.name == "unique_states")
    default = field.default
    assert isinstance(default, tuple), (
        "unique_states has no literal default — the pin cannot read the value it "
        f"exists to guard (got {default!r})"
    )

    assert default == ("pending", "scheduled", "running")
    assert "succeeded" not in default
    assert "failed" not in default
    assert "cancelled" not in default


def test_admin_actions_enabled_defaults_to_false() -> None:
    """PIN: ``TaskQSettings.admin_actions_enabled`` defaults to ``False``.

    WHY IT MATTERS: the surface sweep above proves every mutating route
    *consults* the flag. This proves consulting it is worth something — the
    flag is opt-in, so mounting the admin UI never by itself exposes
    retry/cancel/run-now/deregister. Flip the default and every gate in the
    sweep becomes a no-op while every test still passes.

    UPCOMING CHANGE THIS PROTECTS AGAINST: admin-UI usability work ("the
    buttons 403 out of the box") that fixes the symptom by flipping the
    default instead of documenting the opt-in.
    """
    # TaskQSettings is a dotenvmodel DotEnvConfig, not a pydantic BaseModel:
    # its introspection seam is get_fields(), which maps name -> (type, FieldInfo).
    _type, field_info = TaskQSettings.get_fields()["admin_actions_enabled"]
    assert field_info.default is False
