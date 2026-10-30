"""THE CAPACITY OWNER'S SYNC PINS (C15 — the code-deploy opt-in).

``sync_actor_config``'s UPSERT deliberately omits the capacity columns
from the ``DO UPDATE SET`` clause: an existing row's
``max_concurrent``/``max_pending``/``result_ttl`` survive every
subsequent boot (the operator-owns law). Consequence (C15's conviction):
for an actor whose capacity terms ride the CODE — TAStack's best-effort
dampers ride ``max_concurrent`` — a changed literal deploys NOTHING after
first boot; editing code silently does nothing.

The cure is a PER-ACTOR OPT-IN: ``@actor(capacity_owner="code")`` /
``@app.actor(capacity_owner="code")``. For code-owned actors the upsert's
SET clause INCLUDES ``max_concurrent`` and ``result_ttl`` (the retry
contract's own law, on the capacity axes); the queue and ``max_pending``
stay operator-owned for BOTH owners. The default is byte-identical: no
code-owned rows → the operator statement alone, exactly the pre-opt-in
shape.

Pins here, red-first:
* a code-owned actor's capacity change deploys by code after first boot
  (real PG);
* a default actor's row is untouched by a code change (real PG);
* the seed INSERT path is unchanged (real PG);
* the declaration surface accepts and rejects the right shapes;
* the operator statement is byte-identical when code-owned rows ride
  along (the statement partition).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import asyncpg
import pytest
import structlog
import structlog.types
from pydantic import BaseModel

from taskq._ids import new_base62
from taskq._json import dumps_str
from taskq.actor import actor
from taskq.actor_config import ActorConfig
from taskq.exceptions import ActorConfigDriftList
from taskq.worker.startup import sync_actor_config


class _Probe(BaseModel):
    """The vanilla decorator pins' payload model (module level — the
    ref's hint resolution reads the module's namespace)."""

    doc_id: str


# ═══════════════════════════════════════════════════════════════════════════════
# The fake connection (test_actor_config_sync.py's own shape, local copy —
# cross-test-module imports of fakes are a collection-order fragility)
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class FakeRecord:
    """A record-like object that supports dict-style key access."""

    _fields: dict[str, object] = field(default_factory=dict[str, object])

    def __getitem__(self, key: str) -> object:
        return self._fields[key]


class FakeAsyncpgConnection:
    """A test-double for ``asyncpg.Connection`` that records SELECT/UPSERT calls."""

    def __init__(self) -> None:
        self._select_rows: list[FakeRecord] = []
        self._transaction_count: int = 0
        self._fetch_calls: list[tuple[str, list[Any]]] = []
        self._execute_calls: list[tuple[str, list[Any]]] = []

    def set_select_rows(self, rows: list[FakeRecord]) -> None:
        self._select_rows = list(rows)

    async def fetch(self, query: str, *params: Any) -> list[FakeRecord]:
        self._fetch_calls.append((query, list(params)))
        return list(self._select_rows)

    async def execute(self, query: str, *params: Any) -> str:
        self._execute_calls.append((query, list(params)))
        return "OK"

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)


class FakeTransaction:
    """Async context manager that records enter/exit."""

    def __init__(self, fake_conn: FakeAsyncpgConnection) -> None:
        self._conn = fake_conn
        self._entered = False

    async def __aenter__(self) -> FakeTransaction:
        self._conn._transaction_count += 1
        self._entered = True
        return self

    async def __aexit__(self, *args: object) -> None:
        if not self._entered:
            raise RuntimeError("transaction exited without entering")


def _make_record(
    actor: str,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
    queue: str = "default",
    result_ttl: float | None = None,
    metadata: dict[str, object] | None = None,
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> FakeRecord:
    md = metadata if metadata is not None else {}
    return FakeRecord(
        {
            "actor": actor,
            "max_concurrent": max_concurrent,
            "max_pending": max_pending,
            "queue": queue,
            "result_ttl": result_ttl,
            "metadata": dumps_str(md),
            "max_attempts": max_attempts,
            "retry_kind": retry_kind,
        }
    )


def _make_config(
    actor: str,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
    queue: str = "default",
    result_ttl: float | None = None,
    metadata: dict[str, object] | None = None,
    capacity_owner: str = "operator",
) -> ActorConfig:
    return ActorConfig(
        actor=actor,
        max_concurrent=max_concurrent,
        max_pending=max_pending,
        queue=queue,
        result_ttl=result_ttl,
        metadata=metadata if metadata is not None else {},
        capacity_owner=capacity_owner,
    )


def _on_conflict_clause(sql: str) -> str:
    return sql.split("DO UPDATE SET", 1)[1]


# ═══════════════════════════════════════════════════════════════════════════════
# Unit-tier: the statement partition (fake connection)
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_the_default_sync_keeps_its_single_operator_statement() -> None:
    """C15's byte-identity law, statement-shaped: a sync with NO code-owned
    rows runs EXACTLY ONE upsert, and its conflict arm assigns no capacity
    column — the operator-owns law's own SQL, unchanged by the opt-in."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([_make_record("X", max_concurrent=5, queue="default")])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking.
        [_make_config("X", max_concurrent=3)],
    )

    assert len(fake_conn._execute_calls) == 1
    sql, _params = fake_conn._execute_calls[0]
    on_conflict = _on_conflict_clause(sql)
    assert "max_concurrent" not in on_conflict
    assert "result_ttl" not in on_conflict
    assert "max_pending" not in on_conflict
    assert "queue" not in on_conflict


@pytest.mark.asyncio
async def test_the_code_owned_rows_ride_the_code_owned_statement() -> None:
    """The opt-in's own statement: the code-owned partition's upsert SET
    clause rewrites max_concurrent and result_ttl from EXCLUDED — and
    NOTHING else joins them (the queue and max_pending stay
    operator-owned for BOTH owners). The operator partition's statement
    stays byte-identical in the same transaction."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [
            _make_record("operator-actor", max_concurrent=5, queue="default"),
            _make_record("code-actor", max_concurrent=5, queue="default"),
        ]
    )

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double, see the sibling sync pins.
        [
            _make_config("operator-actor", max_concurrent=3),
            _make_config("code-actor", max_concurrent=7, result_ttl=120.0, capacity_owner="code"),
        ],
    )

    assert len(fake_conn._execute_calls) == 2, (
        "one statement per partition, disjoint actors, one transaction"
    )
    operator_sql, operator_params = fake_conn._execute_calls[0]
    code_sql, code_params = fake_conn._execute_calls[1]

    # THE OPERATOR STATEMENT IS UNCHANGED — even with a code-owned actor
    # riding the same sync, the operator partition's conflict arm assigns
    # no capacity column (the default rows' byte-identity law).
    operator_on_conflict = _on_conflict_clause(operator_sql)
    assert "max_concurrent" not in operator_on_conflict
    assert "result_ttl" not in operator_on_conflict
    assert operator_params[0] == ["operator-actor"], (
        "the operator statement gets only its partition"
    )

    # THE CODE-OWNED STATEMENT'S SET CLAUSE: the two capacity columns
    # join the retry contract's family; the queue and max_pending do NOT.
    code_on_conflict = _on_conflict_clause(code_sql)
    assert "max_concurrent = EXCLUDED.max_concurrent" in code_on_conflict
    assert "result_ttl     = EXCLUDED.result_ttl" in code_on_conflict
    assert "queue" not in code_on_conflict, "the queue stays operator-owned (the move-queue law)"
    assert "max_pending" not in code_on_conflict, (
        "max_pending stays operator-owned (the producer-side admission reads the stored row)"
    )
    assert code_params[0] == ["code-actor"], "the code-owned statement gets only its partition"


# ═══════════════════════════════════════════════════════════════════════════════
# Unit-tier: the events (the deploy is visible, never silent)
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_code_owned_capacity_divergence_logs_the_code_change_event(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """A code-owned actor's stored/registered capacity mismatch is the
    WARNING that announces the deploy (the retry contract's own event
    shape): the upsert is about to overwrite the stored value with the
    literal — visible, never silent. The same actor's max_pending
    mismatch stays the operator-override INFO (it is NOT code-owned)."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [_make_record("X", max_concurrent=5, max_pending=100, queue="default")]
    )

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double, see the sibling sync pins.
        [
            _make_config(
                "X",
                max_concurrent=7,
                max_pending=200,
                result_ttl=120.0,
                capacity_owner="code",
            )
        ],
    )

    change_events = [
        e for e in structlog_capture if e["event"] == "actor-config-capacity-code-change"
    ]
    code_fields = {e["field"] for e in change_events}
    assert code_fields == {"max_concurrent", "result_ttl"}, (
        "the code-owned fields' deploy must announce itself, one event per field"
    )
    override_events = [
        e for e in structlog_capture if e["event"] == "actor-config-capacity-override"
    ]
    override_fields = {e["field"] for e in override_events}
    assert override_fields == {"max_pending"}, (
        "max_pending stays operator-owned even for a code-owned actor: its "
        "divergence is the override log, never the code-change warning"
    )


@pytest.mark.asyncio
async def test_default_capacity_divergence_keeps_the_override_log(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """THE REGRESSION GUARD: a DEFAULT actor's capacity mismatch is
    exactly today's INFO override event — the code-change warning must
    never fire for it (the operator-owns law holds, byte-identically)."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([_make_record("X", max_concurrent=5, queue="default")])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double, see the sibling sync pins.
        [_make_config("X", max_concurrent=3)],
    )

    assert not [
        e for e in structlog_capture if e["event"] == "actor-config-capacity-code-change"
    ], "a default actor's divergence is never a code deploy"
    override_events = [
        e for e in structlog_capture if e["event"] == "actor-config-capacity-override"
    ]
    assert {e["field"] for e in override_events} == {"max_concurrent"}


@pytest.mark.asyncio
async def test_code_owned_drift_never_raises_and_metadata_law_unchanged() -> None:
    """The code-owned opt-in does not touch the DRIFT families: metadata
    still raises (structural), and a code-owned row's capacity mismatch
    is never an ``ActorConfigDriftList``."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [_make_record("X", max_concurrent=5, queue="default", metadata={"env": "prod"})]
    )

    with pytest.raises(ActorConfigDriftList):
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double, see the sibling sync pins.
            [
                _make_config(
                    "X",
                    max_concurrent=7,
                    capacity_owner="code",
                    metadata={"env": "staging"},
                )
            ],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# The declaration surfaces (accepts and rejects the right shapes)
# ═══════════════════════════════════════════════════════════════════════════════


def test_the_vanilla_surface_accepts_the_owner_names() -> None:
    """``@actor(capacity_owner=...)`` accepts exactly the two owner names
    and carries them on the ref."""

    @actor(capacity_owner="code")
    async def code_owned(payload: _Probe) -> str:
        return payload.doc_id  # pragma: no cover - the pin drives the declaration

    assert code_owned.capacity_owner == "code"

    @actor()
    async def default_owned(payload: _Probe) -> str:
        return payload.doc_id  # pragma: no cover

    assert default_owned.capacity_owner == "operator"


@pytest.mark.parametrize("bad", ["fleet", "CODE", "operator ", "", None, True, 5])
def test_the_vanilla_surface_rejects_the_wrong_shapes(bad: object) -> None:
    """Anything but the two owner names is the refused declaration —
    ``ValueError`` at the decoration site, never a silent anything."""
    with pytest.raises(ValueError, match="capacity_owner"):

        @actor(capacity_owner=bad)  # type: ignore[arg-type]  # Why: the REFUSED shapes are the pin's subject — the checker's own refusal and the runtime door agree.
        async def refused(payload: _Probe) -> str:
            return payload.doc_id  # pragma: no cover


def test_the_wf_surface_accepts_and_rejects_the_owner_shapes() -> None:
    """``@app.actor(capacity_owner=...)``: the same door, the wf face —
    the two owner names accepted, anything else refused."""
    from pydantic import BaseModel

    from taskq.workflows import StepContext, WorkflowApp

    class Ingest(BaseModel):
        doc_id: str

    app = WorkflowApp()

    @app.actor(capacity_owner="code", max_concurrent=4)
    async def code_owned(ctx: StepContext, params: Ingest) -> str:
        return params.doc_id  # pragma: no cover - the runner drives real bodies

    assert code_owned.capacity_owner == "code"


@pytest.mark.parametrize("bad", ["fleet", "CODE", "operator ", "", None, True, 5])
def test_the_wf_surface_rejects_the_wrong_shapes(bad: object) -> None:
    """THE WF FACE'S REJECT PARAMETRIZATION (the review's 4a): the same
    seven bad shapes the vanilla surface pins — every one is the refused
    declaration at the wf decorator, never a silent anything."""
    from pydantic import BaseModel

    from taskq.workflows import StepContext, WorkflowApp

    class Ingest(BaseModel):
        doc_id: str

    app = WorkflowApp()
    with pytest.raises(ValueError, match="capacity_owner"):

        @app.actor(capacity_owner=bad)  # type: ignore[arg-type]  # Why: the REFUSED shapes are the pin's subject.
        async def refused(ctx: StepContext, params: Ingest) -> str:
            return params.doc_id  # pragma: no cover


# ═══════════════════════════════════════════════════════════════════════════════
# The projection carries the declaration (the wf face's ride)
# ═══════════════════════════════════════════════════════════════════════════════


def test_the_projection_carries_the_code_owner() -> None:
    """C15's wf face: the handle's declaration rides the wiring into the
    decl and the projection's ``ActorConfig`` — the cohort the sync
    consumes is marked code-owned; a bare cohort stays "operator"."""
    from pydantic import BaseModel

    from taskq.workflows import Promise, StepContext, WorkflowApp, build, step
    from taskq.workflows._worker_execution import project_workflow_actor_configs

    class Ingest(BaseModel):
        doc_id: str

    class Report(BaseModel):
        ref: str

    app = WorkflowApp()

    @app.actor(capacity_owner="code", max_concurrent=4, result_ttl=120.0)
    async def capped(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.actor()
    async def bare(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover

    @app.workflow("code-owner-flow")
    def code_owner_flow() -> Promise[object]:
        one = step(capped, Ingest(doc_id="d"), key="one", actor="wf-code")
        two = step(bare, one, key="two", actor="wf-bare")
        return build(two)

    rows = {c.actor: c for c in project_workflow_actor_configs()}
    assert rows["wf-code"].capacity_owner == "code"
    assert rows["wf-code"].max_concurrent == 4
    assert rows["wf-code"].result_ttl == 120.0
    assert rows["wf-bare"].capacity_owner == "operator", (
        "a cohort with no owner declaration projects the default — the operator-owns law, unchanged"
    )


def test_the_code_owner_participates_in_the_one_cap_law() -> None:
    """The owner declaration is a CAPACITY TERM: two healthy declarations
    fighting over one actor's terms — differing owners included — is the
    refused drift (the across-workflows arm), never a silent skip."""
    from pydantic import BaseModel

    from taskq.workflows import Promise, StepContext, WorkflowApp, build, step
    from taskq.workflows._worker_execution import (
        WorkflowActorCapacityConflictError,
        project_workflow_actor_configs,
    )

    class Ingest(BaseModel):
        doc_id: str

    class Report(BaseModel):
        ref: str

    app_one = WorkflowApp()

    @app_one.actor(capacity_owner="code", max_concurrent=4)
    async def handle_one(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover

    @app_one.workflow("owner-flow-one")
    def owner_flow_one() -> Promise[object]:
        return build(step(handle_one, Ingest(doc_id="d"), key="one", actor="wf-shared"))

    app_two = WorkflowApp()

    @app_two.actor(max_concurrent=4)
    async def handle_two(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover

    @app_two.workflow("owner-flow-two")
    def owner_flow_two() -> Promise[object]:
        return build(step(handle_two, Ingest(doc_id="d"), key="two", actor="wf-shared"))

    with pytest.raises(WorkflowActorCapacityConflictError, match="capacity_owner"):
        project_workflow_actor_configs()


# ═══════════════════════════════════════════════════════════════════════════════
# Integration-tier (real PG via pg_conn)
# ═══════════════════════════════════════════════════════════════════════════════


async def _ensure_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}".actor_config (
            actor          text PRIMARY KEY,
            max_concurrent int,
            max_pending    int,
            queue          text NOT NULL,
            max_attempts   smallint NOT NULL DEFAULT 3,
            retry_kind     text NOT NULL DEFAULT 'transient',
            result_ttl     float,
            metadata       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            updated_at     timestamptz NOT NULL DEFAULT now(),
            retry_base     interval,
            retry_cap      interval,
            retry_backoff  text,
            retry_jitter   float8
        )
    """)


async def _read_row(conn: asyncpg.Connection, schema: str, actor: str) -> dict[str, object]:
    row = await conn.fetchrow(
        f"SELECT actor, max_concurrent, max_pending, queue, result_ttl "  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers.
        f'FROM "{schema}".actor_config WHERE actor = $1',
        actor,
    )
    assert row is not None, f"the row for {actor!r} must exist"
    return dict(row)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_code_owned_capacity_change_deploys_after_first_boot(
    pg_conn: asyncpg.Connection,
) -> None:
    """C15's CORE PIN (red-first): a code-owned actor's capacity change
    deploys by code AFTER first boot. Boot 1 seeds the row from the
    literal; boot 2 carries a CHANGED literal — the stored row READS the
    new values. The pre-cure shape left the row at boot 1's values
    forever: editing code silently did nothing."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    boot_one = [_make_config("damper", max_concurrent=2, result_ttl=60.0, capacity_owner="code")]
    await sync_actor_config(pg_conn, boot_one, schema=schema)
    seeded = await _read_row(pg_conn, schema, "damper")
    assert seeded["max_concurrent"] == 2
    assert seeded["result_ttl"] == 60.0

    boot_two = [_make_config("damper", max_concurrent=8, result_ttl=300.0, capacity_owner="code")]
    await sync_actor_config(pg_conn, boot_two, schema=schema)
    deployed = await _read_row(pg_conn, schema, "damper")
    assert deployed["max_concurrent"] == 8, (
        "the code-owned actor's max_concurrent change deploys by code — the "
        "upsert's SET clause rewrites it from the literal on every boot"
    )
    assert deployed["result_ttl"] == 300.0, "result_ttl deploys with it"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_default_actor_row_is_untouched_by_a_code_change(
    pg_conn: asyncpg.Connection,
) -> None:
    """THE OPERATOR-OWNS LAW (byte-identity, real PG): a DEFAULT actor's
    row survives a changed literal untouched — boot 2's upsert cannot
    flip it back, exactly the pre-opt-in behavior."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    boot_one = [_make_config("operator", max_concurrent=2, result_ttl=60.0)]
    await sync_actor_config(pg_conn, boot_one, schema=schema)

    boot_two = [_make_config("operator", max_concurrent=9, result_ttl=900.0)]
    await sync_actor_config(pg_conn, boot_two, schema=schema)
    stored = await _read_row(pg_conn, schema, "operator")
    assert stored["max_concurrent"] == 2, "the stored value wins on every conflict"
    assert stored["result_ttl"] == 60.0


@pytest.mark.asyncio
@pytest.mark.integration
async def test_the_seed_insert_path_is_unchanged(pg_conn: asyncpg.Connection) -> None:
    """A code-owned actor's FIRST sync seeds every column from the
    literal — the same INSERT list, the same seed-only shape as the
    default actor's. And a convergence boot (same literal) leaves the
    row untouched-and-equal."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    first = [
        _make_config(
            "seeded",
            max_concurrent=3,
            max_pending=50,
            queue="gpu",
            result_ttl=45.0,
            capacity_owner="code",
        )
    ]
    await sync_actor_config(pg_conn, first, schema=schema)
    row = await _read_row(pg_conn, schema, "seeded")
    assert row["max_concurrent"] == 3
    assert row["max_pending"] == 50
    assert row["queue"] == "gpu"
    assert row["result_ttl"] == 45.0

    # The seed is the DEFAULT actor's seed too: a default actor's first
    # boot seeds identically (one law, one INSERT list).
    await sync_actor_config(
        pg_conn,
        [_make_config("vanilla", max_concurrent=3, max_pending=50, queue="gpu", result_ttl=45.0)],
        schema=schema,
    )
    vanilla = await _read_row(pg_conn, schema, "vanilla")
    assert {k: v for k, v in vanilla.items() if k != "actor"} == {
        k: v for k, v in row.items() if k != "actor"
    }, "the seed INSERT path is owner-blind"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_code_owned_queue_and_max_pending_changes_do_not_deploy(
    pg_conn: asyncpg.Connection,
) -> None:
    """The exclusions hold on real PG: a code-owned actor's changed queue
    and max_pending literals stay OUT of the SET clause — the stored
    queue (the move-queue law) and the stored backlog cap (the
    producer-side admission's lever) are the operator's, for BOTH
    owners."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    boot_one = [
        _make_config(
            "both", max_concurrent=2, max_pending=30, queue="default", capacity_owner="code"
        )
    ]
    await sync_actor_config(pg_conn, boot_one, schema=schema)

    boot_two = [
        _make_config(
            "both", max_concurrent=2, max_pending=99, queue="critical", capacity_owner="code"
        )
    ]
    await sync_actor_config(pg_conn, boot_two, schema=schema)
    stored = await _read_row(pg_conn, schema, "both")
    assert stored["queue"] == "default", "the queue assignment is never code-owned"
    assert stored["max_pending"] == 30, "max_pending is never code-owned"
    assert stored["max_concurrent"] == 2  # the unchanged literal deploys as itself


# ═══════════════════════════════════════════════════════════════════════════════
# The review's 4(b) + 4(d): the converged boot's quiet, the SET clauses frozen
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_converged_code_owned_boot_is_quiet(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """THE REVIEW'S 4(b): a CONVERGED code-owned boot (the stored row
    already reads the literal) emits NO ``actor-config-capacity-code-change``
    event — the deploy warning is DIVERGENCE-GATED (it announces a change,
    and a converged boot changes nothing). A boot that warned on every
    converge would be noise about nothing."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [_make_record("X", max_concurrent=7, result_ttl=120.0, queue="default")]
    )

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double, see the sibling sync pins.
        [_make_config("X", max_concurrent=7, result_ttl=120.0, capacity_owner="code")],
    )

    assert not [
        e for e in structlog_capture if e["event"] == "actor-config-capacity-code-change"
    ], "a converged code-owned boot is quiet — the warning is divergence-gated"
    assert not [e for e in structlog_capture if e["event"] == "actor-config-capacity-override"], (
        "and it is not an operator override either"
    )


def test_the_operator_statement_set_clause_is_frozen() -> None:
    """THE REVIEW'S 4(d) — THE BYTE-IDENTITY CLAIM, LITERAL: the operator
    statement's ``DO UPDATE SET`` assigns EXACTLY this column list, in
    this order. A future statement that adds, drops, or REORDERS these
    assignments reds here — the operator-owns law's own shape, frozen
    (the capacity columns' absence is what keeps a stored row's capacity
    operator-owned; a reorder is a reviewable diff, never a drive-by)."""
    from taskq.worker.startup import _UPSERT_ACTOR_CONFIG_SQL

    set_clause = _on_conflict_clause(_UPSERT_ACTOR_CONFIG_SQL)
    assignments = [
        line.strip().removesuffix(",").replace(" ", "")
        for line in set_clause.splitlines()
        if line.strip()
    ]
    assert assignments == [
        "metadata=EXCLUDED.metadata",
        "max_attempts=EXCLUDED.max_attempts",
        "retry_kind=EXCLUDED.retry_kind",
        "updated_at=clock_timestamp()",
    ], "the operator statement's SET clause moved — the byte-identity law's own freeze"


def test_the_code_owned_statement_set_clause_is_frozen() -> None:
    """The code-owned statement's freeze (the operator's own list PLUS
    exactly the two capacity columns, in order): the opt-in's surface is
    exactly these two assignments — a third column joining them is a
    reviewable diff, never a drive-by."""
    from taskq.worker.startup import _UPSERT_CODE_OWNED_CAPACITY_SQL

    set_clause = _on_conflict_clause(_UPSERT_CODE_OWNED_CAPACITY_SQL)
    assignments = [
        line.strip().removesuffix(",").replace(" ", "")
        for line in set_clause.splitlines()
        if line.strip()
    ]
    assert assignments == [
        "metadata=EXCLUDED.metadata",
        "max_concurrent=EXCLUDED.max_concurrent",
        "result_ttl=EXCLUDED.result_ttl",
        "max_attempts=EXCLUDED.max_attempts",
        "retry_kind=EXCLUDED.retry_kind",
        "updated_at=clock_timestamp()",
    ], "the code-owned statement's SET clause moved — the opt-in's own freeze"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_code_owned_uncapping_deploys_null_and_warns(
    pg_conn: asyncpg.Connection,
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """THE NULL-OUT (the re-review's residual 2): a code-owned actor
    whose literal UNCAPS (a numeric cap → ``None``) deploys the NULL —
    the code-owned SET assigns unconditionally (the declaration-of-
    record-wins law) — AND the deploy announces itself: the
    ``actor-config-capacity-code-change`` warning carrying
    ``registered=None`` against the STORED old value, one event per
    field. The pre-cure shape left the stored cap at its
    first-registration value forever: un-capping by code silently did
    nothing."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    boot_one = [_make_config("uncap", max_concurrent=2, result_ttl=60.0, capacity_owner="code")]
    await sync_actor_config(pg_conn, boot_one, schema=schema)
    seeded = await _read_row(pg_conn, schema, "uncap")
    assert seeded["max_concurrent"] == 2
    assert seeded["result_ttl"] == 60.0

    boot_two = [_make_config("uncap", max_concurrent=None, result_ttl=None, capacity_owner="code")]
    await sync_actor_config(pg_conn, boot_two, schema=schema)
    uncapped = await _read_row(pg_conn, schema, "uncap")
    assert uncapped["max_concurrent"] is None, (
        "the code-owned un-cap deploys: the SET clause assigns the NULL "
        "unconditionally — the declaration of record wins"
    )
    assert uncapped["result_ttl"] is None

    change_events = [
        e for e in structlog_capture if e["event"] == "actor-config-capacity-code-change"
    ]
    announced = {e["field"]: (e["registered"], e["stored"]) for e in change_events}
    assert announced == {
        "max_concurrent": (None, 2),
        "result_ttl": (None, 60.0),
    }, "each un-capped field's deploy announces registered=None against the stored old value"
