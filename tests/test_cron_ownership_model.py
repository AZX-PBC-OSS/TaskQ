"""The cron schedule ownership model: who disabled a schedule, and what a
worker restart is allowed to do about it (issue #342).

Before the ownership model, cron auto-disable wrote only ``enabled=false``,
indistinguishable from an operator's deliberate disable, and startup cron
registration was create-only, so BOTH stayed disabled forever: a transient
partial-DB blip (fires fail, strike writes commit -- realistic during
failover/saturation) permanently halted critical recurring work until a human
re-enabled it.

The model, as implemented:

* ``cron_schedules.disabled_by``: ``'auto'`` (the cron loop's failure-count
  auto-disable, written alongside ``enabled=false``), ``'operator'`` (schedule
  handle disable, admin UI, actor deregistration), or NULL (enabled, or a row
  disabled before the column existed: the safe reading of that ambiguity is
  operator intent).
* The startup registration pass re-enables ONLY rows with
  ``disabled_by='auto'`` of code-owned, code-enabled specs: the boot is proof
  the ``@cron`` declaration is live again, so the disable is stale. An
  operator-disabled row is never reverted, whatever owns the spec.

The pins: an auto-disabled row recovered by a restart (the bug), an
operator-disabled row surviving a restart (the negative, and the exact trap
the create-only design guards), and the mid-run auto-disable unchanged (kept
green in test_rt_cron_failure_domains.py, with the ``disabled_by`` assertion
added there).

The restart-level tests seed the exact post-auto-disable row state (the state
the failure path is proven to produce) rather than driving strikes through a
live worker: the failure UPDATE never advances ``next_fire_at``, so a
re-enabled row sits due and the boot's own cron loop would keep striking it,
making the final assertion a race. A future ``next_fire_at`` keeps the loop
out of the row, which is what makes the pins deterministic.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.cron import CronScheduleSpec
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _main

from .test_rt_cron_harness import (
    _HOURLY,
    cron_settings,
    make_backend,
    pool_backend,
    schedule_row,
    seed_schedule,
    server_hour_floor,
)
from .test_worker_bootstrap import (
    _cleanup_schema_for,
    _prepare_schema_for,
    _run_and_cancel,
    _settings_for,
)

pytestmark = pytest.mark.integration

_MISSING_ACTOR = "ownership_missing_actor"
_LOOKUP_ERROR = f"Actor '{_MISSING_ACTOR}' not found in actor_config"


def _spec(actor: str, name: str = "", **overrides: Any) -> CronScheduleSpec:
    base: dict[str, Any] = {
        "actor": actor,
        "name": name,
        "cron_expr": _HOURLY,
        "timezone": "UTC",
    }
    base.update(overrides)
    return CronScheduleSpec(**base)


async def _three_real_strikes(
    clean_pg_conn: asyncpg.Connection,
    schema: str,
    schedule_id: Any,
) -> dict[str, Any]:
    """Drive the REAL failure path (missing actor, three ``tick_cron`` calls)
    to the auto-disable threshold."""
    settings = cron_settings(schema)
    worker_id = new_uuid()
    for _ in range(3):
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
            )
        assert fired == 0
    return await schedule_row(clean_pg_conn, schema, schedule_id)


async def _seed_auto_disabled(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    name: str,
    disabled_by: str = "auto",
) -> Any:
    """The exact row state the cron loop's auto-disable leaves behind
    (``enabled=false``, three strikes, the error, the ownership marker), with
    ``next_fire_at`` pushed out of due range so a boot's own cron loop cannot
    strike the row while the test observes the restart."""
    schedule_id = await seed_schedule(
        conn,
        schema,
        actor=actor,
        name=name,
        cron_expr=_HOURLY,
        next_fire_at=await server_hour_floor(conn) + timedelta(hours=1),
        consecutive_failures=3,
        enabled=False,
        disabled_by=disabled_by,
    )
    await conn.execute(
        f'UPDATE "{schema}".cron_schedules SET last_fire_error = $2 WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
        schedule_id,
        _LOOKUP_ERROR,
    )
    return schedule_id


async def _registration_pass(
    module_pg_schema: ModulePgSchema,
    spec: CronScheduleSpec,
) -> None:
    """One worker registration pass against the module schema -- the restart's
    registration step, extracted verbatim from ``_main`` -- over a shell
    ``WorkerDeps`` (the registration pass touches only the dispatcher pool:
    the clock seed and the re-enable UPDATE)."""
    from taskq.worker._bootstrap import _register_cron_schedules

    settings = _settings_for(module_pg_schema.pg_dsn, module_pg_schema.schema_name)
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:
        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=pool,
            heartbeat_pool=pool,
            worker_pool=pool,
            notify_conn=None,
            leader_conn=None,
        )
        await _register_cron_schedules(pool_backend(settings, pool), deps, settings, [spec])
    finally:
        await pool.close()


class TestAutoDisableRecovery:
    """The bug: an 'auto' disable must not outlive the code that re-declares it."""

    async def test_restart_recovers_a_transient_blip_without_disabled_by(
        self,
        pg_dsn: str,
    ) -> None:
        """The red pin, written without touching ``disabled_by`` so it runs on
        the pre-fix code too: three real strikes auto-disable the schedule, a
        worker restart re-declares it, and the row MUST come back enabled.
        On the pre-fix main this is exactly the bug: the row stays disabled
        forever."""
        schema = f"tcron_{new_base62()}".lower()
        await _prepare_schema_for(pg_dsn, schema)

        conn = await asyncpg.connect(pg_dsn)
        try:
            schedule_id = new_uuid()
            # Seeded without the disabled_by column so the pin runs (and
            # fails behaviorally) on the pre-fix code too: no ownership
            # marker exists yet, exactly the state issue #342 triaged.
            await conn.execute(
                f'INSERT INTO "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
                "(id, actor, name, cron_expr, timezone, dst_strategy, payload_factory, "
                "enabled, next_fire_at, metadata, consecutive_failures) "
                "VALUES ($1, $2, $3, $4, 'UTC', 'skip', NULL, true, $5, '{}'::jsonb, 0)",
                schedule_id,
                _MISSING_ACTOR,
                "blip",
                _HOURLY,
                await server_hour_floor(conn),
            )
            settings = cron_settings(schema)
            worker_id = new_uuid()
            for _ in range(3):
                async with conn.transaction():
                    fired = await tick_cron(
                        conn,
                        settings,
                        make_backend(settings),
                        schema,
                        worker_id,
                    )
                assert fired == 0
            row = await schedule_row(conn, schema, schedule_id)
            assert row["enabled"] is False, "the three strikes must auto-disable"
            # The failure UPDATE never advances next_fire_at, so the disabled
            # row sits due; push it out of due range so the boot's own cron
            # loop cannot strike the row while the test observes the restart.
            await conn.execute(
                f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
                "SET next_fire_at = $2 WHERE id = $1",
                schedule_id,
                await server_hour_floor(conn) + timedelta(hours=1),
            )
        finally:
            await conn.close()

        settings = _settings_for(pg_dsn, schema)
        await _run_and_cancel(
            lambda: _main(settings, _cron_registry=[_spec(_MISSING_ACTOR, "blip")])
        )

        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await schedule_row(conn, schema, schedule_id)
        finally:
            await conn.close()
        assert row["enabled"] is True, (
            "the code re-declares the schedule at every boot: an auto-disable "
            "left by a transient failure blip must not halt recurring work "
            "until a human re-enables it"
        )
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None

        await _cleanup_schema_for(pg_dsn, schema)

    async def test_registration_pass_reverts_auto_disabled_code_owned_schedule(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Three real strikes auto-disable the schedule; a registration pass
        over the same spec reverts the stale auto-disable (enabled=true,
        consecutive_failures=0, last_fire_error and disabled_by cleared)."""
        schema = module_pg_schema.schema_name
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="recoverable",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn),
        )

        row = await _three_real_strikes(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is False
        assert row["disabled_by"] == "auto"

        await _registration_pass(module_pg_schema, _spec(_MISSING_ACTOR, "recoverable"))

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is True, (
            "a code-owned schedule's auto-disable is stale the moment the code "
            "re-declares it at boot; the registration pass must revert it"
        )
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None
        assert row["disabled_by"] is None

    async def test_end_to_end_worker_restart_reenables(
        self,
        pg_dsn: str,
    ) -> None:
        """The full restart path through ``_main``: a worker whose registry
        re-declares an auto-disabled schedule comes back with the row enabled."""
        schema = f"tcron_{new_base62()}".lower()
        await _prepare_schema_for(pg_dsn, schema)

        conn = await asyncpg.connect(pg_dsn)
        try:
            schedule_id = await _seed_auto_disabled(
                conn, schema, actor=_MISSING_ACTOR, name="e2e-recover"
            )
        finally:
            await conn.close()

        settings = _settings_for(pg_dsn, schema)
        await _run_and_cancel(
            lambda: _main(settings, _cron_registry=[_spec(_MISSING_ACTOR, "e2e-recover")])
        )

        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await schedule_row(conn, schema, schedule_id)
        finally:
            await conn.close()
        assert row["enabled"] is True, (
            "the worker restart must re-enable the auto-disabled schedule the "
            "code re-declares; a transient failure blip must not halt "
            "recurring work until a human intervenes"
        )
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None
        assert row["disabled_by"] is None

        await _cleanup_schema_for(pg_dsn, schema)


class TestOperatorIntentIsNeverReverted:
    """The negative: a deliberate disable outlives every restart."""

    async def test_registration_pass_leaves_operator_disabled_row_alone(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """``disabled_by='operator'`` under a code-owned spec: the registration
        pass neither re-enables the row nor touches any of its columns."""
        schema = module_pg_schema.schema_name
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="operator-off",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn) + timedelta(hours=1),
            enabled=False,
            disabled_by="operator",
        )
        before = await schedule_row(clean_pg_conn, schema, schedule_id)

        await _registration_pass(module_pg_schema, _spec(_MISSING_ACTOR, "operator-off"))

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after == before, (
            "an operator's deliberate disable is intent, not a stale "
            "auto-disable: the registration pass must not touch the row"
        )

    async def test_end_to_end_worker_restart_keeps_operator_disabled_row_disabled(
        self,
        pg_dsn: str,
    ) -> None:
        """Full ``_main`` restart over an operator-disabled row: still
        disabled, still attributed to the operator."""
        schema = f"tcron_{new_base62()}".lower()
        await _prepare_schema_for(pg_dsn, schema)

        conn = await asyncpg.connect(pg_dsn)
        try:
            schedule_id = await _seed_auto_disabled(
                conn, schema, actor=_MISSING_ACTOR, name="e2e-operator", disabled_by="operator"
            )
        finally:
            await conn.close()

        settings = _settings_for(pg_dsn, schema)
        await _run_and_cancel(
            lambda: _main(settings, _cron_registry=[_spec(_MISSING_ACTOR, "e2e-operator")])
        )

        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await schedule_row(conn, schema, schedule_id)
        finally:
            await conn.close()
        assert row["enabled"] is False, (
            "the worker restart must not re-enable a row the operator disabled"
        )
        assert row["disabled_by"] == "operator"

        await _cleanup_schema_for(pg_dsn, schema)

    async def test_operator_owned_spec_never_reverts_even_an_auto_disable(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A spec declaring ``owner='operator'`` ships the declaration only:
        the registration pass owns nothing, so even an ``'auto'`` disable it
        could technically revert stays put."""
        schema = module_pg_schema.schema_name
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="operator-owned",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn) + timedelta(hours=1),
            enabled=False,
            disabled_by="auto",
        )

        await _registration_pass(
            module_pg_schema,
            _spec(_MISSING_ACTOR, "operator-owned", owner="operator"),
        )

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after["enabled"] is False
        assert after["disabled_by"] == "auto"

    async def test_code_disabled_spec_does_not_revert_an_auto_disable(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A spec declared ``enabled=False`` does not assert the schedule
        should run, so the registration pass does not spend its revert on it."""
        schema = module_pg_schema.schema_name
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="code-off",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn) + timedelta(hours=1),
            enabled=False,
            disabled_by="auto",
        )

        await _registration_pass(
            module_pg_schema,
            _spec(_MISSING_ACTOR, "code-off", enabled=False),
        )

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after["enabled"] is False
        assert after["disabled_by"] == "auto"

    async def test_pre_ownership_disabled_row_survives_restart(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """NULL ``disabled_by`` on a disabled row predates ownership tracking;
        the safe reading of that ambiguity is operator intent, so the row is
        not re-enabled (and does not crash the registration pass)."""
        schema = module_pg_schema.schema_name
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="legacy-off",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn) + timedelta(hours=1),
            enabled=False,
        )
        before = await schedule_row(clean_pg_conn, schema, schedule_id)

        await _registration_pass(module_pg_schema, _spec(_MISSING_ACTOR, "legacy-off"))

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after == before


class TestCreatePathProvenance:
    """What a fresh INSERT writes into the ownership marker."""

    async def test_operator_owned_schedule_created_disabled_carries_operator_marker(
        self,
        module_pg_pool: asyncpg.Pool,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """An operator-owned schedule created disabled is operator intent from
        birth: ``disabled_by='operator'`` on the fresh row, so no boot reverts
        it. A code-owned schedule created disabled stays NULL: the code
        declared it, no disable event happened, and NULL reads as operator
        intent at any later boot."""
        from taskq.backend._protocol import ScheduleCreateArgs

        from .test_rt_cron_harness import pool_backend

        settings = _settings_for(module_pg_schema.pg_dsn, module_pg_schema.schema_name)
        backend = pool_backend(settings, module_pg_pool)
        next_fire = await server_hour_floor(clean_pg_conn) + timedelta(hours=1)
        common: dict[str, Any] = {
            "actor": _MISSING_ACTOR,
            "cron_expr": _HOURLY,
            "timezone": "UTC",
            "next_fire_at": next_fire,
            "enabled": False,
        }

        operator_row = await backend.create_schedule(
            ScheduleCreateArgs(name="created-operator-off", owner="operator", **common)
        )
        code_row = await backend.create_schedule(
            ScheduleCreateArgs(name="created-code-off", owner="code", **common)
        )

        assert operator_row.disabled_by == "operator"
        assert code_row.disabled_by is None
