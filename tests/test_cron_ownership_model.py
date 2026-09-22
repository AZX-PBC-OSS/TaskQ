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
  an old pod wrote during a mixed-version deploy; see below).
* The startup registration pass re-enables ONLY rows with
  ``disabled_by='auto'`` -- plus the mixed-version deploy's unmarked
  fingerprint of one (NULL marker, ``consecutive_failures`` at or past the
  threshold, ``last_fire_error`` set; issue #460) -- of code-owned,
  code-enabled specs: the boot is proof the ``@cron`` declaration is live
  again, so the disable is stale. An operator-disabled row is never
  reverted, whatever owns the spec.

The mixed-version window (issue #460): migration 01.00.19_02 is additive and
applies while OLD pods run, but the previous release's failure UPDATE cannot
name ``disabled_by``, so an old pod's auto-disable during a rolling deploy
lands as ``enabled=false, disabled_by=NULL``. The backfill migration
01.00.19_05 stamps every disabled row that predates it ``'operator'`` (the
pre-ownership population), which is what makes the residual NULL reading
sound: after it, a NULL-disabled row with the old failure arm's fingerprint
can only be an old pod's auto-disable, and the boot recovers it; without the
fingerprint it reads as an old pod's operator disable and survives.

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
from taskq.backend._records import parse_rowcount
from taskq.cron import CronScheduleSpec
from taskq.migrate import discover
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


def _backfill_sql(schema: str) -> str:
    """Render the bundled 01.00.19_05 backfill migration against a schema,
    so the pin exercises the shipped file, not a copy of its SQL."""
    filename = "01.00.19_05_pre_cron_disabled_by_backfill.sql"
    for migration in discover():
        if migration.filename == filename:
            return migration.render(schema)
    raise AssertionError(f"{filename} is not bundled")


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
    disabled_by: str | None = "auto",
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
        """NULL ``disabled_by`` on a disabled row with NO failure fingerprint
        is not re-enabled (and does not crash the registration pass).

        In a real database the pre-ownership population (rows disabled before
        the column existed) was stamped ``'operator'`` by the backfill
        migration 01.00.19_05, so it survives through that marker. The
        residual NULL-disabled row this test seeds can only be an old pod's
        write from a mixed-version window; without the old failure arm's
        fingerprint (counter at the threshold, error set) it reads as an old
        pod's operator disable, and the boot leaves it either way."""
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


class TestMixedVersionRollingDeploy:
    """Issue #460: the mixed-version deploy window.

    Migration 01.00.19_02 is additive and applies while OLD pods run, but the
    previous release's failure UPDATE writes ``enabled = false`` and cannot
    name ``disabled_by``. An old pod's auto-disable during the roll therefore
    lands as ``enabled=false, disabled_by=NULL`` -- a state the ownership
    model's trichotomy read as operator intent, so the boot recovery
    (``disabled_by = 'auto'``) never matched it and the schedule stayed
    disabled until a human intervened. The pins, one per cell the fix
    touches:

    * the old pod's disable fingerprint (NULL marker, counter at the
      threshold, error set) is recovered by a full restart -- the repro;
    * the same fingerprint at the registration pass level;
    * a NULL marker WITHOUT the fingerprint (an old pod's operator disable
      during the window) survives, like an operator disable;
    * the spec guards (``owner='operator'``, ``enabled=False``) hold for the
      fingerprint row too;
    * the backfill migration 01.00.19_05 stamps the pre-ownership disabled
      population ``'operator'``, which is what makes the residual NULL
      reading sound.

    The old pod is simulated with the PREVIOUS release's failure UPDATE,
    copied verbatim from the parent of 3f9641cd (the #412 commit): it is the
    exact statement an old pod runs during the roll.
    """

    _OLD_FAILURE_UPDATE = (
        'UPDATE "{schema}".cron_schedules s '  # Why: schema is a test-fixture identifier; values are $-bound.
        "SET last_fire_error = f.err, consecutive_failures = f.consecutive, "
        "enabled = CASE WHEN f.disable THEN false ELSE s.enabled END "
        "FROM unnest($1::uuid[], $2::text[], $3::int[], $4::bool[]) "
        "AS f(id, err, consecutive, disable) "
        "WHERE s.id = f.id AND s.enabled = true"
    )

    async def _old_pod_strikes(
        self,
        conn: asyncpg.Connection,
        schema: str,
        schedule_id: Any,
        error: str,
    ) -> None:
        """Three consecutive failing fires through the PREVIOUS release's
        failure UPDATE: no strike names ``disabled_by``, the third reaches
        the threshold and disables."""
        for consecutive, disable in ((1, False), (2, False), (3, True)):
            tag: str = await conn.execute(
                self._OLD_FAILURE_UPDATE.replace("{schema}", schema),
                [schedule_id],
                [error],
                [consecutive],
                [disable],
            )
            assert parse_rowcount(tag) == 1

    async def test_old_pod_auto_disable_during_roll_is_recovered_at_boot(
        self,
        pg_dsn: str,
    ) -> None:
        """The repro: an old pod's auto-disable during a rolling deploy
        leaves ``enabled=false, disabled_by=NULL``; the new release's boot
        re-declares the schedule and MUST return it to service.

        On the pre-fix code the recovery predicate requires
        ``disabled_by='auto'``, the row carries NULL, and the schedule stays
        disabled until a human re-enables it -- the unrecoverable cell."""
        schema = f"tcron_{new_base62()}".lower()
        await _prepare_schema_for(pg_dsn, schema)

        conn = await asyncpg.connect(pg_dsn)
        try:
            schedule_id = await seed_schedule(
                conn,
                schema,
                actor=_MISSING_ACTOR,
                name="roll-blip",
                cron_expr=_HOURLY,
                next_fire_at=await server_hour_floor(conn),
            )
            await self._old_pod_strikes(conn, schema, schedule_id, _LOOKUP_ERROR)

            # The exact state the deploy produces, and the cell the pre-fix
            # recovery cannot match: disabled, unmarked, evidence present.
            row = await schedule_row(conn, schema, schedule_id)
            assert row["enabled"] is False, "the old pod's third strike must disable"
            assert row["disabled_by"] is None, (
                "the previous release's failure UPDATE cannot name disabled_by, "
                "so its auto-disable lands unmarked"
            )
            assert row["consecutive_failures"] == 3
            assert row["last_fire_error"] == _LOOKUP_ERROR

            # The failure UPDATE never advances next_fire_at; push the row
            # out of due range so the boot's own cron loop cannot strike it
            # while the test observes the restart.
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
            lambda: _main(settings, _cron_registry=[_spec(_MISSING_ACTOR, "roll-blip")])
        )

        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await schedule_row(conn, schema, schedule_id)
        finally:
            await conn.close()
        assert row["enabled"] is True, (
            "the rolling deploy's unmarked auto-disable of a code-owned "
            "schedule is a state the new ownership model must recover: the "
            "boot re-declares the schedule, so the disable's only cause (a "
            "transient failure blip on the old pod) no longer exists"
        )
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None

        await _cleanup_schema_for(pg_dsn, schema)

    async def test_registration_pass_recovers_null_marker_with_failure_fingerprint(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The unrecoverable cell, at the registration pass: a disabled row
        with a NULL marker that carries the old release's disable fingerprint
        (counter at the threshold, error set) is reverted like an 'auto'
        row."""
        schema = module_pg_schema.schema_name
        schedule_id = await _seed_auto_disabled(
            conn=clean_pg_conn,
            schema=schema,
            actor=_MISSING_ACTOR,
            name="roll-fingerprint",
            disabled_by=None,
        )

        await _registration_pass(module_pg_schema, _spec(_MISSING_ACTOR, "roll-fingerprint"))

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is True, (
            "a NULL marker plus the old failure arm's fingerprint is an old "
            "pod's auto-disable from the mixed-version window; the boot must "
            "return the schedule to service"
        )
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None
        assert row["disabled_by"] is None

    async def test_registration_pass_leaves_null_marker_without_failure_fingerprint(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A NULL marker WITHOUT the fingerprint (an old pod's operator
        disable during the window, or a row the backfill has not reached):
        no failure evidence, so the boot reads intent and leaves the row
        alone."""
        schema = module_pg_schema.schema_name
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="roll-operator",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn) + timedelta(hours=1),
            enabled=False,
            disabled_by=None,
        )
        before = await schedule_row(clean_pg_conn, schema, schedule_id)

        await _registration_pass(module_pg_schema, _spec(_MISSING_ACTOR, "roll-operator"))

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after == before, (
            "a NULL marker with no failure fingerprint is not the old "
            "failure arm's write; the boot must not re-enable it"
        )

    async def test_operator_owned_spec_leaves_null_fingerprint_row_alone(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The fingerprint row under an ``owner='operator'`` spec: the pass
        owns nothing, the row stays put."""
        schema = module_pg_schema.schema_name
        schedule_id = await _seed_auto_disabled(
            conn=clean_pg_conn,
            schema=schema,
            actor=_MISSING_ACTOR,
            name="roll-operator-owned",
            disabled_by=None,
        )

        await _registration_pass(
            module_pg_schema,
            _spec(_MISSING_ACTOR, "roll-operator-owned", owner="operator"),
        )

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after["enabled"] is False
        assert after["disabled_by"] is None
        assert after["consecutive_failures"] == 3

    async def test_code_disabled_spec_leaves_null_fingerprint_row_alone(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The fingerprint row under a spec declared ``enabled=False``: the
        declaration does not assert the schedule should run, the pass does
        not spend its revert on it."""
        schema = module_pg_schema.schema_name
        schedule_id = await _seed_auto_disabled(
            conn=clean_pg_conn,
            schema=schema,
            actor=_MISSING_ACTOR,
            name="roll-code-off",
            disabled_by=None,
        )

        await _registration_pass(
            module_pg_schema,
            _spec(_MISSING_ACTOR, "roll-code-off", enabled=False),
        )

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after["enabled"] is False
        assert after["disabled_by"] is None
        assert after["consecutive_failures"] == 3

    async def test_backfill_stamps_pre_ownership_disabled_rows_operator(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Migration 01.00.19_05, executed from the bundled file itself: the
        pre-ownership disabled population (NULL marker, whatever the counter)
        is stamped ``'operator'``, enabled rows are left alone, and an
        already-stamped row is untouched (idempotent)."""
        schema = module_pg_schema.schema_name
        next_fire = await server_hour_floor(clean_pg_conn) + timedelta(hours=1)
        pre_ownership = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="backfill-pre-ownership",
            cron_expr=_HOURLY,
            next_fire_at=next_fire,
            enabled=False,
            disabled_by=None,
        )
        old_pod_blip = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="backfill-old-pod-blip",
            cron_expr=_HOURLY,
            next_fire_at=next_fire,
            enabled=False,
            consecutive_failures=3,
            disabled_by=None,
        )
        torn_enabled = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="backfill-torn-enabled",
            cron_expr=_HOURLY,
            next_fire_at=next_fire,
            enabled=True,
            disabled_by="auto",
        )
        already_operator = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="backfill-already-operator",
            cron_expr=_HOURLY,
            next_fire_at=next_fire,
            enabled=False,
            disabled_by="operator",
        )

        await clean_pg_conn.execute(_backfill_sql(schema))

        rows = await clean_pg_conn.fetch(
            f'SELECT id, enabled, disabled_by FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            "WHERE id = ANY($1::uuid[])",
            [pre_ownership, old_pod_blip, torn_enabled, already_operator],
        )
        by_id = {row["id"]: row for row in rows}
        assert len(by_id) == 4
        assert by_id[pre_ownership]["disabled_by"] == "operator", (
            "the pre-ownership population keeps the operator-intent reading "
            "the 01.00.19_02 header declared for it"
        )
        assert by_id[old_pod_blip]["disabled_by"] == "operator", (
            "a disabled row that exists when the backfill runs predates the "
            "residual-NULL reading, whatever wrote it"
        )
        assert by_id[torn_enabled]["enabled"] is True
        assert by_id[torn_enabled]["disabled_by"] == "auto", (
            "a marker on an enabled row is inert; the backfill leaves it for "
            "the next real disable to overwrite"
        )
        assert by_id[already_operator]["disabled_by"] == "operator"


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


class TestMarkerWriteArmsSerialize:
    """The re-enable arm (``update_schedule(enabled=True)`` / the admin
    enable SQL: ``enabled=true`` clears ``disabled_by``) and the cron loop's
    batched failure UPDATE (its disable arm writes ``enabled=false,
    disabled_by='auto'``) are NOT disjoint column sets: both write
    ``enabled``, ``consecutive_failures``, ``last_fire_error`` and
    ``disabled_by``. They still cannot produce a torn marker state: each arm
    writes the ``(enabled, disabled_by)`` pair as ONE atomic row version,
    and READ COMMITTED row locking serializes the two UPDATEs, the second
    writer re-evaluating its WHERE (EvalPlanQual) against the first
    writer's committed version. The pin below holds the loop's failure
    UPDATE in flight (the wedged factory keeps the tick's transaction open)
    while an operator's mid-blip disable AND re-enable both commit on a
    second connection, then asserts the strike still lands, owned by
    ``'auto'`` (recoverable at the next boot, exactly the state the
    registration pass reverts), never ``'operator'`` (which would strand
    the row until a human) and never torn (an enabled row carrying a
    marker)."""

    async def test_operator_reenable_mid_blip_and_the_in_flight_strike_still_lands_auto(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        import asyncio

        from .test_rt_cron_harness import seed_actor_config, wedge_events

        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _MISSING_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="mid-blip-reenable",
            cron_expr=_HOURLY,
            next_fire_at=await server_hour_floor(clean_pg_conn),
            consecutive_failures=2,  # the wedged fire is strike three: the disable arm fires
            payload_factory="tests.test_rt_cron_harness.wedge_then_fail",
        )

        with wedge_events() as (entered, gate):

            async def _tick() -> int:
                async with clean_pg_conn.transaction():
                    return await tick_cron(
                        clean_pg_conn,
                        settings,
                        make_backend(settings),
                        schema,
                        new_uuid(),
                    )

            task = asyncio.create_task(_tick())
            await entered.wait()

            # The operator's mid-blip disable and immediate re-enable, both
            # committing INSIDE the tick's window (the failure UPDATE has
            # not run yet). The disable stamps 'operator' (the admin/handle
            # shape); the re-enable clears the marker (the enable arm's
            # shape).
            operator = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                await operator.execute(
                    f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; the id is $-bound.
                    "SET enabled = false, disabled_by = 'operator' WHERE id = $1",
                    schedule_id,
                )
                await operator.execute(
                    f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; the id is $-bound.
                    "SET enabled = true, consecutive_failures = 0, "
                    "last_fire_error = NULL, disabled_by = NULL WHERE id = $1",
                    schedule_id,
                )
            finally:
                await operator.close()
            gate.set()
            fired = await task

        assert fired == 0, "the wedged fire fails, the strike write runs"

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is False and row["disabled_by"] == "auto", (
            "the in-flight strike (gathered while the schedule was enabled) "
            "re-disables the row and the marker is the loop's own 'auto': "
            "recoverable at the next boot, not stranded as operator intent"
        )
        assert row["consecutive_failures"] == 3
        assert row["last_fire_error"] is not None

        # The pair invariant the two arms guarantee under concurrent commit:
        # an enabled row never carries a marker, a disabled row always
        # carries one (or is pre-ownership NULL).
        assert (row["enabled"] and row["disabled_by"] is not None) is False
        assert ((not row["enabled"]) or row["disabled_by"] is None) is True
