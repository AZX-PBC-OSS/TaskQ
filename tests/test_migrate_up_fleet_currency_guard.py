"""Pin: the rolling-deploy overlap window closes by operator decision, not by a
runner-level guard — and the shipped adoption path keeps old pods working until
it closes.

## Why this pin was rewritten (the argument)

An earlier draft of this file demanded a fleet-currency guard in `migrate up`:
refuse to apply a post-phase migration while any pod might still be running the
previous release. Verified against the shipped system, that demand is
unsatisfiable:

* The only fleet-level oracle the runner could consult is the `workers` table,
  and it carries liveness only — hostname, pid, queues, heartbeat timestamps.
  It has no release or schema-version column, so the runner cannot tell an old
  pod from a new one. Any liveness-based refusal would stay armed while the
  fully-upgraded fleet heartbeats, which is exactly the state the shipped
  close-out runs in: the operator confirms the rollout completed, then applies
  `taskq migrate up --phase post` once, by hand. A liveness guard would turn
  that documented step into an impossibility.
* Refusing a bare `migrate up` outright would break fresh installs and the
  pinned single-run contract that applies `pre` then `post` together.
* The draft's own staged scenario attached no worker row at all, so even a
  faithful liveness guard would not have refused in it.

The shipped architecture instead assigns the overlap-window decision to the
only party who can make it: the operator, who can know that every replica now
runs the new release. That assignment is itself pinned green in
`tests/test_idempotency_scope_migrations.py` ("Who is allowed to close the
overlap window": the automatic migrate-on-start path must not apply the post
phase; the phase-ordering guard refuses a post before its same-version pre; a
plain `migrate up` applies pre then post in one run), and the adoption path is
pinned at the docs layer by `tests/test_deployment_manifest_docs_contract.py`
(the Kubernetes manifest's initContainer runs `migrate up --phase pre`, never
a bare `migrate up`, and the `--phase post` close-out is documented as a
human-gated step). Nothing in the runner can strengthen this further: a guard
needs a signal that distinguishes releases, and none exists.

## What this file pins (a regression in any of these fails)

1. **The docs cannot reach the broken state.** The deployment manifest's
   migrate step carries `--phase pre` and the post close-out is human-gated —
   asserted by delegating to the docs-contract source of truth rather than
   restating it, so the two cannot drift apart.
2. **The runner documents and enforces the operator-decision boundary.**
   `migrate up` accepts `--phase post` and forwards it to `apply_pending` as
   the explicit operator action; the migration runner's own documentation
   (the `apply_pending_locked` docstring, which governs the locked startup
   paths) states that lifecycle events decide nothing ("not on an operator's
   decision") and that the post phase "stays behind the operator's explicit
   ``taskq migrate up --phase post``". The enforcement halves — post-before-
   pre refusal, locked startup path defaulting to pre-only — are pinned green
   in `tests/test_idempotency_scope_migrations.py` and referenced here, not
   duplicated.
3. **The old-pod hazard stays nameable.** On the pre-phase-only mid-rollout
   steady state, previous-release enqueue SQL keeps working (the overlap
   window is real and safe). The shipped CLI behaviour is pinned truthfully:
   a bare `migrate up` applies the post phase in that same run, after which
   previous-release enqueue SQL fails with SQLSTATE 42P10, exactly as the
   post migration file's own header predicts. That consequence is the
   operator's knowledge — the reason property 1 exists — not a behaviour the
   runner is expected to refuse: the runner has no signal that could
   distinguish an old pod from a new one.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's validated fixture schema; all values are $n-bound.

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import asyncpg
import pytest
from typer.testing import CliRunner

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.cli import app
from taskq.settings import TaskQSettings
from tests.test_deployment_manifest_docs_contract import (
    test_manifest_init_container_applies_only_the_pre_phase as _docs_manifest_pre_pin,
)
from tests.test_deployment_manifest_docs_contract import (
    test_post_phase_is_documented_as_a_human_gated_step as _docs_human_gated_post_pin,
)

pytestmark = pytest.mark.integration

_runner = CliRunner()


async def _old_release_enqueue(conn: asyncpg.Connection, schema: str, key: str) -> None:
    """The exact enqueue SQL shape pre-01.00.03_01 code issues.

    Mirrors `01.00.03_01_post_idempotency_scope_drop_old_index.sql`'s own
    header comment verbatim: `ON CONFLICT (idempotency_key) WHERE
    idempotency_key IS NOT NULL`. Postgres resolves the ON CONFLICT arbiter
    index at PLAN time, so this is what fails once the old index is gone,
    regardless of whether the key value actually collides.
    """
    await conn.execute(
        f"""
        INSERT INTO "{schema}".jobs
            (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at,
             idempotency_key)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
        DO NOTHING
        """,
        new_uuid(),
        "legacy_actor",
        "default",
        "{}",
        3,
        "transient",
        datetime.now(UTC),
        key,
    )


class TestOverlapWindowClosesOnlyByOperatorDecision:
    """The mid-rollout contract, end to end.

    The Kubernetes manifest's initContainer applies `--phase pre` only (docs
    contract); old pods keep enqueueing throughout the rollout; the post
    phase that breaks them is applied solely by the operator's explicit,
    human-gated close-out; and a bare `migrate up` — the command an adopter
    would have to write to reach the broken state — applies the post phase
    with no refusal, which is why the manifest pin above is load-bearing.
    """

    def test_docs_contract_manifest_applies_only_the_pre_phase(self) -> None:
        """Delegate to the docs-contract source of truth.

        Importing and invoking the docs pins (aliased to non-`test_` names so
        pytest does not collect them twice) keeps a single definition of the
        manifest contract: if the shipped Kubernetes manifest regresses to a
        bare `migrate up`, or the post close-out stops being human-gated,
        THIS test fails through the same assertions that own the contract.
        """
        _docs_manifest_pre_pin()
        _docs_human_gated_post_pin()

    def test_migrate_up_accepts_phase_post_as_the_explicit_operator_action(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`migrate up --phase post` is a first-class, forwarded operator action.

        The close-out step of the documented deploy sequence has to exist on
        the CLI surface and reach the runner as `phase="post"` — the operator
        is the mechanism that closes the overlap window, so the flag that
        expresses the decision must not silently disappear or stop being
        forwarded. (The forwarding of `--phase pre` is pinned in
        tests/test_cli_migrate.py; this pins the operator's close-out value.)
        """
        conn = AsyncMock()
        monkeypatch.setattr(asyncpg, "connect", AsyncMock(return_value=conn))
        apply_mock = AsyncMock(return_value=[])
        monkeypatch.setattr(migrate_mod, "apply_pending", apply_mock)

        result = _runner.invoke(app, ["migrate", "up", "--phase", "post"])

        assert result.exit_code == 0, f"stderr: {result.stderr}"
        apply_mock.assert_awaited_once()
        assert apply_mock.await_args is not None
        assert apply_mock.await_args.kwargs["phase"] == "post"

    def test_runner_documents_closing_the_overlap_window_as_an_operator_decision(
        self,
    ) -> None:
        """The runner's own documentation must state the decision boundary.

        `apply_pending_locked` is the entry point behind every automatic
        migration path (migrate-on-start, `ui serve --migrate`); its
        docstring is where the runner explains WHY it never applies the post
        phase on its own: lifecycle events are not operator decisions, and
        the post phase stays behind the operator's explicit
        `taskq migrate up --phase post`. If that statement is lost from the
        runner, the next contributor has no documented reason to preserve
        the boundary.
        """
        doc = inspect.getdoc(migrate_mod.apply_pending_locked) or ""
        assert "not on an operator's decision" in doc, (
            "the runner must document that lifecycle events (restart, "
            "rollout, autoscale) are not operator decisions and must not "
            "apply the post phase on their own"
        )
        assert "taskq migrate up --phase post" in doc, (
            "the runner must document the operator's explicit close-out "
            "command as the sanctioned way to apply the post phase"
        )

    async def test_old_pods_keep_enqueueing_until_the_operator_closes_the_window(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Drive the deploy sequence and pin each step's truthful outcome.

          1. Mid-rollout steady state: 01.00.03_01:pre applied, post not —
             the schema state the manifest's `--phase pre` initContainer
             produces on every pod. A previous-release pod is connected and
             enqueuing with the old `ON CONFLICT` shape; the enqueue must
             succeed, which is the entire payoff of the phase split.
          2. Shipped CLI behaviour, pinned truthfully: a bare `migrate up`
             applies every pending migration, post included, in one run —
             the exact `apply_pending` call the CLI's `_up` issues with no
             `--phase` (the CLI forwarding itself is pinned in
             tests/test_cli_migrate.py, and the fresh-schema pre-before-post
             ordering in tests/test_idempotency_scope_migrations.py).
             Nothing refuses: the runner has no signal that could
             distinguish an old pod from a new one.
          3. The consequence the operator must know: once the post phase
             has run, the same old-shape enqueue fails with SQLSTATE 42P10
             (the arbiter index is resolved at plan time), exactly as the
             post migration file's own header predicts.

        Step 3 is why step 1's docs pin is load-bearing: the manifest must
        never run the bare command mid-rollout, and closing the window is
        the operator's deliberate, human-gated act — not something the
        runner can time, because the `workers` table cannot tell a
        heartbeating old pod from a heartbeating new one.
        """
        schema = settings.schema_name

        # Step 1: mid-rollout steady state after the manifest's `--phase pre`.
        await migrate_mod.apply_pending(pg_conn, schema=schema, target="01.00.03_01")
        applied = await migrate_mod.list_applied(pg_conn, schema)
        assert "01.00.03_01:pre" in applied
        assert "01.00.03_01:post" not in applied

        # The previous-release pod's connection is open and enqueuing
        # successfully against this schema — the overlap window is open and
        # safe, which must hold for the entire rollout.
        await _old_release_enqueue(pg_conn, schema, key="steady-state-key")

        # Step 2: a bare `migrate up` (the CLI's phase=None path) applies the
        # post phase in the same run. Pinned as current behaviour: the runner
        # has no release-aware fleet signal to refuse on.
        applied_now = await migrate_mod.apply_pending(pg_conn, schema=schema)
        assert "01.00.03_01:post" in {m.key for m in applied_now}, (
            "bare `migrate up` must keep applying every pending phase in one "
            "run — if this now refuses, the single-run contract and this pin "
            "need a deliberate redesign together"
        )

        # Step 3: the same previous-release pod, still connected, enqueues
        # again with the same SQL shape that worked one step ago. The old
        # arbiter index is gone, so the statement fails at plan time — the
        # hazard the operator's human-gated close-out exists to sequence.
        with pytest.raises(asyncpg.InvalidColumnReferenceError) as exc_info:
            await _old_release_enqueue(pg_conn, schema, key="post-migration-key")
        assert exc_info.value.sqlstate == "42P10"


class TestPostPhaseApplyHasNoReleaseAwareFleetSignal:
    """There is no signal the runner could consult, by design of the schema.

    The `workers` table (populated at pod startup, heartbeated, swept by the
    leader per docs/guides/workers.md) carries liveness only: no release or
    schema-version column. A live row is therefore ambiguous — it is what a
    fully-upgraded fleet looks like during the documented close-out AND what
    a half-rolled-out fleet looks like mid-rollout. The runner cannot
    distinguish them, which is precisely why applying the post phase is the
    operator's decision (see the module docstring and
    tests/test_idempotency_scope_migrations.py's "Who is allowed to close
    the overlap window").
    """

    async def test_explicit_post_apply_proceeds_despite_live_worker_heartbeats(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """The operator's explicit close-out works against a heartbeating fleet.

        Sequence mirroring the documented deploy: the manifest's `--phase
        pre` runs, pods (old and new alike) heartbeat, and the operator's
        explicit `--phase post` then applies WITHOUT being refused. A guard
        keyed on live heartbeats would break exactly this — the shipped
        close-out runs while the upgraded fleet is heartbeating — which is
        the core reason the demand for a liveness-based refusal was dropped.
        """
        schema = settings.schema_name
        await migrate_mod.apply_pending(pg_conn, schema=schema, phase="pre")

        # A live pod, attached and heartbeating: indistinguishable by release.
        worker_id = new_uuid()
        await pg_conn.execute(
            f"""
            INSERT INTO "{schema}".workers
                (id, hostname, pid, queues, started_at, last_seen_at)
            VALUES ($1, $2, $3, $4, now(), now())
            """,
            worker_id,
            "attached-pod",
            12345,
            ["default"],
        )

        # Tripwire: if a release-aware fleet-currency guard ever ships on
        # taskq.migrate, this pin's premise changes — wire it into the CLI's
        # documented paths and REWRITE this assertion (do not just delete
        # it): with a real release signal, a guard could finally distinguish
        # the mid-rollout state from the close-out state.
        assert not hasattr(migrate_mod, "assert_safe_to_apply_post_phase"), (
            "a release-aware fleet-currency guard now exists on taskq.migrate "
            "-- decide how it distinguishes the operator's close-out from a "
            "mid-rollout apply, wire it into the CLI paths, and rewrite this "
            "pin around the new signal"
        )

        # The operator's explicit close-out proceeds despite the live worker
        # row — by design: the runner defers the fleet-wide decision to the
        # party typing the command.
        applied_now = await migrate_mod.apply_pending(pg_conn, schema=schema, phase="post")
        assert "01.00.03_01:post" in {m.key for m in applied_now}
