"""Red test: `migrate up` has no fleet-currency guard, and the shipped
Kubernetes manifest's migration step is therefore unsafe mid-rollout.

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's validated fixture schema; all values are $n-bound.

## The contract this pins

`docs/guides/deployment.md`'s "Kubernetes Deployment" section — the
flagship, copy-pasted-by-every-adopter manifest — runs the migration step
as an initContainer with the bare command:

    command: ["taskq", "migrate", "up"]

No `--phase pre`. Per `src/taskq/cli.py`'s `migrate_up` / `_up`, a bare
`migrate up` applies EVERY pending migration, `pre` and `post` together,
in one call (pinned already by
`tests/test_idempotency_scope_migrations.py::TestPhaseOrderingGuard::
test_plain_up_applies_pre_before_post_in_one_run`).

A Kubernetes rolling update runs every new pod's initContainers —
including this one — as each new pod comes up, WHILE old pods are still
live and still serving traffic (that is what "rolling" means; it is also
explicit fleet doctrine in docs/guides/ops.md's connection-budget section:
"orchestrators ... bring the new pods up before draining the old ones").
So the very first new-code pod's initContainer, followed literally, is
the operator's *only* migration step — and it applies any outstanding
`post`-phase migration immediately, while every old pod is still running.

`src/taskq/migrations/01.00.03_01_post_idempotency_scope_drop_old_index.sql`
documents, in its own header, exactly what this does: dropping the old
single-column unique index "while any pre-that-release worker is still
running turns EVERY enqueue from that worker into a hard failure ...
SQLSTATE 42P10". The file's ONLY protection against being applied early
is the same-VERSION pre-before-post ordering guard in
`taskq.migrate.apply_pending` (ValueError: "cannot be applied before its
pre-phase counterpart") — which says nothing about whether any pod in the
fleet is still running the code that needs the dropped structure. A
schema that already has last release's `pre` applied (the ordinary,
expected steady state between deploys) satisfies that guard trivially:
`--phase post`'s own precondition is already met, so `migrate up` walks
straight through it.

This test drives the exact documented command against the exact
documented topology (old-release connection still open, enqueuing with
the old `ON CONFLICT` shape) and shows the break the migration file's
header predicts. It does NOT modify `apply_pending`, the CLI, or the
manifest — it pins the current (undesired) behaviour as failing, per the
brief's "leave it red" instruction.

## Desired behaviour (what should exist, and doesn't)

Oban ships exactly this hazard class as a *named, callable* safety check:
`vendor/oban/lib/oban/migration.ex`, `Oban.Migration.verify_migrated!/1`
(lines 209-254) raises a clear `RuntimeError` distinguishing "no
migrations run" from "migrations outdated" by comparing the *code's*
compiled-in current version against what the database records — a
currency check callable from application code, independent of whichever
migration tool actually applied the schema. Procrastinate's
`vendor/procrastinate/docs/howto/production/migrations.md` ("The safer
way, without service interruption") describes the same `pre`/`post` /
blue-green naming TaskQ uses, but is explicit that operators must
manually stop at every version with migrations and apply `post` only
after that version's code is confirmed live everywhere — i.e. Procrastinate
does not attempt to automate the ordering guarantee either; it documents
it as an entirely manual discipline and provides no tool-level guard past
naming the files correctly.

TaskQ already does better than Procrastinate by refusing post-before-its-
own-pre programmatically. The gap this test pins is one rung up: nothing
refuses (or even warns on) a `post`-phase apply while the ledger has no
signal that every pod is upgraded — because no such signal exists. A
`taskq migrate up --phase post` (or a bare `migrate up`) has no way to
know "is anyone still running the old code?" TaskQ's own worker registers
itself in the `workers` table with a heartbeat (see
`docs/guides/workers.md`), which is the natural fleet-currency oracle;
nothing reads it here.

## Test-seam note

`Fleet`/`open_fleet` in `tests/_fleet.py` model full pod lifecycles but
have no notion of "pod running an older code release" (every pod in the
harness runs today's code against whatever migration state exists) — the
"old release still connected" side of the rolling-deploy window has to be
simulated by hand-executing the pre-scoped-idempotency `ON CONFLICT` SQL
shape directly, exactly as `tests/test_idempotency_scope_migrations.py`
already does for the same migration. This is not a workaround for an
otherwise-reachable behaviour; it is the only way to exercise "code that
predates this migration" at all, since the current source tree cannot run
old code.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's validated fixture schema; all values are $n-bound.

from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.settings import TaskQSettings

pytestmark = pytest.mark.integration


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


class TestMigrateUpHasNoFleetCurrencyGuard:
    async def test_deployment_doc_migrate_command_breaks_old_pods_mid_rollout(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Reproduce docs/guides/deployment.md's Kubernetes manifest literally.

        Sequence, matching the doc's own stated deploy order plus one
        detail the doc never states because nothing forces it: a rolling
        update runs the new pod's initContainer while old pods are still
        live.

          1. Prior release's steady state: 01.00.03_01:pre already applied
             (an ordinary "already migrated one version" schema — not a
             fresh DB). An old-release pod is connected and enqueuing.
          2. Operator follows deployment.md exactly: the new pod's
             initContainer runs `taskq migrate up` (no --phase flag, per
             the manifest's `command: ["taskq", "migrate", "up"]`).
          3. The still-connected old-release pod enqueues again, using the
             unmodified pre-existing `ON CONFLICT` shape from step 1.

        Desired: the old pod's enqueue keeps working, because nothing
        should apply a breaking post-phase migration while unupgraded pods
        are attached. Actual: `migrate up` has no way to know the old pod
        exists, applies 01.00.03_01:post immediately, and the old pod's
        very next enqueue raises `asyncpg.exceptions.InvalidColumnReferenceError`
        (SQLSTATE 42P10 — "there is no unique or exclusion constraint
        matching the ON CONFLICT specification"), exactly as predicted by
        that migration file's own header.
        """
        schema = settings.schema_name

        # Step 1: ordinary prior-release steady state.
        await migrate_mod.apply_pending(pg_conn, schema=schema, target="01.00.03_01")
        applied = await migrate_mod.list_applied(pg_conn, schema)
        assert "01.00.03_01:pre" in applied
        assert "01.00.03_01:post" not in applied

        # The old-release pod's connection is already open and already
        # enqueuing successfully against this schema -- this call must
        # succeed, proving the "old pod" is genuinely live and working
        # before the new pod's migration step runs.
        await _old_release_enqueue(pg_conn, schema, key="steady-state-key")

        # Step 2: literally what docs/guides/deployment.md's Kubernetes
        # manifest instructs: `taskq migrate up`, no --phase.
        await migrate_mod.apply_pending(pg_conn, schema=schema)
        applied = await migrate_mod.list_applied(pg_conn, schema)
        assert "01.00.03_01:post" in applied, (
            "sanity check: the doc-literal command really does apply the "
            "post-phase migration in one shot"
        )

        # Step 3: the SAME old-release pod, still connected, enqueues
        # again with the same SQL shape that worked one step ago.
        #
        # DESIRED: this succeeds -- a rolling deploy must never break a
        # pod that has not been replaced yet. `migrate up`, run exactly as
        # the shipped manifest instructs, should either refuse to apply a
        # fleet-currency-sensitive post-phase migration with no fleet
        # signal to check against, or the deployment doc's own migrate
        # step should never be able to reach this state. Neither exists
        # today, so this is left failing.
        await _old_release_enqueue(pg_conn, schema, key="post-migration-key")


class TestApplyPendingPostHasNoFleetSignalToCheck:
    async def test_post_phase_apply_has_no_worker_liveness_check_available(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """There is no oracle `apply_pending` could consult even if it wanted to.

        TaskQ's own `workers` table (populated by `create_worker` at pod
        startup, read by the leader's stale-worker sweep per
        docs/guides/workers.md) is the natural fleet-currency signal: a row
        with a live heartbeat is proof a pod is still attached. This test
        shows that signal is not surfaced anywhere in the migration path:
        `apply_pending`'s public parameters (`phase`, `target`,
        `max_steps`) contain nothing that could express "refuse if any
        worker row predates version X", and no such check runs internally
        -- a post-phase apply proceeds with a live `workers` row present
        for the version that is about to break, silently.

        Compare `Oban.Migration.verify_migrated!/1`
        (vendor/oban/lib/oban/migration.ex:209-254), which at least
        exposes a version-currency check as a public, independently
        callable function -- so an operator (or a startup hook) CAN gate
        on it, even though Oban does not wire it into the migration apply
        path automatically either. TaskQ has no equivalent function to
        call, callable or otherwise.
        """
        schema = settings.schema_name
        await migrate_mod.apply_pending(pg_conn, schema=schema, target="01.00.03_01")

        # Simulate a live old-release pod: insert a `workers` row exactly
        # as `create_worker` would, with a fresh heartbeat, so a
        # currency-aware guard would have something to find.
        worker_id = new_uuid()
        await pg_conn.execute(
            f"""
            INSERT INTO "{schema}".workers
                (id, hostname, pid, queues, started_at, last_seen_at)
            VALUES ($1, $2, $3, $4, now(), now())
            """,
            worker_id,
            "old-release-pod",
            12345,
            ["default"],
        )

        # DESIRED: `apply_pending` (or some sibling entry point) exposes a
        # way to ask "is it safe to apply post-phase migrations right
        # now?" that would say no here, because a worker row with a fresh
        # heartbeat is present and its release is unknown/unconfirmed.
        # ACTUAL: no such function exists on the public `taskq.migrate`
        # surface -- `apply_pending` proceeds unconditionally. This
        # assertion documents the missing capability by asserting the
        # function is absent; it must be removed (not adjusted) once one
        # is added.
        assert not hasattr(migrate_mod, "assert_safe_to_apply_post_phase"), (
            "a fleet-currency guard now exists on taskq.migrate -- wire it "
            "into the CLI's `migrate up` / `migrate up --phase post` path "
            "and delete this placeholder assertion, it is no longer the gap"
        )

        # And, unconditionally, the post phase does go ahead despite the
        # live worker row -- pinning today's actual (undesired) behaviour.
        await migrate_mod.apply_pending(pg_conn, schema=schema)
        applied = await migrate_mod.list_applied(pg_conn, schema)
        assert "01.00.03_01:post" in applied
