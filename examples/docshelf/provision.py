"""DocShelf's deployment provisioning: the cohorts the boot projection
cannot see.

THE GAP (friction F2): the boot projection enumerates the COMPILED
graphs' (actor, queue) cohorts — but the TYPED ROUTE's arms fork at
RUNTIME (they are not compiled nodes), so an arm's cohort never
projects and the arm's rows strand (`stranded_jobs_no_actor_config`).
The adoption's cure is the operator path the vanilla fleet already
uses: REGISTER THE COHORTS AT DEPLOY — create-only, idempotent, before
the workers claim. The same discipline as migrate → workers → clients.
"""

from __future__ import annotations

import asyncpg

#: The route arms' cohorts (the wiring's RouteArm(actor=..., queue=...)).
DOCSHELF_ARM_COHORTS: tuple[tuple[str, str], ...] = (
    ("docshelf-summary", "docshelf-summary"),
    ("docshelf-ledger", "docshelf-ledger"),
    ("docshelf-quarantine", "docshelf-quarantine"),
)


async def provision_cohorts(pool: asyncpg.Pool, schema: str) -> None:
    """Create-only registration of the arm cohorts (never overwrites —
    the capacity fields are operator-owned once a row exists)."""
    for actor_name, queue in DOCSHELF_ARM_COHORTS:
        await pool.execute(
            f'INSERT INTO "{schema}".actor_config '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "(actor, queue, metadata, max_attempts, retry_kind) "
            "VALUES ($1, $2, '{\"workflow_actor\": true}'::jsonb, 3, 'transient') "
            "ON CONFLICT (actor) DO NOTHING",
            actor_name,
            queue,
        )
