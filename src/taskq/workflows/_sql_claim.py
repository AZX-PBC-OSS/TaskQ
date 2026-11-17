"""The ONE current-claim association core (the ruling's N-8a renderer).

Every current-claim lookup — the operator cancel, the fail-closed peer
cancel, and the §5.4 non-drainage fence — resolves a job's CURRENT
ledger claim through this exact association, never a second spelling:

    ledger.job_id = job.id
    AND ledger.claim_epoch = job.claim_epoch
    AND ledger.step_key = job.step_key
    AND ledger.map_index IS NOT DISTINCT FROM job.map_index
    AND ledger.status IN (<the caller's status domain>)
    AND NOT EXISTS (        -- THE STRICT SINGLE-ROW ASSOCIATION
        SELECT 1 FROM {schema}.wf_step_ledger AS newer
        WHERE newer.job_id = job.id
          AND newer.claim_epoch = job.claim_epoch
          AND newer.step_key = job.step_key
          AND newer.map_index IS NOT DISTINCT FROM job.map_index
          AND newer.status IN (<the caller's status domain>)
          AND (newer.attempt, newer.updated_at, newer.id)
            > (ledger.attempt, ledger.updated_at, ledger.id)
    )

The core carries NO attempt comparison as an ASSOCIATION leg (a
non-consuming release refunds the job's displayed ordinal below the
recorded claim's attempt while the current epoch stands — an
attempt-equality leg resolves zero rows and unfences the current claim)
and no park-metadata test (the epoch already identifies the current
claim across every refund representation). The epoch identifies the
current physical claim generation; the step and null-safe map legs
exclude inner ``ctx.substep``/loop-iteration rows and map siblings that
share the job and epoch. No ``COALESCE`` sentinel and no job-wide or
flow-wide match is permitted.

THE STRICT SINGLE-ROW ASSOCIATION (round-4 NEW-6) — the single-row
property is a property of THIS SQL, never a caller protocol: the
ledger's own unique arbiter ``(flow_id, step_key, COALESCE(map_index,
-1), attempt, claim_epoch)`` makes same-job/same-epoch/same-step/
same-map rows with DIFFERENT attempts physically distinct rows, so the
four association legs alone can admit two of them and one job winner
would write two fences and two events. The maximality probe admits
exactly one row per job: the tie-break is deterministic and total — the
HIGHEST attempt, then the LATEST ``updated_at``, then the HIGHEST ``id``
(the claim's own generation order, ending in the primary key so no tie
survives). The probe is scoped to the caller's status domain, so a
higher-attempt TERMINAL row outside the domain can never veto the live
row inside it (terminal history must not unfence a current claim).
Because ``id`` is the primary key the comparison is a strict total
order, so exactly one associated row survives per job in every estate.

The catalog cannot call Python (the migration's SQL function inlines
this core), so a parity control proves the function's inline spelling
matches this renderer's output — the shared truth, never a third policy.
"""

from __future__ import annotations

from typing import Final

__all__ = ["current_claim_join"]

# ruff: noqa: S608  # Why: the rendered fragment is module-constant text — the aliases and the fixed status vocabulary are our own call-site constants, never caller input; every value is $-bound by the caller's statement.

#: The ledger table the maximality probe re-queries. The token survives
#: the module-constant assembly and is substituted by the shared render
#: pass with the caller's validated schema (the render's own contract).
_LEDGER_TABLE: Final[str] = "{schema}.wf_step_ledger"


def _core(job_alias: str, ledger_alias: str) -> str:
    """The four association legs, no policy — the shared identity itself."""
    job, ledger = job_alias, ledger_alias
    return (
        f"{ledger}.job_id = {job}.id"
        f" AND {ledger}.claim_epoch = {job}.claim_epoch"
        f" AND {ledger}.step_key = {job}.step_key"
        f" AND {ledger}.map_index IS NOT DISTINCT FROM {job}.map_index"
    )


def current_claim_join(job_alias: str, ledger_alias: str, status_domain: tuple[str, ...]) -> str:
    """Render the current-claim association with the caller's own aliases
    and status domain. Neutral leaf SQL: no parameters, no policy — the
    caller's statement owns its capture and its fence (the exact ledger
    ``id`` + pre-update ``updated_at``); this renderer owns the identity
    legs, the domain's one spelling, and the strict single-row
    maximality."""
    if not status_domain:
        raise ValueError("the current-claim association requires a status domain")
    job, ledger = job_alias, ledger_alias
    statuses = ", ".join(f"'{status}'" for status in status_domain)
    domain = f"{ledger}.status IN ({statuses})"
    return (
        f"{_core(job, ledger)}\n"
        f"      AND {domain}\n"
        f"      AND NOT EXISTS (\n"
        f"          SELECT 1 FROM {_LEDGER_TABLE} AS newer\n"
        f"          WHERE {_core(job, 'newer')}\n"
        f"            AND newer.status IN ({statuses})\n"
        f"            AND (newer.attempt, newer.updated_at, newer.id)\n"
        f"              > ({ledger}.attempt, {ledger}.updated_at, {ledger}.id)\n"
        f"      )"
    )
