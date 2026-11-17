"""The ONE current-claim association core (the ruling's N-8a renderer).

Every current-claim lookup — the operator cancel, the fail-closed peer
cancel, and the §5.4 non-drainage fence — resolves a job's CURRENT
ledger claim through this exact association, never a second spelling:

    ledger.job_id = job.id
    AND ledger.claim_epoch = job.claim_epoch
    AND ledger.step_key = job.step_key
    AND ledger.map_index IS NOT DISTINCT FROM job.map_index

The core carries NO attempt comparison (a non-consuming release refunds
the job's displayed ordinal below the recorded claim's attempt while the
current epoch stands — an attempt-equality leg resolves zero rows and
unfences the current claim), no park-metadata test (the epoch already
identifies the current claim across every refund representation), and
no status domain (each disposition appends its own: ``running/awaited/
crashed`` for cancellation, parked ``awaited`` for the §5.4 fence).
The epoch identifies the current physical claim generation; the step
and null-safe map legs exclude inner ``ctx.substep``/loop-iteration rows
and map siblings that share the job and epoch. No ``COALESCE`` sentinel
and no job-wide or flow-wide match is permitted.

The catalog cannot call Python (the migration's SQL function inlines
this core), so a parity control proves the function's inline spelling
matches this renderer's output — the shared truth, never a third policy.
"""

from __future__ import annotations

__all__ = ["current_claim_join"]


def current_claim_join(job_alias: str, ledger_alias: str) -> str:
    """Render the current-claim association core with the caller's own
    aliases. Neutral leaf SQL: no schema token, no parameters, no policy —
    the caller's statement owns its status domain, its capture, and its
    fence (the exact ledger ``id`` + pre-update ``updated_at``)."""
    job, ledger = job_alias, ledger_alias
    return (
        f"{ledger}.job_id = {job}.id\n"
        f"      AND {ledger}.claim_epoch = {job}.claim_epoch\n"
        f"      AND {ledger}.step_key = {job}.step_key\n"
        f"      AND {ledger}.map_index IS NOT DISTINCT FROM {job}.map_index"
    )
