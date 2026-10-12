"""The MOCK external risk-enrichment API — and the adverse world it injects.

This module stands in for the one external dependency the real service
calls. The mock is the chaos boundary: it fails transiently (~15%), hangs
past its declared timeout (~5%), and charges realistic latency
(50-300 ms). The profile is :mod:`examples.ledgerwatch.config`'s recorded
dynamics; the draws are DETERMINISTIC per (txn_id, call_index) so a
drill's retry story is stable across processes (the workers draw the
same world).

Every call is journalled to a durable table (``lw_enrich_calls``) — the
drills' receipt: retry counts, per-call latency, the concurrency
overlap. A journal row is written at call START (before the outcome is
known) and completed at the end, so a crash mid-call leaves the call
VISIBLE (started-but-never-finished), exactly like a real API's access
log would.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Literal

from examples.ledgerwatch.config import HIGH_RISK_THRESHOLD_CENTS, DynamicsProfile
from examples.ledgerwatch.models import ClearableTxn, HighRiskTxn, TransactionRecord
from examples.ledgerwatch.pooling import LazyPool


class TransientEnrichmentError(Exception):
    """The base class the enrichment ladder retries on."""


class EnrichmentFlakeError(TransientEnrichmentError):
    """The API answered with a transient typed error (its 503 face)."""


class EnrichmentTimeoutError(TransientEnrichmentError):
    """The API hung past the client's declared timeout."""


class MockEnrichmentAPI:
    """The mock external API. Deterministic per (txn_id, call_index)."""

    def __init__(self, pool_ref: LazyPool, schema: str, profile: DynamicsProfile) -> None:
        self._pool_ref = pool_ref
        self._schema = schema
        self._profile = profile

    # ── the deterministic draw ───────────────────────────────────────────

    def _draw(self, txn_id: str, call_index: int) -> tuple[Literal["ok", "flake", "hang"], float]:
        """One draw: the outcome + the latency this call charges.

        Deterministic in (txn_id, call_index): every worker process that
        re-runs the same attempt sees the SAME world (the drills' story
        is stable), while different calls see different draws (the
        ladder genuinely escapes the flake).
        """
        rng = random.Random(f"ledgerwatch:{txn_id}#{call_index}")  # noqa: S311  # Why: the mock's draws are deliberately NON-cryptographic — deterministic per (txn, call), the drills' recorded world.
        roll = rng.random()
        p_flake, p_timeout = self._profile.flake_rate, self._profile.timeout_rate
        if roll < p_flake:
            outcome = "flake"
        elif roll < p_flake + p_timeout:
            outcome = "hang"
        else:
            outcome = "ok"
        latency = self._profile.latency_min_s + (
            (self._profile.latency_max_s - self._profile.latency_min_s) * rng.random()
        )
        return outcome, latency

    # ── the durable journal ──────────────────────────────────────────────

    async def _journal_start(self, txn_id: str, call_index: int) -> str:
        pool = await self._pool_ref.get()
        return await pool.fetchval(
            f'INSERT INTO "{self._schema}".lw_enrich_calls'  # noqa: S608  # Why: the schema is the deployment's own settings-validated identifier; every caller value is a bound parameter.
            " (txn_id, call_index, outcome) VALUES ($1, $2, 'inflight')"
            " RETURNING id",
            txn_id,
            call_index,
        )

    async def _journal_finish(
        self, call_id: str, outcome: str, latency_s: float, detail: str
    ) -> None:
        pool = await self._pool_ref.get()
        await pool.execute(
            f'UPDATE "{self._schema}".lw_enrich_calls'  # noqa: S608  # Why: as above — the schema identifier only.
            " SET outcome = $2, latency_s = $3, detail = $4, finished_at = now() WHERE id = $1",
            call_id,
            outcome,
            latency_s,
            detail,
        )

    async def _next_call_index(self, txn_id: str) -> int:
        pool = await self._pool_ref.get()
        return await pool.fetchval(
            f'SELECT count(*) FROM "{self._schema}".lw_enrich_calls WHERE txn_id = $1',  # noqa: S608
            txn_id,
        )

    # ── the API call ─────────────────────────────────────────────────────

    async def enrich(self, txn: TransactionRecord) -> HighRiskTxn | ClearableTxn:
        """ONE enrichment call: the external API's full face — latency,
        transient flakes, hangs past the declared timeout — and the
        typed risk verdict on success. Raises
        :class:`TransientEnrichmentError` (the ladder's target); a
        transaction never dies of flakiness while the ladder has room.
        """
        call_index = await self._next_call_index(txn.txn_id)
        call_id = await self._journal_start(txn.txn_id, call_index)
        outcome, latency = self._draw(txn.txn_id, call_index)
        started = time.monotonic()
        try:
            # THE JITTER IS EVERY CALL'S: 50-300 ms of realistic latency
            # before ANY answer (ok, flake, or hang).
            await asyncio.sleep(latency)
            if outcome == "flake":
                raise EnrichmentFlakeError(
                    f"enrichment upstream puffed (txn {txn.txn_id}, call {call_index})"
                )
            if outcome == "hang":
                # The server would finish PAST the declared timeout; the
                # client's deadline cancels it — the timeout face.
                try:
                    async with asyncio.timeout(self._profile.enrich_timeout_s):
                        await asyncio.sleep(latency + self._profile.hang_overshoot_s)
                except TimeoutError:
                    raise EnrichmentTimeoutError(
                        f"enrichment hung past the declared timeout "
                        f"({self._profile.enrich_timeout_s}s) "
                        f"(txn {txn.txn_id}, call {call_index})"
                    ) from None
        except TransientEnrichmentError as exc:
            await self._journal_finish(call_id, outcome, time.monotonic() - started, str(exc))
            raise
        # THE VERDICT: the generic industry rule — large-amount
        # transactions are the HIGH-risk class. The score is a
        # deterministic stand-in for the API's model output.
        await self._journal_finish(call_id, "ok", time.monotonic() - started, "")
        score = _risk_score(txn)
        if txn.amount_cents >= HIGH_RISK_THRESHOLD_CENTS:
            return HighRiskTxn(
                kind="high",
                txn_id=txn.txn_id,
                amount_cents=txn.amount_cents,
                score=score,
                reasons=["amount at or above the high-risk threshold"],
            )
        return ClearableTxn(
            kind="clearable", txn_id=txn.txn_id, amount_cents=txn.amount_cents, score=score
        )


def _risk_score(txn: TransactionRecord) -> float:
    """A deterministic pseudo-model score in [0, 100] (the mock's face)."""
    rng = random.Random(f"score:{txn.txn_id}:{txn.merchant}")  # noqa: S311  # Why: as above — the mock's deterministic pseudo-model.
    return round(20.0 + 70.0 * rng.random(), 2)
