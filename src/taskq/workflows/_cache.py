"""The cross-run STEP CACHE (T25) — the TEMPORAL dedup's machinery.

THE TWO-CLAIMS LAW (the design's spine): the run-key arbiter dedups
CONCURRENT (two callers racing one key get ONE run); THIS cache dedups
TEMPORAL — a LATER run over the same body + the same input reads the
EARLIER run's result and never executes the body at all. The two claims
do not overlap and neither substitutes for the other: a concurrent miss
runs its body twice (the arbiter has nothing to say — different runs),
and the CAS (``ON CONFLICT``, the store's insert) yields ONE winner
whose payload every subsequent lookup reads.

THE ADDRESS is the Nix-style recursive hash: the body's §22.1
code-version canon (``compute_code_version`` — module + qualname + the
body's own source) RECURSED with the decoded input's canonical jsonb
(the runner's resolved args, jsonb-safe, in signature order). A body's
code change IS a new address — the stale-code face is dead by
construction; the same body over the same input at any position in any
run is the SAME address — the cross-run face's whole point
(position-independent on purpose).

THE SUCCESS-ONLY LAW: the store is called from the runner's
terminal-succeeded path ONLY — a failed run's key never squats the
address (a cached failure is a lie the next run would read as truth).

The TTL is the in-DB expiry: the store writes ``expires_at``, the
lookup's freshness leg compares it against the DB clock (the estate's
DB-clock doctrine), an expired row IS a miss, and the sweep's retention
arm (``_sweep.prune_expired_step_cache``) prunes the corpses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, cast

from tors import content_hash

from taskq._json import dumps_jsonb_str
from taskq.workflows.api._runner_codec import encode_data_arg

if TYPE_CHECKING:
    import asyncpg
    from tors import JSONValue

    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "STEP_CACHE_TTL_DEFAULT_S",
    "CachedStep",
    "cache_address",
    "cache_receipt_metadata",
    "lookup_step_cache",
    "merge_cache_receipt",
    "store_step_cache",
]

#: The payload's schema tag: a future address-shape change invalidates
#: cleanly (a tag bump IS every address's change, by construction — the
#: same law the code-version payload's tag keeps).
_ADDRESS_VERSION: Final[str] = "wf-step-cache/v1"

#: The TTL's default (seconds) — the design's 24h: a cached result's
#: freshness window when the step does not declare its own
#: (``step(..., cache=True, cache_ttl=…)``).
STEP_CACHE_TTL_DEFAULT_S: Final[float] = 24 * 3600.0

#: The receipt's metadata key on the HIT run's node row: the row's
#: metadata carries the provenance (the address + the producing run's
#: id + the store instant) — the hit is auditable from the row alone.
RECEIPT_KEY: Final[str] = "wf_cache_hit"


def cache_address(code_version: str, inputs: list[object]) -> str:
    """The Nix-style recursive address: the body's §22.1 code-version
    canon + the resolved input's canonical jsonb.

    *inputs* must already be jsonb-safe (the runner's arg walk passes
    through :func:`encode_data_arg`); the tors canonical hash is
    dict-ordering-insensitive (the pin-held discipline), the list order
    is SIGNATURE order (the wiring rule — the positional identity the
    address's input half needs). The step key is deliberately absent:
    it is determined by the body's module + qualname (the code_version
    carries both), and the position-independence IS the cross-run
    face's point.
    """
    payload: list[object] = [encode_data_arg(v) for v in inputs]
    # Annotated as JSONValue (not dict[str, object]): the stub's dict
    # shape is invariant in its value type, and the literal
    # contextualizes cleanly (the code-version payload's own pattern —
    # _version.py). The cast is the declared laundering: encode_data_arg's
    # walk IS the JSON-value guarantee (models dump through their own
    # codec; the JSON-native values pass) — the address's input half is
    # canonical jsonb by construction.
    json_input = cast("JSONValue", payload)
    doc: JSONValue = {
        "schema": _ADDRESS_VERSION,
        "code_version": code_version,
        "input": json_input,
    }
    return content_hash(doc)


@dataclass(frozen=True, slots=True)
class CachedStep:
    """One cache HIT: the producing run's full result envelope (the
    ``{"value": …}`` wrapper — byte-equal to what the body-run's
    finalize wrote) + THE RECEIPT (who paid, and when)."""

    envelope: dict[str, object]
    run_id: str
    address: str
    stored_at: datetime


async def lookup_step_cache(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    address: str,
) -> CachedStep | None:
    """The claim path's lookup: the row's freshness leg is
    ``expires_at > clock_timestamp()`` — the DB clock is the
    comparison. An expired row IS a miss (``None``): the re-run
    re-fills it on its own success.

    The envelope is returned RAW (the row's own jsonb — the
    ``{"value": …}`` wrapper UNTOUCHED): the hit's finalize writes it
    back byte-equal to what the producing run's body-run wrote. The
    consumers' TYPED decode happens downstream, through the ordinary
    parents' result path (R3's face) — never here."""
    from taskq._json import loads as _json_loads
    from taskq.workflows.api._sql_runner import render_sql

    async with pool.acquire() as conn:
        row = await conn.fetchrow(render_sql(STEP_CACHE_LOOKUP_SQL, wsql.schema), address)
    if row is None:
        return None
    raw: Any = row["result"]  # pyright: ignore[reportUnknownMemberType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        # The store writes the runner's own envelope (``{"value": …}`` —
        # encode_result's shape); a non-dict row is corrupted state: a
        # MISS (the body re-runs), never a lie delivered.
        return None
    # The walk's boundary (the house Any-contract style): the dict shape
    # is asserted above; the cast IS the declared laundering — the
    # envelope's members are the runner's own encode_result product.
    envelope: dict[str, object] = cast("dict[str, object]", decoded)
    stored_at: datetime = row["created_at"]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
    run_id_uuid: Any = row["run_id"]  # pyright: ignore[reportUnknownMemberType]  # Why: same walk.
    run_id: str = str(run_id_uuid)
    return CachedStep(
        envelope=envelope,
        run_id=run_id,
        address=address,
        stored_at=stored_at,
    )


async def store_step_cache(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    address: str,
    envelope: dict[str, object],
    run_id: JobId,
    ttl_s: float,
) -> bool:
    """The terminal-succeeded store: the result envelope + the receipt
    (the producing run's id) + the in-DB expiry.

    THE CAS: ``ON CONFLICT (content_address)`` — a FRESH winner never
    loses its row (the concurrent miss's ONE winner; the loser's insert
    does nothing), an EXPIRED corpse is RE-FILLED by the re-run's
    success (the DO UPDATE arm is gated on the expired row — the
    freshness leg's own arm). Returns whether THIS call's write landed
    (the winner's report; the loser's is its own terminal's echo)."""
    from taskq.workflows.api._sql_runner import render_sql

    async with pool.acquire() as conn:
        tag = await conn.fetchval(
            render_sql(STEP_CACHE_STORE_SQL, wsql.schema),
            address,
            dumps_jsonb_str(envelope),
            str(run_id),
            ttl_s,
        )
    return tag is not None


def cache_receipt_metadata(
    *,
    address: str,
    run_id: str,
    stored_at: datetime,
) -> dict[str, object]:
    """THE RECEIPT's metadata merge-shape (the hit run's node row): the
    address + the producing run's id + the store instant — the row
    alone tells the operator the result's provenance."""
    return {
        RECEIPT_KEY: {
            "address": address,
            "run_id": run_id,
            "stored_at": stored_at.isoformat(),
        }
    }


async def merge_cache_receipt(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    job_id: JobId,
    receipt: dict[str, object],
) -> None:
    """THE RECEIPT's write (the hit run's node row): the metadata merge
    — ONE statement, called AFTER the finalize applied (the projection's
    own asymmetry: a lost receipt is a logged freshness loss, never a
    node failure; the LEDGER owns the state)."""
    from taskq.workflows.api._sql_runner import render_sql

    async with pool.acquire() as conn:
        await conn.execute(
            render_sql(STEP_CACHE_RECEIPT_SQL, wsql.schema),
            job_id,
            dumps_jsonb_str(receipt),
        )


STEP_CACHE_LOOKUP_SQL = """\
SELECT result, run_id, created_at FROM {schema}.wf_step_cache
WHERE content_address = $1
  AND expires_at > clock_timestamp()
"""


STEP_CACHE_STORE_SQL = """\
INSERT INTO {schema}.wf_step_cache (content_address, result, run_id, expires_at)
VALUES ($1, $2::jsonb, $3::uuid, clock_timestamp() + ($4::double precision * interval '1 second'))
ON CONFLICT (content_address) DO UPDATE
SET result = EXCLUDED.result,
    run_id = EXCLUDED.run_id,
    expires_at = EXCLUDED.expires_at,
    created_at = EXCLUDED.created_at
WHERE {schema}.wf_step_cache.expires_at <= clock_timestamp()
RETURNING content_address
"""


STEP_CACHE_RECEIPT_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata || $2::jsonb
WHERE id = $1
"""
