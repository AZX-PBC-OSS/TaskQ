"""The bulk ledger import workflow: authoring + bodies.

The graph (spelled dataflow-style by the builder — a promise consumed
downstream is an edge)::

    read_partition ──(map: validate_chunk, per chunk)──▶ flatten ──(route)──▶ tally
                cache=True                              ValidRow → apply_entry
                                                        InvalidRow → quarantine_entry

Every box is a fresh job with its own ledger identity; the map's
children carry the chunk index as their ``map_index``; the route's
children carry their row's verdict; the tally is the fan-in that packs
the typed completion receipt.
"""

import hashlib
import os
import re
from datetime import date
from typing import Any, cast

from pydantic import ValidationError

from examples.ledger_import import store
from examples.ledger_import.models import (
    ACCOUNT_PATTERN,
    CURRENCIES,
    MEMO_MAX_CHARS,
    EntryChunk,
    InvalidBatch,
    InvalidRow,
    LedgerReceipt,
    LedgerRecord,
    RowOutcome,
    RunInput,
    TransformedRow,
    ValidBatch,
    ValidRow,
)
from taskq.workflows import (
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)

app = WorkflowApp()

# ── The transform's cost knob ────────────────────────────────────────────
# The transform is deliberately expensive (~1 ms of deterministic CPU per
# row) so the cross-run cache's second-run hit is measurable in wall clock.
_BURN_ROUNDS = 1_500


def _burn(seed: str) -> float:
    """A deterministic CPU burn keyed on the row — the transform's cost."""
    h = hashlib.sha512(seed.encode()).digest()
    acc = 0.0
    for _ in range(_BURN_ROUNDS):
        h = hashlib.sha512(h + h).digest()
        acc += h[0]
    return acc


# ── The crash-injection seam (the resume drill's determinism) ────────────
# LEDGER_CRASH_AT=<n> makes the Nth apply-batch execution hard-exit the
# process AFTER its upsert committed — the crash class where the driver
# vanishes with some batches terminal and some in flight. Unset: never.
_apply_executions = 0


def _maybe_crash() -> None:
    global _apply_executions
    crash_at = os.environ.get("LEDGER_CRASH_AT")
    if not crash_at:
        return
    _apply_executions += 1
    if _apply_executions == int(crash_at):
        print(
            f"[crash] hard exit at apply-batch #{_apply_executions} (LEDGER_CRASH_AT)", flush=True
        )
        os._exit(137)  # the hard crash: no cleanup, no terminal writes


def validation_reasons(raw: dict[str, Any]) -> list[str]:
    """The intake rules. Empty list = accepted; else the refusal reasons."""
    reasons: list[str] = []
    try:
        record = LedgerRecord.model_validate(raw)
    except ValidationError as exc:
        first = exc.errors()[0]
        return [f"decode: {first['loc'][0]} {first['type']}"]

    if record.amount <= 0:
        reasons.append("amount_must_be_positive")
    if record.currency not in CURRENCIES:
        reasons.append("currency_not_supported")
    if re.fullmatch(ACCOUNT_PATTERN, record.account) is None:
        reasons.append("account_code_format")
    if len(record.memo) > MEMO_MAX_CHARS:
        reasons.append("memo_too_long")
    if record.posted_at > date.today():
        reasons.append("posted_at_not_in_future")
    return reasons


def _partition(rows: list[TransformedRow], chunk_size: int) -> list[EntryChunk]:
    """Fixed-size chunks over the transformed rows (the partition face)."""
    chunks: list[EntryChunk] = []
    for start in range(0, len(rows), chunk_size):
        piece = rows[start : start + chunk_size]
        chunk_index = start // chunk_size
        for t_row in piece:
            t_row.chunk_index = chunk_index
        chunks.append(EntryChunk(chunk_index=chunk_index, rows=piece))
    return chunks


# ── The bodies ───────────────────────────────────────────────────────────


@app.actor
async def read_partition(ctx: StepContext, spec: RunInput) -> list[EntryChunk]:
    """Ingest → transform → partition. Cached across runs (cache=True).

    The spec is WIRED as the node's payload, so the cache address is
    content-keyed: the same feed content hits the cache, a different
    feed misses it. The body records its execution — a cache hit does
    NOT re-execute the body, which is how the cache drill counts hits.
    """
    await store.record_body_run("read_partition", ctx.node_key, ctx.map_index, None)

    from examples.ledger_import import feed

    raw_rows = feed.read_feed(spec.path)
    total = len(raw_rows)
    transformed: list[TransformedRow] = []
    for i, raw in enumerate(raw_rows):
        chash = store.content_hash(raw)
        _burn(chash)  # the expensive, deterministic transform
        transformed.append(TransformedRow(chunk_index=-1, raw=raw, content_hash=chash))
        if i % max(1, total // 20) == 0:
            await ctx.progress(
                round(i / total * 100),
                f"transform {i}/{total}",
                {"rows": total, "transformed": i},
            )
    await ctx.progress(100, f"transformed {total} rows", {"rows": total})
    return _partition(transformed, spec.chunk_size)


@app.actor
async def validate_chunk(ctx: StepContext, chunk: EntryChunk) -> list[ValidBatch | InvalidBatch]:
    """Per-chunk validation: the branch's PRODUCER.

    The chunk body itself writes nothing — its verdict batches are the
    union the typed route dispatches (one valid batch and/or one invalid
    batch per chunk). Progress is reported per chunk: ``chunk N of M``.
    """
    await store.record_body_run("validate_chunk", ctx.node_key, ctx.map_index, None)

    valid_rows: list[ValidRow] = []
    invalid_rows: list[InvalidRow] = []
    for t_row in chunk.rows:
        reasons = validation_reasons(t_row.raw)
        if not reasons:
            valid_rows.append(
                ValidRow(
                    chunk_index=chunk.chunk_index,
                    row=LedgerRecord.model_validate(t_row.raw),
                    content_hash=t_row.content_hash,
                )
            )
        else:
            invalid_rows.append(
                InvalidRow(
                    chunk_index=chunk.chunk_index,
                    entry_id=str(t_row.raw.get("entry_id")) if t_row.raw.get("entry_id") else None,
                    raw=t_row.raw,
                    reasons=reasons,
                )
            )
    verdicts: list[ValidBatch | InvalidBatch] = []
    if valid_rows:
        verdicts.append(ValidBatch(chunk_index=chunk.chunk_index, rows=valid_rows))
    if invalid_rows:
        verdicts.append(InvalidBatch(chunk_index=chunk.chunk_index, rows=invalid_rows))
    await ctx.progress(
        100,
        f"chunk {chunk.chunk_index} validated ({len(chunk.rows)} rows)",
        {"chunk": chunk.chunk_index, "rows": len(chunk.rows)},
    )
    return verdicts


@app.actor
async def _flatten(
    ctx: StepContext, chunks: list[list[ValidBatch | InvalidBatch]]
) -> list[ValidBatch | InvalidBatch]:
    """The map join's verdict batches, flattened for the route."""
    return [verdict for chunk_verdicts in chunks for verdict in chunk_verdicts]


@app.actor
async def apply_batch(ctx: StepContext, batch: ValidBatch) -> list[RowOutcome]:
    """The route's VALID arm: the idempotent batched upsert."""
    await store.record_body_run("apply_batch", ctx.node_key, ctx.map_index, None)
    batch_rows = [(v.row.entry_id, v.row.model_dump(), v.content_hash) for v in batch.rows]
    kinds = await store.upsert_valid_batch(batch_rows)
    _maybe_crash()
    return [
        RowOutcome(kind=kinds[v.row.entry_id], entry_id=v.row.entry_id, chunk_index=v.chunk_index)
        for v in batch.rows
    ]


@app.actor
async def quarantine_batch(ctx: StepContext, batch: InvalidBatch) -> list[RowOutcome]:
    """The route's INVALID arm: batched quarantine with reasons."""
    await store.record_body_run("quarantine_batch", ctx.node_key, ctx.map_index, None)
    await store.quarantine_batch([(v.entry_id, v.raw, v.reasons) for v in batch.rows])
    return [
        RowOutcome(
            kind="quarantined",
            entry_id=v.entry_id,
            chunk_index=v.chunk_index,
            reasons=v.reasons,
        )
        for v in batch.rows
    ]


@app.actor
async def tally(ctx: StepContext, batches: list[list[RowOutcome]]) -> LedgerReceipt:
    """The fan-in: the typed completion receipt. Counts are exact.

    The route join packs each arm body's return verbatim — the arms
    return per-row outcome lists — so the tally flattens them.
    """
    await store.record_body_run("tally", ctx.node_key, ctx.map_index, None)
    outcomes = [o for batch in batches for o in batch]

    counts = {"imported": 0, "updated": 0, "unchanged": 0, "quarantined": 0}
    reasons_tally: dict[str, int] = {}
    samples: list[str] = []
    for outcome in outcomes:
        counts[outcome.kind] += 1
        for reason in outcome.reasons:
            reasons_tally[reason] = reasons_tally.get(reason, 0) + 1
            if len(samples) < 5:
                samples.append(f"{outcome.entry_id}: {reason}")

    return LedgerReceipt(
        run_key="",  # the driver stamps the run key it used (it is run-scoped)
        total_rows=len(outcomes),
        imported=counts["imported"],
        updated=counts["updated"],
        unchanged=counts["unchanged"],
        quarantined=counts["quarantined"],
        chunks=len({outcome.chunk_index for outcome in outcomes}),
        quarantine_reasons=reasons_tally,
        sample_quarantined=samples,
    )


# ── The builder: the DAG, spelled as dataflow ────────────────────────────


def register_flow(spec: RunInput) -> str:
    """Register the workflow for THIS feed content; return its name.

    The spec is wired into the partition step's payload, so the cached
    step's address is content-keyed: a later run over the same feed
    content is served the recorded result; a different feed is a
    different address, never a stale hit.
    """
    name = f"ledger_bulk_import_{spec.sha256[:12]}"

    def builder() -> Promise[LedgerReceipt]:
        chunks = step(read_partition, spec, key="partition", cache=True)
        validated = map_source(chunks, body=validate_chunk)
        flat = step(_flatten, validated, key="flatten")
        settled_raw = route(
            flat,
            {ValidBatch: RouteArm(body=apply_batch), InvalidBatch: RouteArm(body=quarantine_batch)},
        )
        # The actor wrapper's __call__ is not generic over the body's
        # declared return (it is `-> object`), so the wiring verbs'
        # inference degrades to Promise[object] here. The bodies above
        # all declare their real returns; the casts carry them to the
        # checker. The runtime decodes the typed shapes either way.
        settled = cast("Promise[list[list[RowOutcome]]]", settled_raw)
        receipt = cast("Promise[LedgerReceipt]", step(tally, settled, key="tally"))
        return build(receipt)

    builder.__name__ = name  # the registry keys the workflow by name
    if not app.has(name):
        app.workflow(name)(builder)
    return name
