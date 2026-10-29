"""The run door: generate the feed, run the import, print the receipt.

Two subcommands:

- ``generate-feed`` — write the deterministic feed, print its sha256.
- ``run`` — run one import: create the run (the typed claim), listen to
  the progress stream, drive it to terminal, decode the typed receipt.

The receipt's last line is the SUCCESS face: ``IMPORT SUCCEEDED`` with
the exact counts. A failed run prints the failure verdict and exits 1.
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from examples.ledger_import import feed, store, workflow
from examples.ledger_import.models import LedgerReceipt, RunInput
from taskq.workflows import (
    FlowRunner,
    ProgressBackfilled,
    ProgressListener,
    ProgressUpdated,
)

SCHEMA = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
DEFAULT_FEED = os.path.join(tempfile.gettempdir(), "ledger-feed.jsonl")


def _dsn(dsn_arg: str | None) -> str:
    dsn = dsn_arg or os.environ.get("TASKQ_PG_DSN")
    if not dsn:
        raise SystemExit("no DSN: pass --dsn or set TASKQ_PG_DSN")
    return dsn


async def _listen(listener: ProgressListener, log_path: Path | None) -> list[dict[str, object]]:
    """Consume the progress push; optionally capture every event as JSONL."""
    events: list[dict[str, object]] = []
    async with listener:
        async for event in listener.updates():
            record: dict[str, object]
            match event:
                case ProgressUpdated():
                    record = {
                        "kind": "updated",
                        "node_id": str(event.node_id),
                        "pct": event.pct,
                        "message": event.message,
                        "source": event.source,
                    }
                case ProgressBackfilled():
                    record = {"kind": "backfilled", "last_seqs": len(event.last_seqs)}
            events.append(record)
            if log_path is not None:
                with log_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(record) + "\n")
    return events


async def run_import(
    dsn: str,
    feed_path: str,
    run_key: str,
    chunk_size: int = 200,
    progress_log: str | None = None,
) -> LedgerReceipt:
    """One import run: create → listen → drive → decode the receipt."""
    store.configure(dsn)
    await store.apply_ddl()
    pool = await store.pool()

    spec = RunInput(
        path=feed_path,
        sha256=feed.feed_sha256(feed_path),
        chunk_size=chunk_size,
    )
    compiled = workflow.app.get(workflow.register_flow(spec))

    runner = FlowRunner(compiled, pool, SCHEMA)
    claim = await runner.create_flow(input=spec, run_key=run_key)
    print(f"claim: kind={claim.kind} status={claim.status} flow_id={claim.flow_id}", flush=True)

    listener = ProgressListener(pool, SCHEMA, flow_id=claim.flow_id)
    log_path = Path(progress_log) if progress_log else None
    if log_path is not None and log_path.exists():
        log_path.unlink()
    listen_task = asyncio.create_task(_listen(listener, log_path))

    started = time.monotonic()
    try:
        await runner.drive(claim.flow_id)
        result = await runner.result(claim.flow_id)
    finally:
        listen_task.cancel()
        await asyncio.gather(listen_task, return_exceptions=True)
    elapsed = time.monotonic() - started

    receipt = LedgerReceipt.model_validate(result).model_copy(update={"run_key": run_key})
    print(f"drive+result wall: {elapsed:.2f}s", flush=True)
    print(receipt.model_dump_json(indent=2), flush=True)
    print(
        f"IMPORT SUCCEEDED run_key={receipt.run_key} rows={receipt.total_rows} "
        f"imported={receipt.imported} updated={receipt.updated} "
        f"unchanged={receipt.unchanged} quarantined={receipt.quarantined}",
        flush=True,
    )
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="Postgres DSN (default: $TASKQ_PG_DSN)")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate-feed", help="generate the deterministic feed")
    gen.add_argument("--rows", type=int, default=2400)
    gen.add_argument("--path", default=DEFAULT_FEED)
    gen.add_argument("--seed", type=int, default=7)

    run_p = sub.add_parser("run", help="run one import")
    run_p.add_argument("--feed", required=True)
    run_p.add_argument("--run-key", required=True)
    run_p.add_argument("--chunk-size", type=int, default=200)
    run_p.add_argument("--progress-log", default=None)

    args = parser.parse_args(argv)
    dsn = _dsn(args.dsn)

    if args.command == "generate-feed":
        sha = feed.generate_feed(args.path, args.rows, seed=args.seed)
        print(f"feed: rows={args.rows} path={args.path} sha256={sha}")
        return 0

    try:
        asyncio.run(run_import(dsn, args.feed, args.run_key, args.chunk_size, args.progress_log))
    except Exception as exc:  # the run door's failure face: the verdict, exit 1
        print(f"IMPORT FAILED: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
